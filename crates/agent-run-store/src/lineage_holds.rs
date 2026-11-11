//! Durable, signature-released holds over exact successful lineage tails.
//!
//! Acquisition and every mutable precondition share one writer transaction.
//! Holds never expire, and overlapping holds retain independent release rights.

use crate::Store;
use agent_run_domain::{
    Error, Result, StoreId,
    domain::{AgentId, Status},
    lineage_hold::{HoldId, LineageHoldAcquire, LineageHoldTarget, release_signed_bytes},
    status::{
        CleanupState, Coverage, PinState, RestartResume, StatusMatch, StatusRequest,
        StatusSelector, SuccessCommit,
    },
};
use rusqlite::{Connection, OptionalExtension, params};
use serde::{Deserialize, Serialize};
use std::collections::HashSet;

/// Maximum simultaneously active holds in one store; released receipts remain durable.
pub const MAX_ACTIVE_LINEAGE_HOLDS: usize = 1024;

/// Persistent lifecycle; released tombstones can never acquire protection again.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LineageHoldState {
    Active,
    Released,
}

/// Immutable acquisition receipt, with canonical roots in exact target order.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LineageHoldReceipt {
    pub version: u32,
    pub store_id: StoreId,
    pub hold_id: HoldId,
    pub fingerprint: [u8; 32],
    pub targets: Vec<LineageHoldTarget>,
    pub canonical_roots: Vec<AgentId>,
}

/// An acquisition receipt plus its current tombstone state and replay marker.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LineageHoldAcquireResult {
    pub receipt: LineageHoldReceipt,
    pub state: LineageHoldState,
    pub replayed: bool,
}

/// Authenticated release outcome; a repeated release retains the same tombstone.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LineageHoldRelease {
    pub hold_id: HoldId,
    pub state: LineageHoldState,
    pub already_released: bool,
}

/// Public recovery facts from one committed-history snapshot; no key or request text.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LineageHoldObservation {
    pub receipt: LineageHoldReceipt,
    pub state: LineageHoldState,
}

/// Optional lifecycle filter for a read-only inventory page.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LineageHoldFilter {
    All,
    Active,
    Released,
}

/// One snapshot page in ascending bytewise hold-ID order.
/// A cursor exists only when another matching row was observed after this page.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LineageHoldsPage {
    pub holds: Vec<LineageHoldObservation>,
    pub next_after_hold_id: Option<HoldId>,
}

/// Decodes a persisted state without treating corruption as permission to mutate.
fn state(released: i64) -> Result<LineageHoldState> {
    match released {
        0 => Ok(LineageHoldState::Active),
        1 => Ok(LineageHoldState::Released),
        _ => Err(Error::Integrity("invalid lineage hold state".into())),
    }
}

/// Decodes fixed-length stored cryptographic bytes without exposing them in errors.
fn bytes32(bytes: Vec<u8>) -> Result<[u8; 32]> {
    bytes
        .try_into()
        .map_err(|_| Error::Integrity("invalid lineage hold bytes".into()))
}

/// Reconstructs a receipt entirely from permanent ordered membership rows.
fn receipt(
    conn: &Connection,
    store_id: StoreId,
    hold_id: HoldId,
    fingerprint: [u8; 32],
    target_count: usize,
) -> Result<LineageHoldReceipt> {
    let mut statement = conn.prepare(
        "SELECT ordinal,agent_id,sequence,execution_ref,pin_id,root_agent_id \
         FROM lineage_hold_targets WHERE hold_id=? ORDER BY ordinal LIMIT 21",
    )?;
    let mut rows = statement.query([hold_id.as_str()])?;
    let mut targets = Vec::new();
    let mut canonical_roots = Vec::new();
    while let Some(row) = rows.next()? {
        let ordinal: usize = row.get(0)?;
        if ordinal != targets.len() || targets.len() >= 20 {
            return Err(Error::Integrity("invalid lineage hold target order".into()));
        }
        let agent: String = row.get(1)?;
        let reference: String = row.get(3)?;
        let pin: String = row.get(4)?;
        let root: String = row.get(5)?;
        targets.push(LineageHoldTarget {
            agent_id: agent
                .parse()
                .map_err(|_| Error::Integrity("invalid held agent".into()))?,
            sequence: row.get(2)?,
            execution_ref: reference
                .parse()
                .map_err(|_| Error::Integrity("invalid held execution".into()))?,
            pin_id: pin
                .parse()
                .map_err(|_| Error::Integrity("invalid held pin".into()))?,
        });
        canonical_roots.push(
            root.parse()
                .map_err(|_| Error::Integrity("invalid held root".into()))?,
        );
    }
    if !(1..=20).contains(&target_count) || targets.len() != target_count {
        return Err(Error::Integrity("incomplete lineage hold receipt".into()));
    }
    Ok(LineageHoldReceipt {
        version: 1,
        store_id,
        hold_id,
        fingerprint,
        targets,
        canonical_roots,
    })
}

/// Rejects mutation of an execution or its canonical lineage while any hold is active.
/// Call inside the mutation's writer transaction; this is not a preflight permission.
pub(crate) fn require_unheld(conn: &Connection, id: &AgentId) -> Result<()> {
    let held: bool = conn.query_row(
        "SELECT EXISTS(SELECT 1 FROM lineage_hold_targets t JOIN lineage_holds h USING(hold_id) \
         WHERE h.released=0 AND t.root_agent_id=COALESCE(\
         (SELECT COALESCE(NULLIF(root_agent_id,''),id) FROM agents WHERE id=?1),?1))",
        [id.as_str()],
        |row| row.get(0),
    )?;
    if held {
        return Err(Error::Conflict);
    }
    Ok(())
}

/// Rejects releasing a retention pin used by any still-active hold.
pub(crate) fn require_pin_unheld(
    conn: &Connection,
    pin: &agent_run_domain::batch::PinId,
) -> Result<()> {
    let held: bool = conn.query_row(
        "SELECT EXISTS(SELECT 1 FROM lineage_hold_targets t JOIN lineage_holds h USING(hold_id) \
         WHERE h.released=0 AND t.pin_id=?)",
        [pin.as_str()],
        |row| row.get(0),
    )?;
    if held {
        return Err(Error::Conflict);
    }
    Ok(())
}

impl Store<'_> {
    /// Looks up an exact request and fingerprint within the expected store, without writing.
    /// A fingerprint mismatch conflicts, including for a released tombstone.
    /// `None` means only absence from this snapshot's committed history: an in-flight
    /// acquisition may commit later. Lookup never creates, releases or reacquires a hold.
    pub fn lookup_lineage_hold(
        &self,
        expected_store_id: &StoreId,
        request_id: &str,
        fingerprint: &[u8; 32],
    ) -> Result<Option<LineageHoldObservation>> {
        if request_id.len() > 2048 {
            return Err(agent_run_domain::error::invalid(
                "hold request_id exceeds 2048 UTF-8 bytes",
            ));
        }
        agent_run_domain::domain::external_id("hold request_id", request_id)?;
        let tx = self.conn.unchecked_transaction()?;
        let store_id = crate::store_identity::require(&tx, Some(expected_store_id))?;
        let stored: Option<(String, Vec<u8>, i64, usize)> = tx.query_row(
            "SELECT hold_id,fingerprint,released,target_count FROM lineage_holds WHERE request_id=?",
            [request_id], |row| Ok((row.get(0)?,row.get(1)?,row.get(2)?,row.get(3)?)),
        ).optional()?;
        let Some((hold_id, stored_fingerprint, released, count)) = stored else {
            tx.commit()?;
            return Ok(None);
        };
        let stored_fingerprint = bytes32(stored_fingerprint)?;
        if &stored_fingerprint != fingerprint {
            return Err(Error::Conflict);
        }
        let hold_id = hold_id
            .parse()
            .map_err(|_| Error::Integrity("invalid hold identity".into()))?;
        let receipt = receipt(&tx, store_id, hold_id, stored_fingerprint, count)?;
        let state = state(released)?;
        tx.commit()?;
        Ok(Some(LineageHoldObservation { receipt, state }))
    }

    /// Lists up to `limit` (1..=100) matching holds in bytewise ascending `hold_id`
    /// order, exclusively after `after`, with at most 20 exact targets per hold.
    /// Every page uses one read snapshot; pages do not form a shared snapshot.
    /// Concurrent inserts can sort before a cursor, so a fresh scan is necessary
    /// to discover new holds. A missing next cursor means only this snapshot's end.
    /// No public/private keys, intent digests or opaque request text are returned.
    /// This bounds store results, not encoded transport bytes: callers must enforce
    /// their own response limit without silently truncating receipts or targets.
    pub fn list_lineage_holds(
        &self,
        expected_store_id: &StoreId,
        filter: LineageHoldFilter,
        limit: usize,
        after: Option<&HoldId>,
    ) -> Result<LineageHoldsPage> {
        if !(1..=100).contains(&limit) {
            return Err(agent_run_domain::error::invalid(
                "lineage hold limit must be 1..=100",
            ));
        }
        let released = match filter {
            LineageHoldFilter::All => None,
            LineageHoldFilter::Active => Some(0),
            LineageHoldFilter::Released => Some(1),
        };
        let tx = self.conn.unchecked_transaction()?;
        let store_id = crate::store_identity::require(&tx, Some(expected_store_id))?;
        // Separate queries let SQLite seek to the cursor, including when nearly
        // all permanent tombstones have a different released state.
        let query = if released.is_some() {
            "SELECT hold_id,fingerprint,released,target_count FROM lineage_holds \
             WHERE released=?3 AND hold_id>?1 ORDER BY hold_id LIMIT ?2"
        } else {
            "SELECT hold_id,fingerprint,released,target_count FROM lineage_holds \
             WHERE hold_id>?1 ORDER BY hold_id LIMIT ?2"
        };
        let mut statement = tx.prepare(query)?;
        let after = after.map(HoldId::as_str).unwrap_or("");
        let mut rows = if let Some(released) = released {
            statement.query(params![after, limit + 1, released])?
        } else {
            statement.query(params![after, limit + 1])?
        };
        let mut headers = Vec::with_capacity(limit + 1);
        while let Some(row) = rows.next()? {
            headers.push((
                row.get::<_, String>(0)?,
                row.get::<_, Vec<u8>>(1)?,
                row.get::<_, i64>(2)?,
                row.get::<_, usize>(3)?,
            ));
        }
        drop(rows);
        drop(statement);
        let more = headers.len() > limit;
        if more {
            headers.pop();
        }
        let mut holds = Vec::with_capacity(headers.len());
        for (hold_id, fingerprint, released, count) in headers {
            let hold_id = hold_id
                .parse()
                .map_err(|_| Error::Integrity("invalid hold identity".into()))?;
            let receipt = receipt(&tx, store_id.clone(), hold_id, bytes32(fingerprint)?, count)?;
            holds.push(LineageHoldObservation {
                receipt,
                state: state(released)?,
            });
        }
        let next_after_hold_id = if more {
            holds.last().map(|hold| hold.receipt.hold_id.clone())
        } else {
            None
        };
        tx.commit()?;
        Ok(LineageHoldsPage {
            holds,
            next_after_hold_id,
        })
    }

    /// Atomically proves and holds one to twenty unique successful canonical roots.
    /// Exact request replay returns its permanent receipt before rechecking mutable
    /// history, even after release or retention. Different fingerprints conflict.
    pub fn acquire_lineage_hold(
        &mut self,
        acquire: &LineageHoldAcquire,
    ) -> Result<LineageHoldAcquireResult> {
        let fingerprint = acquire.fingerprint()?;
        let tx = crate::writer::WriteTx::begin(&self.conn)?;
        let store_id = crate::store_identity::require(&tx, Some(&acquire.store_id))?;
        let existing: Option<(String, Vec<u8>, i64, usize)> = tx.query_row(
            "SELECT hold_id,fingerprint,released,target_count FROM lineage_holds WHERE request_id=?",
            [&acquire.request_id], |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
        ).optional()?;
        if let Some((id, stored, released, count)) = existing {
            if bytes32(stored)? != fingerprint {
                return Err(Error::Conflict);
            }
            let hold_id = id
                .parse()
                .map_err(|_| Error::Integrity("invalid hold identity".into()))?;
            let receipt = receipt(&tx, store_id, hold_id, fingerprint, count)?;
            let state = state(released)?;
            tx.commit()?;
            return Ok(LineageHoldAcquireResult {
                receipt,
                state,
                replayed: true,
            });
        }
        let active: usize = tx.query_row(
            "SELECT count(*) FROM (SELECT 1 FROM lineage_holds WHERE released=0 LIMIT ?)",
            [MAX_ACTIVE_LINEAGE_HOLDS],
            |row| row.get(0),
        )?;
        if active >= MAX_ACTIVE_LINEAGE_HOLDS {
            return Err(Error::Runtime(
                "active lineage hold capacity reached".into(),
            ));
        }
        crate::hold_crypto::validate_public_key(&acquire.verifying_key)?;
        let request = StatusRequest {
            version: 1,
            expected_store_id: Some(store_id.clone()),
            include_view: false,
            targets: acquire
                .targets
                .iter()
                .map(|target| StatusSelector {
                    agent_id: target.agent_id.clone(),
                    sequence: target.sequence,
                    execution_ref: Some(target.execution_ref.clone()),
                    pin_id: Some(target.pin_id.clone()),
                })
                .collect(),
        };
        let snapshot = crate::status::read_in_snapshot(&tx, &request)?;
        let mut roots = Vec::with_capacity(acquire.targets.len());
        let mut unique = HashSet::new();
        for (target, item) in acquire.targets.iter().zip(&snapshot.response.items) {
            let exact = item.selected.as_ref().is_some_and(|selected| {
                selected.sequence == target.sequence
                    && selected.execution_ref.as_ref() == Some(&target.execution_ref)
                    && selected.raw_status == Status::Succeeded
            });
            if item.r#match != StatusMatch::Verified
                || item.pin_state != PinState::Active
                || item.coverage != Coverage::Complete
                || item.restart_resume != RestartResume::None
                || item.lineage_terminal != Some(true)
                || item.terminal_outcome != Some(Status::Succeeded)
                || item.success_commit != SuccessCommit::Recorded
                || item.cleanup.scope != "lineage"
                || item.cleanup.state != CleanupState::Confirmed
                || !exact
                || item.selected != item.retained_latest
            {
                return Err(Error::Conflict);
            }
            let root = item.canonical_agent_id.clone().ok_or(Error::Conflict)?;
            if !unique.insert(root.clone()) {
                return Err(Error::Conflict);
            }
            roots.push(root);
        }
        if roots.len() != acquire.targets.len() {
            return Err(Error::Conflict);
        }
        let hold_id: HoldId = format!("hold_{}", uuid::Uuid::new_v4().simple()).parse()?;
        tx.execute(
            "INSERT INTO lineage_holds(hold_id,request_id,version,fingerprint,verifying_key,intent_digest,target_count) \
             VALUES(?,?,1,?,?,?,?)",
            params![hold_id.as_str(), acquire.request_id, fingerprint.as_slice(), acquire.verifying_key.as_slice(),
                acquire.intent_digest.as_ref().map(|digest| digest.as_slice()), acquire.targets.len()],
        )?;
        for (ordinal, (target, root)) in acquire.targets.iter().zip(&roots).enumerate() {
            tx.execute(
                "INSERT INTO lineage_hold_targets(hold_id,ordinal,agent_id,sequence,execution_ref,pin_id,root_agent_id) \
                 VALUES(?,?,?,?,?,?,?)",
                params![hold_id.as_str(), ordinal, target.agent_id.as_str(), target.sequence,
                    target.execution_ref.as_str(), target.pin_id.as_str(), root.as_str()],
            )?;
        }
        let receipt = LineageHoldReceipt {
            version: 1,
            store_id,
            hold_id,
            fingerprint,
            targets: acquire.targets.clone(),
            canonical_roots: roots,
        };
        tx.commit()?;
        Ok(LineageHoldAcquireResult {
            receipt,
            state: LineageHoldState::Active,
            replayed: false,
        })
    }

    /// Releases only after verifying the signature over persisted store, hold and
    /// fingerprint bytes. Released replay is also authenticated and never reacquires.
    /// An unknown hold returns `None` only after the exact store identity check.
    pub fn release_lineage_hold(
        &mut self,
        hold_id: &HoldId,
        expected_store_id: &StoreId,
        signature: &[u8; 64],
    ) -> Result<Option<LineageHoldRelease>> {
        let tx = crate::writer::WriteTx::begin(&self.conn)?;
        let store_id = crate::store_identity::require(&tx, Some(expected_store_id))?;
        let stored: Option<(Vec<u8>, Vec<u8>, i64)> = tx
            .query_row(
                "SELECT fingerprint,verifying_key,released FROM lineage_holds WHERE hold_id=?",
                [hold_id.as_str()],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .optional()?;
        let Some((fingerprint, key, released)) = stored else {
            tx.commit()?;
            return Ok(None);
        };
        let fingerprint = bytes32(fingerprint)?;
        let key = bytes32(key)?;
        let bytes = release_signed_bytes(&store_id, hold_id, &fingerprint)?;
        crate::hold_crypto::verify_release(&key, &bytes, signature)?;
        let already_released = state(released)? == LineageHoldState::Released;
        if !already_released
            && tx.execute(
                "UPDATE lineage_holds SET released=1 WHERE hold_id=? AND released=0",
                [hold_id.as_str()],
            )? != 1
        {
            return Err(Error::Integrity("lineage hold release lost its row".into()));
        }
        tx.commit()?;
        Ok(Some(LineageHoldRelease {
            hold_id: hold_id.clone(),
            state: LineageHoldState::Released,
            already_released,
        }))
    }
}
