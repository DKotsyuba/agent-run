//! Permanent pin keys and bounded operator observation for retained lineages.
//!
//! Public pins carry a one-way credential verifier in the pin ID. Managed
//! pins use the durable enrollment record bound to the authenticated Pool route.
//! All releases observe the same active lineage hold fence.

use crate::Store;
use agent_run_domain::{
    Error, Result, StoreId,
    batch::{
        HistoryPin, HistoryPinFilter, HistoryPinRelease, HistoryPinState, HistoryPinsPage, PinId,
    },
    domain::AgentId,
    error::invalid,
};
use rusqlite::{OptionalExtension, params};

/// Release proof reduced by the broker before it reaches the writer.
#[derive(Debug, Clone, serde::Serialize)]
pub enum ReleaseAuthority {
    /// Digest of the public bearer credential, never the credential itself.
    Credential([u8; 32]),
    /// Durable enrollment named on its authenticated Pool route.
    ManagedEnrollment {
        /// Route identity verified by the private attempt channel.
        route: agent_run_domain::attempt_binding::Route,
        /// The original Pool enrollment key.
        request_id: String,
    },
}

impl Store<'_> {
    /// Lists matching pins in exclusive key order without joining retained history.
    pub fn history_pins(
        &self,
        filter: HistoryPinFilter,
        limit: usize,
        after: Option<&PinId>,
    ) -> Result<HistoryPinsPage> {
        self.history_pins_scoped(filter, limit, after, None)
    }

    /// Lists a page with optional identity checked in the same read snapshot.
    pub fn history_pins_scoped(
        &self,
        filter: HistoryPinFilter,
        limit: usize,
        after: Option<&PinId>,
        expected: Option<&StoreId>,
    ) -> Result<HistoryPinsPage> {
        if !(1..=1000).contains(&limit) {
            return Err(invalid("history pin limit must be 1..=1000"));
        }
        let state = match filter {
            HistoryPinFilter::All => None,
            HistoryPinFilter::Active => Some(0),
            HistoryPinFilter::Released => Some(1),
        };
        let tx = self.conn.unchecked_transaction()?;
        if expected.is_some() {
            crate::store_identity::require(&tx, expected)?;
        }
        let mut statement = tx.prepare(
            "SELECT pin_id,root_agent_id,released FROM history_pins \
             WHERE (?1 IS NULL OR pin_id>?1) AND (?2 IS NULL OR released=?2) ORDER BY pin_id LIMIT ?3",
        )?;
        let rows = statement.query_map(
            params![after.map(PinId::as_str), state, (limit + 1) as i64],
            |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, i64>(2)?,
                ))
            },
        )?;
        let mut pins = Vec::new();
        for row in rows {
            let (pin_id, agent_id, released) = row?;
            pins.push(HistoryPin {
                pin_id: pin_id
                    .parse()
                    .map_err(|_| Error::Integrity("invalid stored pin identity".into()))?,
                agent_id: agent_id
                    .parse()
                    .map_err(|_| Error::Integrity("invalid stored pin root".into()))?,
                status: match released {
                    0 => HistoryPinState::Active,
                    1 => HistoryPinState::Released,
                    _ => return Err(Error::Integrity("invalid stored pin state".into())),
                },
            });
        }
        let more = pins.len() > limit;
        if more {
            pins.pop();
        }
        let next_after_pin_id = if more {
            pins.last().map(|pin| pin.pin_id.clone())
        } else {
            None
        };
        drop(statement);
        tx.commit()?;
        Ok(HistoryPinsPage {
            pins,
            next_after_pin_id,
        })
    }

    /// Releases a known pin without any proof in test fixtures.
    #[cfg(feature = "test-fixtures")]
    pub fn release_history_pin(&mut self, pin_id: &PinId) -> Result<Option<HistoryPinRelease>> {
        self.release_history_pin_checked(pin_id, None, |_, _| Ok(true), false)
    }

    /// Authenticates the broker's proof in the same transaction as release.
    /// Public callers get the same conflict for an absent pin or invalid proof.
    pub fn release_history_pin_scoped(
        &mut self,
        pin_id: &PinId,
        expected: Option<&StoreId>,
        authority: &ReleaseAuthority,
    ) -> Result<Option<HistoryPinRelease>> {
        self.release_history_pin_checked(
            pin_id,
            expected,
            |tx, root| match authority {
                ReleaseAuthority::Credential(digest) => {
                    let root: AgentId = root
                        .parse()
                        .map_err(|_| Error::Integrity("invalid stored pin root".into()))?;
                    // The compared value is public. It grants no authority as input.
                    Ok(PinId::from_release_digest(digest, &root) == *pin_id)
                }
                ReleaseAuthority::ManagedEnrollment { route, request_id } => {
                    let route_json = serde_json::to_string(route)?;
                    Ok(tx.query_row(
                        "SELECT EXISTS(SELECT 1 FROM managed_enrollment_members m \
                         JOIN managed_enrollments e USING(pool_route_json,pool_store_id,request_id), \
                         json_each(e.receipt_json,'$.jobs') j \
                         WHERE m.lineage_root_id=?1 AND m.pool_route_json=?2 AND m.pool_store_id=?3 \
                         AND m.request_id=?4 AND e.state='admitted' AND j.key=m.member_index \
                         AND json_extract(j.value,'$.pin_id')=?5)",
                        params![root, route_json, route.component_store_id, request_id, pin_id.as_str()],
                        |row| row.get(0),
                    )?)
                }
            },
            matches!(authority, ReleaseAuthority::Credential(_)),
        )
    }

    /// Releases a public batch pin under exclusive offline owner authority.
    /// The exact receipt prevents a mistaken pin/key pairing or managed release.
    pub(crate) fn release_history_pin_offline(
        &mut self,
        pin_id: &PinId,
        expected: &StoreId,
        request_id: &str,
    ) -> Result<Option<HistoryPinRelease>> {
        agent_run_domain::domain::external_id("request_id", request_id)?;
        if agent_run_domain::batch::is_reserved_internal_key(request_id) {
            return Err(invalid("request_id uses a reserved internal prefix"));
        }
        self.release_history_pin_checked(
            pin_id,
            Some(expected),
            |tx, root| offline_receipt_contains_pin(tx, request_id, pin_id, root),
            false,
        )
    }

    /// Verifies authorization even for tombstones and applies the hold fence.
    fn release_history_pin_checked(
        &mut self,
        pin_id: &PinId,
        expected: Option<&StoreId>,
        authorize: impl FnOnce(&rusqlite::Connection, &str) -> Result<bool>,
        conceal_absence: bool,
    ) -> Result<Option<HistoryPinRelease>> {
        let tx = crate::writer::WriteTx::begin(&self.conn)?;
        if expected.is_some() {
            crate::store_identity::require(&tx, expected)?;
        }
        let row: Option<(i64, String)> = tx
            .query_row(
                "SELECT released,root_agent_id FROM history_pins WHERE pin_id=?",
                [pin_id.as_str()],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .optional()?;
        let Some((state, root)) = row else {
            if conceal_absence {
                return Err(Error::Conflict);
            }
            tx.commit()?;
            return Ok(None);
        };
        if !authorize(&tx, &root)? {
            return Err(Error::Conflict);
        }
        let already_released = match state {
            0 => false,
            1 => true,
            _ => return Err(Error::Integrity("invalid stored pin state".into())),
        };
        if !already_released {
            crate::lineage_holds::require_pin_unheld(&tx, pin_id)?;
            let changed = tx.execute(
                "UPDATE history_pins SET released=1 WHERE pin_id=? AND released=0",
                [pin_id.as_str()],
            )?;
            if changed != 1 {
                return Err(Error::Integrity("history pin release lost its row".into()));
            }
        }
        tx.commit()?;
        Ok(Some(HistoryPinRelease {
            pin_id: pin_id.clone(),
            status: HistoryPinState::Released,
            already_released,
        }))
    }
}

/// Checks whether the permanent receipt minted this exact pin in this snapshot.
fn offline_receipt_contains_pin(
    conn: &rusqlite::Connection,
    key: &str,
    pin_id: &PinId,
    root: &str,
) -> Result<bool> {
    Ok(conn.query_row(
        "SELECT EXISTS(SELECT 1 FROM start_batch_receipts r, json_each(r.receipt_json,'$.jobs') j \
         WHERE r.request_id=?1 AND json_extract(j.value,'$.pin_id')=?2 \
         AND NOT EXISTS(SELECT 1 FROM managed_enrollment_members WHERE lineage_root_id=?3))",
        params![key, pin_id.as_str(), root],
        |row| row.get(0),
    )?)
}
