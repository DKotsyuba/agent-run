//! Safe replacement of one pool member by a new execution.
//!
//! A replacement retires the old seat and installs a new one in a single
//! immediate transaction that also admits the new agent, bumps the roster
//! revision, appends the broker roster entry and fans it out. Nothing is
//! launched here, and no old row is ever rewritten beyond its `replaced_by`
//! link, so history and identities stay intact.

use crate::{
    pool_admission::PoolMemberRecord,
    pool_log::{fanout_entry, lineage_cleanup_complete, tip_of},
    provider_admission::{admit_in_tx, AdmissionInputs},
    tx_event, Store,
};
use agent_run_domain::{
    catalog::ProviderCatalog,
    domain::{now, AgentId, OrchestratorRef, Status},
    pool::{AcceptanceCriterion, PoolDenial, PoolId},
    Error, Result,
};
use rusqlite::{params, Connection, OptionalExtension, TransactionBehavior};
use serde_json::json;

/// Everything an operator replacement must know about the pool and the seat
/// being replaced, read once so the caller can compose the new task.
#[derive(Debug, Clone)]
pub struct PoolReplaceContext {
    /// Pool identity.
    pub pool_id: PoolId,
    /// The shared goal.
    pub goal: String,
    /// The acceptance criteria.
    pub acceptance: Vec<AcceptanceCriterion>,
    /// Roster revision the context was read at.
    pub roster_revision: u32,
    /// Current members in slot order, including the one being replaced.
    pub seats: Vec<PoolMemberRecord>,
    /// The seat being replaced.
    pub old: PoolMemberRecord,
    /// The old seat's original personal task.
    pub personal_task: String,
    /// Highest log sequence at read time.
    pub last_seq: u64,
    /// Current proposal sequence, when one exists.
    pub current_proposal: Option<u64>,
}

/// A committed replacement, new or replayed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PoolReplacement {
    /// Pool identity.
    pub pool_id: PoolId,
    /// The retired member.
    pub old: PoolMemberRecord,
    /// The member now holding the slot.
    pub new: PoolMemberRecord,
    /// Roster revision this replacement created.
    pub roster_revision: u32,
    /// False for a replay of the same original request.
    pub created: bool,
}

/// One replacement's trusted inputs, decided outside the transaction.
pub struct PoolReplaceInput<'a> {
    /// Pool identity.
    pub pool_id: &'a PoolId,
    /// Stable identity of the member being replaced.
    pub old: &'a AgentId,
    /// The operator's idempotency key.
    pub request_id: &'a str,
    /// Digest of the normalized outer replacement request.
    pub request_sha256: &'a str,
    /// Roster revision the new task was composed against.
    pub expected_roster_revision: u32,
    /// Pre-minted stable identity of the new member.
    pub new_id: AgentId,
    /// New member name.
    pub name: &'a str,
    /// The new member's own task before the preamble was composed in.
    pub personal_task: &'a str,
    /// Resolved catalog.
    pub catalog: &'a ProviderCatalog,
    /// Trusted admission inputs carrying the composed task.
    pub inputs: AdmissionInputs<'a>,
}

/// Reads one current member's public record.
fn record_of(conn: &Connection, pool_id: &str, agent: &str) -> Result<PoolMemberRecord> {
    let (slot, name, role): (u8, String, String) = conn.query_row(
        "SELECT slot,name,role FROM pool_members WHERE pool_id=? AND agent_id=?",
        params![pool_id, agent],
        |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
    )?;
    Ok(PoolMemberRecord {
        agent_id: agent.parse()?,
        slot,
        name,
        role,
    })
}

/// Why `old` cannot be replaced right now, or `None` when it can: the pool
/// must be open, `old` a current member, its latest execution terminal and
/// every attempt of its lineage cleaned up and released.
fn refusal(conn: &Connection, pool_id: &str, old: &AgentId) -> Result<Option<PoolDenial>> {
    let state: Option<String> = conn
        .query_row("SELECT state FROM pools WHERE id=?", [pool_id], |row| {
            row.get(0)
        })
        .optional()?;
    let Some(state) = state else {
        return Ok(Some(PoolDenial::PoolNotFound));
    };
    let seat: Option<Option<String>> = conn
        .query_row(
            "SELECT replaced_by FROM pool_members WHERE pool_id=? AND agent_id=?",
            params![pool_id, old.as_str()],
            |row| row.get(0),
        )
        .optional()?;
    match seat {
        None => return Ok(Some(PoolDenial::NotPoolMember)),
        Some(Some(_)) => return Ok(Some(PoolDenial::MemberNotCurrent)),
        Some(None) => {}
    }
    if state == "completed" {
        return Ok(Some(PoolDenial::PoolCompleted));
    }
    let tip = tip_of(conn, old)?;
    let status: String = conn.query_row(
        "SELECT status FROM agents WHERE id=?",
        [tip.as_str()],
        |row| row.get(0),
    )?;
    if !status.parse::<Status>()?.terminal() || !lineage_cleanup_complete(conn, old)? {
        return Ok(Some(PoolDenial::MemberBusy));
    }
    Ok(None)
}

/// Looks up a replacement by its scoped key and compares the original
/// request digest, using only the pool's own rows and the new member's events.
fn replayed(
    conn: &Connection,
    pool_id: &PoolId,
    request_id: &str,
    request_sha256: &str,
) -> Result<Option<std::result::Result<PoolReplacement, PoolDenial>>> {
    let revision: Option<u32> = conn
        .query_row(
            "SELECT roster_revision FROM pool_entries \
             WHERE pool_id=? AND idem_scope='replace' AND request_id=?",
            params![pool_id.as_str(), request_id],
            |row| row.get(0),
        )
        .optional()?;
    let Some(revision) = revision else {
        return Ok(None);
    };
    let new: String = conn.query_row(
        "SELECT agent_id FROM pool_members WHERE pool_id=? AND joined_roster_revision=?",
        params![pool_id.as_str(), revision],
        |row| row.get(0),
    )?;
    let stored: String = conn.query_row(
        "SELECT json_extract(data_json,'$.request_sha256') FROM events \
         WHERE agent_id=? AND kind='pool_member_replaced'",
        [&new],
        |row| row.get(0),
    )?;
    if stored != request_sha256 {
        return Ok(Some(Err(PoolDenial::Conflict)));
    }
    let old: String = conn.query_row(
        "SELECT agent_id FROM pool_members WHERE pool_id=? AND replaced_by=?",
        params![pool_id.as_str(), new],
        |row| row.get(0),
    )?;
    Ok(Some(Ok(PoolReplacement {
        pool_id: pool_id.clone(),
        old: record_of(conn, pool_id.as_str(), &old)?,
        new: record_of(conn, pool_id.as_str(), &new)?,
        roster_revision: revision,
        created: false,
    })))
}

impl Store {
    /// For `pool_id`, checks whether any current member except `old` matches `name` under the
    /// same Unicode lowercase comparison as initial pool admission.
    fn current_name_taken(
        conn: &Connection,
        pool_id: &str,
        old: &AgentId,
        name: &str,
    ) -> Result<bool> {
        let mut statement = conn.prepare(
            "SELECT name FROM pool_members WHERE pool_id=? AND replaced_by IS NULL AND agent_id<>?",
        )?;
        let names = statement
            .query_map(params![pool_id, old.as_str()], |row| {
                row.get::<_, String>(0)
            })?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        let folded = name.to_lowercase();
        Ok(names.iter().any(|current| current.to_lowercase() == folded))
    }

    /// The orchestrator reference a pool is actually bound to, from its stored
    /// session row, or `None` while the pool is unbound. Replacements and
    /// resumes inherit this, never the reference frozen at the original start.
    pub fn pool_binding(&self, pool_id: &PoolId) -> Result<Option<OrchestratorRef>> {
        Ok(self
            .conn
            .query_row(
                "SELECT s.transport,s.external_session_id,s.external_turn_id FROM pools p \
                 JOIN orchestrator_sessions s ON s.id=p.orchestrator_session_id WHERE p.id=?",
                [pool_id.as_str()],
                |row| {
                    Ok(OrchestratorRef {
                        transport: row.get(0)?,
                        external_session_id: row.get(1)?,
                        external_turn_id: row.get(2)?,
                    })
                },
            )
            .optional()?)
    }

    /// The actual shared binding of the pool in which `root` holds a seat.
    pub fn member_pool_binding(&self, root: &AgentId) -> Result<Option<OrchestratorRef>> {
        let pool: Option<String> = self
            .conn
            .query_row(
                "SELECT pool_id FROM pool_members WHERE agent_id=?",
                [root.as_str()],
                |row| row.get(0),
            )
            .optional()?;
        match pool {
            Some(pool) => self.pool_binding(&pool.parse()?),
            None => Ok(None),
        }
    }

    /// Returns an existing replacement for the same scoped key: its original
    /// result when the normalized request digest matches, `Conflict` when it
    /// does not, `None` when the key is unused.
    pub fn replay_pool_replacement(
        &self,
        pool_id: &PoolId,
        request_id: &str,
        request_sha256: &str,
    ) -> Result<Option<std::result::Result<PoolReplacement, PoolDenial>>> {
        replayed(&self.conn, pool_id, request_id, request_sha256)
    }

    /// Reads the pool and the seat being replaced, refusing with a typed
    /// reason when the pool or member does not qualify. This is advisory
    /// preparation; [`Self::replace_pool_member`] decides again atomically.
    pub fn pool_replace_context(
        &self,
        pool_id: &PoolId,
        old: &AgentId,
    ) -> Result<std::result::Result<PoolReplaceContext, PoolDenial>> {
        if let Some(denied) = refusal(&self.conn, pool_id.as_str(), old)? {
            return Ok(Err(denied));
        }
        let (goal, acceptance, roster_revision): (String, String, u32) = self.conn.query_row(
            "SELECT goal,acceptance_json,roster_revision FROM pools WHERE id=?",
            [pool_id.as_str()],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )?;
        let mut statement = self.conn.prepare(
            "SELECT agent_id FROM pool_members WHERE pool_id=? AND replaced_by IS NULL ORDER BY slot",
        )?;
        let ids = statement
            .query_map([pool_id.as_str()], |row| row.get::<_, String>(0))?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        drop(statement);
        let seats = ids
            .iter()
            .map(|id| record_of(&self.conn, pool_id.as_str(), id))
            .collect::<Result<Vec<_>>>()?;
        let personal_task: String = self.conn.query_row(
            "SELECT personal_task FROM pool_members WHERE pool_id=? AND agent_id=?",
            params![pool_id.as_str(), old.as_str()],
            |row| row.get(0),
        )?;
        let last_seq: i64 = self.conn.query_row(
            "SELECT COALESCE(MAX(seq),0) FROM pool_entries WHERE pool_id=?",
            [pool_id.as_str()],
            |row| row.get(0),
        )?;
        let current_proposal: Option<i64> = self.conn.query_row(
            "SELECT MAX(seq) FROM pool_entries WHERE pool_id=? AND kind='proposal'",
            [pool_id.as_str()],
            |row| row.get(0),
        )?;
        Ok(Ok(PoolReplaceContext {
            pool_id: pool_id.clone(),
            goal,
            acceptance: serde_json::from_str(&acceptance)?,
            roster_revision,
            old: seats
                .iter()
                .find(|seat| &seat.agent_id == old)
                .cloned()
                .ok_or_else(|| Error::Integrity("replaced seat missing from roster".into()))?,
            seats,
            personal_task,
            last_seq: last_seq.max(0) as u64,
            current_proposal: current_proposal.map(|seq| seq.max(0) as u64),
        }))
    }

    /// Replaces one current member in a single immediate transaction, or
    /// changes nothing.
    ///
    /// Inside the transaction it replays the same scoped request, then
    /// rechecks that the pool is open at the roster revision the task was
    /// composed against, that `old` is still a current member and that its
    /// latest execution is terminal with its whole lineage cleaned up. A new
    /// name may reuse the old seat's name, but must not match another current
    /// seat under Rust's Unicode lowercase comparison. It then
    /// admits the new agent through the ordinary per-agent admission,
    /// retires the old seat, installs the new one in the same slot, bumps the
    /// roster revision, appends one broker roster entry, fans it out to the
    /// other current members and records the request digest as an event on
    /// the new agent. Votes of the old revision stop counting by derivation.
    /// The quota capacity revision advances once. Nothing is launched.
    pub fn replace_pool_member(
        &mut self,
        input: PoolReplaceInput<'_>,
    ) -> Result<std::result::Result<PoolReplacement, PoolDenial>> {
        let tx = self
            .conn
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        if let Some(found) = replayed(&tx, input.pool_id, input.request_id, input.request_sha256)? {
            tx.commit()?;
            return Ok(found);
        }
        let pool = input.pool_id.as_str();
        if let Some(denied) = refusal(&tx, pool, input.old)? {
            return Ok(Err(denied));
        }
        let current: u32 = tx.query_row(
            "SELECT roster_revision FROM pools WHERE id=?",
            [pool],
            |row| row.get(0),
        )?;
        if current != input.expected_roster_revision {
            return Ok(Err(PoolDenial::StaleRoster { current }));
        }
        let taken = Self::current_name_taken(&tx, pool, input.old, input.name)?;
        if taken {
            return Err(Error::Validation("member names must be unique".into()));
        }
        admit_in_tx(
            &tx,
            input.catalog,
            input.new_id.clone(),
            &input.inputs,
            None,
        )?;
        let old = record_of(&tx, pool, input.old.as_str())?;
        let revision = current + 1;
        tx.execute(
            "UPDATE pool_members SET replaced_by=? WHERE pool_id=? AND agent_id=?",
            params![input.new_id.as_str(), pool, input.old.as_str()],
        )?;
        tx.execute(
            "INSERT INTO pool_members(agent_id,pool_id,slot,name,role,personal_task,joined_roster_revision) \
             VALUES(?,?,?,?,?,?,?)",
            params![
                input.new_id.as_str(),
                pool,
                old.slot,
                input.name,
                old.role,
                input.personal_task,
                revision
            ],
        )?;
        tx.execute(
            "UPDATE pools SET roster_revision=? WHERE id=?",
            params![revision, pool],
        )?;
        let body = format!(
            "Slot {} changed: {} ({}) now runs as {}, replacing {}. Roster revision {}; votes cast \
             before this change no longer count. Re-read the log with pool_read.",
            old.slot, input.name, old.role, input.new_id, old.agent_id, revision
        );
        tx.execute(
            "INSERT INTO pool_entries(pool_id,author_kind,direction,kind,roster_revision,body,\
             idem_scope,request_id,created_at) VALUES(?,'broker','team','roster',?,?,'replace',?,?)",
            params![pool, revision, body, input.request_id, now()],
        )?;
        let seq = tx.last_insert_rowid().max(0) as u64;
        fanout_entry(&tx, pool, input.new_id.as_str(), seq)?;
        tx_event(
            &tx,
            &input.new_id,
            "pool_member_replaced",
            None,
            None,
            &json!({"pool_id": pool, "request_id": input.request_id,
                    "request_sha256": input.request_sha256,
                    "replaces": old.agent_id, "roster_revision": revision, "seq": seq}),
        )?;
        Store::advance_quota_capacity_revision(&tx)?;
        let new = record_of(&tx, pool, input.new_id.as_str())?;
        tx.commit()?;
        Ok(Ok(PoolReplacement {
            pool_id: input.pool_id.clone(),
            old,
            new,
            roster_revision: revision,
            created: true,
        }))
    }
}
