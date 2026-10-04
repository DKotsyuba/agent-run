//! Verified common completion of a pool.
//!
//! A pool completes only when, at one consistent moment inside one immediate
//! transaction, the current proposal carries a valid ready vote from every
//! current member for the current roster revision, every current member's
//! latest execution succeeded, and every attempt of every member lineage has
//! verified cleanup. The decision is formal: agent-run checks the votes, the
//! proofs and the cleanup, never whether the result is semantically right.
//! Completion freezes one immutable event and one linked outbox row, so later
//! resumes or replacements never change or repeat it.

use crate::{
    pool_log::{
        criteria_of, current_proposal, latest_vote, lineage_cleanup_complete, tip_of, vote_validity,
    },
    tx_event, Store,
};
use agent_run_domain::{
    domain::{now, AgentId, OrchestratorRef},
    pool::PoolId,
    Error, Result,
};
use rusqlite::{params, Connection, OptionalExtension, Transaction, TransactionBehavior};
use serde_json::{json, Value};

/// Most UTF-8 bytes of the frozen common notice text.
pub const NOTICE_BYTES: usize = 4096;
/// Most UTF-8 bytes of the frozen completion event document.
const EVENT_BYTES: usize = 256 * 1024;

/// A committed pool completion, new or already recorded.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PoolCompletion {
    /// Pool identity.
    pub pool_id: PoolId,
    /// The one linked notification identity.
    pub delivery_id: String,
    /// Whether an orchestrator binding already existed (the notice is
    /// deliverable) or the notice waits for a pool binding.
    pub bound: bool,
    /// False when the pool had already completed.
    pub created: bool,
}

/// Cuts `text` to at most `max` bytes on a character boundary.
fn cut(text: &str, max: usize) -> &str {
    let mut end = max.min(text.len());
    while !text.is_char_boundary(end) {
        end -= 1;
    }
    &text[..end]
}

/// Builds the frozen, compact English notice for the whole pool. The result
/// is shortened inside [`NOTICE_BYTES`] with an explicit marker naming where
/// the full text stays; no run, attempt, session or credential id appears.
fn notice_text(
    pool_id: &str,
    goal: &str,
    roster: &[(String, String, String)],
    proposal: u64,
    revision: u32,
    snapshot: &str,
) -> String {
    let members = roster
        .iter()
        .map(|(id, name, role)| format!("{name} ({role}, {id})"))
        .collect::<Vec<_>>()
        .join("; ");
    let excerpt = cut(goal, 512);
    let goal_text = if excerpt.len() < goal.len() {
        format!("{excerpt} [goal excerpt; the full goal is in the pool record]")
    } else {
        excerpt.to_owned()
    };
    let head = format!(
        "Pool {pool_id} is complete: every current member voted ready on the same proposal, \
         finished successfully and was cleaned up. Only these formal checks were verified; \
         whether the result is right is for the orchestrator to judge.\nGoal: {goal_text}\nMembers: \
         {members}\nAccepted result (proposal #{proposal}, roster revision {revision}):\n"
    );
    let marker =
        format!("\n[truncated; the full result is proposal entry #{proposal} in the pool log]");
    if head.len() + snapshot.len() <= NOTICE_BYTES {
        return format!("{head}{snapshot}");
    }
    let room = NOTICE_BYTES.saturating_sub(head.len() + marker.len());
    format!("{head}{}{marker}", cut(snapshot, room))
}

/// Everything a verified completion records, read from one consistent snapshot.
struct Ready {
    goal: String,
    revision: u32,
    criteria: Vec<(String, String)>,
    proposal: u64,
    snapshot: String,
    members: Vec<Value>,
    tips: Vec<Value>,
    roster: Vec<(String, String, String)>,
    anchor: AgentId,
    session: Option<String>,
}

/// What one read of a pool shows.
enum Evaluation {
    /// No such pool.
    Missing,
    /// Already completed, with its recorded notice.
    Completed(PoolCompletion),
    /// At least one condition is missing.
    NotReady,
    /// Every condition holds on this snapshot.
    Ready(Box<Ready>),
}

/// Checks every completion condition against one snapshot without writing.
///
/// Used read-only as the cheap preflight of the maintenance sweep and again,
/// inside the immediate transaction, as the deciding read.
fn evaluate(tx: &Connection, pool_id: &PoolId) -> Result<Evaluation> {
    let pool = pool_id.as_str();
    #[allow(clippy::type_complexity)]
    let row: Option<(String, String, u32, String, Option<String>, Option<String>)> = tx
        .query_row(
            "SELECT state,goal,roster_revision,acceptance_json,orchestrator_session_id,completion_delivery_id \
             FROM pools WHERE id=?",
            [pool],
            |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?, r.get(4)?, r.get(5)?)),
        )
        .optional()?;
    let Some((state, goal, revision, acceptance, session, delivery)) = row else {
        return Ok(Evaluation::Missing);
    };
    if state == "completed" {
        return Ok(match delivery {
            Some(delivery_id) => Evaluation::Completed(PoolCompletion {
                pool_id: pool_id.clone(),
                delivery_id,
                bound: session.is_some(),
                created: false,
            }),
            None => Evaluation::NotReady,
        });
    }
    let Some((proposal, snapshot, proposal_revision)) = current_proposal(tx, pool)? else {
        return Ok(Evaluation::NotReady);
    };
    if proposal_revision != revision {
        return Ok(Evaluation::NotReady);
    }
    let criteria = criteria_of(&acceptance)?;
    let mut seats = tx.prepare(
        "SELECT agent_id,slot,name,role FROM pool_members \
         WHERE pool_id=? AND replaced_by IS NULL ORDER BY slot",
    )?;
    let seats = seats
        .query_map([pool], |r| {
            Ok((
                r.get::<_, String>(0)?,
                r.get::<_, i64>(1)?,
                r.get::<_, String>(2)?,
                r.get::<_, String>(3)?,
            ))
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    if seats.len() < 2 {
        return Ok(Evaluation::NotReady);
    }
    let mut members = Vec::new();
    let mut tips = Vec::new();
    let mut roster = Vec::new();
    for (seat, slot, name, role) in &seats {
        let root: AgentId = seat.parse()?;
        let tip = tip_of(tx, &root)?;
        let tip_status: String = tx.query_row(
            "SELECT status FROM agents WHERE id=?",
            [tip.as_str()],
            |r| r.get(0),
        )?;
        let latest = latest_vote(tx, pool, seat, proposal)?;
        let checks = latest.as_ref().and_then(|(_, _, checks, _)| checks.clone());
        let (_, why) = vote_validity(latest, revision, proposal_revision, &tip, &tip_status);
        // Every criterion must be covered once and met, not merely the ones listed.
        let covered: Option<Vec<Value>> = checks
            .as_deref()
            .and_then(|raw| serde_json::from_str(raw).ok());
        let exact = covered.as_ref().is_some_and(|items| {
            let mut ids: Vec<&str> = items
                .iter()
                .filter_map(|c| c["criterion_id"].as_str())
                .collect();
            ids.sort_unstable();
            let mut expected: Vec<&str> = criteria.iter().map(|(id, _)| id.as_str()).collect();
            expected.sort_unstable();
            ids == expected && items.iter().all(|c| c["status"] == json!("met"))
        });
        if why != "valid"
            || tip_status != "succeeded"
            || !exact
            || !lineage_cleanup_complete(tx, &root)?
        {
            return Ok(Evaluation::NotReady);
        }
        let vote_seq: i64 = tx.query_row(
            "SELECT seq FROM pool_entries WHERE pool_id=? AND author_agent_id=? AND proposal_seq=? \
             AND kind IN ('vote','revoke') ORDER BY seq DESC LIMIT 1",
            params![pool, seat, proposal as i64],
            |r| r.get(0),
        )?;
        members.push(
            json!({"agent_id": seat, "slot": slot, "name": name, "role": role,
                            "vote_seq": vote_seq, "checks": covered}),
        );
        tips.push(json!({"agent_id": seat, "tip": tip, "status": tip_status}));
        roster.push((seat.clone(), name.clone(), role.clone()));
    }
    let anchor: AgentId = tips[0]["tip"].as_str().unwrap_or_default().parse()?;
    Ok(Evaluation::Ready(Box::new(Ready {
        goal,
        revision,
        criteria,
        proposal,
        snapshot,
        members,
        tips,
        roster,
        anchor,
        session,
    })))
}

/// Decides and writes the completion inside the caller's transaction.
///
/// Returns `None` while any condition is missing: nothing is written and the
/// pool stays open (derived draining or agreed-but-running are not states).
fn settle_in_tx(tx: &Transaction<'_>, pool_id: &PoolId) -> Result<Option<PoolCompletion>> {
    let pool = pool_id.as_str();
    let ready = match evaluate(tx, pool_id)? {
        Evaluation::Missing | Evaluation::NotReady => return Ok(None),
        Evaluation::Completed(done) => return Ok(Some(done)),
        Evaluation::Ready(ready) => *ready,
    };
    let Ready {
        goal,
        revision,
        criteria,
        proposal,
        snapshot,
        members,
        tips,
        roster,
        anchor,
        session,
    } = ready;
    let notice = notice_text(
        pool,
        &goal,
        &roster
            .iter()
            .map(|(id, n, r)| (id.clone(), n.clone(), r.clone()))
            .collect::<Vec<_>>(),
        proposal,
        revision,
        &snapshot,
    );
    let data = json!({
        "pool_id": pool, "roster_revision": revision, "goal": goal,
        "acceptance": criteria.iter().map(|(id, text)| json!({"id": id, "text": text})).collect::<Vec<_>>(),
        "proposal": {"seq": proposal, "snapshot": snapshot},
        "members": members,
        "proofs": {"tips": tips, "cleanup_complete": true, "formal_checks_only": true},
        "notice": notice,
    });
    if data.to_string().len() > EVENT_BYTES {
        return Err(Error::Integrity(
            "pool completion record exceeds its bound".into(),
        ));
    }
    let event = tx_event(tx, &anchor, "pool_completed", None, None, &data)?;
    let id = format!("ntf_{}", uuid::Uuid::new_v4().simple());
    let at = now();
    let (delivery_state, next) = if session.is_some() {
        ("pending", Some(at))
    } else {
        ("waiting_binding", None)
    };
    tx.execute(
        "INSERT INTO deliveries(id,agent_id,orchestrator_session_id,terminal_event_seq,state,next_attempt_at) \
         VALUES(?,?,?,?,?,?)",
        params![id, anchor.as_str(), session, event, delivery_state, next],
    )?;
    let changed = tx.execute(
        "UPDATE pools SET state='completed',completed_at=?,completion_delivery_id=? \
         WHERE id=? AND state='open'",
        params![at, id, pool],
    )?;
    if changed != 1 {
        return Err(Error::Conflict);
    }
    Ok(Some(PoolCompletion {
        pool_id: pool_id.clone(),
        delivery_id: id,
        bound: session.is_some(),
        created: true,
    }))
}

impl Store {
    /// Completes `pool_id` when every condition holds, in one immediate
    /// transaction. Repeats and concurrent callers record at most one event
    /// and one delivery; `None` means the pool is not (yet) complete.
    pub fn settle_pool(&mut self, pool_id: &PoolId) -> Result<Option<PoolCompletion>> {
        // Read-only preflight: pools that are running, unproven or unvoted
        // never take the writer lock. The deciding read repeats inside the
        // immediate transaction, so a race can only skip, never mis-complete.
        match evaluate(&self.conn, pool_id)? {
            Evaluation::Missing | Evaluation::NotReady => return Ok(None),
            Evaluation::Completed(done) => return Ok(Some(done)),
            Evaluation::Ready(_) => {}
        }
        let tx = self
            .conn
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let result = settle_in_tx(&tx, pool_id)?;
        tx.commit()?;
        Ok(result)
    }

    /// Settles the open pool, if any, in which `agent`'s lineage holds a
    /// current seat; called after that execution reached a terminal state.
    pub fn settle_pool_of(&mut self, agent: &AgentId) -> Result<Option<PoolCompletion>> {
        let pool: Option<String> = self
            .conn
            .query_row(
                "SELECT m.pool_id FROM pool_members m JOIN pools p ON p.id=m.pool_id \
                 JOIN agents a ON m.agent_id=CASE WHEN a.root_agent_id='' THEN a.id ELSE a.root_agent_id END \
                 WHERE a.id=? AND m.replaced_by IS NULL AND p.state='open'",
                [agent.as_str()],
                |r| r.get(0),
            )
            .optional()?;
        match pool {
            Some(pool) => self.settle_pool(&pool.parse()?),
            None => Ok(None),
        }
    }

    /// Settles up to `limit` open pools that already hold a proposal, as the
    /// bounded maintenance sweep that converges crashes, reconciled losses
    /// and cleanup proof that arrived after the terminal write. `seed`
    /// rotates the window each call so no pool starves behind the first
    /// `limit` open ones.
    // ponytail: one count plus one windowed page per tick; an indexed
    // "possibly settleable" marker would avoid it if open pools reach thousands.
    pub fn settle_open_pools(&mut self, limit: usize, seed: usize) -> Result<usize> {
        const OPEN: &str = "state='open' AND EXISTS(SELECT 1 FROM pool_entries e \
                            WHERE e.pool_id=p.id AND e.kind='proposal')";
        let total: i64 = self.conn.query_row(
            &format!("SELECT COUNT(*) FROM pools p WHERE {OPEN}"),
            [],
            |r| r.get(0),
        )?;
        if total == 0 {
            return Ok(0);
        }
        let offset = seed.wrapping_mul(limit) % total as usize;
        let mut statement = self.conn.prepare(&format!(
            "SELECT id FROM pools p WHERE {OPEN} ORDER BY id LIMIT ? OFFSET ?"
        ))?;
        let ids = statement
            .query_map(params![limit as i64, offset as i64], |r| {
                r.get::<_, String>(0)
            })?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        drop(statement);
        let mut completed = 0;
        for id in ids {
            let pool: PoolId = id.parse()?;
            if self.settle_pool(&pool)?.is_some_and(|c| c.created) {
                completed += 1;
            }
        }
        Ok(completed)
    }

    /// Binds a pool and every current member tip to one orchestrator session
    /// and activates the pool's waiting common notice exactly once. An
    /// existing different binding is refused; repeating the same one is a no-op.
    pub fn bind_pool(
        &mut self,
        pool_id: &PoolId,
        reference: &OrchestratorRef,
        at: f64,
    ) -> Result<String> {
        reference.validate()?;
        let tx = self
            .conn
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let current: Option<Option<String>> = tx
            .query_row(
                "SELECT orchestrator_session_id FROM pools WHERE id=?",
                [pool_id.as_str()],
                |r| r.get(0),
            )
            .optional()?;
        let Some(current) = current else {
            return Err(Error::NotFound(pool_id.to_string()));
        };
        if let Some(existing) = &current {
            if !crate::session_matches_reference(&tx, existing, reference)? {
                return Err(Error::Validation(
                    "pool orchestration binding is immutable".into(),
                ));
            }
        }
        let seats = seat_tips(&tx, pool_id.as_str())?;
        let mut bound_sessions = Vec::new();
        for tip in &seats {
            let bound: Option<String> = tx.query_row(
                "SELECT orchestrator_session_id FROM agents WHERE id=?",
                [tip.as_str()],
                |row| row.get(0),
            )?;
            if let Some(existing) = bound {
                if !crate::session_matches_reference(&tx, &existing, reference)? {
                    return Err(Error::Validation(
                        "a pool member is bound to a different orchestrator".into(),
                    ));
                }
                bound_sessions.push(existing);
            }
        }
        let session = match &current {
            Some(existing) => existing.clone(),
            None => crate::session_for_reference(&tx, reference, at)?,
        };
        if current.is_some() {
            crate::touch_session_reference(&tx, &session, reference, at)?;
        }
        for existing in bound_sessions {
            if existing != session {
                crate::touch_session_reference(&tx, &existing, reference, at)?;
            }
        }
        if current.is_none() {
            tx.execute(
                "UPDATE pools SET orchestrator_session_id=? WHERE id=?",
                params![session, pool_id.as_str()],
            )?;
        }
        for tip in seats {
            let bound: Option<String> = tx.query_row(
                "SELECT orchestrator_session_id FROM agents WHERE id=?",
                [tip.as_str()],
                |row| row.get(0),
            )?;
            if bound.is_none() {
                tx.execute(
                    "UPDATE agents SET orchestrator_session_id=? WHERE id=?",
                    params![session, tip.as_str()],
                )?;
            }
        }
        tx.execute(
            "UPDATE deliveries SET orchestrator_session_id=COALESCE(orchestrator_session_id,?),state='pending',next_attempt_at=? \
             WHERE state='waiting_binding' AND id=(SELECT completion_delivery_id FROM pools WHERE id=?)",
            params![session, at, pool_id.as_str()],
        )?;
        tx.commit()?;
        Ok(session)
    }
}

/// The latest execution of every current member of `pool`.
fn seat_tips(conn: &Connection, pool: &str) -> Result<Vec<AgentId>> {
    let mut statement = conn.prepare(
        "SELECT agent_id FROM pool_members WHERE pool_id=? AND replaced_by IS NULL ORDER BY slot",
    )?;
    let seats = statement
        .query_map([pool], |r| r.get::<_, String>(0))?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    seats
        .iter()
        .map(|seat| tip_of(conn, &seat.parse()?))
        .collect()
}
