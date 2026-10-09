//! Authenticated pool writes, the durable shared log, and derived status.
//!
//! Every write authenticates the hidden worker capability, resolves current
//! membership from the stable lineage root, and stamps the author from
//! durable rows inside the same transaction — a caller can never supply an
//! author or a pool. Entries are append-only; vote validity is derived at
//! read time, so invalidating events (new proposal, roster change, resume,
//! failure) never rewrite history.

use crate::worker::authenticate_attempt;
use crate::{Store, tx_event};
use agent_run_domain::domain::{AgentId, now};
use agent_run_domain::pool::{
    AuthorKind, Direction, EntryKind, PoolDenial, PoolEntryView, PoolId, PoolMessage, PoolPropose,
    PoolVote, VoteDecision,
};
use agent_run_domain::worker::WorkerMessageKind;
use agent_run_domain::{Error, Result};
use rusqlite::{Connection, OptionalExtension, Transaction, TransactionBehavior, params};
use serde_json::{Value, json};

/// Maximum ordinary chat rows per pool before the budget refusal.
pub const CHAT_ROW_BUDGET: i64 = 200;
/// Maximum ordinary chat bytes per pool before the budget refusal.
pub const CHAT_BYTE_BUDGET: i64 = 256 * 1024;
/// Maximum proposals per pool.
pub const PROPOSAL_BUDGET: i64 = 20;
/// Vote rows one member may write per proposal; the last only accepts a block.
pub const VOTE_ROW_BUDGET: i64 = 8;

/// One current seat plus the pool facts a write must see.
struct Membership {
    /// Stable seat identity (the lineage root admitted to the pool).
    seat_agent_id: String,
    /// Pool identity.
    pool_id: String,
    /// Slot label.
    name: String,
    /// Role label.
    role: String,
    /// Pool's current roster revision.
    roster_revision: u32,
    /// Pool state.
    state: String,
    /// Acceptance criteria as `(id, text)` pairs.
    acceptance: Vec<(String, String)>,
}

/// Reads the current (unreplaced) seat of `root`, with its pool's state and
/// acceptance criteria; `None` when the lineage holds no current seat.
fn membership(conn: &Connection, root: &AgentId) -> Result<Option<Membership>> {
    let row: Option<(String, String, String, String, u32, String, String)> = conn
        .query_row(
            "SELECT m.agent_id,p.id,m.name,m.role,p.roster_revision,p.state,p.acceptance_json \
             FROM pool_members m JOIN pools p ON p.id=m.pool_id \
             WHERE m.agent_id=? AND m.replaced_by IS NULL",
            [root.as_str()],
            |row| {
                Ok((
                    row.get(0)?,
                    row.get(1)?,
                    row.get(2)?,
                    row.get(3)?,
                    row.get(4)?,
                    row.get(5)?,
                    row.get(6)?,
                ))
            },
        )
        .optional()?;
    let Some((seat_agent_id, pool_id, name, role, roster_revision, state, acceptance)) = row else {
        return Ok(None);
    };
    let criteria = criteria_of(&acceptance)?;
    Ok(Some(Membership {
        seat_agent_id,
        pool_id,
        name,
        role,
        roster_revision,
        state,
        acceptance: criteria,
    }))
}

/// Decodes stored acceptance criteria into `(id, text)` pairs.
pub(crate) fn criteria_of(acceptance: &str) -> Result<Vec<(String, String)>> {
    let parsed: Value = serde_json::from_str(acceptance)
        .map_err(|_| Error::Integrity("pool acceptance is malformed".into()))?;
    parsed
        .as_array()
        .ok_or_else(|| Error::Integrity("pool acceptance is not an array".into()))?
        .iter()
        .map(|criterion| {
            Ok((
                criterion["id"]
                    .as_str()
                    .ok_or_else(|| Error::Integrity("criterion has no id".into()))?
                    .to_owned(),
                criterion["text"].as_str().unwrap_or_default().to_owned(),
            ))
        })
        .collect()
}

/// The current proposal of `pool_id`, when one exists.
pub(crate) fn current_proposal(
    conn: &Connection,
    pool_id: &str,
) -> Result<Option<(u64, String, u32)>> {
    Ok(conn
        .query_row(
            "SELECT seq,snapshot,roster_revision FROM pool_entries \
             WHERE pool_id=? AND kind='proposal' ORDER BY seq DESC LIMIT 1",
            [pool_id],
            |row| {
                Ok((
                    row.get::<_, i64>(0)?.max(0) as u64,
                    row.get(1)?,
                    row.get(2)?,
                ))
            },
        )
        .optional()?)
}

/// The member's latest vote-shaped entry on `proposal`, if any.
#[allow(clippy::type_complexity)]
pub(crate) fn latest_vote(
    conn: &Connection,
    pool_id: &str,
    member: &str,
    proposal: u64,
) -> Result<Option<(String, Option<String>, Option<String>, String)>> {
    Ok(conn
        .query_row(
            "SELECT kind,decision,checks_json,sender_run_id FROM pool_entries \
             WHERE pool_id=? AND author_agent_id=? AND proposal_seq=? \
               AND kind IN ('vote','revoke') ORDER BY seq DESC LIMIT 1",
            params![pool_id, member, proposal as i64],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
        )
        .optional()?)
}

/// Whether every attempt of every execution in `root`'s lineage has verified
/// cleanup proof: the same predicate explicit resume demands of its parent.
pub(crate) fn lineage_cleanup_complete(conn: &Connection, root: &AgentId) -> Result<bool> {
    let uncleaned: i64 = conn.query_row(
        "SELECT COUNT(*) FROM attempts WHERE agent_id IN \
         (SELECT id FROM agents WHERE root_agent_id=?1 OR id=?1) \
         AND (cleanup_proof_json IS NULL OR phase!='cleanup_complete')",
        [root.as_str()],
        |row| row.get(0),
    )?;
    Ok(uncleaned == 0)
}

/// The lineage tip execution id of `root`, matching the public resolver.
pub(crate) fn tip_of(conn: &Connection, root: &AgentId) -> Result<AgentId> {
    let id: String = conn.query_row(
        "SELECT id FROM agents WHERE root_agent_id=?1 OR id=?1 \
         ORDER BY sequence DESC,created_at DESC,id DESC LIMIT 1",
        [root.as_str()],
        |row| row.get(0),
    )?;
    id.parse()
}

/// One pool write as received by the store.
pub enum PoolWrite {
    /// Ordinary informational chat.
    Message(PoolMessage),
    /// A result proposal with its immutable snapshot.
    Proposal(PoolPropose),
    /// A readiness, objection, or withdrawal on the current proposal.
    Vote(PoolVote),
}

impl PoolWrite {
    /// The entry kind this write produces.
    fn kind(&self) -> EntryKind {
        match self {
            Self::Message(_) => EntryKind::Message,
            Self::Proposal(_) => EntryKind::Proposal,
            Self::Vote(vote) => match vote.decision {
                VoteDecision::Revoke => EntryKind::Revoke,
                _ => EntryKind::Vote,
            },
        }
    }

    /// The idempotency request key of the wrapped input.
    fn request_id(&self) -> &str {
        match self {
            Self::Message(input) => &input.request_id,
            Self::Proposal(input) => &input.request_id,
            Self::Vote(input) => &input.request_id,
        }
    }

    /// Validates the wrapped input against its domain contract.
    fn validate(&self) -> Result<()> {
        match self {
            Self::Message(input) => input.validate(),
            Self::Proposal(input) => input.validate(),
            Self::Vote(input) => input.validate(),
        }
    }

    /// The durable fields compared for idempotent replay of the same key.
    fn replay_shape(&self) -> Vec<(&'static str, Value)> {
        match self {
            Self::Message(input) => vec![("body", json!(input.message))],
            Self::Proposal(input) => {
                vec![
                    ("body", json!(input.message)),
                    ("snapshot", json!(input.snapshot)),
                ]
            }
            Self::Vote(input) => vec![
                (
                    "decision",
                    json!(
                        (input.decision != VoteDecision::Revoke).then(|| input.decision.as_str())
                    ),
                ),
                ("proposal_seq", json!(input.proposal_seq)),
                (
                    "checks",
                    json!((!input.checks.is_empty()).then_some(&input.checks)),
                ),
                (
                    "body",
                    json!(input.message.as_deref().unwrap_or("(no note)")),
                ),
            ],
        }
    }
}

/// Durable receipt of one pool write.
#[derive(Debug, Clone, PartialEq)]
pub struct PoolWriteReceipt {
    /// Pool the entry landed in.
    pub pool_id: PoolId,
    /// Log position of the (possibly prior) entry.
    pub seq: u64,
    /// Whether this call replayed an existing entry.
    pub duplicate: bool,
}

/// An append outcome that is either a durable sequence or a typed refusal
/// plus the possibility of an ordinary store error.
type AppendResult = std::result::Result<std::result::Result<u64, PoolDenial>, Error>;

/// Rejects malformed hidden credentials before any database work.
fn input_credentials(run_id: &AgentId, attempt_id: &str, token: &str) -> Result<()> {
    if attempt_id.is_empty()
        || attempt_id.len() > 128
        || token.len() != 64
        || run_id.as_str().is_empty()
    {
        return Err(Error::Validation("invalid worker capability".into()));
    }
    Ok(())
}

/// Authenticates the capability and resolves the caller's current seat.
fn authenticated_member(
    conn: &Connection,
    run_id: &AgentId,
    attempt_id: &str,
    token: &str,
) -> Result<std::result::Result<Membership, PoolDenial>> {
    input_credentials(run_id, attempt_id, token)?;
    let Some(auth) = authenticate_attempt(conn, run_id, attempt_id, token, now())? else {
        return Err(Error::Validation("invalid worker capability".into()));
    };
    if !auth.running {
        return Err(Error::Validation("worker attempt is not running".into()));
    }
    match membership(conn, &auth.root_agent_id)? {
        Some(member) => Ok(Ok(member)),
        None => Ok(Err(PoolDenial::NotPoolMember)),
    }
}

impl Store {
    /// Appends one authenticated pool entry under the writer's stamped
    /// identity, or replays the identical request, or returns a typed denial.
    pub fn pool_write(
        &mut self,
        run_id: &AgentId,
        attempt_id: &str,
        token: &str,
        write: PoolWrite,
    ) -> Result<std::result::Result<PoolWriteReceipt, PoolDenial>> {
        let tx = self
            .conn
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let member = match authenticated_member(&tx, run_id, attempt_id, token)? {
            Ok(member) => member,
            Err(denial) => return Ok(Err(denial)),
        };
        crate::pool_enrollment::validate_ack_replay(
            &tx,
            &member.seat_agent_id.parse()?,
            run_id,
            attempt_id,
            write.request_id(),
            matches!(write, PoolWrite::Message(_)),
        )?;
        // Idempotency: the sender execution scope plus the request key. The
        // same content replays; different content under the key conflicts.
        // This precedes the closed-pool guard so a retry of a write that
        // committed before completion still returns its original receipt.
        let prior: Option<i64> = tx
            .query_row(
                "SELECT seq FROM pool_entries WHERE pool_id=? AND idem_scope=? AND request_id=?",
                params![member.pool_id, run_id.as_str(), write.request_id()],
                |row| row.get(0),
            )
            .optional()?;
        if let Some(seq) = prior {
            if replay_shape_of(&tx, &member.pool_id, seq)? == write.replay_shape() {
                let receipt = PoolWriteReceipt {
                    pool_id: member.pool_id.parse()?,
                    seq: seq.max(0) as u64,
                    duplicate: true,
                };
                tx.commit()?;
                return Ok(Ok(receipt));
            }
            return Ok(Err(PoolDenial::Conflict));
        }
        if member.state == "completed" {
            return Ok(Err(PoolDenial::PoolCompleted));
        }
        write.validate()?;
        let ack = crate::pool_enrollment::ack_allowed(
            &tx,
            &member.seat_agent_id.parse()?,
            run_id,
            attempt_id,
            if let PoolWrite::Message(input) = &write {
                Some(input.request_id.as_str())
            } else {
                None
            },
        )?;
        let seq = if ack {
            let PoolWrite::Message(input) = &write else {
                unreachable!("ACK is a message");
            };
            // One reserved bounded summary per attached seat cannot be starved
            // by ordinary chat volume; replay returns before this append.
            Ok(Ok(append_entry(
                &tx,
                &member,
                run_id,
                attempt_id,
                EntryKind::Message,
                None,
                None,
                None,
                None,
                None,
                &input.message,
                &input.request_id,
            )?))
        } else {
            match &write {
                PoolWrite::Message(input) => {
                    append_message(&tx, &member, run_id, attempt_id, input)
                }
                PoolWrite::Proposal(input) => {
                    append_proposal(&tx, &member, run_id, attempt_id, input)
                }
                PoolWrite::Vote(input) => append_vote(&tx, &member, run_id, attempt_id, input),
            }
        };
        let seq = match seq {
            Ok(Ok(seq)) => seq,
            Ok(Err(reason)) => return Ok(Err(reason)),
            Err(error) => return Err(error),
        };
        if ack {
            crate::pool_enrollment::acknowledge(&tx, &member.seat_agent_id.parse()?, seq)?;
        }
        // The durable event lands on the authenticated execution so followers
        // of the event revision also see pool progress.
        tx_event(
            &tx,
            run_id,
            "pool_entry_appended",
            None,
            None,
            &json!({"seq": seq, "kind": write.kind().as_str()}),
        )?;
        // Fan out in the same transaction: one pending `pool` command holding
        // only the sequence number for every current member's tip execution
        // except the sender. The log stays authoritative; a refused or
        // uncertain delivery never loses the entry, and replays of the same
        // key return before this point so nothing is enqueued twice.
        fanout_entry(&tx, &member.pool_id, &member.seat_agent_id, seq)?;
        tx.commit()?;
        if matches!(write, PoolWrite::Vote(_)) {
            // Convergence is also swept by maintenance, so a failure here is
            // never allowed to fail the already committed vote.
            let _ = self.settle_pool(&member.pool_id.parse()?);
        }
        Ok(Ok(PoolWriteReceipt {
            pool_id: member.pool_id.parse()?,
            seq,
            duplicate: false,
        }))
    }

    /// Reads one bounded page of the caller's pool plus the derived status.
    ///
    /// The page follows the immutable sequence cursor convention; the status
    /// is computed from current rows only and never exposes internal run or
    /// attempt identities.
    pub fn pool_read(
        &self,
        run_id: &AgentId,
        attempt_id: &str,
        token: &str,
        after_seq: u64,
        before_seq: Option<u64>,
        limit: u32,
    ) -> Result<std::result::Result<Value, PoolDenial>> {
        let member = match authenticated_member(&self.conn, run_id, attempt_id, token)? {
            Ok(member) => member,
            Err(_) => return Ok(Err(PoolDenial::NotPoolMemberRead)),
        };
        let mut page = read_page(&self.conn, &member.pool_id, after_seq, before_seq, limit)?;
        let challenge:Option<(String,String)> = self.conn.query_row(
            "SELECT challenge,state FROM pool_enrollments WHERE agent_id=? AND run_id=? AND attempt_id=?",
            params![member.seat_agent_id,run_id.as_str(),attempt_id],|r|Ok((r.get(0)?,r.get(1)?))).optional()?;
        if let Some((request, state)) = challenge {
            page["enrollment"] = json!({"state":state,"request_id":request});
        }
        Ok(Ok(page))
    }

    /// Lists validated compact pool summaries from one deferred read snapshot.
    /// Ordering is created_at descending, then id descending; total is exact for
    /// the optional state filter. Vote counts and completed members reuse the
    /// operator status projection. No log bodies escape and no rows are written.
    pub fn list_pools(
        &self,
        query: &agent_run_domain::pool::ListPoolsQuery,
    ) -> Result<agent_run_domain::views::ListPoolsView> {
        use agent_run_domain::views::{ListPoolsView, PoolListMemberView, PoolListView};
        query.validate()?;
        let tx = self.conn.unchecked_transaction()?;
        let state = query.state.map(|state| state.as_str());
        let total: u64 = tx.query_row(
            "SELECT COUNT(*) FROM pools WHERE (?1 IS NULL OR state=?1)",
            [state],
            |row| row.get(0),
        )?;
        let mut statement = tx.prepare(
            "SELECT id,created_at,completed_at,COALESCE(\
             (SELECT MAX(seq) FROM pool_entries WHERE pool_id=p.id),0) \
             FROM pools p WHERE (?1 IS NULL OR state=?1) \
             ORDER BY created_at DESC,id DESC LIMIT ?2 OFFSET ?3",
        )?;
        let rows = statement
            .query_map(
                params![state, query.limit as i64, query.offset as i64],
                |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, f64>(1)?,
                        row.get::<_, Option<f64>>(2)?,
                        row.get::<_, u64>(3)?,
                    ))
                },
            )?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        drop(statement);
        let mut items = Vec::with_capacity(rows.len());
        for (id, created_at, completed_at, last_seq) in rows {
            let status = pool_status(&tx, &id)?;
            let roster = status["members"]
                .as_array()
                .ok_or_else(|| Error::Integrity("pool roster is malformed".into()))?;
            let members = roster
                .iter()
                .map(|member| {
                    Ok(serde_json::from_value::<PoolListMemberView>(json!({
                        "slot": member["slot"], "name": member["name"], "role": member["role"],
                        "agent_id": member["agent_id"], "tip_status": member["tip_status"],
                    }))?)
                })
                .collect::<Result<Vec<_>>>()?;
            let goal = status["goal"]
                .as_str()
                .ok_or_else(|| Error::Integrity("pool goal is malformed".into()))?;
            let mut end = goal.len().min(512);
            while !goal.is_char_boundary(end) {
                end -= 1;
            }
            items.push(PoolListView {
                pool_id: id.parse()?,
                state: serde_json::from_value(status["state"].clone())?,
                goal: goal[..end].to_owned(),
                goal_truncated: end < goal.len(),
                created_at,
                completed_at,
                last_seq,
                roster_revision: serde_json::from_value(status["roster_revision"].clone())?,
                members_count: members.len(),
                ready: roster
                    .iter()
                    .filter(|member| member["counts"] == json!(true))
                    .count(),
                current_proposal_seq: status["current_proposal"]["seq"].as_u64(),
                members,
            });
        }
        let next = query.offset.saturating_add(items.len());
        tx.commit()?;
        Ok(ListPoolsView {
            items,
            total,
            offset: query.offset,
            limit: query.limit,
            next_offset: (next < total as usize).then_some(next),
            complete: next >= total as usize,
        })
    }

    /// Reads one bounded page of any pool plus the derived status for the
    /// operator; `None` when no pool has the identity. The page and status
    /// come from the same projection members read, so nothing is aggregated
    /// twice and no internal run or attempt identity is exposed.
    pub fn pool_operator_read(
        &self,
        pool_id: &PoolId,
        after_seq: u64,
        before_seq: Option<u64>,
        limit: u32,
    ) -> Result<std::result::Result<Value, PoolDenial>> {
        if !pool_exists(&self.conn, pool_id.as_str())? {
            return Ok(Err(PoolDenial::PoolNotFound));
        }
        read_page(&self.conn, pool_id.as_str(), after_seq, before_seq, limit).map(Ok)
    }

    /// Appends one operator message to the pool under a broker-stamped
    /// operator author and fans it out to every current member's tip.
    ///
    /// The author is never taken from the caller. The same chat budget and
    /// body bounds as member chat apply; the key is scoped `op`, so the same
    /// body replays the original sequence and a different body is `Conflict`.
    pub fn pool_operator_post(
        &mut self,
        pool_id: &PoolId,
        request_id: &str,
        message: &str,
    ) -> Result<std::result::Result<PoolWriteReceipt, PoolDenial>> {
        let tx = self
            .conn
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let Some((state, roster_revision)) = tx
            .query_row(
                "SELECT state,roster_revision FROM pools WHERE id=?",
                [pool_id.as_str()],
                |row| Ok((row.get::<_, String>(0)?, row.get::<_, u32>(1)?)),
            )
            .optional()?
        else {
            return Ok(Err(PoolDenial::PoolNotFound));
        };
        let prior: Option<i64> = tx
            .query_row(
                "SELECT seq FROM pool_entries WHERE pool_id=? AND idem_scope='op' AND request_id=?",
                params![pool_id.as_str(), request_id],
                |row| row.get(0),
            )
            .optional()?;
        if let Some(seq) = prior {
            if replay_shape_of(&tx, pool_id.as_str(), seq)? == vec![("body", json!(message))] {
                return Ok(Ok(PoolWriteReceipt {
                    pool_id: pool_id.clone(),
                    seq: seq.max(0) as u64,
                    duplicate: true,
                }));
            }
            return Ok(Err(PoolDenial::Conflict));
        }
        if state == "completed" {
            return Ok(Err(PoolDenial::PoolCompleted));
        }
        let (rows, bytes): (i64, i64) = tx.query_row(
            "SELECT COUNT(*),COALESCE(SUM(length(CAST(body AS BLOB))),0) FROM pool_entries \
             WHERE pool_id=? AND kind='message'",
            [pool_id.as_str()],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )?;
        if rows >= CHAT_ROW_BUDGET || bytes + message.len() as i64 > CHAT_BYTE_BUDGET {
            return Ok(Err(PoolDenial::ChatBudgetExhausted));
        }
        tx.execute(
            "INSERT INTO pool_entries(pool_id,author_kind,direction,kind,roster_revision,body,\
             idem_scope,request_id,created_at) VALUES(?,'operator','team','message',?,?,'op',?,?)",
            params![
                pool_id.as_str(),
                roster_revision,
                message,
                request_id,
                now()
            ],
        )?;
        let seq = tx.last_insert_rowid().max(0) as u64;
        fanout_entry(&tx, pool_id.as_str(), "", seq)?;
        tx.commit()?;
        Ok(Ok(PoolWriteReceipt {
            pool_id: pool_id.clone(),
            seq,
            duplicate: false,
        }))
    }

    /// Cheap probe for the read long-poll loop: any entry beyond `after_seq`?
    pub fn pool_has_entries_after(
        &self,
        run_id: &AgentId,
        attempt_id: &str,
        token: &str,
        after_seq: u64,
    ) -> Result<std::result::Result<bool, PoolDenial>> {
        let member = match authenticated_member(&self.conn, run_id, attempt_id, token)? {
            Ok(member) => member,
            Err(_) => return Ok(Err(PoolDenial::NotPoolMemberRead)),
        };
        let exists: bool = self.conn.query_row(
            "SELECT EXISTS(SELECT 1 FROM pool_entries WHERE pool_id=? AND seq>?)",
            params![member.pool_id, after_seq as i64],
            |row| row.get(0),
        )?;
        Ok(Ok(exists))
    }
}

/// Whether a pool with this identity exists.
fn pool_exists(conn: &Connection, pool_id: &str) -> Result<bool> {
    Ok(conn.query_row(
        "SELECT EXISTS(SELECT 1 FROM pools WHERE id=?)",
        [pool_id],
        |row| row.get(0),
    )?)
}

/// Projects the pool's common notice status without exposing delivery or session ids.
/// The linked outbox row is live state; when absent, `not_created` describes the pool.
fn pool_delivery_view(conn: &Connection, pool_id: &str) -> Result<Value> {
    const STATES: &[&str] = &[
        "waiting_binding",
        "pending",
        "sending",
        "delivered",
        "retry_wait",
        "failed",
        "cancelled",
        "expired",
    ];
    const CLASSIFIERS: &[&str] = &[
        "relay_accepted",
        "relay_rejected",
        "relay_ambiguous",
        "relay_unavailable",
        "uds_receipt_held",
        "uds_receipt_delivered",
        "uds_receipt_refused",
        "uds_unconfirmed",
        "uds_ambiguous",
        "uds_session_gone",
        "uds_rejected",
        "uds_unavailable",
        "unsupported_transport",
    ];
    let (pool_session, delivery_id): (Option<String>, Option<String>) = conn.query_row(
        "SELECT orchestrator_session_id,completion_delivery_id FROM pools WHERE id=?",
        [pool_id],
        |row| Ok((row.get(0)?, row.get(1)?)),
    )?;
    let Some(delivery_id) = delivery_id else {
        return Ok(json!({
            "bound": pool_session.is_some(),
            "state": "not_created",
            "attempts": 0,
            "ambiguous": false,
            "last_classification": null,
            "last_attempt": null,
        }));
    };
    let (session, state, attempts, ambiguous, last_error): (
        Option<String>,
        String,
        u32,
        bool,
        Option<String>,
    ) = conn
        .query_row(
            "SELECT orchestrator_session_id,state,attempts,ambiguous_result,last_error \
             FROM deliveries WHERE id=?",
            [&delivery_id],
            |row| {
                Ok((
                    row.get(0)?,
                    row.get(1)?,
                    row.get(2)?,
                    row.get(3)?,
                    row.get(4)?,
                ))
            },
        )
        .optional()?
        .ok_or_else(|| Error::Integrity("pool completion delivery is missing".into()))?;
    let attempt = crate::delivery::latest_on(conn, &delivery_id)?;
    let evidence = attempt.filter(|value| {
        value
            .get("classifier")
            .and_then(Value::as_str)
            .is_some_and(|classifier| CLASSIFIERS.contains(&classifier))
    });
    let last_classification = evidence
        .as_ref()
        .and_then(|value| value.get("classifier"))
        .and_then(Value::as_str)
        .or_else(|| {
            last_error
                .as_deref()
                .filter(|classifier| CLASSIFIERS.contains(classifier))
        });
    Ok(json!({
        "bound": session.is_some(),
        "state": if STATES.contains(&state.as_str()) { state.as_str() } else { "unknown" },
        "attempts": attempts,
        "ambiguous": ambiguous,
        "last_classification": last_classification,
        "last_attempt": evidence,
    }))
}

/// One cursor page of a pool's log plus its derived status. Backward pages
/// return ascending entries and continue before their oldest returned sequence;
/// forward pages continue after their newest returned sequence.
fn read_page(
    conn: &Connection,
    pool_id: &str,
    after_seq: u64,
    before_seq: Option<u64>,
    limit: u32,
) -> Result<Value> {
    let tx = conn.unchecked_transaction()?;
    let status = pool_status(&tx, pool_id)?;
    let delivery = pool_delivery_view(&tx, pool_id)?;
    let (selection, order, cursor) = match before_seq {
        Some(before) => ("seq<?", "DESC", before as i64),
        None => ("seq>?", "ASC", after_seq as i64),
    };
    let mut statement = tx.prepare(&format!(
        "SELECT seq,author_kind,author_agent_id,author_name,author_role,direction,kind,\
             severity,proposal_seq,roster_revision,decision,body \
             FROM pool_entries WHERE pool_id=? AND {selection} ORDER BY seq {order} LIMIT ?"
    ))?;
    let rows = statement
        .query_map(params![pool_id, cursor, limit as i64 + 1], entry_view)?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    drop(statement);
    let complete = rows.len() <= limit as usize;
    let mut entries: Vec<_> = rows.into_iter().take(limit as usize).collect();
    if before_seq.is_some() {
        entries.reverse();
    }
    for entry in &entries {
        entry
            .validate()
            .map_err(|e| Error::Integrity(format!("pool entry is invalid: {e}")))?;
    }
    let next_cursor = if complete {
        None
    } else if before_seq.is_some() {
        entries.first().map(|entry| entry.seq)
    } else {
        entries.last().map(|entry| entry.seq)
    };
    let last_seq = entries.last().map(|entry| entry.seq);
    let page = json!({
            "pool_id": pool_id,
            "entries": entries,
            "after_seq": after_seq,
            "before_seq": before_seq,
            "limit": limit,
            "next_cursor": next_cursor,
            "last_seq": last_seq,
            "complete": complete,
            "activity": pool_activity(&status),
            "status": status,
            "delivery": delivery,
    });
    tx.commit()?;
    Ok(page)
}

/// Decodes one journal row into its validated public view shape.
pub fn entry_view(row: &rusqlite::Row<'_>) -> rusqlite::Result<PoolEntryView> {
    let kind: String = row.get(6)?;
    let decision: Option<String> = row.get(10)?;
    Ok(PoolEntryView {
        seq: row.get::<_, i64>(0)?.max(0) as u64,
        roster_revision: row.get(9)?,
        author_kind: match row.get::<_, String>(1)?.as_str() {
            "member" => AuthorKind::Member,
            "operator" => AuthorKind::Operator,
            _ => AuthorKind::Broker,
        },
        author_agent_id: row
            .get::<_, Option<String>>(2)?
            .map(|id| id.parse())
            .transpose()
            .map_err(|e: agent_run_domain::Error| {
                rusqlite::Error::FromSqlConversionFailure(
                    0,
                    rusqlite::types::Type::Text,
                    Box::new(e),
                )
            })?,
        author_name: row.get(3)?,
        author_role: row.get(4)?,
        direction: match row.get::<_, String>(5)?.as_str() {
            "team" => Direction::Team,
            _ => Direction::OrchestratorCopy,
        },
        kind: match kind.as_str() {
            "message" => EntryKind::Message,
            "report" => EntryKind::Report,
            "proposal" => EntryKind::Proposal,
            "vote" => EntryKind::Vote,
            "revoke" => EntryKind::Revoke,
            _ => EntryKind::Roster,
        },
        severity: row
            .get::<_, Option<String>>(7)?
            .and_then(|kind| match kind.as_str() {
                "notice" => Some(WorkerMessageKind::Notice),
                "risk" => Some(WorkerMessageKind::Risk),
                "question" => Some(WorkerMessageKind::Question),
                "blocker" => Some(WorkerMessageKind::Blocker),
                _ => None,
            }),
        proposal_seq: row.get::<_, Option<i64>>(8)?.map(|seq| seq.max(0) as u64),
        decision: decision.as_deref().and_then(|decision| match decision {
            "ready" => Some(VoteDecision::Ready),
            "block" => Some(VoteDecision::Block),
            _ => None,
        }),
        body: row.get(11)?,
    })
}

/// Reads the durable shape of an existing entry for replay comparison.
fn replay_shape_of(
    tx: &Transaction<'_>,
    pool_id: &str,
    seq: i64,
) -> Result<Vec<(&'static str, Value)>> {
    let (kind, body, snapshot, decision, proposal_seq, checks): (
        String,
        String,
        Option<String>,
        Option<String>,
        Option<i64>,
        Option<String>,
    ) = tx.query_row(
        "SELECT kind,body,snapshot,decision,proposal_seq,checks_json FROM pool_entries \
         WHERE pool_id=? AND seq=?",
        params![pool_id, seq],
        |row| {
            Ok((
                row.get(0)?,
                row.get(1)?,
                row.get(2)?,
                row.get(3)?,
                row.get(4)?,
                row.get(5)?,
            ))
        },
    )?;
    Ok(match kind.as_str() {
        "message" => vec![("body", json!(body))],
        "proposal" => vec![("body", json!(body)), ("snapshot", json!(snapshot))],
        _ => vec![
            ("decision", json!(decision)),
            ("proposal_seq", json!(proposal_seq)),
            (
                "checks",
                json!(checks.and_then(|raw| serde_json::from_str::<Value>(&raw).ok())),
            ),
            ("body", json!(body)),
        ],
    })
}

/// Appends one ordinary chat entry within the shared chat budget.
fn append_message(
    tx: &Transaction<'_>,
    member: &Membership,
    run_id: &AgentId,
    attempt_id: &str,
    input: &PoolMessage,
) -> AppendResult {
    let (rows, bytes): (i64, i64) = tx.query_row(
        "SELECT COUNT(*),COALESCE(SUM(length(CAST(body AS BLOB))),0) FROM pool_entries \
         WHERE pool_id=? AND kind='message'",
        [&member.pool_id],
        |row| Ok((row.get(0)?, row.get(1)?)),
    )?;
    // The candidate message itself must still fit: the check is prospective,
    // so a budget nearly full refuses an addition that would exceed it
    // instead of letting one more bounded message cross the line.
    if rows >= CHAT_ROW_BUDGET || bytes + input.message.len() as i64 > CHAT_BYTE_BUDGET {
        return Ok(Err(PoolDenial::ChatBudgetExhausted));
    }
    append_entry(
        tx,
        member,
        run_id,
        attempt_id,
        EntryKind::Message,
        None,
        None,
        None,
        None,
        None,
        &input.message,
        &input.request_id,
    )
    .map(Ok)
}

/// Appends one proposal within the proposal budget.
fn append_proposal(
    tx: &Transaction<'_>,
    member: &Membership,
    run_id: &AgentId,
    attempt_id: &str,
    input: &PoolPropose,
) -> AppendResult {
    let proposals: i64 = tx.query_row(
        "SELECT COUNT(*) FROM pool_entries WHERE pool_id=? AND kind='proposal'",
        [&member.pool_id],
        |row| row.get(0),
    )?;
    if proposals >= PROPOSAL_BUDGET {
        return Ok(Err(PoolDenial::ProposalBudgetExhausted));
    }
    append_entry(
        tx,
        member,
        run_id,
        attempt_id,
        EntryKind::Proposal,
        None,
        None,
        None,
        None,
        Some(&input.snapshot),
        &input.message,
        &input.request_id,
    )
    .map(Ok)
}

/// Appends one vote or revoke on the current proposal with full validation.
fn append_vote(
    tx: &Transaction<'_>,
    member: &Membership,
    run_id: &AgentId,
    attempt_id: &str,
    input: &PoolVote,
) -> AppendResult {
    let Some((current, _, _)) = current_proposal(tx, &member.pool_id)? else {
        return Ok(Err(PoolDenial::StaleProposal { current: None }));
    };
    if input.proposal_seq != current {
        return Ok(Err(PoolDenial::StaleProposal {
            current: Some(current),
        }));
    }
    if input.decision == VoteDecision::Ready {
        // A ready vote must judge exactly the pool's criterion set.
        let mut expected: Vec<&str> = member
            .acceptance
            .iter()
            .map(|(id, _)| id.as_str())
            .collect();
        expected.sort_unstable();
        let mut given: Vec<&str> = input
            .checks
            .iter()
            .map(|c| c.criterion_id.as_str())
            .collect();
        given.sort_unstable();
        if expected != given {
            return Ok(Err(PoolDenial::MalformedChecks));
        }
    }
    let spent: i64 = tx.query_row(
        "SELECT COUNT(*) FROM pool_entries \
         WHERE pool_id=? AND author_agent_id=? AND proposal_seq=? AND kind IN ('vote','revoke')",
        params![member.pool_id, member.seat_agent_id, current as i64],
        |row| row.get(0),
    )?;
    // The final slot is reserved for explicit control — a block or a revoke —
    // so a member can always object or withdraw a valid ready vote without a
    // new proposal; the total stays bounded at the budget.
    if spent >= VOTE_ROW_BUDGET
        || (spent == VOTE_ROW_BUDGET - 1
            && !matches!(input.decision, VoteDecision::Block | VoteDecision::Revoke))
    {
        return Ok(Err(PoolDenial::VoteBudgetExhausted));
    }
    let checks = (!input.checks.is_empty())
        .then(|| serde_json::to_string(&input.checks))
        .transpose()?;
    append_entry(
        tx,
        member,
        run_id,
        attempt_id,
        match input.decision {
            VoteDecision::Revoke => EntryKind::Revoke,
            _ => EntryKind::Vote,
        },
        None,
        Some(current),
        match input.decision {
            VoteDecision::Ready | VoteDecision::Block => Some(input.decision),
            VoteDecision::Revoke => None,
        },
        checks.as_deref(),
        None,
        input.message.as_deref().unwrap_or("(no note)"),
        &input.request_id,
    )
    .map(Ok)
}

/// Inserts the immutable entry with the stamped author and returns its seq.
#[allow(clippy::too_many_arguments)]
fn append_entry(
    tx: &Transaction<'_>,
    member: &Membership,
    run_id: &AgentId,
    attempt_id: &str,
    kind: EntryKind,
    severity: Option<&WorkerMessageKind>,
    proposal_seq: Option<u64>,
    decision: Option<VoteDecision>,
    checks: Option<&str>,
    snapshot: Option<&str>,
    body: &str,
    request_id: &str,
) -> Result<u64> {
    tx.execute(
        "INSERT INTO pool_entries(pool_id,author_kind,author_agent_id,author_name,author_role,\
         direction,kind,severity,proposal_seq,roster_revision,decision,snapshot,checks_json,body,\
         sender_run_id,sender_attempt_id,idem_scope,request_id,created_at) \
         VALUES(?,'member',?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
        params![
            member.pool_id,
            member.seat_agent_id,
            member.name,
            member.role,
            "team",
            kind.as_str(),
            severity.map(|severity| severity.as_str()),
            proposal_seq.map(|seq| seq as i64),
            member.roster_revision,
            decision.map(VoteDecision::as_str),
            snapshot,
            checks,
            body,
            run_id.as_str(),
            attempt_id,
            run_id.as_str(),
            request_id,
            now(),
        ],
    )
    .map_err(|error| match error {
        rusqlite::Error::SqliteFailure(failure, _)
            if failure.code == rusqlite::ErrorCode::ConstraintViolation =>
        {
            Error::Conflict
        }
        other => other.into(),
    })?;
    Ok(tx.last_insert_rowid().max(0) as u64)
}

/// Enqueues one pending `pool` command per current recipient tip.
///
/// The payload carries only the entry's sequence number: the log is the
/// authority and the runner re-reads the stamped entry before delivery and
/// re-checks that the recipient is still a current member's tip. The sender
/// is never pushed to itself. A recipient whose tip already ended is skipped,
/// because nothing would ever claim the command; it catches up through
/// `pool_read`. Called inside the writer's transaction so the entry and its
/// commands commit or roll back together.
pub(crate) fn fanout_entry(
    tx: &Transaction<'_>,
    pool_id: &str,
    sender_seat: &str,
    seq: u64,
) -> Result<()> {
    let mut statement = tx.prepare(
        "SELECT m.agent_id FROM pool_members m \
         WHERE m.pool_id=? AND m.replaced_by IS NULL AND m.agent_id<>?",
    )?;
    let seats = statement
        .query_map(params![pool_id, sender_seat], |row| row.get::<_, String>(0))?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    drop(statement);
    for seat in seats {
        let tip = tip_of(tx, &seat.parse()?)?;
        let status: String = tx.query_row(
            "SELECT status FROM agents WHERE id=?",
            [tip.as_str()],
            |row| row.get(0),
        )?;
        if status
            .parse::<agent_run_domain::domain::Status>()?
            .terminal()
        {
            continue;
        }
        tx.execute(
            "INSERT INTO commands(agent_id,kind,payload_json,state,created_at) \
             VALUES(?, 'pool', ?, 'pending', ?)",
            params![
                tip.as_str(),
                serde_json::to_string(&json!({"seq": seq}))?,
                now()
            ],
        )?;
    }
    Ok(())
}

/// The status of a completed pool exactly as it was when it completed, read
/// from the immutable completion event so a member that later resumes never
/// changes or retroactively invalidates the recorded proof.
fn frozen_status(conn: &Connection, pool_id: &str) -> Result<Option<Value>> {
    let raw: Option<(String, f64)> = conn
        .query_row(
            "SELECT e.data_json,p.completed_at FROM pools p \
             JOIN deliveries d ON d.id=p.completion_delivery_id \
             JOIN events e ON e.seq=d.terminal_event_seq WHERE p.id=?",
            [pool_id],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .optional()?;
    let Some((data, completed_at)) = raw else {
        return Ok(None);
    };
    let data: Value = serde_json::from_str(&data)
        .map_err(|_| Error::Integrity("pool completion record is malformed".into()))?;
    let members: Vec<Value> = data["members"]
        .as_array()
        .into_iter()
        .flatten()
        .map(|member| {
            json!({
                "name": member["name"], "role": member["role"], "agent_id": member["agent_id"],
                "slot": member["slot"], "tip_status": "succeeded", "cleanup_complete": true,
                "vote": {"decision": "ready"}, "counts": true, "why": "valid",
            })
        })
        .collect();
    let mut retired = conn.prepare(
        "SELECT m.agent_id,m.slot,m.name,m.role,m.replaced_by FROM pool_members m \
         WHERE m.pool_id=? AND m.replaced_by IS NOT NULL ORDER BY m.slot,m.joined_roster_revision",
    )?;
    let retired: Vec<Value> = retired
        .query_map([pool_id], |row| {
            Ok(json!({
                "agent_id": row.get::<_, String>(0)?, "slot": row.get::<_, i64>(1)?,
                "name": row.get::<_, String>(2)?, "role": row.get::<_, String>(3)?,
                "replaced_by": row.get::<_, String>(4)?,
            }))
        })?
        .collect::<rusqlite::Result<_>>()?;
    Ok(Some(json!({
        "state": "completed",
        "completed_at": completed_at,
        "roster_revision": data["roster_revision"],
        "goal": data["goal"],
        "criteria": data["acceptance"],
        "current_proposal": {"seq": data["proposal"]["seq"], "snapshot": data["proposal"]["snapshot"],
                             "roster_revision": data["roster_revision"]},
        "members": members,
        "replaced_members": retired,
        "agreed": true,
        "note": "frozen at completion: the recorded proof does not change if a member is resumed later; agent-run verified formal checks only",
    })))
}

/// Computes the derived status: roster, current proposal, per-member vote
/// validity with reasons, and whether unanimity currently holds. Agreement is
/// explicitly not completion; nothing here ever completes the pool.
fn pool_status(conn: &Connection, pool_id: &str) -> Result<Value> {
    let (goal, state, roster_revision, acceptance): (String, String, u32, String) = conn
        .query_row(
            "SELECT goal,state,roster_revision,acceptance_json FROM pools WHERE id=?",
            [pool_id],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
        )?;
    let mut seats = conn.prepare(
        "SELECT m.agent_id,m.slot,m.name,m.role FROM pool_members m \
         WHERE m.pool_id=? AND m.replaced_by IS NULL ORDER BY m.slot",
    )?;
    let rows = seats
        .query_map([pool_id], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, i64>(1)?,
                row.get::<_, String>(2)?,
                row.get::<_, String>(3)?,
            ))
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    drop(seats);
    if state == "completed"
        && let Some(frozen) = frozen_status(conn, pool_id)?
    {
        return Ok(frozen);
    }
    let proposal = current_proposal(conn, pool_id)?;
    let criteria: Vec<Value> = criteria_of(&acceptance)?
        .iter()
        .map(|(id, text)| json!({"id": id, "text": text}))
        .collect();
    let mut members = Vec::new();
    let mut agreed = proposal.is_some();
    for (seat_id, slot, name, role) in rows {
        let seat: AgentId = seat_id.parse()?;
        let tip = tip_of(conn, &seat)?;
        let tip_status: String = conn.query_row(
            "SELECT status FROM agents WHERE id=?",
            [tip.as_str()],
            |row| row.get(0),
        )?;
        let (vote, why) = match &proposal {
            None => (None, "no_proposal".to_owned()),
            Some((current, _, proposal_roster)) => {
                let latest = latest_vote(conn, pool_id, &seat_id, *current)?;
                vote_validity(latest, roster_revision, *proposal_roster, &tip, &tip_status)
            }
        };
        let enrollment = crate::pool_enrollment::view(conn, &seat)?;
        let why = if enrollment.as_ref().is_some_and(|e| e["state"] != "joined") {
            "join_pending".to_owned()
        } else {
            why
        };
        if why != "valid" {
            agreed = false;
        }
        let mut item = json!({
            "name": name, "role": role, "agent_id": seat_id, "slot": slot,
            "tip_status": tip_status,
            "cleanup_complete": lineage_cleanup_complete(conn, &seat)?,
            "vote": vote, "counts": why == "valid", "why": why,
        });
        if let Some(enrollment) = enrollment {
            item["enrollment"] = enrollment;
        }
        members.push(item);
    }
    let mut retired = conn.prepare(
        "SELECT m.agent_id,m.slot,m.name,m.role,m.replaced_by FROM pool_members m \
         WHERE m.pool_id=? AND m.replaced_by IS NOT NULL ORDER BY m.slot,m.joined_roster_revision",
    )?;
    let retired: Vec<Value> = retired
        .query_map([pool_id], |row| {
            Ok(json!({
                "agent_id": row.get::<_, String>(0)?, "slot": row.get::<_, i64>(1)?,
                "name": row.get::<_, String>(2)?, "role": row.get::<_, String>(3)?,
                "replaced_by": row.get::<_, String>(4)?,
            }))
        })?
        .collect::<rusqlite::Result<_>>()?;
    Ok(json!({
        "state": state,
        "roster_revision": roster_revision,
        "goal": goal,
        "criteria": criteria,
        "current_proposal": proposal.as_ref().map(|(seq, snapshot, roster)| json!({
            "seq": seq, "snapshot": snapshot, "roster_revision": roster,
        })),
        "members": members,
        "replaced_members": retired,
        "agreed": agreed,
        "note": "agreement is not completion: the pool completes only after every member execution ends successfully with cleanup evidence",
    }))
}

/// Projects a derived pool status into one read-time activity word.
///
/// Presentation only: it is never stored, completes nothing and leaves the
/// status proof untouched. `state` stays `open` after a cancellation so a
/// member can still resume or be replaced, which returns the activity to
/// `running`. Precedence:
///
/// * `completed` — the frozen completed record;
/// * `running` — any current member's latest run is not terminal;
/// * `stopping` — every latest run is terminal but some member lineage still
///   lacks verified cleanup, so nothing is called stopped or cancelled yet;
/// * `cancelled` — every latest run is cancelled and every lineage cleaned;
/// * `settling` — every latest run succeeded, all votes agree and all cleanup
///   is proven, but the pool has not been recorded completed yet;
/// * `needs_action` — every other fully terminal, cleaned case (a failure, a
///   mixed cancellation, or missing, blocked or stale votes).
///
/// A pool with no current member is `needs_action`.
fn pool_activity(status: &Value) -> &'static str {
    if status["members"].as_array().is_some_and(|members| {
        members
            .iter()
            .any(|m| m["enrollment"]["state"] == "needs_action")
    }) {
        return "needs_action";
    }
    if status["state"] == json!("completed") {
        return "completed";
    }
    let members = status["members"]
        .as_array()
        .map(Vec::as_slice)
        .unwrap_or(&[]);
    let tips: Vec<Option<agent_run_domain::domain::Status>> = members
        .iter()
        .map(|member| member["tip_status"].as_str().and_then(|s| s.parse().ok()))
        .collect();
    if members.is_empty() {
        return "needs_action";
    }
    // An unparsable status is unknown, so it is treated as still active.
    if tips
        .iter()
        .any(|tip| !tip.is_some_and(|tip| tip.terminal()))
    {
        return "running";
    }
    if members
        .iter()
        .any(|member| member["cleanup_complete"] != json!(true))
    {
        return "stopping";
    }
    let all = |wanted| tips.iter().all(|tip| *tip == Some(wanted));
    if all(agent_run_domain::domain::Status::Cancelled) {
        "cancelled"
    } else if all(agent_run_domain::domain::Status::Succeeded) && status["agreed"] == json!(true) {
        "settling"
    } else {
        "needs_action"
    }
}

/// Derives one member's vote status fields and the reason it counts or not.
pub(crate) fn vote_validity(
    latest: Option<(String, Option<String>, Option<String>, String)>,
    roster_revision: u32,
    proposal_roster: u32,
    tip: &AgentId,
    tip_status: &str,
) -> (Option<Value>, String) {
    let Some((kind, decision, checks, sender)) = latest else {
        return (None, "missing".into());
    };
    if kind == "revoke" {
        return (Some(json!({"decision": "revoke"})), "revoked".into());
    }
    let decision = decision.unwrap_or_default();
    if decision == "block" {
        return (Some(json!({"decision": "block"})), "blocked".into());
    }
    if proposal_roster != roster_revision {
        return (Some(json!({"decision": decision})), "stale_roster".into());
    }
    if sender != tip.as_str() {
        return (Some(json!({"decision": decision})), "stale_tip".into());
    }
    if !matches!(tip_status, "running" | "succeeded") {
        return (Some(json!({"decision": decision})), tip_status.to_owned());
    }
    // A ready vote counts only with every criterion met.
    let parsed: Option<Vec<Value>> =
        checks.and_then(|raw| serde_json::from_str::<Vec<Value>>(&raw).ok());
    let all_met =
        parsed.is_some_and(|items| items.iter().all(|check| check["status"] == json!("met")));
    if !all_met {
        return (Some(json!({"decision": decision})), "checks_unmet".into());
    }
    (Some(json!({"decision": decision})), "valid".into())
}
