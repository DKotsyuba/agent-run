//! Attempt-pinned active-worker enrollment; no execution restarts or quota writes.
use crate::{Record, Store};
use agent_run_domain::{
    Error, Result,
    domain::{AgentId, OrchestratorRef, Status, now},
    error::invalid,
};
use rusqlite::{Connection, OptionalExtension, Transaction, params};
use serde_json::{Value, json};

/// Immutable preflight facts pinned again by the pool's immediate admission transaction.
/// No bearer capability or native session content is exposed in public responses.
#[derive(Clone, PartialEq)]
pub struct ExistingMember {
    /// Stable lineage root requested by the operator.
    pub agent_id: AgentId,
    /// Exact current RUNNING execution.
    pub run_id: AgentId,
    /// Exact owned attempt whose actual private catalog has been observed.
    pub attempt_id: String,
    /// Original created-at plus timeout, never renewed by enrollment.
    pub deadline: f64,
    /// Existing display name, or a safe seat-local fallback.
    pub name: Option<String>,
    /// Existing execution task copied into pool history without editing the agent.
    pub task: String,
    /// Existing canonical orchestrator binding, when already bound.
    pub orchestrator: Option<OrchestratorRef>,
    /// Digest of immutable launch/task/session/account facts for atomic race detection.
    pub fingerprint: String,
}

/// Compares only immutable chat identity, allowing legitimate turn/liveness
/// refreshes without changing the attached worker's destination.
pub fn same_binding(
    left: Option<&OrchestratorRef>,
    right: Option<&OrchestratorRef>,
) -> Result<bool> {
    Ok(match (left, right) {
        (None, None) => true,
        (Some(a), Some(b)) => {
            a.canonical_transport()? == b.canonical_transport()?
                && a.external_session_id == b.external_session_id
        }
        _ => false,
    })
}

/// Resolves a stable independent root to its supported exact live execution.
/// Historical aliases, prior membership, expired/non-RUNNING/unowned attempts
/// and unobserved catalogs fail closed; no state, reservation or binding changes.
pub fn existing_member(conn: &Connection, root: &AgentId, at: f64) -> Result<ExistingMember> {
    let original = conn
        .query_row(
            "SELECT * FROM agents WHERE id=?",
            [root.as_str()],
            Record::read,
        )
        .optional()?
        .ok_or_else(|| Error::NotFound(root.to_string()))?;
    let history: Option<i64> = conn.query_row(
        "SELECT pool_membership_ever FROM agents WHERE id=?",
        [root.as_str()],
        |r| r.get(0),
    )?;
    if history == Some(1) {
        return Err(invalid(
            "existing agent already has pool membership history",
        ));
    }
    if history != Some(0) {
        return Err(Error::Unsupported(
            "existing agent independence history is unknown".into(),
        ));
    }
    if original.root_agent_id != *root {
        return Err(invalid("existing_agent_id must name the stable agent root"));
    }
    if conn.query_row(
        "SELECT EXISTS(SELECT 1 FROM pool_members WHERE agent_id=?)",
        [root.as_str()],
        |r| r.get::<_, bool>(0),
    )? {
        return Err(invalid("existing agent already has pool membership"));
    }
    let row = conn.query_row(
        "SELECT * FROM agents WHERE root_agent_id=? OR id=? ORDER BY sequence DESC,created_at DESC,id DESC LIMIT 1",
        params![root.as_str(),root.as_str()], Record::read)?;
    let deadline: f64 = conn.query_row(
        "SELECT created_at+timeout_seconds FROM agents WHERE id=?",
        [row.id.as_str()],
        |r| r.get(0),
    )?;
    if row.status != Status::Running || row.finished_at.is_some() || deadline <= at {
        return Err(invalid(
            "existing pool member must be RUNNING within its original deadline",
        ));
    }
    if agent_run_platform::process::observe(
        row.supervisor_pid,
        row.supervisor_identity.as_deref(),
        row.supervisor_birth_time,
    ) != agent_run_platform::process::ProcessState::Alive
    {
        return Err(Error::Unsupported(
            "existing worker ownership is not verifiably alive".into(),
        ));
    }
    let attempt: Option<(String, Option<i64>, Option<String>)> = conn.query_row(
        "SELECT t.id,c.pool_catalog_version,c.pool_catalog_digest FROM attempts t
         LEFT JOIN worker_capabilities c ON c.attempt_id=t.id
         WHERE t.agent_id=? AND t.state='running' AND t.ownership_active=1 AND t.finished_at IS NULL",
        [row.id.as_str()], |r| Ok((r.get(0)?,r.get(1)?,r.get(2)?))).optional()?;
    let (attempt_id, version, digest) =
        attempt.ok_or_else(|| invalid("existing worker attempt is not owned and RUNNING"))?;
    if version != Some(i64::from(agent_run_domain::worker::POOL_CATALOG_VERSION))
        || digest.as_deref() != Some(agent_run_domain::worker::pool_catalog_digest().as_str())
    {
        return Err(Error::Unsupported(
            "existing worker pool catalog is unknown or unsupported".into(),
        ));
    }
    let orchestrator = row.orchestrator_session_id.as_ref().map(|sid| {
        conn.query_row("SELECT transport,external_session_id,external_turn_id FROM orchestrator_sessions WHERE id=?",
            [sid], |r| Ok(OrchestratorRef { transport:r.get(0)?,external_session_id:r.get(1)?,external_turn_id:r.get(2)? }))
    }).transpose()?;
    let account: Option<String> = conn.query_row(
        "SELECT selected_account_id FROM attempts WHERE id=?",
        [&attempt_id],
        |r| r.get(0),
    )?;
    let fingerprint = agent_run_domain::canonical::sha256_hex(
        &json!([
            row.request,
            row.identity,
            row.runtime_session_id,
            row.created_at,
            deadline,
            account
        ]),
        true,
    );
    Ok(ExistingMember {
        agent_id: root.clone(),
        run_id: row.id,
        attempt_id,
        deadline,
        name: row.display_name,
        task: row.request.task,
        orchestrator,
        fingerprint,
    })
}

/// Reads one seat's enrollment overlay. New-only seats are already joined.
/// Public data never exposes run/attempt/token identities; the challenge is
/// provided only in the recipient's private pool_read response and durable intro.
pub fn view(conn: &Connection, root: &AgentId, at: f64) -> Result<Option<Value>> {
    conn.query_row(
        "SELECT state,deadline,failure_reason FROM pool_enrollments WHERE agent_id=?",
        [root.as_str()],
        |r| {
            let state: String = r.get(0)?;
            let deadline: f64 = r.get(1)?;
            Ok(
                json!({"state":state,"deadline":deadline,"remaining_seconds":(deadline-at).max(0.0),
                "reason":r.get::<_,Option<String>>(2)?}),
            )
        },
    )
    .optional()
    .map_err(Into::into)
}

/// Reserved enrollment keys remain pinned even after joined and before
/// idempotent lookup; another member/run/attempt can never replay an old ACK.
pub(crate) fn validate_ack_replay(
    conn: &Connection,
    root: &AgentId,
    run: &AgentId,
    attempt: &str,
    key: &str,
    is_message: bool,
) -> Result<()> {
    let issued: Option<(String, String, String, f64)> = conn
        .query_row(
            "SELECT agent_id,run_id,attempt_id,deadline FROM pool_enrollments WHERE challenge=?",
            [key],
            |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?)),
        )
        .optional()?;
    if let Some((member, pinned_run, pinned_attempt, deadline)) = issued
        && (!is_message
            || member != root.as_str()
            || pinned_run != run.as_str()
            || pinned_attempt != attempt
            || deadline <= now())
    {
        return Err(invalid(
            "enrollment ACK key is pinned to a different or expired worker attempt",
        ));
    }
    Ok(())
}

/// Requires an exact pinned live attempt and matching opaque request key before
/// a pending member's first message can acknowledge enrollment. Joined/new
/// seats use ordinary pool rules; no message body is parsed as control syntax.
pub(crate) fn ack_allowed(
    conn: &Connection,
    root: &AgentId,
    run: &AgentId,
    attempt: &str,
    request: Option<&str>,
) -> Result<bool> {
    let row: Option<(String,String,String,String,f64)> = conn.query_row(
        "SELECT state,run_id,attempt_id,challenge,deadline FROM pool_enrollments WHERE agent_id=?",
        [root.as_str()], |r| Ok((r.get(0)?,r.get(1)?,r.get(2)?,r.get(3)?,r.get(4)?))).optional()?;
    let Some((state, pinned_run, pinned_attempt, challenge, deadline)) = row else {
        return Ok(false);
    };
    if state == "joined" {
        return Ok(false);
    }
    if pinned_run != run.as_str() || pinned_attempt != attempt || deadline <= now() {
        return Err(invalid("pool enrollment attempt ended, changed or expired"));
    }
    if request != Some(challenge.as_str()) {
        return Err(invalid(
            "pool enrollment requires its exact broker-issued ACK request_id",
        ));
    }
    Ok(true)
}

/// Commits awareness only after the reserved authenticated summary entry exists.
/// The caller owns the transaction, so entry, ACK state and peer fanout are atomic.
pub(crate) fn acknowledge(tx: &Transaction<'_>, root: &AgentId, seq: u64) -> Result<()> {
    let changed=tx.execute("UPDATE pool_enrollments SET state='joined',ack_seq=?,ack_at=?,failure_reason=NULL WHERE agent_id=? AND state<>'joined'",
        params![seq,now(),root.as_str()])?;
    if changed != 1 {
        return Err(Error::Conflict);
    }
    tx.execute("UPDATE deliveries SET state=\'cancelled\',next_attempt_at=NULL WHERE id=(SELECT attention_delivery_id FROM pool_enrollments WHERE agent_id=?) AND state IN (\'pending\',\'waiting_binding\',\'retry_wait\')", [root.as_str()])?;
    Ok(())
}

/// Returns a fixed failure reason when the original pinned execution/attempt
/// can no longer acknowledge. Missing proof is not replaced by a new attempt.
fn failure_reason(
    conn: &Connection,
    root: &str,
    run: &str,
    attempt: &str,
    deadline: f64,
    at: f64,
) -> Result<Option<&'static str>> {
    if deadline <= at {
        return Ok(Some("deadline_expired"));
    }
    let state: Option<String> = conn
        .query_row("SELECT status FROM agents WHERE id=?", [run], |r| r.get(0))
        .optional()?;
    if state.as_deref() != Some("running") {
        return Ok(Some("worker_ended"));
    }
    let owned:bool=conn.query_row("SELECT EXISTS(SELECT 1 FROM attempts WHERE id=? AND agent_id=? AND ownership_active=1 AND finished_at IS NULL AND state='running')",
        params![attempt,run],|r|r.get(0))?;
    let tip:String=conn.query_row("SELECT id FROM agents WHERE root_agent_id=? OR id=? ORDER BY sequence DESC,created_at DESC,id DESC LIMIT 1",params![root,root],|r|r.get(0))?;
    Ok(if !owned || tip != run {
        Some("attempt_changed")
    } else {
        None
    })
}

/// Read-only preflight for actual enrollment mutations. Empty/new-only pools,
/// healthy pending workers and identical recorded failures return no work.
/// The same reads repeat under the admission writer transaction before any
/// update; a concurrent ACK/GC can only remove a proposed change.
fn enrollment_changes(
    conn: &Connection,
    pool: &agent_run_domain::pool::PoolId,
    at: f64,
) -> Result<Vec<(String, &'static str)>> {
    let mut q=conn.prepare("SELECT j.agent_id,j.run_id,j.attempt_id,j.deadline FROM pool_enrollments j JOIN pool_members m ON m.agent_id=j.agent_id WHERE m.pool_id=? AND m.replaced_by IS NULL AND j.state<>'joined'")?;
    let rows = q
        .query_map([pool.as_str()], |r| {
            Ok((
                r.get::<_, String>(0)?,
                r.get::<_, String>(1)?,
                r.get::<_, String>(2)?,
                r.get::<_, f64>(3)?,
            ))
        })?
        .collect::<std::result::Result<Vec<_>, _>>()?;
    drop(q);
    let mut changed = Vec::new();
    for (root, run, attempt, deadline) in rows {
        let delivery_failed:bool=conn.query_row(
            "SELECT EXISTS(SELECT 1 FROM commands WHERE agent_id=? AND kind='steer' AND json_extract(payload_json,'$.pool_enrollment')=? AND state='completed' AND COALESCE(json_extract(result_json,'$.accepted'),0)=0)",
            params![run,root],|r|r.get(0))?;
        let reason =
            failure_reason(conn, &root, &run, &attempt, deadline, at)?.or(if delivery_failed {
                Some("delivery_unconfirmed")
            } else {
                None
            });
        let Some(reason) = reason else {
            continue;
        };
        let recorded:Option<(String,Option<String>,bool)>=conn.query_row(
            "SELECT state,failure_reason,attention_issued=1 FROM pool_enrollments WHERE agent_id=?",
            [&root],|r|Ok((r.get(0)?,r.get(1)?,r.get(2)?))).optional()?;
        let Some((state, previous, issued)) = recorded else {
            continue;
        };
        if state != "joined"
            && (state != "needs_action" || previous.as_deref() != Some(reason) || !issued)
        {
            changed.push((root, reason));
        }
    }
    Ok(changed)
}

impl Store {
    /// Converges only changed attachment state/attention under an immediate
    /// transaction after a read-only preflight. Empty pools, healthy pending and
    /// identical failures never reserve the per-tick writer lock. Every decision
    /// repeats under that lock; terminal error notices suppress duplicate attention.
    pub fn reconcile_pool_enrollments(
        &mut self,
        pool: &agent_run_domain::pool::PoolId,
    ) -> Result<()> {
        if enrollment_changes(&self.conn, pool, now())?.is_empty() {
            return Ok(());
        }
        let tx = self
            .conn
            .transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
        for (root, reason) in enrollment_changes(&tx, pool, now())? {
            let (run, attempt): (String, String) = tx.query_row(
                "SELECT run_id,attempt_id FROM pool_enrollments WHERE agent_id=?",
                [&root],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )?;
            tx.execute("UPDATE pool_enrollments SET state='needs_action',failure_reason=? WHERE agent_id=? AND state<>'joined' AND (state<>'needs_action' OR failure_reason IS NOT ?)",params![reason,root,reason])?;
            let covered:bool=tx.query_row("SELECT EXISTS(SELECT 1 FROM deliveries d JOIN agents a ON a.id=d.agent_id JOIN events e ON e.seq=d.terminal_event_seq WHERE d.agent_id=? AND a.status IN ('failed','lost','timed_out','cancelled') AND e.kind='status' AND e.to_status=a.status)",[&run],|r|r.get(0))?;
            let already: bool = tx.query_row(
                "SELECT attention_issued=1 FROM pool_enrollments WHERE agent_id=?",
                [&root],
                |r| r.get(0),
            )?;
            if covered && !already {
                tx.execute(
                    "UPDATE pool_enrollments SET attention_issued=1 WHERE agent_id=?",
                    [&root],
                )?;
            }
            if !covered && !already {
                let message = format!(
                    "Pool {pool} needs action: existing member {root} did not acknowledge enrollment ({reason}). Its work was not restarted or cancelled by enrollment. Inspect pool status; use explicit existing recovery controls."
                );
                tx.execute("INSERT INTO events(agent_id,attempt_id,at,kind,data_json) VALUES(?,?,?,'pool_join_needs_action',?)",
                    params![run,attempt,now(),serde_json::to_string(&json!({"pool_id":pool,"member":root,"reason":reason,"notice":message}))?])?;
                let event = tx.last_insert_rowid();
                let delivery = format!("ntf_{}", uuid::Uuid::new_v4().simple());
                let session: Option<String> = tx.query_row(
                    "SELECT orchestrator_session_id FROM pools WHERE id=?",
                    [pool.as_str()],
                    |r| r.get(0),
                )?;
                tx.execute("INSERT INTO deliveries(id,agent_id,orchestrator_session_id,terminal_event_seq,state,next_attempt_at) VALUES(?,?,?,?,?,?)",
                    params![delivery,run,session,event,if session.is_some(){"pending"}else{"waiting_binding"},session.as_ref().map(|_|now())])?;
                tx.execute(
                    "UPDATE pool_enrollments SET attention_delivery_id=?,attention_issued=1 WHERE agent_id=?",
                    params![delivery, root],
                )?;
            }
        }
        tx.commit()?;
        Ok(())
    }

    /// Read-only preflight for the operator's stable existing-member identity.
    pub fn existing_pool_member(&self, id: &AgentId) -> Result<ExistingMember> {
        existing_member(&self.conn, id, now())
    }
}
