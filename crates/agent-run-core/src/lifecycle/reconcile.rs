//! Fair, evidence-gated recovery of abandoned active rows.
//!
//! Process observation deliberately occurs after the short cursor transaction
//! and before the short terminal transaction.  In particular, elapsed wall
//! time is never evidence that an admitted run stopped.

use agent_run_domain::{
    domain::{now, AgentId, Status},
    error::invalid,
    Result,
};
use agent_run_platform::process::{self, ProcessState};
use agent_run_store::Store;
use rusqlite::{params, OptionalExtension, TransactionBehavior};
use serde::Deserialize;
use serde_json::json;

/// A captured row that can be rechecked after its process probe completes.
#[derive(Debug, Clone)]
struct Candidate {
    /// Durable agent identity used to recheck the selected row.
    id: AgentId,
    /// Fair keyset ordering timestamp.
    created_at: f64,
    /// Lifecycle state captured before the external probe.
    status: Status,
    /// Supervisor PID when ownership reached READY.
    supervisor_pid: Option<i32>,
    /// Recorded supervisor process group, required for a supervisor proof.
    process_group_id: Option<i32>,
    /// Immutable supervisor token or legacy diagnostic identity.
    supervisor_identity: Option<String>,
    /// Supervisor creation-time evidence for PID-reuse detection.
    supervisor_birth_time: Option<f64>,
    /// Serialized broker/startup owner identity before supervisor READY.
    startup_owner: Option<String>,
    /// Separately persisted startup birth evidence for legacy rows.
    startup_birth_time: Option<f64>,
    /// Last supervisor heartbeat, required for a complete active-owner proof.
    heartbeat_at: Option<f64>,
}

/// The process fields preserved in a Rust startup-owner JSON record.
#[derive(Debug, Deserialize)]
struct StartupOwner {
    /// Positive OS process ID of the broker or handoff owner.
    pid: i32,
    /// Native immutable process-start token.
    token: String,
    /// Native process creation time used for legacy compatibility.
    birth: f64,
}

/// Reconcile up to `limit` active rows with the native PID-reuse-aware observer.
///
/// The return value contains only rows that durably became `lost`.  `limit`
/// must be between one and 1000.  `Unknown`, `Denied`, `Alive`, and incomplete
/// ownership evidence leave rows untouched.
pub fn reconcile(store: &mut Store, limit: usize) -> Result<Vec<AgentId>> {
    reconcile_with(store, limit, process::observe)
}

/// Reconcile rows using `observe`, which exists to make recovery evidence testable.
///
/// `observe` receives the PID plus its immutable token/birth evidence and must
/// return a platform observation.  Production calls [`reconcile`]; callers
/// should not use this hook to treat missing evidence as death.
pub fn reconcile_with<F>(store: &mut Store, limit: usize, observe: F) -> Result<Vec<AgentId>>
where
    F: Fn(Option<i32>, Option<&str>, Option<f64>) -> ProcessState,
{
    if !(1..=1000).contains(&limit) {
        return Err(invalid("reconciliation limit must be 1..1000"));
    }
    let mut changed = Vec::new();
    for row in fair_rows(store, "unowned_starting", unowned_sql(), limit)? {
        let Some(owner) = startup_owner(&row) else {
            continue;
        };
        let state = observe(Some(owner.pid), Some(&owner.token), Some(owner.birth));
        if proved_gone(state)
            && guarded_lost(
                store,
                &row,
                "unowned_starting",
                &format!("startup owner process is {}", state_name(state)),
                json!({"verdict": state_name(state)}),
                now(),
            )?
        {
            changed.push(row.id);
        }
    }
    let remaining = limit.saturating_sub(changed.len());
    if remaining == 0 {
        return Ok(changed);
    }
    for row in fair_rows(store, "active_supervisors", active_sql(), remaining)? {
        let (Some(pid), Some(pgid), Some(identity)) = (
            row.supervisor_pid,
            row.process_group_id,
            row.supervisor_identity.as_deref(),
        ) else {
            continue;
        };
        let state = observe(Some(pid), Some(identity), row.supervisor_birth_time);
        let failure_kind = match state {
            ProcessState::Dead => "supervisor_dead",
            ProcessState::Reused => "supervisor_identity_mismatch",
            ProcessState::Alive
            | ProcessState::Unknown
            | ProcessState::Denied
            | ProcessState::NotStarted => continue,
        };
        if guarded_lost(
            store,
            &row,
            failure_kind,
            "periodic detached supervisor reconciliation",
            json!({"verdict": state_name(state), "supervisor_pid": pid, "process_group_id": pgid}),
            now(),
        )? {
            changed.push(row.id);
        }
    }
    Ok(changed)
}

/// Return whether an OS state affirmatively proves the recorded owner is gone.
fn proved_gone(state: ProcessState) -> bool {
    matches!(state, ProcessState::Dead | ProcessState::Reused)
}

/// Render a stable event verdict without exposing platform debug formatting.
fn state_name(state: ProcessState) -> &'static str {
    match state {
        ProcessState::Alive => "alive",
        ProcessState::Dead => "dead",
        ProcessState::Reused => "reused",
        ProcessState::Unknown => "unknown",
        ProcessState::Denied => "denied",
        ProcessState::NotStarted => "not_started",
    }
}

/// Decode complete startup ownership evidence, preferring the JSON's exact birth.
fn startup_owner(row: &Candidate) -> Option<StartupOwner> {
    let owner: StartupOwner = serde_json::from_str(row.startup_owner.as_deref()?).ok()?;
    (owner.pid > 1 && owner.birth.is_finite() && owner.birth >= 0.0 && !owner.token.is_empty())
        .then_some(owner)
}

/// Fixed query for broker-owned starts which have not yet bound a supervisor.
fn unowned_sql() -> &'static str {
    "SELECT * FROM agents WHERE status='starting' AND supervisor_pid IS NULL \
     AND process_group_id IS NULL AND supervisor_identity IS NULL"
}

/// Fixed query for the complete active lifecycle set, matching Python's sweep.
fn active_sql() -> &'static str {
    "SELECT * FROM agents WHERE status IN ('created','starting','running','cancelling')"
}

/// Return and advance one persisted fair keyset window for a fixed candidate query.
fn fair_rows(store: &mut Store, name: &str, select: &str, limit: usize) -> Result<Vec<Candidate>> {
    let cursor = store
        .conn
        .query_row(
            "SELECT created_at,agent_id FROM reconciliation_cursors WHERE name=?",
            [name],
            |row| Ok((row.get::<_, f64>(0)?, row.get::<_, String>(1)?)),
        )
        .optional()?;
    let mut rows = if let Some((created_at, id)) = cursor.as_ref() {
        read_candidates(
            store,
            &format!(
                "{select} AND (created_at > ? OR (created_at = ? AND id > ?)) ORDER BY created_at,id LIMIT ?"
            ),
            params![created_at, created_at, id, limit as i64],
        )?
    } else {
        read_candidates(
            store,
            &format!("{select} ORDER BY created_at,id LIMIT ?"),
            params![limit as i64],
        )?
    };
    if rows.len() < limit {
        if let Some((created_at, id)) = cursor {
            let remaining = (limit - rows.len()) as i64;
            rows.extend(read_candidates(
                store,
                &format!(
                    "{select} AND (created_at < ? OR (created_at = ? AND id <= ?)) ORDER BY created_at,id LIMIT ?"
                ),
                params![created_at, created_at, id, remaining],
            )?);
        }
    }
    if let Some(last) = rows.last() {
        let tx = store
            .conn
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        tx.execute(
            "INSERT INTO reconciliation_cursors(name,created_at,agent_id) VALUES(?,?,?) \
             ON CONFLICT(name) DO UPDATE SET created_at=excluded.created_at,agent_id=excluded.agent_id",
            params![name, last.created_at, last.id.as_str()],
        )?;
        tx.commit()?;
    }
    Ok(rows)
}

/// Decode one detached candidate page without retaining a SQLite statement.
fn read_candidates<P: rusqlite::Params>(
    store: &Store,
    sql: &str,
    params: P,
) -> Result<Vec<Candidate>> {
    let mut statement = store.conn.prepare(sql)?;
    let mut rows = statement.query(params)?;
    let mut candidates = Vec::new();
    while let Some(row) = rows.next()? {
        candidates.push(read_candidate(row)?);
    }
    Ok(candidates)
}

/// Decode only reconciliation-owned columns from a durable agents row.
fn read_candidate(row: &rusqlite::Row<'_>) -> Result<Candidate> {
    let id: String = row.get("id")?;
    let status: String = row.get("status")?;
    Ok(Candidate {
        id: id.parse()?,
        created_at: row.get("created_at")?,
        status: status.parse()?,
        supervisor_pid: row.get("supervisor_pid")?,
        process_group_id: row.get("process_group_id")?,
        supervisor_identity: row.get("supervisor_identity")?,
        supervisor_birth_time: row.get("supervisor_birth_time")?,
        startup_owner: row.get("startup_owner_pid_identity")?,
        startup_birth_time: row.get("startup_owner_birth_time")?,
        heartbeat_at: row.get("heartbeat_at")?,
    })
}

/// Atomically recheck captured ownership evidence and write the terminal loss.
fn guarded_lost(
    store: &mut Store,
    expected: &Candidate,
    failure_kind: &str,
    failure_text: &str,
    evidence: serde_json::Value,
    checked_at: f64,
) -> Result<bool> {
    let tx = store
        .conn
        .transaction_with_behavior(TransactionBehavior::Immediate)?;
    let current = {
        let mut statement = tx.prepare("SELECT * FROM agents WHERE id=?")?;
        let mut rows = statement.query([expected.id.as_str()])?;
        rows.next()?.map(read_candidate).transpose()?
    };
    let Some(current) = current else {
        tx.commit()?;
        return Ok(false);
    };
    if !same_ownership(expected, &current)
        || current.status.terminal()
        || (expected.supervisor_pid.is_some()
            && current
                .heartbeat_at
                .is_none_or(|heartbeat| heartbeat > checked_at))
    {
        tx.commit()?;
        return Ok(false);
    }
    current.status.transition(Status::Lost)?;
    let finished_at = now();
    tx.execute(
        "UPDATE agents SET status='lost',finished_at=?,failure_kind=?,failure_text=? WHERE id=?",
        params![
            finished_at,
            failure_kind,
            failure_text,
            expected.id.as_str()
        ],
    )?;
    tx.execute(
        "UPDATE attempts SET state='lost',finished_at=? WHERE agent_id=?",
        params![finished_at, expected.id.as_str()],
    )?;
    tx.execute(
        "INSERT INTO events(agent_id,at,kind,from_status,to_status,data_json) VALUES(?,?,?,?,?,?)",
        params![
            expected.id.as_str(),
            finished_at,
            "status",
            current.status.as_str(),
            "lost",
            json!({"failure_kind":failure_kind}).to_string()
        ],
    )?;
    let terminal_event_seq = tx.last_insert_rowid();
    tx.execute(
        "INSERT INTO events(agent_id,at,kind,from_status,to_status,data_json) VALUES(?,?,?,?,?,?)",
        params![
            expected.id.as_str(),
            finished_at,
            "reconciled_lost",
            current.status.as_str(),
            "lost",
            evidence.to_string()
        ],
    )?;
    if let Some(session) = tx.query_row(
        "SELECT orchestrator_session_id FROM agents WHERE id=?",
        [expected.id.as_str()],
        |row| row.get::<_, Option<String>>(0),
    )? {
        tx.execute(
            "INSERT INTO deliveries(id,agent_id,orchestrator_session_id,terminal_event_seq,state,next_attempt_at) VALUES(?,?,?,?,'pending',?)",
            params![format!("ntf_{}", uuid::Uuid::new_v4().simple()), expected.id.as_str(), session, terminal_event_seq, finished_at],
        )?;
    }
    tx.commit()?;
    Ok(true)
}

/// Compare only the immutable ownership facts needed by the specific sweep.
fn same_ownership(expected: &Candidate, current: &Candidate) -> bool {
    if expected.supervisor_pid.is_some() {
        return expected.supervisor_pid == current.supervisor_pid
            && expected.process_group_id == current.process_group_id
            && expected.supervisor_identity == current.supervisor_identity
            && expected.supervisor_birth_time == current.supervisor_birth_time;
    }
    expected.status == Status::Starting
        && current.status == Status::Starting
        && current.supervisor_pid.is_none()
        && current.process_group_id.is_none()
        && current.supervisor_identity.is_none()
        && expected.startup_owner == current.startup_owner
        && expected.startup_birth_time == current.startup_birth_time
}
