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
    birth: Option<f64>,
}

/// Reconcile up to `limit` active rows with the native PID-reuse-aware observer.
///
/// The return value contains only rows that durably became `lost`.  `limit`
/// must be between one and 1000.  `Unknown`, `Denied`, `Alive`, and incomplete
/// ownership evidence leave rows untouched. A verified-dead READY supervisor
/// can be lost before an engine group exists; its attempt cleanup remains
/// separately proof-gated.
pub fn reconcile(store: &mut Store, limit: usize) -> Result<Vec<AgentId>> {
    reconcile_with(store, limit, process::observe)
}

/// Applies waitpid proof to one agent without probing or signalling any group.
pub fn reconcile_reaped_agent(
    store: &mut Store,
    id: &AgentId,
    supervisor_pid: i32,
    checked_at: f64,
) -> Result<bool> {
    if supervisor_pid <= 1 || !checked_at.is_finite() || checked_at < 0.0 {
        return Err(invalid("invalid reaped-agent proof"));
    }
    let Some(row) = read_candidates(store, "SELECT * FROM agents WHERE id=?", [id.as_str()])?
        .into_iter()
        .next()
    else {
        return Ok(false);
    };
    if row.supervisor_pid.is_some_and(|pid| pid != supervisor_pid) {
        return Ok(false);
    }
    if row.status.terminal() {
        return Ok(false);
    }
    guarded_lost(
        store,
        &row,
        "supervisor_dead",
        "detached supervisor exited",
        json!({"verdict":"reaped","supervisor_pid":supervisor_pid}),
        checked_at,
    )
}

/// Marks only active rows owned by one reaped supervisor as lost.
pub fn reconcile_reaped_supervisor(
    store: &mut Store,
    supervisor_pid: i32,
    checked_at: f64,
    limit: usize,
) -> Result<Vec<AgentId>> {
    if supervisor_pid <= 1 || !checked_at.is_finite() || checked_at < 0.0 {
        return Err(invalid("invalid reaped-supervisor proof"));
    }
    if !(1..=1000).contains(&limit) {
        return Err(invalid("reconciliation limit must be 1..1000"));
    }
    let rows = read_candidates(
        store,
        "SELECT * FROM agents WHERE supervisor_pid=? AND status IN ('created','starting','running','cancelling') ORDER BY created_at,id LIMIT ?",
        params![supervisor_pid, limit as i64],
    )?;
    let mut changed = Vec::new();
    for row in rows {
        if row.process_group_id.is_none() || row.supervisor_identity.is_none() {
            continue;
        }
        if guarded_lost(
            store,
            &row,
            "supervisor_dead",
            "detached supervisor exited",
            json!({"verdict":"dead","supervisor_pid":supervisor_pid,"process_group_id":row.process_group_id}),
            checked_at,
        )? {
            changed.push(row.id);
        }
    }
    Ok(changed)
}

/// Reconcile rows using `observe`, which exists to make recovery evidence testable.
///
/// `observe` receives the PID plus its immutable token/birth evidence and must
/// return a platform observation.  Production calls [`reconcile`]; callers
/// should not use this hook to treat missing evidence as death. A starting run
/// with a READY supervisor but no engine group can become lost when that exact
/// supervisor is dead; its attempt ownership is settled separately on proof.
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
        let state = observe(Some(owner.pid), Some(&owner.token), owner.birth);
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
        let (Some(pid), Some(identity)) = (row.supervisor_pid, row.supervisor_identity.as_deref())
        else {
            continue;
        };
        if row.process_group_id.is_none() && row.status != Status::Starting {
            continue;
        }
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
            json!({"verdict": state_name(state), "supervisor_pid": pid, "process_group_id": row.process_group_id}),
            now(),
        )? {
            changed.push(row.id);
        }
    }
    release_orphaned_attempts(store, limit)?;
    Ok(changed)
}

/// Grace between SIGTERM and SIGKILL when recovering an orphaned attempt.
const ORPHAN_GRACE: std::time::Duration = std::time::Duration::from_millis(500);

/// One provider attempt still owned by a terminal logical run.
struct OwnedAttempt {
    /// Attempt id.
    id: String,
    /// Owning logical agent.
    agent: String,
    /// Lifecycle phase (`prepared`, `spawning`, `cleanup_complete`, ...).
    phase: Option<String>,
    /// Recorded leader token, once a child was spawned.
    token: Option<String>,
    /// Recorded leader birth time.
    birth: Option<f64>,
    /// Recorded cleanup proof, when the supervisor finished it.
    proof: Option<String>,
    /// The run's recorded process group (the attempt leader's pid).
    group: Option<i32>,
}

/// Probe only the indexed owning agent's history for a recorded unresolved outcome.
///
/// The attempt and reason guards still enforce deduplication, while the agent
/// key prevents a write transaction from scanning unrelated transcript events.
const UNRESOLVED_EVENT_EXISTS_SQL: &str =
    "SELECT EXISTS(SELECT 1 FROM events WHERE agent_id=? AND attempt_id=? AND kind='attempt_cleanup_unresolved' \
     AND json_extract(data_json,'$.reason')=?)";

/// Releases provider attempts that a terminal logical run still owns (for
/// example a switched attempt whose supervisor died), only on proof:
///
/// * a recorded confirmed cleanup proof is simply honoured;
/// * a `prepared` attempt that never recorded a child closes with exact
///   never-spawned evidence;
/// * a recorded leader that still observes as the same process (token and
///   birth) is re-adopted and its verified group terminated; ownership is
///   released only when the cleanup evidence is confirmed.
///
/// A dead, reused, unknown or denied leader is never signalled. When
/// cleanup cannot be proven the attempt stays owned and one typed
/// `attempt_cleanup_unresolved` event records why; unknown or denied
/// observations are simply retried by the next periodic pass. At most
/// `limit` attempts are examined per call, fairly: a persistent cursor
/// resumes after the last attempt examined and wraps around. Process
/// observation and signalling happen outside any write transaction; each
/// release is its own short transaction. Like the terminal path, a release
/// does not advance `quota_capacity_revision`: admission recounts active
/// reservations inside its own transaction.
fn release_orphaned_attempts(store: &mut Store, limit: usize) -> Result<()> {
    // Fair paging over the existing reconciliation cursor: each pass resumes
    // after the last attempt it examined and wraps around, so a permanently
    // unprovable old attempt cannot starve newer releasable ones.
    let select = "SELECT t.id,t.agent_id,t.phase,t.process_identity,t.process_birth_time,t.cleanup_proof_json,a.process_group_id,t.created_at \
         FROM attempts t JOIN agents a ON a.id=t.agent_id \
         WHERE t.ownership_active=1 AND a.status IN ('succeeded','failed','cancelled','lost','timed_out')";
    let cursor: Option<(f64, String)> = store
        .conn
        .query_row(
            "SELECT created_at,agent_id FROM reconciliation_cursors WHERE name='orphaned_attempts'",
            [],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .optional()?;
    let page = |tail: &str, args: &[&dyn rusqlite::ToSql]| -> Result<Vec<(OwnedAttempt, f64)>> {
        Ok(store
            .conn
            .prepare(&format!("{select}{tail}"))?
            .query_map(args, |row| {
                Ok((
                    OwnedAttempt {
                        id: row.get(0)?,
                        agent: row.get(1)?,
                        phase: row.get(2)?,
                        token: row.get(3)?,
                        birth: row.get(4)?,
                        proof: row.get(5)?,
                        group: row.get(6)?,
                    },
                    row.get(7)?,
                ))
            })?
            .collect::<rusqlite::Result<_>>()?)
    };
    let limit = limit as i64;
    let mut rows = match &cursor {
        Some((at, id)) => page(
            " AND (t.created_at > ?1 OR (t.created_at = ?1 AND t.id > ?2)) ORDER BY t.created_at,t.id LIMIT ?3",
            &[at, id, &limit],
        )?,
        None => page(" ORDER BY t.created_at,t.id LIMIT ?1", &[&limit])?,
    };
    if let Some((at, id)) = &cursor {
        let remaining = limit - rows.len() as i64;
        if remaining > 0 {
            rows.extend(page(
                " AND (t.created_at < ?1 OR (t.created_at = ?1 AND t.id <= ?2)) ORDER BY t.created_at,t.id LIMIT ?3",
                &[at, id, &remaining],
            )?);
        }
    }
    if let Some((last, created)) = rows.last() {
        store.conn.execute(
            "INSERT INTO reconciliation_cursors(name,created_at,agent_id) VALUES('orphaned_attempts',?,?) \
             ON CONFLICT(name) DO UPDATE SET created_at=excluded.created_at,agent_id=excluded.agent_id",
            params![created, last.id],
        )?;
    }
    let rows: Vec<OwnedAttempt> = rows.into_iter().map(|(attempt, _)| attempt).collect();
    for attempt in rows {
        let confirmed = attempt
            .proof
            .as_deref()
            .and_then(|proof| serde_json::from_str::<serde_json::Value>(proof).ok())
            .is_some_and(|proof| proof["confirmed"] == true || proof["never_spawned"] == true);
        let outcome = match (&attempt.token, attempt.birth, attempt.group) {
            _ if confirmed && attempt.phase.as_deref() == Some("cleanup_complete") => Ok(None),
            (None, _, _) if attempt.phase.as_deref() == Some("prepared") => {
                Ok(Some(json!({"never_spawned":true,"reconciled":true})))
            }
            (Some(token), Some(birth), Some(pid)) if pid > 1 => {
                match process::observe(Some(pid), Some(token), Some(birth)) {
                    ProcessState::Alive => {
                        let mut owned = process::OwnedProcess::adopt(process::Identity {
                            pid,
                            ppid: 0,
                            group: pid,
                            birth,
                            token: token.clone(),
                            zombie: false,
                        });
                        match owned.cleanup_blocking(ORPHAN_GRACE) {
                            Ok(cleanup) if cleanup.confirmed => {
                                let mut proof = serde_json::to_value(&cleanup)?;
                                proof["reconciled"] = json!(true);
                                Ok(Some(proof))
                            }
                            Ok(_) => Err("cleanup_unconfirmed"),
                            Err(_) => continue,
                        }
                    }
                    ProcessState::Dead | ProcessState::Reused => {
                        Err("leader_gone_descendants_unverifiable")
                    }
                    ProcessState::Unknown | ProcessState::Denied | ProcessState::NotStarted => {
                        continue
                    }
                }
            }
            _ => Err("no_process_evidence"),
        };
        let tx = store
            .conn
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        match outcome {
            Ok(proof) => {
                let released = match &proof {
                    Some(proof) => tx.execute(
                        "UPDATE attempts SET cleanup_proof_json=?,phase='cleanup_complete',ownership_active=0 \
                         WHERE id=? AND ownership_active=1",
                        params![proof.to_string(), attempt.id],
                    )?,
                    None => tx.execute(
                        "UPDATE attempts SET ownership_active=0 WHERE id=? AND ownership_active=1",
                        [&attempt.id],
                    )?,
                };
                if released == 1 {
                    tx.execute(
                        "INSERT INTO events(agent_id,attempt_id,at,kind,data_json) VALUES(?,?,?,?,?)",
                        params![attempt.agent, attempt.id, now(), "attempt_cleanup_reconciled",
                            proof.unwrap_or_else(|| json!({"recorded_proof":true})).to_string()],
                    )?;
                }
            }
            Err(reason) => {
                let recorded: bool = tx.query_row(
                    UNRESOLVED_EVENT_EXISTS_SQL,
                    params![attempt.agent, attempt.id, reason],
                    |row| row.get(0),
                )?;
                if !recorded {
                    tx.execute(
                        "INSERT INTO events(agent_id,attempt_id,at,kind,data_json) VALUES(?,?,?,?,?)",
                        params![attempt.agent, attempt.id, now(), "attempt_cleanup_unresolved",
                            json!({"reason":reason}).to_string()],
                    )?;
                }
            }
        }
        tx.commit()?;
    }
    Ok(())
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
    let raw = row.startup_owner.as_deref()?;
    if let Ok(owner) = serde_json::from_str::<StartupOwner>(raw) {
        return (owner.pid > 1
            && owner
                .birth
                .or(row.startup_birth_time)
                .is_some_and(|birth| birth.is_finite() && birth >= 0.0)
            && !owner.token.is_empty())
        .then(|| StartupOwner {
            birth: owner.birth.or(row.startup_birth_time),
            ..owner
        });
    }
    let (pid, _) = raw.split_once(' ')?;
    let pid = pid.parse().ok()?;
    (pid > 1
        && row
            .startup_birth_time
            .is_some_and(|birth| birth.is_finite() && birth >= 0.0))
    .then(|| StartupOwner {
        pid,
        token: raw.to_owned(),
        birth: row.startup_birth_time,
    })
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
///
/// The lost status, events, delivery and terminal results for pending commands
/// commit in one transaction; claimed commands are left for their owner.
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
    let finished_at = checked_at;
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
        // Only unfinished attempts are lost; an attempt already closed (for
        // example exhausted before an in-place account switch) keeps its state.
        "UPDATE attempts SET state='lost',finished_at=? WHERE agent_id=? AND finished_at IS NULL",
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
    // Pending commands are finalized in this same transaction: future sweeps skip
    // terminal rows, so a crash between the lost commit and a separate drain
    // would leave them pending forever. Claimed commands stay untouched.
    crate::commands::complete_pending_in(&tx, &expected.id, finished_at)?;
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

/// Checks that orphaned-attempt diagnostics use bounded indexed history reads.
#[cfg(test)]
mod tests {
    use super::UNRESOLVED_EVENT_EXISTS_SQL;

    /// The deduplication probe must use one agent's event index instead of
    /// scanning the entire durable transcript while holding a write lease.
    #[test]
    fn unresolved_cleanup_probe_uses_agent_index() {
        let home = tempfile::tempdir().unwrap();
        let store = agent_run_store::Store::initialize(home.path()).unwrap();
        let mut statement = store
            .conn
            .prepare(&format!("EXPLAIN QUERY PLAN {UNRESOLVED_EVENT_EXISTS_SQL}"))
            .unwrap();
        let plan = statement
            .query_map(rusqlite::params!["agent", "attempt", "reason"], |row| {
                row.get::<_, String>(3)
            })
            .unwrap()
            .collect::<std::result::Result<Vec<_>, _>>()
            .unwrap();
        assert!(
            plan.iter()
                .any(|step| step.contains("SEARCH events USING INDEX idx_events_agent_seq")),
            "{plan:?}"
        );
        assert!(
            !plan.iter().any(|step| step.contains("SCAN events")),
            "{plan:?}"
        );
    }
}
