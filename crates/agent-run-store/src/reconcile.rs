//! Proof-gated reconciliation units of the sole writer (rows 59-62).
//!
//! The broker's reconciler observes processes and prepares cleanup proofs
//! outside any transaction; these units recheck the recorded ownership
//! inside the writer's transaction and apply exactly the guarded change,
//! never by age alone.

use crate::Store;
use agent_run_domain::{
    Result,
    domain::{AgentId, Status, now},
    error::invalid,
};
use rusqlite::{OptionalExtension, params};
use serde_json::json;

/// Closed reasons an orphaned attempt's cleanup stays unresolved.
const UNRESOLVED_REASONS: [&str; 4] = [
    "stored_process_identity_mismatch",
    "cleanup_unconfirmed",
    "leader_gone_descendants_unverifiable",
    "no_process_evidence",
];

/// Durable ownership facts of one execution, compared with an expectation.
struct Ownership {
    /// Status.
    status: Status,
    /// Supervisor pid.
    supervisor_pid: Option<i32>,
    /// Supervisor identity token.
    supervisor_identity: Option<String>,
    /// Supervisor birth time.
    supervisor_birth_time: Option<f64>,
    /// Process group.
    process_group_id: Option<i32>,
    /// Startup owner evidence.
    startup_owner: Option<String>,
    /// Startup owner birth time.
    startup_birth_time: Option<f64>,
    /// Last heartbeat.
    heartbeat_at: Option<f64>,
    /// Bound orchestrator session.
    session: Option<String>,
}

/// Reads the current ownership facts of `id`, if the execution exists.
fn ownership(conn: &rusqlite::Connection, id: &AgentId) -> Result<Option<Ownership>> {
    let row = conn
        .query_row(
            "SELECT status,supervisor_pid,supervisor_identity,supervisor_birth_time,process_group_id,\
             startup_owner_pid_identity,startup_owner_birth_time,heartbeat_at,orchestrator_session_id FROM agents WHERE id=?",
            [id.as_str()],
            |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    Ownership {
                        status: Status::Created,
                        supervisor_pid: row.get(1)?,
                        supervisor_identity: row.get(2)?,
                        supervisor_birth_time: row.get(3)?,
                        process_group_id: row.get(4)?,
                        startup_owner: row.get(5)?,
                        startup_birth_time: row.get(6)?,
                        heartbeat_at: row.get(7)?,
                        session: row.get(8)?,
                    },
                ))
            },
        )
        .optional()?;
    row.map(|(status, mut ownership)| {
        ownership.status = status.parse()?;
        Ok(ownership)
    })
    .transpose()
}

/// Whether the current ownership is exactly the expected one: the same
/// supervisor identity and group, or for an unbound start the same startup
/// owner with no supervisor recorded.
fn same_ownership(expected: &crate::ops::ExpectedOwnership, current: &Ownership) -> bool {
    if expected.supervisor_pid.is_some() {
        return expected.supervisor_pid == current.supervisor_pid
            && expected.process_group_id == current.process_group_id
            && expected.supervisor_identity == current.supervisor_identity
            && expected.supervisor_birth_time == current.supervisor_birth_time;
    }
    current.status == Status::Starting
        && current.supervisor_pid.is_none()
        && current.process_group_id.is_none()
        && current.supervisor_identity.is_none()
        && expected.startup_owner == current.startup_owner
        && expected.startup_birth_time == current.startup_birth_time
}

impl Store<'_> {
    /// Advances one reconciliation cursor (row 59).
    pub fn advance_reconcile_cursor(&self, op: &crate::ops::ReconcileReapedAgent) -> Result<()> {
        let key = match &op.attempt {
            Some(attempt) => {
                if op.cursor != "orphaned_attempts"
                    || attempt.is_empty()
                    || attempt.len() > 256
                    || !self.conn.query_row(
                        "SELECT EXISTS(SELECT 1 FROM attempts WHERE id=?1 AND agent_id=?2)",
                        params![attempt, op.execution.as_str()],
                        |row| row.get::<_, bool>(0),
                    )?
                {
                    return Err(invalid("orphan cursor attempt must belong to execution"));
                }
                attempt.as_str()
            }
            None => op.execution.as_str(),
        };
        if op.cursor.is_empty() || op.cursor.len() > 64 || !op.created_at.is_finite() {
            return Err(invalid("invalid reconciliation cursor"));
        }
        self.conn.execute(
            "INSERT INTO reconciliation_cursors(name,created_at,agent_id) VALUES(?,?,?) \
             ON CONFLICT(name) DO UPDATE SET created_at=excluded.created_at,agent_id=excluded.agent_id",
            params![op.cursor, op.created_at, key],
        )?;
        Ok(())
    }

    /// Releases an orphaned attempt on proof, or records one closed
    /// unresolved cleanup reason once (row 60). Only an ownership-active
    /// attempt of a terminal execution is eligible. A release with a new
    /// proof needs either a confirmed cleanup proof of a spawned attempt, or
    /// never-spawned evidence of a `prepared` attempt that recorded no
    /// process identity; a release without a new proof needs a stored
    /// confirmed (or never-spawned) proof with cleanup complete. Anything
    /// else refuses without change. Returns whether ownership was released.
    pub fn release_orphaned_attempt(
        &self,
        op: &crate::ops::ReleaseOrphanedAttempt,
    ) -> Result<bool> {
        let tx = crate::writer::WriteTx::begin(&self.conn)?;
        /// Recorded attempt facts: execution, its status, ownership flag,
        /// phase, process identity and stored cleanup proof.
        type Row = (
            String,
            String,
            i64,
            Option<String>,
            Option<String>,
            Option<String>,
        );
        let row: Option<Row> = tx
            .query_row(
                "SELECT t.agent_id,a.status,t.ownership_active,t.phase,t.process_identity,t.cleanup_proof_json \
                 FROM attempts t JOIN agents a ON a.id=t.agent_id WHERE t.id=?",
                [&op.attempt],
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
            )
            .optional()?;
        let Some((owner, status, active, phase, identity, stored)) = row else {
            return Err(agent_run_domain::Error::NotFound(op.attempt.clone()));
        };
        if owner != op.execution.as_str() {
            return Err(invalid("attempt does not belong to the execution"));
        }
        if !status.parse::<Status>()?.terminal() {
            return Err(agent_run_domain::Error::Transition(
                "orphaned attempt release requires a terminal execution".into(),
            ));
        }
        if active != 1 {
            tx.commit()?;
            return Ok(false);
        }
        let confirmed = |proof: &serde_json::Value| proof["confirmed"] == true;
        let never_spawned = |proof: &serde_json::Value| proof["never_spawned"] == true;
        if let crate::ops::OrphanOutcome::Release { proof } = &op.outcome {
            let eligible = match proof {
                Some(proof) if never_spawned(proof) => {
                    phase.as_deref() == Some("prepared") && identity.is_none()
                }
                Some(proof) => confirmed(proof) && identity.is_some(),
                None => {
                    phase.as_deref() == Some("cleanup_complete")
                        && stored
                            .as_deref()
                            .and_then(|proof| serde_json::from_str::<serde_json::Value>(proof).ok())
                            .is_some_and(|proof| confirmed(&proof) || never_spawned(&proof))
                }
            };
            if !eligible {
                return Err(invalid(
                    "orphaned attempt release lacks matching confirmed proof",
                ));
            }
        }
        let released = match &op.outcome {
            crate::ops::OrphanOutcome::Release { proof } => {
                let changed = match proof {
                    Some(proof) => tx.execute(
                        "UPDATE attempts SET cleanup_proof_json=?,phase='cleanup_complete',ownership_active=0 \
                         WHERE id=? AND ownership_active=1",
                        params![proof.to_string(), op.attempt],
                    )?,
                    None => tx.execute(
                        "UPDATE attempts SET ownership_active=0 WHERE id=? AND ownership_active=1",
                        [&op.attempt],
                    )?,
                };
                if changed == 1 {
                    tx.execute(
                        "INSERT INTO events(agent_id,attempt_id,at,kind,data_json) VALUES(?,?,?,?,?)",
                        params![
                            op.execution.as_str(),
                            op.attempt,
                            now(),
                            "attempt_cleanup_reconciled",
                            proof
                                .clone()
                                .unwrap_or_else(|| json!({"recorded_proof": true}))
                                .to_string()
                        ],
                    )?;
                }
                changed == 1
            }
            crate::ops::OrphanOutcome::Unresolved { reason } => {
                if !UNRESOLVED_REASONS.contains(&reason.as_str()) {
                    return Err(invalid("unknown unresolved cleanup reason"));
                }
                let recorded: bool = tx.query_row(
                    "SELECT EXISTS(SELECT 1 FROM events WHERE agent_id=? AND attempt_id=? AND kind='attempt_cleanup_unresolved' \
                     AND json_extract(data_json,'$.reason')=?)",
                    params![op.execution.as_str(), op.attempt, reason],
                    |row| row.get(0),
                )?;
                if !recorded {
                    tx.execute(
                        "INSERT INTO events(agent_id,attempt_id,at,kind,data_json) VALUES(?,?,?,?,?)",
                        params![
                            op.execution.as_str(),
                            op.attempt,
                            now(),
                            "attempt_cleanup_unresolved",
                            json!({"reason": reason}).to_string()
                        ],
                    )?;
                }
                false
            }
        };
        tx.commit()?;
        Ok(released)
    }

    /// Proof-gated lost transition (row 61): only while the recorded
    /// ownership is exactly the expected one, the execution is not terminal
    /// and (for a bound supervisor) no heartbeat is newer than the check.
    /// Writes the lost status, unfinished attempts, the status and evidence
    /// events, one completion notice for a bound session, and completes
    /// pending commands in the same transaction. Returns whether it changed.
    pub fn mark_lost(&mut self, op: &crate::ops::MarkLost) -> Result<bool> {
        if !op.checked_at.is_finite()
            || op.failure_kind.is_empty()
            || op.failure_kind.len() > 64
            || op.failure_text.len() > 4096
        {
            return Err(invalid("invalid lost transition"));
        }
        let id = &op.expected.execution;
        let tx = crate::writer::WriteTx::begin(&self.conn)?;
        let Some(current) = ownership(&tx, id)? else {
            tx.commit()?;
            return Ok(false);
        };
        if !same_ownership(&op.expected, &current)
            || current.status.terminal()
            || (op.expected.supervisor_pid.is_some()
                && current
                    .heartbeat_at
                    .is_none_or(|heartbeat| heartbeat > op.checked_at))
        {
            tx.commit()?;
            return Ok(false);
        }
        current.status.transition(Status::Lost)?;
        let at = op.checked_at;
        tx.execute(
            "UPDATE agents SET status='lost',finished_at=?,failure_kind=?,failure_text=? WHERE id=?",
            params![at, op.failure_kind, op.failure_text, id.as_str()],
        )?;
        tx.execute(
            "UPDATE attempts SET state='lost',finished_at=? WHERE agent_id=? AND finished_at IS NULL",
            params![at, id.as_str()],
        )?;
        tx.execute(
            "INSERT INTO events(agent_id,at,kind,from_status,to_status,data_json) VALUES(?,?,?,?,?,?)",
            params![
                id.as_str(),
                at,
                "status",
                current.status.as_str(),
                "lost",
                json!({"failure_kind": op.failure_kind}).to_string()
            ],
        )?;
        let terminal_event_seq = tx.last_insert_rowid();
        tx.execute(
            "INSERT INTO events(agent_id,at,kind,from_status,to_status,data_json) VALUES(?,?,?,?,?,?)",
            params![
                id.as_str(),
                at,
                "reconciled_lost",
                current.status.as_str(),
                "lost",
                op.evidence.to_string()
            ],
        )?;
        if let Some(session) = &current.session {
            tx.execute(
                "INSERT INTO deliveries(id,agent_id,orchestrator_session_id,terminal_event_seq,state,next_attempt_at) VALUES(?,?,?,?,'pending',?)",
                params![
                    format!("ntf_{}", uuid::Uuid::new_v4().simple()),
                    id.as_str(),
                    session,
                    terminal_event_seq,
                    at
                ],
            )?;
        }
        tx.commit()?;
        self.complete_terminal_commands(id, at)?;
        Ok(true)
    }
}

impl Store<'_> {
    /// Reconciles an acknowledged explicit finish after its supervisor died
    /// (row 62). The broker proved the supervisor gone, ran any cleanup and
    /// sealed the immutable summary before submission; this unit rechecks the
    /// recorded ownership, the owned unfinished attempt and the receipt-
    /// observed intent, stores a new confirmed cleanup proof once, keeps an
    /// existing history seal (marking it unavailable only when absent),
    /// commits the verified terminal outcome and completes pending commands.
    /// Returns `false` without change when any recheck fails.
    pub fn recover_finish(&mut self, op: &crate::ops::RecoverFinish) -> Result<bool> {
        let id = &op.expected.execution;
        if !op.checked_at.is_finite() {
            return Err(invalid("invalid finish recovery time"));
        }
        let Some(answer) = &op.proof else {
            return Err(invalid("finish recovery requires the sealed answer proof"));
        };
        let cleanup: agent_run_platform::process::Cleanup =
            serde_json::from_value(op.cleanup.clone())?;
        if !cleanup.confirmed || !cleanup.group_gone {
            return Err(invalid("finish recovery requires confirmed group cleanup"));
        }
        let row = self.get(id)?;
        if !row.request.explicit_finish || row.status.terminal() {
            return Ok(false);
        }
        let Some(intent) = self.worker_finish_intent(id, true)? else {
            return Ok(false);
        };
        let Some(current) = ownership(&self.conn, id)? else {
            return Ok(false);
        };
        if !same_ownership(&op.expected, &current) {
            return Ok(false);
        }
        let attempt: Option<(String, Option<String>, String)> = self
            .conn
            .query_row(
                "SELECT id,cleanup_proof_json,adapter_state_json FROM attempts \
                 WHERE agent_id=? AND ownership_active=1 AND finished_at IS NULL",
                [id.as_str()],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .optional()?;
        let Some((attempt, saved, history)) = attempt else {
            return Ok(false);
        };
        let recorded = saved
            .as_deref()
            .map(serde_json::from_str::<agent_run_platform::process::Cleanup>)
            .transpose()?
            .is_some_and(|proof| proof.confirmed && proof.group_gone);
        if !recorded {
            self.provider_cleanup(id, &attempt, &cleanup)?;
            self.event(id, "process_cleanup", &op.cleanup)?;
        }
        let mut history: serde_json::Value = serde_json::from_str(&history)?;
        if !history.is_object() {
            return Err(agent_run_domain::Error::Integrity(
                "attempt history metadata is not an object".into(),
            ));
        }
        if history.get("native_history").is_none()
            && history.get("native_history_unavailable").is_none()
        {
            history["native_history_unavailable"] =
                json!("supervisor exited before native history was sealed");
            self.provider_history(id, &attempt, &history)?;
        }
        let mut outcome = match intent.status {
            agent_run_domain::worker::FinishStatus::Done => {
                agent_run_domain::domain::Outcome::success(row.runtime_session_id.clone())
            }
            agent_run_domain::worker::FinishStatus::Blocked => {
                agent_run_domain::domain::Outcome::failure("worker_blocked")
            }
            agent_run_domain::worker::FinishStatus::Failed => {
                agent_run_domain::domain::Outcome::failure("worker_failed")
            }
        };
        outcome.runtime_session_id = row.runtime_session_id.clone();
        if self.cancel_pending(id)? {
            outcome.status = Status::Cancelled;
        }
        let evidence = agent_run_platform::verify::AnswerProof::sealed(answer);
        let outcome = agent_run_platform::verify::verify_completion(
            Some(outcome),
            None,
            Some(&evidence),
            true,
            self.last_progress(id)?,
            op.checked_at,
            agent_run_platform::verify::DEFAULT_SILENCE_THRESHOLD_SECONDS,
        )?;
        self.event(
            id,
            "worker_finish_recovered_v1",
            &json!({"receipt_observed": true, "cleanup_confirmed": true}),
        )?;
        crate::terminal::commit_terminal(self, id, &outcome, Some(answer), None)?;
        self.complete_terminal_commands(id, op.checked_at)?;
        Ok(true)
    }
}
