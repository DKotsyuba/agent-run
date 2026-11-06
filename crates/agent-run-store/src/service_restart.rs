//! The durable admission barrier and affected agents of one service restart.

use crate::{Store, writer::WriteTx};
use agent_run_domain::{
    Error, Result,
    domain::{AgentId, OrchestratorRef},
    error::invalid,
};
use rusqlite::{Connection, OptionalExtension, params};
use serde_json::json;

const WARNING_DELAY_NS: i64 = 30_000_000_000;
const RESUME_CONFIRM_NS: i64 = 60_000_000_000;
const NOTICE_WAIT_NS: i64 = 10_000_000_000;
const FIRST_NOTICE: &str = "Agent Run is preparing to restart. Stop your work and call finish with status done and a concise summary of completed and remaining work. This is a temporary interruption; you will resume after the restart. Do not include credentials.";
const SECOND_NOTICE: &str = "Agent Run is restarting. Call finish immediately with status done and the context needed to continue. If you do not stop within 30 seconds, your work will be interrupted.";
const BLOCKED_PREFIX: &str = "Agent Run is restarting. New agent starts are temporarily blocked; please wait. [agent-run-restart:";
const BEFORE_NOTICE: &str = "Agent Run is preparing a service restart. Your agents will pause and resume afterwards. You will receive their restart results.";
const AFTER_NOTICE: &str =
    "Agent Run has finished its restart procedure. New agent starts are available again.";

/// Reserve an exact first line for this restart's technical finish.
pub(crate) fn finish_marker(restart_id: i64) -> String {
    format!("[agent-run-restart-finish:{restart_id}]")
}

/// Distinguish a definite refusal from a continuation whose outcome is unknown.
fn resume_result(status: &str, reason: Option<&str>) -> String {
    match status {
        "started" => "resumed".into(),
        "finished" => "finished before restart".into(),
        "cancelled" => "cancelled by the orchestrator".into(),
        "failed" => format!("failed — {}", reason.unwrap_or("reason unavailable")),
        _ => format!("not confirmed — {}", reason.unwrap_or("reason unavailable")),
    }
}

/// Add a restart payload to the existing delivery outbox in the caller's transaction.
fn enqueue_notice(
    conn: &Connection,
    restart: i64,
    transport: &str,
    session: &str,
    stage: &str,
    message: &str,
) -> Result<()> {
    let exists: bool = conn.query_row(
        "SELECT EXISTS(SELECT 1 FROM service_restart_notices WHERE restart_id=? AND transport=? AND external_session_id=? AND stage=?)",
        params![restart,transport,session,stage], |row| row.get(0),
    )?;
    if exists {
        return Ok(());
    }
    let at = agent_run_domain::domain::now();
    let reference = OrchestratorRef {
        transport: transport.into(),
        external_session_id: session.into(),
        external_turn_id: None,
    };
    let session_id = crate::session_for_reference(conn, &reference, at)?;
    let delivery = format!("ntf_{}", uuid::Uuid::new_v4().simple());
    conn.execute("INSERT INTO deliveries(id,agent_id,orchestrator_session_id,state,next_attempt_at) VALUES(?,NULL,?,'pending',?)",
        params![delivery,session_id,at])?;
    conn.execute("INSERT INTO service_restart_notices(restart_id,transport,external_session_id,stage,message,delivery_id) VALUES(?,?,?,?,?,?)",
        params![restart,transport,session,stage,message,delivery])?;
    Ok(())
}

/// Describe a blocked start and retain its restart identity for the host hook.
pub fn blocked_message(restart_id: i64) -> String {
    format!("{BLOCKED_PREFIX}{restart_id}]")
}

/// Extract the positive restart identity from the broker's bounded refusal.
pub fn blocked_restart_id(message: &str) -> Option<i64> {
    let message = message.trim_start_matches("invalid agent transition: ");
    let tail = message.strip_prefix(BLOCKED_PREFIX)?;
    let (id, _) = tail.split_once(']')?;
    id.parse::<i64>().ok().filter(|id| *id > 0)
}

/// Refuse new admissions while preserving replays of previously accepted work.
pub(crate) fn require_admissions_open(conn: &Connection) -> Result<()> {
    if let Some(id) = active_service_restart(conn)? {
        return Err(Error::Transition(blocked_message(id)));
    }
    Ok(())
}

/// Read the active restart without modifying the admission barrier.
pub fn active_service_restart(conn: &Connection) -> Result<Option<i64>> {
    Ok(conn
        .query_row(
            "SELECT id FROM service_restarts WHERE finished_at IS NULL",
            [],
            |row| row.get(0),
        )
        .optional()?)
}

/// A finish is technical only after the restart warning was queued for this worker.
pub(crate) fn finish_restart(
    conn: &Connection,
    agent: &AgentId,
    attempt: &str,
) -> Result<Option<i64>> {
    Ok(conn.query_row(
        "SELECT r.id FROM service_restart_agents a JOIN service_restarts r ON r.id=a.restart_id \
         JOIN attempts t ON t.agent_id=a.agent_id WHERE a.agent_id=? AND t.id=? AND t.ownership_active=1 \
         AND r.phase='preparing' AND r.finished_at IS NULL AND a.cancelled_before_restart=0 AND a.resume_status='pending' \
         AND a.first_notice_ns IS NOT NULL",
        params![agent.as_str(), attempt], |row| row.get(0),
    ).optional()?)
}

/// A technical stop must never certify a completed agent or pool.
pub(crate) fn is_restart_stop(conn: &Connection, agent: &AgentId) -> Result<bool> {
    Ok(conn.query_row(
        "SELECT EXISTS(SELECT 1 FROM events WHERE agent_id=? AND kind='status' \
         AND json_extract(data_json,'$.service_restart')=1)",
        [agent.as_str()],
        |row| row.get(0),
    )?)
}

/// Current visible restart outcome for a technically stopped execution.
pub fn restart_view(
    conn: &Connection,
    agent: &AgentId,
) -> Result<Option<(String, Option<String>)>> {
    Ok(conn.query_row(
        "SELECT CASE WHEN a.resume_status='cancelled' AND EXISTS(SELECT 1 FROM agents c WHERE c.id=a.resumed_execution_id \
          AND c.status IN ('created','starting','running','cancelling')) THEN 'cancelling' ELSE a.resume_status END,a.resume_error \
         FROM service_restart_agents a JOIN service_restarts r ON r.id=a.restart_id \
         WHERE a.agent_id=? AND ((r.finished_at IS NULL AND a.resume_status='pending') \
         OR a.resume_status IN ('failed','unconfirmed') OR (a.resume_status='cancelled' AND a.cancelled_before_restart=0)) AND EXISTS(SELECT 1 FROM events e \
         WHERE e.agent_id=a.agent_id AND e.kind='status' AND json_extract(e.data_json,'$.service_restart')=1) \
         ORDER BY r.id DESC LIMIT 1",
        [agent.as_str()], |row| Ok((row.get(0)?,row.get(1)?)),
    ).optional()?)
}

/// Attach a captured agent to a host session that bound after capture.
pub(crate) fn bind_captured_agent(
    conn: &Connection,
    agent: &AgentId,
    session_id: &str,
    reference: &OrchestratorRef,
) -> Result<()> {
    let transport = reference.canonical_transport()?;
    let rows = {
        let mut statement = conn.prepare(
            "SELECT a.restart_id,r.phase \
             FROM service_restart_agents a JOIN service_restarts r ON r.id=a.restart_id \
             WHERE a.agent_id=? AND a.orchestrator_session_id IS NULL AND a.cancelled_before_restart=0",
        )?;
        let rows = statement.query_map([agent.as_str()], |row| {
            Ok((row.get::<_, i64>(0)?, row.get::<_, String>(1)?))
        })?;
        rows.collect::<rusqlite::Result<Vec<_>>>()?
    };
    for (restart_id, phase) in rows {
        conn.execute("UPDATE service_restart_agents SET orchestrator_session_id=? WHERE restart_id=? AND agent_id=? AND orchestrator_session_id IS NULL",
            params![session_id,restart_id,agent.as_str()])?;
        conn.execute("INSERT INTO service_restart_recipients(restart_id,transport,external_session_id,had_agents) VALUES(?,?,?,1) \
             ON CONFLICT(restart_id,transport,external_session_id) DO UPDATE SET had_agents=1",
            params![restart_id,transport,reference.external_session_id])?;
        if phase == "complete" {
            queue_restart_result(conn, restart_id, transport, &reference.external_session_id)?;
        } else if phase == "preparing" {
            enqueue_notice(
                conn,
                restart_id,
                transport,
                &reference.external_session_id,
                "before",
                BEFORE_NOTICE,
            )?;
        }
    }
    Ok(())
}

/// Permit only the broker's captured, not-previously-cancelled restart member.
pub(crate) fn require_restart_resume(
    conn: &Connection,
    restart_id: i64,
    agent: &AgentId,
) -> Result<()> {
    let eligible: bool = conn.query_row(
        "SELECT EXISTS(SELECT 1 FROM service_restart_agents a JOIN service_restarts r ON r.id=a.restart_id \
         WHERE r.id=? AND r.phase='recovering' AND r.finished_at IS NULL AND a.agent_id=? \
         AND a.cancelled_before_restart=0 AND a.resume_status='pending' AND a.resumed_execution_id IS NULL)",
        params![restart_id, agent.as_str()], |row| row.get(0),
    )?;
    if !eligible {
        return Err(Error::Conflict);
    }
    Ok(())
}

impl Store<'_> {
    /// Atomically block admission and capture the executions currently in work.
    /// An identical request returns its original restart id; another active
    /// restart is refused. This operation neither sends messages nor stops work.
    pub fn begin_service_restart(&mut self, request_id: &str, at: f64) -> Result<i64> {
        if request_id.trim().is_empty()
            || request_id.len() > 256
            || request_id.chars().any(char::is_control)
            || !at.is_finite()
            || at < 0.0
        {
            return Err(invalid("invalid service restart request"));
        }
        let boot_id = agent_run_platform::monotonic::boot_id()?;
        let tx = WriteTx::begin(&self.conn)?;
        if let Some(id) = tx
            .query_row(
                "SELECT id FROM service_restarts WHERE request_id=?",
                [request_id],
                |row| row.get(0),
            )
            .optional()?
        {
            tx.commit()?;
            return Ok(id);
        }
        if tx.query_row(
            "SELECT EXISTS(SELECT 1 FROM service_restarts WHERE finished_at IS NULL)",
            [],
            |row| row.get::<_, bool>(0),
        )? {
            return Err(Error::Conflict);
        }
        let writer_epoch: Option<i64> = tx
            .query_row("SELECT epoch FROM writer_authority WHERE id=1", [], |row| {
                row.get(0)
            })
            .optional()?;
        tx.execute(
            "INSERT INTO service_restarts(request_id,phase,started_at,boot_id,writer_epoch) VALUES(?,'preparing',?,?,?)",
            params![request_id, at, boot_id, writer_epoch],
        )?;
        let restart_id = tx.last_insert_rowid();
        tx.execute(
            "INSERT INTO service_restart_agents(restart_id,agent_id,orchestrator_session_id,cancelled_before_restart) \
             SELECT ?,a.id,a.orchestrator_session_id, \
             a.status='cancelling' OR EXISTS(SELECT 1 FROM commands c WHERE c.agent_id=a.id AND c.kind='cancel' \
               AND (c.state IN ('pending','claimed') OR (c.state='completed' AND json_extract(c.result_json,'$.accepted')=1))) \
             FROM agents a WHERE a.status IN ('created','starting','running','cancelling') \
             AND NOT EXISTS(SELECT 1 FROM worker_lifecycle l JOIN attempts t ON t.id=l.attempt_id \
               WHERE t.agent_id=a.id AND t.ownership_active=1 AND l.finish_json IS NOT NULL)",
            [restart_id],
        )?;
        tx.execute(
            "UPDATE service_restart_agents SET resume_status='cancelled' WHERE restart_id=? AND cancelled_before_restart=1",
            [restart_id],
        )?;
        tx.execute(
            "INSERT OR IGNORE INTO service_restart_recipients(restart_id,transport,external_session_id,had_agents) \
             SELECT ?,CASE o.transport WHEN 'codex' THEN 'codex_queue' WHEN 'claude' THEN 'claude_uds' ELSE o.transport END, \
             o.external_session_id,1 FROM service_restart_agents a JOIN orchestrator_sessions o ON o.id=a.orchestrator_session_id \
             WHERE a.restart_id=? AND a.cancelled_before_restart=0",
            params![restart_id, restart_id],
        )?;
        let recipients = {
            let mut statement=tx.prepare("SELECT transport,external_session_id FROM service_restart_recipients WHERE restart_id=? AND had_agents=1")?;
            let rows = statement.query_map([restart_id], |row| {
                Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
            })?;
            rows.collect::<rusqlite::Result<Vec<_>>>()?
        };
        for (transport, session) in recipients {
            enqueue_notice(
                &tx,
                restart_id,
                &transport,
                &session,
                "before",
                BEFORE_NOTICE,
            )?;
        }
        tx.commit()?;
        Ok(restart_id)
    }

    /// Record a caller whose start was refused, including one without agents.
    /// A repeated call updates the same recipient and does not create work.
    pub fn remember_service_restart_caller(
        &mut self,
        restart_id: i64,
        reference: &OrchestratorRef,
    ) -> Result<()> {
        reference.validate()?;
        let transport = reference.canonical_transport()?;
        let tx = WriteTx::begin(&self.conn)?;
        let phase: Option<String> = tx
            .query_row(
                "SELECT phase FROM service_restarts WHERE id=?",
                [restart_id],
                |row| row.get(0),
            )
            .optional()?;
        let phase = phase.ok_or_else(|| invalid("unknown service restart"))?;
        tx.execute(
            "INSERT INTO service_restart_recipients(restart_id,transport,external_session_id,tried_start) \
             VALUES(?,?,?,1) ON CONFLICT(restart_id,transport,external_session_id) DO UPDATE SET tried_start=1",
            params![restart_id, transport, reference.external_session_id],
        )?;
        if phase == "complete" {
            queue_restart_result(&tx, restart_id, transport, &reference.external_session_id)?;
        }
        tx.commit()
    }

    /// One caller may invoke the service manager after preparation completes.
    pub fn claim_service_restart(&mut self, restart_id: i64) -> Result<bool> {
        let tx = WriteTx::begin(&self.conn)?;
        let claimed = tx.execute(
            "UPDATE service_restarts SET phase='requesting' WHERE id=? AND phase='ready' \
             AND writer_epoch=(SELECT epoch FROM writer_authority WHERE id=1)",
            [restart_id],
        )? == 1;
        tx.commit()?;
        Ok(claimed)
    }

    /// Enter recovery only after a new broker writer has started.
    pub fn recover_service_restart(&mut self) -> Result<Option<i64>> {
        self.recover_service_restart_for(None)
    }

    /// Resume on the original writer after its service manager failed to restart it.
    pub fn recover_service_restart_after_failure(
        &mut self,
        restart_id: i64,
    ) -> Result<Option<i64>> {
        self.recover_service_restart_for(Some(restart_id))
    }

    fn recover_service_restart_for(&mut self, failed_restart: Option<i64>) -> Result<Option<i64>> {
        let boot_id = agent_run_platform::monotonic::boot_id()?;
        let tx = WriteTx::begin(&self.conn)?;
        let restart: Option<(i64, String, Option<i64>)> = tx
            .query_row(
                "SELECT id,phase,writer_epoch FROM service_restarts WHERE finished_at IS NULL",
                [],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .optional()?;
        let Some((id, phase, previous)) = restart else {
            tx.commit()?;
            return Ok(None);
        };
        if failed_restart.is_some_and(|requested| requested != id) {
            return Err(Error::Conflict);
        }
        if phase == "recovering" {
            tx.commit()?;
            return Ok(Some(id));
        }
        let current: Option<i64> = tx
            .query_row("SELECT epoch FROM writer_authority WHERE id=1", [], |row| {
                row.get(0)
            })
            .optional()?;
        let new_writer = matches!((previous, current), (Some(old), Some(new)) if new > old);
        let failed_on_same_writer = failed_restart == Some(id)
            && matches!((previous, current), (Some(old), Some(new)) if new == old);
        if !matches!(phase.as_str(), "ready" | "requesting")
            || !(new_writer || failed_on_same_writer)
        {
            tx.commit()?;
            return Ok(None);
        }
        tx.execute(
            "UPDATE service_restarts SET phase='recovering',boot_id=? WHERE id=? AND phase IN ('ready','requesting')",
            params![boot_id, id],
        )?;
        tx.commit()?;
        Ok(Some(id))
    }

    /// Start the confirmation clock for one captured continuation, once.
    pub fn request_service_restart_resume(
        &mut self,
        restart_id: i64,
        agent: &AgentId,
        boot_id: &str,
        now_ns: u64,
    ) -> Result<()> {
        let now_ns = i64::try_from(now_ns).map_err(|_| invalid("invalid restart clock"))?;
        let tx = WriteTx::begin(&self.conn)?;
        let changed = tx.execute(
            "UPDATE service_restart_agents SET resume_requested_ns=COALESCE(resume_requested_ns,?) \
             WHERE restart_id=? AND agent_id=? AND cancelled_before_restart=0 AND resume_status='pending' \
             AND EXISTS(SELECT 1 FROM service_restarts WHERE id=? AND phase='recovering' AND boot_id=?)",
            params![now_ns, restart_id, agent.as_str(), restart_id, boot_id],
        )?;
        if changed != 1 {
            return Err(Error::Conflict);
        }
        tx.commit()
    }

    /// Save a bounded reason for a definite refusal to resume one agent.
    pub fn fail_service_restart_resume(
        &mut self,
        restart_id: i64,
        agent: &AgentId,
        reason: &str,
    ) -> Result<()> {
        if reason.is_empty() || reason.len() > 256 || reason.chars().any(char::is_control) {
            return Err(invalid("invalid restart failure reason"));
        }
        let tx = WriteTx::begin(&self.conn)?;
        let changed = tx.execute(
            "UPDATE service_restart_agents SET resume_status='failed',resume_error=? \
             WHERE restart_id=? AND agent_id=? AND resume_status='pending' AND resume_requested_ns IS NOT NULL AND resumed_execution_id IS NULL \
             AND EXISTS(SELECT 1 FROM service_restarts WHERE id=? AND phase='recovering')",
            params![reason, restart_id, agent.as_str(), restart_id],
        )?;
        if changed != 1 {
            return Err(Error::Conflict);
        }
        tx.commit()
    }

    /// Record confirmed continuations, failures and 60-second unknown outcomes.
    /// Returns true once every captured agent has a result and admissions reopen.
    pub fn refresh_service_restart(
        &mut self,
        restart_id: i64,
        boot_id: &str,
        now_ns: u64,
        at: f64,
    ) -> Result<bool> {
        let now_ns = i64::try_from(now_ns).map_err(|_| invalid("invalid restart clock"))?;
        if !at.is_finite() || at < 0.0 {
            return Err(invalid("invalid restart time"));
        }
        let tx = WriteTx::begin(&self.conn)?;
        let active: bool = tx.query_row(
            "SELECT EXISTS(SELECT 1 FROM service_restarts WHERE id=? AND phase='recovering' AND boot_id=?)",
            params![restart_id,boot_id], |row|row.get(0),
        )?;
        if !active {
            return Err(Error::Conflict);
        }
        let pending = {
            let mut statement = tx.prepare(
                "SELECT agent_id,resume_requested_ns,resumed_execution_id FROM service_restart_agents \
                 WHERE restart_id=? AND resume_status='pending' AND resume_requested_ns IS NOT NULL",
            )?;
            let rows = statement.query_map([restart_id], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, i64>(1)?,
                    row.get::<_, Option<String>>(2)?,
                ))
            })?;
            rows.collect::<rusqlite::Result<Vec<_>>>()?
        };
        for (agent, requested, child) in pending {
            let (accepted, terminal, failure) = if let Some(child) = child {
                tx.query_row(
                    "SELECT a.status='succeeded' OR EXISTS(SELECT 1 FROM events WHERE agent_id=a.id AND kind='native_resume_accepted'), \
                     a.status IN ('succeeded','failed','timed_out','cancelled','lost'),a.failure_kind \
                     FROM agents a WHERE a.id=?", [child],
                    |row| Ok((row.get::<_,bool>(0)?,row.get::<_,bool>(1)?,row.get::<_,Option<String>>(2)?)),
                )?
            } else {
                (false, false, None)
            };
            let result = if accepted {
                Some(("started", None))
            } else if terminal {
                let kind = failure
                    .filter(|kind| {
                        !kind.is_empty()
                            && kind.len() <= 128
                            && kind
                                .bytes()
                                .all(|byte| byte.is_ascii_alphanumeric() || b"_-.".contains(&byte))
                    })
                    .unwrap_or_else(|| "execution_ended".into());
                Some((
                    "failed",
                    Some(format!("Continuation ended before confirmation: {kind}")),
                ))
            } else if now_ns.saturating_sub(requested) >= RESUME_CONFIRM_NS {
                Some((
                    "unconfirmed",
                    Some(
                        "No continuation confirmation within 60 seconds; the outcome is unknown."
                            .into(),
                    ),
                ))
            } else {
                None
            };
            if let Some((status, reason)) = result {
                tx.execute("UPDATE service_restart_agents SET resume_status=?,resume_error=? WHERE restart_id=? AND agent_id=? AND resume_status='pending'",
                    params![status,reason,restart_id,agent])?;
            }
        }
        let pending: bool = tx.query_row(
            "SELECT EXISTS(SELECT 1 FROM service_restart_agents a WHERE a.restart_id=? AND \
             (a.resume_status='pending' OR (a.resume_status='cancelled' AND EXISTS(SELECT 1 FROM agents c \
             WHERE c.id=a.resumed_execution_id AND c.status IN ('created','starting','running','cancelling')))))",
            [restart_id], |row|row.get(0),
        )?;
        if !pending {
            queue_restart_results(&tx, restart_id)?;
            tx.execute("UPDATE service_restarts SET phase='complete',finished_at=? WHERE id=? AND phase='recovering'", params![at,restart_id])?;
        }
        tx.commit()?;
        Ok(!pending)
    }

    /// Queue the two 30-second warnings and, if necessary, cancellation.
    /// The monotonic reading must belong to the restart's recorded boot.
    /// Each decision and its existing control command commit together.
    pub fn advance_service_restart(
        &mut self,
        restart_id: i64,
        boot_id: &str,
        now_ns: u64,
    ) -> Result<usize> {
        let now_ns = i64::try_from(now_ns).map_err(|_| invalid("invalid restart clock"))?;
        let tx = WriteTx::begin(&self.conn)?;
        let active: bool = tx.query_row(
            "SELECT EXISTS(SELECT 1 FROM service_restarts WHERE id=? AND phase='preparing' AND boot_id=?)",
            params![restart_id, boot_id], |row| row.get(0),
        )?;
        if !active {
            return Err(Error::Conflict);
        }
        // Give the ordinary dispatcher its first chance to send each warning
        // before asking workers to stop. Existing retry/failure handling remains unchanged.
        let awaiting:bool=tx.query_row(
            "SELECT EXISTS(SELECT 1 FROM service_restart_notices n JOIN deliveries d ON d.id=n.delivery_id \
             WHERE n.restart_id=? AND n.stage='before' AND d.state IN ('pending','sending'))",
            [restart_id],|row|row.get(0),
        )?;
        if awaiting {
            let started: i64 = tx.query_row(
                "UPDATE service_restarts SET notice_wait_started_ns=COALESCE(notice_wait_started_ns,?) \
                 WHERE id=? RETURNING notice_wait_started_ns",
                params![now_ns, restart_id], |row| row.get(0),
            )?;
            if now_ns.saturating_sub(started) < NOTICE_WAIT_NS {
                tx.commit()?;
                return Ok(0);
            }
        }
        let members = {
            let mut statement = tx.prepare(
                "SELECT r.agent_id,r.first_notice_ns,r.second_notice_ns,r.force_requested_ns, \
                 EXISTS(SELECT 1 FROM worker_lifecycle l JOIN attempts t ON t.id=l.attempt_id \
                 WHERE t.agent_id=a.id AND t.ownership_active=1 AND l.finish_json IS NOT NULL), \
                 EXISTS(SELECT 1 FROM events e WHERE e.agent_id=a.id AND e.kind='worker_finish_accepted_v1' \
                 AND json_extract(e.data_json,'$.service_restart_id')=r.restart_id) \
                 FROM service_restart_agents r JOIN agents a ON a.id=r.agent_id \
                 WHERE r.restart_id=? AND a.status IN ('created','starting','running') \
                 AND r.cancelled_before_restart=0 AND r.resume_status='pending' \
                 AND NOT EXISTS(SELECT 1 FROM commands c WHERE c.agent_id=a.id AND c.kind='cancel' AND c.state IN ('pending','claimed'))",
            )?;
            let rows = statement.query_map([restart_id], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, Option<i64>>(1)?,
                    row.get::<_, Option<i64>>(2)?,
                    row.get::<_, Option<i64>>(3)?,
                    row.get::<_, bool>(4)?,
                    row.get::<_, bool>(5)?,
                ))
            })?;
            rows.collect::<rusqlite::Result<Vec<_>>>()?
        };
        let mut queued = 0;
        for (agent, first, second, forced, finished, restart_finish) in members {
            let decision = match (first, second, forced) {
                (_, _, Some(_)) => None,
                (None, _, _) if !finished => {
                    Some(("steer", FIRST_NOTICE, "first_notice_ns", now_ns))
                }
                (Some(first), None, _)
                    if now_ns.saturating_sub(first) >= WARNING_DELAY_NS && !finished =>
                {
                    Some(("steer", SECOND_NOTICE, "second_notice_ns", now_ns))
                }
                (Some(first), None, _)
                    if finished
                        && restart_finish
                        && now_ns.saturating_sub(first) >= 2 * WARNING_DELAY_NS =>
                {
                    Some(("cancel", "", "force_requested_ns", now_ns))
                }
                (_, Some(second), _)
                    if (!finished || restart_finish)
                        && now_ns.saturating_sub(second) >= WARNING_DELAY_NS =>
                {
                    Some(("cancel", "", "force_requested_ns", now_ns))
                }
                _ => None,
            };
            if let Some((kind, text, column, at)) = decision {
                let payload = if kind == "steer" {
                    json!({"text":format!("{text} Begin the finish summary with this exact line followed by a newline: {}", finish_marker(restart_id))})
                } else {
                    json!({})
                };
                tx.execute(
                    "INSERT INTO commands(agent_id,kind,payload_json,state,created_at) VALUES(?,?,?,'pending',?)",
                    params![agent, kind, payload.to_string(), agent_run_domain::domain::now()],
                )?;
                tx.execute(
                    &format!("UPDATE service_restart_agents SET {column}=? WHERE restart_id=? AND agent_id=?"),
                    params![at, restart_id, agent],
                )?;
                queued += 1;
            }
        }
        tx.execute(
            "UPDATE service_restarts SET phase='ready' WHERE id=? AND phase='preparing' \
             AND NOT EXISTS(SELECT 1 FROM agents a WHERE a.status IN ('created','starting','running','cancelling'))",
            [restart_id],
        )?;
        tx.commit()?;
        Ok(queued)
    }
}

/// Queue one outcome list for each recipient.
fn queue_restart_results(conn: &Connection, restart_id: i64) -> Result<()> {
    let recipients = {
        let mut statement = conn.prepare("SELECT transport,external_session_id FROM service_restart_recipients WHERE restart_id=? ORDER BY transport,external_session_id")?;
        let rows = statement.query_map([restart_id], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
        })?;
        rows.collect::<rusqlite::Result<Vec<_>>>()?
    };
    for (transport, session) in recipients {
        queue_restart_result(conn, restart_id, &transport, &session)?;
    }
    Ok(())
}

/// Keep one result per recipient; update it only before any delivery attempt.
fn queue_restart_result(
    conn: &Connection,
    restart_id: i64,
    transport: &str,
    session: &str,
) -> Result<()> {
    let entries = {
        let mut statement = conn.prepare(
            "SELECT COALESCE(NULLIF(a.root_agent_id,''),a.id),r.resume_status,r.resume_error \
             FROM service_restart_agents r JOIN agents a ON a.id=r.agent_id \
             JOIN orchestrator_sessions o ON o.id=r.orchestrator_session_id \
             WHERE r.restart_id=? AND r.cancelled_before_restart=0 AND o.external_session_id=? AND \
             (CASE o.transport WHEN 'codex' THEN 'codex_queue' WHEN 'claude' THEN 'claude_uds' ELSE o.transport END)=? ORDER BY a.id",
        )?;
        let rows = statement.query_map(params![restart_id, session, transport], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, Option<String>>(2)?,
            ))
        })?;
        rows.collect::<rusqlite::Result<Vec<_>>>()?
    };
    let mut message = AFTER_NOTICE.to_owned();
    for (agent, status, reason) in entries {
        message.push_str(&format!(
            "\n- {agent}: {}",
            resume_result(&status, reason.as_deref())
        ));
    }
    conn.execute(
        "UPDATE service_restart_notices SET message=? WHERE restart_id=? AND transport=? AND external_session_id=? \
         AND stage='after' AND delivery_id IN (SELECT d.id FROM deliveries d WHERE d.state='pending' AND d.attempts=0 \
         AND NOT EXISTS(SELECT 1 FROM bridge_delivery_intents b WHERE b.delivery_id=d.id))",
        params![message,restart_id,transport,session],
    )?;
    enqueue_notice(conn, restart_id, transport, session, "after", &message)
}
