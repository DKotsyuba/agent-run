//! Durable admission fence, captured cohort and bound-recipient pre-notices.
//!
//! These units neither stop workers nor establish restart readiness. Existing
//! attempts, commands, pool membership and execution statuses remain untouched.

use crate::{ACTIVE_SQL, Store, writer::WriteTx};
use agent_run_domain::{Error, Result, domain::AgentId, error::invalid};
use rusqlite::{Connection, OptionalExtension, params};
use serde::{Deserialize, Serialize};

/// Maximum time allotted to prepare one maintenance epoch, in seconds.
pub const PREPARATION_LIMIT_SECONDS: f64 = 600.0;

/// The sole safe completed outcomes are the explicit pre-send refusals and
/// native rejections. Unknown, malformed and claimed commands may have had
/// an effect and require reconciliation before the fence can be released.
pub(crate) const COMMAND_MAY_HAVE_EFFECT: &str = "(c.state='claimed' OR (c.state='completed' AND NOT ( \
    COALESCE(json_extract(c.result_json,'$.accepted')=0,0) \
    AND COALESCE(json_extract(c.result_json,'$.reason') IN \
      ('maintenance_context_unavailable','maintenance_binding_unavailable','native_rejected','backlog_pressure_unsent','native_input_closed','maintenance_deferred'),0))))";

/// A monotonic maintenance epoch, retained after its admission fence is lifted.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct MaintenanceFence {
    /// Durable positive epoch for this captured maintenance boundary.
    pub epoch: i64,
    /// Caller-supplied idempotency key for starting the boundary.
    pub request_id: String,
    /// Wall-clock creation time, used only as observation metadata.
    pub created_at: f64,
    /// Release time, absent while admission is fenced.
    pub ended_at: Option<f64>,
    /// Number of captured executions at the boundary.
    pub member_count: u64,
    /// Durable controller stage; admission stays fenced until complete.
    pub phase: String,
    /// Resident writer at capture, absent only in offline fixtures.
    pub origin_writer_epoch: Option<i64>,
    /// Writer which committed readiness, before the service was restarted.
    pub ready_writer_epoch: Option<i64>,
    /// Historical completion versus deferral, absent until admission reopens.
    pub outcome: Option<String>,
}

/// Content-free attempt facts at the fence's atomic capture boundary.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CapturedAttempt {
    /// Captured attempt identity.
    pub id: String,
    /// One-based attempt number within the logical execution.
    pub number: u32,
    /// Attempt state at capture time.
    pub state: String,
    /// Whether the attempt retained its ownership reservation at capture.
    pub ownership_active: bool,
}

/// Immutable captured execution; later binding or progress does not rewrite it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MaintenanceMember {
    /// Logical execution captured at the boundary.
    pub agent_id: AgentId,
    /// Execution state at capture time.
    pub status: String,
    /// Captured logical execution sequence.
    pub sequence: u32,
    /// Captured orchestrator binding, when known at the boundary.
    pub orchestrator_session_id: Option<String>,
    /// Unfinished or still-owned attempts seen at capture.
    pub attempts: Vec<CapturedAttempt>,
}

/// The broker's independently verified parked member before service stop.
/// It is proof input, not permission to start another process.
#[derive(Clone, Serialize)]
pub struct MaintenanceReadyMember {
    /// Captured logical execution.
    pub execution: AgentId,
    /// Original-worker OS, history and route observations.
    pub preflight: crate::maintenance_resume::ResumePreflight,
}

/// Decode the stable summary independently of the cohort's size.
fn read_fence(row: &rusqlite::Row<'_>) -> rusqlite::Result<MaintenanceFence> {
    Ok(MaintenanceFence {
        epoch: row.get(0)?,
        request_id: row.get(1)?,
        created_at: row.get(2)?,
        ended_at: row.get(3)?,
        member_count: row.get(4)?,
        phase: row.get(5)?,
        origin_writer_epoch: row.get(6)?,
        ready_writer_epoch: row.get(7)?,
        outcome: row.get(8)?,
    })
}

/// Refuse only fresh admissions inside their writer transaction, after replay
/// and authorization. Already-admitted execution work remains permitted.
pub(crate) fn require_admissions_open(tx: &Connection) -> Result<()> {
    if tx.query_row(
        "SELECT EXISTS(SELECT 1 FROM maintenance_fences WHERE ended_at IS NULL)",
        [],
        |row| row.get::<_, bool>(0),
    )? {
        return Err(Error::Transition(
            "Agent Run is temporarily not accepting new agents. Retry with the same request ID when the service is ready.".into(),
        ));
    }
    Ok(())
}

impl Store<'_> {
    /// Close fresh admissions, capture active executions and enqueue one
    /// pre-notice per bound recipient in one writer transaction. A repeated
    /// request returns its original epoch even after release; another request
    /// conflicts while the fence remains open. The timestamp is observation
    /// metadata, not part of the request's identity.
    pub fn begin_maintenance_fence(
        &mut self,
        request_id: &str,
        at: f64,
    ) -> Result<MaintenanceFence> {
        if request_id.trim().is_empty()
            || request_id.len() > 256
            || request_id.chars().any(char::is_control)
            || !at.is_finite()
            || at < 0.0
            || !(at + PREPARATION_LIMIT_SECONDS).is_finite()
        {
            return Err(invalid("invalid maintenance fence request"));
        }
        let tx = WriteTx::begin(&self.conn)?;
        if let Some(prior) = tx.query_row(
            "SELECT epoch,request_id,created_at,ended_at,member_count,phase,origin_writer_epoch,ready_writer_epoch,outcome FROM maintenance_fences WHERE request_id=?",
            [request_id], read_fence,
        ).optional()? {
            tx.commit()?;
            return Ok(prior);
        }
        if tx.query_row(
            "SELECT EXISTS(SELECT 1 FROM maintenance_fences WHERE ended_at IS NULL)",
            [],
            |row| row.get::<_, bool>(0),
        )? {
            return Err(Error::Conflict);
        }
        let members = {
            let mut statement = tx.prepare(&format!(
                "SELECT id,status,sequence,orchestrator_session_id FROM agents WHERE status IN {ACTIVE_SQL} ORDER BY id"
            ))?;
            statement
                .query_map([], |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, u32>(2)?,
                        row.get::<_, Option<String>>(3)?,
                    ))
                })?
                .collect::<rusqlite::Result<Vec<_>>>()?
        };
        let origin_writer_epoch: Option<i64> = tx
            .query_row("SELECT epoch FROM writer_authority WHERE id=1", [], |row| {
                row.get(0)
            })
            .optional()?;
        tx.execute(
            "INSERT INTO maintenance_fences(request_id,created_at,member_count,origin_writer_epoch) VALUES(?,?,?,?)",
            params![request_id, at, members.len() as i64, origin_writer_epoch],
        )?;
        let epoch = tx.last_insert_rowid();
        for (id, status, sequence, session) in &members {
            let attempts = {
                let mut statement = tx.prepare("SELECT id,number,state,ownership_active FROM attempts WHERE agent_id=? AND (finished_at IS NULL OR ownership_active=1) ORDER BY number,id")?;
                statement
                    .query_map([id], |row| {
                        Ok(CapturedAttempt {
                            id: row.get(0)?,
                            number: row.get(1)?,
                            state: row.get(2)?,
                            ownership_active: row.get(3)?,
                        })
                    })?
                    .collect::<rusqlite::Result<Vec<_>>>()?
            };
            tx.execute("INSERT INTO maintenance_cohort(epoch,agent_id,status,sequence,orchestrator_session_id,attempts_json) VALUES(?,?,?,?,?,?)",
                params![epoch,id,status,sequence,session,serde_json::to_string(&attempts)?])?;
        }
        // Each bound orchestrator receives one pre-notice, even when several
        // captured agents belong to it. Freeze the known session routing in
        // the same transaction as the admission fence and the outbox rows.
        let mut recipients = std::collections::BTreeMap::new();
        for (id, _, _, session) in &members {
            if let Some(session) = session {
                recipients.entry(session).or_insert(id);
            }
        }
        for (session_id, agent_id) in recipients {
            let (transport, external_session_id): (String, String) = tx.query_row(
                "SELECT transport,external_session_id FROM orchestrator_sessions WHERE id=?",
                [session_id],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )?;
            let notification_id = format!("ntf_{}", uuid::Uuid::new_v4().simple());
            let notice = crate::delivery::MaintenanceNotice::Pre {
                maintenance_epoch: epoch,
            };
            tx.execute(
                "INSERT INTO events(agent_id,at,kind,data_json) VALUES(?,?,'maintenance_notice_v1',?)",
                params![agent_id, at, serde_json::to_string(&notice)?],
            )?;
            let event_seq = tx.last_insert_rowid();
            tx.execute(
                "INSERT INTO deliveries(id,agent_id,orchestrator_session_id,terminal_event_seq,state,next_attempt_at) VALUES(?,?,?,?,'pending',?)",
                params![notification_id, agent_id, session_id, event_seq, at],
            )?;
            tx.execute(
                "INSERT INTO maintenance_recipients(epoch,orchestrator_session_id,transport,external_session_id,anchor_agent_id,before_delivery_id) VALUES(?,?,?,?,?,?)",
                params![epoch, session_id, transport, external_session_id, agent_id, notification_id],
            )?;
        }
        tx.commit()?;
        Ok(MaintenanceFence {
            epoch,
            request_id: request_id.into(),
            created_at: at,
            ended_at: None,
            member_count: members.len() as u64,
            phase: "preparing".into(),
            origin_writer_epoch,
            ready_writer_epoch: None,
            outcome: None,
        })
    }

    /// Commit restart readiness only after every still-active captured agent
    /// has a freshly verified, dead-supervisor parked source. The transaction
    /// rechecks all evidence and all orchestrator pre-notice custody.
    pub fn ready_maintenance_fence(
        &mut self,
        epoch: i64,
        members: &[MaintenanceReadyMember],
        at: f64,
    ) -> Result<MaintenanceFence> {
        if epoch <= 0 || !at.is_finite() || at < 0.0 {
            return Err(invalid("invalid maintenance readiness request"));
        }
        let tx = WriteTx::begin(&self.conn)?;
        let mut fence = tx.query_row("SELECT epoch,request_id,created_at,ended_at,member_count,phase,origin_writer_epoch,ready_writer_epoch,outcome FROM maintenance_fences WHERE epoch=?", [epoch], read_fence)
            .optional()?.ok_or_else(|| invalid("unknown maintenance epoch"))?;
        if fence.phase == "ready" {
            tx.commit()?;
            return Ok(fence);
        }
        if fence.phase != "preparing" || at < fence.created_at {
            return Err(Error::Conflict);
        }
        if at >= fence.created_at + PREPARATION_LIMIT_SECONDS {
            return Err(Error::Transition(
                "maintenance preparation deadline expired".into(),
            ));
        }
        let writer_epoch: i64 =
            tx.query_row("SELECT epoch FROM writer_authority WHERE id=1", [], |row| {
                row.get(0)
            })?;
        if Some(writer_epoch) != fence.origin_writer_epoch {
            return Err(Error::Transition(
                "maintenance preparation was interrupted by a writer restart".into(),
            ));
        }
        let unresolved_notice: bool = tx.query_row(
            "SELECT EXISTS(SELECT 1 FROM maintenance_recipients r LEFT JOIN bridge_delivery_intents b ON b.delivery_id=r.before_delivery_id \
             WHERE r.epoch=? AND (b.custody_state IS NOT 'custody_accepted' OR b.intent_json IS NULL))",
            [epoch], |row| row.get(0),
        )?;
        if unresolved_notice {
            return Err(Error::Transition(
                "maintenance pre-notice has not reached Bridge custody".into(),
            ));
        }
        let captured = {
            let mut statement = tx.prepare("SELECT c.agent_id,a.status FROM maintenance_cohort c LEFT JOIN agents a ON a.id=c.agent_id AND a.sequence=c.sequence WHERE c.epoch=? ORDER BY c.agent_id")?;
            let rows = statement.query_map([epoch], |row| {
                Ok((row.get::<_, String>(0)?, row.get::<_, Option<String>>(1)?))
            })?;
            rows.collect::<rusqlite::Result<Vec<_>>>()?
        };
        if captured.len() != fence.member_count as usize {
            return Err(Error::Integrity(
                "maintenance cohort is inconsistent".into(),
            ));
        }
        let mut by_id = std::collections::BTreeMap::new();
        for member in members {
            if by_id
                .insert(member.execution.as_str(), &member.preflight)
                .is_some()
            {
                return Err(invalid("duplicate maintenance readiness member"));
            }
        }
        for (id, status) in captured {
            match status.as_deref() {
                Some("succeeded" | "failed" | "timed_out" | "cancelled" | "lost") => {
                    if by_id.remove(id.as_str()).is_some() {
                        return Err(Error::Conflict);
                    }
                    let owned: bool = tx.query_row(
                        "SELECT EXISTS(SELECT 1 FROM attempts WHERE agent_id=? AND ownership_active=1)",
                        [&id], |row| row.get(0),
                    )?;
                    if owned {
                        return Err(Error::Transition(
                            "terminal maintenance member still owns a process".into(),
                        ));
                    }
                }
                Some("created" | "starting" | "running" | "cancelling") => {
                    let proof = by_id.remove(id.as_str()).ok_or_else(|| {
                        Error::Transition("maintenance has active work that is not parked".into())
                    })?;
                    self.validate_maintenance_source(epoch, &id.parse()?, proof, at)?;
                }
                _ => {
                    return Err(Error::Integrity(
                        "maintenance cohort member is unavailable".into(),
                    ));
                }
            }
        }
        if !by_id.is_empty() {
            return Err(Error::Conflict);
        }
        tx.execute("UPDATE maintenance_fences SET phase='ready',ready_writer_epoch=? WHERE epoch=? AND phase='preparing'", params![writer_epoch,epoch])?;
        fence.phase = "ready".into();
        fence.ready_writer_epoch = Some(writer_epoch);
        tx.commit()?;
        Ok(fence)
    }

    /// Defer preparation without releasing admission. Parked work must first
    /// be resumed or safely cancelled before the fence can be completed.
    pub fn defer_maintenance_fence(&mut self, epoch: i64) -> Result<MaintenanceFence> {
        let tx = WriteTx::begin(&self.conn)?;
        let mut fence = tx.query_row("SELECT epoch,request_id,created_at,ended_at,member_count,phase,origin_writer_epoch,ready_writer_epoch,outcome FROM maintenance_fences WHERE epoch=?", [epoch], read_fence)
            .optional()?.ok_or_else(|| invalid("unknown maintenance epoch"))?;
        match fence.phase.as_str() {
            "preparing" | "ready" => {
                tx.execute(
                    "UPDATE maintenance_fences SET phase='deferring' WHERE epoch=?",
                    [epoch],
                )?;
                fence.phase = "deferring".into();
            }
            "deferring" => {}
            _ => return Err(Error::Conflict),
        }
        tx.commit()?;
        Ok(fence)
    }

    /// After a new writer starts, distinguish a committed restart from an
    /// interrupted preparation. Never infer readiness from an epoch alone.
    pub fn observe_maintenance_restart(&mut self, epoch: i64) -> Result<MaintenanceFence> {
        let tx = WriteTx::begin(&self.conn)?;
        let mut fence = tx.query_row("SELECT epoch,request_id,created_at,ended_at,member_count,phase,origin_writer_epoch,ready_writer_epoch,outcome FROM maintenance_fences WHERE epoch=?", [epoch], read_fence)
            .optional()?.ok_or_else(|| invalid("unknown maintenance epoch"))?;
        let current: i64 =
            tx.query_row("SELECT epoch FROM writer_authority WHERE id=1", [], |row| {
                row.get(0)
            })?;
        let next = match fence.phase.as_str() {
            "preparing"
                if fence
                    .origin_writer_epoch
                    .is_some_and(|origin| current > origin) =>
            {
                Some("deferring")
            }
            "ready"
                if fence
                    .ready_writer_epoch
                    .is_some_and(|ready| current > ready) =>
            {
                Some("deferring")
            }
            "stopping"
                if fence
                    .ready_writer_epoch
                    .is_some_and(|ready| current > ready) =>
            {
                Some("recovering")
            }
            "recovering" | "deferring" => None,
            _ => {
                return Err(Error::Transition(
                    "maintenance restart is not established".into(),
                ));
            }
        };
        if let Some(next) = next {
            tx.execute(
                "UPDATE maintenance_fences SET phase=? WHERE epoch=?",
                params![next, epoch],
            )?;
            fence.phase = next.into();
        }
        tx.commit()?;
        Ok(fence)
    }

    /// Recheck the committed ready boundary immediately before stopping the
    /// broker. Every still-active execution must remain the exact parked source;
    /// an accepted cancel, changed proof or foreign owned process refuses stop.
    pub fn maintenance_shutdown_ready(&self) -> Result<bool> {
        let Some(fence) = self.active_maintenance_fence()? else {
            return Ok(false);
        };
        if !matches!(fence.phase.as_str(), "ready" | "stopping") {
            return Ok(false);
        }
        let active = {
            let mut statement = self.conn.prepare(
                "SELECT id FROM agents WHERE status IN ('created','starting','running','cancelling') ORDER BY id",
            )?;
            let rows = statement.query_map([], |row| row.get::<_, String>(0))?;
            rows.collect::<rusqlite::Result<Vec<_>>>()?
        };
        for id in active {
            let id: AgentId = id.parse()?;
            let Some(park) = self.maintenance_park(fence.epoch, &id)? else {
                return Ok(false);
            };
            if self
                .parked_maintenance_checkpoint(fence.epoch, &id, &park.attempt_id)?
                .is_none()
            {
                return Ok(false);
            }
        }
        let foreign: bool = self.conn.query_row(
            "SELECT EXISTS(SELECT 1 FROM attempts t WHERE t.ownership_active=1 AND NOT EXISTS( \
             SELECT 1 FROM maintenance_parks p JOIN maintenance_cohort c ON c.epoch=p.epoch AND c.agent_id=p.agent_id \
             JOIN agents a ON a.id=p.agent_id AND a.sequence=c.sequence \
             WHERE p.epoch=? AND p.attempt_id=t.id AND p.agent_id=t.agent_id \
             AND a.status IN ('created','starting','running','cancelling') AND t.state='maintenance_parked' AND t.phase='cleanup_complete' AND t.finished_at IS NULL))",
            [fence.epoch], |row| row.get(0),
        )?;
        Ok(!foreign)
    }

    /// Atomically seal the ready boundary when the service manager actually
    /// begins stopping. No deferral or new continuation can race past it.
    pub fn commit_maintenance_stop(&mut self, epoch: i64) -> Result<bool> {
        let tx = WriteTx::begin(&self.conn)?;
        let fence = self.active_maintenance_fence()?;
        let writer: i64 =
            tx.query_row("SELECT epoch FROM writer_authority WHERE id=1", [], |row| {
                row.get(0)
            })?;
        if !fence.as_ref().is_some_and(|f| {
            f.epoch == epoch
                && f.ready_writer_epoch == Some(writer)
                && matches!(f.phase.as_str(), "ready" | "stopping")
        }) || !self.maintenance_shutdown_ready()?
        {
            tx.commit()?;
            return Ok(false);
        }
        tx.execute(
            "UPDATE maintenance_fences SET phase='stopping' WHERE epoch=? AND phase='ready'",
            [epoch],
        )?;
        tx.commit()?;
        Ok(true)
    }

    /// Lift exactly one epoch's admission fence, idempotently. This does not
    /// assert worker quiescence or authorize stopping/restarting any process.
    /// Ending an old epoch never lifts a newer fence.
    pub fn end_maintenance_fence(&mut self, epoch: i64, at: f64) -> Result<MaintenanceFence> {
        if epoch <= 0 || !at.is_finite() || at < 0.0 {
            return Err(invalid("invalid maintenance fence release"));
        }
        let tx = WriteTx::begin(&self.conn)?;
        let mut fence = tx.query_row("SELECT epoch,request_id,created_at,ended_at,member_count,phase,origin_writer_epoch,ready_writer_epoch,outcome FROM maintenance_fences WHERE epoch=?", [epoch], read_fence)
            .optional()?.ok_or_else(|| invalid("unknown maintenance epoch"))?;
        if fence.ended_at.is_none() {
            if at < fence.created_at {
                return Err(invalid("maintenance release predates its fence"));
            }
            if matches!(fence.phase.as_str(), "ready" | "stopping") {
                return Err(Error::Transition(
                    "maintenance is ready for restart but recovery has not begun".into(),
                ));
            }
            let unresolved: bool = tx.query_row(
                &format!("SELECT EXISTS(SELECT 1 FROM maintenance_checkpoints m JOIN commands c ON c.id=m.command_id \
                 JOIN agents a ON a.id=m.agent_id JOIN attempts t ON t.id=m.attempt_id AND t.agent_id=a.id \
                 WHERE m.epoch=? AND a.status IN ('created','starting','running','cancelling') \
                 AND t.ownership_active=1 AND t.finished_at IS NULL AND ({COMMAND_MAY_HAVE_EFFECT} OR m.checkpoint_json IS NOT NULL))"),
                [epoch],
                |row| row.get(0),
            )?;
            if unresolved {
                return Err(Error::Transition(
                    "maintenance handoff is unresolved; admission remains fenced".into(),
                ));
            }
            let resuming: bool = tx.query_row(
                "SELECT EXISTS(SELECT 1 FROM maintenance_resumes r JOIN agents a ON a.id=r.agent_id \
                 LEFT JOIN attempts t ON t.id=r.target_attempt_id AND t.agent_id=a.id \
                 WHERE r.epoch=? AND a.status IN ('created','starting','running','cancelling') \
                 AND (t.id IS NULL OR NOT EXISTS( \
                   SELECT 1 FROM events e WHERE e.agent_id=r.agent_id AND e.attempt_id=r.target_attempt_id \
                   AND e.kind='maintenance_resume_confirmed_v1' AND json_extract(e.data_json,'$.epoch')=r.epoch \
                   AND json_extract(e.data_json,'$.source_attempt_id')=r.source_attempt_id \
                   AND json_extract(e.data_json,'$.target_attempt_id')=r.target_attempt_id \
                   AND json_type(e.data_json,'$.session')='text' AND json_extract(e.data_json,'$.session')=a.runtime_session_id \
                   AND json_type(e.data_json,'$.turn')='text' AND length(CAST(json_extract(e.data_json,'$.turn') AS BLOB)) BETWEEN 1 AND 1024 \
                   AND json_type(e.data_json,'$.input_sha256')='text' AND length(json_extract(e.data_json,'$.input_sha256'))=64 \
                   AND json_extract(e.data_json,'$.input_sha256') NOT GLOB '*[^0-9a-f]*')))",
                [epoch],
                |row| row.get(0),
            )?;
            if resuming {
                return Err(Error::Transition(
                    "maintenance continuation is unconfirmed; admission remains fenced".into(),
                ));
            }
            if fence.phase == "recovering" {
                let unrestored: bool = tx.query_row(
                    "SELECT EXISTS(SELECT 1 FROM maintenance_cohort c LEFT JOIN agents a ON a.id=c.agent_id AND a.sequence=c.sequence \
                     WHERE c.epoch=? AND (a.id IS NULL OR (a.status IN ('created','starting','running','cancelling') \
                       AND NOT EXISTS(SELECT 1 FROM maintenance_resumes r WHERE r.epoch=c.epoch AND r.agent_id=c.agent_id))))",
                    [epoch], |row| row.get(0),
                )?;
                if unrestored {
                    return Err(Error::Transition(
                        "maintenance work has not been restored; admission remains fenced".into(),
                    ));
                }
            }
            // Deferral fences future claims first; a never-claimed instruction
            // can now be resolved without sending it to the native session.
            tx.execute(
                "UPDATE commands SET state='completed',completed_at=?,result_json=? WHERE state='pending' \
                 AND id IN (SELECT command_id FROM maintenance_checkpoints WHERE epoch=? AND command_id IS NOT NULL)",
                params![at,serde_json::json!({"accepted":false,"reason":"maintenance_deferred"}).to_string(),epoch],
            )?;
            let recipients = {
                let mut statement = tx.prepare(
                    "SELECT orchestrator_session_id,anchor_agent_id FROM maintenance_recipients WHERE epoch=? ORDER BY orchestrator_session_id",
                )?;
                let rows = statement.query_map([epoch], |row| {
                    Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
                })?;
                rows.collect::<rusqlite::Result<Vec<_>>>()?
            };
            for (session, agent) in recipients {
                let notice = if fence.phase == "recovering" {
                    let counts: (u32,u32,u32,u32,u32) = tx.query_row(
                        "SELECT \
                           COALESCE(SUM(CASE WHEN a.status IN ('created','starting','running','cancelling') AND EXISTS(SELECT 1 FROM maintenance_resumes r WHERE r.epoch=c.epoch AND r.agent_id=c.agent_id) THEN 1 ELSE 0 END),0), \
                           COALESCE(SUM(CASE WHEN a.status='succeeded' THEN 1 ELSE 0 END),0), \
                           COALESCE(SUM(CASE WHEN a.status IN ('failed','timed_out','lost') THEN 1 ELSE 0 END),0), \
                           COALESCE(SUM(CASE WHEN a.status='cancelled' THEN 1 ELSE 0 END),0), \
                           COALESCE(SUM(CASE WHEN a.id IS NULL OR (a.status IN ('created','starting','running','cancelling') AND NOT EXISTS(SELECT 1 FROM maintenance_resumes r WHERE r.epoch=c.epoch AND r.agent_id=c.agent_id)) THEN 1 ELSE 0 END),0) \
                         FROM maintenance_cohort c LEFT JOIN agents a ON a.id=c.agent_id AND a.sequence=c.sequence \
                         WHERE c.epoch=? AND c.orchestrator_session_id=?",
                        params![epoch,session], |row| Ok((row.get(0)?,row.get(1)?,row.get(2)?,row.get(3)?,row.get(4)?)),
                    )?;
                    crate::delivery::MaintenanceNotice::After {
                        maintenance_epoch: epoch,
                        resumed: counts.0,
                        completed: counts.1,
                        failed: counts.2,
                        cancelled: counts.3,
                        held: counts.4,
                    }
                } else {
                    crate::delivery::MaintenanceNotice::Deferred {
                        maintenance_epoch: epoch,
                    }
                };
                let notification_id = format!("ntf_{}", uuid::Uuid::new_v4().simple());
                tx.execute(
                    "INSERT INTO events(agent_id,at,kind,data_json) VALUES(?,?,'maintenance_notice_v1',?)",
                    params![agent,at,serde_json::to_string(&notice)?],
                )?;
                let event = tx.last_insert_rowid();
                tx.execute(
                    "INSERT INTO deliveries(id,agent_id,orchestrator_session_id,terminal_event_seq,state,next_attempt_at) VALUES(?,?,?,?,'pending',?)",
                    params![notification_id,agent,session,event,at],
                )?;
                tx.execute(
                    "UPDATE maintenance_recipients SET after_delivery_id=? WHERE epoch=? AND orchestrator_session_id=? AND after_delivery_id IS NULL",
                    params![notification_id,epoch,session],
                )?;
            }
            let outcome = if fence.phase == "recovering" {
                "completed"
            } else {
                "deferred"
            };
            tx.execute(
                "UPDATE maintenance_fences SET ended_at=?,phase='complete',outcome=? WHERE epoch=? AND ended_at IS NULL",
                params![at, outcome, epoch],
            )?;
            fence.ended_at = Some(at);
            fence.phase = "complete".into();
            fence.outcome = Some(outcome.into());
        }
        tx.commit()?;
        Ok(fence)
    }

    /// Read the current durable admission barrier without changing execution state.
    pub fn active_maintenance_fence(&self) -> Result<Option<MaintenanceFence>> {
        Ok(self.conn.query_row("SELECT epoch,request_id,created_at,ended_at,member_count,phase,origin_writer_epoch,ready_writer_epoch,outcome FROM maintenance_fences WHERE ended_at IS NULL", [], read_fence).optional()?)
    }

    /// Read one exact historical epoch; absence never implies completion.
    pub fn maintenance_fence(&self, epoch: i64) -> Result<Option<MaintenanceFence>> {
        if epoch <= 0 {
            return Err(invalid("invalid maintenance epoch"));
        }
        Ok(self.conn.query_row("SELECT epoch,request_id,created_at,ended_at,member_count,phase,origin_writer_epoch,ready_writer_epoch,outcome FROM maintenance_fences WHERE epoch=?", [epoch], read_fence).optional()?)
    }

    /// Read the retained captured cohort ordered by agent id; no task or
    /// transcript is stored. After release, entries expire with agent history.
    /// Unknown epochs are refused rather than mistaken for an empty capture.
    pub fn maintenance_cohort(&self, epoch: i64) -> Result<Vec<MaintenanceMember>> {
        let exists = self.conn.query_row(
            "SELECT EXISTS(SELECT 1 FROM maintenance_fences WHERE epoch=?)",
            [epoch],
            |row| row.get::<_, bool>(0),
        )?;
        if !exists {
            return Err(invalid("unknown maintenance epoch"));
        }
        let mut statement = self.conn.prepare("SELECT agent_id,status,sequence,orchestrator_session_id,attempts_json FROM maintenance_cohort WHERE epoch=? ORDER BY agent_id")?;
        let rows = statement.query_map([epoch], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, u32>(2)?,
                row.get::<_, Option<String>>(3)?,
                row.get::<_, String>(4)?,
            ))
        })?;
        rows.map(|row| {
            let (agent_id, status, sequence, orchestrator_session_id, attempts) = row?;
            Ok(MaintenanceMember {
                agent_id: agent_id.parse()?,
                status,
                sequence,
                orchestrator_session_id,
                attempts: serde_json::from_str(&attempts)?,
            })
        })
        .collect()
    }
}
