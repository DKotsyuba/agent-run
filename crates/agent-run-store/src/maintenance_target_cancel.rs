//! Explicit cancellation of an exact never-spawned maintenance successor.

use crate::{Store, ops::CancelPreparedMaintenance, writer::WriteTx};
use agent_run_domain::{
    Error, Result,
    domain::{Outcome, Status},
    error::invalid,
};
use agent_run_platform::process::{Identity, ProcessState};
use rusqlite::{OptionalExtension, params};
use serde_json::{Value, json};

/// Decode a complete spawned startup child only before supervisor registration.
/// The original broker's pid/token/birth document lacks the required process
/// fields and never proves a child launch. The separate startup birth column
/// remains a CAS fact; local launchers can retain the preceding broker's birth.
pub fn startup_child(owner: &crate::ops::ExpectedOwnership) -> Option<Identity> {
    if owner.supervisor_pid.is_some()
        || owner.supervisor_identity.is_some()
        || owner.supervisor_birth_time.is_some()
        || owner.process_group_id.is_some()
    {
        return None;
    }
    let child: Identity = serde_json::from_str(owner.startup_owner.as_deref()?).ok()?;
    (child.pid > 1
        && child.group == child.pid
        && child.birth.is_finite()
        && child.birth >= 0.0
        && !child.token.is_empty()
        && child.token.len() <= 256
        && !child.token.chars().any(char::is_control)
        && (child.token.starts_with("linux:") || child.token.starts_with("darwin:")))
    .then_some(child)
}

/// Prove the exact target's preparation/discovery boundary from durable ordered
/// markers. The caller must hold a read snapshot or writer transaction while
/// combining this result with ownership and never-spawned evidence. Missing,
/// duplicated, foreign or partially committed markers refuse cancellation.
pub fn preparation_is_settled(
    conn: &rusqlite::Connection,
    run: &agent_run_domain::domain::AgentId,
    target: &str,
) -> Result<bool> {
    let allocation: Option<(i64, String, String)> = conn.query_row(
        "SELECT r.epoch,r.source_attempt_id,json_extract(a.identity_json,'$.authority.harness') \
         FROM maintenance_resumes r JOIN agents a ON a.id=r.agent_id WHERE r.agent_id=? AND r.target_attempt_id=?",
        params![run.as_str(), target], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
    ).optional()?;
    let Some((epoch, source, harness)) = allocation else {
        return Ok(false);
    };
    if !matches!(harness.as_str(), "codex" | "claude-code") {
        return Ok(false);
    }
    let expected = json!({"epoch":epoch,"source_attempt_id":source,"target_attempt_id":target});
    let markers = conn.prepare(
        "SELECT kind,data_json FROM events WHERE agent_id=? AND attempt_id=? \
         AND kind IN ('maintenance_preparation_guard_v1','maintenance_discovery_started_v1','maintenance_discovery_settled_v1') ORDER BY seq LIMIT 4",
    )?.query_map(params![run.as_str(), target], |r| Ok((r.get::<_,String>(0)?, r.get::<_,String>(1)?)))?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    let kinds = markers
        .iter()
        .map(|(kind, _)| kind.as_str())
        .collect::<Vec<_>>();
    let no_discovery = kinds == ["maintenance_preparation_guard_v1"];
    let settled_discovery = harness == "claude-code"
        && kinds
            == [
                "maintenance_preparation_guard_v1",
                "maintenance_discovery_started_v1",
                "maintenance_discovery_settled_v1",
            ];
    if !(no_discovery || settled_discovery)
        || markers
            .iter()
            .any(|(_, raw)| serde_json::from_str::<Value>(raw).ok().as_ref() != Some(&expected))
    {
        return Ok(false);
    }
    if no_discovery && conn.query_row(
        "SELECT EXISTS(SELECT 1 FROM events WHERE agent_id=? AND attempt_id=? AND kind='mcp_discovery_ownership')",
        params![run.as_str(), target], |r| r.get::<_,bool>(0),
    )? {
        return Ok(false);
    }
    Ok(true)
}

impl Store<'_> {
    /// Cancel only an allocated successor whose exact supervisor was observed
    /// dead outside SQL and whose native launch is still provably absent.
    /// Before supervisor registration, only a complete spawned startup-child
    /// identity can substitute for that supervisor, never an initial broker.
    /// Durable preparation/discovery markers must prove no unfinished discovery.
    /// A missing guard or an unpaired discovery start remains held. The terminal
    /// outcome, reservation release, commands and outbox commit atomically;
    /// an uncertain acknowledgement or terminal replay adds no second notice.
    pub fn cancel_prepared_maintenance(&mut self, op: &CancelPreparedMaintenance) -> Result<bool> {
        let owner = &op.expected_owner;
        let bound_supervisor = owner.supervisor_pid.is_some_and(|pid| pid > 1)
            && owner.supervisor_identity.as_deref().is_some_and(|token| {
                !token.is_empty()
                    && token.len() <= 256
                    && !token.chars().any(char::is_control)
                    && (token.starts_with("linux:") || token.starts_with("darwin:"))
            })
            && owner
                .supervisor_birth_time
                .is_some_and(|birth| birth.is_finite() && birth >= 0.0);
        if op.epoch <= 0
            || op.cancel_command_id <= 0
            || !op.at.is_finite()
            || op.at < 0.0
            || owner.execution != op.execution
            || !matches!(
                op.supervisor_state,
                ProcessState::Dead | ProcessState::Reused
            )
            || !(bound_supervisor || startup_child(owner).is_some())
            || owner.process_group_id.is_some()
        {
            return Err(invalid(
                "prepared cancellation requires an exact proven-dead supervisor",
            ));
        }
        let tx = WriteTx::begin(&self.conn)?;
        let row = self.get(&op.execution)?;
        if row.status.terminal() {
            tx.commit()?;
            return Ok(false);
        }
        let (startup, birth): (Option<String>, Option<f64>) = tx.query_row(
            "SELECT startup_owner_pid_identity,startup_owner_birth_time FROM agents WHERE id=?",
            [op.execution.as_str()],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )?;
        if row.supervisor_pid != owner.supervisor_pid
            || row.supervisor_identity != owner.supervisor_identity
            || row.supervisor_birth_time != owner.supervisor_birth_time
            || row.process_group_id != owner.process_group_id
            || startup != owner.startup_owner
            || birth != owner.startup_birth_time
        {
            return Err(Error::Conflict);
        }
        let eligible: bool = tx.query_row(&format!(
            "SELECT EXISTS(SELECT 1 FROM maintenance_resumes r JOIN maintenance_fences f ON f.epoch=r.epoch \
             JOIN maintenance_cohort c ON c.epoch=r.epoch AND c.agent_id=r.agent_id \
             JOIN agents a ON a.id=r.agent_id JOIN attempts t ON t.id=r.target_attempt_id AND t.agent_id=a.id \
             JOIN attempts s ON s.id=r.source_attempt_id AND s.agent_id=a.id \
             WHERE r.epoch=? AND a.id=? AND t.id=? AND f.ended_at IS NULL AND f.phase IN ('recovering','deferring') \
             AND a.sequence=c.sequence AND a.status='starting' AND a.finished_at IS NULL AND a.process_group_id IS NULL \
             AND json_extract(a.identity_json,'$.provider_identity_version')=2 \
             AND t.state='prepared' AND t.finished_at IS NULL AND {} \
             AND t.number=r.target_number AND t.selected_account_id=r.account_id \
             AND t.number=(SELECT MAX(number) FROM attempts WHERE agent_id=a.id) \
             AND s.state='maintenance_paused' AND s.ownership_active=0 AND s.finished_at IS NOT NULL \
             AND s.selected_account_id=r.account_id \
             AND NOT EXISTS(SELECT 1 FROM events e WHERE e.agent_id=a.id AND e.attempt_id=t.id \
                 AND e.kind='maintenance_resume_confirmed_v1'))",
            crate::provider_admission::NEVER_SPAWNED),
            params![op.epoch, op.execution.as_str(), op.target_attempt_id], |r| r.get(0),
        )?;
        if !eligible || !preparation_is_settled(&tx, &op.execution, &op.target_attempt_id)? {
            return Err(Error::Conflict);
        }
        let command: Option<(String, String, String)> = tx
            .query_row(
                "SELECT agent_id,kind,state FROM commands WHERE id=?",
                [op.cancel_command_id],
                |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
            )
            .optional()?;
        if !command.is_some_and(|(run, kind, state)| {
            run == op.execution.as_str()
                && kind == "cancel"
                && matches!(state.as_str(), "pending" | "claimed")
        }) {
            return Err(invalid(
                "prepared cancellation command does not match the execution",
            ));
        }
        let outcome = Outcome {
            status: Status::Cancelled,
            exit_code: None,
            failure_kind: None,
            failure_text: None,
            runtime_session_id: row.runtime_session_id,
        };
        let committed =
            crate::terminal::commit_terminal(self, &op.execution, &outcome, None, None)?;
        let at = committed.unwrap_or(op.at);
        tx.execute("UPDATE commands SET state='completed',completed_at=?,result_json='{\"accepted\":true,\"reason\":\"maintenance_cancelled\"}' WHERE id=? AND agent_id=? AND kind='cancel' AND state='claimed'", params![at, op.cancel_command_id, op.execution.as_str()])?;
        self.complete_terminal_commands(&op.execution, at)?;
        tx.commit()?;
        Ok(committed.is_some())
    }
}
