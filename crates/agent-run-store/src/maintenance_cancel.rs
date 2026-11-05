//! Broker cancellation of a parked attempt after its supervisor is proven gone.
//!
//! No signal or native input is issued. The same writer transaction verifies
//! the prepared owner observation, exact park and cancel command, and commits
//! the normal terminal outcome/outbox exactly once.

use crate::{Store, ops::CancelParkedMaintenance, writer::WriteTx};
use agent_run_domain::{
    Error, Result,
    domain::{Outcome, Status},
    error::invalid,
};
use agent_run_platform::process::{Cleanup, ProcessState};
use rusqlite::{OptionalExtension, params};

impl Store<'_> {
    /// Fulfil one explicit cancel for the exact still-reserved parked source.
    /// Kernel observation is prepared by the trusted broker outside SQL. A
    /// terminal replay changes nothing; a new attempt/owner or changed proof
    /// conflicts rather than borrowing old cleanup to cancel another process.
    pub fn cancel_parked_maintenance(&mut self, op: &CancelParkedMaintenance) -> Result<bool> {
        if op.epoch <= 0
            || op.cancel_command_id <= 0
            || !op.at.is_finite()
            || op.at < 0.0
            || op.expected_owner.execution != op.execution
            || !matches!(
                op.supervisor_state,
                ProcessState::Dead | ProcessState::Reused
            )
            || op.expected_owner.supervisor_pid.is_none_or(|pid| pid <= 1)
            || op
                .expected_owner
                .supervisor_identity
                .as_deref()
                .is_none_or(|token| {
                    token.is_empty()
                        || token.len() > 256
                        || !(token.starts_with("linux:") || token.starts_with("darwin:"))
                })
            || op
                .expected_owner
                .supervisor_birth_time
                .is_none_or(|birth| !birth.is_finite() || birth < 0.0)
        {
            return Err(invalid(
                "parked cancellation requires a complete proven-dead supervisor",
            ));
        }
        let tx = WriteTx::begin(&self.conn)?;
        let row = self.get(&op.execution)?;
        if row.status.terminal() {
            tx.commit()?;
            return Ok(false);
        }
        let park = self
            .maintenance_park(op.epoch, &op.execution)?
            .ok_or_else(|| invalid("maintenance park is unavailable"))?;
        if park.attempt_id != op.attempt || park.proof_sha256 != op.source_proof_sha256 {
            return Err(Error::Conflict);
        }
        let (startup_owner, startup_birth): (Option<String>, Option<f64>) = tx.query_row(
            "SELECT startup_owner_pid_identity,startup_owner_birth_time FROM agents WHERE id=?",
            [op.execution.as_str()],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )?;
        let expected = &op.expected_owner;
        if row.supervisor_pid != expected.supervisor_pid
            || row.supervisor_identity != expected.supervisor_identity
            || row.supervisor_birth_time != expected.supervisor_birth_time
            || row.process_group_id != expected.process_group_id
            || startup_owner != expected.startup_owner
            || startup_birth != expected.startup_birth_time
        {
            return Err(Error::Conflict);
        }
        let eligible:bool=tx.query_row(
            "SELECT EXISTS(SELECT 1 FROM attempts t JOIN maintenance_cohort c ON c.agent_id=t.agent_id \
             JOIN agents a ON a.id=t.agent_id JOIN worker_lifecycle l ON l.attempt_id=t.id \
             WHERE c.epoch=? AND a.id=? AND t.id=? AND a.sequence=c.sequence \
             AND a.status='running' AND a.finished_at IS NULL AND t.state='maintenance_parked' \
             AND t.phase='cleanup_complete' AND t.finished_at IS NULL AND t.ownership_active=1 \
             AND l.finish_json IS NULL AND t.number=(SELECT MAX(number) FROM attempts WHERE agent_id=a.id) \
             AND (SELECT COUNT(*) FROM attempts WHERE agent_id=a.id AND ownership_active=1)=1)",
            params![op.epoch,op.execution.as_str(),op.attempt],|r|r.get(0))?;
        if !eligible {
            return Err(Error::Conflict);
        }
        let command: Option<(String, String, String)> = tx
            .query_row(
                "SELECT agent_id,kind,state FROM commands WHERE id=?",
                [op.cancel_command_id],
                |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
            )
            .optional()?;
        if !command.is_some_and(|(id, kind, state)| {
            id == op.execution.as_str()
                && kind == "cancel"
                && matches!(state.as_str(), "pending" | "claimed")
        }) {
            return Err(invalid(
                "parked cancellation command does not match the execution",
            ));
        }
        if self.maintenance_park_fingerprint(
            op.epoch,
            &op.execution,
            &op.attempt,
            park.native_event_seq,
        )? != park.proof_sha256
        {
            return Err(Error::Integrity(
                "maintenance park source proof changed".into(),
            ));
        }
        let raw: String = tx.query_row(
            "SELECT cleanup_proof_json FROM attempts WHERE id=?",
            [&op.attempt],
            |r| r.get(0),
        )?;
        let cleanup: Cleanup = serde_json::from_str(&raw)?;
        if !cleanup.confirmed
            || !cleanup.group_gone
            || cleanup.descendants_gone != Some(true)
            || cleanup.scope != "verified_descendants"
            || cleanup.process_group_id != row.process_group_id
            || row.process_group_id.is_none_or(|group| group <= 1)
        {
            return Err(invalid("parked cancellation cleanup is unverified"));
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
        // A claimed cancel belongs to the proven-dead owner; completing it is
        // bookkeeping, not replaying a native action. Other claimed work stays untouched.
        tx.execute("UPDATE commands SET state='completed',completed_at=?,result_json='{\"accepted\":true,\"reason\":\"maintenance_cancelled\"}' WHERE id=? AND agent_id=? AND kind='cancel' AND state='claimed'",params![at,op.cancel_command_id,op.execution.as_str()])?;
        self.complete_terminal_commands(&op.execution, at)?;
        tx.commit()?;
        Ok(committed.is_some())
    }
}
