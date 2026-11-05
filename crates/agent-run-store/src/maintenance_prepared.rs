//! Recover an allocated maintenance target which provably never reached launch.
use crate::{Store, ops::ClaimMaintenancePrepared, writer::WriteTx};
use agent_run_domain::{
    Error, Result,
    error::invalid,
    storage::{ProducerKind, WriteReceipt},
};
use agent_run_platform::process::ProcessState;
use rusqlite::params;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

/// One startup ownership transfer, not a second task/attempt allocation.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PreparedMaintenanceClaim {
    /// Unchanged original allocation identity; its own launch flag remains false.
    pub allocation: crate::maintenance_resume::MaintenanceResume,
    /// Current writer which took startup ownership from a proven-dead predecessor.
    pub dispatch_writer_epoch: i64,
    /// Broker-call-local permission; always cleared on writer receipt replay.
    pub launch_authorized: bool,
}

impl PreparedMaintenanceClaim {
    /// Decode only an exact fresh broker dispatch; stored/replayed results do
    /// not authorize a possibly repeated supervisor or native input.
    pub fn from_dispatch_receipt(receipt: &WriteReceipt) -> Result<Self> {
        let mut result: Self = receipt.output()?;
        if result.dispatch_writer_epoch <= 0
            || receipt.op_kind != "claim_maintenance_prepared"
            || receipt.producer.kind != ProducerKind::Broker
            || receipt.scope.as_ref() != Some(&result.allocation.agent_id)
            || receipt.writer_epoch != result.dispatch_writer_epoch as u64
        {
            return Err(Error::Integrity(
                "maintenance prepared receipt does not match".into(),
            ));
        }
        result.allocation.launch_authorized = false;
        result.launch_authorized &= !receipt.replayed;
        Ok(result)
    }
}

impl Store<'_> {
    /// Transfer startup ownership of the SAME prepared target only after the
    /// broker observes the exact prior owner Dead/Reused outside SQL. A prior
    /// capability can be cleared only for the original broker identity with
    /// no discovery or native start. No new attempt or quota key is created.
    pub fn claim_maintenance_prepared(
        &mut self,
        op: &ClaimMaintenancePrepared,
    ) -> Result<PreparedMaintenanceClaim> {
        if op.epoch <= 0
            || !op.at.is_finite()
            || op.at < 0.0
            || !op.startup_deadline_at.is_finite()
            || op.startup_deadline_at <= op.at
            || !op.expected_startup_birth_time.is_finite()
            || op.expected_startup_birth_time < 0.0
            || !matches!(op.owner_state, ProcessState::Dead | ProcessState::Reused)
        {
            return Err(invalid("prepared maintenance owner is not proven gone"));
        }
        let expected: Value = serde_json::from_str(&op.expected_startup_owner)
            .map_err(|_| invalid("invalid prepared startup owner"))?;
        if !expected["pid"]
            .as_i64()
            .is_some_and(|pid| pid > 1 && i32::try_from(pid).is_ok())
            || !expected["token"].as_str().is_some_and(|token| {
                token.len() <= 256
                    && (token.starts_with("linux:") || token.starts_with("darwin:"))
                    && !token.chars().any(char::is_control)
            })
            || expected["birth"].as_f64() != Some(op.expected_startup_birth_time)
        {
            return Err(invalid("incomplete prepared startup owner"));
        }
        if let Some(capability) = &op.expected_capability_sha256 {
            let exact_broker = expected.as_object().is_some_and(|object| {
                object.len() == 3
                    && ["pid", "token", "birth"]
                        .iter()
                        .all(|key| object.contains_key(*key))
            });
            if !exact_broker
                || capability.len() != 64
                || !capability
                    .bytes()
                    .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
            {
                return Err(invalid(
                    "prepared capability owner is not the original broker",
                ));
            }
        }
        let tx = WriteTx::begin(&self.conn)?;
        let allocation = self
            .maintenance_resume(op.epoch, &op.execution)?
            .ok_or_else(|| invalid("maintenance allocation is missing"))?;
        if allocation.target_attempt_id != op.target_attempt_id {
            return Err(Error::Conflict);
        }
        let eligible:bool=tx.query_row(&format!(
            "SELECT EXISTS(SELECT 1 FROM agents a JOIN attempts t ON t.agent_id=a.id JOIN maintenance_fences f ON f.epoch=? \
             WHERE a.id=? AND t.id=? AND f.ended_at IS NULL AND f.phase IN ('recovering','deferring') \
             AND a.status='starting' AND a.finished_at IS NULL AND a.supervisor_pid IS NULL AND a.supervisor_identity IS NULL \
             AND a.supervisor_birth_time IS NULL AND a.process_group_id IS NULL AND a.supervisor_capability_sha256 IS ? \
             AND t.state='prepared' AND t.finished_at IS NULL AND {} \
             AND NOT EXISTS(SELECT 1 FROM commands c WHERE c.agent_id=a.id AND c.kind='cancel' AND c.state IN ('pending','claimed')))",crate::provider_admission::NEVER_SPAWNED),
            params![op.epoch,op.execution.as_str(),op.target_attempt_id,op.expected_capability_sha256],|r|r.get(0))?;
        if !eligible
            || self
                .maintenance_continuation(&op.execution, &op.target_attempt_id)?
                .is_none()
            || (op.expected_capability_sha256.is_some()
                && !crate::maintenance_target_cancel::preparation_not_started(
                    &tx,
                    &op.execution,
                    &op.target_attempt_id,
                )?)
        {
            return Err(Error::Conflict);
        }
        let (old, birth): (Option<String>, Option<f64>) = tx.query_row(
            "SELECT startup_owner_pid_identity,startup_owner_birth_time FROM agents WHERE id=?",
            [op.execution.as_str()],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )?;
        if old.as_deref() != Some(op.expected_startup_owner.as_str())
            || birth != Some(op.expected_startup_birth_time)
        {
            return Err(Error::Conflict);
        }
        let (epoch, pid, token, birth): (i64, i32, Option<String>, Option<f64>) = tx.query_row(
            "SELECT epoch,holder_pid,holder_token,holder_birth FROM writer_authority WHERE id=1",
            [],
            |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?)),
        )?;
        if epoch <= 0
            || pid <= 1
            || token.as_deref().is_none_or(|token| {
                token.is_empty()
                    || token.len() > 256
                    || !(token.starts_with("linux:") || token.starts_with("darwin:"))
            })
            || birth.is_none_or(|birth| !birth.is_finite() || birth < 0.0)
        {
            return Err(invalid("resident writer ownership is incomplete"));
        }
        if expected["pid"].as_i64() == Some(i64::from(pid))
            && expected["token"].as_str() == token.as_deref()
            && expected["birth"].as_f64() == birth
        {
            return Err(invalid(
                "prepared startup is already owned by the current writer",
            ));
        }
        let enabled:bool=tx.query_row("SELECT EXISTS(SELECT 1 FROM provider_accounts WHERE account_id=? AND status='enabled')",[&allocation.account_id],|r|r.get(0))?;
        if !enabled {
            return Err(invalid("maintenance source account is no longer enabled"));
        }
        let owner = json!({"pid":pid,"token":token,"birth":birth}).to_string();
        let changed=tx.execute("UPDATE agents SET startup_owner_pid_identity=?,startup_owner_birth_time=?,startup_deadline_at=?,supervisor_capability_sha256=NULL WHERE id=? AND startup_owner_pid_identity=? AND startup_owner_birth_time=? AND supervisor_capability_sha256 IS ? AND supervisor_pid IS NULL",
            params![owner,birth,op.startup_deadline_at,op.execution.as_str(),op.expected_startup_owner,op.expected_startup_birth_time,op.expected_capability_sha256])?;
        if changed != 1 {
            return Err(Error::Conflict);
        }
        tx.execute("INSERT INTO events(agent_id,attempt_id,at,kind,data_json) VALUES(?,?,?,'maintenance_prepared_claimed_v1',?)",params![op.execution.as_str(),op.target_attempt_id,op.at,json!({"epoch":op.epoch,"target_attempt_id":op.target_attempt_id,"writer_epoch":epoch,"previous_owner_sha256":agent_run_platform::fs::sha256(op.expected_startup_owner.as_bytes())}).to_string()])?;
        tx.commit()?;
        Ok(PreparedMaintenanceClaim {
            allocation,
            dispatch_writer_epoch: epoch,
            launch_authorized: true,
        })
    }
}
