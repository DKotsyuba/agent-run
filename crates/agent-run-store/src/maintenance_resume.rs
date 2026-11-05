//! One durable same-agent attempt allocation after maintenance, without launch.
//!
//! The broker observes OS ownership, history bytes and route policy outside SQL.
//! This unit compares those prepared facts with the exact retained park source
//! and atomically transfers its reservation. It never probes or spawns a process.

use crate::{Store, ops::ExpectedOwnership, writer::WriteTx};
use agent_run_domain::{
    Error, Result, domain::AgentId, error::invalid, worker::MaintenancePauseRequest,
};
use agent_run_platform::process::ProcessState;
use rusqlite::{OptionalExtension, params};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

/// Real observations prepared by the trusted broker outside the writer lock,
/// following the existing reconciliation ownership protocol. No boolean here
/// substitutes for native-history verification or current launch policy checks.
#[derive(Clone, Serialize)]
pub struct ResumePreflight {
    /// Exact parked attempt whose reservation will transfer.
    pub source_attempt_id: String,
    /// Immutable park digest rechecked before any ownership mutation.
    pub source_proof_sha256: String,
    /// Recorded owner facts against which the broker observed the kernel process.
    pub expected_owner: ExpectedOwnership,
    /// Actual PID-reuse-aware observation; only Dead or Reused permits claim.
    pub supervisor_state: ProcessState,
    /// Digest of native history re-verified outside SQL for the original session.
    pub verified_history_sha256: String,
    /// Current launcher route already resolved outside SQL; None is local mode.
    pub current_route_fingerprint: Option<String>,
}

impl std::fmt::Debug for ResumePreflight {
    /// Do not expose native owner identity or private history bindings.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ResumePreflight")
            .field("supervisor_state", &self.supervisor_state)
            .finish_non_exhaustive()
    }
}

/// Immutable allocation identity plus a one-call launch authorization. An
/// already allocated target may have started with an unknown native outcome,
/// so a replay never grants permission to launch or resend.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct MaintenanceResume {
    /// Maintenance epoch which admitted this one continuation.
    pub epoch: i64,
    /// Unchanged logical execution; no child or lineage increment is created.
    pub agent_id: AgentId,
    /// Stable producer intent key, retained for allocation replay.
    pub request_id: String,
    /// Cleaned-up attempt whose account and quota keys were retained.
    pub source_attempt_id: String,
    /// Only successor allocated by this claim; not permission to spawn it again.
    pub target_attempt_id: String,
    /// Attempt ordinal inside the same logical execution, not its public sequence.
    pub target_number: u32,
    /// Original opaque physical account identity, without credentials or aliases.
    pub account_id: String,
    /// Original exact park-source digest, retained after owner fields change.
    pub source_proof_sha256: String,
    /// Resident writer generation which committed the allocation.
    pub writer_epoch: i64,
    /// Allocation commit observation time in Unix seconds, not native launch time.
    pub claimed_at: f64,
    /// True only on the allocating transaction's returned receipt. A replay is
    /// never permission to launch a possibly already-started target again.
    #[serde(default)]
    pub launch_authorized: bool,
}

impl MaintenanceResume {
    /// Decode one broker-owned dispatch receipt. A result retained in SQLite
    /// is not a launch permit; replayed receipts are always disarmed.
    pub fn from_dispatch_receipt(
        receipt: &agent_run_domain::storage::WriteReceipt,
    ) -> Result<Self> {
        let mut allocation: Self = receipt.output()?;
        if receipt.op_kind != "claim_maintenance_resume"
            || receipt.producer.kind != agent_run_domain::storage::ProducerKind::Broker
            || receipt.scope.as_ref() != Some(&allocation.agent_id)
            || allocation.writer_epoch <= 0
            || receipt.writer_epoch != allocation.writer_epoch as u64
        {
            return Err(Error::Integrity(
                "maintenance allocation receipt does not match".into(),
            ));
        }
        allocation.launch_authorized &= !receipt.replayed;
        Ok(allocation)
    }
}

/// Private inputs of an already allocated continuation. Possession does not
/// authorize a second process launch or an additional native control input.
pub struct MaintenanceContinuation {
    /// Open maintenance epoch and exact source and successor attempts.
    pub epoch: i64,
    pub source_attempt_id: String,
    pub target_attempt_id: String,
    /// Session and sealed history previously verified under the worker UID.
    pub session: String,
    pub history: String,
    /// Worker-supplied, untrusted context; callers must recheck launch secrets.
    pub checkpoint: MaintenancePauseRequest,
}

/// Source evidence rechecked inside the broker's current writer transaction.
pub(crate) struct ValidatedSource {
    pub park: crate::maintenance_park::MaintenancePark,
    pub number: u32,
    pub account: String,
}

impl std::fmt::Debug for MaintenanceContinuation {
    /// Private history and checkpoint content must not enter diagnostics.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MaintenanceContinuation")
            .field("epoch", &self.epoch)
            .finish_non_exhaustive()
    }
}

/// Require canonical hashes without exposing their actual values in errors.
fn digest(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
}

/// A complete observed process generation, never a fabricated zero owner.
fn owner_valid(pid: Option<i32>, token: Option<&str>, birth: Option<f64>) -> bool {
    pid.is_some_and(|pid| pid > 1)
        && token.is_some_and(|token| {
            token.len() <= 256
                && (token.starts_with("linux:") || token.starts_with("darwin:"))
                && !token.chars().any(char::is_control)
        })
        && birth.is_some_and(|birth| birth.is_finite() && birth >= 0.0)
}

/// Decode only the stable allocation receipt; no active-attempt lookup is used.
fn read_resume(row: &rusqlite::Row<'_>) -> rusqlite::Result<MaintenanceResume> {
    Ok(MaintenanceResume {
        epoch: row.get(0)?,
        agent_id: row.get::<_, String>(1)?.parse().map_err(|error| {
            rusqlite::Error::FromSqlConversionFailure(
                1,
                rusqlite::types::Type::Text,
                Box::new(error),
            )
        })?,
        request_id: row.get(2)?,
        source_attempt_id: row.get(3)?,
        target_attempt_id: row.get(4)?,
        target_number: row.get(5)?,
        account_id: row.get(6)?,
        source_proof_sha256: row.get(7)?,
        writer_epoch: row.get(8)?,
        claimed_at: row.get(9)?,
        launch_authorized: false,
    })
}

impl Store<'_> {
    /// Revalidate the broker's external process/history observations against
    /// the exact parked attempt while the caller owns its writer transaction.
    pub(crate) fn validate_maintenance_source(
        &self,
        epoch: i64,
        run: &AgentId,
        preflight: &ResumePreflight,
        at: f64,
    ) -> Result<ValidatedSource> {
        if !matches!(
            preflight.supervisor_state,
            ProcessState::Dead | ProcessState::Reused
        ) || preflight.expected_owner.execution != *run
            || !owner_valid(
                preflight.expected_owner.supervisor_pid,
                preflight.expected_owner.supervisor_identity.as_deref(),
                preflight.expected_owner.supervisor_birth_time,
            )
            || !digest(&preflight.source_proof_sha256)
            || !digest(&preflight.verified_history_sha256)
            || preflight
                .current_route_fingerprint
                .as_ref()
                .is_some_and(|value| !digest(value))
        {
            return Err(invalid(
                "maintenance resume preflight is incomplete or owner is not proven gone",
            ));
        }
        let park = self
            .maintenance_park(epoch, run)?
            .ok_or_else(|| invalid("maintenance park is missing"))?;
        if park.attempt_id != preflight.source_attempt_id
            || park.proof_sha256 != preflight.source_proof_sha256
        {
            return Err(Error::Conflict);
        }
        if at < park.parked_at
            || self
                .parked_maintenance_checkpoint(epoch, run, &park.attempt_id)?
                .is_none()
        {
            return Err(invalid("maintenance source is not eligible to resume"));
        }
        let row = self.get(run)?;
        let captured: u32 = self.conn.query_row(
            "SELECT sequence FROM maintenance_cohort WHERE epoch=? AND agent_id=?",
            params![epoch, run.as_str()],
            |r| r.get(0),
        )?;
        if row.sequence != captured {
            return Err(Error::Conflict);
        }
        let (startup_owner, startup_birth): (Option<String>, Option<f64>) = self.conn.query_row(
            "SELECT startup_owner_pid_identity,startup_owner_birth_time FROM agents WHERE id=?",
            [run.as_str()],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )?;
        let expected = &preflight.expected_owner;
        if row.supervisor_pid != expected.supervisor_pid
            || row.supervisor_identity != expected.supervisor_identity
            || row.supervisor_birth_time != expected.supervisor_birth_time
            || row.process_group_id != expected.process_group_id
            || startup_owner != expected.startup_owner
            || startup_birth != expected.startup_birth_time
        {
            return Err(Error::Conflict);
        }
        let identity = row
            .identity
            .as_ref()
            .ok_or_else(|| invalid("maintenance launch identity is missing"))?;
        let frozen_route = identity
            .get("system_route")
            .filter(|route| !route.is_null());
        let route = frozen_route.and_then(|route| route["fingerprint"].as_str());
        if (frozen_route.is_some() && route.is_none())
            || route != preflight.current_route_fingerprint.as_deref()
        {
            return Err(invalid("maintenance launcher route changed"));
        }
        let (number,account,history):(u32,String,String)=self.conn.query_row(
            "SELECT number,selected_account_id,adapter_state_json FROM attempts WHERE id=? AND agent_id=? AND state='maintenance_parked' AND ownership_active=1 AND finished_at IS NULL",
            params![park.attempt_id,run.as_str()],|r|Ok((r.get(0)?,r.get(1)?,r.get(2)?)))?;
        let (owned, latest): (i64, u32) = self.conn.query_row(
            "SELECT SUM(ownership_active),MAX(number) FROM attempts WHERE agent_id=?",
            [run.as_str()],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )?;
        if owned != 1 || number != latest {
            return Err(Error::Conflict);
        }
        let history: Value = serde_json::from_str(&history)?;
        if history["native_history"]["seal"]["sha256"] != preflight.verified_history_sha256 {
            return Err(invalid(
                "verified maintenance history no longer matches its seal",
            ));
        }
        let enabled:bool=self.conn.query_row("SELECT EXISTS(SELECT 1 FROM provider_accounts WHERE account_id=? AND status='enabled')",[&account],|r|r.get(0))?;
        if !enabled {
            return Err(invalid("maintenance source account is no longer enabled"));
        }
        Ok(ValidatedSource {
            park,
            number,
            account,
        })
    }

    /// Read the original claim for recovery; existence does not allow spawning.
    pub fn maintenance_resume(
        &self,
        epoch: i64,
        run: &AgentId,
    ) -> Result<Option<MaintenanceResume>> {
        Ok(self.conn.query_row("SELECT epoch,agent_id,request_id,source_attempt_id,target_attempt_id,target_number,account_id,source_proof_sha256,writer_epoch,claimed_at FROM maintenance_resumes WHERE epoch=? AND agent_id=?",params![epoch,run.as_str()],read_resume).optional()?)
    }

    /// Read the fixed inputs for the exact prepared successor in one snapshot.
    /// A missing allocation returns None; changed or cancelled evidence is an
    /// error, never permission to start a fresh task or repeat native input.
    pub fn maintenance_continuation(
        &self,
        run: &AgentId,
        attempt: &str,
    ) -> Result<Option<MaintenanceContinuation>> {
        let snapshot = if self.conn.is_autocommit() {
            Some(self.conn.unchecked_transaction()?)
        } else {
            None
        };
        let allocated: bool = self.conn.query_row(
            "SELECT EXISTS(SELECT 1 FROM maintenance_resumes WHERE agent_id=? AND target_attempt_id=?)",
            params![run.as_str(), attempt], |row| row.get(0),
        )?;
        if !allocated {
            return Ok(None);
        }
        type Stored = (i64, String, String, String, String, String, String);
        let stored: Option<Stored> = self.conn.query_row(
            "SELECT r.epoch,r.source_attempt_id,c.checkpoint_json,c.checkpoint_sha256,s.adapter_state_json,a.runtime_session_id,r.preflight_json \
             FROM maintenance_resumes r JOIN maintenance_fences f ON f.epoch=r.epoch \
             JOIN maintenance_parks p ON p.epoch=r.epoch AND p.agent_id=r.agent_id AND p.attempt_id=r.source_attempt_id AND p.proof_sha256=r.source_proof_sha256 \
             JOIN maintenance_checkpoints c ON c.epoch=r.epoch AND c.agent_id=r.agent_id AND c.attempt_id=r.source_attempt_id AND c.closing_event_seq=p.native_event_seq \
             JOIN attempts s ON s.id=r.source_attempt_id AND s.agent_id=r.agent_id \
             JOIN attempts t ON t.id=r.target_attempt_id AND t.agent_id=r.agent_id \
             JOIN agents a ON a.id=r.agent_id \
             WHERE r.agent_id=? AND r.target_attempt_id=? AND f.ended_at IS NULL \
             AND a.status='starting' AND a.finished_at IS NULL AND a.runtime_session_id IS NOT NULL \
             AND s.state='maintenance_paused' AND s.ownership_active=0 AND s.finished_at IS NOT NULL \
             AND t.state='prepared' AND t.phase='prepared' AND t.ownership_active=1 AND t.finished_at IS NULL \
             AND s.selected_account_id=r.account_id AND t.selected_account_id=r.account_id AND t.number=r.target_number \
             AND c.receipt_observed=1 AND c.closing_at IS NOT NULL \
             AND NOT EXISTS(SELECT 1 FROM commands q WHERE q.agent_id=a.id AND q.kind='cancel' AND q.state IN ('pending','claimed'))",
            params![run.as_str(), attempt],
            |row| Ok((row.get(0)?,row.get(1)?,row.get(2)?,row.get(3)?,row.get(4)?,row.get(5)?,row.get(6)?)),
        ).optional()?;
        let Some((epoch, source, checkpoint, expected_digest, history, session, preflight)) =
            stored
        else {
            return Err(Error::ExecutionChanged);
        };
        if agent_run_platform::fs::sha256(checkpoint.as_bytes()) != expected_digest {
            return Err(Error::Integrity(
                "maintenance checkpoint digest mismatch".into(),
            ));
        }
        let checkpoint: MaintenancePauseRequest = serde_json::from_str(&checkpoint)?;
        checkpoint.validate()?;
        let history_value: Value = serde_json::from_str(&history)?;
        let preflight: Value = serde_json::from_str(&preflight)?;
        if session.is_empty()
            || history_value["native_history"]["attempt"] != source
            || history_value["native_history"]["task_proven"] != true
            || history_value["native_history"]["seal"]["session"] != session
            || !preflight["verified_history_sha256"]
                .as_str()
                .is_some_and(digest)
            || history_value["native_history"]["seal"]["sha256"]
                != preflight["verified_history_sha256"]
            || preflight["source_attempt_id"] != source
        {
            return Err(Error::Integrity(
                "maintenance continuation evidence changed".into(),
            ));
        }
        if let Some(snapshot) = snapshot {
            snapshot.commit()?;
        }
        Ok(Some(MaintenanceContinuation {
            epoch,
            source_attempt_id: source,
            target_attempt_id: attempt.into(),
            session,
            history,
            checkpoint,
        }))
    }

    /// Allocate exactly one prepared successor for the same logical agent and
    /// account. The broker must verify the supplied OS/history/route facts before
    /// submission. Exact request replay precedes current lifecycle checks and
    /// returns no launch authorization, even if later launch/cancel is unknown.
    /// An unknown launch must reconcile this target, never allocate or send again.
    /// The broker-only allocation itself never starts a model or sends input.
    pub fn claim_maintenance_resume(
        &mut self,
        epoch: i64,
        run: &AgentId,
        request_id: &str,
        preflight: &ResumePreflight,
        at: f64,
        startup_deadline_at: f64,
    ) -> Result<MaintenanceResume> {
        if epoch <= 0
            || request_id.trim().is_empty()
            || request_id.len() > 256
            || request_id.chars().any(char::is_control)
            || !at.is_finite()
            || at < 0.0
            || !startup_deadline_at.is_finite()
            || startup_deadline_at <= at
        {
            return Err(invalid("invalid maintenance resume claim"));
        }
        let document = serde_json::to_string(preflight)?;
        let tx = WriteTx::begin(&self.conn)?;
        let prior:Option<(MaintenanceResume,String)>=tx.query_row(
            "SELECT epoch,agent_id,request_id,source_attempt_id,target_attempt_id,target_number,account_id,source_proof_sha256,writer_epoch,claimed_at,preflight_json FROM maintenance_resumes WHERE request_id=? OR (epoch=? AND agent_id=?)",
            params![request_id,epoch,run.as_str()],|row|Ok((read_resume(row)?,row.get(10)?))).optional()?;
        if let Some((prior, original)) = prior {
            if prior.epoch != epoch
                || prior.agent_id != *run
                || prior.request_id != request_id
                || original != document
            {
                return Err(Error::Conflict);
            }
            tx.commit()?;
            return Ok(prior);
        }
        let ValidatedSource {
            park,
            number,
            account,
        } = self.validate_maintenance_source(epoch, run, preflight, at)?;
        let writer:Option<(i64,i32,Option<String>,Option<f64>)>=tx.query_row(
            "SELECT epoch,holder_pid,holder_token,holder_birth FROM writer_authority WHERE id=1",[],|r|Ok((r.get(0)?,r.get(1)?,r.get(2)?,r.get(3)?))).optional()?;
        let Some((writer_epoch, pid, token, birth)) = writer else {
            return Err(invalid("resident writer ownership is unavailable"));
        };
        if writer_epoch < 1 || !owner_valid(Some(pid), token.as_deref(), birth) {
            return Err(invalid("resident writer ownership is incomplete"));
        }
        let (phase, ready): (String, Option<i64>) = tx.query_row(
            "SELECT phase,ready_writer_epoch FROM maintenance_fences WHERE epoch=? AND ended_at IS NULL",
            [epoch], |row| Ok((row.get(0)?,row.get(1)?)),
        )?;
        if !matches!(phase.as_str(), "deferring" | "recovering")
            || (phase == "recovering" && !ready.is_some_and(|ready| writer_epoch > ready))
        {
            return Err(Error::Transition(
                "maintenance continuation is not in recovery".into(),
            ));
        }
        let number = number
            .checked_add(1)
            .ok_or_else(|| invalid("maintenance attempt number overflow"))?;
        let target = format!("att_{}", uuid::Uuid::new_v4().simple());
        let released=tx.execute("UPDATE attempts SET state='maintenance_paused',ownership_active=0,finished_at=? WHERE id=? AND agent_id=? AND state='maintenance_parked' AND ownership_active=1",params![at,park.attempt_id,run.as_str()])?;
        if released != 1 {
            return Err(Error::Conflict);
        }
        tx.execute("INSERT INTO attempts(id,agent_id,number,state,phase,adapter_state_json,created_at,selected_account_id,ownership_active) VALUES(?,?,?,'prepared','prepared','{}',?,?,1)",params![target,run.as_str(),number,at,account])?;
        tx.execute("INSERT INTO attempt_quota_keys(attempt_id,quota_key) SELECT ?,quota_key FROM attempt_quota_keys WHERE attempt_id=?",params![target,park.attempt_id])?;
        let startup = json!({"pid":pid,"token":token,"birth":birth}).to_string();
        let changed=tx.execute(
            "UPDATE agents SET status='starting',supervisor_pid=NULL,supervisor_identity=NULL,supervisor_birth_time=NULL,process_group_id=NULL,heartbeat_at=NULL,supervisor_capability_sha256=NULL, \
             startup_owner_pid_identity=?,startup_owner_birth_time=?,startup_deadline_at=? WHERE id=? AND status='running' AND finished_at IS NULL",
            params![startup,birth,startup_deadline_at,run.as_str()])?;
        if changed != 1 {
            return Err(Error::Conflict);
        }
        tx.execute("INSERT INTO maintenance_resumes(epoch,agent_id,request_id,source_attempt_id,target_attempt_id,target_number,account_id,source_proof_sha256,writer_epoch,claimed_at,preflight_json) VALUES(?,?,?,?,?,?,?,?,?,?,?)",
            params![epoch,run.as_str(),request_id,park.attempt_id,target,number,account,park.proof_sha256,writer_epoch,at,document])?;
        tx.execute("INSERT INTO events(agent_id,attempt_id,at,kind,data_json) VALUES(?,?,?,'maintenance_preparation_guard_v1',?)",
            params![run.as_str(),target,at,json!({"epoch":epoch,"source_attempt_id":park.attempt_id,"target_attempt_id":target}).to_string()])?;
        tx.execute("INSERT INTO events(agent_id,attempt_id,at,kind,data_json) VALUES(?,?,?,'maintenance_resume_claimed_v1',?)",
            params![run.as_str(),target,at,json!({"epoch":epoch,"source_attempt":park.attempt_id,"target_attempt":target,"account":account,"number":number,"writer_epoch":writer_epoch}).to_string()])?;
        Store::advance_quota_capacity_revision(&tx)?;
        tx.commit()?;
        Ok(MaintenanceResume {
            epoch,
            agent_id: run.clone(),
            request_id: request_id.into(),
            source_attempt_id: park.attempt_id,
            target_attempt_id: target,
            target_number: number,
            account_id: account,
            source_proof_sha256: park.proof_sha256,
            writer_epoch,
            claimed_at: at,
            launch_authorized: true,
        })
    }
}
