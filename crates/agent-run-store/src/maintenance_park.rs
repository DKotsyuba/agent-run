//! Atomic, nonterminal parking after exact native, cleanup and history evidence.
//!
//! No production operation currently calls this unit or produces its native
//! settlement event. Fixture tests exercise storage semantics, not native proof
//! acquisition. Parking neither stops a process nor authorizes a restart.

use crate::{Store, writer::WriteTx};
use agent_run_domain::{
    Error, Result, domain::AgentId, error::invalid, worker::MaintenancePauseRequest,
};
use agent_run_platform::{fs::sha256, process::Cleanup};
use rusqlite::{OptionalExtension, params};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::path::Path;

/// Closed payload emitted only by a future trusted supervisor after strict
/// native observation. It binds the observed callback and completed turn to one
/// attempt; worker prose or a generic `settled=true` event is never sufficient.
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NativeSettlement {
    pub version: u32,
    pub protocol: String,
    pub coverage: String,
    pub maintenance_epoch: i64,
    pub agent_id: AgentId,
    pub attempt_id: String,
    pub challenge_id: String,
    pub receipt_sha256: String,
    /// Exact Codex thread id or Claude session_id from the owned native stream.
    pub thread_id: String,
    /// Exact Codex turn id or Claude user_message_uuid of the sent control.
    pub turn_id: String,
    /// Exact Codex item id or Claude tool_use_id of the checkpoint callback.
    pub item_id: String,
}

impl std::fmt::Debug for NativeSettlement {
    /// Native/session identity and checkpoint binding stay out of diagnostics.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NativeSettlement")
            .field("version", &self.version)
            .finish_non_exhaustive()
    }
}

/// Immutable receipt of nonterminal parking. The logical agent and its account
/// reservation remain owned; a later resume or cancellation needs its own gates.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct MaintenancePark {
    pub epoch: i64,
    pub agent_id: AgentId,
    pub attempt_id: String,
    pub native_event_seq: i64,
    pub proof_sha256: String,
    pub parked_at: f64,
}

/// Check native identifiers without including their contents in errors.
fn native_id(value: &str) -> bool {
    !value.is_empty() && value.len() <= 1024 && !value.chars().any(char::is_control)
}

/// Check the fixed canonical digest spelling before any proof comparison.
fn digest(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
}

/// Decode the bounded park receipt without reading native payloads.
fn read_park(row: &rusqlite::Row<'_>) -> rusqlite::Result<MaintenancePark> {
    Ok(MaintenancePark {
        epoch: row.get(0)?,
        agent_id: row.get::<_, String>(1)?.parse().map_err(|error| {
            rusqlite::Error::FromSqlConversionFailure(
                1,
                rusqlite::types::Type::Text,
                Box::new(error),
            )
        })?,
        attempt_id: row.get(2)?,
        native_event_seq: row.get(3)?,
        proof_sha256: row.get(4)?,
        parked_at: row.get(5)?,
    })
}

impl Store<'_> {
    /// Read an existing immutable park receipt; it is not current readiness.
    pub fn maintenance_park(&self, epoch: i64, run: &AgentId) -> Result<Option<MaintenancePark>> {
        Ok(self.conn.query_row("SELECT epoch,agent_id,attempt_id,native_event_seq,proof_sha256,parked_at FROM maintenance_parks WHERE epoch=? AND agent_id=?",params![epoch,run.as_str()],read_park).optional()?)
    }

    /// Bind current durable source bytes, ownership identities and reservation
    /// keys without copying them into the park row. Attempt state is excluded
    /// because parking changes it; the caller verifies its permitted value.
    pub(crate) fn maintenance_park_fingerprint(
        &self,
        epoch: i64,
        run: &AgentId,
        attempt: &str,
        event_seq: i64,
    ) -> Result<String> {
        let raw:String=self.conn.query_row(
            "SELECT json_object(\
             'epoch',c.epoch,'agent_id',a.id,'attempt_id',t.id,'request',a.request_json,'identity',a.identity_json,\
             'root',a.root_agent_id,'sequence',a.sequence,'session',a.runtime_session_id,'pgid',a.process_group_id,\
             'supervisor',json_array(a.supervisor_pid,a.supervisor_identity,a.supervisor_birth_time),\
             'attempt',json_array(t.number,t.selected_account_id,t.process_identity,t.process_birth_time,t.cleanup_proof_json,t.adapter_state_json),\
             'checkpoint',json_array(c.challenge_id,c.checkpoint_json,c.checkpoint_sha256,c.accepted_at,c.receipt_observed,c.command_id,c.closing_event_seq,c.closing_event_sha256,c.closing_at),\
             'command',json_array(q.id,q.agent_id,q.kind,q.payload_json,q.state,q.result_json),\
             'event',json_array(e.seq,e.kind,e.agent_id,e.attempt_id,e.at,e.from_status,e.to_status,e.data_json),\
             'keys',(SELECT json_group_array(quota_key) FROM (SELECT quota_key FROM attempt_quota_keys WHERE attempt_id=t.id ORDER BY quota_key))) \
             FROM agents a JOIN attempts t ON t.agent_id=a.id JOIN maintenance_checkpoints c ON c.agent_id=a.id AND c.attempt_id=t.id \
             JOIN events e ON e.seq=? LEFT JOIN commands q ON q.id=c.command_id WHERE c.epoch=? AND a.id=? AND t.id=?",
            params![event_seq,epoch,run.as_str(),attempt],|row|row.get(0))?;
        let value: Value = serde_json::from_str(&raw)?;
        Ok(agent_run_domain::canonical::sha256_hex(&value, true))
    }

    /// Read an exact parked checkpoint without weakening the live accessor.
    /// Cancellation, normal finish, changed ownership or a closed fence makes
    /// it unavailable. A changed park source is an integrity error, not a new
    /// baseline. Deferred snapshot reads work through StoreReader as well.
    pub fn parked_maintenance_checkpoint(
        &self,
        epoch: i64,
        run: &AgentId,
        attempt: &str,
    ) -> Result<Option<MaintenancePauseRequest>> {
        let snapshot = if self.conn.is_autocommit() {
            Some(self.conn.unchecked_transaction()?)
        } else {
            None
        };
        let Some(park) = self.maintenance_park(epoch, run)? else {
            return Ok(None);
        };
        if park.attempt_id != attempt {
            return Ok(None);
        }
        let eligible:bool=self.conn.query_row(
            "SELECT EXISTS(SELECT 1 FROM maintenance_fences f JOIN maintenance_parks p ON p.epoch=f.epoch \
             JOIN agents a ON a.id=p.agent_id JOIN attempts t ON t.id=p.attempt_id AND t.agent_id=a.id \
             JOIN worker_lifecycle l ON l.attempt_id=t.id \
             WHERE p.epoch=? AND a.id=? AND t.id=? AND f.ended_at IS NULL AND a.status='running' AND a.finished_at IS NULL \
             AND t.state='maintenance_parked' AND t.phase='cleanup_complete' AND t.ownership_active=1 AND t.finished_at IS NULL \
             AND l.finish_json IS NULL AND NOT EXISTS(SELECT 1 FROM commands q WHERE q.agent_id=a.id AND q.kind='cancel' AND q.state IN ('pending','claimed')))",
            params![epoch,run.as_str(),attempt],|row|row.get(0))?;
        if !eligible {
            return Ok(None);
        }
        let closing:bool=self.conn.query_row(
            "SELECT EXISTS(SELECT 1 FROM maintenance_checkpoints WHERE epoch=? AND agent_id=? AND attempt_id=? \
             AND closing_event_seq=? AND closing_event_sha256 IS NOT NULL AND closing_at IS NOT NULL)",
            params![epoch,run.as_str(),attempt,park.native_event_seq],|row|row.get(0))?;
        if !closing {
            return Err(Error::Integrity(
                "maintenance closing evidence is missing".into(),
            ));
        }
        if self.maintenance_park_fingerprint(epoch, run, attempt, park.native_event_seq)?
            != park.proof_sha256
        {
            return Err(Error::Integrity(
                "maintenance park source proof changed".into(),
            ));
        }
        let (payload,expected):(String,String)=self.conn.query_row(
            "SELECT checkpoint_json,checkpoint_sha256 FROM maintenance_checkpoints WHERE epoch=? AND agent_id=? AND attempt_id=? AND receipt_observed=1",
            params![epoch,run.as_str(),attempt],|row|Ok((row.get(0)?,row.get(1)?)))?;
        if sha256(payload.as_bytes()) != expected {
            return Err(Error::Integrity(
                "maintenance checkpoint digest mismatch".into(),
            ));
        }
        let input: MaintenancePauseRequest = serde_json::from_str(&payload)?;
        input.validate()?;
        if let Some(snapshot) = snapshot {
            snapshot.commit()?;
        }
        Ok(Some(input))
    }

    /// Park the exact still-owned attempt after committed closing, native
    /// receipt/turn settlement, cleanup and sealed same-session history.
    /// The writer operation requires the pinned supervisor, and every predicate
    /// is rechecked in one transaction.
    /// Changed evidence conflicts; cancellation and ordinary finish refuse even
    /// a replay. Only attempt.state changes; no answer, command or notice is made.
    pub fn park_maintenance_attempt(
        &mut self,
        epoch: i64,
        run: &AgentId,
        attempt: &str,
        native_event_seq: i64,
        at: f64,
    ) -> Result<MaintenancePark> {
        if epoch <= 0 || native_event_seq <= 0 || !at.is_finite() || at < 0.0 {
            return Err(invalid("invalid maintenance park request"));
        }
        let tx = WriteTx::begin(&self.conn)?;
        let prior = self.maintenance_park(epoch, run)?;
        if prior.as_ref().is_some_and(|prior| {
            prior.attempt_id != attempt || prior.native_event_seq != native_event_seq
        }) {
            return Err(Error::Conflict);
        }
        let closing:Option<(i64,f64)>=tx.query_row(
            "SELECT closing_event_seq,closing_at FROM maintenance_checkpoints WHERE epoch=? AND agent_id=? AND attempt_id=? \
             AND closing_event_seq IS NOT NULL AND closing_event_sha256 IS NOT NULL AND closing_at IS NOT NULL",
            params![epoch,run.as_str(),attempt],|row|Ok((row.get(0)?,row.get(1)?))).optional()?;
        let Some((closing_event, closing_at)) = closing else {
            return Err(invalid("maintenance closing evidence is missing"));
        };
        if closing_event != native_event_seq {
            return Err(Error::Conflict);
        }
        if !closing_at.is_finite() || closing_at > at {
            return Err(invalid("maintenance closing time is invalid"));
        }
        let eligible:bool=tx.query_row(
            "SELECT EXISTS(SELECT 1 FROM maintenance_cohort c JOIN maintenance_fences f ON f.epoch=c.epoch \
             JOIN agents a ON a.id=c.agent_id JOIN attempts t ON t.agent_id=a.id \
             JOIN worker_lifecycle l ON l.attempt_id=t.id \
             WHERE c.epoch=? AND a.id=? AND a.sequence=c.sequence AND t.id=? AND f.ended_at IS NULL \
             AND a.status='running' AND a.finished_at IS NULL AND t.ownership_active=1 AND t.finished_at IS NULL \
             AND l.finish_json IS NULL AND t.number=(SELECT MAX(number) FROM attempts WHERE agent_id=a.id) \
             AND NOT EXISTS(SELECT 1 FROM commands q WHERE q.agent_id=a.id AND q.kind='cancel' AND q.state IN ('pending','claimed')))",
            params![epoch,run.as_str(),attempt],|row|row.get(0))?;
        if !eligible {
            return Err(invalid("maintenance attempt is no longer eligible to park"));
        }
        let owned: i64 = tx.query_row(
            "SELECT COUNT(*) FROM attempts WHERE agent_id=? AND ownership_active=1",
            [run.as_str()],
            |row| row.get(0),
        )?;
        if owned != 1 {
            return Err(Error::Conflict);
        }
        let row = self.get(run)?;
        if !row.request.explicit_finish {
            return Err(invalid("maintenance requires explicit worker completion"));
        }
        let checkpoint:Option<(String,String,String,f64,bool)>=tx.query_row(
            "SELECT challenge_id,checkpoint_json,checkpoint_sha256,accepted_at,receipt_observed FROM maintenance_checkpoints \
             WHERE epoch=? AND agent_id=? AND attempt_id=? AND checkpoint_json IS NOT NULL",
            params![epoch,run.as_str(),attempt],|row|Ok((row.get(0)?,row.get(1)?,row.get(2)?,row.get(3)?,row.get(4)?))).optional()?;
        let Some((challenge, payload, checkpoint_sha, accepted_at, observed)) = checkpoint else {
            return Err(invalid("maintenance checkpoint is missing"));
        };
        if !observed {
            return Err(invalid("maintenance checkpoint receipt was not observed"));
        }
        let evidence: Option<(String, String, Option<String>, f64, String)> = tx
            .query_row(
                "SELECT kind,agent_id,attempt_id,at,data_json FROM events WHERE seq=?",
                [native_event_seq],
                |row| {
                    Ok((
                        row.get(0)?,
                        row.get(1)?,
                        row.get(2)?,
                        row.get(3)?,
                        row.get(4)?,
                    ))
                },
            )
            .optional()?;
        let Some((kind, event_agent, event_attempt, event_at, event_json)) = evidence else {
            return Err(invalid("native maintenance settlement evidence is missing"));
        };
        let (state,phase,cleanup_raw,history_raw,account):(String,Option<String>,Option<String>,String,Option<String>)=tx.query_row(
            "SELECT state,phase,cleanup_proof_json,adapter_state_json,selected_account_id FROM attempts WHERE id=? AND agent_id=?",params![attempt,run.as_str()],
            |row|Ok((row.get(0)?,row.get(1)?,row.get(2)?,row.get(3)?,row.get(4)?)))?;
        let proof_sha = self.maintenance_park_fingerprint(epoch, run, attempt, native_event_seq)?;
        if prior
            .as_ref()
            .is_some_and(|prior| prior.proof_sha256 != proof_sha)
        {
            return Err(Error::Conflict);
        }
        crate::maintenance_closing::validate_native_settlement(
            self,
            epoch,
            run,
            attempt,
            native_event_seq,
            at,
            prior.is_some(),
        )?;
        if (prior.is_some() && state != "maintenance_parked")
            || (prior.is_none() && state != "running")
        {
            return Err(Error::Conflict);
        }
        if kind != "maintenance_native_settled_v1"
            || event_agent != run.as_str()
            || event_attempt.as_deref() != Some(attempt)
            || event_at < accepted_at
            || event_at > at
        {
            return Err(invalid(
                "native maintenance settlement does not match the attempt",
            ));
        }
        let command: Option<(String,String,String,String,Option<String>)>=tx.query_row(
            "SELECT q.agent_id,q.kind,q.payload_json,q.state,q.result_json FROM maintenance_checkpoints c JOIN commands q ON q.id=c.command_id WHERE c.epoch=? AND c.agent_id=? AND c.attempt_id=?",
            params![epoch,run.as_str(),attempt],|row|Ok((row.get(0)?,row.get(1)?,row.get(2)?,row.get(3)?,row.get(4)?))).optional()?;
        let Some((command_agent, command_kind, command_payload, command_state, command_result)) =
            command
        else {
            return Err(invalid("maintenance pause command binding is missing"));
        };
        let command_payload: Value = serde_json::from_str(&command_payload)?;
        let command_result: Value = command_result
            .as_deref()
            .map(serde_json::from_str)
            .transpose()?
            .unwrap_or(Value::Null);
        let expected_text = crate::maintenance_checkpoint::maintenance_steer_text(&challenge);
        // The command's accepted flag alone is not native-read proof. The exact
        // callback and completed-turn evidence below are independently required.
        if command_agent != run.as_str()
            || command_kind != "steer"
            || command_state != "completed"
            || command_result["accepted"] != true
            || command_payload["maintenance_epoch"] != epoch
            || command_payload["maintenance_attempt"] != attempt
            || command_payload["maintenance_challenge"] != challenge
            || command_payload["text"].as_str() != Some(expected_text.as_str())
        {
            return Err(invalid(
                "maintenance pause command does not match the checkpoint",
            ));
        }
        let evidence: NativeSettlement = serde_json::from_str(&event_json)
            .map_err(|_| invalid("invalid native maintenance settlement evidence"))?;
        let harness = match evidence.protocol.as_str() {
            "codex" => "codex",
            "claude" if uuid::Uuid::parse_str(&evidence.turn_id).is_ok() => "claude-code",
            _ => {
                return Err(invalid(
                    "unsupported native maintenance settlement protocol",
                ));
            }
        };
        if evidence.version != 1
            || evidence.coverage != "owned_foreground_v1"
            || evidence.maintenance_epoch != epoch
            || evidence.agent_id != *run
            || evidence.attempt_id != attempt
            || evidence.challenge_id != challenge
            || evidence.receipt_sha256 != checkpoint_sha
            || !native_id(&evidence.thread_id)
            || !native_id(&evidence.turn_id)
            || !native_id(&evidence.item_id)
            || row.runtime_session_id.as_deref() != Some(evidence.thread_id.as_str())
        {
            return Err(invalid("native maintenance settlement binding is invalid"));
        }
        if sha256(payload.as_bytes()) != checkpoint_sha {
            return Err(Error::Integrity(
                "maintenance checkpoint digest mismatch".into(),
            ));
        }
        let checkpoint: MaintenancePauseRequest = serde_json::from_str(&payload)?;
        checkpoint.validate()?;
        if checkpoint.challenge_id != challenge {
            return Err(Error::Integrity(
                "maintenance checkpoint challenge mismatch".into(),
            ));
        }
        let cleanup: Cleanup = cleanup_raw
            .as_deref()
            .map(serde_json::from_str)
            .transpose()?
            .ok_or_else(|| invalid("maintenance cleanup proof is missing"))?;
        if phase.as_deref() != Some("cleanup_complete")
            || !cleanup.confirmed
            || !cleanup.group_gone
            || cleanup.descendants_gone != Some(true)
            || cleanup.scope != "verified_descendants"
            || cleanup.process_group_id != row.process_group_id
            || !row.process_group_id.is_some_and(|pgid| pgid > 1)
        {
            return Err(invalid("maintenance cleanup is not fully verified"));
        }
        let identity = row
            .identity
            .as_ref()
            .ok_or_else(|| invalid("maintenance launch authority is missing"))?;
        if identity["provider_identity_version"] != 2
            || account.as_deref().is_none_or(|account| account.is_empty())
        {
            return Err(invalid(
                "maintenance requires an account-bound provider execution",
            ));
        }
        if !identity["authority"]["provider"]
            .as_str()
            .is_some_and(native_id)
            || !identity["authority"]["assets_sha256"]
                .as_str()
                .is_some_and(digest)
        {
            return Err(invalid("maintenance launch authority is incomplete"));
        }
        let history: Value = serde_json::from_str(&history_raw)?;
        let native = &history["native_history"];
        let seal = &native["seal"];
        let root = seal["root"]
            .as_str()
            .filter(|root| Path::new(root).is_absolute());
        let relative = seal["relative"]
            .as_str()
            .filter(|relative| !relative.is_empty())
            .ok_or_else(|| invalid("maintenance history path is missing"))?;
        agent_run_platform::fs::relative(Path::new(relative))?;
        if native["attempt"] != attempt
            || native["provider"] != identity["authority"]["provider"]
            || native["assets_sha256"] != identity["authority"]["assets_sha256"]
            || native["task_proven"] != true
            || !native["task_proof"]["turn"].as_str().is_some_and(native_id)
            || !native["task_proof"]["input_sha256"]
                .as_str()
                .is_some_and(digest)
            || seal["session"] != evidence.thread_id
            || seal["harness"] != harness
            || identity["authority"]["harness"] != harness
            || root.is_none()
            || !seal["sha256"].as_str().is_some_and(digest)
            || !seal["bytes"].as_u64().is_some_and(|bytes| bytes > 0)
            || !seal["records"].as_u64().is_some_and(|records| records > 0)
        {
            return Err(invalid(
                "maintenance history is not sealed for the same execution",
            ));
        }
        if let Some(prior) = prior {
            tx.commit()?;
            return Ok(prior);
        }
        tx.execute("INSERT INTO maintenance_parks(epoch,agent_id,attempt_id,native_event_seq,proof_sha256,parked_at) VALUES(?,?,?,?,?,?)",params![epoch,run.as_str(),attempt,native_event_seq,proof_sha,at])?;
        let changed=tx.execute("UPDATE attempts SET state='maintenance_parked' WHERE id=? AND agent_id=? AND state='running' AND ownership_active=1",params![attempt,run.as_str()])?;
        if changed != 1 {
            return Err(Error::Conflict);
        }
        tx.commit()?;
        Ok(MaintenancePark {
            epoch,
            agent_id: run.clone(),
            attempt_id: attempt.into(),
            native_event_seq,
            proof_sha256: proof_sha,
            parked_at: at,
        })
    }
}
