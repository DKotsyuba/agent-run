//! Exact durable-command binding and idempotent Codex checkpoint observation.
//!
//! These helpers never start/stop a process, complete a task or park an attempt.
use super::maintenance::{Gap, Observer};
use crate::{Result, persist::Persist, state::Store};
use agent_run_domain::{domain::AgentId, worker::MaintenancePauseRequest};
use rusqlite::{OptionalExtension, params};
use serde_json::{Value, json};

/// Preserve definite rejection versus possible-send uncertainty without
/// propagating an auxiliary maintenance failure into the original runner.
pub fn send_result(result: Result<agent_run_adapters::io::RpcDisposition>) -> Value {
    use agent_run_adapters::io::RpcDisposition;
    match result {
        Ok(RpcDisposition::Replied(_)) => serde_json::json!({"accepted":true}),
        Ok(RpcDisposition::Rejected { .. }) => {
            serde_json::json!({"accepted":false,"reason":"native_rejected"})
        }
        Ok(RpcDisposition::UnsentPressure) => {
            serde_json::json!({"accepted":false,"reason":"backlog_pressure_unsent"})
        }
        Ok(RpcDisposition::Uncertain(reason)) => {
            serde_json::json!({"accepted":null,"reason":reason.reason()})
        }
        Err(_) => serde_json::json!({"accepted":null,"reason":"maintenance_input_unavailable"}),
    }
}

/// Retain a failed receipt write as unavailable maintenance evidence. The
/// original runner keeps executing; neither the write nor native input repeats.
pub fn receipt_outcome(observer: &mut Observer, result: Result<bool>) -> bool {
    match result {
        Ok(changed) => changed,
        Err(_) => {
            observer.unavailable(Gap::ReceiptPersistenceUnavailable);
            if let Some(logger) = crate::logging::configured() {
                logger.log(
                    crate::logging::Level::Warning,
                    "maintenance receipt acknowledgement unavailable; automatic pause held",
                );
            }
            false
        }
    }
}

/// One claimed durable maintenance command, plus its acknowledged native turn.
/// Fields are private so an ordinary steer cannot manufacture this binding.
#[derive(Clone)]
pub struct ControlBinding {
    epoch: i64,
    attempt: String,
    challenge: String,
    command: i64,
    payload: Value,
    turn: Option<String>,
}

impl std::fmt::Debug for ControlBinding {
    /// Keep opaque challenge/native identities out of diagnostics.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MaintenanceControlBinding")
            .field("epoch", &self.epoch)
            .field("command", &self.command)
            .finish_non_exhaustive()
    }
}

impl ControlBinding {
    /// Bind only after a correlated native input acknowledgment and its durable
    /// command result. An uncertain/rejected write must never call this method.
    pub fn accepted_turn(mut self, turn: &str) -> Self {
        if !turn.is_empty() {
            self.turn = Some(turn.into());
        }
        self
    }
}

/// Recognize reserved control metadata, including incomplete/malformed requests.
pub fn marked(payload: &Value) -> bool {
    [
        "maintenance_epoch",
        "maintenance_attempt",
        "maintenance_challenge",
    ]
    .into_iter()
    .any(|field| payload.get(field).is_some())
}

/// Verify one still-claimed command against its open cohort and current owned
/// attempt before native input. Returns None for unmarked/stale/malformed
/// metadata; it never substitutes the currently active attempt for the binding.
pub fn claimed_control(
    store: &Store<'_>,
    run: &AgentId,
    attempt: &str,
    command: i64,
    payload: &Value,
) -> Result<Option<ControlBinding>> {
    let Some(epoch) = payload["maintenance_epoch"]
        .as_i64()
        .filter(|epoch| *epoch > 0)
    else {
        return Ok(None);
    };
    let Some(challenge) = payload["maintenance_challenge"].as_str() else {
        return Ok(None);
    };
    if !payload
        .as_object()
        .is_some_and(|object| object.len() == 4 && object.get("text").is_some_and(Value::is_string))
        || payload["maintenance_attempt"].as_str() != Some(attempt)
        || (MaintenancePauseRequest {
            challenge_id: challenge.into(),
            checkpoint: "validation".into(),
        })
        .validate()
        .is_err()
    {
        return Ok(None);
    }
    let stored:Option<String>=store.conn.query_row(
        "SELECT q.payload_json FROM maintenance_checkpoints c JOIN maintenance_fences f ON f.epoch=c.epoch \
         JOIN maintenance_cohort m ON m.epoch=c.epoch AND m.agent_id=c.agent_id \
         JOIN commands q ON q.id=c.command_id JOIN agents a ON a.id=c.agent_id JOIN attempts t ON t.id=c.attempt_id AND t.agent_id=a.id \
         JOIN worker_lifecycle l ON l.attempt_id=t.id \
         WHERE c.epoch=? AND c.agent_id=? AND c.attempt_id=? AND c.challenge_id=? AND q.id=? AND q.agent_id=a.id \
         AND q.kind='steer' AND q.state='claimed' AND f.ended_at IS NULL AND c.checkpoint_json IS NULL \
         AND t.ownership_active=1 AND t.finished_at IS NULL AND t.state='running' AND a.status='running' AND a.finished_at IS NULL AND a.sequence=m.sequence \
         AND l.finish_json IS NULL AND json_extract(q.payload_json,'$.maintenance_epoch')=c.epoch \
         AND json_extract(q.payload_json,'$.maintenance_attempt')=c.attempt_id AND json_extract(q.payload_json,'$.maintenance_challenge')=c.challenge_id \
         AND NOT EXISTS(SELECT 1 FROM commands x WHERE x.agent_id=a.id AND x.kind='cancel' AND x.state IN ('pending','claimed'))",
        params![epoch,run.as_str(),attempt,challenge,command],|row|row.get(0)).optional()?;
    let matched = stored
        .as_deref()
        .and_then(|raw| serde_json::from_str::<Value>(raw).ok())
        .as_ref()
        == Some(payload);
    Ok(matched.then(|| ControlBinding {
        epoch,
        attempt: attempt.into(),
        challenge: challenge.into(),
        command,
        payload: payload.clone(),
        turn: None,
    }))
}

/// Recover the exact content-free native proof only while its accepted
/// checkpoint, pinned attempt, completed command and open fence still match.
pub fn native_settlement(
    store: &Persist,
    run: &AgentId,
    binding: &ControlBinding,
    observer: &Observer,
) -> Result<Option<agent_run_store::maintenance_park::NativeSettlement>> {
    let Some(native) = observer.settled_checkpoint() else {
        return Ok(None);
    };
    if store.bound_attempt() != Some(binding.attempt.as_str())
        || binding.turn.as_deref() != Some(native.turn_id)
        || binding.challenge != native.challenge_id
    {
        return Ok(None);
    }
    let snapshot = store.conn().unchecked_transaction()?;
    let Some(input) =
        store.maintenance_checkpoint_intent(binding.epoch, run, &binding.attempt, true)?
    else {
        return Ok(None);
    };
    let digest = agent_run_domain::canonical::sha256_hex(&serde_json::to_value(&input)?, true);
    if input.challenge_id != binding.challenge || digest != native.receipt_sha256 {
        return Ok(None);
    }
    let row:Option<(String,String)>=store.conn.query_row(
        "SELECT q.payload_json,q.result_json FROM maintenance_checkpoints m JOIN commands q ON q.id=m.command_id \
         JOIN agents a ON a.id=m.agent_id WHERE m.epoch=? AND m.agent_id=? AND m.attempt_id=? AND m.challenge_id=? \
         AND m.command_id=? AND m.receipt_observed=1 AND a.runtime_session_id=? AND q.agent_id=a.id AND q.kind='steer' AND q.state='completed' \
         AND NOT EXISTS(SELECT 1 FROM commands x WHERE x.agent_id=a.id AND x.id<>q.id AND x.state='claimed') \
         AND NOT EXISTS(SELECT 1 FROM commands x WHERE x.agent_id=a.id AND x.kind='steer' AND x.state IN ('pending','claimed') AND json_extract(x.payload_json,'$.native_completion')=1)",
        params![binding.epoch,run.as_str(),binding.attempt,binding.challenge,binding.command,native.thread_id],
        |row|Ok((row.get(0)?,row.get(1)?))).optional()?;
    let Some((payload, result)) = row else {
        return Ok(None);
    };
    let payload: Value = serde_json::from_str(&payload).map_err(|_| invalid_evidence())?;
    let result: Value = serde_json::from_str(&result).map_err(|_| invalid_evidence())?;
    if payload != binding.payload
        || result != json!({"accepted":true,"observation":"native_accepted"})
    {
        return Ok(None);
    }
    let proof = agent_run_store::maintenance_park::NativeSettlement {
        version: 1,
        protocol: "codex".into(),
        coverage: "owned_foreground_v1".into(),
        maintenance_epoch: binding.epoch,
        agent_id: run.clone(),
        attempt_id: binding.attempt.clone(),
        challenge_id: binding.challenge.clone(),
        receipt_sha256: digest,
        thread_id: native.thread_id.into(),
        turn_id: native.turn_id.into(),
        item_id: native.item_id.into(),
    };
    snapshot.commit()?;
    Ok(Some(proof))
}

/// Do not expose stored command contents when decoding proof evidence fails.
fn invalid_evidence() -> crate::Error {
    crate::error::invalid("maintenance command evidence is invalid")
}

/// Write one exact native settlement and its pinned closing boundary. An
/// uncertain outcome must be held and reconciled, never issued under a new id.
pub(super) async fn commit_settlement(
    store: &mut Persist,
    run: &AgentId,
    proof: &agent_run_store::maintenance_park::NativeSettlement,
) -> Result<i64> {
    if proof.agent_id != *run || store.bound_attempt() != Some(proof.attempt_id.as_str()) {
        return Err(crate::error::invalid(
            "maintenance settlement authority is unavailable",
        ));
    }
    let value = serde_json::to_value(proof).map_err(|_| invalid_evidence())?;
    let event = store
        .event_durable_seq(run, "maintenance_native_settled_v1", &value)
        .await?;
    store
        .begin_maintenance_closing(
            run,
            proof.maintenance_epoch,
            &proof.attempt_id,
            event,
            crate::domain::now(),
        )
        .await?;
    Ok(event)
}

/// Consume frames while holding an uncertain boundary. A prior accepted real
/// finish remains observable; no task input is submitted from this path.
pub(super) async fn observe_held_frame(
    process: &mut agent_run_adapters::io::Process,
    store: &mut Persist,
    record: &crate::state::Record,
    thread: &str,
    turn: &str,
    frame: &Value,
) {
    let p = &frame["params"];
    let item = &p["item"];
    if frame["method"] == "item/completed"
        && p["threadId"] == thread
        && p["turnId"] == turn
        && item["type"] == "mcpToolCall"
        && item["server"] == agent_run_domain::worker::SERVER_NAME
        && item["tool"] == "finish"
        && item["status"] == "completed"
        && item.get("error").is_none_or(Value::is_null)
        && item
            .pointer("/result/isError")
            .is_none_or(|value| value == false)
    {
        let text = item
            .pointer("/result/content")
            .map(Value::to_string)
            .unwrap_or_default();
        let _ = crate::worker_finish::receipt(process, store, record, &text).await;
    }
}

/// Match exact native callback correlation against the broker's immutable
/// checkpoint and current pinned attempt, then submit one idempotent receipt
/// write. Unknown native send, stale binding, missing acceptance or any observer
/// gap writes nothing. Success never implies process or task completion.
pub async fn observe(
    store: &mut Persist,
    run: &AgentId,
    binding: Option<&ControlBinding>,
    observer: &mut Observer,
) -> Result<bool> {
    if observer.receipt_recorded() {
        return Ok(false);
    }
    let Some(binding) = binding else {
        return Ok(false);
    };
    if store.bound_attempt() != Some(binding.attempt.as_str()) {
        return Ok(false);
    }
    let Some(candidate) = observer.receipt_candidate() else {
        return Ok(false);
    };
    if binding.turn.as_deref() != Some(candidate.turn_id)
        || binding.challenge != candidate.challenge_id
    {
        return Ok(false);
    }
    let native_digest = candidate.receipt_sha256.to_owned();
    let command:Option<(i64,String)>=store.conn.query_row(
        "SELECT c.command_id,q.result_json FROM maintenance_checkpoints c JOIN commands q ON q.id=c.command_id \
         WHERE c.epoch=? AND c.agent_id=? AND c.attempt_id=? AND c.challenge_id=? AND q.agent_id=c.agent_id AND q.kind='steer' AND q.state='completed'",
        params![binding.epoch,run.as_str(),binding.attempt,binding.challenge],|row|Ok((row.get(0)?,row.get(1)?))).optional()?;
    let Some((command, result)) = command else {
        return Ok(false);
    };
    if command != binding.command || serde_json::from_str::<Value>(&result)?["accepted"] != true {
        return Ok(false);
    }
    let Some(input) =
        store.maintenance_checkpoint_intent(binding.epoch, run, &binding.attempt, false)?
    else {
        return Ok(false);
    };
    let expected = agent_run_domain::canonical::sha256_hex(&serde_json::to_value(&input)?, true);
    if input.challenge_id != binding.challenge || expected != native_digest {
        observer.unavailable(Gap::ReceiptMismatch);
        return Ok(false);
    }
    observer.expect_checkpoint(&input.challenge_id, &expected);
    if observer.receipt_candidate().is_none() {
        return Ok(false);
    }
    let changed = store
        .observe_maintenance_checkpoint(
            run,
            binding.epoch,
            &binding.attempt,
            &binding.challenge,
            &expected,
        )
        .await?;
    observer.record_receipt();
    Ok(changed)
}
