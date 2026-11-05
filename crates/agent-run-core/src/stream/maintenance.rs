//! Passive, conservative Claude maintenance event correlation.
//!
//! This tracks only native protocol evidence. Even `Settled` does not prove
//! process cleanup, complete detached-process or remote-effect quiescence, a
//! sealed native history, permission to stop, or permission to send a resume.

use crate::{Result, domain::AgentId, error::invalid, persist::Persist};
use agent_run_domain::worker::{MaintenancePauseReceipt, MaintenancePauseRequest};
use rusqlite::{OptionalExtension, params};
use serde_json::Value;
use std::{
    collections::{BTreeMap, BTreeSet},
    fmt,
};

const MAX_ID: usize = 1024;
const MAX_INPUTS: usize = 4096;
const MAX_PARTIAL_JSON: usize = 64 * 1024;
const MAX_FOREGROUND_TOOLS: usize = 64;
const PAUSE_TOOL: &str = "mcp__agent_run_worker__maintenance_pause";

/// Content-free reason this attempt cannot establish a maintenance boundary.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Gap {
    MissingIdentity,
    ForeignIdentity,
    DuplicateOrUnstarted,
    InputCorrelation,
    InvalidCheckpoint,
    ReceiptMismatch,
    NativeFailure,
    UnsupportedToolCoverage,
    AsyncCoverageUnavailable,
    InheritedWorkUnknown,
    ActivityAfterCheckpoint,
    ObservationLimit,
}

/// Local native evidence only; none of these variants authorizes stopping work.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Assessment {
    /// No independently accepted checkpoint is bound to this observer.
    Unarmed,
    /// The exact private callback receipt has not yet been observed.
    AwaitingReceipt,
    /// The callback receipt was seen but its native input has not completed.
    AwaitingTurnCompletion,
    /// An observed identity, ordering or coverage gap prevents this proof.
    Unavailable(Gap),
    /// The supported local native sequence settled; other shutdown proofs remain.
    Settled,
}

#[derive(Clone, PartialEq, Eq)]
struct Checkpoint {
    challenge: String,
    digest: String,
}

struct ActiveCall {
    id: String,
    stream_index: Option<u64>,
    partial_json: String,
    request: Option<Checkpoint>,
}

/// One built-in foreground tool awaiting its exact native result.
struct ForegroundCall {
    name: &'static str,
    stream_index: Option<u64>,
    partial_json: String,
    input_confirmed: bool,
}

/// Trusted steering identity kept only in the current supervisor attempt.
pub(super) struct TrackedSteer {
    epoch: i64,
    command_id: i64,
    attempt: String,
    challenge: String,
    session: String,
    input: String,
    examined: bool,
    receipt_confirmed: bool,
}

/// Immutable accepted receipt selected for one pinned supervisor write.
pub(super) struct ReceiptObservation {
    pub(super) epoch: i64,
    pub(super) attempt: String,
    pub(super) challenge: String,
    pub(super) digest: String,
}

/// Maintenance metadata must never be treated as ordinary steering on failure.
pub(super) fn is_maintenance_steer(payload: &Value) -> bool {
    [
        "maintenance_epoch",
        "maintenance_attempt",
        "maintenance_challenge",
    ]
    .iter()
    .any(|name| payload.get(name).is_some())
}

/// Keep a failed finish read unavailable for maintenance while preserving the
/// ordinary steering path's existing error contract.
pub(super) fn finish_status(
    intent: Result<Option<agent_run_domain::worker::FinishRequest>>,
    maintenance: bool,
) -> Result<Option<bool>> {
    match intent {
        Ok(intent) => Ok(Some(intent.is_some())),
        Err(_) if maintenance => Ok(None),
        Err(error) => Err(error),
    }
}

/// Correlate a claimed command to the exact open checkpoint before native send.
/// Missing, malformed, stale, unpinned or foreign records are refused without
/// exposing command contents. The final writer observation rechecks liveness.
pub(super) fn track_steer(
    store: &Persist,
    run: &AgentId,
    command_id: i64,
    payload: &Value,
    session: Option<&str>,
    input_uuid: &str,
) -> Result<TrackedSteer> {
    let reject = || invalid("maintenance steering context is unavailable");
    let object = payload.as_object().ok_or_else(reject)?;
    if object.len() != 4 || !object.get("text").is_some_and(Value::is_string) {
        return Err(reject());
    }
    let epoch = object
        .get("maintenance_epoch")
        .and_then(Value::as_i64)
        .filter(|v| *v > 0)
        .ok_or_else(reject)?;
    let attempt = object
        .get("maintenance_attempt")
        .and_then(Value::as_str)
        .filter(|v| valid_id(v))
        .ok_or_else(reject)?;
    let challenge = object
        .get("maintenance_challenge")
        .and_then(Value::as_str)
        .ok_or_else(reject)?;
    let check = MaintenancePauseRequest {
        challenge_id: challenge.into(),
        checkpoint: "validation".into(),
    };
    check.validate()?;
    let session = session.filter(|v| valid_id(v)).ok_or_else(reject)?;
    if command_id <= 0 || !valid_uuid(input_uuid) || store.bound_attempt() != Some(attempt) {
        return Err(reject());
    }
    let raw:Option<String> = store.conn().query_row(
        "SELECT c.payload_json FROM maintenance_checkpoints m \
         JOIN maintenance_fences f ON f.epoch=m.epoch \
         JOIN maintenance_cohort cohort ON cohort.epoch=m.epoch AND cohort.agent_id=m.agent_id \
         JOIN agents a ON a.id=m.agent_id AND a.sequence=cohort.sequence \
         JOIN attempts t ON t.id=m.attempt_id AND t.agent_id=a.id \
         JOIN worker_lifecycle l ON l.attempt_id=t.id \
         JOIN commands c ON c.id=m.command_id AND c.agent_id=a.id \
         WHERE m.epoch=? AND m.agent_id=? AND m.attempt_id=? AND m.challenge_id=? AND m.command_id=? \
         AND f.ended_at IS NULL AND a.status='running' AND a.finished_at IS NULL \
         AND a.runtime_session_id=? AND t.ownership_active=1 AND t.state='running' AND t.finished_at IS NULL \
         AND l.finish_json IS NULL AND c.kind='steer' AND c.state='claimed' \
         AND NOT EXISTS(SELECT 1 FROM commands x WHERE x.agent_id=a.id AND x.kind='cancel' AND x.state IN ('pending','claimed'))",
        params![epoch,run.as_str(),attempt,challenge,command_id,session],|row|row.get(0),
    ).optional()?;
    let stored: Value =
        serde_json::from_str(raw.as_deref().ok_or_else(reject)?).map_err(|_| reject())?;
    if stored != *payload {
        return Err(reject());
    }
    Ok(TrackedSteer {
        epoch,
        command_id,
        attempt: attempt.into(),
        challenge: challenge.into(),
        session: session.into(),
        input: input_uuid.into(),
        examined: false,
        receipt_confirmed: false,
    })
}

/// Match a native receipt to immutable broker intent exactly once. This only
/// prepares a supervisor write; missing or changed state leaves no confirmation.
pub(super) fn accepted_receipt(
    observer: &mut Observer,
    store: &Persist,
    run: &AgentId,
    binding: &mut TrackedSteer,
    contains_secret: impl FnOnce(&str) -> bool,
) -> Option<ReceiptObservation> {
    if binding.examined || !observer.has_native_receipt() {
        return None;
    }
    binding.examined = true;
    let input =
        match store.maintenance_checkpoint_intent(binding.epoch, run, &binding.attempt, false) {
            Ok(Some(input)) => input,
            _ => {
                observer.unavailable(Gap::InvalidCheckpoint);
                return None;
            }
        };
    if input.challenge_id != binding.challenge || contains_secret(&input.checkpoint) {
        observer.unavailable(Gap::InvalidCheckpoint);
        return None;
    }
    let value = match serde_json::to_value(&input) {
        Ok(value) => value,
        Err(_) => {
            observer.unavailable(Gap::InvalidCheckpoint);
            return None;
        }
    };
    let digest = agent_run_domain::canonical::sha256_hex(&value, true);
    observer.expect_checkpoint(
        &binding.attempt,
        &binding.session,
        &binding.input,
        &binding.challenge,
        &digest,
    );
    if !matches!(
        observer.assessment(),
        Assessment::AwaitingTurnCompletion | Assessment::Settled
    ) {
        observer.unavailable(Gap::ReceiptMismatch);
        return None;
    }
    Some(ReceiptObservation {
        epoch: binding.epoch,
        attempt: binding.attempt.clone(),
        challenge: binding.challenge.clone(),
        digest,
    })
}

/// Commit the already matched native receipt once under the pinned supervisor.
/// Unknown or rejected writes invalidate local proof and are never replayed here.
pub(super) async fn confirm_receipt(
    store: &mut Persist,
    observer: &mut Observer,
    run: &AgentId,
    binding: &mut TrackedSteer,
    receipt: ReceiptObservation,
) -> bool {
    if !binding.examined
        || !observer.has_native_receipt()
        || !matches!(
            observer.assessment(),
            Assessment::AwaitingTurnCompletion | Assessment::Settled
        )
        || receipt.epoch != binding.epoch
        || receipt.attempt != binding.attempt
        || receipt.challenge != binding.challenge
        || observer
            .expected
            .as_ref()
            .is_none_or(|expected| expected.digest != receipt.digest)
    {
        observer.unavailable(Gap::ReceiptMismatch);
        return false;
    }
    if binding.receipt_confirmed {
        return true;
    }
    match store
        .observe_maintenance_checkpoint(
            run,
            receipt.epoch,
            &receipt.attempt,
            &receipt.challenge,
            &receipt.digest,
        )
        .await
    {
        Ok(_) => {
            binding.receipt_confirmed = true;
            true
        }
        Err(_) => {
            observer.unavailable(Gap::InvalidCheckpoint);
            false
        }
    }
}

/// Return exact content-free native settlement only after a confirmed receipt,
/// successful matching result and one read snapshot that still has no cancel,
/// finish, closed fence or changed attempt. This does not authorize cleanup.
pub(super) fn native_settlement(
    observer: &Observer,
    store: &Persist,
    run: &AgentId,
    binding: &TrackedSteer,
) -> Result<Option<agent_run_store::maintenance_park::NativeSettlement>> {
    if !binding.receipt_confirmed
        || observer.assessment() != Assessment::Settled
        || store.bound_attempt() != Some(binding.attempt.as_str())
    {
        return Ok(None);
    }
    let Some((native, input_uuid, item_id)) = observer.receipt.as_ref() else {
        return Ok(None);
    };
    if input_uuid != &binding.input
        || native.challenge != binding.challenge
        || observer.session.as_deref() != Some(binding.session.as_str())
    {
        return Ok(None);
    }
    let snapshot = store.conn().unchecked_transaction()?;
    let Some(checkpoint) =
        store.maintenance_checkpoint_intent(binding.epoch, run, &binding.attempt, true)?
    else {
        return Ok(None);
    };
    let value = serde_json::to_value(&checkpoint)?;
    let digest = agent_run_domain::canonical::sha256_hex(&value, true);
    if checkpoint.challenge_id != binding.challenge || digest != native.digest {
        return Ok(None);
    }
    let command_matches:bool = store.conn().query_row(
        "SELECT EXISTS(SELECT 1 FROM maintenance_checkpoints m JOIN commands c ON c.id=m.command_id \
         JOIN agents a ON a.id=m.agent_id WHERE m.epoch=? AND m.agent_id=? AND m.attempt_id=? \
         AND m.challenge_id=? AND m.command_id=? AND m.receipt_observed=1 \
         AND a.runtime_session_id=? AND c.agent_id=a.id AND c.kind='steer' AND c.state='completed' \
         AND json_extract(c.result_json,'$.accepted')=1 \
         AND NOT EXISTS(SELECT 1 FROM commands q WHERE q.agent_id=a.id AND q.id<>c.id AND q.state='claimed') \
         AND NOT EXISTS(SELECT 1 FROM commands q WHERE q.agent_id=a.id AND q.kind='steer' AND q.state IN ('pending','claimed') AND json_extract(q.payload_json,'$.native_completion')=1))",
        params![binding.epoch,run.as_str(),binding.attempt,binding.challenge,binding.command_id,binding.session],
        |row|row.get(0),
    )?;
    if !command_matches {
        return Ok(None);
    }
    let proof = agent_run_store::maintenance_park::NativeSettlement {
        version: 1,
        protocol: "claude".into(),
        coverage: "owned_foreground_v1".into(),
        maintenance_epoch: binding.epoch,
        agent_id: run.clone(),
        attempt_id: binding.attempt.clone(),
        challenge_id: binding.challenge.clone(),
        receipt_sha256: digest,
        thread_id: binding.session.clone(),
        turn_id: binding.input.clone(),
        item_id: item_id.clone(),
    };
    snapshot.commit()?;
    Ok(Some(proof))
}

/// Persist the exact native event once, then commit its pre-cleanup boundary.
/// Any failure may be uncertain and the caller must hold without retrying.
pub(super) async fn commit_settlement(
    store: &mut Persist,
    run: &AgentId,
    proof: &agent_run_store::maintenance_park::NativeSettlement,
) -> Result<i64> {
    if proof.agent_id != *run || store.bound_attempt() != Some(proof.attempt_id.as_str()) {
        return Err(invalid("maintenance settlement authority is unavailable"));
    }
    let event = serde_json::to_value(proof)
        .map_err(|_| invalid("maintenance settlement evidence is invalid"))?;
    let event_seq = store
        .event_durable_seq(run, "maintenance_native_settled_v1", &event)
        .await?;
    store
        .begin_maintenance_closing(
            run,
            proof.maintenance_epoch,
            &proof.attempt_id,
            event_seq,
            crate::domain::now(),
        )
        .await?;
    Ok(event_seq)
}

/// While a closing outcome is uncertain, consume native frames without sending
/// new input. A real finish still wins if its exact tool reply can be observed.
pub(super) async fn observe_held_frame(
    process: &mut agent_run_adapters::io::Process,
    store: &mut Persist,
    record: &crate::state::Record,
    session: Option<&str>,
    names: &mut BTreeMap<String, String>,
    frame: &Value,
) {
    if session.is_none() || frame.get("session_id").and_then(Value::as_str) != session {
        return;
    }
    let mut note_tool = |block: &Value| {
        if block["type"] == "tool_use"
            && block["name"] == "mcp__agent_run_worker__finish"
            && let Some(id) = block["id"].as_str().filter(|id| valid_id(id))
        {
            names.insert(id.into(), "mcp__agent_run_worker__finish".into());
        }
    };
    if frame["type"] == "stream_event" && frame["event"]["type"] == "content_block_start" {
        note_tool(&frame["event"]["content_block"]);
    }
    if frame["type"] == "assistant"
        && let Some(blocks) = frame.pointer("/message/content").and_then(Value::as_array)
    {
        for block in blocks {
            note_tool(block);
        }
    }
    if frame["type"] == "user"
        && let Some(blocks) = frame.pointer("/message/content").and_then(Value::as_array)
    {
        for block in blocks {
            if block["type"] == "tool_result"
                && block.get("is_error").and_then(Value::as_bool) != Some(true)
                && block["tool_use_id"]
                    .as_str()
                    .and_then(|id| names.get(id))
                    .is_some_and(|name| name == "mcp__agent_run_worker__finish")
            {
                let text = block["content"]
                    .as_str()
                    .map(str::to_owned)
                    .unwrap_or_else(|| block["content"].to_string());
                let _ = crate::worker_finish::receipt(process, store, record, &text).await;
            }
        }
    }
}

/// Outcome after checking the already-buffered tail of one settled turn.
pub(super) enum BufferedSettlement {
    Clear,
    Held,
    StreamEnded,
}

/// A queue notice before the native replay is not an acknowledgement. Accept
/// only a queued/started notice for the exact continuation and session.
pub(super) fn resume_queue_notice(frame: &Value, session: &str, input: &str) -> bool {
    frame.as_object().is_some_and(|fields| {
        fields.keys().all(|key| {
            matches!(
                key.as_str(),
                "type" | "command_uuid" | "state" | "uuid" | "session_id"
            )
        })
    }) && frame["type"] == "command_lifecycle"
        && matches!(frame["state"].as_str(), Some("queued" | "started"))
        && frame["command_uuid"].as_str() == Some(input)
        && frame["session_id"].as_str() == Some(session)
        && frame["uuid"].as_str().is_some_and(valid_uuid)
}

/// A correlated model-request notice may precede the replay acknowledgement,
/// but it does not authorize continuation or substitute for that replay.
pub(super) fn resume_request_notice(frame: &Value, session: &str, input: &str) -> bool {
    if frame["type"] != "system"
        || frame["subtype"] != "status"
        || frame["status"] != "requesting"
        || frame["session_id"].as_str() != Some(session)
        || !frame["uuid"].as_str().is_some_and(valid_uuid)
        || frame.as_object().is_none_or(|fields| {
            fields.keys().any(|key| {
                !matches!(
                    key.as_str(),
                    "type"
                        | "subtype"
                        | "status"
                        | "uuid"
                        | "session_id"
                        | "user_message_uuid"
                        | "user_message_uuids"
                )
            })
        })
    {
        return false;
    }
    let one = frame.get("user_message_uuid");
    let many = frame.get("user_message_uuids");
    (one.is_some() || many.is_some())
        && one.is_none_or(|value| value.as_str() == Some(input))
        && many.is_none_or(|value| {
            value
                .as_array()
                .is_some_and(|ids| matches!(ids.as_slice(), [id] if id.as_str() == Some(input)))
        })
}

/// Consume only same-session quota notices and the completion of the exact
/// maintenance input. Any other buffered frame stays under the hold path.
#[expect(
    clippy::too_many_arguments,
    reason = "The existing process, evidence, identity, and finish observer each have separate owners"
)]
pub(super) async fn drain_settled_notices(
    process: &mut agent_run_adapters::io::Process,
    observer: &mut Observer,
    store: &mut Persist,
    record: &crate::state::Record,
    session: Option<&str>,
    input: &str,
    names: &mut BTreeMap<String, String>,
    signals: &mut agent_run_adapters::native_failure::ClaudeSignals,
) -> BufferedSettlement {
    use agent_run_adapters::io::Event;
    for _ in 0..32 {
        if !process.has_buffered_events() {
            return BufferedSettlement::Clear;
        }
        match process.next().await {
            Event::Json(frame) => {
                let notice = (frame["type"] == "command_lifecycle"
                    && frame["state"] == "completed"
                    && frame["command_uuid"].as_str() == Some(input))
                    || frame["type"] == "rate_limit_event";
                if notice {
                    signals.observe(&frame);
                    observer.observe(&frame, None);
                    if observer.assessment() == Assessment::Settled {
                        continue;
                    }
                }
                observe_held_frame(process, store, record, session, names, &frame).await;
                return BufferedSettlement::Held;
            }
            Event::OwnershipFailure(_) | Event::Eof | Event::Failure(_) => {
                return BufferedSettlement::StreamEnded;
            }
        }
    }
    if process.has_buffered_events() {
        BufferedSettlement::Held
    } else {
        BufferedSettlement::Clear
    }
}

/// An unconfirmed maintenance write is never permission to retry the native
/// send or to treat the attempt as ready, regardless of its failure class.
fn evidence_recorded<T>(observer: &mut Observer, result: Result<T>) -> bool {
    if result.is_ok() {
        true
    } else {
        observer.unavailable(Gap::InputCorrelation);
        false
    }
}

/// Retain and drain the existing observed-command recovery entry if the native
/// send occurred but its storage outcome is failed or unknown. Never resend it.
pub(super) async fn record_sent(
    store: &mut Persist,
    observer: &mut Observer,
    run: &AgentId,
    command_id: i64,
    fragments: Vec<agent_run_store::ops::JournalFragment>,
) -> bool {
    let result = store
        .observed_command_result(
            run,
            command_id,
            &serde_json::json!({"accepted":true}),
            "stdin_written",
            Some(false),
            fragments,
        )
        .await;
    if evidence_recorded(observer, result) {
        true
    } else {
        let _ = store.drain_recoveries().await;
        false
    }
}

/// Record a known-unsent maintenance refusal when storage permits it; an
/// unconfirmed storage outcome remains unknown and never causes a native send.
pub(super) async fn reject_unsent(
    store: &mut Persist,
    observer: &mut Observer,
    run: &AgentId,
    command_id: i64,
) {
    observer.unavailable(Gap::InputCorrelation);
    let _ = store
        .complete_command(
            run,
            command_id,
            &serde_json::json!({
                "accepted":false,"reason":"maintenance_context_unavailable"
            }),
        )
        .await;
}

/// An attempt-local observer of the supported Claude stream from its start.
/// Previously resumed sessions and any other tool have unknown background
/// coverage, so they cannot be certified by this narrow observer.
pub struct Observer {
    attempt: String,
    session: Option<String>,
    init_fingerprint: Option<String>,
    init_input: Option<String>,
    input: Option<String>,
    completed_inputs: BTreeSet<String>,
    synthetic_inputs: BTreeSet<String>,
    replay_seen: bool,
    input_activity: bool,
    message: Option<String>,
    message_stopped: bool,
    open_blocks: BTreeSet<u64>,
    seen_blocks: BTreeSet<u64>,
    block_types: BTreeMap<u64, &'static str>,
    delivered_blocks: BTreeSet<u64>,
    completed_messages: BTreeSet<String>,
    active: Option<ActiveCall>,
    foreground: BTreeMap<String, ForegroundCall>,
    completed_tools: BTreeSet<String>,
    expected: Option<Checkpoint>,
    expected_session: Option<String>,
    expected_input: Option<String>,
    receipt: Option<(Checkpoint, String, String)>,
    checkpoint_turn_completed: bool,
    gap: Option<Gap>,
}

impl fmt::Debug for Observer {
    /// Never include attempt, session, input, challenge or checkpoint data.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ClaudeMaintenanceObserver")
            .field("assessment", &self.assessment())
            .finish_non_exhaustive()
    }
}

impl Observer {
    /// Start before native input. A resumed session cannot prove inherited
    /// background work from this attempt's event stream alone.
    pub fn new(attempt: &str, resumed_session: bool) -> Self {
        Self {
            attempt: attempt.into(),
            session: None,
            init_fingerprint: None,
            init_input: None,
            input: None,
            completed_inputs: BTreeSet::new(),
            synthetic_inputs: BTreeSet::new(),
            replay_seen: false,
            input_activity: false,
            message: None,
            message_stopped: false,
            open_blocks: BTreeSet::new(),
            seen_blocks: BTreeSet::new(),
            block_types: BTreeMap::new(),
            delivered_blocks: BTreeSet::new(),
            completed_messages: BTreeSet::new(),
            active: None,
            foreground: BTreeMap::new(),
            completed_tools: BTreeSet::new(),
            expected: None,
            expected_session: None,
            expected_input: None,
            receipt: None,
            checkpoint_turn_completed: false,
            gap: if !valid_id(attempt) {
                Some(Gap::MissingIdentity)
            } else if resumed_session {
                Some(Gap::InheritedWorkUnknown)
            } else {
                None
            },
        }
    }

    /// Record one exact UUID only after the runner's input write succeeded.
    /// A partial/unknown write must instead call [`Self::unavailable`].
    pub fn input_sent(&mut self, uuid: &str) {
        if self.gap.is_some() {
            return;
        }
        if self.checkpoint_turn_completed {
            self.unavailable(Gap::ActivityAfterCheckpoint);
        } else if !valid_uuid(uuid) {
            self.unavailable(Gap::MissingIdentity);
        } else if self.input.is_some()
            || self.completed_inputs.contains(uuid)
            || self
                .expected_input
                .as_deref()
                .is_some_and(|expected| expected != uuid)
        {
            self.unavailable(Gap::InputCorrelation);
        } else if self.completed_inputs.len() >= MAX_INPUTS {
            self.unavailable(Gap::ObservationLimit);
        } else {
            self.input = Some(uuid.into());
            self.replay_seen = false;
            self.input_activity = false;
        }
    }

    /// Bind the accepted store digest to the exact attempt and challenge.
    /// A digest found only in native output is never broker acceptance proof.
    pub fn expect_checkpoint(
        &mut self,
        attempt: &str,
        session: &str,
        input_uuid: &str,
        challenge: &str,
        digest: &str,
    ) {
        if attempt != self.attempt || !valid_id(attempt) {
            self.unavailable(Gap::ForeignIdentity);
            return;
        }
        if !valid_id(session) || !valid_uuid(input_uuid) {
            self.unavailable(Gap::MissingIdentity);
            return;
        }
        if self
            .session
            .as_deref()
            .is_some_and(|prior| prior != session)
            || self
                .expected_session
                .as_deref()
                .is_some_and(|prior| prior != session)
            || self
                .expected_input
                .as_deref()
                .is_some_and(|prior| prior != input_uuid)
            || self
                .input
                .as_deref()
                .is_some_and(|prior| prior != input_uuid)
            || self
                .receipt
                .as_ref()
                .is_some_and(|(_, prior, _)| prior != input_uuid)
            || (self.completed_inputs.contains(input_uuid) && self.receipt.is_none())
        {
            self.unavailable(Gap::ForeignIdentity);
            return;
        }
        let input = MaintenancePauseRequest {
            challenge_id: challenge.into(),
            checkpoint: "validation".into(),
        };
        let receipt = MaintenancePauseReceipt {
            sha256: digest.into(),
            duplicate: false,
        };
        if input.validate().is_err() || receipt.validate().is_err() {
            self.unavailable(Gap::InvalidCheckpoint);
            return;
        }
        let expected = Checkpoint {
            challenge: challenge.into(),
            digest: digest.into(),
        };
        if self
            .expected
            .as_ref()
            .is_some_and(|prior| prior != &expected)
            || self
                .receipt
                .as_ref()
                .is_some_and(|(prior, _, _)| prior != &expected)
            || self
                .active
                .as_ref()
                .and_then(|call| call.request.as_ref())
                .is_some_and(|prior| prior != &expected)
        {
            self.unavailable(Gap::ReceiptMismatch);
        } else {
            self.expected = Some(expected);
            self.expected_session = Some(session.into());
            self.expected_input = Some(input_uuid.into());
        }
    }

    /// Mark an observation gap permanently; silence cannot repair it.
    pub fn unavailable(&mut self, gap: Gap) {
        self.gap.get_or_insert(gap);
    }

    /// A receipt candidate is usable only while no native observation gap exists.
    fn has_native_receipt(&self) -> bool {
        self.gap.is_none() && self.receipt.is_some()
    }

    /// Observe one native frame after ordinary input-correlation classification.
    /// Allow correlated command, quota, and model-request status notices;
    /// ambiguous notices and other system activity fail closed.
    /// `task_result` is Some(true) only for the runner's exact Task scope and
    /// cannot replace UUID matching.
    pub fn observe(&mut self, frame: &Value, task_result: Option<bool>) {
        if self.gap.is_some() {
            return;
        }
        let Some(kind) = frame["type"].as_str() else {
            self.unavailable(Gap::MissingIdentity);
            return;
        };
        if kind == "system" && frame["subtype"] == "init" {
            self.init(frame);
            return;
        }
        if kind == "system"
            && ((frame["subtype"] == "status" && frame["status"] == "requesting")
                || frame["subtype"] == "thinking_tokens")
        {
            self.model_request_notice(frame);
            return;
        }
        if kind == "system" {
            self.unavailable(Gap::AsyncCoverageUnavailable);
            return;
        }
        if kind == "command_lifecycle" {
            self.command_lifecycle(frame);
            return;
        }
        if kind == "rate_limit_event" {
            let Some(session) = frame["session_id"].as_str().filter(|id| valid_id(id)) else {
                self.unavailable(Gap::MissingIdentity);
                return;
            };
            if self.session.as_deref() != Some(session) {
                self.unavailable(Gap::ForeignIdentity);
                return;
            }
            match frame
                .pointer("/rate_limit_info/status")
                .and_then(Value::as_str)
            {
                Some("allowed" | "allowed_warning") => return,
                Some("rejected") => self.unavailable(Gap::NativeFailure),
                _ => self.unavailable(Gap::UnsupportedToolCoverage),
            }
            return;
        }
        if !matches!(kind, "stream_event" | "assistant" | "user" | "result") {
            self.unavailable(Gap::UnsupportedToolCoverage);
            return;
        }
        let Some(session) = frame["session_id"].as_str().filter(|id| valid_id(id)) else {
            self.unavailable(Gap::MissingIdentity);
            return;
        };
        if self.session.as_deref() != Some(session) {
            self.unavailable(Gap::ForeignIdentity);
            return;
        }
        if self.checkpoint_turn_completed {
            self.unavailable(Gap::ActivityAfterCheckpoint);
            return;
        }
        if frame.pointer("/origin/kind").and_then(Value::as_str) == Some("task-notification") {
            self.unavailable(Gap::AsyncCoverageUnavailable);
            return;
        }
        if matches!(kind, "stream_event" | "assistant" | "user")
            && !frame.get("parent_tool_use_id").is_some_and(Value::is_null)
        {
            self.unavailable(Gap::AsyncCoverageUnavailable);
            return;
        }
        if kind == "assistant"
            || (kind == "stream_event" && frame["event"]["type"] == "message_start")
        {
            self.input_activity = true;
        }
        match kind {
            "stream_event" => self.stream_event(&frame["event"]),
            "assistant" => self.assistant(frame),
            "user" => self.user(frame),
            "result" => self.result(frame, task_result),
            _ => unreachable!(),
        }
    }

    /// Accept an identical same-session init re-emitted before a new input's
    /// activity. Changing native configuration or active work fails closed.
    fn init(&mut self, frame: &Value) {
        let Some(session) = frame["session_id"].as_str().filter(|id| valid_id(id)) else {
            self.unavailable(Gap::MissingIdentity);
            return;
        };
        if self
            .expected_session
            .as_deref()
            .is_some_and(|expected| expected != session)
            || self
                .session
                .as_deref()
                .is_some_and(|expected| expected != session)
        {
            self.unavailable(Gap::ForeignIdentity);
            return;
        }
        if frame.get("user_message_uuid").is_some_and(|value| {
            value
                .as_str()
                .is_none_or(|id| self.input.as_deref() != Some(id))
        }) || frame.get("user_message_uuids").is_some_and(|value| {
            !value.as_array().is_some_and(
                |ids| matches!(ids.as_slice(), [id] if id.as_str() == self.input.as_deref()),
            )
        }) {
            self.unavailable(Gap::InputCorrelation);
            return;
        }
        let mut stable = frame.clone();
        let Some(fields) = stable.as_object_mut() else {
            self.unavailable(Gap::MissingIdentity);
            return;
        };
        for key in ["uuid", "user_message_uuid", "user_message_uuids"] {
            fields.remove(key);
        }
        let fingerprint = agent_run_domain::canonical::sha256_hex(&stable, true);
        if self.session.is_some() {
            if self.input.is_none()
                || self.input == self.init_input
                || self
                    .init_input
                    .as_ref()
                    .is_none_or(|prior| !self.completed_inputs.contains(prior))
                || self.input_activity
                || self.message.is_some()
                || self.active.is_some()
                || !self.foreground.is_empty()
                || !self.open_blocks.is_empty()
                || self.checkpoint_turn_completed
            {
                self.unavailable(Gap::DuplicateOrUnstarted);
            } else if self.init_fingerprint.as_deref() != Some(fingerprint.as_str()) {
                self.unavailable(Gap::ForeignIdentity);
            } else {
                self.init_input = self.input.clone();
            }
        } else {
            self.session = Some(session.into());
            self.init_fingerprint = Some(fingerprint);
            self.init_input = self.input.clone();
        }
    }

    /// Accept same-session request and thinking-progress metadata only while
    /// a runner input remains active; neither proves a successful result.
    fn model_request_notice(&mut self, frame: &Value) {
        let thinking = frame["subtype"] == "thinking_tokens";
        if frame.as_object().is_none_or(|fields| {
            fields.keys().any(|key| {
                !matches!(
                    key.as_str(),
                    "type"
                        | "subtype"
                        | "status"
                        | "uuid"
                        | "session_id"
                        | "user_message_uuid"
                        | "user_message_uuids"
                ) && !(thinking
                    && matches!(key.as_str(), "estimated_tokens" | "estimated_tokens_delta"))
            })
        }) || (thinking
            && (frame["estimated_tokens"].as_u64().is_none()
                || frame["estimated_tokens_delta"].as_i64().is_none()))
        {
            self.unavailable(Gap::AsyncCoverageUnavailable);
            return;
        }
        let Some(session) = frame["session_id"].as_str().filter(|id| valid_id(id)) else {
            self.unavailable(Gap::MissingIdentity);
            return;
        };
        if !frame["uuid"].as_str().is_some_and(valid_uuid) {
            self.unavailable(Gap::MissingIdentity);
        } else if self.session.as_deref() != Some(session) {
            self.unavailable(Gap::ForeignIdentity);
        } else if self.checkpoint_turn_completed {
            self.unavailable(Gap::ActivityAfterCheckpoint);
        } else if self.input.is_none()
            || frame.get("user_message_uuid").is_some_and(|value| {
                value
                    .as_str()
                    .is_none_or(|id| self.input.as_deref() != Some(id))
            })
            || frame.get("user_message_uuids").is_some_and(|value| {
                !value.as_array().is_some_and(
                    |ids| matches!(ids.as_slice(), [id] if id.as_str() == self.input.as_deref()),
                )
            })
        {
            self.unavailable(Gap::InputCorrelation);
        }
    }

    /// Correlate the CLI's queued-prompt metadata without using it as proof
    /// of a completed turn or permitting an unknown queued command.
    fn command_lifecycle(&mut self, frame: &Value) {
        if frame.as_object().is_none_or(|fields| {
            fields.keys().any(|key| {
                !matches!(
                    key.as_str(),
                    "type" | "command_uuid" | "state" | "uuid" | "session_id"
                )
            })
        }) {
            self.unavailable(Gap::AsyncCoverageUnavailable);
            return;
        }
        let Some(command) = frame["command_uuid"].as_str().filter(|id| valid_uuid(id)) else {
            self.unavailable(Gap::MissingIdentity);
            return;
        };
        if !frame["uuid"].as_str().is_some_and(valid_uuid) {
            self.unavailable(Gap::MissingIdentity);
            return;
        }
        let Some(session) = frame["session_id"].as_str().filter(|id| valid_id(id)) else {
            self.unavailable(Gap::MissingIdentity);
            return;
        };
        if self.session.as_deref().is_some_and(|seen| seen != session)
            || self
                .expected_session
                .as_deref()
                .is_some_and(|seen| seen != session)
        {
            self.unavailable(Gap::ForeignIdentity);
            return;
        }
        let current = self.input.as_deref() == Some(command);
        let completed = self.completed_inputs.contains(command);
        if !current && !completed {
            self.unavailable(Gap::InputCorrelation);
            return;
        }
        match frame["state"].as_str() {
            Some("queued" | "started") if current && !self.checkpoint_turn_completed => {}
            Some("completed") => {}
            Some("cancelled" | "discarded" | "refused") => {
                self.unavailable(Gap::NativeFailure);
                return;
            }
            Some("queued" | "started") => {
                self.unavailable(Gap::InputCorrelation);
                return;
            }
            _ => {
                self.unavailable(Gap::UnsupportedToolCoverage);
                return;
            }
        }
        if self.session.is_none() {
            self.expected_session.get_or_insert_with(|| session.into());
        }
    }

    /// Track streamed tool starts without treating partial JSON as a complete request.
    fn stream_event(&mut self, event: &Value) {
        match event["type"].as_str() {
            Some("message_start") => {
                if self.input.is_none() {
                    self.unavailable(Gap::InputCorrelation);
                    return;
                }
                let Some(id) = event
                    .pointer("/message/id")
                    .and_then(Value::as_str)
                    .filter(|id| valid_id(id))
                else {
                    self.unavailable(Gap::MissingIdentity);
                    return;
                };
                if self.message.is_some() || self.completed_messages.contains(id) {
                    self.unavailable(Gap::DuplicateOrUnstarted);
                } else {
                    self.message = Some(id.into());
                    self.message_stopped = false;
                }
            }
            Some("content_block_start") => {
                let Some(index) = event["index"].as_u64() else {
                    self.unavailable(Gap::MissingIdentity);
                    return;
                };
                if self.message.is_none()
                    || self.message_stopped
                    || self.seen_blocks.contains(&index)
                {
                    self.unavailable(Gap::DuplicateOrUnstarted);
                    return;
                }
                if self.seen_blocks.len() >= MAX_INPUTS {
                    self.unavailable(Gap::ObservationLimit);
                    return;
                }
                self.seen_blocks.insert(index);
                self.open_blocks.insert(index);
                let block_type = match event["content_block"]["type"].as_str() {
                    Some("tool_use") => "tool_use",
                    Some("text") => "text",
                    Some("thinking") => "thinking",
                    Some("redacted_thinking") => "redacted_thinking",
                    _ => {
                        self.unavailable(Gap::UnsupportedToolCoverage);
                        return;
                    }
                };
                self.block_types.insert(index, block_type);
                if block_type == "tool_use" {
                    self.start(&event["content_block"], Some(index));
                }
            }
            Some("content_block_delta") => {
                let Some(index) = event["index"].as_u64() else {
                    self.unavailable(Gap::MissingIdentity);
                    return;
                };
                if self.message.is_none()
                    || self.message_stopped
                    || !self.open_blocks.contains(&index)
                {
                    self.unavailable(Gap::DuplicateOrUnstarted);
                    return;
                }
                match event["delta"]["type"].as_str() {
                    Some("input_json_delta") => {
                        let Some(delta) = event["delta"]["partial_json"].as_str() else {
                            self.unavailable(Gap::MissingIdentity);
                            return;
                        };
                        let partial = match self.active.as_mut() {
                            Some(call) if call.stream_index == Some(index) => {
                                Some((&mut call.partial_json, call.request.is_some()))
                            }
                            _ => self
                                .foreground
                                .values_mut()
                                .find(|call| call.stream_index == Some(index))
                                .map(|call| (&mut call.partial_json, call.input_confirmed)),
                        };
                        let Some((partial, confirmed)) = partial else {
                            self.unavailable(Gap::MissingIdentity);
                            return;
                        };
                        if confirmed {
                            self.unavailable(Gap::InputCorrelation);
                        } else if partial.len().saturating_add(delta.len()) > MAX_PARTIAL_JSON {
                            self.unavailable(Gap::ObservationLimit);
                        } else {
                            partial.push_str(delta);
                        }
                    }
                    Some("text_delta") if event["delta"]["text"].is_string() => {}
                    Some("thinking_delta") if event["delta"]["thinking"].is_string() => {}
                    Some("signature_delta") if event["delta"]["signature"].is_string() => {}
                    _ => self.unavailable(Gap::UnsupportedToolCoverage),
                }
            }
            Some("content_block_stop") => {
                let Some(index) = event["index"].as_u64() else {
                    self.unavailable(Gap::MissingIdentity);
                    return;
                };
                if self.message.is_none()
                    || self.message_stopped
                    || !self.open_blocks.remove(&index)
                {
                    self.unavailable(Gap::DuplicateOrUnstarted);
                } else {
                    for call in self.foreground.values_mut() {
                        if call.stream_index == Some(index) && call.input_confirmed {
                            call.stream_index = None;
                        }
                    }
                }
            }
            Some("message_delta") if self.message.is_some() && !self.message_stopped => {}
            Some("message_stop")
                if self.message.is_some()
                    && !self.message_stopped
                    && self.open_blocks.is_empty() =>
            {
                self.message_stopped = true;
                if self.delivered_blocks == self.seen_blocks {
                    if self.completed_messages.len() >= MAX_INPUTS {
                        self.unavailable(Gap::ObservationLimit);
                        return;
                    }
                    let id = self.message.take().expect("checked pending message");
                    self.completed_messages.insert(id);
                    self.message_stopped = false;
                    self.seen_blocks.clear();
                    self.block_types.clear();
                    self.delivered_blocks.clear();
                }
            }
            _ => self.unavailable(Gap::UnsupportedToolCoverage),
        }
    }

    /// Correlate either the completed message or one full block emitted while
    /// its matching streamed block remains open; repeated blocks fail closed.
    fn assistant(&mut self, frame: &Value) {
        if self.input.is_none() {
            self.unavailable(Gap::InputCorrelation);
            return;
        }
        if frame["message"]["role"] != "assistant" {
            self.unavailable(Gap::ForeignIdentity);
            return;
        }
        let Some(id) = frame
            .pointer("/message/id")
            .and_then(Value::as_str)
            .filter(|id| valid_id(id))
        else {
            self.unavailable(Gap::MissingIdentity);
            return;
        };
        if self.message.as_deref() == Some(id) && !self.message_stopped {
            let Some(index) = self.open_blocks.iter().next().copied() else {
                self.unavailable(Gap::DuplicateOrUnstarted);
                return;
            };
            let blocks = frame["message"]["content"].as_array();
            let Some([block]) = blocks.map(Vec::as_slice) else {
                self.unavailable(Gap::DuplicateOrUnstarted);
                return;
            };
            if self.open_blocks.len() != 1
                || self.delivered_blocks.contains(&index)
                || block["type"].as_str() != self.block_types.get(&index).copied()
            {
                self.unavailable(Gap::DuplicateOrUnstarted);
                return;
            }
            if block["type"] == "tool_use" {
                self.start(block, None);
            }
            if self.gap.is_none() {
                self.delivered_blocks.insert(index);
            }
            return;
        }
        if self
            .message
            .as_deref()
            .is_some_and(|prior| prior != id || !self.message_stopped)
            || !self.open_blocks.is_empty()
            || self.completed_messages.contains(id)
        {
            self.unavailable(Gap::DuplicateOrUnstarted);
            return;
        }
        if self.completed_messages.len() >= MAX_INPUTS {
            self.unavailable(Gap::ObservationLimit);
            return;
        }
        self.completed_messages.insert(id.into());
        self.message = None;
        self.message_stopped = false;
        self.seen_blocks.clear();
        self.block_types.clear();
        self.delivered_blocks.clear();
        let Some(blocks) = frame["message"]["content"].as_array() else {
            self.unavailable(Gap::MissingIdentity);
            return;
        };
        for block in blocks {
            match block["type"].as_str() {
                Some("tool_use") => self.start(block, None),
                Some("text" | "thinking" | "redacted_thinking") => {}
                _ => self.unavailable(Gap::UnsupportedToolCoverage),
            }
        }
    }

    /// Record or complete one private pause tool start.
    fn start(&mut self, block: &Value, stream_index: Option<u64>) {
        if self.input.is_none() {
            self.unavailable(Gap::InputCorrelation);
            return;
        }
        let Some(id) = block["id"].as_str().filter(|id| valid_id(id)) else {
            self.unavailable(Gap::MissingIdentity);
            return;
        };
        if block["name"] != PAUSE_TOOL {
            self.start_foreground(id, block, stream_index);
            return;
        }
        if !self.foreground.is_empty() {
            self.unavailable(Gap::AsyncCoverageUnavailable);
            return;
        }
        if self.completed_tools.contains(id) || self.receipt.is_some() {
            self.unavailable(Gap::DuplicateOrUnstarted);
            return;
        }
        if stream_index.is_some() {
            if self.active.is_some() {
                self.unavailable(Gap::DuplicateOrUnstarted);
                return;
            }
            if block.get("input").is_some_and(|input| {
                !input.is_null() && input.as_object().is_none_or(|map| !map.is_empty())
            }) {
                self.unavailable(Gap::UnsupportedToolCoverage);
                return;
            }
            self.active = Some(ActiveCall {
                id: id.into(),
                stream_index,
                partial_json: String::new(),
                request: None,
            });
            return;
        }
        let Some(input) = serde_json::from_value::<MaintenancePauseRequest>(block["input"].clone())
            .ok()
            .filter(|input| input.validate().is_ok())
        else {
            self.unavailable(Gap::InvalidCheckpoint);
            return;
        };
        let Ok(value) = serde_json::to_value(&input) else {
            self.unavailable(Gap::InvalidCheckpoint);
            return;
        };
        let checkpoint = Checkpoint {
            challenge: input.challenge_id.clone(),
            digest: agent_run_domain::canonical::sha256_hex(&value, true),
        };
        if self
            .expected
            .as_ref()
            .is_some_and(|expected| expected != &checkpoint)
        {
            self.unavailable(Gap::ReceiptMismatch);
            return;
        }
        match self.active.as_mut() {
            Some(call)
                if call.id == id && call.stream_index.is_some() && call.request.is_none() =>
            {
                if !call.partial_json.is_empty() {
                    let streamed =
                        serde_json::from_str::<MaintenancePauseRequest>(&call.partial_json)
                            .ok()
                            .filter(|value| value.validate().is_ok());
                    if streamed.as_ref() != Some(&input) {
                        self.unavailable(Gap::InvalidCheckpoint);
                        return;
                    }
                }
                call.request = Some(checkpoint)
            }
            Some(_) => self.unavailable(Gap::DuplicateOrUnstarted),
            None => {
                self.active = Some(ActiveCall {
                    id: id.into(),
                    stream_index: None,
                    partial_json: String::new(),
                    request: Some(checkpoint),
                })
            }
        }
    }

    /// Match one supported native foreground call through its complete input.
    /// Unknown tools and explicit background requests retain the safe hold.
    fn start_foreground(&mut self, id: &str, block: &Value, stream_index: Option<u64>) {
        let Some(name) = foreground_tool(block["name"].as_str()) else {
            self.unavailable(Gap::UnsupportedToolCoverage);
            return;
        };
        if self.active.is_some() || self.expected.is_some() || self.receipt.is_some() {
            self.unavailable(Gap::ActivityAfterCheckpoint);
            return;
        }
        if self.completed_tools.contains(id) {
            self.unavailable(Gap::DuplicateOrUnstarted);
            return;
        }
        if stream_index.is_some() {
            if self.foreground.contains_key(id) || self.foreground.len() >= MAX_FOREGROUND_TOOLS {
                self.unavailable(Gap::ObservationLimit);
                return;
            }
            if block.get("input").is_some_and(|input| {
                !input.is_null() && input.as_object().is_none_or(|map| !map.is_empty())
            }) {
                self.unavailable(Gap::UnsupportedToolCoverage);
                return;
            }
            self.foreground.insert(
                id.into(),
                ForegroundCall {
                    name,
                    stream_index,
                    partial_json: String::new(),
                    input_confirmed: false,
                },
            );
            return;
        }
        let input = &block["input"];
        if !foreground_input(name, input) {
            self.unavailable(Gap::AsyncCoverageUnavailable);
            return;
        }
        if let Some(call) = self.foreground.get_mut(id) {
            if call.name != name || call.stream_index.is_none() || call.input_confirmed {
                self.unavailable(Gap::DuplicateOrUnstarted);
                return;
            }
            if !call.partial_json.is_empty()
                && serde_json::from_str::<Value>(&call.partial_json)
                    .ok()
                    .as_ref()
                    != Some(input)
            {
                self.unavailable(Gap::InputCorrelation);
                return;
            }
            call.partial_json.clear();
            call.input_confirmed = true;
        } else if self.foreground.len() >= MAX_FOREGROUND_TOOLS {
            self.unavailable(Gap::ObservationLimit);
        } else {
            self.foreground.insert(
                id.into(),
                ForegroundCall {
                    name,
                    stream_index: None,
                    partial_json: String::new(),
                    input_confirmed: true,
                },
            );
        }
    }

    /// Match native input replay and private tool result without interpreting prose.
    fn user(&mut self, frame: &Value) {
        if frame["message"]["role"] != "user" {
            self.unavailable(Gap::ForeignIdentity);
            return;
        }
        if frame
            .get("isReplay")
            .is_some_and(|value| !value.is_boolean())
        {
            self.unavailable(Gap::InputCorrelation);
            return;
        }
        if frame.get("isReplay").and_then(Value::as_bool) == Some(true) {
            let Some(uuid) = frame["uuid"].as_str().filter(|id| valid_uuid(id)) else {
                self.unavailable(Gap::MissingIdentity);
                return;
            };
            if self.input.as_deref() != Some(uuid) || self.replay_seen || self.input_activity {
                self.unavailable(Gap::InputCorrelation);
                return;
            }
            self.replay_seen = true;
            let content = &frame["message"]["content"];
            let one_text = content.as_str().is_some_and(|text| !text.is_empty())
                || content.as_array().is_some_and(|blocks| {
                    matches!(blocks.as_slice(), [block] if block["type"] == "text"
                        && block["text"].as_str().is_some_and(|text| !text.is_empty()))
                });
            if !one_text {
                self.unavailable(Gap::InputCorrelation);
            }
            return;
        }
        if frame
            .get("isSynthetic")
            .is_some_and(|value| !value.is_boolean())
        {
            self.unavailable(Gap::InputCorrelation);
            return;
        }
        if frame["isSynthetic"] == true
            && !frame
                .pointer("/message/content")
                .and_then(Value::as_array)
                .is_some_and(|blocks| {
                    !blocks.is_empty() && blocks.iter().all(|block| block["type"] == "tool_result")
                })
        {
            self.synthetic_user(frame);
            return;
        }
        let Some(blocks) = frame["message"]["content"].as_array() else {
            self.unavailable(Gap::InputCorrelation);
            return;
        };
        if blocks.is_empty() {
            self.unavailable(Gap::InputCorrelation);
            return;
        }
        for block in blocks {
            match block["type"].as_str() {
                Some("tool_result") => self.tool_result(frame, block),
                _ => self.unavailable(Gap::UnsupportedToolCoverage),
            }
        }
    }

    /// Retain only a bounded, same-turn native synthetic text companion.
    /// It is context, never a runner input or evidence of task completion.
    fn synthetic_user(&mut self, frame: &Value) {
        if frame.as_object().is_none_or(|fields| {
            fields.keys().any(|key| {
                !matches!(
                    key.as_str(),
                    "type"
                        | "message"
                        | "parent_tool_use_id"
                        | "session_id"
                        | "timestamp"
                        | "uuid"
                        | "isSynthetic"
                )
            })
        }) || frame["message"].as_object().is_none_or(|fields| {
            fields
                .keys()
                .any(|key| !matches!(key.as_str(), "role" | "content"))
        }) || self.input.is_none()
            || self.message.is_some()
            || !self.open_blocks.is_empty()
            || self.active.is_some()
            || !self.foreground.is_empty()
        {
            self.unavailable(Gap::AsyncCoverageUnavailable);
            return;
        }
        let Some(id) = frame["uuid"].as_str().filter(|id| valid_uuid(id)) else {
            self.unavailable(Gap::MissingIdentity);
            return;
        };
        let content = &frame["message"]["content"];
        let text = content.as_str().or_else(|| {
            let blocks = content.as_array()?;
            match blocks.as_slice() {
                [block]
                    if block["type"] == "text"
                        && block.as_object().is_some_and(|fields| {
                            fields
                                .keys()
                                .all(|key| matches!(key.as_str(), "type" | "text"))
                        }) =>
                {
                    block["text"].as_str()
                }
                _ => None,
            }
        });
        if text.is_none_or(|text| text.is_empty() || text.len() > MAX_PARTIAL_JSON)
            || self.input.as_deref() == Some(id)
            || self.completed_inputs.contains(id)
            || self.synthetic_inputs.contains(id)
        {
            self.unavailable(Gap::InputCorrelation);
        } else if self.synthetic_inputs.len() >= MAX_INPUTS {
            self.unavailable(Gap::ObservationLimit);
        } else {
            self.synthetic_inputs.insert(id.into());
        }
    }

    /// Settle one matching foreground tool or bind the private pause receipt.
    fn tool_result(&mut self, frame: &Value, block: &Value) {
        let Some(id) = block["tool_use_id"].as_str().filter(|id| valid_id(id)) else {
            self.unavailable(Gap::MissingIdentity);
            return;
        };
        if let Some(call) = self.foreground.remove(id) {
            if !call.input_confirmed || !foreground_result(call.name, frame, block) {
                self.unavailable(Gap::AsyncCoverageUnavailable);
            } else if self.completed_tools.len() >= MAX_INPUTS {
                self.unavailable(Gap::ObservationLimit);
            } else {
                self.completed_tools.insert(id.into());
            }
            return;
        }
        let Some(call) = self.active.take() else {
            self.unavailable(Gap::DuplicateOrUnstarted);
            return;
        };
        if call.id != id {
            self.unavailable(Gap::ForeignIdentity);
            return;
        }
        let Some(expected) = call.request else {
            self.unavailable(Gap::InvalidCheckpoint);
            return;
        };
        let Some(receipt) = parse_receipt(block) else {
            self.unavailable(Gap::NativeFailure);
            return;
        };
        if receipt.sha256 != expected.digest
            || self
                .expected
                .as_ref()
                .is_some_and(|prior| prior != &expected)
        {
            self.unavailable(Gap::ReceiptMismatch);
            return;
        }
        let Some(input) = self.input.clone() else {
            self.unavailable(Gap::InputCorrelation);
            return;
        };
        if self
            .expected_input
            .as_deref()
            .is_some_and(|expected| expected != input.as_str())
        {
            self.unavailable(Gap::InputCorrelation);
            return;
        }
        self.completed_tools.insert(id.into());
        self.receipt = Some((expected, input, id.into()));
    }

    /// A successful correlated result is the native turn boundary, never a task outcome.
    fn result(&mut self, frame: &Value, task_result: Option<bool>) {
        let Some(uuid) = frame["user_message_uuid"]
            .as_str()
            .filter(|id| valid_uuid(id))
        else {
            self.unavailable(Gap::MissingIdentity);
            return;
        };
        if self.input.as_deref() != Some(uuid) || task_result != Some(true) {
            self.unavailable(Gap::InputCorrelation);
            return;
        }
        if frame["subtype"] != "success" || frame["is_error"] != false {
            self.unavailable(Gap::NativeFailure);
            return;
        }
        if self.active.is_some() || !self.foreground.is_empty() || self.message.is_some() {
            self.unavailable(Gap::DuplicateOrUnstarted);
            return;
        }
        if self.expected.is_some() && self.receipt.is_none() {
            self.unavailable(Gap::DuplicateOrUnstarted);
            return;
        }
        self.completed_inputs.insert(uuid.into());
        self.input = None;
        self.checkpoint_turn_completed = self
            .receipt
            .as_ref()
            .is_some_and(|(_, origin, _)| origin == uuid);
    }

    /// Return local evidence without changing execution or process ownership.
    pub fn assessment(&self) -> Assessment {
        if let Some(gap) = self.gap {
            return Assessment::Unavailable(gap);
        }
        let Some(expected) = &self.expected else {
            return Assessment::Unarmed;
        };
        let Some((receipt, _, _)) = &self.receipt else {
            return Assessment::AwaitingReceipt;
        };
        if receipt != expected {
            return Assessment::Unavailable(Gap::ReceiptMismatch);
        }
        if !self.checkpoint_turn_completed {
            return Assessment::AwaitingTurnCompletion;
        }
        Assessment::Settled
    }
}

/// Only the native synchronous tools explicitly provided by the adapter are
/// eligible; arbitrary MCP tools and agent/subprocess managers remain unknown.
fn foreground_tool(name: Option<&str>) -> Option<&'static str> {
    match name? {
        "Read" => Some("Read"),
        "Grep" => Some("Grep"),
        "Glob" => Some("Glob"),
        "Edit" => Some("Edit"),
        "Write" => Some("Write"),
        "NotebookEdit" => Some("NotebookEdit"),
        "Bash" => Some("Bash"),
        _ => None,
    }
}

/// Refuse explicit background execution and malformed native tool input.
fn foreground_input(name: &str, input: &Value) -> bool {
    let Some(fields) = input.as_object() else {
        return false;
    };
    if ["run_in_background", "background", "async"]
        .iter()
        .any(|key| fields.get(*key).is_some_and(|value| value != false))
    {
        return false;
    }
    name != "Bash"
        || fields
            .get("command")
            .and_then(Value::as_str)
            .is_some_and(|s| !s.is_empty())
}

/// A Bash result needs native foreground completion evidence; a background,
/// interrupted, timed-out or ambiguous result cannot establish a boundary.
fn foreground_result(name: &str, frame: &Value, block: &Value) -> bool {
    if block
        .get("is_error")
        .is_some_and(|value| !value.is_boolean())
    {
        return false;
    }
    if name != "Bash" {
        return true;
    }
    let Some(result) = frame.get("tool_use_result").and_then(Value::as_object) else {
        return false;
    };
    // Some native cross-process results omit background fields while the
    // tool-result text still reports the background handoff.
    let plain = |text: &str| {
        !text
            .as_bytes()
            .windows(10)
            .any(|part| part.eq_ignore_ascii_case(b"background"))
    };
    let no_background_notice = match &block["content"] {
        Value::String(text) => plain(text),
        Value::Array(parts) => {
            !parts.is_empty()
                && parts
                    .iter()
                    .all(|part| part["type"] == "text" && part["text"].as_str().is_some_and(plain))
        }
        _ => false,
    };
    frame
        .pointer("/message/content")
        .and_then(Value::as_array)
        .is_some_and(|items| items.len() == 1)
        && no_background_notice
        && result.get("interrupted").and_then(Value::as_bool) == Some(false)
        && result.get("stdout").is_some_and(Value::is_string)
        && result.get("stderr").is_some_and(Value::is_string)
        && result.keys().all(|key| {
            matches!(
                key.as_str(),
                "stdout"
                    | "stderr"
                    | "interrupted"
                    | "isImage"
                    | "rawOutputPath"
                    | "backgroundTaskId"
                    | "backgroundOutputPath"
                    | "backgroundedByUser"
                    | "backgroundedByTurnAbort"
                    | "backgroundedToDeliverMessage"
                    | "backgroundCwdHint"
                    | "backgroundEndsWithFinalResponse"
                    | "timedOutAfterMs"
                    | "leftRunningPgid"
                    | "noExitStatus"
                    | "dangerouslyDisableSandbox"
                    | "returnCodeInterpretation"
                    | "noOutputExpected"
                    | "structuredContent"
                    | "persistedOutputPath"
                    | "persistedOutputSize"
                    | "staleReadFileStateHint"
                    | "ghRateLimitHint"
                    | "gitOperation"
                    | "bashEditDiff"
            )
        })
        && !result.iter().any(|(key, value)| {
            (key.starts_with("background")
                || matches!(
                    key.as_str(),
                    "timedOutAfterMs" | "leftRunningPgid" | "noExitStatus"
                ))
                && value != false
        })
}

/// Keep native identifiers bounded and reject control characters.
fn valid_id(value: &str) -> bool {
    !value.is_empty() && value.len() <= MAX_ID && !value.chars().any(char::is_control)
}

/// Only the canonical UUID spelling emitted by the native runner is accepted.
fn valid_uuid(value: &str) -> bool {
    uuid::Uuid::parse_str(value).is_ok_and(|id| id.hyphenated().to_string() == value)
}

/// Accept only one native text result containing the exact typed receipt.
fn parse_receipt(block: &Value) -> Option<MaintenancePauseReceipt> {
    if block.get("is_error").is_some_and(|value| value != false) {
        return None;
    }
    let text = match &block["content"] {
        Value::String(text) => text.as_str(),
        Value::Array(blocks) if blocks.len() == 1 && blocks[0]["type"] == "text" => {
            blocks[0]["text"].as_str()?
        }
        _ => return None,
    };
    let receipt: MaintenancePauseReceipt = serde_json::from_str(text).ok()?;
    receipt.validate().ok()?;
    Some(receipt)
}

#[cfg(test)]
mod receipt_tests {
    use super::*;
    use agent_run_domain::{
        storage::WriteAuthority,
        worker::{FinishGuard, WorkerCatalogProof},
    };
    use agent_run_store::ops::{AcceptMaintenanceCheckpoint, EnqueueCommand};
    use serde_json::json;
    use std::{collections::BTreeMap, time::Duration};

    const ATTEMPT: &str = "maintenance-attempt";
    const TOKEN: &str = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
    const CHALLENGE: &str = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
    const SESSION: &str = "fixture-session";
    const INPUT: &str = "11111111-1111-4111-8111-111111111111";

    /// Build native receipt observations without starting a process or using a socket.
    fn native(input: &MaintenancePauseRequest) -> Observer {
        let mut observer = Observer::new(ATTEMPT, false);
        observer.input_sent(INPUT);
        observer.observe(
            &json!({"type":"system","subtype":"init","session_id":SESSION}),
            None,
        );
        observer.observe(&json!({"type":"assistant","session_id":SESSION,"parent_tool_use_id":null,
            "message":{"id":"message","role":"assistant","content":[{"type":"tool_use","id":"pause","name":PAUSE_TOOL,"input":input}]}}),None);
        let digest =
            agent_run_domain::canonical::sha256_hex(&serde_json::to_value(input).unwrap(), true);
        observer.observe(&json!({"type":"user","session_id":SESSION,"parent_tool_use_id":null,
            "message":{"role":"user","content":[{"type":"tool_result","tool_use_id":"pause","is_error":false,
                "content":json!({"sha256":digest,"duplicate":false}).to_string()}]}}),None);
        observer
    }

    /// One real duplex writer fixture confirms only the exact accepted receipt,
    /// once; stale inputs and secret-bearing payloads never reach that write.
    #[tokio::test]
    async fn claude_maintenance_receipt_confirms_only_exact_pinned_evidence() {
        let home = tempfile::tempdir().unwrap();
        let mut store = agent_run_store::fixture::initialize(home.path()).unwrap();
        let run: AgentId = "ag-20261101-000000-0000000001".parse().unwrap();
        let mut role = json!({"role_name":"review","role_revision":"fixture","prompt":"review",
            "grants":{"write":false,"network":false,"allow_external_read_roots":false,"read_roots":[]},
            "skills":[],"mcp":[],"required_constraints":[],"auth":{"mode":"global","reference":null},"worker_mcp":true});
        role["config_revision"] = json!(agent_run_domain::canonical::sha256_hex(&role, true));
        let request = json!({"runtime":"mock","model":"fixture","profile":"review","task":"task","workdir":home.path(),"explicit_finish":true});
        store.conn.execute("INSERT INTO agents(id,runtime,model,profile,task,task_summary,workdir,request_json,status,created_at,config_revision,root_agent_id,identity_json,runtime_session_id) VALUES(?,'mock','fixture','review','task','task',?,?,'running',1,'fixture',?,?,?)",
            params![run.as_str(),home.path().to_str().unwrap(),request.to_string(),run.as_str(),json!({"provider_identity_version":2,"authority":{"role_payload":role,"harness":"claude-code"}}).to_string(),SESSION]).unwrap();
        store.conn.execute("INSERT INTO attempts(id,agent_id,number,state,adapter_state_json,created_at,ownership_active) VALUES(?,?,1,'running','{}',1,1)",params![ATTEMPT,run.as_str()]).unwrap();
        store
            .issue_worker_capability(&run, ATTEMPT, TOKEN, 1.0)
            .unwrap();
        store.begin_worker_lifecycle(&run, ATTEMPT, 1.0).unwrap();
        store
            .register_worker_catalog(
                &WorkerCatalogProof {
                    run_id: run.clone(),
                    attempt_id: ATTEMPT.into(),
                    token: TOKEN.into(),
                    version: 5,
                    digest: agent_run_domain::worker::maintenance_pause_catalog_digest(false),
                },
                1.0,
            )
            .unwrap();
        let epoch = store
            .begin_maintenance_fence("receipt-fixture", 2.0)
            .unwrap()
            .epoch;
        let sent = store
            .request_maintenance_steer(epoch, &run, ATTEMPT, CHALLENGE, 3.0)
            .unwrap();
        let (command, kind, payload) = store.claim_command_for_turn(&run, true).unwrap().unwrap();
        assert_eq!((command, kind.as_str()), (sent.command_id, "steer"));
        let input = MaintenancePauseRequest {
            challenge_id: CHALLENGE.into(),
            checkpoint: "fixture checkpoint".into(),
        };
        let prep = crate::writer_test_harness::prepare_supervisor(&mut store, &run);
        let writer = crate::writer_test_harness::Harness::start(home.path());
        let mut persist = writer.supervisor(home.path(), prep, &run).await;

        let mut foreign = payload.clone();
        foreign["maintenance_attempt"] = json!("foreign");
        assert!(track_steer(&persist, &run, command, &foreign, Some(SESSION), INPUT).is_err());
        assert!(track_steer(&persist, &run, command, &payload, Some("foreign"), INPUT).is_err());
        assert!(track_steer(&persist, &run, command + 1, &payload, Some(SESSION), INPUT).is_err());
        let mut unaccepted = native(&input);
        let mut binding =
            track_steer(&persist, &run, command, &payload, Some(SESSION), INPUT).unwrap();
        assert!(
            accepted_receipt(&mut unaccepted, &persist, &run, &mut binding, |_| false).is_none()
        );
        assert_eq!(
            store
                .conn
                .query_row(
                    "SELECT receipt_observed FROM maintenance_checkpoints",
                    [],
                    |row| row.get::<_, i64>(0)
                )
                .unwrap(),
            0
        );
        persist
            .install_finish_guard(&BTreeMap::new())
            .await
            .unwrap();
        writer
            .broker()
            .lane("maintenance-fixture")
            .local_as(
                WriteAuthority::Worker {
                    execution: run.clone(),
                    attempt: ATTEMPT.into(),
                },
                AcceptMaintenanceCheckpoint {
                    execution: run.clone(),
                    attempt: ATTEMPT.into(),
                    token: TOKEN.into(),
                    input: input.clone(),
                    at: 4.0,
                    guard: FinishGuard::Unavailable,
                },
                Duration::from_secs(5),
            )
            .await
            .unwrap();
        let mut missing = Observer::new(ATTEMPT, false);
        let mut binding =
            track_steer(&persist, &run, command, &payload, Some(SESSION), INPUT).unwrap();
        assert!(accepted_receipt(&mut missing, &persist, &run, &mut binding, |_| false).is_none());
        let mut secret = native(&input);
        let mut binding =
            track_steer(&persist, &run, command, &payload, Some(SESSION), INPUT).unwrap();
        assert!(accepted_receipt(&mut secret, &persist, &run, &mut binding, |_| true).is_none());
        let mut changed = input.clone();
        changed.checkpoint.push('!');
        let mut foreign_receipt = native(&changed);
        let mut binding =
            track_steer(&persist, &run, command, &payload, Some(SESSION), INPUT).unwrap();
        assert!(
            accepted_receipt(&mut foreign_receipt, &persist, &run, &mut binding, |_| {
                false
            })
            .is_none()
        );
        assert_eq!(
            store
                .conn
                .query_row(
                    "SELECT receipt_observed FROM maintenance_checkpoints",
                    [],
                    |row| row.get::<_, i64>(0)
                )
                .unwrap(),
            0
        );

        // A constructed internal receipt cannot substitute for the actual
        // matching native callback, even with the exact accepted digest.
        let mut forged_observer = Observer::new(ATTEMPT, false);
        forged_observer.input_sent(INPUT);
        forged_observer.observe(
            &json!({"type":"system","subtype":"init","session_id":SESSION}),
            None,
        );
        let digest =
            agent_run_domain::canonical::sha256_hex(&serde_json::to_value(&input).unwrap(), true);
        forged_observer.expect_checkpoint(ATTEMPT, SESSION, INPUT, CHALLENGE, &digest);
        let mut forged_binding =
            track_steer(&persist, &run, command, &payload, Some(SESSION), INPUT).unwrap();
        forged_binding.examined = true;
        assert!(
            !confirm_receipt(
                &mut persist,
                &mut forged_observer,
                &run,
                &mut forged_binding,
                ReceiptObservation {
                    epoch,
                    attempt: ATTEMPT.into(),
                    challenge: CHALLENGE.into(),
                    digest,
                }
            )
            .await
        );
        assert_eq!(
            store
                .conn
                .query_row(
                    "SELECT receipt_observed FROM maintenance_checkpoints",
                    [],
                    |row| row.get::<_, i64>(0)
                )
                .unwrap(),
            0
        );
        assert_eq!(store.conn.query_row("SELECT COUNT(*) FROM operation_receipts WHERE op_kind='observe_maintenance_checkpoint'",[],|row|row.get::<_,i64>(0)).unwrap(),0);

        let mut observer = native(&input);
        let mut binding =
            track_steer(&persist, &run, command, &payload, Some(SESSION), INPUT).unwrap();
        let mut premature = native(&input);
        let mut premature_binding =
            track_steer(&persist, &run, command, &payload, Some(SESSION), INPUT).unwrap();
        assert!(
            accepted_receipt(
                &mut premature,
                &persist,
                &run,
                &mut premature_binding,
                |_| false
            )
            .is_some()
        );
        premature.observe(&json!({"type":"result","session_id":SESSION,"user_message_uuid":INPUT,"subtype":"success","is_error":false}),Some(true));
        assert_eq!(premature.assessment(), Assessment::Settled);
        assert!(
            native_settlement(&premature, &persist, &run, &premature_binding)
                .unwrap()
                .is_none()
        );
        let proof =
            accepted_receipt(&mut observer, &persist, &run, &mut binding, |_| false).unwrap();
        assert_eq!(observer.assessment(), Assessment::AwaitingTurnCompletion);
        assert!(
            native_settlement(&observer, &persist, &run, &binding)
                .unwrap()
                .is_none()
        );
        // The real supervisor binds the native input only after this command
        // observation commits; an accepted checkpoint alone is insufficient.
        persist
            .observed_command_result(
                &run,
                command,
                &json!({"accepted":true}),
                "stdin_written",
                Some(false),
                vec![],
            )
            .await
            .unwrap();
        assert!(confirm_receipt(&mut persist, &mut observer, &run, &mut binding, proof).await);
        assert!(accepted_receipt(&mut observer, &persist, &run, &mut binding, |_| false).is_none());
        assert_eq!(
            store
                .conn
                .query_row(
                    "SELECT receipt_observed FROM maintenance_checkpoints",
                    [],
                    |row| row.get::<_, i64>(0)
                )
                .unwrap(),
            1
        );
        assert_eq!(store.conn.query_row("SELECT COUNT(*) FROM operation_receipts WHERE op_kind='observe_maintenance_checkpoint' AND outcome='committed'",[],|row|row.get::<_,i64>(0)).unwrap(),1);
        assert_eq!(
            store.get(&run).unwrap().status,
            agent_run_domain::domain::Status::Running
        );
        assert!(store.worker_finish_intent(&run, false).unwrap().is_none());
        // A confirmed receipt is not a completed native turn.
        assert!(
            native_settlement(&observer, &persist, &run, &binding)
                .unwrap()
                .is_none()
        );
        observer.observe(&json!({"type":"result","session_id":SESSION,"user_message_uuid":INPUT,"subtype":"success","is_error":false}),Some(true));
        let settlement = native_settlement(&observer, &persist, &run, &binding)
            .unwrap()
            .unwrap();
        assert_eq!(settlement.version, 1);
        assert_eq!(settlement.coverage, "owned_foreground_v1");
        assert_eq!(settlement.protocol, "claude");
        assert_eq!(settlement.maintenance_epoch, epoch);
        assert_eq!(settlement.agent_id, run);
        assert_eq!(settlement.attempt_id, ATTEMPT);
        assert_eq!(settlement.thread_id, SESSION);
        assert_eq!(settlement.turn_id, INPUT);
        assert_eq!(settlement.item_id, "pause");
        assert_eq!(settlement.challenge_id, CHALLENGE);
        assert_eq!(
            settlement.receipt_sha256,
            agent_run_domain::canonical::sha256_hex(&serde_json::to_value(&input).unwrap(), true)
        );
        let debug = format!("{settlement:?}");
        assert!(
            !debug.contains(CHALLENGE)
                && !debug.contains("fixture checkpoint")
                && !debug.contains(SESSION)
        );

        let mut gapped = native(&input);
        // The original command is completed, so it cannot be rebound or sent again.
        assert!(track_steer(&persist, &run, command, &payload, Some(SESSION), INPUT).is_err());
        gapped.unavailable(Gap::UnsupportedToolCoverage);
        assert!(
            native_settlement(&gapped, &persist, &run, &binding)
                .unwrap()
                .is_none()
        );

        // A user cancel always hides proof, even after an observed receipt and
        // complete native turn. The fixture then disposes this test command.
        writer
            .broker()
            .lane("maintenance-fixture-commands")
            .remote(
                EnqueueCommand {
                    execution: run.clone(),
                    kind: "cancel".into(),
                    payload: json!({}),
                },
                Duration::from_secs(5),
            )
            .await
            .unwrap();
        assert!(
            native_settlement(&observer, &persist, &run, &binding)
                .unwrap()
                .is_none()
        );
        let (cancel, _, _) = persist
            .claim_command_for_turn(&run, true)
            .await
            .unwrap()
            .unwrap();
        persist
            .complete_command(&run, cancel, &json!({"accepted":false,"reason":"fixture"}))
            .await
            .unwrap();
        assert!(
            native_settlement(&observer, &persist, &run, &binding)
                .unwrap()
                .is_some()
        );

        // An unsent rejection whose completion write fails must not stop the
        // task, and cannot replay the native input.
        let mut unsent = Observer::new(ATTEMPT, false);
        reject_unsent(&mut persist, &mut unsent, &run, command + 1000).await;
        assert_eq!(
            unsent.assessment(),
            Assessment::Unavailable(Gap::InputCorrelation)
        );
        assert_eq!(
            store.get(&run).unwrap().status,
            agent_run_domain::domain::Status::Running
        );
        assert_eq!(
            store
                .conn
                .query_row("SELECT state FROM commands WHERE id=?", [command], |row| {
                    row.get::<_, String>(0)
                })
                .unwrap(),
            "completed"
        );

        // A sent input whose evidence is durably rejected stays unavailable;
        // existing recovery writes only unverified evidence and never resends.
        writer
            .broker()
            .lane("maintenance-fixture-commands")
            .remote(
                EnqueueCommand {
                    execution: run.clone(),
                    kind: "steer".into(),
                    payload: json!({"text":"fixture"}),
                },
                Duration::from_secs(5),
            )
            .await
            .unwrap();
        let (extra, _, _) = persist
            .claim_command_for_turn(&run, true)
            .await
            .unwrap()
            .unwrap();
        assert!(
            native_settlement(&observer, &persist, &run, &binding)
                .unwrap()
                .is_none()
        );
        let mut failed = Observer::new(ATTEMPT, false);
        let fragment = agent_run_store::ops::JournalFragment::Event {
            kind: "fixture_invalid_attempt".into(),
            data: json!({}),
            attempt: Some("foreign-attempt".into()),
        };
        assert!(!record_sent(&mut persist, &mut failed, &run, extra, vec![fragment]).await);
        assert_eq!(
            failed.assessment(),
            Assessment::Unavailable(Gap::InputCorrelation)
        );
        assert_eq!(
            store.get(&run).unwrap().status,
            agent_run_domain::domain::Status::Running
        );
        assert_eq!(
            store
                .conn
                .query_row("SELECT state FROM commands WHERE id=?", [extra], |row| {
                    row.get::<_, String>(0)
                })
                .unwrap(),
            "claimed"
        );
        assert_eq!(store.conn.query_row("SELECT COUNT(*) FROM operation_receipts WHERE op_kind='observed_command_result' AND outcome='rejected'",[],|row|row.get::<_,i64>(0)).unwrap(),1);
        assert_eq!(
            store
                .conn
                .query_row(
                    "SELECT COUNT(*) FROM events WHERE kind='recovery_observation_v1'",
                    [],
                    |row| row.get::<_, i64>(0)
                )
                .unwrap(),
            1
        );
        // Once every earlier input is settled, the exact proof produces one
        // event and one closing marker without finishing or parking the task.
        persist
            .complete_command(&run, extra, &json!({"accepted":false}))
            .await
            .unwrap();
        let exact = native_settlement(&observer, &persist, &run, &binding)
            .unwrap()
            .unwrap();
        let mut foreign = exact.clone();
        foreign.attempt_id = "foreign-attempt".into();
        assert!(
            commit_settlement(&mut persist, &run, &foreign)
                .await
                .is_err()
        );
        assert_eq!(
            store
                .conn
                .query_row(
                    "SELECT COUNT(*) FROM events WHERE kind='maintenance_native_settled_v1'",
                    [],
                    |row| row.get::<_, i64>(0)
                )
                .unwrap(),
            0
        );
        let event_seq = commit_settlement(&mut persist, &run, &exact).await.unwrap();
        let marker:(i64,String)=store.conn.query_row("SELECT closing_event_seq,closing_event_sha256 FROM maintenance_checkpoints WHERE epoch=? AND agent_id=? AND attempt_id=?",params![epoch,run.as_str(),ATTEMPT],|row|Ok((row.get(0)?,row.get(1)?))).unwrap();
        assert_eq!(marker.0, event_seq);
        assert_eq!(marker.1.len(), 64);
        assert_eq!(
            store
                .conn
                .query_row(
                    "SELECT COUNT(*) FROM events WHERE kind='maintenance_native_settled_v1'",
                    [],
                    |row| row.get::<_, i64>(0)
                )
                .unwrap(),
            1
        );
        assert!(store.worker_finish_intent(&run, false).unwrap().is_none());
        assert_eq!(
            store.get(&run).unwrap().status,
            agent_run_domain::domain::Status::Running
        );
    }

    /// Unknown commit outcomes take the same conservative branch as a refusal;
    /// they are never turned into a definite unsent result or a native retry.
    #[test]
    fn claude_maintenance_unknown_evidence_never_arms_pause() {
        use agent_run_domain::storage::{UnknownCause, WriterFailure};
        let mut observer = Observer::new(ATTEMPT, false);
        let unknown: Result<()> = Err(agent_run_domain::Error::WriterFailure(
            WriterFailure::Unknown(UnknownCause::CommitUncertain),
        ));
        assert!(!evidence_recorded(&mut observer, unknown));
        assert_eq!(
            observer.assessment(),
            Assessment::Unavailable(Gap::InputCorrelation)
        );
    }

    /// An unavailable finish read refuses only the maintenance control path;
    /// ordinary steering still receives the original error.
    #[test]
    fn claude_maintenance_finish_read_failure_preserves_ordinary_errors() {
        assert!(matches!(
            finish_status(Err(invalid("fixture")), true),
            Ok(None)
        ));
        assert!(finish_status(Err(invalid("fixture")), false).is_err());
        assert!(matches!(finish_status(Ok(None), true), Ok(Some(false))));
        assert!(matches!(
            finish_status(
                Ok(Some(agent_run_domain::worker::FinishRequest {
                    summary: "finished".into(),
                    status: agent_run_domain::worker::FinishStatus::Done,
                })),
                true
            ),
            Ok(Some(true))
        ));
    }
}
