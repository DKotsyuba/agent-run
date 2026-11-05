//! Conservative maintenance-only observation of the owned Codex event stream.
//!
//! This is not a restart or cleanup verdict. It neither sends input nor changes
//! normal finish behavior. Unsupported background semantics permanently make
//! this attempt unavailable for automatic maintenance, without failing its work.

use agent_run_domain::worker::{MaintenancePauseReceipt, MaintenancePauseRequest, SERVER_NAME};
use serde_json::Value;
use std::{
    collections::{BTreeMap, BTreeSet},
    fmt,
};

const MAX_ITEMS: usize = 4096;
const MAX_ID_BYTES: usize = 1024;

/// A content-free reason why this observer cannot prove a safe native boundary.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Gap {
    MissingIdentity,
    ForeignIdentity,
    DuplicateOrUnstartedItem,
    InvalidCheckpoint,
    ReceiptMismatch,
    NativeFailure,
    UnsupportedToolCoverage,
    AsyncCoverageUnavailable,
    InheritedWorkUnknown,
    ActivityAfterCheckpoint,
    ObservationLimit,
    ReceiptPersistenceUnavailable,
}

/// Local native evidence only; even Settled requires separate input fencing,
/// ownership cleanup and exact native-history sealing before an agent can park.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Assessment {
    /// No trusted store acceptance has been bound to this observer.
    Unarmed,
    /// The exact native callback result has not been observed yet.
    AwaitingReceipt,
    /// The callback was observed but its native turn has not completed.
    AwaitingTurnCompletion,
    /// A known native command/call remains unresolved.
    PendingNativeWork,
    /// Missing or unsupported evidence is sticky for this attempt.
    Unavailable(Gap),
    /// The supported observed native calls and exact checkpoint turn settled.
    Settled,
}

#[derive(Clone, PartialEq, Eq)]
struct Checkpoint {
    challenge: String,
    digest: String,
}

/// Exact native callback correlation. It is a candidate until the caller
/// matches durable command/attempt/checkpoint state and records the receipt.
pub struct NativeCheckpoint<'a> {
    pub thread_id: &'a str,
    pub turn_id: &'a str,
    pub item_id: &'a str,
    pub challenge_id: &'a str,
    pub receipt_sha256: &'a str,
}

impl fmt::Debug for NativeCheckpoint<'_> {
    /// Native identities and checkpoint digests stay out of diagnostic output.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("NativeCheckpoint(<private correlation>)")
    }
}

#[derive(Clone, PartialEq, Eq)]
enum Call {
    Command { process_id: Option<String> },
    Pause(Checkpoint),
}

/// Observe every native envelope from the start of one owned execution. Only
/// exact private checkpoint calls and commands with native exit evidence are
/// supported. Other tools may hide asynchronous work and therefore fail closed.
pub struct Observer {
    thread: String,
    active: BTreeMap<(String, String), Call>,
    completed: BTreeSet<(String, String)>,
    expected: Option<Checkpoint>,
    receipt: Option<(Checkpoint, String, String)>,
    receipt_recorded: bool,
    completed_turn: Option<String>,
    checkpoint_turn_completed: bool,
    gap: Option<Gap>,
}

impl fmt::Debug for Observer {
    /// Native identities and checkpoint material never enter diagnostics.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MaintenanceObserver")
            .field("active_calls", &self.active.len())
            .field("assessment", &self.assessment())
            .finish_non_exhaustive()
    }
}

impl Observer {
    /// Start an attempt-local observer. A resumed session has no complete
    /// inherited background-work census in the current protocol and is held.
    pub fn new(thread: &str, resumed_session: bool) -> Self {
        Self {
            thread: thread.into(),
            active: BTreeMap::new(),
            completed: BTreeSet::new(),
            expected: None,
            receipt: None,
            receipt_recorded: false,
            completed_turn: None,
            checkpoint_turn_completed: false,
            gap: if !valid_id(thread) {
                Some(Gap::MissingIdentity)
            } else if resumed_session {
                Some(Gap::InheritedWorkUnknown)
            } else {
                None
            },
        }
    }

    /// Bind the exact durable callback accepted by the store, never a digest
    /// copied only from a native result. This may happen after item/started.
    /// An attempt cannot silently replace an earlier checkpoint binding.
    pub fn expect_checkpoint(&mut self, challenge_id: &str, accepted_sha256: &str) {
        let receipt = MaintenancePauseReceipt {
            sha256: accepted_sha256.into(),
            duplicate: false,
        };
        let input = MaintenancePauseRequest {
            challenge_id: challenge_id.into(),
            checkpoint: "validation".into(),
        };
        if input.validate().is_err() || receipt.validate().is_err() {
            self.unavailable(Gap::InvalidCheckpoint);
            return;
        }
        let expected = Checkpoint {
            challenge: challenge_id.into(),
            digest: accepted_sha256.into(),
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
                .values()
                .any(|call| matches!(call,Call::Pause(prior) if prior != &expected))
        {
            self.unavailable(Gap::ReceiptMismatch);
            return;
        }
        self.expected = Some(expected);
    }

    /// Mark an observed coverage failure. Later callbacks, silence or successful
    /// turns cannot repair an unknown background action by inference.
    pub fn unavailable(&mut self, gap: Gap) {
        self.gap.get_or_insert(gap);
    }

    /// Consume an actual envelope BEFORE the ordinary adapter's tolerant
    /// filtering. Relevant events require explicit exact thread/turn/item ids.
    /// `current_turn` is the adapter's acknowledged active native turn.
    pub fn observe(&mut self, envelope: &Value, current_turn: &str) {
        if self.gap.is_some() {
            return;
        }
        let Some(method) = envelope["method"].as_str() else {
            if envelope.get("id").is_none() {
                self.unavailable(Gap::MissingIdentity);
            }
            return;
        };
        if !matches!(
            method,
            "item/started" | "item/completed" | "turn/completed" | "turn/started"
        ) {
            return;
        }
        if envelope.get("id").is_some() {
            self.unavailable(Gap::MissingIdentity);
            return;
        }
        let params = &envelope["params"];
        let Some(thread) = params["threadId"].as_str().filter(|id| valid_id(id)) else {
            self.unavailable(Gap::MissingIdentity);
            return;
        };
        let turn = params["turnId"]
            .as_str()
            .or_else(|| params.pointer("/turn/id").and_then(Value::as_str));
        let Some(turn) = turn.filter(|id| valid_id(id)) else {
            self.unavailable(Gap::MissingIdentity);
            return;
        };
        if thread != self.thread || !valid_id(current_turn) {
            self.unavailable(Gap::ForeignIdentity);
            return;
        }
        if params.get("turnId").is_some()
            && params.pointer("/turn/id").is_some()
            && params["turnId"] != params["turn"]["id"]
        {
            self.unavailable(Gap::ForeignIdentity);
            return;
        }
        if method == "turn/started" {
            if self.checkpoint_turn_completed {
                self.unavailable(Gap::ActivityAfterCheckpoint);
            } else if turn != current_turn {
                self.unavailable(Gap::ForeignIdentity);
            }
            return;
        }
        if method == "turn/completed" {
            if turn != current_turn {
                self.unavailable(Gap::ForeignIdentity);
                return;
            }
            if self.completed_turn.as_deref() == Some(turn) {
                self.unavailable(Gap::DuplicateOrUnstartedItem);
                return;
            }
            if params["turn"]["status"] != "completed" {
                self.unavailable(Gap::NativeFailure);
                return;
            }
            self.completed_turn = Some(turn.into());
            self.checkpoint_turn_completed = self
                .receipt
                .as_ref()
                .is_some_and(|(_, origin, _)| origin == turn);
            self.completed.retain(|(origin, _)| origin != turn);
            return;
        }
        let item = &params["item"];
        let Some(id) = item["id"].as_str().filter(|id| valid_id(id)) else {
            self.unavailable(Gap::MissingIdentity);
            return;
        };
        let Some(kind) = item["type"].as_str() else {
            self.unavailable(Gap::MissingIdentity);
            return;
        };
        if matches!(kind, "agentMessage" | "reasoning" | "plan" | "userMessage") {
            if self.checkpoint_turn_completed {
                self.unavailable(Gap::ActivityAfterCheckpoint);
            } else if turn != current_turn {
                self.unavailable(Gap::ForeignIdentity);
            }
            return;
        }
        let key = (turn.to_owned(), id.to_owned());
        if method == "item/started" {
            if turn != current_turn {
                self.unavailable(Gap::ForeignIdentity);
                return;
            }
            if self.checkpoint_turn_completed {
                self.unavailable(Gap::ActivityAfterCheckpoint);
                return;
            }
            if self.active.contains_key(&key) || self.completed.contains(&key) {
                self.unavailable(Gap::DuplicateOrUnstartedItem);
                return;
            }
            if self.active.len() + self.completed.len() >= MAX_ITEMS {
                self.unavailable(Gap::ObservationLimit);
                return;
            }
            let call = if kind == "commandExecution" {
                let process_id = item
                    .get("processId")
                    .filter(|value| !value.is_null())
                    .map(|value| value.as_str().filter(|id| valid_id(id)).map(str::to_owned));
                if process_id.as_ref().is_some_and(Option::is_none) {
                    self.unavailable(Gap::AsyncCoverageUnavailable);
                    return;
                }
                Call::Command {
                    process_id: process_id.flatten(),
                }
            } else if private_pause(item) {
                let arguments = match &item["arguments"] {
                    Value::String(text) => serde_json::from_str(text).ok(),
                    value => Some(value.clone()),
                };
                let Some(input) = arguments
                    .and_then(|value| serde_json::from_value::<MaintenancePauseRequest>(value).ok())
                    .filter(|input| input.validate().is_ok())
                else {
                    self.unavailable(Gap::InvalidCheckpoint);
                    return;
                };
                let input = Checkpoint {
                    challenge: input.challenge_id.clone(),
                    digest: agent_run_domain::canonical::sha256_hex(
                        &serde_json::to_value(input).expect("validated request serializes"),
                        true,
                    ),
                };
                if self
                    .expected
                    .as_ref()
                    .is_some_and(|expected| expected != &input)
                {
                    self.unavailable(Gap::ReceiptMismatch);
                    return;
                }
                if self.receipt.is_some()
                    || self
                        .active
                        .values()
                        .any(|call| matches!(call, Call::Pause(_)))
                {
                    self.unavailable(Gap::DuplicateOrUnstartedItem);
                    return;
                }
                Call::Pause(input)
            } else {
                self.unavailable(Gap::UnsupportedToolCoverage);
                return;
            };
            self.active.insert(key, call);
            return;
        }
        let Some(call) = self.active.remove(&key) else {
            self.unavailable(Gap::DuplicateOrUnstartedItem);
            return;
        };
        match call {
            Call::Command { process_id } => {
                if kind != "commandExecution"
                    || process_id
                        .as_deref()
                        .is_some_and(|id| item["processId"].as_str() != Some(id))
                {
                    self.unavailable(Gap::ForeignIdentity);
                    return;
                }
                if item["status"] != "completed" || item["exitCode"].as_i64().is_none() {
                    self.unavailable(Gap::AsyncCoverageUnavailable);
                    return;
                }
            }
            Call::Pause(expected) => {
                if !private_pause(item) {
                    self.unavailable(Gap::ForeignIdentity);
                    return;
                }
                let Some(receipt) = parse_receipt(item) else {
                    self.unavailable(Gap::NativeFailure);
                    return;
                };
                if receipt.sha256 != expected.digest
                    || self
                        .expected
                        .as_ref()
                        .is_some_and(|accepted| accepted != &expected)
                {
                    self.unavailable(Gap::ReceiptMismatch);
                    return;
                }
                if self.completed_turn.as_deref() == Some(turn) {
                    self.unavailable(Gap::ActivityAfterCheckpoint);
                    return;
                }
                self.receipt = Some((expected, turn.into(), id.into()));
            }
        }
        self.completed.insert(key);
    }

    /// Return the current conservative evidence state. This never authorizes
    /// interruption, repeated native input, process cleanup or automatic resume.
    pub fn assessment(&self) -> Assessment {
        if let Some(gap) = self.gap {
            return Assessment::Unavailable(gap);
        }
        let Some(expected) = self.expected.as_ref() else {
            return Assessment::Unarmed;
        };
        let Some((receipt, _, _)) = self.receipt.as_ref() else {
            return Assessment::AwaitingReceipt;
        };
        if receipt != expected {
            return Assessment::Unavailable(Gap::ReceiptMismatch);
        }
        if !self.checkpoint_turn_completed {
            return Assessment::AwaitingTurnCompletion;
        }
        if !self.active.is_empty() {
            return Assessment::PendingNativeWork;
        }
        Assessment::Settled
    }

    /// Read the exact observed native callback, never a substring or a guessed
    /// item identity. A coverage/correlation gap makes even this candidate unavailable.
    pub fn receipt_candidate(&self) -> Option<NativeCheckpoint<'_>> {
        if self.gap.is_some() {
            return None;
        }
        let (receipt, turn, item) = self.receipt.as_ref()?;
        Some(NativeCheckpoint {
            thread_id: &self.thread,
            turn_id: turn,
            item_id: item,
            challenge_id: &receipt.challenge,
            receipt_sha256: &receipt.digest,
        })
    }

    /// Whether this observer already received a successful durable write result.
    pub(super) fn receipt_recorded(&self) -> bool {
        self.receipt_recorded
    }

    /// Called only after the pinned supervisor receipt operation is committed.
    pub(super) fn record_receipt(&mut self) {
        self.receipt_recorded = true;
    }

    /// Exact correlation for a future settlement event, available only after
    /// native completion AND confirmed receipt persistence. No caller currently
    /// turns this observation into a park, process signal or automatic resume.
    pub fn settled_checkpoint(&self) -> Option<NativeCheckpoint<'_>> {
        (self.receipt_recorded && self.assessment() == Assessment::Settled)
            .then(|| self.receipt_candidate())
            .flatten()
    }
}

/// Only a plain, bounded native identifier can participate in exact matching.
fn valid_id(id: &str) -> bool {
    !id.is_empty() && id.len() <= MAX_ID_BYTES && !id.chars().any(char::is_control)
}

/// Match the exact private worker tool rather than suffixes or display names.
fn private_pause(item: &Value) -> bool {
    item["type"] == "mcpToolCall"
        && item["server"] == SERVER_NAME
        && item["tool"] == "maintenance_pause"
}

/// Parse one exact structured receipt from the real successful native result.
/// A digest in prose, an error result, extra blocks or malformed flags refuse.
fn parse_receipt(item: &Value) -> Option<MaintenancePauseReceipt> {
    if item["status"] != "completed"
        || item.get("error").is_some_and(|e| !e.is_null())
        || item["result"]
            .get("isError")
            .is_some_and(|flag| flag != false)
    {
        return None;
    }
    let content = item["result"]["content"].as_array()?;
    if content.len() != 1 || content[0]["type"] != "text" {
        return None;
    }
    let receipt: MaintenancePauseReceipt =
        serde_json::from_str(content[0]["text"].as_str()?).ok()?;
    receipt.validate().ok()?;
    Some(receipt)
}
