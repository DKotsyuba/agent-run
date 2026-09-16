//! Typed state and notification parsing for one Codex app-server turn.
use agent_run_domain::{error::invalid, Result};
use serde_json::Value;

/// The locally-observed lifecycle of one app-server protocol session.
///
/// A session advances only after its corresponding JSON-RPC acknowledgement
/// has supplied the required nonempty identifier. `Closing` and `Closed` are
/// terminal; callers must not reuse a process after either state.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SessionState {
    /// No handshake has completed.
    New,
    /// `initialize` and `initialized` have completed.
    Initialized,
    /// A thread has been started or resumed.
    ThreadActive,
    /// A turn is receiving notifications.
    TurnActive,
    /// Input has been closed while the child is being reaped.
    Closing,
    /// The owned child and protocol stream are no longer usable.
    Closed,
}

/// Identity-bearing state retained for one app-server protocol session.
///
/// The state machine is deliberately transport independent: callers perform
/// JSON-RPC I/O, then provide acknowledgements here before accepting streamed
/// notifications. This prevents stale thread or turn events from being
/// journaled for a different durable run.
#[derive(Debug, Clone)]
pub struct Session {
    state: SessionState,
    thread_id: Option<String>,
    turn_id: Option<String>,
    require_turn_id: bool,
}

/// A validated app-server notification suitable for product normalization.
///
/// `params` is always a JSON object. `Other` retains unrecognized valid
/// notifications so callers can preserve their ordering without treating them
/// as a terminal result.
#[derive(Debug, Clone, PartialEq)]
pub enum Notification {
    /// Incremental assistant text for one item.
    AssistantDelta { item_id: String, delta: String },
    /// A completed item object, including commands and tool results.
    ItemCompleted { item: Value },
    /// A token-usage update object.
    TokenUsage { token_usage: Value },
    /// The terminal turn object.
    TurnCompleted { turn: Value },
    /// A valid non-product notification retained for ordering and diagnostics.
    Other { method: String, params: Value },
}

impl Session {
    /// Creates a fresh session. Resumed turns require explicit matching turn IDs.
    pub const fn new(require_turn_id: bool) -> Self {
        Self {
            state: SessionState::New,
            thread_id: None,
            turn_id: None,
            require_turn_id,
        }
    }

    /// Returns the currently permitted protocol lifecycle state.
    pub const fn state(&self) -> SessionState {
        self.state
    }

    /// Records a completed handshake.
    pub fn initialized(&mut self) -> Result<()> {
        if self.state != SessionState::New {
            return Err(invalid(
                "Codex initialize is not valid in this session state",
            ));
        }
        self.state = SessionState::Initialized;
        Ok(())
    }

    /// Records a started or resumed thread after verifying its nonempty ID.
    pub fn thread_started(&mut self, thread_id: &str) -> Result<()> {
        if self.state != SessionState::Initialized || thread_id.is_empty() {
            return Err(invalid(
                "Codex thread start is not valid in this session state",
            ));
        }
        self.thread_id = Some(thread_id.into());
        self.state = SessionState::ThreadActive;
        Ok(())
    }

    /// Records a turn-start acknowledgement after verifying its nonempty ID.
    pub fn turn_started(&mut self, turn_id: &str) -> Result<()> {
        if self.state != SessionState::ThreadActive || turn_id.is_empty() {
            return Err(invalid(
                "Codex turn start is not valid in this session state",
            ));
        }
        self.turn_id = Some(turn_id.into());
        self.state = SessionState::TurnActive;
        Ok(())
    }

    /// Returns whether an envelope belongs to the active thread and turn.
    ///
    /// An absent turn ID is tolerated for fresh threads for compatibility with
    /// older app-server item notifications, but never for resumed threads.
    pub fn owns(&self, params: &Value) -> bool {
        let Some(params) = params.as_object() else {
            return false;
        };
        if params
            .get("threadId")
            .and_then(Value::as_str)
            .is_some_and(|value| Some(value) != self.thread_id.as_deref())
        {
            return false;
        }
        let turn = params.get("turnId").and_then(Value::as_str).or_else(|| {
            params
                .get("turn")
                .and_then(Value::as_object)
                .and_then(|turn| turn.get("id"))
                .and_then(Value::as_str)
        });
        if self.require_turn_id {
            return turn.is_some_and(|value| Some(value) == self.turn_id.as_deref());
        }
        !turn.is_some_and(|value| Some(value) != self.turn_id.as_deref())
    }

    /// Parses one matching notification without mutating the lifecycle.
    ///
    /// Unknown methods are returned as `Other`; malformed product events fail
    /// closed rather than silently producing a fabricated journal entry.
    pub fn notification(&self, envelope: &Value) -> Result<Option<Notification>> {
        if self.state != SessionState::TurnActive {
            return Err(invalid("Codex notification arrived outside an active turn"));
        }
        let Some(method) = envelope.get("method").and_then(Value::as_str) else {
            return Err(invalid("malformed Codex notification method"));
        };
        let params = envelope
            .get("params")
            .filter(|value| value.is_object())
            .ok_or_else(|| invalid("malformed Codex notification params"))?;
        if !self.owns(params) {
            return Ok(None);
        }
        match method {
            "item/agentMessage/delta" => {
                let item_id = params
                    .get("itemId")
                    .and_then(Value::as_str)
                    .filter(|value| !value.is_empty())
                    .ok_or_else(|| invalid("assistant delta has no itemId"))?;
                let delta = params
                    .get("delta")
                    .and_then(Value::as_str)
                    .ok_or_else(|| invalid("malformed assistant delta"))?;
                Ok(Some(Notification::AssistantDelta {
                    item_id: item_id.into(),
                    delta: delta.into(),
                }))
            }
            "item/completed" => Ok(Some(Notification::ItemCompleted {
                item: params
                    .get("item")
                    .filter(|value| value.is_object())
                    .cloned()
                    .ok_or_else(|| invalid("malformed completed Codex item"))?,
            })),
            "thread/tokenUsage/updated" => Ok(Some(Notification::TokenUsage {
                token_usage: params.get("tokenUsage").cloned().unwrap_or(Value::Null),
            })),
            "turn/completed" => {
                let turn = params
                    .get("turn")
                    .filter(|value| value.is_object())
                    .cloned()
                    .ok_or_else(|| invalid("malformed completed Codex turn"))?;
                if !matches!(
                    turn.get("status").and_then(Value::as_str),
                    Some("completed") | Some("interrupted") | Some("failed")
                ) {
                    return Err(invalid("nonterminal or unknown turn status"));
                }
                Ok(Some(Notification::TurnCompleted { turn }))
            }
            _ => Ok(Some(Notification::Other {
                method: method.into(),
                params: params.clone(),
            })),
        }
    }

    /// Moves the session to closing; repeated closure is harmless.
    pub fn close(&mut self) {
        if self.state != SessionState::Closed {
            self.state = SessionState::Closing;
        }
    }

    /// Marks the protocol and its owned child as unusable.
    pub fn closed(&mut self) {
        self.state = SessionState::Closed;
    }
}

/// Maps a structured Codex error to the Python-compatible durable failure kind.
///
/// Explicit `kind` then `code` values are preserved. Provider-only
/// `codexErrorInfo` is sanitized to a bounded ASCII diagnostic category.
pub fn failure_kind(error: &Value) -> Option<String> {
    for key in ["kind", "code"] {
        if let Some(value) = error.get(key).and_then(Value::as_str) {
            return Some(value.into());
        }
    }
    let value = error.get("codexErrorInfo").and_then(Value::as_str)?.trim();
    if value.is_empty() {
        return None;
    }
    if value == "serverOverloaded" {
        return Some("provider_overloaded".into());
    }
    let safe: String = value
        .chars()
        .map(|character| {
            if character.is_ascii_alphanumeric() || matches!(character, '.' | '_' | '-') {
                character
            } else {
                '_'
            }
        })
        .take(58)
        .collect::<String>()
        .trim_matches(|character| matches!(character, '.' | '_' | '-'))
        .into();
    Some(format!(
        "codex_{}",
        if safe.is_empty() { "error" } else { &safe }
    ))
}
