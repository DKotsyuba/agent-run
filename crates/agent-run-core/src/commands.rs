//! Durable supervisor command result helpers.
//!
//! Command claims are persisted by the store before an engine is touched.  This
//! module centralizes the terminal answers for commands that arrive after the
//! engine has stopped, so every pending command receives one stable result
//! without attempting an engine operation twice.

use crate::{domain::AgentId, state::Store, Result};
use serde_json::{json, Value};

/// Completes all commands that became pending after an agent reached a terminal state.
///
/// The caller invokes this only after its terminal transition has committed.
/// Commands are claimed one at a time in cancellation-first order and are
/// recorded as terminal rather than delivered to an absent engine.  A command
/// already claimed by the crashed owning supervisor is deliberately untouched:
/// replaying it could duplicate an engine-side steer or interrupt.
pub fn complete_terminal(store: &mut Store, agent_id: &AgentId) -> Result<()> {
    while let Some((command_id, kind, _)) = store.claim_command(agent_id)? {
        let result = if kind == "cancel" {
            json!({"accepted": true, "reason": "already_stopping"})
        } else {
            json!({"accepted": false, "reason": "agent_terminal"})
        };
        store.complete_command(agent_id, command_id, &result)?;
    }
    Ok(())
}

/// Extracts a nonblank steer text payload without treating malformed input as an engine error.
///
/// A malformed or blank payload is a rejected command result.  It is not
/// propagated from a runner, because that would strand an already claimed row.
pub fn steer_text(payload: &Value) -> Option<&str> {
    payload
        .get("text")
        .and_then(Value::as_str)
        .filter(|text| !text.trim().is_empty())
}
