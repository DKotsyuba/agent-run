//! Durable supervisor command result helpers.
//!
//! Command claims are persisted by the store before an engine is touched.  This
//! module centralizes the terminal answers for commands that arrive after the
//! engine has stopped, so every pending command receives one stable result
//! without attempting an engine operation twice.

use crate::{domain::AgentId, state::Store, Result};
use serde_json::{json, Value};

/// Maximum number of live-engine commands handled before polling resumes.
pub const COMMAND_PAGE_LIMIT: usize = 16;
/// Maximum elapsed monotonic time spent draining one live-engine command page.
pub const COMMAND_PAGE_SECONDS: f64 = 1.0;

/// Completes all commands that became pending after an agent reached a terminal state.
///
/// The caller invokes this only after its terminal transition has committed;
/// lost reconciliation instead uses [`complete_pending_in`] inside its own
/// transaction. Commands are claimed one at a time in cancellation-first order and are
/// recorded as terminal rather than delivered to an absent engine.  A command
/// already claimed by the crashed owning supervisor is deliberately untouched:
/// replaying it could duplicate an engine-side steer or interrupt.
pub fn complete_terminal(store: &mut Store, agent_id: &AgentId) -> Result<()> {
    while let Some((command_id, kind, _)) = store.claim_command(agent_id)? {
        store.complete_command(agent_id, command_id, &terminal_result(&kind))?;
    }
    Ok(())
}

/// Returns the stable result recorded for a command that arrives after termination.
///
/// `kind` is the durable command kind. A `cancel` is accepted because the agent
/// is already stopped; every other kind is rejected with `agent_terminal`.
fn terminal_result(kind: &str) -> Value {
    if kind == "cancel" {
        json!({"accepted": true, "reason": "already_stopping"})
    } else {
        json!({"accepted": false, "reason": "agent_terminal"})
    }
}

/// Completes every still-pending command inside the caller's open transaction.
///
/// `tx` must be the transaction that writes the agent's terminal transition, so
/// the terminal status and the command results commit or roll back together.
/// `completed_at` is the Unix timestamp recorded as both claim and completion time on each completed row. Only
/// `pending` rows are touched; a command already `claimed` by the crashed
/// supervisor is left as-is because replaying it could duplicate an engine-side
/// steer or interrupt. Returns a database error, which the caller must
/// propagate so the transaction rolls back.
pub(crate) fn complete_pending_in(
    tx: &rusqlite::Transaction<'_>,
    agent_id: &AgentId,
    completed_at: f64,
) -> Result<()> {
    let pending = tx
        .prepare("SELECT id,kind FROM commands WHERE agent_id=? AND state='pending'")?
        .query_map([agent_id.as_str()], |row| {
            Ok((row.get::<_, i64>(0)?, row.get::<_, String>(1)?))
        })?
        .collect::<std::result::Result<Vec<_>, _>>()?;
    for (command_id, kind) in pending {
        tx.execute(
            "UPDATE commands SET state='completed',claimed_at=?,completed_at=?,result_json=? WHERE id=? AND state='pending'",
            rusqlite::params![
                completed_at,
                completed_at,
                terminal_result(&kind).to_string(),
                command_id
            ],
        )?;
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
