//! The frozen agent lifecycle state machine.

use crate::{Error, Result};

pub use crate::domain::Status;

/// The nonterminal lifecycle states in stable wire-value order.
pub const ACTIVE: [Status; 4] = [
    Status::Created,
    Status::Starting,
    Status::Running,
    Status::Cancelling,
];

/// The terminal lifecycle states in stable wire-value order.
pub const TERMINAL: [Status; 5] = [
    Status::Succeeded,
    Status::Failed,
    Status::TimedOut,
    Status::Cancelled,
    Status::Lost,
];

/// Validates one state transition against the Python-compatible frozen transition table.
pub fn validate_transition(current: Status, target: Status) -> Result<()> {
    current.transition(target)
}

/// Builds the typed illegal-transition error used by lifecycle persistence boundaries.
pub fn illegal_transition(current: Status, target: Status) -> Error {
    Error::Transition(format!("{} -> {}", current.as_str(), target.as_str()))
}
