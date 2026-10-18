//! Bounded per-admission health policy and authenticated model probe replies.

use crate::{Result, error::invalid};
use serde::{Deserialize, Serialize};

/// Frozen health policy. Observation never invokes a model; recovery permits
/// one paid control turn per observed task-progress episode, never cancellation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PingPolicy {
    /// Preserve historical execution and explicit operator pauses.
    #[default]
    Off,
    /// Check the owned control channel and report unexplained idle without inference.
    Observe,
    /// Also permit a correlated model control turn in the existing native session.
    Recover,
}

impl PingPolicy {
    /// Whether serialization omits the free new-attempt default, preserving historical request fingerprints.
    pub fn is_observe(&self) -> bool {
        *self == Self::Observe
    }
}

/// Free default when no health policy was supplied. Existing attempts without
/// a health initialization marker remain inert; explicit off is always retained.
pub fn observe() -> PingPolicy {
    PingPolicy::Observe
}

/// The worker's claim, distinct from verified producer or process evidence.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ReplyState {
    /// Continue the original task; this reply alone is not useful task progress.
    Working,
    /// Awaiting a producer; the broker independently checks owned native work.
    Waiting,
    /// Unable to continue; an operator notice is required, not automatic success.
    Blocked,
}

/// Reply to one exact attempt-owned challenge. Contains no arbitrary prose or
/// credentials, cannot finish/cancel a run and grants no new capabilities.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PingReply {
    /// Lowercase SHA-256 challenge issued by the broker, not a caller-selected agent id.
    pub ping_id: String,
    /// Current worker claim; productive activity must be observed separately.
    pub state: ReplyState,
}

impl PingReply {
    /// Reject malformed, unbounded or forged-shaped challenge ids before effects.
    pub fn validate(&self) -> Result<()> {
        if self.ping_id.len() != 64
            || !self
                .ping_id
                .bytes()
                .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
        {
            return Err(invalid("ping_id must be a broker-issued lowercase SHA-256"));
        }
        Ok(())
    }
}
