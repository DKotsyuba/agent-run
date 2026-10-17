//! Read-only native communication observation and private supervisor evidence.

use crate::{HarnessId, Result, error::invalid};
use serde::{Deserialize, Serialize};
use std::path::PathBuf;

/// Private attempt facts recorded by its supervisor, never a public view or grant.
/// The native session is populated only by the native adapter's session event.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NativeIntegration {
    /// Actual selected native executable family.
    pub harness: HarnessId,
    /// Canonical private storage used in the child's launch environment.
    pub store: PathBuf,
    /// Session observed by this attempt's adapter, never the parent notification binding.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub session: Option<String>,
}

impl NativeIntegration {
    /// Rejects malformed or oversized evidence without filesystem access.
    /// Canonicalization and process proof belong to the supervisor and observer.
    pub fn validate(&self) -> Result<()> {
        let Some(store) = self.store.to_str() else {
            return Err(invalid("invalid native integration store"));
        };
        if !self.store.is_absolute()
            || store.len() > 4096
            || store.chars().any(char::is_control)
            || self.session.as_ref().is_some_and(|session| {
                session.is_empty() || session.len() > 256 || session.chars().any(char::is_control)
            })
        {
            return Err(invalid("invalid native integration evidence"));
        }
        Ok(())
    }
}

/// Safe communication projection. An address proves only a sampled native identity.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "state", rename_all = "snake_case", deny_unknown_fields)]
pub enum Communication {
    /// The exact owned attempt passed local checks before and after Bridge lookup.
    Observed {
        /// Canonical Bridge address, with no private process or store tuple.
        address: String,
        /// Unix milliseconds at the Bridge observation, bounded to positive i64 range.
        sampled_at_ms: u64,
    },
    /// Current facts cannot establish the exact attempt's recipient.
    Unknown {
        /// Closed safe reason; no upstream diagnostic or private expected value.
        reason: UnknownReason,
    },
    /// This execution or deployment has no permitted observation path.
    Unsupported {
        /// Closed safe reason; absence never authorizes provisioning or inference.
        reason: UnsupportedReason,
    },
}

/// Reasons an eligible execution cannot currently yield an address.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum UnknownReason {
    /// Native process or session has not yet been recorded.
    NotStarted,
    /// OS, Bridge or historical facts are unavailable or uncertain.
    Unavailable,
    /// The owned attempt or exact native process tuple changed.
    IdentityChanged,
    /// The pinned execution is terminal.
    Terminal,
    /// Explicit finish was accepted and cleanup is pending.
    Closing,
    /// Cancellation is active or durably pending.
    Cancelled,
}

/// Reasons observation is unsupported without changing authority.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum UnsupportedReason {
    /// This harness has no exact supported native observation path.
    UnsupportedHarness,
    /// The run uses legacy process-exit completion.
    LegacyCompletion,
    /// The frozen role forbids this integration surface.
    RestrictedRole,
    /// No separate observer credential is explicitly configured.
    ObserverUnconfigured,
    /// Bridge denied the explicitly configured observer's read grant.
    ObserverForbidden,
}
