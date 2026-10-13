//! Immutable external-notice intent; native routing and delivery belong to Bridge.
use serde::{Deserialize, Serialize};
use std::path::PathBuf;

/// Private immutable deployment association of one external publication.
/// Another socket pair or credential association cannot become a retry owner.
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BridgeOwner {
    /// Original configured native capability endpoint.
    pub registration_socket: PathBuf,
    /// Original authoritative custody/status endpoint; replacing its DB is not recovery.
    pub core_socket: PathBuf,
    /// Original protected credential-file association.
    pub token_file: PathBuf,
    /// Private credential fingerprint, never raw credential or ordinary diagnostic data.
    pub credential_sha256: String,
    /// Required stable authenticated publisher identity; capability policy is not identity.
    pub producer: String,
}

impl std::fmt::Debug for BridgeOwner {
    /// Never expose private endpoints, credential associations or their fingerprint.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("BridgeOwner(<redacted>)")
    }
}

/// Frozen delivery-only publication, retained before the first possible socket write.
/// IDs, destination and rendered content never change during retry or reconciliation.
/// This reference grants no worker, caller, sandbox or admission authority.
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BridgeIntent {
    /// Original retained Bridge deployment and authenticated publisher association.
    pub owner: BridgeOwner,
    /// Stable producer exchange identity; reused verbatim for exact retries.
    pub request_id: String,
    /// Stable producer event identity, derived from the existing notice ID.
    pub event_id: String,
    /// Exact canonical destination from existing binding and trusted deployment mapping.
    pub address: String,
    /// One of the four external agent-run notice kinds.
    pub kind: String,
    /// Immutable rendered message; private outbox data, never diagnostic output.
    pub message: String,
    /// SDK-normalized publication fingerprint; historical receipts must match it exactly.
    pub intent_hash: String,
}

impl std::fmt::Debug for BridgeIntent {
    /// Never expose destination, message, identifiers or private outbox content.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("BridgeIntent(<redacted>)")
    }
}
