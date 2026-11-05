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
    /// The external agent-run notice kind selected for this publication.
    pub kind: String,
    /// Immutable rendered message; private outbox data, never diagnostic output.
    pub message: String,
    /// Earlier event from the same producer that must be accepted by the native
    /// recipient before this event may be handed off, when present.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub predecessor: Option<String>,
    /// SDK-normalized publication fingerprint; historical receipts must match it exactly.
    pub intent_hash: String,
}

impl std::fmt::Debug for BridgeIntent {
    /// Never expose destination, message, identifiers or private outbox content.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("BridgeIntent(<redacted>)")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Historical frozen intents keep their exact JSON shape and no predecessor.
    #[test]
    fn historical_intent_without_predecessor_stays_compatible() {
        let old = serde_json::json!({
            "owner":{"registration_socket":"/fixture/register","core_socket":"/fixture/core","token_file":"/fixture/token","credential_sha256":"a".repeat(64),"producer":"agent-run"},
            "request_id":"request","event_id":"event","address":"host/codex/store/session",
            "kind":"agent-run.completion","message":"fixture","intent_hash":"b".repeat(64)
        });
        let decoded: BridgeIntent = serde_json::from_value(old.clone()).unwrap();
        assert!(decoded.predecessor.is_none());
        assert_eq!(serde_json::to_value(decoded).unwrap(), old);
    }
}
