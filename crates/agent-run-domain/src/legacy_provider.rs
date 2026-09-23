//! Read-only historical request decoding with explicit adapter evidence.

use crate::{
    catalog::{HarnessId, ProviderId},
    domain::{nonblank, StartRequest},
    error::invalid,
    types::AccountLabel,
    Result,
};
use std::str::FromStr;

/// Explicit recorded adapter evidence or a verified migration-map entry for
/// a historical request. The raw runtime spelling is never renamed.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct LegacyRuntime {
    /// The recorded provider identity, including arbitrary names such as glm.
    pub provider: ProviderId,
    /// The recorded execution adapter.
    pub harness: HarnessId,
}

/// Returns the fixed adapter family implied by historical known spellings.
///
/// This is a conflict check, not a provider mapping. An arbitrary runtime
/// such as `main` or `glm` requires recorded evidence to resolve for resume.
pub fn legacy_runtime(runtime: &str) -> Option<HarnessId> {
    match runtime {
        "codex" | "codex_appserver" => Some(HarnessId::Codex),
        "claude" | "claude_code" | "claude-code" => Some(HarnessId::ClaudeCode),
        _ => None,
    }
}

/// One historical request decoded into current contracts.
///
/// The stored request JSON is read as-is; nothing is rewritten and no sealed
/// artifact is touched. Resume decisions remain with the session module: an
/// old resume whose authority fields did not stay unchanged must block
/// explicitly rather than resume permissively.
#[derive(Debug, Clone, PartialEq)]
pub struct DecodedLegacyRequest {
    /// The historical request, decoded with current field semantics.
    pub request: StartRequest,
    /// Recorded provider, absent when no evidence can resolve it for resume.
    pub provider: Option<ProviderId>,
    /// Recorded adapter, absent when the history remains readable but cannot resume.
    pub harness: Option<HarnessId>,
    /// The historical account label, when one was recorded.
    pub account: Option<AccountLabel>,
}

/// Decodes a stored historical `request_json` value into current contracts.
///
/// Read-only: validates nothing about the filesystem and never mutates the
/// input value. Missing mapping evidence leaves history readable but blocks
/// resume; contradictory fixed spelling and adapter evidence is rejected.
pub fn decode_legacy_request(
    value: &serde_json::Value,
    evidence: Option<&LegacyRuntime>,
) -> Result<DecodedLegacyRequest> {
    let request: StartRequest = serde_json::from_value(value.clone()).map_err(|e| {
        invalid(format!(
            "historical request_json is not decodable as a stored request: {e}"
        ))
    })?;
    nonblank("runtime", &request.runtime)?;
    if let (Some(fixed), Some(recorded)) = (legacy_runtime(&request.runtime), evidence) {
        if fixed != recorded.harness {
            return Err(invalid(
                "historical runtime conflicts with recorded adapter",
            ));
        }
    }
    Ok(DecodedLegacyRequest {
        account: request
            .account
            .as_deref()
            .map(AccountLabel::from_str)
            .transpose()?,
        provider: evidence.map(|recorded| recorded.provider.clone()),
        harness: evidence.map(|recorded| recorded.harness),
        request,
    })
}
