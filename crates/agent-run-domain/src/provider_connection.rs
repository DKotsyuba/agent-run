//! Native and custom provider transport identities, with gateway validation.

use crate::{catalog::HarnessId, error::invalid, Result};
use serde::{Deserialize, Serialize};
use url::Url;

/// A provider connection preserves native login or names one custom protocol.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum ProviderConnection {
    /// The harness's existing native login and endpoint, with no override.
    Native,
    /// An explicitly configured HTTP gateway and matching wire protocol.
    Custom {
        /// HTTPS endpoint, or explicitly permitted loopback HTTP endpoint.
        endpoint: String,
        /// Protocol supported by the selected harness.
        protocol: ProviderProtocol,
        /// Enables HTTP only for exact localhost or loopback IP hosts.
        #[serde(default)]
        allow_loopback_http: bool,
    },
}

impl ProviderConnection {
    /// Validates one connection for `harness`; native preserves its login,
    /// while custom requires a credential-free HTTPS or enabled loopback URL
    /// and the matching Responses or Messages wire protocol.
    pub fn validate(&self, harness: HarnessId) -> Result<()> {
        let Self::Custom {
            endpoint,
            protocol,
            allow_loopback_http,
        } = self
        else {
            return Ok(());
        };
        let url =
            Url::parse(endpoint).map_err(|_| invalid("invalid provider protocol endpoint"))?;
        if url.host_str().is_none()
            || !url.username().is_empty()
            || url.password().is_some()
            || url.fragment().is_some()
            || !(url.scheme() == "https"
                || (*allow_loopback_http
                    && url.scheme() == "http"
                    && matches!(url.host_str(), Some("localhost" | "127.0.0.1" | "[::1]"))))
            || !matches!(
                (harness, protocol),
                (HarnessId::Codex, ProviderProtocol::Responses)
                    | (HarnessId::ClaudeCode, ProviderProtocol::Messages)
            )
        {
            return Err(invalid("unsupported provider protocol endpoint"));
        }
        Ok(())
    }
}

/// The initial supported custom gateway wire protocols.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ProviderProtocol {
    /// OpenAI Responses wire protocol through Codex.
    Responses,
    /// Anthropic-compatible Messages wire protocol through Claude Code.
    Messages,
}

/// Explicit source of capacity observations for a provider.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LimitsSource {
    /// Harness-native capacity telemetry.
    Native,
    /// Existing local codexbar bridge.
    Codexbar,
    /// Existing OmniRoute bridge.
    Omniroute,
    /// A provider-specific collector supplied by the quota side.
    Provider,
    /// No known capacity source.
    None,
}
