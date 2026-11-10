//! Native and custom provider transport identities, with gateway validation.

use crate::{Result, catalog::HarnessId, error::invalid};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use url::Url;

/// Maximum number of fixed headers one custom connection may send.
pub const MAX_CUSTOM_HEADERS: usize = 8;
/// Maximum length of one fixed header name.
pub const MAX_HEADER_NAME_BYTES: usize = 64;
/// Maximum length of one fixed header value template.
pub const MAX_HEADER_VALUE_BYTES: usize = 256;
/// The only substitution a fixed header value may contain.
pub const LAUNCH_PLACEHOLDER: &str = "{launch}";

/// A provider connection preserves native login or names one custom protocol.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(
    tag = "kind",
    rename_all = "snake_case",
    deny_unknown_fields,
    from = "ConnectionWire"
)]
pub enum ProviderConnection {
    /// The harness's existing native login and endpoint, with no override.
    Native,
    /// An explicitly configured HTTP gateway and matching wire protocol.
    Custom {
        /// HTTPS endpoint, or explicitly permitted loopback HTTP endpoint.
        endpoint: String,
        /// Protocol supported by the selected harness.
        protocol: ProviderProtocol,
        /// Explicit HTTP authorization header style; bearer by default.
        #[serde(default)]
        auth_header: CredentialHeader,
        /// Enables HTTP only for exact localhost or loopback IP hosts.
        #[serde(default)]
        allow_loopback_http: bool,
        /// Fixed non-secret request headers for Claude Code gateways; a value
        /// may contain the exact token `{launch}`, replaced per launch.
        #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
        headers: BTreeMap<String, String>,
    },
}

/// Decoding shape of [`ProviderConnection`]. Serde skips unknown-field checks
/// on a unit variant of an internally tagged enum, so native is decoded as an
/// empty struct variant to reject stray keys such as `headers`.
#[derive(Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
enum ConnectionWire {
    Native {},
    Custom {
        endpoint: String,
        protocol: ProviderProtocol,
        #[serde(default)]
        auth_header: CredentialHeader,
        #[serde(default)]
        allow_loopback_http: bool,
        #[serde(default)]
        headers: BTreeMap<String, String>,
    },
}

impl From<ConnectionWire> for ProviderConnection {
    fn from(wire: ConnectionWire) -> Self {
        match wire {
            ConnectionWire::Native {} => Self::Native,
            ConnectionWire::Custom {
                endpoint,
                protocol,
                auth_header,
                allow_loopback_http,
                headers,
            } => Self::Custom {
                endpoint,
                protocol,
                auth_header,
                allow_loopback_http,
                headers,
            },
        }
    }
}

/// Credential header used only for an explicitly configured custom gateway.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CredentialHeader {
    /// Authorization: Bearer, including GLM's inference gateway.
    #[default]
    Bearer,
    /// Anthropic-compatible x-api-key header.
    XApiKey,
}

impl ProviderConnection {
    /// Validates one connection for `harness`; native preserves its login,
    /// while custom requires a credential-free HTTPS or enabled loopback URL
    /// and the matching Responses or Messages wire protocol.
    pub fn validate(&self, harness: HarnessId) -> Result<()> {
        let Self::Custom {
            endpoint,
            protocol,
            auth_header,
            allow_loopback_http,
            headers,
        } = self
        else {
            return Ok(());
        };
        validate_headers(headers, harness)?;
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
            || (harness == HarnessId::Codex && *auth_header != CredentialHeader::Bearer)
        {
            return Err(invalid("unsupported provider protocol endpoint"));
        }
        Ok(())
    }

    /// Renders the Claude Code `ANTHROPIC_CUSTOM_HEADERS` value for one launch:
    /// sorted `Name: Value` lines joined by `\n`, with `{launch}` replaced by
    /// `launch`. Returns `None` for native connections and for custom
    /// connections without configured headers.
    pub fn render_custom_headers(&self, launch: &str) -> Option<String> {
        let Self::Custom { headers, .. } = self else {
            return None;
        };
        if headers.is_empty() {
            return None;
        }
        Some(
            headers
                .iter()
                .map(|(name, value)| {
                    format!("{name}: {}", value.replace(LAUNCH_PLACEHOLDER, launch))
                })
                .collect::<Vec<_>>()
                .join("\n"),
        )
    }
}

/// Validates fixed custom headers; secrets belong to accounts, never here.
fn validate_headers(headers: &BTreeMap<String, String>, harness: HarnessId) -> Result<()> {
    if headers.is_empty() {
        return Ok(());
    }
    if harness != HarnessId::ClaudeCode {
        return Err(invalid("custom headers require the claude-code harness"));
    }
    if headers.len() > MAX_CUSTOM_HEADERS {
        return Err(invalid("too many custom headers"));
    }
    let mut seen = std::collections::BTreeSet::new();
    for (name, value) in headers {
        let lower = name.to_ascii_lowercase();
        if name.is_empty()
            || name.len() > MAX_HEADER_NAME_BYTES
            || !name.bytes().all(is_token_byte)
            || !seen.insert(lower.clone())
        {
            return Err(invalid("invalid or duplicate custom header name"));
        }
        if FORBIDDEN_HEADERS.contains(&lower.as_str())
            || FORBIDDEN_HEADER_PREFIXES
                .iter()
                .any(|prefix| lower.starts_with(prefix))
            || SECRET_HEADER_FRAGMENTS
                .iter()
                .any(|fragment| lower.contains(fragment))
        {
            return Err(invalid("custom header name is reserved or secret-bearing"));
        }
        if value.is_empty()
            || value.len() > MAX_HEADER_VALUE_BYTES
            || value.trim() != value
            || !value.bytes().all(|byte| (0x20..=0x7e).contains(&byte))
        {
            return Err(invalid("invalid custom header value"));
        }
        if value.replace(LAUNCH_PLACEHOLDER, "").contains(['{', '}']) {
            return Err(invalid("custom header value has an unknown placeholder"));
        }
    }
    Ok(())
}

/// Headers the transport or credential handling owns exactly.
const FORBIDDEN_HEADERS: &[&str] = &[
    "authorization",
    "proxy-authorization",
    "x-api-key",
    "cookie",
    "host",
    "content-length",
    "content-type",
    "transfer-encoding",
    "connection",
];
/// Namespaces reserved for Anthropic and its SDK.
const FORBIDDEN_HEADER_PREFIXES: &[&str] = &["anthropic-", "x-stainless-"];
/// Name fragments that indicate a credential.
const SECRET_HEADER_FRAGMENTS: &[&str] =
    &["token", "secret", "password", "key", "credential", "auth"];

/// RFC 7230 `tchar`.
fn is_token_byte(byte: u8) -> bool {
    byte.is_ascii_alphanumeric() || b"!#$%&'*+-.^_`|~".contains(&byte)
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
    /// Historical frozen configuration only; new configuration must use `exec`.
    CodexAppserver,
    /// Historical frozen configuration only; no Lua interpreter is shipped.
    Lua,
    /// An operator-configured executable returning the versioned quota document.
    Exec,
    /// No known capacity source.
    None,
}
