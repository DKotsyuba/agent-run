//! Quota-only credential resolution and fixed failure codes.
//!
//! Collectors resolve credentials in Rust only. Explicit env/file/Keychain
//! references go through the shared [`SystemCredentialReader`] unchanged, and
//! the Claude harness's own OAuth store is read for native/named Claude logins
//! through exactly the account-home convention the harness adapters launch
//! with. Tokens never reach Lua, configuration, diagnostics, or the store; the
//! custom-gateway reader keeps refusing native logins for gateway headers.

use crate::adapters::materialize::account_home;
use crate::config::Adapter;
use agent_run_adapters::authorized_request::{CredentialReader, SystemCredentialReader};
use agent_run_domain::{catalog::HarnessId, CredentialRef, Error, Result};
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::path::{Path, PathBuf};

/// Fixed report code for every credential resolution failure.
///
/// Reader error text is never inspected or forwarded: a store may echo the
/// secret it failed to parse, so one closed code is the only safe boundary.
pub const CREDENTIAL_UNAVAILABLE: &str = "credential_unavailable";

/// Fixed report code for a failed quota-store persistence transaction.
pub const STORE_FAILED: &str = "store_failed";

/// Fixed report code for a backoff ledger that could not be persisted.
pub const BACKOFF_PERSIST_FAILED: &str = "backoff_persist_failed";

/// Quota-side protected resolver including the narrow native-login bridge.
///
/// Native Claude resolves the host login the provider adapter launches with
/// (`CLAUDE_CONFIG_DIR` when set, else `~/.claude`); a named Claude label
/// resolves only its own `accounts/claude/<label>/claude-config` directory
/// under the agent-run home. Each directory is read as Claude Code itself
/// reads it: the plaintext `.credentials.json` first, else the macOS
/// Keychain item whose service is `Claude Code-credentials`, suffixed with
/// `-<first 8 hex of sha256(config dir)>` whenever the directory is not the
/// unset default. A missing or invalid named store is an error — it never
/// falls back to the default account. Native and named Codex logins are
/// refused because Codex quota travels through app-server metadata.
pub struct QuotaCredentialReader {
    /// Agent-run home owning labelled account directories.
    app_home: PathBuf,
    /// Host native Claude config directory, and whether it was overridden
    /// through `CLAUDE_CONFIG_DIR` (which changes the Keychain service name).
    native: (PathBuf, bool),
}

impl QuotaCredentialReader {
    /// Builds a reader over explicit homes; `native_override` states whether
    /// `native_config` came from a `CLAUDE_CONFIG_DIR` override.
    pub fn new(app_home: PathBuf, native_config: PathBuf, native_override: bool) -> Self {
        Self {
            app_home,
            native: (native_config, native_override),
        }
    }

    /// Builds the reader for `app_home` from the host environment exactly as
    /// the native Claude login is resolved: a nonempty `CLAUDE_CONFIG_DIR`,
    /// else `$HOME/.claude`. Returns `None` when neither is available.
    pub fn from_host(app_home: PathBuf) -> Option<Self> {
        if let Some(dir) = std::env::var_os("CLAUDE_CONFIG_DIR").filter(|dir| !dir.is_empty()) {
            return Some(Self::new(app_home, PathBuf::from(dir), true));
        }
        let home = std::env::var_os("HOME").filter(|home| !home.is_empty())?;
        Some(Self::new(
            app_home,
            PathBuf::from(home).join(".claude"),
            false,
        ))
    }

    /// Returns the config directory and whether it counts as overridden for
    /// one Claude login; `label` absent selects the native login.
    fn claude_config(&self, label: Option<&str>) -> (PathBuf, bool) {
        match label {
            None => self.native.clone(),
            Some(label) => (
                account_home(&self.app_home, Adapter::Claude, label).join("claude-config"),
                true,
            ),
        }
    }

    /// Reads one Claude login's OAuth access token from its own store only.
    fn claude_oauth(&self, label: Option<&str>) -> Result<String> {
        let (dir, overridden) = self.claude_config(label);
        let file = dir.join(".credentials.json");
        if file.is_file() {
            if let Some(token) = SystemCredentialReader
                .read(&CredentialRef::File(file))
                .ok()
                .as_deref()
                .and_then(access_token)
            {
                return Ok(token);
            }
        }
        agent_run_platform::keychain::generic_password_service(&keychain_service(&dir, overridden))
            .as_deref()
            .and_then(access_token)
            .ok_or_else(|| Error::Validation(CREDENTIAL_UNAVAILABLE.into()))
    }
}

/// Returns Claude Code's Keychain service for one config directory.
///
/// Mirrors Claude Code's own naming: the unset default directory uses the
/// bare `Claude Code-credentials`; any explicit directory appends `-` and the
/// first eight lowercase hex digits of the SHA-256 of its path text.
pub fn keychain_service(dir: &Path, overridden: bool) -> String {
    if !overridden {
        return "Claude Code-credentials".into();
    }
    let digest = Sha256::digest(dir.to_string_lossy().as_bytes());
    let hex: String = digest.iter().take(4).map(|b| format!("{b:02x}")).collect();
    format!("Claude Code-credentials-{hex}")
}

/// Extracts a nonempty OAuth access token from one Claude credential document.
fn access_token(text: &str) -> Option<String> {
    let value: Value = serde_json::from_str(text).ok()?;
    value
        .pointer("/claudeAiOauth/accessToken")
        .or_else(|| value.get("accessToken"))
        .and_then(Value::as_str)
        .filter(|token| !token.is_empty())
        .map(str::to_owned)
}

impl CredentialReader for QuotaCredentialReader {
    /// Resolves native/named Claude through their own stores, refuses Codex
    /// logins, and delegates every explicit store to the system reader.
    fn read(&self, reference: &CredentialRef) -> Result<String> {
        match reference {
            CredentialRef::Native(HarnessId::ClaudeCode) => self.claude_oauth(None),
            CredentialRef::Named {
                harness: HarnessId::ClaudeCode,
                label,
            } => self.claude_oauth(Some(label.as_str())),
            CredentialRef::Native(HarnessId::Codex)
            | CredentialRef::Named {
                harness: HarnessId::Codex,
                ..
            } => Err(Error::Validation(CREDENTIAL_UNAVAILABLE.into())),
            _ => SystemCredentialReader.read(reference),
        }
    }
}
