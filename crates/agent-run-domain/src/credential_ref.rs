//! Canonical, nonsecret references to operator-owned credential stores.

use crate::{
    catalog::{AuthFamily, HarnessId, SecretRef},
    error::invalid,
    types::AccountLabel,
    Result,
};
use std::{path::PathBuf, str::FromStr};

/// One existing credential location; no variant contains credential bytes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CredentialRef {
    /// Existing host-native login owned and refreshed by the harness.
    Native(HarnessId),
    /// Existing labelled native login owned and refreshed by the harness.
    Named {
        harness: HarnessId,
        label: AccountLabel,
    },
    /// Existing environment variable name, read only at request time.
    Environment(String),
    /// Absolute file path read only at request time.
    File(PathBuf),
    /// Existing macOS generic-password service and account names.
    Keychain { service: String, account: String },
}

impl CredentialRef {
    /// Parses a validated nonsecret storage reference, rejecting token-like
    /// untyped text, relative paths, malformed names, and unknown schemes.
    pub fn from_secret(reference: &SecretRef) -> Result<Self> {
        let value = reference.as_str();
        if let Some(harness) = value.strip_prefix("native:") {
            return Ok(Self::Native(harness.parse()?));
        }
        if let Some(named) = value.strip_prefix("named:") {
            let (harness, label) = named
                .split_once(':')
                .ok_or_else(|| invalid("invalid named credential reference"))?;
            return Ok(Self::Named {
                harness: harness.parse()?,
                label: label.parse()?,
            });
        }
        if let Some(name) = value.strip_prefix("env:") {
            if name.is_empty()
                || !name
                    .bytes()
                    .next()
                    .is_some_and(|b| b.is_ascii_uppercase() || b == b'_')
                || !name
                    .bytes()
                    .all(|b| b.is_ascii_uppercase() || b.is_ascii_digit() || b == b'_')
            {
                return Err(invalid("invalid environment credential reference"));
            }
            return Ok(Self::Environment(name.into()));
        }
        if let Some(path) = value.strip_prefix("file:") {
            let path = PathBuf::from(path);
            if !path.is_absolute() {
                return Err(invalid("credential file path must be absolute"));
            }
            return Ok(Self::File(path));
        }
        if let Some(names) = value.strip_prefix("keychain:") {
            let (service, account) = names
                .split_once(':')
                .ok_or_else(|| invalid("invalid Keychain credential reference"))?;
            if [service, account].iter().any(|name| {
                name.is_empty()
                    || !name
                        .bytes()
                        .all(|b| b.is_ascii_alphanumeric() || b"._-".contains(&b))
            }) {
                return Err(invalid("invalid Keychain credential reference"));
            }
            return Ok(Self::Keychain {
                service: service.into(),
                account: account.into(),
            });
        }
        Err(invalid("unsupported credential reference kind"))
    }

    /// Returns the public source kind without disclosing storage details.
    pub fn kind(&self) -> &'static str {
        match self {
            Self::Native(_) => "native",
            Self::Named { .. } => "named",
            Self::Environment(_) => "environment",
            Self::File(_) => "file",
            Self::Keychain { .. } => "keychain",
        }
    }

    /// Returns whether a native or named login belongs to `family`;
    /// explicit environment/file/Keychain stores remain provider-scoped by
    /// the account binding and custom connection contract.
    pub fn matches_family(&self, family: &AuthFamily) -> bool {
        match self {
            Self::Native(HarnessId::Codex)
            | Self::Named {
                harness: HarnessId::Codex,
                ..
            } => family.as_str() == "openai",
            Self::Native(HarnessId::ClaudeCode)
            | Self::Named {
                harness: HarnessId::ClaudeCode,
                ..
            } => family.as_str() == "anthropic",
            _ => true,
        }
    }
}

impl FromStr for CredentialRef {
    type Err = crate::Error;

    /// Parses the complete bounded reference syntax through `SecretRef`.
    fn from_str(value: &str) -> Result<Self> {
        Self::from_secret(&value.parse::<SecretRef>()?)
    }
}
