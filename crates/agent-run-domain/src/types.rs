//! Validated scalar domain values shared across configuration, state, and transports.

use crate::{error::invalid, Error, Result};
use serde::{Deserialize, Deserializer, Serialize};
use std::{
    fmt,
    path::{Component, Path, PathBuf},
    str::FromStr,
};

pub use crate::domain::{
    external_id, nonblank, now, AgentId, Constraint, OrchestratorRef, Outcome, StartRequest,
};

/// The immutable role of one stored transcript message.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MessageRole {
    User,
    Assistant,
    ToolCall,
    ToolResult,
    System,
}

/// A finite, nonnegative timestamped transcript message with nonblank content.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Message {
    pub at: f64,
    pub role: MessageRole,
    pub content: String,
    pub name: Option<String>,
    pub raw_ref: Option<String>,
}

impl Message {
    /// Validates timestamp finiteness and required content before durable transcript storage.
    pub fn validate(&self) -> Result<()> {
        NonNegativeFinite::try_from(self.at)
            .map_err(|_| invalid("message at must be finite and nonnegative"))?;
        nonblank("message content", &self.content)
    }
}

/// A configured runtime identifier with the established ASCII name grammar.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize)]
#[serde(transparent)]
pub struct RuntimeName(String);

impl RuntimeName {
    /// Returns the validated runtime name as its wire representation.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for RuntimeName {
    /// Writes the wire representation without exposing any additional state.
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(formatter)
    }
}

impl FromStr for RuntimeName {
    type Err = Error;

    /// Parses a nonempty ASCII runtime name beginning with an alphanumeric character.
    fn from_str(value: &str) -> Result<Self> {
        let valid = !value.is_empty()
            && value.as_bytes()[0].is_ascii_alphanumeric()
            && value
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || b"_.-".contains(&byte));
        if valid {
            Ok(Self(value.into()))
        } else {
            Err(invalid("invalid runtime name"))
        }
    }
}

impl<'de> Deserialize<'de> for RuntimeName {
    /// Deserializes and validates the transparent string wire value.
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> std::result::Result<Self, D::Error> {
        String::deserialize(deserializer)?
            .parse()
            .map_err(serde::de::Error::custom)
    }
}

/// A declared account label; absence is represented by [`GlobalAccount`], never a label.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize)]
#[serde(transparent)]
pub struct AccountLabel(String);

impl AccountLabel {
    /// Returns the validated account label for path and wire construction.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for AccountLabel {
    /// Writes the safe label itself.
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(formatter)
    }
}

impl FromStr for AccountLabel {
    type Err = Error;

    /// Parses the Python-compatible 1–32 character lowercase account label grammar.
    fn from_str(value: &str) -> Result<Self> {
        if (1..=32).contains(&value.len())
            && value.bytes().all(|byte| {
                byte.is_ascii_lowercase() || byte.is_ascii_digit() || b"_-".contains(&byte)
            })
        {
            Ok(Self(value.into()))
        } else {
            Err(invalid("invalid account label"))
        }
    }
}

impl<'de> Deserialize<'de> for AccountLabel {
    /// Deserializes and validates the transparent account label wire value.
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> std::result::Result<Self, D::Error> {
        String::deserialize(deserializer)?
            .parse()
            .map_err(serde::de::Error::custom)
    }
}

/// The native runtime account selected when the wire account field is absent.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash)]
pub struct GlobalAccount;

/// A wire-account selector that preserves the distinction between native global and named accounts.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum AccountSelector {
    Global(GlobalAccount),
    Named(AccountLabel),
}

impl AccountSelector {
    /// Converts an optional wire field, where `None` means native global rather than a configured default.
    pub fn from_wire(value: Option<&str>) -> Result<Self> {
        value
            .map(|label| label.parse().map(Self::Named))
            .unwrap_or(Ok(Self::Global(GlobalAccount)))
    }

    /// Returns the optional wire representation, omitting the native-global selector.
    pub fn as_wire(&self) -> Option<&str> {
        match self {
            Self::Global(_) => None,
            Self::Named(label) => Some(label.as_str()),
        }
    }
}

/// A lowercase hexadecimal SHA-256 digest used as sealed artifact evidence.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize)]
#[serde(transparent)]
pub struct Sha256Digest(String);

impl Sha256Digest {
    /// Returns the 64-character lowercase hexadecimal digest.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl FromStr for Sha256Digest {
    type Err = Error;

    /// Parses exactly 32 bytes rendered as lowercase hexadecimal.
    fn from_str(value: &str) -> Result<Self> {
        if value.len() == 64
            && value
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
        {
            Ok(Self(value.into()))
        } else {
            Err(invalid(
                "sha256 must be 64 lowercase hexadecimal characters",
            ))
        }
    }
}

impl<'de> Deserialize<'de> for Sha256Digest {
    /// Deserializes and validates a transparent digest string.
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> std::result::Result<Self, D::Error> {
        String::deserialize(deserializer)?
            .parse()
            .map_err(serde::de::Error::custom)
    }
}

/// A finite IEEE-754 value strictly greater than zero.
#[derive(Debug, Clone, Copy, PartialEq, Serialize)]
#[serde(transparent)]
pub struct PositiveFinite(f64);

impl PositiveFinite {
    /// Returns the validated scalar value.
    pub fn get(self) -> f64 {
        self.0
    }
}

impl TryFrom<f64> for PositiveFinite {
    type Error = Error;

    /// Rejects NaN, infinities, zero, and negative values before persistence or scheduling.
    fn try_from(value: f64) -> Result<Self> {
        if value.is_finite() && value > 0.0 {
            Ok(Self(value))
        } else {
            Err(invalid("value must be positive and finite"))
        }
    }
}

impl<'de> Deserialize<'de> for PositiveFinite {
    /// Deserializes a JSON number while rejecting nonfinite and nonpositive values.
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> std::result::Result<Self, D::Error> {
        Self::try_from(f64::deserialize(deserializer)?).map_err(serde::de::Error::custom)
    }
}

/// A finite IEEE-754 value greater than or equal to zero.
#[derive(Debug, Clone, Copy, PartialEq, Serialize)]
#[serde(transparent)]
pub struct NonNegativeFinite(f64);

impl NonNegativeFinite {
    /// Returns the validated scalar value.
    pub fn get(self) -> f64 {
        self.0
    }
}

impl TryFrom<f64> for NonNegativeFinite {
    type Error = Error;

    /// Rejects NaN, infinities, and negative values before they become timestamps or durations.
    fn try_from(value: f64) -> Result<Self> {
        if value.is_finite() && value >= 0.0 {
            Ok(Self(value))
        } else {
            Err(invalid("value must be finite and nonnegative"))
        }
    }
}

impl<'de> Deserialize<'de> for NonNegativeFinite {
    /// Deserializes a JSON number while rejecting nonfinite and negative values.
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> std::result::Result<Self, D::Error> {
        Self::try_from(f64::deserialize(deserializer)?).map_err(serde::de::Error::custom)
    }
}

/// A canonical absolute directory that existed when it was validated.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(transparent)]
pub struct AbsoluteDirectory(PathBuf);

impl AbsoluteDirectory {
    /// Returns the canonical directory path captured during validation.
    pub fn as_path(&self) -> &Path {
        &self.0
    }
}

impl TryFrom<PathBuf> for AbsoluteDirectory {
    type Error = Error;

    /// Resolves an absolute existing directory, matching Python request path validation.
    fn try_from(value: PathBuf) -> Result<Self> {
        if !value.is_absolute() {
            return Err(invalid("path must be an absolute existing directory"));
        }
        let resolved = value
            .canonicalize()
            .map_err(|_| invalid("path must be an absolute existing directory"))?;
        if resolved.is_dir() {
            Ok(Self(resolved))
        } else {
            Err(invalid("path must be an absolute existing directory"))
        }
    }
}

/// A nonempty relative artifact path whose normalized components cannot escape its owned root.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(transparent)]
pub struct RelativeOwnedPath(PathBuf);

impl RelativeOwnedPath {
    /// Returns the checked relative path for descriptor-relative filesystem operations.
    pub fn as_path(&self) -> &Path {
        &self.0
    }
}

impl TryFrom<PathBuf> for RelativeOwnedPath {
    type Error = Error;

    /// Rejects absolute, parent, root, prefix, empty, and NUL-containing owned-path inputs.
    fn try_from(value: PathBuf) -> Result<Self> {
        let text = value.to_string_lossy();
        let valid = !value.as_os_str().is_empty()
            && !text.contains('\0')
            && !value.is_absolute()
            && value
                .components()
                .all(|component| matches!(component, Component::Normal(_)));
        if valid {
            Ok(Self(value))
        } else {
            Err(Error::PathEscape(
                "relative owned path escapes its root".into(),
            ))
        }
    }
}

/// A finite, nonnegative process-birth timestamp used to reject PID reuse.
#[derive(Debug, Clone, Copy, PartialEq, Serialize)]
#[serde(transparent)]
pub struct ProcessBirthProof(NonNegativeFinite);

impl ProcessBirthProof {
    /// Returns the epoch-second birth observation preserved with process identity evidence.
    pub fn get(self) -> f64 {
        self.0.get()
    }
}

impl TryFrom<f64> for ProcessBirthProof {
    type Error = Error;

    /// Validates a process-birth timestamp without accepting nonfinite observations.
    fn try_from(value: f64) -> Result<Self> {
        NonNegativeFinite::try_from(value).map(Self)
    }
}

impl<'de> Deserialize<'de> for ProcessBirthProof {
    /// Deserializes and validates a transparent process-birth value.
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> std::result::Result<Self, D::Error> {
        Self::try_from(f64::deserialize(deserializer)?).map_err(serde::de::Error::custom)
    }
}
