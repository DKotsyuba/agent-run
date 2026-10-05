use crate::{error::invalid, Error, Result};
use chrono::{NaiveDateTime, Utc};
use serde::{Deserialize, Serialize};
use std::{collections::BTreeSet, fmt, path::PathBuf, str::FromStr};

/// Constraints that a resolved execution policy can require or evidence.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Constraint {
    WebToolsDisabled,
    ExternalNetworkIsolation,
    LoopbackTcpIsolation,
    UnixIpcIsolation,
    McpIpcIsolation,
    FilesystemWriteIsolation,
    FilesystemReadIsolation,
    PluginImmutability,
}
impl Constraint {
    /// Lists every recognized constraint in stable declaration order.
    pub const ALL: [Self; 8] = [
        Self::WebToolsDisabled,
        Self::ExternalNetworkIsolation,
        Self::LoopbackTcpIsolation,
        Self::UnixIpcIsolation,
        Self::McpIpcIsolation,
        Self::FilesystemWriteIsolation,
        Self::FilesystemReadIsolation,
        Self::PluginImmutability,
    ];
}

pub fn now() -> f64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs_f64()
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize)]
#[serde(transparent)]
pub struct AgentId(String);
impl AgentId {
    pub fn new() -> Self {
        Self(format!(
            "ag-{}-{}",
            Utc::now().format("%Y%m%d-%H%M%S"),
            &uuid::Uuid::new_v4().simple().to_string()[..10]
        ))
    }
    pub fn as_str(&self) -> &str {
        &self.0
    }
}
impl Default for AgentId {
    fn default() -> Self {
        Self::new()
    }
}
impl fmt::Display for AgentId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}
impl FromStr for AgentId {
    type Err = Error;
    fn from_str(s: &str) -> Result<Self> {
        if s.len() != 29
            || !s.is_ascii()
            || !s.starts_with("ag-")
            || s.as_bytes()[18] != b'-'
            || !s[19..]
                .bytes()
                .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
            || NaiveDateTime::parse_from_str(&s[3..18], "%Y%m%d-%H%M%S").is_err()
        {
            return Err(invalid(
                "agent_id must match ag-YYYYMMDD-HHMMSS-<10 lowercase hex>",
            ));
        }
        Ok(Self(s.into()))
    }
}
impl<'de> Deserialize<'de> for AgentId {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> std::result::Result<Self, D::Error> {
        String::deserialize(d)?
            .parse()
            .map_err(serde::de::Error::custom)
    }
}

#[derive(Debug, Copy, Clone, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Status {
    Created,
    Starting,
    Running,
    Cancelling,
    Succeeded,
    Failed,
    TimedOut,
    Cancelled,
    Lost,
}
impl Status {
    pub const ALL: [Self; 9] = [
        Self::Created,
        Self::Starting,
        Self::Running,
        Self::Cancelling,
        Self::Succeeded,
        Self::Failed,
        Self::TimedOut,
        Self::Cancelled,
        Self::Lost,
    ];
    pub fn terminal(self) -> bool {
        matches!(
            self,
            Self::Succeeded | Self::Failed | Self::TimedOut | Self::Cancelled | Self::Lost
        )
    }
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Created => "created",
            Self::Starting => "starting",
            Self::Running => "running",
            Self::Cancelling => "cancelling",
            Self::Succeeded => "succeeded",
            Self::Failed => "failed",
            Self::TimedOut => "timed_out",
            Self::Cancelled => "cancelled",
            Self::Lost => "lost",
        }
    }
    pub fn transition(self, to: Self) -> Result<()> {
        let ok = match self {
            Self::Created => matches!(
                to,
                Self::Starting | Self::Cancelled | Self::Failed | Self::Lost
            ),
            Self::Starting => matches!(
                to,
                Self::Running | Self::Cancelled | Self::Failed | Self::Lost
            ),
            Self::Running => matches!(
                to,
                Self::Succeeded | Self::Failed | Self::TimedOut | Self::Cancelling | Self::Lost
            ),
            Self::Cancelling => matches!(to, Self::Cancelled | Self::Lost),
            _ => false,
        };
        if ok {
            Ok(())
        } else {
            Err(Error::Transition(format!(
                "{} -> {}",
                self.as_str(),
                to.as_str()
            )))
        }
    }
}
impl FromStr for Status {
    type Err = Error;
    fn from_str(v: &str) -> Result<Self> {
        Self::ALL
            .into_iter()
            .find(|s| s.as_str() == v)
            .ok_or_else(|| invalid("unknown status"))
    }
}

pub fn nonblank(label: &str, s: &str) -> Result<()> {
    if s.trim().is_empty() || s.contains('\0') {
        return Err(invalid(format!(
            "{label} must be a nonblank NUL-free string"
        )));
    }
    Ok(())
}
pub fn external_id(label: &str, s: &str) -> Result<()> {
    nonblank(label, s)?;
    if s.chars().count() > 512 {
        return Err(invalid(format!("{label} exceeds 512 characters")));
    }
    Ok(())
}

/// The largest accepted display label length, in Unicode scalar values.
pub const MAX_DISPLAY_NAME_CHARS: usize = 64;

/// Normalizes one optional human display label for durable storage.
///
/// The label is a human-facing UTF-8 string, not an identifier: it is trimmed,
/// must stay nonblank, hold at most [`MAX_DISPLAY_NAME_CHARS`] Unicode scalar
/// values, and contain no control, bidirectional-embedding, or other format
/// characters that a terminal or list view could render as executable
/// formatting. Anything else is a validation error; the label is never
/// derived from task text and confers no authority.
pub fn display_name(label: &str) -> Result<String> {
    let trimmed = label.trim();
    if trimmed.is_empty() || trimmed.contains('\0') {
        return Err(invalid("display name must be a nonblank NUL-free string"));
    }
    if trimmed.chars().count() > MAX_DISPLAY_NAME_CHARS {
        return Err(invalid("display name exceeds 64 characters"));
    }
    if trimmed.chars().any(is_unsafe_label_char) {
        return Err(invalid(
            "display name must not contain control, bidi or format characters",
        ));
    }
    Ok(trimmed.to_owned())
}

/// Reports one character a display label must not carry: C0/C1 controls and
/// the directional isolates, overrides, and invisible format marks whose
/// rendering could mislabel or execute in terminal output.
fn is_unsafe_label_char(c: char) -> bool {
    c.is_control()
        || matches!(
            c,
            '\u{00ad}'
                | '\u{200e}'..='\u{200f}'
                | '\u{2028}'..='\u{202e}'
                | '\u{2060}'..='\u{206f}'
                | '\u{feff}'
        )
}

/// Identifies one external conversation for binding and completion delivery.
///
/// `transport` accepts canonical `codex_queue` and `claude_uds`, plus the
/// `codex` and `claude` aliases; normalization stores only canonical names.
/// Session and optional turn ids are opaque, nonblank external ids. Session
/// identity excludes the turn so later turns remain in the same durable scope.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OrchestratorRef {
    /// Supported delivery transport or its known user-facing alias.
    pub transport: String,
    /// Opaque host conversation identity; not exposed in delivery diagnostics.
    pub external_session_id: String,
    /// Optional opaque id for the current external turn.
    #[serde(default)]
    pub external_turn_id: Option<String>,
}
impl OrchestratorRef {
    /// Returns the canonical delivery transport for this supported name or alias.
    pub fn canonical_transport(&self) -> Result<&'static str> {
        Self::canonical_transport_name(&self.transport)
    }

    /// Maps one persisted or caller-supplied name to its supported canonical transport.
    /// Unknown names return a typed validation error.
    pub fn canonical_transport_name(transport: &str) -> Result<&'static str> {
        match transport {
            "codex" | "codex_queue" => Ok("codex_queue"),
            "claude" | "claude_uds" => Ok("claude_uds"),
            _ => Err(invalid("unsupported orchestrator transport")),
        }
    }

    /// Replaces a known alias with its canonical name after validating all ids.
    pub fn normalize(&mut self) -> Result<()> {
        self.validate()?;
        self.transport = self.canonical_transport()?.to_owned();
        Ok(())
    }

    /// Validates opaque ids and rejects unsupported delivery transports.
    pub fn validate(&self) -> Result<()> {
        self.canonical_transport()?;
        external_id("external_session_id", &self.external_session_id)?;
        if let Some(v) = &self.external_turn_id {
            external_id("external_turn_id", v)?;
        }
        Ok(())
    }
}
/// Historical runtime request and provider storage projection for one execution.
/// Callers validate before admission; canonical paths and normalized optional
/// labels participate in replay identity. An absent label stays omitted in JSON
/// to preserve pre-label fingerprints. Requests grant no authority on their own.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StartRequest {
    pub runtime: String,
    pub model: String,
    pub profile: String,
    pub task: String,
    pub workdir: PathBuf,
    #[serde(default)]
    pub write: bool,
    #[serde(default)]
    pub fast: bool,
    #[serde(default)]
    pub effort: Option<String>,
    /// Optional human display label for the agent; normalized in place by
    /// [`display_name`] during validation so equal labels replay identically.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub display_name: Option<String>,
    /// The run's whole-run deadline in seconds from admission, at most
    /// [`MAX_TIMEOUT_SECONDS`]; absence takes the configured default.
    #[serde(default)]
    pub timeout_seconds: Option<f64>,
    #[serde(default)]
    pub read_roots: Vec<PathBuf>,
    #[serde(default)]
    pub output_schema: Option<serde_json::Map<String, serde_json::Value>>,
    #[serde(default)]
    pub orchestrator: Option<OrchestratorRef>,
    #[serde(default)]
    pub request_id: Option<String>,
    #[serde(default)]
    pub account: Option<String>,
    #[serde(default, deserialize_with = "unique_constraints")]
    pub required_constraints: BTreeSet<Constraint>,
}
fn unique_constraints<'de, D: serde::Deserializer<'de>>(
    d: D,
) -> std::result::Result<BTreeSet<Constraint>, D::Error> {
    let a = Vec::<Constraint>::deserialize(d)?;
    let b: BTreeSet<_> = a.iter().copied().collect();
    if a.len() != b.len() {
        return Err(serde::de::Error::custom("duplicate constraints"));
    }
    Ok(b)
}
impl StartRequest {
    /// Validates admission inputs and canonicalizes directory paths, the optional
    /// human label and known orchestrator aliases in place. Task text is nonblank
    /// and at most 512 KiB; timeout, namespace and request-id bounds use shared
    /// validators. Unsupported transports and duplicate canonical read roots are
    /// rejected before durable admission.
    pub fn validate(&mut self) -> Result<()> {
        self.validate_intent()?;
        self.workdir = existing_dir(&self.workdir)?;
        self.read_roots = self
            .read_roots
            .iter()
            .map(|p| existing_dir(p))
            .collect::<Result<_>>()?;
        let mut seen = BTreeSet::new();
        if self.read_roots.iter().any(|p| !seen.insert(p.clone())) {
            return Err(invalid("read_roots must not contain duplicates"));
        }
        Ok(())
    }

    /// Validates and normalizes immutable request fields without accessing the filesystem.
    /// Paths must be absolute; only a new admission subsequently requires them to exist.
    /// This permits an exact durable replay after a workspace has been removed.
    pub fn validate_intent(&mut self) -> Result<()> {
        for (name, s) in [
            ("runtime", &self.runtime),
            ("model", &self.model),
            ("profile", &self.profile),
        ] {
            nonblank(name, s)?;
        }
        task_text(&self.task)?;
        for (name, s) in [("effort", &self.effort), ("account", &self.account)] {
            if let Some(s) = s {
                nonblank(name, s)?;
            }
        }
        if let Some(label) = &self.display_name {
            self.display_name = Some(display_name(label)?);
        }
        if let Some(v) = self.timeout_seconds {
            timeout_seconds(v)?;
        }
        if !self.workdir.is_absolute() || self.read_roots.iter().any(|path| !path.is_absolute()) {
            return Err(invalid("paths must be absolute"));
        }
        let mut seen = BTreeSet::new();
        if self.read_roots.iter().any(|p| !seen.insert(p.clone())) {
            return Err(invalid("read_roots must not contain duplicates"));
        }
        if let Some(r) = &mut self.orchestrator {
            r.normalize()?;
        }
        if let Some(r) = &self.request_id {
            external_id("request_id", r)?;
        }
        Ok(())
    }
}
/// The largest accepted task text, in UTF-8 bytes.
pub const MAX_TASK_BYTES: usize = 512 * 1024;

/// Accepts a nonblank NUL-free task of at most [`MAX_TASK_BYTES`] bytes.
pub fn task_text(task: &str) -> Result<()> {
    nonblank("task", task)?;
    if task.len() > MAX_TASK_BYTES {
        return Err(invalid("task exceeds 512 KiB"));
    }
    Ok(())
}

/// The largest accepted run timeout: 30 days in seconds.
///
/// The bound keeps `created_at + timeout` and every remaining-time duration
/// representable, so an accepted timeout can never overflow a runtime clock.
pub const MAX_TIMEOUT_SECONDS: f64 = 30.0 * 24.0 * 3600.0;

/// Accepts a run timeout in seconds that is finite, positive and at most
/// [`MAX_TIMEOUT_SECONDS`]; anything else is a validation error.
pub fn timeout_seconds(value: f64) -> Result<f64> {
    if !value.is_finite() || value <= 0.0 || value > MAX_TIMEOUT_SECONDS {
        return Err(invalid(
            "timeout_seconds must be positive, finite and at most 2592000",
        ));
    }
    Ok(value)
}
pub fn existing_dir(path: &std::path::Path) -> Result<PathBuf> {
    if !path.is_absolute() {
        return Err(invalid("path must be an absolute existing directory"));
    }
    let p = path
        .canonicalize()
        .map_err(|_| invalid("directory does not exist"))?;
    if !p.is_dir() {
        return Err(invalid("path is not a directory"));
    }
    Ok(p)
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Outcome {
    pub status: Status,
    pub exit_code: Option<i32>,
    pub failure_kind: Option<String>,
    pub failure_text: Option<String>,
    pub runtime_session_id: Option<String>,
}

impl Outcome {
    pub fn success(session: Option<String>) -> Self {
        Self {
            status: Status::Succeeded,
            exit_code: Some(0),
            failure_kind: None,
            failure_text: None,
            runtime_session_id: session,
        }
    }
    pub fn failure(kind: impl Into<String>) -> Self {
        Self {
            status: Status::Failed,
            exit_code: None,
            failure_kind: Some(kind.into()),
            failure_text: None,
            runtime_session_id: None,
        }
    }
}

#[cfg(test)]
/// Contract checks for supported orchestrator names and validation.
mod orchestrator_ref_tests {
    use super::*;

    /// Known user-facing names normalize to the transport used by delivery adapters.
    #[test]
    fn known_transport_aliases_normalize() {
        for (alias, canonical) in [("codex", "codex_queue"), ("claude", "claude_uds")] {
            let mut reference = OrchestratorRef {
                transport: alias.into(),
                external_session_id: "session".into(),
                external_turn_id: None,
            };
            reference.normalize().unwrap();
            assert_eq!(reference.transport, canonical);
        }
    }

    /// Unknown transport names fail with the shared typed validation code.
    #[test]
    fn unknown_transport_is_rejected() {
        let reference = OrchestratorRef {
            transport: "other".into(),
            external_session_id: "session".into(),
            external_turn_id: None,
        };
        assert_eq!(
            reference.validate().unwrap_err().machine_code(),
            crate::error::MachineCode::ValidationError
        );
    }
}
