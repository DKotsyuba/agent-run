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

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OrchestratorRef {
    pub transport: String,
    pub external_session_id: String,
    #[serde(default)]
    pub external_turn_id: Option<String>,
}
impl OrchestratorRef {
    pub fn validate(&self) -> Result<()> {
        external_id("transport", &self.transport)?;
        external_id("external_session_id", &self.external_session_id)?;
        if let Some(v) = &self.external_turn_id {
            external_id("external_turn_id", v)?;
        }
        Ok(())
    }
}
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
    /// Legacy metadata only. Never an execution deadline.
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
    pub fn validate(&mut self) -> Result<()> {
        for (name, s) in [
            ("runtime", &self.runtime),
            ("model", &self.model),
            ("profile", &self.profile),
            ("task", &self.task),
        ] {
            nonblank(name, s)?;
        }
        if self.task.len() > 512 * 1024 {
            return Err(invalid("task exceeds 512 KiB"));
        }
        for (name, s) in [("effort", &self.effort), ("account", &self.account)] {
            if let Some(s) = s {
                nonblank(name, s)?;
            }
        }
        if let Some(v) = self.timeout_seconds {
            if !v.is_finite() || v <= 0.0 {
                return Err(invalid("timeout_seconds must be positive and finite"));
            }
        }
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
        if let Some(r) = &self.orchestrator {
            r.validate()?;
        }
        if let Some(r) = &self.request_id {
            external_id("request_id", r)?;
        }
        Ok(())
    }
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
