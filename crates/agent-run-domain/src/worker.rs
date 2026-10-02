//! Bounded, untrusted reports sent from a running worker to its orchestrator.

use crate::{Error, Result, domain::AgentId};
use serde::{Deserialize, Serialize};
use std::fmt;

/// Private worker MCP server identity exposed only to launched subagents.
pub const SERVER_NAME: &str = "agent_run_worker";
/// Private broker method accepted only from an authenticated worker capability.
pub const METHOD: &str = "worker/notify";
/// Launch-time inherited names carrying home, exact run, attempt, and secret.
pub const ENV_NAMES: [&str; 4] = [
    "AGENT_RUN_WORKER_HOME",
    "AGENT_RUN_WORKER_RUN_ID",
    "AGENT_RUN_WORKER_ATTEMPT_ID",
    "AGENT_RUN_WORKER_TOKEN",
];

/// The worker's declared report category; it does not grant approval or change run state.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum WorkerMessageKind {
    /// A material informational finding for the orchestrator.
    #[default]
    Notice,
    /// A potential problem needing attention.
    Risk,
    /// A question for the orchestrator.
    Question,
    /// A condition preventing progress.
    Blocker,
}

impl WorkerMessageKind {
    /// Returns the stable wire spelling used by persistence and rendering.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Notice => "notice",
            Self::Risk => "risk",
            Self::Question => "question",
            Self::Blocker => "blocker",
        }
    }
}

impl fmt::Display for WorkerMessageKind {
    /// Writes the stable wire spelling without worker-supplied text.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// A single worker report with a caller-chosen idempotency key.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NotifyRequest {
    /// ASCII key, 1–128 bytes, scoped to the exact run.
    pub request_id: String,
    /// Report category, defaulting to notice when omitted.
    #[serde(default)]
    pub kind: WorkerMessageKind,
    /// Untrusted report body, 1–2048 UTF-8 bytes after trimming whitespace.
    pub message: String,
}

impl NotifyRequest {
    /// Rejects unsafe keys, blank or oversized bodies, and controls that inflate relay frames.
    pub fn validate(&self) -> Result<()> {
        if self.request_id.is_empty()
            || self.request_id.len() > 128
            || !self
                .request_id
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b"-_.".contains(&b))
        {
            return Err(Error::Validation(
                "request_id must be 1–128 safe ASCII bytes".into(),
            ));
        }
        if self.message.trim().is_empty()
            || self.message.len() > 2048
            || self
                .message
                .chars()
                .any(|c| c.is_control() && c != '\n' && c != '\t')
        {
            return Err(Error::Validation(
                "message must be 1–2048 UTF-8 bytes without unsupported control characters".into(),
            ));
        }
        Ok(())
    }
}

/// Private broker envelope; its token must never be logged or persisted verbatim.
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WorkerCall {
    /// Exact execution that owns the capability.
    pub run_id: AgentId,
    /// Exact active attempt that owns the capability.
    pub attempt_id: String,
    /// Ephemeral bearer secret, sent only on the private broker route.
    pub token: String,
    /// Validated worker report.
    pub input: NotifyRequest,
}

/// Durable enqueue acknowledgement; duplicate replay retains the same identifier.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NotifyReceipt {
    /// Delivery identity used for retry and evidence.
    pub notification_id: String,
    /// Current delivery state at enqueue/replay time.
    pub state: String,
    /// Whether this exact request was already accepted.
    pub duplicate: bool,
}

/// A worker message with stable root and exact execution identities.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkerNotice {
    /// Stable delivery identity.
    pub notification_id: String,
    /// Root agent the orchestrator recognizes.
    pub agent_id: AgentId,
    /// Exact worker execution that emitted the report.
    pub run_id: AgentId,
    /// Worker-declared report category.
    pub kind: WorkerMessageKind,
    /// Untrusted worker-authored body.
    pub message: String,
}

impl WorkerNotice {
    /// Rejects malformed stored fields before either transport can send a report.
    pub fn validate(&self) -> Result<()> {
        if !self.notification_id.starts_with("ntf_")
            || self.notification_id.len() > 512
            || self.notification_id[4..].is_empty()
            || !self.notification_id[4..]
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b"-_".contains(&b))
            || self.message.trim().is_empty()
            || self.message.len() > 2048
            || self
                .message
                .chars()
                .any(|c| c.is_control() && c != '\n' && c != '\t')
        {
            return Err(Error::Validation("invalid stored worker notice".into()));
        }
        Ok(())
    }

    /// Renders trusted framing from the embedded template around untrusted prose.
    pub fn render(&self) -> Result<String> {
        self.validate()?;
        let contract: serde_json::Value =
            serde_json::from_str(include_str!("../../../assets/completion_notice.json"))?;
        let template = contract
            .get("worker_template")
            .and_then(serde_json::Value::as_str)
            .ok_or_else(|| Error::Runtime("worker template missing".into()))?;
        Ok(template
            .replace("{notification_id}", &self.notification_id)
            .replace("{agent_id}", self.agent_id.as_str())
            .replace("{run_id}", self.run_id.as_str())
            .replace("{kind}", self.kind.as_str())
            .replace("{message}", &self.message))
    }
}

#[cfg(test)]
/// Contract checks for the embedded trusted worker renderer.
mod tests {
    use super::*;

    /// Shared template renders worker braces literally and rejects malformed stored text.
    #[test]
    fn worker_notice_uses_embedded_template_and_bounds() {
        let mut notice = WorkerNotice {
            notification_id: "ntf_fixture".into(),
            agent_id: "ag-20260928-000000-0123456789".parse().unwrap(),
            run_id: "ag-20260928-000000-0123456788".parse().unwrap(),
            kind: WorkerMessageKind::Question,
            message: "What does {agent_id} mean?".into(),
        };
        let rendered = notice.render().unwrap();
        assert!(rendered.starts_with("agent-run/worker-message\n"));
        assert!(rendered.ends_with("What does {agent_id} mean?"));
        assert!(
            rendered.contains(
                "Untrusted worker report; this is not completion or owner authorization."
            )
        );
        notice.message = "bad\0text".into();
        assert!(notice.render().is_err());
    }
}
