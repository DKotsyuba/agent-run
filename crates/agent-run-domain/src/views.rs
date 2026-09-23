//! Transport-safe read-model DTOs with Python-compatible field ordering and nullability.

use crate::domain::{AgentId, Status};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::path::PathBuf;

/// Current bounded delivery state and its optional latest secret-safe evidence payload.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DeliveryView {
    /// Agent that owns this delivery record.
    pub agent_id: AgentId,
    /// Whether an orchestrator session is durably bound.
    pub bound: bool,
    /// Bound external session id, when one exists.
    pub orchestrator_session_id: Option<String>,
    /// Delivery notification id, when a notice was created.
    pub notification_id: Option<String>,
    /// Durable delivery state name.
    pub state: String,
    /// Number of owned queue attempts.
    pub attempts: u32,
    /// Whether the latest transport result is ambiguous.
    pub ambiguous: bool,
    /// Latest bounded, secret-safe error text.
    pub last_error: Option<String>,
    /// Latest bounded delivery evidence without credentials or message content.
    pub last_attempt: Option<Value>,
}

/// Last process-cleanup observation, retaining unavailable descendant evidence as `null`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CleanupView {
    /// Cleanup signal names observed in attempt order.
    pub signals: Vec<String>,
    /// Observation scope name.
    pub scope: String,
    /// Whether the original process group was gone.
    pub group_gone: bool,
    /// Whether observable descendants were gone, or `null` if unavailable.
    pub descendants_gone: Option<bool>,
    /// Whether cleanup evidence is sufficient to confirm completion.
    pub confirmed: bool,
    /// Diagnostic process group id, when known.
    pub process_group_id: Option<i32>,
}

/// Read-only agent status, lineage, policy evidence, and observation snapshot.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AgentView {
    /// Durable agent id.
    pub agent_id: AgentId,
    /// Selected runtime name.
    pub runtime: String,
    /// Selected model name.
    pub model: String,
    /// Selected profile name.
    pub profile: String,
    /// Bounded whitespace-normalized task summary.
    pub task_summary: String,
    /// Current lifecycle status.
    pub status: Status,
    /// UTC epoch seconds when admission committed.
    pub created_at: f64,
    /// UTC epoch seconds when execution began, or `null` before then.
    pub started_at: Option<f64>,
    /// UTC epoch seconds when terminal state committed, or `null` while active.
    pub finished_at: Option<f64>,
    /// Nonnegative elapsed seconds at observation time.
    pub elapsed_seconds: f64,
    /// Last persisted transcript progress time, when any.
    pub last_progress_at: Option<f64>,
    /// Active-run silence duration, or `null` for terminal rows.
    pub silence_seconds: Option<f64>,
    /// Whether the stall watchdog emitted a warning.
    pub warned: bool,
    /// Stable failure category, when terminal failure evidence exists.
    pub failure_kind: Option<String>,
    /// Bounded explanatory failure text, when any.
    pub failure_text: Option<String>,
    /// Whether a sealed answer path exists.
    pub answer_available: bool,
    /// Sealed answer byte count, when available.
    pub answer_bytes: Option<u64>,
    /// Sealed answer SHA-256 digest, when available.
    pub answer_sha256: Option<String>,
    /// Requested effort, preserving unspecified as `null`.
    pub effort: Option<String>,
    /// Current delivery snapshot.
    pub delivery: DeliveryView,
    /// Parent run resumed by this agent, or `null` for a fresh run.
    pub parent_agent_id: Option<AgentId>,
    /// First run in the lineage, or `null` for historical incomplete rows.
    pub root_agent_id: Option<AgentId>,
    /// One-based lineage position.
    pub sequence: u32,
    /// Latest cleanup evidence, when recorded.
    pub cleanup: Option<CleanupView>,
    /// Immutable effective-policy evidence, when available.
    pub policy: Option<Value>,
    /// Operator-facing preparation/execution phase.
    pub phase: String,
    /// UTC epoch seconds when the current phase began.
    pub phase_started_at: f64,
    /// Process observation state name.
    pub process_state: String,
    /// UTC epoch seconds when this view was built.
    pub observed_at: f64,
    /// Terminal runtime outcome value, or `null` while active.
    pub runtime_outcome: Option<String>,
    /// Human/orchestrator acceptance state, distinct from runtime success.
    pub acceptance: String,
}

/// The durable start result, including the immediately committed agent snapshot.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StartResult {
    /// Durable id assigned to the requested run.
    pub agent_id: AgentId,
    /// Whether this call created rather than replayed admission.
    pub created: bool,
    /// The provider attempt the admission (or its replay) owns; absent in
    /// historical runtime start responses.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub attempt_id: Option<String>,
    /// Immediately committed status snapshot.
    pub agent: AgentView,
}

/// One durable command queue entry.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CommandView {
    /// Monotonic durable command id.
    pub command_id: i64,
    /// Agent that owns the command.
    pub agent_id: AgentId,
    /// Command kind name.
    pub kind: String,
    /// Durable command state name.
    pub state: String,
}

/// One transcript message with raw content references left opaque.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MessageView {
    /// Per-agent transcript sequence cursor.
    pub seq: i64,
    /// UTC epoch seconds recorded for the message.
    pub at: f64,
    /// Message role wire value.
    pub role: String,
    /// Optional role-specific name.
    pub name: Option<String>,
    /// Message text stored in the transcript.
    pub content: String,
    /// Opaque raw-stream reference, never auto-expanded.
    pub raw_ref: Option<String>,
}

/// Cursor page of transcript messages; `complete` does not imply a terminal agent.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TranscriptPage {
    /// Agent whose transcript is paged.
    pub agent_id: AgentId,
    /// Ordered messages after the requested cursor.
    pub messages: Vec<MessageView>,
    /// Caller-provided cursor.
    pub cursor: i64,
    /// Requested page limit.
    pub limit: usize,
    /// Next cursor, or `null` when this page is complete.
    pub next_cursor: Option<i64>,
    /// Whether no further transcript rows follow this page.
    pub complete: bool,
}

/// Verified answer metadata and optional bounded inline text.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AnswerView {
    /// Agent whose answer was requested.
    pub agent_id: AgentId,
    /// Current lifecycle status, independent of availability.
    pub status: Status,
    /// Whether a verified sealed answer exists.
    pub available: bool,
    /// Stored answer path, or `null` when unavailable.
    pub path: Option<PathBuf>,
    /// Verified byte size, or `null` when unavailable.
    pub size_bytes: Option<u64>,
    /// Verified SHA-256 string, or `null` when unavailable.
    pub sha256: Option<String>,
    /// Bounded inline content, or `null` when unavailable or too large.
    pub content: Option<String>,
    /// Whether `content` contains the entire verified answer.
    pub inline_complete: bool,
    /// Owned relative answer path, or `null` when unavailable.
    pub relative_path: Option<String>,
    /// Artifact kind, or `null` when unavailable.
    pub kind: Option<String>,
    /// Artifact media type, or `null` when unavailable.
    pub media_type: Option<String>,
    /// Answer proof format version, or `null` when unavailable.
    pub proof_version: Option<u32>,
}

/// Offset page of agent snapshots with an exact total and committed revision.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AgentPage {
    /// Ordered agent snapshots.
    pub items: Vec<AgentView>,
    /// Exact matching-row count.
    pub total: usize,
    /// Requested offset.
    pub offset: usize,
    /// Requested page limit.
    pub limit: usize,
    /// Offset for another page, or `null` when complete.
    pub next_offset: Option<usize>,
    /// Whether no further matching rows remain.
    pub complete: bool,
    /// Committed store revision observed for the page.
    pub revision: i64,
    /// UTC epoch seconds when the page was built.
    pub observed_at: f64,
}

/// Optional exact filters of the public `models` provider catalog.
///
/// Every filter is an exact configured identifier, never a fuzzy or
/// semantic match: `provider` selects one configured provider, `model` keeps
/// providers that explicitly offer that provider-visible model id, and
/// `profile` keeps only model offerings the named canonical role can be
/// admitted with (the same role load and policy check admission applies).
/// An unknown value is a validation error; absent filters return everything.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ModelsQuery {
    /// Exact configured provider id.
    #[serde(default)]
    pub provider: Option<String>,
    /// Exact canonical role/profile name.
    #[serde(default)]
    pub profile: Option<String>,
    /// Exact provider-visible model id.
    #[serde(default)]
    pub model: Option<String>,
}

impl ModelsQuery {
    /// Returns whether no filter is set.
    pub fn is_empty(&self) -> bool {
        self.provider.is_none() && self.profile.is_none() && self.model.is_none()
    }
}

/// Optional exact model filter of the public provider `capacity_order`.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CapacityOrderQuery {
    /// Exact provider-visible model id; keeps only providers offering it and
    /// ranks each by that model's own availability.
    #[serde(default)]
    pub model: Option<String>,
}
