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
    /// Legacy execution identity, absent from current public projections.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub run_id: Option<AgentId>,
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

/// Frozen MCP selection, not a claim of live connection or tool availability.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct McpSelectionView {
    /// Catalog server name; credentials and executable arguments are never exposed.
    pub name: String,
    /// Selection source: `global`, `profile`, or `both`.
    pub source: String,
    /// Effective exact tool cap; null means all and an empty list means none.
    pub allowed_tools: Option<Vec<String>>,
}

/// Read-only agent status, lineage, policy evidence, and observation snapshot.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AgentView {
    /// Admitted MCP selections from the sealed role; omitted on historical/empty roles.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub mcp: Vec<McpSelectionView>,
    /// Stable logical agent id, unchanged across resumes.
    pub agent_id: AgentId,
    /// Optional human display label stored at admission; `null` when unnamed.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    /// Legacy execution identity, absent from current public projections.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub run_id: Option<AgentId>,
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
    /// Legacy previous execution identity, omitted by current public projections.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub parent_run_id: Option<AgentId>,
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
    /// Working directory the run was admitted with, when recorded.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub workdir: Option<String>,
    /// Latest observed native usage of this execution, or `null` before any
    /// statistics row exists (for example while the run is still executing).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub usage: Option<UsageView>,
    /// Aggregate native usage across the lineage's executions; each metric
    /// is `null` while any contributing execution has no recorded statistics
    /// row or did not report it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub usage_cumulative: Option<UsageCumulativeView>,
    /// Latest-execution native invocation counts; historical coverage is unknown.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tool_counts: Option<ToolCountsView>,
}

/// Observed native tool invocations of the latest execution only.
/// Historical or incomplete encoder coverage leaves all counts unknown.
/// IDs deduplicate started/completed/fragments within their native attempt.
/// Failures remain null while any result is absent, unreported or contradictory.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ToolCountsView {
    /// Unique native invocations, or null when IDs/coverage are incomplete.
    pub calls: Option<u64>,
    /// Explicitly failed invocations; null while any result is unknown.
    pub failed: Option<u64>,
    /// Invocations without a consistent explicit result, when coverage is known.
    pub unknown_results: Option<u64>,
}

/// Latest observed native usage of one execution, straight from `run_stats`.
///
/// Every numeric field is the harness-reported measurement or `null` when the
/// source did not report it; `usage_source` names the protocol that supplied
/// the row and is `"none"` when no native measurement exists. Only these
/// allowlisted fields are public: internal execution identifiers never appear.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct UsageView {
    /// Prompt/input token count, when reported.
    pub input_tokens: Option<i64>,
    /// Generated/output token count, when reported.
    pub output_tokens: Option<i64>,
    /// Read-from-cache token count, when reported.
    pub cache_read_tokens: Option<i64>,
    /// Written-to-cache token count, when reported.
    pub cache_write_tokens: Option<i64>,
    /// Reasoning/thinking token count, when reported.
    pub reasoning_tokens: Option<i64>,
    /// Runtime-reported total token count, when reported.
    pub total_tokens: Option<i64>,
    /// Runtime-reported model turn count, when reported.
    pub num_turns: Option<i64>,
    /// First-token latency in milliseconds, when reported.
    pub ttft_ms: Option<f64>,
    /// API duration in milliseconds, when reported.
    pub api_duration_ms: Option<f64>,
    /// Runtime-reported USD cost, when reported.
    pub cost_usd: Option<f64>,
    /// Protocol that supplied the row: `runtime_result`, `token_usage_updated`
    /// or `none`.
    pub usage_source: String,
    /// UTC epoch seconds when the row was last recomputed.
    pub recorded_at: f64,
}

/// Aggregate usage across every execution of one logical agent lineage.
///
/// A metric is present only when the complete one-based lineage remains and
/// every execution has a recorded measurement. Missing or pruned history and
/// unreported metrics stay `null` instead of becoming partial totals.
/// `executions` counts retained lineage rows; an absent lineage has zero rows
/// and null totals. No internal execution identifiers are included.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct UsageCumulativeView {
    /// Summed prompt/input tokens, when complete.
    pub input_tokens: Option<i64>,
    /// Summed generated/output tokens, when complete.
    pub output_tokens: Option<i64>,
    /// Summed read-from-cache tokens, when complete.
    pub cache_read_tokens: Option<i64>,
    /// Summed written-to-cache tokens, when complete.
    pub cache_write_tokens: Option<i64>,
    /// Summed reasoning tokens, when complete.
    pub reasoning_tokens: Option<i64>,
    /// Summed runtime-reported totals, when complete.
    pub total_tokens: Option<i64>,
    /// Summed model turn counts, when complete.
    pub num_turns: Option<i64>,
    /// Summed runtime-reported USD cost, when complete.
    pub cost_usd: Option<f64>,
    /// Number of lineage executions the aggregate considered.
    pub executions: u64,
}

/// The durable start result, including the immediately committed agent snapshot.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StartResult {
    /// Machine receipt counter for exact admission binding; absent in legacy responses.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sequence: Option<u32>,
    /// Stable logical agent id, unchanged across explicit resumes.
    pub agent_id: AgentId,
    /// Legacy execution identity, absent from current public projections.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub run_id: Option<AgentId>,
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
    /// Legacy execution identity, absent from current public projections.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub run_id: Option<AgentId>,
    /// Command kind name.
    pub kind: String,
    /// Durable command state name.
    pub state: String,
}

/// One transcript message with raw content references left opaque.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
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
    /// Native tool-result failure flag; absent/null means unreported.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<bool>,
    /// Allowlisted native field supplying error; absent for unknown evidence.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error_source: Option<String>,
    /// First included sequence in a block; absent for raw rows.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub first_seq: Option<i64>,
    /// Last included sequence in a block; seq is this same forward cursor.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_seq: Option<i64>,
    /// Whether a prior fragment of this block lies outside this page.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub partial_before: Option<bool>,
    /// Whether a later fragment of this block lies outside this page.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub partial_after: Option<bool>,
    /// Whether original content is fully inline; historical coverage is unknown.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub content_complete: Option<bool>,
    /// UTF-8 bytes explicitly omitted from an oversized legacy inline row.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub omitted_bytes: Option<usize>,
    /// Safe boundary flag, separating executions/attempts without exposing IDs.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub starts_block: Option<bool>,
}

/// Cursor page of transcript messages; `complete` does not imply a terminal agent.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TranscriptPage {
    /// Agent whose transcript is paged.
    pub agent_id: AgentId,
    /// Legacy execution identity, absent from current public projections.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub run_id: Option<AgentId>,
    /// Ordered messages after the requested cursor.
    pub messages: Vec<MessageView>,
    /// Caller-provided cursor.
    pub cursor: i64,
    /// Requested page limit.
    pub limit: usize,
    /// Next cursor, or `null` when this page is complete.
    pub next_cursor: Option<i64>,
    /// No further rows in the indicated direction; never implies terminal state.
    pub complete: bool,
    /// Blocks when grouping was requested; absent for compatible raw pages.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub view: Option<crate::transcript::TranscriptView>,
    /// Forward or backward paging; absent means historical forward semantics.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub direction: Option<String>,
    /// Exclusive upper sequence used by a reverse block request.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub before_cursor: Option<i64>,
    /// Exclusive upper sequence to request older blocks; absent at the beginning.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub previous_cursor: Option<i64>,
    /// Last included sequence for a subsequent forward read, even on a complete page.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub resume_cursor: Option<i64>,
}

/// Verified answer metadata and optional bounded inline text.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AnswerView {
    /// Agent whose answer was requested.
    pub agent_id: AgentId,
    /// Legacy execution identity, absent from current public projections.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub run_id: Option<AgentId>,
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
