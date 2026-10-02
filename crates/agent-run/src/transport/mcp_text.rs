//! Compact MiniJinja presentation for every public MCP tool result.
//!
//! MCP successes render as one short plain-text page per tool from the
//! repo-owned templates in `assets/mcp/`, embedded at build time (never
//! loaded from the filesystem or hot-reloaded). The private socket and the
//! CLI keep their structured JSON contracts; only the MCP presentation is
//! text. `start`/`resume` additionally keep a tiny structured
//! `{"agent_id": ..., "sequence": ...}` so the PostToolUse binding hook can
//! bind the exact execution from JSON (see `hooks::bind`); no other tool
//! mirrors its result into structured content. Expected failures use typed
//! errors; presentation degradation preserves independent execution receipts.
//! Whole pages either fit their byte/row/content budgets or publish no rows.

use crate::{Error, Result};
use minijinja::{AutoEscape, Environment, UndefinedBehavior};
use rmcp::model::{CallToolResult, ContentBlock};
use serde_json::{Value, json};
use std::sync::OnceLock;

/// The embedded per-tool templates, keyed by public tool name.
const TEMPLATES: &[(&str, &str)] = &[
    (
        "notify_orchestrator",
        include_str!("../../../../assets/mcp/notify_orchestrator.txt.j2"),
    ),
    ("start", include_str!("../../../../assets/mcp/start.txt.j2")),
    (
        "resume",
        include_str!("../../../../assets/mcp/resume.txt.j2"),
    ),
    (
        "cancel",
        include_str!("../../../../assets/mcp/cancel.txt.j2"),
    ),
    ("steer", include_str!("../../../../assets/mcp/steer.txt.j2")),
    (
        "list_agents",
        include_str!("../../../../assets/mcp/list_agents.txt.j2"),
    ),
    (
        "transcript",
        include_str!("../../../../assets/mcp/transcript.txt.j2"),
    ),
    (
        "answer",
        include_str!("../../../../assets/mcp/answer.txt.j2"),
    ),
    (
        "models",
        include_str!("../../../../assets/mcp/models.txt.j2"),
    ),
    (
        "capacity_order",
        include_str!("../../../../assets/mcp/capacity_order.txt.j2"),
    ),
    (
        "limits",
        include_str!("../../../../assets/mcp/limits.txt.j2"),
    ),
    ("doc", include_str!("../../../../assets/mcp/doc.txt.j2")),
];

/// The shared embedded error template.
const ERROR_TEMPLATE: &str = include_str!("../../../../assets/mcp/error.txt.j2");

/// Returns the immutable closed environment, caching registration failures as
/// safe startup errors instead of panicking or admitting work before validation.
fn environment() -> std::result::Result<&'static Environment<'static>, &'static str> {
    /// Immutable parsed templates or their fixed registration failure.
    static ENVIRONMENT: OnceLock<std::result::Result<Environment<'static>, &'static str>> =
        OnceLock::new();
    ENVIRONMENT
        .get_or_init(|| {
            let mut environment = Environment::empty();
            environment.set_trim_blocks(true);
            environment.set_lstrip_blocks(true);
            environment.set_keep_trailing_newline(true);
            environment.set_undefined_behavior(UndefinedBehavior::Strict);
            environment.set_auto_escape_callback(|_| AutoEscape::None);
            environment.set_recursion_limit(16);
            environment.set_fuel(Some(50_000));
            // Pure constant-time optional-field predicate; no implicit globals/filters.
            environment.add_test("none", |value: minijinja::Value| value.is_none());
            for (name, source) in TEMPLATES.iter().copied().chain([("error", ERROR_TEMPLATE)]) {
                environment
                    .add_template(name, source)
                    .map_err(|_| "presentation_registration_failed")?;
            }
            Ok(environment)
        })
        .as_ref()
        .map_err(|error| *error)
}

/// Validates all embedded templates before MCP can forward any mutation.
/// Returns a fixed startup error without template source or caller context.
pub fn initialize() -> Result<()> {
    environment()
        .map(|_| ())
        .map_err(|_| Error::Runtime("MCP presentation initialization failed".into()))
}

/// Captures semantic execution independently of templates. Identities are exact,
/// bounded references from the broker acknowledgement, never reconstructed text.
struct ExecutionReceipt {
    /// Whether this successful broker result confirmed a durable mutation.
    accepted: bool,
    /// Stable agent or notification identity, absent only for reads/bad receipts.
    identity: Option<String>,
    /// Caller retry identity, kept outside the presentation context.
    request_id: Option<String>,
    /// Exact admission counter for the PostToolUse binding mirror.
    sequence: Option<u32>,
    /// Exact queued steering command, retained on formatting failure.
    command_id: Option<i64>,
}

impl ExecutionReceipt {
    /// Captures acknowledged effects before any projection/rendering. Only a
    /// successful mutation with a valid identity is confirmed as accepted.
    fn capture(tool: &str, value: &Value, request_id: Option<&str>) -> Self {
        let mutating = matches!(
            tool,
            "start" | "resume" | "cancel" | "steer" | "notify_orchestrator"
        );
        let identity = value[if tool == "notify_orchestrator" {
            "notification_id"
        } else {
            "agent_id"
        }]
        .as_str()
        .filter(|id| reference(id))
        .map(str::to_owned);
        let sequence = value
            .get("sequence")
            .or_else(|| value["agent"].get("sequence"))
            .and_then(Value::as_u64)
            .and_then(|n| u32::try_from(n).ok())
            .filter(|n| *n > 0);
        Self {
            accepted: mutating && identity.is_some(),
            identity,
            sequence,
            command_id: value["command_id"].as_i64().filter(|n| *n > 0),
            request_id: request_id
                .filter(|id| !id.trim().is_empty() && id.chars().count() <= 512)
                .map(exact_display),
        }
    }

    /// Discards partial presentation and preserves confirmed effects/identity.
    /// Reads fail explicitly; malformed mutation receipts remain unknown.
    fn fallback(&self, tool: &str) -> CallToolResult {
        let mut text = if self.accepted {
            format!(
                "ACCEPTED {tool} {}\nPresentation: degraded (presentation_failed).\nDo not repeat the mutation to repair this response.\n",
                self.identity.as_deref().unwrap_or("")
            )
        } else if matches!(
            tool,
            "start" | "resume" | "cancel" | "steer" | "notify_orchestrator"
        ) {
            "OUTCOME_UNKNOWN\nPresentation: degraded (presentation_failed).\nReconcile the original request before any retry; do not create replacement work.\n".into()
        } else {
            "agent-run error RuntimeError: presentation_failed; no page was published. Request a smaller list/transcript limit or narrower model/provider/profile filter; doc topics and answer artifact paths remain available through CLI/API.\n".into()
        };
        if !self.accepted
            && let Some(id) = &self.identity
        {
            text.push_str(&format!("Target agent: {id}\n"));
        }
        if let Some(command) = self.command_id {
            text.push_str(&format!("Command: {command}\n"));
        }
        if let Some(request) = &self.request_id {
            text.push_str(&format!("Request: {request}\n"));
        }
        let mut result = CallToolResult::success(vec![ContentBlock::text(text)]);
        result.is_error = Some(!self.accepted);
        result
    }
}

/// One frozen MCP selection; names stay exact, while count/all preserves caps.
#[derive(serde::Serialize, serde::Deserialize)]
struct McpRow {
    /// Exact catalog server name.
    name: String,
    /// Declared global/profile/both selection source.
    source: String,
    /// Exact tool count or the explicit all marker.
    tools: String,
}

/// Allowlisted status facts with no provider/account or private execution graph.
#[derive(serde::Serialize, serde::Deserialize)]
struct AgentTextView {
    /// Exact actionable stable identity.
    id: String,
    /// Validated lifecycle enum; unknown values are errors.
    status: crate::domain::Status,
    /// Whether status is already terminal.
    terminal: bool,
    /// Exact route runtime.
    runtime: String,
    /// Exact model reference.
    model: String,
    /// Exact profile reference.
    profile: String,
    /// Optional preparation/execution phase.
    phase: String,
    /// Explicit effort, empty when unspecified.
    effort: String,
    /// Bounded selected integrations without their arguments or credentials.
    mcp: Vec<McpRow>,
    /// Count of additional selections omitted explicitly.
    mcp_more: usize,
    /// Display-safe task label.
    task: String,
    /// Optional fixed failure classification.
    failure_kind: String,
    /// Display-safe failure explanation.
    failure_text: String,
    /// Binding status; missing is unknown rather than false.
    bound: Option<bool>,
    /// Durable state or explicit unknown.
    delivery_state: String,
    /// Display-safe secret-safe diagnostic.
    delivery_error: String,
    /// Whether delivery acknowledgement is ambiguous.
    ambiguous: bool,
    /// Whether a watchdog warning is recorded.
    warned: bool,
    /// Whether stored cleanup remains unconfirmed.
    cleanup_unconfirmed: bool,
    /// Optional process evidence classification.
    process_state: String,
}

/// Admission view adds only committed admission facts to allowlisted status.
#[derive(serde::Serialize, serde::Deserialize)]
struct AdmissionTextView {
    /// Exact stable admission identity.
    agent_id: String,
    /// Created versus request-key replay; never inferred from truthiness.
    created: bool,
    /// Public status projection.
    #[serde(flatten)]
    agent: AgentTextView,
}

/// Cancellation acknowledgement carries the agent state independent of rendering.
#[derive(serde::Serialize, serde::Deserialize)]
struct CancelTextView {
    /// Agent status at command admission.
    agent: AgentTextView,
}

/// One durable steering acknowledgement, with exact command identity.
#[derive(serde::Serialize, serde::Deserialize)]
struct CommandTextView {
    /// Stable target.
    agent_id: String,
    /// Durable command number.
    command_id: i64,
    /// Validated command kind.
    kind: String,
    /// Validated durable queue state.
    state: String,
}

/// Whole offset page; every incoming row is rendered or the entire page refused.
#[derive(serde::Serialize, serde::Deserialize)]
struct AgentPageTextView {
    /// Exact matching total.
    total: u64,
    /// Number of all displayed rows.
    returned: usize,
    /// Exact source offset.
    offset: u64,
    /// Actual source continuation, never emitted for a truncated page.
    next_offset: Option<u64>,
    /// Explicit completeness.
    complete: bool,
    /// Ordered public status rows.
    agents: Vec<AgentTextView>,
}

/// Exact message content and sequence inside a labelled transcript page.
#[derive(serde::Serialize, serde::Deserialize)]
struct MessageTextView {
    /// Exact journal sequence.
    seq: i64,
    /// Validated role label.
    role: String,
    /// Optional display-safe tool name.
    name: Option<String>,
    /// Faithful excerpt content; never label-normalized.
    content: String,
}

/// Whole transcript page with exact retrieval metadata.
#[derive(serde::Serialize, serde::Deserialize)]
struct TranscriptTextView {
    /// Stable transcript identity.
    agent_id: String,
    /// Source continuation, required for incomplete pages.
    next_cursor: Option<i64>,
    /// Explicit source completeness.
    complete: bool,
    /// Every incoming message in order.
    messages: Vec<MessageTextView>,
    /// Displayed message count.
    count: usize,
}

/// Verified answer, preserving absence, exact content and artifact recovery.
#[derive(serde::Serialize, serde::Deserialize)]
struct AnswerTextView {
    /// Exact stable identity.
    agent_id: String,
    /// Validated lifecycle, independent of artifact availability.
    status: crate::domain::Status,
    /// Whether a sealed verified answer exists.
    available: bool,
    /// Whether inline bytes contain the entire answer.
    inline_complete: bool,
    /// Faithful optional content, including empty content.
    content: Option<String>,
    /// Exact reversible artifact location.
    path: Option<String>,
    /// Exact owned relative artifact path.
    relative_path: Option<String>,
    /// Verified byte size; zero remains zero.
    size_bytes: Option<u64>,
    /// Optional verified hash.
    sha256: Option<String>,
    /// Artifact format.
    kind: Option<String>,
    /// Optional media type.
    media_type: Option<String>,
}

/// Exact trusted guide/document text through the same bounded embedded template.
#[derive(serde::Serialize)]
struct ExcerptTextView<'a> {
    /// Faithful requested text.
    text: &'a str,
}

/// Safe worker queue acknowledgement with no capability or message content.
#[derive(serde::Serialize, serde::Deserialize)]
struct ReportTextView {
    /// Exact queue delivery identity.
    notification_id: String,
    /// Validated queue lifecycle.
    state: String,
    /// Whether the request replay reused an existing report.
    duplicate: bool,
}

/// Routine expected error carries only public sanitized diagnostics.
#[derive(serde::Serialize)]
struct ErrorTextView {
    /// Fixed validated public class.
    kind: String,
    /// Quoted bounded public message.
    message: String,
}

/// Per-tool response policy, separate from discovery input schemas.
#[derive(Clone, Copy)]
struct Budget {
    /// Maximum complete rendered UTF-8 bytes.
    bytes: usize,
    /// Maximum page rows, rejected as a whole above this bound.
    rows: usize,
    /// Maximum faithful excerpt bytes before layout.
    content: usize,
}

/// Maps a closed tool set to justified byte/row/content limits.
fn budget(tool: &str) -> Budget {
    match tool {
        "start" | "resume" => Budget {
            bytes: 4096,
            rows: 8,
            content: 0,
        },
        "cancel" | "steer" | "notify_orchestrator" | "error" => Budget {
            bytes: 2048,
            rows: 8,
            content: 0,
        },
        "list_agents" | "limits" => Budget {
            bytes: 8192,
            rows: 20,
            content: 0,
        },
        "transcript" => Budget {
            bytes: 16384,
            rows: 100,
            content: 14336,
        },
        "answer" | "doc" | "delegation_guide" => Budget {
            bytes: 16384,
            rows: 0,
            content: 14336,
        },
        "models" | "capacity_order" => Budget {
            bytes: 16384,
            rows: 100,
            content: 0,
        },
        _ => Budget {
            bytes: 2048,
            rows: 0,
            content: 0,
        },
    }
}

/// A private output buffer published only when the complete render succeeds.
struct BoundedWriter {
    /// Unpublished output bytes.
    bytes: Vec<u8>,
    /// Complete text limit.
    limit: usize,
}

impl std::io::Write for BoundedWriter {
    /// Appends the entire chunk or refuses it without partial publication.
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        if bytes.len() > self.limit.saturating_sub(self.bytes.len()) {
            return Err(std::io::Error::other("presentation_budget_exceeded"));
        }
        self.bytes.extend_from_slice(bytes);
        Ok(bytes.len())
    }
    /// No I/O exists behind this private memory buffer.
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

/// Renders only an owned typed view, dropping every partial buffer on failure.
fn render_view<T: serde::Serialize>(name: &str, view: &T, limit: usize) -> Result<String> {
    let mut writer = BoundedWriter {
        bytes: Vec::new(),
        limit,
    };
    environment()
        .map_err(|_| Error::Runtime("presentation_registration_failed".into()))?
        .get_template(name)
        .map_err(|_| Error::Runtime("presentation_template_missing".into()))?
        .render_captured_to(view, &mut writer)
        .map_err(|_| Error::Runtime("presentation_failed".into()))?;
    String::from_utf8(writer.bytes).map_err(|_| Error::Runtime("presentation_failed".into()))
}

/// Rejects malformed projected dynamic broker data before it reaches a template.
fn projected<T: serde::de::DeserializeOwned>(value: Value) -> Result<T> {
    serde_json::from_value(value).map_err(|_| Error::Runtime("presentation_shape_invalid".into()))
}

/// Builds a reply from successful broker JSON at the real transport boundary.
/// Receipt capture precedes all projection and formatting; writes never replay.
#[must_use]
pub fn success_result(tool: &str, value: &Value) -> CallToolResult {
    success_result_with_request(tool, value, None)
}

/// As success_result, retaining the caller's retry key independently of the view.
#[must_use]
pub fn success_result_with_request(
    tool: &str,
    value: &Value,
    request_id: Option<&str>,
) -> CallToolResult {
    let receipt = ExecutionReceipt::capture(tool, value, request_id);
    let request_line = receipt
        .request_id
        .as_ref()
        .map(|id| format!("Request: {id}\n"))
        .unwrap_or_default();
    let limit = budget(tool).bytes.saturating_sub(request_line.len());
    let rendered = validate_result(tool, value)
        .and_then(|()| render(tool, value, limit))
        .map(|mut text| {
            text.push_str(&request_line);
            text
        });
    let mut result = match rendered {
        Ok(text) => CallToolResult::success(vec![ContentBlock::text(text)]),
        Err(_) => receipt.fallback(tool),
    };
    if matches!(tool, "start" | "resume")
        && let (Some(id), Some(sequence)) = (&receipt.identity, receipt.sequence)
    {
        result.structured_content = Some(json!({"agent_id": id, "sequence": sequence}));
    }
    result
}

/// Preserves uncertain writes and expected business failures without deriving
/// effects from renderer output. Transport/runtime failures may follow admission;
/// resume broker-unavailable is conservative because its client retries internally.
/// The optional retry key and original target are retained as exact bounded
/// references; target identity alone never confirms that a mutation happened.
#[must_use]
pub fn failure_result(
    tool: &str,
    error: &Error,
    request_id: Option<&str>,
    target_id: Option<&str>,
) -> CallToolResult {
    let write = matches!(
        tool,
        "start" | "resume" | "cancel" | "steer" | "notify_orchestrator"
    );
    let uncertain = matches!(error, Error::Runtime(_) | Error::Io(_) | Error::Json(_))
        || (tool == "resume" && matches!(error, Error::BrokerUnavailable));
    if write && uncertain {
        let receipt = ExecutionReceipt {
            identity: target_id.filter(|id| reference(id)).map(str::to_owned),
            ..ExecutionReceipt::capture(tool, &Value::Null, request_id)
        };
        return receipt.fallback(tool);
    }
    let public = error.public();
    error_result(public.kind, &public.message)
}

/// Expected business errors remain errors even if formatting fails. No raw
/// context, Debug value, renderer source or error chain is exposed.
#[must_use]
pub fn error_result(kind: &str, message: &str) -> CallToolResult {
    let view = ErrorTextView {
        kind: if reference(kind) {
            kind.to_owned()
        } else {
            "RuntimeError".into()
        },
        message: label(message),
    };
    let text = render_view("error", &view, budget("error").bytes).unwrap_or_else(|_| {
        "agent-run error RuntimeError: presentation_failed; operation failed.\n".into()
    });
    let mut result = CallToolResult::success(vec![ContentBlock::text(text)]);
    result.is_error = Some(true);
    result
}

/// One canonical role, retaining explicit false grants and exact constraints.
#[derive(serde::Serialize, serde::Deserialize)]
struct RoleTextView {
    /// Actionable role name.
    name: String,
    /// Explicit boolean spelling; not a default grant.
    write: String,
    /// Explicit network grant spelling.
    network: String,
    /// Exact comma-separated constraint identifiers.
    constraints: Option<String>,
}

/// One public model offering, without backend/account or credential metadata.
#[derive(serde::Serialize, serde::Deserialize)]
struct ModelTextView {
    /// Exact offered model name.
    id: String,
    /// Optional native model spelling.
    native_model: Option<String>,
    /// Validated quota status, including unknown.
    status: String,
    /// Explicit observation evidence classification.
    evidence: String,
    /// Exact admissible profile list, optionally hoisted to the provider.
    profiles: Option<String>,
    /// Faithful configured parameter choices.
    params: Option<String>,
    /// Exact hard constraints.
    restrictions: Option<String>,
    /// Display-safe bounded guidance labels.
    #[serde(default)]
    guidance: Vec<String>,
}

/// One provider catalog/order row; optional numbers distinguish zero from absent.
#[derive(serde::Serialize, serde::Deserialize)]
struct ProviderTextView {
    /// Actionable public provider name.
    id: String,
    /// Optional configured harness.
    harness: Option<String>,
    /// Exact common profiles.
    profiles: Option<String>,
    /// Public score, including zero.
    score: Option<f64>,
    /// Explicit priority multiplier, including zero.
    multiplier: Option<f64>,
    /// Display-safe bounded configured guidance.
    #[serde(default)]
    guidance: Vec<String>,
    /// Every validated model in source order.
    models: Vec<ModelTextView>,
}

/// Legacy runtime roster retains explicit availability and exact model references.
#[derive(serde::Serialize, serde::Deserialize)]
struct RuntimeTextView {
    /// Actionable runtime name.
    name: String,
    /// Explicit upstream availability.
    available: bool,
    /// Optional display-safe reason.
    reason: Option<String>,
    /// Exact offered model names.
    models: Vec<String>,
}

/// Legacy route standing, independent of model ability.
#[derive(serde::Serialize, serde::Deserialize)]
struct RouteTextView {
    /// Exact route runtime.
    runtime: String,
    /// Recorded priority; zero is meaningful.
    priority: f64,
    /// Exact configured aliases, without truncation.
    aliases: Option<String>,
}

/// Explicit catalog family, preserving the schema-1/2 compatibility distinction.
#[derive(serde::Serialize, serde::Deserialize)]
struct CatalogTextView {
    /// Validated product result schema, independent of MCP protocol.
    schema: u64,
    /// Optional exact capacity revision.
    capacity_revision: Option<u64>,
    /// Schema-2 canonical role grants.
    profiles: Option<Vec<RoleTextView>>,
    /// Schema-2 ordered providers.
    providers: Option<Vec<ProviderTextView>>,
    /// Schema-1 runtime roster.
    runtimes: Option<Vec<RuntimeTextView>>,
    /// Schema-1 ordered routes.
    routes: Option<Vec<RouteTextView>>,
    /// Exact unavailable route names.
    unavailable: Option<String>,
}

/// One capacity diagnostic row; private account/pool fields never reach templates.
#[derive(serde::Serialize, serde::Deserialize)]
struct LimitTextView {
    /// Exact public runtime.
    runtime: String,
    /// Public quota lane.
    lane: String,
    /// Quota window name.
    window: String,
    /// Explicit observation validity.
    known: bool,
    /// Optional percent, preserving zero.
    remaining_percent: Option<f64>,
    /// Optional observed reset horizon, including 0s.
    resets_in: Option<String>,
}

/// Whole capacity diagnostic page, refusing oversized data without dropping rows.
#[derive(serde::Serialize, serde::Deserialize)]
struct LimitsTextView {
    /// Every source diagnostic in order.
    items: Vec<LimitTextView>,
}

/// Converts the existing dynamic catalog projection to its allowlisted typed view.
fn render_catalog(name: &str, value: Value, limit: usize) -> Result<String> {
    render_view(name, &projected::<CatalogTextView>(value)?, limit)
}

/// Fixed safe error for shape, enum or critical-fidelity rejection.
fn shape_error() -> Error {
    Error::Runtime("presentation_shape_invalid".into())
}

/// Validates an exact actionable identifier: 1–256 ASCII bytes, no controls,
/// formatting, delimiter or whitespace characters. Never normalizes/truncates it.
fn reference(text: &str) -> bool {
    !text.is_empty()
        && text.len() <= 256
        && text
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b"_.:/@+-".contains(&b))
}

/// Returns a required exact bounded reference from dynamic broker JSON.
fn required_reference<'a>(value: &'a Value, key: &str) -> Result<&'a str> {
    value[key]
        .as_str()
        .filter(|s| !s.is_empty() && s.len() <= 4096)
        .ok_or_else(shape_error)
}

/// Returns a required bounded whole array. It never slices a paginated result.
fn required_rows<'a>(value: &'a Value, key: &str, limit: usize) -> Result<&'a Vec<Value>> {
    let rows = value[key].as_array().ok_or_else(shape_error)?;
    if rows.len() > limit {
        return Err(Error::Runtime("presentation_budget_exceeded".into()));
    }
    Ok(rows)
}

/// Requires an explicit boolean rather than conflating false, null and absent.
fn required_bool(value: &Value, key: &str) -> Result<bool> {
    value[key].as_bool().ok_or_else(shape_error)
}

/// Validates lifecycle/route essentials before optional descriptive projection.
fn validate_agent(value: &Value) -> Result<()> {
    for key in ["agent_id", "runtime", "model", "profile"] {
        required_reference(value, key)?;
    }
    serde_json::from_value::<crate::domain::Status>(value["status"].clone())
        .map_err(|_| shape_error())?;
    if !value["delivery"].is_null() && !value["delivery"].is_object() {
        return Err(shape_error());
    }
    for key in ["warned"] {
        if !value[key].is_null() {
            required_bool(value, key)?;
        }
    }
    Ok(())
}

/// Checks source shapes, all page rows/cursors and faithful content before
/// allocating projected rows. Unknown states never become success or empty pages.
fn validate_result(tool: &str, value: &Value) -> Result<()> {
    let policy = budget(tool);
    match tool {
        "start" | "resume" => {
            required_reference(value, "agent_id")?;
            required_bool(value, "created")?;
            validate_agent(&value["agent"])?;
        }
        "cancel" => validate_agent(value)?,
        "steer" => {
            required_reference(value, "agent_id")?;
            if value["command_id"].as_i64().is_none_or(|n| n < 1)
                || value["kind"] != "steer"
                || value["state"] != "queued"
            {
                return Err(shape_error());
            }
        }
        "notify_orchestrator" => {
            required_reference(value, "notification_id")?;
            required_bool(value, "duplicate")?;
            if !matches!(
                value["state"].as_str(),
                Some("pending" | "sending" | "delivered" | "failed" | "cancelled")
            ) {
                return Err(shape_error());
            }
        }
        "list_agents" => {
            let rows = required_rows(value, "items", policy.rows)?;
            for row in rows {
                validate_agent(row)?;
            }
            let total = value["total"].as_u64().ok_or_else(shape_error)?;
            let offset = value["offset"].as_u64().ok_or_else(shape_error)?;
            let end = offset
                .checked_add(rows.len() as u64)
                .ok_or_else(shape_error)?;
            if !rows.is_empty() && end > total {
                return Err(shape_error());
            }
            let complete = required_bool(value, "complete")?;
            if complete != (end >= total) {
                return Err(shape_error());
            }
            if !complete && value["next_offset"].as_u64() != Some(end) {
                return Err(shape_error());
            }
            if complete && !value["next_offset"].is_null() {
                return Err(shape_error());
            }
        }
        "transcript" => {
            required_reference(value, "agent_id")?;
            let rows = required_rows(value, "messages", policy.rows)?;
            let complete = required_bool(value, "complete")?;
            if !complete && value["next_cursor"].as_i64().is_none_or(|n| n < 0) {
                return Err(shape_error());
            }
            if complete && !value["next_cursor"].is_null() {
                return Err(shape_error());
            }
            let mut content = 0usize;
            let mut previous = -1;
            for row in rows {
                let seq = row["seq"].as_i64().ok_or_else(shape_error)?;
                if seq <= previous {
                    return Err(shape_error());
                }
                previous = seq;
                required_reference(row, "role")?;
                content =
                    content.saturating_add(row["content"].as_str().ok_or_else(shape_error)?.len());
            }
            if content > policy.content {
                return Err(Error::Runtime("presentation_budget_exceeded".into()));
            }
        }
        "answer" => {
            required_reference(value, "agent_id")?;
            serde_json::from_value::<crate::domain::Status>(value["status"].clone())
                .map_err(|_| shape_error())?;
            let available = required_bool(value, "available")?;
            let complete = required_bool(value, "inline_complete")?;
            if complete && (!available || !value["content"].is_string()) {
                return Err(shape_error());
            }
            if available && !complete && !value["path"].is_string() {
                return Err(shape_error());
            }
            if value["content"]
                .as_str()
                .is_some_and(|s| s.len() > policy.content)
            {
                return Err(Error::Runtime("presentation_budget_exceeded".into()));
            }
        }
        "doc" | "delegation_guide" => {
            let text = if tool == "doc" {
                value["text"].as_str()
            } else {
                value.as_str()
            }
            .ok_or_else(shape_error)?;
            if text.len() > policy.content {
                return Err(Error::Runtime("presentation_budget_exceeded".into()));
            }
        }
        "models" | "capacity_order" if value.get("providers").is_some() => {
            if value.get("schema_version").is_some_and(|s| s != 2) {
                return Err(shape_error());
            }
            if !value["capacity_revision"].is_u64() {
                return Err(shape_error());
            }
            let mut count = 0usize;
            for provider in required_rows(value, "providers", policy.rows)? {
                required_reference(provider, "provider")?;
                for model in required_rows(provider, "models", policy.rows)? {
                    count += 1;
                    required_reference(model, "model")?;
                    if tool == "models" {
                        for key in ["profiles", "restrictions"] {
                            for entry in required_rows(model, key, policy.rows)? {
                                if !entry.is_string() {
                                    return Err(shape_error());
                                }
                            }
                        }
                        for key in ["params", "allowed_params"] {
                            let entries = model[key].as_object().ok_or_else(shape_error)?;
                            if entries.len() > 20 {
                                return Err(shape_error());
                            }
                            for (name, parameter) in
                                entries.iter().filter(|(name, _)| name.as_str() == "effort")
                            {
                                if !reference(name) {
                                    return Err(shape_error());
                                }
                                let choices = if key == "allowed_params" {
                                    parameter.as_array().ok_or_else(shape_error)?.as_slice()
                                } else {
                                    std::slice::from_ref(parameter)
                                };
                                if choices.len() > 20
                                    || choices.iter().any(|v| {
                                        !matches!(
                                            v,
                                            Value::String(_) | Value::Bool(_) | Value::Number(_)
                                        )
                                    })
                                {
                                    return Err(shape_error());
                                }
                            }
                        }
                    }
                    if !matches!(
                        model["quota"]["status"].as_str(),
                        Some(
                            "available"
                                | "unknown"
                                | "priority_overflow"
                                | "exhausted"
                                | "no_eligible_account"
                        )
                    ) || !matches!(
                        model["quota"]["evidence"].as_str(),
                        Some("fresh" | "stale" | "missing")
                    ) {
                        return Err(shape_error());
                    }
                }
            }
            if count > policy.rows {
                return Err(Error::Runtime("presentation_budget_exceeded".into()));
            }
            if tool == "models" {
                for role in required_rows(value, "profiles", policy.rows)? {
                    required_reference(role, "name")?;
                    required_bool(role, "write")?;
                    required_bool(role, "network")?;
                }
            }
        }
        "models" => {
            if value.get("schema_version").is_some() || !value.is_object() {
                return Err(shape_error());
            }
            for (name, runtime) in value.as_object().ok_or_else(shape_error)? {
                if !reference(name) {
                    return Err(shape_error());
                }
                required_bool(runtime, "available")?;
                for model in required_rows(runtime, "models", policy.rows)? {
                    required_reference(model, "id")?;
                }
            }
        }
        "capacity_order" => {
            if value.get("schema_version").is_some_and(|s| s != 1) {
                return Err(shape_error());
            }
            required_rows(value, "routes", policy.rows)?;
            required_rows(value, "unavailable_runtimes", policy.rows)?;
        }
        "limits" => {
            for item in required_rows(value, "items", policy.rows)? {
                let known = required_bool(item, "known")?;
                for key in ["runtime", "lane", "window"] {
                    required_reference(&item["key"], key)?;
                }
                if known && !item["remaining_percent"].is_number() {
                    return Err(shape_error());
                }
            }
        }
        _ => return Err(shape_error()),
    }
    Ok(())
}

/// Quotes a bounded friendly label. Unicode/control/bidi/ANSI formatting is
/// visible, literal Jinja stays data, and shortening is explicitly marked.
fn label(text: &str) -> String {
    let mut visible = String::new();
    for (index, ch) in text.chars().enumerate() {
        if index == 160 {
            visible.push_str("…[shortened]");
            break;
        }
        if ch.is_control()
            || matches!(ch, '\u{2028}' | '\u{2029}' | '\u{061c}' | '\u{200e}' | '\u{200f}' | '\u{202a}'..='\u{202e}' | '\u{2066}'..='\u{2069}')
        {
            visible.push_str(&format!("\\u{{{:04x}}}", ch as u32));
        } else {
            visible.push(ch);
        }
    }
    // serde JSON string encoding is reversible quoting, never a raw context dump.
    serde_json::to_string(&visible).unwrap_or_else(|_| "\"\"".into())
}

/// Chooses a closed template and explicit allowlisted response-family view.
/// JSON is confined to the dynamic broker adapter/projection boundary.
fn render(name: &str, value: &Value, limit: usize) -> Result<String> {
    match name {
        "start" | "resume" => render_view(
            name,
            &projected::<AdmissionTextView>(start_context(value))?,
            limit,
        ),
        "limits" => render_view(
            name,
            &projected::<LimitsTextView>(limits_context(value))?,
            limit,
        ),
        "cancel" => render_view(
            name,
            &projected::<CancelTextView>(json!({"agent": agent_fields(value)}))?,
            limit,
        ),
        "steer" => render_view(
            name,
            &projected::<CommandTextView>(json!({
                "agent_id": value["agent_id"], "command_id": value["command_id"], "kind": value["kind"], "state": value["state"]
            }))?,
            limit,
        ),
        "list_agents" => render_view(
            name,
            &projected::<AgentPageTextView>(list_context(value))?,
            limit,
        ),
        "transcript" => render_view(
            name,
            &projected::<TranscriptTextView>(transcript_context(value))?,
            limit,
        ),
        "answer" => render_view(
            name,
            &projected::<AnswerTextView>(answer_context(value))?,
            limit,
        ),
        "notify_orchestrator" => render_view(
            name,
            &projected::<ReportTextView>(json!({
                "notification_id": value["notification_id"], "state": value["state"], "duplicate": value["duplicate"]
            }))?,
            limit,
        ),
        "doc" => render_view(
            name,
            &ExcerptTextView {
                text: value["text"].as_str().ok_or_else(shape_error)?,
            },
            limit,
        ),
        "delegation_guide" => render_view(
            "doc",
            &ExcerptTextView {
                text: value.as_str().ok_or_else(shape_error)?,
            },
            limit,
        ),
        "models" => render_catalog(name, models_context(value), limit),
        "capacity_order" => render_catalog(name, order_context(value), limit),
        _ => Err(shape_error()),
    }
}

/// Extracts the compact shared agent facts one template prints per agent.
///
/// Required lifecycle facts are validated before projection. Optional friendly
/// labels may be empty; actionable references use reversible quoting and binding
/// absence stays unknown. Private provider/account/capability fields are omitted.
fn agent_fields(view: &Value) -> Value {
    let text = |key: &str| view[key].as_str().map(exact_display).unwrap_or_default();
    let status = text("status");
    json!({
        "id": view["agent_id"], "status": status,
        "terminal": matches!(status.as_str(),
            "succeeded" | "failed" | "lost" | "timed_out" | "cancelled"),
        "runtime": view["runtime"].as_str().map(exact_display), "model": view["model"].as_str().map(exact_display), "profile": view["profile"].as_str().map(exact_display),
        "phase": text("phase"), "effort": text("effort"),
        "mcp": view["mcp"].as_array().into_iter().flatten().take(8).map(|server| json!({
            "name": server["name"].as_str().map(exact_display), "source": server["source"].as_str().map(exact_display),
            "tools": server["allowed_tools"].as_array().map(|tools| tools.len().to_string()).unwrap_or_else(|| "all".into()),
        })).collect::<Vec<_>>(),
        "mcp_more": view["mcp"].as_array().map_or(0, |servers| servers.len().saturating_sub(8)),
        "task": view["task_summary"].as_str().filter(|s| !s.is_empty()).map(label).unwrap_or_default(),
        "failure_kind": text("failure_kind"),
        "failure_text": view["failure_text"].as_str().filter(|s| !s.is_empty()).map(label).unwrap_or_default(),
        "bound": view["delivery"]["bound"].as_bool(),
        "delivery_state": view["delivery"]["state"].as_str().unwrap_or("unknown"),
        "delivery_error": view["delivery"]["last_error"]
            .as_str()
            .filter(|s| !s.is_empty()).map(label).unwrap_or_default(),
        "ambiguous": view["delivery"]["ambiguous"].as_bool().unwrap_or(false),
        "warned": view["warned"].as_bool().unwrap_or(false),
        "cleanup_unconfirmed": view["cleanup"]
            .as_object()
            .is_some_and(|cleanup| !cleanup["confirmed"].as_bool().unwrap_or(false)),
        "process_state": view["process_state"].as_str().map(exact_display).unwrap_or_default(),
    })
}

/// Start/resume context: the durable id plus the just-committed snapshot.
fn start_context(value: &Value) -> Value {
    let mut fields = agent_fields(&value["agent"]);
    fields["agent_id"] = value["agent_id"].clone();
    fields["created"] = value["created"].clone();
    fields
}

/// List context: exact total, returned page, and continuation facts.
fn list_context(value: &Value) -> Value {
    json!({
        "total": value["total"], "returned": value["items"].as_array().map(Vec::len),
        "offset": value["offset"],
        "next_offset": value["next_offset"].as_u64(),
        "complete": value["complete"],
        "agents": value["items"].as_array().into_iter().flatten()
            .map(agent_fields).collect::<Vec<_>>(),
    })
}

/// Transcript context: every requested message with role/name/sequence.
fn transcript_context(value: &Value) -> Value {
    json!({
        "agent_id": value["agent_id"],
        "next_cursor": value["next_cursor"].as_i64(),
        "complete": value["complete"],
        "messages": value["messages"].as_array().into_iter().flatten().map(|message| {
            json!({
                "seq": message["seq"], "role": message["role"],
                "name": message["name"].as_str().map(label), "content": message["content"],
            })
        }).collect::<Vec<_>>(),
        "count": value["messages"].as_array().map(Vec::len),
    })
}

/// Answer context: availability plus the full inline text or retrieval facts.
fn answer_context(value: &Value) -> Value {
    json!({
        "agent_id": value["agent_id"], "status": value["status"],
        "available": value["available"],
        "inline_complete": value["inline_complete"],
        "content": value["content"].as_str(),
        "path": value["path"].as_str().map(exact_display),
        "sha256": value["sha256"].as_str(),
        "relative_path": value["relative_path"].as_str().map(exact_display),
        "size_bytes": value["size_bytes"].as_u64(),
        "kind": value["kind"].as_str(), "media_type": value["media_type"].as_str(),
    })
}

/// Models context: schema-2 provider catalog or the legacy runtime roster.
fn models_context(value: &Value) -> Value {
    if value["providers"].as_array().is_some() {
        json!({
            "schema": 2,
            "capacity_revision": value["capacity_revision"],
            "profiles": value["profiles"].as_array().into_iter().flatten()
                .map(|role| json!({
                    "name": role["name"].as_str().map(exact_display),
                    "write": role["write"].as_bool().unwrap_or(false).to_string(),
                    "network": role["network"].as_bool().unwrap_or(false).to_string(),
                    "constraints": nonempty(&role["required_constraints"].as_array()
                        .into_iter().flatten().filter_map(Value::as_str)
                        .collect::<Vec<_>>().join(", ")),
                })).collect::<Vec<_>>(),
            "providers": providers_context(value),
        })
    } else {
        json!({
            "schema": 1,
            "runtimes": value.as_object().into_iter().flatten().map(|(name, runtime)| {
                json!({
                    "name": exact_display(name), "available": runtime["available"],
                    "reason": runtime["reason"].as_str().map(label),
                    "models": runtime["models"].as_array().into_iter().flatten()
                        .map(|model| model["id"].as_str().map(exact_display)).collect::<Vec<_>>(),
                })
            }).collect::<Vec<_>>(),
        })
    }
}

/// Provider/model projection shared by the `models` and guide layouts.
fn providers_context(value: &Value) -> Vec<Value> {
    value["providers"]
        .as_array()
        .into_iter()
        .flatten()
        .map(|provider| {
            let models: Vec<Value> = provider["models"]
                .as_array()
                .into_iter()
                .flatten()
                .map(|model| {
                    let profiles = model["profiles"]
                        .as_array()
                        .into_iter()
                        .flatten()
                        .filter_map(Value::as_str).map(exact_display)
                        .collect::<Vec<_>>()
                        .join(", ");
                    json!({
                        "id": model["model"].as_str().map(exact_display),
                        "native_model": model["native_model"].as_str().map(exact_display),
                        "status": model["quota"]["status"],
                        "evidence": model["quota"]["evidence"],
                        "guidance": model["recommendations"]
                            .as_array().into_iter().flatten()
                            .filter_map(Value::as_str).map(prose).collect::<Vec<_>>(),
                        "profiles": if profiles.is_empty() { "none".to_owned() } else { profiles },
                        "params": param_line(&model["params"], &model["allowed_params"]),
                        "restrictions": nonempty(&model["restrictions"]
                            .as_array().into_iter().flatten()
                            .filter_map(Value::as_str).map(exact_display).collect::<Vec<_>>().join(", ")),
                    })
                })
                .collect();
            // When every model admits the same nonempty profile set, state it
            // once at the provider instead of repeating the list per model.
            let common = models
                .iter()
                .map(|model| model["profiles"].as_str().unwrap_or("none"))
                .reduce(|left, right| if left == right { left } else { "" })
                .filter(|common| !common.is_empty() && *common != "none")
                .map(str::to_owned);
            let mut provider = json!({
                "id": provider["provider"].as_str().map(exact_display), "harness": provider["harness"].as_str().map(exact_display),
                "guidance": provider["recommendations"]
                    .as_array().into_iter().flatten()
                    .filter_map(Value::as_str).map(prose).collect::<Vec<_>>(),
                "models": models,
            });
            // Null (not removal) so strict-undefined templates still see a
            // defined, falsy key after hoisting.
            provider["profiles"] = common
                .as_deref()
                .map_or(Value::Null, |common| json!(common));
            if common.is_some() {
                for model in provider["models"].as_array_mut().into_iter().flatten() {
                    model["profiles"] = Value::Null;
                }
            }
            provider
        })
        .collect()
}

/// Capacity-order context: schema-2 provider order or the legacy routes.
fn order_context(value: &Value) -> Value {
    if value["providers"].as_array().is_some() {
        json!({
            "schema": 2,
            "capacity_revision": value["capacity_revision"],
            "providers": value["providers"].as_array().into_iter().flatten().map(|provider| {
                json!({
                    "id": provider["provider"].as_str().map(exact_display),
                    "score": provider["score"].as_f64(),
                    "multiplier": provider["priority_multiplier"].as_f64(),
                    "models": provider["models"].as_array().into_iter().flatten().map(|model| {
                        json!({
                            "id": model["model"].as_str().map(exact_display), "status": model["quota"]["status"],
                            "evidence": model["quota"]["evidence"],
                        })
                    }).collect::<Vec<_>>(),
                })
            }).collect::<Vec<_>>(),
        })
    } else {
        json!({
            "schema": 1,
            "routes": value["routes"].as_array().into_iter().flatten().map(|route| {
                json!({
                    "runtime": route["runtime"].as_str().map(exact_display), "priority": route["priority"],
                    "aliases": nonempty(&route["aliases"].as_array().into_iter().flatten()
                        .filter_map(Value::as_str).map(exact_display).collect::<Vec<_>>().join(", ")),
                })
            }).collect::<Vec<_>>(),
            "unavailable": nonempty(&value["unavailable_runtimes"].as_array()
                .into_iter().flatten().filter_map(Value::as_str)
                .collect::<Vec<_>>().join(", ")),
        })
    }
}

/// Limits context: the diagnostic rows this API already exposes publicly.
///
/// Reset horizons are derived against the read's own `observed_at` clock as
/// readable spans, so no raw fractional epoch reaches the text.
fn limits_context(value: &Value) -> Value {
    let observed_at = value["observed_at"].as_f64();
    json!({
        "items": value["items"].as_array().into_iter().flatten().map(|item| {
            let key = &item["key"];
            json!({
                "runtime": key["runtime"].as_str().map(exact_display), "lane": key["lane"].as_str().map(exact_display), "window": key["window"].as_str().map(exact_display),
                "known": item["known"],
                "remaining_percent": item["remaining_percent"].as_f64(),
                "resets_in": observed_at
                    .zip(item["reset_at"].as_f64())
                    .map(|(observed_at, reset_at)| span(reset_at - observed_at)),
            })
        }).collect::<Vec<_>>(),
    })
}

/// Renders one finite non-negative duration in compact human units.
///
/// Sub-second spans round up to `1s`; anything from seconds upward keeps
/// its largest whole unit (`45s`, `2m`, `2h`, `3d`), and longer spans stay
/// in days. A non-finite or negative input renders as `0s` rather than a
/// misleading clock reading.
fn span(seconds: f64) -> String {
    if !seconds.is_finite() || seconds <= 0.0 {
        return "0s".to_owned();
    }
    let whole = [(86_400.0, "d"), (3_600.0, "h"), (60.0, "m"), (1.0, "s")];
    for (unit, suffix) in whole {
        if seconds >= unit {
            return format!("{}{suffix}", (seconds / unit).ceil());
        }
    }
    "1s".to_owned()
}

/// Renders the public loader's sole admitted parameter, effort, faithfully.
/// Other metadata keys are discarded before templates, including credentials;
/// defaults and allowed values remain exact scalar data rather than labels.
fn param_line(defaults: &Value, allowed: &Value) -> Option<String> {
    let mut parts = Vec::new();
    for (name, value) in defaults
        .as_object()
        .into_iter()
        .flatten()
        .filter(|(name, _)| name.as_str() == "effort")
    {
        // Render the scalar itself, not its JSON encoding.
        let scalar = match value {
            Value::String(text) => exact_display(text),
            other => other.to_string(),
        };
        parts.push(format!("{}={scalar}", exact_display(name)));
    }
    for (name, values) in allowed
        .as_object()
        .into_iter()
        .flatten()
        .filter(|(name, _)| name.as_str() == "effort")
    {
        let choices = values
            .as_array()
            .into_iter()
            .flatten()
            .map(|value| match value {
                Value::String(text) => exact_display(text),
                Value::Bool(_) | Value::Number(_) => value.to_string(),
                _ => String::new(),
            })
            .collect::<Vec<_>>()
            .join("|");
        parts.push(if choices.is_empty() {
            format!("allowed {name}")
        } else {
            format!("allowed {name}: {choices}")
        });
    }
    nonempty(&parts.join("; "))
}

/// Returns `None` for an empty string so templates can omit empty lines.
fn nonempty(text: &str) -> Option<String> {
    (!text.is_empty()).then(|| text.to_owned())
}

/// Formats only friendly guidance labels with quoting and visible shortening.
fn prose(text: &str) -> String {
    label(text)
}

/// Displays an actionable path/value without shortening or invisible formatting.
/// JSON quoting plus a single pass over formatting characters is reversible,
/// linear in input bytes and preserves exact data when decoded.
fn exact_display(text: &str) -> String {
    if reference(text) {
        return text.to_owned();
    }
    let quoted = serde_json::to_string(text).unwrap_or_else(|_| "\"\"".into());
    let mut visible = String::with_capacity(quoted.len());
    for ch in quoted.chars() {
        if matches!(ch, '\u{2028}' | '\u{2029}' | '\u{061c}' | '\u{200e}' | '\u{200f}' | '\u{202a}'..='\u{202e}' | '\u{2066}'..='\u{2069}')
        {
            visible.push_str(&format!("\\u{:04x}", ch as u32));
        } else {
            visible.push(ch);
        }
    }
    visible
}

#[cfg(test)]
mod tests {
    use super::{error_result, success_result};
    use serde_json::{Value, json};

    /// Minimal agent view proving sparse optional metadata remains renderable.
    fn agent_view() -> Value {
        json!({
            "agent_id": "ag-1", "runtime": "glm-user", "model": "glm-5.3",
            "profile": "review", "status": "running", "phase": "running",
            "effort": "high", "task_summary": "fix the parser",
            "sequence": 1,
            "delivery": {"bound": false},
        })
    }

    /// Renders one tool success and returns its single text content.
    fn text(tool: &str, value: &Value) -> String {
        let result = success_result(tool, value);
        let content = serde_json::to_value(&result.content).unwrap();
        let page = content[0]["text"].as_str().unwrap().to_owned();
        assert_eq!(result.is_error, Some(false), "{tool}: {page}");
        page
    }

    /// Start and resume retain one stable ID and a machine receipt counter.
    #[test]
    fn start_and_resume_keep_binding_identity_and_honest_status() {
        for tool in ["start", "resume"] {
            let run = if tool == "resume" { "ag-2" } else { "ag-1" };
            let value = json!({
                "agent_id": "ag-1", "run_id": run, "sequence": 2, "created": true, "attempt_id": "att_9",
                "agent": agent_view(),
            });
            let result = success_result(tool, &value);
            assert_eq!(
                result.structured_content,
                Some(json!({"agent_id": "ag-1", "sequence": 2})),
                "tiny identity mirror only"
            );
            let page = text(tool, &value);
            assert!(page.contains("agent-run"), "{page}");
            assert!(page.contains("- Agent: ag-1"), "{page}");
            assert!(!page.contains("- Run:"), "{page}");
            assert!(!page.contains("att_9"), "{page}");
            assert!(page.contains("NOT a completion"), "{page}");
            assert!(page.contains("- Status: running (running)"), "{page}");
            assert!(page.contains("glm-user/glm-5.3 profile review"), "{page}");
            assert!(page.contains("not bound"), "{page}");
            assert!(page.ends_with('\n') && !page.ends_with("\n\n"), "{page:?}");
        }
    }

    /// Malformed receipts cannot silently bind a resumed agent's original row.
    #[test]
    fn malformed_binding_receipt_preserves_known_admission() {
        for sequence in [
            Value::Null,
            json!(0),
            json!(-1),
            json!("2"),
            json!(4294967296_u64),
        ] {
            let value = json!({"agent_id":"ag-root","sequence":sequence,"created":true});
            let result = success_result("resume", &value);
            assert_eq!(
                result.is_error,
                Some(false),
                "known admission survives presentation failure"
            );
            assert!(
                result.structured_content.is_none(),
                "invalid counter must not bind"
            );
            let content = serde_json::to_value(&result.content).unwrap();
            assert!(
                content[0]["text"]
                    .as_str()
                    .unwrap()
                    .contains("ACCEPTED resume ag-root")
            );
            assert!(
                content[0]["text"]
                    .as_str()
                    .unwrap()
                    .contains("Do not repeat")
            );
        }
    }

    /// Selected MCPs are summarized without claiming that admission proved availability.
    #[test]
    fn start_reports_frozen_mcp_selection_compactly() {
        let mut agent = agent_view();
        agent["mcp"] = json!([
            {"name":"shared","source":"global","allowed_tools":null},
            {"name":"tracker","source":"both","allowed_tools":["read"]},
            {"name":"empty","source":"profile","allowed_tools":[]}
        ]);
        let value = json!({"agent_id":"ag-1","created":true,"agent":agent});
        for tool in ["start", "resume"] {
            let page = text(tool, &value);
            assert!(page.contains("availability not verified"), "{page}");
            assert!(
                page.contains(
                    "shared (global, tools=all); tracker (both, tools=1); empty (profile, tools=0)"
                ),
                "{page}"
            );
            assert!(!page.contains("allowed_tools"));
        }
    }

    /// The real binding hook normalizer extracts the id from the rendered
    /// start envelope exactly as a host would deliver it.
    #[test]
    fn posttooluse_binding_extracts_the_rendered_start_envelope() {
        let value = json!({"agent_id": "ag-2026-1", "created": true, "agent": agent_view()});
        let envelope = serde_json::to_value(success_result("start", &value)).unwrap();
        let payload = agent_run_core::hooks::bind::normalize(
            &json!({
                "hook_event_name": "PostToolUse", "session_id": "s-1",
                "tool_response": envelope,
            }),
            true,
            "claude_uds",
        )
        .expect("binding normalizer accepts the rendered envelope");
        assert_eq!(payload.agent_id.as_deref(), Some("ag-2026-1"));
        let resumed = json!({"agent_id":"ag-root", "run_id":"ag-child", "sequence":2, "created":true,"agent":agent_view()});
        let payload = agent_run_core::hooks::bind::normalize(
            &json!({"session_id":"s-1","tool_response":success_result("resume", &resumed)}),
            true,
            "claude_uds",
        )
        .unwrap();
        assert_eq!(payload.agent_id.as_deref(), Some("ag-root"));
        assert_eq!(payload.sequence, Some(2));
    }

    /// Resume hides internal lineage IDs; other tools cannot bypass templates.
    #[test]
    fn resume_lineage_and_non_guide_result_shapes_are_preserved() {
        let mut agent = agent_view();
        agent["parent_agent_id"] = json!("ag-legacy");
        agent["parent_run_id"] = json!("ag-parent");
        let page = text(
            "resume",
            &json!({"agent_id": "ag-root", "run_id":"ag-child", "created": true, "agent": agent}),
        );
        assert!(!page.contains("ag-parent"), "{page}");
        assert!(!page.contains("ag-child"), "{page}");
        assert!(!page.contains("ag-legacy"), "{page}");
        for (name, value) in [
            ("models", json!("unvalidated text")),
            ("delegation_guide", json!({})),
        ] {
            let result = success_result(name, &value);
            assert_eq!(result.is_error, Some(true), "{name}");
            assert!(
                serde_json::to_value(result.content).unwrap()[0]["text"]
                    .as_str()
                    .unwrap()
                    .contains("RuntimeError")
            );
        }
    }

    /// Cancel and steer report acceptance without claiming a terminal state.
    #[test]
    fn cancel_and_steer_report_pending_acceptance() {
        let cancel = text("cancel", &agent_view());
        assert!(cancel.contains("agent-run cancel accepted"), "{cancel}");
        assert!(cancel.contains("- Agent: ag-1"), "{cancel}");
        assert!(cancel.contains("requested, not yet confirmed"), "{cancel}");
        assert!(!cancel.contains("succeeded"), "{cancel}");
        let steer = text(
            "steer",
            &json!({"command_id": 7, "agent_id": "ag-1", "kind": "steer", "state": "queued"}),
        );
        assert!(steer.contains("agent-run steer accepted"), "{steer}");
        assert!(steer.contains("steer #7 (queued)"), "{steer}");
        assert!(steer.contains("applies to the active run"), "{steer}");
        let mut terminal = agent_view();
        terminal["status"] = json!("cancelled");
        let finished = text("cancel", &terminal);
        assert!(finished.contains("already terminal"));
        assert!(!finished.contains("watch list_agents"));
    }

    /// list_agents keeps the exact total, page size, and continuation.
    #[test]
    fn list_agents_keeps_total_and_continuation() {
        let page = text(
            "list_agents",
            &json!({
                "items": [agent_view(), agent_view()], "total": 5, "offset": 0,
                "limit": 2, "next_offset": 2, "complete": false, "revision": 9,
            }),
        );
        assert!(page.contains("2 of 5 matching (offset 0)"), "{page}");
        assert!(
            page.contains("- ag-1: running (running) — glm-user/glm-5.3 profile review"),
            "{page}"
        );
        assert!(page.contains("task: \"fix the parser\""), "{page}");
        assert!(
            page.contains("next offset: 2 — more matching rows remain"),
            "{page}"
        );
    }

    /// transcript preserves content verbatim and states its cursor contract.
    #[test]
    fn transcript_preserves_content_and_cursor() {
        let page = text(
            "transcript",
            &json!({
                "agent_id": "ag-1", "complete": false, "next_cursor": 4,
                "messages": [
                    {"seq": 1, "role": "user", "content": "please\nfix\tit"},
                    {"seq": 2, "role": "tool_call", "name": "shell", "content": "{\"cmd\":1}"},
                ],
            }),
        );
        assert!(page.contains("[1] user: please\nfix\tit"), "{page}");
        assert!(
            page.contains("[2] tool_call (\"shell\"): {\"cmd\":1}"),
            "{page}"
        );
        assert!(page.contains("continues at cursor 4"), "{page}");
        assert!(page.contains("next_cursor: 4"), "{page}");
    }

    /// answer shows availability honestly and keeps retrieval facts.
    #[test]
    fn answer_is_honest_about_availability_and_retrieval() {
        let missing = text(
            "answer",
            &json!({"agent_id": "ag-1", "status": "running", "available": false,
                    "inline_complete": false}),
        );
        assert!(missing.contains("NOT available"), "{missing}");
        let partial = text(
            "answer",
            &json!({
                "agent_id": "ag-1", "status": "succeeded", "available": true,
                "inline_complete": false, "kind": "agent_answer",
                "media_type": "text/markdown", "relative_path": "answers/a.md",
                "path": "/home/ag-1/agents/ag-1/answers/a.md",
                "size_bytes": 9000, "content": "partial only",
            }),
        );
        assert!(
            partial.contains("inline text absent or partial"),
            "{partial}"
        );
        assert!(
            partial.contains(
                "agent_answer (text/markdown) at /home/ag-1/agents/ag-1/answers/a.md, 9000 bytes"
            ),
            "{partial}"
        );
        let full = text(
            "answer",
            &json!({"agent_id": "ag-1", "status": "succeeded", "available": true,
                    "inline_complete": true, "content": "the whole answer\n"}),
        );
        assert!(full.contains("complete inline text below"), "{full}");
        assert!(full.contains("the whole answer"), "{full}");
    }

    /// One representative schema-2 catalog renders compactly with the
    /// configured facts, hoisted profiles, and no account or endpoint.
    #[test]
    fn models_renders_compact_guidance_and_grants() {
        // The bulky per-role asset arrays a real catalog carries; the text
        // page omits them, which is where its compactness comes from.
        let bulky = |prefix: &str, count: usize| {
            (0..count)
                .map(|index| json!({"id": format!("{prefix}-{index}")}))
                .collect::<Vec<_>>()
        };
        let model = json!({
            "model": "gpt-main", "native_model": "gpt-native",
            "params": {"effort": "medium"}, "allowed_params": {"effort": ["medium", "high"]},
            "restrictions": ["web_tools_disabled"],
            "recommendations": ["broad coding"],
            "profiles": ["code", "review"],
            "quota": {"status": "available", "evidence": "fresh"},
        });
        let mut other = model.clone();
        other["model"] = json!("gpt-review");
        let catalog = json!({
            "schema_version": 2, "config_revision": "abc123",
            "capacity_revision": 7,
            "profiles": [
                {"name": "code", "write": true, "network": false,
                 "read_roots": ["/tmp"], "required_constraints": [],
                 "skills": bulky("skill", 12), "mcp": bulky("server", 6),
                 "revision": "r1", "allow_external_read_roots": false},
                {"name": "review", "write": false, "network": false,
                 "read_roots": [], "required_constraints": ["filesystem_write_isolation"],
                 "skills": bulky("skill", 12), "mcp": bulky("server", 6),
                 "revision": "r2", "allow_external_read_roots": false},
            ],
            "providers": [{
                "provider": "codex", "harness": "codex",
                "recommendations": ["native subscription"],
                "models": [model, other],
            }],
        });
        let page = text("models", &catalog);
        assert!(
            page.contains("- code: write=true, network=false\n"),
            "{page}"
        );
        assert!(
            page.contains(
                "- review: write=false, network=false, constraints: filesystem_write_isolation"
            ),
            "{page}"
        );
        assert!(
            page.contains("provider codex (harness codex) — all models admit: code, review"),
            "{page}"
        );
        assert!(
            page.contains("provider guidance: \"native subscription\""),
            "{page}"
        );
        assert!(
            page.contains("- gpt-main (gpt-native): available, evidence fresh"),
            "{page}"
        );
        assert!(
            page.contains("params: effort=medium; allowed effort: medium|high"),
            "{page}"
        );
        assert!(page.contains("restrictions: web_tools_disabled"), "{page}");
        assert!(page.contains("model guidance: \"broad coding\""), "{page}");
        assert!(!page.contains("acct-"), "{page}");
        assert!(!page.contains("https://"), "{page}");
        assert!(
            page.len() < catalog.to_string().len(),
            "compact: {} vs {}",
            page.len(),
            catalog.to_string().len()
        );
    }

    /// Legacy rosters, both capacity-order schemas, limits, and doc render.
    #[test]
    fn legacy_and_remaining_tools_render_their_essentials() {
        let legacy = text(
            "models",
            &json!({"codex": {"available": true, "reason": null,
                              "models": [{"id": "gpt-5"}]}}),
        );
        assert!(legacy.contains("schema 1 runtime rosters"), "{legacy}");
        assert!(legacy.contains("- codex: available"), "{legacy}");
        assert!(legacy.contains("model: gpt-5"), "{legacy}");
        let order = text(
            "capacity_order",
            &json!({"schema_version": 2, "capacity_revision": 4, "providers": [
                {"provider": "glm", "score": 60.0, "priority_multiplier": 1.5,
                 "models": [{"model": "glm-5.3",
                             "quota": {"status": "available", "evidence": "fresh"}}]},
            ]}),
        );
        assert!(
            order.contains("1. glm (score 60.0, multiplier 1.5)"),
            "{order}"
        );
        assert!(
            order.contains("- glm-5.3: available, evidence fresh"),
            "{order}"
        );
        assert!(order.contains("not model ability"), "{order}");
        let legacy_order = text(
            "capacity_order",
            &json!({"routes": [{"runtime": "codex", "priority": 9.5,
                                "aliases": ["codex/main"]}],
                    "unavailable_runtimes": ["claude"]}),
        );
        assert!(
            legacy_order.contains("1. codex (priority 9.5) aliases: codex/main"),
            "{legacy_order}"
        );
        assert!(
            legacy_order.contains("unavailable runtimes: claude"),
            "{legacy_order}"
        );
        let limits = text(
            "limits",
            &json!({"observed_at": 1000.0, "items": [
                {"key": {"runtime": "glm", "lane": "glm-5.3", "window": "5h"},
                 "account": "work", "pool": "work::glm-5.3", "known": true,
                 "remaining_percent": 60.0, "reset_at": 4600.0},
                {"key": {"runtime": "codex", "lane": "codex", "window": "5h"},
                 "known": false},
            ]}),
        );
        assert!(
            limits.contains("- glm/glm-5.3/5h: 60.0% remaining, resets in 1h"),
            "{limits}"
        );
        assert!(
            limits.contains("- codex/codex/5h: no current sample (stale or expired)"),
            "{limits}"
        );
        let doc = text("doc", &json!({"topic": "config", "text": "guide prose\n"}));
        assert_eq!(doc, "guide prose\n");
    }

    /// Empty catalog states, legacy empties, and errors are explicit.
    #[test]
    fn empty_states_and_errors_are_explicit() {
        let empty_models = text(
            "models",
            &json!({"providers": [], "profiles": [],
                    "config_revision": "empty", "capacity_revision": 3}),
        );
        assert!(
            empty_models.contains("no providers are currently configured"),
            "{empty_models}"
        );
        let empty_legacy = text("models", &json!({}));
        assert!(
            empty_legacy.contains("no enabled runtimes"),
            "{empty_legacy}"
        );
        let empty_agents = text(
            "list_agents",
            &json!({"items": [], "total": 0, "offset": 0, "complete": true}),
        );
        assert!(empty_agents.contains("0 of 0 matching"), "{empty_agents}");
        let empty_limits = text("limits", &json!({"items": []}));
        assert!(
            empty_limits.contains("no stored capacity samples"),
            "{empty_limits}"
        );
        let error = error_result("ValidationError", "unknown arguments: ['x']\nsecond line");
        assert_eq!(error.is_error, Some(true));
        assert_eq!(error.structured_content, None);
        let content = serde_json::to_value(&error.content).unwrap();
        assert_eq!(
            content[0]["text"],
            format!(
                "agent-run error ValidationError: {}\n",
                super::label("unknown arguments: ['x']\nsecond line")
            )
        );
    }

    /// Unknown critical shapes fail safely; unread rows and source cursors are
    /// never published as though a shortened page were complete.
    #[test]
    fn malformed_and_oversized_pages_publish_nothing() {
        for value in [
            json!({"total":0,"offset":0,"complete":true}),
            json!({"items":null,"total":0,"offset":0,"complete":true}),
            json!({"items":[],"total":0,"offset":0,"complete":"true"}),
            json!({"items":[agent_view()],"total":2,"offset":0,"complete":false,"next_offset":2}),
            json!({"items":[agent_view()],"total":2,"offset":0,"complete":true}),
            json!({"items":[agent_view()],"total":u64::MAX,"offset":u64::MAX,"complete":false,"next_offset":0}),
            json!({"items":vec![agent_view();21],"total":22,"offset":0,"complete":false,"next_offset":21}),
        ] {
            let result = success_result("list_agents", &value);
            assert_eq!(result.is_error, Some(true));
            let text = serde_json::to_value(result.content).unwrap()[0]["text"]
                .as_str()
                .unwrap()
                .to_owned();
            assert!(!text.contains("next offset:"));
            assert!(!text.contains("- ag-1:"));
        }
        let mut row = agent_view();
        row["status"] = json!("future_success");
        let result = success_result(
            "list_agents",
            &json!({"items":[row],"total":1,"offset":0,"complete":true}),
        );
        assert_eq!(result.is_error, Some(true));
    }

    /// Friendly labels cannot forge structure; exact content and critical
    /// references survive, while discarded metadata canaries never enter either output.
    #[test]
    fn quoting_exactness_and_secret_projection() {
        let label = "x\n\u{1b}[31m\u{202e}{{ 7*7 }}";
        let mut row = agent_view();
        row["task_summary"] = json!(label.repeat(40));
        row["provider"] = json!({"account":"PRIVATE_CANARY","credential":"SECRET_CANARY"});
        row["delivery"]["orchestrator_session_id"] = json!("SESSION_CANARY");
        let page = text(
            "list_agents",
            &json!({"items":[row],"total":1,"offset":0,"complete":true}),
        );
        assert!(page.contains("{{ 7*7 }}"));
        assert!(page.contains("[shortened]"));
        assert!(!page.contains('\u{1b}'));
        assert!(!page.contains('\u{202e}'));
        for canary in ["PRIVATE_CANARY", "SECRET_CANARY", "SESSION_CANARY"] {
            assert!(!page.contains(canary));
        }
        let content = "code\n\t{{ untouched }}\n";
        let answer = text(
            "answer",
            &json!({"agent_id":"ag-1","status":"succeeded","available":true,
            "inline_complete":true,"content":content,"sha256":"a".repeat(64),"relative_path":"answers/a b.md","size_bytes":0}),
        );
        assert!(answer.contains(content));
        assert!(answer.contains(&"a".repeat(64)));
        assert!(answer.contains("answers/a b.md"));
        for n in [0, 1] {
            let page = text(
                "capacity_order",
                &json!({"schema_version":2,"capacity_revision":0,"providers":[
                {"provider":"p","score":n,"priority_multiplier":0,"models":[
                    {"model":"m","quota":{"status":"unknown","evidence":"missing"}}]}]}),
            );
            assert!(page.contains(&format!("score {n}.0")));
            assert!(page.contains("multiplier 0.0"));
            assert!(page.contains("unknown, evidence missing"));
        }
    }

    /// Rendering owns a bounded private buffer, with exact byte boundaries,
    /// strict missing variables and fuel exhaustion all discarding partial output.
    #[test]
    fn bounded_render_strictness_and_fuel() {
        let view = super::ExcerptTextView { text: "é" };
        for limit in [1, 2, 3] {
            let result = super::render_view("doc", &view, limit);
            assert_eq!(result.is_ok(), limit >= 2);
            if let Ok(text) = result {
                assert_eq!(text, "é");
            }
        }
        assert!(super::render_view("start", &json!({}), 4096).is_err());
        let mut env = minijinja::Environment::empty();
        env.set_fuel(Some(1));
        env.add_template("fuel", "{% for n in rows %}{{ n }}{% endfor %}")
            .unwrap();
        let mut writer = super::BoundedWriter {
            bytes: Vec::new(),
            limit: 1024,
        };
        assert!(
            env.get_template("fuel")
                .unwrap()
                .render_captured_to(json!({"rows":[1,2,3]}), &mut writer)
                .is_err()
        );
        let huge = "é".repeat(super::budget("doc").content / 2 + 1);
        assert_eq!(
            success_result("doc", &json!({"text":huge})).is_error,
            Some(true)
        );
    }

    /// A presentation failure preserves acknowledged writes and binding identity;
    /// uncertain transport writes remain unknown and never advise replacement work.
    #[test]
    fn committed_and_unknown_receipts_survive_presentation_failure() {
        for (tool, value, id) in [
            (
                "start",
                json!({"agent_id":"ag-1","sequence":2,"created":true}),
                "ag-1",
            ),
            (
                "resume",
                json!({"agent_id":"ag-1","sequence":3,"created":false}),
                "ag-1",
            ),
            ("steer", json!({"agent_id":"ag-1","command_id":7}), "ag-1"),
            (
                "notify_orchestrator",
                json!({"notification_id":"ntf_receipt","state":"future","duplicate":false}),
                "ntf_receipt",
            ),
        ] {
            let result = super::success_result_with_request(tool, &value, Some("req-original"));
            assert_eq!(result.is_error, Some(false));
            let public = serde_json::to_string(&result).unwrap();
            assert!(public.contains(id));
            assert!(public.contains("req-original"));
            assert!(public.contains("Do not repeat"));
            assert!(!public.contains("future"));
            if matches!(tool, "start" | "resume") {
                assert_eq!(result.structured_content.as_ref().unwrap()["agent_id"], id);
            }
        }
        let result = super::failure_result(
            "start",
            &crate::Error::Runtime("SECRET_CANARY".into()),
            Some("original"),
            Some("ag-1"),
        );
        let public = serde_json::to_string(&result).unwrap();
        assert_eq!(result.is_error, Some(true));
        assert!(public.contains("OUTCOME_UNKNOWN"));
        assert!(public.contains("original"));
        assert!(public.contains("Target agent: ag-1"));
        assert!(!public.contains("SECRET_CANARY"));
        let failed = super::failure_result(
            "steer",
            &crate::Error::Unsupported("not supported".into()),
            None,
            Some("ag-1"),
        );
        assert_eq!(failed.is_error, Some(true));
        assert!(!serde_json::to_string(&failed).unwrap().contains("ACCEPTED"));
    }

    /// Critical retry keys are reversible and reserved even at their existing
    /// 512-character ceiling; output failure after a valid acknowledgement cannot
    /// discard identities or turn acceptance into a retryable generic error.
    #[test]
    fn output_failure_and_request_key_ceiling_preserve_receipts() {
        let request = "\u{0001}".repeat(512);
        let value = json!({"agent_id":"ag-1","sequence":2,"created":true,"agent":agent_view()});
        let result = super::success_result_with_request("start", &value, Some(&request));
        assert_eq!(result.is_error, Some(false));
        let page = serde_json::to_value(&result.content).unwrap()[0]["text"]
            .as_str()
            .unwrap()
            .to_owned();
        assert!(page.len() <= super::budget("start").bytes);
        let key = page
            .lines()
            .find_map(|line| line.strip_prefix("Request: "))
            .unwrap();
        assert_eq!(serde_json::from_str::<String>(key).unwrap(), request);
        let mut oversized = value;
        oversized["agent"]["mcp"] = json!(
            (0..8)
                .map(|_| json!({
                    "name":"m".repeat(4096),"source":"global","allowed_tools":null
                }))
                .collect::<Vec<_>>()
        );
        let result = super::success_result_with_request("start", &oversized, Some("original"));
        let page = serde_json::to_value(&result.content).unwrap()[0]["text"]
            .as_str()
            .unwrap()
            .to_owned();
        assert_eq!(result.is_error, Some(false));
        assert!(page.contains("ACCEPTED start ag-1"));
        assert!(page.contains("Do not repeat"));
        assert!(!page.contains(&"m".repeat(4096)));
        assert_eq!(result.structured_content.unwrap()["sequence"], 2);
        let label = super::label("x\u{2028}y\u{2029}z");
        assert!(!label.contains('\u{2028}'));
        assert!(!label.contains('\u{2029}'));
        let path = "answers/x\u{2028}y.md";
        assert_eq!(
            serde_json::from_str::<String>(&super::exact_display(path)).unwrap(),
            path
        );
    }

    /// Actionable model names/parameters are reversible rather than shortened,
    /// while unknown credential-bearing parameter metadata never reaches the view.
    #[test]
    fn model_and_answer_empty_fidelity_drops_private_param_metadata() {
        let model = "custom\n\u{202e}{{ data }}";
        let catalog = json!({"schema_version":2,"capacity_revision":0,"profiles":[],
            "providers":[{"provider":"p","harness":"codex","models":[{
                "model":model,"native_model":model,"profiles":[],"restrictions":[],
                "params":{"effort":"medium","credential":{"value":"SECRET_CANARY"}},
                "allowed_params":{"effort":["medium","high"],"account_token":["PRIVATE_CANARY"]},
                "quota":{"status":"available","evidence":"fresh"}}]}]});
        let page = text("models", &catalog);
        assert!(page.contains(&super::exact_display(model)));
        assert_eq!(
            serde_json::from_str::<String>(&super::exact_display(model)).unwrap(),
            model
        );
        assert!(!page.contains("SECRET_CANARY"));
        assert!(!page.contains("PRIVATE_CANARY"));
        assert!(!page.contains('\u{202e}'));
        assert!(page.contains("effort=medium"));
        let empty = text(
            "answer",
            &json!({"agent_id":"ag-1","status":"succeeded",
            "available":true,"inline_complete":true,"content":""}),
        );
        assert!(empty.contains("empty (0 UTF-8 bytes)"));
    }
}
