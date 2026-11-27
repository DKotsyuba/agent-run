//! Compact MiniJinja presentation for every public MCP tool result.
//!
//! MCP successes render as one short plain-text page per tool from the
//! repo-owned templates in `assets/mcp/`, embedded at build time (never
//! loaded from the filesystem or hot-reloaded). The private socket and the
//! CLI keep their structured JSON contracts; only the MCP presentation is
//! text. `start`/`resume` additionally keep a tiny structured
//! `{"agent_id": ..., "sequence": ...}` so the PostToolUse binding hook can
//! bind the exact execution from JSON (see `hooks::bind`). Expected failures use typed
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
            environment.add_test("defined", |value: minijinja::Value| !value.is_undefined());
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
    /// Exact pending steering command, retained on formatting failure.
    command_id: Option<i64>,
}

impl ExecutionReceipt {
    /// Captures acknowledged effects before any projection/rendering. Only a
    /// successful mutation with a valid identity is confirmed as accepted.
    fn capture(tool: &str, value: &Value, request_id: Option<&str>) -> Self {
        let identity = value[if tool == "notify_orchestrator" {
            "notification_id"
        } else {
            "agent_id"
        }]
        .as_str()
        .filter(|id| reference(id))
        .map(str::to_owned);
        let accepted = identity.is_some()
            && matches!(
                tool,
                "start" | "resume" | "cancel" | "steer" | "notify_orchestrator"
            );
        let sequence = value
            .get("sequence")
            .or_else(|| value["agent"].get("sequence"))
            .and_then(Value::as_u64)
            .and_then(|n| u32::try_from(n).ok())
            .filter(|n| *n > 0);
        Self {
            accepted,
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
                if tool == "notify_orchestrator" {
                    ""
                } else {
                    self.identity.as_deref().unwrap_or("")
                }
            )
        } else if matches!(
            tool,
            "start" | "start_batch" | "resume" | "cancel" | "steer" | "notify_orchestrator"
        ) {
            "OUTCOME_UNKNOWN\nPresentation: degraded (presentation_failed).\nReconcile the original request before any retry; do not create replacement work.\n".into()
        } else {
            "agent-run error RuntimeError: presentation_failed; no page was published. For list_agents, use a smaller limit or filter by agent_id. For transcript, use a smaller limit. The operator CLI can retrieve larger results.\n".into()
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
        if tool == "notify_orchestrator" && self.accepted {
            result.structured_content = Some(json!({"notification_id": self.identity}));
        }
        if tool == "start_batch" && !self.accepted {
            result.structured_content = Some(json!({"code":"BatchOutcomeUnknown"}));
        }
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
    /// Broker-projected display name, empty for older views.
    name: String,
    /// Latest observed public usage summary; absence stays empty.
    usage: String,
    /// Honest lineage coverage summary.
    usage_lineage: String,
    /// Relative age for active runs or UTC completion time for terminal runs.
    age: Option<String>,
    /// Optional measured tool counts for the latest execution.
    tool_counts: Option<String>,
    /// Content-free ping state and actionable broker diagnosis.
    health: Option<String>,
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
    /// Inclusive first source sequence for a block.
    first_seq: Option<i64>,
    /// Inclusive final source sequence for a block.
    last_seq: Option<i64>,
    /// Whether earlier block fragments lie outside this page.
    partial_before: Option<bool>,
    /// Whether later block fragments lie outside this page.
    partial_after: Option<bool>,
    /// Explicit omitted inline byte count when known.
    omitted_bytes: Option<u64>,
    /// Inline completeness; absence remains unknown historical coverage.
    content_complete: Option<bool>,
    /// Fixed native result classification: error, ok, or unknown.
    error: String,
    /// Allowlisted native evidence source.
    error_source: Option<String>,
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
    /// Exclusive upper continuation for older blocks.
    previous_cursor: Option<i64>,
    /// Validated forward/backward paging direction.
    direction: String,
    /// Validated raw/blocks representation.
    view: String,
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
    /// Current execution receipt when the broker can provide it.
    sequence: Option<u32>,
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
        "list_agents" | "agent_integration" => Budget {
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

/// Largest encoded tool result, reserving 4 KiB within the 1 MiB MCP frame
/// for the JSON-RPC envelope. Checked before any result bytes reach stdout.
const MAX_RESULT_WIRE_BYTES: usize = 1024 * 1024 - 4096;

/// True when an answer needs retrieval metadata instead of a complete text page.
pub(super) fn answer_needs_metadata(value: &Value) -> bool {
    value["available"] == true
        && (value["inline_complete"] == false
            || validate_result("answer", value)
                .and_then(|()| render("answer", value, budget("answer").bytes))
                .is_err())
}

/// Drops only an oversized verified answer's inline bytes, retaining its proof.
fn compact_answer(value: &Value) -> Option<Value> {
    if value["available"] != true
        || value["inline_complete"] != true
        || !value["content"].is_string()
    {
        return None;
    }
    let size = value["size_bytes"].as_u64()?;
    if size == 0 || size > agent_run_platform::verify::MAX_ANSWER as u64 {
        return None;
    }
    if !matches!(value["proof_version"].as_u64(), Some(1 | 2)) {
        return None;
    }
    value["sha256"]
        .as_str()?
        .parse::<agent_run_domain::Sha256Digest>()
        .ok()?;
    value["path"].as_str()?;
    let compact = json!({
        "agent_id": value["agent_id"], "sequence": value["sequence"],
        "status": value["status"], "available": true, "inline_complete": false,
        "content": null, "path": value["path"], "relative_path": value["relative_path"],
        "size_bytes": size, "sha256": value["sha256"], "kind": value["kind"],
        "media_type": value["media_type"], "proof_version": value["proof_version"],
    });
    validate_result("answer", &compact).ok()?;
    Some(compact)
}

/// Keeps a single oversized transcript block visible with an explicit preview.
fn compact_transcript(value: &Value) -> Option<Value> {
    let messages = value["messages"].as_array()?;
    if messages.len() != 1 {
        return None;
    }
    let message = &messages[0];
    let content = message["content"].as_str()?;
    if content.len() <= budget("transcript").content {
        return None;
    }
    let mut end = content.len().min(4096);
    while !content.is_char_boundary(end) {
        end -= 1;
    }
    let omitted = u64::try_from(content.len() - end).ok()?;
    let known_omitted = match (
        message["content_complete"].as_bool(),
        message["omitted_bytes"].as_u64(),
    ) {
        (_, Some(previous)) => Some(previous.checked_add(omitted)?),
        (Some(true), None) => Some(omitted),
        _ => None,
    };
    let mut compact = value.clone();
    compact["messages"][0]["content"] = json!(&content[..end]);
    compact["messages"][0]["content_complete"] = json!(false);
    compact["messages"][0]["omitted_bytes"] = json!(known_omitted);
    validate_result("transcript", &compact).ok()?;
    Some(compact)
}

/// Links only the existing verified answer file, without creating another copy.
fn answer_file_resource(value: &Value) -> Option<rmcp::model::Resource> {
    use std::path::{Component, Path};

    if value["available"] != true || value["inline_complete"] != false {
        return None;
    }
    let size = value["size_bytes"].as_u64()?;
    if size == 0 || size > agent_run_platform::verify::MAX_ANSWER as u64 {
        return None;
    }
    if !matches!(value["proof_version"].as_u64(), Some(1 | 2)) {
        return None;
    }
    value["sha256"]
        .as_str()?
        .parse::<agent_run_domain::Sha256Digest>()
        .ok()?;
    let raw = value["path"].as_str()?;
    let path = Path::new(raw);
    if !path.is_absolute()
        || path
            .components()
            .any(|part| matches!(part, Component::ParentDir))
        || value["relative_path"].as_str()? != "answer.md"
        || path.file_name()?.to_str()? != "answer.md"
    {
        return None;
    }
    let run = path.parent()?;
    run.file_name()?
        .to_str()?
        .parse::<crate::domain::AgentId>()
        .ok()?;
    if run.parent()?.file_name()?.to_str()? != "agents" {
        return None;
    }
    let mut uri = String::from("file://");
    for byte in raw.bytes() {
        if byte.is_ascii_alphanumeric() || matches!(byte, b'/' | b'-' | b'_' | b'.' | b'~') {
            uri.push(char::from(byte));
        } else {
            use std::fmt::Write as _;
            write!(&mut uri, "%{byte:02X}").ok()?;
        }
    }
    Some(
        rmcp::model::Resource::new(uri, "answer.md")
            .with_description("Existing Agent Run answer; access follows host permissions.")
            .with_mime_type("text/markdown")
            .with_size(size),
    )
}

/// Builds a reply from successful broker JSON at the real transport boundary.
/// Receipt capture precedes all projection and formatting; writes never replay.
#[must_use]
pub fn success_result(tool: &str, value: &Value) -> CallToolResult {
    success_result_with_request(tool, value, None)
}

/// As success_result, retaining the caller's retry key independently of the view.
/// The complete encoded result must fit the wire budget; escaping overflow drops
/// the entire presentation while preserving confirmed effects and binding receipts.
#[must_use]
pub fn success_result_with_request(
    tool: &str,
    value: &Value,
    request_id: Option<&str>,
) -> CallToolResult {
    if tool == "batch_receipt" {
        return batch_receipt_result(value).unwrap_or_else(|_| {
            error_result("RuntimeError", "invalid batch receipt lookup response")
        });
    }
    if tool == "transcript_exact" {
        return transcript_exact_result(value)
            .unwrap_or_else(|_| error_result("RuntimeError", "invalid exact transcript response"));
    }
    if tool == "initial_task" {
        return initial_task_result(value)
            .unwrap_or_else(|_| error_result("RuntimeError", "invalid initial task response"));
    }
    if tool == "status" {
        return status_result(value)
            .unwrap_or_else(|_| error_result("RuntimeError", "invalid status response"));
    }

    if tool == "start_batch" {
        return batch_result_with_caller(value, request_id, &Value::Null, None);
    }
    if matches!(tool, "capabilities" | "answer_chunk") {
        return observation_result(tool, value).unwrap_or_else(|_| {
            error_result(
                "RuntimeError",
                "observation response is invalid or exceeds wire budget",
            )
        });
    }
    let receipt = ExecutionReceipt::capture(tool, value, request_id);
    let request_line = receipt
        .request_id
        .as_ref()
        .map(|id| format!("Request: {id}\n"))
        .unwrap_or_default();
    let limit = budget(tool).bytes.saturating_sub(request_line.len());
    let mut rendered = validate_result(tool, value)
        .and_then(|()| render(tool, value, limit))
        .map(|mut text| {
            text.push_str(&request_line);
            text
        });
    let mut compact = None;
    if rendered.is_err() {
        compact = match tool {
            "answer" => compact_answer(value),
            "transcript" => compact_transcript(value),
            _ => None,
        };
        if let Some(page) = compact.as_ref() {
            rendered = render(tool, page, limit).map(|mut text| {
                text.push_str(&request_line);
                text
            });
        }
    }
    let mut result = match rendered {
        Ok(text) => CallToolResult::success(vec![ContentBlock::text(text)]),
        Err(_) => receipt.fallback(tool),
    };
    let displayed = compact.as_ref().unwrap_or(value);
    if tool == "answer"
        && result.is_error != Some(true)
        && let Some(resource) = answer_file_resource(displayed)
    {
        result.structured_content = Some(json!({
            "agent_id": displayed["agent_id"], "sequence": displayed["sequence"],
            "size_bytes": displayed["size_bytes"], "sha256": displayed["sha256"],
            "path": displayed["path"], "inline_complete": false,
        }));
        result.content.push(ContentBlock::resource_link(resource));
    }
    if tool == "notify_orchestrator"
        && result.structured_content.is_none()
        && result.is_error != Some(true)
    {
        result.structured_content = Some(
            json!({"notification_id": value["notification_id"], "state": value["state"], "duplicate": value["duplicate"]}),
        );
    }
    if matches!(tool, "start" | "resume")
        && let (Some(id), Some(sequence)) = (&receipt.identity, receipt.sequence)
    {
        result.structured_content = Some(json!({"agent_id": id, "sequence": sequence}));
    }
    let mut wire = BoundedWriter {
        bytes: Vec::new(),
        limit: MAX_RESULT_WIRE_BYTES,
    };
    if serde_json::to_writer(&mut wire, &result).is_err() {
        let binding = result.structured_content.take();
        result = receipt.fallback(tool);
        result.structured_content = binding;
    }
    result
}

/// Publishes one complete found/missing lookup without inventing a refusal.
fn batch_receipt_result(value: &Value) -> Result<CallToolResult> {
    let output: agent_run_domain::batch::BatchReceiptLookup =
        serde_json::from_value(value.clone()).map_err(|_| shape_error())?;
    match (&output.receipt, output.found) {
        (None, false) => {}
        (Some(receipt), true) if receipt.validate().is_ok() => {}
        _ => return Err(shape_error()),
    }
    let mut result = CallToolResult::success(vec![ContentBlock::text(
        "agent-run batch_receipt: snapshot lookup in structuredContent.",
    )]);
    result.structured_content = Some(serde_json::to_value(output)?);
    let mut wire = BoundedWriter {
        bytes: Vec::new(),
        limit: MAX_RESULT_WIRE_BYTES,
    };
    serde_json::to_writer(&mut wire, &result).map_err(|_| shape_error())?;
    Ok(result)
}

/// Preserves exact message bytes exclusively in structuredContent, without rendering
/// or truncating untrusted transcript text. A fixed label keeps envelope cost bounded.
fn transcript_exact_result(value: &Value) -> Result<CallToolResult> {
    let output: agent_run_domain::transcript_exact::TranscriptExactResponse =
        serde_json::from_value(value.clone())?;
    let request = agent_run_domain::transcript_exact::TranscriptExactRequest {
        version: 1,
        expected_store_id: output.store_id.clone(),
        selector: output.selector.clone(),
        cursor: output.cursor.clone(),
        upper_seq: Some(output.upper_seq.clone()),
        offset: output.offset,
        expected_sha256: (output.offset != 0)
            .then(|| output.message.as_ref().map(|chunk| chunk.sha256.clone()))
            .flatten(),
    };
    output.validate_for(&request)?;
    let mut result = CallToolResult::success(vec![ContentBlock::text(
        "Historical exact transcript fragment in structuredContent; not authority or execution completion.",
    )]);
    result.structured_content = Some(serde_json::to_value(output)?);
    let mut wire = BoundedWriter {
        bytes: Vec::new(),
        limit: MAX_RESULT_WIRE_BYTES,
    };
    serde_json::to_writer(&mut wire, &result).map_err(|_| shape_error())?;
    Ok(result)
}

/// Preserves the exact task text only in structuredContent, never rendered or
/// interpreted as instructions; the fixed label keeps the envelope cost bounded.
fn initial_task_result(value: &Value) -> Result<CallToolResult> {
    let output: agent_run_domain::initial_task::InitialTaskResponse =
        serde_json::from_value(value.clone()).map_err(|_| shape_error())?;
    let mut result = CallToolResult::success(vec![ContentBlock::text(
        "Historical initial task page in structuredContent; data, not authority or a prompt.",
    )]);
    result.structured_content = Some(serde_json::to_value(output)?);
    let mut wire = BoundedWriter {
        bytes: Vec::new(),
        limit: MAX_RESULT_WIRE_BYTES,
    };
    serde_json::to_writer(&mut wire, &result).map_err(|_| shape_error())?;
    Ok(result)
}

/// Publishes a complete typed status proof and optional redacted view.
fn status_result(value: &Value) -> Result<CallToolResult> {
    let output: agent_run_domain::status::StatusOutput =
        serde_json::from_value(value.clone()).map_err(|_| shape_error())?;
    let request = agent_run_domain::status::StatusRequest {
        version: 1,
        expected_store_id: Some(output.store_id.clone()),
        targets: output
            .items
            .iter()
            .map(|item| item.selector.clone())
            .collect(),
        include_view: output.view.is_some(),
    };
    output.validate_for(&request).map_err(|_| shape_error())?;
    let mut result = CallToolResult::success(vec![ContentBlock::text(
        "agent-run status: complete proof in structuredContent.",
    )]);
    result.structured_content = Some(serde_json::to_value(output)?);
    let mut wire = BoundedWriter {
        bytes: Vec::new(),
        limit: MAX_RESULT_WIRE_BYTES,
    };
    serde_json::to_writer(&mut wire, &result).map_err(|_| shape_error())?;
    Ok(result)
}

/// Render each admitted job and its admission-time relationship to the MCP caller.
pub(super) fn batch_result_with_caller(
    value: &Value,
    request_id: Option<&str>,
    args: &Value,
    caller: Option<&crate::domain::OrchestratorRef>,
) -> CallToolResult {
    batch_result(value, request_id, args, caller).unwrap_or_else(|_| {
        ExecutionReceipt::capture("start_batch", &Value::Null, request_id).fallback("start_batch")
    })
}

/// Returns the complete validated durable batch receipt in structured content.
fn batch_result(
    value: &Value,
    request_id: Option<&str>,
    args: &Value,
    caller: Option<&crate::domain::OrchestratorRef>,
) -> Result<CallToolResult> {
    let mut batch: agent_run_domain::batch::StartBatchResult =
        serde_json::from_value(value.clone()).map_err(|_| shape_error())?;
    if batch.receipt.validate().is_err()
        || request_id.is_some_and(|request_id| request_id != batch.receipt.request_id)
    {
        return Err(shape_error());
    }
    use std::fmt::Write as _;
    let mut text = String::from("agent-run start_batch accepted\n");
    for job in &batch.receipt.jobs {
        let destination: Option<crate::domain::OrchestratorRef> =
            serde_json::from_value(args["jobs"][job.index]["orchestrator"].clone())
                .map_err(|_| shape_error())?;
        let binding = match (destination.as_ref(), caller) {
            (Some(dest), Some(caller))
                if dest.external_session_id == caller.external_session_id
                    && dest.canonical_transport()? == caller.canonical_transport()? =>
            {
                "completion bound to caller at admission"
            }
            (Some(_), None) => "completion destination set; caller identity unavailable",
            (Some(_), Some(_)) => "completion bound to a different destination at admission",
            (None, _) => "completion not bound at admission",
        };
        let _ = writeln!(text, "- agent_id={} — {binding}", job.agent_id);
    }
    let mut result = CallToolResult::success(vec![ContentBlock::text(text)]);
    batch.release_token = None;
    result.structured_content = Some(serde_json::to_value(batch)?);
    let mut wire = BoundedWriter {
        bytes: Vec::new(),
        limit: MAX_RESULT_WIRE_BYTES,
    };
    serde_json::to_writer(&mut wire, &result).map_err(|_| shape_error())?;
    Ok(result)
}

/// Preserves the two additive machine-readable reads as structured MCP results
/// with a fixed text summary. Typed allowlists discard no bytes or proof facts;
/// the entire encoded result must fit the existing wire reserve before publish.
/// Existing text-first tool presentation and binding receipts are unchanged.
fn observation_result(tool: &str, value: &Value) -> Result<CallToolResult> {
    let content = if tool == "answer_chunk" {
        let chunk: agent_run_domain::observation::AnswerChunkView =
            serde_json::from_value(value.clone()).map_err(|_| shape_error())?;
        if chunk.sequence == 0
            || chunk
                .sha256
                .parse::<agent_run_domain::Sha256Digest>()
                .is_err()
            || chunk.content.len() > agent_run_domain::observation::MAX_CHUNK_BYTES
            || chunk.total_bytes > agent_run_platform::verify::MAX_ANSWER as u64
            || chunk.offset_bytes > chunk.next_offset
            || chunk.next_offset > chunk.total_bytes
            || chunk.next_offset - chunk.offset_bytes != chunk.content.len() as u64
            || chunk.complete != (chunk.next_offset == chunk.total_bytes)
            || (chunk.content.is_empty() && !chunk.complete)
        {
            return Err(shape_error());
        }
        serde_json::to_value(chunk)?
    } else {
        let capabilities: agent_run_domain::observation::CapabilitiesView =
            serde_json::from_value(value.clone()).map_err(|_| shape_error())?;
        if capabilities.schema_version != 1 || capabilities.product != "agent-run" {
            return Err(shape_error());
        }
        serde_json::to_value(capabilities)?
    };
    let mut result = CallToolResult::success(vec![ContentBlock::text(format!(
        "agent-run {tool}: complete result in structuredContent."
    ))]);
    result.structured_content = Some(content);
    let mut wire = BoundedWriter {
        bytes: Vec::new(),
        limit: MAX_RESULT_WIRE_BYTES,
    };
    serde_json::to_writer(&mut wire, &result).map_err(|_| shape_error())?;
    Ok(result)
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
    if tool == "start_batch" && error.machine_code().as_str() == "PreconditionFailed" {
        let public = error.public();
        let Some(reason) = public.reason else {
            return failure_result(
                tool,
                &Error::BatchOutcomeUnknown {
                    source: Box::new(crate::error::invalid("invalid precondition refusal")),
                },
                request_id,
                target_id,
            );
        };
        let mut result = error_result(public.kind, &public.message);
        result.structured_content = Some(json!({"code":public.kind,"reason":reason}));
        return result;
    }
    if (tool == "start_batch"
        && error.machine_code() == agent_run_domain::MachineCode::BatchOutcomeUnknown)
        || (tool == "batch_receipt"
            && matches!(
                error.machine_code(),
                agent_run_domain::MachineCode::StoreIdentityMismatch
                    | agent_run_domain::MachineCode::RequestConflict
            ))
    {
        let public = error.public();
        let mut result = error_result(public.kind, &public.message);
        result.structured_content = Some(json!({"code": public.kind}));
        return result;
    }
    let write = matches!(
        tool,
        "start" | "start_batch" | "resume" | "cancel" | "steer" | "notify_orchestrator"
    );
    let uncertain = matches!(error, Error::Runtime(_) | Error::Io(_) | Error::Json(_))
        || (tool == "start_batch"
            && matches!(
                error.machine_code(),
                agent_run_domain::MachineCode::RuntimeError
                    | agent_run_domain::MachineCode::IOError
                    | agent_run_domain::MachineCode::StorageError
                    | agent_run_domain::MachineCode::BrokerUnavailable
            ))
        || (tool == "resume" && matches!(error, Error::BrokerUnavailable));
    if write && uncertain {
        let receipt = ExecutionReceipt {
            identity: target_id.filter(|id| reference(id)).map(str::to_owned),
            ..ExecutionReceipt::capture(tool, &Value::Null, request_id)
        };
        let mut result = receipt.fallback(tool);
        if error.machine_code() == agent_run_domain::MachineCode::BrokerUnavailable {
            result.content.push(ContentBlock::text(
                "Broker unreachable. Check the Agent Run service, then reconcile the original request before retrying.",
            ));
        }
        return result;
    }
    let public = error.public();
    let mut result = error_result(public.kind, &public.message);
    if let Some((sequence, status)) = error.sequence_mismatch() {
        result.structured_content = Some(json!({
            "code": public.kind, "latest_sequence": sequence, "latest_status": status,
        }));
    }
    result
}

/// Presents one operator result through shared text views or the two additive
/// typed structured observation views.
#[must_use]
pub fn public_success_result(tool: &str, value: &Value) -> CallToolResult {
    public_success_result_with_request(tool, value, None)
}

/// Keeps retry receipts independent of presentation. Observation methods
/// retain their complete typed structured content.
#[must_use]
pub fn public_success_result_with_request(
    tool: &str,
    value: &Value,
    request_id: Option<&str>,
) -> CallToolResult {
    success_result_with_request(tool, value, request_id)
}

/// Expected failures remain errors even when their presentation fails.
/// No raw context, renderer source or error chain reaches the caller.
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

/// Returns a required bounded reference from broker JSON. Stable *_id fields
/// and replacement identities must also satisfy the exact ASCII identity shape;
/// descriptive model/runtime aliases retain their reversible projection quoting.
fn required_reference<'a>(value: &'a Value, key: &str) -> Result<&'a str> {
    value[key]
        .as_str()
        .filter(|s| {
            !s.is_empty()
                && s.len() <= 4096
                && (!(key.ends_with("_id") || key == "replaced_by") || reference(s))
        })
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
    Ok(())
}

/// Checks source shapes, all page rows/cursors and faithful content before
/// allocating projected rows. Unknown states never become success or empty pages.
fn validate_result(tool: &str, value: &Value) -> Result<()> {
    let policy = budget(tool);
    match tool {
        "agent_integration" => {
            if value["schema_version"] != 1 {
                return Err(shape_error());
            }
            validate_agent(&value["agent"])?;
            positive_counter(&value["agent"], "sequence")?;
            let communication: agent_run_domain::integration::Communication =
                serde_json::from_value(value["communication"].clone())
                    .map_err(|_| shape_error())?;
            if let agent_run_domain::integration::Communication::Observed {
                address,
                sampled_at_ms,
            } = communication
                && (address.is_empty()
                    || address.len() > 4096
                    || address.chars().any(char::is_control)
                    || sampled_at_ms == 0
                    || sampled_at_ms > i64::MAX as u64)
            {
                return Err(shape_error());
            }
        }
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
                || !matches!(value["state"].as_str(), Some("pending" | "completed"))
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
            let direction = value["direction"].as_str().unwrap_or("forward");
            let view = value["view"].as_str().unwrap_or("raw");
            if !matches!(direction, "forward" | "backward")
                || !matches!(view, "raw" | "blocks")
                || direction == "backward" && view != "blocks"
            {
                return Err(shape_error());
            }
            let continuation = if direction == "backward" {
                "previous_cursor"
            } else {
                "next_cursor"
            };
            let unused = if direction == "backward" {
                "next_cursor"
            } else {
                "previous_cursor"
            };
            if (!complete && value[continuation].as_i64().is_none_or(|n| n < 0))
                || (complete && !value[continuation].is_null())
                || !value[unused].is_null()
            {
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
                if let Some(first) = row["first_seq"].as_i64() {
                    if first < 1 || first > seq || row["last_seq"].as_i64() != Some(seq) {
                        return Err(shape_error());
                    }
                } else if !row["first_seq"].is_null() || !row["last_seq"].is_null() {
                    return Err(shape_error());
                }
                for key in [
                    "partial_before",
                    "partial_after",
                    "content_complete",
                    "error",
                ] {
                    if !row[key].is_null() {
                        required_bool(row, key)?;
                    }
                }
                if !row["omitted_bytes"].is_null() && !row["omitted_bytes"].is_u64() {
                    return Err(shape_error());
                }
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
        _ => return Err(shape_error()),
    }
    Ok(())
}

/// Requires a positive SQLite-range public counter, preserving exact values.
fn positive_counter(value: &Value, key: &str) -> Result<u64> {
    value[key]
        .as_u64()
        .filter(|n| *n > 0 && *n <= i64::MAX as u64)
        .ok_or_else(shape_error)
}

/// Preserves bounded printable display labels while escaping invisible formatting.
fn display_label(text: &str) -> String {
    if text.len() <= 1024 && text.chars().all(|c| !c.is_control()
        && !matches!(c, '\u{2028}' | '\u{2029}' | '\u{061c}' | '\u{200e}' | '\u{200f}' | '\u{202a}'..='\u{202e}' | '\u{2066}'..='\u{2069}')) {
        text.to_owned()
    } else { label(text) }
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
        "agent_integration" => {
            let communication: agent_run_domain::integration::Communication =
                serde_json::from_value(value["communication"].clone())
                    .map_err(|_| shape_error())?;
            let observation = match communication {
                agent_run_domain::integration::Communication::Observed {
                    address,
                    sampled_at_ms,
                } => format!(
                    "observed {} at {}",
                    exact_display(&address),
                    crate::domain::utc_timestamp(sampled_at_ms as f64 / 1000.0)
                        .ok_or_else(shape_error)?,
                ),
                agent_run_domain::integration::Communication::Unknown { reason } => {
                    format!("unknown {}", serde_json::to_value(reason)?)
                }
                agent_run_domain::integration::Communication::Unsupported { reason } => {
                    format!("unsupported {}", serde_json::to_value(reason)?)
                }
            };
            let text = format!(
                "Agent {} execution {}\nCommunication: {observation}\nObservation proves identity only; it does not prove idle state or delivery.\n",
                exact_display(required_reference(&value["agent"], "agent_id")?),
                value["agent"]["sequence"]
            );
            render_view("doc", &ExcerptTextView { text: &text }, limit)
        }
        "start" | "resume" => {
            let mut text = render_view(
                name,
                &projected::<AdmissionTextView>(start_context(value))?,
                limit,
            )?;
            if let Some(warnings) = value["warnings"].as_array() {
                for warning in warnings.iter().filter_map(Value::as_str) {
                    let line = format!("Warning: {}\n", exact_display(warning));
                    if text.len() + line.len() <= limit {
                        text.push_str(&line);
                    }
                }
            }
            Ok(text)
        }
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
                "state": value["state"], "duplicate": value["duplicate"]
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
    let terminal = matches!(
        status.as_str(),
        "succeeded" | "failed" | "lost" | "timed_out" | "cancelled"
    );
    let age = if terminal {
        view["finished_at"]
            .as_f64()
            .and_then(crate::domain::utc_timestamp)
    } else {
        view["elapsed_seconds"].as_f64().map(|seconds| {
            let seconds = seconds as u64;
            match seconds {
                0..=59 => format!("{seconds}s"),
                60..=3599 => format!("{}m", seconds / 60),
                3600..=86399 => format!("{}h", seconds / 3600),
                _ => format!("{}d", seconds / 86400),
            }
        })
    };
    json!({
        "id": view["agent_id"], "status": status, "terminal": terminal, "age": age,
        "runtime": view["runtime"].as_str().map(exact_display), "model": view["model"].as_str().map(exact_display), "profile": view["profile"].as_str().map(exact_display),
        "phase": text("phase"), "effort": text("effort"),
        "name": view["name"].as_str().map(display_label).unwrap_or_default(),
        "usage": usage_line(&view["usage"]),
        "usage_lineage": lineage_line(&view["usage_cumulative"]),
        "health": health_line(&view["health"]),
        "tool_counts": view["tool_counts"]["calls"].as_u64().map(|calls| {
            let mut line = format!("{calls} native calls");
            if let Some(failed) = view["tool_counts"]["failed"].as_u64() {
                line.push_str(&format!(", {failed} failed"));
            }
            line
        }),
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

/// Render measured input and output tokens, omitting counts without evidence.
fn usage_line(usage: &Value) -> String {
    if usage["usage_source"] == "none" {
        return "no native usage reported".into();
    }
    let parts = [("in", "input_tokens"), ("out", "output_tokens")]
        .into_iter()
        .filter_map(|(label, key)| {
            usage[key]
                .as_u64()
                .map(|count| format!("{label} {}", token_count(count)))
        })
        .collect::<Vec<_>>();
    if parts.is_empty() {
        String::new()
    } else {
        format!("{} tokens", parts.join(" "))
    }
}

/// Keep token counts exact below one thousand and compact above it.
fn token_count(count: u64) -> String {
    match count {
        0..=999 => count.to_string(),
        1000..=999_999 => format!("{}k", (count + 500) / 1000),
        _ => format!("{:.1}M", count as f64 / 1_000_000.0).replace(".0M", "M"),
    }
}

/// Translate health observations into agent-facing state and recovery guidance.
fn health_line(health: &Value) -> Option<String> {
    let state = health["state"].as_str()?;
    if state == "terminal" {
        return Some("execution ended — use answer to retrieve the result".into());
    }
    let line = match health["reason"].as_str() {
        Some("idle_without_wait_source" | "idle_without_wait_source_after_ping") => {
            "idle without a wait source — steer it or check the transcript"
        }
        Some("worker_blocked") => {
            "worker reported a blocker — check the transcript and address the blocker"
        }
        Some("control_unresponsive" | "native_control_unavailable") => {
            "worker control is unavailable — check broker health and the transcript"
        }
        Some("model_ping_delivery_unconfirmed" | "model_reply_missing") => {
            "worker has not acknowledged a health check — check the transcript"
        }
        Some(
            "clock_regression"
            | "activity_observation_unavailable"
            | "native_observation_unavailable",
        ) => "worker activity could not be verified — check the transcript",
        _ => match state {
            "waiting" => "waiting for a native tool — check the transcript for progress",
            "checking" => "checking worker activity — check the transcript for progress",
            "observing" => "monitoring worker activity",
            _ => "worker needs attention — check the transcript",
        },
    };
    Some(line.into())
}

/// One compact lineage-aggregate line; empty when no aggregate is attached.
///
/// Incomplete evidence is named as such rather than shown as partial totals.
fn lineage_line(cumulative: &Value) -> String {
    let Some(object) = cumulative.as_object() else {
        return String::new();
    };
    let executions = object
        .get("executions")
        .and_then(Value::as_u64)
        .unwrap_or_default();
    let input = object.get("input_tokens").and_then(Value::as_i64);
    let output = object.get("output_tokens").and_then(Value::as_i64);
    match (input, output) {
        (Some(input), Some(output)) => {
            format!(
                "lineage in {} out {} tokens over {executions} execution(s)",
                token_count(input.max(0) as u64),
                token_count(output.max(0) as u64)
            )
        }
        _ => format!("lineage incomplete over {executions} execution(s)"),
    }
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
        "previous_cursor": value["previous_cursor"].as_i64(),
        "direction": value["direction"].as_str().unwrap_or("forward"),
        "view": value["view"].as_str().unwrap_or("raw"),
        "complete": value["complete"],
        "messages": value["messages"].as_array().into_iter().flatten().map(|message| {
            json!({
                "seq": message["seq"], "role": message["role"],
                "name": message["name"].as_str().map(label), "content": message["content"],
                "first_seq": message["first_seq"], "last_seq": message["last_seq"],
                "partial_before":message["partial_before"],
                "partial_after":message["partial_after"], "omitted_bytes":message["omitted_bytes"],
                "content_complete":message["content_complete"],
                "error":message["error"].as_bool().map(|failed| if failed {"error"} else {"ok"}).unwrap_or(""),
                "error_source":message["error_source"],
            })
        }).collect::<Vec<_>>(),
        "count": value["messages"].as_array().map(Vec::len),
    })
}

/// Answer context: availability plus the full inline text or retrieval facts.
fn answer_context(value: &Value) -> Value {
    json!({
        "agent_id": value["agent_id"], "sequence": value["sequence"].as_u64(), "status": value["status"],
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
    /// A socket-only credential never crosses the MCP presentation boundary.
    #[test]
    fn batch_presentation_omits_socket_release_bearer() {
        use agent_run_domain::batch::{PinId, PinReleaseToken};
        let token = PinReleaseToken::mint();
        let canary = serde_json::to_value(&token).unwrap();
        let root = "ag-20260826-120000-0123456789".parse().unwrap();
        let pin = PinId::from_release_digest(&token.digest(), &root);
        let value = json!({"replayed":false,"release_token":canary,
            "receipt":{"version":1,"store_id":"store_0123456789abcdef0123456789abcdef",
                "request_id":"retained","jobs":[{"index":0,"agent_id":root,"sequence":1,
                    "execution_ref":format!("ex1_{}", "a".repeat(64)),"pin_id":pin}]}});
        let result = super::success_result_with_request("start_batch", &value, Some("retained"));
        assert_ne!(result.is_error, Some(true));
        assert_eq!(
            result.structured_content.as_ref().unwrap()["receipt"],
            value["receipt"]
        );
        let wire = serde_json::to_string(&result).unwrap();
        assert!(!wire.contains("release_token"));
        assert!(!wire.contains(canary.as_str().unwrap()));
    }

    /// Batch text distinguishes caller binding from omitted delivery destinations.
    #[test]
    fn batch_text_shows_every_job_and_its_binding() {
        let value = json!({"replayed":false,"receipt":{
            "version":1,"store_id":"store_0123456789abcdef0123456789abcdef","request_id":"batch",
            "jobs":[
                {"index":0,"agent_id":"ag-20200101-000000-0000000001","sequence":1,"execution_ref":format!("ex1_{}", "a".repeat(64))},
                {"index":1,"agent_id":"ag-20200101-000000-0000000002","sequence":1,"execution_ref":format!("ex1_{}", "b".repeat(64))}
            ]
        }});
        let destination = json!({"transport":"codex","external_session_id":"opaque-caller"});
        let caller = serde_json::from_value(destination.clone()).unwrap();
        let args = json!({"jobs":[{"orchestrator":destination},{}]});
        let result = super::batch_result_with_caller(&value, Some("batch"), &args, Some(&caller));
        assert_ne!(result.is_error, Some(true));
        let wire = serde_json::to_value(&result).unwrap();
        let text = wire["content"][0]["text"].as_str().unwrap();
        assert!(
            text.contains("agent_id=ag-20200101-000000-0000000001 — completion bound to caller")
        );
        assert!(text.contains("agent_id=ag-20200101-000000-0000000002 — completion not bound"));
        assert!(!text.contains("opaque-caller"));
    }

    /// Structured batch receipts preserve every identity; malformed or unknown outcomes retain the key.
    #[test]
    fn batch_receipt_and_uncertain_failure_are_not_replayed_as_new_work() {
        let value = serde_json::json!({"replayed":false,"receipt":{"version":1,"store_id":"store_0123456789abcdef0123456789abcdef","request_id":"batch-key","jobs":[
            {"index":0,"agent_id":"ag-20200101-000000-0000000001","sequence":1,"execution_ref":format!("ex1_{}", "a".repeat(64))},
            {"index":1,"agent_id":"ag-20200101-000000-0000000002","sequence":1,"execution_ref":format!("ex1_{}", "b".repeat(64))}
        ]}});
        let result = super::success_result_with_request("start_batch", &value, Some("batch-key"));
        assert_ne!(result.is_error, Some(true));
        assert_eq!(result.structured_content, Some(value.clone()));
        for bad_ref in [None, Some("ex1_bad")] {
            let mut bad = value.clone();
            match bad_ref {
                Some(reference) => bad["receipt"]["jobs"][0]["execution_ref"] = json!(reference),
                None => {
                    bad["receipt"]["jobs"][0]
                        .as_object_mut()
                        .unwrap()
                        .remove("execution_ref");
                }
            }
            let result = super::success_result_with_request("start_batch", &bad, Some("batch-key"));
            assert_eq!(result.is_error, Some(true));
        }
        let mut malformed = value;
        malformed["receipt"]["jobs"][1]["agent_id"] =
            malformed["receipt"]["jobs"][0]["agent_id"].clone();
        let result =
            super::success_result_with_request("start_batch", &malformed, Some("batch-key"));
        assert_eq!(result.is_error, Some(true));
        let wire = serde_json::to_value(result).unwrap();
        assert!(
            wire["content"][0]["text"]
                .as_str()
                .unwrap()
                .contains("OUTCOME_UNKNOWN")
        );
        assert!(
            wire["content"][0]["text"]
                .as_str()
                .unwrap()
                .contains("batch-key")
        );

        let error = crate::Error::Broker {
            message: "writer uncertain".into(),
            broker_error_code: Some("RuntimeError".into()),
            broker_error_data: None,
        };
        let result = super::failure_result("start_batch", &error, Some("batch-key"), None);
        let wire = serde_json::to_value(result).unwrap();
        assert!(
            wire["content"][0]["text"]
                .as_str()
                .unwrap()
                .contains("OUTCOME_UNKNOWN")
        );
        let unknown = crate::Error::BatchOutcomeUnknown {
            source: Box::new(crate::Error::BrokerUnavailable),
        };
        let wire = serde_json::to_value(super::failure_result(
            "start_batch",
            &unknown,
            Some("batch-key"),
            None,
        ))
        .unwrap();
        assert_eq!(wire["isError"], true);
        assert_eq!(wire["structuredContent"]["code"], "BatchOutcomeUnknown");
        assert!(
            wire["content"][0]["text"]
                .as_str()
                .unwrap()
                .contains("retry the same request_id")
        );
    }

    /// Closed failure reasons survive MCP; a malformed broker refusal stays unknown.
    #[test]
    fn batch_precondition_error_is_structured_and_fail_closed() {
        for reason in ["not_met", "unknown"] {
            let error = crate::Error::Broker {
                message: "condition failed".into(),
                broker_error_code: Some("PreconditionFailed".into()),
                broker_error_data: Some(json!({"code":"PreconditionFailed","reason":reason})),
            };
            let result = super::failure_result("start_batch", &error, Some("batch-key"), None);
            assert_eq!(result.is_error, Some(true));
            assert_eq!(
                result.structured_content,
                Some(json!({"code":"PreconditionFailed","reason":reason}))
            );
        }
        let error = crate::Error::Broker {
            message: "condition failed".into(),
            broker_error_code: Some("PreconditionFailed".into()),
            broker_error_data: Some(json!({"code":"PreconditionFailed","reason":"bad"})),
        };
        let result = super::failure_result("start_batch", &error, Some("batch-key"), None);
        assert_eq!(
            result.structured_content,
            Some(json!({"code":"BatchOutcomeUnknown"}))
        );
    }

    /// A maximum valid batch keeps every prerequisite; oversized output is
    /// explicitly unknown instead of a truncated successful receipt.
    #[test]
    fn batch_precondition_receipt_fits_or_fails_whole() {
        let request_id = "r".repeat(512);
        let conditions: Vec<_> = (0..20)
            .map(|i| {
                json!({
                    "agent_id":format!("ag-20260826-120000-{i:010x}"), "sequence":1,
                    "execution_ref":format!("ex1_{:064x}", i+1),
                    "pin_id":format!("pin_{:012x}4{:03x}8{i:015x}",0,0)
                })
            })
            .collect();
        let jobs: Vec<_> = (0..20)
            .map(|i| {
                json!({
                    "index":i,"agent_id":format!("ag-20260826-120001-{i:010x}"),"sequence":1,
                    "execution_ref":format!("ex1_{:064x}", i+21)
                })
            })
            .collect();
        let value = json!({"replayed":false,"receipt":{"version":1,"store_id":"store_0123456789abcdef0123456789abcdef",
            "request_id":request_id,"preconditions":conditions,"jobs":jobs}});
        let result = super::success_result_with_request("start_batch", &value, Some(&request_id));
        assert_ne!(result.is_error, Some(true));
        assert_eq!(result.structured_content, Some(value.clone()));
        let lookup = json!({"found":true,"receipt":value["receipt"]});
        let result = super::success_result("batch_receipt", &lookup);
        assert_eq!(result.structured_content, Some(lookup));
        let mut oversized = value;
        oversized["receipt"]["request_id"] = json!("x".repeat(super::MAX_RESULT_WIRE_BYTES));
        let result =
            super::success_result_with_request("start_batch", &oversized, Some(&request_id));
        assert_eq!(result.is_error, Some(true));
        assert_eq!(
            result.structured_content,
            Some(json!({"code":"BatchOutcomeUnknown"}))
        );
    }

    /// A long verified answer keeps its proof and existing file without inline overflow.
    #[test]
    fn large_answer_presents_existing_file_and_retrieval_proof() {
        let id = "ag-20200101-000000-0000000001";
        let body = "я".repeat(8_000);
        let digest = agent_run_platform::fs::sha256(body.as_bytes());
        let path = format!("/srv/agent run/agents/{id}/answer.md");
        let value = serde_json::json!({
            "agent_id":id, "sequence":1, "status":"succeeded", "available":true,
            "inline_complete":true, "content":body, "path":path,
            "relative_path":"answer.md", "size_bytes":16_000, "sha256":digest,
            "kind":"agent_answer", "media_type":"text/markdown; charset=utf-8", "proof_version":2
        });
        let result = super::success_result("answer", &value);
        assert_ne!(result.is_error, Some(true));
        let wire = serde_json::to_value(&result).unwrap();
        let text = wire["content"][0]["text"].as_str().unwrap();
        assert!(text.contains("Execution sequence: 1") && text.contains(&digest));
        assert!(
            text.contains(&format!(
                "agent-run answer-chunk {id} --sequence 1 --expected-sha256 {digest} --offset-bytes 0"
            )) && !text.contains(&"я".repeat(100))
        );
        assert_eq!(wire["structuredContent"]["sha256"], digest);
        assert_eq!(wire["structuredContent"]["size_bytes"], 16_000);
        assert_eq!(wire["content"][1]["type"], "resource_link");
        assert!(
            wire["content"][1]["uri"]
                .as_str()
                .unwrap()
                .contains("agent%20run")
        );
        assert!(serde_json::to_vec(&result).unwrap().len() < 16 * 1024);

        // A transient failure to retrieve supplemental sequence metadata must
        // leave the artifact reference available and explain how to retry.
        let mut without_sequence = value;
        without_sequence.as_object_mut().unwrap().remove("sequence");
        let result = super::success_result("answer", &without_sequence);
        assert_ne!(result.is_error, Some(true));
        let wire = serde_json::to_value(&result).unwrap();
        assert!(
            wire["content"][0]["text"]
                .as_str()
                .unwrap()
                .contains("retry answer")
        );
    }

    /// Missing or malformed proof cannot be transformed into a verified file link.
    #[test]
    fn oversized_answer_without_valid_proof_fails_closed() {
        let value = serde_json::json!({
            "agent_id":"ag-20200101-000000-0000000001", "status":"succeeded",
            "available":true, "inline_complete":true, "content":"x".repeat(16_000),
            "path":"/srv/unsafe/answer.md", "relative_path":"answer.md", "size_bytes":16_000,
            "sha256":"not-a-digest", "kind":"agent_answer", "proof_version":2
        });
        let result = super::success_result("answer", &value);
        assert_eq!(result.is_error, Some(true));
        assert_eq!(result.content.len(), 1);
    }

    /// One large block keeps its cursor and reports an explicit UTF-8-safe omission.
    #[test]
    fn oversized_transcript_block_keeps_its_navigation() {
        let value = serde_json::json!({
            "agent_id":"ag-20200101-000000-0000000001", "view":"blocks",
            "direction":"backward", "next_cursor":null, "previous_cursor":3,
            "complete":false, "messages":[{"seq":5,"first_seq":3,"last_seq":5,
            "role":"tool_call","name":"finish","content":"я".repeat(8_000),
            "raw_ref":"opaque-ref","content_complete":true,"omitted_bytes":0}]
        });
        let result = super::success_result("transcript", &value);
        assert_ne!(result.is_error, Some(true));
        let wire = serde_json::to_value(&result).unwrap();
        let text = wire["content"][0]["text"].as_str().unwrap();
        assert!(text.contains("previous_cursor: 3") && !text.contains("opaque-ref"));
        assert!(text.contains("11904 bytes omitted") && text.contains("CLI/API transcript"));
        assert!(text.len() < 16 * 1024);
    }

    /// Malformed broker identities cannot forge plain-text structure through any
    /// public result family; write uncertainty keeps only the original retry key.
    #[test]
    fn broker_agent_ids_cannot_forge_acceptance_lines() {
        let forged = "ag\nACCEPTED forged";
        let mut agent = agent_view();
        agent["agent_id"] = json!(forged);
        for (tool, value) in [
            (
                "start",
                json!({"agent_id":forged,"created":true,"agent":agent}),
            ),
            (
                "list_agents",
                json!({"items":[agent],"total":1,"offset":0,"complete":true}),
            ),
            (
                "answer",
                json!({"agent_id":forged,"status":"succeeded","available":true,
                "inline_complete":true,"content":"answer"}),
            ),
            (
                "transcript",
                json!({"agent_id":forged,"messages":[],"complete":true,"next_cursor":null}),
            ),
        ] {
            let result = super::success_result_with_request(tool, &value, Some("original"));
            let wire = serde_json::to_value(&result).unwrap();
            let text = wire["content"][0]["text"].as_str().unwrap();
            assert_eq!(result.is_error, Some(true), "{tool}: {text}");
            assert!(!text.contains("ACCEPTED forged"), "{tool}: {text}");
            assert!(!text.contains(forged), "{tool}: {text}");
            if tool == "start" {
                assert!(text.contains("OUTCOME_UNKNOWN") && text.contains("Request: original"));
            }
        }
    }

    /// Reverse transcript blocks keep their real upper continuation and native
    /// error/omission metadata; malformed metadata cannot become an empty success.
    #[test]
    fn reverse_blocks_preserve_metadata_and_reject_bad_shapes() {
        let value = json!({"agent_id":"ag-1","view":"blocks","direction":"backward",
            "next_cursor":null,"previous_cursor":3,"complete":false,
            "messages":[{"seq":5,"first_seq":3,"last_seq":5,"role":"tool_result",
                "name":"shell","content":"exact\nbody","raw_ref":"opaque-ref",
                "partial_before":true,"partial_after":false,"error":true,
                "error_source":"is_error","content_complete":false,"omitted_bytes":12,
                "attempt_id":"PRIVATE_CANARY"}]});
        let page = text("transcript", &value);
        for fact in [
            "[3..5]",
            "error: is_error",
            "exact\nbody",
            "12 bytes omitted",
            "previous_cursor: 3",
            "before_cursor=3",
            "earlier fragments",
        ] {
            assert!(page.contains(fact), "{fact}: {page}");
        }
        assert!(!page.contains("PRIVATE_CANARY") && !page.contains("opaque-ref"));
        for (key, bad) in [
            ("previous_cursor", Value::Null),
            ("direction", json!("sideways")),
        ] {
            let mut malformed = value.clone();
            malformed[key] = bad;
            let result = success_result("transcript", &malformed);
            assert_eq!(result.is_error, Some(true));
            assert!(!serde_json::to_string(&result).unwrap().contains("exact"));
        }
    }

    use super::{error_result, public_success_result, success_result};
    use rmcp::model::CallToolResult;
    use serde_json::{Value, json};

    /// Compact start/resume/list output preserves labels, observed zero and
    /// unknown metrics, names incomplete lineage evidence and hides execution IDs.
    #[test]
    fn display_names_and_usage_remain_honest_in_compact_output() {
        let mut agent = agent_view();
        agent["name"] = json!("工程師 / review");
        agent["usage"] = json!({"input_tokens":0,"output_tokens":null,
            "cache_read_tokens":0,"cache_write_tokens":null,"num_turns":null,
            "usage_source":"token_usage_updated","run_id":"private-execution"});
        agent["usage_cumulative"] =
            json!({"executions":2,"input_tokens":null,"output_tokens":null});
        for tool in ["start", "resume"] {
            let page = text(
                tool,
                &json!({"agent_id":"ag-1","sequence":1,"created":true,"agent":agent}),
            );
            assert!(
                page.contains("- Agent: 工程師 / review (review) · ag-1"),
                "{page}"
            );
            assert!(!page.contains("private-execution"), "{page}");
        }
        let page = text(
            "list_agents",
            &json!({"items":[agent],"total":1,"offset":0,"limit":20,"complete":true}),
        );
        assert!(
            page.contains("- 工程師 / review (review) · ag-1:"),
            "{page}"
        );
        assert!(page.contains("in 0 tokens"), "{page}");
        assert!(
            page.contains("lineage incomplete over 2 execution(s)"),
            "{page}"
        );
        assert!(!page.contains("private-execution"), "{page}");
    }

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
        let home = tempfile::tempdir().unwrap();
        agent_run_store::fixture::initialize(home.path()).unwrap();
        let mut store = agent_run_store::fixture::open(home.path()).unwrap();
        let request = json!({"runtime":"codex","model":"fixture","profile":"review",
            "task":"fixture only","workdir":home.path()});
        let id: crate::domain::AgentId = "ag-20261018-000000-0123456789".parse().unwrap();
        store.conn.execute(
            "INSERT INTO agents(id,runtime,model,profile,task,task_summary,workdir,request_json,status,created_at,config_revision,root_agent_id) VALUES(?1,'codex','fixture','review','fixture only','fixture only',?2,?3,'running',100,'fixture',?1)",
            rusqlite::params![id.as_str(), home.path().to_str().unwrap(), request.to_string()],
        ).unwrap();
        let row = store
            .enqueue(&id, "steer", &json!({"message":"continue"}))
            .unwrap();
        let steer = text("steer", &row);
        assert!(steer.contains("agent-run steer accepted"), "{steer}");
        assert!(steer.contains("steer #1 (pending)"), "{steer}");
        assert!(!steer.contains("presentation_failed"), "{steer}");
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
        let mut named = agent_view();
        named["name"] = json!("Delicate Fox");
        named["elapsed_seconds"] = json!(750);
        named["usage"] = json!({"input_tokens":8_700_000,"output_tokens":21_400});
        named["tool_counts"] = json!({"calls":5,"failed":null,"unknown_results":1});
        let mut finished = agent_view();
        finished["status"] = json!("succeeded");
        finished["finished_at"] = json!(0);
        finished["health"] = json!({"state":"terminal","reason":"idle_without_wait_source"});
        let page = text(
            "list_agents",
            &json!({
                "items": [named, finished], "total": 5, "offset": 0,
                "limit": 2, "next_offset": 2, "complete": false, "revision": 9,
            }),
        );
        assert!(page.contains("2 of 5 matching (offset 0)"), "{page}");
        assert!(
            page.contains("- ag-1: succeeded (running) — glm-user/glm-5.3 profile review · 1970-01-01T00:00:00Z"),
            "{page}"
        );
        assert!(
            page.contains("- Delicate Fox (review) · ag-1: running (running) — glm-user/glm-5.3"),
            "{page}"
        );
        assert!(page.contains("execution ended — use answer"), "{page}");
        assert!(!page.contains("steer it"), "{page}");
        assert!(page.contains("task: \"fix the parser\""), "{page}");
        assert!(
            page.contains("· 12m") && page.contains("in 8.7M out 21k tokens"),
            "{page}"
        );
        assert!(
            page.contains("tools: 5 native calls") && !page.contains("unknown results"),
            "{page}"
        );
        assert!(
            page.contains("next offset: 2 — more matching rows remain"),
            "{page}"
        );
    }

    /// A health diagnosis survives closed projection and is visible without
    /// changing the still-active execution into an invented terminal result.
    #[test]
    fn list_agents_exposes_health_attention() {
        let mut agent = agent_view();
        agent["phase"] = json!("idle");
        agent["health"] = json!({"state":"needs_attention","reason":"idle_without_wait_source"});
        let page = text(
            "list_agents",
            &json!({"items":[agent],"total":1,"offset":0,"limit":20,"complete":true}),
        );
        assert!(page.contains("running (idle)"), "{page}");
        assert!(
            page.contains("health: idle without a wait source — steer it or check the transcript"),
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
                    {"seq": 3, "role": "tool_result", "name": "shell", "content": "stored output", "raw_ref": "private"},

                ],
            }),
        );
        assert!(page.contains("[1] user: please\nfix\tit"), "{page}");
        assert!(
            page.contains("[3] tool_result (\"shell\"): stored output"),
            "{page}"
        );
        assert!(
            !page.contains("raw_ref") && !page.contains("[unknown]"),
            "{page}"
        );
        assert!(
            page.contains("[2] tool_call (\"shell\"): {\"cmd\":1}"),
            "{page}"
        );
        assert!(
            page.contains("next_cursor: 4 — request the next page with this cursor"),
            "{page}"
        );
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

    /// The doc page renders verbatim; the removed quota reads have no renderer.
    #[test]
    fn legacy_and_remaining_tools_render_their_essentials() {
        let doc = text("doc", &json!({"topic": "config", "text": "guide prose\n"}));
        assert_eq!(doc, "guide prose\n");
        for removed in ["models", "capacity_order", "limits"] {
            assert_eq!(
                success_result(removed, &json!({"items": [], "providers": []})).is_error,
                Some(true),
                "{removed}"
            );
        }
    }

    /// Empty catalog states, legacy empties, and errors are explicit.
    #[test]
    fn empty_states_and_errors_are_explicit() {
        let empty_agents = text(
            "list_agents",
            &json!({"items": [], "total": 0, "offset": 0, "complete": true}),
        );
        assert!(empty_agents.contains("0 of 0 matching"), "{empty_agents}");
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

    /// Actionable values are reversible rather than shortened, and an empty
    /// verified answer is stated explicitly.
    #[test]
    fn model_and_answer_empty_fidelity_drops_private_param_metadata() {
        let model = "custom\n\u{202e}{{ data }}";
        assert_eq!(
            serde_json::from_str::<String>(&super::exact_display(model)).unwrap(),
            model
        );
        assert!(!super::exact_display(model).contains('\u{202e}'));
        let empty = text(
            "answer",
            &json!({"agent_id":"ag-1","status":"succeeded",
            "available":true,"inline_complete":true,"content":""}),
        );
        assert!(empty.contains("empty (0 UTF-8 bytes)"));
    }

    /// Renders one public (operator) tool success and returns its text and result.
    fn public_text(tool: &str, value: &Value) -> (String, CallToolResult) {
        let result = public_success_result(tool, value);
        let content = serde_json::to_value(&result.content).unwrap();
        let page = content[0]["text"].as_str().unwrap().to_owned();
        assert_eq!(result.is_error, Some(false), "{tool}: {page}");
        (page, result)
    }

    /// Integration renders only its public identity/observation, with finite
    /// unknown states and no process, credential or arbitrary-agent-field dump.
    #[test]
    fn agent_integration_text_preserves_safe_observation() {
        let mut value = json!({"schema_version":1,"agent":{"agent_id":"ag-1","sequence":2,"runtime":"p","model":"m","profile":"code","status":"running","delivery":null,"private":"SECRET_CANARY"},"communication":{"state":"observed","address":"local/codex/private/session","sampled_at_ms":1}});
        let (page, result) = public_text("agent_integration", &value);
        assert!(
            page.contains("ag-1")
                && page.contains("execution 2")
                && page.contains("local/codex/private/session")
        );
        assert!(!page.contains("SECRET_CANARY"));
        assert!(result.structured_content.is_none());
        value["communication"] = json!({"state":"unknown","reason":"identity_changed"});
        let (page, _) = public_text("agent_integration", &value);
        assert!(page.contains("identity_changed"));
        assert!(!page.contains("local/codex/private/session"));
    }

    /// Maximum escaped fragments remain intact in the MCP frame and schema result.
    #[test]
    fn transcript_exact_mcp_preserves_bounded_structured_fragment() {
        let text = "\u{1}".repeat(32 * 1024);
        let value = json!({"version":1,"store_id":"store_00000000000000000000000000000000",
            "selector":{"agent_id":"ag-20260826-120000-0123456789","sequence":1,"execution_ref":format!("ex1_{}","0".repeat(64)),"pin_id":"pin_00000000000040008000000000000000"},
            "cursor":"0","upper_seq":"9007199254740993","offset":0,
            "message":{"seq":"9007199254740993","sha256":"0".repeat(64),"total_bytes":32768,"start":0,"end":32768,"text":text,"complete":true},
            "next_cursor":"9007199254740993","next_offset":0,"snapshot_complete":true});
        let result = success_result("transcript_exact", &value);
        assert_ne!(result.is_error, Some(true));
        assert_eq!(result.structured_content.as_ref(), Some(&value));
        let wire = json!({"jsonrpc":"2.0","id":"i".repeat(agent_run_domain::observation::MAX_RPC_ID_BYTES-2),"result":result});
        assert!(
            serde_json::to_vec(&wire).unwrap().len()
                < agent_run_domain::observation::SOCKET_MAX_FRAME_BYTES
        );
    }
}
