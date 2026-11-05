//! Bounded, untrusted reports sent from a running worker to its orchestrator.

use crate::{Error, Result, domain::AgentId};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::fmt;

/// Private worker MCP server identity exposed only to launched subagents.
pub const SERVER_NAME: &str = "agent_run_worker";

/// Private native handshake; it is not an advertised operator or worker tool.
pub const CATALOG_METHOD: &str = "worker/catalog";

/// The reviewed version of the five-tool private pool-capable server contract.
pub const POOL_CATALOG_VERSION: u32 = 1;
/// Reviewed explicit-completion catalog; legacy pool/research catalogs stay v1.
pub const FINISH_CATALOG_VERSION: u32 = 4;
/// Staged catalog with maintenance checkpoints; it is not yet served or admitted.
pub const MAINTENANCE_PAUSE_CATALOG_VERSION: u32 = 5;

/// Fingerprints the actual embedded private catalog served by this binary.
/// The digest includes schemas/effect hints, not runtime capability material.
pub fn pool_catalog_digest() -> String {
    crate::canonical::sha256_hex(&serde_json::json!(crate::tools::worker_tools_json()), true)
}

/// Nonsecret launch marker selecting the report-capable research worker surface.
/// Only the supervisor sets it from frozen authority; the broker independently
/// checks authority before writing, so the marker itself grants no access.
pub const RESEARCH_ENV: &str = "AGENT_RUN_WORKER_RESEARCH";

/// Nonsecret supervisor-selected explicit completion catalog marker. The
/// broker checks frozen admission and capability independently of this flag.
pub const FINISH_ENV: &str = "AGENT_RUN_WORKER_FINISH";
/// Supervisor-selected value of [`FINISH_ENV`] for the v5 maintenance catalog.
/// Existing supervisors set only `0` or `1`; this value does not itself grant authority.
pub const MAINTENANCE_PAUSE_ENV_VALUE: &str = "5";

/// The broker's pre-SQL verdict on one finish summary against the transient
/// launch-secret context its verified supervisor installed for that exact
/// attempt. Only a clean verdict may create a first finish intent; an exact
/// replay of an already accepted intent is answered whatever the verdict.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FinishGuard {
    /// No installed context for the attempt (supervisor not yet connected,
    /// reconnecting, gone, or a broker restart): refuse without intent; the
    /// worker may retry the same finish.
    Unavailable,
    /// The summary carries a known launch secret: refuse without intent.
    Secret,
    /// Checked against the installed context and free of launch secrets.
    Clean,
}

/// Trusted runtime boundary for a supervisor-admitted explicit continuation.
/// Earlier finishes stay immutable; this new execution uses its own callback.
pub const EXPLICIT_RESUME_BOUNDARY: &str = "Agent Run runtime execution boundary: this is a newly admitted explicit continuation in the retained native session, with a fresh execution and private finish binding. Earlier finish receipts and summaries belong to earlier executions and remain immutable there. They do not close this execution or forbid its newly assigned work. Complete this execution's task, then call its private finish with a NEW final summary; identical-payload retry applies only after this execution's own finish has been accepted. Keep all existing permissions and secret protections.";

/// Keep false completion flags absent from historical canonical request JSON.
pub fn legacy_completion(enabled: &bool) -> bool {
    !enabled
}

/// New provider admissions use callback completion. Historical storage and
/// restored provider requests explicitly keep their original legacy contract.
pub const fn default_explicit_finish() -> bool {
    true
}

/// Worker-declared terminal verdict; cleanup still determines public success.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum FinishStatus {
    /// The assigned task is complete, subject to answer and cleanup verification.
    #[default]
    Done,
    /// Progress requires external input; terminal but never successful.
    Blocked,
    /// The worker declares failure; terminal but never successful.
    Failed,
}

/// Exact final answer supplied by this attempt, not the last assistant text.
/// Retries are keyed by the attempt and complete canonical payload; no caller
/// agent identifier, filesystem path or optional authority is accepted.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FinishRequest {
    /// Nonblank final answer, at most 64 KiB UTF-8; only newline/tab controls.
    pub summary: String,
    /// Defaults to done. Blocked/failed retain the answer with non-success.
    #[serde(default)]
    pub status: FinishStatus,
}

impl FinishRequest {
    /// Reject blank, oversized or control-bearing answers before persistence.
    pub fn validate(&self) -> Result<()> {
        bounded_text("finish summary", &self.summary, 65536)
    }

    /// Check the exact summary as JSON against frozen output_schema. External
    /// file/HTTP resolving is disabled; errors never echo schema or answer text.
    pub fn validate_schema(&self, schema: Option<&serde_json::Map<String, Value>>) -> Result<()> {
        self.validate()?;
        if let Some(schema) = schema {
            let validator = finish_schema(schema)?;
            let instance: Value = serde_json::from_str(&self.summary).map_err(|_| {
                Error::Validation("finish summary must be JSON for output_schema".into())
            })?;
            if !validator.is_valid(&instance) {
                return Err(Error::Validation(
                    "finish summary does not match output_schema".into(),
                ));
            }
        }
        Ok(())
    }
}

/// Compile a local-only schema before admission/callback. Formats are checked;
/// unsupported/malformed schemas return static errors, without fetching refs.
pub fn finish_schema(schema: &serde_json::Map<String, Value>) -> Result<jsonschema::Validator> {
    jsonschema::options()
        .should_validate_formats(true)
        .build(&Value::Object(schema.clone()))
        .map_err(|_| {
            Error::Validation(
                "explicit finish output_schema is invalid or requires external resolution".into(),
            )
        })
}

/// Content-free durable callback receipt. It is intent, not cleanup proof.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FinishReceipt {
    /// Digest of the exact canonical callback payload for identical retries.
    pub sha256: String,
    /// True when the same attempt already accepted this exact payload.
    pub duplicate: bool,
}

/// A worker's checkpoint for one runtime-issued maintenance challenge.
/// The private transport binds the attempt; this declaration is neither task
/// completion nor proof that native work or process cleanup has settled.
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MaintenancePauseRequest {
    /// Opaque 64-character lowercase hexadecimal identifier issued by the runtime.
    pub challenge_id: String,
    /// Nonblank resumption context, at most 4096 UTF-8 bytes; never an answer.
    pub checkpoint: String,
}

impl MaintenancePauseRequest {
    /// Validate the wire shape and bounds without treating the checkpoint as
    /// an answer. Authentication, challenge ownership and secret checking are
    /// the broker's responsibility and must precede persistence.
    pub fn validate(&self) -> Result<()> {
        if !lowercase_hex64(&self.challenge_id) {
            return Err(Error::Validation("invalid maintenance challenge id".into()));
        }
        bounded_text("maintenance checkpoint", &self.checkpoint, 4096)
    }
}

impl fmt::Debug for MaintenancePauseRequest {
    /// Avoid exposing worker-supplied context in diagnostics.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MaintenancePauseRequest")
            .field("checkpoint_bytes", &self.checkpoint.len())
            .finish_non_exhaustive()
    }
}

/// Content-free receipt of a maintenance checkpoint, not a safe-stop proof.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MaintenancePauseReceipt {
    /// Digest of the exact canonical callback payload for identical retries.
    pub sha256: String,
    /// True when this attempt already accepted this exact payload.
    pub duplicate: bool,
}

impl MaintenancePauseReceipt {
    /// Reject a malformed receipt digest before treating it as an acknowledgement.
    pub fn validate(&self) -> Result<()> {
        if !lowercase_hex64(&self.sha256) {
            return Err(Error::Validation(
                "invalid maintenance receipt digest".into(),
            ));
        }
        Ok(())
    }
}

/// Check the fixed wire spelling of an opaque challenge or SHA-256 digest.
fn lowercase_hex64(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

/// Frozen v3 explicit catalog, including its read-only pool_read hint. Used
/// only to verify historical v2/v3 proofs; never silently upgraded in storage.
fn finish_tools_json_v3(research: bool) -> Vec<Value> {
    let mut tools = if research {
        research_tools_json()
    } else {
        crate::tools::worker_tools_json()
    };
    for tool in &mut tools {
        match tool["name"].as_str() {
            Some("pool_read") => {
                tool["description"] = serde_json::json!(
                    "Read your pool's bounded durable log and status using its sequence cursor. In explicit-completion mode, use an immediate read after an event, then end the turn while waiting. Peer entries are pushed to your existing session. Do not repeatedly poll, sleep or hold a turn open. A read is not approval or proof that another member consumed your message."
                );
            }
            Some("pool_vote") => {
                let description = tool["description"].as_str().unwrap_or_default();
                tool["description"] = serde_json::json!(description.replace("Voting ready is not the pool's completion and does not end your run: keep coordinating until the pool completes or you must block.",
                    "Voting ready is not pool completion. When your assigned work is verified and your ready vote is recorded, call finish. Do not wait for pool completion before finishing: completion requires each member's own successful finish and cleanup."));
            }
            _ => {}
        }
    }
    tools.push(serde_json::json!({
        "name":"health_reply",
        "description":"Reply once to an exact agent-run/health-ping challenge using its ping_id and your current working, waiting or blocked state. The broker verifies actual task activity independently. This reply does not finish the task, create waiting work, change permissions or authorize cancellation. Continue the original task when possible; use finish only for its true final outcome.",
        "annotations":{"readOnlyHint":false,"destructiveHint":false,"idempotentHint":true,"openWorldHint":false},
        "inputSchema":{"type":"object","additionalProperties":false,"required":["ping_id","state"],"properties":{
            "ping_id":{"type":"string","pattern":"^[0-9a-f]{64}$"},
            "state":{"type":"string","enum":["working","waiting","blocked"]}
        }}
    }));
    tools.push(serde_json::json!({

        "name":"finish",
        "description":"End your current run with its final verified summary. Ending a turn only waits for notifications; it does not finish. Call only when done, blocked or failed. The summary is immutable. Retry exactly the same payload after an uncertain receipt; a different payload conflicts. The receipt acknowledges durable finish intent, not process cleanup or owner approval. Never include credentials.",
        "annotations":{"readOnlyHint":false,"destructiveHint":false,"idempotentHint":true,"openWorldHint":false},
        "inputSchema":{"type":"object","additionalProperties":false,"required":["summary"],"properties":{
            "summary":{"type":"string","minLength":1,"maxLength":65536},
            "status":{"type":"string","enum":["done","blocked","failed"],"default":"done"}
        }}
    }));
    tools
}

/// Current v4 catalog declares automatic empty-read protection truthfully:
/// pool_read may emit a fixed attention notice and an active-turn correction.
/// Legacy v1/v2/v3 bytes/digests remain separately recognized and unchanged.
pub fn finish_tools_json(research: bool) -> Vec<Value> {
    let mut tools = finish_tools_json_v3(research);
    for tool in &mut tools {
        if tool["name"] == "pool_read" {
            tool["annotations"]["readOnlyHint"] = serde_json::json!(false);
            let description = tool["description"].as_str().unwrap_or("");
            tool["description"] = serde_json::json!(format!(
                "{description} Repeated empty reads of an unchanged log trigger one durable attention notice and an active-turn corrective message, with bounded backpressure. This never finishes or cancels your logical run."
            ));
        }
    }
    tools
}

/// Fingerprint the real mode/role-bound completion catalog, excluding secrets.
pub fn finish_catalog_digest(research: bool) -> String {
    crate::canonical::sha256_hex(&serde_json::json!(finish_tools_json(research)), true)
}

/// The v5 catalog extends v4 for maintenance-aware explicit-completion workers.
/// A callback receipt alone never authorizes stopping or resuming a process.
pub fn maintenance_pause_tools_json(research: bool) -> Vec<Value> {
    let mut tools = finish_tools_json(research);
    tools.push(serde_json::json!({
        "name":"maintenance_pause",
        "description":"Respond only to a runtime-issued maintenance challenge with the context needed to continue your current work. Never include credentials. The receipt records checkpoint intent; it does not prove completion, settled tools, process cleanup or restart. If the receipt is uncertain, retry the exact same payload.",
        "annotations":{"readOnlyHint":false,"destructiveHint":false,"idempotentHint":true,"openWorldHint":false},
        "inputSchema":{"type":"object","additionalProperties":false,"required":["challenge_id","checkpoint"],"properties":{
            "challenge_id":{"type":"string","minLength":64,"maxLength":64,"pattern":"^[0-9a-f]{64}$","description":"Exact identifier from the runtime's pending maintenance challenge."},
            "checkpoint":{"type":"string","minLength":1,"maxLength":4096,"description":"Nonblank context for resuming the current work, at most 4096 UTF-8 bytes."}
        }}
    }));
    tools
}

/// Fingerprint the opt-in role-bound maintenance catalog.
pub fn maintenance_pause_catalog_digest(research: bool) -> String {
    crate::canonical::sha256_hex(
        &serde_json::json!(maintenance_pause_tools_json(research)),
        true,
    )
}

/// The research surface retains safe completion/pool messages and adds only a
/// confined filesystem tool. The ordinary five-tool surface stays unchanged.
pub fn research_tools_json() -> Vec<Value> {
    let mut tools = crate::tools::worker_tools_json();
    tools.push(serde_json::json!({
        "name":"save_report",
        "description":"Save a UTF-8 research report in the assigned report directory (workdir). Accepts one .md, .txt or .json filename, no directory/path, traversal, symlink target, command or overwrite. Identical existing content is a duplicate. Returns filename, bytes and SHA-256; not proof of task completion.",
        "annotations":{"readOnlyHint":false,"destructiveHint":false,"idempotentHint":false,"openWorldHint":false},
        "inputSchema":{"type":"object","additionalProperties":false,"required":["filename","content"],"properties":{
            "filename":{"type":"string","maxLength":128},
            "content":{"type":"string","minLength":1,"maxLength":65536}
        }}
    }));
    tools
}

/// Fingerprints the exact research worker schemas without capability secrets.
pub fn research_catalog_digest() -> String {
    crate::canonical::sha256_hex(&serde_json::json!(research_tools_json()), true)
}

/// Accepts only reviewed pool-capable catalogs. The research catalog includes
/// every ordinary pool tool; an unknown catalog never satisfies enrollment.
pub fn known_pool_catalog(version: u32, digest: &str) -> bool {
    (version == POOL_CATALOG_VERSION
        && (digest == pool_catalog_digest() || digest == research_catalog_digest()))
        || (version == 2
            && [false, true].into_iter().any(|research| {
                let old: Vec<_> = finish_tools_json_v3(research)
                    .into_iter()
                    .filter(|tool| tool["name"] != "health_reply")
                    .collect();
                crate::canonical::sha256_hex(&serde_json::json!(old), true) == digest
            }))
        || (version == 3
            && [false, true].into_iter().any(|research| {
                crate::canonical::sha256_hex(
                    &serde_json::json!(finish_tools_json_v3(research)),
                    true,
                ) == digest
            }))
        || (version == FINISH_CATALOG_VERSION
            && (digest == finish_catalog_digest(false) || digest == finish_catalog_digest(true)))
        || (version == MAINTENANCE_PAUSE_CATALOG_VERSION
            && (digest == maintenance_pause_catalog_digest(false)
                || digest == maintenance_pause_catalog_digest(true)))
}

/// A bounded report request. Its directory comes from frozen authority; a
/// model cannot supply a directory, execution or overwrite option.
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SaveReportRequest {
    /// Single non-hidden report filename, at most 128 UTF-8 bytes.
    pub filename: String,
    /// Nonblank UTF-8 report text, at most 64 KiB; never diagnostic contents.
    pub content: String,
}
impl SaveReportRequest {
    /// Rejects absolute/traversing paths, both separators, hidden/control names
    /// and non-report extensions. Checks content bytes before filesystem effects.
    pub fn validate(&self) -> Result<()> {
        let name = &self.filename;
        if name.is_empty()
            || name.len() > 128
            || name.starts_with('.')
            || name.contains('/')
            || name.as_bytes().contains(&92)
            || name.chars().any(char::is_control)
            || ![".md", ".txt", ".json"]
                .iter()
                .any(|suffix| name.ends_with(suffix))
        {
            return Err(Error::Validation("report filename must be a single non-hidden .md, .txt or .json name inside the assigned directory".into()));
        }
        bounded_text("report content", &self.content, 65536)
    }
}

/// Content-free receipt for atomic report publication or an identical replay.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SaveReportReceipt {
    /// Validated basename; no caller-selected directory.
    pub filename: String,
    /// Exact published UTF-8 byte count.
    pub bytes: u64,
    /// Saved file SHA-256, for verification without repeating contents.
    pub sha256: String,
    /// True only when an existing regular file held identical bytes.
    pub duplicate: bool,
}

/// Hidden attempt-bound catalog proof sent only by the native worker server.
/// No Debug representation is provided because the token is ephemeral secret.
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WorkerCatalogProof {
    /// Exact execution whose launcher supplied the capability.
    pub run_id: AgentId,
    /// Exact owned attempt, not a moving lineage alias.
    pub attempt_id: String,
    /// Hidden transient worker capability; persisted only as a hash.
    pub token: String,
    /// Reviewed catalog contract version.
    pub version: u32,
    /// SHA-256 of the actual five-tool embedded catalog.
    pub digest: String,
}

/// Private broker method accepted only from an authenticated worker capability.
pub const METHOD: &str = "worker/notify";
/// Launch-time inherited names carrying a private home or authenticated system
/// endpoint, followed by the exact run, attempt, and secret.
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

/// Accepts an idempotency key of 1–128 ASCII letters, digits, `_`, `-` or `.`.
pub(crate) fn request_key(key: &str) -> Result<()> {
    if key.is_empty()
        || key.len() > 128
        || !key
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b"-_.".contains(&b))
    {
        return Err(Error::Validation(
            "request_id must be 1–128 safe ASCII bytes".into(),
        ));
    }
    Ok(())
}

/// Accepts nonblank text of at most `max` UTF-8 bytes whose only controls are
/// newline and tab, so relay frames and terminals cannot be inflated or driven.
pub(crate) fn bounded_text(label: &str, text: &str, max: usize) -> Result<()> {
    if text.trim().is_empty()
        || text.len() > max
        || text
            .chars()
            .any(|c| c.is_control() && c != '\n' && c != '\t')
    {
        return Err(Error::Validation(format!(
            "{label} must be 1–{max} UTF-8 bytes without unsupported control characters"
        )));
    }
    Ok(())
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
        request_key(&self.request_id)?;
        bounded_text("message", &self.message, 2048)?;
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

/// Private broker method routing one named worker tool with hidden credentials.
pub const TOOL_METHOD: &str = "worker/call";

/// One fixed private worker tool name.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WorkerTool {
    /// Declare this attempt's immutable final summary through its private capability.
    Finish,
    /// Record a checkpoint for the exact requested maintenance challenge.
    MaintenancePause,
    /// Answer only a current attempt-owned liveness challenge.
    HealthReply,
    /// Report a material finding to this run's orchestrator.
    Notify,
    /// Atomically save a report within frozen research workdir authority.
    SaveReport,
    /// Ordinary informational chat to the pool.
    PoolPost,
    /// Bounded read of the pool log and derived status.
    PoolRead,
    /// Propose one result snapshot for unanimous agreement.
    PoolPropose,
    /// Vote ready, block, or revoke on the current proposal.
    PoolVote,
}

impl WorkerTool {
    /// The stable tool name shared by the fixed catalog and the dispatcher.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Finish => "finish",
            Self::MaintenancePause => "maintenance_pause",
            Self::HealthReply => "health_reply",
            Self::Notify => "notify_orchestrator",
            Self::SaveReport => "save_report",
            Self::PoolPost => "pool_post",
            Self::PoolRead => "pool_read",
            Self::PoolPropose => "pool_propose",
            Self::PoolVote => "pool_vote",
        }
    }

    /// Decodes exactly one catalog name; anything else is not a worker tool.
    pub fn parse(name: &str) -> Option<Self> {
        [
            (Self::Finish.as_str(), Self::Finish),
            (Self::MaintenancePause.as_str(), Self::MaintenancePause),
            (Self::HealthReply.as_str(), Self::HealthReply),
            (Self::Notify.as_str(), Self::Notify),
            (Self::SaveReport.as_str(), Self::SaveReport),
            (Self::PoolPost.as_str(), Self::PoolPost),
            (Self::PoolRead.as_str(), Self::PoolRead),
            (Self::PoolPropose.as_str(), Self::PoolPropose),
            (Self::PoolVote.as_str(), Self::PoolVote),
        ]
        .into_iter()
        .find(|(candidate, _)| *candidate == name)
        .map(|(_, tool)| tool)
    }
}

/// Private broker envelope for one named worker tool; the credentials are the
/// same hidden capability fields as [`WorkerCall`] and are never echoed back.
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WorkerToolCall {
    /// Exact execution that owns the capability.
    pub run_id: AgentId,
    /// Exact active attempt that owns the capability.
    pub attempt_id: String,
    /// Ephemeral bearer secret, sent only on the private broker route.
    pub token: String,
    /// One fixed catalog tool name.
    pub tool: String,
    /// The tool's strict object input, decoded and validated by the broker.
    pub input: Value,
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
    /// New public admissions default to finish, while omitted historical mode
    /// and explicit compatibility requests remain legacy on restoration.
    #[test]
    fn new_admissions_and_history_have_distinct_defaults() {
        let value = serde_json::json!({"provider":"codex","model":"fixture","profile":"implement","task":"task","workdir":"/tmp"});
        let new: crate::ProviderStartRequest = serde_json::from_value(value.clone()).unwrap();
        assert!(new.explicit_finish);
        let history = crate::ProviderStartRequest::from_history(value.clone()).unwrap();
        assert!(!history.explicit_finish);
        assert_eq!(
            serde_json::to_value(&history).unwrap()["explicit_finish"],
            false
        );
        let mut compatible = value;
        compatible["explicit_finish"] = serde_json::json!(false);
        let legacy: crate::ProviderStartRequest = serde_json::from_value(compatible).unwrap();
        assert!(!legacy.explicit_finish);
        let wire: crate::ProviderStartRequest =
            serde_json::from_value(serde_json::to_value(legacy).unwrap()).unwrap();
        assert!(!wire.explicit_finish);
        let restored =
            crate::ProviderStartRequest::from_history(serde_json::to_value(new).unwrap()).unwrap();
        assert!(restored.explicit_finish);
    }
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

#[cfg(test)]
mod completion_catalog_history_tests {
    use super::*;
    /// The previously admitted ordinary-worker v2 fingerprint stays
    /// accepted. v3 preserves its old effect hints; v4 truthfully advertises
    /// the new guard without changing any historical fingerprint.
    #[test]
    fn guarded_reads_preserve_v2_v3_catalog_fingerprints() {
        assert!(known_pool_catalog(
            2,
            "a7ca2499ed9d72fa957780418d2bd45674db3ca9394b5d97fe0270012e6a3089"
        ));
        for research in [false, true] {
            let old = finish_tools_json_v3(research);
            let digest = crate::canonical::sha256_hex(&serde_json::json!(old), true);
            assert_eq!(
                digest,
                if research {
                    "d8ffbe5925a0cb29b7d0ff1a0dcfeb3e5c610de586d3cb0fee40c1964d382a14"
                } else {
                    "57ba0c3ed7f3a689488e78456180031c8ca1df57e8a62e43466f5d0c5549a212"
                },
                "previous compiled native catalog fingerprint must remain exact"
            );
            assert!(known_pool_catalog(3, &digest));
            assert_ne!(digest, finish_catalog_digest(research));
            assert_eq!(
                old.iter().find(|t| t["name"] == "pool_read").unwrap()["annotations"]["readOnlyHint"],
                true
            );
            assert_eq!(
                finish_tools_json(research)
                    .iter()
                    .find(|t| t["name"] == "pool_read")
                    .unwrap()["annotations"]["readOnlyHint"],
                false
            );
        }
    }

    /// Every previously served role/version pair retains its exact fingerprint;
    /// v5 proofs are recognized while the default served catalog remains v4.
    #[test]
    fn maintenance_catalog_preserves_history_without_activation() {
        let historical = [
            [
                "2f8503bdc2be9a8370dbea29de8e420a0de9c95970e6a115fd0f4cd33cbe10f5",
                "a7ca2499ed9d72fa957780418d2bd45674db3ca9394b5d97fe0270012e6a3089",
                "57ba0c3ed7f3a689488e78456180031c8ca1df57e8a62e43466f5d0c5549a212",
                "35c88704bd4cf7a3485717234e1d9446c4103adf14cb9b0cbefa17f9b9e7ebf0",
            ],
            [
                "041c40529dee0630ce31a5ba61090136e801750716248a7044373f0149ddd484",
                "6b2361e439c781a352e4fb578fe273727dc326c5bc5378854588b68620bd7f12",
                "d8ffbe5925a0cb29b7d0ff1a0dcfeb3e5c610de586d3cb0fee40c1964d382a14",
                "1ef92ae3e209e1c3ef9476f835104c84ad0fb94ae5324230c07841ef66c10023",
            ],
        ];
        assert_eq!(FINISH_CATALOG_VERSION, 4);
        assert_eq!(MAINTENANCE_PAUSE_CATALOG_VERSION, 5);
        for (research, digests) in [false, true].into_iter().zip(historical) {
            for (version, digest) in (1..=4).zip(digests) {
                assert!(known_pool_catalog(version, digest));
            }
            let live = finish_tools_json(research);
            assert_eq!(finish_catalog_digest(research), digests[3]);
            let mut staged = maintenance_pause_tools_json(research);
            assert_eq!(staged.pop().unwrap()["name"], "maintenance_pause");
            assert_eq!(staged, live);
            assert!(known_pool_catalog(
                MAINTENANCE_PAUSE_CATALOG_VERSION,
                &maintenance_pause_catalog_digest(research)
            ));
            assert!(live.iter().all(|tool| tool["name"] != "maintenance_pause"));
        }
        assert_eq!(
            WorkerTool::parse("maintenance_pause"),
            Some(WorkerTool::MaintenancePause)
        );
    }

    /// Checkpoint shape, byte limits and receipt digests are strict; a checkpoint
    /// remains private even when a request is formatted for diagnostics.
    #[test]
    fn maintenance_pause_validates_boundaries_and_redacts_debug() {
        let challenge = "a".repeat(64);
        let valid =
            serde_json::json!({"challenge_id":challenge,"checkpoint":"resume here\nnext step"});
        let request: MaintenancePauseRequest = serde_json::from_value(valid.clone()).unwrap();
        assert!(request.validate().is_ok());
        assert!(!format!("{request:?}").contains("resume here"));
        for value in [
            serde_json::json!({"challenge_id":"a".repeat(64)}),
            serde_json::json!({"checkpoint":"resume"}),
            serde_json::json!({"challenge_id":"a".repeat(64),"checkpoint":"resume","status":"done"}),
            serde_json::json!({"challenge_id":123,"checkpoint":"resume"}),
        ] {
            assert!(serde_json::from_value::<MaintenancePauseRequest>(value).is_err());
        }
        for bad in [
            "a".repeat(63),
            "a".repeat(65),
            "A".repeat(64),
            "g".repeat(64),
            "é".repeat(32),
        ] {
            let mut invalid = request.clone();
            invalid.challenge_id = bad;
            assert!(invalid.validate().is_err());
        }
        for bad in [
            "".to_owned(),
            " \n\t".to_owned(),
            "bad\0text".to_owned(),
            "é".repeat(2049),
        ] {
            let mut invalid = request.clone();
            invalid.checkpoint = bad;
            assert!(invalid.validate().is_err());
        }
        let mut boundary = request.clone();
        boundary.checkpoint = "é".repeat(2048);
        assert!(boundary.validate().is_ok());
        let valid_receipt = serde_json::json!({"sha256":"f".repeat(64),"duplicate":false});
        let receipt: MaintenancePauseReceipt = serde_json::from_value(valid_receipt).unwrap();
        assert!(receipt.validate().is_ok());
        let mut bad_receipt = receipt;
        bad_receipt.sha256 = "X".repeat(64);
        assert!(bad_receipt.validate().is_err());
        assert!(
            serde_json::from_value::<MaintenancePauseReceipt>(serde_json::json!({
                "sha256":"f".repeat(64), "duplicate":false, "finished":true
            }))
            .is_err()
        );
        let catalog = maintenance_pause_tools_json(false);
        let schema = &catalog.last().unwrap()["inputSchema"];
        let validator = jsonschema::validator_for(schema).unwrap();
        assert!(validator.is_valid(&valid));
        assert!(!validator.is_valid(&serde_json::json!({
            "challenge_id":"a".repeat(64), "checkpoint":"resume", "status":"done"
        })));
    }
}
