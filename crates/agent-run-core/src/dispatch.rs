//! One tool inventory and strict argument decoder for every public transport.
use crate::{
    Result, agent_identity,
    broker_writer::BROKER_BUDGET,
    domain::{AgentId, OrchestratorRef},
    error::invalid,
    service::{Query, Service},
};
pub use agent_run_domain::tools::{is_tool, tool, tools_json};
use agent_run_domain::{
    storage::WriteAuthority,
    worker::{WorkerCatalogProof, known_worker_catalog},
};
use agent_run_store::{
    ops::{
        AcceptWorkerFinish, NotifyOrchestrator, PoolOutcome, PoolWrite, PoolWriteSpec,
        RegisterWorkerCatalog,
    },
    writer::{CatalogPreflight, StoreReader},
};
use rusqlite::OptionalExtension;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::time::Duration;

/// Maximum encoded CLI transcript page, matching the existing 64 MiB native-history input bound.
pub const CLI_TRANSCRIPT_MAX_PAGE_BYTES: usize = 64 * 1024 * 1024;

/// Socket-only retry code for a transcript page changed between fragments.
pub const CLI_TRANSCRIPT_PAGE_CHANGED_CODE: &str = "TranscriptPageChanged";

/// Returns the one domain-owned, Python-compatible public discovery table.
pub fn tools() -> Vec<Value> {
    tools_json()
}
fn args<T: serde::de::DeserializeOwned>(raw: Value) -> Result<T> {
    if !raw.is_object() {
        return Err(invalid("tool arguments must be an object"));
    }
    serde_json::from_value(raw)
        .map_err(|_| invalid("unknown, missing, or incorrectly typed tool argument"))
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
/// Stable agent selection with an optional exact historical execution.
struct Id {
    /// Root identity or historical alias for the lineage.
    agent_id: AgentId,
    /// Exact execution within that lineage; omission selects the latest.
    #[serde(default)]
    run_id: Option<AgentId>,
}

/// Socket-only CLI answer page and the opaque fence from the initial selection.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct CliAnswerChunk {
    /// Exact lineage, execution sequence, artifact digest and byte cursor.
    query: agent_run_domain::observation::AnswerChunkQuery,
    /// Fingerprint of the originally verified execution and proof.
    proof_token: agent_run_domain::Sha256Digest,
}

/// Socket-only identity of one selected transcript execution.
#[derive(Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct CliTranscriptSelection {
    /// Canonical public lineage identity.
    agent_id: AgentId,
    /// Exact retained execution fixed at viewer startup.
    run_id: AgentId,
    /// Immutable execution receipt checked on every read.
    sequence: std::num::NonZeroU32,
}

/// One bounded fragment of a single serialized transcript page.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct CliTranscriptPage {
    /// Selected execution whose identity must still match.
    selection: CliTranscriptSelection,
    /// Existing transcript page options and validation.
    query: agent_run_domain::transcript::TranscriptQuery,
    /// Read the same retained lineage as the ordinary unpinned CLI viewer.
    lineage: bool,
    /// Return exact-run recovery boundaries, never a filesystem path.
    recovery: bool,
    /// UTF-8 byte offset within the serialized page.
    offset_bytes: u64,
    /// Hash returned by the first fragment; required for every later fragment.
    expected_sha256: Option<agent_run_domain::Sha256Digest>,
}

/// Resolves an exact selection and refuses missing, mismatched or changed receipts.
fn cli_transcript_row(
    store: &StoreReader,
    selection: &CliTranscriptSelection,
) -> Result<crate::state::Record> {
    let row = agent_identity::resolve(store, &selection.agent_id, Some(&selection.run_id))?;
    if row.id != selection.run_id
        || row.root_agent_id != selection.agent_id
        || row.sequence != selection.sequence.get()
    {
        return Err(crate::Error::ExecutionChanged);
    }
    Ok(row)
}

/// Checks candidate row sizes in the caller's page snapshot, using SQLite's
/// length metadata rather than copying unbounded legacy content into Rust.
fn check_cli_transcript_budget(
    store: &StoreReader,
    row: &crate::state::Record,
    request: &CliTranscriptPage,
) -> Result<()> {
    let blocks = request.query.view == agent_run_domain::transcript::TranscriptView::Blocks;
    let reverse =
        blocks && (request.query.tail_blocks.is_some() || request.query.before_cursor.is_some());
    let (column, id) = if request.lineage {
        ("root_agent_id", &row.root_agent_id)
    } else {
        ("agent_id", &row.id)
    };
    let (comparison, order, cursor) = if reverse {
        ("<", "DESC", request.query.before_cursor.unwrap_or(i64::MAX))
    } else {
        (">", "ASC", request.query.cursor)
    };
    let scan = if blocks {
        agent_run_store::transcript::MAX_BLOCK_SCAN_ROWS + 1
    } else {
        request.query.limit + 1
    };
    let sql = format!(
        "SELECT COALESCE(MAX(content_bytes),0),COALESCE(SUM(metadata_bytes),0) FROM (
        SELECT octet_length(content) AS content_bytes,
          COALESCE(octet_length(name),0)+COALESCE(octet_length(raw_ref),0)+
          COALESCE(octet_length(error_source),0)+COALESCE(octet_length(role),0)+
          COALESCE(octet_length(agent_id),0)+COALESCE(octet_length(attempt_id),0) AS metadata_bytes
        FROM messages WHERE {column}=?1 AND seq{comparison}?2 ORDER BY seq {order} LIMIT ?3)"
    );
    let (content, metadata): (i64, i64) = store.conn().query_row(
        &sql,
        rusqlite::params![id.as_str(), cursor, scan as i64],
        |row| Ok((row.get(0)?, row.get(1)?)),
    )?;
    // Both raw and block readers inspect one excluded neighbor for boundaries.
    let (neighbor_comparison, neighbor_order) = if reverse {
        (">=", "ASC")
    } else {
        ("<=", "DESC")
    };
    let neighbor_sql = format!("SELECT COALESCE(octet_length(name),0)+COALESCE(octet_length(raw_ref),0)+
        COALESCE(octet_length(error_source),0)+COALESCE(octet_length(role),0)+
        COALESCE(octet_length(agent_id),0)+COALESCE(octet_length(attempt_id),0)
        FROM messages WHERE {column}=?1 AND seq{neighbor_comparison}?2 ORDER BY seq {neighbor_order} LIMIT 1");
    let neighbor: Option<i64> = store
        .conn()
        .query_row(
            &neighbor_sql,
            rusqlite::params![id.as_str(), cursor],
            |row| row.get(0),
        )
        .optional()?;
    if metadata.saturating_add(neighbor.unwrap_or(0)) > CLI_TRANSCRIPT_MAX_PAGE_BYTES as i64
        || (!blocks && content > CLI_TRANSCRIPT_MAX_PAGE_BYTES as i64)
    {
        return Err(crate::Error::Runtime(
            "transcript page exceeds 64 MiB limit".into(),
        ));
    }
    Ok(())
}

/// Accumulates at most the transcript budget while serde serializes a page.
#[derive(Default)]
struct BoundedTranscriptJson {
    bytes: Vec<u8>,
    exceeded: bool,
}

impl std::io::Write for BoundedTranscriptJson {
    /// Rejects bytes before growing the buffer beyond the configured bound.
    fn write(&mut self, input: &[u8]) -> std::io::Result<usize> {
        let Some(required) = self.bytes.len().checked_add(input.len()) else {
            self.exceeded = true;
            return Err(std::io::Error::other("transcript page exceeds limit"));
        };
        if required > CLI_TRANSCRIPT_MAX_PAGE_BYTES {
            self.exceeded = true;
            return Err(std::io::Error::other("transcript page exceeds limit"));
        }
        if required > self.bytes.capacity() {
            let target = required
                .max(self.bytes.capacity().saturating_mul(2))
                .clamp(4096, CLI_TRANSCRIPT_MAX_PAGE_BYTES);
            self.bytes.reserve_exact(target - self.bytes.len());
        }
        self.bytes.extend_from_slice(input);
        Ok(input.len())
    }

    /// In-memory output has nothing to flush.
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

/// Rebuilds one existing transcript page and returns a bounded fragment. Later
/// fragments fail if the complete serialized page changed since the first read.
fn cli_transcript_page(service: &Service, request: CliTranscriptPage) -> Result<Value> {
    cli_transcript_page_observed(service, request, || {})
}

/// Keeps budget, identity and page reads in one snapshot. The observer allows
/// tests to commit another connection after preflight, before materialization.
fn cli_transcript_page_observed(
    service: &Service,
    request: CliTranscriptPage,
    after_budget: impl FnOnce(),
) -> Result<Value> {
    request.query.validate()?;
    if (request.offset_bytes == 0) != request.expected_sha256.is_none() {
        return Err(invalid("transcript page digest and offset must be paired"));
    }
    let store = StoreReader::open(&service.home)?;
    let snapshot = store.conn().unchecked_transaction()?;
    let row = cli_transcript_row(&store, &request.selection)?;
    check_cli_transcript_budget(&store, &row, &request)?;
    after_budget();
    let page = if request.recovery {
        if request.lineage
            || request.query.view != agent_run_domain::transcript::TranscriptView::Raw
        {
            return Err(invalid(
                "transcript recovery requires an exact raw execution",
            ));
        }
        store.transcript_recovery(&row.id, request.query.cursor, request.query.limit)?
    } else {
        store.transcript_query(&row.id, &request.query, request.lineage)?
    };
    let page = agent_identity::result(&row, page)?;
    cli_transcript_row(&store, &request.selection)?;
    snapshot.commit()?;
    let mut buffer = BoundedTranscriptJson::default();
    if let Err(error) = serde_json::to_writer(&mut buffer, &page) {
        if buffer.exceeded {
            return Err(crate::Error::Runtime(
                "transcript page exceeds 64 MiB limit".into(),
            ));
        }
        return Err(error.into());
    }
    let encoded = String::from_utf8(buffer.bytes)
        .map_err(|_| crate::Error::Runtime("transcript page encoding failed".into()))?;
    let sha256 = crate::fs::sha256(encoded.as_bytes());
    if request
        .expected_sha256
        .as_ref()
        .is_some_and(|expected| expected.as_str() != sha256)
    {
        return Err(crate::Error::Broker {
            message: "transcript page changed during read".into(),
            broker_error_code: Some(CLI_TRANSCRIPT_PAGE_CHANGED_CODE.into()),
            broker_error_data: None,
        });
    }
    let offset = usize::try_from(request.offset_bytes)
        .map_err(|_| invalid("transcript page offset exceeds its size"))?;
    if offset > encoded.len() || !encoded.is_char_boundary(offset) {
        return Err(invalid(
            "transcript page offset must be a UTF-8 boundary within the page",
        ));
    }
    let mut end = offset
        .saturating_add(agent_run_domain::observation::MAX_CHUNK_BYTES)
        .min(encoded.len());
    while !encoded.is_char_boundary(end) {
        end -= 1;
    }
    Ok(
        json!({"selection":request.selection,"sha256":sha256,"total_bytes":encoded.len(),
        "offset_bytes":offset,"next_offset":end,"complete":end == encoded.len(),
        "content":&encoded[offset..end]}),
    )
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
/// A bounded steering message addressed to a stable agent or pinned run.
struct Steer {
    /// Root identity or historical alias for the lineage.
    agent_id: AgentId,
    /// Exact execution, otherwise the latest run is selected once.
    #[serde(default)]
    run_id: Option<AgentId>,
    /// Nonblank UTF-8 control text, validated by the service before enqueue.
    text: String,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
/// One transcript page; run_id pins subsequent pages to the same execution.
struct Transcript {
    /// Stable agent or historical alias.
    agent_id: AgentId,
    /// Optional historical execution, otherwise the latest run.
    #[serde(default)]
    run_id: Option<AgentId>,
    /// Shared representation, bounds and exclusive cursor options.
    #[serde(flatten)]
    page: agent_run_domain::transcript::TranscriptQuery,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
/// Continuation of a logical agent, retaining its native context and grants.
struct Resume {
    /// Stable agent identity or historical alias.
    agent_id: AgentId,
    /// Optional exact parent, otherwise the terminal lineage tip.
    #[serde(default)]
    run_id: Option<AgentId>,
    /// Nonblank next instruction for the retained native conversation.
    task: String,
    #[serde(default)]
    /// Idempotency key in the caller's namespace, reused for transport retries.
    request_id: Option<String>,
    #[serde(default)]
    /// Optional replacement display label; omission inherits the parent's.
    display_name: Option<String>,
    #[serde(default)]
    /// Optional delivery destination override; omission inherits the parent.
    orchestrator: Option<OrchestratorRef>,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Doc {
    #[serde(default)]
    topic: Option<String>,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Empty {}

/// A normalized host hook carried over an authenticated operator connection.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct HookRequest {
    payload: Value,
    transport: String,
}

/// Private CLI request to begin one planned service restart.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ServiceRestartBegin {
    request_id: String,
}

/// Private CLI selection of one planned service restart.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ServiceRestartId {
    restart_id: i64,
}
/// Operator/service-only exact integration selection; null/zero is invalid.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Integration {
    /// Stable lineage id or its retained historical alias.
    agent_id: AgentId,
    /// Positive execution receipt; omission pins the latest once.
    #[serde(default, deserialize_with = "integration_sequence")]
    sequence: Option<std::num::NonZeroU32>,
}

/// Requires a positive integer when present, rejecting explicit null.
fn integration_sequence<'de, D: serde::Deserializer<'de>>(
    deserializer: D,
) -> std::result::Result<Option<std::num::NonZeroU32>, D::Error> {
    std::num::NonZeroU32::deserialize(deserializer).map(Some)
}
/// Compatible socket liveness request; writer diagnostics are explicit so the
/// historical empty request keeps its exact {ok:true} response shape.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Ping {
    /// Opt in to bounded content-free current/last writer observations.
    #[serde(default)]
    writer_health: bool,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
/// Private socket wait arguments; omission means wait without a client deadline.
struct Wait {
    /// Stable agent identity; resolution is pinned for the entire observation.
    agent_id: AgentId,
    /// Exact execution returned by the admission being observed.
    #[serde(default)]
    run_id: Option<AgentId>,
    /// Admission receipt counter, pinning a delayed wait without exposing run IDs.
    #[serde(default)]
    sequence: Option<u32>,
    #[serde(default)]
    /// Optional observer wait in seconds; expiry never stops or limits the execution.
    timeout_seconds: Option<f64>,
}
/// Decodes and dispatches one named tool against the caller-owned service.
///
/// `raw` must be an object matching the named schema. Errors are typed so the
/// socket, MCP, and CLI transports can independently render their protocols.
pub async fn call(service: &Service, name: &str, raw: Value) -> Result<Value> {
    match name {
        "_service_restart_begin" => {
            let request: ServiceRestartBegin = args(raw)?;
            Ok(json!({"restart_id":service.begin_service_restart(&request.request_id).await?}))
        }
        "_service_restart_status" => {
            let request: ServiceRestartId = args(raw)?;
            service.service_restart_status(request.restart_id).await
        }
        "_service_restart_failed" => {
            let request: ServiceRestartId = args(raw)?;
            Ok(
                json!({"restart_id":service.recover_failed_service_restart(request.restart_id).await?}),
            )
        }
        "_service_restart_claim" => {
            let request: ServiceRestartId = args(raw)?;
            Ok(json!({"claimed":service.claim_service_restart(request.restart_id).await?}))
        }
        "_hook_context" => {
            let hook: HookRequest = args(raw)?;
            let normalized = crate::hooks::bind::normalize(&hook.payload, false, &hook.transport)?;
            let context =
                crate::hooks::context::build_for_hook(&service.home, &normalized.reference, None)
                    .await?;
            Ok(if context.injected && !context.text.trim().is_empty() {
                json!({"hookSpecificOutput":{"hookEventName":"UserPromptSubmit","additionalContext":context.text}})
            } else {
                json!({})
            })
        }
        "_hook_bind" => {
            let hook: HookRequest = args(raw)?;
            if let Some((restart_id, reference)) =
                crate::hooks::bind::restart_waiter(&hook.payload, &hook.transport)?
            {
                service
                    .remember_service_restart_caller(restart_id, reference)
                    .await?;
                return Ok(
                    json!({"hookSpecificOutput":{"hookEventName":"PostToolUse","additionalContext":"agent-run: restart wait recorded; retry after the restart notice."}}),
                );
            }
            let bound = crate::hooks::bind::run_hook_bound(
                &service.home,
                &hook.payload,
                &hook.transport,
                None,
            )
            .await?;
            let configured = agent_run_config::provider_config::ProviderConfig::load(&service.home)
                .map(|(config, _)| config.delivery.bridge.is_some())
                .or_else(|_| {
                    crate::config::Config::load(&service.home)
                        .map(|config| config.delivery.bridge.is_some())
                });
            let message = match configured {
                Ok(true) => bound.message(),
                other => {
                    let detail = if other.is_ok() {
                        "Bridge notifications are not configured"
                    } else {
                        "Bridge notification configuration could not be verified"
                    };
                    match &bound {
                        crate::hooks::bind::HookBound::Agent(value) => {
                            format!("agent-run: agent {} is bound; {detail}.", value.agent_id)
                        }
                        crate::hooks::bind::HookBound::Pool(value) => {
                            format!("agent-run: pool {} is bound; {detail}.", value.pool_id)
                        }
                    }
                }
            };
            Ok(
                json!({"hookSpecificOutput":{"hookEventName":"PostToolUse","additionalContext":message}}),
            )
        }
        agent_run_domain::worker::CATALOG_METHOD => register_catalog(service, args(raw)?).await,
        agent_run_domain::worker::METHOD => {
            let call: agent_run_domain::worker::WorkerCall = args(raw)?;
            let receipt = service
                .lane("workers")?
                .local_as(
                    worker_authority(&call.run_id, &call.attempt_id),
                    NotifyOrchestrator {
                        execution: call.run_id,
                        attempt: call.attempt_id,
                        token: call.token,
                        input: call.input,
                        at: crate::domain::now(),
                    },
                    BROKER_BUDGET,
                )
                .await?;
            Ok(serde_json::to_value(receipt)?)
        }
        agent_run_domain::worker::TOOL_METHOD => {
            let call: agent_run_domain::worker::WorkerToolCall = args(raw)?;
            let tool = agent_run_domain::worker::WorkerTool::parse(&call.tool)
                .ok_or_else(|| invalid("unknown worker tool"))?;
            worker_tool_call(service, call, tool).await
        }
        // Public initial start is provider + explicit model only; a legacy
        // `runtime` payload is an unknown field (ValidationError).
        "start" => {
            let result = service
                .start_provider(args::<agent_run_domain::ProviderStartRequest>(raw)?)
                .await?;
            agent_identity::admission(result)
        }
        "start_batch" => Ok(serde_json::to_value(
            service.start_batch(args(raw)?).await?,
        )?),
        "batch_receipt" => Ok(serde_json::to_value(
            service.batch_receipt(args(raw)?).await?,
        )?),
        "status" => Ok(serde_json::to_value(service.status(args(raw)?).await?)?),
        "transcript_exact" => {
            let request: agent_run_domain::transcript_exact::TranscriptExactRequest = args(raw)?;
            request.validate()?;
            service
                .read(move |store| {
                    Ok(serde_json::to_value(
                        agent_run_store::transcript_exact::read(store.conn(), &request)?,
                    )?)
                })
                .await
        }
        "initial_task" => {
            let request: agent_run_domain::initial_task::InitialTaskRequest = args(raw)?;
            request.validate()?;
            service
                .read(move |store| {
                    Ok(serde_json::to_value(agent_run_store::initial_task::read(
                        store.conn(),
                        &request,
                    )?)?)
                })
                .await
        }
        "history_pins" => service.history_pins(args(raw)?).await,
        "_lineage_hold_acquire"
        | "_lineage_hold_lookup"
        | "_lineage_hold_list"
        | "_lineage_hold_release" => crate::lineage_holds::call(service, name, raw).await,
        "resume" => {
            let a: Resume = args(raw)?;
            service
                .resume_public(
                    &a.agent_id,
                    a.run_id.as_ref(),
                    a.task,
                    a.request_id,
                    a.display_name,
                    a.orchestrator,
                )
                .await
        }
        "cancel" => {
            let a: Id = args(raw)?;
            let row = service.resolve_run(&a.agent_id, a.run_id.as_ref())?;
            agent_identity::result(&row, service.cancel(&row.id).await?)
        }
        "steer" => {
            let a: Steer = args(raw)?;
            let row = service.resolve_run(&a.agent_id, a.run_id.as_ref())?;
            agent_identity::result(&row, service.steer(&row.id, &a.text).await?)
        }
        "answer" => {
            let a: Id = args(raw)?;
            let row = service.resolve_run(&a.agent_id, a.run_id.as_ref())?;
            agent_identity::result(&row, service.answer(&row.id)?)
        }
        "_cli_answer" => {
            let selection: Id = args(raw)?;
            let service = service.clone();
            tokio::task::spawn_blocking(move || {
                service.cli_answer(&selection.agent_id, selection.run_id.as_ref())
            })
            .await
            .map_err(|_| crate::Error::Runtime("CLI answer read failed".into()))?
        }
        "_cli_answer_chunk" => {
            let page: CliAnswerChunk = args(raw)?;
            let service = service.clone();
            tokio::task::spawn_blocking(move || {
                serde_json::to_value(service.cli_answer_chunk(&page.query, &page.proof_token)?)
                    .map_err(Into::into)
            })
            .await
            .map_err(|_| crate::Error::Runtime("CLI answer chunk read failed".into()))?
        }
        "answer_chunk" => {
            let query = args::<agent_run_domain::observation::AnswerChunkQuery>(raw)?;
            let service = service.clone();
            tokio::task::spawn_blocking(move || {
                serde_json::to_value(service.answer_chunk(&query)?).map_err(Into::into)
            })
            .await
            .map_err(|_| crate::Error::Runtime("answer chunk read failed".into()))?
        }
        "capabilities" => {
            let query: agent_run_domain::observation::CapabilitiesQuery = args(raw)?;
            let mut result = crate::observation::capabilities();
            if query.include_store_id {
                let home = service.home.clone();
                let id = tokio::task::spawn_blocking(move || {
                    let store = crate::state::Store::open(&home)?;
                    agent_run_store::store_identity::read(&store.conn)
                })
                .await
                .map_err(|_| crate::Error::Runtime("store identity read failed".into()))??;
                result["store_id"] = serde_json::to_value(id)?;
            }
            if query.include_lineage_holds {
                result["lineage_holds"] = json!({
                    "version": 1,
                    "transport": "operator_socket",
                    "acquire": "_lineage_hold_acquire",
                    "lookup": "_lineage_hold_lookup",
                    "list": "_lineage_hold_list",
                    "release": "_lineage_hold_release"
                });
            }
            if query.include_initial_task {
                result["initial_task"] = serde_json::to_value(
                    agent_run_domain::initial_task::InitialTaskCapability::current(),
                )?;
            }
            Ok(result)
        }
        "transcript" => {
            let a: Transcript = args(raw)?;
            service.transcript_with_options(&a.agent_id, a.run_id.as_ref(), &a.page)
        }
        "_cli_transcript_select" => {
            let selection: Id = args(raw)?;
            let service = service.clone();
            tokio::task::spawn_blocking(move || {
                let store = StoreReader::open(&service.home)?;
                let row = agent_identity::resolve(
                    &store,
                    &selection.agent_id,
                    selection.run_id.as_ref(),
                )?;
                let sequence = std::num::NonZeroU32::new(row.sequence)
                    .ok_or_else(|| invalid("transcript execution receipt must be positive"))?;
                Ok(serde_json::to_value(CliTranscriptSelection {
                    agent_id: row.root_agent_id,
                    run_id: row.id,
                    sequence,
                })?)
            })
            .await
            .map_err(|_| crate::Error::Runtime("CLI transcript selection failed".into()))?
        }
        "_cli_transcript_page" => {
            let request: CliTranscriptPage = args(raw)?;
            let service = service.clone();
            tokio::task::spawn_blocking(move || cli_transcript_page(&service, request))
                .await
                .map_err(|_| crate::Error::Runtime("CLI transcript page read failed".into()))?
        }
        "_cli_transcript_status" => {
            let selection: CliTranscriptSelection = args(raw)?;
            let service = service.clone();
            tokio::task::spawn_blocking(move || {
                let store = StoreReader::open(&service.home)?;
                let row = cli_transcript_row(&store, &selection)?;
                let view = service.view(&store, &row)?;
                Ok(json!({"selection":selection,"status":view["status"]}))
            })
            .await
            .map_err(|_| crate::Error::Runtime("CLI transcript status read failed".into()))?
        }
        "start_pool" => {
            service
                .start_pool(args::<agent_run_domain::pool::PoolStartRequest>(raw)?)
                .await
        }
        "pool_post" => {
            let request: agent_run_domain::pool::PoolPost = args(raw)?;
            service
                .pool_post(request)
                .await?
                .map_err(agent_run_domain::pool::PoolDenial::into_error)
        }
        "pool_replace" => service
            .replace_pool_member(args::<agent_run_domain::pool::PoolReplace>(raw)?)
            .await?
            .map_err(agent_run_domain::pool::PoolDenial::into_error),
        "pool" => {
            let query: agent_run_domain::pool::PoolQuery = args(raw)?;
            let service = service.clone();
            tokio::task::spawn_blocking(move || service.pool_status(query))
                .await
                .map_err(|_| crate::Error::Runtime("pool read failed".into()))??
                .map_err(agent_run_domain::pool::PoolDenial::into_error)
        }
        "list_pools" => {
            let query = args::<agent_run_domain::pool::ListPoolsQuery>(raw)?;
            let service = service.clone();
            tokio::task::spawn_blocking(move || service.list_pools(query))
                .await
                .map_err(|_| crate::Error::Runtime("pool list failed".into()))?
        }
        "list_agents" => service.list_public(args::<Query>(raw)?).await,
        "agent_integration" => {
            let selection: Integration = args(raw)?;
            service
                .agent_integration(
                    selection.agent_id,
                    selection.sequence.map(std::num::NonZeroU32::get),
                )
                .await
        }
        "doc" => {
            let a: Doc = args(raw)?;
            let topic = a.topic.as_deref().unwrap_or("index");
            Ok(json!({"topic":topic,"text":doc(topic)?}))
        }
        "models" => service.models(args(raw)?).await,
        "limits" => {
            let _: Empty = args(raw)?;
            service.limits().await
        }
        "capacity_order" => service.capacity_order(args(raw)?).await,
        "delegation_guide" => service.delegation_guide_filtered(args(raw)?).await,
        // Socket-only control/discovery methods; absent from public tool discovery.
        "tools" => {
            let _: Empty = args(raw)?;
            // The private socket discovery method predates MCP and returns the
            // table itself, not an MCP-shaped wrapper.
            Ok(Value::Array(tools()))
        }
        "ping" => {
            let options: Ping = args(raw)?;
            let mut value = json!({"ok":true});
            if options.writer_health
                && let Some(health) = service.writer_health()
            {
                value["writer"] = serde_json::to_value(health)?;
            }
            Ok(value)
        }
        "wait" => {
            let a: Wait = args(raw)?;
            if a.sequence.is_some() && a.run_id.is_some() {
                return Err(invalid("conflicting execution receipt selectors"));
            }
            if a.timeout_seconds
                .is_some_and(|seconds| !seconds.is_finite() || seconds <= 0.0)
            {
                return Err(invalid("timeout_seconds must be a positive finite number"));
            }
            let row = match a.sequence {
                Some(sequence) => {
                    let reader = StoreReader::open(&service.home)?;
                    agent_identity::resolve_sequence(&reader, &a.agent_id, sequence)?
                }
                None => service.resolve_run(&a.agent_id, a.run_id.as_ref())?,
            };
            let mut result =
                agent_identity::result(&row, service.wait(&row.id, a.timeout_seconds).await?)?;
            if a.timeout_seconds.is_some() && result["terminal"] == false {
                result["timed_out"] = Value::Bool(true);
            }
            Ok(result)
        }
        _ => Err(invalid("unknown tool")),
    }
}

/// Returns one packaged operator-guide topic using the transport-neutral registry.
pub fn doc(topic: &str) -> Result<&'static str> {
    crate::doc::topic_text(topic)
}

/// Three empty forward reads of the same actual pool revision form one
/// polling episode. Bounded process-local hints never grant authority; every
/// read/report still authenticates its exact attempt in the store.
#[derive(Default)]
pub(crate) struct PoolReadGuards(std::collections::BTreeMap<String, (String, u64, u8, bool)>);
impl PoolReadGuards {
    /// Whether the prior episode needs bounded backpressure; cursor arguments
    /// cannot reset this because the key is the actual durable log revision.
    fn waiting(&self, key: &str) -> bool {
        self.0.get(key).is_some_and(|v| v.2 >= 3)
    }
    /// Observe one authenticated forward page. Returns (guarded, needs report).
    /// A failed report remains retryable until its durable acknowledgement.
    /// New data/revision rearms; old attempts consume at most 256 small slots.
    fn observe(&mut self, key: String, page: &Value) -> (bool, bool) {
        if page["before_seq"].is_number() {
            return (false, false);
        }
        if page["entries"].as_array().is_some_and(|v| !v.is_empty()) {
            self.0.remove(&key);
            return (false, false);
        }
        let Some(revision) = page["log_revision"].as_u64() else {
            return (false, false);
        };
        let pool = page["pool_id"].as_str().unwrap_or("").to_owned();
        if !self.0.contains_key(&key) && self.0.len() >= 256 {
            self.0.pop_first();
        }
        let state = self
            .0
            .entry(key)
            .or_insert((pool.clone(), revision, 0, false));
        if state.0 != pool || state.1 != revision {
            *state = (pool, revision, 0, false);
        }
        state.2 = state.2.saturating_add(1).min(3);
        (state.2 >= 3, state.2 >= 3 && !state.3)
    }
    /// Mark only the observed revision after the writer acknowledged its
    /// idempotent notice/control unit. A delayed old reply cannot mark new work.
    fn reported(&mut self, key: &str, revision: u64) {
        if let Some(state) = self.0.get_mut(key)
            && state.1 == revision
        {
            state.3 = true;
        }
    }
}

/// Maximum seconds one private `pool_read` may hold for a new entry, kept
/// below the broker socket's per-request deadline so the wait never outlives
/// its transport.
const POOL_READ_WAIT_SECONDS: f64 = 25.0;

/// Routes one authenticated private worker tool through the shared broker.
///
/// Every tool authenticates the hidden capability server-side; pool denials
/// are returned in-band as one typed `{error: {code, message}}` object so the
/// worker transport can render a typed refusal instead of guessing from
/// prose. The read wait is bounded, holds no database lock across awaits,
/// and ends as soon as the attempt stops being usable.
async fn worker_tool_call(
    service: &Service,
    call: agent_run_domain::worker::WorkerToolCall,
    tool: agent_run_domain::worker::WorkerTool,
) -> Result<Value> {
    use agent_run_domain::pool::{PoolDenial, PoolMessage, PoolPropose, PoolReadRequest, PoolVote};
    use agent_run_domain::worker::WorkerTool;
    let denial = |denial: PoolDenial| {
        Ok(json!({"error": {"code": denial.code(), "message": denial.message()}}))
    };
    match tool {
        WorkerTool::HealthReply => {
            let input: agent_run_domain::health::PingReply = serde_json::from_value(call.input)
                .map_err(|_| invalid("invalid health reply arguments"))?;
            let duplicate = service
                .lane("workers")?
                .local_as(
                    worker_authority(&call.run_id, &call.attempt_id),
                    agent_run_store::ops::AcceptHealthReply {
                        execution: call.run_id,
                        attempt: call.attempt_id,
                        token: call.token,
                        input,
                        at: crate::domain::now(),
                    },
                    BROKER_BUDGET,
                )
                .await?;
            Ok(json!({"accepted":true,"duplicate":duplicate}))
        }
        WorkerTool::Finish => {
            let input: agent_run_domain::worker::FinishRequest = serde_json::from_value(call.input)
                .map_err(|_| invalid("invalid finish arguments"))?;
            let receipt = service
                .lane("workers")?
                .local_as(
                    worker_authority(&call.run_id, &call.attempt_id),
                    AcceptWorkerFinish {
                        execution: call.run_id,
                        attempt: call.attempt_id,
                        token: call.token,
                        input,
                        at: crate::domain::now(),
                        // The writer handle replaces this with its pre-SQL verdict.
                        guard: agent_run_domain::worker::FinishGuard::Unavailable,
                    },
                    BROKER_BUDGET,
                )
                .await?;
            Ok(serde_json::to_value(receipt)?)
        }
        WorkerTool::SaveReport => {
            let input: agent_run_domain::worker::SaveReportRequest =
                serde_json::from_value(call.input.clone())
                    .map_err(|_| invalid("invalid save_report arguments"))?;
            let home = service.home.clone();
            let run_id = call.run_id.clone();
            let receipt = tokio::task::spawn_blocking(move || {
                let reader = StoreReader::open(&home)?;
                crate::research_reports::save(&reader, &call, &input)
            })
            .await
            .map_err(|_| crate::Error::Runtime("research report write failed".into()))??;
            let value = serde_json::to_value(&receipt)?;
            service
                .lane("workers")?
                .event(&run_id, "research_report_saved", value.clone())
                .await?;
            Ok(value)
        }
        WorkerTool::Notify => {
            let input: agent_run_domain::worker::NotifyRequest = serde_json::from_value(call.input)
                .map_err(|_| invalid("unknown, missing, or incorrectly typed report argument"))?;
            let receipt = service
                .lane("workers")?
                .local_as(
                    worker_authority(&call.run_id, &call.attempt_id),
                    NotifyOrchestrator {
                        execution: call.run_id,
                        attempt: call.attempt_id,
                        token: call.token,
                        input,
                        at: crate::domain::now(),
                    },
                    BROKER_BUDGET,
                )
                .await?;
            Ok(serde_json::to_value(receipt)?)
        }
        WorkerTool::PoolPost | WorkerTool::PoolPropose | WorkerTool::PoolVote => {
            let write = match tool {
                WorkerTool::PoolPost => PoolWriteSpec::Message(
                    serde_json::from_value::<PoolMessage>(call.input)
                        .map_err(|_| invalid("invalid pool_post arguments"))?,
                ),
                WorkerTool::PoolPropose => PoolWriteSpec::Proposal(
                    serde_json::from_value::<PoolPropose>(call.input)
                        .map_err(|_| invalid("invalid pool_propose arguments"))?,
                ),
                _ => PoolWriteSpec::Vote(
                    serde_json::from_value::<PoolVote>(call.input)
                        .map_err(|_| invalid("invalid pool_vote arguments"))?,
                ),
            };
            let outcome = service
                .lane("workers")?
                .local_as(
                    worker_authority(&call.run_id, &call.attempt_id),
                    PoolWrite {
                        execution: call.run_id,
                        attempt: call.attempt_id,
                        token: call.token,
                        write,
                    },
                    BROKER_BUDGET,
                )
                .await?;
            match outcome {
                PoolOutcome::Done(receipt) => Ok(json!({
                    "pool_id": receipt.pool_id,
                    "seq": receipt.seq,
                    "duplicate": receipt.duplicate,
                    "recorded": true,
                    "note": "recorded durably and readable through pool_read; not pushed or read by anyone",
                })),
                PoolOutcome::Denied(reason) => denial(reason),
            }
        }
        WorkerTool::PoolRead => {
            let request: PoolReadRequest = serde_json::from_value(call.input)
                .map_err(|_| invalid("invalid pool_read arguments"))?;
            request.validate()?;
            let after_seq = request.after_seq.unwrap_or(0).min(i64::MAX as u64);
            let limit = request.limit.unwrap_or(50);
            let run = call.run_id.clone();
            let attempt = call.attempt_id.clone();
            let explicit = service
                .read(move |store| store.pool_read_guard_enabled(&run, &attempt))
                .await?;
            let guard_key = format!("{}:{}", call.run_id, call.attempt_id);
            let guarded = explicit && service.pool_read_guards().waiting(&guard_key);
            // The bounded optional wait: one short store probe per interval,
            // never a lock across awaits, ended by data, deadline or a
            // stopped attempt. A reverse page never waits.
            let deadline = tokio::time::Instant::now()
                + Duration::from_secs_f64(if request.before_seq.is_some() {
                    0.0
                } else if explicit {
                    if guarded { POOL_READ_WAIT_SECONDS } else { 0.0 }
                } else {
                    request
                        .wait_seconds
                        .unwrap_or(0.0)
                        .min(POOL_READ_WAIT_SECONDS)
                });
            loop {
                let home = service.home.clone();
                let (run_id, attempt_id, token) = (
                    call.run_id.clone(),
                    call.attempt_id.clone(),
                    call.token.clone(),
                );
                let probe_after = after_seq;
                let ready = tokio::task::spawn_blocking(move || {
                    StoreReader::open(&home)?.pool_has_entries_after(
                        &run_id,
                        &attempt_id,
                        &token,
                        probe_after,
                    )
                })
                .await
                .map_err(|_| crate::Error::Runtime("worker pool read failed".into()))??;
                match ready {
                    Err(reason) => return denial(reason),
                    Ok(true) => break,
                    Ok(false) => {
                        if tokio::time::Instant::now() >= deadline {
                            break;
                        }
                        tokio::time::sleep(Duration::from_millis(150)).await;
                    }
                }
            }
            let home = service.home.clone();
            let (run_id, attempt_id, token) = (
                call.run_id.clone(),
                call.attempt_id.clone(),
                call.token.clone(),
            );
            let page = tokio::task::spawn_blocking(move || {
                StoreReader::open(&home)?.pool_read(
                    &run_id,
                    &attempt_id,
                    &token,
                    after_seq,
                    request.before_seq.map(|before| before.min(i64::MAX as u64)),
                    limit,
                )
            })
            .await
            .map_err(|_| crate::Error::Runtime("worker pool read failed".into()))??;
            match page {
                Ok(mut page) => {
                    if explicit {
                        let (guarded, first) =
                            service.pool_read_guards().observe(guard_key.clone(), &page);
                        page["suspend_inference"] =
                            json!(guarded || page["entries"].as_array().is_some_and(Vec::is_empty));
                        if first {
                            let _newly_reported = service
                                .lane("workers")?
                                .local_as(
                                    worker_authority(&call.run_id, &call.attempt_id),
                                    agent_run_store::ops::ReportPoolReadLoop {
                                        execution: call.run_id.clone(),
                                        attempt: call.attempt_id.clone(),
                                        token: call.token.clone(),
                                        revision: page["log_revision"].as_u64().ok_or_else(
                                            || invalid("pool page has no log revision"),
                                        )?,
                                        at: crate::domain::now(),
                                    },
                                    BROKER_BUDGET,
                                )
                                .await?;
                            service.pool_read_guards().reported(
                                &guard_key,
                                page["log_revision"].as_u64().expect("validated above"),
                            );
                        }
                    }
                    Ok(page)
                }
                Err(reason) => denial(reason),
            }
        }
    }
}

/// Worker authority of one authenticated tool call (rows 44-47 accept no other).
fn worker_authority(run: &AgentId, attempt: &str) -> WriteAuthority {
    WriteAuthority::Worker {
        execution: run.clone(),
        attempt: attempt.to_owned(),
    }
}

/// Registers one worker's pool catalog proof.
///
/// A read-only preflight answers the unauthenticated, conflicting and
/// already-registered cases with local values; only a first registration
/// reaches the writer, so a repeat never writes and never fabricates a receipt.
async fn register_catalog(service: &Service, proof: WorkerCatalogProof) -> Result<Value> {
    if !known_worker_catalog(proof.version, &proof.digest) {
        return Err(crate::Error::Unsupported(
            "worker pool catalog is not supported".into(),
        ));
    }
    let at = crate::domain::now();
    let home = service.home.clone();
    let probe = proof.clone();
    let preflight = tokio::task::spawn_blocking(move || {
        StoreReader::open(&home)?.worker_catalog_preflight(&probe, at)
    })
    .await
    .map_err(|_| crate::Error::Runtime("worker catalog proof failed".into()))??;
    match preflight {
        CatalogPreflight::Unauthenticated => {
            Err(crate::Error::Validation("invalid worker capability".into()))
        }
        CatalogPreflight::Conflict => Err(crate::Error::Conflict),
        CatalogPreflight::AlreadyRegistered => Ok(json!({"registered":true})),
        CatalogPreflight::NeedsWrite => {
            service
                .lane("workers")?
                .local_as(
                    worker_authority(&proof.run_id, &proof.attempt_id),
                    RegisterWorkerCatalog { proof, at },
                    BROKER_BUDGET,
                )
                .await?;
            Ok(json!({"registered":true}))
        }
    }
}

#[cfg(test)]
mod cli_transcript_snapshot_tests {
    use super::*;

    /// A commit after the budget check cannot add unchecked rows to raw,
    /// block or recovery pages, but becomes visible to the next page snapshot.
    #[test]
    fn cli_transcript_budget_and_page_share_snapshot() {
        for (view, lineage, recovery) in [
            ("raw", false, false),
            ("raw", true, false),
            ("blocks", false, false),
            ("blocks", true, false),
            ("raw", false, true),
        ] {
            let temp = tempfile::tempdir().unwrap();
            let home = temp.path().canonicalize().unwrap();
            crate::fs::private_dir(&home).unwrap();
            std::fs::write(
                home.join("config.toml"),
                format!(
                    "schema_version=1\n[runtimes.mock]\nenabled=true\nadapter='claude'\nbinary='/bin/true'\nhome={}\nmodels=['fixture']\nlimits_source='none'\n",
                    toml::Value::String(home.join("mock").to_string_lossy().into_owned())
                ),
            )
            .unwrap();
            let config = crate::config::Config::load(&home).unwrap();
            agent_run_store::fixture::initialize(&home).unwrap();
            let mut writer = agent_run_store::fixture::open(&home).unwrap();
            let start = serde_json::from_value(json!({
                "runtime":"mock", "model":"fixture", "profile":"review",
                "task":"snapshot fixture", "workdir":home
            }))
            .unwrap();
            let (id, _) = writer.admit(&start, &config, &json!({}), None).unwrap();
            writer
                .message(&id, "assistant", "before", None, Some("before"))
                .unwrap();
            let request = json!({
                "selection":{"agent_id":id,"run_id":id,"sequence":1},
                "query":{"view":view},"lineage":lineage,"recovery":recovery,
                "offset_bytes":0,"expected_sha256":null
            });
            let service = Service::new(home);
            let first = cli_transcript_page_observed(
                &service,
                serde_json::from_value(request.clone()).unwrap(),
                || {
                    writer
                        .message(&id, "assistant", "after", None, Some("after"))
                        .unwrap();
                },
            )
            .unwrap();
            assert_eq!(first["complete"], true);
            let page: Value = serde_json::from_str(first["content"].as_str().unwrap()).unwrap();
            assert_eq!(page["messages"].as_array().unwrap().len(), 1);
            assert_eq!(page["messages"][0]["content"], "before");

            let next =
                cli_transcript_page(&service, serde_json::from_value(request).unwrap()).unwrap();
            let page: Value = serde_json::from_str(next["content"].as_str().unwrap()).unwrap();
            assert_eq!(page["messages"].as_array().unwrap().len(), 2);
            assert_eq!(page["messages"][1]["content"], "after");
        }
    }
}

#[cfg(test)]
mod pool_read_guard_tests {
    use super::*;
    /// Caller cursors cannot evade the guard. Actual new entries/revisions
    /// rearm it, repeated reads emit one corrective episode, and reverse
    /// pagination remains available without being mistaken for polling.
    #[test]
    fn empty_pool_reads_are_bounded_per_actual_revision() {
        let mut guard = PoolReadGuards::default();
        let mut page =
            json!({"pool_id":"p","log_revision":41,"entries":[],"before_seq":null,"after_seq":41});
        assert_eq!(guard.observe("attempt".into(), &page), (false, false));
        page["after_seq"] = json!(999);
        assert_eq!(guard.observe("attempt".into(), &page), (false, false));
        assert_eq!(guard.observe("attempt".into(), &page), (true, true));
        assert_eq!(
            guard.observe("attempt".into(), &page),
            (true, true),
            "unacknowledged report remains retryable"
        );
        guard.reported("attempt", 41);
        for _ in 0..100 {
            assert_eq!(guard.observe("attempt".into(), &page), (true, false));
        }
        assert!(guard.waiting("attempt"));
        page["entries"] = json!([{"seq":42}]);
        page["log_revision"] = json!(42);
        assert_eq!(guard.observe("attempt".into(), &page), (false, false));
        assert!(!guard.waiting("attempt"));
        page["entries"] = json!([]);
        page["before_seq"] = json!(42);
        for _ in 0..5 {
            assert_eq!(guard.observe("attempt".into(), &page), (false, false));
        }
    }
}
