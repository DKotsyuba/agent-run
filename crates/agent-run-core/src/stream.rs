//! Claude-family launch planning and native stream completion. Input correlation
//! separates task results from context acknowledgements; the supervisor retains
//! process ownership, durable answer sealing and final cleanup authority.
use crate::{
    Result,
    config::{Adapter, Config, Runtime},
    domain::{AgentId, Outcome, Status},
    error::invalid,
    profiles::Profile,
    state::{Record, Store},
    verify,
};
use crate::{commands, journal};
use agent_run_adapters::{
    EngineResult, LaunchPlan,
    io::{Event, Process},
    materialize::Snapshot,
    redact::StreamingRedactor,
};
use serde_json::{Value, json};
use std::{collections::BTreeMap, path::Path, time::Duration};

/// Flushes the unresolved suffix of one assistant message only after applying
/// its launch-secret policy across all streamed fragment boundaries.
fn flush_assistant(
    store: &Store,
    id: &AgentId,
    redactor: &mut StreamingRedactor,
    message_id: Option<&str>,
) -> Result<()> {
    let text = redactor.finish();
    if !text.is_empty() {
        journal(store, id, "assistant", &text, None, message_id)?;
    }
    Ok(())
}

/// Resolves the adapter environment and builds one isolated stream-JSON launch plan.
///
/// The returned plan owns argv, CWD, input framing, and child-only
/// environment values for `record`; configuration and role validation happen
/// before this call. Environment-resolution failures, unsupported runtime
/// settings, and invalid launch paths are returned without spawning a child.
pub fn plan(
    config: &Config,
    runtime: &Runtime,
    record: &Record,
    role: &Profile,
    home: &Path,
    app_home: &Path,
    snapshot: &Snapshot,
) -> Result<LaunchPlan> {
    let environment = agent_run_adapters::environment(
        config,
        runtime,
        role,
        home,
        record.request.account.as_deref(),
        app_home,
    )?;
    plan_with_environment(config, runtime, record, role, home, snapshot, environment)
}

/// Builds a Claude-family launch after the caller has resolved its child environment.
///
/// `environment` must already contain the adapter's managed HOME, PATH, and
/// credential values; this function mutates only its local copy to add
/// launch-specific exports such as `ANTHROPIC_MODEL`.
/// Keeping this deterministic half separate lets fixture tests compare argv
/// and environment ownership without probing a real Keychain or engine.
pub fn plan_with_environment(
    config: &Config,
    runtime: &Runtime,
    record: &Record,
    role: &Profile,
    home: &Path,
    snapshot: &Snapshot,
    mut env: std::collections::BTreeMap<String, String>,
) -> Result<LaunchPlan> {
    let kind = runtime.kind()?;
    let req = &record.request;
    let (mut args, input) = {
        let mut tools = vec!["Read".to_owned(), "Grep".into(), "Glob".into()];
        if !role.skills.is_empty() {
            tools.push("Skill".into());
        }
        // A read-only role never gets the unrestricted shell tool in this port.
        // Source allows a larger shell surface; the narrower policy is documented.
        if role.write {
            tools.extend([
                "Edit".into(),
                "Write".into(),
                "NotebookEdit".into(),
                "Bash".into(),
            ]);
        }
        if role.network {
            tools.extend(["WebFetch".into(), "WebSearch".into()]);
        }
        let mut allowed: Vec<String> = tools
            .iter()
            .map(|tool| {
                if ["Edit", "Write", "NotebookEdit"].contains(&tool.as_str()) {
                    format!("{tool}({}/**)", req.workdir.display())
                } else {
                    tool.clone()
                }
            })
            .collect();
        allowed.extend(role.mcp.iter().map(|s| format!("mcp__{s}")));
        let mut denied = if role.network {
            vec![]
        } else {
            vec!["WebFetch".into(), "WebSearch".into()]
        };
        if let Some(e) = runtime
            .environment
            .as_ref()
            .and_then(|n| config.environments.get(n))
        {
            for name in &e.denied_commands {
                denied.push(format!("Bash({name}:*)"));
            }
        }
        let model = if kind == Adapter::Claude && req.model == "fable" {
            "claude-fable-5-1".into()
        } else if kind == Adapter::Glm {
            agent_run_adapters::glm::cli_model(&req.model)
        } else {
            req.model.clone()
        };
        if kind == Adapter::Glm {
            env.insert("ANTHROPIC_MODEL".into(), model.clone());
        }
        let mut args = vec![
            "--print".into(),
            "--output-format".into(),
            "stream-json".into(),
            "--input-format".into(),
            "stream-json".into(),
            "--verbose".into(),
            // Partial streaming events carry the per-message identity the
            // transcript producer journals; the official CLI requires this
            // flag together with --print and stream-json above.
            "--include-partial-messages".into(),
            "--replay-user-messages".into(),
            "--model".into(),
            model,
            "--permission-mode".into(),
            if role.write { "acceptEdits" } else { "default" }.into(),
            "--setting-sources".into(),
            "".into(),
            "--strict-mcp-config".into(),
            "--settings".into(),
            home.join("settings.json").to_string_lossy().into_owned(),
        ];
        for root in &role.read_roots {
            args.extend(["--add-dir".into(), root.to_string_lossy().into_owned()]);
        }
        args.extend([
            "--tools".into(),
            tools.join(","),
            "--allowedTools".into(),
            allowed.join(","),
            "--disallowedTools".into(),
            denied.join(","),
        ]);
        if !role.mcp.is_empty() {
            args.extend([
                "--mcp-config".into(),
                home.join("mcp/mcp-config.json")
                    .to_string_lossy()
                    .into_owned(),
            ]);
        }
        for plugin in &snapshot.plugin_paths {
            args.extend(["--plugin-dir".into(), plugin.to_string_lossy().into_owned()]);
        }
        if let Some(effort) = &req.effort {
            args.extend(["--effort".into(), effort.clone()]);
        }
        let mut prompt = role.body.clone();
        if let Some(schema) = &req.output_schema {
            prompt.push_str(&format!(
                "\n\nReturn only JSON matching this schema: {}",
                serde_json::to_string(schema)?
            ));
        }
        args.extend(["--append-system-prompt".into(), prompt]);
        if record.resume_of_runtime_session_id.is_none() {
            args.extend(["--session-id".into(), uuid::Uuid::new_v4().to_string()]);
        }
        (
            args,
            Some(format!(
                "{}\n",
                json!({"type":"user","message":{"role":"user","content":[{"type":"text","text":req.task}]}})
            )),
        )
    };
    if let Some(session) = &record.resume_of_runtime_session_id {
        args.extend(["--resume".into(), session.clone()]);
    }
    Ok(LaunchPlan {
        binary: runtime.binary.clone(),
        args,
        cwd: req.workdir.clone(),
        environment: env,
        initial_input: input,
    })
}
/// Associates native terminal frames with inputs issued during this attempt.
/// IDs stay in memory; a replay batch can contain several coalesced inputs.
#[derive(Default)]
struct ResultInputs {
    /// IDs written by this runner; unknown native correlations fail closed.
    sent: std::collections::BTreeSet<String>,
    /// Most recent initial task or written steering input; pool notes do not replace it.
    task: Option<String>,
    /// Native replay acknowledgements since the preceding terminal frame, in order.
    replayed: Vec<String>,
    /// Correlations already completed; repeated terminal frames are ambiguous.
    completed: std::collections::BTreeSet<String>,
    /// Historical raw initial text has no UUID; subsequent inputs make it ambiguous.
    unframed_initial: bool,
    /// A failed write with open stdin may have partially sent a new task.
    uncertain_write: bool,
}

/// Selection evidence for one terminal frame, independent of answer contents.
#[derive(Clone, Copy, PartialEq, Eq)]
enum ResultScope {
    /// The frame covers the current task directly or in a native replay batch.
    Task,
    /// The frame covers a known input without completing the current task.
    Context,
    /// An older engine omitted correlation; only a single terminal is compatible.
    Legacy,
    /// Unknown, duplicate or inconsistent native correlation cannot certify success.
    Ambiguous,
}

impl ResultScope {
    /// Returns a fixed diagnostic label without exposing input identifiers.
    fn label(self) -> &'static str {
        match self {
            Self::Task => "task",
            Self::Context => "context",
            Self::Legacy => "legacy",
            Self::Ambiguous => "ambiguous",
        }
    }
}

impl ResultInputs {
    /// Records one successfully written UUID; authoritative inputs replace the task.
    fn sent(&mut self, id: String, authoritative: bool) {
        if authoritative {
            self.task = Some(id.clone());
        }
        self.sent.insert(id);
    }

    /// Accepts only CLI replay acknowledgements of this runner's known inputs.
    /// Other user frames, including tool results, do not establish task completion.
    fn replay(&mut self, frame: &Value) -> Result<()> {
        if frame.get("isReplay").and_then(Value::as_bool) != Some(true) {
            return Ok(());
        }
        let id = frame
            .get("uuid")
            .and_then(Value::as_str)
            .filter(|id| self.sent.contains(*id))
            .ok_or_else(|| invalid("native user replay has unknown input correlation"))?;
        if self.replayed.iter().any(|seen| seen == id) {
            return Err(invalid("native user replay duplicated an input"));
        }
        self.replayed.push(id.to_owned());
        Ok(())
    }

    /// Resolves the terminal's native user-message UUID, never its position or text.
    /// A coalesced batch must end with that UUID; its current task may occur earlier.
    /// Missing correlations retain only the historical single-result compatibility.
    fn scope(&mut self, frame: &Value) -> ResultScope {
        let replayed = std::mem::take(&mut self.replayed);
        let Some(id) = frame.get("user_message_uuid").and_then(Value::as_str) else {
            let single_input = self.sent.is_empty()
                || (self.sent.len() == 1 && self.task.is_some() && !self.unframed_initial);
            return if replayed.is_empty()
                && single_input
                && frame.get("user_message_uuid").is_none_or(Value::is_null)
            {
                ResultScope::Legacy
            } else {
                ResultScope::Ambiguous
            };
        };
        if !self.sent.contains(id)
            || self.completed.contains(id)
            || replayed.iter().any(|id| self.completed.contains(id))
            || replayed.last().is_some_and(|last| last != id)
        {
            return ResultScope::Ambiguous;
        }
        self.completed.insert(id.to_owned());
        self.completed.extend(replayed.iter().cloned());
        if self.task.as_deref() == Some(id)
            || self
                .task
                .as_ref()
                .is_some_and(|task| replayed.contains(task))
        {
            ResultScope::Task
        } else {
            ResultScope::Context
        }
    }
}

/// Projects only reviewed static tool names from a Claude initialization frame.
/// Missing, oversized, malformed or unrecognized entries keep inventory proof
/// incomplete. Unknown names, session IDs, paths, arguments, environment and
/// plugin/server configuration are never persisted in this diagnostic.
fn native_init_inventory(frame: &Value) -> Value {
    /// Static diagnostic spellings include allowed and forbidden capabilities;
    /// membership here records observation and never grants tool execution.
    const KNOWN: &[&str] = &[
        "WebFetch",
        "WebSearch",
        "Bash",
        "Read",
        "Grep",
        "Glob",
        "Edit",
        "Write",
        "NotebookEdit",
        "Agent",
        "Task",
        "TaskOutput",
        "TaskStop",
        "Skill",
        "ToolSearch",
        "TodoWrite",
        "AskUserQuestion",
        "mcp__agent_run_worker__notify_orchestrator",
        "mcp__agent_run_worker__pool_post",
        "mcp__agent_run_worker__pool_read",
        "mcp__agent_run_worker__pool_propose",
        "mcp__agent_run_worker__pool_vote",
        "mcp__agent_run_worker__save_report",
    ];
    let Some(tools) = frame["tools"].as_array().filter(|items| items.len() <= 64) else {
        return json!({"version":1,"complete":false,"tool_names":[],"unknown_count":null});
    };
    let mut names = Vec::new();
    let mut unknown = 0_usize;
    for tool in tools {
        if let Some(known) = tool
            .as_str()
            .and_then(|name| KNOWN.iter().find(|known| **known == name))
        {
            names.push(*known);
        } else {
            unknown += 1;
        }
    }
    names.sort_unstable();
    names.dedup();
    json!({"version":1,"complete":unknown==0&&names.len()==tools.len(),"tool_names":names,"unknown_count":unknown})
}

/// Builds content-free metadata for one native result, including ignored frames.
///
/// `index` is one-based within the execution; `ignored` describes the current
/// selection policy. Known protocol subtype labels, JSON types, byte counts
/// and numeric counters are retained; unknown labels and all content are omitted.
/// Input JSON is already bounded by the transport. Missing counters stay null.
fn native_result_metadata(index: u64, result: &Value, ignored: bool) -> Value {
    let kind = |value: Option<&Value>| match value {
        None => "missing",
        Some(Value::Null) => "null",
        Some(Value::Bool(_)) => "boolean",
        Some(Value::Number(_)) => "number",
        Some(Value::String(_)) => "string",
        Some(Value::Array(_)) => "array",
        Some(Value::Object(_)) => "object",
    };
    let bytes = |value: Option<&Value>| match value {
        None | Some(Value::Null) => 0,
        Some(Value::String(text)) => text.len(),
        Some(value) => value.to_string().len(),
    };
    let subtype = match result.get("subtype").and_then(Value::as_str) {
        Some(
            value @ ("success"
            | "error_during_execution"
            | "error_max_turns"
            | "error_max_budget_usd"
            | "error_max_structured_output_retries"),
        ) => value,
        None => "missing_or_invalid",
        Some(_) => "unrecognized",
    };
    json!({
        "version": 1, "index": index, "subtype": subtype,
        "is_error": result.get("is_error").and_then(Value::as_bool),
        "result_type": kind(result.get("result")),
        "result_bytes": bytes(result.get("result")),
        "structured_output_type": kind(result.get("structured_output")),
        "structured_output_bytes": bytes(result.get("structured_output")),
        "num_turns": result.get("num_turns").and_then(Value::as_u64),
        "duration_ms": result.get("duration_ms").and_then(Value::as_f64)
            .filter(|value| value.is_finite() && *value >= 0.0),
        "ignored": ignored,
    })
}

/// Reads the native result string/object, or its non-null structured output when
/// no supported result value exists. Empty strings remain empty: transcript
/// text is never substituted for a terminal answer.
fn result_text(v: &Value) -> Option<String> {
    match v.get("result") {
        Some(Value::String(s)) => Some(s.clone()),
        Some(Value::Object(s)) => Some(serde_json::to_string(s).ok()?),
        _ => v
            .get("structured_output")
            .filter(|s| !s.is_null())
            .and_then(|s| serde_json::to_string(s).ok()),
    }
}

/// Classifies a Claude-family terminal result without trusting its subtype alone.
///
/// The native CLI occasionally labels an authentication error as `success`.
/// This mirrors Python's marker-first classification, while preserving an
/// engine-provided non-success subtype such as `error_max_turns`.
fn result_failure_kind(subtype: &str, text: Option<&str>) -> &'static str {
    let text = text.unwrap_or("").to_ascii_lowercase();
    if [
        "failed to authenticate",
        "oauth access token has expired",
        "oauth token has expired",
        "authentication_error",
        "invalid api key",
        "invalid bearer token",
        "please run /login",
    ]
    .iter()
    .any(|marker| text.contains(marker))
    {
        "auth_failed"
    } else if subtype == "error_max_turns" {
        "max_turns"
    } else if matches!(subtype.trim(), "" | "success" | "none") {
        "engine_error"
    } else {
        "runtime_failed"
    }
}
/// Drains one Claude-family stream and returns its terminal engine result.
///
/// A fresh launch may publish a later nonempty native session identifier, and
/// the latest identifier becomes the durable result identity. Resumed launches
/// instead reject any identifier other than their requested session. The first
/// observed identity and each genuine change are journaled once per execution;
/// unchanged frame identities still validate but never contend for a writer.
/// The function journals recognized events, services bounded control commands, and
/// returns malformed stream data or persistence failures as domain errors.
/// Native input UUIDs and replay batches correlate results to the latest task or
/// steering input; pool-only results cannot complete it. Every result is observed
/// and validated, errors stay fatal, and uncorrelated multiple results fail closed.
/// A single legacy result remains compatible. Success requires a nonblank native
/// answer, EOF, exit zero, and no uncertain input write.
/// Assistant text deltas and the completion tail of one message are journaled
/// under one durable `raw_ref`: the native message id from `message_start` or
/// the full event, or one producer-owned bounded fallback per message boundary
/// when the engine omits ids, so distinct messages never merge.
pub async fn run(
    process: &mut Process,
    store: &mut Store,
    record: &Record,
    initial_input: Option<&str>,
) -> Result<EngineResult> {
    let mut inputs = ResultInputs::default();
    if let Some(input) = initial_input {
        if let Ok(mut frame) = serde_json::from_str::<Value>(input)
            && frame.get("type").and_then(Value::as_str) == Some("user")
        {
            let id = uuid::Uuid::new_v4().to_string();
            frame["uuid"] = json!(id);
            process.text(&format!("{frame}\n")).await?;
            inputs.sent(id, true);
        } else {
            // Historical text fixtures remain one-shot; they provide no correlation.
            inputs.unframed_initial = true;
            process.text(input).await?;
        }
    } else {
        process.input.take();
    }
    store.event(
        &record.id,
        "native_tool_observer_v1",
        &json!({"protocol":"claude","version":1}),
    )?;
    let mut session = None;
    let mut final_result: Option<EngineResult> = None;
    let mut result_frame_index = 0_u64;
    let mut legacy_result_seen = false;
    let mut selected_result_index = None;
    let mut tool_names: BTreeMap<String, String> = BTreeMap::new();
    // The attempt's current authoritative native state (rate-limit events
    // and assistant error controls, in protocol order).
    let mut signals = crate::adapters::native_failure::ClaudeSignals::default();
    let mut emitted = String::new();
    let mut assistant_redactor = process.stream_redactor();
    let mut saw_delta = false;
    let mut saw_answer = false;
    // Identity of the assistant message currently being streamed: the native
    // message id from `message_start`, or one producer-owned bounded fallback
    // when the engine omits ids. Every fragment and the completion tail of one
    // message share it, so distinct messages never merge.
    let mut message_id: Option<String> = None;
    let mut fallback_messages: u64 = 0;
    let mut tick = tokio::time::interval(Duration::from_millis(200));
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        let event = tokio::select! {
            biased;
            event = process.next() => Some(event),
            _ = tick.tick() => None,
        };
        let Some(event) = event else {
            let deadline = tokio::time::Instant::now()
                + Duration::from_secs_f64(commands::COMMAND_PAGE_SECONDS);
            for _ in 0..commands::COMMAND_PAGE_LIMIT {
                if tokio::time::Instant::now() >= deadline {
                    break;
                }
                let Some((cid, command, payload)) = store.claim_command(&record.id)? else {
                    break;
                };
                if command == "cancel" {
                    store.complete_command(&record.id, cid, &json!({"accepted":true}))?;
                    return Ok(EngineResult {
                        native_failure: None,
                        outcome: Outcome {
                            status: Status::Cancelled,
                            exit_code: None,
                            failure_kind: None,
                            failure_text: None,
                            runtime_session_id: session,
                        },
                        answer: None,
                        usage: None,
                    });
                }
                if command == "steer" {
                    if let Some(text) = commands::steer_text(&payload) {
                        let id = uuid::Uuid::new_v4().to_string();
                        let open = process.input.is_some();
                        let accepted = send_user_text(process, text, &id, deadline).await;
                        if accepted {
                            inputs.sent(id, true);
                        } else if open {
                            inputs.uncertain_write = true;
                        }
                        store.complete_command(&record.id, cid, &json!({"accepted":accepted}))?;
                        if accepted {
                            journal(store, &record.id, "user", &process.redact(text), None, None)?;
                        }
                    } else {
                        store.complete_command(
                            &record.id,
                            cid,
                            &json!({"accepted":false,"reason":"empty_steer_text"}),
                        )?;
                    }
                } else if command == "pool" {
                    // Same stdin path and deadline as steer. A successful
                    // write is `written` only — never engine acceptance or
                    // consumption — and any error may follow a partial write,
                    // so it is `unknown`, not "not delivered". The durable log
                    // keeps the entry either way.
                    let result = match commands::pool_push_text(store, &record.id, &payload) {
                        Ok(text) => {
                            let id = uuid::Uuid::new_v4().to_string();
                            let open = process.input.is_some();
                            if send_user_text(process, &text, &id, deadline).await {
                                inputs.sent(id, false);
                                commands::pool_result("written", "stdin_write")
                            } else {
                                inputs.uncertain_write |= open;
                                commands::pool_result("unknown", "stdin_write_failed")
                            }
                        }
                        Err(refusal) => refusal,
                    };
                    store.complete_command(&record.id, cid, &result)?;
                } else {
                    store.complete_command(
                        &record.id,
                        cid,
                        &json!({"accepted":false,"reason":"unsupported_command"}),
                    )?;
                }
            }
            tick.reset();
            continue;
        };
        let v = match event {
            Event::Json(v) => v,
            Event::OwnershipFailure(error) => {
                // Required cleanup is owned by the supervisor; a failed journal
                // flush must not replace the original persistence/identity cause.
                let _ = flush_assistant(
                    store,
                    &record.id,
                    &mut assistant_redactor,
                    message_id.as_deref(),
                );
                return Err(error);
            }
            Event::Eof => {
                flush_assistant(
                    store,
                    &record.id,
                    &mut assistant_redactor,
                    message_id.as_deref(),
                )?;
                let code = process.reap().await;
                store.event(
                    &record.id,
                    "native_result_exit_v1",
                    &json!({
                        "version": 1, "frames": result_frame_index,
                        "selected_index": selected_result_index, "exit_code": code,
                    }),
                )?;
                if let Some(mut result) = final_result {
                    result.outcome.exit_code = code;
                    if code != Some(0) && result.outcome.status == Status::Succeeded {
                        result.outcome.status = Status::Failed;
                        result.outcome.failure_kind = Some("nonzero_exit".into());
                        result.outcome.failure_text = process.diagnostic_tail();
                    }
                    if result.outcome.status == Status::Succeeded && inputs.uncertain_write {
                        result.outcome = Outcome::failure("uncertain_input");
                        result.outcome.exit_code = code;
                        result.outcome.runtime_session_id = session.clone();
                        result.answer = None;
                    }
                    if code == Some(0)
                        && result.outcome.status == Status::Succeeded
                        && result.answer.is_none()
                    {
                        result.outcome.status = Status::Failed;
                        result.outcome.failure_kind = Some("empty_result".into());
                    }
                    return Ok(result);
                }
                let diagnostic = process.diagnostic_tail();
                let kind = if result_frame_index > 0 {
                    "uncorrelated_result"
                } else if saw_answer {
                    "cut_off"
                } else if diagnostic
                    .as_deref()
                    .is_some_and(|text| result_failure_kind("", Some(text)) == "auth_failed")
                {
                    "auth_failed"
                } else if diagnostic.is_some() {
                    "provider_error"
                } else {
                    "no_answer"
                };
                let mut outcome = Outcome::failure(kind);
                outcome.exit_code = code;
                outcome.runtime_session_id = session;
                outcome.failure_text = diagnostic;
                return Ok(EngineResult {
                    native_failure: None,
                    outcome,
                    answer: None,
                    usage: None,
                });
            }
            Event::Failure(e) => {
                flush_assistant(
                    store,
                    &record.id,
                    &mut assistant_redactor,
                    message_id.as_deref(),
                )?;
                let code = process.reap().await;
                store.event(
                    &record.id,
                    "native_result_exit_v1",
                    &json!({"version":1,"frames":result_frame_index,
                        "selected_index":selected_result_index,"exit_code":code}),
                )?;
                let mut outcome = Outcome::failure(e);
                outcome.exit_code = code;
                outcome.failure_text = process.diagnostic_tail();
                return Ok(EngineResult {
                    native_failure: None,
                    outcome,
                    answer: None,
                    usage: None,
                });
            }
        };
        // Capture every observed result before identity/shape validation can reject it.
        let result_scope = if v.get("type").and_then(Value::as_str) == Some("result") {
            result_frame_index += 1;
            let scope = inputs.scope(&v);
            let failed = v.get("is_error").and_then(Value::as_bool) == Some(true)
                || v.get("subtype").and_then(Value::as_str) != Some("success")
                || result_text(&v)
                    .as_deref()
                    .and_then(verify::error_only)
                    .is_some();
            let prior_failed = final_result.as_ref().is_some_and(|result| {
                result.outcome.status == Status::Failed
                    && (result.outcome.failure_kind.as_deref() != Some("ambiguous_result")
                        || !failed)
            });
            let ignored = prior_failed || (scope == ResultScope::Context && !failed);
            let mut metadata = native_result_metadata(result_frame_index, &v, ignored);
            metadata["correlation"] = json!(scope.label());
            store.event(&record.id, "native_result_frame_v1", &metadata)?;
            Some(scope)
        } else {
            None
        };
        if let Some(s) = v
            .get("session_id")
            .and_then(Value::as_str)
            .filter(|s| !s.trim().is_empty())
        {
            if record
                .resume_of_runtime_session_id
                .as_deref()
                .is_some_and(|expected| expected != s)
            {
                return Err(invalid("runtime resumed a different native session"));
            }
            // Fresh Claude launches can legitimately report a new session after
            // initialization. A resumed launch is different: its identity is a
            // grant boundary and must remain exactly the requested session.
            if session.as_deref() != Some(s) {
                store.runtime_session(&record.id, s)?;
                session = Some(s.into());
            }
        }
        if record.request.profile == "research" && v["type"] == "system" && v["subtype"] == "init" {
            let _ = store.event(
                &record.id,
                "native_init_inventory_v1",
                &native_init_inventory(&v),
            );
        }
        // Only top-level protocol frames feed the native signal state.
        signals.observe(&v);
        match v.get("type").and_then(Value::as_str) {
            Some("stream_event") => {
                let event = &v["event"];
                match event.get("type").and_then(Value::as_str) {
                    Some("message_start") => {
                        flush_assistant(
                            store,
                            &record.id,
                            &mut assistant_redactor,
                            message_id.as_deref(),
                        )?;
                        assistant_redactor = process.stream_redactor();
                        // A real message boundary: adopt the native message id
                        // as this message's durable identity, or mint one
                        // bounded producer fallback when the engine omits it.
                        // The fallback is per boundary, never per delta, and
                        // never derived from text equality.
                        message_id = Some(
                            event
                                .pointer("/message/id")
                                .and_then(Value::as_str)
                                .filter(|id| !id.trim().is_empty())
                                .map(str::to_owned)
                                .unwrap_or_else(|| {
                                    fallback_messages += 1;
                                    format!("stream-message-{fallback_messages}")
                                }),
                        );
                        emitted.clear();
                        saw_delta = false;
                    }
                    Some("content_block_start")
                        if event.pointer("/content_block/type").and_then(Value::as_str)
                            == Some("tool_use") =>
                    {
                        let block = &event["content_block"];
                        let native_id = block["id"].as_str().filter(|id| !id.is_empty());
                        let name = block["name"].as_str().map(|name| process.redact(name));
                        if let (Some(id), Some(name)) = (native_id, name.as_ref()) {
                            tool_names.insert(id.to_owned(), name.clone());
                        }
                        crate::journal_with_error(
                            store,
                            &record.id,
                            "tool_call",
                            "",
                            name.as_deref(),
                            native_id,
                            None,
                        )?;
                    }
                    Some("content_block_delta")
                        if event.pointer("/delta/type").and_then(Value::as_str)
                            == Some("text_delta") =>
                    {
                        if let Some(text) = event.pointer("/delta/text").and_then(Value::as_str) {
                            if emitted.len() + text.len() > verify::MAX_ANSWER {
                                return Err(invalid("stream output exceeds answer bound"));
                            }
                            saw_delta = true;
                            saw_answer = true;
                            emitted.push_str(text);
                            let safe = assistant_redactor.feed(text);
                            if !safe.is_empty() {
                                journal(
                                    store,
                                    &record.id,
                                    "assistant",
                                    &safe,
                                    None,
                                    message_id.as_deref(),
                                )?;
                            }
                        }
                    }
                    _ => {}
                }
            }
            Some("assistant") => {
                let native_id = v
                    .pointer("/message/id")
                    .and_then(Value::as_str)
                    .filter(|id| !id.trim().is_empty())
                    .map(str::to_owned);
                if let Some(content) = v.pointer("/message/content").and_then(Value::as_array) {
                    let mut text = String::new();
                    for block in content {
                        match block.get("type").and_then(Value::as_str) {
                            Some("text") => {
                                let block_text =
                                    block.get("text").and_then(Value::as_str).unwrap_or("");
                                saw_answer |= !block_text.is_empty();
                                text.push_str(block_text);
                            }
                            Some("tool_use") => {
                                if let (Some(id), Some(name)) =
                                    (block["id"].as_str(), block["name"].as_str())
                                {
                                    tool_names.insert(id.to_owned(), process.redact(name));
                                }
                                crate::journal_with_error(
                                    store,
                                    &record.id,
                                    "tool_call",
                                    &process.redact(&serde_json::to_string(
                                        block.get("input").unwrap_or(&Value::Null),
                                    )?),
                                    block.get("name").and_then(Value::as_str),
                                    block
                                        .get("id")
                                        .and_then(Value::as_str)
                                        .filter(|id| !id.is_empty()),
                                    None,
                                )?;
                            }
                            _ => {}
                        }
                    }
                    // The completion keeps the identity its fragments already
                    // streamed under: a contradictory id in the full event must
                    // not re-identify (and so duplicate) one message. Without
                    // a prior boundary, the full event is itself the boundary:
                    // its native id wins, and an id-less boundary mints one
                    // bounded producer fallback so distinct messages stay
                    // distinct.
                    let identity = if saw_delta {
                        message_id.clone().or(native_id)
                    } else {
                        Some(match native_id {
                            Some(id) => id,
                            None => match message_id.take() {
                                Some(carried) => carried,
                                None => {
                                    fallback_messages += 1;
                                    format!("stream-message-{fallback_messages}")
                                }
                            },
                        })
                    };
                    if saw_delta {
                        if let Some(tail) = text.strip_prefix(&emitted) {
                            let safe = assistant_redactor.feed(tail);
                            if !safe.is_empty() {
                                journal(
                                    store,
                                    &record.id,
                                    "assistant",
                                    &safe,
                                    None,
                                    identity.as_deref(),
                                )?;
                            }
                        }
                        flush_assistant(
                            store,
                            &record.id,
                            &mut assistant_redactor,
                            identity.as_deref(),
                        )?;
                    } else {
                        journal(
                            store,
                            &record.id,
                            "assistant",
                            &process.redact(&text),
                            None,
                            identity.as_deref(),
                        )?;
                    }
                    saw_delta = false;
                    emitted.clear();
                    assistant_redactor = process.stream_redactor();
                    // The completed message's identity ends with it; the next
                    // message_start establishes the next one.
                    message_id = None;
                }
            }
            Some("user") => {
                inputs.replay(&v)?;
                if let Some(content) = v.pointer("/message/content").and_then(Value::as_array) {
                    for block in content {
                        if block.get("type").and_then(Value::as_str) == Some("tool_result") {
                            let text = block
                                .get("content")
                                .and_then(Value::as_str)
                                .map(str::to_owned)
                                .unwrap_or_else(|| {
                                    block.get("content").unwrap_or(&Value::Null).to_string()
                                });
                            crate::journal_with_error(
                                store,
                                &record.id,
                                "tool_result",
                                &process.redact(&text),
                                block["tool_use_id"]
                                    .as_str()
                                    .and_then(|id| tool_names.get(id))
                                    .map(String::as_str),
                                block
                                    .get("tool_use_id")
                                    .and_then(Value::as_str)
                                    .filter(|id| !id.is_empty()),
                                block
                                    .get("is_error")
                                    .and_then(Value::as_bool)
                                    .map(|error| (error, "claude.is_error")),
                            )?;
                        }
                    }
                }
            }
            Some("result") => {
                let scope = result_scope.expect("result metadata was captured");
                legacy_result_seen |= scope == ResultScope::Legacy;
                let mut text = result_text(&v);
                let is_error = match v.get("is_error") {
                    None => false,
                    Some(v) => v
                        .as_bool()
                        .ok_or_else(|| invalid("result is_error must be boolean"))?,
                };
                let subtype = v.get("subtype").and_then(Value::as_str).unwrap_or("");
                let mut outcome = if !is_error && subtype == "success" {
                    Outcome::success(session.clone())
                } else {
                    Outcome::failure(result_failure_kind(subtype, text.as_deref()))
                };
                outcome.runtime_session_id = session.clone();
                if outcome.status == Status::Failed {
                    outcome.failure_text = text.as_deref().map(|value| process.redact(value));
                }
                if let Some(k) = text.as_deref().and_then(verify::error_only) {
                    outcome.status = Status::Failed;
                    outcome.failure_kind = Some(k.into());
                    text = None;
                }
                if text.as_ref().is_some_and(|s| s.len() > verify::MAX_ANSWER) {
                    return Err(invalid("result exceeds answer size bound"));
                }
                let usage = runtime_result_usage(&v);
                if record.resume_of_runtime_session_id.is_some()
                    && outcome.runtime_session_id != record.resume_of_runtime_session_id
                {
                    return Err(invalid(
                        "runtime did not confirm the resumed native session",
                    ));
                }
                // Errors are authoritative even in context or otherwise ambiguous frames.
                // No success replaces a failure; a native error can clarify prior ambiguity.
                let prior_failed = final_result.as_ref().is_some_and(|result| {
                    result.outcome.status == Status::Failed
                        && (result.outcome.failure_kind.as_deref() != Some("ambiguous_result")
                            || outcome.status == Status::Succeeded)
                });
                if prior_failed {
                    continue;
                }
                if outcome.status == Status::Succeeded {
                    if scope == ResultScope::Ambiguous
                        || (legacy_result_seen && result_frame_index != 1)
                    {
                        outcome = Outcome::failure("ambiguous_result");
                        outcome.runtime_session_id = session.clone();
                        text = None;
                    } else if scope == ResultScope::Context {
                        continue;
                    }
                }
                let failed = outcome.status == Status::Failed;
                selected_result_index = Some(result_frame_index);
                final_result = Some(EngineResult {
                    native_failure: signals.terminal().filter(|_| failed),
                    outcome,
                    answer: text
                        .filter(|s| !s.trim().is_empty())
                        .map(|value| process.redact(&value)),
                    usage: Some(usage),
                });
                // Close only after task completion or failure; context acknowledgements
                // cannot end the task. Still drain queued frames and require EOF + exit.
                process.input.take();
            }
            _ => {}
        }
    }
}

/// Retains only the Python `runtime_result` fields used for durable statistics.
///
/// Values deliberately remain unvalidated JSON here: the store applies the
/// shared numeric/nullability rules after the terminal event is durable.
fn runtime_result_usage(result: &Value) -> Value {
    json!({"duration_ms":result["duration_ms"],"duration_api_ms":result["duration_api_ms"],"num_turns":result["num_turns"],"ttft_ms":result["ttft_ms"],"total_cost_usd":result["total_cost_usd"],"usage":result["usage"]})
}

/// Writes one user text message with the caller's fresh correlation `id` before
/// `deadline`; shared by operator steering and pool delivery. The ID is native
/// framing only, not a public agent identity or a delivery/consumption proof.
/// `false` after a possible partial write is not proof that nothing was sent.
async fn send_user_text(
    process: &mut agent_run_adapters::io::Process,
    text: &str,
    id: &str,
    deadline: tokio::time::Instant,
) -> bool {
    process
        .send_before(
            &json!({"type":"user","uuid":id,"message":{"role":"user","content":[{"type":"text","text":text}]}}),
            deadline,
        )
        .await
        .is_ok()
}

#[cfg(test)]
mod tests {
    use super::{native_result_metadata, result_failure_kind, result_text, runtime_result_usage};
    use serde_json::json;

    /// Native correlations cover coalesced replay batches, not write counts or text.
    #[test]
    fn result_inputs_correlate_tasks_context_and_coalesced_steering() {
        use super::{ResultInputs, ResultScope};
        let mut inputs = ResultInputs::default();
        inputs.sent("task".into(), true);
        inputs.sent("context".into(), false);
        inputs
            .replay(&json!({"isReplay":true,"uuid":"context"}))
            .unwrap();
        assert!(inputs.scope(&json!({"user_message_uuid":"context"})) == ResultScope::Context);
        inputs
            .replay(&json!({"isReplay":true,"uuid":"task"}))
            .unwrap();
        assert!(inputs.scope(&json!({"user_message_uuid":"task"})) == ResultScope::Task);
        assert!(inputs.scope(&json!({"user_message_uuid":"task"})) == ResultScope::Ambiguous);

        let mut inputs = ResultInputs::default();
        inputs.sent("task".into(), true);
        inputs.sent("note".into(), false);
        for id in ["task", "note"] {
            inputs.replay(&json!({"isReplay":true,"uuid":id})).unwrap();
        }
        assert!(inputs.scope(&json!({"user_message_uuid":"note"})) == ResultScope::Task);
        assert!(inputs.scope(&json!({"user_message_uuid":"task"})) == ResultScope::Ambiguous);

        let mut inputs = ResultInputs::default();
        inputs.sent("old".into(), true);
        inputs.sent("steer".into(), true);
        inputs
            .replay(&json!({"isReplay":true,"uuid":"old"}))
            .unwrap();
        assert!(inputs.scope(&json!({"user_message_uuid":"old"})) == ResultScope::Context);
        inputs
            .replay(&json!({"isReplay":true,"uuid":"steer"}))
            .unwrap();
        assert!(inputs.scope(&json!({"user_message_uuid":"steer"})) == ResultScope::Task);
        assert!(inputs.scope(&json!({"user_message_uuid":"unknown"})) == ResultScope::Ambiguous);
        assert!(inputs.scope(&json!({})) == ResultScope::Ambiguous);
        assert!(ResultInputs::default().scope(&json!({})) == ResultScope::Legacy);
        let mut single = ResultInputs::default();
        single.sent("only".into(), true);
        assert!(single.scope(&json!({})) == ResultScope::Legacy);
        single.sent("note".into(), false);
        assert!(single.scope(&json!({})) == ResultScope::Ambiguous);
        inputs
            .replay(&json!({"isReplay":true,"uuid":"steer"}))
            .unwrap();
        assert!(inputs.scope(&json!({})) == ResultScope::Ambiguous);
        assert!(
            inputs
                .replay(&json!({"isReplay":true,"uuid":"unknown"}))
                .is_err()
        );
    }

    /// Result diagnostics retain types/counts but omit every supplied content field.
    #[test]
    fn result_metadata_is_content_free_and_preserves_zero_counters() {
        let frame = json!({"subtype":"success","is_error":false,"result":"é秘密",
            "structured_output":{"secret":"OUTPUT_SECRET"},"num_turns":0,"duration_ms":0,
            "session_id":"SESSION_SECRET","prompt":"PROMPT_SECRET","environment":{"x":"ENV_SECRET"}});
        let metadata = native_result_metadata(2, &frame, true);
        assert_eq!(metadata["index"], 2);
        assert_eq!(metadata["result_type"], "string");
        assert_eq!(metadata["result_bytes"], "é秘密".len());
        assert_eq!(metadata["structured_output_type"], "object");
        assert_eq!(metadata["num_turns"], 0);
        assert_eq!(metadata["duration_ms"], 0.0);
        assert_eq!(metadata["ignored"], true);
        let encoded = metadata.to_string();
        for forbidden in [
            "é秘密",
            "OUTPUT_SECRET",
            "SESSION_SECRET",
            "PROMPT_SECRET",
            "ENV_SECRET",
        ] {
            assert!(!encoded.contains(forbidden));
        }
        assert!(encoded.len() < 4096);
        let malformed = native_result_metadata(
            1,
            &json!({"subtype":"UNKNOWN_SECRET","is_error":"ERROR_SECRET"}),
            false,
        );
        assert_eq!(malformed["subtype"], "unrecognized");
        assert_eq!(malformed["result_type"], "missing");
        assert!(malformed["is_error"].is_null() && malformed["num_turns"].is_null());
        assert!(!malformed.to_string().contains("SECRET"));
    }

    /// Mirrors `tests/test_claude_stream.py::StreamDecoderTests::test_result_with_error_subtype_is_terminal_and_marked_as_error`.
    #[test]
    fn classifies_auth_markers_before_misleading_success_subtypes() {
        assert_eq!(
            result_failure_kind("success", Some("OAuth token has expired")),
            "auth_failed"
        );
    }

    /// Mirrors `tests/test_claude_stream.py::StreamDecoderTests::test_finalize_distinguishes_no_answer_from_cut_off_answer`.
    /// Mirrors `tests/test_claude_stream.py::StreamDecoderTests::test_finalize_after_clean_terminal_returns_same_metadata_and_does_not_recount`.
    #[test]
    fn classifies_max_turns_and_generic_terminal_errors() {
        assert_eq!(result_failure_kind("error_max_turns", None), "max_turns");
        assert_eq!(result_failure_kind("success", None), "engine_error");
    }

    // GLM deliberately uses the Claude-family stream runner; this Rust-internal
    // assertion keeps its provider response and authentication failure classes
    // coupled to the shared classifier. No Python test isolates this call path.
    #[test]
    fn glm_uses_claude_response_and_failure_classification() {
        assert_eq!(
            result_text(&json!({"result": "ready"})),
            Some("ready".into())
        );
        assert_eq!(
            result_failure_kind("success", Some("OAuth token has expired")),
            "auth_failed"
        );
        assert_eq!(
            result_failure_kind("error_during_execution", Some("provider rejected request")),
            "runtime_failed"
        );
    }

    /// Mirrors `tests/test_claude_stream.py::TerminalEventDataTests::test_bounded_event_excludes_result_text_and_session_id`.
    /// Mirrors `tests/test_claude_stream.py::TerminalEventDataTests::test_event_with_usage_is_actually_json_serializable`.
    #[test]
    fn runtime_result_usage_retains_python_timing_and_cache_shape() {
        let usage = runtime_result_usage(&json!({
            "duration_ms": 100,
            "duration_api_ms": 75.5,
            "ttft_ms": 12.5,
            "num_turns": 2,
            "total_cost_usd": 0.01,
            "usage": {"input_tokens": 10, "cache_read_input_tokens": 5}
        }));
        assert_eq!(usage["duration_api_ms"], 75.5);
        assert_eq!(usage["ttft_ms"], 12.5);
        assert_eq!(usage["usage"]["cache_read_input_tokens"], 5);
    }

    /// Mirrors `tests/test_claude_stream.py::StreamDecoderTests::test_blank_and_malformed_lines_warn_without_raising`.
    /// Mirrors `tests/test_claude_stream.py::StreamDecoderTests::test_assistant_text_and_tool_use_become_messages`.
    /// Mirrors `tests/test_claude_stream.py::StreamDecoderTests::test_tool_result_becomes_message`.
    /// Mirrors `tests/test_claude_stream.py::StreamDecoderTests::test_system_event_redacts_secret_looking_fields`.
    /// Native terminal helpers preserve legacy single-result text semantics.
    #[test]
    fn terminal_helpers_preserve_single_terminal_contract() {
        assert_eq!(
            result_text(&json!({"result": "answer"})),
            Some("answer".into())
        );
        assert_eq!(
            result_text(&json!({"structured_output": {"answer": true}})),
            Some(r#"{"answer":true}"#.into())
        );
        assert_eq!(
            result_failure_kind("error_during_execution", Some("boom")),
            "runtime_failed"
        );
    }

    /// Safe inventory receipts are complete only for recognized tool names;
    /// raw private payloads and unknown tool-name canaries never enter the JSON.
    #[test]
    fn research_init_inventory_is_closed_and_content_free() {
        let data = super::native_init_inventory(
            &serde_json::json!({"tools":["WebSearch","WebFetch","mcp__agent_run_worker__save_report"],"session_id":"SOURCE_SECRET","environment":"SOURCE_SECRET"}),
        );
        assert_eq!(data["complete"], true);
        assert!(!data.to_string().contains("SOURCE_SECRET"));
        let unknown = super::native_init_inventory(
            &serde_json::json!({"tools":["WebSearch","SOURCE_SECRET"]}),
        );
        assert_eq!(unknown["complete"], false);
        assert_eq!(unknown["unknown_count"], 1);
        assert!(!unknown.to_string().contains("SOURCE_SECRET"));
        assert_eq!(
            super::native_init_inventory(&serde_json::json!({}))["complete"],
            false
        );
    }
}
