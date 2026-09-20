use crate::{commands, journal};
use crate::{
    config::{Adapter, Config, Runtime},
    domain::{Outcome, Status},
    error::invalid,
    profiles::Profile,
    state::{Record, Store},
    verify, Result,
};
use agent_run_adapters::{
    io::{Event, Process},
    materialize::Snapshot,
    EngineResult, LaunchPlan,
};
use serde_json::{json, Value};
use std::{path::Path, time::Duration};

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
/// instead reject any identifier other than their requested session. The
/// function journals recognized events, services bounded control commands, and
/// returns malformed stream data or persistence failures as domain errors.
pub async fn run(
    process: &mut Process,
    store: &mut Store,
    record: &Record,
    initial_input: Option<&str>,
) -> Result<EngineResult> {
    if let Some(input) = initial_input {
        process.text(input).await?;
    } else {
        process.input.take();
    }
    let mut session = None;
    let mut final_result: Option<EngineResult> = None;
    let mut emitted = String::new();
    let mut saw_delta = false;
    let mut saw_answer = false;
    let mut tick = tokio::time::interval(Duration::from_millis(200));
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        let event = tokio::select! {
            biased;
            event = process.next() => Some(event),
            _ = tick.tick() => None,
        };
        let Some(event) = event else {
            process.owner.refresh();
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
                        let accepted = process
                            .send_before(
                                &json!({"type":"user","message":{"role":"user","content":[{"type":"text","text":text}]}}),
                                deadline,
                            )
                            .await
                            .is_ok();
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
            Event::Eof => {
                let code = process.reap().await;
                if let Some(mut result) = final_result {
                    result.outcome.exit_code = code;
                    if code != Some(0) && result.outcome.status == Status::Succeeded {
                        result.outcome.status = Status::Failed;
                        result.outcome.failure_kind = Some("nonzero_exit".into());
                        result.outcome.failure_text = process.diagnostic_tail();
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
                let kind = if saw_answer {
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
                    outcome,
                    answer: None,
                    usage: None,
                });
            }
            Event::Failure(e) => {
                let code = process.reap().await;
                let mut outcome = Outcome::failure(e);
                outcome.exit_code = code;
                outcome.failure_text = process.diagnostic_tail();
                return Ok(EngineResult {
                    outcome,
                    answer: None,
                    usage: None,
                });
            }
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
            store.runtime_session(&record.id, s)?;
            session = Some(s.into());
        }
        match v.get("type").and_then(Value::as_str) {
            Some("stream_event") => {
                let event = &v["event"];
                match event.get("type").and_then(Value::as_str) {
                    Some("message_start") => {
                        emitted.clear();
                        saw_delta = false;
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
                            journal(
                                store,
                                &record.id,
                                "assistant",
                                &process.redact(text),
                                None,
                                None,
                            )?;
                        }
                    }
                    _ => {}
                }
            }
            Some("assistant") => {
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
                            Some("tool_use") => journal(
                                store,
                                &record.id,
                                "tool_call",
                                &process.redact(&serde_json::to_string(
                                    block.get("input").unwrap_or(&Value::Null),
                                )?),
                                block.get("name").and_then(Value::as_str),
                                block.get("id").and_then(Value::as_str),
                            )?,
                            _ => {}
                        }
                    }
                    if saw_delta {
                        if let Some(tail) = text.strip_prefix(&emitted) {
                            journal(
                                store,
                                &record.id,
                                "assistant",
                                &process.redact(tail),
                                None,
                                None,
                            )?;
                        }
                    } else {
                        journal(
                            store,
                            &record.id,
                            "assistant",
                            &process.redact(&text),
                            None,
                            None,
                        )?;
                    }
                    saw_delta = false;
                    emitted.clear();
                }
            }
            Some("user") => {
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
                            journal(
                                store,
                                &record.id,
                                "tool_result",
                                &process.redact(&text),
                                None,
                                block.get("tool_use_id").and_then(Value::as_str),
                            )?;
                        }
                    }
                }
            }
            Some("result") => {
                if final_result.is_some() {
                    continue;
                }
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
                    outcome.failure_text = text.clone();
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
                final_result = Some(EngineResult {
                    outcome,
                    answer: text.filter(|s| !s.is_empty()),
                    usage: Some(usage),
                });
                // The one-shot stream must close after its result; EOF plus exit status remains required.
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

#[cfg(test)]
mod tests {
    use super::{result_failure_kind, result_text, runtime_result_usage};
    use serde_json::json;

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
    /// Mirrors `tests/test_claude_stream.py::StreamDecoderTests::test_terminal_line_captures_metadata_and_rejects_duplicates`.
    /// Mirrors `tests/test_claude_stream.py::StreamDecoderTests::test_replays_the_captured_double_init_result_cycle_and_settles_on_the_first`.
    #[test]
    fn terminal_helpers_preserve_first_terminal_contract() {
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
}
