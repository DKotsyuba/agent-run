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
pub fn plan(
    config: &Config,
    runtime: &Runtime,
    record: &Record,
    role: &Profile,
    home: &Path,
    app_home: &Path,
    snapshot: &Snapshot,
) -> Result<LaunchPlan> {
    let kind = runtime.kind()?;
    let mut env = agent_run_adapters::environment(
        config,
        runtime,
        role,
        home,
        record.request.account.as_deref(),
        app_home,
    )?;
    let req = &record.request;
    let (mut args, input) = if kind == Adapter::Qwen {
        env.insert("OPENAI_MODEL".into(), req.model.clone());
        #[cfg(target_os = "macos")]
        {
            let xcode = std::process::Command::new("/usr/bin/xcrun")
                .args(["--find", "git"])
                .output()?;
            if !xcode.status.success() {
                return Err(invalid("Qwen requires an installed Xcode Git toolchain"));
            }
            let git = String::from_utf8_lossy(&xcode.stdout).trim().to_owned();
            let parent = Path::new(&git)
                .parent()
                .ok_or_else(|| invalid("invalid Xcode Git path"))?;
            env.insert(
                "PATH".into(),
                format!(
                    "{}:{}",
                    parent.display(),
                    env.get("PATH").cloned().unwrap_or_default()
                ),
            );
        }
        (
            vec![
                "-p".into(),
                req.task.clone(),
                "--output-format".into(),
                "stream-json".into(),
                "--approval-mode".into(),
                if role.write { "yolo" } else { "plan" }.into(),
                "--sandbox".into(),
                "--model".into(),
                req.model.clone(),
            ],
            None,
        )
    } else {
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
pub async fn run(
    process: &mut Process,
    store: &mut Store,
    record: &Record,
    kind: Adapter,
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
        let event = tokio::select! {event=process.next()=>Some(event),_=tick.tick()=>None};
        let Some(event) = event else {
            process.owner.refresh();
            while let Some((cid, command, payload)) = store.claim_command(&record.id)? {
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
                        let accepted=kind!=Adapter::Qwen&&process.send(&json!({"type":"user","message":{"role":"user","content":[{"type":"text","text":text}]}})).await.is_ok();
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
            if session.as_deref().is_some_and(|old| old != s) {
                return Err(invalid("runtime session identity changed during a run"));
            }
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
                if kind == Adapter::Qwen
                    && text
                        .as_deref()
                        .is_some_and(|t| t.trim_start().starts_with("[API Error:"))
                {
                    outcome = Outcome::failure("provider_error");
                    outcome.runtime_session_id = session.clone();
                    outcome.failure_text = text.as_deref().map(|value| {
                        value
                            .lines()
                            .next()
                            .unwrap_or_default()
                            .chars()
                            .take(500)
                            .collect()
                    });
                    text = None;
                }
                if let Some(k) = text.as_deref().and_then(verify::error_only) {
                    outcome.status = Status::Failed;
                    outcome.failure_kind = Some(k.into());
                    text = None;
                }
                if text.as_ref().is_some_and(|s| s.len() > verify::MAX_ANSWER) {
                    return Err(invalid("result exceeds answer size bound"));
                }
                let u = &v["usage"];
                let input = u.get("input_tokens").and_then(Value::as_i64);
                let output = u.get("output_tokens").and_then(Value::as_i64);
                let usage = json!({"input_tokens":input,"output_tokens":output,"cache_read_tokens":u["cache_read_input_tokens"],"cache_write_tokens":u["cache_creation_input_tokens"],"total_tokens":input.zip(output).and_then(|(a,b)|if a>=0&&b>=0{a.checked_add(b)}else{None}),"duration_ms":v["duration_ms"],"num_turns":v["num_turns"],"cost_usd":v["total_cost_usd"]});
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

#[cfg(test)]
mod tests {
    use super::result_failure_kind;

    /// Mirrors `test_claude_stream.py::test_classify_failure_auth_markers`.
    #[test]
    fn classifies_auth_markers_before_misleading_success_subtypes() {
        assert_eq!(
            result_failure_kind("success", Some("OAuth token has expired")),
            "auth_failed"
        );
    }

    /// Mirrors `test_claude_stream.py::test_classify_failure_max_turns`.
    #[test]
    fn classifies_max_turns_and_generic_terminal_errors() {
        assert_eq!(result_failure_kind("error_max_turns", None), "max_turns");
        assert_eq!(result_failure_kind("success", None), "engine_error");
    }
}
