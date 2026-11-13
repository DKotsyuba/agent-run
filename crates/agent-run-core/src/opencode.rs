//! OpenCode v2 runner over the shared owned process, command queue and finish contract.
use crate::{
    Result, commands,
    domain::{Outcome, Status},
    persist::Persist,
    state::Record,
};
use agent_run_adapters::{
    EngineResult,
    io::{Event, Process},
    opencode::{self, Client},
    provider::ProviderLaunchPlan,
};
use agent_run_domain::error::invalid;
use reqwest::Method;
use serde_json::{Value, json};
use std::{
    collections::{BTreeMap, BTreeSet},
    path::Path,
    time::Duration,
};

/// Native snapshot reconciliation is bounded to this many messages per session.
const MAX_MESSAGES: usize = 4096;
/// Snapshot bytes retained while reconciling one native session.
const MAX_SNAPSHOT: usize = 64 * 1024 * 1024;

/// Returns only a v2 data envelope; malformed bare payloads remain null.
fn data(value: &Value) -> &Value {
    value.get("data").unwrap_or(&Value::Null)
}

/// Requires a plain native identifier before interpolation into a local API route.
fn identifier(value: &Value) -> Result<&str> {
    value
        .as_str()
        .filter(|s| {
            !s.is_empty()
                && s.len() <= 256
                && s.bytes()
                    .all(|b| b.is_ascii_alphanumeric() || b"_-".contains(&b))
        })
        .ok_or_else(|| invalid("OpenCode returned an invalid native identifier"))
}

/// Reads a complete bounded timeline in source order; pagination is never silently omitted.
async fn messages(client: &Client, session: &str) -> Result<Vec<Value>> {
    let mut result = Vec::new();
    let mut bytes = 0;
    let mut cursor: Option<String> = None;
    let mut cursors = BTreeSet::new();
    loop {
        let mut route = format!("/api/session/{session}/message?limit=100");
        if let Some(cursor) = cursor {
            route.push_str("&cursor=");
            route.push_str(&opencode::query_value(&cursor));
        } else {
            route.push_str("&order=asc");
        }
        let page = client
            .call_for(Method::GET, &route, None, Duration::from_secs(2))
            .await?;
        let rows = page["data"]
            .as_array()
            .ok_or_else(|| invalid("OpenCode message page is missing"))?;
        bytes += serde_json::to_vec(rows)?.len();
        if result.len() + rows.len() > MAX_MESSAGES || bytes > MAX_SNAPSHOT {
            return Err(invalid("OpenCode session snapshot exceeds bound"));
        }
        result.extend(rows.iter().cloned());
        match page.pointer("/cursor/next").and_then(Value::as_str) {
            Some(next) if cursors.insert(next.to_owned()) => cursor = Some(next.to_owned()),
            Some(_) => return Err(invalid("OpenCode message pagination repeated its cursor")),
            None => return Ok(result),
        }
    }
}

/// Returns the owned server's version when `/api/info` names this exact child
/// process and one qualified OpenCode release; anything else refuses.
fn server_version(info: &Value, pid: i32) -> Result<&str> {
    match info["version"].as_str() {
        Some(version)
            if info["pid"].as_i64() == Some(i64::from(pid))
                && crate::continuity::opencode_version_qualified(version) =>
        {
            Ok(version)
        }
        _ => Err(invalid("OpenCode server identity/version did not verify")),
    }
}

/// Text representations emitted by a completed native MCP/tool result.
fn content_text(content: &Value) -> String {
    content
        .as_array()
        .map(|parts| {
            parts
                .iter()
                .filter_map(|part| part.get("text").and_then(Value::as_str))
                .collect::<Vec<_>>()
                .join("\n")
        })
        .unwrap_or_default()
}

/// Writes one item and its replay identity atomically through the existing producer.
/// Different content for a previously complete item is a protocol refusal.
#[allow(clippy::too_many_arguments)]
async fn item(
    process: &mut Process,
    store: &mut Persist,
    record: &Record,
    seen: &mut BTreeMap<String, String>,
    key: String,
    role: &str,
    text: &str,
    name: Option<&str>,
    native_ref: Option<&str>,
    error: Option<bool>,
) -> Result<()> {
    let text = process.redact(text);
    let name = name.map(|value| process.redact(value));
    let name = name.as_deref();
    let digest = crate::fs::sha256(&serde_json::to_vec(&(role, &text, name, error))?);
    if let Some(previous) = seen.get(&key) {
        return if *previous == digest {
            Ok(())
        } else {
            Err(invalid("OpenCode changed a settled native item"))
        };
    }
    process
        .observing(store.native_item(
            &record.id,
            role,
            &text,
            name,
            native_ref,
            error.map(|value| (value, "opencode.tool.state")),
            json!({"key":key,"sha256":digest}),
        ))
        .await?;
    seen.insert(key, digest);
    Ok(())
}

/// One settled native observation of an admitted message, in source order.
#[derive(Debug, PartialEq)]
enum Projected {
    /// A replayable journal item keyed by its native identity.
    Item {
        key: String,
        role: &'static str,
        text: String,
        name: Option<String>,
        native_ref: Option<String>,
        error: Option<bool>,
    },
    /// A completed worker `finish` call whose result text is the receipt.
    Finish(String),
}

/// Projects one native message into settled items without side effects.
/// Input/notification messages and assistant text/tool parts are recognized;
/// every other message type (for example 2.3.0 `idle`) and part type (for
/// example `reasoning`) is skipped, so newer releases may add metadata without
/// widening what Run journals. Partial text and streaming tools are not settled.
/// Returns the steps and the native failure kind named by an assistant error.
fn project(message: &Value, finish_name: &str) -> Result<(Vec<Projected>, Option<String>)> {
    let id = identifier(&message["id"])?;
    let mut steps = Vec::new();
    if let Some(kind @ ("user" | "synthetic")) = message["type"].as_str() {
        let Some(text) = message["text"].as_str() else {
            if kind == "synthetic" {
                return Err(invalid("OpenCode notification text is missing"));
            }
            return Ok((steps, None));
        };
        if !text.is_empty() {
            steps.push(Projected::Item {
                key: format!("{id}:input"),
                role: "user",
                text: text.to_owned(),
                name: Some(
                    if kind == "synthetic" {
                        "opencode.notification"
                    } else {
                        "opencode.input"
                    }
                    .to_owned(),
                ),
                native_ref: None,
                error: None,
            });
        }
        return Ok((steps, None));
    }
    if message["type"] != "assistant" {
        return Ok((steps, None));
    }
    let failure = message.get("error").filter(|v| !v.is_null()).map(|error| {
        error["type"]
            .as_str()
            .filter(|s| {
                !s.is_empty()
                    && s.len() <= 128
                    && s.bytes()
                        .all(|b| b.is_ascii_alphanumeric() || b"_.-".contains(&b))
            })
            .unwrap_or("opencode_native_failed")
            .to_owned()
    });
    let Some(parts) = message["content"].as_array() else {
        return Err(invalid("OpenCode assistant content is missing"));
    };
    for (ordinal, part) in parts.iter().enumerate() {
        if part["type"] == "text"
            && message
                .pointer("/time/completed")
                .is_some_and(|value| !value.is_null())
        {
            let text = part["text"]
                .as_str()
                .ok_or_else(|| invalid("OpenCode text is malformed"))?;
            steps.push(Projected::Item {
                key: format!("{id}:{ordinal}:text"),
                role: "assistant",
                text: text.to_owned(),
                name: None,
                native_ref: None,
                error: None,
            });
        }
        if part["type"] != "tool" {
            continue;
        }
        let tool_id = part["id"]
            .as_str()
            .filter(|s| !s.is_empty() && s.len() <= 1024)
            .ok_or_else(|| invalid("OpenCode tool identity is missing"))?;
        let name = part["name"]
            .as_str()
            .filter(|s| !s.is_empty() && s.len() <= 256)
            .ok_or_else(|| invalid("OpenCode tool name is missing"))?;
        let state = &part["state"];
        let status = state["status"]
            .as_str()
            .ok_or_else(|| invalid("OpenCode tool state is missing"))?;
        if status == "streaming" {
            continue;
        }
        if !["running", "completed", "error"].contains(&status) || !state["input"].is_object() {
            return Err(invalid("OpenCode tool lifecycle is malformed"));
        }
        let key = crate::fs::sha256(format!("{id}\0{tool_id}").as_bytes());
        let native_ref = format!("exec-opencode-{key}");
        steps.push(Projected::Item {
            key: format!("{key}:call"),
            role: "tool_call",
            text: serde_json::to_string(&state["input"])?,
            name: Some(name.to_owned()),
            native_ref: Some(native_ref.clone()),
            error: None,
        });
        if status == "running" {
            continue;
        }
        let mut text = content_text(&state["content"]);
        if status == "error" && text.is_empty() {
            text = serde_json::to_string(&state["error"])?;
        }
        let finished = name == finish_name && status == "completed";
        steps.push(Projected::Item {
            key: format!("{key}:result"),
            role: "tool_result",
            text: text.clone(),
            name: Some(name.to_owned()),
            native_ref: Some(native_ref),
            error: Some(status == "error"),
        });
        if finished {
            steps.push(Projected::Finish(text));
        }
    }
    Ok((steps, failure))
}

/// Reconciles only newly admitted native messages; source tool identities own receipts.
/// Native input/notification IDs are journaled once; the initial task echo is excluded.
/// Partial reasoning/text is not mistaken for a settled replayable item.
async fn reconcile(
    process: &mut Process,
    store: &mut Persist,
    record: &Record,
    rows: &[Value],
    baseline: &BTreeSet<String>,
    seen: &mut BTreeMap<String, String>,
) -> Result<Option<String>> {
    let finish_name = format!(
        "{}_finish",
        opencode::tool_prefix(agent_run_domain::worker::SERVER_NAME)
    );
    let mut failure = None;
    for message in rows {
        if baseline.contains(identifier(&message["id"])?) {
            continue;
        }
        let (steps, failed) = project(message, &finish_name)?;
        if failed.is_some() {
            failure = failed;
        }
        for step in steps {
            match step {
                Projected::Item {
                    key,
                    role,
                    text,
                    name,
                    native_ref,
                    error,
                } => {
                    item(
                        process,
                        store,
                        record,
                        seen,
                        key,
                        role,
                        &text,
                        name.as_deref(),
                        native_ref.as_deref(),
                        error,
                    )
                    .await?;
                }
                Projected::Finish(text) => {
                    crate::worker_finish::receipt(process, store, record, &text).await?;
                }
            }
        }
    }
    Ok(failure)
}

/// Rejects every native ask for this exact session; no approval is inferred or saved.
async fn reject_asks(client: &Client, session: &str) -> Result<()> {
    let pending = client
        .call_for(
            Method::GET,
            &format!("/api/session/{session}/permission"),
            None,
            Duration::from_secs(2),
        )
        .await?;
    let requests = data(&pending)
        .as_array()
        .ok_or_else(|| invalid("OpenCode permission inventory is malformed"))?;
    for request in requests {
        if request["sessionID"] != session {
            return Err(invalid("OpenCode permission belongs to another session"));
        }
        let id = identifier(&request["id"])?;
        client.call_for(Method::POST,&format!("/api/session/{session}/permission/{id}/reply"),Some(json!({"decision":"reject","message":"The admitted role does not authorize this action."})),Duration::from_secs(2)).await?;
    }
    Ok(())
}

/// Maps measured native cumulative counters to the common runtime-result schema.
/// Missing/decreasing values stay unknown; final usage requires a completed pass.
fn usage(current: &Value, baseline: &Value) -> Value {
    let delta = |path: &str| {
        current
            .pointer(path)
            .and_then(Value::as_f64)
            .zip(baseline.pointer(path).and_then(Value::as_f64))
            .filter(|(a, b)| a.is_finite() && b.is_finite() && *b >= 0.0 && a >= b)
            .map(|(a, b)| a - b)
    };
    let output = delta("/tokens/output")
        .zip(delta("/tokens/reasoning"))
        .map(|(visible, reasoning)| visible + reasoning)
        .filter(|value| value.is_finite());
    json!({"origin":"opencode","usage":{
        "input_tokens":delta("/tokens/input"),"output_tokens":output,
        "cache_read_input_tokens":delta("/tokens/cache/read"),"cache_creation_input_tokens":delta("/tokens/cache/write"),
        "output_tokens_details":{"thinking_tokens":delta("/tokens/reasoning")}},"total_cost_usd":delta("/cost")})
}

/// Requires this exact admitted input and text; v2 otherwise reuses an existing
/// user ID without comparing a newly supplied payload. No uncertain send is replayed.
fn input_ack(result: &Value, session: &str, id: &str, text: &str) -> bool {
    let result = data(result);
    result["id"] == id
        && result["sessionID"] == session
        && result.pointer("/payload/text").and_then(Value::as_str) == Some(text)
}

/// Submits one stable input identity and verifies the durable inbox receipt.
/// The caller records the payload digest before the HTTP write; an error never resends it.
async fn prompt(client: &Client, session: &str, id: &str, text: &str) -> Result<()> {
    prompt_until(
        client,
        session,
        id,
        text,
        "steer",
        tokio::time::Instant::now() + Duration::from_secs(30),
    )
    .await
}

/// Admits one immutable input by the original deadline. Queue delivery never steers
/// an active native pass; exact id/session/text acknowledgement proves admission only.
async fn prompt_until(
    client: &Client,
    session: &str,
    id: &str,
    text: &str,
    delivery: &str,
    deadline: tokio::time::Instant,
) -> Result<()> {
    let result = client
        .call_until(
            Method::POST,
            &format!("/api/session/{session}/prompt"),
            Some(json!({"id":id,"text":text,"delivery":delivery})),
            deadline,
        )
        .await?;
    if !input_ack(&result, session, id, text) {
        return Err(invalid("OpenCode input receipt belongs to another request"));
    }
    Ok(())
}

/// Returns positive process-local idle proof only after native awaitIdle and an
/// empty inbox, both within one original deadline. Errors/gaps cannot certify idle.
/// This metadata request never starts inference or consumes a queued input.
async fn idle_now(client: &Client, session: &str, deadline: tokio::time::Instant) -> bool {
    if client
        .call_until(
            Method::POST,
            &format!("/api/experimental/session/{session}/wait"),
            None,
            deadline,
        )
        .await
        .is_err()
    {
        return false;
    }
    client
        .call_until(
            Method::GET,
            &format!("/api/session/{session}/inbox"),
            None,
            deadline,
        )
        .await
        .ok()
        .and_then(|reply| reply["data"].as_array().map(Vec::is_empty))
        .unwrap_or(false)
}

/// Only this session's positive execution/status event can mark a native pass busy.
/// Terminals and historical idle watermarks still require fresh native idle proof.
/// 2.3.0 emits no `session.status`; `session.execution.started` plus the
/// wait/inbox proof in [`idle_now`] carry busy and idle detection there.
fn busy_event(event: &Value, session: &str) -> bool {
    event.pointer("/data/sessionID").and_then(Value::as_str) == Some(session)
        && (event["type"] == "session.execution.started"
            || (event["type"] == "session.status"
                && matches!(
                    event.pointer("/data/status/type").and_then(Value::as_str),
                    Some("busy" | "retry")
                )))
}

/// Whether a created or resumed native session carries the admitted role,
/// model, grants and directory. Only the model's `id`/`providerID` are
/// compared: 2.3.0 adds `variant:"default"` to the API model, which the stored
/// session row does not carry, and Run admits no variants (effort refuses).
fn session_authority(session: &Value, model: &Value, permissions: &Value, workdir: &Path) -> bool {
    session["agent"] == opencode::AGENT
        && session["model"].get("id") == model.get("id")
        && session["model"].get("providerID") == model.get("providerID")
        && session["permissions"] == *permissions
        && session
            .pointer("/location/directory")
            .and_then(Value::as_str)
            == workdir.to_str()
}

/// Refuses any change to the native session's frozen id, role, model or grants.
fn verify_session(current: &Value, initial: &Value) -> Result<()> {
    if ["id", "agent", "model", "permissions"]
        .iter()
        .any(|field| current[*field] != initial[*field])
        || current.pointer("/location/directory") != initial.pointer("/location/directory")
    {
        return Err(invalid(
            "OpenCode session changed its frozen execution authority",
        ));
    }
    Ok(())
}

/// Allows the native default prefix shadowed by our initial wildcard denial,
/// then the exact frozen rules, followed only by further native denials.
/// v2.0.28 appends a browser denial after configured agent rules.
fn grant_echo(grants: &[Value], frozen: &[Value]) -> bool {
    !frozen.is_empty()
        && frozen[0] == json!({"action":"*","resource":"*","effect":"deny"})
        && grants
            .windows(frozen.len())
            .enumerate()
            .any(|(offset, rules)| {
                rules == frozen
                    && grants[offset + frozen.len()..]
                        .iter()
                        .all(|rule| rule["effect"] == "deny")
            })
}

/// Waits for the exact admitted model until the caller's absolute startup deadline.
/// OpenCode may initially return its embedded catalog while refreshing metadata.
/// `model` is the validated providerID/id pair; an expired deadline refuses.
/// Only metadata GETs repeat: no session, prompt, inference or fallback is admitted.
/// Malformed catalogs and disabled offerings fail immediately; absence may settle.
async fn verify_model(
    client: &Client,
    model: &Value,
    deadline: tokio::time::Instant,
) -> Result<()> {
    tokio::time::timeout_at(deadline, async {
        loop {
            let models = client
                .call_until(Method::GET, "/api/model", None, deadline)
                .await?;
            let rows = data(&models)
                .as_array()
                .ok_or_else(|| invalid("OpenCode model inventory is missing"))?;
            if let Some(offering) = rows
                .iter()
                .find(|row| row["id"] == model["id"] && row["providerID"] == model["providerID"])
            {
                return if offering["enabled"] == true {
                    Ok(())
                } else {
                    Err(invalid("OpenCode model is disabled"))
                };
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .map_err(|_| invalid("OpenCode did not offer the admitted model before the startup deadline"))?
}

/// Confirms the loaded managed role and exact configured MCP set before any model input.
///
/// Native connected status follows initialization and tools/list; Code Mode is disabled
/// in the sealed settings. Pending startup may settle under a finite control allowance.
async fn verify_tools(
    client: &Client,
    plan: &ProviderLaunchPlan,
    directory: &Path,
    permissions: &Value,
) -> Result<()> {
    let query = format!(
        "?directory={}",
        opencode::query_value(&directory.to_string_lossy())
    );
    let agents = client
        .call(Method::GET, &format!("/api/agent{query}"), None)
        .await?;
    let agent = data(&agents)
        .as_array()
        .and_then(|rows| rows.iter().find(|row| row["id"] == opencode::AGENT))
        .ok_or_else(|| invalid("OpenCode managed agent is unavailable"))?;
    let grants = agent["permissions"]
        .as_array()
        .ok_or_else(|| invalid("OpenCode managed grants are missing"))?;
    if agent["system"] != plan.profile.body
        || agent["mode"] != "primary"
        || !grant_echo(
            grants,
            permissions
                .as_array()
                .ok_or_else(|| invalid("OpenCode frozen grants are missing"))?,
        )
    {
        return Err(invalid(
            "OpenCode loaded role differs from frozen authority",
        ));
    }
    if !plan.role.skills.is_empty() {
        let inventory = client
            .call(Method::GET, &format!("/api/skill{query}"), None)
            .await?;
        let skills = data(&inventory)
            .as_array()
            .ok_or_else(|| invalid("OpenCode skill inventory is missing"))?;
        for selected in &plan.role.skills {
            let expected = plan
                .launch
                .environment
                .get("OPENCODE_CONFIG")
                .and_then(|path| Path::new(path).parent())
                .ok_or_else(|| invalid("OpenCode managed skill root is missing"))?
                .join("skills")
                .join(&selected.id)
                .join("SKILL.md")
                .canonicalize()?;
            let matched: Vec<_> = skills
                .iter()
                .filter(|skill| skill["id"] == selected.id)
                .collect();
            if matched.len() != 1
                || matched[0]["path"]
                    .as_str()
                    .and_then(|path| Path::new(path).canonicalize().ok())
                    .as_ref()
                    != Some(&expected)
            {
                return Err(invalid(
                    "OpenCode selected skill differs from its frozen asset",
                ));
            }
        }
    }
    let mut expected: BTreeSet<String> = plan
        .role
        .mcp
        .iter()
        .map(|server| server.id.clone())
        .collect();
    if plan.role.worker_mcp {
        expected.insert(agent_run_domain::worker::SERVER_NAME.into());
    }
    tokio::time::timeout(Duration::from_secs(30), async {
        loop {
            let inventory = client
                .call_for(
                    Method::GET,
                    &format!("/api/mcp{query}"),
                    None,
                    Duration::from_secs(2),
                )
                .await?;
            let servers = data(&inventory)
                .as_array()
                .ok_or_else(|| invalid("OpenCode MCP inventory is missing"))?;
            let names: BTreeSet<String> = servers
                .iter()
                .map(|server| {
                    server["name"]
                        .as_str()
                        .map(str::to_owned)
                        .ok_or_else(|| invalid("OpenCode MCP name is malformed"))
                })
                .collect::<Result<_>>()?;
            if names != expected || names.len() != servers.len() {
                return Err(invalid("OpenCode MCP set differs from admitted role"));
            }
            if servers.iter().all(|server| {
                server.pointer("/status/status").and_then(Value::as_str) == Some("connected")
            }) {
                return Ok(());
            }
            if servers.iter().any(|server| {
                server.pointer("/status/status").and_then(Value::as_str) != Some("pending")
                    && server.pointer("/status/status").and_then(Value::as_str) != Some("connected")
            }) {
                return Err(invalid("OpenCode selected MCP did not connect"));
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .map_err(|_| invalid("OpenCode MCP startup did not settle"))?
}

/// Executes the owned native runner and records incomplete tool coverage on error.
/// The original error is preserved even when the diagnostic writer is unavailable;
/// the shared supervisor remains responsible for cleanup and terminal evidence.
/// The logical record retains exact admission authority; native record task text may
/// carry the supervisor's explicit continuation boundary and is never used as that authority.
pub async fn run(
    process: &mut Process,
    store: &mut Persist,
    record: &Record,
    logical_record: &Record,
    plan: &ProviderLaunchPlan,
    home: &Path,
) -> Result<EngineResult> {
    if record.id != logical_record.id
        || record.sequence != logical_record.sequence
        || record.identity != logical_record.identity
    {
        return Err(invalid(
            "OpenCode logical/native execution association changed",
        ));
    }
    let result = run_native(process, store, record, logical_record, plan, home).await;
    if result.is_err() {
        let _ = process
            .observing(store.event(
                &record.id,
                "native_tool_coverage_gap_v1",
                &json!({"protocol":"opencode","reason":"observation_incomplete"}),
            ))
            .await;
    }
    result
}

/// Runs one isolated OpenCode server using the same provider supervisor and finish semantics.
/// The exact model must settle within the startup allowance before any session or inference.
/// Standard serve has no durable event retention: native messages are authoritative recovery.
/// No elapsed job/idle deadline or synthetic model wake is introduced.
async fn run_native(
    process: &mut Process,
    store: &mut Persist,
    record: &Record,
    logical_record: &Record,
    plan: &ProviderLaunchPlan,
    _home: &Path,
) -> Result<EngineResult> {
    let ready = tokio::time::timeout(Duration::from_secs(30), process.next())
        .await
        .map_err(|_| invalid("OpenCode readiness timed out"))?;
    let Event::Json(ready) = ready else {
        return Err(invalid("OpenCode exited before readiness"));
    };
    let password = plan
        .launch
        .environment
        .get("OPENCODE_PASSWORD")
        .cloned()
        .ok_or_else(|| invalid("OpenCode server capability is missing"))?;
    let client = Client::new(
        ready["url"]
            .as_str()
            .ok_or_else(|| invalid("OpenCode readiness is missing"))?,
        password,
    )?;
    let info = process
        .observing(client.call(Method::GET, "/api/info", None))
        .await?;
    let version = server_version(&info, process.owner.pid)?.to_owned();
    let model = opencode::model_ref(&plan.native_model)?;
    process
        .observing(verify_model(
            &client,
            &model,
            tokio::time::Instant::now() + Duration::from_secs(30),
        ))
        .await?;
    if record.request.effort.is_some() {
        return Err(invalid("OpenCode effort variants are not yet qualified"));
    }
    let permissions = opencode::permissions(&plan.profile, _home, &record.request.workdir)?;
    process
        .observing(verify_tools(
            &client,
            plan,
            &record.request.workdir,
            &permissions,
        ))
        .await?;
    let mut stream = process.observing(client.subscribe()).await?;
    let created = if let Some(session) = record.resume_of_runtime_session_id.as_deref() {
        process
            .observing(client.call(Method::GET, &format!("/api/session/{session}"), None))
            .await?
    } else {
        process.observing(client.call(Method::POST,"/api/session",Some(json!({"agent":opencode::AGENT,"model":model,"permissions":permissions,"title":"Agent Run worker","location":{"directory":record.request.workdir}})))).await?
    };
    let initial = data(&created).clone();
    let session = identifier(&initial["id"])?.to_owned();
    if !session_authority(&initial, &model, &permissions, &record.request.workdir) {
        return Err(invalid("OpenCode session grants/model did not verify"));
    }
    if record.resume_of_runtime_session_id.is_some() {
        let parent = record
            .parent_agent_id
            .as_ref()
            .ok_or_else(|| invalid("OpenCode continuation has no parent proof"))?;
        let state: Value = serde_json::from_str(&store.latest_attempt_state(parent)?)?;
        let seal: crate::continuity::HistorySeal =
            serde_json::from_value(state["native_history"]["seal"].clone())
                .map_err(|_| invalid("OpenCode parent history proof is missing"))?;
        crate::continuity::verify_opencode_started(
            &seal,
            &_home.join("opencode-data"),
            &session,
            &version,
        )?;
    }
    process
        .observing(store.runtime_session(&record.id, &session))
        .await?;
    let baseline_rows = process.observing(messages(&client, &session)).await?;
    let mut baseline: BTreeSet<String> = baseline_rows
        .iter()
        .map(|row| identifier(&row["id"]).map(str::to_owned))
        .collect::<Result<_>>()?;
    let input_id = format!(
        "msg_{}",
        &crate::fs::sha256(record.id.as_str().as_bytes())[..32]
    );
    // The supervisor has already journaled the original task; skip only that native echo.
    baseline.insert(input_id.clone());
    process
        .observing(store.event(
            &record.id,
            "opencode_input_v1",
            &json!({"id":input_id,"sha256":crate::fs::sha256(record.request.task.as_bytes())}),
        ))
        .await?;
    process.observing(store.flush()).await?;
    process
        .observing(prompt(&client, &session, &input_id, &record.request.task))
        .await?;
    if record.resume_of_runtime_session_id.is_some() {
        process
            .observing(store.event_durable(
                &record.id,
                "native_resume_accepted",
                &json!({"session":session,"input_id":input_id}),
            ))
            .await?;
    }
    process
        .observing(store.worker_turn(&record.id, false, crate::domain::now()))
        .await?;
    process
        .observing(store.event(
            &record.id,
            "native_tool_observer_v1",
            &json!({"protocol":"opencode","version":1}),
        ))
        .await?;
    let mut native_owner = process.native_owner.take();
    let mut native_calls = agent_run_adapters::native_owner::Calls::default();
    let mut seen = BTreeMap::new();
    let mut idle = false;
    let mut last_idle = initial
        .pointer("/time/idle")
        .filter(|value| !value.is_null())
        .cloned();
    let mut closing = None;
    let mut tick = tokio::time::interval(Duration::from_millis(200));
    let mut snapshot_at = tokio::time::Instant::now() - Duration::from_secs(1);
    let mut hint = true;
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        if let Some(result) =
            crate::worker_finish::result(process, store, record, Some(session.clone()), None)?
        {
            closing.get_or_insert_with(tokio::time::Instant::now);
            if idle
                || closing
                    .is_some_and(|at: tokio::time::Instant| at.elapsed() >= Duration::from_secs(2))
            {
                if !idle {
                    process
                        .observing(store.event(
                            &record.id,
                            "native_tool_coverage_gap_v1",
                            &json!({"protocol":"opencode","reason":"closing_before_idle"}),
                        ))
                        .await?;
                }
                return Ok(result);
            }
        }
        tokio::select! {
            call = agent_run_adapters::native_owner::Owner::receive(&mut native_owner) => {
                use agent_run_adapters::native_owner::Reply;
                let call_id = call.request.call_id.clone();
                if let Err(reply) = native_calls.reserve(&call.request) { call.answer(reply); continue; }
                let refusal = crate::native_owner::refusal(process, store, logical_record, &call, Some(&session), idle, closing.is_some()).unwrap_or(Some("unavailable"));
                if let Some(reason) = refusal {
                    let reply = Reply::unsent(&call_id, reason);
                    native_calls.finish(reply.clone()); call.answer(reply); continue;
                }
                let current = process.observing(client.call_until(Method::GET, &format!("/api/session/{session}"), None, call.deadline)).await?;
                verify_session(data(&current), &initial)?;
                let proof_deadline = call.deadline.min(tokio::time::Instant::now() + Duration::from_millis(50));
                if !process.observing(async { Ok::<bool, crate::Error>(idle_now(&client, &session, proof_deadline).await) }).await? {
                    idle = false;
                    let reply = Reply::unsent(&call_id, "unavailable");
                    native_calls.finish(reply.clone()); call.answer(reply); continue;
                }
                let input_id = format!("msg_{}", &crate::fs::sha256(format!("{}:bridge:{call_id}",record.id).as_bytes())[..32]);
                process.observing(store.event(&record.id,"opencode_input_v1",&json!({"id":input_id,"sha256":crate::fs::sha256(call.request.text.as_bytes())}))).await?;
                process.observing(store.flush()).await?;
                if tokio::time::Instant::now() >= call.deadline {
                    let reply = Reply::unsent(&call_id, "unavailable");
                    native_calls.finish(reply.clone()); call.answer(reply); continue;
                }
                let result = process.observing(prompt_until(&client,&session,&input_id,&call.request.text,"queue",call.deadline)).await;
                let reply = if result.is_ok() { Reply::accepted(&call_id,"opencode_input",&input_id) } else { Reply::unknown(&call_id) };
                let unknown = reply.state == "unknown";
                native_calls.finish(reply.clone()); call.answer(reply);
                if unknown { native_owner.take(); return Err(invalid("OpenCode owner input outcome is uncertain")); }
                idle = false;
                process.observing(store.worker_turn(&record.id,false,crate::domain::now())).await?;
                continue;
            }
            event=process.next()=> match event {Event::Eof=>return Err(invalid("OpenCode exited without finish")), Event::OwnershipFailure(error)=>return Err(error), Event::Failure(_)=>return Err(invalid("OpenCode process transport failed")), Event::Json(_)=>{}},
            event=stream.next()=> {
                match event {
                    Ok(Some(event)) => {
                        if event.pointer("/data/sessionID").and_then(Value::as_str)==Some(session.as_str()) { hint=true; }
                        if busy_event(&event,&session) {
                            idle=false;
                            process.observing(store.worker_turn(&record.id,false,crate::domain::now())).await?;
                        }
                    },
                    Ok(None)|Err(_) => { idle=false; stream=process.observing(client.subscribe()).await?; hint=true; }
                }
            },
            _=tick.tick()=>{}
        }
        let command_started = tokio::time::Instant::now();
        for _ in 0..commands::COMMAND_PAGE_LIMIT {
            if command_started.elapsed() >= Duration::from_secs_f64(commands::COMMAND_PAGE_SECONDS)
            {
                break;
            }
            let Some((id, kind, payload)) = process
                .observing(store.claim_command_for_turn(&record.id, idle))
                .await?
            else {
                break;
            };
            if crate::health::command(process, store, &record.id, (id, &kind, &payload), idle)
                .await?
            {
                continue;
            }
            if kind == "cancel" {
                process
                    .observing(store.event(
                        &record.id,
                        "native_tool_coverage_gap_v1",
                        &json!({"protocol":"opencode","reason":"cancelled_before_final_snapshot"}),
                    ))
                    .await?;

                let _ = process
                    .observing(client.call(
                        Method::POST,
                        &format!("/api/session/{session}/interrupt"),
                        Some(json!({})),
                    ))
                    .await;
                process
                    .observing(store.complete_command(&record.id, id, &json!({"accepted":true})))
                    .await?;
                let mut outcome = Outcome::failure("cancelled");
                outcome.status = Status::Cancelled;
                outcome.runtime_session_id = Some(session.clone());
                return Ok(EngineResult {
                    outcome,
                    answer: None,
                    usage: None,
                    native_failure: None,
                });
            }
            let text = if kind == "steer" {
                commands::steer_text(&payload).map(str::to_owned)
            } else if kind == "pool" {
                commands::pool_push_text(store, &record.id, &payload).ok()
            } else {
                None
            };
            let Some(text) = text else {
                process
                    .observing(store.complete_command(
                        &record.id,
                        id,
                        &json!({"accepted":false,"reason":"unsupported_command"}),
                    ))
                    .await?;
                continue;
            };
            if payload.get("health_ping").is_some() {
                let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
                let ready = process
                    .observing(async {
                        Ok::<bool, crate::Error>(idle_now(&client, &session, deadline).await)
                    })
                    .await?;
                if !ready
                    || !store.health_command_current(
                        &record.id,
                        &payload,
                        true,
                        crate::domain::now(),
                    )?
                {
                    process
                        .observing(store.complete_command(
                            &record.id,
                            id,
                            &json!({"accepted":false,"reason":"native_not_idle"}),
                        ))
                        .await?;
                    idle = false;
                    continue;
                }
            }
            let message_id = format!(
                "msg_{}",
                &crate::fs::sha256(format!("{}:{id}", record.id).as_bytes())[..32]
            );
            process.observing(store.event(&record.id,"opencode_input_v1",&json!({"id":message_id,"sha256":crate::fs::sha256(text.as_bytes()),"command":id}))).await?;
            process.observing(store.flush()).await?;
            let sent = process
                .observing(prompt(&client, &session, &message_id, &text))
                .await;
            let result = if sent.is_ok() {
                json!({"accepted":true,"observation":"native_accepted"})
            } else {
                json!({"accepted":null,"reason":"native_delivery_uncertain"})
            };
            if sent.is_err() {
                process
                    .observing(store.complete_command(&record.id, id, &result))
                    .await?;
                return Err(invalid("OpenCode command admission is uncertain"));
            }
            process
                .observing(store.observed_command_result(
                    &record.id,
                    id,
                    &result,
                    "native_accepted",
                    Some(false),
                    Vec::new(),
                ))
                .await?;
            idle = false;
            hint = true;
        }
        // ponytail: bounded native snapshot scan; use incremental projected-message cursors if long-session cost becomes material.
        if (hint && snapshot_at.elapsed() >= Duration::from_millis(200))
            || snapshot_at.elapsed() >= Duration::from_secs(1)
        {
            snapshot_at = tokio::time::Instant::now();
            hint = false;
            process.observing(reject_asks(&client, &session)).await?;
            let rows = process.observing(messages(&client, &session)).await?;
            let current = process
                .observing(client.call_for(
                    Method::GET,
                    &format!("/api/session/{session}"),
                    None,
                    Duration::from_secs(2),
                ))
                .await?;
            let current = data(&current);
            verify_session(current, &initial)?;
            let failure = reconcile(process, store, record, &rows, &baseline, &mut seen).await?;

            let observed_idle = current
                .pointer("/time/idle")
                .filter(|value| !value.is_null())
                .cloned();
            let fresh_idle = process
                .observing(async {
                    Ok::<bool, crate::Error>(
                        idle_now(
                            &client,
                            &session,
                            tokio::time::Instant::now() + Duration::from_millis(50),
                        )
                        .await,
                    )
                })
                .await?;
            let was_idle = idle;
            idle = fresh_idle;
            if idle && (!was_idle || (observed_idle.is_some() && observed_idle != last_idle)) {
                let new_terminal = observed_idle.is_some() && observed_idle != last_idle;
                last_idle = observed_idle;
                process
                    .observing(store.worker_turn(&record.id, true, crate::domain::now()))
                    .await?;
                if let Some(result) = crate::worker_finish::result(
                    process,
                    store,
                    record,
                    Some(session.clone()),
                    Some(usage(current, &initial)),
                )? {
                    return Ok(result);
                }
                if new_terminal && current["outcome"] == "failed" {
                    let mut outcome = Outcome::failure(
                        failure.unwrap_or_else(|| "opencode_native_failed".into()),
                    );
                    outcome.runtime_session_id = Some(session.clone());
                    return Ok(EngineResult {
                        outcome,
                        answer: None,
                        usage: Some(usage(current, &initial)),
                        native_failure: None,
                    });
                }
            }
        }
        process.observing(store.flush_due()).await?;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    /// Serves a finite metadata script; captures actual request lines/payloads, no native engine or model.
    /// Each tuple is an exact path, HTTP status and JSON body; 204 emits no body.
    async fn scripted_client(
        script: Vec<(&'static str, u16, Value)>,
    ) -> (Client, tokio::task::JoinHandle<Vec<(String, Value)>>) {
        use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let client = Client::new(
            &format!("http://{}", listener.local_addr().unwrap()),
            "probe".into(),
        )
        .unwrap();
        let server = tokio::spawn(async move {
            let mut received = Vec::new();
            for (path, status, body) in script {
                let (stream, _) = tokio::time::timeout(Duration::from_secs(2), listener.accept())
                    .await
                    .unwrap()
                    .unwrap();
                let mut stream = BufReader::new(stream);
                let mut first = String::new();
                stream.read_line(&mut first).await.unwrap();
                assert_eq!(first.split_whitespace().nth(1), Some(path));
                let mut length = 0;
                loop {
                    let mut line = String::new();
                    assert!(stream.read_line(&mut line).await.unwrap() > 0);
                    if line == "\r\n" {
                        break;
                    }
                    if let Some(value) = line.to_ascii_lowercase().strip_prefix("content-length:") {
                        length = value.trim().parse().unwrap();
                    }
                }
                assert!(length < 4096);
                let mut payload = vec![0; length];
                stream.read_exact(&mut payload).await.unwrap();
                received.push((
                    first.trim().to_owned(),
                    if payload.is_empty() {
                        Value::Null
                    } else {
                        serde_json::from_slice(&payload).unwrap()
                    },
                ));
                let payload = if status == 204 {
                    Vec::new()
                } else {
                    serde_json::to_vec(&body).unwrap()
                };
                stream.get_mut().write_all(format!("HTTP/1.1 {status} OK\r\nContent-Type: application/json\r\nConnection: close\r\nContent-Length: {}\r\n\r\n",payload.len()).as_bytes()).await.unwrap();
                stream.get_mut().write_all(&payload).await.unwrap();
            }
            received
        });
        (client, server)
    }

    /// Cold native catalogs may briefly omit a newly published model; no inference is sent while waiting.
    #[tokio::test]
    async fn cold_catalog_settles_before_model_admission() {
        let model = json!({"id":"minimax-m3.1-flash-free","providerID":"opencode"});
        let (client, server) = scripted_client(vec![
            ("/api/model", 200, json!({"data":[{"id":"retired-model","providerID":"opencode","enabled":true}]})),
            ("/api/model", 200, json!({"data":[{"id":"minimax-m3.1-flash-free","providerID":"opencode","enabled":true}]})),
        ]).await;
        verify_model(
            &client,
            &model,
            tokio::time::Instant::now() + Duration::from_secs(2),
        )
        .await
        .unwrap();
        assert_eq!(server.await.unwrap().len(), 2);
    }

    /// A same-named offering from another provider never authorizes the admitted model.
    /// Malformed or disabled metadata refuses immediately instead of waiting or falling back.
    #[tokio::test]
    async fn model_admission_remains_exact_and_fail_closed() {
        let model = json!({"id":"selected","providerID":"admitted"});
        for body in [
            json!({"data":null}),
            json!({"data":[{"id":"selected","providerID":"admitted","enabled":false}]}),
        ] {
            let (client, server) = scripted_client(vec![("/api/model", 200, body)]).await;
            assert!(
                verify_model(
                    &client,
                    &model,
                    tokio::time::Instant::now() + Duration::from_secs(1)
                )
                .await
                .is_err()
            );
            assert_eq!(server.await.unwrap().len(), 1);
        }
        let (client, server) = scripted_client(vec![(
            "/api/model",
            200,
            json!({"data":[{"id":"selected","providerID":"foreign","enabled":true}]}),
        )])
        .await;
        let error = verify_model(
            &client,
            &model,
            tokio::time::Instant::now() + Duration::from_millis(50),
        )
        .await
        .unwrap_err();
        assert!(
            matches!(error, crate::Error::Validation(ref message) if message == "OpenCode did not offer the admitted model before the startup deadline")
        );
        assert_eq!(server.await.unwrap().len(), 1);
    }

    /// A parked or malformed inbox cannot certify readiness, even after native awaitIdle succeeds.
    #[tokio::test]
    async fn idle_proof_requires_settlement_and_an_empty_inbox() {
        for (inbox, expected) in [
            (json!([]), true),
            (json!([{"id":"msg_parked"}]), false),
            (Value::Null, false),
        ] {
            let (client, server) = scripted_client(vec![
                ("/api/experimental/session/ses_one/wait", 204, Value::Null),
                ("/api/session/ses_one/inbox", 200, json!({"data":inbox})),
            ])
            .await;
            assert_eq!(
                idle_now(
                    &client,
                    "ses_one",
                    tokio::time::Instant::now() + Duration::from_secs(2)
                )
                .await,
                expected
            );
            assert_eq!(server.await.unwrap().len(), 2);
        }
    }

    /// 2.3.0 answers awaitIdle with 204 and no body: that is success. A failed
    /// wait never certifies idle, and the inbox is then not even consulted.
    #[tokio::test]
    async fn idle_wait_accepts_bodyless_204_and_refuses_failures() {
        let (client, server) = scripted_client(vec![
            ("/api/experimental/session/ses_one/wait", 204, Value::Null),
            ("/api/session/ses_one/inbox", 200, json!({"data":[]})),
        ])
        .await;
        let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
        assert!(idle_now(&client, "ses_one", deadline).await);
        let requests = server.await.unwrap();
        assert_eq!(
            requests[0].0,
            "POST /api/experimental/session/ses_one/wait HTTP/1.1"
        );
        let (client, server) = scripted_client(vec![(
            "/api/experimental/session/ses_one/wait",
            500,
            json!({"error":"busy"}),
        )])
        .await;
        assert!(!idle_now(&client, "ses_one", deadline).await);
        assert_eq!(server.await.unwrap().len(), 1);
    }

    /// 2.3.0 ends pagination with one extra empty page whose `next` is null;
    /// the timeline is complete in source order after exactly that request.
    #[tokio::test]
    async fn message_pagination_ends_on_one_empty_page() {
        let (client, server) = scripted_client(vec![
            (
                "/api/session/ses_one/message?limit=100&order=asc",
                200,
                json!({"data":[{"id":"msg_a","type":"user"}],"cursor":{"previous":null,"next":"c1"}}),
            ),
            (
                "/api/session/ses_one/message?limit=100&cursor=c1",
                200,
                json!({"data":[{"id":"msg_b","type":"idle"}],"cursor":{"previous":"c0","next":"c2"}}),
            ),
            (
                "/api/session/ses_one/message?limit=100&cursor=c2",
                200,
                json!({"data":[],"cursor":{"previous":"c1","next":null}}),
            ),
        ])
        .await;
        let rows = messages(&client, "ses_one").await.unwrap();
        assert_eq!(
            rows.iter().map(|row| row["id"].clone()).collect::<Vec<_>>(),
            [json!("msg_a"), json!("msg_b")]
        );
        assert_eq!(server.await.unwrap().len(), 3);
        // A repeated cursor is a protocol refusal, never an endless loop.
        let page = json!({"data":[],"cursor":{"next":"c1"}});
        let (client, server) = scripted_client(vec![
            (
                "/api/session/ses_one/message?limit=100&order=asc",
                200,
                page.clone(),
            ),
            (
                "/api/session/ses_one/message?limit=100&cursor=c1",
                200,
                page,
            ),
        ])
        .await;
        assert!(messages(&client, "ses_one").await.is_err());
        assert_eq!(server.await.unwrap().len(), 2);
    }

    /// Unknown message types (2.3.0 `idle`) and part types (`reasoning`) are
    /// skipped; settled text, tools and the worker finish receipt still project.
    #[test]
    fn projection_skips_unknown_message_and_part_types() {
        let finish = "agent_run_worker_finish";
        for unknown in [
            json!({"id":"msg_idle","type":"idle","time":{"created":1}}),
            json!({"id":"msg_new","type":"future-kind","content":"opaque"}),
        ] {
            assert_eq!(project(&unknown, finish).unwrap(), (Vec::new(), None));
        }
        let assistant = json!({"id":"msg_one","type":"assistant","time":{"completed":5},"content":[
            {"type":"reasoning","text":"private chain"},
            {"type":"text","text":"done"},
            {"type":"future-part","payload":{}},
            {"type":"tool","id":"call_1","name":finish,"state":{"status":"completed","input":{"result":"OK"},"content":[{"type":"text","text":"accepted"}]}}
        ]});
        let (steps, failure) = project(&assistant, finish).unwrap();
        assert!(failure.is_none());
        let roles: Vec<_> = steps
            .iter()
            .map(|step| match step {
                Projected::Item { role, .. } => *role,
                Projected::Finish(_) => "finish",
            })
            .collect();
        assert_eq!(roles, ["assistant", "tool_call", "tool_result", "finish"]);
        assert!(!format!("{steps:?}").contains("private chain"));
        assert_eq!(steps[3], Projected::Finish("accepted".into()));
        // An unfinished assistant message projects no partial text.
        let mut partial = assistant.clone();
        partial["time"]["completed"] = Value::Null;
        let (steps, _) = project(&partial, finish).unwrap();
        assert!(steps.iter().all(|step| !matches!(
            step,
            Projected::Item {
                role: "assistant",
                ..
            }
        )));
        // Known shapes stay strict: a malformed tool lifecycle still refuses.
        let mut malformed = assistant;
        malformed["content"][3]["state"]["status"] = json!("paused");
        assert!(project(&malformed, finish).is_err());
    }

    /// `/api/info` must name this exact child and one qualified release.
    #[test]
    fn server_version_requires_owned_pid_and_qualified_release() {
        for version in ["2.0.28", "2.3.0"] {
            assert_eq!(
                server_version(&json!({"version":version,"pid":42}), 42).unwrap(),
                version
            );
        }
        for info in [
            json!({"version":"2.3.0","pid":43}),
            json!({"version":"2.3.1","pid":42}),
            json!({"version":"2.2.0","pid":42}),
            json!({"pid":42}),
            json!({"version":230,"pid":42}),
        ] {
            assert!(server_version(&info, 42).is_err(), "{info}");
        }
    }

    /// 2.3.0's session model carries `variant:"default"`; only id/providerID
    /// bind the admitted model, while a different model or grant still refuses.
    #[test]
    fn session_authority_ignores_model_variant_only() {
        let model = json!({"id":"deepseek-v4.1-flash-free","providerID":"opencode"});
        let permissions = json!([{"action":"*","resource":"*","effect":"deny"}]);
        let workdir = Path::new("/work");
        let session = json!({"id":"ses_one","agent":opencode::AGENT,
            "model":{"id":"deepseek-v4.1-flash-free","providerID":"opencode","variant":"default"},
            "permissions":permissions,"location":{"directory":"/work"}});
        assert!(session_authority(&session, &model, &permissions, workdir));
        let mut other = session.clone();
        other["model"]["providerID"] = json!("foreign");
        assert!(!session_authority(&other, &model, &permissions, workdir));
        let mut other = session.clone();
        other["permissions"] = json!([]);
        assert!(!session_authority(&other, &model, &permissions, workdir));
        assert!(!session_authority(
            &session,
            &model,
            &permissions,
            Path::new("/elsewhere")
        ));
    }

    /// A Bridge wake stays queued with the exact immutable payload, never requesting active-pass steering.
    #[tokio::test]
    async fn owner_input_queues_and_requires_its_exact_native_receipt() {
        let (client,server)=scripted_client(vec![("/api/session/ses_one/prompt",200,json!({"data":{"id":"msg_one","sessionID":"ses_one","payload":{"text":"literal marker"}}}))]).await;
        prompt_until(
            &client,
            "ses_one",
            "msg_one",
            "literal marker",
            "queue",
            tokio::time::Instant::now() + Duration::from_secs(2),
        )
        .await
        .unwrap();
        let requests = server.await.unwrap();
        assert_eq!(requests[0].0, "POST /api/session/ses_one/prompt HTTP/1.1");
        assert_eq!(
            requests[0].1,
            json!({"id":"msg_one","text":"literal marker","delivery":"queue"})
        );
    }

    /// Foreign sessions and historical terminals never fabricate a busy observation for this session.
    #[test]
    fn busy_observation_is_owned_and_positive() {
        assert!(busy_event(
            &json!({"type":"session.execution.started","data":{"sessionID":"ses_one"}}),
            "ses_one"
        ));
        assert!(busy_event(
            &json!({"type":"session.status","data":{"sessionID":"ses_one","status":{"type":"retry"}}}),
            "ses_one"
        ));
        assert!(!busy_event(
            &json!({"type":"session.execution.started","data":{"sessionID":"ses_other"}}),
            "ses_one"
        ));
        assert!(!busy_event(
            &json!({"type":"session.execution.succeeded","data":{"sessionID":"ses_one"}}),
            "ses_one"
        ));
        assert!(!busy_event(
            &json!({"type":"session.status","data":{"sessionID":"ses_one","status":{"type":"idle"}}}),
            "ses_one"
        ));
    }

    /// Native additional denials narrow a role; any later grant must refuse.
    #[test]
    fn loaded_role_may_only_narrow_frozen_grants() {
        let frozen = vec![
            json!({"action":"*","resource":"*","effect":"deny"}),
            json!({"action":"read","resource":"*","effect":"allow"}),
        ];
        let mut echo = vec![json!({"action":"*","resource":"*","effect":"allow"})];
        echo.extend(frozen.clone());
        echo.push(json!({"action":"browser","resource":"*","effect":"deny"}));
        assert!(grant_echo(&echo, &frozen));
        echo.push(json!({"action":"shell","resource":"*","effect":"allow"}));
        assert!(!grant_echo(&echo, &frozen));
        assert!(!grant_echo(&echo, &[]));
    }
    /// v2's output is visible text; the common output metric includes reasoning.
    /// Cache/input/cost are independent cumulative deltas, never inferred zeros.
    #[test]
    fn usage_normalizes_native_reasoning_and_preserves_unknown_deltas() {
        let before = json!({"tokens":{"input":100,"output":30,"reasoning":20,"cache":{"read":50,"write":0}},"cost":0.01});
        let after = json!({"tokens":{"input":140,"output":40,"reasoning":45,"cache":{"read":60,"write":0}},"cost":0.02});
        let measured = usage(&after, &before);
        assert_eq!(measured["usage"]["input_tokens"], 40.0);
        assert_eq!(measured["usage"]["output_tokens"], 35.0);
        assert_eq!(
            measured["usage"]["output_tokens_details"]["thinking_tokens"],
            25.0
        );
        assert_eq!(measured["usage"]["cache_read_input_tokens"], 10.0);
        assert!(usage(&before, &after)["usage"]["input_tokens"].is_null());
        assert!(usage(&json!({}), &before)["usage"]["output_tokens"].is_null());
    }
    /// Reused IDs do not acknowledge a different native payload or foreign session.
    #[test]
    fn input_receipt_requires_owned_id_and_exact_payload() {
        let receipt =
            json!({"data":{"id":"msg_one","sessionID":"ses_one","payload":{"text":"first"}}});
        assert!(input_ack(&receipt, "ses_one", "msg_one", "first"));
        assert!(!input_ack(&receipt, "ses_one", "msg_one", "changed"));
        assert!(!input_ack(&receipt, "foreign", "msg_one", "first"));
        assert!(!input_ack(&receipt["data"], "ses_one", "msg_one", "first"));
    }
}
