use crate::{commands, journal};
use crate::{
    config::{Config, Runtime},
    domain::{AgentId, Outcome, StartRequest, Status},
    error::invalid,
    profiles::{self, Profile},
    state::{Record, Store},
    verify, Result,
};
use agent_run_adapters::{
    codex::session::{failure_kind as structured_failure_kind, Session},
    io::{Event, Process},
    EngineResult, LaunchPlan,
};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::{
    collections::{BTreeMap, BTreeSet},
    path::{Path, PathBuf},
    time::Duration,
};

/// Returns the per-run isolated home for a global or labelled Codex account.
///
/// A missing label deliberately keeps `configured_home` unchanged so it uses
/// the native global account. A label suffixes only the final component (for
/// example `codex@work`) before adding the run identifier, matching Python's
/// `account_runtime_home` layout without allowing path traversal in labels.
pub fn runtime_home(
    configured_home: &Path,
    account: Option<&str>,
    id: &AgentId,
) -> Result<PathBuf> {
    let base = match account {
        Some(label) => configured_home.with_file_name(format!(
            "{}@{label}",
            configured_home
                .file_name()
                .ok_or_else(|| invalid("Codex runtime home has no final path component"))?
                .to_string_lossy()
        )),
        None => configured_home.to_path_buf(),
    };
    Ok(base.join("runs").join(id.as_str()))
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Grant {
    pub model: String,
    pub cwd: String,
    pub roots: Vec<String>,
    pub writable_roots: Vec<String>,
    pub sandbox: String,
    pub approval_policy: String,
    pub reviewer: Option<String>,
    pub network_access: bool,
    pub permission_profile: Option<String>,
}
fn system_projects() -> Result<Option<toml::Table>> {
    let text = match std::fs::read_to_string("/etc/codex/requirements.toml") {
        Ok(s) => s,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(e) => return Err(e.into()),
    };
    let doc: toml::Value =
        toml::from_str(&text).map_err(|_| invalid("invalid system Codex permissions"))?;
    match doc.get("permissions").and_then(|v| v.get("Projects")) {
        None => Ok(None),
        Some(t) => t
            .as_table()
            .cloned()
            .map(Some)
            .ok_or_else(|| invalid("invalid system Projects profile")),
    }
}
fn caches(home: &Path) -> Vec<PathBuf> {
    [
        ".cache/uv",
        ".cargo/registry",
        ".npm",
        "Library/Caches/go-build",
        "Library/Caches/pip",
    ]
    .iter()
    .map(|p| home.join(p))
    .collect()
}
fn managed_roots(runtime: &Runtime, system: &toml::Table) -> Result<Vec<String>> {
    let root = runtime
        .workspace_root
        .as_ref()
        .ok_or_else(|| invalid("managed Projects requires workspace_root"))?;
    let table = system
        .get("workspace_roots")
        .and_then(toml::Value::as_table)
        .ok_or_else(|| invalid("invalid managed workspace roots"))?;
    if table
        .get(root.to_string_lossy().as_ref())
        .and_then(toml::Value::as_bool)
        != Some(true)
        || system.get("extends").and_then(toml::Value::as_str) != Some(":workspace")
    {
        return Err(invalid("workspace_root is not granted by managed Projects"));
    }
    let network = system
        .get("network")
        .and_then(|t| t.get("enabled"))
        .and_then(toml::Value::as_bool)
        .unwrap_or(false);
    if network != runtime.workspace_network {
        return Err(invalid("workspace_network differs from managed Projects"));
    }
    let mut roots = Vec::new();
    for (p, v) in table {
        if v.as_bool() == Some(true) {
            roots.push(
                crate::fs::expand(Path::new(p))?
                    .to_string_lossy()
                    .into_owned(),
            );
        }
    }
    if let Some(f) = system.get("filesystem").and_then(toml::Value::as_table) {
        for (p, v) in f {
            if !p.starts_with(':') && v.as_str() == Some("write") {
                roots.push(
                    crate::fs::expand(Path::new(p))?
                        .to_string_lossy()
                        .into_owned(),
                );
            }
        }
    }
    roots.sort();
    roots.dedup();
    Ok(roots)
}
impl Grant {
    pub fn new(
        runtime: &Runtime,
        request: &StartRequest,
        role: &Profile,
        home: &Path,
    ) -> Result<Self> {
        let root = if role.write {
            runtime
                .workspace_root
                .as_deref()
                .unwrap_or(&request.workdir)
        } else {
            &request.workdir
        };
        if role.write && !request.workdir.starts_with(root) {
            return Err(invalid("workdir is outside configured workspace_root"));
        }
        let mut seed = vec![root.to_path_buf()];
        seed.extend(role.read_roots.clone());
        let mut roots: Vec<String> = profiles::normalize_roots(&seed)
            .iter()
            .map(|p| p.to_string_lossy().into_owned())
            .collect();
        let mut writable = if role.write {
            vec![root.to_string_lossy().into_owned()]
        } else {
            vec![]
        };
        if role.write && roots != writable {
            return Err(invalid(
                "Codex workspace-write cannot grant external read roots",
            ));
        }
        let system = system_projects()?;
        let managed = system.is_some();
        let profile;
        if !role.write {
            profile = managed.then(|| ":read-only".into());
        } else if runtime.workspace_root.is_some() && (managed || !role.network) {
            if role.network && !runtime.workspace_network {
                return Err(invalid("network role requires workspace_network"));
            }
            writable = if let Some(system) = &system {
                managed_roots(runtime, system)?
            } else {
                let mut a = vec![root.to_string_lossy().into_owned()];
                a.extend(
                    caches(home)
                        .iter()
                        .map(|p| p.to_string_lossy().into_owned()),
                );
                a.sort();
                a
            };
            roots = vec![request.workdir.to_string_lossy().into_owned()];
            profile = Some("Projects".into());
        } else {
            if managed && role.network {
                return Err(invalid("network role requires managed workspace_root"));
            }
            profile = managed.then(|| ":workspace".into());
        }
        let network_access = if profile.as_deref() == Some("Projects") {
            runtime.workspace_network
        } else {
            role.network
        };
        Ok(Self {
            model: request.model.clone(),
            cwd: request.workdir.to_string_lossy().into_owned(),
            roots,
            writable_roots: writable,
            sandbox: if role.write {
                "workspace-write"
            } else {
                "read-only"
            }
            .into(),
            approval_policy: if role.write { "on-request" } else { "never" }.into(),
            reviewer: role.write.then(|| "auto_review".into()),
            network_access,
            permission_profile: profile,
        })
    }
    pub fn request(&self) -> Value {
        let mut v =
            json!({"cwd":self.cwd,"model":self.model,"approvalPolicy":self.approval_policy});
        if let Some(profile) = &self.permission_profile {
            v["permissions"] = json!(profile);
            if profile != "Projects" {
                v["runtimeWorkspaceRoots"] = json!(self.roots);
            }
        } else {
            v["sandbox"] = json!(self.sandbox);
            v["runtimeWorkspaceRoots"] = json!(self.roots);
            if self.network_access {
                v["config"] = json!({"sandbox_workspace_write":{"network_access":true}});
            }
        }
        if let Some(reviewer) = &self.reviewer {
            v["approvalsReviewer"] = json!(reviewer);
        }
        v
    }
    pub fn verify(&self, v: &Value) -> Result<()> {
        let sandbox = match v.get("sandbox") {
            Some(Value::String(s)) => s.as_str(),
            Some(t) => match t.get("type").and_then(Value::as_str) {
                Some("readOnly") => "read-only",
                Some("workspaceWrite") => "workspace-write",
                Some("dangerFullAccess") => "danger-full-access",
                _ => "unknown",
            },
            None => "missing",
        };
        if v.get("model").and_then(Value::as_str) != Some(self.model.as_str())
            || v.get("cwd").and_then(Value::as_str) != Some(self.cwd.as_str())
            || sandbox != self.sandbox
            || v.get("approvalPolicy").and_then(Value::as_str)
                != Some(self.approval_policy.as_str())
        {
            return Err(invalid(
                "Codex effective model/cwd/sandbox/approval differs from admitted grant",
            ));
        }
        fn list(v: Option<&Value>) -> Result<Vec<String>> {
            match v {
                None => Ok(vec![]),
                Some(Value::Array(a)) => a
                    .iter()
                    .map(|v| {
                        v.as_str()
                            .map(str::to_owned)
                            .ok_or_else(|| invalid("malformed Codex roots echo"))
                    })
                    .collect(),
                _ => Err(invalid("malformed Codex roots echo")),
            }
        }
        let roots = list(v.get("roots").or_else(|| v.get("runtimeWorkspaceRoots")))?;
        let writable = if v.get("writableRoots").is_some() {
            list(v.get("writableRoots"))?
        } else {
            let mut roots = list(v.get("sandbox").and_then(|s| s.get("writableRoots")))?;
            if roots.is_empty()
                && v.pointer("/sandbox/type").and_then(Value::as_str) == Some("workspaceWrite")
            {
                roots.push(self.cwd.clone());
            }
            roots
        };
        if roots != self.roots || writable != self.writable_roots {
            return Err(invalid(
                "Codex effective readable/writable roots differ from admitted grant",
            ));
        }
        if let Some(profile) = &self.permission_profile {
            if v.pointer("/activePermissionProfile/id")
                .and_then(Value::as_str)
                != Some(profile.as_str())
            {
                return Err(invalid("Codex permission profile mismatch"));
            }
        }
        if v.pointer("/sandbox/networkAccess")
            .and_then(Value::as_bool)
            .unwrap_or(false)
            != self.network_access
        {
            return Err(invalid(
                "Codex effective network access differs from the admitted grant",
            ));
        }
        if let Some(reviewer) = &self.reviewer {
            if v.get("approvalsReviewer").and_then(Value::as_str) != Some(reviewer.as_str()) {
                return Err(invalid("Codex reviewer routing differs"));
            }
        }
        Ok(())
    }
}
pub fn render_permissions(doc: &mut toml::Table, runtime: &Runtime, home: &Path) -> Result<()> {
    let Some(root) = &runtime.workspace_root else {
        return Ok(());
    };
    if let Some(system) = system_projects()? {
        managed_roots(runtime, &system)?;
        doc.insert(
            "default_permissions".into(),
            toml::Value::String("Projects".into()),
        );
        return Ok(());
    }
    let mut writes = serde_json::Map::new();
    for path in caches(home) {
        writes.insert(path.to_string_lossy().into_owned(), json!("write"));
    }
    writes.insert(
        home.join("auth.json").to_string_lossy().into_owned(),
        json!("deny"),
    );
    writes.insert(":workspace_roots".into(),json!({".":"write","**/.env":"deny","**/.env.*":"deny","**/*.pem":"deny","**/*.key":"deny"}));
    let projects = json!({"Projects":{"extends":":workspace","workspace_roots":{(root.to_string_lossy().to_string()):true},"filesystem":writes,"network":{"enabled":runtime.workspace_network}}});
    doc.insert(
        "default_permissions".into(),
        toml::Value::String("Projects".into()),
    );
    doc.insert(
        "permissions".into(),
        toml::Value::try_from(projects)
            .map_err(|_| invalid("cannot encode Projects permissions"))?,
    );
    Ok(())
}
pub fn plan(
    config: &Config,
    runtime: &Runtime,
    record: &Record,
    role: &Profile,
    home: &Path,
    app_home: &Path,
) -> Result<LaunchPlan> {
    let mut args = Vec::new();
    if record.request.fast {
        args.extend([
            "-c".into(),
            "service_tier=fast".into(),
            "-c".into(),
            "features.fast_mode=true".into(),
        ]);
    }
    args.push("app-server".into());
    Ok(LaunchPlan {
        binary: runtime.binary.clone(),
        args,
        cwd: record.request.workdir.clone(),
        environment: agent_run_adapters::environment(
            config,
            runtime,
            role,
            home,
            record.request.account.as_deref(),
            app_home,
        )?,
        initial_input: None,
    })
}
/// Initializes the experimental app-server protocol required for grant echoes.
async fn initialize(process: &mut Process) -> Result<()> {
    process
        .rpc(
            "initialize",
            json!({"clientInfo":{"name":"agent-run","version":"1"},"capabilities":{"experimentalApi":true}}),
            Duration::from_secs(30),
        )
        .await?;
    process.send(&json!({"method":"initialized"})).await
}
pub async fn models(process: &mut Process) -> Result<Vec<Value>> {
    let mut result = Vec::new();
    let mut cursor: Option<String> = None;
    let mut seen = BTreeSet::new();
    for _ in 0..32 {
        let mut p = json!({"includeHidden":true,"limit":1000});
        if let Some(cursor) = &cursor {
            p["cursor"] = json!(cursor);
        }
        let response = process
            .rpc("model/list", p, Duration::from_secs(20))
            .await?;
        let data = response
            .get("data")
            .and_then(Value::as_array)
            .ok_or_else(|| invalid("malformed model roster"))?;
        result.extend(data.clone());
        let next = response
            .get("nextCursor")
            .or_else(|| response.get("next_cursor"));
        if next.is_none() || next == Some(&Value::Null) {
            return Ok(result);
        }
        let next = next
            .and_then(Value::as_str)
            .ok_or_else(|| invalid("invalid roster cursor"))?
            .to_owned();
        if !seen.insert(next.clone()) {
            return Err(invalid("repeated roster cursor"));
        }
        cursor = Some(next);
    }
    Err(invalid("model roster exceeds page bound"))
}
pub async fn run(
    process: &mut Process,
    store: &mut Store,
    record: &Record,
    runtime: &Runtime,
    role: &Profile,
    home: &Path,
) -> Result<EngineResult> {
    initialize(process).await?;
    let mut session = Session::new(record.resume_of_runtime_session_id.is_some());
    session.initialized()?;
    agent_run_adapters::codex::models::validate_cached_selection(
        home,
        &record.request.model,
        record.request.effort.as_deref(),
    )?;
    let roster = models(process).await?;
    // Cache publication is advisory exactly as in Python: a successful live
    // roster remains sufficient when a later local cache write is unavailable.
    let _ = agent_run_adapters::codex::models::write_cache(home, &roster);
    let discovered = agent_run_adapters::codex::models::parse_roster(&json!({"models":roster}))?;
    let model = discovered
        .iter()
        .find(|model| model.id == record.request.model)
        .ok_or_else(|| invalid("selected account did not report the configured model"))?;
    if let Some(effort) = &record.request.effort {
        if !model.efforts.iter().any(|choice| choice == effort) {
            return Err(invalid("effort is not offered by selected account/model"));
        }
    }
    let grant = Grant::new(runtime, &record.request, role, home)?;
    let mut params = grant.request();
    let method = if let Some(s) = &record.resume_of_runtime_session_id {
        params["threadId"] = json!(s);
        "thread/resume"
    } else {
        "thread/start"
    };
    let thread = process.rpc(method, params, Duration::from_secs(30)).await?;
    grant.verify(&thread)?;
    if record.resume_of_runtime_session_id.is_some() {
        let status = thread
            .pointer("/thread/status/type")
            .or_else(|| thread.pointer("/thread/status"))
            .and_then(Value::as_str);
        if matches!(status, Some("active") | Some("running")) {
            return Err(invalid("native thread is already active"));
        }
    }
    let tid = thread
        .get("threadId")
        .or_else(|| thread.pointer("/thread/id"))
        .and_then(Value::as_str)
        .filter(|s| !s.is_empty())
        .ok_or_else(|| invalid("thread start did not return an id"))?
        .to_owned();
    session.thread_started(&tid)?;
    if record
        .resume_of_runtime_session_id
        .as_ref()
        .is_some_and(|expected| *expected != tid)
    {
        return Err(invalid("thread resume returned another thread"));
    }
    store.runtime_session(&record.id, &tid)?;
    let mut turn_params = json!({"threadId":tid,"input":[{"type":"text","text":format!("{}\n\n{}",role.body,record.request.task)}]});
    if let Some(effort) = &record.request.effort {
        turn_params["effort"] = json!(effort);
    }
    let response = process
        .rpc("turn/start", turn_params, Duration::from_secs(30))
        .await?;
    let turn_id = response
        .pointer("/turn/id")
        .and_then(Value::as_str)
        .filter(|s| !s.is_empty())
        .ok_or_else(|| invalid("turn start did not return an id"))?
        .to_owned();
    session.turn_started(&turn_id)?;
    let mut streamed: BTreeMap<String, String> = BTreeMap::new();
    let mut completed: BTreeMap<String, String> = BTreeMap::new();
    let mut final_answer: Option<String> = None;
    let mut usage = None;
    let mut tick = tokio::time::interval(Duration::from_millis(200));
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        let event = tokio::select! {event=process.next()=>Some(event),_=tick.tick()=>None};
        let Some(event) = event else {
            process.owner.refresh();
            while let Some((cid, kind, payload)) = store.claim_command(&record.id)? {
                if kind == "cancel" {
                    // App-server interruption is advisory.  Once the durable
                    // command is claimed, return to the supervisor so its
                    // verified process-group cleanup enforces cancellation
                    // and escalates only after the documented grace period.
                    let _ = process
                        .rpc(
                            "turn/interrupt",
                            json!({"threadId":tid,"turnId":turn_id}),
                            Duration::from_secs(1),
                        )
                        .await;
                    store.complete_command(&record.id, cid, &json!({"accepted":true}))?;
                    return Ok(EngineResult {
                        outcome: Outcome {
                            status: Status::Cancelled,
                            exit_code: None,
                            failure_kind: None,
                            failure_text: None,
                            runtime_session_id: Some(tid.clone()),
                        },
                        answer: None,
                        usage,
                    });
                }
                if kind == "steer" {
                    if let Some(text) = commands::steer_text(&payload) {
                        let result=process.rpc("turn/steer",json!({"threadId":tid,"expectedTurnId":turn_id,"input":[{"type":"text","text":text}]}),Duration::from_secs(30)).await;
                        store.complete_command(
                            &record.id,
                            cid,
                            &json!({"accepted":result.is_ok()}),
                        )?;
                        if result.is_ok() {
                            journal(store, &record.id, "user", text, None, None)?;
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
                return Ok(EngineResult {
                    outcome: Outcome::failure("engine_transport_eof"),
                    answer: None,
                    usage,
                })
            }
            Event::Failure(e) => {
                return Ok(EngineResult {
                    outcome: Outcome::failure(e),
                    answer: None,
                    usage,
                })
            }
        };
        if v.get("method").is_some() && v.get("id").is_some() {
            process.deny_request(&v).await?;
            continue;
        }
        if session.notification(&v)?.is_none() {
            continue;
        }
        let method = v.get("method").and_then(Value::as_str).unwrap_or("");
        let p = &v["params"];
        if p.get("threadId")
            .and_then(Value::as_str)
            .is_some_and(|id| id != tid)
        {
            continue;
        }
        let event_turn = p
            .get("turnId")
            .or_else(|| p.pointer("/turn/id"))
            .and_then(Value::as_str);
        if event_turn.is_some_and(|id| id != turn_id) {
            continue;
        }
        if record.resume_of_runtime_session_id.is_some()
            && method.starts_with("item/")
            && event_turn.is_none()
        {
            continue;
        }
        match method {
            "item/agentMessage/delta" => {
                let key = p
                    .get("itemId")
                    .and_then(Value::as_str)
                    .ok_or_else(|| invalid("assistant delta has no itemId"))?
                    .to_owned();
                let delta = p
                    .get("delta")
                    .and_then(Value::as_str)
                    .ok_or_else(|| invalid("malformed assistant delta"))?;
                let text = streamed.entry(key.clone()).or_default();
                if text.len() + delta.len() > verify::MAX_ANSWER {
                    return Err(invalid("assistant stream exceeds bound"));
                }
                text.push_str(delta);
                journal(store, &record.id, "assistant", delta, None, Some(&key))?;
            }
            "item/completed" => {
                let item = &p["item"];
                let key = item.get("id").and_then(Value::as_str).unwrap_or("");
                if item.get("type").and_then(Value::as_str) == Some("agentMessage") {
                    let text = item.get("text").and_then(Value::as_str).unwrap_or("");
                    if !completed.contains_key(key) {
                        let prefix = streamed.get(key).map(String::as_str).unwrap_or("");
                        if let Some(tail) = text.strip_prefix(prefix) {
                            journal(store, &record.id, "assistant", tail, None, Some(key))?;
                        } else if prefix.is_empty() {
                            journal(store, &record.id, "assistant", text, None, Some(key))?;
                        }
                        completed.insert(key.into(), text.into());
                    }
                    if !text.trim().is_empty() {
                        final_answer = Some(text.to_owned());
                    }
                } else if let Some(text) = item.get("aggregatedOutput").and_then(Value::as_str) {
                    journal(
                        store,
                        &record.id,
                        "tool_result",
                        text,
                        Some("command"),
                        Some(key),
                    )?;
                }
            }
            "thread/tokenUsage/updated" => {
                let u = p.pointer("/tokenUsage/total").unwrap_or(&Value::Null);
                usage = Some(token_usage_event(u));
            }
            "turn/completed" => {
                if event_turn != Some(turn_id.as_str()) {
                    continue;
                }
                let turn = &p["turn"];
                if let Some(items) = turn.get("items").and_then(Value::as_array) {
                    for item in items {
                        if item.get("type").and_then(Value::as_str) == Some("agentMessage") {
                            if let Some(text) = item
                                .get("text")
                                .and_then(Value::as_str)
                                .filter(|s| !s.trim().is_empty())
                            {
                                let key = item.get("id").and_then(Value::as_str).unwrap_or("");
                                if !completed.contains_key(key) {
                                    let prefix =
                                        streamed.get(key).map(String::as_str).unwrap_or("");
                                    if let Some(tail) = text.strip_prefix(prefix) {
                                        journal(
                                            store,
                                            &record.id,
                                            "assistant",
                                            tail,
                                            None,
                                            Some(key),
                                        )?;
                                    }
                                }
                                final_answer = Some(text.into());
                            }
                        }
                    }
                }
                let mut outcome = match turn.get("status").and_then(Value::as_str) {
                    Some("completed") => Outcome::success(Some(tid.clone())),
                    Some("interrupted") => {
                        let mut o = Outcome::failure("interrupted");
                        o.status = Status::Cancelled;
                        o
                    }
                    Some("failed") => Outcome::failure(
                        structured_failure_kind(&turn["error"])
                            .unwrap_or_else(|| "runtime_failed".into()),
                    ),
                    _ => return Err(invalid("nonterminal turn/completed status")),
                };
                outcome.runtime_session_id = Some(tid);
                outcome.failure_text = turn
                    .pointer("/error/message")
                    .and_then(Value::as_str)
                    .map(|s| s.chars().take(512).collect());
                if let Some(kind) = final_answer.as_deref().and_then(verify::error_only) {
                    outcome.status = Status::Failed;
                    outcome.failure_kind = Some(kind.into());
                    final_answer = None;
                }
                return Ok(EngineResult {
                    outcome,
                    answer: final_answer,
                    usage,
                });
            }
            _ => {}
        }
    }
}

/// Wraps Codex's cumulative total in the durable Python event payload shape.
///
/// The returned value intentionally preserves malformed or missing fields so
/// store-side normalization can represent them as unavailable rather than zero.
fn token_usage_event(total: &Value) -> Value {
    json!({"tokenUsage":{"total":total},"_source":"token_usage_updated"})
}

pub async fn query(process: &mut Process, method: &str) -> Result<Value> {
    initialize(process).await?;
    if method == "model/list" {
        return Ok(json!({"data":models(process).await?}));
    }
    process
        .rpc(method, json!({}), Duration::from_secs(20))
        .await
}

#[cfg(test)]
mod tests {
    use super::token_usage_event;
    use serde_json::json;

    /// Mirrors `test_codex_app_server.py::test_token_usage_updated_is_cumulative`.
    #[test]
    fn token_usage_event_preserves_cache_write_and_reasoning_counters() {
        let payload = token_usage_event(&json!({
            "inputTokens": 10,
            "cacheWriteInputTokens": 4,
            "reasoningOutputTokens": 2,
            "totalTokens": 12
        }));
        assert_eq!(payload["tokenUsage"]["total"]["cacheWriteInputTokens"], 4);
        assert_eq!(payload["tokenUsage"]["total"]["reasoningOutputTokens"], 2);
    }
}
