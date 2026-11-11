//! Strict managed-role checks for the explicitly enabled selector. Component
//! provenance, durable qualification and admission remain separate fences.
use agent_run_domain::{Result, error::invalid};
use serde_json::{Value, json};
use std::collections::{BTreeMap, BTreeSet};

/// Frozen, nonsecret output of the opt-in managed-role selector.
#[derive(Debug, Clone)]
pub struct Selection {
    pub host_id: String,
    pub role_digest: String,
    pub inventory: BTreeMap<String, BTreeSet<String>>,
}

fn trusted_binary(path: &std::path::Path, expected: &str) -> Result<()> {
    use std::os::unix::fs::MetadataExt;
    let fail = || invalid("managed component executable did not verify");
    if !agent_run_config::provider_config::loader_preload_safe()
        || !path.is_absolute()
        || path.canonicalize().map_err(|_| fail())? != path
    {
        return Err(fail());
    }
    let metadata = std::fs::metadata(path).map_err(|_| fail())?;
    // SAFETY: geteuid has no arguments or side effects.
    let uid = unsafe { libc::geteuid() };
    if !metadata.is_file()
        || !(metadata.uid() == uid || metadata.uid() == 0)
        || metadata.mode() & 0o022 != 0
        || metadata.len() > 64 * 1024 * 1024
    {
        return Err(fail());
    }
    let bytes = std::fs::read(path).map_err(|_| fail())?;
    if agent_run_config::provider_config::forbidden_interpreter_build(
        path,
        expected,
        metadata.len(),
    ) || bytes.starts_with(b"#!")
    {
        return Err(fail());
    }
    let after = std::fs::metadata(path).map_err(|_| fail())?;
    if metadata.dev() != after.dev()
        || metadata.ino() != after.ino()
        || metadata.len() != after.len()
        || metadata.mtime() != after.mtime()
        || metadata.mtime_nsec() != after.mtime_nsec()
        || metadata.ctime() != after.ctime()
        || metadata.ctime_nsec() != after.ctime_nsec()
        || agent_run_platform::fs::sha256(&bytes) != expected
    {
        return Err(fail());
    }
    Ok(())
}

/// Whether the frozen configuration selects `role` as the managed role.
pub fn selects(
    config: &agent_run_config::provider_config::ProviderConfig,
    role: &agent_run_config::role_plan::ResolvedRolePlan,
) -> bool {
    config
        .managed_attempt_binding
        .as_ref()
        .is_some_and(|settings| settings.enabled && settings.role_name == role.role_name)
}

/// Component builds are delivery configuration, not role identity. For one new
/// launch of a frozen managed role, keep every semantic fact of the frozen
/// snapshot (selector, Pool server declaration and tool cap, Bridge hook
/// declaration, host) and take only the component build pins (Pool frontend
/// and resident, Bridge hook, Run transport) from the current validated
/// configuration. A semantic difference is refused, never adopted. The role
/// digest is the frozen role payload's revision and is unaffected.
pub fn launch_build_pins(
    frozen: &agent_run_config::provider_config::ProviderConfig,
    current: &agent_run_config::provider_config::ProviderConfig,
    harness: agent_run_domain::catalog::HarnessId,
) -> Result<agent_run_config::provider_config::ProviderConfig> {
    let changed = || invalid("managed role changed since admission");
    let (Some(original), Some(live)) = (
        frozen.managed_attempt_binding.as_ref(),
        current.managed_attempt_binding.as_ref(),
    ) else {
        return Err(changed());
    };
    let value = |value: Option<Value>| value.unwrap_or(Value::Null);
    let hooks = |config: &agent_run_config::provider_config::ProviderConfig| {
        value(
            config
                .harnesses
                .get(&harness)
                .and_then(|runtime| serde_json::to_value(&runtime.hooks).ok()),
        )
    };
    let server = |config: &agent_run_config::provider_config::ProviderConfig| {
        value(
            config
                .mcp
                .get(&original.pool_server)
                .and_then(|server| serde_json::to_value(server).ok()),
        )
    };
    if !original.enabled
        || !live.enabled
        || live.role_name != original.role_name
        || live.pool_server != original.pool_server
        || live.host_id != original.host_id
        || hooks(current) != hooks(frozen)
        || server(current) != server(frozen)
    {
        return Err(changed());
    }
    let mut launch = frozen.clone();
    let settings = launch
        .managed_attempt_binding
        .as_mut()
        .expect("checked above");
    settings.pool_executable_sha256 = live.pool_executable_sha256.clone();
    settings.bridge_hook_sha256 = live.bridge_hook_sha256.clone();
    settings.pool_resident_executable = live.pool_resident_executable.clone();
    settings.pool_resident_sha256 = live.pool_resident_sha256.clone();
    settings.transport = live.transport.clone();
    Ok(launch)
}

/// Select only the explicitly named restricted role. Every enabled selection
/// rejects plugins, skills, broad tools, extra MCPs and unpinned executables.
/// An absent/disabled switch or a different role always stays ordinary.
pub fn select(
    config: &agent_run_config::provider_config::ProviderConfig,
    harness: agent_run_domain::catalog::HarnessId,
    role: &agent_run_config::role_plan::ResolvedRolePlan,
    explicit_finish: bool,
) -> Result<Option<Selection>> {
    let Some(settings) = config
        .managed_attempt_binding
        .as_ref()
        .filter(|settings| settings.enabled && settings.role_name == role.role_name)
    else {
        return Ok(None);
    };
    if !matches!(
        harness,
        agent_run_domain::catalog::HarnessId::Codex
            | agent_run_domain::catalog::HarnessId::ClaudeCode
    ) || !explicit_finish
        || role.write
        || role.network
        || role.research_tools_only()
        || !role.skills.is_empty()
        || !role.worker_mcp
        || role.mcp.len() != 1
    {
        return Err(invalid("managed role is unqualified"));
    }
    let runtime = config
        .harnesses
        .get(&harness)
        .ok_or_else(|| invalid("managed harness unavailable"))?;
    if !runtime.plugins.is_empty()
        || !runtime.plugin_snapshot_assets.is_empty()
        || runtime.hooks.len() != 1
    {
        return Err(invalid("managed role assets are unqualified"));
    }
    let hook = &runtime.hooks[0];
    if hook.event != "PreToolUse"
        || hook.command.is_empty()
        || hook.matcher.as_deref() != Some(format!("mcp__{}__.*", settings.pool_server).as_str())
    {
        return Err(invalid("managed Bridge hook is unqualified"));
    }
    trusted_binary(
        std::path::Path::new(&hook.command[0]),
        &settings.bridge_hook_sha256,
    )?;
    let server = &role.mcp[0];
    let configured = config
        .mcp
        .get(&settings.pool_server)
        .ok_or_else(|| invalid("managed Pool server unavailable"))?;
    if server.id != settings.pool_server
        || server.transport != "stdio"
        || configured.transport != "stdio"
        || server.command != configured.command.to_string_lossy()
        || server.args != configured.args
        || server.args.len() != 3
        || server.args[0] != "mcp"
        || server.args[1] != "--config"
        || !std::path::Path::new(&server.args[2]).is_absolute()
        || std::path::Path::new(&server.args[2])
            .components()
            .any(|part| {
                matches!(
                    part,
                    std::path::Component::ParentDir | std::path::Component::CurDir
                )
            })
        || server.approval_mode != "approve"
        || !server.env_from.is_empty()
        || server.allowed_tools.as_ref().is_none_or(Vec::is_empty)
    {
        return Err(invalid("managed Pool server is unqualified"));
    }
    const SAFE_POOL_TOOLS: &[&str] = &[
        "pool_create",
        "pool_list",
        "pool_get",
        "pool_update",
        "pool_close",
        "pool_add",
        "pool_cancel_replacement",
        "pool_member_update",
        "pool_remove",
        "pool_release_history",
        "pool_join",
        "pool_post",
        "pool_send_member",
        "pool_notify_owner",
        "pool_propose",
        "pool_vote",
    ];
    if server.allowed_tools.as_ref().is_none_or(|tools| {
        tools
            .iter()
            .any(|tool| !SAFE_POOL_TOOLS.contains(&tool.as_str()))
    }) {
        return Err(invalid("managed Pool tool is unqualified"));
    }
    if hook.command[0] == server.command
        || settings.bridge_hook_sha256 == settings.pool_executable_sha256
        || settings
            .pool_resident_executable
            .as_ref()
            .is_some_and(|path| path == std::path::Path::new(&hook.command[0]))
        || settings.transport.as_ref().is_some_and(|transport| {
            transport.bridge_executable == std::path::Path::new(&hook.command[0])
        })
    {
        return Err(invalid("managed component identity is ambiguous"));
    }
    trusted_binary(
        std::path::Path::new(&server.command),
        &settings.pool_executable_sha256,
    )?;
    let pool = server
        .allowed_tools
        .as_ref()
        .unwrap()
        .iter()
        .cloned()
        .collect();
    let names = BTreeSet::from(["finish".to_owned(), "health_reply".to_owned()]);
    Ok(Some(Selection {
        host_id: settings.host_id.clone(),
        role_digest: role.config_revision.clone(),
        inventory: BTreeMap::from([
            (server.id.clone(), pool),
            (agent_run_domain::worker::SERVER_NAME.into(), names),
        ]),
    }))
}

/// Features which must all be explicitly disabled for a managed Codex role.
pub const CODEX_DISABLED_FEATURES: &[&str] = &[
    "shell_tool",
    "unified_exec",
    "multi_agent",
    "multi_agent_v2",
    "js_repl",
    "code_mode_host",
    "plugins",
    "remote_plugin",
    "apps",
    "browser_use",
    "computer_use",
    "request_permissions_tool",
    "skill_search",
    "skill_mcp_dependency_install",
    "standalone_web_search",
    "tool_suggest",
];

/// Default-on native features that codex-cli 0.167.0 reports in `config/read`
/// without granting a model tool. Only managed launches write them disabled,
/// so the generic rule below (an unrecognized feature must be disabled) holds;
/// an older build that does not report them still verifies.
pub const CODEX_MANAGED_DISABLED_IF_KNOWN: &[&str] = &[
    "api_key_cyber_access_programs",
    "api_key_model_discovery",
    "auth_elicitation",
    "mentions_v2",
    "view_image",
];

/// Native feature values the managed role requires exactly.
fn codex_required_features() -> serde_json::Map<String, Value> {
    let mut features = serde_json::Map::new();
    features.insert("hooks".into(), json!(true));
    features.insert("code_mode".into(), json!({"enabled":false}));
    for feature in CODEX_DISABLED_FEATURES {
        features.insert((*feature).into(), json!(false));
    }
    features
}

/// Native settings for the dedicated managed Codex role. These settings are
/// separate from research; hooks remain enabled while execution surfaces close.
/// `view_image` is written both as the historical `tools.view_image` and as
/// the 0.167.0 feature, which ignores the former.
pub fn codex_settings() -> Value {
    let mut settings = json!({"agents":{"enabled":false},"tools":{"view_image":false},"web_search":"disabled","features":codex_required_features()});
    for feature in CODEX_MANAGED_DISABLED_IF_KNOWN {
        settings["features"][*feature] = json!(false);
    }
    settings
}

/// An unrecognized native value that grants nothing: false, null or a table
/// whose only entry is `enabled = false`.
fn codex_disabled(value: &Value) -> bool {
    match value {
        Value::Bool(false) | Value::Null => true,
        Value::Object(table) => table.len() == 1 && table.get("enabled") == Some(&json!(false)),
        _ => false,
    }
}

/// Compare one live hook event with the pinned declaration. Every pinned
/// group and handler field must be equal, in order. codex-cli 0.167.0 echoes
/// each handler with `commandWindows`, `async` and `statusMessage`; only their
/// inert defaults are accepted, so an asynchronous (non-gating) or otherwise
/// widened handler is refused.
fn codex_hook_groups_match(live: &Value, pinned: &Value) -> bool {
    let (Some(live), Some(pinned)) = (live.as_array(), pinned.as_array()) else {
        return false;
    };
    let object_matches = |live: &Value, pinned: &Value, inert: &[(&str, Value)]| {
        let (Some(live), Some(pinned)) = (live.as_object(), pinned.as_object()) else {
            return false;
        };
        pinned
            .iter()
            .all(|(key, value)| live.get(key) == Some(value))
            && live.iter().all(|(key, value)| {
                pinned.contains_key(key)
                    || inert
                        .iter()
                        .any(|(name, default)| key == name && value == default)
            })
    };
    let inert = [
        ("commandWindows", Value::Null),
        ("async", json!(false)),
        ("statusMessage", Value::Null),
    ];
    !pinned.is_empty()
        && live.len() == pinned.len()
        && live.iter().zip(pinned).all(|(live, pinned)| {
            let (Some(live_handlers), Some(pinned_handlers)) = (
                live.get("hooks").and_then(Value::as_array),
                pinned.get("hooks").and_then(Value::as_array),
            ) else {
                return false;
            };
            let mut live_group = live.clone();
            let mut pinned_group = pinned.clone();
            live_group["hooks"] = Value::Null;
            pinned_group["hooks"] = Value::Null;
            !pinned_handlers.is_empty()
                && live_group == pinned_group
                && live_handlers.len() == pinned_handlers.len()
                && live_handlers
                    .iter()
                    .zip(pinned_handlers)
                    .all(|(live, pinned)| object_matches(live, pinned, &inert))
        })
}

/// Verify a live config/read result against the exact trusted hook groups.
/// The caller supplies the already authenticated hook declarations; an empty,
/// missing or widened group is never accepted. Returns the accepted native
/// drift (unrecognized disabled features and empty hook events) for the
/// caller's qualification evidence.
pub fn verify_managed_config(
    response: &Value,
    pre_tool_use: &Value,
    permission_request: &Value,
    trusted_state: &Value,
) -> Result<Value> {
    let required = codex_required_features();
    for (name, value) in &required {
        if response.pointer(&format!("/config/features/{name}")) != Some(value) {
            return Err(invalid("native managed feature controls did not verify"));
        }
    }
    let features = response
        .pointer("/config/features")
        .and_then(Value::as_object)
        .ok_or_else(|| invalid("native managed feature controls did not verify"))?;
    // Codex reports a version-dependent subset of its features. A feature Run
    // does not pin is accepted only while disabled; `fast_mode` is a reviewed
    // service-tier choice.
    let mut unknown_features = serde_json::Map::new();
    for (name, value) in features {
        if required.contains_key(name) || (name == "fast_mode" && value.is_boolean()) {
            continue;
        }
        if !codex_disabled(value) {
            return Err(invalid("unknown native managed feature"));
        }
        unknown_features.insert(name.clone(), value.clone());
    }
    // 0.167.0 ignores `tools.view_image` and reports `features.view_image`.
    // Either report must be false, and at least one must be present.
    let view_image = [
        response.pointer("/config/tools/view_image"),
        response.pointer("/config/features/view_image"),
    ];
    if response.pointer("/config/agents/enabled") != Some(&json!(false))
        || view_image
            .iter()
            .flatten()
            .any(|value| *value != &json!(false))
        || view_image.iter().all(Option::is_none)
        || response.pointer("/config/web_search") != Some(&json!("disabled"))
    {
        return Err(invalid("native managed agent controls did not verify"));
    }
    for (event, pinned) in [
        ("PreToolUse", pre_tool_use),
        ("PermissionRequest", permission_request),
    ] {
        if !response
            .pointer(&format!("/config/hooks/{event}"))
            .is_some_and(|live| codex_hook_groups_match(live, pinned))
        {
            return Err(invalid("native managed hooks did not verify"));
        }
    }
    let hooks = response
        .pointer("/config/hooks")
        .and_then(Value::as_object)
        .ok_or_else(|| invalid("native managed hooks did not verify"))?;
    // 0.167.0 lists every hook event. Another event is accepted only while it
    // has no handler group; any configured handler there is refused.
    let mut empty_hook_events = BTreeSet::new();
    for (event, groups) in hooks {
        if matches!(event.as_str(), "PreToolUse" | "PermissionRequest" | "state") {
            continue;
        }
        if !(groups.is_null() || groups.as_array().is_some_and(Vec::is_empty)) {
            return Err(invalid("native managed hooks did not verify"));
        }
        empty_hook_events.insert(event.clone());
    }
    if !trusted_state
        .as_object()
        .is_some_and(|state| !state.is_empty())
        || hooks.get("state") != Some(trusted_state)
        || response.pointer("/config/plugins").is_some_and(|plugins| {
            !plugins
                .as_object()
                .is_some_and(|entries| entries.is_empty())
        })
    {
        return Err(invalid(
            "native managed hook trust or plugins did not verify",
        ));
    }
    Ok(json!({
        "unknown_features": unknown_features,
        "empty_hook_events": empty_hook_events,
    }))
}

/// Apply the same no-environment setting to both thread/start and thread/resume.
pub fn codex_thread_parameters(params: &mut Value) -> Result<()> {
    let object = params
        .as_object_mut()
        .ok_or_else(|| invalid("invalid native thread parameters"))?;
    object.insert("environments".into(), json!([]));
    Ok(())
}

/// Reject an incomplete, paginated, widened or disconnected native MCP catalog.
/// `allowed` must be derived from authenticated Pool and worker definitions.
pub fn verify_inventory(
    response: &Value,
    allowed: &BTreeMap<String, BTreeSet<String>>,
) -> Result<()> {
    if allowed.is_empty() || response.get("nextCursor").is_some_and(|v| !v.is_null()) {
        return Err(invalid("native managed inventory did not verify"));
    }
    let servers = response
        .get("data")
        .and_then(Value::as_array)
        .ok_or_else(|| invalid("native managed inventory did not verify"))?;
    if servers.len() != allowed.len() {
        return Err(invalid("native managed inventory did not verify"));
    }
    let mut seen = BTreeSet::new();
    for server in servers {
        let name = server
            .get("name")
            .and_then(Value::as_str)
            .ok_or_else(|| invalid("native managed inventory did not verify"))?;
        let tools = server
            .get("tools")
            .and_then(Value::as_object)
            .ok_or_else(|| invalid("native managed inventory did not verify"))?;
        if !seen.insert(name)
            || server["runtimeStatus"] != "connected"
            || allowed.get(name) != Some(&tools.keys().cloned().collect())
        {
            return Err(invalid("native managed inventory did not verify"));
        }
    }
    Ok(())
}

/// Closed native Claude exclusions for the dedicated managed role.
pub const CLAUDE_DISALLOWED_TOOLS: &str = "Bash,Agent,Task,Skill";

/// Exact managed deny argument: the fixed native exclusions and the sorted
/// discovered complement of each pinned Pool MCP allowlist. The worker MCP
/// is supervisor-owned and has no external catalog-discovery process.
pub fn claude_disallowed_tools(
    allowed: &BTreeMap<String, BTreeSet<String>>,
    catalogs: &BTreeMap<String, BTreeSet<String>>,
) -> Result<String> {
    let fail = || invalid("managed Claude catalog did not verify");
    let mut denied = BTreeSet::new();
    let mut expected = 0;
    for (server, allowed_tools) in allowed {
        if server == agent_run_domain::worker::SERVER_NAME {
            continue;
        }
        expected += 1;
        let catalog = catalogs.get(server).ok_or_else(fail)?;
        if allowed_tools.is_empty()
            || !allowed_tools.is_subset(catalog)
            || server.is_empty()
            || server.contains("__")
            || server.ends_with('_')
            || !server
                .bytes()
                .all(|c| c.is_ascii_alphanumeric() || matches!(c, b'_' | b'-'))
            || catalog.iter().any(|name| {
                name.is_empty()
                    || !name
                        .bytes()
                        .all(|c| c.is_ascii_alphanumeric() || matches!(c, b'_' | b'-'))
            })
        {
            return Err(fail());
        }
        for tool in catalog.difference(allowed_tools) {
            denied.insert(format!("mcp__{server}__{tool}"));
        }
    }
    if expected == 0 || catalogs.len() != expected || denied.len() > 4096 {
        return Err(fail());
    }
    let mut result = CLAUDE_DISALLOWED_TOOLS.to_owned();
    for rule in denied {
        result.push(',');
        result.push_str(&rule);
    }
    if result.len() > 64 * 1024 {
        return Err(fail());
    }
    Ok(result)
}

/// Construct the closed native Claude tool arguments from a previously
/// authenticated exact MCP inventory. No plugin or skill directory is added.
pub fn claude_tool_arguments(allowed: &BTreeMap<String, BTreeSet<String>>) -> Result<Vec<String>> {
    if allowed.is_empty() {
        return Err(invalid("managed MCP allowlist is empty"));
    }
    let mut names = Vec::new();
    for (server, tools) in allowed {
        let valid = |part: &str| {
            !part.is_empty()
                && part
                    .bytes()
                    .all(|c| c.is_ascii_alphanumeric() || matches!(c, b'_' | b'-'))
        };
        if !valid(server)
            || server.contains("__")
            || tools.is_empty()
            || tools.iter().any(|name| !valid(name))
        {
            return Err(invalid("invalid managed MCP allowlist"));
        }
        names.extend(tools.iter().map(|tool| format!("mcp__{server}__{tool}")));
    }
    Ok(vec![
        "--tools".into(),
        "".into(),
        "--allowedTools".into(),
        names.join(","),
        "--disallowedTools".into(),
        CLAUDE_DISALLOWED_TOOLS.into(),
        "--strict-mcp-config".into(),
        "--disable-slash-commands".into(),
        "--no-chrome".into(),
    ])
}

/// Check Claude's native init inventory as the post-input qualification tripwire.
/// Unlike `mcp_status`, `system/init` lists the model-visible tools after the
/// deny rules apply (2.1.311 omits the denied Pool tools), so it stays exact.
pub fn verify_claude_inventory(
    frame: &Value,
    allowed: &BTreeMap<String, BTreeSet<String>>,
) -> Result<()> {
    let args = claude_tool_arguments(allowed)?;
    let expected: BTreeSet<_> = args[3].split(',').collect();
    let tools = frame
        .get("tools")
        .and_then(Value::as_array)
        .ok_or_else(|| invalid("native managed inventory did not verify"))?;
    let mut actual = BTreeSet::new();
    if frame["type"] != "system"
        || frame["subtype"] != "init"
        || tools
            .iter()
            .any(|tool| tool.as_str().is_none_or(|name| !actual.insert(name)))
        || actual != expected
    {
        return Err(invalid("native managed inventory did not verify"));
    }
    Ok(())
}

/// Digest of the discovered Pool catalogs from which the managed deny
/// complement was computed; the supervisor records it in the config digest.
pub fn claude_catalog_digest(catalogs: &BTreeMap<String, BTreeSet<String>>) -> Result<String> {
    Ok(agent_run_domain::canonical::sha256_hex(
        &serde_json::to_value(catalogs)?,
        true,
    ))
}

/// Check the pre-model control replies. Claude 2.1.311 reports declarative
/// built-in agents even when their tools are disabled; their exact list is
/// digested by the caller, while commands must be disabled and servers exact.
///
/// `mcp_status` lists each server's raw `tools/list` result, before Claude
/// applies `--allowedTools`/`--disallowedTools`: 2.1.311 reports the whole
/// Pool catalog, including the tools Run denies. A Pool server's list is
/// therefore accepted when it equals either its allowlist or exactly the
/// catalog discovered for this launch, and only when that catalog has the
/// recorded digest, contains the allowlist, and its complement is exactly the
/// `disallowed` argument passed to the harness. A tool outside the discovered
/// catalog, a missing allowed tool or an unknown server is refused. The
/// model-visible list is pinned separately by the post-input `system/init`
/// tripwire, which must equal the allowlist exactly. Returns, per server,
/// whether the reported list was the allowlist or the catalog.
pub fn verify_claude_control_inventory(
    initialize: &Value,
    status: &Value,
    allowed: &BTreeMap<String, BTreeSet<String>>,
    catalogs: &BTreeMap<String, BTreeSet<String>>,
    catalog_sha256: &str,
    disallowed: &str,
) -> Result<Value> {
    if claude_catalog_digest(catalogs)? != catalog_sha256
        || claude_disallowed_tools(allowed, catalogs)? != disallowed
    {
        return Err(invalid("native managed MCP catalog did not verify"));
    }
    let init = initialize
        .pointer("/response/response")
        .and_then(Value::as_object)
        .ok_or_else(|| invalid("native managed initialization did not verify"))?;
    for field in [
        "pending_permission_requests",
        "pending_user_dialog_requests",
    ] {
        if initialize
            .pointer(&format!("/response/{field}"))
            .is_some_and(|value| !value.as_array().is_some_and(Vec::is_empty))
        {
            return Err(invalid("native managed initialization did not verify"));
        }
    }
    if !init
        .get("commands")
        .and_then(Value::as_array)
        .is_some_and(Vec::is_empty)
        || !init.get("agents").is_some_and(Value::is_array)
    {
        return Err(invalid("native managed initialization did not verify"));
    }
    let servers = status
        .pointer("/response/response/mcpServers")
        .and_then(Value::as_array)
        .ok_or_else(|| invalid("native managed MCP status did not verify"))?;
    if servers.len() != allowed.len() {
        return Err(invalid("native managed MCP status did not verify"));
    }
    let mut seen = BTreeSet::new();
    let mut reported = serde_json::Map::new();
    for server in servers {
        let name = server
            .get("name")
            .and_then(Value::as_str)
            .ok_or_else(|| invalid("native managed MCP status did not verify"))?;
        let tools = allowed
            .get(name)
            .ok_or_else(|| invalid("native managed MCP status did not verify"))?;
        if !seen.insert(name) || server.get("status").and_then(Value::as_str) != Some("connected") {
            return Err(invalid("native managed MCP status did not verify"));
        }
        // Some native versions omit per-server tools here. The caller has
        // separately pinned --tools "" and the later system/init tripwire.
        let mut kind = "omitted";
        if let Some(value) = server.get("tools") {
            let list = value
                .as_array()
                .ok_or_else(|| invalid("native managed MCP status did not verify"))?;
            let mut found = BTreeSet::new();
            for item in list {
                let name = item
                    .as_str()
                    .or_else(|| item.get("name").and_then(Value::as_str))
                    .ok_or_else(|| invalid("native managed MCP status did not verify"))?;
                if !found.insert(name.to_owned()) {
                    return Err(invalid("native managed MCP status did not verify"));
                }
            }
            // The worker server has no discovered catalog and stays exact.
            kind = if &found == tools {
                "allowlist"
            } else if catalogs.get(name) == Some(&found) {
                "catalog"
            } else {
                return Err(invalid("native managed MCP status did not verify"));
            };
        }
        reported.insert(name.to_owned(), json!(kind));
    }
    Ok(Value::Object(reported))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn managed_role_features_and_hooks() {
        let pre = json!([{"matcher":"mcp__pool__.*","hooks":[{"type":"command","command":"/verified/bridge-hook","timeout":600}]}]);
        let permission = json!([{"matcher":"mcp__pool__.*","hooks":[{"type":"command","command":"/verified/run-hook","timeout":600}]}]);
        let mut config = codex_settings();
        let trust = json!({"bridge":{"trusted_hash":"a"},"run":{"trusted_hash":"b"}});
        config["hooks"] = json!({"PreToolUse":pre,"PermissionRequest":permission,"state":trust});
        let response = json!({"config":config});
        let pre = &response["config"]["hooks"]["PreToolUse"];
        let permission = &response["config"]["hooks"]["PermissionRequest"];
        let trust = &response["config"]["hooks"]["state"];
        verify_managed_config(&response, pre, permission, trust).unwrap();
        for feature in CODEX_DISABLED_FEATURES
            .iter()
            .chain([&"code_mode", &"hooks"])
        {
            let mut missing = response.clone();
            missing["config"]["features"]
                .as_object_mut()
                .unwrap()
                .remove(*feature);
            assert!(
                verify_managed_config(&missing, pre, permission, trust).is_err(),
                "{feature}"
            );
            let mut widened = response.clone();
            widened["config"]["features"][*feature] = if *feature == "hooks" {
                json!(false)
            } else {
                json!(true)
            };
            assert!(
                verify_managed_config(&widened, pre, permission, trust).is_err(),
                "{feature}"
            );
        }
        let mut extra_default = response.clone();
        extra_default["config"]["features"]["fast_mode"] = json!(true);
        assert!(verify_managed_config(&extra_default, pre, permission, trust).is_ok());
        for disabled in [json!(false), json!(null), json!({"enabled":false})] {
            extra_default["config"]["features"]["unknown_future"] = disabled.clone();
            let accepted = verify_managed_config(&extra_default, pre, permission, trust).unwrap();
            assert_eq!(accepted["unknown_features"]["unknown_future"], disabled);
        }
        for enabled in [json!(true), json!("on"), json!({"enabled":true}), json!(1)] {
            extra_default["config"]["features"]["unknown_future"] = enabled;
            assert!(verify_managed_config(&extra_default, pre, permission, trust).is_err());
        }
        for feature in CODEX_MANAGED_DISABLED_IF_KNOWN {
            let mut enabled = response.clone();
            enabled["config"]["features"][*feature] = json!(true);
            assert!(
                verify_managed_config(&enabled, pre, permission, trust).is_err(),
                "{feature}"
            );
            enabled["config"]["features"]
                .as_object_mut()
                .unwrap()
                .remove(*feature);
            verify_managed_config(&enabled, pre, permission, trust).unwrap();
        }
        // view_image: either report false, at least one present, none true.
        let mut view = response.clone();
        view["config"]["tools"]
            .as_object_mut()
            .unwrap()
            .remove("view_image");
        verify_managed_config(&view, pre, permission, trust).unwrap();
        view["config"]["features"]
            .as_object_mut()
            .unwrap()
            .remove("view_image");
        assert!(verify_managed_config(&view, pre, permission, trust).is_err());
        view["config"]["tools"]["view_image"] = json!(false);
        verify_managed_config(&view, pre, permission, trust).unwrap();
        view["config"]["features"]["view_image"] = json!(true);
        assert!(verify_managed_config(&view, pre, permission, trust).is_err());
        for pointer in ["PreToolUse", "PermissionRequest"] {
            let mut changed = response.clone();
            changed["config"]["hooks"][pointer][0]["hooks"][0]["command"] = json!("/foreign");
            assert!(verify_managed_config(&changed, pre, permission, trust).is_err());
            // Echoed handler defaults are inert; any other value or field is not.
            let mut echoed = response.clone();
            let handler = &mut echoed["config"]["hooks"][pointer][0]["hooks"][0];
            handler["commandWindows"] = json!(null);
            handler["async"] = json!(false);
            handler["statusMessage"] = json!(null);
            verify_managed_config(&echoed, pre, permission, trust).unwrap();
            for (field, value) in [
                ("async", json!(true)),
                ("commandWindows", json!("C:\\foreign.exe")),
                ("statusMessage", json!("x")),
                ("env", json!({})),
            ] {
                let mut widened = echoed.clone();
                widened["config"]["hooks"][pointer][0]["hooks"][0][field] = value;
                assert!(
                    verify_managed_config(&widened, pre, permission, trust).is_err(),
                    "{field}"
                );
            }
            let mut removed = response.clone();
            removed["config"]["hooks"][pointer][0]["hooks"][0]
                .as_object_mut()
                .unwrap()
                .remove("timeout");
            assert!(verify_managed_config(&removed, pre, permission, trust).is_err());
            let mut doubled = response.clone();
            let group = doubled["config"]["hooks"][pointer][0].clone();
            doubled["config"]["hooks"][pointer]
                .as_array_mut()
                .unwrap()
                .push(group);
            assert!(verify_managed_config(&doubled, pre, permission, trust).is_err());
            let mut matcher = response.clone();
            matcher["config"]["hooks"][pointer][0]["matcher"] = json!(".*");
            assert!(verify_managed_config(&matcher, pre, permission, trust).is_err());
            let mut missing = response.clone();
            missing["config"]["hooks"]
                .as_object_mut()
                .unwrap()
                .remove(pointer);
            assert!(verify_managed_config(&missing, pre, permission, trust).is_err());
        }
        let mut widened = response.clone();
        widened["config"]["agents"]["enabled"] = json!(true);
        assert!(verify_managed_config(&widened, pre, permission, trust).is_err());
        let mut missing = response.clone();
        missing["config"].as_object_mut().unwrap().remove("agents");
        assert!(verify_managed_config(&missing, pre, permission, trust).is_err());
        let mut empty = response.clone();
        empty["config"]["hooks"]["PostToolUse"] = json!([]);
        empty["config"]["hooks"]["Stop"] = json!(null);
        let accepted = verify_managed_config(&empty, pre, permission, trust).unwrap();
        assert_eq!(
            accepted["empty_hook_events"],
            json!(["PostToolUse", "Stop"])
        );
        for groups in [
            json!([{"hooks":[{"type":"command","command":"/foreign"}]}]),
            json!([{}]),
            json!({}),
            json!(false),
        ] {
            let mut widened = response.clone();
            widened["config"]["hooks"]["PostToolUse"] = groups;
            assert!(verify_managed_config(&widened, pre, permission, trust).is_err());
        }
        let mut widened = response.clone();
        widened["config"]["hooks"]["state"]["bridge"]["trusted_hash"] = json!("foreign");
        assert!(verify_managed_config(&widened, pre, permission, trust).is_err());
        let mut widened = response.clone();
        widened["config"]["plugins"]["foreign"]["enabled"] = json!(true);
        assert!(verify_managed_config(&widened, pre, permission, trust).is_err());
        for method in ["thread/start", "thread/resume"] {
            let mut params = json!({"method":method,"environments":[{"id":"foreign"}]});
            codex_thread_parameters(&mut params).unwrap();
            assert_eq!(params["environments"], json!([]));
        }
        let allowed = BTreeMap::from([("pool".into(), BTreeSet::from(["pool_get".into()]))]);
        let inventory = json!({"data":[{"name":"pool","runtimeStatus":"connected","tools":{"pool_get":{}}}],"nextCursor":null});
        verify_inventory(&inventory, &allowed).unwrap();
        let mut widened = inventory.clone();
        widened["data"][0]["tools"]["unsafe"] = json!({});
        assert!(verify_inventory(&widened, &allowed).is_err());
        let mut disconnected = inventory.clone();
        disconnected["data"][0]["runtimeStatus"] = json!("disconnected");
        assert!(verify_inventory(&disconnected, &allowed).is_err());
        let mut paged = inventory.clone();
        paged["nextCursor"] = json!("next");
        assert!(verify_inventory(&paged, &allowed).is_err());
        assert_eq!(CLAUDE_DISALLOWED_TOOLS, "Bash,Agent,Task,Skill");
        let args = claude_tool_arguments(&allowed).unwrap();
        assert_eq!(
            &args[..6],
            [
                "--tools",
                "",
                "--allowedTools",
                "mcp__pool__pool_get",
                "--disallowedTools",
                "Bash,Agent,Task,Skill"
            ]
        );
        assert!(!args.iter().any(|arg| arg == "--plugin-dir"));
        let init = json!({"type":"system","subtype":"init","tools":["mcp__pool__pool_get"]});
        verify_claude_inventory(&init, &allowed).unwrap();
        let mut widened = init.clone();
        widened["tools"].as_array_mut().unwrap().push(json!("Bash"));
        assert!(verify_claude_inventory(&widened, &allowed).is_err());
    }

    #[test]
    fn claude_controls_require_no_commands_and_exact_connected_servers() {
        let allowed = BTreeMap::from([("pool".into(), BTreeSet::from(["pool_get".into()]))]);
        let catalogs = BTreeMap::from([(
            "pool".into(),
            BTreeSet::from(["pool_get".into(), "pool_start_batch".into()]),
        )]);
        let digest = claude_catalog_digest(&catalogs).unwrap();
        let denied = claude_disallowed_tools(&allowed, &catalogs).unwrap();
        let verify = |init: &Value, status: &Value| {
            verify_claude_control_inventory(init, status, &allowed, &catalogs, &digest, &denied)
        };
        let init = json!({"type":"control_response","response":{"subtype":"success","request_id":"i","response":{"commands":[],"agents":[{"name":"Explore"}]}}});
        let status = json!({"type":"control_response","response":{"subtype":"success","request_id":"s","response":{"mcpServers":[{"name":"pool","status":"connected"}]}}});
        assert_eq!(verify(&init, &status).unwrap(), json!({"pool":"omitted"}));
        let mut bad = init.clone();
        bad["response"]["response"]["commands"] = json!([{"name":"command"}]);
        assert!(verify(&bad, &status).is_err());
        let mut bad = status.clone();
        bad["response"]["response"]["mcpServers"][0]["status"] = json!("failed");
        assert!(verify(&init, &bad).is_err());
        bad = status.clone();
        bad["response"]["response"]["mcpServers"][0]["tools"] = json!(["foreign"]);
        assert!(verify(&init, &bad).is_err());
        bad["response"]["response"]["mcpServers"][0]["tools"] = json!(["pool_get"]);
        assert_eq!(verify(&init, &bad).unwrap(), json!({"pool":"allowlist"}));
        // The raw server list may be the whole discovered catalog, never more,
        // fewer or different tools.
        bad["response"]["response"]["mcpServers"][0]["tools"] =
            json!([{"name":"pool_start_batch"}, {"name":"pool_get"}]);
        assert_eq!(verify(&init, &bad).unwrap(), json!({"pool":"catalog"}));
        for tools in [
            json!(["pool_get", "pool_start_batch", "pool_start_agent"]),
            json!(["pool_start_batch"]),
            json!(["pool_get", "pool_get", "pool_start_batch"]),
        ] {
            bad["response"]["response"]["mcpServers"][0]["tools"] = tools;
            assert!(verify(&init, &bad).is_err());
        }
        bad["response"]["response"]["mcpServers"][0]["tools"] =
            json!(["pool_get", "pool_start_batch"]);
        // The catalog must be the recorded one and its complement the passed deny argument.
        let other = BTreeMap::from([(
            "pool".into(),
            BTreeSet::from(["pool_get".into(), "pool_start_agent".into()]),
        )]);
        assert!(
            verify_claude_control_inventory(
                &init,
                &bad,
                &allowed,
                &catalogs,
                &"0".repeat(64),
                &denied
            )
            .is_err()
        );
        assert!(
            verify_claude_control_inventory(&init, &bad, &allowed, &other, &digest, &denied)
                .is_err()
        );
        assert!(
            verify_claude_control_inventory(
                &init,
                &bad,
                &allowed,
                &catalogs,
                &digest,
                CLAUDE_DISALLOWED_TOOLS
            )
            .is_err()
        );
        let mut foreign = bad.clone();
        foreign["response"]["response"]["mcpServers"][0]["name"] = json!("foreign");
        assert!(verify(&init, &foreign).is_err());
    }
}
