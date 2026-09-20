//! Native plugin installation and Codex hook trust digests.
use super::materialize::{shell_command, Publisher};
use agent_run_config::config::{Adapter, Runtime};
use agent_run_domain::{error::invalid, Result};
use agent_run_platform::fs;
use serde_json::{json, Value};
use std::{
    collections::BTreeMap,
    path::{Path, PathBuf},
};
/// Materialized plugin roots and trust evidence for one runtime home.
pub struct Installed {
    /// Runtime-visible plugin roots in declaration order.
    pub paths: Vec<PathBuf>,
    /// Plugin manifest name to runtime-visible root.
    pub roots: BTreeMap<String, PathBuf>,
    /// Codex marketplace plugin names in declaration order.
    pub codex_names: Vec<String>,
    /// Codex hook identity to trusted content digest.
    pub trust: BTreeMap<String, String>,
}

/// Returns the one declared plugin skill directory, or `None` when no plugin
/// owns `name`; duplicate owners are rejected so skill resolution cannot pick
/// an arbitrary source.
pub fn plugin_skill_dir(plugins: &[PathBuf], name: &str) -> Result<Option<PathBuf>> {
    let mut found = None;
    for plugin in plugins {
        let candidate = plugin.join("skills").join(name);
        if !candidate.join("SKILL.md").is_file() {
            continue;
        }
        if found.is_some() {
            return Err(invalid(format!(
                "skill {name:?} is shipped by two declared plugins"
            )));
        }
        found = Some(candidate);
    }
    Ok(found)
}

/// Resolves selected names to plugin-owned or local skill directories in input
/// order, preserving the host's deterministic selection order.
pub fn skill_dirs(
    plugins: &[PathBuf],
    skills_root: &Path,
    names: &[String],
) -> Result<Vec<(String, PathBuf)>> {
    names
        .iter()
        .map(|name| {
            let path = plugin_skill_dir(plugins, name)?.unwrap_or_else(|| skills_root.join(name));
            Ok((name.clone(), path))
        })
        .collect()
}

/// Returns selected skill names that must be copied by the runtime itself
/// because no declared plugin provides them.
pub fn local_skill_names(plugins: &[PathBuf], names: &[String]) -> Result<Vec<String>> {
    names
        .iter()
        .filter_map(|name| match plugin_skill_dir(plugins, name) {
            Ok(None) => Some(Ok(name.clone())),
            Ok(Some(_)) => None,
            Err(error) => Some(Err(error)),
        })
        .collect()
}

/// Reports plugin-shipped skills omitted from `names`, which wholesale plugin
/// hosts would expose unless their configuration fails closed.
pub fn unlisted_plugin_skills(plugins: &[PathBuf], names: &[String]) -> Vec<String> {
    let selected = names.iter().collect::<std::collections::BTreeSet<_>>();
    let mut found = std::collections::BTreeSet::new();
    for plugin in plugins {
        let Ok(entries) = std::fs::read_dir(plugin.join("skills")) else {
            continue;
        };
        for entry in entries.flatten() {
            let path = entry.path();
            if !selected.contains(&entry.file_name().to_string_lossy().to_string())
                && path.join("SKILL.md").is_file()
            {
                found.insert(entry.file_name().to_string_lossy().into_owned());
            }
        }
    }
    found.into_iter().collect()
}
fn event_label(event: &str) -> Result<&'static str> {
    Ok(match event {
        "PreToolUse" => "pre_tool_use",
        "PermissionRequest" => "permission_request",
        "PostToolUse" => "post_tool_use",
        "PostToolUseFailure" => "post_tool_use_failure",
        "PreCompact" => "pre_compact",
        "PostCompact" => "post_compact",
        "SessionStart" => "session_start",
        "UserPromptSubmit" => "user_prompt_submit",
        "SubagentStart" => "subagent_start",
        "SubagentStop" => "subagent_stop",
        "SessionEnd" => "session_end",
        "Stop" => "stop",
        _ => return Err(invalid("unsupported Codex hook event")),
    })
}
fn digest(event: &str, matcher: Option<&str>, raw: &Value) -> Result<String> {
    if raw.get("type").and_then(Value::as_str) != Some("command") {
        return Err(invalid("only command hooks can be trusted"));
    }
    let command = raw
        .get("command")
        .and_then(Value::as_str)
        .filter(|s| !s.is_empty())
        .ok_or_else(|| invalid("hook command is missing"))?;
    if raw.get("additionalContextLimit").is_some() {
        return Err(invalid("additionalContextLimit trust is unsupported"));
    }
    let timeout = match raw.get("timeout") {
        None => 600,
        Some(v) => v
            .as_i64()
            .ok_or_else(|| invalid("hook timeout must be an integer"))?
            .max(1),
    };
    let asynchronous = match raw.get("async") {
        None => false,
        Some(v) => v
            .as_bool()
            .ok_or_else(|| invalid("hook async must be boolean"))?,
    };
    let mut handler =
        json!({"type":"command","command":command,"timeout":timeout,"async":asynchronous});
    if let Some(status) = raw.get("statusMessage") {
        if !status.is_string() {
            return Err(invalid("hook statusMessage must be a string"));
        }
        handler["statusMessage"] = status.clone();
    }
    let mut identity = json!({"event_name":event,"hooks":[handler]});
    if !["user_prompt_submit", "stop"].contains(&event) {
        if let Some(matcher) = matcher {
            identity["matcher"] = json!(matcher);
        }
    }
    Ok(format!(
        "sha256:{}",
        fs::sha256(&serde_json::to_vec(&identity)?)
    ))
}
fn manifest(path: &Path) -> Result<(String, String)> {
    let dir = fs::Dir::open(path)?;
    for file in [".codex-plugin/plugin.json", ".claude-plugin/plugin.json"] {
        if let Some(data) = dir.optional(Path::new(file), 64 * 1024)? {
            let v: Value = serde_json::from_slice(&data)?;
            if let (Some(name), Some(version)) = (
                v.get("name").and_then(Value::as_str),
                v.get("version").and_then(Value::as_str),
            ) {
                let safe = |s: &str| {
                    !s.is_empty()
                        && s.bytes()
                            .all(|c| c.is_ascii_alphanumeric() || b"_.+-".contains(&c))
                };
                if safe(name) && safe(version) {
                    return Ok((name.into(), version.into()));
                }
            }
        }
    }
    Err(invalid("plugin needs a safe name/version manifest"))
}
/// Installs declared plugins for `kind` and returns their runtime-visible roots.
///
/// Codex receives copied, digest-bound plugin assets. Claude-family runtimes
/// retain declared roots unless a bounded snapshot asset list requires a copy.
/// Invalid manifests, duplicate names, unsafe hooks, and missing assets fail
/// before the returned installation can be used.
pub fn install(p: &mut Publisher, runtime: &Runtime, kind: Adapter) -> Result<Installed> {
    let mut installed = Installed {
        paths: vec![],
        roots: BTreeMap::new(),
        codex_names: vec![],
        trust: BTreeMap::new(),
    };
    let mut listed = Vec::new();
    for source in &runtime.plugins {
        let (name, version) = manifest(source)?;
        if installed.roots.contains_key(&name) {
            return Err(invalid("duplicate plugin name"));
        }
        let base = source
            .file_name()
            .and_then(|n| n.to_str())
            .ok_or_else(|| invalid("invalid plugin directory"))?;
        let selected = runtime.plugin_snapshot_assets.get(base);
        let relative = match kind {
            Adapter::Codex => format!("plugins/cache/personal/{name}/{version}"),
            _ => format!("declared-plugins/{base}"),
        };
        let copy = kind == Adapter::Codex || selected.is_some();
        let root = if copy {
            p.tree(source, &relative, selected.map(Vec::as_slice))?;
            p.root.join(&relative)
        } else {
            source.clone()
        };
        installed.paths.push(root.clone());
        installed.roots.insert(name.clone(), root.clone());
        if kind == Adapter::Codex {
            installed.codex_names.push(name.clone());
            listed.push(json!({"name":name,"source":{"source":"local","path":format!("./{relative}")},"policy":{"installation":"AVAILABLE","authentication":"ON_INSTALL"}}));
        }
        if kind == Adapter::Codex {
            if let Some(data) =
                fs::Dir::open(source)?.optional(Path::new("hooks/hooks.json"), 1024 * 1024)?
            {
                let doc: Value = serde_json::from_slice(&data)?;
                let events = doc
                    .get("hooks")
                    .and_then(Value::as_object)
                    .ok_or_else(|| invalid("plugin hooks table is missing"))?;
                for (event, groups) in events {
                    let label = event_label(event)?;
                    let groups = groups
                        .as_array()
                        .ok_or_else(|| invalid("plugin hook groups must be an array"))?;
                    for (gi, group) in groups.iter().enumerate() {
                        let matcher = group
                            .get("matcher")
                            .map(|v| {
                                v.as_str()
                                    .ok_or_else(|| invalid("hook matcher must be a string"))
                            })
                            .transpose()?;
                        let handlers = group
                            .get("hooks")
                            .and_then(Value::as_array)
                            .ok_or_else(|| invalid("plugin handlers must be an array"))?;
                        for (hi, handler) in handlers.iter().enumerate() {
                            installed.trust.insert(
                                format!("{name}@personal:hooks/hooks.json:{label}:{gi}:{hi}"),
                                digest(label, matcher, handler)?,
                            );
                        }
                    }
                }
            }
        }
    }
    if kind == Adapter::Codex && !listed.is_empty() {
        p.json(
            ".agents/plugins/marketplace.json",
            &json!({"name":"personal","interface":{"displayName":"agent-run"},"plugins":listed}),
        )?;
    }
    Ok(installed)
}
pub fn expand(args: &[String], roots: &BTreeMap<String, PathBuf>) -> Result<Vec<String>> {
    let re = regex::Regex::new(r"\{plugin:([A-Za-z0-9][A-Za-z0-9_.+-]*)\}").expect("static regex");
    args.iter()
        .map(|arg| {
            for caps in re.captures_iter(arg) {
                if !roots.contains_key(&caps[1]) {
                    return Err(invalid("hook references an undeclared plugin"));
                }
            }
            Ok(re
                .replace_all(arg, |caps: &regex::Captures<'_>| {
                    roots[&caps[1]].to_string_lossy().into_owned()
                })
                .into_owned())
        })
        .collect()
}
pub fn hook_groups(runtime: &Runtime, roots: &BTreeMap<String, PathBuf>) -> Result<Value> {
    let mut hooks = serde_json::Map::new();
    for hook in &runtime.hooks {
        let command = shell_command(&expand(&hook.command, roots)?);
        let mut group = json!({"hooks":[{"type":"command","command":command,"timeout":600}]});
        if let Some(m) = &hook.matcher {
            group["matcher"] = json!(m);
        }
        hooks
            .entry(hook.event.clone())
            .or_insert(json!([]))
            .as_array_mut()
            .expect("array")
            .push(group);
    }
    Ok(Value::Object(hooks))
}
/// Adds generated Codex hooks, trust receipts, and enabled plugin declarations.
///
/// Empty generated tables are omitted to preserve the Python home shape. Hook
/// trust derives from the exact rendered command and matcher; invalid plugin
/// hook forms fail instead of being serialized as an untrusted capability.
pub fn codex_config(
    doc: &mut toml::Table,
    hooks: &Value,
    home: &Path,
    plugins: &Installed,
) -> Result<()> {
    let mut all = hooks.clone();
    let mut state = serde_json::Map::new();
    for (key, digest) in &plugins.trust {
        state.insert(key.clone(), json!({"trusted_hash":digest}));
    }
    for (event, groups) in hooks
        .as_object()
        .ok_or_else(|| invalid("invalid generated hooks"))?
    {
        let label = event_label(event)?;
        for (gi, g) in groups
            .as_array()
            .expect("generated array")
            .iter()
            .enumerate()
        {
            let command = &g["hooks"][0];
            let hash = digest(label, g.get("matcher").and_then(Value::as_str), command)?;
            state.insert(
                format!("{}:{label}:{gi}:0", home.join("config.toml").display()),
                json!({"trusted_hash":hash}),
            );
        }
    }
    if !state.is_empty() {
        all["state"] = Value::Object(state);
    }
    if all.as_object().is_some_and(|hooks| !hooks.is_empty()) {
        let t = toml::Value::try_from(all).map_err(|_| invalid("cannot encode native hooks"))?;
        doc.insert("hooks".into(), t);
    }
    let mut enabled = toml::Table::new();
    for name in &plugins.codex_names {
        let mut t = toml::Table::new();
        t.insert("enabled".into(), toml::Value::Boolean(true));
        enabled.insert(format!("{name}@personal"), toml::Value::Table(t));
    }
    if !enabled.is_empty() {
        doc.insert("plugins".into(), toml::Value::Table(enabled));
    }
    Ok(())
}
