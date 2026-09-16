//! Codex configuration rendering that does not require persistence access.
/// Bounded parsing of Codex rollout rate-limit evidence.
pub mod limits;
/// Isolated Codex model-roster cache parsing and publication.
pub mod models;
pub mod session;
use agent_run_config::config::Runtime;
use agent_run_domain::{error::invalid, Result};
use agent_run_platform::fs;
use serde_json::{json, Value};
use std::{
    collections::BTreeSet,
    path::{Path, PathBuf},
};

/// The native profile name that binds an admitted workspace and cache grants.
pub const PROJECTS_PROFILE: &str = "Projects";

/// Renders one validated native setting as a TOML inline value.
///
/// Tables remain inline so later agent-run-owned sections cannot be captured by
/// an operator's native table. Invalid dates and non-finite floats are rejected
/// even for programmatically constructed runtimes that bypass config loading.
pub fn inline_toml_value(value: &toml::Value) -> Result<String> {
    match value {
        toml::Value::String(value) => Ok(serde_json::to_string(value)?),
        toml::Value::Integer(value) => Ok(value.to_string()),
        toml::Value::Float(value) if value.is_finite() => Ok(value.to_string()),
        toml::Value::Float(_) => Err(invalid("native settings floats must be finite")),
        toml::Value::Boolean(value) => Ok(value.to_string()),
        toml::Value::Datetime(_) => Err(invalid("native settings do not accept dates")),
        toml::Value::Array(values) => values
            .iter()
            .map(inline_toml_value)
            .collect::<Result<Vec<_>>>()
            .map(|values| format!("[{}]", values.join(", "))),
        toml::Value::Table(values) => values
            .iter()
            .map(|(key, value)| Ok(format!("{key} = {}", inline_toml_value(value)?)))
            .collect::<Result<Vec<_>>>()
            .map(|values| format!("{{ {} }}", values.join(", "))),
    }
}

/// Reads an administrator-provided Projects policy when one is installed.
fn system_projects() -> Result<Option<toml::Table>> {
    let text = match std::fs::read_to_string("/etc/codex/requirements.toml") {
        Ok(text) => text,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error.into()),
    };
    let document: toml::Value =
        toml::from_str(&text).map_err(|_| invalid("invalid system Codex permissions"))?;
    let Some(permissions) = document.get("permissions") else {
        return Ok(None);
    };
    let permissions = permissions
        .as_table()
        .ok_or_else(|| invalid("invalid system Codex permissions"))?;
    match permissions.get(PROJECTS_PROFILE) {
        None => Ok(None),
        Some(table) => table
            .as_table()
            .cloned()
            .map(Some)
            .ok_or_else(|| invalid("invalid system Projects profile")),
    }
}

/// Returns the host cache locations which a generated Projects policy may write.
fn caches(home: &Path) -> Vec<PathBuf> {
    [
        ".cache/uv",
        ".cargo/registry",
        ".npm",
        "Library/Caches/go-build",
        "Library/Caches/pip",
    ]
    .iter()
    .map(|path| home.join(path))
    .collect()
}

/// Validates an installed Projects policy and returns its permitted roots.
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
        .and_then(toml::Value::as_table)
        .and_then(|value| value.get("enabled"))
        .and_then(toml::Value::as_bool)
        .ok_or_else(|| invalid("invalid managed Projects network policy"))?;
    if network != runtime.workspace_network {
        return Err(invalid("workspace_network differs from managed Projects"));
    }
    let mut roots = Vec::new();
    for (path, value) in table {
        if value.as_bool() == Some(true) {
            roots.push(fs::expand(Path::new(path))?.to_string_lossy().into_owned());
        }
    }
    if let Some(filesystem) = system.get("filesystem").and_then(toml::Value::as_table) {
        for (path, value) in filesystem {
            if !path.starts_with(':') && value.as_str() == Some("write") {
                roots.push(fs::expand(Path::new(path))?.to_string_lossy().into_owned());
            }
        }
    }
    roots.sort();
    roots.dedup();
    Ok(roots)
}

/// Renders the immutable Codex Projects policy for a configured workspace.
pub fn render_permissions(
    document: &mut toml::Table,
    runtime: &Runtime,
    home: &Path,
    auth_target: Option<&str>,
) -> Result<()> {
    let Some(root) = &runtime.workspace_root else {
        return Ok(());
    };
    if let Some(system) = system_projects()? {
        managed_roots(runtime, &system)?;
        document.insert(
            "default_permissions".into(),
            toml::Value::String(PROJECTS_PROFILE.into()),
        );
        return Ok(());
    }
    let mut writes = serde_json::Map::new();
    for path in caches(home) {
        writes.insert(path.to_string_lossy().into_owned(), json!("write"));
    }
    if let Some(target) = auth_target {
        writes.insert(
            home.join(target).to_string_lossy().into_owned(),
            json!("deny"),
        );
    }
    writes.insert(":workspace_roots".into(), json!({".":"write","**/.env":"deny","**/.env.*":"deny","**/*.pem":"deny","**/*.key":"deny"}));
    let projects = json!({PROJECTS_PROFILE:{"extends":":workspace","workspace_roots":{(root.to_string_lossy().to_string()):true},"filesystem":writes,"network":{"enabled":runtime.workspace_network}}});
    document.insert(
        "default_permissions".into(),
        toml::Value::String(PROJECTS_PROFILE.into()),
    );
    document.insert(
        "permissions".into(),
        toml::Value::try_from(projects)
            .map_err(|_| invalid("cannot encode Projects permissions"))?,
    );
    Ok(())
}

/// Returns whether a PermissionRequest targets exactly one trusted MCP namespace.
///
/// Hyphenated Codex server names may be echoed with underscores. Every other
/// event, malformed payload, shell tool, and unknown server returns `false` so
/// Codex retains its normal approval flow.
pub fn allows_permission_request(payload: &Value, trusted_servers: &BTreeSet<String>) -> bool {
    let Some(event) = payload.get("hook_event_name").and_then(Value::as_str) else {
        return false;
    };
    let Some(tool) = payload.get("tool_name").and_then(Value::as_str) else {
        return false;
    };
    event == "PermissionRequest"
        && trusted_servers.iter().any(|server| {
            tool.starts_with(&format!("mcp__{server}__"))
                || tool.starts_with(&format!("mcp__{}__", server.replace('-', "_")))
        })
}

/// Produces the narrow native allow reply for a proven trusted MCP request.
///
/// `None` deliberately writes no decision, leaving residual work to Codex's
/// ordinary reviewer rather than widening the agent's authority.
pub fn permission_request_decision(
    payload: &Value,
    trusted_servers: &BTreeSet<String>,
) -> Option<Value> {
    allows_permission_request(payload, trusted_servers).then(|| {
        json!({"hookSpecificOutput":{"hookEventName":"PermissionRequest","decision":{"behavior":"allow"}}})
    })
}

/// Builds the generated PermissionRequest hook arguments for trusted MCPs.
///
/// The current agent-run executable owns the stdin/stdout hook protocol. Only
/// lowercase MCP identifiers with Codex's hyphen/underscore normalization are
/// admitted; an invalid trusted declaration fails materialization rather than
/// creating a broad matcher.
pub fn permission_request_hook(
    trusted_servers: &BTreeSet<String>,
    executable: &Path,
) -> Result<Option<(String, Vec<String>)>> {
    if trusted_servers.is_empty() {
        return Ok(None);
    }
    if !executable.is_absolute() {
        return Err(invalid("permission request executable must be absolute"));
    }
    if trusted_servers.iter().any(|server| {
        server.is_empty()
            || !server.bytes().enumerate().all(|(index, byte)| {
                (index == 0 && byte.is_ascii_lowercase())
                    || byte.is_ascii_lowercase()
                    || byte.is_ascii_digit()
                    || matches!(byte, b'-' | b'_')
            })
    }) {
        return Err(invalid(
            "trusted Codex MCP names must be lowercase identifiers",
        ));
    }
    let variants = trusted_servers
        .iter()
        .flat_map(|server| [server.clone(), server.replace('-', "_")])
        .collect::<BTreeSet<_>>();
    let matcher = format!(
        "^(?:{})",
        variants
            .iter()
            .map(|name| format!("mcp__{name}__"))
            .collect::<Vec<_>>()
            .join("|")
    );
    let mut command = vec![
        executable.to_string_lossy().into_owned(),
        "_permission-request".into(),
    ];
    for server in trusted_servers {
        command.extend(["--allow-mcp".into(), server.clone()]);
    }
    Ok(Some((matcher, command)))
}

/// Renders deterministic forbidden-prefix rules for denied command names.
///
/// The result covers bare names, first executable PATH entries, and resolved
/// symlink targets. It is an approval rule plus the generated PATH shim, not
/// an operating-system confinement guarantee.
pub fn render_denial_rules(commands: &[String], path: &str) -> String {
    render_command_rules(
        commands,
        path,
        "forbidden",
        "Denied by owner command policy",
    )
}

/// Renders deterministic prompt-prefix rules for review-only command names.
pub fn render_review_rules(commands: &[String], path: &str) -> String {
    render_command_rules(
        commands,
        path,
        "prompt",
        "Review network command before execution",
    )
}

/// Builds one Codex native rule line per bare, resolved, and canonical command form.
fn render_command_rules(
    commands: &[String],
    path: &str,
    decision: &str,
    justification: &str,
) -> String {
    let mut patterns: BTreeSet<String> = commands.iter().cloned().collect();
    for command in commands {
        if let Some(candidate) = resolve_command(command, path) {
            patterns.insert(candidate.to_string_lossy().into_owned());
            if let Ok(target) = candidate.canonicalize() {
                patterns.insert(target.to_string_lossy().into_owned());
            }
        }
    }
    patterns
        .into_iter()
        .map(|pattern| format!(
            "prefix_rule(pattern=[{}], decision=\"{decision}\", justification=\"{justification}\")\n",
            serde_json::to_string(&pattern).expect("string JSON serialization")
        ))
        .collect()
}

/// Finds the first executable command in absolute PATH entries only.
fn resolve_command(command: &str, path: &str) -> Option<PathBuf> {
    use std::os::unix::fs::PermissionsExt;

    path.split(':').map(Path::new).find_map(|directory| {
        let candidate = directory.join(command);
        (directory.is_absolute()
            && std::fs::metadata(&candidate)
                .map(|metadata| metadata.is_file() && metadata.permissions().mode() & 0o111 != 0)
                .unwrap_or(false))
        .then_some(candidate)
    })
}
