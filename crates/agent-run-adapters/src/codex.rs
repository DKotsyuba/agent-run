//! Codex configuration rendering that does not require persistence access.
/// Bounded parsing of Codex rollout rate-limit evidence.
pub mod limits;
/// Isolated Codex model-roster cache parsing and publication.
pub mod models;
pub mod session;
use agent_run_config::config::Runtime;
use agent_run_domain::{error::invalid, Error, Result};
use agent_run_platform::fs;
use agent_run_platform::shared_asset_guard::SharedAssetGuard;
use agent_run_platform::shared_assets::SharedStoreLock;
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
///
/// Every configured `workspace_roots` entry must be explicitly granted by the
/// system table's `workspace_roots` map, `extends` must select `:workspace`,
/// and the network switch must agree exactly with `workspace_network`. Any
/// ungranted root, malformed table, or network disagreement fails closed.
fn managed_roots(runtime: &Runtime, system: &toml::Table) -> Result<Vec<String>> {
    let roots = &runtime.workspace_roots;
    if roots.is_empty() {
        return Err(invalid("managed Projects requires workspace_roots"));
    }
    let table = system
        .get("workspace_roots")
        .and_then(toml::Value::as_table)
        .ok_or_else(|| invalid("invalid managed workspace roots"))?;
    if !roots.iter().all(|root| {
        table
            .get(root.to_string_lossy().as_ref())
            .and_then(toml::Value::as_bool)
            == Some(true)
    }) || system.get("extends").and_then(toml::Value::as_str) != Some(":workspace")
    {
        return Err(invalid(
            "workspace_roots are not granted by managed Projects",
        ));
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

/// Renders the immutable Codex Projects policy for the configured workspace
/// roots.
///
/// Without configured roots the document is left untouched. With a managed
/// system Projects policy every configured root must be granted and only the
/// native `default_permissions` selector is emitted. Otherwise the generated
/// `Projects` profile grants every configured root plus the runtime caches,
/// denies shell access to the auth bridge target when given, and records
/// `workspace_network`.
pub fn render_permissions(
    document: &mut toml::Table,
    runtime: &Runtime,
    home: &Path,
    auth_target: Option<&str>,
) -> Result<()> {
    if runtime.workspace_roots.is_empty() {
        return Ok(());
    }
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
    let projects = projects_profile(runtime, writes);
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

/// Builds the generated `Projects` profile payload granting every configured
/// workspace root, `writes` filesystem rules, and the declared network state.
fn projects_profile(runtime: &Runtime, writes: serde_json::Map<String, Value>) -> Value {
    let granted: serde_json::Map<String, Value> = runtime
        .workspace_roots
        .iter()
        .map(|root| (root.to_string_lossy().into_owned(), json!(true)))
        .collect();
    json!({PROJECTS_PROFILE:{"extends":":workspace","workspace_roots":granted,"filesystem":writes,"network":{"enabled":runtime.workspace_network}}})
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

/// One harness-owned stdio MCP server exactly as the sealed native config
/// carries it: the launch `command` and its argument vector.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SealedMcpServer {
    /// Program the native harness launches for this server.
    pub command: String,
    /// Exact argument vector following `command`.
    pub args: Vec<String>,
}

/// Compiles native launch-time `-c` overrides wrapping each harness-owned
/// stdio MCP server of a shared-layout launch with the shared-asset guard.
///
/// `store_root` is the trusted canonical shared-store root the layout was
/// installed under; `servers` are the frozen `(command, args)` definitions the
/// sealed native config already carries for this role. The bytes of that
/// config are never rewritten: for every server the compiler emits
/// `-c mcp_servers.<name>.command=<guard helper>` and
/// `-c mcp_servers.<name>.args=[…]`, where the helper and its arguments are
/// [`agent_run_platform::shared_asset_guard::SharedAssetGuard::wrap`] applied
/// to the original program and arguments (`sandbox-exec` on macOS,
/// `bwrap` on Linux, taken from the wrapped argv itself so the override can
/// never name a different program than the guard) — nothing else about the server
/// (environment, approval mode, enabled tools) changes. The dotted override
/// form is the native CLI's own verified contract: a probe against Codex
/// 0.156.1 (`codex mcp get --json` with the same `-c` pairs) reflected the
/// exact guard argv and preserved every other server property with the source
/// config bytes unchanged, while a quoted key segment
/// (`mcp_servers."a.b".command`) silently failed to override. Server names are
/// therefore restricted to bare native key segments — ASCII letters, digits,
/// underscore and hyphen — and any other name is an explicit error, never an
/// invented quoting. Every recorded command is wrapped, including a command
/// that itself names the guard helper: its recorded arguments are not proof of
/// a safe profile. An unavailable guard is an
/// error, never a silent private fallback: the caller must refuse the shared
/// launch instead. One store publish/GC lock covers every wrapper scan, so
/// concurrent imports cannot alter inode counts between checks.
pub fn shared_guard_mcp_overrides(
    store_root: &Path,
    servers: &std::collections::BTreeMap<String, SealedMcpServer>,
) -> Result<Vec<String>> {
    let _publish_lock = SharedStoreLock::acquire(store_root)?;
    let guard =
        SharedAssetGuard::new(store_root).map_err(|error| Error::Unsupported(error.to_string()))?;
    let mut overrides = Vec::new();
    for (name, server) in servers {
        if name.is_empty()
            || !name
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_' || byte == b'-')
        {
            return Err(invalid(format!(
                "MCP server name {name:?} is not a bare native key segment; rename the server \
                 before enabling a shared layout"
            )));
        }
        let argv = guard
            .wrap(Path::new(&server.command), &server.args)
            .map_err(|error| Error::Unsupported(error.to_string()))?;
        let helper = argv[0].to_string_lossy();
        let args = argv[1..]
            .iter()
            .map(|value| value.to_string_lossy().into_owned())
            .collect::<Vec<_>>();
        overrides.push("-c".into());
        overrides.push(format!("mcp_servers.{name}.command={helper}"));
        overrides.push("-c".into());
        overrides.push(format!(
            "mcp_servers.{name}.args=[{}]",
            args.iter()
                .map(|value| serde_json::to_string(value)
                    .map_err(|_| invalid("MCP override argument is not encodable")))
                .collect::<Result<Vec<_>>>()?
                .join(",")
        ));
    }
    Ok(overrides)
}

/// Exercises managed Projects policy validation without depending on host policy files.
#[cfg(test)]
mod tests {
    use super::*;

    /// The platform's fixed guard helper, or `None` when this host lacks it
    /// and every shared wrapper must therefore be refused.
    #[cfg(any(target_os = "macos", target_os = "linux"))]
    fn guard_helper() -> Option<&'static str> {
        #[cfg(target_os = "macos")]
        let helper = agent_run_platform::shared_asset_guard::MACOS_SANDBOX_EXEC;
        #[cfg(target_os = "linux")]
        let helper = agent_run_platform::shared_asset_guard::LINUX_BWRAP;
        Path::new(helper).is_file().then_some(helper)
    }

    /// Shared MCP overrides guard the original command and exact arguments
    /// under bare native keys with the platform helper as the command;
    /// ambiguous keys are refused before launch and a host without the
    /// helper refuses instead of emitting an unguarded override.
    #[cfg(any(target_os = "macos", target_os = "linux"))]
    #[test]
    fn shared_mcp_overrides_preserve_server_argv() {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().canonicalize().unwrap();
        let Some(helper) = guard_helper() else {
            let servers = std::collections::BTreeMap::from([(
                "worker".to_owned(),
                SealedMcpServer {
                    command: "/bin/echo".into(),
                    args: vec![],
                },
            )]);
            assert!(matches!(
                shared_guard_mcp_overrides(&root, &servers),
                Err(Error::Unsupported(_))
            ));
            return;
        };
        let servers = std::collections::BTreeMap::from([(
            "worker-probe".to_owned(),
            SealedMcpServer {
                command: "/bin/echo".into(),
                args: vec!["a b".into(), "quoted\"value".into()],
            },
        )]);
        let overrides = shared_guard_mcp_overrides(&root, &servers).unwrap();
        assert_eq!(overrides.len(), 4);
        assert_eq!(overrides[0], "-c");
        assert_eq!(
            overrides[1],
            format!("mcp_servers.worker-probe.command={helper}")
        );
        let args: Vec<String> = serde_json::from_str(
            overrides[3]
                .strip_prefix("mcp_servers.worker-probe.args=")
                .unwrap(),
        )
        .unwrap();
        assert!(args
            .windows(3)
            .any(|window| { window == ["/bin/echo", "a b", "quoted\"value"] }));
        let ambiguous = std::collections::BTreeMap::from([(
            "worker.probe".to_owned(),
            servers["worker-probe"].clone(),
        )]);
        assert!(shared_guard_mcp_overrides(&root, &ambiguous).is_err());
        let self_named = std::collections::BTreeMap::from([(
            "worker".to_owned(),
            SealedMcpServer {
                command: helper.into(),
                args: vec!["--version".into()],
            },
        )]);
        let nested = shared_guard_mcp_overrides(&root, &self_named).unwrap();
        let nested_args: Vec<String> =
            serde_json::from_str(nested[3].strip_prefix("mcp_servers.worker.args=").unwrap())
                .unwrap();
        assert_eq!(nested[1], format!("mcp_servers.worker.command={helper}"));
        assert!(nested_args
            .windows(2)
            .any(|window| { window == [helper, "--version"] }));
    }

    /// A launch waits for an in-flight publisher to finish creating both
    /// internal hardlinks before its whole-store alias scan begins; the
    /// finished scan succeeds exactly when the host has the guard helper.
    #[cfg(any(target_os = "macos", target_os = "linux"))]
    #[test]
    fn shared_mcp_scan_waits_for_store_publisher() {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().canonicalize().unwrap();
        let publisher = SharedStoreLock::acquire(&root).unwrap();
        let (started_tx, started_rx) = std::sync::mpsc::channel();
        let (done_tx, done_rx) = std::sync::mpsc::channel();
        let other_root = root.clone();
        let handle = std::thread::spawn(move || {
            started_tx.send(()).unwrap();
            done_tx
                .send(shared_guard_mcp_overrides(
                    &other_root,
                    &std::collections::BTreeMap::from([(
                        "worker".into(),
                        SealedMcpServer {
                            command: "/bin/true".into(),
                            args: vec![],
                        },
                    )]),
                ))
                .unwrap();
        });
        started_rx.recv().unwrap();
        std::fs::write(root.join("first"), b"asset").unwrap();
        std::fs::hard_link(root.join("first"), root.join("second")).unwrap();
        assert!(matches!(
            done_rx.recv_timeout(std::time::Duration::from_millis(50)),
            Err(std::sync::mpsc::RecvTimeoutError::Timeout)
        ));
        drop(publisher);
        assert_eq!(
            done_rx
                .recv_timeout(std::time::Duration::from_secs(2))
                .unwrap()
                .is_ok(),
            guard_helper().is_some()
        );
        handle.join().unwrap();
    }

    /// Mirrors `test_codex_adapter.py::test_managed_projects_uses_one_definition_and_verifies_all_write_roots` refusal behavior.
    ///
    /// The singular fixture key also proves the legacy `workspace_root`
    /// declaration still parses into the normalized plural contract.
    #[test]
    fn python_test_codex_adapter_projects_refuses_an_unproven_managed_root() {
        let runtime: Runtime = serde_json::from_value(json!({
            "enabled": true,
            "adapter": "codex",
            "binary": "/bin/true",
            "home": "/tmp/agent-run-codex-test",
            "models": ["fixture"],
            "workspace_root": "/workspace/unproven",
        }))
        .expect("fixture runtime");
        let policy: toml::Value = toml::from_str(
            r#"
extends = ":workspace"
workspace_roots = { "/workspace/granted" = true }
network = { enabled = false }
"#,
        )
        .expect("fixture policy");

        let error = managed_roots(&runtime, policy.as_table().expect("Projects policy table"))
            .expect_err("unproven workspace root must be refused");
        assert!(error
            .to_string()
            .contains("workspace_roots are not granted by managed Projects"));
    }

    /// Managed Projects validation requires every configured root to be
    /// granted, and returns the granted roots sorted and deduplicated.
    #[test]
    fn managed_projects_verifies_every_configured_workspace_root() {
        let runtime: Runtime = serde_json::from_value(json!({
            "enabled": true,
            "adapter": "codex",
            "binary": "/bin/true",
            "home": "/tmp/agent-run-codex-test",
            "models": ["fixture"],
            "workspace_roots": ["/workspace/a", "/workspace/unproven"],
        }))
        .expect("fixture runtime");
        let policy: toml::Value = toml::from_str(
            r#"
extends = ":workspace"
workspace_roots = { "/workspace/a" = true, "/workspace/b" = true }
network = { enabled = false }
"#,
        )
        .expect("fixture policy");
        let table = policy.as_table().expect("Projects policy table");

        let error = managed_roots(&runtime, table)
            .expect_err("one ungranted root must refuse the whole policy");
        assert!(error
            .to_string()
            .contains("workspace_roots are not granted by managed Projects"));

        let runtime: Runtime = serde_json::from_value(json!({
            "enabled": true,
            "adapter": "codex",
            "binary": "/bin/true",
            "home": "/tmp/agent-run-codex-test",
            "models": ["fixture"],
            "workspace_roots": ["/workspace/b", "/workspace/a"],
        }))
        .expect("fixture runtime");
        assert_eq!(
            managed_roots(&runtime, table).expect("all roots granted"),
            vec!["/workspace/a", "/workspace/b"]
        );
    }

    /// The generated unmanaged Projects profile grants every configured root.
    #[test]
    fn generated_projects_permissions_include_every_workspace_root() {
        let runtime: Runtime = serde_json::from_value(json!({
            "enabled": true,
            "adapter": "codex",
            "binary": "/bin/true",
            "home": "/tmp/agent-run-codex-test",
            "models": ["fixture"],
            "workspace_roots": ["/workspace/a", "/workspace/b"],
        }))
        .expect("fixture runtime");

        let projects = projects_profile(&runtime, serde_json::Map::new());
        let roots = projects[PROJECTS_PROFILE]["workspace_roots"]
            .as_object()
            .expect("roots map");
        assert_eq!(roots.len(), 2);
        assert_eq!(roots["/workspace/a"].as_bool(), Some(true));
        assert_eq!(roots["/workspace/b"].as_bool(), Some(true));
        assert_eq!(
            projects[PROJECTS_PROFILE]["extends"].as_str(),
            Some(":workspace")
        );
    }
}
