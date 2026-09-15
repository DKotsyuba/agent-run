//! Codex configuration rendering that does not require persistence access.
use agent_run_config::config::Runtime;
use agent_run_domain::{error::invalid, Result};
use agent_run_platform::fs;
use serde_json::json;
use std::path::{Path, PathBuf};

/// Reads an administrator-provided Projects policy when one is installed.
fn system_projects() -> Result<Option<toml::Table>> {
    let text = match std::fs::read_to_string("/etc/codex/requirements.toml") {
        Ok(text) => text,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error.into()),
    };
    let document: toml::Value =
        toml::from_str(&text).map_err(|_| invalid("invalid system Codex permissions"))?;
    match document
        .get("permissions")
        .and_then(|value| value.get("Projects"))
    {
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
        .and_then(|value| value.get("enabled"))
        .and_then(toml::Value::as_bool)
        .unwrap_or(false);
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
) -> Result<()> {
    let Some(root) = &runtime.workspace_root else {
        return Ok(());
    };
    if let Some(system) = system_projects()? {
        managed_roots(runtime, &system)?;
        document.insert(
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
    writes.insert(":workspace_roots".into(), json!({".":"write","**/.env":"deny","**/.env.*":"deny","**/*.pem":"deny","**/*.key":"deny"}));
    let projects = json!({"Projects":{"extends":":workspace","workspace_roots":{(root.to_string_lossy().to_string()):true},"filesystem":writes,"network":{"enabled":runtime.workspace_network}}});
    document.insert(
        "default_permissions".into(),
        toml::Value::String("Projects".into()),
    );
    document.insert(
        "permissions".into(),
        toml::Value::try_from(projects)
            .map_err(|_| invalid("cannot encode Projects permissions"))?,
    );
    Ok(())
}
