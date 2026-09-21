//! Python-v1 effective-configuration snapshot documents.
//!
//! Attempt records bind this canonical document's digest to a verified runtime
//! index. The document intentionally records hashes of environment values,
//! never the values themselves.

use crate::{
    config::{Config, Runtime},
    role_plan::ResolvedRolePlan,
};
use agent_run_domain::{canonical, error::invalid, Result};
use agent_run_platform::{fs, snapshot_tree};
use serde_json::{json, Map, Value};
use std::path::Path;

/// Python's attempt-relative effective configuration evidence filename.
pub const CONFIG_SNAPSHOT_FILENAME: &str = "config-snapshot.json";

/// Exact bytes and bindings of one Python-v1 configuration snapshot.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConfigSnapshot {
    /// Canonical JSON bytes, including the required trailing newline.
    pub document: Vec<u8>,
    /// Lowercase SHA-256 of `document`.
    pub sha256: String,
    /// SHA-256 of the finalized Python-v1 runtime index.
    pub snapshot_index_sha256: String,
    /// Adapter materialization revision carried by that index.
    pub materialize_revision: String,
    /// Observed native version, or no observation.
    pub runtime_version: Option<String>,
}

fn canonical(value: &Value) -> Vec<u8> {
    let mut bytes = canonical::dumps(value, true);
    bytes.push(b'\n');
    bytes
}

fn runtime_document(config: &Config, runtime: &Runtime) -> Result<Value> {
    let auth = match &runtime.auth {
        None => Value::Null,
        Some(crate::config::Auth::Environment { names }) => {
            json!({"kind": "environment", "source": null, "target": null, "names": names})
        }
        Some(crate::config::Auth::FileLink { source, target }) => {
            json!({"kind": "file_link", "source": source, "target": target, "names": []})
        }
    };
    let environment = match runtime
        .environment
        .as_ref()
        .and_then(|name| config.environments.get(name))
    {
        None => Value::Null,
        Some(environment) => json!({
            "path": environment.path,
            "variable_sha256": environment.variables.iter().map(|(name, value)| (name.clone(), Value::String(fs::sha256(value.as_bytes())))).collect::<Map<_, _>>(),
            "required_commands": environment.required_commands,
            "denied_commands": environment.denied_commands,
            "rust": environment.rust.as_ref().map(|roots| json!([roots.rustup_home, roots.cargo_bin])),
        }),
    };
    let mut document = json!({
        "enabled": runtime.enabled,
        "adapter": runtime.adapter,
        "binary": runtime.binary,
        "home": runtime.home,
        "models": runtime.models,
        "skills": runtime.skills,
        "mcp": runtime.mcp,
        "max_active_agents": runtime.max_active_agents,
        "auth": auth,
        "hooks": runtime.hooks.iter().map(|hook| json!([hook.event, hook.command, hook.matcher])).collect::<Vec<_>>(),
        "plugins": runtime.plugins,
        "plugin_snapshot_assets": runtime.plugin_snapshot_assets,
        "limits_source": runtime.limits_source,
        "accounts": runtime.accounts,
        "default_account": runtime.default_account,
        "priority_multiplier": runtime.priority_multiplier,
        "priority_account_multipliers": runtime.priority_account_multipliers,
        "priority_lane_multipliers": runtime.priority_lane_multipliers,
        "rust": runtime.rust.as_ref().map(|roots| json!([roots.rustup_home, roots.cargo_bin])),
        "environment": environment,
    });
    if !runtime.native_settings.is_empty() {
        document["native_settings"] = serde_json::to_value(&runtime.native_settings)?;
    }
    if !runtime.workspace_roots.is_empty() {
        document["workspace_roots"] = json!(runtime.workspace_roots);
    }
    if runtime.workspace_network {
        document["workspace_network"] = Value::Bool(true);
    }
    Ok(document)
}

/// Build Python-v1 effective configuration evidence without persisting secrets.
#[allow(clippy::too_many_arguments)] // Mirrors Python's explicit snapshot builder contract.
pub fn build_config_snapshot(
    runtime_name: &str,
    adapter_api_version: u32,
    schema_version: u32,
    materialize_revision: &str,
    snapshot_index_sha256: &str,
    config: &Config,
    runtime: &Runtime,
    profile: &ResolvedRolePlan,
    runtime_version: Option<&str>,
) -> Result<ConfigSnapshot> {
    if runtime_name.trim().is_empty()
        || adapter_api_version == 0
        || schema_version == 0
        || materialize_revision.trim().is_empty()
        || !snapshot_tree::is_sha256(snapshot_index_sha256)
        || runtime_version.is_some_and(|value| value.trim().is_empty())
    {
        return Err(invalid("config snapshot inputs are invalid"));
    }
    let runtime_config = runtime_document(config, runtime)?;
    let runtime_bytes = canonical::dumps(&runtime_config, true);
    let document = json!({
        "snapshot_version": 1,
        "runtime": runtime_name,
        "runtime_version": runtime_version,
        "adapter_api_version": adapter_api_version,
        "config_schema_version": schema_version,
        "runtime_config_sha256": fs::sha256(&runtime_bytes),
        "runtime_config": runtime_config,
        "materialize_revision": materialize_revision,
        "snapshot_index_sha256": snapshot_index_sha256,
        "profile": profile.to_payload(),
    });
    let document = canonical(&document);
    if document.len() > 64 * 1024 {
        return Err(invalid("config snapshot exceeds the metadata bound"));
    }
    Ok(ConfigSnapshot {
        sha256: fs::sha256(&document),
        document,
        snapshot_index_sha256: snapshot_index_sha256.into(),
        materialize_revision: materialize_revision.into(),
        runtime_version: runtime_version.map(str::to_owned),
    })
}

/// Durably write one configuration snapshot as Python's managed file does.
pub fn write_config_snapshot(directory: &Path, snapshot: &ConfigSnapshot) -> Result<()> {
    agent_run_platform::publish::publish_group(
        &fs::Dir::open(directory)?,
        &[agent_run_platform::publish::Entry::new(
            Path::new(CONFIG_SNAPSHOT_FILENAME),
            &snapshot.document,
            0o600,
        )],
    )
}

/// Read and validate a persisted Python-v1 configuration snapshot by digest.
pub fn inspect_config_snapshot(directory: &Path, expected_sha256: &str) -> Result<ConfigSnapshot> {
    if !snapshot_tree::is_sha256(expected_sha256) {
        return Err(invalid("config snapshot sha256 is invalid"));
    }
    let raw = fs::Dir::open(directory)?
        .optional(Path::new(CONFIG_SNAPSHOT_FILENAME), 64 * 1024)?
        .ok_or_else(|| invalid("config snapshot is missing"))?;
    if fs::sha256(&raw) != expected_sha256 {
        return Err(invalid(
            "config snapshot hash does not match its recorded revision",
        ));
    }
    let document: Value =
        serde_json::from_slice(&raw).map_err(|_| invalid("config snapshot is malformed"))?;
    let required: std::collections::BTreeSet<_> = [
        "snapshot_version",
        "runtime",
        "runtime_version",
        "adapter_api_version",
        "config_schema_version",
        "runtime_config_sha256",
        "runtime_config",
        "materialize_revision",
        "snapshot_index_sha256",
        "profile",
    ]
    .into_iter()
    .collect();
    if document
        .as_object()
        .map(|value| value.keys().map(String::as_str).collect())
        != Some(required)
        || document["snapshot_version"] != 1
        || raw != canonical(&document)
    {
        return Err(invalid("config snapshot shape is unsupported"));
    }
    let runtime_config = document["runtime_config"]
        .as_object()
        .ok_or_else(|| invalid("config snapshot runtime declaration is invalid"))?;
    if document["runtime_config_sha256"]
        != fs::sha256(&canonical::dumps(
            &Value::Object(runtime_config.clone()),
            true,
        ))
        || !document["snapshot_index_sha256"]
            .as_str()
            .is_some_and(snapshot_tree::is_sha256)
    {
        return Err(invalid(
            "config snapshot runtime declaration hash is invalid",
        ));
    }
    let materialize_revision = document["materialize_revision"]
        .as_str()
        .filter(|value| !value.trim().is_empty())
        .ok_or_else(|| invalid("config snapshot materialize revision is invalid"))?
        .to_owned();
    let runtime_version = match &document["runtime_version"] {
        Value::Null => None,
        Value::String(value) if !value.trim().is_empty() => Some(value.clone()),
        _ => return Err(invalid("config snapshot runtime version is invalid")),
    };
    Ok(ConfigSnapshot {
        document: raw,
        sha256: expected_sha256.into(),
        snapshot_index_sha256: document["snapshot_index_sha256"]
            .as_str()
            .expect("validated index hash")
            .into(),
        materialize_revision,
        runtime_version,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Builds one codex runtime fixture declaring the given workspace roots.
    fn workspace_runtime(roots: Value) -> Runtime {
        serde_json::from_value(json!({
            "enabled": true,
            "adapter": "codex",
            "binary": "/bin/true",
            "home": "/tmp/agent-run-snapshot-test",
            "models": ["fixture"],
            "workspace_roots": roots,
        }))
        .expect("fixture runtime")
    }

    /// Legacy singular and plural workspace declarations both serialize the
    /// effective runtime configuration with the plural `workspace_roots` key
    /// only, and the plural form retains every configured root.
    #[test]
    fn runtime_config_exposes_plural_workspace_roots_only() {
        let config: Config = toml::from_str("schema_version = 1").expect("minimal config");

        let singular_runtime: Runtime = serde_json::from_value(json!({
            "enabled": true,
            "adapter": "codex",
            "binary": "/bin/true",
            "home": "/tmp/agent-run-snapshot-test",
            "models": ["fixture"],
            "workspace_root": "/workspace/a",
        }))
        .expect("singular fixture runtime");
        let singular = runtime_document(&config, &singular_runtime).expect("singular document");
        assert!(singular.get("workspace_root").is_none());
        assert_eq!(singular["workspace_roots"], json!(["/workspace/a"]));

        let plural = runtime_document(
            &config,
            &workspace_runtime(json!(["/workspace/a", "/workspace/b"])),
        )
        .expect("plural document");
        assert!(plural.get("workspace_root").is_none());
        assert_eq!(
            plural["workspace_roots"],
            json!(["/workspace/a", "/workspace/b"])
        );
    }
}
