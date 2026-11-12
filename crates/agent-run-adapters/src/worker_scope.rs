//! Scopes Agent Run's private launch variables away from agent tool processes.
//!
//! The worker capability (`AGENT_RUN_WORKER_*`) belongs only to the worker MCP
//! child, and the system writer/operator sockets (`AGENT_RUN_SYSTEM_*`) belong
//! only to the supervisor. Neither may reach a shell, test or child CLI the
//! agent spawns, where it could accidentally act on the live Run.
//!
//! - Codex: the harness keeps the capability because its MCP children receive
//!   only the explicitly forwarded `env_vars`; the generated
//!   `shell_environment_policy.exclude` removes every `AGENT_RUN_*` name from
//!   tool shells ([`CODEX_SHELL_EXCLUDE`](crate::worker_scope::CODEX_SHELL_EXCLUDE)).
//! - Claude: Claude Code passes its whole environment to Bash and every other
//!   child, so
//!   [`scope_claude_worker_mcp`](crate::worker_scope::scope_claude_worker_mcp)
//!   removes the capability from the harness environment and writes it only
//!   into the worker MCP entry of a private per-attempt copy of the sealed
//!   `--mcp-config`.
//!
//! `AGENT_RUN_LAUNCH_ID` stays in every harness environment: it is a
//! non-secret correlation marker that Bridge reads from the harness process
//! itself and that Run and Bridge ancestry checks treat as "inside a managed
//! launch". It grants no authority.
//!
//! This is protection against accidental inheritance, not a security boundary
//! against a same-UID agent that deliberately reads `/proc/<pid>/environ` or
//! the private per-attempt file.

use crate::LaunchPlan;
use agent_run_domain::{Result, error::invalid};
use serde_json::Value;
use std::{
    collections::BTreeMap,
    path::{Path, PathBuf},
};

/// Prefix of the attempt-bound worker capability and its catalog selectors.
pub const WORKER_PREFIX: &str = "AGENT_RUN_WORKER_";
/// Prefix of the supervisor-only system writer and operator sockets.
pub const SYSTEM_PREFIX: &str = "AGENT_RUN_SYSTEM_";
/// Codex `shell_environment_policy.exclude` patterns written into every
/// generated Codex config. Codex matches them case-insensitively against tool
/// shell variables; MCP children are configured separately via `env_vars`.
pub const CODEX_SHELL_EXCLUDE: &[&str] = &["AGENT_RUN_*"];
/// Directory, relative to the runtime home and outside the sealed index, that
/// holds the per-attempt Claude MCP configuration.
pub const CLAUDE_ATTEMPT_DIR: &str = "worker-mcp";
/// Upper bound on the sealed Claude MCP configuration read per attempt.
const MCP_CONFIG_LIMIT: u64 = 1024 * 1024;

/// Returns whether `name` is a private Agent Run variable that only the
/// supervisor (system sockets) or the worker MCP (capability) may receive.
pub fn is_private_launch_name(name: &str) -> bool {
    name.starts_with(WORKER_PREFIX) || name.starts_with(SYSTEM_PREFIX)
}

/// The per-attempt Claude MCP configuration that alone carries the worker
/// capability. Dropping it removes the file; the capability values are also
/// kept in memory for the broker's finish guard. No `Debug` is provided.
pub struct ScopedWorkerMcp {
    path: PathBuf,
    environment: BTreeMap<String, String>,
}

impl ScopedWorkerMcp {
    /// Path of the private per-attempt `--mcp-config` file.
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// The worker variables removed from the harness environment.
    pub fn environment(&self) -> &BTreeMap<String, String> {
        &self.environment
    }

    /// Returns `launch` plus the scoped worker variables, for consumers such
    /// as the finish guard that must still recognize every launch secret.
    pub fn with_launch(&self, launch: &BTreeMap<String, String>) -> BTreeMap<String, String> {
        let mut merged = launch.clone();
        merged.extend(self.environment.clone());
        merged
    }
}

impl Drop for ScopedWorkerMcp {
    /// Removes the capability-bearing file once the attempt no longer needs it.
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.path);
    }
}

/// Moves every `AGENT_RUN_WORKER_*` variable out of a Claude launch and into
/// the `env` of the worker MCP entry in a private per-attempt copy of the
/// sealed `--mcp-config`, then points the launch at that copy.
///
/// Returns `None` when the launch carries no worker variables. The copy lives
/// in `<runtime_home>/worker-mcp/` (mode 0700, file 0600) outside the sealed
/// index; any stale copy from an earlier attempt is replaced. Fails when the
/// launch has worker variables but no single `--mcp-config` argument, or the
/// sealed configuration has no worker MCP entry.
pub fn scope_claude_worker_mcp(
    launch: &mut LaunchPlan,
    runtime_home: &Path,
) -> Result<Option<ScopedWorkerMcp>> {
    let names: Vec<String> = launch
        .environment
        .keys()
        .filter(|name| name.starts_with(WORKER_PREFIX))
        .cloned()
        .collect();
    if names.is_empty() {
        return Ok(None);
    }
    let positions: Vec<usize> = launch
        .args
        .iter()
        .enumerate()
        .filter_map(|(index, arg)| (arg == "--mcp-config").then_some(index + 1))
        .collect();
    let [position] = positions[..] else {
        return Err(invalid("worker MCP launch has no single native MCP config"));
    };
    let sealed = PathBuf::from(
        launch
            .args
            .get(position)
            .ok_or_else(|| invalid("worker MCP launch has no native MCP config path"))?,
    );
    let length = std::fs::metadata(&sealed)?.len();
    if length > MCP_CONFIG_LIMIT {
        return Err(invalid("sealed MCP config exceeds its size bound"));
    }
    let mut config: Value = serde_json::from_slice(&std::fs::read(&sealed)?)
        .map_err(|_| invalid("sealed MCP config is malformed"))?;
    let entry = config
        .get_mut("mcpServers")
        .and_then(|servers| servers.get_mut(agent_run_domain::worker::SERVER_NAME))
        .and_then(Value::as_object_mut)
        .ok_or_else(|| invalid("sealed MCP config has no worker MCP entry"))?;
    let mut environment = BTreeMap::new();
    for name in names {
        if let Some(value) = launch.environment.remove(&name) {
            environment.insert(name, value);
        }
    }
    entry.insert(
        "env".into(),
        Value::Object(
            environment
                .iter()
                .map(|(name, value)| (name.clone(), Value::String(value.clone())))
                .collect(),
        ),
    );
    let directory = runtime_home.join(CLAUDE_ATTEMPT_DIR);
    agent_run_platform::fs::private_dir(&directory)?;
    let path = directory.join("mcp-config.json");
    let scoped = ScopedWorkerMcp {
        path: path.clone(),
        environment,
    };
    agent_run_platform::fs::Dir::open(&directory)?.write(
        Path::new("mcp-config.json"),
        &serde_json::to_vec(&config)?,
        0o600,
    )?;
    launch.args[position] = path.to_string_lossy().into_owned();
    Ok(Some(scoped))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    /// Builds a Claude-shaped launch whose sealed config names the worker MCP.
    fn launch(root: &Path) -> LaunchPlan {
        std::fs::create_dir_all(root.join("mcp")).unwrap();
        std::fs::write(
            root.join("mcp/mcp-config.json"),
            serde_json::to_vec(&json!({"mcpServers":{
                "agent_run_worker":{"command":"/bin/agent-run","args":["_worker-mcp"],
                    "env":{"AGENT_RUN_WORKER_TOKEN":"${AGENT_RUN_WORKER_TOKEN}"}},
                "pool":{"command":"/bin/pool","args":[],"env":{"POOL":"${POOL}"}}
            }}))
            .unwrap(),
        )
        .unwrap();
        LaunchPlan {
            binary: "/bin/claude".into(),
            args: vec![
                "--mcp-config".into(),
                root.join("mcp/mcp-config.json").to_string_lossy().into(),
            ],
            cwd: root.into(),
            environment: BTreeMap::from([
                ("AGENT_RUN_WORKER_TOKEN".into(), "a".repeat(64)),
                ("AGENT_RUN_WORKER_FINISH".into(), "1".into()),
                ("AGENT_RUN_LAUNCH_ID".into(), "launch".into()),
                ("BRIDGE_NATIVE_OWNER_CHANNEL".into(), "/bridge".into()),
                ("POOL".into(), "kept".into()),
            ]),
            initial_input: None,
        }
    }

    /// The capability leaves the harness environment and enters only the
    /// worker entry of the per-attempt copy; the sealed file is unchanged and
    /// the copy disappears with its guard.
    #[test]
    fn claude_worker_capability_moves_into_the_attempt_mcp_entry() {
        let root = tempfile::tempdir().unwrap();
        let mut plan = launch(root.path());
        let sealed = std::fs::read(root.path().join("mcp/mcp-config.json")).unwrap();
        let scoped = scope_claude_worker_mcp(&mut plan, root.path())
            .unwrap()
            .unwrap();
        assert!(
            plan.environment
                .keys()
                .all(|name| !name.starts_with(WORKER_PREFIX))
        );
        for kept in ["AGENT_RUN_LAUNCH_ID", "BRIDGE_NATIVE_OWNER_CHANNEL", "POOL"] {
            assert!(plan.environment.contains_key(kept), "{kept}");
        }
        assert_eq!(plan.args[1], scoped.path().to_string_lossy());
        assert_eq!(
            std::fs::read(root.path().join("mcp/mcp-config.json")).unwrap(),
            sealed
        );
        use std::os::unix::fs::PermissionsExt as _;
        let mode = std::fs::metadata(scoped.path())
            .unwrap()
            .permissions()
            .mode();
        assert_eq!(mode & 0o777, 0o600);
        let copy: Value = serde_json::from_slice(&std::fs::read(scoped.path()).unwrap()).unwrap();
        assert_eq!(
            copy["mcpServers"]["agent_run_worker"]["env"],
            json!({"AGENT_RUN_WORKER_FINISH":"1","AGENT_RUN_WORKER_TOKEN":"a".repeat(64)})
        );
        assert_eq!(copy["mcpServers"]["pool"]["env"], json!({"POOL":"${POOL}"}));
        let merged = scoped.with_launch(&plan.environment);
        assert_eq!(merged["AGENT_RUN_WORKER_TOKEN"], "a".repeat(64));
        let path = scoped.path().to_owned();
        drop(scoped);
        assert!(!path.exists());
    }

    /// A launch without worker variables is untouched; a worker launch without
    /// a sealed worker entry or MCP config refuses instead of dropping it.
    #[test]
    fn claude_worker_scope_refuses_incomplete_launches() {
        let root = tempfile::tempdir().unwrap();
        let mut plan = launch(root.path());
        plan.environment
            .retain(|name, _| !name.starts_with(WORKER_PREFIX));
        let before = plan.args.clone();
        assert!(
            scope_claude_worker_mcp(&mut plan, root.path())
                .unwrap()
                .is_none()
        );
        assert_eq!(plan.args, before);

        let mut plan = launch(root.path());
        plan.args.clear();
        assert!(scope_claude_worker_mcp(&mut plan, root.path()).is_err());
        assert!(plan.environment.contains_key("AGENT_RUN_WORKER_TOKEN"));

        let mut plan = launch(root.path());
        std::fs::write(
            root.path().join("mcp/mcp-config.json"),
            br#"{"mcpServers":{"pool":{"command":"/bin/pool"}}}"#,
        )
        .unwrap();
        assert!(scope_claude_worker_mcp(&mut plan, root.path()).is_err());
        assert!(plan.environment.contains_key("AGENT_RUN_WORKER_TOKEN"));
    }

    /// Only worker and system names are private; the launch marker, Bridge
    /// settings and other Agent Run names are not.
    #[test]
    fn private_launch_names_are_worker_and_system_only() {
        for name in [
            "AGENT_RUN_WORKER_TOKEN",
            "AGENT_RUN_WORKER_HOME",
            "AGENT_RUN_SYSTEM_WRITER_SOCKET",
            "AGENT_RUN_SYSTEM_OPERATOR_SOCKET",
        ] {
            assert!(is_private_launch_name(name), "{name}");
        }
        for name in [
            "AGENT_RUN_LAUNCH_ID",
            "AGENT_RUN_HOME",
            "BRIDGE_NATIVE_OWNER_SOCKET",
        ] {
            assert!(!is_private_launch_name(name), "{name}");
        }
    }
}
