//! Native engines remain external tools. No Python runtime or Python fallback is used.
pub mod auth;
pub mod claude;
pub mod codex;
pub mod command_policy;
pub mod glm;
pub mod io;
pub mod materialize;
pub mod plugins;
pub mod redact;
use agent_run_config::{
    config::{Adapter, Auth, Config, Runtime},
    profiles::Profile,
};
use agent_run_domain::{
    domain::{Outcome, StartRequest},
    error::invalid,
    Result,
};
use std::{collections::BTreeMap, path::PathBuf};
/// Contains live secrets. Deliberately not Debug or Serialize and never persisted.
pub struct LaunchPlan {
    pub binary: PathBuf,
    pub args: Vec<String>,
    pub cwd: PathBuf,
    pub environment: BTreeMap<String, String>,
    pub initial_input: Option<String>,
}
/// Validates one request against the selected adapter's closed capability set.
///
/// The runtime model, profile grants, authentication shape, and executable
/// path are checked without spawning a child or consulting provider state.
pub fn validate(request: &StartRequest, runtime: &Runtime, profile: &Profile) -> Result<()> {
    let kind = runtime.kind()?;
    claude::validate_runtime(runtime, kind)?;
    agent_run_config::config::native_settings(kind, &runtime.native_settings)?;
    if kind == Adapter::Qwen {
        match &runtime.auth {
            Some(Auth::Environment { names })
                if names
                    .iter()
                    .all(|name| matches!(name.as_str(), "OPENAI_API_KEY" | "OPENAI_BASE_URL")) => {}
            Some(Auth::Environment { .. }) => {
                return Err(invalid("qwen runtime auth.names has unsupported entries"));
            }
            Some(Auth::FileLink { .. }) => {
                return Err(invalid("qwen runtime auth.kind must be 'environment'"));
            }
            None => {}
        }
    }
    if !runtime.models.contains(&request.model) {
        return Err(invalid("model is not configured for this runtime"));
    }
    if request.fast && kind != Adapter::Codex {
        return Err(invalid("fast mode is supported only by codex"));
    }
    if request.output_schema.is_some() && !matches!(kind, Adapter::Claude | Adapter::Glm) {
        return Err(invalid("adapter does not advertise output_schema"));
    }
    if kind == Adapter::Qwen && (profile.network || request.effort.is_some()) {
        return Err(invalid("qwen does not support network profiles or effort"));
    }
    if kind == Adapter::Qwen
        && (request.write != profile.write || request.read_roots != profile.read_roots)
    {
        return Err(invalid(
            "qwen request grants do not match the resolved role",
        ));
    }
    if kind == Adapter::Codex && profile.network && !profile.write {
        return Err(invalid(
            "codex read-only sandbox cannot grant network access",
        ));
    }
    if request.model == "gpt-6-astra"
        && kind == Adapter::Codex
        && (profile.write || !["architect", "review"].contains(&profile.name.as_str()))
    {
        return Err(invalid(
            "gpt-6-astra permits only read-only architect/review roles",
        ));
    }
    if matches!(kind, Adapter::Claude | Adapter::Glm)
        && request
            .effort
            .as_deref()
            .is_some_and(|e| !["low", "medium", "high", "xhigh", "max"].contains(&e))
    {
        return Err(invalid("unsupported Claude effort"));
    }
    use std::os::unix::fs::PermissionsExt;
    if !std::fs::metadata(&runtime.binary)
        .map(|m| m.is_file() && m.permissions().mode() & 0o111 != 0)
        .unwrap_or(false)
    {
        return Err(invalid("runtime binary is not executable"));
    }
    Ok(())
}
pub fn capabilities(kind: Adapter) -> Vec<&'static str> {
    let mut c = vec![
        "read_roots",
        "write",
        "transcript",
        "model_roster",
        "mcp",
        "skills",
        "hooks",
        "resume",
    ];
    if kind != Adapter::Qwen {
        c.extend(["steer", "effort"]);
    } else {
        c.push("live_limits");
    }
    if matches!(kind, Adapter::Claude | Adapter::Glm) {
        c.push("output_schema");
    }
    c.sort_unstable();
    c
}
pub fn environment(
    config: &Config,
    runtime: &Runtime,
    profile: &Profile,
    home: &std::path::Path,
    account: Option<&str>,
    app_home: &std::path::Path,
) -> Result<BTreeMap<String, String>> {
    materialize::environment(config, runtime, profile, home, account, app_home)
}

pub struct EngineResult {
    pub outcome: Outcome,
    pub answer: Option<String>,
    pub usage: Option<serde_json::Value>,
}
