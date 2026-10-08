//! Native engines remain external tools. No Python runtime or Python fallback is used.
pub mod auth;
pub mod authorized_request;
pub mod claude;
pub mod codex;
pub mod command_policy;
pub mod glm;
pub mod io;
pub mod materialize;
pub mod mcp_catalog;
pub mod native_failure;
pub mod plugins;
pub mod provider;
pub mod redact;
use agent_run_config::{
    config::{Adapter, Config, Runtime},
    profiles::Profile,
};
use agent_run_domain::{
    Result,
    domain::{Outcome, StartRequest},
    error::invalid,
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
/// Validates one legacy request against the selected adapter's closed capability set.
/// Restricted research requires the schema-2 attempt-bound worker channel;
/// legacy launches are rejected rather than receiving weaker native tools.
///
/// The runtime model, profile grants, authentication shape, and executable
/// path are checked without spawning a child or consulting provider state.
pub fn validate(request: &StartRequest, runtime: &Runtime, profile: &Profile) -> Result<()> {
    let kind = runtime.kind()?;
    if profile.research_tools_only() {
        return Err(invalid(
            "restricted research requires a schema-2 provider launch",
        ));
    }
    claude::validate_runtime(runtime, kind)?;
    agent_run_config::config::native_settings(kind, &runtime.native_settings)?;
    if !runtime.models.contains(&request.model) {
        return Err(invalid("model is not configured for this runtime"));
    }
    if request.fast && kind != Adapter::Codex {
        return Err(invalid("fast mode is supported only by codex"));
    }
    if request.output_schema.is_some() && !matches!(kind, Adapter::Claude | Adapter::Glm) {
        return Err(invalid("adapter does not advertise output_schema"));
    }
    validate_role(runtime, profile)?;
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
    validate_executable(runtime)
}

/// Checks static native role compatibility for catalog, admission and launch.
/// Restricted Codex research uses hosted web with raw environment access
/// disabled; generated native controls are verified again before any turn.
/// Other read-only Codex network roles retain the established rejection.
/// No child, credential or mutable state is opened by this check.
pub fn validate_role(runtime: &Runtime, profile: &Profile) -> Result<()> {
    profile.validate_research()?;
    if runtime.kind()? == Adapter::Codex
        && profile.network
        && !profile.write
        && !profile.research_tools_only()
    {
        return Err(invalid(
            "codex read-only sandbox cannot grant network access",
        ));
    }
    Ok(())
}

/// Checks the configured native binary's file type and executable mode without
/// spawning it or reading credentials. Shared by legacy and provider admission
/// and real launch validation; missing files and non-executable paths are typed
/// input errors. Mutable file state must still be rechecked at actual spawn.
pub fn validate_executable(runtime: &Runtime) -> Result<()> {
    use std::os::unix::fs::PermissionsExt;
    if !std::fs::metadata(&runtime.binary)
        .map(|m| m.is_file() && m.permissions().mode() & 0o111 != 0)
        .unwrap_or(false)
    {
        return Err(invalid("runtime binary is not executable"));
    }
    Ok(())
}
/// Returns the stable capability roster for a packaged adapter family.
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
    c.extend(["steer", "effort"]);
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
    /// Typed disposition of an authoritative native failure signal, when the
    /// harness protocol reported one (see [`native_failure`]); never derived
    /// from message text.
    pub native_failure: Option<native_failure::NativeFailure>,
}
