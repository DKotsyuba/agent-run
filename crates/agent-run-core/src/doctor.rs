//! Bounded, read-only installation diagnostics.

use crate::{
    config::{Auth, Config, Runtime},
    state, Result,
};
use agent_run_config::{profiles, role_plan};
use agent_run_domain::{domain::StartRequest, error::invalid};
use agent_run_platform::{
    keychain,
    process::{self, ProcessState},
};
use serde::Serialize;
use serde_json::Value;
use std::{
    fs,
    os::unix::fs::PermissionsExt,
    path::{Path, PathBuf},
    time::{Instant, SystemTime, UNIX_EPOCH},
};

const LIMIT: usize = 256;

/// One secret-safe diagnosis emitted by [`run`].
#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct Finding {
    /// Stable Python-compatible finding identifier.
    pub code: String,
    /// Finding impact: `info`, `warning`, or `error`.
    pub severity: String,
    /// Narrow component identity, never a secret-bearing configuration value.
    pub component: String,
    /// Bounded safe detail explaining the condition.
    pub detail: String,
}

/// Read-only diagnostic report for one agent-run home.
#[derive(Debug, Clone, Serialize)]
pub struct Report {
    /// Resolved home diagnosed by this report.
    pub home: PathBuf,
    /// Unix epoch seconds at which this report was assembled.
    pub checked_at: f64,
    /// At most 256 stable findings.
    pub findings: Vec<Finding>,
}

impl Report {
    /// Returns whether the report contains no error-severity finding.
    pub fn ok(&self) -> bool {
        !self
            .findings
            .iter()
            .any(|finding| finding.severity == "error")
    }
}

/// Diagnoses configuration and durable state without changing either.
///
/// The checks deliberately report only evidence this Rust build can observe.
/// In particular, process identity is read through the native platform layer;
/// an unreadable process is never classified as dead or as an MCP server.
pub fn run(home: &Path) -> Result<Report> {
    let home = home.to_path_buf();
    let mut report = Report {
        home,
        checked_at: now()?,
        findings: Vec::new(),
    };
    let config_path = report.home.join("config.toml");
    plaintext_secrets(&config_path, &mut report.findings);
    let config = match Config::load(&report.home) {
        Ok(config) => config,
        Err(_) => {
            add(
                &mut report.findings,
                "config_invalid",
                "error",
                "config",
                "ValidationError",
            );
            return Ok(report);
        }
    };
    configuration(&config, &report.home, &mut report.findings);
    let snapshot = match state::diagnostics::diagnostic_snapshot(
        &report.home.join("state.db"),
        report.checked_at,
        LIMIT,
    ) {
        Ok(snapshot) => snapshot,
        Err(_) => {
            add(
                &mut report.findings,
                "state_invalid",
                "error",
                "state",
                "ValidationError",
            );
            return Ok(report);
        }
    };
    capacity(
        &config,
        &snapshot.capacity,
        report.checked_at,
        &mut report.findings,
    );
    supervisors(&snapshot.agents, &mut report.findings);
    canary(&report.home, &mut report.findings);
    mcp_self(&report.home, &mut report.findings);
    Ok(report)
}

/// Executes the provider-free detached-launch proof used by Python doctor.
///
/// The invoked binary is this executable's hidden `_doctor_canary` command.
/// It proves session identity and READY through the production launch helper,
/// then exits without opening state, materializing an adapter, or contacting a
/// provider.  Launch failures are represented as a bounded type name only.
fn canary(home: &Path, findings: &mut Vec<Finding>) {
    let started = Instant::now();
    let executable = match std::env::current_exe() {
        Ok(path) => path,
        Err(_) => {
            add(
                findings,
                "supervisor_canary_failed",
                "error",
                "canary",
                "IOError",
            );
            return;
        }
    };
    let args = [
        std::ffi::OsStr::new("--home"),
        home.as_os_str(),
        std::ffi::OsStr::new("_doctor_canary"),
    ];
    match agent_run_platform::launch::launch_detached(
        &executable,
        &args,
        agent_run_platform::launch::Timeouts::default(),
        |_, _| Ok(()),
    ) {
        Ok(launched) => {
            let _ = agent_run_platform::launch::spawn_reaper(launched.pid);
            add(
                findings,
                "supervisor_canary_ok",
                "info",
                "canary",
                format!(
                    "handshake completed in {:.1}ms",
                    started.elapsed().as_secs_f64() * 1000.
                ),
            );
        }
        Err(_) => add(
            findings,
            "supervisor_canary_failed",
            "error",
            "canary",
            "LaunchError",
        ),
    }
}

/// Reports identity and READY for the hidden, provider-free doctor canary.
///
/// The command owns only the inherited launch descriptors.  It does not read
/// or modify the supplied home, allowing doctor to test detached supervision
/// even when an installation contains no configured runtime.
pub fn run_canary(fds: [i32; 3]) -> Result<()> {
    let [ready_fd, identity_fd, error_fd] = fds;
    if let Err(error) = agent_run_platform::launch::report_identity(identity_fd, error_fd) {
        let _ = agent_run_platform::launch::report_ready(ready_fd, Err(&error.to_string()));
        return Err(error.into());
    }
    agent_run_platform::launch::report_ready(ready_fd, Ok(())).map_err(Into::into)
}

/// Returns wall-clock epoch seconds while rejecting a pre-epoch host clock.
fn now() -> Result<f64> {
    Ok(SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| invalid("system clock is before the Unix epoch"))?
        .as_secs_f64())
}

/// Appends one bounded finding.
fn add(
    findings: &mut Vec<Finding>,
    code: &str,
    severity: &str,
    component: &str,
    detail: impl Into<String>,
) {
    if findings.len() < LIMIT {
        findings.push(Finding {
            code: code.into(),
            severity: severity.into(),
            component: component.into(),
            detail: detail.into(),
        });
    }
}

/// Detects secret-looking assignments without preserving their values.
fn plaintext_secrets(path: &Path, findings: &mut Vec<Finding>) {
    let Ok(text) = fs::read_to_string(path) else {
        return;
    };
    for (index, line) in text.lines().take(10_000).enumerate() {
        let key = line
            .split_once('=')
            .map(|(key, _)| key.trim().to_ascii_lowercase());
        if key.is_some_and(|key| {
            let key = key.replace(['_', '-'], "");
            ["secret", "token", "password", "apikey"]
                .iter()
                .any(|needle| key.contains(needle))
        }) {
            add(
                findings,
                "plaintext_secret_config",
                "error",
                &format!("config:{}", index + 1),
                "secret-looking assignment",
            );
        }
    }
}

/// Checks static configuration artifacts which have direct local evidence.
fn configuration(config: &Config, home: &Path, findings: &mut Vec<Finding>) {
    for (name, server) in config.mcp.iter().take(LIMIT) {
        if !executable(&server.command) {
            add(
                findings,
                "mcp_executable_missing",
                "error",
                &format!("mcp:{name}"),
                server.command.display().to_string(),
            );
        }
    }
    let canonical_roles = roles(config, home, findings);
    for (name, runtime) in config
        .runtimes
        .iter()
        .filter(|(_, runtime)| runtime.enabled)
        .take(LIMIT)
    {
        let component = format!("runtime:{name}");
        if !executable(&runtime.binary) {
            add(
                findings,
                "runtime_binary_missing",
                "error",
                &component,
                runtime.binary.display().to_string(),
            );
        }
        if canonical_roles && (!runtime.skills.is_empty() || !runtime.mcp.is_empty()) {
            add(
                findings,
                "mixed_role_assets",
                "error",
                &component,
                "canonical roles cannot be mixed with runtime skills or MCP lists",
            );
        } else if !canonical_roles {
            skills(home, name, runtime, findings);
        }
        if runtime.rust.is_some() || runtime.environment.is_some() {
            add(
                findings,
                "legacy_environment_config",
                "warning",
                &component,
                "legacy environment/toolchain declarations are not used for readiness",
            );
        }
        if runtime.default_account.is_some() {
            add(
                findings,
                "legacy_default_account",
                "warning",
                &component,
                "default_account is ignored; omit account for native global auth",
            );
        }
        hooks(runtime, &component, home, findings);
        auth(name, runtime, &component, findings);
    }
}

/// Validates every profile against the shared canonical-role resolver.
///
/// The return value records whether at least one revisioned role was found.
/// Invalid profile files are findings rather than a reason to stop unrelated
/// installation checks; missing or unreadable catalog directories use the
/// Python-compatible missing-directory finding.
fn roles(config: &Config, home: &Path, findings: &mut Vec<Finding>) -> bool {
    let directory = config.profiles_dir();
    if !directory.is_dir() {
        add(
            findings,
            "profile_directory_missing",
            "error",
            "profiles",
            directory.display().to_string(),
        );
        return false;
    }
    let Ok(entries) = fs::read_dir(directory) else {
        return false;
    };
    let mut paths = entries
        .filter_map(std::result::Result::ok)
        .map(|entry| entry.path())
        .filter(|path| path.extension().is_some_and(|ext| ext == "md"))
        .collect::<Vec<_>>();
    paths.sort();
    let mut canonical = false;
    let mut legacy = false;
    for path in paths.into_iter().take(LIMIT) {
        let Some(name) = path.file_stem().and_then(|name| name.to_str()) else {
            continue;
        };
        let request = StartRequest {
            runtime: "doctor".into(),
            model: "doctor".into(),
            profile: name.into(),
            task: "doctor profile validation".into(),
            workdir: home.to_path_buf(),
            write: false,
            fast: false,
            effort: None,
            timeout_seconds: None,
            read_roots: Vec::new(),
            output_schema: None,
            orchestrator: None,
            request_id: None,
            account: None,
            required_constraints: Default::default(),
        };
        let result = fs::read_to_string(&path)
            .map_err(agent_run_domain::Error::from)
            .and_then(|text| profiles::parse(&text, &request))
            .and_then(|profile| {
                if profile.canonical {
                    canonical = true;
                    role_plan::resolve_role_plan(
                        &profile,
                        config.skills_dir(),
                        &config.mcp,
                        "global",
                        None,
                    )?;
                } else {
                    legacy = true;
                }
                Ok(())
            });
        if let Err(error) = result {
            add(
                findings,
                "role_invalid",
                "error",
                &format!("profile:{name}"),
                error.to_string(),
            );
        }
    }
    if canonical && legacy {
        add(
            findings,
            "mixed_role_catalog",
            "error",
            "profiles",
            "canonical and legacy profiles cannot be mixed",
        );
    }
    canonical
}

/// Checks legacy runtime skill files while role-plan inspection remains shared elsewhere.
fn skills(home: &Path, name: &str, runtime: &Runtime, findings: &mut Vec<Finding>) {
    for skill in &runtime.skills {
        if !home
            .join("skills")
            .join(name)
            .join(skill)
            .join("SKILL.md")
            .is_file()
        {
            add(
                findings,
                "runtime_skill_missing",
                "error",
                &format!("runtime:{name}"),
                skill,
            );
        }
    }
}

/// Checks executable and trusted-location evidence for configured hooks.
fn hooks(runtime: &Runtime, component: &str, home: &Path, findings: &mut Vec<Finding>) {
    for (index, hook) in runtime.hooks.iter().enumerate() {
        let item = format!("{component}:hook:{index}");
        let Some(command) = hook.command.first() else {
            continue;
        };
        let executable_path = Path::new(command);
        if !executable(executable_path) {
            add(findings, "hook_executable_missing", "error", &item, command);
        }
        if !executable_path.is_absolute() || !executable_path.starts_with(home) {
            add(findings, "hook_untrusted", "warning", &item, command);
        }
    }
}

/// Checks declared environment and file-link authentication without reading credentials.
fn auth(name: &str, runtime: &Runtime, component: &str, findings: &mut Vec<Finding>) {
    match &runtime.auth {
        Some(Auth::Environment { names })
            if !names.iter().any(|name| std::env::var_os(name).is_some())
                && !keychain_present(name) =>
        {
            add(
                findings,
                "auth_environment_missing",
                "warning",
                component,
                names.join(","),
            );
        }
        Some(Auth::FileLink { source, target }) => {
            let bridge = runtime.home.join(target);
            if !source.exists() {
                add(
                    findings,
                    "auth_source_missing",
                    "error",
                    component,
                    source.display().to_string(),
                );
            }
            match fs::read_link(&bridge) {
                Ok(actual) if actual == *source => {}
                Ok(_) => add(
                    findings,
                    "auth_bridge_mismatch",
                    "error",
                    component,
                    bridge.display().to_string(),
                ),
                Err(_) => add(
                    findings,
                    "auth_bridge_missing",
                    "error",
                    component,
                    bridge.display().to_string(),
                ),
            }
        }
        _ => {}
    }
}

/// Returns whether a configured runtime has a readable macOS Keychain fallback.
///
/// Values returned by the platform helper are immediately discarded, keeping
/// the report limited to the presence signal used by Python doctor.
fn keychain_present(name: &str) -> bool {
    keychain_present_with(name, |account, service| match account {
        Some(account) => keychain::generic_password(account, service).is_some(),
        None => keychain::generic_password_service(service).is_some(),
    })
}

/// Applies the fallback catalog to a secret-free credential-presence probe.
///
/// `lookup` receives only fixed Keychain selectors and returns whether a
/// nonempty value could be read.  The injectable probe tests this policy
/// without accessing an operator's Keychain.
fn keychain_present_with(name: &str, lookup: impl FnOnce(Option<&str>, &str) -> bool) -> bool {
    let fallback = match name {
        "qwen" => Some((
            Some("OMNIROUTE_API_KEY"),
            "com.pluto.agent-run.opencode.omniroute",
        )),
        "glm" => Some((Some("GLM_CODING_KEY"), "com.pluto.agent-run.glm")),
        "claude" => Some((None, "Claude Code-credentials")),
        _ => None,
    };
    fallback.is_some_and(|(account, service)| lookup(account, service))
}

/// Flags capacity samples whose validity window or age has expired.
fn capacity(config: &Config, rows: &[Value], at: f64, findings: &mut Vec<Finding>) {
    let stale_after = config.capacity.collect_interval_seconds.max(1) as f64 * 2.0;
    for row in rows {
        let stale = match row.get("valid_until").and_then(Value::as_f64) {
            Some(valid_until) => valid_until < at,
            None => row
                .get("observed_at")
                .and_then(Value::as_f64)
                .is_some_and(|observed| at - observed > stale_after),
        };
        if stale {
            let runtime = row
                .get("runtime")
                .and_then(Value::as_str)
                .unwrap_or("unknown");
            let detail = ["lane", "window", "target", "source"]
                .iter()
                .map(|key| row.get(*key).and_then(Value::as_str).unwrap_or("-"))
                .collect::<Vec<_>>()
                .join("/");
            add(
                findings,
                "capacity_stale",
                "warning",
                &format!("capacity:{runtime}"),
                detail,
            );
        }
    }
}

/// Reports missing active supervisor PIDs and safely observes recorded PID births.
///
/// A native identity observation is the only death proof accepted here.  When
/// the platform cannot read a process, doctor mirrors Python by warning that
/// identity is unavailable rather than turning a sandbox restriction into a
/// false dead-supervisor error.
fn supervisors(rows: &[Value], findings: &mut Vec<Finding>) {
    for row in rows {
        let id = row.get("id").and_then(Value::as_str).unwrap_or("unknown");
        let status = row
            .get("status")
            .and_then(Value::as_str)
            .unwrap_or_default();
        let Some(pid) = row
            .get("supervisor_pid")
            .and_then(Value::as_i64)
            .map(|pid| pid as i32)
        else {
            if matches!(status, "running" | "cancelling") {
                add(
                    findings,
                    "dead_supervisor",
                    "error",
                    &format!("agent:{id}"),
                    "missing pid",
                );
            }
            continue;
        };
        let token = row.get("supervisor_identity").and_then(Value::as_str);
        let birth = row.get("supervisor_birth_time").and_then(Value::as_f64);
        let observation = process::observe(Some(pid), token, birth);
        if matches!(observation, ProcessState::Unknown | ProcessState::Denied) {
            add(
                findings,
                "supervisor_identity_unavailable",
                "warning",
                &format!("agent:{id}"),
                "process birth identity unavailable",
            );
            continue;
        }
        if matches!(observation, ProcessState::Dead | ProcessState::Reused) {
            add(
                findings,
                "dead_supervisor",
                "error",
                &format!("agent:{id}"),
                "dead or reused process identity",
            );
            if let Some(group) = row.get("process_group_id").and_then(Value::as_i64) {
                if process_group_alive(group as i32) {
                    add(
                        findings,
                        "suspected_orphan",
                        "error",
                        &format!("agent:{id}"),
                        "engine group remains alive",
                    );
                }
            }
        }
    }
}

/// Returns whether a native process snapshot proves a member remains in `group`.
///
/// A failed snapshot intentionally returns false: it supplies no orphan proof,
/// matching Python's distinction between a missing process and unavailable
/// process-observation permissions.
fn process_group_alive(group: i32) -> bool {
    group > 1
        && process::processes()
            .map(|processes| {
                processes
                    .into_iter()
                    .any(|item| item.group == group && !item.zombie)
            })
            .unwrap_or(false)
}

/// Emits the Python inventory self entry after a bounded native process snapshot.
///
/// The platform intentionally does not expose argv for arbitrary processes.
/// Consequently this check never labels an unrelated PID as MCP merely from
/// its identity; a failed or opaque enumeration is the same no-finding result
/// as Python's unavailable `ps` command.
fn mcp_self(home: &Path, findings: &mut Vec<Finding>) {
    let _ = process::processes();
    let executable = std::env::current_exe()
        .ok()
        .map(|path| path.display().to_string())
        .unwrap_or_else(|| "unknown".into());
    let current = fs::read_link(home.join("standalone/current"))
        .ok()
        .map(|path| path.display().to_string())
        .unwrap_or_else(|| "unknown".into());
    add(
        findings,
        "mcp_inventory_self",
        "info",
        "mcp:self",
        format!("release={executable} current_target={current}"),
    );
}

/// Returns whether a configured path is an absolute executable regular file.
fn executable(path: &Path) -> bool {
    fs::metadata(path)
        .map(|meta| path.is_absolute() && meta.is_file() && meta.permissions().mode() & 0o111 != 0)
        .unwrap_or(false)
}

#[cfg(test)]
mod tests {
    use super::keychain_present_with;

    /// Mirrors `KeychainFallbackAuthTests.test_a_present_keychain_item_suppresses_the_warning`.
    #[test]
    fn python_doctor_keychain_fallback_uses_only_fixed_selectors() {
        let mut seen = None;
        assert!(keychain_present_with("glm", |account, service| {
            seen = Some((account.map(str::to_owned), service.to_owned()));
            true
        }));
        assert_eq!(
            seen,
            Some((
                Some("GLM_CODING_KEY".into()),
                "com.pluto.agent-run.glm".into()
            ))
        );
    }

    /// Mirrors `KeychainFallbackAuthTests.test_an_item_without_a_fixed_account_is_probed_by_service_alone`.
    #[test]
    fn python_doctor_keychain_claude_probe_has_no_account() {
        let mut seen = None;
        assert!(keychain_present_with("claude", |account, service| {
            seen = Some((account.map(str::to_owned), service.to_owned()));
            true
        }));
        assert_eq!(seen, Some((None, "Claude Code-credentials".into())));
    }
}
