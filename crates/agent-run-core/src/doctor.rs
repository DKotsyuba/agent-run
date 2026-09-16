//! Bounded, read-only installation diagnostics.

use crate::{
    config::{Auth, Config, Runtime},
    state, Result,
};
use agent_run_domain::error::invalid;
use serde::Serialize;
use serde_json::Value;
use std::{
    fs,
    os::unix::fs::PermissionsExt,
    path::{Path, PathBuf},
    time::{SystemTime, UNIX_EPOCH},
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
/// Provider handshakes and release-process enumeration are omitted instead of
/// inventing a healthy result; configuration, files, state snapshots, process
/// existence, and stale capacity evidence retain Python's finding identifiers.
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
    mcp_self(&report.home, &mut report.findings);
    Ok(report)
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
    if !config.profiles_dir().is_dir() {
        add(
            findings,
            "profile_directory_missing",
            "error",
            "profiles",
            config.profiles_dir().display().to_string(),
        );
    }
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
        skills(home, name, runtime, findings);
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
        auth(runtime, &component, findings);
    }
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
fn auth(runtime: &Runtime, component: &str, findings: &mut Vec<Finding>) {
    match &runtime.auth {
        Some(Auth::Environment { names })
            if !names.iter().any(|name| std::env::var_os(name).is_some()) =>
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

/// Reports missing active supervisor PIDs and safely observes recorded PIDs/groups.
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
        // SAFETY: signal 0 observes the specific recorded PID and does not alter it.
        if unsafe { libc::kill(pid, 0) } == -1
            && std::io::Error::last_os_error().raw_os_error() == Some(libc::ESRCH)
        {
            add(
                findings,
                "dead_supervisor",
                "error",
                &format!("agent:{id}"),
                "dead or reused process identity",
            );
            if let Some(group) = row.get("process_group_id").and_then(Value::as_i64) {
                // SAFETY: signal 0 observes the specific recorded process group only.
                if unsafe { libc::killpg(group as i32, 0) } == 0 {
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

/// Emits the Python inventory self entry without enumerating unrelated processes.
fn mcp_self(home: &Path, findings: &mut Vec<Finding>) {
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
