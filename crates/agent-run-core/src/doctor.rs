//! Bounded, read-only installation diagnostics.

use crate::{
    Result,
    config::{Auth, Config, Runtime},
    state,
};
use agent_run_config::{profiles, role_plan};
use agent_run_domain::{domain::StartRequest, error::invalid};
use agent_run_platform::{
    keychain,
    process::{self, ProcessState},
};
use chrono::TimeZone;
use serde::Serialize;
use serde_json::Value;
use std::{
    collections::{BTreeMap, BTreeSet},
    fs,
    io::{self, Read},
    os::fd::AsRawFd,
    os::unix::fs::PermissionsExt,
    path::{Path, PathBuf},
    process::{Command, Stdio},
    sync::Arc,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

/// Maximum independently reported legacy findings and state snapshot rows.
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
    /// Typed additive observations serialized beside the historical fields.
    #[serde(flatten)]
    pub diagnostics: Diagnostics,
}

/// Closed local-check outcomes; absence of evidence is never success.
#[derive(Debug, Clone, Copy, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum CheckStatus {
    /// Named local observation passed.
    Ok,
    /// Optional or recoverable limitation was observed.
    Warning,
    /// Required local observation failed.
    Failed,
    /// Observation was unavailable or deliberately skipped.
    NotChecked,
}

/// Bounded local observation and safe remediation, never raw error metadata.
#[derive(Debug, Clone, Serialize)]
pub struct Check {
    /// Stable public check identifier.
    pub name: String,
    /// Closed evidence classification.
    pub status: CheckStatus,
    /// Bounded explanation of observed facts.
    pub detail: String,
    /// Safe local repair or follow-up, never a model turn or credential value.
    pub remediation: String,
}

/// Honest compiler build facts; checkout identity is not runtime evidence.
#[derive(Debug, Clone, Serialize)]
pub struct BuildInfo {
    /// Unknown unless embedded; never guessed from the current checkout.
    pub source_commit: Option<String>,
    /// Actual rustc-selected debug-assertion mode.
    pub debug_assertions: bool,
}

/// Compiler-selected platform, independent of installed release declarations.
#[derive(Debug, Clone, Serialize)]
pub struct TargetInfo {
    /// Target architecture selected by rustc.
    pub arch: &'static str,
    /// Target OS selected by rustc.
    pub os: &'static str,
}

/// Read-only COMPLETE/manifest/metadata observation of one release directory.
#[derive(Debug, Clone, Serialize)]
pub struct ReleaseSummary {
    /// Resolved observed release directory; absent in source builds.
    pub path: Option<PathBuf>,
    /// Shared seal-verifier observation.
    pub check: Check,
    /// Bounded version from verified metadata, absent without usable evidence.
    pub version: Option<String>,
    /// Store schema from verified metadata.
    pub schema_version: Option<u64>,
}

/// Resolved dependency evidence; args, environment and credentials are absent.
#[derive(Debug, Clone, Serialize)]
pub struct ToolSummary {
    /// Public component identity.
    pub name: String,
    /// Actual canonical executable when observable.
    pub executable: Option<PathBuf>,
    /// Extracted numeric version; unknown stays absent.
    pub version: Option<String>,
    /// Executable/version classification; required missing files fail.
    pub status: CheckStatus,
    /// Safe local follow-up for missing or unobserved evidence.
    pub remediation: String,
}

/// Additive diagnostic fields serialized beside the historical report fields.
#[derive(Debug, Clone, Serialize)]
pub struct Diagnostics {
    /// Product version of this executing Cargo build.
    pub version: &'static str,
    /// Honest compile-time build facts.
    pub build: BuildInfo,
    /// Compiler-selected platform.
    pub target: TargetInfo,
    /// Configuration read/parse observation.
    pub config: Check,
    /// Executing sealed release; source builds remain not_checked.
    pub executing_release: ReleaseSummary,
    /// Current installed release, when its pointer exists.
    pub current_release: ReleaseSummary,
    /// Resident version/schema compatibility, explicitly unavailable on current ping.
    pub resident_compatibility: Check,
    /// Local observations including original legacy finding codes.
    pub checks: Vec<Check>,
    /// Required configured dependencies and optional host Node bridge.
    pub tools: Vec<ToolSummary>,
}

impl Default for Report {
    /// Creates unknown observations for injectable seams without any I/O.
    fn default() -> Self {
        Self {
            home: PathBuf::new(),
            checked_at: 0.0,
            findings: Vec::new(),
            diagnostics: Diagnostics {
                version: env!("CARGO_PKG_VERSION"),
                build: BuildInfo {
                    source_commit: None,
                    debug_assertions: cfg!(debug_assertions),
                },
                target: TargetInfo {
                    arch: std::env::consts::ARCH,
                    os: std::env::consts::OS,
                },
                config: check(
                    "config",
                    CheckStatus::NotChecked,
                    "not read",
                    "run local doctor",
                ),
                executing_release: unobserved_release("executing_release"),
                current_release: unobserved_release("current_release"),
                resident_compatibility: check(
                    "resident_compatibility",
                    CheckStatus::NotChecked,
                    "resident ping exposes no version/schema compatibility; process inventory is reported separately",
                    "verify the installed release and reconnect older sessions after switching releases",
                ),
                checks: Vec::new(),
                tools: Vec::new(),
            },
        }
    }
}

/// Builds fixed bounded check framing, discarding underlying error chains.
fn check(name: &str, status: CheckStatus, detail: &str, remediation: &str) -> Check {
    Check {
        name: name.into(),
        status,
        detail: detail.chars().take(512).collect(),
        remediation: remediation.into(),
    }
}

/// Represents unavailable release evidence without inventing a version or seal.
fn unobserved_release(name: &str) -> ReleaseSummary {
    ReleaseSummary {
        path: None,
        version: None,
        schema_version: None,
        check: check(
            name,
            CheckStatus::NotChecked,
            "no sealed release observed",
            "source builds require no installed release; inspect installed releases with release verify",
        ),
    }
}

/// Applies the shared read-only seal verifier to one observed directory.
/// Raw manifest contents, filesystem errors and parser diagnostics stay private.
fn release_summary(name: &str, path: Option<PathBuf>) -> ReleaseSummary {
    let Some(path) = path else {
        return unobserved_release(name);
    };
    let verified = agent_run_platform::release::verify(&path).is_ok();
    let metadata = verified
        .then(|| fs::read(path.join("metadata.json")).ok())
        .flatten()
        .and_then(|bytes| serde_json::from_slice::<Value>(&bytes).ok());
    ReleaseSummary {
        version: metadata
            .as_ref()
            .and_then(|v| v["version"].as_str())
            .and_then(|v| numeric_version(v.as_bytes())),
        schema_version: metadata.as_ref().and_then(|v| v["schema_version"].as_u64()),
        check: check(
            name,
            if verified {
                CheckStatus::Ok
            } else {
                CheckStatus::Failed
            },
            if verified {
                "COMPLETE, SHA256SUMS and metadata verified"
            } else {
                "sealed release verification failed"
            },
            if verified {
                "none"
            } else {
                "verify this release directory before switching its current pointer"
            },
        ),
        path: Some(path),
    }
}

/// Extracts only a short numeric dotted token; arbitrary tool output and
/// credential-shaped assignments are never serialized into a report.
fn numeric_version(bytes: &[u8]) -> Option<String> {
    std::str::from_utf8(bytes)
        .ok()?
        .split_whitespace()
        .find_map(|token| {
            let token = token.trim_matches(|c: char| !c.is_ascii_alphanumeric() && c != '.');
            let token = token.strip_prefix('v').unwrap_or(token);
            if token.len() > 64
                || !token
                    .bytes()
                    .all(|b| b.is_ascii_alphanumeric() || b".-+".contains(&b))
            {
                return None;
            }
            let base = token.split(['-', '+']).next()?;
            let parts: Vec<_> = base.split('.').collect();
            (parts.len() >= 2
                && parts
                    .iter()
                    .all(|part| !part.is_empty() && part.bytes().all(|b| b.is_ascii_digit())))
            .then(|| token.to_owned())
        })
}

/// Owns the exact version-probe child and its captured group on every path.
struct VersionChild {
    /// Unreaped direct child, not an unrelated reused PID.
    child: std::process::Child,
    /// Existing PID-safe cleanup authority for captured descendants.
    owner: process::OwnedProcess,
}
impl Drop for VersionChild {
    /// Cleans captured identities through the platform helper, then kills/reaps the direct child.
    fn drop(&mut self) {
        let _ = self.owner.cleanup_blocking(Duration::from_millis(100));
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

/// Runs --version with null stdin/stderr, a two-second lifetime and 8 KiB
/// accepted stdout. No auth, network or model command is issued.
fn tool_version(binary: &Path) -> Option<String> {
    use std::os::unix::process::CommandExt;
    let child = Command::new(binary)
        .arg("--version")
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .process_group(0)
        .spawn()
        .ok()?;
    let owner = process::OwnedProcess::capture(child.id() as i32);
    let mut probe = VersionChild { child, owner };
    let deadline = Instant::now() + Duration::from_secs(2);
    let mut refresh_at = Instant::now();
    loop {
        if Instant::now() >= refresh_at {
            probe.owner.refresh();
            refresh_at = Instant::now() + Duration::from_millis(100);
        }
        match probe.child.try_wait() {
            Ok(Some(status)) if status.success() => {
                let output = read_until(probe.child.stdout.as_mut()?, deadline, 8192)?;
                return (output.len() <= 8192)
                    .then(|| numeric_version(&output))
                    .flatten();
            }
            Ok(Some(_)) | Err(_) => return None,
            Ok(None) if Instant::now() >= deadline => return None,
            Ok(None) => std::thread::sleep(Duration::from_millis(10)),
        }
    }
}

/// Resolves a dependency and optionally probes its known version interface.
/// Missing required binaries fail; absent optional bridge or unknown interface
/// remains not_checked. Args, environment and authentication are omitted.
fn tool_summary(name: String, binary: Option<&Path>, required: bool, probe: bool) -> ToolSummary {
    let present = binary.is_some_and(executable);
    let path = binary.filter(|_| present).map(resolved);
    let version = path.as_deref().filter(|_| probe).and_then(tool_version);
    ToolSummary {
        name,
        executable: path,
        version: version.clone(),
        status: if !present && required {
            CheckStatus::Failed
        } else if !present || !probe {
            CheckStatus::NotChecked
        } else if version.is_some() {
            CheckStatus::Ok
        } else {
            CheckStatus::Warning
        },
        remediation: if !present && required {
            "configure an existing absolute executable path"
        } else if !present {
            "optional host Node bridge absent; direct Rust MCP remains available"
        } else if !probe {
            "version CLI is unrecognized; inspect the tool manually"
        } else if version.is_none() {
            "verify the local version response; only --version was requested"
        } else {
            "none"
        }
        .into(),
    }
}

/// Checks every configured dependency; at most 16 version CLIs including Node
/// are probed. The CLI applies its complete-report byte bound without dropping rows.
/// Arbitrary MCP command interfaces are never executed speculatively.
fn diagnostic_tools(
    config: &Config,
    v2: Option<&agent_run_config::provider_config::ProviderConfig>,
) -> Vec<ToolSummary> {
    let mut binaries = BTreeMap::new();
    if let Some(v2) = v2 {
        for (name, harness) in &v2.harnesses {
            binaries.insert(
                format!("harness:{}", name.as_str()),
                harness.binary.as_path(),
            );
        }
    } else {
        for (name, runtime) in config.runtimes.iter().filter(|(_, v)| v.enabled) {
            binaries.insert(format!("runtime:{name}"), runtime.binary.as_path());
        }
    }
    let mut tools = Vec::new();
    for (index, (name, binary)) in binaries.into_iter().enumerate() {
        let known = binary
            .file_name()
            .and_then(|v| v.to_str())
            .is_some_and(|v| {
                matches!(
                    v,
                    "codex" | "claude" | "claude-code" | "glm" | "opencode" | "node"
                )
            });
        tools.push(tool_summary(name, Some(binary), true, known && index < 15));
    }
    for (name, server) in &config.mcp {
        tools.push(tool_summary(
            format!("mcp:{name}"),
            Some(&server.command),
            true,
            false,
        ));
    }
    let node = std::env::var_os("CODEX_MCP_NODE_PATH").map(PathBuf::from);
    tools.push(tool_summary(
        "host_node_bridge".into(),
        node.as_deref(),
        node.is_some(),
        true,
    ));
    tools
}

/// Mirrors existing finding codes/severities into explicit checks while retaining
/// the historical findings array. No skipped observation becomes an ok check.
fn finish_report(report: &mut Report) {
    report
        .diagnostics
        .checks
        .extend(report.findings.iter().map(|finding| {
            check(
                &finding.code,
                match finding.severity.as_str() {
                    "error" => CheckStatus::Failed,
                    "warning" => CheckStatus::Warning,
                    _ => CheckStatus::Ok,
                },
                &finding.detail,
                if finding.severity == "error" {
                    "repair this local component and rerun doctor; do not retry admitted work"
                } else if finding.severity == "warning" {
                    "inspect the named local evidence before relying on this component"
                } else {
                    "none"
                },
            )
        }));
}

/// One process record needed to diagnose stale MCP sessions.
#[derive(Debug, Clone, PartialEq)]
pub struct McpProcess {
    /// Operating-system process identifier.
    pub pid: i32,
    /// Process start time in Unix seconds, when it could be parsed.
    pub started_at: Option<f64>,
    /// Bounded command line reported by the process lister.
    pub command: String,
}

/// Injectable native probes used by [`run_with`].
pub struct Dependencies {
    /// Executable used for the detached canary; `None` uses this executable.
    pub canary_executable: Option<PathBuf>,
    /// Process inventory provider; production uses bounded `ps` snapshots.
    pub process_lister: Arc<dyn Fn() -> Vec<McpProcess> + Send + Sync>,
}

impl Default for Dependencies {
    /// Select the same executable and native process inventory as production doctor.
    fn default() -> Self {
        Self {
            canary_executable: None,
            process_lister: Arc::new(list_mcp_processes),
        }
    }
}

impl Dependencies {
    /// Builds doctor probes with an optional canary executable and process lister.
    pub fn new(
        canary_executable: Option<PathBuf>,
        process_lister: impl Fn() -> Vec<McpProcess> + Send + Sync + 'static,
    ) -> Self {
        Self {
            canary_executable,
            process_lister: Arc::new(process_lister),
        }
    }
}

impl Report {
    /// Returns whether all required observed local checks passed.
    /// Optional warnings/not_checked observations preserve the historical success policy.
    pub fn ok(&self) -> bool {
        !self
            .findings
            .iter()
            .any(|finding| finding.severity == "error")
            && self.diagnostics.config.status != CheckStatus::Failed
            && self.diagnostics.executing_release.check.status != CheckStatus::Failed
            && self.diagnostics.current_release.check.status != CheckStatus::Failed
            && !self
                .diagnostics
                .tools
                .iter()
                .any(|tool| tool.status == CheckStatus::Failed)
            && !self
                .diagnostics
                .checks
                .iter()
                .any(|check| check.status == CheckStatus::Failed)
    }
}

/// Diagnoses configuration and durable state without changing either.
///
/// The checks deliberately report only evidence this Rust build can observe.
/// In particular, process identity is read through the native platform layer;
/// an unreadable process is never classified as dead or as an MCP server.
pub fn run(home: &Path) -> Result<Report> {
    run_with(home, &Dependencies::default())
}

/// Runs doctor with injectable canary and process-inventory probes, including read-only
/// findings for terminal lineages whose attempt ownership remains unresolved.
pub fn run_with(home: &Path, dependencies: &Dependencies) -> Result<Report> {
    let home = home.to_path_buf();
    let mut report = Report {
        home,
        checked_at: now()?,
        findings: Vec::new(),
        ..Report::default()
    };
    let executing = std::env::current_exe().ok().and_then(|binary| {
        let bin = binary.parent()?;
        (bin.file_name()? == "bin" && bin.parent()?.join("COMPLETE").exists())
            .then(|| bin.parent().map(Path::to_path_buf))
            .flatten()
    });
    report.diagnostics.executing_release = release_summary("executing_release", executing);
    let current = report.home.join("standalone/current");
    if fs::symlink_metadata(&current).is_ok() {
        report.diagnostics.current_release =
            release_summary("current_release", Some(resolved(&current)));
    }
    let config_path = report.home.join("config.toml");
    plaintext_secrets(&config_path, &mut report.findings);
    let (config, provider_config) = match (
        agent_run_config::provider_config::ProviderConfig::load(&report.home),
        Config::load(&report.home),
    ) {
        (Ok((v2, _)), _) => (providers(&v2, &mut report.findings), Some(v2)),
        (_, Ok(config)) => (config, None),
        (Err(_), Err(_)) => {
            add(
                &mut report.findings,
                "config_invalid",
                "error",
                "config",
                "ValidationError",
            );
            report.diagnostics.config = check(
                "config",
                CheckStatus::Failed,
                "configuration is unreadable or invalid",
                "repair config.toml locally; no authentication check was attempted",
            );
            finish_report(&mut report);
            return Ok(report);
        }
    };
    report.diagnostics.config = check(
        "config",
        CheckStatus::Ok,
        "configuration parsed without changing it",
        "none",
    );
    report.diagnostics.tools = diagnostic_tools(&config, provider_config.as_ref());
    configuration(
        &config,
        &report.home,
        provider_config
            .as_ref()
            .is_some_and(|config| !config.providers.is_empty()),
        &mut report.findings,
    );
    let snapshot = match state::diagnostics::diagnostic_snapshot(
        &report.home.join("state.db"),
        report.checked_at,
        LIMIT,
    ) {
        Ok(snapshot) => snapshot,
        Err(error) if error.to_string().starts_with("state migration required:") => {
            add(
                &mut report.findings,
                "state_migration_pending",
                "error",
                "state",
                error.to_string(),
            );
            finish_report(&mut report);
            return Ok(report);
        }
        Err(_) => {
            add(
                &mut report.findings,
                "state_invalid",
                "error",
                "state",
                "ValidationError",
            );
            finish_report(&mut report);
            return Ok(report);
        }
    };
    report.diagnostics.checks.push(check(
        "state",
        CheckStatus::Ok,
        "read-only state snapshot obtained",
        "none",
    ));
    if let Some(config) = &provider_config {
        provider_bindings(config, &report.home, &mut report.findings);
        managed_services(
            config,
            &report.home,
            report.checked_at,
            &mut report.findings,
        );
    }
    capacity(
        &config,
        &snapshot.capacity,
        report.checked_at,
        &mut report.findings,
    );
    supervisors(&snapshot.agents, &mut report.findings);
    terminal_attempt_ownership(&report.home, &mut report.findings);
    match state::incidents::read_summary(&report.home.join("state.db")) {
        Ok(value) => report.diagnostics.checks.push(check(
            "incident_ledger",
            CheckStatus::Ok,
            &format!(
                "{} retained content-free phase records",
                value["records"].as_i64().unwrap_or(0)
            ),
            "none",
        )),
        Err(_) => add(
            &mut report.findings,
            "incident_ledger_unavailable",
            "warning",
            "state",
            "read-only incident summary is unavailable",
        ),
    }
    canary(
        &report.home,
        dependencies.canary_executable.as_deref(),
        &mut report.findings,
    );
    mcp_inventory(
        &report.home,
        &mut report.findings,
        dependencies.process_lister.as_ref(),
    );
    finish_report(&mut report);
    Ok(report)
}

/// Reports cold/warm/unhealthy service state from a read-only connection and fresh PID evidence.
/// Never launches a probe, starts a daemon, or emits command arguments, environment values or credentials.
fn managed_services(
    config: &agent_run_config::provider_config::ProviderConfig,
    home: &Path,
    at: f64,
    findings: &mut Vec<Finding>,
) {
    let inspect = (|| -> Result<()> {
        let connection = rusqlite::Connection::open_with_flags(
            home.join("state.db"),
            rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY,
        )?;
        let mut query=connection.prepare("SELECT g.service_id,g.state,g.process_identity_json,g.checked_at,p.leader_json,g.ownership FROM managed_service_generations g LEFT JOIN process_ownership p ON p.owner_kind='service' AND p.owner_id=g.id WHERE g.state!='stopped' ORDER BY g.service_id LIMIT 256")?;
        let mut rows = query.query([])?;
        let mut seen = BTreeSet::new();
        while let Some(row) = rows.next()? {
            let name: String = row.get(0)?;
            let state: String = row.get(1)?;
            let identity: Option<String> = row.get(2)?;
            let checked: Option<f64> = row.get(3)?;
            let captured: Option<String> = row.get(4)?;
            let external = row.get::<_, String>(5)? == "external";
            let component = if config.services.contains_key(&name) {
                format!("service:{name}")
            } else {
                "services".into()
            };
            seen.insert(name.clone());
            let root =
                identity.and_then(|value| serde_json::from_str::<process::Identity>(&value).ok());
            let captured =
                captured.and_then(|value| serde_json::from_str::<process::Identity>(&value).ok());
            let alive = root.as_ref().is_some_and(|root| {
                captured.as_ref().is_some_and(|saved| {
                    saved.pid == root.pid
                        && saved.token == root.token
                        && saved.birth == root.birth
                        && saved.group == root.group
                }) && process::observe(Some(root.pid), Some(&root.token), Some(root.birth))
                    == ProcessState::Alive
            });
            if matches!(state.as_str(), "unhealthy" | "unknown")
                || (state == "ready" && !external && !alive)
            {
                add(
                    findings,
                    "managed_service_unavailable",
                    "error",
                    &component,
                    "service health or process ownership is unresolved",
                );
            } else if state == "ready"
                && checked.is_none_or(|checked| {
                    at - checked
                        > config.services.get(&name).map_or(15.0, |service| {
                            service.monitor_interval_seconds as f64 * 3.0
                        })
                })
            {
                add(
                    findings,
                    "managed_service_health_stale",
                    "warning",
                    &component,
                    "broker health observation is stale",
                );
            } else {
                add(
                    findings,
                    "managed_service_state",
                    "info",
                    &component,
                    if state == "ready" && external {
                        "external service passed its application check; the broker does not own its process"
                    } else if state == "ready" {
                        "service is warm and its process identity is alive"
                    } else {
                        "service is starting or stopping"
                    },
                );
            }
        }
        for name in config.services.keys().filter(|name| !seen.contains(*name)) {
            add(
                findings,
                "managed_service_cold",
                "info",
                &format!("service:{name}"),
                "service starts before the next agent; a cold service is expected while idle",
            );
        }
        Ok(())
    })();
    if inspect.is_err() {
        add(
            findings,
            "managed_service_state_invalid",
            "error",
            "services",
            "service state could not be read",
        );
    }
}

/// Checks every configured provider binding against a WAL-aware, read-only
/// account snapshot without exposing credential references in findings.
fn provider_bindings(
    config: &agent_run_config::provider_config::ProviderConfig,
    home: &Path,
    findings: &mut Vec<Finding>,
) {
    if config.providers.is_empty() {
        return;
    }
    let ids = config
        .providers
        .values()
        .flat_map(|provider| {
            provider
                .bindings
                .iter()
                .map(|binding| binding.account.clone())
        })
        .collect::<BTreeSet<_>>();
    let valid = state::diagnostics::provider_accounts_snapshot(&home.join("state.db"), &ids)
        .and_then(|accounts| config.resolve_catalog(accounts).map(drop));
    if valid.is_err() {
        add(
            findings,
            "provider_bindings_invalid",
            "error",
            "providers",
            "provider bindings do not match the account registry",
        );
    }
}

/// Checks a valid schema-2 config: each harness executable, and an explicitly
/// empty catalog reported as information (nothing can start yet), never as
/// invalid configuration. Returns the shared controls for the remaining
/// common checks.
fn providers(
    v2: &agent_run_config::provider_config::ProviderConfig,
    findings: &mut Vec<Finding>,
) -> Config {
    for (id, harness) in &v2.harnesses {
        if !executable(&harness.binary) {
            add(
                findings,
                "harness_binary_missing",
                "error",
                &format!("harness:{}", id.as_str()),
                harness.binary.display().to_string(),
            );
        }
    }
    if v2.providers.is_empty() {
        add(
            findings,
            "provider_catalog_empty",
            "info",
            "config",
            "no provider is configured; declare harnesses, providers and accounts before starting agents",
        );
    }
    v2.shared()
}

/// Executes the provider-free detached-launch proof used by Python doctor.
///
/// The invoked binary is this executable's hidden `_doctor_canary` command.
/// It proves session identity and READY through the production launch helper,
/// then exits without opening state, materializing an adapter, or contacting a
/// provider.  Launch failures are represented as a bounded type name only.
fn canary(home: &Path, configured_executable: Option<&Path>, findings: &mut Vec<Finding>) {
    let started = Instant::now();
    let executable = match configured_executable {
        Some(path) => path.to_path_buf(),
        None => match std::env::current_exe() {
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
        },
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
        Err(agent_run_platform::launch::LaunchError::Bootstrap(failure)) => add(
            findings,
            failure.failure_kind,
            "error",
            "canary",
            failure.message,
        ),
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

/// Checks static configuration artifacts; configured schema-2 providers
/// require canonical roles even though shared controls have no runtimes.
fn configuration(
    config: &Config,
    home: &Path,
    provider_starts_configured: bool,
    findings: &mut Vec<Finding>,
) {
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
    if provider_starts_configured && !canonical_roles {
        add(
            findings,
            "canonical_role_required",
            "error",
            "profiles",
            "schema-2 provider starts require complete canonical role files",
        );
    }
    let trusted = [
        home.to_path_buf(),
        resolved(&home.join("standalone").join("current")),
    ];
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
        hooks(runtime, &component, &trusted, findings);
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
            display_name: None,
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

/// System interpreters rendered hooks launch scripts with.
///
/// Their own absolute paths sit outside the trusted roots by design, so the
/// trust check for such a hook applies to the script argument instead.
const HOOK_INTERPRETERS: [&str; 4] = ["/usr/bin/python3", "/bin/sh", "/bin/zsh", "/usr/bin/env"];
/// Python options that consume their following argv token before a script path.
const PYTHON_OPTIONS_WITH_VALUE: [&str; 3] = ["-W", "-X", "--check-hash-based-pycs"];
/// Short Python switches that preserve normal script execution when grouped.
const PYTHON_SAFE_FLAG_CHARACTERS: &str = "bBdEiIOPqsuvSx";

/// Expands a single leading `~` against `HOME`, mirroring Python `expanduser`.
///
/// A path without a leading tilde, or a missing `HOME`, is returned unchanged.
fn expanduser(path: &Path) -> PathBuf {
    let text = path.to_string_lossy();
    let Some(rest) = text.strip_prefix('~') else {
        return path.to_path_buf();
    };
    if !rest.is_empty() && !rest.starts_with('/') {
        return path.to_path_buf();
    }
    match std::env::var_os("HOME") {
        Some(home) => PathBuf::from(home).join(rest.trim_start_matches('/')),
        None => path.to_path_buf(),
    }
}

/// Resolves symlinks without requiring the whole path to exist.
///
/// Python's `Path.resolve()` is non-strict, so a trust comparison against a
/// not-yet-materialized script still normalizes its existing ancestors. This
/// canonicalizes the deepest existing prefix and re-appends the remainder;
/// a path with no resolvable ancestor is returned unchanged.
fn resolved(path: &Path) -> PathBuf {
    if let Ok(real) = fs::canonicalize(path) {
        return real;
    }
    let mut suffix = Vec::new();
    let mut cursor = path;
    while let (Some(parent), Some(name)) = (cursor.parent(), cursor.file_name()) {
        suffix.push(name.to_owned());
        if let Ok(mut real) = fs::canonicalize(parent) {
            real.extend(suffix.iter().rev());
            return real;
        }
        cursor = parent;
    }
    path.to_path_buf()
}

/// Returns whether `path` resolves to `root` or somewhere beneath it.
fn under(path: &Path, root: &Path) -> bool {
    resolved(path).starts_with(resolved(root))
}

/// Returns whether a basename is one of Python's `python`, `python3`, `python3.14`.
fn is_python_executable(name: &str) -> bool {
    let Some(rest) = name.strip_prefix("python") else {
        return false;
    };
    let Some(rest) = ({
        if rest.is_empty() {
            return true;
        }
        rest.strip_prefix('3')
    }) else {
        return false;
    };
    if rest.is_empty() {
        return true;
    }
    rest.strip_prefix('.')
        .is_some_and(|digits| !digits.is_empty() && digits.bytes().all(|b| b.is_ascii_digit()))
}

/// Returns uv's existing managed-install root, or `None` when there is none.
///
/// An explicit `UV_PYTHON_INSTALL_DIR` wins; otherwise `XDG_DATA_HOME` and then
/// `~/.local/share` are checked, each suffixed with `uv/python`. A missing or
/// non-directory candidate yields `None`, matching Python's strict resolve.
fn managed_uv_python_root() -> Option<PathBuf> {
    let configured = std::env::var_os("UV_PYTHON_INSTALL_DIR").filter(|value| !value.is_empty());
    let candidate = match configured {
        Some(value) => expanduser(Path::new(&value)),
        None => {
            let base = match std::env::var_os("XDG_DATA_HOME").filter(|value| !value.is_empty()) {
                Some(value) => expanduser(Path::new(&value)),
                None => PathBuf::from(std::env::var_os("HOME")?).join(".local/share"),
            };
            base.join("uv").join("python")
        }
    };
    let root = fs::canonicalize(&candidate).ok()?;
    root.is_dir().then_some(root)
}

/// Returns whether `interpreter` safely introduces a hook script path.
///
/// Fixed system interpreters are accepted directly. A Python executable is
/// accepted only when it has Python's expected basename, sits in a `bin`
/// directory, and resolves beneath `uv_root`. This prevents an arbitrary
/// executable merely named `python3` from changing which argv token is trusted.
fn is_hook_interpreter(interpreter: &Path, uv_root: Option<&Path>) -> bool {
    if HOOK_INTERPRETERS.contains(&interpreter.to_string_lossy().as_ref()) {
        return true;
    }
    let Some(root) = uv_root else {
        return false;
    };
    interpreter
        .parent()
        .and_then(Path::file_name)
        .is_some_and(|name| name == "bin")
        && interpreter
            .file_name()
            .and_then(|name| name.to_str())
            .is_some_and(is_python_executable)
        && under(interpreter, root)
}

/// Returns a Python hook's script path only after safe interpreter options.
///
/// Standalone safe flags, their grouped short form, options taking one value,
/// and `--` before a positional script are recognized. Execution modes (`-c`,
/// `-m`), their attached or grouped forms, and unknown options return
/// `interpreter` so a following word cannot become a trusted script path.
fn python_hook_script(interpreter: &Path, command: &[String]) -> PathBuf {
    let mut index = 1;
    while index < command.len() {
        let word = command[index].as_str();
        if word == "--" {
            return command
                .get(index + 1)
                .map_or_else(|| interpreter.to_path_buf(), |v| expanduser(Path::new(v)));
        }
        if PYTHON_OPTIONS_WITH_VALUE.contains(&word) {
            if index + 1 >= command.len() {
                return interpreter.to_path_buf();
            }
            index += 2;
            continue;
        }
        if word.starts_with("-c") || word.starts_with("-m") {
            return interpreter.to_path_buf();
        }
        if let Some(flags) = word.strip_prefix('-') {
            if !flags.is_empty()
                && flags
                    .chars()
                    .all(|c| PYTHON_SAFE_FLAG_CHARACTERS.contains(c))
            {
                index += 1;
                continue;
            }
            return interpreter.to_path_buf();
        }
        return expanduser(Path::new(word));
    }
    interpreter.to_path_buf()
}

/// Returns the path the hook trust check should evaluate.
///
/// For a plain hook command that is `command[0]`. A recognized system or uv
/// managed Python interpreter launches a script from a later argument, so the
/// interpreter itself is not the artifact whose location matters. An
/// interpreter without a script remains its own untrusted subject.
fn hook_script(command: &[String], uv_root: Option<&Path>) -> PathBuf {
    let interpreter = expanduser(Path::new(&command[0]));
    if !is_hook_interpreter(&interpreter, uv_root) {
        return interpreter;
    }
    if interpreter
        .file_name()
        .and_then(|name| name.to_str())
        .is_some_and(is_python_executable)
    {
        return python_hook_script(&interpreter, command);
    }
    for word in &command[1..] {
        if !word.starts_with('-') {
            return expanduser(Path::new(word));
        }
    }
    interpreter
}

/// Returns a `{plugin:NAME}/...` script token's plugin name, if it is one.
///
/// The token must open the path and be followed by `/` plus at least one more
/// character, matching the Python trust check's anchored pattern.
fn plugin_token(script: &str) -> Option<&str> {
    let rest = script.strip_prefix("{plugin:")?;
    let end = rest.find('}').filter(|end| *end > 0)?;
    let mut tail = rest[end + 1..].chars();
    (tail.next()? == '/' && tail.next().is_some()).then(|| &rest[..end])
}

/// Checks executable and trusted-location evidence for configured hooks.
fn hooks(runtime: &Runtime, component: &str, trusted: &[PathBuf], findings: &mut Vec<Finding>) {
    hooks_with(
        runtime,
        component,
        trusted,
        managed_uv_python_root().as_deref(),
        findings,
    );
}

/// Applies the hook trust policy against one explicit uv managed-install root.
///
/// The executable check always targets `command[0]` -- the interpreter, when
/// there is one -- while the trust check targets [`hook_script`], so a hook
/// running a trusted script through a system interpreter is not flagged merely
/// because that interpreter lives outside `trusted`. A `{plugin:NAME}` token is
/// trusted when NAME is declared for the runtime: adapters expand it beneath
/// the runtime home, so the declaration is the trust evidence. `uv_root` is
/// injectable so this policy is testable without a real uv installation.
fn hooks_with(
    runtime: &Runtime,
    component: &str,
    trusted: &[PathBuf],
    uv_root: Option<&Path>,
    findings: &mut Vec<Finding>,
) {
    let declared = runtime
        .plugins
        .iter()
        .filter_map(|path| path.file_name().and_then(|name| name.to_str()))
        .collect::<std::collections::BTreeSet<_>>();
    for (index, hook) in runtime.hooks.iter().enumerate() {
        let Some(command) = hook.command.first() else {
            continue;
        };
        let item = format!("{component}:hook:{index}");
        let interpreter = expanduser(Path::new(command));
        if !executable(&interpreter) {
            add(
                findings,
                "hook_executable_missing",
                "error",
                &item,
                interpreter.display().to_string(),
            );
        }
        let script = hook_script(&hook.command, uv_root);
        let text = script.display().to_string();
        if plugin_token(&text).is_some_and(|name| declared.contains(name)) {
            continue;
        }
        if !script.is_absolute() || !trusted.iter().any(|root| under(&script, root)) {
            add(findings, "hook_untrusted", "warning", &item, text);
        }
    }
}

/// Checks declared environment and file-link authentication without reading credentials.
fn auth(name: &str, runtime: &Runtime, component: &str, findings: &mut Vec<Finding>) {
    auth_with(
        name,
        runtime,
        component,
        findings,
        |account, service| match account {
            Some(account) => keychain::generic_password(account, service).is_some(),
            None => keychain::generic_password_service(service).is_some(),
        },
    );
}

/// Applies the auth checks against one injectable Keychain presence probe.
///
/// `lookup` receives only fixed Keychain selectors and returns whether a
/// nonempty value could be read; it is consulted only when no declared
/// environment variable is set and the runtime actually has a fallback item,
/// so a runtime without one is never probed. Injecting it keeps this policy
/// testable without touching an operator's Keychain.
fn auth_with(
    name: &str,
    runtime: &Runtime,
    component: &str,
    findings: &mut Vec<Finding>,
    lookup: impl FnOnce(Option<&str>, &str) -> bool,
) {
    match &runtime.auth {
        Some(Auth::Environment { names })
            if !names.iter().any(|name| std::env::var_os(name).is_some())
                && !keychain_present_with(name, lookup) =>
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

/// Applies the fallback catalog to a secret-free credential-presence probe.
///
/// `lookup` receives only fixed Keychain selectors and returns whether a
/// nonempty value could be read.  The injectable probe tests this policy
/// without accessing an operator's Keychain.
fn keychain_present_with(name: &str, lookup: impl FnOnce(Option<&str>, &str) -> bool) -> bool {
    let fallback = match name {
        "glm" => Some((Some("GLM_CODING_KEY"), "com.pluto.agent-run.glm")),
        "claude" => Some((None, "Claude Code-credentials")),
        _ => None,
    };
    fallback.is_some_and(|(account, service)| lookup(account, service))
}

/// Flags capacity samples whose validity window or age has expired.
fn capacity(config: &Config, rows: &[Value], at: f64, findings: &mut Vec<Finding>) {
    let stale_after = config.capacity.collect_interval_seconds.max(1) as f64 * 2.0;
    let mut latest = BTreeSet::new();
    for row in rows {
        let identity = (
            row.get("runtime").and_then(Value::as_str),
            row.get("lane").and_then(Value::as_str),
            row.get("window").and_then(Value::as_str),
            row.get("target").and_then(Value::as_str),
            row.get("source").and_then(Value::as_str),
        );
        if !latest.insert(identity) {
            continue;
        }
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
            if let Some(group) = row.get("process_group_id").and_then(Value::as_i64)
                && process_group_alive(group as i32)
            {
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

/// Reports terminal agent lineages that still have process-owned attempts.
///
/// Reads the current WAL-aware database at `home` and appends findings to `findings`.
/// Components expose validated public root agent ids; details contain bounded counts
/// and allowlisted reasons only. The read-only query never releases ownership or
/// observes, signals, or otherwise changes child processes.
fn terminal_attempt_ownership(home: &Path, findings: &mut Vec<Finding>) {
    let rows = (|| -> rusqlite::Result<Vec<(String, i64, Option<String>)>> {
        let connection = rusqlite::Connection::open_with_flags(
            home.join("state.db"),
            rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY,
        )?;
        connection.pragma_update(None, "query_only", true)?;
        let mut statement = connection.prepare(
                "SELECT COALESCE(NULLIF(a.root_agent_id,''),a.id),COUNT(*),
                    (SELECT json_extract(e.data_json,'$.reason')
                     FROM attempts unresolved
                     JOIN agents ua ON ua.id=unresolved.agent_id
                     JOIN events e ON e.attempt_id=unresolved.id AND e.agent_id=ua.id
                        AND e.kind='attempt_cleanup_unresolved'
                     WHERE unresolved.ownership_active=1
                       AND ua.status IN ('succeeded','failed','cancelled','lost','timed_out')
                       AND COALESCE(NULLIF(ua.root_agent_id,''),ua.id)=COALESCE(NULLIF(a.root_agent_id,''),a.id)
                     ORDER BY e.at DESC,e.seq DESC LIMIT 1)
                 FROM attempts t JOIN agents a ON a.id=t.agent_id
                 WHERE t.ownership_active=1
                   AND a.status IN ('succeeded','failed','cancelled','lost','timed_out')
                 GROUP BY COALESCE(NULLIF(a.root_agent_id,''),a.id)
                 ORDER BY MAX(t.created_at) DESC LIMIT ?",
            )?;
        let rows: Vec<(String, i64, Option<String>)> = statement
            .query_map([LIMIT as i64], |row| {
                Ok((row.get(0)?, row.get(1)?, row.get(2)?))
            })?
            .collect::<rusqlite::Result<_>>()?;
        Ok(rows)
    })();
    let rows = match rows {
        Ok(rows) => rows,
        Err(_) => {
            add(
                findings,
                "terminal_attempt_diagnostics_unavailable",
                "warning",
                "state",
                "cleanup ownership diagnosis unavailable",
            );
            return;
        }
    };
    for (root, count, reason) in rows {
        let root = if root.parse::<agent_run_domain::domain::AgentId>().is_ok() {
            root.as_str()
        } else {
            "unknown"
        };
        let reason = match reason.as_deref() {
            Some("cleanup_unconfirmed") => "cleanup_unconfirmed",
            Some("leader_gone_descendants_unverifiable") => "leader_gone_descendants_unverifiable",
            Some("no_process_evidence") => "no_process_evidence",
            Some("stored_process_identity_mismatch") => "stored_process_identity_mismatch",
            _ => "cleanup_reason_unavailable",
        };
        add(
            findings,
            "terminal_attempt_ownership_unresolved",
            "warning",
            &format!("agent:{root}"),
            format!("owned_attempts={count};last_reason={reason}"),
        );
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

/// Lists process commands and start times using two independently time-bounded `ps` queries.
fn list_mcp_processes() -> Vec<McpProcess> {
    let commands = ps_by_pid(&["/bin/ps", "-A", "-o", "pid=,command="]);
    if commands.is_empty() {
        return Vec::new();
    }
    let starts = ps_by_pid(&["/bin/ps", "-A", "-o", "pid=,lstart="]);
    commands
        .into_iter()
        .map(|(pid, command)| McpProcess {
            pid,
            started_at: starts.get(&pid).and_then(|raw| parse_lstart(raw)),
            command,
        })
        .collect()
}

/// Runs one two-second-bounded process listing and splits only at the leading PID.
fn ps_by_pid(args: &[&str]) -> BTreeMap<i32, String> {
    ps_by_pid_with_timeout(args, Duration::from_secs(2))
}

/// Runs one process listing with a bounded exit and stdout-drain deadline.
fn ps_by_pid_with_timeout(args: &[&str], timeout: Duration) -> BTreeMap<i32, String> {
    let Some(executable) = args.first() else {
        return BTreeMap::new();
    };
    let deadline = Instant::now() + timeout;
    let Ok(mut child) = Command::new(executable)
        .args(&args[1..])
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()
    else {
        return BTreeMap::new();
    };
    loop {
        match child.try_wait() {
            Ok(Some(status)) if status.success() => {
                let Some(stdout) = child.stdout.as_mut() else {
                    return BTreeMap::new();
                };
                let Some(output) = read_until(stdout, deadline, 1024 * 1024) else {
                    let _ = child.kill();
                    let _ = child.wait();
                    return BTreeMap::new();
                };
                return parse_ps_output(&output);
            }
            Ok(Some(_)) | Err(_) => return BTreeMap::new(),
            Ok(None) if Instant::now() >= deadline => {
                let _ = child.kill();
                let _ = child.wait();
                return BTreeMap::new();
            }
            Ok(None) => std::thread::sleep(Duration::from_millis(10)),
        }
    }
}

/// Drains a pipe until EOF within a byte limit and absolute deadline.
/// Overflow, timeout or I/O failure discards the whole captured output.
fn read_until(
    output: &mut (impl Read + AsRawFd),
    deadline: Instant,
    limit: usize,
) -> Option<Vec<u8>> {
    let fd = output.as_raw_fd();
    let mut bytes = Vec::new();
    loop {
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            return None;
        }
        let mut pollfd = libc::pollfd {
            fd,
            events: libc::POLLIN,
            revents: 0,
        };
        let millis = remaining.as_millis().max(1).min(libc::c_int::MAX as u128) as libc::c_int;
        // SAFETY: `pollfd` points to one valid descriptor for this call.
        let ready = unsafe { libc::poll(&mut pollfd, 1, millis) };
        if ready == 0 {
            return None;
        }
        if ready < 0 {
            let error = io::Error::last_os_error();
            if error.kind() == io::ErrorKind::Interrupted {
                continue;
            }
            return None;
        }
        let mut chunk = [0u8; 4096];
        match output.read(&mut chunk) {
            Ok(0) => return Some(bytes),
            Ok(read) if read <= limit.saturating_sub(bytes.len()) => {
                bytes.extend_from_slice(&chunk[..read])
            }
            Ok(_) => return None,
            Err(error) if error.kind() == io::ErrorKind::Interrupted => {}
            Err(_) => return None,
        }
    }
}

/// Parses process-listing bytes after the subprocess has completed.
fn parse_ps_output(output: &[u8]) -> BTreeMap<i32, String> {
    output
        .split(|byte| *byte == b'\n')
        .filter_map(|line| {
            let line = std::str::from_utf8(line).ok()?.trim();
            let (pid, value) = line.split_once(char::is_whitespace)?;
            Some((pid.trim().parse().ok()?, value.trim().to_owned()))
        })
        .collect()
}

/// Parses the platform `ps lstart` representation into Unix seconds.
fn parse_lstart(raw: &str) -> Option<f64> {
    let normalized = raw.split_whitespace().collect::<Vec<_>>().join(" ");
    let parsed = chrono::NaiveDateTime::parse_from_str(&normalized, "%a %b %d %H:%M:%S %Y").ok()?;
    Some(
        chrono::Local
            .from_local_datetime(&parsed)
            .single()?
            .timestamp() as f64,
    )
}

/// Emits release inventory evidence and flags MCP processes older than `current`.
///
/// Process identity alone is never enough to classify a process as MCP: the
/// command line must name both the MCP subcommand and agent-run entry point.
fn mcp_inventory(home: &Path, findings: &mut Vec<Finding>, lister: &dyn Fn() -> Vec<McpProcess>) {
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
    let self_pid = std::process::id() as i32;
    let switch_epoch = fs::symlink_metadata(home.join("standalone/current"))
        .ok()
        .and_then(|metadata| metadata.modified().ok())
        .and_then(|time| time.duration_since(UNIX_EPOCH).ok())
        .map(|duration| duration.as_secs_f64());
    for process in lister().into_iter().take(LIMIT) {
        if process.pid == self_pid || !looks_like_mcp(&process.command) {
            continue;
        }
        let started = process
            .started_at
            .map(|time| time.to_string())
            .unwrap_or_else(|| "unknown".into());
        let release = process
            .command
            .split_whitespace()
            .next()
            .unwrap_or("unknown");
        if switch_epoch.is_some_and(|switch| process.started_at.is_some_and(|start| start < switch))
        {
            add(
                findings,
                "mcp_process_older_release",
                "warning",
                &format!("mcp:{}", process.pid),
                format!(
                    "started={started} release={release}; started before the current release switch and may run older code -- reconnect MCP in this session before pruning releases"
                ),
            );
        } else {
            add(
                findings,
                "mcp_process",
                "info",
                &format!("mcp:{}", process.pid),
                format!("started={started} release={release}"),
            );
        }
    }
}

/// Recognizes only command lines that name the agent-run MCP entry point.
fn looks_like_mcp(command: &str) -> bool {
    let tokens = command.split_whitespace().collect::<Vec<_>>();
    tokens.contains(&"mcp")
        && tokens.iter().any(|token| {
            *token == "agent_run.cli"
                || Path::new(token)
                    .file_name()
                    .is_some_and(|name| name == "agent-run")
        })
}

/// Returns whether a configured path is an absolute executable regular file.
fn executable(path: &Path) -> bool {
    fs::metadata(path)
        .map(|meta| path.is_absolute() && meta.is_file() && meta.permissions().mode() & 0o111 != 0)
        .unwrap_or(false)
}

#[cfg(test)]
mod tests {
    /// Verified releases expose only bounded public metadata; corrupt releases,
    /// missing required tools and absent optional bridges keep distinct outcomes.
    #[test]
    fn diagnostic_metadata_and_tool_absence_are_honest() {
        let home = tempfile::tempdir().unwrap();
        std::fs::create_dir(home.path().join("bin")).unwrap();
        std::fs::write(home.path().join("bin/agent-run"), b"binary").unwrap();
        let metadata = serde_json::json!({"version":"1.2.3","schema_version":25,"private_token":"SECRET_CANARY"}).to_string();
        std::fs::write(home.path().join("metadata.json"), metadata.as_bytes()).unwrap();
        std::fs::write(home.path().join("COMPLETE"), "complete\n").unwrap();
        std::fs::write(
            home.path().join("SHA256SUMS"),
            format!(
                "{}  bin/agent-run\n{}  metadata.json\n",
                agent_run_platform::fs::sha256(b"binary"),
                agent_run_platform::fs::sha256(metadata.as_bytes())
            ),
        )
        .unwrap();
        let sealed = release_summary("current_release", Some(home.path().into()));
        assert_eq!(sealed.check.status, CheckStatus::Ok);
        assert_eq!(sealed.version.as_deref(), Some("1.2.3"));
        assert_eq!(sealed.schema_version, Some(25));
        assert!(
            !serde_json::to_string(&sealed)
                .unwrap()
                .contains("SECRET_CANARY")
        );
        std::fs::write(home.path().join("bin/agent-run"), b"corrupt").unwrap();
        assert_eq!(
            release_summary("current_release", Some(home.path().into()))
                .check
                .status,
            CheckStatus::Failed
        );
        let missing = home.path().join("missing");
        assert_eq!(
            tool_summary("required".into(), Some(&missing), true, false).status,
            CheckStatus::Failed
        );
        assert_eq!(
            tool_summary("host_node_bridge".into(), None, false, true).status,
            CheckStatus::NotChecked
        );
        let report = Report::default();
        assert_eq!(report.diagnostics.version, env!("CARGO_PKG_VERSION"));
        assert_eq!(report.diagnostics.target.arch, std::env::consts::ARCH);
        assert_eq!(report.diagnostics.build.source_commit, None);
        assert_eq!(
            report.diagnostics.resident_compatibility.status,
            CheckStatus::NotChecked
        );
        assert_eq!(numeric_version(b"token=SECRET_CANARY"), None);
        let entries: serde_json::Map<String, Value> = (0..=LIMIT)
            .map(|n| {
                (
                    format!("missing-{n:04}"),
                    json!({"command":missing,"args":[],"transport":"stdio"}),
                )
            })
            .collect();
        let config: Config =
            serde_json::from_value(json!({"schema_version":1,"mcp":entries})).unwrap();
        let tools = diagnostic_tools(&config, None);
        assert_eq!(tools.len(), LIMIT + 2, "no configured row may disappear");
        assert!(
            tools[..=LIMIT]
                .iter()
                .all(|tool| tool.status == CheckStatus::Failed)
        );
        let mut report = Report::default();
        report.diagnostics.tools = tools;
        assert!(!report.ok(), "missing dependencies beyond row256 must fail");
    }

    /// Writes a private executable probe fixture; callers supply only fixed shell
    /// bodies for local version/timeout checks, never model or authentication commands.
    fn version_fixture(root: &Path, body: &str) -> PathBuf {
        let binary = root.join("probe");
        std::fs::write(&binary, format!("#!/bin/sh\n{body}\n")).unwrap();
        std::fs::set_permissions(&binary, std::fs::Permissions::from_mode(0o700)).unwrap();
        binary
    }

    /// Version probes issue only --version, omit stderr secrets, refuse oversized
    /// output, and kill/reap a timeout without signalling an unrelated process group.
    #[test]
    fn version_probes_are_bounded_secret_safe_and_reaped() {
        let home = tempfile::tempdir().unwrap();
        let probe = version_fixture(
            home.path(),
            "[ \"$1\" = '--version' ] || exit 9\nprintf 'tool 1.2.3\\n'\nprintf 'SECRET_CANARY' >&2",
        );
        assert_eq!(tool_version(&probe).as_deref(), Some("1.2.3"));
        let probe = version_fixture(
            home.path(),
            "i=0; while [ \"$i\" -lt 1000 ]; do printf 'abcdefghij'; i=$((i+1)); done",
        );
        assert_eq!(tool_version(&probe), None);
        let pid_file = home.path().join("pid");
        let probe = version_fixture(
            home.path(),
            &format!(
                "printf '%s' \"$$\" > '{}'\nexec /bin/sleep 30",
                pid_file.display()
            ),
        );
        let started = Instant::now();
        assert_eq!(tool_version(&probe), None);
        assert!(started.elapsed() < Duration::from_secs(5));
        let pid: i32 = std::fs::read_to_string(pid_file).unwrap().parse().unwrap();
        assert!(process::inspect(pid).is_err(), "owned probe must be reaped");
    }

    use super::*;
    use serde_json::json;
    use std::cell::Cell;

    /// Doctor reports only unresolved terminal ownership, grouped by stable lineage root.
    #[test]
    fn terminal_owned_attempt_diagnostics_are_read_only_and_lineage_scoped() {
        let home = tempfile::tempdir().unwrap();
        let store = state::Store::initialize(home.path()).unwrap();
        for (id, status, parent, root) in [
            (
                "ag-20260101-000000-0000000001",
                "succeeded",
                None,
                "ag-20260101-000000-0000000001",
            ),
            (
                "ag-20260101-000000-0000000002",
                "succeeded",
                Some("ag-20260101-000000-0000000001"),
                "ag-20260101-000000-0000000001",
            ),
            (
                "ag-20260101-000000-0000000003",
                "running",
                None,
                "ag-20260101-000000-0000000003",
            ),
            (
                "ag-20260101-000000-0000000004",
                "failed",
                None,
                "ag-20260101-000000-0000000004",
            ),
        ] {
            store.conn.execute(
                "INSERT INTO agents(id,runtime,model,profile,task,task_summary,workdir,request_json,status,created_at,timeout_seconds,config_revision,parent_agent_id,root_agent_id) VALUES(?, 'fixture','model','','private-task-sentinel','', '/tmp','{}',?,1,60,'',?,?)",
                rusqlite::params![id, status, parent, root],
            ).unwrap();
        }
        store
            .conn
            .execute(
                "UPDATE agents SET sequence=2 WHERE id='ag-20260101-000000-0000000002'",
                [],
            )
            .unwrap();
        for (id, agent, active) in [
            ("terminal-owned", "ag-20260101-000000-0000000002", 1),
            ("active-owned", "ag-20260101-000000-0000000003", 1),
            ("terminal-released", "ag-20260101-000000-0000000004", 0),
        ] {
            store.conn.execute(
                "INSERT INTO attempts(id,agent_id,number,state,adapter_state_json,created_at,ownership_active) VALUES(?,?,1,'failed','{}',1,?)",
                rusqlite::params![id, agent, active],
            ).unwrap();
        }
        store.conn.execute(
            "INSERT INTO events(agent_id,attempt_id,at,kind,data_json) VALUES('ag-20260101-000000-0000000002','terminal-owned',2,'attempt_cleanup_unresolved',?)",
            [json!({"reason":"leader_gone_descendants_unverifiable","debug":"private-event-sentinel"}).to_string()],
        ).unwrap();
        let before: (i64, i64) = store.conn.query_row(
            "SELECT SUM(ownership_active),(SELECT COUNT(*) FROM attempt_quota_keys) FROM attempts",
            [], |row| Ok((row.get(0)?, row.get(1)?)),
        ).unwrap();
        let version: i64 = store
            .conn
            .pragma_query_value(None, "data_version", |row| row.get(0))
            .unwrap();

        let mut findings = Vec::new();
        terminal_attempt_ownership(home.path(), &mut findings);

        assert_eq!(findings.len(), 1);
        assert_eq!(findings[0].code, "terminal_attempt_ownership_unresolved");
        assert_eq!(findings[0].severity, "warning");
        assert_eq!(findings[0].component, "agent:ag-20260101-000000-0000000001");
        assert_eq!(
            findings[0].detail,
            "owned_attempts=1;last_reason=leader_gone_descendants_unverifiable"
        );
        let printed = serde_json::to_string(&findings).unwrap();
        assert!(!printed.contains("terminal-owned"));
        assert!(!printed.contains("private-task-sentinel"));
        assert!(!printed.contains("private-event-sentinel"));
        let after: (i64, i64) = store.conn.query_row(
            "SELECT SUM(ownership_active),(SELECT COUNT(*) FROM attempt_quota_keys) FROM attempts",
            [], |row| Ok((row.get(0)?, row.get(1)?)),
        ).unwrap();
        let after_version: i64 = store
            .conn
            .pragma_query_value(None, "data_version", |row| row.get(0))
            .unwrap();
        assert_eq!(before, after);
        assert_eq!(version, after_version);
    }

    /// Cold services are normal, missing ownership is an error, and diagnostics never expose commands or write state.
    #[test]
    fn service_diagnostics_are_read_only_and_secret_safe() {
        let home = tempfile::tempdir().unwrap();
        state::Store::initialize(home.path()).unwrap();
        let config = agent_run_config::provider_config::ProviderConfig::parse(
            r#"schema_version=2
[services.hot]
command="/bin/sleep"
args=["sensitive-argument"]
cwd="/tmp"
env_from=["PRIVATE_SERVICE_TOKEN"]
readiness={command="/bin/true"}
"#,
            home.path(),
        )
        .unwrap();
        let mut findings = Vec::new();
        managed_services(&config, home.path(), 100.0, &mut findings);
        assert_eq!(findings[0].code, "managed_service_cold");
        assert_eq!(findings[0].severity, "info");
        let mut store = state::Store::open(home.path()).unwrap();
        let owner = process::OwnedProcess::capture(std::process::id() as i32);
        let root = owner.leader.as_ref().unwrap();
        store.conn.execute("INSERT INTO managed_service_generations(id,service_id,revision,definition_json,state,broker_identity_json,process_identity_json,created_at,checked_at) VALUES ('fixture','hot',?1,?2,'ready',?3,?3,1,100)",rusqlite::params![config.services["hot"].revision().unwrap(),serde_json::to_string(&config.services["hot"]).unwrap(),serde_json::to_string(root).unwrap()]).unwrap();
        findings.clear();
        managed_services(&config, home.path(), 100.0, &mut findings);
        assert_eq!(findings[0].code, "managed_service_unavailable");
        store
            .remember_processes("service", "fixture", &owner.snapshot().unwrap())
            .unwrap();
        let revision: i64 = store
            .conn
            .pragma_query_value(None, "data_version", |row| row.get(0))
            .unwrap();
        findings.clear();
        managed_services(&config, home.path(), 100.0, &mut findings);
        assert_eq!(findings[0].severity, "info");
        let after: i64 = store
            .conn
            .pragma_query_value(None, "data_version", |row| row.get(0))
            .unwrap();
        assert_eq!(revision, after);
        let printed = serde_json::to_string(&findings).unwrap();
        assert!(!printed.contains("sensitive-argument"));
        assert!(!printed.contains("PRIVATE_SERVICE_TOKEN"));
        findings.clear();
        managed_services(&config, home.path(), 200.0, &mut findings);
        assert_eq!(findings[0].code, "managed_service_health_stale");
    }

    /// A declared auth variable guaranteed absent, standing in for Python's
    /// `mock.patch.dict(os.environ, {}, clear=True)`: Rust tests share one
    /// process environment, so a name that is never set is the deterministic
    /// equivalent of clearing it.
    const ABSENT_AUTH_NAME: &str = "AGENT_RUN_DOCTOR_ABSENT_TEST_KEY";

    /// Mirrors `tests/test_state_migrations.py::MigrationDiagnosticsTests::test_doctor_surfaces_a_pending_migration`.
    #[test]
    fn python_doctor_surfaces_a_pending_migration() {
        let home = tempfile::tempdir().expect("temporary home");
        std::fs::write(home.path().join("config.toml"), "schema_version = 1\n").expect("config");
        let connection = rusqlite::Connection::open(home.path().join("state.db")).unwrap();
        connection
            .execute_batch(include_str!(
                "../../agent-run-store/tests/fixtures/schema_v1.sql"
            ))
            .unwrap();
        connection.pragma_update(None, "user_version", 1).unwrap();

        let report = run(home.path()).expect("doctor report");
        let findings: Vec<_> = report
            .findings
            .iter()
            .filter(|finding| finding.component == "state")
            .collect();
        assert_eq!(findings.len(), 1);
        assert_eq!(findings[0].code, "state_migration_pending");
        assert!(
            findings[0]
                .detail
                .contains(&format!("expected v{}", crate::state::VERSION))
        );
        assert!(!report.ok());
    }

    /// Configured schema-2 providers require canonical roles; a deliberately
    /// empty catalog remains clean until a provider can actually start.
    #[test]
    fn provider_doctor_requires_canonical_roles() {
        let home = tempfile::tempdir().unwrap();
        let profiles = home.path().join("profiles");
        std::fs::create_dir(&profiles).unwrap();
        std::fs::write(home.path().join("config.toml"), "schema_version = 2\n").unwrap();
        let role = profiles.join("review.md");
        std::fs::write(&role, "+++\nwrite = false\n+++\nReview.\n").unwrap();
        let report = run(home.path()).unwrap();
        assert!(
            !report
                .findings
                .iter()
                .any(|finding| finding.code == "canonical_role_required")
        );
        std::fs::write(
            home.path().join("config.toml"),
            format!(
                "schema_version = 2\n[harnesses.codex]\nbinary = '/bin/true'\nhome = '{0}/codex'\n[harnesses.claude-code]\nbinary = '/bin/true'\nhome = '{0}/claude'\n[providers.codex]\nharness = 'codex'\nconnection = {{ kind = 'native' }}\nauth_family = 'openai'\nlimits_source = 'none'\n[[providers.codex.models]]\nid = 'gpt'\n[[providers.codex.bindings]]\nlabel = 'global'\naccount = 'acct'\n",
                home.path().display()
            ),
        )
        .unwrap();
        let report = run(home.path()).unwrap();
        assert!(
            report
                .findings
                .iter()
                .any(|finding| finding.code == "canonical_role_required")
        );
        std::fs::write(
            &role,
            "+++\nrevision = 'v2'\nwrite = false\nnetwork = false\nallow_external_read_roots = true\nskills = []\nmcp = []\nrequired_constraints = []\n+++\nReview.\n",
        )
        .unwrap();
        let report = run(home.path()).unwrap();
        assert!(
            !report
                .findings
                .iter()
                .any(|finding| finding.code == "canonical_role_required")
        );
    }

    /// Builds one enabled runtime fixture from Python-equivalent config fields.
    fn runtime_fixture(extra: serde_json::Value) -> Runtime {
        let mut value = json!({
            "enabled": true,
            "adapter": "example:ADAPTER",
            "binary": "/usr/bin/true",
            "home": "/tmp",
            "models": ["model"],
        });
        let (Some(base), Some(extra)) = (value.as_object_mut(), extra.as_object()) else {
            panic!("runtime fixture takes a JSON object");
        };
        base.extend(extra.iter().map(|(k, v)| (k.clone(), v.clone())));
        serde_json::from_value(value).expect("runtime fixture deserializes")
    }

    /// Collects auth findings for one runtime name against an injected probe.
    fn auth_findings(name: &str, lookup: impl FnOnce(Option<&str>, &str) -> bool) -> Vec<Finding> {
        let runtime = runtime_fixture(json!({
            "auth": {"kind": "environment", "names": [ABSENT_AUTH_NAME]},
        }));
        let mut findings = Vec::new();
        auth_with(
            name,
            &runtime,
            &format!("runtime:{name}"),
            &mut findings,
            lookup,
        );
        findings
    }

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

    /// Mirrors `test_doctor.py::KeychainFallbackAuthTests::test_a_runtime_without_a_fallback_is_never_probed`.
    #[test]
    fn python_doctor_runtime_without_a_fallback_is_never_probed() {
        let probed = Cell::new(false);
        let findings = auth_findings("codex", |_, _| {
            probed.set(true);
            true
        });
        assert_eq!(
            findings
                .iter()
                .map(|finding| finding.code.as_str())
                .collect::<Vec<_>>(),
            ["auth_environment_missing"]
        );
        assert!(
            !probed.get(),
            "a runtime with no fallback must not be probed"
        );
    }

    /// Mirrors `test_doctor.py::KeychainFallbackAuthTests::test_a_failing_probe_counts_as_absent`.
    ///
    /// Python raises `OSError` from `subprocess.run`; the Rust probe reports an
    /// unusable Keychain as `false`, which is the same "not present" outcome.
    #[test]
    fn python_doctor_failing_keychain_probe_counts_as_absent() {
        assert_eq!(
            auth_findings("glm", |_, _| false)
                .iter()
                .map(|finding| finding.code.as_str())
                .collect::<Vec<_>>(),
            ["auth_environment_missing"]
        );
    }

    /// Mirrors `test_doctor.py::KeychainFallbackAuthTests::test_an_item_without_a_fixed_account_keeps_the_warning_when_absent`.
    #[test]
    fn python_doctor_account_free_keychain_item_keeps_the_warning_when_absent() {
        let findings = auth_findings("claude", |_, _| false);
        assert_eq!(
            findings
                .iter()
                .map(|finding| finding.code.as_str())
                .collect::<Vec<_>>(),
            ["auth_environment_missing"]
        );
        assert_eq!(findings[0].severity, "warning");
    }

    /// Builds one capacity snapshot row shaped like Python's `_row`.
    fn capacity_row(lane: &str, observed_at: f64, valid_until: Option<f64>) -> Value {
        json!({
            "runtime": "fixture",
            "lane": lane,
            "window": "5h",
            "target": null,
            "source": "omniroute",
            "observed_at": observed_at,
            "valid_until": valid_until,
        })
    }

    /// Returns the stale lanes for `rows`, mirroring Python's `_lanes`.
    fn capacity_lanes(rows: &[Value]) -> Vec<String> {
        let config: Config =
            serde_json::from_value(json!({"schema_version": 1})).expect("default config");
        let mut findings = Vec::new();
        capacity(&config, rows, 1_000., &mut findings);
        findings
            .iter()
            .map(|finding| {
                finding
                    .detail
                    .split('/')
                    .next()
                    .unwrap_or_default()
                    .to_owned()
            })
            .collect()
    }

    /// Mirrors `test_doctor.py::CapacityStalenessTests::test_a_sample_still_within_its_validity_is_never_stale`.
    #[test]
    fn python_doctor_sample_within_its_validity_is_never_stale() {
        assert!(capacity_lanes(&[capacity_row("fresh", 100., Some(2_000.))]).is_empty());
    }

    /// Mirrors `test_doctor.py::CapacityStalenessTests::test_an_expired_sample_is_stale_even_when_recently_observed`.
    #[test]
    fn python_doctor_expired_sample_is_stale_even_when_recently_observed() {
        assert_eq!(
            capacity_lanes(&[capacity_row("expired", 999., Some(500.))]),
            ["expired"]
        );
    }

    /// Mirrors `test_doctor.py::CapacityStalenessTests::test_a_sample_without_validity_keeps_the_age_bound`.
    #[test]
    fn python_doctor_sample_without_validity_keeps_the_age_bound() {
        assert_eq!(
            capacity_lanes(&[
                capacity_row("aged", 100., None),
                capacity_row("recent", 900., None),
            ]),
            ["aged"]
        );
    }

    /// Keeps only the newest row for each capacity identity before staleness checks.
    /// Mirrors `tests/test_capacity_diagnostics.py::CapacityDiagnosticTests::test_frequent_healthy_samples_do_not_hide_stale_identity`.
    #[test]
    fn capacity_staleness_deduplicates_identity_before_reporting() {
        assert!(
            capacity_lanes(&[
                capacity_row("same", 900., Some(2_000.)),
                capacity_row("same", 100., Some(500.)),
            ])
            .is_empty()
        );
    }

    /// Creates Python `HookTrustTests.setUp`'s resolved home and install root.
    fn hook_home() -> (tempfile::TempDir, PathBuf, PathBuf) {
        let temp = tempfile::tempdir().expect("temporary home");
        let home = temp.path().canonicalize().expect("resolved home");
        let root = home.join("install");
        fs::create_dir_all(root.join("hooks")).expect("hook root");
        (temp, home, root)
    }

    /// Collects hook findings exactly as Python's `HookTrustTests._findings`.
    fn hook_findings(
        root: &Path,
        home: &Path,
        command: &[&str],
        plugins: &[&str],
        uv_root: Option<&Path>,
    ) -> Vec<Finding> {
        let runtime = runtime_fixture(json!({
            "home": home,
            "hooks": [{"event": "PostToolUse", "command": command}],
            "plugins": plugins,
        }));
        let trusted = [
            root.to_path_buf(),
            resolved(&root.join("standalone").join("current")),
        ];
        let mut findings = Vec::new();
        hooks_with(&runtime, "runtime:claude", &trusted, uv_root, &mut findings);
        findings
    }

    /// Returns only the hook finding codes, mirroring Python's `_codes`.
    fn hook_codes(
        root: &Path,
        home: &Path,
        command: &[&str],
        plugins: &[&str],
        uv_root: Option<&Path>,
    ) -> Vec<String> {
        hook_findings(root, home, command, plugins, uv_root)
            .into_iter()
            .map(|finding| finding.code)
            .collect()
    }

    /// Creates an executable empty file, mirroring Python's `touch(mode=0o700)`.
    fn touch_executable(path: &Path) {
        fs::create_dir_all(path.parent().expect("executable parent")).expect("executable parent");
        fs::write(path, "").expect("executable file");
        fs::set_permissions(path, fs::Permissions::from_mode(0o700)).expect("executable mode");
    }

    /// The plugin-expanded hook script both trust directions are judged on.
    const PLUGIN_SCRIPT: &str = "{plugin:agent-lsp-plugin}/hooks/guard.py";
    /// A declared plugin whose location is deliberately outside every root.
    const PLUGIN_DECLARATION: &str = "/anywhere/agent-lsp-plugin";

    /// Mirrors `test_doctor.py::HookTrustTests::test_a_script_inside_the_trusted_roots_is_trusted`.
    #[test]
    fn python_doctor_script_inside_the_trusted_roots_is_trusted() {
        let (_temp, home, root) = hook_home();
        let script = root.join("hooks/context.py");
        fs::write(&script, "pass\n").expect("hook script");
        let script = script.to_string_lossy().into_owned();
        assert!(
            hook_codes(
                &root,
                &home,
                &["/usr/bin/python3", &script, "--event", "PostToolUse"],
                &[],
                None,
            )
            .is_empty()
        );
    }

    /// Mirrors `test_doctor.py::HookTrustTests::test_a_script_outside_the_trusted_roots_is_untrusted`.
    #[test]
    fn python_doctor_script_outside_the_trusted_roots_is_untrusted() {
        let (_temp, home, root) = hook_home();
        let script = home.join("outside/context.py").display().to_string();
        let findings = hook_findings(&root, &home, &["/usr/bin/python3", &script], &[], None);
        assert_eq!(
            findings
                .iter()
                .map(|finding| finding.code.as_str())
                .collect::<Vec<_>>(),
            ["hook_untrusted"]
        );
        assert_eq!(findings[0].severity, "warning");
        assert_eq!(findings[0].detail, script);
    }

    /// Mirrors `test_doctor.py::HookTrustTests::test_an_interpreter_without_a_script_stays_untrusted`.
    #[test]
    fn python_doctor_interpreter_without_a_script_stays_untrusted() {
        let (_temp, home, root) = hook_home();
        assert_eq!(
            hook_codes(&root, &home, &["/usr/bin/python3"], &[], None),
            ["hook_untrusted"]
        );
        assert_eq!(
            hook_codes(&root, &home, &["/bin/sh", "-c", "echo hi"], &[], None),
            ["hook_untrusted"]
        );
        assert_eq!(
            hook_findings(&root, &home, &["/bin/sh", "-c", "echo hi"], &[], None)
                .iter()
                .map(|finding| finding.detail.as_str())
                .collect::<Vec<_>>(),
            ["echo hi"]
        );
    }

    /// Mirrors `test_doctor.py::HookTrustTests::test_a_declared_plugin_token_is_trusted`.
    #[test]
    fn python_doctor_declared_plugin_token_is_trusted() {
        let (_temp, home, root) = hook_home();
        assert!(
            hook_codes(
                &root,
                &home,
                &["/usr/bin/python3", PLUGIN_SCRIPT],
                &[PLUGIN_DECLARATION],
                None,
            )
            .is_empty()
        );
    }

    /// Mirrors `test_doctor.py::HookTrustTests::test_an_undeclared_plugin_token_stays_untrusted`.
    #[test]
    fn python_doctor_undeclared_plugin_token_stays_untrusted() {
        let (_temp, home, root) = hook_home();
        assert_eq!(
            hook_codes(
                &root,
                &home,
                &["/usr/bin/python3", PLUGIN_SCRIPT],
                &[],
                None
            ),
            ["hook_untrusted"]
        );
    }

    /// Mirrors `test_doctor.py::HookTrustTests::test_uv_managed_python_trusts_a_declared_plugin_script`.
    #[test]
    fn python_doctor_uv_managed_python_trusts_a_declared_plugin_script() {
        let (_temp, home, root) = hook_home();
        let install = home.join("uv/python");
        let interpreter = install.join("cpython-3.14/bin/python3.14");
        touch_executable(&interpreter);
        assert!(
            hook_codes(
                &root,
                &home,
                &[&interpreter.to_string_lossy(), PLUGIN_SCRIPT],
                &[PLUGIN_DECLARATION],
                Some(&install),
            )
            .is_empty()
        );
    }

    /// Mirrors `test_doctor.py::HookTrustTests::test_an_arbitrary_python_named_program_does_not_trust_its_argument`.
    #[test]
    fn python_doctor_arbitrary_python_named_program_does_not_trust_its_argument() {
        let (_temp, home, root) = hook_home();
        let interpreter = home.join("outside/bin/python3.14");
        touch_executable(&interpreter);
        let script = root.join("hooks/context.py");
        fs::write(&script, "pass\n").expect("hook script");
        let findings = hook_findings(
            &root,
            &home,
            &[&interpreter.to_string_lossy(), &script.to_string_lossy()],
            &[],
            Some(&home.join("uv/python")),
        );
        assert_eq!(
            findings
                .iter()
                .map(|finding| finding.code.as_str())
                .collect::<Vec<_>>(),
            ["hook_untrusted"]
        );
        assert_eq!(findings[0].detail, interpreter.display().to_string());
    }

    /// Mirrors `test_doctor.py::HookTrustTests::test_python_flag_operands_are_not_trusted_as_scripts`.
    #[test]
    fn python_doctor_python_flag_operands_are_not_trusted_as_scripts() {
        let (_temp, home, root) = hook_home();
        let install = home.join("uv/python");
        let interpreter = install.join("cpython-3.14/bin/python3.14");
        touch_executable(&interpreter);
        let script = root.join("hooks/context.py");
        fs::write(&script, "pass\n").expect("hook script");
        let findings = hook_findings(
            &root,
            &home,
            &[
                &interpreter.to_string_lossy(),
                "-c",
                &script.to_string_lossy(),
            ],
            &[],
            Some(&install),
        );
        assert_eq!(
            findings
                .iter()
                .map(|finding| finding.code.as_str())
                .collect::<Vec<_>>(),
            ["hook_untrusted"]
        );
        assert_eq!(findings[0].detail, interpreter.display().to_string());
    }

    /// Mirrors `test_doctor.py::HookTrustTests::test_attached_or_grouped_python_execution_modes_stay_untrusted`.
    #[test]
    fn python_doctor_attached_or_grouped_python_execution_modes_stay_untrusted() {
        let (_temp, home, root) = hook_home();
        let install = home.join("uv/python");
        let interpreter = install.join("cpython-3.14/bin/python3.14");
        touch_executable(&interpreter);
        for mode in ["-cprint(1)", "-mmodule", "-Icprint(1)"] {
            let findings = hook_findings(
                &root,
                &home,
                &[&interpreter.to_string_lossy(), mode, PLUGIN_SCRIPT],
                &[],
                Some(&install),
            );
            assert_eq!(
                findings
                    .iter()
                    .map(|finding| finding.code.as_str())
                    .collect::<Vec<_>>(),
                ["hook_untrusted"],
                "mode {mode}"
            );
            assert_eq!(
                findings[0].detail,
                interpreter.display().to_string(),
                "mode {mode}"
            );
        }
    }

    /// Mirrors `test_doctor.py::HookTrustTests::test_grouped_safe_python_flags_keep_the_plugin_script_trusted`.
    #[test]
    fn python_doctor_grouped_safe_python_flags_keep_the_plugin_script_trusted() {
        let (_temp, home, root) = hook_home();
        let install = home.join("uv/python");
        let interpreter = install.join("cpython-3.14/bin/python3.14");
        touch_executable(&interpreter);
        assert!(
            hook_codes(
                &root,
                &home,
                &[&interpreter.to_string_lossy(), "-EsS", PLUGIN_SCRIPT],
                &[PLUGIN_DECLARATION],
                Some(&install),
            )
            .is_empty()
        );
    }

    // Protects doctor from a process-listing descendant that inherits stdout.
    #[test]
    fn ps_probe_bounds_stdout_drain() {
        let started = Instant::now();
        let result = ps_by_pid_with_timeout(
            &[
                "/bin/sh",
                "-c",
                "tail -f /dev/null & holder=$!; (sleep 1; kill $holder) & exit 0",
            ],
            Duration::from_millis(50),
        );
        assert!(result.is_empty());
        assert!(started.elapsed() < Duration::from_millis(500));
    }
}
