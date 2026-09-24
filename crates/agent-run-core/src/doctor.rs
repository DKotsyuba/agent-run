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
    run_with(home, &Dependencies::default())
}

/// Runs doctor with injectable canary and process-inventory probes.
pub fn run_with(home: &Path, dependencies: &Dependencies) -> Result<Report> {
    let home = home.to_path_buf();
    let mut report = Report {
        home,
        checked_at: now()?,
        findings: Vec::new(),
    };
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
            return Ok(report);
        }
    };
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
            return Ok(report);
        }
    };
    if let Some(config) = &provider_config {
        provider_bindings(config, &report.home, &mut report.findings);
    }
    capacity(
        &config,
        &snapshot.capacity,
        report.checked_at,
        &mut report.findings,
    );
    supervisors(&snapshot.agents, &mut report.findings);
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
    Ok(report)
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
                let Some(output) = read_until(stdout, deadline) else {
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

/// Drains one process-listing pipe until EOF or the listing deadline.
fn read_until(output: &mut (impl Read + AsRawFd), deadline: Instant) -> Option<Vec<u8>> {
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
            Ok(read) => bytes.extend_from_slice(&chunk[..read]),
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
                format!("started={started} release={release}; started before the current release switch and may run older code -- reconnect MCP in this session before pruning releases"),
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
    use super::*;
    use serde_json::json;
    use std::cell::Cell;

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
        assert!(findings[0]
            .detail
            .contains(&format!("expected v{}", crate::state::VERSION)));
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
        assert!(!report
            .findings
            .iter()
            .any(|finding| finding.code == "canonical_role_required"));
        std::fs::write(
            home.path().join("config.toml"),
            format!(
                "schema_version = 2\n[harnesses.codex]\nbinary = '/bin/true'\nhome = '{0}/codex'\n[harnesses.claude-code]\nbinary = '/bin/true'\nhome = '{0}/claude'\n[providers.codex]\nharness = 'codex'\nconnection = {{ kind = 'native' }}\nauth_family = 'openai'\nlimits_source = 'codex_appserver'\n[[providers.codex.models]]\nid = 'gpt'\n[[providers.codex.bindings]]\nlabel = 'global'\naccount = 'acct'\n",
                home.path().display()
            ),
        )
        .unwrap();
        let report = run(home.path()).unwrap();
        assert!(report
            .findings
            .iter()
            .any(|finding| finding.code == "canonical_role_required"));
        std::fs::write(
            &role,
            "+++\nrevision = 'v2'\nwrite = false\nnetwork = false\nallow_external_read_roots = true\nskills = []\nmcp = []\nrequired_constraints = []\n+++\nReview.\n",
        )
        .unwrap();
        let report = run(home.path()).unwrap();
        assert!(!report
            .findings
            .iter()
            .any(|finding| finding.code == "canonical_role_required"));
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
        assert!(capacity_lanes(&[
            capacity_row("same", 900., Some(2_000.)),
            capacity_row("same", 100., Some(500.)),
        ])
        .is_empty());
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
        assert!(hook_codes(
            &root,
            &home,
            &["/usr/bin/python3", &script, "--event", "PostToolUse"],
            &[],
            None,
        )
        .is_empty());
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
        assert!(hook_codes(
            &root,
            &home,
            &["/usr/bin/python3", PLUGIN_SCRIPT],
            &[PLUGIN_DECLARATION],
            None,
        )
        .is_empty());
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
        assert!(hook_codes(
            &root,
            &home,
            &[&interpreter.to_string_lossy(), PLUGIN_SCRIPT],
            &[PLUGIN_DECLARATION],
            Some(&install),
        )
        .is_empty());
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
        assert!(hook_codes(
            &root,
            &home,
            &[&interpreter.to_string_lossy(), "-EsS", PLUGIN_SCRIPT],
            &[PLUGIN_DECLARATION],
            Some(&install),
        )
        .is_empty());
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
