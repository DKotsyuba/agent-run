//! Strict, credential-free configuration; legacy packaged adapter names remain accepted.
use agent_run_domain::{domain, error::invalid, Result};
use agent_run_platform::fs;
use serde::{Deserialize, Serialize};
use std::{
    collections::{BTreeMap, BTreeSet},
    path::{Path, PathBuf},
};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Adapter {
    Codex,
    Claude,
    Glm,
    Qwen,
}
impl Adapter {
    pub fn parse(s: &str) -> Result<Self> {
        match s {
            "codex"
            | "agent_run.adapters.codex:ADAPTER"
            | "agent_run.adapters.codex.adapter:ADAPTER" => Ok(Self::Codex),
            "claude"
            | "agent_run.adapters.claude:ADAPTER"
            | "agent_run.adapters.claude.adapter:ADAPTER" => Ok(Self::Claude),
            "glm" | "agent_run.adapters.glm:ADAPTER" | "agent_run.adapters.glm.adapter:ADAPTER" => {
                Ok(Self::Glm)
            }
            "qwen"
            | "agent_run.adapters.qwen:ADAPTER"
            | "agent_run.adapters.qwen.adapter:ADAPTER" => Ok(Self::Qwen),
            _ => Err(invalid(
                "unknown adapter; Rust builds accept packaged codex/claude/glm/qwen adapters only",
            )),
        }
    }
    pub fn name(self) -> &'static str {
        match self {
            Self::Codex => "codex",
            Self::Claude => "claude",
            Self::Glm => "glm",
            Self::Qwen => "qwen",
        }
    }
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    pub schema_version: u32,
    #[serde(default)]
    pub core: Core,
    #[serde(default)]
    pub capacity: Capacity,
    #[serde(default)]
    pub delivery: Delivery,
    #[serde(default)]
    pub profiles: Catalog,
    #[serde(default)]
    pub skills: Catalog,
    #[serde(default)]
    pub mcp: BTreeMap<String, Mcp>,
    #[serde(default)]
    pub environments: BTreeMap<String, Environment>,
    #[serde(default)]
    pub runtimes: BTreeMap<String, Runtime>,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct Core {
    pub default_timeout_seconds: f64,
    pub max_active_agents: usize,
    pub warning_fraction: f64,
    pub stalled_after_seconds: f64,
}
impl Default for Core {
    fn default() -> Self {
        Self {
            default_timeout_seconds: 480.,
            max_active_agents: 6,
            warning_fraction: 0.9,
            stalled_after_seconds: 900.,
        }
    }
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct Capacity {
    pub collect_interval_seconds: u64,
    pub sample_retention: usize,
    /// Maximum host-injected context characters; zero disables context injection.
    pub context_max_chars: usize,
    pub codexbar_binary: PathBuf,
}
impl Default for Capacity {
    fn default() -> Self {
        Self {
            collect_interval_seconds: 300,
            sample_retention: 1000,
            context_max_chars: 2500,
            codexbar_binary: "/opt/homebrew/bin/codexbar".into(),
        }
    }
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct Delivery {
    pub retry_base_seconds: f64,
    pub retry_cap_seconds: f64,
    pub max_attempts: u32,
    pub codex_queue_bin: Option<PathBuf>,
}
impl Default for Delivery {
    fn default() -> Self {
        Self {
            retry_base_seconds: 2.,
            retry_cap_seconds: 60.,
            max_attempts: 0,
            codex_queue_bin: None,
        }
    }
}
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Catalog {
    pub directory: Option<PathBuf>,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Mcp {
    pub transport: String,
    pub command: PathBuf,
    #[serde(default)]
    pub args: Vec<String>,
    #[serde(default)]
    pub env_from: Vec<String>,
    #[serde(default = "auto")]
    pub approval_mode: String,
}
fn auto() -> String {
    "auto".into()
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum Auth {
    Environment { names: Vec<String> },
    FileLink { source: PathBuf, target: String },
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Hook {
    pub event: String,
    pub command: Vec<String>,
    #[serde(default)]
    pub matcher: Option<String>,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RustRoots {
    pub rustup_home: PathBuf,
    pub cargo_bin: PathBuf,
}
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct Environment {
    pub path: Vec<PathBuf>,
    pub variables: BTreeMap<String, String>,
    pub required_commands: Vec<String>,
    pub denied_commands: Vec<String>,
    pub rust: Option<RustRoots>,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Runtime {
    pub enabled: bool,
    pub adapter: String,
    pub binary: PathBuf,
    pub home: PathBuf,
    pub models: Vec<String>,
    #[serde(default)]
    pub skills: Vec<String>,
    #[serde(default)]
    pub mcp: Vec<String>,
    #[serde(default)]
    pub max_active_agents: Option<usize>,
    #[serde(default)]
    pub auth: Option<Auth>,
    #[serde(default)]
    pub hooks: Vec<Hook>,
    #[serde(default)]
    pub plugins: Vec<PathBuf>,
    #[serde(default)]
    pub limits_source: Option<String>,
    #[serde(default)]
    pub accounts: Vec<String>,
    #[serde(default)]
    pub default_account: Option<String>,
    #[serde(default = "one")]
    pub priority_multiplier: f64,
    #[serde(default)]
    pub priority_account_multipliers: BTreeMap<String, f64>,
    #[serde(default)]
    pub priority_lane_multipliers: BTreeMap<String, f64>,
    #[serde(default)]
    pub rust: Option<RustRoots>,
    #[serde(default)]
    pub environment: Option<String>,
    #[serde(default)]
    pub plugin_snapshot_assets: BTreeMap<String, Vec<String>>,
    #[serde(default)]
    pub workspace_root: Option<PathBuf>,
    #[serde(default)]
    pub workspace_network: bool,
    #[serde(default)]
    pub native_settings: BTreeMap<String, toml::Value>,
}
fn one() -> f64 {
    1.0
}
pub fn name(s: &str) -> bool {
    !s.is_empty()
        && s.as_bytes()[0].is_ascii_alphanumeric()
        && s.bytes()
            .all(|b| b.is_ascii_alphanumeric() || b"_.-".contains(&b))
}
pub fn account(s: &str) -> bool {
    (1..=32).contains(&s.len())
        && s.bytes()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b"_-".contains(&b))
}
pub fn env_name(s: &str) -> bool {
    !s.is_empty()
        && (s.as_bytes()[0].is_ascii_uppercase() || s.starts_with('_'))
        && s.bytes()
            .all(|b| b.is_ascii_uppercase() || b.is_ascii_digit() || b == b'_')
}
fn unique(v: &[String], label: &str) -> Result<()> {
    let a: BTreeSet<_> = v.iter().collect();
    if a.len() != v.len() || v.iter().any(|s| s.trim().is_empty() || s.contains('\0')) {
        return Err(invalid(format!(
            "{label} contains duplicates or invalid strings"
        )));
    }
    Ok(())
}
fn commands(v: &[String], label: &str) -> Result<()> {
    unique(v, label)?;
    if v.iter().any(|s| {
        let bytes = s.as_bytes();
        !(bytes[0].is_ascii_alphanumeric() || bytes[0] == b'_')
            || !bytes
                .iter()
                .all(|b| b.is_ascii_alphanumeric() || b"_.+-".contains(b))
    }) {
        return Err(invalid(format!(
            "{label} must contain bare executable names"
        )));
    }
    Ok(())
}
fn names(v: &[String], label: &str) -> Result<()> {
    unique(v, label)?;
    if v.iter().any(|s| !name(s)) {
        return Err(invalid(format!("{label} must contain asset names")));
    }
    Ok(())
}
fn positive(v: f64, label: &str) -> Result<()> {
    if !v.is_finite() || v <= 0. {
        return Err(invalid(format!("{label} must be positive and finite")));
    }
    Ok(())
}
fn expand(p: &mut PathBuf) -> Result<()> {
    *p = fs::expand(p)?;
    if p.exists() {
        *p = p.canonicalize()?;
    }
    Ok(())
}
impl Runtime {
    pub fn kind(&self) -> Result<Adapter> {
        Adapter::parse(&self.adapter)
    }
    pub fn selected_account(&self, requested: Option<&str>) -> Result<Option<String>> {
        // `default_account` remains readable for Python-era configurations but
        // must never turn an omitted selector into a labelled credential. The
        // native global Codex account is represented exclusively by `None`.
        let a = requested;
        if let Some(a) = a {
            if !self.accounts.iter().any(|v| v == a) {
                return Err(invalid("account is not declared for this runtime"));
            }
        }
        Ok(a.map(str::to_owned))
    }
    pub fn weight(&self, account: Option<&str>, lane: &str) -> f64 {
        account
            .and_then(|a| self.priority_account_multipliers.get(a))
            .or_else(|| self.priority_lane_multipliers.get(lane))
            .copied()
            .unwrap_or(self.priority_multiplier)
    }
}
impl Config {
    pub fn load(home: &Path) -> Result<Self> {
        let text = fs::Dir::open(home)?.read(Path::new("config.toml"), 1024 * 1024)?;
        let text = std::str::from_utf8(&text).map_err(|_| invalid("config must be UTF-8"))?;
        // Remove only the explicitly retired runtime table before strict parsing.
        let mut raw: toml::Value =
            toml::from_str(text).map_err(|_| invalid("invalid config TOML"))?;
        if let Some(t) = raw.get_mut("runtimes").and_then(toml::Value::as_table_mut) {
            t.remove("opencode");
        }
        // Re-render to TOML text and re-parse from there rather than calling
        // `raw.try_into()` directly: converting a parsed `toml::Value` into
        // another type through serde's generic Value-to-Value path silently
        // decays `Datetime` values to plain strings (verified against the
        // `toml` crate actually in use), which would let a TOML date sneak
        // past every `toml::Value::Datetime` rejection in this module (most
        // importantly `native_settings`, whose Python counterpart rejects
        // dates explicitly because they cannot round-trip through JSON).
        // Re-parsing from text keeps the parser's own datetime handling.
        let rewritten = toml::to_string(&raw).map_err(|_| invalid("invalid config TOML"))?;
        let mut cfg: Self = toml::from_str(&rewritten)
            .map_err(|_| invalid("invalid config shape, type, or unknown field"))?;
        cfg.validate(home)?;
        Ok(cfg)
    }
    pub fn profiles_dir(&self) -> &Path {
        self.profiles
            .directory
            .as_deref()
            .expect("validated config")
    }
    pub fn skills_dir(&self) -> &Path {
        self.skills.directory.as_deref().expect("validated config")
    }
    pub fn runtime(&self, name: &str) -> Result<&Runtime> {
        self.runtimes
            .get(name)
            .filter(|r| r.enabled)
            .ok_or_else(|| invalid("runtime is not enabled"))
    }
    pub fn validate(&mut self, home: &Path) -> Result<()> {
        if self.schema_version != 1 {
            return Err(invalid("unsupported config schema_version"));
        }
        positive(
            self.core.default_timeout_seconds,
            "core.default_timeout_seconds",
        )?;
        if self.core.max_active_agents == 0 || self.core.max_active_agents > 4096 {
            return Err(invalid("max_active_agents must be 1..4096"));
        }
        if !(0.0..1.0).contains(&self.core.warning_fraction) || self.core.warning_fraction == 0.0 {
            return Err(invalid("warning_fraction must be between zero and one"));
        }
        if !self.core.stalled_after_seconds.is_finite() || self.core.stalled_after_seconds < 0.0 {
            return Err(invalid("stalled_after_seconds must be nonnegative"));
        }
        if self.capacity.collect_interval_seconds == 0
            || self.capacity.sample_retention == 0
            || self.capacity.context_max_chars > 2500
        {
            return Err(invalid("invalid capacity bounds"));
        }
        expand(&mut self.capacity.codexbar_binary)?;
        positive(
            self.delivery.retry_base_seconds,
            "delivery.retry_base_seconds",
        )?;
        positive(
            self.delivery.retry_cap_seconds,
            "delivery.retry_cap_seconds",
        )?;
        if self.delivery.retry_cap_seconds < self.delivery.retry_base_seconds {
            return Err(invalid("retry cap is below base"));
        }
        if let Some(p) = &mut self.delivery.codex_queue_bin {
            expand(p)?;
        }
        for (catalog, suffix) in [
            (&mut self.profiles, "profiles"),
            (&mut self.skills, "skills"),
        ] {
            let p = catalog.directory.get_or_insert_with(|| home.join(suffix));
            expand(p)?;
        }
        for (n, m) in &mut self.mcp {
            if !name(n)
                || m.transport != "stdio"
                || !["auto", "prompt", "writes", "approve"].contains(&m.approval_mode.as_str())
            {
                return Err(invalid("invalid MCP declaration"));
            }
            expand(&mut m.command)?;
            unique(&m.env_from, "MCP env_from")?;
            if m.env_from.iter().any(|s| !env_name(s)) || m.args.iter().any(|s| s.contains('\0')) {
                return Err(invalid("invalid MCP argument/environment declaration"));
            }
        }
        for (n, e) in &mut self.environments {
            if !name(n) {
                return Err(invalid("invalid environment name"));
            }
            for p in &mut e.path {
                expand(p)?;
            }
            commands(&e.required_commands, "required_commands")?;
            commands(&e.denied_commands, "denied_commands")?;
            if e.required_commands
                .iter()
                .any(|c| e.denied_commands.contains(c))
            {
                return Err(invalid("a command is both required and denied"));
            }
            for (k, v) in &e.variables {
                if !env_name(k) || v.contains('\0') {
                    return Err(invalid("invalid environment variable declaration"));
                }
                if [
                    "HOME",
                    "PATH",
                    "CODEX_HOME",
                    "CLAUDE_CONFIG_DIR",
                    "AGENT_RUN_HOME",
                ]
                .contains(&k.as_str())
                    || ["TOKEN", "SECRET", "PASSWORD", "API_KEY", "CREDENTIAL"]
                        .iter()
                        .any(|part| k.contains(part))
                {
                    return Err(invalid("developer environment variables must not override private homes or embed credentials"));
                }
            }
            if let Some(r) = &mut e.rust {
                expand(&mut r.rustup_home)?;
                r.cargo_bin = fs::expand(&r.cargo_bin)?;
            }
        }
        for (n, r) in &mut self.runtimes {
            if !name(n) {
                return Err(invalid("invalid runtime name"));
            }
            // Python's `_parse_runtimes` only checks the adapter string's
            // `module:attribute` shape at config-load time; a foreign (but
            // well-formed) adapter reference is accepted and every other
            // field is still validated normally. Only fail closed here on
            // features whose semantics are actually adapter-specific
            // (scoped accounts, Codex workspace controls, native settings).
            let kind = Adapter::parse(&r.adapter).ok();
            r.binary = fs::expand(&r.binary)?;
            expand(&mut r.home)?;
            unique(&r.models, "models")?;
            if r.models.is_empty() {
                return Err(invalid("runtime models must not be empty"));
            }
            names(&r.skills, "skills")?;
            names(&r.mcp, "mcp")?;
            if r.mcp.iter().any(|m| !self.mcp.contains_key(m)) {
                return Err(invalid("runtime references unknown MCP"));
            }
            unique(&r.accounts, "accounts")?;
            if r.accounts.iter().any(|a| !account(a))
                || (!r.accounts.is_empty()
                    && !matches!(kind, Some(Adapter::Codex) | Some(Adapter::Claude)))
            {
                return Err(invalid("invalid scoped accounts declaration"));
            }
            if let Some(a) = &r.default_account {
                if !r.accounts.contains(a) {
                    return Err(invalid("default_account must be declared in accounts"));
                }
            }
            if r.max_active_agents == Some(0) {
                return Err(invalid("runtime max_active_agents must be positive"));
            }
            positive(r.priority_multiplier, "priority_multiplier")?;
            for (key, v) in r
                .priority_account_multipliers
                .iter()
                .chain(r.priority_lane_multipliers.iter())
            {
                domain::nonblank("priority key", key)?;
                positive(*v, "priority override")?;
            }
            if let Some(auth) = &mut r.auth {
                match auth {
                    Auth::Environment { names } => {
                        unique(names, "auth.names")?;
                        if names.is_empty() || names.iter().any(|s| !env_name(s)) {
                            return Err(invalid("auth.names must be environment variable names"));
                        }
                    }
                    Auth::FileLink { source, target } => {
                        expand(source)?;
                        fs::relative(Path::new(target))?;
                    }
                }
            }
            for h in &r.hooks {
                domain::nonblank("hook event", &h.event)?;
                if h.command.is_empty()
                    || h.command.iter().any(|s| s.is_empty() || s.contains('\0'))
                {
                    return Err(invalid("invalid hook command"));
                }
            }
            if let Some(env) = &r.environment {
                if !self.environments.contains_key(env) {
                    return Err(invalid("unknown environment"));
                }
            }
            if let Some(p) = &mut r.workspace_root {
                if kind != Some(Adapter::Codex) {
                    return Err(invalid("workspace_root requires codex"));
                }
                expand(p)?;
            }
            if r.workspace_network && (kind != Some(Adapter::Codex) || r.workspace_root.is_none()) {
                return Err(invalid("workspace_network requires codex workspace_root"));
            }
            if let Some(roots) = &mut r.rust {
                expand(&mut roots.rustup_home)?;
                roots.cargo_bin = fs::expand(&roots.cargo_bin)?;
            }
            for p in &mut r.plugins {
                expand(p)?;
                if !p.is_dir() {
                    return Err(invalid("plugin must be an existing directory"));
                }
            }
            let unique_plugins: BTreeSet<_> = r.plugins.iter().collect();
            if unique_plugins.len() != r.plugins.len() {
                return Err(invalid("duplicate plugin directory"));
            }
            for (base, assets) in &r.plugin_snapshot_assets {
                if r.plugins
                    .iter()
                    .filter(|p| p.file_name().and_then(|n| n.to_str()) == Some(base.as_str()))
                    .count()
                    != 1
                {
                    return Err(invalid(
                        "plugin snapshot key must identify exactly one plugin",
                    ));
                }
                unique(assets, "plugin assets")?;
                if assets.is_empty() {
                    return Err(invalid("empty plugin assets"));
                }
                for a in assets {
                    fs::relative(Path::new(a))?;
                    if a.chars().any(|c| "*?[]{}".contains(c)) {
                        return Err(invalid("plugin assets cannot contain glob syntax"));
                    }
                }
            }
            if let Some(source) = &r.limits_source {
                if !["native", "codex_appserver", "codexbar", "omniroute", "none"]
                    .contains(&source.as_str())
                {
                    return Err(invalid("unknown limits source"));
                }
            }
            match kind {
                Some(k) => native_settings(k, &r.native_settings)?,
                // Mirrors Python's `ADAPTER_RESERVED_ROOTS.get(adapter) is
                // None` fail-closed path: an adapter Rust does not ship a
                // merge implementation for cannot accept tuning at all.
                None if !r.native_settings.is_empty() => {
                    return Err(invalid(
                        "native_settings is supported only by the codex, claude, glm, and qwen adapters",
                    ));
                }
                None => {}
            }
        }
        Ok(())
    }
}
/// Top-level Codex settings owned by the agent-run launch/security contract.
const CODEX_RESERVED:&str="model model_provider model_providers model_reasoning_effort cli_auth_credentials_store mcp_oauth_credentials_store forced_login_method forced_chatgpt_workspace_id openai_base_url chatgpt_base_url openai_api_key approvals_reviewer approval_policy sandbox_mode sandbox_workspace_write shell_environment_policy notify features otel profile profiles projects default_permissions permissions plugins hooks mcp_servers skills tools agents apps web_search trust auth credentials env environment provider providers web";
const CLAUDE_RESERVED:&str="model env environment permissions sandbox credentials auth hooks mcpServers apiKeyHelper agent autoMemoryDirectory forceLoginMethod forceLoginOrgUUID disableAllHooks statusLine enableAllProjectMcpServers enabledMcpjsonServers disabledMcpjsonServers awsAuthRefresh awsCredentialExport gcpAuthRefresh enabledPlugins extraKnownMarketplaces fileSuggestion providers";
const QWEN_RESERVED:&str="model modelProviders providers extensions env environment credentials auth tools context security permissions hooks mcpServers mcp skills sandbox";
/// Validates unowned native tuning settings for one packaged adapter.
///
/// Only identifier keys and TOML values that round-trip through the generated
/// TOML/JSON configuration are admitted. Runtime-owned roots (model, auth,
/// permissions, sandbox, hooks, and transport controls) always fail closed so
/// an owner's native table cannot weaken the delegated capability grant.
pub fn native_settings(kind: Adapter, settings: &BTreeMap<String, toml::Value>) -> Result<()> {
    let reserved = match kind {
        Adapter::Codex => CODEX_RESERVED,
        Adapter::Claude | Adapter::Glm => CLAUDE_RESERVED,
        Adapter::Qwen => QWEN_RESERVED,
    };
    /// Returns whether a TOML key cannot splice another generated namespace.
    fn key(k: &str) -> bool {
        !k.is_empty()
            && k.bytes()
                .all(|c| c.is_ascii_alphanumeric() || b"_-".contains(&c))
    }
    /// Rejects values that cannot safely round-trip through native renderers.
    fn value(v: &toml::Value) -> Result<()> {
        match v {
            toml::Value::Datetime(_) => return Err(invalid("native settings do not accept dates")),
            toml::Value::Float(v) if !v.is_finite() => {
                return Err(invalid("native settings floats must be finite"))
            }
            toml::Value::Table(t) => {
                for (k, v) in t {
                    if !key(k) {
                        return Err(invalid("native setting keys must be plain identifiers"));
                    }
                    value(v)?;
                }
            }
            toml::Value::Array(a) => {
                for v in a {
                    value(v)?;
                }
            }
            _ => {}
        }
        Ok(())
    }
    for (k, v) in settings {
        if !key(k) || reserved.split_whitespace().any(|s| s == k) {
            return Err(invalid("native setting is reserved or has an invalid key"));
        }
        value(v)?;
    }
    Ok(())
}
