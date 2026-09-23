use crate::{auth, redact::is_secret_name};
use agent_run_config::{
    config::{Adapter, Auth, Config, Runtime},
    profiles::Profile,
};
use agent_run_domain::{domain::StartRequest, error::invalid, Error, Result};
use agent_run_platform::{
    fs::{self, Dir},
    snapshot_tree,
};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::{
    collections::{BTreeMap, BTreeSet},
    path::{Path, PathBuf},
};
const CREDENTIAL_CARRIERS: &[&str] = &[
    "AWS_PROFILE",
    "AWS_CONFIG_FILE",
    "AWS_SHARED_CREDENTIALS_FILE",
    "AZURE_CONFIG_DIR",
    "BOTO_CONFIG",
    "BUNDLE_USER_CONFIG",
    "CLOUDSDK_CONFIG",
    "DOCKER_CONFIG",
    "GH_CONFIG_DIR",
    "GIT_ASKPASS",
    "GIT_CONFIG_GLOBAL",
    "GOOGLE_APPLICATION_CREDENTIALS",
    "GNUPGHOME",
    "KRB5CCNAME",
    "KUBECONFIG",
    "NETRC",
    "NPM_CONFIG_USERCONFIG",
    "PIP_CONFIG_FILE",
    "PGPASSFILE",
    "SSH_ASKPASS",
    "SSH_AUTH_SOCK",
    "TF_CLI_CONFIG_FILE",
];

/// Applies Python's host-environment deny-list before adapter-owned overrides.
///
/// Ordinary host variables survive. Credential-shaped names and known
/// credential carriers are omitted unless `allowed_secret_names` explicitly
/// names them. Existing host Rust toolchain directories are retained when the
/// corresponding variable is absent, so replacing `HOME` does not hide them.
pub fn inherited_environment(
    host: &BTreeMap<String, String>,
    allowed_secret_names: &BTreeSet<String>,
) -> BTreeMap<String, String> {
    let mut environment: BTreeMap<_, _> = host
        .iter()
        .filter(|(name, _)| {
            allowed_secret_names.contains(*name)
                || (!CREDENTIAL_CARRIERS.contains(&name.as_str()) && !is_secret_name(name))
        })
        .map(|(name, value)| (name.clone(), value.clone()))
        .collect();
    if let Some(home) = host.get("HOME") {
        for (name, directory) in [("RUSTUP_HOME", ".rustup"), ("CARGO_HOME", ".cargo")] {
            let candidate = Path::new(home).join(directory);
            if !environment.contains_key(name) && candidate.is_dir() {
                environment.insert(name.into(), candidate.to_string_lossy().into_owned());
            }
        }
    }
    environment
}

/// Applies runtime-owned values after host inheritance so they always win.
pub fn apply_environment_overrides(
    environment: &mut BTreeMap<String, String>,
    overrides: BTreeMap<String, String>,
) {
    environment.extend(overrides);
}
/// Launch-time paths derived while materializing one generated home.
///
/// For legacy Claude-family runtimes these values are also written to the
/// indexed [`PLUGIN_LAUNCH`] file, so a verified resume reuses the exact
/// first-launch order and plugin roots. Provider (schema-2) homes seal the
/// same paths in their indexed `provider-launch.json` instead.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Snapshot {
    /// Ordered paths passed to the native CLI as `--plugin-dir`.
    pub plugin_paths: Vec<PathBuf>,
    /// Manifest names and the matching runtime-visible plugin roots.
    pub plugin_roots: BTreeMap<String, PathBuf>,
}
/// Indexed first-launch plugin metadata of legacy Claude-family homes.
const PLUGIN_LAUNCH: &str = ".agent-run-plugin-launch.json";
/// Builds one Python-v1 managed runtime home before its manifest/index freeze.
pub struct Publisher {
    pub root: PathBuf,
    dir: Dir,
    pub snapshot: Snapshot,
    total: usize,
    files: BTreeMap<String, String>,
    links: BTreeMap<String, String>,
}
impl Publisher {
    pub fn new(root: &Path) -> Result<Self> {
        fs::private_dir(root)?;
        Ok(Self {
            root: root.into(),
            dir: Dir::open(root)?,
            snapshot: Snapshot::default(),
            total: 0,
            files: BTreeMap::new(),
            links: BTreeMap::new(),
        })
    }
    pub fn file(&mut self, path: &str, bytes: &[u8], mode: u32) -> Result<()> {
        self.total += bytes.len();
        if self.total > 256 * 1024 * 1024 {
            return Err(invalid("generated assets exceed 256 MiB"));
        }
        self.dir.write(Path::new(path), bytes, mode)?;
        self.files.insert(path.into(), fs::sha256(bytes));
        Ok(())
    }
    pub fn json(&mut self, path: &str, value: &Value) -> Result<()> {
        let mut data = serde_json::to_vec_pretty(value)?;
        data.push(b'\n');
        self.file(path, &data, 0o600)
    }
    pub fn link(&mut self, path: &str, target: &Path) -> Result<()> {
        let real = target
            .canonicalize()
            .map_err(|_| invalid("credential source does not exist"))?;
        if !real.is_file() {
            return Err(invalid("credential source must be a regular file"));
        }
        if let Ok(metadata) = std::fs::symlink_metadata(self.root.join(path)) {
            if metadata.file_type().is_symlink() || metadata.is_file() {
                self.dir.remove(Path::new(path))?;
            } else {
                return Err(invalid("credential bridge target must not be a directory"));
            }
        }
        self.dir.symlink(&real, Path::new(path))?;
        self.links
            .insert(path.into(), real.to_string_lossy().into_owned());
        Ok(())
    }
    pub fn tree(&mut self, source: &Path, prefix: &str, selected: Option<&[String]>) -> Result<()> {
        snapshot_tree::snapshot_managed_tree(&self.root, Path::new(prefix), source, selected)
            .map(drop)
    }
    /// Bind adapter-owned files and links into the Python-v1 runtime index.
    pub fn finish(self) -> Result<(Snapshot, String)> {
        let revision = agent_run_domain::canonical::sha256_hex(
            &json!({"files": self.files, "links": self.links}),
            true,
        );
        let digest = snapshot_tree::finalize_runtime_snapshots(
            &self.root,
            &revision,
            &self.files.into_keys().collect::<Vec<_>>(),
            &self.links.into_iter().collect::<Vec<_>>(),
        )?;
        Ok((self.snapshot, digest))
    }
}
/// Verifies a runtime home against its index digest `expected` and returns
/// the recorded plugin launch paths.
///
/// Returns the indexed [`PLUGIN_LAUNCH`] contents when present; homes
/// without it (provider homes, other adapters, and legacy Claude-family
/// homes from older releases) return an empty snapshot, so a legacy resume
/// must use [`verify_for_resume`]. A malformed index, any modified indexed
/// artifact, or a launch file that exists without being indexed (or is
/// indexed but missing) is an integrity error.
pub fn verify(root: &Path, expected: &str) -> Result<Snapshot> {
    let dir = Dir::open(root)?;
    let raw = dir.read(Path::new(snapshot_tree::RUNTIME_SNAPSHOT_INDEX), 64 * 1024)?;
    let document: Value = serde_json::from_slice(&raw)
        .map_err(|_| Error::Integrity("runtime snapshot index is malformed".into()))?;
    let revision = document
        .get("materialize_revision")
        .and_then(Value::as_str)
        .map(str::to_owned)
        .ok_or_else(|| Error::Integrity("runtime snapshot index is malformed".into()))?;
    let inspection = snapshot_tree::inspect_runtime_snapshots(root, &revision, expected)
        .map_err(|_| Error::Integrity("runtime snapshot index was modified".into()))?;
    if !inspection.verified {
        return Err(Error::Integrity(
            "generated runtime snapshot was modified".into(),
        ));
    }
    let indexed = document["files"]
        .as_array()
        .is_some_and(|files| files.iter().any(|file| file["path"] == PLUGIN_LAUNCH));
    match (indexed, dir.optional(Path::new(PLUGIN_LAUNCH), 64 * 1024)?) {
        (true, Some(raw)) => serde_json::from_slice(&raw)
            .map_err(|_| Error::Integrity("plugin launch metadata is malformed".into())),
        (false, None) => Ok(Snapshot::default()),
        _ => Err(Error::Integrity(
            "plugin launch metadata is not indexed".into(),
        )),
    }
}

/// Verifies a legacy (schema-1) resume home and returns the first launch's
/// plugin paths, reconstructing them only for older indexes.
///
/// `runtime` and `profile` must be the STORED launch identity's runtime and
/// profile, never current configuration. A home with indexed launch
/// metadata returns it as recorded. For an older Claude/GLM index without
/// it, the paths are rebuilt in first-launch order from the stored runtime's
/// plugins (the verified in-home copy when the plugin was snapshotted, the
/// original source otherwise) followed by the stored profile's projected
/// catalog skill plugins inside the home. A plugin whose manifest cannot be
/// read, a duplicated plugin name, or a missing projected skill plugin fails
/// closed with an integrity error instead of launching without it. Other
/// adapters return the verified (empty) snapshot unchanged.
pub fn verify_for_resume(
    root: &Path,
    expected: &str,
    runtime: &Runtime,
    profile: &Profile,
) -> Result<Snapshot> {
    let snapshot = verify(root, expected)?;
    if Dir::open(root)?
        .optional(Path::new(PLUGIN_LAUNCH), 64 * 1024)?
        .is_some()
    {
        return Ok(snapshot);
    }
    if !matches!(runtime.kind()?, Adapter::Claude | Adapter::Glm) {
        return Ok(snapshot);
    }
    let mut legacy = Snapshot::default();
    for source in &runtime.plugins {
        let base = source
            .file_name()
            .ok_or_else(|| Error::Integrity("legacy plugin path is invalid".into()))?;
        let path = if runtime
            .plugin_snapshot_assets
            .contains_key(&base.to_string_lossy().to_string())
        {
            root.join("declared-plugins").join(base)
        } else {
            source.clone()
        };
        let (name, _, _) = super::plugins::manifest(&path)
            .map_err(|_| Error::Integrity("legacy plugin source is unavailable".into()))?;
        if legacy.plugin_roots.insert(name, path.clone()).is_some() {
            return Err(Error::Integrity(
                "legacy plugin names are duplicated".into(),
            ));
        }
        legacy.plugin_paths.push(path);
    }
    for skill in &profile.skills {
        let owned = super::plugins::plugin_skill_dir(&runtime.plugins, skill)
            .map_err(|_| Error::Integrity("legacy plugin skill source is unavailable".into()))?;
        if owned.is_none() {
            let path = root.join("plugins").join(skill);
            if !path.is_dir() {
                return Err(Error::Integrity(
                    "legacy skill plugin is unavailable".into(),
                ));
            }
            legacy.plugin_paths.push(path);
        }
    }
    Ok(legacy)
}
pub fn shell_quote(s: &str) -> String {
    if !s.is_empty()
        && s.bytes()
            .all(|c| c.is_ascii_alphanumeric() || b"_@%+=:,./-".contains(&c))
    {
        s.into()
    } else {
        format!("'{}'", s.replace('\'', "'\"'\"'"))
    }
}
pub fn shell_command(args: &[String]) -> String {
    args.iter()
        .map(|s| shell_quote(s))
        .collect::<Vec<_>>()
        .join(" ")
}
pub fn account_home(app_home: &Path, kind: Adapter, label: &str) -> PathBuf {
    app_home.join("accounts").join(kind.name()).join(label)
}

/// Returns the single `CLAUDE_CONFIG_DIR` of one labelled Claude login.
///
/// This is the one authority shared by `auth login`, run/provider
/// materialization, and quota credential resolution. The canonical directory
/// is `<app_home>/accounts/claude/<label>/claude-config`. Logins created by
/// earlier releases live at `<runtime_home>@<label>/claude-config` (the
/// runtime home's final component suffixed with `@<label>`) and stay usable in
/// place without re-login or copying: when only that legacy directory exists
/// it is returned. When neither exists the canonical directory is returned
/// (it is not created here). When both exist and are not the same directory,
/// ownership is ambiguous and a validation error is returned instead of
/// silently choosing one; there is never a fallback to the default login.
pub fn claude_account_config(app_home: &Path, runtime_home: &Path, label: &str) -> Result<PathBuf> {
    let canonical = account_home(app_home, Adapter::Claude, label).join("claude-config");
    let name = runtime_home
        .file_name()
        .ok_or_else(|| invalid("Claude runtime home has no final path component"))?
        .to_string_lossy()
        .into_owned();
    let legacy = runtime_home
        .with_file_name(format!("{name}@{label}"))
        .join("claude-config");
    match (canonical.is_dir(), legacy.is_dir()) {
        (true, true) if canonical.canonicalize().ok() != legacy.canonicalize().ok() => Err(
            invalid("labelled Claude login exists in both account homes; remove one"),
        ),
        (false, true) => Ok(legacy),
        _ => Ok(canonical),
    }
}
pub fn environment(
    config: &Config,
    runtime: &Runtime,
    profile: &Profile,
    home: &Path,
    account: Option<&str>,
    app_home: &Path,
) -> Result<BTreeMap<String, String>> {
    let host: BTreeMap<_, _> = std::env::vars().collect();
    environment_with_host(config, runtime, profile, home, account, app_home, &host)
}

/// Resolves one adapter environment from an explicitly supplied host snapshot.
///
/// The production wrapper supplies the process environment; tests and callers
/// that already captured a host snapshot can use this deterministic variant.
pub fn environment_with_host(
    config: &Config,
    runtime: &Runtime,
    profile: &Profile,
    home: &Path,
    account: Option<&str>,
    app_home: &Path,
    host: &BTreeMap<String, String>,
) -> Result<BTreeMap<String, String>> {
    let kind = runtime.kind()?;
    // A relative executable would put its own resolution at the mercy of the
    // child's working directory, so Python refuses it before building any
    // environment (`build_environment`, adapters/codex/environment.py:108-110).
    if !runtime.binary.is_absolute() {
        return Err(invalid("runtime binary must be an absolute path"));
    }
    let mut names = BTreeSet::new();
    if let Some(Auth::Environment { names: declared }) = &runtime.auth {
        names.extend(declared.iter().cloned());
    }
    for name in &profile.mcp {
        names.extend(config.mcp[name].env_from.iter().cloned());
    }
    let mut env = inherited_environment(host, &names);
    let host_home = host
        .get("HOME")
        .cloned()
        .ok_or_else(|| invalid("HOME is unavailable"))?;
    env.insert(
        "HOME".into(),
        if kind == Adapter::Claude && account.is_none() {
            host_home
        } else {
            home.to_string_lossy().into_owned()
        },
    );
    if kind == Adapter::Codex {
        env.insert("CODEX_HOME".into(), home.to_string_lossy().into_owned());
    }
    if kind == Adapter::Claude {
        if let Some(a) = account {
            env.insert(
                "CLAUDE_CONFIG_DIR".into(),
                claude_account_config(app_home, &runtime.home, a)?
                    .to_string_lossy()
                    .into_owned(),
            );
        }
    }
    let mut paths = Vec::new();
    let mut overrides = BTreeMap::new();
    if let Some(e) = runtime
        .environment
        .as_ref()
        .and_then(|n| config.environments.get(n))
    {
        paths.extend(e.path.iter().map(|p| p.to_string_lossy().into_owned()));
        for (k, v) in &e.variables {
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
                    .any(|s| k.contains(s))
            {
                return Err(invalid(
                    "developer environment must not override private homes or embed credentials",
                ));
            }
            overrides.insert(k.clone(), v.clone());
        }
    }
    apply_environment_overrides(&mut env, overrides);
    if let Some(r) = runtime.rust.as_ref().or_else(|| {
        runtime
            .environment
            .as_ref()
            .and_then(|n| config.environments.get(n))
            .and_then(|e| e.rust.as_ref())
    }) {
        env.insert(
            "RUSTUP_HOME".into(),
            r.rustup_home.to_string_lossy().into_owned(),
        );
        env.insert(
            "CARGO_HOME".into(),
            home.join(".cargo").to_string_lossy().into_owned(),
        );
        paths.push(r.cargo_bin.to_string_lossy().into_owned());
    }
    // Python's shared child environment keeps the inherited PATH exactly as
    // the host supplied it (`build_environment`,
    // adapters/codex/environment.py:94-115). The configured binary's own
    // directory is never prefixed, an absent PATH is never invented, and no
    // empty entry -- which POSIX resolves as the working directory -- is
    // ever appended.
    paths.retain(|entry| !entry.is_empty());
    let prefix = paths.join(":");
    if !prefix.is_empty() {
        let value = match env.get("PATH") {
            Some(host) if !host.is_empty() => format!("{prefix}:{host}"),
            _ => prefix,
        };
        env.insert("PATH".into(), value);
    }
    if let Some(e) = runtime
        .environment
        .as_ref()
        .and_then(|n| config.environments.get(n))
    {
        if !e.denied_commands.is_empty() {
            let directory = "command-refusals";
            // An absent or empty inherited PATH must not become an empty entry.
            let policy = home.join(directory).display().to_string();
            let value = match env.get("PATH") {
                Some(host) if !host.is_empty() => format!("{policy}:{host}"),
                _ => policy,
            };
            env.insert("PATH".into(), value);
        }
    }
    let mut mcp_names = Vec::new();
    for name in &profile.mcp {
        mcp_names.extend(config.mcp[name].env_from.clone());
    }
    for name in mcp_names {
        if [
            "HOME",
            "PATH",
            "CODEX_HOME",
            "CLAUDE_CONFIG_DIR",
            "AGENT_RUN_HOME",
            "RUSTUP_HOME",
            "CARGO_HOME",
        ]
        .contains(&name.as_str())
        {
            return Err(invalid(
                "auth/MCP environment forwarding cannot override managed environment controls",
            ));
        }
        let value = host.get(&name).cloned().ok_or_else(|| {
            invalid("declared authentication/MCP environment variable is missing")
        })?;
        if value.is_empty() {
            return Err(invalid(
                "declared authentication/MCP environment variable is empty",
            ));
        }
        env.insert(name, value);
    }
    match kind {
        Adapter::Glm => {
            env.extend(auth::glm_environment(host)?);
        }
        Adapter::Claude => {
            if let Some(Auth::Environment { names }) = &runtime.auth {
                for name in names {
                    if let Some(value) = host.get(name).filter(|value| !value.is_empty()) {
                        env.insert(name.clone(), value.clone());
                    }
                }
            }
        }
        _ => {
            if let Some(Auth::Environment { names }) = &runtime.auth {
                for name in names {
                    let value = host.get(name).cloned().ok_or_else(|| {
                        invalid("declared authentication/MCP environment variable is missing")
                    })?;
                    if value.is_empty() {
                        return Err(invalid(
                            "declared authentication/MCP environment variable is empty",
                        ));
                    }
                    env.insert(name.clone(), value);
                }
            }
        }
    }
    Ok(env)
}
/// Publishes one isolated adapter home for a validated request and role.
///
/// The generated files contain only configured capability grants, copied
/// skills/plugins, and an optional credential link. Codex homes receive owned
/// Projects, MCP, hook, command-policy, and native-setting configuration;
/// malformed inputs or unsafe sources fail before a launch plan is returned.
pub fn materialize(
    config: &Config,
    runtime: &Runtime,
    request: &StartRequest,
    profile: &Profile,
    home: &Path,
    app_home: &Path,
) -> Result<(Snapshot, String)> {
    materialize_with_provider(config, runtime, request, profile, home, app_home, None)
}

/// Seals provider-specific native settings and nonsecret launch metadata into
/// the same verified runtime index as skills, MCP, hooks, and permissions.
// Each argument is an independent, already-validated input of the one seal.
#[allow(clippy::too_many_arguments)]
pub fn materialize_provider(
    config: &Config,
    runtime: &Runtime,
    request: &StartRequest,
    profile: &Profile,
    home: &Path,
    app_home: &Path,
    provider: &agent_run_domain::ProviderDefinition,
    native_model: &str,
    config_sha256: &str,
) -> Result<(Snapshot, String)> {
    materialize_with_provider(
        config,
        runtime,
        request,
        profile,
        home,
        app_home,
        Some((provider, native_model, config_sha256)),
    )
}

/// Shared v1/v2 writer; only the optional provider branch adds new files or
/// custom gateway settings, so historical materialization stays byte-stable.
fn materialize_with_provider(
    config: &Config,
    runtime: &Runtime,
    request: &StartRequest,
    profile: &Profile,
    home: &Path,
    app_home: &Path,
    provider: Option<(&agent_run_domain::ProviderDefinition, &str, &str)>,
) -> Result<(Snapshot, String)> {
    let kind = runtime.kind()?;
    agent_run_config::config::native_settings(kind, &runtime.native_settings)?;
    super::claude::validate_runtime(runtime, kind)?;
    if std::fs::symlink_metadata(home.join("skills"))
        .map(|metadata| metadata.file_type().is_symlink())
        .unwrap_or(false)
    {
        return Err(invalid("skills root must not be a symlink"));
    }
    let mut p = Publisher::new(home)?;
    let plugins = super::plugins::install(&mut p, runtime, kind)?;
    p.snapshot.plugin_paths = plugins.paths.clone();
    p.snapshot.plugin_roots = plugins.roots.clone();
    let skill_catalog = if profile.canonical {
        config.skills_dir().to_path_buf()
    } else {
        app_home.join("skills").join(&request.runtime)
    };
    for skill in &profile.skills {
        let plugin_source = super::plugins::plugin_skill_dir(&runtime.plugins, skill)?;
        if matches!(kind, Adapter::Claude | Adapter::Glm) && plugin_source.is_some() {
            continue;
        }
        let source = plugin_source.unwrap_or_else(|| skill_catalog.join(skill));
        if !source.join("SKILL.md").is_file() {
            return Err(invalid("declared skill is unavailable"));
        }
        let relative = if matches!(kind, Adapter::Claude | Adapter::Glm) {
            format!("plugins/{skill}/skills/{skill}")
        } else {
            format!("skills/{skill}")
        };
        p.tree(&source, &relative, None)?;
        if matches!(kind, Adapter::Claude | Adapter::Glm) {
            p.json(
                &format!("plugins/{skill}/.claude-plugin/plugin.json"),
                &json!({"name":skill,"version":"1.0.0"}),
            )?;
            p.snapshot
                .plugin_paths
                .push(home.join("plugins").join(skill));
        }
    }
    let mut mcp = serde_json::Map::new();
    for name in &profile.mcp {
        let server = &config.mcp[name];
        let env: serde_json::Map<String, Value> = server
            .env_from
            .iter()
            .map(|n| (n.clone(), json!(format!("${{{n}}}"))))
            .collect();
        let mut entry = json!({"command":server.command,"args":server.args});
        if !env.is_empty() {
            entry["env"] = Value::Object(env);
        }
        mcp.insert(name.clone(), entry);
    }
    let mut hooks = super::plugins::hook_groups(runtime, &plugins.roots)?;
    if kind == Adapter::Codex {
        let trusted = profile
            .mcp
            .iter()
            .filter(|name| config.mcp[*name].approval_mode == "approve")
            .cloned()
            .collect::<BTreeSet<_>>();
        if let Some((matcher, command)) =
            super::codex::permission_request_hook(&trusted, &std::env::current_exe()?)?
        {
            hooks["PermissionRequest"] = json!([{
                "matcher": matcher,
                "hooks": [{"type":"command", "command":shell_command(&command), "timeout":600}],
            }]);
        }
    }
    let native: Value = serde_json::to_value(&runtime.native_settings)?;
    match kind {
        Adapter::Codex => {
            let mut native_lines = vec!["# generated by agent-run; do not edit by hand".into()];
            for (key, default) in [
                ("model_context_window", toml::Value::Integer(1_000_000)),
                (
                    "model_auto_compact_token_limit",
                    toml::Value::Integer(780_000),
                ),
                (
                    "model_auto_compact_token_limit_scope",
                    toml::Value::String("total".into()),
                ),
            ] {
                let value = runtime.native_settings.get(key).unwrap_or(&default);
                native_lines.push(format!(
                    "{key} = {}",
                    super::codex::inline_toml_value(value)?
                ));
            }
            for (key, value) in &runtime.native_settings {
                if ![
                    "model_context_window",
                    "model_auto_compact_token_limit",
                    "model_auto_compact_token_limit_scope",
                ]
                .contains(&key.as_str())
                {
                    native_lines.push(format!(
                        "{key} = {}",
                        super::codex::inline_toml_value(value)?
                    ));
                }
            }
            let mut doc = toml::Table::new();
            let mut servers = toml::Table::new();
            for name in &profile.mcp {
                let s = &config.mcp[name];
                let mut t = toml::Table::new();
                t.insert(
                    "command".into(),
                    toml::Value::String(s.command.to_string_lossy().into_owned()),
                );
                t.insert(
                    "args".into(),
                    toml::Value::Array(s.args.iter().cloned().map(toml::Value::String).collect()),
                );
                if !s.env_from.is_empty() {
                    t.insert(
                        "env_vars".into(),
                        toml::Value::Array(
                            s.env_from
                                .iter()
                                .cloned()
                                .map(toml::Value::String)
                                .collect(),
                        ),
                    );
                }
                if s.approval_mode != "auto" {
                    t.insert(
                        "default_tools_approval_mode".into(),
                        toml::Value::String(s.approval_mode.clone()),
                    );
                }
                servers.insert(name.clone(), toml::Value::Table(t));
            }
            if !servers.is_empty() {
                doc.insert("mcp_servers".into(), toml::Value::Table(servers));
            }
            let mut projects = toml::Table::new();
            let mut receipt = toml::Table::new();
            receipt.insert("trust_level".into(), toml::Value::String("trusted".into()));
            projects.insert(
                request.workdir.to_string_lossy().into_owned(),
                toml::Value::Table(receipt),
            );
            doc.insert("projects".into(), toml::Value::Table(projects));
            super::plugins::codex_config(&mut doc, &hooks, home, &plugins)?;
            let custom = provider.is_some_and(|(provider, _, _)| {
                matches!(
                    provider.connection,
                    agent_run_domain::ProviderConnection::Custom { .. }
                )
            });
            let auth_target = if custom {
                None
            } else if provider.is_some() || request.account.is_some() {
                Some("auth.json")
            } else if let Some(Auth::FileLink { target, .. }) = &runtime.auth {
                Some(target.as_str())
            } else {
                None
            };
            super::codex::render_permissions(&mut doc, runtime, home, auth_target)?;
            if let Some((provider, _, _)) = provider {
                if let agent_run_domain::ProviderConnection::Custom { endpoint, .. } =
                    &provider.connection
                {
                    let mut gateway = toml::Table::new();
                    gateway.insert(
                        "name".into(),
                        toml::Value::String("agent-run gateway".into()),
                    );
                    gateway.insert("base_url".into(), toml::Value::String(endpoint.clone()));
                    gateway.insert(
                        "env_key".into(),
                        toml::Value::String("AGENT_RUN_PROVIDER_TOKEN".into()),
                    );
                    gateway.insert("wire_api".into(), toml::Value::String("responses".into()));
                    doc.insert(
                        "model_provider".into(),
                        toml::Value::String("agent_run_gateway".into()),
                    );
                    let mut providers = toml::Table::new();
                    providers.insert("agent_run_gateway".into(), toml::Value::Table(gateway));
                    doc.insert("model_providers".into(), toml::Value::Table(providers));
                    let mut shell = toml::Table::new();
                    shell.insert("inherit".into(), toml::Value::String("core".into()));
                    shell.insert(
                        "exclude".into(),
                        toml::Value::Array(vec![toml::Value::String(
                            "AGENT_RUN_PROVIDER_TOKEN".into(),
                        )]),
                    );
                    doc.insert("shell_environment_policy".into(), toml::Value::Table(shell));
                }
            }
            let text = toml::to_string_pretty(&doc)
                .map_err(|_| invalid("native config could not be serialized"))?;
            let mut config_text = native_lines.join("\n");
            config_text.push_str("\n\n");
            config_text.push_str(&text);
            p.file("config.toml", config_text.as_bytes(), 0o600)?;
            let source_target = if provider.is_some() {
                None
            } else if let Some(a) = request.account.as_deref() {
                Some((
                    account_home(app_home, kind, a).join("auth.json"),
                    "auth.json".into(),
                ))
            } else if let Some(Auth::FileLink { source, target }) = &runtime.auth {
                Some((source.clone(), target.clone()))
            } else {
                let global = std::env::var_os("CODEX_HOME")
                    .map(PathBuf::from)
                    .unwrap_or_else(|| {
                        PathBuf::from(std::env::var_os("HOME").unwrap_or_default()).join(".codex")
                    });
                Some((global.join("auth.json"), "auth.json".into()))
            };
            if let Some((source, target)) = source_target {
                p.link(&target, &source)?;
            }
        }
        Adapter::Claude | Adapter::Glm => {
            let mut settings = native;
            if !hooks.is_null() && !hooks.as_object().is_some_and(|groups| groups.is_empty()) {
                settings["hooks"] = hooks.clone();
            }
            p.json("settings.json", &settings)?;
            if !mcp.is_empty() {
                p.json("mcp/mcp-config.json", &json!({"mcpServers":mcp}))?;
            }
            if let Some(Auth::FileLink { source, target }) = &runtime.auth {
                p.link(target, source)?;
            }
        }
    }
    if let Some(env) = runtime
        .environment
        .as_ref()
        .and_then(|n| config.environments.get(n))
    {
        for name in &env.denied_commands {
            let wrapper = "#!/bin/sh\nprintf '%s\\n' 'agent-run: command denied by owner policy' >&2\nexit 126\n";
            let directory = "command-refusals";
            p.file(&format!("{directory}/{name}"), wrapper.as_bytes(), 0o700)?;
        }
    }
    if kind == Adapter::Codex {
        let commands = runtime
            .environment
            .as_ref()
            .and_then(|name| config.environments.get(name))
            .map(|environment| environment.denied_commands.as_slice())
            .unwrap_or(&[]);
        let mut search_paths = runtime
            .environment
            .as_ref()
            .and_then(|name| config.environments.get(name))
            .into_iter()
            .flat_map(|environment| environment.path.iter())
            .map(|path| path.to_string_lossy().into_owned())
            .collect::<Vec<_>>();
        search_paths.push(std::env::var("PATH").unwrap_or_default());
        let path = search_paths.join(":");
        let mut rules = super::codex::render_denial_rules(commands, &path);
        if runtime.workspace_network {
            rules.push_str(&super::codex::render_review_rules(&["curl".into()], &path));
        }
        p.file(
            "rules/agent-run-command-policy.rules",
            rules.as_bytes(),
            0o600,
        )?;
        let marker = format!(
            "{{\"version\": 1, \"commands\": {}}}\n",
            serde_json::to_string(commands)?
        );
        p.file(
            "command-refusals/.agent-run-command-policy.json",
            marker.as_bytes(),
            0o600,
        )?;
    }
    // Legacy Claude-family homes record their launch plugin set for resume;
    // provider homes already seal it in `provider-launch.json` below.
    if provider.is_none() && matches!(kind, Adapter::Claude | Adapter::Glm) {
        p.json(PLUGIN_LAUNCH, &serde_json::to_value(&p.snapshot)?)?;
    }
    if let Some((provider, native_model, config_sha256)) = provider {
        p.json(
            "provider-launch.json",
            &json!({
                "provider": provider.id,
                "config_sha256": config_sha256,
                "harness": provider.harness,
                "model": request.model,
                "native_model": native_model,
                "connection": provider.connection,
                "binary": runtime.binary,
                "plugin_paths": p.snapshot.plugin_paths,
                "workdir": request.workdir,
                "profile": profile.name,
                "restrictions": provider.models.iter()
                    .find(|model| model.id == request.model)
                    .map(|model| &model.restrictions),
            }),
        )?;
    }
    p.finish()
}

#[cfg(test)]
mod tests {
    use super::*;
    use agent_run_config::config::{Capacity, Catalog, Config, Core, Delivery};
    use agent_run_config::profiles::Profile;
    use agent_run_domain::domain::StartRequest;
    use serde_json::json;
    use std::collections::{BTreeMap, BTreeSet};

    /// Builds an isolated Claude fixture for materialization assertions.
    fn claude_fixture(root: &Path) -> (Config, Runtime, StartRequest, Profile) {
        let runtime: Runtime = serde_json::from_value(json!({
            "enabled": true,
            "adapter": "claude",
            "binary": "/bin/true",
            "home": root.join("runtime"),
            "models": ["fixture"],
        }))
        .expect("fixture runtime");
        let config = Config {
            schema_version: 1,
            core: Core::default(),
            capacity: Capacity::default(),
            delivery: Delivery::default(),
            profiles: Catalog::default(),
            skills: Catalog::default(),
            mcp: BTreeMap::new(),
            environments: BTreeMap::new(),
            runtimes: BTreeMap::new(),
        };
        let request: StartRequest = serde_json::from_value(json!({
            "runtime": "claude", "model": "fixture", "profile": "review",
            "task": "fixture", "workdir": root,
        }))
        .expect("fixture request");
        let profile = Profile {
            name: "review".into(),
            body: "Fixture role.".into(),
            write: false,
            network: false,
            revision: "fixture".into(),
            canonical: false,
            allow_external_read_roots: true,
            read_roots: vec![],
            skills: vec![],
            mcp: vec![],
            required_constraints: BTreeSet::new(),
        };
        (config, runtime, request, profile)
    }

    /// Mirrors the empty-MCP case in `test_claude_developer_environment.py`.
    #[test]
    fn claude_home_omits_empty_mcp_configuration() {
        let root = std::env::temp_dir().join(format!("agent-run-claude-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&root).expect("owned test root");
        let (config, runtime, request, profile) = claude_fixture(&root);
        materialize(
            &config,
            &runtime,
            &request,
            &profile,
            &root.join("home"),
            &root,
        )
        .expect("materialize Claude home");
        assert!(!root.join("home/mcp/mcp-config.json").exists());
        std::fs::remove_dir_all(root).expect("remove owned test root");
    }
}
