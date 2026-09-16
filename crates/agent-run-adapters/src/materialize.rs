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
use serde_json::{json, Value};
use std::{
    collections::{BTreeMap, BTreeSet},
    os::unix::fs::PermissionsExt,
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
/// The durable proof is the Python-v1 runtime index; this value deliberately
/// remains process-local because plugin launch paths are not snapshot metadata.
#[derive(Debug, Clone, Default)]
pub struct Snapshot {
    pub plugin_paths: Vec<PathBuf>,
    pub plugin_roots: BTreeMap<String, PathBuf>,
}
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
pub fn verify(root: &Path, expected: &str) -> Result<Snapshot> {
    let dir = Dir::open(root)?;
    let raw = dir.read(Path::new(snapshot_tree::RUNTIME_SNAPSHOT_INDEX), 64 * 1024)?;
    let revision = serde_json::from_slice::<Value>(&raw)
        .ok()
        .and_then(|document| {
            document
                .get("materialize_revision")
                .and_then(Value::as_str)
                .map(str::to_owned)
        })
        .ok_or_else(|| Error::Integrity("runtime snapshot index is malformed".into()))?;
    let inspection = snapshot_tree::inspect_runtime_snapshots(root, &revision, expected)
        .map_err(|_| Error::Integrity("runtime snapshot index was modified".into()))?;
    if !inspection.verified {
        return Err(Error::Integrity(
            "generated runtime snapshot was modified".into(),
        ));
    }
    Ok(Snapshot::default())
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
pub fn resolve_executable(name: &str, path: &str) -> Option<PathBuf> {
    path.split(':')
        .filter(|s| !s.is_empty())
        .map(|p| Path::new(p).join(name))
        .find(|p| {
            std::fs::metadata(p)
                .map(|m| m.is_file() && m.permissions().mode() & 0o111 != 0)
                .unwrap_or(false)
        })
}
pub fn account_home(app_home: &Path, kind: Adapter, label: &str) -> PathBuf {
    app_home.join("accounts").join(kind.name()).join(label)
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
/// Qwen ignores legacy developer preset paths, variables, and required-command
/// probes while retaining its native command-denial shims.
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
                account_home(app_home, kind, a)
                    .join("claude-config")
                    .to_string_lossy()
                    .into_owned(),
            );
        }
    }
    let mut paths = Vec::new();
    let mut overrides = BTreeMap::new();
    if kind != Adapter::Qwen {
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
    }
    if let Some(e) = runtime
        .environment
        .as_ref()
        .and_then(|n| config.environments.get(n))
    {
        if !e.denied_commands.is_empty() {
            let directory = if kind == Adapter::Qwen {
                ".qwen/denied-commands"
            } else {
                "command-refusals"
            };
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
        Adapter::Qwen => {
            let auth_names = match &runtime.auth {
                Some(Auth::Environment { names }) => names.as_slice(),
                _ => &["OPENAI_API_KEY".into(), "OPENAI_BASE_URL".into()],
            };
            for name in auth_names {
                let value = auth::qwen_auth_value(name, host).ok_or_else(|| {
                    invalid(format!(
                        "qwen requires environment variable {name}, which is not set"
                    ))
                })?;
                env.insert(name.clone(), value);
            }
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
        let mut source = skill_catalog.join(skill);
        for plugin in &runtime.plugins {
            let candidate = plugin.join("skills").join(skill);
            if candidate.join("SKILL.md").is_file() {
                source = candidate;
                break;
            }
        }
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
        if kind == Adapter::Qwen {
            entry["trust"] = json!(true);
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
            let auth_target = if request.account.is_some() {
                Some("auth.json")
            } else if let Some(Auth::FileLink { target, .. }) = &runtime.auth {
                Some(target.as_str())
            } else {
                None
            };
            super::codex::render_permissions(&mut doc, runtime, home, auth_target)?;
            let text = toml::to_string_pretty(&doc)
                .map_err(|_| invalid("native config could not be serialized"))?;
            let mut config_text = native_lines.join("\n");
            config_text.push_str("\n\n");
            config_text.push_str(&text);
            p.file("config.toml", config_text.as_bytes(), 0o600)?;
            let (source, target) = if let Some(a) = request.account.as_deref() {
                (
                    account_home(app_home, kind, a).join("auth.json"),
                    "auth.json".into(),
                )
            } else if let Some(Auth::FileLink { source, target }) = &runtime.auth {
                (source.clone(), target.clone())
            } else {
                let global = std::env::var_os("CODEX_HOME")
                    .map(PathBuf::from)
                    .unwrap_or_else(|| {
                        PathBuf::from(std::env::var_os("HOME").unwrap_or_default()).join(".codex")
                    });
                (global.join("auth.json"), "auth.json".into())
            };
            p.link(&target, &source)?;
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
        Adapter::Qwen => {
            let mut settings = native;
            settings["context"] = json!({"fileName":home.join("agent-run-context.md")});
            settings["tools"] = json!({"sandbox":true});
            settings["security"] = json!({"auth":{"selectedType":"openai"}});
            settings["mcpServers"] = Value::Object(mcp);
            let mut qhooks = plugins.qwen_hooks.clone();
            if let (Some(target), Some(extra)) = (qhooks.as_object_mut(), hooks.as_object()) {
                for (k, v) in extra {
                    target
                        .entry(k.clone())
                        .or_insert(json!([]))
                        .as_array_mut()
                        .ok_or_else(|| invalid("invalid Qwen hook group"))?
                        .extend(v.as_array().cloned().unwrap_or_default());
                }
            }
            if !qhooks.is_null() && !qhooks.as_object().is_some_and(|groups| groups.is_empty()) {
                settings["hooks"] = qhooks;
            }
            if let Some(environment) = runtime
                .environment
                .as_ref()
                .and_then(|name| config.environments.get(name))
            {
                let host_path = std::env::var("PATH").unwrap_or_default();
                let mut patterns = Vec::new();
                for command in &environment.denied_commands {
                    patterns.push(command.clone());
                    if let Some(path) = resolve_executable(command, &host_path) {
                        patterns.push(path.to_string_lossy().into_owned());
                    }
                }
                patterns.sort();
                patterns.dedup();
                settings["permissions"] = json!({"deny":patterns
                    .iter()
                    .flat_map(|pattern| [format!("Bash({pattern})"), format!("Bash({pattern} *)")])
                    .collect::<Vec<_>>()});
            }
            let mut context = profile.body.clone();
            for skill in &profile.skills {
                context.push_str(&format!(
                    "\n\nDeclared skill: {}\n",
                    home.join("skills").join(skill).join("SKILL.md").display()
                ));
            }
            if !context.ends_with('\n') {
                context.push('\n');
            }
            p.file("agent-run-context.md", context.as_bytes(), 0o600)?;
            p.json(".qwen/settings.json", &settings)?;
            let commands = runtime
                .environment
                .as_ref()
                .and_then(|name| config.environments.get(name))
                .map(|environment| environment.denied_commands.as_slice())
                .unwrap_or(&[]);
            let marker = format!(
                "{{\"version\": 1, \"commands\": {}}}\n",
                serde_json::to_string(commands)?
            );
            p.file(
                ".qwen/denied-commands/.agent-run-command-policy.json",
                marker.as_bytes(),
                0o600,
            )?;
        }
    }
    if let Some(env) = runtime
        .environment
        .as_ref()
        .and_then(|n| config.environments.get(n))
    {
        for name in &env.denied_commands {
            let wrapper = "#!/bin/sh\nprintf '%s\\n' 'agent-run: command denied by owner policy' >&2\nexit 126\n";
            let directory = if kind == Adapter::Qwen {
                ".qwen/denied-commands"
            } else {
                "command-refusals"
            };
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
    p.finish()
}

#[cfg(test)]
mod tests {
    use super::*;
    use agent_run_config::config::{Capacity, Catalog, Config, Core, Delivery, Environment};
    use agent_run_config::profiles::Profile;
    use agent_run_domain::domain::StartRequest;
    use serde_json::json;
    use std::collections::{BTreeMap, BTreeSet};

    /// Builds the isolated Qwen fixture configuration used for home assertions.
    fn qwen_fixture(root: &Path) -> (Config, Runtime, StartRequest, Profile) {
        let runtime: Runtime = serde_json::from_value(json!({
            "enabled": true,
            "adapter": "qwen",
            "binary": "/bin/true",
            "home": root.join("runtime"),
            "models": ["fixture"],
            "environment": "developer",
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
            environments: BTreeMap::from([(
                "developer".into(),
                Environment {
                    denied_commands: vec!["git".into()],
                    ..Environment::default()
                },
            )]),
            runtimes: BTreeMap::new(),
        };
        let request: StartRequest = serde_json::from_value(json!({
            "runtime": "qwen", "model": "fixture", "profile": "review",
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

    /// Mirrors `test_qwen_adapter.py::test_materialize_denied_commands`.
    #[test]
    fn qwen_home_contains_native_denials_and_python_policy_marker() {
        let root = std::env::temp_dir().join(format!("agent-run-qwen-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&root).expect("owned test root");
        let (config, runtime, request, profile) = qwen_fixture(&root);
        materialize(
            &config,
            &runtime,
            &request,
            &profile,
            &root.join("home"),
            &root,
        )
        .expect("materialize Qwen home");
        let settings: Value = serde_json::from_slice(
            &std::fs::read(root.join("home/.qwen/settings.json")).expect("settings"),
        )
        .expect("settings JSON");
        assert!(settings
            .pointer("/permissions/deny")
            .and_then(Value::as_array)
            .expect("deny array")
            .iter()
            .any(|entry| entry.as_str() == Some("Bash(git)")));
        assert_eq!(
            std::fs::read_to_string(
                root.join("home/.qwen/denied-commands/.agent-run-command-policy.json")
            )
            .expect("policy marker"),
            "{\"version\": 1, \"commands\": [\"git\"]}\n"
        );
        std::fs::remove_dir_all(root).expect("remove owned test root");
    }

    /// Mirrors the empty-MCP case in `test_claude_developer_environment.py`.
    #[test]
    fn claude_home_omits_empty_mcp_configuration() {
        let root = std::env::temp_dir().join(format!("agent-run-claude-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&root).expect("owned test root");
        let (mut config, mut runtime, request, profile) = qwen_fixture(&root);
        runtime.adapter = "claude".into();
        runtime.auth = None;
        config.environments.clear();
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
