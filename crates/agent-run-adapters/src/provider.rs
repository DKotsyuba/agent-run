//! Provider-aware materialization and launch planning over the two native harnesses.
//!
//! Admission writes one sealed, credential-free runtime home. A later attempt
//! verifies those bytes and the frozen role before binding its own account.

use crate::{
    authorized_request::CredentialReader,
    materialize::{self, Snapshot},
    LaunchPlan,
};
use agent_run_config::{
    config::{self, Config, Runtime},
    policy,
    profiles::Profile,
    provider_config::ProviderConfig,
    role_plan::{role_from_authority, ResolvedRolePlan},
};
use agent_run_domain::{
    catalog::{
        AccountId, AttemptCredentials, HarnessId, ProviderCatalog, ProviderConnection, ProviderId,
        ResolvedLaunchAuthority,
    },
    domain::{Constraint, StartRequest},
    error::invalid,
    CredentialRef, Result, Sha256Digest,
};
use agent_run_platform::fs;
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::{
    collections::BTreeMap,
    path::{Path, PathBuf},
    str::FromStr,
};

/// Credential-free launch identity sealed alongside generated native assets.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct SealedProvider {
    /// Configured provider identity.
    provider: ProviderId,
    /// Digest of complete validated v2 settings, so retry cannot import edits.
    config_sha256: String,
    /// The one native execution harness.
    harness: HarnessId,
    /// Orchestrator-selected catalog model.
    model: String,
    /// Frozen native model alias, including optional context marker.
    native_model: String,
    /// Native or custom connection, without credential values.
    connection: ProviderConnection,
    /// Native executable frozen at admission.
    binary: PathBuf,
    /// Materialized plugin paths, frozen with the runtime snapshot.
    plugin_paths: Vec<PathBuf>,
    /// Immutable admitted working directory.
    workdir: PathBuf,
    /// Immutable admitted canonical role name.
    profile: String,
    /// Model-specific requirements frozen before any attempt.
    restrictions: Vec<Constraint>,
}

/// A process plan plus its sealed native model and role. The process plan
/// contains ephemeral child-only credentials and is never serializable.
pub struct ProviderLaunchPlan {
    /// Existing supervisor process plan; never log its environment.
    pub launch: LaunchPlan,
    /// Native alias passed to app-server turn start or Claude `--model`.
    pub native_model: String,
    /// Verified frozen role used for grant echo and tool checks.
    pub role: ResolvedRolePlan,
    /// V1-shaped, digest-checked harness settings for existing Codex echo checks.
    pub runtime: Runtime,
    /// Frozen role grants for existing harness echo and stream checks.
    pub profile: Profile,
}

/// Returns one v1-shaped runtime solely to reuse the established asset and
/// grant materializer; no v1 provider selection or GLM adapter is invoked.
fn runtime(config: &ProviderConfig, harness: HarnessId, model: &str) -> Result<Runtime> {
    let native = config
        .harnesses
        .get(&harness)
        .ok_or_else(|| invalid("unknown harness"))?;
    Ok(serde_json::from_value(json!({
        "enabled": true,
        "adapter": match harness { HarnessId::Codex => "codex", HarnessId::ClaudeCode => "claude" },
        "binary": native.binary,
        "home": native.home,
        "models": [model],
        "max_active_agents": native.max_active_agents,
        "native_settings": native.native_settings,
        "hooks": native.hooks,
        "plugins": native.plugins,
        "plugin_snapshot_assets": native.plugin_snapshot_assets,
        "workspace_roots": native.workspace_roots,
        "workspace_network": native.workspace_network,
        "environment": native.environment,
        "rust": native.rust,
    }))
    .map_err(|_| invalid("cannot translate validated harness settings"))?)
}

/// Returns existing shared role, skill, MCP, and environment declarations for
/// the established asset writer, without selecting a v1 runtime.
fn shared(config: &ProviderConfig) -> Config {
    Config {
        schema_version: 1,
        core: config.core.clone(),
        capacity: config.capacity.clone(),
        delivery: config.delivery.clone(),
        profiles: config.profiles.clone(),
        skills: config.skills.clone(),
        mcp: config.mcp.clone(),
        environments: config.environments.clone(),
        runtimes: BTreeMap::new(),
    }
}

/// Reconstructs role grants from a validated canonical plan rather than a
/// mutable profile file; the materializer still verifies skill/MCP assets.
fn profile(role: &ResolvedRolePlan) -> Profile {
    Profile {
        name: role.role_name.clone(),
        body: role.prompt.clone(),
        write: role.write,
        network: role.network,
        revision: role.role_revision.clone(),
        canonical: true,
        allow_external_read_roots: role.allow_external_read_roots,
        read_roots: role.read_roots.clone(),
        skills: role.skills.iter().map(|skill| skill.id.clone()).collect(),
        mcp: role.mcp.iter().map(|server| server.id.clone()).collect(),
        required_constraints: role.required_constraints.clone(),
    }
}

/// Seals one provider's native settings, tools, grants, and model alias once.
///
/// `role` must be the admitted canonical role, `workdir` an existing absolute
/// directory, and `account` an enabled in-scope registered identity. Returns
/// the digest to persist in `ResolvedLaunchAuthority.assets_sha256`; no
/// credential value enters the generated files or returned digest.
#[allow(clippy::too_many_arguments)]
pub fn materialize_selected(
    config: &ProviderConfig,
    catalog: &ProviderCatalog,
    provider: &ProviderId,
    model: &str,
    account: &AccountId,
    role: &ResolvedRolePlan,
    workdir: &Path,
    home: &Path,
    app_home: &Path,
) -> Result<(Snapshot, Sha256Digest)> {
    let definition = catalog
        .provider(provider)
        .ok_or_else(|| invalid("unknown provider"))?;
    let offering = definition
        .models
        .iter()
        .find(|offering| offering.id == model)
        .ok_or_else(|| invalid("provider model is unavailable"))?;
    let lease = AttemptCredentials::from_selected(catalog, provider, model, account)?;
    let reference = CredentialRef::from_str(lease.secret().reference())?;
    let native = matches!(definition.connection, ProviderConnection::Native);
    match (&reference, native) {
        (CredentialRef::Native(harness), true) if *harness == definition.harness => {}
        (CredentialRef::Named { harness, .. }, true) if *harness == definition.harness => {}
        (
            CredentialRef::Environment(_) | CredentialRef::File(_) | CredentialRef::Keychain { .. },
            false,
        ) => {}
        _ => {
            return Err(invalid(
                "credential reference is incompatible with provider connection",
            ))
        }
    }
    let role = ResolvedRolePlan::from_payload(&role.to_payload())?;
    if !offering
        .restrictions
        .iter()
        .all(|restriction| role.required_constraints.contains(restriction))
    {
        return Err(invalid("model restriction is absent from admitted role"));
    }
    if !workdir.is_absolute() || !workdir.is_dir() {
        return Err(invalid("provider workdir must exist"));
    }
    let runtime = runtime(config, definition.harness, model)?;
    policy::evaluate(provider.as_str(), &runtime, &profile(&role)).admit()?;
    let selected_label = match &reference {
        CredentialRef::Named { label, .. } => Some(label.as_str()),
        _ => None,
    };
    let request: StartRequest = serde_json::from_value(json!({
        "runtime": provider.as_str(), "model": model, "profile": role.role_name,
        "task": "provider materialization", "workdir": workdir,
        "account": selected_label,
    }))?;
    let config_sha256 = config
        .snapshot()?
        .get("sha256")
        .and_then(serde_json::Value::as_str)
        .ok_or_else(|| invalid("v2 config snapshot lacks a digest"))?
        .to_owned();
    let config = shared(config);
    if profile(&role)
        .mcp
        .iter()
        .any(|name| !config.mcp.contains_key(name))
    {
        return Err(invalid(
            "frozen role references an unavailable MCP definition",
        ));
    }
    if definition.harness == HarnessId::ClaudeCode && !native {
        fs::private_dir(&home.join("claude-config"))?;
    }
    let native_model = offering.native_model.as_deref().unwrap_or(&offering.id);
    let (snapshot, digest) = materialize::materialize_provider(
        &config,
        &runtime,
        &request,
        &profile(&role),
        home,
        app_home,
        definition,
        native_model,
        &config_sha256,
    )?;
    Ok((snapshot, Sha256Digest::from_str(&digest)?))
}

/// Loads one sealed provider record after runtime-index verification and
/// rejects drift from the durable launch authority before credentials bind.
fn sealed(home: &Path, authority: &ResolvedLaunchAuthority) -> Result<SealedProvider> {
    materialize::verify(home, authority.assets_sha256.as_str())?;
    let raw = fs::Dir::open(home)?.read(Path::new("provider-launch.json"), 64 * 1024)?;
    let sealed: SealedProvider = serde_json::from_slice(&raw)
        .map_err(|_| invalid("sealed provider launch metadata is invalid"))?;
    if sealed.provider != authority.provider
        || sealed.harness != authority.harness
        || sealed.model != authority.model
        || sealed.connection != authority.connection
        || sealed.workdir != authority.workdir
        || sealed.profile != authority.profile
        || !sealed.binary.is_absolute()
    {
        return Err(invalid("sealed provider launch differs from authority"));
    }
    Ok(sealed)
}

/// Links the selected native Codex auth file only after immutable assets
/// verify. The link is deliberately outside the sealed asset manifest so a
/// later attempt can bind another account after session-owned cleanup.
fn bind_native_codex(
    reference: &CredentialRef,
    home: &Path,
    app_home: &Path,
    host: &BTreeMap<String, String>,
) -> Result<()> {
    let source = match reference {
        CredentialRef::Native(HarnessId::Codex) => host
            .get("CODEX_HOME")
            .map(PathBuf::from)
            .or_else(|| {
                host.get("HOME")
                    .map(|value| PathBuf::from(value).join(".codex"))
            })
            .ok_or_else(|| invalid("native Codex home is unavailable"))?
            .join("auth.json"),
        CredentialRef::Named {
            harness: HarnessId::Codex,
            label,
        } => materialize::account_home(app_home, config::Adapter::Codex, label.as_str())
            .join("auth.json"),
        _ => return Err(invalid("selected account is not a native Codex login")),
    };
    materialize::Publisher::new(home)?.link("auth.json", &source)
}

/// Builds one attempt from verified immutable assets and the selected account.
///
/// The caller supplies fresh task text and an optional exact native session
/// id; neither can alter frozen model, grants, connection, binary, or assets.
/// `reader` resolves custom credentials only into the child environment.
#[allow(clippy::too_many_arguments)]
pub fn plan_selected(
    config: &ProviderConfig,
    catalog: &ProviderCatalog,
    authority: &ResolvedLaunchAuthority,
    account: &AccountId,
    home: &Path,
    app_home: &Path,
    host: &BTreeMap<String, String>,
    reader: &impl CredentialReader,
    task: &str,
    resume_session: Option<&str>,
) -> Result<ProviderLaunchPlan> {
    let role = role_from_authority(authority, &authority.assets_sha256)?;
    let sealed = sealed(home, authority)?;
    if config.snapshot()?["sha256"] != sealed.config_sha256 {
        return Err(invalid("provider configuration changed since admission"));
    }
    if task.trim().is_empty() || resume_session.is_some_and(|id| id.is_empty()) {
        return Err(invalid("provider task or resume session is empty"));
    }
    if !authority.eligible_accounts.contains(account) {
        return Err(invalid("selected account is outside frozen authority"));
    }
    let definition = catalog
        .provider(&authority.provider)
        .ok_or_else(|| invalid("unknown provider"))?;
    if definition.harness != authority.harness || definition.connection != authority.connection {
        return Err(invalid("provider connection changed since admission"));
    }
    let lease =
        AttemptCredentials::from_selected(catalog, &authority.provider, &authority.model, account)?;
    if !sealed
        .restrictions
        .iter()
        .all(|restriction| role.required_constraints.contains(restriction))
    {
        return Err(invalid("sealed model restriction is absent from role"));
    }
    let reference = CredentialRef::from_str(lease.secret().reference())?;
    let runtime = runtime(config, sealed.harness, &authority.model)?;
    policy::evaluate(authority.provider.as_str(), &runtime, &profile(&role)).admit()?;
    let selected_label = match &reference {
        CredentialRef::Named { label, .. } => Some(label.as_str()),
        _ => None,
    };
    let mut environment = materialize::environment_with_host(
        &shared(config),
        &runtime,
        &profile(&role),
        home,
        selected_label,
        app_home,
        host,
    )?;
    let host_home = host
        .get("HOME")
        .cloned()
        .ok_or_else(|| invalid("HOME is unavailable"))?;
    environment.insert(
        "HOME".into(),
        if sealed.harness == HarnessId::ClaudeCode && matches!(reference, CredentialRef::Native(_))
        {
            host_home
        } else {
            home.to_string_lossy().into_owned()
        },
    );
    match (&sealed.connection, &reference, sealed.harness) {
        (ProviderConnection::Native, CredentialRef::Native(harness), _)
        | (ProviderConnection::Native, CredentialRef::Named { harness, .. }, _)
            if *harness == sealed.harness => {}
        (
            ProviderConnection::Custom {
                endpoint,
                auth_header,
                ..
            },
            CredentialRef::Environment(_) | CredentialRef::File(_) | CredentialRef::Keychain { .. },
            HarnessId::Codex,
        ) => {
            if *auth_header != agent_run_domain::CredentialHeader::Bearer {
                return Err(invalid(
                    "Codex custom gateway requires bearer authorization",
                ));
            }
            environment.insert("AGENT_RUN_PROVIDER_TOKEN".into(), reader.read(&reference)?);
            // The sealed Codex config names this env key and excludes it from tool shells.
            let _ = endpoint;
        }
        (
            ProviderConnection::Custom {
                endpoint,
                auth_header,
                ..
            },
            CredentialRef::Environment(_) | CredentialRef::File(_) | CredentialRef::Keychain { .. },
            HarnessId::ClaudeCode,
        ) => {
            environment.insert(
                "CLAUDE_CONFIG_DIR".into(),
                home.join("claude-config").to_string_lossy().into_owned(),
            );
            environment.insert("ANTHROPIC_BASE_URL".into(), endpoint.clone());
            environment.remove("ANTHROPIC_AUTH_TOKEN");
            environment.remove("ANTHROPIC_API_KEY");
            environment.insert(
                match auth_header {
                    agent_run_domain::CredentialHeader::Bearer => "ANTHROPIC_AUTH_TOKEN",
                    agent_run_domain::CredentialHeader::XApiKey => "ANTHROPIC_API_KEY",
                }
                .into(),
                reader.read(&reference)?,
            );
            environment.insert("ANTHROPIC_MODEL".into(), sealed.native_model.clone());
        }
        _ => {
            return Err(invalid(
                "credential reference is incompatible with provider connection",
            ))
        }
    }
    if sealed.harness == HarnessId::Codex {
        if matches!(sealed.connection, ProviderConnection::Native) {
            bind_native_codex(&reference, home, app_home, host)?;
        }
        environment.insert("CODEX_HOME".into(), home.to_string_lossy().into_owned());
    } else if let CredentialRef::Named { label, .. } = &reference {
        environment.insert(
            "CLAUDE_CONFIG_DIR".into(),
            materialize::claude_account_config(app_home, &runtime.home, label.as_str())?
                .to_string_lossy()
                .into_owned(),
        );
    }
    let args = if sealed.harness == HarnessId::Codex {
        vec!["app-server".into()]
    } else {
        claude_args(
            &sealed,
            &role,
            home,
            authority.effort.as_deref(),
            resume_session,
        )?
    };
    Ok(ProviderLaunchPlan {
        launch: LaunchPlan {
            binary: sealed.binary,
            args,
            cwd: authority.workdir.clone(),
            environment,
            initial_input: (sealed.harness == HarnessId::ClaudeCode).then(|| format!(
                "{}\n", json!({"type":"user","message":{"role":"user","content":[{"type":"text","text":task}]}})
            )),
        },
        native_model: sealed.native_model,
        runtime,
        profile: profile(&role),
        role,
    })
}

/// Builds Claude's partial-message stream with the sealed model, role grants,
/// MCP/plugin assets, and exact resume id, without model-name special cases.
fn claude_args(
    sealed: &SealedProvider,
    role: &ResolvedRolePlan,
    home: &Path,
    effort: Option<&str>,
    resume_session: Option<&str>,
) -> Result<Vec<String>> {
    let mut tools = vec!["Read", "Grep", "Glob"];
    if !role.skills.is_empty() {
        tools.push("Skill");
    }
    if role.write {
        tools.extend(["Edit", "Write", "NotebookEdit", "Bash"]);
    }
    if role.network {
        tools.extend(["WebFetch", "WebSearch"]);
    }
    let mut allowed = tools
        .iter()
        .map(|tool| {
            if matches!(*tool, "Edit" | "Write" | "NotebookEdit") {
                format!("{tool}({}/**)", sealed.workdir.display())
            } else {
                (*tool).to_owned()
            }
        })
        .collect::<Vec<_>>();
    allowed.extend(role.mcp.iter().map(|server| format!("mcp__{}", server.id)));
    let denied = if role.network {
        String::new()
    } else {
        "WebFetch,WebSearch".into()
    };
    let mut args = vec![
        "--print".into(),
        "--output-format".into(),
        "stream-json".into(),
        "--include-partial-messages".into(),
        "--input-format".into(),
        "stream-json".into(),
        "--verbose".into(),
        "--model".into(),
        sealed.native_model.clone(),
        "--permission-mode".into(),
        if role.write { "acceptEdits" } else { "default" }.into(),
        "--setting-sources".into(),
        "".into(),
        "--strict-mcp-config".into(),
        "--settings".into(),
        home.join("settings.json").to_string_lossy().into_owned(),
    ];
    for root in &role.read_roots {
        args.extend(["--add-dir".into(), root.to_string_lossy().into_owned()]);
    }
    args.extend([
        "--tools".into(),
        tools.join(","),
        "--allowedTools".into(),
        allowed.join(","),
        "--disallowedTools".into(),
        denied,
    ]);
    if !role.mcp.is_empty() {
        args.extend([
            "--mcp-config".into(),
            home.join("mcp/mcp-config.json")
                .to_string_lossy()
                .into_owned(),
        ]);
    }
    for plugin in &sealed.plugin_paths {
        args.extend(["--plugin-dir".into(), plugin.to_string_lossy().into_owned()]);
    }
    if let Some(effort) = effort {
        args.extend(["--effort".into(), effort.into()]);
    }
    args.extend(["--append-system-prompt".into(), role.prompt.clone()]);
    if let Some(session) = resume_session {
        args.extend(["--resume".into(), session.into()]);
    } else {
        args.extend(["--session-id".into(), uuid::Uuid::new_v4().to_string()]);
    }
    Ok(args)
}
