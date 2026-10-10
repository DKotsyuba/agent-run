//! Provider-aware materialization and launch planning over the two native harnesses.
//!
//! Admission writes one sealed, credential-free runtime home. A later attempt
//! verifies those bytes and the frozen role before binding its own account.

use crate::{
    LaunchPlan,
    authorized_request::CredentialReader,
    materialize::{self, Snapshot},
};
use agent_run_config::{
    config::{self, Config, Runtime},
    policy,
    profiles::Profile,
    provider_config::ProviderConfig,
    role_plan::{ResolvedRolePlan, role_from_authority},
};
use agent_run_domain::{
    CredentialRef, Result, Sha256Digest,
    catalog::{
        AccountId, AttemptCredentials, HarnessId, ProviderCatalog, ProviderConnection, ProviderId,
        ResolvedLaunchAuthority,
    },
    domain::{Constraint, StartRequest},
    error::invalid,
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
/// grant materializer and policy evaluation; no v1 provider selection or GLM
/// adapter is invoked. Errors when `harness` is not configured.
pub fn runtime(config: &ProviderConfig, harness: HarnessId, model: &str) -> Result<Runtime> {
    let native = config
        .harnesses
        .get(&harness)
        .ok_or_else(|| invalid("unknown harness"))?;
    serde_json::from_value(json!({
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
    .map_err(|_| invalid("cannot translate validated harness settings"))
}

/// Returns existing shared role, skill, MCP, and environment declarations for
/// the established asset writer, without selecting a v1 runtime. When frozen
/// `worker_mcp` is enabled, add the built-in server using the current executable
/// and environment names only; executable lookup can fail without writing files.
fn shared(config: &ProviderConfig, worker_mcp: bool) -> Result<Config> {
    let mut shared = Config {
        schema_version: 1,
        core: config.core.clone(),
        capacity: config.capacity.clone(),
        delivery: config.delivery.clone(),
        profiles: config.profiles.clone(),
        skills: config.skills.clone(),
        mcp: config.mcp.clone(),
        environments: config.environments.clone(),
        runtimes: BTreeMap::new(),
    };
    if worker_mcp {
        shared.mcp.insert(
            agent_run_domain::worker::SERVER_NAME.into(),
            config::Mcp {
                transport: "stdio".into(),
                command: std::env::current_exe()?,
                args: vec!["_worker-mcp".into()],
                env_from: agent_run_domain::worker::ENV_NAMES
                    .iter()
                    .map(|name| (*name).into())
                    .collect(),
                approval_mode: "approve".into(),
                global: false,
                allowed_tools: None,
            },
        );
    }
    Ok(shared)
}

/// Reconstructs role grants from a validated canonical plan rather than a
/// mutable profile file; the materializer still verifies skill/MCP assets.
/// Includes the built-in worker namespace only when frozen in the role.
pub fn profile(role: &ResolvedRolePlan) -> Profile {
    let mut profile = Profile {
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
        mcp_tools: role
            .mcp
            .iter()
            .filter_map(|server| {
                server
                    .allowed_tools
                    .clone()
                    .map(|tools| (server.id.clone(), tools))
            })
            .collect(),
        required_constraints: role.required_constraints.clone(),
    };
    if role.worker_mcp {
        profile
            .mcp
            .push(agent_run_domain::worker::SERVER_NAME.into());
    }
    profile
}

/// Validates an enabled model-bound account's reference against the actual
/// native/custom launch contract without opening credentials or spawning.
/// Returns its parsed nonsecret reference; rejects mismatched harnesses,
/// connection types and unsupported Codex authorization headers. Admission,
/// materialization and execution call this same check; live auth is read later.
pub fn credential_reference(
    catalog: &ProviderCatalog,
    provider: &ProviderId,
    model: &str,
    account: &AccountId,
) -> Result<CredentialRef> {
    let definition = catalog
        .provider(provider)
        .ok_or_else(|| invalid("unknown provider"))?;
    let lease = AttemptCredentials::from_selected(catalog, provider, model, account)?;
    let reference = CredentialRef::from_str(lease.secret().reference())?;
    match (&definition.connection, &reference) {
        (ProviderConnection::Native, CredentialRef::Native(harness))
        | (ProviderConnection::Native, CredentialRef::Named { harness, .. })
            if *harness == definition.harness => {}
        (
            ProviderConnection::Custom { auth_header, .. },
            CredentialRef::Environment(_) | CredentialRef::File(_) | CredentialRef::Keychain { .. },
        ) => {
            if definition.harness == HarnessId::Codex
                && *auth_header != agent_run_domain::CredentialHeader::Bearer
            {
                return Err(invalid(
                    "Codex custom gateway requires bearer authorization",
                ));
            }
        }
        _ => {
            return Err(invalid(
                "credential reference is incompatible with provider connection",
            ));
        }
    }
    Ok(reference)
}

/// Seals one provider's native settings, tools, grants, and model alias once.
///
/// `role` must be the admitted canonical role, `workdir` an existing absolute
/// directory, and `account` an enabled in-scope registered identity. Returns
/// the digest to persist in `ResolvedLaunchAuthority.assets_sha256`; no
/// credential value enters the generated files or returned digest. Claude Code
/// plugin skills are allowed only when the frozen role declares them.
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
    let reference = credential_reference(catalog, provider, model, account)?;
    let native = matches!(definition.connection, ProviderConnection::Native);
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
    let role_profile = profile(&role);
    let mut runtime = runtime(config, definition.harness, model)?;
    crate::validate_executable(&runtime)?;
    crate::validate_role(&runtime, &role_profile)?;
    if definition.harness == HarnessId::ClaudeCode {
        runtime.skills = role_profile.skills.clone();
    }
    policy::evaluate(provider.as_str(), &runtime, &role_profile).admit()?;
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
    let config = shared(config, role.worker_mcp)?;
    if role_profile
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
        &role_profile,
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
///
/// With `shared` the same original index proof runs through
/// [`materialize::verify_with_shared`], which additionally proves each mapped
/// root is exactly one whole-tree symlink to the object the registry bound
/// and that the shared tree's own content is unchanged; a private copy, a
/// dangling link or a foreign target is never accepted. Without it the home
/// verifies strictly as a private tree, exactly as before.
fn sealed(
    home: &Path,
    authority: &ResolvedLaunchAuthority,
    shared: Option<&SharedLaunchAssets>,
) -> Result<SealedProvider> {
    match shared {
        Some(assets) => materialize::verify_with_shared(
            home,
            authority.assets_sha256.as_str(),
            &assets.store_root,
            &assets.roots,
        )?,
        None => materialize::verify(home, authority.assets_sha256.as_str())?,
    };
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

/// Verified shared-store placement backing one sealed home's managed trees.
///
/// `store_root` is the trusted canonical shared-store root and `roots` maps
/// each indexed managed-root path to the shared tree the durable registry
/// bound when it was imported. The pair may only be built from a `committed`
/// registry row whose layout already proved through the registry's own
/// verifier: a missing, still-pending or corrupt row yields `None` and the
/// strict private-home verifier stays in force. Passing it never widens what
/// is accepted — the same original index bytes and hashes are checked, plus
/// the shared targets' physical integrity.
#[derive(Debug, Clone)]
pub struct SharedLaunchAssets {
    /// Canonical caller-owned shared store root.
    pub store_root: PathBuf,
    /// Indexed managed-root path to its bound shared tree reference.
    pub roots: BTreeMap<String, agent_run_platform::shared_assets::SharedTreeRef>,
}

/// Per-request harness options frozen with the admitted request.
///
/// `fast` asks the codex harness for its fast service tier; `output_schema`
/// asks the claude-code harness to answer as JSON matching the schema.
/// Admission rejects either on the other harness.
#[derive(Debug, Clone, Copy, Default)]
pub struct LaunchOptions<'a> {
    /// Codex fast service tier.
    pub fast: bool,
    /// Claude JSON answer schema.
    pub output_schema: Option<&'a serde_json::Map<String, serde_json::Value>>,
}

/// [`plan_selected_with`] with no per-request harness options and a strictly
/// private sealed home.
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
    plan_selected_with(
        config,
        catalog,
        authority,
        account,
        home,
        app_home,
        host,
        reader,
        task,
        resume_session,
        LaunchOptions::default(),
        None,
    )
}

/// Builds one attempt from verified immutable assets and the selected account.
/// Host worker capabilities and mode markers are discarded; the supervisor
/// supplies them after planning from the current attempt and frozen authority.
///
/// The caller supplies fresh task text, an optional exact native session id
/// and the admitted request's [`LaunchOptions`]; none can alter frozen
/// model, grants, connection, binary, or assets. `reader` resolves custom
/// credentials only into the child environment. `shared` names the
/// committed shared-store placement of the home's managed trees, when the
/// registry recorded one; without it the sealed home verifies strictly as a
/// private tree. The returned plan always launches the sealed native binary
/// directly: agent-run adds no launch wrapper, so the shared store is guarded
/// by owner-only modes and by digest verification before each launch, never by
/// an OS-level write denial on the child or its descendants.
#[allow(clippy::too_many_arguments)]
pub fn plan_selected_with(
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
    options: LaunchOptions<'_>,
    shared_assets: Option<&SharedLaunchAssets>,
) -> Result<ProviderLaunchPlan> {
    let role = role_from_authority(authority, &authority.assets_sha256)?;
    let sealed = sealed(home, authority, shared_assets)?;
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
    let reference = credential_reference(catalog, &authority.provider, &authority.model, account)?;
    if !sealed
        .restrictions
        .iter()
        .all(|restriction| role.required_constraints.contains(restriction))
    {
        return Err(invalid("sealed model restriction is absent from role"));
    }
    let runtime = runtime(config, sealed.harness, &authority.model)?;
    crate::validate_executable(&runtime)?;
    crate::validate_role(&runtime, &profile(&role))?;
    policy::evaluate(authority.provider.as_str(), &runtime, &profile(&role)).admit()?;
    let selected_label = match &reference {
        CredentialRef::Named { label, .. } => Some(label.as_str()),
        _ => None,
    };
    // The supervisor injects the worker capability after planning. Only user
    // MCP environment declarations are sourced from the host; a worker must
    // never inherit another attempt's capability from an upstream environment.
    let mut environment_profile = profile(&role);
    if role.worker_mcp {
        environment_profile
            .mcp
            .retain(|name| name != agent_run_domain::worker::SERVER_NAME);
    }
    let shared_config = shared(config, false)?;
    let mut environment = materialize::environment_with_host(
        &shared_config,
        &runtime,
        &environment_profile,
        home,
        selected_label,
        app_home,
        host,
    )?;
    for name in agent_run_domain::worker::ENV_NAMES.into_iter().chain([
        agent_run_domain::worker::FINISH_ENV,
        agent_run_domain::worker::RESEARCH_ENV,
    ]) {
        environment.remove(name);
    }
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
            ProviderConnection::Custom { endpoint, .. },
            CredentialRef::Environment(_) | CredentialRef::File(_) | CredentialRef::Keychain { .. },
            HarnessId::Codex,
        ) => {
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
            ));
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
        // The same fast-tier overrides the historical codex launch uses.
        let mut args: Vec<String> = Vec::new();
        if options.fast {
            args.extend([
                "-c".into(),
                "service_tier=fast".into(),
                "-c".into(),
                "features.fast_mode=true".into(),
            ]);
        }
        args.push("app-server".into());
        args
    } else {
        claude_args(
            &sealed,
            &role,
            home,
            authority.effort.as_deref(),
            resume_session,
            options.output_schema,
        )?
    };
    Ok(ProviderLaunchPlan {
        launch: LaunchPlan {
            binary: sealed.binary.clone(),
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

/// Builds Claude's partial-message stream with input replay acknowledgements,
/// the sealed model, role grants, MCP/plugin assets and exact resume id.
/// Replay frames let the shared runner correlate coalesced input batches without
/// changing permissions, model selection or the public agent identity.
fn claude_args(
    sealed: &SealedProvider,
    role: &ResolvedRolePlan,
    home: &Path,
    effort: Option<&str>,
    resume_session: Option<&str>,
    output_schema: Option<&serde_json::Map<String, serde_json::Value>>,
) -> Result<Vec<String>> {
    let research = role.research_tools_only();
    let mut tools = if research {
        vec![]
    } else {
        vec!["Read", "Grep", "Glob"]
    };
    if !research && !role.skills.is_empty() {
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
    if role.worker_mcp {
        allowed.push(format!("mcp__{}", agent_run_domain::worker::SERVER_NAME));
    }
    let denied = if research {
        "Bash,Agent,Task,Skill,Read,Grep,Glob,Edit,Write,NotebookEdit".into()
    } else if role.network {
        String::new()
    } else {
        "WebFetch,WebSearch".into()
    };
    let mut args = vec![
        "--print".into(),
        "--output-format".into(),
        "stream-json".into(),
        "--include-partial-messages".into(),
        "--replay-user-messages".into(),
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
    if !role.mcp.is_empty() || role.worker_mcp {
        args.extend([
            "--mcp-config".into(),
            home.join("mcp/mcp-config.json")
                .to_string_lossy()
                .into_owned(),
        ]);
    }
    if research {
        args.extend([
            "--restricted".into(),
            "--disable-slash-commands".into(),
            "--no-chrome".into(),
        ]);
    }
    for plugin in sealed.plugin_paths.iter().filter(|_| !research) {
        args.extend(["--plugin-dir".into(), plugin.to_string_lossy().into_owned()]);
    }
    if let Some(effort) = effort {
        args.extend(["--effort".into(), effort.into()]);
    }
    let mut prompt = role.prompt.clone();
    if let Some(schema) = output_schema {
        // The same schema instruction the historical Claude stream uses.
        prompt.push_str(&format!(
            "\n\nReturn only JSON matching this schema: {}",
            serde_json::to_string(schema)?
        ));
    }
    args.extend(["--append-system-prompt".into(), prompt]);
    if let Some(session) = resume_session {
        args.extend(["--resume".into(), session.into()]);
    } else {
        args.extend(["--session-id".into(), uuid::Uuid::new_v4().to_string()]);
    }
    Ok(args)
}
