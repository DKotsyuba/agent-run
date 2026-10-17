//! Fake-engine evidence for configured provider materialization and launch.

use agent_run_adapters::{
    authorized_request::CredentialReader,
    provider::{effective_skill_sources, materialize_selected, plan_selected},
};
use agent_run_config::{
    profiles::Profile,
    provider_config::ProviderConfig,
    role_plan::{
        ResolvedRolePlan, resolve_role_plan, resolve_role_plan_with_sources, tree_revision,
    },
};
use agent_run_domain::{
    CredentialRef, Result, Sha256Digest,
    catalog::{
        AccountRecord, AccountStatus, HarnessId, ProviderCatalog, ProviderId,
        ResolvedLaunchAuthority,
    },
    domain::Constraint,
};
use std::{
    collections::{BTreeMap, BTreeSet},
    fs,
    os::unix::fs::PermissionsExt,
    path::Path,
    process::Command,
};

/// Supplies a synthetic selected credential without reading a host store.
struct FakeReader;

impl CredentialReader for FakeReader {
    /// Returns a test value for an explicit environment-store reference.
    fn read(&self, reference: &CredentialRef) -> Result<String> {
        assert_eq!(reference.kind(), "environment");
        Ok("synthetic-secret".into())
    }
}

/// Writes an executable that reports safe argv and gateway metadata only.
fn fake_engine(root: &Path) -> String {
    let path = root.join("fake-engine");
    fs::write(&path, "#!/bin/sh\nprintf '%s\\n' \"$@\"\nprintf '%s\\n' \"$ANTHROPIC_BASE_URL\" \"$ANTHROPIC_MODEL\"\n").unwrap();
    fs::set_permissions(&path, fs::Permissions::from_mode(0o700)).unwrap();
    path.to_string_lossy().into_owned()
}

/// Parses explicit native Codex and custom Codex/Claude providers.
fn config(root: &Path, binary: &str) -> ProviderConfig {
    ProviderConfig::parse(
        &format!(
            r#"
schema_version = 2
[harnesses.codex]
binary = "{binary}"
home = "{root}/codex"
[harnesses.claude-code]
binary = "{binary}"
home = "{root}/claude"
[providers.codex-plus]
harness = "codex"
connection = {{ kind = "native" }}
auth_family = "openai"
limits_source = "none"
[[providers.codex-plus.models]]
id = "gpt"
native_model = "gpt-native"
[[providers.codex-plus.bindings]]
label = "work"
account = "acct-native"
[[providers.codex-plus.bindings]]
label = "other"
account = "acct-native-b"
[providers.codex-gateway]
harness = "codex"
connection = {{ kind = "custom", endpoint = "https://openai.example/v1", protocol = "responses" }}
auth_family = "openai"
limits_source = "none"
[[providers.codex-gateway.models]]
id = "gpt"
native_model = "gpt-custom"
[[providers.codex-gateway.bindings]]
label = "gateway"
account = "acct-openai"
[providers.claude-main]
harness = "claude-code"
connection = {{ kind = "native" }}
auth_family = "anthropic"
limits_source = "none"
[[providers.claude-main.models]]
id = "sonnet"
native_model = "claude-sonnet"
[[providers.claude-main.bindings]]
label = "global"
account = "acct-claude"
[providers.glm-any]
harness = "claude-code"
connection = {{ kind = "custom", endpoint = "https://gateway.example/api", protocol = "messages" }}
auth_family = "anthropic"
limits_source = "exec"
collector = {{ command = "/bin/true" }}
[[providers.glm-any.models]]
id = "glm"
native_model = "glm-5.3[1m]"
restrictions = ["web_tools_disabled"]
[[providers.glm-any.bindings]]
label = "work"
account = "acct-glm"
"#,
            root = root.display()
        ),
        root,
    )
    .unwrap()
}

/// Resolves only nonsecret fake references against the provider declarations.
fn catalog(config: &ProviderConfig) -> ProviderCatalog {
    let accounts = [
        ("acct-native", "openai", "named:codex:work"),
        ("acct-native-b", "openai", "named:codex:other"),
        ("acct-openai", "openai", "env:FAKE_OPENAI_KEY"),
        ("acct-claude", "anthropic", "native:claude-code"),
        ("acct-glm", "anthropic", "env:FAKE_TOKEN"),
    ]
    .into_iter()
    .map(|(id, family, reference)| AccountRecord {
        account_id: id.parse().unwrap(),
        auth_family: family.parse().unwrap(),
        secret_ref: reference.parse().unwrap(),
        status: AccountStatus::Enabled,
    })
    .collect();
    config.resolve_catalog(accounts).unwrap()
}

/// Freezes a read-only canonical role carrying the configured model restriction.
fn role(root: &Path, network: bool) -> ResolvedRolePlan {
    let profile = Profile {
        name: "review".into(),
        body: "Review safely.".into(),
        write: false,
        network,
        revision: "1".into(),
        canonical: true,
        allow_external_read_roots: false,
        read_roots: vec![],
        skills: vec![],
        mcp: vec![],
        mcp_tools: Default::default(),
        required_constraints: BTreeSet::from([Constraint::WebToolsDisabled]),
    };
    resolve_role_plan(&profile, root, &BTreeMap::new(), "account", Some("work")).unwrap()
}

/// Restricted research excludes global command-capable MCP declarations,
/// materializes no plugin execution path and exposes native web plus reports.
/// Ordinary role materialization remains covered by the existing golden cases.
#[test]
fn research_native_plans_remove_command_and_inherited_tools() {
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().canonicalize().unwrap();
    let mut config = config(&root, &fake_engine(&root));
    for harness in config.harnesses.values_mut() {
        harness.hooks.push(agent_run_config::config::Hook {
            event: "SessionStart".into(),
            command: vec!["/usr/bin/false".into()],
            matcher: None,
        });
    }
    config.mcp.insert(
        "execution".into(),
        serde_json::from_value(serde_json::json!({
            "transport":"stdio","command":"/usr/bin/true","args":[],"env_from":[],"approval_mode":"auto","global":true
        }))
        .unwrap(),
    );
    let account_home = agent_run_adapters::materialize::account_home(
        &root,
        agent_run_config::config::Adapter::Codex,
        "work",
    );
    fs::create_dir_all(&account_home).unwrap();
    fs::write(account_home.join("auth.json"), "{}\n").unwrap();
    fs::set_permissions(
        account_home.join("auth.json"),
        fs::Permissions::from_mode(0o600),
    )
    .unwrap();
    let catalog = catalog(&config);
    let profile = Profile {
        name: "research".into(),
        body: "Search, retrieve primary sources and save_report.".into(),
        write: false,
        network: true,
        revision: "research-reports-v1".into(),
        canonical: true,
        allow_external_read_roots: false,
        read_roots: vec![],
        skills: vec![],
        mcp: vec![],
        mcp_tools: Default::default(),
        required_constraints: BTreeSet::from([Constraint::ResearchToolsOnly]),
    };
    let legacy: agent_run_domain::domain::StartRequest = serde_json::from_value(serde_json::json!({
        "runtime":"legacy","model":"sonnet","profile":"research","task":"Research","workdir":root
    })).unwrap();
    let legacy_runtime = agent_run_adapters::provider::runtime(
        &config,
        agent_run_domain::HarnessId::ClaudeCode,
        "sonnet",
    )
    .unwrap();
    assert!(
        agent_run_adapters::validate(&legacy, &legacy_runtime, &profile)
            .unwrap_err()
            .to_string()
            .contains("schema-2 provider launch")
    );
    let role = resolve_role_plan(&profile, &root, &config.mcp, "global", None).unwrap();
    assert!(role.mcp.is_empty());
    for (provider, model, account) in [
        ("codex-plus", "gpt", "acct-native"),
        ("claude-main", "sonnet", "acct-claude"),
    ] {
        let home = root.join(provider);
        let provider: ProviderId = provider.parse().unwrap();
        let account_id = account.parse().unwrap();
        let materialized = materialize_selected(
            &config,
            &catalog,
            &provider,
            model,
            &account_id,
            &role,
            &root,
            &home,
            &root,
        );
        let (_, digest) = materialized.unwrap();
        let authority = make_authority(&catalog, &provider, model, &role, &root, digest, account);
        let plan = plan_selected(
            &config,
            &catalog,
            &authority,
            &account_id,
            &home,
            &root,
            &BTreeMap::from([("HOME".into(), root.display().to_string())]),
            &FakeReader,
            "bounded research",
            None,
        )
        .unwrap();
        assert!(plan.launch.args.iter().all(|arg| arg != "--plugin-dir"));
        if provider.as_str() == "codex-plus" {
            let native: toml::Value =
                toml::from_str(&fs::read_to_string(home.join("config.toml")).unwrap()).unwrap();
            let native = serde_json::to_value(native).unwrap();
            agent_run_adapters::codex::verify_research_config(
                &serde_json::json!({"config":native}),
            )
            .unwrap();
            assert_eq!(native["tools"]["view_image"], false);
            assert_eq!(native["mcp_servers"].as_object().unwrap().len(), 1);
            let worker = &native["mcp_servers"]["agent_run_worker"];
            assert_eq!(worker["required"], true);
            assert!(
                worker["env_vars"]
                    .as_array()
                    .unwrap()
                    .contains(&serde_json::json!(agent_run_domain::worker::RESEARCH_ENV))
            );
            assert!(native.get("hooks").is_none());
            assert_eq!(plan.launch.args, ["app-server"]);
            continue;
        }
        let value = |flag: &str| {
            plan.launch.args[plan.launch.args.iter().position(|arg| arg == flag).unwrap() + 1]
                .clone()
        };
        assert_eq!(value("--tools"), "WebFetch,WebSearch");
        assert!(value("--disallowedTools").contains("Bash"));
        assert!(plan.launch.args.iter().any(|arg| arg == "--restricted"));
        assert!(
            plan.launch
                .args
                .iter()
                .any(|arg| arg == "--disable-slash-commands")
        );
    }
}

/// Historical role payloads do not acquire a worker MCP when materialized by
/// a newer executable; both native harness config formats keep that boundary.
#[test]
fn historical_role_keeps_worker_channel_absent() {
    let home = tempfile::tempdir().unwrap();
    let root = home.path();
    let config = config(root, &fake_engine(root));
    let catalog = catalog(&config);
    let mut payload = role(root, false).to_payload();
    let seed = payload.as_object_mut().unwrap();
    seed.remove("worker_mcp");
    seed.remove("config_revision");
    payload["config_revision"] =
        serde_json::json!(agent_run_domain::canonical::sha256_hex(&payload, true));
    let historical = ResolvedRolePlan::from_payload(&payload).unwrap();
    assert!(!historical.worker_mcp);
    for (provider, model, account) in [
        ("codex-plus", "gpt", "acct-native"),
        ("claude-main", "sonnet", "acct-claude"),
    ] {
        let run_home = root.join(provider);
        materialize_selected(
            &config,
            &catalog,
            &provider.parse().unwrap(),
            model,
            &account.parse().unwrap(),
            &historical,
            root,
            &run_home,
            root,
        )
        .unwrap();
        assert!(!run_home.join("mcp/mcp-config.json").exists());
        if provider == "codex-plus" {
            assert!(
                !fs::read_to_string(run_home.join("config.toml"))
                    .unwrap()
                    .contains("agent_run_worker")
            );
        }
    }
}

/// Claude Code providers admit every plugin skill declared by the frozen role
/// and still refuse an undeclared skill shipped by the same plugin.
#[test]
fn claude_provider_uses_frozen_role_as_plugin_skill_allowlist() {
    let home = tempfile::tempdir().unwrap();
    let root = home.path();
    let plugin = root.join("routing-plugin");
    fs::create_dir_all(plugin.join(".claude-plugin")).unwrap();
    fs::write(
        plugin.join(".claude-plugin/plugin.json"),
        serde_json::json!({"name":"routing-plugin","version":"1.0.0"}).to_string(),
    )
    .unwrap();
    fs::create_dir_all(plugin.join("skills/plugin-review")).unwrap();
    fs::write(plugin.join("skills/plugin-review/SKILL.md"), "Review.").unwrap();
    let skills = root.join("skills/plugin-review");
    fs::create_dir_all(&skills).unwrap();
    fs::write(skills.join("SKILL.md"), "Review.").unwrap();

    let mut config = config(root, &fake_engine(root));
    config
        .harnesses
        .get_mut(&HarnessId::ClaudeCode)
        .unwrap()
        .plugins
        .push(plugin);
    let catalog = catalog(&config);
    let mut profile = Profile {
        name: "review".into(),
        body: "Review safely.".into(),
        write: false,
        network: false,
        revision: "1".into(),
        canonical: true,
        allow_external_read_roots: false,
        read_roots: vec![],
        skills: vec!["plugin-review".into()],
        mcp: vec![],
        mcp_tools: Default::default(),
        required_constraints: BTreeSet::from([Constraint::WebToolsDisabled]),
    };
    let role = resolve_role_plan(
        &profile,
        &root.join("skills"),
        &BTreeMap::new(),
        "account",
        Some("work"),
    )
    .unwrap();
    for (provider, model, account) in [
        ("claude-main", "sonnet", "acct-claude"),
        ("glm-any", "glm", "acct-glm"),
    ] {
        let run_home = root.join(format!("{provider}-plugin-run"));
        let (snapshot, _) = materialize_selected(
            &config,
            &catalog,
            &provider.parse().unwrap(),
            model,
            &account.parse().unwrap(),
            &role,
            root,
            &run_home,
            root,
        )
        .unwrap();
        assert_eq!(snapshot.plugin_paths.len(), 1);
    }

    profile.skills.clear();
    let missing = resolve_role_plan(
        &profile,
        &root.join("skills"),
        &BTreeMap::new(),
        "account",
        Some("work"),
    )
    .unwrap();
    let error = materialize_selected(
        &config,
        &catalog,
        &"claude-main".parse().unwrap(),
        "sonnet",
        &"acct-claude".parse().unwrap(),
        &missing,
        root,
        &root.join("missing-plugin-skill-run"),
        root,
    )
    .unwrap_err();
    assert!(error.to_string().contains("unlisted: plugin-review"));
}

/// Constructs launch authority only after the runtime assets have been sealed.
fn make_authority(
    catalog: &ProviderCatalog,
    provider: &ProviderId,
    model: &str,
    role: &ResolvedRolePlan,
    root: &Path,
    digest: Sha256Digest,
    account: &str,
) -> ResolvedLaunchAuthority {
    let definition = catalog.provider(provider).unwrap();
    ResolvedLaunchAuthority {
        provider: provider.clone(),
        harness: definition.harness,
        connection: definition.connection.clone(),
        model: model.into(),
        effort: None,
        profile: role.role_name.clone(),
        workdir: root.into(),
        role_payload: role.to_payload(),
        assets_sha256: digest,
        eligible_accounts: vec![account.parse().unwrap()],
    }
}

/// Runs a fake child with its planned environment and captures only safe output.
fn run_fake(plan: &agent_run_adapters::LaunchPlan) -> String {
    let output = Command::new(&plan.binary)
        .args(&plan.args)
        .current_dir(&plan.cwd)
        .env_clear()
        .envs(&plan.environment)
        .output()
        .unwrap();
    assert!(output.status.success());
    String::from_utf8(output.stdout).unwrap()
}

/// Named native Codex retains its auth bridge and never selects API-key mode.
#[test]
fn native_provider_keeps_login_and_model_alias() {
    let home = tempfile::tempdir().unwrap();
    let root = home.path();
    let config = config(root, &fake_engine(root));
    let catalog = catalog(&config);
    let role = role(root, false);
    let auth = root.join("accounts/codex/work/auth.json");
    fs::create_dir_all(auth.parent().unwrap()).unwrap();
    fs::write(&auth, "{}").unwrap();
    let provider: ProviderId = "codex-plus".parse().unwrap();
    let account = "acct-native".parse().unwrap();
    let run_home = root.join("native-run");
    let (_, digest) = materialize_selected(
        &config, &catalog, &provider, "gpt", &account, &role, root, &run_home, root,
    )
    .unwrap();
    assert!(!run_home.join("auth.json").exists());
    let mut authority = make_authority(
        &catalog,
        &provider,
        "gpt",
        &role,
        root,
        digest,
        "acct-native",
    );
    authority
        .eligible_accounts
        .push("acct-native-b".parse().unwrap());
    let plan = plan_selected(
        &config,
        &catalog,
        &authority,
        &account,
        &run_home,
        root,
        &BTreeMap::from([
            ("HOME".into(), root.to_string_lossy().into_owned()),
            ("AGENT_RUN_WORKER_HOME".into(), "/stale/home".into()),
            ("AGENT_RUN_WORKER_RUN_ID".into(), "stale-run".into()),
            ("AGENT_RUN_WORKER_ATTEMPT_ID".into(), "stale-attempt".into()),
            ("AGENT_RUN_WORKER_TOKEN".into(), "stale-capability".into()),
            (agent_run_domain::worker::FINISH_ENV.into(), "1".into()),
            (agent_run_domain::worker::RESEARCH_ENV.into(), "1".into()),
        ]),
        &FakeReader,
        "task",
        None,
    )
    .unwrap();
    assert_eq!(plan.native_model, "gpt-native");
    assert_eq!(plan.profile.body, role.prompt);
    assert!(
        plan.profile
            .body
            .starts_with(include_str!("../../../assets/worker_instructions.md").trim_end())
    );
    assert!(plan.profile.body.ends_with("Review safely."));
    assert!(run_home.join("auth.json").exists());
    let native_config: toml::Value =
        toml::from_str(&fs::read_to_string(run_home.join("config.toml")).unwrap()).unwrap();
    let worker = &native_config["mcp_servers"]["agent_run_worker"];
    assert_eq!(worker["args"][0].as_str(), Some("_worker-mcp"));
    assert_eq!(
        worker["env_vars"].as_array().unwrap(),
        &agent_run_domain::worker::ENV_NAMES
            .into_iter()
            .chain([agent_run_domain::worker::FINISH_ENV])
            .map(|name| toml::Value::String(name.into()))
            .collect::<Vec<_>>()
    );
    assert_eq!(
        worker["default_tools_approval_mode"].as_str(),
        Some("approve")
    );
    for name in agent_run_domain::worker::ENV_NAMES.into_iter().chain([
        agent_run_domain::worker::FINISH_ENV,
        agent_run_domain::worker::RESEARCH_ENV,
    ]) {
        assert!(!plan.launch.environment.contains_key(name));
    }
    assert!(
        !fs::read_to_string(run_home.join("config.toml"))
            .unwrap()
            .contains("model_providers")
    );
    assert!(
        !plan
            .launch
            .environment
            .contains_key("AGENT_RUN_PROVIDER_TOKEN")
    );
    assert!(run_fake(&plan.launch).contains("app-server"));
    // The admitted fast option becomes the codex fast service tier.
    let fast = agent_run_adapters::provider::plan_selected_with(
        &config,
        &catalog,
        &authority,
        &account,
        &run_home,
        root,
        &BTreeMap::from([("HOME".into(), root.to_string_lossy().into_owned())]),
        &FakeReader,
        "task",
        None,
        agent_run_adapters::provider::LaunchOptions {
            fast: true,
            output_schema: None,
        },
        None,
    )
    .unwrap();
    assert_eq!(
        fast.launch.args,
        [
            "-c",
            "service_tier=fast",
            "-c",
            "features.fast_mode=true",
            "app-server"
        ]
    );
    agent_run_adapters::materialize::verify(&run_home, authority.assets_sha256.as_str()).unwrap();
    let other_auth = root.join("accounts/codex/other/auth.json");
    fs::create_dir_all(other_auth.parent().unwrap()).unwrap();
    fs::write(&other_auth, "{}").unwrap();
    let other = plan_selected(
        &config,
        &catalog,
        &authority,
        &"acct-native-b".parse().unwrap(),
        &run_home,
        root,
        &BTreeMap::from([("HOME".into(), root.to_string_lossy().into_owned())]),
        &FakeReader,
        "next task",
        None,
    )
    .unwrap();
    assert_eq!(other.native_model, "gpt-native");
    assert_eq!(
        run_home.join("auth.json").canonicalize().unwrap(),
        other_auth.canonicalize().unwrap()
    );
    agent_run_adapters::materialize::verify(&run_home, authority.assets_sha256.as_str()).unwrap();

    let provider: ProviderId = "claude-main".parse().unwrap();
    let account = "acct-claude".parse().unwrap();
    let run_home = root.join("claude-native-run");
    let (_, digest) = materialize_selected(
        &config, &catalog, &provider, "sonnet", &account, &role, root, &run_home, root,
    )
    .unwrap();
    let authority = make_authority(
        &catalog,
        &provider,
        "sonnet",
        &role,
        root,
        digest,
        "acct-claude",
    );
    let plan = plan_selected(
        &config,
        &catalog,
        &authority,
        &account,
        &run_home,
        root,
        &BTreeMap::from([("HOME".into(), root.to_string_lossy().into_owned())]),
        &FakeReader,
        "task",
        None,
    )
    .unwrap();
    assert_eq!(plan.native_model, "claude-sonnet");
    // A shared-layout launch runs the sealed native binary directly: agent-run
    // adds no sandbox-exec wrapper, so program and leading argv are unchanged.
    let store = root.join("shared-store");
    fs::create_dir_all(&store).unwrap();
    let shared = agent_run_adapters::provider::plan_selected_with(
        &config,
        &catalog,
        &authority,
        &account,
        &run_home,
        root,
        &BTreeMap::from([("HOME".into(), root.to_string_lossy().into_owned())]),
        &FakeReader,
        "task",
        None,
        agent_run_adapters::provider::LaunchOptions::default(),
        Some(&agent_run_adapters::provider::SharedLaunchAssets {
            store_root: store.canonicalize().unwrap(),
            roots: BTreeMap::new(),
        }),
    )
    .unwrap();
    assert_eq!(shared.launch.binary, plan.launch.binary);
    assert_eq!(shared.launch.binary, Path::new(&fake_engine(root)));
    assert_eq!(
        shared.launch.args.first().map(String::as_str),
        Some("--print")
    );
    assert!(
        !shared
            .launch
            .args
            .iter()
            .any(|arg| arg == "-p" || arg.contains("(deny file-write"))
    );
    assert!(plan.launch.args.iter().any(|arg| arg == "--mcp-config"));
    assert!(
        plan.launch
            .args
            .iter()
            .any(|arg| arg.contains("mcp__agent_run_worker"))
    );
    let mcp: serde_json::Value =
        serde_json::from_str(&fs::read_to_string(run_home.join("mcp/mcp-config.json")).unwrap())
            .unwrap();
    let worker = &mcp["mcpServers"]["agent_run_worker"];
    assert_eq!(worker["args"], serde_json::json!(["_worker-mcp"]));
    assert_eq!(
        worker["env"]["AGENT_RUN_WORKER_TOKEN"],
        "${AGENT_RUN_WORKER_TOKEN}"
    );
    let system_prompt = &plan.launch.args[plan
        .launch
        .args
        .iter()
        .position(|arg| arg == "--append-system-prompt")
        .unwrap()
        + 1];
    assert_eq!(system_prompt, &role.prompt);
    assert!(
        system_prompt
            .starts_with(include_str!("../../../assets/worker_instructions.md").trim_end())
    );
    assert!(system_prompt.ends_with("Review safely."));
    // The admitted output_schema joins the claude system prompt.
    let schema = serde_json::json!({"type": "object"});
    let with_schema = agent_run_adapters::provider::plan_selected_with(
        &config,
        &catalog,
        &authority,
        &account,
        &run_home,
        root,
        &BTreeMap::from([("HOME".into(), root.to_string_lossy().into_owned())]),
        &FakeReader,
        "task",
        None,
        agent_run_adapters::provider::LaunchOptions {
            fast: false,
            output_schema: schema.as_object(),
        },
        None,
    )
    .unwrap();
    assert!(with_schema.launch.args.iter().any(|arg| {
        arg.contains("Return only JSON matching this schema: {\"type\":\"object\"}")
    }));
    assert!(
        plan.launch
            .args
            .iter()
            .any(|arg| arg == "--include-partial-messages")
    );
    assert!(!plan.launch.environment.contains_key("ANTHROPIC_BASE_URL"));
    assert!(!plan.launch.environment.contains_key("ANTHROPIC_AUTH_TOKEN"));
    assert!(!plan.launch.environment.contains_key("ANTHROPIC_API_KEY"));
    assert!(run_fake(&plan.launch).contains("claude-sonnet"));
}

/// Custom Claude uses only the selected credential; bearer gateways explicitly
/// blank the API-key variable so Claude cannot fall back to another login.
#[test]
fn custom_claude_auth_clears_conflicting_host_credentials() {
    use agent_run_domain::catalog::{CredentialHeader, ProviderConnection};

    for header in [CredentialHeader::Bearer, CredentialHeader::XApiKey] {
        let home = tempfile::tempdir().unwrap();
        let root = home.path();
        let mut config = config(root, &fake_engine(root));
        let provider: ProviderId = "glm-any".parse().unwrap();
        let ProviderConnection::Custom { auth_header, .. } =
            &mut config.providers.get_mut(&provider).unwrap().connection
        else {
            panic!("fixture must use a custom connection");
        };
        *auth_header = header;
        let catalog = catalog(&config);
        let role = role(root, false);
        let account = "acct-glm".parse().unwrap();
        let run_home = root.join("gateway-run");
        let (_, digest) = materialize_selected(
            &config, &catalog, &provider, "glm", &account, &role, root, &run_home, root,
        )
        .unwrap();
        let authority = make_authority(&catalog, &provider, "glm", &role, root, digest, "acct-glm");
        let host = BTreeMap::from([
            ("HOME".into(), root.to_string_lossy().into_owned()),
            ("ANTHROPIC_AUTH_TOKEN".into(), "stale-host-token".into()),
            ("ANTHROPIC_API_KEY".into(), "stale-host-api-key".into()),
        ]);
        let plan = plan_selected(
            &config,
            &catalog,
            &authority,
            &account,
            &run_home,
            root,
            &host,
            &FakeReader,
            "task",
            None,
        )
        .unwrap();
        match header {
            CredentialHeader::Bearer => {
                assert_eq!(
                    plan.launch.environment["ANTHROPIC_AUTH_TOKEN"],
                    "synthetic-secret"
                );
                assert_eq!(
                    plan.launch
                        .environment
                        .get("ANTHROPIC_API_KEY")
                        .map(String::as_str),
                    Some("")
                );
            }
            CredentialHeader::XApiKey => {
                assert_eq!(
                    plan.launch.environment["ANTHROPIC_API_KEY"],
                    "synthetic-secret"
                );
                assert!(!plan.launch.environment.contains_key("ANTHROPIC_AUTH_TOKEN"));
            }
        }
    }
}

/// Arbitrary custom providers use configured models and gateways; drift in
/// authority, account scope, or sealed files fails before native execution.
#[test]
fn custom_providers_use_sealed_settings_and_fake_credentials() {
    let home = tempfile::tempdir().unwrap();
    let root = home.path();
    let config = config(root, &fake_engine(root));
    let mut forbidden = config.clone();
    forbidden
        .harnesses
        .get_mut(&agent_run_domain::HarnessId::Codex)
        .unwrap()
        .native_settings
        .insert(
            "model_provider".into(),
            toml::Value::String("override".into()),
        );
    assert!(forbidden.validate(root).is_err());
    let catalog = catalog(&config);
    let unsafe_role = role(root, true);
    let role = role(root, false);
    assert!(
        materialize_selected(
            &config,
            &catalog,
            &"glm-any".parse().unwrap(),
            "glm",
            &"acct-glm".parse().unwrap(),
            &unsafe_role,
            root,
            &root.join("unsafe-run"),
            root,
        )
        .is_err()
    );
    let host = BTreeMap::from([("HOME".into(), root.to_string_lossy().into_owned())]);

    let provider: ProviderId = "glm-any".parse().unwrap();
    let account = "acct-glm".parse().unwrap();
    let run_home = root.join("glm-run");
    let (_, digest) = materialize_selected(
        &config, &catalog, &provider, "glm", &account, &role, root, &run_home, root,
    )
    .unwrap();
    let authority = make_authority(&catalog, &provider, "glm", &role, root, digest, "acct-glm");
    let plan = plan_selected(
        &config,
        &catalog,
        &authority,
        &account,
        &run_home,
        root,
        &host,
        &FakeReader,
        "task",
        None,
    )
    .unwrap();
    assert_eq!(plan.native_model, "glm-5.3[1m]");
    assert!(
        plan.launch
            .args
            .iter()
            .any(|arg| arg == "--include-partial-messages")
    );
    assert_eq!(
        plan.launch.environment["ANTHROPIC_BASE_URL"],
        "https://gateway.example/api"
    );
    assert_eq!(
        plan.launch.environment["ANTHROPIC_AUTH_TOKEN"],
        "synthetic-secret"
    );
    assert_eq!(plan.launch.environment["ANTHROPIC_API_KEY"], "");
    let output = run_fake(&plan.launch);
    assert!(output.contains("glm-5.3[1m]"));
    assert!(!output.contains("synthetic-secret"));
    // Custom Messages providers preserve the configured effort, without a
    // vendor-specific Rust mapping or a downgrade in the harness arguments.
    for effort in ["low", "high", "max"] {
        let mut with_effort = authority.clone();
        with_effort.effort = Some(effort.into());
        let planned = plan_selected(
            &config,
            &catalog,
            &with_effort,
            &account,
            &run_home,
            root,
            &host,
            &FakeReader,
            "task",
            None,
        )
        .unwrap();
        assert!(
            planned
                .launch
                .args
                .windows(2)
                .any(|pair| pair == ["--effort", effort])
        );
    }
    let mut changed_config = config.clone();
    changed_config
        .harnesses
        .get_mut(&agent_run_domain::HarnessId::Codex)
        .unwrap()
        .native_settings
        .insert("model_verbosity".into(), toml::Value::String("high".into()));
    changed_config.validate(root).unwrap();
    assert!(
        plan_selected(
            &changed_config,
            &catalog,
            &authority,
            &account,
            &run_home,
            root,
            &host,
            &FakeReader,
            "task",
            None,
        )
        .is_err()
    );
    let mut changed = authority.clone();
    changed.connection = catalog
        .provider(&"codex-gateway".parse().unwrap())
        .unwrap()
        .connection
        .clone();
    assert!(
        plan_selected(
            &config,
            &catalog,
            &changed,
            &account,
            &run_home,
            root,
            &host,
            &FakeReader,
            "task",
            None
        )
        .is_err()
    );
    let mut changed_grant = authority.clone();
    changed_grant.role_payload["grants"]["write"] = serde_json::json!(true);
    assert!(
        plan_selected(
            &config,
            &catalog,
            &changed_grant,
            &account,
            &run_home,
            root,
            &host,
            &FakeReader,
            "task",
            None,
        )
        .is_err()
    );
    assert!(
        plan_selected(
            &config,
            &catalog,
            &authority,
            &"acct-native".parse().unwrap(),
            &run_home,
            root,
            &host,
            &FakeReader,
            "task",
            None
        )
        .is_err()
    );

    let provider: ProviderId = "codex-gateway".parse().unwrap();
    let account = "acct-openai".parse().unwrap();
    let run_home = root.join("codex-run");
    let (_, digest) = materialize_selected(
        &config, &catalog, &provider, "gpt", &account, &role, root, &run_home, root,
    )
    .unwrap();
    let authority = make_authority(
        &catalog,
        &provider,
        "gpt",
        &role,
        root,
        digest,
        "acct-openai",
    );
    let plan = plan_selected(
        &config,
        &catalog,
        &authority,
        &account,
        &run_home,
        root,
        &host,
        &FakeReader,
        "task",
        None,
    )
    .unwrap();
    let generated = fs::read_to_string(run_home.join("config.toml")).unwrap();
    assert!(generated.contains("model_provider = \"agent_run_gateway\""));
    assert!(generated.contains("wire_api = \"responses\""));
    assert!(!generated.contains("synthetic-secret"));
    assert!(!run_home.join("auth.json").exists());
    assert_eq!(
        plan.launch.environment["AGENT_RUN_PROVIDER_TOKEN"],
        "synthetic-secret"
    );
    assert!(run_fake(&plan.launch).contains("app-server"));
    fs::write(run_home.join("provider-launch.json"), "{}").unwrap();
    assert!(
        plan_selected(
            &config,
            &catalog,
            &authority,
            &account,
            &run_home,
            root,
            &host,
            &FakeReader,
            "task",
            None
        )
        .is_err()
    );
}

/// OpenCode selects only the registered credential, isolates native paths and
/// never puts a credential or the local server capability into sealed assets.
/// An explicitly configured Bridge component is copied into the same immutable
/// snapshot through the actual provider path; absent configuration adds nothing.
#[test]
fn opencode_launch_isolates_ambient_state_and_selected_credentials() {
    use agent_run_config::role_plan::resolve_role_plan;
    let temporary = tempfile::tempdir().unwrap();
    let root = temporary.path().canonicalize().unwrap();
    let mut config = config(&root, &fake_engine(&root));
    let mut harness = config.harnesses[&HarnessId::Codex].clone();
    harness.home = root.join("opencode");
    config.harnesses.insert(HarnessId::OpenCode, harness);
    let mut provider = config.providers[&"codex-gateway".parse().unwrap()].clone();
    provider.harness = HarnessId::OpenCode;
    provider.connection = agent_run_domain::ProviderConnection::Native;
    provider.auth_family = "opencode".parse().unwrap();
    provider.models[0].native_model = Some("opencode/big-pickle".into());
    provider.bindings[0].account = "acct-opencode".parse().unwrap();
    let provider_id: ProviderId = "zen".parse().unwrap();
    config.providers = BTreeMap::from([(provider_id.clone(), provider)]);
    let mut profile = agent_run_adapters::provider::profile(&role(&root, false));
    profile.mcp.clear();
    let role = resolve_role_plan(&profile, &root, &BTreeMap::new(), "global", None).unwrap();
    let host = BTreeMap::from([
        ("HOME".into(), root.display().to_string()),
        ("OPENCODE_DB".into(), "/foreign.db".into()),
        ("OPENCODE_CONFIG".into(), "/foreign.json".into()),
        ("OPENCODE_API_KEY".into(), "foreign-secret".into()),
        ("XDG_DATA_HOME".into(), "/foreign-data".into()),
    ]);
    for (name, reference) in [
        ("public", "native:opencode"),
        ("key", "env:FAKE_OPENCODE_KEY"),
    ] {
        if name == "key" {
            let package = root.join("bridge-package");
            fs::create_dir(&package).unwrap();
            fs::write(package.join("package.json"), r#"{"name":"@agent-bridge/opencode","main":"index.js","type":"module","agentBridge":{"protocol":1,"mode":"lifecycle"}}"#).unwrap();
            fs::write(
                package.join("index.js"),
                "export default {id:'agent-bridge'};\n",
            )
            .unwrap();
            config.delivery.bridge = Some(serde_json::from_value(serde_json::json!({
                "registration_socket":root.join("bridge.sock"),"core_socket":root.join("core.sock"),
                "token_file":root.join("credential"),"host_id":"local","codex_store":"main",
                "opencode_plugin":{"package":package,"hook_command":fake_engine(&root)}
            })).unwrap());
            config.environments.insert("bridge".into(), serde_json::from_value(serde_json::json!({
                "variables":{"BRIDGE_NATIVE_OWNER_CHANNEL":root.join("bridge.sock"),"BRIDGE_NATIVE_OWNER_DIRECTORY":root.join("owner")}
            })).unwrap());
            config
                .harnesses
                .get_mut(&HarnessId::OpenCode)
                .unwrap()
                .environment = Some("bridge".into());
        }
        let catalog = config
            .resolve_catalog(vec![AccountRecord {
                account_id: "acct-opencode".parse().unwrap(),
                auth_family: "opencode".parse().unwrap(),
                secret_ref: reference.parse().unwrap(),
                status: AccountStatus::Enabled,
            }])
            .unwrap();
        let home = root.join(name);
        let (_, digest) = materialize_selected(
            &config,
            &catalog,
            &provider_id,
            "gpt",
            &"acct-opencode".parse().unwrap(),
            &role,
            &root,
            &home,
            &root,
        )
        .unwrap();
        let authority = make_authority(
            &catalog,
            &provider_id,
            "gpt",
            &role,
            &root,
            digest,
            "acct-opencode",
        );
        let planned = plan_selected(
            &config,
            &catalog,
            &authority,
            &"acct-opencode".parse().unwrap(),
            &home,
            &root,
            &host,
            &FakeReader,
            "task",
            None,
        )
        .unwrap();
        assert_eq!(
            planned.launch.args,
            [
                "serve",
                "--stdio",
                "--port",
                "0",
                "--hostname",
                "127.0.0.1",
                "--log-level",
                "error"
            ]
        );
        assert_eq!(
            planned.launch.environment["OPENCODE_DB"],
            home.join("opencode-data/opencode.db").display().to_string()
        );
        assert_eq!(
            planned.launch.environment["HOME"],
            home.display().to_string()
        );
        assert_eq!(
            planned.launch.environment["OPENCODE_CONFIG_PROJECT_DISABLE"],
            "true"
        );
        assert_eq!(
            planned
                .launch
                .environment
                .get("OPENCODE_API_KEY")
                .map(String::as_str),
            if name == "key" {
                Some("synthetic-secret")
            } else {
                None
            }
        );
        let native = fs::read_to_string(home.join("opencode.json")).unwrap();
        let rendered: serde_json::Value = serde_json::from_str(&native).unwrap();
        assert_eq!(
            rendered["plugins"].as_array().unwrap().len(),
            usize::from(name == "key")
        );
        if name == "key" {
            assert_eq!(
                rendered["plugins"][0]["package"],
                home.join("bridge-opencode/plugin").display().to_string()
            );
            assert_eq!(
                rendered["plugins"][0]["options"]["hookCommand"],
                home.join("bridge-opencode/bridge-hook")
                    .display()
                    .to_string()
            );
            let sealed = home.join("bridge-opencode/plugin/index.js");
            let original = fs::read(&sealed).unwrap();
            fs::write(root.join("bridge-package/index.js"), "changed source").unwrap();
            assert_eq!(fs::read(&sealed).unwrap(), original);
            assert_eq!(
                fs::metadata(&sealed).unwrap().permissions().mode() & 0o777,
                0o400
            );
            assert_eq!(
                fs::metadata(home.join("bridge-opencode/bridge-hook"))
                    .unwrap()
                    .permissions()
                    .mode()
                    & 0o777,
                0o500
            );
        }
        for secret in [
            "synthetic-secret",
            "foreign-secret",
            &planned.launch.environment["OPENCODE_PASSWORD"],
        ] {
            assert!(!native.contains(secret));
        }
    }
}

/// Read-only canonical profile declaring `skills`.
fn skill_profile(skills: &[&str]) -> Profile {
    Profile {
        name: "review".into(),
        body: "Review safely.".into(),
        write: false,
        network: false,
        revision: "1".into(),
        canonical: true,
        allow_external_read_roots: false,
        read_roots: vec![],
        skills: skills.iter().map(|skill| (*skill).to_owned()).collect(),
        mcp: vec![],
        mcp_tools: Default::default(),
        required_constraints: BTreeSet::from([Constraint::WebToolsDisabled]),
    }
}

/// Freezes a role declaring `skills`, hashed from `<root>/skills` at call time.
fn skill_role(root: &Path, skills: &[&str]) -> ResolvedRolePlan {
    resolve_role_plan(
        &skill_profile(skills),
        &root.join("skills"),
        &BTreeMap::new(),
        "account",
        Some("work"),
    )
    .unwrap()
}

/// Writes one catalog skill directory and returns it.
fn catalog_skill(root: &Path, name: &str, text: &str) -> std::path::PathBuf {
    let dir = root.join("skills").join(name);
    fs::create_dir_all(&dir).unwrap();
    fs::write(dir.join("SKILL.md"), text).unwrap();
    dir
}

/// Materializes `role` for one provider into `<root>/<home>` with the fixture config.
fn materialize_skill_home(
    root: &Path,
    provider: &str,
    role: &ResolvedRolePlan,
    home: &str,
) -> Result<(agent_run_adapters::materialize::Snapshot, Sha256Digest)> {
    let config = config(root, &fake_engine(root));
    let catalog = catalog(&config);
    let (model, account) = match provider {
        "codex-plus" => ("gpt", "acct-native"),
        _ => ("sonnet", "acct-claude"),
    };
    materialize_selected(
        &config,
        &catalog,
        &provider.parse().unwrap(),
        model,
        &account.parse().unwrap(),
        role,
        root,
        &root.join(home),
        root,
    )
}

/// Returns where `provider` places a copied catalog skill below its run home.
fn copied_skill(root: &Path, provider: &str, home: &str, name: &str) -> std::path::PathBuf {
    let base = root.join(home);
    match provider {
        "codex-plus" => base.join("skills").join(name),
        _ => base.join("plugins").join(name).join("skills").join(name),
    }
}

/// A catalog change between admission and initial capture is refused for every
/// harness shape, names only the skill and never seals the generated index.
#[test]
fn changed_skill_between_admission_and_capture_is_refused() {
    for provider in ["codex-plus", "claude-main"] {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path();
        let dir = catalog_skill(root, "demo", "Admitted text.\n");
        let role = skill_role(root, &["demo"]);
        fs::write(dir.join("SKILL.md"), "Edited after admission.\n").unwrap();
        let error = materialize_skill_home(root, provider, &role, "run").unwrap_err();
        assert!(
            error
                .to_string()
                .contains("skill content differs from admitted revision: demo"),
            "{provider}: {error}"
        );
        assert!(!error.to_string().contains(root.to_str().unwrap()));
        let index =
            fs::read_to_string(root.join("run/.agent-run-snapshots.json")).unwrap_or_default();
        assert!(
            !index.contains("materialize_revision"),
            "{provider}: sealed"
        );
    }
}

/// A catalog link retargeted after admission is refused when its bytes differ and
/// accepted only when the new target holds exactly the admitted bytes.
#[test]
fn retargeted_skill_source_is_checked_against_admitted_bytes() {
    for provider in ["codex-plus", "claude-main"] {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().canonicalize().unwrap();
        let outside = root.join("outside");
        for (name, text) in [
            ("admitted", "Admitted text.\n"),
            ("other", "Other text.\n"),
            ("same", "Admitted text.\n"),
        ] {
            fs::create_dir_all(outside.join(name)).unwrap();
            fs::write(outside.join(name).join("SKILL.md"), text).unwrap();
        }
        fs::create_dir_all(root.join("skills")).unwrap();
        let link = root.join("skills/demo");
        std::os::unix::fs::symlink(outside.join("admitted"), &link).unwrap();
        let role = skill_role(&root, &["demo"]);
        fs::remove_file(&link).unwrap();
        std::os::unix::fs::symlink(outside.join("other"), &link).unwrap();
        let error = materialize_skill_home(&root, provider, &role, "refused").unwrap_err();
        assert!(
            error
                .to_string()
                .contains("differs from admitted revision: demo"),
            "{provider}: {error}"
        );
        fs::remove_file(&link).unwrap();
        std::os::unix::fs::symlink(outside.join("same"), &link).unwrap();
        materialize_skill_home(&root, provider, &role, "accepted").unwrap();
        assert_eq!(
            fs::read_to_string(copied_skill(&root, provider, "accepted", "demo").join("SKILL.md"))
                .unwrap(),
            "Admitted text.\n"
        );
    }
}

/// An unchanged catalog launches, the sealed home is reused unchanged by later
/// plans even after the catalog moves on, and post-seal tampering is still refused.
#[test]
fn unchanged_skill_seals_and_sealed_home_is_reused_after_catalog_drift() {
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path();
    let dir = catalog_skill(root, "demo", "Admitted text.\n");
    let role = skill_role(root, &["demo"]);
    let (_, digest) = materialize_skill_home(root, "claude-main", &role, "run").unwrap();
    let config = config(root, &fake_engine(root));
    let catalog = catalog(&config);
    let provider: ProviderId = "claude-main".parse().unwrap();
    let authority = make_authority(
        &catalog,
        &provider,
        "sonnet",
        &role,
        root,
        digest,
        "acct-claude",
    );
    let plan = |home: &Path| {
        plan_selected(
            &config,
            &catalog,
            &authority,
            &"acct-claude".parse().unwrap(),
            home,
            root,
            &BTreeMap::from([("HOME".into(), root.display().to_string())]),
            &FakeReader,
            "task",
            None,
        )
    };
    plan(&root.join("run")).unwrap();
    fs::write(dir.join("SKILL.md"), "Catalog moved on.\n").unwrap();
    plan(&root.join("run")).unwrap();
    let sealed = copied_skill(root, "claude-main", "run", "demo").join("SKILL.md");
    assert_eq!(fs::read_to_string(&sealed).unwrap(), "Admitted text.\n");
    fs::set_permissions(&sealed, fs::Permissions::from_mode(0o600)).unwrap();
    fs::write(&sealed, "Tampered after seal.\n").unwrap();
    assert!(plan(&root.join("run")).is_err());
}

/// Builds a Claude plugin shipping `skills/demo/SKILL.md` with `text`.
fn plugin_with_skill(root: &Path, text: &str) -> std::path::PathBuf {
    let plugin = root.join("demo-plugin");
    fs::create_dir_all(plugin.join(".claude-plugin")).unwrap();
    fs::write(
        plugin.join(".claude-plugin/plugin.json"),
        serde_json::json!({"name":"demo-plugin","version":"1.0.0"}).to_string(),
    )
    .unwrap();
    fs::create_dir_all(plugin.join("skills/demo")).unwrap();
    fs::write(plugin.join("skills/demo/SKILL.md"), text).unwrap();
    plugin
}

/// Adds `plugin` to the harness `provider` runs on.
fn add_plugin(config: &mut ProviderConfig, provider: &str, plugin: std::path::PathBuf) {
    let harness = if provider == "codex-plus" {
        HarnessId::Codex
    } else {
        HarnessId::ClaudeCode
    };
    config
        .harnesses
        .get_mut(&harness)
        .unwrap()
        .plugins
        .push(plugin);
}

/// Returns the model and account the fixture binds for `provider`.
fn model_account(provider: &str) -> (&'static str, &'static str) {
    match provider {
        "glm-any" => ("glm", "acct-glm"),
        "codex-plus" => ("gpt", "acct-native"),
        _ => ("sonnet", "acct-claude"),
    }
}

/// An unchanged configured plugin launches whether its bytes equal or differ from
/// a catalog duplicate, because admission records the plugin's revision as the
/// effective source. Changing that plugin after admission, or admitting without
/// the plugin-aware sources, is refused for every harness shape.
#[test]
fn configured_plugin_skill_is_admitted_and_checked_as_effective_source() {
    for text in ["Admitted text.\n", "Plugin text.\n"] {
        for provider in ["claude-main", "glm-any", "codex-plus"] {
            let temp = tempfile::tempdir().unwrap();
            let root = temp.path();
            catalog_skill(root, "demo", "Admitted text.\n");
            let plugin = plugin_with_skill(root, text);
            let mut config = config(root, &fake_engine(root));
            add_plugin(&mut config, provider, plugin.clone());
            let catalog = catalog(&config);
            let (model, account) = model_account(provider);
            let harness = catalog
                .provider(&provider.parse().unwrap())
                .unwrap()
                .harness;
            let profile = skill_profile(&["demo"]);
            let sources = effective_skill_sources(&config, harness, &profile).unwrap();
            assert_eq!(sources.len(), 1, "{provider}");
            let role = resolve_role_plan_with_sources(
                &profile,
                &root.join("skills"),
                &sources,
                &BTreeMap::new(),
                "account",
                Some("work"),
            )
            .unwrap();
            // Truthful provenance: the revision names the plugin bytes that will run.
            assert_eq!(
                role.skills[0].revision,
                tree_revision(&plugin.join("skills/demo")).unwrap()
            );
            assert_eq!(
                role.skills[0].revision == skill_role(root, &["demo"]).skills[0].revision,
                text == "Admitted text.\n"
            );
            let launch = |role: &ResolvedRolePlan, home: &str| {
                materialize_selected(
                    &config,
                    &catalog,
                    &provider.parse().unwrap(),
                    model,
                    &account.parse().unwrap(),
                    role,
                    root,
                    &root.join(home),
                    root,
                )
            };
            let (snapshot, _) = launch(&role, "run").unwrap();
            assert_eq!(snapshot.plugin_paths.len(), 1, "{provider}");
            if provider == "codex-plus" {
                assert_eq!(
                    fs::read_to_string(root.join("run/skills/demo/SKILL.md")).unwrap(),
                    text
                );
            }
            // Admission that ignores the plugin records the catalog bytes; a
            // plugin that differs from them is refused rather than mislabeled.
            if text != "Admitted text.\n" {
                let error = launch(&skill_role(root, &["demo"]), "unaware").unwrap_err();
                assert!(
                    error
                        .to_string()
                        .contains("differs from admitted revision: demo"),
                    "{provider}: {error}"
                );
            }
            // Post-admission change of the effective plugin is always refused.
            fs::write(
                plugin.join("skills/demo/SKILL.md"),
                "Changed after admission.\n",
            )
            .unwrap();
            let error = launch(&role, "changed").unwrap_err();
            assert!(
                error
                    .to_string()
                    .contains("differs from admitted revision: demo"),
                "{provider}: {error}"
            );
        }
    }
}

/// Restricted research uses no plugin, so the catalog skill stays the effective source.
#[test]
fn research_profiles_keep_catalog_skill_sources() {
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path();
    catalog_skill(root, "demo", "Admitted text.\n");
    let mut config = config(root, &fake_engine(root));
    add_plugin(
        &mut config,
        "claude-main",
        plugin_with_skill(root, "Plugin text.\n"),
    );
    let mut profile = skill_profile(&["demo"]);
    profile.required_constraints = BTreeSet::from([Constraint::ResearchToolsOnly]);
    assert!(
        effective_skill_sources(&config, HarnessId::ClaudeCode, &profile)
            .unwrap()
            .is_empty()
    );
    profile.required_constraints = BTreeSet::from([Constraint::WebToolsDisabled]);
    assert_eq!(
        effective_skill_sources(&config, HarnessId::ClaudeCode, &profile)
            .unwrap()
            .len(),
        1
    );
}
