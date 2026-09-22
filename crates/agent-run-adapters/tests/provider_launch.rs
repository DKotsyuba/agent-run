//! Fake-engine evidence for configured provider materialization and launch.

use agent_run_adapters::{
    authorized_request::CredentialReader,
    provider::{materialize_selected, plan_selected},
};
use agent_run_config::{
    profiles::Profile,
    provider_config::ProviderConfig,
    role_plan::{resolve_role_plan, ResolvedRolePlan},
};
use agent_run_domain::{
    catalog::{AccountRecord, AccountStatus, ProviderCatalog, ProviderId, ResolvedLaunchAuthority},
    domain::Constraint,
    CredentialRef, Result, Sha256Digest,
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
limits_source = "codex_appserver"
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
limits_source = "lua"
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
fn role(root: &Path) -> ResolvedRolePlan {
    let profile = Profile {
        name: "review".into(),
        body: "Review safely.".into(),
        write: false,
        network: false,
        revision: "1".into(),
        canonical: true,
        allow_external_read_roots: false,
        read_roots: vec![],
        skills: vec![],
        mcp: vec![],
        required_constraints: BTreeSet::from([Constraint::WebToolsDisabled]),
    };
    resolve_role_plan(&profile, root, &BTreeMap::new(), "account", Some("work")).unwrap()
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
    let role = role(root);
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
        &BTreeMap::from([("HOME".into(), root.to_string_lossy().into_owned())]),
        &FakeReader,
        "task",
        None,
    )
    .unwrap();
    assert_eq!(plan.native_model, "gpt-native");
    assert!(run_home.join("auth.json").exists());
    assert!(!fs::read_to_string(run_home.join("config.toml"))
        .unwrap()
        .contains("model_providers"));
    assert!(!plan
        .launch
        .environment
        .contains_key("AGENT_RUN_PROVIDER_TOKEN"));
    assert!(run_fake(&plan.launch).contains("app-server"));
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
    assert!(!plan.launch.environment.contains_key("ANTHROPIC_BASE_URL"));
    assert!(!plan.launch.environment.contains_key("ANTHROPIC_AUTH_TOKEN"));
    assert!(!plan.launch.environment.contains_key("ANTHROPIC_API_KEY"));
    assert!(run_fake(&plan.launch).contains("claude-sonnet"));
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
    let role = role(root);
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
    assert_eq!(
        plan.launch.environment["ANTHROPIC_BASE_URL"],
        "https://gateway.example/api"
    );
    assert_eq!(
        plan.launch.environment["ANTHROPIC_AUTH_TOKEN"],
        "synthetic-secret"
    );
    assert!(!plan.launch.environment.contains_key("ANTHROPIC_API_KEY"));
    let output = run_fake(&plan.launch);
    assert!(output.contains("glm-5.3[1m]"));
    assert!(!output.contains("synthetic-secret"));
    let mut changed_config = config.clone();
    changed_config
        .harnesses
        .get_mut(&agent_run_domain::HarnessId::Codex)
        .unwrap()
        .native_settings
        .insert("model_verbosity".into(), toml::Value::String("high".into()));
    changed_config.validate(root).unwrap();
    assert!(plan_selected(
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
    .is_err());
    let mut changed = authority.clone();
    changed.connection = catalog
        .provider(&"codex-gateway".parse().unwrap())
        .unwrap()
        .connection
        .clone();
    assert!(plan_selected(
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
    .is_err());
    let mut changed_grant = authority.clone();
    changed_grant.role_payload["grants"]["write"] = serde_json::json!(true);
    assert!(plan_selected(
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
    .is_err());
    assert!(plan_selected(
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
    .is_err());

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
    assert!(plan_selected(
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
    .is_err());
}
