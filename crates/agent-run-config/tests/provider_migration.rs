//! Read-only v1-to-v2 mapping with explicit identities and model aliases.

use agent_run_config::{
    config::Config,
    provider_config::HarnessConfig,
    provider_migration::{plan_v1, RuntimeMapping},
};
use agent_run_domain::catalog::{
    decode_legacy_request, AccountRecord, AccountStatus, CollectorBinding, HarnessId, LimitsSource,
    ProviderConnection, ProviderProtocol,
};
use serde_json::json;
use std::{collections::BTreeMap, fs, path::Path};

/// Creates one disposable validated v1 config with legacy main and glm names.
fn old(home: &Path) -> Config {
    fs::write(
        home.join("config.toml"),
        format!(
            r#"
schema_version = 1
[capacity]
codexbar_binary = "/bin/true"
[runtimes.main]
enabled = true
adapter = "codex"
binary = "/bin/true"
home = "{home}/codex"
models = ["gpt"]
accounts = ["work"]
priority_multiplier = 2.0
[runtimes.main.priority_account_multipliers]
work = 3.0
[runtimes.glm]
enabled = true
adapter = "glm"
binary = "/bin/true"
home = "{home}/glm"
models = ["glm-5.3"]
limits_source = "codexbar"
"#,
            home = home.display()
        ),
    )
    .unwrap();
    Config::load(home).unwrap()
}

/// Declares the two supported stable harnesses independently of runtime names.
fn harnesses(home: &Path) -> BTreeMap<HarnessId, HarnessConfig> {
    [
        (HarnessId::Codex, home.join("codex")),
        (HarnessId::ClaudeCode, home.join("glm")),
    ]
    .into_iter()
    .map(|(id, path)| {
        (
            id,
            serde_json::from_value(json!({"binary":"/bin/true","home":path})).unwrap(),
        )
    })
    .collect()
}

/// Supplies every provider, model, and global-account mapping explicitly.
fn mappings() -> BTreeMap<String, RuntimeMapping> {
    BTreeMap::from([
        (
            "main".into(),
            RuntimeMapping {
                provider: "codex".parse().unwrap(),
                harness: HarnessId::Codex,
                connection: ProviderConnection::Native,
                auth_family: "openai".parse().unwrap(),
                limits_source: LimitsSource::CodexAppserver,
                collector: None,
                native_models: BTreeMap::from([("gpt".into(), "gpt-native".into())]),
                model_restrictions: BTreeMap::new(),
                global_account: "acct-main".parse().unwrap(),
                labelled_accounts: BTreeMap::from([("work".into(), "acct-work".parse().unwrap())]),
            },
        ),
        (
            "glm".into(),
            RuntimeMapping {
                provider: "glm-user".parse().unwrap(),
                harness: HarnessId::ClaudeCode,
                connection: ProviderConnection::Custom {
                    endpoint: "https://gateway.example/api".into(),
                    protocol: ProviderProtocol::Messages,
                    auth_header: Default::default(),
                    allow_loopback_http: false,
                },
                auth_family: "anthropic".parse().unwrap(),
                limits_source: LimitsSource::Lua,
                collector: Some(CollectorBinding {
                    script: "glm_quota".into(),
                    origins: vec!["https://gateway.example".into()],
                    script_file: None,
                    auth: None,
                }),
                native_models: BTreeMap::from([("glm-5.3".into(), "glm-5.3[1m]".into())]),
                model_restrictions: BTreeMap::new(),
                global_account: "acct-glm".parse().unwrap(),
                labelled_accounts: BTreeMap::new(),
            },
        ),
    ])
}

/// Returns fake account records; their references name stores, not values.
fn accounts() -> Vec<AccountRecord> {
    [
        ("acct-main", "openai", "native:codex"),
        ("acct-work", "openai", "named:codex:work"),
        ("acct-glm", "anthropic", "env:FAKE_TOKEN"),
    ]
    .into_iter()
    .map(|(id, family, reference)| AccountRecord {
        account_id: id.parse().unwrap(),
        auth_family: family.parse().unwrap(),
        secret_ref: reference.parse().unwrap(),
        status: AccountStatus::Enabled,
    })
    .collect()
}

/// Mapping is deterministic and preserves raw historical names for decoding,
/// while conversion changes no config bytes or database files.
#[test]
fn explicit_migration_plan_preserves_history_and_weights() {
    let home = tempfile::tempdir().unwrap();
    let old = old(home.path());
    let before = fs::read(home.path().join("config.toml")).unwrap();
    let plan = plan_v1(
        &old,
        harnesses(home.path()),
        mappings(),
        accounts(),
        home.path(),
    )
    .unwrap();
    assert!(plan.manual_review.is_empty());
    // Retired CodexBar input (source and binary) migrates, but never carries.
    assert!(plan.config.capacity.legacy_codexbar_binary.is_none());
    assert_eq!(fs::read(home.path().join("config.toml")).unwrap(), before);
    assert!(!home.path().join("state.db").exists());
    let catalog = plan.config.resolve_catalog(accounts()).unwrap();
    let provider = catalog.provider(&"codex".parse().unwrap()).unwrap();
    assert_eq!(provider.priority_multiplier.get(), 2.0);
    assert_eq!(provider.binding("work").unwrap().multiplier.get(), 1.5);
    assert_eq!(
        provider.models[0].native_model.as_deref(),
        Some("gpt-native")
    );
    assert_eq!(
        catalog
            .provider(&"glm-user".parse().unwrap())
            .unwrap()
            .models[0]
            .native_model
            .as_deref(),
        Some("glm-5.3[1m]")
    );
    for (runtime, provider) in [("main", "codex"), ("glm", "glm-user")] {
        let stored =
            json!({"runtime":runtime,"model":"m","profile":"p","task":"t","workdir":"/tmp"});
        let decoded = decode_legacy_request(&stored, plan.legacy_runtime_map.get(runtime)).unwrap();
        assert_eq!(decoded.request.runtime, runtime);
        assert_eq!(decoded.provider.unwrap().as_str(), provider);
    }
    let repeat = plan_v1(
        &old,
        harnesses(home.path()),
        mappings(),
        accounts(),
        home.path(),
    )
    .unwrap();
    assert_eq!(
        plan.config.snapshot().unwrap(),
        repeat.config.snapshot().unwrap()
    );
}

/// Missing model/account evidence and untranslatable lane priorities are
/// typed refusals; none can silently become a guessed v2 provider.
#[test]
fn migration_plan_refuses_implicit_mapping() {
    let home = tempfile::tempdir().unwrap();
    let old = old(home.path());
    let mut missing_model = mappings();
    missing_model.get_mut("main").unwrap().native_models.clear();
    assert!(plan_v1(
        &old,
        harnesses(home.path()),
        missing_model,
        accounts(),
        home.path()
    )
    .is_err());
    let mut missing_account = mappings();
    missing_account
        .get_mut("main")
        .unwrap()
        .labelled_accounts
        .clear();
    assert!(plan_v1(
        &old,
        harnesses(home.path()),
        missing_account,
        accounts(),
        home.path()
    )
    .is_err());
    let mut wrong_harness = mappings();
    wrong_harness.get_mut("glm").unwrap().harness = HarnessId::Codex;
    assert!(plan_v1(
        &old,
        harnesses(home.path()),
        wrong_harness,
        accounts(),
        home.path()
    )
    .is_err());
    let mut lane_weight = old.clone();
    lane_weight
        .runtimes
        .get_mut("main")
        .unwrap()
        .priority_lane_multipliers
        .insert("gpt".into(), 2.0);
    assert!(plan_v1(
        &lane_weight,
        harnesses(home.path()),
        mappings(),
        accounts(),
        home.path()
    )
    .is_err());
    let mut disabled = old.clone();
    disabled.runtimes.get_mut("main").unwrap().enabled = false;
    assert!(plan_v1(
        &disabled,
        harnesses(home.path()),
        mappings(),
        accounts(),
        home.path()
    )
    .is_err());
    let mut duplicate_provider = mappings();
    duplicate_provider.get_mut("glm").unwrap().provider = "codex".parse().unwrap();
    assert!(plan_v1(
        &old,
        harnesses(home.path()),
        duplicate_provider,
        accounts(),
        home.path()
    )
    .is_err());
    assert!(plan_v1(
        &old,
        harnesses(home.path()),
        mappings(),
        vec![],
        home.path()
    )
    .is_err());
}
