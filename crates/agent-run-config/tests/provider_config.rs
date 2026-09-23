//! V2 harness/provider parsing, account cross-references, and safe snapshots.

use agent_run_config::provider_config::ProviderConfig;
use agent_run_domain::{
    catalog::{AccountRecord, AccountStatus, ProviderConnection},
    AccountId, AuthFamily, SecretRef,
};
use std::{fs, path::Path, str::FromStr};

/// Builds one v2 document with two harnesses and three arbitrary provider ids.
fn document(home: &Path) -> String {
    format!(
        r#"
schema_version = 2
[environments.fixture.variables]
VALUE = "fake-secret-value"
[harnesses.codex]
binary = "/bin/true"
home = "{home}/codex"
[harnesses.claude-code]
binary = "/bin/true"
home = "{home}/claude"
[providers.codex]
harness = "codex"
connection = {{ kind = "native" }}
auth_family = "openai"
limits_source = "codex_appserver"
recommendations = ["native subscription"]
[[providers.codex.models]]
id = "gpt"
native_model = "gpt-native"
params = {{ reasoning = "medium" }}
allowed_params = {{ effort = ["medium", "high"] }}
recommendations = ["coding"]
restrictions = ["web_tools_disabled"]
[[providers.codex.bindings]]
label = "personal"
account = "acct-native"
[providers.codex-plus]
harness = "codex"
connection = {{ kind = "native" }}
auth_family = "openai"
limits_source = "codex_appserver"
priority_multiplier = 5.0
[[providers.codex-plus.models]]
id = "gpt"
[[providers.codex-plus.bindings]]
label = "plus"
account = "acct-native"
priority_multiplier = 2.0
models = ["gpt"]
[providers.glm]
harness = "claude-code"
connection = {{ kind = "custom", endpoint = "https://api.example.com/messages", protocol = "messages" }}
auth_family = "anthropic"
limits_source = "lua"
collector = {{ script = "glm_quota", origins = ["https://api.example.com"] }}
[[providers.glm.models]]
id = "glm-5.3"
native_model = "glm-5.3[1m]"
[[providers.glm.bindings]]
label = "work"
account = "acct-glm"
"#,
        home = home.display()
    )
}

/// Returns fake global account records without credential bytes.
fn accounts() -> Vec<AccountRecord> {
    [
        ("acct-native", "openai", "native:codex"),
        ("acct-glm", "anthropic", "keychain:agent-run:fake-glm"),
    ]
    .into_iter()
    .map(|(id, family, reference)| AccountRecord {
        account_id: AccountId::from_str(id).unwrap(),
        auth_family: AuthFamily::from_str(family).unwrap(),
        secret_ref: SecretRef::from_str(reference).unwrap(),
        status: AccountStatus::Enabled,
    })
    .collect()
}

/// Native aliases share one account, custom providers retain their protocol,
/// and defaults, model parameters, prose, and snapshots remain explicit.
#[test]
fn v2_resolves_named_providers_without_secret_snapshot_values() {
    let home = tempfile::tempdir().unwrap();
    let config = ProviderConfig::parse(&document(home.path()), home.path()).unwrap();
    let catalog = config.resolve_catalog(accounts()).unwrap();
    let native: AccountId = "acct-native".parse().unwrap();
    assert_eq!(catalog.aliases_of(&native).len(), 2);
    let codex = catalog.provider(&"codex".parse().unwrap()).unwrap();
    assert!(matches!(codex.connection, ProviderConnection::Native));
    assert_eq!(codex.priority_multiplier.get(), 1.0);
    assert_eq!(codex.recommendations, ["native subscription"]);
    assert_eq!(codex.models[0].native_model.as_deref(), Some("gpt-native"));
    assert_eq!(codex.models[0].allowed_params["effort"], ["medium", "high"]);
    let plus = catalog.provider(&"codex-plus".parse().unwrap()).unwrap();
    assert_eq!(plus.priority_multiplier.get(), 5.0);
    assert_eq!(plus.bindings[0].multiplier.get(), 2.0);
    assert!(matches!(
        catalog
            .provider(&"glm".parse().unwrap())
            .unwrap()
            .connection,
        ProviderConnection::Custom { .. }
    ));
    let snapshot = config.snapshot().unwrap().to_string();
    assert!(snapshot.contains("codex-plus"));
    assert!(!snapshot.contains("fake-secret-value"));
    assert!(!snapshot.contains("keychain:fake"));
}

/// Structural, protocol, model, and account failures never yield a resolved catalog.
#[test]
fn v2_rejects_invalid_provider_contracts() {
    let home = tempfile::tempdir().unwrap();
    let valid = document(home.path());
    for invalid in [
        valid.replace("schema_version = 2", "schema_version = 1"),
        format!("{valid}\n[providers.codex]\n"),
        valid.replacen("id = \"gpt\"", "native_model = \"gpt\"", 1),
        valid.replace(
            "binary = \"/bin/true\"",
            "binary = \"/bin/true\"\nunknown = 1",
        ),
        valid.replace("models = [\"gpt\"]", "models = [\"missing\"]"),
        valid.replace("priority_multiplier = 5.0", "priority_multiplier = -1.0"),
        valid.replace("protocol = \"messages\"", "protocol = \"responses\""),
        valid.replace(
            "connection = { kind = \"native\" }",
            "connection = { kind = \"custom\", endpoint = \"https://api.example.com\", protocol = \"responses\", auth_header = \"x_api_key\" }",
        ),
        valid.replace("https://api.example.com/messages", ""),
        valid.replace(
            "allowed_params = { effort = [\"medium\", \"high\"] }",
            "allowed_params = { effort = [] }",
        ),
        valid.replace(
            "allowed_params = { effort = [\"medium\", \"high\"] }",
            "allowed_params = { reasoning = [\"high\"] }",
        ),
    ] {
        let parsed = ProviderConfig::parse(&invalid, home.path());
        assert!(
            match parsed {
                Err(_) => true,
                Ok(config) => config.resolve_catalog(accounts()).is_err(),
            },
            "{invalid}"
        );
    }
    let config = ProviderConfig::parse(&valid, home.path()).unwrap();
    assert!(config.resolve_catalog(vec![]).is_err());
    for retired in ["native", "codexbar", "omniroute", "provider"] {
        assert!(
            ProviderConfig::parse(&valid.replace("codex_appserver", retired), home.path()).is_err()
        );
    }
    // The retired CodexBar binary is schema-1 migration input only.
    let with_codexbar = format!("{valid}\n[capacity]\ncodexbar_binary = \"/bin/true\"\n");
    let error = ProviderConfig::parse(&with_codexbar, home.path()).unwrap_err();
    assert!(
        error.to_string().contains("codexbar_binary is retired"),
        "{error}"
    );
}

/// Invalid revisions leave the caller's last valid parsed value untouched.
#[test]
fn v2_reload_preserves_last_valid_revision() {
    let home = tempfile::tempdir().unwrap();
    fs::write(home.path().join("config.toml"), document(home.path())).unwrap();
    let (active, revision) = ProviderConfig::load(home.path()).unwrap();
    assert!(
        ProviderConfig::load_if_changed(home.path(), Some(&revision))
            .unwrap()
            .is_none()
    );
    fs::write(
        home.path().join("config.toml"),
        "schema_version = 2\n[providers.bad]\n",
    )
    .unwrap();
    assert!(ProviderConfig::load_if_changed(home.path(), Some(&revision)).is_err());
    assert_eq!(
        active
            .resolve_catalog(accounts())
            .unwrap()
            .providers()
            .len(),
        3
    );
}
