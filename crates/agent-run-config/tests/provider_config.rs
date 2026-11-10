//! V2 harness/provider parsing, account cross-references, and safe snapshots.

use agent_run_config::provider_config::ProviderConfig;
use agent_run_domain::{
    AccountId, AuthFamily, SecretRef,
    catalog::{AccountRecord, AccountStatus, ProviderConnection},
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
limits_source = "none"
recommendations = ["native subscription"]
[[providers.codex.models]]
id = "gpt"
native_model = "gpt-native"
params = {{ effort = "medium" }}
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
limits_source = "none"
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
limits_source = "exec"
collector = {{ command = "/bin/bash", args = ["/collectors/glm.sh"] }}
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

/// Ordered Claude bindings are opt-in and leave legacy normalized snapshots unchanged.
#[test]
fn ordered_accounts_preserve_snapshots_and_validate_connections() {
    let home = tempfile::tempdir().unwrap();
    let text = document(home.path());
    let legacy = ProviderConfig::parse(&text, home.path()).unwrap();
    let legacy_json = serde_json::to_value(&legacy).unwrap();
    for provider in legacy_json["providers"].as_object().unwrap().values() {
        assert!(provider.get("ordered_accounts").is_none());
        assert!(
            provider["bindings"]
                .as_array()
                .unwrap()
                .iter()
                .all(|binding| binding.get("connection").is_none())
        );
    }
    let explicit_false = text.replace(
        "[providers.glm]",
        "[providers.glm]\nordered_accounts = false",
    );
    let restored = ProviderConfig::parse(&explicit_false, home.path()).unwrap();
    assert_eq!(
        serde_json::to_vec(&legacy).unwrap(),
        serde_json::to_vec(&restored).unwrap()
    );
    assert_eq!(legacy.snapshot().unwrap(), restored.snapshot().unwrap());
    let reread: ProviderConfig = serde_json::from_value(legacy_json.clone()).unwrap();
    assert_eq!(serde_json::to_value(&reread).unwrap(), legacy_json);
    assert_eq!(reread.snapshot().unwrap(), legacy.snapshot().unwrap());

    let ordered = text
        .replace("[providers.glm]", "[providers.glm]\nordered_accounts = true")
        .replace("limits_source = \"exec\"\ncollector = { command = \"/bin/bash\", args = [\"/collectors/glm.sh\"] }", "limits_source = \"none\"");
    let ordered = format!(
        "{ordered}\n[[providers.glm.bindings]]\nlabel='backup'\naccount='acct-backup'\nconnection={{kind='native'}}\npriority_multiplier=0.5\n"
    );
    let config = ProviderConfig::parse(&ordered, home.path()).unwrap();
    let mut registered = accounts();
    registered.push(AccountRecord {
        account_id: "acct-backup".parse().unwrap(),
        auth_family: "anthropic".parse().unwrap(),
        secret_ref: "named:claude-code:backup".parse().unwrap(),
        status: AccountStatus::Enabled,
    });
    let catalog = config.resolve_catalog(registered).unwrap();
    let definition = catalog.provider(&"glm".parse().unwrap()).unwrap();
    assert!(definition.ordered_accounts);
    assert_eq!(
        definition.connection_for(&"acct-backup".parse().unwrap(), "glm-5.3"),
        Some(&ProviderConnection::Native)
    );
    let encoded = serde_json::to_value(&config).unwrap();
    assert_eq!(encoded["providers"]["glm"]["ordered_accounts"], true);
    let mut catalog_entry = serde_json::to_value(definition).unwrap();
    catalog_entry.as_object_mut().unwrap().remove("id");
    assert_eq!(catalog_entry, encoded["providers"]["glm"]);

    for invalid in [
        ordered.replace("ordered_accounts = true", "ordered_accounts = false"),
        ordered.replace("account='acct-backup'", "account='acct-glm'"),
        ordered.replace("connection={kind='native'}", "connection={kind='custom',endpoint='https://other.example',protocol='messages'}"),
        ordered.replace("connection = { kind = \"custom\", endpoint = \"https://api.example.com/messages\", protocol = \"messages\" }", "connection = { kind = \"native\" }"),
        ordered.replace("limits_source = \"none\"\n[[providers.glm.models]]", "limits_source = \"exec\"\ncollector = { command = '/bin/bash' }\n[[providers.glm.models]]"),
    ] {
        assert!(ProviderConfig::parse(&invalid, home.path()).is_err(), "{invalid}");
    }
}

/// The global Fast flag is Codex-only and omission preserves historical normalized snapshots.
#[test]
fn harness_fast_mode_defaults_and_validation() {
    let home = tempfile::tempdir().unwrap();
    let text = document(home.path());
    let legacy = ProviderConfig::parse(&text, home.path()).unwrap();
    let encoded = serde_json::to_value(&legacy).unwrap();
    assert!(encoded["harnesses"]["codex"].get("fast_mode").is_none());
    let enabled = text.replace("[harnesses.codex]", "[harnesses.codex]\nfast_mode = true");
    let config = ProviderConfig::parse(&enabled, home.path()).expect("global Codex Fast flag");
    assert_eq!(
        serde_json::to_value(&config).unwrap()["harnesses"]["codex"]["fast_mode"],
        true
    );
    assert_ne!(legacy.snapshot().unwrap(), config.snapshot().unwrap());
    let restored: ProviderConfig = serde_json::from_value(encoded).unwrap();
    assert_eq!(legacy.snapshot().unwrap(), restored.snapshot().unwrap());
    let disabled = text.replace("[harnesses.codex]", "[harnesses.codex]\nfast_mode = false");
    assert_eq!(
        legacy.snapshot().unwrap(),
        ProviderConfig::parse(&disabled, home.path())
            .unwrap()
            .snapshot()
            .unwrap()
    );
    let wrong = text.replace(
        "[harnesses.claude-code]",
        "[harnesses.claude-code]\nfast_mode = true",
    );
    assert!(ProviderConfig::parse(&wrong, home.path()).is_err());
    let malformed = text.replace(
        "[harnesses.codex]",
        "[harnesses.codex]\nfast_mode = \"yes\"",
    );
    assert!(ProviderConfig::parse(&malformed, home.path()).is_err());
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

/// Schema 2 preserves explicit MCP controls through normalization and snapshots.
#[test]
fn mcp_controls_validate_without_changing_legacy_defaults() {
    let home = tempfile::tempdir().unwrap();
    let base = format!(
        "{}\n[mcp.shared]\ntransport='stdio'\ncommand='/bin/echo'\n",
        document(home.path())
    );
    let config = ProviderConfig::parse(
        &format!("{base}global=true\nallowed_tools=['read']\n"),
        home.path(),
    )
    .unwrap();
    assert!(config.mcp["shared"].global);
    assert_eq!(
        config.mcp["shared"].allowed_tools,
        Some(vec!["read".into()])
    );
    assert_eq!(
        serde_json::to_value(&config).unwrap()["mcp"]["shared"]["allowed_tools"],
        serde_json::json!(["read"])
    );
    for tools in ["['*']", "['read','read']", "['']", "[1]"] {
        assert!(
            ProviderConfig::parse(&format!("{base}allowed_tools={tools}\n"), home.path()).is_err()
        );
    }
    let legacy = ProviderConfig::parse(&base, home.path()).unwrap();
    assert!(
        !serde_json::to_string(&legacy.mcp)
            .unwrap()
            .contains("allowed_tools")
    );
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

/// New configuration rejects unimplemented parameter keys, while an older
/// frozen catalog carrying one remains readable for its existing run.
#[test]
fn old_frozen_model_parameters_remain_resolvable() {
    let home = tempfile::tempdir().unwrap();
    let valid = document(home.path());
    assert!(
        ProviderConfig::parse(
            &valid.replace(
                "params = { effort = \"medium\" }",
                "params = { reasoning = \"medium\" }"
            ),
            home.path(),
        )
        .is_err()
    );
    let mut frozen = ProviderConfig::parse(&valid, home.path()).unwrap();
    let offering = &mut frozen
        .providers
        .get_mut(&"codex".parse().unwrap())
        .unwrap()
        .models[0];
    offering.params.remove("effort");
    offering.params.insert("reasoning".into(), "medium".into());
    assert!(frozen.snapshot().is_ok());
    assert!(frozen.resolve_catalog(accounts()).is_ok());
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
        valid.replace("params = { effort = \"medium\" }", "params = { reasoning = \"medium\" }"),
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
            ProviderConfig::parse(
                &valid.replace(
                    "limits_source = \"none\"",
                    &format!("limits_source = \"{retired}\"")
                ),
                home.path()
            )
            .is_err()
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

/// Old frozen collector bytes round-trip unchanged; live configs refuse the retired execution mode.
#[test]
fn frozen_collectors_keep_their_digest_without_reactivating_lua() {
    let raw = serde_json::json!({"script":"anthropic_usage","origins":["https://api.anthropic.com"],"script_file":null,"auth":null});
    let frozen: agent_run_domain::catalog::CollectorBinding =
        serde_json::from_value(raw.clone()).unwrap();
    assert_eq!(serde_json::to_value(&frozen).unwrap(), raw);
    assert!(frozen.validate().is_err());
    let root = tempfile::tempdir().unwrap();
    let configured = document(root.path());
    for retired in ["lua", "codex_appserver"] {
        let error = ProviderConfig::parse(
            &configured.replace(
                "limits_source = \"exec\"",
                &format!("limits_source = \"{retired}\""),
            ),
            root.path(),
        )
        .unwrap_err();
        assert!(error.to_string().contains("retired"), "{error}");
    }
}

/// The optional OpenCode harness extends schema two without removing required
/// legacy harnesses or changing provider/account identity semantics.
#[test]
fn opencode_is_an_optional_third_native_harness() {
    use agent_run_domain::HarnessId;
    let home = tempfile::tempdir().unwrap();
    let text = format!(
        "{}\n[harnesses.opencode]\nbinary='/bin/true'\nhome='{}'\n[providers.zen]\nharness='opencode'\nconnection={{kind='native'}}\nauth_family='opencode'\nlimits_source='none'\nbindings=[]\n[[providers.zen.models]]\nid='free'\nnative_model='opencode/big-pickle'\n",
        document(home.path()),
        home.path().join("opencode").display()
    );
    let config = ProviderConfig::parse(&text, home.path()).unwrap();
    assert_eq!(config.harnesses.len(), 3);
    assert!(config.harnesses.contains_key(&HarnessId::OpenCode));
    assert_eq!(
        config.providers[&"zen".parse().unwrap()].harness,
        HarnessId::OpenCode
    );
    assert!(
        ProviderConfig::parse(
            &text.replace("[harnesses.opencode]", "[harnesses.unknown]"),
            home.path()
        )
        .is_err()
    );
}

/// Replaces the Claude gateway connection of the shared fixture.
fn with_glm_connection(home: &Path, connection: &str) -> String {
    document(home).replace(
        "connection = { kind = \"custom\", endpoint = \"https://api.example.com/messages\", protocol = \"messages\" }",
        &format!("connection = {connection}"),
    )
}

/// Fixed custom headers validate at load, are sealed into the digest, and leave
/// the serialized form and digest of header-free configurations unchanged.
#[test]
fn custom_headers_validate_and_stay_absent_without_configuration() {
    let home = tempfile::tempdir().unwrap();
    let custom = |headers: &str| {
        format!(
            "{{ kind = \"custom\", endpoint = \"https://api.example.com/messages\", protocol = \"messages\", headers = {headers} }}"
        )
    };
    let base = ProviderConfig::parse(&document(home.path()), home.path()).unwrap();
    let encoded = serde_json::to_value(&base).unwrap();
    assert!(
        encoded["providers"]["glm"]["connection"]
            .get("headers")
            .is_none()
    );
    let empty = ProviderConfig::parse(
        &with_glm_connection(home.path(), &custom("{}")),
        home.path(),
    )
    .unwrap();
    assert_eq!(base.snapshot().unwrap(), empty.snapshot().unwrap());

    let valid = ProviderConfig::parse(
        &with_glm_connection(
            home.path(),
            &custom("{ \"x-opencode-session\" = \"{launch}\", X-Static = \"a b/c\" }"),
        ),
        home.path(),
    )
    .unwrap();
    assert_ne!(base.snapshot().unwrap(), valid.snapshot().unwrap());
    let reread: ProviderConfig =
        serde_json::from_value(serde_json::to_value(&valid).unwrap()).unwrap();
    assert_eq!(reread.snapshot().unwrap(), valid.snapshot().unwrap());
    let definition = valid
        .resolve_catalog(accounts())
        .unwrap()
        .provider(&"glm".parse().unwrap())
        .unwrap()
        .clone();
    assert_eq!(
        definition.connection.render_custom_headers("L1").unwrap(),
        "X-Static: a b/c\nx-opencode-session: L1"
    );
}

/// Every forbidden header class, malformed name or value, and unsupported
/// connection or harness fails closed while parsing the configuration.
#[test]
fn custom_headers_reject_unsafe_names_values_and_placements() {
    let home = tempfile::tempdir().unwrap();
    let too_many = (0..9)
        .map(|n| format!("\"x-h{n}\" = \"v\""))
        .collect::<Vec<_>>()
        .join(", ");
    let long_name = format!("\"{}\" = \"v\"", "x".repeat(65));
    let long_value = format!("\"x-ok\" = \"{}\"", "v".repeat(257));
    let mut bad: Vec<String> = [
        "\"Authorization\" = \"v\"",
        "\"proxy-authorization\" = \"v\"",
        "\"X-Api-Key\" = \"v\"",
        "\"cookie\" = \"v\"",
        "\"Host\" = \"v\"",
        "\"content-length\" = \"1\"",
        "\"Content-Type\" = \"v\"",
        "\"transfer-encoding\" = \"v\"",
        "\"connection\" = \"v\"",
        "\"anthropic-beta\" = \"v\"",
        "\"X-Stainless-Lang\" = \"v\"",
        "\"x-session-token\" = \"v\"",
        "\"x-secret\" = \"v\"",
        "\"x-password\" = \"v\"",
        "\"x-apikey\" = \"v\"",
        "\"x-credential-id\" = \"v\"",
        "\"x-oauth\" = \"v\"",
        "\"bad name\" = \"v\"",
        "\"bad:name\" = \"v\"",
        "\"\" = \"v\"",
        "\"x-dup\" = \"a\", \"X-DUP\" = \"b\"",
        "\"x-ok\" = \"line\\nbreak\"",
        "\"x-ok\" = \"line\\rbreak\"",
        "\"x-ok\" = \"nul\\u0000\"",
        "\"x-ok\" = \"caf\\u00e9\"",
        "\"x-ok\" = \"tab\\there\"",
        "\"x-ok\" = \"\"",
        "\"x-ok\" = \" padded\"",
        "\"x-ok\" = \"{session}\"",
        "\"x-ok\" = \"{{launch}}\"",
        "\"x-ok\" = \"{launch\"",
        "\"x-ok\" = \"{LAUNCH}\"",
        "\"x-ok\" = \"launch}\"",
        &too_many,
        &long_name,
        &long_value,
    ]
    .iter()
    .map(|headers| {
        with_glm_connection(
            home.path(),
            &format!(
                "{{ kind = \"custom\", endpoint = \"https://api.example.com/messages\", protocol = \"messages\", headers = {{ {headers} }} }}"
            ),
        )
    })
    .collect();
    // Native connections cannot carry headers.
    bad.push(with_glm_connection(
        home.path(),
        "{ kind = \"native\", headers = { \"x-ok\" = \"v\" } }",
    ));
    // Codex custom headers are out of scope.
    bad.push(document(home.path()).replace(
        "connection = { kind = \"native\" }\nauth_family = \"openai\"\nlimits_source = \"none\"\nrecommendations",
        "connection = { kind = \"custom\", endpoint = \"https://api.example.com\", protocol = \"responses\", headers = { \"x-ok\" = \"v\" } }\nauth_family = \"openai\"\nlimits_source = \"none\"\nrecommendations",
    ));
    for text in &bad {
        assert!(ProviderConfig::parse(text, home.path()).is_err(), "{text}");
    }
    // The Codex variant parses without headers, proving the rejection is the header.
    ProviderConfig::parse(
        &document(home.path()).replace(
            "connection = { kind = \"native\" }\nauth_family = \"openai\"\nlimits_source = \"none\"\nrecommendations",
            "connection = { kind = \"custom\", endpoint = \"https://api.example.com\", protocol = \"responses\" }\nauth_family = \"openai\"\nlimits_source = \"none\"\nrecommendations",
        ),
        home.path(),
    )
    .unwrap();
    // Exactly the limits are accepted.
    let edge = format!("\"{}\" = \"{}\"", "x".repeat(64), "v".repeat(256));
    let eight = (0..8)
        .map(|n| format!("\"x-h{n}\" = \"v\""))
        .collect::<Vec<_>>()
        .join(", ");
    for headers in [edge, eight] {
        ProviderConfig::parse(
            &with_glm_connection(
                home.path(),
                &format!(
                    "{{ kind = \"custom\", endpoint = \"https://api.example.com/messages\", protocol = \"messages\", headers = {{ {headers} }} }}"
                ),
            ),
            home.path(),
        )
        .unwrap();
    }
}
