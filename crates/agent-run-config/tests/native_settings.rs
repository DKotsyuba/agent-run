//! Ports `tests/test_native_settings.py::NativeSettingsParsing` — strict
//! type, key, adapter, and reserved-root validation of `native_settings` at
//! config-load time. Materialization (rendering into each native config
//! file) belongs to the adapter crates and is out of this file's scope; see
//! `crates/agent-run-config/tests/config_compat.rs` for the golden-corpus
//! cases (`tests/fixtures/baseline/config/cases.json`, `source_test`
//! `tests/test_native_settings.py`).
mod common;

use agent_run_config::{
    config::{self, Adapter, Config, Runtime},
    role_plan::ResolvedRolePlan,
    snapshot,
};
use serde_json::{json, Value};
use std::collections::{BTreeMap, BTreeSet};

fn write(home: &common::Home, extra: &str) -> String {
    format!(
        "schema_version = 1\n[runtimes.codex]\nenabled = true\nadapter = \"agent_run.adapters.codex:ADAPTER\"\nbinary = \"/bin/echo\"\nhome = {:?}\nmodels = [\"gpt-5\"]\n{extra}",
        home.path.join("codex-home").to_string_lossy()
    )
}

fn load(home: &common::Home, extra: &str) -> agent_run_domain::Result<Config> {
    std::fs::write(home.path.join("config.toml"), write(home, extra)).unwrap();
    Config::load(&home.path)
}

/// Mirrors `test_valid_tree_parses_immutable`: nested options round-trip and
/// the map is a strict, deeply validated structure (immutability is a type
/// guarantee here: `BTreeMap`/`toml::Value` have no interior mutability).
#[test]
fn valid_tree_parses_with_nested_tables() {
    let home = common::Home::new();
    let cfg = load(
        &home,
        "[runtimes.codex.native_settings]\n\
         model_context_window = 500000\n\
         ratio = 0.5\n\
         labels = [\"a\", \"b\"]\n\
         [runtimes.codex.native_settings.tuning]\n\
         retries = 3\n\
         quiet = true\n",
    )
    .unwrap();
    let settings = &cfg.runtimes["codex"].native_settings;
    assert_eq!(
        settings["model_context_window"],
        toml::Value::Integer(500_000)
    );
    assert_eq!(
        settings["labels"],
        toml::Value::Array(vec![
            toml::Value::String("a".into()),
            toml::Value::String("b".into())
        ])
    );
    let toml::Value::Table(tuning) = &settings["tuning"] else {
        panic!("tuning must parse as a table")
    };
    assert_eq!(tuning["retries"], toml::Value::Integer(3));
}

/// Mirrors `test_empty_declaration_preserves_previous_defaults`.
#[test]
fn omitted_table_defaults_to_empty() {
    let home = common::Home::new();
    let cfg = load(&home, "").unwrap();
    assert!(cfg.runtimes["codex"].native_settings.is_empty());
}

/// Mirrors `test_dotted_key_literal_is_rejected`.
#[test]
fn dotted_key_literal_is_rejected() {
    let home = common::Home::new();
    assert!(load(&home, "[runtimes.codex.native_settings]\n\"a.b\" = 1\n").is_err());
}

// Rust-internal security coverage: config parsing validates the credential
// reference without opening the referenced secret source.
#[test]
fn config_parsing_does_not_read_file_link_secret_source() {
    let home = common::Home::new();
    let source = home.path.join("credential-source-that-must-not-be-read");
    std::fs::create_dir(&source).unwrap();
    let text = format!(
        "schema_version=1\n[runtimes.codex]\nenabled=true\nadapter=\"codex\"\nbinary=\"/bin/echo\"\nhome=\"{}\"\nmodels=[\"fixture\"]\n[runtimes.codex.auth]\nkind=\"file_link\"\nsource=\"{}\"\ntarget=\"auth.json\"\n",
        home.path.join("runtime").display(),
        source.display()
    );
    std::fs::write(home.path.join("config.toml"), text).unwrap();
    let config = Config::load(&home.path).expect("source contents are not part of parsing");
    assert_eq!(
        config.runtimes["codex"]
            .auth
            .as_ref()
            .and_then(|auth| match auth {
                agent_run_config::config::Auth::FileLink { source, .. } => Some(source),
                _ => None,
            }),
        Some(&source)
    );
}

/// Mirrors `test_blank_key_is_rejected`.
#[test]
fn blank_key_is_rejected() {
    let home = common::Home::new();
    assert!(load(&home, "[runtimes.codex.native_settings]\n\"\" = 1\n").is_err());
}

/// Mirrors `test_date_value_is_rejected`.
#[test]
fn date_value_is_rejected() {
    let home = common::Home::new();
    assert!(load(
        &home,
        "[runtimes.codex.native_settings]\nseen = 2024-01-01\n"
    )
    .is_err());
}

/// Mirrors `test_nonfinite_and_exotic_values_are_rejected` for the subset TOML
/// can even represent (TOML itself has no null/object literal, so those two
/// Python cases have no Rust analog: a `toml::Value` can never hold them).
#[test]
fn nonfinite_float_values_are_rejected() {
    for v in [f64::INFINITY, f64::NEG_INFINITY, f64::NAN] {
        let settings = std::collections::BTreeMap::from([("k".to_string(), toml::Value::Float(v))]);
        assert!(config::native_settings(Adapter::Codex, &settings).is_err());
    }
}

/// Mirrors `tests/test_native_settings.py::NativeSettingsParsing::test_model_verbosity_is_ordinary_tuning`.
#[test]
fn ordinary_model_verbosity_is_accepted() {
    let home = common::Home::new();
    let cfg = load(
        &home,
        "[runtimes.codex.native_settings]\nmodel_verbosity = \"low\"\n",
    )
    .unwrap();
    assert_eq!(
        cfg.runtimes["codex"].native_settings["model_verbosity"],
        toml::Value::String("low".into())
    );
}

/// Mirrors `tests/test_native_settings.py::NativeSettingsParsing::test_documented_full_config_example_loads`.
#[test]
fn documented_full_config_example_loads() {
    let guide = include_str!("../../../assets/operator_guide/config.md");
    let block = guide
        .split_once("```toml\n")
        .and_then(|(_, rest)| rest.split_once("```").map(|(text, _)| text))
        .expect("operator guide must keep a TOML example");
    let home = common::Home::new();
    std::fs::write(home.path.join("config.toml"), block).unwrap();
    let (cfg, _) = agent_run_config::provider_config::ProviderConfig::load(&home.path).unwrap();
    assert!(cfg
        .harnesses
        .contains_key(&agent_run_domain::catalog::HarnessId::Codex));
}

/// Build a fixed runtime and role so canonical snapshot bytes are reproducible.
fn role_fixture() -> ResolvedRolePlan {
    ResolvedRolePlan {
        role_name: "review".into(),
        role_revision: "legacy".into(),
        prompt: "Review carefully.".into(),
        write: false,
        network: false,
        allow_external_read_roots: true,
        read_roots: vec![],
        skills: vec![],
        mcp: vec![],
        required_constraints: BTreeSet::new(),
        auth_mode: "global".into(),
        auth_reference: None,
        config_revision: "c".repeat(64),
    }
}

/// Build a fixed runtime and role so canonical snapshot bytes are reproducible.
fn snapshot_fixture(settings: BTreeMap<String, toml::Value>) -> snapshot::ConfigSnapshot {
    let runtime: Runtime = serde_json::from_value(json!({
        "enabled": true,
        "adapter": "codex",
        "binary": "/bin/true",
        "home": "/tmp/native-settings-home",
        "models": ["fixture"],
        "native_settings": settings,
    }))
    .expect("runtime");
    let config: Config = serde_json::from_value(json!({"schema_version": 1})).expect("config");
    let role = role_fixture();
    snapshot::build_config_snapshot(
        "codex",
        2,
        1,
        &"a".repeat(64),
        &"b".repeat(64),
        &config,
        &runtime,
        &role,
        None,
    )
    .expect("snapshot")
}

/// Mirrors `tests/test_native_settings.py::SnapshotAndScopedCopies::test_runtime_document_records_sorted_settings`.
#[test]
fn runtime_snapshot_records_sorted_native_settings() {
    let snapshot = snapshot_fixture(BTreeMap::from([
        ("z".into(), toml::Value::Integer(1)),
        (
            "a".into(),
            toml::Value::Table(toml::map::Map::from_iter([(
                "nested".into(),
                toml::Value::Array(vec![
                    toml::Value::Integer(1),
                    toml::Value::String("x".into()),
                ]),
            )])),
        ),
    ]));
    let document: Value = serde_json::from_slice(&snapshot.document).expect("JSON");
    assert_eq!(
        document["runtime_config"]["native_settings"],
        json!({"a": {"nested": [1, "x"]}, "z": 1})
    );
}

/// Mirrors `tests/test_native_settings.py::SnapshotAndScopedCopies::test_runtime_document_omits_empty_settings`.
#[test]
fn empty_native_settings_are_omitted_from_snapshot() {
    let snapshot = snapshot_fixture(BTreeMap::new());
    let document: Value = serde_json::from_slice(&snapshot.document).expect("JSON");
    assert!(document["runtime_config"].get("native_settings").is_none());
}

/// Mirrors `tests/test_native_settings.py::SnapshotAndScopedCopies::test_settings_change_changes_document_identity`.
#[test]
fn changing_native_settings_changes_snapshot_identity() {
    let first = snapshot_fixture(BTreeMap::from([("k".into(), toml::Value::Integer(1))]));
    let second = snapshot_fixture(BTreeMap::from([("k".into(), toml::Value::Integer(2))]));
    assert_ne!(first.document, second.document);
    assert_ne!(first.sha256, second.sha256);
}

/// Mirrors `tests/test_native_settings.py::SnapshotAndScopedCopies::test_parse_scoped_snapshot_roundtrip_materializes`.
#[test]
fn native_settings_survive_snapshot_roundtrip() {
    let original = BTreeMap::from([
        ("model_context_window".into(), toml::Value::Integer(250_000)),
        (
            "tuning".into(),
            toml::Value::Table(toml::map::Map::from_iter([(
                "retries".into(),
                toml::Value::Integer(3),
            )])),
        ),
    ]);
    let snapshot = snapshot_fixture(original);
    let document: Value = serde_json::from_slice(&snapshot.document).expect("JSON");
    let restored: BTreeMap<String, toml::Value> =
        serde_json::from_value(document["runtime_config"]["native_settings"].clone())
            .expect("native settings");
    assert_eq!(
        restored["model_context_window"],
        toml::Value::Integer(250_000)
    );
    assert_eq!(restored["tuning"]["retries"], toml::Value::Integer(3));
}

/// Mirrors `tests/test_native_settings.py::SnapshotAndScopedCopies::test_json_conversion_is_deterministic_for_snapshot_bytes`.
#[test]
fn canonical_snapshot_bytes_are_exact_and_key_sorted() {
    let settings = BTreeMap::from([
        (
            "z".into(),
            toml::Value::Array(vec![toml::Value::Integer(1), toml::Value::Integer(2)]),
        ),
        (
            "a".into(),
            toml::Value::Table(toml::map::Map::from_iter([(
                "b".into(),
                toml::Value::Boolean(true),
            )])),
        ),
    ]);
    let actual = snapshot_fixture(settings).document;
    let runtime_config = json!({
        "adapter": "codex",
        "auth": null,
        "binary": "/bin/true",
        "default_account": null,
        "enabled": true,
        "environment": null,
        "hooks": [],
        "home": "/tmp/native-settings-home",
        "limits_source": null,
        "mcp": [],
        "max_active_agents": null,
        "models": ["fixture"],
        "native_settings": {"a": {"b": true}, "z": [1, 2]},
        "accounts": [],
        "plugin_snapshot_assets": {},
        "plugins": [],
        "priority_account_multipliers": {},
        "priority_lane_multipliers": {},
        "priority_multiplier": 1.0,
        "rust": null,
        "skills": [],
    });
    let runtime_hash =
        agent_run_platform::fs::sha256(&agent_run_domain::canonical::dumps(&runtime_config, true));
    let expected = json!({
        "snapshot_version": 1,
        "runtime": "codex",
        "runtime_version": null,
        "adapter_api_version": 2,
        "config_schema_version": 1,
        "runtime_config_sha256": runtime_hash,
        "runtime_config": runtime_config,
        "materialize_revision": "a".repeat(64),
        "snapshot_index_sha256": "b".repeat(64),
        "profile": role_fixture().to_payload(),
    });
    let mut expected_bytes = agent_run_domain::canonical::dumps(&expected, true);
    expected_bytes.push(b'\n');
    assert_eq!(actual, expected_bytes);
}

/// Mirrors `test_unsupported_adapter_is_rejected`: a runtime whose adapter
/// has no packaged reserved-root/merge contract cannot accept the table.
#[test]
fn unsupported_adapter_is_rejected() {
    let home = common::Home::new();
    let text = format!(
        "schema_version = 1\n[runtimes.stub]\nenabled = true\nadapter = \"agent_run.adapters.stub:ADAPTER\"\nbinary = \"/bin/echo\"\nhome = {:?}\nmodels = [\"m\"]\n[runtimes.stub.native_settings]\nkey = 1\n",
        home.path.join("stub-home").to_string_lossy()
    );
    std::fs::write(home.path.join("config.toml"), text).unwrap();
    assert!(Config::load(&home.path).is_err());
}

/// Mirrors `test_reserved_roots_fail_closed_per_adapter`: every listed
/// control surface is rejected with the declaring runtime still named.
#[test]
fn reserved_roots_fail_closed_per_adapter() {
    let cases = [
        (Adapter::Codex, "model"),
        (Adapter::Codex, "shell_environment_policy"),
        (Adapter::Codex, "notify"),
        (Adapter::Codex, "hooks"),
        (Adapter::Codex, "model_providers"),
        (Adapter::Claude, "env"),
        (Adapter::Claude, "apiKeyHelper"),
        (Adapter::Claude, "permissions"),
        (Adapter::Claude, "awsAuthRefresh"),
        (Adapter::Claude, "enabledPlugins"),
        (Adapter::Glm, "disableAllHooks"),
    ];
    for (adapter, key) in cases {
        let settings =
            std::collections::BTreeMap::from([(key.to_string(), toml::Value::Integer(1))]);
        assert!(
            config::native_settings(adapter, &settings).is_err(),
            "{adapter:?}.{key} must be reserved"
        );
    }
}
