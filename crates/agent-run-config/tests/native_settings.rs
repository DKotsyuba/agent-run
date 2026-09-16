//! Ports `tests/test_native_settings.py::NativeSettingsParsing` — strict
//! type, key, adapter, and reserved-root validation of `native_settings` at
//! config-load time. Materialization (rendering into each native config
//! file) belongs to the adapter crates and is out of this file's scope; see
//! `crates/agent-run-config/tests/config_compat.rs` for the golden-corpus
//! cases (`tests/fixtures/baseline/config/cases.json`, `source_test`
//! `tests/test_native_settings.py`).
mod common;

use agent_run_config::config::{self, Adapter, Config};

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
        (Adapter::Qwen, "tools"),
        (Adapter::Qwen, "mcpServers"),
        (Adapter::Qwen, "mcp"),
        (Adapter::Qwen, "security"),
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

/// Mirrors `test_qwen_tools_sandbox_is_not_tunable`: the whole `tools` root
/// stays reserved even when the declared value is a nested table.
#[test]
fn qwen_tools_root_stays_reserved_even_nested() {
    let settings = std::collections::BTreeMap::from([(
        "tools".to_string(),
        toml::Value::Table(toml::map::Map::from_iter([(
            "sandbox".to_string(),
            toml::Value::Boolean(false),
        )])),
    )]);
    assert!(config::native_settings(Adapter::Qwen, &settings).is_err());
}
