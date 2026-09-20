//! Materialization and programmatic validation for native runtime settings.

use agent_run_adapters::{materialize, validate};
use agent_run_config::{
    config::{Config, Hook, Runtime},
    profiles::Profile,
};
use agent_run_domain::domain::StartRequest;
use serde_json::json;
use std::{
    collections::{BTreeMap, BTreeSet},
    path::{Path, PathBuf},
};

/// Build the minimal config needed by the adapter materializer.
fn config() -> Config {
    serde_json::from_value(json!({"schema_version": 1})).expect("config")
}

/// Build a legacy profile with no additional capability declarations.
fn profile() -> Profile {
    Profile {
        name: "review".into(),
        body: "Review carefully.".into(),
        write: false,
        network: false,
        revision: "legacy".into(),
        canonical: false,
        allow_external_read_roots: true,
        read_roots: vec![],
        skills: vec![],
        mcp: vec![],
        required_constraints: BTreeSet::new(),
    }
}

/// Build a request matching the fixture profile and configured model.
fn request(runtime: &str, model: &str, workdir: &Path) -> StartRequest {
    serde_json::from_value(json!({
        "runtime": runtime,
        "model": model,
        "profile": "review",
        "task": "fixture",
        "workdir": workdir,
    }))
    .expect("request")
}

/// Build one runtime with a private home and optional native settings.
fn runtime(root: &Path, adapter: &str, native_settings: BTreeMap<String, toml::Value>) -> Runtime {
    let mut value = json!({
        "enabled": true,
        "adapter": adapter,
        "binary": "/bin/true",
        "home": root.join("runtime-home"),
        "models": ["fixture"],
        "native_settings": native_settings,
    });
    if adapter == "codex" {
        value["auth"] = json!({
            "kind": "file_link",
            "source": root.join("auth.json"),
            "target": "auth.json"
        });
    }
    serde_json::from_value(value).expect("runtime")
}

/// Materialize a runtime into an isolated temporary home and return its output.
fn materialize_runtime(
    root: &Path,
    adapter: &str,
    native_settings: BTreeMap<String, toml::Value>,
) -> (Runtime, StartRequest, PathBuf, String) {
    std::fs::write(root.join("auth.json"), "{}\n").expect("auth fixture");
    let runtime = runtime(root, adapter, native_settings);
    let workdir = root.join("workdir");
    std::fs::create_dir_all(&workdir).expect("workdir");
    let request = request(adapter, "fixture", &workdir);
    let home = root.join(format!("{}-home", adapter));
    let (_, revision) =
        materialize::materialize(&config(), &runtime, &request, &profile(), &home, root)
            .expect("materialize");
    (runtime, request, home, revision)
}

/// Mirrors `tests/test_native_settings.py::CodexNativeSettingsMaterialize::test_omitted_settings_preserve_current_defaults`.
#[test]
fn omitted_codex_settings_preserve_current_defaults() {
    let root = tempfile::tempdir().expect("temporary root");
    let (_, _, home, _) = materialize_runtime(root.path(), "codex", BTreeMap::new());
    let document: toml::Value =
        toml::from_str(&std::fs::read_to_string(home.join("config.toml")).expect("config"))
            .expect("TOML");
    assert_eq!(
        document["model_context_window"].as_integer(),
        Some(1_000_000)
    );
    assert_eq!(
        document["model_auto_compact_token_limit"].as_integer(),
        Some(780_000)
    );
    assert_eq!(
        document["model_auto_compact_token_limit_scope"].as_str(),
        Some("total")
    );
}

/// Mirrors `tests/test_native_settings.py::CodexNativeSettingsMaterialize::test_declared_settings_land_and_change_fingerprint`.
#[test]
fn declared_codex_settings_change_values_and_fingerprint() {
    let first = tempfile::tempdir().expect("first root");
    let (_, _, _, default_revision) = materialize_runtime(first.path(), "codex", BTreeMap::new());
    let second = tempfile::tempdir().expect("second root");
    let settings = BTreeMap::from([
        ("model_context_window".into(), toml::Value::Integer(500_000)),
        (
            "tuning".into(),
            toml::Value::Table(toml::map::Map::from_iter([
                ("retries".into(), toml::Value::Integer(3)),
                (
                    "labels".into(),
                    toml::Value::Array(vec![
                        toml::Value::String("fast".into()),
                        toml::Value::String("deep".into()),
                    ]),
                ),
            ])),
        ),
    ]);
    let (_, _, home, tuned_revision) = materialize_runtime(second.path(), "codex", settings);
    let document: toml::Value =
        toml::from_str(&std::fs::read_to_string(home.join("config.toml")).expect("config"))
            .expect("TOML");
    assert_ne!(default_revision, tuned_revision);
    assert_eq!(document["model_context_window"].as_integer(), Some(500_000));
    assert_eq!(document["tuning"]["retries"].as_integer(), Some(3));
    assert_eq!(
        document["tuning"]["labels"].as_array().map(Vec::len),
        Some(2)
    );
}

/// Mirrors `tests/test_native_settings.py::CodexNativeSettingsMaterialize::test_string_escaping_roundtrips_through_tomllib`.
#[test]
fn codex_native_strings_round_trip() {
    let root = tempfile::tempdir().expect("temporary root");
    let nasty = "quote \" backslash \\ newline \n tab \t";
    let settings = BTreeMap::from([("label".into(), toml::Value::String(nasty.into()))]);
    let (_, _, home, _) = materialize_runtime(root.path(), "codex", settings);
    let document: toml::Value =
        toml::from_str(&std::fs::read_to_string(home.join("config.toml")).expect("config"))
            .expect("TOML");
    assert_eq!(document["label"].as_str(), Some(nasty));
}

/// Mirrors `tests/test_native_settings.py::CodexNativeSettingsMaterialize::test_nested_settings_do_not_capture_following_agent_run_roots`.
#[test]
fn nested_codex_settings_stay_before_owned_roots() {
    let root = tempfile::tempdir().expect("temporary root");
    let settings = BTreeMap::from([(
        "tui".into(),
        toml::Value::Table(toml::map::Map::from_iter([(
            "theme".into(),
            toml::Value::String("dark".into()),
        )])),
    )]);
    let (_, _, home, _) = materialize_runtime(root.path(), "codex", settings);
    let document: toml::Value =
        toml::from_str(&std::fs::read_to_string(home.join("config.toml")).expect("config"))
            .expect("TOML");
    assert_eq!(document["tui"]["theme"].as_str(), Some("dark"));
    assert!(document.get("projects").is_some());
    assert!(document["tui"].get("projects").is_none());
}

/// Mirrors `tests/test_native_settings.py::CodexNativeSettingsMaterialize::test_known_routing_and_reviewer_aliases_fail_closed`.
#[test]
fn codex_routing_and_reviewer_aliases_fail_closed() {
    let root = tempfile::tempdir().expect("temporary root");
    for key in [
        "model_providers",
        "openai_base_url",
        "chatgpt_base_url",
        "approvals_reviewer",
        "openai_api_key",
        "cli_auth_credentials_store",
        "mcp_oauth_credentials_store",
        "forced_login_method",
        "forced_chatgpt_workspace_id",
        "skills",
        "tools",
        "agents",
        "apps",
        "web_search",
    ] {
        let settings = BTreeMap::from([(key.into(), toml::Value::Integer(1))]);
        let runtime = runtime(root.path(), "codex", settings);
        assert!(
            validate(
                &request("codex", "fixture", root.path()),
                &runtime,
                &profile()
            )
            .is_err(),
            "{key}"
        );
    }
}

/// Mirrors `tests/test_native_settings.py::CodexNativeSettingsMaterialize::test_validate_rejects_reserved_roots_for_programmatic_configs`.
#[test]
fn programmatic_codex_reserved_roots_fail_closed() {
    let root = tempfile::tempdir().expect("temporary root");
    for key in [
        "model",
        "approval_policy",
        "sandbox_mode",
        "mcp_servers",
        "features",
    ] {
        let settings = BTreeMap::from([(key.into(), toml::Value::String("x".into()))]);
        let runtime = runtime(root.path(), "codex", settings);
        assert!(
            validate(
                &request("codex", "fixture", root.path()),
                &runtime,
                &profile()
            )
            .is_err(),
            "{key}"
        );
    }
}

/// Mirrors `tests/test_native_settings.py::ClaudeNativeSettings::test_declared_settings_and_hooks_coexist_in_settings_json`.
#[test]
fn claude_settings_and_hooks_coexist() {
    let root = tempfile::tempdir().expect("temporary root");
    let mut runtime = runtime(
        root.path(),
        "claude",
        BTreeMap::from([("spinnerTipsEnabled".into(), toml::Value::Boolean(false))]),
    );
    runtime.hooks.push(Hook {
        event: "PreToolUse".into(),
        command: vec!["/bin/echo".into(), "hook".into()],
        matcher: None,
    });
    let request = request("claude", "fixture", root.path());
    let home = root.path().join("claude-home");
    materialize::materialize(
        &config(),
        &runtime,
        &request,
        &profile(),
        &home,
        root.path(),
    )
    .expect("materialize");
    let document: serde_json::Value =
        serde_json::from_slice(&std::fs::read(home.join("settings.json")).expect("settings"))
            .expect("JSON");
    assert_eq!(document["spinnerTipsEnabled"], false);
    assert!(document.get("hooks").is_some());
}

/// Mirrors `tests/test_native_settings.py::ClaudeNativeSettings::test_render_settings_keeps_hooks_when_declared_settings_empty`.
#[test]
fn empty_claude_settings_keep_hooks() {
    let root = tempfile::tempdir().expect("temporary root");
    let mut runtime = runtime(root.path(), "claude", BTreeMap::new());
    runtime.hooks.push(Hook {
        event: "PreToolUse".into(),
        command: vec!["/bin/echo".into(), "hook".into()],
        matcher: None,
    });
    let request = request("claude", "fixture", root.path());
    let home = root.path().join("claude-home");
    materialize::materialize(
        &config(),
        &runtime,
        &request,
        &profile(),
        &home,
        root.path(),
    )
    .expect("materialize");
    let document: serde_json::Value =
        serde_json::from_slice(&std::fs::read(home.join("settings.json")).expect("settings"))
            .expect("JSON");
    assert!(document.get("hooks").is_some());
    assert!(document.get("native_settings").is_none());
}

/// Mirrors `tests/test_native_settings.py::ClaudeNativeSettings::test_reserved_roots_rejected_for_claude_and_glm`.
#[test]
fn claude_and_glm_reserved_roots_fail_closed() {
    let root = tempfile::tempdir().expect("temporary root");
    for adapter in ["claude", "glm"] {
        for key in [
            "hooks",
            "env",
            "statusLine",
            "disableAllHooks",
            "model",
            "agent",
            "autoMemoryDirectory",
        ] {
            let settings = BTreeMap::from([(key.into(), toml::Value::Integer(1))]);
            let runtime = runtime(root.path(), adapter, settings);
            assert!(
                validate(
                    &request(adapter, "fixture", root.path()),
                    &runtime,
                    &profile()
                )
                .is_err(),
                "{adapter}.{key}"
            );
        }
    }
}
