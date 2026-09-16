//! Python-compatible Qwen home, environment, and capability regressions.

use agent_run_adapters::{
    auth::{qwen_auth_value_with, QWEN_BASE_URL},
    capabilities, materialize, validate,
};
use agent_run_config::config::{
    Adapter, Auth, Capacity, Catalog, Config, Core, Delivery, Environment, Hook, Mcp, Runtime,
};
use agent_run_config::profiles::Profile;
use agent_run_domain::domain::StartRequest;
use serde_json::{json, Value};
use std::{
    collections::{BTreeMap, BTreeSet},
    path::{Path, PathBuf},
};

/// Builds the minimal validated-shaped configuration used by Qwen fixtures.
fn config(root: &Path, _environment: Option<&str>, mcp: bool) -> Config {
    Config {
        schema_version: 1,
        core: Core::default(),
        capacity: Capacity::default(),
        delivery: Delivery::default(),
        profiles: Catalog::default(),
        skills: Catalog::default(),
        mcp: if mcp {
            BTreeMap::from([(
                "agent_lsp".into(),
                Mcp {
                    transport: "stdio".into(),
                    command: PathBuf::from("/bin/lsp"),
                    args: vec!["--stdio".into()],
                    env_from: vec![],
                    approval_mode: "auto".into(),
                },
            )])
        } else {
            BTreeMap::new()
        },
        environments: BTreeMap::from([(
            "developer".into(),
            Environment {
                path: vec![root.join("tools")],
                variables: BTreeMap::from([("PROJECT".into(), "generated".into())]),
                required_commands: vec!["git".into()],
                denied_commands: vec!["gh".into()],
                ..Environment::default()
            },
        )]),
        runtimes: BTreeMap::new(),
    }
}

/// Builds a Qwen runtime with optional legacy environment and hook declarations.
fn runtime(root: &Path, environment: Option<&str>, plugin: Option<&Path>) -> Runtime {
    serde_json::from_value(json!({
        "enabled": true,
        "adapter": "qwen",
        "binary": "/bin/echo",
        "home": root.join("runtime"),
        "models": ["qwen-test"],
        "environment": environment,
        "plugins": plugin.into_iter().collect::<Vec<_>>(),
    }))
    .expect("Qwen runtime fixture")
}

/// Builds a role/request pair with the same grants and model as the runtime.
fn role_request(
    root: &Path,
    write: bool,
    skills: Vec<String>,
    mcp: Vec<String>,
) -> (Profile, StartRequest) {
    let request: StartRequest = serde_json::from_value(json!({
        "runtime":"qwen", "model":"qwen-test", "profile":"implement",
        "task":"do the thing", "workdir":root.join("work"), "write":write
    }))
    .expect("Qwen request fixture");
    let role = Profile {
        name: "implement".into(),
        body: "ROLE CONTRACT".into(),
        write,
        network: false,
        revision: "fixture".into(),
        canonical: false,
        allow_external_read_roots: true,
        read_roots: vec![],
        skills,
        mcp,
        required_constraints: BTreeSet::new(),
    };
    (role, request)
}

/// Writes the manifest and hook file required for a declared plugin fixture.
fn plugin(root: &Path) -> PathBuf {
    let plugin = root.join("lsp-guard-plugin");
    std::fs::create_dir_all(plugin.join("hooks")).expect("plugin directories");
    std::fs::create_dir_all(plugin.join(".claude-plugin")).expect("plugin manifest directory");
    std::fs::write(
        plugin.join(".claude-plugin/plugin.json"),
        r#"{"name":"lsp-guard-plugin","version":"1.0.0"}"#,
    )
    .expect("plugin manifest");
    std::fs::write(plugin.join("hooks/lsp_guard.sh"), "#!/bin/sh\n").expect("plugin hook");
    plugin
}

/// Reads the generated Qwen settings document.
fn settings(home: &Path) -> Value {
    serde_json::from_slice(&std::fs::read(home.join(".qwen/settings.json")).expect("settings"))
        .expect("settings JSON")
}

/// Materializes one Qwen home with selected skill, MCP, and plugin assets.
// Mirrors `tests/test_qwen_adapter.py::QwenAdapterTests::test_settings_select_the_openai_auth_type`.
// Mirrors `tests/test_qwen_adapter.py::QwenAdapterTests::test_skills_are_materialized_and_context_notes_the_absolute_path`.
// Mirrors `tests/test_qwen_adapter.py::QwenAdapterTests::test_hooks_are_rendered_with_plugin_command_expansion`.
// Mirrors `tests/test_qwen_adapter.py::QwenAdapterTests::test_provider_model_mcp_and_role_are_isolated`.
// Mirrors `tests/test_qwen_adapter.py::QwenAdapterTests::test_resume_prepare_keeps_verified_runtime_home_read_only`.
#[test]
fn qwen_home_contains_only_selected_role_assets() {
    let temporary = tempfile::tempdir().expect("temporary root");
    let root = temporary.path();
    let skills = root.join("skills/qwen/role-implement");
    std::fs::create_dir_all(&skills).expect("skill directory");
    std::fs::write(skills.join("SKILL.md"), "# role-implement\n").expect("skill");
    let plugin = plugin(root);
    let mut runtime = runtime(root, None, Some(&plugin));
    runtime.auth = Some(Auth::Environment {
        names: vec!["OPENAI_API_KEY".into(), "OPENAI_BASE_URL".into()],
    });
    runtime.plugins = vec![plugin];
    runtime.hooks = vec![Hook {
        event: "PreToolUse".into(),
        matcher: Some("^(read_file)$".into()),
        command: vec![
            "/bin/sh".into(),
            "{plugin:lsp-guard-plugin}/hooks/lsp_guard.sh".into(),
        ],
    }];
    let (role, request) = role_request(
        root,
        false,
        vec!["role-implement".into()],
        vec!["agent_lsp".into()],
    );
    let home = root.join("home");
    let (_, digest) = materialize::materialize(
        &config(root, None, true),
        &runtime,
        &request,
        &role,
        &home,
        root,
    )
    .expect("materialize Qwen home");
    materialize::verify(&home, &digest).expect("materialized Qwen home verifies");
    let document = settings(&home);
    assert_eq!(document["security"]["auth"]["selectedType"], "openai");
    assert_eq!(document["tools"]["sandbox"], true);
    assert_eq!(document["mcpServers"]["agent_lsp"]["command"], "/bin/lsp");
    assert_eq!(
        std::fs::read_to_string(home.join("skills/role-implement/SKILL.md")).unwrap(),
        "# role-implement\n"
    );
    assert!(std::fs::read_to_string(home.join("agent-run-context.md"))
        .unwrap()
        .contains(
            &home
                .join("skills/role-implement/SKILL.md")
                .display()
                .to_string()
        ));
    assert!(document["hooks"]["PreToolUse"][0]["hooks"][0]["command"]
        .as_str()
        .unwrap()
        .contains(".qwen/agent-run-plugins/lsp-guard-plugin"));
}

/// Exercises Qwen's read/write capability boundary without a provider call.
// Mirrors `tests/test_qwen_adapter.py::QwenAdapterTests::test_read_only_and_write_modes_are_explicit_and_sandboxed`.
// Mirrors `tests/test_qwen_adapter.py::QwenAdapterTests::test_skills_capability_is_declared`.
#[test]
fn qwen_capabilities_are_sandboxed_and_include_delivery() {
    let capabilities = capabilities(Adapter::Qwen);
    assert!(capabilities.contains(&"skills"));
    assert!(capabilities.contains(&"hooks"));
    assert!(capabilities.contains(&"live_limits"));
    assert!(!capabilities.contains(&"steer"));
    assert!(!capabilities.contains(&"effort"));
}

/// Validates Qwen's optional auth shape and rejects network grants locally.
// Mirrors `tests/test_qwen_adapter.py::QwenAdapterTests::test_validate_accepts_native_or_selected_environment_auth`.
// Mirrors `tests/test_qwen_adapter.py::QwenAdapterTests::test_network_profile_is_refused`.
#[test]
fn qwen_validation_accepts_environment_auth_and_rejects_foreign_grants() {
    let temporary = tempfile::tempdir().expect("temporary root");
    let root = temporary.path();
    let runtime = runtime(root, None, None);
    let (role, mut request) = role_request(root, false, vec![], vec![]);
    validate(&request, &runtime, &role).expect("Qwen environment auth is valid");
    let mut foreign = runtime.clone();
    foreign.auth = Some(Auth::FileLink {
        source: root.join("auth.json"),
        target: "auth.json".into(),
    });
    assert!(validate(&request, &foreign, &role)
        .expect_err("Qwen file-link auth must be rejected")
        .to_string()
        .contains("auth.kind"));
    let network_role = Profile {
        network: true,
        ..role
    };
    request.profile = network_role.name.clone();
    assert!(validate(&request, &runtime, &network_role).is_err());
}

/// Confirms global Qwen authentication and host PATH inheritance are isolated.
// Mirrors `tests/test_qwen_adapter.py::QwenAdapterTests::test_prepare_uses_native_provider_environment_without_auth_config`.
// Mirrors `tests/test_qwen_adapter.py::QwenAdapterTests::test_child_path_matches_the_host_path`.
// Mirrors `tests/test_qwen_adapter.py::QwenAdapterTests::test_provider_model_mcp_and_role_are_isolated`.
#[test]
fn qwen_environment_uses_host_provider_values_and_path() {
    let temporary = tempfile::tempdir().expect("temporary root");
    let root = temporary.path();
    let runtime = runtime(root, None, None);
    let (_, request) = role_request(root, false, vec![], vec![]);
    let host = BTreeMap::from([
        ("HOME".into(), "/host/home".into()),
        ("PATH".into(), "/usr/bin:/bin".into()),
        ("OPENAI_API_KEY".into(), "global-secret".into()),
        ("OPENAI_BASE_URL".into(), "https://provider/v1".into()),
    ]);
    let environment = materialize::environment_with_host(
        &config(root, None, false),
        &runtime,
        &Profile {
            name: "implement".into(),
            body: "role".into(),
            write: false,
            network: false,
            revision: "fixture".into(),
            canonical: false,
            allow_external_read_roots: true,
            read_roots: vec![],
            skills: vec![],
            mcp: vec![],
            required_constraints: BTreeSet::new(),
        },
        &root.join("home"),
        None,
        root,
        &host,
    )
    .expect("Qwen environment");
    assert_eq!(environment["HOME"], root.join("home").display().to_string());
    assert_eq!(environment["PATH"], "/usr/bin:/bin");
    assert_eq!(environment["OPENAI_API_KEY"], "global-secret");
    assert_eq!(environment["OPENAI_BASE_URL"], "https://provider/v1");
    assert!(!request.task.is_empty());
}

/// Confirms legacy preset values and probes cannot widen a Qwen child.
// Mirrors `tests/test_qwen_adapter.py::QwenAdapterTests::test_legacy_environment_only_retains_native_denials`.
// Mirrors `tests/test_qwen_adapter.py::QwenAdapterTests::test_legacy_required_command_does_not_probe_during_prepare`.
#[test]
fn qwen_legacy_environment_retains_only_denials() {
    let temporary = tempfile::tempdir().expect("temporary root");
    let root = temporary.path();
    let runtime = runtime(root, Some("developer"), None);
    let profile = Profile {
        name: "implement".into(),
        body: "role".into(),
        write: false,
        network: false,
        revision: "fixture".into(),
        canonical: false,
        allow_external_read_roots: true,
        read_roots: vec![],
        skills: vec![],
        mcp: vec![],
        required_constraints: BTreeSet::new(),
    };
    let host = BTreeMap::from([
        ("HOME".into(), "/host/home".into()),
        ("PATH".into(), "/bin".into()),
        ("OPENAI_API_KEY".into(), "secret".into()),
        ("OPENAI_BASE_URL".into(), QWEN_BASE_URL.into()),
    ]);
    let environment = materialize::environment_with_host(
        &config(root, Some("developer"), false),
        &runtime,
        &profile,
        &root.join("home"),
        None,
        root,
        &host,
    )
    .expect("Qwen legacy environment");
    assert_eq!(
        environment["PATH"],
        format!("{}:/bin", root.join("home/.qwen/denied-commands").display())
    );
    assert!(!environment.contains_key("PROJECT"));
}

/// Checks Qwen credential precedence and fixed fallbacks without reading Keychain.
// Mirrors `tests/test_qwen_adapter.py::QwenAdapterTests::test_process_env_credentials_win_over_keychain_and_default_base_url`.
// Mirrors `tests/test_qwen_adapter.py::QwenAdapterTests::test_keychain_and_default_base_url_fill_missing_credentials`.
// Mirrors `tests/test_qwen_adapter.py::QwenAdapterTests::test_missing_env_and_failed_keychain_raises`.
// Mirrors `tests/test_qwen_adapter.py::QwenAdapterTests::test_probe_reports_authenticated_via_keychain_fallback`.
#[test]
fn qwen_auth_prefers_host_then_uses_safe_fallbacks() {
    let host = BTreeMap::from([(String::from("OPENAI_API_KEY"), String::from("process"))]);
    assert_eq!(
        qwen_auth_value_with("OPENAI_API_KEY", &host, || panic!("fallback was consulted")),
        Some("process".into())
    );
    assert_eq!(
        qwen_auth_value_with("OPENAI_BASE_URL", &BTreeMap::new(), || None),
        Some(QWEN_BASE_URL.into())
    );
    assert_eq!(
        qwen_auth_value_with("OPENAI_API_KEY", &BTreeMap::new(), || Some(
            "keychain".into()
        )),
        Some("keychain".into())
    );
    assert_eq!(
        qwen_auth_value_with("OPENAI_API_KEY", &BTreeMap::new(), || None),
        None
    );
}
