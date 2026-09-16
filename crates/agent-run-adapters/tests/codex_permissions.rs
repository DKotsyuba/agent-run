//! Python-compatible Codex Projects, permission-hook, and command-policy tests.

use agent_run_adapters::{
    codex::{
        allows_permission_request, permission_request_decision, permission_request_hook,
        render_denial_rules, render_review_rules,
    },
    materialize,
};
use agent_run_config::{
    config::{Adapter, Capacity, Catalog, Config, Core, Delivery, Mcp, Runtime},
    profiles::Profile,
};
use agent_run_domain::domain::StartRequest;
use serde_json::{json, Value};
use std::{
    collections::{BTreeMap, BTreeSet},
    path::Path,
};

/// Creates a unique test-owned directory below the platform temporary root.
fn temporary_directory() -> std::path::PathBuf {
    let path = std::env::temp_dir().join(format!("agent-run-codex-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir(&path).expect("temporary directory");
    path
}

/// Builds a minimal Codex runtime that links only a test-owned auth file.
fn codex_runtime(root: &Path) -> Runtime {
    serde_json::from_value(json!({
        "enabled": true,
        "adapter": "codex",
        "binary": "/bin/true",
        "home": root.join("runtime"),
        "models": ["fixture"],
        "auth": {"kind":"file_link", "source":root.join("codex-auth.json"), "target":"auth.json"},
    }))
    .expect("fixture runtime")
}

/// Builds the materializer inputs shared by the checked-in home fixture shapes.
fn fixture(root: &Path, write: bool) -> (Config, Runtime, StartRequest, Profile) {
    std::fs::write(root.join("codex-auth.json"), "{}\n").expect("fixture auth");
    let runtime = codex_runtime(root);
    let config = Config {
        schema_version: 1,
        core: Core::default(),
        capacity: Capacity::default(),
        delivery: Delivery::default(),
        profiles: Catalog::default(),
        skills: Catalog::default(),
        mcp: BTreeMap::new(),
        environments: BTreeMap::new(),
        runtimes: BTreeMap::new(),
    };
    let request: StartRequest = serde_json::from_value(json!({
        "runtime": "codex", "model": "fixture", "profile": if write {"implement"} else {"review"},
        "task": "fixture", "workdir": root.join("workdir"), "write": write,
    }))
    .expect("fixture request");
    std::fs::create_dir_all(&request.workdir).expect("fixture workdir");
    let profile = Profile {
        name: request.profile.clone(),
        body: "Fixture role.".into(),
        write,
        network: false,
        revision: "fixture".into(),
        canonical: false,
        allow_external_read_roots: true,
        read_roots: vec![],
        skills: vec![],
        mcp: vec![],
        required_constraints: BTreeSet::new(),
    };
    (config, runtime, request, profile)
}

/// Mirrors `test_codex_permission_request.py::test_allows_hyphen_and_underscore_namespace_variants`.
#[test]
fn python_test_codex_permission_request_allows_only_trusted_mcp_namespaces() {
    let trusted = BTreeSet::from(["agent-run".into(), "agent-ide".into()]);
    for tool_name in [
        "mcp__agent-run__start",
        "mcp__agent_run__status",
        "mcp__agent-ide__context",
        "mcp__agent_ide__diff",
    ] {
        assert!(allows_permission_request(
            &json!({"hook_event_name":"PermissionRequest", "tool_name":tool_name}),
            &trusted,
        ));
    }
    for payload in [
        json!({"hook_event_name":"PermissionRequest", "tool_name":"Bash"}),
        json!({"hook_event_name":"PermissionRequest", "tool_name":"mcp__github__push"}),
        json!({"hook_event_name":"PreToolUse", "tool_name":"mcp__agent-run__start"}),
        json!({"hook_event_name":"PermissionRequest"}),
        Value::Array(vec![]),
    ] {
        assert!(!allows_permission_request(&payload, &trusted));
    }
    assert_eq!(
        permission_request_decision(
            &json!({"hook_event_name":"PermissionRequest", "tool_name":"mcp__agent_run__answer"}),
            &BTreeSet::from(["agent-run".into()]),
        ),
        Some(
            json!({"hookSpecificOutput":{"hookEventName":"PermissionRequest","decision":{"behavior":"allow"}}})
        )
    );
    let (matcher, command) = permission_request_hook(
        &BTreeSet::from(["agent-run".into()]),
        Path::new("/usr/local/bin/agent-run"),
    )
    .expect("valid hook")
    .expect("trusted hook");
    assert!(matcher.contains("mcp__agent-run__"));
    assert!(matcher.contains("mcp__agent_run__"));
    assert_eq!(
        command,
        vec![
            "/usr/local/bin/agent-run",
            "_permission-request",
            "--allow-mcp",
            "agent-run",
        ]
    );
}

/// Mirrors `test_command_policy.py::test_native_rendering_is_deterministic_and_exact`.
#[test]
fn python_test_command_policy_codex_rules_cover_bare_and_absolute_commands() {
    let root = temporary_directory();
    let bin = root.join("bin");
    std::fs::create_dir(&bin).expect("bin directory");
    let tool = bin.join("gh");
    std::fs::write(&tool, "#!/bin/sh\nexit 0\n").expect("fixture tool");
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&tool, std::fs::Permissions::from_mode(0o700))
            .expect("fixture tool permissions");
    }
    let rules = render_denial_rules(
        &["hub".into(), "gh".into(), "gh".into()],
        &bin.display().to_string(),
    );
    assert!(rules.contains("pattern=[\"gh\"]"));
    assert!(rules.contains(&format!("pattern=[\"{}\"]", tool.display())));
    assert!(rules.contains("decision=\"forbidden\""));
    let review = render_review_rules(&["gh".into()], &bin.display().to_string());
    assert!(review.contains("decision=\"prompt\""));
    assert!(review.contains("Review network command before execution"));
}

/// Mirrors the read-only/write generated-home captures in `tests/fixtures/baseline/homes/codex`.
#[test]
fn python_golden_codex_home_config_matches_read_only_and_write_captures() {
    for (name, write) in [("read-only", false), ("write", true)] {
        let root = temporary_directory();
        let (config, runtime, request, profile) = fixture(&root, write);
        let home = root.join("home");
        materialize::materialize(&config, &runtime, &request, &profile, &home, &root)
            .expect("materialize Codex home");
        let expected: Value = serde_json::from_slice(
            &std::fs::read(
                Path::new(env!("CARGO_MANIFEST_DIR"))
                    .join("../../tests/fixtures/baseline/homes/codex")
                    .join(name)
                    .join("tree.json"),
            )
            .expect("golden tree"),
        )
        .expect("golden JSON");
        let expected_config = expected["config.toml"]["content"]
            .as_str()
            .expect("golden config")
            .replace(
                "${TEMP_ROOT}/workdir",
                &request.workdir.display().to_string(),
            );
        assert_eq!(
            std::fs::read_to_string(home.join("config.toml")).expect("generated config"),
            expected_config,
        );
        assert_eq!(
            std::fs::read_to_string(home.join("command-refusals/.agent-run-command-policy.json"))
                .expect("policy marker"),
            "{\"version\": 1, \"commands\": []}\n",
        );
        assert_eq!(
            std::fs::read_to_string(home.join("rules/agent-run-command-policy.rules"))
                .expect("empty rules"),
            "",
        );
    }
}

/// Mirrors `test_native_settings.py::CodexNativeSettingsMaterialize::test_nested_tables_render_as_valid_inline_toml`.
#[test]
fn python_test_native_settings_nested_tables_stay_inline_before_owned_sections() {
    let root = temporary_directory();
    let (config, mut runtime, request, profile) = fixture(&root, false);
    runtime.native_settings.insert(
        "outer".into(),
        toml::Value::Table(toml::map::Map::from_iter([(
            "inner".into(),
            toml::Value::Table(toml::map::Map::from_iter([(
                "deep".into(),
                toml::Value::Array(vec![toml::Value::Integer(1), toml::Value::Integer(2)]),
            )])),
        )])),
    );
    let home = root.join("home");
    materialize::materialize(&config, &runtime, &request, &profile, &home, &root)
        .expect("materialize Codex home");
    let generated = std::fs::read_to_string(home.join("config.toml")).expect("generated config");
    assert!(generated.contains("outer = { inner = { deep = [1, 2] } }"));
    let document: toml::Value = toml::from_str(&generated).expect("valid TOML");
    assert_eq!(
        document
            .get("outer")
            .and_then(|value| value.get("inner"))
            .and_then(|value| value.get("deep"))
            .and_then(toml::Value::as_array)
            .expect("nested native tuning")
            .len(),
        2,
    );
    assert!(document.get("projects").is_some());
}

/// Mirrors `test_codex_adapter.py::test_managed_projects_uses_one_definition_and_verifies_all_write_roots` refusal behavior.
#[test]
fn python_test_codex_adapter_projects_refuses_an_unproven_managed_root() {
    let root = temporary_directory();
    let (config, mut runtime, request, profile) = fixture(&root, true);
    runtime.workspace_root = Some(root.join("projects"));
    std::fs::create_dir_all(runtime.workspace_root.as_ref().expect("project root"))
        .expect("project root");
    let home = root.join("home");
    assert!(materialize::materialize(&config, &runtime, &request, &profile, &home, &root).is_err());
    assert_eq!(runtime.kind().expect("Codex kind"), Adapter::Codex);
}

/// Mirrors `test_codex_adapter.py::test_materialize_approves_only_configured_mcp_and_adds_narrow_hook`.
#[test]
fn python_test_codex_adapter_materializes_only_declared_mcp_approval() {
    let root = temporary_directory();
    let (mut config, runtime, request, mut profile) = fixture(&root, false);
    config.mcp.insert(
        "approved".into(),
        Mcp {
            transport: "stdio".into(),
            command: Path::new("/bin/true").into(),
            args: vec!["--fixture".into()],
            env_from: vec!["PATH".into()],
            approval_mode: "approve".into(),
        },
    );
    profile.mcp = vec!["approved".into()];
    let home = root.join("home");

    materialize::materialize(&config, &runtime, &request, &profile, &home, &root)
        .expect("materialize Codex home");

    let config: toml::Value = toml::from_str(
        &std::fs::read_to_string(home.join("config.toml")).expect("generated config"),
    )
    .expect("valid generated TOML");
    assert_eq!(
        config
            .get("mcp_servers")
            .and_then(|servers| servers.get("approved"))
            .and_then(|server| server.get("default_tools_approval_mode"))
            .and_then(toml::Value::as_str),
        Some("approve")
    );
    let hooks = config
        .get("hooks")
        .and_then(|hooks| hooks.get("PermissionRequest"))
        .and_then(toml::Value::as_array)
        .expect("narrow permission hook");
    assert_eq!(hooks.len(), 1);
    assert!(hooks[0]
        .get("matcher")
        .and_then(toml::Value::as_str)
        .is_some_and(|matcher| matcher.contains("mcp__approved__")));
}

/// Mirrors `test_codex_adapter.py::test_materialize_uses_only_each_explicit_service_skill_root`.
/// Mirrors `test_codex_adapter.py::test_materialize_fails_for_missing_skill_or_unresolved_mcp`.
#[test]
fn python_test_codex_adapter_materializes_only_declared_service_skills() {
    let root = temporary_directory();
    let (config, runtime, request, mut profile) = fixture(&root, false);
    let skill = root.join("skills/codex/declared");
    std::fs::create_dir_all(&skill).expect("skill root");
    std::fs::write(skill.join("SKILL.md"), "declared skill\n").expect("skill body");
    profile.skills = vec!["declared".into()];
    let home = root.join("home");

    materialize::materialize(&config, &runtime, &request, &profile, &home, &root)
        .expect("declared skill materializes");
    assert_eq!(
        std::fs::read_to_string(home.join("skills/declared/SKILL.md")).expect("copied skill"),
        "declared skill\n"
    );

    profile.skills = vec!["missing".into()];
    assert!(materialize::materialize(
        &config,
        &runtime,
        &request,
        &profile,
        &root.join("missing-home"),
        &root,
    )
    .is_err());
}
