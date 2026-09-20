//! Ports for plugin skill ownership, Codex hook trust, and MCP configuration.

use agent_run_adapters::{materialize, plugins};
use agent_run_config::{
    config::{Config, Hook, Mcp, Runtime},
    profiles::Profile,
};
use agent_run_domain::domain::StartRequest;
use serde_json::{json, Value};
use std::{
    collections::{BTreeMap, BTreeSet},
    path::{Path, PathBuf},
};

/// Creates a plugin manifest, selected skills, and an optional Codex hook.
fn plugin(root: &Path, name: &str, skills: &[&str], with_hook: bool) -> PathBuf {
    let directory = root.join(name);
    std::fs::create_dir_all(directory.join(".claude-plugin")).unwrap();
    std::fs::write(
        directory.join(".claude-plugin/plugin.json"),
        json!({"name":name,"version":"1.0.0"}).to_string(),
    )
    .unwrap();
    for skill in skills {
        std::fs::create_dir_all(directory.join("skills").join(skill)).unwrap();
        std::fs::write(
            directory.join("skills").join(skill).join("SKILL.md"),
            format!("fresh {skill}"),
        )
        .unwrap();
    }
    if with_hook {
        std::fs::create_dir_all(directory.join("hooks")).unwrap();
        std::fs::write(directory.join("hooks/lsp_guard.py"), "print('guard')\n").unwrap();
        std::fs::write(
            directory.join("hooks/hooks.json"),
            json!({"hooks":{"PreToolUse":[{"matcher":"Edit|Write","hooks":[{
                "type":"command","command":"python3 ${CLAUDE_PLUGIN_ROOT}/hooks/lsp_guard.py","timeout":5
            }]}]}})
            .to_string(),
        )
        .unwrap();
    }
    directory
}

/// Builds the smallest runtime accepted by the adapter materializer.
fn runtime(
    adapter: &str,
    home: &Path,
    plugins: Vec<PathBuf>,
    skills: Vec<String>,
    mcp: Vec<String>,
    hooks: Vec<Hook>,
) -> Runtime {
    let auth = home.parent().unwrap().join("auth.json");
    std::fs::write(&auth, "{}").unwrap();
    serde_json::from_value(json!({
        "enabled": true, "adapter": adapter, "binary": "/bin/echo", "home": home,
        "models": ["fixture"], "skills": skills, "mcp": mcp, "plugins": plugins,
        "hooks": hooks, "auth": {"kind":"file_link","source":auth,"target":"auth.json"}
    }))
    .unwrap()
}

/// Creates a request and matching legacy profile for one generated home.
fn request_profile(
    runtime: &str,
    workdir: &Path,
    skills: Vec<String>,
    mcp: Vec<String>,
) -> (StartRequest, Profile) {
    let request: StartRequest = serde_json::from_value(json!({
        "runtime":runtime,"model":"fixture","profile":"review","task":"fixture",
        "workdir":workdir,"write":false
    }))
    .unwrap();
    let profile = Profile {
        name: "review".into(),
        body: "Review the fixture.".into(),
        write: false,
        network: false,
        revision: "fixture".into(),
        canonical: false,
        allow_external_read_roots: true,
        read_roots: vec![],
        skills,
        mcp,
        required_constraints: BTreeSet::new(),
    };
    (request, profile)
}

/// Builds a config containing one runtime and the supplied MCP definitions.
fn config(runtime: &Runtime, mcp: BTreeMap<String, Mcp>) -> Config {
    Config {
        schema_version: 1,
        core: Default::default(),
        capacity: Default::default(),
        delivery: Default::default(),
        profiles: Default::default(),
        skills: Default::default(),
        mcp,
        environments: BTreeMap::new(),
        runtimes: BTreeMap::from([(runtime.adapter.clone(), runtime.clone())]),
    }
}

/// Returns a stdio MCP definition with the requested arguments.
fn mcp(args: &[&str]) -> Mcp {
    Mcp {
        transport: "stdio".into(),
        command: PathBuf::from("/bin/echo"),
        args: args.iter().map(|arg| (*arg).into()).collect(),
        env_from: vec![],
        approval_mode: "auto".into(),
    }
}

/// Mirrors `tests/test_plugin_integration.py::PluginSkillSourceTests::test_plugin_wins_and_unowned_names_fall_back`.
#[test]
fn plugin_skill_sources_prefer_declared_plugins_and_preserve_order() {
    let root = tempfile::tempdir().unwrap();
    let selected = plugin(
        root.path(),
        "demo-plugin",
        &["lsp-first", "code-reading"],
        false,
    );
    let names = vec!["delegate".into(), "lsp-first".into(), "code-reading".into()];
    let resolved = plugins::skill_dirs(&[selected], &root.path().join("stale"), &names).unwrap();
    assert_eq!(
        resolved
            .iter()
            .map(|(name, _)| name.as_str())
            .collect::<Vec<_>>(),
        ["delegate", "lsp-first", "code-reading"]
    );
    assert!(resolved[1].1.ends_with("skills/lsp-first"));
    assert!(resolved[0].1.ends_with("stale/delegate"));
}

/// Mirrors `tests/test_plugin_integration.py::PluginSkillSourceTests::test_local_names_exclude_plugin_owned_skills`.
#[test]
fn local_skill_names_exclude_plugin_owned_names() {
    let root = tempfile::tempdir().unwrap();
    let selected = plugin(root.path(), "demo-plugin", &["lsp-first"], false);
    let names = vec!["delegate".into(), "lsp-first".into()];
    assert_eq!(
        plugins::local_skill_names(&[selected], &names).unwrap(),
        ["delegate"]
    );
    assert_eq!(plugins::local_skill_names(&[], &names).unwrap(), names);
}

/// Mirrors `tests/test_plugin_integration.py::PluginSkillSourceTests::test_two_plugins_claiming_one_name_fail_closed`.
#[test]
fn duplicate_plugin_skill_owners_fail_closed() {
    let root = tempfile::tempdir().unwrap();
    let first = plugin(root.path(), "first", &["shared"], false);
    let second = plugin(root.path(), "second", &["shared"], false);
    let error = plugins::plugin_skill_dir(&[first, second], "shared").unwrap_err();
    assert!(error.to_string().contains("two declared plugins"));
}

/// Mirrors `tests/test_plugin_integration.py::PluginSkillSourceTests::test_claude_does_not_project_a_plugin_owned_skill`.
#[test]
fn claude_does_not_project_plugin_owned_skills() {
    let root = tempfile::tempdir().unwrap();
    let plugin_path = plugin(
        root.path(),
        "demo-plugin",
        &["lsp-first", "code-reading"],
        false,
    );
    let skills = root.path().join("skills/claude/delegate");
    std::fs::create_dir_all(&skills).unwrap();
    std::fs::write(skills.join("SKILL.md"), "stale delegate").unwrap();
    let home = root.path().join("claude-home");
    let runtime = runtime(
        "claude",
        &home,
        vec![plugin_path],
        ["delegate", "lsp-first", "code-reading"]
            .into_iter()
            .map(str::to_owned)
            .collect(),
        vec![],
        vec![],
    );
    let config = config(&runtime, BTreeMap::new());
    let (request, profile) = request_profile("claude", root.path(), runtime.skills.clone(), vec![]);
    materialize::materialize(&config, &runtime, &request, &profile, &home, root.path()).unwrap();
    assert!(home.join("plugins/delegate").is_dir());
    assert!(!home.join("plugins/lsp-first").exists());
    assert!(!home.join("plugins/code-reading").exists());
}

/// Mirrors `tests/test_plugin_integration.py::PluginSkillSourceTests::test_unlisted_plugin_skills_are_reported_for_the_wholesale_host`.
#[test]
fn unlisted_plugin_skills_are_reported() {
    let root = tempfile::tempdir().unwrap();
    let path = plugin(
        root.path(),
        "routing-plugin",
        &["role-review", "delegate"],
        false,
    );
    assert_eq!(
        plugins::unlisted_plugin_skills(&[path], &["role-review".into(), "lsp-first".into()]),
        ["delegate"]
    );
}

/// Mirrors `tests/test_plugin_integration.py::PluginSkillSourceTests::test_claude_refuses_a_plugin_shipping_an_unlisted_skill`.
#[test]
fn claude_rejects_unlisted_wholesale_plugin_skills() {
    let root = tempfile::tempdir().unwrap();
    let path = plugin(
        root.path(),
        "routing-plugin",
        &["role-review", "delegate"],
        false,
    );
    let runtime = runtime(
        "claude",
        &root.path().join("home"),
        vec![path],
        vec!["role-review".into()],
        vec![],
        vec![],
    );
    let error = agent_run_adapters::claude::validate_runtime(
        &runtime,
        agent_run_config::config::Adapter::Claude,
    )
    .unwrap_err();
    assert!(error.to_string().contains("unlisted: delegate"));
}

/// Returns a Codex runtime fixture with a declared plugin hook.
fn codex_hook_fixture(
    root: &Path,
    command: &str,
) -> (Config, Runtime, StartRequest, Profile, PathBuf) {
    let plugin_path = plugin(root, "demo-plugin", &["lsp-first"], true);
    let home = root.join("codex-home");
    let hook = Hook {
        event: "PreToolUse".into(),
        command: vec![
            "/usr/bin/python3".into(),
            "{plugin:demo-plugin}/hooks/lsp_guard.py".into(),
            command.into(),
        ],
        matcher: Some("^(apply_patch|mcp__agent_lsp__.*)$".into()),
    };
    let runtime = runtime(
        "codex",
        &home,
        vec![plugin_path],
        vec![],
        vec![],
        vec![hook],
    );
    let config = config(&runtime, BTreeMap::new());
    let (request, profile) = request_profile("codex", root, vec![], vec![]);
    (config, runtime, request, profile, home)
}

/// Mirrors `tests/test_plugin_integration.py::CodexGuardHookTests::test_hook_groups_carry_a_trust_entry_keyed_by_the_config_path`.
#[test]
fn codex_hook_groups_include_config_path_trust_entries() {
    let root = tempfile::tempdir().unwrap();
    let (config, runtime, request, profile, home) =
        codex_hook_fixture(root.path(), "--mode=strict");
    materialize::materialize(&config, &runtime, &request, &profile, &home, root.path()).unwrap();
    let document: toml::Value =
        toml::from_str(&std::fs::read_to_string(home.join("config.toml")).unwrap()).unwrap();
    let hooks = document.get("hooks").unwrap();
    assert_eq!(
        hooks["PreToolUse"][0]["hooks"][0]["timeout"].as_integer(),
        Some(600)
    );
    assert!(hooks["state"]
        .as_table()
        .unwrap()
        .keys()
        .any(|key| key.contains(":pre_tool_use:0:0")));
    assert!(hooks["state"]
        .as_table()
        .unwrap()
        .keys()
        .any(|key| key.contains("demo-plugin@personal:hooks/hooks.json:pre_tool_use:0:0")));
}

/// Accepts a safe manifest-selected hook file and keys trust to that exact path.
#[test]
fn codex_plugin_manifest_selects_relative_hook_file() {
    let root = tempfile::tempdir().unwrap();
    let plugin_path = plugin(root.path(), "custom-hooks", &[], false);
    std::fs::write(
        plugin_path.join(".claude-plugin/plugin.json"),
        json!({"name":"custom-hooks","version":"1.0.0","hooks":"config/custom.json"}).to_string(),
    )
    .unwrap();
    std::fs::create_dir_all(plugin_path.join("config")).unwrap();
    std::fs::write(
        plugin_path.join("config/custom.json"),
        json!({"hooks":{"PreToolUse":[{"hooks":[{
            "type":"command","command":"true"
        }]}]}})
        .to_string(),
    )
    .unwrap();
    let home = root.path().join("home");
    let runtime = runtime("codex", &home, vec![plugin_path], vec![], vec![], vec![]);
    let config = config(&runtime, BTreeMap::new());
    let (request, profile) = request_profile("codex", root.path(), vec![], vec![]);

    materialize::materialize(&config, &runtime, &request, &profile, &home, root.path()).unwrap();

    let document: toml::Value =
        toml::from_str(&std::fs::read_to_string(home.join("config.toml")).unwrap()).unwrap();
    assert!(document["hooks"]["state"]
        .as_table()
        .unwrap()
        .keys()
        .any(|key| key.contains("custom-hooks@personal:config/custom.json:pre_tool_use:0:0")));
}

/// Rejects non-string, blank, absolute, and traversing manifest hook paths.
#[test]
fn codex_plugin_manifest_rejects_unsafe_hook_paths() {
    for (index, hooks) in [
        json!(false),
        json!(""),
        json!("/tmp/hooks.json"),
        json!("../hooks.json"),
        json!("hooks/../hooks.json"),
    ]
    .into_iter()
    .enumerate()
    {
        let root = tempfile::tempdir().unwrap();
        let plugin_path = plugin(root.path(), &format!("unsafe-{index}"), &[], false);
        std::fs::write(
            plugin_path.join(".claude-plugin/plugin.json"),
            json!({"name":format!("unsafe-{index}"),"version":"1.0.0","hooks":hooks}).to_string(),
        )
        .unwrap();
        let home = root.path().join("home");
        let runtime = runtime("codex", &home, vec![plugin_path], vec![], vec![], vec![]);
        let config = config(&runtime, BTreeMap::new());
        let (request, profile) = request_profile("codex", root.path(), vec![], vec![]);
        assert!(
            materialize::materialize(&config, &runtime, &request, &profile, &home, root.path())
                .is_err(),
            "unsafe hooks value {hooks} was accepted"
        );
    }
}

/// Mirrors `tests/test_plugin_integration.py::CodexGuardHookTests::test_guard_command_resolves_to_the_copy_inside_the_home`.
#[test]
fn codex_plugin_hook_command_uses_the_installed_copy() {
    let root = tempfile::tempdir().unwrap();
    let (config, runtime, request, profile, home) =
        codex_hook_fixture(root.path(), "--mode=strict");
    materialize::materialize(&config, &runtime, &request, &profile, &home, root.path()).unwrap();
    let text = std::fs::read_to_string(home.join("config.toml")).unwrap();
    let installed = home.join("plugins/cache/personal/demo-plugin/1.0.0/hooks/lsp_guard.py");
    assert!(text.contains(&installed.to_string_lossy().to_string()));
    assert!(installed.is_file() && !installed.is_symlink());
}

/// Mirrors `tests/test_plugin_integration.py::CodexGuardHookTests::test_trusted_hash_tracks_the_command`.
#[test]
fn codex_hook_trust_hash_tracks_the_rendered_command() {
    let root = tempfile::tempdir().unwrap();
    let (config, runtime, request, profile, home) =
        codex_hook_fixture(root.path(), "--mode=strict");
    materialize::materialize(&config, &runtime, &request, &profile, &home, root.path()).unwrap();
    let before: toml::Value =
        toml::from_str(&std::fs::read_to_string(home.join("config.toml")).unwrap()).unwrap();
    let (config, runtime, request, profile, _) = codex_hook_fixture(root.path(), "--mode=nudge");
    materialize::materialize(&config, &runtime, &request, &profile, &home, root.path()).unwrap();
    let after: toml::Value =
        toml::from_str(&std::fs::read_to_string(home.join("config.toml")).unwrap()).unwrap();
    let key = format!("{}:pre_tool_use:0:0", home.join("config.toml").display());
    assert_ne!(
        before["hooks"]["state"][&key],
        after["hooks"]["state"][&key]
    );
}

/// Mirrors `tests/test_plugin_integration.py::CodexGuardHookTests::test_unknown_plugin_reference_fails_closed`.
#[test]
fn codex_unknown_plugin_hook_reference_fails_closed() {
    let root = tempfile::tempdir().unwrap();
    let home = root.path().join("home");
    let runtime = runtime(
        "codex",
        &home,
        vec![],
        vec![],
        vec![],
        vec![Hook {
            event: "PreToolUse".into(),
            command: vec!["{plugin:absent}".into()],
            matcher: None,
        }],
    );
    let config = config(&runtime, BTreeMap::new());
    let (request, profile) = request_profile("codex", root.path(), vec![], vec![]);
    let error = materialize::materialize(&config, &runtime, &request, &profile, &home, root.path())
        .unwrap_err();
    assert!(error.to_string().contains("undeclared plugin"));
}

/// Mirrors `tests/test_plugin_integration.py::CodexGuardHookTests::test_unsupported_hook_event_fails_closed`.
#[test]
fn codex_unsupported_hook_event_fails_closed() {
    let root = tempfile::tempdir().unwrap();
    let home = root.path().join("home");
    let runtime = runtime(
        "codex",
        &home,
        vec![],
        vec![],
        vec![],
        vec![Hook {
            event: "NotAnEvent".into(),
            command: vec!["true".into()],
            matcher: None,
        }],
    );
    let config = config(&runtime, BTreeMap::new());
    let (request, profile) = request_profile("codex", root.path(), vec![], vec![]);
    assert!(
        materialize::materialize(&config, &runtime, &request, &profile, &home, root.path())
            .is_err()
    );
}

/// Mirrors `tests/test_plugin_integration.py::ExtraMcpServerTests::test_codex_config_lists_both_servers`.
#[test]
fn codex_config_lists_both_declared_mcp_servers() {
    let root = tempfile::tempdir().unwrap();
    let home = root.path().join("codex");
    let servers = BTreeMap::from([
        ("agent_lsp".into(), mcp(&["lsp"])),
        ("codegraph".into(), mcp(&["serve", "--mcp"])),
    ]);
    let runtime = runtime(
        "codex",
        &home,
        vec![],
        vec![],
        vec!["agent_lsp".into(), "codegraph".into()],
        vec![],
    );
    let config = config(&runtime, servers);
    let (request, profile) = request_profile(
        "codex",
        root.path(),
        vec![],
        vec!["agent_lsp".into(), "codegraph".into()],
    );
    materialize::materialize(&config, &runtime, &request, &profile, &home, root.path()).unwrap();
    let document: toml::Value =
        toml::from_str(&std::fs::read_to_string(home.join("config.toml")).unwrap()).unwrap();
    assert_eq!(
        document["mcp_servers"]
            .as_table()
            .unwrap()
            .keys()
            .collect::<Vec<_>>(),
        [&"agent_lsp".to_string(), &"codegraph".to_string()]
    );
    assert_eq!(
        document["mcp_servers"]["codegraph"]["args"]
            .as_array()
            .unwrap(),
        &[
            toml::Value::String("serve".into()),
            toml::Value::String("--mcp".into())
        ]
    );
}

/// Mirrors `tests/test_plugin_integration.py::ExtraMcpServerTests::test_claude_mcp_config_lists_both_servers`.
#[test]
fn claude_config_lists_both_declared_mcp_servers() {
    let root = tempfile::tempdir().unwrap();
    let home = root.path().join("claude");
    let servers = BTreeMap::from([
        ("agent_lsp".into(), mcp(&["lsp"])),
        ("codegraph".into(), mcp(&["serve", "--mcp"])),
    ]);
    let runtime = runtime(
        "claude",
        &home,
        vec![],
        vec![],
        vec!["agent_lsp".into(), "codegraph".into()],
        vec![],
    );
    let config = config(&runtime, servers);
    let (request, profile) = request_profile(
        "claude",
        root.path(),
        vec![],
        vec!["agent_lsp".into(), "codegraph".into()],
    );
    materialize::materialize(&config, &runtime, &request, &profile, &home, root.path()).unwrap();
    let document: Value =
        serde_json::from_str(&std::fs::read_to_string(home.join("mcp/mcp-config.json")).unwrap())
            .unwrap();
    assert_eq!(document["mcpServers"].as_object().unwrap().len(), 2);
    assert_eq!(
        document["mcpServers"]["codegraph"]["args"],
        json!(["serve", "--mcp"])
    );
}
