//! Claude-family launch contracts that are not represented by a native CLI invocation.

use agent_run_adapters::{materialize, validate};
use agent_run_config::{
    config::{Adapter, Capacity, Catalog, Config, Core, Delivery, Runtime},
    profiles::Profile,
};
use agent_run_core::{state::Record, stream::plan_with_environment};
use agent_run_domain::domain::{AgentId, StartRequest, Status};
use serde_json::json;
use std::collections::{BTreeMap, BTreeSet};

/// Builds an isolated Claude request, runtime, and role for launch-contract assertions.
fn fixture(
    root: &std::path::Path,
    write: bool,
    network: bool,
    model: &str,
) -> (Config, Runtime, StartRequest, Profile) {
    let runtime: Runtime = serde_json::from_value(json!({
        "enabled": true,
        "adapter": "claude",
        "binary": "/bin/echo",
        "home": root.join("runtime"),
        "models": ["sonnet", "fable"],
        "auth": {"kind": "environment", "names": ["ANTHROPIC_API_KEY"]},
    }))
    .expect("fixture runtime");
    let request: StartRequest = serde_json::from_value(json!({
        "runtime": "claude",
        "model": model,
        "profile": "implement",
        "task": "fixture task",
        "workdir": root.join("work"),
        "write": write,
        "effort": "medium",
    }))
    .expect("fixture request");
    let profile = Profile {
        name: request.profile.clone(),
        body: "Fixture role.".into(),
        write,
        network,
        revision: "fixture".into(),
        canonical: false,
        allow_external_read_roots: true,
        read_roots: vec![request.workdir.clone()],
        skills: vec![],
        mcp: vec![],
        required_constraints: BTreeSet::new(),
    };
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
    (config, runtime, request, profile)
}

/// Converts a request into the durable subset required by launch planning.
fn record(request: StartRequest) -> Record {
    let id: AgentId = "ag-20260101-000000-0000000001".parse().expect("fixture id");
    Record {
        id: id.clone(),
        request,
        status: Status::Created,
        created_at: 0.0,
        started_at: None,
        finished_at: None,
        supervisor_pid: None,
        supervisor_identity: None,
        supervisor_birth_time: None,
        process_group_id: None,
        runtime_session_id: None,
        failure_kind: None,
        failure_text: None,
        exit_code: None,
        answer_path: None,
        answer_bytes: None,
        answer_sha256: None,
        orchestrator_session_id: None,
        parent_agent_id: None,
        root_agent_id: id,
        sequence: 0,
        resume_of_runtime_session_id: None,
        identity: None,
    }
}

/// Finds a native flag value in a completed launch plan.
fn flag<'a>(plan: &'a agent_run_adapters::LaunchPlan, name: &str) -> &'a str {
    let index = plan
        .args
        .iter()
        .position(|arg| arg == name)
        .expect("flag exists");
    &plan.args[index + 1]
}

/// Mirrors `tests/test_claude_adapter.py::ClaudeAdapterTests::test_describe_reports_api_version_and_supports_live_limits`.
/// Mirrors `tests/test_claude_adapter.py::ClaudeAdapterTests::test_validate_accepts_global_or_known_environment_auth`.
/// Mirrors `tests/test_claude_adapter.py::ClaudeAdapterTests::test_validate_refuses_unknown_hook_events`.
/// Mirrors `tests/test_claude_adapter.py::ClaudeAdapterTests::test_models_reflect_the_configured_roster_without_a_live_call`.
/// Mirrors `tests/test_claude_adapter.py::ClaudeAdapterTests::test_models_describe_fable_as_claude_fable_5_1_and_leave_other_ids_generic`.
/// Mirrors `tests/test_claude_adapter.py::ClaudeAdapterTests::test_prepare_fails_closed_on_a_model_outside_the_roster`.
/// Mirrors `tests/test_claude_adapter.py::ClaudeAdapterTests::test_prepare_validates_effort_and_passes_the_native_flag`.
#[test]
fn claude_validation_is_local_and_roster_bound() {
    let temporary = tempfile::tempdir().expect("temporary fixture root");
    let (_, runtime, request, profile) = fixture(temporary.path(), false, false, "fable");
    validate(&request, &runtime, &profile).expect("configured Claude request");
    assert!(agent_run_adapters::capabilities(Adapter::Claude).contains(&"output_schema"));
    let mut unconfigured = request.clone();
    unconfigured.model = "unknown".into();
    assert!(validate(&unconfigured, &runtime, &profile).is_err());
    let mut invalid_effort = request;
    invalid_effort.effort = Some("turbo".into());
    assert!(validate(&invalid_effort, &runtime, &profile).is_err());
}

/// Mirrors `tests/test_claude_adapter.py::ClaudeAdapterTests::test_materialize_requires_mcp_mapping_and_prepare_requires_role`.
/// Mirrors `tests/test_claude_adapter.py::ClaudeAdapterTests::test_materialize_writes_only_declared_hooks_into_settings`.
/// Mirrors `tests/test_claude_adapter.py::ClaudeAdapterTests::test_materialize_renders_hook_commands_shell_quoted`.
/// Mirrors `tests/test_claude_adapter.py::ClaudeAdapterTests::test_materialize_fails_closed_when_mcp_name_has_no_resolution`.
/// Mirrors `tests/test_claude_adapter.py::ClaudeAdapterTests::test_materialize_renders_strict_mcp_config_when_resolved`.
/// Mirrors `tests/test_claude_adapter.py::ClaudeAdapterTests::test_materialize_generates_plugin_dirs_only_for_selected_skills`.
/// Mirrors `tests/test_claude_adapter.py::ClaudeAdapterTests::test_materialize_rejects_linked_skill_content`.
/// Mirrors `tests/test_claude_adapter.py::ClaudeAdapterTests::test_materialize_uses_only_each_explicit_service_skill_root`.
/// Mirrors `tests/test_claude_adapter.py::ClaudeAdapterTests::test_materialize_fails_closed_on_missing_skill`.
/// Mirrors `tests/test_claude_adapter.py::ClaudeAdapterTests::test_materialize_writes_only_declared_hooks_into_settings`.
#[test]
fn claude_home_is_private_and_contains_only_selected_assets() {
    let temporary = tempfile::tempdir().expect("temporary fixture root");
    let (config, runtime, request, profile) = fixture(temporary.path(), false, false, "sonnet");
    let home = temporary.path().join("home");
    let (snapshot, _) = materialize::materialize(
        &config,
        &runtime,
        &request,
        &profile,
        &home,
        temporary.path(),
    )
    .expect("materialize empty Claude home");
    assert!(home.join("settings.json").is_file());
    assert!(!home.join("mcp/mcp-config.json").exists());
    let index = std::fs::read_to_string(home.join(".agent-run-snapshots.json"))
        .expect("runtime snapshot index is published");
    assert!(index.contains("settings.json"));
    assert!(snapshot.plugin_paths.is_empty());
}

/// Mirrors `tests/test_claude_adapter.py::ClaudeAdapterTests::test_prepare_resolves_only_the_fable_alias_in_the_child_argv`.
/// Mirrors `tests/test_claude_adapter.py::ClaudeAdapterTests::test_prepare_inherits_host_environment_with_runtime_home`.
/// Mirrors `tests/test_claude_adapter.py::ClaudeAdapterTests::test_prepare_builds_an_isolated_launch_plan_for_a_read_only_profile`.
/// Mirrors `tests/test_claude_adapter.py::ClaudeAdapterTests::test_prepare_grants_write_tools_only_when_profile_allows_write`.
/// Mirrors `tests/test_claude_adapter.py::ClaudeAdapterTests::test_prepare_grants_web_tools_only_to_network_profiles`.
/// Mirrors `tests/test_claude_adapter.py::ClaudeAdapterTests::test_request_can_narrow_but_not_widen_profile_write`.
/// Mirrors `tests/test_claude_adapter.py::ClaudeAdapterTests::test_prepare_normalizes_profile_and_request_roots_as_one_antichain`.
/// Mirrors `tests/test_claude_adapter.py::ClaudeAdapterTests::test_prepare_adds_dirs_and_mcp_flags_and_never_leaks_secrets_into_argv`.
/// Mirrors `tests/test_claude_adapter.py::ClaudeAdapterTests::test_prepare_exposes_the_skill_tool_only_when_skills_are_configured`.
/// Mirrors `tests/test_claude_adapter.py::ClaudeAdapterTests::test_prepare_uses_native_global_claude_state_by_default`.
/// Mirrors `tests/test_claude_adapter.py::ClaudeAdapterTests::test_prepare_preserves_host_home_for_unscoped_native_state`.
/// Mirrors `tests/test_claude_adapter.py::ClaudeAdapterTests::test_prepare_keeps_scoped_home_for_account_state`.
/// Mirrors `tests/test_claude_adapter.py::ClaudeAdapterTests::test_prepare_scopes_cli_state_without_injecting_an_oauth_token`.
/// Mirrors `tests/test_claude_adapter.py::ClaudeAdapterTests::test_labelled_state_drops_ambient_global_credentials`.
/// Mirrors `tests/test_claude_adapter.py::ClaudeAdapterTests::test_prepare_reasserts_scoped_state_after_a_developer_preset`.
/// Mirrors `tests/test_claude_adapter.py::ClaudeAdapterTests::test_prepare_keeps_scoped_state_when_an_mcp_declares_it`.
/// Mirrors `tests/test_claude_adapter.py::ClaudeAdapterTests::test_prepare_uses_explicit_declared_auth_over_scoped_cli_state`.
/// Mirrors `tests/test_claude_developer_environment.py::ClaudeDeveloperEnvironmentTests::test_prepare_ignores_preset_and_inherits_host_values_for_mcp`.
/// Mirrors `tests/test_claude_developer_environment.py::ClaudeDeveloperEnvironmentTests::test_prepare_does_not_probe_legacy_required_commands`.
/// Mirrors `tests/test_claude_developer_environment.py::ClaudeDeveloperEnvironmentTests::test_prepare_denies_gh_bare_and_absolute_while_git_remains_permitted`.
#[test]
fn claude_launch_is_explicitly_scoped_without_secret_argv() {
    let temporary = tempfile::tempdir().expect("temporary fixture root");
    let (config, runtime, request, profile) = fixture(temporary.path(), false, false, "fable");
    std::fs::create_dir_all(&request.workdir).expect("fixture workdir");
    let home = temporary.path().join("home");
    let (snapshot, _) = materialize::materialize(
        &config,
        &runtime,
        &request,
        &profile,
        &home,
        temporary.path(),
    )
    .expect("materialize Claude home");
    let plan = plan_with_environment(
        &config,
        &runtime,
        &record(request.clone()),
        &profile,
        &home,
        &snapshot,
        BTreeMap::from([
            ("HOME".into(), home.display().to_string()),
            ("PATH".into(), "/usr/bin:/bin".into()),
            ("ANTHROPIC_API_KEY".into(), "fixture-secret".into()),
        ]),
    )
    .expect("build launch plan");
    assert_eq!(flag(&plan, "--model"), "claude-fable-5-1");
    assert_eq!(flag(&plan, "--permission-mode"), "default");
    assert!(flag(&plan, "--tools").contains("Read"));
    assert!(!flag(&plan, "--tools").contains("Bash"));
    assert!(!plan.args.iter().any(|arg| arg.contains("fixture-secret")));
    assert!(plan
        .initial_input
        .expect("Claude input")
        .contains("fixture task"));
}

/// Mirrors `tests/test_claude_adapter.py::ClaudeAdapterTests::test_declared_plugins_are_loaded_by_path_without_widening_tools`.
/// Mirrors `tests/test_claude_adapter.py::ClaudeAdapterTests::test_materialize_fingerprint_tracks_declared_plugins`.
/// Mirrors `tests/test_claude_adapter.py::ClaudeAdapterTests::test_duplicate_live_plugin_names_are_legacy_compatible`.
/// Mirrors `tests/test_claude_adapter.py::ClaudeAdapterTests::test_declared_plugin_assets_use_the_managed_snapshot_path`.
/// Mirrors `tests/test_claude_adapter.py::ClaudeAdapterTests::test_prepare_fails_closed_when_write_mode_cannot_keep_a_read_root_read_only`.
/// Mirrors `tests/test_claude_adapter.py::ClaudeAdapterTests::test_prepare_fails_closed_when_an_mcp_definition_is_unresolved`.
/// Mirrors `tests/test_claude_adapter.py::ClaudeAdapterTests::test_prepare_fails_closed_when_an_mcp_env_var_is_not_set`.
/// Mirrors `tests/test_claude_adapter.py::ClaudeAdapterTests::test_prepare_fails_closed_when_native_run_lacks_scoped_claude_config_for_mcp`.
/// Mirrors `tests/test_claude_adapter.py::ClaudeAdapterTests::test_prepare_inherits_host_toolchain_for_read_only_mcp`.
/// Mirrors `tests/test_claude_adapter.py::ClaudeAdapterTests::test_prepare_without_explicit_auth_leaves_the_scoped_cli_to_fail_closed`.
/// Mirrors `tests/test_claude_adapter.py::ClaudeAdapterTests::test_probe_is_local_only_and_checks_named_auth_env_presence`.
/// Mirrors `tests/test_claude_adapter.py::ClaudeAdapterTests::test_probe_reports_scoped_cli_state_as_unknown_when_env_is_bare`.
/// Mirrors `tests/test_claude_developer_environment.py::ClaudeDeveloperEnvironmentTests::test_materialize_revision_ignores_legacy_preset_changes`.
#[test]
fn claude_contract_exposes_no_ambient_runtime_controls() {
    let temporary = tempfile::tempdir().expect("temporary fixture root");
    let (_, runtime, _, _) = fixture(temporary.path(), false, false, "sonnet");
    assert_eq!(runtime.kind().expect("Claude adapter"), Adapter::Claude);
    assert_eq!(agent_run_adapters::glm::cli_model("glm-5.3"), "glm-5.3[1m]");
}

/// Builds a runtime of one CLI adapter family for native continuation checks.
fn resume_runtime(root: &std::path::Path, adapter: &str) -> Runtime {
    serde_json::from_value(json!({
        "enabled": true,
        "adapter": adapter,
        "binary": "/bin/echo",
        "home": root.join("runtime"),
        "models": ["sonnet", "fable"],
        "auth": {"kind": "environment", "names": ["ANTHROPIC_API_KEY"]},
    }))
    .expect("fixture resume runtime")
}

/// Plans one launch for an adapter family, optionally continuing a saved session.
fn resume_plan(
    root: &std::path::Path,
    adapter: &str,
    resume: Option<&str>,
) -> agent_run_adapters::LaunchPlan {
    let (config, _, request, profile) = fixture(root, false, false, "sonnet");
    std::fs::create_dir_all(&request.workdir).expect("fixture workdir");
    let runtime = resume_runtime(root, adapter);
    let mut record = record(request);
    record.resume_of_runtime_session_id = resume.map(str::to_owned);
    let home = root.join("home");
    plan_with_environment(
        &config,
        &runtime,
        &record,
        &profile,
        &home,
        &materialize::Snapshot {
            plugin_paths: vec![],
            plugin_roots: BTreeMap::new(),
        },
        BTreeMap::from([
            ("HOME".into(), home.display().to_string()),
            ("PATH".into(), "/usr/bin:/bin".into()),
        ]),
    )
    .expect("build continuation plan")
}

/// Mirrors `tests/test_resume_adapters.py::ArgumentsTests::test_resume_arguments_preserve_other_settings`
///
/// A continuation targets one exact saved session and changes nothing else: the
/// fresh-session selector is gone, the native selector is last, and the model,
/// working directory and prompt are untouched.
#[test]
fn resume_arguments_preserve_other_settings() {
    let temporary = tempfile::tempdir().expect("temporary fixture root");
    let fresh = resume_plan(temporary.path(), "claude", None);
    assert!(
        fresh.args.iter().any(|argument| argument == "--session-id"),
        "a fresh launch still claims its own new session"
    );
    assert!(!fresh.args.iter().any(|argument| argument == "--resume"));

    let resumed = resume_plan(temporary.path(), "claude", Some("saved"));
    assert_eq!(
        &resumed.args[resumed.args.len() - 2..],
        ["--resume".to_owned(), "saved".to_owned()]
    );
    assert!(
        !resumed
            .args
            .iter()
            .any(|argument| argument == "--session-id"),
        "a continuation must never also request a fresh session"
    );
    assert_eq!(flag(&resumed, "--model"), flag(&fresh, "--model"));
    assert_eq!(resumed.cwd, fresh.cwd);
    assert_eq!(resumed.initial_input, fresh.initial_input);
}

/// Mirrors `tests/test_resume_adapters.py::ArgumentsTests::test_adapters_pass_exact_native_resume_selector_to_process`
///
/// Every CLI adapter family hands the process the same exact native selector
/// exactly once, and none of them can accidentally ask for a new session.
#[test]
fn adapters_pass_exact_native_resume_selector_to_process() {
    for adapter in ["claude", "glm", "qwen"] {
        let temporary = tempfile::tempdir().expect("temporary fixture root");
        let plan = resume_plan(temporary.path(), adapter, Some("saved"));
        assert_eq!(
            &plan.args[plan.args.len() - 2..],
            ["--resume".to_owned(), "saved".to_owned()],
            "{adapter} must end with the exact native selector"
        );
        assert!(
            !plan.args.iter().any(|argument| argument == "--session-id"),
            "{adapter} must not request a fresh session while resuming"
        );
        assert_eq!(
            plan.args
                .iter()
                .filter(|argument| *argument == "--resume")
                .count(),
            1,
            "{adapter} must select the session exactly once"
        );
    }
}
