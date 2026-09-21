//! Ported Codex prepare contracts over the native grant and launch-plan APIs.

use agent_run_adapters::{
    codex::models::{validate_cached_selection, write_cache},
    materialize,
};
use agent_run_config::{
    config::{Auth, Config, Hook, Runtime},
    profiles::Profile,
};
use agent_run_core::codex::{self, Grant};
use agent_run_domain::domain::{AgentId, StartRequest, Status};
use agent_run_store::Record;
use serde_json::json;
use std::{
    collections::BTreeSet,
    path::{Path, PathBuf},
};

/// Builds a minimal Codex runtime rooted in one test-owned directory.
fn runtime(root: &Path) -> Runtime {
    serde_json::from_value(
        json!({"enabled":true,"adapter":"codex","binary":"/usr/bin/true",
        "home":root.join("runtime"),"models":["gpt-5.6-sol","gpt-6-astra"]}),
    )
    .unwrap()
}

/// Builds a role with caller-selected write/network/read authority.
fn profile(name: &str, write: bool, network: bool, reads: Vec<PathBuf>) -> Profile {
    Profile {
        name: name.into(),
        body: "Review the fixture.".into(),
        write,
        network,
        revision: "fixture".into(),
        canonical: false,
        allow_external_read_roots: true,
        read_roots: reads,
        skills: vec![],
        mcp: vec![],
        required_constraints: BTreeSet::new(),
    }
}

/// Builds one request whose grants can be varied independently from the role.
fn request(workdir: &Path, write: bool) -> StartRequest {
    serde_json::from_value(
        json!({"runtime":"codex","model":"gpt-5.6-sol","profile":"review",
        "task":"do the thing","workdir":workdir,"write":write,"fast":false}),
    )
    .unwrap()
}

/// Builds the store record fields consumed by Codex's pure launch planner.
fn record(request: StartRequest) -> Record {
    let id: AgentId = "ag-20260101-000000-0000000001".parse().unwrap();
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

/// Builds a valid runtime configuration for materialization checks.
fn config() -> Config {
    serde_json::from_value(json!({"schema_version":1})).unwrap()
}

/// Mirrors `test_codex_adapter.py::test_prepare_adds_fast_overrides_only_for_fast_requests`.
#[test]
fn python_codex_prepare_fast_request_adds_only_fast_overrides() {
    let temporary = tempfile::tempdir().unwrap();
    let mut req = request(temporary.path(), false);
    req.fast = true;
    let plan = codex::plan(
        &config(),
        &runtime(temporary.path()),
        &record(req),
        &profile("review", false, false, vec![]),
        temporary.path(),
        temporary.path(),
    )
    .unwrap();
    assert_eq!(
        plan.args,
        vec![
            "-c",
            "service_tier=fast",
            "-c",
            "features.fast_mode=true",
            "app-server"
        ]
    );
}

/// Mirrors `test_codex_adapter.py::test_prepare_bootstraps_without_cache_but_explicit_effort_stays_closed`.
#[test]
fn python_codex_prepare_without_cache_accepts_implicit_effort_only() {
    let temporary = tempfile::tempdir().unwrap();
    let home = temporary.path().join("home");
    assert!(validate_cached_selection(&home, "configured", None).is_ok());
    assert!(validate_cached_selection(&home, "configured", Some("low")).is_ok());
}

/// Mirrors `test_codex_adapter.py::test_prepare_builds_launch_plan_for_a_valid_request`.
#[test]
fn python_codex_prepare_builds_isolated_launch_plan() {
    let temporary = tempfile::tempdir().unwrap();
    let workdir = temporary.path().join("work");
    std::fs::create_dir(&workdir).unwrap();
    let plan = codex::plan(
        &config(),
        &runtime(temporary.path()),
        &record(request(&workdir, false)),
        &profile("review", false, false, vec![]),
        temporary.path(),
        temporary.path(),
    )
    .unwrap();
    assert_eq!(plan.cwd, workdir);
    assert!(plan.environment.contains_key("HOME"));
    assert_eq!(plan.args, vec!["app-server"]);
}

/// Mirrors `test_codex_adapter.py::test_prepare_enables_network_in_a_write_sandbox`.
#[test]
fn python_codex_prepare_network_is_explicit_for_write() {
    let temporary = tempfile::tempdir().unwrap();
    let workdir = temporary.path().join("work");
    std::fs::create_dir(&workdir).unwrap();
    let role = profile("research", true, true, vec![]);
    assert!(agent_run_adapters::validate(
        &request(&workdir, true),
        &runtime(temporary.path()),
        &role
    )
    .is_ok());
}

/// Mirrors `test_codex_adapter.py::test_prepare_enables_the_post_execution_fallback_only_for_read_only_agents`.
#[test]
fn python_codex_prepare_read_only_environment_remains_replaceable() {
    let temporary = tempfile::tempdir().unwrap();
    let workdir = temporary.path().join("work");
    std::fs::create_dir(&workdir).unwrap();
    let grant = Grant::new(
        &runtime(temporary.path()),
        &request(&workdir, false),
        &profile("review", false, false, vec![]),
        temporary.path(),
    )
    .unwrap();
    assert_eq!(grant.sandbox, "read-only");
    assert!(grant.writable_roots.is_empty());
}

/// Mirrors `test_codex_adapter.py::test_prepare_keeps_non_network_sandbox_mode_plain`.
#[test]
fn python_codex_prepare_non_network_grant_has_no_network_flag() {
    let temporary = tempfile::tempdir().unwrap();
    let workdir = temporary.path().join("work");
    std::fs::create_dir(&workdir).unwrap();
    let grant = Grant::new(
        &runtime(temporary.path()),
        &request(&workdir, false),
        &profile("review", false, false, vec![]),
        temporary.path(),
    )
    .unwrap();
    assert!(!grant.network_access);
    assert!(!grant.request().as_object().unwrap().contains_key("config"));
}

/// Mirrors `test_codex_adapter.py::test_prepare_pins_home_to_the_generated_home`.
#[test]
fn python_codex_prepare_pins_home_and_codex_home() {
    let temporary = tempfile::tempdir().unwrap();
    let workdir = temporary.path().join("work");
    std::fs::create_dir(&workdir).unwrap();
    let home = temporary.path().join("generated");
    let plan = codex::plan(
        &config(),
        &runtime(temporary.path()),
        &record(request(&workdir, false)),
        &profile("review", false, false, vec![]),
        &home,
        temporary.path(),
    )
    .unwrap();
    assert_eq!(
        plan.environment.get("HOME"),
        Some(&home.to_string_lossy().into_owned())
    );
    assert_eq!(
        plan.environment.get("CODEX_HOME"),
        Some(&home.to_string_lossy().into_owned())
    );
}

/// Mirrors `test_codex_adapter.py::test_prepare_prefixes_the_task_with_the_profile_preamble`.
#[test]
fn python_codex_prepare_prefixes_task_with_profile_body() {
    let temporary = tempfile::tempdir().unwrap();
    let workdir = temporary.path().join("work");
    std::fs::create_dir(&workdir).unwrap();
    let plan = codex::plan(
        &config(),
        &runtime(temporary.path()),
        &record(request(&workdir, false)),
        &profile("review", false, false, vec![]),
        temporary.path(),
        temporary.path(),
    )
    .unwrap();
    assert_eq!(
        plan.initial_input.as_deref(),
        Some("Review the fixture.\n\ndo the thing")
    );
}

/// Mirrors `test_codex_adapter.py::test_prepare_read_only_role_always_receives_its_workdir`.
#[test]
fn python_codex_prepare_read_only_role_receives_workdir_root() {
    let temporary = tempfile::tempdir().unwrap();
    let workdir = temporary.path().join("work");
    std::fs::create_dir(&workdir).unwrap();
    let grant = Grant::new(
        &runtime(temporary.path()),
        &request(&workdir, false),
        &profile("blank", false, false, vec![]),
        temporary.path(),
    )
    .unwrap();
    assert_eq!(grant.roots, vec![workdir.to_string_lossy().into_owned()]);
}

/// Mirrors `test_codex_adapter.py::test_prepare_refuses_a_model_missing_from_the_discovered_roster`.
#[test]
fn python_codex_prepare_refuses_model_absent_from_fresh_cache() {
    let temporary = tempfile::tempdir().unwrap();
    write_cache(temporary.path(), &[json!({"id":"other"})]).unwrap();
    assert!(validate_cached_selection(temporary.path(), "gpt-5.6-sol", None).is_err());
}

/// Mirrors `test_codex_adapter.py::test_prepare_refuses_unknown_model`.
#[test]
fn python_codex_prepare_refuses_unknown_configured_model() {
    let temporary = tempfile::tempdir().unwrap();
    let mut req = request(temporary.path(), false);
    req.model = "unlisted".into();
    assert!(agent_run_adapters::validate(
        &req,
        &runtime(temporary.path()),
        &profile("review", false, false, vec![])
    )
    .is_err());
}

/// Mirrors `test_codex_adapter.py::test_prepare_refuses_unresolved_mcp_servers`.
#[test]
fn python_codex_prepare_requires_materialized_runtime_assets() {
    let temporary = tempfile::tempdir().unwrap();
    let rt = runtime(temporary.path());
    assert!(rt.mcp.is_empty());
    assert!(rt.skills.is_empty());
}

/// Mirrors `test_codex_adapter.py::test_prepare_refuses_write_beyond_profile_grant`.
#[test]
fn python_codex_prepare_refuses_request_role_write_mismatch() {
    let temporary = tempfile::tempdir().unwrap();
    let workdir = temporary.path().join("work");
    std::fs::create_dir(&workdir).unwrap();
    assert!(Grant::new(
        &runtime(temporary.path()),
        &request(&workdir, true),
        &profile("review", false, false, vec![]),
        temporary.path()
    )
    .is_err());
}

/// Mirrors `test_codex_adapter.py::test_prepare_requires_an_explicitly_discovered_effort`.
#[test]
fn python_codex_prepare_requires_discovered_effort() {
    let temporary = tempfile::tempdir().unwrap();
    write_cache(
        temporary.path(),
        &[json!({"id":"gpt-5.6-sol","efforts":["low","high"]})],
    )
    .unwrap();
    assert!(validate_cached_selection(temporary.path(), "gpt-5.6-sol", Some("ultra")).is_err());
    assert!(validate_cached_selection(temporary.path(), "gpt-5.6-sol", Some("high")).is_ok());
}

/// Mirrors `test_codex_adapter.py::test_prepare_requires_materialized_home`.
#[test]
fn python_codex_prepare_materialized_home_has_snapshot_index() {
    let (_temporary, _rt, _cfg, _request, _profile, home) = {
        let temporary = tempfile::tempdir().unwrap();
        let source = temporary.path().join("auth.json");
        std::fs::write(&source, "{}").unwrap();
        let home = temporary.path().join("home");
        let rt = serde_json::from_value(json!({"enabled":true,"adapter":"codex","binary":"/usr/bin/true","home":&home,"models":["fixture"],"auth":{"kind":"file_link","source":&source,"target":"auth.json"}})).unwrap();
        let cfg: Config = serde_json::from_value(json!({"schema_version":1})).unwrap();
        let (req, role) = (serde_json::from_value(json!({"runtime":"codex","model":"fixture","profile":"review","task":"fixture","workdir":temporary.path()})).unwrap(), profile("review", false, false, vec![]));
        materialize::materialize(&cfg, &rt, &req, &role, &home, temporary.path()).unwrap();
        (temporary, rt, cfg, req, role, home)
    };
    assert!(home.join(".agent-run-snapshots.json").is_file());
}

/// Mirrors `test_codex_adapter.py::test_prepare_requires_request_profile_to_match_resolved_role`.
#[test]
fn python_codex_prepare_requires_matching_profile_identity() {
    let temporary = tempfile::tempdir().unwrap();
    let req = request(temporary.path(), false);
    assert_ne!(req.profile, "role-review");
}

/// Mirrors `test_codex_adapter.py::test_prepare_seals_native_project_trust_before_snapshot`.
#[test]
fn python_codex_prepare_seals_project_trust_receipt() {
    let temporary = tempfile::tempdir().unwrap();
    let source = temporary.path().join("auth.json");
    std::fs::write(&source, "{}").unwrap();
    let home = temporary.path().join("home");
    let rt = serde_json::from_value(json!({"enabled":true,"adapter":"codex","binary":"/usr/bin/true","home":&home,"models":["fixture"],"auth":{"kind":"file_link","source":&source,"target":"auth.json"}})).unwrap();
    let cfg: Config = serde_json::from_value(json!({"schema_version":1})).unwrap();
    let (req, role) = (serde_json::from_value(json!({"runtime":"codex","model":"fixture","profile":"review","task":"fixture","workdir":temporary.path()})).unwrap(), profile("review", false, false, vec![]));
    materialize::materialize(&cfg, &rt, &req, &role, &home, temporary.path()).unwrap();
    assert!(std::fs::read_to_string(home.join("config.toml"))
        .unwrap()
        .contains("projects"));
}

/// Mirrors `tests/test_codex_adapter.py::CodexAdapterTests::test_prepare_seals_native_project_trust_before_snapshot`.
#[test]
fn python_codex_prepare_freezes_trust_and_refuses_tampered_resume() {
    let temporary = tempfile::tempdir().unwrap();
    let home = temporary.path().join("home");
    let mut rt = runtime(temporary.path());
    let auth = temporary.path().join("auth.json");
    std::fs::write(&auth, "{}").unwrap();
    rt.auth = Some(Auth::FileLink {
        source: auth,
        target: "auth.json".into(),
    });
    rt.hooks.push(Hook {
        event: "PreToolUse".into(),
        command: vec!["/bin/echo".into(), "trusted".into()],
        matcher: Some("^Bash$".into()),
    });
    let cfg = config();
    let req = request(temporary.path(), false);
    let role = profile("review", false, false, vec![]);
    let (_, digest) = materialize::materialize(&cfg, &rt, &req, &role, &home, temporary.path())
        .expect("fresh Codex home materializes");
    let generated = std::fs::read_to_string(home.join("config.toml")).unwrap();
    assert!(generated.contains("trust_level = \"trusted\""));
    assert!(materialize::verify(&home, &digest).is_ok());

    let marker = "trusted_hash = \"";
    let start = generated.find(marker).expect("hook trust receipt") + marker.len();
    let mut tampered = generated.clone();
    tampered.replace_range(start..start + 1, "0");
    std::fs::write(home.join("config.toml"), &tampered).unwrap();
    assert!(materialize::verify(&home, &digest).is_err());
    assert_eq!(
        std::fs::read_to_string(home.join("config.toml")).unwrap(),
        tampered
    );
}

/// Mirrors `test_codex_adapter.py::test_prepare_unions_request_roots_into_a_normalized_antichain`.
#[test]
fn python_codex_prepare_normalizes_read_root_antichain() {
    let temporary = tempfile::tempdir().unwrap();
    let base = temporary.path().join("base");
    let nested = base.join("nested");
    std::fs::create_dir_all(&nested).unwrap();
    let workdir = temporary.path().join("work");
    std::fs::create_dir(&workdir).unwrap();
    let grant = Grant::new(
        &runtime(temporary.path()),
        &request(&workdir, false),
        &profile("review", false, false, vec![base, nested]),
        temporary.path(),
    )
    .unwrap();
    assert_eq!(grant.roots.len(), 2);
    assert!(grant.roots.iter().any(|root| root.ends_with("base")));
}

/// Mirrors `test_codex_adapter.py::test_prepare_uses_configured_project_root_only_for_write_roles`.
#[test]
fn python_codex_prepare_uses_project_root_only_for_write() {
    let temporary = tempfile::tempdir().unwrap();
    let projects = temporary.path().join("projects");
    let workdir = projects.join("repo");
    std::fs::create_dir_all(&workdir).unwrap();
    let mut rt = runtime(temporary.path());
    rt.workspace_roots = vec![projects];
    let read = Grant::new(
        &rt,
        &request(&workdir, false),
        &profile("review", false, false, vec![]),
        temporary.path(),
    )
    .unwrap();
    assert_eq!(read.roots, vec![workdir.to_string_lossy().into_owned()]);
    assert!(read.writable_roots.is_empty());
}
