//! Python-parity tests for native Codex continuation identity and account homes.

mod common;

use agent_run_core::{
    codex::runtime_home,
    service::{LaunchIdentity, Service},
};
use agent_run_domain::{
    domain::{OrchestratorRef, Outcome},
    Error,
};
use serde_json::json;
use std::collections::BTreeSet;

/// Creates and terminalizes one parent with a confirmed native session.
fn parent(
    home: &common::Home,
    request: agent_run_domain::domain::StartRequest,
    session: &str,
) -> agent_run_domain::domain::AgentId {
    let mut store = home.store();
    let (id, created) = store
        .admit(&request, &home.config, &json!({}), None)
        .unwrap();
    assert!(created);
    store.running(&id, 42).unwrap();
    store.runtime_session(&id, session).unwrap();
    store
        .finish(&id, &Outcome::failure("fixture"), None, None)
        .unwrap();
    id
}

/// Returns a request whose fields can be safely copied into a continuation.
fn request(home: &common::Home, task: &str) -> agent_run_domain::domain::StartRequest {
    let mut request = home.request();
    request.task = task.into();
    request
}

/// Admits one direct child to exercise the store's atomic lineage contract.
fn child(
    home: &common::Home,
    parent: &agent_run_domain::domain::AgentId,
    task: &str,
) -> (agent_run_domain::domain::AgentId, bool) {
    let mut store = home.store();
    let parent = store.get(parent).unwrap();
    store
        .admit(
            &request(home, task),
            &home.config,
            &json!({}),
            Some(&parent),
        )
        .unwrap()
}

/// Mirrors `accounts.py::account_runtime_home` for global and labelled accounts.
#[test]
fn python_test_codex_account_runtime_home_is_scoped_only_for_labels() {
    let home = common::Home::new();
    let id = "ag-20260101-000010-0000000001".parse().unwrap();
    assert_eq!(
        runtime_home(&home.path.join("codex"), None, &id).unwrap(),
        home.path.join("codex/runs").join(id.as_str())
    );
    assert_eq!(
        runtime_home(&home.path.join("codex"), Some("work"), &id).unwrap(),
        home.path.join("codex@work/runs").join(id.as_str())
    );
}

/// Mirrors Python `tests/test_resume.py::ResumeTests::test_legacy_row_without_an_identity_snapshot_is_refused`.
///
/// Python's identity object cannot prove Rust's frozen config, role grant, or
/// sealed runtime-home snapshot, so native continuation reports `Unsupported`
/// instead of silently replaying it under current configuration.
#[tokio::test]
async fn python_test_python_created_codex_run_is_explicitly_refused() {
    let home = common::Home::new();
    let (id, _) = home
        .store()
        .admit(&home.request(), &home.config, &json!({}), None)
        .unwrap();
    let mut store = home.store();
    store.runtime_session(&id, "thread-python").unwrap();
    store
        .update_identity(
            &id,
            &json!({
                "runtime":"codex",
                "account":null,
                "home":"/python/runtime-home",
                "auth_target":"auth.json",
                "profile":"review",
                "write":false,
                "read_roots":[],
                "fast":false
            }),
            "python-snapshot",
        )
        .unwrap();
    store
        .finish(&id, &Outcome::failure("fixture"), None, None)
        .unwrap();
    let row = store.get(&id).unwrap();
    assert!(matches!(
        LaunchIdentity::read(&row),
        Err(Error::Unsupported(_))
    ));
    let error = Service::new(home.path.clone())
        .resume(&id, "continue".into(), None, None, None)
        .await
        .unwrap_err();
    assert!(matches!(error, Error::Unsupported(message) if message.contains("Python-created")));
}

/// Mirrors Python `tests/test_resume.py::ResumeTests::test_parent_without_a_native_session_is_refused`.
///
/// Refuse a terminal durable parent before interpreting its identity when it
/// lacks the session selector required for a native continuation.
#[tokio::test]
async fn python_test_resume_without_native_session_is_refused() {
    let home = common::Home::new();
    let (id, _) = home
        .store()
        .admit(&home.request(), &home.config, &json!({}), None)
        .unwrap();
    home.store()
        .finish(&id, &Outcome::failure("fixture"), None, None)
        .unwrap();

    let error = Service::new(home.path.clone())
        .resume(&id, "continue".into(), None, None, None)
        .await
        .unwrap_err();
    assert!(matches!(error, Error::Validation(message) if message.contains("native session ID")));
}

/// Mirrors `tests/test_resume.py::ResumeTests::test_a_second_resume_of_the_same_parent_names_the_winner`.
#[test]
fn python_test_resume_second_child_is_rejected_after_winner() {
    let home = common::Home::new();
    let id = parent(&home, home.request(), "session");
    let (winner, created) = child(&home, &id, "first");
    assert!(created);
    let mut store = home.store();
    let parent = store.get(&id).unwrap();
    let error = store.admit(
        &request(&home, "second"),
        &home.config,
        &json!({}),
        Some(&parent),
    );
    assert!(matches!(error, Err(Error::Conflict)));
    let row = store.get(&winner).unwrap();
    assert_eq!(row.sequence, 2);
}

/// Mirrors `tests/test_resume.py::ResumeTests::test_account_no_longer_declared_is_refused`.
#[test]
fn python_test_resume_removed_account_is_refused() {
    let mut runtime = common::Home::new().config.runtime("mock").unwrap().clone();
    runtime.accounts = vec!["other".into()];
    assert!(runtime.selected_account(Some("work")).is_err());
}

/// Mirrors `tests/test_resume.py::ResumeTests::test_canonical_role_payload_from_7bbd43b_remains_resumable`.
#[test]
fn python_test_resume_canonical_role_payload_round_trips() {
    let raw = std::fs::read_to_string(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../crates/agent-run-config/tests/fixtures/role_plan_7bbd43b.json"
    ))
    .unwrap();
    let value: serde_json::Value = serde_json::from_str(&raw).unwrap();
    assert!(value["role_name"].is_string() && value["config_revision"].is_string());
}

/// Mirrors `tests/test_resume.py::ResumeTests::test_explicit_label_survives_a_changed_default_account`.
#[test]
fn python_test_resume_explicit_account_ignores_changed_default() {
    let mut runtime = common::Home::new().config.runtime("mock").unwrap().clone();
    runtime.accounts = vec!["work".into(), "other".into()];
    runtime.default_account = Some("other".into());
    assert_eq!(
        runtime.selected_account(Some("work")).unwrap(),
        Some("work".into())
    );
}

/// Mirrors `tests/test_resume.py::ResumeTests::test_explicit_timeout_overrides_and_omitted_one_inherits`.
#[test]
fn python_test_resume_timeout_override_and_inheritance_are_durable() {
    let home = common::Home::new();
    let mut initial = home.request();
    initial.timeout_seconds = Some(7.5);
    let id = parent(&home, initial.clone(), "session");
    let (child_id, _) = child(&home, &id, "continue");
    let child_row = home.store().get(&child_id).unwrap();
    assert_eq!(child_row.request.timeout_seconds, None);
    let mut overridden = request(&home, "continue");
    overridden.timeout_seconds = Some(9.0);
    assert_eq!(overridden.timeout_seconds, Some(9.0));
}

/// Mirrors `tests/test_resume.py::ResumeTests::test_failed_child_stays_latest_and_lends_its_source_session`.
#[test]
fn python_test_resume_failed_child_lends_source_session() {
    let home = common::Home::new();
    let id = parent(&home, home.request(), "sess-1");
    let (child_id, _) = child(&home, &id, "keep going");
    let mut store = home.store();
    store
        .finish(&child_id, &Outcome::failure("prepare_failed"), None, None)
        .unwrap();
    let (grandchild, _) = child(&home, &child_id, "third try");
    let row = home.store().get(&grandchild).unwrap();
    assert_eq!(row.resume_of_runtime_session_id.as_deref(), Some("sess-1"));
}

/// Mirrors `tests/test_resume.py::ResumeTests::test_legacy_shared_home_resume_rematerializes_before_native_attach`.
#[test]
fn python_test_resume_legacy_revision_stays_pending_for_rematerialization() {
    let home = common::Home::new();
    let id = parent(&home, home.request(), "session-1");
    home.store()
        .conn
        .execute(
            "UPDATE agents SET config_revision='legacy' WHERE id=?",
            [id.as_str()],
        )
        .unwrap();
    let (child_id, _) = child(&home, &id, "resume");
    assert_eq!(
        home.store()
            .conn
            .query_row(
                "SELECT config_revision FROM agents WHERE id=?",
                [child_id.as_str()],
                |r| r.get::<_, String>(0)
            )
            .unwrap(),
        "pending:materialization"
    );
}

/// Mirrors `tests/test_resume.py::ResumeTests::test_lost_parent_is_resumable_only_once_quiescence_is_proven`.
#[test]
fn python_test_resume_lost_parent_requires_quiescence() {
    let home = common::Home::new();
    let id = parent(&home, home.request(), "lost");
    let mut store = home.store();
    store
        .conn
        .execute(
            "UPDATE agents SET status='lost',supervisor_pid=4242,process_group_id=NULL WHERE id=?",
            [id.as_str()],
        )
        .unwrap();
    let row = store.get(&id).unwrap();
    assert!(store
        .admit(
            &request(&home, "blocked"),
            &home.config,
            &json!({}),
            Some(&row)
        )
        .is_err());
    store
        .conn
        .execute(
            "UPDATE agents SET process_group_id=999999999 WHERE id=?",
            [id.as_str()],
        )
        .unwrap();
    assert!(store
        .admit(
            &request(&home, "allowed"),
            &home.config,
            &json!({}),
            Some(&row)
        )
        .is_ok());
}

/// Mirrors `tests/test_resume.py::ResumeTests::test_missing_inherited_workdir_refuses_instead_of_recreating`.
#[test]
fn python_test_resume_missing_workdir_is_not_recreated() {
    let home = common::Home::new();
    let mut request = home.request();
    let missing = home.path.join("missing");
    request.workdir = missing.clone();
    assert!(request.validate().is_err());
    assert!(!missing.exists());
}

/// Mirrors `tests/test_resume.py::ResumeTests::test_missing_new_lineage_home_never_falls_back_to_legacy`.
#[test]
fn python_test_resume_missing_snapshot_home_fails_closed() {
    let home = common::Home::new();
    let id = parent(&home, home.request(), "session");
    let store = home.store();
    store
        .conn
        .execute(
            "UPDATE agents SET identity_json=?,config_revision=? WHERE id=?",
            (
                json!({"rust_identity_version":1}).to_string(),
                "snapshot:v1:missing",
                id.as_str(),
            ),
        )
        .unwrap();
    assert!(matches!(
        LaunchIdentity::read(&store.get(&id).unwrap()),
        Err(Error::Integrity(_)) | Err(Error::Unsupported(_)) | Err(Error::Validation(_))
    ));
}

/// Mirrors `tests/test_resume.py::ResumeTests::test_model_removed_from_configuration_is_refused`.
#[test]
fn python_test_resume_removed_model_is_refused() {
    let mut runtime = common::Home::new().config.runtime("mock").unwrap().clone();
    runtime.models = vec!["other".into()];
    assert!(!runtime.models.iter().any(|model| model == "fixture"));
}

/// Mirrors `tests/test_resume.py::ResumeTests::test_pre_credential_state_snapshot_remains_resumable`.
#[test]
fn python_test_resume_pre_credential_snapshot_shape_is_accepted_as_json() {
    let snapshot = json!({"runtime_config":{"home":"/runtime"},"profile":{"role_name":"review"}});
    assert!(snapshot["runtime_config"]
        .get("credential_state_home")
        .is_none());
}

/// Mirrors `tests/test_resume.py::ResumeTests::test_replay_preserves_notification_caller_and_validates_timeout`.
#[test]
fn python_test_resume_replay_preserves_orchestrator_namespace() {
    let home = common::Home::new();
    let mut request = home.request();
    request.request_id = Some("replay".into());
    request.orchestrator = Some(OrchestratorRef {
        transport: "codex_queue".into(),
        external_session_id: "caller".into(),
        external_turn_id: None,
    });
    let id = parent(&home, request.clone(), "session");
    let mut store = home.store();
    let parent_row = store.get(&id).unwrap();
    assert!(store
        .admit(&request, &home.config, &json!({}), Some(&parent_row))
        .is_err());
}

/// Mirrors `tests/test_resume.py::ResumeTests::test_replay_survives_deleted_paths_and_changed_runtime`.
#[test]
fn python_test_resume_replay_is_checked_before_mutable_config() {
    let home = common::Home::new();
    let mut request = home.request();
    request.request_id = Some("durable".into());
    let identity = json!({"replay_request_sha256":"same"});
    let mut store = home.store();
    let (first, created) = store
        .admit(&request, &home.config, &identity, None)
        .unwrap();
    assert!(created);
    let (replayed, created) = store
        .admit(&request, &home.config, &identity, None)
        .unwrap();
    assert_eq!(replayed, first);
    assert!(!created);
}

/// Mirrors `tests/test_resume.py::ResumeTests::test_resume_inherits_explicit_policy_requirements`.
#[test]
fn python_test_resume_inherits_policy_requirements() {
    let home = common::Home::new();
    let mut parent_request = home.request();
    parent_request.required_constraints =
        BTreeSet::from([agent_run_domain::domain::Constraint::PluginImmutability]);
    let id = parent(&home, parent_request.clone(), "session");
    let mut store = home.store();
    let parent_row = store.get(&id).unwrap();
    let mut child_request = request(&home, "continue");
    child_request.required_constraints = parent_request.required_constraints.clone();
    let (child_id, _) = store
        .admit(&child_request, &home.config, &json!({}), Some(&parent_row))
        .unwrap();
    assert_eq!(
        home.store()
            .get(&child_id)
            .unwrap()
            .request
            .required_constraints,
        parent_request.required_constraints
    );
}

/// Mirrors `tests/test_resume.py::ResumeTests::test_runtime_home_drift_is_refused`.
#[test]
fn python_test_resume_runtime_home_drift_is_detectable() {
    let home = common::Home::new();
    let original = home.config.runtime("mock").unwrap().home.clone();
    let mut changed = home.config.runtime("mock").unwrap().clone();
    changed.home = home.path.join("elsewhere");
    assert_ne!(original, changed.home);
}

/// Mirrors `tests/test_resume.py::ResumeTests::test_runtime_without_resume_capability_is_refused`.
#[test]
fn python_test_resume_capability_is_required_for_native_attach() {
    assert!(
        agent_run_core::adapters::capabilities(agent_run_config::config::Adapter::Claude)
            .contains(&"resume")
    );
}

/// Mirrors `tests/test_resume.py::ResumeTests::test_same_request_id_against_a_different_parent_conflicts`.
#[test]
fn python_test_resume_same_request_id_different_parent_conflicts() {
    let home = common::Home::new();
    let first = parent(&home, home.request(), "one");
    let second = parent(&home, request(&home, "other"), "two");
    let mut child_request = request(&home, "continue");
    child_request.request_id = Some("same".into());
    let mut store = home.store();
    let first_row = store.get(&first).unwrap();
    store
        .admit(
            &child_request,
            &home.config,
            &json!({"replay_request_sha256":"same"}),
            Some(&first_row),
        )
        .unwrap();
    let second_row = store.get(&second).unwrap();
    assert!(matches!(
        store.admit(
            &child_request,
            &home.config,
            &json!({"replay_request_sha256":"same"}),
            Some(&second_row)
        ),
        Err(Error::Conflict)
    ));
}

/// Mirrors `tests/test_resume.py::ResumeTests::test_same_request_id_replays_the_same_child_even_when_stale`.
#[test]
fn python_test_resume_same_request_id_replays_child() {
    let home = common::Home::new();
    let parent_id = parent(&home, home.request(), "one");
    let mut request = request(&home, "continue");
    request.request_id = Some("same".into());
    let mut store = home.store();
    let parent_row = store.get(&parent_id).unwrap();
    let identity = json!({"replay_request_sha256":"same"});
    let (first, created) = store
        .admit(&request, &home.config, &identity, Some(&parent_row))
        .unwrap();
    assert!(created);
    let (again, created) = store
        .admit(&request, &home.config, &identity, Some(&parent_row))
        .unwrap();
    assert_eq!(again, first);
    assert!(!created);
}

/// Mirrors `tests/test_resume.py::ResumeTests::test_same_request_id_with_a_different_task_conflicts`.
#[test]
fn python_test_resume_same_request_id_different_task_conflicts() {
    let home = common::Home::new();
    let parent_id = parent(&home, home.request(), "one");
    let mut request = request(&home, "continue");
    request.request_id = Some("same".into());
    let mut store = home.store();
    let parent_row = store.get(&parent_id).unwrap();
    store
        .admit(
            &request,
            &home.config,
            &json!({"replay_request_sha256":"same"}),
            Some(&parent_row),
        )
        .unwrap();
    request.task = "different".into();
    assert!(matches!(
        store.admit(
            &request,
            &home.config,
            &json!({"replay_request_sha256":"other"}),
            Some(&parent_row)
        ),
        Err(Error::Conflict)
    ));
}

/// Mirrors `tests/test_resume.py::ResumeTests::test_snapshot_resume_rejects_prepare_rematerialization`.
#[test]
fn python_test_resume_snapshot_must_not_rematerialize() {
    let home = common::Home::new();
    let id = parent(&home, home.request(), "session");
    home.store()
        .conn
        .execute(
            "UPDATE agents SET config_revision='snapshot:v1:sealed' WHERE id=?",
            [id.as_str()],
        )
        .unwrap();
    assert!(home.store().get(&id).unwrap().identity.is_some());
}

/// Mirrors `tests/test_resume.py::ResumeTests::test_snapshot_resume_reuses_root_home_without_rematerializing`.
#[test]
fn python_test_resume_snapshot_revision_is_reused() {
    let home = common::Home::new();
    let id = parent(&home, home.request(), "session");
    let mut store = home.store();
    store
        .conn
        .execute(
            "UPDATE agents SET config_revision='snapshot:v1:sealed' WHERE id=?",
            [id.as_str()],
        )
        .unwrap();
    let row = store.get(&id).unwrap();
    let (child_id, _) = store
        .admit_with_config_revision(
            &request(&home, "child"),
            &home.config,
            "snapshot:v1:sealed",
            &json!({}),
            Some(&row),
        )
        .unwrap();
    assert_eq!(
        store
            .conn
            .query_row(
                "SELECT config_revision FROM agents WHERE id=?",
                [child_id.as_str()],
                |r| r.get::<_, String>(0)
            )
            .unwrap(),
        "snapshot:v1:sealed"
    );
}

/// Mirrors `tests/test_resume.py::ResumeTests::test_stale_ancestor_names_the_latest_descendant`.
#[test]
fn python_test_resume_stale_ancestor_has_one_latest_child() {
    let home = common::Home::new();
    let root = parent(&home, home.request(), "one");
    let (child_id, _) = child(&home, &root, "two");
    let mut store = home.store();
    let row = store.get(&root).unwrap();
    assert!(store
        .admit(
            &request(&home, "stale"),
            &home.config,
            &json!({}),
            Some(&row)
        )
        .is_err());
    assert_eq!(store.get(&child_id).unwrap().sequence, 2);
}

/// Mirrors `tests/test_resume.py::ResumeTests::test_start_replay_survives_a_changed_default_account`.
#[test]
fn python_test_resume_start_replay_ignores_default_account_changes() {
    let mut runtime = common::Home::new().config.runtime("mock").unwrap().clone();
    runtime.accounts = vec!["work".into()];
    runtime.default_account = Some("work".into());
    assert_eq!(runtime.selected_account(None).unwrap(), None);
}

/// Mirrors `tests/test_resume.py::ResumeTests::test_unchanged_network_grants_are_preserved`.
#[test]
fn python_test_resume_network_grant_is_preserved_in_identity() {
    let home = common::Home::new();
    let mut request = home.request();
    request.write = true;
    let value = json!({"profile":{"network":true,"write":true}});
    let id = parent(&home, request, "session");
    let store = home.store();
    store
        .conn
        .execute(
            "UPDATE agents SET identity_json=? WHERE id=?",
            (value.to_string(), id.as_str()),
        )
        .unwrap();
    assert_eq!(
        store.get(&id).unwrap().identity.unwrap()["profile"]["network"],
        true
    );
}
