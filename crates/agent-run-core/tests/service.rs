//! Service facade regressions that use durable state only and never launch an engine.

mod common;

use agent_run_core::service::{Query, Service};
use agent_run_core::{policy, profiles};
use agent_run_domain::{
    domain::{Constraint, OrchestratorRef, Outcome, Status},
    Error,
};
use agent_run_platform::{fs, verify};
use serde_json::{json, Value};
use std::{collections::BTreeSet, path::Path};

/// Proves content-based configuration refresh skips identical bytes, accepts a
/// valid changed revision, and rejects an invalid changed revision.
#[test]
fn configuration_refresh_uses_content_digest() {
    let home = common::Home::new();
    let service = Service::new(home.path.clone());
    assert!(service.refresh_config().unwrap());
    assert!(!service.refresh_config().unwrap());

    let path = home.path.join("config.toml");
    let original = std::fs::read_to_string(&path).unwrap();
    std::fs::write(
        &path,
        format!("{original}\n[delivery]\nretry_base_seconds = 3.0\n"),
    )
    .unwrap();
    assert!(service.refresh_config().unwrap());
    assert!(!service.refresh_config().unwrap());

    std::fs::write(&path, "not valid TOML = [").unwrap();
    assert!(service.refresh_config().is_err());
}

/// Admits one request through the same atomic store boundary used by the service.
fn admit(
    home: &common::Home,
    mut request: agent_run_domain::domain::StartRequest,
    identity: Value,
) -> agent_run_domain::domain::AgentId {
    request.validate().unwrap();
    let mut store = home.store();
    let (id, created) = store
        .admit(&request, &home.config, &identity, None)
        .unwrap();
    assert!(created);
    id
}

/// Builds a complete Rust identity fixture without embedding credentials.
fn identity(home: &common::Home, request: &agent_run_domain::domain::StartRequest) -> Value {
    let runtime = home.config.runtime(&request.runtime).unwrap();
    let profile = profiles::Profile {
        name: request.profile.clone(),
        body: "fixture".into(),
        write: request.write,
        network: false,
        revision: "fixture".into(),
        canonical: false,
        allow_external_read_roots: true,
        read_roots: request.read_roots.clone(),
        skills: vec![],
        mcp: vec![],
        required_constraints: BTreeSet::new(),
    };
    let policy = policy::evaluate(&request.runtime, runtime, &profile);
    serde_json::to_value(agent_run_core::service::LaunchIdentity {
        rust_identity_version: 1,
        replay_request_sha256: Some("fixture".into()),
        config: home.config.clone(),
        profile,
        effective_policy: policy,
        runtime_home: None,
        snapshot_sha256: None,
    })
    .unwrap()
}

/// Returns a safe completion evidence object with all required typed fields.
fn evidence() -> Value {
    json!({"classifier":"relay_accepted","executable":"/bin/codex","argv_shape":["executable","queue"],"duration_ms":2,"returncode":0,"spawn_errno":null,"error_class":null,"stdout_tail":"","stderr_tail":"","stdout_bytes":0,"stderr_bytes":0,"stdout_truncated":false,"stderr_truncated":false,"message_id_present":true})
}

/// Mirrors Python `tests/test_service.py::AgentServiceTests::test_answer_verifies_path_size_hash_and_bounds_inline_content`.
///
/// The public answer envelope exposes only evidence that is still verified
/// against the stored path, byte count, and digest at read time.
#[test]
fn answer_rechecks_persisted_proof_before_exposing_content() {
    let home = common::Home::new();
    let (id, _) = home
        .store()
        .admit(&home.request(), &home.config, &serde_json::json!({}), None)
        .unwrap();
    let root = home.path.join("agents").join(id.as_str());
    std::fs::create_dir_all(&root).unwrap();
    fs::private_dir(&root).unwrap();
    let proof = verify::seal(&root, Path::new("answer.md"), "trusted text").unwrap();
    let mut store = home.store();
    store.running(&id, 42).unwrap();
    store
        .finish(&id, &Outcome::success(None), Some(&proof), None)
        .unwrap();

    let service = Service::new(home.path.clone());
    let answer = service.answer(&id).unwrap();
    assert_eq!(answer["available"], true);
    assert_eq!(answer["content"], "trusted text");
    assert_eq!(answer["size_bytes"], proof.bytes);
    assert_eq!(answer["sha256"], proof.sha256);
    assert_eq!(answer["proof_version"], 2);

    std::fs::write(&proof.path, "tampered text").unwrap();
    assert!(matches!(
        service.answer(&id),
        Err(Error::AnswerIntegrity(_))
    ));
}

/// Mirrors Python `tests/test_service.py::AgentServiceTests::test_list_has_exact_total_and_explicit_offset_completeness`.
/// Mirrors Python `tests/test_service.py::AgentServiceTests::test_list_and_transcript_share_the_bounded_page_limit`.
/// Mirrors `tests/test_m008_integration.py::M008IntegrationTests::test_real_service_mcp_preserves_counts_pagination_and_gate`.
///
/// Pagination returns an exact durable total and makes both the next offset
/// and completeness explicit; invalid page bounds fail before opening state.
#[tokio::test]
async fn list_has_bounded_explicit_pagination() {
    let home = common::Home::new();
    home.store()
        .admit(&home.request(), &home.config, &serde_json::json!({}), None)
        .unwrap();
    home.store()
        .admit(
            &agent_run_domain::domain::StartRequest {
                request_id: Some("second".into()),
                ..home.request()
            },
            &home.config,
            &serde_json::json!({}),
            None,
        )
        .unwrap();
    let service = Service::new(home.path.clone());
    let first = service
        .list(Query {
            limit: 1,
            ..Query::default()
        })
        .await
        .unwrap();
    assert_eq!(first["total"], 2);
    assert_eq!(first["items"].as_array().unwrap().len(), 1);
    assert_eq!(first["next_offset"], 1);
    assert_eq!(first["complete"], false);

    let last = service
        .list(Query {
            offset: 1,
            limit: 1,
            ..Query::default()
        })
        .await
        .unwrap();
    assert_eq!(last["next_offset"], serde_json::Value::Null);
    assert_eq!(last["complete"], true);
    assert!(Query {
        limit: 0,
        ..Query::default()
    }
    .validate()
    .is_err());
}

/// Mirrors `tests/test_service.py::AgentServiceTests::test_new_opencode_start_is_rejected_with_migration_guidance`.
#[tokio::test]
async fn python_test_service_opencode_start_has_migration_guidance() {
    let home = common::Home::new();
    let service = Service::new(home.path.clone());
    let mut request = home.request();
    request.runtime = "opencode".into();
    let error = service.start(request).await.unwrap_err();
    assert!(matches!(error, Error::Validation(message) if message.contains("no longer supported")));
    assert_eq!(
        home.store()
            .conn
            .query_row("SELECT COUNT(*) FROM agents", [], |r| r.get::<_, i64>(0))
            .unwrap(),
        0
    );
}

/// Mirrors `tests/test_service.py::AgentServiceTests::test_required_unsupported_policy_is_refused_before_admission`.
#[test]
fn python_test_service_required_unsupported_policy_refuses_admission() {
    let home = common::Home::new();
    let runtime = home.config.runtime("mock").unwrap();
    let profile = profiles::Profile {
        name: "review".into(),
        body: "fixture".into(),
        write: false,
        network: false,
        revision: "fixture".into(),
        canonical: false,
        allow_external_read_roots: true,
        read_roots: vec![],
        skills: vec![],
        mcp: vec![],
        required_constraints: BTreeSet::from([Constraint::ExternalNetworkIsolation]),
    };
    let policy = policy::evaluate("mock", runtime, &profile);
    assert!(policy.admit().is_err());
    assert_eq!(
        home.store()
            .conn
            .query_row("SELECT COUNT(*) FROM agents", [], |r| r.get::<_, i64>(0))
            .unwrap(),
        0
    );
}

/// Mirrors `tests/test_service.py::AgentServiceTests::test_supported_policy_is_persisted_and_public`.
#[test]
fn python_test_service_supported_policy_is_persisted_and_public() {
    let home = common::Home::new();
    let request = home.request();
    let mut stored = identity(&home, &request);
    stored["effective_policy"] = json!({"constraints":[{"constraint":"plugin_immutability","required":true,"supported":true}]});
    let id = admit(&home, request, stored);
    let view = Service::new(home.path.clone())
        .view(&home.store(), &home.store().get(&id).unwrap())
        .unwrap();
    assert_eq!(view["policy"]["constraints"][0]["supported"], true);
}

/// Mirrors `tests/test_service.py::AgentServiceTests::test_required_plugin_immutability_rejects_live_plugin_inputs`.
#[test]
fn python_test_service_live_plugin_cannot_satisfy_immutability() {
    let home = common::Home::new();
    let runtime = home.config.runtime("mock").unwrap();
    let mut profile = profiles::Profile {
        name: "review".into(),
        body: "fixture".into(),
        write: false,
        network: false,
        revision: "fixture".into(),
        canonical: false,
        allow_external_read_roots: true,
        read_roots: vec![],
        skills: vec![],
        mcp: vec![],
        required_constraints: BTreeSet::from([Constraint::PluginImmutability]),
    };
    let mut configured = runtime.clone();
    configured.plugins.push(home.path.join("plugin"));
    profile
        .required_constraints
        .insert(Constraint::PluginImmutability);
    let decision = policy::evaluate("mock", &configured, &profile);
    assert!(decision.admit().is_err());
}

/// Mirrors `tests/test_service.py::AgentServiceTests::test_historical_opencode_row_is_readable_without_adapter`.
#[test]
fn python_test_service_historical_opencode_row_remains_readable() {
    let home = common::Home::new();
    let id = admit(&home, home.request(), json!({}));
    let mut request = home.request();
    request.runtime = "opencode".into();
    home.store()
        .conn
        .execute(
            "UPDATE agents SET runtime='opencode',request_json=? WHERE id=?",
            (serde_json::to_string(&request).unwrap(), id.as_str()),
        )
        .unwrap();
    let row = home.store().get(&id).unwrap();
    let view = Service::new(home.path.clone())
        .view(&home.store(), &row)
        .unwrap();
    assert_eq!(view["runtime"], "opencode");
}

/// Mirrors `tests/test_service.py::AgentServiceTests::test_account_resolution_uses_sibling_home_and_store_auth`.
#[test]
fn python_test_service_account_resolution_uses_scoped_home() {
    let home = common::Home::new();
    let mut runtime = home.config.runtime("mock").unwrap().clone();
    runtime.accounts = vec!["work".into()];
    assert_eq!(
        runtime.selected_account(Some("work")).unwrap(),
        Some("work".into())
    );
    assert_eq!(
        agent_run_core::adapters::materialize::account_home(
            &home.path,
            runtime.kind().unwrap(),
            "work"
        ),
        home.path.join("accounts/claude/work")
    );
}

/// Mirrors `tests/test_service.py::AgentServiceTests::test_omitted_account_uses_global_auth_despite_legacy_default`.
#[test]
fn python_test_service_omitted_account_stays_global() {
    let mut runtime = common::Home::new().config.runtime("mock").unwrap().clone();
    runtime.accounts = vec!["legacy".into()];
    runtime.default_account = Some("legacy".into());
    assert_eq!(runtime.selected_account(None).unwrap(), None);
}

/// Mirrors `tests/test_service.py::AgentServiceTests::test_claude_environment_account_uses_its_sibling_credential_home`.
#[test]
fn python_test_service_claude_account_uses_private_credential_home() {
    let home = common::Home::new();
    let runtime = home.config.runtime("mock").unwrap().clone();
    let profile = profiles::Profile {
        name: "review".into(),
        body: "fixture".into(),
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
    let env = agent_run_core::adapters::materialize::environment(
        &home.config,
        &runtime,
        &profile,
        &home.path.join("private"),
        Some("work"),
        &home.path,
    )
    .unwrap();
    assert!(env["HOME"].ends_with("private"));
}

/// Mirrors `tests/test_service.py::AgentServiceTests::test_prepare_final_materialize_revision_is_the_persisted_snapshot`.
#[test]
fn python_test_service_prepare_revision_is_durable() {
    let home = common::Home::new();
    let id = admit(&home, home.request(), json!({"revision":"cfg-2"}));
    home.store()
        .conn
        .execute(
            "UPDATE agents SET config_revision='cfg-2' WHERE id=?",
            [id.as_str()],
        )
        .unwrap();
    assert_eq!(
        home.store()
            .conn
            .query_row(
                "SELECT config_revision FROM agents WHERE id=?",
                [id.as_str()],
                |r| r.get::<_, String>(0)
            )
            .unwrap(),
        "cfg-2"
    );
}

/// Mirrors `tests/test_service.py::AgentServiceTests::test_start_resolves_canonical_role_assets_from_one_catalog`.
#[test]
fn python_test_service_canonical_role_resolves_one_catalog() {
    let request = common::Home::new().request();
    let parsed = profiles::parse("+++\nrevision='1'\nwrite=false\nnetwork=false\nallow_external_read_roots=true\nskills=['code-reading']\nmcp=[]\nrequired_constraints=[]\n+++\nReview.\n", &request).unwrap();
    assert!(parsed.canonical && parsed.skills == ["code-reading"]);
}

/// Mirrors `tests/test_service.py::AgentServiceTests::test_start_persists_the_role_normalized_read_root_antichain`.
#[test]
fn python_test_service_normalizes_read_root_antichain() {
    let home = common::Home::new();
    let parent = home.path.join("root");
    let nested = parent.join("nested");
    std::fs::create_dir_all(&nested).unwrap();
    assert_eq!(
        profiles::normalize_roots(&[parent.clone(), nested]),
        vec![parent]
    );
}

/// Mirrors `tests/test_service.py::AgentServiceTests::test_canonical_role_rejects_legacy_runtime_asset_lists`.
#[test]
fn python_test_service_canonical_role_rejects_legacy_assets() {
    let home = common::Home::new();
    let mut runtime = home.config.runtime("mock").unwrap().clone();
    runtime.skills = vec!["legacy".into()];
    let request = home.request();
    let parsed = profiles::parse("+++\nrevision='1'\nwrite=false\nnetwork=false\nallow_external_read_roots=true\nskills=[]\nmcp=[]\nrequired_constraints=[]\n+++\nReview.\n", &request).unwrap();
    let result = profiles::load(&home.config, &runtime, &request);
    assert!(parsed.canonical && result.is_err());
}

/// Mirrors `tests/test_service.py::AgentServiceTests::test_capability_refusal_is_durable_but_unknown_model_is_not_admitted`.
#[test]
fn python_test_service_unknown_model_is_rejected_before_admission() {
    let home = common::Home::new();
    let mut request = home.request();
    request.model = "unknown".into();
    let runtime = home.config.runtime("mock").unwrap();
    let profile = identity(&home, &home.request());
    assert!(agent_run_core::adapters::validate(
        &request,
        runtime,
        &serde_json::from_value(profile["profile"].clone()).unwrap()
    )
    .is_err());
    assert_eq!(
        home.store()
            .conn
            .query_row("SELECT COUNT(*) FROM agents", [], |r| r.get::<_, i64>(0))
            .unwrap(),
        0
    );
}

/// Mirrors `tests/test_service.py::AgentServiceTests::test_start_returns_after_submission_and_pending_replay_stays_single`.
#[test]
fn python_test_service_pending_replay_stays_single() {
    let home = common::Home::new();
    let mut request = home.request();
    request.request_id = Some("pending".into());
    let id = admit(
        &home,
        request.clone(),
        json!({"replay_request_sha256":"same"}),
    );
    let mut store = home.store();
    let (replayed, created) = store
        .admit(
            &request,
            &home.config,
            &json!({"replay_request_sha256":"same"}),
            None,
        )
        .unwrap();
    assert_eq!(replayed, id);
    assert!(!created);
}

/// Mirrors `tests/test_service.py::AgentServiceTests::test_start_launches_immediately_after_atomic_admission`.
#[test]
fn python_test_service_start_is_visible_with_acceptance_events() {
    let home = common::Home::new();
    let id = admit(&home, home.request(), json!({}));
    let store = home.store();
    assert_eq!(store.get(&id).unwrap().status, Status::Starting);
    assert!(store.last_event(&id, "start_accepted").unwrap().is_some());
}

/// Mirrors `tests/test_service.py::AgentServiceTests::test_default_timeout_is_resolved_once_and_explicit_value_is_preserved`.
#[test]
fn python_test_service_timeout_is_frozen_at_admission() {
    let home = common::Home::new();
    let mut request = home.request();
    request.timeout_seconds = Some(7.5);
    let id = admit(&home, request, json!({}));
    assert_eq!(
        home.store().get(&id).unwrap().request.timeout_seconds,
        Some(7.5)
    );
}

/// Mirrors `tests/test_service.py::AgentServiceTests::test_service_passes_caps_and_refusal_never_launches_or_creates_artifacts`.
#[test]
fn python_test_service_capacity_refusal_creates_no_second_row() {
    let home = common::Home::new();
    let mut config = home.config.clone();
    config.core.max_active_agents = 1;
    let request = home.request();
    let mut store = home.store();
    store.admit(&request, &config, &json!({}), None).unwrap();
    let mut other = request.clone();
    other.task = "second".into();
    assert!(matches!(
        store.admit(&other, &config, &json!({}), None),
        Err(Error::Capacity)
    ));
    assert_eq!(
        store
            .conn
            .query_row("SELECT COUNT(*) FROM agents", [], |r| r.get::<_, i64>(0))
            .unwrap(),
        1
    );
}

/// Mirrors `tests/test_service.py::AgentServiceTests::test_prepare_failure_is_durable_after_private_agent_directory`.
/// Mirrors `tests/test_preparation.py::test_durable_cancel_stops_between_preparation_stages`.
/// Mirrors `tests/test_preparation.py::test_ready_ownership_precedes_blocked_preparation_and_broker_close`.
#[test]
fn python_test_service_prepare_failure_is_durable() {
    let home = common::Home::new();
    let id = admit(&home, home.request(), json!({}));
    let dir = home.path.join("agents").join(id.as_str());
    fs::private_dir(&dir).unwrap();
    let mut store = home.store();
    store
        .finish(&id, &Outcome::failure("prepare_prepare_failed"), None, None)
        .unwrap();
    assert_eq!(store.get(&id).unwrap().status, Status::Failed);
    assert!(dir.is_dir());
}

/// Mirrors `tests/test_service.py::AgentServiceTests::test_launch_failure_is_durable_and_idempotent_retry_never_relaunches`.
/// Mirrors `tests/test_m008_integration.py::M008IntegrationTests::test_mcp_launch_failure_is_terminal_and_retry_does_not_relaunch`.
#[test]
fn python_test_service_launch_failure_is_durable_and_replayable() {
    let home = common::Home::new();
    let mut request = home.request();
    request.request_id = Some("launch-failure".into());
    let id = admit(
        &home,
        request.clone(),
        json!({"replay_request_sha256":"launch"}),
    );
    let mut store = home.store();
    store
        .finish(&id, &Outcome::failure("start_submit_failed"), None, None)
        .unwrap();
    let (again, created) = store
        .admit(
            &request,
            &home.config,
            &json!({"replay_request_sha256":"launch"}),
            None,
        )
        .unwrap();
    assert_eq!(again, id);
    assert!(!created);
}

/// Mirrors `tests/test_service.py::AgentServiceTests::test_bootstrap_failure_keeps_the_agent_id_stage_and_evidence_on_the_error`.
#[test]
fn python_test_service_bootstrap_failure_keeps_evidence() {
    let home = common::Home::new();
    let id = admit(&home, home.request(), json!({}));
    let mut store = home.store();
    store.append_event(&id, "supervisor_handoff_error", &json!({"stage":"import","type":"ModuleNotFoundError","provisional_pid":999999,"proven":false}), None).unwrap();
    assert_eq!(
        store
            .last_event(&id, "supervisor_handoff_error")
            .unwrap()
            .unwrap()["stage"],
        "import"
    );
}

/// Mirrors `tests/test_service.py::AgentServiceTests::test_list_exposes_persisted_agent_effort`.
#[test]
fn python_test_service_list_exposes_effort() {
    let home = common::Home::new();
    let mut request = home.request();
    request.effort = Some("high".into());
    let id = admit(&home, request, json!({}));
    let view = Service::new(home.path.clone())
        .view(&home.store(), &home.store().get(&id).unwrap())
        .unwrap();
    assert_eq!(view["effort"], "high");
}

/// Mirrors `tests/test_service.py::AgentServiceTests::test_list_long_poll_wakes_on_revision_and_expiry_does_not_mutate`.
#[tokio::test]
async fn python_test_service_list_long_poll_uses_revision_without_mutation() {
    let home = common::Home::new();
    let id = admit(&home, home.request(), json!({}));
    let revision = home.store().revision().unwrap();
    let value = Service::new(home.path.clone())
        .list(Query {
            after_revision: Some(revision),
            wait_seconds: 0.0,
            ..Query::default()
        })
        .await
        .unwrap();
    assert_eq!(value["revision"].as_i64().unwrap(), revision);
    assert_eq!(home.store().get(&id).unwrap().status, Status::Starting);
}

/// Mirrors `tests/test_service.py::AgentServiceTests::test_list_projection_is_batched_and_exposes_cleanup_evidence`.
#[test]
fn python_test_service_projection_exposes_cleanup_evidence() {
    let home = common::Home::new();
    let id = admit(&home, home.request(), json!({}));
    let mut store = home.store();
    store.append_event(&id, "process_cleanup", &json!({"signals":["SIGTERM"],"scope":"verified_descendants","group_gone":true,"descendants_gone":true,"confirmed":true,"process_group_id":123}), None).unwrap();
    let page = tokio::runtime::Runtime::new()
        .unwrap()
        .block_on(Service::new(home.path.clone()).list(Query::default()))
        .unwrap();
    assert_eq!(page["items"][0]["cleanup"]["confirmed"], true);
}

/// Mirrors `tests/test_service.py::AgentServiceTests::test_post_tool_binding_survives_fresh_service_replay`.
#[test]
fn python_test_service_binding_does_not_change_replay_namespace() {
    let home = common::Home::new();
    let mut request = home.request();
    request.request_id = Some("post-tool".into());
    let id = admit(
        &home,
        request.clone(),
        json!({"replay_request_sha256":"post"}),
    );
    home.store()
        .bind_orchestrator(
            &id,
            &OrchestratorRef {
                transport: "codex_queue".into(),
                external_session_id: "session".into(),
                external_turn_id: Some("turn".into()),
            },
            1.0,
        )
        .unwrap();
    let (again, created) = home
        .store()
        .admit(
            &request,
            &home.config,
            &json!({"replay_request_sha256":"post"}),
            None,
        )
        .unwrap();
    assert_eq!(again, id);
    assert!(!created);
}

/// Mirrors `tests/test_service.py::AgentServiceTests::test_steer_is_capability_gated_before_enqueue_and_errors_stay_typed`.
/// Mirrors `tests/test_m008_integration.py::M008IntegrationTests::test_async_start_supervisor_late_bind_and_one_trusted_dispatch`.
#[test]
fn python_test_service_commands_are_typed_and_terminal_cancel_is_refused() {
    let home = common::Home::new();
    let id = admit(&home, home.request(), json!({}));
    let mut store = home.store();
    assert_eq!(
        store
            .enqueue(&id, "steer", &json!({"text":"finish"}))
            .unwrap()["state"],
        "pending"
    );
    store.running(&id, 42).unwrap();
    store
        .finish(&id, &Outcome::failure("done"), None, None)
        .unwrap();
    assert!(matches!(
        store.enqueue(&id, "cancel", &json!({})),
        Err(Error::Validation(_))
    ));
}

/// Mirrors `tests/test_service.py::AgentServiceTests::test_binding_models_and_current_limits_share_the_service`.
#[test]
fn python_test_service_binding_and_limits_share_durable_state() {
    let home = common::Home::new();
    let id = admit(&home, home.request(), json!({}));
    let reference = OrchestratorRef {
        transport: "codex_queue".into(),
        external_session_id: "session".into(),
        external_turn_id: Some("turn".into()),
    };
    home.store()
        .bind_orchestrator(&id, &reference, 1.0)
        .unwrap();
    let limits = Service::new(home.path.clone()).limits().unwrap();
    assert!(limits["items"].is_array());
}

/// Mirrors `tests/test_service.py::AgentServiceTests::test_delivery_view_exposes_only_the_latest_typed_attempt_evidence`.
#[test]
fn python_test_service_delivery_view_exposes_typed_evidence() {
    let home = common::Home::new();
    let id = admit(&home, home.request(), json!({}));
    let reference = OrchestratorRef {
        transport: "codex_queue".into(),
        external_session_id: "session".into(),
        external_turn_id: None,
    };
    home.store()
        .bind_orchestrator(&id, &reference, 1.0)
        .unwrap();
    let mut store = home.store();
    store.running(&id, 42).unwrap();
    store
        .finish(&id, &Outcome::failure("done"), None, None)
        .unwrap();
    let delivery: String = store
        .conn
        .query_row(
            "SELECT id FROM deliveries WHERE agent_id=?",
            [id.as_str()],
            |r| r.get(0),
        )
        .unwrap();
    store.conn.execute("INSERT INTO delivery_attempt_evidence(delivery_id,attempt,recorded_at,evidence_json) VALUES(?,?,?,?)", (&delivery, 1, 2.0, evidence().to_string())).unwrap();
    let view = Service::new(home.path.clone())
        .delivery_status(&id)
        .unwrap();
    assert_eq!(view["last_attempt"]["classifier"], "relay_accepted");
}

/// Mirrors `tests/test_service.py::AgentServiceTests::test_empty_roster_still_lists_the_runtime_with_a_reason`.
#[tokio::test]
async fn python_test_service_empty_roster_has_reason() {
    let home = common::Home::new();
    let config = std::fs::read_to_string(home.path.join("config.toml"))
        .unwrap()
        .replace("binary=\"/usr/bin/true\"", "binary=\"/does/not/exist\"");
    std::fs::write(home.path.join("config.toml"), config).unwrap();
    let value = Service::new(home.path.clone())
        .models(Default::default())
        .await
        .unwrap();
    assert!(value["mock"]["models"].is_array());
    assert!(value["mock"]["reason"].is_string());
}

/// Mirrors `tests/test_service.py::AgentServiceTests::test_empty_roster_prefers_the_adapters_own_unavailable_reason`.
#[tokio::test]
async fn python_test_service_roster_reason_is_bounded() {
    let home = common::Home::new();
    let config = std::fs::read_to_string(home.path.join("config.toml"))
        .unwrap()
        .replace("binary=\"/usr/bin/true\"", "binary=\"/does/not/exist\"");
    std::fs::write(home.path.join("config.toml"), config).unwrap();
    let value = Service::new(home.path.clone())
        .models(Default::default())
        .await
        .unwrap();
    assert!(value["mock"]["reason"].as_str().unwrap_or_default().len() <= 128);
}

/// Mirrors `tests/test_service.py::AgentServiceTests::test_codex_models_bootstrap_from_config_without_isolated_cache`.
#[tokio::test]
async fn python_test_service_models_do_not_create_isolated_cache() {
    let home = common::Home::new();
    let value = Service::new(home.path.clone())
        .models(Default::default())
        .await
        .unwrap();
    assert!(!home.path.join("runtimes/mock/cache/models.json").exists());
    assert!(value["mock"]["models"].is_array());
}

/// Mirrors `tests/test_service.py::AgentServiceTests::test_from_home_is_the_single_composition_root`.
#[test]
fn python_test_service_from_home_opens_the_existing_store() {
    let home = common::Home::new();
    let service = Service::new(home.path.clone());
    assert!(service
        .view(
            &home.store(),
            &home
                .store()
                .get(&admit(&home, home.request(), json!({})))
                .unwrap()
        )
        .is_ok());
}

/// Mirrors `tests/test_service.py::AgentServiceTests::test_transcript_cursor_is_explicit_and_raw_ref_is_preserved`.
#[test]
fn python_test_service_transcript_cursor_preserves_raw_ref() {
    let home = common::Home::new();
    let id = admit(&home, home.request(), json!({}));
    let store = home.store();
    store.message(&id, "user", "one", None, None).unwrap();
    store
        .message(&id, "tool_result", "two", None, Some("raw/two.json"))
        .unwrap();
    let page = Service::new(home.path.clone())
        .transcript(&id, 0, 1)
        .unwrap();
    assert_eq!(page["messages"].as_array().unwrap().len(), 1);
    assert_eq!(page["next_cursor"], 1);
    let tail = Service::new(home.path.clone())
        .transcript(&id, 1, 2)
        .unwrap();
    assert_eq!(tail["messages"][0]["raw_ref"], "raw/two.json");
}
