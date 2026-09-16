//! Ports the remaining durable-store behaviors from the Python state-store suite.

mod common;

use agent_run_domain::{
    domain::{OrchestratorRef, Outcome, StartRequest, Status},
    Error,
};
use agent_run_store::Store;
use serde_json::{json, Value};
use std::sync::{Arc, Barrier};
use std::thread;

fn admitted(
    home: &common::Home,
    request: &StartRequest,
) -> (Store, agent_run_domain::domain::AgentId) {
    let mut store = home.store();
    let (id, created) = store
        .admit(request, &home.config, &json!({}), None)
        .unwrap();
    assert!(created);
    (store, id)
}

fn admitted_with_config(
    home: &common::Home,
    request: &StartRequest,
    config: &agent_run_config::config::Config,
) -> (Store, agent_run_domain::domain::AgentId) {
    let mut store = home.store();
    let (id, created) = store.admit(request, config, &json!({}), None).unwrap();
    assert!(created);
    (store, id)
}

/// Mirrors `tests/test_state_store.py::StateStoreTests::test_concurrent_same_key_admission_creates_one_owned_row`.
#[test]
fn concurrent_same_request_id_creates_one_row() {
    let home = common::Home::new();
    let mut request = home.request();
    request.request_id = Some("concurrent-admission".into());
    let barrier = Arc::new(Barrier::new(2));
    let workers: Vec<_> = (0..2)
        .map(|_| {
            let path = home.path.clone();
            let config = home.config.clone();
            let request = request.clone();
            let barrier = Arc::clone(&barrier);
            thread::spawn(move || {
                let mut store = Store::open(&path).unwrap();
                barrier.wait();
                store.admit(&request, &config, &json!({}), None)
            })
        })
        .collect();
    let results: Vec<_> = workers
        .into_iter()
        .map(|worker| worker.join().unwrap())
        .collect();
    assert_eq!(results.iter().filter(|result| result.is_ok()).count(), 2);
    let ids: Vec<_> = results
        .iter()
        .map(|result| result.as_ref().unwrap().0.as_str())
        .collect();
    assert_eq!(ids[0], ids[1]);
    assert_eq!(
        results
            .iter()
            .filter(|result| result.as_ref().unwrap().1)
            .count(),
        1
    );
}

/// Mirrors `tests/test_state_store.py::StateStoreTests::test_unbound_request_id_is_globally_concurrent_and_exact`.
#[test]
fn unbound_request_replay_remains_exact_after_late_binding() {
    let home = common::Home::new();
    let mut request = home.request();
    request.request_id = Some("shared-request".into());
    let (mut store, id) = admitted(&home, &request);
    let reference = OrchestratorRef {
        transport: "codex_queue".into(),
        external_session_id: "late-session".into(),
        external_turn_id: Some("turn-1".into()),
    };
    store.bind_orchestrator(&id, &reference, 3.0).unwrap();
    let replay = store
        .admit(&request, &home.config, &json!({}), None)
        .unwrap();
    assert_eq!(replay.0, id);
    assert!(!replay.1);
    let mut conflicting = request.clone();
    conflicting.task = "different".into();
    assert!(matches!(
        store.admit(&conflicting, &home.config, &json!({}), None),
        Err(Error::Conflict)
    ));
}

/// Mirrors `tests/test_state_store.py::StateStoreTests::test_every_semantic_request_field_rejects_same_key_reuse`.
#[test]
fn every_semantic_request_field_conflicts_on_replay() {
    let home = common::Home::new();
    let mut config = home.config.clone();
    config.core.max_active_agents = 100;
    let other = home.path.join("other");
    std::fs::create_dir(&other).unwrap();
    let variants = [
        ("runtime", json!("other-runtime")),
        ("model", json!("other-model")),
        ("profile", json!("other-profile")),
        ("task", json!("other task")),
        ("workdir", json!(other)),
        ("write", json!(true)),
        ("effort", json!("high")),
        ("timeout_seconds", json!(481.0)),
        ("read_roots", json!([other])),
        ("output_schema", json!({"type":"object"})),
        ("fast", json!(true)),
        ("account", json!("other-account")),
    ];
    for (field, value) in variants {
        let mut request = home.request();
        request.request_id = Some(format!("semantic-{field}"));
        let (mut store, _) = admitted_with_config(&home, &request, &config);
        let mut changed = serde_json::to_value(&request).unwrap();
        changed[field] = value;
        let changed: StartRequest = serde_json::from_value(changed).unwrap();
        assert!(
            matches!(
                store.admit(&changed, &config, &json!({}), None),
                Err(Error::Conflict)
            ),
            "{field}"
        );
    }
}

/// Mirrors `tests/test_state_store.py::StateStoreTests::test_same_request_id_is_distinct_across_orchestrator_namespaces`.
#[test]
fn request_ids_are_scoped_to_orchestrator_namespaces() {
    let home = common::Home::new();
    let refs = [None, Some("namespace-a"), Some("namespace-b")];
    let mut ids = Vec::new();
    for session in refs {
        let mut request = home.request();
        request.request_id = Some("shared-key".into());
        request.orchestrator = session.map(|external_session_id| OrchestratorRef {
            transport: "codex_queue".into(),
            external_session_id: external_session_id.into(),
            external_turn_id: None,
        });
        let (store, id) = admitted(&home, &request);
        ids.push(id);
        drop(store);
    }
    assert_eq!(ids.len(), 3);
    assert_ne!(ids[0], ids[1]);
    assert_ne!(ids[1], ids[2]);
}

/// Mirrors `tests/test_state_store.py::StateStoreTests::test_session_request_id_is_idempotent_and_binding_is_immutable`.
#[test]
fn session_replay_is_idempotent_and_binding_is_immutable() {
    let home = common::Home::new();
    let reference = OrchestratorRef {
        transport: "codex_queue".into(),
        external_session_id: "session-1".into(),
        external_turn_id: Some("turn-1".into()),
    };
    let mut request = home.request();
    request.request_id = Some("request-1".into());
    request.orchestrator = Some(reference.clone());
    let (mut store, id) = admitted(&home, &request);
    let replay = store
        .admit(&request, &home.config, &json!({}), None)
        .unwrap();
    assert_eq!(replay.0, id);
    assert!(!replay.1);
    assert_eq!(
        store.bind_orchestrator(&id, &reference, 2.0).unwrap(),
        store
            .find_orchestrator_session(&reference)
            .unwrap()
            .unwrap()
    );
    let different = OrchestratorRef {
        transport: "codex_queue".into(),
        external_session_id: "different-session".into(),
        external_turn_id: None,
    };
    assert!(matches!(
        store.bind_orchestrator(&id, &different, 3.0),
        Err(Error::Validation(_))
    ));
}

/// Mirrors `tests/test_state_store.py::StateStoreTests::test_request_json_covers_fields_and_legacy_defaults_replay_exactly`.
#[test]
fn request_json_round_trips_all_fields_and_legacy_defaults() {
    let home = common::Home::new();
    let mut request = home.request();
    request.request_id = Some("legacy-fast".into());
    let (mut store, id) = admitted(&home, &request);
    let raw: String = store
        .conn
        .query_row(
            "SELECT request_json FROM agents WHERE id=?",
            [id.as_str()],
            |row| row.get(0),
        )
        .unwrap();
    let value: Value = serde_json::from_str(&raw).unwrap();
    assert_eq!(
        value.as_object().unwrap().len(),
        serde_json::to_value(&request)
            .unwrap()
            .as_object()
            .unwrap()
            .len()
    );
    let mut legacy = value.as_object().unwrap().clone();
    legacy.remove("fast");
    legacy.remove("required_constraints");
    let legacy = Value::Object(legacy).to_string();
    store
        .conn
        .execute(
            "UPDATE agents SET request_json=? WHERE id=?",
            (&legacy, id.as_str()),
        )
        .unwrap();
    let replay = store
        .admit(&request, &home.config, &json!({}), None)
        .unwrap();
    assert_eq!(replay.0, id);
    let mut changed = request.clone();
    changed.fast = true;
    assert!(matches!(
        store.admit(&changed, &home.config, &json!({}), None),
        Err(Error::Conflict)
    ));
}

/// Mirrors `tests/test_state_store.py::StateStoreTests::test_unresolved_timeout_is_never_persisted`.
#[test]
fn admitted_requests_always_persist_a_resolved_timeout() {
    let home = common::Home::new();
    let request = home.request();
    let (store, id) = admitted(&home, &request);
    let timeout: f64 = store
        .conn
        .query_row(
            "SELECT timeout_seconds FROM agents WHERE id=?",
            [id.as_str()],
            |row| row.get(0),
        )
        .unwrap();
    assert!(timeout.is_finite() && timeout > 0.0);
}

/// Mirrors `tests/test_state_store.py::StateStoreTests::test_session_lookup_and_agent_filter_are_read_only_and_composable`.
#[test]
fn session_lookup_and_agent_listing_are_read_only_and_composable() {
    let home = common::Home::new();
    let reference = OrchestratorRef {
        transport: "codex_queue".into(),
        external_session_id: "session-a".into(),
        external_turn_id: Some("turn-a".into()),
    };
    let mut first_request = home.request();
    first_request.orchestrator = Some(reference.clone());
    let (mut store, first) = admitted(&home, &first_request);
    let mut second_request = home.request();
    second_request.task = "second".into();
    second_request.orchestrator = Some(OrchestratorRef {
        external_turn_id: Some("turn-b".into()),
        ..reference.clone()
    });
    let second = store
        .admit(&second_request, &home.config, &json!({}), None)
        .unwrap()
        .0;
    assert!(store
        .find_orchestrator_session(&reference)
        .unwrap()
        .is_some());
    let before: i64 = store
        .conn
        .query_row("SELECT COUNT(*) FROM orchestrator_sessions", [], |row| {
            row.get(0)
        })
        .unwrap();
    assert_eq!(
        store
            .find_orchestrator_session(&OrchestratorRef {
                transport: "codex_queue".into(),
                external_session_id: "unknown".into(),
                external_turn_id: None
            })
            .unwrap(),
        None
    );
    assert_eq!(
        store
            .conn
            .query_row("SELECT COUNT(*) FROM orchestrator_sessions", [], |row| row
                .get::<_, i64>(
                0
            ))
            .unwrap(),
        before
    );
    let (rows, total) = store.list(false, 0, 100, Some(&reference)).unwrap();
    assert_eq!(total, 2);
    assert_eq!(
        rows.iter().map(|row| &row.id).collect::<Vec<_>>(),
        vec![&second, &first]
    );
}

/// Mirrors `tests/test_state_store.py::StateStoreTests::test_guarded_transitions_attempts_and_atomic_terminal_outbox`.
#[test]
fn guarded_lifecycle_transitions_create_attempts_and_terminal_delivery() {
    let home = common::Home::new();
    let mut request = home.request();
    request.orchestrator = Some(OrchestratorRef {
        transport: "codex_queue".into(),
        external_session_id: "session".into(),
        external_turn_id: None,
    });
    let (mut store, id) = admitted(&home, &request);
    store.running(&id, 7).unwrap();
    let attempt: String = store
        .conn
        .query_row(
            "SELECT id FROM attempts WHERE agent_id=?",
            [id.as_str()],
            |row| row.get(0),
        )
        .unwrap();
    store.finish_attempt(&id, &attempt, "finished").unwrap();
    store
        .finish(&id, &Outcome::failure("fixture"), None, None)
        .unwrap();
    assert_eq!(store.get(&id).unwrap().status, Status::Failed);
    assert!(
        store
            .conn
            .query_row(
                "SELECT COUNT(*) FROM deliveries WHERE agent_id=?",
                [id.as_str()],
                |row| row.get::<_, i64>(0)
            )
            .unwrap()
            == 1
    );
}

/// Mirrors `tests/test_state_store.py::StateStoreTests::test_transcript_order_exact_active_count_and_command_ownership`.
#[test]
fn transcript_and_commands_preserve_order_and_ownership() {
    let home = common::Home::new();
    let (mut store, id) = admitted(&home, &home.request());
    store
        .append_message(&id, "tool_result", "second", None, Some("raw/2.json"), None)
        .unwrap();
    store
        .append_message(&id, "user", "first", None, None, None)
        .unwrap();
    let transcript = store.transcript(&id, 0, 10).unwrap();
    let messages = transcript["messages"].as_array().unwrap();
    assert_eq!(
        messages
            .iter()
            .map(|item| item["content"].as_str().unwrap())
            .collect::<Vec<_>>(),
        ["second", "first"]
    );
    let cancel = store.enqueue(&id, "cancel", &json!({})).unwrap()["command_id"]
        .as_i64()
        .unwrap();
    let steer = store
        .enqueue(&id, "steer", &json!({"text":"finish"}))
        .unwrap()["command_id"]
        .as_i64()
        .unwrap();
    let (claimed, kind, _) = store.claim_command(&id).unwrap().unwrap();
    assert_eq!((claimed, kind), (cancel, "cancel".into()));
    assert!(store.complete_command(&id, steer, &json!({})).is_err());
    store
        .complete_command(&id, cancel, &json!({"ok":true}))
        .unwrap();
    assert_eq!(store.claim_command(&id).unwrap().unwrap().0, steer);
}

/// Mirrors `tests/test_state_store.py::StateStoreTests::test_path_reopens_a_second_writable_connection_to_the_same_file`.
#[test]
fn path_reopens_an_independent_writable_connection() {
    let home = common::Home::new();
    let (store, id) = admitted(&home, &home.request());
    assert_eq!(store.path(), home.path.join("state.db"));
    let mut second = Store::open(store.path().parent().unwrap()).unwrap();
    second
        .append_event(&id, "from_second_connection", &json!({}), None)
        .unwrap();
    assert_eq!(
        store
            .conn
            .query_row(
                "SELECT COUNT(*) FROM events WHERE agent_id=? AND kind='from_second_connection'",
                [id.as_str()],
                |row| row.get::<_, i64>(0)
            )
            .unwrap(),
        1
    );
}
