mod common;
use agent_run_core::service::Service;
use agent_run_domain::{
    domain::{OrchestratorRef, Outcome, Status},
    Error,
};
use agent_run_platform::{fs, verify};
use agent_run_store::Store;
use serde_json::json;
use std::path::Path;

/// Mirrors `test_state_db.py::test_fresh_init_and_reopen_apply_schema_pragmas_and_private_modes`.
///
/// Initialization and a second connection retain the durable schema and WAL safety settings.
#[test]
fn schema_initialization_and_reopen() {
    let h = common::Home::new();
    let a = h.store().health().unwrap();
    assert_eq!(a["ok"], true);
    assert_eq!(a["schema_version"], 16);
    assert_eq!(a["tables"], 16);
    assert_eq!(h.store().health().unwrap()["integrity"], "ok");
    let store = h.store();
    assert_eq!(
        store
            .conn
            .pragma_query_value(None, "journal_mode", |row| row.get::<_, String>(0))
            .unwrap(),
        "wal"
    );
    assert_eq!(
        store
            .conn
            .pragma_query_value(None, "synchronous", |row| row.get::<_, i64>(0))
            .unwrap(),
        2
    );
    assert_eq!(
        store
            .conn
            .pragma_query_value(None, "foreign_keys", |row| row.get::<_, i64>(0))
            .unwrap(),
        1
    );
}
#[test]
fn admission_is_durable_and_replay_is_exact() {
    let h = common::Home::new();
    let mut r = h.request();
    r.request_id = Some("replay".into());
    let (id, created) = h.store().admit(&r, &h.config, &json!({}), None).unwrap();
    assert!(created);
    assert_eq!(h.store().get(&id).unwrap().status, Status::Starting);
    let (same, created) = h.store().admit(&r, &h.config, &json!({}), None).unwrap();
    assert!(!created);
    assert_eq!(same, id);
    r.task = "different".into();
    assert!(matches!(
        h.store().admit(&r, &h.config, &json!({}), None),
        Err(Error::Conflict)
    ));
}
#[test]
fn request_ids_are_scoped_to_orchestrator() {
    let h = common::Home::new();
    let mut r = h.request();
    r.request_id = Some("same".into());
    let (a, _) = h.store().admit(&r, &h.config, &json!({}), None).unwrap();
    r.orchestrator = Some(OrchestratorRef {
        transport: "codex_queue".into(),
        external_session_id: "session".into(),
        external_turn_id: None,
    });
    let (b, _) = h.store().admit(&r, &h.config, &json!({}), None).unwrap();
    assert_ne!(a, b);
}
#[test]
fn concurrency_limit_does_not_create_an_extra_row() {
    let h = common::Home::new();
    let mut config = h.config.clone();
    config.core.max_active_agents = 1;
    h.store()
        .admit(&h.request(), &config, &json!({}), None)
        .unwrap();
    assert!(matches!(
        h.store().admit(&h.request(), &config, &json!({}), None),
        Err(Error::Capacity)
    ));
    assert_eq!(h.store().list(false, 0, 100, None).unwrap().1, 1);
}
#[test]
fn terminal_success_requires_valid_artifact() {
    let h = common::Home::new();
    let (id, _) = h
        .store()
        .admit(&h.request(), &h.config, &json!({}), None)
        .unwrap();
    h.store().running(&id, 42).unwrap();
    let outcome = Outcome::success(None);
    assert!(h.store().finish(&id, &outcome, None, None).is_err());
    let root = h.path.join("agents").join(id.as_str());
    fs::private_dir(&root).unwrap();
    let proof = verify::seal(&root, Path::new("answer.md"), "Complete answer").unwrap();
    std::fs::write(&proof.path, "Tampered answer").unwrap();
    assert!(h.store().finish(&id, &outcome, Some(&proof), None).is_err());
    assert_eq!(h.store().get(&id).unwrap().status, Status::Running);
}
#[test]
fn success_is_durable_and_terminal_state_is_immutable() {
    let h = common::Home::new();
    let (id, _) = h
        .store()
        .admit(&h.request(), &h.config, &json!({}), None)
        .unwrap();
    h.store().running(&id, 42).unwrap();
    let root = h.path.join("agents").join(id.as_str());
    fs::private_dir(&root).unwrap();
    let proof = verify::seal(&root, Path::new("answer.md"), "Answer").unwrap();
    h.store()
        .finish(
            &id,
            &Outcome::success(Some("native-session".into())),
            Some(&proof),
            None,
        )
        .unwrap();
    h.store()
        .finish(&id, &Outcome::failure("late_error"), None, None)
        .unwrap();
    let row = h.store().get(&id).unwrap();
    assert_eq!(row.status, Status::Succeeded);
    assert_eq!(row.answer_bytes, Some(6));
    assert_eq!(row.runtime_session_id.as_deref(), Some("native-session"));
}
#[test]
fn commands_are_claimed_once_and_completed() {
    let h = common::Home::new();
    let (id, _) = h
        .store()
        .admit(&h.request(), &h.config, &json!({}), None)
        .unwrap();
    h.store().enqueue(&id, "cancel", &json!({})).unwrap();
    assert!(h.store().cancel_pending(&id).unwrap());
    let (command_id, _, _) = h.store().claim_command(&id).unwrap().unwrap();
    assert!(h.store().claim_command(&id).unwrap().is_none());
    h.store()
        .complete_command(&id, command_id, &json!({"accepted":true}))
        .unwrap();
    assert!(!h.store().cancel_pending(&id).unwrap());
}
#[test]
fn message_cursors_preserve_order_whitespace_and_repetitions() {
    let h = common::Home::new();
    let (id, _) = h
        .store()
        .admit(&h.request(), &h.config, &json!({}), None)
        .unwrap();
    for text in ["x", " ", "x", "\n"] {
        h.store()
            .message(&id, "assistant", text, None, None)
            .unwrap();
    }
    let first = h.store().transcript(&id, 0, 2).unwrap();
    assert_eq!(first["complete"], false);
    let next = first["next_cursor"].as_i64().unwrap();
    let second = h.store().transcript(&id, next, 2).unwrap();
    assert_eq!(second["complete"], true);
    let all = first["messages"]
        .as_array()
        .unwrap()
        .iter()
        .chain(second["messages"].as_array().unwrap())
        .map(|m| m["content"].as_str().unwrap())
        .collect::<String>();
    assert_eq!(all, "x x\n");
}
/// Terminal runs retain an inert receipt until a late post-tool bind can activate it.
#[test]
fn terminal_runs_create_pending_or_waiting_binding_deliveries() {
    let h = common::Home::new();
    let mut r = h.request();
    let (a, _) = h.store().admit(&r, &h.config, &json!({}), None).unwrap();
    h.store()
        .finish(&a, &Outcome::failure("test"), None, None)
        .unwrap();
    assert_eq!(
        h.store().delivery_status(&a).unwrap()["state"],
        "waiting_binding"
    );
    r.orchestrator = Some(OrchestratorRef {
        transport: "codex_queue".into(),
        external_session_id: "session".into(),
        external_turn_id: None,
    });
    let (b, _) = h.store().admit(&r, &h.config, &json!({}), None).unwrap();
    h.store()
        .finish(&b, &Outcome::failure("test"), None, None)
        .unwrap();
    assert_eq!(h.store().delivery_status(&b).unwrap()["state"], "pending");
}
#[test]
fn resume_lineage_has_one_child_and_a_stable_root() {
    let h = common::Home::new();
    let r = h.request();
    let (id, _) = h.store().admit(&r, &h.config, &json!({}), None).unwrap();
    h.store().runtime_session(&id, "native-session").unwrap();
    h.store()
        .finish(&id, &Outcome::failure("test"), None, None)
        .unwrap();
    let parent = h.store().get(&id).unwrap();
    let (child, _) = h
        .store()
        .admit(&r, &h.config, &json!({}), Some(&parent))
        .unwrap();
    let row = h.store().get(&child).unwrap();
    assert_eq!(row.parent_agent_id, Some(id.clone()));
    assert_eq!(row.root_agent_id, id);
    assert_eq!(row.sequence, 2);
    assert!(matches!(
        h.store().admit(&r, &h.config, &json!({}), Some(&parent)),
        Err(Error::Conflict)
    ));
}
#[test]
fn backup_is_openable_and_never_overwrites_an_existing_file() {
    let h = common::Home::new();
    let dst = h.path.join("backup.db");
    h.store().backup(&dst).unwrap();
    assert!(h.store().backup(&dst).is_err());
    let db = rusqlite::Connection::open(dst).unwrap();
    assert_eq!(
        db.pragma_query_value(None, "user_version", |r| r.get::<_, i64>(0))
            .unwrap(),
        16
    );
}
/// Mirrors `test_state_db.py::test_invalid_and_newer_versions_refuse_without_schema_mutation`.
///
/// A newer schema version is rejected without being changed by this binary.
#[test]
fn newer_database_version_is_refused_without_upgrade() {
    let h = common::Home::new();
    let db = rusqlite::Connection::open(h.path.join("state.db")).unwrap();
    db.pragma_update(None, "user_version", 17).unwrap();
    drop(db);
    assert!(Store::open(&h.path).is_err());
    let db = rusqlite::Connection::open(h.path.join("state.db")).unwrap();
    assert_eq!(
        db.pragma_query_value(None, "user_version", |r| r.get::<_, i32>(0))
            .unwrap(),
        17
    );
}
#[test]
fn legacy_database_version_is_migrated_on_open() {
    // Schema 15 is only one migration (016_reconciliation_cursors.sql) behind
    // current: drop the table and index it adds and stamp the store back to
    // v15, then confirm `Store::open` upgrades it transparently instead of
    // refusing it (see rust/src/state/migrations.rs, ported from
    // src/agent_run/state/migrations.py).
    let h = common::Home::new();
    {
        let db = rusqlite::Connection::open(h.path.join("state.db")).unwrap();
        db.execute_batch(
            "DROP INDEX idx_agents_request_id; DROP TABLE reconciliation_cursors; \
             PRAGMA user_version=15;",
        )
        .unwrap();
    }
    let store = Store::open(&h.path).unwrap();
    assert_eq!(store.health().unwrap()["schema_version"], 16);
}
#[tokio::test]
async fn start_replay_is_independent_of_later_configuration_edits() {
    let h = common::Home::new();
    let mut original = h.request();
    original.request_id = Some("stable-replay".into());
    let fingerprint =
        fs::sha256(&fs::canonical_json(&serde_json::to_value(&original).unwrap()).unwrap());
    let mut effective = original.clone();
    effective.timeout_seconds = Some(480.0);
    let (id, _) = h
        .store()
        .admit(
            &effective,
            &h.config,
            &json!({"replay_request_sha256":fingerprint}),
            None,
        )
        .unwrap();
    std::fs::write(h.path.join("config.toml"), "invalid mutable configuration").unwrap();
    let replay = Service::new(h.path.clone()).start(original).await.unwrap();
    assert_eq!(replay["agent_id"], id.as_str());
    assert_eq!(replay["created"], false);
}
#[tokio::test]
async fn changed_start_payload_cannot_reuse_a_recorded_fingerprint() {
    let h = common::Home::new();
    let mut original = h.request();
    original.request_id = Some("stable-replay".into());
    let fingerprint =
        fs::sha256(&fs::canonical_json(&serde_json::to_value(&original).unwrap()).unwrap());
    h.store()
        .admit(
            &original,
            &h.config,
            &json!({"replay_request_sha256":fingerprint}),
            None,
        )
        .unwrap();
    original.task = "different task".into();
    assert!(matches!(
        Service::new(h.path.clone()).start(original).await,
        Err(Error::Conflict)
    ));
}
