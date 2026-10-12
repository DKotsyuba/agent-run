//! F6 and F7 on disposable private homes: the diagnostic snapshot sees
//! committed rows still in the WAL, and incident projections keep the closed
//! persistence operation, wait and attempts, bounded, after their source
//! events are deleted.
mod common;

use agent_run_store::{diagnostics::diagnostic_snapshot, writer::StoreReader};
use rusqlite::params;
use serde_json::json;

/// A committed agent that exists only in the WAL (no checkpoint) is visible
/// to the read-only diagnostic snapshot.
#[test]
fn diagnostic_snapshot_sees_committed_wal_rows() {
    let home = common::Home::new();
    let store = home.store();
    store
        .conn
        .pragma_update(None, "wal_autocheckpoint", 0)
        .unwrap();
    let mut writer = home.store();
    writer
        .conn
        .pragma_update(None, "wal_autocheckpoint", 0)
        .unwrap();
    let (id, _) = writer
        .admit(&home.request(), &home.config, &json!({}), None)
        .unwrap();
    let wal = std::fs::metadata(home.path.join("state.db-wal")).unwrap();
    assert!(wal.len() > 0);
    let snapshot = diagnostic_snapshot(&home.path.join("state.db"), 1.0, 10).unwrap();
    assert!(
        snapshot
            .agents
            .iter()
            .any(|agent| agent["id"] == json!(id.as_str()))
    );
    drop(store);
}

/// Records a failed agent with one execution-failure event carrying `data`.
fn failed_agent(home: &common::Home, data: serde_json::Value) -> agent_run_domain::domain::AgentId {
    let mut store = home.store();
    let (id, _) = store
        .admit(&home.request(), &home.config, &json!({}), None)
        .unwrap();
    store.event(&id, "execution_failure_v1", &data).unwrap();
    store
        .conn
        .execute(
            "UPDATE agents SET status='failed',finished_at=10 WHERE id=?",
            [id.as_str()],
        )
        .unwrap();
    id
}

/// The projection keeps the closed operation, wait and attempts; after the
/// source events are deleted the ledger still holds them.
#[test]
fn incident_projection_keeps_operation_wait_and_attempts() {
    let home = common::Home::new();
    let id = failed_agent(
        &home,
        json!({"class":"runtime_ownership_checkpoint_failed","stage":"streaming",
               "ownership_category":"persistence_sqlite","operation":"ownership_acquire",
               "wait_ms":600,"attempts":4,"sqlite_extended_code":5}),
    );
    let projection = StoreReader::open(&home.path)
        .unwrap()
        .incident_projection(&id, 20.0)
        .unwrap();
    let mut store = home.store();
    assert!(store.capture_incident(&projection).unwrap());
    store
        .conn
        .execute("DELETE FROM events WHERE agent_id=?", [id.as_str()])
        .unwrap();
    let details: String = store
        .conn
        .query_row(
            "SELECT details_json FROM incident_ledger WHERE execution_id=? AND phase='execution_failure'",
            [id.as_str()],
            |row| row.get(0),
        )
        .unwrap();
    let details: serde_json::Value = serde_json::from_str(&details).unwrap();
    assert_eq!(details["operation"], "ownership_acquire");
    assert_eq!(details["wait_ms"], 600);
    assert_eq!(details["attempts"], 4);
    assert_eq!(details["sqlite_extended_code"], 5);
}

/// Unknown operations become `unknown` and out-of-range numbers become null.
#[test]
fn incident_projection_bounds_untrusted_fields() {
    let home = common::Home::new();
    let id = failed_agent(
        &home,
        json!({"operation":"DROP TABLE agents","wait_ms":86_400_001u64,"attempts":-1}),
    );
    let projection = StoreReader::open(&home.path)
        .unwrap()
        .incident_projection(&id, 20.0)
        .unwrap();
    let failure = projection
        .phases
        .iter()
        .find(|phase| phase.phase == "execution_failure")
        .unwrap();
    assert_eq!(failure.details["operation"], "unknown");
    assert!(failure.details["wait_ms"].is_null());
    assert!(failure.details["attempts"].is_null());
    let mut store = home.store();
    store
        .conn
        .execute(
            "UPDATE agents SET status='running',finished_at=NULL WHERE id=?",
            params![id.as_str()],
        )
        .unwrap();
    assert!(store.capture_incident(&projection).is_err());
}

/// The writer fallback persistence label is a known closed operation in
/// incident projections.
#[test]
fn incident_projection_keeps_the_writer_fallback_label() {
    let home = common::Home::new();
    let id = failed_agent(
        &home,
        json!({"operation":"writer_apply","wait_ms":3,"attempts":1}),
    );
    let projection = StoreReader::open(&home.path)
        .unwrap()
        .incident_projection(&id, 20.0)
        .unwrap();
    let failure = projection
        .phases
        .iter()
        .find(|phase| phase.phase == "execution_failure")
        .unwrap();
    assert_eq!(failure.details["operation"], "writer_apply");
}

/// The closed writer phase of a `writer_apply` failure survives projection
/// and incident capture for every phase; a malformed, secret-looking phase
/// becomes null and its text appears nowhere in the stored details.
#[test]
fn incident_projection_keeps_the_closed_writer_phase() {
    for phase in ["gate", "authorization", "acquire", "statement", "commit"] {
        let home = common::Home::new();
        let id = failed_agent(
            &home,
            json!({"operation":"writer_apply","phase":phase,"wait_ms":3,"attempts":1}),
        );
        let projection = StoreReader::open(&home.path)
            .unwrap()
            .incident_projection(&id, 20.0)
            .unwrap();
        let mut store = home.store();
        assert!(store.capture_incident(&projection).unwrap());
        let details: String = store
            .conn
            .query_row(
                "SELECT details_json FROM incident_ledger WHERE execution_id=? AND phase='execution_failure'",
                [id.as_str()],
                |row| row.get(0),
            )
            .unwrap();
        let details: serde_json::Value = serde_json::from_str(&details).unwrap();
        assert_eq!(details["operation"], "writer_apply");
        assert_eq!(details["phase"], phase);
    }
    let home = common::Home::new();
    let id = failed_agent(
        &home,
        json!({"operation":"writer_apply","phase":"SECRET_TOKEN_sk-123","wait_ms":3,"attempts":1}),
    );
    let projection = StoreReader::open(&home.path)
        .unwrap()
        .incident_projection(&id, 20.0)
        .unwrap();
    let failure = projection
        .phases
        .iter()
        .find(|phase| phase.phase == "execution_failure")
        .unwrap();
    assert!(failure.details["phase"].is_null());
    assert!(!failure.details.to_string().contains("SECRET_TOKEN"));
}
