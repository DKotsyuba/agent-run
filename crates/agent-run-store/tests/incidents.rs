//! Incident retention/immutability tests use owned fixture databases and no model turns.
mod common;
use agent_run_domain::domain::{AgentId, Outcome};
use agent_run_store::{Store, incidents};
use rusqlite::params;
use serde_json::json;

/// Deterministic Unix clock newer than every generated fixture admission.
const NOW: f64 = 2_000_000_000.0;

/// Creates one failed terminal execution whose old source fields deliberately
/// contain a secret canary. Only closed diagnostic codes may enter the ledger.
fn failed(home: &common::Home, store: &mut Store, finished: f64) -> AgentId {
    let (id, _) = store
        .admit(&home.request(), &home.config, &json!({}), None)
        .unwrap();
    store.conn.execute("UPDATE agents SET status='failed',finished_at=?,created_at=?,failure_text='SOURCE_SECRET',failure_kind='SOURCE_SECRET' WHERE id=?",params![finished,finished,id.as_str()]).unwrap();
    store.event(&id,"execution_failure_v1",&json!({"stage":"streaming","class":"StorageError","ownership_category":"persistence_sqlite","sqlite_extended_code":5,"message":"SOURCE_SECRET","session_id":"SOURCE_SECRET","args":["SOURCE_SECRET"]})).unwrap();
    store.event(&id,"process_cleanup",&json!({"confirmed":true,"group_gone":true,"descendants_gone":true,"environment":"SOURCE_SECRET"})).unwrap();
    // All phases belong to this deterministic same-day fixture clock.
    store
        .conn
        .execute(
            "UPDATE events SET at=? WHERE agent_id=?",
            params![finished, id.as_str()],
        )
        .unwrap();
    id
}

/// More than one hundred same-day failed sessions lose bulky history while
/// keeping bounded incident phases. Duplicate captures are immutable and never
/// copy private strings, native identifiers or arbitrary event keys.
#[test]
fn incident_summary_survives_count_retirement_without_source_contents() {
    let home = common::Home::new();
    let mut store = home.store();
    let mut ids = Vec::new();
    for n in 0..105 {
        let id = failed(&home, &mut store, NOW - 1000.0 + n as f64);
        if n == 0 {
            store.conn.execute("UPDATE events SET data_json=json_set(data_json,'$.ownership_category','SOURCE_SECRET') WHERE agent_id=? AND kind='execution_failure_v1'", [id.as_str()]).unwrap();
        }
        incidents::capture(&store.conn, id.as_str(), NOW).unwrap();
        ids.push(id);
    }
    let before = incidents::summary(&store.conn).unwrap()["records"]
        .as_i64()
        .unwrap();
    for id in &ids {
        incidents::capture(&store.conn, id.as_str(), NOW + 1.0).unwrap();
    }
    assert_eq!(incidents::summary(&store.conn).unwrap()["records"], before);
    for _ in 0..30 {
        if store.prune_history(NOW).unwrap() == 0 {
            break;
        }
    }
    let agents: i64 = store
        .conn
        .query_row("SELECT COUNT(*) FROM agents", [], |r| r.get(0))
        .unwrap();
    assert_eq!(agents, 100);
    let retained:i64=store.conn.query_row("SELECT COUNT(*) FROM incident_ledger WHERE execution_id NOT IN (SELECT id FROM agents)",[],|r|r.get(0)).unwrap();
    assert!(retained >= 15);
    let categories: (i64, i64) = store.conn.query_row("SELECT SUM(json_extract(details_json,'$.ownership_category')='persistence_sqlite'),SUM(json_extract(details_json,'$.ownership_category')='unknown') FROM incident_ledger WHERE phase='execution_failure'", [], |r| Ok((r.get(0)?,r.get(1)?))).unwrap();
    assert_eq!(
        categories,
        (104, 1),
        "closed ownership category survives retirement; unknown input stays unknown"
    );
    let mut stmt = store
        .conn
        .prepare("SELECT details_json FROM incident_ledger")
        .unwrap();
    for raw in stmt.query_map([], |r| r.get::<_, String>(0)).unwrap() {
        let raw = raw.unwrap();
        assert!(!raw.contains("SOURCE_SECRET"));
        assert!(raw.len() <= 4096);
    }
    drop(stmt);
    assert!(
        store
            .conn
            .execute("UPDATE incident_ledger SET details_json='{}'", [])
            .is_err()
    );
    let summary = incidents::read_summary(&store.path()).unwrap();
    assert_eq!(summary["records"], before);
}

/// A broken diagnostic sink cannot undo an engine outcome. Retirement refuses
/// to erase its failed source until the compact projection is available.
#[test]
fn ledger_failure_preserves_source_and_does_not_undo_terminal_state() {
    let home = common::Home::new();
    let mut store = home.store();
    let (id, _) = store
        .admit(&home.request(), &home.config, &json!({}), None)
        .unwrap();
    store
        .conn
        .execute_batch("DROP TABLE incident_ledger")
        .unwrap();
    store
        .finish(&id, &Outcome::failure("fixture_failure"), None, None)
        .unwrap();
    assert_eq!(
        store.get(&id).unwrap().status,
        agent_run_domain::domain::Status::Failed
    );
    store
        .conn
        .execute(
            "UPDATE agents SET finished_at=1,created_at=1 WHERE id=?",
            [id.as_str()],
        )
        .unwrap();
    assert!(store.prune_history(NOW).is_err());
    assert!(store.get(&id).is_ok());
    assert!(
        store
            .conn
            .query_row(
                "SELECT COUNT(*) FROM events WHERE agent_id=?",
                [id.as_str()],
                |r| r.get::<_, i64>(0)
            )
            .unwrap()
            > 0
    );
}

/// Ledger ageing and overflow are independent of ordinary history eligibility;
/// the numerical cap never removes an agent, transcript or ownership record.
#[test]
fn ledger_age_and_record_cap_are_independent() {
    let home = common::Home::new();
    let mut store = home.store();
    let id = failed(&home, &mut store, NOW);
    incidents::capture(&store.conn, id.as_str(), NOW).unwrap();
    assert!(incidents::prune_pending(&store.conn, NOW + incidents::HISTORY_SECONDS + 1.0).unwrap());
    assert!(incidents::prune(&store.conn, NOW + incidents::HISTORY_SECONDS + 1.0).unwrap() > 0);
    assert!(store.get(&id).is_ok());
    store.conn.execute("WITH RECURSIVE n(x) AS (VALUES(1) UNION ALL SELECT x+1 FROM n WHERE x<10005) INSERT INTO incident_ledger(execution_id,phase,occurred_at,details_json) SELECT printf('ag-%d',x),'terminal',?,'{}' FROM n",[NOW]).unwrap();
    incidents::prune(&store.conn, NOW).unwrap();
    assert_eq!(incidents::summary(&store.conn).unwrap()["records"], 10000);
    assert!(store.get(&id).is_ok());
}
