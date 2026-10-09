//! Schema-28 migration preserves exact historical evidence while retiring active clocks.
mod common;
use agent_run_store::{Store, VERSION, migrations};
use rusqlite::{Connection, params};
use serde_json::json;

/// The schema-27 clock values move to historical events, without rewriting any
/// original request, identity, journal entry or pinned enrollment identity.
/// Fresh schema controls and the remaining immutable triggers stay enforced.
#[test]
fn migration_preserves_history_and_retires_execution_clocks() {
    let home = common::Home::new();
    let db = home.path.join("state.db");
    std::fs::copy(
        std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../../tests/fixtures/baseline/db/current-v27.sqlite"),
        &db,
    )
    .unwrap();
    let id = agent_run_domain::domain::AgentId::new();
    let mut historical = serde_json::to_value(home.request()).unwrap();
    historical["timeout_seconds"] = json!(600.0);
    let request = serde_json::to_string(&historical).unwrap();
    let identity = " { \"historical_grants\" : [ \"read-only\" ], \"retired_policy\" : 600.0 } ";
    let event = "{ \"original\" : \"journal bytes remain exact\" }";
    let conn = Connection::open(&db).unwrap();
    conn.execute_batch("PRAGMA foreign_keys=ON").unwrap();
    conn.execute(
        "INSERT INTO agents(id,runtime,model,profile,task,task_summary,workdir,request_json,status,created_at,timeout_seconds,warned,silent_seconds,config_revision,root_agent_id,identity_json)
         VALUES(?1,'mock','fixture','review','fixture task','fixture',?2,?3,'running',1,600,1,9,'frozen',?1,?4)",
        params![id.as_str(),home.path.to_string_lossy(),request,identity],
    ).unwrap();
    conn.execute("INSERT INTO attempts(id,agent_id,number,state,adapter_state_json,created_at,ownership_active) VALUES('att-history',?,1,'running','{}',1,1)",[id.as_str()]).unwrap();
    conn.execute(
        "INSERT INTO events(agent_id,at,kind,data_json) VALUES(?,2,'fixture-original',?)",
        params![id.as_str(), event],
    )
    .unwrap();
    conn.execute("INSERT INTO pools(id,request_namespace,request_id,request_sha256,goal,acceptance_json,state,roster_revision,created_at) VALUES('pool-history','fixture','history',?,'goal','[]','open',1,1)",["a".repeat(64)]).unwrap();
    conn.execute("INSERT INTO pool_members(agent_id,pool_id,slot,name,role,personal_task,joined_roster_revision) VALUES(?,'pool-history',1,'existing','review','task',1)",[id.as_str()]).unwrap();
    conn.execute("INSERT INTO pool_enrollments(agent_id,run_id,attempt_id,challenge,deadline,created_at) VALUES(?1,?1,'att-history','join-history',601,1)",[id.as_str()]).unwrap();
    drop(conn);
    let store = Store::open(&home.path).unwrap();
    assert_eq!(store.health().unwrap()["schema_version"], VERSION);
    // Automatic intermediate backups retire after a verified commit; explicit
    // paired config migration retains its recoverable snapshot (tested separately).
    assert!(!migrations::backup_path(&db, VERSION).exists());
    let raw: (String, String, String) = store
        .conn
        .query_row(
            "SELECT request_json,identity_json,status FROM agents WHERE id=?",
            [id.as_str()],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .unwrap();
    assert_eq!(raw, (request, identity.into(), "running".into()));
    let original: String = store
        .conn
        .query_row(
            "SELECT data_json FROM events WHERE agent_id=? AND kind='fixture-original'",
            [id.as_str()],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(original, event);
    assert_eq!(
        store
            .last_event(&id, "historical_execution_policy")
            .unwrap()
            .unwrap(),
        json!({"version":1,"timeout_seconds":600.0,"warned":1,"silent_seconds":9.0})
    );
    assert_eq!(
        store
            .last_event(&id, "historical_enrollment_policy")
            .unwrap()
            .unwrap(),
        json!({"version":1,"deadline":601.0})
    );
    assert!(
        serde_json::to_value(store.get(&id).unwrap().request)
            .unwrap()
            .get("timeout_seconds")
            .is_none()
    );
    let enrollment: (String,String,String,String,String) = store.conn.query_row(
        "SELECT agent_id,run_id,attempt_id,challenge,state FROM pool_enrollments WHERE agent_id=?", [id.as_str()],
        |r| Ok((r.get(0)?,r.get(1)?,r.get(2)?,r.get(3)?,r.get(4)?)),
    ).unwrap();
    assert_eq!(
        enrollment,
        (
            id.to_string(),
            id.to_string(),
            "att-history".into(),
            "join-history".into(),
            "pending".into()
        )
    );
    for key in ["timeout_seconds", "warned", "silent_seconds"] {
        assert_eq!(
            store
                .conn
                .query_row(
                    "SELECT COUNT(*) FROM pragma_table_info('agents') WHERE name=?",
                    [key],
                    |r| r.get::<_, i64>(0)
                )
                .unwrap(),
            0
        );
    }
    assert_eq!(
        store
            .conn
            .query_row(
                "SELECT COUNT(*) FROM pragma_table_info('pool_enrollments') WHERE name='deadline'",
                [],
                |r| r.get::<_, i64>(0)
            )
            .unwrap(),
        0
    );
    assert!(
        store
            .conn
            .execute(
                "UPDATE pool_enrollments SET challenge='forged' WHERE agent_id=?",
                [id.as_str()]
            )
            .is_err()
    );
    assert!(
        store
            .conn
            .execute(
                "UPDATE agents SET pool_membership_ever=0 WHERE id=?",
                [id.as_str()]
            )
            .is_err()
    );
    assert!(
        store
            .conn
            .execute(
                "UPDATE pool_enrollments SET state='joined' WHERE agent_id=?",
                [id.as_str()]
            )
            .is_err()
    );
    assert_eq!(
        store
            .conn
            .query_row("PRAGMA quick_check", [], |r| r.get::<_, String>(0))
            .unwrap(),
        "ok"
    );
    assert!(
        store
            .conn
            .prepare("PRAGMA foreign_key_check")
            .unwrap()
            .query([])
            .unwrap()
            .next()
            .unwrap()
            .is_none()
    );
}
