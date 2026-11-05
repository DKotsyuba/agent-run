//! Admission barriers capture a durable cohort without completing any work.
mod common;

use agent_run_domain::{Error, domain::OrchestratorRef};
use agent_run_store::writer::WriteTx;
use serde_json::json;
use std::{
    sync::{Arc, Barrier},
    thread,
};

/// Fresh schema stamping must already match the compiled version: applying
/// its own final migration again would collide with newly created tables.
#[test]
fn maintenance_fresh_schema_has_current_version() {
    let conn = rusqlite::Connection::open_in_memory().unwrap();
    conn.execute_batch(include_str!("../../../sql/schema.sql"))
        .unwrap();
    assert_eq!(
        conn.pragma_query_value::<i64, _>(None, "user_version", |row| row.get(0))
            .unwrap(),
        agent_run_store::VERSION
    );
    assert_eq!(
        conn.query_row("SELECT COUNT(*) FROM maintenance_fences", [], |row| row
            .get::<_, i64>(0))
            .unwrap(),
        0
    );
}

/// A resumed execution keeps its original start time when its new supervisor
/// returns it from starting to running.
#[test]
fn maintenance_running_keeps_the_original_start_time() {
    let home = common::Home::new();
    let mut store = home.store();
    let (id, _) = store
        .admit(&home.request(), &home.config, &json!({}), None)
        .unwrap();
    store
        .conn
        .execute("UPDATE agents SET started_at=123 WHERE id=?", [id.as_str()])
        .unwrap();
    store.running(&id, 12345).unwrap();
    assert_eq!(store.get(&id).unwrap().started_at, Some(123.0));
}

/// Admission replay and the cohort survive reopening; later binding/progress
/// cannot rewrite the boundary, and stale release cannot lift a newer epoch.
#[test]
fn maintenance_fence_replays_and_preserves_snapshot() {
    let home = common::Home::new();
    let mut request = home.request();
    request.request_id = Some("original".into());
    let mut store = home.store();
    let (id, _) = store
        .admit(&request, &home.config, &json!({}), None)
        .unwrap();
    store.conn.execute("INSERT INTO attempts(id,agent_id,number,state,adapter_state_json,created_at,ownership_active) VALUES('attempt',?,1,'running','{}',1,1)", [id.as_str()]).unwrap();
    store.conn.execute("INSERT INTO commands(agent_id,kind,payload_json,state,created_at) VALUES(?,'steer','{}','pending',1)", [id.as_str()]).unwrap();
    let fence = store
        .begin_maintenance_fence("maintenance-one", 2.0)
        .unwrap();
    assert_eq!(fence.member_count, 1);
    let captured = store.maintenance_cohort(fence.epoch).unwrap();
    assert_eq!(captured[0].agent_id, id);
    assert_eq!(captured[0].status, "starting");
    assert_eq!(captured[0].attempts[0].id, "attempt");
    assert!(captured[0].attempts[0].ownership_active);
    assert!(captured[0].orchestrator_session_id.is_none());
    assert!(matches!(
        store.begin_maintenance_fence("competing", 3.0),
        Err(Error::Conflict)
    ));
    drop(store);
    let mut store = home.store();
    assert_eq!(
        store
            .begin_maintenance_fence("maintenance-one", 4.0)
            .unwrap(),
        fence
    );
    assert_eq!(
        store
            .admit(&request, &home.config, &json!({}), None)
            .unwrap(),
        (id.clone(), false)
    );
    let mut conflict = request.clone();
    conflict.task = "different".into();
    assert!(matches!(
        store.admit(&conflict, &home.config, &json!({}), None),
        Err(Error::Conflict)
    ));
    request.request_id = Some("new".into());
    assert!(matches!(
        store.admit(&request, &home.config, &json!({}), None),
        Err(Error::Transition(_))
    ));
    let reference: OrchestratorRef = serde_json::from_value(
        json!({"transport":"codex_queue","external_session_id":"later-session"}),
    )
    .unwrap();
    store.bind_orchestrator(&id, &reference, 5.0).unwrap();
    assert_eq!(store.maintenance_cohort(fence.epoch).unwrap(), captured);
    assert_eq!(
        store
            .conn
            .query_row(
                "SELECT COUNT(*) FROM commands WHERE state='pending'",
                [],
                |r| r.get::<_, i64>(0)
            )
            .unwrap(),
        1
    );
    assert_eq!(
        store
            .conn
            .query_row(
                "SELECT COUNT(*) FROM attempts WHERE ownership_active=1",
                [],
                |r| r.get::<_, i64>(0)
            )
            .unwrap(),
        1
    );
    assert_eq!(
        store
            .conn
            .query_row("SELECT COUNT(*) FROM deliveries", [], |r| r
                .get::<_, i64>(0))
            .unwrap(),
        0
    );
    let ended = store.end_maintenance_fence(fence.epoch, 6.0).unwrap();
    assert_eq!(
        store
            .begin_maintenance_fence("maintenance-one", 7.0)
            .unwrap(),
        ended
    );
    assert!(store.active_maintenance_fence().unwrap().is_none());
    store
        .admit(&request, &home.config, &json!({}), None)
        .unwrap();
    let next = store
        .begin_maintenance_fence("maintenance-two", 8.0)
        .unwrap();
    assert!(next.epoch > fence.epoch);
    store.end_maintenance_fence(fence.epoch, 9.0).unwrap();
    assert_eq!(store.active_maintenance_fence().unwrap(), Some(next));
}

/// One durable pre-notice is created for each already-bound orchestrator. Its
/// native session is frozen even if the mutable session row changes later.
#[test]
fn maintenance_captures_distinct_recipients_and_frozen_routes() {
    use agent_run_store::ops::{ClaimDelivery, DeliveryPolicySpec};

    let home = common::Home::new();
    let mut store = home.store();
    let reference = |session: &str| OrchestratorRef {
        transport: "codex_queue".into(),
        external_session_id: session.into(),
        external_turn_id: None,
    };
    for target in [
        Some("session-one"),
        Some("session-one"),
        Some("session-two"),
        None,
    ] {
        let mut request = home.request();
        request.orchestrator = target.map(reference);
        store
            .admit(&request, &home.config, &json!({}), None)
            .unwrap();
    }
    let fence = store
        .begin_maintenance_fence("notice-fixture", 2.0)
        .unwrap();
    assert_eq!(fence.member_count, 4);
    assert_eq!(
        store
            .conn
            .query_row("SELECT COUNT(*) FROM maintenance_recipients", [], |r| r
                .get::<_, i64>(0))
            .unwrap(),
        2
    );
    assert_eq!(
        store
            .conn
            .query_row(
                "SELECT COUNT(*) FROM deliveries WHERE state='pending'",
                [],
                |r| r.get::<_, i64>(0)
            )
            .unwrap(),
        2
    );
    assert_eq!(
        store
            .conn
            .query_row(
                "SELECT COUNT(*) FROM events WHERE kind='maintenance_notice_v1'",
                [],
                |r| r.get::<_, i64>(0)
            )
            .unwrap(),
        2
    );
    let unbound = store
        .maintenance_cohort(fence.epoch)
        .unwrap()
        .into_iter()
        .find(|member| member.orchestrator_session_id.is_none())
        .unwrap()
        .agent_id;
    store
        .bind_orchestrator(&unbound, &reference("late-session"), 3.0)
        .unwrap();
    store.conn.execute("UPDATE orchestrator_sessions SET external_session_id='changed-later' WHERE external_session_id='session-one'", []).unwrap();
    assert_eq!(
        store
            .conn
            .query_row("SELECT COUNT(*) FROM maintenance_recipients", [], |r| r
                .get::<_, i64>(0))
            .unwrap(),
        2
    );
    let mut targets = std::collections::BTreeSet::new();
    for _ in 0..2 {
        let claim = store
            .claim_delivery_lease(&ClaimDelivery {
                owner: "fixture".into(),
                at: 4.0,
                policy: DeliveryPolicySpec {
                    retry_base_seconds: 1.0,
                    retry_cap_seconds: 2.0,
                    max_attempts: 0,
                    lease_seconds: 30.0,
                },
            })
            .unwrap()
            .unwrap();
        assert_eq!(claim["payload"]["kind"], "maintenance");
        assert_eq!(
            claim["payload"]["notice"],
            json!({"maintenance_epoch":fence.epoch,"stage":"pre"})
        );
        targets.insert(claim["session"].as_str().unwrap().to_owned());
    }
    assert_eq!(
        targets,
        ["session-one".to_owned(), "session-two".to_owned()]
            .into_iter()
            .collect()
    );
    assert_eq!(
        store
            .begin_maintenance_fence("notice-fixture", 5.0)
            .unwrap(),
        fence
    );
    assert_eq!(
        store
            .conn
            .query_row("SELECT COUNT(*) FROM maintenance_recipients", [], |r| r
                .get::<_, i64>(0))
            .unwrap(),
        2
    );
    // Deferral publishes exactly one follow-up per original recipient, even
    // when several agents share it and the operation is replayed.
    store.end_maintenance_fence(fence.epoch, 6.0).unwrap();
    store.end_maintenance_fence(fence.epoch, 7.0).unwrap();
    assert_eq!(store.conn.query_row(
        "SELECT COUNT(*) FROM maintenance_recipients r JOIN deliveries d ON d.id=r.after_delivery_id JOIN events e ON e.seq=d.terminal_event_seq WHERE r.epoch=? AND json_extract(e.data_json,'$.stage')='deferred'",
        [fence.epoch], |row| row.get::<_,i64>(0),
    ).unwrap(), 2);
}

/// A final notice follows the captured recipient once and reports failures
/// separately instead of treating them as successful completions.
#[test]
fn maintenance_after_notice_reports_failed_work() {
    let home = common::Home::new();
    let mut store = home.store();
    let mut request = home.request();
    request.orchestrator = Some(OrchestratorRef {
        transport: "codex_queue".into(),
        external_session_id: "notice-session".into(),
        external_turn_id: None,
    });
    let (id, _) = store
        .admit(&request, &home.config, &json!({}), None)
        .unwrap();
    store
        .conn
        .execute(
            "INSERT INTO writer_authority(id,epoch,holder_pid,acquired_at) VALUES(1,1,999,1)",
            [],
        )
        .unwrap();
    let fence = store.begin_maintenance_fence("notice-after", 2.0).unwrap();
    store
        .conn
        .execute(
            "UPDATE agents SET status='failed',finished_at=3 WHERE id=?",
            [id.as_str()],
        )
        .unwrap();
    store.conn.execute("INSERT INTO bridge_delivery_intents(delivery_id,intent_json,custody_state) SELECT before_delivery_id,'{}','custody_accepted' FROM maintenance_recipients WHERE epoch=?", [fence.epoch]).unwrap();
    store
        .ready_maintenance_fence(fence.epoch, &[], 4.0)
        .unwrap();
    assert!(store.commit_maintenance_stop(fence.epoch).unwrap());
    store
        .conn
        .execute("UPDATE writer_authority SET epoch=2 WHERE id=1", [])
        .unwrap();
    store.observe_maintenance_restart(fence.epoch).unwrap();
    store.end_maintenance_fence(fence.epoch, 5.0).unwrap();
    store.end_maintenance_fence(fence.epoch, 6.0).unwrap();
    let raw:String = store.conn.query_row("SELECT e.data_json FROM maintenance_recipients r JOIN deliveries d ON d.id=r.after_delivery_id JOIN events e ON e.seq=d.terminal_event_seq WHERE r.epoch=?", [fence.epoch], |row| row.get(0)).unwrap();
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(&raw).unwrap(),
        json!({"maintenance_epoch":fence.epoch,"stage":"after","resumed":0,"completed":0,"failed":1,"cancelled":0,"held":0})
    );
}

/// Competing admission and fence transactions serialize: admitted work is
/// captured, or refusal leaves no agent. There is no committed uncaptured run.
#[test]
fn maintenance_admission_race_never_escapes_cohort() {
    for _ in 0..8 {
        let home = common::Home::new();
        let barrier = Arc::new(Barrier::new(2));
        let path = home.path.clone();
        let gate = barrier.clone();
        let fence = thread::spawn(move || {
            let mut store = agent_run_store::fixture::open(&path).unwrap();
            gate.wait();
            store.begin_maintenance_fence("race", 1.0).unwrap()
        });
        let mut store = home.store();
        barrier.wait();
        let admitted = store.admit(&home.request(), &home.config, &json!({}), None);
        let fence = fence.join().unwrap();
        let cohort = store.maintenance_cohort(fence.epoch).unwrap();
        match admitted {
            Ok((id, true)) => assert_eq!(
                cohort.iter().map(|m| &m.agent_id).collect::<Vec<_>>(),
                [&id]
            ),
            Err(Error::Transition(_)) => assert!(cohort.is_empty()),
            other => panic!("unexpected admission: {other:?}"),
        }
        assert_eq!(
            store
                .conn
                .query_row("SELECT COUNT(*) FROM agents", [], |r| r.get::<_, u64>(0))
                .unwrap(),
            fence.member_count
        );
    }
}

/// The fence and captured rows participate in the caller's writer transaction;
/// aborting it cannot leave a barrier or consume the first committed epoch.
#[test]
fn maintenance_capture_rolls_back_with_writer_transaction() {
    let home = common::Home::new();
    let mut store = home.store();
    let mut request = home.request();
    request.orchestrator = Some(OrchestratorRef {
        transport: "codex_queue".into(),
        external_session_id: "rollback-session".into(),
        external_turn_id: None,
    });
    store
        .admit(&request, &home.config, &json!({}), None)
        .unwrap();
    store.conn.execute_batch("BEGIN IMMEDIATE").unwrap();
    store.begin_maintenance_fence("rolled-back", 1.0).unwrap();
    assert_eq!(
        store
            .conn
            .query_row("SELECT COUNT(*) FROM maintenance_recipients", [], |r| r
                .get::<_, i64>(0))
            .unwrap(),
        1
    );
    assert_eq!(
        store
            .conn
            .query_row("SELECT COUNT(*) FROM deliveries", [], |r| r
                .get::<_, i64>(0))
            .unwrap(),
        1
    );
    store.conn.execute_batch("ROLLBACK").unwrap();
    assert!(store.active_maintenance_fence().unwrap().is_none());
    assert_eq!(
        store
            .conn
            .query_row("SELECT COUNT(*) FROM maintenance_cohort", [], |r| r
                .get::<_, i64>(0))
            .unwrap(),
        0
    );
    assert_eq!(
        store
            .conn
            .query_row("SELECT COUNT(*) FROM maintenance_recipients", [], |r| r
                .get::<_, i64>(0))
            .unwrap(),
        0
    );
    assert_eq!(
        store
            .conn
            .query_row("SELECT COUNT(*) FROM deliveries", [], |r| r
                .get::<_, i64>(0))
            .unwrap(),
        0
    );
    let tx = WriteTx::begin(&store.conn).unwrap();
    assert!(tx.is_top_level());
    tx.commit().unwrap();
    assert_eq!(
        store
            .begin_maintenance_fence("committed", 2.0)
            .unwrap()
            .epoch,
        1
    );
}

/// Exact schema 33 upgrades without touching existing active attempts or
/// creating an implicit maintenance epoch; new stores have the same objects.
#[test]
fn migration_33_adds_only_empty_maintenance_state() {
    let home = common::Home::new();
    let mut store = home.store();
    let (id, _) = store
        .admit(&home.request(), &home.config, &json!({}), None)
        .unwrap();
    let before = serde_json::to_value(store.get(&id).unwrap()).unwrap();
    store
        .conn
        .execute_batch(
            "DROP TABLE maintenance_resumes; DROP TABLE maintenance_parks; DROP TABLE maintenance_checkpoints; DROP TABLE maintenance_recipients; DROP TABLE maintenance_cohort; DROP TABLE maintenance_fences; PRAGMA user_version=33;",
        )
        .unwrap();
    drop(store);
    assert_eq!(
        agent_run_store::migrations::migrate(&home.path.join("state.db")).unwrap(),
        agent_run_store::VERSION
    );
    let store = home.store();
    assert_eq!(
        serde_json::to_value(store.get(&id).unwrap()).unwrap(),
        before
    );
    assert!(store.active_maintenance_fence().unwrap().is_none());
    let violation: Option<String> = store
        .conn
        .prepare("PRAGMA foreign_key_check")
        .unwrap()
        .query_map([], |r| r.get(0))
        .unwrap()
        .next()
        .transpose()
        .unwrap();
    assert_eq!(violation, None);
}
