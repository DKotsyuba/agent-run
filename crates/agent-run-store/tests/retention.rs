//! History expiry tests use private databases and never spawn engine processes.

mod common;

use agent_run_domain::domain::AgentId;
use agent_run_store::{retention::HISTORY_SECONDS, Store};
use rusqlite::params;
use serde_json::json;

/// Fixed Unix time keeps strict fourteen-day boundaries deterministic.
const NOW: f64 = 1_800_000_000.0;

/// Admits a fixture agent, then assigns the supplied state and optional completion time.
fn agent(home: &common::Home, store: &mut Store, status: &str, finished: Option<f64>) -> AgentId {
    let (id, _) = store
        .admit(&home.request(), &home.config, &json!({}), None)
        .unwrap();
    store
        .conn
        .execute(
            "UPDATE agents SET status=?,created_at=?,finished_at=? WHERE id=?",
            params![status, NOW - HISTORY_SECONDS - 100.0, finished, id.as_str()],
        )
        .unwrap();
    id
}

/// Returns a table's row count; table names are test constants, never user input.
fn count(store: &Store, table: &str) -> i64 {
    store
        .conn
        .query_row(&format!("SELECT count(*) FROM {table}"), [], |r| r.get(0))
        .unwrap()
}

/// Asserts every persisted reference still resolves, including relationships without cascade deletion.
fn integrity(store: &Store) {
    assert_eq!(
        store
            .conn
            .query_row("SELECT count(*) FROM pragma_foreign_key_check", [], |r| r
                .get::<_, i64>(
                0
            ))
            .unwrap(),
        0
    );
}

/// Old active/unknown-completion/owned work and ancestors of recent continuations survive expiry.
#[test]
fn retention_protects_active_ownership_lineage_and_exact_age_boundary() {
    let home = common::Home::new();
    let mut store = home.store();
    let cutoff = NOW - HISTORY_SECONDS;
    let expired = agent(&home, &mut store, "failed", Some(cutoff - 1.0));
    let boundary = agent(&home, &mut store, "succeeded", Some(cutoff));
    let active = agent(&home, &mut store, "running", Some(cutoff - 1.0));
    let unknown = agent(&home, &mut store, "lost", None);
    let owned = agent(&home, &mut store, "lost", Some(cutoff - 1.0));
    let attempt = store.create_attempt(&owned, "lost", &json!({})).unwrap();
    store
        .conn
        .execute(
            "UPDATE attempts SET ownership_active=1 WHERE id=?",
            [&attempt],
        )
        .unwrap();
    let parent = agent(&home, &mut store, "succeeded", Some(cutoff - 1.0));
    let child = agent(&home, &mut store, "succeeded", Some(NOW));
    store
        .conn
        .execute(
            "UPDATE agents SET parent_agent_id=?,root_agent_id=? WHERE id=?",
            params![parent.as_str(), parent.as_str(), child.as_str()],
        )
        .unwrap();
    assert!(store.prune_history(NOW).unwrap() > 0);
    assert!(store.get(&expired).is_err());
    for id in [&boundary, &active, &unknown, &owned, &parent, &child] {
        assert!(store.get(id).is_ok(), "must retain {id}");
    }
    assert_eq!(store.prune_history(NOW).unwrap(), 0);
    assert!(store.prune_history(f64::NAN).is_err());
    integrity(&store);
}

/// Large journals drain in batches, retaining referenced terminal events until their notices drain.
#[test]
fn retention_drains_large_runs_and_all_attempt_dependents() {
    let home = common::Home::new();
    let mut store = home.store();
    let id = agent(
        &home,
        &mut store,
        "failed",
        Some(NOW - HISTORY_SECONDS - 1.0),
    );
    let attempt = store.create_attempt(&id, "failed", &json!({})).unwrap();
    store
        .conn
        .execute(
            "INSERT INTO provider_accounts VALUES ('account','fixture','env:TEST','enabled',0,0)",
            [],
        )
        .unwrap();
    store
        .conn
        .execute(
            "UPDATE attempts SET selected_account_id='account' WHERE id=?",
            [&attempt],
        )
        .unwrap();
    store
        .conn
        .execute(
            "INSERT INTO attempt_quota_keys VALUES (?,'account::pool')",
            [&attempt],
        )
        .unwrap();
    store
        .conn
        .execute(
            "INSERT INTO process_ownership VALUES ('attempt',?,'{}',1,0)",
            [&attempt],
        )
        .unwrap();
    store
        .conn
        .execute(
            "INSERT INTO process_members VALUES ('attempt',?,123,'test','{}')",
            [&attempt],
        )
        .unwrap();
    store
        .conn
        .execute(
            "INSERT INTO agent_service_gates VALUES (?,'ready',NULL)",
            [id.as_str()],
        )
        .unwrap();
    store
        .conn
        .execute(
            "INSERT INTO run_stats(agent_id,runtime,model,profile,status,usage_source,recorded_at)
        VALUES (?,'mock','fixture','review','failed','none',0)",
            [id.as_str()],
        )
        .unwrap();
    let tx = store.conn.transaction().unwrap();
    for _ in 0..2_010 {
        tx.execute(
            "INSERT INTO events(agent_id,attempt_id,at,kind) VALUES (?,?,0,'fixture')",
            params![id.as_str(), attempt],
        )
        .unwrap();
        tx.execute("INSERT INTO messages(agent_id,attempt_id,at,role,content) VALUES (?,?,0,'assistant','fixture')", params![id.as_str(),attempt]).unwrap();
        tx.execute("INSERT INTO commands(agent_id,kind,payload_json,state,created_at) VALUES (?,'steer','{}','completed',0)", [id.as_str()]).unwrap();
    }
    tx.execute("INSERT INTO deliveries(id,agent_id,terminal_event_seq,state) SELECT 'notice',?,max(seq),'retry_wait' FROM events", [id.as_str()]).unwrap();
    for index in 1..=2_010 {
        tx.execute(
            "INSERT INTO delivery_attempt_evidence VALUES ('notice',?,0,'{}')",
            [index],
        )
        .unwrap();
    }
    tx.commit().unwrap();
    assert!(store.prune_history(NOW).unwrap() > 0);
    assert!(store.get(&id).is_ok());
    assert_eq!(count(&store, "messages"), 10);
    assert_eq!(count(&store, "delivery_attempt_evidence"), 10);
    integrity(&store);
    assert!(store.prune_history(NOW).unwrap() > 0);
    for table in [
        "agents",
        "events",
        "messages",
        "commands",
        "deliveries",
        "delivery_attempt_evidence",
        "attempts",
        "attempt_quota_keys",
        "process_members",
        "process_ownership",
        "agent_service_gates",
        "run_stats",
    ] {
        assert_eq!(count(&store, table), 0, "leftover in {table}");
    }
    assert_eq!(count(&store, "provider_accounts"), 1);
    integrity(&store);
}

/// A failure during final deletion rolls back the whole batch instead of losing half its records.
#[test]
fn retention_rolls_back_on_storage_failure() {
    let home = common::Home::new();
    let mut store = home.store();
    let id = agent(
        &home,
        &mut store,
        "failed",
        Some(NOW - HISTORY_SECONDS - 1.0),
    );
    store
        .append_message(&id, "assistant", "retain on failure", None, None, None)
        .unwrap();
    let events = count(&store, "events");
    store.conn.execute_batch("CREATE TRIGGER refuse_retention BEFORE DELETE ON agents BEGIN SELECT RAISE(ABORT,'fixture'); END;").unwrap();
    assert!(store.prune_history(NOW).is_err());
    assert!(store.get(&id).is_ok());
    assert_eq!(count(&store, "messages"), 1);
    assert_eq!(count(&store, "events"), events);
    store
        .conn
        .execute_batch("DROP TRIGGER refuse_retention")
        .unwrap();
    assert!(store.prune_history(NOW).unwrap() > 0);
    integrity(&store);
}

/// Workflow dependencies and live sending leases delay expiry; completed old dependencies drain first.
#[test]
fn retention_respects_workflows_and_delivery_leases() {
    let home = common::Home::new();
    let mut store = home.store();
    let old = NOW - HISTORY_SECONDS - 1.0;
    let id = agent(&home, &mut store, "failed", Some(old));
    store.conn.execute("INSERT INTO workflow_runs(id,name,script_sha,status,created_at,finished_at) VALUES ('workflow','fixture','x','running',0,?)", [old]).unwrap();
    store
        .conn
        .execute(
            "INSERT INTO workflow_steps VALUES ('workflow','step','{}',?,'failed',NULL,NULL,NULL)",
            [id.as_str()],
        )
        .unwrap();
    assert_eq!(store.prune_history(NOW).unwrap(), 0);
    store
        .conn
        .execute("UPDATE workflow_runs SET status='failed'", [])
        .unwrap();
    store.conn.execute("INSERT INTO deliveries(id,agent_id,state,lease_until) VALUES ('sending',?,'sending',?)", params![id.as_str(),NOW+10.0]).unwrap();
    assert!(store.prune_history(NOW).unwrap() > 0);
    assert!(store.get(&id).is_ok());
    assert_eq!(count(&store, "workflow_runs"), 0);
    assert!(store.prune_history(NOW + 11.0).unwrap() > 0);
    assert!(store.get(&id).is_err());
    integrity(&store);
}

/// VACUUM actually shrinks the file, refuses active work, and remains a no-op on a compact database.
#[test]
fn retention_vacuum_reclaims_disk_only_when_idle() {
    let home = common::Home::new();
    let mut store = home.store();
    let id = agent(&home, &mut store, "running", None);
    store
        .conn
        .execute(
            "INSERT INTO capacity_samples(runtime,lane,window,source,payload_json,observed_at)
        VALUES ('mock','shared','fixture','fixture',zeroblob(33554432),?)",
            [NOW - HISTORY_SECONDS - 1.0],
        )
        .unwrap();
    store
        .conn
        .execute_batch("PRAGMA wal_checkpoint(TRUNCATE)")
        .unwrap();
    let before = home.path.join("state.db").metadata().unwrap().len();
    assert!(store.prune_history(NOW).unwrap() > 0);
    assert!(!store.vacuum_history().unwrap());
    store
        .conn
        .execute(
            "UPDATE agents SET status='failed',finished_at=? WHERE id=?",
            params![NOW, id.as_str()],
        )
        .unwrap();
    let unresolved = store.create_attempt(&id, "lost", &json!({})).unwrap();
    store
        .conn
        .execute(
            "UPDATE attempts SET ownership_active=1 WHERE id=?",
            [&unresolved],
        )
        .unwrap();
    assert!(store.vacuum_history().unwrap());
    for _ in 0..16 {
        if !store.vacuum_history().unwrap() {
            break;
        }
    }
    assert_eq!(
        count(&store, "attempts"),
        1,
        "compaction preserves unresolved evidence"
    );
    let after = home.path.join("state.db").metadata().unwrap().len();
    assert!(
        before > after + 16 * 1024 * 1024,
        "before={before}, after={after}"
    );
    assert!(!store.vacuum_history().unwrap());
    assert!(store.get(&id).is_ok());
    integrity(&store);
}

/// Unresolved service leases/probes survive; unreferenced stopped generations and sessions expire.
#[test]
fn retention_keeps_service_recovery_and_account_quota_latches() {
    let home = common::Home::new();
    let mut store = home.store();
    let old = NOW - HISTORY_SECONDS - 1.0;
    let agent = agent(&home, &mut store, "lost", Some(old));
    for id in ["stopped", "probe", "leased", "live"] {
        store
            .conn
            .execute(
                "INSERT INTO managed_service_generations
            (id,service_id,revision,definition_json,state,broker_identity_json,created_at)
            VALUES (?,? ,?,'{}',?,'{}',?)",
                params![
                    id,
                    id,
                    "a".repeat(64),
                    if id == "live" { "ready" } else { "stopped" },
                    old
                ],
            )
            .unwrap();
        store
            .conn
            .execute(
                "INSERT INTO process_ownership VALUES ('service',?,'{}',1,?)",
                params![id, old],
            )
            .unwrap();
    }
    store
        .conn
        .execute(
            "INSERT INTO managed_service_probes VALUES ('probe-owner','probe',?)",
            [old],
        )
        .unwrap();
    store
        .conn
        .execute(
            "INSERT INTO process_ownership VALUES ('probe','probe-owner','{}',1,?)",
            [old],
        )
        .unwrap();
    store
        .conn
        .execute(
            "INSERT INTO managed_service_leases VALUES ('leased',?,?,NULL)",
            params![agent.as_str(), old],
        )
        .unwrap();
    store
        .conn
        .execute(
            "INSERT INTO orchestrator_sessions VALUES ('unused','fixture','unused',NULL,?,?)",
            params![old, old],
        )
        .unwrap();
    store
        .conn
        .execute(
            "INSERT INTO context_receipts VALUES ('unused','fixture',?)",
            [old],
        )
        .unwrap();
    store
        .conn
        .execute(
            "INSERT INTO provider_accounts VALUES ('account','fixture','env:TEST','enabled',0,0)",
            [],
        )
        .unwrap();
    store.conn.execute("INSERT INTO quota_exhaustion VALUES ('account','account::pool','fixture','window',?,NULL,NULL)",[old]).unwrap();
    assert!(store.prune_history(NOW).unwrap() > 0);
    assert_eq!(count(&store, "managed_service_generations"), 3);
    assert_eq!(count(&store, "managed_service_probes"), 1);
    assert_eq!(count(&store, "process_ownership"), 4);
    assert_eq!(count(&store, "context_receipts"), 0);
    assert_eq!(count(&store, "orchestrator_sessions"), 0);
    assert_eq!(count(&store, "quota_exhaustion"), 1);
    assert!(store.get(&agent).is_ok());
    assert!(!store.vacuum_history().unwrap());
    integrity(&store);
}

/// Retention's foreign-key checks use indexes instead of scanning other agents' full journals.
#[test]
fn retention_deletes_do_not_scan_unrelated_journals() {
    let home = common::Home::new();
    let store = home.store();
    for statement in [
        "DELETE FROM attempts WHERE id='fixture'",
        "DELETE FROM events WHERE seq=1",
    ] {
        let plans: Vec<String> = store
            .conn
            .prepare(&format!("EXPLAIN QUERY PLAN {statement}"))
            .unwrap()
            .query_map([], |r| r.get(3))
            .unwrap()
            .collect::<rusqlite::Result<_>>()
            .unwrap();
        for plan in plans {
            assert!(
                !["SCAN events", "SCAN messages", "SCAN deliveries"]
                    .iter()
                    .any(|scan| plan.starts_with(scan)),
                "unbounded FK check: {plan}"
            );
        }
    }
}
