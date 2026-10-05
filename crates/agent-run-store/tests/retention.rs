//! History expiry tests use private databases and never spawn engine processes.

mod common;

use agent_run_domain::Error;
use agent_run_domain::domain::{AgentId, OrchestratorRef};
use agent_run_store::{Store, retention, retention::HISTORY_SECONDS};
use rusqlite::params;
use serde_json::json;
use std::path::Path;

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

/// Admits one terminal run of a logical session at explicit `created`/`finished`
/// times, so count retention can rank lineages that age expiry would keep.
fn run(home: &common::Home, store: &mut Store, created: f64, finished: f64) -> AgentId {
    let (id, _) = store
        .admit(&home.request(), &home.config, &json!({}), None)
        .unwrap();
    store
        .conn
        .execute(
            "UPDATE agents SET status='succeeded',created_at=?,finished_at=? WHERE id=?",
            params![created, finished, id.as_str()],
        )
        .unwrap();
    id
}

/// Links `child` into `root`'s resume lineage as the tail run.
fn resume(store: &mut Store, root: &AgentId, child: &AgentId) {
    store
        .conn
        .execute(
            "UPDATE agents SET parent_agent_id=?,root_agent_id=?,sequence=2 WHERE id=?",
            params![root.as_str(), root.as_str(), child.as_str()],
        )
        .unwrap();
}

/// Repeats prune passes until one reports no work, bounded by `limit` passes.
fn drain(store: &mut Store, limit: usize) -> usize {
    let mut total = 0;
    for _ in 0..limit {
        let removed = store.prune_history(NOW).unwrap();
        total += removed;
        if removed == 0 {
            return total;
        }
    }
    panic!("retention did not settle within {limit} passes");
}

/// Number of stored logical sessions (distinct resume lineages).
fn sessions(store: &Store) -> i64 {
    store
        .conn
        .query_row(
            "SELECT count(DISTINCT COALESCE(NULLIF(root_agent_id,''),id)) FROM agents",
            [],
            |row| row.get(0),
        )
        .unwrap()
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

/// Filesystem evidence decodes JSON paths and protects disabled-account credentials and aliases.
#[test]
fn storage_protection_preserves_decoded_paths_and_account_files() {
    let home = common::Home::new();
    let mut store = home.store();
    let id = agent(&home, &mut store, "succeeded", Some(NOW));
    let runtime = home.path.join("runtime-résumé-\"quoted\"");
    let answer = home.path.join("answer-old");
    let attempt_home = home.path.join("attempt-old");
    let credential = home.path.join("standalone/backups/old/secret");
    std::fs::create_dir_all(credential.parent().unwrap()).unwrap();
    std::fs::create_dir_all(home.path.join("temporary")).unwrap();
    std::fs::write(&credential, "fixture only").unwrap();
    let identity = json!({"runtime_home": runtime.join("session.json")})
        .to_string()
        .replace('é', "\\u00e9")
        .replace('/', "\\/");
    store
        .conn
        .execute(
            "UPDATE agents SET identity_json=?,answer_path=? WHERE id=?",
            params![
                identity,
                answer.join("answer.txt").to_str().unwrap(),
                id.as_str()
            ],
        )
        .unwrap();
    store
        .create_attempt(&id, "succeeded", &json!({"runtime_home": attempt_home}))
        .unwrap();
    let alias = home.path.join("temporary/../standalone/backups/old/secret");
    store
        .conn
        .execute(
            "INSERT INTO provider_accounts VALUES ('disabled-file','fixture',?,'disabled',0,0)",
            [format!("file:{}", alias.display())],
        )
        .unwrap();
    let mut proof = store.storage_protection_snapshot().unwrap();
    for path in [
        &runtime,
        &answer,
        &attempt_home,
        credential.parent().unwrap(),
    ] {
        assert!(
            proof.retains("unregistered", path),
            "lost reference: {path:?}"
        );
    }
    assert!(proof.retains(id.as_str(), &home.path.join("other")));
    assert!(!proof.retains(
        "unregistered",
        &home.path.join("runtime-résumé-\"quoted\"-other")
    ));
    assert!(!proof.retains("unregistered", &home.path.join("unreferenced")));
    let config_target = home.path.join("config-only/data");
    proof.protect_path(config_target.to_str().unwrap());
    assert!(proof.retains("unregistered", config_target.parent().unwrap()));
    // Ordinary retained metadata can already exceed eight MiB across a home.
    store
        .conn
        .execute(
            "UPDATE agents SET identity_json=json_set(identity_json,'$.padding',?) WHERE id=?",
            params!["x".repeat(9 * 1024 * 1024), id.as_str()],
        )
        .unwrap();
    assert!(
        store
            .storage_protection_snapshot()
            .unwrap()
            .retains("unregistered", &runtime)
    );
    integrity(&store);
}

/// Unreadable structured evidence rejects the entire snapshot and leaves the connection usable.
#[test]
fn storage_protection_rejects_corrupt_evidence_without_losing_history() {
    let home = common::Home::new();
    let mut store = home.store();
    let id = agent(&home, &mut store, "succeeded", Some(NOW));
    store
        .conn
        .execute(
            "UPDATE agents SET identity_json='{' WHERE id=?",
            [id.as_str()],
        )
        .unwrap();
    assert!(store.storage_protection_snapshot().is_err());
    assert_eq!(count(&store, "agents"), 1);
    store
        .conn
        .execute(
            "UPDATE agents SET identity_json='{}' WHERE id=?",
            [id.as_str()],
        )
        .unwrap();
    assert!(store.storage_protection_snapshot().is_ok());
    integrity(&store);
}

/// Excessive metadata, even without JSON payloads, fails closed rather than growing indefinitely.
#[test]
fn storage_protection_bounds_account_rows_and_clears_sql_handler() {
    let home = common::Home::new();
    let store = home.store();
    store.conn.execute_batch(
        "WITH RECURSIVE ids(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM ids WHERE n<20001)
         INSERT INTO provider_accounts SELECT 'bounded-'||n,'fixture','env:BOUND_'||n,'disabled',0,0 FROM ids;"
    ).unwrap();
    assert!(store.storage_protection_snapshot().is_err());
    assert_eq!(count(&store, "provider_accounts"), 20_001);
    std::thread::sleep(std::time::Duration::from_millis(2_100));
    // More than 1,000 VM instructions exercise the progress callback after the failed read.
    let total: i64 = store.conn.query_row(
        "WITH RECURSIVE ids(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM ids WHERE n<10000) SELECT sum(n) FROM ids",
        [], |row| row.get(0),
    ).unwrap();
    assert_eq!(total, 50_005_000);
    integrity(&store);
}

/// A still-prepared storage-layout row pins its runtime home's agents and the
/// home path itself; committing the layout releases both.
#[test]
fn retention_pins_pending_layout_homes_only() {
    let home = common::Home::new();
    let mut store = home.store();
    let runtime_home = home.path.join("runtime").to_string_lossy().to_string();
    let pinned = agent(
        &home,
        &mut store,
        "failed",
        Some(NOW - HISTORY_SECONDS - 1.0),
    );
    store
        .conn
        .execute(
            "UPDATE agents SET identity_json=? WHERE id=?",
            params![
                json!({"runtime_home": runtime_home}).to_string(),
                pinned.as_str()
            ],
        )
        .unwrap();
    let layout = serde_json::to_string(&json!({
        "version": 1,
        "runtime_home": runtime_home,
        "index_sha256": "1".repeat(64),
        "roots": {"assets": {"scope": "2".repeat(64), "manifest_sha256": "3".repeat(64)}},
    }))
    .unwrap();
    let prepared = store.prepare_runtime_storage_layout(&layout, None).unwrap();
    assert_eq!(store.prune_history(NOW).unwrap(), 0);
    assert_eq!(count(&store, "agents"), 1);
    let proof = store.storage_protection_snapshot().unwrap();
    assert!(proof.retains(pinned.as_str(), Path::new(&runtime_home)));
    // Committing turns the row into a plain mapping: the expired agent and its
    // home age out again, and no committed home path is protected.
    store
        .commit_runtime_storage_layout(
            &runtime_home,
            &prepared.operation_token,
            &prepared.layout_sha256,
        )
        .unwrap();
    assert!(store.prune_history(NOW).unwrap() > 0);
    assert_eq!(count(&store, "agents"), 0);
    let proof = store.storage_protection_snapshot().unwrap();
    assert!(!proof.retains("anyone", Path::new(&runtime_home)));
    integrity(&store);
}

/// An idle database takes no writer lock at all: with nothing eligible, prune
/// Safe maintenance diagnostics classify contention and carry code names
/// without any SQLite message text.
#[test]
fn maintenance_diagnostics_map_busy_locked_and_interrupt_codes() {
    for (extended, name) in [
        (5, Some("SQLITE_BUSY")),
        (6, Some("SQLITE_LOCKED")),
        (9, Some("SQLITE_INTERRUPT")),
        (261, Some("SQLITE_BUSY_RECOVERY")),
        (262, Some("SQLITE_LOCKED_SHAREDCACHE")),
        (517, Some("SQLITE_BUSY_SNAPSHOT")),
        (11, None),
    ] {
        let error = Error::Sql(rusqlite::Error::SqliteFailure(
            rusqlite::ffi::Error::new(extended),
            Some("never logged".into()),
        ));
        let codes = retention::sqlite_codes(&error).expect("sqlite failure detail");
        assert_eq!(codes.primary, extended & 0xff);
        assert_eq!(codes.extended, extended);
        assert_eq!(codes.extended_name, name, "extended {extended}");
        if matches!(extended, 5 | 6 | 9 | 261 | 262 | 517) {
            assert!(
                retention::is_writer_contention(&error),
                "extended {extended}"
            );
        } else {
            assert!(
                !retention::is_writer_contention(&error),
                "extended {extended}"
            );
        }
    }
    // Non-SQLite failures classify as neither contention nor SQLite detail.
    let plain = Error::Validation("fixture".into());
    assert!(retention::sqlite_codes(&plain).is_none());
    assert!(!retention::is_writer_contention(&plain));
}

/// An idle database takes no writer lock at all: with nothing eligible, prune
/// completes as a no-op through its read-only probe even while another
/// connection holds the single WAL writer. Regression for the old cadence,
/// which opened `BEGIN IMMEDIATE` before checking for work and failed here.
#[test]
fn idle_prune_skips_the_writer_transaction_entirely() {
    let home = common::Home::new();
    let mut store = home.store();
    let id = agent(&home, &mut store, "running", None);
    let holder = home.store();
    holder.conn.execute_batch("BEGIN IMMEDIATE").unwrap();
    holder
        .conn
        .execute(
            "INSERT INTO capacity_samples(runtime,lane,window,source,payload_json,observed_at)
             VALUES ('mock','shared','fixture','fixture',zeroblob(16),0)",
            [],
        )
        .unwrap();
    // The broker's maintenance connection only waits out a writer briefly.
    store
        .conn
        .busy_timeout(std::time::Duration::from_millis(100))
        .unwrap();
    assert_eq!(store.prune_history(NOW).unwrap(), 0);
    assert!(store.get(&id).is_ok());
    holder.conn.execute_batch("COMMIT").unwrap();
    integrity(&store);
}

/// A held writer makes prune defer — bounded, classified as writer contention,
/// and never reported as completion — and releasing it resumes real progress.
#[test]
fn prune_defers_while_a_writer_holds_the_database_then_resumes() {
    let home = common::Home::new();
    let mut store = home.store();
    let id = agent(
        &home,
        &mut store,
        "failed",
        Some(NOW - HISTORY_SECONDS - 1.0),
    );
    let holder = home.store();
    holder.conn.execute_batch("BEGIN IMMEDIATE").unwrap();
    holder
        .conn
        .execute(
            "INSERT INTO capacity_samples(runtime,lane,window,source,payload_json,observed_at)
             VALUES ('mock','shared','fixture','fixture',zeroblob(16),0)",
            [],
        )
        .unwrap();
    store
        .conn
        .busy_timeout(std::time::Duration::from_millis(100))
        .unwrap();
    let started = std::time::Instant::now();
    let deferred = store.prune_history(NOW);
    let elapsed = started.elapsed();
    let error = deferred.expect_err("a deferred pass must not report completion");
    assert!(
        retention::is_writer_contention(&error),
        "expected writer contention, got {error:?}"
    );
    let codes = retention::sqlite_codes(&error).expect("sqlite failure detail");
    assert_eq!(codes.primary, 5, "primary busy code, got {codes:?}");
    assert!(
        codes.primary_name.contains("Busy"),
        "primary code name, got {codes:?}"
    );
    assert!(
        elapsed < std::time::Duration::from_secs(2),
        "deferral must be bounded by the short busy timeout, took {elapsed:?}"
    );
    assert!(store.get(&id).is_ok(), "deferred work removed nothing");
    holder.conn.execute_batch("COMMIT").unwrap();
    drop(holder);
    assert!(store.prune_history(NOW).unwrap() > 0);
    assert!(store.get(&id).is_err());
    integrity(&store);
}

/// Repeated maintenance passes concurrent with real journal writes, transcript
/// reads and terminal delivery commits never lose or corrupt either side.
#[test]
fn maintenance_concurrent_with_transcripts_and_deliveries_preserves_both() {
    let at = agent_run_domain::domain::now();
    let home = common::Home::new();
    let mut store = home.store();
    let mut request = home.request();
    request.orchestrator = Some(OrchestratorRef {
        transport: "codex_queue".into(),
        external_session_id: "fixture-session".into(),
        external_turn_id: None,
    });
    let (id, _) = store
        .admit(&request, &home.config, &json!({}), None)
        .unwrap();
    store.running(&id, 7).unwrap();
    // A genuine multi-batch backlog keeps every maintenance pass busy writing.
    for _ in 0..3 {
        agent(
            &home,
            &mut store,
            "failed",
            Some(at - HISTORY_SECONDS - 1.0),
        );
    }
    let backlog = at - HISTORY_SECONDS - 1.0;
    for index in 0..4_000i64 {
        store
            .conn
            .execute(
                "INSERT INTO capacity_samples(runtime,lane,window,source,payload_json,observed_at)
                 VALUES ('mock','shared','fixture','fixture',zeroblob(64),?)",
                [backlog - index as f64],
            )
            .unwrap();
    }
    // The maintenance connection mirrors the broker worker: its own SQLite
    // handle, the broker's short busy timeout, and deferral on contention.
    let maintenance_home = home.path.clone();
    let maintenance = std::thread::spawn(move || {
        let mut deferred = 0usize;
        let mut passes = 0usize;
        for _ in 0..24 {
            let Ok(mut store) = Store::open(&maintenance_home) else {
                continue;
            };
            let _ = store
                .conn
                .busy_timeout(std::time::Duration::from_millis(100));
            passes += 1;
            match store.prune_history(at) {
                Ok(_) => {}
                Err(error) if retention::is_writer_contention(&error) => deferred += 1,
                Err(error) => panic!("maintenance failure is not contention: {error:?}"),
            }
            // Compaction stays idle-only: the live agent keeps it a no-op.
            let _ = store.vacuum_history();
        }
        (passes, deferred)
    });
    let messages = 120;
    for index in 0..messages {
        let text = format!("fixture message {index}");
        store
            .append_message(&id, "assistant", &text, None, None, None)
            .unwrap();
        let page = store.transcript_page(&id, 0, 1000).unwrap();
        assert!(page.complete, "concurrent reads stay consistent");
        // Interleave a bounded read of the newest writes.
        let tail = store
            .transcript_page(
                &id,
                (page.messages.len().saturating_sub(2)).max(1) as i64 - 1,
                2,
            )
            .unwrap();
        assert_eq!(tail.messages.len(), 2.min(page.messages.len()));
    }
    let root = home.path.join("agents").join(id.as_str());
    std::fs::create_dir_all(&root).unwrap();
    let proof =
        agent_run_platform::verify::seal(&root, Path::new("answer.md"), "fixture answer").unwrap();
    // Commit the completion while the maintenance thread remains eligible to run.
    // Its real completion time stays inside the same retention window.
    store
        .finish(
            &id,
            &agent_run_domain::domain::Outcome::failure("fixture"),
            Some(&proof),
            None,
        )
        .unwrap();
    let (passes, _deferred) = maintenance.join().unwrap();
    assert_eq!(passes, 24);
    // Repeated passes after the commit must leave the durable completion and
    // its transcript exactly as committed.
    for _ in 0..5 {
        let mut pass = home.store();
        assert_eq!(pass.prune_history(at).unwrap(), 0);
        let _ = pass.vacuum_history();
    }
    // Every transcript message survives maintenance, in order, and the
    // completion delivery stays durable with its terminal event.
    let page = store.transcript_page(&id, 0, 1000).unwrap();
    let contents: Vec<String> = page
        .messages
        .iter()
        .map(|message| message.content.clone())
        .collect();
    assert_eq!(contents.len(), messages);
    for (index, content) in contents.iter().enumerate() {
        assert_eq!(*content, format!("fixture message {index}"));
    }
    let (state, terminal): (String, Option<i64>) = store
        .conn
        .query_row(
            "SELECT state,terminal_event_seq FROM deliveries WHERE agent_id=?",
            [id.as_str()],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .unwrap();
    assert_eq!(state, "pending");
    assert!(terminal.is_some());
    assert_eq!(count(&store, "agents"), 1, "only the retained run remains");
    assert_eq!(count(&store, "capacity_samples"), 0);
    integrity(&store);
}

/// Count retention keeps the newest hundred logical sessions: fewer and exactly
/// hundred change nothing, more expire by lineage rank, and a tied boundary is
/// broken deterministically by root id.
#[test]
fn count_retention_keeps_newest_hundred_sessions_with_deterministic_ties() {
    let home = common::Home::new();
    let mut store = home.store();
    // Fewer than the cap: nothing is eligible and no pass removes a row.
    for index in 0..50 {
        run(&home, &mut store, NOW - 5_000.0 + index as f64, NOW - 10.0);
    }
    assert_eq!(store.prune_history(NOW).unwrap(), 0);
    assert_eq!(sessions(&store), 50);
    // Ninety-nine sessions strictly newer than the tie instant, then two
    // sessions whose latest admitted run shares that one created_at: the
    // hundredth slot is a tie between exactly those two.
    for index in 0..99 {
        run(&home, &mut store, NOW - 900.0 + index as f64, NOW - 10.0);
    }
    let tied: Vec<AgentId> = (0..2)
        .map(|_| run(&home, &mut store, NOW - 1_000.0, NOW - 5.0))
        .collect();
    let (kept, dropped) = if tied[0].as_str() < tied[1].as_str() {
        (&tied[0], &tied[1])
    } else {
        (&tied[1], &tied[0])
    };
    // Fifty older sessions, ninety-nine newer ones and the tied pair.
    assert_eq!(sessions(&store), 151);
    let removed = drain(&mut store, 8);
    assert!(removed > 0, "the tied loser is count-expired");
    assert_eq!(sessions(&store), 100, "exactly the newest hundred survive");
    assert!(
        store.get(kept).is_ok(),
        "tie-break keeps the smaller root id"
    );
    assert!(
        store.get(dropped).is_err(),
        "tie-break drops the larger root id"
    );
    // A settled database stays at the cap: no further pass removes anything.
    assert_eq!(store.prune_history(NOW).unwrap(), 0);
    assert_eq!(sessions(&store), 100);
    integrity(&store);
}

/// A recent resume of an old root counts once, lifts the whole lineage into the
/// protected set, and leaves every chain row usable.
#[test]
fn count_retention_ranks_lineage_once_and_keeps_resumed_chain_usable() {
    let home = common::Home::new();
    let mut store = home.store();
    // One old root resumed recently, plus a hundred newer single-run sessions:
    // the lineage ranks by the resume's created_at, not the root's age.
    let root = run(&home, &mut store, NOW - 3_000.0, NOW - 2_900.0);
    let child = run(&home, &mut store, NOW - 10.0, NOW - 5.0);
    resume(&mut store, &root, &child);
    for index in 0..100 {
        run(&home, &mut store, NOW - 900.0 + index as f64, NOW - 4.0);
    }
    assert_eq!(sessions(&store), 101);
    drain(&mut store, 8);
    // The resumed chain counts once, both rows survive, and history reads work.
    assert_eq!(sessions(&store), 100);
    for id in [&root, &child] {
        let row = store.get(id).unwrap();
        assert_eq!(row.root_agent_id, root, "chain stays intact for {id}");
    }
    // The oldest of the hundred newer sessions is what count expiry removed;
    // the two-row chain keeps both of its rows.
    assert_eq!(count(&store, "agents"), 101);
    integrity(&store);
}

/// A retired multi-run lineage drains tail-first across batches without cap
/// drift: after the first tail deletion its ancestors stay expired and none is
/// stranded behind a broken reference.
#[test]
fn retired_chains_drain_tail_first_without_cap_drift() {
    let home = common::Home::new();
    let mut store = home.store();
    // The count-expired lineage holds three runs; a hundred protected single
    // sessions keep it ranked outside the newest hundred.
    let root = run(&home, &mut store, NOW - 5_000.0, NOW - 4_900.0);
    let middle = run(&home, &mut store, NOW - 4_000.0, NOW - 3_900.0);
    let tail = run(&home, &mut store, NOW - 3_000.0, NOW - 2_900.0);
    resume(&mut store, &root, &middle);
    store
        .conn
        .execute(
            "UPDATE agents SET parent_agent_id=?,root_agent_id=?,sequence=3 WHERE id=?",
            params![middle.as_str(), root.as_str(), tail.as_str()],
        )
        .unwrap();
    for index in 0..100 {
        run(&home, &mut store, NOW - 1_000.0 + index as f64, NOW - 10.0);
    }
    assert_eq!(sessions(&store), 101);
    // First pass removes only the leaf: ancestors keep their child protection.
    assert!(store.prune_history(NOW).unwrap() > 0);
    assert!(store.get(&tail).is_err(), "the leaf run is deleted first");
    assert!(store.get(&middle).is_ok() && store.get(&root).is_ok());
    assert_eq!(
        sessions(&store),
        101,
        "shrinking a lineage must not re-rank it"
    );
    drain(&mut store, 8);
    // The whole chain is gone, including the root: no stranded ancestors.
    for id in [&root, &middle, &tail] {
        assert!(store.get(id).is_err(), "chain row {id} must drain");
    }
    assert_eq!(sessions(&store), 100);
    integrity(&store);
}

/// Protections beat the numerical cap: active descendants, unresolved process
/// ownership and live sending leases inside count-expired lineages all survive.
#[test]
fn count_expiry_never_deletes_protected_work() {
    let home = common::Home::new();
    let mut store = home.store();
    // An old lineage whose newest run is still active: nothing inside may go.
    let root = run(&home, &mut store, NOW - 5_000.0, NOW - 4_900.0);
    let active = run(&home, &mut store, NOW - 4_000.0, NOW - 3_900.0);
    resume(&mut store, &root, &active);
    store
        .conn
        .execute(
            "UPDATE agents SET status='running',finished_at=NULL WHERE id=?",
            [active.as_str()],
        )
        .unwrap();
    // A count-expired run with unresolved ownership, and one holding a live
    // sending lease, each form their own old lineage.
    let owned = run(&home, &mut store, NOW - 6_000.0, NOW - 5_900.0);
    let attempt = store.create_attempt(&owned, "lost", &json!({})).unwrap();
    store
        .conn
        .execute(
            "UPDATE attempts SET ownership_active=1 WHERE id=?",
            [&attempt],
        )
        .unwrap();
    let sending = run(&home, &mut store, NOW - 7_000.0, NOW - 6_900.0);
    store
        .conn
        .execute(
            "INSERT INTO deliveries(id,agent_id,state,lease_until) VALUES ('sending',?,'sending',?)",
            params![sending.as_str(), NOW + 10.0],
        )
        .unwrap();
    for index in 0..99 {
        run(&home, &mut store, NOW - 1_000.0 + index as f64, NOW - 10.0);
    }
    assert_eq!(sessions(&store), 102);
    // Every pass reports no work: protected lineages exceed the cap safely.
    assert_eq!(store.prune_history(NOW).unwrap(), 0);
    for id in [&root, &active, &owned, &sending] {
        assert!(store.get(id).is_ok(), "protected work must survive: {id}");
    }
    assert_eq!(sessions(&store), 102);
    integrity(&store);
}

/// A count-only backlog — nothing age-expired — still trips the read-only
/// probe and takes the writer path, while at or below the cap the probe proves
/// the database idle without acquiring the writer at all.
#[test]
fn count_only_backlog_trips_the_probe_and_the_cap_does_not() {
    let home = common::Home::new();
    let mut store = home.store();
    for index in 0..100 {
        run(&home, &mut store, NOW - 900.0 + index as f64, NOW - 10.0);
    }
    let holder = home.store();
    holder.conn.execute_batch("BEGIN IMMEDIATE").unwrap();
    holder
        .conn
        .execute(
            "INSERT INTO capacity_samples(runtime,lane,window,source,payload_json,observed_at)
             VALUES ('mock','shared','fixture','fixture',zeroblob(16),0)",
            [],
        )
        .unwrap();
    store
        .conn
        .busy_timeout(std::time::Duration::from_millis(100))
        .unwrap();
    // At exactly the cap the probe proves nothing eligible: no writer lock.
    assert_eq!(store.prune_history(NOW).unwrap(), 0);
    holder.conn.execute_batch("COMMIT").unwrap();
    // One more recent session makes a count-only backlog: the probe must see
    // it and the pass must defer on the held writer instead of reporting idle.
    run(&home, &mut store, NOW - 1.0, NOW - 1.0);
    holder.conn.execute_batch("BEGIN IMMEDIATE").unwrap();
    holder
        .conn
        .execute(
            "INSERT INTO capacity_samples(runtime,lane,window,source,payload_json,observed_at)
             VALUES ('mock','shared','fixture','fixture',zeroblob(16),0)",
            [],
        )
        .unwrap();
    let deferred = store.prune_history(NOW);
    let error = deferred.expect_err("a count-only backlog must take the writer");
    assert!(
        retention::is_writer_contention(&error),
        "expected writer contention, got {error:?}"
    );
    holder.conn.execute_batch("COMMIT").unwrap();
    drop(holder);
    drain(&mut store, 8);
    assert_eq!(sessions(&store), 100);
    integrity(&store);
}

/// Large journals of count-expired lineages drain in bounded batches, exactly
/// as age-expired ones do, until the whole session is gone.
#[test]
fn count_expired_large_transcripts_drain_in_bounded_batches() {
    let home = common::Home::new();
    let mut store = home.store();
    let id = run(&home, &mut store, NOW - 5_000.0, NOW - 4_900.0);
    let attempt = store.create_attempt(&id, "succeeded", &json!({})).unwrap();
    let tx = store.conn.transaction().unwrap();
    for _ in 0..2_010 {
        tx.execute(
            "INSERT INTO events(agent_id,attempt_id,at,kind) VALUES (?,?,0,'fixture')",
            params![id.as_str(), attempt],
        )
        .unwrap();
        tx.execute(
            "INSERT INTO messages(agent_id,attempt_id,at,role,content) VALUES (?,?,0,'assistant','fixture')",
            params![id.as_str(), attempt],
        )
        .unwrap();
    }
    tx.commit().unwrap();
    for index in 0..100 {
        run(&home, &mut store, NOW - 900.0 + index as f64, NOW - 10.0);
    }
    // First batch is bounded and keeps the agent row until its journals drain.
    assert!(store.prune_history(NOW).unwrap() > 0);
    assert!(store.get(&id).is_ok());
    assert_eq!(count(&store, "messages"), 10);
    drain(&mut store, 8);
    assert!(store.get(&id).is_err());
    assert_eq!(sessions(&store), 100);
    integrity(&store);
}

/// Count-retirement evidence comes from a fresh row read plus the snapshot's
/// pass-wide boundary, never from the id's second-resolution timestamp: a run
/// admitted after the protection snapshot — in the same wall-clock second,
/// with an early-generated id, or under a rolled-back clock — is not retired
/// while its row exists, retirement stays actionable at exactly the cap, and
/// below the cap there is no boundary so recent orphans keep the age rule.
#[test]
fn count_retirement_is_proved_by_a_fresh_row_not_id_timestamps() {
    let home = common::Home::new();
    let store = home.store();
    // The stale snapshot a filesystem pass held when the racing admission
    // landed: it must not retain the racer, so only the fresh row read can.
    // The racer is a bare agents row, like one whose dependents never landed.
    let stale = store.storage_protection_snapshot().unwrap();
    assert_eq!(
        stale.count_boundary(),
        None,
        "an empty store has no boundary"
    );
    let racer = format!("ag-20260101-000000-{:010x}", NOW as i64);
    store
        .conn
        .execute(
            "INSERT INTO agents(id,runtime,model,profile,task,task_summary,workdir,request_json,
             status,created_at,finished_at,timeout_seconds,config_revision,root_agent_id)
             VALUES(?,'mock','fixture','review','task','task','/tmp','{}',
             'succeeded',?,?,100,'fixture',?)",
            params![racer, NOW, NOW - 1.0, racer],
        )
        .unwrap();
    let racer_dir = home.path.join("agents").join(&racer);
    assert!(
        !stale.retains(&racer, &racer_dir),
        "the snapshot predates the racing admission"
    );
    // A hundred newer sessions put the count boundary far above any
    // early-encoded id the racer could carry; bare rows keep deletion cheap.
    for index in 0..100 {
        store
            .conn
            .execute(
                "INSERT INTO agents(id,runtime,model,profile,task,task_summary,workdir,request_json,
                 status,created_at,finished_at,timeout_seconds,config_revision,root_agent_id)
                 VALUES(?,'mock','fixture','review','task','task','/tmp','{}',
                 'succeeded',?,?,100,'fixture',?)",
                params![
                    format!("ag-20260101-000000-{:010x}", index),
                    NOW + 1_000.0 + index as f64,
                    NOW + 900.0,
                    format!("ag-20260101-000000-{:010x}", index),
                ],
            )
            .unwrap();
    }
    // The racer's row exists: however early its encoded creation reads — an
    // id generated before admission or under a rolled-back clock — the fresh
    // indexed row check keeps its tree.
    assert!(store.run_admitted(&racer).unwrap());
    let boundary = store
        .storage_protection_snapshot()
        .unwrap()
        .count_boundary();
    assert!(boundary.is_some_and(|boundary| boundary > 0.0));
    // Retention removed the row while exactly a hundred sessions remain: the
    // boundary must stay actionable at the cap itself, not only above it.
    store
        .conn
        .execute("DELETE FROM agents WHERE id=?", [&racer])
        .unwrap();
    assert_eq!(sessions(&store), 100);
    let at_cap = store.storage_protection_snapshot().unwrap();
    assert_eq!(
        at_cap.count_boundary(),
        Some(NOW + 1_000.0),
        "the hundredth-newest session still bounds count retirement at the cap"
    );
    assert!(!store.run_admitted(&racer).unwrap());
    // Below the cap no lineage can rank outside the protected set: no
    // boundary exists, so recent orphan trees keep the fourteen-day rule.
    store
        .conn
        .execute(
            "DELETE FROM agents WHERE id=?",
            ["ag-20260101-000000-0000000000"],
        )
        .unwrap();
    assert_eq!(sessions(&store), 99);
    assert_eq!(
        store
            .storage_protection_snapshot()
            .unwrap()
            .count_boundary(),
        None
    );
    integrity(&store);
}

/// Inserts one open pool whose current members are `members` (slot order) with
/// one operator entry, and returns its identity.
fn pool(store: &Store, members: &[&AgentId]) -> String {
    let id = "pool-20200101-000000-0123456789".to_owned();
    store
        .conn
        .execute(
            "INSERT INTO pools(id,request_namespace,request_id,request_sha256,goal,acceptance_json,state,roster_revision,created_at) \
             VALUES(?,'ns','r',lower(hex(zeroblob(32))),'goal','[]','open',1,1.0)",
            [&id],
        )
        .unwrap();
    for (slot, member) in members.iter().enumerate() {
        store
            .conn
            .execute(
                "INSERT INTO pool_members(agent_id,pool_id,slot,name,role,personal_task,joined_roster_revision) VALUES(?,?,?,?,'r','t',1)",
                params![member.as_str(), id, slot + 1, format!("m{slot}")],
            )
            .unwrap();
    }
    store
        .conn
        .execute(
            "INSERT INTO pool_entries(pool_id,author_kind,direction,kind,roster_revision,body,idem_scope,request_id,created_at) VALUES(?,'operator','team','message',1,'hi','op','k',1.0)",
            [&id],
        )
        .unwrap();
    id
}

/// A workflow-protected member keeps the pool log and roster until its dependency expires.
#[test]
fn stability_pool_history_follows_member_workflow_protection() {
    let home = common::Home::new();
    let mut store = home.store();
    let old = NOW - HISTORY_SECONDS - 1.0;
    let id = agent(&home, &mut store, "failed", Some(old));
    pool(&store, &[&id]);
    store.conn.execute("INSERT INTO workflow_runs(id,name,script_sha,status,created_at,finished_at) VALUES('workflow','fixture','x','running',0,?)", [old]).unwrap();
    store
        .conn
        .execute(
            "INSERT INTO workflow_steps VALUES('workflow','step','{}',?,'failed',NULL,NULL,NULL)",
            [id.as_str()],
        )
        .unwrap();
    assert_eq!(drain(&mut store, 8), 0);
    assert_eq!(count(&store, "pool_entries"), 1);
    store
        .conn
        .execute("UPDATE workflow_runs SET status='failed'", [])
        .unwrap();
    assert!(drain(&mut store, 8) > 0);
    assert_eq!(count(&store, "pools"), 0);
    integrity(&store);
}

/// Every member lineage of a pool, replaced members included, stays stored
/// while any member is not expired; once all are, the pool log, roster and
/// pool rows go before the agents and every foreign key still resolves.
#[test]
fn pool_members_are_protected_until_the_whole_pool_expires_then_purged_first() {
    let home = common::Home::new();
    let mut store = home.store();
    let cutoff = NOW - HISTORY_SECONDS;
    let old = agent(&home, &mut store, "succeeded", Some(cutoff - 1.0));
    let replaced = agent(&home, &mut store, "failed", Some(cutoff - 1.0));
    let recent = agent(&home, &mut store, "succeeded", Some(NOW));
    let pool_id = pool(&store, &[&old, &replaced]);
    // `replaced` is retired in favour of `recent`: history keeps its row.
    store
        .conn
        .execute(
            "INSERT INTO pool_members(agent_id,pool_id,slot,name,role,personal_task,joined_roster_revision) VALUES(?,?,3,'m9','r','t',2)",
            params![recent.as_str(), pool_id],
        )
        .unwrap();
    store
        .conn
        .execute(
            "UPDATE pool_members SET replaced_by=? WHERE agent_id=?",
            params![recent.as_str(), replaced.as_str()],
        )
        .unwrap();
    assert_eq!(drain(&mut store, 8), 0, "nothing is releasable yet");
    for id in [&old, &replaced, &recent] {
        assert!(store.get(id).is_ok(), "must retain {id}");
    }
    assert_eq!(count(&store, "pool_entries"), 1);
    store
        .conn
        .execute(
            "UPDATE agents SET finished_at=? WHERE id=?",
            params![cutoff - 1.0, recent.as_str()],
        )
        .unwrap();
    assert!(drain(&mut store, 8) > 0);
    for table in ["pools", "pool_members", "pool_entries"] {
        assert_eq!(count(&store, table), 0, "{table}");
    }
    for id in [&old, &replaced, &recent] {
        assert!(store.get(id).is_err(), "expired agent {id} must be gone");
    }
    integrity(&store);
}

/// A queued linked notice or an owned or uncleaned attempt keeps the pool;
/// once final and clean the pool is released.
#[test]
fn pool_with_pending_notice_or_unknown_cleanup_is_kept() {
    let home = common::Home::new();
    let mut store = home.store();
    let cutoff = NOW - HISTORY_SECONDS;
    let first = agent(&home, &mut store, "succeeded", Some(cutoff - 1.0));
    let second = agent(&home, &mut store, "succeeded", Some(cutoff - 1.0));
    pool(&store, &[&first, &second]);
    store
        .conn
        .execute(
            "INSERT INTO deliveries(id,agent_id,state,next_attempt_at) VALUES('ntf_pending',?,'pending',1.0)",
            [first.as_str()],
        )
        .unwrap();
    let attempt = store
        .create_attempt(&second, "succeeded", &json!({}))
        .unwrap();
    store
        .conn
        .execute(
            "UPDATE attempts SET ownership_active=0,phase='running',cleanup_proof_json=NULL WHERE id=?",
            [&attempt],
        )
        .unwrap();
    assert_eq!(drain(&mut store, 8), 0);
    assert_eq!(count(&store, "pools"), 1);
    store
        .conn
        .execute("UPDATE deliveries SET state='retry_wait'", [])
        .unwrap();
    assert_eq!(drain(&mut store, 8), 0, "retry_wait still protects");
    store
        .conn
        .execute("UPDATE deliveries SET state='delivered'", [])
        .unwrap();
    assert_eq!(drain(&mut store, 8), 0, "unknown cleanup still protects");
    store
        .conn
        .execute(
            "UPDATE attempts SET phase='cleanup_complete',cleanup_proof_json='{}' WHERE id=?",
            [&attempt],
        )
        .unwrap();
    assert!(drain(&mut store, 8) > 0);
    assert_eq!(count(&store, "pools"), 0);
    integrity(&store);
}

/// A completed but never bound pool's waiting common notice is expired
/// explicitly once the whole pool has aged out, then purged cleanly.
#[test]
fn unbound_waiting_completion_notice_expires_with_the_aged_out_pool() {
    let home = common::Home::new();
    let mut store = home.store();
    let cutoff = NOW - HISTORY_SECONDS;
    let first = agent(&home, &mut store, "succeeded", Some(cutoff - 1.0));
    let second = agent(&home, &mut store, "succeeded", Some(cutoff - 1.0));
    let pool_id = pool(&store, &[&first, &second]);
    store
        .conn
        .execute(
            "INSERT INTO events(agent_id,at,kind,data_json) VALUES(?,1.0,'pool_completed','{}')",
            [first.as_str()],
        )
        .unwrap();
    let event = store.conn.last_insert_rowid();
    store
        .conn
        .execute(
            "INSERT INTO deliveries(id,agent_id,terminal_event_seq,state) VALUES('ntf_wait',?,?,'waiting_binding')",
            params![first.as_str(), event],
        )
        .unwrap();
    store
        .conn
        .execute(
            "UPDATE pools SET state='completed',completed_at=2.0,completion_delivery_id='ntf_wait' WHERE id=?",
            [&pool_id],
        )
        .unwrap();
    assert!(drain(&mut store, 8) > 0);
    for table in ["pools", "pool_members", "pool_entries", "deliveries"] {
        assert_eq!(count(&store, table), 0, "{table}");
    }
    integrity(&store);
}

/// Protected pool members alone never make a pass take the writer lock: the
/// read-only preflight proves there is nothing to do.
#[test]
fn protected_only_pools_do_not_wake_the_writer() {
    let home = common::Home::new();
    let mut store = home.store();
    let cutoff = NOW - HISTORY_SECONDS;
    let old = agent(&home, &mut store, "succeeded", Some(cutoff - 1.0));
    let recent = agent(&home, &mut store, "succeeded", Some(NOW));
    pool(&store, &[&old, &recent]);
    // Another connection holds the writer lock: a pass that tried to write
    // would fail with contention, while a preflight-clean pass returns Ok(0).
    let holder = home.store();
    holder.conn.execute_batch("BEGIN IMMEDIATE").unwrap();
    store
        .conn
        .busy_timeout(std::time::Duration::from_millis(0))
        .unwrap();
    assert_eq!(store.prune_history(NOW).unwrap(), 0);
    holder.conn.execute_batch("ROLLBACK").unwrap();
}
