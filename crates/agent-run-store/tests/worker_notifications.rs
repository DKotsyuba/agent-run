//! Worker report security and outbox behavior against a private SQLite home.

use agent_run_domain::{
    domain::AgentId,
    worker::{NotifyRequest, WorkerMessageKind},
    Error,
};
use agent_run_store::{retention::HISTORY_SECONDS, Store};
use rusqlite::params;

/// Fixed test bearer secret; no live worker or process is launched.
const TOKEN: &str = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";

/// Builds one bound running execution with active attempt ownership.
fn running() -> (tempfile::TempDir, Store, AgentId, String) {
    let home = tempfile::tempdir().unwrap();
    let store = Store::initialize(home.path()).unwrap();
    let run: AgentId = "ag-20260928-000000-0123456789".parse().unwrap();
    store.conn.execute(
        "INSERT INTO orchestrator_sessions(id,transport,external_session_id,created_at,last_seen_at)
         VALUES('session','codex_queue','thread',100,100)", [],
    ).unwrap();
    store.conn.execute(
        r#"INSERT INTO agents(id,orchestrator_session_id,runtime,model,profile,task,task_summary,workdir,
          request_json,status,created_at,started_at,timeout_seconds,config_revision,root_agent_id)
         VALUES(?,'session','codex','fixture','review','task','task','/tmp',
         '{"runtime":"codex","model":"fixture","profile":"review","task":"task","workdir":"/tmp"}',
         'running',100,190,120,'fixture',?)"#,
        params![run.as_str(), run.as_str()],
    ).unwrap();
    let attempt = "att-worker-1".to_owned();
    store.conn.execute(
        "INSERT INTO attempts(id,agent_id,number,state,adapter_state_json,created_at,ownership_active)
         VALUES(?,?,1,'running','{}',100,1)",
        params![attempt, run.as_str()],
    ).unwrap();
    (home, store, run, attempt)
}

/// Returns one valid report whose key can be changed by a test.
fn report(key: &str) -> NotifyRequest {
    NotifyRequest {
        request_id: key.into(),
        kind: WorkerMessageKind::Risk,
        message: "Material issue found".into(),
    }
}

/// Immutable pre-normalization session aliases admit reports through the same family.
#[test]
fn stability_legacy_transport_alias_reports() {
    for alias in ["codex", "claude"] {
        let (_home, mut store, run, attempt) = running();
        store
            .issue_worker_capability(&run, &attempt, TOKEN, 100.0)
            .unwrap();
        store
            .conn
            .execute("UPDATE orchestrator_sessions SET transport=?", [alias])
            .unwrap();
        let receipt = store
            .notify_orchestrator(&run, &attempt, TOKEN, &report("alias"), 191.0)
            .unwrap();
        assert_eq!(receipt.state, "pending");
    }
}

/// A delayed start still expires at admission time; accepted reports are durable and idempotent.
#[test]
fn active_report_replay_and_deadline() {
    let (_home, mut store, run, attempt) = running();
    store
        .issue_worker_capability(&run, &attempt, TOKEN, 100.0)
        .unwrap();
    let other_run: AgentId = "ag-20260928-000000-0123456788".parse().unwrap();
    assert!(store
        .issue_worker_capability(&other_run, &attempt, TOKEN, 100.0)
        .is_err());
    assert!(store
        .notify_orchestrator(&other_run, &attempt, TOKEN, &report("one"), 191.0)
        .is_err());
    store
        .conn
        .execute(
            "UPDATE agents SET orchestrator_session_id=NULL WHERE id=?",
            [run.as_str()],
        )
        .unwrap();
    assert!(store
        .notify_orchestrator(&run, &attempt, TOKEN, &report("one"), 191.0)
        .is_err());
    store
        .conn
        .execute(
            "UPDATE agents SET orchestrator_session_id='session',status='cancelling' WHERE id=?",
            [run.as_str()],
        )
        .unwrap();
    assert!(store
        .notify_orchestrator(&run, &attempt, TOKEN, &report("one"), 191.0)
        .is_err());
    store
        .conn
        .execute(
            "UPDATE agents SET status='running' WHERE id=?",
            [run.as_str()],
        )
        .unwrap();
    let first = store
        .notify_orchestrator(&run, &attempt, TOKEN, &report("one"), 191.0)
        .unwrap();
    assert_eq!(first.state, "pending");
    assert!(!first.duplicate);
    let duplicate = store
        .notify_orchestrator(&run, &attempt, TOKEN, &report("one"), 191.0)
        .unwrap();
    assert_eq!(duplicate.notification_id, first.notification_id);
    assert!(duplicate.duplicate);
    assert!(matches!(
        store.notify_orchestrator(&run, &attempt, TOKEN, &report("two"), 191.0),
        Err(Error::Validation(_))
    ));
    assert!(store
        .notify_orchestrator(&run, &attempt, TOKEN, &report("two"), 219.0)
        .is_err());
    assert!(store
        .notify_orchestrator(&run, &attempt, TOKEN, &report("two"), 220.0)
        .is_err());
    let (event_kind, event_data): (String, String) = store.conn.query_row(
        "SELECT e.kind,e.data_json FROM events e JOIN deliveries d ON d.terminal_event_seq=e.seq WHERE d.id=?",
        [&first.notification_id], |r| Ok((r.get(0)?,r.get(1)?)),
    ).unwrap();
    assert_eq!(event_kind, "worker_notification");
    assert!(!event_data.contains("Material issue found"));
    assert_eq!(store.delivery_status(&run).unwrap()["state"], "not_created");
}

/// Forgery, inactive ownership, conflicting replay, and body secret disclosure all fail closed.
#[test]
fn forged_and_inactive_reports_are_rejected() {
    let (_home, mut store, run, attempt) = running();
    store
        .issue_worker_capability(&run, &attempt, TOKEN, 100.0)
        .unwrap();
    assert!(store
        .notify_orchestrator(&run, "wrong-attempt", TOKEN, &report("one"), 191.0)
        .is_err());
    assert!(store
        .notify_orchestrator(&run, &attempt, &"b".repeat(64), &report("one"), 191.0)
        .is_err());
    let first = store
        .notify_orchestrator(&run, &attempt, TOKEN, &report("one"), 191.0)
        .unwrap();
    let mut changed = report("one");
    changed.message = "different".into();
    assert!(matches!(
        store.notify_orchestrator(&run, &attempt, TOKEN, &changed, 192.0),
        Err(Error::Conflict)
    ));
    let mut leak = report("leak");
    leak.message = TOKEN.into();
    assert!(store
        .notify_orchestrator(&run, &attempt, TOKEN, &leak, 192.0)
        .is_err());
    let digest: String = store
        .conn
        .query_row("SELECT token_sha256 FROM worker_capabilities", [], |r| {
            r.get(0)
        })
        .unwrap();
    assert_ne!(digest, TOKEN);
    store
        .conn
        .execute(
            "UPDATE agents SET status='succeeded',finished_at=192 WHERE id=?",
            [run.as_str()],
        )
        .unwrap();
    assert!(store
        .notify_orchestrator(&run, &attempt, TOKEN, &report("one"), 192.0)
        .is_err());
    store
        .conn
        .execute(
            "UPDATE agents SET status='running',finished_at=NULL WHERE id=?",
            [run.as_str()],
        )
        .unwrap();
    store
        .conn
        .execute(
            "UPDATE attempts SET ownership_active=0 WHERE id=?",
            [&attempt],
        )
        .unwrap();
    assert!(store
        .notify_orchestrator(&run, &attempt, TOKEN, &report("one"), 192.0)
        .is_err());
    assert_eq!(
        store
            .conn
            .query_row("SELECT COUNT(*) FROM worker_notifications", [], |r| r
                .get::<_, i64>(0))
            .unwrap(),
        1
    );
    assert!(!first.notification_id.is_empty());
}

/// The twentieth distinct report is accepted; the next one is rejected with no extra row.
#[test]
fn volume_limit_is_per_run() {
    let (_home, mut store, run, attempt) = running();
    store
        .conn
        .execute(
            "UPDATE agents SET timeout_seconds=1000 WHERE id=?",
            [run.as_str()],
        )
        .unwrap();
    store
        .issue_worker_capability(&run, &attempt, TOKEN, 100.0)
        .unwrap();
    for n in 0..20 {
        store
            .notify_orchestrator(
                &run,
                &attempt,
                TOKEN,
                &report(&format!("key-{n}")),
                191.0 + 30.0 * n as f64,
            )
            .unwrap();
    }
    assert!(matches!(
        store.notify_orchestrator(&run, &attempt, TOKEN, &report("overflow"), 791.0),
        Err(Error::Validation(_))
    ));
    assert_eq!(
        store
            .conn
            .query_row("SELECT COUNT(*) FROM worker_notifications", [], |r| r
                .get::<_, i64>(0))
            .unwrap(),
        20
    );
}

/// Retention removes worker rows before their parent attempt and agent records.
#[test]
fn retention_drains_worker_rows() {
    let (_home, mut store, run, attempt) = running();
    store
        .issue_worker_capability(&run, &attempt, TOKEN, 100.0)
        .unwrap();
    let receipt = store
        .notify_orchestrator(&run, &attempt, TOKEN, &report("one"), 191.0)
        .unwrap();
    store
        .conn
        .execute(
            "UPDATE deliveries SET state='delivered' WHERE id=?",
            [&receipt.notification_id],
        )
        .unwrap();
    store
        .conn
        .execute(
            "UPDATE attempts SET ownership_active=0,state='finished',finished_at=200 WHERE id=?",
            [&attempt],
        )
        .unwrap();
    store
        .conn
        .execute(
            "UPDATE agents SET status='succeeded',finished_at=200 WHERE id=?",
            [run.as_str()],
        )
        .unwrap();
    assert!(store.prune_history(200.0 + HISTORY_SECONDS + 1.0).unwrap() > 0);
    for table in [
        "worker_capabilities",
        "worker_notifications",
        "deliveries",
        "agents",
    ] {
        assert_eq!(
            store
                .conn
                .query_row(&format!("SELECT COUNT(*) FROM {table}"), [], |r| r
                    .get::<_, i64>(0))
                .unwrap(),
            0
        );
    }
}
