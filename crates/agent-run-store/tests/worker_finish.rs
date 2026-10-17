//! Explicit callback authentication, immutable answer and unbounded idle semantics.
use agent_run_domain::{
    Error,
    domain::AgentId,
    worker::{FinishRequest, FinishStatus},
};
use agent_run_store::Store;
use serde_json::json;

/// Static fixture capability with no live provider or credential material.
const TOKEN: &str = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";

/// Create one owned running attempt, optionally with the frozen callback mode.
fn running(explicit: bool) -> (tempfile::TempDir, Store<'static>, AgentId) {
    let home = tempfile::tempdir().unwrap();
    let mut store = agent_run_store::fixture::initialize(home.path()).unwrap();
    let run: AgentId = "ag-20261010-000000-0123456789".parse().unwrap();
    store.conn.execute("INSERT INTO agents(id,runtime,model,profile,task,task_summary,workdir,request_json,status,created_at,config_revision,root_agent_id) VALUES(?,'codex','fixture','review','task','task','/tmp',?,'running',100,'fixture',?)",
        rusqlite::params![run.as_str(),json!({"runtime":"codex","model":"fixture","profile":"review","task":"task","workdir":"/tmp","explicit_finish":explicit}).to_string(),run.as_str()]).unwrap();
    store.conn.execute("INSERT INTO attempts(id,agent_id,number,state,adapter_state_json,created_at,ownership_active) VALUES('attempt',?,1,'running','{}',100,1)",[run.as_str()]).unwrap();
    store
        .issue_worker_capability(&run, "attempt", TOKEN, 100.0)
        .unwrap();
    store
        .begin_worker_lifecycle(&run, "attempt", 100.0)
        .unwrap();
    (home, store, run)
}

/// Return the minimal strict final report; default done remains canonical.
fn report() -> FinishRequest {
    FinishRequest {
        summary: "Verified final summary".into(),
        status: FinishStatus::Done,
    }
}

/// Callback intent persists through restart; receipt, retries, conflict and
/// turn fences preserve one summary while idle never expires worker authority.
#[test]
fn finish_persists_and_idle_never_expires() {
    let (home, mut store, run) = running(true);
    store.worker_turn(&run, true, 101.0).unwrap();
    assert!(
        agent_run_store::worker::authenticate_attempt(&store.conn, &run, "attempt", TOKEN, 1e12)
            .unwrap()
            .unwrap()
            .running
    );
    store.worker_turn(&run, false, 1e12).unwrap();
    assert_eq!(
        store.worker_lifecycle_view(&run).unwrap().unwrap()["turn_count"],
        2
    );
    let receipt = store
        .accept_worker_finish(
            &run,
            "attempt",
            TOKEN,
            &report(),
            1e12,
            agent_run_domain::worker::FinishGuard::Clean,
        )
        .unwrap();
    assert!(!receipt.duplicate);
    assert!(store.worker_finish_intent(&run, true).unwrap().is_none());
    assert!(
        store
            .conn
            .execute("UPDATE worker_lifecycle SET finish_json='{}'", [])
            .is_err()
    );
    assert!(
        !agent_run_store::worker::authenticate_attempt(&store.conn, &run, "attempt", TOKEN, 1e12)
            .unwrap()
            .unwrap()
            .running
    );
    assert!(
        store
            .accept_worker_finish(
                &run,
                "attempt",
                TOKEN,
                &report(),
                1e12 + 1.0,
                agent_run_domain::worker::FinishGuard::Clean
            )
            .unwrap()
            .duplicate
    );
    let mut other = report();
    other.summary = "Different".into();
    assert!(matches!(
        store.accept_worker_finish(
            &run,
            "attempt",
            TOKEN,
            &other,
            1e12,
            agent_run_domain::worker::FinishGuard::Clean
        ),
        Err(Error::Conflict)
    ));
    store.worker_turn(&run, false, 1e12 + 2.0).unwrap();
    assert_eq!(
        store.worker_lifecycle_view(&run).unwrap().unwrap()["phase"],
        "closing"
    );
    drop(store);
    let store = agent_run_store::fixture::open(home.path()).unwrap();
    assert_eq!(
        store.worker_finish_intent(&run, false).unwrap(),
        Some(report())
    );
    store.observe_worker_finish_receipt(&run).unwrap();
    assert_eq!(
        store.worker_finish_intent(&run, true).unwrap(),
        Some(report())
    );
    let event = store
        .last_event(&run, "worker_finish_accepted_v1")
        .unwrap()
        .unwrap();
    assert_eq!(event["sha256"], receipt.sha256);
    assert!(!event.to_string().contains("Verified final summary"));
}

/// Mode, wrong run/attempt/secret, malformed payload and pending cancellation
/// fail before intent; closed/retired capabilities cannot be replayed.
#[test]
fn callback_authority_and_validation_are_closed() {
    for explicit in [false, true] {
        let (_home, mut store, run) = running(explicit);
        let other: AgentId = "ag-20261010-000000-0123456788".parse().unwrap();
        for (id, attempt, token) in [
            (&other, "attempt", TOKEN),
            (&run, "wrong", TOKEN),
            (
                &run,
                "attempt",
                "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
            ),
        ] {
            assert!(
                store
                    .accept_worker_finish(
                        id,
                        attempt,
                        token,
                        &report(),
                        200.0,
                        agent_run_domain::worker::FinishGuard::Clean
                    )
                    .is_err()
            );
        }
        if !explicit {
            assert!(
                store
                    .accept_worker_finish(
                        &run,
                        "attempt",
                        TOKEN,
                        &report(),
                        200.0,
                        agent_run_domain::worker::FinishGuard::Clean
                    )
                    .is_err()
            );
            continue;
        }
        for text in [
            " ".to_owned(),
            "bad\0text".into(),
            TOKEN.into(),
            "x".repeat(65537),
        ] {
            let mut r = report();
            r.summary = text;
            assert!(
                store
                    .accept_worker_finish(
                        &run,
                        "attempt",
                        TOKEN,
                        &r,
                        200.0,
                        agent_run_domain::worker::FinishGuard::Clean
                    )
                    .is_err()
            );
        }
        assert!(
            serde_json::from_value::<FinishRequest>(json!({"summary":"x","agent_id":"other"}))
                .is_err()
        );
        store
            .conn
            .execute("UPDATE agents SET status='cancelling'", [])
            .unwrap();
        assert!(
            store
                .accept_worker_finish(
                    &run,
                    "attempt",
                    TOKEN,
                    &report(),
                    200.0,
                    agent_run_domain::worker::FinishGuard::Clean
                )
                .is_err()
        );
        store
            .conn
            .execute("UPDATE agents SET status='running'", [])
            .unwrap();
        store
            .accept_worker_finish(
                &run,
                "attempt",
                TOKEN,
                &report(),
                200.0,
                agent_run_domain::worker::FinishGuard::Clean,
            )
            .unwrap();
        store
            .conn
            .execute("UPDATE attempts SET ownership_active=0", [])
            .unwrap();
        assert!(
            store
                .accept_worker_finish(
                    &run,
                    "attempt",
                    TOKEN,
                    &report(),
                    201.0,
                    agent_run_domain::worker::FinishGuard::Clean
                )
                .is_err()
        );
    }
}

/// Migration from an exact historical schema 28 (the current schema with the
/// schema-29 lifecycle, schema-30 writer and schema-31 Bridge tables and columns
/// removed) through every later migration leaves legacy request JSON intact
/// and adds no implicit lifecycle/finish state to preexisting attempts.
#[test]
fn migration_28_to_29_preserves_legacy_requests() {
    let (home, mut store, run) = running(false);
    let request: String = store
        .conn
        .query_row(
            "SELECT request_json FROM agents WHERE id=?",
            [run.as_str()],
            |r| r.get(0),
        )
        .unwrap();
    store
        .conn
        .execute_batch(
            "DROP TABLE worker_native_wakes; DROP TABLE worker_lifecycle; \
             DROP TABLE operation_receipts; DROP TABLE writer_producers; DROP TABLE writer_authority; \
             DROP TABLE bridge_delivery_intents; \
             ALTER TABLE agents DROP COLUMN supervisor_capability_sha256; \
             ALTER TABLE managed_service_generations DROP COLUMN exec_capability_sha256; \
             DROP INDEX idx_events_agent_kind_seq; \
             PRAGMA user_version=28;",
        )
        .unwrap();
    drop(store);
    agent_run_store::migrations::migrate(&home.path().join("state.db")).unwrap();
    store = agent_run_store::fixture::open(home.path()).unwrap();
    assert_eq!(
        store
            .conn
            .pragma_query_value::<i64, _>(None, "user_version", |r| r.get(0))
            .unwrap(),
        agent_run_store::VERSION
    );
    assert_eq!(
        store
            .conn
            .query_row::<String, _, _>(
                "SELECT request_json FROM agents WHERE id=?",
                [run.as_str()],
                |r| r.get(0)
            )
            .unwrap(),
        request
    );
    assert!(store.worker_lifecycle_view(&run).unwrap().is_none());
}

/// Invalid structured answers remain retryable; local refs work and external
/// schemas cannot fetch files/network. Exact JSON bytes become the final answer.
#[test]
fn output_schema_is_checked_before_finish_intent() {
    let (_home, mut store, run) = running(true);
    let mut row = store.get(&run).unwrap().request;
    row.output_schema=Some(json!({"type":"object","properties":{"ok":{"type":"boolean"}},"required":["ok"],"additionalProperties":false}).as_object().unwrap().clone());
    store
        .conn
        .execute(
            "UPDATE agents SET request_json=? WHERE id=?",
            rusqlite::params![serde_json::to_string(&row).unwrap(), run.as_str()],
        )
        .unwrap();
    for text in ["not JSON", r#"{"ok":3}"#, r#"{"ok":true,"extra":0}"#] {
        let r = FinishRequest {
            summary: text.into(),
            status: FinishStatus::Done,
        };
        assert!(
            store
                .accept_worker_finish(
                    &run,
                    "attempt",
                    TOKEN,
                    &r,
                    200.0,
                    agent_run_domain::worker::FinishGuard::Clean
                )
                .is_err()
        );
        assert!(store.worker_finish_intent(&run, false).unwrap().is_none());
    }
    let r = FinishRequest {
        summary: "{ \"ok\" : true }".into(),
        status: FinishStatus::Done,
    };
    store
        .accept_worker_finish(
            &run,
            "attempt",
            TOKEN,
            &r,
            201.0,
            agent_run_domain::worker::FinishGuard::Clean,
        )
        .unwrap();
    assert_eq!(store.worker_finish_intent(&run, false).unwrap(), Some(r));
    for reference in ["file:///etc/passwd", "https://example.invalid/schema"] {
        assert!(
            agent_run_domain::worker::finish_schema(json!({"$ref":reference}).as_object().unwrap())
                .is_err()
        );
    }
}

/// Native jobs enqueue durably once, stay pending through active turns, and
/// cannot create a new turn after finish. Operator control remains available.
#[test]
fn native_wakes_are_durable_deduplicated_and_fenced() {
    let (_home, mut store, run) = running(true);
    let key = "b".repeat(64);
    assert!(
        store
            .enqueue_native_wake(&run, &key, "native result", 101.0)
            .unwrap()
    );
    assert!(
        store
            .enqueue_native_wake(&run, &key, "native result", 102.0)
            .unwrap()
    );
    assert!(store.claim_command_for_turn(&run, false).unwrap().is_none());
    store
        .enqueue(&run, "steer", &json!({"text":"operator"}))
        .unwrap();
    let (cmd, _, payload) = store.claim_command_for_turn(&run, false).unwrap().unwrap();
    assert_eq!(payload["text"], "operator");
    store
        .complete_command(&run, cmd, &json!({"accepted":true}))
        .unwrap();
    let (cmd, _, payload) = store.claim_command_for_turn(&run, true).unwrap().unwrap();
    assert_eq!(payload["native_completion"], true);
    store
        .complete_command(&run, cmd, &json!({"accepted":true}))
        .unwrap();
    assert_eq!(
        store
            .conn
            .query_row::<i64, _, _>("SELECT COUNT(*) FROM worker_native_wakes", [], |r| r.get(0))
            .unwrap(),
        1
    );
    store
        .accept_worker_finish(
            &run,
            "attempt",
            TOKEN,
            &report(),
            103.0,
            agent_run_domain::worker::FinishGuard::Clean,
        )
        .unwrap();
    assert!(
        !store
            .enqueue_native_wake(&run, &"c".repeat(64), "late", 104.0)
            .unwrap()
    );
    assert!(
        store
            .enqueue(&run, "steer", &json!({"text":"late"}))
            .is_err()
    );
    assert!(store.enqueue(&run, "cancel", &json!({})).is_ok());
}

/// An accepted intent whose native finish reply was never observed cannot
/// become any run's public answer: a matching proof is refused even for a
/// failed outcome, the same failure without an answer commits, and the
/// observed receipt still permits a blocked/failed worker's answer.
#[test]
fn unobserved_finish_intent_is_never_a_public_answer() {
    use agent_run_domain::domain::{Outcome, Status};
    for observed in [false, true] {
        let (home, mut store, run) = running(true);
        let input = FinishRequest {
            status: FinishStatus::Failed,
            ..report()
        };
        store
            .accept_worker_finish(
                &run,
                "attempt",
                TOKEN,
                &input,
                200.0,
                agent_run_domain::worker::FinishGuard::Clean,
            )
            .unwrap();
        if observed {
            store.observe_worker_finish_receipt(&run).unwrap();
        }
        let root = home.path().join("agents").join(run.as_str());
        agent_run_platform::fs::private_dir(&root).unwrap();
        let proof = agent_run_platform::verify::seal(
            &root,
            std::path::Path::new("answer.md"),
            &input.summary,
        )
        .unwrap();
        let outcome = Outcome::failure("runtime_protocol_failed");
        if observed {
            store.finish(&run, &outcome, Some(&proof), None).unwrap();
            assert!(store.get(&run).unwrap().answer_path.is_some());
        } else {
            assert!(store.finish(&run, &outcome, Some(&proof), None).is_err());
            assert_eq!(store.get(&run).unwrap().status, Status::Running);
            store.finish(&run, &outcome, None, None).unwrap();
            assert!(store.get(&run).unwrap().answer_path.is_none());
        }
        assert_eq!(store.get(&run).unwrap().status, Status::Failed);
    }
}

/// Cancellation arriving after accepted finish but before terminal commit wins
/// for done, blocked and failed alike, without replacing the immutable summary.
#[test]
fn cancel_after_finish_wins_at_the_terminal_transaction() {
    use agent_run_domain::domain::{Outcome, Status};
    for status in [
        FinishStatus::Done,
        FinishStatus::Blocked,
        FinishStatus::Failed,
    ] {
        let (home, mut store, run) = running(true);
        let input = FinishRequest { status, ..report() };
        store
            .accept_worker_finish(
                &run,
                "attempt",
                TOKEN,
                &input,
                200.0,
                agent_run_domain::worker::FinishGuard::Clean,
            )
            .unwrap();
        store.observe_worker_finish_receipt(&run).unwrap();
        let root = home.path().join("agents").join(run.as_str());
        agent_run_platform::fs::private_dir(&root).unwrap();
        let proof = agent_run_platform::verify::seal(
            &root,
            std::path::Path::new("answer.md"),
            &input.summary,
        )
        .unwrap();
        let outcome = if status == FinishStatus::Done {
            Outcome::success(None)
        } else {
            assert!(
                store
                    .finish(&run, &Outcome::success(None), Some(&proof), None)
                    .is_err()
            );
            assert_eq!(store.get(&run).unwrap().status, Status::Running);
            Outcome::failure("worker_declared_failure")
        };
        store.enqueue(&run, "cancel", &json!({})).unwrap();
        store.finish(&run, &outcome, Some(&proof), None).unwrap();
        assert_eq!(store.get(&run).unwrap().status, Status::Cancelled);
        assert!(store.worker_finish_intent(&run, true).unwrap().is_none());
        let preserved: String = store
            .conn
            .query_row(
                "SELECT finish_json FROM worker_lifecycle WHERE attempt_id='attempt'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(
            serde_json::from_str::<FinishRequest>(&preserved).unwrap(),
            input
        );
        assert_eq!(
            store
                .conn
                .query_row::<String, _, _>(
                    "SELECT state FROM commands WHERE agent_id=? AND kind='cancel'",
                    [run.as_str()],
                    |r| r.get(0)
                )
                .unwrap(),
            "completed"
        );
    }
}

/// The shared observer DTO exposes idle state and stops its counter at terminal
/// time, including native exit without a callback. Legacy wire fields stay absent.
#[test]
fn typed_idle_observations_freeze_after_native_exit() {
    let (_home, store, run) = running(true);
    store.worker_turn(&run, true, 101.0).unwrap();
    let idle = store.agent_view_at(&run, 120.0).unwrap();
    assert_eq!(idle.phase, "idle");
    assert_eq!(idle.completion_mode.as_deref(), Some("explicit_finish"));
    assert_eq!(idle.turn_count, Some(1));
    assert_eq!(idle.idle_seconds, Some(19.0));
    assert!(
        serde_json::from_value::<agent_run_domain::views::AgentView>(
            serde_json::to_value(idle).unwrap()
        )
        .is_ok()
    );
    store
        .conn
        .execute(
            "UPDATE agents SET status='failed',finished_at=150 WHERE id=?",
            [run.as_str()],
        )
        .unwrap();
    for at in [1000.0, 2000.0] {
        let terminal = store.agent_view_at(&run, at).unwrap();
        assert_eq!(terminal.phase, "terminal");
        assert_eq!(terminal.idle_seconds, Some(49.0));
    }
    let (_legacy_home, legacy, id) = running(false);
    let view = serde_json::to_value(legacy.agent_view_at(&id, 120.0).unwrap()).unwrap();
    for field in ["completion_mode", "turn_count", "idle_seconds"] {
        assert!(view.get(field).is_none());
    }
}

/// Only a clean broker verdict creates the first finish intent; an
/// unavailable or secret verdict writes neither intent nor event, while an
/// identical replay of the accepted intent is answered whatever the verdict
/// and a changed payload still conflicts.
#[test]
fn finish_guard_verdict_gates_only_the_first_intent() {
    use agent_run_domain::worker::FinishGuard;
    let (_home, mut store, run) = running(true);
    let accepted_events = |store: &Store<'_>| -> i64 {
        store
            .conn
            .query_row(
                "SELECT COUNT(*) FROM events WHERE agent_id=? AND kind='worker_finish_accepted_v1'",
                [run.as_str()],
                |row| row.get(0),
            )
            .unwrap()
    };
    for (guard, expected) in [
        (FinishGuard::Unavailable, "unsupported"),
        (FinishGuard::Secret, "validation"),
    ] {
        let refused = store
            .accept_worker_finish(&run, "attempt", TOKEN, &report(), 200.0, guard)
            .unwrap_err();
        match (expected, &refused) {
            ("unsupported", Error::Unsupported(_)) | ("validation", Error::Validation(_)) => {}
            _ => panic!("{guard:?} refused as {refused:?}"),
        }
        assert!(store.worker_finish_intent(&run, false).unwrap().is_none());
        assert_eq!(accepted_events(&store), 0);
    }
    let first = store
        .accept_worker_finish(&run, "attempt", TOKEN, &report(), 201.0, FinishGuard::Clean)
        .unwrap();
    assert!(!first.duplicate);
    for guard in [FinishGuard::Unavailable, FinishGuard::Secret] {
        let replay = store
            .accept_worker_finish(&run, "attempt", TOKEN, &report(), 202.0, guard)
            .unwrap();
        assert!(replay.duplicate);
        assert_eq!(replay.sha256, first.sha256);
        let changed = FinishRequest {
            summary: "other".into(),
            ..report()
        };
        assert!(matches!(
            store.accept_worker_finish(&run, "attempt", TOKEN, &changed, 203.0, guard),
            Err(Error::Conflict)
        ));
    }
    assert_eq!(accepted_events(&store), 1);
}

/// The broker's verdict travels beside the payload, never inside it: setting
/// it changes neither the envelope digest nor the serialized operation, so
/// replay identity and receipts are independent of the verdict.
#[test]
fn finish_guard_verdict_never_changes_operation_identity() {
    use agent_run_domain::{
        storage::{MonoDeadline, OperationId, ProducerId, ProducerKind},
        worker::FinishGuard,
    };
    use agent_run_store::ops::{AcceptWorkerFinish, LocalEnvelope, LocalOp};
    let op = AcceptWorkerFinish {
        execution: "ag-20261010-000000-0123456789".parse().unwrap(),
        attempt: "attempt".into(),
        token: TOKEN.into(),
        input: report(),
        at: 200.0,
        guard: FinishGuard::Unavailable,
    };
    let payload = serde_json::to_value(&op).unwrap();
    assert!(payload.get("guard").is_none());
    let (boot, now) = agent_run_platform::monotonic::now().unwrap();
    let mut envelope = LocalEnvelope::new(
        OperationId::new(&boot, now),
        ProducerId {
            kind: ProducerKind::Broker,
            scope: Some("workers".into()),
            instance: "instance-1".into(),
        },
        1,
        MonoDeadline {
            boot_id: boot,
            ns: now + 60_000_000_000,
        },
        LocalOp::from(op),
    )
    .unwrap();
    let digest = envelope.header().digest.clone();
    for guard in [FinishGuard::Clean, FinishGuard::Secret] {
        envelope.set_finish_guard(guard);
        assert_eq!(envelope.header().digest, digest);
        let LocalOp::AcceptWorkerFinish(op) = envelope.op() else {
            panic!("finish envelope changed kind");
        };
        assert_eq!(op.guard, guard);
        assert_eq!(serde_json::to_value(op).unwrap(), payload);
    }
}
