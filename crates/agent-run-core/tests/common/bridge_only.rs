//! Bridge-only outbox regression controls; no real native sender, credentials or model engine.
use super::*;

/// Multiple unconfigured holds do not consume the first real failure allowance or reset lease fencing.
#[tokio::test]
async fn unconfigured_holds_preserve_later_publication_retry_budget() {
    let home = common::Home::new();
    delivery_config(&home.path, 1.0, 1.0, 3);
    let original = std::fs::read_to_string(home.path.join("config.toml")).unwrap();
    delivery(
        &home.path,
        "ntf_budget_after_hold",
        "codex_queue",
        "pending",
    );
    for _ in 0..3 {
        assert_eq!(dispatch(&home.path).await.unwrap().retried, 1);
        make_due(&home.path, "ntf_budget_after_hold");
    }
    let db = fixture_connection(home.path.join("state.db")).unwrap();
    assert_eq!(
        db.query_row("SELECT attempts FROM deliveries", [], |r| r
            .get::<_, u32>(0))
            .unwrap(),
        3
    );
    let offline = bridge_fixture::start(&home.path, false);
    drop(offline);
    tokio::task::yield_now().await;
    let failure = dispatch(&home.path).await.unwrap();
    assert_eq!(
        (failure.retried, failure.failed, failure.transferred),
        (1, 0, 0)
    );
    assert_eq!(
        db.query_row("SELECT attempts FROM deliveries", [], |r| r
            .get::<_, u32>(0))
            .unwrap(),
        4
    );
    let frozen: String = db
        .query_row("SELECT intent_json FROM bridge_delivery_intents", [], |r| {
            r.get(0)
        })
        .unwrap();
    // Restart only the disposable fixture endpoint, retaining exact service/key paths and publication.
    std::fs::remove_file(home.path.join("bridge-fixture.sock")).unwrap();
    std::fs::write(home.path.join("config.toml"), original).unwrap();
    let bridge = bridge_fixture::start(&home.path, false);
    make_due(&home.path, "ntf_budget_after_hold");
    assert_eq!(dispatch(&home.path).await.unwrap().transferred, 1);
    assert_eq!(
        db.query_row("SELECT attempts FROM deliveries", [], |r| r
            .get::<_, u32>(0))
            .unwrap(),
        5
    );
    assert_eq!(
        db.query_row("SELECT intent_json FROM bridge_delivery_intents", [], |r| r
            .get::<_, String>(0))
            .unwrap(),
        frozen
    );
    assert_eq!(bridge.published.lock().unwrap().len(), 1);
}

/// An unconfigured new notice has one Bridge owner, stays durable and never spins or exhausts.
#[tokio::test]
async fn unconfigured_notices_are_held_without_legacy_effect_or_attempt_exhaustion() {
    for transport in ["codex_queue", "claude_uds"] {
        let home = common::Home::new();
        delivery_config(&home.path, 1.0, 1.0, 1);
        delivery(&home.path, "ntf_held", transport, "pending");
        let before = now();
        let result = dispatch_with_batch(&home.path, 10).await.unwrap();
        assert_eq!(
            (
                result.claimed,
                result.retried,
                result.delivered,
                result.transferred
            ),
            (1, 1, 0, 0)
        );
        let db = fixture_connection(home.path.join("state.db")).unwrap();
        let row: (String, String, f64, u32) = db
            .query_row(
                "SELECT state,last_error,next_attempt_at,attempts FROM deliveries",
                [],
                |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?)),
            )
            .unwrap();
        assert_eq!(
            (row.0.as_str(), row.1.as_str(), row.3),
            ("retry_wait", "bridge_unconfigured", 1)
        );
        assert!(row.2 >= before + 60.0);
        assert_eq!(
            db.query_row(
                "SELECT COUNT(*) FROM bridge_delivery_intents WHERE intent_json IS NULL",
                [],
                |r| r.get::<_, i64>(0)
            )
            .unwrap(),
            1
        );
        assert_eq!(
            db.query_row("SELECT status FROM agents", [], |r| r.get::<_, String>(0))
                .unwrap(),
            "succeeded"
        );
        assert_eq!(dispatch(&home.path).await.unwrap().claimed, 0);
        // Enabling the sole route later publishes the same previously held outbox row.
        let bridge = bridge_fixture::start(&home.path, false);
        make_due(&home.path, "ntf_held");
        assert_eq!(dispatch(&home.path).await.unwrap().transferred, 1);
        assert_eq!(bridge.published.lock().unwrap().len(), 1);
    }
}

/// Historical accepted, ambiguous, known-unsent and missing-evidence attempts never replay or re-home.
#[tokio::test]
async fn historical_native_attempts_are_retained_without_publication() {
    for transport in ["codex_queue", "claude_uds"] {
        for (state, classifier) in [
            ("delivered", "relay_accepted"),
            ("retry_wait", "relay_ambiguous"),
            ("retry_wait", "uds_unavailable"),
            ("sending", "uds_unconfirmed"),
            ("retry_wait", ""),
        ] {
            let home = common::Home::new();
            let bridge = bridge_fixture::start(&home.path, false);
            delivery(&home.path, "ntf_history", transport, state);
            let db = fixture_connection(home.path.join("state.db")).unwrap();
            db.execute(
                "UPDATE deliveries SET attempts=1,lease_until=0,remote_message_id='historical-id'",
                [],
            )
            .unwrap();
            let historical = json!({"classifier":classifier,"historical":"immutable"}).to_string();
            if !classifier.is_empty() {
                db.execute(
                    "INSERT INTO delivery_attempt_evidence VALUES('ntf_history',1,1,?)",
                    [&historical],
                )
                .unwrap();
            }
            let result = dispatch(&home.path).await.unwrap();
            assert_eq!(
                (result.claimed, result.transferred, result.delivered),
                (0, 0, 0)
            );
            assert!(bridge.published.lock().unwrap().is_empty());
            assert_eq!(
                db.query_row("SELECT COUNT(*) FROM bridge_delivery_intents", [], |r| r
                    .get::<_, i64>(0))
                    .unwrap(),
                0
            );
            let actual: (String, u32, String) = db
                .query_row(
                    "SELECT state,attempts,remote_message_id FROM deliveries",
                    [],
                    |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
                )
                .unwrap();
            assert_eq!((actual.1, actual.2.as_str()), (1, "historical-id"));
            assert_eq!(
                actual.0,
                if state == "delivered" {
                    "delivered"
                } else {
                    "failed"
                }
            );
            if state != "delivered" {
                assert_eq!(
                    db.query_row("SELECT last_error FROM deliveries", [], |r| r
                        .get::<_, String>(0))
                        .unwrap(),
                    "legacy_delivery_held"
                );
            }
            if !classifier.is_empty() {
                assert_eq!(
                    db.query_row(
                        "SELECT evidence_json FROM delivery_attempt_evidence",
                        [],
                        |r| r.get::<_, String>(0)
                    )
                    .unwrap(),
                    historical
                );
            }
        }
    }
}

/// All four notice forms publish through the same SDK route; none creates native acceptance evidence.
#[tokio::test]
async fn all_four_notice_forms_use_only_bridge() {
    let home = common::Home::new();
    let bridge = bridge_fixture::start(&home.path, false);
    delivery(&home.path, "ntf_completion", "codex_queue", "pending");
    let db = fixture_connection(home.path.join("state.db")).unwrap();
    let agent = "ag-20260825-120000-0123456789";
    db.execute("INSERT INTO attempts(id,agent_id,number,state,adapter_state_json,created_at) VALUES('report-attempt',?,1,'finished','{}',?)", params![agent,now()]).unwrap();
    db.execute("INSERT INTO deliveries(id,agent_id,orchestrator_session_id,state,next_attempt_at) VALUES('ntf_report',?,'sess','pending',0)", [agent]).unwrap();
    db.execute("INSERT INTO worker_notifications(delivery_id,agent_id,attempt_id,request_id,kind,message,created_at) VALUES('ntf_report',?,'report-attempt','report','risk','untrusted report',?)", params![agent,now()]).unwrap();
    let pool = "pool-20261013-000000-0123456789";
    for (id, kind) in [
        ("ntf_pool", "pool_completed"),
        ("ntf_attention", "pool_join_needs_action"),
    ] {
        db.execute(
            "INSERT INTO events(agent_id,at,kind,data_json) VALUES(?,?,?,?)",
            params![
                agent,
                now(),
                kind,
                json!({"pool_id":pool,"notice":"pool control"}).to_string()
            ],
        )
        .unwrap();
        let event = db.last_insert_rowid();
        db.execute("INSERT INTO deliveries(id,agent_id,orchestrator_session_id,terminal_event_seq,state,next_attempt_at) VALUES(?,?,'sess',?,'pending',0)", params![id,agent,event]).unwrap();
        if id == "ntf_pool" {
            db.execute("INSERT INTO pools(id,request_namespace,request_id,request_sha256,orchestrator_session_id,goal,acceptance_json,state,roster_revision,completed_at,completion_delivery_id,created_at) VALUES(?,'fixture','four',?,'sess','fixture','[]','completed',1,?,'ntf_pool',?)", params![pool,"a".repeat(64),now(),now()]).unwrap();
        }
    }
    let result = dispatch_with_batch(&home.path, 4).await.unwrap();
    assert_eq!(
        (result.claimed, result.transferred, result.delivered),
        (4, 4, 0)
    );
    let mut kinds: Vec<_> = bridge
        .published
        .lock()
        .unwrap()
        .iter()
        .map(|p| p.kind.clone())
        .collect();
    kinds.sort();
    assert_eq!(
        kinds,
        [
            "agent-run.completion",
            "agent-run.pool-attention",
            "agent-run.pool-completion",
            "agent-run.worker-report"
        ]
    );
    assert_eq!(
        db.query_row(
            "SELECT COUNT(*) FROM bridge_delivery_intents WHERE custody_state='custody_accepted'",
            [],
            |r| r.get::<_, i64>(0)
        )
        .unwrap(),
        4
    );
    assert_eq!(dispatch(&home.path).await.unwrap().claimed, 0);
}

/// Cancellation/expired ownership wins over late custody; captured policy survives configuration damage.
#[tokio::test]
async fn late_ack_respects_cancellation_lease_and_captured_policy() {
    for action in ["cancel", "expire", "remove_config", "corrupt_config"] {
        let home = common::Home::new();
        let bridge = bridge_fixture::start(&home.path, true);
        delivery(&home.path, "ntf_race", "codex_queue", "pending");
        let path = home.path.clone();
        let sending = tokio::spawn(async move { dispatch_with_batch(&path, 1).await.unwrap() });
        tokio::time::timeout(Duration::from_secs(3), bridge.started.notified())
            .await
            .unwrap();
        let db = fixture_connection(home.path.join("state.db")).unwrap();
        match action {
            "cancel" => {
                assert!(home.store().cancel_delivery("ntf_race").unwrap());
            }
            "expire" => {
                db.execute("UPDATE deliveries SET lease_until=?", [now() - 1.0])
                    .unwrap();
            }
            "remove_config" => {
                std::fs::remove_file(home.path.join("config.toml")).unwrap();
            }
            "corrupt_config" => {
                std::fs::write(home.path.join("config.toml"), "invalid = [").unwrap();
            }
            _ => unreachable!(),
        }
        bridge.release.notify_one();
        let result = sending.await.unwrap();
        if matches!(action, "cancel" | "expire") {
            assert_eq!((result.transferred, result.delivered), (0, 0));
            assert_eq!(db.query_row("SELECT COUNT(*) FROM bridge_delivery_intents WHERE custody_state='custody_accepted'", [], |r| r.get::<_,i64>(0)).unwrap(), 0);
            let state: String = db
                .query_row("SELECT state FROM deliveries", [], |r| r.get(0))
                .unwrap();
            assert_eq!(
                state,
                if action == "cancel" {
                    "cancelled"
                } else {
                    "sending"
                }
            );
        } else {
            assert_eq!((result.transferred, result.delivered), (1, 0));
        }
        assert_eq!(bridge.published.lock().unwrap().len(), 1);
    }
}

/// The bounded drain leaves overflow due, and contention cannot claim a second notice.
#[tokio::test]
async fn bounded_drain_and_nonblocking_dispatcher_keep_one_owner() {
    let home = common::Home::new();
    let bridge = bridge_fixture::start(&home.path, true);
    delivery(&home.path, "ntf_batch_1", "codex_queue", "pending");
    let db = fixture_connection(home.path.join("state.db")).unwrap();
    for id in ["ntf_batch_2", "ntf_batch_3"] {
        db.execute("INSERT INTO deliveries(id,agent_id,orchestrator_session_id,state,next_attempt_at) SELECT ?,agent_id,orchestrator_session_id,'pending',0 FROM deliveries WHERE id='ntf_batch_1'", [id]).unwrap();
    }
    let path = home.path.clone();
    let sending = tokio::spawn(async move { dispatch_with_batch(&path, 2).await.unwrap() });
    tokio::time::timeout(Duration::from_secs(3), bridge.started.notified())
        .await
        .unwrap();
    assert!(dispatch_with_batch(&home.path, 1).await.unwrap().locked_out);
    bridge.release.notify_one();
    tokio::time::timeout(Duration::from_secs(3), bridge.started.notified())
        .await
        .unwrap();
    bridge.release.notify_one();
    let first = sending.await.unwrap();
    assert_eq!((first.claimed, first.transferred), (2, 2));
    assert_eq!(
        db.query_row(
            "SELECT COUNT(*) FROM deliveries WHERE attempts=0 AND state='pending'",
            [],
            |r| r.get::<_, i64>(0)
        )
        .unwrap(),
        1
    );
    let path = home.path.clone();
    let sending = tokio::spawn(async move { dispatch(&path).await.unwrap() });
    tokio::time::timeout(Duration::from_secs(3), bridge.started.notified())
        .await
        .unwrap();
    bridge.release.notify_one();
    assert_eq!(sending.await.unwrap().transferred, 1);
}

/// A known-unsent unavailable service uses the configured attempt budget and preserves terminal outcome.
#[tokio::test]
async fn unavailable_bridge_exhausts_without_native_fallback() {
    let home = common::Home::new();
    delivery_config(&home.path, 1.0, 1.0, 2);
    let bridge = bridge_fixture::start(&home.path, false);
    drop(bridge);
    tokio::task::yield_now().await;
    delivery(&home.path, "ntf_budget", "codex_queue", "pending");
    assert_eq!(dispatch_once(&home.path).await.unwrap(), 1);
    make_due(&home.path, "ntf_budget");
    assert_eq!(dispatch_once(&home.path).await.unwrap(), 1);
    let db = fixture_connection(home.path.join("state.db")).unwrap();
    let state: (String, u32, Option<f64>) = db
        .query_row(
            "SELECT state,attempts,next_attempt_at FROM deliveries",
            [],
            |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
        )
        .unwrap();
    assert_eq!(state, ("failed".into(), 2, None));
    assert_eq!(
        db.query_row("SELECT status FROM agents", [], |r| r.get::<_, String>(0))
            .unwrap(),
        "succeeded"
    );
    assert_eq!(dispatch(&home.path).await.unwrap().claimed, 0);
}
