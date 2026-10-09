//! Lossless bounded control exchanges: pressure stops before reading.

use agent_run_adapters::{
    LaunchPlan,
    io::{Event, Process, RPC_BACKLOG_LIMIT, RpcDisposition, RpcUncertain},
};
use std::{collections::BTreeMap, path::PathBuf, time::Duration};

/// Terminates the captured fixture even if an assertion interrupts draining.
struct Fixture(Process);
impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = self.0.owner.cleanup_blocking(Duration::from_millis(100));
    }
}

/// Spawns a shell engine double whose `script` reads request lines forever.
fn engine(script: &str) -> LaunchPlan {
    LaunchPlan {
        binary: PathBuf::from("/bin/sh"),
        args: vec![
            "-c".into(),
            format!("while IFS= read -r line; do {script}; done"),
        ],
        cwd: std::env::current_dir().unwrap(),
        environment: BTreeMap::new(),
        initial_input: None,
    }
}

/// Reads the next JSON event within a bounded wait.
async fn next(fixture: &mut Fixture) -> Event {
    tokio::time::timeout(Duration::from_secs(2), fixture.0.next())
        .await
        .expect("bounded event")
}

/// One notification statement, numbered for order assertions.
fn notification(n: u64) -> String {
    format!("printf '{{\"method\":\"item/x\",\"params\":{{\"n\":{n}}}}}\\n'")
}

/// Joins statements into one shell body without a trailing separator.
fn body(statements: impl IntoIterator<Item = String>) -> String {
    let joined: Vec<_> = statements.into_iter().collect();
    joined.join("; ")
}

/// An observer failure has a typed first cause during initialization and stream
/// draining. A control already sent remains uncertain, while its exact cause is
/// retained for the next runner read; only this fixture's owned child is cleaned.
#[tokio::test]
async fn ownership_failure_is_typed_and_control_acceptance_stays_uncertain() {
    use agent_run_domain::{Error, OwnershipStage};
    let mut fixture = Fixture(Process::spawn(&engine("printf '{}\\n'")).unwrap());
    let initial = fixture
        .0
        .observe_ownership(|_| {
            Err(Error::Io(std::io::Error::new(
                std::io::ErrorKind::PermissionDenied,
                "SOURCE_SECRET",
            )))
        })
        .unwrap_err();
    assert!(matches!(
        initial,
        Error::OwnershipCheckpoint {
            stage: OwnershipStage::Initial,
            ..
        }
    ));
    assert!(!initial.public().message.contains("SOURCE_SECRET"));
    let result = fixture
        .0
        .rpc_exchange("turn/steer", serde_json::json!({}), Duration::from_secs(1))
        .await
        .unwrap();
    assert_eq!(
        result,
        RpcDisposition::Uncertain(RpcUncertain::Transport(
            "engine_ownership_checkpoint_failed"
        ))
    );
    match next(&mut fixture).await {
        Event::OwnershipFailure(Error::OwnershipCheckpoint {
            stage: OwnershipStage::Streaming,
            source,
        }) => {
            assert!(
                matches!(*source, Error::Io(ref io) if io.kind() == std::io::ErrorKind::PermissionDenied)
            );
        }
        _ => panic!("the typed checkpoint cause was lost"),
    }
    let final_error = fixture
        .0
        .checkpoint_ownership_at(OwnershipStage::Final)
        .unwrap_err();
    assert!(matches!(
        final_error,
        Error::OwnershipCheckpoint {
            stage: OwnershipStage::Final,
            ..
        }
    ));
}

/// Pressure after a possible write stops before reading: the backlog keeps
/// its fixed capacity, the next engine event stays in the channel, and every
/// notification eventually reaches the consumer in channel-then-backlog
/// order with none dropped.
#[tokio::test]
async fn pressure_retains_every_notification_and_stops_before_reading() {
    let flood = body((1..=(RPC_BACKLOG_LIMIT as u64 + 1)).map(notification));
    let mut fixture = Fixture(Process::spawn(&engine(&flood)).unwrap());
    let outcome = fixture
        .0
        .rpc_exchange("turn/steer", serde_json::json!({}), Duration::from_secs(5))
        .await
        .unwrap();
    assert_eq!(
        outcome,
        RpcDisposition::Uncertain(RpcUncertain::Pressure),
        "a full backlog after the write is uncertain, never an error"
    );
    // Every notification survives: the retained backlog drains first, then
    // the event that stayed unread in the channel when pressure stopped the
    // exchange — the exact event the previous implementation dropped.
    let mut seen = Vec::new();
    for _ in 0..=(RPC_BACKLOG_LIMIT as u64) {
        match next(&mut fixture).await {
            Event::Json(value) => seen.push(value["params"]["n"].as_u64().unwrap()),
            other => panic!("an engine event was lost: {other:?}"),
        }
    }
    assert_eq!(
        seen,
        (1..=(RPC_BACKLOG_LIMIT as u64 + 1)).collect::<Vec<_>>(),
        "order is retained and nothing is dropped"
    );
}

/// A full backlog before the send proves the request unsent: the engine
/// observes no second request, and the not-yet-read events survive intact.
#[tokio::test]
async fn full_backlog_proves_the_next_request_unsent() {
    let temporary = tempfile::tempdir().unwrap();
    let log = temporary.path().join("requests.log");
    let flood = body((1..=(RPC_BACKLOG_LIMIT as u64 + 1)).map(notification));
    let script = format!("printf '%s\\n' \"$line\" >> \"$FIXTURE_LOG\"; {flood}");
    let mut plan = engine(&script);
    plan.environment
        .insert("FIXTURE_LOG".into(), log.to_string_lossy().into_owned());
    let mut fixture = Fixture(Process::spawn(&plan).unwrap());
    let first = fixture
        .0
        .rpc_exchange("turn/steer", serde_json::json!({}), Duration::from_secs(5))
        .await
        .unwrap();
    assert_eq!(first, RpcDisposition::Uncertain(RpcUncertain::Pressure));
    let second = fixture
        .0
        .rpc_exchange("turn/steer", serde_json::json!({}), Duration::from_secs(1))
        .await
        .unwrap();
    assert_eq!(
        second,
        RpcDisposition::UnsentPressure,
        "a full backlog must refuse before writing"
    );
    // Only the first request reached the engine; the second stayed unsent.
    let recorded = std::fs::read_to_string(&log).unwrap();
    assert_eq!(
        recorded.lines().count(),
        1,
        "the unsent request must not appear in the engine log: {recorded}"
    );
    // The retained events are still intact and complete for the drain.
    let mut seen = Vec::new();
    for _ in 0..=(RPC_BACKLOG_LIMIT as u64) {
        match next(&mut fixture).await {
            Event::Json(value) => seen.push(value["params"]["n"].as_u64().unwrap()),
            other => panic!("retained event lost after unsent refusal: {other:?}"),
        }
    }
    assert_eq!(
        seen,
        (1..=(RPC_BACKLOG_LIMIT as u64 + 1)).collect::<Vec<_>>()
    );
}

/// Correlated replies stay proven in both directions and carry their result.
#[tokio::test]
async fn correlated_replies_are_rejected_or_replied() {
    let script = "case \"$line\" in *\"turn/steer\"*) printf '{\"id\":%s,\"error\":{\"code\":\"noTurn\"}}\\n' \"$(printf %s \"$line\" | sed 's/.*\"id\":\\([0-9]*\\).*/\\1/')\" ;; *) printf '{\"id\":%s,\"result\":{\"ok\":true}}\\n' \"$(printf %s \"$line\" | sed 's/.*\"id\":\\([0-9]*\\).*/\\1/')\" ;; esac";
    let mut fixture = Fixture(Process::spawn(&engine(script)).unwrap());
    let outcome = fixture
        .0
        .rpc_exchange("turn/steer", serde_json::json!({}), Duration::from_secs(2))
        .await
        .unwrap();
    assert_eq!(
        outcome,
        RpcDisposition::Rejected {
            code: "\"noTurn\"".into()
        }
    );
    let outcome = fixture
        .0
        .rpc_exchange("other", serde_json::json!({}), Duration::from_secs(2))
        .await
        .unwrap();
    assert_eq!(
        outcome,
        RpcDisposition::Replied(serde_json::json!({"ok":true}))
    );
}

/// The intentional one-second control cap stays: a stalled engine ends the
/// exchange uncertain within the cap, not the caller's larger timeout.
#[tokio::test]
async fn control_exchange_keeps_the_one_second_cap() {
    let mut fixture = Fixture(Process::spawn(&engine(":")).unwrap());
    let started = std::time::Instant::now();
    let outcome = fixture
        .0
        .rpc_exchange("turn/steer", serde_json::json!({}), Duration::from_secs(30))
        .await
        .unwrap();
    assert_eq!(outcome, RpcDisposition::Uncertain(RpcUncertain::Timeout));
    let elapsed = started.elapsed();
    assert!(
        elapsed < Duration::from_millis(1500),
        "the control cap bounds the wait: {elapsed:?}"
    );
}

/// A closed transport after the send is uncertain, not a guessed rejection.
#[tokio::test]
async fn closed_transport_after_send_is_uncertain() {
    let mut fixture = Fixture(Process::spawn(&engine("exit 0")).unwrap());
    let outcome = fixture
        .0
        .rpc_exchange("turn/steer", serde_json::json!({}), Duration::from_secs(2))
        .await
        .unwrap();
    assert_eq!(outcome, RpcDisposition::Uncertain(RpcUncertain::Closed));
}

/// Only valid correlated results/rejection codes prove an outcome. Missing,
/// conflicting, mistyped or oversized error codes remain explicitly uncertain.
/// Every table engine is closed and reaped before checking its disposition.
#[tokio::test]
async fn correlated_envelope_shape_preserves_truthful_dispositions() {
    use serde_json::json;
    let malformed = RpcDisposition::Uncertain(RpcUncertain::MalformedReply);
    let cases = [
        (json!({"id":1}), malformed.clone()),
        (json!({"id":1,"error":null}), malformed.clone()),
        (json!({"id":1,"error":{}}), malformed.clone()),
        (json!({"id":1,"error":{"code":null}}), malformed.clone()),
        (
            json!({"id":1,"error":{"code":{"payload":"private"}}}),
            malformed.clone(),
        ),
        (json!({"id":1,"error":{"code":[]}}), malformed.clone()),
        (json!({"id":1,"error":{"code":true}}), malformed.clone()),
        (json!({"id":1,"error":{"code":1.5}}), malformed.clone()),
        (json!({"id":1,"error":{"code":" "}}), malformed.clone()),
        (
            json!({"id":1,"error":{"code":"bad\ncode"}}),
            malformed.clone(),
        ),
        (
            json!({"id":1,"error":{"code":"x".repeat(65)}}),
            malformed.clone(),
        ),
        (
            json!({"id":1,"result":{},"error":{"code":-32601}}),
            malformed,
        ),
        (
            json!({"id":1,"result":null}),
            RpcDisposition::Replied(json!(null)),
        ),
        (
            json!({"id":1,"result":{"ok":true}}),
            RpcDisposition::Replied(json!({"ok":true})),
        ),
        (
            json!({"id":1,"error":{"code":-32601}}),
            RpcDisposition::Rejected {
                code: "-32601".into(),
            },
        ),
        (
            json!({"id":1,"error":{"code":"noTurn"}}),
            RpcDisposition::Rejected {
                code: "\"noTurn\"".into(),
            },
        ),
    ];
    for (reply, expected) in cases {
        let script = format!("printf '%s\\n' '{reply}'");
        let mut fixture = Fixture(Process::spawn(&engine(&script)).unwrap());
        let observed = fixture
            .0
            .rpc_exchange("turn/steer", json!({}), Duration::from_secs(1))
            .await;
        drop(fixture.0.input.take());
        assert_eq!(fixture.0.reap().await, Some(0), "fixture reaped");
        assert_eq!(observed.unwrap(), expected, "{reply}");
    }
}

/// A late unsolicited approval request forces a large denial write into an
/// unread pipe. Denial shares the original one-second control deadline; a
/// fresh one-second budget would exceed the bound after the initial delay.
/// The owned child and captured descendants are cleaned up and reaped first.
#[tokio::test]
async fn unsolicited_denial_backpressure_keeps_original_control_deadline() {
    let script = r#"sleep 0.75; printf '{"id":"'; head -c 1048576 /dev/zero | tr '\000' x; printf '","method":"item/permissions/requestApproval"}\n'; sleep 30"#;
    let mut fixture = Fixture(Process::spawn(&engine(script)).unwrap());
    let started = std::time::Instant::now();
    let observed = fixture
        .0
        .rpc_exchange("turn/steer", serde_json::json!({}), Duration::from_secs(30))
        .await;
    let elapsed = started.elapsed();
    fixture
        .0
        .owner
        .cleanup(Duration::from_millis(100))
        .await
        .unwrap();
    fixture.0.reap().await;
    // A signal exit has no numeric exit code; verify the child was waited instead.
    assert!(
        fixture.0.child.try_wait().unwrap().is_some(),
        "fixture reaped"
    );
    assert_eq!(
        observed.unwrap(),
        RpcDisposition::Uncertain(RpcUncertain::Timeout)
    );
    assert!(
        elapsed < Duration::from_millis(1500),
        "denial renewed the control budget: {elapsed:?}"
    );
}
