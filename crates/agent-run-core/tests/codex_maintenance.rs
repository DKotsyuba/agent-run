//! Deterministic native event correlation; never launches a model or a process.
use agent_run_core::codex::maintenance::{Assessment, Gap, Observer};
use agent_run_domain::worker::MaintenancePauseRequest;
use serde_json::{Value, json};

const THREAD: &str = "owned-thread";
const TURN: &str = "owned-turn";

/// One exact callback payload and its independently accepted store digest.
fn request() -> (MaintenancePauseRequest, String) {
    let request = MaintenancePauseRequest {
        challenge_id: "a".repeat(64),
        checkpoint: "The verified task context is retained.".into(),
    };
    let digest =
        agent_run_domain::canonical::sha256_hex(&serde_json::to_value(&request).unwrap(), true);
    (request, digest)
}

/// Bind the expected receipt as a future controller would after a store read.
fn observer() -> Observer {
    let (request, digest) = request();
    let mut observer = Observer::new(THREAD, false);
    observer.expect_checkpoint(&request.challenge_id, &digest);
    observer
}

/// The native start carries the exact private server, tool and arguments.
fn started() -> Value {
    json!({"method":"item/started","params":{"threadId":THREAD,"turnId":TURN,"item":{
        "id":"pause-item","type":"mcpToolCall","server":"agent_run_worker","tool":"maintenance_pause",
        "arguments":request().0
    }}})
}

/// A real successful native tool result contains one structured receipt block.
fn receipt() -> Value {
    json!({"method":"item/completed","params":{"threadId":THREAD,"turnId":TURN,"item":{
        "id":"pause-item","type":"mcpToolCall","server":"agent_run_worker","tool":"maintenance_pause","status":"completed",
        "result":{"content":[{"type":"text","text":json!({"sha256":request().1,"duplicate":false}).to_string()}]}
    }}})
}

/// The later owned successful turn boundary is separate from the receipt.
fn completed() -> Value {
    json!({"method":"turn/completed","params":{"threadId":THREAD,"turn":{"id":TURN,"status":"completed"}}})
}

/// Finish a sequence without masking an earlier sticky gap.
fn finish(observer: &mut Observer) {
    observer.observe(&receipt(), TURN);
    observer.observe(&completed(), TURN);
}

/// Receipt and native turn completion remain two distinct observations.
#[test]
fn codex_maintenance_exact_receipt_then_completed_turn_settles() {
    let mut observer = observer();
    assert_eq!(observer.assessment(), Assessment::AwaitingReceipt);
    observer.observe(&started(), TURN);
    observer.observe(&receipt(), TURN);
    assert_eq!(observer.assessment(), Assessment::AwaitingTurnCompletion);
    observer.observe(&completed(), TURN);
    assert_eq!(observer.assessment(), Assessment::Settled);
    let debug = format!("{observer:?}");
    for secret in [
        THREAD,
        TURN,
        &request().0.checkpoint,
        &request().0.challenge_id,
        &request().1,
    ] {
        assert!(!debug.contains(secret));
    }
}

/// Native result data alone never replaces the separately trusted store digest.
#[test]
fn codex_maintenance_requires_store_binding_and_exact_structured_receipt() {
    let mut observer = Observer::new(THREAD, false);
    observer.observe(&started(), TURN);
    finish(&mut observer);
    assert_eq!(observer.assessment(), Assessment::Unarmed);
    observer.expect_checkpoint(&request().0.challenge_id, &request().1);
    assert_eq!(observer.assessment(), Assessment::Settled);
    observer.expect_checkpoint(&request().0.challenge_id, &"b".repeat(64));
    assert_eq!(
        observer.assessment(),
        Assessment::Unavailable(Gap::ReceiptMismatch)
    );
    for text in [
        request().1,
        format!("Accepted digest: {}", request().1),
        json!({"sha256":request().1,"duplicate":false,"extra":true}).to_string(),
    ] {
        let mut observer = self::observer();
        observer.observe(&started(), TURN);
        let mut wrong = receipt();
        wrong["params"]["item"]["result"]["content"][0]["text"] = json!(text);
        observer.observe(&wrong, TURN);
        observer.observe(&completed(), TURN);
        assert!(matches!(observer.assessment(), Assessment::Unavailable(_)));
    }
}

/// Missing/foreign identities are refused even though ordinary transcript
/// processing tolerates some missing identifiers for historical fresh turns.
#[test]
fn codex_maintenance_rejects_missing_or_foreign_identity() {
    for field in ["threadId", "turnId"] {
        for wrong in [Value::Null, json!("foreign")] {
            let mut observer = observer();
            let mut event = started();
            event["params"][field] = wrong;
            observer.observe(&event, TURN);
            finish(&mut observer);
            assert!(matches!(observer.assessment(), Assessment::Unavailable(_)));
        }
    }
    for (field, wrong) in [
        ("id", json!("foreign")),
        ("server", json!("other")),
        ("tool", json!("other")),
        ("type", json!("dynamicToolCall")),
    ] {
        let mut observer = observer();
        observer.observe(&started(), TURN);
        let mut event = receipt();
        event["params"]["item"][field] = wrong;
        observer.observe(&event, TURN);
        observer.observe(&completed(), TURN);
        assert!(matches!(observer.assessment(), Assessment::Unavailable(_)));
    }
}

/// Missing starts, repeated events and an early turn boundary cannot certify a pause.
#[test]
fn codex_maintenance_rejects_missing_duplicate_or_reordered_events() {
    for events in [
        vec![receipt(), completed()],
        vec![started(), started(), receipt(), completed()],
        vec![started(), receipt(), receipt(), completed()],
        vec![started(), completed(), receipt()],
        vec![started(), receipt(), completed(), completed()],
    ] {
        let mut observer = observer();
        for event in events {
            observer.observe(&event, TURN);
        }
        assert!(matches!(observer.assessment(), Assessment::Unavailable(_)));
    }
    let mut observer = observer();
    observer.observe(&started(), TURN);
    observer.observe(&completed(), TURN);
    assert_eq!(observer.assessment(), Assessment::AwaitingReceipt);
}

/// Error flags, malformed receipts and failed/interrupted turns remain unavailable.
#[test]
fn codex_maintenance_rejects_native_errors_and_bad_digest() {
    for field in ["error", "isError", "status", "digest"] {
        let mut observer = observer();
        observer.observe(&started(), TURN);
        let mut event = receipt();
        match field {
            "error" => event["params"]["item"]["error"] = json!({"message":"failure"}),
            "isError" => event["params"]["item"]["result"]["isError"] = json!(true),
            "status" => event["params"]["item"]["status"] = json!("failed"),
            _ => {
                event["params"]["item"]["result"]["content"][0]["text"] =
                    json!(json!({"sha256":"b".repeat(64),"duplicate":false}).to_string())
            }
        }
        observer.observe(&event, TURN);
        observer.observe(&completed(), TURN);
        assert!(matches!(observer.assessment(), Assessment::Unavailable(_)));
    }
    for status in ["failed", "interrupted", "inProgress"] {
        let mut observer = observer();
        observer.observe(&started(), TURN);
        observer.observe(&receipt(), TURN);
        let mut event = completed();
        event["params"]["turn"]["status"] = json!(status);
        observer.observe(&event, TURN);
        assert_eq!(
            observer.assessment(),
            Assessment::Unavailable(Gap::NativeFailure)
        );
    }
}

/// An unfinished command/returned handle blocks settlement; missing native
/// exit evidence is sticky unknown, not a successful tool completion.
#[test]
fn codex_maintenance_does_not_settle_pending_or_uncertain_async_work() {
    let command = json!({"method":"item/started","params":{"threadId":THREAD,"turnId":TURN,"item":{"id":"job","type":"commandExecution","processId":"handle-1"}}});
    let mut observer = observer();
    observer.observe(&command, TURN);
    observer.observe(&started(), TURN);
    finish(&mut observer);
    assert_eq!(observer.assessment(), Assessment::PendingNativeWork);
    let unknown = json!({"method":"item/completed","params":{"threadId":THREAD,"turnId":TURN,"item":{"id":"job","type":"commandExecution","status":"completed","processId":"handle-1"}}});
    observer.observe(&unknown, TURN);
    assert_eq!(
        observer.assessment(),
        Assessment::Unavailable(Gap::AsyncCoverageUnavailable)
    );
    let mut observer = self::observer();
    observer.observe(&started(), TURN);
    observer.unavailable(Gap::AsyncCoverageUnavailable);
    finish(&mut observer);
    assert_eq!(
        observer.assessment(),
        Assessment::Unavailable(Gap::AsyncCoverageUnavailable)
    );
}

/// A known foreground command may complete before the private checkpoint;
/// unsupported tool families cannot be guessed synchronous from their return.
#[test]
fn codex_maintenance_requires_supported_complete_tool_lifecycle() {
    let mut observer = observer();
    observer.observe(&json!({"method":"item/started","params":{"threadId":THREAD,"turnId":TURN,"item":{"id":"job","type":"commandExecution"}}}),TURN);
    observer.observe(&json!({"method":"item/completed","params":{"threadId":THREAD,"turnId":TURN,"item":{"id":"job","type":"commandExecution","status":"completed","exitCode":0}}}),TURN);
    observer.observe(&started(), TURN);
    finish(&mut observer);
    assert_eq!(observer.assessment(), Assessment::Settled);
    for kind in [
        "dynamicToolCall",
        "collabAgentToolCall",
        "mcpToolCall",
        "unknownTool",
    ] {
        let mut observer = self::observer();
        observer.observe(&json!({"method":"item/started","params":{"threadId":THREAD,"turnId":TURN,"item":{"id":"other","type":kind,"server":"external","tool":"background"}}}),TURN);
        observer.observe(&started(), TURN);
        finish(&mut observer);
        assert_eq!(
            observer.assessment(),
            Assessment::Unavailable(Gap::UnsupportedToolCoverage)
        );
    }
}

/// Earlier sessions and new activity after a checkpoint need real reconciliation.
#[test]
fn codex_maintenance_keeps_inherited_or_new_work_unavailable() {
    let mut observer = Observer::new(THREAD, true);
    observer.expect_checkpoint(&request().0.challenge_id, &request().1);
    observer.observe(&started(), TURN);
    finish(&mut observer);
    assert_eq!(
        observer.assessment(),
        Assessment::Unavailable(Gap::InheritedWorkUnknown)
    );
    let mut observer = self::observer();
    observer.observe(&started(), TURN);
    finish(&mut observer);
    observer.observe(
        &json!({"method":"turn/started","params":{"threadId":THREAD,"turn":{"id":"later"}}}),
        "later",
    );
    assert_eq!(
        observer.assessment(),
        Assessment::Unavailable(Gap::ActivityAfterCheckpoint)
    );
}
