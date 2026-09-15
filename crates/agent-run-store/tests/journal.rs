mod common;

use serde_json::json;

/// Mirrors Python `test_state_store.py::test_attempts_and_messages_require_ownership`.
#[test]
fn python_test_state_store_attempts_messages_and_spool() {
    let home = common::Home::new();
    let mut store = home.store();
    let (id, _) = store
        .admit(&home.request(), &home.config, &json!({}), None)
        .unwrap();
    let first = store.create_attempt(&id, "running", &json!({})).unwrap();
    let second = store
        .create_attempt(&id, "retrying", &json!({"retry": true}))
        .unwrap();
    assert_ne!(first, second);
    let event = store
        .append_event(&id, "runtime_result", &json!({"usage": {}}), Some(&first))
        .unwrap();
    assert!(event > 0);
    let large = "z".repeat(32 * 1024 + 1);
    let seq = store
        .append_message(&id, "assistant", &large, None, None, Some(&second))
        .unwrap();
    let (content, raw_ref): (String, Option<String>) = store
        .conn
        .query_row(
            "SELECT content,raw_ref FROM messages WHERE seq=?",
            [seq],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .unwrap();
    let raw_ref = raw_ref.expect("oversized body is spooled");
    assert!(content.contains("exceed the 32 KiB inline limit"));
    assert_eq!(
        std::fs::read(home.path.join("agents").join(id.as_str()).join(raw_ref)).unwrap(),
        large.as_bytes()
    );
}

/// Mirrors Python `test_state_store.py::test_claim_command_prioritizes_cancel`.
#[test]
fn python_test_state_store_commands_claim_cancel_before_steer() {
    let home = common::Home::new();
    let mut store = home.store();
    let (id, _) = store
        .admit(&home.request(), &home.config, &json!({}), None)
        .unwrap();
    store
        .enqueue(&id, "steer", &json!({"text":"continue"}))
        .unwrap();
    store.enqueue(&id, "cancel", &json!({})).unwrap();
    let (command, kind, _) = store.claim_command(&id).unwrap().unwrap();
    assert_eq!(kind, "cancel");
    store
        .complete_command(&id, command, &json!({"accepted":true}))
        .unwrap();
}
