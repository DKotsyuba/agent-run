//! Durable transcript coverage for adapter-provided redaction.

mod common;

use agent_run_adapters::redact::Redactor;
use agent_run_adapters::{io::Process, LaunchPlan};
use std::collections::BTreeMap;

/// The stream-journal call persists only the adapter-redacted message text.
#[test]
fn journal_stores_redacted_message_text() {
    let h = common::Home::new();
    let (id, _) = h
        .store()
        .admit(&h.request(), &h.config, &serde_json::json!({}), None)
        .unwrap();
    let secret = "stored-secret";
    let redactor =
        Redactor::from_environment(&BTreeMap::from([("SERVICE_TOKEN".into(), secret.into())]));
    agent_run_core::journal(
        &h.store(),
        &id,
        "assistant",
        &redactor.redact(&format!("provider echoed {secret}")),
        None,
        None,
    )
    .unwrap();
    let transcript = h.store().transcript(&id, 0, 10).unwrap().to_string();
    assert!(!transcript.contains(secret));
    assert!(transcript.contains("<redacted>"));
}

/// Real Claude-family stream frames cannot persist or seal a launch secret
/// split across two partial text events.
#[tokio::test]
async fn split_claude_deltas_are_redacted_before_journaling() {
    let home = common::Home::new();
    let mut store = home.store();
    let (id, _) = store
        .admit(&home.request(), &home.config, &serde_json::json!({}), None)
        .unwrap();
    let frames = [
        serde_json::json!({"type":"system","session_id":"fixture-session"}),
        serde_json::json!({"type":"stream_event","event":{"type":"message_start","message":{"id":"message-1"}}}),
        serde_json::json!({"type":"stream_event","event":{"type":"content_block_delta","delta":{"type":"text_delta","text":"synthetic-"}}}),
        serde_json::json!({"type":"stream_event","event":{"type":"content_block_delta","delta":{"type":"text_delta","text":"secret"}}}),
        serde_json::json!({"type":"assistant","message":{"id":"message-1","content":[{"type":"text","text":"synthetic-secret"}]}}),
        serde_json::json!({"type":"result","subtype":"success","is_error":false,"result":"synthetic-secret","session_id":"fixture-session"}),
    ]
    .into_iter()
    .map(|frame| frame.to_string())
    .collect::<Vec<_>>()
    .join("\n");
    let output = home.path.join("engine-output.jsonl");
    std::fs::write(&output, format!("{frames}\n")).unwrap();
    let plan = LaunchPlan {
        binary: "/bin/cat".into(),
        args: vec![output.to_string_lossy().into_owned()],
        cwd: home.path.clone(),
        environment: BTreeMap::from([("SERVICE_TOKEN".into(), "synthetic-secret".into())]),
        initial_input: None,
    };
    let mut process = Process::spawn(&plan).unwrap();
    let record = store.get(&id).unwrap();
    let result = agent_run_core::stream::run(&mut process, &mut store, &record, None)
        .await
        .unwrap();
    let transcript = store.transcript(&id, 0, 20).unwrap().to_string();
    assert!(!transcript.contains("synthetic-secret"));
    assert!(!transcript.contains("synthetic-"));
    assert!(transcript.contains("<redacted>"));
    assert_eq!(result.answer.as_deref(), Some("<redacted>"));
}
