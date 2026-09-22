//! Claude-family assistant message identity in the durable transcript.
//!
//! These tests drive the real stream runner against a file-backed fake engine
//! so journal identity, dedup, and ordering are proven without a provider.

mod common;

use agent_run_adapters::{io::Process, LaunchPlan};
use agent_run_core::{domain::Status, stream};
use serde_json::{json, Value};
use std::{collections::BTreeMap, path::Path};

/// Builds a one-shot engine double that replays `lines` and then exits.
///
/// The lines are written to a file and replayed with `cat`, so arbitrary JSON
/// payloads need no shell quoting and the child closes its stdout immediately
/// after the record, which is the stream shape a one-shot launch produces.
fn fake_engine(directory: &Path, lines: &[Value]) -> LaunchPlan {
    let script = directory.join("engine-stream.jsonl");
    let body = lines
        .iter()
        .map(|line| line.to_string())
        .collect::<Vec<_>>()
        .join("\n");
    std::fs::write(&script, format!("{body}\n")).expect("fake engine stream");
    LaunchPlan {
        binary: "/bin/cat".into(),
        args: vec![script.display().to_string()],
        cwd: directory.to_path_buf(),
        environment: BTreeMap::new(),
        initial_input: None,
    }
}

/// One journaled transcript row reduced to its observable identity triple.
type Row = (String, String, Option<String>);

/// Runs one fake Claude stream to completion and returns its assistant rows.
///
/// Returns `(rows, answer)` where `rows` are `(role, content, raw_ref)` in
/// journal order and `answer` is the terminal result text.
async fn run_stream(lines: &[Value]) -> (Vec<Row>, Option<String>) {
    let home = common::Home::new();
    let (id, _) = home
        .store()
        .admit(&home.request(), &home.config, &json!({}), None)
        .unwrap();
    let mut store = home.store();
    let record = store.get(&id).unwrap();
    let mut process = Process::spawn(&fake_engine(&home.path, lines)).expect("fake engine");
    let result = stream::run(&mut process, &mut store, &record, None)
        .await
        .expect("stream completes");
    assert_eq!(result.outcome.status, Status::Succeeded);
    let page = store.transcript(&id, 0, 1000).unwrap();
    let rows = page["messages"]
        .as_array()
        .unwrap()
        .iter()
        .map(|message| {
            (
                message["role"].as_str().unwrap_or("").to_owned(),
                message["content"].as_str().unwrap_or("").to_owned(),
                message["raw_ref"].as_str().map(str::to_owned),
            )
        })
        .collect();
    (rows, result.answer)
}

/// Wraps one native stream event.
fn stream_event(event: Value) -> Value {
    json!({"type": "stream_event", "event": event})
}

/// Proves two native messages stay distinct while each message's fragments and
/// completion tail share one identity, with whitespace preserved and no
/// duplicated full text.
#[tokio::test]
async fn partial_fragments_and_tails_share_identity_per_message() {
    let (rows, answer) = run_stream(&[
        stream_event(json!({"type":"message_start","message":{"id":"msg_one","role":"assistant"}})),
        stream_event(json!({"type":"content_block_delta","delta":{"type":"text_delta","text":"first "}})),
        stream_event(json!({"type":"content_block_delta","delta":{"type":"text_delta","text":"message"}})),
        json!({"type":"assistant","message":{"id":"msg_one","role":"assistant","content":[{"type":"text","text":"first message"}]}}),
        stream_event(json!({"type":"message_start","message":{"id":"msg_two","role":"assistant"}})),
        stream_event(json!({"type":"content_block_delta","delta":{"type":"text_delta","text":"second"}})),
        json!({"type":"assistant","message":{"id":"msg_two","role":"assistant","content":[{"type":"text","text":"second message"}]}}),
        json!({"type":"result","subtype":"success","is_error":false,"result":"second message","session_id":"sess-1","usage":{}}),
    ])
    .await;
    assert_eq!(
        rows,
        [
            ("assistant".into(), "first ".into(), Some("msg_one".into())),
            ("assistant".into(), "message".into(), Some("msg_one".into())),
            // The full message's tail is its text minus the streamed prefix.
            ("assistant".into(), "second".into(), Some("msg_two".into())),
            (
                "assistant".into(),
                " message".into(),
                Some("msg_two".into())
            ),
        ]
    );
    assert_eq!(answer.as_deref(), Some("second message"));
}

/// Proves a boundary without a native id gets one producer-owned fallback that
/// is shared by that message's fragments and tail but distinct per message.
#[tokio::test]
async fn message_boundaries_without_native_ids_stay_distinct() {
    let (rows, _) = run_stream(&[
        stream_event(json!({"type":"message_start","message":{"role":"assistant"}})),
        stream_event(json!({"type":"content_block_delta","delta":{"type":"text_delta","text":"first "}})),
        stream_event(json!({"type":"content_block_delta","delta":{"type":"text_delta","text":"message"}})),
        json!({"type":"assistant","message":{"role":"assistant","content":[{"type":"text","text":"first message"}]}}),
        stream_event(json!({"type":"message_start","message":{"role":"assistant"}})),
        stream_event(json!({"type":"content_block_delta","delta":{"type":"text_delta","text":"second message"}})),
        json!({"type":"assistant","message":{"role":"assistant","content":[{"type":"text","text":"second message"}]}}),
        json!({"type":"result","subtype":"success","is_error":false,"result":"done","usage":{}}),
    ])
    .await;
    let first = &rows[0].2;
    let tail = &rows[1].2;
    let second = &rows[2].2;
    assert!(first.is_some(), "a boundary mints a fallback identity");
    assert_eq!(first, tail, "one message's fragments and tail join");
    assert_ne!(first, second, "distinct messages never merge");
}

/// Proves a legacy whole-event stream keeps one row per full message under the
/// native message id, with no identity changes or duplicated text.
#[tokio::test]
async fn whole_event_stream_journals_each_message_once_under_native_id() {
    let (rows, _) = run_stream(&[
        json!({"type":"assistant","message":{"id":"msg_one","role":"assistant","content":[{"type":"text","text":"first message"}]}}),
        json!({"type":"assistant","message":{"id":"msg_two","role":"assistant","content":[{"type":"text","text":"second message"}]}}),
        json!({"type":"result","subtype":"success","is_error":false,"result":"done","usage":{}}),
    ])
    .await;
    assert_eq!(
        rows,
        [
            (
                "assistant".into(),
                "first message".into(),
                Some("msg_one".into())
            ),
            (
                "assistant".into(),
                "second message".into(),
                Some("msg_two".into())
            ),
        ]
    );
}

/// Proves a full event naming a different id than the streamed fragments cannot
/// re-identify the message: the tail keeps the fragments' identity.
#[tokio::test]
async fn full_event_contradicting_stream_identity_keeps_fragments_joined() {
    let (rows, _) = run_stream(&[
        stream_event(json!({"type":"message_start","message":{"id":"msg_a","role":"assistant"}})),
        stream_event(json!({"type":"content_block_delta","delta":{"type":"text_delta","text":"keep "}})),
        json!({"type":"assistant","message":{"id":"msg_b","role":"assistant","content":[{"type":"text","text":"keep going"}]}}),
        json!({"type":"result","subtype":"success","is_error":false,"result":"done","usage":{}}),
    ])
    .await;
    assert_eq!(
        rows,
        [
            ("assistant".into(), "keep ".into(), Some("msg_a".into())),
            ("assistant".into(), "going".into(), Some("msg_a".into())),
        ]
    );
}

/// Proves tool activity between two streamed messages keeps journal order and
/// the per-message identity across the tool boundary.
#[tokio::test]
async fn tool_boundary_keeps_order_and_message_identity() {
    let (rows, _) = run_stream(&[
        stream_event(json!({"type":"message_start","message":{"id":"msg_one","role":"assistant"}})),
        stream_event(json!({"type":"content_block_delta","delta":{"type":"text_delta","text":"checking"}})),
        json!({"type":"assistant","message":{"id":"msg_one","role":"assistant","content":[{"type":"tool_use","id":"toolu_1","name":"shell","input":{"cmd":"ls"}}]}}),
        json!({"type":"user","message":{"role":"user","content":[{"type":"tool_result","tool_use_id":"toolu_1","content":"file.txt"}]}}),
        stream_event(json!({"type":"message_start","message":{"id":"msg_two","role":"assistant"}})),
        stream_event(json!({"type":"content_block_delta","delta":{"type":"text_delta","text":"done"}})),
        json!({"type":"assistant","message":{"id":"msg_two","role":"assistant","content":[{"type":"text","text":"done"}]}}),
        json!({"type":"result","subtype":"success","is_error":false,"result":"done","usage":{}}),
    ])
    .await;
    assert_eq!(
        rows,
        [
            (
                "assistant".into(),
                "checking".into(),
                Some("msg_one".into())
            ),
            (
                "tool_call".into(),
                "{\"cmd\":\"ls\"}".into(),
                Some("toolu_1".into())
            ),
            (
                "tool_result".into(),
                "file.txt".into(),
                Some("toolu_1".into())
            ),
            ("assistant".into(), "done".into(), Some("msg_two".into())),
        ]
    );
}
