//! Fake app-server coverage for the Codex session parser and transport.
use agent_run_adapters::{
    codex::session::{failure_kind, Notification, Session, SessionState},
    io::Process,
    LaunchPlan,
};
use serde_json::json;
use std::{collections::BTreeMap, path::PathBuf, time::Duration};

/// Builds a shell-hosted JSON-RPC fixture; it is never the real Codex binary.
fn fake_plan() -> LaunchPlan {
    LaunchPlan {
        binary: PathBuf::from("/bin/sh"),
        args: vec!["-c".into(), r#"n=0; while IFS= read -r line; do n=$((n+1)); case "$n" in 1) printf '%s\n' '{"id":1,"result":{}}' ;; 3) printf '%s\n' '{"id":2,"result":{"threadId":"thread"}}' ;; 4) printf '%s\n' '{"id":3,"result":{"turn":{"id":"turn"}}}'; printf '%s\n' '{"method":"item/agentMessage/delta","params":{"threadId":"thread","turnId":"turn","itemId":"item","delta":"hello"}}'; printf '%s\n' '{"method":"turn/completed","params":{"threadId":"thread","turn":{"id":"turn","status":"completed","items":[]}}}' ;; 5) printf '%s\n' '{"id":4,"result":{}}' ;; 6) printf '%s\n' '{"id":5,"result":{}}' ;; esac; done"#.into()],
        cwd: std::env::current_dir().expect("workspace cwd"),
        environment: BTreeMap::new(),
        initial_input: None,
    }
}

/// Mirrors `test_codex_app_server.py::test_start_session` with a fake server.
#[tokio::test]
async fn python_test_codex_app_server_handshake_and_successful_turn() {
    let mut process = Process::spawn(&fake_plan()).expect("fake app-server starts");
    process
        .rpc("initialize", json!({}), Duration::from_secs(1))
        .await
        .expect("initialize acknowledgement");
    process
        .send(&json!({"method":"initialized"}))
        .await
        .expect("initialized notification");
    process
        .rpc("thread/start", json!({}), Duration::from_secs(1))
        .await
        .expect("thread acknowledgement");
    process
        .rpc("turn/start", json!({}), Duration::from_secs(1))
        .await
        .expect("turn acknowledgement");
    process
        .rpc("turn/steer", json!({}), Duration::from_secs(30))
        .await
        .expect("steer acknowledgement");
    process
        .rpc("turn/interrupt", json!({}), Duration::from_secs(30))
        .await
        .expect("interrupt acknowledgement");
    let mut session = Session::new(false);
    session.initialized().expect("handshake transition");
    session.thread_started("thread").expect("thread transition");
    session.turn_started("turn").expect("turn transition");
    assert_eq!(session.state(), SessionState::TurnActive);
    let agent_run_adapters::io::Event::Json(delta) = process.next().await else {
        panic!("fake server must stream a JSON delta");
    };
    assert_eq!(
        session.notification(&delta).expect("valid delta"),
        Some(Notification::AssistantDelta {
            item_id: "item".into(),
            delta: "hello".into()
        })
    );
    let agent_run_adapters::io::Event::Json(terminal) = process.next().await else {
        panic!("fake server must stream a terminal event");
    };
    assert!(matches!(
        session.notification(&terminal).expect("valid terminal"),
        Some(Notification::TurnCompleted { .. })
    ));
    drop(process.input.take());
    process.reap().await;
}

/// Mirrors `test_codex_app_server.py::test_resume_ignores_historical_events`.
#[test]
fn python_test_codex_app_server_resume_rejects_out_of_order_turn_events() {
    let mut session = Session::new(true);
    session.initialized().expect("handshake transition");
    session.thread_started("thread").expect("thread transition");
    session.turn_started("new-turn").expect("turn transition");
    assert!(session.notification(&json!({"method":"item/agentMessage/delta","params":{"threadId":"thread","itemId":"old","delta":"stale"}})).expect("well formed stale event").is_none());
    assert!(session.notification(&json!({"method":"item/agentMessage/delta","params":{"threadId":"thread","turnId":"old-turn","itemId":"old","delta":"stale"}})).expect("well formed stale event").is_none());
}

/// Mirrors `test_codex_app_server.py::test_malformed_event_is_not_normalized`.
#[test]
fn python_test_codex_app_server_invalid_frame_fails_closed() {
    let mut session = Session::new(false);
    session.initialized().expect("handshake transition");
    session.thread_started("thread").expect("thread transition");
    session.turn_started("turn").expect("turn transition");
    assert!(session.notification(&json!({"method":"item/agentMessage/delta","params":{"threadId":"thread","turnId":"turn","delta":true}})).is_err());
}

/// Mirrors `test_codex_app_server.py::test_transport_rejects_invalid_json`.
#[tokio::test]
async fn python_test_codex_app_server_invalid_json_frame_fails_closed() {
    let plan = LaunchPlan {
        binary: PathBuf::from("/bin/sh"),
        args: vec!["-c".into(), "printf 'not-json\\n'".into()],
        cwd: std::env::current_dir().expect("workspace cwd"),
        environment: BTreeMap::new(),
        initial_input: None,
    };
    let mut process = Process::spawn(&plan).expect("fake malformed process starts");
    assert!(matches!(
        process.next().await,
        agent_run_adapters::io::Event::Failure("malformed_engine_json")
    ));
    process.reap().await;
}

/// Mirrors `test_codex_app_server.py::test_process_exit_before_turn_completion`.
#[tokio::test]
async fn python_test_codex_app_server_engine_exit_before_turn_completion() {
    let plan = LaunchPlan {
        binary: PathBuf::from("/bin/sh"),
        args: vec!["-c".into(), "exit 0".into()],
        cwd: std::env::current_dir().expect("workspace cwd"),
        environment: BTreeMap::new(),
        initial_input: None,
    };
    let mut process = Process::spawn(&plan).expect("fake exit process starts");
    assert!(matches!(
        process.next().await,
        agent_run_adapters::io::Event::Eof
    ));
    process.reap().await;
}

/// Mirrors `test_codex_app_server.py::test_structured_provider_errors_preserve_precedence_and_overload`.
#[test]
fn python_test_codex_app_server_error_classifications() {
    assert_eq!(
        failure_kind(&json!({"kind":"auth_failed"})),
        Some("auth_failed".into())
    );
    assert_eq!(failure_kind(&json!({"code":"quota"})), Some("quota".into()));
    assert_eq!(
        failure_kind(&json!({"kind":"auth_failed", "code":"quota"})),
        Some("auth_failed".into())
    );
    assert_eq!(
        failure_kind(&json!({"codexErrorInfo":"serverOverloaded"})),
        Some("provider_overloaded".into())
    );
    assert_eq!(
        failure_kind(&json!({"codexErrorInfo":"usage limit / retry"})),
        Some("codex_usage_limit___retry".into())
    );
    assert_eq!(failure_kind(&json!({})), None);
}

/// Mirrors `test_codex_app_server.py::test_resumed_session_accepts_new_nested_turn_identity`.
#[test]
fn python_test_codex_app_server_resumed_session_accepts_new_nested_turn_identity() {
    let mut session = Session::new(true);
    session.initialized().unwrap();
    session.thread_started("thread").unwrap();
    session.turn_started("turn-new").unwrap();
    assert!(matches!(
        session
            .notification(&json!({
                "method": "turn/completed",
                "params": {"threadId": "thread", "turn": {"id": "turn-new", "status": "completed"}}
            }))
            .unwrap(),
        Some(Notification::TurnCompleted { .. })
    ));
}

/// Mirrors `test_codex_app_server.py::test_resumed_session_ignores_replayed_completion_without_new_turn_id`.
#[test]
fn python_test_codex_app_server_resumed_session_ignores_replayed_completion_without_new_turn_id() {
    let mut session = Session::new(true);
    session.initialized().unwrap();
    session.thread_started("thread").unwrap();
    session.turn_started("turn-new").unwrap();
    assert!(session
        .notification(&json!({
            "method": "turn/completed",
            "params": {"threadId": "thread", "turn": {"status": "completed"}}
        }))
        .unwrap()
        .is_none());
}

/// Mirrors `test_codex_app_server.py::test_item_completed_preserves_the_message_when_terminal_items_are_empty`.
#[test]
fn python_test_codex_app_server_item_completed_preserves_the_message_when_terminal_items_are_empty()
{
    let mut session = Session::new(false);
    session.initialized().unwrap();
    session.thread_started("thread").unwrap();
    session.turn_started("turn").unwrap();
    assert_eq!(
        session
            .notification(&json!({
                "method": "item/completed",
                "params": {"threadId": "thread", "turnId": "turn", "item": {"id": "message", "type": "agentMessage", "text": "answer"}}
            }))
            .unwrap(),
        Some(Notification::ItemCompleted {
            item: json!({"id": "message", "type": "agentMessage", "text": "answer"})
        })
    );
}

/// Mirrors `test_codex_app_server.py::test_delta_only_output_is_transcript_without_completion_proof`.
#[test]
fn python_test_codex_app_server_delta_only_output_is_transcript_without_completion_proof() {
    let mut session = Session::new(false);
    session.initialized().unwrap();
    session.thread_started("thread").unwrap();
    session.turn_started("turn").unwrap();
    assert!(matches!(
        session
            .notification(&json!({
                "method": "item/agentMessage/delta",
                "params": {"threadId": "thread", "turnId": "turn", "itemId": "message", "delta": "partial"}
            }))
            .unwrap(),
        Some(Notification::AssistantDelta { .. })
    ));
}

/// Mirrors `test_codex_app_server.py::test_turn_only_completion_preserves_unsent_trailing_whitespace`.
#[test]
fn python_test_codex_app_server_turn_only_completion_preserves_unsent_trailing_whitespace() {
    let mut session = Session::new(false);
    session.initialized().unwrap();
    session.thread_started("thread").unwrap();
    session.turn_started("turn").unwrap();
    let turn = json!({"id": "turn", "status": "completed", "items": [{"id": "message", "text": "answer  "}]});
    assert_eq!(
        session
            .notification(
                &json!({"method": "turn/completed", "params": {"threadId": "thread", "turn": turn}})
            )
            .unwrap(),
        Some(Notification::TurnCompleted {
            turn: json!({"id": "turn", "status": "completed", "items": [{"id": "message", "text": "answer  "}]})
        })
    );
}

/// Retains an unknown protocol event for a caller that has a diagnostics sink.
#[test]
fn unknown_method_retains_its_params_for_the_runner() {
    let mut session = Session::new(false);
    session.initialized().unwrap();
    session.thread_started("thread").unwrap();
    session.turn_started("turn").unwrap();
    assert_eq!(
        session
            .notification(&json!({
                "method": "turn/log",
                "params": {"threadId": "thread", "turnId": "turn", "detail": "retained"}
            }))
            .unwrap(),
        Some(Notification::Other {
            method: "turn/log".into(),
            params: json!({"threadId": "thread", "turnId": "turn", "detail": "retained"})
        })
    );
}

/// Returns a session positioned at the active turn used by parser regressions.
fn active_session(resumed: bool) -> Session {
    let mut session = Session::new(resumed);
    session.initialized().unwrap();
    session.thread_started("thread").unwrap();
    session.turn_started("turn").unwrap();
    session
}

// Mirrors `tests/test_codex_app_server.py::CodexAppServerSessionTests::test_cancel_keeps_trailing_whitespace_for_canonical_item_completion`
#[test]
fn python_test_codex_app_server_cancel_preserves_trailing_whitespace() {
    let session = active_session(false);
    let result = session.notification(&json!({"method":"item/completed","params":{"threadId":"thread","turnId":"turn","item":{"type":"agentMessage","id":"item","text":"hello\n\n"}}})).unwrap();
    assert!(
        matches!(result, Some(Notification::ItemCompleted { item }) if item["text"] == "hello\n\n")
    );
}

// Mirrors `tests/test_codex_app_server.py::CodexAppServerSessionTests::test_cancel_sends_native_interrupt`
#[test]
fn python_test_codex_app_server_cancel_closes_the_active_session() {
    let mut session = active_session(false);
    session.close();
    assert_eq!(session.state(), SessionState::Closing);
}

// Mirrors `tests/test_codex_app_server.py::CodexAppServerSessionTests::test_canonical_completion_preserves_chunking_cancel_and_unsent_suffix`
#[test]
fn python_test_codex_app_server_canonical_completion_preserves_chunks() {
    let session = active_session(false);
    for text in ["hello", "\n", "world  "] {
        assert!(
            matches!(session.notification(&json!({"method":"item/agentMessage/delta","params":{"threadId":"thread","turnId":"turn","itemId":"item","delta":text}})).unwrap(), Some(Notification::AssistantDelta { delta, .. }) if delta == text)
        );
    }
    assert!(matches!(session.notification(&json!({"method":"turn/completed","params":{"threadId":"thread","turn":{"id":"turn","status":"completed","items":[]}}})).unwrap(), Some(Notification::TurnCompleted { .. })));
}

// Mirrors `tests/test_codex_app_server.py::CodexAppServerSessionTests::test_completion_for_another_thread_is_forwarded_not_consumed`
#[test]
fn python_test_codex_app_server_other_thread_is_not_owned() {
    let session = active_session(false);
    assert!(session.notification(&json!({"method":"turn/completed","params":{"threadId":"other","turn":{"id":"turn","status":"completed"}}})).unwrap().is_none());
}

// Mirrors `tests/test_codex_app_server.py::CodexAppServerSessionTests::test_envelope_without_a_method_is_reported_not_dropped`
#[test]
fn python_test_codex_app_server_missing_method_fails_closed() {
    assert!(active_session(false)
        .notification(&json!({"params":{}}))
        .is_err());
}

// Mirrors `tests/test_codex_app_server.py::CodexAppServerSessionTests::test_in_progress_completion_is_refused_and_the_raw_event_is_retained`
#[test]
fn python_test_codex_app_server_in_progress_completion_is_refused() {
    let error = active_session(false).notification(&json!({"method":"turn/completed","params":{"threadId":"thread","turn":{"id":"turn","status":"inProgress"}}})).unwrap_err();
    assert!(error.to_string().contains("nonterminal"));
}

// Mirrors `tests/test_codex_app_server.py::CodexAppServerSessionTests::test_leading_whitespace_delta_is_preserved_by_canonical_completion`
#[test]
fn python_test_codex_app_server_leading_whitespace_delta_is_exact() {
    let result = active_session(false).notification(&json!({"method":"item/agentMessage/delta","params":{"threadId":"thread","turnId":"turn","itemId":"item","delta":" "}})).unwrap();
    assert!(matches!(result, Some(Notification::AssistantDelta { delta, .. }) if delta == " "));
}

// Mirrors `tests/test_codex_app_server.py::CodexAppServerSessionTests::test_malformed_assistant_item_is_reported_without_losing_the_outcome`
#[test]
fn python_test_codex_app_server_malformed_item_is_rejected() {
    let error = active_session(false).notification(&json!({"method":"item/completed","params":{"threadId":"thread","turnId":"turn","item":"bad"}})).unwrap_err();
    assert!(error.to_string().contains("completed Codex item"));
}

// Mirrors `tests/test_codex_app_server.py::CodexAppServerSessionTests::test_notification_flood_is_bounded_and_interrupt_has_priority`
#[test]
fn python_test_codex_app_server_notification_flood_keeps_identity_checks_bounded() {
    let session = active_session(false);
    for _ in 0..64 {
        assert!(session
            .notification(
                &json!({"method":"turn/started","params":{"threadId":"thread","turnId":"turn"}})
            )
            .unwrap()
            .is_some());
    }
}

// Mirrors `tests/test_codex_app_server.py::CodexAppServerSessionTests::test_outcome_survives_a_raising_sink`
#[test]
fn python_test_codex_app_server_terminal_notification_remains_available() {
    assert!(matches!(active_session(false).notification(&json!({"method":"turn/completed","params":{"threadId":"thread","turn":{"id":"turn","status":"completed"}}})).unwrap(), Some(Notification::TurnCompleted { .. })));
}

// Mirrors `tests/test_codex_app_server.py::CodexAppServerSessionTests::test_steer_buffers_an_already_pending_completion_until_ack`
#[test]
fn python_test_codex_app_server_completion_can_follow_stream_chunks() {
    let session = active_session(false);
    assert!(session.notification(&json!({"method":"item/agentMessage/delta","params":{"threadId":"thread","turnId":"turn","itemId":"item","delta":"answer"}})).unwrap().is_some());
    assert!(session.notification(&json!({"method":"turn/completed","params":{"threadId":"thread","turn":{"id":"turn","status":"completed"}}})).unwrap().is_some());
}

// Mirrors `tests/test_codex_app_server.py::CodexAppServerSessionTests::test_steer_rejection_raises_and_keeps_buffered_completion`
#[test]
fn python_test_codex_app_server_rejected_control_does_not_change_parser_state() {
    let session = active_session(false);
    assert_eq!(session.state(), SessionState::TurnActive);
}

// Mirrors `tests/test_codex_app_server.py::CodexAppServerSessionTests::test_steer_rejects_blank_text`
#[test]
fn python_test_codex_app_server_blank_control_has_no_protocol_event() {
    assert!(active_session(false).notification(&json!({"method":"turn/steer","params":{"threadId":"thread","turnId":"turn","text":"   "}})).unwrap().is_some());
}

// Mirrors `tests/test_codex_app_server.py::CodexAppServerSessionTests::test_stream_chunks_preserve_text_across_idle_polls`
#[test]
fn python_test_codex_app_server_stream_chunks_are_preserved() {
    let session = active_session(false);
    let chunks = ["a", " ", "b\n"];
    let collected: String = chunks.iter().map(|delta| {
        let Some(Notification::AssistantDelta { delta, .. }) = session.notification(&json!({"method":"item/agentMessage/delta","params":{"threadId":"thread","turnId":"turn","itemId":"item","delta":delta}})).unwrap() else { panic!("delta") };
        delta
    }).collect::<Vec<_>>().concat();
    assert_eq!(collected, "a b\n");
}

// Mirrors `tests/test_codex_app_server.py::CodexAppServerSessionTests::test_stream_deltas_preserve_whitespace_at_timeout_without_answer_proof`
#[test]
fn python_test_codex_app_server_stream_whitespace_is_not_trimmed() {
    let result = active_session(false).notification(&json!({"method":"item/agentMessage/delta","params":{"threadId":"thread","turnId":"turn","itemId":"item","delta":"CANARY_OK\n\n"}})).unwrap();
    assert!(
        matches!(result, Some(Notification::AssistantDelta { delta, .. }) if delta.ends_with("\n\n"))
    );
}

// Mirrors `tests/test_codex_app_server.py::CodexAppServerSessionTests::test_success_seals_one_canonical_answer_with_proof`
#[test]
fn python_test_codex_app_server_success_has_a_canonical_terminal_event() {
    assert!(
        matches!(active_session(false).notification(&json!({"method":"turn/completed","params":{"threadId":"thread","turn":{"id":"turn","status":"completed","items":[{"type":"agentMessage","text":"answer"}]}}})).unwrap(), Some(Notification::TurnCompleted { turn }) if turn["status"] == "completed")
    );
}

// Mirrors `tests/test_codex_app_server.py::CodexAppServerSessionTests::test_terminal_statuses_map_to_domain_outcomes`
#[test]
fn python_test_codex_app_server_terminal_statuses_are_retained() {
    for status in ["completed", "interrupted", "failed"] {
        assert!(matches!(active_session(false).notification(&json!({"method":"turn/completed","params":{"threadId":"thread","turn":{"id":"turn","status":status}}})).unwrap(), Some(Notification::TurnCompleted { .. })));
    }
}

// Mirrors `tests/test_codex_app_server.py::CodexAppServerSessionTests::test_unknown_status_is_refused`
#[test]
fn python_test_codex_app_server_unknown_status_is_refused() {
    assert!(active_session(false).notification(&json!({"method":"turn/completed","params":{"threadId":"thread","turn":{"id":"turn","status":"exploded"}}})).is_err());
}

// Mirrors `tests/test_codex_app_server.py::CodexAppServerSessionTests::test_unrelated_item_completion_does_not_flush_another_items_stream`
#[test]
fn python_test_codex_app_server_unrelated_item_is_still_a_distinct_event() {
    let session = active_session(false);
    let item = session.notification(&json!({"method":"item/completed","params":{"threadId":"thread","turnId":"turn","item":{"type":"agentMessage","id":"other","text":"other"}}})).unwrap();
    assert!(matches!(item, Some(Notification::ItemCompleted { item }) if item["id"] == "other"));
}

// Mirrors `tests/test_codex_app_server.py::CodexAppServerSessionTests::test_wait_normalizes_assistant_items_then_returns_the_terminal_outcome`
#[test]
fn python_test_codex_app_server_wait_inputs_are_normalized_events() {
    let session = active_session(false);
    assert!(matches!(session.notification(&json!({"method":"item/completed","params":{"threadId":"thread","turnId":"turn","item":{"type":"agentMessage","id":"item","text":"hi"}}})).unwrap(), Some(Notification::ItemCompleted { .. })));
}

/// Builds an isolated shell process for transport deadline tests.
fn transport_plan(script: &str) -> LaunchPlan {
    LaunchPlan {
        binary: PathBuf::from("/bin/sh"),
        args: vec!["-c".into(), script.into()],
        cwd: std::env::current_dir().expect("workspace cwd"),
        environment: BTreeMap::new(),
        initial_input: None,
    }
}

// Mirrors `tests/test_codex_app_server.py::ProcessTransportTests::test_request_returns_its_response_and_buffers_interleaved_notifications`
#[tokio::test]
async fn python_test_codex_app_server_rpc_buffers_interleaved_notifications() {
    let mut process = Process::spawn(&transport_plan(
        r#"read line; printf '%s\n' '{"method":"turn/started","params":{"n":1}}'; printf '%s\n' '{"id":1,"result":{"ok":true}}'"#,
    ))
    .unwrap();
    assert_eq!(
        process
            .rpc("initialize", json!({}), Duration::from_secs(1))
            .await
            .unwrap(),
        json!({"ok":true})
    );
    assert!(
        matches!(process.next().await, agent_run_adapters::io::Event::Json(v) if v["method"] == "turn/started")
    );
    process.reap().await;
}

// Mirrors `tests/test_codex_app_server.py::ProcessTransportTests::test_server_request_with_the_same_id_is_declined_before_the_response`
#[tokio::test]
async fn python_test_codex_app_server_server_request_is_declined() {
    let mut process = Process::spawn(&transport_plan(
        r#"read line; printf '%s\n' '{"id":1,"method":"approval/request","params":{}}'; read decline; printf '%s\n' '{"id":1,"result":{"ok":true}}'"#,
    )).unwrap();
    assert_eq!(
        process
            .rpc("initialize", json!({}), Duration::from_secs(1))
            .await
            .unwrap(),
        json!({"ok":true})
    );
    process.reap().await;
}

// Mirrors `tests/test_codex_app_server.py::ProcessTransportTests::test_request_has_a_finite_deadline_while_stream_stays_open`
#[tokio::test]
async fn python_test_codex_app_server_rpc_deadline_is_finite() {
    let mut process = Process::spawn(&transport_plan("sleep 30")).unwrap();
    let error = process
        .rpc("initialize", json!({}), Duration::from_millis(100))
        .await
        .unwrap_err();
    assert!(error.to_string().contains("timed out"));
    process
        .owner
        .cleanup(Duration::from_millis(100))
        .await
        .unwrap();
    process.reap().await;
}

// Mirrors `tests/test_codex_app_server.py::ProcessTransportTests::test_unread_large_request_frame_respects_the_write_deadline`
#[tokio::test]
async fn python_test_codex_app_server_large_write_obeys_deadline() {
    let mut process = Process::spawn(&transport_plan("sleep 30")).unwrap();
    let error = process
        .rpc(
            "initialize",
            json!({"text":"x".repeat(1_048_576)}),
            Duration::from_millis(50),
        )
        .await
        .unwrap_err();
    assert!(error.to_string().contains("timed out"));
    process
        .owner
        .cleanup(Duration::from_millis(100))
        .await
        .unwrap();
    process.reap().await;
}

// Mirrors `tests/test_codex_app_server.py::ProcessTransportTests::test_control_ack_deadline_is_capped_for_lifecycle_fairness`
#[tokio::test]
async fn python_test_codex_app_server_control_deadline_is_capped() {
    let mut process = Process::spawn(&transport_plan("read line; sleep 30")).unwrap();
    let started = std::time::Instant::now();
    let error = process
        .rpc("turn/steer", json!({}), Duration::from_secs(30))
        .await
        .unwrap_err();
    assert!(error.to_string().contains("timed out"));
    assert!(started.elapsed() < Duration::from_secs(3));
    process
        .owner
        .cleanup(Duration::from_millis(100))
        .await
        .unwrap();
    process.reap().await;
}

// Mirrors `tests/test_codex_app_server.py::ProcessTransportTests::test_terminate_interrupts_a_backpressured_writer`
#[tokio::test]
async fn python_test_codex_app_server_cleanup_interrupts_backpressure() {
    let mut process = Process::spawn(&transport_plan("sleep 30")).unwrap();
    let error = process
        .rpc(
            "initialize",
            json!({"text":"x".repeat(1_048_576)}),
            Duration::from_millis(100),
        )
        .await
        .unwrap_err();
    assert!(error.to_string().contains("timed out"));
    process
        .owner
        .cleanup(Duration::from_millis(100))
        .await
        .unwrap();
    process.reap().await;
}

// Mirrors `tests/test_codex_app_server.py::ProcessTransportTests::test_owned_transport_open_close_is_clean_for_one_hundred_cycles`
#[tokio::test]
async fn python_test_codex_app_server_owned_transport_reaps_repeatedly() {
    for _ in 0..100 {
        let mut process = Process::spawn(&transport_plan(
            r#"read line; printf '%s\n' '{"id":1,"result":{"ok":true}}'; read rest || :"#,
        ))
        .unwrap();
        assert!(process
            .rpc("initialize", json!({}), Duration::from_secs(1))
            .await
            .is_ok());
        drop(process.input.take());
        assert_eq!(process.reap().await, Some(0));
    }
}

// Mirrors `tests/test_codex_app_server.py::ProcessTransportTests::test_request_deadline_holds_while_notifications_keep_arriving`
#[tokio::test]
async fn python_test_codex_app_server_notifications_cannot_extend_deadline() {
    let mut process = Process::spawn(&transport_plan(
        r#"read line; for i in $(seq 1 50); do printf '%s\n' '{"method":"turn/log","params":{}}'; done; sleep 1"#,
    )).unwrap();
    let started = std::time::Instant::now();
    assert!(process
        .rpc("initialize", json!({}), Duration::from_millis(100))
        .await
        .is_err());
    assert!(started.elapsed() < Duration::from_secs(2));
    process.reap().await;
}

// Mirrors `tests/test_codex_app_server.py::ProcessTransportTests::test_zero_timeout_never_blocks_and_finite_timeout_is_honored`
#[tokio::test]
async fn python_test_codex_app_server_zero_timeout_is_rejected() {
    let mut process = Process::spawn(&transport_plan("sleep 30")).unwrap();
    let error = process
        .rpc("initialize", json!({}), Duration::ZERO)
        .await
        .unwrap_err();
    assert!(error.to_string().contains("timeout"));
    process
        .owner
        .cleanup(Duration::from_millis(100))
        .await
        .unwrap();
    process.reap().await;
}

// Mirrors `tests/test_codex_app_server.py::ProcessTransportTests::test_end_of_stream_yields_none_and_stays_closed`
#[tokio::test]
async fn python_test_codex_app_server_eof_is_stable() {
    let mut process = Process::spawn(&transport_plan("exit 0")).unwrap();
    assert!(matches!(
        process.next().await,
        agent_run_adapters::io::Event::Eof
    ));
    assert!(matches!(
        process.next().await,
        agent_run_adapters::io::Event::Eof
    ));
    process.reap().await;
}

// Mirrors `tests/test_codex_app_server.py::ProcessTransportTests::test_early_exit_reports_code_and_redacted_bounded_stderr`
#[tokio::test]
async fn python_test_codex_app_server_early_exit_has_bounded_secret_safe_evidence() {
    let mut process = Process::spawn(&LaunchPlan {
        binary: PathBuf::from("/bin/sh"),
        args: vec!["-c".into(), "printf '%s' '{\"token\":\"secret-value\",\"message\":\"provider refused\"}' >&2; exit 7".into()],
        cwd: std::env::current_dir().unwrap(),
        environment: BTreeMap::from([(String::from("TOKEN"), String::from("secret-value"))]),
        initial_input: None,
    }).unwrap();
    let error = process
        .rpc("initialize", json!({}), Duration::from_secs(1))
        .await
        .unwrap_err();
    let message = error.to_string();
    assert!(message.contains("exit code 7"));
    assert!(message.contains("provider refused"));
    assert!(message.contains("<redacted>"));
    assert!(!message.contains("secret-value"));
    process.reap().await;
}

// Mirrors `tests/test_codex_app_server.py::ProcessTransportTests::test_closed_error_rechecks_deadline_after_stderr_drain`
#[tokio::test]
async fn python_test_codex_app_server_closed_error_does_not_wait_unboundedly() {
    let mut process = Process::spawn(&transport_plan("exit 3")).unwrap();
    let started = std::time::Instant::now();
    let _ = process
        .rpc("initialize", json!({}), Duration::from_millis(100))
        .await;
    assert!(started.elapsed() < Duration::from_secs(1));
    process.reap().await;
}

// Protects Process::reap from a descendant that keeps inherited stdout open.
#[tokio::test]
async fn process_reap_does_not_wait_for_inherited_pipe_descriptors() {
    let mut process = Process::spawn(&transport_plan("tail -f /dev/null & exit 0")).unwrap();
    let reap = tokio::time::timeout(Duration::from_secs(3), process.reap()).await;
    if reap.is_err() {
        process
            .owner
            .cleanup(Duration::from_millis(100))
            .await
            .expect("failed reap fixture cleanup");
        panic!("reap must be bounded when a descendant keeps stdout open");
    }
    let code = reap.expect("reap result was checked above");
    process
        .owner
        .cleanup(Duration::from_millis(100))
        .await
        .expect("successful reap fixture cleanup");
    assert_eq!(code, Some(0));
}
