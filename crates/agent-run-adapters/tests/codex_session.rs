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

/// Mirrors `test_codex_app_server.py::test_terminal_error_classification`.
#[test]
fn python_test_codex_app_server_error_classifications() {
    assert_eq!(
        failure_kind(&json!({"kind":"auth_failed"})),
        Some("auth_failed".into())
    );
    assert_eq!(failure_kind(&json!({"code":"quota"})), Some("quota".into()));
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
