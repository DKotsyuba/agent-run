//! Completion delivery parity fixtures for Python's delivery dispatcher and notice contract.

use agent_run_core::{
    delivery::{claude, dispatch_once, relay, safe_evidence, Notice},
    domain::{now, AgentId, Status},
};
use rusqlite::{params, Connection};
use serde_json::{json, Value};
use std::{
    path::{Path, PathBuf},
    sync::OnceLock,
    time::Duration,
};

mod common;

/// Returns a repository fixture path without making the immutable fixture writable.
fn fixture(path: &str) -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .join(path)
}

/// Inserts the minimum terminal agent, bound session, and delivery rows for a dispatcher test.
fn delivery(home: &Path, id: &str, transport: &str, state: &str) {
    let connection = Connection::open(home.join("state.db")).unwrap();
    connection
        .execute(
            "INSERT INTO orchestrator_sessions(id,transport,external_session_id,created_at,last_seen_at) VALUES('sess',?,?,?,?)",
            params![transport, "thread", now(), now()],
        )
        .unwrap();
    connection
        .execute(
            "INSERT INTO agents(id,runtime,model,profile,task,task_summary,workdir,request_json,status,created_at,timeout_seconds,config_revision,root_agent_id) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)",
            params!["ag-20260825-120000-0123456789", "mock", "fixture", "review", "private task", "summary", home.to_string_lossy(), "{}", "succeeded", now(), 1.0, "fixture", "ag-20260825-120000-0123456789"],
        )
        .unwrap();
    connection
        .execute(
            "INSERT INTO deliveries(id,agent_id,orchestrator_session_id,state,next_attempt_at) VALUES(?,?, 'sess',?,?)",
            params![id, "ag-20260825-120000-0123456789", state, now() - 1.0],
        )
        .unwrap();
}

/// Mirrors `tests/test_delivery_base.py::test_every_terminal_status_renders_its_own_line`.
/// Mirrors `tests/test_delivery_base.py::test_failed_notice_explains_known_and_unknown_failure_categories`.
/// Mirrors `tests/test_delivery_base.py::test_configured_identifier_punctuation_renders_verbatim`.
/// Mirrors `tests/test_delivery_base.py::test_metadata_can_never_add_list_lines_or_commands`.
/// Mirrors `tests/test_delivery_base.py::test_missing_metadata_renders_unknown_and_unspecified`.
/// Replays every frozen Python rendering capture (`tests/fixtures/baseline/notices/cases.json`).
#[test]
fn notice_rendering_matches_python_golden_cases() {
    let cases: Vec<Value> = serde_json::from_slice(
        &std::fs::read(fixture("tests/fixtures/baseline/notices/cases.json")).unwrap(),
    )
    .unwrap();
    for case in cases {
        let input = case.get("input").unwrap();
        let notice = Notice {
            notification_id: input["notification_id"].as_str().unwrap().into(),
            agent_id: input["agent_id"]
                .as_str()
                .unwrap()
                .parse::<AgentId>()
                .unwrap(),
            status: serde_json::from_value(input["status"].clone()).unwrap(),
            runtime: input
                .get("runtime")
                .and_then(Value::as_str)
                .map(str::to_owned),
            model: input
                .get("model")
                .and_then(Value::as_str)
                .map(str::to_owned),
            effort: input
                .get("effort")
                .and_then(Value::as_str)
                .map(str::to_owned),
            failure_kind: input
                .get("failure_kind")
                .and_then(Value::as_str)
                .map(str::to_owned),
        };
        assert_eq!(notice.render().unwrap(), case["rendered"], "{}", case["id"]);
    }
}

/// Mirrors `tests/test_codex_queue.py::test_attempt_evidence_is_bounded_redacted_and_exactly_classified`.
#[test]
fn evidence_redacts_secret_shaped_tails_and_caps_utf8() {
    let raw = json!({
        "classifier":"retry", "executable":"codex-queue", "argv_shape":["codex-queue", "<redacted>"],
        "duration_ms":1, "returncode":75, "spawn_errno":null, "error_class":"temporary",
        "stdout_tail": format!("Authorization: Bearer sk-delivery-secret\\n{}", "ёж".repeat(3000)),
        "stderr_tail":"token=also-secret", "stdout_bytes":10000, "stderr_bytes":17,
        "stdout_truncated":false, "stderr_truncated":false, "message_id_present":false
    });
    let safe = safe_evidence(&raw).unwrap();
    let persisted = serde_json::to_string(&safe).unwrap();
    assert!(!persisted.contains("sk-delivery-secret"));
    assert!(!persisted.contains("also-secret"));
    assert!(safe.stdout_tail.len() <= 4096);
    assert!(safe.stderr_tail.len() <= 4096);
    assert!(persisted.len() <= 16 * 1024);
}

/// Mirrors `tests/test_delivery_dispatch.py::test_missing_codex_relay_retries_then_recovers_without_queue`.
#[tokio::test]
async fn unavailable_queue_retries_once_with_one_evidence_row() {
    let home = common::Home::new();
    delivery(&home.path, "ntf_retry", "codex_queue", "pending");
    assert_eq!(dispatch_once(&home.path).await.unwrap(), 1);
    let connection = Connection::open(home.path.join("state.db")).unwrap();
    let row: (String, u32, Option<f64>, Option<String>) = connection
        .query_row(
            "SELECT state,attempts,next_attempt_at,last_error FROM deliveries WHERE id='ntf_retry'",
            [],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
        )
        .unwrap();
    assert_eq!(row.0, "retry_wait");
    assert_eq!(row.1, 1);
    assert!(row.2.unwrap() > now());
    assert_eq!(row.3.as_deref(), Some("relay_unavailable"));
    let evidence: i64 = connection
        .query_row(
            "SELECT COUNT(*) FROM delivery_attempt_evidence WHERE delivery_id='ntf_retry'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(evidence, 1);
}

/// Mirrors `tests/test_delivery_dispatch.py::test_ambiguous_timeout_retries_with_capped_backoff_and_stays_durable`.
#[tokio::test]
async fn ambiguous_acknowledgement_is_recorded_and_retried() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    let home = common::Home::new();
    delivery(&home.path, "ntf_ambiguous", "codex_queue", "pending");
    let listener = tokio::net::UnixListener::bind(home.path.join("ar-cdx-v3-fake.sock")).unwrap();
    let peer = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        let length = stream.read_u32_le().await.unwrap() as usize;
        let mut request = vec![0; length];
        stream.read_exact(&mut request).await.unwrap();
        let reply = br#"{"outcome":"ambiguous"}"#;
        stream.write_u32_le(reply.len() as u32).await.unwrap();
        stream.write_all(reply).await.unwrap();
    });
    assert_eq!(dispatch_once(&home.path).await.unwrap(), 1);
    peer.await.unwrap();
    let connection = Connection::open(home.path.join("state.db")).unwrap();
    let row: (String, bool, String) = connection
        .query_row(
            "SELECT state,ambiguous_result,last_error FROM deliveries WHERE id='ntf_ambiguous'",
            [],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .unwrap();
    assert_eq!(row.0, "retry_wait");
    assert!(row.1);
    assert_eq!(row.2, "relay_ambiguous");
}

/// Mirrors `tests/test_delivery_dispatch.py::test_unknown_transport_is_a_permanent_configuration_failure`.
#[tokio::test]
async fn unknown_transport_is_terminal_and_unbound_from_schedule() {
    let home = common::Home::new();
    delivery(&home.path, "ntf_unknown", "not-configured", "pending");
    assert_eq!(dispatch_once(&home.path).await.unwrap(), 1);
    let connection = Connection::open(home.path.join("state.db")).unwrap();
    let row: (String, Option<f64>) = connection
        .query_row(
            "SELECT state,next_attempt_at FROM deliveries WHERE id='ntf_unknown'",
            [],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .unwrap();
    assert_eq!(row.0, "failed");
    assert_eq!(row.1, None);
}

/// Mirrors `tests/test_delivery_dispatch.py::test_never_bound_notice_for_a_terminal_agent_expires_after_the_window`.
#[tokio::test]
async fn unbound_terminal_delivery_expires_before_claiming() {
    let home = common::Home::new();
    let connection = Connection::open(home.path.join("state.db")).unwrap();
    let agent = "ag-20260825-120000-0123456789";
    connection.execute("INSERT INTO agents(id,runtime,model,profile,task,task_summary,workdir,request_json,status,created_at,timeout_seconds,config_revision,root_agent_id) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)", params![agent,"mock","fixture","review","private task","summary",home.path.to_string_lossy(),"{}","succeeded",now()-7200.0,1.0,"fixture",agent]).unwrap();
    connection
        .execute(
            "INSERT INTO events(agent_id,at,kind,data_json) VALUES(?,?,?,?)",
            params![agent, now() - 7200.0, "status", "{}"],
        )
        .unwrap();
    let event: i64 = connection.last_insert_rowid();
    connection.execute("INSERT INTO deliveries(id,agent_id,terminal_event_seq,state) VALUES(?,?,?,'waiting_binding')", params!["ntf_expire",agent,event]).unwrap();
    drop(connection);
    assert_eq!(dispatch_once(&home.path).await.unwrap(), 0);
    let connection = Connection::open(home.path.join("state.db")).unwrap();
    let state: String = connection
        .query_row(
            "SELECT state FROM deliveries WHERE id='ntf_expire'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(state, "expired");
}

/// Fixture-integrity check: the frozen Python database capture is copied
/// before inspection so this test suite never mutates a shared golden file.
/// Not a direct port of a single Python test.
#[test]
fn golden_delivery_database_is_read_from_a_copy() {
    let temporary = tempfile::tempdir().unwrap();
    let copy = temporary.path().join("current-v16.sqlite");
    std::fs::copy(
        fixture("tests/fixtures/baseline/db/current-v16.sqlite"),
        &copy,
    )
    .unwrap();
    let connection = Connection::open(copy).unwrap();
    let rows: i64 = connection
        .query_row(
            "SELECT COUNT(*) FROM delivery_attempt_evidence",
            [],
            |row| row.get(0),
        )
        .unwrap();
    let state: String = connection
        .query_row(
            "SELECT state FROM deliveries WHERE id='delivery-succeeded'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(rows, 2);
    assert_eq!(state, "delivered");
}

/// Builds a valid notice without any task, answer, or host-provided text.
fn notice() -> Notice {
    Notice {
        notification_id: "ntf_test".into(),
        agent_id: "ag-20260825-120000-0123456789".parse().unwrap(),
        status: Status::Succeeded,
        runtime: None,
        model: None,
        effort: None,
        failure_kind: None,
    }
}

/// Creates a notice carrying the rich v2 selectors and v3 failure category.
fn rich_notice() -> Notice {
    Notice {
        notification_id: "ntf_rich".into(),
        agent_id: "ag-20260825-120000-0123456789".parse().unwrap(),
        status: Status::Succeeded,
        runtime: Some("codex".into()),
        model: Some("gpt-5.2-codex".into()),
        effort: Some("high".into()),
        failure_kind: None,
    }
}

/// Serves one local relay frame and returns the decoded request.
async fn fake_relay(path: &Path, outcome: &'static str) -> tokio::task::JoinHandle<Value> {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let listener = tokio::net::UnixListener::bind(path).unwrap();
    tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        let length = stream.read_u32_le().await.unwrap() as usize;
        let mut data = vec![0; length];
        stream.read_exact(&mut data).await.unwrap();
        let reply = serde_json::to_vec(&json!({"outcome":outcome})).unwrap();
        stream.write_u32_le(reply.len() as u32).await.unwrap();
        stream.write_all(&reply).await.unwrap();
        serde_json::from_slice(&data).unwrap()
    })
}

/// Builds a little-endian framed JSON value without the relay's local request cap.
fn frame(value: &Value) -> Vec<u8> {
    let data = serde_json::to_vec(value).unwrap();
    let mut frame = (data.len() as u32).to_le_bytes().to_vec();
    frame.extend(data);
    frame
}

/// Reads one little-endian framed JSON value from a private fake Desktop pipe.
async fn read_frame(stream: &mut tokio::net::UnixStream) -> Value {
    use tokio::io::AsyncReadExt;
    let length = stream.read_u32_le().await.unwrap() as usize;
    let mut data = vec![0; length];
    stream.read_exact(&mut data).await.unwrap();
    serde_json::from_slice(&data).unwrap()
}

/// Writes one host-sized framed JSON response to a private fake Desktop pipe.
async fn write_frame(stream: &mut tokio::net::UnixStream, value: &Value) {
    use tokio::io::AsyncWriteExt;
    stream.write_all(&frame(value)).await.unwrap();
}

/// Serializes tests that temporarily configure the process-wide relay paths.
fn relay_env_lock() -> &'static tokio::sync::Mutex<()> {
    static LOCK: OnceLock<tokio::sync::Mutex<()>> = OnceLock::new();
    LOCK.get_or_init(|| tokio::sync::Mutex::new(()))
}

/// Runs one Rust relay host exchange against a private fake Desktop MCP pipe.
async fn host_exchange(request: Value, mode: &str) -> (Value, Option<Value>) {
    use tokio::io::AsyncWriteExt;
    let _guard = relay_env_lock().lock().await;
    let root = tempfile::tempdir().unwrap();
    let pipe_path = root.path().join("desktop.sock");
    let pipe_listener = tokio::net::UnixListener::bind(&pipe_path).unwrap();
    let mode = mode.to_owned();
    let host_peer = tokio::spawn(async move {
        if mode == "malformed" {
            let _ = tokio::time::timeout(Duration::from_millis(200), pipe_listener.accept()).await;
            return None;
        }
        let (mut stream, _) = pipe_listener.accept().await.unwrap();
        let listed = read_frame(&mut stream).await;
        assert_eq!(listed["id"], 1);
        if mode == "precall" {
            write_frame(&mut stream, &json!({"jsonrpc":"2.0","id":999,"result":{}})).await;
            return None;
        }
        if mode == "slow" {
            tokio::time::sleep(Duration::from_secs(2)).await;
        }
        let inventory = json!({"jsonrpc":"2.0","id":1,"result":{"tools":[
            {"name":"send_message_to_thread","namespace":"codex_app","description":"x".repeat(42_000)}
        ]}});
        write_frame(&mut stream, &inventory).await;
        let called = read_frame(&mut stream).await;
        assert_eq!(called["id"], 2);
        if mode == "drop" {
            return Some(called);
        }
        let response = if mode == "badid" {
            json!({"jsonrpc":"2.0","id":999,"result":{"success":true,"contentItems":[]}})
        } else if mode == "error" {
            json!({"jsonrpc":"2.0","id":2,"error":{"code":-1}})
        } else {
            json!({"jsonrpc":"2.0","id":2,"result":{"success":mode != "false","contentItems":[]}})
        };
        write_frame(&mut stream, &response).await;
        Some(called)
    });
    let node = [
        "/usr/local/bin/node",
        "/opt/homebrew/bin/node",
        "/usr/bin/node",
    ]
    .iter()
    .map(PathBuf::from)
    .find(|path| path.is_file())
    .expect("Node is required for relay host tests");
    // SAFETY: host tests serialize process-environment mutation with relay_env_lock.
    unsafe {
        std::env::set_var("CODEX_APP_TOOLS_PIPE_PATH", &pipe_path);
        std::env::set_var("CODEX_MCP_NODE_PATH", &node);
    }
    let host = relay::host(root.path()).unwrap().expect("relay host");
    let endpoint = std::fs::read_dir(root.path())
        .unwrap()
        .flatten()
        .map(|entry| entry.path())
        .find(|path| {
            path.file_name()
                .unwrap()
                .to_string_lossy()
                .starts_with("ar-cdx-v3-")
        })
        .expect("relay endpoint");
    let mut client = tokio::net::UnixStream::connect(endpoint).await.unwrap();
    client.write_all(&frame(&request)).await.unwrap();
    let response = read_frame(&mut client).await;
    let called = host_peer.await.unwrap();
    drop(host);
    // SAFETY: host tests serialize process-environment mutation with relay_env_lock.
    unsafe {
        std::env::remove_var("CODEX_APP_TOOLS_PIPE_PATH");
        std::env::remove_var("CODEX_MCP_NODE_PATH");
    }
    (response, called)
}

/// Writes a fake Claude descriptor and its paired inbox key under a temporary registry.
fn claude_descriptor(registry: &Path, session: &str, socket: &Path) {
    std::fs::write(
        registry.join("41.json"),
        json!({"sessionId":session,"messagingSocketPath":socket,"pid":41}).to_string(),
    )
    .unwrap();
    std::fs::write(
        registry.join("41.fixture.key"),
        r#"{"peerToken":"fixture-token"}"#,
    )
    .unwrap();
}

/// Mirrors `tests/test_delivery_base.py::test_metadata_must_be_bounded_strings_or_none`.
/// Mirrors `tests/test_delivery_base.py::test_success_and_cancelled_notices_reject_failure_categories`.
/// Mirrors `tests/test_delivery_base.py::test_render_is_the_exact_structured_list`.
/// Mirrors `tests/test_delivery_base.py::test_rendered_message_repeats_only_payload_facts`.
/// Mirrors `tests/test_delivery_base.py::test_notice_carries_trusted_fields_and_a_frozen_legacy_payload`.
#[test]
fn notice_rejects_invalid_metadata_and_contains_only_trusted_facts() {
    let mut invalid = notice();
    invalid.runtime = Some(" ".into());
    assert!(invalid.validate().is_err());
    invalid.runtime = Some("x".repeat(129));
    assert!(invalid.validate().is_err());
    invalid.runtime = None;
    invalid.failure_kind = Some("prepare_failed".into());
    assert!(invalid.validate().is_err());
    let rendered = notice().render().unwrap();
    assert_eq!(rendered.lines().count(), 6);
    assert!(rendered.contains("- Notice: [notification ntf_test v1]"));
    assert!(!rendered.contains("task"));
    assert!(!rendered.contains("answer"));
}

/// Mirrors `tests/test_claude_uds.py::ClaudeSessionSenderTests::test_clean_send_writes_the_auth_line_then_the_user_line`.
/// Mirrors `tests/test_claude_uds.py::ClaudeUdsTransportTests::test_send_injects_the_fixed_trusted_message_without_a_remote_id`.
/// Unix socket test: the endpoint is a private temporary fake, never a live Claude socket.
#[tokio::test]
async fn claude_uds_writes_auth_then_trusted_notice_to_fake_socket() {
    use tokio::io::AsyncReadExt;

    let temporary = tempfile::tempdir().unwrap();
    let registry = temporary.path().join("sessions");
    std::fs::create_dir(&registry).unwrap();
    let socket = temporary.path().join("inbox.sock");
    claude_descriptor(&registry, "session-1", &socket);
    let listener = tokio::net::UnixListener::bind(&socket).unwrap();
    let receiver = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        let mut received = String::new();
        stream.read_to_string(&mut received).await.unwrap();
        received
    });
    let evidence = claude::send(&registry, "session-1", &notice()).await;
    let received = receiver.await.unwrap();
    let lines = received.lines().collect::<Vec<_>>();
    assert_eq!(evidence.classifier, "uds_written");
    assert_eq!(
        serde_json::from_str::<Value>(lines[0]).unwrap(),
        json!({"token":"fixture-token","type":"auth"})
    );
    assert_eq!(
        serde_json::from_str::<Value>(lines[1]).unwrap()["message"]["content"],
        notice().render().unwrap()
    );
}

/// Mirrors `tests/test_claude_uds.py::ClaudeSessionSenderTests::test_malformed_descriptors_are_skipped_not_fatal`.
/// Mirrors `tests/test_claude_uds.py::ClaudeSessionSenderTests::test_missing_or_unreadable_auth_token_is_a_clean_refusal`.
/// Mirrors `tests/test_claude_uds.py::ClaudeSessionSenderTests::test_registry_miss_is_session_gone_not_ambiguous`.
/// Mirrors `tests/test_claude_uds.py::ClaudeSessionSenderTests::test_stale_descriptor_with_a_dead_socket_is_session_gone`.
/// Unix socket test: the stale path is a nonexistent temporary endpoint.
#[tokio::test]
async fn claude_uds_classifies_registry_and_auth_failures_without_host_contact() {
    let temporary = tempfile::tempdir().unwrap();
    let registry = temporary.path().join("sessions");
    std::fs::create_dir(&registry).unwrap();
    std::fs::write(registry.join("broken.json"), "not json").unwrap();
    assert_eq!(
        claude::send(&registry, "missing", &notice())
            .await
            .classifier,
        "uds_session_gone"
    );
    let dead = temporary.path().join("dead.sock");
    std::fs::write(
        registry.join("1.json"),
        json!({"sessionId":"missing-key","messagingSocketPath":dead,"pid":1}).to_string(),
    )
    .unwrap();
    assert_eq!(
        claude::send(&registry, "missing-key", &notice())
            .await
            .classifier,
        "uds_rejected"
    );
    claude_descriptor(&registry, "stale", &dead);
    assert_eq!(
        claude::send(&registry, "stale", &notice()).await.classifier,
        "uds_session_gone"
    );
}

/// Mirrors `tests/test_codex_desktop_relay.py::RelayClientTests::test_v2_advertised_endpoint_receives_the_exact_rich_payload`.
/// Mirrors `tests/test_codex_desktop_relay.py::RelayClientTests::test_v3_endpoint_receives_failure_category_without_error_prose`.
/// Mirrors `tests/test_codex_desktop_relay.py::RelayClientTests::test_v3_endpoints_are_preferred_over_v2_and_legacy`.
/// Unix socket test: the endpoint is a private temporary fake Desktop relay.
#[tokio::test]
async fn desktop_relay_prefers_v3_and_preserves_the_versioned_wire_contract() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    let temporary = tempfile::tempdir().unwrap();
    let v3 = temporary.path().join("ar-cdx-v3-fake.sock");
    let v2 = temporary.path().join("ar-cdx-v2-unused.sock");
    let listener = tokio::net::UnixListener::bind(&v3).unwrap();
    let _unused = tokio::net::UnixListener::bind(&v2).unwrap();
    let peer = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        let length = stream.read_u32_le().await.unwrap() as usize;
        let mut data = vec![0; length];
        stream.read_exact(&mut data).await.unwrap();
        stream.write_all(&(22_u32).to_le_bytes()).await.unwrap();
        stream
            .write_all(br#"{"outcome":"accepted"}"#)
            .await
            .unwrap();
        serde_json::from_slice::<Value>(&data).unwrap()
    });
    let mut rich = notice();
    rich.status = Status::Failed;
    rich.runtime = Some("codex".into());
    rich.model = Some("gpt-5".into());
    rich.effort = Some("high".into());
    rich.failure_kind = Some("prepare_failed".into());
    let evidence = relay::send(temporary.path(), "thread-test", &rich).await;
    let request = peer.await.unwrap();
    assert_eq!(evidence.classifier, "relay_accepted");
    assert_eq!(request["version"], 3);
    assert_eq!(request["failure_kind"], "prepare_failed");
    assert_eq!(request["runtime"], "codex");
}

/// Mirrors `tests/test_codex_desktop_relay.py::RelayClientTests::test_missing_relay_is_retryable`.
#[tokio::test]
async fn desktop_relay_missing_endpoint_is_retryable() {
    let root = tempfile::tempdir().unwrap();
    let evidence = relay::send(root.path(), "thread-test", &notice()).await;
    assert_eq!(evidence.classifier, "relay_unavailable");
}

/// Mirrors `tests/test_codex_desktop_relay.py::RelayClientTests::test_partial_send_is_ambiguous_and_stops_discovery`.
#[tokio::test]
async fn desktop_relay_partial_send_is_ambiguous_without_fallback() {
    use tokio::io::AsyncReadExt;
    let root = tempfile::tempdir().unwrap();
    let first = root.path().join("ar-cdx-v3-first.sock");
    let second = root.path().join("ar-cdx-v2-second.sock");
    let first_listener = tokio::net::UnixListener::bind(&first).unwrap();
    let second_listener = tokio::net::UnixListener::bind(&second).unwrap();
    let peer = tokio::spawn(async move {
        let (mut stream, _) = first_listener.accept().await.unwrap();
        let _ = stream.read_u32_le().await;
        drop(stream);
    });
    let evidence = relay::send(root.path(), "thread-test", &notice()).await;
    peer.await.unwrap();
    assert_eq!(evidence.classifier, "relay_ambiguous");
    assert!(
        tokio::time::timeout(Duration::from_millis(50), second_listener.accept())
            .await
            .is_err()
    );
}

/// Mirrors `tests/test_codex_desktop_relay.py::RelayClientTests::test_old_style_endpoint_receives_the_exact_legacy_six_keys`.
#[tokio::test]
async fn desktop_relay_legacy_endpoint_receives_exact_six_key_wire() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("ar-cdx-7.sock");
    let peer = fake_relay(&path, "accepted").await;
    let evidence = relay::send(root.path(), "thread-test", &rich_notice()).await;
    let request = peer.await.unwrap();
    assert_eq!(evidence.classifier, "relay_accepted");
    assert_eq!(
        request,
        json!({"version":1,"op":"completion","thread_id":"thread-test","notification_id":"ntf_rich","agent_id":"ag-20260825-120000-0123456789","status":"succeeded"})
    );
}

/// Mirrors `tests/test_codex_desktop_relay.py::RelayClientTests::test_v2_advertised_endpoint_receives_the_exact_rich_payload`.
#[tokio::test]
async fn desktop_relay_v2_endpoint_receives_exact_rich_wire() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("ar-cdx-v2-421.sock");
    let peer = fake_relay(&path, "accepted").await;
    let evidence = relay::send(root.path(), "thread-test", &rich_notice()).await;
    let request = peer.await.unwrap();
    assert_eq!(evidence.classifier, "relay_accepted");
    assert_eq!(request["version"], 2);
    assert_eq!(request["runtime"], "codex");
    assert_eq!(request["model"], "gpt-5.2-codex");
    assert_eq!(request["effort"], "high");
    assert_eq!(request.as_object().unwrap().len(), 9);
}

/// Mirrors `tests/test_codex_desktop_relay.py::RelayClientTests::test_v2_endpoints_are_preferred_during_discovery`.
#[tokio::test]
async fn desktop_relay_v2_endpoint_precedes_legacy_endpoint() {
    let root = tempfile::tempdir().unwrap();
    let v2 = root.path().join("ar-cdx-v2-2.sock");
    let legacy = root.path().join("ar-cdx-1.sock");
    let peer = fake_relay(&v2, "accepted").await;
    let legacy_listener = tokio::net::UnixListener::bind(&legacy).unwrap();
    assert_eq!(
        relay::send(root.path(), "thread-test", &rich_notice())
            .await
            .classifier,
        "relay_accepted"
    );
    assert_eq!(peer.await.unwrap()["version"], 2);
    assert!(
        tokio::time::timeout(Duration::from_millis(50), legacy_listener.accept())
            .await
            .is_err()
    );
}

/// Returns the exact v3 wire request used by the host behavior tests.
fn v3_request(notice: &Notice) -> Value {
    json!({"version":3,"op":"completion","thread_id":"thread-test","notification_id":notice.notification_id,"agent_id":notice.agent_id,"status":notice.status,"runtime":notice.runtime,"model":notice.model,"effort":notice.effort,"failure_kind":notice.failure_kind})
}

/// Mirrors `tests/test_codex_desktop_relay.py::NodeWrapperTests::test_large_inventory_and_exact_notice_are_accepted`.
#[tokio::test]
async fn desktop_relay_host_accepts_large_inventory() {
    let response = host_exchange(v3_request(&notice()), "true").await.0;
    assert_eq!(response, json!({"outcome":"accepted"}));
}

/// Mirrors `tests/test_codex_desktop_relay.py::NodeWrapperTests::test_rich_notice_is_rendered_by_real_node_byte_for_byte`.
#[tokio::test]
async fn desktop_relay_host_renders_rich_notice_exactly() {
    let rich = rich_notice();
    let (response, called) = host_exchange(v3_request(&rich), "true").await;
    assert_eq!(response, json!({"outcome":"accepted"}));
    assert_eq!(
        called.unwrap()["params"]["arguments"]["prompt"],
        rich.render().unwrap()
    );
}

/// Mirrors `tests/test_codex_desktop_relay.py::NodeWrapperTests::test_escaped_metadata_renders_identically_on_the_real_host`.
#[tokio::test]
async fn desktop_relay_host_escapes_metadata_without_changing_text() {
    let mut tricky = rich_notice();
    tricky.status = Status::Failed;
    tricky.runtime = Some("co\ndex".into());
    tricky.model = Some("claude-opus-5@anthropic/ss-1:1m".into());
    tricky.effort = Some("hi\u{2028}low\u{2029}end".into());
    tricky.failure_kind = Some("prepare_failed".into());
    let (response, called) = host_exchange(v3_request(&tricky), "true").await;
    assert_eq!(response, json!({"outcome":"accepted"}));
    assert_eq!(
        called.unwrap()["params"]["arguments"]["prompt"],
        tricky.render().unwrap()
    );
}

/// Mirrors `tests/test_codex_desktop_relay.py::NodeWrapperTests::test_legacy_wire_from_an_old_client_is_accepted_by_the_new_host`.
#[tokio::test]
async fn desktop_relay_host_accepts_legacy_wire() {
    let legacy = notice();
    let (response, called) = host_exchange(json!({"version":1,"op":"completion","thread_id":"thread-test","notification_id":legacy.notification_id,"agent_id":legacy.agent_id,"status":legacy.status}), "true").await;
    assert_eq!(response, json!({"outcome":"accepted"}));
    assert_eq!(
        called.unwrap()["params"]["arguments"]["prompt"],
        legacy.render().unwrap()
    );
}

/// Mirrors `tests/test_codex_desktop_relay.py::NodeWrapperTests::test_placeholder_metadata_is_literal_on_the_real_host`.
#[tokio::test]
async fn desktop_relay_host_keeps_placeholder_metadata_literal() {
    let mut literal = rich_notice();
    literal.model = Some("{agent_id}-$&-$1".into());
    literal.effort = Some("{version}".into());
    let (response, called) = host_exchange(v3_request(&literal), "true").await;
    assert_eq!(response, json!({"outcome":"accepted"}));
    assert_eq!(
        called.unwrap()["params"]["arguments"]["prompt"],
        literal.render().unwrap()
    );
}

/// Mirrors `tests/test_codex_desktop_relay.py::NodeWrapperTests::test_arbitrary_extra_keys_are_rejected_before_any_host_contact`.
#[tokio::test]
async fn desktop_relay_host_rejects_arbitrary_extra_keys_before_pipe_contact() {
    let exact = v3_request(&notice());
    for malformed in [
        json!({"version":3,"op":"completion","thread_id":"thread-test","notification_id":"ntf_test","agent_id":"ag-20260825-120000-0123456789","status":"succeeded","runtime":null,"model":null,"effort":null,"failure_kind":null,"prompt":"inject"}),
        json!({"version":3,"op":"completion","thread_id":"thread-test","notification_id":"ntf_test","agent_id":"ag-20260825-120000-0123456789","status":"succeeded","runtime":null,"model":null,"effort":null}),
        json!({"version":3,"op":"completion","thread_id":"thread-test","notification_id":"ntf_test","agent_id":"ag-20260825-120000-0123456789","status":"succeeded","runtime":null,"model":null,"effort":null,"failure_kind":null,"task":"inject"}),
    ] {
        let (response, called) = host_exchange(malformed, "malformed").await;
        assert_eq!(response, json!({"outcome":"rejected"}));
        assert!(called.is_none());
    }
    let (response, _) = host_exchange(exact, "malformed").await;
    assert_eq!(response, json!({"outcome":"rejected"}));
}

/// Mirrors `tests/test_codex_desktop_relay.py::NodeWrapperTests::test_slow_discovery_exceeds_the_former_deadline_and_succeeds`.
#[tokio::test]
async fn desktop_relay_host_allows_slow_discovery() {
    assert_eq!(
        host_exchange(v3_request(&notice()), "slow").await.0,
        json!({"outcome":"accepted"})
    );
}

/// Mirrors `tests/test_codex_desktop_relay.py::NodeWrapperTests::test_explicit_rejection_and_precall_failure_are_retryable`.
#[tokio::test]
async fn desktop_relay_host_classifies_pre_dispatch_failures_as_rejected() {
    for mode in ["false", "precall"] {
        assert_eq!(
            host_exchange(v3_request(&notice()), mode).await.0,
            json!({"outcome":"rejected"})
        );
    }
}

/// Mirrors `tests/test_codex_desktop_relay.py::NodeWrapperTests::test_postcall_uncertainty_never_allows_fallback`.
#[tokio::test]
async fn desktop_relay_host_classifies_post_dispatch_uncertainty_as_ambiguous() {
    for mode in ["drop", "badid", "error"] {
        assert_eq!(
            host_exchange(v3_request(&notice()), mode).await.0,
            json!({"outcome":"ambiguous"})
        );
    }
}

/// Mirrors `tests/test_codex_desktop_relay.py::ExecWrapperTests::test_missing_capability_does_not_exec`.
#[tokio::test]
async fn desktop_relay_host_is_not_started_without_both_capabilities() {
    let _guard = relay_env_lock().lock().await;
    // SAFETY: this test owns and serializes the process-wide relay environment.
    unsafe {
        std::env::remove_var("CODEX_APP_TOOLS_PIPE_PATH");
        std::env::remove_var("CODEX_MCP_NODE_PATH");
    }
    let root = tempfile::tempdir().unwrap();
    assert!(relay::host(root.path()).unwrap().is_none());
}

/// Mirrors `tests/test_codex_desktop_relay.py::ExecWrapperTests::test_exec_uses_exact_node_and_python_child`.
#[tokio::test]
async fn desktop_relay_host_uses_the_absolute_configured_node_transport() {
    let _guard = relay_env_lock().lock().await;
    let root = tempfile::tempdir().unwrap();
    let pipe = root.path().join("host.sock");
    let node = [
        "/usr/local/bin/node",
        "/opt/homebrew/bin/node",
        "/usr/bin/node",
    ]
    .iter()
    .map(PathBuf::from)
    .find(|path| path.is_file())
    .expect("Node is required for relay host tests");
    // SAFETY: this test owns and serializes the process-wide relay environment.
    unsafe {
        std::env::set_var("CODEX_APP_TOOLS_PIPE_PATH", &pipe);
        std::env::set_var("CODEX_MCP_NODE_PATH", &node);
    }
    let host = relay::host(root.path())
        .unwrap()
        .expect("configured relay host");
    drop(host);
    // SAFETY: this test owns and serializes the process-wide relay environment.
    unsafe {
        std::env::remove_var("CODEX_APP_TOOLS_PIPE_PATH");
        std::env::remove_var("CODEX_MCP_NODE_PATH");
    }
}

/// Mirrors `tests/test_delivery_dispatch.py::test_attempt_evidence_follows_retry_and_success_transactions`.
/// Mirrors `tests/test_delivery_dispatch.py::test_bound_deliveries_are_never_expired_by_the_sweep`.
/// Mirrors `tests/test_delivery_dispatch.py::test_corrupt_request_json_still_delivers_with_unspecified_effort`.
/// Mirrors `tests/test_delivery_dispatch.py::test_delivered_notice_carries_launch_metadata_from_the_row`.
/// Mirrors `tests/test_delivery_dispatch.py::test_pending_notice_is_delivered_once_and_the_outbox_then_rests`.
/// Unix socket test: the fake relay is private to this temporary home.
#[tokio::test]
async fn dispatch_records_retry_and_success_evidence_for_one_bound_notice() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    let home = common::Home::new();
    delivery(&home.path, "ntf_once", "codex_queue", "pending");
    let connection = Connection::open(home.path.join("state.db")).unwrap();
    connection
        .execute(
            "UPDATE agents SET runtime='codex',model='gpt-5',request_json='{broken'",
            [],
        )
        .unwrap();
    drop(connection);
    assert_eq!(dispatch_once(&home.path).await.unwrap(), 1);
    let socket = home.path.join("ar-cdx-v3-accepted.sock");
    let listener = tokio::net::UnixListener::bind(socket).unwrap();
    let peer = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        let length = stream.read_u32_le().await.unwrap() as usize;
        let mut data = vec![0; length];
        stream.read_exact(&mut data).await.unwrap();
        stream.write_all(&(22_u32).to_le_bytes()).await.unwrap();
        stream
            .write_all(br#"{"outcome":"accepted"}"#)
            .await
            .unwrap();
        serde_json::from_slice::<Value>(&data).unwrap()
    });
    let connection = Connection::open(home.path.join("state.db")).unwrap();
    connection
        .execute(
            "UPDATE deliveries SET next_attempt_at=? WHERE id='ntf_once'",
            [now() - 1.0],
        )
        .unwrap();
    drop(connection);
    assert_eq!(dispatch_once(&home.path).await.unwrap(), 1);
    let request = peer.await.unwrap();
    let connection = Connection::open(home.path.join("state.db")).unwrap();
    let row: (String, i64, Option<f64>) = connection
        .query_row(
            "SELECT state,attempts,next_attempt_at FROM deliveries WHERE id='ntf_once'",
            [],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .unwrap();
    let evidence: i64 = connection
        .query_row(
            "SELECT COUNT(*) FROM delivery_attempt_evidence WHERE delivery_id='ntf_once'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(row, ("delivered".into(), 2, None));
    assert_eq!(evidence, 2);
    assert_eq!(request["runtime"], "codex");
    assert_eq!(request["model"], "gpt-5");
    assert!(request["effort"].is_null());
    assert_eq!(dispatch_once(&home.path).await.unwrap(), 0);
}
