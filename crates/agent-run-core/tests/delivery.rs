//! Completion delivery parity fixtures for Python's delivery dispatcher and notice contract.

use agent_run_core::{
    delivery::{
        claude, dispatch, dispatch_once, dispatch_with_batch, relay, safe_evidence, Notice,
    },
    domain::{now, AgentId, Status},
};
use rusqlite::{params, Connection};
use serde_json::{json, Value};
use std::path::{Path, PathBuf};

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

/// Rewrites only delivery policy values in the fixture configuration.
fn delivery_config(home: &Path, base: f64, cap: f64, max_attempts: u32) {
    let path = home.join("config.toml");
    let mut config = std::fs::read_to_string(&path).unwrap();
    config.push_str(&format!(
        "\n[delivery]\nretry_base_seconds={base}\nretry_cap_seconds={cap}\nmax_attempts={max_attempts}\n"
    ));
    std::fs::write(path, config).unwrap();
}

/// Makes one pending delivery due immediately after an earlier attempt.
fn make_due(home: &Path, id: &str) {
    Connection::open(home.join("state.db"))
        .unwrap()
        .execute(
            "UPDATE deliveries SET next_attempt_at=? WHERE id=?",
            params![now() - 1.0, id],
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
    let result = dispatch_with_batch(&home.path, 1).await.unwrap();
    assert_eq!((result.claimed, result.ambiguous), (1, 1));
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
/// Mirrors `tests/test_claude_uds.py::ClaudeUdsTransportTests::test_missing_session_fails_and_no_replacement_is_started`.
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

/// Mirrors `tests/test_claude_uds.py::ClaudeUdsTransportTests::test_unreachable_socket_and_foreign_target_are_delivery_errors`.
/// Unix socket test: a non-socket endpoint exercises the unavailable delivery class.
#[tokio::test]
async fn claude_uds_classifies_unreachable_endpoint_as_unavailable() {
    let temporary = tempfile::tempdir().unwrap();
    let registry = temporary.path().join("sessions");
    std::fs::create_dir(&registry).unwrap();
    let endpoint = temporary.path().join("endpoint-directory");
    std::fs::create_dir(&endpoint).unwrap();
    claude_descriptor(&registry, "unavailable", &endpoint);
    assert_eq!(
        claude::send(&registry, "unavailable", &notice())
            .await
            .classifier,
        "uds_unavailable"
    );
}

/// Mirrors `tests/test_claude_uds.py::ClaudeUdsTransportTests::test_validate_and_arguments_are_checked`.
#[tokio::test]
async fn claude_uds_rejects_unbounded_arguments_without_socket_contact() {
    let temporary = tempfile::tempdir().unwrap();
    let registry = temporary.path().join("sessions");
    std::fs::create_dir(&registry).unwrap();
    assert_eq!(
        claude::send(&registry, "", &notice()).await.classifier,
        "uds_rejected"
    );
    assert_eq!(
        claude::send(&registry, &"x".repeat(513), &notice())
            .await
            .classifier,
        "uds_rejected"
    );
    let mut invalid = notice();
    invalid.runtime = Some(" ".into());
    assert_eq!(
        claude::send(&registry, "session", &invalid)
            .await
            .classifier,
        "uds_rejected"
    );
}

/// Mirrors `tests/test_claude_uds.py::ClaudeSessionSenderTests::test_a_write_that_never_completes_becomes_an_ambiguous_delivery`.
/// Mirrors `tests/test_claude_uds.py::ClaudeUdsTransportTests::test_ambiguous_timeout_is_at_least_once_not_a_failure`.
/// Unix socket test: the peer closes before the unauthenticated write can be acknowledged.
#[tokio::test]
async fn claude_uds_peer_close_is_ambiguous() {
    use std::os::fd::AsRawFd;

    let temporary = tempfile::tempdir().unwrap();
    let registry = temporary.path().join("sessions");
    std::fs::create_dir(&registry).unwrap();
    let socket = temporary.path().join("inbox.sock");
    claude_descriptor(&registry, "ambiguous", &socket);
    let listener = tokio::net::UnixListener::bind(&socket).unwrap();
    let peer = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let linger = libc::linger {
            l_onoff: 1,
            l_linger: 0,
        };
        // SAFETY: the accepted stream owns this valid socket descriptor until drop.
        let result = unsafe {
            libc::setsockopt(
                stream.as_raw_fd(),
                libc::SOL_SOCKET,
                libc::SO_LINGER,
                (&linger as *const libc::linger).cast(),
                std::mem::size_of::<libc::linger>() as libc::socklen_t,
            )
        };
        assert_eq!(result, 0);
        drop(stream);
    });
    let send = tokio::spawn({
        let registry = registry.clone();
        async move { claude::send(&registry, "ambiguous", &notice()).await }
    });
    let evidence = send.await.unwrap();
    peer.await.unwrap();
    assert_eq!(evidence.classifier, "uds_ambiguous");
    assert_eq!(evidence.error_class.as_deref(), Some("ambiguous"));
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

/// Mirrors `tests/test_codex_queue.py::test_send_uses_the_relay_without_a_queue_message_id`.
#[tokio::test]
async fn relay_acceptance_has_no_queue_message_identifier() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    let home = common::Home::new();
    let socket = home.path.join("ar-cdx-v3-accepted.sock");
    let listener = tokio::net::UnixListener::bind(&socket).unwrap();
    let peer = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        let length = stream.read_u32_le().await.unwrap() as usize;
        let mut request = vec![0; length];
        stream.read_exact(&mut request).await.unwrap();
        let reply = br#"{"outcome":"accepted"}"#;
        stream.write_u32_le(reply.len() as u32).await.unwrap();
        stream.write_all(reply).await.unwrap();
    });
    let evidence = relay::send(&home.path, "session-1", &notice()).await;
    peer.await.unwrap();
    assert_eq!(evidence.classifier, "relay_accepted");
    assert!(evidence.message_id_present);
}

/// Mirrors `tests/test_codex_queue.py::test_ambiguous_timeout_is_at_least_once_not_a_failure`.
#[tokio::test]
async fn relay_ambiguity_is_classified_as_at_least_once() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    let home = common::Home::new();
    let socket = home.path.join("ar-cdx-v3-ambiguous.sock");
    let listener = tokio::net::UnixListener::bind(&socket).unwrap();
    let peer = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        let length = stream.read_u32_le().await.unwrap() as usize;
        let mut request = vec![0; length];
        stream.read_exact(&mut request).await.unwrap();
        let reply = br#"{"outcome":"ambiguous"}"#;
        stream.write_u32_le(reply.len() as u32).await.unwrap();
        stream.write_all(reply).await.unwrap();
    });
    let evidence = relay::send(&home.path, "session-1", &notice()).await;
    peer.await.unwrap();
    assert_eq!(evidence.classifier, "relay_ambiguous");
}

/// Mirrors `tests/test_codex_queue.py::test_missing_session_fails_and_no_replacement_is_started`.
#[tokio::test]
async fn missing_relay_does_not_create_a_replacement_agent() {
    let home = common::Home::new();
    let before: i64 = Connection::open(home.path.join("state.db"))
        .unwrap()
        .query_row("SELECT COUNT(*) FROM agents", [], |row| row.get(0))
        .unwrap();
    let evidence = relay::send(&home.path, "gone", &notice()).await;
    let after: i64 = Connection::open(home.path.join("state.db"))
        .unwrap()
        .query_row("SELECT COUNT(*) FROM agents", [], |row| row.get(0))
        .unwrap();
    assert_eq!(evidence.classifier, "relay_unavailable");
    assert_eq!(before, after);
}

/// Mirrors `tests/test_codex_queue.py::test_relay_acceptance_bypasses_the_queue_without_a_remote_id`.
#[tokio::test]
async fn relay_acceptance_has_no_second_queue_path() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    let home = common::Home::new();
    let socket = home.path.join("ar-cdx-v3-only.sock");
    let listener = tokio::net::UnixListener::bind(&socket).unwrap();
    let peer = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        let length = stream.read_u32_le().await.unwrap() as usize;
        let mut request = vec![0; length];
        stream.read_exact(&mut request).await.unwrap();
        let reply = br#"{"outcome":"accepted"}"#;
        stream.write_u32_le(reply.len() as u32).await.unwrap();
        stream.write_all(reply).await.unwrap();
    });
    let evidence = relay::send(&home.path, "session-1", &notice()).await;
    peer.await.unwrap();
    assert_eq!(evidence.classifier, "relay_accepted");
    assert_eq!(evidence.executable, "desktop-relay");
}

/// Mirrors `tests/test_codex_queue.py::test_relay_rejection_never_falls_back_to_the_queue`.
#[tokio::test]
async fn relay_rejection_does_not_fall_back_to_a_queue() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    let home = common::Home::new();
    let socket = home.path.join("ar-cdx-v3-rejected.sock");
    let listener = tokio::net::UnixListener::bind(&socket).unwrap();
    let peer = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        let length = stream.read_u32_le().await.unwrap() as usize;
        let mut request = vec![0; length];
        stream.read_exact(&mut request).await.unwrap();
        let reply = br#"{"outcome":"rejected"}"#;
        stream.write_u32_le(reply.len() as u32).await.unwrap();
        stream.write_all(reply).await.unwrap();
    });
    let evidence = relay::send(&home.path, "session-1", &notice()).await;
    peer.await.unwrap();
    assert_eq!(evidence.classifier, "relay_rejected");
    assert!(!evidence.message_id_present);
}

/// Mirrors `tests/test_codex_queue.py::test_relay_ambiguity_never_falls_back_to_the_queue`.
#[tokio::test]
async fn relay_ambiguity_stops_discovery_without_a_second_path() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    let home = common::Home::new();
    let first = home.path.join("ar-cdx-v3-ambiguous.sock");
    let second = home.path.join("ar-cdx-v2-unused.sock");
    let first_listener = tokio::net::UnixListener::bind(&first).unwrap();
    let second_listener = tokio::net::UnixListener::bind(&second).unwrap();
    let first_peer = tokio::spawn(async move {
        let (mut stream, _) = first_listener.accept().await.unwrap();
        let length = stream.read_u32_le().await.unwrap() as usize;
        let mut request = vec![0; length];
        stream.read_exact(&mut request).await.unwrap();
        let reply = br#"{"unexpected":true}"#;
        stream.write_u32_le(reply.len() as u32).await.unwrap();
        stream.write_all(reply).await.unwrap();
    });
    let second_peer = tokio::spawn(async move {
        tokio::time::timeout(
            std::time::Duration::from_millis(50),
            second_listener.accept(),
        )
        .await
        .is_ok()
    });
    let evidence = relay::send(&home.path, "session-1", &notice()).await;
    first_peer.await.unwrap();
    assert!(!second_peer.await.unwrap());
    assert_eq!(evidence.classifier, "relay_ambiguous");
}

/// Mirrors `tests/test_codex_queue.py::test_unavailable_relay_and_foreign_target_are_delivery_errors`.
#[tokio::test]
async fn unavailable_relay_and_unknown_transport_are_delivery_failures() {
    let home = common::Home::new();
    assert_eq!(
        relay::send(&home.path, "session-1", &notice())
            .await
            .classifier,
        "relay_unavailable"
    );
    delivery(&home.path, "ntf_foreign", "slack", "pending");
    assert_eq!(dispatch_once(&home.path).await.unwrap(), 1);
    let state: String = Connection::open(home.path.join("state.db"))
        .unwrap()
        .query_row(
            "SELECT state FROM deliveries WHERE id='ntf_foreign'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(state, "failed");
}

/// Mirrors `tests/test_codex_queue.py::test_validate_and_arguments_are_checked`.
#[test]
fn delivery_configuration_rejects_invalid_retry_arguments() {
    let home = common::Home::new();
    let path = home.path.join("config.toml");
    let original = std::fs::read_to_string(&path).unwrap();
    for delivery in [
        "[delivery]\nretry_base_seconds=0\nretry_cap_seconds=1\nmax_attempts=0\n",
        "[delivery]\nretry_base_seconds=2\nretry_cap_seconds=1\nmax_attempts=0\n",
    ] {
        std::fs::write(&path, format!("{original}\n{delivery}")).unwrap();
        assert!(agent_run_config::config::Config::load(&home.path).is_err());
    }
}

/// Mirrors `tests/test_delivery_dispatch.py::test_cancel_during_send_is_terminal_and_does_not_retry`.
#[tokio::test]
async fn cancellation_during_send_wins_over_a_late_acknowledgement() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    let home = common::Home::new();
    delivery(&home.path, "ntf_cancel_send", "codex_queue", "pending");
    let socket = home.path.join("ar-cdx-v3-cancel.sock");
    let listener = tokio::net::UnixListener::bind(&socket).unwrap();
    let cancel_home = home.path.clone();
    let peer = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        let length = stream.read_u32_le().await.unwrap() as usize;
        let mut request = vec![0; length];
        stream.read_exact(&mut request).await.unwrap();
        let mut store = agent_run_store::Store::open(&cancel_home).unwrap();
        assert!(store.cancel_delivery("ntf_cancel_send").unwrap());
        let reply = br#"{"outcome":"accepted"}"#;
        stream.write_u32_le(reply.len() as u32).await.unwrap();
        stream.write_all(reply).await.unwrap();
    });
    assert_eq!(dispatch_once(&home.path).await.unwrap(), 1);
    peer.await.unwrap();
    let state: String = Connection::open(home.path.join("state.db"))
        .unwrap()
        .query_row(
            "SELECT state FROM deliveries WHERE id='ntf_cancel_send'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(state, "cancelled");
}

/// Mirrors `tests/test_delivery_dispatch.py::test_cancelled_delivery_stops_retries_and_preserves_the_result`.
#[tokio::test]
async fn cancelled_delivery_is_not_claimed_again() {
    let home = common::Home::new();
    delivery(&home.path, "ntf_cancelled", "codex_queue", "pending");
    let mut store = agent_run_store::Store::open(&home.path).unwrap();
    assert!(store.cancel_delivery("ntf_cancelled").unwrap());
    drop(store);
    assert_eq!(dispatch_once(&home.path).await.unwrap(), 0);
    let state: String = Connection::open(home.path.join("state.db"))
        .unwrap()
        .query_row(
            "SELECT state FROM deliveries WHERE id='ntf_cancelled'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(state, "cancelled");
}

/// Mirrors `tests/test_delivery_dispatch.py::test_exhausted_attempt_budget_fails_the_delivery`.
#[tokio::test]
async fn exhausted_attempt_budget_fails_without_a_third_retry() {
    let home = common::Home::new();
    delivery_config(&home.path, 1.0, 1.0, 2);
    delivery(&home.path, "ntf_exhausted", "codex_queue", "pending");
    assert_eq!(dispatch_once(&home.path).await.unwrap(), 1);
    make_due(&home.path, "ntf_exhausted");
    assert_eq!(dispatch_once(&home.path).await.unwrap(), 1);
    let connection = Connection::open(home.path.join("state.db")).unwrap();
    let row: (String, u32, Option<f64>) = connection
        .query_row(
            "SELECT state,attempts,next_attempt_at FROM deliveries WHERE id='ntf_exhausted'",
            [],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .unwrap();
    assert_eq!(row, ("failed".into(), 2, None));
    assert_eq!(dispatch_once(&home.path).await.unwrap(), 0);
}

/// Mirrors `tests/test_delivery_dispatch.py::test_never_bound_notice_for_a_running_agent_never_expires`.
#[tokio::test]
async fn running_unbound_delivery_never_expires() {
    let home = common::Home::new();
    let connection = Connection::open(home.path.join("state.db")).unwrap();
    let agent = "ag-20260825-120000-0123456789";
    connection.execute("INSERT INTO agents(id,runtime,model,profile,task,task_summary,workdir,request_json,status,created_at,timeout_seconds,config_revision,root_agent_id) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)", params![agent,"mock","fixture","review","task","summary",home.path.to_string_lossy(),"{}","running",now()-7200.0,1.0,"fixture",agent]).unwrap();
    connection
        .execute(
            "INSERT INTO events(agent_id,at,kind,data_json) VALUES(?,?,?,?)",
            params![agent, now() - 7200.0, "status", "{}"],
        )
        .unwrap();
    let event = connection.last_insert_rowid();
    connection.execute("INSERT INTO deliveries(id,agent_id,terminal_event_seq,state) VALUES(?,?,?,'waiting_binding')", params!["ntf_running",agent,event]).unwrap();
    drop(connection);
    assert_eq!(dispatch_once(&home.path).await.unwrap(), 0);
    let state: String = Connection::open(home.path.join("state.db"))
        .unwrap()
        .query_row(
            "SELECT state FROM deliveries WHERE id='ntf_running'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(state, "waiting_binding");
}

/// Mirrors `tests/test_delivery_dispatch.py::test_never_bound_notice_inside_the_window_is_left_to_bind`.
#[tokio::test]
async fn recent_unbound_terminal_delivery_waits_for_binding() {
    let home = common::Home::new();
    let connection = Connection::open(home.path.join("state.db")).unwrap();
    let agent = "ag-20260825-120000-0123456789";
    connection.execute("INSERT INTO agents(id,runtime,model,profile,task,task_summary,workdir,request_json,status,created_at,timeout_seconds,config_revision,root_agent_id) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)", params![agent,"mock","fixture","review","task","summary",home.path.to_string_lossy(),"{}","succeeded",now()-3599.0,1.0,"fixture",agent]).unwrap();
    connection
        .execute(
            "INSERT INTO events(agent_id,at,kind,data_json) VALUES(?,?,?,?)",
            params![agent, now() - 3599.0, "status", "{}"],
        )
        .unwrap();
    let event = connection.last_insert_rowid();
    connection.execute("INSERT INTO deliveries(id,agent_id,terminal_event_seq,state) VALUES(?,?,?,'waiting_binding')", params!["ntf_recent",agent,event]).unwrap();
    drop(connection);
    assert_eq!(dispatch_once(&home.path).await.unwrap(), 0);
    let state: String = Connection::open(home.path.join("state.db"))
        .unwrap()
        .query_row(
            "SELECT state FROM deliveries WHERE id='ntf_recent'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(state, "waiting_binding");
}

/// Mirrors `tests/test_delivery_dispatch.py::test_trigger_drains_bounded_backlog_and_overflow_is_recoverable`.
#[tokio::test]
async fn bounded_dispatch_drains_backlog_and_leaves_overflow_due() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    let home = common::Home::new();
    delivery(&home.path, "ntf_batch_1", "codex_queue", "pending");
    let connection = Connection::open(home.path.join("state.db")).unwrap();
    for index in 2..=3 {
        let session = format!("sess{index}");
        let id = format!("ntf_batch_{index}");
        connection
            .execute(
                "INSERT INTO orchestrator_sessions(id,transport,external_session_id,created_at,last_seen_at) VALUES(?,?,?,?,?)",
                params![session, "codex_queue", format!("thread-{index}"), now(), now()],
            )
            .unwrap();
        connection
            .execute(
                "INSERT INTO deliveries(id,agent_id,orchestrator_session_id,state,next_attempt_at) VALUES(?,?,?,'pending',?)",
                params![id, "ag-20260825-120000-0123456789", session, now() - 1.0],
            )
            .unwrap();
    }
    let listener = tokio::net::UnixListener::bind(home.path.join("ar-cdx-v3-batch.sock")).unwrap();
    let peer = tokio::spawn(async move {
        for _ in 0..3 {
            let (mut stream, _) = listener.accept().await.unwrap();
            let length = stream.read_u32_le().await.unwrap() as usize;
            let mut request = vec![0; length];
            stream.read_exact(&mut request).await.unwrap();
            let reply = br#"{"outcome":"accepted"}"#;
            stream.write_u32_le(reply.len() as u32).await.unwrap();
            stream.write_all(reply).await.unwrap();
        }
    });
    let first = dispatch_with_batch(&home.path, 2).await.unwrap();
    assert_eq!((first.claimed, first.delivered), (2, 2));
    let second = dispatch(&home.path).await.unwrap();
    assert_eq!((second.claimed, second.delivered), (1, 1));
    peer.await.unwrap();
}

/// Mirrors `tests/test_delivery_dispatch.py::test_only_one_dispatcher_runs_at_a_time`.
#[tokio::test]
async fn concurrent_dispatchers_have_one_nonblocking_owner() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::sync::oneshot;

    let home = common::Home::new();
    delivery(&home.path, "ntf_lock", "codex_queue", "pending");
    let listener = tokio::net::UnixListener::bind(home.path.join("ar-cdx-v3-lock.sock")).unwrap();
    let (ready_tx, ready_rx) = oneshot::channel();
    let (release_tx, release_rx) = oneshot::channel();
    let peer = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        let length = stream.read_u32_le().await.unwrap() as usize;
        let mut request = vec![0; length];
        stream.read_exact(&mut request).await.unwrap();
        ready_tx.send(()).unwrap();
        release_rx.await.unwrap();
        let reply = br#"{"outcome":"accepted"}"#;
        stream.write_u32_le(reply.len() as u32).await.unwrap();
        stream.write_all(reply).await.unwrap();
    });
    let first_home = home.path.clone();
    let first_task = tokio::spawn(async move { dispatch_with_batch(&first_home, 1).await });
    ready_rx.await.unwrap();
    let blocked = dispatch_with_batch(&home.path, 1).await.unwrap();
    assert!(blocked.locked_out);
    release_tx.send(()).unwrap();
    let first = first_task.await.unwrap().unwrap();
    assert_eq!((first.claimed, first.delivered), (1, 1));
    peer.await.unwrap();
}

/// Mirrors `tests/test_delivery_dispatch.py::test_notice_for_degrades_malformed_effort_to_unspecified`.
#[tokio::test]
async fn malformed_effort_values_degrade_to_unspecified_without_blocking_delivery() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    let home = common::Home::new();
    delivery(&home.path, "ntf_effort", "codex_queue", "pending");
    let malformed = vec![
        "not json".to_owned(),
        String::new(),
        "null".to_owned(),
        "[\"x\"]".to_owned(),
        "{\"effort\":12}".to_owned(),
        "{\"effort\":\"  \"}".to_owned(),
        format!("{{\"effort\":\"{}\"}}", "x".repeat(200)),
        "{\"effort\":\"medium\"}".to_owned(),
    ];
    let effort_count = malformed.len();
    let listener = tokio::net::UnixListener::bind(home.path.join("ar-cdx-v3-effort.sock")).unwrap();
    let peer = tokio::spawn(async move {
        let mut efforts = Vec::new();
        for _ in 0..effort_count {
            let (mut stream, _) = listener.accept().await.unwrap();
            let length = stream.read_u32_le().await.unwrap() as usize;
            let mut request = vec![0; length];
            stream.read_exact(&mut request).await.unwrap();
            let value: Value = serde_json::from_slice(&request).unwrap();
            efforts.push(value.get("effort").cloned().unwrap_or(Value::Null));
            let reply = br#"{"outcome":"accepted"}"#;
            stream.write_u32_le(reply.len() as u32).await.unwrap();
            stream.write_all(reply).await.unwrap();
        }
        efforts
    });
    for raw in malformed {
        Connection::open(home.path.join("state.db"))
            .unwrap()
            .execute(
                "UPDATE agents SET request_json=? WHERE id=?",
                params![raw, "ag-20260825-120000-0123456789"],
            )
            .unwrap();
        if raw != "not json" {
            make_due(&home.path, "ntf_effort");
            Connection::open(home.path.join("state.db"))
                .unwrap()
                .execute(
                    "UPDATE deliveries SET state='pending' WHERE id='ntf_effort'",
                    [],
                )
                .unwrap();
        }
        assert_eq!(dispatch_once(&home.path).await.unwrap(), 1);
    }
    let efforts = peer.await.unwrap();
    assert!(efforts[..7].iter().all(Value::is_null));
    assert_eq!(efforts[7], json!("medium"));
}

/// Mirrors `tests/test_delivery_dispatch.py::test_send_longer_than_lease_is_ambiguous_and_reclaimable`.
#[tokio::test]
async fn expired_send_lease_is_claim_lost_and_reclaimable() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    let home = common::Home::new();
    delivery(&home.path, "ntf_lease", "codex_queue", "pending");
    let first = tokio::net::UnixListener::bind(home.path.join("ar-cdx-v3-lease.sock")).unwrap();
    let first_home = home.path.clone();
    let first_peer = tokio::spawn(async move {
        let (mut stream, _) = first.accept().await.unwrap();
        let length = stream.read_u32_le().await.unwrap() as usize;
        let mut request = vec![0; length];
        stream.read_exact(&mut request).await.unwrap();
        Connection::open(first_home.join("state.db"))
            .unwrap()
            .execute(
                "UPDATE deliveries SET lease_until=? WHERE id='ntf_lease'",
                [now() - 1.0],
            )
            .unwrap();
        let reply = br#"{"outcome":"accepted"}"#;
        stream.write_u32_le(reply.len() as u32).await.unwrap();
        stream.write_all(reply).await.unwrap();
    });
    assert_eq!(dispatch_once(&home.path).await.unwrap(), 1);
    first_peer.await.unwrap();
    let state: String = Connection::open(home.path.join("state.db"))
        .unwrap()
        .query_row(
            "SELECT state FROM deliveries WHERE id='ntf_lease'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(state, "sending");
    std::fs::remove_file(home.path.join("ar-cdx-v3-lease.sock")).unwrap();
    let second = tokio::net::UnixListener::bind(home.path.join("ar-cdx-v3-lease.sock")).unwrap();
    let second_peer = tokio::spawn(async move {
        let (mut stream, _) = second.accept().await.unwrap();
        let length = stream.read_u32_le().await.unwrap() as usize;
        let mut request = vec![0; length];
        stream.read_exact(&mut request).await.unwrap();
        let reply = br#"{"outcome":"accepted"}"#;
        stream.write_u32_le(reply.len() as u32).await.unwrap();
        stream.write_all(reply).await.unwrap();
    });
    assert_eq!(dispatch_once(&home.path).await.unwrap(), 1);
    second_peer.await.unwrap();
    let state: String = Connection::open(home.path.join("state.db"))
        .unwrap()
        .query_row(
            "SELECT state FROM deliveries WHERE id='ntf_lease'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(state, "delivered");
}
