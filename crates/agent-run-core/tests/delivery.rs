//! Completion delivery parity fixtures for Python's delivery dispatcher and notice contract.

use agent_run_core::{
    delivery::{dispatch_once, safe_evidence, Notice},
    domain::{now, AgentId},
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
