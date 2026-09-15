use agent_run::{
    delivery::Notice,
    dispatch,
    domain::AgentId,
    domain::Status,
    service::{Query, Service},
    transport::{frame, socket},
};
use serde_json::json;
use tokio::io::BufReader;
fn service() -> Service {
    Service::new(std::path::PathBuf::from(
        "/nonexistent-agent-run-protocol-fixture",
    ))
}
#[test]
fn packaged_table_has_exactly_the_shared_eleven_tools() {
    let tools = dispatch::tools();
    assert_eq!(tools.len(), 11);
    let names: std::collections::BTreeSet<_> =
        tools.iter().map(|v| v["name"].as_str().unwrap()).collect();
    assert_eq!(
        names,
        agent_run_domain::registry()
            .iter()
            .map(|tool| tool.name.as_str())
            .collect()
    );
    for tool in tools {
        assert_eq!(tool["inputSchema"]["additionalProperties"], false);
    }
}
#[tokio::test]
async fn ping_and_discovery_do_not_require_database_access() {
    let response = socket::respond(&service(), json!({"jsonrpc":"2.0","id":1,"method":"ping"}))
        .await
        .unwrap();
    assert_eq!(response["result"]["ok"], true);
    let response = socket::respond(
        &service(),
        json!({"jsonrpc":"2.0","id":"tools","method":"tools"}),
    )
    .await
    .unwrap();
    assert_eq!(response["result"].as_array().unwrap().len(), 11);
}
#[tokio::test]
async fn invalid_envelopes_and_unknown_method_get_standard_codes() {
    let service = service();
    for value in [
        json!([]),
        json!({"jsonrpc":"1.0","id":1,"method":"ping"}),
        json!({"jsonrpc":"2.0","id":true,"method":"ping"}),
    ] {
        assert_eq!(
            socket::respond(&service, value).await.unwrap()["error"]["code"],
            -32600
        );
    }
    assert_eq!(
        socket::respond(
            &service,
            json!({"jsonrpc":"2.0","id":1,"method":"not_a_tool"})
        )
        .await
        .unwrap()["error"]["code"],
        -32601
    );
}
/// Mirrors Python `test_api_socket.py::test_invalid_request_error_messages`.
#[tokio::test]
async fn invalid_requests_preserve_python_error_classes() {
    let service = service();
    let batch = socket::respond(&service, json!([])).await.unwrap();
    assert_eq!(
        batch["error"]["message"],
        "batch requests are not supported"
    );
    let bad_id = socket::respond(&service, json!({"jsonrpc":"2.0","id":true,"method":"ping"}))
        .await
        .unwrap();
    assert_eq!(bad_id["error"]["message"], "invalid request id");
    let params = socket::respond(
        &service,
        json!({"jsonrpc":"2.0","id":1,"method":"ping","params":[]}),
    )
    .await
    .unwrap();
    assert_eq!(params["error"]["code"], -32602);
    assert_eq!(params["error"]["message"], "params must be an object");
}
#[tokio::test]
async fn notifications_have_no_response_and_unknown_arguments_fail() {
    assert!(
        socket::respond(&service(), json!({"jsonrpc":"2.0","method":"ping"}))
            .await
            .is_none()
    );
    assert_eq!(
        socket::respond(
            &service(),
            json!({"jsonrpc":"2.0","id":null,"method":"ping","params":{"bad":1}})
        )
        .await
        .unwrap()["error"]["code"],
        -32602
    );
}

/// Mirrors Python `test_api_socket.py::test_domain_errors_have_code_and_message`.
#[tokio::test]
async fn domain_errors_use_the_socket_domain_envelope() {
    let response = socket::respond(
        &service(),
        json!({"jsonrpc":"2.0","id":1,"method":"answer","params":{"agent_id":"bad"}}),
    )
    .await
    .unwrap();
    assert_eq!(response["error"]["code"], -32602);
    assert!(response["error"].get("data").is_none());
}
#[tokio::test]
async fn framing_rejects_partial_and_oversized_lines() {
    let mut partial = BufReader::new(&b"{\"id\":1}"[..]);
    assert!(frame::read(&mut partial, 100).await.is_err());
    let mut large = BufReader::new(&b"123456789\n"[..]);
    assert!(frame::read(&mut large, 5).await.is_err());
    let mut crlf = BufReader::new(&b"{}\r\n"[..]);
    assert_eq!(
        frame::read(&mut crlf, 10).await.unwrap(),
        Some(b"{}".to_vec())
    );
    assert!(frame::read(&mut crlf, 10).await.unwrap().is_none());
}
#[tokio::test]
async fn framing_does_not_consume_the_next_message() {
    let mut input = BufReader::new(&b"one\ntwo\n"[..]);
    assert_eq!(frame::read(&mut input, 10).await.unwrap().unwrap(), b"one");
    assert_eq!(frame::read(&mut input, 10).await.unwrap().unwrap(), b"two");
}
#[test]
fn list_query_bounds_reject_nonfinite_or_negative_waits() {
    let mut q = Query::default();
    q.wait_seconds = f64::NAN;
    assert!(q.validate().is_err());
    q.wait_seconds = -1.0;
    assert!(q.validate().is_err());
    q.wait_seconds = 0.0;
    q.limit = 1001;
    assert!(q.validate().is_err());
}
#[tokio::test]
async fn huge_wait_is_rejected_without_panicking_or_opening_state() {
    assert!(service()
        .wait(&AgentId::new(), Some(f64::MAX))
        .await
        .is_err());
}
fn notice() -> Notice {
    Notice {
        notification_id: "ntf_test".into(),
        agent_id: AgentId::new(),
        status: Status::Succeeded,
        runtime: Some("mock".into()),
        model: Some("fixture".into()),
        effort: None,
        failure_kind: None,
    }
}
#[test]
fn notices_escape_controls_and_never_accept_arbitrary_lifecycle() {
    let mut n = notice();
    n.model = Some("model\nforged-header\u{2028}x".into());
    let text = n.render().unwrap();
    assert!(text.contains("model\\u000aforged-header\\u2028x"));
    assert!(!text.contains("\nforged-header"));
    n.notification_id = "ntf_".into();
    assert!(n.validate().is_err());
    n.notification_id = "ntf_x".into();
    n.status = Status::Running;
    assert!(n.validate().is_err());
}
#[test]
fn evidence_does_not_accept_extra_private_fields() {
    let mut v =
        json!({"classifier":"relay_accepted","duration_ms":1,"accepted":true,"ambiguous":false});
    assert!(agent_run::delivery::safe_evidence(&v).is_some());
    v["secret"] = json!("must-not-pass");
    assert!(agent_run::delivery::safe_evidence(&v).is_none());
}
#[test]
fn unknown_failure_categories_do_not_leak_provider_prose_in_notices() {
    let mut n = notice();
    n.status = Status::Failed;
    n.failure_kind = Some("secret-provider-error-text".into());
    let text = n.render().unwrap();
    assert!(!text.contains("secret-provider-error-text"));
    assert!(text.contains("verified successful outcome"));
}
