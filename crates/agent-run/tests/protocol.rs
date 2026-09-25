use agent_run::{
    delivery::Notice,
    dispatch,
    domain::AgentId,
    domain::Status,
    service::{Query, Service},
    transport::{frame, socket},
};
use serde_json::{json, Value};
use tokio::io::{AsyncReadExt, BufReader};
fn service() -> Service {
    Service::new(std::path::PathBuf::from(
        "/nonexistent-agent-run-protocol-fixture",
    ))
}
/// Mirrors `test_dispatch.py::test_tools_table_is_exactly_pinned`.
#[test]
fn packaged_table_has_exactly_the_shared_twelve_tools() {
    let tools = dispatch::tools();
    assert_eq!(tools.len(), 12);
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
/// Mirrors `test_api_socket.py::test_ping_and_tools_discovery`.
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
    assert_eq!(response["result"].as_array().unwrap().len(), 12);
}
/// Mirrors `test_api_socket.py::test_unknown_method_and_validation_error`.
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
/// Mirrors `test_api_socket.py::test_invalid_request_error_messages`.
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
/// Mirrors `test_api_socket.py::test_notification_produces_no_reply`.
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

/// Mirrors `test_api_socket.py::test_domain_errors_have_code_and_message`.
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
/// Mirrors `test_api_socket.py::test_oversized_line_is_rejected`.
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
/// The frame reader stops at the configured bound instead of consuming an
/// arbitrarily large unterminated line into memory.
#[tokio::test]
async fn framing_does_not_consume_beyond_the_memory_bound() {
    let payload = vec![b'x'; socket::MAX_FRAME * 2];
    let mut input = BufReader::with_capacity(4096, &payload[..]);
    assert!(frame::read(&mut input, socket::MAX_FRAME).await.is_err());
    let mut remaining = Vec::new();
    input.read_to_end(&mut remaining).await.unwrap();
    assert!(
        !remaining.is_empty(),
        "oversized input was consumed in full"
    );
}
#[tokio::test]
async fn framing_does_not_consume_the_next_message() {
    let mut input = BufReader::new(&b"one\ntwo\n"[..]);
    assert_eq!(frame::read(&mut input, 10).await.unwrap().unwrap(), b"one");
    assert_eq!(frame::read(&mut input, 10).await.unwrap().unwrap(), b"two");
}
/// Mirrors `test_dispatch.py::test_list_agents_accepts_revision_long_poll_fields`.
#[test]
fn list_query_bounds_reject_nonfinite_or_negative_waits() {
    let decoded: Query = serde_json::from_value(json!({
        "after_revision": 12, "wait_seconds": 1.5, "limit": 7
    }))
    .expect("dispatch list query decodes Python long-poll fields");
    assert_eq!(
        (decoded.after_revision, decoded.wait_seconds, decoded.limit),
        (Some(12), 1.5, 7)
    );
    assert!(Query {
        wait_seconds: f64::NAN,
        ..Query::default()
    }
    .validate()
    .is_err());
    assert!(Query {
        wait_seconds: -1.0,
        ..Query::default()
    }
    .validate()
    .is_err());
    assert!(Query {
        limit: 1001,
        ..Query::default()
    }
    .validate()
    .is_err());
}

/// Mirrors `test_dispatch.py::test_removed_tools_are_rejected`.
#[test]
fn retired_tools_are_not_advertised_by_dispatch() {
    for name in ["fast", "status", "list_orchestrators", "summary", "chain"] {
        assert!(!dispatch::is_tool(name), "retired tool {name} is public");
    }
}

/// Mirrors `test_dispatch.py::test_start_accepts_account`.
#[test]
fn start_request_decodes_an_optional_account() {
    let workdir = std::env::current_dir().expect("worktree is a directory");
    let request: agent_run::domain::StartRequest = serde_json::from_value(json!({
        "runtime": "codex", "model": "fixture", "profile": "review",
        "task": "inspect", "workdir": workdir, "account": "personal2"
    }))
    .expect("dispatch request schema accepts an account");
    assert_eq!(request.account.as_deref(), Some("personal2"));
}

/// Mirrors `test_dispatch.py::test_start_accepts_only_unique_known_policy_requirements`.
#[test]
fn start_request_accepts_only_unique_known_policy_requirements() {
    let workdir = std::env::current_dir().expect("worktree is a directory");
    let base = json!({
        "runtime": "codex", "model": "fixture", "profile": "review",
        "task": "inspect", "workdir": workdir,
        "required_constraints": ["external_network_isolation"]
    });
    let request: agent_run::domain::StartRequest =
        serde_json::from_value(base).expect("known unique constraints decode");
    assert_eq!(request.required_constraints.len(), 1);
    for constraints in [
        json!(["unknown"]),
        json!(["external_network_isolation", "external_network_isolation"]),
        json!("external_network_isolation"),
    ] {
        let mut value = serde_json::to_value(&request).expect("request serializes");
        value["required_constraints"] = constraints;
        assert!(serde_json::from_value::<agent_run::domain::StartRequest>(value).is_err());
    }
}

/// Mirrors `test_doc.py::test_doc_tool_call_returns_index_and_topic`.
#[tokio::test]
async fn doc_dispatch_returns_the_index_and_requested_topic() {
    let service = service();
    let index = dispatch::call(&service, "doc", json!({}))
        .await
        .expect("index document");
    let models = dispatch::call(&service, "doc", json!({"topic":"models"}))
        .await
        .expect("models document");
    assert_eq!(index["topic"], "index");
    assert!(index["text"]
        .as_str()
        .is_some_and(|text| text.contains("agent-run")));
    assert_eq!(models["topic"], "models");
    assert!(models["text"]
        .as_str()
        .is_some_and(|text| text.contains("claude, codex, and glm")));
}

/// Mirrors `test_doc.py::test_doc_tool_call_rejects_unknown_topic`.
#[tokio::test]
async fn doc_dispatch_refuses_unknown_topics() {
    assert!(
        dispatch::call(&service(), "doc", json!({"topic":"not-a-real-topic"}))
            .await
            .is_err()
    );
}

/// Mirrors `test_doc.py::test_doc_tool_is_listed`.
#[test]
fn doc_is_advertised_by_the_shared_tool_table() {
    assert!(dispatch::tools().iter().any(|tool| tool["name"] == "doc"));
}

/// Mirrors `test_dispatch.py::test_start_description_includes_completion_contract_text`.
#[test]
fn start_tool_description_embeds_the_completion_contract() {
    let start = dispatch::tools()
        .into_iter()
        .find(|tool| tool["name"] == "start")
        .expect("start tool");
    let description = start["description"].as_str().expect("description text");
    assert!(description.starts_with("Start one asynchronous durable agent."));
    assert!(description.contains(dispatch::doc("completion").expect("completion contract")));
    for field in ["- ID:", "- Status:", "- Runtime/model:", "- Notice:"] {
        assert!(description.contains(field), "missing {field}");
    }
}

/// Mirrors `test_api_socket.py::test_wait_timeout_validation`.
#[tokio::test]
async fn socket_wait_rejects_nonpositive_timeouts_before_store_access() {
    for timeout_seconds in [Value::from(0), Value::from(-1)] {
        let response = socket::respond(
            &service(),
            json!({"jsonrpc":"2.0","id":1,"method":"wait","params":{
                "agent_id":"ag-test", "timeout_seconds":timeout_seconds
            }}),
        )
        .await
        .expect("request response");
        assert_eq!(response["error"]["code"], -32602);
    }
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
/// Mirrors Python `tests/test_delivery_base.py::test_untrusted_or_nonterminal_values_are_refused`
/// (`src/agent_run/delivery/base.py:186-191` `_trusted_id` only rejects blank or oversized ids —
/// there is no `ntf_` prefix format rule in `CompletionNotice`, only in the unrelated Node relay
/// host's own request parsing) and its nonterminal-status branch (`base.py:265-266`).
#[test]
fn notices_escape_controls_and_never_accept_arbitrary_lifecycle() {
    let mut n = notice();
    n.model = Some("model\nforged-header\u{2028}x".into());
    let text = n.render().unwrap();
    assert!(text.contains("model\\u000aforged-header\\u2028x"));
    assert!(!text.contains("\nforged-header"));
    n.notification_id = "   ".into();
    assert!(n.validate().is_err());
    n.notification_id = "n".repeat(513);
    assert!(n.validate().is_err());
    n.notification_id = "ntf_x".into();
    n.status = Status::Running;
    assert!(n.validate().is_err());
}
/// Mirrors Python `DeliveryAttemptEvidence.from_payload` (`src/agent_run/delivery/base.py:121-156`),
/// which accepts exactly its 14 declared fields and rejects any payload whose key set differs
/// (`base.py:136`: `set(value) != expected`). `accepted`/`ambiguous` are derived Rust-side methods,
/// not stored fields, so a payload carrying only them was never a valid shape in either language.
#[test]
fn evidence_does_not_accept_extra_private_fields() {
    let mut v = json!({
        "classifier": "relay_accepted",
        "executable": "desktop-relay",
        "argv_shape": ["relay"],
        "duration_ms": 1,
        "returncode": null,
        "spawn_errno": null,
        "error_class": null,
        "stdout_tail": "",
        "stderr_tail": "",
        "stdout_bytes": 0,
        "stderr_bytes": 0,
        "stdout_truncated": false,
        "stderr_truncated": false,
        "message_id_present": true
    });
    assert!(agent_run::delivery::safe_evidence(&v).is_some());
    v["secret"] = json!("must-not-pass");
    assert!(agent_run::delivery::safe_evidence(&v).is_none());
}
/// Mirrors Python fixture case `failed-codex_futureProviderCode` in
/// `tests/fixtures/baseline/notices/cases.json` and `completion_notice_contract.py:97-110`: an
/// unrecognized failure kind still renders as its own (escaped) label, paired with the
/// package-owned `default_failure` reason/advice — never a caller-supplied reason or advice string.
#[test]
fn unknown_failure_categories_render_with_package_owned_default_guidance() {
    let mut n = notice();
    n.status = Status::Failed;
    n.failure_kind = Some("codex_futureProviderCode".into());
    let text = n.render().unwrap();
    assert!(text.contains(
        "- Failure: codex_futureProviderCode — The agent ended without a recognized failure category."
    ));
    assert!(text.contains(
        "- Advice: Inspect list_agents, transcript, and supervisor logs for this ID; retry only with a fresh ID after the cause is understood."
    ));
}
