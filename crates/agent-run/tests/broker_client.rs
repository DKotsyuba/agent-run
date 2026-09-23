//! Broker-client transport parity checks.
//!
//! These tests require a real Unix socket. Each listener is private to a
//! temporary home and is removed with that home after the test completes.

use agent_run::{
    transport::{frame, socket},
    Error,
};
use serde_json::{json, Value};
use std::path::PathBuf;
use tokio::{io::BufReader, net::UnixListener};

/// Builds one private client endpoint for a test server.
fn endpoint(home: &tempfile::TempDir) -> PathBuf {
    home.path().join("api.sock")
}

/// Mirrors `test_broker_client.py::test_validation_error_mapping`.
#[tokio::test]
async fn client_maps_a_validation_envelope_to_a_typed_error() {
    let home = tempfile::tempdir().expect("temporary home");
    let listener = UnixListener::bind(home.path().join("api.sock")).expect("private listener");
    let server = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.expect("client connects");
        let (input, mut output) = stream.into_split();
        let mut input = BufReader::new(input);
        let request: Value = serde_json::from_slice(
            &frame::read(&mut input, socket::MAX_FRAME)
                .await
                .expect("read request")
                .expect("request frame"),
        )
        .expect("JSON request");
        frame::write(
            &mut output,
            &json!({
                "jsonrpc": "2.0", "id": request["id"],
                "error": {"code": -32602, "message": "bad params"}
            }),
            socket::MAX_FRAME,
        )
        .await
        .expect("write response");
    });

    let error = socket::client(home.path(), "capacity_order", json!({}))
        .await
        .expect_err("validation response is an error");
    assert!(matches!(error, Error::Validation(message) if message == "bad params"));
    server.await.expect("server exits");
}

/// Mirrors `tests/test_broker_client.py::test_round_trip_and_monotonic_ids`.
#[tokio::test]
async fn client_reuses_a_connection_with_monotonic_ids() {
    let home = tempfile::tempdir().unwrap();
    let listener = UnixListener::bind(endpoint(&home)).unwrap();
    let server = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let (input, mut output) = stream.into_split();
        let mut input = BufReader::new(input);
        let mut ids = Vec::new();
        for _ in 0..2 {
            let request: Value = serde_json::from_slice(
                &frame::read(&mut input, socket::MAX_FRAME)
                    .await
                    .unwrap()
                    .unwrap(),
            )
            .unwrap();
            ids.push(request["id"].as_u64().unwrap());
            frame::write(
                &mut output,
                &json!({"jsonrpc":"2.0","id":request["id"],"result":{"value":request["params"]}}),
                socket::MAX_FRAME,
            )
            .await
            .unwrap();
        }
        ids
    });
    let client = socket::BrokerClient::new(endpoint(&home));
    assert_eq!(
        client
            .call("capacity_order", Some(json!({"x":1})))
            .await
            .unwrap(),
        json!({"value":{"x":1}})
    );
    assert_eq!(
        client.call("answer", Some(json!({"x":2}))).await.unwrap(),
        json!({"value":{"x":2}})
    );
    assert_eq!(server.await.unwrap(), vec![1, 2]);
}

/// Mirrors `tests/test_broker_client.py::test_start_serializes_request_and_rejects_malformed_results`.
#[tokio::test]
async fn client_start_serializes_request_and_rejects_malformed_results() {
    let home = tempfile::tempdir().unwrap();
    let listener = UnixListener::bind(endpoint(&home)).unwrap();
    let workdir = home.path().to_path_buf();
    let server = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let (input, mut output) = stream.into_split();
        let mut input = BufReader::new(input);
        let mut seen = Vec::new();
        for index in 0..2 {
            let request: Value = serde_json::from_slice(
                &frame::read(&mut input, socket::MAX_FRAME)
                    .await
                    .unwrap()
                    .unwrap(),
            )
            .unwrap();
            seen.push(request.clone());
            let result = if index == 0 {
                json!({"agent_id":"ag-test","attempt_id":"at-test","created":true})
            } else {
                json!({})
            };
            frame::write(
                &mut output,
                &json!({"jsonrpc":"2.0","id":request["id"],"result":result}),
                socket::MAX_FRAME,
            )
            .await
            .unwrap();
        }
        seen
    });
    let client = socket::BrokerClient::new(endpoint(&home));
    let request: agent_run_domain::ProviderStartRequest = serde_json::from_value(json!({
        "provider": "codex-user", "model": "model", "profile": "review",
        "task": "task", "workdir": workdir,
    }))
    .unwrap();
    let result = client.start(&request).await.unwrap();
    assert_eq!(
        (
            result.agent_id.as_str(),
            result.created,
            result.attempt_id.as_deref()
        ),
        ("ag-test", true, Some("at-test"))
    );
    let error = client.start(&request).await.unwrap_err();
    assert!(
        matches!(error, Error::Runtime(message) if message == "broker returned an invalid start result")
    );
    let seen = server.await.unwrap();
    assert_eq!(seen[0]["method"], "start");
    // The strict provider shape the dispatcher accepts, never `runtime`.
    assert_eq!(seen[0]["params"]["provider"], "codex-user");
    assert!(seen[0]["params"].get("runtime").is_none());
    serde_json::from_value::<agent_run_domain::ProviderStartRequest>(seen[0]["params"].clone())
        .unwrap();
    assert_eq!(
        seen[0]["params"]["workdir"],
        home.path().to_string_lossy().as_ref()
    );
}

/// Mirrors `tests/test_broker_client.py::test_reconnects_once_after_server_restart`.
#[tokio::test]
async fn client_reconnects_once_after_server_restart() {
    let home = tempfile::tempdir().unwrap();
    let path = endpoint(&home);
    let first = UnixListener::bind(&path).unwrap();
    let first_server = tokio::spawn(async move {
        let (stream, _) = first.accept().await.unwrap();
        drop(stream);
    });
    let client = socket::BrokerClient::new(&path);
    let _ = client.call("ping", None).await;
    first_server.await.unwrap();
    std::fs::remove_file(&path).unwrap();
    let second = UnixListener::bind(&path).unwrap();
    let second_server = tokio::spawn(async move {
        let (stream, _) = second.accept().await.unwrap();
        let (input, mut output) = stream.into_split();
        let mut input = BufReader::new(input);
        let request: Value = serde_json::from_slice(
            &frame::read(&mut input, socket::MAX_FRAME)
                .await
                .unwrap()
                .unwrap(),
        )
        .unwrap();
        frame::write(
            &mut output,
            &json!({"jsonrpc":"2.0","id":request["id"],"result":{"restarted":true}}),
            socket::MAX_FRAME,
        )
        .await
        .unwrap();
    });
    assert_eq!(
        client.call("capacity_order", None).await.unwrap(),
        json!({"restarted":true})
    );
    second_server.await.unwrap();
}

/// Mirrors `tests/test_broker_client.py::test_unavailable_has_actionable_message`.
#[tokio::test]
async fn client_unavailable_error_is_actionable() {
    let home = tempfile::tempdir().unwrap();
    let error = socket::BrokerClient::new(endpoint(&home))
        .call("capacity_order", None)
        .await
        .unwrap_err();
    assert!(matches!(error, Error::BrokerUnavailable));
    assert!(error.to_string().contains("agent-run api serve"));
}

/// Mirrors `tests/test_broker_client.py::test_invalid_deadlines_are_rejected_before_socket_creation`.
#[tokio::test]
async fn client_rejects_invalid_deadlines_before_connecting() {
    let home = tempfile::tempdir().unwrap();
    for timeout in [0.0, -1.0, f64::INFINITY, f64::NAN] {
        let error = socket::BrokerClient::new(endpoint(&home))
            .call_with_timeout("capacity_order", None, timeout)
            .await
            .unwrap_err();
        assert!(
            matches!(error, Error::Validation(message) if message.contains("positive and finite"))
        );
    }
}

/// Mirrors `tests/test_broker_client.py::test_agent_error_mapping_preserves_data_code`.
#[tokio::test]
async fn client_preserves_agent_error_mapping_data_code() {
    let home = tempfile::tempdir().unwrap();
    let listener = UnixListener::bind(endpoint(&home)).unwrap();
    let server = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let (input, mut output) = stream.into_split();
        let mut input = BufReader::new(input);
        let request: Value = serde_json::from_slice(
            &frame::read(&mut input, socket::MAX_FRAME)
                .await
                .unwrap()
                .unwrap(),
        )
        .unwrap();
        frame::write(&mut output, &json!({"jsonrpc":"2.0","id":request["id"],"error":{"code":-32000,"message":"domain failure","data":{"code":"AuthError","message":"domain failure"}}}), socket::MAX_FRAME).await.unwrap();
    });
    let error = socket::BrokerClient::new(endpoint(&home))
        .call("capacity_order", None)
        .await
        .unwrap_err();
    assert!(
        matches!(error, Error::Broker { broker_error_code: Some(ref code), .. } if code == "AuthError")
    );
    assert_eq!(error.to_string(), "domain failure");
    server.await.unwrap();
}

/// Mirrors `tests/test_broker_client.py::test_abort_interrupts_each_connect_without_retry_or_worker_leak`.
#[tokio::test]
async fn client_abort_interrupts_a_pending_connect_without_retry() {
    let home = tempfile::tempdir().unwrap();
    let client = socket::BrokerClient::new(endpoint(&home));
    client.abort();
    let error = client.call("capacity_order", None).await.unwrap_err();
    assert!(matches!(error, Error::Runtime(message) if message == "broker call cancelled"));
}
