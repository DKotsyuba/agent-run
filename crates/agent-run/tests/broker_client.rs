//! Broker-client transport parity checks.
//!
//! These tests require a real Unix socket. Each listener is private to a
//! temporary home and is removed with that home after the test completes.

use agent_run::{
    transport::{frame, socket},
    Error,
};
use serde_json::{json, Value};
use tokio::{io::BufReader, net::UnixListener};

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
