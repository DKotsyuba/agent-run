//! Live Unix-socket protocol checks.
//!
//! These mirror Python `tests/test_api_socket.py` and require a real Unix
//! socket; they intentionally use a temporary home rather than `~/.agent-run`.

use agent_run::{
    cli,
    transport::{frame, socket},
};
use serde_json::{json, Value};
use std::{path::Path, time::Duration};
use tokio::{io::BufReader, net::UnixStream};

/// Mirrors `test_api_socket.py::test_socket_mode_and_stale_socket_replacement`.
#[tokio::test]
async fn python_socket_binds_custom_path_and_answers_ping() {
    let temp = tempfile::tempdir().expect("temporary home");
    cli::init(temp.path()).expect("initialize temporary home");
    let path = temp.path().join("broker.sock");
    let task = tokio::spawn({
        let home = temp.path().to_owned();
        let path = path.clone();
        async move { socket::serve_at(&home, &path).await }
    });
    let stream = wait_for_socket(&path).await;
    let mut stream = stream.expect("server published custom socket");
    frame::write(
        &mut stream,
        &json!({"jsonrpc":"2.0","id":1,"method":"ping","params":{}}),
        socket::MAX_FRAME,
    )
    .await
    .expect("write ping");
    let mut input = BufReader::new(stream);
    let response: Value = serde_json::from_slice(
        &frame::read(&mut input, socket::MAX_FRAME)
            .await
            .expect("read ping")
            .expect("ping response"),
    )
    .expect("JSON response");
    assert_eq!(response["result"], json!({"ok":true}));
    task.abort();
    let _ = task.await;
    assert!(!path.exists(), "guard removes only its socket on shutdown");
}

/// Waits for a test-only listener without touching the owner's live socket.
async fn wait_for_socket(path: &Path) -> Option<UnixStream> {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(3);
    loop {
        if let Ok(stream) = UnixStream::connect(path).await {
            return Some(stream);
        }
        if tokio::time::Instant::now() >= deadline {
            return None;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}
