//! Unix-socket shutdown and disconnect regression checks.
//!
//! These mirror Python `tests/test_api_socket.py` connection-pressure cases;
//! they need a real Unix socket and run only against temporary homes.

use agent_run::{
    cli,
    transport::{frame, socket},
};
use serde_json::json;
use std::time::Duration;
use tokio::{io::BufReader, net::UnixStream};

/// Mirrors Python `test_live_slow_socket_is_never_reclaimed_as_stale`.
/// Mirrors `tests/test_api_socket.py::ApiSocketTests::test_second_owner_boot_failure_closes_the_first_owner`.
#[tokio::test]
async fn python_second_owner_refuses_a_live_listener() {
    let temp = tempfile::tempdir().expect("temporary home");
    cli::init(temp.path()).expect("initialize temporary home");
    let path = temp.path().join("broker.sock");
    let task = tokio::spawn({
        let home = temp.path().to_owned();
        let path = path.clone();
        async move { socket::serve_at(&home, &path).await }
    });
    wait_for_socket(&path).await;
    let second = socket::serve_at(temp.path(), &path).await;
    assert!(second.is_err(), "the ownership lock fences stale probing");
    task.abort();
    let _ = task.await;
}

/// Mirrors Python `test_disconnect_does_not_cancel_admitted_work` at the
/// transport boundary: a dropped client socket is not a server cancellation.
#[tokio::test]
async fn python_disconnect_closes_only_the_client_connection() {
    let temp = tempfile::tempdir().expect("temporary home");
    cli::init(temp.path()).expect("initialize temporary home");
    let path = temp.path().join("broker.sock");
    let task = tokio::spawn({
        let home = temp.path().to_owned();
        let path = path.clone();
        async move { socket::serve_at(&home, &path).await }
    });
    wait_for_socket(&path).await;
    drop(
        UnixStream::connect(&path)
            .await
            .expect("test client connects"),
    );
    assert!(
        UnixStream::connect(&path).await.is_ok(),
        "broker survives client disconnect"
    );
    task.abort();
    let _ = task.await;
}

/// Mirrors Python `test_control_slot_is_reserved_when_regular_slots_are_full`.
#[tokio::test]
async fn python_control_lane_survives_regular_connection_pressure() {
    let temp = tempfile::tempdir().expect("temporary home");
    cli::init(temp.path()).expect("initialize temporary home");
    let path = temp.path().join("broker.sock");
    let task = tokio::spawn({
        let home = temp.path().to_owned();
        let path = path.clone();
        async move { socket::serve_at(&home, &path).await }
    });
    wait_for_socket(&path).await;
    let mut regular = Vec::new();
    for _ in 0..31 {
        regular.push(UnixStream::connect(&path).await.expect("regular slot"));
    }
    let mut control = UnixStream::connect(&path)
        .await
        .expect("reserved control slot");
    frame::write(
        &mut control,
        &json!({"jsonrpc":"2.0","id":1,"method":"start","params":{}}),
        socket::MAX_FRAME,
    )
    .await
    .expect("write reserved control request");
    let mut input = BufReader::new(control);
    let response = frame::read(&mut input, socket::MAX_FRAME)
        .await
        .expect("read reserved control response")
        .expect("response frame");
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&response).expect("JSON")["error"]["code"],
        -32602
    );
    drop(regular);
    task.abort();
    let _ = task.await;
}

/// Waits for the real test listener to publish without using a global socket.
async fn wait_for_socket(path: &std::path::Path) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(3);
    loop {
        if UnixStream::connect(path).await.is_ok() {
            return;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "broker did not bind"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}
