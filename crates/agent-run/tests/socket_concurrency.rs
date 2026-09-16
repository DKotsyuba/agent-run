//! Unix-socket shutdown and disconnect regression checks.
//!
//! These mirror Python `tests/test_api_socket.py` connection-pressure cases;
//! they need a real Unix socket and run only against temporary homes.

use agent_run::{
    cli,
    transport::{frame, socket},
};
use serde_json::json;
use std::{
    os::unix::fs::MetadataExt,
    path::Path,
    process::{Command, Stdio},
    time::Duration,
};
use tokio::{
    io::{AsyncWriteExt, BufReader},
    net::{UnixListener, UnixStream},
    sync::oneshot,
};

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

/// Reclaims an unlistened socket only after the kernel reports ECONNREFUSED.
#[tokio::test]
async fn python_stale_socket_reclaim_requires_econnrefused() {
    let temp = tempfile::tempdir().expect("temporary home");
    cli::init(temp.path()).expect("initialize temporary home");
    let path = temp.path().join("broker.sock");
    let stale = UnixListener::bind(&path).expect("bind stale socket");
    let stale_inode = std::fs::symlink_metadata(&path).unwrap().ino();
    drop(stale);
    let error = UnixStream::connect(&path).await.unwrap_err();
    assert_eq!(error.raw_os_error(), Some(libc::ECONNREFUSED));

    let task = tokio::spawn({
        let home = temp.path().to_owned();
        let path = path.clone();
        async move { socket::serve_at(&home, &path).await }
    });
    wait_for_socket(&path).await;
    assert_ne!(
        std::fs::symlink_metadata(&path).unwrap().ino(),
        stale_inode,
        "reclaim must replace the refused socket inode"
    );
    task.abort();
    let _ = task.await;
}

/// A slow or malformed owner probe remains protected because connect success
/// is not evidence that the existing listener is dead.
#[tokio::test]
async fn python_slow_and_malformed_owner_probes_are_not_reclaimed() {
    for malformed in [false, true] {
        let temp = tempfile::tempdir().expect("temporary home");
        cli::init(temp.path()).expect("initialize temporary home");
        let path = temp.path().join("broker.sock");
        let owner = UnixListener::bind(&path).expect("bind owner socket");
        let (accepted_tx, accepted_rx) = oneshot::channel();
        let (release_tx, release_rx) = oneshot::channel();
        let owner_task = tokio::spawn(async move {
            let (mut stream, _) = owner.accept().await.expect("accept ownership probe");
            accepted_tx.send(()).unwrap();
            if malformed {
                let _ = stream.write_all(b"not-json\n").await;
            }
            release_rx.await.unwrap();
        });

        let result = socket::serve_at(temp.path(), &path).await;
        assert!(
            result.is_err(),
            "owner probe unexpectedly reclaimed the socket"
        );
        accepted_rx.await.unwrap();
        assert!(path.exists());
        release_tx.send(()).unwrap();
        owner_task.await.unwrap();
    }
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

/// Sends one long-poll request and decodes its ordered broker response.
async fn request(path: &Path, value: serde_json::Value) -> serde_json::Value {
    let mut stream = UnixStream::connect(path).await.unwrap();
    frame::write(&mut stream, &value, socket::MAX_FRAME)
        .await
        .unwrap();
    let mut input = BufReader::new(stream);
    serde_json::from_slice(
        &frame::read(&mut input, socket::MAX_FRAME)
            .await
            .unwrap()
            .unwrap(),
    )
    .unwrap()
}

/// Mirrors `tests/test_api_socket.py::ApiSocketTests::test_shutdown_closes_each_owner_service_once_in_its_thread`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn python_sigterm_completes_admitted_requests_and_releases_the_store() {
    let temp = tempfile::tempdir().expect("temporary home");
    cli::init(temp.path()).expect("initialize temporary home");
    let mut broker = Command::new(env!("CARGO_BIN_EXE_agent-run"))
        .arg("--home")
        .arg(temp.path())
        .args(["api", "serve"])
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("start broker");
    let path = temp.path().join("api.sock");
    wait_for_socket(&path).await;

    let first_path = path.clone();
    let first = tokio::spawn(async move {
        request(
            &first_path,
            json!({
                "jsonrpc":"2.0",
                "id":1,
                "method":"list_agents",
                "params":{"after_revision":1000000,"wait_seconds":1.0}
            }),
        )
        .await
    });
    let second_path = path.clone();
    let second = tokio::spawn(async move {
        request(
            &second_path,
            json!({
                "jsonrpc":"2.0",
                "id":2,
                "method":"list_agents",
                "params":{"after_revision":1000000,"wait_seconds":1.0}
            }),
        )
        .await
    });
    let ping = request(
        &path,
        json!({"jsonrpc":"2.0","id":3,"method":"ping","params":{}}),
    )
    .await;
    assert_eq!(ping["result"]["ok"], true);

    // SIGTERM is the only shutdown trigger used by the production daemon.
    assert_eq!(
        // SAFETY: the child PID came directly from the test-owned broker process.
        unsafe { libc::kill(broker.id() as libc::pid_t, libc::SIGTERM) },
        0
    );
    let status = tokio::task::spawn_blocking(move || broker.wait())
        .await
        .unwrap()
        .unwrap();
    assert!(status.success());
    for response in [first.await.unwrap(), second.await.unwrap()] {
        assert_eq!(response["result"]["complete"], true, "response={response}");
    }
    assert!(!path.exists());
    assert!(agent_run::state::Store::open(temp.path()).is_ok());
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
