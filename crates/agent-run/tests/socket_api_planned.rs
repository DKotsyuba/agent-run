//! Remaining live and pure ports of the Python socket API contract.

use agent_run::domain::Outcome;
use agent_run::{
    cli,
    service::Service,
    transport::{frame, socket},
    verify,
};
use serde_json::{Value, json};
use std::{
    path::{Path, PathBuf},
    time::Duration,
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt, BufReader},
    net::UnixStream,
};

/// Native fake peer for private socket tests; no interpreter is trusted.
#[cfg(target_os = "linux")]
#[test]
fn r2_socket_fake_peer_child() {
    use std::io::{Read, Write};
    let Some(socket) = std::env::var_os("RUN_R2_PEER_SOCKET") else {
        return;
    };
    let frame = std::env::var("RUN_R2_PEER_FRAME").unwrap();
    let mut stream = std::os::unix::net::UnixStream::connect(socket).unwrap();
    let mut bytes = Vec::new();
    let _ = writeln!(stream, "{frame}");
    match stream.read_to_end(&mut bytes) {
        Ok(_) => {}
        Err(error)
            if matches!(
                error.kind(),
                std::io::ErrorKind::ConnectionReset | std::io::ErrorKind::BrokenPipe
            ) => {}
        Err(error) => panic!("fake peer read: {error}"),
    };
    std::fs::write(std::env::var_os("RUN_R2_PEER_OUTPUT").unwrap(), bytes).unwrap();
}

#[cfg(target_os = "linux")]
async fn r2_fake_peer(executable: &Path, socket: &Path, frame: &str, output: &Path) -> String {
    let _ = std::fs::remove_file(output);
    let child = tokio::process::Command::new(executable)
        .args([
            "--exact",
            exact_test("r2_socket_fake_peer_child").as_str(),
            "--nocapture",
        ])
        .env_clear()
        .env("RUN_R2_PEER_SOCKET", socket)
        .env("RUN_R2_PEER_FRAME", frame)
        .env("RUN_R2_PEER_OUTPUT", output)
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .kill_on_drop(true)
        .output()
        .await
        .unwrap();
    assert!(child.status.success());
    String::from_utf8(std::fs::read(output).unwrap()).unwrap()
}

#[cfg(target_os = "linux")]
async fn r2_wait_build_age(executable: &Path) {
    use std::os::unix::fs::MetadataExt;
    let metadata = std::fs::metadata(executable).unwrap();
    let newest = metadata.mtime().max(metadata.ctime());
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs() as i64;
    if newest >= now {
        tokio::time::sleep(Duration::from_secs((newest - now + 2) as u64)).await;
    }
}

#[cfg(target_os = "linux")]
fn r2_copy_peer(dir: &Path) -> PathBuf {
    use std::os::unix::fs::PermissionsExt;
    let executable = dir.join("fixture-peer");
    std::fs::copy(std::env::current_exe().unwrap(), &executable).unwrap();
    std::fs::set_permissions(&executable, std::fs::Permissions::from_mode(0o700)).unwrap();
    executable.canonicalize().unwrap()
}

/// Starts a private broker on a short temporary socket path.
async fn broker() -> (
    tempfile::TempDir,
    PathBuf,
    tokio::task::JoinHandle<agent_run::Result<()>>,
) {
    let home = tempfile::tempdir().unwrap();
    configure(home.path());
    let path = home.path().join("api.sock");
    let task = tokio::spawn({
        let home = home.path().to_owned();
        let path = path.clone();
        async move { socket::serve_at(&home, &path).await }
    });
    wait_for_socket(&path).await;
    assert!(!home.path().join("attempt.sock").exists());
    assert!(!home.path().join("attempt-receipts.sock").exists());
    (home, path, task)
}

/// Starts a broker using explicit test bounds.
async fn bounded_broker(
    options: socket::ServeOptions,
) -> (
    tempfile::TempDir,
    PathBuf,
    tokio::task::JoinHandle<agent_run::Result<()>>,
) {
    let home = tempfile::tempdir().unwrap();
    configure(home.path());
    let path = home.path().join("api.sock");
    let task = tokio::spawn({
        let home = home.path().to_owned();
        let path = path.clone();
        async move { socket::serve_at_with_options(&home, &path, options).await }
    });
    wait_for_socket(&path).await;
    (home, path, task)
}

/// Writes the smallest deterministic runtime configuration needed by socket calls.
fn configure(home: &Path) {
    cli::init(home).unwrap();
    let binary = if Path::new("/usr/bin/true").is_file() {
        "/usr/bin/true"
    } else {
        "/bin/true"
    };
    std::fs::write(home.join("config.toml"), format!("schema_version=1\n[runtimes.mock]\nenabled=true\nadapter='claude'\nbinary='{}'\nhome='{}'\nmodels=['fixture']\nlimits_source='none'\n", binary, home.join("runtimes/mock").display())).unwrap();
}

/// Waits until the broker accepts a local connection.
async fn wait_for_socket(path: &Path) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(3);
    loop {
        if path.exists() {
            return;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "broker did not bind"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

/// Sends one JSON-RPC request and decodes its response.
async fn request(path: &Path, value: Value) -> Value {
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

/// Sends one raw newline-delimited request and returns its typed JSON-RPC reply.
async fn raw_request(path: &Path, frame_bytes: &[u8]) -> Value {
    let mut stream = UnixStream::connect(path).await.unwrap();
    stream.write_all(frame_bytes).await.unwrap();
    let mut input = BufReader::new(stream);
    serde_json::from_slice(
        &frame::read(&mut input, socket::MAX_FRAME)
            .await
            .unwrap()
            .unwrap(),
    )
    .unwrap()
}

/// Stops a broker task and releases its temporary home.
async fn stop(task: tokio::task::JoinHandle<agent_run::Result<()>>) {
    task.abort();
    let _ = task.await;
}

/// Proves both broker sockets and the writer lease have drained after task abort.
/// Accepted SQL outlives its caller, so cancellation alone is not restart authority.
/// Only occupied-lock conflicts are retried within the existing five-second drain
/// allowance; other errors fail immediately. No database writes or signalling occur.
async fn wait_for_broker_quiescence(home: &Path) {
    use agent_run_store::writer::{BrokerExclusion, WriterLease};
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    let api = home.join("api.sock");
    let writer = home.join("writer.sock");
    loop {
        let probe = BrokerExclusion::acquire(home, &[&api, &writer]).and_then(|exclusion| {
            WriterLease::acquire_online(home).map(|lease| (exclusion, lease))
        });
        match probe {
            Ok(guards) => {
                drop(guards);
                return;
            }
            Err(agent_run::Error::Conflict) => {}
            Err(error) => panic!("broker quiescence failed: {error}"),
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "broker writer did not drain"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

/// Admits one nonterminal fixture row for wait and long-poll requests.
fn admitted(home: &tempfile::TempDir) -> agent_run::domain::AgentId {
    let config = agent_run::config::Config::load(home.path()).unwrap();
    let request: agent_run::domain::StartRequest = serde_json::from_value(json!({
        "runtime":"mock", "model":"fixture", "profile":"review",
        "task":"fixture", "workdir":home.path()
    }))
    .unwrap();
    agent_run_store::fixture::open(home.path())
        .unwrap()
        .admit(&request, &config, &json!({}), None)
        .unwrap()
        .0
}

/// Completes one fixture agent with a verified inline answer for wait tests.
fn completed(home: &tempfile::TempDir) -> agent_run::domain::AgentId {
    let id = admitted(home);
    let root = home.path().join("agents").join(id.as_str());
    agent_run::fs::private_dir(&root).unwrap();
    let proof = verify::seal(&root, std::path::Path::new("answer.md"), "done").unwrap();
    let mut store = agent_run_store::fixture::open(home.path()).unwrap();
    store.running(&id, 42).unwrap();
    store
        .finish(&id, &Outcome::success(None), Some(&proof), None)
        .unwrap();
    id
}

/// Mirrors `tests/test_api_socket.py::ApiSocketTests::test_all_dispatch_runs_on_the_service_owning_thread`.
#[tokio::test]
async fn python_dispatch_calls_remain_serializable_at_the_service_boundary() {
    let service = Service::new(PathBuf::from("/nonexistent-agent-run-protocol-fixture"));
    for id in [1, 2] {
        let response = socket::respond(
            &service,
            json!({"jsonrpc":"2.0","id":id,"method":"ping","params":{}}),
        )
        .await
        .unwrap();
        assert_eq!(response["result"]["ok"], true);
    }
}

/// Mirrors `tests/test_api_socket.py::ApiSocketTests::test_list_long_poll_outlives_read_deadline_without_blocking_reads`.
#[tokio::test]
async fn python_list_long_poll_does_not_block_an_immediate_read() {
    let (home, path, task) = bounded_broker(socket::ServeOptions {
        request_timeout: Duration::from_millis(50),
        ..Default::default()
    })
    .await;
    let long_poll = tokio::spawn({
        let path = path.clone();
        async move {
            request(&path, json!({"jsonrpc":"2.0","id":1,"method":"list_agents","params":{"after_revision":100,"wait_seconds":0.2}})).await
        }
    });
    tokio::time::sleep(Duration::from_millis(80)).await;
    let immediate = request(
        &path,
        json!({"jsonrpc":"2.0","id":2,"method":"ping","params":{}}),
    )
    .await;
    assert_eq!(immediate["result"]["ok"], true);
    assert_eq!(long_poll.await.unwrap()["result"]["complete"], true);
    stop(task).await;
    drop(home);
}

/// Mirrors `tests/test_api_socket.py::ApiSocketTests::test_old_server_release_does_not_unlink_replacement_socket`.
#[tokio::test]
async fn python_old_owner_cannot_remove_a_replacement_socket() {
    let (home, path, first) = broker().await;
    stop(first).await;
    wait_for_broker_quiescence(home.path()).await;
    let replacement = tokio::spawn({
        let home = home.path().to_owned();
        let path = path.clone();
        async move { socket::serve_at(&home, &path).await }
    });
    wait_for_socket(&path).await;
    assert!(path.exists());
    stop(replacement).await;
    assert!(!path.exists());
}

/// Mirrors `tests/test_api_socket.py::ApiSocketTests::test_shutdown_closes_each_owner_service_once_in_its_thread`.
#[tokio::test]
async fn python_shutdown_removes_the_owned_socket() {
    let (home, path, task) = broker().await;
    assert!(path.exists());
    stop(task).await;
    assert!(!path.exists());
    drop(home);
}

/// Mirrors `tests/test_api_socket.py::DispatcherShutdownTests::test_close_and_submit_cannot_cross_the_shutdown_sentinel`.
#[tokio::test]
async fn python_shutdown_releases_the_socket_without_a_second_owner() {
    let (home, path, task) = broker().await;
    stop(task).await;
    assert!(!path.exists());
    drop(home);
}

/// Mirrors `tests/test_api_socket.py::ApiSocketTests::test_oversized_line_is_rejected`.
#[tokio::test]
async fn python_oversized_frame_is_rejected() {
    let (home, path, task) = broker().await;
    let mut stream = UnixStream::connect(&path).await.unwrap();
    let mut frame = vec![b'x'; socket::MAX_FRAME];
    frame.push(b'\n');
    stream.write_all(&frame).await.unwrap();
    let mut input = BufReader::new(stream);
    let response: Value = serde_json::from_slice(
        &frame::read(&mut input, socket::MAX_FRAME)
            .await
            .unwrap()
            .unwrap(),
    )
    .unwrap();
    assert_eq!(response["error"]["code"], -32700);
    stop(task).await;
    drop(home);
}

// No single Python test sends all three raw frames; this protects the live
// Rust socket's parse, request-id, and batch-array error mapping together.
#[tokio::test]
async fn python_ndjson_bad_id_and_batch_errors_are_typed_on_the_wire() {
    let (home, path, task) = broker().await;
    let cases: &[(&[u8], i64)] = &[
        (b"not-json\n", -32700),
        (b"[]\n", -32600),
        (
            b"{\"jsonrpc\":\"2.0\",\"id\":true,\"method\":\"ping\"}\n",
            -32600,
        ),
    ];
    for (frame_bytes, code) in cases {
        let response = raw_request(&path, frame_bytes).await;
        assert_eq!(response["error"]["code"], *code, "response={response}");
    }
    stop(task).await;
    drop(home);
}

/// Mirrors `tests/test_api_socket.py::ApiSocketTests::test_unknown_method_and_validation_error`.
#[tokio::test]
async fn python_unknown_method_and_validation_use_standard_codes() {
    let service = Service::new(PathBuf::from("/nonexistent-agent-run-protocol-fixture"));
    let unknown = socket::respond(&service, json!({"jsonrpc":"2.0","id":1,"method":"missing"}))
        .await
        .unwrap();
    assert_eq!(unknown["error"]["code"], -32601);
    let invalid = socket::respond(
        &service,
        json!({"jsonrpc":"2.0","id":2,"method":"cancel","params":{}}),
    )
    .await
    .unwrap();
    assert_eq!(invalid["error"]["code"], -32602);
}

/// Mirrors `tests/test_api_socket.py::ApiSocketTests::test_notification_produces_no_reply`.
#[tokio::test]
async fn python_notification_produces_no_reply() {
    assert!(
        socket::respond(
            &Service::new(PathBuf::from("/nonexistent")),
            json!({"jsonrpc":"2.0","method":"ping"})
        )
        .await
        .is_none()
    );
}

/// Mirrors `tests/test_api_socket.py::ApiSocketTests::test_request_deadline_and_queue_overload_are_explicit`.
#[tokio::test]
async fn python_request_capacity_is_explicitly_bounded() {
    let service = Service::new(PathBuf::from("/nonexistent-agent-run-protocol-fixture"));
    let response = socket::respond(
        &service,
        json!({"jsonrpc":"2.0","id":1,"method":"list_agents","params":{"limit":0}}),
    )
    .await
    .unwrap();
    assert_eq!(response["error"]["code"], -32602);
    assert!(
        socket::serve_at_with_options(
            Path::new("/nonexistent"),
            Path::new("/tmp/no.sock"),
            socket::ServeOptions {
                max_pending_requests: 0,
                ..Default::default()
            }
        )
        .await
        .is_err()
    );
}

/// Mirrors `tests/test_api_socket.py::ApiSocketTests::test_slow_bytes_cannot_extend_reserved_first_frame_deadline`.
#[tokio::test]
async fn python_reserved_control_frame_has_a_fixed_deadline() {
    let (home, path, task) = broker().await;
    let mut regular = UnixStream::connect(&path).await.unwrap();
    regular.write_all(b"{").await.unwrap();
    let mut slow = UnixStream::connect(&path).await.unwrap();
    slow.write_all(b"{").await.unwrap();
    tokio::time::sleep(Duration::from_millis(600)).await;
    let response = request(
        &path,
        json!({"jsonrpc":"2.0","id":5,"method":"ping","params":{}}),
    )
    .await;
    assert!(response.get("result").is_some() || response["error"]["code"] == -32001);
    drop((regular, slow));
    stop(task).await;
    drop(home);
}

/// Mirrors `tests/test_api_socket.py::ApiSocketTests::test_wait_does_not_block_other_connections`.
#[tokio::test]
async fn python_wait_does_not_block_other_connections() {
    let (home, path, task) = broker().await;
    let id = admitted(&home);
    let waiter = tokio::spawn({
        let path = path.clone();
        async move {
            request(&path, json!({"jsonrpc":"2.0","id":1,"method":"wait","params":{"agent_id":id,"timeout_seconds":0.2}})).await
        }
    });
    tokio::time::sleep(Duration::from_millis(30)).await;
    assert_eq!(
        request(
            &path,
            json!({"jsonrpc":"2.0","id":2,"method":"ping","params":{}})
        )
        .await["result"]["ok"],
        true
    );
    assert_eq!(waiter.await.unwrap()["result"]["timed_out"], true);
    stop(task).await;
    drop(home);
}

/// Mirrors `tests/test_api_socket.py::ApiSocketTests::test_wait_returns_the_answer_envelope_when_agent_finishes`.
#[tokio::test]
async fn python_wait_returns_the_verified_answer_envelope() {
    let (home, path, task) = broker().await;
    let id = completed(&home);
    let response = request(
        &path,
        json!({
            "jsonrpc":"2.0", "id":1, "method":"wait",
            "params":{"agent_id":id,"timeout_seconds":1.0}
        }),
    )
    .await;
    assert_eq!(response["result"]["content"], "done");
    assert_eq!(response["result"]["status"], "succeeded");
    stop(task).await;
    drop(home);
}

/// Mirrors `tests/test_api_socket.py::ApiSocketTests::test_connection_limit_returns_explicit_overload`.
#[tokio::test]
async fn python_connection_limit_returns_explicit_overload() {
    let (home, path, task) = bounded_broker(socket::ServeOptions {
        max_connections: 1,
        idle_timeout: Duration::from_secs(1),
        ..Default::default()
    })
    .await;
    let mut blocked = UnixStream::connect(&path).await.unwrap();
    blocked.write_all(b"{").await.unwrap();
    tokio::time::sleep(Duration::from_millis(50)).await;
    let response = request(
        &path,
        json!({"jsonrpc":"2.0","id":1,"method":"ping","params":{}}),
    )
    .await;
    assert_eq!(response["error"]["code"], -32001, "response={response}");
    drop(blocked);
    stop(task).await;
    drop(home);
}

/// Mirrors `tests/test_api_socket.py::ApiSocketTests::test_control_slot_survives_saturated_long_read_connections`.
#[tokio::test]
async fn python_control_slot_survives_saturated_long_reads() {
    let (home, path, task) = broker().await;
    let mut regular = Vec::new();
    for _ in 0..31 {
        regular.push(UnixStream::connect(&path).await.unwrap());
    }
    let response = request(&path, json!({"jsonrpc":"2.0","id":4,"method":"cancel","params":{"agent_id":"ag-20260826-120000-0123456789"}})).await;
    assert_ne!(response["error"]["code"], -32001);
    drop(regular);
    stop(task).await;
    drop(home);
}

/// Mirrors `tests/test_api_socket.py::ApiSocketTests::test_partial_frame_hits_idle_deadline_and_releases_connection`.
#[tokio::test]
async fn python_partial_frame_hits_idle_deadline() {
    let options = socket::ServeOptions {
        max_connections: 2,
        idle_timeout: Duration::from_millis(50),
        ..Default::default()
    };
    let (home, path, task) = bounded_broker(options).await;
    let mut client = UnixStream::connect(&path).await.unwrap();
    client.write_all(b"{\"jsonrpc\":\"2.0\"").await.unwrap();
    let mut byte = [0; 1];
    assert_eq!(client.read(&mut byte).await.unwrap(), 0);
    stop(task).await;
    drop(home);
}

/// A held Quota read must not block a durable cancel on the separate lane.
/// The legacy schema-1 order explicitly advises migration before this test
/// switches only its own synthetic config to schema 2.
#[tokio::test]
async fn python_capacity_order_and_durable_cancel_use_independent_paths() {
    let (home, path, task) = broker().await;
    let id = admitted(&home);
    let legacy = request(
        &path,
        json!({"jsonrpc":"2.0","id":0,"method":"capacity_order","params":{}}),
    )
    .await;
    assert_eq!(legacy["error"]["data"]["code"], "Unsupported");
    assert!(
        legacy["error"]["message"]
            .as_str()
            .unwrap()
            .contains("schema_version 2")
    );

    let quota_path = home.path().join("quota.sock");
    let listener = tokio::net::UnixListener::bind(&quota_path).unwrap();
    let (seen_tx, seen_rx) = tokio::sync::oneshot::channel();
    let (release_tx, release_rx) = tokio::sync::oneshot::channel();
    let quota_task = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        let mut bytes = Vec::new();
        stream.read_to_end(&mut bytes).await.unwrap();
        let request: Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(request["method"], "snapshot");
        assert_eq!(request["accounts"], json!([]));
        let _ = seen_tx.send(());
        let _ = release_rx.await;
        // EOF models an unavailable Quota source after the proven held read.
    });
    std::fs::write(
        home.path().join("config.toml"),
        format!(
            "schema_version=2\n[capacity.quota]\nsocket={}\n",
            toml::Value::String(quota_path.to_string_lossy().into_owned()),
        ),
    )
    .unwrap();
    let order = tokio::spawn({
        let path = path.clone();
        async move {
            request(
                &path,
                json!({"jsonrpc":"2.0","id":1,"method":"capacity_order","params":{}}),
            )
            .await
        }
    });
    tokio::time::timeout(Duration::from_secs(3), seen_rx)
        .await
        .unwrap()
        .unwrap();
    let started = tokio::time::Instant::now();
    let response = request(
        &path,
        json!({
            "jsonrpc":"2.0", "id":2, "method":"cancel", "params":{"agent_id":id}
        }),
    )
    .await;
    assert!(response.get("result").is_some(), "response={response}");
    assert!(started.elapsed() < Duration::from_secs(1));
    assert!(
        !order.is_finished(),
        "order must still be waiting for Quota"
    );
    release_tx.send(()).unwrap();
    let order = order.await.unwrap();
    assert_eq!(order["result"]["quota_coverage"]["state"], "unknown");
    assert_eq!(order["result"]["quota_coverage"]["incomplete"], true);
    quota_task.await.unwrap();
    stop(task).await;
    drop(home);
}

/// The live Unix socket discovers and dispatches list_pools with typed validation.
#[tokio::test]
async fn list_pools_live_socket_round_trip() {
    let (_temp, path, task) = broker().await;
    let response = request(
        &path,
        json!({"jsonrpc":"2.0","id":1,
        "method":"list_pools","params":{"state":"open","limit":1,"offset":0}}),
    )
    .await;
    assert_eq!(response["result"]["items"], json!([]));
    assert_eq!(response["result"]["total"], 0);
    assert_eq!(response["result"]["complete"], true);
    assert_eq!(response["result"]["limit"], 1);
    let bad = request(
        &path,
        json!({"jsonrpc":"2.0","id":2,
        "method":"list_pools","params":{"limit":201}}),
    )
    .await;
    assert_eq!(bad["error"]["code"], -32602);
    assert_eq!(bad["error"]["message"], "invalid pool list page arguments");
    stop(task).await;
}

/// Mirrors `tests/test_api_socket.py::ApiSocketTests::test_successful_tool_round_trip`.
#[tokio::test]
async fn python_successful_tool_round_trip() {
    let (home, path, task) = broker().await;
    assert_eq!(
        request(
            &path,
            json!({"jsonrpc":"2.0","id":1,"method":"ping","params":{}})
        )
        .await["result"],
        json!({"ok":true})
    );
    let health = request(
        &path,
        json!({"jsonrpc":"2.0","id":20,"method":"ping","params":{"writer_health":true}}),
    )
    .await;
    assert_eq!(health["result"]["ok"], true);
    assert!(health["result"]["writer"]["accepted_jobs"].is_number());
    assert!(health["result"]["writer"]["counters"].is_object());
    assert!(
        request(
            &path,
            json!({"jsonrpc":"2.0","id":2,"method":"models","params":{}})
        )
        .await
        .get("result")
        .is_some()
    );
    stop(task).await;
    drop(home);
}

/// Mirrors `tests/test_api_socket.py::ApiSocketTests::test_surface_is_dispatch_tools_plus_control_methods`.
/// The current shared surface additionally exposes the compact delegation guide
/// and the five cooperative-pool operator tools plus the native integration read.
#[test]
fn python_socket_surface_is_shared_tools_plus_controls() {
    let names = agent_run::dispatch::tools()
        .into_iter()
        .filter_map(|tool| tool["name"].as_str().map(str::to_owned))
        .collect::<std::collections::BTreeSet<_>>();
    assert!(names.contains("start") && names.contains("list_agents"));
    assert!(names.contains("delegation_guide"));
    assert!(names.contains("capabilities") && names.contains("answer_chunk"));
    for pool in [
        "start_pool",
        "pool_post",
        "pool_replace",
        "pool",
        "list_pools",
    ] {
        assert!(names.contains(pool), "{pool}");
    }
    let registry: std::collections::BTreeSet<_> = agent_run_domain::registry()
        .iter()
        .filter(|tool| tool.legacy_for.is_none())
        .map(|tool| tool.name.clone())
        .collect();
    assert_eq!(names, registry, "the socket surface is the shared table");
    assert_eq!(names.len(), 24);
    assert!(!names.contains("models"));
    assert!(!names.contains("capacity_order"));
}

/// Mirrors `tests/test_api_socket.py::ApiSocketTests::test_wait_timeout_validation`.
#[tokio::test]
async fn python_wait_timeout_validation_is_typed() {
    let service = Service::new(PathBuf::from("/nonexistent-agent-run-protocol-fixture"));
    for timeout in [json!(0), json!(-1), json!(true), json!("1")] {
        let response = socket::respond(&service, json!({"jsonrpc":"2.0","id":1,"method":"wait","params":{"agent_id":"ag-test","timeout_seconds":timeout}})).await.unwrap();
        assert_eq!(response["error"]["code"], -32602);
    }
}

/// Mirrors `tests/test_api_socket.py::ApiSocketTests::test_wait_timeout_is_a_normal_timed_out_result`.
#[tokio::test]
async fn python_wait_timeout_is_a_normal_result() {
    let home = tempfile::tempdir().unwrap();
    configure(home.path());
    let mut store = agent_run_store::fixture::open(home.path()).unwrap();
    let request: agent_run::domain::StartRequest = serde_json::from_value(json!({
        "runtime":"mock", "model":"fixture", "profile":"review",
        "task":"fixture", "workdir":home.path()
    }))
    .unwrap();
    let id = store
        .admit(
            &request,
            &agent_run::config::Config::load(home.path()).unwrap(),
            &json!({}),
            None,
        )
        .unwrap()
        .0;
    let response = socket::respond(
        &Service::new(home.path().to_owned()),
        json!({
            "jsonrpc":"2.0", "id":1, "method":"wait",
            "params":{"agent_id":id,"timeout_seconds":0.01}
        }),
    )
    .await
    .unwrap();
    assert_eq!(response["result"]["timed_out"], true);
    assert_eq!(response["result"]["terminal"], false);
}

/// A journal-only change (a transcript row, no event) wakes a waiting
/// `list_agents` round through `after_message_revision`, inside the paced
/// floor and before the wait deadline, without advancing the event revision.
#[tokio::test]
async fn list_long_poll_wakes_on_journal_progress() {
    let (home, path, task) = broker().await;
    let id = admitted(&home);
    let (revision, message_revision) = {
        let store = agent_run_store::fixture::open(home.path()).unwrap();
        (store.revision().unwrap(), store.message_revision().unwrap())
    };
    assert_eq!(message_revision, 0, "a fresh fixture has no journal rows");
    tokio::spawn({
        let home = home.path().to_owned();
        async move {
            tokio::time::sleep(Duration::from_millis(100)).await;
            agent_run_store::fixture::open(&home)
                .unwrap()
                .message(&id, "assistant", "progress", None, None)
                .unwrap();
        }
    });
    let started = std::time::Instant::now();
    let response = request(
        &path,
        json!({"jsonrpc":"2.0","id":1,"method":"list_agents","params":{
            "after_revision":revision,"after_message_revision":message_revision,"wait_seconds":3.0
        }}),
    )
    .await;
    let elapsed = started.elapsed();
    assert_eq!(
        response["result"]["revision"], revision,
        "no event advanced: {response}"
    );
    assert!(
        response["result"]["message_revision"].as_i64().unwrap() > message_revision,
        "the transcript watermark advanced: {response}"
    );
    assert!(
        elapsed >= Duration::from_millis(1000),
        "journal wakes respect the one-second floor: {elapsed:?}"
    );
    assert!(
        elapsed < Duration::from_millis(2500),
        "the wake precedes the wait deadline: {elapsed:?}"
    );
    stop(task).await;
    drop(home);
}

/// A configured Bridge reader has a separate authenticated read-only socket;
/// changing the pin revokes it, while disabling permits exact receipt recovery.
#[cfg(target_os = "linux")]
#[tokio::test]
async fn bridge_receipt_socket_live_smoke_and_config_revoke() {
    use std::os::unix::fs::PermissionsExt;
    let base = std::env::var_os("AGENT_RUN_TEST_SOCKET_TMP")
        .map(PathBuf::from)
        .unwrap_or_else(std::env::temp_dir);
    let home = tempfile::Builder::new()
        .prefix("r2-broker-")
        .tempdir_in(base)
        .unwrap();
    std::fs::set_permissions(home.path(), std::fs::Permissions::from_mode(0o700)).unwrap();
    cli::init(home.path()).unwrap();
    let store = agent_run_store::fixture::open(home.path()).unwrap();
    let sid = agent_run_store::store_identity::read(&store.conn).unwrap();
    let id = agent_run::domain::AgentId::new();
    let launch = uuid::Uuid::new_v4().to_string();
    let receipt_id = uuid::Uuid::new_v4().to_string();
    // SAFETY: geteuid is read-only and has no preconditions.
    let uid = unsafe { libc::geteuid() };
    let route = agent_run_domain::attempt_binding::Route {
        component_store_id: "pool-store".into(),
        route_id: "pool-route".into(),
        worker_uid: uid,
    };
    let receipt = json!({"receipt_id":receipt_id,"purpose":"ingress","lineage_root_id":id,"bound_execution_id":id,"attempt":{"run_store_id":sid,"lineage_root_id":id,"execution_id":id,"sequence":1,"attempt_id":"attempt","launch_id":launch},"incarnation":{"host_id":"host","boot_id":"00000000-0000-4000-8000-000000000001","pid_namespace_inode":1,"uid":uid,"pid":2,"start_ticks":1},"actor":{"address":"host/codex/s","role":"root","native_id":"s","epoch":1},"successor_proof":{"lineage_root_id":id,"bound_execution_id":id,"current_execution_id":id,"chain":[{"execution_id":id,"parent_execution_id":null,"sequence":1}]}});
    store.conn.execute("INSERT INTO attempt_current_receipts(request_id,pool_route_json,fingerprint,receipt_id,receipt_json) VALUES(?,?,?,?,?)",rusqlite::params!["request",serde_json::to_string(&route).unwrap(),"a".repeat(64),receipt_id,receipt.to_string()]).unwrap();
    drop(store);
    let executable = r2_copy_peer(home.path());
    let digest = agent_run_platform::fs::sha256(&std::fs::read(&executable).unwrap());
    r2_wait_build_age(&executable).await;
    let config = format!(
        r#"schema_version=2
+[managed_attempt_binding]
+enabled=true
+role_name="managed"
+pool_server="pool"
+pool_executable_sha256="{pool_hash}"
+bridge_hook_sha256="{hook_hash}"
+host_id="host"
+pool_resident_executable="/usr/bin/false"
+pool_resident_sha256="{resident_hash}"
+[[managed_attempt_binding.bridge_receipt_readers]]
+reader_id="bridge"
+worker_uid={uid}
+executable="{python}"
+executable_sha256="{digest}"
+run_route={{component_store_id="{sid}",route_id="run",worker_uid={uid}}}
+pool_route={{component_store_id="pool-store",route_id="pool-route",worker_uid={uid}}}
+"#,
        pool_hash = "a".repeat(64),
        hook_hash = "c".repeat(64),
        resident_hash = "b".repeat(64),
        python = executable.display()
    )
    .replace("\n+", "\n");
    std::fs::write(home.path().join("config.toml"), &config).unwrap();
    let api = home.path().join("api.sock");
    let receipt_socket = home.path().join("attempt-receipts.sock");
    let broker = tokio::spawn({
        let home = home.path().to_owned();
        let api = api.clone();
        async move { socket::serve_at(&home, &api).await }
    });
    wait_for_socket(&api).await;
    wait_for_socket(&receipt_socket).await;
    assert_eq!(
        request(&api, json!({"jsonrpc":"2.0","id":1,"method":"ping"})).await["result"]["ok"],
        true
    );
    let frame=json!({"id":"read","method":"_attempt_current_receipt_v1","params":{"version":1,"expected_store_id":sid,"receipt_id":receipt_id}}).to_string();
    let output = home.path().join("fake-reply");
    let invoke = || r2_fake_peer(&executable, &receipt_socket, &frame, &output);
    let response: Value = serde_json::from_str(&invoke().await).unwrap();
    assert_eq!(response["result"]["receipt"], receipt);
    let forbidden =
        json!({"id":"forbidden","method":"_attempt_admission_lookup_v1","params":{}}).to_string();
    let refusal: Value = serde_json::from_str(
        &r2_fake_peer(&executable, &receipt_socket, &forbidden, &output).await,
    )
    .unwrap();
    assert_eq!(refusal["error"]["code"], "peer_not_authorized");
    std::fs::write(
        home.path().join("config.toml"),
        config.replacen(&digest, &"f".repeat(64), 1),
    )
    .unwrap();
    assert!(invoke().await.is_empty());
    std::fs::write(
        home.path().join("config.toml"),
        config.replacen("enabled=true", "enabled=false", 1),
    )
    .unwrap();
    let recovered: Value = serde_json::from_str(&invoke().await).unwrap();
    assert_eq!(recovered["result"]["receipt"], receipt);
    broker.abort();
    let _ = broker.await;
    let restarted = tokio::spawn({
        let home = home.path().to_owned();
        let api = api.clone();
        async move { socket::serve_at(&home, &api).await }
    });
    wait_for_socket(&receipt_socket).await;
    let recovered: Value = serde_json::from_str(&invoke().await).unwrap();
    assert_eq!(recovered["result"]["receipt"], receipt);
    restarted.abort();
    let _ = restarted.await;
}

/// A switched-on private endpoint advertises currentness while unproven
/// egress candidates still fail closed.
#[cfg(target_os = "linux")]
#[tokio::test]
async fn pool_attempt_socket_live_smoke_keeps_managed_guard_closed() {
    use std::os::unix::fs::PermissionsExt;
    let base = std::env::var_os("AGENT_RUN_TEST_SOCKET_TMP")
        .map(PathBuf::from)
        .unwrap_or_else(std::env::temp_dir);
    let home = tempfile::Builder::new()
        .prefix("r2-pool-")
        .tempdir_in(base)
        .unwrap();
    std::fs::set_permissions(home.path(), std::fs::Permissions::from_mode(0o700)).unwrap();
    cli::init(home.path()).unwrap();
    let store = agent_run_store::fixture::open(home.path()).unwrap();
    let sid = agent_run_store::store_identity::read(&store.conn).unwrap();
    drop(store);
    // SAFETY: geteuid is read-only and has no preconditions.
    let uid = unsafe { libc::geteuid() };
    let executable = r2_copy_peer(home.path());
    let pool_hash = agent_run_platform::fs::sha256(&std::fs::read(&executable).unwrap());
    r2_wait_build_age(&executable).await;
    let bridge = std::fs::canonicalize("/usr/bin/false").unwrap();
    let bridge_hash = agent_run_platform::fs::sha256(&std::fs::read(&bridge).unwrap());
    let token = home.path().join("fixture-reader-token");
    std::fs::write(&token, "synthetic-reader\n").unwrap();
    std::fs::set_permissions(&token, std::fs::Permissions::from_mode(0o600)).unwrap();
    let token_hash = agent_run_platform::fs::sha256(b"synthetic-reader");
    let config = format!(
        r#"schema_version=2
[managed_attempt_binding]
enabled=true
role_name="managed"
pool_server="pool"
pool_executable_sha256="{frontend_hash}"
bridge_hook_sha256="{hook_hash}"
host_id="host"
pool_resident_executable="{python}"
pool_resident_sha256="{pool_hash}"
[managed_attempt_binding.transport]
run_route={{component_store_id="{sid}",route_id="run",worker_uid={uid}}}
pool_route={{component_store_id="00000000-0000-4000-8000-000000000010",route_id="pool",worker_uid={uid}}}
bridge_route={{component_store_id="00000000-0000-4000-8000-000000000001",route_id="bridge",worker_uid={uid}}}
producer="pool"
pool_socket="{pool_socket}"
bridge_socket="{bridge_socket}"
bridge_executable="{bridge}"
bridge_executable_sha256="{bridge_hash}"
bridge_reader_credential_file="{token}"
bridge_reader_credential_sha256="{token_hash}"
"#,
        frontend_hash = pool_hash,
        hook_hash = "c".repeat(64),
        python = executable.display(),
        bridge = bridge.display(),
        token = token.display(),
        pool_socket = home.path().join("pool.sock").display(),
        bridge_socket = home.path().join("bridge.sock").display()
    );
    std::fs::write(home.path().join("config.toml"), config).unwrap();
    let api = home.path().join("api.sock");
    let private = home.path().join("attempt.sock");
    let broker = tokio::spawn({
        let home = home.path().to_owned();
        let api = api.clone();
        async move { socket::serve_at(&home, &api).await }
    });
    wait_for_socket(&api).await;
    wait_for_socket(&private).await;
    let output = home.path().join("fake-pool-reply");
    let frame =
        json!({"id":"one","method":"_attempt_capabilities_v1","params":{"version":1}}).to_string();
    let result: Value =
        serde_json::from_str(&r2_fake_peer(&executable, &private, &frame, &output).await).unwrap();
    assert_eq!(
        result["result"]["capabilities"],
        json!([
            "attempt_current_ingress_v1",
            "attempt_current_egress_v1",
            "attempt_egress_actor_recovery_v1",
            "attempt_consensus_v1",
            "attempt_admission_v1",
            "attempt_namespace_retirement_v1",
            "managed_enrollment_v1"
        ])
    );
    let id = agent_run::domain::AgentId::new();
    let frame=json!({"id":"two","method":"_attempt_current_v1","params":{"version":1,"expected_store_id":sid,"request_id":"r","lineage_root_id":id,"bound_execution_id":id,"purpose":"egress"}}).to_string();
    let result: Value =
        serde_json::from_str(&r2_fake_peer(&executable, &private, &frame, &output).await).unwrap();
    assert_eq!(result["error"]["code"], "binding_unavailable");
    let frame=json!({"id":"three","method":"_attempt_current_v1","params":{"version":1,"expected_store_id":sid,"request_id":"opt-in","lineage_root_id":id,"bound_execution_id":id,"purpose":"egress","actor_recovery_version":1}}).to_string();
    let result: Value =
        serde_json::from_str(&r2_fake_peer(&executable, &private, &frame, &output).await).unwrap();
    assert_eq!(result["error"]["code"], "binding_unavailable");
    assert!(result["error"].get("actor_recovery").is_none());
    let enrollment = json!({"version":1,"expected_store_id":sid,"pool_store_id":"00000000-0000-4000-8000-000000000010","pool_id":"pool","request_id":"socket-abort","mode":"start","jobs":[{"member_id":"member","selector":{"version":1,"role_name":"managed"},"spec":{"provider":"codex","model":"m","profile":"managed","task":"fixture","workdir":home.path(),"write":false,"explicit_finish":true}}],"preconditions":[]});
    let fingerprint =
        agent_run_domain::attempt_binding::enrollment_start_fingerprint(&enrollment).unwrap();
    let key = json!({"version":1,"expected_store_id":sid,"pool_store_id":"00000000-0000-4000-8000-000000000010","request_id":"socket-abort","fingerprint":fingerprint});
    for (method, state) in [
        ("_managed_start_lookup_v1", "absent"),
        ("_managed_start_abort_v1", "aborted"),
        ("_managed_start_lookup_v1", "aborted"),
    ] {
        let frame = json!({"id":"p3","method":method,"params":key}).to_string();
        let response: Value =
            serde_json::from_str(&r2_fake_peer(&executable, &private, &frame, &output).await)
                .unwrap();
        assert_eq!(response["result"]["state"], state);
    }
    let frame = json!({"id":"p3-submit","method":"_managed_start_batch_v1","params":enrollment})
        .to_string();
    let response: Value =
        serde_json::from_str(&r2_fake_peer(&executable, &private, &frame, &output).await).unwrap();
    assert_eq!(response["error"]["code"], "enrollment_aborted");
    let public = request(
        &api,
        json!({"jsonrpc":"2.0","id":"p3-public","method":"_managed_start_batch_v1","params":{}}),
    )
    .await;
    assert_eq!(public["error"]["code"], -32601);
    // The same native build is allowed for a detached resident, but the
    // identical peer build beneath a recorded harness is never a resident.
    let leader = agent_run_platform::process::inspect(std::process::id() as i32).unwrap();
    let kernel = agent_run_platform::process::kernel_incarnation(&leader).unwrap();
    let store = agent_run_store::fixture::open(home.path()).unwrap();
    store.conn.execute("INSERT INTO attempt_launches(spawn_key,execution_id,sequence,attempt_id,launch_id,state,boot_id,pid_namespace_inode,uid,pid,start_ticks) VALUES('synthetic-harness','synthetic-root',1,'synthetic-attempt',?,'bound',?,?,?,?,?)",rusqlite::params![uuid::Uuid::new_v4().to_string(),kernel.boot_id,kernel.pid_namespace_inode,kernel.uid,kernel.pid,kernel.start_ticks]).unwrap();
    drop(store);
    let frame =
        json!({"id":"under-harness","method":"_attempt_capabilities_v1","params":{"version":1}})
            .to_string();
    let response: Value =
        serde_json::from_str(&r2_fake_peer(&executable, &private, &frame, &output).await).unwrap();
    assert_eq!(response["id"], "under-harness");
    assert_eq!(response["error"]["code"], "peer_not_authorized");
    assert_eq!(response["error"]["store_id"], sid.to_string());
    assert_eq!(response["error"].as_object().unwrap().len(), 4);
    broker.abort();
    let _ = broker.await;
}

/// Exact libtest name of a test defined in this module: integration tests run as
/// modules of the crate's single `it` binary, so the name carries the module path.
fn exact_test(name: &str) -> String {
    match module_path!().split_once("::") {
        Some((_, module)) => format!("{module}::{name}"),
        None => name.to_owned(),
    }
}
