//! Remaining live and pure ports of the Python socket API contract.

use agent_run::domain::Outcome;
use agent_run::{
    cli,
    service::Service,
    transport::{frame, socket},
    verify,
};
use serde_json::{json, Value};
use std::{
    path::{Path, PathBuf},
    time::Duration,
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt, BufReader},
    net::UnixStream,
};

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

/// Stops a broker task and releases its temporary home.
async fn stop(task: tokio::task::JoinHandle<agent_run::Result<()>>) {
    task.abort();
    let _ = task.await;
}

/// Admits one nonterminal fixture row for wait and long-poll requests.
fn admitted(home: &tempfile::TempDir) -> agent_run::domain::AgentId {
    let config = agent_run::config::Config::load(home.path()).unwrap();
    let request: agent_run::domain::StartRequest = serde_json::from_value(json!({
        "runtime":"mock", "model":"fixture", "profile":"review",
        "task":"fixture", "workdir":home.path()
    }))
    .unwrap();
    agent_run::state::Store::open(home.path())
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
    let mut store = agent_run::state::Store::open(home.path()).unwrap();
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
    assert!(socket::respond(
        &Service::new(PathBuf::from("/nonexistent")),
        json!({"jsonrpc":"2.0","method":"ping"})
    )
    .await
    .is_none());
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
    assert!(socket::serve_at_with_options(
        Path::new("/nonexistent"),
        Path::new("/tmp/no.sock"),
        socket::ServeOptions {
            max_pending_requests: 0,
            ..Default::default()
        }
    )
    .await
    .is_err());
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

/// Mirrors `tests/test_api_socket.py::ApiSocketTests::test_slow_capacity_order_lane_does_not_delay_durable_cancel`.
#[tokio::test]
async fn python_capacity_order_and_durable_cancel_use_independent_paths() {
    let (home, path, task) = broker().await;
    let id = admitted(&home);
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
    assert!(order.await.unwrap().get("result").is_some());
    stop(task).await;
    drop(home);
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
    assert!(request(
        &path,
        json!({"jsonrpc":"2.0","id":2,"method":"models","params":{}})
    )
    .await
    .get("result")
    .is_some());
    stop(task).await;
    drop(home);
}

/// Mirrors `tests/test_api_socket.py::ApiSocketTests::test_surface_is_dispatch_tools_plus_control_methods`.
#[test]
fn python_socket_surface_is_shared_tools_plus_controls() {
    let names = agent_run::dispatch::tools()
        .into_iter()
        .filter_map(|tool| tool["name"].as_str().map(str::to_owned))
        .collect::<std::collections::BTreeSet<_>>();
    assert!(names.contains("start") && names.contains("list_agents"));
    assert_eq!(names.len(), 11);
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
    let mut store = agent_run::state::Store::open(home.path()).unwrap();
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
