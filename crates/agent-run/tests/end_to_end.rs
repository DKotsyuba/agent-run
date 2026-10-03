#![cfg(feature = "test-fixtures")]
//! These tests launch only the feature-gated local fake engine, never providers.
use agent_run::{
    domain::{AgentId, Status},
    service::Service,
    state::Store,
    transport::socket,
};
use serde_json::{json, Value};
use std::{
    path::PathBuf,
    process::{Child, Command, Stdio},
    time::{Duration, Instant},
};

struct Harness {
    temp: tempfile::TempDir,
    home: PathBuf,
    broker: Option<Child>,
}
impl Harness {
    /// Builds a broker fixture with the explicitly provisioned profile Python init omits.
    fn new() -> Self {
        let temp = tempfile::Builder::new()
            .prefix("ar-")
            // Short base keeps the Unix socket path under the platform limit.
            .tempdir_in(std::env::var_os("AGENT_RUN_TEST_TMP").unwrap_or_else(|| "/tmp".into()))
            .unwrap();
        let home = temp.path().canonicalize().unwrap();
        let output = Command::new(env!("CARGO_BIN_EXE_agent-run"))
            .args(["--home", home.to_str().unwrap(), "init"])
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        // One schema-2 provider on the fake engine behind a synthetic env account.
        let binary = toml::Value::String(env!("CARGO_BIN_EXE_agent-run-fixture").into());
        let config = format!(
            "schema_version=2\n[harnesses.codex]\nbinary={binary}\nhome={codex}\n[harnesses.claude-code]\nbinary={binary}\nhome={claude}\n[providers.mock]\nharness='claude-code'\nconnection={{kind='custom',endpoint='https://gateway.example/api',protocol='messages'}}\nauth_family='anthropic'\nlimits_source='none'\n[[providers.mock.models]]\nid='fixture'\n[[providers.mock.bindings]]\nlabel='work'\naccount='acct-work'\n",
            codex = toml::Value::String(home.join("codex").to_string_lossy().into_owned()),
            claude = toml::Value::String(home.join("claude").to_string_lossy().into_owned()),
        );
        std::fs::write(home.join("config.toml"), config).unwrap();
        std::fs::create_dir_all(home.join("profiles")).unwrap();
        std::fs::write(home.join("profiles/review.md"), "+++\nrevision = \"1\"\nwrite = false\nnetwork = false\nallow_external_read_roots = false\nskills = []\nmcp = []\nrequired_constraints = []\n+++\nReview.\n").unwrap();
        let registered = Command::new(env!("CARGO_BIN_EXE_agent-run"))
            .args([
                "--home",
                home.to_str().unwrap(),
                "accounts",
                "register",
                "--id",
                "acct-work",
                "--auth-family",
                "anthropic",
                "--reference",
                "env:FAKE_TOKEN",
            ])
            .output()
            .unwrap();
        assert!(
            registered.status.success(),
            "{}",
            String::from_utf8_lossy(&registered.stderr)
        );
        Self {
            temp,
            home,
            broker: None,
        }
    }
    async fn start_broker(&mut self) {
        let error = std::fs::File::create(self.home.join("broker-test.stderr")).unwrap();
        self.broker = Some(
            Command::new(env!("CARGO_BIN_EXE_agent-run"))
                .arg("--home")
                .arg(&self.home)
                .args(["api", "serve"])
                .env("HOME", &self.home)
                .env("FAKE_TOKEN", "synthetic-token")
                .stdin(Stdio::null())
                .stdout(Stdio::null())
                .stderr(error)
                .spawn()
                .unwrap(),
        );
        let deadline = Instant::now() + Duration::from_secs(10);
        loop {
            if socket::client(&self.home, "ping", json!({})).await.is_ok() {
                break;
            }
            assert!(
                Instant::now() < deadline,
                "broker failed to become available: {}",
                std::fs::read_to_string(self.home.join("broker-test.stderr")).unwrap_or_default()
            );
            tokio::time::sleep(Duration::from_millis(30)).await;
        }
    }
    fn submit_cli(&self, task: &str) -> AgentId {
        let out = Command::new(env!("CARGO_BIN_EXE_agent-run"))
            .arg("--home")
            .arg(&self.home)
            .args([
                "start",
                "--provider",
                "mock",
                "--model",
                "fixture",
                "--profile",
                "review",
                "--task",
                task,
                "--workdir",
            ])
            .arg(&self.home)
            .output()
            .unwrap();
        assert!(
            out.status.success(),
            "{}",
            String::from_utf8_lossy(&out.stderr)
        );
        let value: Value = serde_json::from_slice(&out.stdout).unwrap();
        serde_json::from_value(value["agent_id"].clone()).unwrap()
    }
    fn stop_broker(&mut self) {
        if let Some(mut child) = self.broker.take() {
            let _ = child.kill();
            let _ = child.wait();
        }
    }
    async fn terminal(&self, id: &AgentId) -> Value {
        let deadline = Instant::now() + Duration::from_secs(20);
        let service = Service::new(self.home.clone());
        loop {
            let row = Store::open(&self.home).unwrap().get(id).unwrap();
            if row.status.terminal() {
                return service.answer(id).unwrap();
            }
            assert!(Instant::now() < deadline, "agent did not terminate: {}", id);
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }
}
impl Drop for Harness {
    fn drop(&mut self) {
        // Request cancellation while the temporary state still exists. Never
        // blindly signal a persisted PID, which could have been reused.
        if let Ok(store) = Store::open(&self.home) {
            if let Ok((rows, _)) = store.list(true, 0, 1000, None) {
                let service = Service::new(self.home.clone());
                for row in rows {
                    let _ = service.cancel(&row.id);
                }
            }
        }
        self.stop_broker();
        let deadline = Instant::now() + Duration::from_secs(8);
        while Instant::now() < deadline {
            let active = Store::open(&self.home)
                .and_then(|s| s.list(true, 0, 1000, None))
                .map(|(_, n)| n)
                .unwrap_or(0);
            if active == 0 {
                break;
            }
            std::thread::sleep(Duration::from_millis(50));
        }
        let _ = &self.temp;
    }
}
/// A real supervisor injects a worker-only MCP capability, the fake harness
/// reports through the real stdio/socket boundaries, and the run stays active.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn worker_mcp_queues_a_report_without_ending_the_run() {
    let mut h = Harness::new();
    h.start_broker().await;
    let result = socket::client(
        &h.home,
        "start",
        json!({
            "provider":"mock","model":"fixture","profile":"review",
            "task":"fixture:worker-notify","workdir":h.home,"timeout_seconds":20,
            "orchestrator":{"transport":"codex_queue","external_session_id":"fixture-worker-thread"}
        }),
    )
    .await
    .unwrap();
    let id: AgentId = serde_json::from_value(result["agent_id"].clone()).unwrap();
    let deadline = Instant::now() + Duration::from_secs(10);
    while !h.home.join("worker-receipt.json").exists() {
        let row = Store::open(&h.home).unwrap().get(&id).unwrap();
        assert!(
            !row.status.terminal(),
            "worker exited before reporting: {:?}",
            row.failure_text
        );
        assert!(Instant::now() < deadline, "missing worker receipt");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    let store = Store::open(&h.home).unwrap();
    assert_eq!(store.get(&id).unwrap().status, Status::Running);
    let (count, message): (i64, String) = store
        .conn
        .query_row(
            "SELECT count(*),message FROM worker_notifications WHERE agent_id=?",
            [id.as_str()],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .unwrap();
    assert_eq!(count, 1);
    assert_eq!(message, "Fixture material finding");
    let receipt = std::fs::read_to_string(h.home.join("worker-receipt.json")).unwrap();
    assert!(receipt.contains("Queue acknowledgement only"));
    assert!(!receipt.contains("AGENT_RUN_WORKER_TOKEN"));
    std::fs::write(h.home.join("worker-continue"), "").unwrap();
    let answer = h.terminal(&id).await;
    assert_eq!(answer["status"], "succeeded");
    assert_eq!(
        store
            .conn
            .query_row(
                "SELECT count(*) FROM deliveries WHERE agent_id=?",
                [id.as_str()],
                |row| row.get::<_, i64>(0)
            )
            .unwrap(),
        2
    );
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cli_exit_and_broker_restart_do_not_cancel_admitted_job() {
    let mut h = Harness::new();
    h.start_broker().await;
    let id = h.submit_cli("fixture:slow");
    h.stop_broker();
    let answer = h.terminal(&id).await;
    assert_eq!(answer["status"], "succeeded");
    assert_eq!(answer["content"], "fixture final answer\n");
    h.start_broker().await;
    let result = socket::client(&h.home, "answer", json!({"agent_id":id}))
        .await
        .unwrap();
    assert_eq!(result["sha256"], answer["sha256"]);
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cancellation_stops_a_hanging_engine_and_records_terminal_state() {
    let mut h = Harness::new();
    h.start_broker().await;
    let id = h.submit_cli("fixture:hang");
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        if Store::open(&h.home).unwrap().get(&id).unwrap().status == Status::Running {
            break;
        }
        assert!(Instant::now() < deadline);
        tokio::time::sleep(Duration::from_millis(30)).await;
    }
    socket::client(&h.home, "cancel", json!({"agent_id":id}))
        .await
        .unwrap();
    let answer = h.terminal(&id).await;
    assert_eq!(answer["status"], "cancelled");
    assert_eq!(answer["available"], false);
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn zero_exit_without_terminal_result_is_not_success() {
    let mut h = Harness::new();
    h.start_broker().await;
    let id = h.submit_cli("fixture:missing-result");
    let answer = h.terminal(&id).await;
    assert_eq!(answer["status"], "failed");
    assert_eq!(answer["available"], false);
    let transcript = socket::client(&h.home, "transcript", json!({"agent_id":id}))
        .await
        .unwrap();
    assert!(!transcript["messages"].as_array().unwrap().is_empty());
}

/// One `agents --follow` viewer child, killed and reaped on drop even when an
/// assertion panics, so no viewer outlives its test.
struct Viewer(Child);
impl Drop for Viewer {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

/// Spawns the real `agent-run agents --follow` binary against `home` and
/// records its NDJSON snapshots until it exits or a 30 s reader TTL ends.
fn spawn_follow_viewer(
    home: &std::path::Path,
) -> (Viewer, std::sync::Arc<std::sync::Mutex<Vec<Value>>>) {
    let mut child = Command::new(env!("CARGO_BIN_EXE_agent-run"))
        .arg("--home")
        .arg(home)
        .args(["agents", "--follow", "--limit", "5"])
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()
        .expect("follow viewer starts");
    let snapshots = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
    let recorded = std::sync::Arc::clone(&snapshots);
    let stdout = child.stdout.take().expect("viewer stdout");
    std::thread::spawn(move || {
        let deadline = Instant::now() + Duration::from_secs(30);
        for line in std::io::BufRead::lines(std::io::BufReader::new(stdout)) {
            if Instant::now() > deadline {
                break;
            }
            let Ok(line) = line else { break };
            if let Ok(value) = serde_json::from_str(&line) {
                recorded.lock().unwrap().push(value);
            }
        }
    });
    (Viewer(child), snapshots)
}

/// Waits until a recorded snapshot satisfies `predicate`, bounded to 12 s.
async fn wait_for_snapshot(
    snapshots: &std::sync::Arc<std::sync::Mutex<Vec<Value>>>,
    predicate: impl Fn(&Value) -> bool,
) -> Value {
    let deadline = Instant::now() + Duration::from_secs(12);
    loop {
        if let Some(found) = snapshots
            .lock()
            .unwrap()
            .iter()
            .rev()
            .find(|page| predicate(page))
            .cloned()
        {
            return found;
        }
        assert!(
            Instant::now() < deadline,
            "no matching follow snapshot in {:?}",
            snapshots.lock().unwrap()
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

/// The real follow binary, driven through the real broker and the fake
/// engine's marker-controlled lifecycle, proves change delivery: each viewer
/// first observes the settled baseline, the marker-released tool-count and
/// terminal changes then arrive on that same viewer, a store event that
/// changes no displayed fact never re-emits a page, and interrupting a
/// viewer never cancels the agent.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn agents_follow_binary_tracks_the_real_lifecycle_without_duplicates() {
    let mut harness = Harness::new();
    harness.start_broker().await;
    let id = harness.submit_cli("fixture:follow-tools");

    // First viewer, started before any marker: its baseline page predates
    // every change this test releases.
    let (mut first, seen_first) = spawn_follow_viewer(&harness.home);
    let baseline = wait_for_snapshot(&seen_first, |page| {
        page["items"][0]["status"] == "running"
            && page["items"][0]["tool_counts"]["calls"] == 0
            && page["items"][0]["tool_counts"]["failed"] == 0
    })
    .await;
    assert_eq!(baseline["items"][0]["agent_id"], id.as_str());

    // Release the tool phase and observe the native-count change arrive.
    std::fs::write(harness.home.join("follow-tools"), b"").unwrap();
    let tool_page = wait_for_snapshot(&seen_first, |page| {
        page["items"][0]["status"] == "running"
            && page["items"][0]["tool_counts"]["calls"] == 1
            && page["items"][0]["tool_counts"]["failed"] == 0
    })
    .await;
    assert_eq!(tool_page["items"][0]["tool_counts"]["unknown_results"], 0);

    // A store event that changes no displayed fact must not re-emit a page:
    // the wake rebuilds the page with fresh observation timestamps only.
    let settled = {
        let store = Store::open(&harness.home).unwrap();
        store
            .event(&id, "fixture_follow_probe", &serde_json::json!({}))
            .unwrap();
        seen_first.lock().unwrap().len()
    };
    tokio::time::sleep(Duration::from_millis(1500)).await;
    assert_eq!(
        seen_first.lock().unwrap().len(),
        settled,
        "an unchanged page was re-emitted after the probe event"
    );

    // Interrupting the viewer leaves the supervised agent running.
    // SAFETY: the signal targets only the owned viewer child process.
    unsafe {
        libc::kill(first.0.id() as libc::pid_t, libc::SIGINT);
    }
    let deadline = Instant::now() + Duration::from_secs(5);
    while !matches!(first.0.try_wait(), Ok(Some(_))) {
        assert!(Instant::now() < deadline, "viewer ignored SIGINT");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert_eq!(
        Store::open(&harness.home).unwrap().get(&id).unwrap().status,
        Status::Running,
        "the viewer never cancels the agent"
    );

    // Second viewer, also started before its marker: its baseline is the
    // settled tool state, and only then is the terminal change released.
    let (mut second, seen_second) = spawn_follow_viewer(&harness.home);
    let running = wait_for_snapshot(&seen_second, |page| {
        page["items"][0]["status"] == "running"
            && page["items"][0]["tool_counts"]["calls"] == 1
            && page["items"][0]["tool_counts"]["failed"] == 0
    })
    .await;
    assert_eq!(running["items"][0]["agent_id"], id.as_str());
    std::fs::write(harness.home.join("follow-release"), b"").unwrap();
    let terminal_page = wait_for_snapshot(&seen_second, |page| {
        page["items"][0]["status"] == "succeeded" && page["items"][0]["answer_available"] == true
    })
    .await;
    assert_eq!(terminal_page["items"][0]["agent_id"], id.as_str());
    // SAFETY: the signal targets only the owned viewer child process.
    unsafe {
        libc::kill(second.0.id() as libc::pid_t, libc::SIGINT);
    }
    let deadline = Instant::now() + Duration::from_secs(5);
    while !matches!(second.0.try_wait(), Ok(Some(_))) {
        assert!(Instant::now() < deadline, "second viewer ignored SIGINT");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    let answer = harness.terminal(&id).await;
    assert_eq!(answer["content"], "fixture final answer\n");
}

/// A finite real-broker pool run: two members use the private worker catalog to
/// read, propose and vote, end successfully with cleanup, and the broker
/// completes the pool exactly once, delivering one common notice through the
/// bound Desktop relay and keeping the completed record frozen and closed.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn scripted_pool_completes_with_one_correlated_common_notice() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let mut h = Harness::new();
    h.start_broker().await;
    let listener = tokio::net::UnixListener::bind(h.home.join("ar-cdx-v4-pool.sock")).unwrap();
    // The bound members' own terminal notices are deliberately additive, so the
    // finite relay acknowledges every notice and records them all; the pool's
    // one common notice is then picked out by its operation. The task is
    // aborted by the guard on every exit path.
    struct Relay(tokio::task::JoinHandle<()>);
    impl Drop for Relay {
        fn drop(&mut self) {
            self.0.abort();
        }
    }
    let received = std::sync::Arc::new(std::sync::Mutex::new(Vec::<Value>::new()));
    let sink = received.clone();
    let _relay = Relay(tokio::spawn(async move {
        loop {
            let (mut stream, _) = listener.accept().await.unwrap();
            let length = stream.read_u32_le().await.unwrap() as usize;
            let mut data = vec![0; length];
            stream.read_exact(&mut data).await.unwrap();
            sink.lock()
                .unwrap()
                .push(serde_json::from_slice(&data).unwrap());
            let reply = br#"{"outcome":"accepted"}"#;
            stream.write_u32_le(reply.len() as u32).await.unwrap();
            stream.write_all(reply).await.unwrap();
        }
    }));
    let member = |role: &str, task: &str| {
        json!({"role": role, "start": {
            "provider":"mock","model":"fixture","profile":"review","task":task,
            "workdir":h.home,"timeout_seconds":40}})
    };
    let started = socket::client(
        &h.home,
        "start_pool",
        json!({
            "request_id": "scripted-pool", "goal": "ship the fixture result",
            "orchestrator": {"transport":"codex_queue","external_session_id":"fixture-pool-thread"},
            "members": [member("lead","fixture:pool-script lead"), member("peer","fixture:pool-script peer")]
        }),
    )
    .await
    .unwrap();
    assert_eq!(started["bound"], true);
    let pool_id = started["pool_id"].as_str().unwrap().to_owned();
    let ids: Vec<AgentId> = started["members"]
        .as_array()
        .unwrap()
        .iter()
        .map(|m| serde_json::from_value(m["agent_id"].clone()).unwrap())
        .collect();
    for id in &ids {
        let answer = h.terminal(id).await;
        let row = Store::open(&h.home).unwrap().get(id).unwrap();
        assert_eq!(
            answer["status"], "succeeded",
            "{:?} {:?}",
            row.failure_kind, row.failure_text
        );
    }
    // Two member notices and exactly one common notice, all acknowledged.
    let relay_deadline = Instant::now() + Duration::from_secs(30);
    let request = loop {
        let seen = received.lock().unwrap().clone();
        let pool_notices: Vec<_> = seen
            .iter()
            .filter(|r| r["op"] == "pool_completion")
            .collect();
        let member_notices = seen.iter().filter(|r| r["op"] == "completion").count();
        if pool_notices.len() == 1 && member_notices == 2 {
            break pool_notices[0].clone();
        }
        assert!(
            pool_notices.len() <= 1 && member_notices <= 2 && Instant::now() < relay_deadline,
            "relay saw {} pool and {member_notices} member notices",
            pool_notices.len()
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    };
    assert_eq!(request["op"], "pool_completion");
    assert_eq!(request["pool_id"], pool_id.as_str());
    let message = request["message"].as_str().unwrap();
    assert!(message.contains("commit abc123") && message.contains("ship the fixture result"));
    assert!(request.get("agent_id").is_none() && request.get("run_id").is_none());
    let deadline = Instant::now() + Duration::from_secs(15);
    let store = loop {
        let store = Store::open(&h.home).unwrap();
        let state: String = store
            .conn
            .query_row(
                "SELECT d.state FROM pools p JOIN deliveries d ON d.id=p.completion_delivery_id",
                [],
                |row| row.get(0),
            )
            .unwrap_or_default();
        if state == "delivered" {
            break store;
        }
        assert!(Instant::now() < deadline, "notice not delivered: {state}");
        tokio::time::sleep(Duration::from_millis(50)).await;
    };
    let count = |sql: &str| -> i64 { store.conn.query_row(sql, [], |r| r.get(0)).unwrap() };
    assert_eq!(
        count("SELECT COUNT(*) FROM events WHERE kind='pool_completed'"),
        1
    );
    assert_eq!(
        count("SELECT COUNT(*) FROM pools WHERE state='completed'"),
        1
    );
    // The closed pool reads frozen and refuses further operator writes.
    let page = socket::client(&h.home, "pool", json!({"pool_id": pool_id}))
        .await
        .unwrap();
    assert_eq!(page["status"]["state"], "completed");
    assert!(socket::client(
        &h.home,
        "pool_post",
        json!({"pool_id": pool_id, "request_id": "late", "message": "too late"})
    )
    .await
    .is_err());
}
