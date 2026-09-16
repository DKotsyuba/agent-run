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
        let config=format!("schema_version=1\n[runtimes.mock]\nenabled=true\nadapter='claude'\nbinary={}\nhome={}\nmodels=['fixture']\nlimits_source='none'\n",toml::Value::String(env!("CARGO_BIN_EXE_agent-run-fixture").into()),toml::Value::String(home.join("runtime").to_string_lossy().into_owned()));
        std::fs::write(home.join("config.toml"), config).unwrap();
        std::fs::create_dir(home.join("profiles")).unwrap();
        std::fs::write(home.join("profiles/review.md"), "Review.\n").unwrap();
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
                "--runtime",
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
