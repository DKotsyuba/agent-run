//! Synthetic socket and process checks for execution-fenced operator answers.

use agent_run::{
    domain::AgentId,
    service::Service,
    transport::{frame, socket},
};
use agent_run_platform::{fs, verify};
use serde_json::{Value, json};
use std::{
    path::{Path, PathBuf},
    time::Duration,
};
use tokio::{io::BufReader, net::UnixListener, process::Command, task::JoinHandle};

/// Owns an isolated synthetic store and an absent operator home.
struct Fixture {
    temp: tempfile::TempDir,
    home: PathBuf,
    socket: PathBuf,
    root: AgentId,
    child: AgentId,
}

impl Fixture {
    /// Creates two synthetic executions in one lineage without starting an engine.
    fn new() -> Self {
        let temp = tempfile::Builder::new().prefix("oa-").tempdir().unwrap();
        let home = temp.path().join("service");
        fs::private_dir(&home).unwrap();
        std::fs::write(home.join("config.toml"), format!(
            "schema_version=1\n[runtimes.mock]\nenabled=true\nadapter='claude'\nbinary='/bin/true'\nhome={}\nmodels=['fixture']\nlimits_source='none'\n",
            toml::Value::String(home.join("mock").to_string_lossy().into_owned())
        )).unwrap();
        let config = agent_run::config::Config::load(&home).unwrap();
        agent_run_store::fixture::initialize(&home).unwrap();
        let mut store = agent_run_store::fixture::open(&home).unwrap();
        let mut request: agent_run_domain::domain::StartRequest = serde_json::from_value(json!({
            "runtime":"mock","model":"fixture","profile":"review","task":"fixture","workdir":home
        }))
        .unwrap();
        request.validate().unwrap();
        let (root, _) = store.admit(&request, &config, &json!({}), None).unwrap();
        let (child, _) = store.admit(&request, &config, &json!({}), None).unwrap();
        store
            .conn
            .execute(
                "UPDATE agents SET root_agent_id=?1,parent_agent_id=?1,sequence=2 WHERE id=?2",
                rusqlite::params![root.as_str(), child.as_str()],
            )
            .unwrap();
        drop(store);
        let socket = temp.path().join("api.sock");
        Self {
            temp,
            home,
            socket,
            root,
            child,
        }
    }

    /// Stores a sealed modern answer or a correctly framed historical legacy answer.
    fn answer(&self, id: &AgentId, text: &str, legacy: bool) {
        let root = self.home.join("agents").join(id.as_str());
        fs::private_dir(&root).unwrap();
        let proof = if legacy {
            let path = root.join("answer.md");
            let mut bytes = text.as_bytes().to_vec();
            bytes.extend_from_slice(verify::LEGACY_FRAME);
            std::fs::write(&path, &bytes).unwrap();
            verify::Proof {
                path,
                bytes: bytes.len() as u64,
                sha256: fs::sha256(&bytes),
                proof_version: 1,
            }
        } else {
            verify::seal(&root, Path::new("answer.md"), text).unwrap()
        };
        agent_run_store::fixture::open(&self.home)
            .unwrap()
            .conn
            .execute(
                "UPDATE agents SET answer_path=?1,answer_bytes=?2,answer_sha256=?3 WHERE id=?4",
                rusqlite::params![
                    proof.path.to_str().unwrap(),
                    proof.bytes,
                    proof.sha256,
                    id.as_str()
                ],
            )
            .unwrap();
    }

    /// Starts only an in-process framed dispatcher on an isolated same-UID socket.
    fn broker(&self) -> FakeSocket {
        let listener = UnixListener::bind(&self.socket).unwrap();
        let service = Service::new(self.home.clone());
        FakeSocket(tokio::spawn(async move {
            loop {
                let (stream, _) = listener.accept().await.unwrap();
                let (input, mut output) = stream.into_split();
                let raw = frame::read(&mut BufReader::new(input), socket::MAX_FRAME)
                    .await
                    .unwrap()
                    .unwrap();
                let request: Value = serde_json::from_slice(&raw).unwrap();
                let response = socket::respond(&service, request).await.unwrap();
                frame::write(&mut output, &response, socket::MAX_FRAME)
                    .await
                    .unwrap();
            }
        }))
    }

    /// Runs the real CLI in local or operator mode with a bounded process deadline.
    async fn cli(&self, operator: bool, run_id: Option<&AgentId>) -> std::process::Output {
        let mut command = Command::new(env!("CARGO_BIN_EXE_agent-run"));
        command.kill_on_drop(true);
        command.arg("--home").arg(if operator {
            self.temp.path().join("absent")
        } else {
            self.home.clone()
        });
        if operator {
            // SAFETY: geteuid reads only the identity of the fixture process.
            let uid = unsafe { libc::geteuid() };
            command
                .arg("--broker-socket")
                .arg(&self.socket)
                .arg("--broker-uid")
                .arg(uid.to_string());
        }
        command.arg("answer").arg(self.root.as_str());
        if let Some(run_id) = run_id {
            command.arg("--run-id").arg(run_id.as_str());
        }
        command
            .env("HOME", self.temp.path().join("caller-home"))
            .env("AGENT_RUN_HOME", self.temp.path().join("env-home"));
        tokio::time::timeout(Duration::from_secs(20), command.output())
            .await
            .unwrap()
            .unwrap()
    }
}

/// Aborts the test-owned socket task on both success and assertion failure.
struct FakeSocket(JoinHandle<()>);
impl Drop for FakeSocket {
    fn drop(&mut self) {
        self.0.abort();
    }
}

/// Latest and explicit historical selections reproduce the ordinary CLI, including legacy framing.
#[tokio::test]
async fn operator_answer_matches_local_and_historical_legacy() {
    let fixture = Fixture::new();
    fixture.answer(&fixture.root, "historical é", true);
    fixture.answer(&fixture.child, "current answer", false);
    let mut socket = fixture.broker();
    for run in [None, Some(&fixture.root)] {
        let local = fixture.cli(false, run).await;
        let remote = fixture.cli(true, run).await;
        assert!(
            local.status.success(),
            "{}",
            String::from_utf8_lossy(&local.stderr)
        );
        assert!(
            remote.status.success(),
            "{}",
            String::from_utf8_lossy(&remote.stderr)
        );
        assert_eq!(remote.stdout, local.stdout);
        let value: Value = serde_json::from_slice(&remote.stdout).unwrap();
        assert_eq!(
            value["content"],
            if run.is_some() {
                "historical é"
            } else {
                "current answer"
            }
        );
    }
    assert!(!fixture.temp.path().join("absent").exists());
    socket.0.abort();
    assert!((&mut socket.0).await.unwrap_err().is_cancelled());
}

/// Reconstructs escaped data that fits the CLI exactly but exceeds one wrapped RPC frame.
#[tokio::test]
async fn operator_answer_reassembles_large_escaped_inline_result() {
    let fixture = Fixture::new();
    fixture.answer(&fixture.child, "x", false);
    let service = Service::new(fixture.home.clone());
    let mut sample = service
        .public_run_result(&fixture.child, service.answer(&fixture.child).unwrap())
        .unwrap();
    sample["content"] = json!("");
    let base = serde_json::to_vec(&sample).unwrap().len();
    // A multibyte character begins at the end of the first 128 KiB raw page.
    let prefix = format!(
        "{}é🦀",
        "\0".repeat(agent_run_domain::observation::MAX_CHUNK_BYTES - 1)
    );
    let escaped_prefix = serde_json::to_vec(&prefix).unwrap().len() - 2;
    let remaining = socket::MAX_FRAME - 40 - base - escaped_prefix;
    let text = format!(
        "{prefix}{}{}",
        "\0".repeat(remaining / 6),
        "x".repeat(remaining % 6)
    );
    fixture.answer(&fixture.child, &text, false);
    let mut socket = fixture.broker();
    let local = fixture.cli(false, None).await;
    assert!(
        local.status.success(),
        "{}",
        String::from_utf8_lossy(&local.stderr)
    );
    assert!(local.stdout.len() <= socket::MAX_FRAME);
    let answer: Value = serde_json::from_slice(&local.stdout).unwrap();
    let rpc = json!({"jsonrpc":"2.0","id":"x".repeat(36),"result":answer});
    assert!(serde_json::to_vec(&rpc).unwrap().len() + 1 > socket::MAX_FRAME);
    let remote = fixture.cli(true, None).await;
    assert!(
        remote.status.success(),
        "{}",
        String::from_utf8_lossy(&remote.stderr)
    );
    assert_eq!(remote.stdout, local.stdout);
    let value: Value = serde_json::from_slice(&remote.stdout).unwrap();
    assert_eq!(value["content"].as_str().unwrap(), text);

    // The operator path retains the local CLI's own encoded-output limit.
    fixture.answer(&fixture.child, &"\0".repeat(200_000), false);
    let local = fixture.cli(false, None).await;
    let remote = fixture.cli(true, None).await;
    assert!(!local.status.success());
    assert_eq!(remote.status.code(), local.status.code());
    assert!(local.stdout.is_empty() && remote.stdout.is_empty());
    assert_eq!(remote.stderr, local.stderr);

    // Raw artifacts above the historical inline cutoff retain metadata only.
    fixture.answer(
        &fixture.child,
        &"x".repeat(verify::INLINE_ANSWER + 1),
        false,
    );
    let local = fixture.cli(false, None).await;
    let remote = fixture.cli(true, None).await;
    assert!(local.status.success() && remote.status.success());
    assert_eq!(remote.stdout, local.stdout);
    assert!(serde_json::from_slice::<Value>(&remote.stdout).unwrap()["content"].is_null());
    assert!(!fixture.temp.path().join("absent").exists());
    socket.0.abort();
    assert!((&mut socket.0).await.unwrap_err().is_cancelled());
}
