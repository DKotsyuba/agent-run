//! Socket-backed transcript parity over an isolated synthetic journal.

use agent_run::{
    domain::AgentId,
    service::Service,
    transport::{frame, socket},
};
use agent_run_platform::fs;
use serde_json::{Value, json};
use std::{path::PathBuf, process::Stdio, time::Duration};
use tokio::{
    io::{AsyncReadExt, BufReader},
    net::UnixListener,
    process::Command,
    task::JoinHandle,
};

struct Fixture {
    temp: tempfile::TempDir,
    home: PathBuf,
    socket: PathBuf,
    root: AgentId,
    child: AgentId,
}

impl Fixture {
    fn new() -> Self {
        let temp = tempfile::Builder::new().prefix("ot-").tempdir().unwrap();
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
            "runtime":"mock", "model":"fixture", "profile":"review", "task":"fixture", "workdir":home
        })).unwrap();
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

    fn message(&self, id: &AgentId, text: &str, raw_ref: &str) {
        agent_run_store::fixture::open(&self.home)
            .unwrap()
            .message(id, "assistant", text, None, Some(raw_ref))
            .unwrap();
    }

    fn broker(&self) -> Broker {
        let listener = UnixListener::bind(&self.socket).unwrap();
        let service = Service::new(self.home.clone());
        Broker(tokio::spawn(async move {
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

    fn command(&self, operator: bool, args: &[&str]) -> Command {
        let mut command = Command::new(env!("CARGO_BIN_EXE_agent-run"));
        command.kill_on_drop(true).arg("--home").arg(if operator {
            self.temp.path().join("absent")
        } else {
            self.home.clone()
        });
        if operator {
            // SAFETY: geteuid only reads this fixture process's identity.
            let uid = unsafe { libc::geteuid() };
            command
                .arg("--broker-socket")
                .arg(&self.socket)
                .arg("--broker-uid")
                .arg(uid.to_string());
        }
        command
            .arg("transcript")
            .arg(self.root.as_str())
            .args(args)
            .env("HOME", self.temp.path().join("caller-home"))
            .env("AGENT_RUN_HOME", self.temp.path().join("env-home"));
        command
    }

    async fn output(&self, operator: bool, args: &[&str]) -> std::process::Output {
        tokio::time::timeout(
            Duration::from_secs(20),
            self.command(operator, args).output(),
        )
        .await
        .unwrap()
        .unwrap()
    }
}

struct Broker(JoinHandle<()>);
impl Drop for Broker {
    fn drop(&mut self) {
        self.0.abort();
    }
}

#[tokio::test]
async fn operator_pages_match_local_for_raw_blocks_full_and_history() {
    let f = Fixture::new();
    f.message(&f.root, "old one", "old-1");
    f.message(&f.root, "old two", "old-2");
    f.message(&f.child, "new one", "new-1");
    f.message(&f.child, "new two", "new-2");
    let _broker = f.broker();
    let cases: Vec<Vec<&str>> = vec![
        vec!["--limit", "1"],
        vec!["--cursor", "1", "--limit", "1"],
        vec!["--view", "blocks", "--limit", "1"],
        vec!["--view", "blocks", "--tail-blocks", "1"],
        vec!["--full", "--limit", "1"],
        vec!["--run-id", f.root.as_str(), "--limit", "1"],
        vec!["--run-id", f.root.as_str(), "--full", "--limit", "1"],
    ];
    for args in cases {
        let local = f.output(false, &args).await;
        let remote = f.output(true, &args).await;
        assert!(
            local.status.success(),
            "{args:?}: {}",
            String::from_utf8_lossy(&local.stderr)
        );
        assert!(
            remote.status.success(),
            "{args:?}: {}",
            String::from_utf8_lossy(&remote.stderr)
        );
        assert_eq!(remote.stdout, local.stdout, "{args:?}");
    }
    assert!(!f.temp.path().join("absent").exists());
}

#[tokio::test]
async fn operator_reassembles_large_escaped_page() {
    let f = Fixture::new();
    let content = format!("{}é🦀", "\0".repeat(16_000));
    for _ in 0..4 {
        f.message(&f.child, &content, "large");
    }
    let _broker = f.broker();
    let local = f.output(false, &[]).await;
    let remote = f.output(true, &[]).await;
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
    let page: Value = serde_json::from_slice(&remote.stdout).unwrap();
    let messages = page["messages"].as_array().unwrap();
    assert_eq!(messages.len(), 4);
    assert!(messages.iter().all(|m| m["content"] == content));
    assert!(remote.stdout.len() > agent_run_domain::observation::MAX_CHUNK_BYTES);
}

/// A page cannot be resumed after its contents or selected execution receipt changes.
#[tokio::test]
async fn operator_rejects_changed_page_and_selection() {
    let f = Fixture::new();
    let content = "\0".repeat(16_000);
    for _ in 0..4 {
        f.message(&f.child, &content, "large");
    }
    let _broker = f.broker();
    // SAFETY: geteuid only reads this fixture process's identity.
    let uid = unsafe { libc::geteuid() };
    let selection = socket::client_at(
        &f.socket,
        uid,
        "_cli_transcript_select",
        json!({
            "agent_id": f.root, "run_id": null
        }),
    )
    .await
    .unwrap();
    let request = json!({
        "selection": selection,
        "query": {"cursor": 0, "limit": 100, "view": "raw", "tail_blocks": null, "before_cursor": null},
        "lineage": true, "recovery": false, "offset_bytes": 0, "expected_sha256": null
    });
    let first = socket::client_at(&f.socket, uid, "_cli_transcript_page", request.clone())
        .await
        .unwrap();
    assert_eq!(first["complete"], false);
    let mut resume = request.clone();
    resume["offset_bytes"] = first["next_offset"].clone();
    resume["expected_sha256"] = first["sha256"].clone();
    f.message(&f.child, "new content", "new");
    assert!(
        socket::client_at(&f.socket, uid, "_cli_transcript_page", resume)
            .await
            .is_err()
    );

    let mut mismatch = request;
    mismatch["selection"]["sequence"] = json!(999);
    assert!(
        socket::client_at(&f.socket, uid, "_cli_transcript_page", mismatch)
            .await
            .is_err()
    );
}

/// An oversized claimed page is refused on the first fragment, before rendering or another request.
#[tokio::test]
async fn operator_rejects_oversized_claim_before_reading_more() {
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };

    let f = Fixture::new();
    let listener = UnixListener::bind(&f.socket).unwrap();
    let selection = json!({"agent_id": f.root, "run_id": f.child, "sequence": 2});
    let requests = Arc::new(AtomicUsize::new(0));
    let observed = Arc::clone(&requests);
    let _broker = Broker(tokio::spawn(async move {
        loop {
            let (stream, _) = listener.accept().await.unwrap();
            let (input, mut output) = stream.into_split();
            let raw = frame::read(&mut BufReader::new(input), socket::MAX_FRAME)
                .await
                .unwrap()
                .unwrap();
            let request: Value = serde_json::from_slice(&raw).unwrap();
            let response = if request["method"] == "_cli_transcript_select" {
                json!({"jsonrpc":"2.0", "id":request["id"], "result":selection})
            } else {
                assert_eq!(request["method"], "_cli_transcript_page");
                if observed.fetch_add(1, Ordering::SeqCst) == 0 {
                    json!({"jsonrpc":"2.0", "id":request["id"], "result": {
                        "selection":selection, "sha256":"0".repeat(64),
                        "total_bytes":agent_run_core::dispatch::CLI_TRANSCRIPT_MAX_PAGE_BYTES as u64 + 1,
                        "offset_bytes":0, "next_offset":1, "complete":false, "content":"x"
                    }})
                } else {
                    json!({"jsonrpc":"2.0", "id":request["id"], "error":{
                        "code":-32603, "message":"unexpected second page request"
                    }})
                }
            };
            frame::write(&mut output, &response, socket::MAX_FRAME)
                .await
                .unwrap();
        }
    }));
    let result = f.output(true, &[]).await;
    assert!(!result.status.success());
    assert!(result.stdout.is_empty());
    assert_eq!(requests.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn operator_follow_streams_and_drains_after_terminal() {
    let f = Fixture::new();
    f.message(&f.child, "Hello", "m1");
    let _broker = f.broker();
    let mut command = f.command(true, &["--follow", "--format", "text"]);
    let mut child = command
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    let mut stdout = child.stdout.take().unwrap();
    let mut first = [0; 5];
    tokio::time::timeout(Duration::from_secs(20), stdout.read_exact(&mut first))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(&first, b"Hello");
    assert!(child.try_wait().unwrap().is_none());
    f.message(&f.child, " world", "m1");
    agent_run_store::fixture::open(&f.home)
        .unwrap()
        .finish(
            &f.child,
            &agent_run::domain::Outcome::failure("fixture_complete"),
            None,
            None,
        )
        .unwrap();
    let mut rest = Vec::new();
    tokio::time::timeout(Duration::from_secs(20), stdout.read_to_end(&mut rest))
        .await
        .unwrap()
        .unwrap();
    let output = tokio::time::timeout(Duration::from_secs(20), child.wait_with_output())
        .await
        .unwrap()
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert_eq!(rest, b" world\n");
}
