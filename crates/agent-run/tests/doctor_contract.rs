//! Real-binary local doctor contract: JSON, read-only homes and exit status 0/2/3.
use std::{
    io::Read,
    path::Path,
    process::{Child, Command, Output, Stdio},
    sync::mpsc,
    time::{Duration, Instant},
};

/// Owns one finite fixture child and kills/reaps it on panic or deadline.
struct DoctorChild(Child);
impl Drop for DoctorChild {
    /// Always releases the exact unreaped fixture child; never signals a process group.
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

/// Drains one owned pipe into a bounded buffer; the parent bounds result retrieval.
fn drain(reader: impl Read + Send + 'static) -> mpsc::Receiver<Vec<u8>> {
    let (sender, receiver) = mpsc::sync_channel(1);
    std::thread::spawn(move || {
        let mut bytes = Vec::new();
        reader.take(1024 * 1024).read_to_end(&mut bytes).unwrap();
        let _ = sender.send(bytes);
    });
    receiver
}

/// Runs only a temporary-home command, clearing optional host bridge capability.
/// The child has 20 seconds, captured output has 1 MiB per pipe, and all children
/// are killed/reaped on timeout. No production HOME or global environment changes.
fn execute(home: &Path, arguments: &[&str], stdout: Stdio) -> Output {
    let mut command = Command::new(env!("CARGO_BIN_EXE_agent-run"));
    command
        .arg("--home")
        .arg(home)
        .args(arguments)
        .env_remove("CODEX_MCP_NODE_PATH")
        .env_remove("CODEX_APP_TOOLS_PIPE_PATH")
        .stdin(Stdio::null())
        .stdout(stdout)
        .stderr(Stdio::piped());
    let mut child = DoctorChild(command.spawn().unwrap());
    let stdout = child.0.stdout.take().map(drain);
    let stderr = child.0.stderr.take().map(drain);
    let deadline = Instant::now() + Duration::from_secs(20);
    let status = loop {
        if let Some(status) = child.0.try_wait().unwrap() {
            break status;
        }
        assert!(
            Instant::now() < deadline,
            "doctor fixture exceeded its deadline"
        );
        std::thread::sleep(Duration::from_millis(10));
    };
    Output {
        status,
        stdout: stdout
            .map(|pipe| pipe.recv_timeout(Duration::from_secs(1)).unwrap())
            .unwrap_or_default(),
        stderr: stderr
            .map(|pipe| pipe.recv_timeout(Duration::from_secs(1)).unwrap())
            .unwrap_or_default(),
    }
}

/// Default and explicit JSON agree; a healthy initialized home stays byte-for-byte
/// unchanged, including no operational log directory from the detached canary.
#[test]
fn doctor_json_is_read_only_and_honest() {
    let home = tempfile::tempdir().unwrap();
    agent_run::init::initialize(home.path()).unwrap();
    let config = std::fs::read(home.path().join("config.toml")).unwrap();
    let database = std::fs::read(home.path().join("state.db")).unwrap();
    assert!(!home.path().join("logs").exists());
    for arguments in [&["doctor"][..], &["doctor", "--json"][..]] {
        let output = execute(home.path(), arguments, Stdio::piped());
        assert_eq!(
            output.status.code(),
            Some(0),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        let report: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
        assert!(report["findings"].is_array());
        assert!(report["checks"].is_array());
        assert_eq!(report["version"], env!("CARGO_PKG_VERSION"));
        assert_eq!(report["build"]["source_commit"], serde_json::Value::Null);
        assert_eq!(report["target"]["arch"], std::env::consts::ARCH);
        assert_eq!(report["config"]["status"], "ok");
        assert_eq!(
            report["executing_release"]["check"]["status"],
            "not_checked"
        );
        assert_eq!(report["current_release"]["check"]["status"], "not_checked");
        assert_eq!(report["resident_compatibility"]["status"], "not_checked");
        let bridge = report["tools"]
            .as_array()
            .unwrap()
            .iter()
            .find(|tool| tool["name"] == "host_node_bridge")
            .unwrap();
        assert_eq!(bridge["status"], "not_checked");
    }
    assert_eq!(
        std::fs::read(home.path().join("config.toml")).unwrap(),
        config
    );
    assert_eq!(
        std::fs::read(home.path().join("state.db")).unwrap(),
        database
    );
    assert!(!home.path().join("logs").exists());
}

/// Failed local checks return a report/2; invocation and report-output failures
/// return 3, while unrelated command parser failures retain their historical 2.
#[test]
fn doctor_statuses_preserve_global_cli_policy() {
    let home = tempfile::tempdir().unwrap();
    let bad = execute(home.path(), &["doctor", "--json"], Stdio::piped());
    assert_eq!(bad.status.code(), Some(2));
    let report: serde_json::Value = serde_json::from_slice(&bad.stdout).unwrap();
    assert_eq!(report["config"]["status"], "failed");
    assert_eq!(report["findings"][0]["code"], "config_invalid");
    let invalid = execute(home.path(), &["doctor", "--unknown-option"], Stdio::piped());
    assert_eq!(invalid.status.code(), Some(3));
    assert!(invalid.stdout.is_empty());
    let unrelated = execute(home.path(), &["models", "--unknown-option"], Stdio::piped());
    assert_eq!(unrelated.status.code(), Some(2));
    let (closed_peer, output_socket) = std::os::unix::net::UnixStream::pair().unwrap();
    drop(closed_peer);
    let output_fd: std::os::fd::OwnedFd = output_socket.into();
    let failed_output = execute(home.path(), &["doctor", "--json"], Stdio::from(output_fd));
    assert_eq!(failed_output.status.code(), Some(3));
    assert!(!home.path().join("logs").exists());
    assert!(!home.path().join("state.db").exists());
    std::fs::write(home.path().join("config.toml"), "schema_version=2\n").unwrap();
    let helper = tokio::runtime::Runtime::new()
        .unwrap()
        .block_on(agent_run::cli::doctor(home.path()))
        .unwrap();
    assert_eq!(helper["config"]["status"], "ok");
    assert!(
        helper["findings"]
            .as_array()
            .unwrap()
            .iter()
            .any(|finding| finding["code"] == "state_invalid")
    );
    assert!(
        !home.path().join("state.db").exists(),
        "public helper must not bootstrap state"
    );
    assert!(!home.path().join("logs").exists());
}
