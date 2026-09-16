//! Wire-level parity checks against captured Python MCP stdio exchanges.
//!
//! These tests require Unix sockets because the MCP proxy forwards every tool
//! call through a temporary resident broker; they never use the owner's socket.

use serde_json::{json, Value};
use std::{
    io::{BufRead, BufReader, Write},
    os::unix::fs::FileTypeExt,
    path::{Path, PathBuf},
    process::{Child, ChildStdin, ChildStdout, Command, Stdio},
    time::{Duration, Instant},
};

/// Own one initialized temporary home and, optionally, its resident broker.
struct Harness {
    /// Keeps the short Unix-socket base directory alive for every child.
    temp: tempfile::TempDir,
    /// Home passed to the Rust CLI and its resident broker.
    home: PathBuf,
    /// Broker process, retained so Drop can terminate the test-owned process.
    broker: Option<Child>,
}

impl Harness {
    /// Create a mode-appropriate empty home using the caller's short socket base.
    fn new() -> Self {
        let base = std::env::var_os("AGENT_RUN_TEST_TMP").unwrap_or_else(|| "/tmp".into());
        let temp = tempfile::Builder::new()
            .prefix("ar-")
            .tempdir_in(base)
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
        Self {
            temp,
            home,
            broker: None,
        }
    }

    /// Start the real broker and wait only until its Unix socket is published.
    fn start_broker(&mut self) {
        self.broker = Some(
            Command::new(env!("CARGO_BIN_EXE_agent-run"))
                .arg("--home")
                .arg(&self.home)
                .args(["api", "serve"])
                .stdin(Stdio::null())
                .stdout(Stdio::null())
                .stderr(Stdio::null())
                .spawn()
                .unwrap(),
        );
        let deadline = Instant::now() + Duration::from_secs(5);
        while !socket_ready(&self.home.join("api.sock")) {
            assert!(Instant::now() < deadline, "broker did not publish api.sock");
            std::thread::sleep(Duration::from_millis(20));
        }
    }

    /// Start the MCP proxy with pipes used to make exact JSON-RPC exchanges.
    fn mcp(&self) -> Mcp {
        let mut child = Command::new(env!("CARGO_BIN_EXE_agent-run"))
            .arg("--home")
            .arg(&self.home)
            .arg("mcp")
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap();
        Mcp {
            stdin: child.stdin.take().unwrap(),
            stdout: BufReader::new(child.stdout.take().unwrap()),
            child,
        }
    }
}

impl Drop for Harness {
    /// Terminate only the broker process created by this harness before its home disappears.
    fn drop(&mut self) {
        if let Some(mut broker) = self.broker.take() {
            let _ = broker.kill();
            let _ = broker.wait();
        }
        let _ = &self.temp;
    }
}

/// Drive one MCP subprocess with newline-delimited JSON-RPC messages.
struct Mcp {
    /// Request stream owned by the MCP child.
    stdin: ChildStdin,
    /// Response stream decoded one complete JSON line at a time.
    stdout: BufReader<ChildStdout>,
    /// Child retained for EOF status and stderr diagnostics.
    child: Child,
}

impl Mcp {
    /// Send one request or notification, returning no value for notifications.
    fn send(&mut self, request: Value) -> Option<Value> {
        writeln!(self.stdin, "{}", request).unwrap();
        self.stdin.flush().unwrap();
        request.get("id")?;
        let mut line = String::new();
        self.stdout.read_line(&mut line).unwrap();
        Some(serde_json::from_str(&line).unwrap())
    }

    /// Close stdin and require the proxy to treat clean EOF as a clean exit.
    fn finish(self) {
        drop(self.stdin);
        let output = self.child.wait_with_output().unwrap();
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
    }
}

/// Return whether a broker-published path is a Unix socket without following a symlink.
fn socket_ready(path: &Path) -> bool {
    path.symlink_metadata()
        .is_ok_and(|metadata| metadata.file_type().is_socket())
}

/// Read the captured Python exchange for one supported handshake protocol version.
fn baseline(version: &str) -> Vec<Value> {
    serde_json::from_str::<Value>(include_str!(
        "../../../tests/fixtures/baseline/mcp/handshake.json"
    ))
    .unwrap()[version]
        .as_array()
        .unwrap()
        .clone()
}

/// Build a deterministic initialize request shared with the captured Python session.
fn initialize(version: &str) -> Value {
    json!({"jsonrpc":"2.0","id":1,"method":"initialize","params":{
        "protocolVersion":version,"capabilities":{},"clientInfo":{"name":"parity-fixture","version":"1"}
    }})
}

/// Compare every observable non-time-dependent exchange from one captured Python session.
/// Mirrors `test_mcp.py::test_official_client_negotiates_lists_and_calls_over_stdio`.
#[test]
fn mcp_matches_python_handshake_tools_calls_notifications_and_eof() {
    for version in ["2024-11-05", "2025-03-26", "2025-06-18", "2025-11-25"] {
        let expected = baseline(version);
        let mut harness = Harness::new();
        harness.start_broker();
        let mut mcp = harness.mcp();
        assert_eq!(
            mcp.send(initialize(version)).unwrap(),
            expected[0]["response"]
        );
        assert_eq!(
            mcp.send(json!({"jsonrpc":"2.0","method":"notifications/initialized","params":{}})),
            None
        );
        assert_eq!(
            mcp.send(expected[2]["request"].clone()).unwrap(),
            expected[2]["response"]
        );
        assert_eq!(
            mcp.send(expected[3]["request"].clone()).unwrap(),
            expected[3]["response"]
        );
        assert_eq!(
            mcp.send(expected[4]["request"].clone()).unwrap(),
            expected[4]["response"]
        );
        assert_eq!(
            mcp.send(expected[5]["request"].clone()).unwrap(),
            expected[5]["response"]
        );
        assert_eq!(mcp.send(expected[7]["request"].clone()), None);
        mcp.finish();
    }
}

/// Ensure the advertised list is exactly the captured schema table after null omission on the wire.
#[test]
fn mcp_tools_list_matches_the_packaged_python_table() {
    let mut harness = Harness::new();
    harness.start_broker();
    let mut mcp = harness.mcp();
    let _ = mcp.send(initialize("2025-11-25"));
    let response = mcp
        .send(json!({"jsonrpc":"2.0","id":2,"method":"tools/list","params":{}}))
        .unwrap();
    let mut expected: Value =
        serde_json::from_str(include_str!("../../../tests/fixtures/baseline/tools.json")).unwrap();
    for tool in expected.as_array_mut().unwrap() {
        tool.as_object_mut().unwrap().retain(|key, value| {
            !matches!(key.as_str(), "outputSchema" | "resultShape") || !value.is_null()
        });
    }
    assert_eq!(response["result"]["tools"], expected);
    mcp.finish();
}

/// Verify the unavailable broker is a Python-shaped tool error, not a local fallback.
#[test]
fn mcp_broker_unavailable_matches_python_tool_error() {
    let expected: Vec<Value> = serde_json::from_str(include_str!(
        "../../../tests/fixtures/baseline/mcp/broker-unavailable.json"
    ))
    .unwrap();
    let harness = Harness::new();
    let mut mcp = harness.mcp();
    assert_eq!(
        mcp.send(initialize("2025-11-25")).unwrap(),
        expected[0]["response"]
    );
    assert_eq!(mcp.send(expected[1]["request"].clone()), None);
    assert_eq!(
        mcp.send(expected[2]["request"].clone()).unwrap(),
        expected[2]["response"]
    );
    mcp.finish();
}

/// Reject an oversized pre-initialize frame instead of buffering it without bound.
/// Mirrors `test_mcp.py::test_oversized_stdio_frame_stays_bounded_and_uses_sdk_error_path`.
#[test]
fn mcp_rejects_oversized_stdio_frame() {
    let harness = Harness::new();
    let mut mcp = harness.mcp();
    mcp.stdin.write_all(&vec![b'x'; 1024 * 1024 + 1]).unwrap();
    mcp.stdin.write_all(b"\n").unwrap();
    mcp.stdin.flush().unwrap();
    drop(mcp.stdin);
    let output = mcp.child.wait_with_output().unwrap();
    assert!(!output.status.success());
}
