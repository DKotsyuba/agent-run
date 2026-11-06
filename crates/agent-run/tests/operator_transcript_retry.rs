//! Deterministic socket regressions for transcript retries and terminal draining.

use agent_run::transport::{frame, socket};
use agent_run_platform::fs;
use serde_json::{Value, json};
use std::{path::Path, process::Stdio, time::Duration};
use tokio::{
    io::{AsyncReadExt, BufReader},
    net::UnixListener,
    process::Command,
};

const ROOT: &str = "ag-20260826-120000-0123456789";
const RUN: &str = "ag-20260826-120001-0123456789";

/// Creates an operator viewer using only an explicitly selected fixture socket.
fn viewer(root: &Path, socket_path: &Path, args: &[&str]) -> Command {
    let mut command = Command::new(env!("CARGO_BIN_EXE_agent-run"));
    // SAFETY: geteuid only reads the current process identity.
    let uid = unsafe { libc::geteuid() };
    command
        .kill_on_drop(true)
        .arg("--home")
        .arg(root.join("absent"))
        .arg("--broker-socket")
        .arg(socket_path)
        .arg("--broker-uid")
        .arg(uid.to_string())
        .arg("transcript")
        .arg(ROOT)
        .args(args)
        .env("HOME", root.join("caller-home"))
        .env("AGENT_RUN_HOME", root.join("env-home"));
    command
}

/// Builds a normal broker transcript page suitable for a text or JSON viewer.
fn page(content: &str, sequence: i64) -> Value {
    json!({"agent_id":ROOT,"messages":[{"seq":sequence,"role":"assistant","content":content,"raw_ref":"m"}],
        "cursor":sequence-1,"limit":200,"next_cursor":null,"complete":true})
}

/// Builds one correctly identified fragment of a serialized page.
fn fragment(selection: &Value, encoded: &str, start: usize, end: usize) -> Value {
    json!({"selection":selection,"sha256":fs::sha256(encoded.as_bytes()),"total_bytes":encoded.len(),
        "offset_bytes":start,"next_offset":end,"complete":end==encoded.len(),"content":&encoded[start..end]})
}

/// A changed page is restarted from offset zero before any result is printed.
#[tokio::test]
async fn changed_page_restarts_from_zero_without_mixing_or_partial_output() {
    let temp = tempfile::Builder::new().prefix("tr-").tempdir().unwrap();
    let path = temp.path().join("s");
    let listener = UnixListener::bind(&path).unwrap();
    let selection = json!({"agent_id":ROOT,"run_id":RUN,"sequence":1});
    let old = serde_json::to_string(&page("old", 1)).unwrap();
    let new = serde_json::to_string(&page("new", 1)).unwrap();
    let selection_server = selection.clone();
    let old_server = old.clone();
    let new_server = new.clone();
    let server = tokio::spawn(async move {
        let mut calls = Vec::new();
        for step in 0..5 {
            let (stream, _) = listener.accept().await.unwrap();
            let (input, mut output) = stream.into_split();
            let raw = frame::read(&mut BufReader::new(input), socket::MAX_FRAME)
                .await
                .unwrap()
                .unwrap();
            let request: Value = serde_json::from_slice(&raw).unwrap();
            let response = match step {
                0 => json!({"jsonrpc":"2.0","id":request["id"],"result":selection_server}),
                1 => {
                    json!({"jsonrpc":"2.0","id":request["id"],"result":fragment(&selection_server,&old_server,0,20)})
                }
                2 => json!({"jsonrpc":"2.0","id":request["id"],"error":{"code":-32000,
                    "message":"transcript page changed during read","data":{"code":"TranscriptPageChanged"}}}),
                3 => {
                    json!({"jsonrpc":"2.0","id":request["id"],"result":fragment(&selection_server,&new_server,0,20)})
                }
                _ => {
                    json!({"jsonrpc":"2.0","id":request["id"],"result":fragment(&selection_server,&new_server,20,new_server.len())})
                }
            };
            frame::write(&mut output, &response, socket::MAX_FRAME)
                .await
                .unwrap();
            calls.push(request);
        }
        calls
    });
    let output = tokio::time::timeout(
        Duration::from_secs(10),
        viewer(temp.path(), &path, &[]).output(),
    )
    .await
    .unwrap()
    .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert_eq!(
        serde_json::from_slice::<Value>(&output.stdout).unwrap(),
        serde_json::from_str::<Value>(&new).unwrap()
    );
    let calls = server.await.unwrap();
    assert_eq!(calls[0]["method"], "_cli_transcript_select");
    for (index, offset) in [(1, 0), (2, 20), (3, 0), (4, 20)] {
        assert_eq!(calls[index]["method"], "_cli_transcript_page");
        assert_eq!(calls[index]["params"]["selection"], selection);
        assert_eq!(calls[index]["params"]["offset_bytes"], offset);
    }
    assert!(calls[1]["params"]["expected_sha256"].is_null());
    assert_eq!(
        calls[2]["params"]["expected_sha256"],
        fs::sha256(old.as_bytes())
    );
    assert!(calls[3]["params"]["expected_sha256"].is_null());
    assert_eq!(
        calls[4]["params"]["expected_sha256"],
        fs::sha256(new.as_bytes())
    );
    assert!(!temp.path().join("absent").exists());
}

/// Continual changes stop after the bounded retry budget without printing a partial page.
#[tokio::test]
async fn repeated_page_changes_stop_without_output() {
    let temp = tempfile::Builder::new().prefix("tr-").tempdir().unwrap();
    let path = temp.path().join("s");
    let listener = UnixListener::bind(&path).unwrap();
    let selection = json!({"agent_id":ROOT,"run_id":RUN,"sequence":1});
    let encoded = serde_json::to_string(&page("changing", 1)).unwrap();
    let server = tokio::spawn(async move {
        for step in 0..9 {
            let (stream, _) = listener.accept().await.unwrap();
            let (input, mut output) = stream.into_split();
            let raw = frame::read(&mut BufReader::new(input), socket::MAX_FRAME)
                .await
                .unwrap()
                .unwrap();
            let request: Value = serde_json::from_slice(&raw).unwrap();
            let response = if step == 0 {
                json!({"jsonrpc":"2.0","id":request["id"],"result":selection})
            } else if step % 2 == 1 {
                json!({"jsonrpc":"2.0","id":request["id"],"result":fragment(&selection,&encoded,0,20)})
            } else {
                json!({"jsonrpc":"2.0","id":request["id"],"error":{"code":-32000,
                    "message":"transcript page changed during read","data":{"code":"TranscriptPageChanged"}}})
            };
            frame::write(&mut output, &response, socket::MAX_FRAME)
                .await
                .unwrap();
        }
    });
    let output = tokio::time::timeout(
        Duration::from_secs(10),
        viewer(temp.path(), &path, &[]).output(),
    )
    .await
    .unwrap()
    .unwrap();
    assert!(!output.status.success());
    assert!(output.stdout.is_empty());
    assert!(
        String::from_utf8_lossy(&output.stderr).contains("transcript page changed during read")
    );
    server.await.unwrap();
    assert!(!temp.path().join("absent").exists());
}

/// A terminal status after an earlier page forces one final read of its cursor.
#[tokio::test]
async fn terminal_status_drains_messages_appended_after_last_page() {
    let temp = tempfile::Builder::new().prefix("tr-").tempdir().unwrap();
    let path = temp.path().join("s");
    let listener = UnixListener::bind(&path).unwrap();
    let selection = json!({"agent_id":ROOT,"run_id":RUN,"sequence":1});
    let initial = serde_json::to_string(&page("Hello", 1)).unwrap();
    let tail = serde_json::to_string(&page(" world", 2)).unwrap();
    let server = tokio::spawn(async move {
        let mut requests = Vec::new();
        for step in 0..4 {
            let (stream, _) = listener.accept().await.unwrap();
            let (input, mut output) = stream.into_split();
            let raw = frame::read(&mut BufReader::new(input), socket::MAX_FRAME)
                .await
                .unwrap()
                .unwrap();
            let request: Value = serde_json::from_slice(&raw).unwrap();
            let result = match step {
                0 => selection.clone(),
                1 => fragment(&selection, &initial, 0, initial.len()),
                2 => json!({"selection":selection,"status":"succeeded"}),
                _ => fragment(&selection, &tail, 0, tail.len()),
            };
            frame::write(
                &mut output,
                &json!({"jsonrpc":"2.0","id":request["id"],"result":result}),
                socket::MAX_FRAME,
            )
            .await
            .unwrap();
            requests.push(request);
        }
        requests
    });
    let output = tokio::time::timeout(
        Duration::from_secs(10),
        viewer(temp.path(), &path, &["--follow", "--format", "text"]).output(),
    )
    .await
    .unwrap()
    .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert_eq!(output.stdout, b"Hello world\n");
    let calls = server.await.unwrap();
    assert_eq!(calls[2]["method"], "_cli_transcript_status");
    assert_eq!(calls[3]["method"], "_cli_transcript_page");
    assert_eq!(calls[3]["params"]["query"]["cursor"], 1);
    assert!(!temp.path().join("absent").exists());
}

/// SIGINT closes a pending status read, flushes the viewer and never sends a mutation.
#[tokio::test]
async fn follow_ctrl_c_closes_pending_socket_without_cancelling_agent() {
    let temp = tempfile::Builder::new().prefix("tr-").tempdir().unwrap();
    let path = temp.path().join("s");
    let listener = UnixListener::bind(&path).unwrap();
    let selection = json!({"agent_id":ROOT,"run_id":RUN,"sequence":1});
    let encoded = serde_json::to_string(&page("Hello", 1)).unwrap();
    let (ready_tx, ready_rx) = tokio::sync::oneshot::channel();
    let server = tokio::spawn(async move {
        for step in 0..2 {
            let (stream, _) = listener.accept().await.unwrap();
            let (input, mut output) = stream.into_split();
            let raw = frame::read(&mut BufReader::new(input), socket::MAX_FRAME)
                .await
                .unwrap()
                .unwrap();
            let request: Value = serde_json::from_slice(&raw).unwrap();
            let result = if step == 0 {
                selection.clone()
            } else {
                fragment(&selection, &encoded, 0, encoded.len())
            };
            frame::write(
                &mut output,
                &json!({"jsonrpc":"2.0","id":request["id"],"result":result}),
                socket::MAX_FRAME,
            )
            .await
            .unwrap();
        }
        let (stream, _) = listener.accept().await.unwrap();
        let (input, _output) = stream.into_split();
        let mut input = BufReader::new(input);
        let raw = frame::read(&mut input, socket::MAX_FRAME)
            .await
            .unwrap()
            .unwrap();
        let request: Value = serde_json::from_slice(&raw).unwrap();
        assert_eq!(request["method"], "_cli_transcript_status");
        ready_tx.send(()).unwrap();
        let mut byte = [0_u8];
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(10), input.read(&mut byte))
                .await
                .unwrap()
                .unwrap(),
            0
        );
    });
    let mut child = viewer(temp.path(), &path, &["--follow", "--format", "text"])
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    let mut stdout = child.stdout.take().unwrap();
    let mut prefix = [0_u8; 5];
    tokio::time::timeout(Duration::from_secs(10), stdout.read_exact(&mut prefix))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(&prefix, b"Hello");
    tokio::time::timeout(Duration::from_secs(10), ready_rx)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        // SAFETY: this owned child is alive at the pending-status checkpoint.
        unsafe { libc::kill(i32::try_from(child.id().unwrap()).unwrap(), libc::SIGINT) },
        0
    );
    let mut tail = Vec::new();
    tokio::time::timeout(Duration::from_secs(10), stdout.read_to_end(&mut tail))
        .await
        .unwrap()
        .unwrap();
    let status = tokio::time::timeout(Duration::from_secs(10), child.wait())
        .await
        .unwrap()
        .unwrap();
    assert!(status.success(), "viewer exited with {status}");
    assert_eq!(tail, b"\n");
    server.await.unwrap();
    assert!(!temp.path().join("absent").exists());
}
