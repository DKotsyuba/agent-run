//! Production CLI/MCP continuation retries must share one broker request identity.

use agent_run::transport::{frame, socket};
use serde_json::{json, Value};
use std::{path::Path, process::Stdio, time::Duration};
use tokio::{io::BufReader, net::UnixListener, process::Command};

/// Stable synthetic identity used only by the private broker fixture.
const AGENT: &str = "ag-20260925-000000-0000000001";
/// Exact synthetic resumed execution returned by the private broker fixture.
const RUN: &str = "ag-20260925-000001-0000000002";

/// Start a bounded production command without Desktop relay or model processes.
fn command(home: &Path) -> Command {
    let mut command = Command::new(env!("CARGO_BIN_EXE_agent-run"));
    command
        .arg("--home")
        .arg(home)
        .env_remove("CODEX_APP_TOOLS_PIPE_PATH")
        .env_remove("CODEX_MCP_NODE_PATH")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .kill_on_drop(true);
    command
}

/// Lose the first acknowledgement and return the same durable execution on retry.
/// Every accepted frame must retain the caller's exact resume intent and key.
async fn lost_reply(listener: UnixListener, supplied: Option<&str>) {
    let mut original = None;
    for attempt in 0..2 {
        let (stream, _) = listener.accept().await.unwrap();
        let (input, mut output) = stream.into_split();
        let request: Value = serde_json::from_slice(
            &frame::read(&mut BufReader::new(input), socket::MAX_FRAME)
                .await
                .unwrap()
                .unwrap(),
        )
        .unwrap();
        assert_eq!(request["method"], "resume");
        let key = request["params"]["request_id"].as_str().unwrap();
        assert!(!key.is_empty());
        if let Some(supplied) = supplied {
            assert_eq!(key, supplied);
        }
        if let Some(original) = &original {
            assert_eq!(&request["params"], original);
        } else {
            original = Some(request["params"].clone());
        }
        if attempt == 1 {
            frame::write(
                &mut output,
                &json!({"jsonrpc":"2.0","id":request["id"],"result":{
                    "agent_id":AGENT,"run_id":RUN,"created":false,
                    "agent":{"agent_id":AGENT,"run_id":RUN,"status":"running"}
                }}),
                socket::MAX_FRAME,
            )
            .await
            .unwrap();
        }
    }
}

/// Both real frontends reconnect using generated or explicitly supplied keys.
#[tokio::test]
async fn cli_and_mcp_resume_keep_one_intent_after_lost_acknowledgement() {
    for mcp in [false, true] {
        for supplied in [None, Some("caller-owned-resume-key")] {
            let home = tempfile::Builder::new()
                .prefix("ar-retry-")
                .tempdir_in("/tmp")
                .unwrap();
            let listener = UnixListener::bind(home.path().join("api.sock")).unwrap();
            let server = lost_reply(listener, supplied);
            let client = async {
                let mut command = command(home.path());
                if mcp {
                    let mut child = command.arg("mcp").spawn().unwrap();
                    let mut input = child.stdin.take().unwrap();
                    let mut output = BufReader::new(child.stdout.take().unwrap());
                    frame::write(&mut input, &json!({"jsonrpc":"2.0","id":1,"method":"initialize","params":{
                        "protocolVersion":"2025-11-25","capabilities":{},"clientInfo":{"name":"retry-fixture","version":"1"}
                    }}), socket::MAX_FRAME).await.unwrap();
                    assert!(frame::read(&mut output, socket::MAX_FRAME)
                        .await
                        .unwrap()
                        .is_some());
                    frame::write(
                        &mut input,
                        &json!({"jsonrpc":"2.0","method":"notifications/initialized"}),
                        socket::MAX_FRAME,
                    )
                    .await
                    .unwrap();
                    let mut arguments = json!({"agent_id":AGENT,"task":"continue"});
                    if let Some(key) = supplied {
                        arguments["request_id"] = json!(key);
                    }
                    frame::write(
                        &mut input,
                        &json!({"jsonrpc":"2.0","id":2,"method":"tools/call","params":{
                            "name":"resume","arguments":arguments
                        }}),
                        socket::MAX_FRAME,
                    )
                    .await
                    .unwrap();
                    let response: Value = serde_json::from_slice(
                        &frame::read(&mut output, socket::MAX_FRAME)
                            .await
                            .unwrap()
                            .unwrap(),
                    )
                    .unwrap();
                    assert_ne!(response["result"]["isError"], true, "{response}");
                    assert_eq!(response["result"]["structuredContent"]["run_id"], RUN);
                    drop(input);
                    assert!(child.wait().await.unwrap().success());
                } else {
                    command.args(["resume", AGENT, "--task", "continue"]);
                    if let Some(key) = supplied {
                        command.args(["--request-id", key]);
                    }
                    let result = command.output().await.unwrap();
                    assert!(
                        result.status.success(),
                        "{}",
                        String::from_utf8_lossy(&result.stdout)
                    );
                    let response: Value = serde_json::from_slice(&result.stdout).unwrap();
                    assert_eq!(response["run_id"], RUN);
                    assert_eq!(response["created"], false);
                }
            };
            tokio::time::timeout(Duration::from_secs(10), async {
                tokio::join!(server, client);
            })
            .await
            .expect("bounded retry fixture");
        }
    }
}
