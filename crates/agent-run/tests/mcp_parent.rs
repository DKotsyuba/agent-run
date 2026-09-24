//! Desktop frontend death must not leave a Rust MCP child behind with inherited open stdin.
use agent_run_platform::process::{self, OwnedProcess, ProcessState};
use std::{path::Path, process::Stdio, time::Duration};

/// Owns the fixture tree even if a regression causes an assertion or task panic.
struct FixtureProcesses(OwnedProcess);
impl Drop for FixtureProcesses {
    /// Terminates only the captured private fixture group and verified descendants.
    fn drop(&mut self) {
        let _ = self.0.cleanup_blocking(Duration::from_millis(250));
    }
}

/// Killing the real frontend with SIGKILL closes its child even while the upstream pipe remains open.
#[tokio::test]
async fn frontend_sigkill_does_not_orphan_the_mcp_child() {
    use std::os::unix::process::CommandExt;
    let home = tempfile::tempdir().unwrap();
    let node = [
        "/opt/homebrew/bin/node",
        "/usr/local/bin/node",
        "/usr/bin/node",
    ]
    .into_iter()
    .find(|path| Path::new(path).is_file())
    .expect("Node is required for Desktop transport tests");
    let mut command = tokio::process::Command::new(node);
    command
        .args([
            "-e",
            include_str!("../../../assets/desktop-transport.cjs"),
            "--",
            env!("CARGO_BIN_EXE_agent-run"),
        ])
        .arg(home.path())
        .arg(include_str!("../../../assets/completion_notice.json"))
        .arg("--home")
        .arg(home.path())
        .arg("mcp")
        .env("CODEX_APP_TOOLS_PIPE_PATH", home.path().join("unused.sock"))
        .env("CODEX_MCP_NODE_PATH", node)
        .stdin(Stdio::piped())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .kill_on_drop(true);
    command.as_std_mut().process_group(0);
    let mut host = command.spawn().unwrap();
    let host_pid = host.id().unwrap() as i32;
    let mut owned = FixtureProcesses(OwnedProcess::capture(host_pid));
    let keep_stdin_open = host.stdin.take().unwrap();
    let child = tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            owned.0.refresh();
            if let Some(child) = process::processes()
                .unwrap()
                .into_iter()
                .find(|p| p.ppid == host_pid && !p.zombie)
            {
                break child;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("frontend must start its Rust child");
    assert_eq!(
        process::observe(Some(child.pid), Some(&child.token), Some(child.birth)),
        ProcessState::Alive
    );
    host.kill().await.unwrap();
    tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            if matches!(
                process::observe(Some(child.pid), Some(&child.token), Some(child.birth)),
                ProcessState::Dead | ProcessState::Reused
            ) {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("Rust MCP child survived a killed frontend");
    drop(keep_stdin_open);
}
