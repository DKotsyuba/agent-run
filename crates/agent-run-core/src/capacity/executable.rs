//! Bounded execution of operator-owned quota commands, independent of language or provider.

use agent_run_domain::catalog::CollectorBinding;
use agent_run_platform::process::OwnedProcess;
use serde_json::Value;
use std::{collections::BTreeMap, path::Path, process::Stdio, time::Duration};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWriteExt};

/// Maximum context or quota JSON bytes; stderr has a smaller independent ceiling.
const MAX_BYTES: usize = 2 * 1024 * 1024;

/// Reads one stream without exceeding its byte ceiling, including when EOF never arrives.
/// The caller owns the deadline and drops this future before cleaning up the process.
async fn bounded_read(reader: impl AsyncRead + Unpin, max: usize) -> Result<Vec<u8>, &'static str> {
    let mut bytes = Vec::new();
    reader
        .take(max as u64 + 1)
        .read_to_end(&mut bytes)
        .await
        .map_err(|_| "collector_io_failed")?;
    if bytes.len() > max {
        return Err("collector_output_too_large");
    }
    Ok(bytes)
}

/// Ensures cancellation of the collector future also tears down its captured processes.
struct ProcessGuard {
    /// Exact leader and observed descendant identities.
    owner: OwnedProcess,
    /// True until the ordinary asynchronous cleanup has been attempted.
    armed: bool,
}
impl Drop for ProcessGuard {
    /// A cancelled future cannot await: use the existing bounded synchronous cleanup.
    fn drop(&mut self) {
        if self.armed {
            let _ = self.owner.cleanup_blocking(Duration::from_millis(250));
        }
    }
}

/// Executes exactly the configured command/args with private JSON stdin and bounded streams.
///
/// The context may contain credentials and is never persisted or put in argv. The
/// environment is cleared, retaining basic host paths and explicitly granted names.
/// A zero exit, one JSON document and confirmed process cleanup are all required.
/// Provider output and OS error text never escape this boundary. The caller validates
/// quota semantics and rejects any credential echoed into the result before storage.
pub async fn run(
    binding: &CollectorBinding,
    context: &Value,
    cwd: &Path,
) -> Result<Value, &'static str> {
    binding.validate().map_err(|_| "collector_config_invalid")?;
    let input = serde_json::to_vec(context).map_err(|_| "collector_input_invalid")?;
    if input.len() > MAX_BYTES {
        return Err("collector_input_too_large");
    }
    let mut environment: BTreeMap<String, String> = [
        "HOME", "PATH", "USER", "LOGNAME", "LANG", "LC_ALL", "TMPDIR",
    ]
    .into_iter()
    .chain(binding.env_from.iter().map(String::as_str))
    .filter_map(|name| {
        std::env::var(name)
            .ok()
            .map(|value| (name.to_owned(), value))
    })
    .collect();
    environment
        .entry("PATH".into())
        .or_insert_with(|| "/usr/bin:/bin:/usr/sbin:/sbin".into());
    let mut command = tokio::process::Command::new(&binding.command);
    command
        .args(&binding.args)
        .current_dir(cwd)
        .env_clear()
        .envs(environment)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true);
    use std::os::unix::process::CommandExt;
    command.as_std_mut().process_group(0);
    let mut child = command.spawn().map_err(|_| "collector_spawn_failed")?;
    let mut guard = ProcessGuard {
        owner: OwnedProcess::capture(child.id().expect("spawned child has a PID") as i32),
        armed: true,
    };
    let mut stdin = child.stdin.take().expect("piped collector stdin");
    let stdout = child.stdout.take().expect("piped collector stdout");
    let stderr = child.stderr.take().expect("piped collector stderr");
    let write = async move {
        stdin
            .write_all(&input)
            .await
            .map_err(|_| "collector_io_failed")?;
        stdin.shutdown().await.map_err(|_| "collector_io_failed")?;
        drop(stdin);
        Ok::<_, &'static str>(())
    };
    let streams = async {
        tokio::try_join!(
            write,
            bounded_read(stdout, MAX_BYTES),
            bounded_read(stderr, 64 * 1024)
        )
    };
    tokio::pin!(streams);
    let deadline = tokio::time::sleep(Duration::from_secs(binding.timeout_seconds));
    tokio::pin!(deadline);
    let mut tick = tokio::time::interval(Duration::from_millis(25));
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    let mut output = None;
    // Observe the actual child independently of EOF: inherited pipes must never
    // keep a finished script alive until the whole collection deadline.
    let status = loop {
        tokio::select! {
            result = child.wait() => break result.map_err(|_| "collector_wait_failed"),
            result = &mut streams, if output.is_none() => {
                match result {
                    Ok(((), stdout, _stderr)) => output = Some(stdout),
                    Err(code) => break Err(code),
                }
            }
            _ = &mut deadline => break Err("collector_timeout"),
            _ = tick.tick() => guard.owner.refresh(),
        }
    };
    let cleanup = guard.owner.cleanup(Duration::from_millis(250)).await;
    guard.armed = false;
    let reaped = tokio::time::timeout(Duration::from_secs(2), child.wait()).await;
    if !cleanup.is_ok_and(|proof| proof.confirmed) || !matches!(reaped, Ok(Ok(_))) {
        return Err("collector_cleanup_unverified");
    }
    if !status?.success() {
        return Err("collector_exit_failed");
    }
    let output = match output {
        Some(output) => output,
        None => {
            tokio::time::timeout(Duration::from_secs(1), &mut streams)
                .await
                .map_err(|_| "collector_pipe_timeout")??
                .1
        }
    };
    serde_json::from_slice(&output).map_err(|_| "collector_output_invalid")
}
