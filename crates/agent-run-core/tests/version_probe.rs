//! Python-parity tests for bounded configured-binary version observation.
//!
//! Python observes a runtime version with `observe_binary_version`
//! (`src/agent_run/adapters/version.py`): one `<binary> --version` child with a
//! bounded deadline, a bounded output, a replaced environment, and an owned
//! process group that is killed and reaped on failure. The Rust workspace keeps
//! that spawn contract in `capacity::sources::capture`, which the metadata
//! collector runs with `--version` as its liveness evidence.
//!
//! These tests pin the shared half: freshness, bounded failure, bounded output,
//! reaping, environment isolation, and process-group cleanup. Rust has no
//! version *string* or diagnostic-text observation, so the Python behaviors that
//! only describe that text are reported as divergences rather than invented here.

use agent_run_core::capacity::sources::capture;
use std::{
    collections::BTreeMap,
    os::unix::fs::PermissionsExt,
    path::Path,
    time::{Duration, Instant},
};

/// Runs fixture output through an existing shell, avoiding a write/exec race.
fn shell_args(body: &str) -> Vec<String> {
    vec![
        "-c".into(),
        body.into(),
        "version-probe".into(),
        "--version".into(),
    ]
}

/// Returns the minimal replaced child environment used by every probe here.
///
/// `home` becomes the child's `HOME`, mirroring Python's `{HOME, PATH}` probe
/// environment. Nothing else from the parent process is forwarded.
fn isolated(home: &Path) -> BTreeMap<String, String> {
    BTreeMap::from([
        ("HOME".into(), home.display().to_string()),
        ("PATH".into(), "/usr/bin:/bin".into()),
    ])
}

/// Mirrors `tests/test_adapter_versions.py::test_version_is_fresh_and_not_cached`
#[tokio::test]
async fn version_is_fresh_and_not_cached() {
    let root = tempfile::tempdir().expect("temporary root");
    let version_file = root.path().join("version.txt");
    std::fs::write(&version_file, "runtime 1.0\n").unwrap();
    let args = [
        "-c".into(),
        "cat \"$1\"".into(),
        "version-probe".into(),
        version_file.to_string_lossy().into_owned(),
    ];
    let first = capture(Path::new("/bin/sh"), &args, 5, &isolated(root.path()))
        .await
        .expect("first observation");
    assert_eq!(String::from_utf8_lossy(&first).trim(), "runtime 1.0");

    // The same configured path now reports a different version: each call must
    // execute the binary again rather than answer from a retained observation.
    std::fs::write(&version_file, "runtime 2.0\n").unwrap();
    let second = capture(Path::new("/bin/sh"), &args, 5, &isolated(root.path()))
        .await
        .expect("second observation");
    assert_eq!(String::from_utf8_lossy(&second).trim(), "runtime 2.0");
}

/// Mirrors `tests/test_adapter_versions.py::test_version_failures_are_bounded[exit 7-status 7]`
#[tokio::test]
async fn nonzero_status_is_a_bounded_failure() {
    let root = tempfile::tempdir().expect("temporary root");
    let result = capture(
        Path::new("/bin/sh"),
        &shell_args("exit 7"),
        5,
        &isolated(root.path()),
    )
    .await;
    assert!(
        result.is_err(),
        "a nonzero version command must not become an observation"
    );
}

/// Mirrors `tests/test_adapter_versions.py::test_version_failures_are_bounded[i=0; while [ $i -lt 5000 ]; do printf x; i=$((i + 1)); done-exceeds 4096]`
#[tokio::test]
async fn oversized_output_is_refused() {
    // Python bounds this observation at 4096 bytes and Rust at `BODY_MAX`
    // (2 MiB); the contract ported here is that output past the bound is
    // refused outright rather than retained or truncated into a version.
    let root = tempfile::tempdir().expect("temporary root");
    let result = capture(
        Path::new("/bin/sh"),
        &shell_args("yes x | head -c 2200000"),
        10,
        &isolated(root.path()),
    )
    .await;
    assert!(
        result.is_err(),
        "output beyond the bound must be refused, not retained"
    );
}

/// Mirrors `tests/test_adapter_versions.py::test_version_failures_are_bounded[printf 'stderr-only\\n' >&2-no version]`
#[tokio::test]
async fn stderr_only_version_failure_is_bounded() {
    let root = tempfile::tempdir().expect("temporary root");
    let result = capture(
        Path::new("/bin/sh"),
        &shell_args("printf 'stderr-only\\n' >&2"),
        5,
        &isolated(root.path()),
    )
    .await;
    assert!(
        result.is_err(),
        "stderr-only version output is not a version"
    );
}

/// Mirrors `tests/test_adapter_versions.py::test_missing_and_timed_out_commands_are_reaped`
#[tokio::test]
async fn missing_and_timed_out_commands_are_reaped() {
    let root = tempfile::tempdir().expect("temporary root");
    let missing = root.path().join("missing");
    assert!(
        capture(&missing, &["--version".into()], 5, &isolated(root.path()))
            .await
            .is_err(),
        "an unstartable version command must fail rather than hang"
    );

    let started = Instant::now();
    let result = capture(
        Path::new("/bin/sh"),
        &shell_args("sleep 60"),
        1,
        &isolated(root.path()),
    )
    .await;
    let elapsed = started.elapsed();
    assert!(result.is_err(), "a stalled version command must time out");
    assert!(
        elapsed < Duration::from_secs(10),
        "the deadline must bound the call, took {elapsed:?}"
    );
}

/// Mirrors `tests/test_adapter_versions.py::test_version_command_receives_no_ambient_secret`
#[tokio::test]
async fn version_command_receives_no_ambient_value() {
    // `CARGO_MANIFEST_DIR` is set in this parent process by Cargo, so a child
    // that can read it would prove the parent environment leaked through.
    assert!(
        std::env::var_os("CARGO_MANIFEST_DIR").is_some(),
        "the parent must hold the value this test proves is not inherited"
    );
    let root = tempfile::tempdir().expect("temporary root");
    let args = shell_args(
        "if [ -n \"$CARGO_MANIFEST_DIR\" ]; then printf leaked; else printf 'clean 1\\n'; fi",
    );
    let observed = capture(Path::new("/bin/sh"), &args, 5, &isolated(root.path()))
        .await
        .expect("isolated observation");
    assert_eq!(String::from_utf8_lossy(&observed).trim(), "clean 1");
}

/// Immediate successful commands retain valid ownership and cleanup evidence.
#[tokio::test]
async fn rapid_commands_succeed_without_a_fixture_gate() {
    let root = tempfile::tempdir().expect("temporary root");
    let args = shell_args("printf 'runtime 1.0\\n'");
    for _ in 0..48 {
        let output = capture(Path::new("/bin/sh"), &args, 5, &isolated(root.path()))
            .await
            .expect("rapid command is a valid observation");
        assert_eq!(output, b"runtime 1.0\n");
    }
}

/// Spawn errors retain their native errno, and an ENOEXEC file never runs as a shell script.
#[tokio::test]
async fn native_spawn_refusals_do_not_execute_the_file() {
    let root = tempfile::tempdir().expect("temporary root");
    let environment = isolated(root.path());
    let executable = root.path().join("runtime");
    let marker = root.path().join("executed");
    std::fs::write(&executable, "printf ran > \"$HOME/executed\"\n").unwrap();
    for (path, mode, expected) in [
        (root.path().join("missing"), None, libc::ENOENT),
        (executable.clone(), Some(0o600), libc::EACCES),
        #[cfg(target_os = "linux")]
        (executable.clone(), Some(0o700), libc::ENOEXEC),
    ] {
        if let Some(mode) = mode {
            std::fs::set_permissions(&path, std::fs::Permissions::from_mode(mode)).unwrap();
        }
        let error = capture(&path, &["--version".into()], 5, &environment)
            .await
            .expect_err("the original command must refuse to start");
        assert!(
            matches!(&error, agent_run_core::Error::Io(error) if error.raw_os_error() == Some(expected)),
            "expected errno {expected}, got {error:?}"
        );
        assert!(!marker.exists(), "the refused file must not execute");
    }
    let non_directory = root.path().join("regular-file");
    std::fs::write(&non_directory, "file").unwrap();
    let error = capture(&non_directory.join("child"), &[], 5, &environment)
        .await
        .expect_err("a path component is not a directory");
    assert!(
        matches!(error, agent_run_core::Error::Io(error) if error.raw_os_error() == Some(libc::ENOTDIR))
    );
}

/// A no-shebang executable retains the original command's behavior on this OS.
#[tokio::test]
async fn no_shebang_behavior_matches_the_original_command() {
    use std::os::unix::process::CommandExt;

    let root = tempfile::tempdir().expect("temporary root");
    let path = root.path().join("plain-probe");
    std::fs::write(&path, "printf 'plain:%s' \"$1\"\n").unwrap();
    std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o700)).unwrap();
    let args = ["literal $HOME".into()];
    let environment = isolated(root.path());
    let expected = std::process::Command::new(&path)
        .args(&args)
        .env_clear()
        .envs(&environment)
        .stdin(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .process_group(0)
        .output();
    let observed = capture(&path, &args, 5, &environment).await;
    match expected {
        Ok(output) => {
            assert!(output.status.success());
            assert_eq!(observed.unwrap(), output.stdout);
        }
        Err(expected) => assert!(
            matches!(observed, Err(agent_run_core::Error::Io(error)) if error.raw_os_error() == expected.raw_os_error())
        ),
    }
}

/// A bare executable without a shebang keeps std's existing PATH fallback.
#[tokio::test]
async fn bare_name_no_shebang_matches_the_original_command() {
    use std::os::unix::process::CommandExt;

    let root = tempfile::tempdir().expect("temporary root");
    let path = root.path().join("plain-probe");
    std::fs::write(&path, "printf 'plain:%s' \"$1\"\n").unwrap();
    std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o700)).unwrap();
    let args = ["literal $HOME".into()];
    let mut environment = isolated(root.path());
    environment.insert("PATH".into(), root.path().display().to_string());
    let expected = std::process::Command::new("plain-probe")
        .args(&args)
        .env_clear()
        .envs(&environment)
        .stdin(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .process_group(0)
        .output();
    let observed = capture(Path::new("plain-probe"), &args, 5, &environment).await;
    match expected {
        Ok(output) => {
            assert!(output.status.success());
            assert_eq!(observed.unwrap(), output.stdout);
        }
        Err(expected) => assert!(
            matches!(observed, Err(agent_run_core::Error::Io(error)) if error.raw_os_error() == expected.raw_os_error())
        ),
    }
}

/// Bare commands use the supplied PATH, continuing past a non-executable candidate.
#[tokio::test]
async fn bare_name_path_search_and_denial() {
    let root = tempfile::tempdir().expect("temporary root");
    let first = root.path().join("first");
    let second = root.path().join("second");
    std::fs::create_dir(&first).unwrap();
    std::fs::create_dir(&second).unwrap();
    for (directory, mode) in [(&first, 0o600), (&second, 0o700)] {
        let file = directory.join("path-probe");
        std::fs::write(&file, "#!/bin/sh\nprintf found-path\n").unwrap();
        std::fs::set_permissions(file, std::fs::Permissions::from_mode(mode)).unwrap();
    }
    let mut environment = isolated(root.path());
    environment.insert(
        "PATH".into(),
        format!("{}:{}", first.display(), second.display()),
    );
    assert_eq!(
        capture(Path::new("path-probe"), &[], 5, &environment)
            .await
            .unwrap(),
        b"found-path"
    );
    std::fs::remove_file(second.join("path-probe")).unwrap();
    let error = capture(Path::new("path-probe"), &[], 5, &environment)
        .await
        .expect_err("the only candidate is not executable");
    assert!(
        matches!(error, agent_run_core::Error::Io(error) if error.raw_os_error() == Some(libc::EACCES))
    );
}

/// Arguments and the explicitly supplied environment remain literal, with EOF on stdin.
#[tokio::test]
async fn command_receives_literal_arguments_environment_and_null_stdin() {
    let root = tempfile::tempdir().expect("temporary root");
    let mut environment = isolated(root.path());
    environment.insert("EXPLICIT".into(), "literal $HOME;* value".into());
    let args = vec![
        "-c".into(),
        "if IFS= read -r line; then exit 9; fi; printf '%s\\n%s\\n%s\\n%s\\n' \"$1\" \"$2\" \"$EXPLICIT\" \"${CARGO_MANIFEST_DIR-unset}\"".into(),
        "probe".into(),
        "a $HOME;*".into(),
        "--literal $(false)".into(),
    ];
    let result = capture(Path::new("/bin/sh"), &args, 5, &environment)
        .await
        .expect("literal invocation");
    assert_eq!(
        result,
        b"a $HOME;*\n--literal $(false)\nliteral $HOME;* value\nunset\n"
    );
}

/// Cancelling after startup terminates and reaps the verified leader and its child.
#[tokio::test]
async fn cancellation_cleans_up_the_started_group() {
    use agent_run_platform::process::{ProcessState, inspect, observe};

    let root = tempfile::tempdir().expect("temporary root");
    let home = root.path().to_owned();
    let environment = isolated(&home);
    let args = shell_args("echo $$ > \"$HOME/leader\"; sleep 60 & echo $! > \"$HOME/child\"; wait");
    let task =
        tokio::spawn(async move { capture(Path::new("/bin/sh"), &args, 60, &environment).await });
    let (leader, child) = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let read_pid = |name: &str| {
                std::fs::read_to_string(home.join(name))
                    .ok()
                    .and_then(|value| value.trim().parse::<i32>().ok())
            };
            if let (Some(leader), Some(child)) = (read_pid("leader"), read_pid("child")) {
                break (inspect(leader).unwrap(), inspect(child).unwrap());
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("fixture started and reported both process identities");
    assert_eq!(leader.group, leader.pid);
    assert_eq!(child.group, leader.pid);
    task.abort();
    assert!(task.await.unwrap_err().is_cancelled());
    tokio::time::timeout(Duration::from_secs(5), async {
        for process in [&leader, &child] {
            loop {
                match observe(Some(process.pid), Some(&process.token), Some(process.birth)) {
                    ProcessState::Dead | ProcessState::Reused => break,
                    ProcessState::Alive => tokio::time::sleep(Duration::from_millis(10)).await,
                    state => panic!("process cleanup evidence unavailable: {state:?}"),
                }
            }
        }
    })
    .await
    .expect("cancelled process group stopped");
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            match inspect(leader.pid) {
                Ok(current) if current.token == leader.token && current.birth == leader.birth => {
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
                Ok(_) => break,
                Err(error) if matches!(error.raw_os_error(), Some(libc::ENOENT | libc::ESRCH)) => {
                    break;
                }
                Err(error) => panic!("leader reap evidence unavailable: {error}"),
            }
        }
    })
    .await
    .expect("leader reaped, not only terminated");
}

/// A zero deadline never runs a command with an observable side effect.
#[tokio::test]
async fn zero_deadline_does_not_start_the_command() {
    let root = tempfile::tempdir().expect("temporary root");
    let args = shell_args("printf ran > \"$HOME/executed\"; printf version");
    assert!(
        capture(Path::new("/bin/sh"), &args, 0, &isolated(root.path()))
            .await
            .is_err()
    );
    assert!(!root.path().join("executed").exists());
}

/// Time spent waiting for a blocking worker consumes the command's deadline.
#[test]
fn deadline_cancels_a_queued_spawn() {
    let root = tempfile::tempdir().expect("temporary root");
    let home = root.path().to_owned();
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .max_blocking_threads(1)
        .build()
        .unwrap();
    runtime.block_on(async {
        let (started_tx, started_rx) = std::sync::mpsc::channel();
        let (release_tx, release_rx) = std::sync::mpsc::channel();
        let blocker = tokio::task::spawn_blocking(move || {
            started_tx.send(()).unwrap();
            release_rx.recv().unwrap();
        });
        started_rx
            .recv_timeout(Duration::from_secs(5))
            .expect("blocking pool occupied");
        let args = shell_args("printf ran > \"$HOME/executed\"; printf version");
        let start = Instant::now();
        let result = capture(Path::new("/bin/sh"), &args, 1, &isolated(&home)).await;
        assert!(result.is_err());
        assert!(start.elapsed() < Duration::from_secs(5));
        release_tx.send(()).unwrap();
        blocker.await.unwrap();
        tokio::task::spawn_blocking(|| ()).await.unwrap();
        assert!(!home.join("executed").exists());
    });
}

// NOT PORTED, deliberately:
// `tests/test_adapter_versions.py::test_grandchild_holding_stdout_cannot_outlive_the_deadline`
//
// Python's `_stop_process` (adapters/version.py:18-29) calls
// `os.killpg(checked_pgid(group), SIGKILL)` unconditionally, so a grandchild
// that holds stdout open after its leader exited is still killed. Rust's
// `OwnedProcess::signal` (agent-run-platform/src/process.rs:501-513) sends
// `kill(-pid)` only while the leader is observed `Alive`, which was chosen
// on purpose (see `docs/process-identity.md`) to avoid signalling a reused PID's
// group.
// A written-out port of this behavior fails against the current Rust code, and
// reconciling it changes that documented safety rule inside the supervisor
// lifecycle. It is recorded for an owner decision rather than resolved here.
