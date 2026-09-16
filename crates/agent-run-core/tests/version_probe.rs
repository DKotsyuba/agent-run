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
    path::{Path, PathBuf},
    time::{Duration, Instant},
};

/// Writes one executable `/bin/sh` fixture holding `body` and returns its path.
///
/// `directory` owns the fixture; `body` is placed verbatim after the shebang.
/// The file is created private-executable (`0700`), like Python's fixture.
fn executable(directory: &Path, body: &str) -> PathBuf {
    let path = directory.join("runtime");
    std::fs::write(&path, format!("#!/bin/sh\n{body}\n")).expect("fixture written");
    std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o700)).expect("fixture mode");
    path
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
    let binary = executable(root.path(), "printf 'runtime 1.0\\n'");
    let first = capture(&binary, &["--version".into()], 5, &isolated(root.path()))
        .await
        .expect("first observation");
    assert_eq!(String::from_utf8_lossy(&first).trim(), "runtime 1.0");

    // The same configured path now reports a different version: each call must
    // execute the binary again rather than answer from a retained observation.
    let binary = executable(root.path(), "printf 'runtime 2.0\\n'");
    let second = capture(&binary, &["--version".into()], 5, &isolated(root.path()))
        .await
        .expect("second observation");
    assert_eq!(String::from_utf8_lossy(&second).trim(), "runtime 2.0");
}

/// Mirrors `tests/test_adapter_versions.py::test_version_failures_are_bounded[exit 7-status 7]`
#[tokio::test]
async fn nonzero_status_is_a_bounded_failure() {
    let root = tempfile::tempdir().expect("temporary root");
    let binary = executable(root.path(), "exit 7");
    let result = capture(&binary, &["--version".into()], 5, &isolated(root.path())).await;
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
    let binary = executable(root.path(), "yes x | head -c 2200000");
    let result = capture(&binary, &["--version".into()], 10, &isolated(root.path())).await;
    assert!(
        result.is_err(),
        "output beyond the bound must be refused, not retained"
    );
}

/// Mirrors `tests/test_adapter_versions.py::test_version_failures_are_bounded[printf 'stderr-only\n' >&2-no version]`
#[tokio::test]
async fn stderr_only_version_failure_is_bounded() {
    let root = tempfile::tempdir().expect("temporary root");
    let binary = executable(root.path(), "printf 'stderr-only\\n' >&2");
    let result = capture(&binary, &["--version".into()], 5, &isolated(root.path())).await;
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

    let binary = executable(root.path(), "sleep 60");
    let started = Instant::now();
    let result = capture(&binary, &["--version".into()], 1, &isolated(root.path())).await;
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
    let binary = executable(
        root.path(),
        "if [ -n \"$CARGO_MANIFEST_DIR\" ]; then printf leaked; else printf 'clean 1\\n'; fi",
    );
    let observed = capture(&binary, &["--version".into()], 5, &isolated(root.path()))
        .await
        .expect("isolated observation");
    assert_eq!(String::from_utf8_lossy(&observed).trim(), "clean 1");
}

// NOT PORTED, deliberately:
// `tests/test_adapter_versions.py::test_grandchild_holding_stdout_cannot_outlive_the_deadline`
//
// Python's `_stop_process` (adapters/version.py:18-29) calls
// `os.killpg(checked_pgid(group), SIGKILL)` unconditionally, so a grandchild
// that holds stdout open after its leader exited is still killed. Rust's
// `OwnedProcess::signal` (agent-run-platform/src/process.rs:501-513) sends
// `kill(-pid)` only while the leader is observed `Alive`, which ADR A10
// decision 5 established on purpose to avoid signalling a reused PID's group.
// A written-out port of this behavior fails against the current Rust code, and
// reconciling it changes that documented safety rule inside the supervisor
// lifecycle. It is recorded for an owner decision rather than resolved here.
