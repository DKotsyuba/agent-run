//! Bounded process behavior for the retained native metadata probes.
use agent_run_core::capacity::sources::{self, capture};
use agent_run_domain::Error;
use std::{collections::BTreeMap, path::Path};

/// Builds an output fixture that waits until its process owner has been captured.
fn capture_script(output: &str, status: &str, gate: &Path) -> (std::path::PathBuf, Vec<String>) {
    (
        std::path::PathBuf::from("/bin/sh"),
        vec![
            "-c".into(),
            "while [ ! -f \"$1\" ]; do /bin/sleep 0.01; done; printf '%s' \"$2\"; exit \"$3\""
                .into(),
            "fixture".into(),
            gate.to_string_lossy().into_owned(),
            output.into(),
            status.into(),
        ],
    )
}

/// Returns a no-op environment for isolated fake metadata probes.
fn empty_environment() -> BTreeMap<String, String> {
    BTreeMap::new()
}

/// A failing bounded source process yields fixed text, never its output.
// Mirrors `tests/test_capacity_sources.py::CodexbarMappingTests::test_failure_logs_and_exception_are_secret_safe`.
#[tokio::test]
async fn capture_returns_fixed_secret_free_failures() {
    let error = sources::capture(
        Path::new("/bin/sh"),
        &["-c".into(), "printf provider-secret; exit 1".into()],
        1,
        &BTreeMap::new(),
    )
    .await
    .expect_err("nonzero source must fail");
    assert!(!error.to_string().contains("provider-secret"));
}

// Mirrors `tests/test_capacity_codex_appserver.py::FetchRateLimitsTests::test_returns_direct_result_and_terminates`
#[tokio::test(flavor = "current_thread")]
async fn python_test_capacity_codex_appserver_capture_returns_direct_output() {
    let directory = tempfile::tempdir().unwrap();
    let gate = directory.path().join("release");
    let (binary, args) = capture_script("capacity", "0", &gate);
    let (started, ready) = tokio::sync::oneshot::channel();
    let task = tokio::spawn(async move {
        started.send(()).unwrap();
        capture(&binary, &args, 5, &empty_environment()).await
    });
    // On this single-threaded runtime, the capture task reaches its first
    // await only after capturing ownership of the waiting child.
    ready.await.unwrap();
    std::fs::write(gate, b"ready").unwrap();
    let bytes = task.await.unwrap().unwrap();
    assert_eq!(bytes, b"capacity");
}

// Mirrors `tests/test_capacity_codex_appserver.py::FetchRateLimitsTests::test_timeout_propagates_and_transport_is_still_terminated`
// Mirrors `tests/test_capacity_codex_appserver.py::FetchRateLimitsTests::test_real_transport_times_out_and_reaps_the_child`
#[tokio::test]
async fn python_test_capacity_codex_appserver_capture_timeout_is_bounded() {
    let error = capture(
        &std::path::PathBuf::from("/bin/sh"),
        &["-c".into(), "sleep 30".into()],
        1,
        &empty_environment(),
    )
    .await;
    assert!(
        matches!(error, Err(Error::Validation(message)) if message == "metadata command timed out")
    );
}
