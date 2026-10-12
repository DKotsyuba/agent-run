//! End-to-end launch parity against the real compiled `_supervisor` binary.
//! Complements the `launch` primitive tests in `agent-run-platform`, which use
//! `/bin/sh` in place of a built binary; this test needs the real binary, so it
//! lives in the package that produces it (`CARGO_BIN_EXE_agent-run` is only set
//! for tests built inside that package).
use agent_run_platform::launch::{self, LaunchError, Launched, Timeouts};
use std::{ffi::OsStr, path::Path, time::Duration};

/// Short deadlines so failures are observed in seconds, not minutes.
fn quick() -> Timeouts {
    Timeouts {
        ready: Duration::from_secs(10),
        dispatch: Duration::from_millis(500),
        grace: Duration::from_millis(200),
        kill: Duration::from_millis(200),
    }
}

fn refusal(result: Result<Launched, LaunchError>) -> (i32, String) {
    match result {
        Err(LaunchError::Refused { pid, message }) => (pid, message),
        other => panic!("expected a refusal, got {other:?}"),
    }
}

/// Whether `pid` is no longer a child of this process (already reaped).
fn reaped(pid: i32) -> bool {
    let mut status = 0;
    // SAFETY: WNOHANG waitpid on one exact PID with a valid status pointer.
    let waited = unsafe { libc::waitpid(pid, &mut status, libc::WNOHANG) };
    waited == -1 && std::io::Error::last_os_error().raw_os_error() == Some(libc::ECHILD)
}

/// The real `_supervisor` binary proves its session identity to the launcher
/// before it reports that it cannot start (here: no broker serves the home), and
/// the launcher reaps it.
#[test]
fn the_real_supervisor_binary_proves_identity_before_reporting_failure() {
    let home = tempfile::tempdir().unwrap();
    // The supervisor now authenticates with a launch capability, so the test
    // hands it one exactly like the broker's launch does.
    let args = [
        OsStr::new("--home"),
        home.path().as_os_str(),
        OsStr::new("_supervisor"),
        OsStr::new("ag-20260916-120000-0123456789"),
    ];
    let mut started = launch::spawn_launch(
        Path::new(env!("CARGO_BIN_EXE_agent-run")),
        &args,
        quick(),
        true,
    )
    .expect("spawn");
    started
        .release_capability(&[b'a'; 64])
        .expect("capability released");
    let result = started.complete(quick());
    let (pid, message) = refusal(result);
    // Identity was proven by the real child; only the missing run made it fail.
    assert!(
        message.starts_with("supervisor failed to start: "),
        "{message}"
    );
    assert!(reaped(pid));
}
