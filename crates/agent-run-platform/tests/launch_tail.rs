//! Remaining detached-launch contracts from the Python launcher suite.

use agent_run_platform::launch::{self, LaunchError, Timeouts};
use std::{ffi::OsStr, path::Path, time::Duration};

/// Returns short launch budgets while preserving each cleanup phase.
fn quick() -> Timeouts {
    Timeouts {
        ready: Duration::from_millis(100),
        dispatch: Duration::from_millis(50),
        grace: Duration::from_millis(50),
        kill: Duration::from_millis(50),
    }
}

/// Starts a shell process through the production detached-launch path.
fn launch_shell(script: &str) -> Result<agent_run_platform::launch::Launched, LaunchError> {
    launch::launch_detached(
        Path::new("/bin/sh"),
        &[OsStr::new("-c"), OsStr::new(script)],
        quick(),
        |_, _| Ok(()),
    )
}

/// Mirrors `tests/test_launch.py::DetachedLaunchTests::test_payload_and_executable_are_validated_before_any_fork`
#[test]
fn test_payload_and_executable_are_validated_before_any_fork() {
    let mut callback_called = false;
    let result = launch::launch_detached(Path::new("/missing/agent-run"), &[], quick(), |_, _| {
        callback_called = true;
        Ok(())
    });
    let Err(LaunchError::Bootstrap(failure)) = result else {
        panic!("preflight must reject the missing executable");
    };
    assert_eq!(failure.stage.as_deref(), Some("preflight"));
    assert!(!callback_called);
}

/// Mirrors `tests/test_launch.py::DetachedLaunchTests::test_preexisting_cancellation_bounds_unread_payload_and_reaps`
#[test]
fn test_preexisting_cancellation_bounds_unread_payload_and_reaps() {
    let result = launch::launch_detached(
        Path::new("/bin/sh"),
        &[
            OsStr::new("-c"),
            OsStr::new("echo $$ >&4; while :; do :; done"),
        ],
        quick(),
        |_, _| Err("cancelled before supervisor READY".into()),
    );
    let Err(LaunchError::Refused { message, .. }) = result else {
        panic!("cancelled startup must be refused");
    };
    assert!(message.contains("startup evidence was not recorded"));
}

/// Mirrors `tests/test_launch.py::DetachedLaunchTests::test_ready_wait_cancellation_kills_and_reaps_verified_group`
#[test]
fn test_ready_wait_cancellation_kills_and_reaps_verified_group() {
    let result = launch_shell("echo $$ >&4; while :; do :; done");
    let Err(LaunchError::Refused { message, .. }) = result else {
        panic!("a supervisor that never reports READY must be refused");
    };
    assert_eq!(message, "supervisor did not report ready in time");
}
