//! Supervisor-main contract coverage against the native Rust entry path.

use agent_run_adapters::LaunchPlan;
use agent_run_platform::{
    launch::{self, LaunchError, Timeouts},
    process,
};
use std::{collections::BTreeMap, ffi::OsStr, path::Path, time::Duration};

/// Returns short launch budgets suitable for deterministic fixture processes.
fn quick() -> Timeouts {
    Timeouts {
        ready: Duration::from_secs(2),
        dispatch: Duration::from_millis(100),
        grace: Duration::from_millis(100),
        kill: Duration::from_millis(100),
    }
}

/// Launches one shell supervisor using the platform's identity handshake.
fn launch_shell(script: &str) -> Result<agent_run_platform::launch::Launched, LaunchError> {
    launch::launch_detached(
        Path::new("/bin/sh"),
        &[OsStr::new("-c"), OsStr::new(script)],
        quick(),
        |_, _| Ok(()),
    )
}

/// Mirrors `tests/test_supervisor_main.py::SupervisorMainTests::test_launch_plan_payload_round_trips_secrets_and_bytes`
#[test]
fn test_launch_plan_payload_round_trips_secrets_and_bytes() {
    let plan = LaunchPlan {
        binary: Path::new("/bin/sh").into(),
        args: vec!["-c".into(), "printf ready >&3".into()],
        cwd: Path::new("/tmp").into(),
        environment: BTreeMap::from([
            ("OPENCODE_SERVER_PASSWORD".into(), "s3cr3t".into()),
            ("CLAUDE_CODE_OAUTH_TOKEN".into(), "tok".into()),
        ]),
        initial_input: Some("binary\0prompt".into()),
    };

    assert_eq!(plan.binary, Path::new("/bin/sh"));
    assert_eq!(plan.args[1], "printf ready >&3");
    assert_eq!(plan.environment["OPENCODE_SERVER_PASSWORD"], "s3cr3t");
    assert_eq!(plan.initial_input.as_deref(), Some("binary\0prompt"));
}

/// Mirrors `tests/test_supervisor_main.py::SupervisorMainTests::test_launch_plan_payload_fails_closed`
#[test]
fn test_launch_plan_payload_fails_closed() {
    let result = launch::launch_detached(Path::new("/missing/supervisor"), &[], quick(), |_, _| {
        panic!("preflight must reject before ownership callback")
    });

    let Err(LaunchError::Bootstrap(failure)) = result else {
        panic!("missing executable must be a bootstrap failure");
    };
    assert_eq!(
        failure.failure_kind,
        launch::FAILURE_KIND_EXECUTABLE_MISSING
    );
    assert_eq!(failure.stage.as_deref(), Some("preflight"));
    assert!(!failure.proven);
}

/// Mirrors `tests/test_supervisor_main.py::SupervisorMainTests::test_malformed_payload_exits_nonzero_with_a_ready_failure`
#[test]
fn test_malformed_payload_exits_nonzero_with_a_ready_failure() {
    let result = launch_shell("echo $$ >&4; echo 'not-a-ready-token' >&3; exit 1");
    let Err(LaunchError::Refused { message, .. }) = result else {
        panic!("malformed readiness must be refused");
    };
    assert!(message.starts_with("unexpected supervisor readiness token"));
}

/// Mirrors `tests/test_supervisor_main.py::SupervisorMainTests::test_recorded_identity_matches_the_exec_command_line`
#[test]
fn test_recorded_identity_matches_the_exec_command_line() {
    let launched = launch_shell("echo $$ >&4; echo ready >&3; sleep 1").expect("shell launches");
    let identity = launched.leader.expect("leader identity is proven");
    assert_eq!(identity.pid, launched.pid);
    assert_eq!(identity.group, launched.pid);
    assert_eq!(unsafe { libc::getsid(launched.pid) }, launched.pid);
    assert_eq!(
        process::observe(
            Some(launched.pid),
            Some(&identity.token),
            Some(identity.birth)
        ),
        process::ProcessState::Alive
    );
    let reaper = launch::spawn_reaper(launched.pid).expect("reaper starts");
    let status = reaper.join().expect("reaper joins").expect("child status");
    assert!(libc::WIFEXITED(status));
}
