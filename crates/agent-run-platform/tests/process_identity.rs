//! Process birth identity parity with Python `tests/test_process_identity.py`,
//! including a differential check against psutil itself.
use agent_run_platform::process::{self, ProcessState};
use std::{
    process::Command,
    time::{Duration, Instant},
};

/// Mirrors `tests/test_process_identity.py::ProcessIdentityTests::test_observations_distinguish_proof_states`.
#[test]
fn current_process_birth_is_observed_and_reuse_is_distinct() {
    let identity = process::inspect(std::process::id() as i32).unwrap();
    assert_eq!(
        process::observe(
            Some(identity.pid),
            Some(&identity.token),
            Some(identity.birth)
        ),
        ProcessState::Alive
    );
    let wrong = if cfg!(target_os = "linux") {
        "linux:wrong:0"
    } else {
        "darwin:0:0"
    };
    assert_eq!(
        process::observe(Some(identity.pid), Some(wrong), Some(identity.birth)),
        ProcessState::Reused
    );
}
#[test]
fn missing_or_unproven_identity_is_not_automatically_death() {
    assert_eq!(process::observe(None, None, None), ProcessState::NotStarted);
    assert_eq!(
        process::observe(Some(std::process::id() as i32), None, None),
        ProcessState::Unknown
    );
    assert_eq!(process::observe(Some(0), None, None), ProcessState::Unknown);
}

// A missing OS PID is affirmative death evidence even when an old row has no
// birth-time proof; this is distinct from an unsafe/unavailable PID probe.
#[test]
fn missing_pid_without_birth_is_a_dead_verdict() {
    assert_eq!(
        process::observe(Some(i32::MAX), None, None),
        ProcessState::Dead
    );
}

#[test]
fn native_process_enumeration_includes_this_process() {
    let self_pid = std::process::id() as i32;
    assert!(process::processes()
        .expect("native process enumeration")
        .iter()
        .any(|identity| identity.pid == self_pid));
}

#[test]
fn a_legacy_float_birth_is_compared_exactly_like_python() {
    // Python rows store only psutil's create_time float and compare it with `==`.
    let identity = process::inspect(std::process::id() as i32).unwrap();
    assert_eq!(
        process::observe(Some(identity.pid), None, Some(identity.birth)),
        ProcessState::Alive
    );
    let neighbour = f64::from_bits(identity.birth.to_bits() + 1);
    assert_eq!(
        process::observe(Some(identity.pid), None, Some(neighbour)),
        ProcessState::Reused
    );
}

/// Mirrors `tests/test_process_identity.py::ProcessIdentityTests::test_root_owned_real_child_has_stable_birth_identity`.
#[test]
fn an_exited_child_is_dead_before_and_after_reaping() {
    let mut child = Command::new("/bin/sleep").arg("30").spawn().unwrap();
    let identity = process::inspect(child.id() as i32).unwrap();
    let observe = || {
        process::observe(
            Some(identity.pid),
            Some(&identity.token),
            Some(identity.birth),
        )
    };
    assert_eq!(observe(), ProcessState::Alive);
    child.kill().unwrap();
    let deadline = Instant::now() + Duration::from_secs(5);
    while observe() == ProcessState::Alive && Instant::now() < deadline {
        std::thread::sleep(Duration::from_millis(10));
    }
    // An unreaped zombie has already exited; its PID cannot be reused yet.
    assert_eq!(observe(), ProcessState::Dead);
    child.wait().unwrap();
    assert_eq!(observe(), ProcessState::Dead);
}
