//! Process birth identity parity with Python `tests/test_process_identity.py`,
//! including a differential check against psutil itself.
use agent_run_platform::process::{self, ProcessState};
use std::{
    process::{Command, Stdio},
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

/// Assert that Rust observes exactly the float psutil stored for `pid`.
fn compare_with_psutil(pid: i32, expected: f64) {
    let identity = process::inspect(pid)
        .unwrap_or_else(|error| panic!("psutil read pid {pid} but Rust could not: {error}"));
    eprintln!(
        "pid={pid} psutil={expected:?} rust={:?} bits={:#x}/{:#x} token={}",
        identity.birth,
        expected.to_bits(),
        identity.birth.to_bits(),
        identity.token
    );
    assert_eq!(identity.birth.to_bits(), expected.to_bits(), "pid {pid}");
}

/// Differential evidence against Python psutil, which wrote every stored
/// `supervisor_birth_time` this port must keep reading.
///
/// `AGENT_RUN_DIFF_PID` plus `AGENT_RUN_DIFF_BIRTH` compare one PID whose
/// create_time the caller already read with psutil; `AGENT_RUN_PSUTIL_PYTHON`
/// runs the probe here instead, where the sandbox allows that interpreter.
#[test]
fn birth_matches_python_psutil_bit_for_bit() {
    if let (Ok(pid), Ok(birth)) = (
        std::env::var("AGENT_RUN_DIFF_PID"),
        std::env::var("AGENT_RUN_DIFF_BIRTH"),
    ) {
        compare_with_psutil(pid.trim().parse().unwrap(), birth.trim().parse().unwrap());
    }
    let Some(python) = std::env::var_os("AGENT_RUN_PSUTIL_PYTHON") else {
        eprintln!("skipped: AGENT_RUN_PSUTIL_PYTHON is not set");
        return;
    };
    const PROBE: &str = "import psutil, sys\ntry:\n    print(repr(psutil.Process(int(sys.argv[1])).create_time()))\nexcept psutil.Error as error:\n    print(type(error).__name__)";
    let mut child = Command::new("/bin/sleep").arg("30").spawn().unwrap();
    let mut pids = vec![child.id() as i32, std::process::id() as i32];
    // One root-owned process exercises foreign-user readability, when listable.
    if let Ok(listing) = Command::new("/bin/ps").args(["-axo", "pid=,uid="]).output() {
        pids.extend(
            String::from_utf8_lossy(&listing.stdout)
                .lines()
                .filter_map(|line| {
                    let mut fields = line.split_whitespace();
                    let pid: i32 = fields.next()?.parse().ok()?;
                    (fields.next()? == "0" && pid > 1).then_some(pid)
                })
                .take(1),
        );
    }
    let mut probed = 0;
    for pid in pids {
        let probe = match Command::new(&python)
            .args(["-c", PROBE, &pid.to_string()])
            .stderr(Stdio::inherit())
            .output()
        {
            Ok(probe) => probe,
            Err(error) => {
                eprintln!("skipped: the psutil probe cannot run here: {error}");
                break;
            }
        };
        let psutil = String::from_utf8_lossy(&probe.stdout).trim().to_owned();
        match psutil.parse::<f64>() {
            Ok(expected) => {
                compare_with_psutil(pid, expected);
                probed += 1;
            }
            Err(_) => eprintln!("pid={pid} psutil={psutil} rust={:?}", process::inspect(pid)),
        }
    }
    child.kill().unwrap();
    child.wait().unwrap();
    assert!(
        probed == 0 || probed >= 2,
        "psutil must read this process and its child"
    );
}
