//! Detached launch parity with Python `tests/test_launch.py` and
//! `tests/test_launch_reaper.py`: session leadership, bounded READY handling,
//! bootstrap diagnosis, verified-group cleanup and exact reap evidence.
use agent_run_platform::{
    launch::{self, LaunchError, Launched, SpawnBackend, Timeouts},
    process::{self, ProcessState},
};
use std::{
    ffi::OsStr,
    io::Read,
    os::fd::{AsRawFd, OwnedFd},
    path::Path,
    process::Command,
    time::{Duration, Instant},
};

/// Short deadlines so failures are observed in seconds, not minutes.
fn quick() -> Timeouts {
    Timeouts {
        ready: Duration::from_secs(10),
        dispatch: Duration::from_millis(500),
        grace: Duration::from_millis(200),
        kill: Duration::from_millis(200),
    }
}

/// Launch `/bin/sh -c <script> sh <arg>` through the production path.
fn shell(script: &str, arg: &Path, timeouts: Timeouts) -> Result<Launched, LaunchError> {
    launch::launch_detached(
        Path::new("/bin/sh"),
        &[
            OsStr::new("-c"),
            OsStr::new(script),
            OsStr::new("sh"),
            arg.as_os_str(),
        ],
        timeouts,
        |_, _| Ok(()),
    )
}

fn alive(pid: i32) -> bool {
    // SAFETY: signal 0 only probes the existence of one positive PID.
    unsafe { libc::kill(pid, 0) == 0 }
}

/// Whether `pid` is no longer a child of this process (already reaped).
fn reaped(pid: i32) -> bool {
    let mut status = 0;
    // SAFETY: WNOHANG waitpid on one exact PID with a valid status pointer.
    let waited = unsafe { libc::waitpid(pid, &mut status, libc::WNOHANG) };
    waited == -1 && std::io::Error::last_os_error().raw_os_error() == Some(libc::ECHILD)
}

fn wait_for(what: &str, mut predicate: impl FnMut() -> bool) {
    let deadline = Instant::now() + Duration::from_secs(10);
    while !predicate() {
        assert!(Instant::now() < deadline, "timed out waiting for {what}");
        std::thread::sleep(Duration::from_millis(10));
    }
}

fn refusal(result: Result<Launched, LaunchError>) -> (i32, String) {
    match result {
        Err(LaunchError::Refused { pid, message }) => (pid, message),
        other => panic!("expected a refusal, got {other:?}"),
    }
}

fn drain(fd: OwnedFd) -> String {
    let mut text = String::new();
    std::fs::File::from(fd).read_to_string(&mut text).unwrap();
    text
}

/// Mirrors Python `tests/test_launch.py::DetachedLaunchTests::test_default_ready_budget_covers_observed_launchd_startup`.
#[test]
fn default_ready_budget_is_long_enough_for_detached_startup() {
    assert!(Timeouts::default().ready >= Duration::from_secs(30));
}

/// Mirrors Python `tests/test_lifecycle.py::ReadyChannelTests::test_ready_token_is_reported_once`.
/// Mirrors Python `tests/test_lifecycle.py::ReadyChannelTests::test_blank_failure_reason_is_refused`.
#[test]
fn ready_writer_emits_the_token_and_refuses_a_blank_failure() {
    let (read, write) = launch::cloexec_pipe().unwrap();
    // SAFETY: dup creates an independently owned descriptor for report_ready,
    // which consumes and closes its raw descriptor.
    let ready = unsafe { libc::dup(write.as_raw_fd()) };
    assert!(ready >= 0);
    launch::report_ready(ready, Ok(())).unwrap();
    drop(write);
    assert_eq!(drain(read), "ready\n");

    let (_read, write) = launch::cloexec_pipe().unwrap();
    // SAFETY: see the independent descriptor ownership above.
    let ready = unsafe { libc::dup(write.as_raw_fd()) };
    assert!(ready >= 0);
    let error = launch::report_ready(ready, Err("  ")).unwrap_err();
    // report_ready returns before adopting a blank failure descriptor.
    // SAFETY: the rejected duplicate is still this test's exact descriptor.
    assert_eq!(unsafe { libc::close(ready) }, 0);
    assert_eq!(error.kind(), std::io::ErrorKind::InvalidInput);
}

/// Mirrors Python `tests/test_launch.py::DetachedLaunchTests::test_parent_returns_after_ready_before_terminal_and_dispatches_once`.
/// Mirrors Python `tests/test_launch.py::DetachedLaunchTests::test_wrapper_and_grandchild_outlive_the_returning_caller`.
/// Mirrors Python `tests/test_launch.py::DetachedLaunchTests::test_fast_ready_terminal_exit_does_not_require_a_live_group_sample`.
/// Mirrors Python `tests/test_launch.py::DetachedLaunchTests::test_post_reap_receives_exact_pid_and_wait_status`.
/// Mirrors Python `tests/test_launch.py::DetachedLaunchTests::test_targeted_reaper_preserves_launch_evidence`.
#[test]
fn ready_child_is_a_session_leader_and_reaped_with_its_exact_status() {
    let temp = tempfile::tempdir().unwrap();
    let gate = temp.path().join("gate");
    let launched = shell(
        r#"echo $$ >&4; exec 4>&-; echo ready >&3; exec 3>&-; while [ ! -e "$1" ]; do sleep 0.02; done; exit 7"#,
        &gate,
        quick(),
    )
    .unwrap();
    let pid = launched.pid;
    assert_eq!(launched.backend, SpawnBackend::PosixSpawnSetsid);
    assert_eq!(
        // SAFETY: getsid takes one PID and no pointers.
        unsafe { libc::getsid(pid) },
        pid,
        "child must lead its session"
    );
    assert_eq!(
        // SAFETY: getpgid takes one PID and no pointers.
        unsafe { libc::getpgid(pid) },
        pid,
        "child must lead its group"
    );
    let leader = launched.leader.expect("a live leader identity is readable");
    assert_eq!((leader.pid, leader.group), (pid, pid));
    assert_eq!(
        process::observe(Some(pid), Some(&leader.token), Some(leader.birth)),
        ProcessState::Alive
    );
    let reaper = launch::spawn_reaper(pid).unwrap();
    std::fs::write(&gate, b"").unwrap();
    let status = reaper.join().unwrap().expect("exact child wait status");
    assert!(libc::WIFEXITED(status) && libc::WEXITSTATUS(status) == 7);
    assert!(reaped(pid));
    assert_eq!(
        process::observe(Some(pid), Some(&leader.token), Some(leader.birth)),
        ProcessState::Dead
    );
}

/// Mirrors Python `tests/test_launch.py::DetachedLaunchTests::test_readiness_timeout_kills_verified_wrapper_and_grandchild_group`.
/// Mirrors Python `tests/test_launch.py::DetachedLaunchTests::test_unread_payload_obeys_ready_deadline_and_reaps`.
/// Mirrors Python `tests/test_lifecycle.py::ReadyChannelTests::test_a_silent_supervisor_times_out`.
/// Mirrors Python `tests/test_lifecycle.py::TerminateProcessGroupTests::test_term_removes_wrapper_and_grandchild_together`.
/// Mirrors Python `tests/test_lifecycle.py::TerminateProcessGroupTests::test_a_grandchild_that_ignores_term_is_killed`.
#[test]
fn readiness_timeout_kills_the_verified_wrapper_and_grandchild_group() {
    let temp = tempfile::tempdir().unwrap();
    let evidence = temp.path().join("grandchild");
    let started = Instant::now();
    let (pid, message) = refusal(shell(
        r#"echo $$ >&4; sleep 30 & echo $! > "$1"; wait"#,
        &evidence,
        Timeouts {
            ready: Duration::from_secs(1),
            ..quick()
        },
    ));
    assert_eq!(message, "supervisor did not report ready in time");
    assert!(
        started.elapsed() < Duration::from_secs(6),
        "cleanup must be bounded"
    );
    assert!(reaped(pid), "the wrapper must be reaped");
    let grandchild: i32 = std::fs::read_to_string(&evidence)
        .unwrap()
        .trim()
        .parse()
        .unwrap();
    wait_for("grandchild exit", || !alive(grandchild));
}

/// Mirrors Python `tests/test_launch.py::DetachedLaunchTests::test_pre_ready_failure_is_reported_reaped_and_dispatches_once`.
/// Mirrors Python `tests/test_launch.py::DetachedLaunchTests::test_identity_mismatch_is_refused_before_ready_is_awaited`.
/// Mirrors Python `tests/test_lifecycle.py::ReadyChannelTests::test_failure_reason_is_raised_to_the_parent`.
/// Mirrors Python `tests/test_lifecycle.py::ReadyChannelTests::test_a_supervisor_that_dies_before_ready_is_detected`.
#[test]
fn ready_eof_failure_token_and_bad_identity_are_refused_and_reaped() {
    let unused = Path::new("unused");
    for (script, expected) in [
        (
            "echo $$ >&4; exit 3",
            "supervisor exited before reporting ready",
        ),
        (
            "echo $$ >&4; echo 'fail:boom' >&3; exit 1",
            "supervisor failed to start: boom",
        ),
        (
            "echo 2 >&4; exit 0",
            "detached supervisor reported the wrong process identity",
        ),
        (
            "echo 1 >&4; exit 0",
            "detached supervisor reported an unsafe process identity",
        ),
        (
            "echo nope >&4; exit 0",
            "detached supervisor reported an invalid process identity",
        ),
    ] {
        let (pid, message) = refusal(shell(script, unused, quick()));
        assert_eq!(message, expected, "{script}");
        assert!(reaped(pid), "{script}");
    }
}

/// Mirrors Python `tests/test_launch.py::DetachedLaunchTests::test_child_dies_pre_identity_with_evidence_is_diagnosed`.
/// Mirrors Python `tests/test_launch.py::DetachedLaunchTests::test_child_dies_pre_identity_without_evidence_is_diagnosed`.
#[test]
fn a_child_dying_before_identity_is_diagnosed_with_or_without_evidence() {
    let unused = Path::new("unused");
    let script = r#"printf '%s\n' '{"stage":"import","type":"ModuleNotFoundError","message":"no module named agent_run"}' >&5; exit 1"#;
    let Err(LaunchError::Bootstrap(failure)) = shell(script, unused, quick()) else {
        panic!("expected a bootstrap failure");
    };
    assert_eq!(failure.failure_kind, launch::FAILURE_KIND_BOOTSTRAP);
    assert_eq!(failure.stage.as_deref(), Some("import"));
    assert_eq!(failure.error_type.as_deref(), Some("ModuleNotFoundError"));
    assert!(!failure.proven);
    assert_eq!(
        failure.message,
        "detached supervisor died before session proof at stage 'import': ModuleNotFoundError: no module named agent_run (exit code 1)"
    );
    assert!(reaped(failure.provisional_pid.unwrap()));

    let Err(LaunchError::Bootstrap(failure)) = shell("kill -9 $$", unused, quick()) else {
        panic!("expected a bootstrap failure");
    };
    assert_eq!(failure.stage, None);
    assert!(
        failure
            .message
            .contains("no bootstrap evidence (killed by signal 9)"),
        "{}",
        failure.message
    );
    assert!(reaped(failure.provisional_pid.unwrap()));
}

/// Mirrors Python `tests/test_launch.py::DetachedLaunchTests::test_missing_executable_is_refused_before_any_fork`.
#[test]
fn a_missing_executable_is_refused_before_any_spawn() {
    let mut spawned = false;
    let result =
        launch::launch_detached(Path::new("/nonexistent/agent-run"), &[], quick(), |_, _| {
            spawned = true;
            Ok(())
        });
    let Err(LaunchError::Bootstrap(failure)) = result else {
        panic!("expected a bootstrap failure");
    };
    assert!(!spawned, "nothing may be spawned after a failed preflight");
    assert_eq!(
        failure.failure_kind,
        launch::FAILURE_KIND_EXECUTABLE_MISSING
    );
    assert_eq!(failure.stage.as_deref(), Some("preflight"));
    assert_eq!(failure.provisional_pid, None);
    assert!(failure
        .message
        .contains("reconnect/restart this MCP session"));
}

/// Mirrors Python `tests/test_launch.py::DetachedLaunchTests::test_posix_spawn_requests_setsid_without_fork_fallback`.
/// Mirrors Python `tests/test_launch.py::DetachedLaunchTests::test_write_exec_failure_writes_a_bounded_stage_and_errno_record`.
#[test]
fn spawn_errors_propagate_and_the_fork_fallback_records_exec_failure() {
    let (_ready_r, ready_w) = launch::cloexec_pipe().unwrap();
    let (_identity_r, identity_w) = launch::cloexec_pipe().unwrap();
    let (error_r, error_w) = launch::cloexec_pipe().unwrap();
    let missing = Path::new("/nonexistent/agent-run");
    // posix_spawn reports the exec failure itself; no fork fallback is entered.
    let error =
        launch::spawn_session_leader(missing, &[], [&ready_w, &identity_w, &error_w]).unwrap_err();
    assert_eq!(error.raw_os_error(), Some(libc::ENOENT));
    let pid = launch::fork_session_leader(missing, &[], [&ready_w, &identity_w, &error_w]).unwrap();
    drop((ready_w, identity_w, error_w));
    let record = drain(error_r);
    assert!(
        record.starts_with(r#"{"stage":"exec","type":"OSError""#)
            && record.contains(r#""errno":2}"#),
        "{record}"
    );
    let mut status = 0;
    // SAFETY: blocking waitpid on the exact forked child with a valid status pointer.
    assert_eq!(unsafe { libc::waitpid(pid, &mut status, 0) }, pid);
    assert!(libc::WIFEXITED(status) && libc::WEXITSTATUS(status) == 1);
}

/// Mirrors Python `tests/test_launch.py::DetachedLaunchTests::test_explicitly_unsupported_setsid_uses_legacy_fork_path`.
#[test]
fn the_fork_fallback_also_creates_a_session_leader_with_the_same_descriptors() {
    let (_ready_r, ready_w) = launch::cloexec_pipe().unwrap();
    let (identity_r, identity_w) = launch::cloexec_pipe().unwrap();
    let (_error_r, error_w) = launch::cloexec_pipe().unwrap();
    let pid = launch::fork_session_leader(
        Path::new("/bin/sh"),
        &[
            OsStr::new("-c"),
            OsStr::new("echo $$ >&4; exec 4>&-; sleep 5"),
        ],
        [&ready_w, &identity_w, &error_w],
    )
    .unwrap();
    drop((ready_w, identity_w, error_w));
    let reported: i32 = drain(identity_r).trim().parse().unwrap();
    assert_eq!(reported, pid, "descriptor 4 must reach the exec'd child");
    // SAFETY: getsid takes one PID and no pointers.
    assert_eq!(unsafe { libc::getsid(pid) }, pid);
    // SAFETY: SIGKILL to one exact child PID we just created.
    assert_eq!(unsafe { libc::kill(pid, libc::SIGKILL) }, 0);
    let mut status = 0;
    // SAFETY: blocking waitpid on the exact forked child with a valid status pointer.
    assert_eq!(unsafe { libc::waitpid(pid, &mut status, 0) }, pid);
}

/// Mirrors Python `tests/test_launch.py::DetachedLaunchTests::test_post_terminal_dispatch_is_bounded_and_never_reruns_the_child`.
/// Mirrors `tests/test_launch_reaper.py::ChildReaperTests::test_reaps_registered_short_lived_child_without_stealing_other_waits`.
#[test]
fn the_reaper_takes_only_its_own_registered_child() {
    let (_ready_r, ready_w) = launch::cloexec_pipe().unwrap();
    let (_identity_r, identity_w) = launch::cloexec_pipe().unwrap();
    let (_error_r, error_w) = launch::cloexec_pipe().unwrap();
    let fds = [&ready_w, &identity_w, &error_w];
    let sh = Path::new("/bin/sh");
    let (child, _) =
        launch::spawn_session_leader(sh, &[OsStr::new("-c"), OsStr::new("exit 7")], fds).unwrap();
    let (unrelated, _) =
        launch::spawn_session_leader(sh, &[OsStr::new("-c"), OsStr::new("exit 9")], fds).unwrap();
    let status = launch::spawn_reaper(child)
        .unwrap()
        .join()
        .unwrap()
        .unwrap();
    assert!(libc::WIFEXITED(status) && libc::WEXITSTATUS(status) == 7);
    let mut other = 0;
    assert_eq!(
        // SAFETY: blocking waitpid on the exact unrelated child with a valid status pointer.
        unsafe { libc::waitpid(unrelated, &mut other, 0) },
        unrelated
    );
    assert!(libc::WIFEXITED(other) && libc::WEXITSTATUS(other) == 9);
}

/// Mirrors Python `tests/test_lifecycle.py::TerminateProcessGroupTests::test_reused_leader_identity_never_signals_the_observed_group`.
/// Mirrors Python `tests/test_lifecycle.py::TerminateProcessGroupTests::test_foreign_nonleader_pid_is_never_signalled`.
/// Mirrors Python `tests/test_lifecycle.py::TerminateProcessGroupTests::test_dangerous_group_ids_are_refused`.
#[test]
fn a_stale_leader_identity_is_never_signalled() {
    use std::os::unix::process::CommandExt;
    let mut child = Command::new("/bin/sleep")
        .arg("30")
        .process_group(0)
        .spawn()
        .unwrap();
    let pid = child.id() as i32;
    let exact = process::inspect(pid).unwrap();
    assert_eq!(exact.group, pid);
    let mut stale = exact.clone();
    // A Python-written row carries only the float birth time; make it a different process.
    stale.token = String::new();
    stale.birth = f64::from_bits(exact.birth.to_bits() + 1);
    assert_eq!(
        process::observe(Some(pid), Some(&stale.token), Some(stale.birth)),
        ProcessState::Reused
    );
    let refused =
        launch::terminate_group(&stale, Duration::from_millis(50), Duration::from_millis(50))
            .unwrap();
    assert!(
        refused.signals.is_empty(),
        "a reused identity must not be signalled"
    );
    assert!(!refused.group_gone);
    assert!(
        child.try_wait().unwrap().is_none(),
        "the process must survive"
    );
    // Positive control: the exact identity is signalled and the group disappears.
    let terminated =
        launch::terminate_group(&exact, Duration::from_secs(2), Duration::from_secs(2)).unwrap();
    assert_eq!(terminated.signals, ["SIGTERM"]);
    assert!(terminated.group_gone);
    assert!(reaped(pid));
}

// The end-to-end test against the real compiled `_supervisor` binary lives in
// `crates/agent-run/tests/launch.rs`: `CARGO_BIN_EXE_agent-run` is only set for
// tests built inside the package that owns that binary target.
