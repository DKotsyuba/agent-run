//! Split detached launch and the private capability descriptor: the child is
//! gated on the capability until durable evidence exists, the secret never
//! reaches argv or the environment, and every failure path cleans up a verified
//! child.
use agent_run_platform::launch::{self, LaunchError, Timeouts};
use std::{
    ffi::OsStr,
    io::{ErrorKind, Write},
    os::fd::{AsRawFd, IntoRawFd},
    path::Path,
    time::{Duration, Instant},
};

/// Short deadlines so failures are observed in seconds.
fn quick() -> Timeouts {
    Timeouts {
        ready: Duration::from_secs(10),
        dispatch: Duration::from_millis(500),
        grace: Duration::from_millis(200),
        kill: Duration::from_millis(200),
    }
}

/// Spawn the fixture shell with a capability pipe and an explicitly EMPTY child
/// environment (`env -i`), so no ambient `AGENT_RUN_HOME` or worker flag can reach
/// it; the test process's own environment is never touched. The appended launch
/// flags appear after `out`: `$8 $9` is `--capability-fd 6`.
fn spawn(script: &str, out: &Path, timeouts: Timeouts) -> launch::StartedLaunch {
    launch::spawn_launch(
        Path::new("/usr/bin/env"),
        &[
            OsStr::new("-i"),
            OsStr::new("PATH=/usr/bin:/bin"),
            OsStr::new("/bin/sh"),
            OsStr::new("-c"),
            OsStr::new(script),
            OsStr::new("sh"),
            out.as_os_str(),
        ],
        timeouts,
        true,
    )
    .expect("spawn")
}

/// Whether `pid` has been reaped by this process (no longer its child).
fn reaped(pid: i32) -> bool {
    let mut status = 0;
    // SAFETY: WNOHANG waitpid on one exact PID with a valid status pointer.
    let waited = unsafe { libc::waitpid(pid, &mut status, libc::WNOHANG) };
    waited == -1 && std::io::Error::last_os_error().raw_os_error() == Some(libc::ECHILD)
}

/// Wait up to ten seconds for `predicate`.
fn wait_for(what: &str, mut predicate: impl FnMut() -> bool) {
    let deadline = Instant::now() + Duration::from_secs(10);
    while !predicate() {
        assert!(Instant::now() < deadline, "timed out waiting for {what}");
        std::thread::sleep(Duration::from_millis(10));
    }
}

/// The child blocks on the capability, receives exactly the released secret on
/// descriptor 6 (and the descriptor number as an argument), and only then proves
/// its identity and READY.
#[test]
fn child_is_gated_on_the_capability_until_it_is_released() {
    let temp = tempfile::tempdir().unwrap();
    let out = temp.path().join("secret");
    let mut started = spawn(
        r#"cap=$(cat <&6); exec 6<&-; printf '%s|%s %s' "$cap" "$8" "$9" > "$1"; echo $$ >&4; exec 4>&-; echo ready >&3"#,
        &out,
        quick(),
    );
    std::thread::sleep(Duration::from_millis(300));
    assert!(!out.exists(), "the gated child must not proceed unreleased");
    started.release_capability(b"s3cret-bytes").unwrap();
    let launched = started.complete(quick()).expect("launch completes");
    assert_eq!(
        launched.pid,
        launched.leader.as_ref().map_or(launched.pid, |l| l.pid)
    );
    wait_for("capability file", || out.exists());
    assert_eq!(
        std::fs::read_to_string(&out).unwrap(),
        "s3cret-bytes|--capability-fd 6"
    );
    let _ = launch::spawn_reaper(launched.pid);
}

/// An abandoned launch (evidence refused) never receives the secret, is cleaned
/// up through its verified group and carries the caller's message.
#[test]
fn abandon_cleans_up_the_gated_child() {
    let temp = tempfile::tempdir().unwrap();
    let started = spawn(
        r#"cat <&6 >/dev/null; sleep 60"#,
        &temp.path().join("x"),
        quick(),
    );
    let pid = started.pid;
    let error = started.abandon(quick(), "evidence not recorded".into());
    match error {
        LaunchError::Refused {
            pid: refused,
            message,
        } => {
            assert_eq!(refused, pid);
            assert_eq!(message, "evidence not recorded");
        }
        other => panic!("unexpected {other:?}"),
    }
    wait_for("child reaped", || reaped(pid));
}

/// Completing without ever releasing the secret times out and cleans up.
#[test]
fn unreleased_capability_times_out_with_verified_cleanup() {
    let temp = tempfile::tempdir().unwrap();
    let timeouts = Timeouts {
        ready: Duration::from_millis(800),
        ..quick()
    };
    let started = spawn(
        r#"cat <&6 >/dev/null; sleep 60"#,
        &temp.path().join("x"),
        timeouts,
    );
    let pid = started.pid;
    match started.complete(timeouts) {
        Err(LaunchError::Refused { message, .. }) => {
            assert!(message.contains("did not report ready"), "{message}");
        }
        other => panic!("unexpected {other:?}"),
    }
    wait_for("child reaped", || reaped(pid));
}

/// Release refuses empty, oversize and repeated secrets without touching the child.
#[test]
fn release_refuses_invalid_and_repeated_secrets() {
    let temp = tempfile::tempdir().unwrap();
    let mut started = spawn(
        r#"cat <&6 >/dev/null; sleep 60"#,
        &temp.path().join("x"),
        quick(),
    );
    let pid = started.pid;
    assert_eq!(
        started.release_capability(b"").unwrap_err().kind(),
        ErrorKind::InvalidInput
    );
    assert_eq!(
        started
            .release_capability(&[b'a'; launch::CAPABILITY_MAX_BYTES + 1])
            .unwrap_err()
            .kind(),
        ErrorKind::InvalidInput
    );
    started.release_capability(b"ok").unwrap();
    assert_eq!(
        started.release_capability(b"again").unwrap_err().kind(),
        ErrorKind::InvalidInput
    );
    let _ = started.abandon(quick(), "done".into());
    wait_for("child reaped", || reaped(pid));
}

/// `read_capability` returns the secret, closes the descriptor and maps the
/// closed-empty and never-closed pipes to distinct errors.
#[test]
fn read_capability_bounds_and_consumes_the_descriptor() {
    let (read, write) = launch::cloexec_pipe().unwrap();
    let mut file = std::fs::File::from(write);
    file.write_all(b"abc").unwrap();
    drop(file);
    let fd = read.into_raw_fd();
    assert_eq!(
        launch::read_capability(fd, Duration::from_secs(2)).unwrap(),
        b"abc"
    );

    let (read, write) = launch::cloexec_pipe().unwrap();
    drop(write);
    let error = launch::read_capability(read.into_raw_fd(), Duration::from_secs(2)).unwrap_err();
    assert_eq!(error.kind(), ErrorKind::UnexpectedEof);

    let (read, write) = launch::cloexec_pipe().unwrap();
    let error =
        launch::read_capability(read.into_raw_fd(), Duration::from_millis(150)).unwrap_err();
    assert_eq!(error.kind(), ErrorKind::TimedOut);
    assert!(write.as_raw_fd() > 2);
}

/// The synchronous wrapper still behaves as before and passes no capability flag.
#[test]
fn legacy_launch_detached_passes_no_capability_descriptor() {
    let temp = tempfile::tempdir().unwrap();
    let out = temp.path().join("args");
    let launched = launch::launch_detached(
        Path::new("/usr/bin/env"),
        &[
            OsStr::new("-i"),
            OsStr::new("PATH=/usr/bin:/bin"),
            OsStr::new("/bin/sh"),
            OsStr::new("-c"),
            OsStr::new(r#"printf '%s' "$*" > "$1"; echo $$ >&4; exec 4>&-; echo ready >&3"#),
            OsStr::new("sh"),
            out.as_os_str(),
        ],
        quick(),
        |_, _| Ok(()),
    )
    .unwrap();
    wait_for("args file", || out.exists());
    let text = std::fs::read_to_string(&out).unwrap();
    assert!(!text.contains("capability"), "{text}");
    let _ = launch::spawn_reaper(launched.pid);
}

/// L1: after the original launch deadline the secret is NOT granted; the child
/// never receives it, and the abandoned child is cleaned up and reaped.
#[test]
fn release_after_the_original_deadline_grants_nothing() {
    let temp = tempfile::tempdir().unwrap();
    let out = temp.path().join("received");
    let timeouts = Timeouts {
        ready: Duration::from_millis(250),
        ..quick()
    };
    let mut started = spawn(
        r#"cap=$(cat <&6); [ -n "$cap" ] && printf '%s' "$cap" > "$1"; sleep 60"#,
        &out,
        timeouts,
    );
    let pid = started.pid;
    std::thread::sleep(Duration::from_millis(500));
    let error = started.release_capability(b"late-secret").unwrap_err();
    assert_eq!(error.kind(), ErrorKind::TimedOut);
    // A second attempt still cannot renew the budget.
    assert_eq!(
        started
            .release_capability(b"late-secret")
            .unwrap_err()
            .kind(),
        ErrorKind::TimedOut
    );
    let _ = started.abandon(timeouts, "budget expired".into());
    wait_for("child reaped", || reaped(pid));
    assert!(!out.exists(), "the child must never have received a secret");
}
