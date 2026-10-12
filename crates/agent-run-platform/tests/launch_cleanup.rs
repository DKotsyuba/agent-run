//! Verified-group cleanup: the leader is re-verified immediately before every
//! signal, so a leader that exits during the TERM grace period is never
//! assumed alive when escalating to KILL.
use agent_run_platform::{
    launch::{self, Timeouts},
    process::{self, ProcessState},
};
use std::{
    ffi::OsStr,
    path::Path,
    time::{Duration, Instant},
};

/// Short deadlines so failures are observed in seconds.
fn quick() -> Timeouts {
    Timeouts {
        ready: Duration::from_secs(10),
        dispatch: Duration::from_millis(500),
        grace: Duration::from_millis(300),
        kill: Duration::from_millis(300),
    }
}

/// Launch the fixture shell with an empty child environment. `$1` receives the
/// background descendant's PID, written only AFTER that descendant installed its
/// TERM-ignoring disposition, and the leader reports READY only once it exists, so
/// no signal can race the fixture setup.
fn launch_fixture(script: &str, pid_file: &Path) -> launch::Launched {
    launch::launch_detached(
        Path::new("/usr/bin/env"),
        &[
            OsStr::new("-i"),
            OsStr::new("PATH=/usr/bin:/bin"),
            OsStr::new("/bin/sh"),
            OsStr::new("-c"),
            OsStr::new(script),
            OsStr::new("sh"),
            pid_file.as_os_str(),
        ],
        quick(),
        |_, _| Ok(()),
    )
    .expect("fixture launches")
}

/// Wait up to ten seconds for a non-empty file and parse its PID.
fn read_pid(path: &Path) -> i32 {
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        if let Ok(text) = std::fs::read_to_string(path)
            && let Ok(pid) = text.trim().parse()
        {
            return pid;
        }
        assert!(Instant::now() < deadline, "descendant pid never written");
        std::thread::sleep(Duration::from_millis(10));
    }
}

/// Kills the captured descendant only while its exact PID/birth is alive.
struct Teardown(process::Identity);

impl Drop for Teardown {
    /// Fenced cleanup: never signals a reused PID.
    fn drop(&mut self) {
        if process::observe(Some(self.0.pid), Some(&self.0.token), Some(self.0.birth))
            == ProcessState::Alive
        {
            // SAFETY: the exact PID/birth identity was just verified alive.
            unsafe { libc::kill(self.0.pid, libc::SIGKILL) };
        }
    }
}

/// The leader exits on TERM while a descendant ignores TERM: with no verified
/// living leader there is no KILL, cleanup is honestly unconfirmed and the
/// descendant is left alone.
#[test]
fn no_kill_after_the_leader_died_during_the_grace_wait() {
    let temp = tempfile::tempdir().unwrap();
    let pid_file = temp.path().join("descendant");
    let launched = launch_fixture(
        r#"trap 'exit 0' TERM; /bin/sh -c 'trap "" TERM; echo $$ > "$0"; exec sleep 30' "$1" & while [ ! -s "$1" ]; do sleep 0.02; done; echo $$ >&4; exec 4>&-; echo ready >&3; wait"#,
        &pid_file,
    );
    let leader = launched.leader.clone().expect("leader identity");
    let descendant = process::inspect(read_pid(&pid_file)).unwrap();
    let _teardown = Teardown(descendant.clone());
    let outcome = launch::terminate_group(&leader, quick().grace, quick().kill).unwrap();
    assert_eq!(
        outcome.signals,
        vec!["SIGTERM"],
        "KILL needs a living leader"
    );
    assert!(
        !outcome.group_gone,
        "an unconfirmed cleanup must not claim success"
    );
    assert_eq!(
        process::observe(
            Some(descendant.pid),
            Some(&descendant.token),
            Some(descendant.birth)
        ),
        ProcessState::Alive,
        "the descendant was not signalled"
    );
}

/// Control: a leader that ignores TERM is still alive at escalation, so the
/// verified group is killed and reported gone.
#[test]
fn a_living_term_resistant_leader_is_killed_and_the_group_confirmed_gone() {
    let temp = tempfile::tempdir().unwrap();
    let pid_file = temp.path().join("descendant");
    let launched = launch_fixture(
        r#"trap '' TERM; sleep 30 & echo $! > "$1"; echo $$ >&4; exec 4>&-; echo ready >&3; wait"#,
        &pid_file,
    );
    let leader = launched.leader.clone().expect("leader identity");
    let descendant = process::inspect(read_pid(&pid_file)).unwrap();
    let _teardown = Teardown(descendant.clone());
    let outcome = launch::terminate_group(&leader, quick().grace, quick().kill).unwrap();
    assert_eq!(outcome.signals, vec!["SIGTERM", "SIGKILL"]);
    assert!(outcome.group_gone);
    let _ = launch::spawn_reaper(launched.pid);
}
