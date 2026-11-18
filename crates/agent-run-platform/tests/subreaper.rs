//! Linux child-subreaper ownership: orphaned (double-forked, `setsid`)
//! descendants are reached by parentage at termination points, killed by
//! cleanup, and only exactly captured adopted zombies are reaped.
//!
//! The subreaper flag and the spawned-owner registry are process-wide, so each
//! case re-runs itself in a fresh process of this test binary.
#![cfg(target_os = "linux")]

use agent_run_platform::process::{self, Identity, OwnedProcess};
use std::{
    os::unix::process::CommandExt,
    process::{Child, Command},
    time::{Duration, Instant},
};

/// Names the case a re-executed child process must run.
const CASE: &str = "AGENT_RUN_SUBREAPER_CASE";

/// Run `body` in a fresh process of this binary, as exactly `name`.
fn isolated(name: &str, body: fn()) {
    if std::env::var(CASE).as_deref() == Ok(name) {
        process::become_subreaper().expect("become subreaper");
        body();
        return;
    }
    let status = Command::new(std::env::current_exe().unwrap())
        .args(["--exact", &format!("subreaper::{name}"), "--nocapture"])
        .env(CASE, name)
        .env_remove(process::REFRESH_ENV)
        .status()
        .unwrap();
    assert!(status.success(), "isolated case {name} failed: {status}");
}

/// Kills the fixture group and any listed daemon even when a case panics.
struct Fixture {
    leader: Child,
    daemons: Vec<i32>,
}
impl Drop for Fixture {
    fn drop(&mut self) {
        // SAFETY: fixture PIDs are > 1 and owned by this test process.
        unsafe {
            libc::kill(-(self.leader.id() as i32), libc::SIGKILL);
            for pid in &self.daemons {
                libc::kill(*pid, libc::SIGKILL);
            }
        }
        let _ = self.leader.wait();
    }
}

/// Spawn a group leader whose subshell double-forks `daemon` into a new session.
fn spawn_with_daemon(daemon: &str) -> Fixture {
    let script = format!("( setsid {daemon} </dev/null >/dev/null 2>&1 & ) ; exec sleep 600");
    Fixture {
        leader: Command::new("/bin/sh")
            .args(["-c", &script])
            .process_group(0)
            .spawn()
            .unwrap(),
        daemons: Vec::new(),
    }
}

/// Wait until this process has an adopted child, other than `leader` and
/// this process's own same-group helpers, matching `want`.
fn adopted_child(leader: i32, want: impl Fn(&Identity) -> bool) -> Identity {
    let me = std::process::id() as i32;
    // SAFETY: getpgrp has no preconditions.
    let own_group = unsafe { libc::getpgrp() };
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        if let Some(found) = process::processes().unwrap().into_iter().find(|p| {
            p.ppid == me && p.pid != leader && p.group != leader && p.group != own_group && want(p)
        }) {
            return found;
        }
        assert!(Instant::now() < deadline, "no adopted orphan appeared");
        std::thread::sleep(Duration::from_millis(20));
    }
}

/// Whether `owner` captured this exact identity.
fn captured(owner: &OwnedProcess, identity: &Identity) -> bool {
    owner
        .snapshot()
        .unwrap()
        .members
        .iter()
        .any(|p| p.pid == identity.pid && p.token == identity.token)
}

/// Whether this exact identity is gone or an unreaped zombie.
fn ended(identity: &Identity) -> bool {
    match process::inspect(identity.pid) {
        Ok(now) => now.token != identity.token || now.zombie,
        Err(_) => true,
    }
}

#[test]
fn a_daemonized_grandchild_is_captured_and_killed_at_finish() {
    isolated(
        "a_daemonized_grandchild_is_captured_and_killed_at_finish",
        || {
            assert_eq!(
                process::descendant_refresh_interval(),
                process::SUBREAPER_REFRESH
            );
            let mut fixture = spawn_with_daemon("sleep 600");
            let leader = fixture.leader.id() as i32;
            let mut owner = OwnedProcess::capture(leader);
            let daemon = adopted_child(leader, |p| !p.zombie);
            fixture.daemons.push(daemon.pid);
            assert!(!captured(&owner, &daemon), "no mid-run refresh happened");

            // Explicit finish while the agent is still alive.
            let cleanup = owner.cleanup_blocking(Duration::from_secs(1)).unwrap();
            assert!(
                captured(&owner, &daemon),
                "termination capture reached the orphan"
            );
            assert!(cleanup.confirmed, "{cleanup:?}");
            assert!(ended(&daemon), "the daemon survived cleanup");
            // The spawned leader's exit status still belongs to its spawner.
            let status = fixture.leader.wait().expect("leader was not stolen");
            assert!(!status.success());
        },
    );
}

#[test]
fn a_daemon_outliving_a_crashed_harness_is_killed() {
    isolated("a_daemon_outliving_a_crashed_harness_is_killed", || {
        let mut fixture = spawn_with_daemon("sleep 600");
        let leader = fixture.leader.id() as i32;
        let mut owner = OwnedProcess::capture(leader);
        let daemon = adopted_child(leader, |p| !p.zombie);
        fixture.daemons.push(daemon.pid);
        // The harness crashes alone; its spawner reaps it.
        // SAFETY: exact live leader PID owned by this test.
        unsafe { libc::kill(leader, libc::SIGKILL) };
        fixture.leader.wait().unwrap();
        assert!(!ended(&daemon));

        let cleanup = owner.cleanup_blocking(Duration::from_secs(1)).unwrap();
        assert!(captured(&owner, &daemon));
        assert!(cleanup.confirmed, "{cleanup:?}");
        assert_eq!(cleanup.descendants_gone, Some(true));
        assert!(ended(&daemon), "the daemon survived cleanup");
    });
}

#[test]
fn adopted_zombies_are_reaped_but_spawned_children_are_not() {
    isolated(
        "adopted_zombies_are_reaped_but_spawned_children_are_not",
        || {
            let mut fixture = spawn_with_daemon("true");
            let leader = fixture.leader.id() as i32;
            let mut owner = OwnedProcess::capture(leader);
            // A same-group helper whose own spawner waits for its status.
            let mut helper = Command::new("/bin/sh")
                .args(["-c", "exit 7"])
                .spawn()
                .unwrap();
            let orphan = adopted_child(leader, |p| p.zombie);
            let deadline = Instant::now() + Duration::from_secs(10);
            while !process::inspect(helper.id() as i32).unwrap().zombie {
                assert!(Instant::now() < deadline, "helper never exited");
                std::thread::sleep(Duration::from_millis(20));
            }

            owner.refresh();
            assert!(captured(&owner, &orphan));
            assert!(
                process::inspect(orphan.pid).is_err_and(|e| e.raw_os_error() == Some(libc::ENOENT)),
                "the adopted zombie was not reaped"
            );
            assert!(process::inspect(helper.id() as i32).unwrap().zombie);
            assert_eq!(helper.wait().unwrap().code(), Some(7), "helper status kept");
            assert!(
                fixture.leader.try_wait().unwrap().is_none(),
                "leader untouched"
            );
            assert!(!process::inspect(leader).unwrap().zombie);
        },
    );
}

#[test]
fn orphans_are_not_attributed_while_several_owners_live() {
    isolated(
        "orphans_are_not_attributed_while_several_owners_live",
        || {
            let mut fixture = spawn_with_daemon("sleep 600");
            let other = Fixture {
                leader: Command::new("/bin/sleep")
                    .arg("600")
                    .process_group(0)
                    .spawn()
                    .unwrap(),
                daemons: Vec::new(),
            };
            let leader = fixture.leader.id() as i32;
            let mut owner = OwnedProcess::capture(leader);
            let mut second = OwnedProcess::capture(other.leader.id() as i32);
            let daemon = adopted_child(leader, |p| !p.zombie && p.pid != other.leader.id() as i32);
            fixture.daemons.push(daemon.pid);
            owner.refresh();
            second.refresh();
            assert!(!captured(&owner, &daemon), "ambiguous orphan attributed");
            assert!(!captured(&second, &daemon), "ambiguous orphan attributed");
            drop(second);
            owner.refresh();
            assert!(captured(&owner, &daemon), "sole owner adopts it");
            drop(other);
        },
    );
}
