//! Linux child-subreaper ownership: orphaned (double-forked, `setsid`, or
//! regrouped) descendants are reached by parentage at termination points and
//! killed by cleanup; registered spawns are never adopted, signalled through
//! adoption or reaped; orphans are attributed only to an exclusive spawn.
//!
//! The subreaper flag and spawn registry are process-wide, so each case
//! re-runs itself in a fresh process of this test binary.
#![cfg(target_os = "linux")]

use agent_run_platform::process::{self, Identity, OwnedProcess, SpawnedChild};
use std::{
    os::unix::process::CommandExt,
    process::{Child, Command},
    time::{Duration, Instant},
};

/// Names the case a re-executed child process must run.
const CASE: &str = "AGENT_RUN_SUBREAPER_CASE";
/// Process group a re-executed joiner fixture moves itself into.
const JOIN: &str = "AGENT_RUN_SUBREAPER_JOIN_PGID";

/// Run `body` in a fresh subreaper process of this binary, as exactly `name`.
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

/// A registered spawn: kills its group and any listed daemon, then reports it reaped.
struct Spawned {
    child: Child,
    claim: Option<SpawnedChild>,
    daemons: Vec<i32>,
}
impl Spawned {
    /// Collect the exit status as the spawner, releasing the registration.
    fn wait(&mut self) -> std::process::ExitStatus {
        let status = self.child.wait().unwrap();
        if let Some(claim) = self.claim.take() {
            claim.reaped();
        }
        status
    }
}
impl Drop for Spawned {
    fn drop(&mut self) {
        // SAFETY: fixture PIDs are > 1 and owned by this test process.
        unsafe {
            libc::kill(-(self.child.id() as i32), libc::SIGKILL);
            for pid in &self.daemons {
                libc::kill(*pid, libc::SIGKILL);
            }
        }
        if self.child.wait().is_ok()
            && let Some(claim) = self.claim.take()
        {
            claim.reaped();
        }
    }
}

/// Spawn and register `command`, like the engine transport does.
fn spawn(command: &mut Command) -> Spawned {
    let (child, claim) = process::spawn_child(|| command.spawn(), |c| Some(c.id())).unwrap();
    Spawned {
        child,
        claim: Some(claim),
        daemons: Vec::new(),
    }
}

/// Spawn a registered group leader whose subshell double-forks `daemon`.
///
/// The short delay keeps the fork out of the spawn-time exclusivity
/// snapshots; an orphan already present there (correctly) refuses exclusivity.
fn spawn_with_daemon(daemon: &str) -> Spawned {
    let script = format!("sleep 0.3; ( {daemon} </dev/null >/dev/null 2>&1 & ) ; exec sleep 600");
    spawn(
        Command::new("/bin/sh")
            .args(["-c", &script])
            .process_group(0),
    )
}

/// Wait until this process has a direct child other than `leader` matching `want`.
fn adopted_child(leader: i32, want: impl Fn(&Identity) -> bool) -> Identity {
    let me = std::process::id() as i32;
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        if let Some(found) = process::processes()
            .unwrap()
            .into_iter()
            .find(|p| p.ppid == me && p.pid != leader && want(p))
        {
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

/// This process's own process group.
fn own_group() -> i32 {
    // SAFETY: getpgrp has no preconditions.
    unsafe { libc::getpgrp() }
}

/// Re-executed by the PGID case: join the requested group, then live on.
#[test]
fn pgid_joiner_fixture() {
    let Some(group) = std::env::var(JOIN).ok().and_then(|g| g.parse::<i32>().ok()) else {
        return;
    };
    // SAFETY: setpgid on this process into an existing group of its session.
    assert_eq!(unsafe { libc::setpgid(0, group) }, 0);
    std::thread::sleep(Duration::from_secs(600));
}

#[test]
fn a_daemonized_grandchild_is_captured_and_killed_at_finish() {
    isolated(
        "a_daemonized_grandchild_is_captured_and_killed_at_finish",
        || {
            let mut fixture = spawn_with_daemon("setsid sleep 600");
            let leader = fixture.child.id() as i32;
            let mut owner = OwnedProcess::capture(leader);
            assert_eq!(owner.refresh_interval(), process::DURABLE_REFRESH);
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
            // The registered leader's exit status still belongs to its spawner.
            assert!(!fixture.wait().success());
        },
    );
}

#[test]
fn a_daemon_outliving_a_crashed_harness_is_killed() {
    isolated("a_daemon_outliving_a_crashed_harness_is_killed", || {
        let mut fixture = spawn_with_daemon("setsid sleep 600");
        let leader = fixture.child.id() as i32;
        let mut owner = OwnedProcess::capture(leader);
        let daemon = adopted_child(leader, |p| !p.zombie);
        fixture.daemons.push(daemon.pid);
        // The harness crashes alone; its spawner reaps it.
        // SAFETY: exact live leader PID owned by this test.
        unsafe { libc::kill(leader, libc::SIGKILL) };
        fixture.wait();
        assert!(!ended(&daemon));

        let cleanup = owner.cleanup_blocking(Duration::from_secs(1)).unwrap();
        assert!(captured(&owner, &daemon));
        assert!(cleanup.confirmed, "{cleanup:?}");
        assert_eq!(cleanup.descendants_gone, Some(true));
        assert!(ended(&daemon), "the daemon survived cleanup");
    });
}

/// A descendant may join the supervisor's own process group (same session);
/// it is still an orphan of the owned tree, never a supervisor helper.
#[test]
fn a_descendant_joining_the_supervisor_group_is_still_killed() {
    isolated(
        "a_descendant_joining_the_supervisor_group_is_still_killed",
        || {
            let exe = std::env::current_exe().unwrap();
            let script = "sleep 0.3; ( \"$JOINER\" --exact subreaper::pgid_joiner_fixture --nocapture \
                      </dev/null >/dev/null 2>&1 & ) ; exec sleep 600";
            let mut fixture = spawn(
                Command::new("/bin/sh")
                    .args(["-c", script])
                    .env("JOINER", &exe)
                    .env(JOIN, own_group().to_string())
                    .env_remove(CASE)
                    .process_group(0),
            );
            let leader = fixture.child.id() as i32;
            let mut owner = OwnedProcess::capture(leader);
            let joiner = adopted_child(leader, |p| !p.zombie && p.group == own_group());
            fixture.daemons.push(joiner.pid);

            let cleanup = owner.cleanup_blocking(Duration::from_secs(1)).unwrap();
            assert!(
                captured(&owner, &joiner),
                "a same-group orphan was not captured"
            );
            assert!(cleanup.confirmed, "{cleanup:?}");
            assert!(ended(&joiner), "the regrouped orphan survived cleanup");
        },
    );
}

#[test]
fn adopted_zombies_are_reaped_but_spawned_children_are_not() {
    isolated(
        "adopted_zombies_are_reaped_but_spawned_children_are_not",
        || {
            // A registered helper in its own group, already exited: its status
            // belongs to its spawner even though it is a zombie direct child.
            let mut helper = spawn(
                Command::new("/bin/sh")
                    .args(["-c", "exit 7"])
                    .process_group(0),
            );
            let deadline = Instant::now() + Duration::from_secs(10);
            while !process::inspect(helper.child.id() as i32).unwrap().zombie {
                assert!(Instant::now() < deadline, "helper never exited");
                std::thread::sleep(Duration::from_millis(20));
            }
            let mut fixture = spawn_with_daemon("setsid true");
            let leader = fixture.child.id() as i32;
            let mut owner = OwnedProcess::capture(leader);
            let orphan = adopted_child(leader, |p| p.zombie && p.pid != helper.child.id() as i32);

            owner.refresh();
            assert!(captured(&owner, &orphan));
            assert!(
                process::inspect(orphan.pid).is_err_and(|e| e.raw_os_error() == Some(libc::ENOENT)),
                "the adopted zombie was not reaped"
            );
            let helper_now = process::inspect(helper.child.id() as i32).unwrap();
            assert!(helper_now.zombie, "the registered helper was reaped");
            assert!(!captured(&owner, &helper_now));
            assert_eq!(helper.wait().code(), Some(7), "helper status kept");
            assert!(
                fixture.child.try_wait().unwrap().is_none(),
                "leader untouched"
            );
            assert!(!process::inspect(leader).unwrap().zombie);
        },
    );
}

/// Several possible trees: nothing is attributed, the owner keeps 200 ms
/// polling, and a registered direct child whose owner was dropped while its
/// `Child` lives is never captured, signalled or reaped.
#[test]
fn orphans_are_never_attributed_across_trees() {
    isolated("orphans_are_never_attributed_across_trees", || {
        let mut fixture = spawn_with_daemon("setsid sleep 600");
        let leader = fixture.child.id() as i32;
        let mut owner = OwnedProcess::capture(leader);
        assert_eq!(owner.refresh_interval(), process::DURABLE_REFRESH);
        let mut other = spawn(Command::new("/bin/sleep").arg("600").process_group(0));
        let other_pid = other.child.id() as i32;
        let other_identity = process::inspect(other_pid).unwrap();
        assert_eq!(
            owner.refresh_interval(),
            process::POLLING_REFRESH,
            "exclusivity ended"
        );
        let second = OwnedProcess::capture(other_pid);
        assert_eq!(second.refresh_interval(), process::POLLING_REFRESH);
        let daemon = adopted_child(leader, |p| !p.zombie && p.pid != other_pid);
        fixture.daemons.push(daemon.pid);
        drop(second);

        owner.refresh();
        assert!(
            !captured(&owner, &daemon),
            "an ambiguous orphan was attributed"
        );
        assert!(
            !captured(&owner, &other_identity),
            "a registered spawn was adopted"
        );
        let cleanup = owner.cleanup_blocking(Duration::from_millis(200)).unwrap();
        assert!(!captured(&owner, &other_identity));
        assert!(
            !ended(&other_identity),
            "the registered spawn was signalled"
        );
        assert!(other.child.try_wait().unwrap().is_none(), "status stolen");
        // The unattributable daemon is outside the verified scope, not proof.
        assert!(!ended(&daemon));
        assert!(cleanup.group_gone);
        // SAFETY: exact live registered fixture PID.
        unsafe { libc::kill(other_pid, libc::SIGKILL) };
        assert!(
            !other.wait().success(),
            "the spawner still collects the status"
        );
    });
}
