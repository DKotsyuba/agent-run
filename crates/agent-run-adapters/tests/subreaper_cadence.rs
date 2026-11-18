//! With a Linux child subreaper the engine `Process` stops polling `/proc`
//! every 200 ms, yet a daemonized descendant is still captured and killed when
//! the primary exits. Subreaper state is process-wide, so each case re-runs
//! itself in a fresh process of this test binary.
#![cfg(target_os = "linux")]

use agent_run_adapters::{
    LaunchPlan,
    io::{Event, Process},
};
use agent_run_platform::process::{self, Identity};
use std::{
    collections::BTreeMap,
    path::PathBuf,
    process::Command,
    time::{Duration, Instant},
};

/// Names the case a re-executed child process must run, and whether it becomes a subreaper.
const CASE: &str = "AGENT_RUN_SUBREAPER_CADENCE_CASE";

/// Run `body` in a fresh process of this binary, as exactly `name`.
fn isolated(name: &str, subreaper: bool, body: fn()) {
    if std::env::var(CASE).as_deref() == Ok(name) {
        if subreaper {
            process::become_subreaper().expect("become subreaper");
        }
        body();
        return;
    }
    let status = Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            &format!("subreaper_cadence::{name}"),
            "--nocapture",
        ])
        .env(CASE, name)
        .env_remove(process::REFRESH_ENV)
        .status()
        .unwrap();
    assert!(status.success(), "isolated case {name} failed: {status}");
}

/// Cleans verified members even if an assertion unwinds.
struct Fixture(Process);
impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = self.0.owner.cleanup_blocking(Duration::from_millis(100));
    }
}

/// Spawn `/bin/sh -c script` as an engine process.
fn spawn(script: &str) -> Fixture {
    Fixture(
        Process::spawn(&LaunchPlan {
            binary: PathBuf::from("/bin/sh"),
            args: vec!["-c".into(), script.into()],
            cwd: std::env::current_dir().unwrap(),
            environment: BTreeMap::new(),
            initial_input: None,
        })
        .unwrap(),
    )
}

/// Reads this process's cumulative read-syscall count.
fn read_syscalls() -> u64 {
    std::fs::read_to_string("/proc/self/io")
        .unwrap()
        .lines()
        .find_map(|line| line.strip_prefix("syscr: "))
        .unwrap()
        .parse()
        .unwrap()
}

/// Observe a silent engine for three seconds through both wait paths and
/// return the read syscalls spent and the host process count.
fn quiet_window_reads() -> (u64, u64) {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    runtime.block_on(async {
        let mut fixture = spawn("exec sleep 30");
        let host = process::processes().unwrap().len() as u64;
        let before = read_syscalls();
        fixture
            .0
            .observing(tokio::time::sleep(Duration::from_millis(1500)))
            .await;
        assert!(
            tokio::time::timeout(Duration::from_millis(1500), fixture.0.next())
                .await
                .is_err(),
            "the silent engine produced an event"
        );
        (read_syscalls() - before, host)
    })
}

#[test]
fn a_subreaper_engine_does_not_poll_proc_while_running() {
    isolated(
        "a_subreaper_engine_does_not_poll_proc_while_running",
        true,
        || {
            let (reads, host) = quiet_window_reads();
            println!("subreaper: {reads} read syscalls in 3 s over {host} processes");
            // One refresh alone reads every /proc/<pid>/stat at least once.
            assert!(reads < host, "a /proc scan ran mid-run: {reads} reads");
        },
    );
}

#[test]
fn without_a_subreaper_the_engine_keeps_polling() {
    isolated(
        "without_a_subreaper_the_engine_keeps_polling",
        false,
        || {
            let (reads, host) = quiet_window_reads();
            println!("polling: {reads} read syscalls in 3 s over {host} processes");
            // At 200 ms, three seconds hold well over five full scans.
            assert!(reads >= 5 * host, "polling refresh stopped: {reads} reads");
        },
    );
}

#[test]
fn primary_exit_captures_and_kills_a_daemonized_descendant() {
    isolated(
        "primary_exit_captures_and_kills_a_daemonized_descendant",
        true,
        || {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap();
            runtime.block_on(async {
                let mut fixture = spawn(
                    "( setsid sleep 600 </dev/null >/dev/null 2>&1 & ) ; \
                     printf '{\"ready\":1}\\n'; read request; exit 3",
                );
                let leader = fixture.0.owner.pid;
                let Event::Json(_) = tokio::time::timeout(Duration::from_secs(5), fixture.0.next())
                    .await
                    .unwrap()
                else {
                    panic!("fixture readiness missing");
                };
                let me = std::process::id() as i32;
                // SAFETY: getpgrp has no preconditions.
                let own_group = unsafe { libc::getpgrp() };
                let deadline = Instant::now() + Duration::from_secs(10);
                let daemon: Identity = loop {
                    if let Some(found) = process::processes().unwrap().into_iter().find(|p| {
                        p.ppid == me && p.pid != leader && p.group != own_group && !p.zombie
                    }) {
                        break found;
                    }
                    assert!(Instant::now() < deadline, "daemon never adopted");
                    tokio::time::sleep(Duration::from_millis(20)).await;
                };
                /// Kills the fixture daemon if an assertion unwinds first.
                struct Kill(i32);
                impl Drop for Kill {
                    fn drop(&mut self) {
                        // SAFETY: exact positive fixture PID; it is this process's child until reaped.
                        unsafe { libc::kill(self.0, libc::SIGKILL) };
                    }
                }
                let _guard = Kill(daemon.pid);

                fixture.0.text("crash\n").await.unwrap();
                let deadline = Instant::now() + Duration::from_secs(10);
                loop {
                    let ended = match process::inspect(daemon.pid) {
                        Ok(now) => now.token != daemon.token || now.zombie,
                        Err(_) => true,
                    };
                    if ended {
                        break;
                    }
                    assert!(Instant::now() < deadline, "daemon survived primary exit");
                    let _ = tokio::time::timeout(Duration::from_millis(50), fixture.0.next()).await;
                }
                let members = fixture.0.owner.snapshot().unwrap().members;
                assert!(
                    members
                        .iter()
                        .any(|p| p.pid == daemon.pid && p.token == daemon.token),
                    "the daemon was not captured"
                );
                let cleanup = fixture
                    .0
                    .owner
                    .cleanup(Duration::from_secs(1))
                    .await
                    .unwrap();
                assert!(cleanup.confirmed, "{cleanup:?}");
                assert_eq!(fixture.0.reap().await, Some(3));
            });
        },
    );
}
