//! The engine's own OS process: one registered launch, a separate async
//! wrapping step, and an owned cleanup path when wrapping fails after the
//! engine program already started.
//!
//! The OS launch is [`agent_run_platform::process::spawn_registered`], which yields a standard
//! library child. Wrapping (async stdio and the exit wait) happens afterwards
//! with that child still in hand, so a wrapping failure never loses the only
//! handle to a running engine: it becomes a [`StartedLaunch`] whose whole
//! process tree goes through the ordinary [`OwnedProcess`] cleanup.

use agent_run_domain::{Error, Result};
use agent_run_platform::process::{Cleanup, OwnedProcess, OwnershipSnapshot, SpawnedChild};
use std::{process::ExitStatus, time::Duration};
use tokio::process::{ChildStderr, ChildStdin, ChildStdout};

/// Bound for collecting the engine's exit status after verified cleanup.
const REAP_BOUND: Duration = Duration::from_secs(2);

/// The engine process with an asynchronous exit wait.
///
/// It owns the standard library child, so the exit status is collected exactly
/// once, here. On Linux the wait is driven by a pidfd; elsewhere (or without
/// pidfd support) it polls `try_wait` every 50 ms.
pub struct EngineChild {
    /// The launched child; `None` once handed off by [`Self::detach`].
    inner: Option<std::process::Child>,
    /// Collected exit status.
    status: Option<ExitStatus>,
    /// Readable once the process exits.
    #[cfg(target_os = "linux")]
    exit: Option<tokio::io::unix::AsyncFd<std::os::fd::OwnedFd>>,
}

impl EngineChild {
    /// The engine PID.
    pub fn id(&self) -> Option<u32> {
        self.inner.as_ref().map(std::process::Child::id)
    }

    /// Collect the exit status if the engine has exited, without blocking.
    pub fn try_wait(&mut self) -> std::io::Result<Option<ExitStatus>> {
        if self.status.is_none()
            && let Some(child) = self.inner.as_mut()
        {
            self.status = child.try_wait()?;
        }
        Ok(self.status)
    }

    /// Wait for the engine to exit and collect its status. Cancel-safe.
    pub async fn wait(&mut self) -> std::io::Result<ExitStatus> {
        loop {
            if let Some(status) = self.try_wait()? {
                return Ok(status);
            }
            if self.inner.is_none() {
                return Err(std::io::Error::other("engine child was handed off"));
            }
            #[cfg(target_os = "linux")]
            if let Some(exit) = &self.exit {
                exit.readable().await?.clear_ready();
                continue;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }

    /// Hand off the unreaped child (for example to a waiter thread on drop).
    pub(crate) fn detach(&mut self) -> Option<std::process::Child> {
        if self.status.is_some() {
            return None;
        }
        self.inner.take()
    }
}

/// Async pipes of a wrapped engine.
pub(crate) struct Wrapped {
    pub(crate) child: EngineChild,
    pub(crate) stdin: Option<ChildStdin>,
    pub(crate) stdout: ChildStdout,
    pub(crate) stderr: ChildStderr,
}

/// Wrap a launched child for the async runtime. On failure the untouched
/// child is returned with the error, so the caller still owns the engine.
pub(crate) fn wrap(
    mut child: std::process::Child,
) -> std::result::Result<Wrapped, (std::io::Error, std::process::Child)> {
    #[cfg(any(test, feature = "test-fixtures"))]
    if let Some(error) = fixture::injected_wrap_failure() {
        return Err((error, child));
    }
    let stdin = child.stdin.take();
    let stdout = child.stdout.take();
    let stderr = child.stderr.take();
    let converted = (|| {
        let stdin = stdin.map(ChildStdin::from_std).transpose()?;
        let stdout = stdout
            .map(ChildStdout::from_std)
            .transpose()?
            .ok_or_else(|| std::io::Error::other("child stdout unavailable"))?;
        let stderr = stderr
            .map(ChildStderr::from_std)
            .transpose()?
            .ok_or_else(|| std::io::Error::other("child stderr unavailable"))?;
        Ok::<_, std::io::Error>((stdin, stdout, stderr))
    })();
    let (stdin, stdout, stderr) = match converted {
        Ok(pipes) => pipes,
        Err(error) => return Err((error, child)),
    };
    #[cfg(target_os = "linux")]
    let exit = pidfd(child.id());
    Ok(Wrapped {
        child: EngineChild {
            inner: Some(child),
            status: None,
            #[cfg(target_os = "linux")]
            exit,
        },
        stdin,
        stdout,
        stderr,
    })
}

/// An async-readable pidfd for `pid`, or `None` (the wait then polls).
#[cfg(target_os = "linux")]
fn pidfd(pid: u32) -> Option<tokio::io::unix::AsyncFd<std::os::fd::OwnedFd>> {
    use std::os::fd::FromRawFd;
    // SAFETY: pidfd_open takes a PID and flags and returns a new descriptor.
    let raw = unsafe { libc::syscall(libc::SYS_pidfd_open, pid as libc::pid_t, 0) };
    if raw < 0 {
        return None;
    }
    // SAFETY: pidfd_open returned a new descriptor owned by nobody else.
    let fd = unsafe { std::os::fd::OwnedFd::from_raw_fd(raw as i32) };
    // Registration panics outside a runtime; the wait then polls instead.
    tokio::runtime::Handle::try_current().ok()?;
    // SAFETY: the AsyncFd owns this open OwnedFd for its whole lifetime, so the
    // registered descriptor stays valid and `as_raw_fd` never changes.
    unsafe { tokio::io::unix::AsyncFd::register_with_interest(fd, tokio::io::Interest::READABLE) }
        .ok()
}

/// How an engine's stdout is consumed and which native owner it may use.
pub enum LaunchKind {
    /// Line-framed JSON-RPC engine.
    Rpc,
    /// HTTP server whose stdin is a lifetime lease.
    Lease,
    /// Managed-selected harness with an optional owner-v2 endpoint.
    Managed(Option<Box<crate::native_owner_v2::Config>>),
}

/// Why an engine launch failed.
pub enum SpawnError {
    /// No engine program is running: preparation failed or the OS launch was
    /// refused (a failed `exec` was already reaped). Nothing is owned.
    NotStarted(Error),
    /// The engine program started, but wrapping it failed. Its process tree is
    /// still owned and must be cleaned through [`StartedLaunch`].
    Started(Box<StartedLaunch>),
}

/// A running engine whose launch failed after the program started.
pub struct StartedLaunch {
    /// The wrapping failure.
    pub error: Error,
    /// Ownership of the engine's process tree, captured from the exact
    /// registered root; every signal still requires fresh identity evidence.
    pub owner: OwnedProcess,
    /// The only handle that may collect the engine's exit status.
    child: std::process::Child,
    /// Registration of the engine as this process's own spawn.
    claim: SpawnedChild,
}

/// Result of cleaning a [`StartedLaunch`].
pub struct LaunchCleanup {
    /// The original wrapping failure.
    pub error: Error,
    /// Standard cleanup evidence of the engine's process tree.
    pub proof: Result<Cleanup>,
    /// Whether the tree is proven gone and the engine's status was collected.
    /// When false, ownership is retained: the registration stays and the
    /// caller must not certify the launch as never started.
    pub confirmed: bool,
    /// Final captured identities, for durable recovery evidence.
    pub snapshot: Option<OwnershipSnapshot>,
    /// The engine child when its status could not be collected.
    pub retained: Option<std::process::Child>,
}

impl StartedLaunch {
    /// Capture ownership of a started engine whose wrapping failed.
    pub(crate) fn new(error: Error, child: std::process::Child, claim: SpawnedChild) -> Self {
        let pid = child.id() as i32;
        #[cfg(any(test, feature = "test-fixtures"))]
        let pid = if fixture::OWNER_FAULT.swap(false, std::sync::atomic::Ordering::SeqCst) {
            0
        } else {
            pid
        };
        let mut owner = OwnedProcess::capture(pid);
        owner.refresh();
        Self {
            error,
            owner,
            child,
            claim,
        }
    }

    /// Run the standard bounded cleanup, then collect the engine's status.
    pub async fn cleanup(mut self, grace: Duration) -> LaunchCleanup {
        let proof = self.owner.cleanup(grace).await;
        let deadline = tokio::time::Instant::now() + REAP_BOUND;
        let reaped = loop {
            match self.child.try_wait() {
                Ok(Some(_)) => break true,
                Ok(None) if tokio::time::Instant::now() < deadline => {
                    tokio::time::sleep(Duration::from_millis(20)).await;
                }
                _ => break false,
            }
        };
        self.finish(proof, reaped)
    }

    /// Blocking twin of [`Self::cleanup`] for synchronous callers.
    pub fn cleanup_blocking(mut self, grace: Duration) -> LaunchCleanup {
        let proof = self.owner.cleanup_blocking(grace);
        let deadline = std::time::Instant::now() + REAP_BOUND;
        let reaped = loop {
            match self.child.try_wait() {
                Ok(Some(_)) => break true,
                Ok(None) if std::time::Instant::now() < deadline => {
                    std::thread::sleep(Duration::from_millis(20));
                }
                _ => break false,
            }
        };
        self.finish(proof, reaped)
    }

    /// Release the registration only on positive evidence.
    fn finish(self, proof: Result<Cleanup>, reaped: bool) -> LaunchCleanup {
        let confirmed = reaped && proof.as_ref().is_ok_and(|proof| proof.confirmed);
        let snapshot = self.owner.snapshot();
        let retained = if reaped {
            self.claim.reaped();
            None
        } else {
            // The claim is dropped unreleased, which keeps the registration:
            // nothing else may adopt or reap this engine while it may run.
            Some(self.child)
        };
        LaunchCleanup {
            error: self.error,
            proof,
            confirmed,
            snapshot,
            retained,
        }
    }
}

/// The error a synchronous caller reports after cleaning a started launch.
pub(crate) fn started_failure(cleanup: &LaunchCleanup) -> Error {
    Error::Runtime(format!(
        "engine launch failed after the program started ({}); cleanup {}",
        cleanup.error,
        if cleanup.confirmed {
            "confirmed"
        } else {
            "unconfirmed"
        }
    ))
}

/// Deterministic launch-failure seams for tests (never in production builds).
#[cfg(any(test, feature = "test-fixtures"))]
pub mod fixture {
    use std::{
        path::PathBuf,
        sync::{
            Mutex,
            atomic::{AtomicBool, Ordering},
        },
        time::{Duration, Instant},
    };

    /// When set, the next wrap fails once this marker exists (bounded wait),
    /// so the engine can fork before the failure.
    static WRAP_FAULT: Mutex<Option<PathBuf>> = Mutex::new(None);
    /// When set, the next started engine's root cannot be identified.
    pub(super) static OWNER_FAULT: AtomicBool = AtomicBool::new(false);

    /// The injected wrapping failure, after the marker appeared.
    pub(super) fn injected_wrap_failure() -> Option<std::io::Error> {
        let marker = WRAP_FAULT.lock().ok()?.take()?;
        let deadline = Instant::now() + Duration::from_secs(10);
        while !marker.exists() && Instant::now() < deadline {
            std::thread::sleep(Duration::from_millis(10));
        }
        Some(std::io::Error::other("injected wrap failure"))
    }

    /// Make the next wrap fail once `marker` exists.
    pub fn fail_next_wrap(marker: PathBuf) {
        if let Ok(mut fault) = WRAP_FAULT.lock() {
            *fault = Some(marker);
        }
    }

    /// Make the next started-launch owner unidentifiable.
    pub fn hide_next_owner(hidden: bool) {
        OWNER_FAULT.store(hidden, Ordering::SeqCst);
    }
}

#[cfg(test)]
mod tests {
    use super::fixture::{fail_next_wrap, hide_next_owner};
    use agent_run_domain::Error;
    use std::{
        path::PathBuf,
        time::{Duration, Instant},
    };

    use super::{SpawnError, StartedLaunch};
    use crate::{LaunchPlan, engine_launch::LaunchKind, io::Process};
    use agent_run_platform::process::{self, Identity, OwnedProcess};
    use std::collections::BTreeMap;

    /// Names the case a re-executed unit-test process must run.
    const CASE: &str = "AGENT_RUN_ENGINE_LAUNCH_CASE";

    /// Run `body` as a subreaper in a fresh process of this test binary.
    fn isolated(name: &str, body: fn()) {
        if std::env::var(CASE).as_deref() == Ok(name) {
            process::become_subreaper().unwrap();
            body();
            return;
        }
        let output = std::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                &format!("engine_launch::tests::{name}"),
                "--nocapture",
            ])
            .env(CASE, name)
            .output()
            .unwrap();
        let stdout = String::from_utf8_lossy(&output.stdout);
        assert!(
            output.status.success() && stdout.contains("test result: ok. 1 passed"),
            "isolated case {name} failed: {}\n{stdout}{}",
            output.status,
            String::from_utf8_lossy(&output.stderr)
        );
        print!("{stdout}");
    }

    /// An engine that forks a child and a daemonized grandchild, then touches
    /// `marker` (which releases the injected wrap failure) and keeps running.
    fn forking_engine(marker: &std::path::Path) -> LaunchPlan {
        LaunchPlan {
            binary: "/bin/sh".into(),
            args: vec![
                "-c".into(),
                "sleep 600 & ( setsid sleep 600 </dev/null >/dev/null 2>&1 & ) ; \
                 touch \"$MARK\"; exec sleep 600"
                    .into(),
            ],
            cwd: std::env::current_dir().unwrap(),
            environment: BTreeMap::from([(
                "MARK".to_owned(),
                marker.to_string_lossy().into_owned(),
            )]),
            initial_input: None,
        }
    }

    /// The started engine's leader, child and adopted daemon.
    fn tree(started: &StartedLaunch) -> Vec<Identity> {
        let leader = started.owner.leader.clone().expect("identified root");
        let me = std::process::id() as i32;
        let mut tree = vec![leader.clone()];
        let deadline = Instant::now() + Duration::from_secs(10);
        while tree.len() < 3 {
            assert!(Instant::now() < deadline, "engine tree incomplete");
            tree.truncate(1);
            tree.extend(process::processes().unwrap().into_iter().filter(|p| {
                !p.zombie && (p.ppid == leader.pid || (p.ppid == me && p.pid != leader.pid))
            }));
            std::thread::sleep(Duration::from_millis(20));
        }
        tree
    }

    /// Whether this exact identity is gone (not even a zombie).
    fn gone(identity: &Identity) -> bool {
        process::inspect(identity.pid).map_or(true, |now| now.token != identity.token)
    }

    /// A marker path unique to this process.
    fn marker(name: &str) -> PathBuf {
        std::env::temp_dir().join(format!("{name}-{}", std::process::id()))
    }

    /// A wrap failure after the engine forked still ends the whole tree
    /// through the ordinary cleanup, collects the engine's status, releases
    /// its registration and restores the clean boundary.
    #[test]
    fn a_wrap_failure_after_start_cleans_the_whole_tree() {
        isolated("a_wrap_failure_after_start_cleans_the_whole_tree", || {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap();
            runtime.block_on(async {
                let marker = marker("wrap-tree");
                fail_next_wrap(marker.clone());
                let Err(SpawnError::Started(started)) =
                    Process::launch(&forking_engine(&marker), LaunchKind::Rpc)
                else {
                    panic!("a started launch must be reported as started");
                };
                let _ = std::fs::remove_file(&marker);
                let tree = tree(&started);
                let outcome = started.cleanup(Duration::from_millis(500)).await;
                assert!(outcome.confirmed, "{:?}", outcome.proof.as_ref().err());
                assert!(outcome.retained.is_none());
                for member in &tree {
                    assert!(gone(member), "engine tree member {} survived", member.pid);
                }
                // The registration was released: the next spawn is exclusive.
                let mut next = std::process::Command::new("/bin/sleep");
                next.arg("30");
                let (mut child, claim) = process::spawn_registered(next).unwrap();
                let owner = OwnedProcess::capture(child.id() as i32);
                assert_eq!(owner.refresh_interval(), process::DURABLE_REFRESH);
                child.kill().unwrap();
                child.wait().unwrap();
                claim.reaped();
            });
        });
    }

    /// When the started root cannot be identified, nothing can be proven or
    /// signalled: the outcome is unconfirmed and ownership (registration and
    /// the child handle) is retained.
    #[test]
    fn an_unidentifiable_started_engine_keeps_ownership() {
        isolated("an_unidentifiable_started_engine_keeps_ownership", || {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap();
            runtime.block_on(async {
                let marker = marker("wrap-hidden");
                fail_next_wrap(marker.clone());
                hide_next_owner(true);
                let Err(SpawnError::Started(started)) =
                    Process::launch(&forking_engine(&marker), LaunchKind::Rpc)
                else {
                    panic!("a started launch must be reported as started");
                };
                hide_next_owner(false);
                let _ = std::fs::remove_file(&marker);
                assert!(started.owner.leader.is_none());
                let outcome = started.cleanup(Duration::from_millis(200)).await;
                assert!(!outcome.confirmed);
                let mut retained = outcome.retained.expect("engine handle retained");
                let leader = process::inspect(retained.id() as i32).unwrap();
                assert!(!leader.zombie, "the unverified engine was not signalled");
                // Still registered: a new spawn is not exclusive.
                let mut next = std::process::Command::new("/bin/sleep");
                next.arg("30");
                let (mut child, claim) = process::spawn_registered(next).unwrap();
                let owner = OwnedProcess::capture(child.id() as i32);
                assert_eq!(owner.refresh_interval(), process::POLLING_REFRESH);
                child.kill().unwrap();
                child.wait().unwrap();
                claim.reaped();
                // Test teardown of the retained engine tree.
                let me = std::process::id() as i32;
                for p in process::processes().unwrap() {
                    if p.ppid == me || p.group == leader.pid {
                        // SAFETY: fixture processes of this isolated test process only.
                        unsafe { libc::kill(p.pid, libc::SIGKILL) };
                    }
                }
                retained.wait().unwrap();
            });
        });
    }

    /// Synchronous callers clean a started launch and get a distinct,
    /// non-I/O error, so it is never mistaken for a refused launch.
    #[test]
    fn a_synchronous_spawn_reports_a_started_failure_distinctly() {
        isolated(
            "a_synchronous_spawn_reports_a_started_failure_distinctly",
            || {
                let runtime = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .unwrap();
                runtime.block_on(async {
                    let marker = marker("wrap-sync");
                    fail_next_wrap(marker.clone());
                    let error = Process::spawn(&forking_engine(&marker))
                        .err()
                        .expect("wrap failure reported");
                    let _ = std::fs::remove_file(&marker);
                    assert!(!matches!(error, Error::Io(_)), "{error}");
                    assert!(error.to_string().contains("cleanup confirmed"), "{error}");
                });
            },
        );
    }
}
