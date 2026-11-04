//! Unprivileged worker that launches only the existing supervisor.

use super::wire::{self, Release, Reply, Start};
use agent_run_platform::{
    fs,
    launch::{self, StartedLaunch, Timeouts},
    process::{self, Identity},
};
#[cfg(test)]
use std::path::Path;
use std::{
    ffi::OsStr,
    io,
    os::unix::net::UnixStream,
    path::PathBuf,
    thread,
    time::{Duration, Instant},
};

/// A launcher already running as the native account selected by the host.
pub struct Worker {
    program: PathBuf,
    home: PathBuf,
}

impl Worker {
    /// Binds the fixed supervisor executable and shared state home for this worker.
    pub fn new(program: PathBuf, home: PathBuf) -> io::Result<Self> {
        if !program.is_absolute() || !home.is_absolute() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "worker paths must be absolute",
            ));
        }
        Ok(Self { program, home })
    }

    /// Handles one launch. The OS controls socket access. The broker must record
    /// the capability and spawned identity before it releases this child.
    pub fn serve_one(&self, mut stream: UnixStream) -> io::Result<()> {
        let start: Start = wire::read(&mut stream)?;
        if !wire::lowercase_hex64(&start.capability_sha256) {
            return wire::write(
                &mut stream,
                &Reply::Refused {
                    reason: "invalid launch capability hash".into(),
                },
            );
        }
        let timeouts = Timeouts::default();
        let deadline = Instant::now() + timeouts.ready;
        let args = [
            OsStr::new("--home"),
            self.home.as_os_str(),
            OsStr::new("_supervisor"),
            OsStr::new(start.execution.as_str()),
        ];
        let started = match launch::spawn_launch(&self.program, &args, timeouts, true) {
            Ok(started) => started,
            Err(error) => {
                return unknown(
                    &mut stream,
                    &format!("supervisor could not be spawned: {error}"),
                );
            }
        };
        let mut held = Held::new(started, timeouts);
        let pid = held.started().pid;
        let identity = loop {
            if let Ok(identity) = process::inspect(pid)
                && identity.group == pid
                && !identity.zombie
            {
                break identity;
            }
            if Instant::now() >= deadline {
                return held.abandon(&mut stream, "supervisor identity was not available");
            }
            thread::sleep(Duration::from_millis(10));
        };
        serve_started(stream, held, identity, &start.capability_sha256, deadline)
    }
}

/// Holds one child until durable startup ownership and the READY handshake.
fn serve_started(
    mut stream: UnixStream,
    mut held: Held,
    target: Identity,
    capability_sha256: &str,
    deadline: Instant,
) -> io::Result<()> {
    let pid = target.pid;
    if wire::write_until(
        &mut stream,
        &Reply::Spawned {
            identity: target,
            backend: held.started().backend.into(),
        },
        deadline.min(Instant::now() + Duration::from_secs(5)),
    )
    .is_err()
    {
        return held.abandon(
            &mut stream,
            "broker disconnected before startup owner was recorded",
        );
    }
    let release: Release = match wire::read_until(&mut stream, deadline) {
        Ok(release) => release,
        Err(_) => {
            return held.abandon(&mut stream, "broker disconnected before capability release");
        }
    };
    if !wire::lowercase_hex64(&release.capability)
        || fs::sha256(release.capability.as_bytes()) != capability_sha256
    {
        return held.abandon(&mut stream, "capability did not match the recorded hash");
    }
    if let Err(error) = held
        .started()
        .release_capability(release.capability.as_bytes())
    {
        return held.abandon(&mut stream, &format!("capability release failed: {error}"));
    }
    let timeouts = held.timeouts;
    let started = held.take();
    let reaper = Reaper(pid);
    let sent = match started.complete(timeouts) {
        Ok(_) => wire::write(&mut stream, &Reply::Ready),
        Err(error) => {
            eprintln!("supervisor {pid} failed during launch or cleanup: {error}");
            unknown(&mut stream, "supervisor failed during launch or cleanup")
        }
    };
    drop(stream);
    drop(reaper);
    sent
}

/// Owns a gated child through early returns and unwinding.
struct Held {
    started: Option<StartedLaunch>,
    timeouts: Timeouts,
}

impl Held {
    /// Adopts the not-yet-released child.
    fn new(started: StartedLaunch, timeouts: Timeouts) -> Self {
        Self {
            started: Some(started),
            timeouts,
        }
    }
    /// Borrows the held launch.
    fn started(&mut self) -> &mut StartedLaunch {
        self.started.as_mut().expect("launch is held")
    }
    /// Transfers the launch after release.
    fn take(&mut self) -> StartedLaunch {
        self.started.take().expect("launch is held")
    }
    /// Closes the gate, cleans and reaps the exact child.
    fn abandon(mut self, stream: &mut UnixStream, reason: &str) -> io::Result<()> {
        let started = self.take();
        let pid = started.pid;
        let error = started.abandon(self.timeouts, reason.to_owned());
        eprintln!("supervisor {pid} was abandoned: {error}");
        let sent = unknown(
            stream,
            "supervisor was abandoned; cleanup requires reconciliation",
        );
        drop(Reaper(pid));
        sent
    }
}

impl Drop for Held {
    /// Cleans a still-gated child if its handler exits unexpectedly.
    fn drop(&mut self) {
        if let Some(started) = self.started.take() {
            let pid = started.pid;
            let error = started.abandon(self.timeouts, "worker handler stopped".into());
            eprintln!("supervisor {pid} was abandoned: {error}");
            drop(Reaper(pid));
        }
    }
}

/// The account-owned worker reaps the exact child it launched.
struct Reaper(i32);

impl Drop for Reaper {
    /// Waits for that child even if its response was lost.
    fn drop(&mut self) {
        loop {
            let mut status = 0;
            // SAFETY: this is one positive child PID and status points to writable storage.
            let result = unsafe { libc::waitpid(self.0, &mut status, 0) };
            if result == self.0 {
                return;
            }
            let error = io::Error::last_os_error();
            if error.kind() == io::ErrorKind::Interrupted {
                continue;
            }
            if error.raw_os_error() != Some(libc::ECHILD) {
                eprintln!("cannot reap supervisor {}: {error}", self.0);
            }
            return;
        }
    }
}

/// Reports an uncertain launch without authorizing a duplicate.
fn unknown(stream: &mut UnixStream, reason: &str) -> io::Result<()> {
    wire::write(
        stream,
        &Reply::Unknown {
            reason: reason.to_owned(),
        },
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::system::PendingLaunch;
    use std::{io::Write, path::PathBuf, thread, time::Instant};

    /// Short real-process launch and cleanup deadlines for the handshake fixtures.
    fn quick() -> Timeouts {
        Timeouts {
            ready: Duration::from_secs(10),
            dispatch: Duration::from_millis(500),
            grace: Duration::from_millis(300),
            kill: Duration::from_millis(300),
        }
    }

    /// Start a real session leader behind the production capability pipe.
    fn start_fixture(marker: &Path) -> (UnixStream, i32, thread::JoinHandle<io::Result<()>>) {
        start_fixture_with_timeouts(marker, quick())
    }

    /// Start the same real launcher with a specified shared READY budget.
    fn start_fixture_with_timeouts(
        marker: &Path,
        timeouts: Timeouts,
    ) -> (UnixStream, i32, thread::JoinHandle<io::Result<()>>) {
        let script = r#"cap=$(cat <&6); [ ${#cap} -eq 64 ] || exit 9; printf received > "$1"; echo $$ >&4; echo ready >&3; sleep 0.1"#;
        let args = [
            OsStr::new("-c"),
            OsStr::new(script),
            OsStr::new("sh"),
            marker.as_os_str(),
        ];
        let launch_deadline = Instant::now() + timeouts.ready;
        let started = launch::spawn_launch(Path::new("/bin/sh"), &args, timeouts, true).unwrap();
        let pid = started.pid;
        let deadline = Instant::now() + Duration::from_secs(5);
        let identity = loop {
            if let Ok(value) = process::inspect(pid)
                && value.group == pid
            {
                break value;
            }
            assert!(
                Instant::now() < deadline,
                "fixture did not become a session leader"
            );
            thread::sleep(Duration::from_millis(10));
        };
        let (client, server) = UnixStream::pair().unwrap();
        client
            .set_read_timeout(Some(Duration::from_secs(10)))
            .unwrap();
        server
            .set_read_timeout(Some(Duration::from_secs(10)))
            .unwrap();
        let expected = fs::sha256("a".repeat(64).as_bytes());
        let handle = thread::spawn(move || {
            serve_started(
                server,
                Held::new(started, timeouts),
                identity,
                &expected,
                launch_deadline,
            )
        });
        (client, pid, handle)
    }

    /// Confirm the exact child has already been collected by its coordinator.
    fn assert_reaped(pid: i32) {
        let mut status = 0;
        // SAFETY: WNOHANG tests one exact PID with valid status storage.
        let result = unsafe { libc::waitpid(pid, &mut status, libc::WNOHANG) };
        assert_eq!(result, -1);
        assert_eq!(
            io::Error::last_os_error().raw_os_error(),
            Some(libc::ECHILD)
        );
    }

    /// The protocol proves PID before capability and reaps after real READY.
    #[test]
    fn started_child_remains_gated_until_release_and_is_reaped() {
        let temp = tempfile::tempdir().unwrap();
        let marker: PathBuf = temp.path().join("received");
        let (mut client, pid, handle) = start_fixture(&marker);
        assert!(
            matches!(wire::read::<Reply>(&mut client).unwrap(), Reply::Spawned { identity, .. } if identity.pid == pid)
        );
        assert!(!marker.exists());
        wire::write(
            &mut client,
            &Release {
                capability: "a".repeat(64),
            },
        )
        .unwrap();
        assert!(matches!(
            wire::read::<Reply>(&mut client).unwrap(),
            Reply::Ready
        ));
        assert_eq!(std::fs::read(&marker).unwrap(), b"received");
        handle.join().unwrap().unwrap();
        assert_reaped(pid);
    }

    /// The client waits for the owner's durable completion before releasing real capability FD 6.
    #[tokio::test]
    async fn client_waits_for_owner_commit_before_release() {
        let temp = tempfile::tempdir().unwrap();
        let marker = temp.path().join("received");
        let (mut client, pid, handle) = start_fixture(&marker);
        let (identity, backend) = match wire::read::<Reply>(&mut client).unwrap() {
            Reply::Spawned {
                identity, backend, ..
            } => (identity, backend),
            _ => panic!("expected a spawned child"),
        };
        let pending = PendingLaunch::fixture(
            client,
            identity,
            backend,
            fs::sha256("a".repeat(64).as_bytes()),
        );
        let (observed_send, observed) = tokio::sync::oneshot::channel();
        let (commit_send, commit) = tokio::sync::oneshot::channel();
        let operation = tokio::spawn(pending.record_and_release("a".repeat(64), async move {
            let _ = observed_send.send(());
            commit.await.map_err(io::Error::other)
        }));
        observed.await.unwrap();
        assert!(!marker.exists(), "no capability before the owner commit");
        commit_send.send(()).unwrap();
        operation.await.unwrap().unwrap();
        assert_eq!(std::fs::read(&marker).unwrap(), b"received");
        handle.join().unwrap().unwrap();
        assert_reaped(pid);
    }

    /// A failed owner write closes the gate without releasing the capability.
    #[tokio::test]
    async fn failed_owner_commit_cleans_the_real_child() {
        let temp = tempfile::tempdir().unwrap();
        let marker = temp.path().join("received");
        let (mut client, pid, handle) = start_fixture(&marker);
        let (identity, backend) = match wire::read::<Reply>(&mut client).unwrap() {
            Reply::Spawned {
                identity, backend, ..
            } => (identity, backend),
            _ => panic!("expected a spawned child"),
        };
        let pending = PendingLaunch::fixture(
            client,
            identity,
            backend,
            fs::sha256("a".repeat(64).as_bytes()),
        );
        let result = pending
            .record_and_release("a".repeat(64), async {
                Err(io::Error::other("commit failed"))
            })
            .await;
        assert!(matches!(
            result,
            Err(crate::system::ClientError::OwnerNotRecorded(_))
        ));
        let _ = handle.join().unwrap();
        assert!(!marker.exists());
        assert_reaped(pid);
    }

    /// Cancelling the client while its owner write is pending closes the gate.
    #[tokio::test]
    async fn cancelling_owner_commit_cleans_the_real_child() {
        let temp = tempfile::tempdir().unwrap();
        let marker = temp.path().join("received");
        let (mut client, pid, handle) = start_fixture(&marker);
        let (identity, backend) = match wire::read::<Reply>(&mut client).unwrap() {
            Reply::Spawned {
                identity, backend, ..
            } => (identity, backend),
            _ => panic!("expected a spawned child"),
        };
        let pending = PendingLaunch::fixture(
            client,
            identity,
            backend,
            fs::sha256("a".repeat(64).as_bytes()),
        );
        let (observed_send, observed) = tokio::sync::oneshot::channel();
        let operation = tokio::spawn(pending.record_and_release("a".repeat(64), async move {
            let _ = observed_send.send(());
            std::future::pending::<io::Result<()>>().await
        }));
        observed.await.unwrap();
        operation.abort();
        assert!(operation.await.unwrap_err().is_cancelled());
        let _ = handle.join().unwrap();
        assert!(!marker.exists());
        assert_reaped(pid);
    }

    /// Dropping the broker connection before release closes the gate and cleans the child.
    #[test]
    fn disconnect_before_release_cleans_and_reaps_the_child() {
        let temp = tempfile::tempdir().unwrap();
        let marker = temp.path().join("received");
        let (mut client, pid, handle) = start_fixture(&marker);
        assert!(matches!(
            wire::read::<Reply>(&mut client).unwrap(),
            Reply::Spawned { .. }
        ));
        drop(client);
        let _ = handle.join().unwrap();
        assert!(!marker.exists());
        assert_reaped(pid);
    }

    /// A silent broker cannot hold the gate beyond the original launch budget.
    #[test]
    fn silent_release_expires_and_cleans_the_child() {
        let temp = tempfile::tempdir().unwrap();
        let marker = temp.path().join("received");
        let timeouts = Timeouts {
            ready: Duration::from_millis(900),
            ..quick()
        };
        let (mut client, pid, handle) = start_fixture_with_timeouts(&marker, timeouts);
        assert!(matches!(
            wire::read::<Reply>(&mut client).unwrap(),
            Reply::Spawned { .. }
        ));
        assert!(matches!(
            wire::read_until::<Reply>(&mut client, Instant::now() + Duration::from_secs(3))
                .unwrap(),
            Reply::Unknown { .. }
        ));
        handle.join().unwrap().unwrap();
        assert!(!marker.exists());
        assert_reaped(pid);
    }

    /// A wrong capability is refused and cannot start the child.
    #[test]
    fn wrong_capability_cleans_and_reaps_the_child() {
        let temp = tempfile::tempdir().unwrap();
        let marker = temp.path().join("received");
        let (mut client, pid, handle) = start_fixture(&marker);
        assert!(matches!(
            wire::read::<Reply>(&mut client).unwrap(),
            Reply::Spawned { .. }
        ));
        wire::write(
            &mut client,
            &Release {
                capability: "b".repeat(64),
            },
        )
        .unwrap();
        assert!(matches!(
            wire::read::<Reply>(&mut client).unwrap(),
            Reply::Unknown { .. }
        ));
        handle.join().unwrap().unwrap();
        assert!(!marker.exists());
        assert_reaped(pid);
    }

    /// An oversized release frame cannot keep the spawned child or pass the gate.
    #[test]
    fn oversized_release_cleans_and_reaps_the_child() {
        let temp = tempfile::tempdir().unwrap();
        let marker = temp.path().join("received");
        let (mut client, pid, handle) = start_fixture(&marker);
        assert!(matches!(
            wire::read::<Reply>(&mut client).unwrap(),
            Reply::Spawned { .. }
        ));
        client.write_all(&4097_u32.to_be_bytes()).unwrap();
        assert!(matches!(
            wire::read::<Reply>(&mut client).unwrap(),
            Reply::Unknown { .. }
        ));
        handle.join().unwrap().unwrap();
        assert!(!marker.exists());
        assert_reaped(pid);
    }

    /// A lost READY reply never cancels or duplicates an already released child.
    #[test]
    fn disconnect_after_release_still_completes_and_reaps() {
        let temp = tempfile::tempdir().unwrap();
        let marker = temp.path().join("received");
        let (mut client, pid, handle) = start_fixture(&marker);
        assert!(matches!(
            wire::read::<Reply>(&mut client).unwrap(),
            Reply::Spawned { .. }
        ));
        wire::write(
            &mut client,
            &Release {
                capability: "a".repeat(64),
            },
        )
        .unwrap();
        drop(client);
        let _ = handle.join().unwrap();
        assert_eq!(std::fs::read(&marker).unwrap(), b"received");
        assert_reaped(pid);
    }
}
