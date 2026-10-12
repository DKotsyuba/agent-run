//! Kernel-attested identity of a Unix-socket peer.
//!
//! The writer transport never trusts a role, actor or process id written by the
//! peer. It asks the kernel for the connecting user and process id
//! (`LOCAL_PEERPID` / `SO_PEERCRED` through tokio) and then inspects that exact
//! process for its start token and birth time, so a reused PID cannot inherit
//! another process's identity. Any missing or unverifiable fact refuses the
//! connection (fail closed).
use crate::{Error, Result, error::invalid};
use agent_run_platform::process::{self, Identity};
use tokio::net::UnixStream;

/// Verified identity of the process on the other end of an accepted or
/// connected Unix stream.
#[derive(Debug, Clone)]
pub struct PeerIdentity {
    /// Effective user id of the peer, equal to this process's user.
    pub uid: u32,
    /// Live identity (pid, birth, token, group) of the peer process.
    pub process: Identity,
}

/// Returns the kernel-verified identity of `stream`'s peer.
///
/// Requires the peer to run as the same effective user as this process, to
/// expose a positive process id, and to be inspectable (alive, not a zombie).
/// The identity is a snapshot: a caller that keeps the connection pinned to it
/// must still compare it with durable ownership records inside each write.
///
/// # Errors
/// A validation error for another user, a missing/invalid pid or an
/// unreadable/zombie process; the connection must then be closed.
pub fn peer_identity(stream: &UnixStream) -> Result<PeerIdentity> {
    let credentials = stream.peer_cred().map_err(Error::Io)?;
    // SAFETY: geteuid has no arguments or memory safety preconditions.
    let uid = unsafe { libc::geteuid() };
    if credentials.uid() != uid {
        return Err(invalid("socket peer has another user ID"));
    }
    let pid = credentials
        .pid()
        .filter(|pid| *pid > 1)
        .ok_or_else(|| invalid("socket peer process id is unavailable"))?;
    let process =
        process::inspect(pid).map_err(|_| invalid("socket peer process cannot be inspected"))?;
    if process.zombie {
        return Err(invalid("socket peer process has exited"));
    }
    Ok(PeerIdentity { uid, process })
}

#[cfg(test)]
mod tests {
    //! The kernel reports this very process as the peer of a local connection.
    use super::*;
    use tokio::net::UnixListener;

    /// Both ends of a loopback connection resolve to this process's identity.
    #[tokio::test]
    async fn peer_identity_is_this_process() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("peer.sock");
        let listener = UnixListener::bind(&path).unwrap();
        let client = UnixStream::connect(&path).await.unwrap();
        let (server, _) = listener.accept().await.unwrap();
        let me = process::inspect(std::process::id() as i32).unwrap();
        for stream in [&client, &server] {
            let peer = peer_identity(stream).expect("kernel peer identity");
            assert_eq!(peer.process.pid, me.pid);
            assert_eq!(peer.process.token, me.token);
            assert_eq!(peer.process.birth, me.birth);
        }
    }

    /// Kills and reaps the helper child even when an assertion unwinds.
    struct Child(
        /// The spawned peer; owned exclusively, so dropping it ends the process.
        std::process::Child,
    );

    impl Drop for Child {
        /// Terminates the child process.
        fn drop(&mut self) {
            let _ = self.0.kill();
            let _ = self.0.wait();
        }
    }

    /// Environment variable that turns this test binary into the connecting peer.
    const CHILD_SOCKET: &str = "AGENT_RUN_PEER_TEST_CHILD_SOCKET";

    /// Names of inherited variables that must never reach a test child: the
    /// real runtime home and every worker identity, token or finish flag.
    fn leaked_runtime_variables() -> Vec<String> {
        std::env::vars_os()
            .map(|(name, _)| name.to_string_lossy().into_owned())
            .filter(|name| name == "AGENT_RUN_HOME" || name.starts_with("AGENT_RUN_WORKER_"))
            .collect()
    }

    /// The server side names the connecting child, not the accepting process.
    ///
    /// The test binary re-runs this very test as the client with a cleared
    /// environment holding only [`CHILD_SOCKET`]: in client mode it first
    /// asserts that no runtime home or worker variable (names only, never
    /// values) was inherited, then connects to the socket and blocks until the
    /// server closes the connection. A client that fails its assertion exits
    /// before connecting, which the server reports as an early child exit.
    #[tokio::test]
    async fn peer_identity_names_the_other_process() {
        if let Ok(path) = std::env::var(CHILD_SOCKET) {
            let leaked = leaked_runtime_variables();
            assert!(leaked.is_empty(), "child inherited {leaked:?}");
            let mut stream = std::os::unix::net::UnixStream::connect(path).unwrap();
            let _ = std::io::Read::read(&mut stream, &mut [0u8; 1]);
            return;
        }
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("peer.sock");
        let listener = UnixListener::bind(&path).unwrap();
        let name = format!(
            "{}::peer_identity_names_the_other_process",
            module_path!().split_once("::").unwrap().1
        );
        let mut child = Child(
            std::process::Command::new(std::env::current_exe().unwrap())
                .env_clear()
                .args(["--exact", &name, "--nocapture", "--test-threads=1"])
                .env(CHILD_SOCKET, &path)
                .spawn()
                .unwrap(),
        );
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
        let server = loop {
            if let Some(status) = child.0.try_wait().unwrap() {
                panic!("peer child exited before connecting: {status}");
            }
            assert!(
                std::time::Instant::now() < deadline,
                "peer child did not connect"
            );
            let wait = std::time::Duration::from_millis(50);
            if let Ok(accepted) = tokio::time::timeout(wait, listener.accept()).await {
                break accepted.unwrap().0;
            }
        };
        let peer = peer_identity(&server).expect("kernel peer identity");
        let expected = process::inspect(child.0.id() as i32).unwrap();
        assert_ne!(peer.process.pid, std::process::id() as i32);
        assert_eq!(peer.process.pid, expected.pid);
        assert_eq!(peer.process.token, expected.token);
        assert_eq!(peer.process.birth, expected.birth);
    }
}
