//! Test support: an in-process writer broker for tests that run the real
//! `_supervisor` entrypoint without a resident `agent-run` broker process.
//!
//! The test process plays the broker's two roles exactly as production does:
//! it serves `<home>/writer.sock` over a real [`WriterService`] (kernel peer
//! identity, hello capability, writer-side authority checks all apply to the
//! supervisor child) and it performs the launch hand-over — capability hash and
//! startup-owner evidence are committed BEFORE the secret is written to the
//! child's private pipe. Seeding rows through the fenced fixture helpers before
//! the broker starts is raw setup only; it is not proof of an online fence.
//! Include with `#[path = "support/test_broker.rs"] mod test_broker;`.

#![allow(dead_code)]

use agent_run::transport::writer_socket::serve_listener;
use agent_run_core::{
    domain::AgentId,
    writer::{WriterConfig, WriterService},
    writer_server::WriterEndpoint,
};
use agent_run_platform::process;
use std::{
    os::fd::{AsRawFd, FromRawFd, OwnedFd},
    path::{Path, PathBuf},
    process::Stdio,
};
use tokio::{process::Command, sync::oneshot, task::JoinHandle};

/// A running writer service plus its socket server over one private home.
pub struct TestBroker {
    /// Home served.
    home: PathBuf,
    /// Stops the socket server.
    stop: Option<oneshot::Sender<()>>,
    /// The server task.
    server: Option<JoinHandle<agent_run::Result<()>>>,
    /// The writer (shut down at [`Self::stop`]).
    service: Option<WriterService>,
}

impl TestBroker {
    /// Starts the writer over the EXISTING schema-30 store of `home` and binds
    /// `<home>/writer.sock`.
    pub async fn start(home: &Path) -> Self {
        let service = WriterService::start(home, WriterConfig::default()).expect("writer starts");
        let socket = home.join("writer.sock");
        let _ = std::fs::remove_file(&socket);
        let listener = tokio::net::UnixListener::bind(&socket).expect("bind writer.sock");
        let endpoint = WriterEndpoint::new(
            service.handle(),
            process::inspect(std::process::id() as i32).expect("own identity"),
            home,
        );
        let (stop, stopped) = oneshot::channel::<()>();
        let server = tokio::spawn(async move {
            serve_listener(listener, endpoint, async {
                let _ = stopped.await;
            })
            .await
        });
        Self {
            home: home.to_path_buf(),
            stop: Some(stop),
            server: Some(server),
            service: Some(service),
        }
    }

    /// The broker's lane factory over this writer, for a `Service` under test.
    pub fn writer(&self) -> agent_run_core::broker_writer::BrokerWriter {
        agent_run_core::broker_writer::BrokerWriter::new(
            self.service.as_ref().expect("running").handle(),
        )
    }

    /// The broker's producer lane `scope` over this writer.
    pub fn lane(&self, scope: &str) -> agent_run_core::broker_writer::BrokerLane {
        agent_run_core::broker_writer::BrokerWriter::new(
            self.service.as_ref().expect("running").handle(),
        )
        .lane(scope)
    }

    /// Stops the server and drains the writer so the home is free again.
    pub async fn stop(mut self) {
        if let Some(stop) = self.stop.take() {
            let _ = stop.send(());
        }
        if let Some(server) = self.server.take() {
            let _ = server.await;
        }
        if let Some(service) = self.service.take() {
            let (boot_id, now) = agent_run_platform::monotonic::now().expect("clock");
            let _ = service
                .shutdown(agent_run_domain::storage::MonoDeadline {
                    boot_id,
                    ns: now + 5_000_000_000,
                })
                .await;
        }
        let _ = std::fs::remove_file(self.home.join("writer.sock"));
    }
}

impl Drop for TestBroker {
    /// A test that ends without [`TestBroker::stop`] still stops serving.
    fn drop(&mut self) {
        if let Some(server) = self.server.take() {
            server.abort();
        }
    }
}

/// One close-on-exec descriptor above the bootstrap range, backed by `/dev/null`.
fn bootstrap_fd() -> OwnedFd {
    let file = std::fs::File::options()
        .read(true)
        .write(true)
        .open("/dev/null")
        .expect("open bootstrap descriptor");
    // SAFETY: F_DUPFD_CLOEXEC duplicates the test-owned descriptor at a number
    // above the bootstrap targets; the returned descriptor has one owner.
    let duplicated = unsafe { libc::fcntl(file.as_raw_fd(), libc::F_DUPFD_CLOEXEC, 10) };
    assert!(duplicated >= 0, "duplicate bootstrap descriptor");
    // SAFETY: fcntl returned a fresh descriptor that is not owned elsewhere.
    unsafe { OwnedFd::from_raw_fd(duplicated) }
}

/// Brokers by home: every supervisor of one home shares its single writer.
static BROKERS: std::sync::OnceLock<
    tokio::sync::Mutex<std::collections::HashMap<PathBuf, std::sync::Arc<TestBroker>>>,
> = std::sync::OnceLock::new();

/// The broker serving `home`, started on first use.
pub async fn broker_for(home: &Path) -> std::sync::Arc<TestBroker> {
    let registry = BROKERS.get_or_init(Default::default);
    let mut brokers = registry.lock().await;
    if let Some(broker) = brokers.get(home) {
        return broker.clone();
    }
    let broker = std::sync::Arc::new(TestBroker::start(home).await);
    brokers.insert(home.to_path_buf(), broker.clone());
    broker
}

/// Stops and forgets the broker of `home` (if any) so the home is free again.
pub async fn stop_broker(home: &Path) {
    let registry = BROKERS.get_or_init(Default::default);
    let broker = registry.lock().await.remove(home);
    if let Some(broker) = broker.and_then(|broker| std::sync::Arc::try_unwrap(broker).ok()) {
        broker.stop().await;
    }
}

/// A launched real supervisor: the child process and the broker serving it.
pub struct Supervised {
    /// The `_supervisor` child (its own session leader).
    pub child: tokio::process::Child,
    /// Home served.
    home: PathBuf,
    /// Exact execution whose owned descendants this panic guard may clean.
    execution: AgentId,
    /// Supervisor birth/token captured before the bootstrap capability was released.
    identity: process::Identity,
    /// The writer broker serving it (shared by every supervisor of the home).
    _broker: std::sync::Arc<TestBroker>,
}

impl Drop for Supervised {
    /// On fixture panic, clean only verified owned processes before the temporary home disappears.
    /// Normal successful runs retain their existing explicit wait/cancellation lifecycle.
    fn drop(&mut self) {
        if !std::thread::panicking() {
            return;
        }
        let safe = |owned: &mut process::OwnedProcess| {
            let Some(leader) = &owned.leader else {
                return;
            };
            // SAFETY: getpgrp reads the current process group without side effects.
            if leader.pid == std::process::id() as i32 || leader.group == unsafe { libc::getpgrp() }
            {
                return;
            }
            if !matches!(owned.cleanup_blocking(std::time::Duration::from_millis(100)), Ok(proof) if proof.confirmed)
            {
                eprintln!("fixture panic cleanup remains unconfirmed");
            }
        };
        if process::observe(
            Some(self.identity.pid),
            Some(&self.identity.token),
            Some(self.identity.birth),
        ) == process::ProcessState::Alive
        {
            let mut owned = process::OwnedProcess::capture(self.identity.pid);
            if owned.leader.as_ref().is_some_and(|leader| {
                leader.token == self.identity.token && leader.birth == self.identity.birth
            }) {
                safe(&mut owned);
            }
        }
        if let Ok(store) = agent_run_store::fixture::open(&self.home) {
            let owners = store.conn.prepare("SELECT owner_kind,owner_id FROM process_ownership WHERE owner_id=?1 OR owner_id IN(SELECT id FROM attempts WHERE agent_id=?1)")
                .and_then(|mut statement| statement.query_map([self.execution.as_str()], |row| Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?)))?.collect::<rusqlite::Result<Vec<_>>>());
            if let Ok(owners) = owners {
                for (kind, id) in owners {
                    if let Ok(Some(mut owned)) = store.remembered_processes(&kind, &id) {
                        safe(&mut owned);
                    }
                }
            }
        }
    }
}

impl std::ops::Deref for Supervised {
    type Target = tokio::process::Child;

    fn deref(&self) -> &Self::Target {
        &self.child
    }
}

impl std::ops::DerefMut for Supervised {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.child
    }
}

impl Supervised {
    /// Waits for the supervisor to exit, as `Child::wait`.
    pub async fn wait(&mut self) -> std::io::Result<std::process::ExitStatus> {
        self.child.wait().await
    }

    /// Stops the home's broker after the supervisor ended.
    pub async fn finish(self) {
        let home = self.home.clone();
        drop(self);
        stop_broker(&home).await;
    }
}

/// Starts a broker for `home`, spawns the real `_supervisor` of `id` (an
/// admitted `starting` execution) with the launch capability on its private
/// descriptor 6, commits the capability hash and the child's startup-owner
/// evidence through the fenced fixture store, and then releases the secret.
pub async fn spawn_supervisor(program: &str, home: &Path, id: &AgentId) -> Supervised {
    spawn_supervisor_with_env(program, home, id, &[]).await
}

/// [`spawn_supervisor`] with extra child-only environment values. The parent
/// runtime home and every worker binding are removed from the child first.
pub async fn spawn_supervisor_with_env(
    program: &str,
    home: &Path,
    id: &AgentId,
    environment: &[(&str, &std::ffi::OsStr)],
) -> Supervised {
    use std::os::unix::process::CommandExt;
    let broker = broker_for(home).await;
    let secret = format!(
        "{}{}",
        uuid::Uuid::new_v4().simple(),
        uuid::Uuid::new_v4().simple()
    );
    let (read, write) = {
        let mut fds = [0_i32; 2];
        // SAFETY: pipe fills two fresh descriptors on success.
        assert_eq!(unsafe { libc::pipe(fds.as_mut_ptr()) }, 0);
        // SAFETY: both descriptors are fresh and exclusively owned here.
        unsafe {
            for fd in fds {
                libc::fcntl(fd, libc::F_SETFD, libc::FD_CLOEXEC);
            }
            (OwnedFd::from_raw_fd(fds[0]), OwnedFd::from_raw_fd(fds[1]))
        }
    };
    let (ready, identity, error) = (bootstrap_fd(), bootstrap_fd(), bootstrap_fd());
    let mut command = Command::new(program);
    command.env_remove("AGENT_RUN_HOME");
    for (name, _) in std::env::vars_os() {
        if name.to_string_lossy().starts_with("AGENT_RUN_") {
            command.env_remove(name);
        }
    }
    command.envs(environment.iter().copied());
    command
        .arg("--home")
        .arg(home)
        .args([
            "_supervisor",
            "--ready-fd",
            "3",
            "--identity-fd",
            "4",
            "--error-fd",
            "5",
            "--capability-fd",
            "6",
            id.as_str(),
        ])
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null());
    // Duplicate every borrowed input above ALL bootstrap targets before fork:
    // dup2(source, source) retains CLOEXEC and overlapping mappings clobber inputs.
    let private_sources = [
        ready.as_raw_fd(),
        identity.as_raw_fd(),
        error.as_raw_fd(),
        read.as_raw_fd(),
    ]
    .map(|fd| {
        // SAFETY: fd is borrowed from a live owned input; fcntl returns a fresh owned copy.
        let duplicated = unsafe { libc::fcntl(fd, libc::F_DUPFD_CLOEXEC, 10) };
        assert!(duplicated >= 10, "safe fixture bootstrap duplicate");
        // SAFETY: fcntl returned a fresh descriptor with no other Rust owner.
        unsafe { OwnedFd::from_raw_fd(duplicated) }
    });
    let sources = private_sources.each_ref().map(AsRawFd::as_raw_fd);
    // SAFETY: this test-only pre-exec closure captures only raw descriptors and
    // invokes the async-signal-safe `setsid` and `dup2` syscalls before exec.
    unsafe {
        command.as_std_mut().pre_exec(move || {
            if libc::setsid() < 0 {
                return Err(std::io::Error::last_os_error());
            }
            for (source, target) in sources.into_iter().zip([3, 4, 5, 6]) {
                if libc::dup2(source, target) < 0 {
                    return Err(std::io::Error::last_os_error());
                }
            }
            for source in sources {
                libc::close(source);
            }
            Ok(())
        });
    }
    let child = command
        .kill_on_drop(true)
        .spawn()
        .expect("spawn supervisor");
    drop((ready, identity, error, read, private_sources));
    // Launch hand-over: durable evidence first, secret last.
    let pid = child.id().expect("child pid") as i32;
    let evidence = process::inspect(pid).expect("child identity");
    let identity = evidence.clone();
    {
        let mut store = agent_run_store::fixture::open(home).expect("fixture store");
        store
            .record_supervisor_launch(id, &agent_run_core::fs::sha256(secret.as_bytes()))
            .expect("capability hash recorded");
        store
            .record_startup_owner(id, &serde_json::to_value(evidence).unwrap())
            .expect("startup owner recorded");
    }
    let mut pipe = std::fs::File::from(write);
    std::io::Write::write_all(&mut pipe, secret.as_bytes()).expect("capability released");
    drop(pipe);
    Supervised {
        child,
        home: home.to_path_buf(),
        execution: id.clone(),
        identity,
        _broker: broker,
    }
}
