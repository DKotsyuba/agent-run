//! Test support: a real writer service reached through an in-process duplex
//! transport that speaks the production wire protocol.
//!
//! Engines and supervisors persist only through [`Persist`]; this harness gives
//! a test the same path in a disposable private home without a second process.
//! Most tests use duplex streams; native hook tests can explicitly serve the
//! same writer through their private `writer.sock`. The server side is the production
//! [`agent_run_core::writer_server::serve_connection`]; the only thing the test
//! supplies is the peer identity, which is this very process (the "supervisor"
//! of the execution under test). Include with
//! `#[path = "common/writer.rs"] mod writer;` (or the sibling path).

#![allow(dead_code)]

use agent_run_core::{
    broker_writer::BrokerWriter,
    domain::AgentId,
    persist::{self, Persist, Producer},
    storage_client::{Connection, Dial},
    writer::{WriterConfig, WriterService},
    writer_limits::ConnectionSlots,
    writer_server::{PeerIdentity, WriterEndpoint, serve_connection},
};
use agent_run_platform::process;
use agent_run_store::Store;
use std::{future::Future, path::Path, pin::Pin, sync::Arc};

/// Runs one future to completion on a private current-thread runtime, for
/// synchronous tests that exercise asynchronous storage entry points.
pub fn block_on<F: Future>(future: F) -> F::Output {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("test runtime")
        .block_on(future)
}

/// A running writer service plus the in-process server side of its socket.
pub struct Harness {
    /// The resident writer (shut down when dropped with the harness).
    pub service: WriterService,
    /// Server-side wiring shared by every dialed connection.
    endpoint: Arc<WriterEndpoint>,
    /// Connection pools shared by every dialed connection.
    slots: ConnectionSlots,
}

impl Harness {
    /// Serve native hook callers over this fixture's private socket, using the real writer endpoint.
    /// Only this test process is an accepted peer; dropping the guard aborts listening and removes the socket.
    pub fn serve_unix(&self, home: &Path) -> NativeSocket {
        let path = home.join("writer.sock");
        let listener = tokio::net::UnixListener::bind(&path).expect("fixture writer socket");
        let endpoint = self.endpoint.clone();
        let slots = self.slots.clone();
        let task = tokio::spawn(async move {
            while let Ok((stream, _)) = listener.accept().await {
                let Ok(credentials) = stream.peer_cred() else {
                    continue;
                };
                if credentials
                    .pid()
                    .is_some_and(|pid| pid != std::process::id() as i32)
                {
                    continue;
                }
                let Some(permit) = slots.preauth() else {
                    continue;
                };
                let peer = PeerIdentity {
                    uid: credentials.uid(),
                    process: process::inspect(std::process::id() as i32).expect("fixture peer"),
                };
                let endpoint = endpoint.clone();
                let slots = slots.clone();
                tokio::spawn(async move {
                    let _ = serve_connection(stream, peer, endpoint, slots, permit).await;
                });
            }
        });
        NativeSocket { task, path }
    }
    /// Starts the writer over the EXISTING schema-30 store of `home`.
    pub fn start(home: &Path) -> Self {
        Self::start_with(home, WriterConfig::default())
    }

    /// [`Self::start`] with an explicit writer configuration.
    pub fn start_with(home: &Path, config: WriterConfig) -> Self {
        let service = WriterService::start(home, config).expect("writer service starts");
        let broker = process::inspect(std::process::id() as i32).expect("own identity");
        let endpoint = WriterEndpoint::new(service.handle(), broker, home);
        let slots = ConnectionSlots::new(endpoint.limits);
        Self {
            service,
            endpoint: Arc::new(endpoint),
            slots,
        }
    }

    /// The broker's producer lanes over this writer.
    pub fn broker(&self) -> BrokerWriter {
        BrokerWriter::new(self.service.handle())
    }

    /// A dialer whose every connection is served in process.
    pub fn dial(&self) -> DuplexDial {
        DuplexDial {
            endpoint: self.endpoint.clone(),
            slots: self.slots.clone(),
        }
    }

    /// Connects the supervisor of `id` prepared by [`prepare_supervisor`].
    ///
    /// Only typed operations run here: the producer's hello carries the
    /// capability recorded during preparation, a launching execution commits
    /// its ownership through the writer exactly like a real supervisor
    /// bootstrap, and an already owned attempt is pinned by an ordered re-hello.
    pub async fn supervisor(&self, home: &Path, prep: SupervisorPrep, id: &AgentId) -> Persist {
        let producer = Producer::with_dial(
            self.dial(),
            persist::supervisor_hello(&prep.logical, id, prep.capability),
            persist::supervisor_producer_id(id),
        );
        let mut persist = Persist::open(home, id, producer).expect("persist opens");
        if prep.launching {
            persist
                .record_owner(&prep.me, None)
                .await
                .expect("owner committed");
        }
        if let Some(attempt) = prep.attempt {
            persist.repin(&attempt).await.expect("attempt pinned");
        }
        persist
    }
}

/// Test-owned native listener lifetime; it never outlives its temporary home intentionally.
pub struct NativeSocket {
    /// Listener task serving this fixture's actual writer.
    task: tokio::task::JoinHandle<()>,
    /// Socket inode owned by this guard.
    path: std::path::PathBuf,
}

impl Drop for NativeSocket {
    /// Stop the listener and remove only its own fixture socket, without changing production services.
    fn drop(&mut self) {
        self.task.abort();
        let _ = std::fs::remove_file(&self.path);
    }
}

/// Everything a test records about one execution BEFORE the writer starts.
pub struct SupervisorPrep {
    /// Launch capability secret (64 lowercase hex characters).
    capability: String,
    /// Lineage root of the execution.
    logical: AgentId,
    /// This process, the supervisor under test.
    me: agent_run_platform::process::Identity,
    /// Whether the execution is still launching (ownership is committed
    /// through the writer) rather than already owned by fixture setup.
    launching: bool,
    /// The execution's active owned attempt, when it has one.
    attempt: Option<String>,
}

/// Raw offline fixture setup for the supervisor of `id`, to run BEFORE
/// [`Harness::start`] (the writer is then the only writer of the database).
///
/// A `starting`/`cancelling` execution gets its launch capability and this
/// process as startup owner, as the broker's launch would record them; a row a
/// fixture already moved past launch gets this process installed as its owner
/// directly. Every query failure is a test failure; only a missing optional
/// attempt row means "no attempt".
pub fn prepare_supervisor(store: &mut Store, id: &AgentId) -> SupervisorPrep {
    use rusqlite::OptionalExtension;
    let capability = format!(
        "{}{}",
        uuid::Uuid::new_v4().simple(),
        uuid::Uuid::new_v4().simple()
    );
    let digest = agent_run_core::fs::sha256(capability.as_bytes());
    let me = process::inspect(std::process::id() as i32).expect("own identity");
    let status: String = store
        .conn
        .query_row(
            "SELECT status FROM agents WHERE id=?",
            [id.as_str()],
            |row| row.get(0),
        )
        .expect("admitted execution");
    let launching = matches!(status.as_str(), "starting" | "cancelling");
    if launching {
        store
            .record_supervisor_launch(id, &digest)
            .expect("launch capability recorded");
        store
            .record_startup_owner(id, &serde_json::to_value(&me).unwrap())
            .expect("startup owner recorded");
    } else {
        store
            .conn
            .execute(
                "UPDATE agents SET supervisor_capability_sha256=?, supervisor_pid=?, \
                 supervisor_identity=?, supervisor_birth_time=? WHERE id=?",
                rusqlite::params![digest, me.pid, me.token, me.birth, id.as_str()],
            )
            .expect("owner installed");
    }
    let logical = store.get(id).expect("admitted execution").root_agent_id;
    let attempt: Option<String> = store
        .conn
        .query_row(
            "SELECT id FROM attempts WHERE agent_id=? AND ownership_active=1",
            [id.as_str()],
            |row| row.get(0),
        )
        .optional()
        .expect("attempt lookup");
    SupervisorPrep {
        capability,
        logical,
        me,
        launching,
        attempt,
    }
}

/// Dials the in-process server of a [`Harness`].
pub struct DuplexDial {
    /// Server-side wiring.
    endpoint: Arc<WriterEndpoint>,
    /// Shared connection pools.
    slots: ConnectionSlots,
}

impl Dial for DuplexDial {
    /// Creates a duplex pair, serves its server end with the production
    /// connection logic and returns the client end.
    fn dial(&self) -> Pin<Box<dyn Future<Output = std::io::Result<Connection>> + Send + '_>> {
        Box::pin(async move {
            let (client, server) = tokio::io::duplex(1 << 20);
            let permit = self
                .slots
                .preauth()
                .ok_or_else(|| std::io::Error::other("no pre-authentication slot"))?;
            let me = process::inspect(std::process::id() as i32)?;
            let peer = PeerIdentity {
                // SAFETY: geteuid only reads this process's effective identity.
                uid: unsafe { libc::geteuid() },
                process: me,
            };
            let (endpoint, slots) = (self.endpoint.clone(), self.slots.clone());
            tokio::spawn(async move {
                let _ = serve_connection(server, peer, endpoint, slots, permit).await;
            });
            Ok(Box::new(client) as Connection)
        })
    }
}
