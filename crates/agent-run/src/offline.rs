//! Exclusive offline authority over a home's database.
//!
//! Initialization, schema migration, rollback, storage administration, deployment
//! and installation are the only operations besides the resident writer that
//! may hold a read-write database connection, and they hold it only through an
//! [`agent_run_store::writer::OfflineWriterGuard`]. The guard proves, before anything is touched, that no
//! broker serves the home (exclusive bind locks on both broker sockets,
//! `api.sock` and `writer.sock`), that no writer or other offline tool holds
//! the writer lease, and that every recorded supervisor, startup owner and
//! managed-service process is gone. The service-manager startup lock, which a
//! broker serving a custom socket still holds, is taken as well. All locks stay
//! held until the session is dropped; there is no hot initialization or
//! migration on any online path.

use crate::{
    Result,
    state::writer::{BrokerExclusion, OfflineWriterGuard, WriterLease},
};
use agent_run_domain::error::invalid;
use fs2::FileExt;
use std::{fs::File, os::unix::fs::OpenOptionsExt, path::Path};

/// One held offline session: the writer guard plus the service-manager lock.
pub struct OfflineSession {
    /// Exclusive offline authority over the home's database.
    pub guard: OfflineWriterGuard,
    /// Service-manager startup lock, released with the session.
    _services: File,
}

impl Drop for OfflineSession {
    /// Explicitly unlock the ended service-manager exclusion so unrelated
    /// forked children cannot retain it until exec. The database/broker guard
    /// still owns its exclusion while its SQL connection closes.
    fn drop(&mut self) {
        let _ = self._services.unlock();
    }
}

/// Acquires the exclusive offline authority of `home`.
///
/// # Errors
/// A running broker, writer, service manager or other offline tool, a
/// recorded process that is not provably gone, or an unusable lock file
/// refuses the session; nothing is created or modified before the refusal
/// other than empty private lock files.
pub fn acquire(home: &Path) -> Result<OfflineSession> {
    let api = home.join("api.sock");
    let writer = home.join("writer.sock");
    let exclusion = BrokerExclusion::acquire(home, &[api.as_path(), writer.as_path()])
        .map_err(busy("stop the resident broker before offline storage work"))?;
    let services = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .mode(0o600)
        .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC)
        .open(home.join(".services.lock"))?;
    services.try_lock_exclusive().map_err(|_| {
        invalid("stop the resident broker and its service manager before offline storage work")
    })?;
    let guard = match WriterLease::acquire_exclusive(home, exclusion) {
        Ok(guard) => guard,
        Err(error) => {
            // This file is already ours: release it even if an unrelated fork
            // inherited its descriptor while writer acquisition was failing.
            let _ = services.unlock();
            return Err(busy("another writer or offline tool holds this home")(
                error,
            ));
        }
    };
    Ok(OfflineSession {
        guard,
        _services: services,
    })
}

/// How a command-line write reaches the database.
pub enum Route {
    /// A broker serves the home: the write is a typed operation of an ordered
    /// producer on its writer socket.
    Online(agent_run_core::persist::Producer),
    /// No broker serves the home: the command holds the exclusive offline
    /// authority and writes through its guard.
    Offline(Box<OfflineSession>),
}

/// Chooses how a command-line write is made.
///
/// A connectable `<home>/writer.sock` means a broker is serving, and the write
/// goes through it; otherwise the exclusive offline authority is acquired, which
/// itself refuses when a broker or another offline tool appears in between.
///
/// # Errors
/// The offline authority could not be acquired.
pub async fn route(home: &Path) -> Result<Route> {
    let socket = home.join("writer.sock");
    if tokio::net::UnixStream::connect(&socket).await.is_ok() {
        return Ok(Route::Online(agent_run_core::persist::Producer::connect(
            &socket,
            agent_run_core::persist::cli_hello(),
            agent_run_core::persist::cli_producer_id(),
        )));
    }
    Ok(Route::Offline(Box::new(acquire(home)?)))
}

/// Maps the storage layer's `Conflict` (a held lock) to an operator message and
/// leaves every other refusal unchanged.
fn busy(message: &'static str) -> impl Fn(crate::Error) -> crate::Error {
    move |error| match error {
        crate::Error::Conflict => invalid(message),
        other => other,
    }
}
