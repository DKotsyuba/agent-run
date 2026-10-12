//! The broker's private writer socket (`writer.sock`): bind, accept, shutdown.
//!
//! The per-connection protocol (hello, authority derivation, frame bounds,
//! request hand-off to the one writer) is [`agent_run_core::writer_server`]; this
//! module only owns the listening socket and the kernel-attested identity of
//! each connecting process (the internal `peer` helper). The `.writer.sock.lock` startup
//! lock taken at bind time is the lock offline tools take to prove that no
//! broker serves the home, and it is held for the whole life of the listener.

use super::{peer::peer_identity, socket::bind_private};
use crate::Result;
use agent_run_core::writer_server::{PeerIdentity, WriterEndpoint, serve_connection};
use std::{future::Future, path::Path, sync::Arc, time::Duration};
use tokio::{net::UnixListener, task::JoinSet};

/// Open-file soft limit test candidates ask for at start (connection table plus
/// database and API descriptors).
#[cfg(feature = "test-fixtures")]
const WANTED_OPEN_FILES: u64 = 4096;

/// Descriptors beyond the connection pools: the database and its journal files,
/// the API socket and its connections' headroom, logs and child pipes.
const RESERVED_DESCRIPTORS: u64 = 64;

/// Serves `<home>/writer.sock` until `shutdown` resolves.
///
/// On shutdown it stops accepting, gives admitted connections five seconds to
/// finish and then closes them; accepted writer jobs still complete or roll
/// back inside the writer. The socket inode it created is removed.
///
/// # Errors
/// The lock is held, the socket path is not a private owned socket, or the
/// bind or accept fails.
pub async fn serve(
    home: &Path,
    endpoint: WriterEndpoint,
    shutdown: impl Future<Output = ()>,
) -> Result<()> {
    let (listener, _guard) = bind_private(&home.join("writer.sock")).await?;
    serve_listener(listener, endpoint, shutdown).await
}

/// Serves an already bound listener; see [`serve`].
pub async fn serve_listener(
    listener: UnixListener,
    endpoint: WriterEndpoint,
    shutdown: impl Future<Output = ()>,
) -> Result<()> {
    // Test candidates may raise the soft limit (common R9 section 9); production
    // never changes a host limit and instead refuses to serve when the limit
    // cannot hold the configured connection pools.
    #[cfg(feature = "test-fixtures")]
    let _ = agent_run_core::writer_limits::raise_open_file_limit(WANTED_OPEN_FILES);
    let soft = agent_run_core::writer_limits::open_file_soft_limit()?;
    let mut endpoint = endpoint;
    endpoint.limits = endpoint
        .limits
        .fit_open_files(soft, RESERVED_DESCRIPTORS)
        .ok_or_else(|| {
            crate::error::invalid(format!(
                "open-file limit {soft} cannot hold the writer socket pools; raise the service limit"
            ))
        })?;
    let slots = agent_run_core::writer_limits::ConnectionSlots::new(endpoint.limits);
    let endpoint = Arc::new(endpoint);
    let mut connections = JoinSet::new();
    tokio::pin!(shutdown);
    loop {
        tokio::select! {
            () = &mut shutdown => break,
            accepted = listener.accept() => {
                let (stream, _) = match accepted {
                    Ok(accepted) => accepted,
                    // Descriptor or memory exhaustion and aborted handshakes are
                    // transient: back off and keep serving. Anything else ends
                    // the listener, and its caller observes the failure.
                    Err(error) if transient_accept(&error) => {
                        tokio::time::sleep(Duration::from_millis(50)).await;
                        continue;
                    }
                    Err(error) => return Err(error.into()),
                };
                // A full pre-authentication pool drops the socket unread.
                let Some(permit) = slots.preauth() else {
                    continue;
                };
                // The kernel, not the peer, names the connecting process.
                let Ok(super::peer::PeerIdentity { uid, process }) = peer_identity(&stream) else {
                    continue;
                };
                let endpoint = endpoint.clone();
                let slots = slots.clone();
                connections.spawn(async move {
                    let _ = serve_connection(
                        stream,
                        PeerIdentity { uid, process },
                        endpoint,
                        slots,
                        permit,
                    )
                    .await;
                });
            }
            Some(_) = connections.join_next(), if !connections.is_empty() => {}
        }
    }
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    while !connections.is_empty() && tokio::time::Instant::now() < deadline {
        let left = deadline.saturating_duration_since(tokio::time::Instant::now());
        if tokio::time::timeout(left, connections.join_next())
            .await
            .is_err()
        {
            break;
        }
    }
    connections.abort_all();
    Ok(())
}

/// Whether an `accept` error is a transient resource condition.
fn transient_accept(error: &std::io::Error) -> bool {
    matches!(
        error.raw_os_error(),
        Some(libc::EMFILE | libc::ENFILE | libc::ENOBUFS | libc::ENOMEM | libc::ECONNABORTED)
    )
}

#[cfg(test)]
mod tests {
    //! Classification of `accept` failures.
    use super::transient_accept;

    /// Resource exhaustion and aborted handshakes are backed off; a broken
    /// listener (bad descriptor, not a socket, invalid) is fatal and surfaces.
    #[test]
    fn only_resource_conditions_are_transient() {
        for code in [
            libc::EMFILE,
            libc::ENFILE,
            libc::ENOBUFS,
            libc::ENOMEM,
            libc::ECONNABORTED,
        ] {
            assert!(transient_accept(&std::io::Error::from_raw_os_error(code)));
        }
        for code in [libc::EBADF, libc::ENOTSOCK, libc::EINVAL, libc::EPERM] {
            assert!(!transient_accept(&std::io::Error::from_raw_os_error(code)));
        }
        assert!(!transient_accept(&std::io::Error::other("custom")));
    }
}
