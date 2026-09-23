//! Bounded Claude inbox delivery through a session descriptor and Unix socket.

use super::{Evidence, Notice};
use serde_json::{json, Value};
use std::os::unix::fs::FileTypeExt;
use std::path::{Path, PathBuf};
use std::time::Duration;
use tokio::io::AsyncWriteExt;
use tokio::net::UnixStream;

const DESCRIPTOR_LIMIT: usize = 512;
const SESSION_LIMIT: usize = 512;
const MESSAGE_LIMIT: usize = 4096;

/// Delivers one rendered completion notice through a specified Claude registry.
///
/// The registry and socket path are supplied by the caller so tests and embedded
/// hosts can use private temporary directories. A missing descriptor, dead
/// socket, or deleted endpoint is classified as a definite session loss, while
/// an endpoint that is not a socket (such as a directory) is classified as an
/// unavailable endpoint because Linux reports both as `ECONNREFUSED`; an
/// interrupted write is
/// classified as ambiguous because the inbox has no acknowledgement protocol.
pub async fn send(registry: &Path, session: &str, notice: &Notice) -> Evidence {
    send_after(registry, session, notice, async {}).await
}

/// [`send`] with `before_write` awaited after the connection is established
/// and before any frame is written. Production passes an already-ready
/// future; tests use it to order a peer close before the write, so an
/// interrupted write is observed deterministically instead of racing.
pub async fn send_after(
    registry: &Path,
    session: &str,
    notice: &Notice,
    before_write: impl std::future::Future<Output = ()>,
) -> Evidence {
    if !bounded(session, SESSION_LIMIT) || notice.render().is_err() {
        return Evidence::new("uds_rejected", false, false);
    }
    let message = match notice.render() {
        Ok(message) if message.len() <= MESSAGE_LIMIT => message,
        _ => return Evidence::new("uds_rejected", false, false),
    };
    let Some((pid, socket)) = resolve(registry, session) else {
        return Evidence::new("uds_session_gone", false, false);
    };
    let Some(token) = token(registry, pid) else {
        return Evidence::new("uds_rejected", false, false);
    };
    let mut stream =
        match tokio::time::timeout(Duration::from_secs(1), UnixStream::connect(&socket)).await {
            Ok(Ok(stream)) => stream,
            // A refused connection to a socket endpoint, or a path removed
            // after descriptor resolution, means the peer died. Linux also
            // refuses non-socket files, which are unavailable rather than lost.
            Ok(Err(error))
                if matches!(
                    error.kind(),
                    std::io::ErrorKind::ConnectionRefused | std::io::ErrorKind::NotFound
                ) && socket_is_gone_or_socket(&socket) =>
            {
                return Evidence::new("uds_session_gone", false, false);
            }
            Ok(Err(_)) => return Evidence::new("uds_unavailable", false, false),
            Err(_) => return Evidence::new("uds_session_gone", false, false),
        };
    before_write.await;
    let frames = format!(
        "{}\n{}\n",
        json!({"type":"auth","token":token}),
        json!({"type":"user","message":{"role":"user","content":message}}),
    );
    match tokio::time::timeout(Duration::from_secs(5), stream.write_all(frames.as_bytes())).await {
        Ok(Ok(())) => Evidence::new("uds_written", true, false),
        Ok(Err(error))
            if matches!(
                error.kind(),
                std::io::ErrorKind::BrokenPipe
                    | std::io::ErrorKind::ConnectionReset
                    | std::io::ErrorKind::TimedOut
            ) =>
        {
            Evidence::new("uds_ambiguous", false, true)
        }
        Ok(Err(_)) | Err(_) => Evidence::new("uds_unavailable", false, false),
    }
}

/// Returns whether a failed endpoint is absent or is an actual Unix socket.
///
/// Missing paths and socket inodes represent a peer that disappeared or stopped
/// listening. Existing regular files, directories, FIFOs, and symlinks return
/// `false` so callers classify them as unavailable configuration rather than a
/// lost session. Metadata errors other than not-found are likewise unavailable.
fn socket_is_gone_or_socket(path: &Path) -> bool {
    match std::fs::symlink_metadata(path) {
        Ok(metadata) => metadata.file_type().is_socket(),
        Err(error) => error.kind() == std::io::ErrorKind::NotFound,
    }
}

/// Returns whether a nonblank NUL-free value fits its protocol field limit.
fn bounded(value: &str, limit: usize) -> bool {
    !value.trim().is_empty() && !value.contains('\0') && value.chars().count() <= limit
}

/// Resolves one Claude descriptor while ignoring malformed or unreadable peers.
fn resolve(registry: &Path, session: &str) -> Option<(i64, PathBuf)> {
    let mut paths = std::fs::read_dir(registry)
        .ok()?
        .filter_map(Result::ok)
        .map(|entry| entry.path())
        .collect::<Vec<_>>();
    paths.sort();
    for path in paths
        .into_iter()
        .filter(|path| {
            path.extension()
                .is_some_and(|extension| extension == "json")
        })
        .take(DESCRIPTOR_LIMIT)
    {
        let Ok(document) = std::fs::read_to_string(path)
            .and_then(|text| serde_json::from_str::<Value>(&text).map_err(std::io::Error::other))
        else {
            continue;
        };
        let Some(object) = document.as_object() else {
            continue;
        };
        if object.get("sessionId").and_then(Value::as_str) != Some(session) {
            continue;
        }
        let (Some(pid), Some(socket)) = (
            object.get("pid").and_then(Value::as_i64),
            object.get("messagingSocketPath").and_then(Value::as_str),
        ) else {
            continue;
        };
        if pid >= 0 && !socket.is_empty() {
            return Some((pid, socket.into()));
        }
    }
    None
}

/// Reads the paired nonempty Claude inbox token without retaining descriptor prose.
fn token(registry: &Path, pid: i64) -> Option<String> {
    let mut paths = std::fs::read_dir(registry)
        .ok()?
        .filter_map(Result::ok)
        .map(|entry| entry.path())
        .collect::<Vec<_>>();
    paths.sort();
    for path in paths.into_iter().filter(|path| {
        path.file_name()
            .is_some_and(|name| name.to_string_lossy().starts_with(&format!("{pid}.")))
            && path.extension().is_some_and(|extension| extension == "key")
    }) {
        let Ok(document) = std::fs::read_to_string(path)
            .and_then(|text| serde_json::from_str::<Value>(&text).map_err(std::io::Error::other))
        else {
            continue;
        };
        if let Some(token) = document
            .get("peerToken")
            .and_then(Value::as_str)
            .filter(|token| !token.is_empty())
        {
            return Some(token.into());
        }
    }
    None
}
