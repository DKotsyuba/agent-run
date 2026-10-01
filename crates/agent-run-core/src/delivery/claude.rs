//! Bounded Claude inbox delivery through a session descriptor and Unix socket.

use super::{Evidence, Notice};
use agent_run_domain::worker::WorkerNotice;
use serde_json::{json, Value};
use std::os::unix::fs::FileTypeExt;
use std::path::{Path, PathBuf};
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::UnixStream;

const DESCRIPTOR_LIMIT: usize = 512;
const SESSION_LIMIT: usize = 512;
const MESSAGE_LIMIT: usize = 4096;
/// Bounded wait for an inbox acknowledgement after the frames are written.
const CONFIRM_WAIT: Duration = Duration::from_millis(250);

/// Builds one Claude inbox attempt observation with transport-fixed labels.
///
/// Every field is a static classifier or boolean; no host response payload,
/// filesystem path, session identifier, message text, or token ever enters
/// the persisted evidence. The classifier is never accepted, because a write
/// to this inbox cannot confirm enqueue (see [`send`]).
fn uds_evidence(classifier: &str, ambiguous: bool) -> Evidence {
    let mut evidence = Evidence::new(classifier, false, ambiguous);
    evidence.executable = "claude-uds".into();
    evidence.argv_shape = vec!["claude-uds".into()];
    evidence
}

/// Delivers one rendered completion notice through a specified Claude registry.
///
/// The registry and socket path are supplied by the caller so tests and embedded
/// hosts can use private temporary directories. A missing descriptor, dead
/// socket, or deleted endpoint is classified as a definite session loss, while
/// an endpoint that is not a socket (such as a directory) is classified as an
/// unavailable endpoint because Linux reports both as `ECONNREFUSED`.
///
/// The inbox is fire-and-forget for senders that are not themselves published
/// Claude sessions: it accepts written frames without ever acknowledging them
/// on the connection, so a completed write proves only that the kernel accepted
/// the bytes. A clean write is therefore reported as `uds_unconfirmed`
/// (ambiguous, not delivered) and the dispatcher retries a bounded number of
/// times before failing explicitly; a write interrupted by a peer abort stays
/// `uds_ambiguous` for the same reason.
pub async fn send(registry: &Path, session: &str, notice: &Notice) -> Evidence {
    send_after(registry, session, notice, async {}).await
}

/// Delivers a bounded worker report through the same Claude inbox route.
pub async fn send_worker(registry: &Path, session: &str, notice: &WorkerNotice) -> Evidence {
    send_text_after(
        registry,
        session,
        notice
            .render()
            .ok()
            .map(|text| (notice.notification_id.as_str(), text)),
        async {},
    )
    .await
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
    send_text_after(
        registry,
        session,
        notice
            .render()
            .ok()
            .map(|text| (notice.notification_id.as_str(), text)),
        before_write,
    )
    .await
}

/// Writes already-rendered trusted framing around bounded report text.
///
/// The user frame repeats two facts the recipient already owns: the resolved
/// `session_id` (the inbox drops a frame whose session no longer matches its
/// socket, which guards against a socket rebound to another session) and the
/// stable notification id as `msg_id` so retries and any receiver-side drop
/// telemetry correlate with one notice.
async fn send_text_after(
    registry: &Path,
    session: &str,
    message: Option<(&str, String)>,
    before_write: impl std::future::Future<Output = ()>,
) -> Evidence {
    if !bounded(session, SESSION_LIMIT) {
        return uds_evidence("uds_rejected", false);
    }
    let Some((msg_id, text)) = message else {
        return uds_evidence("uds_rejected", false);
    };
    if text.len() > MESSAGE_LIMIT || !bounded(msg_id, SESSION_LIMIT) {
        return uds_evidence("uds_rejected", false);
    }
    let Some((pid, socket)) = resolve(registry, session) else {
        return uds_evidence("uds_session_gone", false);
    };
    let Some(token) = token(registry, pid) else {
        return uds_evidence("uds_rejected", false);
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
                return uds_evidence("uds_session_gone", false);
            }
            Ok(Err(_)) => return uds_evidence("uds_unavailable", false),
            Err(_) => return uds_evidence("uds_session_gone", false),
        };
    before_write.await;
    let frames = format!(
        "{}\n{}\n",
        json!({"type":"auth","token":token}),
        json!({"type":"user","session_id":session,"msg_id":msg_id,"message":{"role":"user","content":text}}),
    );
    match tokio::time::timeout(Duration::from_secs(5), stream.write_all(frames.as_bytes())).await {
        Ok(Ok(())) => {}
        Ok(Err(error))
            if matches!(
                error.kind(),
                std::io::ErrorKind::BrokenPipe
                    | std::io::ErrorKind::ConnectionReset
                    | std::io::ErrorKind::TimedOut
            ) =>
        {
            return uds_evidence("uds_ambiguous", true);
        }
        Ok(Err(_)) | Err(_) => return uds_evidence("uds_unavailable", false),
    }
    // The frames are already kernel-buffered, so dropping the socket now
    // would not lose them; the half-close instead hands the allowHalfOpen
    // inbox its end-of-frames marker, and one bounded read observes a
    // peer-side abort (reset) if the inbox destroys the connection now.
    let _ = stream.shutdown().await;
    let mut sink = [0u8; 1024];
    match tokio::time::timeout(CONFIRM_WAIT, stream.read(&mut sink)).await {
        Ok(Err(error))
            if matches!(
                error.kind(),
                std::io::ErrorKind::ConnectionReset
                    | std::io::ErrorKind::BrokenPipe
                    | std::io::ErrorKind::ConnectionAborted
                    | std::io::ErrorKind::TimedOut
            ) =>
        {
            uds_evidence("uds_ambiguous", true)
        }
        // Clean EOF, silence within the bound, or unsolicited bytes: none of
        // these confirm enqueue, and unsolicited bytes are never trusted as
        // a receipt, so the attempt stays explicitly unconfirmed.
        _ => uds_evidence("uds_unconfirmed", true),
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
