//! Bounded Claude inbox delivery through a session descriptor and Unix socket.

use super::{Evidence, Notice};
use agent_run_domain::worker::WorkerNotice;
use serde_json::{json, Value};
use std::os::fd::AsRawFd;
use std::os::unix::fs::{FileTypeExt, MetadataExt};
use std::path::{Path, PathBuf};
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{UnixListener, UnixStream};

const DESCRIPTOR_LIMIT: usize = 512;
const SESSION_LIMIT: usize = 512;
const MESSAGE_LIMIT: usize = 4096;
/// Bounded wait for the inbox hold-receipt after the frames are written.
const RECEIPT_WAIT: Duration = Duration::from_millis(1500);
/// Receipt frames are one short JSON line; anything longer is not a receipt.
const RECEIPT_FRAME_LIMIT: usize = 4096;
/// Bound on stray connections accepted while waiting for one receipt.
const RECEIPT_CONNECTION_LIMIT: usize = 4;

/// Builds one Claude inbox attempt observation with transport-fixed labels.
///
/// Every field is a static classifier or boolean; no host response payload,
/// filesystem path, session identifier, message text, or token ever enters
/// the persisted evidence. Only a correlated native hold-receipt makes the
/// classifier accepted (see [`send`]).
fn uds_evidence(classifier: &str, accepted: bool, ambiguous: bool) -> Evidence {
    let mut evidence = Evidence::new(classifier, accepted, ambiguous);
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
/// Delivery confirmation uses the inbox's native hold-receipt: the user frame
/// names an ephemeral reply socket owned by this process as `from`, and the
/// inbox — after it queues the message — connects back and writes one
/// `peer_message_status` control frame correlated by `orig_msg_id`. A matched
/// `held` or `delivered` receipt is the only accepted outcome, and it confirms
/// enqueue, never consumption by the recipient's model. A refused receipt, an
/// uncorrelated or silent reply, or an interrupted write stays not delivered
/// (`uds_receipt_refused`, `uds_unconfirmed`, or `uds_ambiguous`) so the
/// dispatcher retries with backoff instead of reporting a false completion.
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

/// An ephemeral reply socket this process bound beside the target inbox.
///
/// The bound inode's kernel identity is recorded at bind time; dropping the
/// guard removes the path only while it is still a socket with that exact
/// device and inode number, so cancellation, panics, and early returns
/// cannot leak the endpoint and can never unlink a file another process
/// renamed over it.
struct OwnedCallback {
    path: PathBuf,
    /// Device number of the bound socket inode.
    device: u64,
    /// Inode number of the bound socket inode.
    inode: u64,
}

impl Drop for OwnedCallback {
    fn drop(&mut self) {
        let still_owned = std::fs::symlink_metadata(&self.path)
            .map(|metadata| {
                metadata.file_type().is_socket()
                    && metadata.dev() == self.device
                    && metadata.ino() == self.inode
            })
            .unwrap_or(false);
        if still_owned {
            let _ = std::fs::remove_file(&self.path);
        }
    }
}

/// Binds one private reply socket in the inbox socket's own directory.
///
/// The native reply-address check accepts any `*.sock` endpoint that shares
/// the inbox socket's resolved directory, so a fresh unguessable name there
/// is a valid `from` target. The directory is used exactly as the inbox
/// socket spells it, without canonicalization: the native check resolves
/// both sides itself, and rewrites such as `/tmp` to `/private/tmp` on macOS
/// must not diverge from the advertised path. Binding uses a fresh name so
/// an existing file fails the bind instead of being touched; the endpoint is
/// advertised only when the bound inode is provably the plain socket this
/// bind created. A path replaced after the bind is left for its new owner,
/// never unlinked. Returns `None` when the directory is missing or not
/// writable, in which case the send proceeds without a reply address and
/// cannot be confirmed.
fn bind_callback(inbox_socket: &Path) -> Option<(OwnedCallback, UnixListener)> {
    let directory = inbox_socket.parent()?;
    for _ in 0..3 {
        // Twelve hex characters keep the full path inside the kernel's Unix
        // socket address limit even under long canonicalized temp paths.
        let name = format!("{}.sock", &uuid::Uuid::new_v4().simple().to_string()[..12]);
        let path = directory.join(name);
        let listener = UnixListener::bind(&path).ok()?;
        match std::fs::symlink_metadata(&path) {
            Ok(metadata) if metadata.file_type().is_socket() => {
                return Some((
                    OwnedCallback {
                        path,
                        device: metadata.dev(),
                        inode: metadata.ino(),
                    },
                    listener,
                ));
            }
            // The path no longer names our bound socket: another process
            // replaced or removed it, and the replacement is not ours to
            // unlink. Dropping the listener releases our inode only.
            _ => drop(listener),
        }
    }
    None
}

/// Reads the kernel-proven `(pid, uid)` identity of a connected socket's peer
/// using the macOS local-socket credential options; `None` when the kernel
/// does not report a peer identity.
#[cfg(target_os = "macos")]
fn peer_identity(stream: &UnixStream) -> Option<(i32, u32)> {
    let fd = stream.as_raw_fd();
    let mut pid: libc::pid_t = 0;
    let mut pid_size = std::mem::size_of::<libc::pid_t>() as libc::socklen_t;
    // SAFETY: getsockopt writes at most one pid_t into the correctly sized
    // buffer and reads nothing else.
    let pid_ok = unsafe {
        libc::getsockopt(
            fd,
            libc::SOL_LOCAL,
            libc::LOCAL_PEERPID,
            (&mut pid as *mut libc::pid_t).cast(),
            &mut pid_size,
        ) == 0
    };
    // SAFETY: an all-zero xucred is a valid unpopulated credential value
    // that getsockopt fills completely below.
    let mut cred: libc::xucred = unsafe { std::mem::zeroed() };
    let mut cred_size = std::mem::size_of::<libc::xucred>() as libc::socklen_t;
    // SAFETY: getsockopt writes at most one xucred into the correctly sized
    // buffer and reads nothing else.
    let cred_ok = unsafe {
        libc::getsockopt(
            fd,
            libc::SOL_LOCAL,
            libc::LOCAL_PEERCRED,
            (&mut cred as *mut libc::xucred).cast(),
            &mut cred_size,
        ) == 0
    };
    (pid_ok && cred_ok && pid > 0).then_some((pid, cred.cr_uid))
}

/// Reads the kernel-proven `(pid, uid)` identity of a connected socket's peer
/// from the portable socket credential API; `None` when unavailable.
#[cfg(not(target_os = "macos"))]
fn peer_identity(stream: &UnixStream) -> Option<(i32, u32)> {
    let cred = stream.peer_cred().ok()?;
    Some((cred.pid(), cred.uid()))
}

/// Returns this process's effective user id, the only uid permitted on
/// either delivery connection.
fn effective_uid() -> u32 {
    // SAFETY: geteuid only reads kernel state and cannot fail.
    unsafe { libc::geteuid() }
}

/// Waits one bounded window for the inbox hold-receipt that matches `msg_id`.
///
/// Only a `control`/`peer_message_status` frame whose `orig_msg_id` equals
/// this attempt's notification id and whose `status` is a native enqueue
/// outcome is honoured, and only when the connecting peer's kernel-proven
/// identity is the descriptor's receiver process running as this user; every
/// other frame, connection, or silence is ignored. Each connection may feed
/// at most `RECEIPT_FRAME_LIMIT + 1` bytes into memory, so a hostile or
/// endless stream is bounded before allocation, not after. Returns the
/// native status word of the first correlated receipt.
async fn await_receipt(
    listener: UnixListener,
    msg_id: &str,
    receiver: (i64, u32),
) -> Option<&'static str> {
    let deadline = tokio::time::Instant::now() + RECEIPT_WAIT;
    for _ in 0..RECEIPT_CONNECTION_LIMIT {
        let wait = deadline.saturating_duration_since(tokio::time::Instant::now());
        if wait.is_zero() {
            return None;
        }
        let (stream, _) = match tokio::time::timeout(wait, listener.accept()).await {
            Ok(Ok(connection)) => connection,
            Ok(Err(_)) | Err(_) => return None,
        };
        // A different process that found the reply socket is not the
        // receiver, even with a perfectly formed receipt.
        if peer_identity(&stream).map(|(pid, uid)| (pid as i64, uid)) != Some(receiver) {
            continue;
        }
        let mut frame = Vec::new();
        let mut bounded = stream.take(RECEIPT_FRAME_LIMIT as u64 + 1);
        let read = tokio::time::timeout_at(deadline, bounded.read_to_end(&mut frame)).await;
        if !matches!(read, Ok(Ok(_))) || frame.len() > RECEIPT_FRAME_LIMIT {
            continue;
        }
        let text = String::from_utf8_lossy(&frame);
        for line in text.lines() {
            if let Some(status) = correlated_status(line, msg_id) {
                return Some(status);
            }
        }
    }
    None
}

/// Validates one received line as this attempt's native hold-receipt.
///
/// The frame must be exactly the inbox's control envelope with a known
/// enqueue status and our own notification id; `from`, `reason`, and any
/// other receiver-authored field are never trusted or retained.
fn correlated_status(line: &str, msg_id: &str) -> Option<&'static str> {
    let frame: Value = serde_json::from_str(line).ok()?;
    let status = frame.get("status")?.as_str()?;
    let correlated = frame.get("type")?.as_str()? == "control"
        && frame.get("action")?.as_str()? == "peer_message_status"
        && frame.get("orig_msg_id")?.as_str()? == msg_id;
    if !correlated {
        return None;
    }
    match status {
        "held" => Some("held"),
        "delivered" => Some("delivered"),
        "denied" => Some("denied"),
        "expired" => Some("expired"),
        "refused" => Some("refused"),
        "dropped" => Some("dropped"),
        _ => None,
    }
}

/// Writes already-rendered trusted framing around bounded report text.
///
/// The user frame repeats three facts the recipient already owns: the
/// resolved `session_id` (the inbox drops a frame whose session no longer
/// matches its socket, which guards against a socket rebound to another
/// session), the stable notification id as `msg_id` so the inbox's receipt
/// and drop telemetry correlate with exactly this notice, and the ephemeral
/// reply socket as `from` so a successful enqueue produces a hold-receipt.
async fn send_text_after(
    registry: &Path,
    session: &str,
    message: Option<(&str, String)>,
    before_write: impl std::future::Future<Output = ()>,
) -> Evidence {
    if !bounded(session, SESSION_LIMIT) {
        return uds_evidence("uds_rejected", false, false);
    }
    let Some((msg_id, text)) = message else {
        return uds_evidence("uds_rejected", false, false);
    };
    if text.len() > MESSAGE_LIMIT || !bounded(msg_id, SESSION_LIMIT) {
        return uds_evidence("uds_rejected", false, false);
    }
    let Some((pid, socket)) = resolve(registry, session) else {
        return uds_evidence("uds_session_gone", false, false);
    };
    let Some(token) = token(registry, pid) else {
        return uds_evidence("uds_rejected", false, false);
    };
    // The reply endpoint is owned for exactly this attempt and removed on any
    // exit path, including cancellation of the returned future.
    let callback = bind_callback(&socket);
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
                return uds_evidence("uds_session_gone", false, false);
            }
            Ok(Err(_)) => return uds_evidence("uds_unavailable", false, false),
            Err(_) => return uds_evidence("uds_session_gone", false, false),
        };
    // Prove the endpoint still belongs to the descriptor's process, as this
    // user, before any token or frame is written: a rebound or foreign
    // endpoint fails closed with nothing sent.
    if peer_identity(&stream).map(|(pid, uid)| (pid as i64, uid)) != Some((pid, effective_uid())) {
        return uds_evidence("uds_unavailable", false, false);
    }
    before_write.await;
    let from = callback
        .as_ref()
        .map(|(owned, _)| format!("uds:{}", owned.path.display()));
    let frames = format!(
        "{}\n{}\n",
        json!({"type":"auth","token":token}),
        json!({"type":"user","session_id":session,"msg_id":msg_id,"from":from,"message":{"role":"user","content":text}}),
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
            return uds_evidence("uds_ambiguous", false, true);
        }
        Ok(Err(_)) | Err(_) => return uds_evidence("uds_unavailable", false, false),
    }
    // The frames are already kernel-buffered; the half-close hands the
    // allowHalfOpen inbox its end-of-frames marker without waiting on it.
    let _ = stream.shutdown().await;
    drop(stream);
    let Some((_owned, listener)) = callback else {
        // Without a reply endpoint the inbox cannot report anything back, so
        // the attempt is explicitly unconfirmed rather than delivered.
        return uds_evidence("uds_unconfirmed", false, true);
    };
    match await_receipt(listener, msg_id, (pid, effective_uid())).await {
        Some("held") => uds_evidence("uds_receipt_held", true, false),
        Some("delivered") => uds_evidence("uds_receipt_delivered", true, false),
        // A native refusal is a definite non-acceptance, not an ambiguity.
        Some(_) => uds_evidence("uds_receipt_refused", false, false),
        None => uds_evidence("uds_unconfirmed", false, true),
    }
    // `_owned` drops here and removes exactly the reply socket this attempt
    // created, on success, refusal, silence, error, or cancellation alike.
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

#[cfg(test)]
mod tests {
    use super::bind_callback;
    use std::os::unix::fs::FileTypeExt;

    /// The reply endpoint removes only the exact inode it bound: a path
    /// replaced by a regular file or by a different socket survives the
    /// guard, while the owned endpoint itself is always removed.
    #[tokio::test]
    async fn callback_removal_requires_the_exact_owned_inode() {
        let directory = tempfile::tempdir().unwrap();
        let inbox = directory.path().join("inbox.sock");

        // A replacement regular file is never unlinked.
        let (owned, listener) = bind_callback(&inbox).unwrap();
        let replaced = owned.path.clone();
        std::fs::remove_file(&replaced).unwrap();
        std::fs::write(&replaced, b"replacement").unwrap();
        drop(listener);
        drop(owned);
        assert!(replaced.is_file(), "a replacement file must survive");

        // A replacement socket bound by someone else is never unlinked.
        let (owned, listener) = bind_callback(&inbox).unwrap();
        let rebound = owned.path.clone();
        std::fs::remove_file(&rebound).unwrap();
        let other = tokio::net::UnixListener::bind(&rebound).unwrap();
        drop(listener);
        drop(owned);
        assert!(
            std::fs::symlink_metadata(&rebound)
                .map(|metadata| metadata.file_type().is_socket())
                .unwrap_or(false),
            "a foreign replacement socket must survive"
        );
        drop(other);
        let _ = std::fs::remove_file(&rebound);

        // The endpoint this process owns is removed.
        let (owned, listener) = bind_callback(&inbox).unwrap();
        let owned_path = owned.path.clone();
        drop(listener);
        drop(owned);
        assert!(!owned_path.exists(), "the owned endpoint must be removed");
    }
}
