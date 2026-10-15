//! Protected delivery handoff to an existing native owner; this module never writes engine stdin.
use agent_run_domain::{Result, error::invalid};
use agent_run_platform::{fs as private_fs, process};
use serde::{Deserialize, Serialize};
use std::{
    collections::BTreeMap,
    path::{Path, PathBuf},
    time::Duration,
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{UnixListener, UnixStream},
    sync::{mpsc, oneshot},
    task::JoinHandle,
    time::{Instant, timeout_at},
};

/// Maximum encoded request or response, including JSON escaping.
pub const FRAME_BYTES: usize = 1024 * 1024;
/// Maximum decoded manifest; the sender has already frozen its external-data framing.
pub const TEXT_BYTES: usize = 98304;
/// One original owner budget, including peer checks, frame, handoff and native acceptance.
pub const OWNER_BUDGET: Duration = Duration::from_secs(30);
/// Private launch settings, deliberately separate from producer credentials and request authority.
pub const SETTINGS: [&str; 2] = [
    "BRIDGE_NATIVE_OWNER_CHANNEL",
    "BRIDGE_NATIVE_OWNER_DIRECTORY",
];
/// Removed static build settings are stripped from every native child environment.
pub const RETIRED_SETTINGS: [&str; 2] = [
    "BRIDGE_NATIVE_OWNER_DAEMON",
    "BRIDGE_NATIVE_OWNER_DAEMON_SHA256",
];
/// The actual native harness advertises only this locator; possession grants no authority.
pub const LOCATOR: &str = "BRIDGE_NATIVE_OWNER_SOCKET";

/// Exact agreed six-field request; native association must be checked by the owner loop.
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Request {
    /// Already-attested native session, never a grant or a request to create a job.
    pub thread_id: String,
    /// Existing stable Bridge native-attempt identity, retained across ACK loss.
    pub call_id: String,
    /// Exact frozen manifest, at most TEXT_BYTES decoded UTF-8 bytes.
    pub text: String,
    /// Actual owned native child PID.
    pub native_pid: u32,
    /// Bridge sysinfo start time in Unix seconds; full internal ownership tokens remain stronger.
    pub native_start_time: u64,
    /// Must be true: active-turn steering is not part of this boundary.
    pub wake_only: bool,
}
/// A control-free nonempty protocol identifier of at most 256 UTF-8 bytes.
pub fn valid_id(value: &str) -> bool {
    !value.is_empty() && value.len() <= 256 && !value.chars().any(char::is_control)
}
impl Request {
    /// Reject malformed identity, active-steer requests and oversized decoded text before any effect.
    pub fn valid(&self) -> bool {
        valid_id(&self.thread_id)
            && valid_id(&self.call_id)
            && self.native_pid > 1
            && self.native_start_time > 0
            && self.wake_only
            && !self.text.trim().is_empty()
            && self.text.len() <= TEXT_BYTES
    }
    /// Hash the typed request, so member order or equivalent JSON escaping cannot bypass call-id fencing.
    fn digest(&self) -> String {
        private_fs::sha256(&serde_json::to_vec(self).expect("typed request serializes"))
    }
}
/// Exact five-field receipt; enqueue and pipe write never construct accepted receipts.
#[derive(Clone, Debug, Serialize)]
pub struct Reply {
    /// Original attempt identity; a mismatch never proves acceptance.
    pub call_id: String,
    /// accepted, known_unsent or unknown.
    pub state: &'static str,
    /// codex_turn, claude_input or opencode_input only for genuine native acknowledgement.
    pub protocol: Option<&'static str>,
    /// Fresh Codex turn ID, replayed Claude UUID or durably admitted OpenCode input ID.
    pub native_id: Option<String>,
    /// Finite refusal/uncertainty code; never arbitrary diagnostic text or payload.
    pub reason: Option<&'static str>,
}
impl Reply {
    /// Report a positive pre-effect refusal using one finite contract reason.
    pub fn unsent(id: &str, reason: &'static str) -> Self {
        Self {
            call_id: id.into(),
            state: "known_unsent",
            protocol: None,
            native_id: None,
            reason: Some(reason),
        }
    }
    /// Preserve possible effects when an acknowledgement is absent, invalid or lost.
    pub fn unknown(id: &str) -> Self {
        Self {
            call_id: id.into(),
            state: "unknown",
            protocol: None,
            native_id: None,
            reason: Some("native_uncertain"),
        }
    }
    /// Construct acceptance only from the corresponding actually observed native identifier.
    pub fn accepted(id: &str, protocol: &'static str, native: &str) -> Self {
        Self {
            call_id: id.into(),
            state: "accepted",
            protocol: Some(protocol),
            native_id: Some(native.into()),
            reason: None,
        }
    }
}
/// Non-evicting owner-incarnation control state; no delivery queue or durable replay path.
#[derive(Default)]
pub struct Calls {
    /// Frozen typed hashes and original receipts, reserved unknown before a possible native write.
    entries: BTreeMap<String, (String, Reply)>,
}
impl Calls {
    /// Reserve a new call before effect; identical retries return its retained receipt, conflict/capacity refuse.
    pub fn reserve(&mut self, request: &Request) -> std::result::Result<(), Reply> {
        let hash = request.digest();
        if let Some((prior, reply)) = self.entries.get(&request.call_id) {
            return Err(if *prior == hash {
                reply.clone()
            } else {
                Reply::unsent(&request.call_id, "conflict")
            });
        }
        if self.entries.len() >= 1024 {
            return Err(Reply::unsent(&request.call_id, "capacity"));
        }
        self.entries.insert(
            request.call_id.clone(),
            (hash, Reply::unknown(&request.call_id)),
        );
        Ok(())
    }
    /// Retain a final original receipt; never evict or change the frozen request identity.
    pub fn finish(&mut self, reply: Reply) {
        if let Some((_, prior)) = self.entries.get_mut(&reply.call_id) {
            *prior = reply;
        }
    }
}
/// One validated transport handoff; only the existing native loop may act on it.
pub struct Call {
    /// Strict parsed request authenticated through the protected daemon channel.
    pub request: Request,
    /// Original absolute owner deadline; it is never renewed at native I/O.
    pub deadline: Instant,
    /// One response slot; dropping it cannot authorize a repeat native effect.
    reply: oneshot::Sender<Reply>,
}
impl Call {
    /// Complete the original response slot; a missing reader only means ACK loss, never a new request.
    pub fn answer(self, reply: Reply) {
        let _ = self.reply.send(reply);
    }
}
/// Protected channel locator; each connection proves the current live same-user kernel peer.
#[derive(Clone)]
struct Trust {
    /// Protected semantic adapter channel, distinct from producer registration/core RPC.
    channel: PathBuf,
}
/// Reject symlink traversal and require an existing absolute path of the expected private type.
fn protected(path: &Path, socket: bool) -> Result<()> {
    use std::os::unix::fs::{FileTypeExt, MetadataExt};
    if !path.is_absolute()
        || path.components().any(|c| {
            matches!(
                c,
                std::path::Component::ParentDir | std::path::Component::CurDir
            )
        })
    {
        return Err(invalid("native owner path must be absolute"));
    }
    let mut prefix = PathBuf::new();
    for part in path.components() {
        prefix.push(part.as_os_str());
        if std::fs::symlink_metadata(&prefix)?.file_type().is_symlink() {
            return Err(invalid("native owner symlink refused"));
        }
    }
    let meta = std::fs::symlink_metadata(path)?;
    // SAFETY: geteuid has no arguments or memory preconditions.
    if meta.uid() != unsafe { libc::geteuid() }
        || meta.mode() & 0o022 != 0
        || (socket && (!meta.file_type().is_socket() || meta.mode() & 0o777 != 0o600))
    {
        return Err(invalid("native owner endpoint protection refused"));
    }
    if socket {
        let parent = std::fs::symlink_metadata(
            path.parent()
                .ok_or_else(|| invalid("native owner parent missing"))?,
        )?;
        if !parent.is_dir() || parent.uid() != meta.uid() || parent.mode() & 0o777 != 0o700 {
            return Err(invalid("native owner channel parent not private"));
        }
    }
    Ok(())
}
/// Capture one live same-user kernel peer with a full PID-reuse fence.
fn peer(stream: &UnixStream) -> Result<process::Identity> {
    let credentials = stream.peer_cred()?;
    // SAFETY: geteuid has no arguments or memory preconditions.
    if credentials.uid() != unsafe { libc::geteuid() } {
        return Err(invalid("native owner foreign UID"));
    }
    let pid = credentials
        .pid()
        .filter(|id| *id > 1)
        .ok_or_else(|| invalid("native owner peer PID missing"))?;
    let identity = process::inspect(pid)?;
    if identity.zombie {
        return Err(invalid("native owner peer exited"));
    }
    Ok(identity)
}
impl Trust {
    /// Authenticate against the current protected channel peer, not an obsolete launch-time PID.
    async fn verify(&self, stream: &UnixStream, deadline: Instant) -> Result<()> {
        protected(&self.channel, true)?;
        let actual = peer(stream)?;
        let channel = timeout_at(deadline, UnixStream::connect(&self.channel))
            .await
            .map_err(|_| invalid("native owner channel deadline"))??;
        let current = peer(&channel)?;
        if actual.pid != current.pid
            || actual.token != current.token
            || actual.birth != current.birth
        {
            return Err(invalid("native owner daemon identity refused"));
        }
        if process::observe(Some(current.pid), Some(&current.token), Some(current.birth))
            != process::ProcessState::Alive
        {
            return Err(invalid("native owner daemon exited"));
        }
        Ok(())
    }
}
/// One protected listener and bounded handoff lane, owned only for this native launch.
pub struct Owner {
    /// Requests wait only here, without a second engine writer.
    incoming: mpsc::Receiver<Call>,
    /// Sole transport reader; never a native stdin writer.
    transport: JoinHandle<()>,
    /// Only this newly-created owned directory may be removed on retirement.
    directory: PathBuf,
    /// Bound private socket published to the actual child environment.
    socket: PathBuf,
    /// Original device/inode identities; a replaced path is never removed at retirement.
    identity: ((u64, u64), (u64, u64)),
}
impl Owner {
    /// Prepare an opt-in endpoint before native spawn, adding its actual locator only to the child environment.
    /// Retired build qualifiers are removed. Missing channel/directory settings clear a forged locator;
    /// partial or unsafe settings fail before a child exists. No executable path or digest admits a caller.
    pub fn prepare(environment: &mut BTreeMap<String, String>) -> Result<Option<Self>> {
        for key in RETIRED_SETTINGS {
            environment.remove(key);
        }
        if !SETTINGS.iter().any(|key| environment.contains_key(*key)) {
            environment.remove(LOCATOR);
            return Ok(None);
        }
        let value = |name: &str| {
            environment
                .get(name)
                .cloned()
                .filter(|s| !s.is_empty())
                .ok_or_else(|| invalid("native owner launch setting incomplete"))
        };
        let trust = Trust {
            channel: PathBuf::from(value(SETTINGS[0])?),
        };
        protected(&trust.channel, true)?;
        let root = PathBuf::from(value(SETTINGS[1])?);
        protected(&root, false)?;
        use std::os::unix::fs::{DirBuilderExt, MetadataExt, PermissionsExt};
        let meta = std::fs::symlink_metadata(&root)?;
        if !meta.is_dir() || meta.mode() & 0o777 != 0o700 {
            return Err(invalid("native owner directory not private"));
        }
        let directory = root.join(uuid::Uuid::new_v4().simple().to_string());
        std::fs::DirBuilder::new().mode(0o700).create(&directory)?;
        let socket = directory.join("o.sock");
        let listener = match UnixListener::bind(&socket) {
            Ok(l) => l,
            Err(e) => {
                let _ = std::fs::remove_dir(&directory);
                return Err(e.into());
            }
        };
        if let Err(error) =
            std::fs::set_permissions(&socket, std::fs::Permissions::from_mode(0o600))
        {
            drop(listener);
            let _ = std::fs::remove_file(&socket);
            let _ = std::fs::remove_dir(&directory);
            return Err(error.into());
        }
        let d = std::fs::symlink_metadata(&directory)?;
        let s = std::fs::symlink_metadata(&socket)?;
        let identity = ((d.dev(), d.ino()), (s.dev(), s.ino()));
        let (send, incoming) = mpsc::channel(1);
        let transport = tokio::spawn(async move {
            while let Ok((mut stream, _)) = listener.accept().await {
                let deadline = Instant::now() + OWNER_BUDGET;
                let request = async {
                    trust.verify(&stream, deadline).await?;
                    let length = stream.read_u32_le().await? as usize;
                    if length == 0 || length > FRAME_BYTES {
                        return Err(invalid("native owner frame length refused"));
                    }
                    let mut bytes = vec![0; length];
                    stream.read_exact(&mut bytes).await?;
                    let request: Request = serde_json::from_slice(&bytes)?;
                    if !request.valid() {
                        return Err(invalid("native owner request refused"));
                    }
                    trust.verify(&stream, deadline).await?;
                    Ok::<_, agent_run_domain::Error>(request)
                };
                let Ok(Ok(request)) = timeout_at(deadline, request).await else {
                    continue;
                };
                let id = request.call_id.clone();
                let (reply, receive) = oneshot::channel();
                let call = Call {
                    request,
                    deadline,
                    reply,
                };
                let result = if !matches!(timeout_at(deadline, send.send(call)).await, Ok(Ok(()))) {
                    Reply::unsent(&id, "unavailable")
                } else {
                    match timeout_at(deadline, receive).await {
                        Ok(Ok(reply)) => reply,
                        _ => Reply::unknown(&id),
                    }
                };
                if let Ok(bytes) = serde_json::to_vec(&result) {
                    let _ = timeout_at(deadline, async {
                        stream.write_u32_le(bytes.len() as u32).await?;
                        stream.write_all(&bytes).await?;
                        stream.flush().await
                    })
                    .await;
                }
            }
        });
        environment.insert(LOCATOR.into(), socket.to_string_lossy().into_owned());
        Ok(Some(Self {
            incoming,
            transport,
            directory,
            socket,
            identity,
        }))
    }
    /// Receive one native handoff; absence or retirement stays pending without busy polling.
    pub async fn receive(owner: &mut Option<Self>) -> Call {
        if let Some(owner) = owner
            && let Some(call) = owner.incoming.recv().await
        {
            return call;
        }
        std::future::pending().await
    }
}
impl Drop for Owner {
    /// Retire listener before removing only its private socket and empty owned directory.
    fn drop(&mut self) {
        use std::os::unix::fs::MetadataExt;
        self.transport.abort();
        if std::fs::symlink_metadata(&self.directory)
            .is_ok_and(|m| (m.dev(), m.ino()) == self.identity.0)
        {
            if std::fs::symlink_metadata(&self.socket)
                .is_ok_and(|m| (m.dev(), m.ino()) == self.identity.1)
            {
                let _ = std::fs::remove_file(&self.socket);
            }
            let _ = std::fs::remove_dir(&self.directory);
        }
    }
}

#[cfg(test)]
mod tests {
    //! Pure one-effect, identity and finite capacity controls; real engines/stores are never used.
    use super::*;
    /// One exact typed owner request with fixed synthetic process identity.
    fn request() -> Request {
        Request {
            thread_id: "session".into(),
            call_id: "call".into(),
            text: "frozen manifest".into(),
            native_pid: 42,
            native_start_time: 10,
            wake_only: true,
        }
    }
    /// Lost ACK retains original acceptance and changed payload cannot repeat an effect.
    #[test]
    fn one_effect_ack_loss_conflict_and_unknown() {
        let r = request();
        let mut calls = Calls::default();
        let mut effects = 0;
        assert!(calls.reserve(&r).is_ok());
        effects += 1;
        calls.finish(Reply::accepted(&r.call_id, "codex_turn", "native-fresh"));
        let ack = calls.reserve(&r).unwrap_err();
        assert_eq!(ack.native_id.as_deref(), Some("native-fresh"));
        assert_eq!(effects, 1);
        let mut changed = r.clone();
        changed.text.push('!');
        assert_eq!(
            calls.reserve(&changed).unwrap_err().reason,
            Some("conflict")
        );
        let mut unknown = r.clone();
        unknown.call_id = "uncertain".into();
        assert!(calls.reserve(&unknown).is_ok());
        assert_eq!(calls.reserve(&unknown).unwrap_err().state, "unknown");
        assert_eq!(effects, 1);
    }
    /// Bounded state never evicts an old receipt to admit another effect.
    #[test]
    fn capacity_does_not_reopen_old_call() {
        let mut calls = Calls::default();
        for i in 0..1024 {
            let mut r = request();
            r.call_id = format!("c{i}");
            assert!(calls.reserve(&r).is_ok());
        }
        assert_eq!(
            calls.reserve(&request()).unwrap_err().reason,
            Some("capacity")
        );
        let mut old = request();
        old.call_id = "c0".into();
        assert_eq!(calls.reserve(&old).unwrap_err().state, "unknown");
    }
    /// Closed syntax and decoded bounds refuse unsupported or duplicate fields before effect.
    #[test]
    fn wire_is_closed_and_bounded() {
        let r = request();
        assert!(r.valid());
        let mut bad = r.clone();
        bad.wake_only = false;
        assert!(!bad.valid());
        bad = r.clone();
        bad.text = "x".repeat(TEXT_BYTES + 1);
        assert!(!bad.valid());
        let mut value = serde_json::to_value(&r).unwrap();
        value["version"] = 1.into();
        assert!(serde_json::from_value::<Request>(value).is_err());
        let duplicate = r#"{"thread_id":"session","thread_id":"other","call_id":"c","text":"manifest","native_pid":42,"native_start_time":10,"wake_only":true}"#;
        assert!(serde_json::from_str::<Request>(duplicate).is_err());
    }
    /// Private real-kernel channel fixture; its accept loop owns no model or engine input.
    struct Fixture {
        /// All paths remain in the configured project-specific TMPDIR.
        root: tempfile::TempDir,
        /// Current same-user protected-channel owner; shutdown aborts this test-only listener.
        channel: JoinHandle<()>,
        /// Launch settings locate the protected live channel without an executable allowlist.
        environment: BTreeMap<String, String>,
    }
    impl Fixture {
        /// Create a protected channel with the test process as the actual current daemon peer.
        fn new() -> Self {
            use std::os::unix::fs::PermissionsExt;
            let root = tempfile::tempdir().unwrap();
            std::fs::set_permissions(root.path(), std::fs::Permissions::from_mode(0o700)).unwrap();
            let path = root.path().join("c.sock");
            let listener = UnixListener::bind(&path).unwrap();
            std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600)).unwrap();
            let channel = tokio::spawn(async move {
                while let Ok((mut stream, _)) = listener.accept().await {
                    let _ = stream.read(&mut [0]).await;
                }
            });
            let environment = BTreeMap::from([
                (SETTINGS[0].into(), path.to_str().unwrap().into()),
                (SETTINGS[1].into(), root.path().to_str().unwrap().into()),
            ]);
            Self {
                root,
                channel,
                environment,
            }
        }
        /// Bind an owner before any test native actor exists, with a kernel-protected actual locator.
        fn owner(&mut self) -> Owner {
            Owner::prepare(&mut self.environment).unwrap().unwrap()
        }
    }
    impl Drop for Fixture {
        /// Stop the test's current protected channel before its private root is cleaned.
        fn drop(&mut self) {
            self.channel.abort();
        }
    }
    /// Send one actual closed frame and read one finite receipt; caller tests decide its semantic proof.
    async fn exchange(socket: &Path, value: &Request) -> serde_json::Value {
        let mut stream = UnixStream::connect(socket).await.unwrap();
        let bytes = serde_json::to_vec(value).unwrap();
        stream.write_u32_le(bytes.len() as u32).await.unwrap();
        stream.write_all(&bytes).await.unwrap();
        let size = stream.read_u32_le().await.unwrap() as usize;
        assert!(size <= FRAME_BYTES);
        let mut out = vec![0; size];
        stream.read_exact(&mut out).await.unwrap();
        serde_json::from_slice(&out).unwrap()
    }
    /// Real framed handoff reaches only the existing owner lane; lost readers cannot repeat a reserved effect.
    #[tokio::test]
    async fn kernel_handoff_and_ack_loss_keep_one_effect() {
        use std::os::unix::fs::MetadataExt;
        let mut f = Fixture::new();
        let mut owner = Some(f.owner());
        let socket = owner.as_ref().unwrap().socket.clone();
        assert_eq!(std::fs::metadata(&socket).unwrap().mode() & 0o777, 0o600);
        let r = request();
        let mut stream = UnixStream::connect(&socket).await.unwrap();
        let bytes = serde_json::to_vec(&r).unwrap();
        stream.write_u32_le(bytes.len() as u32).await.unwrap();
        stream.write_all(&bytes).await.unwrap();
        let first = tokio::time::timeout(Duration::from_secs(5), Owner::receive(&mut owner))
            .await
            .unwrap();
        let mut calls = Calls::default();
        assert!(calls.reserve(&first.request).is_ok());
        drop(stream);
        calls.finish(Reply::accepted(
            &r.call_id,
            "codex_turn",
            "test-native-receipt",
        ));
        first.answer(Reply::accepted(
            &r.call_id,
            "codex_turn",
            "test-native-receipt",
        ));
        let retry = exchange(&socket, &r);
        let receive = Owner::receive(&mut owner);
        let (reply, ()) = tokio::join!(retry, async {
            let call = receive.await;
            let prior = calls.reserve(&call.request).unwrap_err();
            call.answer(prior);
        });
        assert_eq!(reply["native_id"], "test-native-receipt");
        assert_eq!(calls.entries.len(), 1);
        drop(owner);
        assert!(!socket.exists());
    }
    /// HTTP-lease launches prepare the same private owner endpoint without changing stdout or stdin lifetime semantics.
    #[tokio::test]
    async fn lease_launch_keeps_native_owner_endpoint() {
        use crate::{
            LaunchPlan,
            io::{Event, Process},
        };
        let fixture = Fixture::new();
        let mut process = Process::spawn_lease(&LaunchPlan {
            binary: "/bin/sh".into(),
            args: vec![
                "-c".into(),
                "printf '{\"socket\":\"%s\"}\\n' \"$BRIDGE_NATIVE_OWNER_SOCKET\"; read lease"
                    .into(),
            ],
            cwd: fixture.root.path().to_owned(),
            environment: fixture.environment.clone(),
            initial_input: None,
        })
        .unwrap();
        let expected = process.native_owner.as_ref().unwrap().socket.clone();
        let Event::Json(ready) = process.next().await else {
            panic!("lease readiness missing");
        };
        assert_eq!(ready["socket"].as_str(), expected.to_str());
        assert!(process.input.is_some());
        assert!(
            process
                .owner
                .cleanup(Duration::from_millis(50))
                .await
                .unwrap()
                .confirmed
        );
        process.reap().await;
        drop(process);
        assert!(!expected.exists());
    }
    /// Same executable/UID with another actual PID is not the current daemon and cannot enter the owner lane.
    #[tokio::test]
    async fn kernel_rejects_other_process_even_same_build() {
        let mut f = Fixture::new();
        let mut owner = Some(f.owner());
        let socket = owner.as_ref().unwrap().socket.clone();
        let child = std::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "native_owner::tests::untrusted_peer_helper",
                "--nocapture",
            ])
            .env("AGENT_RUN_OWNER_UNTRUSTED_SOCKET", &socket)
            .spawn()
            .unwrap();
        let status = tokio::task::spawn_blocking(move || {
            let mut child = child;
            child.wait().unwrap()
        })
        .await
        .unwrap();
        assert!(status.success());
        assert!(
            tokio::time::timeout(Duration::from_millis(50), Owner::receive(&mut owner))
                .await
                .is_err()
        );
    }
    /// Subprocess-only untrusted connection, invoked by the negative kernel test; ordinary test runs do nothing.
    #[test]
    fn untrusted_peer_helper() {
        use std::io::{Read, Write};
        let Some(path) = std::env::var_os("AGENT_RUN_OWNER_UNTRUSTED_SOCKET") else {
            return;
        };
        let mut stream = std::os::unix::net::UnixStream::connect(path).unwrap();
        stream
            .set_read_timeout(Some(Duration::from_secs(5)))
            .unwrap();
        let bytes = serde_json::to_vec(&request()).unwrap();
        let _ = stream.write_all(&(bytes.len() as u32).to_le_bytes());
        let _ = stream.write_all(&bytes);
        let mut out = [0];
        assert!(matches!(stream.read(&mut out), Ok(0) | Err(_)));
    }
    /// Partial settings, public parent and replaced endpoint never gain an owner effect.
    #[tokio::test]
    async fn unsafe_settings_and_replaced_socket_fail_closed() {
        use std::os::unix::fs::PermissionsExt;
        let mut f = Fixture::new();
        let original = f.environment.clone();
        f.environment.remove(SETTINGS[1]);
        assert!(Owner::prepare(&mut f.environment).is_err());
        f.environment = original;
        std::fs::set_permissions(f.root.path(), std::fs::Permissions::from_mode(0o755)).unwrap();
        assert!(Owner::prepare(&mut f.environment).is_err());
        std::fs::set_permissions(f.root.path(), std::fs::Permissions::from_mode(0o700)).unwrap();
        let owner = f.owner();
        let socket = owner.socket.clone();
        std::fs::rename(&socket, socket.with_extension("old")).unwrap();
        std::fs::write(&socket, b"foreign replacement").unwrap();
        drop(owner);
        assert_eq!(std::fs::read(&socket).unwrap(), b"foreign replacement");
        let mut env = BTreeMap::from([(LOCATOR.into(), "forged-locator".into())]);
        assert!(Owner::prepare(&mut env).unwrap().is_none());
        assert!(!env.contains_key(LOCATOR));
        for key in RETIRED_SETTINGS {
            env.insert(key.into(), "retired-setting".into());
        }
        assert!(Owner::prepare(&mut env).unwrap().is_none());
        assert!(RETIRED_SETTINGS.iter().all(|key| !env.contains_key(*key)));
    }
    /// Closed/malformed/oversized frames never hand off and do not poison the next valid connection.
    #[tokio::test]
    async fn invalid_frames_have_zero_handoff() {
        let mut f = Fixture::new();
        let mut owner = Some(f.owner());
        let socket = owner.as_ref().unwrap().socket.clone();
        for bytes in [Vec::new(),b"{}".to_vec(),b"{\"thread_id\":\"s\",\"call_id\":\"c\",\"text\":\"x\",\"native_pid\":42,\"native_start_time\":10,\"wake_only\":true,\"version\":1}".to_vec()] {
            let mut stream=UnixStream::connect(&socket).await.unwrap();stream.write_u32_le(bytes.len() as u32).await.unwrap();stream.write_all(&bytes).await.unwrap();let mut out=[0];assert!(matches!(tokio::time::timeout(Duration::from_secs(5),stream.read(&mut out)).await,Ok(Ok(0))|Ok(Err(_))));
        }
        let mut stream = UnixStream::connect(&socket).await.unwrap();
        stream.write_u32_le((FRAME_BYTES + 1) as u32).await.unwrap();
        let mut out = [0];
        assert!(matches!(
            tokio::time::timeout(Duration::from_secs(5), stream.read(&mut out)).await,
            Ok(Ok(0)) | Ok(Err(_))
        ));
        assert!(
            tokio::time::timeout(Duration::from_millis(50), Owner::receive(&mut owner))
                .await
                .is_err()
        );
        let r = request();
        let (reply, ()) = tokio::join!(exchange(&socket, &r), async {
            let call = Owner::receive(&mut owner).await;
            let reply = Reply::unsent(&call.request.call_id, "not_idle");
            call.answer(reply);
        });
        assert_eq!(reply["state"], "known_unsent");
    }

    /// Trickle bytes cannot renew the original accept deadline; an expired partial frame never enters the native lane.
    #[tokio::test]
    async fn partial_frame_uses_original_thirty_second_deadline() {
        let mut fixture = Fixture::new();
        let mut owner = Some(fixture.owner());
        let socket = owner.as_ref().unwrap().socket.clone();
        let mut stream = UnixStream::connect(&socket).await.unwrap();
        let start = Instant::now();
        stream.write_u32_le(100).await.unwrap();
        for _ in 0..3 {
            tokio::time::sleep(Duration::from_secs(7)).await;
            stream.write_all(b" ").await.unwrap();
        }
        let mut byte = [0];
        assert!(matches!(
            tokio::time::timeout(Duration::from_secs(13), stream.read(&mut byte)).await,
            Ok(Ok(0)) | Ok(Err(_))
        ));
        assert!(
            start.elapsed() >= Duration::from_secs(28) && start.elapsed() < Duration::from_secs(34)
        );
        assert!(
            tokio::time::timeout(Duration::from_millis(50), Owner::receive(&mut owner))
                .await
                .is_err()
        );
    }
    /// Separate-process protected-channel server and sender; only explicit test invocations use these environment locators.
    #[test]
    fn channel_owner_helper() {
        use std::io::{BufRead, Read, Write};
        use std::os::unix::fs::PermissionsExt;
        let Some(path) = std::env::var_os("AGENT_RUN_OWNER_TEST_CHANNEL") else {
            return;
        };
        let listener = std::os::unix::net::UnixListener::bind(&path).unwrap();
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600)).unwrap();
        let accept = std::thread::spawn(move || {
            for _ in 0..2 {
                let (mut stream, _) = listener.accept().unwrap();
                let _ = stream.read(&mut [0]);
            }
        });
        println!("READY");
        std::io::stdout().flush().unwrap();
        let mut line = String::new();
        std::io::stdin().lock().read_line(&mut line).unwrap();
        let mut stream = std::os::unix::net::UnixStream::connect(line.trim()).unwrap();
        stream
            .set_read_timeout(Some(Duration::from_secs(5)))
            .unwrap();
        let bytes = serde_json::to_vec(&request()).unwrap();
        stream
            .write_all(&(bytes.len() as u32).to_le_bytes())
            .unwrap();
        stream.write_all(&bytes).unwrap();
        let mut length = [0; 4];
        stream.read_exact(&mut length).unwrap();
        let mut body = vec![0; u32::from_le_bytes(length) as usize];
        stream.read_exact(&mut body).unwrap();
        println!("REPLY {}", std::str::from_utf8(&body).unwrap());
        accept.join().unwrap();
    }
    /// Current channel proof admits a restarted daemon without build admission; the original call keeps one effect.
    #[tokio::test]
    async fn current_channel_daemon_restart_preserves_original_call() {
        use std::os::unix::fs::PermissionsExt;
        use std::process::Stdio;
        use tokio::io::{AsyncBufReadExt, BufReader};
        let root = tempfile::tempdir().unwrap();
        std::fs::set_permissions(root.path(), std::fs::Permissions::from_mode(0o700)).unwrap();
        let channel = root.path().join("c.sock");
        let exe = std::env::current_exe().unwrap().canonicalize().unwrap();
        let mut env = BTreeMap::from([
            (SETTINGS[0].into(), channel.to_str().unwrap().into()),
            (SETTINGS[1].into(), root.path().to_str().unwrap().into()),
        ]);
        let mut owner = None;
        let mut calls = Calls::default();
        let mut effects = 0;
        let mut previous_pid = None;
        for _ in 0..2 {
            let mut child = tokio::process::Command::new(&exe)
                .args([
                    "--exact",
                    "native_owner::tests::channel_owner_helper",
                    // Serial libtest otherwise prefixes the helper's READY/REPLY lines.
                    "--quiet",
                    "--nocapture",
                ])
                .env("AGENT_RUN_OWNER_TEST_CHANNEL", &channel)
                .stdin(Stdio::piped())
                .stdout(Stdio::piped())
                .kill_on_drop(true)
                .spawn()
                .unwrap();
            let pid = child.id().unwrap();
            assert_ne!(previous_pid, Some(pid));
            previous_pid = Some(pid);
            let mut lines = BufReader::new(child.stdout.take().unwrap()).lines();
            tokio::time::timeout(Duration::from_secs(5), async {
                while let Some(line) = lines.next_line().await.unwrap() {
                    if line == "READY" {
                        return;
                    }
                }
                panic!("daemon readiness missing");
            })
            .await
            .unwrap();
            if owner.is_none() {
                owner = Owner::prepare(&mut env).unwrap();
            }
            let socket = owner.as_ref().unwrap().socket.clone();
            child
                .stdin
                .as_mut()
                .unwrap()
                .write_all(format!("{}\n", socket.display()).as_bytes())
                .await
                .unwrap();
            let call = tokio::time::timeout(Duration::from_secs(5), Owner::receive(&mut owner))
                .await
                .unwrap();
            let result = match calls.reserve(&call.request) {
                Ok(()) => {
                    effects += 1;
                    let r =
                        Reply::accepted(&call.request.call_id, "claude_input", "fixture-replay");
                    calls.finish(r.clone());
                    r
                }
                Err(r) => r,
            };
            call.answer(result);
            tokio::time::timeout(Duration::from_secs(5), async {
                while let Some(line) = lines.next_line().await.unwrap() {
                    if let Some(value) = line.strip_prefix("REPLY ") {
                        let body: serde_json::Value = serde_json::from_str(value).unwrap();
                        assert_eq!(body["native_id"], "fixture-replay");
                        return;
                    }
                }
                panic!("daemon receipt missing");
            })
            .await
            .unwrap();
            assert!(child.wait().await.unwrap().success());
            std::fs::remove_file(&channel).unwrap();
        }
        assert_eq!(effects, 1);
        assert_eq!(calls.entries.len(), 1);
    }
}
