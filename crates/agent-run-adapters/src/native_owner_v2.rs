//! Separate per-launch managed owner-v2 transport. It never writes native stdin.
use crate::native_owner::{self, FRAME_BYTES, OWNER_BUDGET};
use agent_run_domain::{
    Result,
    error::invalid,
    owner_v2::{ReplyV2, RequestV2, decode_request, invalid_request_echo},
};
use std::{
    path::PathBuf,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{UnixListener, UnixStream},
    sync::{mpsc, oneshot},
    task::JoinHandle,
    time::{Instant, timeout_at},
};

/// V2 address injected only into a selected managed harness at exec.
pub const LOCATOR: &str = "BRIDGE_NATIVE_OWNER_SOCKET_V2";
/// Frozen trusted Bridge channel/build and private per-launch parent directory.
#[derive(Clone)]
pub struct Config {
    pub channel: PathBuf,
    /// Bridge socket pinned independently by the frozen Run transport.
    pub expected_channel: PathBuf,
    pub directory: PathBuf,
    pub bridge_executable: PathBuf,
    pub bridge_sha256: String,
}
/// One authenticated handoff, owned by the existing native writer lane.
pub struct Call {
    pub request: RequestV2,
    pub deadline: Instant,
    /// The endpoint's checked local trust anchor, never from request JSON.
    pub channel: PathBuf,
    reply: oneshot::Sender<ReplyV2>,
}
impl Call {
    pub fn answer(self, reply: ReplyV2) {
        let _ = self.reply.send(reply);
    }
    /// Construct a synthetic owner call without a real Bridge process.
    #[cfg(feature = "test-fixtures")]
    pub fn fixture(
        request: RequestV2,
        deadline: Instant,
        channel: PathBuf,
    ) -> (Self, oneshot::Receiver<ReplyV2>) {
        let (reply, receiver) = oneshot::channel();
        (
            Self {
                request,
                deadline,
                channel,
                reply,
            },
            receiver,
        )
    }
}
/// Construct a response with the current dispatch correlation and immutable effect identity.
pub fn reply(
    request: &RequestV2,
    state: &str,
    protocol: Option<&str>,
    native_id: Option<&str>,
    reason: Option<&str>,
) -> ReplyV2 {
    ReplyV2 {
        version: 2,
        call_id: request.call_id.clone(),
        effect_id: request.outbox_intent.effect_id.clone(),
        effect_digest: request.effect_digest.clone(),
        state: state.into(),
        protocol: protocol.map(str::to_owned),
        native_id: native_id.map(str::to_owned),
        reason: reason.map(str::to_owned),
    }
}

#[cfg(target_os = "linux")]
fn verify_build(identity: &agent_run_platform::process::Identity, config: &Config) -> Result<()> {
    use std::{io::Read, os::unix::fs::MetadataExt};
    let fail = || invalid("owner v2 peer unavailable");
    if !agent_run_config::provider_config::loader_preload_safe()
        || !config.bridge_executable.is_absolute()
        || agent_run_config::provider_config::forbidden_interpreter(&config.bridge_executable)
        || config
            .bridge_executable
            .canonicalize()
            .map_err(|_| fail())?
            != config.bridge_executable
        || config.bridge_sha256.len() != 64
        || !config
            .bridge_sha256
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
    {
        return Err(fail());
    }
    let before = agent_run_platform::process::kernel_incarnation(identity).map_err(|_| fail())?;
    let peer_safe = || -> Result<()> {
        let mut environment = Vec::new();
        std::fs::File::open(format!("/proc/{}/environ", identity.pid))
            .map_err(|_| fail())?
            .take(1024 * 1024 + 1)
            .read_to_end(&mut environment)
            .map_err(|_| fail())?;
        if environment.len() > 1024 * 1024
            || (!environment.is_empty() && environment.last() != Some(&0))
            || environment
                .split(|byte| *byte == 0)
                .any(|entry| entry.starts_with(b"LD_"))
        {
            return Err(fail());
        }
        let mut status = Vec::new();
        std::fs::File::open(format!("/proc/{}/status", identity.pid))
            .map_err(|_| fail())?
            .take(65537)
            .read_to_end(&mut status)
            .map_err(|_| fail())?;
        let text = std::str::from_utf8(&status).map_err(|_| fail())?;
        let mut tracers = text
            .lines()
            .filter_map(|line| line.strip_prefix("TracerPid:"));
        if status.len() > 65536
            || !matches!((tracers.next(), tracers.next()), (Some(value), None) if value.trim() == "0")
        {
            return Err(fail());
        }
        Ok(())
    };
    peer_safe()?;
    let path = agent_run_platform::process::executable(identity.pid).map_err(|_| fail())?;
    if path != config.bridge_executable {
        return Err(fail());
    }
    let metadata = std::fs::metadata(&path).map_err(|_| fail())?;
    let loaded = std::fs::metadata(format!("/proc/{}/exe", identity.pid)).map_err(|_| fail())?;
    // SAFETY: geteuid has no arguments or side effects.
    let uid = unsafe { libc::geteuid() };
    if !metadata.is_file()
        || !(metadata.uid() == uid || metadata.uid() == 0)
        || metadata.mode() & 0o022 != 0
        || before.uid != uid
        || metadata.dev() != loaded.dev()
        || metadata.ino() != loaded.ino()
        || metadata.len() > 128 * 1024 * 1024
    {
        return Err(fail());
    }
    if !agent_run_platform::process::executable_predates_start(before.start_ticks, &metadata)
        .map_err(|_| fail())?
    {
        return Err(fail());
    }
    let bytes = std::fs::read(&path).map_err(|_| fail())?;
    let after = std::fs::metadata(&path).map_err(|_| fail())?;
    let loaded_after =
        std::fs::metadata(format!("/proc/{}/exe", identity.pid)).map_err(|_| fail())?;
    if bytes.starts_with(b"#!")
        || agent_run_config::provider_config::forbidden_interpreter_build(
            &path,
            &config.bridge_sha256,
            metadata.len(),
        )
        || metadata.dev() != after.dev()
        || metadata.ino() != after.ino()
        || metadata.len() != after.len()
        || metadata.mtime() != after.mtime()
        || metadata.mtime_nsec() != after.mtime_nsec()
        || metadata.ctime() != after.ctime()
        || metadata.ctime_nsec() != after.ctime_nsec()
        || metadata.dev() != loaded_after.dev()
        || metadata.ino() != loaded_after.ino()
        || agent_run_platform::fs::sha256(&bytes) != config.bridge_sha256
    {
        return Err(fail());
    }
    if agent_run_platform::process::executable(identity.pid).map_err(|_| fail())? != path
        || agent_run_platform::process::kernel_incarnation(identity).map_err(|_| fail())? != before
    {
        return Err(fail());
    }
    peer_safe()?;
    if !agent_run_config::provider_config::loader_preload_safe() {
        return Err(fail());
    }
    Ok(())
}
#[cfg(not(target_os = "linux"))]
fn verify_build(_: &agent_run_platform::process::Identity, _: &Config) -> Result<()> {
    Err(agent_run_domain::Error::Unsupported(
        "owner v2 requires Linux".into(),
    ))
}
async fn authenticate(
    stream: &UnixStream,
    trust: &native_owner::Trust,
    config: &Config,
    deadline: Instant,
) -> Result<()> {
    trust.verify(stream, deadline).await?;
    let identity = native_owner::peer(stream)?;
    let config = config.clone();
    tokio::task::spawn_blocking(move || verify_build(&identity, &config))
        .await
        .map_err(|_| invalid("owner v2 peer unavailable"))??;
    trust.verify(stream, deadline).await
}

/// Per-launch listener. Qualification readiness begins false and is enabled only
/// after the supervisor's exact-launch qualification commit succeeds.
pub struct Owner {
    incoming: mpsc::Receiver<Call>,
    transport: JoinHandle<()>,
    ready: Arc<AtomicBool>,
    directory: PathBuf,
    socket: PathBuf,
    identity: ((u64, u64), (u64, u64)),
}
impl Owner {
    /// Create the distinct v2 endpoint; caller has already selected managed role.
    pub fn prepare(
        environment: &mut std::collections::BTreeMap<String, String>,
        config: Config,
    ) -> Result<Self> {
        use std::os::unix::fs::{DirBuilderExt, MetadataExt, PermissionsExt};
        environment.remove(LOCATOR);
        environment.remove(native_owner::LOCATOR);
        for name in native_owner::SETTINGS
            .into_iter()
            .chain(native_owner::RETIRED_SETTINGS)
        {
            environment.remove(name);
        }
        if config.channel != config.expected_channel
            || config.channel.canonicalize().ok().as_ref() != Some(&config.expected_channel)
        {
            return Err(invalid("owner v2 channel differs from pinned Bridge route"));
        }
        native_owner::protected(&config.channel, true)?;
        native_owner::protected(&config.directory, false)?;
        let root = std::fs::symlink_metadata(&config.directory)?;
        if !root.is_dir() || root.mode() & 0o777 != 0o700 {
            return Err(invalid("owner v2 directory unavailable"));
        }
        let directory = config
            .directory
            .join(uuid::Uuid::new_v4().simple().to_string());
        std::fs::DirBuilder::new().mode(0o700).create(&directory)?;
        let socket = directory.join("v2.sock");
        let listener = match UnixListener::bind(&socket) {
            Ok(listener) => listener,
            Err(error) => {
                let _ = std::fs::remove_dir(&directory);
                return Err(error.into());
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
        let trust = native_owner::Trust {
            channel: config.channel.clone(),
        };
        let (send, incoming) = mpsc::channel(1);
        let ready = Arc::new(AtomicBool::new(false));
        let current = ready.clone();
        let transport = tokio::spawn(async move {
            while let Ok((mut stream, _)) = listener.accept().await {
                let deadline = Instant::now() + OWNER_BUDGET;
                let input = async {
                    authenticate(&stream, &trust, &config, deadline).await?;
                    let length = stream.read_u32_le().await? as usize;
                    if length == 0 || length > FRAME_BYTES {
                        return Err(invalid("owner v2 frame unavailable"));
                    }
                    let mut bytes = vec![0; length];
                    stream.read_exact(&mut bytes).await?;
                    let request = match decode_request(&bytes) {
                        Ok(request) => Ok(request),
                        Err(_) => {
                            let (call_id, effect_id, effect_digest) = invalid_request_echo(&bytes)
                                .ok_or_else(|| invalid("owner v2 frame unavailable"))?;
                            Err(ReplyV2 {
                                version: 2,
                                call_id,
                                effect_id,
                                effect_digest,
                                state: "known_unsent".into(),
                                protocol: None,
                                native_id: None,
                                reason: Some("invalid_request".into()),
                            })
                        }
                    };
                    authenticate(&stream, &trust, &config, deadline).await?;
                    Ok::<_, agent_run_domain::Error>(request)
                };
                let Ok(Ok(request)) = timeout_at(deadline, input).await else {
                    continue;
                };
                let result = if let Err(refusal) = request {
                    refusal
                } else {
                    let request = request.expect("checked valid request");
                    if !current.load(Ordering::Acquire) {
                        reply(
                            &request,
                            "known_unsent",
                            None,
                            None,
                            Some("qualification_pending"),
                        )
                    } else {
                        let fallback =
                            reply(&request, "unknown", None, None, Some("native_uncertain"));
                        let unsent =
                            reply(&request, "known_unsent", None, None, Some("unavailable"));
                        let (response, receive) = oneshot::channel();
                        let call = Call {
                            request,
                            deadline,
                            channel: config.channel.clone(),
                            reply: response,
                        };
                        if !matches!(timeout_at(deadline, send.send(call)).await, Ok(Ok(()))) {
                            unsent
                        } else {
                            match timeout_at(deadline, receive).await {
                                Ok(Ok(result)) => result,
                                _ => fallback,
                            }
                        }
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
        Ok(Self {
            incoming,
            transport,
            ready,
            directory,
            socket,
            identity,
        })
    }
    /// Called only after exact native role qualification is durable.
    pub fn enable(&self) {
        self.ready.store(true, Ordering::Release);
    }
    /// Receive one call; absent endpoint remains pending without polling.
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

#[cfg(all(test, target_os = "linux"))]
mod tests {
    use super::*;
    use crate::{
        LaunchPlan,
        io::{Event, Process},
    };
    use agent_run_domain::{
        attempt_binding::{Actor, AttemptRef, Binding, Incarnation, Route},
        domain::AgentId,
        owner_v2::{OutboxIntent, RequestV2, outbox_digest},
    };
    use std::{collections::BTreeMap, os::unix::fs::PermissionsExt, path::Path, time::Duration};

    #[test]
    fn fake_bridge_process() {
        use std::io::{Read, Write};
        let Some(root) = std::env::var_os("RUN_R3_BRIDGE_ROOT").map(PathBuf::from) else {
            return;
        };
        let channel = root.join("bridge.sock");
        let _listener = std::os::unix::net::UnixListener::bind(&channel).unwrap();
        std::fs::set_permissions(&channel, std::fs::Permissions::from_mode(0o600)).unwrap();
        std::fs::write(root.join("ready"), b"ready").unwrap();
        fn wait_file(path: &Path) {
            let start = std::time::Instant::now();
            while !path.exists() {
                assert!(start.elapsed() < Duration::from_secs(20));
                std::thread::sleep(Duration::from_millis(10));
            }
        }
        wait_file(&root.join("locator"));
        let socket = std::fs::read_to_string(root.join("locator")).unwrap();
        let input = std::fs::read(root.join("request")).unwrap();
        for phase in 1..=3 {
            if phase == 2 {
                wait_file(&root.join("qualified"));
            }
            let mut stream = std::os::unix::net::UnixStream::connect(&socket).unwrap();
            let input = if phase == 3 {
                std::fs::read(root.join("invalid-request")).unwrap()
            } else {
                input.clone()
            };
            stream
                .write_all(&(input.len() as u32).to_le_bytes())
                .unwrap();
            stream.write_all(&input).unwrap();
            let mut length = [0u8; 4];
            stream.read_exact(&mut length).unwrap();
            let mut bytes = vec![0; u32::from_le_bytes(length) as usize];
            stream.read_exact(&mut bytes).unwrap();
            std::fs::write(root.join(format!("result-{phase}")), bytes).unwrap();
        }
    }

    async fn wait_file(path: &Path) {
        tokio::time::timeout(Duration::from_secs(15), async {
            while !path.exists() {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn verify_build_rejects_hash_replacement_permissions_and_loader_environment() {
        let base = std::env::var_os("AGENT_RUN_TEST_SOCKET_TMP")
            .map(PathBuf::from)
            .unwrap_or_else(std::env::temp_dir);
        for preload in [false, true] {
            let root = tempfile::Builder::new()
                .prefix("r3-build-")
                .tempdir_in(&base)
                .unwrap();
            std::fs::set_permissions(root.path(), std::fs::Permissions::from_mode(0o700)).unwrap();
            let executable = root.path().join("fixture-bridge");
            std::fs::copy(std::env::current_exe().unwrap(), &executable).unwrap();
            std::fs::set_permissions(&executable, std::fs::Permissions::from_mode(0o700)).unwrap();
            let digest = agent_run_platform::fs::sha256(&std::fs::read(&executable).unwrap());
            // A newly installed Bridge may start in the same wall second.
            tokio::time::sleep(Duration::from_millis(30)).await;
            let mut command = tokio::process::Command::new(&executable);
            command
                .args([
                    "--exact",
                    "native_owner_v2::tests::fake_bridge_process",
                    "--nocapture",
                ])
                .env_clear()
                .env("RUN_R3_BRIDGE_ROOT", root.path())
                .stdout(std::process::Stdio::null())
                .stderr(std::process::Stdio::null())
                .kill_on_drop(true);
            if preload {
                command.env("LD_PRELOAD", "/nonexistent/synthetic.so");
            }
            let mut child = command.spawn().unwrap();
            wait_file(&root.path().join("ready")).await;
            let identity =
                agent_run_platform::process::inspect(child.id().unwrap() as i32).unwrap();
            let config = Config {
                channel: root.path().join("bridge.sock"),
                expected_channel: root.path().join("bridge.sock"),
                directory: root.path().into(),
                bridge_executable: executable.clone(),
                bridge_sha256: digest,
            };
            assert_eq!(verify_build(&identity, &config).is_ok(), !preload);
            if !preload {
                let mut wrong = config.clone();
                wrong.bridge_sha256 = "0".repeat(64);
                assert!(verify_build(&identity, &wrong).is_err());
                wrong = config.clone();
                wrong.bridge_executable = "/usr/bin/python3".into();
                assert!(verify_build(&identity, &wrong).is_err());
                std::fs::set_permissions(&executable, std::fs::Permissions::from_mode(0o770))
                    .unwrap();
                assert!(verify_build(&identity, &config).is_err());
                std::fs::rename(&executable, root.path().join("old-build")).unwrap();
                std::fs::copy(std::env::current_exe().unwrap(), &executable).unwrap();
                std::fs::set_permissions(&executable, std::fs::Permissions::from_mode(0o700))
                    .unwrap();
                assert!(verify_build(&identity, &config).is_err());
            }
            child.kill().await.unwrap();
            let _ = child.wait().await;
        }
    }

    #[tokio::test]
    async fn selected_launch_advertises_v2_before_qualification_but_never_handoffs() {
        let base = std::env::var_os("AGENT_RUN_TEST_SOCKET_TMP")
            .map(PathBuf::from)
            .unwrap_or_else(std::env::temp_dir);
        let root = tempfile::Builder::new()
            .prefix("r3-owner-")
            .tempdir_in(base)
            .unwrap();
        std::fs::set_permissions(root.path(), std::fs::Permissions::from_mode(0o700)).unwrap();
        let executable = root.path().join("fixture-bridge");
        std::fs::copy(std::env::current_exe().unwrap(), &executable).unwrap();
        std::fs::set_permissions(&executable, std::fs::Permissions::from_mode(0o700)).unwrap();
        let digest = agent_run_platform::fs::sha256(&std::fs::read(&executable).unwrap());
        tokio::time::sleep(Duration::from_secs(2)).await;
        let mut bridge = tokio::process::Command::new(&executable)
            .args([
                "--exact",
                "native_owner_v2::tests::fake_bridge_process",
                "--nocapture",
            ])
            .env_clear()
            .env("RUN_R3_BRIDGE_ROOT", root.path())
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .kill_on_drop(true)
            .spawn()
            .unwrap();
        wait_file(&root.path().join("ready")).await;
        let id = AgentId::new();
        let store: agent_run_domain::StoreId =
            "store_0123456789abcdef0123456789abcdef".parse().unwrap();
        let route = |component: &str, name: &str| Route {
            component_store_id: component.into(),
            route_id: name.into(),
            worker_uid: 1000,
        };
        let bridge_route = route("01234567-89ab-4cde-8fab-0123456789ab", "bridge");
        let outbox = OutboxIntent {
            effect_id: "effect".into(),
            producer_scope: "scope".into(),
            event_key: "event".into(),
            payload_sha256: agent_run_platform::fs::sha256(b"text"),
            payload_length: 4,
            target_binding: Binding {
                pool_store_id: "pool".into(),
                pool_id: "pool".into(),
                member_id: "member".into(),
                binding_generation: 1,
                attempt: AttemptRef {
                    run_store_id: store.clone(),
                    lineage_root_id: id.clone(),
                    execution_id: id,
                    sequence: 1,
                    attempt_id: "attempt".into(),
                    launch_id: "01234567-89ab-4cde-8fab-0123456789ab".into(),
                },
                actor: Actor {
                    address: "host/codex/native".into(),
                    role: "root".into(),
                    native_id: "native".into(),
                    epoch: 1,
                },
                run_route: route(&store.to_string(), "run"),
                bridge_route: bridge_route.clone(),
            },
            expected_incarnation: Incarnation {
                host_id: "host".into(),
                boot_id: "01234567-89ab-4cde-8fab-0123456789ab".into(),
                pid_namespace_inode: 2,
                uid: 1000,
                pid: 42,
                start_ticks: 7,
            },
            bridge_route,
            mode: "message".into(),
            retention: "keep_all".into(),
        };
        let request = RequestV2 {
            version: 2,
            thread_id: "native".into(),
            call_id: "call".into(),
            effect_digest: outbox_digest(&outbox).unwrap(),
            outbox_intent: outbox,
            text: "text".into(),
            wake_only: true,
        };
        std::fs::write(
            root.path().join("request"),
            serde_json::to_vec(&request).unwrap(),
        )
        .unwrap();
        let mut invalid = serde_json::to_value(&request).unwrap();
        invalid["text"] = serde_json::json!("tampered");
        std::fs::write(
            root.path().join("invalid-request"),
            serde_json::to_vec(&invalid).unwrap(),
        )
        .unwrap();
        let plan = LaunchPlan {
            binary:"/bin/sh".into(),
            args:vec!["-c".into(),"printf '{\"v1\":\"%s\",\"v2\":\"%s\"}\\n' \"${BRIDGE_NATIVE_OWNER_SOCKET-unset}\" \"${BRIDGE_NATIVE_OWNER_SOCKET_V2-unset}\"; read stop".into()],
            cwd:root.path().into(),environment:BTreeMap::from([("BRIDGE_NATIVE_OWNER_SOCKET".into(),"forged".into())]),initial_input:None,
        };
        let mut untrusted = BTreeMap::new();
        let mismatched = Config {
            channel: root.path().join("bridge.sock"),
            expected_channel: root.path().join("other.sock"),
            directory: root.path().into(),
            bridge_executable: executable.clone(),
            bridge_sha256: digest.clone(),
        };
        assert!(Owner::prepare(&mut untrusted, mismatched).is_err());
        assert!(!untrusted.contains_key(LOCATOR));
        let mut process = Process::spawn_managed(
            &plan,
            Some(Config {
                channel: root.path().join("bridge.sock"),
                expected_channel: root.path().join("bridge.sock"),
                directory: root.path().into(),
                bridge_executable: executable,
                bridge_sha256: digest,
            }),
        )
        .unwrap();
        let Event::Json(environment) = tokio::time::timeout(Duration::from_secs(5), process.next())
            .await
            .unwrap()
        else {
            panic!("harness environment missing")
        };
        assert_eq!(environment["v1"], "unset");
        let socket = environment["v2"].as_str().unwrap();
        assert!(socket.ends_with("/v2.sock"));
        std::fs::write(root.path().join("locator"), socket).unwrap();
        wait_file(&root.path().join("result-1")).await;
        let first: ReplyV2 =
            serde_json::from_slice(&std::fs::read(root.path().join("result-1")).unwrap()).unwrap();
        assert_eq!(first.state, "known_unsent");
        assert_eq!(first.reason.as_deref(), Some("qualification_pending"));
        assert!(
            tokio::time::timeout(
                Duration::from_millis(30),
                Owner::receive(&mut process.native_owner_v2)
            )
            .await
            .is_err()
        );
        process.native_owner_v2.as_ref().unwrap().enable();
        std::fs::write(root.path().join("qualified"), b"ok").unwrap();
        let call = tokio::time::timeout(
            Duration::from_secs(15),
            Owner::receive(&mut process.native_owner_v2),
        )
        .await
        .unwrap();
        assert_eq!(call.request.effect_digest, request.effect_digest);
        let response = reply(&call.request, "known_unsent", None, None, Some("not_idle"));
        call.answer(response);
        assert!(bridge.wait().await.unwrap().success());
        let second: ReplyV2 =
            serde_json::from_slice(&std::fs::read(root.path().join("result-2")).unwrap()).unwrap();
        assert_eq!(second.reason.as_deref(), Some("not_idle"));
        let invalid: ReplyV2 =
            serde_json::from_slice(&std::fs::read(root.path().join("result-3")).unwrap()).unwrap();
        assert_eq!(invalid.state, "known_unsent");
        assert_eq!(invalid.reason.as_deref(), Some("invalid_request"));
        assert_eq!(invalid.call_id, request.call_id);
        assert_eq!(invalid.effect_id, request.outbox_intent.effect_id);
        assert!(
            process
                .owner
                .cleanup(Duration::from_secs(2))
                .await
                .unwrap()
                .confirmed
        );
        process.reap().await;
    }
}
