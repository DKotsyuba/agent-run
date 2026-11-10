//! Same-UID executable-pinned Unix peer verification for the opt-in private wire.
//! The caller must additionally enforce its configured component grant/route.
#![allow(dead_code)]
use crate::{Error, Result, error::invalid};
#[cfg(target_os = "linux")]
use sha2::{Digest, Sha256};
use std::{
    io::Read,
    os::unix::fs::MetadataExt,
    path::{Path, PathBuf},
};
use tokio::net::UnixStream;

/// Protected executable identity, configured out of band, never from a frame.
#[derive(Clone)]
pub(crate) struct PeerPin {
    pub uid: u32,
    pub executable: PathBuf,
    pub sha256: String,
}

#[cfg(target_os = "linux")]
fn clean_peer_environment(bytes: &[u8]) -> bool {
    (bytes.is_empty() || bytes.last() == Some(&0))
        && !bytes
            .split(|b| *b == 0)
            .any(|entry| entry.starts_with(b"LD_"))
}
#[cfg(target_os = "linux")]
fn untraced(status: &str) -> bool {
    let mut values = status
        .lines()
        .filter_map(|line| line.strip_prefix("TracerPid:"));
    matches!((values.next(),values.next()),(Some(value),None) if value.trim()=="0")
}
#[cfg(target_os = "linux")]
fn interpreter(path: &Path) -> bool {
    agent_run_config::provider_config::forbidden_interpreter(path)
}

/// Verify a connected peer's effective UID, full kernel incarnation, exact
/// executable inode/path, trusted ownership and unchanged loaded-build bytes.
/// PID reuse before the kernel peer credential is inspected remains outside the
/// guarantee until SO_PEERPIDFD is available through a reviewed safe wrapper.
#[cfg(target_os = "linux")]
pub(crate) fn verify_peer(stream: &UnixStream, pin: &PeerPin) -> Result<()> {
    let reject = || invalid("attempt peer verification failed");
    if !agent_run_config::provider_config::loader_preload_safe()
        || !pin.executable.is_absolute()
        || pin.sha256.len() != 64
        || !pin
            .sha256
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
    {
        return Err(reject());
    }
    let credentials = stream.peer_cred().map_err(|_| reject())?;
    let pid = credentials.pid().ok_or_else(reject)?;
    // SAFETY: geteuid has no pointer arguments or side effects.
    if credentials.uid() != pin.uid || pin.uid != unsafe { libc::geteuid() } {
        return Err(reject());
    }
    let before = agent_run_platform::process::inspect(pid).map_err(|_| reject())?;
    let kernel = agent_run_platform::process::kernel_incarnation(&before).map_err(|_| reject())?;
    if kernel.uid != pin.uid {
        return Err(reject());
    }
    let path = pin.executable.canonicalize().map_err(|_| reject())?;
    if path != pin.executable || interpreter(&path) {
        return Err(reject());
    }
    let read_peer = || -> Result<()> {
        let mut environment = Vec::new();
        std::fs::File::open(format!("/proc/{pid}/environ"))
            .map_err(|_| reject())?
            .take(1024 * 1024 + 1)
            .read_to_end(&mut environment)
            .map_err(|_| reject())?;
        if environment.len() > 1024 * 1024 || !clean_peer_environment(&environment) {
            return Err(reject());
        }
        let mut bytes = Vec::new();
        std::fs::File::open(format!("/proc/{pid}/status"))
            .map_err(|_| reject())?
            .take(65537)
            .read_to_end(&mut bytes)
            .map_err(|_| reject())?;
        if bytes.len() > 65536 || !std::str::from_utf8(&bytes).ok().is_some_and(untraced) {
            return Err(reject());
        }
        Ok(())
    };
    read_peer()?;
    let link = std::fs::read_link(format!("/proc/{pid}/exe")).map_err(|_| reject())?;
    if link != path {
        return Err(reject());
    }
    let mut file = std::fs::File::open(format!("/proc/{pid}/exe")).map_err(|_| reject())?;
    let metadata = file.metadata().map_err(|_| reject())?;
    let loaded = std::fs::metadata(&path).map_err(|_| reject())?;
    if !metadata.is_file()
        || (metadata.uid() != 0 && metadata.uid() != pin.uid)
        || metadata.mode() & 0o022 != 0
        || metadata.dev() != loaded.dev()
        || metadata.ino() != loaded.ino()
        || metadata.len() > 512 * 1024 * 1024
    {
        return Err(reject());
    }
    if !agent_run_platform::process::executable_write_excluded(&file).map_err(|_| reject())? {
        return Err(reject());
    }
    let mut hash = Sha256::new();
    let mut buf = [0u8; 65536];
    loop {
        let n = file.read(&mut buf).map_err(|_| reject())?;
        if n == 0 {
            break;
        }
        hash.update(&buf[..n]);
    }
    let after_meta = file.metadata().map_err(|_| reject())?;
    let after_loaded = std::fs::metadata(format!("/proc/{pid}/exe")).map_err(|_| reject())?;
    let after_link = std::fs::read_link(format!("/proc/{pid}/exe")).map_err(|_| reject())?;
    if agent_run_config::provider_config::forbidden_interpreter_build(
        &path,
        &pin.sha256,
        metadata.len(),
    ) || metadata.dev() != after_meta.dev()
        || metadata.ino() != after_meta.ino()
        || metadata.len() != after_meta.len()
        || metadata.mtime() != after_meta.mtime()
        || metadata.mtime_nsec() != after_meta.mtime_nsec()
        || metadata.ctime() != after_meta.ctime()
        || metadata.ctime_nsec() != after_meta.ctime_nsec()
        || metadata.dev() != after_loaded.dev()
        || metadata.ino() != after_loaded.ino()
        || after_link != path
    {
        return Err(reject());
    }
    if agent_run_domain::canonical::hex_digest(&hash.finalize()) != pin.sha256 {
        return Err(invalid("attempt peer executable changed"));
    }
    let after = agent_run_platform::process::kernel_incarnation(&before).map_err(|_| reject())?;
    read_peer()?;
    if after != kernel || !agent_run_config::provider_config::loader_preload_safe() {
        return Err(reject());
    }
    Ok(())
}
#[cfg(not(target_os = "linux"))]
pub(crate) fn verify_peer(_: &UnixStream, _: &PeerPin) -> Result<()> {
    Err(Error::Unsupported(
        "attempt transport requires Linux".into(),
    ))
}

/// Parent-chain facts are read twice; no argv or environment values are kept.
#[cfg(target_os = "linux")]
#[derive(Clone, PartialEq, Eq)]
struct RoleLink {
    pid: u32,
    parent: u32,
    uid: u32,
    ticks: u64,
}

#[cfg(target_os = "linux")]
fn role_link(pid: u32) -> Result<RoleLink> {
    let reject = || invalid("Pool resident process context unavailable");
    let bounded = |name: &str, limit: u64| -> Result<Vec<u8>> {
        let mut bytes = Vec::new();
        std::fs::File::open(format!("/proc/{pid}/{name}"))
            .map_err(|_| reject())?
            .take(limit + 1)
            .read_to_end(&mut bytes)
            .map_err(|_| reject())?;
        if bytes.len() as u64 > limit {
            return Err(reject());
        }
        Ok(bytes)
    };
    let status = bounded("status", 65536)?;
    let text = std::str::from_utf8(&status).map_err(|_| reject())?;
    let mut lines = text.lines().filter_map(|line| line.strip_prefix("Uid:"));
    let uids = lines
        .next()
        .ok_or_else(reject)?
        .split_ascii_whitespace()
        .map(str::parse::<u32>)
        .collect::<std::result::Result<Vec<_>, _>>()
        .map_err(|_| reject())?;
    if uids.len() != 4 || lines.next().is_some() {
        return Err(reject());
    }
    let stat = bounded("stat", 65536)?;
    let end = stat
        .iter()
        .rposition(|byte| *byte == b')')
        .ok_or_else(reject)?;
    let open = stat[..end]
        .iter()
        .position(|byte| *byte == b'(')
        .ok_or_else(reject)?;
    if std::str::from_utf8(&stat[..open])
        .ok()
        .and_then(|text| text.trim().parse::<u32>().ok())
        != Some(pid)
    {
        return Err(reject());
    }
    let fields: Vec<_> = stat[end + 1..]
        .split(|byte| byte.is_ascii_whitespace())
        .filter(|field| !field.is_empty())
        .collect();
    if fields
        .first()
        .is_none_or(|state| *state == b"Z" || *state == b"X")
    {
        return Err(reject());
    }
    let number = |index: usize| {
        fields
            .get(index)
            .and_then(|field| std::str::from_utf8(field).ok())
            .and_then(|value| value.parse::<u64>().ok())
            .ok_or_else(reject)
    };
    let parent = u32::try_from(number(1)?).map_err(|_| reject())?;
    let ticks = number(19)?;
    if parent == pid || (pid > 1 && ticks == 0) {
        return Err(reject());
    }
    Ok(RoleLink {
        pid,
        parent,
        uid: uids[1],
        ticks,
    })
}

/// A launch marker is negative evidence only. Never return its value.
#[cfg(target_os = "linux")]
fn has_launch_marker(pid: u32) -> Result<bool> {
    let reject = || invalid("Pool resident process context unavailable");
    let mut bytes = Vec::new();
    std::fs::File::open(format!("/proc/{pid}/environ"))
        .map_err(|_| reject())?
        .take(1024 * 1024 + 1)
        .read_to_end(&mut bytes)
        .map_err(|_| reject())?;
    if bytes.is_empty() || bytes.len() > 1024 * 1024 || bytes.last() != Some(&0) {
        return Err(reject());
    }
    Ok(bytes
        .split(|byte| *byte == 0)
        .any(|entry| entry.starts_with(b"AGENT_RUN_LAUNCH_ID=")))
}

/// Verify that a Pool peer is not a Run harness or a descendant of one. The
/// complete ancestry, inherited negative marker and recorded ownership are all
/// checked, then the whole chain is reread to reject reparent/PID reuse races.
#[cfg(target_os = "linux")]
pub(crate) fn verify_pool_resident(stream: &UnixStream, home: &Path) -> Result<()> {
    let reject = || invalid("Pool resident process context unavailable");
    let credentials = stream.peer_cred().map_err(|_| reject())?;
    let peer_pid = credentials.pid().ok_or_else(reject)?;
    let peer = agent_run_platform::process::inspect(peer_pid).map_err(|_| reject())?;
    let incarnation =
        agent_run_platform::process::kernel_incarnation(&peer).map_err(|_| reject())?;
    if incarnation.uid != credentials.uid() {
        return Err(reject());
    }
    let mut chain: Vec<(
        RoleLink,
        Option<agent_run_platform::process::KernelIncarnation>,
    )> = Vec::new();
    let mut next = incarnation.pid;
    while next != 0 {
        if chain.len() >= 128 || chain.iter().any(|(link, _)| link.pid == next) {
            return Err(reject());
        }
        let link = role_link(next)?;
        if chain
            .last()
            .is_some_and(|(child, _)| link.ticks > child.ticks)
        {
            return Err(reject());
        }
        let same = if link.uid == incarnation.uid && link.pid > 1 {
            let observed = agent_run_platform::process::inspect(
                i32::try_from(link.pid).map_err(|_| reject())?,
            )
            .map_err(|_| reject())?;
            let exact =
                agent_run_platform::process::kernel_incarnation(&observed).map_err(|_| reject())?;
            if exact.boot_id != incarnation.boot_id
                || exact.pid_namespace_inode != incarnation.pid_namespace_inode
                || exact.start_ticks != link.ticks
                || observed.ppid != i32::try_from(link.parent).map_err(|_| reject())?
                || has_launch_marker(link.pid)?
            {
                return Err(reject());
            }
            Some(exact)
        } else {
            None
        };
        next = link.parent;
        chain.push((link, same));
    }
    if chain.first().and_then(|(_, exact)| exact.as_ref()) != Some(&incarnation)
        || chain.last().is_none_or(|(link, _)| link.pid != 1)
    {
        return Err(reject());
    }
    let store = agent_run_store::Store::open(home).map_err(|_| reject())?;
    let snapshot = store.conn.unchecked_transaction().map_err(|_| reject())?;
    for (_, exact) in &chain {
        let Some(exact) = exact else {
            continue;
        };
        let namespace = i64::try_from(exact.pid_namespace_inode).map_err(|_| reject())?;
        let ticks = i64::try_from(exact.start_ticks).map_err(|_| reject())?;
        let token = format!("linux:{}:{}", exact.boot_id, exact.start_ticks);
        let owned:bool = snapshot.query_row("SELECT EXISTS(SELECT 1 FROM attempt_launches WHERE state='bound' AND boot_id=?1 AND pid_namespace_inode=?2 AND uid=?3 AND pid=?4 AND start_ticks=?5 UNION ALL SELECT 1 FROM process_members WHERE owner_kind='attempt' AND pid=?4 AND token=?6)",rusqlite::params![exact.boot_id,namespace,exact.uid,exact.pid,ticks,token],|row|row.get(0)).map_err(|_| reject())?;
        if owned {
            return Err(reject());
        }
    }
    for (link, exact) in &chain {
        if role_link(link.pid)? != *link {
            return Err(reject());
        }
        if let Some(exact) = exact {
            let observed = agent_run_platform::process::inspect(
                i32::try_from(link.pid).map_err(|_| reject())?,
            )
            .map_err(|_| reject())?;
            if agent_run_platform::process::kernel_incarnation(&observed).map_err(|_| reject())?
                != *exact
                || has_launch_marker(link.pid)?
            {
                return Err(reject());
            }
        }
    }
    drop(snapshot);
    Ok(())
}

#[cfg(not(target_os = "linux"))]
pub(crate) fn verify_pool_resident(_: &UnixStream, _: &Path) -> Result<()> {
    Err(Error::Unsupported(
        "attempt transport requires Linux".into(),
    ))
}

/// Check a private, owned socket and its direct directory before connecting.
pub(crate) fn verify_socket(path: &Path, uid: u32) -> Result<()> {
    use std::os::unix::fs::FileTypeExt;
    let reject = || invalid("attempt socket verification failed");
    if !path.is_absolute()
        || path.components().any(|part| {
            matches!(
                part,
                std::path::Component::ParentDir | std::path::Component::CurDir
            )
        })
    {
        return Err(reject());
    }
    let parent = path.parent().ok_or_else(reject)?;
    if parent.canonicalize().map_err(|_| reject())? != parent {
        return Err(reject());
    }
    let directory = std::fs::symlink_metadata(parent).map_err(|_| reject())?;
    let socket = std::fs::symlink_metadata(path).map_err(|_| reject())?;
    if !directory.is_dir()
        || directory.uid() != uid
        || directory.mode() & 0o077 != 0
        || !socket.file_type().is_socket()
        || socket.uid() != uid
        || socket.mode() & 0o077 != 0
    {
        return Err(reject());
    }
    Ok(())
}

/// A recursively duplicate-free JSON value at a bounded transport boundary.
struct Strict(serde_json::Value);
impl<'de> serde::Deserialize<'de> for Strict {
    fn deserialize<D: serde::Deserializer<'de>>(
        deserializer: D,
    ) -> std::result::Result<Self, D::Error> {
        use serde::de::{self, MapAccess, SeqAccess, Visitor};
        struct V;
        impl<'de> Visitor<'de> for V {
            type Value = Strict;
            fn expecting(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                f.write_str("strict JSON")
            }
            fn visit_bool<E: de::Error>(self, v: bool) -> std::result::Result<Strict, E> {
                Ok(Strict(v.into()))
            }
            fn visit_i64<E: de::Error>(self, v: i64) -> std::result::Result<Strict, E> {
                Ok(Strict(v.into()))
            }
            fn visit_u64<E: de::Error>(self, v: u64) -> std::result::Result<Strict, E> {
                Ok(Strict(v.into()))
            }
            fn visit_f64<E: de::Error>(self, v: f64) -> std::result::Result<Strict, E> {
                serde_json::Number::from_f64(v)
                    .map(|n| Strict(n.into()))
                    .ok_or_else(|| E::custom("invalid number"))
            }
            fn visit_str<E: de::Error>(self, v: &str) -> std::result::Result<Strict, E> {
                Ok(Strict(v.into()))
            }
            fn visit_string<E: de::Error>(self, v: String) -> std::result::Result<Strict, E> {
                Ok(Strict(v.into()))
            }
            fn visit_unit<E: de::Error>(self) -> std::result::Result<Strict, E> {
                Ok(Strict(serde_json::Value::Null))
            }
            fn visit_seq<A: SeqAccess<'de>>(
                self,
                mut seq: A,
            ) -> std::result::Result<Strict, A::Error> {
                let mut values = Vec::new();
                while let Some(value) = seq.next_element::<Strict>()? {
                    values.push(value.0)
                }
                Ok(Strict(values.into()))
            }
            fn visit_map<A: MapAccess<'de>>(
                self,
                mut source: A,
            ) -> std::result::Result<Strict, A::Error> {
                let mut values = serde_json::Map::new();
                while let Some(key) = source.next_key::<String>()? {
                    if values.contains_key(&key) {
                        return Err(de::Error::custom("duplicate key"));
                    }
                    let value = source.next_value::<Strict>()?;
                    values.insert(key, value.0);
                }
                Ok(Strict(values.into()))
            }
        }
        deserializer.deserialize_any(V)
    }
}

pub(super) fn strict(bytes: &[u8]) -> Result<serde_json::Value> {
    if bytes.is_empty()
        || bytes.len() > 1024 * 1024
        || bytes.contains(&b'\n')
        || bytes.contains(&b'\r')
    {
        return Err(invalid("attempt frame invalid"));
    }
    // Raw-byte boundary: serde_json otherwise converts overflowing integer
    // tokens to f64. Ignore quoted strings and retain legitimate float tokens.
    let mut i = 0;
    let mut quoted = false;
    let mut escaped = false;
    while i < bytes.len() {
        let b = bytes[i];
        if quoted {
            if escaped {
                escaped = false;
            } else if b == b'\\' {
                escaped = true;
            } else if b == b'"' {
                quoted = false;
            }
            i += 1;
            continue;
        }
        if b == b'"' {
            quoted = true;
            i += 1;
            continue;
        }
        if b == b'-' || b.is_ascii_digit() {
            let start = i;
            i += 1;
            while i < bytes.len()
                && (bytes[i].is_ascii_digit()
                    || matches!(bytes[i], b'.' | b'e' | b'E' | b'+' | b'-'))
            {
                i += 1;
            }
            let token = &bytes[start..i];
            if !token.iter().any(|c| matches!(c, b'.' | b'e' | b'E')) {
                let value = std::str::from_utf8(token)
                    .ok()
                    .and_then(|s| s.parse::<i128>().ok())
                    .ok_or_else(|| invalid("attempt frame invalid"))?;
                if value < i128::from(i64::MIN) || value > i128::from(u64::MAX) {
                    return Err(invalid("attempt frame invalid"));
                }
            }
            continue;
        }
        i += 1;
    }
    let value: Strict =
        serde_json::from_slice(bytes).map_err(|_| invalid("attempt frame invalid"))?;
    if !value.0.is_object() {
        return Err(invalid("attempt frame invalid"));
    }
    Ok(value.0)
}

/// One configured peer. Never `Debug`: it may be associated with credentials.
pub(crate) struct Endpoint {
    socket: PathBuf,
    pin: PeerPin,
}
impl Endpoint {
    /// Build only from validated operator configuration, never request data.
    pub(crate) fn configured(socket: PathBuf, pin: PeerPin) -> Result<Self> {
        if !socket.is_absolute() || !pin.executable.is_absolute() || pin.sha256.len() != 64 {
            return Err(invalid("attempt endpoint configuration invalid"));
        }
        Ok(Self { socket, pin })
    }
    async fn exchange(
        &self,
        value: &serde_json::Value,
        pool_home: Option<&Path>,
    ) -> Result<serde_json::Value> {
        use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
        const LIMIT: u64 = 1024 * 1024;
        let mut bytes = serde_json::to_vec(value)?;
        if bytes.len() + 1 > LIMIT as usize {
            return Err(invalid("attempt frame invalid"));
        }
        bytes.push(b'\n');
        let work = async {
            verify_socket(&self.socket, self.pin.uid)?;
            let stream = UnixStream::connect(&self.socket)
                .await
                .map_err(|_| invalid("attempt peer unavailable"))?;
            let pin = PeerPin {
                uid: self.pin.uid,
                executable: self.pin.executable.clone(),
                sha256: self.pin.sha256.clone(),
            };
            let pool_home = pool_home.map(Path::to_path_buf);
            let mut stream = tokio::task::spawn_blocking(move || {
                verify_peer(&stream, &pin)?;
                if let Some(home) = pool_home.as_deref() {
                    verify_pool_resident(&stream, home)
                        .map_err(|_| crate::attempt_wire::peer_not_authorized())?;
                }
                Ok::<_, Error>(stream)
            })
            .await
            .map_err(|_| invalid("attempt peer unavailable"))??;
            stream
                .write_all(&bytes)
                .await
                .map_err(|_| invalid("attempt peer unavailable"))?;
            let mut response = Vec::new();
            BufReader::new(stream)
                .take(LIMIT + 1)
                .read_until(b'\n', &mut response)
                .await
                .map_err(|_| invalid("attempt peer unavailable"))?;
            if response.len() as u64 > LIMIT || response.pop() != Some(b'\n') {
                return Err(invalid("attempt frame invalid"));
            }
            strict(&response)
        };
        tokio::time::timeout(std::time::Duration::from_secs(5), work)
            .await
            .map_err(|_| invalid("attempt peer unavailable"))?
    }
}

/// Configured Bridge reader. `credential` is read only from a protected grant by
/// its owner and is never serialized to storage or printed by this type.
pub(crate) struct AuthenticatedBridge {
    endpoint: Endpoint,
    credential: String,
    route: agent_run_domain::attempt_binding::Route,
}
/// Configured Pool Run-reader endpoint; peer grant is verified by Pool.
pub(crate) struct AuthenticatedPool {
    endpoint: Endpoint,
    route: agent_run_domain::attempt_binding::Route,
    home: PathBuf,
}

impl AuthenticatedBridge {
    /// Read only the pinned private reader credential; the fingerprint is
    /// configured out of band and a changed or missing file fails closed.
    pub(crate) fn from_pinned_file(
        endpoint: Endpoint,
        route: agent_run_domain::attempt_binding::Route,
        path: &Path,
        expected_sha256: &str,
    ) -> Result<Self> {
        let fail = || invalid("attempt reader credential unavailable");
        route.validate()?;
        if route.worker_uid != endpoint.pin.uid {
            return Err(fail());
        }
        if !path.is_absolute() || expected_sha256.len() != 64 {
            return Err(fail());
        }
        let parent = path.parent().ok_or_else(fail)?;
        if parent.canonicalize().map_err(|_| fail())? != parent {
            return Err(fail());
        }
        let directory = agent_run_platform::fs::Dir::open_searchable(parent).map_err(|_| fail())?;
        let entry = directory.entry(None).map_err(|_| fail())?;
        let name = path.file_name().ok_or_else(fail)?;
        let file = directory.open_file(Path::new(name)).map_err(|_| fail())?;
        let metadata = file.metadata().map_err(|_| fail())?;
        if entry.uid != endpoint.pin.uid
            || entry.mode & 0o077 != 0
            || metadata.uid() != endpoint.pin.uid
            || !metadata.is_file()
            || metadata.mode() & 0o077 != 0
            || metadata.nlink() != 1
            || metadata.len() > 16384
        {
            return Err(fail());
        }
        let mut bytes = Vec::new();
        file.take(16385)
            .read_to_end(&mut bytes)
            .map_err(|_| fail())?;
        if bytes.len() > 16384 {
            return Err(fail());
        }
        let text = String::from_utf8(bytes).map_err(|_| fail())?;
        let credential = text.trim_end_matches(['\r', '\n']).to_string();
        if credential.is_empty()
            || agent_run_platform::fs::sha256(credential.as_bytes()) != expected_sha256
        {
            return Err(fail());
        }
        Ok(Self {
            endpoint,
            credential,
            route,
        })
    }
}
impl AuthenticatedPool {
    /// Pin the Pool store/route to the already configured endpoint.
    pub(crate) fn configured(
        endpoint: Endpoint,
        route: agent_run_domain::attempt_binding::Route,
        home: PathBuf,
    ) -> Result<Self> {
        route.validate()?;
        if route.worker_uid != endpoint.pin.uid {
            return Err(invalid("attempt endpoint configuration invalid"));
        }
        Ok(Self {
            endpoint,
            route,
            home,
        })
    }
}

fn peer_error(value: &serde_json::Value, expected_store: Option<&str>) -> Error {
    let Some(error) = value.as_object() else {
        return invalid("attempt response invalid");
    };
    if error.len() != 4
        || error.get("version") != Some(&serde_json::json!(1))
        || error
            .get("store_id")
            .and_then(|v| v.as_str())
            .is_none_or(str::is_empty)
        || expected_store
            .is_some_and(|store| error.get("store_id").and_then(|v| v.as_str()) != Some(store))
    {
        return invalid("attempt response invalid");
    }
    let Some(code) = error.get("code").and_then(|v| v.as_str()) else {
        return invalid("attempt response invalid");
    };
    let Some(recovery) = error.get("recovery").and_then(|v| v.as_str()) else {
        return invalid("attempt response invalid");
    };
    let valid = matches!(
        (code, recovery),
        (
            "unsupported_version"
                | "capability_missing"
                | "transport_unsupported"
                | "role_unqualified"
                | "peer_not_authorized"
                | "audience_mismatch"
                | "attempt_mismatch"
                | "actor_mismatch"
                | "route_mismatch"
                | "intent_conflict"
                | "aborted",
            "none"
        ) | (
            "assertion_stale"
                | "assertion_historical"
                | "invocation_spent"
                | "binding_unavailable"
                | "attempt_not_ready",
            "fresh_invocation"
        ) | (
            "attempt_not_ready" | "request_in_progress" | "outcome_unknown",
            "same_key_lookup"
        ) | (
            "recovery_required" | "capacity_exhausted" | "binding_unavailable",
            "operator"
        ) | ("binding_unavailable", "same_key_lookup" | "none")
    );
    if !valid {
        return invalid("attempt response invalid");
    }
    Error::Broker {
        message: "attempt peer refused".into(),
        broker_error_code: Some(code.into()),
        broker_error_data: Some(serde_json::json!({"recovery":recovery})),
    }
}

fn bridge_result(
    value: serde_json::Value,
    id: &str,
    operation: &str,
    expected_store: Option<&str>,
) -> Result<serde_json::Value> {
    let object = value
        .as_object()
        .ok_or_else(|| invalid("attempt response invalid"))?;
    if object.len() != 4
        || object.get("api_version") != Some(&serde_json::json!(1))
        || object.get("request_id").and_then(|v| v.as_str()) != Some(id)
        || object.get("success") != Some(&serde_json::json!(true))
    {
        return Err(invalid("attempt response invalid"));
    }
    let result = object
        .get("result")
        .and_then(|v| v.as_object())
        .ok_or_else(|| invalid("attempt response invalid"))?;
    if result.len() != 2 || result.get("op").and_then(|v| v.as_str()) != Some(operation) {
        return Err(invalid("attempt response invalid"));
    }
    let tagged = result
        .get("value")
        .and_then(|v| v.as_object())
        .ok_or_else(|| invalid("attempt response invalid"))?;
    if tagged.len() != 2 {
        return Err(invalid("attempt response invalid"));
    }
    match tagged.get("status").and_then(|v| v.as_str()) {
        Some("ok") => tagged
            .get("value")
            .cloned()
            .ok_or_else(|| invalid("attempt response invalid")),
        Some("error") => Err(peer_error(
            tagged
                .get("error")
                .ok_or_else(|| invalid("attempt response invalid"))?,
            expected_store,
        )),
        _ => Err(invalid("attempt response invalid")),
    }
}
fn pool_result(
    value: serde_json::Value,
    id: &str,
    expected_store: Option<&str>,
) -> Result<serde_json::Value> {
    let object = value
        .as_object()
        .ok_or_else(|| invalid("attempt response invalid"))?;
    if object.len() != 2 || object.get("id").and_then(|v| v.as_str()) != Some(id) {
        return Err(invalid("attempt response invalid"));
    }
    if let Some(result) = object.get("result") {
        return Ok(result.clone());
    }
    if let Some(error) = object.get("error") {
        return Err(peer_error(error, expected_store));
    }
    Err(invalid("attempt response invalid"))
}

impl AuthenticatedBridge {
    async fn call(&self, operation: &str, params: serde_json::Value) -> Result<serde_json::Value> {
        let id = uuid::Uuid::new_v4().to_string();
        let expected = params
            .get("expected_store_id")
            .and_then(|v| v.as_str())
            .map(str::to_owned);
        let request = serde_json::json!({"api_version":1,"request_id":id,"credential":self.credential,"op":operation,"params":params});
        bridge_result(
            self.endpoint.exchange(&request, None).await?,
            &id,
            operation,
            expected.as_deref(),
        )
    }
}
impl AuthenticatedPool {
    async fn call(&self, method: &str, params: serde_json::Value) -> Result<serde_json::Value> {
        let id = uuid::Uuid::new_v4().to_string();
        let expected = params
            .get("expected_store_id")
            .and_then(|v| v.as_str())
            .map(str::to_owned);
        let request = serde_json::json!({"id":id,"method":method,"params":params});
        pool_result(
            self.endpoint.exchange(&request, Some(&self.home)).await?,
            &id,
            expected.as_deref(),
        )
    }
}

fn object_at(value: &serde_json::Value, pointer: &str) -> Result<()> {
    if value
        .pointer(pointer)
        .is_some_and(serde_json::Value::is_object)
    {
        Ok(())
    } else {
        Err(invalid("attempt response invalid"))
    }
}
fn decode<T: serde::de::DeserializeOwned>(value: serde_json::Value) -> Result<T> {
    if !value.is_object() {
        return Err(invalid("attempt response invalid"));
    }
    serde_json::from_value(value).map_err(|_| invalid("attempt response invalid"))
}
impl super::attempt_binding::BridgeReader for AuthenticatedBridge {
    fn actor_recovery<'a>(
        &'a self,
        route: &'a agent_run_domain::attempt_binding::Route,
        producer: &'a str,
        incarnation: &'a agent_run_domain::attempt_binding::Incarnation,
        launch_id: &'a str,
    ) -> super::attempt_binding::PeerFuture<'a, super::attempt_binding::BridgeActorRecovery> {
        Box::pin(async move {
            if *route != self.route {
                return Err(Error::Conflict);
            }
            let value = self
                .call(
                    "run_actor_recovery_status_v1",
                    serde_json::json!({
                        "version":1,"expected_store_id":route.component_store_id,
                        "producer":producer,"incarnation":incarnation,"launch_id":launch_id
                    }),
                )
                .await?;
            object_at(&value, "/incarnation")?;
            decode(value)
        })
    }
    fn actor<'a>(
        &'a self,
        route: &'a agent_run_domain::attempt_binding::Route,
        producer: &'a str,
        incarnation: &'a agent_run_domain::attempt_binding::Incarnation,
        launch_id: &'a str,
    ) -> super::attempt_binding::PeerFuture<'a, super::attempt_binding::BridgeActor> {
        Box::pin(async move {
            if *route != self.route {
                return Err(Error::Conflict);
            }
            let value = self
                .call(
                    "run_actor_lookup_v1",
                    serde_json::json!({
                        "version":1,"expected_store_id":route.component_store_id,
                        "producer":producer,"incarnation":incarnation,"launch_id":launch_id
                    }),
                )
                .await?;
            object_at(&value, "/incarnation")?;
            if value.get("actor").is_none() || value.get("issuer_epoch").is_none() {
                return Err(invalid("attempt response invalid"));
            }
            decode(value)
        })
    }
    fn context<'a>(
        &'a self,
        request: &'a agent_run_domain::attempt_binding::IngressCurrent,
    ) -> super::attempt_binding::PeerFuture<'a, super::attempt_binding::BridgeContext> {
        Box::pin(async move {
            if request.bridge_route != self.route {
                return Err(Error::Conflict);
            }
            let result=self.call("context_lookup_v1",serde_json::json!({"version":1,"expected_store_id":request.bridge_route.component_store_id,"producer":request.producer,"bridge_context_ref":request.bridge_context_ref,"invocation_key":request.invocation_key})).await?;
            object_at(&result, "/observation")?;
            object_at(&result, "/observation/incarnation")?;
            object_at(&result, "/observation/actor")?;
            decode(result)
        })
    }
    fn consume<'a>(
        &'a self,
        request: &'a agent_run_domain::attempt_binding::AdmissionRequest,
    ) -> super::attempt_binding::PeerFuture<'a, super::attempt_binding::BridgeConsume> {
        Box::pin(async move {
            let source = &request.consumed_ref;
            if source.bridge_route != self.route {
                return Err(Error::Conflict);
            }
            let result=self.call("observation_lookup_v1",serde_json::json!({"version":1,"expected_store_id":source.bridge_route.component_store_id,"producer":source.producer,"consume_request_id":source.consume_request_id,"invocation_key":source.invocation_key,"security_digest":source.security_digest})).await?;
            for path in [
                "/receipt",
                "/receipt/observation",
                "/receipt/observation/incarnation",
                "/receipt/observation/actor",
            ] {
                object_at(&result, path)?;
            }
            decode(result)
        })
    }
    fn issuer<'a>(
        &'a self,
        request: &'a agent_run_store::attempt_binding::NamespaceRequest,
    ) -> super::attempt_binding::PeerFuture<'a, super::attempt_binding::BridgeIssuer> {
        Box::pin(async move {
            if request.bridge_route != self.route {
                return Err(Error::Conflict);
            }
            decode(
                self.call(
                    "attempt_binding_capabilities_v1",
                    serde_json::json!({"version":1}),
                )
                .await?,
            )
        })
    }
    fn retire<'a>(
        &'a self,
        request: &'a agent_run_store::attempt_binding::NamespaceRequest,
    ) -> super::attempt_binding::PeerFuture<'a, super::attempt_binding::BridgeRetired> {
        Box::pin(async move {
            if request.bridge_route != self.route {
                return Err(Error::Conflict);
            }
            decode(self.call("retire_attempt_namespace_v1",serde_json::json!({"version":1,"expected_store_id":request.bridge_route.component_store_id,"producer":request.producer,"issuer_epoch":request.issuer_epoch,"retirement_key":request.retirement_key})).await?)
        })
    }
}
impl super::attempt_binding::PoolReader for AuthenticatedPool {
    fn outcome<'a>(
        &'a self,
        route: &'a agent_run_domain::attempt_binding::Route,
        intent: &'a str,
        digest: &'a str,
    ) -> super::attempt_binding::PeerFuture<'a, super::attempt_binding::PoolOutcome> {
        Box::pin(async move {
            if *route != self.route {
                return Err(Error::Conflict);
            }
            let result=self.call("_managed_intent_outcome_v1",serde_json::json!({"version":1,"expected_store_id":route.component_store_id,"intent_id":intent,"security_digest":digest})).await?;
            if result.get("outcome_digest").is_none() {
                return Err(invalid("attempt response invalid"));
            }
            decode(result)
        })
    }
    fn seal<'a>(
        &'a self,
        request: &'a agent_run_store::attempt_binding::NamespaceRequest,
    ) -> super::attempt_binding::PeerFuture<'a, agent_run_store::attempt_binding::PoolSeal> {
        Box::pin(async move {
            if request.pool_route != self.route {
                return Err(Error::Conflict);
            }
            let result=self.call("_managed_namespace_seal_v1",serde_json::json!({"version":1,"expected_store_id":request.pool_route.component_store_id,"request_id":request.pool_seal_request_id,"bridge_route":request.bridge_route,"producer":request.producer,"issuer_epoch":request.issuer_epoch,"retirement_key":request.retirement_key})).await?;
            object_at(&result, "/bridge_route")?;
            decode(result)
        })
    }
}

#[cfg(all(test, target_os = "linux"))]
mod tests {
    use super::*;
    use crate::attempt_binding::{BridgeReader, PoolReader};

    #[test]
    fn strict_frames_refuse_duplicates_and_bad_envelopes() {
        assert!(strict(br#"{"x":{"a":1,"a":2}}"#).is_err());
        assert!(strict(br#"{"x":1} {"x":2}"#).is_err());
        assert!(strict(br#"[]"#).is_err());
        assert!(strict(br#"{"x":18446744073709551616}"#).is_err());
        assert!(strict(br#"{"x":-9223372036854775809}"#).is_err());
        assert!(strict(br#"{"x":1e20}"#).is_ok());
        let valid = serde_json::json!({"api_version":1,"request_id":"id","success":true,"result":{"op":"lookup","value":{"status":"ok","value":{"version":1}}}});
        assert!(bridge_result(valid.clone(), "id", "lookup", None).is_ok());
        assert!(bridge_result(valid.clone(), "wrong", "lookup", None).is_err());
        assert!(bridge_result(valid, "id", "retire", None).is_err());
    }

    #[test]
    fn peer_proc_filters_reject_loader_and_tracer_evidence() {
        assert!(clean_peer_environment(b"PATH=/fixture\0NAME=value\0"));
        assert!(!clean_peer_environment(b"LD_PRELOAD=/fixture\0"));
        assert!(!clean_peer_environment(b"LD_PRELOAD\0"));
        assert!(!clean_peer_environment(b"LD_LIBRARY_PATH=\0"));
        assert!(untraced("Name:\tx\nTracerPid:\t0\n"));
        assert!(!untraced("TracerPid:\t42\n"));
        assert!(!untraced("TracerPid:\t0\nTracerPid:\t0\n"));
        assert!(interpreter(Path::new("/usr/bin/python3.11")));
        assert!(interpreter(Path::new("/usr/bin/bash")));
        let python = Path::new("/usr/bin/python3");
        if let Ok(bytes) = std::fs::read(python) {
            assert!(
                agent_run_config::provider_config::forbidden_interpreter_build(
                    Path::new("/synthetic/renamed-peer"),
                    &agent_run_platform::fs::sha256(&bytes),
                    bytes.len() as u64,
                )
            );
        }
    }

    #[tokio::test]
    async fn verify_peer_rejects_uid_hash_permissions_replacement_and_ld_preload() {
        use std::os::unix::fs::PermissionsExt;
        let base = std::env::var_os("AGENT_RUN_TEST_SOCKET_TMP")
            .map(PathBuf::from)
            .unwrap_or_else(std::env::temp_dir);
        let dir = tempfile::Builder::new()
            .prefix("r2-auth-")
            .tempdir_in(base)
            .unwrap();
        std::fs::set_permissions(dir.path(), std::fs::Permissions::from_mode(0o700)).unwrap();
        let executable = dir.path().join("fixture-peer");
        std::fs::copy(std::env::current_exe().unwrap(), &executable).unwrap();
        std::fs::set_permissions(&executable, std::fs::Permissions::from_mode(0o700)).unwrap();
        let sha = agent_run_platform::fs::sha256(&std::fs::read(&executable).unwrap());
        super::test_peer::wait_build_age(&executable).await;
        // SAFETY: geteuid has no arguments or side effects.
        let uid = unsafe { libc::geteuid() };
        for case in [
            "valid",
            "wrong_uid",
            "wrong_hash",
            "ld_preload",
            "group_writable",
            "replaced",
        ] {
            if case == "group_writable" {
                std::fs::set_permissions(&executable, std::fs::Permissions::from_mode(0o770))
                    .unwrap();
                super::test_peer::wait_build_age(&executable).await;
            }
            let socket = dir.path().join(format!("{case}.sock"));
            let ready = dir.path().join(format!("{case}.ready"));
            let listener = tokio::net::UnixListener::bind(&socket).unwrap();
            let mut command = super::test_peer::child_at(&executable, "hold-client", &socket);
            command.env("RUN_R2_PEER_READY", &ready);
            if case == "ld_preload" {
                command.env("LD_PRELOAD", "/nonexistent/synthetic-preload.so");
            }
            let mut child = command.spawn().unwrap();
            let (stream, _) =
                tokio::time::timeout(std::time::Duration::from_secs(5), listener.accept())
                    .await
                    .unwrap()
                    .unwrap();
            super::test_peer::wait_file(&ready).await;
            if case == "replaced" {
                std::fs::rename(&executable, dir.path().join("previous-peer")).unwrap();
                std::fs::copy(std::env::current_exe().unwrap(), &executable).unwrap();
                std::fs::set_permissions(&executable, std::fs::Permissions::from_mode(0o700))
                    .unwrap();
            }
            let pin = PeerPin {
                uid: if case == "wrong_uid" {
                    uid.wrapping_add(1)
                } else {
                    uid
                },
                executable: executable.clone(),
                sha256: if case == "wrong_hash" {
                    "0".repeat(64)
                } else {
                    sha.clone()
                },
            };
            assert_eq!(
                verify_peer(&stream, &pin).is_ok(),
                case == "valid",
                "{case}"
            );
            drop(stream);
            assert!(child.wait().await.unwrap().success(), "{case}");
            if case == "group_writable" {
                std::fs::set_permissions(&executable, std::fs::Permissions::from_mode(0o700))
                    .unwrap();
                super::test_peer::wait_build_age(&executable).await;
            }
        }
    }

    /// An in-place build upgrade must admit immediate new/resumed launches,
    /// while the old launch's frozen pin and the old loaded inode refuse.
    #[tokio::test]
    async fn in_place_bridge_upgrade_preserves_launch_pins_without_second_age_floor() {
        use std::{io::Write, os::unix::fs::PermissionsExt};
        let dir = tempfile::Builder::new()
            .prefix("r2-upgrade-")
            .tempdir()
            .unwrap();
        std::fs::set_permissions(dir.path(), std::fs::Permissions::from_mode(0o700)).unwrap();
        let executable = super::test_peer::copy_peer(dir.path());
        // SAFETY: geteuid is read-only with no preconditions.
        let uid = unsafe { libc::geteuid() };
        let old_pin = PeerPin {
            uid,
            executable: executable.clone(),
            sha256: agent_run_platform::fs::sha256(&std::fs::read(&executable).unwrap()),
        };
        tokio::time::sleep(std::time::Duration::from_millis(30)).await;
        let socket = dir.path().join("old.sock");
        let ready = dir.path().join("old.ready");
        let listener = tokio::net::UnixListener::bind(&socket).unwrap();
        let mut old = super::test_peer::child_at(&executable, "hold-client", &socket)
            .env("RUN_R2_PEER_READY", &ready)
            .spawn()
            .unwrap();
        let (old_stream, _) = listener.accept().await.unwrap();
        super::test_peer::wait_file(&ready).await;
        assert!(verify_peer(&old_stream, &old_pin).is_ok());
        let replacement = dir.path().join("replacement");
        std::fs::copy(std::env::current_exe().unwrap(), &replacement).unwrap();
        // A second valid ELF build with a distinct digest; the loader ignores
        // trailing bytes. Atomically replace the exact pinned executable path.
        std::fs::OpenOptions::new()
            .append(true)
            .open(&replacement)
            .unwrap()
            .write_all(b"upgrade-build-2")
            .unwrap();
        std::fs::set_permissions(&replacement, std::fs::Permissions::from_mode(0o700)).unwrap();
        std::fs::rename(&replacement, &executable).unwrap();
        let new_pin = PeerPin {
            uid,
            executable: executable.clone(),
            sha256: agent_run_platform::fs::sha256(&std::fs::read(&executable).unwrap()),
        };
        assert_ne!(new_pin.sha256, old_pin.sha256);
        assert!(verify_peer(&old_stream, &new_pin).is_err());
        drop(old_stream);
        assert!(old.wait().await.unwrap().success());
        for launch in ["resumed", "new"] {
            let socket = dir.path().join(format!("{launch}.sock"));
            let ready = dir.path().join(format!("{launch}.ready"));
            let listener = tokio::net::UnixListener::bind(&socket).unwrap();
            let mut child = super::test_peer::child_at(&executable, "hold-client", &socket)
                .env("RUN_R2_PEER_READY", &ready)
                .spawn()
                .unwrap();
            let (stream, _) = listener.accept().await.unwrap();
            super::test_peer::wait_file(&ready).await;
            assert!(verify_peer(&stream, &new_pin).is_ok(), "{launch}");
            assert!(verify_peer(&stream, &old_pin).is_err(), "stale launch pin");
            // Metadata-only changes after start do not change loaded-build authority.
            std::fs::set_permissions(&executable, std::fs::Permissions::from_mode(0o700)).unwrap();
            assert!(verify_peer(&stream, &new_pin).is_ok());
            // Inject wall timestamps on either side of the old inferred start,
            // including the +60s clock-step counterexample. Neither affects proof.
            for seconds in [1, 1001, 1060, 4_000_000_000] {
                std::fs::File::open(&executable)
                    .unwrap()
                    .set_times(std::fs::FileTimes::new().set_modified(
                        std::time::UNIX_EPOCH + std::time::Duration::from_secs(seconds),
                    ))
                    .unwrap();
                assert!(
                    verify_peer(&stream, &new_pin).is_ok(),
                    "timestamp {seconds}"
                );
            }
            // Current permissions still refuse a post-start group-write grant.
            std::fs::set_permissions(&executable, std::fs::Permissions::from_mode(0o770)).unwrap();
            assert!(verify_peer(&stream, &new_pin).is_err());
            std::fs::set_permissions(&executable, std::fs::Permissions::from_mode(0o700)).unwrap();
            drop(stream);
            assert!(child.wait().await.unwrap().success());
            tokio::time::sleep(std::time::Duration::from_millis(30)).await;
        }
    }

    /// A real same-UID local fake process exercises peer UID/build pinning and
    /// one complete Pool NDJSON request; no running services are contacted.
    #[tokio::test]
    async fn fake_pool_reader_authenticates_peer_and_exact_namespace() {
        let base = std::env::var_os("AGENT_RUN_TEST_SOCKET_TMP")
            .map(PathBuf::from)
            .unwrap_or_else(std::env::temp_dir);
        let dir = tempfile::Builder::new()
            .prefix("r2-")
            .tempdir_in(base)
            .unwrap();
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(dir.path(), std::fs::Permissions::from_mode(0o700)).unwrap();
        agent_run_store::fixture::initialize(dir.path()).unwrap();
        let socket = dir.path().join("pool.sock");
        let executable = super::test_peer::copy_peer(dir.path());
        let sha = agent_run_platform::fs::sha256(&std::fs::read(&executable).unwrap());
        super::test_peer::wait_build_age(&executable).await;
        let ready = dir.path().join("ready");
        let mut child = super::test_peer::child_at(&executable, "pool-server", &socket)
            .env("RUN_R2_PEER_READY", &ready)
            .spawn()
            .unwrap();
        super::test_peer::wait_file(&ready).await;
        // SAFETY: geteuid has no arguments or memory effects.
        let uid = unsafe { libc::geteuid() };
        let route = |store: &str, id: &str| agent_run_domain::attempt_binding::Route {
            component_store_id: store.into(),
            route_id: id.into(),
            worker_uid: uid,
        };
        let store: agent_run_domain::StoreId =
            "store_0123456789abcdef0123456789abcdef".parse().unwrap();
        let request = agent_run_store::attempt_binding::NamespaceRequest {
            expected_store_id: store,
            request_id: "retire".into(),
            pool_route: route("pool-store", "pool"),
            bridge_route: route("00000000-0000-4000-8000-000000000001", "bridge"),
            producer: "pool".into(),
            issuer_epoch: "old".into(),
            retirement_key: "key".into(),
            pool_seal_request_id: "seal-request".into(),
        };
        let client = AuthenticatedPool {
            route: request.pool_route.clone(),
            home: dir.path().to_path_buf(),
            endpoint: Endpoint {
                socket,
                pin: PeerPin {
                    uid,
                    executable,
                    sha256: sha,
                },
            },
        };
        let reply = client.seal(&request).await.unwrap();
        assert!(reply.matches_request(&request));
        assert_eq!(reply.state, "sealed");
        assert!(child.wait().await.unwrap().success());

        // A same-build fake underneath an observed Run harness cannot be used
        // as the outbound Pool resident even when its socket and hash match.
        let leader = agent_run_platform::process::inspect(std::process::id() as i32).unwrap();
        let kernel = agent_run_platform::process::kernel_incarnation(&leader).unwrap();
        let store = agent_run_store::fixture::open(dir.path()).unwrap();
        store.conn.execute("INSERT INTO attempt_launches(spawn_key,execution_id,sequence,attempt_id,launch_id,state,boot_id,pid_namespace_inode,uid,pid,start_ticks) VALUES('harness','root',1,'attempt',?,'bound',?,?,?,?,?)",rusqlite::params![uuid::Uuid::new_v4().to_string(),kernel.boot_id,kernel.pid_namespace_inode,kernel.uid,kernel.pid,kernel.start_ticks]).unwrap();
        drop(store);
        std::fs::remove_file(&client.endpoint.socket).unwrap();
        std::fs::remove_file(&ready).unwrap();
        let mut child = super::test_peer::child_at(
            &client.endpoint.pin.executable,
            "pool-server",
            &client.endpoint.socket,
        )
        .env("RUN_R2_PEER_READY", &ready)
        .spawn()
        .unwrap();
        super::test_peer::wait_file(&ready).await;
        let error = client.seal(&request).await.err().unwrap();
        assert!(
            matches!(error,Error::Broker { broker_error_code:Some(ref code), .. } if code == "peer_not_authorized")
        );
        let _ = child.kill().await;
    }

    #[tokio::test]
    async fn bridge_actor_lookup_uses_pinned_route_and_exact_incarnation() {
        use std::os::unix::fs::PermissionsExt;
        let base = std::env::var_os("AGENT_RUN_TEST_SOCKET_TMP")
            .map(PathBuf::from)
            .unwrap_or_else(std::env::temp_dir);
        let dir = tempfile::Builder::new()
            .prefix("r3-actor-")
            .tempdir_in(base)
            .unwrap();
        std::fs::set_permissions(dir.path(), std::fs::Permissions::from_mode(0o700)).unwrap();
        let socket = dir.path().join("bridge.sock");
        let executable = super::test_peer::copy_peer(dir.path());
        let sha = agent_run_platform::fs::sha256(&std::fs::read(&executable).unwrap());
        super::test_peer::wait_build_age(&executable).await;
        let ready = dir.path().join("ready");
        let mut child = super::test_peer::child_at(&executable, "bridge-server", &socket)
            .env("RUN_R2_PEER_READY", &ready)
            .spawn()
            .unwrap();
        super::test_peer::wait_file(&ready).await;
        // SAFETY: geteuid is read-only and has no preconditions.
        let uid = unsafe { libc::geteuid() };
        let route = agent_run_domain::attempt_binding::Route {
            component_store_id: "00000000-0000-4000-8000-000000000001".into(),
            route_id: "bridge".into(),
            worker_uid: uid,
        };
        let incarnation = agent_run_domain::attempt_binding::Incarnation {
            host_id: "host".into(),
            boot_id: "00000000-0000-4000-8000-000000000002".into(),
            pid_namespace_inode: 1,
            uid,
            pid: 2,
            start_ticks: 1,
        };
        let client = AuthenticatedBridge {
            endpoint: Endpoint {
                socket,
                pin: PeerPin {
                    uid,
                    executable,
                    sha256: sha,
                },
            },
            credential: "synthetic".into(),
            route: route.clone(),
        };
        let result = client
            .actor(
                &route,
                "pool",
                &incarnation,
                "00000000-0000-4000-8000-000000000003",
            )
            .await
            .unwrap();
        assert_eq!(result.state, "current");
        assert_eq!(result.incarnation, incarnation);
        assert_eq!(result.actor.unwrap().native_id, "native");
        assert!(child.wait().await.unwrap().success());
    }

    #[tokio::test]
    async fn bridge_actor_recovery_uses_pinned_reader_and_exact_echo() {
        use std::os::unix::fs::PermissionsExt;
        let base = std::env::var_os("AGENT_RUN_TEST_SOCKET_TMP")
            .map(PathBuf::from)
            .unwrap_or_else(std::env::temp_dir);
        let dir = tempfile::Builder::new()
            .prefix("p2-recovery-")
            .tempdir_in(base)
            .unwrap();
        std::fs::set_permissions(dir.path(), std::fs::Permissions::from_mode(0o700)).unwrap();
        let socket = dir.path().join("bridge.sock");
        let executable = super::test_peer::copy_peer(dir.path());
        let sha = agent_run_platform::fs::sha256(&std::fs::read(&executable).unwrap());
        super::test_peer::wait_build_age(&executable).await;
        let ready = dir.path().join("ready");
        let mut child = super::test_peer::child_at(&executable, "bridge-recovery", &socket)
            .env("RUN_R2_PEER_READY", &ready)
            .spawn()
            .unwrap();
        super::test_peer::wait_file(&ready).await;
        // SAFETY: geteuid is read-only and has no preconditions.
        let uid = unsafe { libc::geteuid() };
        let route = agent_run_domain::attempt_binding::Route {
            component_store_id: "00000000-0000-4000-8000-000000000001".into(),
            route_id: "bridge".into(),
            worker_uid: uid,
        };
        let incarnation = agent_run_domain::attempt_binding::Incarnation {
            host_id: "host".into(),
            boot_id: "00000000-0000-4000-8000-000000000002".into(),
            pid_namespace_inode: 1,
            uid,
            pid: 2,
            start_ticks: 1,
        };
        let client = AuthenticatedBridge {
            endpoint: Endpoint {
                socket,
                pin: PeerPin {
                    uid,
                    executable,
                    sha256: sha,
                },
            },
            credential: "synthetic".into(),
            route: route.clone(),
        };
        let result = client
            .actor_recovery(
                &route,
                "pool",
                &incarnation,
                "00000000-0000-4000-8000-000000000003",
            )
            .await
            .unwrap();
        assert_eq!(
            result.state,
            crate::attempt_binding::ActorRecoveryState::AwaitingRootHook
        );
        assert_eq!(result.incarnation, incarnation);
        assert_eq!(result.launch_id, "00000000-0000-4000-8000-000000000003");
        assert!(child.wait().await.unwrap().success());
    }
}

#[cfg(all(test, target_os = "linux"))]
pub(crate) mod test_peer {
    use serde_json::{Value, json};
    use std::{
        io::{BufRead, Read, Write},
        os::unix::net::{UnixListener, UnixStream},
        path::Path,
        time::{Duration, SystemTime, UNIX_EPOCH},
    };
    const MODE: &str = "RUN_R2_FAKE_PEER_MODE";
    /// Cargo may create group-writable binaries; use a private pinned fixture.
    pub(crate) fn copy_peer(dir: &Path) -> std::path::PathBuf {
        use std::os::unix::fs::PermissionsExt;
        let target = dir.join("fixture-peer");
        std::fs::copy(std::env::current_exe().unwrap(), &target).unwrap();
        std::fs::set_permissions(&target, std::fs::Permissions::from_mode(0o700)).unwrap();
        target.canonicalize().unwrap()
    }
    /// Called by a child test binary with an empty inherited environment.
    #[test]
    fn synthetic_peer_child() {
        let Ok(mode) = std::env::var(MODE) else {
            return;
        };
        let socket = std::env::var_os("RUN_R2_PEER_SOCKET").unwrap();
        if mode == "pool-server" || mode == "bridge-server" || mode == "bridge-recovery" {
            let listener = UnixListener::bind(&socket).unwrap();
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(&socket, std::fs::Permissions::from_mode(0o600)).unwrap();
            std::fs::write(std::env::var_os("RUN_R2_PEER_READY").unwrap(), b"ready").unwrap();
            let (mut stream, _) = listener.accept().unwrap();
            let mut line = String::new();
            std::io::BufReader::new(&mut stream)
                .read_line(&mut line)
                .unwrap();
            let request: Value = serde_json::from_str(&line).unwrap();
            let p = &request["params"];
            let response = if mode == "bridge-recovery" {
                assert_eq!(request["op"], "run_actor_recovery_status_v1");
                json!({"api_version":1,"request_id":request["request_id"],"success":true,"result":{"op":"run_actor_recovery_status_v1","value":{"status":"ok","value":{"version":1,"store_id":p["expected_store_id"],"producer":p["producer"],"incarnation":p["incarnation"],"launch_id":p["launch_id"],"state":"awaiting_root_hook"}}}})
            } else if mode == "bridge-server" {
                assert_eq!(request["op"], "run_actor_lookup_v1");
                json!({"api_version":1,"request_id":request["request_id"],"success":true,"result":{"op":"run_actor_lookup_v1","value":{"status":"ok","value":{"version":1,"store_id":p["expected_store_id"],"producer":p["producer"],"incarnation":p["incarnation"],"launch_id":p["launch_id"],"state":"current","actor":{"address":"host/codex/native","role":"root","native_id":"native","epoch":1},"issuer_epoch":"issuer"}}}})
            } else {
                json!({"id":request["id"],"result":{"version":1,"store_id":p["expected_store_id"],"request_id":p["request_id"],"seal_id":"seal","bridge_route":p["bridge_route"],"producer":p["producer"],"issuer_epoch":p["issuer_epoch"],"retirement_key":p["retirement_key"],"state":"sealed"}})
            };
            writeln!(stream, "{response}").unwrap();
        } else if mode == "hold-client" {
            let mut stream = UnixStream::connect(socket).unwrap();
            std::fs::write(std::env::var_os("RUN_R2_PEER_READY").unwrap(), b"ready").unwrap();
            let mut byte = [0u8; 1];
            let _ = stream.read(&mut byte);
        } else if mode == "client" || mode == "delayed-client" {
            let mut stream = UnixStream::connect(socket).unwrap();
            if mode == "delayed-client" {
                std::fs::write(std::env::var_os("RUN_R2_PEER_READY").unwrap(), b"ready").unwrap();
                let release = std::env::var_os("RUN_R2_PEER_RELEASE").unwrap();
                let deadline = std::time::Instant::now() + Duration::from_secs(5);
                while !Path::new(&release).exists() {
                    assert!(
                        std::time::Instant::now() < deadline,
                        "peer release timed out"
                    );
                    std::thread::sleep(Duration::from_millis(10));
                }
            }
            let frame = std::env::var("RUN_R2_PEER_FRAME").unwrap();
            writeln!(stream, "{frame}").unwrap();
            let mut bytes = Vec::new();
            match stream.read_to_end(&mut bytes) {
                Ok(_) => {}
                Err(error)
                    if matches!(
                        error.kind(),
                        std::io::ErrorKind::ConnectionReset | std::io::ErrorKind::BrokenPipe
                    ) => {}
                Err(error) => panic!("peer read: {error}"),
            };
            std::fs::write(std::env::var_os("RUN_R2_PEER_OUTPUT").unwrap(), bytes).unwrap();
        } else {
            panic!("unknown fake peer mode");
        }
    }
    /// Ensure the test process starts after its executable's recorded mtime/ctime.
    pub(crate) async fn wait_build_age(path: &Path) {
        use std::os::unix::fs::MetadataExt;
        let metadata = std::fs::metadata(path).unwrap();
        let newest = metadata.mtime().max(metadata.ctime());
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs() as i64;
        if newest >= now {
            tokio::time::sleep(Duration::from_secs((newest - now + 2) as u64)).await;
        }
    }
    pub(crate) fn child_at(
        executable: &Path,
        mode: &str,
        socket: &Path,
    ) -> tokio::process::Command {
        let mut command = tokio::process::Command::new(executable);
        command
            .args([
                "--exact",
                "attempt_transport::test_peer::synthetic_peer_child",
                "--nocapture",
            ])
            .env_clear()
            .env(MODE, mode)
            .env("RUN_R2_PEER_SOCKET", socket)
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .kill_on_drop(true);
        command
    }
    pub(crate) async fn wait_file(path: &Path) {
        tokio::time::timeout(Duration::from_secs(5), async {
            while !path.exists() {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
    }
}
