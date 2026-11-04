//! Bounded native probes. Provider response bodies and backend account IDs are never logged.
use super::{Key, Pool, Route, Sample, Slice, Topology, account_token};
use crate::{
    Error, Result,
    adapters::{
        self, LaunchPlan,
        io::Process,
        materialize::{self, Publisher},
    },
    config::{Adapter, Auth, Config, Runtime},
    domain::now,
    error::invalid,
    fs,
    process::{OwnedProcess, ProcessState, observe},
    profiles::Profile,
};
use serde_json::{Value, json};
use std::{
    collections::{BTreeMap, BTreeSet},
    ffi::CString,
    io,
    os::{
        fd::{AsRawFd, FromRawFd, OwnedFd, RawFd},
        unix::{ffi::OsStrExt, process::CommandExt},
    },
    path::{Path, PathBuf},
    process::{Child, Command, ExitStatus, Stdio},
    sync::{
        Arc,
        atomic::{AtomicU8, Ordering},
        mpsc,
    },
    time::{Duration, Instant},
};
use tokio::io::AsyncReadExt;
const TTL: f64 = 900.0;
/// Fixed issue for any schema-1 collector: nothing runs until explicit exec configuration.
pub const COLLECTOR_RETIRED: &str = "external_collector_configuration_required";
const BODY_MAX: usize = 2 * 1024 * 1024;
fn number(v: Option<&Value>) -> Option<f64> {
    v.and_then(Value::as_f64).filter(|v| v.is_finite())
}
fn timestamp(v: Option<&Value>) -> Option<f64> {
    let s = v.and_then(Value::as_str)?;
    if s.len() > 64 {
        return None;
    }
    chrono::DateTime::parse_from_rfc3339(s)
        .ok()
        .map(|d| d.timestamp_millis() as f64 / 1000.0)
        .filter(|v| *v >= 0.0)
}
/// Maps one app-server window duration to its stable window name.
pub fn window_name(minutes: f64) -> String {
    if minutes == 300.0 {
        "five_hour".into()
    } else if minutes == 10080.0 {
        "seven_day".into()
    } else {
        format!("min{minutes}")
    }
}
// The fields intentionally mirror one provider window; grouping them would
// obscure the direct mapping and churn every source normalizer call site.
#[allow(clippy::too_many_arguments)]
fn sample(
    runtime: &str,
    lane: &str,
    window: String,
    target: Option<String>,
    source: &str,
    remaining: f64,
    reset: Option<f64>,
    observed: f64,
) -> Sample {
    Sample {
        key: Key {
            runtime: runtime.into(),
            lane: lane.into(),
            window,
            target,
            source: source.into(),
        },
        remaining_percent: Some(remaining),
        reset_at: reset,
        observed_at: Some(observed),
        valid_until: Some(observed + TTL),
    }
}
/// One sample's own declared shelf life in seconds, or `None` when it declares none.
///
/// Rust stores a sample's shelf life materialized as `valid_until`, which is
/// exactly what Python's `persist_slice` writes for a sample carrying
/// `valid_for_seconds`; the declared duration is therefore
/// `valid_until - observed_at`. A sample missing either bound declares no
/// shelf life, and so does a non-positive or non-finite span -- mirroring
/// Python's `sample.valid_for_seconds and sample.valid_for_seconds > 0`
/// filter, under which absent, zero, and negative values are all ignored
/// rather than shortening the slice.
fn shelf_life(sample: &Sample) -> Option<f64> {
    let span = sample.valid_until? - sample.observed_at?;
    (span.is_finite() && span > 0.0).then_some(span)
}
/// Builds one collection slice whose bounds come from the samples it carries.
///
/// Mirrors Python's `capacity.collect._slice_from_samples`. `runtime` and
/// `scope_id` are the slice's non-empty identities, `samples` are the already
/// normalized measurements (moved into the slice unchanged), `topology` is the
/// source's declared pool/route shape, and `started` is the round's epoch used
/// only as a fallback.
///
/// The slice observes the newest positive per-sample `observed_at`, falling
/// back to `started` when no sample carries one, and expires after the
/// **shortest positive** per-sample shelf life so no pool outlives its
/// least-fresh constituent. When no sample declares a shelf life the bounded
/// `TTL` default applies; a weekly provider reset therefore never keeps a
/// topology snapshot fresh for days. This function performs no validation:
/// [`super::persist`] checks the whole slice before anything is written.
pub fn slice_from_samples(
    runtime: &str,
    scope_id: &str,
    samples: Vec<Sample>,
    topology: Topology,
    started: f64,
) -> Slice {
    let newest = samples
        .iter()
        .filter_map(|sample| sample.observed_at)
        .filter(|observed| *observed > 0.0)
        .fold(f64::NEG_INFINITY, f64::max);
    let observed_at = if newest.is_finite() { newest } else { started };
    let shortest = samples
        .iter()
        .filter_map(shelf_life)
        .fold(f64::INFINITY, f64::min);
    let valid_for = if shortest.is_finite() { shortest } else { TTL };
    Slice {
        runtime: runtime.into(),
        scope_id: scope_id.into(),
        samples,
        topology,
        observed_at,
        valid_until: observed_at + valid_for,
    }
}
pub fn normalize_codex(
    runtime: &str,
    account: Option<&str>,
    response: &Value,
    observed: f64,
) -> Result<(Slice, Option<String>)> {
    if !observed.is_finite() || observed < 0.0 {
        return Err(invalid("invalid quota observation"));
    }
    let raw = response
        .get("result")
        .filter(|v| v.is_object())
        .unwrap_or(response);
    let buckets = if let Some(map) = raw.get("rateLimitsByLimitId").and_then(Value::as_object) {
        map.clone()
    } else if let Some(v) = raw.get("rateLimits").filter(|v| v.is_object()) {
        let name = v
            .get("limitId")
            .and_then(Value::as_str)
            .filter(|s| !s.is_empty())
            .unwrap_or("codex");
        let mut map = serde_json::Map::new();
        map.insert(name.into(), v.clone());
        map
    } else {
        return Err(invalid("codex quota response has no bucket map"));
    };
    let credits = raw
        .pointer("/rateLimitResetCredits/availableCount")
        .and_then(Value::as_u64);
    let backend = raw
        .get("accountId")
        .and_then(Value::as_str)
        .filter(|s| !s.is_empty())
        .map(str::to_owned);
    let mut samples = Vec::new();
    let mut topology = Topology::default();
    for (limit_id, bucket) in buckets {
        if limit_id.is_empty() || !bucket.is_object() {
            continue;
        }
        let mut complete = true;
        let mut keys = BTreeSet::new();
        for field in ["primary", "secondary"] {
            let Some(value) = bucket.get(field).filter(|v| !v.is_null()) else {
                continue;
            };
            let used = number(value.get("usedPercent"));
            let minutes = number(value.get("windowDurationMins"));
            let reset = number(value.get("resetsAt"));
            if used.is_none_or(|v| !(0.0..=100.0).contains(&v))
                || minutes.is_none_or(|v| v <= 0.0)
                || value.get("resetsAt").is_some_and(|v| {
                    !v.is_null() && reset.is_none_or(|n| n < 0.0 || n > 253_402_300_799.0)
                })
            {
                complete = false;
                continue;
            }
            let s = sample(
                runtime,
                &limit_id,
                window_name(minutes.unwrap_or(0.0)),
                account.map(str::to_owned),
                "codex_appserver",
                100.0 - used.unwrap_or(0.0),
                reset,
                observed,
            );
            if !keys.insert(s.key.clone()) {
                complete = false;
                continue;
            }
            samples.push(s);
        }
        // A malformed present window disables the WHOLE bucket, including its good sibling.
        if complete && !keys.is_empty() {
            let id = format!("{runtime}:{}:{limit_id}", account_token(account));
            topology.pools.push(Pool {
                pool_id: id.clone(),
                keys,
            });
            topology.routes.push(Route {
                route_id: id.clone(),
                runtime: runtime.into(),
                account: account.map(str::to_owned),
                quota_lane: bucket
                    .get("limitName")
                    .and_then(Value::as_str)
                    .filter(|s| !s.is_empty())
                    .unwrap_or(&limit_id)
                    .into(),
                pool_ids: vec![id],
                reset_credits: if limit_id == "codex" { credits } else { None },
            });
        }
    }
    topology.validate(runtime)?;
    Ok((
        slice_from_samples(
            runtime,
            &format!("codex:{}", account_token(account)),
            samples,
            topology,
            observed,
        ),
        backend,
    ))
}
fn probe_profile() -> Profile {
    Profile {
        name: "probe".into(),
        body: "Read provider metadata only.".into(),
        write: false,
        network: false,
        revision: "probe-v1".into(),
        canonical: true,
        allow_external_read_roots: false,
        read_roots: vec![],
        skills: vec![],
        mcp: vec![],
        mcp_tools: Default::default(),
        required_constraints: BTreeSet::new(),
    }
}
/// Query one bounded Codex metadata endpoint in an isolated, verified process group.
///
/// Only the two supported metadata methods are accepted. The temporary probe home is
/// removed after cleanup only when native group/descendant evidence confirms it; an
/// unavailable cleanup observation returns its typed runtime error after reaping.
pub async fn codex_probe(
    app_home: &Path,
    cfg: &Config,
    rt: &Runtime,
    account: Option<&str>,
    method: &str,
) -> Result<Value> {
    if rt.kind()? != Adapter::Codex || method != "model/list" {
        return Err(invalid("invalid metadata probe"));
    }
    let probes = app_home.join("probes");
    fs::private_dir(&probes)?;
    let home = probes.join(uuid::Uuid::new_v4().to_string());
    let prepared = (|| -> Result<LaunchPlan> {
        let mut p = Publisher::new(&home)?;
        p.file(
            "config.toml",
            b"cli_auth_credentials_store = \"file\"\n",
            0o600,
        )?;
        let source = if let Some(label) = account {
            materialize::account_home(app_home, Adapter::Codex, label).join("auth.json")
        } else if let Some(Auth::FileLink { source, .. }) = &rt.auth {
            source.clone()
        } else {
            let root = std::env::var_os("CODEX_HOME")
                .map(PathBuf::from)
                .unwrap_or(fs::expand(Path::new("~/.codex"))?);
            root.join("auth.json")
        };
        p.link("auth.json", &source)?;
        p.finish()?;
        let mut clean = rt.clone();
        clean.environment = None;
        clean.rust = None;
        clean.mcp.clear();
        clean.skills.clear();
        Ok(LaunchPlan {
            binary: rt.binary.clone(),
            args: vec!["app-server".into()],
            cwd: home.clone(),
            environment: adapters::environment(
                cfg,
                &clean,
                &probe_profile(),
                &home,
                account,
                app_home,
            )?,
            initial_input: None,
        })
    })();
    let plan = match prepared {
        Ok(v) => v,
        Err(e) => {
            let _ = std::fs::remove_dir_all(&home);
            return Err(e);
        }
    };
    let mut process = match Process::spawn(&plan) {
        Ok(p) => p,
        Err(e) => {
            let _ = std::fs::remove_dir_all(&home);
            return Err(e);
        }
    };
    let result = tokio::time::timeout(
        Duration::from_secs(35),
        crate::codex::query(&mut process, method),
    )
    .await
    .map_err(|_| Error::Runtime("metadata probe timed out".into()))
    .and_then(|r| r);
    let cleanup = process.owner.cleanup(Duration::from_secs(1)).await;
    process.reap().await;
    let cleanup = cleanup?;
    if cleanup.confirmed {
        let _ = std::fs::remove_dir_all(&home);
    } else {
        return Err(Error::Runtime(
            "metadata probe cleanup was not confirmed".into(),
        ));
    }
    result
}
fn host_environment() -> BTreeMap<String, String> {
    [
        "HOME", "PATH", "USER", "LOGNAME", "LANG", "LC_ALL", "TMPDIR",
    ]
    .iter()
    .filter_map(|k| std::env::var(k).ok().map(|v| (k.to_string(), v)))
    .collect()
}

/// Bounds both sides of the pre-exec identity handshake.
const PROBE_STARTUP_MS: i64 = 2_000;
const PROBE_PENDING: u8 = 0;
const PROBE_RELEASED: u8 = 1;
const PROBE_CANCELLED: u8 = 2;

/// Create a close-on-exec pipe whose descriptors cannot collide with stdio.
fn probe_pipe() -> io::Result<(OwnedFd, OwnedFd)> {
    /// Move a low descriptor above stdio without changing its close-on-exec property.
    fn above_stdio(fd: OwnedFd) -> io::Result<OwnedFd> {
        if fd.as_raw_fd() >= 3 {
            return Ok(fd);
        }
        // SAFETY: fd is open and owned; F_DUPFD_CLOEXEC takes no pointer argument.
        let copy = unsafe { libc::fcntl(fd.as_raw_fd(), libc::F_DUPFD_CLOEXEC, 3) };
        if copy < 0 {
            return Err(io::Error::last_os_error());
        }
        // SAFETY: fcntl returned a fresh descriptor owned by this function.
        Ok(unsafe { OwnedFd::from_raw_fd(copy) })
    }
    let (read, write) = agent_run_platform::launch::cloexec_pipe()?;
    Ok((above_stdio(read)?, above_stdio(write)?))
}

/// Return monotonic milliseconds using only a post-fork-safe system call.
fn probe_clock_ms() -> io::Result<i64> {
    let mut now = libc::timespec {
        tv_sec: 0,
        tv_nsec: 0,
    };
    // SAFETY: now points to writable storage for one timespec.
    if unsafe { libc::clock_gettime(libc::CLOCK_MONOTONIC, &mut now) } < 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(now
        .tv_sec
        .saturating_mul(1_000)
        .saturating_add(now.tv_nsec / 1_000_000))
}

/// Read an exact small handshake with an absolute deadline, including after EINTR.
///
/// This is also called between fork and exec; it must remain allocation-free and
/// use only async-signal-safe operations. The other endpoint has a single writer.
fn probe_read(fd: RawFd, bytes: &mut [u8], deadline: i64) -> io::Result<()> {
    let mut offset = 0;
    while offset < bytes.len() {
        let remaining = deadline.saturating_sub(probe_clock_ms()?);
        if remaining <= 0 {
            return Err(io::Error::from_raw_os_error(libc::ETIMEDOUT));
        }
        let mut poll_fd = libc::pollfd {
            fd,
            events: libc::POLLIN,
            revents: 0,
        };
        // SAFETY: poll_fd is initialized and writable; the timeout fits i32.
        let ready = unsafe { libc::poll(&mut poll_fd, 1, remaining as i32) };
        if ready < 0 {
            let error = io::Error::last_os_error();
            if error.raw_os_error() == Some(libc::EINTR) {
                continue;
            }
            return Err(error);
        }
        if ready == 0 {
            return Err(io::Error::from_raw_os_error(libc::ETIMEDOUT));
        }
        if poll_fd.revents & libc::POLLIN == 0 {
            return Err(io::Error::from_raw_os_error(libc::EPIPE));
        }
        // SAFETY: offset is within bytes, and the remaining slice is writable.
        let count = unsafe {
            libc::read(
                fd,
                bytes[offset..].as_mut_ptr().cast(),
                bytes.len() - offset,
            )
        };
        if count < 0 {
            let error = io::Error::last_os_error();
            if error.raw_os_error() == Some(libc::EINTR) {
                continue;
            }
            return Err(error);
        }
        if count == 0 {
            return Err(io::Error::from_raw_os_error(libc::EPIPE));
        }
        offset += count as usize;
    }
    Ok(())
}

/// Write the small handshake, retrying partial writes and EINTR.
///
/// The reader remains open in the parent until both writers finish, and each
/// pipe is initially empty, so these at-most-four-byte writes cannot block or
/// raise SIGPIPE. This function is also used in the post-fork child.
fn probe_write(fd: RawFd, bytes: &[u8], deadline: i64) -> io::Result<()> {
    let mut offset = 0;
    while offset < bytes.len() {
        if probe_clock_ms()? >= deadline {
            return Err(io::Error::from_raw_os_error(libc::ETIMEDOUT));
        }
        // SAFETY: offset is within bytes and the remaining slice is readable.
        let count =
            unsafe { libc::write(fd, bytes[offset..].as_ptr().cast(), bytes.len() - offset) };
        if count < 0 {
            let error = io::Error::last_os_error();
            if error.raw_os_error() == Some(libc::EINTR) {
                continue;
            }
            return Err(error);
        }
        if count == 0 {
            return Err(io::Error::from_raw_os_error(libc::EIO));
        }
        offset += count as usize;
    }
    Ok(())
}

/// Immutable execve storage; every pointer targets the owned C strings below.
struct ProbeExec {
    path: CString,
    _args: Vec<CString>,
    argv: Vec<*const libc::c_char>,
    _environment: Vec<CString>,
    envp: Vec<*const libc::c_char>,
}

// SAFETY: the pointers reference separately allocated immutable C strings owned
// by this value. Moving it never moves those allocations, and no API mutates them.
unsafe impl Send for ProbeExec {}
// SAFETY: the pointed-to bytes and pointer arrays are immutable after creation;
// execution only reads them in the forked child, with the owner still alive.
unsafe impl Sync for ProbeExec {}

impl ProbeExec {
    /// Prepare the original path, argv and environment before fork.
    fn new(
        binary: &Path,
        args: &[String],
        environment: &BTreeMap<String, String>,
    ) -> io::Result<Self> {
        /// Match std Command's rejection of NUL bytes before entering the child.
        fn cstring(bytes: &[u8]) -> io::Result<CString> {
            CString::new(bytes).map_err(|_| {
                io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "nul byte found in provided data",
                )
            })
        }
        let program = binary.as_os_str().as_bytes();
        let path = cstring(program)?;
        let mut arg_storage = Vec::with_capacity(args.len() + 1);
        arg_storage.push(cstring(program)?);
        for arg in args {
            arg_storage.push(cstring(arg.as_bytes())?);
        }
        let argv = arg_storage
            .iter()
            .map(|arg| arg.as_ptr())
            .chain([std::ptr::null()])
            .collect();
        let env_storage = environment
            .iter()
            .map(|(key, value)| cstring(format!("{key}={value}").as_bytes()))
            .collect::<io::Result<Vec<_>>>()?;
        let envp = env_storage
            .iter()
            .map(|entry| entry.as_ptr())
            .chain([std::ptr::null()])
            .collect();
        Ok(Self {
            path,
            _args: arg_storage,
            argv,
            _environment: env_storage,
            envp,
        })
    }

    /// Execute a Linux path directly, preserving the original spawn's errno.
    ///
    /// This runs after fork and uses only prepared immutable memory and execve.
    fn exec(&self) -> io::Result<()> {
        // SAFETY: the path and both null-terminated pointer arrays reference
        // live immutable C strings. execve never returns on success.
        unsafe { libc::execve(self.path.as_ptr(), self.argv.as_ptr(), self.envp.as_ptr()) };
        Err(io::Error::last_os_error())
    }
}

/// Hold exec until the parent has saved a verified identity for this child.
///
/// Called after fork: no locks, allocation, unwinding, or non-signal-safe calls
/// are permitted here. The four descriptor numbers stay owned until spawn ends.
fn probe_barrier(
    ready_r: RawFd,
    ready_w: RawFd,
    gate_r: RawFd,
    gate_w: RawFd,
    deadline: i64,
) -> io::Result<()> {
    for fd in [ready_r, gate_w] {
        // SAFETY: these inherited descriptors are unused by the child and are
        // distinct from stdio and both endpoints retained for the handshake.
        if unsafe { libc::close(fd) } < 0 {
            return Err(io::Error::last_os_error());
        }
    }
    // SAFETY: getpid has no arguments and is async-signal-safe after fork.
    let pid = unsafe { libc::getpid() };
    probe_write(ready_w, &pid.to_ne_bytes(), deadline)?;
    // SAFETY: ready_w is the child's now-unused, distinct write descriptor.
    if unsafe { libc::close(ready_w) } < 0 {
        return Err(io::Error::last_os_error());
    }
    let mut release = [0];
    probe_read(gate_r, &mut release, deadline)?;
    // SAFETY: gate_r is the child's now-unused, distinct read descriptor.
    if unsafe { libc::close(gate_r) } < 0 {
        return Err(io::Error::last_os_error());
    }
    if release != [1] {
        return Err(io::Error::from_raw_os_error(libc::ECANCELED));
    }
    Ok(())
}

/// Mark a pending blocking launch cancelled when its caller goes away.
struct ProbeLaunchCancel {
    state: Arc<AtomicU8>,
    abort: tokio::task::AbortHandle,
}
impl Drop for ProbeLaunchCancel {
    /// Prevent a queued or not-yet-released command from starting after cancellation.
    fn drop(&mut self) {
        let _ = self.state.compare_exchange(
            PROBE_PENDING,
            PROBE_CANCELLED,
            Ordering::SeqCst,
            Ordering::SeqCst,
        );
        self.abort.abort();
    }
}

/// Own a spawned probe until its verified group is cleaned and the child reaped.
struct ProbeProcess {
    child: Child,
    owner: OwnedProcess,
    armed: bool,
}
impl Drop for ProbeProcess {
    /// Cancellation cannot await, so use the existing bounded synchronous cleanup.
    fn drop(&mut self) {
        if self.armed {
            let _ = self.owner.cleanup_blocking(Duration::from_millis(250));
            reap_probe(&mut self.child);
        }
    }
}

/// Reap an exited child now or hand its still-owned PID to the existing reaper.
fn reap_probe(child: &mut Child) {
    if matches!(child.try_wait(), Ok(None)) {
        let _ = agent_run_platform::launch::spawn_reaper(child.id() as i32);
    }
}

/// Spawn the original command only after its blocked child has a live identity.
///
/// The observer sends ownership into the parent's channel before releasing exec.
/// An exec failure has already been reaped by `Command::spawn`; its saved PID is
/// discarded without signalling. All endpoints stay open until the observer has
/// joined, including on spawn errors and caller cancellation.
fn spawn_probe(
    mut command: Command,
    exec: Option<ProbeExec>,
    state: Arc<AtomicU8>,
    deadline: Instant,
    gate_deadline: i64,
) -> io::Result<ProbeProcess> {
    if state.load(Ordering::SeqCst) != PROBE_PENDING || Instant::now() >= deadline {
        return Err(io::Error::from_raw_os_error(libc::ECANCELED));
    }
    let (ready_r, ready_w) = probe_pipe()?;
    let (gate_r, gate_w) = probe_pipe()?;
    let ready_r = Arc::new(ready_r);
    let gate_w = Arc::new(gate_w);
    let (owner_tx, owner_rx) = mpsc::channel();
    let observer_ready = Arc::clone(&ready_r);
    let observer_gate = Arc::clone(&gate_w);
    let observer_state = Arc::clone(&state);
    let observer = std::thread::Builder::new()
        .name("metadata-identity".into())
        .spawn(move || {
            let observed = (|| -> io::Result<OwnedProcess> {
                let mut bytes = [0; std::mem::size_of::<libc::pid_t>()];
                probe_read(observer_ready.as_raw_fd(), &mut bytes, gate_deadline)?;
                let pid = libc::pid_t::from_ne_bytes(bytes);
                if pid <= 1 || observer_state.load(Ordering::SeqCst) != PROBE_PENDING {
                    return Err(io::Error::from_raw_os_error(libc::ECANCELED));
                }
                let owner = OwnedProcess::capture(pid);
                let verified = owner.leader.as_ref().is_some_and(|leader| {
                    leader.pid == pid
                        && leader.ppid == std::process::id() as i32
                        && leader.group == pid
                        && !leader.zombie
                        && observe(Some(pid), Some(&leader.token), Some(leader.birth))
                            == ProcessState::Alive
                });
                if !verified {
                    return Err(io::Error::from_raw_os_error(libc::ESRCH));
                }
                Ok(owner)
            })();
            let release = observed.is_ok_and(|owner| {
                owner_tx.send(owner).is_ok()
                    && Instant::now() < deadline
                    && observer_state
                        .compare_exchange(
                            PROBE_PENDING,
                            PROBE_RELEASED,
                            Ordering::SeqCst,
                            Ordering::SeqCst,
                        )
                        .is_ok()
            });
            let _ = probe_write(
                observer_gate.as_raw_fd(),
                &[u8::from(release)],
                gate_deadline,
            );
        })?;
    let ready_r_fd = ready_r.as_raw_fd();
    let ready_w_fd = ready_w.as_raw_fd();
    let gate_r_fd = gate_r.as_raw_fd();
    let gate_w_fd = gate_w.as_raw_fd();
    // SAFETY: the callback uses only async-signal-safe operations and stack
    // storage. Its descriptors are distinct, CLOEXEC, and kept open until spawn
    // returns; the prepared exec plan owns every pointed-to argument and env byte.
    // No Rust lock, allocation or destructor runs in the child.
    unsafe {
        command.pre_exec(move || {
            probe_barrier(ready_r_fd, ready_w_fd, gate_r_fd, gate_w_fd, gate_deadline)?;
            match &exec {
                Some(exec) => exec.exec(),
                None => Ok(()),
            }
        });
    }
    command.process_group(0);
    let spawned = command.spawn();
    drop(ready_w);
    let observer_result = observer.join();
    let mut child = spawned?;
    if let Ok(owner) = owner_rx.try_recv()
        && owner.pid == child.id() as i32
    {
        let process = ProbeProcess {
            child,
            owner,
            armed: true,
        };
        if observer_result.is_err() {
            return Err(io::Error::from_raw_os_error(libc::EIO));
        }
        return Ok(process);
    }
    // A successful exec without the handshake is an internal protocol failure.
    // The std Child still owns this unreaped PID, so its direct kill is safe;
    // no unverified process group is signalled.
    let _ = child.kill();
    reap_probe(&mut child);
    Err(io::Error::from_raw_os_error(libc::EIO))
}

/// Reap the child without blocking the async executor while waiting for exit.
async fn wait_probe(child: &mut Child) -> io::Result<ExitStatus> {
    loop {
        if let Some(status) = child.try_wait()? {
            return Ok(status);
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

/// A bounded subprocess reader shared by metadata probes and diagnostics.
///
/// The original command runs in a verified process group and is reaped before
/// success; incomplete cleanup or unavailable native evidence returns an error.
/// Cancellation also requests bounded cleanup of the verified group.
/// Successful commands must emit at least one byte, so an empty or stderr-only
/// version probe is classified as missing version evidence.
pub async fn capture(
    binary: &Path,
    args: &[String],
    seconds: u64,
    environment: &BTreeMap<String, String>,
) -> Result<Vec<u8>> {
    if seconds == 0 {
        return Err(invalid("metadata command timed out"));
    }
    let deadline = Instant::now()
        .checked_add(Duration::from_secs(seconds))
        .ok_or_else(|| invalid("metadata command timeout out of range"))?;
    let gate_ms = PROBE_STARTUP_MS.min((seconds.min(2) * 1_000) as i64);
    let gate_deadline = probe_clock_ms()?.saturating_add(gate_ms);
    // With env_clear, std uses execvp for bare names. On Linux a path containing
    // '/' normally takes posix_spawn instead, so retain its ENOEXEC semantics.
    let exec = if cfg!(target_os = "linux") && binary.as_os_str().as_bytes().contains(&b'/') {
        Some(ProbeExec::new(binary, args, environment)?)
    } else {
        None
    };
    let mut command = Command::new(binary);
    command
        .args(args)
        .env_clear()
        .envs(environment)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::null());
    let state = Arc::new(AtomicU8::new(PROBE_PENDING));
    let launch_state = Arc::clone(&state);
    let launch = tokio::task::spawn_blocking(move || {
        spawn_probe(command, exec, launch_state, deadline, gate_deadline)
    });
    let _launch_cancel = ProbeLaunchCancel {
        state,
        abort: launch.abort_handle(),
    };
    let mut process = tokio::time::timeout_at(tokio::time::Instant::from_std(deadline), launch)
        .await
        .map_err(|_| invalid("metadata command startup timed out"))?
        .map_err(|_| invalid("metadata command startup failed"))??;
    let stdout = process
        .child
        .stdout
        .take()
        .ok_or_else(|| invalid("probe output missing"))?;
    let stdout = tokio::process::ChildStdout::from_std(stdout)?;
    let read = async {
        let mut bytes = Vec::new();
        let mut reader = stdout.take((BODY_MAX + 1) as u64);
        reader.read_to_end(&mut bytes).await?;
        if bytes.len() > BODY_MAX {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "probe output bound",
            ));
        }
        Ok(bytes)
    };
    let result = tokio::time::timeout_at(tokio::time::Instant::from_std(deadline), async {
        let (bytes, status) = tokio::try_join!(read, wait_probe(&mut process.child))?;
        if !status.success() {
            return Err(invalid("metadata command failed"));
        }
        if bytes.is_empty() {
            return Err(invalid("metadata command returned no output"));
        }
        Ok(bytes)
    })
    .await
    .map_err(|_| invalid("metadata command timed out"))
    .and_then(|r| r);
    let clean = process.owner.cleanup(Duration::from_secs(1)).await;
    let reaped = tokio::time::timeout(Duration::from_secs(2), wait_probe(&mut process.child)).await;
    let clean = clean?;
    if !clean.confirmed || !matches!(reaped, Ok(Ok(_))) {
        return Err(invalid("metadata command cleanup unconfirmed"));
    }
    process.armed = false;
    result
}
/// Groups samples into one physical pool per distinct sample identity.
///
/// Mirrors Python's `capacity.topology.pools_from_samples`: the identity is
/// the whole `(lane, window, target, source)` tuple, so samples sharing an
/// identity share one reservoir while any difference keeps pools apart.
/// `runtime` names the engine owning every emitted key and prefixes every pool
/// id; the absent target is encoded with the `shared` token so it can never
/// collide with a literal account label. The result is sorted by pool id, so
/// the input sample order never changes it, and it is empty exactly when
/// `samples` is.
fn pools_from_samples(runtime: &str, samples: &[Sample]) -> Vec<Pool> {
    let mut grouped: BTreeMap<String, BTreeSet<Key>> = BTreeMap::new();
    for sample in samples {
        let key = &sample.key;
        let id = format!(
            "{runtime}:{}:{}:{}:{}",
            key.lane,
            key.window,
            super::account_token_with(key.target.as_deref(), "shared"),
            key.source
        );
        grouped.entry(id).or_default().insert(key.clone());
    }
    grouped
        .into_iter()
        .map(|(pool_id, keys)| Pool { pool_id, keys })
        .collect()
}
/// Composes the native source's shared default route plus one route per scope.
///
/// Mirrors Python's `capacity.sources._native_routes`. Target-less pools are
/// the capacity every model shares and get one `default` route; each distinct
/// non-empty target is a model scope whose route carries no account, takes the
/// scope name as its quota lane, and draws on the union of the shared pools
/// and its own. Scope names are data, not a fixed catalogue, so any discovered
/// scope follows the same rule. `runtime` owns every id and key. A sample-free
/// input yields an empty topology.
///
/// Keeping the scopes apart is what makes a runtime usable when one scoped
/// model is exhausted or unknown: [`super::order`] drops a route whose pools
/// hold any unknown forecast, and ranking drops a route with any exhausted
/// window, so folding every scope into one pool would let a single scoped
/// model disable the whole runtime.
fn native_topology(runtime: &str, samples: &[Sample]) -> Topology {
    let pools = pools_from_samples(runtime, samples);
    let mut shared: BTreeSet<String> = BTreeSet::new();
    let mut scoped: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();
    for pool in &pools {
        for key in &pool.keys {
            match key.target.as_deref() {
                None => {
                    shared.insert(pool.pool_id.clone());
                }
                Some(target) => {
                    scoped
                        .entry(target.to_owned())
                        .or_default()
                        .insert(pool.pool_id.clone());
                }
            }
        }
    }
    let mut routes = Vec::new();
    if !shared.is_empty() {
        routes.push(Route {
            route_id: format!("{runtime}:default"),
            runtime: runtime.into(),
            account: None,
            quota_lane: "default".into(),
            pool_ids: shared.iter().cloned().collect(),
            reset_credits: None,
        });
    }
    for (scope, own) in scoped {
        let mut pool_ids = shared.clone();
        pool_ids.extend(own);
        routes.push(Route {
            route_id: format!("{runtime}:scope:{scope}"),
            runtime: runtime.into(),
            account: None,
            quota_lane: scope,
            pool_ids: pool_ids.into_iter().collect(),
            reset_credits: None,
        });
    }
    Topology { pools, routes }
}
/// Composes one aggregate route over every pooled window of a shared reservoir.
///
/// Mirrors the `omniroute` branch of Python's `capacity.sources.sample_topology`:
/// the provider reports one pooled quota, so there is exactly one launchable
/// route -- id `{runtime}:aggregate`, lane `aggregate`, no account -- drawing
/// on every pool in declaration order. `runtime` owns every id and key. A
/// sample-free input yields an empty topology rather than a route referencing
/// no pool, which would not validate.
fn aggregate_topology(runtime: &str, samples: &[Sample]) -> Topology {
    let pools = pools_from_samples(runtime, samples);
    if pools.is_empty() {
        return Topology::default();
    }
    let routes = vec![Route {
        route_id: format!("{runtime}:aggregate"),
        runtime: runtime.into(),
        account: None,
        quota_lane: "aggregate".into(),
        pool_ids: pools.iter().map(|pool| pool.pool_id.clone()).collect(),
        reset_credits: None,
    }];
    Topology { pools, routes }
}
/// Derives the explicit topology one source's collected samples describe.
///
/// Mirrors Python's `capacity.sources.sample_topology`. `runtime` owns every
/// emitted pool and route identity, `source` is the runtime's configured
/// limits source, and `samples` are that source's already normalized
/// measurements, which are only read. `omniroute` reports one aggregate route
/// over all of its pooled windows; every other source composes the native
/// shared default route plus one route per model scope. Pools always come from
/// the neutral per-identity grouping, so the result is canonically ordered and
/// the input sample order never changes it. An empty `samples` yields an empty
/// topology.
pub fn sample_topology(runtime: &str, source: &str, samples: &[Sample]) -> Topology {
    match source {
        "omniroute" => aggregate_topology(runtime, samples),
        _ => native_topology(runtime, samples),
    }
}
pub fn normalize_claude(runtime: &str, raw: &Value, observed: f64) -> Result<Slice> {
    let entries = raw
        .get("limits")
        .and_then(Value::as_array)
        .ok_or_else(|| invalid("claude_malformed_response"))?;
    let mut samples = Vec::new();
    for entry in entries {
        let used = number(entry.get("percent"))
            .filter(|p| (0.0..=100.0).contains(p))
            .ok_or_else(|| invalid("claude_malformed_response"))?;
        let kind = entry
            .get("kind")
            .and_then(Value::as_str)
            .ok_or_else(|| invalid("claude_malformed_response"))?;
        let (lane, window, target) = match kind {
            "session" => ("primary", "five_hour", None),
            "weekly_all" => ("secondary", "seven_day", None),
            "weekly_scoped" => (
                "secondary",
                "seven_day",
                Some(
                    entry
                        .pointer("/scope/model/display_name")
                        .and_then(Value::as_str)
                        .unwrap_or("weekly_scoped")
                        .to_lowercase(),
                ),
            ),
            other => ("secondary", "seven_day", Some(other.into())),
        };
        let reset = timestamp(entry.get("resets_at"));
        if entry.get("resets_at").is_some_and(|v| !v.is_null()) && reset.is_none() {
            return Err(invalid("claude_invalid_reset"));
        }
        samples.push(sample(
            runtime,
            lane,
            window.into(),
            target,
            "native",
            100.0 - used,
            reset,
            observed,
        ));
    }
    let topology = sample_topology(runtime, "native", &samples);
    Ok(slice_from_samples(
        runtime, runtime, samples, topology, observed,
    ))
}
/// Returns the OAuth token a native Claude capacity round may use.
///
/// Mirrors Python's `capacity.sources._claude_oauth_token`. An exported
/// `CLAUDE_CODE_OAUTH_TOKEN` is honoured **only** when `rt`'s auth
/// configuration declares that exact variable: honouring an undeclared export
/// would silently widen the configured auth bridge. An empty export counts as
/// absent, and `ANTHROPIC_API_KEY` is an API key that is never accepted as an
/// OAuth token. Agent-run cannot inspect Claude Code's scoped credential store
/// without reintroducing a global-account fallback, so every other case
/// returns `None` and the caller maps that to its fixed safe failure. Reads
/// process environment state, so the result depends on the ambient
/// environment at call time.
pub fn claude_oauth_token(rt: &Runtime) -> Option<String> {
    match &rt.auth {
        Some(Auth::Environment { names })
            if names.iter().any(|name| name == "CLAUDE_CODE_OAUTH_TOKEN") =>
        {
            std::env::var("CLAUDE_CODE_OAUTH_TOKEN")
                .ok()
                .filter(|token| !token.is_empty())
        }
        _ => None,
    }
}
/// Compatibility refusal for the retired native quota entry point.
/// No runtime metadata or ambient credential can authorize a request here;
/// configure an external executable through schema 2 instead.
pub async fn claude_native(_runtime: &str, _rt: &Runtime) -> Result<Slice> {
    Err(invalid(
        "native quota collector retired; configure an external exec collector",
    ))
}
/// Reads the newest bounded Claude runtime stream as a credential-free fallback.
///
/// Only regular files below the supplied agent-run home are considered. Missing,
/// malformed, or stale stream data produces an empty slice or unknown evidence;
/// it never manufactures a percentage and never triggers a provider request.
pub fn read_claude_stream(home: &Path, runtime: &str) -> Result<Slice> {
    const MAX_FILES: usize = 24;
    const MAX_BYTES: u64 = 262_144;
    const MAX_LINES: usize = 2_048;
    let root = home.join("agents");
    let mut paths = match std::fs::read_dir(&root) {
        Ok(entries) => entries
            .filter_map(std::result::Result::ok)
            .filter(|entry| {
                entry
                    .file_type()
                    .is_ok_and(|kind| kind.is_dir() && !kind.is_symlink())
            })
            .map(|entry| entry.path().join("runtime.jsonl"))
            .filter(|path| path.is_file() && !path.is_symlink())
            .filter_map(|path| {
                path.metadata()
                    .ok()
                    .and_then(|meta| meta.modified().ok())
                    .and_then(|modified| modified.duration_since(std::time::UNIX_EPOCH).ok())
                    .map(|modified| (modified.as_secs_f64(), path))
            })
            .collect::<Vec<_>>(),
        Err(_) => Vec::new(),
    };
    paths.sort_by(|left, right| right.0.total_cmp(&left.0));
    let observed = now();
    for (modified, path) in paths.into_iter().take(MAX_FILES) {
        let Ok(mut file) = std::fs::File::open(path) else {
            continue;
        };
        use std::io::{Read, Seek, SeekFrom};
        let end = file.seek(SeekFrom::End(0)).unwrap_or(0);
        if file
            .seek(SeekFrom::Start(end.saturating_sub(MAX_BYTES)))
            .is_err()
        {
            continue;
        }
        let mut data = Vec::new();
        if file.take(MAX_BYTES).read_to_end(&mut data).is_err() {
            continue;
        }
        let text = String::from_utf8(data).unwrap_or_default();
        for line in text.lines().rev().take(MAX_LINES) {
            if !line.contains("\"rate_limit_event\"") {
                continue;
            }
            let Ok(event) = serde_json::from_str::<Value>(line) else {
                continue;
            };
            if event.get("type").and_then(Value::as_str) != Some("rate_limit_event") {
                continue;
            }
            let Some(windows) = event
                .pointer("/rate_limit_info/unifiedWindows")
                .and_then(Value::as_object)
            else {
                continue;
            };
            let stale = observed - modified > TTL;
            let mut samples = Vec::new();
            for (window, entry) in windows {
                let Some(utilization) = number(entry.get("utilization")) else {
                    continue;
                };
                let reset = entry
                    .get("resetsAt")
                    .and_then(Value::as_f64)
                    .filter(|value| value.is_finite() && *value >= 0.0);
                samples.push(Sample {
                    key: Key {
                        runtime: runtime.into(),
                        lane: "usage".into(),
                        window: window.clone(),
                        target: None,
                        source: if stale {
                            "unknown".into()
                        } else {
                            "runtime_stream_evidence".into()
                        },
                    },
                    remaining_percent: (!stale)
                        .then_some((1.0 - utilization).mul_add(100.0, 0.0).clamp(0.0, 100.0)),
                    reset_at: reset,
                    observed_at: Some(modified),
                    valid_until: Some(modified + TTL),
                });
            }
            if !samples.is_empty() {
                let topology = sample_topology(runtime, "native", &samples);
                return Ok(slice_from_samples(
                    runtime, runtime, samples, topology, modified,
                ));
            }
        }
    }
    Ok(slice_from_samples(
        runtime,
        runtime,
        Vec::new(),
        Topology::default(),
        observed,
    ))
}
/// Reads the local OmniRoute current-cache into one capacity slice.
///
/// The route has one physical pool for the shared `opencode-go` capacity.
/// Docker failures and malformed cache values propagate fixed unavailable
/// reasons, while a healthy empty cache remains a no-data outcome.
/// Runs only explicitly configured external quota executables; legacy configurations must migrate.
pub async fn collect(home: &Path) -> Result<Value> {
    match agent_run_config::provider_config::ProviderConfig::load(home) {
        Ok((config, _)) => super::collectors::collect_providers(home, &config).await,
        Err(error) => {
            // Old homes remain inspectable, but no legacy provider collector may run.
            let Ok(config) = Config::load(home) else {
                return Err(error);
            };
            let results: Vec<_> = config
                .runtimes
                .iter()
                .filter(|(_, runtime)| runtime.enabled)
                .map(|(name, runtime)| {
                    let disabled = runtime.limits_source.as_deref() == Some("none");
                    let issues: Vec<&str> = if disabled {
                        vec![]
                    } else {
                        vec![COLLECTOR_RETIRED]
                    };
                    json!({"runtime":name,"status":if disabled {"unsupported"} else {"failed"},
                        "sample_count":0,"error":issues.first(),"issues":issues})
                })
                .collect();
            super::prune(home, config.capacity.sample_retention).await?;
            Ok(json!({"ok":results.iter().all(|row|row["status"]!="failed"),"results":results}))
        }
    }
}
pub async fn models(home: &Path) -> Result<Value> {
    let cfg = Config::load(home)?;
    let mut result = serde_json::Map::new();
    for (name, rt) in cfg.runtimes.iter().filter(|(_, r)| r.enabled) {
        let kind = rt.kind()?;
        let mut roster: BTreeMap<String, Value> = BTreeMap::new();
        let mut accounts = Vec::new();
        let mut reason = None;
        if kind == Adapter::Codex {
            let mut scopes = vec![None];
            scopes.extend(rt.accounts.iter().map(|s| Some(s.as_str())));
            for account in scopes {
                match codex_probe(home, &cfg, rt, account, "model/list").await {
                    Ok(value) => {
                        let mut ids = Vec::new();
                        if let Some(data) = value.get("data").and_then(Value::as_array) {
                            for model in data {
                                if let Some(id) = model
                                    .get("model")
                                    .or_else(|| model.get("id"))
                                    .and_then(Value::as_str)
                                    .filter(|s| rt.models.iter().any(|m| m == s))
                                {
                                    let efforts: Vec<_> = model
                                        .get("supportedReasoningEfforts")
                                        .and_then(Value::as_array)
                                        .into_iter()
                                        .flatten()
                                        .filter_map(|v| {
                                            v.get("reasoningEffort").and_then(Value::as_str)
                                        })
                                        .collect();
                                    roster.insert(id.into(),json!({"id":id,"description":"configured and observed in native account roster","efforts":efforts}));
                                    ids.push(id.to_owned());
                                }
                            }
                        }
                        accounts.push(
                            json!({"account":account,"available":!ids.is_empty(),"models":ids}),
                        );
                    }
                    Err(_) => {
                        reason = Some("one or more account probes failed");
                        accounts.push(json!({"account":account,"available":false,"models":[],"reason":"probe_failed"}));
                    }
                }
            }
        } else {
            if capture(&rt.binary, &["--version".into()], 5, &host_environment())
                .await
                .is_ok()
            {
                for id in &rt.models {
                    roster.insert(id.clone(),json!({"id":id,"description":"configured native CLI model; authentication checked on launch","efforts":["low","medium","high","xhigh","max"]}));
                }
            } else {
                reason = Some("configured binary probe failed");
            }
        }
        if roster.is_empty() && reason.is_none() {
            reason = Some("roster empty");
        }
        let available = !roster.is_empty();
        result.insert(name.clone(),json!({"models":roster.into_values().collect::<Vec<_>>(),"capabilities":adapters::capabilities(kind),"available":available,"reason":reason,"accounts":accounts}));
    }
    Ok(Value::Object(result))
}

#[cfg(test)]
mod probe_startup_tests {
    use super::*;

    /// A failed handshake returns an error instead of starting the target command.
    #[test]
    fn denied_gate_does_not_exec() {
        let root = tempfile::tempdir().unwrap();
        let marker = root.path().join("executed");
        let (ready_r, ready_w) = probe_pipe().unwrap();
        let (gate_r, gate_w) = probe_pipe().unwrap();
        let deadline = probe_clock_ms().unwrap() + PROBE_STARTUP_MS;
        probe_write(gate_w.as_raw_fd(), &[0], deadline).unwrap();
        let fds = (
            ready_r.as_raw_fd(),
            ready_w.as_raw_fd(),
            gate_r.as_raw_fd(),
            gate_w.as_raw_fd(),
        );
        let mut command = Command::new("/bin/sh");
        command
            .arg("-c")
            .arg("touch \"$1\"")
            .arg("probe")
            .arg(&marker);
        // SAFETY: the descriptors remain owned and distinct until spawn returns;
        // the callback is the same post-fork-safe handshake used in production.
        unsafe { command.pre_exec(move || probe_barrier(fds.0, fds.1, fds.2, fds.3, deadline)) };
        let error = command.spawn().unwrap_err();
        assert_eq!(error.raw_os_error(), Some(libc::ECANCELED));
        assert!(!marker.exists());
    }

    /// EOF on a handshake pipe is a prompt startup failure, not a hang.
    #[test]
    fn closed_gate_returns_failure() {
        let (read, write) = probe_pipe().unwrap();
        drop(write);
        let deadline = probe_clock_ms().unwrap() + PROBE_STARTUP_MS;
        let error = probe_read(read.as_raw_fd(), &mut [0], deadline).unwrap_err();
        assert_eq!(error.raw_os_error(), Some(libc::EPIPE));
    }
}
