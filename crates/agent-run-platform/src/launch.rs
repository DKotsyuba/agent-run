//! Detached supervisor launch, ported from Python `launch.py`/`launch_evidence.py`.
//!
//! Session-creating `posix_spawn` is the normal path, so no parent code runs in
//! the child between fork and exec. A narrow fork fallback exists only when the
//! C library explicitly rejects `POSIX_SPAWN_SETSID`. Under one deadline the
//! parent then waits for the child's PID proof and READY token, verifies that
//! the leader heads its own group, and on failure signals only that verified
//! group before reaping it. See `docs/process-identity.md`.
use crate::process::{self, Identity};
use serde::Serialize;
use serde_json::Value;
use std::{
    ffi::{CString, OsStr},
    fmt, io,
    mem::MaybeUninit,
    os::fd::{AsRawFd, FromRawFd, OwnedFd, RawFd},
    os::unix::ffi::{OsStrExt, OsStringExt},
    path::Path,
    thread::JoinHandle,
    time::{Duration, Instant},
};

/// One budget for exec, identity proof, ownership commit and READY.
pub const READY_TIMEOUT: Duration = Duration::from_secs(30);
/// Natural-exit wait for a child that failed before the READY deadline.
pub const DISPATCH_TIMEOUT: Duration = Duration::from_secs(5);
/// SIGTERM grace for pre-READY cleanup.
pub const CLEANUP_GRACE: Duration = Duration::from_secs(1);
/// SIGKILL grace for pre-READY cleanup.
pub const CLEANUP_KILL: Duration = Duration::from_secs(1);
/// Child descriptor that receives `ready\n` or `fail:<reason>\n`.
pub const READY_FD: RawFd = 3;
/// Child descriptor that receives the exec'd PID proof.
pub const IDENTITY_FD: RawFd = 4;
/// Child descriptor that receives one bounded bootstrap failure record.
pub const ERROR_FD: RawFd = 5;
/// Child descriptor that carries the optional per-launch writer capability.
///
/// It is the read end of a private pipe created for exactly one launch; the
/// parent writes the secret only after durable startup evidence exists and then
/// closes its end. The child reads it with [`read_capability`], which closes the
/// descriptor, so no later child (native harness) can inherit it.
pub const CAPABILITY_FD: RawFd = 6;
/// Upper bound on the capability secret a launch may carry.
pub const CAPABILITY_MAX_BYTES: usize = 128;
/// Failure kind when the launch executable is unusable before spawning.
pub const FAILURE_KIND_EXECUTABLE_MISSING: &str = "supervisor_executable_missing";
/// Failure kind when the child died before proving its identity.
pub const FAILURE_KIND_BOOTSTRAP: &str = "supervisor_start_failed";
const READY_TOKEN: &str = "ready";
const FAILURE_PREFIX: &str = "fail:";
const RECORD_BYTES: usize = 4096;
const ERROR_READ: Duration = Duration::from_millis(500);
const POLL: Duration = Duration::from_millis(10);
const NOT_READY: &str = "supervisor did not report ready in time";
const INVALID_IDENTITY: &str = "detached supervisor reported an invalid process identity";

/// `<sys/spawn.h>` value; libc 0.2.189 does not export it for Apple targets.
#[cfg(target_os = "macos")]
const POSIX_SPAWN_SETSID: libc::c_short = 0x0400;
#[cfg(target_os = "linux")]
const POSIX_SPAWN_SETSID: libc::c_short = libc::POSIX_SPAWN_SETSID;

/// Which session-creating backend started a child; recorded as launch evidence.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum SpawnBackend {
    /// `posix_spawn` with `POSIX_SPAWN_SETSID`; no parent code runs in the child.
    PosixSpawnSetsid,
    /// Legacy fork, setsid and execve, used only when the flag is explicitly unsupported.
    ForkSetsid,
}

/// Deadlines governing one detached launch.
#[derive(Debug, Clone, Copy)]
pub struct Timeouts {
    /// Spawn-to-READY budget shared by identity proof and READY.
    pub ready: Duration,
    /// Natural-exit wait for a child that failed before the deadline.
    pub dispatch: Duration,
    /// Wait after SIGTERM before SIGKILL.
    pub grace: Duration,
    /// Wait after SIGKILL before cleanup is declared failed.
    pub kill: Duration,
}
impl Default for Timeouts {
    /// Python `launch.py` defaults.
    fn default() -> Self {
        Self {
            ready: READY_TIMEOUT,
            dispatch: DISPATCH_TIMEOUT,
            grace: CLEANUP_GRACE,
            kill: CLEANUP_KILL,
        }
    }
}

/// A supervisor that proved its PID and reported READY.
#[derive(Debug)]
pub struct Launched {
    /// Exact spawned PID, equal to the child's own proof.
    pub pid: i32,
    /// Backend that created the session.
    pub backend: SpawnBackend,
    /// Leader identity when readable; `None` is unknown, never death.
    pub leader: Option<Identity>,
}

/// Diagnosable evidence for a child that never proved its identity.
#[derive(Debug, Clone, Serialize)]
pub struct BootstrapFailure {
    /// `supervisor_executable_missing` or `supervisor_start_failed`.
    pub failure_kind: &'static str,
    /// Stage named by the child's record, or `preflight`.
    pub stage: Option<String>,
    /// Error type named by the child's record.
    pub error_type: Option<String>,
    /// Errno named by an exec failure record.
    pub errno: Option<i64>,
    /// Spawned but never proven PID.
    pub provisional_pid: Option<i32>,
    /// Raw `waitpid` status when the provisional child was reaped.
    pub wait_status: Option<i32>,
    /// Always false: identity was never proven.
    pub proven: bool,
    /// Python-compatible summary text.
    pub message: String,
}

/// Why a detached launch did not hand over ownership.
#[derive(Debug)]
pub enum LaunchError {
    /// The spawn call failed; it is never retried through another backend.
    Spawn(io::Error),
    /// The executable was unusable or the child died before proving its PID.
    Bootstrap(Box<BootstrapFailure>),
    /// The handshake was refused after spawning; the child was reaped unless
    /// the message says cleanup failed.
    Refused {
        /// Spawned PID.
        pid: i32,
        /// Python-compatible refusal text.
        message: String,
    },
}
impl fmt::Display for LaunchError {
    /// Render the Python-compatible message.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Spawn(error) => write!(f, "cannot spawn detached supervisor: {error}"),
            Self::Bootstrap(failure) => f.write_str(&failure.message),
            Self::Refused { message, .. } => f.write_str(message),
        }
    }
}
impl std::error::Error for LaunchError {}

/// Signals sent to one verified group and whether that group disappeared.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct Termination {
    /// Signals actually delivered, in order.
    pub signals: Vec<&'static str>,
    /// Whether the original process group is gone.
    pub group_gone: bool,
}

/// Spawn `program args… --ready-fd 3 --identity-fd 4 --error-fd 5` as a
/// session leader and return only after its PID proof and READY token.
///
/// `on_spawned` runs once with the provisional PID before any proof, for
/// durable startup evidence; its error is handled like a failed handshake.
/// Every failure after spawning reaps the child or reports failed cleanup.
/// This synchronous form carries no capability; asynchronous callers use
/// [`spawn_launch`] so they can await durable evidence between the steps.
pub fn launch_detached(
    program: &Path,
    args: &[&OsStr],
    timeouts: Timeouts,
    on_spawned: impl FnOnce(i32, SpawnBackend) -> Result<(), String>,
) -> Result<Launched, LaunchError> {
    let started = spawn_launch(program, args, timeouts, false)?;
    match on_spawned(started.pid, started.backend) {
        Ok(()) => started.complete(timeouts),
        Err(message) => Err(started.abandon(
            timeouts,
            format!("supervisor startup evidence was not recorded: {message}"),
        )),
    }
}

/// A spawned but not yet proven detached supervisor.
///
/// Splitting the launch lets an asynchronous caller await durable startup
/// evidence while holding the child at its first gate: when a capability was
/// requested the child cannot authenticate (and therefore cannot do anything
/// durable) until [`Self::release_capability`] runs. Dropping the value without
/// calling [`Self::complete`] or [`Self::abandon`] leaves the child to the
/// caller's recovery; both methods own verified cleanup.
#[derive(Debug)]
pub struct StartedLaunch {
    /// Exact spawned PID, not yet proven by the child.
    pub pid: i32,
    /// Backend that created the session.
    pub backend: SpawnBackend,
    /// Single deadline for identity proof, capability hand-over and READY.
    deadline: Instant,
    ready: OwnedFd,
    identity: OwnedFd,
    error: OwnedFd,
    /// Parent write end of the capability pipe until it is released.
    capability: Option<OwnedFd>,
}

/// Preflight the executable and spawn a detached session leader without
/// waiting for any proof.
///
/// With `with_capability` the child additionally inherits the read end of a
/// private pipe as [`CAPABILITY_FD`] and receives `--capability-fd 6` on its
/// command line; the secret itself is never an argument or environment value.
/// The one `timeouts.ready` budget starts here.
///
/// # Errors
/// [`LaunchError::Bootstrap`] when the executable is unusable, and
/// [`LaunchError::Spawn`] when pipes or the spawn call fail (never retried
/// through another backend).
pub fn spawn_launch(
    program: &Path,
    args: &[&OsStr],
    timeouts: Timeouts,
    with_capability: bool,
) -> Result<StartedLaunch, LaunchError> {
    preflight(program)?;
    let (ready_r, ready_w) = cloexec_pipe().map_err(LaunchError::Spawn)?;
    let (identity_r, identity_w) = cloexec_pipe().map_err(LaunchError::Spawn)?;
    let (error_r, error_w) = cloexec_pipe().map_err(LaunchError::Spawn)?;
    let (capability_r, capability_w) = if with_capability {
        let (read, write) = cloexec_pipe().map_err(LaunchError::Spawn)?;
        (Some(read), Some(write))
    } else {
        (None, None)
    };
    let (pid, backend) = spawn_with_fds(
        program,
        args,
        [&ready_w, &identity_w, &error_w],
        capability_r.as_ref(),
    )
    .map_err(LaunchError::Spawn)?;
    drop((ready_w, identity_w, error_w, capability_r));
    Ok(StartedLaunch {
        pid,
        backend,
        deadline: Instant::now() + timeouts.ready,
        ready: ready_r,
        identity: identity_r,
        error: error_r,
        capability: capability_w,
    })
}

impl StartedLaunch {
    /// Write the capability secret to the child and close the parent's end.
    ///
    /// Call only after durable startup evidence exists. The pipe buffer holds
    /// the bounded secret, so this never blocks. The launch's single original
    /// budget still governs: once its deadline passed the secret is NOT granted
    /// (the pipe stays unreleased and unwritten), because the child's READY
    /// window is over and the caller must [`Self::abandon`]. Releasing twice,
    /// releasing a launch spawned without a capability, or an oversize secret is
    /// refused without side effects on the child.
    ///
    /// # Errors
    /// `InvalidInput` for a refused call, `TimedOut` after the original deadline
    /// (no budget is renewed), or the pipe write error (for example the child
    /// already exited); after any error the caller must [`Self::abandon`].
    pub fn release_capability(&mut self, secret: &[u8]) -> io::Result<()> {
        if secret.is_empty() || secret.len() > CAPABILITY_MAX_BYTES {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "capability secret must be 1..=128 bytes",
            ));
        }
        if self.capability.is_none() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "launch has no unreleased capability",
            ));
        }
        if Instant::now() >= self.deadline {
            return Err(io::Error::new(
                io::ErrorKind::TimedOut,
                "launch budget expired before the capability was released",
            ));
        }
        let fd = self.capability.take().expect("presence checked above");
        let mut pipe = std::fs::File::from(fd);
        // Dropping `pipe` after the write closes the parent end, giving the
        // child EOF so it can bound its read.
        io::Write::write_all(&mut pipe, secret)
    }

    /// Prove the child's identity and READY under the remaining launch budget.
    ///
    /// A capability launch must have released its secret first, otherwise the
    /// child is still gated and this fails with a READY timeout and verified
    /// cleanup. Every failure reaps the child or reports failed cleanup.
    pub fn complete(self, timeouts: Timeouts) -> Result<Launched, LaunchError> {
        let mut leader = None;
        let deadline = self.deadline;
        let pid = self.pid;
        let backend = self.backend;
        let result = prove_identity(pid, &self.identity, &self.error, deadline).and_then(|()| {
            leader = verify_group(pid)?;
            wait_ready(pid, &self.ready, deadline)
        });
        drop(self);
        match result {
            Ok(()) => Ok(Launched {
                pid,
                backend,
                leader,
            }),
            Err(error) => Err(finish_failed(pid, leader, error, deadline, timeouts)),
        }
    }

    /// Clean up the not-yet-authenticated child after the caller could not
    /// record durable startup evidence or release the capability.
    ///
    /// Only a verified group is signalled; the returned error carries `message`
    /// unless cleanup itself failed, in which case that failure wins.
    pub fn abandon(self, timeouts: Timeouts, message: String) -> LaunchError {
        let (pid, deadline) = (self.pid, self.deadline);
        drop(self);
        finish_failed(
            pid,
            None,
            LaunchError::Refused { pid, message },
            deadline,
            timeouts,
        )
    }
}

/// Shared failure path: wait for natural exit, else signal only a verified group.
fn finish_failed(
    pid: i32,
    leader: Option<Identity>,
    error: LaunchError,
    deadline: Instant,
    timeouts: Timeouts,
) -> LaunchError {
    if Instant::now() < deadline
        && wait_child(pid, timeouts.dispatch + Duration::from_millis(250)).is_some()
    {
        return error;
    }
    let leader = leader.or_else(|| verify_group(pid).ok().flatten());
    match cleanup(pid, leader.as_ref(), timeouts) {
        Ok(()) => error,
        Err(message) => {
            // Never leave a zombie behind, even when the group outlived cleanup.
            let _ = spawn_reaper(pid);
            LaunchError::Refused { pid, message }
        }
    }
}

/// Child half of the capability hand-over: read the secret from
/// [`CAPABILITY_FD`] until the parent closes its end, then close the descriptor.
///
/// The descriptor is consumed even on failure so a later spawned harness can
/// never inherit it. Reads at most [`CAPABILITY_MAX_BYTES`] before `timeout`.
///
/// # Errors
/// `TimedOut` when the parent keeps the pipe open, `UnexpectedEof` when it
/// closed without a secret, `InvalidData` when the secret is oversize.
pub fn read_capability(fd: RawFd, timeout: Duration) -> io::Result<Vec<u8>> {
    let pipe = inherit(fd)?;
    let raw = pipe.as_raw_fd();
    // The launcher's dup2 cleared close-on-exec so this process could inherit the
    // descriptor. Restore it BEFORE any blocking wait so a concurrent exec in
    // this process (a native harness) can never inherit the secret's pipe; on
    // failure `pipe` drops and closes the descriptor.
    set_cloexec(raw)?;
    let deadline = Instant::now() + timeout;
    let mut secret = Vec::new();
    loop {
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            return Err(io::Error::new(
                io::ErrorKind::TimedOut,
                "capability was not released in time",
            ));
        }
        if !poll_readable(raw, remaining)? {
            continue;
        }
        let mut chunk = [0u8; 64];
        // SAFETY: `chunk` is writable for its full length.
        let read = unsafe { libc::read(raw, chunk.as_mut_ptr().cast(), chunk.len()) };
        match read {
            0 => break,
            n if n > 0 => {
                secret.extend_from_slice(&chunk[..n as usize]);
                if secret.len() > CAPABILITY_MAX_BYTES {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "capability exceeds its bound",
                    ));
                }
            }
            _ => {
                let error = io::Error::last_os_error();
                if error.kind() != io::ErrorKind::Interrupted {
                    return Err(error);
                }
            }
        }
    }
    if secret.is_empty() {
        return Err(io::Error::new(
            io::ErrorKind::UnexpectedEof,
            "launcher closed the capability pipe without a secret",
        ));
    }
    Ok(secret)
}

/// Create a pipe whose both ends are close-on-exec.
pub fn cloexec_pipe() -> io::Result<(OwnedFd, OwnedFd)> {
    let mut fds = [0; 2];
    #[cfg(target_os = "linux")]
    // SAFETY: `fds` is a writable two-element array, as pipe2 requires.
    let code = unsafe { libc::pipe2(fds.as_mut_ptr(), libc::O_CLOEXEC) };
    #[cfg(target_os = "macos")]
    // SAFETY: `fds` is a writable two-element array, as pipe requires.
    let code = unsafe { libc::pipe(fds.as_mut_ptr()) };
    if code < 0 {
        return Err(io::Error::last_os_error());
    }
    // SAFETY: the call above returned two fresh descriptors that nothing else owns.
    let pair = unsafe { (OwnedFd::from_raw_fd(fds[0]), OwnedFd::from_raw_fd(fds[1])) };
    // ponytail: macOS has no pipe2, so a fork in another thread between pipe and
    // FD_CLOEXEC may inherit an end; that only delays EOF within the READY
    // deadline. Our own spawns use POSIX_SPAWN_CLOEXEC_DEFAULT and are immune.
    #[cfg(target_os = "macos")]
    for fd in [&pair.0, &pair.1] {
        // SAFETY: F_SETFD on an owned open descriptor takes no pointers.
        if unsafe { libc::fcntl(fd.as_raw_fd(), libc::F_SETFD, libc::FD_CLOEXEC) } < 0 {
            return Err(io::Error::last_os_error());
        }
    }
    Ok(pair)
}

/// Spawn a session leader, preferring `posix_spawn(POSIX_SPAWN_SETSID)`.
///
/// `fds` become child descriptors 3, 4 and 5; stdio is `/dev/null`. The fork
/// fallback is taken only when the flag is explicitly rejected (`EINVAL` from
/// `posix_spawnattr_setflags`); every other spawn error propagates.
pub fn spawn_session_leader(
    program: &Path,
    args: &[&OsStr],
    fds: [&OwnedFd; 3],
) -> io::Result<(i32, SpawnBackend)> {
    spawn_with_fds(program, args, fds, None)
}

/// [`spawn_session_leader`] with an optional fourth descriptor.
///
/// `capability` (a pipe read end) becomes child descriptor [`CAPABILITY_FD`]
/// and `--capability-fd 6` is appended to the command line; every other
/// descriptor of this process stays closed in the child.
pub fn spawn_with_fds(
    program: &Path,
    args: &[&OsStr],
    fds: [&OwnedFd; 3],
    capability: Option<&OwnedFd>,
) -> io::Result<(i32, SpawnBackend)> {
    let plan = prepare(program, args, fds, capability)?;
    match posix_spawn_setsid(&plan)? {
        Some(pid) => Ok((pid, SpawnBackend::PosixSpawnSetsid)),
        None => fork_setsid(&plan).map(|pid| (pid, SpawnBackend::ForkSetsid)),
    }
}

/// Legacy fork path, public only so tests can exercise it directly.
///
/// The child runs only async-signal-safe libc calls on memory prepared before
/// fork; an exec failure writes one bounded record to descriptor 5's pipe.
pub fn fork_session_leader(program: &Path, args: &[&OsStr], fds: [&OwnedFd; 3]) -> io::Result<i32> {
    fork_setsid(&prepare(program, args, fds, None)?)
}

/// Reap exactly `pid` on a dedicated thread and return its raw wait status.
///
/// ponytail: one blocking thread per live supervisor, as Python's default
/// launcher; switch to a polling registry if supervisor counts grow large.
pub fn spawn_reaper(pid: i32) -> io::Result<JoinHandle<Option<i32>>> {
    std::thread::Builder::new()
        .name(format!("reap-{pid}"))
        .spawn(move || {
            loop {
                let mut status = 0;
                // SAFETY: blocking waitpid on one exact PID with a valid status pointer.
                let waited = unsafe { libc::waitpid(pid, &mut status, 0) };
                if waited == pid {
                    return Some(status);
                }
                if waited < 0 && io::Error::last_os_error().kind() == io::ErrorKind::Interrupted {
                    continue;
                }
                return None;
            }
        })
}

/// Whether `leader` is, right now, the same live process (PID, birth/token)
/// and still the leader of its own group, judged from ONE fresh snapshot.
///
/// Evaluated immediately before EVERY signal so a leader that exited or was
/// reused during a grace wait is never trusted from an earlier observation. A
/// single [`process::inspect`] supplies every fact (PID, birth, token, current
/// group, zombie state); mixing two snapshots could let a reused PID satisfy one
/// check with another process's facts. Identity matching follows the recorded
/// evidence: Rust-written `linux:`/`darwin:` tokens must match exactly, and the
/// birth time must always match. Dead, reused, zombie, unreadable, denied or
/// no-longer-leading all answer `false`.
fn leader_may_be_signalled(leader: &Identity) -> bool {
    let pgid = leader.pid;
    if pgid <= 1 || leader.group != pgid {
        return false;
    }
    let Ok(current) = process::inspect(pgid) else {
        return false;
    };
    let token_matches = if leader.token.starts_with("linux:") || leader.token.starts_with("darwin:")
    {
        current.token == leader.token
    } else {
        true
    };
    current.pid == pgid
        && token_matches
        && current.birth == leader.birth
        && current.group == pgid
        && !current.zombie
}

/// Python `terminate_process_group` without descendant snapshots.
///
/// TERM, then KILL, the group led by `leader`, but only while that exact
/// PID/birth identity is alive and leads its group, re-verified immediately
/// before each signal. A reused, exited, unknown or denied leader is never
/// signalled: if the leader disappears during the TERM grace wait while
/// members remain, no KILL is sent and the result honestly reports
/// `group_gone == false` (cleanup unconfirmed) rather than assuming success.
pub fn terminate_group(
    leader: &Identity,
    grace: Duration,
    kill: Duration,
) -> io::Result<Termination> {
    let pgid = leader.pid;
    let mut signals = Vec::new();
    if await_group_exit(pgid, Duration::ZERO) {
        return Ok(Termination {
            signals,
            group_gone: true,
        });
    }
    if leader_may_be_signalled(leader) && signal_group(pgid, libc::SIGTERM)? {
        signals.push("SIGTERM");
    }
    let mut group_gone = await_group_exit(pgid, grace);
    if !group_gone && leader_may_be_signalled(leader) {
        if signal_group(pgid, libc::SIGKILL)? {
            signals.push("SIGKILL");
        }
        group_gone = await_group_exit(pgid, kill);
    }
    Ok(Termination {
        signals,
        group_gone,
    })
}

/// Child half: prove this exec'd process leads its own session, then close
/// the inherited identity and error descriptors.
pub fn report_identity(identity_fd: RawFd, error_fd: RawFd) -> io::Result<()> {
    let mut identity = inherit(identity_fd)?;
    let mut error = inherit(error_fd)?;
    // SAFETY: getpid and getsid(0) take no pointers.
    let (pid, session) = unsafe { (libc::getpid(), libc::getsid(0)) };
    if session != pid {
        let message = "detached supervisor is not its session leader";
        let record =
            serde_json::json!({"stage":"identity","type":"ValidationError","message":message});
        let _ = io::Write::write_all(&mut error, format!("{record}\n").as_bytes());
        return Err(io::Error::other(message));
    }
    io::Write::write_all(&mut identity, format!("{pid}\n").as_bytes())
}

/// Child half of READY: one `ready` or bounded nonblank `fail:<reason>` line, then close.
///
/// A blank failure reason is rejected before the descriptor is consumed so the
/// parent never receives an ambiguous startup failure token.
pub fn report_ready(ready_fd: RawFd, outcome: Result<(), &str>) -> io::Result<()> {
    if outcome.is_err_and(|reason| reason.trim().is_empty()) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "supervisor failure reason must be nonblank",
        ));
    }
    let mut ready = inherit(ready_fd)?;
    let line = match outcome {
        Ok(()) => format!("{READY_TOKEN}\n"),
        Err(reason) => {
            let reason: String = reason.trim().chars().take(300).collect();
            format!("{FAILURE_PREFIX}{}\n", reason.replace('\n', " "))
        }
    };
    io::Write::write_all(&mut ready, line.as_bytes())
}

/// Set `FD_CLOEXEC` on an open descriptor.
fn set_cloexec(fd: RawFd) -> io::Result<()> {
    // SAFETY: F_GETFD reads the flags of an open descriptor; no pointers.
    let flags = unsafe { libc::fcntl(fd, libc::F_GETFD) };
    if flags < 0 {
        return Err(io::Error::last_os_error());
    }
    // SAFETY: F_SETFD stores plain flags on an open descriptor; no pointers.
    if unsafe { libc::fcntl(fd, libc::F_SETFD, flags | libc::FD_CLOEXEC) } < 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(())
}

/// Take ownership of one inherited bootstrap descriptor named on the command line.
fn inherit(fd: RawFd) -> io::Result<std::fs::File> {
    if fd <= 2 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "bootstrap descriptors must be above stdio",
        ));
    }
    // SAFETY: F_GETFD only checks that the descriptor is open.
    if unsafe { libc::fcntl(fd, libc::F_GETFD) } < 0 {
        return Err(io::Error::last_os_error());
    }
    // SAFETY: the launcher created this descriptor for exactly this role and
    // nothing else in this process owns it.
    Ok(unsafe { std::fs::File::from_raw_fd(fd) })
}

/// C strings and high descriptors prepared before any spawn or fork.
struct Prepared {
    program: CString,
    argv: Vec<CString>,
    envp: Vec<CString>,
    /// Close-on-exec duplicates numbered >= 10, so dup2 onto 3..=6 never clobbers a source.
    /// Index 3, when present, is the capability pipe read end.
    sources: Vec<OwnedFd>,
}

/// Build argv, a consistent environment snapshot and collision-free sources.
fn prepare(
    program: &Path,
    args: &[&OsStr],
    fds: [&OwnedFd; 3],
    capability: Option<&OwnedFd>,
) -> io::Result<Prepared> {
    let program = cstring(program.as_os_str().as_bytes())?;
    let mut argv = vec![program.clone()];
    for arg in args {
        argv.push(cstring(arg.as_bytes())?);
    }
    for (flag, fd) in [
        ("--ready-fd", READY_FD),
        ("--identity-fd", IDENTITY_FD),
        ("--error-fd", ERROR_FD),
    ] {
        argv.push(cstring(flag.as_bytes())?);
        argv.push(cstring(fd.to_string().as_bytes())?);
    }
    if capability.is_some() {
        argv.push(cstring(b"--capability-fd")?);
        argv.push(cstring(CAPABILITY_FD.to_string().as_bytes())?);
    }
    // std's environment lock makes this one snapshot; NUL-bearing entries cannot be passed.
    let envp = std::env::vars_os()
        .filter_map(|(key, value)| {
            let mut entry = key.into_vec();
            entry.push(b'=');
            entry.extend(value.into_vec());
            CString::new(entry).ok()
        })
        .collect();
    let sources = fds
        .iter()
        .copied()
        .chain(capability)
        .map(|fd| {
            // SAFETY: F_DUPFD_CLOEXEC duplicates a caller-owned open descriptor; no pointers.
            let high = unsafe { libc::fcntl(fd.as_raw_fd(), libc::F_DUPFD_CLOEXEC, 10) };
            if high < 0 {
                return Err(io::Error::last_os_error());
            }
            // SAFETY: fcntl returned a fresh descriptor that nothing else owns.
            Ok(unsafe { OwnedFd::from_raw_fd(high) })
        })
        .collect::<io::Result<_>>()?;
    Ok(Prepared {
        program,
        argv,
        envp,
        sources,
    })
}

/// Convert bytes to a C string, refusing interior NUL.
fn cstring(bytes: &[u8]) -> io::Result<CString> {
    CString::new(bytes)
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "launch argument contains NUL"))
}

/// NULL-terminated pointer array borrowing `values`.
fn pointers(values: &[CString]) -> Vec<*mut libc::c_char> {
    values
        .iter()
        .map(|value| value.as_ptr().cast_mut())
        .chain(std::iter::once(std::ptr::null_mut()))
        .collect()
}

/// Map a posix_spawn* return code (an error number, not errno) to a result.
fn check(code: libc::c_int) -> io::Result<()> {
    if code == 0 {
        Ok(())
    } else {
        Err(io::Error::from_raw_os_error(code))
    }
}

/// An empty signal mask and a set holding only SIGPIPE.
fn signal_sets() -> (libc::sigset_t, libc::sigset_t) {
    let mut empty = MaybeUninit::<libc::sigset_t>::uninit();
    let mut sigpipe = MaybeUninit::<libc::sigset_t>::uninit();
    // SAFETY: sigemptyset initializes each stack-owned set before sigaddset and assume_init.
    unsafe {
        libc::sigemptyset(empty.as_mut_ptr());
        libc::sigemptyset(sigpipe.as_mut_ptr());
        libc::sigaddset(sigpipe.as_mut_ptr(), libc::SIGPIPE);
        (empty.assume_init(), sigpipe.assume_init())
    }
}

/// `Ok(None)` only when the C library explicitly rejects the session flag.
fn posix_spawn_setsid(plan: &Prepared) -> io::Result<Option<i32>> {
    let (argv, envp) = (pointers(&plan.argv), pointers(&plan.envp));
    let (empty, sigpipe) = signal_sets();
    let mut attr = MaybeUninit::<libc::posix_spawnattr_t>::uninit();
    let mut actions = MaybeUninit::<libc::posix_spawn_file_actions_t>::uninit();
    // SAFETY: posix_spawnattr_init initializes its out-parameter on success.
    check(unsafe { libc::posix_spawnattr_init(attr.as_mut_ptr()) })?;
    // SAFETY: posix_spawn_file_actions_init initializes its out-parameter on success.
    if let Err(error) = check(unsafe { libc::posix_spawn_file_actions_init(actions.as_mut_ptr()) })
    {
        // SAFETY: attr was initialized above and is destroyed exactly once.
        unsafe { libc::posix_spawnattr_destroy(attr.as_mut_ptr()) };
        return Err(error);
    }
    let (attr, actions) = (attr.as_mut_ptr(), actions.as_mut_ptr());
    let spawned = (|| {
        // Reset SIGPIPE (Rust ignores it) and the mask, as std's spawn does.
        let common = (libc::POSIX_SPAWN_SETSIGDEF | libc::POSIX_SPAWN_SETSIGMASK) as libc::c_short;
        // Only descriptors named by file actions survive into the child.
        #[cfg(target_os = "macos")]
        let common = common | libc::POSIX_SPAWN_CLOEXEC_DEFAULT as libc::c_short;
        // SAFETY: attr is initialized; the flags are plain values.
        let code = unsafe { libc::posix_spawnattr_setflags(attr, POSIX_SPAWN_SETSID | common) };
        if code == libc::EINVAL {
            return Ok(None);
        }
        check(code)?;
        // SAFETY: attr is initialized and `empty` lives on this stack frame.
        check(unsafe { libc::posix_spawnattr_setsigmask(attr, &empty) })?;
        // SAFETY: attr is initialized and `sigpipe` lives on this stack frame.
        check(unsafe { libc::posix_spawnattr_setsigdefault(attr, &sigpipe) })?;
        for (source, target) in
            plan.sources
                .iter()
                .zip([READY_FD, IDENTITY_FD, ERROR_FD, CAPABILITY_FD])
        {
            // SAFETY: actions is initialized; each source (>= 10) differs from every target.
            check(unsafe {
                libc::posix_spawn_file_actions_adddup2(actions, source.as_raw_fd(), target)
            })?;
        }
        for target in 0..3 {
            // SAFETY: actions is initialized and the path is a static NUL-terminated string.
            check(unsafe {
                libc::posix_spawn_file_actions_addopen(
                    actions,
                    target,
                    c"/dev/null".as_ptr(),
                    libc::O_RDWR,
                    0,
                )
            })?;
        }
        let mut pid = 0;
        // SAFETY: program, argv and envp are NUL-terminated strings in NULL-terminated
        // arrays owned by `plan` and this frame, all outliving the call.
        check(unsafe {
            libc::posix_spawn(
                &mut pid,
                plan.program.as_ptr(),
                actions,
                attr,
                argv.as_ptr(),
                envp.as_ptr(),
            )
        })?;
        Ok(Some(pid))
    })();
    // SAFETY: both objects were initialized above and are destroyed exactly once.
    unsafe {
        libc::posix_spawn_file_actions_destroy(actions);
        libc::posix_spawnattr_destroy(attr);
    }
    spawned
}

/// Fork and exec with everything the child touches prepared first.
fn fork_setsid(plan: &Prepared) -> io::Result<i32> {
    let (argv, envp) = (pointers(&plan.argv), pointers(&plan.envp));
    let (empty, _) = signal_sets();
    let sources = [
        plan.sources[0].as_raw_fd(),
        plan.sources[1].as_raw_fd(),
        plan.sources[2].as_raw_fd(),
        plan.sources.get(3).map_or(-1, AsRawFd::as_raw_fd),
    ];
    // SAFETY: fork takes no pointers; the child branch only enters `exec_child`.
    match unsafe { libc::fork() } {
        -1 => Err(io::Error::last_os_error()),
        // SAFETY: this is the freshly forked, single-threaded child.
        0 => unsafe { exec_child(plan.program.as_ptr(), &argv, &envp, sources, &empty) },
        pid => Ok(pid),
    }
}

/// Session creation, descriptor setup and exec in a forked child.
///
/// # Safety
/// Call only in a freshly forked child. It allocates nothing and takes no
/// locks: only setsid, dup2, open, close, signal, sigprocmask, execve, write and _exit.
unsafe fn exec_child(
    program: *const libc::c_char,
    argv: &[*mut libc::c_char],
    envp: &[*mut libc::c_char],
    sources: [RawFd; 4],
    mask: &libc::sigset_t,
) -> ! {
    // SAFETY: the caller guarantees a forked child; every pointer was prepared before fork.
    unsafe {
        if libc::setsid() >= 0
            && libc::dup2(sources[0], READY_FD) == READY_FD
            && libc::dup2(sources[1], IDENTITY_FD) == IDENTITY_FD
            && libc::dup2(sources[2], ERROR_FD) == ERROR_FD
            && (sources[3] < 0 || libc::dup2(sources[3], CAPABILITY_FD) == CAPABILITY_FD)
        {
            let null = libc::open(c"/dev/null".as_ptr(), libc::O_RDWR);
            if null >= 0 {
                for fd in 0..3 {
                    libc::dup2(null, fd);
                }
                if null > 2 {
                    libc::close(null);
                }
            }
            libc::signal(libc::SIGPIPE, libc::SIG_DFL);
            libc::sigprocmask(libc::SIG_SETMASK, mask, std::ptr::null_mut());
            libc::execve(program, argv.as_ptr().cast(), envp.as_ptr().cast());
        }
        let errno = io::Error::last_os_error().raw_os_error().unwrap_or(0);
        write_exec_failure(sources[2], errno);
        libc::_exit(1)
    }
}

/// Python `write_exec_failure` in a fork child: one fixed-size, allocation-free record.
fn write_exec_failure(fd: RawFd, errno: i32) {
    const PREFIX: &[u8] = br#"{"stage":"exec","type":"OSError","message":"exec failed","errno":"#;
    let mut record = [0u8; 96];
    record[..PREFIX.len()].copy_from_slice(PREFIX);
    let mut digits = [0u8; 10];
    let (mut value, mut start) = (errno.unsigned_abs(), digits.len());
    loop {
        start -= 1;
        digits[start] = b'0' + (value % 10) as u8;
        value /= 10;
        if value == 0 {
            break;
        }
    }
    let mut len = PREFIX.len();
    record[len..len + digits.len() - start].copy_from_slice(&digits[start..]);
    len += digits.len() - start;
    record[len..len + 2].copy_from_slice(b"}\n");
    len += 2;
    // SAFETY: `record` is an initialized buffer of at least `len` bytes.
    unsafe { libc::write(fd, record.as_ptr().cast(), len) };
}

/// Python `preflight_executable`: refuse before spawning when the program is gone.
fn preflight(program: &Path) -> Result<(), LaunchError> {
    let usable = program.exists()
        && cstring(program.as_os_str().as_bytes()).is_ok_and(|path| {
            // SAFETY: access reads one NUL-terminated path and writes nothing.
            unsafe { libc::access(path.as_ptr(), libc::X_OK) == 0 }
        });
    if usable {
        return Ok(());
    }
    Err(LaunchError::Bootstrap(Box::new(BootstrapFailure {
        failure_kind: FAILURE_KIND_EXECUTABLE_MISSING,
        stage: Some("preflight".into()),
        error_type: None,
        errno: None,
        provisional_pid: None,
        wait_status: None,
        proven: false,
        message: format!(
            "supervisor executable is gone (release deleted?): {}; reconnect/restart this MCP session",
            program.display()
        ),
    })))
}

/// Python `_read_identity`: the exact spawned PID, or a named refusal.
fn prove_identity(
    pid: i32,
    identity: &OwnedFd,
    error: &OwnedFd,
    deadline: Instant,
) -> Result<(), LaunchError> {
    let refused = |message: &str| LaunchError::Refused {
        pid,
        message: message.into(),
    };
    let line = match read_line(identity.as_raw_fd(), deadline, 32) {
        Ok(Line::Text(line)) => line,
        Ok(Line::Eof) => {
            return Err(LaunchError::Bootstrap(Box::new(diagnose(
                pid,
                error.as_raw_fd(),
            ))));
        }
        Ok(Line::Timeout) => return Err(refused(NOT_READY)),
        Ok(Line::Overflow) | Err(_) => return Err(refused(INVALID_IDENTITY)),
    };
    let reported: i64 = std::str::from_utf8(&line)
        .ok()
        .and_then(|text| text.trim().parse().ok())
        .ok_or_else(|| refused(INVALID_IDENTITY))?;
    if reported <= 1 {
        return Err(refused(
            "detached supervisor reported an unsafe process identity",
        ));
    }
    if reported != i64::from(pid) {
        return Err(refused(
            "detached supervisor reported the wrong process identity",
        ));
    }
    Ok(())
}

/// Python `verify_process_group`: one kernel snapshot must show the PID
/// leading its own group; unreadable or exited identity stays unknown.
fn verify_group(pid: i32) -> Result<Option<Identity>, LaunchError> {
    match process::inspect(pid) {
        Ok(leader) if leader.zombie => Ok(None),
        Ok(leader) if leader.group != pid => Err(LaunchError::Refused {
            pid,
            message: format!("engine pid {pid} is not its process group leader"),
        }),
        Ok(leader) => Ok(Some(leader)),
        Err(_) => Ok(None),
    }
}

/// Python `ReadyChannel.wait`: `ready`, `fail:<reason>`, EOF, timeout or overflow.
fn wait_ready(pid: i32, ready: &OwnedFd, deadline: Instant) -> Result<(), LaunchError> {
    let message = match read_line(ready.as_raw_fd(), deadline, RECORD_BYTES) {
        Ok(Line::Text(line)) => {
            let token = String::from_utf8_lossy(&line);
            if token == READY_TOKEN {
                return Ok(());
            }
            match token.strip_prefix(FAILURE_PREFIX) {
                Some(reason) => format!("supervisor failed to start: {reason}"),
                None => format!("unexpected supervisor readiness token: {token:?}"),
            }
        }
        Ok(Line::Eof) => "supervisor exited before reporting ready".into(),
        Ok(Line::Timeout) => NOT_READY.into(),
        Ok(Line::Overflow) => format!("supervisor readiness frame exceeded {RECORD_BYTES} bytes"),
        Err(error) => format!("supervisor readiness channel failed: {error}"),
    };
    Err(LaunchError::Refused { pid, message })
}

/// Outcome of one bounded line read.
enum Line {
    Text(Vec<u8>),
    Eof,
    Timeout,
    Overflow,
}

/// Read one newline-terminated line of at most `max` bytes before `deadline`.
fn read_line(fd: RawFd, deadline: Instant, max: usize) -> io::Result<Line> {
    let mut buffer = Vec::new();
    loop {
        if let Some(end) = buffer.iter().position(|byte| *byte == b'\n') {
            buffer.truncate(end);
            return Ok(Line::Text(buffer));
        }
        if buffer.len() > max {
            return Ok(Line::Overflow);
        }
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            return Ok(Line::Timeout);
        }
        if !poll_readable(fd, remaining)? {
            continue;
        }
        let mut chunk = [0u8; 256];
        // SAFETY: `chunk` is writable for its full length.
        let read = unsafe { libc::read(fd, chunk.as_mut_ptr().cast(), chunk.len()) };
        match read {
            0 => return Ok(Line::Eof),
            n if n > 0 => buffer.extend_from_slice(&chunk[..n as usize]),
            _ => {
                let error = io::Error::last_os_error();
                if error.kind() != io::ErrorKind::Interrupted {
                    return Err(error);
                }
            }
        }
    }
}

/// Wait up to `wait` for `fd` to become readable (data, EOF or error).
fn poll_readable(fd: RawFd, wait: Duration) -> io::Result<bool> {
    let mut entry = libc::pollfd {
        fd,
        events: libc::POLLIN,
        revents: 0,
    };
    let millis = wait.as_millis().clamp(1, libc::c_int::MAX as u128) as libc::c_int;
    // SAFETY: `entry` is one valid pollfd for the duration of the call.
    let ready = unsafe { libc::poll(&mut entry, 1, millis) };
    if ready < 0 {
        let error = io::Error::last_os_error();
        return if error.kind() == io::ErrorKind::Interrupted {
            Ok(false)
        } else {
            Err(error)
        };
    }
    Ok(ready > 0)
}

/// Python `diagnose_bootstrap_failure`: bounded record read plus bounded reap.
fn diagnose(pid: i32, error_fd: RawFd) -> BootstrapFailure {
    let record = read_record(error_fd);
    let wait_status = wait_child(pid, ERROR_READ).flatten();
    let status = describe_status(wait_status);
    let text = |key: &str| {
        record
            .as_ref()
            .and_then(|fields| fields.get(key))
            .and_then(Value::as_str)
            .map(str::to_owned)
    };
    let (stage, error_type) = (text("stage"), text("type"));
    let message = match &record {
        Some(_) => format!(
            "detached supervisor died before session proof at stage {}: {}: {} ({status})",
            stage
                .as_ref()
                .map_or("None".into(), |stage| format!("'{stage}'")),
            error_type.as_deref().unwrap_or("None"),
            text("message").as_deref().unwrap_or("None"),
        ),
        None => format!(
            "detached supervisor exited before session proof with no bootstrap evidence ({status}); provisional pid {pid} was never proven"
        ),
    };
    BootstrapFailure {
        failure_kind: FAILURE_KIND_BOOTSTRAP,
        errno: record
            .as_ref()
            .and_then(|fields| fields.get("errno"))
            .and_then(Value::as_i64),
        stage,
        error_type,
        provisional_pid: Some(pid),
        wait_status,
        proven: false,
        message,
    }
}

/// Python `read_bootstrap_record`: one bounded nonblocking read of a JSON object line.
fn read_record(fd: RawFd) -> Option<serde_json::Map<String, Value>> {
    if !poll_readable(fd, ERROR_READ).ok()? {
        return None;
    }
    let mut buffer = [0u8; RECORD_BYTES];
    // SAFETY: `buffer` is writable for its full length.
    let read = unsafe { libc::read(fd, buffer.as_mut_ptr().cast(), buffer.len()) };
    if read <= 0 {
        return None;
    }
    let first = buffer[..read as usize]
        .split(|byte| *byte == b'\n')
        .next()?;
    match serde_json::from_slice(first).ok()? {
        Value::Object(fields) => Some(fields),
        _ => None,
    }
}

/// Python `_describe_exit_status`.
fn describe_status(status: Option<i32>) -> String {
    match status {
        None => "exit status unknown".into(),
        Some(status) if libc::WIFSIGNALED(status) => {
            format!("killed by signal {}", libc::WTERMSIG(status))
        }
        Some(status) if libc::WIFEXITED(status) => {
            format!("exit code {}", libc::WEXITSTATUS(status))
        }
        Some(status) => format!("raw status {status}"),
    }
}

/// Reap exactly `pid` within `timeout`: `Some(Some(status))` when reaped here,
/// `Some(None)` when it is no longer this process's child, `None` while it runs.
fn wait_child(pid: i32, timeout: Duration) -> Option<Option<i32>> {
    let until = Instant::now() + timeout;
    loop {
        let mut status = 0;
        // SAFETY: WNOHANG waitpid on one exact PID with a valid status pointer.
        let waited = unsafe { libc::waitpid(pid, &mut status, libc::WNOHANG) };
        if waited == pid {
            return Some(Some(status));
        }
        if waited < 0 && io::Error::last_os_error().raw_os_error() == Some(libc::ECHILD) {
            return Some(None);
        }
        if Instant::now() >= until {
            return None;
        }
        std::thread::sleep(POLL);
    }
}

/// Python `_cleanup`: signal only a verified group, then require absence and reap.
fn cleanup(pid: i32, leader: Option<&Identity>, timeouts: Timeouts) -> Result<(), String> {
    let Some(leader) = leader else {
        return wait_child(pid, POLL)
            .map(drop)
            .ok_or_else(|| "detached supervisor could not be verified for cleanup".to_owned());
    };
    let termination = terminate_group(leader, timeouts.grace, timeouts.kill)
        .map_err(|error| format!("cannot signal process group {pid}: {error}"))?;
    let reaped = wait_child(
        pid,
        timeouts.grace + timeouts.kill + Duration::from_millis(250),
    )
    .is_some();
    if termination.group_gone && reaped {
        Ok(())
    } else {
        Err("detached supervisor process group survived cleanup".to_owned())
    }
}

/// Reap the leader when it is our child, then poll group existence until `budget`.
fn await_group_exit(pgid: i32, budget: Duration) -> bool {
    let until = Instant::now() + budget;
    loop {
        let mut status = 0;
        // SAFETY: WNOHANG waitpid on one exact PID; a non-child reports ECHILD harmlessly.
        unsafe { libc::waitpid(pgid, &mut status, libc::WNOHANG) };
        if !group_alive(pgid) {
            return true;
        }
        if Instant::now() >= until {
            return false;
        }
        std::thread::sleep(POLL);
    }
}

/// Signal-0 group probe; EPERM means the group exists but is not ours.
pub fn group_alive(pgid: i32) -> bool {
    // SAFETY: signal 0 to a group id above 1 only probes existence.
    let probed = unsafe { libc::killpg(pgid, 0) };
    probed == 0 || io::Error::last_os_error().raw_os_error() == Some(libc::EPERM)
}

/// Deliver `signal` to a verified group; `Ok(false)` when it is already gone.
fn signal_group(pgid: i32, signal: libc::c_int) -> io::Result<bool> {
    // SAFETY: callers pass a verified group leader PID above 1, never 0 or -1.
    if unsafe { libc::killpg(pgid, signal) } == 0 {
        return Ok(true);
    }
    let error = io::Error::last_os_error();
    if error.raw_os_error() == Some(libc::ESRCH) {
        Ok(false)
    } else {
        Err(error)
    }
}

#[cfg(test)]
mod tests {
    //! Both spawn backends map the capability pipe to descriptor 6 and announce
    //! it only as a descriptor number, never as a secret.
    use super::*;
    use std::io::Write;

    /// Spawn `plan` through `backend`, release `secret` on the capability pipe
    /// and return what the child's `cat <&6` wrote to its output file.
    fn through(backend: fn(&Prepared) -> io::Result<Option<i32>>, secret: &[u8]) -> String {
        let temp = tempfile::tempdir().unwrap();
        let out = temp.path().join("received");
        let (_ready_r, ready_w) = cloexec_pipe().unwrap();
        let (_identity_r, identity_w) = cloexec_pipe().unwrap();
        let (_error_r, error_w) = cloexec_pipe().unwrap();
        let (capability_r, capability_w) = cloexec_pipe().unwrap();
        let plan = prepare(
            Path::new("/usr/bin/env"),
            &[
                OsStr::new("-i"),
                OsStr::new("PATH=/usr/bin:/bin"),
                OsStr::new("/bin/sh"),
                OsStr::new("-c"),
                OsStr::new(r#"cat <&6 > "$1"; printf ' %s %s' "$8" "$9" >> "$1""#),
                OsStr::new("sh"),
                out.as_os_str(),
            ],
            [&ready_w, &identity_w, &error_w],
            Some(&capability_r),
        )
        .unwrap();
        let pid = backend(&plan).unwrap().expect("backend spawned");
        let mut pipe = std::fs::File::from(capability_w);
        pipe.write_all(secret).unwrap();
        drop(pipe);
        let mut status = 0;
        // SAFETY: blocking waitpid on the exact child just spawned.
        assert_eq!(unsafe { libc::waitpid(pid, &mut status, 0) }, pid);
        assert!(libc::WIFEXITED(status) && libc::WEXITSTATUS(status) == 0);
        std::fs::read_to_string(&out).unwrap()
    }

    /// The `posix_spawn` backend delivers the secret on descriptor 6.
    #[test]
    fn posix_spawn_maps_the_capability_descriptor() {
        assert_eq!(
            through(posix_spawn_setsid, b"posix-secret"),
            "posix-secret --capability-fd 6"
        );
    }

    /// The fork fallback maps the same descriptor the same way.
    #[test]
    fn fork_fallback_maps_the_capability_descriptor() {
        assert_eq!(
            through(|plan| fork_setsid(plan).map(Some), b"fork-secret"),
            "fork-secret --capability-fd 6"
        );
    }

    /// Spawn `sleep` with an empty child environment as the leader of its own
    /// process group; return its captured identity and child handle for teardown.
    fn group_leader() -> (std::process::Child, Identity) {
        use std::os::unix::process::CommandExt;
        let mut command = std::process::Command::new("/bin/sleep");
        command.arg("30").env_clear().process_group(0);
        let child = command.spawn().unwrap();
        let identity = process::inspect(child.id() as i32).unwrap();
        (child, identity)
    }

    /// One fresh snapshot gates signalling: the exact identity passes, and a
    /// changed token, birth, group or PID fails even though everything else is
    /// the live leader's own data (no actual PID reuse is needed).
    #[test]
    fn leader_predicate_requires_every_fact_of_one_snapshot() {
        let (mut child, leader) = group_leader();
        assert_eq!(leader.group, leader.pid, "fixture leads its own group");
        assert!(leader_may_be_signalled(&leader));
        let mut other = leader.clone();
        other.token = format!("{}-other", leader.token);
        assert!(!leader_may_be_signalled(&other), "changed token");
        let mut other = leader.clone();
        other.birth += 1.0;
        assert!(!leader_may_be_signalled(&other), "changed birth");
        let mut other = leader.clone();
        other.group = leader.pid + 1;
        assert!(
            !leader_may_be_signalled(&other),
            "recorded group is not its own"
        );
        let mut other = leader.clone();
        other.pid = 1;
        assert!(
            !leader_may_be_signalled(&other),
            "init is never a leader target"
        );
        // A killed but not yet reaped leader is not signalling-eligible. On Linux the
        // snapshot reports it as a zombie (covered by the explicit zombie check); on
        // macOS the kernel reports it as absent. Either way the answer is `false`.
        child.kill().unwrap();
        let deadline = Instant::now() + Duration::from_secs(5);
        while process::inspect(leader.pid).is_ok_and(|now| !now.zombie) {
            assert!(Instant::now() < deadline, "leader never became a zombie");
            std::thread::sleep(Duration::from_millis(10));
        }
        assert!(!leader_may_be_signalled(&leader), "zombie leader");
        child.wait().unwrap();
        assert!(!leader_may_be_signalled(&leader), "dead leader");
    }

    /// A recorded leader that no longer leads its group (here: the test process,
    /// which belongs to another group) is never a signalling target.
    #[test]
    fn a_non_leader_is_never_signalled() {
        let me = process::inspect(std::process::id() as i32).unwrap();
        assert_ne!(me.group, me.pid);
        let mut claimed = me.clone();
        claimed.group = me.pid;
        assert!(
            !leader_may_be_signalled(&claimed),
            "claimed leadership is re-checked"
        );
    }
}
