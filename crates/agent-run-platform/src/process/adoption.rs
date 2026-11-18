//! Linux child-subreaper bookkeeping for one process that owns agent trees.
//!
//! Once [`become_subreaper`] succeeds, orphaned descendants reparent to this
//! process. Two facts make adoption exact by construction:
//!
//! - **Registration before the program runs.** Every child is spawned through
//!   [`spawn_registered`]: the forked child reports its PID and then waits, before
//!   `exec`, until the parent registered its exact `(pid, token)` under the
//!   admission lock. A child whose registration fails never execs; its spawn
//!   fails and the standard library reaps it. A registration stays until the
//!   spawner reports the exit status collected ([`SpawnedChild::reaped`]).
//! - **A clean boundary instead of a census.** Under the same lock and before
//!   forking, `waitid(P_ALL, WNOHANG | WNOWAIT)` failing with `ECHILD` proves
//!   atomically that this process has no child at all, so it has no living
//!   descendant. A spawn admitted at such a boundary is *exclusive*: every
//!   orphan that later reparents here descends from it, until another spawn is
//!   admitted. Any other spawn attributes nothing and its owner keeps the
//!   200 ms polling refresh.
//!
//! Process groups and PID listings never decide ownership. The engine is not
//! made a subreaper itself: Bun- and tokio-based engines reap only their own
//! children, so orphans would stay zombies inside the engine.

use std::{
    sync::{
        Condvar, Mutex, MutexGuard, PoisonError,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    time::Duration,
};

/// Whether this process successfully became a Linux child subreaper.
static SUBREAPER: AtomicBool = AtomicBool::new(false);

/// Registered spawns and the admission generation, behind the admission lock.
static STATE: Mutex<State> = Mutex::new(State {
    spawned: Vec::new(),
    generation: 0,
});

/// Lock-free mirror of [`State::generation`] for cadence decisions only.
static GENERATION: AtomicU64 = AtomicU64::new(0);

/// Bound for a forked child to report its PID during registration.
#[cfg(target_os = "linux")]
const REPORT_BOUND: Duration = Duration::from_secs(10);

/// The PID-report bound in effect (shortened only by unit tests).
#[cfg(target_os = "linux")]
fn report_bound() -> Duration {
    #[cfg(test)]
    if let Some(ms) = std::num::NonZeroU64::new(tests::REPORT_BOUND_MS.load(Ordering::SeqCst)) {
        return Duration::from_millis(ms.get());
    }
    REPORT_BOUND
}

/// Default mid-run durable refresh period for an exclusive owner.
pub const DURABLE_REFRESH: Duration = Duration::from_secs(2);

/// Descendant refresh period without exclusive adoption (macOS, no
/// subreaper, or a spawn admitted while another child existed).
pub const POLLING_REFRESH: Duration = Duration::from_millis(200);

/// Environment override, in milliseconds, of [`DURABLE_REFRESH`].
pub const REFRESH_ENV: &str = "AGENT_RUN_DESCENDANT_REFRESH_MS";

/// Admission state shared by spawners and observers.
pub(super) struct State {
    /// Direct children this process spawned and has not reported reaped.
    spawned: Vec<Spawn>,
    /// Advanced by every registered spawn.
    #[cfg_attr(not(target_os = "linux"), allow(dead_code))]
    generation: u64,
}

/// One registered direct child.
struct Spawn {
    /// Exact `(pid, token)` of the child.
    key: (i32, String),
    /// Generation in which this spawn was admitted at a clean boundary, if it was.
    exclusive: Option<u64>,
}

#[cfg(target_os = "linux")]
impl State {
    /// Whether `process` is a registered spawned child (exact PID and token).
    pub(super) fn registered(&self, process: &super::Identity) -> bool {
        self.spawned
            .iter()
            .any(|spawn| spawn.key.0 == process.pid && spawn.key.1 == process.token)
    }

    /// The current admission generation.
    pub(super) fn generation(&self) -> u64 {
        self.generation
    }
}

impl State {
    /// The generation in which `leader` was admitted exclusively, if it was.
    fn exclusive(&self, leader: &super::Identity) -> Option<u64> {
        self.spawned
            .iter()
            .find(|spawn| spawn.key.0 == leader.pid && spawn.key.1 == leader.token)
            .and_then(|spawn| spawn.exclusive)
    }

    /// Register one child admitted in a new generation.
    #[cfg(target_os = "linux")]
    fn admit(&mut self, key: (i32, String), clean: bool) {
        self.generation += 1;
        GENERATION.store(self.generation, Ordering::SeqCst);
        let exclusive = clean.then_some(self.generation);
        self.spawned.push(Spawn { key, exclusive });
    }

    /// Forget one registration whose exit status was collected.
    fn release(&mut self, key: &(i32, String)) {
        if let Some(at) = self.spawned.iter().position(|spawn| spawn.key == *key) {
            self.spawned.swap_remove(at);
        }
    }
}

/// Take the admission lock. Every update is a single push or removal, so a
/// panic while it was held cannot leave a half-registered spawn; a poisoned
/// lock is recovered rather than disabling ownership checks.
pub(super) fn lock() -> MutexGuard<'static, State> {
    STATE.lock().unwrap_or_else(PoisonError::into_inner)
}

/// The exclusive admission generation of `leader`, read under the lock.
pub(super) fn exclusive_generation(leader: &super::Identity) -> Option<u64> {
    lock().exclusive(leader)
}

/// Return the current admission generation without taking the lock.
pub(super) fn generation() -> u64 {
    GENERATION.load(Ordering::SeqCst)
}

/// Make this process the Linux child subreaper for all its descendants.
///
/// Call once, before this process spawns any child, and spawn every child
/// through [`spawn_registered`] afterwards: an unregistered direct child would be
/// indistinguishable from an adopted orphan. Orphaned descendants (double
/// fork, `setsid`, `nohup`, regrouping) then reparent here instead of init, so
/// termination points reach them by parentage. Other platforms return
/// `Unsupported` and keep the polling refresh.
pub fn become_subreaper() -> std::io::Result<()> {
    #[cfg(target_os = "linux")]
    {
        // SAFETY: PR_SET_CHILD_SUBREAPER takes one integer flag and no pointers.
        if unsafe { libc::prctl(libc::PR_SET_CHILD_SUBREAPER, 1, 0, 0, 0) } != 0 {
            return Err(std::io::Error::last_os_error());
        }
        SUBREAPER.store(true, Ordering::SeqCst);
        Ok(())
    }
    #[cfg(not(target_os = "linux"))]
    Err(std::io::Error::new(
        std::io::ErrorKind::Unsupported,
        "child subreaper is Linux-only",
    ))
}

/// Return whether [`become_subreaper`] succeeded in this process.
pub fn subreaper_active() -> bool {
    SUBREAPER.load(Ordering::SeqCst)
}

/// Mid-run durable refresh period of an exclusive owner: [`DURABLE_REFRESH`],
/// or [`REFRESH_ENV`] milliseconds within 200..=600000.
pub fn durable_refresh_interval() -> Duration {
    *DURABLE.get_or_init(|| refresh_override(std::env::var(REFRESH_ENV).ok().as_deref()))
}

/// The durable refresh period, fixed on first use.
static DURABLE: std::sync::OnceLock<Duration> = std::sync::OnceLock::new();

/// Pin the durable refresh period before its first use, within 200..=600000
/// milliseconds. Returns whether this value is now in effect (it is not once
/// the period was read or pinned, or for an out-of-range value).
pub fn pin_durable_refresh_interval(period: Duration) -> bool {
    let ms = u64::try_from(period.as_millis()).unwrap_or(u64::MAX);
    (200..=600_000).contains(&ms) && DURABLE.set(period).is_ok()
}

/// Parse a refresh override; absent or out-of-range values keep the default.
fn refresh_override(value: Option<&str>) -> Duration {
    value
        .and_then(|text| text.trim().parse::<u64>().ok())
        .filter(|ms| (200..=600_000).contains(ms))
        .map_or(DURABLE_REFRESH, Duration::from_millis)
}

/// Exclusive claim on one spawned direct child's exit status.
///
/// While it exists the child is never adopted, captured through adoption,
/// signalled through adoption or reaped by an owner. Call [`Self::reaped`]
/// only after the spawner itself collected the exit status. Dropping the claim
/// without that keeps the registration for the life of this process, because
/// another waiter (for example tokio's orphan queue) may still own the status.
#[must_use = "report the child reaped once its exit status was collected"]
pub struct SpawnedChild(Option<(i32, String)>);

impl SpawnedChild {
    /// Release the claim after the spawner collected the child's exit status.
    pub fn reaped(mut self) {
        if let Some(key) = self.0.take() {
            lock().release(&key);
        }
    }
}

/// Launch exactly one child of `command`, registered before its program runs.
///
/// The command is consumed and spawned here exactly once, so its `pre_exec`
/// handshake can never run again on a reused command. The result is the
/// standard library child (the OS launch); wrapping it for an async runtime
/// is the caller's separate step, and a caller whose wrapping fails still
/// owns this child and its claim. Without an active subreaper this is a plain
/// `spawn` with an empty claim.
///
/// With one, the forked child reports its PID and waits before `exec` until
/// its exact `(pid, token)` is registered under the admission lock; the
/// clean-boundary check runs first under the same lock. An `Err` is positive
/// evidence that no program is running: a refused registration stops the
/// child before `exec`, and the standard library waits for a child whose
/// `exec` failed before it returns the error.
pub fn spawn_registered(
    mut command: std::process::Command,
) -> std::io::Result<(std::process::Child, SpawnedChild)> {
    #[cfg(target_os = "linux")]
    if subreaper_active() {
        return spawn_registered_linux(command);
    }
    command.spawn().map(|child| (child, SpawnedChild(None)))
}

/// Linux body of [`spawn_registered`] with an active subreaper.
#[cfg(target_os = "linux")]
fn spawn_registered_linux(
    mut command: std::process::Command,
) -> std::io::Result<(std::process::Child, SpawnedChild)> {
    use std::os::fd::AsRawFd;
    use std::os::unix::process::CommandExt;
    let (report_read, report_write) = pipe()?;
    let (release_read, release_write) = pipe()?;
    let fds = [
        report_read.as_raw_fd(),
        report_write.as_raw_fd(),
        release_read.as_raw_fd(),
        release_write.as_raw_fd(),
    ];
    // SAFETY: the hook runs in the forked child before exec and calls only
    // async-signal-safe functions (close, getpid, write, read) on descriptors
    // inherited from this handshake; it allocates nothing. The command is
    // spawned once below and then dropped, so the hook never outlives them.
    unsafe {
        command.pre_exec(move || child_handshake(fds));
    }
    // The admission lock is held from the clean-boundary check through the
    // fork to the registration, so no observer can see the new child
    // unregistered and no other spawn can interleave. The registrar's bounded
    // wait for the PID report refuses a child that never reports, which then
    // never execs, so the spawn returns.
    let mut state = lock();
    let clean = clean_boundary();
    let (spawned, registered) = std::thread::scope(|scope| {
        let registrar_state = &mut *state;
        let registrar = std::thread::Builder::new()
            .name("agent-run-registrar".into())
            .spawn_scoped(scope, move || {
                register(report_read, release_write, registrar_state, clean)
            })?;
        let spawned = command.spawn();
        // Without the parent's copies the registrar sees EOF if no child reports.
        drop(report_write);
        drop(release_read);
        Ok::<_, std::io::Error>((spawned, registrar.join().ok().flatten()))
    })?;
    drop(command);
    match (spawned, registered) {
        (Ok(child), Some(key)) if u32::try_from(key.0).ok() == Some(child.id()) => {
            Ok((child, SpawnedChild(Some(key))))
        }
        (Ok(mut child), key) => {
            // Unreachable by the handshake (no release means no exec), but if
            // the program runs unregistered it is ended here, never trusted.
            drop(state);
            let _ = child.kill();
            reap_later(child, Some(SpawnedChild(key)));
            Err(std::io::Error::other(
                "spawned child could not be registered",
            ))
        }
        (Err(error), key) => {
            // The standard library already waited for the failed child.
            if let Some(key) = key {
                state.release(&key);
            }
            Err(error)
        }
    }
}

/// Create one close-on-exec pipe.
#[cfg(target_os = "linux")]
fn pipe() -> std::io::Result<(std::os::fd::OwnedFd, std::os::fd::OwnedFd)> {
    use std::os::fd::FromRawFd;
    let mut fds = [0; 2];
    // SAFETY: `fds` is a writable two-element array, as pipe2 requires.
    let code = unsafe { libc::pipe2(fds.as_mut_ptr(), libc::O_CLOEXEC) };
    if code != 0 {
        return Err(std::io::Error::last_os_error());
    }
    // SAFETY: pipe returned two new descriptors owned by nobody else.
    Ok(unsafe {
        (
            std::os::fd::OwnedFd::from_raw_fd(fds[0]),
            std::os::fd::OwnedFd::from_raw_fd(fds[1]),
        )
    })
}

/// Child side of the handshake, between fork and exec: report the PID, then
/// wait for the release byte. Any failure refuses the exec.
#[cfg(target_os = "linux")]
fn child_handshake(
    [report_read, report_write, release_read, release_write]: [i32; 4],
) -> std::io::Result<()> {
    // SAFETY: async-signal-safe calls on this child's inherited copies only.
    unsafe {
        libc::close(report_read);
        libc::close(release_write);
        let pid = libc::getpid().to_ne_bytes();
        let mut sent = 0;
        // Test-only: a child that never reports (an atomic load is
        // async-signal-safe).
        #[cfg(test)]
        if tests::SKIP_REPORT.load(Ordering::SeqCst) {
            sent = pid.len();
        }
        while sent < pid.len() {
            let n = libc::write(report_write, pid[sent..].as_ptr().cast(), pid.len() - sent);
            if n > 0 {
                sent += n as usize;
            } else if n < 0 && *libc::__errno_location() == libc::EINTR {
                continue;
            } else {
                return Err(std::io::Error::from_raw_os_error(libc::ECANCELED));
            }
        }
        libc::close(report_write);
        let mut byte = 0u8;
        let n = loop {
            let n = libc::read(release_read, (&mut byte as *mut u8).cast(), 1);
            if n < 0 && *libc::__errno_location() == libc::EINTR {
                continue;
            }
            break n;
        };
        libc::close(release_read);
        if n == 1 && byte == 1 {
            Ok(())
        } else {
            Err(std::io::Error::from_raw_os_error(libc::ECANCELED))
        }
    }
}

/// Parent side, under the admission lock: read the blocked child's PID,
/// register its exact identity in a new generation, then release it. Returns
/// the key, or `None` (leaving the child unreleased, so it never execs) on any
/// failure.
///
/// The wait for the PID report is bounded ([`REPORT_BOUND`]): a child that
/// never reports is refused and the spawn returns. A child *stopped* before
/// exec (for example by SIGSTOP or a tracer) keeps the standard library's
/// exec-status pipe open, so `Command::spawn` and therefore the admission lock
/// wait until it continues or dies; the supervisor never stops its children.
#[cfg(target_os = "linux")]
fn register(
    report_read: std::os::fd::OwnedFd,
    release_write: std::os::fd::OwnedFd,
    state: &mut State,
    clean: bool,
) -> Option<(i32, String)> {
    let key = report(report_read)?;
    state.admit(key.clone(), clean);
    use std::io::Write;
    if std::fs::File::from(release_write).write_all(&[1]).is_err() {
        // The child cannot exec without the byte; its spawn fails and is reaped.
        state.release(&key);
        return None;
    }
    Some(key)
}

/// Read and verify the forked child's PID report within [`REPORT_BOUND`].
#[cfg(target_os = "linux")]
fn report(report_read: std::os::fd::OwnedFd) -> Option<(i32, String)> {
    use std::io::Read;
    use std::os::fd::AsRawFd;
    let mut ready = libc::pollfd {
        fd: report_read.as_raw_fd(),
        events: libc::POLLIN,
        revents: 0,
    };
    let deadline = std::time::Instant::now() + report_bound();
    loop {
        let left = deadline.saturating_duration_since(std::time::Instant::now());
        // SAFETY: one valid pollfd for a descriptor this function owns.
        let n = unsafe { libc::poll(&mut ready, 1, left.as_millis().min(i32::MAX as u128) as i32) };
        if n > 0 {
            break;
        }
        if n == 0 || std::io::Error::last_os_error().raw_os_error() != Some(libc::EINTR) {
            return None;
        }
    }
    let mut pid = [0u8; 4];
    std::fs::File::from(report_read).read_exact(&mut pid).ok()?;
    let pid = i32::from_ne_bytes(pid);
    #[cfg(all(test, target_os = "linux"))]
    {
        if tests::FAIL_REGISTRATION.load(Ordering::SeqCst) {
            return None;
        }
        // Test-only: report entry and wait for the test to let it proceed.
        let gate = tests::REGISTRAR_GATE
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .take();
        if let Some((entered, proceed)) = gate {
            let _ = entered.send(());
            let _ = proceed.recv_timeout(Duration::from_secs(10));
        }
    }
    let identity = super::inspect(pid).ok()?;
    if identity.ppid != std::process::id() as i32 || identity.zombie {
        return None;
    }
    Some((identity.pid, identity.token))
}

/// Whether this process has no child at all, live or zombie, right now.
///
/// `waitid(P_ALL, WNOHANG | WNOWAIT | __WALL)` covers children of every
/// thread and of every exit signal (not only `SIGCHLD` children; adopted
/// orphans always signal `SIGCHLD`), never reaps, and fails with `ECHILD` only
/// when no child exists; the kernel answers atomically, so this is not a PID
/// census.
#[cfg(target_os = "linux")]
fn clean_boundary() -> bool {
    {
        // SAFETY: siginfo_t is plain old data; waitid only writes it.
        let mut info: libc::siginfo_t = unsafe { std::mem::zeroed() };
        // SAFETY: valid out-pointer; WNOWAIT and WNOHANG neither reap nor block.
        let code = unsafe {
            libc::waitid(
                libc::P_ALL,
                0,
                &mut info,
                libc::WEXITED | libc::WNOHANG | libc::WNOWAIT | libc::__WALL,
            )
        };
        code == -1 && std::io::Error::last_os_error().raw_os_error() == Some(libc::ECHILD)
    }
}

/// Children waiting for their exit status and the reaper state, behind one lock.
static LATER: Mutex<Later> = Mutex::new(Later {
    queue: Vec::new(),
    reaper: Reaper::Idle,
});

/// Queued children and who reaps them. Every state transition, and every
/// "queue empty, so stop" decision, happens under this one lock, so an
/// enqueue can never observe a reaper that has already decided to stop.
struct Later {
    /// Children waiting for their exit status, with their registrations.
    queue: Vec<(std::process::Child, Option<SpawnedChild>)>,
    /// Who is responsible for the queue right now.
    reaper: Reaper,
}

/// Who polls the queue.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Reaper {
    /// Nobody; the next enqueue starts a reaper.
    Idle,
    /// A caller (or the fallback) is creating the reaper thread; it decides
    /// the next state under the lock once creation succeeded or failed.
    Starting,
    /// The reaper thread runs (forever).
    Running,
    /// A task on an async runtime polls because the thread could not start.
    Fallback,
}

/// Signalled when [`LATER`] gains an entry.
static LATER_READY: Condvar = Condvar::new();

/// Collect `child`'s exit status whenever it exits, then release `claim`.
///
/// The single managed reaping path for children whose owner went away before
/// their status was collected (a dropped engine transport, an unconfirmed
/// started launch). The child is never killed and never dropped while its
/// exit is uncertain: one process-wide reaper thread polls queued children
/// every 100 ms while any are queued and sleeps otherwise. If that thread
/// cannot be created, a task on the current async runtime polls instead (and
/// keeps retrying the thread); without either, the next enqueue retries.
pub fn reap_later(child: std::process::Child, claim: Option<SpawnedChild>) {
    LATER
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .queue
        .push((child, claim));
    LATER_READY.notify_one();
    poll_later();
    ensure_reaper();
}

/// Make sure someone polls a non-empty queue: claim `Idle → Starting`, try
/// the thread, then settle on `Running`, `Fallback` (a runtime task) or
/// `Idle` (no thread, no runtime: the next enqueue retries).
fn ensure_reaper() {
    {
        let mut later = LATER.lock().unwrap_or_else(PoisonError::into_inner);
        if later.reaper != Reaper::Idle || later.queue.is_empty() {
            return;
        }
        later.reaper = Reaper::Starting;
    }
    #[cfg(all(test, target_os = "linux"))]
    tests::pause_gate(&tests::STARTING_GATE);
    if spawn_reaper_thread() {
        LATER.lock().unwrap_or_else(PoisonError::into_inner).reaper = Reaper::Running;
        return;
    }
    let runtime = tokio::runtime::Handle::try_current().ok();
    let mut later = LATER.lock().unwrap_or_else(PoisonError::into_inner);
    let Some(runtime) = runtime else {
        later.reaper = Reaper::Idle;
        return;
    };
    later.reaper = Reaper::Fallback;
    drop(later);
    // Built before the spawn and moved whole into the task: a task cancelled
    // or never polled (runtime shutdown) still returns the state to `Idle`.
    let guard = FallbackGuard { armed: true };
    runtime.spawn(async move {
        let mut guard = guard;
        loop {
            tokio::time::sleep(Duration::from_millis(100)).await;
            poll_later();
            let mut later = LATER.lock().unwrap_or_else(PoisonError::into_inner);
            if later.queue.is_empty() {
                // Stop: observed empty, state relinquished and guard disarmed
                // in one locked step.
                later.reaper = Reaper::Idle;
                guard.armed = false;
                drop(later);
                #[cfg(all(test, target_os = "linux"))]
                tests::pause_gate(&tests::STOP_GATE);
                return;
            }
            // Retry the thread; enqueues meanwhile see `Starting` and rely
            // on this task, which stays alive until the outcome is known.
            later.reaper = Reaper::Starting;
            drop(later);
            let started = spawn_reaper_thread();
            let mut later = LATER.lock().unwrap_or_else(PoisonError::into_inner);
            if started {
                later.reaper = Reaper::Running;
                guard.armed = false;
                return;
            }
            later.reaper = Reaper::Fallback;
        }
    });
}

/// Returns the state to `Idle` if its fallback task ends abnormally
/// (cancelled, or never polled). A task that stops normally relinquishes the
/// state itself under the queue lock and disarms this guard first.
struct FallbackGuard {
    /// Whether the fallback state still belongs to this task.
    armed: bool,
}

impl Drop for FallbackGuard {
    fn drop(&mut self) {
        if self.armed {
            LATER.lock().unwrap_or_else(PoisonError::into_inner).reaper = Reaper::Idle;
        }
    }
}

/// Create the reaper thread; returns whether it started.
fn spawn_reaper_thread() -> bool {
    #[cfg(all(test, target_os = "linux"))]
    if tests::FAIL_REAPER_THREAD.load(Ordering::SeqCst) {
        return false;
    }
    std::thread::Builder::new()
        .name("agent-run-reaper".into())
        .spawn(reaper)
        .is_ok()
}

/// Whether a queued child's exit is established and its entry may go.
///
/// A collected status establishes it. `ECHILD` (another waiter collected the
/// status) establishes it only when the registered identity is provably gone
/// (the PID is absent or names another start token); an unregistered entry
/// then has nothing left to protect. Any other error keeps the entry.
fn exit_established(child: &mut std::process::Child, claim: Option<&SpawnedChild>) -> bool {
    match child.try_wait() {
        Ok(Some(_)) => true,
        Ok(None) => false,
        Err(error) if error.raw_os_error() == Some(libc::ECHILD) => {
            match claim.and_then(|claim| claim.0.as_ref()) {
                None => true,
                Some((pid, token)) => super::inspect(*pid).map_or_else(
                    |error| matches!(error.raw_os_error(), Some(libc::ENOENT) | Some(libc::ESRCH)),
                    |now| now.token != *token,
                ),
            }
        }
        Err(_) => false,
    }
}

/// Reap every queued child whose exit is established; releases happen outside
/// the queue lock. Returns whether the queue is empty.
fn poll_later() -> bool {
    let mut done = Vec::new();
    let empty = {
        let mut later = LATER.lock().unwrap_or_else(PoisonError::into_inner);
        let queue = &mut later.queue;
        let mut index = 0;
        while index < queue.len() {
            let (child, claim) = &mut queue[index];
            if exit_established(child, claim.as_ref()) {
                done.push(queue.swap_remove(index).1);
            } else {
                index += 1;
            }
        }
        queue.is_empty()
    };
    for claim in done.into_iter().flatten() {
        claim.reaped();
    }
    empty
}

/// The reaper thread: poll while children are queued, sleep otherwise.
fn reaper() {
    loop {
        if poll_later() {
            let later = LATER.lock().unwrap_or_else(PoisonError::into_inner);
            drop(
                LATER_READY
                    .wait_while(later, |later| later.queue.is_empty())
                    .unwrap_or_else(PoisonError::into_inner),
            );
        } else {
            std::thread::sleep(Duration::from_millis(100));
        }
    }
}

#[cfg(all(test, target_os = "linux"))]
pub(super) mod tests {
    use super::*;
    use std::process::Command;

    /// Makes the registrar fail after reading the child's PID.
    pub(in crate::process) static FAIL_REGISTRATION: AtomicBool = AtomicBool::new(false);
    /// Makes the forked child skip its PID report.
    pub(in crate::process) static SKIP_REPORT: AtomicBool = AtomicBool::new(false);
    /// Overrides the PID-report bound, in milliseconds (0 keeps the default).
    pub(in crate::process) static REPORT_BOUND_MS: std::sync::atomic::AtomicU64 =
        std::sync::atomic::AtomicU64::new(0);
    /// The registrar, once it read a PID, signals entry and waits to proceed.
    pub(in crate::process) static REGISTRAR_GATE: Gate = Mutex::new(None);
    /// Refuses to start the reaper thread, forcing the runtime fallback.
    pub(in crate::process) static FAIL_REAPER_THREAD: AtomicBool = AtomicBool::new(false);
    /// A one-shot pause point: signals `entered`, then waits (bounded) to proceed.
    pub(in crate::process) type Gate =
        Mutex<Option<(std::sync::mpsc::Sender<()>, std::sync::mpsc::Receiver<()>)>>;
    /// Pauses `ensure_reaper` while the state is `Starting`.
    pub(in crate::process) static STARTING_GATE: Gate = Mutex::new(None);
    /// Pauses a fallback right after its locked stop decision.
    pub(in crate::process) static STOP_GATE: Gate = Mutex::new(None);

    /// Pass through `gate` if armed: signal entry, wait up to 10 s to proceed.
    pub(in crate::process) fn pause_gate(gate: &Gate) {
        let armed = gate.lock().unwrap_or_else(PoisonError::into_inner).take();
        if let Some((entered, proceed)) = armed {
            let _ = entered.send(());
            let _ = proceed.recv_timeout(Duration::from_secs(10));
        }
    }

    /// Arm `gate`; returns its entry receiver and proceed sender.
    fn arm(gate: &Gate) -> (std::sync::mpsc::Receiver<()>, std::sync::mpsc::Sender<()>) {
        let (entered_tx, entered) = std::sync::mpsc::channel();
        let (proceed, proceed_rx) = std::sync::mpsc::channel();
        *gate.lock().unwrap() = Some((entered_tx, proceed_rx));
        (entered, proceed)
    }

    /// The current reaper state.
    fn state() -> Reaper {
        LATER.lock().unwrap().reaper
    }

    /// Wait (bounded) until none of `pids` exists any more.
    fn wait_reaped(pids: &[i32]) {
        let deadline = std::time::Instant::now() + Duration::from_secs(10);
        while pids.iter().any(|pid| super::super::inspect(*pid).is_ok()) {
            assert!(std::time::Instant::now() < deadline, "a child was stranded");
            std::thread::sleep(Duration::from_millis(20));
        }
    }

    /// Names the case a re-executed unit-test process must run.
    const CASE: &str = "AGENT_RUN_ADOPTION_UNIT_CASE";

    /// Run `body` as a subreaper in a fresh process of this test binary.
    fn isolated(name: &str, body: fn()) {
        if std::env::var(CASE).as_deref() == Ok(name) {
            become_subreaper().unwrap();
            body();
            return;
        }
        let output = Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                &format!("process::adoption::tests::{name}"),
                "--nocapture",
            ])
            .env(CASE, name)
            .output()
            .unwrap();
        let stdout = String::from_utf8_lossy(&output.stdout);
        assert!(
            output.status.success() && stdout.contains("test result: ok. 1 passed"),
            "isolated case {name} failed: {}\n{stdout}{}",
            output.status,
            String::from_utf8_lossy(&output.stderr)
        );
        print!("{stdout}");
    }

    /// Launch `command` once through the registration handshake.
    fn gated(command: Command) -> std::io::Result<(std::process::Child, SpawnedChild)> {
        spawn_registered(command)
    }

    /// An owned command for `program` with `args`.
    fn command(program: &str, args: &[&str]) -> Command {
        let mut command = Command::new(program);
        command.args(args);
        command
    }

    /// Absent, malformed or out-of-range overrides keep the 2 s durable default.
    #[test]
    fn durable_refresh_override_is_bounded() {
        assert_eq!(refresh_override(None), DURABLE_REFRESH);
        assert_eq!(refresh_override(Some("x")), DURABLE_REFRESH);
        assert_eq!(refresh_override(Some("199")), DURABLE_REFRESH);
        assert_eq!(refresh_override(Some("600001")), DURABLE_REFRESH);
        assert_eq!(refresh_override(Some(" 5000 ")), Duration::from_secs(5));
    }

    /// A child whose registration fails never runs its program: the spawn
    /// fails, the child is already reaped and nothing stays registered.
    #[test]
    fn a_failed_registration_refuses_the_spawn() {
        isolated("a_failed_registration_refuses_the_spawn", || {
            let marker = std::env::temp_dir().join(format!("gate-{}", std::process::id()));
            FAIL_REGISTRATION.store(true, Ordering::SeqCst);
            let error = gated(command(
                "/bin/sh",
                &["-c", &format!("touch '{}'", marker.display())],
            ))
            .err()
            .expect("unregistered spawn refused");
            FAIL_REGISTRATION.store(false, Ordering::SeqCst);
            assert_eq!(error.raw_os_error(), Some(libc::ECANCELED), "{error}");
            assert!(!marker.exists(), "the program ran unregistered");
            assert!(clean_boundary(), "the refused child was not reaped");
            assert!(lock().spawned.is_empty());
            assert_eq!(generation(), 0);
        });
    }

    /// The exact identity is registered before the program runs; a program
    /// that fails to exec leaves no registration behind.
    #[test]
    fn registration_precedes_exec_and_exec_failure_is_released() {
        isolated(
            "registration_precedes_exec_and_exec_failure_is_released",
            || {
                let (mut child, claim) = gated(command("/bin/sleep", &["30"])).unwrap();
                let identity = super::super::inspect(child.id() as i32).unwrap();
                assert!(lock().registered(&identity), "exact token registered");
                assert!(gated(command("/nonexistent/engine", &[])).is_err());
                assert_eq!(lock().spawned.len(), 1, "failed exec released");
                child.kill().unwrap();
                child.wait().unwrap();
                claim.reaped();
                assert!(lock().spawned.is_empty());
            },
        );
    }

    /// The admission lock is held through the registration: while the
    /// registrar handles a forked child the lock cannot be taken, and an
    /// observer waiting for it only gets it once that child is registered.
    #[test]
    fn an_observer_never_sees_a_forked_child_unregistered() {
        isolated("an_observer_never_sees_a_forked_child_unregistered", || {
            let (entered, proceed) = arm(&REGISTRAR_GATE);
            let launch = std::thread::spawn(|| gated(command("/bin/sleep", &["30"])));
            if entered.recv_timeout(Duration::from_secs(10)).is_err() {
                panic!(
                    "the registrar never ran: {:?}",
                    launch.join().map(|r| r.map(|_| ()))
                );
            }
            assert!(
                STATE.try_lock().is_err(),
                "the lock is free mid-registration"
            );
            let observer = std::thread::spawn(|| lock().spawned.len());
            proceed.send(()).unwrap();
            let (mut child, claim) = launch.join().unwrap().unwrap();
            assert_eq!(
                observer.join().unwrap(),
                1,
                "observed an unregistered child"
            );
            let identity = super::super::inspect(child.id() as i32).unwrap();
            assert!(lock().registered(&identity));
            child.kill().unwrap();
            child.wait().unwrap();
            claim.reaped();
        });
    }

    /// With the reaper thread refused, the runtime fallback reaps every child
    /// even when enqueues race its stop decision, and a fallback cancelled by
    /// runtime shutdown does not suppress later fallbacks.
    #[test]
    fn the_fallback_reaper_strands_no_child() {
        isolated("the_fallback_reaper_strands_no_child", || {
            FAIL_REAPER_THREAD.store(true, Ordering::SeqCst);
            let spawn = |seconds: &str| {
                let child = command("/bin/sleep", &[seconds]).spawn().unwrap();
                let pid = child.id() as i32;
                (child, pid)
            };
            let wait_gone = |pids: &[i32]| {
                let deadline = std::time::Instant::now() + Duration::from_secs(10);
                while pids.iter().any(|pid| super::super::inspect(*pid).is_ok()) {
                    assert!(std::time::Instant::now() < deadline, "a child was stranded");
                    std::thread::sleep(Duration::from_millis(20));
                }
            };
            let runtime = tokio::runtime::Builder::new_multi_thread()
                .worker_threads(2)
                .enable_all()
                .build()
                .unwrap();
            // Many enqueues from several threads while fallbacks start and stop.
            let pids = runtime.block_on(async {
                let mut pids = Vec::new();
                for round in 0..20 {
                    let (child, pid) = spawn(if round % 2 == 0 { "0.05" } else { "0.15" });
                    pids.push(pid);
                    let handle = tokio::runtime::Handle::current();
                    std::thread::spawn(move || {
                        let _entered = handle.enter();
                        reap_later(child, None);
                    })
                    .join()
                    .unwrap();
                    tokio::time::sleep(Duration::from_millis(30 * (round % 5))).await;
                }
                pids
            });
            wait_gone(&pids);
            // A fallback cancelled by runtime shutdown leaves its child queued
            // but returns the state to `Idle`, so the next enqueue starts a new one.
            let (first, first_pid) = spawn("0.3");
            runtime.block_on(async { reap_later(first, None) });
            assert_eq!(state(), Reaper::Fallback);
            drop(runtime);
            assert_eq!(state(), Reaper::Idle, "a cancelled fallback kept its state");
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap();
            let (second, second_pid) = spawn("0.1");
            runtime.block_on(async {
                reap_later(second, None);
                let deadline = std::time::Instant::now() + Duration::from_secs(10);
                while [first_pid, second_pid]
                    .iter()
                    .any(|pid| super::super::inspect(*pid).is_ok())
                {
                    assert!(std::time::Instant::now() < deadline, "a child was stranded");
                    tokio::time::sleep(Duration::from_millis(20)).await;
                }
            });
            assert!(LATER.lock().unwrap().queue.is_empty());
            FAIL_REAPER_THREAD.store(false, Ordering::SeqCst);
        });
    }

    /// An enqueue landing after a fallback decided to stop, but before that
    /// task returned, starts a new fallback (the state was relinquished in the
    /// same locked step as the stop decision) and its child is reaped.
    #[test]
    fn an_enqueue_after_a_fallback_stops_is_still_reaped() {
        isolated("an_enqueue_after_a_fallback_stops_is_still_reaped", || {
            FAIL_REAPER_THREAD.store(true, Ordering::SeqCst);
            let runtime = tokio::runtime::Builder::new_multi_thread()
                .worker_threads(2)
                .enable_all()
                .build()
                .unwrap();
            let (stopped, proceed) = arm(&STOP_GATE);
            let first = command("/bin/sleep", &["0.05"]).spawn().unwrap();
            let first_pid = first.id() as i32;
            runtime.block_on(async { reap_later(first, None) });
            // The first fallback reaped its child and decided to stop.
            stopped.recv_timeout(Duration::from_secs(10)).unwrap();
            assert!(super::super::inspect(first_pid).is_err());
            assert_eq!(state(), Reaper::Idle, "state outlived the stop decision");
            let second = command("/bin/sleep", &["0.05"]).spawn().unwrap();
            let second_pid = second.id() as i32;
            {
                let _entered = runtime.enter();
                reap_later(second, None);
            }
            assert_eq!(state(), Reaper::Fallback, "no fallback for the new child");
            proceed.send(()).unwrap();
            wait_reaped(&[second_pid]);
            FAIL_REAPER_THREAD.store(false, Ordering::SeqCst);
        });
    }

    /// An enqueue that finds a reaper `Starting` relies on it; whether the
    /// thread then fails (fallback) or starts (running), that child is reaped.
    #[test]
    fn an_enqueue_while_the_reaper_is_starting_is_still_reaped() {
        isolated(
            "an_enqueue_while_the_reaper_is_starting_is_still_reaped",
            || {
                let runtime = tokio::runtime::Builder::new_multi_thread()
                    .worker_threads(2)
                    .enable_all()
                    .build()
                    .unwrap();
                for start_fails in [true, false] {
                    FAIL_REAPER_THREAD.store(true, Ordering::SeqCst);
                    let (starting, proceed) = arm(&STARTING_GATE);
                    let first = command("/bin/sleep", &["0.05"]).spawn().unwrap();
                    let first_pid = first.id() as i32;
                    let handle = runtime.handle().clone();
                    let starter = std::thread::spawn(move || {
                        let _entered = handle.enter();
                        reap_later(first, None);
                    });
                    starting.recv_timeout(Duration::from_secs(10)).unwrap();
                    assert_eq!(state(), Reaper::Starting);
                    let second = command("/bin/sleep", &["0.05"]).spawn().unwrap();
                    let second_pid = second.id() as i32;
                    {
                        let _entered = runtime.enter();
                        reap_later(second, None);
                    }
                    assert_eq!(state(), Reaper::Starting, "the second enqueue took over");
                    FAIL_REAPER_THREAD.store(start_fails, Ordering::SeqCst);
                    proceed.send(()).unwrap();
                    starter.join().unwrap();
                    assert_eq!(
                        state(),
                        if start_fails {
                            Reaper::Fallback
                        } else {
                            Reaper::Running
                        }
                    );
                    wait_reaped(&[first_pid, second_pid]);
                    if start_fails {
                        let deadline = std::time::Instant::now() + Duration::from_secs(10);
                        while state() != Reaper::Idle {
                            assert!(
                                std::time::Instant::now() < deadline,
                                "fallback never stopped"
                            );
                            std::thread::sleep(Duration::from_millis(20));
                        }
                    }
                }
                FAIL_REAPER_THREAD.store(false, Ordering::SeqCst);
            },
        );
    }

    /// A child that never reports its PID is refused within the report bound:
    /// it never execs, the spawn returns an error, it is reaped and nothing
    /// stays registered or admitted.
    #[test]
    fn a_child_that_never_reports_is_refused_and_the_spawn_returns() {
        isolated(
            "a_child_that_never_reports_is_refused_and_the_spawn_returns",
            || {
                let marker = std::env::temp_dir().join(format!("silent-{}", std::process::id()));
                SKIP_REPORT.store(true, Ordering::SeqCst);
                REPORT_BOUND_MS.store(300, Ordering::SeqCst);
                let started = std::time::Instant::now();
                let result = gated(command(
                    "/bin/sh",
                    &["-c", &format!("touch '{}'", marker.display())],
                ));
                SKIP_REPORT.store(false, Ordering::SeqCst);
                REPORT_BOUND_MS.store(0, Ordering::SeqCst);
                assert!(result.is_err(), "the silent child was not refused");
                assert!(
                    started.elapsed() < Duration::from_secs(5),
                    "spawn not bounded"
                );
                assert!(!marker.exists(), "the program ran unregistered");
                assert!(clean_boundary(), "the refused child was not reaped");
                assert!(lock().spawned.is_empty());
                assert_eq!(generation(), 0);
            },
        );
    }

    /// A registered child handed to the reaper keeps its registration until
    /// the reaper collected its status, then releases it.
    #[test]
    fn the_reaper_releases_a_registration_after_collecting_the_status() {
        isolated(
            "the_reaper_releases_a_registration_after_collecting_the_status",
            || {
                let (child, claim) = gated(command("/bin/sh", &["-c", "sleep 0.3"])).unwrap();
                let identity = super::super::inspect(child.id() as i32).unwrap();
                reap_later(child, Some(claim));
                assert!(lock().registered(&identity), "registered while running");
                let deadline = std::time::Instant::now() + Duration::from_secs(10);
                while !lock().spawned.is_empty() {
                    assert!(std::time::Instant::now() < deadline, "never released");
                    std::thread::sleep(Duration::from_millis(20));
                }
                assert!(super::super::inspect(identity.pid).is_err(), "reaped");
                assert!(clean_boundary());
            },
        );
    }

    /// When another waiter took a child's status (`ECHILD`), a registered
    /// entry stays queued while its registered identity is still alive and is
    /// released only once that identity is provably gone.
    #[test]
    fn an_uncertain_exit_keeps_the_registration_until_the_identity_is_gone() {
        let mut stolen = command("/bin/sh", &["-c", "exit 0"]).spawn().unwrap();
        let mut status = 0;
        let pid = stolen.id() as i32;
        // SAFETY: waits for this test's own exact child, taking its status.
        let waited = unsafe { libc::waitpid(pid, &mut status, 0) };
        assert_eq!(waited, pid);
        assert!(
            stolen.try_wait().is_err(),
            "ECHILD after the status was taken"
        );
        let mut live = command("/bin/sleep", &["30"]).spawn().unwrap();
        let alive = super::super::inspect(live.id() as i32).unwrap();
        let claim = SpawnedChild(Some((alive.pid, alive.token.clone())));
        assert!(
            !exit_established(&mut stolen, Some(&claim)),
            "identity still alive"
        );
        live.kill().unwrap();
        live.wait().unwrap();
        assert!(exit_established(&mut stolen, Some(&claim)), "identity gone");
        assert!(exit_established(&mut stolen, None), "nothing registered");
        std::mem::forget(claim);
    }

    /// Exclusivity follows the kernel's child list, not timing: only a spawn
    /// admitted with no child at all (live or zombie) is exclusive.
    #[test]
    fn only_a_spawn_at_a_clean_boundary_is_exclusive() {
        isolated("only_a_spawn_at_a_clean_boundary_is_exclusive", || {
            let identity =
                |child: &std::process::Child| super::super::inspect(child.id() as i32).unwrap();
            let (mut first, first_claim) = gated(command("/bin/sleep", &["30"])).unwrap();
            assert!(lock().exclusive(&identity(&first)).is_some());
            let (mut second, second_claim) = gated(command("/bin/sleep", &["30"])).unwrap();
            assert!(
                lock().exclusive(&identity(&second)).is_none(),
                "a live child"
            );
            second.kill().unwrap();
            first.kill().unwrap();
            // Kill both, but leave `first` an unreaped zombie.
            second.wait().unwrap();
            second_claim.reaped();
            while !identity(&first).zombie {
                std::thread::sleep(Duration::from_millis(5));
            }
            let (mut third, third_claim) = gated(command("/bin/sleep", &["30"])).unwrap();
            assert!(
                lock().exclusive(&identity(&third)).is_none(),
                "a zombie child"
            );
            first.wait().unwrap();
            first_claim.reaped();
            third.kill().unwrap();
            third.wait().unwrap();
            third_claim.reaped();
            let (mut fourth, fourth_claim) = gated(command("/bin/sleep", &["30"])).unwrap();
            assert!(
                lock().exclusive(&identity(&fourth)).is_some(),
                "clean again"
            );
            fourth.kill().unwrap();
            fourth.wait().unwrap();
            fourth_claim.reaped();
        });
    }
}
