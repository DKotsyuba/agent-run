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
    admitting: false,
});

/// Signalled whenever an admission ends.
#[cfg(target_os = "linux")]
static ADMITTED: Condvar = Condvar::new();

/// Bound for waiting until another admission ends.
#[cfg(target_os = "linux")]
const ADMISSION_WAIT: Duration = Duration::from_secs(30);

/// Lock-free mirror of [`State::generation`] for cadence decisions only.
static GENERATION: AtomicU64 = AtomicU64::new(0);

/// Bound for a forked child to report its PID during registration.
#[cfg(target_os = "linux")]
const REPORT_BOUND: Duration = Duration::from_secs(10);

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
    /// A spawn is between its clean-boundary check and its registration: its
    /// forked child may exist unregistered, so nothing is adopted meanwhile.
    #[cfg_attr(not(target_os = "linux"), allow(dead_code))]
    admitting: bool,
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

    /// Whether a spawn is being admitted right now (adoption must wait).
    pub(super) fn admitting(&self) -> bool {
        self.admitting
    }

    /// End the current admission and wake waiting spawners.
    fn end_admission(&mut self) {
        self.admitting = false;
        ADMITTED.notify_all();
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
    // Admission: wait for any other admission, then mark this one and check
    // the clean boundary under the lock. The lock itself is NOT held across
    // the spawn; the `admitting` mark keeps observers from adopting the
    // forked child until the registrar registers it and ends the admission.
    let clean = {
        let deadline = std::time::Instant::now() + ADMISSION_WAIT;
        let mut state = lock();
        while state.admitting {
            let left = deadline.saturating_duration_since(std::time::Instant::now());
            if left.is_zero() {
                return Err(std::io::Error::other("another spawn admission is stuck"));
            }
            state = ADMITTED
                .wait_timeout(state, left)
                .unwrap_or_else(PoisonError::into_inner)
                .0;
        }
        state.admitting = true;
        clean_boundary()
    };
    let (spawned, registered) = std::thread::scope(|scope| {
        let registrar = scope.spawn(move || register(report_read, release_write, clean));
        let spawned = command.spawn();
        // Without the parent's copies the registrar sees EOF if no child reports.
        drop(report_write);
        drop(release_read);
        let registered = registrar.join().ok().flatten();
        // The registrar ends the admission on every path; a panic must too.
        let mut state = lock();
        if state.admitting {
            state.end_admission();
        }
        (spawned, registered)
    });
    drop(command);
    match (spawned, registered) {
        (Ok(child), Some(key)) if u32::try_from(key.0).ok() == Some(child.id()) => {
            Ok((child, SpawnedChild(Some(key))))
        }
        (Ok(mut child), key) => {
            // Unreachable by the handshake (no release means no exec), but if
            // the program runs unregistered it is ended here, never trusted.
            let _ = child.kill();
            reap_later(child, Some(SpawnedChild(key)));
            Err(std::io::Error::other(
                "spawned child could not be registered",
            ))
        }
        (Err(error), key) => {
            // The standard library already waited for the failed child.
            if let Some(key) = key {
                lock().release(&key);
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

/// Parent side: read the blocked child's PID, register its exact identity in
/// a new generation, end the admission, then release the child. Returns the
/// key, or `None` (leaving the child unreleased, so it never execs) on any
/// failure; every path ends the admission.
///
/// The wait for the PID report is bounded, so a child stuck before reporting
/// cannot stall other spawns or adoption. Its own launching thread can still
/// wait inside `Command::spawn` until that child exits or execs: the standard
/// library offers no bound there, and the PID needed to end it was never
/// reported. The admission lock is never held during that wait.
#[cfg(target_os = "linux")]
fn register(
    report_read: std::os::fd::OwnedFd,
    release_write: std::os::fd::OwnedFd,
    clean: bool,
) -> Option<(i32, String)> {
    let key = report(report_read);
    let mut state = lock();
    let Some(key) = key else {
        state.end_admission();
        return None;
    };
    state.admit(key.clone(), clean);
    state.end_admission();
    drop(state);
    use std::io::Write;
    if std::fs::File::from(release_write).write_all(&[1]).is_err() {
        // The child cannot exec without the byte; its spawn fails and is reaped.
        lock().release(&key);
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
    let deadline = std::time::Instant::now() + REPORT_BOUND;
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
        std::thread::sleep(Duration::from_millis(
            tests::REGISTRAR_DELAY_MS.load(Ordering::SeqCst),
        ));
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

/// Children waiting for their exit status, with their registrations.
static LATER: Mutex<Vec<(std::process::Child, Option<SpawnedChild>)>> = Mutex::new(Vec::new());

/// Signalled when [`LATER`] gains an entry.
static LATER_READY: Condvar = Condvar::new();

/// Whether the reaper thread is running.
static REAPER: AtomicBool = AtomicBool::new(false);

/// Collect `child`'s exit status whenever it exits, then release `claim`.
///
/// The single managed reaping path for children whose owner went away before
/// their status was collected (a dropped engine transport, an unconfirmed
/// started launch). The child is never killed and never dropped: one
/// process-wide reaper thread polls queued children every 100 ms while any are
/// queued and sleeps otherwise. If that thread cannot be created, the queue is
/// kept and every later call retries it and polls the queue itself.
pub fn reap_later(child: std::process::Child, claim: Option<SpawnedChild>) {
    LATER
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .push((child, claim));
    LATER_READY.notify_one();
    poll_later();
    if !REAPER.swap(true, Ordering::SeqCst)
        && std::thread::Builder::new()
            .name("agent-run-reaper".into())
            .spawn(reaper)
            .is_err()
    {
        REAPER.store(false, Ordering::SeqCst);
    }
}

/// Reap every queued child that has exited; releases happen outside the queue lock.
fn poll_later() -> bool {
    let mut done = Vec::new();
    let empty = {
        let mut queue = LATER.lock().unwrap_or_else(PoisonError::into_inner);
        let mut index = 0;
        while index < queue.len() {
            match queue[index].0.try_wait() {
                Ok(None) => index += 1,
                // Exited, or no longer this process's child: nothing to wait for.
                Ok(Some(_)) | Err(_) => done.push(queue.swap_remove(index).1),
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
            let queue = LATER.lock().unwrap_or_else(PoisonError::into_inner);
            drop(
                LATER_READY
                    .wait_while(queue, |queue| queue.is_empty())
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
    /// Delays the registrar after reading the child's PID, in milliseconds.
    pub(in crate::process) static REGISTRAR_DELAY_MS: std::sync::atomic::AtomicU64 =
        std::sync::atomic::AtomicU64::new(0);

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

    /// The admission lock is not held across the spawn: while a forked child
    /// waits for registration, the lock is free and marks an admission in
    /// flight (so nothing is adopted), and the mark ends with registration.
    #[test]
    fn the_admission_lock_is_free_while_a_spawn_is_in_flight() {
        isolated(
            "the_admission_lock_is_free_while_a_spawn_is_in_flight",
            || {
                REGISTRAR_DELAY_MS.store(500, Ordering::SeqCst);
                let launch = std::thread::spawn(|| gated(command("/bin/sleep", &["30"])));
                std::thread::sleep(Duration::from_millis(150));
                let asked = std::time::Instant::now();
                let state = lock();
                assert!(asked.elapsed() < Duration::from_millis(100), "lock held");
                assert!(state.admitting(), "admission in flight");
                drop(state);
                let (mut child, claim) = launch.join().unwrap().unwrap();
                REGISTRAR_DELAY_MS.store(0, Ordering::SeqCst);
                let identity = super::super::inspect(child.id() as i32).unwrap();
                let state = lock();
                assert!(!state.admitting());
                assert!(state.registered(&identity));
                drop(state);
                child.kill().unwrap();
                child.wait().unwrap();
                claim.reaped();
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
