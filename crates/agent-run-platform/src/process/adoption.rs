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
        Mutex, MutexGuard, PoisonError,
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
    static CONFIGURED: std::sync::OnceLock<Duration> = std::sync::OnceLock::new();
    *CONFIGURED.get_or_init(|| refresh_override(std::env::var(REFRESH_ENV).ok().as_deref()))
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
/// The command is taken by value: a `pre_exec` handshake is installed on it
/// and the command is handed to `launch` for this single launch, so no hook
/// with consumed descriptors can survive on a reusable command. `launch` must
/// spawn the command (for example through tokio) and `pid` reads the child's
/// PID from the result. Without an active subreaper this is a plain launch.
///
/// With one, the forked child reports its PID and waits before `exec` until
/// its exact `(pid, token)` is registered under the admission lock; the
/// clean-boundary check runs first under the same lock. If registration fails
/// the child never execs and the launch fails. If `launch` fails *after* the
/// program started (tokio can fail while wrapping a live child, dropping it
/// without waiting), the registered child is pinned by pidfd, killed and
/// reaped before the error is returned; a child the standard library already
/// reaped (failed exec) is only released.
pub fn spawn_registered<T>(
    command: std::process::Command,
    launch: impl FnOnce(std::process::Command) -> std::io::Result<T>,
    pid: impl FnOnce(&T) -> Option<u32>,
) -> std::io::Result<(T, SpawnedChild)> {
    #[cfg(target_os = "linux")]
    if subreaper_active() {
        return spawn_registered_linux(command, launch, pid);
    }
    launch(command).map(|child| (child, SpawnedChild(None)))
}

/// Linux body of [`spawn_registered`] with an active subreaper.
#[cfg(target_os = "linux")]
fn spawn_registered_linux<T>(
    mut command: std::process::Command,
    launch: impl FnOnce(std::process::Command) -> std::io::Result<T>,
    pid: impl FnOnce(&T) -> Option<u32>,
) -> std::io::Result<(T, SpawnedChild)> {
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
    // consumed by this one launch, so the hook never outlives its descriptors.
    unsafe {
        command.pre_exec(move || child_handshake(fds));
    }
    let mut state = lock();
    let clean = clean_boundary();
    let (launched, registered) = std::thread::scope(|scope| {
        let state = &mut *state;
        let registrar = scope.spawn(move || register(report_read, release_write, state, clean));
        let launched = launch(command);
        // Without the parent's copies the registrar sees EOF if no child reports.
        drop(report_write);
        drop(release_read);
        (launched, registrar.join().ok().flatten())
    });
    match (launched, registered) {
        (Ok(child), Some(key)) if pid(&child) == u32::try_from(key.0).ok() => {
            Ok((child, SpawnedChild(Some(key))))
        }
        (Ok(_), None) => Err(std::io::Error::other(
            "spawned child could not be registered",
        )),
        (result, Some(key)) => {
            // The program may be running although the launch failed or
            // returned another child: end it before reporting the failure.
            terminate_registered(&key);
            state.release(&key);
            Err(result.err().unwrap_or_else(|| {
                std::io::Error::other("launched child does not match its registration")
            }))
        }
        (Err(error), None) => Err(error),
    }
}

/// Kill and reap one registered child whose launch failed, if it still exists.
///
/// The PID is pinned with a pidfd and the exact start token is verified
/// through it before any signal, so a PID that was already reaped and reused
/// is never touched. `waitid(P_PIDFD)` then collects the status; `ECHILD`
/// means another waiter (for example tokio's orphan queue) collected it first.
#[cfg(target_os = "linux")]
fn terminate_registered(key: &(i32, String)) {
    use std::os::fd::{AsRawFd, FromRawFd};
    /// `P_PIDFD` from linux/wait.h (Linux 5.4).
    const P_PIDFD: libc::idtype_t = 3;
    // SAFETY: pidfd_open takes a PID and flags and returns a new descriptor.
    let raw = unsafe { libc::syscall(libc::SYS_pidfd_open, key.0, 0) };
    if raw < 0 {
        return; // Already reaped (the standard library waits for a failed exec).
    }
    // SAFETY: pidfd_open returned a new descriptor owned by nobody else.
    let pidfd = unsafe { std::os::fd::OwnedFd::from_raw_fd(raw as i32) };
    if super::inspect(key.0).map_or(true, |now| now.token != key.1) {
        return; // The PID now names another process.
    }
    // SAFETY: the pidfd refers to the verified registered child.
    unsafe {
        libc::syscall(
            libc::SYS_pidfd_send_signal,
            pidfd.as_raw_fd(),
            libc::SIGKILL,
            std::ptr::null::<libc::siginfo_t>(),
            0,
        );
    }
    // SAFETY: siginfo_t is plain old data; waitid only writes it.
    let mut info: libc::siginfo_t = unsafe { std::mem::zeroed() };
    loop {
        // SAFETY: waits only for the pinned child; blocks until SIGKILL lands.
        let code = unsafe {
            libc::waitid(
                P_PIDFD,
                pidfd.as_raw_fd() as libc::id_t,
                &mut info,
                libc::WEXITED,
            )
        };
        if code == 0 || std::io::Error::last_os_error().raw_os_error() != Some(libc::EINTR) {
            break;
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
/// a new generation, then release it. Returns the key, or `None` (leaving the
/// child unreleased, so it never execs) on any failure.
#[cfg(target_os = "linux")]
fn register(
    report_read: std::os::fd::OwnedFd,
    release_write: std::os::fd::OwnedFd,
    state: &mut State,
    clean: bool,
) -> Option<(i32, String)> {
    use std::io::{Read, Write};
    let mut pid = [0u8; 4];
    std::fs::File::from(report_read).read_exact(&mut pid).ok()?;
    let pid = i32::from_ne_bytes(pid);
    #[cfg(all(test, target_os = "linux"))]
    if tests::FAIL_REGISTRATION.load(Ordering::SeqCst) {
        return None;
    }
    let identity = super::inspect(pid).ok()?;
    if identity.ppid != std::process::id() as i32 || identity.zombie {
        return None;
    }
    let key = (identity.pid, identity.token);
    state.admit(key.clone(), clean);
    if std::fs::File::from(release_write).write_all(&[1]).is_err() {
        // The child cannot exec without the byte; its spawn fails and is reaped.
        state.release(&key);
        return None;
    }
    Some(key)
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

#[cfg(all(test, target_os = "linux"))]
pub(super) mod tests {
    use super::*;
    use std::process::Command;

    /// Makes the registrar fail after reading the child's PID.
    pub(in crate::process) static FAIL_REGISTRATION: AtomicBool = AtomicBool::new(false);

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
        spawn_registered(
            command,
            |mut command| command.spawn(),
            |child| Some(child.id()),
        )
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

    /// A launch that fails after the program started (as tokio can while
    /// wrapping a live child, dropping it unwaited) still ends that exact
    /// registered child before the failure is reported.
    #[test]
    fn a_post_exec_launch_failure_kills_and_reaps_the_child() {
        isolated(
            "a_post_exec_launch_failure_kills_and_reaps_the_child",
            || {
                let started = std::sync::Mutex::new(None);
                let error = spawn_registered(
                    command("/bin/sleep", &["30"]),
                    |mut command| {
                        let child = command.spawn()?;
                        let identity = super::super::inspect(child.id() as i32)?;
                        *started.lock().unwrap() = Some(identity);
                        drop(child); // like tokio: the std child is dropped unwaited
                        Err::<std::process::Child, _>(std::io::Error::other(
                            "injected wrap failure",
                        ))
                    },
                    |child| Some(child.id()),
                )
                .err()
                .expect("the launch failure is reported");
                assert_eq!(error.to_string(), "injected wrap failure");
                let started = started.lock().unwrap().take().expect("program started");
                assert!(
                    super::super::inspect(started.pid)
                        .map_or(true, |now| now.token != started.token),
                    "the started engine is still running or unreaped"
                );
                assert!(lock().spawned.is_empty(), "registration released");
                assert!(clean_boundary(), "no child left behind");
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
