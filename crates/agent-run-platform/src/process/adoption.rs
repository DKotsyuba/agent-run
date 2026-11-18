//! Linux child-subreaper bookkeeping for one process that owns agent trees.
//!
//! Once [`become_subreaper`] succeeds, orphaned descendants reparent to this
//! process. Two facts make adoption exact by construction:
//!
//! - **Registration before the program runs.** Every child is spawned through
//!   a [`SpawnGate`]: the forked child reports its PID and then waits, before
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
/// through a [`SpawnGate`] afterwards: an unregistered direct child would be
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

/// Registers one spawn before its program runs.
///
/// [`Self::install`] adds a `pre_exec` handshake to the command; [`Self::spawn`]
/// then runs the spawn under the admission lock. Without an active subreaper
/// both are pass-through and nothing is registered.
pub struct SpawnGate(#[cfg(target_os = "linux")] Option<Handshake>);

/// Both pipe pairs of one handshake; every descriptor is close-on-exec.
#[cfg(target_os = "linux")]
struct Handshake {
    /// Parent reads the child's PID here.
    report_read: std::os::fd::OwnedFd,
    /// Child writes its PID here (the parent's copy closes after the spawn).
    report_write: std::os::fd::OwnedFd,
    /// Child waits here for the release byte (the parent's copy closes after the spawn).
    release_read: std::os::fd::OwnedFd,
    /// Parent writes the release byte here only after registration.
    release_write: std::os::fd::OwnedFd,
}

impl SpawnGate {
    /// Prepare `command` so its child waits, before `exec`, for registration.
    pub fn install(command: &mut std::process::Command) -> std::io::Result<Self> {
        #[cfg(not(target_os = "linux"))]
        {
            let _ = command;
            Ok(Self())
        }
        #[cfg(target_os = "linux")]
        Self::install_linux(command)
    }

    /// Linux body of [`Self::install`].
    #[cfg(target_os = "linux")]
    fn install_linux(command: &mut std::process::Command) -> std::io::Result<Self> {
        if !subreaper_active() {
            return Ok(Self(None));
        }
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
        // async-signal-safe functions (close, getpid, write, read) on
        // descriptors inherited from this handshake; it allocates nothing.
        unsafe {
            command.pre_exec(move || child_handshake(fds));
        }
        Ok(Self(Some(Handshake {
            report_read,
            report_write,
            release_read,
            release_write,
        })))
    }

    /// Spawn under the admission lock, registering the child before it execs.
    ///
    /// The clean-boundary check runs first, under the same lock, so nothing
    /// this process spawns can appear between the check and the fork. If the
    /// child cannot be registered it never execs and `spawn` fails; if the
    /// program then fails to exec, its registration is removed (the standard
    /// library already reaped it).
    pub fn spawn<T>(
        self,
        spawn: impl FnOnce() -> std::io::Result<T>,
        pid: impl FnOnce(&T) -> Option<u32>,
    ) -> std::io::Result<(T, SpawnedChild)> {
        #[cfg(not(target_os = "linux"))]
        {
            let _ = pid;
            spawn().map(|child| (child, SpawnedChild(None)))
        }
        #[cfg(target_os = "linux")]
        self.spawn_linux(spawn, pid)
    }

    /// Linux body of [`Self::spawn`].
    #[cfg(target_os = "linux")]
    fn spawn_linux<T>(
        self,
        spawn: impl FnOnce() -> std::io::Result<T>,
        pid: impl FnOnce(&T) -> Option<u32>,
    ) -> std::io::Result<(T, SpawnedChild)> {
        let Some(handshake) = self.0 else {
            return spawn().map(|child| (child, SpawnedChild(None)));
        };
        let Handshake {
            report_read,
            report_write,
            release_read,
            release_write,
        } = handshake;
        let mut state = lock();
        let clean = clean_boundary();
        let (spawned, registered) = std::thread::scope(|scope| {
            let state = &mut *state;
            let registrar = scope.spawn(move || register(report_read, release_write, state, clean));
            let spawned = spawn();
            // Without the parent's copies the registrar sees EOF if no child reports.
            drop(report_write);
            drop(release_read);
            (spawned, registrar.join().ok().flatten())
        });
        match (spawned, registered) {
            (Ok(child), Some(key)) if pid(&child) == u32::try_from(key.0).ok() => {
                Ok((child, SpawnedChild(Some(key))))
            }
            (Ok(_), _) => Err(std::io::Error::other(
                "spawned child could not be registered",
            )),
            (Err(error), key) => {
                if let Some(key) = key {
                    state.release(&key);
                }
                Err(error)
            }
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
/// `waitid(P_ALL, WNOHANG | WNOWAIT)` covers children of every thread, never
/// reaps, and fails with `ECHILD` only when no child exists; the kernel
/// answers atomically, so this is not a PID census.
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
                libc::WEXITED | libc::WNOHANG | libc::WNOWAIT,
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
        let status = Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                &format!("process::adoption::tests::{name}"),
                "--nocapture",
            ])
            .env(CASE, name)
            .status()
            .unwrap();
        assert!(status.success(), "isolated case {name} failed: {status}");
    }

    /// Spawn `command` through a gate.
    fn gated(command: &mut Command) -> std::io::Result<(std::process::Child, SpawnedChild)> {
        let gate = SpawnGate::install(command)?;
        gate.spawn(|| command.spawn(), |child| Some(child.id()))
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
            let error = gated(
                Command::new("/bin/sh").args(["-c", &format!("touch '{}'", marker.display())]),
            )
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
                let (mut child, claim) = gated(Command::new("/bin/sleep").arg("30")).unwrap();
                let identity = super::super::inspect(child.id() as i32).unwrap();
                assert!(lock().registered(&identity), "exact token registered");
                assert!(gated(&mut Command::new("/nonexistent/engine")).is_err());
                assert_eq!(lock().spawned.len(), 1, "failed exec released");
                child.kill().unwrap();
                child.wait().unwrap();
                claim.reaped();
                assert!(lock().spawned.is_empty());
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
            let (mut first, first_claim) = gated(Command::new("/bin/sleep").arg("30")).unwrap();
            assert!(lock().exclusive(&identity(&first)).is_some());
            let (mut second, second_claim) = gated(Command::new("/bin/sleep").arg("30")).unwrap();
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
            let (mut third, third_claim) = gated(Command::new("/bin/sleep").arg("30")).unwrap();
            assert!(
                lock().exclusive(&identity(&third)).is_none(),
                "a zombie child"
            );
            first.wait().unwrap();
            first_claim.reaped();
            third.kill().unwrap();
            third.wait().unwrap();
            third_claim.reaped();
            let (mut fourth, fourth_claim) = gated(Command::new("/bin/sleep").arg("30")).unwrap();
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
