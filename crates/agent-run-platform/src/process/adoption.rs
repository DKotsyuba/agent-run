//! Linux child-subreaper bookkeeping for one process that owns agent trees.
//!
//! Once [`become_subreaper`] succeeds, orphaned descendants reparent to this
//! process. Telling an adopted orphan from a child this process spawned itself
//! is done only by explicit ownership: every spawn goes through
//! [`spawn_child`], which registers the exact `(pid, token)` until the spawner
//! reports it reaped. Process groups are never used for that distinction, since
//! an engine descendant may join any group of the shared session.
//!
//! An orphan cannot name the tree it came from, so it is attributed to an owner
//! only when that owner's spawn was *exclusive*: two native snapshots taken
//! under the admission lock right after the spawn showed no other live direct
//! child. Every later living descendant then belongs to that spawn's tree,
//! until another spawn is admitted (which advances the generation and ends the
//! exclusivity). Without exclusivity nothing is attributed and owners keep the
//! 200 ms polling refresh.
//!
//! One lock serializes spawn admission with adoption and reaping, so a fresh
//! spawn can never be mistaken for an orphan and no PID is reaped twice.

use super::Identity;
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

/// Descendant refresh period without exclusive adoption (macOS, no subreaper,
/// or several possible trees).
pub const POLLING_REFRESH: Duration = Duration::from_millis(200);

/// Environment override, in milliseconds, of [`DURABLE_REFRESH`].
pub const REFRESH_ENV: &str = "AGENT_RUN_DESCENDANT_REFRESH_MS";

/// Admission state shared by spawners and observers.
pub(super) struct State {
    /// Direct children this process spawned and has not reported reaped.
    spawned: Vec<Spawn>,
    /// Advanced by every registered spawn.
    generation: u64,
}

/// One registered direct child.
struct Spawn {
    /// Exact `(pid, token)` of the child.
    key: (i32, String),
    /// Generation in which this spawn was proven the only live tree, if it was.
    exclusive: Option<u64>,
}

impl State {
    /// Whether `process` is a registered spawned child (exact PID and token).
    pub(super) fn registered(&self, process: &Identity) -> bool {
        self.spawned
            .iter()
            .any(|spawn| spawn.key.0 == process.pid && spawn.key.1 == process.token)
    }

    /// The current admission generation if `leader`'s spawn is still exclusive.
    pub(super) fn exclusive(&self, leader: &Identity) -> Option<u64> {
        self.spawned
            .iter()
            .find(|spawn| spawn.key.0 == leader.pid && spawn.key.1 == leader.token)
            .and_then(|spawn| spawn.exclusive)
            .filter(|generation| *generation == self.generation)
    }

    /// The current admission generation.
    pub(super) fn generation(&self) -> u64 {
        self.generation
    }
}

/// Take the admission lock. A panic while it was held cannot leave a
/// half-registered spawn (every update is a single push or removal), so a
/// poisoned lock is recovered rather than disabling ownership checks.
pub(super) fn lock() -> MutexGuard<'static, State> {
    STATE.lock().unwrap_or_else(PoisonError::into_inner)
}

/// Return the current admission generation without taking the lock.
pub(super) fn generation() -> u64 {
    GENERATION.load(Ordering::SeqCst)
}

/// Make this process the Linux child subreaper for all its descendants.
///
/// Call once, before this process spawns any child, and spawn every child
/// through [`spawn_child`] afterwards: an unregistered direct child would be
/// indistinguishable from an adopted orphan. Orphaned descendants (double
/// fork, `setsid`, `nohup`) then reparent here instead of init, so termination
/// points reach them by parentage. Other platforms return `Unsupported` and
/// keep the polling refresh.
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
            let mut state = lock();
            if let Some(at) = state.spawned.iter().position(|spawn| spawn.key == key) {
                state.spawned.swap_remove(at);
            }
        }
    }
}

/// Spawn one direct child under the admission lock and register it.
///
/// `spawn` runs while the lock is held, so no observer can see the new child
/// before it is registered. `pid` reads the child's PID from the result. With
/// an active subreaper the spawn is also tested for exclusivity: two snapshots
/// must show it as the only live direct child. Without a subreaper nothing is
/// registered or adopted.
pub fn spawn_child<T>(
    spawn: impl FnOnce() -> std::io::Result<T>,
    pid: impl FnOnce(&T) -> Option<u32>,
) -> std::io::Result<(T, SpawnedChild)> {
    if !subreaper_active() {
        return spawn().map(|child| (child, SpawnedChild(None)));
    }
    let mut state = lock();
    let child = spawn()?;
    let key = pid(&child)
        .and_then(|pid| i32::try_from(pid).ok())
        .and_then(|pid| super::inspect(pid).ok())
        .map(|identity| (identity.pid, identity.token));
    let Some(key) = key else {
        // An unreadable new child was already reaped by its waiter; any later
        // process with its PID is unrelated, so there is nothing to protect.
        return Ok((child, SpawnedChild(None)));
    };
    state.generation += 1;
    GENERATION.store(state.generation, Ordering::SeqCst);
    let exclusive = (only_live_child(&key) && only_live_child(&key)).then_some(state.generation);
    state.spawned.push(Spawn {
        key: key.clone(),
        exclusive,
    });
    Ok((child, SpawnedChild(Some(key))))
}

/// Whether one native snapshot shows `key` as this process's only live direct child.
fn only_live_child(key: &(i32, String)) -> bool {
    let me = std::process::id() as i32;
    super::processes().is_ok_and(|all| {
        all.iter()
            .filter(|p| p.ppid == me && !p.zombie)
            .all(|p| p.pid == key.0 && p.token == key.1)
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Absent, malformed or out-of-range overrides keep the 2 s durable default.
    #[test]
    fn durable_refresh_override_is_bounded() {
        assert_eq!(refresh_override(None), DURABLE_REFRESH);
        assert_eq!(refresh_override(Some("x")), DURABLE_REFRESH);
        assert_eq!(refresh_override(Some("199")), DURABLE_REFRESH);
        assert_eq!(refresh_override(Some("600001")), DURABLE_REFRESH);
        assert_eq!(refresh_override(Some(" 5000 ")), Duration::from_secs(5));
    }
}
