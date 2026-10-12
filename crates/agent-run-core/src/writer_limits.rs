//! Bounded admission for the private writer socket: connection slots per
//! authority class and the byte budget for frames being assembled.
//!
//! The broker must keep serving ownership and cancel traffic when ordinary
//! clients flood it, so capacity is split by authority: supervisors and
//! service bootstraps own a reserved pool that operators can never consume, a
//! small pre-authentication pool bounds unauthenticated sockets, and frame
//! assembly memory keeps a share only critical frames may use. All counters are
//! released by dropping the returned permit, including on cancellation.
use std::sync::{Arc, Mutex};

/// Authority class of a connection after its hello was verified.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SlotClass {
    /// Supervisors and service bootstraps (reserved pool).
    Owner,
    /// CLI and collector clients.
    Operator,
}

/// Connection pool sizes; the sum is the total number of sockets served.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ConnectionLimits {
    /// Unauthenticated sockets waiting for a valid hello.
    pub preauth: usize,
    /// Verified supervisor / service-exec connections.
    pub owner: usize,
    /// Verified CLI / collector connections.
    pub operator: usize,
}

impl Default for ConnectionLimits {
    /// 256 sockets: 160 owner (64 supervisors x 1 plus headroom for re-pins and
    /// services), 80 operator, 16 pre-auth. Provisional until measured by the
    /// 64-process load tier.
    fn default() -> Self {
        Self {
            preauth: 16,
            owner: 160,
            operator: 80,
        }
    }
}

impl ConnectionLimits {
    /// Total sockets the listener may hold open.
    pub fn total(&self) -> usize {
        self.preauth + self.owner + self.operator
    }
}

/// Live counters of the three pools; each is guarded by the shared mutex and
/// never exceeds its limit in [`ConnectionLimits`].
#[derive(Debug, Default)]
struct Counts {
    /// Unauthenticated sockets currently holding a pre-auth slot.
    preauth: usize,
    /// Verified supervisor / service-exec sockets (the reserved pool).
    owner: usize,
    /// Verified CLI / collector sockets.
    operator: usize,
}

/// Shared connection accounting.
#[derive(Debug, Clone)]
pub struct ConnectionSlots {
    /// Immutable pool sizes the counters are checked against.
    limits: ConnectionLimits,
    /// Counters shared with every outstanding [`SlotPermit`], so a permit
    /// returns its slot even after the accounting handle was dropped.
    counts: Arc<Mutex<Counts>>,
}

/// One held connection slot; released on drop.
#[derive(Debug)]
pub struct SlotPermit {
    /// The counters this permit decrements exactly once, on drop.
    counts: Arc<Mutex<Counts>>,
    /// The pool whose counter this permit holds one unit of.
    pool: Pool,
}

/// Which pool a permit belongs to.
#[derive(Debug, Clone, Copy)]
enum Pool {
    /// Unauthenticated sockets awaiting a valid hello.
    Preauth,
    /// Verified supervisor / service-exec sockets.
    Owner,
    /// Verified CLI / collector sockets.
    Operator,
}

impl Drop for SlotPermit {
    /// Returns the slot to its pool.
    fn drop(&mut self) {
        let mut counts = self.counts.lock().unwrap_or_else(|e| e.into_inner());
        match self.pool {
            Pool::Preauth => counts.preauth -= 1,
            Pool::Owner => counts.owner -= 1,
            Pool::Operator => counts.operator -= 1,
        }
    }
}

impl ConnectionSlots {
    /// Creates empty accounting for `limits`.
    pub fn new(limits: ConnectionLimits) -> Self {
        Self {
            limits,
            counts: Arc::default(),
        }
    }

    /// Takes a pre-authentication slot, or `None` when that pool is full.
    pub fn preauth(&self) -> Option<SlotPermit> {
        self.take(Pool::Preauth)
    }

    /// Takes the verified-class slot for an authenticated connection, or `None`
    /// when its pool is exhausted. The caller drops its pre-auth permit only
    /// after this succeeds, so a refused connection frees its pre-auth slot.
    pub fn verified(&self, class: SlotClass) -> Option<SlotPermit> {
        self.take(match class {
            SlotClass::Owner => Pool::Owner,
            SlotClass::Operator => Pool::Operator,
        })
    }

    /// Takes one slot of `pool` if below its limit.
    fn take(&self, pool: Pool) -> Option<SlotPermit> {
        let mut counts = self.counts.lock().unwrap_or_else(|e| e.into_inner());
        let (used, limit) = match pool {
            Pool::Preauth => (&mut counts.preauth, self.limits.preauth),
            Pool::Owner => (&mut counts.owner, self.limits.owner),
            Pool::Operator => (&mut counts.operator, self.limits.operator),
        };
        if *used >= limit {
            return None;
        }
        *used += 1;
        Some(SlotPermit {
            counts: self.counts.clone(),
            pool,
        })
    }
}

/// Memory budget for frame bodies being assembled before hand-off.
///
/// Ordinary frames may use at most `total - critical_reserve`; critical frames
/// may use the whole budget, so a flood of journal frames can never make a
/// legitimate ownership checkpoint unreadable.
#[derive(Debug, Clone)]
pub struct AssemblyBudget {
    /// Bytes available to all frames together, in bytes.
    total: usize,
    /// Bytes of `total` that only critical frames may use; never above `total`.
    critical_reserve: usize,
    /// Bytes currently reserved by outstanding [`AssemblyPermit`]s, shared with
    /// them so a permit releases its bytes even after the budget handle drops.
    used: Arc<Mutex<usize>>,
}

/// Bytes reserved for one frame body; released on drop.
#[derive(Debug)]
pub struct AssemblyPermit {
    /// Bytes this permit holds, returned to `used` exactly once on drop.
    bytes: usize,
    /// The budget's shared reserved-bytes counter.
    used: Arc<Mutex<usize>>,
}

impl Drop for AssemblyPermit {
    /// Returns the bytes to the budget.
    fn drop(&mut self) {
        let mut used = self.used.lock().unwrap_or_else(|e| e.into_inner());
        *used -= self.bytes;
    }
}

impl AssemblyBudget {
    /// Default: 96 MiB total with 48 MiB only critical frames may use.
    pub const DEFAULT_TOTAL: usize = 96 * 1024 * 1024;
    /// Default critical-only share.
    pub const DEFAULT_CRITICAL_RESERVE: usize = 48 * 1024 * 1024;

    /// Creates an empty budget.
    pub fn new(total: usize, critical_reserve: usize) -> Self {
        Self {
            total,
            critical_reserve: critical_reserve.min(total),
            used: Arc::default(),
        }
    }

    /// Reserves `bytes`, or `None` when the applicable ceiling would be exceeded.
    ///
    /// `critical` is the transport's preliminary hint from the operation's
    /// default class; it only selects the ceiling and never grants any right.
    pub fn reserve(&self, bytes: usize, critical: bool) -> Option<AssemblyPermit> {
        let ceiling = if critical {
            self.total
        } else {
            self.total - self.critical_reserve
        };
        let mut used = self.used.lock().unwrap_or_else(|e| e.into_inner());
        if used.checked_add(bytes).is_none_or(|next| next > ceiling) {
            return None;
        }
        *used += bytes;
        Some(AssemblyPermit {
            bytes,
            used: self.used.clone(),
        })
    }
}

/// Raise the soft open-file limit toward `wanted`, never above the hard limit.
///
/// launchd starts the broker with a soft limit of 256, which cannot hold the
/// writer socket's connection table plus database and API descriptors. Returns
/// the soft limit in force afterwards.
///
/// # Errors
/// The `getrlimit`/`setrlimit` error when the limit cannot be read or set.
#[cfg(any(test, feature = "test-fixtures"))]
#[allow(clippy::unnecessary_cast)] // rlim_t is u64 on 64-bit targets only
pub fn raise_open_file_limit(wanted: u64) -> std::io::Result<u64> {
    let mut limit = libc::rlimit {
        rlim_cur: 0,
        rlim_max: 0,
    };
    // SAFETY: `limit` is a valid writable rlimit for the call.
    if unsafe { libc::getrlimit(libc::RLIMIT_NOFILE, &mut limit) } != 0 {
        return Err(std::io::Error::last_os_error());
    }
    // macOS reports an unlimited hard limit but rejects values above OPEN_MAX.
    #[cfg(target_os = "macos")]
    let hard = (limit.rlim_max as u64).min(10_240);
    #[cfg(not(target_os = "macos"))]
    let hard = limit.rlim_max as u64;
    let target = wanted.min(hard);
    if (limit.rlim_cur as u64) >= target {
        return Ok(limit.rlim_cur as u64);
    }
    limit.rlim_cur = target as libc::rlim_t;
    // SAFETY: `limit` holds a soft value not above the hard limit.
    if unsafe { libc::setrlimit(libc::RLIMIT_NOFILE, &limit) } != 0 {
        return Err(std::io::Error::last_os_error());
    }
    Ok(target)
}

impl ConnectionLimits {
    /// These pools reduced, without touching any host limit, so that all of
    /// them plus `reserved` other descriptors fit in `soft` open files.
    ///
    /// Pre-authentication sockets and the operator pool shrink first; the
    /// reserved owner pool keeps the rest. `None` when even a minimal owner
    /// pool (8 connections) cannot fit, which the caller reports as a typed
    /// refusal to serve.
    pub fn fit_open_files(self, soft: u64, reserved: u64) -> Option<Self> {
        let available = usize::try_from(soft.saturating_sub(reserved)).unwrap_or(usize::MAX);
        if self.total() <= available {
            return Some(self);
        }
        let preauth = self.preauth.min((available / 16).max(1));
        let operator = self.operator.min((available / 4).max(1));
        let owner = self.owner.min(available.checked_sub(preauth + operator)?);
        (owner >= 8).then_some(Self {
            preauth,
            owner,
            operator,
        })
    }
}

/// The process's current soft open-file limit (read only; this module never
/// changes a host limit in production).
///
/// # Errors
/// The `getrlimit` error when the limit cannot be read.
#[allow(clippy::unnecessary_cast)] // rlim_t is u64 on 64-bit targets only
pub fn open_file_soft_limit() -> std::io::Result<u64> {
    let mut limit = libc::rlimit {
        rlim_cur: 0,
        rlim_max: 0,
    };
    // SAFETY: `limit` is a valid writable rlimit for the call.
    if unsafe { libc::getrlimit(libc::RLIMIT_NOFILE, &mut limit) } != 0 {
        return Err(std::io::Error::last_os_error());
    }
    Ok(limit.rlim_cur as u64)
}

#[cfg(test)]
mod tests {
    //! Pool exhaustion, reservation and release behavior.
    use super::*;

    /// Operators can never consume the owner pool and slots return on drop.
    #[test]
    fn pools_are_independent_and_released() {
        let slots = ConnectionSlots::new(ConnectionLimits {
            preauth: 2,
            owner: 2,
            operator: 1,
        });
        let operator = slots.verified(SlotClass::Operator).unwrap();
        assert!(slots.verified(SlotClass::Operator).is_none());
        let owners = [
            slots.verified(SlotClass::Owner).unwrap(),
            slots.verified(SlotClass::Owner).unwrap(),
        ];
        assert!(slots.verified(SlotClass::Owner).is_none());
        let first = slots.preauth().unwrap();
        let _second = slots.preauth().unwrap();
        assert!(slots.preauth().is_none());
        drop(first);
        assert!(slots.preauth().is_some());
        drop(owners);
        drop(operator);
        assert!(slots.verified(SlotClass::Owner).is_some());
        assert!(slots.verified(SlotClass::Operator).is_some());
    }

    /// The defaults hold 64 supervisors plus headroom and total 256 sockets.
    #[test]
    fn defaults_cover_the_sixty_four_process_tier() {
        let limits = ConnectionLimits::default();
        assert_eq!(limits.total(), 256);
        assert!(limits.owner >= 64 * 2);
    }

    /// Ordinary frames cannot starve the critical share of the assembly budget.
    #[test]
    fn critical_share_is_protected() {
        let budget = AssemblyBudget::new(100, 40);
        let first = budget.reserve(60, false).unwrap();
        assert!(budget.reserve(1, false).is_none(), "ordinary ceiling is 60");
        let critical = budget.reserve(40, true).unwrap();
        assert!(budget.reserve(1, true).is_none());
        drop(first);
        assert!(budget.reserve(1, true).is_some());
        drop(critical);
    }

    /// The soft and hard open-file limits as the kernel reports them now.
    #[allow(clippy::unnecessary_cast)] // rlim_t is u64 on 64-bit targets only
    fn limits() -> (u64, u64) {
        let mut limit = libc::rlimit {
            rlim_cur: 0,
            rlim_max: 0,
        };
        // SAFETY: `limit` is a valid writable rlimit for the call.
        let status = unsafe { libc::getrlimit(libc::RLIMIT_NOFILE, &mut limit) };
        assert_eq!(status, 0);
        (limit.rlim_cur as u64, limit.rlim_max as u64)
    }

    /// A low host limit shrinks the pools (operators and pre-authentication
    /// first, owners keep the rest) instead of failing or changing the limit;
    /// a limit that cannot hold even a minimal owner pool is refused.
    #[test]
    fn pools_fit_the_host_open_file_limit() {
        let defaults = ConnectionLimits::default();
        assert_eq!(defaults.fit_open_files(4096, 64), Some(defaults));
        let fitted = defaults.fit_open_files(256, 64).expect("256 files fit");
        assert!(fitted.total() <= 192, "{fitted:?}");
        assert!(fitted.owner >= 64, "64 supervisors must fit: {fitted:?}");
        assert!(fitted.preauth >= 1 && fitted.operator >= 1);
        assert_eq!(defaults.fit_open_files(70, 64), None);
    }

    /// A smaller request never lowers the soft limit, a larger one raises it,
    /// and the result never passes the hard limit.
    #[test]
    fn open_file_limit_only_rises_within_the_hard_limit() {
        let (soft, hard) = limits();
        assert_eq!(raise_open_file_limit(0).unwrap(), soft);
        assert_eq!(limits().0, soft, "a smaller request changed the limit");
        let raised = raise_open_file_limit(u64::MAX).unwrap();
        assert!(
            raised >= soft && raised <= hard,
            "{soft} -> {raised} (hard {hard})"
        );
        assert_eq!(limits().0, raised, "the reported value is the one in force");
    }
}
