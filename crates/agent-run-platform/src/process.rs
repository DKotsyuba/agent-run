//! PID reuse-aware process ownership. Unknown/denied evidence is never death.
use agent_run_domain::{Error, Result};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Identity {
    pub pid: i32,
    pub ppid: i32,
    pub group: i32,
    pub birth: f64,
    pub token: String,
    pub zombie: bool,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ProcessState {
    Alive,
    Dead,
    Reused,
    Unknown,
    Denied,
    NotStarted,
}
/// Reads one Linux process identity from procfs without trusting command-name encoding.
///
/// `pid` must be greater than one. The function returns the underlying procfs,
/// boot-clock, or numeric-field error when identity evidence is unavailable or
/// malformed; callers decide whether that evidence permits lifecycle action.
#[cfg(target_os = "linux")]
pub fn inspect(pid: i32) -> std::io::Result<Identity> {
    if pid <= 1 {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "unsafe process id",
        ));
    }
    // Read raw bytes: the parenthesized command name inherits the executable's
    // filename bytes and is not guaranteed UTF-8 (psutil reads bytes for the
    // same reason), so the line must never be validated as a String.
    let stat = std::fs::read(format!("/proc/{pid}/stat"))?;
    let parts = stat_fields(&stat)?;
    let num = |i: usize| {
        std::str::from_utf8(parts.get(i).ok_or_else(proc_field_error)?)
            .map_err(|_| proc_field_error())?
            .parse::<u64>()
            .map_err(|_| proc_field_error())
    };
    let ticks = num(19)?;
    let boot = std::fs::read_to_string("/proc/sys/kernel/random/boot_id")?;
    let clock = std::fs::read_to_string("/proc/stat")?;
    let epoch = clock
        .lines()
        .find_map(|s| s.strip_prefix("btime "))
        .and_then(|s| s.parse::<f64>().ok())
        .ok_or_else(|| std::io::Error::new(std::io::ErrorKind::InvalidData, "boot time"))?;
    // SAFETY: sysconf has no pointer arguments or side effects on process ownership.
    let hz = unsafe { libc::sysconf(libc::_SC_CLK_TCK) };
    if hz <= 0 {
        return Err(std::io::Error::last_os_error());
    }
    Ok(Identity {
        pid,
        ppid: num(1)? as i32,
        group: num(2)? as i32,
        birth: epoch + ticks as f64 / hz as f64,
        token: format!("linux:{}:{ticks}", boot.trim()),
        zombie: parts
            .first()
            .is_some_and(|state| *state == b"Z" || *state == b"X"),
    })
}
/// Split one `/proc/<pid>/stat` line into the fields after the command name.
///
/// The line is arbitrary bytes: the kernel copies the executable's filename
/// into the parenthesized command name verbatim, so it may contain invalid
/// UTF-8 or unbalanced parentheses. Fields are therefore taken after the final
/// `)`, which cannot appear in the numeric tail. Returns the whitespace-split
/// tail fields, or `InvalidData` when the line has no command name at all.
#[cfg(any(target_os = "linux", test))]
fn stat_fields(stat: &[u8]) -> std::io::Result<Vec<&[u8]>> {
    let end = stat
        .iter()
        .rposition(|&byte| byte == b')')
        .ok_or_else(|| std::io::Error::new(std::io::ErrorKind::InvalidData, "proc stat"))?;
    Ok(stat[end + 1..]
        .split(|&byte| byte.is_ascii_whitespace())
        .filter(|field| !field.is_empty())
        .collect())
}
/// Report one unparsable `/proc/<pid>/stat` tail field.
#[cfg(target_os = "linux")]
fn proc_field_error() -> std::io::Error {
    std::io::Error::new(std::io::ErrorKind::InvalidData, "proc field")
}
/// Read one PID's kernel start time, group and zombie state.
///
/// `birth` is the kernel `p_start` timeval computed exactly like psutil's
/// `tv_sec + tv_usec / 1000000.0`, so it equals Python's stored create_time
/// bit for bit (see `docs/process-identity.md`).
#[cfg(target_os = "macos")]
pub fn inspect(pid: i32) -> std::io::Result<Identity> {
    if pid <= 1 {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "unsafe process id",
        ));
    }
    // SAFETY: proc_bsdinfo is plain old data, so the all-zero value is valid.
    let mut info: libc::proc_bsdinfo = unsafe { std::mem::zeroed() };
    let size = std::mem::size_of::<libc::proc_bsdinfo>() as libc::c_int;
    // SAFETY: the buffer is a writable proc_bsdinfo of exactly `size` bytes, as
    // PROC_PIDTBSDINFO requires.
    let n = unsafe {
        libc::proc_pidinfo(
            pid,
            libc::PROC_PIDTBSDINFO,
            0,
            (&mut info as *mut libc::proc_bsdinfo).cast(),
            size,
        )
    };
    if n != size {
        let error = std::io::Error::last_os_error();
        // proc_pidinfo is same-user only; psutil reads every process through
        // sysctl. Without this fallback a PID reused by another user's process
        // would read as denied instead of reused.
        if !matches!(error.raw_os_error(), Some(libc::EPERM) | Some(libc::EACCES)) {
            return Err(error);
        }
        let (seconds, micros) = sysctl_start_time(pid).map_err(|_| error)?;
        // Only the start time is readable this way: relationship fields stay 0
        // (unknown), which keeps every ownership and signal check fail-closed.
        return Ok(Identity {
            pid,
            ppid: 0,
            group: 0,
            birth: seconds as f64 + micros as f64 / 1_000_000.,
            token: format!("darwin:{seconds}:{micros}"),
            zombie: false,
        });
    }
    Ok(Identity {
        pid,
        ppid: info.pbi_ppid as i32,
        group: info.pbi_pgid as i32,
        birth: info.pbi_start_tvsec as f64 + info.pbi_start_tvusec as f64 / 1_000_000.,
        token: format!("darwin:{}:{}", info.pbi_start_tvsec, info.pbi_start_tvusec),
        zombie: info.pbi_status == 5, // SZOMB
    })
}
/// Read one PID's start time from `KERN_PROC_PID`, exactly psutil's source.
///
/// `kinfo_proc` begins with `struct extern_proc`, whose first member is the
/// `p_starttime` timeval psutil converts; only that prefix is read here.
#[cfg(target_os = "macos")]
fn sysctl_start_time(pid: i32) -> std::io::Result<(u64, u64)> {
    #[repr(C)]
    struct StartTime {
        seconds: i64,
        micros: i32,
        padding: i32,
    }
    let mut name = [libc::CTL_KERN, libc::KERN_PROC, libc::KERN_PROC_PID, pid];
    let mut length: libc::size_t = 0;
    // SAFETY: `name` is a four-element MIB and `length` a valid out-parameter;
    // a null buffer only asks for the record size.
    let sized = unsafe {
        libc::sysctl(
            name.as_mut_ptr(),
            4,
            std::ptr::null_mut(),
            &mut length,
            std::ptr::null_mut(),
            0,
        )
    };
    if sized != 0 {
        return Err(std::io::Error::last_os_error());
    }
    if length < std::mem::size_of::<StartTime>() {
        return Err(std::io::Error::new(
            std::io::ErrorKind::NotFound,
            "no kinfo_proc record",
        ));
    }
    let mut record = vec![0u8; length];
    // SAFETY: the buffer is writable for exactly `length` bytes, as reported above.
    let read = unsafe {
        libc::sysctl(
            name.as_mut_ptr(),
            4,
            record.as_mut_ptr().cast(),
            &mut length,
            std::ptr::null_mut(),
            0,
        )
    };
    if read != 0 || length < std::mem::size_of::<StartTime>() {
        return Err(std::io::Error::last_os_error());
    }
    // SAFETY: `record` holds at least one whole kinfo_proc, so its first
    // bytes are an initialized, suitably aligned (Vec<u8> from malloc) timeval.
    let start = unsafe { &*record.as_ptr().cast::<StartTime>() };
    let _ = start.padding;
    Ok((start.seconds as u64, start.micros as u64))
}
/// Compare a PID with stored evidence; unreadable evidence is never death.
///
/// Rust-written `linux:`/`darwin:` tokens compare exactly. Python-written rows
/// carry only psutil's float create_time, compared with `==` as Python does.
pub fn observe(pid: Option<i32>, token: Option<&str>, birth: Option<f64>) -> ProcessState {
    let Some(pid) = pid else {
        return ProcessState::NotStarted;
    };
    verdict(inspect(pid), token, birth)
}
/// Classify one inspection result against stored token/birth evidence.
fn verdict(
    observed: std::io::Result<Identity>,
    token: Option<&str>,
    birth: Option<f64>,
) -> ProcessState {
    match observed {
        Ok(actual) => {
            let matches = if let Some(token) =
                token.filter(|s| s.starts_with("linux:") || s.starts_with("darwin:"))
            {
                actual.token == token
            } else if let Some(birth) = birth {
                actual.birth == birth
            } else {
                return ProcessState::Unknown;
            };
            if !matches {
                ProcessState::Reused
            } else if actual.zombie {
                ProcessState::Dead
            } else {
                ProcessState::Alive
            }
        }
        Err(e) => match e.raw_os_error() {
            Some(libc::ENOENT) | Some(libc::ESRCH) => ProcessState::Dead,
            Some(libc::EPERM) | Some(libc::EACCES) => ProcessState::Denied,
            _ => ProcessState::Unknown,
        },
    }
}

/// Return whether an exact PID observation proves the captured process exited.
fn identity_gone(state: ProcessState) -> bool {
    state == ProcessState::Dead
}

/// Return whether an exact PID observation permits an ownership signal.
fn signal_allowed(state: ProcessState) -> bool {
    state == ProcessState::Alive
}

/// Native observation of the original owned process group.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum GroupObservation {
    /// The original group has no remaining members.
    Gone,
    /// The original group still has a member.
    Alive,
    /// The leader or group could not be observed safely.
    Unknown,
}
/// Enumerate every process whose identity can be observed without spawning a child.
///
/// Linux obtains candidate PIDs directly from `/proc`; macOS asks the kernel for
/// `KERN_PROC_ALL` records. A process which exits during this snapshot is omitted,
/// but any other unreadable identity aborts the snapshot so cleanup cannot mistake
/// an unknown group member for a dead one.
pub fn processes() -> Result<Vec<Identity>> {
    #[cfg(target_os = "linux")]
    let ids = proc_ids()?;
    #[cfg(target_os = "macos")]
    let ids = sysctl_process_ids()?;

    let mut observed = Vec::with_capacity(ids.len());
    for pid in ids {
        match inspect(pid) {
            Ok(identity) => observed.push(identity),
            Err(error)
                if matches!(error.raw_os_error(), Some(libc::ENOENT) | Some(libc::ESRCH)) => {}
            Err(error) => return Err(error.into()),
        }
    }
    Ok(observed)
}

/// List numeric process directory names from Linux's native procfs.
#[cfg(target_os = "linux")]
fn proc_ids() -> std::io::Result<Vec<i32>> {
    let mut ids = Vec::new();
    for entry in std::fs::read_dir("/proc")? {
        let entry = entry?;
        if let Some(pid) = safe_process_id(&entry.file_name()) {
            ids.push(pid);
        }
    }
    Ok(ids)
}

/// Parse one process-directory name only when it is safe to inspect or signal.
///
/// PID 0 and PID 1 are excluded before [`processes`] calls [`inspect`], so the
/// platform-wide snapshot cannot fail merely because procfs exposes its init
/// process alongside owned runtime processes.
#[cfg(any(target_os = "linux", test))]
fn safe_process_id(name: &std::ffi::OsStr) -> Option<i32> {
    name.to_str()?.parse().ok().filter(|pid| *pid > 1)
}

/// Prefix of Darwin's `struct extern_proc` ending at its stable PID field.
///
/// `KERN_PROC_ALL` returns full `kinfo_proc` records. Their total size is
/// runtime-discovered below because Apple can extend the trailing fields; this
/// fixed ABI prefix is sufficient to read `extern_proc.p_pid` from each record.
#[cfg(target_os = "macos")]
#[repr(C)]
struct KernProcPrefix {
    /// `extern_proc.p_un`, either two links or one `timeval`.
    _links_or_start: [usize; 2],
    /// `extern_proc.p_vmspace`.
    _vmspace: *mut libc::c_void,
    /// `extern_proc.p_sigacts`.
    _sigacts: *mut libc::c_void,
    /// `extern_proc.p_flag`.
    _flags: libc::c_int,
    /// `extern_proc.p_stat`.
    _status: libc::c_char,
    /// C ABI padding before the following `pid_t`.
    _padding: [u8; 3],
    /// `extern_proc.p_pid`, the process identifier in this record.
    pid: libc::pid_t,
}

/// Read a Darwin sysctl payload, retrying once if process creation grew it.
#[cfg(target_os = "macos")]
fn sysctl_bytes(name: &mut [libc::c_int]) -> std::io::Result<Vec<u8>> {
    for _ in 0..2 {
        let mut length: libc::size_t = 0;
        // SAFETY: `name` is a valid mutable MIB, `length` is a valid out-pointer,
        // and a null output buffer requests the exact payload size without writing.
        if unsafe {
            libc::sysctl(
                name.as_mut_ptr(),
                name.len() as libc::c_uint,
                std::ptr::null_mut(),
                &mut length,
                std::ptr::null_mut(),
                0,
            )
        } != 0
        {
            return Err(std::io::Error::last_os_error());
        }
        let mut bytes = vec![0u8; length.saturating_add(4096)];
        let mut capacity = bytes.len();
        // SAFETY: `bytes` owns a writable buffer of exactly `capacity` bytes;
        // `capacity` is passed to the kernel and returned with the initialized
        // payload length, while `name` remains a valid MIB for this call.
        if unsafe {
            libc::sysctl(
                name.as_mut_ptr(),
                name.len() as libc::c_uint,
                bytes.as_mut_ptr().cast(),
                &mut capacity,
                std::ptr::null_mut(),
                0,
            )
        } == 0
        {
            bytes.truncate(capacity);
            return Ok(bytes);
        }
        if std::io::Error::last_os_error().raw_os_error() != Some(libc::ENOMEM) {
            return Err(std::io::Error::last_os_error());
        }
    }
    Err(std::io::Error::from_raw_os_error(libc::ENOMEM))
}

/// List PIDs from Darwin's `KERN_PROC_ALL` records without invoking `ps`.
#[cfg(target_os = "macos")]
fn sysctl_process_ids() -> std::io::Result<Vec<i32>> {
    match sysctl_process_ids_inner() {
        Ok(ids) => Ok(ids),
        Err(error) if matches!(error.raw_os_error(), Some(libc::EPERM) | Some(libc::EACCES)) => {
            proc_list_all_pids()
        }
        Err(error) => Err(error),
    }
}

/// Read Darwin `KERN_PROC_ALL` records and extract their stable PID prefix.
#[cfg(target_os = "macos")]
fn sysctl_process_ids_inner() -> std::io::Result<Vec<i32>> {
    let mut one = [
        libc::CTL_KERN,
        libc::KERN_PROC,
        libc::KERN_PROC_PID,
        std::process::id() as i32,
    ];
    let record_size = sysctl_bytes(&mut one)?.len();
    if record_size < std::mem::size_of::<KernProcPrefix>() {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "short kinfo_proc record",
        ));
    }
    let mut all = [libc::CTL_KERN, libc::KERN_PROC, libc::KERN_PROC_ALL];
    let records = sysctl_bytes(&mut all)?;
    if records.len() % record_size != 0 {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "partial kinfo_proc record",
        ));
    }
    const PID_OFFSET: usize = std::mem::offset_of!(KernProcPrefix, pid);
    Ok(records
        .chunks_exact(record_size)
        .filter_map(|record| {
            let bytes: [u8; std::mem::size_of::<libc::pid_t>()] = record
                .get(PID_OFFSET..PID_OFFSET + std::mem::size_of::<libc::pid_t>())?
                .try_into()
                .ok()?;
            let pid = libc::pid_t::from_ne_bytes(bytes);
            (pid > 1).then_some(pid)
        })
        .collect())
}

/// List Darwin PIDs through libproc when a sandbox denies the sysctl snapshot.
#[cfg(target_os = "macos")]
fn proc_list_all_pids() -> std::io::Result<Vec<i32>> {
    // SAFETY: a null buffer and zero size only ask libproc for the required byte count.
    let bytes = unsafe { libc::proc_listallpids(std::ptr::null_mut(), 0) };
    if bytes < 0 {
        return Err(std::io::Error::last_os_error());
    }
    let mut ids = vec![0i32; bytes as usize / std::mem::size_of::<libc::pid_t>()];
    // SAFETY: `ids` owns exactly its byte length as writable `pid_t` storage, and
    // libproc writes at most the supplied byte count before returning that count.
    let read = unsafe {
        libc::proc_listallpids(
            ids.as_mut_ptr().cast(),
            (ids.len() * std::mem::size_of::<libc::pid_t>()) as libc::c_int,
        )
    };
    if read < 0 {
        return Err(std::io::Error::last_os_error());
    }
    ids.truncate(read as usize / std::mem::size_of::<libc::pid_t>());
    ids.retain(|pid| *pid > 1);
    Ok(ids)
}
#[derive(Debug, Default, Clone, Serialize, Deserialize)]
/// Process-group and descendant cleanup evidence for one owned runtime.
pub struct Cleanup {
    /// Signals successfully delivered to the verified process group.
    pub signals: Vec<String>,
    /// Scope of the descendant evidence.
    ///
    /// `verified_descendants` when a descendant verdict exists, otherwise
    /// `process_group`: the group alone was observed and the descendant closure
    /// was never verified.
    pub scope: String,
    /// Whether native group observation proved the original group absent.
    pub group_gone: bool,
    /// Whether every descendant identity captured while the leader was alive is
    /// gone, or `None` when that could not be established.
    ///
    /// `None` means unknown — no snapshot was ever verified, or some captured
    /// identity was unreadable at cleanup. It never means "clean".
    pub descendants_gone: Option<bool>,
    /// Whether both group and descendant observations confirm cleanup.
    pub confirmed: bool,
    /// Original process-group identifier, when safe to report.
    pub process_group_id: Option<i32>,
}
/// PID-reuse-aware ownership evidence for one child process group.
pub struct OwnedProcess {
    /// Captured identity of the group leader, when readable at spawn time.
    pub leader: Option<Identity>,
    /// Expected process-group identifier, equal to the spawned leader PID.
    pub pid: i32,
    /// Captured leader and observed descendants indexed by PID.
    known: BTreeMap<i32, Identity>,
    /// Whether a complete native snapshot was taken while the leader was alive.
    ///
    /// False means no descendant set was ever verified, so cleanup reports the
    /// descendant scope as unknown rather than clean.
    descendants_observed: bool,
}
impl OwnedProcess {
    /// Capture the spawned leader identity without assuming failure is death.
    ///
    /// A live leader read here is itself the first verified descendant snapshot:
    /// a process observed at the moment it is spawned cannot have descendants
    /// yet, so its empty set is proven rather than assumed, and even a child
    /// exiting before the caller's first periodic refresh leaves verified
    /// evidence behind. That proof costs only the leader's own inspection; a
    /// full process-table scan here would delay the caller's stream readers and
    /// is not needed for an empty set. A leader that is already gone, a zombie,
    /// or unreadable records no snapshot, and cleanup then reports the
    /// descendant scope as unknown rather than clean.
    pub fn capture(pid: i32) -> Self {
        let leader = inspect(pid).ok();
        let known = leader.clone().into_iter().map(|p| (p.pid, p)).collect();
        let descendants_observed = leader.as_ref().is_some_and(|p| !p.zombie);
        Self {
            leader,
            pid,
            known,
            descendants_observed,
        }
    }
    /// Record descendants visible from a complete native process snapshot.
    ///
    /// Captured identities accumulate in `known` and survive later refreshes, so
    /// a descendant seen while the leader lived is still probed individually by
    /// `cleanup`. The snapshot counts as *verified* descendant evidence only when
    /// the captured leader is itself observably alive: after it exits, a
    /// descendant which left the original process group is reachable through
    /// neither parentage nor the group, so an empty snapshot then proves nothing.
    /// This mirrors Python's `SystemProcessOps.descendants`
    /// (src/agent_run/lifecycle.py:134-155), which returns `None` unless the
    /// leader identity is ALIVE. A failed `processes()` call records nothing and
    /// leaves the existing evidence untouched.
    pub fn refresh(&mut self) {
        let Ok(all) = processes() else {
            return;
        };
        let leader_state = self
            .leader
            .as_ref()
            .map(|p| observe(Some(p.pid), Some(&p.token), Some(p.birth)));
        if leader_state == Some(ProcessState::Alive) {
            self.descendants_observed = true;
        }
        let leader_owned = matches!(leader_state, Some(ProcessState::Alive | ProcessState::Dead));
        // Expand only from currently verified owned parents, never an unverified reused PID.
        loop {
            let mut changed = false;
            for p in &all {
                let child = self.known.get(&p.ppid).is_some_and(|parent| {
                    observe(Some(parent.pid), Some(&parent.token), Some(parent.birth))
                        == ProcessState::Alive
                });
                let in_group = leader_owned && p.group == self.pid;
                if (child || in_group) && !self.known.contains_key(&p.pid) {
                    self.known.insert(p.pid, p.clone());
                    changed = true;
                }
            }
            if !changed {
                break;
            }
        }
    }
    /// Signal only a currently verified owned process group.
    pub fn signal(&mut self, sig: i32) -> Result<bool> {
        self.refresh();
        let mut signalled = false;
        if let Some(p) = &self.leader {
            if p.group == self.pid
                && signal_allowed(observe(Some(p.pid), Some(&p.token), Some(p.birth)))
            {
                // SAFETY: signal only the verified child-created process group; never group 0/1.
                let code = unsafe { libc::kill(-self.pid, sig) };
                if code == 0 {
                    signalled = true;
                } else if std::io::Error::last_os_error().raw_os_error() != Some(libc::ESRCH) {
                    return Err(std::io::Error::last_os_error().into());
                }
            }
        }
        Ok(signalled)
    }
    /// Return whether the leader and its original group are both proven absent.
    pub fn gone(&self) -> bool {
        self.group_observation() == GroupObservation::Gone
    }
    /// Observe the captured leader and its original group without sending a signal.
    fn group_observation(&self) -> GroupObservation {
        match &self.leader {
            Some(p) => {
                let state = observe(Some(p.pid), Some(&p.token), Some(p.birth));
                if signal_allowed(state) {
                    return GroupObservation::Alive;
                }
                if !identity_gone(state) && state != ProcessState::Reused {
                    return GroupObservation::Unknown;
                }
            }
            None => match inspect(self.pid) {
                Err(error)
                    if matches!(error.raw_os_error(), Some(libc::ENOENT) | Some(libc::ESRCH)) => {}
                Err(_) => return GroupObservation::Unknown,
                Ok(identity) if identity.zombie => {}
                Ok(_) => return GroupObservation::Unknown,
            },
        }
        if self.pid <= 1 {
            return GroupObservation::Unknown;
        }
        if let Ok(processes) = processes() {
            return if processes
                .iter()
                .any(|process| process.group == self.pid && !process.zombie)
            {
                GroupObservation::Alive
            } else {
                GroupObservation::Gone
            };
        }
        // SAFETY: signal 0 observes only the explicitly captured process group;
        // `pid > 1` prevents the wildcard and this supervisor's own group.
        match unsafe { libc::kill(-self.pid, 0) } {
            // Without the native snapshot, signal 0 cannot distinguish a live
            // member from an unreaped zombie, so it is not completion evidence.
            0 => GroupObservation::Unknown,
            _ => match std::io::Error::last_os_error().raw_os_error() {
                Some(libc::ESRCH) => GroupObservation::Gone,
                // POSIX signal 0 with EPERM proves the group exists but does
                // not grant permission to signal it, matching Python's group_alive.
                Some(libc::EPERM) | Some(libc::EACCES) => GroupObservation::Alive,
                _ => GroupObservation::Unknown,
            },
        }
    }
    /// Terminate the verified group and return separate group/descendant evidence.
    pub async fn cleanup(&mut self, grace: std::time::Duration) -> Result<Cleanup> {
        let mut signals = Vec::new();
        self.refresh();
        if self.group_observation() == GroupObservation::Unknown {
            return Err(Error::Runtime(
                "process cleanup observation unavailable".into(),
            ));
        }
        if !self.gone() {
            if self.signal(libc::SIGTERM).unwrap_or(false) {
                signals.push("SIGTERM".into());
            }
            let until = tokio::time::Instant::now() + grace;
            while tokio::time::Instant::now() < until && !self.gone() {
                tokio::time::sleep(std::time::Duration::from_millis(25)).await;
            }
        }
        if !self.gone() {
            if self.signal(libc::SIGKILL).unwrap_or(false) {
                signals.push("SIGKILL".into());
            }
            let until = tokio::time::Instant::now() + std::time::Duration::from_secs(2);
            while tokio::time::Instant::now() < until && !self.gone() {
                tokio::time::sleep(std::time::Duration::from_millis(25)).await;
            }
        }
        let group = self.group_observation();
        if group == GroupObservation::Unknown {
            return Err(Error::Runtime(
                "process cleanup observation unavailable".into(),
            ));
        }
        let group_gone = group == GroupObservation::Gone;
        let descendants_gone = self.descendants_observed.then(|| {
            let mut unknown = false;
            for process in self
                .known
                .values()
                .filter(|process| process.pid != self.pid)
            {
                match observe(Some(process.pid), Some(&process.token), Some(process.birth)) {
                    ProcessState::Dead | ProcessState::Reused => {}
                    ProcessState::Alive => return false,
                    ProcessState::Unknown | ProcessState::Denied | ProcessState::NotStarted => {
                        unknown = true;
                    }
                }
            }
            !unknown
        });
        Ok(Cleanup {
            signals,
            // Mirrors Python's `Termination.scope` (src/agent_run/lifecycle.py:196-204):
            // the wider scope is claimed only when a descendant verdict exists.
            scope: if descendants_gone.is_some() {
                "verified_descendants"
            } else {
                "process_group"
            }
            .into(),
            group_gone,
            descendants_gone,
            confirmed: group_gone && descendants_gone == Some(true),
            process_group_id: Some(self.pid),
        })
    }
}
#[cfg(test)]
mod tests {
    use super::*;

    /// Procfs init and wildcard-like names never enter a process snapshot.
    #[test]
    fn process_directory_names_require_a_pid_above_one() {
        assert_eq!(safe_process_id(std::ffi::OsStr::new("0")), None);
        assert_eq!(safe_process_id(std::ffi::OsStr::new("1")), None);
        assert_eq!(safe_process_id(std::ffi::OsStr::new("self")), None);
        assert_eq!(safe_process_id(std::ffi::OsStr::new("2")), Some(2));
    }

    /// A stat line whose command name carries non-UTF-8 filename bytes still
    /// yields the numeric identity fields; reading it as a String would reject
    /// the whole observation as unreadable and lose the leader identity.
    #[test]
    fn stat_fields_tolerate_non_utf8_command_names() {
        let line =
            b"1234 (sh\xee) R 1200 1234 1234 0 -1 4194304 200 0 0 0 1 2 3 4 20 0 6 0 98765 0";
        let fields = stat_fields(line).expect("non-UTF-8 command name");
        assert_eq!(fields.first(), Some(&b"R".as_slice()));
        assert_eq!(fields.get(1), Some(&b"1200".as_slice()));
        assert_eq!(fields.get(2), Some(&b"1234".as_slice()));
        assert_eq!(fields.get(19), Some(&b"98765".as_slice()));
        assert!(stat_fields(b"no command name").is_err());
    }

    #[test]
    fn unreadable_evidence_is_never_death() {
        let failed = |code| Err(std::io::Error::from_raw_os_error(code));
        assert_eq!(
            verdict(failed(libc::EPERM), None, Some(1.0)),
            ProcessState::Denied
        );
        assert_eq!(
            verdict(failed(libc::EACCES), None, Some(1.0)),
            ProcessState::Denied
        );
        assert_eq!(
            verdict(failed(libc::EIO), None, Some(1.0)),
            ProcessState::Unknown
        );
        assert_eq!(
            verdict(failed(libc::ESRCH), None, Some(1.0)),
            ProcessState::Dead
        );
        assert_eq!(
            verdict(failed(libc::ENOENT), None, Some(1.0)),
            ProcessState::Dead
        );
    }

    #[test]
    fn unknown_or_denied_identity_is_never_gone_or_signallable() {
        for state in [ProcessState::Unknown, ProcessState::Denied] {
            assert!(!identity_gone(state));
            assert!(!signal_allowed(state));
        }
    }

    /// A readable process table is not descendant evidence once the leader is
    /// gone: a descendant which left the original group is then reachable
    /// through neither parentage nor the group, so the empty result must report
    /// unknown rather than a clean, confirmed teardown.
    #[tokio::test]
    async fn an_unverified_descendant_snapshot_is_unknown_not_clean() {
        let mut child = std::process::Command::new("/bin/sleep")
            .arg("30")
            .spawn()
            .expect("spawn owned test child");
        let pid = child.id() as i32;
        let mut owned = OwnedProcess::capture(pid);
        assert!(
            owned.descendants_observed,
            "a snapshot taken while the leader lives is verified evidence"
        );
        child.kill().expect("stop owned test child");
        child.wait().expect("reap owned test child");
        // Model an owner whose leader died before any snapshot succeeded: the
        // process table is still readable, and that alone must not verify.
        owned.descendants_observed = false;
        owned.refresh();
        assert!(
            !owned.descendants_observed,
            "a post-exit snapshot must never count as verification"
        );
        let cleanup = owned
            .cleanup(std::time::Duration::from_millis(50))
            .await
            .expect("cleanup observation available");
        assert_eq!(cleanup.descendants_gone, None);
        assert_eq!(cleanup.scope, "process_group");
        assert!(!cleanup.confirmed);
        assert!(cleanup.signals.is_empty());
    }
}
