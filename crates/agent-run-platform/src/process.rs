//! PID reuse-aware process ownership. Unknown/denied evidence is never death.
use agent_run_domain::{Error, Result};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
/// Kernel process identity; optional program metadata never grants signalling rights.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Identity {
    /// Kernel process identifier, greater than one for owned processes.
    pub pid: i32,
    /// Parent identifier at observation time.
    pub ppid: i32,
    /// Process group at observation time.
    pub group: i32,
    /// Kernel start time in Unix seconds, used with the token to fence PID reuse.
    pub birth: f64,
    /// Immutable platform start token for this PID generation.
    pub token: String,
    /// Whether the observed process was already a zombie.
    pub zombie: bool,
    /// Observed executable basename, at most 128 bytes; absent for old or unreadable evidence.
    /// Arguments and full paths are never collected. This is diagnostic metadata only.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub program: Option<String>,
}

/// Exact Linux process incarnation. These facts are correlation evidence, not
/// a credential; an unavailable or changing procfs observation is not death.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct KernelIncarnation {
    pub boot_id: String,
    pub pid_namespace_inode: u64,
    pub uid: u32,
    pub pid: u32,
    pub start_ticks: u64,
}

/// Inspect the owned leader twice, requiring the same live generation and
/// local PID namespace throughout. Never inspect the process environment.
#[cfg(target_os = "linux")]
pub fn kernel_incarnation(leader: &Identity) -> std::io::Result<KernelIncarnation> {
    use std::os::unix::fs::MetadataExt;
    let invalid = || {
        std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "kernel identity unavailable",
        )
    };
    let read = || -> std::io::Result<KernelIncarnation> {
        let observed = inspect(leader.pid)?;
        if observed.zombie || observed.token != leader.token {
            return Err(invalid());
        }
        let (boot, ticks) = observed
            .token
            .strip_prefix("linux:")
            .and_then(|s| s.rsplit_once(':'))
            .ok_or_else(invalid)?;
        let parsed = uuid::Uuid::parse_str(boot).map_err(|_| invalid())?;
        if parsed.to_string() != boot {
            return Err(invalid());
        }
        let start_ticks = ticks
            .parse::<u64>()
            .ok()
            .filter(|n| *n > 0)
            .ok_or_else(invalid)?;
        let local_ns = std::fs::metadata("/proc/self/ns/pid")?.ino();
        let target_ns = std::fs::metadata(format!("/proc/{}/ns/pid", leader.pid))?.ino();
        if target_ns == 0 || target_ns != local_ns {
            return Err(invalid());
        }
        let status = std::fs::read_to_string(format!("/proc/{}/status", leader.pid))?;
        let mut lines = status.lines().filter_map(|line| line.strip_prefix("Uid:"));
        let uids = lines
            .next()
            .ok_or_else(invalid)?
            .split_ascii_whitespace()
            .map(str::parse::<u32>)
            .collect::<std::result::Result<Vec<_>, _>>()
            .map_err(|_| invalid())?;
        if lines.next().is_some() || uids.len() != 4 {
            return Err(invalid());
        }
        // SAFETY: geteuid has no preconditions.
        if uids[1] != unsafe { libc::geteuid() } {
            return Err(invalid());
        }
        Ok(KernelIncarnation {
            boot_id: boot.into(),
            pid_namespace_inode: target_ns,
            uid: uids[1],
            pid: leader.pid.try_into().map_err(|_| invalid())?,
            start_ticks,
        })
    };
    let before = read()?;
    if read()? != before {
        return Err(invalid());
    }
    Ok(before)
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
/// Resolve the executable of one live kernel-identified process without consulting argv or peer claims.
/// Missing, exited, unreadable and unsupported processes return an I/O error; callers must fail closed.
pub fn executable(pid: i32) -> std::io::Result<std::path::PathBuf> {
    if pid <= 1 {
        return Err(std::io::ErrorKind::InvalidInput.into());
    }
    #[cfg(target_os = "linux")]
    {
        std::fs::read_link(format!("/proc/{pid}/exe"))
    }
    #[cfg(target_os = "macos")]
    {
        use std::os::unix::ffi::OsStrExt;
        let mut buffer = [0u8; 4096];
        // SAFETY: buffer is writable for the exact supplied length; proc_pidpath returns its bounded C string.
        let n = unsafe { libc::proc_pidpath(pid, buffer.as_mut_ptr().cast(), buffer.len() as u32) };
        if n <= 0 {
            return Err(std::io::Error::last_os_error());
        }
        let length = buffer
            .iter()
            .position(|b| *b == 0)
            .ok_or(std::io::ErrorKind::InvalidData)?;
        Ok(std::path::PathBuf::from(std::ffi::OsStr::from_bytes(
            &buffer[..length],
        )))
    }
    #[cfg(not(any(target_os = "linux", target_os = "macos")))]
    {
        Err(std::io::ErrorKind::Unsupported.into())
    }
}

/// Require a filesystem whose Linux executable write exclusion protects the loaded inode.
///
/// Resolve the *open file's* mount ID via fdinfo, then its filesystem type in
/// this process's mountinfo. This avoids path-prefix/bind-mount ambiguity and
/// needs no unsafe statfs call or new dependency. Hidden/detached mounts and
/// unknown types fail closed. Overlay is deliberately unsupported: its visible
/// type alone does not establish the upper filesystem's write exclusion.
/// The caller must open /proc/PID/exe and also verify inode, digest, mode and
/// incarnation. No wall clock or file timestamp contributes authority.
#[cfg(target_os = "linux")]
pub fn executable_write_excluded(file: &std::fs::File) -> std::io::Result<bool> {
    use std::os::fd::AsRawFd;
    let info = std::fs::read_to_string(format!("/proc/self/fdinfo/{}", file.as_raw_fd()))?;
    let mounts = std::fs::read_to_string("/proc/self/mountinfo")?;
    Ok(executable_mount_allowed(&info, &mounts))
}

#[cfg(target_os = "linux")]
fn executable_mount_allowed(info: &str, mounts: &str) -> bool {
    let mut ids = info.lines().filter_map(|line| line.strip_prefix("mnt_id:"));
    let Some(id) = ids.next().and_then(|s| s.trim().parse::<u64>().ok()) else {
        return false;
    };
    if ids.next().is_some() {
        return false;
    }
    let mut matching = mounts.lines().filter(|line| {
        line.split_ascii_whitespace()
            .next()
            .and_then(|s| s.parse::<u64>().ok())
            == Some(id)
    });
    let Some(line) = matching.next() else {
        return false;
    };
    if matching.next().is_some() {
        return false;
    }
    let kind = line
        .split_once(" - ")
        .and_then(|(_, tail)| tail.split_ascii_whitespace().next());
    // Names are the kernel mountinfo equivalents of statfs f_type magic values.
    let magic = match kind {
        Some("ext2" | "ext3" | "ext4") => 0xef53,
        Some("xfs") => 0x58465342,
        Some("btrfs") => 0x9123683e,
        Some("tmpfs") => 0x01021994,
        _ => return false,
    };
    executable_fs_magic_allowed(magic)
}

#[cfg(target_os = "linux")]
fn executable_fs_magic_allowed(magic: u64) -> bool {
    matches!(magic, 0xef53 | 0x58465342 | 0x9123683e | 0x01021994)
}

#[cfg(all(test, target_os = "linux"))]
mod executable_mount_tests {
    use super::*;

    #[test]
    fn unsupported_filesystems_fail_closed() {
        for magic in [0xef53, 0x58465342, 0x9123683e, 0x01021994] {
            assert!(executable_fs_magic_allowed(magic));
        }
        for magic in [0, 0x794c7630, 0x6969, u64::MAX] {
            assert!(!executable_fs_magic_allowed(magic));
        }
        for kind in ["ext2", "ext3", "ext4", "xfs", "btrfs", "tmpfs"] {
            let mounts = format!("42 1 0:1 / / rw - {kind} source rw\n");
            assert!(executable_mount_allowed("mnt_id:\t42\n", &mounts));
        }
        for (info, mounts) in [
            ("mnt_id: 42", "42 1 0:1 / / rw - overlay source rw"),
            ("mnt_id: 42", "42 1 0:1 / / rw - nfs source rw"),
            ("mnt_id: 42", "43 1 0:1 / / rw - ext4 source rw"),
            ("mnt_id: 42\nmnt_id: 42", "42 1 0:1 / / rw - ext4 source rw"),
            (
                "mnt_id: 42",
                "42 1 0:1 / / rw - ext4 source rw\n42 1 0:1 / / rw - tmpfs source rw",
            ),
            ("mnt_id: invalid", ""),
            ("", ""),
        ] {
            assert!(!executable_mount_allowed(info, mounts));
        }
    }
}

/// Reads one Linux process identity from procfs without trusting command-name encoding.
///
/// `pid` must be greater than one. The function returns the underlying procfs,
/// boot-clock, or numeric-field error when identity evidence is unavailable or
/// malformed; callers decide whether that evidence permits lifecycle action.
/// On success, `ppid` and `group` come from the stat fields, `birth` is Unix
/// seconds derived from boot time plus start ticks, `token` is the stable
/// `linux:<boot-id>:<start-ticks>` PID-reuse fence, and `zombie` covers both the
/// kernel `Z` and dead-process `X` states.
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
    let stat = read_proc_stat(pid)?;
    let parts = stat_fields(&stat)?;
    let num = |i: usize| {
        std::str::from_utf8(parts.get(i).ok_or_else(proc_field_error)?)
            .map_err(|_| proc_field_error())?
            .parse::<u64>()
            .map_err(|_| proc_field_error())
    };
    let ticks = num(19)?;
    let clock = boot_clock()?;
    Ok(Identity {
        pid,
        ppid: num(1)? as i32,
        group: num(2)? as i32,
        birth: clock.btime + ticks as f64 / clock.hz,
        token: format!("linux:{}:{ticks}", clock.boot_id),
        program: None,
        zombie: parts
            .first()
            .is_some_and(|state| *state == b"Z" || *state == b"X"),
    })
}
/// Boot-constant Linux facts behind every process token and birth time.
#[cfg(target_os = "linux")]
#[derive(Debug, Clone, PartialEq)]
struct BootClock {
    /// Trimmed `/proc/sys/kernel/random/boot_id`, the token's boot fence.
    boot_id: String,
    /// `btime` from `/proc/stat`: boot time in Unix seconds.
    btime: f64,
    /// `_SC_CLK_TCK`, the unit of the stat start-time field.
    hz: f64,
}

/// Read the boot identity, boot time and clock tick rate from procfs.
///
/// Errors are returned unchanged so callers keep treating unavailable evidence
/// as unknown rather than death.
#[cfg(target_os = "linux")]
fn read_boot_clock() -> std::io::Result<BootClock> {
    let boot = std::fs::read_to_string("/proc/sys/kernel/random/boot_id")?;
    let clock = std::fs::read_to_string("/proc/stat")?;
    let btime = clock
        .lines()
        .find_map(|s| s.strip_prefix("btime "))
        .and_then(|s| s.parse::<f64>().ok())
        .ok_or_else(|| std::io::Error::new(std::io::ErrorKind::InvalidData, "boot time"))?;
    // SAFETY: sysconf has no pointer arguments or side effects on process ownership.
    let hz = unsafe { libc::sysconf(libc::_SC_CLK_TCK) };
    if hz <= 0 {
        return Err(std::io::Error::last_os_error());
    }
    Ok(BootClock {
        boot_id: boot.trim().to_owned(),
        btime,
        hz: hz as f64,
    })
}

/// Return the boot facts read once per process; they cannot change before reboot.
///
/// Before this cache every [`inspect`] re-read the whole kernel-generated
/// `/proc/stat` (several KiB) and `boot_id`, which dominated the cost of the
/// supervisor's process snapshots. A failed read is not cached and is retried.
/// Within one process this also keeps `birth` stable for a PID generation even
/// if the wall clock is stepped, so exact `birth` comparisons stay consistent.
#[cfg(target_os = "linux")]
fn boot_clock() -> std::io::Result<&'static BootClock> {
    static CLOCK: std::sync::OnceLock<BootClock> = std::sync::OnceLock::new();
    if let Some(clock) = CLOCK.get() {
        return Ok(clock);
    }
    let clock = read_boot_clock()?;
    Ok(CLOCK.get_or_init(|| clock))
}

/// Read one `/proc/<pid>/stat` line with one open and two reads, without the
/// size probe and repeated small reads `std::fs::read` performs on procfs.
#[cfg(target_os = "linux")]
fn read_proc_stat(pid: i32) -> std::io::Result<Vec<u8>> {
    use std::io::Read;
    let mut stat = Vec::with_capacity(1024);
    std::fs::File::open(format!("/proc/{pid}/stat"))?.read_to_end(&mut stat)?;
    Ok(stat)
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
            program: None,
            zombie: false,
        });
    }
    Ok(Identity {
        pid,
        ppid: info.pbi_ppid as i32,
        group: info.pbi_pgid as i32,
        birth: info.pbi_start_tvsec as f64 + info.pbi_start_tvusec as f64 / 1_000_000.,
        token: format!("darwin:{}:{}", info.pbi_start_tvsec, info.pbi_start_tvusec),
        program: None,
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
    /// Signal names successfully delivered to the verified group or captured descendants.
    pub signals: Vec<String>,
    /// Scope of the descendant evidence.
    ///
    /// `verified_descendants` when a descendant verdict exists, otherwise
    /// `process_group`: the group alone was observed and the descendant closure
    /// was never verified.
    pub scope: String,
    /// Whether native group observation proved the original group absent.
    pub group_gone: bool,
    /// Whether every captured descendant identity is gone. `Some(false)`
    /// includes live or unreadable identities; `None` means no verified snapshot.
    ///
    /// Neither `Some(false)` nor `None` confirms cleanup.
    pub descendants_gone: Option<bool>,
    /// Whether both group and descendant observations confirm cleanup.
    pub confirmed: bool,
    /// Original process-group identifier, when safe to report.
    pub process_group_id: Option<i32>,
}
/// Serializable captured identities used to recover ownership after a supervisor or broker restart.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OwnershipSnapshot {
    /// Original leader identity, retained even after that PID disappears or is reused.
    pub leader: Identity,
    /// All observed members, including the leader; distinct start tokens survive PID reuse.
    pub members: Vec<Identity>,
    /// Whether observation began from a verified live root; does not assert a complete historical tree.
    pub descendants_observed: bool,
}

/// Accepts a bounded diagnostic basename, never a path, empty value or control text.
fn valid_program(name: &str) -> bool {
    !name.is_empty()
        && name.len() <= 128
        && !name.contains(['/', '\\'])
        && !name.chars().any(char::is_control)
}

/// Adds a basename only if the same PID generation still exists after executable lookup.
/// Lookup/encoding/identity failures leave the name unknown and never alter ownership evidence.
fn with_program(mut identity: Identity) -> Identity {
    identity.program = executable(identity.pid)
        .ok()
        .and_then(|path| {
            path.file_name()
                .and_then(|name| name.to_str())
                .map(str::to_owned)
        })
        .filter(|name| valid_program(name))
        .filter(|_| {
            observe(
                Some(identity.pid),
                Some(&identity.token),
                Some(identity.birth),
            ) == ProcessState::Alive
        });
    identity
}

mod adoption;
pub use adoption::{
    DURABLE_REFRESH, POLLING_REFRESH, REFRESH_ENV, SpawnedChild, become_subreaper,
    durable_refresh_interval, pin_durable_refresh_interval, spawn_registered, subreaper_active,
};

/// Per-refresh memo of [`observe`] verdicts for captured parents.
///
/// Keys are the captured `(pid, token)`, so a recycled PID with a different
/// token is a distinct key and is probed (and rejected) on its own.
#[derive(Default)]
struct ParentLiveness(BTreeMap<(i32, String), bool>);
impl ParentLiveness {
    /// Record one verdict already obtained for this exact identity.
    fn record(&mut self, pid: i32, token: &str, alive: bool) {
        self.0.insert((pid, token.to_owned()), alive);
    }
    /// Return whether `parent` observes alive, probing it at most once per memo.
    fn alive(&mut self, parent: &Identity) -> bool {
        if let Some(alive) = self.0.get(&(parent.pid, parent.token.clone())) {
            return *alive;
        }
        let alive = observe(Some(parent.pid), Some(&parent.token), Some(parent.birth))
            == ProcessState::Alive;
        self.record(parent.pid, &parent.token, alive);
        alive
    }
}

/// PID-reuse-aware ownership evidence for one child process group.
pub struct OwnedProcess {
    /// Captured identity of the group leader, when readable at spawn time.
    pub leader: Option<Identity>,
    /// Expected process-group identifier, equal to the spawned leader PID.
    pub pid: i32,
    /// Captured leader and descendants indexed by PID and immutable start token.
    known: BTreeMap<(i32, String), Identity>,
    /// Keys in capture order, allowing bounded deltas without copying historical identities.
    captured: Vec<(i32, String)>,
    /// Whether tracking began from a live verified leader or a later live snapshot.
    ///
    /// False means no descendant set was ever verified, so cleanup reports the
    /// descendant scope as unknown rather than clean.
    descendants_observed: bool,
    /// Admission generation in which this owner's leader was spawned as the
    /// only live tree of a subreaper process; orphans are adopted only while
    /// it is still the current generation.
    exclusive: Option<u64>,
}
impl OwnedProcess {
    /// Capture the spawned leader identity without assuming failure is death.
    ///
    /// A live identity establishes the root from which periodic snapshots expand.
    /// The child may already have forked: this observation does not prove an empty
    /// historical tree. Cleanup confirms only the original group and identities
    /// actually captured, never unobserved descendants which escaped beforehand.
    /// An already absent, zombie or unreadable leader cannot establish this root,
    /// so descendant cleanup stays unknown unless a later live snapshot succeeds.
    pub fn capture(pid: i32) -> Self {
        let leader = inspect(pid).ok().map(with_program);
        let known: BTreeMap<_, _> = leader
            .clone()
            .into_iter()
            .map(|p| ((p.pid, p.token.clone()), p))
            .collect();
        let descendants_observed = leader.as_ref().is_some_and(|p| !p.zombie);
        Self {
            exclusive: leader
                .as_ref()
                .filter(|_| subreaper_active())
                .and_then(adoption::exclusive_generation),
            leader,
            pid,
            captured: known.keys().cloned().collect(),
            known,
            descendants_observed,
        }
    }
    /// Re-adopt a group from a durably recorded leader identity (for example
    /// after the owning supervisor died). Nothing is assumed: the leader must
    /// still observe as the same process (token and birth) before any signal,
    /// and descendants count as verified only through a later snapshot taken
    /// while that leader is alive.
    pub fn adopt(leader: Identity) -> Self {
        let pid = leader.pid;
        Self {
            captured: vec![(pid, leader.token.clone())],
            known: BTreeMap::from([((pid, leader.token.clone()), leader.clone())]),
            leader: Some(leader),
            pid,
            descendants_observed: false,
            exclusive: None,
        }
    }
    /// Exports captured identities without probing or changing the tree; absent root evidence returns None.
    pub fn snapshot(&self) -> Option<OwnershipSnapshot> {
        Some(OwnershipSnapshot {
            leader: self.leader.clone()?,
            members: self.known.values().cloned().collect(),
            descendants_observed: self.descendants_observed,
        })
    }
    /// Copies at most 512 newly captured identities after acknowledged offset after.
    /// The root is included in every batch so each delta is independently validated.
    /// Returns the batch and its exclusive capture offset; missing root or invalid offset returns None.
    /// Offsets remain stable for this owner and advance only after durable acknowledgement.
    pub fn snapshot_since(&self, after: usize) -> Option<(OwnershipSnapshot, usize)> {
        let end = after.saturating_add(512).min(self.captured.len());
        let leader = self.leader.clone()?;
        let mut members: Vec<_> = self
            .captured
            .get(after..end)?
            .iter()
            .map(|key| self.known[key].clone())
            .collect();
        if !members
            .iter()
            .any(|p| p.pid == leader.pid && p.token == leader.token)
        {
            members.push(leader.clone());
        }
        Some((
            OwnershipSnapshot {
                leader,
                members,
                descendants_observed: self.descendants_observed,
            },
            end,
        ))
    }
    /// Returns the append-only capture revision without cloning identities, for change-only persistence.
    pub fn capture_revision(&self) -> (usize, bool) {
        (self.known.len(), self.descendants_observed)
    }
    /// Restores previously recorded evidence; every future signal still requires a fresh kernel identity check.
    /// Rejects malformed identities, duplicate members or a missing/mismatched original root.
    pub fn restore(snapshot: OwnershipSnapshot) -> Result<Self> {
        let mut known = BTreeMap::new();
        for member in snapshot.members {
            if member.pid <= 1
                || member.ppid < 0
                || member.group < 0
                || member.token.is_empty()
                || member.token.len() > 256
                || member
                    .program
                    .as_deref()
                    .is_some_and(|name| !valid_program(name))
                || !member.birth.is_finite()
                || member.birth < 0.0
                || known
                    .insert((member.pid, member.token.clone()), member)
                    .is_some()
            {
                return Err(Error::Integrity("invalid captured process identity".into()));
            }
        }
        let leader = snapshot.leader;
        if leader
            .program
            .as_deref()
            .is_some_and(|name| !valid_program(name))
            || known
                .get(&(leader.pid, leader.token.clone()))
                .is_none_or(|root| root.birth != leader.birth || root.group != leader.group)
        {
            return Err(Error::Integrity(
                "captured process root is missing or inconsistent".into(),
            ));
        }
        Ok(Self {
            pid: leader.pid,
            leader: Some(leader),
            captured: known.keys().cloned().collect(),
            known,
            descendants_observed: snapshot.descendants_observed,
            exclusive: None,
        })
    }
    /// Record descendants visible from a complete native process snapshot.
    ///
    /// Captured identities accumulate in `known` and survive later refreshes, so
    /// a descendant seen while the leader lived is still probed individually by
    /// `cleanup`. On Linux, new group members are captured only while the
    /// recorded leader is alive; its number may name an unrelated later group.
    /// The snapshot counts as *verified* descendant evidence only when
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
        self.refresh_from(&all);
    }
    /// Expand captured ownership from one complete native snapshot.
    ///
    /// Each known parent is observed at most once per call, after the snapshot,
    /// exactly as [`observe`] would report it; the verdict is keyed by the
    /// parent's `(pid, token)`, so a reused PID carrying another token is a
    /// different key and never inherits a live verdict.
    fn refresh_from(&mut self, all: &[Identity]) {
        let leader_state = self
            .leader
            .as_ref()
            .map(|p| observe(Some(p.pid), Some(&p.token), Some(p.birth)));
        if leader_state == Some(ProcessState::Alive) {
            self.descendants_observed = true;
        }
        #[cfg(target_os = "linux")]
        let leader_owned = leader_state == Some(ProcessState::Alive);
        #[cfg(not(target_os = "linux"))]
        let leader_owned = matches!(leader_state, Some(ProcessState::Alive | ProcessState::Dead));
        let mut parents = ParentLiveness::default();
        if let (Some(leader), Some(state)) = (&self.leader, leader_state)
            && self
                .known
                .get(&(leader.pid, leader.token.clone()))
                .is_some_and(|root| root.birth == leader.birth)
        {
            // The leader's own observation is the same probe with the same evidence.
            parents.record(leader.pid, &leader.token, state == ProcessState::Alive);
        }
        // Adoption, expansion and reaping hold the admission lock, so no spawn
        // is admitted (and mistaken for an orphan) while they decide.
        #[cfg(target_os = "linux")]
        let admission = subreaper_active().then(adoption::lock);
        #[cfg(target_os = "linux")]
        let adopting = admission.as_ref().is_some_and(|state| {
            self.exclusive
                .is_some_and(|generation| generation == state.generation())
        });
        // Expand only from currently verified owned parents, never an unverified reused PID.
        loop {
            let mut changed = false;
            for p in all {
                if self.knows(p) {
                    continue;
                }
                let in_group = leader_owned && p.group == self.pid;
                // An exclusive owner's adopted orphan is reachable by parentage from
                // this process; registered spawns are never orphans.
                #[cfg(target_os = "linux")]
                let in_group = in_group
                    || (adopting
                        && p.ppid == std::process::id() as i32
                        && admission.as_ref().is_some_and(|state| !state.registered(p)));
                let child = in_group
                    || self
                        .known
                        .range((p.ppid, String::new())..)
                        .take_while(|((pid, _), _)| *pid == p.ppid)
                        .any(|(_, parent)| parents.alive(parent));
                if child {
                    let key = (p.pid, p.token.clone());
                    self.captured.push(key.clone());
                    self.known.insert(key, with_program(p.clone()));
                    changed = true;
                }
            }
            if !changed {
                break;
            }
        }
        #[cfg(target_os = "linux")]
        if let Some(state) = &admission {
            self.reap_captured(state, all);
        }
    }
    /// Reap this owner's captured zombies that are adopted children of this process.
    ///
    /// Runs under the admission lock. Only an exact captured `(pid, token)` is
    /// reaped, after re-inspection shows it is still a zombie child of this
    /// process and not a registered spawn, with `waitpid(pid, WNOHANG)` on that
    /// exact PID. Its original parent is gone and a registered spawn's status
    /// belongs to its spawner, so no other waiter exists; an unreaped child's
    /// PID cannot be reused. `waitpid(-1)` is never used, and orphans this
    /// owner never captured are left alone. A reaped member observes as dead.
    #[cfg(target_os = "linux")]
    fn reap_captured(&self, state: &adoption::State, all: &[Identity]) {
        let me = std::process::id() as i32;
        for p in all.iter().filter(|p| p.ppid == me && p.zombie) {
            if self.knows(p) {
                self.reap_if_adopted_zombie(state, p);
            }
        }
    }
    /// Reap every captured member that is now an adopted zombie of this process.
    ///
    /// The final step before this owner is released; cleanup ends with it and
    /// an owner dropped without cleanup should call it first:
    /// a member can exit after the last settling snapshot, observe as dead in
    /// the cleanup proof and would otherwise stay an unreaped zombie child,
    /// which blocks the next spawn's clean boundary. Same exact checks as
    /// [`Self::reap_captured`], driven by the captured identities instead of a
    /// process snapshot.
    /// Without an active subreaper (and on macOS) this does nothing.
    pub fn reap_captured_zombies(&self) {
        #[cfg(target_os = "linux")]
        if subreaper_active() {
            self.reap_captured_zombies_linux();
        }
    }
    /// Linux body of [`Self::reap_captured_zombies`].
    #[cfg(target_os = "linux")]
    fn reap_captured_zombies_linux(&self) {
        let state = adoption::lock();
        for member in self.known.values() {
            self.reap_if_adopted_zombie(&state, member);
        }
    }
    /// Reap `member` iff it is exactly a captured, unregistered zombie child of
    /// this process and not this owner's leader. Runs under the admission lock.
    #[cfg(target_os = "linux")]
    fn reap_if_adopted_zombie(&self, state: &adoption::State, member: &Identity) {
        let me = std::process::id() as i32;
        if member.pid <= 1
            || self
                .leader
                .as_ref()
                .is_some_and(|leader| leader.pid == member.pid && leader.token == member.token)
        {
            return;
        }
        let Ok(now) = inspect(member.pid) else {
            return;
        };
        if now.zombie && now.ppid == me && now.token == member.token && !state.registered(&now) {
            let mut status = 0;
            // SAFETY: exact positive PID of an unreaped, captured, unregistered zombie child; WNOHANG never blocks.
            unsafe { libc::waitpid(member.pid, &mut status, libc::WNOHANG) };
        }
    }
    /// Period until the next mid-run refresh of this owner.
    ///
    /// An exclusive owner of a subreaper process reaches every orphan of its
    /// tree at termination points, so its mid-run refresh only keeps durable
    /// crash-recovery evidence and zombie reaping current
    /// ([`durable_refresh_interval`]). Every other owner keeps
    /// [`POLLING_REFRESH`], because a descendant which orphans before a refresh
    /// cannot be attributed to it later.
    pub fn refresh_interval(&self) -> std::time::Duration {
        if subreaper_active() && self.exclusive.is_some_and(|g| g == adoption::generation()) {
            durable_refresh_interval()
        } else {
            POLLING_REFRESH
        }
    }
    /// Return whether this exact PID generation is already captured, without allocating a key.
    fn knows(&self, process: &Identity) -> bool {
        self.known
            .range((process.pid, String::new())..)
            .take_while(|((pid, _), _)| *pid == process.pid)
            .any(|((_, token), _)| *token == process.token)
    }
    /// Signal only a currently verified owned process group.
    pub fn signal(&mut self, sig: i32) -> Result<bool> {
        self.refresh();
        let mut signalled = false;
        if let Some(p) = &self.leader
            && p.group == self.pid
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
        Ok(signalled)
    }
    /// Observe a captured descendant by its exact PID and native token (birth
    /// time only for legacy evidence without a native token).
    /// Missing evidence stays unknown; a matching zombie is dead, not signallable.
    fn descendant_state(process: &Identity) -> ProcessState {
        if process.pid <= 1 || process.token.is_empty() || !process.birth.is_finite() {
            return ProcessState::Unknown;
        }
        match inspect(process.pid) {
            Ok(actual) if actual.pid != process.pid => ProcessState::Unknown,
            // A native token is the exact identity; `birth` decides only for
            // legacy evidence without one. Linux birth depends on the wall-clock
            // boot time, which another process can compute differently after a
            // clock step, and must not turn a live exact match into reuse.
            observed => verdict(observed, Some(&process.token), Some(process.birth)),
        }
    }
    /// Signal each still-live captured descendant after a fresh exact identity check.
    /// A vanished, reused, unreadable or incomplete identity is never signalled.
    /// Failed signals do not skip later descendants; evidence remains unconfirmed if they live.
    pub fn signal_descendants(&mut self, sig: i32) -> bool {
        self.refresh();
        let mut signalled = false;
        for process in self.known.values().filter(|process| {
            self.leader
                .as_ref()
                .map_or(process.pid != self.pid, |leader| {
                    process.pid != leader.pid || process.token != leader.token
                })
        }) {
            if Self::descendant_state(process) != ProcessState::Alive {
                continue;
            }
            // SAFETY: the positive PID was just inspected against its captured token and birth.
            if unsafe { libc::kill(process.pid, sig) } == 0 {
                signalled = true;
            }
        }
        signalled
    }
    /// Report whether all captured descendants are gone, or unknown without a verified snapshot.
    fn descendants_gone(&self) -> Option<bool> {
        self.descendants_observed.then(|| {
            let mut unknown = false;
            for process in self.known.values().filter(|process| {
                self.leader
                    .as_ref()
                    .map_or(process.pid != self.pid, |leader| {
                        process.pid != leader.pid || process.token != leader.token
                    })
            }) {
                match Self::descendant_state(process) {
                    ProcessState::Dead | ProcessState::Reused => {}
                    ProcessState::Alive => return false,
                    ProcessState::Unknown | ProcessState::Denied | ProcessState::NotStarted => {
                        unknown = true;
                    }
                }
            }
            !unknown
        })
    }
    /// Refresh parentage and check whether group and descendant evidence can settle.
    /// An absent descendant snapshot cannot confirm cleanup but needs no extra wait.
    ///
    /// One native snapshot serves both the parentage refresh and the group
    /// check. This only decides when waiting may stop: the final cleanup proof
    /// in [`Self::evidence`] still takes its own fresh snapshot.
    fn settled(&mut self) -> bool {
        let all = processes().ok();
        if let Some(all) = &all {
            self.refresh_from(all);
        }
        self.group_observation_with(all.as_deref()) == GroupObservation::Gone
            && self.descendants_gone() != Some(false)
    }
    /// Return whether the leader and its original group are both proven absent.
    pub fn gone(&self) -> bool {
        self.group_observation() == GroupObservation::Gone
    }
    /// Observe the captured leader and its original group without sending a signal.
    fn group_observation(&self) -> GroupObservation {
        self.group_observation_with(None)
    }
    /// Observe the leader freshly, then judge the group from `snapshot` when
    /// given, otherwise from a new native snapshot taken after the leader probe.
    fn group_observation_with(&self, snapshot: Option<&[Identity]>) -> GroupObservation {
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
        let fresh;
        let processes = match snapshot {
            Some(snapshot) => Ok(snapshot),
            None => {
                fresh = processes();
                fresh.as_deref()
            }
        };
        if let Ok(processes) = processes {
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
    /// Blocking twin of [`Self::cleanup`] for synchronous recovery paths.
    /// Group signals keep their leader guard; each surviving captured descendant
    /// is independently verified before TERM and, after a bounded wait, KILL.
    pub fn cleanup_blocking(&mut self, grace: std::time::Duration) -> Result<Cleanup> {
        let mut signals = Vec::new();
        self.refresh();
        if !self.gone() && self.signal(libc::SIGTERM).unwrap_or(false) {
            signals.push("SIGTERM".into());
        }
        if self.signal_descendants(libc::SIGTERM)
            && !signals.iter().any(|signal| signal == "SIGTERM")
        {
            signals.push("SIGTERM".into());
        }
        let until = std::time::Instant::now() + grace;
        while std::time::Instant::now() < until && !self.settled() {
            std::thread::sleep(std::time::Duration::from_millis(25));
        }
        if !self.gone() && self.signal(libc::SIGKILL).unwrap_or(false) {
            signals.push("SIGKILL".into());
        }
        if self.signal_descendants(libc::SIGKILL)
            && !signals.iter().any(|signal| signal == "SIGKILL")
        {
            signals.push("SIGKILL".into());
        }
        let until = std::time::Instant::now() + std::time::Duration::from_secs(2);
        while std::time::Instant::now() < until && !self.settled() {
            std::thread::sleep(std::time::Duration::from_millis(25));
        }
        let settle_deadline = std::time::Instant::now() + std::time::Duration::from_secs(2);
        loop {
            let result = self.evidence(signals.clone());
            if result
                .as_ref()
                .is_ok_and(|proof| proof.confirmed || proof.descendants_gone.is_none())
                || std::time::Instant::now() >= settle_deadline
            {
                self.reap_captured_zombies();
                return result;
            }
            std::thread::sleep(std::time::Duration::from_millis(25));
        }
    }
    /// Terminate the verified group and separately signal captured live descendants.
    /// TERM and KILL use fresh identity checks, bounded waits, and final evidence settling.
    pub async fn cleanup(&mut self, grace: std::time::Duration) -> Result<Cleanup> {
        let mut signals = Vec::new();
        self.refresh();
        if !self.gone() && self.signal(libc::SIGTERM).unwrap_or(false) {
            signals.push("SIGTERM".into());
        }
        if self.signal_descendants(libc::SIGTERM)
            && !signals.iter().any(|signal| signal == "SIGTERM")
        {
            signals.push("SIGTERM".into());
        }
        let until = tokio::time::Instant::now() + grace;
        while tokio::time::Instant::now() < until && !self.settled() {
            tokio::time::sleep(std::time::Duration::from_millis(25)).await;
        }
        if !self.gone() && self.signal(libc::SIGKILL).unwrap_or(false) {
            signals.push("SIGKILL".into());
        }
        if self.signal_descendants(libc::SIGKILL)
            && !signals.iter().any(|signal| signal == "SIGKILL")
        {
            signals.push("SIGKILL".into());
        }
        let until = tokio::time::Instant::now() + std::time::Duration::from_secs(2);
        while tokio::time::Instant::now() < until && !self.settled() {
            tokio::time::sleep(std::time::Duration::from_millis(25)).await;
        }
        let settle_deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(2);
        loop {
            let result = self.evidence(signals.clone());
            if result
                .as_ref()
                .is_ok_and(|proof| proof.confirmed || proof.descendants_gone.is_none())
                || tokio::time::Instant::now() >= settle_deadline
            {
                self.reap_captured_zombies();
                return result;
            }
            tokio::time::sleep(std::time::Duration::from_millis(25)).await;
        }
    }
    /// Final group and exact descendant observation after `signals` were sent.
    /// An unknown group returns an error instead of confirmed cleanup evidence.
    fn evidence(&self, signals: Vec<String>) -> Result<Cleanup> {
        let group = self.group_observation();
        if group == GroupObservation::Unknown {
            return Err(Error::Runtime(
                "process cleanup observation unavailable".into(),
            ));
        }
        let group_gone = group == GroupObservation::Gone;
        let descendants_gone = self.descendants_gone();
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

    #[cfg(target_os = "linux")]
    #[test]
    fn exact_kernel_incarnation_rejects_a_reused_leader() {
        let mut leader = inspect(std::process::id() as i32).unwrap();
        let facts = kernel_incarnation(&leader).unwrap();
        assert_eq!(facts.pid, std::process::id());
        assert_eq!(
            leader.token,
            format!("linux:{}:{}", facts.boot_id, facts.start_ticks)
        );
        leader.token.push('0');
        assert!(kernel_incarnation(&leader).is_err());
    }

    /// After a wall-clock step another process computes a different Linux
    /// birth for the same PID generation; the exact native token still proves
    /// the descendant alive, so cleanup is never confirmed while it runs.
    /// Legacy evidence without a native token keeps the exact birth comparison.
    #[cfg(target_os = "linux")]
    #[test]
    fn a_native_token_outranks_a_stepped_birth() {
        struct Reap(std::process::Child);
        impl Drop for Reap {
            fn drop(&mut self) {
                let _ = self.0.kill();
                let _ = self.0.wait();
            }
        }
        let leader = Reap(
            std::process::Command::new("/bin/sleep")
                .arg("30")
                .spawn()
                .unwrap(),
        );
        let helper = Reap(
            std::process::Command::new("/bin/sleep")
                .arg("30")
                .spawn()
                .unwrap(),
        );
        let mut stepped = inspect(helper.0.id() as i32).unwrap();
        let exact_birth = stepped.birth;
        stepped.birth += 3600.0;
        assert_eq!(
            OwnedProcess::descendant_state(&stepped),
            ProcessState::Alive
        );
        let mut owned = OwnedProcess::capture(leader.0.id() as i32);
        assert!(owned.descendants_observed);
        owned
            .known
            .insert((stepped.pid, stepped.token.clone()), stepped.clone());
        assert_eq!(
            owned.descendants_gone(),
            Some(false),
            "a live descendant is not gone"
        );

        let mut legacy = stepped;
        legacy.token = "legacy-psutil".into();
        legacy.birth = exact_birth;
        assert_eq!(OwnedProcess::descendant_state(&legacy), ProcessState::Alive);
        legacy.birth += 3600.0;
        assert_eq!(
            OwnedProcess::descendant_state(&legacy),
            ProcessState::Reused
        );
    }

    /// Boot facts are read once, match a fresh procfs read and keep the token format.
    #[cfg(target_os = "linux")]
    #[test]
    fn cached_boot_clock_matches_procfs_and_token_format() {
        let cached = boot_clock().unwrap();
        assert!(std::ptr::eq(cached, boot_clock().unwrap()), "read once");
        let fresh = read_boot_clock().unwrap();
        assert_eq!(*cached, fresh);
        assert_eq!(
            fresh.boot_id,
            std::fs::read_to_string("/proc/sys/kernel/random/boot_id")
                .unwrap()
                .trim()
        );
        let me = inspect(std::process::id() as i32).unwrap();
        let stat = std::fs::read(format!("/proc/{}/stat", me.pid)).unwrap();
        let ticks: u64 = std::str::from_utf8(stat_fields(&stat).unwrap()[19])
            .unwrap()
            .parse()
            .unwrap();
        assert_eq!(me.token, format!("linux:{}:{ticks}", fresh.boot_id));
        assert_eq!(me.birth, fresh.btime + ticks as f64 / fresh.hz);
        // Counters in the line change between reads; compare identity fields only.
        let again = read_proc_stat(me.pid).unwrap();
        let identity = |line: &[u8]| {
            let fields = stat_fields(line).unwrap();
            [1, 2, 19].map(|i| fields[i].to_vec())
        };
        assert_eq!(identity(&again), identity(&stat));
    }

    /// A memoized parent verdict is keyed by the exact generation: a recycled
    /// PID carrying another token is probed separately and never alive.
    #[cfg(target_os = "linux")]
    #[test]
    fn memoized_parent_liveness_rejects_a_reused_pid() {
        let live = inspect(std::process::id() as i32).unwrap();
        let mut stale = live.clone();
        stale.token = format!("{}:1", live.token.rsplit_once(':').unwrap().0);
        let mut parents = ParentLiveness::default();
        assert!(parents.alive(&live));
        assert!(!parents.alive(&stale), "same PID, older generation");
        assert!(parents.alive(&live), "the live verdict is unchanged");
        // A recorded verdict is reused for its own key only.
        let mut parents = ParentLiveness::default();
        parents.record(live.pid, &live.token, true);
        assert!(!parents.alive(&stale));
    }

    /// Children of a captured parent whose PID now names another generation
    /// are not captured; the same children are captured from the live parent.
    #[cfg(target_os = "linux")]
    #[test]
    fn refresh_expands_only_from_the_exact_live_parent_generation() {
        struct Reap(std::process::Child);
        impl Drop for Reap {
            fn drop(&mut self) {
                let _ = self.0.kill();
                let _ = self.0.wait();
            }
        }
        let child = Reap(
            std::process::Command::new("/bin/sleep")
                .arg("30")
                .spawn()
                .unwrap(),
        );
        let child_pid = child.0.id() as i32;
        let child_identity = inspect(child_pid).unwrap();
        let live = inspect(std::process::id() as i32).unwrap();
        let mut stale = live.clone();
        stale.token = format!("{}:1", live.token.rsplit_once(':').unwrap().0);
        let captured = |root: &Identity| {
            let mut owner = OwnedProcess::restore(OwnershipSnapshot {
                leader: root.clone(),
                members: vec![root.clone()],
                descendants_observed: false,
            })
            .unwrap();
            owner.refresh();
            owner
                .snapshot()
                .unwrap()
                .members
                .iter()
                .any(|p| p.pid == child_pid && p.token == child_identity.token)
        };
        assert!(!captured(&stale), "a reused parent PID granted ownership");
        assert!(captured(&live), "the exact live parent owns its child");
    }

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

    /// A surviving captured helper receives TERM immediately, without passive exit grace.
    #[tokio::test]
    async fn cleanup_signals_captured_descendant_after_group_exit() {
        use std::os::unix::process::CommandExt;

        let mut helper = std::process::Command::new("/bin/sleep")
            .arg("0.5")
            .spawn()
            .expect("spawn helper");
        let mut leader = std::process::Command::new("/bin/sleep")
            .arg("0.05")
            .process_group(0)
            .spawn()
            .expect("spawn owned leader");
        let mut owned = OwnedProcess::capture(leader.id() as i32);
        let helper_identity = inspect(helper.id() as i32).expect("inspect helper");
        assert_ne!(helper_identity.group, owned.pid);
        owned.known.insert(
            (helper_identity.pid, helper_identity.token.clone()),
            helper_identity,
        );
        leader.wait().expect("reap leader");

        let cleanup = owned
            .cleanup(std::time::Duration::from_millis(750))
            .await
            .expect("cleanup observation available");
        helper.wait().expect("reap helper");
        assert!(cleanup.confirmed);
        assert_eq!(cleanup.descendants_gone, Some(true));
        assert_eq!(cleanup.signals, ["SIGTERM"]);
    }

    /// Spawn a helper in its own group while this test process is the owned leader.
    /// The leader deliberately exits first; the outer regression owns helper cleanup.
    #[test]
    #[allow(clippy::zombie_processes)]
    fn detached_helper_fixture() {
        use std::io::Write;
        use std::os::unix::process::CommandExt;

        if std::env::var_os("AGENT_RUN_DETACHED_HELPER_FIXTURE").is_none() {
            return;
        }
        let helper = std::process::Command::new("/bin/sleep")
            .arg("10")
            .process_group(0)
            .spawn()
            .expect("spawn detached helper");
        println!("DETACHED_HELPER_PID={}", helper.id());
        std::io::stdout().flush().expect("flush helper PID");
        std::thread::sleep(std::time::Duration::from_secs(2));
    }

    /// Cleanup captures a real child by verified parentage, then signals it
    /// after the leader exits even though the child changed process groups.
    #[tokio::test]
    async fn cleanup_terminates_captured_helper_outside_original_group() {
        use std::io::BufRead;
        use std::os::unix::process::CommandExt;

        let mut leader = std::process::Command::new(std::env::current_exe().expect("test binary"))
            .arg("--exact")
            .arg("process::tests::detached_helper_fixture")
            .arg("--nocapture")
            .env("AGENT_RUN_DETACHED_HELPER_FIXTURE", "1")
            .process_group(0)
            .stdout(std::process::Stdio::piped())
            .spawn()
            .expect("spawn owned leader");
        let mut owned = OwnedProcess::capture(leader.id() as i32);
        let mut output = std::io::BufReader::new(leader.stdout.take().expect("fixture output"));
        let helper_pid = (&mut output)
            .lines()
            .find_map(|line| {
                line.ok()?
                    .split("DETACHED_HELPER_PID=")
                    .nth(1)?
                    .trim()
                    .parse::<i32>()
                    .ok()
            })
            .expect("fixture helper PID");
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(1);
        while !owned.known.values().any(|member| member.pid == helper_pid)
            && std::time::Instant::now() < deadline
        {
            owned.refresh();
            std::thread::sleep(std::time::Duration::from_millis(25));
        }
        let helper_identity = owned
            .known
            .values()
            .find(|member| member.pid == helper_pid)
            .expect("captured child identity");
        assert_ne!(helper_identity.group, owned.pid);
        leader.wait().expect("reap leader");
        drop(output);

        // Recovery restores captured evidence even though the original parent is now gone.
        let saved = serde_json::to_vec(&owned.snapshot().expect("captured root")).unwrap();
        let mut owned = OwnedProcess::restore(serde_json::from_slice(&saved).unwrap()).unwrap();

        let cleanup = owned
            .cleanup(std::time::Duration::from_millis(50))
            .await
            .expect("cleanup observation available");
        assert!(cleanup.group_gone);
        assert_eq!(cleanup.descendants_gone, Some(true));
        assert!(cleanup.confirmed);
        assert_eq!(cleanup.signals, ["SIGTERM"]);
    }

    /// Blocking cleanup escalates to KILL for an exact detached identity that
    /// ignores TERM, and confirms its exit independently of the original group.
    #[test]
    fn blocking_cleanup_kills_detached_helper_after_grace() {
        use std::io::BufRead;
        use std::os::unix::process::CommandExt;

        let mut helper = std::process::Command::new("/bin/sh")
            .arg("-c")
            .arg("trap '' TERM; echo ready; exec /bin/sleep 30")
            .process_group(0)
            .stdout(std::process::Stdio::piped())
            .spawn()
            .expect("spawn TERM-resistant helper");
        let mut ready = String::new();
        std::io::BufReader::new(helper.stdout.take().expect("helper output"))
            .read_line(&mut ready)
            .expect("read helper readiness");
        assert_eq!(ready.trim(), "ready");
        let mut leader = std::process::Command::new("/bin/sleep")
            .arg("0.05")
            .process_group(0)
            .spawn()
            .expect("spawn owned leader");
        let mut owned = OwnedProcess::capture(leader.id() as i32);
        let identity = inspect(helper.id() as i32).expect("inspect helper");
        assert_ne!(identity.group, owned.pid);
        owned
            .known
            .insert((identity.pid, identity.token.clone()), identity);
        leader.wait().expect("reap leader");

        let cleanup = owned
            .cleanup_blocking(std::time::Duration::from_millis(50))
            .expect("cleanup observation available");
        if !cleanup.confirmed {
            helper.kill().expect("stop surviving helper");
        }
        helper.wait().expect("reap helper");
        assert!(cleanup.confirmed);
        assert_eq!(cleanup.signals, ["SIGTERM", "SIGKILL"]);
    }

    /// Reused or incomplete descendant identities never authorize a PID signal;
    /// an incomplete live identity also cannot become positive cleanup evidence.
    #[test]
    fn reused_or_unknown_descendant_identity_is_not_signalled() {
        use std::os::unix::process::CommandExt;

        let mut helper = std::process::Command::new("/bin/sleep")
            .arg("30")
            .process_group(0)
            .spawn()
            .expect("spawn unrelated helper");
        let mut leader = std::process::Command::new("/bin/sleep")
            .arg("30")
            .process_group(0)
            .spawn()
            .expect("spawn owned leader");
        let mut owned = OwnedProcess::capture(leader.id() as i32);
        let mut identity = inspect(helper.id() as i32).expect("inspect helper");
        let captured = identity.clone();
        identity.token.push_str(":reused");
        assert_eq!(
            OwnedProcess::descendant_state(&identity),
            ProcessState::Reused
        );
        owned
            .known
            .insert((identity.pid, identity.token.clone()), identity.clone());
        assert!(!owned.signal_descendants(libc::SIGTERM));
        assert!(!inspect(helper.id() as i32).expect("helper survives").zombie);

        // Legacy evidence without a native token is matched by birth time.
        identity = captured;
        identity.token = "legacy-psutil".into();
        identity.birth += 1.0;
        assert_eq!(
            OwnedProcess::descendant_state(&identity),
            ProcessState::Reused
        );
        owned
            .known
            .insert((identity.pid, identity.token.clone()), identity.clone());
        assert!(!owned.signal_descendants(libc::SIGTERM));
        assert!(!inspect(helper.id() as i32).expect("helper survives").zombie);

        identity.token.clear();
        assert_eq!(
            OwnedProcess::descendant_state(&identity),
            ProcessState::Unknown
        );
        owned
            .known
            .insert((identity.pid, identity.token.clone()), identity);
        assert!(!owned.signal_descendants(libc::SIGKILL));
        assert_eq!(owned.descendants_gone(), Some(false));
        assert!(!inspect(helper.id() as i32).expect("helper survives").zombie);
        helper.kill().expect("stop helper");
        helper.wait().expect("reap helper");
        leader.kill().expect("stop leader");
        leader.wait().expect("reap leader");
    }

    /// An unverified group does not block safe individual cleanup, but the
    /// final outcome cannot claim confirmed ownership teardown.
    #[tokio::test]
    async fn unknown_group_still_cleans_verified_descendant_without_confirmation() {
        use std::os::unix::process::CommandExt;

        let mut helper = std::process::Command::new("/bin/sleep")
            .arg("30")
            .process_group(0)
            .spawn()
            .expect("spawn helper");
        let mut leader = std::process::Command::new("/bin/sleep")
            .arg("30")
            .process_group(0)
            .spawn()
            .expect("spawn leader");
        let mut owned = OwnedProcess::capture(leader.id() as i32);
        let identity = inspect(helper.id() as i32).expect("inspect helper");
        owned
            .known
            .insert((identity.pid, identity.token.clone()), identity);
        owned.leader = None;
        assert_eq!(owned.group_observation(), GroupObservation::Unknown);

        let result = owned.cleanup(std::time::Duration::from_millis(50)).await;
        let helper_exited = helper.try_wait().expect("observe helper").is_some();
        if !helper_exited {
            helper.kill().expect("stop surviving helper");
        }
        helper.wait().expect("reap helper");
        leader.kill().expect("stop unverified leader");
        leader.wait().expect("reap leader");
        assert!(
            helper_exited,
            "captured helper should receive an individual signal"
        );
        assert!(result.is_err(), "unknown group cannot confirm cleanup");
    }
}
