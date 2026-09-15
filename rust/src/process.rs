//! PID reuse-aware process ownership. Unknown/denied evidence is never death.
use crate::{Error, Result};
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
#[cfg(target_os = "linux")]
pub fn inspect(pid: i32) -> std::io::Result<Identity> {
    if pid <= 1 {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "unsafe process id",
        ));
    }
    let stat = std::fs::read_to_string(format!("/proc/{pid}/stat"))?;
    let end = stat
        .rfind(')')
        .ok_or_else(|| std::io::Error::new(std::io::ErrorKind::InvalidData, "proc stat"))?;
    let parts: Vec<_> = stat[end + 1..].split_whitespace().collect();
    if parts.len() < 20 {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "proc stat",
        ));
    }
    let num = |i: usize| {
        parts[i]
            .parse::<u64>()
            .map_err(|_| std::io::Error::new(std::io::ErrorKind::InvalidData, "proc field"))
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
        zombie: matches!(parts[0], "Z" | "X"),
    })
}
/// Read one PID's kernel start time, group and zombie state.
///
/// `birth` is the kernel `p_start` timeval computed exactly like psutil's
/// `tv_sec + tv_usec / 1000000.0`, so it equals Python's stored create_time
/// bit for bit (migration/adr/A10-spawn-backend.md).
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
        if !matches!(
            error.raw_os_error(),
            Some(libc::EPERM) | Some(libc::EACCES)
        ) {
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
pub fn processes() -> Result<Vec<Identity>> {
    #[cfg(target_os = "linux")]
    let ids: Vec<i32> = std::fs::read_dir("/proc")?
        .filter_map(|e| e.ok()?.file_name().to_str()?.parse().ok())
        .collect();
    #[cfg(target_os = "macos")]
    let ids: Vec<i32> = {
        let out = std::process::Command::new("/bin/ps")
            .args(["-axo", "pid="])
            .output()?;
        if !out.status.success() {
            return Err(Error::Runtime("process observation unavailable".into()));
        }
        String::from_utf8_lossy(&out.stdout)
            .split_whitespace()
            .filter_map(|s| s.parse().ok())
            .collect()
    };
    Ok(ids
        .into_iter()
        .filter_map(|pid| inspect(pid).ok())
        .collect())
}
#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct Cleanup {
    pub signals: Vec<String>,
    pub scope: String,
    pub group_gone: bool,
    pub descendants_gone: Option<bool>,
    pub confirmed: bool,
    pub process_group_id: Option<i32>,
}
pub struct OwnedProcess {
    pub leader: Option<Identity>,
    pub pid: i32,
    known: BTreeMap<i32, Identity>,
}
impl OwnedProcess {
    pub fn capture(pid: i32) -> Self {
        let leader = inspect(pid).ok();
        let known = leader.clone().into_iter().map(|p| (p.pid, p)).collect();
        Self { leader, pid, known }
    }
    pub fn refresh(&mut self) {
        let Ok(all) = processes() else {
            return;
        };
        let leader_alive = self.leader.as_ref().is_some_and(|p| {
            observe(Some(p.pid), Some(&p.token), Some(p.birth)) == ProcessState::Alive
        });
        // Expand only from currently verified owned parents, never an unverified reused PID.
        loop {
            let mut changed = false;
            for p in &all {
                let child = self.known.get(&p.ppid).is_some_and(|parent| {
                    observe(Some(parent.pid), Some(&parent.token), Some(parent.birth))
                        == ProcessState::Alive
                });
                let in_group = leader_alive && p.group == self.pid;
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
    pub fn signal(&mut self, sig: i32) -> Result<bool> {
        self.refresh();
        let mut signalled = false;
        if let Some(p) = &self.leader {
            if p.group == self.pid
                && observe(Some(p.pid), Some(&p.token), Some(p.birth)) == ProcessState::Alive
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
        for p in self.known.values() {
            if p.pid > 1
                && observe(Some(p.pid), Some(&p.token), Some(p.birth)) == ProcessState::Alive
            {
                // SAFETY: the immutable birth token was checked immediately before this signal.
                let code = unsafe { libc::kill(p.pid, sig) };
                if code == 0 {
                    signalled = true;
                }
            }
        }
        Ok(signalled)
    }
    pub fn gone(&self) -> bool {
        let leader = match &self.leader {
            Some(p) => matches!(
                observe(Some(p.pid), Some(&p.token), Some(p.birth)),
                ProcessState::Dead
            ),
            None => match inspect(self.pid) {
                Err(e) => matches!(e.raw_os_error(), Some(libc::ENOENT) | Some(libc::ESRCH)),
                Ok(p) => p.zombie,
            },
        };
        if !leader {
            return false;
        }
        let Ok(all) = processes() else {
            return false;
        };
        !all.iter().any(|p| p.group == self.pid && !p.zombie)
            && self.known.values().all(|p| {
                matches!(
                    observe(Some(p.pid), Some(&p.token), Some(p.birth)),
                    ProcessState::Dead
                )
            })
    }
    pub async fn cleanup(&mut self, grace: std::time::Duration) -> Cleanup {
        let mut signals = Vec::new();
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
        let gone = self.gone();
        Cleanup {
            signals,
            scope: "verified_descendants".into(),
            group_gone: gone,
            descendants_gone: Some(gone),
            confirmed: gone,
            process_group_id: Some(self.pid),
        }
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn unreadable_evidence_is_never_death() {
        let failed = |code| Err(std::io::Error::from_raw_os_error(code));
        assert_eq!(verdict(failed(libc::EPERM), None, Some(1.0)), ProcessState::Denied);
        assert_eq!(verdict(failed(libc::EACCES), None, Some(1.0)), ProcessState::Denied);
        assert_eq!(verdict(failed(libc::EIO), None, Some(1.0)), ProcessState::Unknown);
        assert_eq!(verdict(failed(libc::ESRCH), None, Some(1.0)), ProcessState::Dead);
        assert_eq!(verdict(failed(libc::ENOENT), None, Some(1.0)), ProcessState::Dead);
    }
}
