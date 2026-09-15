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
#[cfg(target_os = "macos")]
mod darwin {
    #[repr(C)]
    #[derive(Default)]
    pub struct BsdInfo {
        pub flags: u32,
        pub status: u32,
        pub xstatus: u32,
        pub pid: u32,
        pub ppid: u32,
        pub uid: u32,
        pub gid: u32,
        pub ruid: u32,
        pub rgid: u32,
        pub svuid: u32,
        pub svgid: u32,
        pub rfu: u32,
        pub comm: [libc::c_char; 16],
        pub name: [libc::c_char; 32],
        pub nfiles: u32,
        pub pgid: u32,
        pub jobc: u32,
        pub tdev: u32,
        pub tpgid: u32,
        pub nice: i32,
        pub start_sec: u64,
        pub start_usec: u64,
    }
    #[link(name = "proc")]
    extern "C" {
        pub fn proc_pidinfo(
            pid: libc::c_int,
            flavor: libc::c_int,
            arg: u64,
            buffer: *mut libc::c_void,
            size: libc::c_int,
        ) -> libc::c_int;
    }
}
#[cfg(target_os = "macos")]
pub fn inspect(pid: i32) -> std::io::Result<Identity> {
    if pid <= 1 {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "unsafe process id",
        ));
    }
    let mut info = darwin::BsdInfo::default();
    let size = std::mem::size_of::<darwin::BsdInfo>();
    // SAFETY: repr(C) buffer matches proc_bsdinfo and is sized for PROC_PIDTBSDINFO (3).
    let n = unsafe {
        darwin::proc_pidinfo(
            pid,
            3,
            0,
            &mut info as *mut _ as *mut libc::c_void,
            size as i32,
        )
    };
    if n != size as i32 {
        return Err(std::io::Error::last_os_error());
    }
    Ok(Identity {
        pid,
        ppid: info.ppid as i32,
        group: info.pgid as i32,
        birth: info.start_sec as f64 + info.start_usec as f64 / 1_000_000.,
        token: format!("darwin:{}:{}", info.start_sec, info.start_usec),
        zombie: info.status == 5,
    })
}
pub fn observe(pid: Option<i32>, token: Option<&str>, birth: Option<f64>) -> ProcessState {
    let Some(pid) = pid else {
        return ProcessState::NotStarted;
    };
    match inspect(pid) {
        Ok(actual) => {
            let matches = if let Some(token) =
                token.filter(|s| s.starts_with("linux:") || s.starts_with("darwin:"))
            {
                actual.token == token
            } else if let Some(birth) = birth {
                (actual.birth - birth).abs() < 0.000_001
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
