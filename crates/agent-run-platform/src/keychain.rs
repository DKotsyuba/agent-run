//! macOS Keychain access without exposing credential values in diagnostics.

#[cfg(target_os = "macos")]
use std::{
    io::{self, Read},
    os::fd::AsRawFd,
    process::Command,
    thread,
    time::{Duration, Instant},
};

/// Reads one generic-password item from the login Keychain without logging its value.
///
/// `account` and `service` select the item provisioned by the owner.  The
/// lookup is bounded to five seconds from spawn through output drain, like the
/// Python runtime, and returns
/// `None` for an unavailable, locked, missing, blank, or non-macOS Keychain.
/// The returned text is trimmed but is otherwise never serialized or logged.
#[cfg(target_os = "macos")]
pub fn generic_password(account: &str, service: &str) -> Option<String> {
    lookup(service, Some(account))
}

/// Reads a service-selected generic password with no fixed account selector.
///
/// Claude's legacy credential is keyed only by service.  The result is never
/// logged or serialized; callers should reduce it to a presence signal unless
/// they are the credential adapter that consumes it.
#[cfg(target_os = "macos")]
pub fn generic_password_service(service: &str) -> Option<String> {
    lookup(service, None)
}

/// Performs one bounded login-Keychain lookup and returns a nonempty secret.
///
/// `account=None` deliberately omits `security -a`, which is required for
/// service-only legacy entries.  Every non-success outcome is indistinguishable
/// from an unavailable credential to avoid diagnostics leaking Keychain state.
#[cfg(target_os = "macos")]
fn lookup(service: &str, account: Option<&str>) -> Option<String> {
    // Python uses pwd.getpwuid rather than the generated runtime HOME so the
    // login keychain remains addressable after adapters replace HOME.
    // SAFETY: libc returns either null or a valid passwd entry owned by libc.
    let entry = unsafe { libc::getpwuid(libc::geteuid()) };
    if entry.is_null() {
        return None;
    }
    // SAFETY: a non-null passwd entry has a null-terminated pw_dir string.
    let home = unsafe { std::ffi::CStr::from_ptr((*entry).pw_dir) }
        .to_str()
        .ok()?;
    let path = format!("{home}/Library/Keychains/login.keychain-db");
    let mut command = Command::new("/usr/bin/security");
    command.arg("find-generic-password").arg("-s").arg(service);
    if let Some(account) = account {
        command.arg("-a").arg(account);
    }
    command.args(["-w", &path]);
    lookup_command(command, Duration::from_secs(5))
}

/// Runs one Keychain command with a deadline covering process exit and stdout EOF.
#[cfg(target_os = "macos")]
fn lookup_command(mut command: Command, timeout: Duration) -> Option<String> {
    let deadline = Instant::now() + timeout;
    let mut child = command
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::null())
        .spawn()
        .ok()?;
    loop {
        match child.try_wait() {
            Ok(Some(status)) if status.success() => {
                let output = {
                    let stdout = child.stdout.as_mut()?;
                    read_until(stdout, deadline)
                };
                let Some(output) = output else {
                    let _ = child.kill();
                    let _ = child.wait();
                    return None;
                };
                let value = String::from_utf8(output).ok()?;
                return (!value.trim().is_empty()).then(|| value.trim().to_owned());
            }
            Ok(Some(_)) | Err(_) => return None,
            Ok(None) if Instant::now() >= deadline => {
                let _ = child.kill();
                let _ = child.wait();
                return None;
            }
            Ok(None) => thread::sleep(Duration::from_millis(10)),
        }
    }
}

/// Drains a child stdout pipe until EOF or the shared command deadline.
#[cfg(target_os = "macos")]
fn read_until(output: &mut (impl Read + AsRawFd), deadline: Instant) -> Option<Vec<u8>> {
    let fd = output.as_raw_fd();
    let mut bytes = Vec::new();
    loop {
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            return None;
        }
        let mut pollfd = libc::pollfd {
            fd,
            events: libc::POLLIN,
            revents: 0,
        };
        let millis = remaining.as_millis().max(1).min(libc::c_int::MAX as u128) as libc::c_int;
        // SAFETY: `pollfd` points to one valid descriptor for this call.
        let ready = unsafe { libc::poll(&mut pollfd, 1, millis) };
        if ready == 0 {
            return None;
        }
        if ready < 0 {
            let error = io::Error::last_os_error();
            if error.kind() == io::ErrorKind::Interrupted {
                continue;
            }
            return None;
        }
        let mut chunk = [0u8; 4096];
        match output.read(&mut chunk) {
            Ok(0) => return Some(bytes),
            Ok(read) => bytes.extend_from_slice(&chunk[..read]),
            Err(error) if error.kind() == io::ErrorKind::Interrupted => {}
            Err(_) => return None,
        }
    }
}

#[cfg(all(test, target_os = "macos"))]
mod tests {
    use super::*;

    // Protects the probe from a descendant that inherits security's stdout pipe.
    #[test]
    fn keychain_probe_bounds_stdout_drain() {
        let mut command = Command::new("/bin/sh");
        command.args([
            "-c",
            "tail -f /dev/null & holder=$!; (sleep 1; kill $holder) & exit 0",
        ]);
        let started = Instant::now();
        assert_eq!(lookup_command(command, Duration::from_millis(50)), None);
        assert!(started.elapsed() < Duration::from_millis(500));
    }
}

/// Returns no credential on platforms without the macOS login Keychain.
#[cfg(not(target_os = "macos"))]
pub fn generic_password(_account: &str, _service: &str) -> Option<String> {
    None
}

/// Returns no service-only credential on platforms without the macOS Keychain.
#[cfg(not(target_os = "macos"))]
pub fn generic_password_service(_service: &str) -> Option<String> {
    None
}
