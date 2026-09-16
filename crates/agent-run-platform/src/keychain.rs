//! macOS Keychain access without exposing credential values in diagnostics.

use std::{
    process::Command,
    thread,
    time::{Duration, Instant},
};

/// Reads one generic-password item from the login Keychain without logging its value.
///
/// `account` and `service` select the item provisioned by the owner.  The
/// lookup is limited to three seconds like the Python runtime and returns
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
    let mut child = command
        .args(["-w", &path])
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::null())
        .spawn()
        .ok()?;
    let deadline = Instant::now() + Duration::from_secs(3);
    loop {
        match child.try_wait() {
            Ok(Some(status)) if status.success() => {
                let output = child.wait_with_output().ok()?;
                let value = String::from_utf8(output.stdout).ok()?;
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
