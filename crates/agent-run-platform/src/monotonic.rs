//! Same-host monotonic clock and boot-session identity shared by every SQLite
//! writer participant.
//!
//! A write deadline and an operation identity are only comparable between
//! processes when they use one clock domain. This module is that domain:
//! `CLOCK_MONOTONIC` nanoseconds plus an identifier of the current boot
//! session. Neither value is a wall-clock reading, so a clock step cannot move
//! a deadline or change the boot identity. The boot identity is read once per
//! process and cached; a deadline whose boot identity differs from the current
//! one belongs to another boot and must be refused by its consumer.
use std::{io, sync::OnceLock};

/// Process-wide cache of the boot-session identifier, filled on first use.
static BOOT_ID: OnceLock<String> = OnceLock::new();

/// Returns the current `CLOCK_MONOTONIC` reading in nanoseconds.
///
/// The value is a non-decreasing offset from an arbitrary point that is shared
/// by all processes of the same boot on this host. It is only meaningful with
/// the matching [`boot_id`]. It never fails: the clock id is mandatory on every
/// supported platform, and a failed read would be a kernel invariant breach.
///
/// # Panics
/// Panics if `clock_gettime(CLOCK_MONOTONIC)` fails, which POSIX only allows
/// for an invalid clock id or pointer, neither of which this call can pass.
pub fn now_ns() -> u64 {
    let mut spec = libc::timespec {
        tv_sec: 0,
        tv_nsec: 0,
    };
    // SAFETY: `spec` is a valid, writable timespec and CLOCK_MONOTONIC is a
    // constant clock id accepted by every supported kernel.
    let status = unsafe { libc::clock_gettime(libc::CLOCK_MONOTONIC, &mut spec) };
    assert_eq!(status, 0, "CLOCK_MONOTONIC must be readable");
    (spec.tv_sec as u64)
        .saturating_mul(1_000_000_000)
        .saturating_add(spec.tv_nsec as u64)
}

/// Returns the identifier of the current boot session.
///
/// The text is stable for the whole boot, differs between boots and does not
/// change when the wall clock is stepped: Linux uses
/// `/proc/sys/kernel/random/boot_id` and macOS uses `kern.bootsessionuuid`
/// (never `kern.boottime`, which follows wall-clock adjustments). The result
/// is lowercase ASCII, at most 64 bytes, and cached after the first success.
///
/// # Errors
/// Returns the underlying I/O error when the platform source is unreadable or
/// empty; callers must then refuse to issue or accept deadlines, not guess.
pub fn boot_id() -> io::Result<String> {
    if let Some(cached) = BOOT_ID.get() {
        return Ok(cached.clone());
    }
    let read = read_boot_id()?;
    Ok(BOOT_ID.get_or_init(|| read).clone())
}

/// Returns one coherent `(boot_id, monotonic_ns)` reading.
///
/// The boot identity is resolved first so a failure never yields a half-valid
/// pair; the nanosecond value is sampled immediately afterwards.
///
/// # Errors
/// Propagates [`boot_id`] failures.
pub fn now() -> io::Result<(String, u64)> {
    let boot = boot_id()?;
    Ok((boot, now_ns()))
}

/// Reads and normalizes the boot-session identifier from the host.
#[cfg(target_os = "linux")]
fn read_boot_id() -> io::Result<String> {
    normalize(&std::fs::read_to_string("/proc/sys/kernel/random/boot_id")?)
}

/// Reads and normalizes the boot-session identifier from the host.
#[cfg(target_os = "macos")]
fn read_boot_id() -> io::Result<String> {
    let name = c"kern.bootsessionuuid";
    let mut buffer = [0u8; 64];
    let mut length = buffer.len();
    // SAFETY: `name` is NUL-terminated, `buffer` is writable for `length`
    // bytes and `length` is a valid in/out size; no new value is supplied.
    let status = unsafe {
        libc::sysctlbyname(
            name.as_ptr(),
            buffer.as_mut_ptr().cast(),
            &mut length,
            std::ptr::null_mut(),
            0,
        )
    };
    if status != 0 {
        return Err(io::Error::last_os_error());
    }
    let text = std::str::from_utf8(&buffer[..length.min(buffer.len())])
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "boot session id is not UTF-8"))?;
    normalize(text)
}

/// Rejects hosts without a boot-session source instead of inventing one.
#[cfg(not(any(target_os = "linux", target_os = "macos")))]
fn read_boot_id() -> io::Result<String> {
    Err(io::Error::new(
        io::ErrorKind::Unsupported,
        "boot session identity is unavailable on this platform",
    ))
}

/// Trims NUL/whitespace, lowercases and validates the identifier shape.
///
/// Accepts only non-empty ASCII alphanumerics and `-`, at most 64 bytes, so
/// the value is safe to embed in identifiers and canonical JSON.
fn normalize(raw: &str) -> io::Result<String> {
    let text = raw
        .trim_matches(|c: char| c == '\0' || c.is_whitespace())
        .to_ascii_lowercase();
    if text.is_empty()
        || text.len() > 64
        || !text.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'-')
    {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "boot session id has an unsupported shape",
        ));
    }
    Ok(text)
}

#[cfg(test)]
mod tests {
    //! Clock monotonicity and boot-identity shape checks.
    use super::*;

    /// The clock never moves backwards and advances across a real sleep.
    #[test]
    fn clock_is_monotonic_and_advances() {
        let first = now_ns();
        std::thread::sleep(std::time::Duration::from_millis(5));
        let second = now_ns();
        assert!(second >= first + 4_000_000, "{first} -> {second}");
    }

    /// The boot identity is stable, normalized and paired with a reading.
    #[test]
    fn boot_identity_is_stable_and_well_formed() {
        let first = boot_id().expect("boot id");
        assert_eq!(first, boot_id().expect("cached boot id"));
        assert!(!first.is_empty() && first.len() <= 64);
        assert!(
            first
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b == b'-')
        );
        assert_eq!(first, first.to_ascii_lowercase());
        let (boot, ns) = now().expect("pair");
        assert_eq!(boot, first);
        assert!(ns > 0);
    }

    /// Malformed or empty platform text is refused, not repaired.
    #[test]
    fn normalization_rejects_unsafe_text() {
        assert_eq!(normalize(" ABC-123\n\0").unwrap(), "abc-123");
        assert!(normalize("").is_err());
        assert!(normalize("a b").is_err());
        assert!(normalize(&"a".repeat(65)).is_err());
        assert!(normalize("é").is_err());
    }
}
