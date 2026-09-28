//! Dense process-wide logging to private UTC-daily files with stderr fallback.

use agent_run_platform::fs;
use std::{
    ffi::CString,
    fs::{File, OpenOptions},
    io::Write,
    os::{
        fd::{AsRawFd, FromRawFd},
        unix::fs::{MetadataExt, OpenOptionsExt, PermissionsExt},
    },
    path::Path,
    sync::{Arc, Mutex, OnceLock},
};

/// Process-wide log severity, ordered from least to most important.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum Level {
    /// Detailed lifecycle and diagnostic information.
    Debug,
    /// Normal lifecycle information.
    Info,
    /// Recoverable operational problem.
    Warning,
    /// Failed operation requiring attention.
    Error,
}

impl Level {
    /// Parses a conventional environment level, defaulting to dense debug logs.
    fn from_environment() -> Self {
        match std::env::var("AGENT_RUN_LOG_LEVEL")
            .unwrap_or_else(|_| "DEBUG".into())
            .trim()
            .to_ascii_uppercase()
            .as_str()
        {
            "INFO" => Self::Info,
            "WARNING" | "WARN" => Self::Warning,
            "ERROR" => Self::Error,
            _ => Self::Debug,
        }
    }
}

/// One configured output sink and its fixed component label.
pub struct Logger {
    /// Minimum severity written by this logger.
    level: Level,
    /// Stable component name included on every line.
    component: String,
    /// File sink or stderr fallback, serialized for concurrent callers.
    sink: Mutex<Sink>,
}

/// The two safe destinations selected during configuration and rollover.
enum Sink {
    /// Append-only daily file and its no-follow parent directory descriptor.
    File {
        directory: File,
        day: String,
        file: File,
    },
    /// Process stderr when the home cannot host logs.
    Stderr,
}

static LOGGER: OnceLock<Mutex<Option<Arc<Logger>>>> = OnceLock::new();

/// Returns the process-wide logger slot without opening files.
fn slot() -> &'static Mutex<Option<Arc<Logger>>> {
    LOGGER.get_or_init(|| Mutex::new(None))
}

/// Returns the current UTC calendar day used in a direct daily filename.
fn utc_day() -> String {
    chrono::Utc::now().format("%Y-%m-%d").to_string()
}

/// Opens one real, singly linked private regular file under an already-open log directory.
/// The safe component and date become one basename; no rename or truncation occurs.
fn open_daily(directory: &File, component: &str, day: &str) -> std::io::Result<File> {
    if component.is_empty()
        || !component
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || b"-_".contains(&byte))
        || day.len() != 10
        || !day.bytes().enumerate().all(|(index, byte)| {
            if index == 4 || index == 7 {
                byte == b'-'
            } else {
                byte.is_ascii_digit()
            }
        })
    {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "invalid log basename",
        ));
    }
    let name = CString::new(format!("{component}.{day}.log")).expect("validated ASCII basename");
    // SAFETY: directory and name stay live; openat returns a new owned descriptor.
    let descriptor = unsafe {
        libc::openat(
            directory.as_raw_fd(),
            name.as_ptr(),
            libc::O_WRONLY
                | libc::O_APPEND
                | libc::O_CREAT
                | libc::O_NOFOLLOW
                | libc::O_CLOEXEC
                | libc::O_NONBLOCK,
            0o600,
        )
    };
    if descriptor < 0 {
        return Err(std::io::Error::last_os_error());
    }
    // SAFETY: this function exclusively owns the successful openat descriptor.
    let file = unsafe { File::from_raw_fd(descriptor) };
    let metadata = file.metadata()?;
    // SAFETY: geteuid reports the current process user without retaining state.
    if !metadata.is_file() || metadata.nlink() != 1 || metadata.uid() != unsafe { libc::geteuid() }
    {
        return Err(std::io::Error::new(
            std::io::ErrorKind::PermissionDenied,
            "unsafe log file",
        ));
    }
    file.set_permissions(std::fs::Permissions::from_mode(0o600))?;
    Ok(file)
}

/// Creates or opens the private logs directory without following its final link.
fn file_sink(home: &Path, component: &str, day: String) -> std::io::Result<Sink> {
    let log_dir = home.join("logs");
    fs::private_dir(&log_dir).map_err(|_| std::io::Error::other("unsafe logs directory"))?;
    let directory = OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NOFOLLOW | libc::O_DIRECTORY | libc::O_CLOEXEC)
        .open(&log_dir)?;
    let file = open_daily(&directory, component, &day)?;
    Ok(Sink::File {
        directory,
        day,
        file,
    })
}

/// Configures one idempotent dense component logger with direct UTC-daily files.
pub fn configure(home: &Path, component: &str) -> Arc<Logger> {
    let mut configured = slot().lock().expect("logger lock is not poisoned");
    if let Some(logger) = configured.as_ref() {
        return logger.clone();
    }
    let sink = file_sink(home, component, utc_day()).unwrap_or(Sink::Stderr);
    let logger = Arc::new(Logger {
        level: Level::from_environment(),
        component: component.into(),
        sink: Mutex::new(sink),
    });
    *configured = Some(logger.clone());
    logger
}

/// Returns the configured logger, if an entry point has initialized logging.
pub fn configured() -> Option<Arc<Logger>> {
    slot().lock().expect("logger lock is not poisoned").clone()
}

impl Logger {
    /// Returns the effective minimum severity selected from the environment.
    pub fn level(&self) -> Level {
        self.level
    }

    /// Returns the first-call component label retained by idempotent setup.
    pub fn component(&self) -> &str {
        &self.component
    }

    /// Returns whether the current sink is a daily file rather than stderr.
    pub fn uses_file(&self) -> bool {
        matches!(
            *self.sink.lock().expect("logger lock is not poisoned"),
            Sink::File { .. }
        )
    }

    /// Writes one bounded lifecycle line to the current UTC day without renaming shared files.
    pub fn log(&self, level: Level, message: &str) {
        self.log_on(level, message, &utc_day());
    }

    /// Writes one line on the given validated UTC day; tests inject a date here.
    fn log_on(&self, level: Level, message: &str, day: &str) {
        if level < self.level {
            return;
        }
        let line = format!("{level:?} {} {message}\n", self.component);
        let mut sink = self.sink.lock().expect("logger lock is not poisoned");
        match &mut *sink {
            Sink::File {
                directory,
                day: active,
                file,
            } => {
                if active != day {
                    match open_daily(directory, &self.component, day) {
                        Ok(next) => {
                            *file = next;
                            *active = day.to_owned();
                        }
                        Err(_) => {
                            *sink = Sink::Stderr;
                            eprint!("{line}");
                            return;
                        }
                    }
                }
                if file.write_all(line.as_bytes()).is_err() {
                    *sink = Sink::Stderr;
                    eprint!("{line}");
                }
            }
            Sink::Stderr => eprint!("{line}"),
        }
    }
}

/// Emits the safe start lifecycle fields when logging is configured.
pub fn start(runtime: &str, model: &str, agent_id: &str, created: bool) {
    if let Some(logger) = configured() {
        logger.log(
            Level::Info,
            &format!("start runtime={runtime} model={model} agent_id={agent_id} created={created}"),
        );
    }
}

/// Emits one bounded configuration reload outcome when logging is configured.
///
/// `accepted` marks a newly adopted revision; `!accepted` marks invalid bytes
/// whose rejection leaves the named revision active.  `revision` is the
/// lowercase SHA-256 of the exact `config.toml` bytes for the active cached
/// revision, or `None` when no valid revision has been cached yet, which is
/// enough to distinguish successful adoption from a rejected change.
/// Configuration contents, paths, environment values, and credentials are
/// never included. Accepted revisions are normal `Info`; rejected revisions
/// are recoverable `Warning` events so operators cannot suppress invalid
/// configuration diagnostics with the conventional warning threshold.
pub fn config_reload(accepted: bool, revision: Option<&str>) {
    if let Some(logger) = configured() {
        let outcome = if accepted { "accepted" } else { "rejected" };
        let revision = revision.unwrap_or("none");
        logger.log(
            if accepted {
                Level::Info
            } else {
                Level::Warning
            },
            &format!("config reload {outcome} revision={revision}"),
        );
    }
}

/// Clears the process logger for isolated Rust tests.
#[doc(hidden)]
pub fn reset_for_tests() {
    *slot().lock().expect("logger lock is not poisoned") = None;
}

#[cfg(test)]
/// Deterministic daily-file and no-follow checks without changing the system clock.
mod tests {
    use super::*;

    /// Constructs an independent writer whose first file is already open on `day`.
    fn writer(home: &Path, day: &str) -> Logger {
        Logger {
            level: Level::Debug,
            component: "cli".into(),
            sink: Mutex::new(file_sink(home, "cli", day.into()).unwrap()),
        }
    }

    /// Separate writers share append files across UTC rollover without renaming legacy logs.
    #[test]
    fn daily_rollover_preserves_both_writers_and_legacy_file() {
        let home = tempfile::tempdir().unwrap();
        let first = writer(home.path(), "2026-09-27");
        let second = writer(home.path(), "2026-09-27");
        let logs = home.path().join("logs");
        std::fs::write(logs.join("cli.log"), "legacy\n").unwrap();
        first.log_on(Level::Info, "first-old", "2026-09-27");
        second.log_on(Level::Info, "second-old", "2026-09-27");
        first.log_on(Level::Info, "first-new", "2026-09-28");
        second.log_on(Level::Info, "second-late-old", "2026-09-27");
        second.log_on(Level::Info, "second-new", "2026-09-28");
        let old = std::fs::read_to_string(logs.join("cli.2026-09-27.log")).unwrap();
        let new = std::fs::read_to_string(logs.join("cli.2026-09-28.log")).unwrap();
        for message in ["first-old", "second-old", "second-late-old"] {
            assert!(old.contains(message));
        }
        for message in ["first-new", "second-new"] {
            assert!(new.contains(message));
        }
        assert_eq!(
            std::fs::read_to_string(logs.join("cli.log")).unwrap(),
            "legacy\n"
        );
        assert_eq!(
            std::fs::metadata(logs.join("cli.2026-09-28.log"))
                .unwrap()
                .permissions()
                .mode()
                & 0o777,
            0o600
        );
    }

    /// A planted logs symlink is rejected before creating or opening a daily file.
    #[test]
    fn planted_logs_symlink_fails_closed() {
        let home = tempfile::tempdir().unwrap();
        let outside = tempfile::tempdir().unwrap();
        std::os::unix::fs::symlink(outside.path(), home.path().join("logs")).unwrap();
        assert!(file_sink(home.path(), "cli", "2026-09-28".into()).is_err());
        assert_eq!(std::fs::read_dir(outside.path()).unwrap().count(), 0);

        let other_home = tempfile::tempdir().unwrap();
        std::fs::create_dir(other_home.path().join("logs")).unwrap();
        let target = outside.path().join("target");
        std::fs::write(&target, "untouched").unwrap();
        std::os::unix::fs::symlink(&target, other_home.path().join("logs/cli.2026-09-28.log"))
            .unwrap();
        assert!(file_sink(other_home.path(), "cli", "2026-09-28".into()).is_err());
        assert_eq!(std::fs::read_to_string(target).unwrap(), "untouched");
    }
}
