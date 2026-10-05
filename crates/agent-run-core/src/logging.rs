//! Private UTC-daily operational logs: at most 4 MiB per component/day across
//! cooperating writers and 8 KiB per record. Lock contention, overflow and unsafe
//! sinks drop whole records; I/O failures may retain a bounded prefix. Nothing
//! is redirected to stderr, and failed writes never truncate existing bytes.
//! Existing logs and the separate 30-day retention policy are preserved.

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
    /// Private file sink or silent suppression, serialized for concurrent callers.
    sink: Mutex<Sink>,
}

/// Private append destination or silent suppression selected during setup.
enum Sink {
    /// Append-only daily file and its no-follow parent directory descriptor.
    File {
        directory: File,
        day: String,
        file: File,
    },
    /// Silent suppression when the home cannot host protected logs.
    Discard,
}

/// Process-wide configured operational logger; initialization never opens state.
static LOGGER: OnceLock<Mutex<Option<Arc<Logger>>>> = OnceLock::new();

/// Maximum bytes appended by all cooperating writers to one component's UTC day.
pub const MAX_DAILY_BYTES: u64 = 4 * 1024 * 1024;
/// Maximum complete operational record, including severity, component and newline.
pub const MAX_RECORD_BYTES: usize = 8 * 1024;

/// Attempts one finite nonblocking append under a descriptor-scoped advisory lock.
/// Busy locks, unsafe metadata and full files drop the whole record silently.
/// I/O failures may retain a bounded prefix; existing bytes are never truncated,
/// rotated or redirected to stderr.
fn append_bounded(file: &mut File, record: &[u8]) {
    if fs2::FileExt::try_lock_exclusive(file).is_err() {
        return;
    }
    if file.metadata().is_ok_and(|metadata| {
        metadata.is_file() && metadata.nlink() == 1
            // SAFETY: geteuid reads this process's effective uid without retaining state.
            && metadata.uid() == unsafe { libc::geteuid() }
            && metadata.permissions().mode() & 0o777 == 0o600
            && metadata.len().saturating_add(record.len() as u64) <= MAX_DAILY_BYTES
    }) {
        let _ = file.write_all(record);
    }
    let _ = fs2::FileExt::unlock(file);
}

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

/// Configures one idempotent component logger with protected UTC-daily files.
/// Unsafe/unavailable homes suppress operational records; setup never redirects
/// them to unbounded stderr. Records use the shared daily and per-record caps.
pub fn configure(home: &Path, component: &str) -> Arc<Logger> {
    let mut configured = slot().lock().expect("logger lock is not poisoned");
    if let Some(logger) = configured.as_ref() {
        return logger.clone();
    }
    let sink = file_sink(home, component, utc_day()).unwrap_or(Sink::Discard);
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

    /// Returns whether the current sink is a private daily file rather than suppression.
    pub fn uses_file(&self) -> bool {
        matches!(
            *self.sink.lock().expect("logger lock is not poisoned"),
            Sink::File { .. }
        )
    }

    /// Appends one record within the shared UTC-day and individual-record bounds.
    /// Busy, full, oversized or unsafe sinks silently drop the record; no stderr spill.
    pub fn log(&self, level: Level, message: &str) {
        self.log_on(level, message, &utc_day());
    }

    /// Applies record bounds and writes on an injected UTC day for deterministic tests.
    /// A failed rollover preserves the old file and suppresses this day's record.
    fn log_on(&self, level: Level, message: &str, day: &str) {
        if level < self.level {
            return;
        }
        if message.len() > MAX_RECORD_BYTES {
            return;
        }
        let line = format!("{level:?} {} {message}\n", self.component);
        if line.len() > MAX_RECORD_BYTES {
            return;
        }
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
                        Err(_) => return,
                    }
                }
                append_bounded(file, line.as_bytes());
            }
            Sink::Discard => {}
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
    /// Exact daily and record boundaries suppress whole records without overflow.
    #[test]
    fn cap_boundaries_preserve_existing_bytes() {
        let home = tempfile::tempdir().unwrap();
        let logger = writer(home.path(), "2026-10-05");
        let path = home.path().join("logs/cli.2026-10-05.log");
        let record = "Info cli boundary\n";
        std::fs::write(&path, vec![b'x'; MAX_DAILY_BYTES as usize - record.len()]).unwrap();
        logger.log_on(Level::Info, "boundary", "2026-10-05");
        assert_eq!(std::fs::metadata(&path).unwrap().len(), MAX_DAILY_BYTES);
        logger.log_on(Level::Info, "suppressed", "2026-10-05");
        assert_eq!(std::fs::metadata(&path).unwrap().len(), MAX_DAILY_BYTES);
        let next = writer(home.path(), "2026-10-06");
        next.log_on(Level::Info, &"x".repeat(MAX_RECORD_BYTES), "2026-10-06");
        assert_eq!(
            std::fs::metadata(home.path().join("logs/cli.2026-10-06.log"))
                .unwrap()
                .len(),
            0
        );
    }

    /// Independent descriptors coordinate one size-check/append across writers;
    /// contention may drop records, but retained records are whole and never overflow.
    #[test]
    fn concurrent_writers_share_one_daily_ceiling() {
        let home = tempfile::tempdir().unwrap();
        let loggers: Vec<_> = (0..8).map(|_| writer(home.path(), "2026-10-05")).collect();
        let path = home.path().join("logs/cli.2026-10-05.log");
        let record = b"Info cli concurrent\n";
        let prefix = MAX_DAILY_BYTES as usize - 100 * record.len();
        std::fs::write(&path, vec![b'x'; prefix]).unwrap();
        let barrier = Arc::new(std::sync::Barrier::new(loggers.len()));
        let workers: Vec<_> = loggers
            .into_iter()
            .map(|logger| {
                let barrier = barrier.clone();
                std::thread::spawn(move || {
                    barrier.wait();
                    for _ in 0..200 {
                        logger.log_on(Level::Info, "concurrent", "2026-10-05");
                    }
                })
            })
            .collect();
        for worker in workers {
            worker.join().unwrap();
        }
        let bytes = std::fs::read(&path).unwrap();
        assert!(bytes.len() > prefix && bytes.len() <= MAX_DAILY_BYTES as usize);
        assert_eq!((bytes.len() - prefix) % record.len(), 0);
        for line in bytes[prefix..].chunks(record.len()) {
            assert_eq!(line, record);
        }
    }

    /// A held lock or newly hardlinked open file is preserved without blocking,
    /// writing its bytes, changing links, or falling back to stderr.
    #[test]
    fn busy_and_unsafe_open_files_are_preserved() {
        let home = tempfile::tempdir().unwrap();
        let first = writer(home.path(), "2026-10-05");
        let second = writer(home.path(), "2026-10-05");
        let path = home.path().join("logs/cli.2026-10-05.log");
        let locked = OpenOptions::new().append(true).open(&path).unwrap();
        fs2::FileExt::lock_exclusive(&locked).unwrap();
        let started = std::time::Instant::now();
        second.log_on(Level::Info, "busy", "2026-10-05");
        assert!(started.elapsed() < std::time::Duration::from_secs(1));
        fs2::FileExt::unlock(&locked).unwrap();
        std::fs::write(&path, "preserved").unwrap();
        std::fs::hard_link(&path, home.path().join("foreign-alias")).unwrap();
        first.log_on(Level::Info, "unsafe", "2026-10-05");
        assert_eq!(std::fs::read_to_string(&path).unwrap(), "preserved");
        assert_eq!(std::fs::metadata(&path).unwrap().nlink(), 2);
        assert!(first.uses_file());
    }

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
