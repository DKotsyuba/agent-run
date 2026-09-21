//! Dense process-wide logging with a private file and stderr fallback.

use std::{
    fs::{create_dir_all, File, OpenOptions},
    io::Write,
    os::unix::fs::PermissionsExt,
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

/// The two safe destinations selected during configuration.
enum Sink {
    /// Append-only component log file.
    File(File),
    /// Process stderr when the home cannot host logs.
    Stderr,
}

static LOGGER: OnceLock<Mutex<Option<Arc<Logger>>>> = OnceLock::new();

/// Returns the process-wide logger slot without opening files.
fn slot() -> &'static Mutex<Option<Arc<Logger>>> {
    LOGGER.get_or_init(|| Mutex::new(None))
}

/// Configures one idempotent dense component logger for an agent-run home.
pub fn configure(home: &Path, component: &str) -> Arc<Logger> {
    let mut configured = slot().lock().expect("logger lock is not poisoned");
    if let Some(logger) = configured.as_ref() {
        return logger.clone();
    }
    let log_dir = home.join("logs");
    let sink = create_dir_all(&log_dir)
        .and_then(|()| std::fs::set_permissions(&log_dir, std::fs::Permissions::from_mode(0o700)))
        .and_then(|_| {
            OpenOptions::new()
                .create(true)
                .append(true)
                .open(log_dir.join(format!("{component}.log")))
        })
        .map(Sink::File)
        .unwrap_or(Sink::Stderr);
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

    /// Returns whether setup opened the component file rather than stderr.
    pub fn uses_file(&self) -> bool {
        matches!(
            *self.sink.lock().expect("logger lock is not poisoned"),
            Sink::File(_)
        )
    }

    /// Writes one bounded lifecycle line without accepting arbitrary task or secret text.
    pub fn log(&self, level: Level, message: &str) {
        if level < self.level {
            return;
        }
        let line = format!("{level:?} {} {message}\n", self.component);
        let mut sink = self.sink.lock().expect("logger lock is not poisoned");
        match &mut *sink {
            Sink::File(file) => {
                let _ = file.write_all(line.as_bytes());
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
