//! Logging setup parity checks for the process-wide Rust logger.

mod common;

use agent_run_core::logging::{self, Level};
use agent_run_core::service::Service;
use agent_run_platform::fs;
use std::path::{Path, PathBuf};

/// Lists direct UTC-daily component files, excluding legacy component.log.
fn daily_files(home: &Path, component: &str) -> Vec<PathBuf> {
    let prefix = format!("{component}.");
    let mut files: Vec<_> = std::fs::read_dir(home.join("logs"))
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .filter(|path| {
            path.file_name().is_some_and(|name| {
                let name = name.to_string_lossy();
                name.starts_with(&prefix) && name.ends_with(".log")
            })
        })
        .collect();
    files.sort();
    files
}

/// Concatenates this component's daily files for assertions independent of midnight.
fn daily_text(home: &Path, component: &str) -> String {
    daily_files(home, component)
        .iter()
        .map(|path| std::fs::read_to_string(path).unwrap())
        .collect()
}

/// Owns one isolated logger test child; panic and timeout cleanup kills and
/// reaps only this owned child, which never launches an engine.
struct LoggerChild(std::process::Child);

impl LoggerChild {
    /// Requires a successful exit within 60 seconds; failures unwind through
    /// the owned child's kill-and-reap guard.
    fn finish(mut self) {
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(60);
        loop {
            if let Some(status) = self.0.try_wait().expect("logger child must be waitable") {
                assert!(status.success(), "isolated logger case failed: {status}");
                return;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "logger case timed out"
            );
            std::thread::sleep(std::time::Duration::from_millis(20));
        }
    }
}

impl Drop for LoggerChild {
    /// Kills and reaps the exact child on every exit, including parent panic.
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

/// Runs exactly `test` in a bounded child with the requested logger level,
/// removing an inherited level when `level` is absent. Returns true in the
/// parent after success and false in the marked child so it executes assertions.
/// Environment values are supplied at spawn; neither process mutates its environment.
fn isolated_logging(test: &str, level: Option<&str>) -> bool {
    if std::env::var("AGENT_RUN_TEST_LOGGING_CASE").as_deref() == Ok(test) {
        return false;
    }
    let mut command = std::process::Command::new(std::env::current_exe().unwrap());
    command
        .args(["--exact", test, "--nocapture"])
        .env("AGENT_RUN_TEST_LOGGING_CASE", test)
        .env_remove("AGENT_RUN_LOG_LEVEL");
    if let Some(level) = level {
        command.env("AGENT_RUN_LOG_LEVEL", level);
    }
    LoggerChild(command.spawn().expect("logger child must start")).finish();
    true
}

/// Checks default/idempotent/fallback setup and warning-level setup in separate
/// bounded children, retaining fixture files only for each child's assertions.
/// Mirrors `tests/test_logging_setup.py::ConfigureLoggingTests::test_configure_logging_creates_the_log_directory_and_file`.
/// Mirrors `tests/test_logging_setup.py::ConfigureLoggingTests::test_repeated_configure_is_idempotent_and_keeps_the_first_component`.
/// Mirrors `tests/test_logging_setup.py::ConfigureLoggingTests::test_env_level_is_honored`.
/// Mirrors `tests/test_logging_setup.py::ConfigureLoggingTests::test_default_level_is_debug_for_dense_logging`.
/// Mirrors `tests/test_logging_setup.py::ConfigureLoggingTests::test_uncreatable_log_dir_falls_back_to_stderr_silently`.
#[test]
fn logging_setup_is_bounded_idempotent_and_falls_back() {
    let test = "logging_setup_is_bounded_idempotent_and_falls_back";
    if isolated_logging(test, None) {
        assert!(isolated_logging(test, Some("WARNING")));
        return;
    }
    let temporary = tempfile::tempdir().unwrap();
    if std::env::var("AGENT_RUN_LOG_LEVEL").as_deref() == Ok("WARNING") {
        assert_eq!(
            logging::configure(temporary.path(), "cli").level(),
            Level::Warning
        );
        return;
    }
    let first = logging::configure(temporary.path(), "cli");
    assert_eq!(daily_files(temporary.path(), "cli").len(), 1);
    assert_eq!(first.level(), Level::Debug);
    let second_home = temporary.path().join("other");
    let second = logging::configure(&second_home, "mcp");
    assert!(std::sync::Arc::ptr_eq(&first, &second));
    assert_eq!(first.component(), "cli");
    assert!(!second_home.join("logs").exists());

    logging::reset_for_tests();
    let blocked = temporary.path().join("not-a-directory");
    std::fs::write(&blocked, "x").unwrap();
    assert!(!logging::configure(&blocked, "cli").uses_file());
}

/// Checks lifecycle log evidence in a bounded child with the default level,
/// asserting that its temporary log contains no task text or credential value.
/// Mirrors `tests/test_logging_setup.py::ServiceLoggingTests::test_service_start_logs_a_reconstructable_lifecycle`.
/// Mirrors `tests/test_logging_setup.py::ServiceLoggingTests::test_service_start_never_logs_env_secret_values`.
/// Mirrors `tests/test_logging_setup.py::ServiceLoggingTests::test_service_start_never_logs_the_full_task_text`.
#[test]
fn service_start_logging_is_reconstructable_without_private_payloads() {
    if isolated_logging(
        "service_start_logging_is_reconstructable_without_private_payloads",
        None,
    ) {
        return;
    }
    let temporary = tempfile::tempdir().unwrap();
    let logger = logging::configure(temporary.path(), "cli");
    logging::start("fake", "model", "ag-test", true);
    logger.log(Level::Debug, "task text is intentionally not accepted here");
    let text = daily_text(temporary.path(), "cli");
    assert!(text.contains("start runtime=fake model=model agent_id=ag-test created=true"));
    assert!(!text.contains("sk-super-secret-token-value"));
    assert!(!text.contains("do the work do the work"));
}

/// Proves bounded reload diagnostics name the active revision for both
/// adoption and rejection, an invalid revision never replaces the last valid
/// cached snapshot, and a restored valid revision is accepted again. Runs in a
/// bounded default-level child so logger state never races another case.
#[test]
fn config_reload_diagnostics_name_the_active_revision() {
    if isolated_logging("config_reload_diagnostics_name_the_active_revision", None) {
        return;
    }
    let home = common::Home::new();
    let config_path = home.path.join("config.toml");
    logging::configure(&home.path, "cli");
    let service = Service::new(home.path.clone());
    let digest = |bytes: &[u8]| fs::sha256(bytes);
    let log = || daily_text(&home.path, "cli");
    // The newest line after an adoption names the revision that is now active.
    let newest_is = |line: String| assert!(log().lines().last().unwrap().ends_with(&line));

    let original = std::fs::read(&config_path).unwrap();
    assert!(service.refresh_config().unwrap());
    newest_is(format!(
        "config reload accepted revision={}",
        digest(&original)
    ));
    let changed = format!(
        "{}\n[delivery]\nretry_base_seconds = 3.0\n",
        String::from_utf8_lossy(&original)
    );
    std::fs::write(&config_path, &changed).unwrap();
    assert!(service.refresh_config().unwrap());
    newest_is(format!(
        "config reload accepted revision={}",
        digest(changed.as_bytes())
    ));

    let invalid = "not valid TOML = [";
    std::fs::write(&config_path, invalid).unwrap();
    assert!(service.refresh_config().is_err());
    newest_is(format!(
        "config reload rejected revision={}",
        digest(changed.as_bytes())
    ));
    // Restoring the last valid bytes is a digest hit only when the invalid
    // revision never replaced the cached snapshot.
    std::fs::write(&config_path, &changed).unwrap();
    assert!(!service.refresh_config().unwrap());
    std::fs::write(&config_path, &original).unwrap();
    assert!(service.refresh_config().unwrap());

    let text = log();
    assert!(text.contains(&format!(
        "config reload accepted revision={}",
        digest(&original)
    )));
    assert!(text.contains(&format!(
        "config reload accepted revision={}",
        digest(changed.as_bytes())
    )));
    assert!(text.contains(&format!(
        "config reload rejected revision={}",
        digest(changed.as_bytes())
    )));
    assert!(!text.contains(invalid), "config contents are never logged");
    assert!(
        !text.contains("config.toml"),
        "configuration paths are never logged"
    );
}

/// Proves invalid configuration remains visible at the warning log threshold
/// in a bounded child whose level is supplied only through its spawn environment.
#[test]
fn config_reload_rejection_survives_warning_threshold() {
    if isolated_logging(
        "config_reload_rejection_survives_warning_threshold",
        Some("WARNING"),
    ) {
        return;
    }
    let home = common::Home::new();
    let config_path = home.path.join("config.toml");
    let original = std::fs::read(&config_path).unwrap();
    logging::configure(&home.path, "cli");
    let service = Service::new(home.path.clone());
    assert!(service.refresh_config().unwrap());

    let invalid = "not valid TOML = [";
    std::fs::write(&config_path, invalid).unwrap();
    assert!(service.refresh_config().is_err());
    let text = daily_text(&home.path, "cli");
    assert!(text.contains(&format!(
        "Warning cli config reload rejected revision={}",
        fs::sha256(&original)
    )));
    assert!(!text.contains(invalid));
}
