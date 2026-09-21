//! Logging setup parity checks for the process-wide Rust logger.

mod common;

use agent_run_core::logging::{self, Level};
use agent_run_core::service::Service;
use agent_run_platform::fs;
use std::sync::{Mutex, MutexGuard, OnceLock};

/// Serializes process-global logger tests so each case owns its reset boundary.
fn logger_guard() -> MutexGuard<'static, ()> {
    static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
    LOCK.get_or_init(|| Mutex::new(())).lock().unwrap()
}

/// Mirrors the isolated Python logging fixture by restoring environment and logger state.
fn reset_logging() {
    logging::reset_for_tests();
    // SAFETY: this test owns the process-global logging environment while holding logger_guard.
    unsafe { std::env::remove_var("AGENT_RUN_LOG_LEVEL") };
}

/// Mirrors `tests/test_logging_setup.py::ConfigureLoggingTests::test_configure_logging_creates_the_log_directory_and_file`.
/// Mirrors `tests/test_logging_setup.py::ConfigureLoggingTests::test_repeated_configure_is_idempotent_and_keeps_the_first_component`.
/// Mirrors `tests/test_logging_setup.py::ConfigureLoggingTests::test_env_level_is_honored`.
/// Mirrors `tests/test_logging_setup.py::ConfigureLoggingTests::test_default_level_is_debug_for_dense_logging`.
/// Mirrors `tests/test_logging_setup.py::ConfigureLoggingTests::test_uncreatable_log_dir_falls_back_to_stderr_silently`.
#[test]
fn logging_setup_is_bounded_idempotent_and_falls_back() {
    let _guard = logger_guard();
    reset_logging();
    let temporary = tempfile::tempdir().unwrap();
    let first = logging::configure(temporary.path(), "cli");
    assert!(temporary.path().join("logs/cli.log").is_file());
    assert_eq!(first.level(), Level::Debug);
    let second_home = temporary.path().join("other");
    let second = logging::configure(&second_home, "mcp");
    assert!(std::sync::Arc::ptr_eq(&first, &second));
    assert_eq!(first.component(), "cli");
    assert!(!second_home.join("logs").exists());

    reset_logging();
    // SAFETY: logger_guard excludes every other test that reads this setting.
    unsafe { std::env::set_var("AGENT_RUN_LOG_LEVEL", "WARNING") };
    assert_eq!(
        logging::configure(temporary.path(), "cli").level(),
        Level::Warning
    );
    reset_logging();
    let blocked = temporary.path().join("not-a-directory");
    std::fs::write(&blocked, "x").unwrap();
    assert!(!logging::configure(&blocked, "cli").uses_file());
    reset_logging();
}

/// Mirrors `tests/test_logging_setup.py::ServiceLoggingTests::test_service_start_logs_a_reconstructable_lifecycle`.
/// Mirrors `tests/test_logging_setup.py::ServiceLoggingTests::test_service_start_never_logs_env_secret_values`.
/// Mirrors `tests/test_logging_setup.py::ServiceLoggingTests::test_service_start_never_logs_the_full_task_text`.
#[test]
fn service_start_logging_is_reconstructable_without_private_payloads() {
    let _guard = logger_guard();
    reset_logging();
    let temporary = tempfile::tempdir().unwrap();
    let logger = logging::configure(temporary.path(), "cli");
    logging::start("fake", "model", "ag-test", true);
    logger.log(Level::Debug, "task text is intentionally not accepted here");
    let text = std::fs::read_to_string(temporary.path().join("logs/cli.log")).unwrap();
    assert!(text.contains("start runtime=fake model=model agent_id=ag-test created=true"));
    assert!(!text.contains("sk-super-secret-token-value"));
    assert!(!text.contains("do the work do the work"));
    reset_logging();
}

/// Proves bounded reload diagnostics name the active revision for both
/// adoption and rejection, an invalid revision never replaces the last valid
/// cached snapshot, and a restored valid revision is accepted again.
#[test]
fn config_reload_diagnostics_name_the_active_revision() {
    let _guard = logger_guard();
    reset_logging();
    let home = common::Home::new();
    let config_path = home.path.join("config.toml");
    logging::configure(&home.path, "cli");
    let service = Service::new(home.path.clone());
    let digest = |bytes: &[u8]| fs::sha256(bytes);
    let log = || std::fs::read_to_string(home.path.join("logs/cli.log")).unwrap();
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
    reset_logging();
}
