//! `core.timeout_multiplier` validation for both configuration schemas.
//!
//! The multiplier must be finite and at least one in a schema-1 `Config` and a
//! schema-2 `ProviderConfig` alike (both share `Core` and its validation), and
//! the effective default allowance (`default_timeout_seconds` times the
//! multiplier) must stay inside the shared 2592000-second run-timeout bound so
//! a fresh admission can never overflow the deadline arithmetic.

use agent_run_config::{config::Config, provider_config::ProviderConfig};

/// Loads one schema-1 home whose `[core]` section is exactly `core`.
fn load(core: &str) -> Result<Config, agent_run_domain::Error> {
    let home = tempfile::tempdir().unwrap();
    std::fs::write(
        home.path().join("config.toml"),
        format!("schema_version = 1\n{core}\n"),
    )
    .unwrap();
    Config::load(home.path())
}

/// Parses one schema-2 document whose `[core]` section is exactly `core`.
fn parse_v2(core: &str) -> Result<ProviderConfig, agent_run_domain::Error> {
    let home = tempfile::tempdir().unwrap();
    ProviderConfig::parse(&format!("schema_version = 2\n{core}\n"), home.path())
}

/// Accepts the documented default 1.2 and any finite multiplier at or above
/// one, and rejects sub-one, zero, negative, NaN and infinite factors in both
/// schemas with the `core.timeout_multiplier` validation message.
#[test]
fn multiplier_must_be_finite_and_at_least_one() {
    let default = load("").unwrap();
    assert_eq!(default.core.timeout_multiplier, 1.2);
    for accepted in ["timeout_multiplier = 1.0", "timeout_multiplier = 2.5"] {
        assert!(
            load(&format!("[core]\n{accepted}")).is_ok(),
            "{accepted} must load"
        );
    }
    for rejected in [
        "timeout_multiplier = 0.9",
        "timeout_multiplier = 0",
        "timeout_multiplier = -1",
        "timeout_multiplier = nan",
        "timeout_multiplier = inf",
    ] {
        let error = load(&format!(
            "[core]\ndefault_timeout_seconds = 480\n{rejected}"
        ))
        .expect_err(rejected);
        assert!(
            error.to_string().contains("core.timeout_multiplier"),
            "{rejected}: {error}"
        );
        let v2 = parse_v2(&format!(
            "[core]\ndefault_timeout_seconds = 480\n{rejected}"
        ))
        .expect_err(rejected);
        assert!(
            v2.to_string().contains("core.timeout_multiplier"),
            "schema 2 {rejected}: {v2}"
        );
    }
}

/// A multiplier whose default allowance exceeds the run-timeout ceiling is
/// rejected at load time, so no default admission can overflow the bound.
#[test]
fn effective_default_allowance_must_stay_bounded() {
    let error = load("[core]\ndefault_timeout_seconds = 2500000\n")
        .expect_err("2500000 times 1.2 exceeds the ceiling");
    assert!(
        error
            .to_string()
            .contains("core.default_timeout_seconds times core.timeout_multiplier"),
        "{error}"
    );
    let v2 = parse_v2("[core]\ndefault_timeout_seconds = 2500000\n")
        .expect_err("schema 2 shares the same bound");
    assert!(
        v2.to_string()
            .contains("core.default_timeout_seconds times core.timeout_multiplier"),
        "schema 2: {v2}"
    );
}

/// The one admission-side helper every start path shares: an explicit request
/// and the configured default are each multiplied exactly once, and a product
/// outside the shared run-timeout contract is a typed validation error.
#[test]
fn effective_timeout_seconds_multiplies_exactly_once() {
    let core = load("[core]\ndefault_timeout_seconds = 600\n")
        .unwrap()
        .core;
    assert_eq!(core.effective_timeout_seconds(None).unwrap(), 720.0);
    assert_eq!(core.effective_timeout_seconds(Some(600.0)).unwrap(), 720.0);
    let disabled = load("[core]\ndefault_timeout_seconds = 600\ntimeout_multiplier = 1.0\n")
        .unwrap()
        .core;
    assert_eq!(disabled.effective_timeout_seconds(None).unwrap(), 600.0);
    assert_eq!(
        disabled.effective_timeout_seconds(Some(600.0)).unwrap(),
        600.0
    );
    let overflow = core.effective_timeout_seconds(Some(2500000.0)).unwrap_err();
    assert!(
        overflow.to_string().contains("core.timeout_multiplier"),
        "{overflow}"
    );
}
