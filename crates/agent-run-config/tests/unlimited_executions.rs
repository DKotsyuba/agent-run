//! Unlimited execution configuration and byte-preserving historical projection.
use agent_run_config::{
    config::{Config, historical_config},
    provider_config::ProviderConfig,
};
use serde_json::json;

/// Loads one schema-1 home with the supplied core fragment; no runtime starts.
fn load(core: &str) -> Result<Config, agent_run_domain::Error> {
    let home = tempfile::tempdir().unwrap();
    std::fs::write(
        home.path().join("config.toml"),
        format!("schema_version = 1\n{core}\n"),
    )
    .unwrap();
    Config::load(home.path())
}

/// Parses a schema-2 config with the supplied core fragment; no files are mutated.
fn parse_v2(core: &str) -> Result<ProviderConfig, agent_run_domain::Error> {
    let home = tempfile::tempdir().unwrap();
    ProviderConfig::parse(&format!("schema_version = 2\n{core}\n"), home.path())
}

/// Both live schemas reject every retired lifetime key, even a formerly valid value.
#[test]
fn live_config_has_no_execution_lifetime_policy() {
    assert_eq!(
        serde_json::to_value(load("").unwrap().core).unwrap(),
        json!({"max_active_agents": 6})
    );
    assert_eq!(
        serde_json::to_value(parse_v2("").unwrap().core).unwrap(),
        json!({"max_active_agents": 6})
    );
    for entry in [
        "default_timeout_seconds = 480",
        "timeout_multiplier = 1.2",
        "warning_fraction = 0.9",
        "stalled_after_seconds = 900",
    ] {
        let section = format!("[core]\n{entry}\n");
        assert!(load(&section).is_err(), "{entry}");
        assert!(parse_v2(&section).is_err(), "{entry}");
    }
}

/// Original historical documents keep their digest; projection only removes retired
/// policy in memory and keeps concurrency, providers, and all remaining grants exact.
#[test]
fn historical_snapshot_is_verified_before_policy_projection() {
    let current = parse_v2("[core]\nmax_active_agents = 12").unwrap();
    let mut historical = serde_json::to_value(&current).unwrap();
    historical["core"] = json!({
        "default_timeout_seconds": 480.0, "timeout_multiplier": 2.5,
        "max_active_agents": 12, "warning_fraction": 0.9, "stalled_after_seconds": 900.0,
    });
    let original_bytes = serde_json::to_vec(&historical).unwrap();
    let frozen = ProviderConfig::snapshot_document(&historical).unwrap();
    assert_eq!(
        frozen["sha256"],
        json!(agent_run_domain::canonical::sha256_hex(&historical, true))
    );
    assert!(serde_json::from_value::<ProviderConfig>(historical.clone()).is_err());
    let projected = historical_config(historical.clone());
    let revived: ProviderConfig = serde_json::from_value(projected.clone()).unwrap();
    assert_eq!(serde_json::to_value(&revived).unwrap(), projected);
    assert_eq!(revived.core.max_active_agents, 12);
    assert_ne!(revived.snapshot().unwrap()["sha256"], frozen["sha256"]);
    assert_eq!(serde_json::to_vec(&historical).unwrap(), original_bytes);
    assert_eq!(historical_config(projected.clone()), projected);
}

/// Compatibility never filters unrelated unknown fields or invalid admission caps.
#[test]
fn historical_projection_keeps_strict_remaining_validation() {
    let config = serde_json::to_value(load("").unwrap()).unwrap();
    let mut unknown = config.clone();
    unknown["core"]["unknown_permission"] = json!(true);
    assert!(serde_json::from_value::<Config>(historical_config(unknown)).is_err());
    let mut invalid = config;
    invalid["core"]["max_active_agents"] = json!(0);
    let mut revived: Config = serde_json::from_value(historical_config(invalid)).unwrap();
    assert!(
        revived
            .validate(tempfile::tempdir().unwrap().path())
            .is_err()
    );
}
