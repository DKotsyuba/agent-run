//! Service configuration validation without subprocesses or credentials.

use agent_run_config::provider_config::ProviderConfig;

/// Validates portable foreground declarations, strict bounds and historical snapshot compatibility.
#[test]
fn services_are_explicit_bounded_and_absent_from_old_snapshots() {
    let home = tempfile::tempdir().unwrap();
    let empty = ProviderConfig::parse("schema_version=2", home.path()).unwrap();
    assert!(serde_json::to_value(&empty)
        .unwrap()
        .get("services")
        .is_none());
    let document = r#"
schema_version=2
[services.index]
command="/bin/sleep"
args=["30"]
cwd="/tmp"
env_from=["INDEX_TOKEN"]
readiness={command="/bin/true"}
"#;
    let config = ProviderConfig::parse(document, home.path()).unwrap();
    assert_eq!(config.services["index"].idle_timeout_seconds, 1800);
    assert_eq!(config.services["index"].revision().unwrap().len(), 64);
    assert!(!config
        .snapshot()
        .unwrap()
        .to_string()
        .contains("INDEX_TOKEN"));
    for change in [
        document.replace("/bin/sleep", "sleep"),
        document.replace("[services.index]", "[services.'../index']"),
        document.replace("args=[\"30\"]", "args=[\"30\"]\nidle_timeout_seconds=0"),
        document.replace(
            "args=[\"30\"]",
            "args=[\"30\"]\nstartup_timeout_seconds=301",
        ),
        document.replace("INDEX_TOKEN", "AGENT_RUN_HOME"),
        document.replace("env_from=[\"INDEX_TOKEN\"]", "env={TOKEN='inline-secret'}"),
        document.replace("/bin/true", "true"),
    ] {
        assert!(
            ProviderConfig::parse(&change, home.path()).is_err(),
            "{change}"
        );
    }
}
