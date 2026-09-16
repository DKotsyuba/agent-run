//! Regression coverage for Python-compatible child-environment inheritance.

use agent_run_adapters::materialize::{apply_environment_overrides, inherited_environment};
use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;

/// Ordinary host configuration remains available in the eventual child environment.
#[test]
fn ordinary_host_variable_reaches_child_environment() {
    let host = BTreeMap::from([("HTTP_PROXY".into(), "http://proxy.test".into())]);
    let environment = inherited_environment(&host, &BTreeSet::new());
    assert_eq!(
        environment.get("HTTP_PROXY"),
        Some(&"http://proxy.test".into())
    );
}

/// Credential-shaped host names are absent without an explicit launch allow-list entry.
#[test]
fn secret_shaped_host_variable_is_dropped() {
    let host = BTreeMap::from([("SERVICE_TOKEN".into(), "do-not-forward".into())]);
    assert!(!inherited_environment(&host, &BTreeSet::new()).contains_key("SERVICE_TOKEN"));
}

/// Declared auth or MCP names are the only credential-shaped values retained.
#[test]
fn explicitly_allowed_secret_reaches_child_environment() {
    let host = BTreeMap::from([("SERVICE_TOKEN".into(), "selected".into())]);
    let environment = inherited_environment(&host, &BTreeSet::from(["SERVICE_TOKEN".into()]));
    assert_eq!(environment.get("SERVICE_TOKEN"), Some(&"selected".into()));
}

/// Runtime configuration is applied after inheritance and therefore has final precedence.
#[test]
fn runtime_overrides_win_over_host_environment() {
    let host = BTreeMap::from([("LANG".into(), "host".into())]);
    let mut environment = inherited_environment(&host, &BTreeSet::new());
    apply_environment_overrides(
        &mut environment,
        BTreeMap::from([("LANG".into(), "runtime".into())]),
    );
    assert_eq!(environment.get("LANG"), Some(&"runtime".into()));
}

/// Mirrors `tests/test_adapter_environment.py::HostEnvironmentTests::test_derives_only_existing_rust_homes_before_isolating_home`
#[test]
fn derives_only_existing_rust_homes_before_isolating_home() {
    let root = tempfile::tempdir().expect("temporary home");
    let host_home = root.path().join("host");
    std::fs::create_dir_all(host_home.join(".rustup")).expect("rustup home");
    std::fs::create_dir_all(host_home.join(".cargo")).expect("cargo home");
    let host = BTreeMap::from([("HOME".into(), host_home.display().to_string())]);
    let environment = inherited_environment(&host, &BTreeSet::new());
    assert_eq!(
        environment["RUSTUP_HOME"],
        host_home.join(".rustup").display().to_string()
    );
    assert_eq!(
        environment["CARGO_HOME"],
        host_home.join(".cargo").display().to_string()
    );

    std::fs::remove_dir_all(host_home.join(".rustup")).expect("remove rustup home");
    std::fs::remove_dir_all(host_home.join(".cargo")).expect("remove cargo home");
    let environment = inherited_environment(&host, &BTreeSet::new());
    assert!(!environment.contains_key("RUSTUP_HOME"));
    assert!(!environment.contains_key("CARGO_HOME"));
    assert!(!Path::new(&environment["HOME"]).join(".rustup").exists());
}

/// Mirrors `tests/test_adapter_environment.py::HostEnvironmentTests::test_inherits_host_tools_and_only_selected_secrets`
#[test]
fn inherits_host_tools_and_only_selected_secrets() {
    let host = BTreeMap::from([
        ("PATH".into(), "/host/bin".into()),
        ("LANG".into(), "en_US.UTF-8".into()),
        ("RUSTUP_HOME".into(), "/host/rustup".into()),
        ("UNRELATED_TOKEN".into(), "drop-me".into()),
        ("SELECTED_API_KEY".into(), "keep-me".into()),
        ("SSH_AUTH_SOCK".into(), "/tmp/agent.sock".into()),
        ("CARGO_HOME".into(), "/host/cargo".into()),
        ("SDKROOT".into(), "/host/sdk".into()),
        ("PROJECT_BUILD_MODE".into(), "release".into()),
        ("HOME".into(), "/host/home".into()),
    ]);
    let environment = inherited_environment(&host, &BTreeSet::from(["SELECTED_API_KEY".into()]));
    assert_eq!(environment["PATH"], "/host/bin");
    assert_eq!(environment["RUSTUP_HOME"], "/host/rustup");
    assert_eq!(environment["SDKROOT"], "/host/sdk");
    assert_eq!(environment["CARGO_HOME"], "/host/cargo");
    assert_eq!(environment["PROJECT_BUILD_MODE"], "release");
    assert_eq!(environment["SELECTED_API_KEY"], "keep-me");
    for name in ["UNRELATED_TOKEN", "SSH_AUTH_SOCK"] {
        assert!(
            !environment.contains_key(name),
            "unexpected inherited {name}"
        );
    }
}

/// Mirrors `tests/test_adapter_environment.py::HostEnvironmentTests::test_selected_credential_carrier_is_forwarded`
#[test]
fn selected_credential_carrier_is_forwarded() {
    let host = BTreeMap::from([("KRB5CCNAME".into(), "FILE:/tmp/selected".into())]);
    let environment = inherited_environment(&host, &BTreeSet::from(["KRB5CCNAME".into()]));
    assert_eq!(environment["KRB5CCNAME"], "FILE:/tmp/selected");
}
