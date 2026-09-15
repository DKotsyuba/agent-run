//! Regression coverage for Python-compatible child-environment inheritance.

use agent_run_adapters::materialize::{apply_environment_overrides, inherited_environment};
use std::collections::{BTreeMap, BTreeSet};

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
