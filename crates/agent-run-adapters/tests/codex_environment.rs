//! Python-parity tests for Codex child-environment inheritance and bridges.
//!
//! Python builds every adapter-owned Codex child environment with
//! `build_environment` (`src/agent_run/adapters/codex/environment.py`): the host
//! environment is inherited through the credential filter, only `HOME` and
//! `CODEX_HOME` are replaced, and `PATH` is passed through exactly as inherited.
//! The Rust counterpart is `materialize::environment_with_host`, which the agent
//! launch and the metadata probes both route through.

use agent_run_adapters::materialize::{self, Publisher};
use agent_run_config::{
    config::{Capacity, Catalog, Config, Core, Delivery, Runtime},
    profiles::Profile,
};
use serde_json::json;
use std::{
    collections::{BTreeMap, BTreeSet},
    path::{Path, PathBuf},
};

/// Builds a minimal enabled Codex runtime pointing at `binary` and `home`.
fn runtime(binary: &str, home: &str) -> Runtime {
    serde_json::from_value(json!({
        "enabled": true,
        "adapter": "codex",
        "binary": binary,
        "home": home,
        "models": ["model-a"],
    }))
    .expect("fixture runtime")
}

/// Builds the smallest valid configuration with no presets or MCP servers.
fn config() -> Config {
    Config {
        schema_version: 1,
        core: Core::default(),
        capacity: Capacity::default(),
        delivery: Delivery::default(),
        profiles: Catalog::default(),
        skills: Catalog::default(),
        mcp: BTreeMap::new(),
        environments: BTreeMap::new(),
        runtimes: BTreeMap::new(),
    }
}

/// Builds a read-only role that declares no skills, MCP servers, or roots.
fn profile() -> Profile {
    Profile {
        name: "review".into(),
        body: "fixture role".into(),
        write: false,
        network: false,
        revision: "fixture".into(),
        canonical: false,
        allow_external_read_roots: true,
        read_roots: vec![],
        skills: vec![],
        mcp: vec![],
        required_constraints: BTreeSet::new(),
    }
}

/// Resolves one Codex child environment from an explicit host snapshot.
fn environment(
    binary: &str,
    home: &str,
    host: BTreeMap<String, String>,
) -> agent_run_domain::Result<BTreeMap<String, String>> {
    materialize::environment_with_host(
        &config(),
        &runtime(binary, home),
        &profile(),
        Path::new(home),
        None,
        Path::new("/tmp/agent-run-fixture-app-home"),
        &host,
    )
}

/// Mirrors `tests/test_codex_environment.py::BuildEnvironmentTests::test_path_keeps_the_inherited_order`
#[test]
fn path_keeps_the_inherited_order() {
    let host = BTreeMap::from([
        ("HOME".into(), "/host/home".into()),
        ("PATH".into(), "/usr/local/bin:/usr/bin:/bin".into()),
    ]);
    let environment = environment("/opt/tools/bin/codex", "/tmp/home", host).expect("environment");
    assert_eq!(environment["PATH"], "/usr/local/bin:/usr/bin:/bin");
}

/// Mirrors `tests/test_codex_environment.py::BuildEnvironmentTests::test_repeated_path_entries_remain_unchanged`
#[test]
fn repeated_path_entries_remain_unchanged() {
    let host = BTreeMap::from([
        ("HOME".into(), "/host/home".into()),
        (
            "PATH".into(),
            "/opt/tools/bin:/usr/bin:/opt/tools/bin:/bin:/usr/bin".into(),
        ),
    ]);
    let environment = environment("/opt/tools/bin/codex", "/tmp/home", host).expect("environment");
    assert_eq!(
        environment["PATH"],
        "/opt/tools/bin:/usr/bin:/opt/tools/bin:/bin:/usr/bin"
    );
}

/// Mirrors `tests/test_codex_environment.py::BuildEnvironmentTests::test_missing_or_empty_path_is_not_invented`
#[test]
fn missing_or_empty_path_is_not_invented() {
    let absent = BTreeMap::from([("HOME".into(), "/host/home".into())]);
    let resolved = environment("/opt/tools/bin/codex", "/tmp/home", absent).expect("environment");
    assert!(
        !resolved.contains_key("PATH"),
        "an absent host PATH must stay absent, not become an empty entry"
    );

    let blank = BTreeMap::from([
        ("HOME".into(), "/host/home".into()),
        ("PATH".into(), String::new()),
    ]);
    let resolved = environment("/opt/tools/bin/codex", "/tmp/home", blank).expect("environment");
    assert_eq!(resolved["PATH"], "");
}

/// Mirrors `tests/test_codex_environment.py::BuildEnvironmentTests::test_relative_binary_is_rejected_instead_of_searching_the_workdir`
#[test]
fn relative_binary_is_rejected_instead_of_searching_the_workdir() {
    let host = BTreeMap::from([
        ("HOME".into(), "/host/home".into()),
        ("PATH".into(), "/usr/bin:/bin".into()),
    ]);
    assert!(
        environment("codex", "/tmp/home", host).is_err(),
        "a relative executable must fail loudly rather than gain a PATH hole"
    );
}

/// Mirrors `tests/test_codex_environment.py::BuildEnvironmentTests::test_home_and_codex_home_track_only_the_supplied_home`
/// Mirrors `tests/test_codex_environment.py::RealSubprocessRateLimitsTests::test_probe_resolves_the_bundled_interpreter_for_base_and_account_homes`
#[test]
fn home_and_codex_home_track_only_the_supplied_home() {
    let host = BTreeMap::from([
        ("HOME".into(), "/host/home".into()),
        ("PATH".into(), "/host/bin".into()),
    ]);
    let base = environment("/opt/bin/codex", "/tmp/base", host.clone()).expect("base environment");
    let account = environment("/opt/bin/codex", "/tmp/plus", host).expect("account environment");

    assert_eq!(base["HOME"], "/tmp/base");
    assert_eq!(base["CODEX_HOME"], "/tmp/base");
    assert_eq!(account["HOME"], "/tmp/plus");
    assert_eq!(account["CODEX_HOME"], "/tmp/plus");
    assert_eq!(
        base.keys().cloned().collect::<BTreeSet<String>>(),
        BTreeSet::from(["HOME".into(), "CODEX_HOME".into(), "PATH".into()]),
        "nothing beyond the two homes and the inherited PATH reaches the child"
    );
    // The probe's own launcher directory is resolvable because the inherited
    // PATH is passed through unchanged, and it carries no empty entry.
    assert!(!base["PATH"].split(':').any(str::is_empty));
}

/// Mirrors `tests/test_codex_environment.py::BuildEnvironmentTests::test_uv_install_root_honors_the_explicit_parent_setting`
#[test]
fn uv_install_root_honors_the_explicit_parent_setting() {
    // Replacing the child's HOME must not hide an explicitly configured uv
    // managed-install root; it is an ordinary host value and survives verbatim.
    let install = "/opt/uv/python";
    let host = BTreeMap::from([
        ("HOME".into(), "/host/home".into()),
        ("PATH".into(), "/usr/bin".into()),
        ("UV_PYTHON_INSTALL_DIR".into(), install.into()),
    ]);
    let environment = environment("/opt/bin/codex", "/tmp/home", host).expect("environment");
    assert_eq!(environment["UV_PYTHON_INSTALL_DIR"], install);
}

/// Mirrors `tests/test_adapter_home.py::AdapterHomeTests::test_symlink_bridges_are_explicit_and_validated`
#[test]
fn symlink_bridges_are_explicit_and_validated() {
    let root = tempfile::tempdir().expect("temporary root");
    let source = root.path().join("auth.json");
    std::fs::write(&source, "{}").expect("credential source");
    let home = root.path().join("generated");
    let mut publisher = Publisher::new(&home).expect("generated home");

    publisher.link("auth/auth.json", &source).expect("bridge");
    let bridge = home.join("auth/auth.json");
    assert!(std::fs::symlink_metadata(&bridge)
        .expect("bridge metadata")
        .file_type()
        .is_symlink());
    assert_eq!(
        std::fs::canonicalize(&bridge).expect("bridge target"),
        std::fs::canonicalize(&source).expect("source")
    );

    assert!(
        publisher.link("../auth.json", &source).is_err(),
        "a bridge must stay beneath the generated home"
    );
    assert!(
        publisher
            .link("auth/other.json", &PathBuf::from("relative"))
            .is_err(),
        "a bridge source must be an absolute existing path"
    );
}
