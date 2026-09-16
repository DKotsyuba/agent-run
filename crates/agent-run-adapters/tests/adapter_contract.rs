//! Python-parity tests for the adapter contract and runtime selection.
//!
//! Python resolves adapters dynamically: `load_adapter` imports a module path,
//! checks its declared API version and capability set, and `AdapterRegistry`
//! refuses unknown or disabled runtimes (`src/agent_run/adapters/registry.py`).
//! Rust has no dynamic import: `Adapter::parse` maps a configured name onto a
//! closed enum, `capabilities` reports the same closed capability set, and
//! `Config::runtime` performs the registry refusals. The import-time behaviors
//! that only describe Python's module loader are reported as divergences.

use agent_run_adapters::{capabilities, validate};
use agent_run_config::{
    config::{Adapter, Capacity, Catalog, Config, Core, Delivery, Runtime},
    profiles::Profile,
};
use agent_run_domain::domain::StartRequest;
use serde_json::json;
use std::{collections::BTreeMap, path::Path};

/// Builds a configuration whose single runtime carries `enabled` and `adapter`.
fn config(name: &str, adapter: &str, enabled: bool) -> Config {
    let runtime: Runtime = serde_json::from_value(json!({
        "enabled": enabled,
        "adapter": adapter,
        "binary": "/bin/echo",
        "home": "/tmp/agent-run-fixture-home",
        "models": ["model-a"],
    }))
    .expect("fixture runtime");
    Config {
        schema_version: 1,
        core: Core::default(),
        capacity: Capacity::default(),
        delivery: Delivery::default(),
        profiles: Catalog::default(),
        skills: Catalog::default(),
        mcp: BTreeMap::new(),
        environments: BTreeMap::new(),
        runtimes: BTreeMap::from([(name.to_owned(), runtime)]),
    }
}

/// Mirrors `tests/test_adapters_base.py::AdapterTests::test_registry_refuses_unknown_and_disabled_runtimes`
#[test]
fn registry_refuses_unknown_and_disabled_runtimes() {
    let disabled = config("fake", "codex", false);
    assert!(
        disabled.runtime("fake").is_err(),
        "a disabled runtime must not be loadable"
    );
    assert!(
        disabled.runtime("missing").is_err(),
        "an unconfigured runtime must not be loadable"
    );
    assert!(config("fake", "codex", true).runtime("fake").is_ok());
}

/// Mirrors `tests/test_adapters_base.py::AdapterTests::test_loader_checks_api_members_and_capabilities_before_runtime_use`
#[test]
fn adapter_selection_checks_identity_and_capabilities_before_runtime_use() {
    // Python rejects a module that is not a conforming adapter; Rust rejects a
    // configured name that is not one of the closed adapter kinds.
    assert!(Adapter::parse("fake_module:ADAPTER").is_err());
    assert!(Adapter::parse("codex").is_ok());
    assert!(
        config("fake", "fake_module:ADAPTER", true)
            .runtime("fake")
            .expect("enabled runtime")
            .kind()
            .is_err(),
        "a configured name outside the closed adapter set must not resolve"
    );

    // A capability the selected adapter does not advertise is refused before
    // the runtime is used, exactly like Python's required-capability check.
    assert!(!capabilities(Adapter::Codex).contains(&"output_schema"));
    assert!(capabilities(Adapter::Claude).contains(&"output_schema"));
    let configuration = config("codex", "codex", true);
    let runtime = configuration.runtime("codex").expect("enabled runtime");
    let profile = Profile {
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
        required_constraints: Default::default(),
    };
    let request: StartRequest = serde_json::from_value(json!({
        "runtime": "codex",
        "model": "model-a",
        "profile": "review",
        "task": "fixture",
        "workdir": "/tmp",
        "output_schema": {"type": "object"},
    }))
    .expect("fixture request");
    assert!(
        validate(&request, runtime, &profile).is_err(),
        "an unadvertised capability must be refused before launch"
    );
}

/// Mirrors `tests/test_adapters_base.py::AdapterTests::test_adapter_family_sources_stay_below_hard_gate`
#[test]
fn adapter_family_sources_stay_below_hard_gate() {
    // One 700-line ceiling per adapter-family source file, enforced rather than
    // remembered, so splitting a family stays cheap while the split is small.
    let source = Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
    let mut checked = Vec::new();
    let mut families = vec![
        source.join("claude.rs"),
        source.join("codex.rs"),
        source.join("glm.rs"),
    ];
    for entry in std::fs::read_dir(source.join("codex")).expect("codex family directory") {
        let path = entry.expect("family entry").path();
        if path.extension().is_some_and(|extension| extension == "rs") {
            families.push(path);
        }
    }
    for path in families {
        let text = std::fs::read_to_string(&path)
            .unwrap_or_else(|error| panic!("adapter source {}: {error}", path.display()));
        let lines = text.lines().count();
        assert!(
            lines <= 700,
            "{} has {lines} lines, above the 700-line adapter ceiling",
            path.display()
        );
        checked.push(path);
    }
    assert!(!checked.is_empty(), "no adapter family sources were found");
}
