mod common;
use agent_run::{
    config::{self, Adapter, Config},
    domain::{AgentId, Status},
    policy::{self, Constraint},
    profiles,
};
use serde_json::{json, Value};
use std::collections::BTreeMap;

#[test]
fn generated_ids_roundtrip() {
    let mut seen = std::collections::HashSet::new();
    for _ in 0..1000 {
        let id = AgentId::new();
        assert_eq!(id.as_str().len(), 29);
        assert_eq!(id.as_str().parse::<AgentId>().unwrap(), id);
        assert!(seen.insert(id));
    }
}
#[test]
fn invalid_ids_are_rejected() {
    for s in [
        "../state.db",
        "ag-20260230-000000-0123456789",
        "ag-20260915-240001-0123456789",
        "ag-20260915-200000-012345ABCD",
        "ag-20260915-200000-01234567890",
        "",
    ] {
        assert!(s.parse::<AgentId>().is_err(), "{s}");
    }
}
#[test]
fn terminal_states_cannot_transition() {
    for from in Status::ALL.into_iter().filter(|s| s.terminal()) {
        for to in Status::ALL {
            assert!(from.transition(to).is_err());
        }
    }
}
#[test]
fn transition_matrix_matches_contract() {
    let allowed = [
        (Status::Created, Status::Starting),
        (Status::Created, Status::Cancelled),
        (Status::Created, Status::Failed),
        (Status::Created, Status::Lost),
        (Status::Starting, Status::Running),
        (Status::Starting, Status::Cancelled),
        (Status::Starting, Status::Failed),
        (Status::Starting, Status::Lost),
        (Status::Running, Status::Succeeded),
        (Status::Running, Status::Failed),
        (Status::Running, Status::TimedOut),
        (Status::Running, Status::Cancelling),
        (Status::Running, Status::Lost),
        (Status::Cancelling, Status::Cancelled),
        (Status::Cancelling, Status::Lost),
    ];
    for from in Status::ALL {
        for to in Status::ALL {
            assert_eq!(
                from.transition(to).is_ok(),
                allowed.contains(&(from, to)),
                "{from:?} -> {to:?}"
            );
        }
    }
}
#[test]
fn request_scalars_are_not_coerced() {
    let h = common::Home::new();
    let original = serde_json::to_value(h.request()).unwrap();
    for (key, value) in [
        ("write", json!(1)),
        ("fast", json!("true")),
        ("timeout_seconds", json!(true)),
        ("read_roots", json!("/tmp")),
        ("surprise", json!(true)),
    ] {
        let mut raw = original.clone();
        raw[key] = value;
        assert!(serde_json::from_value::<agent_run::domain::StartRequest>(raw).is_err());
    }
}
#[test]
fn request_rejects_bad_timeout_and_duplicate_roots() {
    let h = common::Home::new();
    for timeout in [0.0, -1.0, f64::NAN, f64::INFINITY] {
        let mut r = h.request();
        r.timeout_seconds = Some(timeout);
        assert!(r.validate().is_err());
    }
    let mut r = h.request();
    r.read_roots = vec![h.path.clone(), h.path.clone()];
    assert!(r.validate().is_err());
}
#[test]
fn constraint_duplicates_and_unknown_names_fail() {
    let h = common::Home::new();
    let mut raw = serde_json::to_value(h.request()).unwrap();
    raw["required_constraints"] = json!(["web_tools_disabled", "web_tools_disabled"]);
    assert!(serde_json::from_value::<agent_run::domain::StartRequest>(raw.clone()).is_err());
    raw["required_constraints"] = json!(["invented_sandbox"]);
    assert!(serde_json::from_value::<agent_run::domain::StartRequest>(raw).is_err());
}
#[test]
fn unknown_config_fields_fail_closed() {
    let h = common::Home::new();
    let file = h.path.join("config.toml");
    for text in [
        "schema_version=1\nunknown=true",
        "schema_version=1\n[core]\nmax_active_agents=true",
        "schema_version=1\n[core]\nmax_active_agents=0",
        "schema_version=2",
    ] {
        std::fs::write(&file, text).unwrap();
        assert!(Config::load(&h.path).is_err());
    }
}
#[test]
fn packaged_adapter_aliases_are_accepted_not_dynamic_imports() {
    assert_eq!(
        Adapter::parse("agent_run.adapters.codex.adapter:ADAPTER").unwrap(),
        Adapter::Codex
    );
    assert_eq!(
        Adapter::parse("agent_run.adapters.claude:ADAPTER").unwrap(),
        Adapter::Claude
    );
    assert!(Adapter::parse("arbitrary.module:OBJECT").is_err());
}
#[test]
fn weights_are_absolute_with_account_precedence() {
    let h = common::Home::new();
    let mut r = h.config.runtime("mock").unwrap().clone();
    r.priority_multiplier = 2.0;
    r.priority_lane_multipliers.insert("spark".into(), 3.0);
    r.priority_account_multipliers
        .insert("personal".into(), 4.0);
    assert_eq!(r.weight(None, "ordinary"), 2.0);
    assert_eq!(r.weight(None, "spark"), 3.0);
    assert_eq!(r.weight(Some("personal"), "spark"), 4.0);
}
#[test]
fn weights_reject_nonfinite_and_nonpositive() {
    let h = common::Home::new();
    for n in [0.0, -1.0, f64::INFINITY, f64::NAN] {
        let mut c = h.config.clone();
        c.runtimes.get_mut("mock").unwrap().priority_multiplier = n;
        assert!(c.validate(&h.path).is_err());
    }
}
#[test]
fn native_security_roots_cannot_be_overridden() {
    for (adapter, key) in [
        (Adapter::Codex, "sandbox_mode"),
        (Adapter::Claude, "permissions"),
        (Adapter::Glm, "apiKeyHelper"),
        (Adapter::Qwen, "tools"),
    ] {
        let m = BTreeMap::from([(key.to_owned(), toml::Value::String("override".into()))]);
        assert!(config::native_settings(adapter, &m).is_err());
    }
    assert!(config::native_settings(
        Adapter::Codex,
        &BTreeMap::from([(
            "model_context_window".into(),
            toml::Value::Integer(1_000_000)
        )])
    )
    .is_ok());
}
#[test]
fn native_values_reject_dotted_keys_dates_and_nan() {
    for (key, value) in [
        ("auth.token", toml::Value::String("x".into())),
        ("tuning", toml::Value::Float(f64::NAN)),
        (
            "tuning",
            toml::Value::Datetime("2026-09-15".parse().unwrap()),
        ),
    ] {
        assert!(
            config::native_settings(Adapter::Codex, &BTreeMap::from([(key.into(), value)]))
                .is_err()
        );
    }
}
#[test]
fn account_labels_and_environment_names_are_strict() {
    assert!(config::account("base"));
    assert!(!config::account("../x"));
    assert!(!config::account("Personal"));
    assert!(config::env_name("API_TOKEN"));
    assert!(!config::env_name("secret-value"));
    assert!(!config::env_name("9TOKEN"));
}
#[test]
fn legacy_profile_only_narrows_writes() {
    let h = common::Home::new();
    let mut r = h.request();
    r.write = false;
    assert!(
        !profiles::parse("+++\nwrite=true\n+++\nRole", &r)
            .unwrap()
            .write
    );
    r.write = true;
    assert!(
        !profiles::parse("+++\nwrite=false\n+++\nRole", &r)
            .unwrap()
            .write
    );
    assert!(
        profiles::parse("+++\nwrite=true\n+++\nRole", &r)
            .unwrap()
            .write
    );
}
fn canonical(write: bool) -> String {
    format!("+++\nrevision='v1'\nwrite={write}\nnetwork=false\nallow_external_read_roots=false\nskills=[]\nmcp=[]\nrequired_constraints=[]\n+++\nRole")
}
#[test]
fn canonical_role_owns_writes_and_retains_caller_constraints() {
    let h = common::Home::new();
    let mut r = h.request();
    r.required_constraints
        .insert(Constraint::ExternalNetworkIsolation);
    let role = profiles::parse(&canonical(true), &r).unwrap();
    assert!(role.write);
    assert!(role
        .required_constraints
        .contains(&Constraint::ExternalNetworkIsolation));
}
#[test]
fn incomplete_canonical_role_and_external_roots_fail() {
    let h = common::Home::new();
    let mut r = h.request();
    assert!(profiles::parse("+++\nrevision='v1'\n+++\nRole", &r).is_err());
    r.read_roots = vec![h.path.clone()];
    assert!(profiles::parse(&canonical(false), &r).is_err());
}
#[test]
fn roots_form_a_minimal_antichain() {
    let roots = vec![
        "/a/b".into(),
        "/a".into(),
        "/z".into(),
        "/a/b/c".into(),
        "/a".into(),
    ];
    assert_eq!(
        profiles::normalize_roots(&roots),
        vec![
            std::path::PathBuf::from("/a"),
            std::path::PathBuf::from("/z")
        ]
    );
}
#[test]
fn policy_does_not_invent_os_isolation() {
    let h = common::Home::new();
    let mut r = h.request();
    r.required_constraints
        .insert(Constraint::FilesystemReadIsolation);
    let p = profiles::parse("Read-only role", &r).unwrap();
    let evidence = policy::evaluate("mock", h.config.runtime("mock").unwrap(), &p);
    assert_eq!(evidence.constraints.len(), 8);
    assert!(evidence.admit().is_err());
    assert!(evidence
        .constraints
        .iter()
        .any(|e| e.constraint == Constraint::WebToolsDisabled && e.supported));
}
#[test]
fn codex_echo_cannot_widen_network() {
    use agent_run::adapters::codex::Grant;
    let grant = Grant {
        model: "fixture".into(),
        cwd: "/workspace".into(),
        roots: vec!["/workspace".into()],
        writable_roots: vec![],
        sandbox: "read-only".into(),
        approval_policy: "never".into(),
        reviewer: None,
        network_access: false,
        permission_profile: None,
    };
    let mut echo = json!({"model":"fixture","cwd":"/workspace","roots":["/workspace"],"sandbox":{"type":"readOnly","networkAccess":false},"approvalPolicy":"never"});
    assert!(grant.verify(&echo).is_ok());
    echo["sandbox"]["networkAccess"] = Value::Bool(true);
    assert!(grant.verify(&echo).is_err());
}
