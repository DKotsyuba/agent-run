//! Ports `tests/test_profiles.py` and `tests/test_effective_policy.py`
//! (profile parsing/write decisions and effective-policy admission), plus
//! the `required_constraints` union regression (see `docs/api.md`).
mod common;

use agent_run_config::{
    policy::{self, Constraint, DeclaredCapability, Enforcement},
    profiles,
};
use std::{
    collections::{BTreeMap, BTreeSet},
    path::PathBuf,
};

/// Mirrors `test_profiles.py::test_named_profile_body_and_write_can_only_narrow`
/// and `test_canonical_role_owns_every_grant_and_asset_selection`: a legacy
/// profile's write grant can only narrow the request; a canonical role's
/// write grant is its own and ignores the request entirely.
#[test]
fn legacy_write_narrows_but_canonical_write_is_owned_by_the_role() {
    let home = common::Home::new();

    let mut request = home.request();
    request.write = true;
    let legacy_allowed = profiles::parse("+++\nwrite = true\n+++\nImplement.\n", &request).unwrap();
    assert!(!legacy_allowed.canonical);
    assert!(legacy_allowed.write);

    request.write = false;
    let legacy_denied = profiles::parse("+++\nwrite = true\n+++\nImplement.\n", &request).unwrap();
    assert!(
        !legacy_denied.write,
        "a legacy profile's own write flag can only narrow the request, never grant beyond it"
    );

    let canonical_text = "+++\n\
revision = \"1\"\n\
write = true\n\
network = false\n\
allow_external_read_roots = true\n\
skills = []\n\
mcp = []\n\
required_constraints = []\n\
+++\n\
Implement and verify the requested change.\n";
    request.write = false;
    let canonical = profiles::parse(canonical_text, &request).unwrap();
    assert!(
        canonical.write,
        "a canonical role's write grant does not depend on the request's write flag"
    );
}

/// Mirrors `tests/fixtures/baseline/config/cases.json` id
/// `test_profiles.py:106` and `test_profiles.py::test_incomplete_or_unrevisioned_canonical_role_is_rejected`.
#[test]
fn unknown_or_invalid_role_declarations_are_rejected() {
    let home = common::Home::new();
    let request = home.request();

    let mixed = profiles::parse(
        "+++\nwrite = false\nskills = [\"code-reading\"]\n+++\nReview.\n",
        &request,
    )
    .unwrap_err();
    assert!(mixed.to_string().contains("revision"));

    let incomplete = profiles::parse(
        "+++\nrevision = \"1\"\nwrite = false\n+++\nReview.\n",
        &request,
    )
    .unwrap_err();
    assert!(incomplete.to_string().contains("incomplete"));
}

/// Mirrors `tests/test_profiles.py::ProfileTests::test_profile_names_and_symlink_escape_are_rejected`
#[test]
fn profile_names_and_symlink_escape_are_rejected() {
    let home = common::Home::new();
    let outside = tempfile::tempdir().unwrap();
    std::fs::write(outside.path().join("role.md"), "Outside").unwrap();
    std::fs::create_dir_all(home.config.profiles_dir()).unwrap();
    std::os::unix::fs::symlink(
        outside.path().join("role.md"),
        home.config.profiles_dir().join("linked.md"),
    )
    .unwrap();
    assert!(profiles::profile_path(home.config.profiles_dir(), "../role").is_err());
    assert!(matches!(
        profiles::profile_path(home.config.profiles_dir(), "linked"),
        Err(agent_run_domain::Error::PathEscape(_))
    ));
}

/// Mirrors `tests/test_profiles.py::ProfileTests::test_read_roots_are_resolved_deduplicated_and_minimal`
#[test]
fn read_roots_are_resolved_deduplicated_and_minimal() {
    let home = common::Home::new();
    let child = home.path.join("child");
    std::fs::create_dir(&child).unwrap();
    let alias = home.path.join("alias");
    std::os::unix::fs::symlink(&child, &alias).unwrap();
    assert_eq!(
        profiles::normalize_read_roots(&[child, alias, home.path.clone()]).unwrap(),
        vec![home.path.clone()]
    );
    assert!(profiles::normalize_read_roots(&[PathBuf::from("relative")]).is_err());
    assert!(profiles::normalize_read_roots(&[home.path.join("missing")]).is_err());
}

/// Mirrors `tests/test_profiles.py::ProfileTests::test_canonical_role_owns_every_grant_and_asset_selection`
#[test]
fn canonical_role_owns_every_grant_and_asset_selection() {
    let home = common::Home::new();
    let mut request = home.request();
    request.profile = "implement".into();
    let role = profiles::parse(
        "+++
revision = \"1\"
write = true
network = false
allow_external_read_roots = true
skills = [\"lsp-first\", \"document-code\"]
mcp = [\"agent-lsp\"]
required_constraints = [\"plugin_immutability\"]
+++
Implement and verify the requested change.
",
        &request,
    )
    .unwrap();
    assert!(role.canonical && role.write && role.revision == "1");
    assert_eq!(role.skills, vec!["lsp-first", "document-code"]);
    assert_eq!(role.mcp, vec!["agent-lsp"]);
    assert!(role
        .required_constraints
        .contains(&Constraint::PluginImmutability));
}

/// Mirrors `tests/test_profiles.py::ProfileTests::test_incomplete_or_unrevisioned_canonical_role_is_rejected`
#[test]
fn incomplete_or_unrevisioned_canonical_role_is_rejected() {
    let home = common::Home::new();
    let request = home.request();
    assert!(profiles::parse(
        "+++
write = false
skills = [\"code-reading\"]
+++
Review.
",
        &request
    )
    .unwrap_err()
    .to_string()
    .contains("revision"));
    assert!(profiles::parse(
        "+++
revision = \"1\"
write = false
+++
Review.
",
        &request
    )
    .unwrap_err()
    .to_string()
    .contains("incomplete"));
}

/// Mirrors `test_effective_policy.py::test_only_explicit_required_unsupported_constraints_reject`:
/// a required, unsupported constraint denies admission with named evidence.
#[test]
fn admission_denies_with_evidence_when_a_required_isolation_constraint_is_unsupported() {
    let home = common::Home::new();
    let profile = profiles::parse("Implement.", &home.request()).unwrap();

    let required: BTreeSet<Constraint> = [Constraint::ExternalNetworkIsolation].into();
    let policy =
        policy::effective_policy(&profile, "claude", "darwin", &BTreeMap::new(), &required);
    let decision = policy::admission_decision(&policy);

    assert!(!decision.allowed);
    assert_eq!(
        decision.unsupported_required,
        vec![Constraint::ExternalNetworkIsolation]
    );
    let evidence = policy
        .constraints
        .iter()
        .find(|item| item.constraint == Constraint::ExternalNetworkIsolation)
        .unwrap();
    assert!(evidence.required);
    assert!(!evidence.supported);
    assert!(!evidence.reason.is_empty());
    assert!(policy.admit().is_err());
}

/// Mirrors `tests/test_effective_policy.py::test_network_boundaries_are_independent_and_legacy_admission_stays_open`
#[test]
fn network_boundaries_are_independent_and_legacy_admission_stays_open() {
    let home = common::Home::new();
    let profile = profiles::parse("Implement.", &home.request()).unwrap();
    let policy = policy::effective_policy(
        &profile,
        "claude",
        "darwin",
        &BTreeMap::new(),
        &BTreeSet::new(),
    );
    let web = policy
        .constraints
        .iter()
        .find(|item| item.constraint == Constraint::WebToolsDisabled)
        .unwrap();
    assert!(web.supported && web.enforcement == Enforcement::ToolFilter);
    for constraint in [
        Constraint::ExternalNetworkIsolation,
        Constraint::LoopbackTcpIsolation,
        Constraint::UnixIpcIsolation,
        Constraint::McpIpcIsolation,
    ] {
        let evidence = policy
            .constraints
            .iter()
            .find(|item| item.constraint == constraint)
            .unwrap();
        assert!(!evidence.supported && evidence.enforcement == Enforcement::Unsupported);
        assert_eq!(evidence.platform, "darwin");
        assert!(!evidence.reason.is_empty());
    }
    assert!(policy::admission_decision(&policy).allowed);
}

/// Mirrors `tests/test_effective_policy.py::test_platform_scopes_are_applied_without_upgrading_configuration`
#[test]
fn platform_scopes_are_applied_without_upgrading_configuration() {
    let home = common::Home::new();
    let profile = profiles::parse(
        "+++
write = false
network = false
+++
Read.",
        &home.request(),
    )
    .unwrap();
    let required = BTreeSet::from([Constraint::FilesystemWriteIsolation]);
    let capabilities = BTreeMap::from([(
        Constraint::FilesystemWriteIsolation,
        DeclaredCapability {
            enforcement: Enforcement::OsEnforced,
            scope: "writes outside selected roots".into(),
            reason: "Linux sandbox probe passed".into(),
            platforms: BTreeSet::from(["linux".into()]),
        },
    )]);
    let policy = policy::effective_policy(&profile, "codex", "darwin", &capabilities, &required);
    let write = policy
        .constraints
        .iter()
        .find(|item| item.constraint == Constraint::FilesystemWriteIsolation)
        .unwrap();
    assert_eq!(write.enforcement, Enforcement::Unsupported);
    assert_eq!(write.platform, "darwin");
    assert!(write.reason.contains("linux"));
    assert_eq!(
        policy::admission_decision(&policy).unsupported_required,
        vec![Constraint::FilesystemWriteIsolation]
    );
}

/// Mirrors `tests/test_effective_policy.py::test_required_input_is_strictly_typed[required0]`
#[test]
fn required_input_is_strictly_typed_empty_set() {
    let home = common::Home::new();
    let profile = profiles::parse("Implement.", &home.request()).unwrap();
    let required: BTreeSet<Constraint> = BTreeSet::new();
    let policy =
        policy::effective_policy(&profile, "claude", "darwin", &BTreeMap::new(), &required);
    assert!(policy::admission_decision(&policy).allowed);
}

/// Mirrors `tests/test_effective_policy.py::test_required_input_is_strictly_typed[required1]`
#[test]
fn required_input_is_strictly_typed_constraint_set() {
    let home = common::Home::new();
    let profile = profiles::parse("Implement.", &home.request()).unwrap();
    let required: BTreeSet<Constraint> = BTreeSet::from([Constraint::PluginImmutability]);
    let policy =
        policy::effective_policy(&profile, "claude", "darwin", &BTreeMap::new(), &required);
    assert!(!policy::admission_decision(&policy).allowed);
}

/// Mirrors `tests/test_effective_policy.py::test_required_isolation_rejects_insufficient_levels[advisory]`
#[test]
fn required_advisory_isolation_is_rejected() {
    assert_insufficient_isolation(Enforcement::Advisory);
}

/// Mirrors `tests/test_effective_policy.py::test_required_isolation_rejects_insufficient_levels[tool_filter]`
#[test]
fn required_tool_filter_isolation_is_rejected() {
    assert_insufficient_isolation(Enforcement::ToolFilter);
}

/// Check one weak enforcement declaration in required and legacy modes.
fn assert_insufficient_isolation(enforcement: Enforcement) {
    let home = common::Home::new();
    let profile = profiles::parse("Implement.", &home.request()).unwrap();
    let capabilities = BTreeMap::from([(
        Constraint::ExternalNetworkIsolation,
        DeclaredCapability {
            enforcement,
            scope: "prompt or tool surface only".into(),
            reason: "no socket enforcement".into(),
            platforms: BTreeSet::new(),
        },
    )]);
    let required = BTreeSet::from([Constraint::ExternalNetworkIsolation]);
    let policy = policy::effective_policy(&profile, "claude", "darwin", &capabilities, &required);
    let external = policy
        .constraints
        .iter()
        .find(|item| item.constraint == Constraint::ExternalNetworkIsolation)
        .unwrap();
    assert_eq!(external.enforcement, enforcement);
    assert!(!external.supported);
    assert_eq!(
        policy::admission_decision(&policy).unsupported_required,
        vec![Constraint::ExternalNetworkIsolation]
    );
    let legacy = policy::effective_policy(
        &profile,
        "claude",
        "darwin",
        &capabilities,
        &BTreeSet::new(),
    );
    assert!(policy::admission_decision(&legacy).allowed);
}

/// Mirrors `test_effective_policy.py::test_required_isolation_rejects_insufficient_levels`:
/// advisory and tool-filter evidence stay visible but never satisfy an
/// isolation requirement, and the same evidence is fine when not required.
#[test]
fn advisory_and_tool_filter_evidence_cannot_satisfy_isolation_requirements() {
    let home = common::Home::new();
    let profile = profiles::parse("Implement.", &home.request()).unwrap();
    let required: BTreeSet<Constraint> = [Constraint::ExternalNetworkIsolation].into();

    for enforcement in [Enforcement::Advisory, Enforcement::ToolFilter] {
        let mut capabilities = BTreeMap::new();
        capabilities.insert(
            Constraint::ExternalNetworkIsolation,
            DeclaredCapability {
                enforcement,
                scope: "prompt or tool surface only".into(),
                reason: "no socket enforcement".into(),
                platforms: BTreeSet::new(),
            },
        );

        let required_policy =
            policy::effective_policy(&profile, "claude", "darwin", &capabilities, &required);
        assert!(!policy::admission_decision(&required_policy).allowed);

        let legacy_policy = policy::effective_policy(
            &profile,
            "claude",
            "darwin",
            &capabilities,
            &BTreeSet::new(),
        );
        assert!(policy::admission_decision(&legacy_policy).allowed);
    }
}

/// Regression: `Profile.required_constraints` is `role ∪ request`
/// (tightening), not Python's canonical-vs-legacy override. See `docs/api.md`.
#[test]
fn required_constraints_union_role_and_request_matches_python_when_only_one_side_is_set() {
    let home = common::Home::new();

    // Old Python baseline case 1: legacy profile, only the request declares a
    // requirement (a legacy profile has none of its own) -- union equals the
    // request's set alone, so old and new behavior agree.
    let mut legacy_request = home.request();
    legacy_request.required_constraints = [Constraint::ExternalNetworkIsolation].into();
    let legacy = profiles::parse("Implement.", &legacy_request).unwrap();
    assert_eq!(
        legacy.required_constraints,
        [Constraint::ExternalNetworkIsolation].into()
    );

    // Old Python baseline case 2: canonical role, no request requirement --
    // union equals the role's own declared set, so old and new behavior agree.
    let canonical_text = "+++\n\
revision = \"1\"\n\
write = false\n\
network = false\n\
allow_external_read_roots = false\n\
skills = []\n\
mcp = []\n\
required_constraints = [\"plugin_immutability\"]\n\
+++\n\
Review.\n";
    let mut request = home.request();
    let canonical_only = profiles::parse(canonical_text, &request).unwrap();
    assert_eq!(
        canonical_only.required_constraints,
        [Constraint::PluginImmutability].into()
    );

    // The intentional A11 divergence: a canonical role AND the request each
    // declare a different requirement. Python's `service.py` selection
    // (`profile.required_constraints if profile.canonical else
    // request.required_constraints`) would use only the role's set here,
    // silently dropping the request's. Rust unions both, tightening instead
    // of overwriting.
    request.required_constraints = [Constraint::ExternalNetworkIsolation].into();
    let canonical_and_request = profiles::parse(canonical_text, &request).unwrap();
    assert_eq!(
        canonical_and_request.required_constraints,
        [
            Constraint::PluginImmutability,
            Constraint::ExternalNetworkIsolation
        ]
        .into()
    );
}
