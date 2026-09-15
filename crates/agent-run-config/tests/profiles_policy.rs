//! Ports `tests/test_profiles.py` and `tests/test_effective_policy.py`
//! (profile parsing/write decisions and effective-policy admission), plus
//! the ADR A11 `required_constraints` regression
//! (see `migration/adr/A11-required-constraints.md`).
mod common;

use agent_run_config::{
    policy::{self, Constraint, DeclaredCapability, Enforcement},
    profiles,
};
use std::collections::{BTreeMap, BTreeSet};

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

/// ADR A11 regression: `Profile.required_constraints` is `role ∪ request`
/// (tightening), not Python's canonical-vs-legacy override. See
/// `migration/adr/A11-required-constraints.md`.
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
