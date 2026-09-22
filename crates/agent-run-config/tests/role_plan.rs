//! Ports `tests/test_role_plan.py` (canonical role resolution and its
//! frozen, Python-compatible payload/hash).
mod common;

use agent_run_config::{
    config::Mcp,
    profiles,
    role_plan::{resolve_role_plan, role_from_authority, ResolvedRolePlan},
};
use agent_run_domain::catalog::ResolvedLaunchAuthority;
use serde_json::{json, Value};
use std::{collections::BTreeMap, fs, path::PathBuf};

fn reseal(document: &mut Value) {
    let mut seed = document.as_object().unwrap().clone();
    seed.remove("config_revision");
    document["config_revision"] = json!(agent_run_domain::canonical::sha256_hex(
        &Value::Object(seed),
        true
    ));
}

/// Mirrors `test_resolves_serializable_revisioned_role`: bind prompt, grants,
/// skill revisions, MCP, auth reference, and hash. The skill revision and
/// this test's expected value were cross-checked against the Python
/// `tree_revision`/`content_hash` implementation for the same single-file
/// skill tree.
#[test]
fn resolves_serializable_revisioned_role_with_python_compatible_hash() {
    let home = common::Home::new();
    let root = &home.path;
    let skills = root.join("skills");
    fs::create_dir_all(skills.join("code-reading")).unwrap();
    fs::write(skills.join("code-reading/SKILL.md"), "Read code.\n").unwrap();

    let text = "+++\n\
revision = \"3\"\n\
write = false\n\
network = false\n\
allow_external_read_roots = true\n\
skills = [\"code-reading\"]\n\
mcp = [\"codegraph\"]\n\
required_constraints = []\n\
+++\n\
Review the assigned change.\n";
    let mut request = home.request();
    request.read_roots = vec![root.clone()];
    let profile = profiles::parse(text, &request).unwrap();

    let mut mcp_catalog = BTreeMap::new();
    mcp_catalog.insert(
        "codegraph".to_string(),
        Mcp {
            transport: "stdio".into(),
            command: PathBuf::from("/bin/echo").canonicalize().unwrap(),
            args: vec!["serve".into()],
            env_from: vec!["PATH".into()],
            approval_mode: "approve".into(),
        },
    );

    let plan = resolve_role_plan(
        &profile,
        &skills,
        &mcp_catalog,
        "account",
        Some("personal2"),
    )
    .expect("resolves");
    let payload = plan.to_payload();
    assert_eq!(payload["role_revision"], "3");
    assert_eq!(
        payload["auth"],
        json!({"mode": "account", "reference": "personal2"})
    );
    assert_eq!(payload["skills"][0]["id"], "code-reading");
    assert_eq!(payload["mcp"][0]["approval_mode"], "approve");
    assert_eq!(
        payload["skills"][0]["revision"],
        "29ec0f76a446221ad66bb45110a822e8b2803b1d953ed99dea296614339a8746"
    );
    assert_eq!(plan.config_revision.len(), 64);
    assert_eq!(ResolvedRolePlan::from_payload(&payload).unwrap(), plan);
    let mut authority = ResolvedLaunchAuthority {
        provider: "codex".parse().unwrap(),
        harness: agent_run_domain::HarnessId::Codex,
        protocol_endpoint: "https://api.example.com".into(),
        model: "gpt-5.1".into(),
        effort: None,
        profile: plan.role_name.clone(),
        workdir: root.clone(),
        role_payload: payload.clone(),
        assets_sha256: "a".repeat(64).parse().unwrap(),
        eligible_accounts: vec!["acct-one".parse().unwrap()],
    };
    let actual_assets_sha256 = "a".repeat(64).parse().unwrap();
    assert_eq!(
        role_from_authority(&authority, &actual_assets_sha256).unwrap(),
        plan
    );
    assert!(role_from_authority(&authority, &"b".repeat(64).parse().unwrap()).is_err());
    authority.role_payload["grants"]["write"] = json!(true);
    assert!(role_from_authority(&authority, &actual_assets_sha256).is_err());
    authority.role_payload = payload.clone();
    authority.role_payload["grants"]["write"] = json!("invalid");
    reseal(&mut authority.role_payload);
    assert!(authority.validate().is_ok());
    assert!(role_from_authority(&authority, &actual_assets_sha256).is_err());
    authority.role_payload = payload.clone();
    authority.profile = "other-role".into();
    assert!(role_from_authority(&authority, &actual_assets_sha256).is_err());

    let again = resolve_role_plan(
        &profile,
        &skills,
        &mcp_catalog,
        "account",
        Some("personal2"),
    )
    .expect("resolves again");
    assert_eq!(again.config_revision, plan.config_revision);
}

/// Mirrors `test_from_payload_rejects_malformed_or_tampered_documents`.
#[test]
fn from_payload_rejects_malformed_or_tampered_documents() {
    let payload: Value =
        serde_json::from_str(include_str!("fixtures/role_plan_7bbd43b.json")).unwrap();
    assert_eq!(
        ResolvedRolePlan::from_payload(&payload)
            .unwrap()
            .to_payload(),
        payload
    );

    let mut cases = Vec::new();

    let mut extra = payload.clone();
    extra
        .as_object_mut()
        .unwrap()
        .insert("extra".into(), json!(true));
    cases.push(extra);

    let mut missing = payload.clone();
    missing.as_object_mut().unwrap().remove("prompt");
    cases.push(missing);

    let mut nested_extra = payload.clone();
    nested_extra["grants"]
        .as_object_mut()
        .unwrap()
        .insert("extra".into(), json!(false));
    cases.push(nested_extra);

    let mut wrong_bool = payload.clone();
    wrong_bool["grants"]["write"] = json!(1);
    cases.push(wrong_bool);

    let mut bad_root = payload.clone();
    bad_root["grants"]["read_roots"] = json!(["relative"]);
    cases.push(bad_root);

    let mut bad_skill = payload.clone();
    bad_skill["skills"][0]["revision"] = json!("bad");
    cases.push(bad_skill);

    let mut duplicate_skill = payload.clone();
    let first_skill = duplicate_skill["skills"][0].clone();
    duplicate_skill["skills"]
        .as_array_mut()
        .unwrap()
        .push(first_skill);
    cases.push(duplicate_skill);

    let mut bad_mcp = payload.clone();
    bad_mcp["mcp"] = json!([{
        "id": "tool", "transport": "http", "command": "relative", "args": [], "env_from": []
    }]);
    cases.push(bad_mcp);

    let mut bad_env = payload.clone();
    bad_env["mcp"] = json!([{
        "id": "tool", "transport": "stdio", "command": "/bin/echo", "args": [], "env_from": ["bad-name"]
    }]);
    cases.push(bad_env);

    let mut bad_constraint = payload.clone();
    bad_constraint["required_constraints"] = json!(["unknown"]);
    cases.push(bad_constraint);

    let mut bad_auth = payload.clone();
    bad_auth["auth"] = json!({"mode": "global", "reference": "account"});
    cases.push(bad_auth);

    let mut bad_revision = payload.clone();
    bad_revision["config_revision"] = json!("0".repeat(64));
    cases.push(bad_revision);

    for case in cases {
        assert!(
            ResolvedRolePlan::from_payload(&case).is_err(),
            "expected rejection for {case}"
        );
    }
}

/// Mirrors `test_from_payload_keeps_duplicate_args_and_rejects_command_drift`.
#[test]
fn from_payload_keeps_duplicate_args_and_rejects_command_drift() {
    let mut payload: Value =
        serde_json::from_str(include_str!("fixtures/role_plan_7bbd43b.json")).unwrap();
    let command = std::fs::canonicalize("/bin/echo")
        .unwrap()
        .to_string_lossy()
        .into_owned();
    payload["mcp"] = json!([{
        "id": "tool",
        "transport": "stdio",
        "command": command,
        "args": ["--flag", "--flag"],
        "env_from": [],
    }]);
    reseal(&mut payload);

    let plan = ResolvedRolePlan::from_payload(&payload).expect("duplicate args are preserved");
    assert_eq!(
        plan.mcp[0].args,
        vec!["--flag".to_string(), "--flag".to_string()]
    );

    let mut drifted = payload.clone();
    let path = std::path::Path::new(&command);
    let parent = path.parent().unwrap().to_string_lossy().into_owned();
    let name = path.file_name().unwrap().to_string_lossy().into_owned();
    drifted["mcp"][0]["command"] = json!(format!("{parent}/nested/../{name}"));
    reseal(&mut drifted);

    let error = ResolvedRolePlan::from_payload(&drifted).unwrap_err();
    let message = error.to_string();
    assert!(
        message.contains("mcp[0]") && message.contains("invalid"),
        "unexpected error: {message}"
    );
}

/// Mirrors `test_missing_skill_or_mcp_fails_closed`.
#[test]
fn missing_skill_or_mcp_fails_closed() {
    let home = common::Home::new();
    let skills = home.path.join("skills");
    fs::create_dir_all(&skills).unwrap();

    let text = "+++\n\
revision = \"1\"\n\
write = false\n\
network = false\n\
allow_external_read_roots = false\n\
skills = [\"missing\"]\n\
mcp = [\"missing\"]\n\
required_constraints = []\n\
+++\n\
Review.\n";
    let profile = profiles::parse(text, &home.request()).unwrap();

    let error = resolve_role_plan(&profile, &skills, &BTreeMap::new(), "global", None).unwrap_err();
    assert!(error.to_string().contains("skill is not available"));
}

/// Extra coverage (not in the Python suite, needed for board acceptance):
/// `resolve_role_plan` rejects an unresolved/non-canonical role outright.
#[test]
fn resolve_role_plan_rejects_a_non_canonical_role() {
    let home = common::Home::new();
    let profile =
        profiles::parse("+++\nwrite = true\n+++\nLegacy body.\n", &home.request()).unwrap();
    assert!(!profile.canonical);

    let error =
        resolve_role_plan(&profile, &home.path, &BTreeMap::new(), "global", None).unwrap_err();
    assert!(error.to_string().contains("canonical profile"));
}

/// Extra coverage (board acceptance: "frozen-plan hash stability across
/// configuration edits"): the config_revision does not move when unrelated
/// catalog entries are added, only when the role's own inputs change.
#[test]
fn config_revision_is_stable_across_unrelated_configuration_edits() {
    let home = common::Home::new();
    let skills = home.path.join("skills");
    fs::create_dir_all(skills.join("code-reading")).unwrap();
    fs::write(skills.join("code-reading/SKILL.md"), "Read code.\n").unwrap();

    let text = "+++\n\
revision = \"1\"\n\
write = false\n\
network = false\n\
allow_external_read_roots = false\n\
skills = [\"code-reading\"]\n\
mcp = []\n\
required_constraints = []\n\
+++\n\
Review.\n";
    let profile = profiles::parse(text, &home.request()).unwrap();

    let baseline = resolve_role_plan(&profile, &skills, &BTreeMap::new(), "global", None).unwrap();

    let mut unrelated = BTreeMap::new();
    unrelated.insert(
        "unused".to_string(),
        Mcp {
            transport: "stdio".into(),
            command: PathBuf::from("/bin/echo").canonicalize().unwrap(),
            args: vec![],
            env_from: vec![],
            approval_mode: "auto".into(),
        },
    );
    let after_edit = resolve_role_plan(&profile, &skills, &unrelated, "global", None).unwrap();

    assert_eq!(baseline.config_revision, after_edit.config_revision);

    // Editing the role's own prompt body, in contrast, must move the hash.
    let changed_text = text.replace("Review.\n", "Review carefully.\n");
    let changed_profile = profiles::parse(&changed_text, &home.request()).unwrap();
    let after_prompt_change =
        resolve_role_plan(&changed_profile, &skills, &BTreeMap::new(), "global", None).unwrap();
    assert_ne!(
        baseline.config_revision,
        after_prompt_change.config_revision
    );
}
