//! Global/profile selection, exact tool caps and frozen legacy compatibility.
mod common;

use agent_run_config::{
    config::Mcp,
    profiles,
    role_plan::{ResolvedRolePlan, resolve_role_plan},
};
use serde_json::json;
use std::collections::BTreeMap;

/// Parses a canonical role with the supplied TOML MCP list and no other assets.
fn profile(home: &common::Home, entries: &str) -> profiles::Profile {
    profiles::parse(&format!("+++\nrevision='1'\nwrite=false\nnetwork=false\nallow_external_read_roots=false\nskills=[]\nmcp={entries}\nrequired_constraints=[]\n+++\nReview."), &home.request()).unwrap()
}

/// Creates an inert catalog declaration; resolving roles never starts it.
fn server(global: bool, tools: Option<&[&str]>) -> Mcp {
    serde_json::from_value(
        json!({"transport":"stdio", "command":"/bin/echo", "global":global, "allowed_tools":tools}),
    )
    .unwrap()
}

/// Global selection joins the legacy profile order once; filters can only narrow.
#[test]
fn global_union_intersection_and_empty_caps_are_frozen() {
    let home = common::Home::new();
    let catalog = BTreeMap::from([
        ("shared".into(), server(true, Some(&["write", "read"]))),
        ("all".into(), server(true, None)),
        ("off".into(), server(false, None)),
    ]);
    let selected = profile(
        &home,
        r#"[{name='shared',allowed_tools=['unknown','read']},{name='off',allowed_tools=[]}]"#,
    );
    let plan = resolve_role_plan(&selected, &home.path, &catalog, "global", None).unwrap();
    assert_eq!(
        plan.mcp
            .iter()
            .map(|s| (s.id.as_str(), s.selection.as_str()))
            .collect::<Vec<_>>(),
        [("shared", "both"), ("off", "profile"), ("all", "global")]
    );
    assert_eq!(plan.mcp[0].allowed_tools, Some(vec!["read".into()]));
    assert_eq!(plan.mcp[1].allowed_tools, Some(vec![]));
    assert_eq!(plan.mcp[2].allowed_tools, None);
    let payload = plan.to_payload();
    assert_eq!(ResolvedRolePlan::from_payload(&payload).unwrap(), plan);
    let mut tampered = payload;
    tampered["mcp"][0]["allowed_tools"] = json!(["write"]);
    assert!(ResolvedRolePlan::from_payload(&tampered).is_err());
    let empty =
        resolve_role_plan(&profile(&home, "[]"), &home.path, &catalog, "global", None).unwrap();
    assert_eq!(
        empty.mcp.iter().map(|s| s.id.as_str()).collect::<Vec<_>>(),
        ["all", "shared"]
    );
    assert_eq!(
        empty.mcp[1].allowed_tools,
        Some(vec!["read".into(), "write".into()])
    );
}

/// Bare profile names and absent catalog flags retain their historical JSON bytes.
#[test]
fn defaults_do_not_change_old_serialized_assets() {
    let home = common::Home::new();
    let server: Mcp =
        serde_json::from_value(json!({"transport":"stdio","command":"/bin/echo"})).unwrap();
    assert_eq!(
        serde_json::to_value(&server).unwrap(),
        json!({"transport":"stdio","command":"/bin/echo","args":[],"env_from":[],"approval_mode":"auto"})
    );
    let profile = profile(&home, "['old']");
    assert!(
        serde_json::to_value(&profile)
            .unwrap()
            .get("mcp_tools")
            .is_none()
    );
    let plan = resolve_role_plan(
        &profile,
        &home.path,
        &BTreeMap::from([("old".into(), server)]),
        "global",
        None,
    )
    .unwrap();
    assert_eq!(
        plan.to_payload()["mcp"][0],
        json!({"id":"old","transport":"stdio","command":"/bin/echo","args":[],"env_from":[]})
    );
    assert_eq!(
        ResolvedRolePlan::from_payload(&plan.to_payload()).unwrap(),
        plan
    );
}

/// Misspelled fields, duplicate selectors and wildcard tool names fail at parsing.
#[test]
fn malformed_profile_controls_are_rejected() {
    let home = common::Home::new();
    let base = "+++\nrevision='1'\nwrite=false\nnetwork=false\nallow_external_read_roots=false\nskills=[]\nrequired_constraints=[]\n";
    for entries in [
        "['a','a']",
        "[{name='a',tools=['read']}]",
        "[{name='a',allowed_tools=['read','read']}]",
        "[{name='a',allowed_tools=['*']}]",
        "[{name='a',allowed_tools='read'}]",
        "[{name='../a'}]",
    ] {
        assert!(
            profiles::parse(
                &format!("{base}mcp={entries}\n+++\nReview."),
                &home.request()
            )
            .is_err(),
            "{entries}"
        );
    }
}
