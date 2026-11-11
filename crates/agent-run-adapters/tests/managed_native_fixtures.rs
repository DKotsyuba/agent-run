//! Managed-role verifiers against control replies captured from the real
//! Claude Code 2.1.311 and codex-cli 0.167.0 harnesses on dobi
//! (`tests/fixtures/managed-native/README.md`).

use agent_run_adapters::managed_role::{
    CLAUDE_DISALLOWED_TOOLS, CODEX_MANAGED_DISABLED_IF_KNOWN, claude_catalog_digest,
    claude_disallowed_tools, codex_settings, verify_claude_control_inventory,
    verify_claude_inventory, verify_inventory, verify_managed_config,
};
use serde_json::{Value, json};
use std::collections::{BTreeMap, BTreeSet};

const POOL: &str = "agent-pool";
const WORKER: &str = "agent_run_worker";
/// The Pool tools the dobi managed role allows (all of SAFE_POOL_TOOLS).
const ALLOWED: &[&str] = &[
    "pool_create",
    "pool_list",
    "pool_get",
    "pool_update",
    "pool_close",
    "pool_add",
    "pool_cancel_replacement",
    "pool_member_update",
    "pool_remove",
    "pool_release_history",
    "pool_join",
    "pool_post",
    "pool_send_member",
    "pool_notify_owner",
    "pool_propose",
    "pool_vote",
];
/// Pool tools outside the managed allowlist, served by the same catalog.
const DENIED: &[&str] = &[
    "get_status",
    "pool_start_agent",
    "pool_start_batch",
    "pool_replace",
    "pool_enrollment_get",
    "pool_enrollment_resolve",
];

fn fixture(name: &str) -> String {
    let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../tests/fixtures/managed-native")
        .join(name);
    std::fs::read_to_string(&path).unwrap_or_else(|error| panic!("{}: {error}", path.display()))
}

fn json_fixture(name: &str) -> Value {
    serde_json::from_str(&fixture(name)).unwrap()
}

fn set(names: &[&str]) -> BTreeSet<String> {
    names.iter().map(|name| (*name).to_owned()).collect()
}

fn allowed() -> BTreeMap<String, BTreeSet<String>> {
    BTreeMap::from([
        (POOL.into(), set(ALLOWED)),
        (WORKER.into(), set(&["finish", "health_reply"])),
    ])
}

/// The catalog Run discovers from the same Pool frontend before launch.
fn catalogs() -> BTreeMap<String, BTreeSet<String>> {
    let mut catalog = set(ALLOWED);
    catalog.extend(set(DENIED));
    BTreeMap::from([(POOL.into(), catalog)])
}

fn status_tools<'a>(status: &'a mut Value, server: &str) -> &'a mut Vec<Value> {
    status["response"]["response"]["mcpServers"]
        .as_array_mut()
        .unwrap()
        .iter_mut()
        .find(|entry| entry["name"] == server)
        .unwrap()["tools"]
        .as_array_mut()
        .unwrap()
}

#[test]
fn claude_2_1_311_reports_the_whole_discovered_catalog_before_input() {
    let initialize = json_fixture("claude-2.1.311-initialize.json");
    let status = json_fixture("claude-2.1.311-mcp-status.json");
    let init = json_fixture("claude-2.1.311-system-init.json");
    let (allowed, catalogs) = (allowed(), catalogs());
    let digest = claude_catalog_digest(&catalogs).unwrap();
    let denied = claude_disallowed_tools(&allowed, &catalogs).unwrap();
    assert_eq!(
        denied,
        format!(
            "{CLAUDE_DISALLOWED_TOOLS},{}",
            DENIED
                .iter()
                .map(|tool| format!("mcp__{POOL}__{tool}"))
                .collect::<BTreeSet<_>>()
                .into_iter()
                .collect::<Vec<_>>()
                .join(",")
        )
    );
    // The captured raw list is the full 22-tool catalog despite the deny rules.
    let reported: BTreeSet<String> = status["response"]["response"]["mcpServers"][0]["tools"]
        .as_array()
        .unwrap()
        .iter()
        .map(|tool| tool["name"].as_str().unwrap().to_owned())
        .collect();
    assert_eq!(&reported, &catalogs[POOL]);
    let verify = |status: &Value| {
        verify_claude_control_inventory(&initialize, status, &allowed, &catalogs, &digest, &denied)
    };
    assert_eq!(
        verify(&status).unwrap(),
        json!({POOL:"catalog",WORKER:"allowlist"})
    );
    // The model-visible list after input omits every denied tool.
    verify_claude_inventory(&init, &allowed).unwrap();

    // A tool outside the discovered catalog is refused.
    let mut extra = status.clone();
    status_tools(&mut extra, POOL).push(json!({"name":"pool_foreign"}));
    assert!(verify(&extra).is_err());
    // A missing allowed tool is refused, whether or not denied tools remain.
    for keep_denied in [true, false] {
        let mut missing = status.clone();
        status_tools(&mut missing, POOL).retain(|tool| {
            tool["name"] != "pool_vote"
                && (keep_denied || !DENIED.contains(&tool["name"].as_str().unwrap()))
        });
        assert!(verify(&missing).is_err());
    }
    // A partial deny-complement is neither the allowlist nor the catalog.
    let mut partial = status.clone();
    status_tools(&mut partial, POOL).retain(|tool| tool["name"] != "pool_start_batch");
    assert!(verify(&partial).is_err());
    // The worker server has no discovered catalog and stays exact.
    let mut worker = status.clone();
    status_tools(&mut worker, WORKER).push(json!({"name":"pool_start_batch"}));
    assert!(verify(&worker).is_err());
    // An unknown server is refused.
    let mut foreign = status.clone();
    foreign["response"]["response"]["mcpServers"][0]["name"] = json!("foreign");
    assert!(verify(&foreign).is_err());
    // The recorded catalog and its passed complement must both match.
    assert!(
        verify_claude_control_inventory(
            &initialize,
            &status,
            &allowed,
            &catalogs,
            &"0".repeat(64),
            &denied
        )
        .is_err()
    );
    assert!(
        verify_claude_control_inventory(
            &initialize,
            &status,
            &allowed,
            &catalogs,
            &digest,
            CLAUDE_DISALLOWED_TOOLS
        )
        .is_err()
    );
    let mut wider = catalogs.clone();
    wider.get_mut(POOL).unwrap().insert("pool_foreign".into());
    let wider_digest = claude_catalog_digest(&wider).unwrap();
    assert!(
        verify_claude_control_inventory(
            &initialize,
            &status,
            &allowed,
            &wider,
            &wider_digest,
            &denied
        )
        .is_err()
    );

    // The post-input tripwire stays exact.
    let mut widened = init.clone();
    widened["tools"]
        .as_array_mut()
        .unwrap()
        .push(json!(format!("mcp__{POOL}__pool_start_batch")));
    assert!(verify_claude_inventory(&widened, &allowed).is_err());
    let mut narrowed = init.clone();
    narrowed["tools"]
        .as_array_mut()
        .unwrap()
        .retain(|tool| tool != &json!(format!("mcp__{POOL}__pool_vote")));
    assert!(verify_claude_inventory(&narrowed, &allowed).is_err());
}

/// Pinned hook declarations as the supervisor reads them from the
/// Run-generated `config.toml`.
fn pinned_hooks() -> Value {
    let native: toml::Value = toml::from_str(&fixture("codex-0.167.0-run-config.toml")).unwrap();
    serde_json::to_value(native).unwrap()["hooks"].clone()
}

#[test]
fn codex_0_167_0_config_read_accepts_only_disabled_drift() {
    let hooks = pinned_hooks();
    let (pre, permission, state) = (
        &hooks["PreToolUse"],
        &hooks["PermissionRequest"],
        &hooks["state"],
    );
    let verify = |response: &Value| verify_managed_config(response, pre, permission, state);

    // The failed attempt's configuration: four default-on features and the
    // ignored `tools.view_image` keep it refused.
    let failed = json_fixture("codex-0.167.0-config-read.json");
    for feature in [
        "api_key_cyber_access_programs",
        "api_key_model_discovery",
        "auth_elicitation",
        "mentions_v2",
    ] {
        assert_eq!(failed["config"]["features"][feature], json!(true));
    }
    assert!(failed["config"]["tools"].get("view_image").is_none());
    assert!(verify(&failed).is_err());

    // Run now writes those features disabled for managed launches only.
    for feature in CODEX_MANAGED_DISABLED_IF_KNOWN {
        assert_eq!(codex_settings()["features"][*feature], json!(false));
    }
    let managed = json_fixture("codex-0.167.0-config-read-managed.json");
    let accepted = verify(&managed).unwrap();
    let unknown: BTreeSet<_> = accepted["unknown_features"]
        .as_object()
        .unwrap()
        .keys()
        .cloned()
        .collect();
    assert_eq!(
        unknown,
        set(&[
            "api_key_cyber_access_programs",
            "api_key_model_discovery",
            "auth_elicitation",
            "background_paginated_rollout_migration",
            "codex_apps_mcp_2026_07_28",
            "mcp_2026_07_28",
            "mcp_connection_sharing",
            "memories",
            "mentions_v2",
            "network_proxy",
            "remote_control",
            "view_image",
            "windows_sandbox_service",
        ])
    );
    assert_eq!(accepted["unknown_features"]["network_proxy"], json!(null));
    assert_eq!(
        accepted["empty_hook_events"],
        json!([
            "Interrupt",
            "PostCompact",
            "PostModelRequest",
            "PostToolUse",
            "PreCompact",
            "PreModelRequest",
            "SessionEnd",
            "SessionStart",
            "Stop",
            "SubagentStart",
            "SubagentStop",
            "UserPromptSubmit"
        ])
    );

    // An enabled unrecognized feature is refused.
    for feature in ["mentions_v2", "memories", "network_proxy", "future_feature"] {
        let mut enabled = managed.clone();
        enabled["config"]["features"][feature] = json!(true);
        assert!(verify(&enabled).is_err(), "{feature}");
    }
    // A non-empty handler list on any other hook event is refused.
    for event in ["PostToolUse", "SessionStart", "Stop", "FutureEvent"] {
        let mut hooked = managed.clone();
        hooked["config"]["hooks"][event] = pre.clone();
        assert!(verify(&hooked).is_err(), "{event}");
    }
    // The required hooks still match every pinned field; an asynchronous
    // (non-gating) or retargeted handler is refused.
    for (field, value) in [
        ("async", json!(true)),
        ("command", json!("/foreign")),
        ("timeout", json!(1)),
        ("commandWindows", json!("C:\\foreign.exe")),
    ] {
        let mut changed = managed.clone();
        changed["config"]["hooks"]["PreToolUse"][0]["hooks"][0][field] = value;
        assert!(verify(&changed).is_err(), "{field}");
    }
    let mut trust = managed.clone();
    trust["config"]["hooks"]["state"]
        .as_object_mut()
        .unwrap()
        .values_mut()
        .next()
        .unwrap()["trusted_hash"] = json!("sha256:foreign");
    assert!(verify(&trust).is_err());
    // Known security features keep their exact values.
    for (pointer, value) in [
        ("/config/features/shell_tool", json!(true)),
        ("/config/features/hooks", json!(false)),
        ("/config/features/view_image", json!(true)),
        ("/config/agents/enabled", json!(true)),
        ("/config/web_search", json!("live")),
    ] {
        let mut widened = managed.clone();
        *widened.pointer_mut(pointer).unwrap() = value;
        assert!(verify(&widened).is_err(), "{pointer}");
    }
}

#[test]
fn codex_0_167_0_inventory_is_exactly_the_enabled_tools() {
    let status = json_fixture("codex-0.167.0-mcp-server-status.json");
    let allowed = allowed();
    verify_inventory(&status, &allowed).unwrap();
    let mut extra = status.clone();
    extra["data"][0]["tools"]["pool_start_batch"] = json!({"name":"pool_start_batch"});
    assert!(verify_inventory(&extra, &allowed).is_err());
    let mut missing = status.clone();
    missing["data"][0]["tools"]
        .as_object_mut()
        .unwrap()
        .remove("pool_vote");
    assert!(verify_inventory(&missing, &allowed).is_err());
}
