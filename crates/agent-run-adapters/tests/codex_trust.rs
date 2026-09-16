//! Acceptance evidence for Codex MCP trust decisions and generated hooks.

use agent_run_adapters::codex::{
    allows_permission_request, permission_request_decision, permission_request_hook,
};
use serde_json::json;
use std::{collections::BTreeSet, path::Path};

/// Mirrors `tests/test_codex_permission_request.py::PermissionRequestTests::test_allows_hyphen_and_underscore_namespace_variants`
/// Mirrors `tests/test_codex_permission_request.py::PermissionRequestTests::test_declines_shell_unknown_and_malformed_requests`
#[test]
fn permission_requests_allow_only_trusted_mcp_namespaces() {
    let trusted = BTreeSet::from(["agent-run".into(), "agent-ide".into()]);
    for tool_name in [
        "mcp__agent-run__start",
        "mcp__agent_run__status",
        "mcp__agent-ide__context",
        "mcp__agent_ide__diff",
    ] {
        assert!(allows_permission_request(
            &json!({"hook_event_name":"PermissionRequest", "tool_name":tool_name}),
            &trusted,
        ));
    }
    for payload in [
        json!({"hook_event_name":"PermissionRequest", "tool_name":"Bash"}),
        json!({"hook_event_name":"PermissionRequest", "tool_name":"mcp__github__push"}),
        json!({"hook_event_name":"PreToolUse", "tool_name":"mcp__agent-run__start"}),
        json!({"hook_event_name":"PermissionRequest"}),
        json!([]),
    ] {
        assert!(!allows_permission_request(&payload, &trusted));
    }
    assert_eq!(
        permission_request_decision(
            &json!({"hook_event_name":"PermissionRequest", "tool_name":"mcp__agent_run__answer"}),
            &BTreeSet::from(["agent-run".into()]),
        ),
        Some(
            json!({"hookSpecificOutput":{"hookEventName":"PermissionRequest","decision":{"behavior":"allow"}}})
        )
    );
}

/// Mirrors `tests/test_codex_adapter.py::CodexAdapterTests::test_materialize_approves_only_configured_mcp_and_adds_narrow_hook`
#[test]
fn generated_permission_hook_contains_normalized_matcher_and_allow_arguments() {
    let (matcher, command) = permission_request_hook(
        &BTreeSet::from(["agent-run".into()]),
        Path::new("/usr/local/bin/agent-run"),
    )
    .unwrap()
    .unwrap();
    assert!(matcher.contains("mcp__agent-run__"));
    assert!(matcher.contains("mcp__agent_run__"));
    assert_eq!(
        command,
        vec![
            "/usr/local/bin/agent-run",
            "_permission-request",
            "--allow-mcp",
            "agent-run",
        ]
    );
}
