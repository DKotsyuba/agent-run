//! Temp-home parity coverage for Python bind and context hook contracts.

mod common;

use agent_run_core::{
    domain::{OrchestratorRef, Outcome},
    hooks::{bind, context},
};
use serde_json::json;

/// Mirrors Python `test_bind_hook.py::test_binding_is_immutable_empty_then_same_target_but_never_another`.
#[test]
fn python_bind_hook_is_idempotent_and_activates_late_delivery() {
    let home = common::Home::new();
    let (agent_id, _) = home
        .store()
        .admit(&home.request(), &home.config, &json!({}), None)
        .expect("fake durable admission");
    home.store()
        .finish(&agent_id, &Outcome::failure("fixture"), None, None)
        .expect("terminal receipt waits for post-tool binding");
    let payload = json!({
        "session_id":"session-1",
        "hook_event_name":"PostToolUse",
        "tool_response":{"content":[{"text":json!({"agent_id":agent_id.to_string()}).to_string()}]}
    });
    let first = bind::run_hook(&mut home.store(), &payload, "codex_queue", Some(5.0))
        .expect("raw post-tool hook binds");
    let second = bind::run_hook(&mut home.store(), &payload, "codex_queue", Some(6.0))
        .expect("same target is idempotent");
    assert_eq!(first.session_id, second.session_id);
    assert!(first.message().contains("bound to session"));
    let delivery: String = home
        .store()
        .conn
        .query_row(
            "SELECT state FROM deliveries WHERE agent_id=?",
            [agent_id.as_str()],
            |row| row.get(0),
        )
        .expect("one late delivery receipt");
    assert_eq!(delivery, "pending");
    let conflict = bind::run_hook(
        &mut home.store(),
        &json!({"agent_id":agent_id,"transport":"codex_queue","external_session_id":"other"}),
        "codex_queue",
        Some(7.0),
    )
    .expect_err("different session is refused loudly");
    assert!(conflict.to_string().contains("NOT confirmed"));
    assert!(conflict.to_string().contains("immutable"));
}

/// Mirrors Python `test_context_hook.py::test_first_prompt_creates_receipt_dedups_and_reuses_later_binding`.
#[test]
fn python_context_hook_is_bounded_changed_only_and_session_scoped() {
    let home = common::Home::new();
    let reference = OrchestratorRef {
        transport: "codex_queue".into(),
        external_session_id: "session-1".into(),
        external_turn_id: Some("turn-1".into()),
    };
    let first = context::build(&home.path, &reference, Some(1000.0)).expect("first context");
    let second = context::build(&home.path, &reference, Some(1001.0)).expect("dedup context");
    assert!(first.injected);
    assert!(first
        .text
        .starts_with("Runtime priorities (highest first)."));
    assert!(first.text.chars().count() <= context::CONTEXT_HARD_LIMIT_CHARS);
    assert!(!second.injected);
    assert!(second.text.is_empty());
    assert_eq!(
        first.orchestrator_session_id,
        second.orchestrator_session_id
    );
}

/// Mirrors Python `test_priority_context_regressions.py::test_route_identity_is_json_safe_and_aliases_collapse_per_route`.
#[test]
fn python_context_hook_rejects_conflicting_raw_agent_ids() {
    let error = bind::normalize(
        &json!({"session_id":"session-1","tool_response":[{"agent_id":"ag-20260916-120000-0123456789"},{"agent_id":"ag-20260916-120001-0123456789"}]}),
        true,
        "codex_queue",
    )
    .expect_err("conflicting ids cannot bind an arbitrary durable agent");
    assert!(error.to_string().contains("conflicting agent_id"));
}
