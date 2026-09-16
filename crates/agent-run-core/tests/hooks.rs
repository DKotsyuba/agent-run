//! Temp-home parity coverage for Python bind and context hook contracts.

mod common;

use agent_run_core::{
    domain::{now, AgentId, OrchestratorRef, Outcome, Status},
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

/// Returns the fixed host session identity used by the hook parity tests.
fn reference() -> OrchestratorRef {
    OrchestratorRef {
        transport: "codex_queue".into(),
        external_session_id: "session-1".into(),
        external_turn_id: Some("turn-1".into()),
    }
}

/// Mirrors `tests/test_bind_hook.py::BindHookTests::test_unbound_running_agent_keeps_running_without_a_delivery`
///
/// Binding a live agent only routes its future completion: it must neither
/// disturb the run nor manufacture a notice before there is a terminal result.
#[test]
fn unbound_running_agent_keeps_running_without_a_delivery() {
    let home = common::Home::new();
    let (agent_id, _) = home
        .store()
        .admit(&home.request(), &home.config, &json!({}), None)
        .unwrap();
    let mut store = home.store();
    store.running(&agent_id, std::process::id() as i32).unwrap();
    assert_eq!(
        store.delivery_status(&agent_id).unwrap()["state"],
        "not_created"
    );
    bind::bind(&mut store, agent_id.clone(), reference(), 5.0).unwrap();
    assert_eq!(store.get(&agent_id).unwrap().status, Status::Running);
    assert_eq!(
        store.delivery_status(&agent_id).unwrap()["state"],
        "not_created",
        "a running agent has no completion to announce yet"
    );
}

/// Mirrors `tests/test_bind_hook.py::BindHookTests::test_bound_agent_gets_exactly_one_notice_and_a_rebind_never_resurrects_it`
///
/// An orchestrator-backed start binds before the agent finishes, so exactly one
/// notice is created by the terminal commit; repeating the identical bind later
/// must not duplicate that row or drag an already delivered notice back.
#[test]
fn bound_agent_gets_exactly_one_notice_and_a_rebind_never_resurrects_it() {
    let home = common::Home::new();
    let (agent_id, _) = home
        .store()
        .admit(&home.request(), &home.config, &json!({}), None)
        .unwrap();
    let mut store = home.store();
    let first = bind::bind(&mut store, agent_id.clone(), reference(), 1.0).unwrap();
    store
        .finish(&agent_id, &Outcome::failure("fixture"), None, None)
        .unwrap();
    let notices = |store: &agent_run_store::Store| -> Vec<String> {
        let mut statement = store
            .conn
            .prepare("SELECT state FROM deliveries WHERE agent_id=? ORDER BY id")
            .unwrap();
        let rows = statement
            .query_map([agent_id.as_str()], |row| row.get::<_, String>(0))
            .unwrap()
            .collect::<rusqlite::Result<Vec<_>>>()
            .unwrap();
        rows
    };
    assert_eq!(notices(&store), vec!["pending".to_owned()]);

    store
        .conn
        .execute(
            "UPDATE deliveries SET state='delivered',next_attempt_at=NULL WHERE agent_id=?",
            [agent_id.as_str()],
        )
        .unwrap();
    let repeated = bind::bind(&mut store, agent_id.clone(), reference(), 7.0).unwrap();
    assert_eq!(repeated.session_id, first.session_id);
    assert_eq!(
        notices(&store),
        vec!["delivered".to_owned()],
        "a repeated bind must neither duplicate nor resurrect the notice"
    );
}

/// Mirrors `tests/test_bind_hook.py::BindHookTests::test_bind_rejects_arguments_that_are_not_the_declared_contract`
///
/// Python passes a foreign object where a store belongs and a raw mapping where
/// an orchestrator reference belongs; Rust's signature makes both unrepresentable,
/// so the same "only the declared contract is accepted" rule is asserted at the
/// payload boundary that does accept untyped input.
#[test]
fn bind_rejects_arguments_that_are_not_the_declared_contract() {
    let listed = json!([["agent_id", "ag-20260916-120000-0123456789"]]);
    assert!(
        bind::normalize(&listed, true, "codex_queue").is_err(),
        "a non-object payload is not the declared contract"
    );
    let surprising = json!({
        "agent_id": "ag-20260916-120000-0123456789",
        "transport": "codex_queue",
        "external_session_id": "session-1",
        "surprise": 1,
    });
    assert!(
        bind::normalize(&surprising, true, "codex_queue").is_err(),
        "undeclared keys are refused"
    );
    let incomplete =
        json!({"agent_id": "ag-20260916-120000-0123456789", "transport": "codex_queue"});
    assert!(bind::normalize(&incomplete, true, "codex_queue").is_err());
    let unknown_transport = json!({
        "agent_id": "ag-20260916-120000-0123456789",
        "transport": "codex_queue",
        "external_session_id": "session-1",
    });
    assert!(
        bind::normalize(&unknown_transport, true, "smoke_signal").is_err(),
        "only declared host transports may bind"
    );
}

/// Admits one active agent attributed to the shared host session.
///
/// The configuration is re-read from the home on every call so a test that
/// raises an admission limit before admitting sees that limit.
fn active_agent(home: &common::Home, task: &str, request_id: &str) -> AgentId {
    let config = agent_run_config::config::Config::load(&home.path).expect("home configuration");
    let mut request = home.request();
    request.task = task.into();
    request.request_id = Some(request_id.into());
    request.orchestrator = Some(reference());
    home.store()
        .admit(&request, &config, &json!({}), None)
        .unwrap()
        .0
}

/// Mirrors `tests/test_context_hook.py::ContextHookTests::test_bounds_and_dedup_key_is_stable_until_agent_state_changes`
///
/// The active block stays inside its own reservation, and its receipt key tracks
/// agent state rather than elapsed time, so a quiet turn repeats nothing while a
/// terminal transition still changes the key without re-injecting stale text.
#[test]
fn context_bounds_and_dedup_key_is_stable_until_agent_state_changes() {
    let home = common::Home::new();
    let agent_id = active_agent(&home, &format!("summary {}", "x".repeat(100)), "req-bounds");
    let first = context::build(&home.path, &reference(), Some(1000.0)).expect("first context");
    assert!(first.orchestrator_session_id.is_some());
    assert!(first.injected);
    assert!(first.text.contains("Active agents (1)"));
    assert!(first.text.chars().count() <= context::CONTEXT_HARD_LIMIT_CHARS);
    let active_block = first.text.lines().next_back().expect("active block");
    assert!(active_block.chars().count() <= context::ACTIVE_BLOCK_MAX_CHARS);
    assert!(active_block.contains("summary"));
    assert!(active_block.contains("do not start replacements for existing ids"));

    let second = context::build(&home.path, &reference(), Some(1030.0)).expect("quiet turn");
    assert!(!second.injected);
    assert!(second.text.is_empty());
    assert_eq!(second.context_key, first.context_key);

    home.store()
        .finish(&agent_id, &Outcome::failure("fixture"), None, None)
        .unwrap();
    let third = context::build(&home.path, &reference(), Some(1041.0)).expect("terminal turn");
    assert!(!third.injected);
    assert_ne!(third.context_key, first.context_key);
    assert!(!third.text.contains("Active agents"));
}

/// Mirrors `tests/test_context_hook.py::ContextHookTests::test_context_budget_is_never_exceeded_with_many_active_agents`
///
/// A crowded session is summarized, not truncated at the host: the block names a
/// bounded number of agents, reports the remainder, and still fits both the hard
/// limit and the active reservation.
#[test]
fn context_budget_is_never_exceeded_with_many_active_agents() {
    let home = common::Home::new();
    // The crowd itself is the fixture, so the admission ceiling must not be the
    // thing under test: raise it above the twelve agents this contract needs.
    let path = home.path.join("config.toml");
    let original = std::fs::read_to_string(&path).unwrap();
    std::fs::write(&path, format!("{original}\n[core]\nmax_active_agents=32\n")).unwrap();
    for index in 0..12 {
        active_agent(&home, &format!("task {index}"), &format!("req-{index}"));
    }
    let result = context::build(&home.path, &reference(), Some(2000.0)).expect("crowded context");
    assert!(result.text.contains("Active agents (12)"));
    assert!(result.text.contains("more"));
    assert!(result.text.chars().count() <= context::CONTEXT_HARD_LIMIT_CHARS);
    let active_block = result.text.lines().next_back().expect("active block");
    assert!(active_block.chars().count() <= context::ACTIVE_BLOCK_MAX_CHARS);
    assert!(active_block.contains("do not start replacements for existing ids"));
}

/// Mirrors `tests/test_context_hook.py::ContextHookTests::test_warning_and_silence_use_events_and_latest_message_time`
///
/// Deadline warnings come from durable events and silence from the latest
/// transcript message, so each is re-injected when it appears and silence clears
/// as soon as the agent speaks again.
#[test]
fn context_warning_and_silence_use_events_and_latest_message_time() {
    let home = common::Home::new();
    let agent_id = active_agent(&home, "summary task", "req-silence");
    let mut store = home.store();
    store.running(&agent_id, std::process::id() as i32).unwrap();
    let started = now();

    let first = context::build(&home.path, &reference(), Some(started + 1.0)).expect("first");
    assert!(first.injected);

    store
        .event(&agent_id, "deadline_warning", &json!({}))
        .unwrap();
    let warned = context::build(&home.path, &reference(), Some(started + 2.0)).expect("warned");
    assert!(warned.injected);
    assert_ne!(warned.context_key, first.context_key);
    assert!(warned.text.contains(" warn"));

    let silent = context::build(&home.path, &reference(), Some(started + 120.0)).expect("silent");
    assert!(silent.injected);
    assert!(silent.text.contains(" silent"));

    store
        .message(&agent_id, "assistant", "progress", None, None)
        .unwrap();
    let spoke = now();
    let recovered = context::build(&home.path, &reference(), Some(spoke + 1.0)).expect("recovered");
    assert!(recovered.injected);
    assert!(
        !recovered.text.contains(" silent"),
        "a fresh message clears silence: {}",
        recovered.text
    );
}
