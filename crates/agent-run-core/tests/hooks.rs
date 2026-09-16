//! Temp-home parity coverage for Python bind and context hook contracts.

mod common;

use agent_run_core::{
    capacity::{self, Key, Pool, Route, Sample, Slice, Topology},
    domain::{now, AgentId, OrchestratorRef, Outcome, Status},
    hooks::{bind, context},
};
use serde_json::json;
use std::collections::BTreeSet;

/// Persist deterministic fresh routes for context-rendering tests.
fn routes(home: &common::Home, entries: &[(&str, f64)]) {
    for (index, (runtime, remaining)) in entries.iter().enumerate() {
        let mut config = std::fs::read_to_string(home.path.join("config.toml")).expect("config");
        let section = format!("[runtimes.{}]", toml::Value::String((*runtime).into()));
        if !config.contains(&section) {
            config.push_str(&format!(
                "\n{section}\nenabled=true\nadapter='claude'\nbinary='/usr/bin/true'\nhome='{}'\nmodels=['fixture']\nlimits_source='none'\n",
                home.path.join("runtime").display()
            ));
            std::fs::write(home.path.join("config.toml"), config).expect("route config");
        }
        let observed_at = agent_run_core::domain::now();
        let key = Key {
            runtime: (*runtime).into(),
            lane: "standard".into(),
            window: "window".into(),
            target: None,
            source: "fixture".into(),
        };
        let pool_id = format!("pool-{runtime}-{index}");
        capacity::persist(
            &home.path,
            &Slice {
                runtime: (*runtime).into(),
                scope_id: "fixture".into(),
                samples: vec![Sample {
                    key: key.clone(),
                    remaining_percent: Some(*remaining),
                    reset_at: None,
                    observed_at: Some(observed_at),
                    valid_until: Some(observed_at + 100_000.0),
                }],
                topology: Topology {
                    pools: vec![Pool {
                        pool_id: pool_id.clone(),
                        keys: BTreeSet::from([key]),
                    }],
                    routes: vec![Route {
                        route_id: format!("route-{index}"),
                        runtime: (*runtime).into(),
                        account: None,
                        quota_lane: "standard".into(),
                        pool_ids: vec![pool_id],
                        reset_credits: None,
                    }],
                },
                observed_at,
                valid_until: observed_at + 100_000.0,
            },
            1_000,
        )
        .expect("fresh context route");
    }
}

/// Change the persisted context budget for a test home.
fn budget(home: &common::Home, chars: usize) {
    let config = std::fs::read_to_string(home.path.join("config.toml")).expect("config");
    let line = format!("context_max_chars={chars}");
    let config = if config
        .lines()
        .any(|value| value.starts_with("context_max_chars="))
    {
        config
            .lines()
            .map(|value| {
                if value.starts_with("context_max_chars=") {
                    line.as_str()
                } else {
                    value
                }
            })
            .collect::<Vec<_>>()
            .join("\n")
    } else {
        format!("{config}\n[capacity]\n{line}\n")
    };
    std::fs::write(home.path.join("config.toml"), config).expect("budget config");
}

/// Admit one active agent bound to the supplied orchestrator reference.
fn active_agent(home: &common::Home, reference: &OrchestratorRef) {
    let mut request = home.request();
    request.orchestrator = Some(reference.clone());
    let mut store = home.store();
    let (id, _) = store
        .admit(&request, &home.config, &json!({}), None)
        .expect("active admission");
    store.running(&id, 1234).expect("active transition");
}

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

/// Mirrors `tests/test_priority_context_regressions.py::ContextRegressionTests::test_route_identity_is_json_safe_and_aliases_collapse_per_route`.
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

/// Mirrors `tests/test_priority_context_regressions.py::ContextRegressionTests::test_active_appearance_keeps_the_same_priority_budget`.
#[test]
fn python_context_active_appearance_keeps_priority_allocation_fixed() {
    let home = common::Home::new();
    let reference = OrchestratorRef {
        transport: "codex_queue".into(),
        external_session_id: "session-1".into(),
        external_turn_id: Some("turn-1".into()),
    };
    routes(&home, &[("alpha", 90.0)]);
    let first = context::build(&home.path, &reference, Some(agent_run_core::domain::now()))
        .expect("priority context");
    let first_priority: String = home
        .store()
        .conn
        .query_row("SELECT context_key FROM context_receipts", [], |row| {
            row.get(0)
        })
        .expect("priority receipt");
    active_agent(&home, &reference);
    let second = context::build(&home.path, &reference, Some(agent_run_core::domain::now()))
        .expect("active context");
    let second_key: String = home
        .store()
        .conn
        .query_row("SELECT context_key FROM context_receipts", [], |row| {
            row.get(0)
        })
        .expect("active receipt");
    let first_priority = serde_json::from_str::<serde_json::Value>(&first_priority).unwrap();
    let second_priority = serde_json::from_str::<serde_json::Value>(&second_key).unwrap();
    assert_eq!(
        first_priority["components"]["priority"],
        second_priority["components"]["priority"]
    );
    assert!(first.injected);
    assert!(second.injected);
    assert!(!second.text.contains("Runtime priorities"));
}

/// Mirrors `tests/test_priority_context_regressions.py::ContextRegressionTests::test_malformed_component_payloads_are_legacy_not_crashes`.
#[test]
fn python_context_malformed_component_receipts_are_replaced_safely() {
    let home = common::Home::new();
    let reference = OrchestratorRef {
        transport: "codex_queue".into(),
        external_session_id: "session-1".into(),
        external_turn_id: None,
    };
    let mut store = home.store();
    for (index, value) in [
        json!([]),
        json!(["x"]),
        json!("x"),
        json!(1),
        json!(true),
        json!({"p":""}),
        json!({"":"hash"}),
    ]
    .into_iter()
    .enumerate()
    {
        store
            .record_context_components_for_ref(
                &reference,
                &std::collections::BTreeMap::from([(format!("component-{index}"), "hash".into())]),
                1.0,
            )
            .expect("legacy receipt replacement");
        let session: String = store
            .conn
            .query_row(
                "SELECT id FROM orchestrator_sessions WHERE transport=? AND external_session_id=?",
                ["codex_queue", "session-1"],
                |row| row.get(0),
            )
            .expect("session");
        store
            .conn
            .execute(
                "UPDATE context_receipts SET context_key=? WHERE orchestrator_session_id=?",
                [value.to_string(), session],
            )
            .expect("malformed receipt");
    }
    assert!(store
        .record_context_components_for_ref(
            &reference,
            &std::collections::BTreeMap::from([(String::from("priority"), String::from("next"))]),
            2.0,
        )
        .is_ok());
}

/// Mirrors `tests/test_priority_context_regressions.py::ContextRegressionTests::test_changed_only_priority_resends_after_returning_to_an_order`.
#[test]
fn python_context_priority_order_changes_are_a_b_a_and_then_silent() {
    let home = common::Home::new();
    let reference = OrchestratorRef {
        transport: "codex_queue".into(),
        external_session_id: "session-1".into(),
        external_turn_id: None,
    };
    routes(&home, &[("alpha", 90.0), ("beta", 50.0)]);
    let first = context::build(&home.path, &reference, Some(agent_run_core::domain::now()))
        .expect("first context");
    assert!(first.injected, "first={:?}", first);
    routes(&home, &[("beta", 90.0), ("alpha", 50.0)]);
    let second = context::build(&home.path, &reference, Some(agent_run_core::domain::now()))
        .expect("second context");
    assert!(second.injected && second.text.contains("beta"));
    routes(&home, &[("alpha", 90.0), ("beta", 50.0)]);
    let third = context::build(&home.path, &reference, Some(agent_run_core::domain::now()))
        .expect("third context");
    assert!(third.injected && third.text.contains("alpha"));
    let silent = context::build(&home.path, &reference, Some(agent_run_core::domain::now()))
        .expect("silent context");
    assert!(!silent.injected && silent.text.is_empty());
}

/// Mirrors `tests/test_priority_context_regressions.py::ContextRegressionTests::test_active_only_change_resends_the_active_block_without_priorities`.
#[test]
fn python_context_active_only_change_omits_priority_block() {
    let home = common::Home::new();
    let reference = OrchestratorRef {
        transport: "codex_queue".into(),
        external_session_id: "session-1".into(),
        external_turn_id: None,
    };
    routes(&home, &[("alpha", 90.0)]);
    active_agent(&home, &reference);
    let first = context::build(&home.path, &reference, Some(agent_run_core::domain::now()))
        .expect("first context");
    let second = context::build(
        &home.path,
        &reference,
        Some(agent_run_core::domain::now() + 1.0),
    )
    .expect("unchanged context");
    assert!(first.injected);
    assert!(!second.injected);
    budget(&home, 120);
    let third = context::build(
        &home.path,
        &reference,
        Some(agent_run_core::domain::now() + 2.0),
    )
    .expect("tight active context");
    assert!(third.injected);
    assert!(third.text.contains("Active agents"));
    assert!(!third.text.contains("Runtime priorities"));
}

/// Mirrors `tests/test_priority_context_regressions.py::ContextRegressionTests::test_zero_budget_writes_no_receipt_and_restored_budget_delivers`.
#[test]
fn python_context_zero_budget_writes_no_receipt_then_restores_delivery() {
    let home = common::Home::new();
    let reference = OrchestratorRef {
        transport: "codex_queue".into(),
        external_session_id: "never-bound".into(),
        external_turn_id: None,
    };
    routes(&home, &[("alpha", 90.0)]);
    budget(&home, 0);
    let empty = context::build(&home.path, &reference, Some(agent_run_core::domain::now()))
        .expect("zero-budget context");
    assert_eq!((empty.text, empty.injected), (String::new(), false));
    assert!(empty.orchestrator_session_id.is_none());
    let store = home.store();
    assert_eq!(
        store
            .conn
            .query_row("SELECT COUNT(*) FROM context_receipts", [], |row| row
                .get::<_, i64>(0))
            .unwrap(),
        0
    );
    budget(&home, 2_500);
    let restored = context::build(&home.path, &reference, Some(agent_run_core::domain::now()))
        .expect("restored context");
    assert!(restored.injected);
    assert!(restored.text.contains("Runtime priorities"));
}

/// Mirrors `tests/test_priority_context_regressions.py::ContextRegressionTests::test_tight_budget_clips_priority_and_growth_delivers_the_full_summary`.
#[test]
fn python_context_tight_budget_hides_then_reveals_priority_lines() {
    let home = common::Home::new();
    let reference = OrchestratorRef {
        transport: "codex_queue".into(),
        external_session_id: "session-1".into(),
        external_turn_id: None,
    };
    routes(&home, &[("alpha", 90.0), ("beta", 50.0), ("gamma", 10.0)]);
    budget(&home, 1_000);
    let clipped = context::build(&home.path, &reference, Some(agent_run_core::domain::now()))
        .expect("clipped context");
    assert!(clipped.injected);
    assert!(clipped.text.contains("More routes omitted"));
    assert!(clipped.text.contains("alpha"));
    assert!(!clipped.text.contains("beta"));
    budget(&home, 2500);
    let grown = context::build(&home.path, &reference, Some(agent_run_core::domain::now()))
        .expect("grown context");
    assert!(grown.injected && grown.text.contains("beta"));
    assert!(!grown.text.contains("More routes omitted"));
}

/// Mirrors `tests/test_priority_context_regressions.py::ContextRegressionTests::test_legacy_digest_receipt_is_replaced_in_place_and_components_preserved`.
#[test]
fn python_context_legacy_receipt_migrates_in_place_and_preserves_components() {
    let home = common::Home::new();
    let reference = OrchestratorRef {
        transport: "codex_queue".into(),
        external_session_id: "session-1".into(),
        external_turn_id: None,
    };
    routes(&home, &[("alpha", 90.0)]);
    active_agent(&home, &reference);
    let first = context::build(&home.path, &reference, Some(agent_run_core::domain::now()))
        .expect("first context");
    let session = first.orchestrator_session_id.expect("session");
    let store = home.store();
    store
        .conn
        .execute(
            "UPDATE context_receipts SET context_key=?,injected_at=500 WHERE orchestrator_session_id=?",
            ["a".repeat(64), session.clone()],
        )
        .expect("legacy receipt");
    let migrated = context::build(&home.path, &reference, Some(1001.0)).expect("migration");
    assert!(migrated.injected);
    let key: String = store
        .conn
        .query_row(
            "SELECT context_key FROM context_receipts WHERE orchestrator_session_id=?",
            [session.as_str()],
            |row| row.get(0),
        )
        .expect("migrated key");
    let value: serde_json::Value = serde_json::from_str(&key).expect("versioned receipt");
    assert_eq!(value["v"], 2);
    assert_eq!(
        value["components"].as_object().expect("components").len(),
        2
    );
}

/// Mirrors `tests/test_priority_context_regressions.py::ContextRegressionTests::test_sessions_and_transports_keep_independent_receipts`.
#[test]
fn python_context_receipts_are_independent_per_transport_and_session() {
    let home = common::Home::new();
    let first = OrchestratorRef {
        transport: "codex_queue".into(),
        external_session_id: "session-1".into(),
        external_turn_id: None,
    };
    let transport = OrchestratorRef {
        transport: "claude_uds".into(),
        ..first.clone()
    };
    let second = OrchestratorRef {
        external_session_id: "session-2".into(),
        ..first.clone()
    };
    routes(&home, &[("alpha", 90.0)]);
    let a = context::build(&home.path, &first, Some(agent_run_core::domain::now())).unwrap();
    let b = context::build(&home.path, &transport, Some(agent_run_core::domain::now())).unwrap();
    let c = context::build(&home.path, &second, Some(agent_run_core::domain::now())).unwrap();
    assert_eq!(
        [
            a.orchestrator_session_id,
            b.orchestrator_session_id,
            c.orchestrator_session_id
        ]
        .into_iter()
        .collect::<std::collections::BTreeSet<_>>()
        .len(),
        3
    );
    let store = home.store();
    assert_eq!(
        store
            .conn
            .query_row("SELECT COUNT(*) FROM context_receipts", [], |row| row
                .get::<_, i64>(0))
            .unwrap(),
        3
    );
}

/// Mirrors `tests/test_priority_context_regressions.py::ContextRegressionTests::test_concurrent_identical_component_receipts_change_exactly_once`.
#[test]
fn python_context_concurrent_identical_receipt_changes_have_one_winner() {
    let home = common::Home::new();
    let reference = OrchestratorRef {
        transport: "codex_queue".into(),
        external_session_id: "session-1".into(),
        external_turn_id: None,
    };
    let first =
        context::build(&home.path, &reference, Some(agent_run_core::domain::now())).unwrap();
    let path = home.path.clone();
    let workers = 4;
    let barrier = std::sync::Arc::new(std::sync::Barrier::new(workers));
    let handles = (0..workers)
        .map(|_| {
            let barrier = barrier.clone();
            let path = path.clone();
            let reference = reference.clone();
            std::thread::spawn(move || {
                let mut store = agent_run_core::state::Store::open(&path).unwrap();
                barrier.wait();
                store
                    .record_context_components_for_ref(
                        &reference,
                        &std::collections::BTreeMap::from([
                            (String::from("priority"), String::from("p-1")),
                            (String::from("active"), String::from("a-1")),
                        ]),
                        1500.0,
                    )
                    .unwrap()
                    .1
            })
        })
        .collect::<Vec<_>>();
    let changed = handles
        .into_iter()
        .map(|handle| handle.join().unwrap())
        .collect::<Vec<_>>();
    assert!(first.orchestrator_session_id.is_some());
    assert_eq!(changed.iter().filter(|names| !names.is_empty()).count(), 1);
}

/// Mirrors `tests/test_priority_context_regressions.py::HookContextCliTests::test_hook_context_delivers_changes_then_an_empty_payload`.
#[test]
fn python_context_hook_cli_delivers_once_then_on_change() {
    let home = common::Home::new();
    let reference = OrchestratorRef {
        transport: "codex_queue".into(),
        external_session_id: "external-cli-1".into(),
        external_turn_id: None,
    };
    routes(&home, &[("alpha", 90.0)]);
    let first =
        context::build(&home.path, &reference, Some(agent_run_core::domain::now())).unwrap();
    let second =
        context::build(&home.path, &reference, Some(agent_run_core::domain::now())).unwrap();
    assert!(first.injected && !second.injected && second.text.is_empty());
    routes(&home, &[("beta", 50.0)]);
    let third =
        context::build(&home.path, &reference, Some(agent_run_core::domain::now())).unwrap();
    assert!(third.injected && third.text.contains("beta"));
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
