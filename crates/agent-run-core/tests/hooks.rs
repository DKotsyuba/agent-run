//! Temp-home parity coverage for Python bind and context hook contracts.

use crate::common;

use agent_run_core::{
    domain::{AgentId, OrchestratorRef, Outcome, Status, now},
    hooks::{bind, context},
};
use agent_run_domain::catalog::{AccountRecord, AccountStatus};
use serde_json::json;
use std::{path::Path, time::Duration};
use tokio::net::UnixListener;

/// Configure synthetic providers with registered accounts but no Quota service.
async fn routes(home: &common::Home, entries: &[(&str, f64)]) {
    let root = home.path.display();
    let mut config = format!(
        "schema_version=2\n[harnesses.codex]\nbinary='/usr/bin/true'\nhome='{root}/codex'\n[harnesses.claude-code]\nbinary='/usr/bin/true'\nhome='{root}/claude'\n"
    );
    let mut store = home.store();
    for (provider, multiplier) in entries {
        let account = format!("acct-{provider}");
        let id = account.parse().unwrap();
        if store.account(&id).unwrap().is_none() {
            store
                .register_account(&AccountRecord {
                    account_id: id,
                    auth_family: "openai".parse().unwrap(),
                    secret_ref: format!("named:codex:{account}").parse().unwrap(),
                    status: AccountStatus::Enabled,
                })
                .unwrap();
        }
        config.push_str(&format!("[providers.{provider}]\nharness='codex'\nconnection={{kind='native'}}\nauth_family='openai'\nlimits_source='none'\npriority_multiplier={multiplier:.1}\n[[providers.{provider}.models]]\nid='fixture'\n[[providers.{provider}.bindings]]\nlabel='private-{provider}'\naccount='{account}'\n"));
    }
    std::fs::write(home.path.join("config.toml"), config).expect("provider config");
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

/// Connects a synthetic schema-2 home to a test-owned Quota socket.
fn quota_endpoint(home: &common::Home, socket: &Path) {
    let path = home.path.join("config.toml");
    let config = std::fs::read_to_string(&path).unwrap();
    let endpoint = format!(
        "schema_version=2\n[capacity.quota]\nsocket={}\n",
        toml::Value::String(socket.to_string_lossy().into_owned())
    );
    std::fs::write(path, config.replacen("schema_version=2\n", &endpoint, 1)).unwrap();
}

/// Creates a private short socket path for the local fake service.
fn quota_listener() -> (tempfile::TempDir, UnixListener) {
    let base = std::env::var_os("AGENT_RUN_TEST_SOCKET_TMP")
        .map(std::path::PathBuf::from)
        .unwrap_or_else(std::env::temp_dir);
    let dir = tempfile::Builder::new()
        .prefix("hook-q-")
        .tempdir_in(base)
        .unwrap();
    let listener = UnixListener::bind(dir.path().join("quota.sock")).unwrap();
    (dir, listener)
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

/// Mirrors `tests/test_bind_hook.py::BindHookTests::test_bound_agent_gets_exactly_one_notice_and_a_rebind_never_resurrects_it`.
#[tokio::test]
async fn python_bind_hook_is_idempotent_and_activates_one_delivery() {
    let home = common::Home::new();
    let writer = common::writer::Harness::start(&home.path);
    let _socket = writer.serve_unix(&home.path);
    let (agent_id, _) = home
        .store()
        .admit(&home.request(), &home.config, &json!({}), None)
        .expect("fake durable admission");
    let payload = json!({
        "session_id":"session-1",
        "hook_event_name":"PostToolUse",
        "tool_response":{"content":[{"text":json!({"agent_id":agent_id.to_string()}).to_string()}]}
    });
    let first = bind::run_hook(&home.path, &payload, "codex_queue", Some(5.0))
        .await
        .expect("raw post-tool hook binds");
    home.store()
        .finish(&agent_id, &Outcome::failure("fixture"), None, None)
        .expect("bound terminal receipt");
    let second = bind::run_hook(&home.path, &payload, "codex_queue", Some(6.0))
        .await
        .expect("same target is idempotent");
    assert_eq!(first.session_id, second.session_id);
    let name = agent_run_domain::agent_name(agent_id.as_str());
    let profile = home.request().profile;
    assert!(
        first
            .message()
            .contains(&format!("{name} ({profile}) · {agent_id} is bound"))
    );
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
        &home.path,
        &json!({"agent_id":agent_id,"transport":"codex_queue","external_session_id":"other"}),
        "codex_queue",
        Some(7.0),
    )
    .await
    .expect_err("different session is refused loudly");
    assert!(conflict.to_string().contains("NOT confirmed"));
    assert!(conflict.to_string().contains("immutable"));
}

/// OpenCode root binding reuses the same immutable session, replay and waiting-notice mechanism.
#[tokio::test]
async fn opencode_root_binding_replay_and_context_stay_in_one_session() {
    let home = common::Home::new();
    let reference = OrchestratorRef {
        transport: "opencode".into(),
        external_session_id: "root-session".into(),
        external_turn_id: Some("message-1".into()),
    };
    let mut request = home.request();
    request.orchestrator = Some(reference.clone());
    request.request_id = Some("opencode-start".into());
    let mut store = home.store();
    let (id, created) = store
        .admit(&request, &home.config, &json!({}), None)
        .unwrap();
    assert!(created);
    let (replayed, created) = store
        .admit(&request, &home.config, &json!({}), None)
        .unwrap();
    assert!(!created);
    assert_eq!(replayed, id);
    assert_eq!(store.list(false, 0, 10, Some(&reference)).unwrap().1, 1);
    store
        .finish(&id, &Outcome::failure("fixture"), None, None)
        .unwrap();
    drop(store);
    let writer = common::writer::Harness::start(&home.path);
    let _socket = writer.serve_unix(&home.path);
    let payload = json!({
        "agent_id":id,
        "transport":"opencode",
        "external_session_id":"root-session",
        "external_turn_id":"message-1"
    });
    let first = bind::run_hook(&home.path, &payload, "opencode", Some(5.0))
        .await
        .unwrap();
    let second = bind::run_hook(&home.path, &payload, "opencode", Some(6.0))
        .await
        .unwrap();
    assert_eq!(first.session_id, second.session_id);
    let context = context::build(&home.path, &reference, Some(7.0))
        .await
        .unwrap();
    assert_eq!(
        context.orchestrator_session_id.as_deref(),
        Some(first.session_id.as_str())
    );
    let store = home.store();
    let destinations: Vec<(String, String)> = store.conn.prepare(
        "SELECT o.transport,o.external_session_id FROM deliveries d JOIN orchestrator_sessions o ON o.id=d.orchestrator_session_id WHERE d.agent_id=?"
    ).unwrap().query_map([id.as_str()], |row| Ok((row.get(0)?,row.get(1)?))).unwrap().collect::<rusqlite::Result<_>>().unwrap();
    assert_eq!(
        destinations,
        vec![("opencode".into(), "root-session".into())]
    );
    let mut conflict = payload;
    conflict["external_session_id"] = json!("other-session");
    assert!(
        bind::run_hook(&home.path, &conflict, "opencode", Some(8.0))
            .await
            .is_err()
    );
}

/// Mirrors `tests/test_bind_hook.py::BindHookTests::test_unbound_terminal_agent_never_gets_a_notice_row`.
#[tokio::test]
async fn unbound_terminal_agent_never_gets_a_notice_row() {
    let home = common::Home::new();
    let writer = common::writer::Harness::start(&home.path);
    let _socket = writer.serve_unix(&home.path);
    let (agent_id, _) = home
        .store()
        .admit(&home.request(), &home.config, &json!({}), None)
        .expect("durable admission");
    home.store()
        .finish(&agent_id, &Outcome::failure("fixture"), None, None)
        .expect("terminal transition");

    let store = home.store();
    assert_eq!(
        store.delivery_status(&agent_id).unwrap()["state"],
        "not_created"
    );
    assert_eq!(
        store
            .conn
            .query_row(
                "SELECT COUNT(*) FROM deliveries WHERE agent_id=?",
                [agent_id.as_str()],
                |row| row.get::<_, i64>(0)
            )
            .unwrap(),
        0
    );
}

/// Mirrors Python `test_context_hook.py::test_first_prompt_creates_receipt_dedups_and_reuses_later_binding`.
#[tokio::test]
async fn python_context_hook_is_bounded_changed_only_and_session_scoped() {
    let home = common::Home::new();
    let writer = common::writer::Harness::start(&home.path);
    let _socket = writer.serve_unix(&home.path);
    let reference = OrchestratorRef {
        transport: "codex_queue".into(),
        external_session_id: "session-1".into(),
        external_turn_id: Some("turn-1".into()),
    };
    active_agent(&home, &reference);
    let first = context::build(&home.path, &reference, Some(1000.0))
        .await
        .expect("first context");
    let second = context::build(&home.path, &reference, Some(1001.0))
        .await
        .expect("dedup context");
    assert!(first.injected);
    assert!(
        first.text.starts_with("Active agents (1)"),
        "{}",
        first.text
    );
    assert!(!first.text.contains("schema_version"), "{}", first.text);
    assert!(first.text.chars().count() <= context::CONTEXT_HARD_LIMIT_CHARS);
    assert!(!second.injected);
    assert!(second.text.is_empty());
    assert_eq!(
        first.orchestrator_session_id,
        second.orchestrator_session_id
    );
}

/// Mirrors `tests/test_priority_context_regressions.py::ContextRegressionTests::test_route_identity_is_json_safe_and_aliases_collapse_per_route`.
#[tokio::test]
async fn python_context_hook_rejects_conflicting_raw_agent_ids() {
    let error = bind::normalize(
        &json!({"session_id":"session-1","tool_response":[{"agent_id":"ag-20260916-120000-0123456789"},{"agent_id":"ag-20260916-120001-0123456789"}]}),
        true,
        "codex_queue",
    )
    .expect_err("conflicting ids cannot bind an arbitrary durable agent");
    assert!(error.to_string().contains("conflicting agent_id"));
}

/// Provider configuration alone injects nothing; an active agent injects only
/// the active block, recorded as the sole receipt component.
#[tokio::test]
async fn python_context_active_appearance_keeps_priority_allocation_fixed() {
    let home = common::Home::new();
    let writer = common::writer::Harness::start(&home.path);
    let _socket = writer.serve_unix(&home.path);
    let reference = OrchestratorRef {
        transport: "codex_queue".into(),
        external_session_id: "session-1".into(),
        external_turn_id: Some("turn-1".into()),
    };
    routes(&home, &[("alpha", 90.0)]).await;
    let first = context::build(&home.path, &reference, Some(agent_run_core::domain::now()))
        .await
        .expect("provider-only context");
    assert!(!first.injected && first.text.is_empty(), "{}", first.text);
    let receipts: i64 = home
        .store()
        .conn
        .query_row("SELECT COUNT(*) FROM context_receipts", [], |row| {
            row.get(0)
        })
        .unwrap();
    assert_eq!(receipts, 0, "nothing visible, nothing recorded");
    active_agent(&home, &reference);
    let second = context::build(&home.path, &reference, Some(agent_run_core::domain::now()))
        .await
        .expect("active context");
    let key: String = home
        .store()
        .conn
        .query_row("SELECT context_key FROM context_receipts", [], |row| {
            row.get(0)
        })
        .expect("active receipt");
    let key = serde_json::from_str::<serde_json::Value>(&key).unwrap();
    let components: Vec<_> = key["components"]
        .as_object()
        .unwrap()
        .keys()
        .cloned()
        .collect();
    assert_eq!(components, ["active"]);
    assert!(second.injected);
    assert!(second.text.starts_with("Active agents (1)"));
    assert!(!second.text.contains("Provider capacity order"));
    assert!(!second.text.contains("acct-alpha"));
}

/// Mirrors `tests/test_priority_context_regressions.py::ContextRegressionTests::test_malformed_component_payloads_are_legacy_not_crashes`.
#[tokio::test]
async fn python_context_malformed_component_receipts_are_replaced_safely() {
    let home = common::Home::new();
    let writer = common::writer::Harness::start(&home.path);
    let _socket = writer.serve_unix(&home.path);
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
    assert!(
        store
            .record_context_components_for_ref(
                &reference,
                &std::collections::BTreeMap::from([(
                    String::from("priority"),
                    String::from("next")
                )]),
                2.0,
            )
            .is_ok()
    );
}

/// Provider configuration changes are not Run context: A/B/A edits inject nothing.
#[tokio::test]
async fn context_provider_changes_are_a_b_a_and_then_silent() {
    let home = common::Home::new();
    let writer = common::writer::Harness::start(&home.path);
    let _socket = writer.serve_unix(&home.path);
    let reference = OrchestratorRef {
        transport: "codex_queue".into(),
        external_session_id: "session-1".into(),
        external_turn_id: None,
    };
    for provider in ["alpha", "beta", "alpha"] {
        routes(&home, &[(provider, 90.0)]).await;
        let result = context::build(&home.path, &reference, Some(agent_run_core::domain::now()))
            .await
            .expect("provider-only context");
        assert!(!result.injected && result.text.is_empty(), "{provider}");
    }
}

/// Mirrors `tests/test_priority_context_regressions.py::ContextRegressionTests::test_active_only_change_resends_the_active_block_without_priorities`.
#[tokio::test]
async fn python_context_active_only_change_omits_priority_block() {
    let home = common::Home::new();
    let writer = common::writer::Harness::start(&home.path);
    let _socket = writer.serve_unix(&home.path);
    let reference = OrchestratorRef {
        transport: "codex_queue".into(),
        external_session_id: "session-1".into(),
        external_turn_id: None,
    };
    routes(&home, &[("alpha", 90.0)]).await;
    active_agent(&home, &reference);
    let first = context::build(&home.path, &reference, Some(agent_run_core::domain::now()))
        .await
        .expect("first context");
    let second = context::build(
        &home.path,
        &reference,
        Some(agent_run_core::domain::now() + 1.0),
    )
    .await
    .expect("unchanged context");
    assert!(first.injected);
    assert!(!second.injected);
    budget(&home, 120);
    let third = context::build(
        &home.path,
        &reference,
        Some(agent_run_core::domain::now() + 2.0),
    )
    .await
    .expect("tight active context");
    assert!(third.injected);
    assert!(third.text.contains("Active agents"));
    assert!(!third.text.contains("Provider capacity order"));
}

/// Mirrors `tests/test_priority_context_regressions.py::ContextRegressionTests::test_zero_budget_writes_no_receipt_and_restored_budget_delivers`.
#[tokio::test]
async fn python_context_zero_budget_writes_no_receipt_then_restores_delivery() {
    let home = common::Home::new();
    let writer = common::writer::Harness::start(&home.path);
    let _socket = writer.serve_unix(&home.path);
    let reference = OrchestratorRef {
        transport: "codex_queue".into(),
        external_session_id: "never-bound".into(),
        external_turn_id: None,
    };
    routes(&home, &[("alpha", 90.0)]).await;
    active_agent(&home, &reference);
    budget(&home, 0);
    let empty = context::build(&home.path, &reference, Some(agent_run_core::domain::now()))
        .await
        .expect("zero-budget context");
    assert_eq!((empty.text, empty.injected), (String::new(), false));
    // The active agent's binding created the session; a zero budget records nothing.
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
        .await
        .expect("restored context");
    assert!(restored.injected);
    assert!(restored.text.starts_with("Active agents (1)"));
    assert!(!restored.text.contains("Provider capacity order"));
}

/// A tight budget clips the active block to its slot; growth re-delivers the
/// full summary because the slot is part of the receipt key.
#[tokio::test]
async fn python_context_tight_budget_hides_then_reveals_priority_lines() {
    let home = common::Home::new();
    let writer = common::writer::Harness::start(&home.path);
    let _socket = writer.serve_unix(&home.path);
    let reference = OrchestratorRef {
        transport: "codex_queue".into(),
        external_session_id: "session-1".into(),
        external_turn_id: None,
    };
    routes(&home, &[("alpha", 90.0), ("beta", 50.0), ("gamma", 10.0)]).await;
    active_agent(&home, &reference);
    budget(&home, 60);
    let clipped = context::build(&home.path, &reference, Some(agent_run_core::domain::now()))
        .await
        .expect("clipped context");
    assert!(clipped.injected);
    assert!(clipped.text.chars().count() <= 60, "{}", clipped.text);
    assert!(!clipped.text.contains("do not start replacements"));
    budget(&home, 2500);
    let grown = context::build(&home.path, &reference, Some(agent_run_core::domain::now()))
        .await
        .expect("grown context");
    assert!(grown.injected && grown.text.contains("do not start replacements"));
    assert!(!grown.text.contains("gamma"), "{}", grown.text);
}

/// Mirrors `tests/test_priority_context_regressions.py::ContextRegressionTests::test_legacy_digest_receipt_is_replaced_in_place_and_components_preserved`.
#[tokio::test]
async fn python_context_legacy_receipt_migrates_in_place_and_preserves_components() {
    let home = common::Home::new();
    let writer = common::writer::Harness::start(&home.path);
    let _socket = writer.serve_unix(&home.path);
    let reference = OrchestratorRef {
        transport: "codex_queue".into(),
        external_session_id: "session-1".into(),
        external_turn_id: None,
    };
    routes(&home, &[("alpha", 90.0)]).await;
    active_agent(&home, &reference);
    let first = context::build(&home.path, &reference, Some(agent_run_core::domain::now()))
        .await
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
    let migrated = context::build(&home.path, &reference, Some(1001.0))
        .await
        .expect("migration");
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
    let components = value["components"].as_object().expect("components");
    assert_eq!(components.len(), 1);
    assert!(components.contains_key("active"));
}

/// Mirrors `tests/test_priority_context_regressions.py::ContextRegressionTests::test_sessions_and_transports_keep_independent_receipts`.
#[tokio::test]
async fn python_context_receipts_are_independent_per_transport_and_session() {
    let home = common::Home::new();
    let writer = common::writer::Harness::start(&home.path);
    let _socket = writer.serve_unix(&home.path);
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
    routes(&home, &[("alpha", 90.0)]).await;
    for reference in [&first, &transport, &second] {
        active_agent(&home, reference);
    }
    let a = context::build(&home.path, &first, Some(agent_run_core::domain::now()))
        .await
        .unwrap();
    let b = context::build(&home.path, &transport, Some(agent_run_core::domain::now()))
        .await
        .unwrap();
    let c = context::build(&home.path, &second, Some(agent_run_core::domain::now()))
        .await
        .unwrap();
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
#[tokio::test]
async fn python_context_concurrent_identical_receipt_changes_have_one_winner() {
    let home = common::Home::new();
    let writer = common::writer::Harness::start(&home.path);
    let _socket = writer.serve_unix(&home.path);
    let reference = OrchestratorRef {
        transport: "codex_queue".into(),
        external_session_id: "session-1".into(),
        external_turn_id: None,
    };
    active_agent(&home, &reference);
    let first = context::build(&home.path, &reference, Some(agent_run_core::domain::now()))
        .await
        .unwrap();
    let path = home.path.clone();
    let workers = 4;
    let barrier = std::sync::Arc::new(std::sync::Barrier::new(workers));
    let handles = (0..workers)
        .map(|_| {
            let barrier = barrier.clone();
            let path = path.clone();
            let reference = reference.clone();
            std::thread::spawn(move || {
                let mut store = agent_run_store::fixture::open(&path).unwrap();
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
#[tokio::test]
async fn python_context_hook_cli_delivers_once_then_on_change() {
    let home = common::Home::new();
    let writer = common::writer::Harness::start(&home.path);
    let _socket = writer.serve_unix(&home.path);
    let reference = OrchestratorRef {
        transport: "codex_queue".into(),
        external_session_id: "external-cli-1".into(),
        external_turn_id: None,
    };
    routes(&home, &[("alpha", 90.0)]).await;
    active_agent(&home, &reference);
    let first = context::build(&home.path, &reference, Some(agent_run_core::domain::now()))
        .await
        .unwrap();
    let second = context::build(&home.path, &reference, Some(agent_run_core::domain::now()))
        .await
        .unwrap();
    assert!(first.injected && !second.injected && second.text.is_empty());
    // A provider edit is not a change; another active agent is.
    routes(&home, &[("beta", 50.0)]).await;
    let unchanged = context::build(&home.path, &reference, Some(agent_run_core::domain::now()))
        .await
        .unwrap();
    assert!(!unchanged.injected);
    active_agent(&home, &reference);
    let third = context::build(&home.path, &reference, Some(agent_run_core::domain::now()))
        .await
        .unwrap();
    assert!(third.injected && third.text.starts_with("Active agents (2)"));
}

/// Returns the fixed host session identity used by the hook parity tests.
fn reference() -> OrchestratorRef {
    OrchestratorRef {
        transport: "codex_queue".into(),
        external_session_id: "session-1".into(),
        external_turn_id: Some("turn-1".into()),
    }
}

/// Admits one active agent attributed to the shared host session.
///
/// The configuration is re-read from the home on every call so a test that
/// raises an admission limit before admitting sees that limit.
fn active_agent_row(home: &common::Home, task: &str, request_id: &str) -> AgentId {
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

/// Mirrors `tests/test_bind_hook.py::BindHookTests::test_bind_rejects_arguments_that_are_not_the_declared_contract`
///
/// Python passes a foreign object where a store belongs and a raw mapping where
/// an orchestrator reference belongs; Rust's signature makes both unrepresentable,
/// so the same "only the declared contract is accepted" rule is asserted at the
/// payload boundary that does accept untyped input.
#[tokio::test]
async fn bind_rejects_arguments_that_are_not_the_declared_contract() {
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

/// Mirrors `tests/test_bind_hook.py::BindHookTests::test_bound_agent_gets_exactly_one_notice_and_a_rebind_never_resurrects_it`
///
/// An orchestrator-backed start binds before the agent finishes, so exactly one
/// notice is created by the terminal commit; repeating the identical bind later
/// must not duplicate that row or drag an already delivered notice back.
#[tokio::test]
async fn bound_agent_gets_exactly_one_notice_and_a_rebind_never_resurrects_it() {
    let home = common::Home::new();
    let writer = common::writer::Harness::start(&home.path);
    let _socket = writer.serve_unix(&home.path);
    let (agent_id, _) = home
        .store()
        .admit(&home.request(), &home.config, &json!({}), None)
        .unwrap();
    let mut store = home.store();
    let first = bind::bind(&home.path, agent_id.clone(), reference(), 1.0)
        .await
        .unwrap();
    store
        .finish(&agent_id, &Outcome::failure("fixture"), None, None)
        .unwrap();
    let notices = |store: &agent_run_store::Store| -> Vec<String> {
        let mut statement = store
            .conn
            .prepare("SELECT state FROM deliveries WHERE agent_id=? ORDER BY id")
            .unwrap();

        statement
            .query_map([agent_id.as_str()], |row| row.get::<_, String>(0))
            .unwrap()
            .collect::<rusqlite::Result<Vec<_>>>()
            .unwrap()
    };
    assert_eq!(notices(&store), vec!["pending".to_owned()]);

    store
        .conn
        .execute(
            "UPDATE deliveries SET state='delivered',next_attempt_at=NULL WHERE agent_id=?",
            [agent_id.as_str()],
        )
        .unwrap();
    let repeated = bind::bind(&home.path, agent_id.clone(), reference(), 7.0)
        .await
        .unwrap();
    assert_eq!(repeated.session_id, first.session_id);
    assert_eq!(
        notices(&store),
        vec!["delivered".to_owned()],
        "a repeated bind must neither duplicate nor resurrect the notice"
    );
}

/// Mirrors `tests/test_bind_hook.py::BindHookTests::test_unbound_running_agent_keeps_running_without_a_delivery`
///
/// Binding a live agent only routes its future completion: it must neither
/// disturb the run nor manufacture a notice before there is a terminal result.
#[tokio::test]
async fn unbound_running_agent_keeps_running_without_a_delivery() {
    let home = common::Home::new();
    let writer = common::writer::Harness::start(&home.path);
    let _socket = writer.serve_unix(&home.path);
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
    bind::bind(&home.path, agent_id.clone(), reference(), 5.0)
        .await
        .unwrap();
    assert_eq!(store.get(&agent_id).unwrap().status, Status::Running);
    assert_eq!(
        store.delivery_status(&agent_id).unwrap()["state"],
        "not_created",
        "a running agent has no completion to announce yet"
    );
}

/// Mirrors `tests/test_context_hook.py::ContextHookTests::test_bounds_and_dedup_key_is_stable_until_agent_state_changes`
///
/// The active block stays inside its own reservation, and its receipt key tracks
/// agent state rather than elapsed time, so a quiet turn repeats nothing while a
/// terminal transition still changes the key without re-injecting stale text.
#[tokio::test]
async fn context_bounds_and_dedup_key_is_stable_until_agent_state_changes() {
    let home = common::Home::new();
    let writer = common::writer::Harness::start(&home.path);
    let _socket = writer.serve_unix(&home.path);
    let agent_id = active_agent_row(&home, &format!("summary {}", "x".repeat(100)), "req-bounds");
    let first = context::build(&home.path, &reference(), Some(1000.0))
        .await
        .expect("first context");
    assert!(first.orchestrator_session_id.is_some());
    assert!(first.injected);
    assert!(first.text.contains("Active agents (1)"));
    assert!(first.text.chars().count() <= context::CONTEXT_HARD_LIMIT_CHARS);
    let active_block = first.text.lines().next_back().expect("active block");
    assert!(active_block.chars().count() <= context::ACTIVE_BLOCK_MAX_CHARS);
    assert!(active_block.contains("summary"));
    assert!(active_block.contains("do not start replacements for existing ids"));

    let second = context::build(&home.path, &reference(), Some(1030.0))
        .await
        .expect("quiet turn");
    assert!(!second.injected);
    assert!(second.text.is_empty());
    assert_eq!(second.context_key, first.context_key);

    home.store()
        .finish(&agent_id, &Outcome::failure("fixture"), None, None)
        .unwrap();
    let third = context::build(&home.path, &reference(), Some(1041.0))
        .await
        .expect("terminal turn");
    assert!(!third.injected);
    assert_ne!(third.context_key, first.context_key);
    assert!(!third.text.contains("Active agents"));
}

/// Mirrors `tests/test_context_hook.py::ContextHookTests::test_context_budget_is_never_exceeded_with_many_active_agents`
///
/// A crowded session is summarized, not truncated at the host: the block names a
/// bounded number of agents, reports the remainder, and still fits both the hard
/// limit and the active reservation.
#[tokio::test]
async fn context_budget_is_never_exceeded_with_many_active_agents() {
    let home = common::Home::new();
    let writer = common::writer::Harness::start(&home.path);
    let _socket = writer.serve_unix(&home.path);
    // The crowd itself is the fixture, so the admission ceiling must not be the
    // thing under test: raise it above the twelve agents this contract needs.
    let path = home.path.join("config.toml");
    let original = std::fs::read_to_string(&path).unwrap();
    std::fs::write(&path, format!("{original}\n[core]\nmax_active_agents=32\n")).unwrap();
    for index in 0..12 {
        active_agent_row(&home, &format!("task {index}"), &format!("req-{index}"));
    }
    let result = context::build(&home.path, &reference(), Some(2000.0))
        .await
        .expect("crowded context");
    assert!(result.text.contains("Active agents (12)"));
    assert!(result.text.contains("more"));
    assert!(result.text.chars().count() <= context::CONTEXT_HARD_LIMIT_CHARS);
    let active_block = result.text.lines().next_back().expect("active block");
    assert!(active_block.chars().count() <= context::ACTIVE_BLOCK_MAX_CHARS);
    assert!(active_block.contains("do not start replacements for existing ids"));
}

/// Historical deadline events no longer change active hints; observed silence
/// still clears immediately when a fresh transcript message arrives.
#[tokio::test]
async fn context_ignores_deadline_warnings_and_tracks_latest_message_time() {
    let home = common::Home::new();
    let writer = common::writer::Harness::start(&home.path);
    let _socket = writer.serve_unix(&home.path);
    let agent_id = active_agent_row(&home, "summary task", "req-silence");
    let mut store = home.store();
    store.running(&agent_id, std::process::id() as i32).unwrap();
    let started = now();

    let first = context::build(&home.path, &reference(), Some(started + 1.0))
        .await
        .expect("first");
    assert!(first.injected);

    store
        .event(&agent_id, "deadline_warning", &json!({}))
        .unwrap();
    let warned = context::build(&home.path, &reference(), Some(started + 2.0))
        .await
        .expect("warned");
    assert!(!warned.injected);
    assert_eq!(warned.context_key, first.context_key);
    assert!(!warned.text.contains(" warn"));

    let silent = context::build(&home.path, &reference(), Some(started + 120.0))
        .await
        .expect("silent");
    assert!(silent.injected);
    assert!(silent.text.contains(" silent"));

    store
        .message(&agent_id, "assistant", "progress", None, None)
        .unwrap();
    let spoke = now();
    let recovered = context::build(&home.path, &reference(), Some(spoke + 1.0))
        .await
        .expect("recovered");
    assert!(recovered.injected);
    assert!(
        !recovered.text.contains(" silent"),
        "a fresh message clears silence: {}",
        recovered.text
    );
}

/// A legacy whole-pool receipt or normalized `pool_id` binding no longer
/// binds anything: the hook names no agent and refuses loudly.
#[tokio::test]
async fn legacy_pool_receipts_never_bind() {
    let home = common::Home::new();
    let pool = "pool-20260101-000000-0123456789";
    let receipt = json!({"hook_event_name":"PostToolUse","session_id":"chat-1",
        "tool_response":{"structuredContent":{"pool_id":pool}}});
    assert!(bind::normalize(&receipt, true, "claude_uds").is_err());
    assert!(
        bind::run_hook(&home.path, &receipt, "claude_uds", Some(2.0))
            .await
            .is_err()
    );
    assert!(
        bind::normalize(
            &json!({"transport":"claude_uds","external_session_id":"s","pool_id":pool}),
            true,
            "claude_uds"
        )
        .is_err()
    );
}

/// The hook never contacts Quota, even when a Quota socket is configured, and
/// a zero budget performs no read; the active block is the whole context.
#[tokio::test]
async fn quota_hook_timeout_preserves_active_and_zero_budget_makes_no_request() {
    let home = common::Home::new();
    let reference = reference();
    routes(&home, &[("codex", 1.0)]).await;
    active_agent(&home, &reference);
    let writer = common::writer::Harness::start(&home.path);
    let _socket = writer.serve_unix(&home.path);
    let (dir, listener) = quota_listener();
    quota_endpoint(&home, &dir.path().join("quota.sock"));
    budget(&home, 0);
    let disabled = context::build_for_hook(&home.path, &reference, None)
        .await
        .unwrap();
    assert!(!disabled.injected);
    budget(&home, 2500);
    let result = context::build_for_hook(&home.path, &reference, None)
        .await
        .unwrap();
    assert!(
        result.text.starts_with("Active agents (1)"),
        "{}",
        result.text
    );
    for removed in [
        "Provider capacity order",
        "\"fixture\":",
        "quota",
        "acct-codex",
    ] {
        assert!(!result.text.contains(removed), "{removed}: {}", result.text);
    }
    assert!(
        tokio::time::timeout(Duration::from_millis(100), listener.accept())
            .await
            .is_err(),
        "the hook context must not read Quota"
    );
}

/// An unreadable active-agent component is reported as unavailable in
/// advisory context, while strict operator context still reports the
/// storage failure rather than success.
#[tokio::test]
async fn advisory_context_preserves_independently_available_components() {
    let home = common::Home::new();
    let reference = OrchestratorRef {
        transport: "claude_uds".into(),
        external_session_id: "partial-context".into(),
        external_turn_id: None,
    };
    routes(&home, &[("alpha", 1.0)]).await;
    active_agent(&home, &reference);
    home.store()
        .conn
        .execute_batch("DROP TABLE messages")
        .unwrap();
    let writer = common::writer::Harness::start(&home.path);
    let _socket = writer.serve_unix(&home.path);
    let result = context::build_for_hook(&home.path, &reference, None)
        .await
        .unwrap();
    assert!(result.injected);
    assert!(
        result.text.contains("Active-agent state unavailable"),
        "{}",
        result.text
    );
    assert!(!result.text.contains("Provider capacity order"));
    assert!(context::build(&home.path, &reference, None).await.is_err());
}

/// A deliberately expensive read is interrupted by SQLite itself, preserving
/// the active component without leaving a detached blocking worker behind.
#[tokio::test]
async fn advisory_context_bounds_sql_work_and_keeps_the_prepared_active_block() {
    let home = common::Home::new();
    let reference = OrchestratorRef {
        transport: "codex_queue".into(),
        external_session_id: "slow-context".into(),
        external_turn_id: None,
    };
    routes(&home, &[("alpha", 1.0)]).await;
    active_agent(&home, &reference);
    home.store()
        .conn
        .execute_batch(
            "DROP TABLE binding_cooldowns;
        CREATE VIEW binding_cooldowns AS WITH RECURSIVE n(x) AS
        (VALUES(1) UNION ALL SELECT x+1 FROM n WHERE x<1000000000)
        SELECT 'alpha' AS provider_id,'acct-alpha' AS account_id,
        'billing_unavailable' AS reason,2000000000+x AS blocked_until,0 AS observed_at FROM n;",
        )
        .unwrap();
    let writer = common::writer::Harness::start(&home.path);
    let _socket = writer.serve_unix(&home.path);
    let started = std::time::Instant::now();
    let result = context::build_for_hook(&home.path, &reference, None)
        .await
        .unwrap();
    assert!(started.elapsed() < std::time::Duration::from_secs(2));
    assert!(result.text.contains("Active agents (1)"));
    assert!(!result.text.contains("Provider capacity order"));
    assert_eq!(
        home.store()
            .conn
            .query_row(
                "SELECT count(*) FROM operation_receipts WHERE op_kind='record_context_components'",
                [],
                |row| row.get::<_, i64>(0)
            )
            .unwrap(),
        1
    );
}
/// A refused first start can use the same post-tool hook without an agent ID.
#[test]
fn restart_waiter_uses_the_host_session() {
    use agent_run_core::hooks::bind::restart_waiter;
    let receipt = serde_json::json!({"accepted":false,"agent_run_restart_id":7,"message":"wait"});
    let hook = serde_json::json!({
        "hook_event_name":"PostToolUse", "session_id":"session-one",
        "tool_response":{"content":[{"type":"text","text":receipt.to_string()}]}
    });
    let (id, reference) = restart_waiter(&hook, "codex_queue").unwrap().unwrap();
    assert_eq!(id, 7);
    assert_eq!(reference.external_session_id, "session-one");
    let mut conflict = hook.clone();
    conflict["tool_response"]["other"] =
        serde_json::json!({"accepted":false,"agent_run_restart_id":8});
    assert!(restart_waiter(&conflict, "codex_queue").is_err());
}

/// The real broker path stores a first-time blocked caller without an agent.
#[tokio::test]
async fn restart_waiter_hook_records_a_first_time_caller() {
    let home = common::Home::new();
    let writer = common::writer::Harness::start(&home.path);
    let service =
        agent_run_core::service::Service::new(home.path.clone()).with_writer(writer.broker());
    let restart = service.begin_service_restart("hook-restart").await.unwrap();
    let payload = json!({
        "hook_event_name":"PostToolUse", "session_id":"first-session",
        "tool_response":{"accepted":false,"agent_run_restart_id":restart}
    });
    for _ in 0..2 {
        agent_run_core::dispatch::call(
            &service,
            "_hook_bind",
            json!({"payload":payload,"transport":"codex_queue"}),
        )
        .await
        .unwrap();
    }
    let store = home.store();
    assert_eq!(store.conn.query_row(
        "SELECT COUNT(*) FROM service_restart_recipients WHERE restart_id=? AND external_session_id='first-session' AND tried_start=1",
        [restart], |row| row.get::<_,i64>(0),
    ).unwrap(), 1);
    assert_eq!(
        store
            .conn
            .query_row("SELECT COUNT(*) FROM agents", [], |row| row
                .get::<_, i64>(0))
            .unwrap(),
        0
    );
}
