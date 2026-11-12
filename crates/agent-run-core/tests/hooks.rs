//! Temp-home parity coverage for Python bind and context hook contracts.

use crate::common;

use agent_run_core::{
    domain::{AgentId, OrchestratorRef, Outcome, Status, now},
    hooks::{bind, context},
};
use agent_run_domain::catalog::{AccountRecord, AccountStatus};
use serde_json::{Value, json};
use std::{path::Path, time::Duration};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::UnixListener,
};

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

/// Moves every fixture timestamp by the same amount without changing quota facts.
fn shift_quota_times(value: &mut Value, offset: f64) {
    match value {
        Value::Object(fields) => {
            for (key, value) in fields {
                if matches!(
                    key.as_str(),
                    "at" | "evaluated_at"
                        | "observed_at"
                        | "newest_observed_at"
                        | "valid_until"
                        | "reset_at"
                        | "last_reset_at"
                        | "exhausted_until"
                ) {
                    if let Some(time) = value.as_f64() {
                        *value = json!(time + offset);
                    }
                } else {
                    shift_quota_times(value, offset);
                }
            }
        }
        Value::Array(values) => values.iter_mut().for_each(|v| shift_quota_times(v, offset)),
        _ => {}
    }
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
    assert!(first.message().contains("is bound"));
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
    let first = context::build(&home.path, &reference, Some(1000.0))
        .await
        .expect("first context");
    let second = context::build(&home.path, &reference, Some(1001.0))
        .await
        .expect("dedup context");
    assert!(first.injected);
    assert!(
        first
            .text
            .contains("migrate configuration to schema_version 2")
    );
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

/// Mirrors `tests/test_priority_context_regressions.py::ContextRegressionTests::test_active_appearance_keeps_the_same_priority_budget`.
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
        .await
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
    assert!(first.text.contains("\"fixture\":unknown"));
    assert!(!first.text.contains("acct-alpha"));
    assert!(second.injected);
    assert!(!second.text.contains("Provider capacity order"));
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

/// Priority changes are delivered again when the provider configuration returns to its prior state.
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
    routes(&home, &[("alpha", 90.0)]).await;
    let first = context::build(&home.path, &reference, Some(agent_run_core::domain::now()))
        .await
        .expect("first context");
    assert!(first.injected, "first={:?}", first);
    routes(&home, &[("beta", 90.0)]).await;
    let second = context::build(&home.path, &reference, Some(agent_run_core::domain::now()))
        .await
        .expect("second context");
    assert!(second.injected && second.text.contains("beta"));
    routes(&home, &[("alpha", 90.0)]).await;
    let third = context::build(&home.path, &reference, Some(agent_run_core::domain::now()))
        .await
        .expect("third context");
    assert!(third.injected && third.text.contains("alpha"));
    let silent = context::build(&home.path, &reference, Some(agent_run_core::domain::now()))
        .await
        .expect("silent context");
    assert!(!silent.injected && silent.text.is_empty());
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
    budget(&home, 0);
    let empty = context::build(&home.path, &reference, Some(agent_run_core::domain::now()))
        .await
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
        .await
        .expect("restored context");
    assert!(restored.injected);
    assert!(restored.text.contains("Provider capacity order"));
}

/// Mirrors `tests/test_priority_context_regressions.py::ContextRegressionTests::test_tight_budget_clips_priority_and_growth_delivers_the_full_summary`.
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
    budget(&home, 940);
    let clipped = context::build(&home.path, &reference, Some(agent_run_core::domain::now()))
        .await
        .expect("clipped context");
    assert!(clipped.injected);
    assert!(clipped.text.contains("More routes omitted"));
    assert!(clipped.text.contains("alpha"));
    assert!(!clipped.text.contains("gamma"));
    budget(&home, 2500);
    let grown = context::build(&home.path, &reference, Some(agent_run_core::domain::now()))
        .await
        .expect("grown context");
    assert!(grown.injected && grown.text.contains("gamma"));
    assert!(!grown.text.contains("More routes omitted"));
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
    assert_eq!(
        value["components"].as_object().expect("components").len(),
        2
    );
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
    let first = context::build(&home.path, &reference, Some(agent_run_core::domain::now()))
        .await
        .unwrap();
    let second = context::build(&home.path, &reference, Some(agent_run_core::domain::now()))
        .await
        .unwrap();
    assert!(first.injected && !second.injected && second.text.is_empty());
    routes(&home, &[("beta", 50.0)]).await;
    let third = context::build(&home.path, &reference, Some(agent_run_core::domain::now()))
        .await
        .unwrap();
    assert!(third.injected && third.text.contains("beta"));
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

/// A structured `start_pool` receipt binds the whole pool and every current
/// member tip atomically and activates the pool's waiting common notice once;
/// a conflicting member binding changes nothing; other pool replies and a lone
/// member binding never bind the pool.
#[tokio::test]
async fn start_pool_receipt_binds_the_whole_pool_atomically() {
    let home = common::Home::new();
    let writer = common::writer::Harness::start(&home.path);
    let _socket = writer.serve_unix(&home.path);
    let config = agent_run_config::config::Config::load(&home.path).unwrap();
    let mut request = home.request();
    request.workdir = home.path.clone();
    request.validate().unwrap();
    let mut store = agent_run_store::fixture::open(&home.path).unwrap();
    let members: Vec<AgentId> = (0..2)
        .map(|_| store.admit(&request, &config, &json!({}), None).unwrap().0)
        .collect();
    let pool = "pool-20260101-000000-0123456789";
    store
        .conn
        .execute(
            "INSERT INTO pools(id,request_namespace,request_id,request_sha256,goal,acceptance_json,state,roster_revision,created_at) VALUES(?,'ns','r',lower(hex(zeroblob(32))),'goal','[]','open',1,1.0)",
            [pool],
        )
        .unwrap();
    for (slot, id) in members.iter().enumerate() {
        store
            .conn
            .execute(
                "INSERT INTO pool_members(agent_id,pool_id,slot,name,role,personal_task,joined_roster_revision) VALUES(?,?,?,?,'r','t',1)",
                rusqlite::params![id.as_str(), pool, slot + 1, format!("m{slot}")],
            )
            .unwrap();
    }
    let receipt = |tool_response: serde_json::Value| json!({"hook_event_name":"PostToolUse","session_id":"chat-1","tool_response":tool_response});
    // Replies of the other pool tools, and a roster-less object, never bind.
    for response in [
        json!({"pool_id": pool, "created": true, "state": "open"}),
        json!({"content":[{"type":"text","text":"agent-run pool"}]}),
    ] {
        assert!(
            bind::normalize(&receipt(response), true, "claude_uds")
                .map(|p| p.pool_id)
                .unwrap_or(None)
                .is_none()
        );
    }
    // A conflicting binding of one member makes the pool binding fail whole.
    let other = OrchestratorRef {
        transport: "claude_uds".into(),
        external_session_id: "someone-else".into(),
        external_turn_id: None,
    };
    store.bind_orchestrator(&members[1], &other, 1.0).unwrap();
    let structured = json!({"structuredContent": {"pool_id": pool}});
    assert!(
        bind::run_hook_bound(
            &home.path,
            &receipt(structured.clone()),
            "claude_uds",
            Some(2.0)
        )
        .await
        .is_err()
    );
    let bound: i64 = store
        .conn
        .query_row(
            "SELECT COUNT(*) FROM pools WHERE orchestrator_session_id IS NOT NULL",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(bound, 0, "a failed whole-pool binding changes nothing");
    let first_bound: Option<String> = store
        .conn
        .query_row(
            "SELECT orchestrator_session_id FROM agents WHERE id=?",
            [members[0].as_str()],
            |r| r.get(0),
        )
        .unwrap();
    assert!(first_bound.is_none());
    // Without the conflict the structured receipt binds everything.
    store
        .conn
        .execute(
            "UPDATE agents SET orchestrator_session_id=NULL WHERE id=?",
            [members[1].as_str()],
        )
        .unwrap();
    let done = bind::run_hook_bound(&home.path, &receipt(structured), "claude_uds", Some(3.0))
        .await
        .unwrap();
    assert!(matches!(&done, bind::HookBound::Pool(p) if p.pool_id == pool));
    assert!(done.message().contains("all its members are bound"));
    let sessions: i64 = store
        .conn
        .query_row(
            "SELECT COUNT(DISTINCT orchestrator_session_id) FROM agents WHERE orchestrator_session_id IS NOT NULL",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(sessions, 1);
    // The raw normalized form accepts exactly one of pool_id or agent_id.
    assert!(bind::normalize(
        &json!({"transport":"claude_uds","external_session_id":"s","pool_id":pool,"agent_id":members[0]}),
        true,
        "claude_uds"
    )
    .is_err());
}

/// A slow or missing Quota response stays unknown and cannot consume the active block.
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
    assert!(
        tokio::time::timeout(Duration::from_millis(40), listener.accept())
            .await
            .is_err()
    );
    budget(&home, 2500);
    let (release, released) = tokio::sync::oneshot::channel();
    let server = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        let mut request = Vec::new();
        stream.read_to_end(&mut request).await.unwrap();
        released.await.unwrap();
        assert!(
            tokio::time::timeout(Duration::from_millis(50), listener.accept())
                .await
                .is_err()
        );
        request
    });
    let started = std::time::Instant::now();
    let result = context::build_for_hook(&home.path, &reference, None)
        .await
        .unwrap();
    assert!(started.elapsed() < Duration::from_secs(2));
    assert!(result.text.contains("Active agents (1)"));
    assert!(
        result.text.contains("\"fixture\":unknown"),
        "{}",
        result.text
    );
    assert!(
        result
            .text
            .contains("quota cannot be determined from available data")
    );
    assert!(!result.text.contains("acct-codex"));
    assert!(!result.text.contains("private-codex"));
    release.send(()).unwrap();
    let request = tokio::time::timeout(Duration::from_secs(2), server)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        serde_json::from_slice::<Value>(&request).unwrap()["method"],
        "snapshot"
    );
}

/// The actual socket snapshot supplies standings and respects explicit and live clocks.
#[tokio::test]
async fn quota_hook_uses_validated_snapshot_and_clock_after_fetch() {
    let home = common::Home::new();
    routes(&home, &[("codex", 1.0)]).await;
    let writer = common::writer::Harness::start(&home.path);
    let _socket = writer.serve_unix(&home.path);
    let (dir, listener) = quota_listener();
    quota_endpoint(&home, &dir.path().join("quota.sock"));
    let server = tokio::spawn(async move {
        for live in [false, false, true] {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut request = Vec::new();
            stream.read_to_end(&mut request).await.unwrap();
            let mut frame: Value = serde_json::from_str(
                &include_str!("fixtures/quota-client-v1/snapshot-v2.json")
                    .replace("acct-test", "acct-codex")
                    .replace("model-a", "fixture"),
            )
            .unwrap();
            if live {
                tokio::time::sleep(Duration::from_millis(25)).await;
                shift_quota_times(&mut frame, now() - 10000.0);
            }
            let mut bytes = serde_json::to_vec(&frame).unwrap();
            bytes.push(b'\n');
            stream.write_all(&bytes).await.unwrap();
        }
    });
    let reference = reference();
    let before = context::build(&home.path, &reference, Some(9999.0))
        .await
        .unwrap();
    assert!(
        before.text.contains("\"fixture\":unknown"),
        "{}",
        before.text
    );
    let current = context::build(&home.path, &reference, Some(10002.0))
        .await
        .unwrap();
    assert!(
        current.text.contains("\"fixture\":available"),
        "{}",
        current.text
    );
    assert!(!current.text.contains("acct-codex"));
    let live_reference = OrchestratorRef {
        external_session_id: "live-clock".into(),
        ..reference
    };
    let live = context::build(&home.path, &live_reference, None)
        .await
        .unwrap();
    assert!(live.text.contains("\"fixture\":available"), "{}", live.text);
    tokio::time::timeout(Duration::from_secs(2), server)
        .await
        .unwrap()
        .unwrap();
}

/// A failed independent SQL component cannot discard the available component;
/// strict operator context still reports the storage failure rather than success.
#[tokio::test]
async fn advisory_context_preserves_independently_available_components() {
    for broken_table in ["binding_cooldowns", "messages"] {
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
            .execute_batch(&format!("DROP TABLE {broken_table}"))
            .unwrap();
        let writer = common::writer::Harness::start(&home.path);
        let _socket = writer.serve_unix(&home.path);
        let result = context::build_for_hook(&home.path, &reference, None)
            .await
            .unwrap();
        assert!(result.injected);
        if broken_table == "binding_cooldowns" {
            assert!(result.text.contains("Active agents (1)"));
            assert!(!result.text.contains("Provider capacity order"));
        } else {
            assert!(result.text.contains("Provider capacity order"));
            assert!(!result.text.contains("Active agents"));
        }
        assert!(context::build(&home.path, &reference, None).await.is_err());
    }
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
        'billing_unavailable' AS reason,2000000000+x AS blocked_until FROM n;",
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
