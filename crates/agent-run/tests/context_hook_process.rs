//! Real-process advisory context regressions for both native client envelopes.
//! `AGENT_RUN_CONTEXT_TEST_BINARY` optionally selects the exact sealed candidate;
//! ordinary CI uses Cargo's binary. Every home, socket and payload is synthetic.
#[path = "support/test_broker.rs"]
mod test_broker;

use agent_run::{config::Config, domain::OrchestratorRef};
use agent_run_core::{
    storage_client::HelloOk,
    storage_wire::{self as wire, Message},
};
use agent_run_domain::storage::{NotEnqueuedReason, SubmitResult};
use serde_json::{Value, json};
use std::{
    path::{Path, PathBuf},
    process::{Output, Stdio},
    time::{Duration, Instant},
};
use tokio::{io::AsyncWriteExt, net::UnixListener};

/// Selects an explicit packaged binary or the binary built with this test.
fn binary() -> PathBuf {
    std::env::var_os("AGENT_RUN_CONTEXT_TEST_BINARY")
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from(env!("CARGO_BIN_EXE_agent-run")))
}

/// Initializes an empty current-schema home with the tested executable and then
/// configures one synthetic runtime. Never imports real config or credentials.
async fn home() -> tempfile::TempDir {
    let home = tempfile::tempdir().unwrap();
    let initialized = tokio::process::Command::new(binary())
        .args(["--home", home.path().to_str().unwrap(), "init"])
        .output()
        .await
        .unwrap();
    assert!(
        initialized.status.success(),
        "{}",
        String::from_utf8_lossy(&initialized.stderr)
    );
    std::fs::write(home.path().join("config.toml"), format!(
        "schema_version=1\n[runtimes.mock]\nenabled=true\nadapter='claude'\nbinary='/usr/bin/true'\nhome={:?}\nmodels=['fixture']\nlimits_source='none'\n", home.path().join("runtime")
    )).unwrap();
    home
}

/// Records a synthetic running agent, optionally bound to one test host, before
/// the online writer starts. Returns its stable id; no engine is launched.
fn active(home: &Path, transport: Option<&str>) -> String {
    let config = Config::load(home).unwrap();
    let reference = transport.map(|transport| OrchestratorRef {
        transport: transport.into(),
        external_session_id: "context-probe".into(),
        external_turn_id: None,
    });
    let request = serde_json::from_value(json!({"runtime":"mock", "model":"fixture", "profile":"review", "task":"Synthetic hook task", "workdir":home, "orchestrator":reference})).unwrap();
    let mut store = agent_run_store::fixture::open(home).unwrap();
    let (id, _) = store.admit(&request, &config, &json!({}), None).unwrap();
    store.running(&id, 1234).unwrap();
    id.to_string()
}

/// Runs the actual hook with a finite hang guard, draining both output pipes.
/// Only the optional context command is expected to fail open; bind stays strict.
async fn hook(home: &Path, kind: &str, transport: &str, payload: Value) -> Output {
    let mut child = tokio::process::Command::new(binary())
        .args([
            "--home",
            home.to_str().unwrap(),
            "hook",
            kind,
            "--transport",
            transport,
        ])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true)
        .spawn()
        .unwrap();
    child
        .stdin
        .take()
        .unwrap()
        .write_all(payload.to_string().as_bytes())
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(4), child.wait_with_output())
        .await
        .expect("hook must be bounded")
        .unwrap()
}

/// One valid host event containing a private prompt canary that must stay private.
fn prompt() -> Value {
    json!({"hook_event_name":"UserPromptSubmit", "session_id":"context-probe", "prompt":"PRIVATE_PROMPT_CONTEXT_CANARY"})
}

/// Checks exit, JSON, prepared priority and active-agent preservation, and limits.
fn preserved(output: &Output, agent: &str) {
    assert_eq!(
        output.status.code(),
        Some(0),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let value: Value = serde_json::from_slice(&output.stdout).unwrap();
    let text = value["hookSpecificOutput"]["additionalContext"]
        .as_str()
        .expect("prepared context must survive");
    assert!(text.contains("Runtime priorities"), "{text}");
    assert!(text.contains(agent), "{text}");
    assert!(text.chars().count() <= 2500);
    assert!(
        value.get("systemMessage").is_none(),
        "no repeated warning in the chat"
    );
    assert!(!String::from_utf8_lossy(&output.stdout).contains("PRIVATE_PROMPT_CONTEXT_CANARY"));
    assert!(!String::from_utf8_lossy(&output.stderr).contains("PRIVATE_PROMPT_CONTEXT_CANARY"));
}

/// Reads only this synthetic home's bounded operational logs for safe assertions.
fn logs(home: &Path) -> String {
    std::fs::read_dir(home.join("logs"))
        .unwrap()
        .map(|entry| std::fs::read_to_string(entry.unwrap().path()).unwrap())
        .collect::<Vec<_>>()
        .join("\n")
}

/// Missing writer retains both components; restoration resumes changed-only
/// behavior and a later real bind updates the active list for both transports.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn no_writer_retains_context_and_recovery_restores_deduplication() {
    for transport in ["codex_queue", "claude_uds"] {
        let home = home().await;
        let id = active(home.path(), Some(transport));
        let unbound = active(home.path(), None);
        let started = Instant::now();
        preserved(
            &hook(home.path(), "context", transport, prompt()).await,
            &id,
        );
        assert!(started.elapsed() < Duration::from_secs(2));
        let diagnostic = logs(home.path());
        assert!(diagnostic.contains("writer_apply"));
        assert!(!diagnostic.contains("PRIVATE_PROMPT_CONTEXT_CANARY"));
        let broker = test_broker::TestBroker::start(home.path()).await;
        preserved(
            &hook(home.path(), "context", transport, prompt()).await,
            &id,
        );
        let repeated = hook(home.path(), "context", transport, prompt()).await;
        assert_eq!(
            serde_json::from_slice::<Value>(&repeated.stdout).unwrap(),
            json!({})
        );
        let bound = hook(home.path(), "bind", transport, json!({"hook_event_name":"PostToolUse", "session_id":"context-probe", "tool_response":{"agent_id":unbound}})).await;
        assert_eq!(bound.status.code(), Some(0));
        let changed = hook(home.path(), "context", transport, prompt()).await;
        let changed: Value = serde_json::from_slice(&changed.stdout).unwrap();
        let text = changed["hookSpecificOutput"]["additionalContext"]
            .as_str()
            .unwrap();
        assert!(text.contains(&unbound));
        assert!(
            !text.contains("Runtime priorities"),
            "unchanged priority must stay deduplicated"
        );
        broker.stop().await;
    }
}

/// Serves genuine writer frames with a controlled pre-admission refusal.
/// Binding the listener proves readiness; the task exits when the child closes.
fn refusing_writer(home: &Path, reason: NotEnqueuedReason) -> tokio::task::JoinHandle<()> {
    let listener = UnixListener::bind(home.join("writer.sock")).unwrap();
    tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        let hello = wire::read_header(&mut stream, |_, _| Some(wire::HELLO_MAX))
            .await
            .unwrap()
            .unwrap();
        wire::read_body(&mut stream, &hello).await.unwrap();
        let (boot_id, mono_ns) = agent_run_platform::monotonic::now().unwrap();
        wire::write_frame(
            &mut stream,
            Message::HelloOk,
            0,
            hello.seq,
            &serde_json::to_vec(&HelloOk {
                epoch: 1,
                boot_id,
                mono_ns,
            })
            .unwrap(),
        )
        .await
        .unwrap();
        while let Ok(Some(header)) =
            wire::read_header(&mut stream, |_, _| Some(wire::BODY_HARD_MAX)).await
        {
            if wire::read_body(&mut stream, &header).await.is_err() {
                break;
            }
            assert_eq!(
                header.op_kind, 83,
                "only the context write may be attempted"
            );
            if wire::write_frame(
                &mut stream,
                Message::Reply,
                0,
                header.seq,
                &serde_json::to_vec(&SubmitResult::NotEnqueued(reason)).unwrap(),
            )
            .await
            .is_err()
            {
                break;
            }
        }
    })
}

/// Queue pressure and admission gates preserve context and report their actual
/// reason, rather than treating a missing acknowledgement as successful storage.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn admission_refusals_are_bounded_and_keep_the_prepared_context() {
    for transport in ["codex_queue", "claude_uds"] {
        for reason in [
            NotEnqueuedReason::QueueFull,
            NotEnqueuedReason::RetentionGate,
            NotEnqueuedReason::AdmissionsClosed,
        ] {
            let home = home().await;
            let id = active(home.path(), Some(transport));
            let server = refusing_writer(home.path(), reason);
            let started = Instant::now();
            preserved(
                &hook(home.path(), "context", transport, prompt()).await,
                &id,
            );
            assert!(started.elapsed() < Duration::from_secs(2));
            tokio::time::timeout(Duration::from_secs(2), server)
                .await
                .unwrap()
                .unwrap();
            assert!(
                logs(home.path()).contains(
                    &serde_json::to_value(reason)
                        .unwrap()
                        .as_str()
                        .unwrap()
                        .to_string()
                )
            );
        }
    }
}

/// A real writer commits once, but its reply is held at a socket proxy until the
/// caller's original deadline. Fallback must still deliver that same snapshot.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn lost_commit_reply_keeps_context_without_duplicate_write() {
    for transport in ["codex_queue", "claude_uds"] {
        let home = home().await;
        let id = active(home.path(), Some(transport));
        let broker = test_broker::TestBroker::start(home.path()).await;
        let real = home.path().join("actual.sock");
        std::fs::rename(home.path().join("writer.sock"), &real).unwrap();
        let listener = UnixListener::bind(home.path().join("writer.sock")).unwrap();
        let (committed, observed) = tokio::sync::oneshot::channel();
        let (release, released) = tokio::sync::oneshot::channel();
        let proxy = tokio::spawn(async move {
            let (mut child, _) = listener.accept().await.unwrap();
            let mut writer = tokio::net::UnixStream::connect(real).await.unwrap();
            for _ in 0..2 {
                let header = wire::read_header(&mut child, |_, _| Some(wire::BODY_HARD_MAX))
                    .await
                    .unwrap()
                    .unwrap();
                let body = wire::read_body(&mut child, &header).await.unwrap();
                wire::write_frame(
                    &mut writer,
                    header.message,
                    header.op_kind,
                    header.seq,
                    &body,
                )
                .await
                .unwrap();
                let reply = wire::read_header(&mut writer, |_, _| Some(wire::BODY_HARD_MAX))
                    .await
                    .unwrap()
                    .unwrap();
                let body = wire::read_body(&mut writer, &reply).await.unwrap();
                if reply.message == Message::HelloOk {
                    wire::write_frame(&mut child, reply.message, reply.op_kind, reply.seq, &body)
                        .await
                        .unwrap();
                } else {
                    assert!(matches!(
                        serde_json::from_slice::<SubmitResult>(&body).unwrap(),
                        SubmitResult::Committed(_)
                    ));
                    let _ = committed.send(());
                    let _ = released.await;
                    break;
                }
            }
        });
        preserved(
            &hook(home.path(), "context", transport, prompt()).await,
            &id,
        );
        observed.await.unwrap();
        release.send(()).unwrap();
        proxy.await.unwrap();
        let store = agent_run_store::Store::open(home.path()).unwrap();
        let count: i64 = store
            .conn
            .query_row(
                "SELECT count(*) FROM operation_receipts WHERE op_kind='record_context_components'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(count, 1);
        drop(store);
        let diagnostic = logs(home.path());
        assert!(diagnostic.contains("unknown"));
        assert!(diagnostic.contains("writer_operation_id"));
        std::fs::remove_file(home.path().join("writer.sock")).unwrap();
        broker.stop().await;
    }
}

/// Both incompatible schema directions and malformed configuration fail open
/// without migration, context fabrication or a weakened bind exit status.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn preflight_failures_are_nonblocking_and_bind_remains_strict() {
    for transport in ["codex_queue", "claude_uds"] {
        for version in [agent_run_store::VERSION - 1, agent_run_store::VERSION + 1] {
            let home = home().await;
            let db = rusqlite::Connection::open(home.path().join("state.db")).unwrap();
            db.pragma_update(None, "user_version", version).unwrap();
            let output = hook(home.path(), "context", transport, prompt()).await;
            assert_eq!(output.status.code(), Some(0));
            assert_eq!(
                serde_json::from_slice::<Value>(&output.stdout).unwrap(),
                json!({})
            );
            let unchanged: i64 = db
                .pragma_query_value(None, "user_version", |row| row.get(0))
                .unwrap();
            assert_eq!(unchanged, version);
            assert!(logs(home.path()).contains(&format!("\"stored_schema\":{version}")));
            let binding = hook(home.path(), "bind", transport, prompt()).await;
            assert_eq!(binding.status.code(), Some(2));
        }
        let home = home().await;
        std::fs::write(
            home.path().join("config.toml"),
            "schema_version='PRIVATE_CONFIG_CONTEXT_CANARY'\n",
        )
        .unwrap();
        let output = hook(home.path(), "context", transport, prompt()).await;
        assert_eq!(output.status.code(), Some(0));
        assert_eq!(
            serde_json::from_slice::<Value>(&output.stdout).unwrap(),
            json!({})
        );
        assert!(!logs(home.path()).contains("PRIVATE_CONFIG_CONTEXT_CANARY"));
        let malformed = hook(
            home.path(),
            "context",
            transport,
            json!({"prompt":"PRIVATE_PROMPT_CONTEXT_CANARY"}),
        )
        .await;
        assert_eq!(malformed.status.code(), Some(0));
        assert!(!logs(home.path()).contains("PRIVATE_PROMPT_CONTEXT_CANARY"));
    }
}

/// Disabled context does not require a database or create a session, and parser
/// failure stays advisory only for the exact hook-context command prefix.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn disabled_context_and_invalid_hook_arguments_do_not_block() {
    let home = home().await;
    let config = std::fs::read_to_string(home.path().join("config.toml")).unwrap();
    std::fs::write(
        home.path().join("config.toml"),
        format!("{config}\n[capacity]\ncontext_max_chars=0\n"),
    )
    .unwrap();
    std::fs::remove_file(home.path().join("state.db")).unwrap();
    for transport in ["codex_queue", "claude_uds"] {
        let output = hook(home.path(), "context", transport, prompt()).await;
        assert_eq!(output.status.code(), Some(0));
        assert_eq!(
            serde_json::from_slice::<Value>(&output.stdout).unwrap(),
            json!({})
        );
        assert!(!home.path().join("state.db").exists());
    }
    let invalid = hook(
        home.path(),
        "context",
        "PRIVATE_UNKNOWN_TRANSPORT",
        prompt(),
    )
    .await;
    assert_eq!(invalid.status.code(), Some(0));
    assert!(!String::from_utf8_lossy(&invalid.stderr).contains("PRIVATE_UNKNOWN_TRANSPORT"));
    let strict = hook(home.path(), "bind", "PRIVATE_UNKNOWN_TRANSPORT", prompt()).await;
    assert_eq!(strict.status.code(), Some(2));
}

/// Real SQL writer contention cannot discard readable context. Releasing the
/// injected lock restores recording without weakening the normal context CLI.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sqlite_busy_preserves_context_and_the_explicit_context_command_stays_strict() {
    for transport in ["codex_queue", "claude_uds"] {
        let home = home().await;
        let id = active(home.path(), Some(transport));
        let broker = test_broker::TestBroker::start(home.path()).await;
        let lock = rusqlite::Connection::open(home.path().join("state.db")).unwrap();
        lock.execute_batch("BEGIN IMMEDIATE").unwrap();
        preserved(
            &hook(home.path(), "context", transport, prompt()).await,
            &id,
        );
        lock.execute_batch("ROLLBACK").unwrap();
        let restored = hook(home.path(), "context", transport, prompt()).await;
        assert_eq!(restored.status.code(), Some(0));
        // The timed-out operation may commit after release. The first fallback
        // already delivered its context, so either outcome is truthful here.
        if serde_json::from_slice::<Value>(&restored.stdout).unwrap() != json!({}) {
            preserved(&restored, &id);
        }
        let repeated = hook(home.path(), "context", transport, prompt()).await;
        assert_eq!(
            serde_json::from_slice::<Value>(&repeated.stdout).unwrap(),
            json!({})
        );
        broker.stop().await;
        let output = tokio::process::Command::new(binary())
            .args([
                "--home",
                home.path().to_str().unwrap(),
                "context",
                "--session-transport",
                transport,
                "--session-id",
                "context-probe",
            ])
            .output()
            .await
            .unwrap();
        assert_eq!(output.status.code(), Some(2));
        assert!(
            String::from_utf8_lossy(&output.stderr).contains("state persistence operation failed")
        );
    }
}

#[path = "../../agent-run-core/tests/common/bridge.rs"]
mod bridge;

/// Finishes one synthetic fixture without launching an engine. Its notice is
/// explicitly labelled as a transport control, never as a real task result.
fn terminal(home: &Path) -> String {
    let id = active(home, None);
    let mut store = agent_run_store::fixture::open(home).unwrap();
    store
        .conn
        .execute(
            "UPDATE agents SET runtime='hook-context-probe',model='synthetic-control' WHERE id=?",
            [&id],
        )
        .unwrap();
    store
        .finish(
            &id.parse().unwrap(),
            &agent_run::domain::Outcome::failure("synthetic_transport_control"),
            None,
            None,
        )
        .unwrap();
    id
}

/// Real CLI context fallback and binding feed exactly one completion into the
/// production outbox/SDK route for each client. The Bridge endpoint is synthetic;
/// custody is deliberately asserted separately from native receiver acceptance.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn context_failure_does_not_break_binding_or_result_delivery_for_either_client() {
    use bridge_sdk::bridge_protocol::{Destination, Host, NativeAddress};
    for (transport, host) in [
        ("codex_queue", Host::Codex),
        ("claude_uds", Host::ClaudeCode),
    ] {
        let home = home().await;
        let id = terminal(home.path());
        let receiver = bridge::start(home.path(), false);
        let context = hook(home.path(), "context", transport, prompt()).await;
        assert_eq!(context.status.code(), Some(0));
        assert!(serde_json::from_slice::<Value>(&context.stdout).unwrap()["hookSpecificOutput"]["additionalContext"].as_str().unwrap().contains("Runtime priorities"));
        let broker = test_broker::TestBroker::start(home.path()).await;
        let payload = json!({"hook_event_name":"PostToolUse", "session_id":"context-probe", "tool_response":{"agent_id":id}});
        for _ in 0..2 {
            assert_eq!(
                hook(home.path(), "bind", transport, payload.clone())
                    .await
                    .status
                    .code(),
                Some(0)
            );
        }
        let result = agent_run_core::delivery::dispatch_with_batch(home.path(), 10)
            .await
            .unwrap();
        assert_eq!(result.transferred, 1);
        assert_eq!(
            result.delivered, 0,
            "SDK custody is not native acknowledgement"
        );
        {
            let publications = receiver.published.lock().unwrap();
            assert_eq!(publications.len(), 1);
            let Destination::Address { address } = &publications[0].destination else {
                panic!("native address expected")
            };
            let address = NativeAddress::parse(address).unwrap();
            assert_eq!(address.harness, host);
            assert_eq!(address.session, "context-probe");
        }
        assert_eq!(
            agent_run_core::delivery::dispatch_with_batch(home.path(), 10)
                .await
                .unwrap()
                .claimed,
            0
        );
        broker.stop().await;
    }
}

/// Owner-authorized native control using an explicit protected Bridge config and
/// test recipient. Retains a private sender home for independent receiver/receipt
/// verification. Ordinary CI never sends to a real client or reads production config.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires explicit native test recipient and independent receiver acknowledgement"]
async fn native_context_result_delivery_probe() {
    use bridge_sdk::bridge_protocol::{Host, NativeAddress};
    let config_path =
        std::env::var_os("AGENT_RUN_BRIDGE_LIVE_CONFIG").expect("explicit Bridge config");
    let config: agent_run_config::config::BridgeDelivery =
        toml::from_str(&std::fs::read_to_string(config_path).unwrap()).unwrap();
    config.validate().unwrap();
    let address = NativeAddress::parse(
        &std::env::var("AGENT_RUN_BRIDGE_LIVE_RECIPIENT").expect("explicit test recipient"),
    )
    .unwrap();
    assert_eq!(address.host, config.host_id);
    let transport = match address.harness {
        Host::Codex => {
            assert_eq!(config.codex_store.as_ref(), Some(&address.store));
            "codex_queue"
        }
        Host::ClaudeCode => {
            assert_eq!(config.claude_store.as_ref(), Some(&address.store));
            "claude_uds"
        }
        Host::OpenCode => panic!("this native control supports only Codex and Claude Code"),
    };
    let home = home().await.keep();
    println!("native probe sender retained at {}", home.display());
    let id = terminal(home.as_path());
    let original = std::fs::read_to_string(home.as_path().join("config.toml")).unwrap();
    std::fs::write(
        home.as_path().join("config.toml"),
        format!(
            "{original}\n[delivery.bridge]\n{}",
            toml::to_string(&config).unwrap()
        ),
    )
    .unwrap();
    let mut event = prompt();
    event["session_id"] = json!(address.session);
    let context = hook(home.as_path(), "context", transport, event).await;
    assert_eq!(context.status.code(), Some(0));
    assert!(serde_json::from_slice::<Value>(&context.stdout).unwrap()["hookSpecificOutput"]["additionalContext"].is_string());
    let broker = test_broker::TestBroker::start(home.as_path()).await;
    let binding = hook(home.as_path(), "bind", transport, json!({"hook_event_name":"PostToolUse", "session_id":address.session, "tool_response":{"agent_id":id}})).await;
    assert_eq!(binding.status.code(), Some(0));
    let result = agent_run_core::delivery::dispatch_with_batch(home.as_path(), 10)
        .await
        .unwrap();
    assert_eq!(result.transferred, 1);
    assert_eq!(result.delivered, 0);
    assert_eq!(
        agent_run_core::delivery::dispatch_with_batch(home.as_path(), 10)
            .await
            .unwrap()
            .claimed,
        0
    );
    let store = agent_run_store::Store::open(home.as_path()).unwrap();
    let (intent, receipt): (String, String) = store
        .conn
        .query_row(
            "SELECT intent_json,custody_receipt_json FROM bridge_delivery_intents",
            [],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .unwrap();
    let intent: Value = serde_json::from_str(&intent).unwrap();
    let receipt: Value = serde_json::from_str(&receipt).unwrap();
    drop(store);
    std::fs::write(home.as_path().join("native-probe.json"), serde_json::to_vec_pretty(&json!({"agent_id":id, "transport":transport, "intent":intent, "custody_receipt":receipt, "native_acceptance":"requires independent receiver evidence"})).unwrap()).unwrap();
    broker.stop().await;
}
