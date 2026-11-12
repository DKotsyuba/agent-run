//! Initial Claude submission provenance through the real writer and finite
//! shell engines. No provider, credentials or native Claude process is used.

use crate::common;

use agent_run_adapters::{EngineResult, LaunchPlan, io::Process};
use agent_run_core::{Result, domain::Status, stream};
use serde_json::{Value, json};
use std::{collections::BTreeMap, time::Duration};

/// A failure injected before submission or while the fake engine reads stdin.
#[derive(PartialEq)]
enum Scenario {
    Normal,
    JournalRefused,
    OwnershipRefused,
    Cancelled,
    PartialWrite,
    ProviderInitial,
    ExplicitResume,
    AutomaticContinuation,
    NativeSubstitution,
}

/// Runs one input with owned cleanup even on failure/timeout, returning only
/// the stream outcome, durable proof events and bytes read by the fake engine.
async fn submit(
    admitted: &str,
    initial: &str,
    scenario: Scenario,
) -> (Result<EngineResult>, Vec<Value>, Vec<u8>) {
    let home = common::Home::new();
    let mut store = home.store();
    let mut request = home.request();
    request.task = admitted.to_owned();
    let (id, _) = store
        .admit(&request, &home.config, &json!({}), None)
        .unwrap();
    let provider = matches!(
        scenario,
        Scenario::ProviderInitial | Scenario::ExplicitResume | Scenario::AutomaticContinuation
    );
    if scenario == Scenario::AutomaticContinuation {
        store.create_attempt(&id, "exhausted", &json!({})).unwrap();
    }
    if provider {
        let attempt = store.create_attempt(&id, "prepared", &json!({})).unwrap();
        store
            .conn
            .execute(
                "UPDATE attempts SET ownership_active=1 WHERE id=?",
                [attempt],
            )
            .unwrap();
    }
    if scenario == Scenario::JournalRefused {
        store
            .conn
            .execute_batch(
                "CREATE TRIGGER refuse_turn BEFORE INSERT ON events \
             WHEN NEW.kind='native_turn_started' \
             BEGIN SELECT RAISE(ABORT,'fixture turn refusal'); END;",
            )
            .unwrap();
    }
    if scenario == Scenario::Cancelled {
        // Commit cancellation in the same transaction as the proof: the
        // runner must recheck it after the durable await, before sending.
        store
            .conn
            .execute_batch(
                "CREATE TRIGGER cancel_turn AFTER INSERT ON events \
             WHEN NEW.kind='native_turn_started' BEGIN \
             INSERT INTO commands(agent_id,kind,payload_json,state,created_at) \
             VALUES(NEW.agent_id,'cancel','{}','pending',1.0); END;",
            )
            .unwrap();
    }
    let mut logical = store.get(&id).unwrap();
    if provider {
        // Only the stream's provider/attempt distinction is under test; launch
        // authority materialization and the full catalogue have their own tests.
        logical.identity = Some(json!({"provider_identity_version":2}));
    }
    if matches!(
        scenario,
        Scenario::ExplicitResume | Scenario::AutomaticContinuation
    ) {
        logical.parent_agent_id = Some(id.clone());
        logical.resume_of_runtime_session_id = Some("fixture".into());
        logical.request.explicit_finish = true;
    }
    let mut record = logical.clone();
    if matches!(
        scenario,
        Scenario::ExplicitResume | Scenario::AutomaticContinuation | Scenario::NativeSubstitution
    ) {
        record.request.task = serde_json::from_str::<Value>(initial).unwrap()["message"]["content"]
            [0]["text"]
            .as_str()
            .unwrap()
            .to_owned();
    }
    let prep = common::writer::prepare_supervisor(&mut store, &id);
    let harness = common::writer::Harness::start(&home.path);
    let mut persist = harness.supervisor(&home.path, prep, &id).await;
    let read = if scenario == Scenario::PartialWrite {
        "/bin/dd bs=1 count=1 of=received 2>/dev/null; exec 0<&-;"
    } else {
        "IFS= read -r input || exit 0; printf '%s' \"$input\" > received;"
    };
    let script = format!(
        "{read} printf '%s\\n' '{{\"type\":\"result\",\"subtype\":\"success\",\"is_error\":false,\"session_id\":\"fixture\",\"result\":\"done\"}}'"
    );
    let mut process = Process::spawn(&LaunchPlan {
        binary: "/bin/sh".into(),
        args: vec!["-c".into(), script],
        cwd: home.path.clone(),
        environment: BTreeMap::new(),
        initial_input: None,
    })
    .unwrap();
    if scenario == Scenario::OwnershipRefused {
        process.install_ownership_sink(Box::new(|_| {
            Box::pin(async {
                Err(agent_run_core::Error::Runtime(
                    "fixture ownership refusal".into(),
                ))
            })
        }));
    }
    let outcome = tokio::time::timeout(
        Duration::from_secs(20),
        stream::run(&mut process, &mut persist, &record, &logical, Some(initial)),
    )
    .await;
    process.input.take();
    let cleanup = process.owner.cleanup(Duration::from_secs(2)).await;
    process.reap().await;
    assert!(cleanup.unwrap().confirmed);
    let outcome = outcome.expect("finite input submission");
    if scenario != Scenario::JournalRefused {
        persist.flush().await.unwrap();
    }
    let events = store
        .conn
        .prepare("SELECT data_json FROM events WHERE agent_id=? AND kind='native_turn_started' ORDER BY seq")
        .unwrap()
        .query_map([id.as_str()], |row| row.get::<_, String>(0))
        .unwrap()
        .map(|row| serde_json::from_str(&row.unwrap()).unwrap())
        .collect();
    let received = std::fs::read(home.path.join("received")).unwrap_or_default();
    (outcome, events, received)
}

/// Builds the exact single-text native input shape accepted for provenance.
fn input(text: &str) -> String {
    json!({"type":"user","message":{"role":"user","content":[{"type":"text","text":text}]}})
        .to_string()
}

/// The durable turn names exactly the UUID sent to the engine and hashes the
/// untrimmed UTF-8 text, not JSON escapes or the framing newline; no text leaks.
#[tokio::test]
async fn initial_turn_records_exact_sent_uuid_and_text_hash() {
    let text = "  Привет\n\"quoted\"\\tail\t ";
    let (outcome, events, received) = submit(text, &input(text), Scenario::ProviderInitial).await;
    assert_eq!(outcome.unwrap().outcome.status, Status::Succeeded);
    let sent: Value = serde_json::from_slice(&received).unwrap();
    assert_eq!(sent["message"]["content"][0]["text"], text);
    assert_eq!(events.len(), 1);
    let turn = events[0]["turn"].as_str().unwrap();
    assert_eq!(uuid::Uuid::parse_str(turn).unwrap().get_version_num(), 4);
    assert_eq!(sent["uuid"], turn);
    assert_eq!(
        events[0]["input_sha256"],
        agent_run_core::fs::sha256(text.as_bytes())
    );
    assert_eq!(events[0].as_object().unwrap().len(), 2);
    assert!(!events[0].to_string().contains("Привет"));
}

/// Journal/ownership refusal prevents even the first input byte; cancellation
/// after recording intent similarly leaves no native input to mistake for delivery.
#[tokio::test]
async fn initial_turn_refusal_and_cancel_prevent_input() {
    let text = "must not reach engine";
    let initial = input(text);
    let (outcome, events, received) = submit(text, &initial, Scenario::JournalRefused).await;
    assert!(outcome.is_err());
    assert!(events.is_empty());
    assert!(received.is_empty());
    let (outcome, events, received) = submit(text, &initial, Scenario::OwnershipRefused).await;
    assert!(outcome.is_err());
    assert_eq!(events.len(), 1);
    assert!(received.is_empty());
    let (outcome, events, received) = submit(text, &initial, Scenario::Cancelled).await;
    assert_eq!(outcome.unwrap().outcome.status, Status::Cancelled);
    assert_eq!(events.len(), 1, "recording intent is not delivery proof");
    assert!(received.is_empty());
}

/// A partial initial frame is never retried or accepted as a successful task,
/// even when the fake engine emits success; only the submission intent remains.
#[tokio::test]
async fn initial_turn_partial_write_cannot_certify_success() {
    let text = "x".repeat(512 * 1024);
    let (outcome, events, received) = submit(&text, &input(&text), Scenario::PartialWrite).await;
    assert!(outcome.is_err());
    assert_eq!(events.len(), 1);
    assert_eq!(received.len(), 1);
}

/// Historical text and unsupported framed shapes are still sent, but cannot
/// produce an invented single-text continuation proof.
#[tokio::test]
async fn initial_turn_legacy_and_ambiguous_inputs_have_no_proof() {
    let text = json!({"type":"text","text":"one"});
    for initial in [
        "legacy task\n".to_owned(),
        json!({"type":"user","message":{"role":"user","content":"text"}}).to_string(),
        json!({"type":"user","message":{"role":"user","content":[text.clone(),text.clone()]}})
            .to_string(),
        json!({"type":"user","message":{"role":"assistant","content":[text]}}).to_string(),
        json!({"type":"user","message":{"role":"user","content":[{"type":"text","text":3}]}})
            .to_string(),
    ] {
        let (outcome, events, received) = submit("one", &initial, Scenario::Normal).await;
        assert_eq!(outcome.unwrap().outcome.status, Status::Succeeded);
        assert!(!received.is_empty());
        assert!(events.is_empty());
    }
}

/// Neither a replaced frame nor matching forged native metadata can certify a
/// different task; whitespace is part of the immutable admitted input.
#[tokio::test]
async fn initial_turn_substitution_cannot_prove_admitted_task() {
    for scenario in [Scenario::Normal, Scenario::NativeSubstitution] {
        let (outcome, events, received) = submit(" one\n", &input("one"), scenario).await;
        assert_eq!(outcome.unwrap().outcome.status, Status::Succeeded);
        assert!(!received.is_empty());
        assert!(events.is_empty());
    }
}

/// An explicit resume may prefix this child's task with the exact trusted
/// boundary. Automatic attempts never originate proof, even on a text collision.
#[tokio::test]
async fn initial_turn_resume_prefix_and_automatic_control_keep_distinct_proofs() {
    let task = "new admitted task";
    let prefixed = format!(
        "{}\n\n{task}",
        agent_run_domain::worker::EXPLICIT_RESUME_BOUNDARY
    );
    let (outcome, events, received) =
        submit(task, &input(&prefixed), Scenario::ExplicitResume).await;
    assert!(outcome.is_ok());
    assert_eq!(events.len(), 1);
    assert_eq!(
        events[0]["input_sha256"],
        agent_run_core::fs::sha256(prefixed.as_bytes())
    );
    let sent: Value = serde_json::from_slice(&received).unwrap();
    assert_eq!(sent["uuid"], events[0]["turn"]);

    for (admitted, text, scenario) in [
        (
            "different task",
            prefixed.as_str(),
            Scenario::ExplicitResume,
        ),
        (task, prefixed.as_str(), Scenario::NativeSubstitution),
        (
            task,
            agent_run_core::supervisor::CONTINUATION_CONTROL,
            Scenario::AutomaticContinuation,
        ),
        (
            agent_run_core::supervisor::CONTINUATION_CONTROL,
            agent_run_core::supervisor::CONTINUATION_CONTROL,
            Scenario::AutomaticContinuation,
        ),
    ] {
        let (outcome, events, received) = submit(admitted, &input(text), scenario).await;
        assert!(outcome.is_ok());
        assert!(!received.is_empty());
        assert!(events.is_empty());
    }
}
