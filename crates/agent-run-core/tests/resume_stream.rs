//! Native continuation identity and resume dispatch-authority contracts.
//!
//! These tests use a file-backed fake engine and temporary homes only; none of
//! them need a Unix socket or a real provider.

use crate::common;

use agent_run_adapters::{LaunchPlan, io::Process};
use agent_run_core::{dispatch, domain::Status, service::Service, stream};
use serde_json::json;
use std::{collections::BTreeMap, path::Path};

/// Builds a one-shot engine double that emits exactly `stream` and then exits.
///
/// The stream is written to a file and replayed with `cat`, so an arbitrary JSON
/// payload needs no shell quoting and the child closes its stdout immediately
/// after the record, which is the stream shape a one-shot launch produces.
fn fake_engine(directory: &Path, stream: &str) -> LaunchPlan {
    let script = directory.join("engine-stream.jsonl");
    std::fs::write(&script, format!("{stream}\n")).expect("fake engine stream");
    LaunchPlan {
        binary: "/bin/cat".into(),
        args: vec![script.display().to_string()],
        cwd: directory.to_path_buf(),
        environment: BTreeMap::new(),
        initial_input: None,
    }
}

/// Mirrors `tests/test_resume_adapters.py::StreamIdentityTests::test_resume_refuses_missing_or_wrong_stream_identity`
///
/// An otherwise successful result cannot certify a continuation unless it names
/// the exact session that was requested: a missing or foreign identity fails the
/// run instead of silently accepting work from another native context.
#[tokio::test]
async fn resume_refuses_missing_or_wrong_stream_identity() {
    for identity in [None, Some("other"), Some("saved")] {
        let home = common::Home::new();
        let (id, _) = home
            .store()
            .admit(&home.request(), &home.config, &json!({}), None)
            .unwrap();
        let mut store = home.store();
        store
            .conn
            .execute(
                "UPDATE agents SET resume_of_runtime_session_id='saved' WHERE id=?",
                [id.as_str()],
            )
            .unwrap();
        let record = store.get(&id).unwrap();
        let prep = common::writer::prepare_supervisor(&mut store, &id);
        let harness = common::writer::Harness::start(&home.path);
        let mut persist = harness.supervisor(&home.path, prep, &id).await;
        let mut result = json!({
            "type": "result",
            "subtype": "success",
            "is_error": false,
            "result": "answer",
            "usage": {},
        });
        if let Some(identity) = identity {
            result["session_id"] = json!(identity);
        }
        let mut process =
            Process::spawn(&fake_engine(&home.path, &result.to_string())).expect("fake engine");
        let outcome = stream::run(&mut process, &mut persist, &record, &record, None).await;
        persist.flush().await.unwrap();
        if identity == Some("saved") {
            let confirmed = outcome.expect("the requested session is confirmed");
            assert_eq!(confirmed.outcome.status, Status::Succeeded);
            assert_eq!(
                confirmed.outcome.runtime_session_id.as_deref(),
                Some("saved")
            );
        } else {
            assert!(
                outcome.is_err(),
                "identity {identity:?} must not certify the resumed run"
            );
        }
    }
}

/// SessionStart lifecycle events can carry Claude's provisional session ID.
#[tokio::test]
async fn session_start_hooks_do_not_supply_resume_identity() {
    for (resume, foreign_frame) in [
        (true, None),
        (true, Some("init")),
        (true, Some("assistant")),
        (true, Some("result")),
        (false, None),
    ] {
        let home = common::Home::new();
        let mut store = home.store();
        let (id, _) = store
            .admit(&home.request(), &home.config, &json!({}), None)
            .unwrap();
        if resume {
            store
                .conn
                .execute(
                    "UPDATE agents SET resume_of_runtime_session_id='saved' WHERE id=?",
                    [id.as_str()],
                )
                .unwrap();
        }
        let record = store.get(&id).unwrap();
        let prep = common::writer::prepare_supervisor(&mut store, &id);
        let harness = common::writer::Harness::start(&home.path);
        let mut persist = harness.supervisor(&home.path, prep, &id).await;
        let mut frames = Vec::new();
        for subtype in ["hook_started", "hook_progress", "hook_response"] {
            let mut hook = json!({
                "type": "system", "subtype": subtype, "hook_event": "SessionStart",
                "hook_name": "SessionStart:resume", "hook_id": "fixture-hook",
                "session_id": "temporary", "uuid": uuid::Uuid::new_v4().to_string(),
            });
            if subtype != "hook_started" {
                hook["stdout"] = json!("");
                hook["stderr"] = json!("");
                hook["output"] = json!("");
            }
            if subtype == "hook_response" {
                hook["outcome"] = json!("success");
                hook["exit_code"] = json!(0);
            }
            frames.push(hook);
        }
        let identity = |frame| {
            if foreign_frame == Some(frame) {
                "foreign"
            } else {
                "saved"
            }
        };
        frames.extend([
            json!({"type":"system","subtype":"init","session_id":identity("init")}),
            json!({"type":"assistant","session_id":identity("assistant"),
                "message":{"id":"msg-1","content":[{"type":"text","text":"answer"}]}}),
            json!({"type":"result","subtype":"success","is_error":false,
                "session_id":identity("result"),"result":"answer","usage":{}}),
        ]);
        let stream = frames
            .iter()
            .map(ToString::to_string)
            .collect::<Vec<_>>()
            .join("\n");
        let mut process = Process::spawn(&fake_engine(&home.path, &stream)).unwrap();
        let result = stream::run(&mut process, &mut persist, &record, &record, None).await;
        process.reap().await;
        persist.flush().await.unwrap();

        if let Some(frame) = foreign_frame {
            assert!(
                result
                    .err()
                    .expect(frame)
                    .to_string()
                    .contains("runtime resumed a different native session")
            );
        } else {
            let outcome = result.unwrap().outcome;
            assert_eq!(outcome.status, Status::Succeeded);
            assert_eq!(outcome.runtime_session_id.as_deref(), Some("saved"));
        }
        let expected = if foreign_frame == Some("init") {
            None
        } else {
            Some("saved")
        };
        assert_eq!(
            store.get(&id).unwrap().runtime_session_id.as_deref(),
            expected
        );
    }
}

/// Mirrors `tests/test_resume_adapters.py::ArgumentsTests::test_dispatch_inherits_authority_and_preserves_caller`
///
/// Resume inherits the parent's authority rather than accepting a new grant, so
/// an authority override is refused at the argument boundary while the caller's
/// orchestrator reference remains part of the declared contract.
#[tokio::test]
async fn dispatch_inherits_authority_and_preserves_caller() {
    let home = common::Home::new();
    let writer = common::writer::Harness::start(&home.path);
    let service = Service::new(home.path.clone()).with_writer(writer.broker());
    let parent = "ag-20260916-120000-0123456789";

    let overridden = dispatch::call(
        &service,
        "resume",
        json!({"agent_id": parent, "task": "fix", "write": true}),
    )
    .await
    .expect_err("resume must not accept an authority override");
    assert!(
        overridden.to_string().contains("argument"),
        "an authority override is refused while decoding: {overridden}"
    );

    let caller = dispatch::call(
        &service,
        "resume",
        json!({
            "agent_id": parent,
            "task": "fix",
            "orchestrator": {"transport": "codex_queue", "external_session_id": "caller"},
        }),
    )
    .await
    .expect_err("the fixture parent does not exist");
    assert!(
        !caller.to_string().contains("argument"),
        "the caller's orchestrator reference is part of the contract: {caller}"
    );
}
