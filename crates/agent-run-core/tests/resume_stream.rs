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

/// Rejects partial tool data and quota controls from outside the owned session.
#[tokio::test]
async fn foreign_session_frames_are_rejected_before_consumption() {
    let partial = json!({"type":"stream_event","session_id":"foreign","event":{
        "type":"content_block_start","content_block":{
            "type":"tool_use","id":"foreign-tool","name":"mcp__agent_run_worker__finish"
        }
    }});
    let mut unidentified_partial = partial.clone();
    unidentified_partial
        .as_object_mut()
        .unwrap()
        .remove("session_id");
    let rate_limit = |status| {
        json!({"type":"rate_limit_event","session_id":"foreign",
        "rate_limit_info":{"status":status,"rateLimitType":"five_hour"}})
    };
    for (resume, frame, foreign_before_init) in [
        (true, partial.clone(), true),
        (true, partial.clone(), false),
        (false, partial.clone(), true),
        (false, partial, false),
        (false, unidentified_partial, false),
        (false, rate_limit("rejected"), true),
        (true, rate_limit("rejected"), false),
        (true, rate_limit("allowed"), false),
    ] {
        let home = common::Home::new();
        let mut store = home.store();
        let mut request = home.request();
        request.explicit_finish = !resume;
        let (id, _) = store
            .admit(&request, &home.config, &json!({}), None)
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
        let mut frames = vec![
            json!({"type":"system","subtype":"init","session_id":"saved"}),
            frame,
            json!({"type":"result","subtype":"error_during_execution","is_error":true,
                "session_id":"saved","usage":{}}),
        ];
        if foreign_before_init {
            frames.swap(0, 1);
        } else if frames[1]["rate_limit_info"]["status"] == "allowed" {
            let mut owned_rejection = rate_limit("rejected");
            owned_rejection["session_id"] = json!("saved");
            frames.insert(1, owned_rejection);
        }
        let stream = frames
            .iter()
            .map(ToString::to_string)
            .collect::<Vec<_>>()
            .join("\n");
        let mut process = Process::spawn(&fake_engine(&home.path, &stream)).unwrap();
        let result = stream::run(&mut process, &mut persist, &record, &record, None).await;
        process.reap().await;
        persist.flush().await.unwrap();

        let expected = if resume {
            "runtime resumed a different native session"
        } else if foreign_before_init {
            "explicit run has not established its native session"
        } else {
            "explicit run changed its native session"
        };
        assert!(
            matches!(result, Err(agent_run_core::Error::Validation(message)) if message == expected)
        );
        assert!(
            !store
                .transcript(&id, 0, 100)
                .unwrap()
                .to_string()
                .contains("foreign-tool")
        );
        let result_frames: i64 = store
            .conn
            .query_row(
                "SELECT COUNT(*) FROM events WHERE agent_id=? AND kind='native_result_frame_v1'",
                [id.as_str()],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(result_frames, 0, "must stop before the subsequent result");
    }
}

/// An ordinary fresh run accepts a legacy partial without a session ID.
#[tokio::test]
async fn fresh_launch_accepts_unidentified_partial_after_init() {
    let home = common::Home::new();
    let mut store = home.store();
    let (id, _) = store
        .admit(&home.request(), &home.config, &json!({}), None)
        .unwrap();
    let record = store.get(&id).unwrap();
    let prep = common::writer::prepare_supervisor(&mut store, &id);
    let harness = common::writer::Harness::start(&home.path);
    let mut persist = harness.supervisor(&home.path, prep, &id).await;
    let stream = [
        json!({"type":"system","subtype":"init","session_id":"saved"}),
        json!({"type":"stream_event","event":{"type":"content_block_delta",
            "delta":{"type":"text_delta","text":"legacy partial"}}}),
        json!({"type":"result","subtype":"success","is_error":false,
            "session_id":"saved","result":"legacy partial","usage":{}}),
    ]
    .map(|frame| frame.to_string())
    .join("\n");
    let mut process = Process::spawn(&fake_engine(&home.path, &stream)).unwrap();
    let result = stream::run(&mut process, &mut persist, &record, &record, None)
        .await
        .unwrap();
    process.reap().await;
    persist.flush().await.unwrap();
    assert_eq!(result.outcome.status, Status::Succeeded);
    assert_eq!(result.outcome.runtime_session_id.as_deref(), Some("saved"));
    assert!(
        store
            .transcript(&id, 0, 100)
            .unwrap()
            .to_string()
            .contains("legacy partial")
    );
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
        overridden.to_string().contains("unknown field `write`"),
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
