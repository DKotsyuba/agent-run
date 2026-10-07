//! Finite control-write regression and opted-in native Claude correlation smoke.
//! The native test starts no tools/MCP or source edits; auth uses normal HOME.

mod common;

use agent_run_adapters::{LaunchPlan, io::Process};
use agent_run_core::{domain::Status, stream};
use serde_json::json;
use std::{collections::BTreeMap, time::Duration};

/// Runs a fresh no-tool Claude task plus queued steering through the actual
/// stream runner, then continues the same native session without control messages.
/// Opt-in requires AGENT_RUN_NATIVE_CLAUDE to name the installed
/// CLI; the temporary store is independent of production and keeps safe frame
/// metadata only. Both timeout and normal completion perform owned cleanup
/// before assertions, so test failures cannot leave the native harness alive.
#[tokio::test]
#[ignore = "requires explicit native Claude smoke authorization and local auth"]
async fn native_claude_correlates_queued_steering() {
    let binary = std::env::var("AGENT_RUN_NATIVE_CLAUDE").expect("explicit native CLI path");
    let home = common::Home::new();
    let mut requested = home.request();
    requested.task = "Respond with only INITIAL.".into();
    let (id, _) = home
        .store()
        .admit(&requested, &home.config, &json!({}), None)
        .unwrap();
    let mut store = home.store();
    store
        .enqueue(
            &id,
            "steer",
            &json!({"text":"For the final task response, respond with only DONE."}),
        )
        .unwrap();
    let record = store.get(&id).unwrap();
    let mut environment = BTreeMap::new();
    for key in ["HOME", "PATH", "USER", "LANG"] {
        if let Ok(value) = std::env::var(key) {
            environment.insert(key.into(), value);
        }
    }
    let plan = LaunchPlan {
        binary: binary.into(),
        args: [
            "--print",
            "--output-format",
            "stream-json",
            "--input-format",
            "stream-json",
            "--verbose",
            "--include-partial-messages",
            "--replay-user-messages",
            "--model",
            "sonnet",
            "--effort",
            "low",
            "--permission-mode",
            "default",
            "--setting-sources",
            "",
            "--strict-mcp-config",
            "--mcp-config",
            "{\"mcpServers\":{}}",
            "--tools",
            "",
        ]
        .into_iter()
        .map(str::to_owned)
        .collect(),
        cwd: home.path.clone(),
        environment,
        initial_input: None,
    };
    let mut process = Process::spawn(&plan).expect("native adapter spawn");
    let initial = json!({"type":"user","message":{"role":"user","content":[{"type":"text","text":requested.task}]}}).to_string();
    let result = tokio::time::timeout(
        Duration::from_secs(90),
        stream::run(&mut process, &mut store, &record, Some(&initial)),
    )
    .await;
    let cleanup = process.owner.cleanup(Duration::from_secs(2)).await;
    process.reap().await;
    let cleanup = cleanup.expect("owned native cleanup");
    assert!(cleanup.confirmed && cleanup.group_gone && cleanup.descendants_gone == Some(true));
    let result = result.expect("bounded native task").expect("native stream");
    assert_eq!(result.outcome.status, Status::Succeeded);
    assert_eq!(result.outcome.exit_code, Some(0));
    assert_eq!(result.answer.as_deref().map(str::trim), Some("DONE"));

    let saved = result
        .outcome
        .runtime_session_id
        .clone()
        .expect("native session identity");
    let mut resumed = store.get(&id).unwrap();
    resumed.resume_of_runtime_session_id = Some(saved.clone());
    let mut resume_plan = plan;
    resume_plan.args.extend(["--resume".into(), saved.clone()]);
    let mut process = Process::spawn(&resume_plan).expect("native continuation spawn");
    let initial = json!({"type":"user","message":{"role":"user","content":[{"type":"text","text":"Respond with only RESUMED."}]}}).to_string();
    let continuation = tokio::time::timeout(
        Duration::from_secs(90),
        stream::run(&mut process, &mut store, &resumed, Some(&initial)),
    )
    .await;
    let cleanup = process.owner.cleanup(Duration::from_secs(2)).await;
    process.reap().await;
    let cleanup = cleanup.expect("owned continuation cleanup");
    assert!(cleanup.confirmed && cleanup.group_gone && cleanup.descendants_gone == Some(true));
    let continuation = continuation
        .expect("bounded continuation")
        .expect("native continuation");
    assert_eq!(continuation.outcome.status, Status::Succeeded);
    assert_eq!(continuation.outcome.exit_code, Some(0));
    assert_eq!(
        continuation.outcome.runtime_session_id.as_deref(),
        Some(saved.as_str())
    );
    assert_eq!(
        continuation.answer.as_deref().map(str::trim),
        Some("RESUMED")
    );

    let metadata: Vec<String> = store.conn.prepare(
        "SELECT data_json FROM events WHERE agent_id=? AND kind='native_result_frame_v1' ORDER BY seq"
    ).unwrap().query_map([id.as_str()], |row| row.get(0)).unwrap()
        .map(Result::unwrap).collect();
    assert!(!metadata.is_empty());
    for frame in &metadata {
        assert!(!frame.contains("INITIAL") && !frame.contains("DONE"));
        assert!(!frame.contains("user_message_uuid") && !frame.contains("session_id"));
    }
    let exit: String = store.conn.query_row(
        "SELECT data_json FROM events WHERE agent_id=? AND kind='native_result_exit_v1' ORDER BY seq DESC LIMIT 1",
        [id.as_str()], |row| row.get(0),
    ).unwrap();
    println!(
        "native+resume frames={} exit={} cleanup_confirmed={}",
        metadata.len(),
        exit,
        cleanup.confirmed
    );
}

/// A control write after the engine closed stdin may be partial/uncertain.
/// Even a later nonblank, exit-zero terminal cannot certify the changed task.
/// The finite engine and timed driver use only a disposable home and marker.
#[tokio::test]
async fn closed_stdin_control_cannot_certify_success() {
    let home = common::Home::new();
    let (id, _) = home
        .store()
        .admit(&home.request(), &home.config, &json!({}), None)
        .unwrap();
    let mut store = home.store();
    let record = store.get(&id).unwrap();
    let plan = LaunchPlan {
        binary: "/bin/sh".into(),
        args: vec!["-c".into(),
            r#"read initial; exec 0<&-; : > stdin-closed; sleep 1; printf '%s\n' '{"type":"result","subtype":"success","is_error":false,"session_id":"s","result":"done"}'"#.into()],
        cwd: home.path.clone(),
        environment: BTreeMap::new(),
        initial_input: None,
    };
    let mut process = Process::spawn(&plan).unwrap();
    let driver = async {
        let ready = tokio::time::timeout(Duration::from_secs(5), async {
            while !home.path.join("stdin-closed").exists() {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .is_ok();
        if ready {
            let mut control = agent_run_core::state::Store::open(&home.path)?;
            control.enqueue(&id, "steer", &json!({"text":"changed task"}))?;
        }
        Ok::<bool, agent_run_core::error::Error>(ready)
    };
    let initial = json!({"type":"user","message":{"role":"user","content":[{"type":"text","text":"initial task"}]}}).to_string();
    let (result, driven) = tokio::join!(
        tokio::time::timeout(
            Duration::from_secs(5),
            stream::run(&mut process, &mut store, &record, Some(&initial))
        ),
        driver,
    );
    let cleanup = process.owner.cleanup(Duration::from_secs(2)).await;
    process.reap().await;
    assert!(cleanup.unwrap().confirmed);
    assert!(driven.unwrap());
    let result = result.unwrap().unwrap();
    assert_eq!(result.outcome.status, Status::Failed);
    assert_eq!(
        result.outcome.failure_kind.as_deref(),
        Some("uncertain_input")
    );
    assert!(result.answer.is_none());
}
