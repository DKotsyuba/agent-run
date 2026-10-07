//! Pool push through the real Claude stream runner against a finite shell
//! engine double: a successful stdin write is `written` only, and an error
//! after the engine closed stdin is `unknown`, never "not delivered".

mod common;

use agent_run_adapters::{LaunchPlan, io::Process};
use agent_run_core::{domain::Status, stream};
use serde_json::{Value, json};
use std::collections::BTreeMap;

const RESULT: &str = r#"{"type":"result","subtype":"success","is_error":false,"session_id":"s","result":"done","usage":{"input_tokens":1,"output_tokens":1},"num_turns":1}"#;

/// Admits one run in a one-seat pool with a pending `pool` command, runs the
/// stream against `script`, and returns the durable command result.
async fn push_through(script: &str, initial: Option<&str>) -> Value {
    let home = common::Home::new();
    let (id, _) = home
        .store()
        .admit(&home.request(), &home.config, &json!({}), None)
        .unwrap();
    let mut store = home.store();
    for sql in [
        "INSERT INTO pools(id,request_namespace,request_id,request_sha256,goal,acceptance_json,state,roster_revision,created_at) VALUES('pool-20260101-000000-0123456789','ns','r',lower(hex(zeroblob(32))),'goal','[]','open',1,1.0)",
        "INSERT INTO pool_entries(pool_id,author_kind,direction,kind,roster_revision,body,idem_scope,request_id,created_at) VALUES('pool-20260101-000000-0123456789','operator','team','message',1,'please recheck','op','k',1.0)",
    ] {
        store.conn.execute(sql, []).unwrap();
    }
    store
        .conn
        .execute(
            "INSERT INTO pool_members(agent_id,pool_id,slot,name,role,personal_task,joined_roster_revision) VALUES(?,'pool-20260101-000000-0123456789',1,'Ada','reviewer','t',1)",
            [id.as_str()],
        )
        .unwrap();
    store.enqueue(&id, "pool", &json!({"seq": 1})).unwrap();
    let record = store.get(&id).unwrap();
    let plan = LaunchPlan {
        binary: "/bin/sh".into(),
        args: vec!["-c".into(), script.into()],
        cwd: home.path.clone(),
        environment: BTreeMap::new(),
        initial_input: None,
    };
    let mut process = Process::spawn(&plan).expect("shell engine");
    let result = tokio::time::timeout(
        std::time::Duration::from_secs(20),
        stream::run(&mut process, &mut store, &record, initial),
    )
    .await
    .expect("finite engine ends")
    .expect("stream completes");
    assert_eq!(result.outcome.status, Status::Succeeded);
    process.reap().await;
    let raw: String = store
        .conn
        .query_row(
            "SELECT result_json FROM commands WHERE agent_id=? AND kind='pool'",
            [id.as_str()],
            |row| row.get(0),
        )
        .unwrap();
    serde_json::from_str(&raw).unwrap()
}

/// The engine reads the initial JSON task and pool frame, and identifies its
/// terminal as the task result. The pool write is still only `written`, carries
/// the stamped render, and claims no delivery or consumption.
#[tokio::test]
async fn successful_stdin_write_is_written_not_accepted() {
    let seen = std::env::temp_dir().join(format!("pool-push-{}", std::process::id()));
    let script = format!(
        r#"read initial; read line; printf '%s' "$line" > {path}; id=$(printf '%s' "$initial" | sed -n 's/.*"uuid":"\([^"]*\)".*/\1/p'); printf '%s\n' '{RESULT}' | sed "s/\"session_id\"/\"user_message_uuid\":\"$id\",\"session_id\"/""#,
        path = seen.display()
    );
    let initial =
        json!({"type":"user","message":{"role":"user","content":[{"type":"text","text":"hello"}]}})
            .to_string();
    let result = push_through(&script, Some(&initial)).await;
    assert_eq!(result["push"], "written");
    assert_eq!(result["reason"], "stdin_write");
    assert!(result.get("delivered").is_none() && result.get("accepted").is_none());
    let frame = std::fs::read_to_string(&seen).unwrap();
    let _ = std::fs::remove_file(&seen);
    assert!(frame.contains("agent-run/pool #1 roster r1"), "{frame}");
    assert!(frame.contains("from orchestrator to team"), "{frame}");
}

/// A stdin the runner already closed makes the write fail; because a failed
/// write may follow a partial one that is `unknown`, never a claim that
/// nothing was delivered.
#[tokio::test]
async fn failed_stdin_write_is_unknown() {
    let script = format!("sleep 1; echo '{RESULT}'");
    let result = push_through(&script, None).await;
    assert_eq!(result["push"], "unknown");
    assert_eq!(result["reason"], "stdin_write_failed");
}
