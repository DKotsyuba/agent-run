//! A dead harness must not wait for stdout/stderr inherited by its descendants.

use agent_run_adapters::{
    io::{Event, Process},
    LaunchPlan,
};
use agent_run_platform::process::{self, ProcessState};
use serde_json::json;
use std::{collections::BTreeMap, path::PathBuf, time::Duration};

/// Ensures assertion failures terminate the whole captured fixture; every child also has an 8s TTL.
struct Fixture(Process);

impl Drop for Fixture {
    /// Cleans verified members even if an assertion unwinds before the asynchronous reap.
    fn drop(&mut self) {
        let _ = self.0.owner.cleanup_blocking(Duration::from_millis(100));
    }
}

/// Creates a shell leader whose child inherits both output pipes until killed or its TTL expires.
async fn spawn(tail: &str) -> (Fixture, process::Identity) {
    let script = format!("sleep 8 & printf '{{\"pid\":%s}}\\n' \"$!\"; read request; {tail}");
    let plan = LaunchPlan {
        binary: PathBuf::from("/bin/sh"),
        args: vec!["-c".into(), script],
        cwd: std::env::current_dir().unwrap(),
        environment: BTreeMap::new(),
        initial_input: None,
    };
    let mut fixture = Fixture(Process::spawn(&plan).unwrap());
    let event = tokio::time::timeout(Duration::from_secs(2), fixture.0.next())
        .await
        .unwrap();
    let Event::Json(event) = event else {
        panic!("fixture child identity missing");
    };
    let child = process::inspect(event["pid"].as_i64().unwrap() as i32).unwrap();
    fixture.0.owner.refresh();
    (fixture, child)
}

/// Keeps the final result while detecting leader exit and terminating a pipe-holding child.
#[tokio::test]
async fn leader_exit_closes_stream_without_waiting_for_descendant_eof() {
    let (mut fixture, child) = spawn("printf '%s\\n' '{\"result\":\"done\"}'; exit 0").await;
    let captures = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
    let recorded = captures.clone();
    fixture
        .0
        .observe_ownership(move |snapshot| {
            recorded.lock().unwrap().push(snapshot.members.len());
            Ok(())
        })
        .unwrap();
    fixture.0.text("finish\n").await.unwrap();
    let Event::Json(value) = fixture.0.next().await else {
        panic!("final result lost");
    };
    assert_eq!(value["result"], "done");
    let ended = tokio::time::timeout(Duration::from_secs(2), fixture.0.next()).await;
    assert!(
        matches!(ended, Ok(Event::Eof)),
        "leader exit must end the stream promptly: {ended:?}"
    );
    assert_eq!(fixture.0.reap().await, Some(0));
    assert_eq!(
        *captures.lock().unwrap(),
        vec![2],
        "unchanged observations must not rewrite durable snapshots"
    );
    assert_eq!(
        process::observe(Some(child.pid), Some(&child.token), Some(child.birth)),
        ProcessState::Dead
    );
}

/// RPC startup uses the same PID observation even when a descendant never closes its descriptors.
#[tokio::test]
async fn rpc_reports_leader_exit_instead_of_waiting_for_its_deadline() {
    let (mut fixture, child) = spawn("exit 7").await;
    let started = std::time::Instant::now();
    let error = tokio::time::timeout(
        Duration::from_secs(2),
        fixture
            .0
            .rpc("initialize", json!({}), Duration::from_secs(10)),
    )
    .await
    .unwrap()
    .unwrap_err();
    assert!(error.to_string().contains("exit code 7"), "{error}");
    assert!(started.elapsed() < Duration::from_secs(2));
    assert_eq!(fixture.0.reap().await, Some(7));
    assert_eq!(
        process::observe(Some(child.pid), Some(&child.token), Some(child.birth)),
        ProcessState::Dead
    );
}
