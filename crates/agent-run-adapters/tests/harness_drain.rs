//! Preserve already-written output after primary exit, independently of consumer processing time.

use agent_run_adapters::{
    io::{Event, Process},
    LaunchPlan,
};
use std::{collections::BTreeMap, path::PathBuf, time::Duration};

/// Terminates the captured fixture even if an assertion interrupts draining.
struct Fixture(Process);
impl Drop for Fixture {
    /// Bounds fixture cleanup without relying on normal test completion.
    fn drop(&mut self) {
        let _ = self.0.owner.cleanup_blocking(Duration::from_millis(100));
    }
}

/// A finite stdout burst must survive more than 200ms of downstream work after the writer has exited.
#[tokio::test]
async fn primary_exit_does_not_discard_queued_frames_for_a_slow_consumer() {
    let plan=LaunchPlan {
        binary:PathBuf::from("/bin/sh"),
        args:vec!["-c".into(),"read begin; n=0; while [ $n -lt 32 ]; do printf '{\"n\":%s}\\n' \"$n\"; n=$((n+1)); done".into()],
        cwd:std::env::current_dir().unwrap(),environment:BTreeMap::new(),initial_input:None,
    };
    let mut fixture = Fixture(Process::spawn(&plan).unwrap());
    fixture.0.text("begin\n").await.unwrap();
    tokio::time::timeout(Duration::from_secs(2), fixture.0.child.wait())
        .await
        .unwrap()
        .unwrap();
    let mut received = Vec::new();
    loop {
        match tokio::time::timeout(Duration::from_secs(2), fixture.0.next())
            .await
            .unwrap()
        {
            Event::Json(value) => received.push(value["n"].as_u64().unwrap()),
            Event::Eof => break,
            other => panic!("unexpected event: {other:?}"),
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert_eq!(received, (0..32).collect::<Vec<_>>());
}

/// A captured writer ignoring TERM cannot postpone KILL by continuously keeping the output queue busy.
#[tokio::test]
async fn continuous_descendant_output_does_not_postpone_escalation() {
    use agent_run_platform::process::{self, ProcessState};
    let plan=LaunchPlan {
        binary:PathBuf::from("/bin/sh"),
        args:vec!["-c".into(),r#"node -e 'process.on("SIGTERM", () => {}); setInterval(() => console.log("{\"tick\":true}"), 5); setTimeout(() => process.exit(0), 12000)' & printf '{"pid":%s}\n' "$!"; read begin; exit 0"#.into()],
        cwd:std::env::current_dir().unwrap(),
        environment:BTreeMap::from([("PATH".into(),std::env::var("PATH").unwrap())]),initial_input:None,
    };
    let mut fixture = Fixture(Process::spawn(&plan).unwrap());
    let startup = tokio::time::Instant::now() + Duration::from_secs(2);
    let mut pid = None;
    loop {
        let Event::Json(value) = tokio::time::timeout_at(startup, fixture.0.next())
            .await
            .unwrap()
        else {
            panic!("fixture did not start");
        };
        if let Some(value) = value["pid"].as_i64() {
            pid = Some(value as i32);
        }
        if value["tick"] == true {
            break;
        }
    }
    let child = process::inspect(pid.expect("fixture child pid")).unwrap();
    fixture.0.owner.refresh();
    fixture.0.text("begin\n").await.unwrap();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(4);
    loop {
        match tokio::time::timeout_at(deadline, fixture.0.next())
            .await
            .unwrap()
        {
            Event::Json(_) => {}
            Event::Eof => break,
            other => panic!("unexpected event: {other:?}"),
        }
    }
    assert!(matches!(
        process::observe(Some(child.pid), Some(&child.token), Some(child.birth)),
        ProcessState::Dead | ProcessState::Reused
    ));
    assert_eq!(fixture.0.reap().await, Some(0));
}
