//! Real stream loop: turn completion idles, steering wakes, callback owns answer.
mod common;
use agent_run_adapters::{LaunchPlan, io::Process};
use agent_run_core::{domain::Status, stream};
use agent_run_domain::worker::{FinishRequest, FinishStatus};
use serde_json::json;
use std::{collections::BTreeMap, time::Duration};

/// Static capability owned solely by these provider-free fixture attempts.
const TOKEN: &str = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
/// Successful native text which must never become an explicit-mode answer.
const TURN: &str = r#"{"type":"result","subtype":"success","is_error":false,"session_id":"s","result":"NOT_A_FINISH","usage":{}}"#;

/// An owned finite shell fixture, without a real provider or model turn.
fn plan(home: &common::Home, script: String) -> LaunchPlan {
    LaunchPlan {
        binary: "/bin/sh".into(),
        args: vec!["-c".into(), script],
        cwd: home.path.clone(),
        environment: BTreeMap::new(),
        initial_input: None,
    }
}

/// Wait for a durable phase only inside the bounded fixture driver. This
/// exercises the supervisor's DB observation, not model polling or inference.
async fn phase(
    store: &agent_run_store::Store,
    id: &agent_run_domain::domain::AgentId,
    expected: &str,
) {
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if store
                .worker_lifecycle_view(id)
                .unwrap()
                .is_some_and(|v| v["phase"] == expected)
            {
                return;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("phase observed");
}

/// Exactly the same native success/EOF yields legacy success and explicit
/// ended_without_finish, proving zero exit/assistant text cannot finish a run.
#[tokio::test]
async fn native_success_without_callback_is_not_completion() {
    for explicit in [false, true] {
        let home = common::Home::new();
        let mut req = home.request();
        req.explicit_finish = explicit;
        let (id, _) = home
            .store()
            .admit(&req, &home.config, &json!({}), None)
            .unwrap();
        let mut store = home.store();
        let row = store.get(&id).unwrap();
        let mut process = Process::spawn(&plan(
            &home,
            format!("read initial; printf '%s\\n' '{TURN}'"),
        ))
        .unwrap();
        let result = stream::run(&mut process, &mut store, &row, Some("initial\n"))
            .await
            .unwrap();
        assert_eq!(
            result.outcome.status,
            if explicit {
                Status::Failed
            } else {
                Status::Succeeded
            }
        );
        if explicit {
            assert_eq!(
                result.outcome.failure_kind.as_deref(),
                Some("ended_without_finish")
            );
            assert!(result.answer.is_none());
        }
    }
}

/// A native turn ends, the same live process idles without success, one
/// steering input wakes it, and an observed finish receipt fixes the answer.
/// Later native text never replaces summary; owned cleanup is verified.
#[tokio::test]
async fn idle_wake_and_private_finish_keep_one_run_and_exact_summary() {
    let home = common::Home::new();
    let mut req = home.request();
    req.explicit_finish = true;
    let (id, _) = home
        .store()
        .admit(&req, &home.config, &json!({}), None)
        .unwrap();
    let mut store = home.store();
    store.conn.execute("INSERT INTO attempts(id,agent_id,number,state,adapter_state_json,created_at,ownership_active) VALUES('attempt',?,1,'running','{}',100,1)",[id.as_str()]).unwrap();
    store
        .conn
        .execute(
            "UPDATE agents SET status='running' WHERE id=?",
            [id.as_str()],
        )
        .unwrap();
    let attempt: String = store
        .conn
        .query_row(
            "SELECT id FROM attempts WHERE agent_id=?",
            [id.as_str()],
            |r| r.get(0),
        )
        .unwrap();
    store
        .issue_worker_capability(&id, &attempt, TOKEN, 100.0)
        .unwrap();
    store.begin_worker_lifecycle(&id, &attempt, 100.0).unwrap();
    let row = store.get(&id).unwrap();
    let script = format!(
        r#"read initial
printf '%s\n' '{TURN}'
read wake
printf '%s\n' '{{"type":"assistant","session_id":"s","message":{{"role":"assistant","content":[{{"type":"tool_use","id":"finish-call","name":"mcp__agent_run_worker__finish","input":{{"summary":"FINAL"}}}}]}}}}'
for i in $(seq 1 500); do test -f receipt && break; sleep 0.01; done
read digest < receipt
printf '{{"type":"user","session_id":"s","message":{{"role":"user","content":[{{"type":"tool_result","tool_use_id":"finish-call","content":"%s"}}]}}}}\n' "$digest"
printf '%s\n' '{{"type":"assistant","session_id":"s","message":{{"role":"assistant","content":[{{"type":"text","text":"LATE_OVERWRITE"}}]}}}}'
sleep 1
"#
    );
    let mut process = Process::spawn(&plan(&home, script)).unwrap();
    let pid = process.owner.pid;
    let mut control = home.store();
    let driver = async {
        phase(&control, &id, "idle").await;
        assert_eq!(control.get(&id).unwrap().status, Status::Running);
        control
            .enqueue(&id, "steer", &json!({"text":"wake"}))
            .unwrap();
        phase(&control, &id, "running").await;
        let receipt = control
            .accept_worker_finish(
                &id,
                &attempt,
                TOKEN,
                &FinishRequest {
                    summary: "FINAL".into(),
                    status: FinishStatus::Done,
                },
                agent_run_domain::domain::now(),
            )
            .unwrap();
        std::fs::write(home.path.join("receipt"), format!("{}\n", receipt.sha256)).unwrap();
    };
    let result = tokio::time::timeout(Duration::from_secs(10), async {
        let (result, ()) = tokio::join!(
            stream::run(&mut process, &mut store, &row, Some("initial\n")),
            driver
        );
        result.unwrap()
    })
    .await
    .expect("finite callback fixture");
    assert_eq!(process.owner.pid, pid);
    assert_eq!(result.outcome.status, Status::Succeeded);
    assert_eq!(result.answer.as_deref(), Some("FINAL"));
    assert_eq!(result.outcome.runtime_session_id.as_deref(), Some("s"));
    assert_eq!(
        store.worker_lifecycle_view(&id).unwrap().unwrap()["turn_count"],
        2
    );
    let proof = process.owner.cleanup(Duration::from_secs(2)).await.unwrap();
    assert!(proof.confirmed && proof.group_gone);
    process.reap().await;
}

/// A reaped supervisor after callback/cleanup is recovered from immutable
/// intent exactly once. Unknown cleanup remains closing even at extreme age.
#[tokio::test]
async fn acknowledged_finish_recovers_after_supervisor_exit() {
    let home = common::Home::new();
    let mut req = home.request();
    req.explicit_finish = true;
    req.orchestrator = Some(
        serde_json::from_value(
            json!({"transport":"codex_queue","external_session_id":"fixture-thread"}),
        )
        .unwrap(),
    );
    let (id, _) = home
        .store()
        .admit(&req, &home.config, &json!({}), None)
        .unwrap();
    let mut store = home.store();
    let mut process = Process::spawn(&plan(&home, "sleep 30".into())).unwrap();
    let leader = process.owner.leader.clone().unwrap();
    store.conn.execute("UPDATE agents SET status='running',supervisor_pid=?,supervisor_identity=?,supervisor_birth_time=?,process_group_id=?,heartbeat_at=100 WHERE id=?",
        rusqlite::params![leader.pid,leader.token,leader.birth,leader.group,id.as_str()]).unwrap();
    store.conn.execute("INSERT INTO attempts(id,agent_id,number,state,adapter_state_json,created_at,ownership_active,phase,process_identity,process_birth_time) VALUES('attempt',?,1,'running','{}',100,1,'running',?,?)",
        rusqlite::params![id.as_str(),leader.token,leader.birth]).unwrap();
    store
        .issue_worker_capability(&id, "attempt", TOKEN, 100.0)
        .unwrap();
    store.begin_worker_lifecycle(&id, "attempt", 100.0).unwrap();
    store
        .accept_worker_finish(
            &id,
            "attempt",
            TOKEN,
            &FinishRequest {
                summary: "RECOVERED".into(),
                status: FinishStatus::Done,
            },
            101.0,
        )
        .unwrap();
    store.observe_worker_finish_receipt(&id).unwrap();
    let proof = process.owner.cleanup(Duration::from_secs(2)).await.unwrap();
    assert!(proof.confirmed);
    process.reap().await;
    assert!(
        !agent_run_core::lifecycle::reconcile::reconcile_reaped_agent(
            &mut store, &id, leader.pid, 1e12
        )
        .unwrap()
    );
    assert_eq!(store.get(&id).unwrap().status, Status::Running);
    store.provider_cleanup(&id, "attempt", &proof).unwrap();
    assert!(
        agent_run_core::lifecycle::reconcile::reconcile_reaped_agent(
            &mut store, &id, leader.pid, 1e12
        )
        .unwrap()
    );
    assert_eq!(store.get(&id).unwrap().status, Status::Succeeded);
    let recovered: serde_json::Value =
        serde_json::from_str(&store.latest_attempt_state(&id).unwrap()).unwrap();
    assert_eq!(
        recovered["native_history_unavailable"],
        "supervisor exited before native history was sealed"
    );
    assert_eq!(
        std::fs::read_to_string(store.get(&id).unwrap().answer_path.unwrap()).unwrap(),
        "RECOVERED"
    );
    assert!(
        !agent_run_core::lifecycle::reconcile::reconcile_reaped_agent(
            &mut store, &id, leader.pid, 1e12
        )
        .unwrap()
    );
    assert_eq!(
        store
            .conn
            .query_row::<i64, _, _>(
                "SELECT COUNT(*) FROM deliveries WHERE agent_id=?",
                [id.as_str()],
                |r| r.get(0)
            )
            .unwrap(),
        1
    );
}
