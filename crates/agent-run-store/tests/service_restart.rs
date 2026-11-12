//! A restart captures existing work and atomically closes new admissions.
use crate::common;

use agent_run_domain::{Error, domain::OrchestratorRef};
use serde_json::json;

/// The captured cohort, deduplicated owners and admission barrier survive reopen.
#[test]
fn restart_captures_work_and_blocks_new_admissions() {
    let home = common::Home::new();
    let mut store = home.store();
    let mut request = home.request();
    request.request_id = Some("existing-one".into());
    request.orchestrator = Some(OrchestratorRef {
        transport: "codex".into(),
        external_session_id: "session-one".into(),
        external_turn_id: None,
    });
    let (first, _) = store
        .admit(&request, &home.config, &json!({}), None)
        .unwrap();
    let mut second_request = request.clone();
    second_request.request_id = Some("existing-two".into());
    let (second, _) = store
        .admit(&second_request, &home.config, &json!({}), None)
        .unwrap();
    let restart = store.begin_service_restart("restart-one", 10.0).unwrap();
    assert_eq!(
        store.begin_service_restart("restart-one", 11.0).unwrap(),
        restart
    );
    assert!(matches!(
        store.begin_service_restart("restart-two", 11.0),
        Err(Error::Conflict)
    ));
    let agents: i64 = store
        .conn
        .query_row(
            "SELECT COUNT(*) FROM service_restart_agents WHERE restart_id=?",
            [restart],
            |row| row.get(0),
        )
        .unwrap();
    let owners: i64 = store.conn.query_row(
        "SELECT COUNT(*) FROM service_restart_recipients WHERE restart_id=? AND transport='codex_queue' AND external_session_id='session-one' AND had_agents=1",
        [restart], |row| row.get(0),
    ).unwrap();
    assert_eq!((agents, owners), (2, 1));
    assert_ne!(first, second);
    assert_eq!(
        store
            .admit(&request, &home.config, &json!({}), None)
            .unwrap(),
        (first, false)
    );
    let mut new_request = request.clone();
    new_request.request_id = Some("new-during-restart".into());
    assert!(matches!(
        store.admit(&new_request, &home.config, &json!({}), None),
        Err(Error::Transition(message)) if message.contains("restarting")
    ));
    drop(store);
    let store = home.store();
    assert_eq!(
        store
            .conn
            .query_row(
                "SELECT COUNT(*) FROM service_restart_agents WHERE restart_id=?",
                [restart],
                |row| row.get::<_, i64>(0),
            )
            .unwrap(),
        2
    );
}

/// A caller with no existing agents is retained once for the final notice.
#[test]
fn restart_remembers_first_time_caller_without_creating_an_agent() {
    let home = common::Home::new();
    let mut store = home.store();
    let restart = store.begin_service_restart("empty-restart", 1.0).unwrap();
    let reference = OrchestratorRef {
        transport: "claude".into(),
        external_session_id: "first-session".into(),
        external_turn_id: None,
    };
    store
        .remember_service_restart_caller(restart, &reference)
        .unwrap();
    store
        .remember_service_restart_caller(restart, &reference)
        .unwrap();
    let recipient: (i64, bool, bool) = store.conn.query_row(
        "SELECT COUNT(*),had_agents,tried_start FROM service_restart_recipients WHERE restart_id=? AND transport='claude_uds' AND external_session_id='first-session'",
        [restart], |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
    ).unwrap();
    assert_eq!(recipient, (1, false, true));
    assert_eq!(
        store
            .conn
            .query_row("SELECT COUNT(*) FROM agents", [], |row| row
                .get::<_, i64>(0))
            .unwrap(),
        0
    );
}

/// Binding an already captured agent after restart preparation still records
/// its owner and schedules the warning once.
#[test]
fn restart_captures_a_late_agent_binding() {
    let home = common::Home::new();
    let mut store = home.store();
    let (agent, _) = store
        .admit(&home.request(), &home.config, &json!({}), None)
        .unwrap();
    let restart = store.begin_service_restart("late-binding", 1.0).unwrap();
    let reference = OrchestratorRef {
        transport: "codex".into(),
        external_session_id: "owner".into(),
        external_turn_id: None,
    };
    store.bind_orchestrator(&agent, &reference, 2.0).unwrap();
    store.bind_orchestrator(&agent, &reference, 3.0).unwrap();
    assert_eq!(store.conn.query_row(
        "SELECT COUNT(*) FROM service_restart_notices WHERE restart_id=? AND external_session_id='owner' AND stage='before'",
        [restart], |row|row.get::<_,i64>(0),
    ).unwrap(), 1);
    assert_eq!(store.conn.query_row(
        "SELECT COUNT(*) FROM service_restart_agents WHERE restart_id=? AND agent_id=? AND orchestrator_session_id IS NOT NULL",
        rusqlite::params![restart,agent.as_str()], |row|row.get::<_,i64>(0),
    ).unwrap(), 1);
}

/// Give the existing outbox a chance to send the warning, without allowing a
/// stalled dispatcher to block restart forever.
#[test]
fn restart_waits_for_warning_attempt_but_not_indefinitely() {
    for outcome in ["delivered", "retry_wait", "pending"] {
        let home = common::Home::new();
        let mut store = home.store();
        let mut request = home.request();
        request.orchestrator = Some(OrchestratorRef {
            transport: "codex".into(),
            external_session_id: "owner".into(),
            external_turn_id: None,
        });
        let (agent, _) = store
            .admit(&request, &home.config, &json!({}), None)
            .unwrap();
        let restart = store.begin_service_restart("warning-order", 1.0).unwrap();
        let boot = agent_run_platform::monotonic::boot_id().unwrap();
        assert_eq!(
            store
                .advance_service_restart(restart, &boot, 1_000_000_000)
                .unwrap(),
            0
        );
        assert_eq!(
            store
                .conn
                .query_row(
                    "SELECT COUNT(*) FROM commands WHERE agent_id=?",
                    [agent.as_str()],
                    |r| r.get::<_, i64>(0)
                )
                .unwrap(),
            0
        );
        if outcome != "pending" {
            store.conn.execute("UPDATE deliveries SET state=? WHERE id IN (SELECT delivery_id FROM service_restart_notices WHERE restart_id=?)", rusqlite::params![outcome,restart]).unwrap();
            assert_eq!(
                store
                    .advance_service_restart(restart, &boot, 2_000_000_000)
                    .unwrap(),
                1
            );
        } else {
            assert_eq!(
                store
                    .advance_service_restart(restart, &boot, 10_999_999_999)
                    .unwrap(),
                0
            );
            assert_eq!(
                store
                    .advance_service_restart(restart, &boot, 11_000_000_000)
                    .unwrap(),
                1
            );
        }
    }
}

/// Late bindings that arrive before delivery are merged into one result.
#[test]
fn restart_groups_late_bindings_before_result_delivery() {
    let home = common::Home::new();
    let mut store = home.store();
    let (first, _) = store
        .admit(&home.request(), &home.config, &json!({}), None)
        .unwrap();
    let (second, _) = store
        .admit(&home.request(), &home.config, &json!({}), None)
        .unwrap();
    let restart = store.begin_service_restart("late-results", 1.0).unwrap();
    store.conn.execute("UPDATE service_restart_agents SET resume_status='failed',resume_error='history_unverifiable' WHERE restart_id=?", [restart]).unwrap();
    store
        .conn
        .execute(
            "UPDATE service_restarts SET phase='recovering' WHERE id=?",
            [restart],
        )
        .unwrap();
    let boot = agent_run_platform::monotonic::boot_id().unwrap();
    assert!(
        store
            .refresh_service_restart(restart, &boot, 1, 2.0)
            .unwrap()
    );
    let owner = OrchestratorRef {
        transport: "codex".into(),
        external_session_id: "owner".into(),
        external_turn_id: None,
    };
    store.bind_orchestrator(&first, &owner, 3.0).unwrap();
    store.bind_orchestrator(&second, &owner, 4.0).unwrap();
    let (count, message): (i64,String) = store.conn.query_row(
        "SELECT COUNT(*),message FROM service_restart_notices WHERE restart_id=? AND external_session_id='owner' AND stage='after'", [restart], |row| Ok((row.get(0)?,row.get(1)?))
    ).unwrap();
    assert_eq!(count, 1);
    assert!(message.contains(first.as_str()) && message.contains(second.as_str()));
}

/// Recovery starts only after a new broker writer, never merely because the
/// original broker reached the ready state.
#[test]
fn restart_recovery_requires_a_new_writer() {
    let home = common::Home::new();
    let mut store = home.store();
    store.conn.execute("INSERT INTO writer_authority(id,epoch,holder_pid,acquired_at,probe_at) VALUES(1,1,123,1,1)", []).unwrap();
    let restart = store.begin_service_restart("writer-restart", 1.0).unwrap();
    let boot = agent_run_platform::monotonic::boot_id().unwrap();
    assert_eq!(
        store
            .advance_service_restart(restart, &boot, 1_000_000_000)
            .unwrap(),
        0
    );
    assert_eq!(store.recover_service_restart().unwrap(), None);
    store
        .conn
        .execute("UPDATE writer_authority SET epoch=2 WHERE id=1", [])
        .unwrap();
    assert_eq!(store.recover_service_restart().unwrap(), Some(restart));
    assert_eq!(store.recover_service_restart().unwrap(), Some(restart));
}

/// A confirmed manager failure resumes on the same writer while retaining
/// the usual admission barrier until recovery and its notices are complete.
#[test]
fn restart_manager_failure_recovers_on_the_original_writer() {
    let home = common::Home::new();
    let mut store = home.store();
    store
        .conn
        .execute(
            "INSERT INTO writer_authority(id,epoch,holder_pid,acquired_at) VALUES(1,1,123,1)",
            [],
        )
        .unwrap();
    let restart = store.begin_service_restart("failed-manager", 1.0).unwrap();
    let caller = OrchestratorRef {
        transport: "codex".into(),
        external_session_id: "waiting".into(),
        external_turn_id: None,
    };
    store
        .remember_service_restart_caller(restart, &caller)
        .unwrap();
    assert_eq!(
        store
            .recover_service_restart_after_failure(restart)
            .unwrap(),
        None
    );
    let boot = agent_run_platform::monotonic::boot_id().unwrap();
    store.advance_service_restart(restart, &boot, 1).unwrap();
    assert!(
        store
            .recover_service_restart_after_failure(restart + 1)
            .is_err()
    );
    assert_eq!(store.recover_service_restart().unwrap(), None);
    assert!(store.claim_service_restart(restart).unwrap());
    assert!(!store.claim_service_restart(restart).unwrap());
    assert_eq!(
        store
            .recover_service_restart_after_failure(restart)
            .unwrap(),
        Some(restart)
    );
    assert!(
        store
            .refresh_service_restart(restart, &boot, 2, 3.0)
            .unwrap()
    );
    let message: String = store
        .conn
        .query_row(
            "SELECT message FROM service_restart_notices WHERE restart_id=? AND stage='after'",
            [restart],
            |row| row.get(0),
        )
        .unwrap();
    assert!(message.contains("finished its restart procedure"));
    assert!(!message.contains("has restarted"));
}

/// Only native confirmation reports a continuation as started; a definite
/// refusal and a 60-second unknown outcome remain distinct.
#[test]
fn restart_recovery_reports_confirmation_failure_and_timeout() {
    let home = common::Home::new();
    let mut store = home.store();
    let mut agents = Vec::new();
    for _ in 0..3 {
        agents.push(
            store
                .admit(&home.request(), &home.config, &json!({}), None)
                .unwrap()
                .0,
        );
    }
    let restart = store.begin_service_restart("resume-results", 1.0).unwrap();
    store
        .conn
        .execute(
            "UPDATE service_restarts SET phase='recovering' WHERE id=?",
            [restart],
        )
        .unwrap();
    let boot = agent_run_platform::monotonic::boot_id().unwrap();
    for agent in &agents {
        store
            .request_service_restart_resume(restart, agent, &boot, 1_000_000_000)
            .unwrap();
    }
    let child = agent_run_domain::domain::AgentId::new();
    store.conn.execute(
        "INSERT INTO agents(id,runtime,model,profile,task,task_summary,workdir,request_json,status,created_at,config_revision,parent_agent_id,root_agent_id,sequence) \
         SELECT ?1,runtime,model,profile,task,task_summary,workdir,request_json,'running',2,config_revision,id,root_agent_id,2 FROM agents WHERE id=?2",
        rusqlite::params![child.as_str(), agents[0].as_str()],
    ).unwrap();
    store.conn.execute(
        "UPDATE service_restart_agents SET resumed_execution_id=? WHERE restart_id=? AND agent_id=?",
        rusqlite::params![child.as_str(),restart,agents[0].as_str()],
    ).unwrap();
    store.conn.execute(
        "INSERT INTO events(agent_id,at,kind,data_json) VALUES(?,2,'native_resume_accepted','{}')", [child.as_str()],
    ).unwrap();
    store
        .fail_service_restart_resume(restart, &agents[1], "history_unverifiable")
        .unwrap();
    assert!(
        !store
            .refresh_service_restart(restart, &boot, 60_000_000_000, 2.0)
            .unwrap()
    );
    assert!(
        store
            .refresh_service_restart(restart, &boot, 61_000_000_000, 3.0)
            .unwrap()
    );
    let results: Vec<(String,Option<String>)> = agents.iter().map(|id| store.conn.query_row(
        "SELECT resume_status,resume_error FROM service_restart_agents WHERE restart_id=? AND agent_id=?",
        rusqlite::params![restart,id.as_str()], |row|Ok((row.get(0)?,row.get(1)?)),
    ).unwrap()).collect();
    assert_eq!(results[0], ("started".into(), None));
    assert_eq!(
        results[1],
        ("failed".into(), Some("history_unverifiable".into()))
    );
    assert_eq!(results[2].0, "unconfirmed");
    assert!(
        results[2]
            .1
            .as_deref()
            .unwrap()
            .contains("outcome is unknown")
    );
}

/// Each owner gets their agent's result; a first-time blocked caller gets a
/// ready notice even when its hook arrives after the restart finished.
#[test]
fn restart_queues_personal_results_and_first_caller_notice() {
    let home = common::Home::new();
    let mut store = home.store();
    let mut request = home.request();
    request.orchestrator = Some(OrchestratorRef {
        transport: "codex".into(),
        external_session_id: "owner".into(),
        external_turn_id: None,
    });
    let (agent, _) = store
        .admit(&request, &home.config, &json!({}), None)
        .unwrap();
    let (resumed, _) = store
        .admit(&request, &home.config, &json!({}), None)
        .unwrap();
    let restart = store.begin_service_restart("notice-restart", 1.0).unwrap();
    let waiter = OrchestratorRef {
        transport: "claude".into(),
        external_session_id: "first-caller".into(),
        external_turn_id: None,
    };
    store
        .remember_service_restart_caller(restart, &waiter)
        .unwrap();
    store
        .conn
        .execute(
            "UPDATE service_restarts SET phase='recovering' WHERE id=?",
            [restart],
        )
        .unwrap();
    let boot = agent_run_platform::monotonic::boot_id().unwrap();
    store
        .request_service_restart_resume(restart, &agent, &boot, 1_000_000_000)
        .unwrap();
    store
        .fail_service_restart_resume(restart, &agent, "history_unverifiable")
        .unwrap();
    store.conn.execute(
        "UPDATE service_restart_agents SET resume_status='started' WHERE restart_id=? AND agent_id=?",
        rusqlite::params![restart, resumed.as_str()],
    ).unwrap();
    assert!(
        store
            .refresh_service_restart(restart, &boot, 2_000_000_000, 3.0)
            .unwrap()
    );
    let message: String = store.conn.query_row(
        "SELECT message FROM service_restart_notices WHERE restart_id=? AND external_session_id='owner' AND stage='after'",
        [restart], |row|row.get(0),
    ).unwrap();
    assert!(message.contains(agent.as_str()) && message.contains("failed — history_unverifiable"));
    assert!(message.contains(resumed.as_str()) && message.contains("resumed"));
    assert_eq!(store.conn.query_row(
        "SELECT COUNT(*) FROM service_restart_notices WHERE restart_id=? AND external_session_id='owner' AND stage='after'",
        [restart], |row|row.get::<_,i64>(0),
    ).unwrap(), 1);
    let caller: String = store.conn.query_row(
        "SELECT message FROM service_restart_notices WHERE restart_id=? AND external_session_id='first-caller' AND stage='after'",
        [restart], |row|row.get(0),
    ).unwrap();
    assert!(caller.contains("New agent starts are available"));
    store
        .remember_service_restart_caller(restart, &waiter)
        .unwrap();
    assert_eq!(store.conn.query_row(
        "SELECT COUNT(*) FROM service_restart_notices WHERE restart_id=? AND external_session_id='first-caller'",
        [restart], |row| row.get::<_,i64>(0),
    ).unwrap(), 1, "a delayed hook must not duplicate the existing notice");
    let late = OrchestratorRef {
        transport: "codex".into(),
        external_session_id: "late-caller".into(),
        external_turn_id: None,
    };
    store
        .remember_service_restart_caller(restart, &late)
        .unwrap();
    assert_eq!(store.conn.query_row(
        "SELECT COUNT(*) FROM service_restart_notices WHERE restart_id=? AND external_session_id='late-caller' AND stage='after'",
        [restart], |row|row.get::<_,i64>(0),
    ).unwrap(), 1);
}

/// The second warning and forced cancellation happen only after their own
/// 30-second intervals, and repeated ticks never duplicate a command.
#[test]
fn restart_warns_twice_then_cancels_unresponsive_agent() {
    let home = common::Home::new();
    let mut store = home.store();
    let (agent, _) = store
        .admit(&home.request(), &home.config, &json!({}), None)
        .unwrap();
    let restart = store.begin_service_restart("timed-restart", 1.0).unwrap();
    let boot = agent_run_platform::monotonic::boot_id().unwrap();
    let second = 1_000_000_000_u64;
    let advance = |store: &mut agent_run_store::Store<'_>, seconds| {
        store
            .advance_service_restart(restart, &boot, seconds * second)
            .unwrap()
    };
    assert_eq!(advance(&mut store, 1), 1);
    assert_eq!(advance(&mut store, 30), 0);
    assert_eq!(advance(&mut store, 31), 1);
    assert_eq!(advance(&mut store, 60), 0);
    assert_eq!(advance(&mut store, 61), 1);
    assert_eq!(advance(&mut store, 120), 0);
    let commands = store
        .conn
        .prepare("SELECT kind,payload_json FROM commands WHERE agent_id=? ORDER BY id")
        .unwrap()
        .query_map([agent.as_str()], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
        })
        .unwrap()
        .collect::<rusqlite::Result<Vec<_>>>()
        .unwrap();
    assert_eq!(
        commands
            .iter()
            .map(|entry| entry.0.as_str())
            .collect::<Vec<_>>(),
        ["steer", "steer", "cancel"]
    );
    assert!(commands[0].1.contains("call finish"));
    assert!(commands[1].1.contains("interrupted"));
    assert!(
        store
            .advance_service_restart(restart, "different-boot", 121 * second)
            .is_err()
    );
}

/// An already requested cancellation is captured separately and is not
/// replaced by a restart warning or a new cancellation.
#[test]
fn restart_preserves_prior_user_cancellation() {
    let home = common::Home::new();
    let mut store = home.store();
    let (cancelled, _) = store
        .admit(&home.request(), &home.config, &json!({}), None)
        .unwrap();
    let (working, _) = store
        .admit(&home.request(), &home.config, &json!({}), None)
        .unwrap();
    store
        .bind_orchestrator(
            &cancelled,
            &OrchestratorRef {
                transport: "codex".into(),
                external_session_id: "cancelled-owner".into(),
                external_turn_id: None,
            },
            1.0,
        )
        .unwrap();
    store.enqueue(&cancelled, "cancel", &json!({})).unwrap();
    let restart = store
        .begin_service_restart("cancelled-before-restart", 1.0)
        .unwrap();
    for (agent, expected) in [(&cancelled, true), (&working, false)] {
        let prior: bool = store.conn.query_row(
            "SELECT cancelled_before_restart FROM service_restart_agents WHERE restart_id=? AND agent_id=?",
            rusqlite::params![restart, agent.as_str()], |row| row.get(0),
        ).unwrap();
        assert_eq!(prior, expected);
    }
    let warned: i64 = store.conn.query_row(
        "SELECT COUNT(*) FROM service_restart_recipients WHERE restart_id=? AND external_session_id='cancelled-owner'",
        [restart], |row| row.get(0),
    ).unwrap();
    assert_eq!(warned, 0);
    // The worker may have accepted the old cancel but still be cleaning up.
    store.conn.execute(
        "UPDATE commands SET state='completed',result_json='{\"accepted\":true}' WHERE agent_id=? AND kind='cancel'",
        [cancelled.as_str()],
    ).unwrap();
    let boot = agent_run_platform::monotonic::boot_id().unwrap();
    assert_eq!(
        store
            .advance_service_restart(restart, &boot, 1_000_000_000)
            .unwrap(),
        1
    );
    let commands: Vec<String> = store
        .conn
        .prepare("SELECT kind FROM commands WHERE agent_id=? ORDER BY id")
        .unwrap()
        .query_map([cancelled.as_str()], |row| row.get(0))
        .unwrap()
        .collect::<rusqlite::Result<_>>()
        .unwrap();
    assert_eq!(commands, ["cancel"]);
}

/// A finish accepted before the restart stays a normal completion; the
/// restart waits for cleanup but does not capture it for continuation.
#[test]
fn restart_does_not_resume_a_previously_finished_agent() {
    use agent_run_domain::{
        domain::Outcome,
        worker::{FinishGuard, FinishRequest, FinishStatus},
    };
    let home = common::Home::new();
    let mut store = home.store();
    let mut request = home.request();
    request.explicit_finish = true;
    request.orchestrator = Some(OrchestratorRef {
        transport: "codex".into(),
        external_session_id: "finished-owner".into(),
        external_turn_id: None,
    });
    let (agent, _) = store
        .admit(&request, &home.config, &json!({}), None)
        .unwrap();
    store
        .conn
        .execute(
            "UPDATE agents SET status='running' WHERE id=?",
            [agent.as_str()],
        )
        .unwrap();
    store.conn.execute("INSERT INTO attempts(id,agent_id,number,state,adapter_state_json,created_at,ownership_active) VALUES('attempt',?,1,'running','{}',1,1)", [agent.as_str()]).unwrap();
    let token = "a".repeat(64);
    store
        .issue_worker_capability(&agent, "attempt", &token, 1.0)
        .unwrap();
    store
        .begin_worker_lifecycle(&agent, "attempt", 1.0)
        .unwrap();
    let finish = FinishRequest {
        summary: "Finished work".into(),
        status: FinishStatus::Done,
    };
    store
        .accept_worker_finish(&agent, "attempt", &token, &finish, 2.0, FinishGuard::Clean)
        .unwrap();
    store.observe_worker_finish_receipt(&agent).unwrap();
    let restart = store.begin_service_restart("previous-finish", 3.0).unwrap();
    assert_eq!(
        store
            .conn
            .query_row(
                "SELECT COUNT(*) FROM service_restart_agents WHERE restart_id=?",
                [restart],
                |row| row.get::<_, i64>(0)
            )
            .unwrap(),
        0
    );
    let boot = agent_run_platform::monotonic::boot_id().unwrap();
    store.advance_service_restart(restart, &boot, 1).unwrap();
    assert_eq!(
        store
            .conn
            .query_row(
                "SELECT phase FROM service_restarts WHERE id=?",
                [restart],
                |row| row.get::<_, String>(0)
            )
            .unwrap(),
        "preparing"
    );
    let root = home.path.join("agents").join(agent.as_str());
    std::fs::create_dir_all(&root).unwrap();
    let proof =
        agent_run_platform::verify::seal(&root, std::path::Path::new("answer.md"), &finish.summary)
            .unwrap();
    store
        .finish(&agent, &Outcome::success(None), Some(&proof), None)
        .unwrap();
    store.advance_service_restart(restart, &boot, 2).unwrap();
    assert_eq!(
        store
            .conn
            .query_row(
                "SELECT phase FROM service_restarts WHERE id=?",
                [restart],
                |row| row.get::<_, String>(0)
            )
            .unwrap(),
        "ready"
    );
    assert!(store.get(&agent).unwrap().answer_path.is_some());
    assert_eq!(
        store
            .conn
            .query_row(
                "SELECT COUNT(*) FROM deliveries WHERE agent_id=?",
                [agent.as_str()],
                |row| row.get::<_, i64>(0)
            )
            .unwrap(),
        1
    );
}

/// A normal finish remains a completion even after a restart warning is queued.
#[test]
fn restart_does_not_resume_work_finished_normally() {
    use agent_run_domain::{
        domain::Outcome,
        worker::{FinishGuard, FinishRequest, FinishStatus},
    };
    let home = common::Home::new();
    let mut store = home.store();
    let mut request = home.request();
    request.explicit_finish = true;
    request.output_schema = Some(json!({"type":"object"}).as_object().unwrap().clone());
    request.orchestrator = Some(OrchestratorRef {
        transport: "codex".into(),
        external_session_id: "owner".into(),
        external_turn_id: None,
    });
    let (agent, _) = store
        .admit(&request, &home.config, &json!({}), None)
        .unwrap();
    store
        .conn
        .execute(
            "UPDATE agents SET status='running' WHERE id=?",
            [agent.as_str()],
        )
        .unwrap();
    store.conn.execute("INSERT INTO attempts(id,agent_id,number,state,adapter_state_json,created_at,ownership_active) VALUES('attempt',?,1,'running','{}',1,1)", [agent.as_str()]).unwrap();
    let token = "a".repeat(64);
    store
        .issue_worker_capability(&agent, "attempt", &token, 1.0)
        .unwrap();
    store
        .begin_worker_lifecycle(&agent, "attempt", 1.0)
        .unwrap();
    let restart = store.begin_service_restart("completion-race", 2.0).unwrap();
    store.conn.execute("UPDATE deliveries SET state='delivered' WHERE id IN (SELECT delivery_id FROM service_restart_notices WHERE restart_id=?)", [restart]).unwrap();
    let boot = agent_run_platform::monotonic::boot_id().unwrap();
    assert_eq!(
        store
            .advance_service_restart(restart, &boot, 1_000_000_000)
            .unwrap(),
        1
    );
    let invalid = FinishRequest {
        summary: "not JSON".into(),
        status: FinishStatus::Done,
    };
    assert!(
        store
            .accept_worker_finish(&agent, "attempt", &token, &invalid, 2.5, FinishGuard::Clean)
            .is_err()
    );
    let finish = FinishRequest {
        summary: "{\"done\":true}".into(),
        status: FinishStatus::Done,
    };
    store
        .accept_worker_finish(&agent, "attempt", &token, &finish, 3.0, FinishGuard::Clean)
        .unwrap();
    store.observe_worker_finish_receipt(&agent).unwrap();
    assert_eq!(
        store
            .advance_service_restart(restart, &boot, 61_000_000_000)
            .unwrap(),
        0
    );
    let root = home.path.join("agents").join(agent.as_str());
    std::fs::create_dir_all(&root).unwrap();
    let proof =
        agent_run_platform::verify::seal(&root, std::path::Path::new("answer.md"), &finish.summary)
            .unwrap();
    store
        .finish(&agent, &Outcome::success(None), Some(&proof), None)
        .unwrap();
    assert!(store.get(&agent).unwrap().answer_path.is_some());
    assert_eq!(
        store.get(&agent).unwrap().status,
        agent_run_domain::domain::Status::Succeeded
    );
    assert!(
        store
            .last_event(&agent, "status")
            .unwrap()
            .unwrap()
            .get("service_restart")
            .is_none()
    );
    assert_eq!(
        store
            .conn
            .query_row(
                "SELECT resume_status FROM service_restart_agents WHERE restart_id=? AND agent_id=?",
                rusqlite::params![restart, agent.as_str()],
                |r| r.get::<_, String>(0)
            )
            .unwrap(),
        "finished"
    );
    assert_eq!(
        store
            .conn
            .query_row(
                "SELECT COUNT(*) FROM deliveries WHERE agent_id=?",
                [agent.as_str()],
                |r| r.get::<_, i64>(0)
            )
            .unwrap(),
        1
    );
}

/// User cancellation, including during the technical pause, fences resume.
#[test]
fn restart_honors_user_cancellation_during_preparation_and_pause() {
    use agent_run_domain::domain::{Outcome, Status};
    let home = common::Home::new();
    let mut store = home.store();
    let (cancelled, _) = store
        .admit(&home.request(), &home.config, &json!({}), None)
        .unwrap();
    let (paused, _) = store
        .admit(&home.request(), &home.config, &json!({}), None)
        .unwrap();
    store
        .conn
        .execute(
            "UPDATE agents SET status='running' WHERE id IN (?,?)",
            rusqlite::params![cancelled.as_str(), paused.as_str()],
        )
        .unwrap();
    let restart = store.begin_service_restart("cancel-during", 1.0).unwrap();
    store.enqueue(&cancelled, "cancel", &json!({})).unwrap();
    let boot = agent_run_platform::monotonic::boot_id().unwrap();
    for second in [1, 31, 61] {
        store
            .advance_service_restart(restart, &boot, second * 1_000_000_000)
            .unwrap();
    }
    let mut outcome = Outcome::failure("cancelled");
    outcome.status = Status::Cancelled;
    store.finish(&cancelled, &outcome, None, None).unwrap();
    store.finish(&paused, &outcome, None, None).unwrap();
    assert_eq!(
        store.enqueue(&paused, "cancel", &json!({})).unwrap()["state"],
        "completed"
    );
    for agent in [&cancelled, &paused] {
        let status: String = store.conn.query_row("SELECT resume_status FROM service_restart_agents WHERE restart_id=? AND agent_id=?", rusqlite::params![restart,agent.as_str()], |row| row.get(0)).unwrap();
        assert_eq!(status, "cancelled");
    }
    assert_eq!(
        agent_run_store::service_restart::restart_view(&store.conn, &paused)
            .unwrap()
            .unwrap()
            .0,
        "cancelled"
    );
    store
        .advance_service_restart(restart, &boot, 62_000_000_000)
        .unwrap();
    assert_eq!(
        store
            .conn
            .query_row(
                "SELECT phase FROM service_restarts WHERE id=?",
                [restart],
                |row| row.get::<_, String>(0)
            )
            .unwrap(),
        "ready"
    );
}

/// A cancel resolved against a paused source still reaches the child if
/// automatic admission wins the race immediately before the cancel commits.
#[test]
fn restart_cancellation_reaches_a_just_admitted_child() {
    use agent_run_domain::domain::{AgentId, Outcome, Status};
    for completed in [false, true] {
        let home = common::Home::new();
        let mut store = home.store();
        let (parent, _) = store
            .admit(&home.request(), &home.config, &json!({}), None)
            .unwrap();
        store
            .conn
            .execute(
                "UPDATE agents SET status='running' WHERE id=?",
                [parent.as_str()],
            )
            .unwrap();
        let restart = store.begin_service_restart("cancel-race", 1.0).unwrap();
        let boot = agent_run_platform::monotonic::boot_id().unwrap();
        for second in [1, 31, 61] {
            store
                .advance_service_restart(restart, &boot, second * 1_000_000_000)
                .unwrap();
        }
        let mut outcome = Outcome::failure("cancelled");
        outcome.status = Status::Cancelled;
        store.finish(&parent, &outcome, None, None).unwrap();
        store
            .conn
            .execute(
                "UPDATE service_restarts SET phase='recovering' WHERE id=?",
                [restart],
            )
            .unwrap();
        let child = AgentId::new();
        store.conn.execute(
        "INSERT INTO agents(id,runtime,model,profile,task,task_summary,workdir,request_json,status,created_at,config_revision,parent_agent_id,root_agent_id,sequence) \
         SELECT ?1,runtime,model,profile,task,task_summary,workdir,request_json,'running',2,config_revision,id,root_agent_id,2 FROM agents WHERE id=?2",
        rusqlite::params![child.as_str(),parent.as_str()],
    ).unwrap();
        store.conn.execute("UPDATE service_restart_agents SET resumed_execution_id=? WHERE restart_id=? AND agent_id=?", rusqlite::params![child.as_str(),restart,parent.as_str()]).unwrap();
        if completed {
            store
                .conn
                .execute(
                    "UPDATE service_restart_agents SET resume_status='started' WHERE restart_id=?",
                    [restart],
                )
                .unwrap();
            store
                .conn
                .execute(
                    "UPDATE service_restarts SET phase='complete',finished_at=100 WHERE id=?",
                    [restart],
                )
                .unwrap();
        }
        store.enqueue(&parent, "cancel", &json!({})).unwrap();
        let status: String = store
        .conn
        .query_row(
            "SELECT resume_status FROM service_restart_agents WHERE restart_id=? AND agent_id=?",
            rusqlite::params![restart, parent.as_str()],
            |row| row.get(0),
        )
        .unwrap();
        assert_eq!(status, if completed { "started" } else { "cancelled" });
        assert_eq!(store.conn.query_row("SELECT COUNT(*) FROM commands WHERE agent_id=? AND kind='cancel' AND state='pending'", [child.as_str()], |row| row.get::<_,i64>(0)).unwrap(), 1);
        if !completed {
            assert_eq!(
                agent_run_store::service_restart::restart_view(&store.conn, &parent)
                    .unwrap()
                    .unwrap()
                    .0,
                "cancelling"
            );
            assert!(
                !store
                    .refresh_service_restart(restart, &boot, 62_000_000_000, 62.0)
                    .unwrap()
            );
            store
                .conn
                .execute(
                    "UPDATE agents SET status='cancelled' WHERE id=?",
                    [child.as_str()],
                )
                .unwrap();
            assert_eq!(
                agent_run_store::service_restart::restart_view(&store.conn, &parent)
                    .unwrap()
                    .unwrap()
                    .0,
                "cancelled"
            );
            assert!(
                store
                    .refresh_service_restart(restart, &boot, 63_000_000_000, 63.0)
                    .unwrap()
            );
        }
    }
}

/// An unconfirmed continuation remains cancellable after the restart cycle ends.
#[test]
fn restart_unconfirmed_pause_can_be_cancelled() {
    use agent_run_domain::domain::{Outcome, Status};
    let home = common::Home::new();
    let mut store = home.store();
    let (agent, _) = store
        .admit(&home.request(), &home.config, &json!({}), None)
        .unwrap();
    store
        .conn
        .execute(
            "UPDATE agents SET status='running' WHERE id=?",
            [agent.as_str()],
        )
        .unwrap();
    let restart = store.begin_service_restart("cancel-unknown", 1.0).unwrap();
    let boot = agent_run_platform::monotonic::boot_id().unwrap();
    for second in [1, 31, 61] {
        store
            .advance_service_restart(restart, &boot, second * 1_000_000_000)
            .unwrap();
    }
    let mut outcome = Outcome::failure("cancelled");
    outcome.status = Status::Cancelled;
    store.finish(&agent, &outcome, None, None).unwrap();
    store
        .conn
        .execute(
            "UPDATE service_restart_agents SET resume_status='unconfirmed' WHERE restart_id=?",
            [restart],
        )
        .unwrap();
    store
        .conn
        .execute(
            "UPDATE service_restarts SET phase='complete',finished_at=100 WHERE id=?",
            [restart],
        )
        .unwrap();
    assert_eq!(
        store.enqueue(&agent, "cancel", &json!({})).unwrap()["state"],
        "completed"
    );
    assert_eq!(
        agent_run_store::service_restart::restart_view(&store.conn, &agent)
            .unwrap()
            .unwrap()
            .0,
        "cancelled"
    );
}

/// A marked restart finish is not a completion notice, even when the agent's
/// final answer normally requires a JSON schema.
#[test]
fn restart_finish_does_not_publish_a_completion() {
    use agent_run_domain::{
        domain::Outcome,
        worker::{FinishGuard, FinishRequest, FinishStatus},
    };
    let home = common::Home::new();
    let mut store = home.store();
    let mut request = home.request();
    request.explicit_finish = true;
    request.output_schema = Some(json!({"type":"object"}).as_object().unwrap().clone());
    request.orchestrator = Some(OrchestratorRef {
        transport: "codex".into(),
        external_session_id: "session-one".into(),
        external_turn_id: None,
    });
    let (agent, _) = store
        .admit(&request, &home.config, &json!({}), None)
        .unwrap();
    store
        .conn
        .execute(
            "UPDATE agents SET status='running' WHERE id=?",
            [agent.as_str()],
        )
        .unwrap();
    store.conn.execute("INSERT INTO attempts(id,agent_id,number,state,adapter_state_json,created_at,ownership_active) VALUES('attempt',?,1,'running','{}',1,1)", [agent.as_str()]).unwrap();
    let token = "a".repeat(64);
    store
        .issue_worker_capability(&agent, "attempt", &token, 1.0)
        .unwrap();
    store
        .begin_worker_lifecycle(&agent, "attempt", 1.0)
        .unwrap();
    let mut finish = FinishRequest {
        summary: "Work in progress".into(),
        status: FinishStatus::Done,
    };
    assert!(
        store
            .accept_worker_finish(&agent, "attempt", &token, &finish, 2.0, FinishGuard::Clean)
            .is_err()
    );
    finish.summary = "[agent-run-restart-finish:1]\nWork in progress".into();
    assert!(
        store
            .accept_worker_finish(&agent, "attempt", &token, &finish, 2.5, FinishGuard::Clean)
            .is_err()
    );
    let restart = store.begin_service_restart("finish-restart", 3.0).unwrap();
    finish.summary = format!("[agent-run-restart-finish:{restart}]\nWork in progress");
    assert!(
        store
            .accept_worker_finish(&agent, "attempt", &token, &finish, 3.1, FinishGuard::Clean)
            .is_err()
    );
    store.conn.execute("UPDATE deliveries SET state='delivered' WHERE id IN (SELECT delivery_id FROM service_restart_notices WHERE restart_id=?)", [restart]).unwrap();
    let boot = agent_run_platform::monotonic::boot_id().unwrap();
    assert_eq!(
        store
            .advance_service_restart(restart, &boot, 1_000_000_000)
            .unwrap(),
        1
    );
    let mut wrong = finish.clone();
    wrong.summary = "[agent-run-restart-finish:999]\nWork in progress".into();
    assert!(
        store
            .accept_worker_finish(&agent, "attempt", &token, &wrong, 3.2, FinishGuard::Clean)
            .is_err()
    );
    wrong.summary = format!("[agent-run-restart-finish:{restart}]\nWork in progress");
    wrong.status = FinishStatus::Blocked;
    assert!(
        store
            .accept_worker_finish(&agent, "attempt", &token, &wrong, 3.3, FinishGuard::Clean)
            .is_err()
    );
    wrong.status = FinishStatus::Done;
    wrong.summary = format!("[agent-run-restart-finish:{restart}]\n ");
    assert!(
        store
            .accept_worker_finish(&agent, "attempt", &token, &wrong, 3.4, FinishGuard::Clean)
            .is_err()
    );
    store
        .accept_worker_finish(&agent, "attempt", &token, &finish, 4.0, FinishGuard::Clean)
        .unwrap();
    store.observe_worker_finish_receipt(&agent).unwrap();
    assert_eq!(
        store
            .advance_service_restart(restart, &boot, 61_000_000_000)
            .unwrap(),
        1
    );
    let root = home.path.join("agents").join(agent.as_str());
    std::fs::create_dir_all(&root).unwrap();
    let proof =
        agent_run_platform::verify::seal(&root, std::path::Path::new("answer.md"), &finish.summary)
            .unwrap();
    store
        .finish(&agent, &Outcome::success(None), Some(&proof), None)
        .unwrap();
    assert!(store.get(&agent).unwrap().answer_path.is_none());
    assert_eq!(
        store
            .conn
            .query_row(
                "SELECT COUNT(*) FROM deliveries WHERE agent_id=?",
                [agent.as_str()],
                |row| row.get::<_, i64>(0)
            )
            .unwrap(),
        0
    );
    assert_eq!(
        store.last_event(&agent, "status").unwrap().unwrap()["service_restart"],
        true
    );
    store
        .bind_orchestrator(&agent, request.orchestrator.as_ref().unwrap(), 5.0)
        .unwrap();
    assert_eq!(
        store
            .conn
            .query_row(
                "SELECT COUNT(*) FROM deliveries WHERE agent_id=?",
                [agent.as_str()],
                |row| row.get::<_, i64>(0)
            )
            .unwrap(),
        0
    );
}

/// A timeout-driven cancellation is a technical interruption, not a final
/// cancellation notice to the orchestrator.
#[test]
fn restart_forced_stop_does_not_publish_a_cancellation() {
    use agent_run_domain::domain::{Outcome, Status};
    let home = common::Home::new();
    let mut store = home.store();
    let mut request = home.request();
    request.orchestrator = Some(OrchestratorRef {
        transport: "codex".into(),
        external_session_id: "session-one".into(),
        external_turn_id: None,
    });
    let (agent, _) = store
        .admit(&request, &home.config, &json!({}), None)
        .unwrap();
    store
        .conn
        .execute(
            "UPDATE agents SET status='running' WHERE id=?",
            [agent.as_str()],
        )
        .unwrap();
    let restart = store.begin_service_restart("forced-restart", 1.0).unwrap();
    store.conn.execute("UPDATE deliveries SET state='delivered' WHERE id IN (SELECT delivery_id FROM service_restart_notices WHERE restart_id=?)", [restart]).unwrap();
    let boot = agent_run_platform::monotonic::boot_id().unwrap();
    for second in [1, 31, 61] {
        store
            .advance_service_restart(restart, &boot, second * 1_000_000_000)
            .unwrap();
    }
    let mut outcome = Outcome::failure("cancelled");
    outcome.status = Status::Cancelled;
    store.finish(&agent, &outcome, None, None).unwrap();
    assert_eq!(
        store.last_event(&agent, "status").unwrap().unwrap()["service_restart"],
        true
    );
    assert_eq!(
        store
            .conn
            .query_row(
                "SELECT COUNT(*) FROM deliveries WHERE agent_id=?",
                [agent.as_str()],
                |row| row.get::<_, i64>(0)
            )
            .unwrap(),
        0
    );
}
