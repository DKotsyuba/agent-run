//! Provider-free regression for lost application-level wakes and bounded ping recovery.
use agent_run_domain::{
    Error,
    domain::AgentId,
    health::{PingPolicy, PingReply, ReplyState},
};
use agent_run_store::Store;
use serde_json::{Value, json};

/// Synthetic worker capability; no provider credentials or paid inference.
const TOKEN: &str = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";

/// Create a running explicit attempt at a controlled observation clock.
fn running(policy: PingPolicy) -> (tempfile::TempDir, Store<'static>, AgentId) {
    let home = tempfile::tempdir().unwrap();
    let mut store = agent_run_store::fixture::initialize(home.path()).unwrap();
    let run: AgentId = "ag-20261018-000000-0123456789".parse().unwrap();
    store.conn.execute("INSERT INTO agents(id,runtime,model,profile,task,task_summary,workdir,request_json,status,created_at,config_revision,root_agent_id) VALUES(?,'codex','fixture','review','task','task',?,?,'running',100,'fixture',?)",
        rusqlite::params![run.as_str(),home.path().to_str().unwrap(),json!({"runtime":"codex","model":"fixture","profile":"review","task":"task","workdir":home.path(),"explicit_finish":true,"idle_ping":policy}).to_string(),run.as_str()]).unwrap();
    store.conn.execute("INSERT INTO attempts(id,agent_id,number,state,adapter_state_json,created_at,ownership_active) VALUES('attempt',?,1,'running','{}',100,1)",[run.as_str()]).unwrap();
    store
        .issue_worker_capability(&run, "attempt", TOKEN, 100.0)
        .unwrap();
    store
        .register_worker_catalog(
            &agent_run_domain::worker::WorkerCatalogProof {
                run_id: run.clone(),
                attempt_id: "attempt".into(),
                token: TOKEN.into(),
                version: agent_run_domain::worker::FINISH_CATALOG_VERSION,
                digest: agent_run_domain::worker::finish_catalog_digest(false),
            },
            100.0,
        )
        .unwrap();
    store.conn.execute("INSERT INTO events(agent_id,attempt_id,at,kind,data_json) VALUES(?,'attempt',100,'native_tool_observer_v1','{}')",[run.as_str()]).unwrap();
    store
        .begin_worker_lifecycle(&run, "attempt", 100.0)
        .unwrap();
    store.worker_turn(&run, true, 101.0).unwrap();
    (home, store, run)
}

/// Reply to a service probe at a deterministic fixture time; no model turn.
fn control(store: &mut Store<'_>, run: &AgentId, at: f64) {
    let row:Option<(i64,String)>=store.conn.query_row("SELECT id,payload_json FROM commands WHERE agent_id=? AND kind='ping' AND state='pending' ORDER BY id LIMIT 1",[run.as_str()],|r|Ok((r.get(0)?,r.get(1)?))).ok();
    if let Some((id, payload)) = row {
        assert!(
            store
                .health_command_current(
                    run,
                    &serde_json::from_str::<Value>(&payload).unwrap(),
                    false,
                    at,
                )
                .unwrap()
        );
        store
            .conn
            .execute(
                "UPDATE commands SET state='claimed',claimed_at=? WHERE id=?",
                rusqlite::params![at, id],
            )
            .unwrap();
        store
            .complete_command(run, id, &json!({"accepted":true,"native_alive":true}))
            .unwrap();
        store
            .conn
            .execute(
                "UPDATE commands SET completed_at=? WHERE id=?",
                rusqlite::params![at, id],
            )
            .unwrap();
    }
}

/// Reach idle grace with responsive service probes and no task producer.
fn stalled(store: &mut Store<'_>, run: &AgentId) {
    for at in [160.0, 220.0, 280.0, 340.0, 400.0] {
        assert!(store.health_tick(run, at).unwrap());
        control(store, run, at + 1.0);
    }
    assert!(store.health_tick(run, 460.0).unwrap());
}

/// Select and claim the sole model challenge as its existing adapter would.
fn model(store: &mut Store<'_>, run: &AgentId) -> (i64, Value, PingReply) {
    let (id, kind, payload) = store.claim_command_for_turn(run, true).unwrap().unwrap();
    assert_eq!(kind, "steer");
    assert!(
        store
            .health_command_current(run, &payload, true, 461.0)
            .unwrap()
    );
    let reply = PingReply {
        ping_id: payload["health_ping"].as_str().unwrap().into(),
        state: ReplyState::Working,
    };
    (id, payload, reply)
}

/// Count model probes, not service probes or user steering commands.
fn models(store: &Store<'_>, run: &AgentId) -> i64 {
    store.conn.query_row("SELECT COUNT(*) FROM commands WHERE agent_id=? AND json_extract(payload_json,'$.health_ping') IS NOT NULL",[run.as_str()],|r|r.get(0)).unwrap()
}

/// Broker health traffic never authorizes model work by default or terminates
/// an idle run; the original lost-wake incident becomes one actionable notice.
#[test]
fn observe_detects_unexplained_idle_without_paid_turns() {
    let (_home, mut store, run) = running(PingPolicy::Observe);
    stalled(&mut store, &run);
    assert_eq!(models(&store, &run), 0);
    assert_eq!(
        store.health_view(&run).unwrap().unwrap()["reason"],
        "idle_without_wait_source"
    );
    control(&mut store, &run, 461.0);
    store.health_tick(&run, 520.0).unwrap();
    assert_eq!(
        store
            .conn
            .query_row(
                "SELECT COUNT(*) FROM worker_notifications WHERE agent_id=?",
                [run.as_str()],
                |r| r.get::<_, i64>(0)
            )
            .unwrap(),
        1
    );
    assert_eq!(store.get(&run).unwrap().status.as_str(), "running");
    assert!(store.worker_finish_intent(&run, false).unwrap().is_none());
}

/// An explicit pause/historical off mode has no monitor or synthetic model input.
#[test]
fn off_and_historical_attempts_remain_untouched() {
    let (_home, mut store, run) = running(PingPolicy::Off);
    assert!(store.health_view(&run).unwrap().is_none());
    assert!(store.health_due(1e12, 100).unwrap().is_empty());
    assert!(!store.health_tick(&run, 1e12).unwrap());
    assert_eq!(models(&store, &run), 0);
}

/// A real outstanding native command is allowed to take arbitrarily long.
/// A synchronous MCP pending response counts as completed, not such a producer.
#[test]
fn real_pending_native_work_is_not_a_hang() {
    let (_home, mut store, run) = running(PingPolicy::Recover);
    store.conn.execute("INSERT INTO messages(agent_id,attempt_id,at,role,name,content,raw_ref) VALUES(?,'attempt',102,'tool_call','command','','owned-job')",[run.as_str()]).unwrap();
    stalled(&mut store, &run);
    assert_eq!(models(&store, &run), 0);
    assert_eq!(
        store.health_view(&run).unwrap().unwrap()["waiting_for"],
        "native_tool"
    );
    control(&mut store, &run, 461.0);
    store.health_tick(&run, 1e9).unwrap();
    assert_eq!(models(&store, &run), 0);
    assert_eq!(store.get(&run).unwrap().status.as_str(), "running");
}

/// Exactly one control turn is admitted; restart/retry/pong traffic cannot
/// manufacture progress or cause repeated paid wakeups of the same idle work.
#[test]
fn recovery_reply_is_authenticated_durable_and_budgeted() {
    let (home, mut store, run) = running(PingPolicy::Recover);
    stalled(&mut store, &run);
    assert_eq!(models(&store, &run), 1);
    let (id, payload, reply) = model(&mut store, &run);
    assert!(
        store
            .accept_health_reply(&run, "attempt", &"b".repeat(64), &reply, 461.0)
            .is_err()
    );
    assert!(
        !store
            .accept_health_reply(&run, "attempt", TOKEN, &reply, 461.0)
            .unwrap()
    );
    assert!(
        store
            .accept_health_reply(&run, "attempt", TOKEN, &reply, 462.0)
            .unwrap()
    );
    store.worker_turn(&run, false, 461.0).unwrap();
    store.worker_turn(&run, true, 463.0).unwrap();
    let changed = PingReply {
        state: ReplyState::Waiting,
        ..reply.clone()
    };
    assert!(matches!(
        store.accept_health_reply(&run, "attempt", TOKEN, &changed, 462.0),
        Err(Error::Conflict)
    ));
    store
        .complete_command(&run, id, &json!({"accepted":true}))
        .unwrap();
    store
        .conn
        .execute("UPDATE commands SET completed_at=461 WHERE id=?", [id])
        .unwrap();
    store.conn.execute("INSERT INTO messages(agent_id,attempt_id,at,role,name,content,raw_ref) VALUES(?,'attempt',462,'tool_result','agent_run_worker/health_reply','pong','health')",[run.as_str()]).unwrap();
    control(&mut store, &run, 463.0);
    drop(store);
    let mut store = agent_run_store::fixture::open(home.path()).unwrap();
    assert!(
        store
            .accept_health_reply(&run, "attempt", TOKEN, &reply, 1e8)
            .unwrap()
    );
    store.health_tick(&run, 580.0).unwrap();
    assert_eq!(models(&store, &run), 1);
    assert_eq!(
        store.health_view(&run).unwrap().unwrap()["reason"],
        "idle_without_wait_source_after_ping"
    );
    assert!(
        !store
            .health_command_current(&run, &payload, true, 461.0)
            .unwrap()
    );
}

/// Work admitted after selection fences stale queued model input; cancellation
/// and finish also win before any native bytes can be written.
#[test]
fn queued_ping_cannot_interrupt_new_work_or_cancel() {
    let (_home, mut store, run) = running(PingPolicy::Recover);
    stalled(&mut store, &run);
    let (_id, payload, reply) = model(&mut store, &run);
    let forged = PingReply {
        ping_id: "f".repeat(64),
        ..reply
    };
    assert!(
        store
            .accept_health_reply(&run, "attempt", TOKEN, &forged, 461.0)
            .is_err()
    );
    store.conn.execute("INSERT INTO messages(agent_id,attempt_id,at,role,name,content,raw_ref) VALUES(?,'attempt',462,'tool_call','command','','new-job')",[run.as_str()]).unwrap();
    assert!(
        !store
            .health_command_current(&run, &payload, true, 461.0)
            .unwrap()
    );
    store.enqueue(&run, "cancel", &json!({})).unwrap();
    assert!(
        !store
            .health_command_current(&run, &payload, false, 461.0)
            .unwrap()
    );
    assert!(!store.health_tick(&run, 1e9).unwrap());
}

/// A missing service response remains unknown, cannot trigger inference or
/// cancellation, and is distinct from a model that failed to answer a challenge.
#[test]
fn unresponsive_control_reports_without_replaying_or_killing() {
    let (_home, mut store, run) = running(PingPolicy::Recover);
    store.health_tick(&run, 160.0).unwrap();
    store.health_tick(&run, 460.0).unwrap();
    assert_eq!(models(&store, &run), 0);
    assert_eq!(
        store.health_view(&run).unwrap().unwrap()["reason"],
        "control_unresponsive"
    );
    assert_eq!(
        store
            .conn
            .query_row("SELECT COUNT(*) FROM commands WHERE kind='ping'", [], |r| r
                .get::<_, i64>(0))
            .unwrap(),
        1
    );
    assert_eq!(store.get(&run).unwrap().status.as_str(), "running");
}

/// A queued operator instruction invalidates a health wake before any bytes,
/// while its own service ping remains legal and responsive.
#[test]
fn operator_steering_has_priority_over_probe() {
    let (_home, mut store, run) = running(PingPolicy::Recover);
    stalled(&mut store, &run);
    let (_, payload, _) = model(&mut store, &run);
    store
        .enqueue(&run, "steer", &json!({"text":"new owner instruction"}))
        .unwrap();
    assert!(
        !store
            .health_command_current(&run, &payload, true, 461.0)
            .unwrap()
    );
}

/// Clock anomalies cannot certify native readiness or authorize a model turn.
#[test]
fn future_control_receipt_is_not_health_proof() {
    let (_home, mut store, run) = running(PingPolicy::Recover);
    stalled(&mut store, &run);
    assert_eq!(models(&store, &run), 1);
    // Reset fixture to the point before its first permitted model probe.
    store
        .conn
        .execute("DELETE FROM commands WHERE kind='steer'", [])
        .unwrap();
    store
        .conn
        .execute(
            "DELETE FROM events WHERE kind='worker_ping_state_v1' AND at>=460",
            [],
        )
        .unwrap();
    store
        .conn
        .execute(
            "UPDATE commands SET completed_at=1e12 WHERE kind='ping'",
            [],
        )
        .unwrap();
    store.health_tick(&run, 460.0).unwrap();
    assert_eq!(models(&store, &run), 0);
    assert_eq!(
        store.health_view(&run).unwrap().unwrap()["reason"],
        "native_control_unavailable"
    );
}

/// A synchronously returned application-level pending handle is not a native
/// producer. The monitor catches precisely the IDE pending/idle incident.
#[test]
fn completed_mcp_pending_response_still_requires_recovery() {
    let (_home, mut store, run) = running(PingPolicy::Recover);
    for role in ["tool_call", "tool_result"] {
        store.conn.execute("INSERT INTO messages(agent_id,attempt_id,at,role,name,content,raw_ref) VALUES(?,'attempt',102,?,'agent_ide/ide.inspect','pending','mcp-call')",rusqlite::params![run.as_str(),role]).unwrap();
    }
    stalled(&mut store, &run);
    assert_eq!(models(&store, &run), 1);
    assert_eq!(
        store.health_view(&run).unwrap().unwrap()["pending_native_tools"],
        0
    );
}

/// Only genuine task activity starts another episode; pong, service receipts
/// and repeated waiting claims cannot create a model-ping loop.
#[test]
fn real_progress_rearms_recovery_but_waiting_pong_does_not() {
    let (_home, mut store, run) = running(PingPolicy::Recover);
    stalled(&mut store, &run);
    let (id, _, mut reply) = model(&mut store, &run);
    reply.state = ReplyState::Waiting;
    store
        .accept_health_reply(&run, "attempt", TOKEN, &reply, 461.0)
        .unwrap();
    store
        .complete_command(&run, id, &json!({"accepted":true}))
        .unwrap();
    store.worker_turn(&run, false, 461.0).unwrap();
    store.worker_turn(&run, true, 463.0).unwrap();
    control(&mut store, &run, 462.0);
    store.health_tick(&run, 580.0).unwrap();
    assert_eq!(models(&store, &run), 1);
    assert_eq!(
        store.health_view(&run).unwrap().unwrap()["reason"],
        "idle_without_wait_source_after_ping"
    );
    store.worker_turn(&run, false, 581.0).unwrap();
    for role in ["tool_call", "tool_result"] {
        store.conn.execute("INSERT INTO messages(agent_id,attempt_id,at,role,name,content,raw_ref) VALUES(?,'attempt',582,?,'command','useful result','actual-work')",rusqlite::params![run.as_str(),role]).unwrap();
    }
    store.worker_turn(&run, true, 583.0).unwrap();
    for at in [640.0, 700.0, 760.0, 820.0, 880.0, 940.0] {
        control(&mut store, &run, at - 1.0);
        store.health_tick(&run, at).unwrap();
    }
    assert_eq!(models(&store, &run), 2);
}

/// Unobserved/partial native ownership and missing callback catalogs cannot
/// justify a paid turn even if the native process/control channel is alive.
#[test]
fn unknown_observation_does_not_authorize_inference() {
    for missing_catalog in [false, true] {
        let (_home, mut store, run) = running(PingPolicy::Recover);
        if missing_catalog {
            store.conn.execute("UPDATE worker_capabilities SET pool_catalog_version=NULL,pool_catalog_digest=NULL WHERE attempt_id='attempt'",[]).unwrap();
        } else {
            store.conn.execute("INSERT INTO events(agent_id,attempt_id,at,kind,data_json) VALUES(?,'attempt',102,'native_tool_coverage_gap_v1','{}')",[run.as_str()]).unwrap();
        }
        stalled(&mut store, &run);
        assert_eq!(models(&store, &run), 0);
        assert_eq!(
            store.health_view(&run).unwrap().unwrap()["reason"],
            "native_observation_unavailable"
        );
    }
}

/// State diagnostics cannot upgrade a frozen admission's paid-inference policy.
#[test]
fn frozen_policy_is_authoritative_over_diagnostic_state() {
    let (_home, mut store, run) = running(PingPolicy::Observe);
    store.conn.execute("UPDATE events SET data_json=json_set(data_json,'$.policy','recover') WHERE kind='worker_ping_state_v1'",[]).unwrap();
    assert!(matches!(
        store.health_tick(&run, 460.0),
        Err(Error::Integrity(_))
    ));
    assert_eq!(models(&store, &run), 0);
}

/// A queued challenge expires as a control request; it cannot later spend
/// inference or accept a first pong after its original observation allowance.
#[test]
fn expired_queued_probe_cannot_start_a_late_model_turn() {
    let (_home, mut store, run) = running(PingPolicy::Recover);
    stalled(&mut store, &run);
    let (_, payload, reply) = model(&mut store, &run);
    assert!(
        !store
            .health_command_current(&run, &payload, true, 580.0)
            .unwrap()
    );
    assert!(
        store
            .accept_health_reply(&run, "attempt", TOKEN, &reply, 580.0)
            .is_err()
    );
    assert_eq!(models(&store, &run), 1);
    assert_eq!(store.get(&run).unwrap().status.as_str(), "running");
}

/// Clock rollback cannot reopen a closed challenge or suppress all future
/// service checks behind a previously observed future timestamp.
#[test]
fn clock_rollback_does_not_reopen_expired_challenge() {
    let (_home, mut store, run) = running(PingPolicy::Recover);
    stalled(&mut store, &run);
    let (_, payload, reply) = model(&mut store, &run);
    control(&mut store, &run, 461.0);
    store.health_tick(&run, 580.0).unwrap();
    assert!(
        !store
            .health_command_current(&run, &payload, true, 470.0)
            .unwrap()
    );
    assert!(
        store
            .accept_health_reply(&run, "attempt", TOKEN, &reply, 470.0)
            .is_err()
    );
    assert!(store.health_due(470.0, 100).unwrap().contains(&run));
    store.health_tick(&run, 470.0).unwrap();
    assert_eq!(
        store.health_view(&run).unwrap().unwrap()["reason"],
        "clock_regression"
    );
    assert_eq!(models(&store, &run), 1);
}

/// An opaque MCP inspect that still says pending inside the recovery turn is
/// not a fresh progress episode. It cannot buy another paid polling turn.
#[test]
fn recovery_turn_inspections_cannot_rearm_paid_polling() {
    let (_home, mut store, run) = running(PingPolicy::Recover);
    stalled(&mut store, &run);
    let (id, _, mut reply) = model(&mut store, &run);
    store.worker_turn(&run, false, 461.0).unwrap();
    reply.state = ReplyState::Waiting;
    store
        .accept_health_reply(&run, "attempt", TOKEN, &reply, 461.0)
        .unwrap();
    store
        .complete_command(&run, id, &json!({"accepted":true}))
        .unwrap();
    for role in ["tool_call", "tool_result"] {
        store.conn.execute("INSERT INTO messages(agent_id,attempt_id,at,role,name,content,raw_ref) VALUES(?,'attempt',462,?,'agent_ide/ide.inspect','pending','recovery-inspect')",rusqlite::params![run.as_str(),role]).unwrap();
    }
    store.worker_turn(&run, true, 463.0).unwrap();
    control(&mut store, &run, 464.0);
    for at in [580.0, 640.0, 700.0, 760.0, 820.0, 880.0] {
        if at > 580.0 {
            control(&mut store, &run, at - 1.0);
        }
        store.health_tick(&run, at).unwrap();
    }
    // A later empty external wake must not inherit inspection activity from
    // the recovery turn and manufacture a fresh paid episode.
    store.worker_turn(&run, false, 881.0).unwrap();
    store.worker_turn(&run, true, 882.0).unwrap();
    for at in [940.0, 1000.0, 1060.0, 1120.0, 1180.0, 1240.0] {
        control(&mut store, &run, at - 1.0);
        store.health_tick(&run, at).unwrap();
    }
    assert_eq!(models(&store, &run), 1);
    assert_eq!(
        store.health_view(&run).unwrap().unwrap()["reason"],
        "idle_without_wait_source_after_ping"
    );
}

/// The compatible free default cannot retroactively initialize a lifecycle
/// that already existed before this implementation was installed.
#[test]
fn existing_unmonitored_lifecycle_is_not_reinitialized() {
    let (_home, store, run) = running(PingPolicy::Observe);
    store
        .conn
        .execute("DELETE FROM events WHERE kind='worker_ping_state_v1'", [])
        .unwrap();
    store.begin_worker_lifecycle(&run, "attempt", 1e9).unwrap();
    assert!(store.health_view(&run).unwrap().is_none());
    assert!(store.health_due(1e9, 100).unwrap().is_empty());
}

/// A journal/turn change after the reader's snapshot permits only a free control
/// probe; stale empty-work evidence cannot buy model input in the writer.
#[test]
fn stale_prepared_activity_cannot_authorize_recovery() {
    for new_turn in [false, true] {
        let (_home, mut store, run) = running(PingPolicy::Recover);
        store.health_tick(&run, 400.0).unwrap();
        control(&mut store, &run, 401.0);
        let facts = store.prepare_health(&run).unwrap().unwrap();
        if new_turn {
            store.worker_turn(&run, false, 402.0).unwrap();
        } else {
            store.conn.execute("INSERT INTO messages(agent_id,attempt_id,at,role,name,content,raw_ref) VALUES(?,'attempt',402,'tool_call','command','','new-job')",[run.as_str()]).unwrap();
        }
        assert!(store.health_tick_prepared(&run, 460.0, &facts).unwrap());
        assert_eq!(models(&store, &run), 0);
        assert_eq!(store.get(&run).unwrap().status.as_str(), "running");
    }
}

/// Missing activity observations remain explicit and never authorize inference,
/// while the cheap supervisor control channel remains available.
#[test]
fn bounded_read_fallback_preserves_control_without_inference() {
    let (_home, mut store, run) = running(PingPolicy::Recover);
    store.health_tick(&run, 400.0).unwrap();
    control(&mut store, &run, 401.0);
    let facts = store.prepare_health_control(&run).unwrap().unwrap();
    assert!(store.health_tick_prepared(&run, 460.0, &facts).unwrap());
    assert_eq!(models(&store, &run), 0);
    assert_eq!(
        store.health_view(&run).unwrap().unwrap()["reason"],
        "activity_observation_unavailable"
    );
    control(&mut store, &run, 461.0);
    store.health_tick(&run, 520.0).unwrap();
    assert_eq!(models(&store, &run), 1);
}

/// The sole writer's work is independent of transcript length: preparation owns
/// the aggregate scan and public diagnostics use only its bounded cached facts.
/// SQLite VM instructions, rather than machine timing, guard the write budget.
#[test]
fn prepared_tick_and_public_view_do_not_scan_transcript_history() {
    let (_home, mut store, run) = running(PingPolicy::Observe);
    let tx = store.conn.unchecked_transaction().unwrap();
    for index in 0..5000 {
        for role in ["tool_call", "tool_result"] {
            tx.execute("INSERT INTO messages(agent_id,attempt_id,at,role,name,content,raw_ref) VALUES(?,'attempt',102,?,'command','',?)",rusqlite::params![run.as_str(),role,format!("job-{index}")]).unwrap();
        }
    }
    // Native results can omit a display name; ownership still completes the call.
    tx.execute("INSERT INTO messages(agent_id,attempt_id,at,role,name,content,raw_ref) VALUES(?,'attempt',103,'tool_call','command','','unnamed-result')",[run.as_str()]).unwrap();
    tx.execute("INSERT INTO messages(agent_id,attempt_id,at,role,name,content,raw_ref) VALUES(?,'attempt',104,'tool_result',NULL,'','unnamed-result')",[run.as_str()]).unwrap();
    tx.commit().unwrap();
    let facts = store.prepare_health(&run).unwrap().unwrap();
    let steps = std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0));
    let counter = std::sync::Arc::clone(&steps);
    store
        .conn
        .progress_handler(
            1,
            Some(move || {
                counter.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                false
            }),
        )
        .unwrap();
    let tick = store.health_tick_prepared(&run, 460.0, &facts);
    let view = store.health_view(&run);
    store
        .conn
        .progress_handler(0, None::<fn() -> bool>)
        .unwrap();
    assert!(tick.unwrap());
    let view = view.unwrap().unwrap();
    assert_eq!(view["pending_native_tools"], 0);
    assert_eq!(view["reason"], "idle_without_wait_source");
    let steps = steps.load(std::sync::atomic::Ordering::Relaxed);
    assert!(steps < 5000, "writer/view used {steps} SQLite VM steps");
}

/// A delayed reader from a retired attempt cannot close probes or record a
/// false clock regression on its replacement. Fresh checks use the new identity.
#[test]
fn retired_attempt_facts_cannot_mutate_replacement_health() {
    let (_home, mut store, run) = running(PingPolicy::Recover);
    let old = store.prepare_health(&run).unwrap().unwrap();
    store
        .conn
        .execute(
            "UPDATE attempts SET ownership_active=0,finished_at=500 WHERE id='attempt'",
            [],
        )
        .unwrap();
    store.conn.execute("INSERT INTO attempts(id,agent_id,number,state,adapter_state_json,created_at,ownership_active) VALUES('replacement',?,2,'running','{}',500,1)",[run.as_str()]).unwrap();
    store
        .issue_worker_capability(&run, "replacement", &"b".repeat(64), 500.0)
        .unwrap();
    store
        .begin_worker_lifecycle(&run, "replacement", 500.0)
        .unwrap();
    assert!(!store.health_tick_prepared(&run, 460.0, &old).unwrap());
    assert!(store.health_view(&run).unwrap().unwrap()["reason"].is_null());
    assert_eq!(
        store
            .conn
            .query_row("SELECT COUNT(*) FROM commands", [], |r| r.get::<_, i64>(0))
            .unwrap(),
        0
    );
    assert!(store.health_tick(&run, 560.0).unwrap());
    let attempt: String = store
        .conn
        .query_row(
            "SELECT json_extract(payload_json,'$.attempt_id') FROM commands WHERE kind='ping'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(attempt, "replacement");
    assert_eq!(models(&store, &run), 0);
}

/// The underscore in the private health_reply name is literal. Similar ordinary
/// MCP names still represent owned work and must never trigger a recovery turn.
#[test]
fn similarly_named_task_tools_remain_real_wait_sources() {
    for name in ["external/healthXreply", "external/health_REPLY"] {
        let (_home, mut store, run) = running(PingPolicy::Recover);
        store.conn.execute("INSERT INTO messages(agent_id,attempt_id,at,role,name,content,raw_ref) VALUES(?,'attempt',102,'tool_call',?,'','owned-job')",rusqlite::params![run.as_str(),name]).unwrap();
        stalled(&mut store, &run);
        assert_eq!(models(&store, &run), 0, "{name}");
        assert_eq!(
            store.health_view(&run).unwrap().unwrap()["pending_native_tools"],
            1,
            "{name}"
        );
    }
}
