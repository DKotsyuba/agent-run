//! Staged checkpoints never complete work or authorize process cleanup.
mod common;

use agent_run_domain::{
    Error,
    domain::AgentId,
    worker::{
        FinishGuard, FinishRequest, FinishStatus, MaintenancePauseRequest, WorkerCatalogProof,
        maintenance_pause_catalog_digest,
    },
};
use agent_run_store::{Store, writer::StoreReader};
use serde_json::json;

const TOKEN: &str = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
const ATTEMPT: &str = "checkpoint-attempt";
const CHALLENGE: &str = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";

/// An isolated live explicit worker with a fixture-registered v5 catalog.
struct Fixture {
    home: common::Home,
    store: Store<'static>,
    run: AgentId,
    epoch: i64,
}

impl Fixture {
    /// Seed frozen role authority and a real capability without starting a model.
    fn new() -> Self {
        Self::with_orchestrator(false)
    }

    /// Optionally bind an orchestrator before the maintenance cohort is captured.
    fn with_orchestrator(bound: bool) -> Self {
        let home = common::Home::new();
        let mut store = home.store();
        let mut role = json!({
            "role_name":"review", "role_revision":"fixture", "prompt":"Review the task.",
            "grants":{"write":false,"network":false,"allow_external_read_roots":false,"read_roots":[]},
            "skills":[],"mcp":[],"required_constraints":[],"auth":{"mode":"global","reference":null},"worker_mcp":true,
        });
        role["config_revision"] = json!(agent_run_domain::canonical::sha256_hex(&role, true));
        agent_run_config::role_plan::ResolvedRolePlan::from_payload(&role).unwrap();
        let mut request = home.request();
        if bound {
            request.orchestrator = Some(agent_run_domain::domain::OrchestratorRef {
                transport: "codex_queue".into(),
                external_session_id: "checkpoint-orchestrator".into(),
                external_turn_id: None,
            });
        }
        request.explicit_finish = true;
        request.output_schema = Some(
            json!({"type":"object","required":["answer"]})
                .as_object()
                .unwrap()
                .clone(),
        );
        let (run, _) = store
            .admit(
                &request,
                &home.config,
                &json!({"authority":{"role_payload":role}}),
                None,
            )
            .unwrap();
        store
            .conn
            .execute(
                "UPDATE agents SET status='running',runtime_session_id='fixture-native-session' WHERE id=?",
                [run.as_str()],
            )
            .unwrap();
        store.conn.execute("INSERT INTO attempts(id,agent_id,number,state,adapter_state_json,created_at,ownership_active) VALUES(?,?,1,'running','{}',1,1)",rusqlite::params![ATTEMPT,run.as_str()]).unwrap();
        store
            .issue_worker_capability(&run, ATTEMPT, TOKEN, 1.0)
            .unwrap();
        store.begin_worker_lifecycle(&run, ATTEMPT, 1.0).unwrap();
        store
            .register_worker_catalog(
                &WorkerCatalogProof {
                    run_id: run.clone(),
                    attempt_id: ATTEMPT.into(),
                    token: TOKEN.into(),
                    version: 5,
                    digest: maintenance_pause_catalog_digest(false),
                },
                1.0,
            )
            .unwrap();
        let epoch = store
            .begin_maintenance_fence("checkpoint-maintenance", 2.0)
            .unwrap()
            .epoch;
        Self {
            home,
            store,
            run,
            epoch,
        }
    }

    /// Record the exact attempt's challenge, without enqueuing steering.
    fn request(&mut self) {
        self.store
            .request_maintenance_checkpoint(self.epoch, &self.run, ATTEMPT, CHALLENGE, 3.0)
            .unwrap();
    }

    /// A non-JSON checkpoint proves task output_schema does not apply here.
    fn input() -> MaintenancePauseRequest {
        MaintenancePauseRequest {
            challenge_id: CHALLENGE.into(),
            checkpoint: "Completed the read; next inspect the unchanged patch. No tool is pending."
                .into(),
        }
    }

    /// Enqueue an actual cancellation command using the retained execution id.
    fn cancel(&self) {
        self.store.conn.execute("INSERT INTO commands(agent_id,kind,payload_json,state,created_at) VALUES(?,'cancel','{}','pending',4)",[self.run.as_str()]).unwrap();
    }
}

/// The challenge and its instruction commit together and identical retries
/// never enqueue a second native input.
#[test]
fn maintenance_steer_is_atomic_and_idempotent() {
    let mut f = Fixture::new();
    let first = f
        .store
        .request_maintenance_steer(f.epoch, &f.run, ATTEMPT, CHALLENGE, 3.0)
        .unwrap();
    assert!(!first.duplicate);
    let repeated = f
        .store
        .request_maintenance_steer(f.epoch, &f.run, ATTEMPT, CHALLENGE, 4.0)
        .unwrap();
    assert!(repeated.duplicate);
    assert_eq!(first.command_id, repeated.command_id);
    let (state, payload): (String, String) = f
        .store
        .conn
        .query_row(
            "SELECT state,payload_json FROM commands WHERE id=?",
            [first.command_id],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )
        .unwrap();
    let payload: serde_json::Value = serde_json::from_str(&payload).unwrap();
    assert_eq!(state, "pending");
    assert_eq!(payload["maintenance_attempt"], ATTEMPT);
    assert_eq!(payload["maintenance_challenge"], CHALLENGE);
    assert!(
        payload["text"]
            .as_str()
            .unwrap()
            .contains("call maintenance_pause")
    );
    assert!(matches!(
        f.store
            .request_maintenance_steer(f.epoch, &f.run, ATTEMPT, &"c".repeat(64), 5.0),
        Err(Error::Conflict)
    ));
    assert_eq!(
        f.store
            .conn
            .query_row("SELECT COUNT(*) FROM commands", [], |r| r.get::<_, i64>(0))
            .unwrap(),
        1
    );
    let (claimed, _, _) = f.store.claim_command(&f.run).unwrap().unwrap();
    assert_eq!(claimed, first.command_id);
    let uncertain_retry = f
        .store
        .request_maintenance_steer(f.epoch, &f.run, ATTEMPT, CHALLENGE, 5.5)
        .unwrap();
    assert!(uncertain_retry.duplicate);
    assert_eq!(uncertain_retry.command_id, claimed);
    assert!(f.store.claim_command(&f.run).unwrap().is_none());
    f.cancel();
    assert!(
        f.store
            .request_maintenance_steer(f.epoch, &f.run, ATTEMPT, CHALLENGE, 6.0)
            .is_err()
    );
}

/// Deferral prevents a never-claimed instruction from reaching the native
/// session and resolves it as unsent when normal admission is restored.
#[test]
fn maintenance_deferral_retires_unsent_pause() {
    let mut f = Fixture::new();
    let command = f
        .store
        .request_maintenance_steer(f.epoch, &f.run, ATTEMPT, CHALLENGE, 3.0)
        .unwrap()
        .command_id;
    f.store.defer_maintenance_fence(f.epoch).unwrap();
    assert!(
        f.store
            .claim_command_for_turn(&f.run, false)
            .unwrap()
            .is_none()
    );
    assert!(
        f.store
            .request_maintenance_steer(f.epoch, &f.run, ATTEMPT, CHALLENGE, 4.0)
            .is_err()
    );
    f.store.end_maintenance_fence(f.epoch, 5.0).unwrap();
    let (state, result): (String, String) = f
        .store
        .conn
        .query_row(
            "SELECT state,result_json FROM commands WHERE id=?",
            [command],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .unwrap();
    assert_eq!(state, "completed");
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(&result).unwrap(),
        json!({"accepted":false,"reason":"maintenance_deferred"})
    );
}

/// A pause already claimed before deferral may have been sent. Its callback
/// remains accepted, while admission stays fenced until that handoff settles.
#[test]
fn maintenance_deferral_retains_claimed_pause() {
    let mut f = Fixture::new();
    let command = f
        .store
        .request_maintenance_steer(f.epoch, &f.run, ATTEMPT, CHALLENGE, 3.0)
        .unwrap()
        .command_id;
    assert_eq!(
        f.store
            .claim_command_for_turn(&f.run, false)
            .unwrap()
            .unwrap()
            .0,
        command
    );
    f.store.defer_maintenance_fence(f.epoch).unwrap();
    f.store
        .complete_command(
            &f.run,
            command,
            &json!({"accepted":true,"observation":"native_accepted"}),
        )
        .unwrap();
    f.store
        .accept_maintenance_checkpoint(
            f.epoch,
            &f.run,
            ATTEMPT,
            TOKEN,
            &Fixture::input(),
            4.0,
            FinishGuard::Clean,
        )
        .unwrap();
    assert!(f.store.end_maintenance_fence(f.epoch, 5.0).is_err());
}

/// A bound orchestrator's pre-notice must reach Bridge custody before the
/// pause instruction is allowed; unknown custody is not acceptance.
#[test]
fn maintenance_steer_waits_for_bound_recipient_custody() {
    let mut f = Fixture::with_orchestrator(true);
    let before: String = f
        .store
        .conn
        .query_row(
            "SELECT before_delivery_id FROM maintenance_recipients WHERE epoch=?",
            [f.epoch],
            |r| r.get(0),
        )
        .unwrap();
    assert!(
        f.store
            .request_maintenance_steer(f.epoch, &f.run, ATTEMPT, CHALLENGE, 3.0)
            .is_err()
    );
    f.store.conn.execute("INSERT INTO bridge_delivery_intents(delivery_id,intent_json,custody_state) VALUES(?,'{}','outcome_unknown')", [&before]).unwrap();
    assert!(
        f.store
            .request_maintenance_steer(f.epoch, &f.run, ATTEMPT, CHALLENGE, 3.0)
            .is_err()
    );
    assert_eq!(
        f.store
            .conn
            .query_row("SELECT COUNT(*) FROM maintenance_checkpoints", [], |r| r
                .get::<_, i64>(0))
            .unwrap(),
        0
    );
    assert_eq!(
        f.store
            .conn
            .query_row("SELECT COUNT(*) FROM commands", [], |r| r.get::<_, i64>(0))
            .unwrap(),
        0
    );
    f.store.conn.execute("UPDATE bridge_delivery_intents SET custody_state='custody_accepted' WHERE delivery_id=?", [&before]).unwrap();
    assert!(
        !f.store
            .request_maintenance_steer(f.epoch, &f.run, ATTEMPT, CHALLENGE, 4.0)
            .unwrap()
            .duplicate
    );
}

/// A failed queue insertion rolls back the associated challenge, and an
/// already accepted checkpoint cannot acquire a late new instruction.
#[test]
fn maintenance_steer_rolls_back_on_queue_failure_and_refuses_late_send() {
    let mut f = Fixture::new();
    f.store
        .conn
        .execute(
            "UPDATE agents SET runtime_session_id=NULL WHERE id=?",
            [f.run.as_str()],
        )
        .unwrap();
    assert!(
        f.store
            .request_maintenance_steer(f.epoch, &f.run, ATTEMPT, CHALLENGE, 2.5)
            .is_err()
    );
    assert_eq!(
        f.store
            .conn
            .query_row("SELECT COUNT(*) FROM maintenance_checkpoints", [], |r| r
                .get::<_, i64>(0))
            .unwrap(),
        0
    );
    f.store
        .conn
        .execute(
            "UPDATE agents SET runtime_session_id='fixture-native-session' WHERE id=?",
            [f.run.as_str()],
        )
        .unwrap();
    f.store.conn.execute_batch("CREATE TRIGGER fixture_refuse_steer BEFORE INSERT ON commands BEGIN SELECT RAISE(ABORT,'fixture'); END;").unwrap();
    assert!(
        f.store
            .request_maintenance_steer(f.epoch, &f.run, ATTEMPT, CHALLENGE, 3.0)
            .is_err()
    );
    assert_eq!(
        f.store
            .conn
            .query_row("SELECT COUNT(*) FROM maintenance_checkpoints", [], |r| r
                .get::<_, i64>(0))
            .unwrap(),
        0
    );
    f.store
        .conn
        .execute_batch("DROP TRIGGER fixture_refuse_steer")
        .unwrap();
    f.request();
    f.store
        .accept_maintenance_checkpoint(
            f.epoch,
            &f.run,
            ATTEMPT,
            TOKEN,
            &Fixture::input(),
            4.0,
            FinishGuard::Clean,
        )
        .unwrap();
    assert!(
        f.store
            .request_maintenance_steer(f.epoch, &f.run, ATTEMPT, CHALLENGE, 5.0)
            .is_err()
    );
    assert_eq!(
        f.store
            .conn
            .query_row("SELECT COUNT(*) FROM commands", [], |r| r.get::<_, i64>(0))
            .unwrap(),
        0
    );
}

/// A queued request for a previous attempt cannot be delivered after account
/// failover; it also cannot starve a later ordinary instruction.
#[test]
fn maintenance_steer_never_crosses_attempts_or_blocks_other_commands() {
    let mut f = Fixture::new();
    let queued = f
        .store
        .request_maintenance_steer(f.epoch, &f.run, ATTEMPT, CHALLENGE, 3.0)
        .unwrap();
    let reader = StoreReader::open(&f.home.path).unwrap();
    assert!(reader.command_pending(&f.run, true).unwrap());
    f.store
        .conn
        .execute(
            "UPDATE attempts SET ownership_active=0,state='failed' WHERE id=?",
            [ATTEMPT],
        )
        .unwrap();
    f.store.conn.execute("INSERT INTO attempts(id,agent_id,number,state,adapter_state_json,created_at,ownership_active) VALUES('successor',?,2,'running','{}',4,1)", [f.run.as_str()]).unwrap();
    assert!(!reader.command_pending(&f.run, true).unwrap());
    assert!(f.store.claim_command(&f.run).unwrap().is_none());
    let ordinary = f
        .store
        .enqueue(&f.run, "steer", &json!({"text":"ordinary control"}))
        .unwrap();
    assert!(reader.command_pending(&f.run, true).unwrap());
    let (id, kind, payload) = f.store.claim_command(&f.run).unwrap().unwrap();
    assert_eq!(id, ordinary["command_id"].as_i64().unwrap());
    assert_eq!(kind, "steer");
    assert_eq!(payload["text"], "ordinary control");
    assert_eq!(
        f.store
            .conn
            .query_row(
                "SELECT state FROM commands WHERE id=?",
                [queued.command_id],
                |r| r.get::<_, String>(0)
            )
            .unwrap(),
        "pending"
    );
}

/// An accepted checkpoint or released fence invalidates an unsent pause
/// instruction; cancellation remains claimable before any maintenance work.
#[test]
fn maintenance_steer_stops_being_claimable_after_receipt_or_cancel() {
    let mut f = Fixture::new();
    f.store
        .request_maintenance_steer(f.epoch, &f.run, ATTEMPT, CHALLENGE, 3.0)
        .unwrap();
    f.store
        .accept_maintenance_checkpoint(
            f.epoch,
            &f.run,
            ATTEMPT,
            TOKEN,
            &Fixture::input(),
            4.0,
            FinishGuard::Clean,
        )
        .unwrap();
    assert!(f.store.claim_command(&f.run).unwrap().is_none());
    f.cancel();
    let (_, kind, _) = f.store.claim_command(&f.run).unwrap().unwrap();
    assert_eq!(kind, "cancel");

    let mut released = Fixture::new();
    released
        .store
        .request_maintenance_steer(released.epoch, &released.run, ATTEMPT, CHALLENGE, 3.0)
        .unwrap();
    released
        .store
        .end_maintenance_fence(released.epoch, 4.0)
        .unwrap();
    assert!(
        released
            .store
            .claim_command(&released.run)
            .unwrap()
            .is_none()
    );
}

/// Once a maintenance control is claimed, its native effect may be unknown.
/// A dead supervisor must not turn that uncertainty into a lost task.
#[test]
fn maintenance_claimed_control_is_held_during_reconciliation() {
    use agent_run_store::ops::{ExpectedOwnership, MarkLost};
    let mut f = Fixture::new();
    f.store.conn.execute("UPDATE agents SET supervisor_pid=12345,supervisor_identity='fixture-owner',supervisor_birth_time=1,process_group_id=12345,heartbeat_at=1 WHERE id=?", [f.run.as_str()]).unwrap();
    let queued = f
        .store
        .request_maintenance_steer(f.epoch, &f.run, ATTEMPT, CHALLENGE, 3.0)
        .unwrap();
    let (claimed, _, _) = f.store.claim_command(&f.run).unwrap().unwrap();
    assert_eq!(claimed, queued.command_id);
    assert!(f.store.end_maintenance_fence(f.epoch, 4.0).is_err());
    assert!(f.store.active_maintenance_fence().unwrap().is_some());
    let lost = MarkLost {
        expected: ExpectedOwnership {
            execution: f.run.clone(),
            supervisor_pid: Some(12345),
            supervisor_identity: Some("fixture-owner".into()),
            supervisor_birth_time: Some(1.0),
            process_group_id: Some(12345),
            startup_owner: None,
            startup_birth_time: None,
        },
        failure_kind: "supervisor_dead".into(),
        failure_text: "fixture".into(),
        evidence: json!({"verdict":"dead"}),
        checked_at: 5.0,
    };
    assert!(!f.store.mark_lost(&lost).unwrap());
    assert_eq!(f.store.get(&f.run).unwrap().status.as_str(), "running");
    f.store
        .conn
        .execute(
            "UPDATE commands SET state='pending',claimed_at=NULL WHERE id=?",
            [claimed],
        )
        .unwrap();
    assert!(f.store.mark_lost(&lost).unwrap());
    assert_eq!(f.store.get(&f.run).unwrap().status.as_str(), "lost");
    assert!(f.store.end_maintenance_fence(f.epoch, 6.0).is_ok());
}

/// A definite pre-send refusal lets an aborted restart reopen admissions;
/// a possibly sent control remains fenced for reconciliation.
#[test]
fn maintenance_fence_releases_only_for_definitely_unsent_control() {
    for (accepted, reason, may_release) in [
        (json!(false), "maintenance_context_unavailable", true),
        (json!(false), "native_rejected", true),
        (serde_json::Value::Null, "native_delivery_uncertain", false),
        (json!(true), "", false),
    ] {
        let mut f = Fixture::new();
        let queued = f
            .store
            .request_maintenance_steer(f.epoch, &f.run, ATTEMPT, CHALLENGE, 3.0)
            .unwrap();
        let (id, _, _) = f.store.claim_command(&f.run).unwrap().unwrap();
        assert_eq!(id, queued.command_id);
        f.store
            .complete_command(&f.run, id, &json!({"accepted":accepted,"reason":reason}))
            .unwrap();
        assert_eq!(
            f.store.end_maintenance_fence(f.epoch, 5.0).is_ok(),
            may_release
        );
        assert_eq!(
            f.store.active_maintenance_fence().unwrap().is_none(),
            may_release
        );
    }
}

/// Worker callbacks resolve the epoch from the exact authenticated challenge;
/// the task output schema never applies to the checkpoint.
#[test]
fn maintenance_callback_derives_epoch_and_preserves_exact_retry() {
    let mut f = Fixture::new();
    f.request();
    let input = Fixture::input();
    let other: AgentId = "ag-20261010-000000-0000000009".parse().unwrap();
    for (run, attempt, token, request) in [
        (&f.run, ATTEMPT, "wrong-token", input.clone()),
        (&f.run, "foreign-attempt", TOKEN, input.clone()),
        (&other, ATTEMPT, TOKEN, input.clone()),
        (
            &f.run,
            ATTEMPT,
            TOKEN,
            MaintenancePauseRequest {
                challenge_id: "c".repeat(64),
                ..input.clone()
            },
        ),
    ] {
        assert!(
            f.store
                .accept_maintenance_checkpoint_for_challenge(
                    run,
                    attempt,
                    token,
                    &request,
                    4.0,
                    FinishGuard::Clean
                )
                .is_err()
        );
    }
    assert!(
        f.store
            .accept_maintenance_checkpoint_for_challenge(
                &f.run,
                ATTEMPT,
                TOKEN,
                &input,
                4.0,
                FinishGuard::Unavailable
            )
            .is_err()
    );
    assert!(
        f.store
            .accept_maintenance_checkpoint_for_challenge(
                &f.run,
                ATTEMPT,
                TOKEN,
                &input,
                4.0,
                FinishGuard::Secret
            )
            .is_err()
    );
    let first = f
        .store
        .accept_maintenance_checkpoint_for_challenge(
            &f.run,
            ATTEMPT,
            TOKEN,
            &input,
            4.0,
            FinishGuard::Clean,
        )
        .unwrap();
    let replay = f
        .store
        .accept_maintenance_checkpoint_for_challenge(
            &f.run,
            ATTEMPT,
            TOKEN,
            &input,
            5.0,
            FinishGuard::Unavailable,
        )
        .unwrap();
    assert!(!first.duplicate && replay.duplicate);
    assert_eq!(first.sha256, replay.sha256);
    let mut changed = input;
    changed.checkpoint.push('!');
    assert!(matches!(
        f.store.accept_maintenance_checkpoint_for_challenge(
            &f.run,
            ATTEMPT,
            TOKEN,
            &changed,
            6.0,
            FinishGuard::Unavailable
        ),
        Err(Error::Conflict)
    ));
    f.cancel();
    assert!(
        f.store
            .accept_maintenance_checkpoint_for_challenge(
                &f.run,
                ATTEMPT,
                TOKEN,
                &changed,
                7.0,
                FinishGuard::Clean
            )
            .is_err()
    );
}

/// The callback's guard is not part of its serialized operation identity and
/// no model-supplied epoch or private text can leak through Debug.
#[test]
fn maintenance_callback_guard_preserves_operation_identity() {
    use agent_run_domain::storage::{MonoDeadline, OperationId, ProducerId, ProducerKind};
    use agent_run_store::ops::{AcceptMaintenanceCheckpoint, LocalEnvelope, LocalOp};
    let input = Fixture::input();
    let op = AcceptMaintenanceCheckpoint {
        execution: "ag-20261010-000000-0123456789".parse().unwrap(),
        attempt: ATTEMPT.into(),
        token: TOKEN.into(),
        input: input.clone(),
        at: 4.0,
        guard: FinishGuard::Unavailable,
    };
    let payload = serde_json::to_value(&op).unwrap();
    assert!(payload.get("guard").is_none() && payload.get("epoch").is_none());
    let debug = format!("{op:?}");
    assert!(
        !debug.contains(TOKEN) && !debug.contains(CHALLENGE) && !debug.contains(&input.checkpoint)
    );
    let (boot, now) = agent_run_platform::monotonic::now().unwrap();
    let mut envelope = LocalEnvelope::new(
        OperationId::new(&boot, now),
        ProducerId {
            kind: ProducerKind::Broker,
            scope: Some("workers".into()),
            instance: "instance-1".into(),
        },
        1,
        MonoDeadline {
            boot_id: boot,
            ns: now + 60_000_000_000,
        },
        LocalOp::from(op),
    )
    .unwrap();
    let digest = envelope.header().digest.clone();
    for guard in [FinishGuard::Clean, FinishGuard::Secret] {
        envelope.set_maintenance_guard(guard);
        assert_eq!(envelope.header().digest, digest);
        let LocalOp::AcceptMaintenanceCheckpoint(op) = envelope.op() else {
            panic!("wrong operation");
        };
        assert_eq!(op.guard, guard);
        assert_eq!(serde_json::to_value(op).unwrap(), payload);
    }
}

/// Checkpoints have their own immutable digest, observable receipt and bounded
/// content; neither acceptance nor observation mutates task completion state.
#[test]
fn maintenance_checkpoint_is_separate_immutable_and_read_only_observable() {
    let mut f = Fixture::new();
    let before = serde_json::to_value(f.store.get(&f.run).unwrap()).unwrap();
    f.request();
    let first = f
        .store
        .request_maintenance_checkpoint(f.epoch, &f.run, ATTEMPT, CHALLENGE, 9.0)
        .unwrap();
    assert_eq!(first.requested_at, 3.0);
    let receipt = f
        .store
        .accept_maintenance_checkpoint(
            f.epoch,
            &f.run,
            ATTEMPT,
            TOKEN,
            &Fixture::input(),
            4.0,
            FinishGuard::Clean,
        )
        .unwrap();
    assert!(!receipt.duplicate);
    let retry = f
        .store
        .accept_maintenance_checkpoint(
            f.epoch,
            &f.run,
            ATTEMPT,
            TOKEN,
            &Fixture::input(),
            5.0,
            FinishGuard::Unavailable,
        )
        .unwrap();
    assert!(retry.duplicate);
    assert_eq!(retry.sha256, receipt.sha256);
    let mut changed = Fixture::input();
    changed.checkpoint.push('!');
    assert!(matches!(
        f.store.accept_maintenance_checkpoint(
            f.epoch,
            &f.run,
            ATTEMPT,
            TOKEN,
            &changed,
            6.0,
            FinishGuard::Unavailable
        ),
        Err(Error::Conflict)
    ));
    let reader = StoreReader::open(&f.home.path).unwrap();
    assert!(
        reader
            .maintenance_checkpoint_intent(f.epoch, &f.run, ATTEMPT, true)
            .unwrap()
            .is_none()
    );
    assert_eq!(
        reader
            .maintenance_checkpoint_intent(f.epoch, &f.run, ATTEMPT, false)
            .unwrap(),
        Some(Fixture::input())
    );
    assert!(
        f.store
            .observe_maintenance_checkpoint_receipt(
                f.epoch,
                &f.run,
                ATTEMPT,
                CHALLENGE,
                &receipt.sha256
            )
            .unwrap()
    );
    assert!(
        !f.store
            .observe_maintenance_checkpoint_receipt(
                f.epoch,
                &f.run,
                ATTEMPT,
                CHALLENGE,
                &receipt.sha256
            )
            .unwrap()
    );
    assert_eq!(
        reader
            .maintenance_checkpoint_intent(f.epoch, &f.run, ATTEMPT, true)
            .unwrap(),
        Some(Fixture::input())
    );
    assert_eq!(
        serde_json::to_value(f.store.get(&f.run).unwrap()).unwrap(),
        before
    );
    assert!(
        f.store
            .worker_finish_intent(&f.run, false)
            .unwrap()
            .is_none()
    );
    assert_eq!(
        f.store.worker_lifecycle_view(&f.run).unwrap().unwrap()["phase"],
        "running"
    );
    assert!(
        f.store
            .conn
            .query_row(
                "SELECT ownership_active FROM attempts WHERE id=?",
                [ATTEMPT],
                |r| r.get::<_, bool>(0)
            )
            .unwrap()
    );
    assert_eq!(
        f.store
            .conn
            .query_row("SELECT COUNT(*) FROM commands", [], |r| r.get::<_, i64>(0))
            .unwrap(),
        0
    );
    assert_eq!(
        f.store
            .conn
            .query_row("SELECT COUNT(*) FROM deliveries", [], |r| r
                .get::<_, i64>(0))
            .unwrap(),
        0
    );
    assert!(
        f.store
            .conn
            .execute(
                "UPDATE maintenance_checkpoints SET checkpoint_json='{}'",
                []
            )
            .is_err()
    );
    assert!(
        f.store
            .conn
            .execute("UPDATE maintenance_checkpoints SET receipt_observed=0", [])
            .is_err()
    );
    assert!(
        f.store
            .conn
            .execute(
                "UPDATE maintenance_checkpoints SET challenge_id=?",
                ["c".repeat(64)]
            )
            .is_err()
    );
    drop(reader);
    drop(f.store);
    let reopened = StoreReader::open(&f.home.path).unwrap();
    assert_eq!(
        reopened
            .maintenance_checkpoint_intent(f.epoch, &f.run, ATTEMPT, true)
            .unwrap(),
        Some(Fixture::input())
    );
}

/// Auth, secret verdict and exact issued challenge are independently required;
/// rejected first callbacks leave no checkpoint to be mistaken for acceptance.
#[test]
fn maintenance_checkpoint_rejects_unissued_foreign_and_secret_inputs() {
    let mut f = Fixture::new();
    assert!(
        f.store
            .accept_maintenance_checkpoint(
                f.epoch,
                &f.run,
                ATTEMPT,
                TOKEN,
                &Fixture::input(),
                3.0,
                FinishGuard::Clean
            )
            .is_err()
    );
    f.request();
    for guard in [FinishGuard::Unavailable, FinishGuard::Secret] {
        assert!(
            f.store
                .accept_maintenance_checkpoint(
                    f.epoch,
                    &f.run,
                    ATTEMPT,
                    TOKEN,
                    &Fixture::input(),
                    4.0,
                    guard
                )
                .is_err()
        );
    }
    let mut secret = Fixture::input();
    secret.checkpoint = TOKEN.into();
    assert!(
        f.store
            .accept_maintenance_checkpoint(
                f.epoch,
                &f.run,
                ATTEMPT,
                TOKEN,
                &secret,
                4.0,
                FinishGuard::Clean
            )
            .is_err()
    );
    assert!(
        f.store
            .accept_maintenance_checkpoint(
                f.epoch,
                &f.run,
                ATTEMPT,
                &"c".repeat(64),
                &Fixture::input(),
                4.0,
                FinishGuard::Clean
            )
            .is_err()
    );
    assert!(
        f.store
            .accept_maintenance_checkpoint(
                f.epoch,
                &f.run,
                "other-attempt",
                TOKEN,
                &Fixture::input(),
                4.0,
                FinishGuard::Clean
            )
            .is_err()
    );
    let mut foreign = Fixture::input();
    foreign.challenge_id = "c".repeat(64);
    assert!(
        f.store
            .accept_maintenance_checkpoint(
                f.epoch,
                &f.run,
                ATTEMPT,
                TOKEN,
                &foreign,
                4.0,
                FinishGuard::Clean
            )
            .is_err()
    );
    assert!(
        f.store
            .accept_maintenance_checkpoint(
                f.epoch + 1,
                &f.run,
                ATTEMPT,
                TOKEN,
                &Fixture::input(),
                4.0,
                FinishGuard::Clean
            )
            .is_err()
    );
    assert_eq!(
        f.store
            .conn
            .query_row(
                "SELECT COUNT(*) FROM maintenance_checkpoints WHERE checkpoint_json IS NOT NULL",
                [],
                |r| r.get::<_, i64>(0)
            )
            .unwrap(),
        0
    );
}

/// Legacy, missing and wrong-role catalog proofs cannot issue a challenge.
/// Valid v5 registration remains idempotent for the exact frozen attempt.
#[test]
fn maintenance_checkpoint_requires_exact_staged_role_catalog() {
    for version in 1..=4 {
        let mut f = Fixture::new();
        f.store
            .conn
            .execute(
                "UPDATE worker_capabilities SET pool_catalog_version=?",
                [version],
            )
            .unwrap();
        assert!(
            f.store
                .request_maintenance_checkpoint(f.epoch, &f.run, ATTEMPT, CHALLENGE, 3.0)
                .is_err()
        );
    }
    for digest in ["0".repeat(64), maintenance_pause_catalog_digest(true)] {
        let mut f = Fixture::new();
        f.store
            .conn
            .execute(
                "UPDATE worker_capabilities SET pool_catalog_digest=?",
                [digest],
            )
            .unwrap();
        assert!(
            f.store
                .request_maintenance_checkpoint(f.epoch, &f.run, ATTEMPT, CHALLENGE, 3.0)
                .is_err()
        );
    }
    let mut f = Fixture::new();
    f.store
        .conn
        .execute(
            "UPDATE worker_capabilities SET pool_catalog_observed_at=NULL",
            [],
        )
        .unwrap();
    assert!(
        f.store
            .request_maintenance_checkpoint(f.epoch, &f.run, ATTEMPT, CHALLENGE, 3.0)
            .is_err()
    );
    let mut registered = Fixture::new();
    let proof = WorkerCatalogProof {
        run_id: registered.run.clone(),
        attempt_id: ATTEMPT.into(),
        token: TOKEN.into(),
        version: 5,
        digest: maintenance_pause_catalog_digest(false),
    };
    assert!(
        registered
            .store
            .register_worker_catalog(&proof, 3.0)
            .is_ok()
    );
    assert!(agent_run_domain::worker::known_pool_catalog(
        5,
        &proof.digest
    ));
}

/// Accepted intent cannot suppress a real finish or cancellation; either one
/// makes later pause/receipt reads ineligible without rewriting old evidence.
#[test]
fn maintenance_checkpoint_cancel_and_real_finish_win() {
    for completed in [false, true] {
        let mut f = Fixture::new();
        f.request();
        let receipt = f
            .store
            .accept_maintenance_checkpoint(
                f.epoch,
                &f.run,
                ATTEMPT,
                TOKEN,
                &Fixture::input(),
                4.0,
                FinishGuard::Clean,
            )
            .unwrap();
        if completed {
            f.store
                .accept_worker_finish(
                    &f.run,
                    ATTEMPT,
                    TOKEN,
                    &FinishRequest {
                        summary: "{\"answer\":\"Task complete\"}".into(),
                        status: FinishStatus::Done,
                    },
                    5.0,
                    FinishGuard::Clean,
                )
                .unwrap();
        } else {
            f.cancel();
        }
        assert!(
            f.store
                .maintenance_checkpoint_intent(f.epoch, &f.run, ATTEMPT, false)
                .unwrap()
                .is_none()
        );
        assert!(
            f.store
                .observe_maintenance_checkpoint_receipt(
                    f.epoch,
                    &f.run,
                    ATTEMPT,
                    CHALLENGE,
                    &receipt.sha256
                )
                .is_err()
        );
        assert!(
            f.store
                .accept_maintenance_checkpoint(
                    f.epoch,
                    &f.run,
                    ATTEMPT,
                    TOKEN,
                    &Fixture::input(),
                    6.0,
                    FinishGuard::Clean
                )
                .is_err()
        );
    }
    let mut f = Fixture::new();
    f.request();
    f.cancel();
    assert!(
        f.store
            .accept_maintenance_checkpoint(
                f.epoch,
                &f.run,
                ATTEMPT,
                TOKEN,
                &Fixture::input(),
                4.0,
                FinishGuard::Clean
            )
            .is_err()
    );
}

/// Exact receipt identity, current attempt, open epoch and running lifecycle
/// are checked again after acceptance; elapsed time never proves readiness.
#[test]
fn maintenance_checkpoint_rechecks_receipt_and_liveness() {
    for reason in ["closed", "terminal", "attempt", "ownership"] {
        let mut f = Fixture::new();
        f.request();
        let receipt = f
            .store
            .accept_maintenance_checkpoint(
                f.epoch,
                &f.run,
                ATTEMPT,
                TOKEN,
                &Fixture::input(),
                4.0,
                FinishGuard::Clean,
            )
            .unwrap();
        assert!(
            f.store
                .observe_maintenance_checkpoint_receipt(
                    f.epoch,
                    &f.run,
                    ATTEMPT,
                    CHALLENGE,
                    &"0".repeat(64)
                )
                .is_err()
        );
        assert!(
            f.store
                .observe_maintenance_checkpoint_receipt(
                    f.epoch,
                    &f.run,
                    ATTEMPT,
                    &"0".repeat(64),
                    &receipt.sha256
                )
                .is_err()
        );
        match reason {
            "closed" => {
                f.store.end_maintenance_fence(f.epoch, 5.0).unwrap();
            }
            "terminal" => {
                f.store
                    .conn
                    .execute(
                        "UPDATE agents SET status='cancelled',finished_at=5 WHERE id=?",
                        [f.run.as_str()],
                    )
                    .unwrap();
            }
            "attempt" => {
                f.store
                    .conn
                    .execute("UPDATE attempts SET finished_at=5 WHERE id=?", [ATTEMPT])
                    .unwrap();
            }
            _ => {
                f.store
                    .conn
                    .execute(
                        "UPDATE attempts SET ownership_active=0 WHERE id=?",
                        [ATTEMPT],
                    )
                    .unwrap();
            }
        }
        assert!(
            f.store
                .maintenance_checkpoint_intent(f.epoch, &f.run, ATTEMPT, false)
                .unwrap()
                .is_none()
        );
        assert!(
            f.store
                .observe_maintenance_checkpoint_receipt(
                    f.epoch,
                    &f.run,
                    ATTEMPT,
                    CHALLENGE,
                    &receipt.sha256
                )
                .is_err()
        );
    }
}

/// A real read-only connection owns/reuses a deferred snapshot. A concurrent
/// cancel is visible after that snapshot ends, never halfway through the read.
#[test]
fn maintenance_checkpoint_read_only_snapshot_is_consistent() {
    let mut f = Fixture::new();
    f.request();
    f.store
        .accept_maintenance_checkpoint(
            f.epoch,
            &f.run,
            ATTEMPT,
            TOKEN,
            &Fixture::input(),
            4.0,
            FinishGuard::Clean,
        )
        .unwrap();
    let reader = StoreReader::open(&f.home.path).unwrap();
    reader.conn.execute_batch("BEGIN").unwrap();
    reader
        .conn
        .query_row("SELECT COUNT(*) FROM agents", [], |r| r.get::<_, i64>(0))
        .unwrap();
    f.cancel();
    assert_eq!(
        reader
            .maintenance_checkpoint_intent(f.epoch, &f.run, ATTEMPT, false)
            .unwrap(),
        Some(Fixture::input())
    );
    assert!(
        !reader.conn.is_autocommit(),
        "the caller still owns its snapshot"
    );
    reader.conn.execute_batch("ROLLBACK").unwrap();
    assert!(
        reader
            .maintenance_checkpoint_intent(f.epoch, &f.run, ATTEMPT, false)
            .unwrap()
            .is_none()
    );
    assert!(reader.conn.is_autocommit());
}
