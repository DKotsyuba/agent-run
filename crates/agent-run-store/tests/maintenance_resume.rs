//! Durable allocation tests with synthetic history; no native process is launched.
mod common;
use agent_run_domain::{
    Error,
    domain::AgentId,
    worker::{FinishGuard, MaintenancePauseRequest, maintenance_pause_catalog_digest},
};
use agent_run_platform::process::{self, ProcessState};
use agent_run_store::{
    Store,
    maintenance_park::NativeSettlement,
    maintenance_resume::{MaintenanceResume, ResumePreflight},
    ops::ExpectedOwnership,
};
use serde_json::{Value, json};
use std::{
    sync::{Arc, Barrier},
    thread,
};

const ATTEMPT: &str = "source-attempt";
const ACCOUNT: &str = "acct-resume";
const TOKEN: &str = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
const SESSION: &str = "same-native-session";

/// A parked source with retained account/keys and a complete current writer owner.
struct Fixture {
    home: common::Home,
    store: Store<'static>,
    run: AgentId,
    epoch: i64,
    preflight: ResumePreflight,
}

impl Fixture {
    /// Seed store-only native evidence, then obtain a real OS Reused observation
    /// against this process's deliberately old fixture generation outside SQL.
    fn new() -> Self {
        let home = common::Home::new();
        let mut store = home.store();
        let account=serde_json::from_value(json!({"account_id":ACCOUNT,"auth_family":"openai","secret_ref":"env:FIXTURE_RESUME","status":"enabled"})).unwrap();
        store.register_account(&account).unwrap();
        let mut role = json!({"role_name":"review","role_revision":"fixture","prompt":"Review.","grants":{"write":false,"network":false,"allow_external_read_roots":false,"read_roots":[]},"skills":[],"mcp":[],"required_constraints":[],"auth":{"mode":"global","reference":null},"worker_mcp":true});
        role["config_revision"] = json!(agent_run_domain::canonical::sha256_hex(&role, true));
        let mut request = home.request();
        request.explicit_finish = true;
        let identity = json!({"provider_identity_version":2,"authority":{"role_payload":role,"provider":"codex-fixture","harness":"codex","assets_sha256":"c".repeat(64)}});
        let (run, _) = store
            .admit(&request, &home.config, &identity, None)
            .unwrap();
        let me = process::inspect(std::process::id() as i32).unwrap();
        store.conn.execute("INSERT INTO writer_authority(id,epoch,holder_pid,holder_birth,holder_token,acquired_at,probe_at) VALUES(1,17,?,?,?,1,1)",rusqlite::params![me.pid,me.birth,me.token]).unwrap();
        let old_token = format!("{}-old-generation", me.token);
        let old_birth = me.birth - 1.0;
        store.conn.execute("UPDATE agents SET status='running',started_at=0.5,runtime_session_id=?,process_group_id=12345,selection_intent='pinned',sequence=3,supervisor_pid=?,supervisor_identity=?,supervisor_birth_time=?,supervisor_capability_sha256=? WHERE id=?",rusqlite::params![SESSION,me.pid,old_token,old_birth,"f".repeat(64),run.as_str()]).unwrap();
        store.conn.execute("INSERT INTO attempts(id,agent_id,number,state,adapter_state_json,created_at,ownership_active,selected_account_id,process_identity,process_birth_time) VALUES(?,?,1,'running','{}',1,1,?,'fixture-engine',1)",rusqlite::params![ATTEMPT,run.as_str(),ACCOUNT]).unwrap();
        for key in ["tokens", "requests"] {
            store
                .conn
                .execute(
                    "INSERT INTO attempt_quota_keys(attempt_id,quota_key) VALUES(?,?)",
                    rusqlite::params![ATTEMPT, format!("{ACCOUNT}::{key}")],
                )
                .unwrap();
        }
        store.conn.execute("INSERT INTO pools(id,request_namespace,request_id,request_sha256,goal,acceptance_json,state,roster_revision,created_at) VALUES('pool-resume','fixture','pool',?,'Review','[]','open',1,1)",["0".repeat(64)]).unwrap();
        store.conn.execute("INSERT INTO pool_members(agent_id,pool_id,slot,name,role,personal_task,joined_roster_revision) VALUES(?,'pool-resume',1,'worker','review','task',1)",[run.as_str()]).unwrap();
        store
            .issue_worker_capability(&run, ATTEMPT, TOKEN, 1.0)
            .unwrap();
        store.begin_worker_lifecycle(&run, ATTEMPT, 1.0).unwrap();
        store.conn.execute("UPDATE worker_capabilities SET pool_catalog_version=5,pool_catalog_digest=?,pool_catalog_observed_at=1 WHERE attempt_id=?",rusqlite::params![maintenance_pause_catalog_digest(false),ATTEMPT]).unwrap();
        let epoch = store
            .begin_maintenance_fence("resume-fixture", 2.0)
            .unwrap()
            .epoch;
        let input = MaintenancePauseRequest {
            challenge_id: "b".repeat(64),
            checkpoint: "Preserved task context.".into(),
        };
        let steer = store
            .request_maintenance_steer(epoch, &run, ATTEMPT, &input.challenge_id, 3.0)
            .unwrap();
        assert_eq!(
            store
                .claim_command_for_turn(&run, false)
                .unwrap()
                .unwrap()
                .0,
            steer.command_id
        );
        store
            .complete_command(
                &run,
                steer.command_id,
                &json!({"accepted":true,"observation":"native_accepted"}),
            )
            .unwrap();
        let receipt = store
            .accept_maintenance_checkpoint(
                epoch,
                &run,
                ATTEMPT,
                TOKEN,
                &input,
                4.0,
                FinishGuard::Clean,
            )
            .unwrap();
        store
            .observe_maintenance_checkpoint_receipt(
                epoch,
                &run,
                ATTEMPT,
                &input.challenge_id,
                &receipt.sha256,
            )
            .unwrap();
        let event = NativeSettlement {
            version: 1,
            protocol: "codex".into(),
            coverage: "owned_foreground_v1".into(),
            maintenance_epoch: epoch,
            agent_id: run.clone(),
            attempt_id: ATTEMPT.into(),
            challenge_id: input.challenge_id,
            receipt_sha256: receipt.sha256,
            thread_id: SESSION.into(),
            turn_id: "pause-turn".into(),
            item_id: "pause-item".into(),
        };
        store.conn.execute("INSERT INTO events(agent_id,attempt_id,at,kind,data_json) VALUES(?,?,5,'maintenance_native_settled_v1',?)",rusqlite::params![run.as_str(),ATTEMPT,serde_json::to_string(&event).unwrap()]).unwrap();
        let event = store.conn.last_insert_rowid();
        assert!(
            store
                .begin_maintenance_closing(epoch, &run, ATTEMPT, event, 5.5)
                .unwrap()
        );
        let cleanup = json!({"signals":[],"scope":"verified_descendants","group_gone":true,"descendants_gone":true,"confirmed":true,"process_group_id":12345});
        let history = json!({"native_history":{"attempt":ATTEMPT,"provider":"codex-fixture","assets_sha256":"c".repeat(64),"task_proven":true,"task_proof":{"turn":"original-task","input_sha256":"d".repeat(64)},"seal":{"harness":"codex","session":SESSION,"root":home.path,"relative":"sessions/history.jsonl","bytes":128,"records":3,"sha256":"e".repeat(64)}}});
        store.conn.execute("UPDATE attempts SET phase='cleanup_complete',cleanup_proof_json=?,adapter_state_json=? WHERE id=?",rusqlite::params![cleanup.to_string(),history.to_string(),ATTEMPT]).unwrap();
        let park = store
            .park_maintenance_attempt(epoch, &run, ATTEMPT, event, 6.0)
            .unwrap();
        // Admission records its actual startup owner even though this fixture
        // later supplies a different historical supervisor generation.
        let (startup_owner, startup_birth_time) = store
            .conn
            .query_row(
                "SELECT startup_owner_pid_identity,startup_owner_birth_time FROM agents WHERE id=?",
                [run.as_str()],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .unwrap();
        let expected_owner = ExpectedOwnership {
            execution: run.clone(),
            supervisor_pid: Some(me.pid),
            supervisor_identity: Some(old_token),
            supervisor_birth_time: Some(old_birth),
            process_group_id: Some(12345),
            startup_owner,
            startup_birth_time,
        };
        let supervisor_state = process::observe(
            expected_owner.supervisor_pid,
            expected_owner.supervisor_identity.as_deref(),
            expected_owner.supervisor_birth_time,
        );
        assert_eq!(supervisor_state, ProcessState::Reused);
        let preflight = ResumePreflight {
            source_attempt_id: ATTEMPT.into(),
            source_proof_sha256: park.proof_sha256,
            expected_owner,
            supervisor_state,
            verified_history_sha256: "e".repeat(64),
            current_route_fingerprint: None,
        };
        Self {
            home,
            store,
            run,
            epoch,
            preflight,
        }
    }

    /// Claim storage allocation only; never invoke any supervisor launch method.
    fn claim(&mut self) -> agent_run_domain::Result<MaintenanceResume> {
        self.store.defer_maintenance_fence(self.epoch)?;
        self.store.claim_maintenance_resume(
            self.epoch,
            &self.run,
            "resume-request",
            &self.preflight,
            10.0,
            40.0,
        )
    }

    /// Failed admission must retain the exact parked reservation and logical state.
    fn assert_unclaimed(&self) {
        assert!(
            self.store
                .maintenance_resume(self.epoch, &self.run)
                .unwrap()
                .is_none()
        );
        assert_eq!(
            self.store
                .conn
                .query_row(
                    "SELECT COUNT(*) FROM attempts WHERE agent_id=?",
                    [self.run.as_str()],
                    |r| r.get::<_, i64>(0)
                )
                .unwrap(),
            1
        );
        assert_eq!(
            self.store
                .conn
                .query_row(
                    "SELECT state FROM attempts WHERE id=? AND ownership_active=1",
                    [ATTEMPT],
                    |r| r.get::<_, String>(0)
                )
                .unwrap(),
            "maintenance_parked"
        );
    }
}

/// One successor keeps logical/session/pool/account identity and every physical
/// key, while the original capability is revoked and startup gets a real owner.
#[test]
fn maintenance_resume_transfers_one_reservation_without_launch() {
    let mut f = Fixture::new();
    let before = f.store.get(&f.run).unwrap();
    let revision = f.store.quota_capacity_revision().unwrap();
    let claim = f.claim().unwrap();
    let after = f.store.get(&f.run).unwrap();
    assert_eq!(after.id, before.id);
    assert_eq!(after.sequence, before.sequence);
    assert_eq!(after.root_agent_id, before.root_agent_id);
    assert_eq!(after.request, before.request);
    assert_eq!(after.identity, before.identity);
    assert_eq!(after.runtime_session_id, before.runtime_session_id);
    assert_eq!(after.started_at, Some(0.5));
    assert_eq!(after.status, agent_run_domain::domain::Status::Starting);
    assert!(after.supervisor_pid.is_none());
    assert_eq!(claim.account_id, ACCOUNT);
    assert!(claim.launch_authorized);
    assert_eq!(claim.writer_epoch, 17);
    assert_eq!(claim.target_number, 2);
    let source: (String, bool, Option<f64>) = f
        .store
        .conn
        .query_row(
            "SELECT state,ownership_active,finished_at FROM attempts WHERE id=?",
            [ATTEMPT],
            |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
        )
        .unwrap();
    assert_eq!(source, ("maintenance_paused".into(), false, Some(10.0)));
    let target: (String, String, bool, Option<String>) = f
        .store
        .conn
        .query_row(
            "SELECT state,phase,ownership_active,process_identity FROM attempts WHERE id=?",
            [&claim.target_attempt_id],
            |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?)),
        )
        .unwrap();
    assert_eq!(target, ("prepared".into(), "prepared".into(), true, None));
    assert_eq!(f.store.conn.query_row("SELECT COUNT(*) FROM attempt_quota_keys a JOIN attempt_quota_keys b ON a.quota_key=b.quota_key WHERE a.attempt_id=? AND b.attempt_id=?",rusqlite::params![ATTEMPT,claim.target_attempt_id],|r|r.get::<_,i64>(0)).unwrap(),2);
    let (owner, cap): (String, Option<String>) = f
        .store
        .conn
        .query_row(
            "SELECT startup_owner_pid_identity,supervisor_capability_sha256 FROM agents WHERE id=?",
            [f.run.as_str()],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )
        .unwrap();
    assert!(cap.is_none());
    assert_eq!(
        serde_json::from_str::<Value>(&owner).unwrap()["pid"],
        std::process::id()
    );
    assert_eq!(
        f.store
            .conn
            .query_row("SELECT COUNT(*) FROM agents", [], |r| r.get::<_, i64>(0))
            .unwrap(),
        1
    );
    assert_eq!(
        f.store
            .conn
            .query_row(
                "SELECT pool_id FROM pool_members WHERE agent_id=?",
                [f.run.as_str()],
                |r| r.get::<_, String>(0)
            )
            .unwrap(),
        "pool-resume"
    );
    assert_eq!(f.store.quota_capacity_revision().unwrap(), revision + 1);
}

/// Only the exact prepared successor can read its verified original context;
/// cancellation or changed evidence never falls back to the original task.
#[test]
fn maintenance_continuation_reads_only_verified_prepared_successor() {
    let mut f = Fixture::new();
    assert!(
        f.store
            .maintenance_continuation(&f.run, ATTEMPT)
            .unwrap()
            .is_none()
    );
    let claim = f.claim().unwrap();
    let context = f
        .store
        .maintenance_continuation(&f.run, &claim.target_attempt_id)
        .unwrap()
        .unwrap();
    assert_eq!(context.epoch, f.epoch);
    assert_eq!(context.source_attempt_id, ATTEMPT);
    assert_eq!(context.session, SESSION);
    assert_eq!(context.checkpoint.checkpoint, "Preserved task context.");
    assert!(!format!("{context:?}").contains("Preserved task context."));
    f.store
        .conn
        .execute(
            "UPDATE attempts SET adapter_state_json='{}' WHERE id=?",
            [ATTEMPT],
        )
        .unwrap();
    assert!(matches!(
        f.store
            .maintenance_continuation(&f.run, &claim.target_attempt_id),
        Err(Error::Integrity(_))
    ));
}

/// A cancellation accepted between allocation and preparation wins before a
/// new native process or continuation input is submitted.
#[test]
fn maintenance_continuation_refuses_pending_cancel() {
    let mut f = Fixture::new();
    let claim = f.claim().unwrap();
    f.store.conn.execute("INSERT INTO commands(agent_id,kind,payload_json,state,created_at) VALUES(?,'cancel','{}','pending',11)", [f.run.as_str()]).unwrap();
    assert!(matches!(
        f.store
            .maintenance_continuation(&f.run, &claim.target_attempt_id),
        Err(Error::ExecutionChanged)
    ));
}

/// Restart readiness requires every parked owner to be proven gone; only a
/// newer writer can enter recovery and allocate the same session afterwards.
#[test]
fn maintenance_ready_and_restart_are_distinct_durable_phases() {
    use agent_run_store::maintenance::MaintenanceReadyMember;
    let mut f = Fixture::new();
    let mut member = MaintenanceReadyMember {
        execution: f.run.clone(),
        preflight: f.preflight.clone(),
    };
    member.preflight.supervisor_state = ProcessState::Alive;
    assert!(
        f.store
            .ready_maintenance_fence(f.epoch, &[member.clone()], 8.0)
            .is_err()
    );
    member.preflight.supervisor_state = ProcessState::Reused;
    let ready = f
        .store
        .ready_maintenance_fence(f.epoch, &[member], 8.0)
        .unwrap();
    assert_eq!(ready.phase, "ready");
    assert_eq!(ready.ready_writer_epoch, Some(17));
    assert!(f.store.observe_maintenance_restart(f.epoch).is_err());
    assert!(
        f.store
            .claim_maintenance_resume(f.epoch, &f.run, "too-early", &f.preflight, 9.0, 40.0)
            .is_err()
    );
    assert!(f.store.maintenance_shutdown_ready().unwrap());
    assert!(f.store.commit_maintenance_stop(f.epoch).unwrap());
    assert!(f.store.defer_maintenance_fence(f.epoch).is_err());
    f.store
        .conn
        .execute("UPDATE writer_authority SET epoch=18 WHERE id=1", [])
        .unwrap();
    assert!(!f.store.commit_maintenance_stop(f.epoch).unwrap());
    assert_eq!(
        f.store.observe_maintenance_restart(f.epoch).unwrap().phase,
        "recovering"
    );
    let claim = f
        .store
        .claim_maintenance_resume(f.epoch, &f.run, "after-restart", &f.preflight, 10.0, 40.0)
        .unwrap();
    assert_eq!(claim.source_attempt_id, ATTEMPT);
}

/// Readiness without an actual stop is not reported as a completed restart.
#[test]
fn maintenance_ready_then_broker_crash_defers() {
    use agent_run_store::maintenance::MaintenanceReadyMember;
    let mut f = Fixture::new();
    f.store
        .ready_maintenance_fence(
            f.epoch,
            &[MaintenanceReadyMember {
                execution: f.run.clone(),
                preflight: f.preflight.clone(),
            }],
            8.0,
        )
        .unwrap();
    f.store
        .conn
        .execute("UPDATE writer_authority SET epoch=18 WHERE id=1", [])
        .unwrap();
    assert!(!f.store.commit_maintenance_stop(f.epoch).unwrap());
    assert_eq!(
        f.store.observe_maintenance_restart(f.epoch).unwrap().phase,
        "deferring"
    );
}

/// Stop and deferral serialize at one writer boundary: neither may begin a
/// continuation after the other committed an OS stop.
#[test]
fn maintenance_stop_and_deferral_cannot_both_win() {
    use agent_run_store::maintenance::MaintenanceReadyMember;
    for _ in 0..4 {
        let mut f = Fixture::new();
        f.store
            .ready_maintenance_fence(
                f.epoch,
                &[MaintenanceReadyMember {
                    execution: f.run.clone(),
                    preflight: f.preflight.clone(),
                }],
                8.0,
            )
            .unwrap();
        let gate = Arc::new(Barrier::new(2));
        let mut handles = Vec::new();
        for stop in [true, false] {
            let path = f.home.path.clone();
            let gate = gate.clone();
            let epoch = f.epoch;
            handles.push(thread::spawn(move || {
                let mut store = agent_run_store::fixture::open(&path).unwrap();
                gate.wait();
                if stop {
                    store.commit_maintenance_stop(epoch).unwrap()
                } else {
                    store.defer_maintenance_fence(epoch).is_ok()
                }
            }));
        }
        let stop = handles.remove(0).join().unwrap();
        let deferred = handles.remove(0).join().unwrap();
        assert_ne!(stop, deferred);
        let phase = f.store.active_maintenance_fence().unwrap().unwrap().phase;
        assert_eq!(phase, if stop { "stopping" } else { "deferring" });
    }
}

/// A broker failure before readiness is a deferral, never evidence that a
/// requested restart completed or that the work has already resumed.
#[test]
fn maintenance_interrupted_preparation_enters_deferral() {
    let mut f = Fixture::new();
    f.store
        .conn
        .execute("UPDATE writer_authority SET epoch=18 WHERE id=1", [])
        .unwrap();
    assert_eq!(
        f.store.observe_maintenance_restart(f.epoch).unwrap().phase,
        "deferring"
    );
    assert!(f.store.end_maintenance_fence(f.epoch, 9.0).is_err());
}

/// Replay returns allocation identity after an uncertain launch boundary; it
/// cannot reset the target or create another attempt, even after reconnection.
#[test]
fn maintenance_resume_replay_never_reallocates_unknown_target() {
    let mut f = Fixture::new();
    let claim = f.claim().unwrap();
    f.store
        .conn
        .execute(
            "UPDATE attempts SET state='starting',phase='spawning' WHERE id=?",
            [&claim.target_attempt_id],
        )
        .unwrap();
    drop(f.store);
    f.store = f.home.store();
    let replay = f.claim().unwrap();
    assert!(!replay.launch_authorized);
    let mut original_identity = claim.clone();
    original_identity.launch_authorized = false;
    assert_eq!(replay, original_identity);
    assert_eq!(
        f.store
            .conn
            .query_row("SELECT COUNT(*) FROM attempts", [], |r| r.get::<_, i64>(0))
            .unwrap(),
        2
    );
    assert_eq!(
        f.store
            .conn
            .query_row(
                "SELECT phase FROM attempts WHERE id=?",
                [&claim.target_attempt_id],
                |r| r.get::<_, String>(0)
            )
            .unwrap(),
        "spawning"
    );
    assert!(matches!(
        f.store.claim_maintenance_resume(
            f.epoch,
            &f.run,
            "different-request",
            &f.preflight,
            11.0,
            41.0
        ),
        Err(Error::Conflict)
    ));
    f.preflight.verified_history_sha256 = "0".repeat(64);
    assert!(matches!(f.claim(), Err(Error::Conflict)));
}

/// A replay of the exact same writer envelope keeps its durable result but
/// cannot authorize another native supervisor dispatch.
#[test]
fn maintenance_resume_same_writer_receipt_replay_disarms_dispatch() {
    use agent_run_domain::storage::{
        MonoDeadline, OperationId, ProducerId, ProducerKind, SubmitResult, WriteAuthority,
    };
    use agent_run_store::{
        ops::{ClaimMaintenanceResume, LocalEnvelope, LocalOp},
        writer::{StoreWriter, WriterLease},
    };
    let mut f = Fixture::new();
    f.store.defer_maintenance_fence(f.epoch).unwrap();
    let lease = WriterLease::acquire_online(&f.home.path).unwrap();
    let mut writer = StoreWriter::open(lease).unwrap();
    let (boot, now) = agent_run_platform::monotonic::now().unwrap();
    let op = ClaimMaintenanceResume {
        epoch: f.epoch,
        execution: f.run.clone(),
        request_id: "same-envelope".into(),
        preflight: f.preflight.clone(),
        at: 10.0,
        startup_deadline_at: 40.0,
    };
    let envelope = LocalEnvelope::new(
        OperationId::new(&boot, now),
        ProducerId {
            kind: ProducerKind::Broker,
            scope: Some("maintenance".into()),
            instance: "fixture".into(),
        },
        1,
        MonoDeadline {
            boot_id: boot,
            ns: now + 60_000_000_000,
        },
        LocalOp::from(op),
    )
    .unwrap();
    let SubmitResult::Committed(first) = writer.apply_local(&WriteAuthority::Broker, &envelope)
    else {
        panic!("first claim was not committed")
    };
    let SubmitResult::Committed(replayed) = writer.apply_local(&WriteAuthority::Broker, &envelope)
    else {
        panic!("claim replay was not committed")
    };
    assert!(!first.replayed);
    assert!(replayed.replayed);
    assert_eq!(first.result, replayed.result);
    let first = MaintenanceResume::from_dispatch_receipt(&first).unwrap();
    let replayed = MaintenanceResume::from_dispatch_receipt(&replayed).unwrap();
    assert!(first.launch_authorized);
    assert!(!replayed.launch_authorized);
    assert_eq!(first.target_attempt_id, replayed.target_attempt_id);
}

/// An allocated successor is still an unknown launch until the exact native
/// continuation is confirmed. It remains fenced and is not marked lost.
#[test]
fn maintenance_resume_is_held_until_exact_continuation_is_confirmed() {
    let mut f = Fixture::new();
    let resume = f.claim().unwrap();
    assert!(agent_run_store::reconcile::has_maintenance_handoff(&f.store.conn, &f.run).unwrap());
    assert!(agent_run_store::reconcile::maintenance_loss_blocked(&f.store.conn, &f.run).unwrap());
    assert!(f.store.end_maintenance_fence(f.epoch, 12.0).is_err());
    f.store.conn.execute(
        "INSERT INTO events(agent_id,attempt_id,at,kind,data_json) VALUES(?,?,10,'maintenance_resume_confirmed_v1',?)",
        rusqlite::params![f.run.as_str(),resume.target_attempt_id.as_str(),json!({"epoch":f.epoch,"source_attempt_id":resume.source_attempt_id.as_str(),"target_attempt_id":resume.target_attempt_id.as_str()}).to_string()],
    ).unwrap();
    assert!(agent_run_store::reconcile::has_maintenance_handoff(&f.store.conn, &f.run).unwrap());
    assert!(f.store.end_maintenance_fence(f.epoch, 12.0).is_err());
    f.store
        .conn
        .execute(
            "UPDATE attempts SET ownership_active=0,finished_at=10 WHERE id=?",
            [&resume.target_attempt_id],
        )
        .unwrap();
    assert!(agent_run_store::reconcile::has_maintenance_handoff(&f.store.conn, &f.run).unwrap());
    assert!(f.store.end_maintenance_fence(f.epoch, 12.0).is_err());
    f.store
        .conn
        .execute(
            "UPDATE attempts SET ownership_active=1,finished_at=NULL WHERE id=?",
            [&resume.target_attempt_id],
        )
        .unwrap();
    f.store.conn.execute(
        "INSERT INTO events(agent_id,attempt_id,at,kind,data_json) VALUES(?,?,11,'maintenance_resume_confirmed_v1',?)",
        rusqlite::params![f.run.as_str(), resume.target_attempt_id.as_str(),
            json!({"epoch":f.epoch,"source_attempt_id":resume.source_attempt_id.as_str(),"target_attempt_id":resume.target_attempt_id.as_str(),"session":SESSION,"turn":"resumed-turn","input_sha256":"d".repeat(64)}).to_string()],
    ).unwrap();
    assert!(!agent_run_store::reconcile::has_maintenance_handoff(&f.store.conn, &f.run).unwrap());
    assert!(f.store.end_maintenance_fence(f.epoch, 12.0).is_ok());
}

/// A proven dead owner of a started but unconfirmed continuation is reported
/// lost without resending input, and no longer blocks other admissions.
#[test]
fn dead_unconfirmed_continuation_is_reported_and_releases_the_fence() {
    let mut f = Fixture::new();
    let resume = f.claim().unwrap();
    f.store.conn.execute(
        "UPDATE attempts SET state='running',phase='spawning',process_identity='fixture',process_birth_time=5 WHERE id=?",
        [&resume.target_attempt_id],
    ).unwrap();
    assert!(agent_run_store::reconcile::maintenance_loss_blocked(&f.store.conn, &f.run).unwrap());
    f.store.conn.execute(
        "UPDATE agents SET status='running',supervisor_pid=123,supervisor_identity='linux:fixture',supervisor_birth_time=5,process_group_id=456,heartbeat_at=9 WHERE id=?",
        [f.run.as_str()],
    ).unwrap();
    let (startup, birth): (Option<String>, Option<f64>) = f
        .store
        .conn
        .query_row(
            "SELECT startup_owner_pid_identity,startup_owner_birth_time FROM agents WHERE id=?",
            [f.run.as_str()],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .unwrap();
    let mut op = agent_run_store::ops::MarkLost {
        expected: ExpectedOwnership {
            execution: f.run.clone(),
            supervisor_pid: Some(123),
            supervisor_identity: Some("linux:fixture".into()),
            supervisor_birth_time: Some(5.0),
            process_group_id: Some(456),
            startup_owner: startup,
            startup_birth_time: birth,
        },
        failure_kind: "supervisor_dead".into(),
        failure_text: "fixture".into(),
        evidence: json!({"verdict":"dead"}),
        checked_at: 12.0,
    };
    assert!(agent_run_store::reconcile::has_maintenance_handoff(&f.store.conn, &f.run).unwrap());
    assert!(!agent_run_store::reconcile::maintenance_loss_blocked(&f.store.conn, &f.run).unwrap());
    op.expected.supervisor_pid = Some(124);
    assert!(!f.store.mark_lost(&op).unwrap());
    op.expected.supervisor_pid = Some(123);
    op.checked_at = 8.0;
    assert!(!f.store.mark_lost(&op).unwrap());
    op.checked_at = 12.0;
    assert!(f.store.mark_lost(&op).unwrap());
    assert!(!f.store.mark_lost(&op).unwrap());
    let row = f.store.get(&f.run).unwrap();
    assert_eq!(row.status, agent_run_domain::domain::Status::Lost);
    assert_eq!(
        row.failure_kind.as_deref(),
        Some("maintenance_continuation_unconfirmed")
    );
    assert!(
        row.failure_text
            .as_deref()
            .unwrap()
            .contains("may have started")
    );
    assert!(f.store.end_maintenance_fence(f.epoch, 13.0).is_ok());
    assert_eq!(
        f.store
            .conn
            .query_row(
                "SELECT COUNT(*) FROM maintenance_resumes WHERE agent_id=?",
                [f.run.as_str()],
                |row| row.get::<_, i64>(0),
            )
            .unwrap(),
        1
    );
    assert!(
        f.store
            .conn
            .query_row(
                "SELECT ownership_active=1 AND cleanup_proof_json IS NULL FROM attempts WHERE id=?",
                [&resume.target_attempt_id],
                |row| row.get::<_, bool>(0),
            )
            .unwrap()
    );
}

/// A dead, registered supervisor that never launched the continuation can be
/// reported lost; manual resume still waits for every cleanup proof.
#[test]
fn dead_prepared_supervisor_requires_settled_preparation_and_cleanup() {
    let mut f = Fixture::new();
    let resume = f.claim().unwrap();
    let me = process::inspect(std::process::id() as i32).unwrap();
    let old_token = format!("{}-old", me.token);
    let old_birth = me.birth - 1.0;
    f.store.conn.execute(
        "UPDATE agents SET supervisor_pid=?,supervisor_identity=?,supervisor_birth_time=?,process_group_id=NULL,heartbeat_at=9 WHERE id=?",
        rusqlite::params![me.pid,old_token,old_birth,f.run.as_str()],
    ).unwrap();
    let (startup, birth): (Option<String>, Option<f64>) = f
        .store
        .conn
        .query_row(
            "SELECT startup_owner_pid_identity,startup_owner_birth_time FROM agents WHERE id=?",
            [f.run.as_str()],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .unwrap();
    let op = agent_run_store::ops::MarkLost {
        expected: ExpectedOwnership {
            execution: f.run.clone(),
            supervisor_pid: Some(me.pid),
            supervisor_identity: Some(old_token),
            supervisor_birth_time: Some(old_birth),
            process_group_id: None,
            startup_owner: startup,
            startup_birth_time: birth,
        },
        failure_kind: "supervisor_dead".into(),
        failure_text: "fixture".into(),
        evidence: json!({"verdict":"reused"}),
        checked_at: 12.0,
    };
    f.store.conn.execute(
        "INSERT INTO events(agent_id,attempt_id,at,kind,data_json) VALUES(?,?,11,'maintenance_discovery_started_v1','{}')",
        rusqlite::params![f.run.as_str(),resume.target_attempt_id],
    ).unwrap();
    assert!(!f.store.mark_lost(&op).unwrap());
    f.store
        .conn
        .execute(
            "DELETE FROM events WHERE agent_id=? AND kind='maintenance_discovery_started_v1'",
            [f.run.as_str()],
        )
        .unwrap();
    assert!(f.store.mark_lost(&op).unwrap());
    let row = f.store.get(&f.run).unwrap();
    assert_eq!(
        row.failure_kind.as_deref(),
        Some("maintenance_resume_start_failed")
    );
    f.store.end_maintenance_fence(f.epoch, 13.0).unwrap();
    assert!(
        f.store
            .admit(&f.home.request(), &f.home.config, &json!({}), Some(&row))
            .is_err()
    );
    f.store.conn.execute(
        "UPDATE attempts SET phase='cleanup_complete',cleanup_proof_json='{\"never_spawned\":true}',ownership_active=0 WHERE id=?",
        [&resume.target_attempt_id],
    ).unwrap();
    f.store
        .conn
        .execute(
            "UPDATE agents SET supervisor_identity=?,supervisor_birth_time=? WHERE id=?",
            rusqlite::params![me.token, me.birth, f.run.as_str()],
        )
        .unwrap();
    let live = f.store.get(&f.run).unwrap();
    assert!(
        f.store
            .admit(&f.home.request(), &f.home.config, &json!({}), Some(&live))
            .is_err()
    );
    f.store
        .conn
        .execute(
            "UPDATE agents SET supervisor_identity=?,supervisor_birth_time=? WHERE id=?",
            rusqlite::params![op.expected.supervisor_identity, old_birth, f.run.as_str()],
        )
        .unwrap();
    let row = f.store.get(&f.run).unwrap();
    assert!(
        f.store
            .admit(&f.home.request(), &f.home.config, &json!({}), Some(&row))
            .is_ok()
    );
}

/// A proven-dead pre-registration child is reported without launching a new
/// attempt; incomplete child identity never authorizes that transition.
#[test]
fn dead_unregistered_child_is_reported_after_exact_ownership_proof() {
    let mut f = Fixture::new();
    let resume = f.claim().unwrap();
    let me = process::inspect(std::process::id() as i32).unwrap();
    let old_token = format!("{}-old", me.token);
    let old_birth = me.birth - 1.0;
    let full = json!({"pid":me.pid,"ppid":me.ppid,"group":me.pid,"birth":old_birth,"token":old_token,"zombie":false});
    f.store.conn.execute(
        "UPDATE agents SET startup_owner_pid_identity=?,startup_owner_birth_time=?,supervisor_capability_sha256=? WHERE id=?",
        rusqlite::params![full.to_string(),old_birth,"a".repeat(64),f.run.as_str()],
    ).unwrap();
    let mut op = agent_run_store::ops::MarkLost {
        expected: ExpectedOwnership {
            execution: f.run.clone(),
            supervisor_pid: None,
            supervisor_identity: None,
            supervisor_birth_time: None,
            process_group_id: None,
            startup_owner: Some(full.to_string()),
            startup_birth_time: Some(old_birth),
        },
        failure_kind: "unowned_starting".into(),
        failure_text: "fixture".into(),
        evidence: json!({"verdict":"reused"}),
        checked_at: 12.0,
    };
    let mut incomplete = full.clone();
    incomplete.as_object_mut().unwrap().remove("group");
    f.store
        .conn
        .execute(
            "UPDATE agents SET startup_owner_pid_identity=? WHERE id=?",
            rusqlite::params![incomplete.to_string(), f.run.as_str()],
        )
        .unwrap();
    op.expected.startup_owner = Some(incomplete.to_string());
    assert!(!f.store.mark_lost(&op).unwrap());
    f.store
        .conn
        .execute(
            "UPDATE agents SET startup_owner_pid_identity=? WHERE id=?",
            rusqlite::params![full.to_string(), f.run.as_str()],
        )
        .unwrap();
    op.expected.startup_owner = Some(full.to_string());
    let marker = json!({"epoch":f.epoch,"source_attempt_id":resume.source_attempt_id,"target_attempt_id":resume.target_attempt_id}).to_string();
    f.store.conn.execute("UPDATE agents SET identity_json=json_set(identity_json,'$.authority.harness','claude-code') WHERE id=?",[f.run.as_str()]).unwrap();
    for kind in [
        "maintenance_discovery_started_v1",
        "maintenance_discovery_settled_v1",
    ] {
        f.store
            .conn
            .execute(
                "INSERT INTO events(agent_id,attempt_id,at,kind,data_json) VALUES(?,?,11,?,?)",
                rusqlite::params![f.run.as_str(), resume.target_attempt_id, kind, marker],
            )
            .unwrap();
    }
    assert!(!f.store.mark_lost(&op).unwrap());
    f.store.conn.execute("DELETE FROM events WHERE agent_id=? AND kind IN ('maintenance_discovery_started_v1','maintenance_discovery_settled_v1')",[f.run.as_str()]).unwrap();
    f.store.conn.execute("UPDATE agents SET identity_json=json_set(identity_json,'$.authority.harness','codex') WHERE id=?",[f.run.as_str()]).unwrap();
    assert!(f.store.mark_lost(&op).unwrap());
    let parent = f.store.get(&f.run).unwrap();
    assert_eq!(
        parent.failure_kind.as_deref(),
        Some("maintenance_resume_start_failed")
    );
    f.store.end_maintenance_fence(f.epoch, 13.0).unwrap();
    assert!(
        f.store
            .admit(&f.home.request(), &f.home.config, &json!({}), Some(&parent))
            .is_err()
    );
    f.store.conn.execute("UPDATE attempts SET phase='cleanup_complete',cleanup_proof_json='{\"never_spawned\":true}',ownership_active=0 WHERE id=?",[&resume.target_attempt_id]).unwrap();
    assert!(
        f.store
            .admit(&f.home.request(), &f.home.config, &json!({}), Some(&parent))
            .is_ok()
    );
}

/// Actual non-death/unknown verdicts, changed owner/history/route or disabled
/// account refuse before releasing the source reservation.
#[test]
fn maintenance_resume_refuses_unverified_or_changed_preflight() {
    for state in [
        ProcessState::Alive,
        ProcessState::Unknown,
        ProcessState::Denied,
        ProcessState::NotStarted,
    ] {
        let mut f = Fixture::new();
        f.preflight.supervisor_state = state;
        assert!(f.claim().is_err());
        f.assert_unclaimed();
    }
    for kind in ["owner", "proof", "history", "route", "account"] {
        let mut f = Fixture::new();
        match kind {
            "owner" => f.preflight.expected_owner.supervisor_birth_time = Some(1.0),
            "proof" => f.preflight.source_proof_sha256 = "0".repeat(64),
            "history" => f.preflight.verified_history_sha256 = "0".repeat(64),
            "route" => f.preflight.current_route_fingerprint = Some("0".repeat(64)),
            _ => {
                f.store.disable_account(&ACCOUNT.parse().unwrap()).unwrap();
            }
        }
        assert!(f.claim().is_err(), "{kind}");
        f.assert_unclaimed();
    }
}

/// Missing writer pid/token/birth cannot become a fabricated new startup owner.
#[test]
fn maintenance_resume_requires_complete_resident_writer_identity() {
    for statement in [
        "DELETE FROM writer_authority",
        "UPDATE writer_authority SET holder_token=NULL",
        "UPDATE writer_authority SET holder_birth=NULL",
        "UPDATE writer_authority SET holder_pid=1",
    ] {
        let mut f = Fixture::new();
        f.store.conn.execute(statement, []).unwrap();
        assert!(f.claim().is_err());
        f.assert_unclaimed();
    }
}

/// Current cancellation, completion or a closed maintenance epoch defeats a
/// fresh claim. An outer transaction abort restores both source and target.
#[test]
fn maintenance_resume_preserves_cancel_and_outer_transaction_atomicity() {
    for reason in ["cancel", "finish", "closed"] {
        let mut f = Fixture::new();
        match reason {
            "cancel" => {
                f.store.conn.execute("INSERT INTO commands(agent_id,kind,payload_json,state,created_at) VALUES(?,'cancel','{}','pending',8)",[f.run.as_str()]).unwrap();
            }
            "finish" => {
                f.store
                    .conn
                    .execute(
                        "UPDATE agents SET status='succeeded',finished_at=8 WHERE id=?",
                        [f.run.as_str()],
                    )
                    .unwrap();
            }
            _ => {
                assert!(f.store.end_maintenance_fence(f.epoch, 8.0).is_err());
                // Model a historical closed row in this disposable fixture;
                // the live API correctly refuses to abandon its parked source.
                f.store
                    .conn
                    .execute(
                        "UPDATE maintenance_fences SET ended_at=8,phase='complete',outcome='deferred' WHERE epoch=?",
                        [f.epoch],
                    )
                    .unwrap();
            }
        }
        assert!(f.claim().is_err());
        f.assert_unclaimed();
    }
    let mut f = Fixture::new();
    let revision = f.store.quota_capacity_revision().unwrap();
    f.store.conn.execute_batch("BEGIN IMMEDIATE").unwrap();
    f.claim().unwrap();
    f.store.conn.execute_batch("ROLLBACK").unwrap();
    f.assert_unclaimed();
    assert_eq!(
        f.store.get(&f.run).unwrap().status,
        agent_run_domain::domain::Status::Running
    );
    assert_eq!(f.store.quota_capacity_revision().unwrap(), revision);
}

/// Concurrent recoverers converge on one allocation and one quota revision.
#[test]
fn maintenance_resume_concurrent_claims_share_exact_target() {
    let mut f = Fixture::new();
    f.store.defer_maintenance_fence(f.epoch).unwrap();
    let revision = f.store.quota_capacity_revision().unwrap();
    let barrier = Arc::new(Barrier::new(2));
    let threads = (0..2)
        .map(|_| {
            let path = f.home.path.clone();
            let run = f.run.clone();
            let preflight = f.preflight.clone();
            let barrier = barrier.clone();
            let epoch = f.epoch;
            thread::spawn(move || {
                let mut store = agent_run_store::fixture::open(&path).unwrap();
                barrier.wait();
                store
                    .claim_maintenance_resume(epoch, &run, "same-request", &preflight, 10.0, 40.0)
                    .unwrap()
            })
        })
        .collect::<Vec<_>>();
    let results = threads
        .into_iter()
        .map(|thread| thread.join().unwrap())
        .collect::<Vec<_>>();
    assert_ne!(results[0].launch_authorized, results[1].launch_authorized);
    let mut first = results[0].clone();
    let mut second = results[1].clone();
    first.launch_authorized = false;
    second.launch_authorized = false;
    assert_eq!(first, second);
    assert_eq!(
        f.store
            .conn
            .query_row("SELECT COUNT(*) FROM attempts", [], |r| r.get::<_, i64>(0))
            .unwrap(),
        2
    );
    assert_eq!(f.store.quota_capacity_revision().unwrap(), revision + 1);
}

/// Prepare the broker's exact parked cancellation from the already verified
/// fixture owner observation and a real durable user cancel command.
fn parked_cancel(f: &mut Fixture) -> agent_run_store::ops::CancelParkedMaintenance {
    let command = f.store.enqueue(&f.run, "cancel", &json!({})).unwrap();
    agent_run_store::ops::CancelParkedMaintenance {
        epoch: f.epoch,
        execution: f.run.clone(),
        attempt: ATTEMPT.into(),
        source_proof_sha256: f.preflight.source_proof_sha256.clone(),
        cancel_command_id: command["command_id"].as_i64().unwrap(),
        expected_owner: f.preflight.expected_owner.clone(),
        supervisor_state: f.preflight.supervisor_state,
        at: 10.0,
    }
}

/// A post-park cancel produces exactly one ordinary cancellation event/outbox,
/// releases its reserved attempt and completes its command without signalling.
#[test]
fn maintenance_parked_cancel_finishes_once_and_releases_reservation() {
    let mut f = Fixture::new();
    let binding = serde_json::from_value(
        json!({"transport":"codex_queue","external_session_id":"cancel-fixture"}),
    )
    .unwrap();
    f.store.bind_orchestrator(&f.run, &binding, 8.0).unwrap();
    let op = parked_cancel(&mut f);
    assert!(f.store.cancel_parked_maintenance(&op).unwrap());
    assert!(!f.store.cancel_parked_maintenance(&op).unwrap());
    assert_eq!(
        f.store.get(&f.run).unwrap().status,
        agent_run_domain::domain::Status::Cancelled
    );
    assert!(
        !f.store
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
            .query_row(
                "SELECT state FROM commands WHERE id=?",
                [op.cancel_command_id],
                |r| r.get::<_, String>(0)
            )
            .unwrap(),
        "completed"
    );
    assert_eq!(f.store.conn.query_row("SELECT COUNT(*) FROM events WHERE agent_id=? AND kind='status' AND to_status='cancelled'",[f.run.as_str()],|r|r.get::<_,i64>(0)).unwrap(),1);
    assert_eq!(f.store.conn.query_row("SELECT COUNT(*) FROM deliveries d JOIN events e ON e.seq=d.terminal_event_seq WHERE d.agent_id=? AND e.to_status='cancelled'",[f.run.as_str()],|r|r.get::<_,i64>(0)).unwrap(),1);
}

/// Unknown/live owners, another command and stale owner/proof observations
/// cannot borrow a parked attempt's cleanup to finish a logical task.
#[test]
fn maintenance_parked_cancel_refuses_unknown_and_stale_authority() {
    for reason in ["alive", "unknown", "owner", "command", "proof"] {
        let mut f = Fixture::new();
        let mut op = parked_cancel(&mut f);
        match reason {
            "alive" => op.supervisor_state = ProcessState::Alive,
            "unknown" => op.supervisor_state = ProcessState::Unknown,
            "owner" => op.expected_owner.supervisor_birth_time = Some(1.0),
            "command" => op.cancel_command_id = -1,
            _ => op.source_proof_sha256 = "0".repeat(64),
        }
        assert!(f.store.cancel_parked_maintenance(&op).is_err(), "{reason}");
        f.assert_unclaimed();
        assert_eq!(
            f.store.get(&f.run).unwrap().status,
            agent_run_domain::domain::Status::Running
        );
    }
}

/// Terminal state, delivery and command completion share the outer transaction;
/// the new operation remains unavailable to CLI and worker writer peers.
#[test]
fn maintenance_parked_cancel_is_atomic_and_broker_only() {
    let mut f = Fixture::new();
    let op = parked_cancel(&mut f);
    f.store.conn.execute_batch("BEGIN IMMEDIATE").unwrap();
    assert!(f.store.cancel_parked_maintenance(&op).unwrap());
    f.store.conn.execute_batch("ROLLBACK").unwrap();
    f.assert_unclaimed();
    assert_eq!(
        f.store.get(&f.run).unwrap().status,
        agent_run_domain::domain::Status::Running
    );
    assert_eq!(
        f.store
            .conn
            .query_row(
                "SELECT state FROM commands WHERE id=?",
                [op.cancel_command_id],
                |r| r.get::<_, String>(0)
            )
            .unwrap(),
        "pending"
    );
    use agent_run_domain::storage::WriteAuthority;
    let kind = agent_run_store::ops::OpKindRef::CancelParkedMaintenance;
    assert!(kind.permits(&WriteAuthority::Broker));
    assert!(!kind.is_remote());
    assert!(!kind.permits(&WriteAuthority::Worker {
        execution: f.run.clone(),
        attempt: ATTEMPT.into()
    }));
}
