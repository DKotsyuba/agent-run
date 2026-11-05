//! Recovery of an already-allocated synthetic target; no native process starts.
mod common;
use agent_run_domain::{
    domain::AgentId,
    storage::{MonoDeadline, OperationId, ProducerId, ProducerKind, SubmitResult, WriteAuthority},
    worker::MaintenancePauseRequest,
};
use agent_run_platform::process::{self, ProcessState};
use agent_run_store::{
    Store,
    maintenance_prepared::PreparedMaintenanceClaim,
    ops::{ClaimMaintenancePrepared, LocalEnvelope, LocalOp},
    writer::{StoreWriter, WriterLease},
};
use serde_json::json;
use std::{
    sync::{Arc, Barrier},
    thread,
};

const SOURCE: &str = "source";
const TARGET: &str = "target";

/// Seed the durable state left AFTER the separately tested allocation commit.
/// Only the startup owner is stale; target capability/process evidence is absent.
struct Fixture {
    home: common::Home,
    store: Store<'static>,
    run: AgentId,
    op: ClaimMaintenancePrepared,
}

impl Fixture {
    fn new() -> Self {
        let home = common::Home::new();
        let mut store = home.store();
        let account=serde_json::from_value(json!({"account_id":"acct-test","auth_family":"openai","secret_ref":"env:PREPARED_FIXTURE","status":"enabled"})).unwrap();
        store.register_account(&account).unwrap();
        let (run, _) = store
            .admit(&home.request(), &home.config, &json!({}), None)
            .unwrap();
        let epoch = store
            .begin_maintenance_fence("prepared-fixture", 1.0)
            .unwrap()
            .epoch;
        store.defer_maintenance_fence(epoch).unwrap();
        let history = json!({"native_history":{"attempt":SOURCE,"task_proven":true,"seal":{"session":"session","sha256":"e".repeat(64)}}});
        store.conn.execute("INSERT INTO attempts(id,agent_id,number,state,phase,adapter_state_json,created_at,finished_at,selected_account_id,ownership_active) VALUES(?,?,1,'maintenance_paused','cleanup_complete',?,1,4,'acct-test',0)",rusqlite::params![SOURCE,run.as_str(),history.to_string()]).unwrap();
        store.conn.execute("INSERT INTO attempts(id,agent_id,number,state,phase,adapter_state_json,created_at,selected_account_id,ownership_active) VALUES(?,?,2,'prepared','prepared','{}',4,'acct-test',1)",rusqlite::params![TARGET,run.as_str()]).unwrap();
        for attempt in [SOURCE, TARGET] {
            store.conn.execute("INSERT INTO attempt_quota_keys(attempt_id,quota_key) VALUES(?,'acct-test::tokens')",[attempt]).unwrap();
        }
        let checkpoint = MaintenancePauseRequest {
            challenge_id: "b".repeat(64),
            checkpoint: "Retained context.".into(),
        };
        let raw = String::from_utf8(agent_run_domain::canonical::dumps(
            &serde_json::to_value(&checkpoint).unwrap(),
            true,
        ))
        .unwrap();
        let digest = agent_run_platform::fs::sha256(raw.as_bytes());
        store.conn.execute("INSERT INTO maintenance_checkpoints(epoch,agent_id,attempt_id,challenge_id,requested_at,closing_event_seq,closing_event_sha256,closing_at,checkpoint_json,checkpoint_sha256,accepted_at,receipt_observed) VALUES(?,?,?,?,1,1,?,2,?,?,2,1)",rusqlite::params![epoch,run.as_str(),SOURCE,checkpoint.challenge_id,"a".repeat(64),raw,digest]).unwrap();
        store.conn.execute("INSERT INTO maintenance_parks(epoch,agent_id,attempt_id,native_event_seq,proof_sha256,parked_at) VALUES(?,?,?,1,?,3)",rusqlite::params![epoch,run.as_str(),SOURCE,"c".repeat(64)]).unwrap();
        let preflight =
            json!({"source_attempt_id":SOURCE,"verified_history_sha256":"e".repeat(64)});
        store.conn.execute("INSERT INTO maintenance_resumes(epoch,agent_id,request_id,source_attempt_id,target_attempt_id,target_number,account_id,source_proof_sha256,writer_epoch,claimed_at,preflight_json) VALUES(?,?,'original-allocation',?,?,2,'acct-test',?,1,4,?)",rusqlite::params![epoch,run.as_str(),SOURCE,TARGET,"c".repeat(64),preflight.to_string()]).unwrap();
        let me = process::inspect(std::process::id() as i32).unwrap();
        let old_token = format!("{}-old", me.token);
        let old_birth = me.birth - 1.0;
        let old = json!({"pid":me.pid,"token":old_token,"birth":old_birth}).to_string();
        store.conn.execute("UPDATE agents SET runtime_session_id='session',started_at=0.5,startup_owner_pid_identity=?,startup_owner_birth_time=?,supervisor_capability_sha256=NULL WHERE id=?",rusqlite::params![old,old_birth,run.as_str()]).unwrap();
        store.conn.execute("INSERT INTO writer_authority(id,epoch,holder_pid,holder_birth,holder_token,acquired_at,probe_at) VALUES(1,2,?,?,?,5,5)",rusqlite::params![me.pid,me.birth,me.token]).unwrap();
        let state = process::observe(Some(me.pid), Some(&old_token), Some(old_birth));
        assert_eq!(state, ProcessState::Reused);
        let op = ClaimMaintenancePrepared {
            epoch,
            execution: run.clone(),
            target_attempt_id: TARGET.into(),
            expected_startup_owner: old,
            expected_startup_birth_time: old_birth,
            expected_capability_sha256: None,
            retake_current_writer: None,
            expected_current_retake_count: None,
            owner_state: state,
            at: 6.0,
            startup_deadline_at: 36.0,
        };
        Self {
            home,
            store,
            run,
            op,
        }
    }

    /// A new ownership claim must never create another target or quota lease.
    fn counts(&self) -> (i64, i64, i64) {
        (
            self.store
                .conn
                .query_row("SELECT COUNT(*) FROM attempts", [], |r| r.get(0))
                .unwrap(),
            self.store
                .conn
                .query_row("SELECT COUNT(*) FROM attempt_quota_keys", [], |r| r.get(0))
                .unwrap(),
            self.store.quota_capacity_revision().unwrap(),
        )
    }

    /// Model the old broker recording a capability but never releasing it.
    fn with_unreleased_capability(&mut self) {
        let capability = "a".repeat(64);
        self.op.expected_capability_sha256 = Some(capability.clone());
        self.store
            .conn
            .execute(
                "UPDATE agents SET identity_json=?,supervisor_capability_sha256=? WHERE id=?",
                rusqlite::params![
                    json!({"provider_identity_version":2,"authority":{"harness":"codex"}})
                        .to_string(),
                    capability,
                    self.run.as_str()
                ],
            )
            .unwrap();
        self.store.conn.execute(
            "INSERT INTO events(agent_id,attempt_id,at,kind,data_json) VALUES(?,'target',4,'maintenance_preparation_guard_v1',?)",
            rusqlite::params![self.run.as_str(),json!({"epoch":self.op.epoch,"source_attempt_id":SOURCE,"target_attempt_id":TARGET}).to_string()],
        ).unwrap();
    }
}

/// Crash-before-launch recovery changes only the startup owner, keeps the same
/// target and original allocation writer epoch, and permits one fresh dispatch.
#[test]
fn prepared_maintenance_recovery_preserves_target_and_reservation() {
    let mut f = Fixture::new();
    let before = f.counts();
    let claim = f.store.claim_maintenance_prepared(&f.op).unwrap();
    assert!(claim.launch_authorized);
    assert!(!claim.allocation.launch_authorized);
    assert_eq!(claim.allocation.target_attempt_id, TARGET);
    assert_eq!(claim.allocation.writer_epoch, 1);
    assert_eq!(claim.dispatch_writer_epoch, 2);
    assert_eq!(f.counts(), before);
    assert!(f.store.claim_maintenance_prepared(&f.op).is_err());
    let (owner, cap): (String, Option<String>) = f
        .store
        .conn
        .query_row(
            "SELECT startup_owner_pid_identity,supervisor_capability_sha256 FROM agents WHERE id=?",
            [f.run.as_str()],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )
        .unwrap();
    assert_ne!(owner, f.op.expected_startup_owner);
    assert!(cap.is_none());
    assert_eq!(f.store.get(&f.run).unwrap().sequence, 1);
}

/// A dead original broker's unreleased capability is fenced and replaced once.
#[test]
fn prepared_maintenance_reclaims_unreleased_broker_capability_once() {
    let mut f = Fixture::new();
    f.with_unreleased_capability();
    let before = f.counts();
    assert!(
        f.store
            .claim_maintenance_prepared(&f.op)
            .unwrap()
            .launch_authorized
    );
    assert!(f.store.claim_maintenance_prepared(&f.op).is_err());
    let cap: Option<String> = f
        .store
        .conn
        .query_row(
            "SELECT supervisor_capability_sha256 FROM agents WHERE id=?",
            [f.run.as_str()],
            |row| row.get(0),
        )
        .unwrap();
    assert!(cap.is_none());
    assert_eq!(f.counts(), before);
}

/// Concurrent reclaimers cannot both replace an unreleased capability.
#[test]
fn unreleased_broker_capability_has_one_concurrent_claim() {
    let mut f = Fixture::new();
    f.with_unreleased_capability();
    let barrier = Arc::new(Barrier::new(2));
    let jobs = (0..2)
        .map(|_| {
            let path = f.home.path.clone();
            let op = f.op.clone();
            let barrier = barrier.clone();
            thread::spawn(move || {
                let mut store = agent_run_store::fixture::open(&path).unwrap();
                barrier.wait();
                store.claim_maintenance_prepared(&op)
            })
        })
        .collect::<Vec<_>>();
    let results = jobs
        .into_iter()
        .map(|job| job.join().unwrap())
        .collect::<Vec<_>>();
    assert_eq!(results.iter().filter(|result| result.is_ok()).count(), 1);
}

/// The current writer can retire an unreleased capability once per attempt;
/// a late local owner write with the old capability is rejected.
#[test]
fn current_writer_retake_fences_late_local_owner_and_is_bounded() {
    let mut f = Fixture::new();
    f.with_unreleased_capability();
    let me = process::inspect(std::process::id() as i32).unwrap();
    let owner = json!({"pid":me.pid,"token":me.token,"birth":me.birth}).to_string();
    f.store
        .conn
        .execute(
            "UPDATE agents SET startup_owner_pid_identity=?,startup_owner_birth_time=? WHERE id=?",
            rusqlite::params![owner, me.birth, f.run.as_str()],
        )
        .unwrap();
    f.op.expected_startup_owner = owner;
    f.op.expected_startup_birth_time = me.birth;
    f.op.owner_state = ProcessState::Alive;
    f.op.retake_current_writer = Some(true);
    f.op.expected_current_retake_count = Some(0);
    let old = f.op.expected_capability_sha256.clone().unwrap();
    assert!(
        f.store
            .claim_maintenance_prepared(&f.op)
            .unwrap()
            .launch_authorized
    );
    let child = json!({"pid":123,"token":"linux:fixture","birth":1});
    assert!(matches!(
        f.store.record_local_startup_owner(&f.run, &child, &old),
        Err(agent_run_domain::Error::Conflict)
    ));
    f.op.expected_capability_sha256 = None;
    assert!(f.store.claim_maintenance_prepared(&f.op).is_err());
    let second = "b".repeat(64);
    f.store.record_supervisor_launch(&f.run, &second).unwrap();
    f.op.expected_capability_sha256 = Some(second);
    f.op.expected_current_retake_count = Some(1);
    assert!(
        f.store
            .claim_maintenance_prepared(&f.op)
            .unwrap()
            .launch_authorized
    );
    let third = "c".repeat(64);
    f.store.record_supervisor_launch(&f.run, &third).unwrap();
    f.op.expected_capability_sha256 = Some(third.clone());
    f.op.expected_current_retake_count = Some(2);
    assert!(
        !f.store
            .claim_maintenance_prepared(&f.op)
            .unwrap()
            .launch_authorized
    );
    assert_eq!(
        f.store.get(&f.run).unwrap().status,
        agent_run_domain::domain::Status::Failed
    );
    assert!(matches!(
        f.store.record_local_startup_owner(&f.run, &child, &third),
        Err(agent_run_domain::Error::Conflict)
    ));
}

/// An empty-capability recovery uses the durable retake count as its CAS.
#[test]
fn current_writer_empty_capability_has_one_concurrent_retake() {
    let mut f = Fixture::new();
    f.with_unreleased_capability();
    let me = process::inspect(std::process::id() as i32).unwrap();
    let owner = json!({"pid":me.pid,"token":me.token,"birth":me.birth}).to_string();
    f.store.conn.execute("UPDATE agents SET startup_owner_pid_identity=?,startup_owner_birth_time=?,supervisor_capability_sha256=NULL WHERE id=?",rusqlite::params![owner,me.birth,f.run.as_str()]).unwrap();
    f.op.expected_startup_owner = owner;
    f.op.expected_startup_birth_time = me.birth;
    f.op.expected_capability_sha256 = None;
    f.op.retake_current_writer = Some(true);
    f.op.expected_current_retake_count = Some(0);
    f.op.owner_state = ProcessState::Alive;
    let barrier = Arc::new(Barrier::new(2));
    let jobs = (0..2)
        .map(|_| {
            let path = f.home.path.clone();
            let op = f.op.clone();
            let barrier = barrier.clone();
            thread::spawn(move || {
                let mut store = agent_run_store::fixture::open(&path).unwrap();
                barrier.wait();
                store.claim_maintenance_prepared(&op)
            })
        })
        .collect::<Vec<_>>();
    let results = jobs
        .into_iter()
        .map(|job| job.join().unwrap())
        .collect::<Vec<_>>();
    assert_eq!(results.iter().filter(|result| result.is_ok()).count(), 1);
}

/// Changed capabilities, child owners, discovery and cancellation stay held.
#[test]
fn unreleased_broker_capability_requires_exact_unstarted_evidence() {
    for case in ["capability", "child", "discovery", "confirmed", "cancel"] {
        let mut f = Fixture::new();
        f.with_unreleased_capability();
        match case {
            "capability" => f.op.expected_capability_sha256 = Some("b".repeat(64)),
            "child" => {
                let mut owner: serde_json::Value =
                    serde_json::from_str(&f.op.expected_startup_owner).unwrap();
                owner["group"] = json!(123);
                f.op.expected_startup_owner = owner.to_string();
                f.store
                    .conn
                    .execute(
                        "UPDATE agents SET startup_owner_pid_identity=? WHERE id=?",
                        rusqlite::params![f.op.expected_startup_owner, f.run.as_str()],
                    )
                    .unwrap();
            }
            "discovery" | "confirmed" => {
                let kind = if case == "discovery" {
                    "maintenance_discovery_started_v1"
                } else {
                    "maintenance_resume_confirmed_v1"
                };
                f.store.conn.execute("INSERT INTO events(agent_id,attempt_id,at,kind,data_json) VALUES(?,'target',5,?,'{}')",rusqlite::params![f.run.as_str(),kind]).unwrap();
            }
            _ => {
                f.store.enqueue(&f.run, "cancel", &json!({})).unwrap();
            }
        }
        assert!(f.store.claim_maintenance_prepared(&f.op).is_err(), "{case}");
    }
}

/// Any prior capability/owner/process/spawn evidence or explicit cancellation
/// makes the outcome uncertain and forbids reclaiming a prepared-looking row.
#[test]
fn prepared_maintenance_refuses_ambiguous_launch_and_live_owner() {
    for sql in [
        "UPDATE agents SET supervisor_capability_sha256='aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa'",
        "UPDATE agents SET supervisor_pid=123",
        "UPDATE attempts SET process_identity='possible-process' WHERE id='target'",
        "UPDATE attempts SET phase='spawning' WHERE id='target'",
        "UPDATE attempts SET cleanup_proof_json='{}' WHERE id='target'",
    ] {
        let mut f = Fixture::new();
        let before = f.counts();
        f.store.conn.execute(sql, []).unwrap();
        assert!(f.store.claim_maintenance_prepared(&f.op).is_err(), "{sql}");
        assert_eq!(f.counts(), before);
    }
    for state in [
        ProcessState::Alive,
        ProcessState::Unknown,
        ProcessState::Denied,
        ProcessState::NotStarted,
    ] {
        let mut f = Fixture::new();
        f.op.owner_state = state;
        assert!(f.store.claim_maintenance_prepared(&f.op).is_err());
    }
    let mut f = Fixture::new();
    f.store.enqueue(&f.run, "cancel", &json!({})).unwrap();
    assert!(f.store.claim_maintenance_prepared(&f.op).is_err());
}

/// Competing recoverers receive at most one fresh ownership grant.
#[test]
fn prepared_maintenance_concurrent_recovery_has_one_winner() {
    let f = Fixture::new();
    let before = f.counts();
    let barrier = Arc::new(Barrier::new(2));
    let jobs = (0..2)
        .map(|_| {
            let path = f.home.path.clone();
            let op = f.op.clone();
            let barrier = barrier.clone();
            thread::spawn(move || {
                let mut store = agent_run_store::fixture::open(&path).unwrap();
                barrier.wait();
                store.claim_maintenance_prepared(&op)
            })
        })
        .collect::<Vec<_>>();
    let results = jobs
        .into_iter()
        .map(|job| job.join().unwrap())
        .collect::<Vec<_>>();
    assert_eq!(results.iter().filter(|result| result.is_ok()).count(), 1);
    assert_eq!(f.counts(), before);
}

/// The same writer envelope retains its identity but its replay cannot grant
/// another dispatch; a rolled-back outer transaction transfers no ownership.
#[test]
fn prepared_maintenance_writer_replay_disarms_and_rollback_preserves_owner() {
    let mut f = Fixture::new();
    assert!(
        serde_json::to_value(&f.op)
            .unwrap()
            .get("expected_capability_sha256")
            .is_none()
    );
    let legacy = serde_json::to_value(&f.op).unwrap();
    assert!(legacy.get("retake_current_writer").is_none());
    assert!(legacy.get("expected_current_retake_count").is_none());
    f.store.conn.execute_batch("BEGIN IMMEDIATE").unwrap();
    f.store.claim_maintenance_prepared(&f.op).unwrap();
    f.store.conn.execute_batch("ROLLBACK").unwrap();
    let owner: String = f
        .store
        .conn
        .query_row(
            "SELECT startup_owner_pid_identity FROM agents WHERE id=?",
            [f.run.as_str()],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(owner, f.op.expected_startup_owner);
    let mut writer = StoreWriter::open(WriterLease::acquire_online(&f.home.path).unwrap()).unwrap();
    let (boot, now) = agent_run_platform::monotonic::now().unwrap();
    let envelope = LocalEnvelope::new(
        OperationId::new(&boot, now),
        ProducerId {
            kind: ProducerKind::Broker,
            scope: Some("prepared".into()),
            instance: "fixture".into(),
        },
        1,
        MonoDeadline {
            boot_id: boot,
            ns: now + 60_000_000_000,
        },
        LocalOp::from(f.op.clone()),
    )
    .unwrap();
    let SubmitResult::Committed(first) = writer.apply_local(&WriteAuthority::Broker, &envelope)
    else {
        panic!("fresh ownership claim must commit")
    };
    assert!(
        PreparedMaintenanceClaim::from_dispatch_receipt(&first)
            .unwrap()
            .launch_authorized
    );
    let SubmitResult::Committed(replay) = writer.apply_local(&WriteAuthority::Broker, &envelope)
    else {
        panic!("exact envelope must replay")
    };
    assert!(replay.replayed);
    assert!(
        !PreparedMaintenanceClaim::from_dispatch_receipt(&replay)
            .unwrap()
            .launch_authorized
    );
    let mut invalid = first;
    invalid.result["dispatch_writer_epoch"] = json!(-1);
    assert!(PreparedMaintenanceClaim::from_dispatch_receipt(&invalid).is_err());
}
