//! Synthetic supervisor crashes and cancellation races; no native process starts.
mod common;

use agent_run_domain::{
    domain::Status,
    storage::{MonoDeadline, OperationId, ProducerId, ProducerKind, SubmitResult, WriteAuthority},
};
use agent_run_platform::process::{self, ProcessState};
use agent_run_store::{
    Store,
    ops::{CancelPreparedMaintenance, ExpectedOwnership, LocalEnvelope, LocalOp},
    writer::{StoreWriter, WriterLease},
};
use serde_json::{Value, json};
use std::{
    os::unix::process::CommandExt,
    process::{Child, Command, Stdio},
    sync::{Arc, Barrier},
    thread,
};

/// Harmless native-account-independent fixture child, always killed and reaped.
struct StartupChild {
    child: Child,
    identity: process::Identity,
}

impl StartupChild {
    /// Keep cat idle on a pipe in its own process group while identity is read.
    fn new() -> Self {
        let child = Command::new("/bin/cat")
            .stdin(Stdio::piped())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .process_group(0)
            .spawn()
            .unwrap();
        let identity = process::inspect(child.id() as i32).unwrap();
        assert_eq!(identity.group, identity.pid);
        Self { child, identity }
    }

    /// Establish actual process death before preparing a kernel observation.
    fn stop(&mut self) {
        let _ = self.child.kill();
        self.child.wait().unwrap();
    }
}

impl Drop for StartupChild {
    /// A failed assertion must not abandon the fixture process.
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

/// Retained allocation after bootstrap committed, before native spawning.
struct Fixture {
    home: common::Home,
    store: Store<'static>,
    op: CancelPreparedMaintenance,
}

impl Fixture {
    /// Observe a real reused generation while retaining entirely synthetic work.
    fn new() -> Self {
        let home = common::Home::new();
        let mut store = home.store();
        let identity = json!({"provider_identity_version":2,"authority":{"harness":"codex"}});
        let (run, _) = store
            .admit(&home.request(), &home.config, &identity, None)
            .unwrap();
        let epoch = store
            .begin_maintenance_fence("cancel-target", 1.0)
            .unwrap()
            .epoch;
        store.defer_maintenance_fence(epoch).unwrap();
        store.conn.execute("INSERT INTO orchestrator_sessions(id,transport,external_session_id,created_at,last_seen_at) VALUES('owner','codex','fixture-owner',1,1)", []).unwrap();
        let me = process::inspect(std::process::id() as i32).unwrap();
        let token = format!("{}-old-generation", me.token);
        let birth = me.birth - 1.0;
        store.conn.execute("UPDATE agents SET orchestrator_session_id='owner',runtime_session_id='session',supervisor_pid=?,supervisor_identity=?,supervisor_birth_time=?,supervisor_capability_sha256=? WHERE id=?",rusqlite::params![me.pid,token,birth,"a".repeat(64),run.as_str()]).unwrap();
        store.conn.execute("INSERT INTO attempts(id,agent_id,number,state,phase,adapter_state_json,created_at,finished_at,selected_account_id,ownership_active) VALUES('source',?,1,'maintenance_paused','cleanup_complete','{}',1,2,'account',0)",[run.as_str()]).unwrap();
        store.conn.execute("INSERT INTO attempts(id,agent_id,number,state,phase,adapter_state_json,created_at,selected_account_id,ownership_active) VALUES('target',?,2,'prepared','prepared','{}',2,'account',1)",[run.as_str()]).unwrap();
        store.conn.execute("INSERT INTO maintenance_resumes(epoch,agent_id,request_id,source_attempt_id,target_attempt_id,target_number,account_id,source_proof_sha256,writer_epoch,claimed_at,preflight_json) VALUES(?,?,'allocation','source','target',2,'account',?,1,2,'{}')",rusqlite::params![epoch,run.as_str(),"b".repeat(64)]).unwrap();
        store.conn.execute("INSERT INTO attempt_quota_keys(attempt_id,quota_key) VALUES('target','account::tokens')",[]).unwrap();
        store.enqueue(&run, "cancel", &json!({})).unwrap();
        let cancel_command_id = store
            .conn
            .query_row(
                "SELECT id FROM commands WHERE agent_id=? AND kind='cancel'",
                [run.as_str()],
                |r| r.get(0),
            )
            .unwrap();
        let state = process::observe(Some(me.pid), Some(&token), Some(birth));
        assert_eq!(state, ProcessState::Reused);
        let (startup_owner, startup_birth_time): (Option<String>, Option<f64>) = store
            .conn
            .query_row(
                "SELECT startup_owner_pid_identity,startup_owner_birth_time FROM agents WHERE id=?",
                [run.as_str()],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .unwrap();
        let op = CancelPreparedMaintenance {
            epoch,
            execution: run.clone(),
            target_attempt_id: "target".into(),
            cancel_command_id,
            expected_owner: ExpectedOwnership {
                execution: run,
                supervisor_pid: Some(me.pid),
                supervisor_identity: Some(token),
                supervisor_birth_time: Some(birth),
                process_group_id: None,
                startup_owner,
                startup_birth_time,
            },
            supervisor_state: state,
            expected_capability_sha256: None,
            cancel_current_broker: None,
            at: 3.0,
        };
        let f = Self { home, store, op };
        f.marker("maintenance_preparation_guard_v1", f.binding());
        f
    }

    /// Exact correlation shared by all three preparation markers.
    fn binding(&self) -> Value {
        json!({"epoch":self.op.epoch,"source_attempt_id":"source","target_attempt_id":"target"})
    }

    /// Append synthetic supervisor evidence without any native action.
    fn marker(&self, kind: &str, data: Value) {
        self.store.conn.execute("INSERT INTO events(agent_id,attempt_id,at,kind,data_json) VALUES(?,'target',2,?,?)",rusqlite::params![self.op.execution.as_str(),kind,data.to_string()]).unwrap();
    }

    /// Count public terminal notices and commits, excluding maintenance notices.
    fn terminal_counts(&self) -> (i64, i64) {
        (
            self.store
                .conn
                .query_row("SELECT COUNT(*) FROM deliveries", [], |r| r.get(0))
                .unwrap(),
            self.store
                .conn
                .query_row(
                    "SELECT COUNT(*) FROM events WHERE to_status='cancelled'",
                    [],
                    |r| r.get(0),
                )
                .unwrap(),
        )
    }

    /// Replace the registered supervisor with the exact full pre-READY child.
    /// Keep the separate birth column deliberately different, as local launch
    /// may retain the initial broker's birth there.
    fn startup_owner(&mut self, owner: &Value) {
        let raw = owner.to_string();
        self.store.conn.execute("UPDATE agents SET supervisor_pid=NULL,supervisor_identity=NULL,supervisor_birth_time=NULL,startup_owner_pid_identity=?,startup_owner_birth_time=123 WHERE id=?", rusqlite::params![raw,self.op.execution.as_str()]).unwrap();
        self.op.expected_owner.supervisor_pid = None;
        self.op.expected_owner.supervisor_identity = None;
        self.op.expected_owner.supervisor_birth_time = None;
        self.op.expected_owner.startup_owner = Some(raw);
        self.op.expected_owner.startup_birth_time = Some(123.0);
    }
}

/// Bootstrap failure before owner registration can cancel using actual child
/// death even when the separate startup-birth column still names the broker.
#[test]
fn dead_full_startup_child_cancels_before_supervisor_registration() {
    let mut child = StartupChild::new();
    child.stop();
    let mut f = Fixture::new();
    f.startup_owner(&serde_json::to_value(&child.identity).unwrap());
    f.op.supervisor_state = process::observe(
        Some(child.identity.pid),
        Some(&child.identity.token),
        Some(child.identity.birth),
    );
    assert_eq!(f.op.supervisor_state, ProcessState::Dead);
    assert!(f.store.cancel_prepared_maintenance(&f.op).unwrap());
    assert!(!f.store.cancel_prepared_maintenance(&f.op).unwrap());
    assert_eq!(f.terminal_counts(), (1, 1));
}

/// A live generation refuses, while a PID genuinely occupied by a different
/// recorded generation safely proves the old startup owner has gone.
#[test]
fn full_startup_child_rejects_alive_and_accepts_reused_generation() {
    let child = StartupChild::new();
    let mut f = Fixture::new();
    f.startup_owner(&serde_json::to_value(&child.identity).unwrap());
    f.op.supervisor_state = process::observe(
        Some(child.identity.pid),
        Some(&child.identity.token),
        Some(child.identity.birth),
    );
    assert_eq!(f.op.supervisor_state, ProcessState::Alive);
    assert!(f.store.cancel_prepared_maintenance(&f.op).is_err());
    let mut prior = child.identity.clone();
    prior.token.push_str("-old-generation");
    prior.birth -= 1.0;
    f.startup_owner(&serde_json::to_value(&prior).unwrap());
    f.op.supervisor_state =
        process::observe(Some(prior.pid), Some(&prior.token), Some(prior.birth));
    assert_eq!(f.op.supervisor_state, ProcessState::Reused);
    assert!(f.store.cancel_prepared_maintenance(&f.op).unwrap());
    assert_eq!(f.terminal_counts(), (1, 1));
}

/// Initial broker documents, incomplete children and changed ownership never
/// inherit cancellation authority from an earlier dead-child observation.
#[test]
fn startup_cancel_refuses_incomplete_owner_and_stale_snapshot() {
    let mut child = StartupChild::new();
    child.stop();
    let observed = process::observe(
        Some(child.identity.pid),
        Some(&child.identity.token),
        Some(child.identity.birth),
    );
    assert_eq!(observed, ProcessState::Dead);
    let full = serde_json::to_value(&child.identity).unwrap();
    for case in [
        "broker",
        "missing_group",
        "foreign_group",
        "changed_document",
        "changed_birth",
        "registered_owner",
    ] {
        let mut f = Fixture::new();
        let mut owner = full.clone();
        match case {
            "broker" => {
                owner = json!({"pid":child.identity.pid,"token":child.identity.token,"birth":child.identity.birth})
            }
            "missing_group" => {
                owner.as_object_mut().unwrap().remove("group");
            }
            "foreign_group" => owner["group"] = json!(child.identity.pid + 1),
            _ => {}
        }
        f.startup_owner(&owner);
        f.op.supervisor_state = observed;
        match case {
            "changed_document" => {
                f.store.conn.execute("UPDATE agents SET startup_owner_pid_identity=json_set(startup_owner_pid_identity,'$.ppid',1)", []).unwrap();
            }
            "changed_birth" => {
                f.store
                    .conn
                    .execute("UPDATE agents SET startup_owner_birth_time=124", [])
                    .unwrap();
            }
            "registered_owner" => {
                f.store
                    .conn
                    .execute("UPDATE agents SET supervisor_pid=1234", [])
                    .unwrap();
            }
            _ => {}
        }
        assert!(
            f.store.cancel_prepared_maintenance(&f.op).is_err(),
            "{case}"
        );
        assert_eq!(f.terminal_counts(), (0, 0), "{case}");
    }
}

/// An explicit cancel may close an unregistered target owned by a proven-dead
/// broker, with or without an unreleased capability.
#[test]
fn dead_broker_prepared_target_cancels_without_launch() {
    for capability in [None, Some("a".repeat(64))] {
        let mut f = Fixture::new();
        let me = process::inspect(std::process::id() as i32).unwrap();
        let old_token = format!("{}-old", me.token);
        let old_birth = me.birth - 1.0;
        let owner = json!({"pid":me.pid,"token":old_token,"birth":old_birth});
        f.startup_owner(&owner);
        f.store.conn.execute(
            "UPDATE agents SET startup_owner_birth_time=?,supervisor_capability_sha256=? WHERE id=?",
            rusqlite::params![old_birth,capability,f.op.execution.as_str()],
        ).unwrap();
        f.op.expected_owner.startup_birth_time = Some(old_birth);
        f.op.supervisor_state = process::observe(Some(me.pid), Some(&old_token), Some(old_birth));
        f.store.conn.execute(
            "INSERT INTO writer_authority(id,epoch,holder_pid,holder_birth,holder_token,acquired_at,probe_at) VALUES(1,2,?,?,?,1,1)",
            rusqlite::params![me.pid,me.birth,me.token],
        ).unwrap();
        assert!(f.store.cancel_prepared_maintenance(&f.op).unwrap());
        assert_eq!(
            f.store.get(&f.op.execution).unwrap().status,
            Status::Cancelled
        );
        assert!(!f.store.cancel_prepared_maintenance(&f.op).unwrap());
        f.store.end_maintenance_fence(f.op.epoch, 4.0).unwrap();
        f.store
            .conn
            .execute(
                "UPDATE attempts SET cleanup_proof_json='{\"confirmed\":true}' WHERE id='source'",
                [],
            )
            .unwrap();
        f.store
            .conn
            .execute(
                "UPDATE attempts SET cleanup_proof_json=NULL WHERE id='target'",
                [],
            )
            .unwrap();
        let parent = f.store.get(&f.op.execution).unwrap();
        assert!(
            f.store
                .admit(&f.home.request(), &f.home.config, &json!({}), Some(&parent))
                .is_err()
        );
        f.store.conn.execute("UPDATE attempts SET cleanup_proof_json='{\"never_spawned\":true}' WHERE id='target'",[]).unwrap();
        assert!(
            f.store
                .admit(&f.home.request(), &f.home.config, &json!({}), Some(&parent))
                .is_ok()
        );
    }
}

/// An explicit cancel can fence an unreleased launch owned by the current
/// broker; a late local child cannot record ownership or start afterwards.
#[test]
fn current_broker_prepared_target_cancels_and_fences_late_launch() {
    for capability in [None, Some("a".repeat(64))] {
        let mut f = Fixture::new();
        let me = process::inspect(std::process::id() as i32).unwrap();
        let owner = json!({"pid":me.pid,"token":me.token,"birth":me.birth});
        f.startup_owner(&owner);
        f.store.conn.execute(
            "UPDATE agents SET startup_owner_birth_time=?,supervisor_capability_sha256=? WHERE id=?",
            rusqlite::params![me.birth,capability,f.op.execution.as_str()],
        ).unwrap();
        f.op.expected_owner.startup_birth_time = Some(me.birth);
        f.op.supervisor_state = ProcessState::Alive;
        f.op.cancel_current_broker = Some(true);
        f.op.expected_capability_sha256 = capability.clone();
        f.store.conn.execute(
            "INSERT INTO writer_authority(id,epoch,holder_pid,holder_birth,holder_token,acquired_at,probe_at) VALUES(1,2,?,?,?,1,1)",
            rusqlite::params![me.pid,me.birth,me.token],
        ).unwrap();
        assert!(f.store.cancel_prepared_maintenance(&f.op).unwrap());
        assert_eq!(
            f.store.get(&f.op.execution).unwrap().status,
            Status::Cancelled
        );
        if let Some(old) = capability {
            let child = json!({"pid":123,"token":"linux:fixture","birth":1});
            assert!(matches!(
                f.store
                    .record_local_startup_owner(&f.op.execution, &child, &old),
                Err(agent_run_domain::Error::Conflict)
            ));
        }
        assert!(
            f.store
                .record_supervisor_launch(&f.op.execution, &"b".repeat(64))
                .is_err()
        );
        f.store.end_maintenance_fence(f.op.epoch, 4.0).unwrap();
        f.store
            .conn
            .execute(
                "UPDATE attempts SET cleanup_proof_json='{\"confirmed\":true}' WHERE id='source'",
                [],
            )
            .unwrap();
        let parent = f.store.get(&f.op.execution).unwrap();
        let marker:String=f.store.conn.query_row("SELECT data_json FROM events WHERE agent_id=? AND kind='maintenance_current_broker_cancelled_v1'",[f.op.execution.as_str()],|row|row.get(0)).unwrap();
        f.store.conn.execute("DELETE FROM events WHERE agent_id=? AND kind='maintenance_current_broker_cancelled_v1'",[f.op.execution.as_str()]).unwrap();
        assert!(
            f.store
                .admit(&f.home.request(), &f.home.config, &json!({}), Some(&parent))
                .is_err()
        );
        f.store.conn.execute("INSERT INTO events(agent_id,attempt_id,at,kind,data_json) VALUES(?,'target',4,'maintenance_current_broker_cancelled_v1',?)",rusqlite::params![f.op.execution.as_str(),marker]).unwrap();
        assert!(
            f.store
                .admit(&f.home.request(), &f.home.config, &json!({}), Some(&parent))
                .is_ok()
        );
    }
}

/// Broker-owned cancellation refuses incomplete discovery or owner evidence.
#[test]
fn broker_prepared_cancel_refuses_discovery_and_live_owner() {
    for case in ["discovery", "live", "current_writer", "incomplete_writer"] {
        let mut f = Fixture::new();
        let me = process::inspect(std::process::id() as i32).unwrap();
        let old_token = if case == "current_writer" {
            me.token.clone()
        } else {
            format!("{}-old", me.token)
        };
        let old_birth = if case == "current_writer" {
            me.birth
        } else {
            me.birth - 1.0
        };
        let owner = json!({"pid":me.pid,"token":old_token,"birth":old_birth});
        f.startup_owner(&owner);
        f.store
            .conn
            .execute(
                "UPDATE agents SET startup_owner_birth_time=? WHERE id=?",
                rusqlite::params![old_birth, f.op.execution.as_str()],
            )
            .unwrap();
        f.op.expected_owner.startup_birth_time = Some(old_birth);
        f.op.supervisor_state = if case == "live" {
            ProcessState::Alive
        } else {
            ProcessState::Reused
        };
        f.store.conn.execute("INSERT INTO writer_authority(id,epoch,holder_pid,holder_birth,holder_token,acquired_at,probe_at) VALUES(1,2,?,?,?,1,1)",rusqlite::params![me.pid,me.birth,me.token]).unwrap();
        if case == "incomplete_writer" {
            f.store
                .conn
                .execute("UPDATE writer_authority SET holder_token=NULL", [])
                .unwrap();
        }
        if case == "discovery" {
            f.marker("maintenance_discovery_started_v1", f.binding());
        }
        assert!(
            f.store.cancel_prepared_maintenance(&f.op).is_err(),
            "{case}"
        );
    }
}

/// A pre-registration child cannot borrow discovery evidence from another
/// execution, even when those markers look settled.
#[test]
fn unregistered_child_cancel_refuses_discovery_markers() {
    let mut child = StartupChild::new();
    child.stop();
    let mut f = Fixture::new();
    f.startup_owner(&serde_json::to_value(&child.identity).unwrap());
    f.op.supervisor_state = process::observe(
        Some(child.identity.pid),
        Some(&child.identity.token),
        Some(child.identity.birth),
    );
    f.store.conn.execute("UPDATE agents SET identity_json=json_set(identity_json,'$.authority.harness','claude-code')",[]).unwrap();
    f.marker("maintenance_discovery_started_v1", f.binding());
    f.marker("maintenance_discovery_settled_v1", f.binding());
    assert!(f.store.cancel_prepared_maintenance(&f.op).is_err());
}

/// A crash before native spawning remains cancellable without allocating a new
/// attempt/account, and committed cancellation permits releasing the fence.
#[test]
fn dead_prepared_target_cancels_once_and_releases_fence() {
    let mut f = Fixture::new();
    assert!(
        agent_run_store::maintenance_target_cancel::preparation_is_settled(
            &f.store.conn,
            &f.op.execution,
            &f.op.target_attempt_id
        )
        .unwrap()
    );
    assert!(f.store.cancel_prepared_maintenance(&f.op).unwrap());
    assert!(!f.store.cancel_prepared_maintenance(&f.op).unwrap());
    assert_eq!(
        f.store.get(&f.op.execution).unwrap().status,
        Status::Cancelled
    );
    assert_eq!(f.terminal_counts(), (1, 1));
    let target: (String,i64,String) = f.store.conn.query_row("SELECT selected_account_id,ownership_active,cleanup_proof_json FROM attempts WHERE id='target'", [], |r| Ok((r.get(0)?,r.get(1)?,r.get(2)?))).unwrap();
    assert_eq!(
        target,
        ("account".into(), 0, "{\"never_spawned\":true}".into())
    );
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
                "SELECT state FROM commands WHERE id=?",
                [f.op.cancel_command_id],
                |r| r.get::<_, String>(0)
            )
            .unwrap(),
        "completed"
    );
    f.store
        .end_maintenance_fence(f.op.epoch, agent_run_domain::domain::now())
        .unwrap();
}

/// A cancelled prepared target remains resumable after the exact dead owner
/// and all source/target cleanup proofs are verified.
#[test]
fn cancelled_prepared_target_can_resume_after_cleanup() {
    let mut f = Fixture::new();
    assert!(f.store.cancel_prepared_maintenance(&f.op).unwrap());
    f.store.end_maintenance_fence(f.op.epoch, 4.0).unwrap();
    let parent = f.store.get(&f.op.execution).unwrap();
    assert!(
        f.store
            .admit(&f.home.request(), &f.home.config, &json!({}), Some(&parent))
            .is_err()
    );
    f.store
        .conn
        .execute(
            "UPDATE attempts SET cleanup_proof_json='{\"confirmed\":true}' WHERE id='source'",
            [],
        )
        .unwrap();
    assert!(
        f.store
            .admit(&f.home.request(), &f.home.config, &json!({}), Some(&parent))
            .is_ok()
    );
}

/// Any current owner change, native evidence or command/allocation change wins
/// over a previously captured dead-supervisor observation.
#[test]
fn prepared_target_cancel_rechecks_every_racing_boundary() {
    for sql in [
        "UPDATE agents SET supervisor_birth_time=supervisor_birth_time+1",
        "UPDATE agents SET startup_owner_birth_time=1",
        "UPDATE agents SET process_group_id=1234",
        "UPDATE attempts SET phase='spawning' WHERE id='target'",
        "UPDATE attempts SET process_identity='possible-native' WHERE id='target'",
        "UPDATE attempts SET process_birth_time=2 WHERE id='target'",
        "UPDATE attempts SET cleanup_proof_json='{}' WHERE id='target'",
        "UPDATE attempts SET number=3 WHERE id='target'",
        "UPDATE agents SET sequence=sequence+1",
        "UPDATE commands SET state='completed'",
        "UPDATE maintenance_fences SET ended_at=3,phase='complete',outcome='deferred'",
        "INSERT INTO process_ownership(owner_kind,owner_id,leader_json,descendants_observed,updated_at) VALUES('attempt','target','{}',0,2)",
    ] {
        let mut f = Fixture::new();
        f.store.conn.execute(sql, []).unwrap();
        assert!(f.store.cancel_prepared_maintenance(&f.op).is_err(), "{sql}");
        assert_eq!(f.terminal_counts(), (0, 0), "{sql}");
    }
    for state in [
        ProcessState::Alive,
        ProcessState::Denied,
        ProcessState::Unknown,
        ProcessState::NotStarted,
    ] {
        let mut f = Fixture::new();
        f.op.supervisor_state = state;
        assert!(f.store.cancel_prepared_maintenance(&f.op).is_err());
    }
    let mut f = Fixture::new();
    f.op.target_attempt_id = "source".into();
    assert!(f.store.cancel_prepared_maintenance(&f.op).is_err());
    let mut f = Fixture::new();
    f.marker("maintenance_resume_confirmed_v1", f.binding());
    assert!(f.store.cancel_prepared_maintenance(&f.op).is_err());
}

/// Missing or unknown discovery evidence retains the reservation. Only a
/// correlated successful Claude settlement can close its start marker.
#[test]
fn prepared_target_discovery_requires_exact_ordered_settlement() {
    for case in [
        "missing_guard",
        "started",
        "duplicate_guard",
        "wrong_binding",
        "settled_without_start",
        "ownership_without_start",
        "wrong_order",
        "valid_claude",
    ] {
        let mut f = Fixture::new();
        f.store.conn.execute("UPDATE agents SET identity_json=json_set(identity_json,'$.authority.harness','claude-code')", []).unwrap();
        match case {
            "missing_guard" => {
                f.store
                    .conn
                    .execute(
                        "DELETE FROM events WHERE kind='maintenance_preparation_guard_v1'",
                        [],
                    )
                    .unwrap();
            }
            "duplicate_guard" => f.marker("maintenance_preparation_guard_v1", f.binding()),
            "settled_without_start" => f.marker("maintenance_discovery_settled_v1", f.binding()),
            "ownership_without_start" => f.marker("mcp_discovery_ownership", json!({})),
            "wrong_order" => {
                f.marker("maintenance_discovery_settled_v1", f.binding());
                f.marker("maintenance_discovery_started_v1", f.binding());
            }
            _ => {
                f.marker("maintenance_discovery_started_v1", f.binding());
                if case != "started" {
                    let mut binding = f.binding();
                    if case == "wrong_binding" {
                        binding["source_attempt_id"] = json!("other");
                    }
                    f.marker("maintenance_discovery_settled_v1", binding);
                }
            }
        }
        let outcome = f.store.cancel_prepared_maintenance(&f.op);
        assert_eq!(outcome.is_ok(), case == "valid_claude", "{case}");
    }
}

/// Competing broker sweeps serialize into one terminal commit and outbox row.
#[test]
fn concurrent_prepared_target_cancels_have_one_effect() {
    let f = Fixture::new();
    let barrier = Arc::new(Barrier::new(2));
    let jobs = (0..2)
        .map(|_| {
            let path = f.home.path.clone();
            let op = f.op.clone();
            let barrier = barrier.clone();
            thread::spawn(move || {
                let mut store = agent_run_store::fixture::open(&path).unwrap();
                barrier.wait();
                store.cancel_prepared_maintenance(&op).unwrap()
            })
        })
        .collect::<Vec<_>>();
    assert_eq!(
        jobs.into_iter()
            .map(|job| job.join().unwrap())
            .filter(|changed| *changed)
            .count(),
        1
    );
    assert_eq!(f.terminal_counts(), (1, 1));
}

/// Dropped commit replies replay their receipt; an outer rollback leaves no
/// partial status, reservation release, cancel completion or notice behind.
#[test]
fn prepared_target_cancel_writer_replay_and_rollback_are_atomic() {
    let mut f = Fixture::new();
    f.store
        .conn
        .execute("UPDATE commands SET state='claimed',claimed_at=2", [])
        .unwrap();
    f.store.conn.execute_batch("BEGIN IMMEDIATE").unwrap();
    f.store.cancel_prepared_maintenance(&f.op).unwrap();
    f.store.conn.execute_batch("ROLLBACK").unwrap();
    assert_eq!(
        f.store.get(&f.op.execution).unwrap().status,
        Status::Starting
    );
    assert_eq!(f.terminal_counts(), (0, 0));
    let mut writer = StoreWriter::open(WriterLease::acquire_online(&f.home.path).unwrap()).unwrap();
    let (boot, now) = agent_run_platform::monotonic::now().unwrap();
    let envelope = LocalEnvelope::new(
        OperationId::new(&boot, now),
        ProducerId {
            kind: ProducerKind::Broker,
            scope: Some("cancel-target".into()),
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
        panic!("cancel must commit");
    };
    assert!(first.output::<bool>().unwrap());
    let SubmitResult::Committed(replay) = writer.apply_local(&WriteAuthority::Broker, &envelope)
    else {
        panic!("cancel must replay");
    };
    assert!(replay.replayed);
    assert_eq!(f.terminal_counts(), (1, 1));
}
