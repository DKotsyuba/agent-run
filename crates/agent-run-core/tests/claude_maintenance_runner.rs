//! Fake Claude foreground turns through the real runner and in-process writer.
//! No provider, network, credentials, or service restart is used.
mod common;
#[cfg(feature = "test-fixtures")]
#[path = "common/maintenance_resume_fixture.rs"]
mod maintenance_resume_fixture;

use agent_run_adapters::{LaunchPlan, io::Process};
use agent_run_core::{domain::Status, maintenance_execution::EngineExit, stream};
use agent_run_domain::{
    storage::WriteAuthority,
    worker::{
        FinishGuard, MaintenancePauseRequest, WorkerCatalogProof, maintenance_pause_catalog_digest,
    },
};
use agent_run_store::ops::{AcceptMaintenanceCheckpoint, EnqueueCommand, RequestMaintenanceSteer};
use serde_json::json;
use std::{collections::BTreeMap, time::Duration};

const ATTEMPT: &str = "claude-maintenance-fixture";
const TOKEN: &str = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
const CHALLENGE: &str = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
const SESSION: &str = "fixture-session";
const CHECKPOINT: &str = "Checked the current work; continue with the remaining review.";

/// One fake native process records inputs and emits one correlated tool turn.
const FAKE_CLAUDE: &str = r#"
import json, os, sys, time
session = 'fixture-session'
challenge = 'b' * 64
checkpoint = 'Checked the current work; continue with the remaining review.'
mode = sys.argv[1]
buffered = mode == 'buffered'
def emit(frame):
    sys.stdout.write(json.dumps(frame, separators=(',', ':')) + '\n')
    sys.stdout.flush()
def read_input():
    line = sys.stdin.readline()
    if not line: return None
    with open('received.jsonl', 'a', encoding='utf-8') as log:
        log.write(line)
        log.flush()
    return json.loads(line)
initial = read_input()
if initial is None: sys.exit(1)
if mode.startswith('resume_lifecycle'):
    lifecycle = {'type':'command_lifecycle','command_uuid':initial['uuid'],'state':'queued',
                 'uuid':'33333333-3333-4333-8333-333333333333','session_id':session}
    if mode == 'resume_lifecycle_foreign': lifecycle['command_uuid'] = '44444444-4444-4444-8444-444444444444'
    if mode == 'resume_lifecycle_cancelled': lifecycle['state'] = 'cancelled'
    emit(lifecycle)
init = {'type':'system','subtype':'init','session_id':session}
if mode == 'resume_foreign': init['session_id'] = 'foreign-session'
if mode == 'resume_missing': del init['session_id']
emit(init)
if mode == 'foreground':
    emit({'type':'assistant','session_id':session,'parent_tool_use_id':None,
          'message':{'id':'work-message','role':'assistant','content':[
          {'type':'tool_use','id':'read-work','name':'Read','input':{'file_path':'fixture'}},
          {'type':'tool_use','id':'bash-work','name':'Bash','input':{'command':'true'}}]}})
    emit({'type':'user','session_id':session,'parent_tool_use_id':None,
          'message':{'role':'user','content':[{'type':'tool_result','tool_use_id':'read-work','content':'ok'}]}})
    emit({'type':'user','session_id':session,'parent_tool_use_id':None,
          'tool_use_result':{'stdout':'','stderr':'','interrupted':False},
          'message':{'role':'user','content':[{'type':'tool_result','tool_use_id':'bash-work','content':'ok'}]}})
if mode == 'resume_lifecycle':
    lifecycle['state'] = 'started'
    emit(lifecycle)
if mode.startswith('resume_status'):
    status = {'type':'system','subtype':'status','status':'requesting','session_id':session,
              'uuid':'33333333-3333-4333-8333-333333333333','user_message_uuids':[initial['uuid']]}
    if mode == 'resume_status_foreign': status['user_message_uuids'] = ['44444444-4444-4444-8444-444444444444']
    emit(status)
if mode.startswith('resume') and mode not in ('resume_foreign','resume_missing','resume_no_ack'):
    replay = {'type':'user','session_id':session,'parent_tool_use_id':None,'isReplay':True,
              'uuid':initial['uuid'],'message':initial['message']}
    if mode == 'resume_wrong_uuid': replay['uuid'] = '22222222-2222-4222-8222-222222222222'
    if mode == 'resume_foreign_ack': replay['session_id'] = 'foreign-session'
    if mode == 'resume_wrong_text': replay['message'] = {'role':'user','content':[{'type':'text','text':'changed'}]}
    emit(replay)
emit({'type':'result','session_id':session,'user_message_uuid':initial['uuid'],
      'subtype':'success','is_error':False,'result':'Initial work is in progress.'})
control = read_input()
if control is None: sys.exit(1)
emit({'type':'assistant','session_id':session,'parent_tool_use_id':None,
      'message':{'id':'pause-message','role':'assistant','content':[
      {'type':'tool_use','id':'pause-tool','name':'mcp__agent_run_worker__maintenance_pause',
       'input':{'challenge_id':challenge,'checkpoint':checkpoint}}]}})
while not os.path.exists('receipt.json'): time.sleep(0.01)
with open('receipt.json', encoding='utf-8') as file: receipt = json.load(file)
emit({'type':'user','session_id':session,'parent_tool_use_id':None,
      'message':{'role':'user','content':[{'type':'tool_result','tool_use_id':'pause-tool',
      'is_error':False,'content':json.dumps(receipt, separators=(',', ':'))}]}})
result = {'type':'result','session_id':session,'user_message_uuid':control['uuid'],
          'subtype':'success','is_error':False,'result':'Checkpoint accepted.'}
batch = json.dumps(result, separators=(',', ':')) + '\n'
if mode in ('lifecycle_tail', 'foreign_lifecycle', 'cancelled_lifecycle'):
    lifecycle = {'type':'command_lifecycle','command_uuid':control['uuid'],
                 'state':'completed','uuid':'33333333-3333-4333-8333-333333333333',
                 'session_id':session}
    if mode == 'foreign_lifecycle': lifecycle['command_uuid'] = '44444444-4444-4444-8444-444444444444'
    if mode == 'cancelled_lifecycle': lifecycle['state'] = 'cancelled'
    batch += json.dumps(lifecycle, separators=(',', ':')) + '\n'
if buffered:
    late = {'type':'assistant','session_id':session,'parent_tool_use_id':None,
            'message':{'id':'late-message','role':'assistant','content':[{'type':'text','text':'late'}]}}
    batch += json.dumps(late, separators=(',', ':')) + '\n'
os.write(sys.stdout.fileno(), batch.encode())
while read_input() is not None: pass
"#;

/// Poll one fixture observation with an overall deadline.
async fn wait_for(mut condition: impl FnMut() -> bool) -> Result<(), &'static str> {
    tokio::time::timeout(Duration::from_secs(10), async {
        while !condition() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .map_err(|_| "fixture observation timed out")
}

/// Exercise both the successful exit and the held paths using one protocol fixture.
#[tokio::test]
async fn claude_maintenance_runner_exits_only_after_unbuffered_closing() {
    for scenario in [
        "success",
        "foreground",
        "lifecycle_tail",
        "foreign_lifecycle",
        "cancelled_lifecycle",
        "buffered",
        "refused",
    ] {
        let home = common::Home::new();
        let mut store = home.store();
        let mut role = common::role_payload();
        role.as_object_mut().unwrap().remove("config_revision");
        role["worker_mcp"] = json!(true);
        role["config_revision"] = json!(agent_run_domain::canonical::sha256_hex(&role, true));
        let mut request = home.request();
        request.explicit_finish = true;
        let identity = json!({"provider_identity_version":2,"authority":{"role_payload":role,"harness":"claude-code","provider":"fixture"}});
        let (run, _) = store
            .admit(&request, &home.config, &identity, None)
            .unwrap();
        store
            .conn
            .execute(
                "UPDATE agents SET status='running',runtime_session_id=? WHERE id=?",
                rusqlite::params![SESSION, run.as_str()],
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
            .begin_maintenance_fence("runner-fixture", 2.0)
            .unwrap()
            .epoch;
        if scenario == "refused" {
            store.conn.execute_batch("CREATE TRIGGER fixture_refuse_closing BEFORE UPDATE OF closing_event_seq ON maintenance_checkpoints WHEN NEW.closing_event_seq IS NOT NULL BEGIN SELECT RAISE(ABORT,'fixture closing refusal'); END;").unwrap();
        }
        let record = store.get(&run).unwrap();
        let prep = common::writer::prepare_supervisor(&mut store, &run);
        let harness = common::writer::Harness::start(&home.path);
        let mut persist = harness.supervisor(&home.path, prep, &run).await;
        persist
            .install_finish_guard(&BTreeMap::new())
            .await
            .unwrap();
        let mut process = Process::spawn(&LaunchPlan {
            binary: "/usr/bin/python3".into(),
            args: vec![
                "-u".into(),
                "-c".into(),
                FAKE_CLAUDE.into(),
                scenario.into(),
            ],
            cwd: home.path.clone(),
            environment: BTreeMap::new(),
            initial_input: None,
        })
        .unwrap();
        let broker = harness.broker();
        let lane = broker.lane("claude-maintenance-runner-fixture");
        let initial = json!({"type":"user","message":{"role":"user","content":[{"type":"text","text":request.task}]}}).to_string();
        let drive = async {
            wait_for(|| {
                store
                    .conn
                    .query_row(
                        "SELECT phase='idle' FROM worker_lifecycle WHERE attempt_id=?",
                        [ATTEMPT],
                        |r| r.get::<_, bool>(0),
                    )
                    .unwrap_or(false)
            })
            .await?;
            lane.local(
                RequestMaintenanceSteer {
                    epoch,
                    execution: run.clone(),
                    attempt: ATTEMPT.into(),
                    challenge_id: CHALLENGE.into(),
                    at: agent_run_domain::domain::now(),
                },
                Duration::from_secs(5),
            )
            .await
            .map_err(|_| "steer request failed")?;
            wait_for(|| {
                std::fs::read_to_string(home.path.join("received.jsonl"))
                    .is_ok_and(|s| s.lines().count() >= 2)
            })
            .await?;
            let receipt = lane
                .local_as(
                    WriteAuthority::Worker {
                        execution: run.clone(),
                        attempt: ATTEMPT.into(),
                    },
                    AcceptMaintenanceCheckpoint {
                        execution: run.clone(),
                        attempt: ATTEMPT.into(),
                        token: TOKEN.into(),
                        input: MaintenancePauseRequest {
                            challenge_id: CHALLENGE.into(),
                            checkpoint: CHECKPOINT.into(),
                        },
                        at: agent_run_domain::domain::now(),
                        guard: FinishGuard::Unavailable,
                    },
                    Duration::from_secs(5),
                )
                .await
                .map_err(|_| "checkpoint callback failed")?;
            std::fs::write(
                home.path.join("receipt.tmp"),
                serde_json::to_vec(&receipt).unwrap(),
            )
            .map_err(|_| "receipt fixture failed")?;
            std::fs::rename(
                home.path.join("receipt.tmp"),
                home.path.join("receipt.json"),
            )
            .map_err(|_| "receipt fixture failed")?;
            if matches!(
                scenario,
                "buffered" | "refused" | "foreign_lifecycle" | "cancelled_lifecycle"
            ) {
                wait_for(|| store.conn.query_row("SELECT COUNT(*) FROM events WHERE agent_id=? AND kind='maintenance_native_settled_v1'",[run.as_str()],|r|r.get::<_,i64>(0)).unwrap_or(0)==1).await?;
                if scenario != "refused" {
                    wait_for(|| store.conn.query_row("SELECT closing_event_seq IS NOT NULL FROM maintenance_checkpoints WHERE epoch=? AND attempt_id=?",rusqlite::params![epoch,ATTEMPT],|r|r.get::<_,bool>(0)).unwrap_or(false)).await?;
                }
                lane.remote(
                    EnqueueCommand {
                        execution: run.clone(),
                        kind: "steer".into(),
                        payload: json!({"text":"must remain unsent"}),
                    },
                    Duration::from_secs(5),
                )
                .await
                .map_err(|_| "steer enqueue failed")?;
                tokio::time::sleep(Duration::from_millis(350)).await;
                lane.remote(
                    EnqueueCommand {
                        execution: run.clone(),
                        kind: "cancel".into(),
                        payload: json!({}),
                    },
                    Duration::from_secs(5),
                )
                .await
                .map_err(|_| "cancel enqueue failed")?;
            }
            Ok::<(), &'static str>(())
        };
        let (outcome, driven) = tokio::join!(
            tokio::time::timeout(
                Duration::from_secs(15),
                stream::run_maintenance(
                    &mut process,
                    &mut persist,
                    &record,
                    &record,
                    Some(&initial),
                    true
                )
            ),
            drive,
        );
        process.input.take();
        let cleanup = process.owner.cleanup(Duration::from_secs(2)).await;
        process.reap().await;
        assert!(cleanup.unwrap().confirmed, "{scenario}");
        driven.unwrap();
        let outcome = outcome.expect("runner timeout").expect("runner failure");
        match (scenario, outcome) {
            (
                "success" | "foreground" | "lifecycle_tail",
                EngineExit::MaintenancePause {
                    epoch: actual,
                    attempt_id,
                    native_event_seq,
                },
            ) => {
                assert_eq!(actual, epoch);
                assert_eq!(attempt_id, ATTEMPT);
                assert_eq!(store.conn.query_row("SELECT closing_event_seq FROM maintenance_checkpoints WHERE epoch=? AND attempt_id=?",rusqlite::params![epoch,ATTEMPT],|r|r.get::<_,i64>(0)).unwrap(),native_event_seq);
            }
            (
                "buffered" | "refused" | "foreign_lifecycle" | "cancelled_lifecycle",
                EngineExit::Finished(result),
            ) => {
                assert_eq!(result.outcome.status, Status::Cancelled)
            }
            _ => panic!("unexpected maintenance runner exit for {scenario}"),
        }
        let sent = std::fs::read_to_string(home.path.join("received.jsonl")).unwrap();
        assert_eq!(
            sent.lines().count(),
            2,
            "native input was repeated in {scenario}"
        );
        assert_eq!(store.conn.query_row("SELECT COUNT(*) FROM events WHERE agent_id=? AND kind='maintenance_native_settled_v1'",[run.as_str()],|r|r.get::<_,i64>(0)).unwrap(),1);
        assert_eq!(store.get(&run).unwrap().status, Status::Running);
        assert!(store.worker_finish_intent(&run, false).unwrap().is_none());
    }
}

/// Confirmation is written only after the one initial continuation frame; a
/// rejected marker holds the attempt and cancellation never replays the input.
#[cfg(feature = "test-fixtures")]
#[tokio::test]
async fn claude_resume_confirms_only_after_one_native_input() {
    use maintenance_resume_fixture::{CONTROL, Fixture};
    for scenario in [
        "resume",
        "resume_lifecycle",
        "resume_lifecycle_foreign",
        "resume_lifecycle_cancelled",
        "resume_status",
        "resume_status_foreign",
        "resume_rejected",
        "resume_foreign",
        "resume_missing",
        "resume_foreign_ack",
        "resume_wrong_uuid",
        "resume_wrong_text",
        "resume_no_ack",
    ] {
        let reject = scenario == "resume_rejected";
        let mut f = Fixture::new(SESSION, reject);
        let prep = common::writer::prepare_supervisor(&mut f.store, &f.run);
        let harness = common::writer::Harness::start(&f.home.path);
        let mut persist = harness.supervisor(&f.home.path, prep, &f.run).await;
        let mut process = Process::spawn(&LaunchPlan {
            binary: "/usr/bin/python3".into(),
            args: vec![
                "-u".into(),
                "-c".into(),
                FAKE_CLAUDE.into(),
                scenario.into(),
            ],
            cwd: f.home.path.clone(),
            environment: BTreeMap::new(),
            initial_input: None,
        })
        .unwrap();
        let mut native = f.record.clone();
        native.request.task = CONTROL.into();
        let initial=json!({"type":"user","message":{"role":"user","content":[{"type":"text","text":CONTROL}]}}).to_string();
        let broker = harness.broker();
        let lane = broker.lane("claude-resume-fixture");
        let drive = async {
            wait_for(|| {
                std::fs::read_to_string(f.home.path.join("received.jsonl"))
                    .is_ok_and(|s| s.lines().count() == 1)
            })
            .await?;
            if matches!(scenario, "resume" | "resume_lifecycle" | "resume_status") {
                wait_for(||f.store.conn.query_row("SELECT COUNT(*) FROM events WHERE agent_id=? AND kind='maintenance_resume_confirmed_v1'",[f.run.as_str()],|r|r.get::<_,i64>(0)).unwrap_or(0)==1).await?;
            }
            if matches!(
                scenario,
                "resume" | "resume_lifecycle" | "resume_status" | "resume_rejected"
            ) {
                lane.remote(
                    EnqueueCommand {
                        execution: f.run.clone(),
                        kind: "cancel".into(),
                        payload: json!({}),
                    },
                    Duration::from_secs(5),
                )
                .await
                .map_err(|_| "cancel enqueue failed")?;
            }
            Ok::<(), &'static str>(())
        };
        let (outcome, driven) = tokio::join!(
            tokio::time::timeout(
                Duration::from_secs(15),
                stream::fixture_run_maintenance_resume(
                    &mut process,
                    &mut persist,
                    &native,
                    &f.record,
                    Some(&initial),
                    false,
                    &f.boundary
                )
            ),
            drive,
        );
        process.input.take();
        let cleanup = process.owner.cleanup(Duration::from_secs(2)).await;
        process.reap().await;
        assert!(cleanup.unwrap().confirmed);
        driven.unwrap();
        let outcome = outcome.expect("resume timeout").expect("resume failure");
        if !matches!(scenario, "resume" | "resume_lifecycle" | "resume_status") {
            assert!(matches!(outcome, EngineExit::MaintenanceUnconfirmed));
        } else {
            assert!(
                matches!(outcome,EngineExit::Finished(result) if result.outcome.status==Status::Cancelled)
            );
        }
        let sent = std::fs::read_to_string(f.home.path.join("received.jsonl")).unwrap();
        assert_eq!(sent.lines().count(), 1);
        assert!(sent.contains(CONTROL));
        assert!(!sent.contains("fixture task"));
        if matches!(scenario, "resume" | "resume_lifecycle" | "resume_status") {
            let written: serde_json::Value =
                serde_json::from_str(sent.lines().next().unwrap()).unwrap();
            let raw:String=f.store.conn.query_row("SELECT data_json FROM events WHERE agent_id=? AND kind='maintenance_resume_confirmed_v1'",[f.run.as_str()],|r|r.get(0)).unwrap();
            let evidence: serde_json::Value = serde_json::from_str(&raw).unwrap();
            assert_eq!(evidence["session"], SESSION);
            assert_eq!(evidence["turn"], written["uuid"]);
            assert_eq!(
                evidence["input_sha256"],
                agent_run_platform::fs::sha256(CONTROL.as_bytes())
            );
            assert!(!raw.contains(CONTROL));
        }
        assert_eq!(f.store.conn.query_row("SELECT COUNT(*) FROM events WHERE agent_id=? AND kind='maintenance_resume_confirmed_v1'",[f.run.as_str()],|r|r.get::<_,i64>(0)).unwrap(),if matches!(scenario,"resume" | "resume_lifecycle" | "resume_status") {1}else{0});
        assert_eq!(f.store.get(&f.run).unwrap().status, Status::Running);
    }
}

/// A resumed maintenance attempt can reach a second independently confirmed
/// pause without treating its verified source as unknown inherited work.
#[cfg(feature = "test-fixtures")]
#[tokio::test]
async fn claude_verified_maintenance_continuation_can_pause_again() {
    use agent_run_store::ops::{BeginMaintenanceFence, EndMaintenanceFence};
    use maintenance_resume_fixture::{CONTROL, Fixture, TARGET};
    let mut f = Fixture::new(SESSION, false);
    let mut role = common::role_payload();
    role.as_object_mut().unwrap().remove("config_revision");
    role["worker_mcp"] = json!(true);
    role["config_revision"] = json!(agent_run_domain::canonical::sha256_hex(&role, true));
    let identity = json!({"provider_identity_version":2,"authority":{
        "role_payload":role,"harness":"claude-code","provider":"fixture"}});
    f.store
        .conn
        .execute(
            "UPDATE agents SET identity_json=? WHERE id=?",
            rusqlite::params![identity.to_string(), f.run.as_str()],
        )
        .unwrap();
    f.record = f.store.get(&f.run).unwrap();
    f.record.resume_of_runtime_session_id = Some(SESSION.into());
    f.store
        .issue_worker_capability(&f.run, TARGET, TOKEN, 1.0)
        .unwrap();
    f.store.begin_worker_lifecycle(&f.run, TARGET, 1.0).unwrap();
    f.store
        .register_worker_catalog(
            &WorkerCatalogProof {
                run_id: f.run.clone(),
                attempt_id: TARGET.into(),
                token: TOKEN.into(),
                version: 5,
                digest: maintenance_pause_catalog_digest(false),
            },
            1.0,
        )
        .unwrap();
    let prep = common::writer::prepare_supervisor(&mut f.store, &f.run);
    let harness = common::writer::Harness::start(&f.home.path);
    let mut persist = harness.supervisor(&f.home.path, prep, &f.run).await;
    persist
        .install_finish_guard(&BTreeMap::new())
        .await
        .unwrap();
    let mut process = Process::spawn(&LaunchPlan {
        binary: "/usr/bin/python3".into(),
        args: vec![
            "-u".into(),
            "-c".into(),
            FAKE_CLAUDE.into(),
            "resume".into(),
        ],
        cwd: f.home.path.clone(),
        environment: BTreeMap::new(),
        initial_input: None,
    })
    .unwrap();
    let mut native = f.record.clone();
    native.request.task = CONTROL.into();
    let initial =
        json!({"type":"user","message":{"role":"user","content":[{"type":"text","text":CONTROL}]}})
            .to_string();
    let lane = harness.broker().lane("claude-second-maintenance");
    let drive = async {
        wait_for(|| f.store.conn.query_row("SELECT COUNT(*) FROM events WHERE agent_id=? AND kind='maintenance_resume_confirmed_v1'",[f.run.as_str()],|r|r.get::<_,i64>(0)).unwrap_or(0)==1).await?;
        wait_for(|| {
            f.store
                .conn
                .query_row(
                    "SELECT phase='idle' FROM worker_lifecycle WHERE attempt_id=?",
                    [TARGET],
                    |r| r.get::<_, bool>(0),
                )
                .unwrap_or(false)
        })
        .await?;
        lane.local(
            EndMaintenanceFence {
                epoch: f.boundary.epoch,
                at: agent_run_domain::domain::now(),
            },
            Duration::from_secs(5),
        )
        .await
        .map_err(|_| "close first fence")?;
        let fence = lane
            .local(
                BeginMaintenanceFence {
                    request_id: "second-maintenance".into(),
                    at: agent_run_domain::domain::now(),
                },
                Duration::from_secs(5),
            )
            .await
            .map_err(|_| "begin second fence")?;
        lane.local(
            RequestMaintenanceSteer {
                epoch: fence.epoch,
                execution: f.run.clone(),
                attempt: TARGET.into(),
                challenge_id: CHALLENGE.into(),
                at: agent_run_domain::domain::now(),
            },
            Duration::from_secs(5),
        )
        .await
        .map_err(|_| "request second pause")?;
        wait_for(|| {
            std::fs::read_to_string(f.home.path.join("received.jsonl"))
                .is_ok_and(|s| s.lines().count() == 2)
        })
        .await?;
        let receipt = lane
            .local_as(
                WriteAuthority::Worker {
                    execution: f.run.clone(),
                    attempt: TARGET.into(),
                },
                AcceptMaintenanceCheckpoint {
                    execution: f.run.clone(),
                    attempt: TARGET.into(),
                    token: TOKEN.into(),
                    input: MaintenancePauseRequest {
                        challenge_id: CHALLENGE.into(),
                        checkpoint: CHECKPOINT.into(),
                    },
                    at: agent_run_domain::domain::now(),
                    guard: FinishGuard::Unavailable,
                },
                Duration::from_secs(5),
            )
            .await
            .map_err(|_| "accept second checkpoint")?;
        std::fs::write(
            f.home.path.join("receipt.tmp"),
            serde_json::to_vec(&receipt).unwrap(),
        )
        .map_err(|_| "write receipt")?;
        std::fs::rename(
            f.home.path.join("receipt.tmp"),
            f.home.path.join("receipt.json"),
        )
        .map_err(|_| "publish receipt")?;
        Ok::<_, &'static str>(fence.epoch)
    };
    let (outcome, driven) = tokio::join!(
        tokio::time::timeout(
            Duration::from_secs(20),
            stream::fixture_run_maintenance_resume(
                &mut process,
                &mut persist,
                &native,
                &f.record,
                Some(&initial),
                true,
                &f.boundary
            )
        ),
        drive
    );
    process.input.take();
    assert!(
        process
            .owner
            .cleanup(Duration::from_secs(2))
            .await
            .unwrap()
            .confirmed
    );
    process.reap().await;
    let epoch = driven.unwrap();
    assert!(
        matches!(outcome.unwrap().unwrap(), EngineExit::MaintenancePause {epoch: actual,attempt_id,..} if actual==epoch && attempt_id==TARGET)
    );
}
