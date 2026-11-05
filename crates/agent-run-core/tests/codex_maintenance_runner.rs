//! Fake Codex app-server through the real runner and in-process writer.
//! No provider, network, credentials, or service restart is used.
mod common;
#[cfg(feature = "test-fixtures")]
#[path = "common/maintenance_resume_fixture.rs"]
mod maintenance_resume_fixture;

use agent_run_adapters::{LaunchPlan, io::Process};
use agent_run_config::{config::Runtime, profiles::Profile};
use agent_run_core::{
    codex::{self, Grant},
    domain::Status,
    maintenance_execution::EngineExit,
};
use agent_run_domain::{
    storage::WriteAuthority,
    worker::{FinishGuard, MaintenancePauseRequest, WorkerCatalogProof},
};
use agent_run_store::ops::{AcceptMaintenanceCheckpoint, EnqueueCommand, RequestMaintenanceSteer};
use serde_json::{Value, json};
use std::{collections::BTreeMap, time::Duration};

const ATTEMPT: &str = "codex-maintenance-fixture";
const TOKEN: &str = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
const CHALLENGE: &str = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
const THREAD: &str = "fixture-thread";
const CHECKPOINT: &str = "Checked the current work; continue with the remaining review.";

/// One line-framed app-server double. Correlated RPC responses and native
/// frames are real runner inputs; the worker callback is supplied by the driver.
const FAKE_CODEX: &str = r#"
import json, os, sys, time
thread = 'fixture-thread'
challenge = 'b' * 64
checkpoint = 'Checked the current work; continue with the remaining review.'
mode = sys.argv[1]
turns = 0
def encode(value): return json.dumps(value, separators=(',', ':')) + '\n'
def send(value):
    sys.stdout.write(encode(value)); sys.stdout.flush()
def reply(id, result): send({'id':id,'result':result})
for line in sys.stdin:
    msg = json.loads(line)
    method = msg.get('method')
    ident = msg.get('id')
    if method == 'initialize': reply(ident,{})
    elif method == 'initialized': pass
    elif method == 'model/list': reply(ident,{'data':[{'id':'fixture'}]})
    elif method == 'config/read': reply(ident,{'config':{'developer_instructions':None}})
    elif method == 'thread/start' or method == 'thread/resume':
        key = 'ECHO_MODERN' if 'permissions' in msg.get('params',{}) else 'ECHO_LEGACY'
        echo = json.loads(os.environ[key])
        if mode == 'resume_foreign': echo['threadId'] = 'foreign-thread'
        if mode == 'resume_conflicting': echo['thread']['id'] = 'foreign-thread'
        if mode == 'resume_no_status': del echo['thread']['status']
        reply(ident,echo)
        if method == 'thread/resume':
            with open('thread-resumed','w',encoding='utf-8') as ready: ready.write('done')
    elif method == 'mcpServerStatus/list': reply(ident,json.loads(os.environ['FAKE_CATALOG']))
    elif method == 'turn/start':
        turns += 1
        turn = 'turn-' + str(turns)
        with open('turns.jsonl','a',encoding='utf-8') as log:
            log.write(line); log.flush()
        turn_reply={'turn':{'id':turn}}
        if mode == 'resume_foreign_turn': turn_reply['turn']['threadId']='foreign-thread'
        reply(ident,turn_reply)
        if turns == 1:
            if mode != 'prior_unknown':
                send({'method':'thread/tokenUsage/updated','params':{'threadId':thread,'tokenUsage':{'total':{'inputTokens':5,'totalTokens':7}}}})
                send({'method':'turn/completed','params':{'threadId':thread,'turn':{'id':turn,'status':'completed','items':[]}}})
            continue
        send({'method':'item/started','params':{'threadId':thread,'turnId':turn,'item':{
            'id':'pause-item','type':'mcpToolCall','server':'agent_run_worker','tool':'maintenance_pause',
            'arguments':{'challenge_id':challenge,'checkpoint':checkpoint}}}})
        while not os.path.exists('receipt.json'): time.sleep(0.01)
        with open('receipt.json',encoding='utf-8') as file: receipt=json.load(file)
        send({'method':'item/completed','params':{'threadId':thread,'turnId':turn,'item':{
            'id':'pause-item','type':'mcpToolCall','server':'agent_run_worker','tool':'maintenance_pause',
            'status':'completed','result':{'content':[{'type':'text','text':json.dumps(receipt,separators=(',', ':'))}]}}}})
        result={'method':'turn/completed','params':{'threadId':thread,'turn':{'id':turn,'status':'completed','items':[]}}}
        batch=encode(result)
        if mode == 'buffered':
            batch+=encode({'method':'item/started','params':{'threadId':thread,'turnId':turn,'item':{'id':'late','type':'commandExecution','command':'true','status':'inProgress'}}})
        elif mode == 'usage':
            batch+=encode({'method':'thread/tokenUsage/updated','params':{'threadId':thread,'turnId':turn,'tokenUsage':{'total':{'inputTokens':10,'totalTokens':12,'authorization':'must-not-persist'}}}})
        elif mode == 'rpc':
            batch+=encode({'id':1,'result':{}})
        elif mode == 'foreign_usage':
            batch+=encode({'method':'thread/tokenUsage/updated','params':{'threadId':'foreign','tokenUsage':{'total':{'inputTokens':10}}}})
        elif mode == 'unknown_rpc':
            batch+=encode({'id':999999,'result':{}})
        os.write(sys.stdout.fileno(),batch.encode())
        with open('checkpoint-done','w',encoding='utf-8') as done: done.write('done')
    elif method == 'turn/steer' and mode == 'prior_unknown':
        with open('uncertain.jsonl','a',encoding='utf-8') as log: log.write(line)
        send({'id':ident})
        send({'method':'turn/completed','params':{'threadId':thread,'turn':{'id':'turn-1','status':'completed','items':[]}}})
    elif method == 'turn/interrupt': reply(ident,{})
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

/// Exercise v5 exact proof, buffered/refused closing, and the ordinary v4 path.
#[tokio::test]
async fn codex_maintenance_runner_requires_exact_staged_inventory_and_settlement() {
    for scenario in [
        "success",
        "usage",
        "rpc",
        "buffered",
        "foreign_usage",
        "unknown_rpc",
        "refused",
        "prior_unknown",
        "legacy",
    ] {
        let home = common::Home::new();
        let mut store = home.store();
        let mut role_payload = common::role_payload();
        role_payload
            .as_object_mut()
            .unwrap()
            .remove("config_revision");
        role_payload["worker_mcp"] = json!(true);
        role_payload["config_revision"] =
            json!(agent_run_domain::canonical::sha256_hex(&role_payload, true));
        let mut request = home.request();
        request.explicit_finish = true;
        let identity = json!({"provider_identity_version":2,"authority":{"role_payload":role_payload,"harness":"codex","provider":"fixture"}});
        let (run, _) = store
            .admit(&request, &home.config, &identity, None)
            .unwrap();
        store
            .conn
            .execute(
                "UPDATE agents SET status='running',runtime_session_id=? WHERE id=?",
                rusqlite::params![THREAD, run.as_str()],
            )
            .unwrap();
        store.conn.execute("INSERT INTO attempts(id,agent_id,number,state,adapter_state_json,created_at,ownership_active) VALUES(?,?,1,'running','{}',1,1)",rusqlite::params![ATTEMPT,run.as_str()]).unwrap();
        store
            .issue_worker_capability(&run, ATTEMPT, TOKEN, 1.0)
            .unwrap();
        store.begin_worker_lifecycle(&run, ATTEMPT, 1.0).unwrap();
        let legacy = scenario == "legacy";
        let (version, digest, tools) = if legacy {
            (
                4,
                agent_run_domain::worker::finish_catalog_digest(false),
                agent_run_domain::worker::finish_tools_json(false),
            )
        } else {
            (
                5,
                agent_run_domain::worker::maintenance_pause_catalog_digest(false),
                agent_run_domain::worker::maintenance_pause_tools_json(false),
            )
        };
        store
            .register_worker_catalog(
                &WorkerCatalogProof {
                    run_id: run.clone(),
                    attempt_id: ATTEMPT.into(),
                    token: TOKEN.into(),
                    version,
                    digest,
                },
                1.0,
            )
            .unwrap();
        let epoch = if legacy {
            None
        } else {
            Some(
                store
                    .begin_maintenance_fence("codex-runner-fixture", 2.0)
                    .unwrap()
                    .epoch,
            )
        };
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
        let app_home = home.path.join("codex-home");
        agent_run_platform::fs::private_dir(&app_home).unwrap();
        let runtime:Runtime=serde_json::from_value(json!({"enabled":true,"adapter":"codex","binary":"/usr/bin/python3","home":home.path.join("native"),"models":["fixture"]})).unwrap();
        let role = Profile {
            name: "review".into(),
            body: "Review fixture".into(),
            write: false,
            network: false,
            revision: "fixture".into(),
            canonical: false,
            allow_external_read_roots: true,
            read_roots: vec![],
            skills: vec![],
            mcp: vec![],
            mcp_tools: Default::default(),
            required_constraints: Default::default(),
        };
        let grant = Grant::new(&runtime, &record.request, &role, &app_home).unwrap();
        let legacy_echo = json!({"model":grant.model,"cwd":grant.cwd,"roots":grant.roots,"writableRoots":grant.writable_roots,"sandbox":grant.sandbox,"approvalPolicy":grant.approval_policy,"threadId":THREAD});
        let modern_echo = json!({"model":grant.model,"cwd":grant.cwd,"runtimeWorkspaceRoots":grant.roots,"sandbox":{"type":"readOnly","networkAccess":grant.network_access},"approvalPolicy":grant.approval_policy,"activePermissionProfile":{"id":grant.permission_profile.as_deref().unwrap_or(":read-only")},"threadId":THREAD});
        let tools: serde_json::Map<String, Value> = tools
            .into_iter()
            .map(|tool| (tool["name"].as_str().unwrap().into(), tool))
            .collect();
        let catalog =
            json!({"data":[{"name":"agent_run_worker","runtimeStatus":"connected","tools":tools}]});
        let environment = BTreeMap::from([
            ("ECHO_LEGACY".into(), legacy_echo.to_string()),
            ("ECHO_MODERN".into(), modern_echo.to_string()),
            ("FAKE_CATALOG".into(), catalog.to_string()),
            (
                agent_run_domain::worker::FINISH_ENV.into(),
                if legacy { "1" } else { "5" }.into(),
            ),
        ]);
        let mut process = Process::spawn(&LaunchPlan {
            binary: "/usr/bin/python3".into(),
            args: vec!["-u".into(), "-c".into(), FAKE_CODEX.into(), scenario.into()],
            cwd: home.path.clone(),
            environment,
            initial_input: None,
        })
        .unwrap();
        let broker = harness.broker();
        let lane = broker.lane("codex-maintenance-runner-fixture");
        let drive = async {
            if scenario == "prior_unknown" {
                wait_for(|| {
                    std::fs::read_to_string(home.path.join("turns.jsonl"))
                        .is_ok_and(|s| s.lines().count() == 1)
                })
                .await?;
                lane.remote(
                    EnqueueCommand {
                        execution: run.clone(),
                        kind: "steer".into(),
                        payload: json!({"text":"uncertain input"}),
                    },
                    Duration::from_secs(5),
                )
                .await
                .map_err(|_| "uncertain steer enqueue failed")?;
                wait_for(||store.conn.query_row("SELECT EXISTS(SELECT 1 FROM commands WHERE agent_id=? AND kind='steer' AND state='completed' AND json_type(result_json,'$.accepted')='null')",[run.as_str()],|r|r.get::<_,bool>(0)).unwrap_or(false)).await?;
            }
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
            if legacy {
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
                return Ok::<(), &'static str>(());
            }
            let epoch = epoch.unwrap();
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
                std::fs::read_to_string(home.path.join("turns.jsonl"))
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
            if scenario == "prior_unknown" {
                wait_for(|| home.path.join("checkpoint-done").exists()).await?;
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
            } else if matches!(
                scenario,
                "buffered" | "foreign_usage" | "unknown_rpc" | "refused"
            ) {
                wait_for(||store.conn.query_row("SELECT COUNT(*) FROM events WHERE agent_id=? AND kind='maintenance_native_settled_v1'",[run.as_str()],|r|r.get::<_,i64>(0)).unwrap_or(0)==1).await?;
                if scenario != "refused" {
                    wait_for(||store.conn.query_row("SELECT closing_event_seq IS NOT NULL FROM maintenance_checkpoints WHERE epoch=? AND attempt_id=?",rusqlite::params![epoch,ATTEMPT],|r|r.get::<_,bool>(0)).unwrap_or(false)).await?;
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
            Ok(())
        };
        let run_future = async {
            if legacy {
                codex::run(
                    &mut process,
                    &mut persist,
                    &record,
                    &record,
                    &runtime,
                    &role,
                    &app_home,
                )
                .await
                .map(EngineExit::Finished)
            } else {
                codex::run_maintenance(
                    &mut process,
                    &mut persist,
                    &record,
                    &record,
                    &runtime,
                    &role,
                    &app_home,
                    true,
                    "5",
                )
                .await
            }
        };
        let (outcome, driven) = tokio::join!(
            tokio::time::timeout(Duration::from_secs(15), run_future),
            drive
        );
        process.input.take();
        let cleanup = process.owner.cleanup(Duration::from_secs(2)).await;
        process.reap().await;
        assert!(cleanup.unwrap().confirmed, "{scenario}");
        driven.unwrap();
        let outcome = outcome.expect("runner timeout").expect("runner failure");
        match (scenario, outcome) {
            (
                "success" | "usage" | "rpc",
                EngineExit::MaintenancePause {
                    epoch: actual,
                    attempt_id,
                    native_event_seq,
                },
            ) => {
                assert_eq!(Some(actual), epoch);
                assert_eq!(attempt_id, ATTEMPT);
                assert_eq!(store.conn.query_row("SELECT closing_event_seq FROM maintenance_checkpoints WHERE epoch=? AND attempt_id=?",rusqlite::params![actual,ATTEMPT],|r|r.get::<_,i64>(0)).unwrap(),native_event_seq);
            }
            (
                "buffered" | "foreign_usage" | "unknown_rpc" | "refused" | "prior_unknown"
                | "legacy",
                EngineExit::Finished(result),
            ) => {
                assert_eq!(result.outcome.status, Status::Cancelled)
            }
            _ => panic!("unexpected maintenance runner exit for {scenario}"),
        }
        let sent = std::fs::read_to_string(home.path.join("turns.jsonl")).unwrap();
        assert_eq!(
            sent.lines().count(),
            if legacy { 1 } else { 2 },
            "native input was repeated in {scenario}"
        );
        let events=store.conn.query_row("SELECT COUNT(*) FROM events WHERE agent_id=? AND kind='maintenance_native_settled_v1'",[run.as_str()],|r|r.get::<_,i64>(0)).unwrap();
        assert_eq!(
            events,
            if legacy || scenario == "prior_unknown" {
                0
            } else {
                1
            }
        );
        if scenario == "prior_unknown" {
            assert_eq!(
                std::fs::read_to_string(home.path.join("uncertain.jsonl"))
                    .unwrap()
                    .lines()
                    .count(),
                1
            );
        }
        if matches!(scenario, "success" | "usage" | "rpc") {
            let raw:String=store.conn.query_row("SELECT data_json FROM events WHERE agent_id=? AND kind='thread/tokenUsage/updated' ORDER BY seq DESC LIMIT 1",[run.as_str()],|r|r.get(0)).unwrap();
            let usage: Value = serde_json::from_str(&raw).unwrap();
            assert_eq!(
                usage["tokenUsage"]["total"]["inputTokens"],
                if scenario == "usage" { 10 } else { 5 }
            );
            assert!(!raw.contains("must-not-persist"));
        }
        assert_eq!(store.get(&run).unwrap().status, Status::Running);
        assert!(store.worker_finish_intent(&run, false).unwrap().is_none());
    }
}

/// A resumed turn is confirmed only after the correlated turn/start reply;
/// failed marker persistence leaves the single native send unconfirmed.
#[cfg(feature = "test-fixtures")]
#[tokio::test]
async fn codex_resume_confirms_only_after_one_native_turn_start() {
    use agent_run_store::ops::{BeginMaintenanceFence, EndMaintenanceFence};
    use maintenance_resume_fixture::{CONTROL, Fixture, TARGET};
    for scenario in [
        "resume",
        "resume_second_pause",
        "resume_rejected",
        "resume_foreign",
        "resume_conflicting",
        "resume_no_status",
        "resume_foreign_turn",
    ] {
        let reject = scenario == "resume_rejected";
        let before_turn = matches!(
            scenario,
            "resume_foreign" | "resume_conflicting" | "resume_no_status"
        );
        let mut f = Fixture::new(THREAD, reject);
        let second_pause = scenario == "resume_second_pause";
        if second_pause {
            let mut role = common::role_payload();
            role.as_object_mut().unwrap().remove("config_revision");
            role["worker_mcp"] = json!(true);
            role["config_revision"] = json!(agent_run_domain::canonical::sha256_hex(&role, true));
            let identity = json!({"provider_identity_version":2,"authority":{
                "role_payload":role,"harness":"codex","provider":"fixture"}});
            f.store
                .conn
                .execute(
                    "UPDATE agents SET identity_json=? WHERE id=?",
                    rusqlite::params![identity.to_string(), f.run.as_str()],
                )
                .unwrap();
            f.record = f.store.get(&f.run).unwrap();
            f.record.resume_of_runtime_session_id = Some(THREAD.into());
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
                        digest: agent_run_domain::worker::maintenance_pause_catalog_digest(false),
                    },
                    1.0,
                )
                .unwrap();
        }
        let prep = common::writer::prepare_supervisor(&mut f.store, &f.run);
        let harness = common::writer::Harness::start(&f.home.path);
        let mut persist = harness.supervisor(&f.home.path, prep, &f.run).await;
        if second_pause {
            persist
                .install_finish_guard(&BTreeMap::new())
                .await
                .unwrap();
        }
        let app_home = f.home.path.join("codex-home");
        agent_run_platform::fs::private_dir(&app_home).unwrap();
        let runtime:Runtime=serde_json::from_value(json!({"enabled":true,"adapter":"codex","binary":"/usr/bin/python3","home":f.home.path.join("native"),"models":["fixture"]})).unwrap();
        let role = Profile {
            name: "review".into(),
            body: "Review fixture".into(),
            write: false,
            network: false,
            revision: "fixture".into(),
            canonical: false,
            allow_external_read_roots: true,
            read_roots: vec![],
            skills: vec![],
            mcp: vec![],
            mcp_tools: Default::default(),
            required_constraints: Default::default(),
        };
        let mut native = f.record.clone();
        native.request.task = CONTROL.into();
        let grant = Grant::new(&runtime, &native.request, &role, &app_home).unwrap();
        let legacy = json!({"model":grant.model,"cwd":grant.cwd,"roots":grant.roots,"writableRoots":grant.writable_roots,"sandbox":grant.sandbox,"approvalPolicy":grant.approval_policy,"threadId":THREAD,"thread":{"status":"idle"}});
        let modern = json!({"model":grant.model,"cwd":grant.cwd,"runtimeWorkspaceRoots":grant.roots,"sandbox":{"type":"readOnly","networkAccess":grant.network_access},"approvalPolicy":grant.approval_policy,"activePermissionProfile":{"id":grant.permission_profile.as_deref().unwrap_or(":read-only")},"threadId":THREAD,"thread":{"status":"idle"}});
        let tools: serde_json::Map<String, Value> = (if second_pause {
            agent_run_domain::worker::maintenance_pause_tools_json(false)
        } else {
            agent_run_domain::worker::finish_tools_json(false)
        })
        .into_iter()
        .map(|tool| (tool["name"].as_str().unwrap().into(), tool))
        .collect();
        let environment=BTreeMap::from([("ECHO_LEGACY".into(),legacy.to_string()),("ECHO_MODERN".into(),modern.to_string()),("FAKE_CATALOG".into(),json!({"data":[{"name":"agent_run_worker","runtimeStatus":"connected","tools":tools}]}).to_string())]);
        let mut process = Process::spawn(&LaunchPlan {
            binary: "/usr/bin/python3".into(),
            args: vec!["-u".into(), "-c".into(), FAKE_CODEX.into(), scenario.into()],
            cwd: f.home.path.clone(),
            environment,
            initial_input: None,
        })
        .unwrap();
        let broker = harness.broker();
        let lane = broker.lane("codex-resume-fixture");
        let drive = async {
            if before_turn {
                wait_for(|| f.home.path.join("thread-resumed").exists()).await?;
            } else {
                wait_for(|| {
                    std::fs::read_to_string(f.home.path.join("turns.jsonl"))
                        .is_ok_and(|s| s.lines().count() == 1)
                })
                .await?;
            }
            if scenario == "resume" {
                wait_for(||f.store.conn.query_row("SELECT COUNT(*) FROM events WHERE agent_id=? AND kind='maintenance_resume_confirmed_v1'",[f.run.as_str()],|r|r.get::<_,i64>(0)).unwrap_or(0)==1).await?;
            }
            if second_pause {
                wait_for(||f.store.conn.query_row("SELECT COUNT(*) FROM events WHERE agent_id=? AND kind='maintenance_resume_confirmed_v1'",[f.run.as_str()],|r|r.get::<_,i64>(0)).unwrap_or(0)==1).await?;
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
                            request_id: "second-codex-maintenance".into(),
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
                    std::fs::read_to_string(f.home.path.join("turns.jsonl"))
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
            }
            if matches!(scenario, "resume" | "resume_rejected") {
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
                codex::fixture_run_maintenance_resume(
                    &mut process,
                    &mut persist,
                    &native,
                    &f.record,
                    &runtime,
                    &role,
                    &app_home,
                    second_pause,
                    if second_pause { "5" } else { "1" },
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
        if second_pause {
            assert!(
                matches!(outcome,EngineExit::MaintenancePause {attempt_id,..} if attempt_id==TARGET)
            );
        } else if scenario != "resume" {
            assert!(matches!(outcome, EngineExit::MaintenanceUnconfirmed));
        } else {
            assert!(
                matches!(outcome,EngineExit::Finished(result) if result.outcome.status==Status::Cancelled)
            );
        }
        let sent = std::fs::read_to_string(f.home.path.join("turns.jsonl")).unwrap_or_default();
        assert_eq!(
            sent.lines().count(),
            if before_turn {
                0
            } else if second_pause {
                2
            } else {
                1
            }
        );
        if !before_turn {
            assert!(sent.contains(CONTROL));
        }
        assert!(!sent.contains("fixture task"));
        if scenario == "resume" {
            let written: Value = serde_json::from_str(sent.lines().next().unwrap()).unwrap();
            let text = written
                .pointer("/params/input/0/text")
                .and_then(Value::as_str)
                .unwrap();
            let raw:String=f.store.conn.query_row("SELECT data_json FROM events WHERE agent_id=? AND kind='maintenance_resume_confirmed_v1'",[f.run.as_str()],|r|r.get(0)).unwrap();
            let evidence: Value = serde_json::from_str(&raw).unwrap();
            assert_eq!(evidence["session"], THREAD);
            assert_eq!(evidence["turn"], "turn-1");
            assert_eq!(
                evidence["input_sha256"],
                agent_run_platform::fs::sha256(text.as_bytes())
            );
            assert!(!raw.contains(CONTROL));
        }
        assert_eq!(f.store.conn.query_row("SELECT COUNT(*) FROM events WHERE agent_id=? AND kind='maintenance_resume_confirmed_v1'",[f.run.as_str()],|r|r.get::<_,i64>(0)).unwrap(),if scenario=="resume" || second_pause {1}else{0});
        assert_eq!(f.store.get(&f.run).unwrap().status, Status::Running);
    }
}
