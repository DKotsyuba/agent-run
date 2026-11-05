//! Finalization fixtures use an owned dummy child, sealed files and an in-process writer.
mod common {
    use agent_run_config::config::Config;
    use agent_run_domain::domain::StartRequest;
    use serde_json::json;
    use std::path::PathBuf;

    /// Disposable local state and configuration for supervisor tests.
    pub struct Home {
        _temp: tempfile::TempDir,
        pub path: PathBuf,
        pub config: Config,
    }
    impl Home {
        /// Create an isolated, initialized state directory.
        pub fn new() -> Self {
            let temp = tempfile::tempdir().unwrap();
            let path = temp.path().canonicalize().unwrap();
            agent_run_platform::fs::private_dir(&path).unwrap();
            let binary = if std::path::Path::new("/usr/bin/true").is_file() {
                "/usr/bin/true"
            } else {
                "/bin/true"
            };
            let text = format!(
                "schema_version=1\n[runtimes.mock]\nenabled=true\nadapter='claude'\nbinary={}\nhome={}\nmodels=['fixture']\nlimits_source='none'\n",
                toml::Value::String(binary.into()),
                toml::Value::String(path.join("runtimes/mock").to_string_lossy().into_owned())
            );
            std::fs::write(path.join("config.toml"), text).unwrap();
            let config = Config::load(&path).unwrap();
            agent_run_store::fixture::initialize(&path).unwrap();
            Self {
                _temp: temp,
                path,
                config,
            }
        }
        /// Build a valid local test request.
        pub fn request(&self) -> StartRequest {
            serde_json::from_value(json!({"runtime":"mock","model":"fixture","profile":"review","task":"fixture task","workdir":self.path})).unwrap()
        }
        /// Open an independent writable fixture connection.
        pub fn store(&self) -> agent_run_store::Store<'static> {
            agent_run_store::fixture::open(&self.path).unwrap()
        }
    }
    /// Build one valid role payload for the provider fixture.
    pub fn role_payload() -> serde_json::Value {
        let mut role = json!({"role_name":"review","role_revision":"fixture","prompt":"Review the fixture.","grants":{"write":false,"network":false,"allow_external_read_roots":false,"read_roots":[]},"skills":[],"mcp":[],"required_constraints":[],"auth":{"mode":"global","reference":null}});
        role["config_revision"] = json!(agent_run_domain::canonical::sha256_hex(&role, true));
        role
    }
}

use super::{
    maintenance::{self, PauseContext},
    *,
};
use agent_run_domain::{
    ProviderStartRequest,
    catalog::{HarnessId, ResolvedLaunchAuthority},
    worker::{FinishGuard, MaintenancePauseRequest, maintenance_pause_catalog_digest},
};
use agent_run_store::maintenance_park::NativeSettlement;
use serde_json::{Value, json};
use std::{
    future::Future,
    pin::Pin,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
};

/// Commit a park in the real writer but hide its first ACK and refuse the
/// identical retry, forcing the caller to resolve an unknown result by reading.
struct LostParkAck {
    inner: crate::writer_test_harness::DuplexDial,
    seen: Arc<AtomicUsize>,
}

impl crate::storage_client::Dial for LostParkAck {
    fn dial(
        &self,
    ) -> Pin<Box<dyn Future<Output = std::io::Result<crate::storage_client::Connection>> + Send + '_>>
    {
        Box::pin(async move {
            let writer = self.inner.dial().await?;
            let (client, proxy) = tokio::io::duplex(1 << 20);
            let seen = self.seen.clone();
            tokio::spawn(async move {
                use crate::storage_wire::{self as wire, Message};
                use agent_run_domain::storage::{NotEnqueuedReason, SubmitResult, UnknownCause};
                let (mut input, mut output) = tokio::io::split(proxy);
                let (mut read_writer, mut write_writer) = tokio::io::split(writer);
                loop {
                    let Ok(Some(header)) =
                        wire::read_header(&mut input, |_, _| Some(wire::BODY_HARD_MAX)).await
                    else {
                        break;
                    };
                    let Ok(body) = wire::read_body(&mut input, &header).await else {
                        break;
                    };
                    let park = header.message == Message::Request
                        && header.op_kind
                            == agent_run_store::ops::OpKindRef::ParkMaintenanceAttempt.wire_id();
                    if park && seen.load(Ordering::SeqCst) == 1 {
                        seen.store(2, Ordering::SeqCst);
                        let body = serde_json::to_vec(&SubmitResult::NotEnqueued(
                            NotEnqueuedReason::DeadlineExpired,
                        ))
                        .unwrap();
                        if wire::write_frame(&mut output, Message::Reply, 0, header.seq, &body)
                            .await
                            .is_err()
                        {
                            break;
                        }
                        continue;
                    }
                    if wire::write_frame(
                        &mut write_writer,
                        header.message,
                        header.op_kind,
                        header.seq,
                        &body,
                    )
                    .await
                    .is_err()
                    {
                        break;
                    }
                    let Ok(Some(reply)) =
                        wire::read_header(&mut read_writer, |_, _| Some(wire::BODY_HARD_MAX)).await
                    else {
                        break;
                    };
                    let Ok(mut response) = wire::read_body(&mut read_writer, &reply).await else {
                        break;
                    };
                    if park && seen.load(Ordering::SeqCst) == 0 {
                        if !matches!(
                            serde_json::from_slice::<SubmitResult>(&response),
                            Ok(SubmitResult::Committed(_))
                        ) {
                            break;
                        }
                        seen.store(1, Ordering::SeqCst);
                        response =
                            serde_json::to_vec(&SubmitResult::Unknown(UnknownCause::Transport))
                                .unwrap();
                    }
                    if wire::write_frame(
                        &mut output,
                        reply.message,
                        reply.op_kind,
                        reply.seq,
                        &response,
                    )
                    .await
                    .is_err()
                    {
                        break;
                    }
                }
            });
            Ok(Box::new(client) as crate::storage_client::Connection)
        })
    }
}

const ATTEMPT: &str = "finalization-attempt";
const ACCOUNT: &str = "acct-finalization";
const SESSION: &str = "finalization-session";
const TOKEN: &str = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
const CHALLENGE: &str = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";

/// A later failover can only carry forward a maintenance control that actually
/// appears in the same sealed native session, alongside the original task.
#[test]
fn maintenance_history_proof_requires_the_exact_control() {
    let home = common::Home::new();
    let mut store = home.store();
    let (run, _) = store
        .admit(&home.request(), &home.config, &json!({}), None)
        .unwrap();
    store
        .conn
        .execute(
            "UPDATE agents SET status='running',runtime_session_id=? WHERE id=?",
            rusqlite::params![SESSION, run.as_str()],
        )
        .unwrap();
    let original = "original-task";
    let control = "resume-control";
    let original_hash = crate::fs::sha256(original.as_bytes());
    let control_hash = crate::fs::sha256(control.as_bytes());
    let prior =
        json!({"native_history":{"task_proof":{"turn":"task-turn","input_sha256":original_hash}}})
            .to_string();
    store.conn.execute("INSERT INTO attempts(id,agent_id,number,state,adapter_state_json,created_at,finished_at,ownership_active) VALUES('history-source',?,1,'maintenance_paused',?,1,2,0)",rusqlite::params![run.as_str(),prior]).unwrap();
    store.conn.execute("INSERT INTO attempts(id,agent_id,number,state,adapter_state_json,created_at,ownership_active) VALUES('history-target',?,2,'running','{}',2,1)",[run.as_str()]).unwrap();
    let epoch = store
        .begin_maintenance_fence("history-proof", 3.0)
        .unwrap()
        .epoch;
    store.conn.execute("INSERT INTO maintenance_resumes(epoch,agent_id,request_id,source_attempt_id,target_attempt_id,target_number,account_id,source_proof_sha256,writer_epoch,claimed_at,preflight_json) VALUES(?,?,'history-resume','history-source','history-target',2,'fixture',?,1,4,'{}')",rusqlite::params![epoch,run.as_str(),"c".repeat(64)]).unwrap();
    let root = home.path.join("history-proof");
    std::fs::create_dir(&root).unwrap();
    std::fs::create_dir(root.join("sessions")).unwrap();
    let path = root.join(format!("sessions/rollout-fixture-{SESSION}.jsonl"));
    let meta = json!({"type":"session_meta","payload":{"id":SESSION}});
    let user = |turn: &str, text: &str| json!({"type":"response_item","payload":{"type":"message","role":"user","internal_chat_message_metadata_passthrough":{"turn_id":turn},"content":[{"type":"input_text","text":text}]}});
    let write = |records: Vec<Value>| {
        std::fs::write(
            &path,
            records
                .iter()
                .map(|record| format!("{record}\n"))
                .collect::<String>(),
        )
        .unwrap()
    };
    write(vec![meta.clone(), user("task-turn", original)]);
    let seal = crate::continuity::seal(HarnessId::Codex, &root, SESSION).unwrap();
    assert!(
        task_proof(&store, &run, "history-target", &seal)
            .unwrap()
            .is_none()
    );
    let marker = json!({"epoch":epoch,"source_attempt_id":"history-source","target_attempt_id":"history-target","session":SESSION,"turn":"control-turn","input_sha256":control_hash});
    store.conn.execute("INSERT INTO events(agent_id,attempt_id,at,kind,data_json) VALUES(?,'history-target',5,'maintenance_resume_confirmed_v1',?)",rusqlite::params![run.as_str(),marker.to_string()]).unwrap();
    assert!(
        task_proof(&store, &run, "history-target", &seal)
            .unwrap()
            .is_none()
    );
    write(vec![
        meta.clone(),
        user("task-turn", original),
        user("wrong-turn", control),
    ]);
    let wrong = crate::continuity::seal(HarnessId::Codex, &root, SESSION).unwrap();
    assert!(
        task_proof(&store, &run, "history-target", &wrong)
            .unwrap()
            .is_none()
    );
    write(vec![
        meta.clone(),
        user("task-turn", original),
        user("control-turn", control),
    ]);
    let correct = crate::continuity::seal(HarnessId::Codex, &root, SESSION).unwrap();
    assert!(
        task_proof(&store, &run, "history-target", &correct)
            .unwrap()
            .is_some()
    );
    write(vec![
        meta.clone(),
        user("task-turn", original),
        user("control-turn", control),
        user("control-turn", control),
    ]);
    assert!(task_proof(&store, &run, "history-target", &correct).is_err());
    let repeated = crate::continuity::seal(HarnessId::Codex, &root, SESSION).unwrap();
    assert!(task_proof(&store, &run, "history-target", &repeated).is_err());
}

/// A live supervisor can honor explicit cancellation after preparation fails,
/// but an unpaired discovery start must remain held for inspection.
#[tokio::test]
async fn maintenance_unstarted_cancel_requires_settled_discovery() {
    for discovery_started in [false, true] {
        let home = common::Home::new();
        let mut store = home.store();
        let mut request = home.request();
        request.explicit_finish = true;
        let (run, _) = store
            .admit(
                &request,
                &home.config,
                &json!({"provider_identity_version":2,"authority":{"harness":"claude-code"}}),
                None,
            )
            .unwrap();
        let epoch = store
            .begin_maintenance_fence("unstarted-cancel", 1.0)
            .unwrap()
            .epoch;
        store.defer_maintenance_fence(epoch).unwrap();
        store.conn.execute("INSERT INTO attempts(id,agent_id,number,state,phase,adapter_state_json,created_at,finished_at,ownership_active,selected_account_id) VALUES('cancel-source',?,1,'maintenance_paused','cleanup_complete','{}',1,2,0,'fixture')",[run.as_str()]).unwrap();
        store.conn.execute("INSERT INTO attempts(id,agent_id,number,state,phase,adapter_state_json,created_at,ownership_active,selected_account_id) VALUES('cancel-target',?,2,'prepared','prepared','{}',2,1,'fixture')",[run.as_str()]).unwrap();
        store.conn.execute("INSERT INTO maintenance_resumes(epoch,agent_id,request_id,source_attempt_id,target_attempt_id,target_number,account_id,source_proof_sha256,writer_epoch,claimed_at,preflight_json) VALUES(?,?,'cancel-resume','cancel-source','cancel-target',2,'fixture',?,1,2,'{}')",rusqlite::params![epoch,run.as_str(),"a".repeat(64)]).unwrap();
        let marker = json!({"epoch":epoch,"source_attempt_id":"cancel-source","target_attempt_id":"cancel-target"}).to_string();
        store.conn.execute("INSERT INTO events(agent_id,attempt_id,at,kind,data_json) VALUES(?,'cancel-target',2,'maintenance_preparation_guard_v1',?)",rusqlite::params![run.as_str(),marker]).unwrap();
        if discovery_started {
            store.conn.execute("INSERT INTO events(agent_id,attempt_id,at,kind,data_json) VALUES(?,'cancel-target',3,'maintenance_discovery_started_v1',?)",rusqlite::params![run.as_str(),marker]).unwrap();
        }
        let prep = crate::writer_test_harness::prepare_supervisor(&mut store, &run);
        let harness = crate::writer_test_harness::Harness::start(&home.path);
        let mut persist = harness.supervisor(&home.path, prep, &run).await;
        harness
            .broker()
            .lane("unstarted-cancel")
            .remote(
                agent_run_store::ops::EnqueueCommand {
                    execution: run.clone(),
                    kind: "cancel".into(),
                    payload: json!({}),
                },
                Duration::from_secs(5),
            )
            .await
            .unwrap();
        let result = tokio::time::timeout(
            Duration::from_millis(if discovery_started { 1400 } else { 10000 }),
            maintenance::hold_unstarted(&run, &mut persist),
        )
        .await;
        if discovery_started {
            assert!(result.is_err());
            assert!(!store.get(&run).unwrap().status.terminal());
        } else {
            result.expect("cancellation remained stuck").unwrap();
            assert_eq!(store.get(&run).unwrap().status, Status::Cancelled);
        }
    }
}

/// One still-owned closing attempt with exact synthetic native evidence.
struct Fixture {
    home: common::Home,
    store: agent_run_store::Store<'static>,
    run: AgentId,
    epoch: i64,
    event: i64,
    identity: ProviderLaunchIdentity,
    history_root: PathBuf,
    history: PathBuf,
    agent_dir: PathBuf,
    process: Process,
}

impl Fixture {
    /// Prepare the same account-bound attempt and sealed original task for each case.
    fn new() -> Self {
        let home = common::Home::new();
        let mut store = home.store();
        let account=serde_json::from_value(json!({"account_id":ACCOUNT,"auth_family":"openai","secret_ref":"env:FIXTURE_ONLY","status":"enabled"})).unwrap();
        store.register_account(&account).unwrap();
        let config_root = home.path.join("provider-config");
        std::fs::create_dir(&config_root).unwrap();
        std::fs::create_dir(config_root.join("profiles")).unwrap();
        std::fs::write(config_root.join("profiles/review.md"),"+++\nrevision = \"fixture\"\nwrite = false\nnetwork = false\nallow_external_read_roots = false\nskills = []\nmcp = []\nrequired_constraints = []\n+++\nReview the fixture.\n").unwrap();
        std::fs::write(config_root.join("config.toml"),format!("schema_version=2\n[harnesses.codex]\nbinary='/bin/true'\nhome='{}'\n[harnesses.claude-code]\nbinary='/bin/true'\nhome='{}'\n[providers.codex]\nharness='codex'\nconnection={{kind='native'}}\nauth_family='openai'\nlimits_source='none'\n[[providers.codex.models]]\nid='fixture'\n[[providers.codex.bindings]]\nlabel='fixture'\naccount='{ACCOUNT}'\n",home.path.join("native").display(),home.path.join("native-claude").display())).unwrap();
        let (config, config_sha) =
            agent_run_config::provider_config::ProviderConfig::load(&config_root).unwrap();
        let catalog = config
            .resolve_catalog(store.list_accounts().unwrap())
            .unwrap();
        let mut role = common::role_payload();
        role.as_object_mut().unwrap().remove("config_revision");
        role["worker_mcp"] = json!(true);
        role["config_revision"] = json!(agent_run_domain::canonical::sha256_hex(&role, true));
        let provider_request:ProviderStartRequest=serde_json::from_value(json!({"provider":"codex","model":"fixture","profile":"review","task":"fixture task","workdir":home.path,"request_id":"finalization-source","explicit_finish":true})).unwrap();
        let authority = ResolvedLaunchAuthority {
            provider: provider_request.provider.clone(),
            harness: HarnessId::Codex,
            connection: catalog
                .provider(&provider_request.provider)
                .unwrap()
                .connection
                .clone(),
            model: provider_request.model.clone(),
            effort: None,
            profile: provider_request.profile.clone(),
            workdir: home.path.clone(),
            role_payload: role,
            assets_sha256: "c".repeat(64).parse().unwrap(),
            eligible_accounts: vec![ACCOUNT.parse().unwrap()],
        };
        let history_root = home.path.join("native-history");
        std::fs::create_dir(&history_root).unwrap();
        std::fs::create_dir(history_root.join("sessions")).unwrap();
        let history = history_root.join(format!("sessions/rollout-fixture-{SESSION}.jsonl"));
        let history_rows = [
            json!({"type":"session_meta","payload":{"id":SESSION}}),
            json!({"type":"response_item","payload":{"type":"message","role":"user","internal_chat_message_metadata_passthrough":{"turn_id":"original-turn"},"content":[{"type":"input_text","text":"fixture task"}]}}),
        ];
        std::fs::write(
            &history,
            format!(
                "{}\n",
                history_rows
                    .iter()
                    .map(Value::to_string)
                    .collect::<Vec<_>>()
                    .join("\n")
            ),
        )
        .unwrap();
        let identity = ProviderLaunchIdentity {
            provider_identity_version: 2,
            replay_request_sha256: agent_run_domain::canonical::sha256_hex(
                &serde_json::to_value(&provider_request).unwrap(),
                true,
            ),
            provider_request,
            provider_config_sha256: config_sha,
            provider_config_snapshot: config.snapshot().unwrap(),
            provider_config: config,
            authority,
            system_route: None,
            runtime_home: Some(history_root.clone()),
            snapshot_sha256: Some("c".repeat(64)),
        };
        let mut request = home.request();
        request.explicit_finish = true;
        let (run, _) = store
            .admit(
                &request,
                &home.config,
                &serde_json::to_value(&identity).unwrap(),
                None,
            )
            .unwrap();
        let process = Process::spawn(&crate::adapters::LaunchPlan {
            binary: "/bin/cat".into(),
            args: vec![],
            cwd: home.path.clone(),
            environment: BTreeMap::new(),
            initial_input: None,
        })
        .unwrap();
        let leader = process
            .owner
            .leader
            .as_ref()
            .expect("owned fixture process");
        store.conn.execute("UPDATE agents SET status='running',sequence=3,runtime_session_id=?,process_group_id=? WHERE id=?",rusqlite::params![SESSION,process.owner.pid,run.as_str()]).unwrap();
        store.conn.execute("INSERT INTO attempts(id,agent_id,number,state,phase,adapter_state_json,created_at,ownership_active,selected_account_id,process_identity,process_birth_time) VALUES(?,?,1,'running','running','{}',1,1,?,?,?)",rusqlite::params![ATTEMPT,run.as_str(),ACCOUNT,leader.token,leader.birth]).unwrap();
        store
            .issue_worker_capability(&run, ATTEMPT, TOKEN, 1.0)
            .unwrap();
        store.begin_worker_lifecycle(&run, ATTEMPT, 1.0).unwrap();
        store.conn.execute("UPDATE worker_capabilities SET pool_catalog_version=5,pool_catalog_digest=?,pool_catalog_observed_at=1 WHERE attempt_id=?",rusqlite::params![maintenance_pause_catalog_digest(false),ATTEMPT]).unwrap();
        let epoch = store
            .begin_maintenance_fence("supervisor-finalization", 2.0)
            .unwrap()
            .epoch;
        let command = store
            .request_maintenance_steer(epoch, &run, ATTEMPT, CHALLENGE, 3.0)
            .unwrap()
            .command_id;
        store.conn.execute("UPDATE commands SET state='completed',claimed_at=3,completed_at=3,result_json='{\"accepted\":true,\"observation\":\"native_accepted\"}' WHERE id=?",[command]).unwrap();
        let input = MaintenancePauseRequest {
            challenge_id: CHALLENGE.into(),
            checkpoint: "Retained work context.".into(),
        };
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
                CHALLENGE,
                &receipt.sha256,
            )
            .unwrap();
        let initial = json!({"turn":"original-turn","input_sha256":agent_run_platform::fs::sha256(b"fixture task")});
        store.conn.execute("INSERT INTO events(agent_id,attempt_id,at,kind,data_json) VALUES(?,?,4,'native_turn_started',?)",rusqlite::params![run.as_str(),ATTEMPT,initial.to_string()]).unwrap();
        let evidence = NativeSettlement {
            version: 1,
            protocol: "codex".into(),
            coverage: "owned_foreground_v1".into(),
            maintenance_epoch: epoch,
            agent_id: run.clone(),
            attempt_id: ATTEMPT.into(),
            challenge_id: CHALLENGE.into(),
            receipt_sha256: receipt.sha256,
            thread_id: SESSION.into(),
            turn_id: "pause-turn".into(),
            item_id: "pause-item".into(),
        };
        store.conn.execute("INSERT INTO events(agent_id,attempt_id,at,kind,data_json) VALUES(?,?,5,'maintenance_native_settled_v1',?)",rusqlite::params![run.as_str(),ATTEMPT,serde_json::to_string(&evidence).unwrap()]).unwrap();
        let event = store.conn.last_insert_rowid();
        store
            .begin_maintenance_closing(epoch, &run, ATTEMPT, event, 5.5)
            .unwrap();
        let agent_dir = home.path.join("agents").join(run.as_str());
        agent_run_platform::fs::private_dir(&agent_dir).unwrap();
        Self {
            home,
            store,
            run,
            epoch,
            event,
            identity,
            history_root,
            history,
            agent_dir,
            process,
        }
    }

    /// The finalizer must never synthesize success or release the reservation in a hold.
    fn assert_held(&self) {
        assert_eq!(self.store.get(&self.run).unwrap().status, Status::Running);
        assert!(
            self.store
                .maintenance_park(self.epoch, &self.run)
                .unwrap()
                .is_none()
        );
        let row: (bool, Option<f64>, String) = self
            .store
            .conn
            .query_row(
                "SELECT ownership_active,finished_at,selected_account_id FROM attempts WHERE id=?",
                [ATTEMPT],
                |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
            )
            .unwrap();
        assert_eq!(row, (true, None, ACCOUNT.into()));
        assert!(
            self.store
                .worker_finish_intent(&self.run, false)
                .unwrap()
                .is_none()
        );
    }
}

/// Park only after actual child cleanup and a verified same-session task seal;
/// cancellation and three failure boundaries retain their distinct outcomes.
#[tokio::test]
async fn maintenance_finalizer_preserves_work_or_holds_without_false_terminal() {
    for scenario in [
        "success",
        "park_ack_unknown",
        "cancel",
        "receipt_cancel",
        "history",
        "storage",
        "cleanup",
    ] {
        let mut f = Fixture::new();
        if scenario == "cancel" {
            f.store.enqueue(&f.run, "cancel", &json!({})).unwrap();
        }
        if scenario == "history" {
            std::fs::remove_file(&f.history).unwrap();
        }
        if scenario == "receipt_cancel" {
            std::fs::remove_dir(&f.agent_dir).unwrap();
        }
        if scenario == "storage" {
            f.store.conn.execute_batch("CREATE TRIGGER fixture_refuse_cleanup BEFORE UPDATE OF cleanup_proof_json ON attempts BEGIN SELECT RAISE(ABORT,'fixture cleanup refusal'); END;").unwrap();
        }
        let original_owner = if scenario == "cleanup" {
            Some(std::mem::replace(
                &mut f.process.owner,
                agent_run_platform::process::OwnedProcess::capture(i32::MAX),
            ))
        } else {
            None
        };
        let unknown = scenario == "park_ack_unknown";
        let capability = "f".repeat(64);
        let prep = if unknown {
            let me = agent_run_platform::process::inspect(std::process::id() as i32).unwrap();
            f.store.conn.execute("UPDATE agents SET supervisor_capability_sha256=?,supervisor_pid=?,supervisor_identity=?,supervisor_birth_time=? WHERE id=?",rusqlite::params![agent_run_platform::fs::sha256(capability.as_bytes()),me.pid,me.token,me.birth,f.run.as_str()]).unwrap();
            None
        } else {
            Some(crate::writer_test_harness::prepare_supervisor(
                &mut f.store,
                &f.run,
            ))
        };
        let harness = crate::writer_test_harness::Harness::start(&f.home.path);
        let fault_count = Arc::new(AtomicUsize::new(0));
        let mut persist = if unknown {
            let logical = f.store.get(&f.run).unwrap().root_agent_id;
            let producer = crate::persist::Producer::with_dial(
                LostParkAck {
                    inner: harness.dial(),
                    seen: fault_count.clone(),
                },
                crate::persist::supervisor_hello(&logical, &f.run, capability),
                crate::persist::supervisor_producer_id(&f.run),
            );
            let mut persist = Persist::open(&f.home.path, &f.run, producer).unwrap();
            persist.repin(ATTEMPT).await.unwrap();
            persist
        } else {
            harness
                .supervisor(&f.home.path, prep.unwrap(), &f.run)
                .await
        };
        let environment = BTreeMap::new();
        let context = PauseContext {
            id: &f.run,
            attempt: ATTEMPT,
            epoch: f.epoch,
            native_event_seq: f.event,
            agent_dir: &f.agent_dir,
            identity: &f.identity,
            environment: &environment,
            continuation_root: Some(&f.history_root),
        };
        let mut finalization = Box::pin(maintenance::finalize_pause(
            context,
            &mut persist,
            &mut f.process,
        ));
        if matches!(scenario, "success" | "park_ack_unknown" | "cancel") {
            let result = tokio::time::timeout(Duration::from_secs(12), &mut finalization)
                .await
                .expect("finalizer timeout");
            assert!(result.is_ok(), "{scenario}");
        } else {
            tokio::time::timeout(Duration::from_secs(12),async {
                loop {
                    let held:bool=f.store.conn.query_row("SELECT EXISTS(SELECT 1 FROM events WHERE agent_id=? AND kind='maintenance_held_v1')",[f.run.as_str()],|r|r.get(0)).unwrap();
                    if held {break;}
                    tokio::select! {
                        result=&mut finalization=>panic!("unexpected terminal finalizer result: {}",result.is_ok()),
                        _=tokio::time::sleep(Duration::from_millis(10))=>{}
                    }
                }
            }).await.expect("hold was not observed");
            if scenario == "receipt_cancel" {
                agent_run_platform::fs::private_dir(&f.agent_dir).unwrap();
                let original = f.agent_dir.join(format!("recovery.{ATTEMPT}.json"));
                std::fs::write(&original, b"{\"marker\":\"original\"}").unwrap();
                harness
                    .broker()
                    .lane("maintenance-finalizer-cancel")
                    .remote(
                        agent_run_store::ops::EnqueueCommand {
                            execution: f.run.clone(),
                            kind: "cancel".into(),
                            payload: json!({}),
                        },
                        Duration::from_secs(5),
                    )
                    .await
                    .unwrap();
                let result = tokio::time::timeout(Duration::from_secs(12), &mut finalization)
                    .await
                    .expect("held cancel timed out");
                assert!(result.is_ok());
                assert_eq!(
                    std::fs::read(&original).unwrap(),
                    b"{\"marker\":\"original\"}"
                );
            }
        }
        drop(finalization);
        if let Some(owner) = original_owner {
            f.process.owner = owner;
            let cleanup = f
                .process
                .owner
                .cleanup(Duration::from_secs(2))
                .await
                .unwrap();
            assert!(cleanup.confirmed);
            f.process.reap().await;
        }
        match scenario {
            "success" | "park_ack_unknown" => {
                let park = f.store.maintenance_park(f.epoch, &f.run).unwrap().unwrap();
                assert_eq!(park.attempt_id, ATTEMPT);
                assert_eq!(park.native_event_seq, f.event);
                assert_eq!(f.store.get(&f.run).unwrap().status, Status::Running);
                assert_eq!(f.store.get(&f.run).unwrap().sequence, 3);
                assert_eq!(f.store.conn.query_row("SELECT selected_account_id FROM attempts WHERE id=? AND state='maintenance_parked' AND ownership_active=1",[ATTEMPT],|r|r.get::<_,String>(0)).unwrap(),ACCOUNT);
                assert!(
                    f.store
                        .parked_maintenance_checkpoint(f.epoch, &f.run, ATTEMPT)
                        .unwrap()
                        .is_some()
                );
                if unknown {
                    assert_eq!(fault_count.load(Ordering::SeqCst), 2);
                    assert_eq!(f.store.conn.query_row("SELECT COUNT(*) FROM operation_receipts WHERE op_kind='park_maintenance_attempt' AND outcome='committed'",[],|r|r.get::<_,i64>(0)).unwrap(),1);
                }
            }
            "cancel" | "receipt_cancel" => {
                assert_eq!(f.store.get(&f.run).unwrap().status, Status::Cancelled);
                assert!(f.store.maintenance_park(f.epoch, &f.run).unwrap().is_none());
                if scenario == "receipt_cancel" {
                    let original = f.agent_dir.join(format!("recovery.{ATTEMPT}.json"));
                    let cancellation = f
                        .agent_dir
                        .join(format!("recovery.{ATTEMPT}.maintenance-cancel.json"));
                    assert!(original.exists() && cancellation.exists());
                    // This disposable fixture supplies the compacted summary
                    // normally written by the owned process-observation path.
                    f.store.conn.execute("INSERT INTO events(agent_id,attempt_id,at,kind,data_json) VALUES(?,?,1,'process_summary_v1','{}')",rusqlite::params![f.run.as_str(),ATTEMPT]).unwrap();
                    assert_eq!(
                        crate::housekeeping::compact_process_artifacts(
                            &f.home.path,
                            &f.store.conn,
                            f.run.as_str(),
                            10
                        )
                        .unwrap(),
                        2
                    );
                    assert!(!original.exists() && !cancellation.exists());
                }
            }
            _ => f.assert_held(),
        }
    }
}
