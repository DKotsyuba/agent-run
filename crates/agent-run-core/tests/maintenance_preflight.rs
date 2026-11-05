//! Worker-side private-file proof with real sealed fixture files, no model calls.
mod common;
use agent_run_core::{
    continuity,
    service::{
        ProviderLaunchIdentity, system_maintenance_resume_preflight, system_resume_preflight,
    },
    system_launch::{MaintenancePreflightRequest, ResumePreflightFailure, SystemRoute},
};
use agent_run_domain::{
    PositiveFinite, ProviderStartRequest,
    catalog::{
        HarnessId, PhysicalQuotaKey, QuotaCandidate, QuotaCandidateSet, ResolvedLaunchAuthority,
        SelectionIntent,
    },
    worker::{FinishGuard, MaintenancePauseRequest, maintenance_pause_catalog_digest},
};
use agent_run_store::{Store, maintenance_park::NativeSettlement};
use serde_json::json;

const SESSION: &str = "fixture-native-session";
const TOKEN: &str = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";

/// A synthetic parked execution backed by genuine runtime-index/history seals.
struct Fixture {
    home: tempfile::TempDir,
    store: Store<'static>,
    route: SystemRoute,
    request: MaintenancePreflightRequest,
    history: std::path::PathBuf,
    asset: std::path::PathBuf,
    history_sha: String,
}

impl Fixture {
    /// Build all files under a disposable account directory. The old supervisor
    /// is either this live process or a provably different historical generation.
    fn new(alive: bool) -> Self {
        Self::with_mode(alive, true)
    }

    /// Prepare the same preserved source in account-worker or ordinary local mode.
    fn with_mode(alive: bool, system: bool) -> Self {
        let home = tempfile::tempdir().unwrap();
        let root = home.path().canonicalize().unwrap();
        let mut store = agent_run_store::fixture::initialize(&root).unwrap();
        std::fs::create_dir(root.join("profiles")).unwrap();
        std::fs::write(root.join("profiles/review.md"),"+++\nrevision = \"fixture\"\nwrite = false\nnetwork = false\nallow_external_read_roots = false\nskills = []\nmcp = []\nrequired_constraints = []\n+++\nReview the fixture.\n").unwrap();
        std::fs::write(root.join("config.toml"),format!("schema_version=2\n[harnesses.codex]\nbinary='/bin/true'\nhome='{}'\n[harnesses.claude-code]\nbinary='/bin/true'\nhome='{}'\n[providers.codex]\nharness='codex'\nconnection={{kind='native'}}\nauth_family='openai'\nlimits_source='none'\n[[providers.codex.models]]\nid='fixture'\n[[providers.codex.bindings]]\nlabel='fixture'\naccount='acct-fixture'\n",root.join("native").display(),root.join("native-claude").display())).unwrap();
        let (config, config_sha) =
            agent_run_config::provider_config::ProviderConfig::load(&root).unwrap();
        let account=serde_json::from_value(json!({"account_id":"acct-fixture","auth_family":"openai","secret_ref":"env:FIXTURE_NATIVE","status":"enabled"})).unwrap();
        store.register_account(&account).unwrap();
        let catalog = config
            .resolve_catalog(store.list_accounts().unwrap())
            .unwrap();
        let private = root.join("private-account");
        std::fs::create_dir(&private).unwrap();
        let runtime = private.join("runtime");
        let mut publisher = agent_run_adapters::materialize::Publisher::new(&runtime).unwrap();
        publisher
            .file("settings", b"fixture settings", 0o600)
            .unwrap();
        let (_, assets) = publisher.finish().unwrap();
        let asset = runtime.join("settings");
        std::fs::create_dir(runtime.join("sessions")).unwrap();
        let history = runtime.join(format!("sessions/rollout-fixture-{SESSION}.jsonl"));
        let text = "fixture task";
        let input_hash = agent_run_platform::fs::sha256(text.as_bytes());
        let rows = [
            json!({"type":"session_meta","payload":{"id":SESSION}}),
            json!({"type":"response_item","payload":{"type":"message","role":"user","internal_chat_message_metadata_passthrough":{"turn_id":"original-turn"},"content":[{"type":"input_text","text":text}]}}),
        ];
        std::fs::write(
            &history,
            format!(
                "{}\n",
                rows.iter()
                    .map(|row| row.to_string())
                    .collect::<Vec<_>>()
                    .join("\n")
            ),
        )
        .unwrap();
        let seal = continuity::seal(HarnessId::Codex, &runtime, SESSION).unwrap();
        assert!(continuity::admitted_turn_recorded(&seal, "original-turn", &input_hash).unwrap());
        let route = SystemRoute {
            provider: "codex".parse().unwrap(),
            harness: HarnessId::Codex,
            state_home: root.clone(),
            private_home: private,
            operator_socket: root.join("operator.sock"),
            writer_socket: root.join("writer.sock"),
            worker_socket: root.join("worker.sock"),
            fingerprint: "d".repeat(64).parse().unwrap(),
        };
        let request:ProviderStartRequest=serde_json::from_value(json!({"provider":"codex","model":"fixture","profile":"review","task":text,"workdir":root,"request_id":"preflight-source","explicit_finish":true})).unwrap();
        let mut role = common::role_payload();
        role.as_object_mut().unwrap().remove("config_revision");
        role["worker_mcp"] = json!(true);
        role["config_revision"] = json!(agent_run_domain::canonical::sha256_hex(&role, true));
        let account_id = "acct-fixture".parse().unwrap();
        let authority = ResolvedLaunchAuthority {
            provider: request.provider.clone(),
            harness: HarnessId::Codex,
            connection: catalog
                .provider(&request.provider)
                .unwrap()
                .connection
                .clone(),
            model: request.model.clone(),
            effort: None,
            profile: request.profile.clone(),
            workdir: root.clone(),
            role_payload: role,
            assets_sha256: assets.parse().unwrap(),
            eligible_accounts: vec![account_id],
        };
        let frozen = ProviderLaunchIdentity {
            provider_identity_version: 2,
            replay_request_sha256: agent_run_domain::canonical::sha256_hex(
                &serde_json::to_value(&request).unwrap(),
                true,
            ),
            provider_request: request.clone(),
            provider_config_sha256: config_sha,
            provider_config_snapshot: config.snapshot().unwrap(),
            provider_config: config,
            authority: authority.clone(),
            system_route: system.then(|| route.clone()),
            runtime_home: Some(runtime),
            snapshot_sha256: Some(assets),
        };
        let candidates = QuotaCandidateSet {
            provider: request.provider.clone(),
            model: request.model.clone(),
            intent: SelectionIntent::Auto,
            candidates: vec![QuotaCandidate {
                account: "acct-fixture".parse().unwrap(),
                rank: 0,
                physical_keys: vec![
                    PhysicalQuotaKey::new(&"acct-fixture".parse().unwrap(), "tokens").unwrap(),
                ],
                multiplier: PositiveFinite::try_from(1.0).unwrap(),
                quota_known: true,
            }],
            capacity_revision: store.quota_capacity_revision().unwrap(),
        };
        let admitted = store
            .admit_provider(
                &request,
                &request.storage_projection(),
                &catalog,
                &authority,
                &candidates,
                &serde_json::to_value(&frozen).unwrap(),
                8,
                None,
                None,
            )
            .unwrap();
        let run = admitted.agent_id;
        let attempt = admitted.attempt_id;
        let me = agent_run_platform::process::inspect(std::process::id() as i32).unwrap();
        let token = if alive {
            me.token
        } else {
            format!("{}-old", me.token)
        };
        store.conn.execute("UPDATE agents SET status='running',runtime_session_id=?,process_group_id=12345,supervisor_pid=?,supervisor_identity=?,supervisor_birth_time=? WHERE id=?",rusqlite::params![SESSION,me.pid,token,me.birth,run.as_str()]).unwrap();
        store
            .conn
            .execute("UPDATE attempts SET state='running' WHERE id=?", [&attempt])
            .unwrap();
        ProviderLaunchIdentity::read(&store.get(&run).unwrap()).unwrap();
        store
            .issue_worker_capability(&run, &attempt, TOKEN, 1.0)
            .unwrap();
        store.begin_worker_lifecycle(&run, &attempt, 1.0).unwrap();
        store.conn.execute("UPDATE worker_capabilities SET pool_catalog_version=5,pool_catalog_digest=?,pool_catalog_observed_at=1 WHERE attempt_id=?",rusqlite::params![maintenance_pause_catalog_digest(false),attempt]).unwrap();
        let epoch = store
            .begin_maintenance_fence("preflight", 2.0)
            .unwrap()
            .epoch;
        let input = MaintenancePauseRequest {
            challenge_id: "b".repeat(64),
            checkpoint: "Retained fixture context.".into(),
        };
        let command = store
            .request_maintenance_steer(epoch, &run, &attempt, &input.challenge_id, 3.0)
            .unwrap()
            .command_id;
        store.conn.execute("UPDATE commands SET state='completed',completed_at=3,result_json='{\"accepted\":true,\"observation\":\"native_accepted\"}' WHERE id=?",[command]).unwrap();
        let receipt = store
            .accept_maintenance_checkpoint(
                epoch,
                &run,
                &attempt,
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
                &attempt,
                &input.challenge_id,
                &receipt.sha256,
            )
            .unwrap();
        let evidence = NativeSettlement {
            version: 1,
            protocol: "codex".into(),
            coverage: "owned_foreground_v1".into(),
            maintenance_epoch: epoch,
            agent_id: run.clone(),
            attempt_id: attempt.clone(),
            challenge_id: input.challenge_id,
            receipt_sha256: receipt.sha256,
            thread_id: SESSION.into(),
            turn_id: "pause-turn".into(),
            item_id: "pause-item".into(),
        };
        store.conn.execute("INSERT INTO events(agent_id,attempt_id,at,kind,data_json) VALUES(?,?,5,'maintenance_native_settled_v1',?)",rusqlite::params![run.as_str(),attempt,serde_json::to_string(&evidence).unwrap()]).unwrap();
        let event = store.conn.last_insert_rowid();
        store
            .begin_maintenance_closing(epoch, &run, &attempt, event, 5.5)
            .unwrap();
        let cleanup = json!({"signals":[],"scope":"verified_descendants","group_gone":true,"descendants_gone":true,"confirmed":true,"process_group_id":12345});
        let history_state = json!({"native_history":{"attempt":attempt,"provider":"codex","assets_sha256":frozen.authority.assets_sha256,"task_proven":true,"task_proof":{"turn":"original-turn","input_sha256":input_hash},"seal":seal}});
        store.conn.execute("UPDATE attempts SET phase='cleanup_complete',cleanup_proof_json=?,adapter_state_json=? WHERE id=?",rusqlite::params![cleanup.to_string(),history_state.to_string(),attempt]).unwrap();
        let park = store
            .park_maintenance_attempt(epoch, &run, &attempt, event, 6.0)
            .unwrap();
        store.defer_maintenance_fence(epoch).unwrap();
        Self {
            home,
            store,
            route,
            request: MaintenancePreflightRequest {
                epoch,
                execution: run,
                source_attempt_id: attempt,
                source_proof_sha256: park.proof_sha256,
            },
            history,
            asset,
            history_sha: seal.sha256,
        }
    }

    /// Call only the read-only account-worker proof function.
    fn preflight(
        &self,
    ) -> Result<agent_run_core::system_launch::MaintenancePreflightResult, ResumePreflightFailure>
    {
        system_maintenance_resume_preflight(
            self.home.path(),
            &self.request,
            &self.route.worker_socket,
            self.route.fingerprint.as_str(),
        )
    }
}

/// Real private asset/history hashes are verified; the reply contains no paths
/// or checkpoint, and the ordinary terminal-only preflight remains terminal-only.
#[test]
fn maintenance_private_preflight_verifies_files_without_changing_store() {
    let f = Fixture::new(false);
    let before = serde_json::to_value(f.store.get(&f.request.execution).unwrap()).unwrap();
    let result = f.preflight().unwrap();
    assert_eq!(result.verified_history_sha256, f.history_sha);
    assert_eq!(result.source_proof_sha256, f.request.source_proof_sha256);
    assert_eq!(
        result.supervisor_state,
        agent_run_platform::process::ProcessState::Reused
    );
    let text = serde_json::to_string(&result).unwrap();
    assert!(!text.contains(&f.home.path().to_string_lossy().to_string()));
    assert!(!text.contains("Retained fixture context"));
    assert_eq!(
        serde_json::to_value(f.store.get(&f.request.execution).unwrap()).unwrap(),
        before
    );
    assert_eq!(
        system_resume_preflight(
            f.home.path(),
            &f.request.execution,
            &f.route.worker_socket,
            f.route.fingerprint.as_str()
        ),
        Err(ResumePreflightFailure::ParentChanged)
    );
}

/// A running old supervisor cannot be replaced merely because SQL says parked.
#[test]
fn maintenance_private_preflight_requires_real_os_death() {
    assert_eq!(
        Fixture::new(true).preflight(),
        Err(ResumePreflightFailure::ParentChanged)
    );
}

/// Corrupted native history and runtime assets are classified without leaking
/// private paths and never resealed or repaired by the preflight.
#[test]
fn maintenance_private_preflight_refuses_changed_private_files() {
    let f = Fixture::new(false);
    std::fs::write(&f.history, "changed history").unwrap();
    assert_eq!(
        f.preflight(),
        Err(ResumePreflightFailure::HistoryUnverifiable)
    );
    let f = Fixture::new(false);
    std::fs::write(&f.asset, "changed settings").unwrap();
    assert_eq!(
        f.preflight(),
        Err(ResumePreflightFailure::AssetsUnverifiable)
    );
}

/// Source/route identities cannot be redirected to another account or record.
#[test]
fn maintenance_private_preflight_refuses_foreign_source_and_route() {
    let mut f = Fixture::new(false);
    assert_eq!(
        system_maintenance_resume_preflight(
            f.home.path(),
            &f.request,
            &f.route.worker_socket,
            &"0".repeat(64)
        ),
        Err(ResumePreflightFailure::RouteChanged)
    );
    f.request.source_attempt_id = "foreign".into();
    assert_eq!(f.preflight(), Err(ResumePreflightFailure::ParentChanged));
    let mut f = Fixture::new(false);
    f.request.source_proof_sha256 = "0".repeat(64);
    assert_eq!(f.preflight(), Err(ResumePreflightFailure::ParentChanged));
}

/// Delegate private proof under the fixture account while refusing every launch.
struct AccountWorker {
    route: SystemRoute,
    calls: std::sync::atomic::AtomicUsize,
}

impl agent_run_core::system_launch::SystemLauncher for AccountWorker {
    fn route(
        &self,
        _: &agent_run_domain::catalog::ProviderId,
        _: HarnessId,
    ) -> agent_run_domain::Result<SystemRoute> {
        Ok(self.route.clone())
    }

    fn preflight_maintenance_resume(
        &self,
        request: MaintenancePreflightRequest,
        route: SystemRoute,
    ) -> std::pin::Pin<
        Box<
            dyn std::future::Future<
                    Output = agent_run_domain::Result<
                        agent_run_core::system_launch::MaintenancePreflightResult,
                    >,
                > + Send
                + 'static,
        >,
    > {
        self.calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        Box::pin(async move {
            system_maintenance_resume_preflight(
                &route.state_home,
                &request,
                &route.worker_socket,
                route.fingerprint.as_str(),
            )
            .map_err(|reason| agent_run_domain::Error::Unsupported(reason.code().into()))
        })
    }

    fn launch(
        &self,
        _: std::path::PathBuf,
        _: agent_run_domain::domain::AgentId,
        _: SystemRoute,
        _: agent_run_core::broker_writer::BrokerLane,
    ) -> std::pin::Pin<
        Box<dyn std::future::Future<Output = agent_run_domain::Result<()>> + Send + 'static>,
    > {
        panic!("a maintenance allocation is never permission to launch")
    }
}

/// A fixture worker proves private history and records a launch without
/// executing a real provider process.
struct RecordingWorker {
    inner: AccountWorker,
    launches: std::sync::atomic::AtomicUsize,
    failure: Option<bool>,
    child_owner: bool,
    succeed_after_first: bool,
}

impl agent_run_core::system_launch::SystemLauncher for RecordingWorker {
    fn route(
        &self,
        provider: &agent_run_domain::catalog::ProviderId,
        harness: HarnessId,
    ) -> agent_run_domain::Result<SystemRoute> {
        agent_run_core::system_launch::SystemLauncher::route(&self.inner, provider, harness)
    }
    fn preflight_maintenance_resume(
        &self,
        request: MaintenancePreflightRequest,
        route: SystemRoute,
    ) -> std::pin::Pin<
        Box<
            dyn std::future::Future<
                    Output = agent_run_domain::Result<
                        agent_run_core::system_launch::MaintenancePreflightResult,
                    >,
                > + Send
                + 'static,
        >,
    > {
        agent_run_core::system_launch::SystemLauncher::preflight_maintenance_resume(
            &self.inner,
            request,
            route,
        )
    }
    fn launch(
        &self,
        _: std::path::PathBuf,
        id: agent_run_domain::domain::AgentId,
        _: SystemRoute,
        lane: agent_run_core::broker_writer::BrokerLane,
    ) -> std::pin::Pin<
        Box<dyn std::future::Future<Output = agent_run_domain::Result<()>> + Send + 'static>,
    > {
        let previous = self
            .launches
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        let recovered = self.succeed_after_first && previous > 0;
        let failure = if recovered { None } else { self.failure };
        let child_owner = self.child_owner;
        Box::pin(async move {
            if recovered {
                lane.local(
                    agent_run_store::ops::RecordSupervisorLaunch {
                        execution: id.clone(),
                        capability_sha256: "b".repeat(64),
                    },
                    std::time::Duration::from_secs(5),
                )
                .await?;
                lane.local(agent_run_store::ops::RecordStartupOwner {
                    execution:id,
                    identity:json!({"pid":124,"ppid":1,"group":124,"birth":1,"token":"linux:fixture","zombie":false}),
                    system_fence:None,local_capability_sha256:Some("b".repeat(64)),
                },std::time::Duration::from_secs(5)).await?;
                return Ok(());
            }
            if let Some(ambiguous) = failure {
                lane.local(
                    agent_run_store::ops::RecordSupervisorLaunch {
                        execution: id.clone(),
                        capability_sha256: "a".repeat(64),
                    },
                    std::time::Duration::from_secs(5),
                )
                .await?;
                if child_owner {
                    lane.local(
                        agent_run_store::ops::RecordStartupOwner {
                            execution:id,
                            identity:json!({"pid":123,"ppid":1,"group":123,"birth":1,"token":"linux:fixture","zombie":false}),
                            system_fence:None,
                            local_capability_sha256:Some("a".repeat(64)),
                        },
                        std::time::Duration::from_secs(5),
                    ).await?;
                }
                return if ambiguous {
                    Err(agent_run_domain::Error::Runtime("unknown launch".into()))
                } else {
                    Err(agent_run_domain::Error::Io(std::io::Error::other(
                        "fixture spawn refusal",
                    )))
                };
            }
            Ok(())
        })
    }
}

/// Recovery dispatches one exact same-session successor; repeated controller
/// ticks cannot launch it again or release admission before native confirmation.
#[tokio::test]
async fn maintenance_controller_dispatches_once_and_waits_for_confirmation() {
    use agent_run_core::{maintenance_controller, service::Service};
    use std::sync::{Arc, atomic::Ordering};
    let f = Fixture::with_mode(false, true);
    let harness = common::writer::Harness::start(f.home.path());
    let worker = Arc::new(RecordingWorker {
        inner: AccountWorker {
            route: f.route.clone(),
            calls: std::sync::atomic::AtomicUsize::new(0),
        },
        launches: std::sync::atomic::AtomicUsize::new(0),
        failure: None,
        child_owner: false,
        succeed_after_first: false,
    });
    let service = Service::new(f.home.path().into())
        .with_writer(harness.broker())
        .with_system_launcher(worker.clone());
    maintenance_controller::recovery_tick(&service)
        .await
        .unwrap();
    assert_eq!(worker.launches.load(Ordering::SeqCst), 1);
    let allocation = f
        .store
        .maintenance_resume(f.request.epoch, &f.request.execution)
        .unwrap()
        .unwrap();
    assert_eq!(allocation.source_attempt_id, f.request.source_attempt_id);
    assert_eq!(
        f.store
            .get(&f.request.execution)
            .unwrap()
            .runtime_session_id
            .as_deref(),
        Some(SESSION)
    );
    maintenance_controller::recovery_tick(&service)
        .await
        .unwrap();
    assert_eq!(worker.launches.load(Ordering::SeqCst), 1);
    assert!(f.store.active_maintenance_fence().unwrap().is_some());
    f.store
        .conn
        .execute(
            "UPDATE agents SET status='running' WHERE id=?",
            [f.request.execution.as_str()],
        )
        .unwrap();
    f.store
        .conn
        .execute(
            "UPDATE attempts SET state='running' WHERE id=?",
            [&allocation.target_attempt_id],
        )
        .unwrap();
    f.store.conn.execute("INSERT INTO events(agent_id,attempt_id,at,kind,data_json) VALUES(?,?,7,'maintenance_resume_confirmed_v1',?)",rusqlite::params![f.request.execution.as_str(),allocation.target_attempt_id,json!({"epoch":f.request.epoch,"source_attempt_id":allocation.source_attempt_id,"target_attempt_id":allocation.target_attempt_id,"session":SESSION,"turn":"resume-turn","input_sha256":"d".repeat(64)}).to_string()]).unwrap();
    maintenance_controller::recovery_tick(&service)
        .await
        .unwrap();
    assert!(f.store.active_maintenance_fence().unwrap().is_none());
    assert_eq!(
        f.store
            .maintenance_fence(f.request.epoch)
            .unwrap()
            .unwrap()
            .outcome
            .as_deref(),
        Some("deferred")
    );
}

/// A committed retake whose acknowledgement is lost is observed on the next
/// tick; after the retry limit, the same proof ends the run without a launch.
#[tokio::test]
async fn maintenance_recovers_lost_retake_receipts_without_duplicate_launch() {
    use agent_run_core::{maintenance_controller, service::Service};
    use std::sync::{Arc, atomic::Ordering};
    for count in [1_i64, 2] {
        let f = Fixture::with_mode(false, true);
        let harness = common::writer::Harness::start(f.home.path());
        let worker = Arc::new(RecordingWorker {
            inner: AccountWorker {
                route: f.route.clone(),
                calls: std::sync::atomic::AtomicUsize::new(0),
            },
            launches: std::sync::atomic::AtomicUsize::new(0),
            failure: None,
            child_owner: false,
            succeed_after_first: false,
        });
        let service = Service::new(f.home.path().into())
            .with_writer(harness.broker())
            .with_system_launcher(worker.clone());
        let allocation = service
            .claim_maintenance_resume(
                f.request.epoch,
                &f.request.execution,
                "lost-retake-fixture".into(),
            )
            .await
            .unwrap();
        let lane = harness.broker().lane("lost-retake-fixture");
        for prior in 0..count {
            let cap = format!("{:064x}", prior + 1);
            lane.local(
                agent_run_store::ops::RecordSupervisorLaunch {
                    execution: f.request.execution.clone(),
                    capability_sha256: cap.clone(),
                },
                std::time::Duration::from_secs(5),
            )
            .await
            .unwrap();
            let (owner,birth):(String,f64)=f.store.conn.query_row("SELECT startup_owner_pid_identity,startup_owner_birth_time FROM agents WHERE id=?",[f.request.execution.as_str()],|row|Ok((row.get(0)?,row.get(1)?))).unwrap();
            let at = agent_run_core::domain::now();
            let _lost_receipt = lane
                .local(
                    agent_run_store::ops::ClaimMaintenancePrepared {
                        epoch: f.request.epoch,
                        execution: f.request.execution.clone(),
                        target_attempt_id: allocation.target_attempt_id.clone(),
                        expected_startup_owner: owner,
                        expected_startup_birth_time: birth,
                        expected_capability_sha256: Some(cap),
                        retake_current_writer: Some(true),
                        expected_current_retake_count: Some(prior),
                        owner_state: agent_run_platform::process::ProcessState::Alive,
                        at,
                        startup_deadline_at: at + 30.0,
                    },
                    std::time::Duration::from_secs(5),
                )
                .await
                .unwrap();
        }
        maintenance_controller::recovery_tick(&service)
            .await
            .unwrap();
        let row = f.store.get(&f.request.execution).unwrap();
        if count == 1 {
            assert_eq!(worker.launches.load(Ordering::SeqCst), 1);
            assert_eq!(row.status, agent_run_domain::domain::Status::Starting);
            assert!(f.store.active_maintenance_fence().unwrap().is_some());
        } else {
            assert_eq!(worker.launches.load(Ordering::SeqCst), 0);
            assert_eq!(row.status, agent_run_domain::domain::Status::Failed);
            assert!(f.store.active_maintenance_fence().unwrap().is_none());
        }
    }
}

/// Invalid configuration must not terminate a prepared continuation; after
/// the configuration is restored, the same target can be launched once.
#[tokio::test]
async fn maintenance_holds_unstarted_target_until_config_is_valid() {
    use agent_run_core::{maintenance_controller, service::Service};
    use std::sync::{Arc, atomic::Ordering};
    let f = Fixture::with_mode(false, true);
    let harness = common::writer::Harness::start(f.home.path());
    let worker = Arc::new(RecordingWorker {
        inner: AccountWorker {
            route: f.route.clone(),
            calls: std::sync::atomic::AtomicUsize::new(0),
        },
        launches: std::sync::atomic::AtomicUsize::new(0),
        failure: None,
        child_owner: false,
        succeed_after_first: false,
    });
    let service = Service::new(f.home.path().into())
        .with_writer(harness.broker())
        .with_system_launcher(worker.clone());
    let allocation = service
        .claim_maintenance_resume(
            f.request.epoch,
            &f.request.execution,
            "config-fixture".into(),
        )
        .await
        .unwrap();
    let lane = harness.broker().lane("config-fixture");
    lane.local(
        agent_run_store::ops::RecordSupervisorLaunch {
            execution: f.request.execution.clone(),
            capability_sha256: "a".repeat(64),
        },
        std::time::Duration::from_secs(5),
    )
    .await
    .unwrap();
    let (owner, birth): (String, f64) = f
        .store
        .conn
        .query_row(
            "SELECT startup_owner_pid_identity,startup_owner_birth_time FROM agents WHERE id=?",
            [f.request.execution.as_str()],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .unwrap();
    let at = agent_run_core::domain::now();
    lane.local(
        agent_run_store::ops::ClaimMaintenancePrepared {
            epoch: f.request.epoch,
            execution: f.request.execution.clone(),
            target_attempt_id: allocation.target_attempt_id,
            expected_startup_owner: owner,
            expected_startup_birth_time: birth,
            expected_capability_sha256: Some("a".repeat(64)),
            retake_current_writer: Some(true),
            expected_current_retake_count: Some(0),
            owner_state: agent_run_platform::process::ProcessState::Alive,
            at,
            startup_deadline_at: at + 30.0,
        },
        std::time::Duration::from_secs(5),
    )
    .await
    .unwrap();
    let path = f.home.path().join("config.toml");
    let original = std::fs::read(&path).unwrap();
    std::fs::write(&path, "invalid = [").unwrap();
    maintenance_controller::recovery_tick(&service)
        .await
        .unwrap();
    assert_eq!(
        f.store.get(&f.request.execution).unwrap().status,
        agent_run_domain::domain::Status::Starting
    );
    assert!(f.store.active_maintenance_fence().unwrap().is_some());
    assert_eq!(worker.launches.load(Ordering::SeqCst), 0);
    std::fs::write(path, original).unwrap();
    maintenance_controller::recovery_tick(&service)
        .await
        .unwrap();
    assert_eq!(worker.launches.load(Ordering::SeqCst), 1);
}

/// A definite refusal or twice-unreleased launch reports failure; a launch
/// whose child already owns the handoff remains fenced and is not repeated.
#[tokio::test]
async fn maintenance_launch_failure_distinguishes_refusal_from_unknown() {
    use agent_run_core::{maintenance_controller, service::Service};
    use std::sync::{Arc, atomic::Ordering};
    for case in ["refused", "unreleased", "recovered", "child"] {
        let f = Fixture::with_mode(false, true);
        let harness = common::writer::Harness::start(f.home.path());
        let worker = Arc::new(RecordingWorker {
            inner: AccountWorker {
                route: f.route.clone(),
                calls: std::sync::atomic::AtomicUsize::new(0),
            },
            launches: std::sync::atomic::AtomicUsize::new(0),
            failure: Some(case != "refused"),
            child_owner: case == "child",
            succeed_after_first: case == "recovered",
        });
        let service = Service::new(f.home.path().into())
            .with_writer(harness.broker())
            .with_system_launcher(worker.clone());
        maintenance_controller::recovery_tick(&service)
            .await
            .unwrap();
        maintenance_controller::recovery_tick(&service)
            .await
            .unwrap();
        assert_eq!(
            worker.launches.load(Ordering::SeqCst),
            if matches!(case, "unreleased" | "recovered") {
                2
            } else {
                1
            }
        );
        let row = f.store.get(&f.request.execution).unwrap();
        if matches!(case, "child" | "recovered") {
            assert_eq!(row.status, agent_run_domain::domain::Status::Starting);
            assert!(f.store.active_maintenance_fence().unwrap().is_some());
        } else {
            assert_eq!(row.status, agent_run_domain::domain::Status::Failed);
            assert_eq!(row.failure_kind.as_deref(), Some("supervisor_spawn_failed"));
            assert!(f.store.active_maintenance_fence().unwrap().is_none());
        }
    }
}

/// Reaping the exact dead supervisor reports an unconfirmed continuation and
/// releases the maintenance fence without submitting another native input.
#[tokio::test]
async fn maintenance_reaped_unconfirmed_continuation_does_not_block_admission() {
    use agent_run_core::{
        lifecycle::reconcile::reconcile_reaped_agent, maintenance_controller, service::Service,
    };
    let mut f = Fixture::with_mode(false, true);
    let harness = common::writer::Harness::start(f.home.path());
    let worker = std::sync::Arc::new(AccountWorker {
        route: f.route.clone(),
        calls: std::sync::atomic::AtomicUsize::new(0),
    });
    let service = Service::new(f.home.path().into())
        .with_writer(harness.broker())
        .with_system_launcher(worker);
    let allocation = service
        .claim_maintenance_resume(
            f.request.epoch,
            &f.request.execution,
            "reaped-unconfirmed-fixture".into(),
        )
        .await
        .unwrap();
    let now = agent_run_core::domain::now();
    f.store.conn.execute(
        "UPDATE agents SET status='running',supervisor_pid=123,supervisor_identity='linux:fixture',supervisor_birth_time=5,process_group_id=456,heartbeat_at=? WHERE id=?",
        rusqlite::params![now - 1.0, f.request.execution.as_str()],
    ).unwrap();
    f.store.conn.execute(
        "UPDATE attempts SET state='running',phase='spawning',process_identity='fixture',process_birth_time=5 WHERE id=?",
        [&allocation.target_attempt_id],
    ).unwrap();
    assert!(reconcile_reaped_agent(&mut f.store, &f.request.execution, 123, now).unwrap());
    let row = f.store.get(&f.request.execution).unwrap();
    assert_eq!(row.status, agent_run_domain::domain::Status::Lost);
    assert_eq!(
        row.failure_kind.as_deref(),
        Some("maintenance_continuation_unconfirmed")
    );
    maintenance_controller::recovery_tick(&service)
        .await
        .unwrap();
    assert!(f.store.active_maintenance_fence().unwrap().is_none());
}

/// A registered supervisor lost before native launch is cleaned up and its
/// preserved session can be manually continued after the fence opens.
#[tokio::test]
async fn maintenance_reconciles_dead_prepared_supervisor_then_allows_resume() {
    use agent_run_core::{
        lifecycle::reconcile::reconcile_with, maintenance_controller, service::Service,
    };
    let mut f = Fixture::with_mode(false, true);
    let harness = common::writer::Harness::start(f.home.path());
    let worker = std::sync::Arc::new(AccountWorker {
        route: f.route.clone(),
        calls: std::sync::atomic::AtomicUsize::new(0),
    });
    let service = Service::new(f.home.path().into())
        .with_writer(harness.broker())
        .with_system_launcher(worker);
    service
        .claim_maintenance_resume(
            f.request.epoch,
            &f.request.execution,
            "dead-prepared-fixture".into(),
        )
        .await
        .unwrap();
    let me = agent_run_platform::process::inspect(std::process::id() as i32).unwrap();
    let now = agent_run_core::domain::now();
    f.store.conn.execute(
        "UPDATE agents SET supervisor_pid=?,supervisor_identity=?,supervisor_birth_time=?,process_group_id=NULL,heartbeat_at=? WHERE id=?",
        rusqlite::params![me.pid,format!("{}-old",me.token),me.birth-1.0,now-1.0,f.request.execution.as_str()],
    ).unwrap();
    let changed = reconcile_with(&mut f.store, 10, |_, _, _| {
        agent_run_platform::process::ProcessState::Reused
    })
    .unwrap();
    assert!(changed.contains(&f.request.execution));
    let parent = f.store.get(&f.request.execution).unwrap();
    assert_eq!(
        parent.failure_kind.as_deref(),
        Some("maintenance_resume_start_failed")
    );
    maintenance_controller::recovery_tick(&service)
        .await
        .unwrap();
    assert!(f.store.active_maintenance_fence().unwrap().is_none());
    let client = common::Home::new();
    assert!(
        f.store
            .admit(&client.request(), &client.config, &json!({}), Some(&parent))
            .is_ok()
    );
}

/// A dead broker that never released its capability can transfer the same
/// prepared attempt to the current writer without creating a second attempt.
#[tokio::test]
async fn maintenance_reclaims_unreleased_dead_broker_capability() {
    use agent_run_core::service::Service;
    let f = Fixture::with_mode(false, true);
    let harness = common::writer::Harness::start(f.home.path());
    let worker = std::sync::Arc::new(AccountWorker {
        route: f.route.clone(),
        calls: std::sync::atomic::AtomicUsize::new(0),
    });
    let service = Service::new(f.home.path().into())
        .with_writer(harness.broker())
        .with_system_launcher(worker);
    let allocation = service
        .claim_maintenance_resume(
            f.request.epoch,
            &f.request.execution,
            "reclaim-fixture".into(),
        )
        .await
        .unwrap();
    let me = agent_run_platform::process::inspect(std::process::id() as i32).unwrap();
    let old_birth = me.birth - 1.0;
    let old_owner = json!({"pid":me.pid,"token":format!("{}-old",me.token),"birth":old_birth});
    f.store.conn.execute(
        "UPDATE agents SET startup_owner_pid_identity=?,startup_owner_birth_time=?,supervisor_capability_sha256=? WHERE id=?",
        rusqlite::params![old_owner.to_string(),old_birth,"a".repeat(64),f.request.execution.as_str()],
    ).unwrap();
    let claim = service
        .claim_maintenance_prepared(f.request.epoch, &f.request.execution)
        .await
        .unwrap()
        .unwrap();
    assert!(claim.launch_authorized);
    assert_eq!(
        claim.allocation.target_attempt_id,
        allocation.target_attempt_id
    );
    assert!(
        service
            .claim_maintenance_prepared(f.request.epoch, &f.request.execution)
            .await
            .unwrap()
            .is_none()
    );
}

/// An unverified child inspection is retried once, then reported without
/// leaving admission fenced or recording a partial process identity.
#[tokio::test]
async fn maintenance_failed_child_inspection_has_bounded_recovery() {
    use agent_run_core::{maintenance_controller, service::Service};
    let f = Fixture::with_mode(false, false);
    let harness = common::writer::Harness::start(f.home.path());
    let service = Service::new(f.home.path().into()).with_writer(harness.broker());
    std::fs::write(f.home.path().join("fixture-supervisor-inspect-error"), "").unwrap();
    std::fs::write(
        f.home.path().join("fixture-supervisor-inspect-unverified"),
        "",
    )
    .unwrap();
    maintenance_controller::recovery_tick(&service)
        .await
        .unwrap();
    let (owner, capability): (String, Option<String>) = f
        .store
        .conn
        .query_row(
            "SELECT startup_owner_pid_identity,supervisor_capability_sha256 FROM agents WHERE id=?",
            [f.request.execution.as_str()],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .unwrap();
    let owner: serde_json::Value = serde_json::from_str(&owner).unwrap();
    assert_eq!(owner.as_object().unwrap().len(), 3);
    assert!(capability.is_none());
    let row = f.store.get(&f.request.execution).unwrap();
    assert_eq!(row.status, agent_run_domain::domain::Status::Failed);
    assert!(f.store.active_maintenance_fence().unwrap().is_none());
    let claims:i64=f.store.conn.query_row(
        "SELECT COUNT(*) FROM events WHERE agent_id=? AND kind='maintenance_prepared_claimed_v1' AND json_extract(data_json,'$.current_writer')=1",
        [f.request.execution.as_str()], |row|row.get(0),
    ).unwrap();
    assert_eq!(claims, 2);
    let quarantined:i64=f.store.conn.query_row(
        "SELECT COUNT(*) FROM events WHERE agent_id=? AND kind='supervisor_unreleased_cleanup_unverified' AND json_extract(data_json,'$.pid')>1",
        [f.request.execution.as_str()], |row|row.get(0),
    ).unwrap();
    assert_eq!(quarantined, 2);
}

/// If the unauthenticated child and its group are gone, an inspection failure
/// is a definite pre-start failure and cannot keep admission fenced.
#[tokio::test]
async fn maintenance_failed_child_inspection_finishes_when_cleanup_is_proven() {
    use agent_run_core::{maintenance_controller, service::Service};
    let f = Fixture::with_mode(false, false);
    let harness = common::writer::Harness::start(f.home.path());
    let service = Service::new(f.home.path().into()).with_writer(harness.broker());
    std::fs::write(f.home.path().join("fixture-supervisor-inspect-error"), "").unwrap();
    maintenance_controller::recovery_tick(&service)
        .await
        .unwrap();
    let row = f.store.get(&f.request.execution).unwrap();
    assert_eq!(row.status, agent_run_domain::domain::Status::Failed);
    assert_eq!(row.failure_kind.as_deref(), Some("supervisor_spawn_failed"));
    assert!(f.store.active_maintenance_fence().unwrap().is_none());
}

/// An explicit cancellation also resolves an unregistered target owned by a
/// proven-dead broker without starting its native continuation.
#[tokio::test]
async fn maintenance_broker_cancels_dead_broker_prepared_target() {
    use agent_run_core::{maintenance_controller, service::Service};
    let f = Fixture::with_mode(false, true);
    let harness = common::writer::Harness::start(f.home.path());
    let worker = std::sync::Arc::new(AccountWorker {
        route: f.route.clone(),
        calls: std::sync::atomic::AtomicUsize::new(0),
    });
    let service = Service::new(f.home.path().into())
        .with_writer(harness.broker())
        .with_system_launcher(worker);
    service
        .claim_maintenance_resume(
            f.request.epoch,
            &f.request.execution,
            "cancel-broker-fixture".into(),
        )
        .await
        .unwrap();
    let me = agent_run_platform::process::inspect(std::process::id() as i32).unwrap();
    let old_birth = me.birth - 1.0;
    let old_owner = json!({"pid":me.pid,"token":format!("{}-old",me.token),"birth":old_birth});
    f.store.conn.execute(
        "UPDATE agents SET startup_owner_pid_identity=?,startup_owner_birth_time=?,supervisor_capability_sha256=? WHERE id=?",
        rusqlite::params![old_owner.to_string(),old_birth,"a".repeat(64),f.request.execution.as_str()],
    ).unwrap();
    harness
        .broker()
        .lane("cancel-broker-fixture")
        .remote(
            agent_run_store::ops::EnqueueCommand {
                execution: f.request.execution.clone(),
                kind: "cancel".into(),
                payload: json!({}),
            },
            std::time::Duration::from_secs(5),
        )
        .await
        .unwrap();
    service.reconcile().await.unwrap();
    assert_eq!(
        f.store.get(&f.request.execution).unwrap().status,
        agent_run_domain::domain::Status::Cancelled
    );
    maintenance_controller::recovery_tick(&service)
        .await
        .unwrap();
    assert!(f.store.active_maintenance_fence().unwrap().is_none());
}

/// A user cancel can fence the current broker's unreleased launch before a
/// late child records ownership or receives its capability.
#[tokio::test]
async fn maintenance_broker_cancels_current_unreleased_target() {
    use agent_run_core::{maintenance_controller, service::Service};
    let f = Fixture::with_mode(false, true);
    let harness = common::writer::Harness::start(f.home.path());
    let worker = std::sync::Arc::new(AccountWorker {
        route: f.route.clone(),
        calls: std::sync::atomic::AtomicUsize::new(0),
    });
    let service = Service::new(f.home.path().into())
        .with_writer(harness.broker())
        .with_system_launcher(worker);
    service
        .claim_maintenance_resume(
            f.request.epoch,
            &f.request.execution,
            "cancel-current-fixture".into(),
        )
        .await
        .unwrap();
    f.store
        .conn
        .execute(
            "UPDATE agents SET supervisor_capability_sha256=? WHERE id=?",
            rusqlite::params!["a".repeat(64), f.request.execution.as_str()],
        )
        .unwrap();
    harness
        .broker()
        .lane("cancel-current-fixture")
        .remote(
            agent_run_store::ops::EnqueueCommand {
                execution: f.request.execution.clone(),
                kind: "cancel".into(),
                payload: json!({}),
            },
            std::time::Duration::from_secs(5),
        )
        .await
        .unwrap();
    service.reconcile().await.unwrap();
    assert_eq!(
        f.store.get(&f.request.execution).unwrap().status,
        agent_run_domain::domain::Status::Cancelled
    );
    maintenance_controller::recovery_tick(&service)
        .await
        .unwrap();
    assert!(f.store.active_maintenance_fence().unwrap().is_none());
}

/// A proven-dead pre-registration child closes the prepared target instead of
/// leaving admission fenced indefinitely.
#[tokio::test]
async fn maintenance_reconciles_dead_unregistered_startup_child() {
    use agent_run_core::{
        lifecycle::reconcile::reconcile_with, maintenance_controller, service::Service,
    };
    let mut f = Fixture::with_mode(false, true);
    let harness = common::writer::Harness::start(f.home.path());
    let worker = std::sync::Arc::new(AccountWorker {
        route: f.route.clone(),
        calls: std::sync::atomic::AtomicUsize::new(0),
    });
    let service = Service::new(f.home.path().into())
        .with_writer(harness.broker())
        .with_system_launcher(worker);
    service
        .claim_maintenance_resume(
            f.request.epoch,
            &f.request.execution,
            "dead-child-fixture".into(),
        )
        .await
        .unwrap();
    let me = agent_run_platform::process::inspect(std::process::id() as i32).unwrap();
    let old_birth = me.birth - 1.0;
    let owner = json!({"pid":me.pid,"ppid":me.ppid,"group":me.pid,"birth":old_birth,"token":format!("{}-old",me.token),"zombie":false});
    f.store.conn.execute(
        "UPDATE agents SET startup_owner_pid_identity=?,startup_owner_birth_time=?,supervisor_capability_sha256=? WHERE id=?",
        rusqlite::params![owner.to_string(),old_birth,"a".repeat(64),f.request.execution.as_str()],
    ).unwrap();
    let changed = reconcile_with(&mut f.store, 10, |_, _, _| {
        agent_run_platform::process::ProcessState::Reused
    })
    .unwrap();
    assert!(changed.contains(&f.request.execution));
    let row = f.store.get(&f.request.execution).unwrap();
    assert_eq!(
        row.failure_kind.as_deref(),
        Some("maintenance_resume_start_failed")
    );
    maintenance_controller::recovery_tick(&service)
        .await
        .unwrap();
    assert!(f.store.active_maintenance_fence().unwrap().is_none());
    let (phase,proof):(String,String) = f.store.conn.query_row(
        "SELECT phase,cleanup_proof_json FROM attempts WHERE id=(SELECT target_attempt_id FROM maintenance_resumes WHERE agent_id=?)",
        [f.request.execution.as_str()], |row|Ok((row.get(0)?,row.get(1)?)),
    ).unwrap();
    assert_eq!(phase, "cleanup_complete");
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(&proof).unwrap()["never_spawned"],
        true
    );
    let parent = f.store.get(&f.request.execution).unwrap();
    let client = common::Home::new();
    assert!(
        f.store
            .admit(&client.request(), &client.config, &json!({}), Some(&parent))
            .is_ok()
    );
}

/// The periodic broker sweep closes a user cancellation when the exact
/// successor supervisor died before it could start a native process.
#[tokio::test]
async fn maintenance_broker_cancels_dead_unstarted_successor() {
    use agent_run_core::service::Service;
    let f = Fixture::with_mode(false, true);
    let harness = common::writer::Harness::start(f.home.path());
    let worker = std::sync::Arc::new(AccountWorker {
        route: f.route.clone(),
        calls: std::sync::atomic::AtomicUsize::new(0),
    });
    let service = Service::new(f.home.path().into())
        .with_writer(harness.broker())
        .with_system_launcher(worker);
    let allocation = service
        .claim_maintenance_resume(
            f.request.epoch,
            &f.request.execution,
            "cancel-prepared-fixture".into(),
        )
        .await
        .unwrap();
    let me = agent_run_platform::process::inspect(std::process::id() as i32).unwrap();
    f.store.conn.execute(
        "UPDATE agents SET supervisor_pid=?,supervisor_identity=?,supervisor_birth_time=?,supervisor_capability_sha256=? WHERE id=?",
        rusqlite::params![me.pid,format!("{}-old",me.token),me.birth-1.0,"f".repeat(64),f.request.execution.as_str()],
    ).unwrap();
    harness
        .broker()
        .lane("cancel-prepared-fixture")
        .remote(
            agent_run_store::ops::EnqueueCommand {
                execution: f.request.execution.clone(),
                kind: "cancel".into(),
                payload: json!({}),
            },
            std::time::Duration::from_secs(5),
        )
        .await
        .unwrap();
    service.reconcile().await.unwrap();
    assert_eq!(
        f.store.get(&f.request.execution).unwrap().status,
        agent_run_domain::domain::Status::Cancelled
    );
    assert_eq!(
        f.store
            .conn
            .query_row(
                "SELECT COUNT(*) FROM attempts WHERE agent_id=?",
                [f.request.execution.as_str()],
                |row| row.get::<_, i64>(0)
            )
            .unwrap(),
        2
    );
    assert_eq!(
        f.store
            .conn
            .query_row(
                "SELECT ownership_active FROM attempts WHERE id=?",
                [allocation.target_attempt_id],
                |row| row.get::<_, i64>(0)
            )
            .unwrap(),
        0
    );
}

/// The broker can also prove the spawned startup child died before it
/// registered supervisor ownership; an initial broker identity is not enough.
#[tokio::test]
async fn maintenance_broker_cancels_dead_startup_child() {
    use agent_run_core::service::Service;
    let f = Fixture::with_mode(false, true);
    let harness = common::writer::Harness::start(f.home.path());
    let worker = std::sync::Arc::new(AccountWorker {
        route: f.route.clone(),
        calls: std::sync::atomic::AtomicUsize::new(0),
    });
    let service = Service::new(f.home.path().into())
        .with_writer(harness.broker())
        .with_system_launcher(worker);
    let allocation = service
        .claim_maintenance_resume(
            f.request.epoch,
            &f.request.execution,
            "cancel-startup-fixture".into(),
        )
        .await
        .unwrap();
    let mut old = agent_run_platform::process::inspect(std::process::id() as i32).unwrap();
    old.group = old.pid;
    old.token.push_str("-old");
    old.birth -= 1.0;
    f.store.conn.execute(
        "UPDATE agents SET startup_owner_pid_identity=?,supervisor_capability_sha256=? WHERE id=?",
        rusqlite::params![serde_json::to_string(&old).unwrap(),"f".repeat(64),f.request.execution.as_str()],
    ).unwrap();
    harness
        .broker()
        .lane("cancel-startup-fixture")
        .remote(
            agent_run_store::ops::EnqueueCommand {
                execution: f.request.execution.clone(),
                kind: "cancel".into(),
                payload: json!({}),
            },
            std::time::Duration::from_secs(5),
        )
        .await
        .unwrap();
    service.reconcile().await.unwrap();
    assert_eq!(
        f.store.get(&f.request.execution).unwrap().status,
        agent_run_domain::domain::Status::Cancelled
    );
    assert_eq!(
        f.store
            .conn
            .query_row(
                "SELECT ownership_active FROM attempts WHERE id=?",
                [allocation.target_attempt_id],
                |row| row.get::<_, i64>(0)
            )
            .unwrap(),
        0
    );
}

/// Both ordinary local mode and account-worker mode allocate only one target;
/// replay remains an identity read even after mutable policy/history disappears.
#[tokio::test]
async fn maintenance_broker_claim_delegates_or_reads_locally_without_launching() {
    use agent_run_core::service::Service;
    for system in [false, true] {
        let f = Fixture::with_mode(false, system);
        let harness = common::writer::Harness::start(f.home.path());
        let worker = std::sync::Arc::new(AccountWorker {
            route: f.route.clone(),
            calls: std::sync::atomic::AtomicUsize::new(0),
        });
        let mut service = Service::new(f.home.path().into()).with_writer(harness.broker());
        if system {
            service = service.with_system_launcher(worker.clone());
        }
        let first = service
            .claim_maintenance_resume(
                f.request.epoch,
                &f.request.execution,
                "broker-resume".into(),
            )
            .await
            .unwrap();
        assert_eq!(first.agent_id, f.request.execution);
        assert_eq!(first.account_id, "acct-fixture");
        assert!(first.launch_authorized);
        assert_eq!(
            f.store
                .conn
                .query_row(
                    "SELECT COUNT(*) FROM attempts WHERE agent_id=?",
                    [f.request.execution.as_str()],
                    |row| row.get::<_, i64>(0)
                )
                .unwrap(),
            2
        );
        assert_eq!(
            worker.calls.load(std::sync::atomic::Ordering::SeqCst),
            usize::from(system)
        );
        std::fs::remove_file(f.home.path().join("config.toml")).unwrap();
        std::fs::write(&f.history, "unavailable after allocation").unwrap();
        let replay = service
            .claim_maintenance_resume(
                f.request.epoch,
                &f.request.execution,
                "broker-resume".into(),
            )
            .await
            .unwrap();
        assert!(!replay.launch_authorized);
        let mut original_identity = first.clone();
        original_identity.launch_authorized = false;
        assert_eq!(replay, original_identity);
        assert!(matches!(
            service
                .claim_maintenance_resume(
                    f.request.epoch,
                    &f.request.execution,
                    "another-request".into()
                )
                .await,
            Err(agent_run_domain::Error::Conflict)
        ));
        assert_eq!(
            worker.calls.load(std::sync::atomic::Ordering::SeqCst),
            usize::from(system)
        );
        assert_eq!(
            f.store
                .conn
                .query_row(
                    "SELECT phase FROM attempts WHERE id=?",
                    [first.target_attempt_id],
                    |row| row.get::<_, String>(0)
                )
                .unwrap(),
            "prepared"
        );
    }
}

/// A revoked original account, changed worker route or missing launcher mode
/// refuses before private preflight/claim; no alternate account is selected.
#[tokio::test]
async fn maintenance_broker_claim_rechecks_account_route_and_mode() {
    use agent_run_core::service::Service;
    for reason in ["account", "route", "mode"] {
        let mut f = Fixture::new(false);
        if reason == "account" {
            f.store
                .disable_account(&"acct-fixture".parse().unwrap())
                .unwrap();
        }
        let harness = common::writer::Harness::start(f.home.path());
        let mut route = f.route.clone();
        if reason == "route" {
            route.fingerprint = "0".repeat(64).parse().unwrap();
        }
        let worker = std::sync::Arc::new(AccountWorker {
            route,
            calls: std::sync::atomic::AtomicUsize::new(0),
        });
        let mut service = Service::new(f.home.path().into()).with_writer(harness.broker());
        if reason != "mode" {
            service = service.with_system_launcher(worker.clone());
        }
        assert!(
            service
                .claim_maintenance_resume(
                    f.request.epoch,
                    &f.request.execution,
                    "refused-resume".into()
                )
                .await
                .is_err(),
            "{reason}"
        );
        assert_eq!(worker.calls.load(std::sync::atomic::Ordering::SeqCst), 0);
        assert!(
            f.store
                .maintenance_resume(f.request.epoch, &f.request.execution)
                .unwrap()
                .is_none()
        );
    }
}

/// A user cancel arriving after park is fulfilled by the periodic broker only
/// after OS death/reuse proof. Private history and a launcher are not required.
#[tokio::test]
async fn maintenance_broker_reconciles_cancel_after_park_and_exit() {
    use agent_run_core::service::Service;
    for alive in [false, true] {
        let f = Fixture::new(alive);
        let harness = common::writer::Harness::start(f.home.path());
        let service = Service::new(f.home.path().into()).with_writer(harness.broker());
        service.cancel(&f.request.execution).await.unwrap();
        // Cancellation needs immutable cleanup evidence, not access to the
        // account's native transcript or any new model/harness invocation.
        std::fs::remove_file(&f.history).unwrap();
        service.reconcile().await.unwrap();
        let status = f.store.get(&f.request.execution).unwrap().status;
        assert_eq!(
            status,
            if alive {
                agent_run_domain::domain::Status::Running
            } else {
                agent_run_domain::domain::Status::Cancelled
            }
        );
        service.reconcile().await.unwrap();
        let cancelled:i64=f.store.conn.query_row("SELECT COUNT(*) FROM events WHERE agent_id=? AND kind='status' AND to_status='cancelled'",[f.request.execution.as_str()],|row|row.get(0)).unwrap();
        assert_eq!(cancelled, if alive { 0 } else { 1 });
    }
}
