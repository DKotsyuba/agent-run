//! Offline mixed-pool contract proofs; no engine/provider is launched.
use agent_run_core::service::Service;
use agent_run_domain::{
    catalog::{AccountRecord, AccountStatus},
    domain::{AgentId, OrchestratorRef},
    pool::{PoolMessage, PoolPropose, PoolStartRequest},
    worker::WorkerCatalogProof,
};
use agent_run_store::{Store, pool_log::PoolWrite};
use rusqlite::params;
use serde_json::{Value, json};
use std::{fs, path::PathBuf};

/// Owns one isolated provider configuration and a fixture-owned running attempt.
struct Fixture {
    /// Holds the disposable database and role assets.
    _temp: tempfile::TempDir,
    /// Exact isolated home.
    root: PathBuf,
    /// Shared production service, used without supervisor handoff.
    service: Service,
    /// Stable independent root.
    agent: AgentId,
    /// Pinned owned attempt.
    attempt: String,
    /// Synthetic fixture capability; no production credential is read.
    token: String,
}
impl Fixture {
    /// Admits one independent execution, then models verified local ownership
    /// and an actual-catalog registration for storage-level boundary tests.
    fn new(binding: Option<OrchestratorRef>, cap: usize) -> Self {
        Self::with_timeout(binding, cap, 600.0)
    }

    /// Builds a fixture with a deliberately finite original execution deadline.
    fn with_timeout(binding: Option<OrchestratorRef>, cap: usize, timeout: f64) -> Self {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().to_path_buf();
        Store::initialize(&root).unwrap();
        fs::create_dir_all(root.join("profiles")).unwrap();
        fs::write(root.join("profiles/review.md"),"+++\nrevision=\"1\"\nwrite=false\nnetwork=false\nallow_external_read_roots=false\nskills=[]\nmcp=[]\nrequired_constraints=[]\n+++\nReview.\n").unwrap();
        fs::write(root.join("config.toml"),format!("schema_version=2\n[core]\nmax_active_agents={cap}\n[harnesses.codex]\nbinary=\"/bin/true\"\nhome=\"{}\"\n[harnesses.claude-code]\nbinary=\"/bin/true\"\nhome=\"{}\"\n[providers.codex]\nharness=\"codex\"\nconnection={{kind=\"native\"}}\nauth_family=\"openai\"\nlimits_source=\"none\"\n[[providers.codex.models]]\nid=\"m\"\n[[providers.codex.bindings]]\nlabel=\"test\"\naccount=\"acct\"\n",root.join("codex").display(),root.join("claude").display())).unwrap();
        let mut store = Store::open(&root).unwrap();
        store
            .register_account(&AccountRecord {
                account_id: "acct".parse().unwrap(),
                auth_family: "openai".parse().unwrap(),
                secret_ref: "env:FIXTURE_KEY".parse().unwrap(),
                status: AccountStatus::Enabled,
            })
            .unwrap();
        agent_run_config::provider_config::ProviderConfig::load(&root).unwrap();
        let service = Service::new(root.clone());
        let request=serde_json::from_value(json!({"provider":"codex","model":"m","profile":"review","task":"Keep current independent work.","workdir":root,"timeout_seconds":timeout,"display_name":"existing","orchestrator":binding})).unwrap();
        let admitted = service.admit_provider(request).unwrap();
        let agent: AgentId = serde_json::from_value(admitted["agent_id"].clone()).unwrap();
        let attempt: String = store
            .conn
            .query_row(
                "SELECT id FROM attempts WHERE agent_id=?",
                [agent.as_str()],
                |r| r.get(0),
            )
            .unwrap();
        let owner = agent_run_platform::process::inspect(std::process::id() as i32).unwrap();
        store.conn.execute("UPDATE agents SET status='running',runtime_session_id='fixture-native-session',supervisor_pid=?,supervisor_identity=?,supervisor_birth_time=? WHERE id=?",params![std::process::id(),owner.token,owner.birth,agent.as_str()]).unwrap();
        store
            .conn
            .execute(
                "UPDATE attempts SET state='running',ownership_active=1 WHERE id=?",
                [&attempt],
            )
            .unwrap();
        let token = "a".repeat(64);
        let now = agent_run_core::domain::now();
        store
            .issue_worker_capability(&agent, &attempt, &token, now)
            .unwrap();
        store
            .register_worker_catalog(
                &WorkerCatalogProof {
                    run_id: agent.clone(),
                    attempt_id: attempt.clone(),
                    token: token.clone(),
                    version: agent_run_domain::worker::POOL_CATALOG_VERSION,
                    digest: agent_run_domain::worker::pool_catalog_digest(),
                },
                now,
            )
            .unwrap();
        Self {
            _temp: temp,
            root,
            service,
            agent,
            attempt,
            token,
        }
    }

    /// Returns the backward-compatible mixed request, with distinct new-member names.
    fn request(
        &self,
        key: &str,
        new_count: usize,
        binding: Option<OrchestratorRef>,
    ) -> PoolStartRequest {
        let mut members = vec![json!({"existing_agent_id":self.agent,"role":"reviewer"})];
        for i in 0..new_count {
            members.push(json!({"role":"helper","start":{"provider":"codex","model":"m","profile":"review","task":format!("New bounded work {i}"),"workdir":self.root,"display_name":format!("new-{i}")}}));
        }
        serde_json::from_value(json!({"request_id":key,"goal":"One common goal","acceptance":[{"id":"done","text":"Goal is verified"}],"orchestrator":binding,"members":members})).unwrap()
    }

    /// Captures immutable execution/session/grant/account/reservation facts.
    fn snapshot(&self) -> Value {
        let store = Store::open(&self.root).unwrap();
        let row = store.get(&self.agent).unwrap();
        let account: Option<String> = store
            .conn
            .query_row(
                "SELECT selected_account_id FROM attempts WHERE id=?",
                [&self.attempt],
                |r| r.get(0),
            )
            .unwrap();
        let keys = store
            .conn
            .prepare(
                "SELECT quota_key FROM attempt_quota_keys WHERE attempt_id=? ORDER BY quota_key",
            )
            .unwrap()
            .query_map([&self.attempt], |r| r.get::<_, String>(0))
            .unwrap()
            .collect::<std::result::Result<Vec<_>, _>>()
            .unwrap();
        let deadline: f64 = store
            .conn
            .query_row(
                "SELECT created_at+timeout_seconds FROM agents WHERE id=?",
                [self.agent.as_str()],
                |r| r.get(0),
            )
            .unwrap();
        json!([
            row.request,
            row.identity,
            row.runtime_session_id,
            row.created_at,
            deadline,
            account,
            keys
        ])
    }

    /// Reads the opaque challenge for this fixture member from the isolated store.
    fn challenge(&self) -> String {
        Store::open(&self.root)
            .unwrap()
            .conn
            .query_row(
                "SELECT challenge FROM pool_enrollments WHERE agent_id=?",
                [self.agent.as_str()],
                |r| r.get(0),
            )
            .unwrap()
    }
}

/// Mixed admission preserves every existing execution fact and marks only new
/// tasks for launch; duplicate replay survives later terminal state.
#[test]
fn mixed_preserves_existing_and_replays_after_exit() {
    let f = Fixture::new(None, 8);
    let before = f.snapshot();
    let request = f.request("mixed", 1, None);
    let pool = f.service.admit_pool(request.clone()).unwrap();
    assert_eq!(pool["members"][0]["agent_id"], json!(f.agent));
    assert_eq!(pool["members"][0]["existing"], true);
    assert_eq!(pool["members"][0]["enrollment"]["state"], "pending");
    assert_eq!(before, f.snapshot());
    let mut store = Store::open(&f.root).unwrap();
    assert_eq!(
        store
            .conn
            .query_row("SELECT COUNT(*) FROM agents", [], |r| r.get::<_, i64>(0))
            .unwrap(),
        2
    );
    assert_eq!(
        store
            .conn
            .query_row(
                "SELECT COUNT(*) FROM attempts WHERE agent_id=?",
                [f.agent.as_str()],
                |r| r.get::<_, i64>(0)
            )
            .unwrap(),
        1
    );
    let new: AgentId = serde_json::from_value(pool["members"][1]["agent_id"].clone()).unwrap();
    assert!(
        store
            .get(&new)
            .unwrap()
            .request
            .task
            .contains("pending authenticated enrollment")
    );
    store
        .conn
        .execute(
            "UPDATE agents SET status='succeeded',finished_at=? WHERE id=?",
            params![agent_run_core::domain::now(), f.agent.as_str()],
        )
        .unwrap();
    let replay = f.service.admit_pool(request.clone()).unwrap();
    assert_eq!(replay["created"], false);
    assert_eq!(replay["pool_id"], pool["pool_id"]);
    let mut conflict = request;
    conflict.goal = "Different goal".into();
    assert!(f.service.admit_pool(conflict).is_err());
    assert!(
        store
            .settle_pool(&serde_json::from_value(pool["pool_id"].clone()).unwrap())
            .unwrap()
            .is_none()
    );
}

/// Existing capacity is counted once: a later new-member cap failure rolls
/// back the entire batch without changing the independent worker.
#[test]
fn mixed_atomic_cap_failure_rolls_back() {
    let f = Fixture::new(None, 2);
    let before = f.snapshot();
    assert!(
        f.service
            .admit_pool(f.request("rollback", 2, None))
            .is_err()
    );
    assert_eq!(before, f.snapshot());
    let store = Store::open(&f.root).unwrap();
    assert_eq!(
        store
            .conn
            .query_row("SELECT COUNT(*) FROM agents", [], |r| r.get::<_, i64>(0))
            .unwrap(),
        1
    );
    assert_eq!(
        store
            .conn
            .query_row("SELECT COUNT(*) FROM pools", [], |r| r.get::<_, i64>(0))
            .unwrap(),
        0
    );
    assert_eq!(
        store
            .conn
            .query_row(
                "SELECT pool_membership_ever FROM agents WHERE id=?",
                [f.agent.as_str()],
                |r| r.get::<_, i64>(0)
            )
            .unwrap(),
        0
    );
}

/// ACK requires the exact current capability/challenge, records one summary
/// and becomes idempotent; unacknowledged proposals/settlement are refused.
#[test]
fn ack_wrong_missing_replayed_and_expired_are_closed() {
    let f = Fixture::new(None, 8);
    let pool = f.service.admit_pool(f.request("ack", 1, None)).unwrap();
    let mut store = Store::open(&f.root).unwrap();
    let propose = PoolWrite::Proposal(PoolPropose {
        request_id: "p".into(),
        message: "ready".into(),
        snapshot: "answer".into(),
    });
    assert!(
        store
            .pool_write(&f.agent, &f.attempt, &f.token, propose)
            .is_err()
    );
    let wrong = PoolWrite::Message(PoolMessage {
        request_id: "wrong".into(),
        message: "summary".into(),
    });
    assert!(
        store
            .pool_write(&f.agent, &f.attempt, &f.token, wrong)
            .is_err()
    );
    let input = || {
        PoolWrite::Message(PoolMessage {
            request_id: f.challenge(),
            message: "Current work is still bounded.".into(),
        })
    };
    let receipt = store
        .pool_write(&f.agent, &f.attempt, &f.token, input())
        .unwrap()
        .unwrap();
    assert!(!receipt.duplicate);
    let replay = store
        .pool_write(&f.agent, &f.attempt, &f.token, input())
        .unwrap()
        .unwrap();
    assert!(replay.duplicate);
    assert_eq!(receipt.seq, replay.seq);
    assert_eq!(
        store
            .conn
            .query_row(
                "SELECT state FROM pool_enrollments WHERE agent_id=?",
                [f.agent.as_str()],
                |r| r.get::<_, String>(0)
            )
            .unwrap(),
        "joined"
    );
    assert!(
        store
            .settle_pool(&serde_json::from_value(pool["pool_id"].clone()).unwrap())
            .unwrap()
            .is_none()
    );
    store
        .conn
        .execute(
            "UPDATE attempts SET ownership_active=0,finished_at=? WHERE id=?",
            params![agent_run_core::domain::now(), f.attempt],
        )
        .unwrap();
    assert!(
        store
            .pool_write(&f.agent, &f.attempt, &f.token, input())
            .is_err()
    );
}

/// Binding inference avoids extra manual hook steps; explicit incompatible
/// binding refuses without altering the existing destination.
#[test]
fn binding_inference_and_unbound_explicit_are_consistent() {
    let bound = OrchestratorRef {
        transport: "codex_queue".into(),
        external_session_id: "chat".into(),
        external_turn_id: Some("turn".into()),
    };
    let f = Fixture::new(Some(bound.clone()), 8);
    let pool = f
        .service
        .admit_pool(f.request("inferred", 1, None))
        .unwrap();
    assert_eq!(pool["bound"], true);
    let f = Fixture::new(None, 8);
    let pool = f
        .service
        .admit_pool(f.request("explicit", 1, Some(bound.clone())))
        .unwrap();
    assert_eq!(pool["bound"], true);
    assert!(
        Store::open(&f.root)
            .unwrap()
            .get(&f.agent)
            .unwrap()
            .orchestrator_session_id
            .is_some()
    );
    let f = Fixture::new(None, 8);
    let pool = f
        .service
        .admit_pool(f.request("auto-hook", 1, None))
        .unwrap();
    assert_eq!(pool["bound"], false);
    let mut store = Store::open(&f.root).unwrap();
    store
        .bind_pool(
            &serde_json::from_value(pool["pool_id"].clone()).unwrap(),
            &bound,
            agent_run_core::domain::now(),
        )
        .unwrap();
    assert!(
        store
            .get(&f.agent)
            .unwrap()
            .orchestrator_session_id
            .is_some()
    );
    assert!(
        store
            .settle_pool(&serde_json::from_value(pool["pool_id"].clone()).unwrap())
            .unwrap()
            .is_none()
    );
    let f = Fixture::new(Some(bound), 8);
    let different = OrchestratorRef {
        transport: "codex_queue".into(),
        external_session_id: "other".into(),
        external_turn_id: None,
    };
    assert!(
        f.service
            .admit_pool(f.request("wrong-bind", 1, Some(different)))
            .is_err()
    );
}

/// Unknown actual catalog proof, historical independence, terminal workers,
/// and prior membership cannot be silently assumed attachable.
#[test]
fn unsupported_terminal_and_prior_pool_are_refused() {
    let f = Fixture::new(None, 8);
    let store = Store::open(&f.root).unwrap();
    store.conn.execute("UPDATE worker_capabilities SET pool_catalog_digest=NULL,pool_catalog_version=NULL WHERE attempt_id=?",[&f.attempt]).unwrap();
    assert!(f.service.admit_pool(f.request("unknown", 1, None)).is_err());
    let f = Fixture::new(None, 8);
    f.service.admit_pool(f.request("first", 1, None)).unwrap();
    assert!(f.service.admit_pool(f.request("second", 1, None)).is_err());
    let f = Fixture::new(None, 8);
    Store::open(&f.root)
        .unwrap()
        .conn
        .execute(
            "UPDATE agents SET status='cancelled',finished_at=created_at WHERE id=?",
            [f.agent.as_str()],
        )
        .unwrap();
    assert!(
        f.service
            .admit_pool(f.request("terminal", 1, None))
            .is_err()
    );
}

/// The original deadline bounds ACK even if the worker is otherwise alive;
/// fixture time advances past it rather than renewing enrollment.
#[test]
fn ack_original_deadline_expires_without_extension() {
    let f = Fixture::with_timeout(None, 8, 2.0);
    f.service
        .admit_pool(f.request("deadline", 1, None))
        .unwrap();
    let mut store = Store::open(&f.root).unwrap();
    let deadline: f64 = store
        .conn
        .query_row(
            "SELECT deadline FROM pool_enrollments WHERE agent_id=?",
            [f.agent.as_str()],
            |r| r.get(0),
        )
        .unwrap();
    while agent_run_core::domain::now() <= deadline {
        std::thread::sleep(std::time::Duration::from_millis(20));
    }
    let ack = PoolWrite::Message(PoolMessage {
        request_id: f.challenge(),
        message: "Late summary".into(),
    });
    assert!(
        store
            .pool_write(&f.agent, &f.attempt, &f.token, ack)
            .is_err()
    );
    let pool: String = store
        .conn
        .query_row(
            "SELECT pool_id FROM pool_members WHERE agent_id=?",
            [f.agent.as_str()],
            |r| r.get(0),
        )
        .unwrap();
    store
        .reconcile_pool_enrollments(&pool.parse().unwrap())
        .unwrap();
    assert_eq!(
        store
            .conn
            .query_row(
                "SELECT state FROM pool_enrollments WHERE agent_id=?",
                [f.agent.as_str()],
                |r| r.get::<_, String>(0)
            )
            .unwrap(),
        "needs_action"
    );
}

/// A replacement attempt on the same execution cannot use the old enrollment
/// challenge, and the previous token loses authority immediately.
#[test]
fn new_attempt_cannot_ack_old_pinned_enrollment() {
    let f = Fixture::new(None, 8);
    f.service
        .admit_pool(f.request("attempt-race", 1, None))
        .unwrap();
    let challenge = f.challenge();
    let mut store = Store::open(&f.root).unwrap();
    let at = agent_run_core::domain::now();
    store
        .conn
        .execute(
            "UPDATE attempts SET ownership_active=0,finished_at=? WHERE id=?",
            params![at, f.attempt],
        )
        .unwrap();
    let next = "att_enrollment_second";
    store.conn.execute("INSERT INTO attempts(id,agent_id,number,state,adapter_state_json,created_at,ownership_active) VALUES(?,?,2,'running','{}',?,1)",params![next,f.agent.as_str(),at]).unwrap();
    let token = "b".repeat(64);
    store
        .issue_worker_capability(&f.agent, next, &token, at)
        .unwrap();
    let ack = || {
        PoolWrite::Message(PoolMessage {
            request_id: challenge.clone(),
            message: "Summary".into(),
        })
    };
    assert!(
        store
            .pool_write(&f.agent, &f.attempt, &f.token, ack())
            .is_err()
    );
    assert!(store.pool_write(&f.agent, next, &token, ack()).is_err());
}

/// Actual catalog registration permits owned STARTING and keeps the first
/// observed proof timestamp on legitimate identical retries.
#[test]
fn catalog_starting_registration_is_idempotent_and_unknown_is_not_upgraded() {
    let f = Fixture::new(None, 8);
    let mut store = Store::open(&f.root).unwrap();
    store
        .conn
        .execute(
            "UPDATE agents SET status='starting' WHERE id=?",
            [f.agent.as_str()],
        )
        .unwrap();
    store
        .conn
        .execute(
            "UPDATE attempts SET state='preparing' WHERE id=?",
            [&f.attempt],
        )
        .unwrap();
    let first: f64 = store
        .conn
        .query_row(
            "SELECT pool_catalog_observed_at FROM worker_capabilities WHERE attempt_id=?",
            [&f.attempt],
            |r| r.get(0),
        )
        .unwrap();
    let proof = WorkerCatalogProof {
        run_id: f.agent.clone(),
        attempt_id: f.attempt.clone(),
        token: f.token.clone(),
        version: agent_run_domain::worker::POOL_CATALOG_VERSION,
        digest: agent_run_domain::worker::pool_catalog_digest(),
    };
    store
        .register_worker_catalog(&proof, agent_run_core::domain::now())
        .unwrap();
    assert_eq!(
        first,
        store
            .conn
            .query_row(
                "SELECT pool_catalog_observed_at FROM worker_capabilities WHERE attempt_id=?",
                [&f.attempt],
                |r| r.get::<_, f64>(0)
            )
            .unwrap()
    );
    let mut bad = proof;
    bad.digest = "0".repeat(64);
    assert!(
        store
            .register_worker_catalog(&bad, agent_run_core::domain::now())
            .is_err()
    );
}

/// The exact preflight attempt is rechecked under the write transaction:
/// a concurrent exit rolls back the whole all-existing batch without membership.
#[test]
fn atomic_existing_exit_race_leaves_every_worker_independent() {
    use agent_run_store::pool_admission::{
        PoolAdmissionInput, PoolAdmissionSource, PoolMemberAdmission,
    };
    let f = Fixture::new(None, 8);
    let request=serde_json::from_value(json!({"provider":"codex","model":"m","profile":"review","task":"other independent","workdir":f.root,"timeout_seconds":600.0,"display_name":"other"})).unwrap();
    let admitted = f.service.admit_provider(request).unwrap();
    let other: AgentId = serde_json::from_value(admitted["agent_id"].clone()).unwrap();
    let mut store = Store::open(&f.root).unwrap();
    let attempt: String = store
        .conn
        .query_row(
            "SELECT id FROM attempts WHERE agent_id=?",
            [other.as_str()],
            |r| r.get(0),
        )
        .unwrap();
    let owner = agent_run_platform::process::inspect(std::process::id() as i32).unwrap();
    store.conn.execute("UPDATE agents SET status='running',runtime_session_id='other-session',supervisor_pid=?,supervisor_identity=?,supervisor_birth_time=? WHERE id=?",params![std::process::id(),owner.token,owner.birth,other.as_str()]).unwrap();
    store
        .conn
        .execute(
            "UPDATE attempts SET state='running',ownership_active=1 WHERE id=?",
            [&attempt],
        )
        .unwrap();
    let token = "c".repeat(64);
    store
        .issue_worker_capability(&other, &attempt, &token, agent_run_core::domain::now())
        .unwrap();
    store
        .register_worker_catalog(
            &WorkerCatalogProof {
                run_id: other.clone(),
                attempt_id: attempt.clone(),
                token,
                version: agent_run_domain::worker::POOL_CATALOG_VERSION,
                digest: agent_run_domain::worker::pool_catalog_digest(),
            },
            agent_run_core::domain::now(),
        )
        .unwrap();
    let pins = [
        store.existing_pool_member(&f.agent).unwrap(),
        store.existing_pool_member(&other).unwrap(),
    ];
    store
        .conn
        .execute(
            "UPDATE agents SET status='succeeded',finished_at=? WHERE id=?",
            params![agent_run_core::domain::now(), other.as_str()],
        )
        .unwrap();
    let (config, _) = agent_run_config::provider_config::ProviderConfig::load(&f.root).unwrap();
    let catalog = config
        .resolve_catalog(store.list_accounts().unwrap())
        .unwrap();
    let pool = agent_run_domain::pool::PoolId::new();
    let members = pins
        .into_iter()
        .enumerate()
        .map(|(i, pin)| PoolMemberAdmission {
            id: pin.agent_id.clone(),
            slot: (i + 1) as u8,
            name: format!("seat-{i}"),
            role: "reviewer".into(),
            personal_task: pin.task.clone(),
            source: PoolAdmissionSource::Existing(pin),
        })
        .collect();
    assert!(
        store
            .admit_pool(PoolAdmissionInput {
                pool_id: &pool,
                request_namespace: "global",
                request_id: "exit-race",
                request_sha256: &"e".repeat(64),
                goal: "goal",
                acceptance: &[],
                catalog: &catalog,
                members,
                orchestrator: None
            })
            .is_err()
    );
    assert_eq!(
        store
            .conn
            .query_row("SELECT COUNT(*) FROM pool_members", [], |r| r
                .get::<_, i64>(0))
            .unwrap(),
        0
    );
    assert_eq!(
        store
            .conn
            .query_row(
                "SELECT pool_membership_ever FROM agents WHERE id=?",
                [f.agent.as_str()],
                |r| r.get::<_, i64>(0)
            )
            .unwrap(),
        0
    );
}

/// Active enrollment/ACK references fence retention and history deletion until
/// the overlay is removed by eligible pool GC; lifetime membership remains set.
#[test]
fn enrollment_references_and_lifetime_marker_survive_history_gc() {
    let f = Fixture::new(None, 8);
    let pool = f
        .service
        .admit_pool(f.request("retention", 1, None))
        .unwrap();
    let mut store = Store::open(&f.root).unwrap();
    let ack = PoolWrite::Message(PoolMessage {
        request_id: f.challenge(),
        message: "Current summary".into(),
    });
    let seq = store
        .pool_write(&f.agent, &f.attempt, &f.token, ack)
        .unwrap()
        .unwrap()
        .seq;
    assert!(
        store
            .conn
            .execute("DELETE FROM pool_entries WHERE seq=?", [seq])
            .is_err()
    );
    assert!(
        store
            .conn
            .execute("DELETE FROM attempts WHERE id=?", [&f.attempt])
            .is_err()
    );
    store
        .prune_history(agent_run_core::domain::now() + 20.0 * 86400.0)
        .unwrap();
    assert_eq!(
        store
            .conn
            .query_row("SELECT COUNT(*) FROM pool_enrollments", [], |r| r
                .get::<_, i64>(0))
            .unwrap(),
        1,
        "active proof must be retained"
    );
    let id = pool["pool_id"].as_str().unwrap();
    // Exercise the eligible-GC dependency order without deleting the fixture's
    // still-active execution; production selection itself was fenced above.
    store
        .conn
        .execute(
            "DELETE FROM pool_enrollments WHERE agent_id=?",
            [f.agent.as_str()],
        )
        .unwrap();
    store
        .conn
        .execute("DELETE FROM pool_entries WHERE pool_id=?", [id])
        .unwrap();
    store
        .conn
        .execute("DELETE FROM pool_members WHERE pool_id=?", [id])
        .unwrap();
    store
        .conn
        .execute("DELETE FROM pools WHERE id=?", [id])
        .unwrap();
    assert_eq!(
        store
            .conn
            .query_row(
                "SELECT pool_membership_ever FROM agents WHERE id=?",
                [f.agent.as_str()],
                |r| r.get::<_, i64>(0)
            )
            .unwrap(),
        1
    );
    assert!(store.existing_pool_member(&f.agent).is_err());
    assert!(
        store
            .conn
            .execute(
                "UPDATE agents SET pool_membership_ever=0 WHERE id=?",
                [f.agent.as_str()]
            )
            .is_err()
    );
}

/// A transport rejection leaves a retryable active challenge and queues one
/// attention; later authenticated ACK resolves the queued attention safely.
#[test]
fn active_join_failure_is_visible_retryable_and_attention_is_idempotent() {
    let binding = OrchestratorRef {
        transport: "codex_queue".into(),
        external_session_id: "chat".into(),
        external_turn_id: None,
    };
    let f = Fixture::new(Some(binding), 8);
    let pool = f.service.admit_pool(f.request("recover", 1, None)).unwrap();
    let mut store = Store::open(&f.root).unwrap();
    store.conn.execute("UPDATE commands SET state='completed',result_json='{\"accepted\":false}' WHERE agent_id=? AND kind='steer'",[f.agent.as_str()]).unwrap();
    let id = serde_json::from_value(pool["pool_id"].clone()).unwrap();
    store.reconcile_pool_enrollments(&id).unwrap();
    store.reconcile_pool_enrollments(&id).unwrap();
    assert_eq!(
        store
            .conn
            .query_row(
                "SELECT COUNT(*) FROM events WHERE kind='pool_join_needs_action'",
                [],
                |r| r.get::<_, i64>(0)
            )
            .unwrap(),
        1
    );
    assert_eq!(
        store.delivery_status(&f.agent).unwrap()["state"],
        "not_created",
        "join attention is not completion"
    );
    let ack = PoolWrite::Message(PoolMessage {
        request_id: f.challenge(),
        message: "Read context; continuing current work.".into(),
    });
    store
        .pool_write(&f.agent, &f.attempt, &f.token, ack)
        .unwrap()
        .unwrap();
    assert_eq!(
        store
            .conn
            .query_row(
                "SELECT state FROM pool_enrollments WHERE agent_id=?",
                [f.agent.as_str()],
                |r| r.get::<_, String>(0)
            )
            .unwrap(),
        "joined"
    );
    assert_eq!(store.conn.query_row("SELECT d.state FROM deliveries d JOIN pool_enrollments j ON j.attention_delivery_id=d.id WHERE j.agent_id=?",[f.agent.as_str()],|r|r.get::<_,String>(0)).unwrap(),"cancelled");
}

/// Reserved ACK receipts remain pinned after JOINED, while ordinary messages
/// from a legitimate later attempt/run retain existing lineage semantics.
#[test]
fn joined_ack_cannot_replay_from_new_attempt_or_resumed_run() {
    let f = Fixture::new(None, 8);
    f.service
        .admit_pool(f.request("joined-replay", 1, None))
        .unwrap();
    let key = f.challenge();
    let ack = || {
        PoolWrite::Message(PoolMessage {
            request_id: key.clone(),
            message: "Current summary".into(),
        })
    };
    let mut store = Store::open(&f.root).unwrap();
    let original = store
        .pool_write(&f.agent, &f.attempt, &f.token, ack())
        .unwrap()
        .unwrap();
    let at = agent_run_core::domain::now();
    store
        .conn
        .execute(
            "UPDATE attempts SET ownership_active=0,finished_at=? WHERE id=?",
            params![at, f.attempt],
        )
        .unwrap();
    let next = "att_joined_next";
    store.conn.execute("INSERT INTO attempts(id,agent_id,number,state,adapter_state_json,created_at,ownership_active) VALUES(?,?,2,'running','{}',?,1)",params![next,f.agent.as_str(),at]).unwrap();
    let token = "b".repeat(64);
    store
        .issue_worker_capability(&f.agent, next, &token, at)
        .unwrap();
    assert!(
        store.pool_write(&f.agent, next, &token, ack()).is_err(),
        "joined ACK must revalidate attempt before idem lookup"
    );
    assert!(
        store
            .pool_write(
                &f.agent,
                next,
                &token,
                PoolWrite::Message(PoolMessage {
                    request_id: "ordinary-later-attempt".into(),
                    message: "ordinary progress".into()
                })
            )
            .unwrap()
            .is_ok()
    );

    // Storage fixture for a legitimately owned later lineage run: only the
    // reserved ACK must reject it; ordinary joined-member messages still work.
    store
        .conn
        .execute(
            "UPDATE attempts SET ownership_active=0,finished_at=? WHERE id=?",
            params![at, next],
        )
        .unwrap();
    store
        .conn
        .execute(
            "UPDATE agents SET status='succeeded',finished_at=? WHERE id=?",
            params![at, f.agent.as_str()],
        )
        .unwrap();
    let resumed = AgentId::new();
    store.conn.execute("INSERT INTO agents(id,runtime,model,profile,task,task_summary,workdir,request_json,status,created_at,timeout_seconds,config_revision,root_agent_id,parent_agent_id,sequence) SELECT ?,runtime,model,profile,task,task_summary,workdir,request_json,'running',?,timeout_seconds,config_revision,root_agent_id,id,2 FROM agents WHERE id=?",params![resumed.as_str(),at,f.agent.as_str()]).unwrap();
    let attempt = "att_joined_resume";
    store.conn.execute("INSERT INTO attempts(id,agent_id,number,state,adapter_state_json,created_at,ownership_active) VALUES(?,?,1,'running','{}',?,1)",params![attempt,resumed.as_str(),at]).unwrap();
    let token = "c".repeat(64);
    store
        .issue_worker_capability(&resumed, attempt, &token, at)
        .unwrap();
    assert!(store.pool_write(&resumed, attempt, &token, ack()).is_err());
    assert!(
        store
            .pool_write(
                &resumed,
                attempt,
                &token,
                PoolWrite::Message(PoolMessage {
                    request_id: "ordinary-resume".into(),
                    message: "resumed progress".into()
                })
            )
            .unwrap()
            .is_ok()
    );
    assert_eq!(
        store
            .conn
            .query_row(
                "SELECT COUNT(*) FROM pool_entries WHERE request_id=?",
                [&key],
                |r| r.get::<_, i64>(0)
            )
            .unwrap(),
        1
    );
    assert_eq!(
        store
            .conn
            .query_row(
                "SELECT ack_seq FROM pool_enrollments WHERE agent_id=?",
                [f.agent.as_str()],
                |r| r.get::<_, u64>(0)
            )
            .unwrap(),
        original.seq
    );
}

/// The central membership trigger also covers real replacement admission and
/// keeps both roots permanently marked after pool history is collected.
#[test]
fn replacement_members_keep_lifetime_marker_after_pool_gc() {
    let f = Fixture::new(None, 8);
    let pool = f
        .service
        .admit_pool(f.request("replacement-marker", 1, None))
        .unwrap();
    let store = Store::open(&f.root).unwrap();
    let at = agent_run_core::domain::now();
    store.conn.execute("UPDATE agents SET status='failed',finished_at=?,supervisor_pid=NULL,supervisor_identity=NULL,supervisor_birth_time=NULL WHERE id=?",params![at,f.agent.as_str()]).unwrap();
    store.conn.execute("UPDATE attempts SET state='failed',finished_at=?,ownership_active=0,phase='cleanup_complete',cleanup_proof_json='{}' WHERE id=?",params![at,f.attempt]).unwrap();
    let request=serde_json::from_value(json!({"pool_id":pool["pool_id"],"agent_id":f.agent,"request_id":"replace","start":{"provider":"codex","model":"m","profile":"review","task":"Replacement bounded work","workdir":f.root,"display_name":"replacement"}})).unwrap();
    let replacement = f.service.admit_pool_replacement(request).unwrap().unwrap();
    assert_eq!(
        store
            .conn
            .query_row(
                "SELECT pool_membership_ever FROM agents WHERE id=?",
                [replacement.new.agent_id.as_str()],
                |r| r.get::<_, i64>(0)
            )
            .unwrap(),
        1
    );
    let id = pool["pool_id"].as_str().unwrap();
    store
        .conn
        .execute(
            "DELETE FROM pool_enrollments WHERE agent_id=?",
            [f.agent.as_str()],
        )
        .unwrap();
    store
        .conn
        .execute("DELETE FROM pool_entries WHERE pool_id=?", [id])
        .unwrap();
    store
        .conn
        .execute("DELETE FROM pool_members WHERE pool_id=?", [id])
        .unwrap();
    store
        .conn
        .execute("DELETE FROM pools WHERE id=?", [id])
        .unwrap();
    for root in [&f.agent, &replacement.new.agent_id] {
        assert_eq!(
            store
                .conn
                .query_row(
                    "SELECT pool_membership_ever FROM agents WHERE id=?",
                    [root.as_str()],
                    |r| r.get::<_, i64>(0)
                )
                .unwrap(),
            1
        );
        assert!(store.existing_pool_member(root).is_err());
    }
}

/// An already queued individual terminal error communicates the failed join;
/// enrollment remains visible without creating a duplicate attention notice.
#[test]
fn terminal_error_notice_suppresses_duplicate_join_attention() {
    let f = Fixture::new(None, 8);
    let pool = f
        .service
        .admit_pool(f.request("suppressed", 1, None))
        .unwrap();
    let mut store = Store::open(&f.root).unwrap();
    let at = agent_run_core::domain::now();
    store
        .conn
        .execute(
            "UPDATE agents SET status='failed',finished_at=? WHERE id=?",
            params![at, f.agent.as_str()],
        )
        .unwrap();
    store
        .conn
        .execute(
            "INSERT INTO events(agent_id,attempt_id,at,kind,data_json) VALUES(?,?,?,'status','{}')",
            params![f.agent.as_str(), f.attempt, at],
        )
        .unwrap();
    let event = store.conn.last_insert_rowid();
    store
        .conn
        .execute("UPDATE events SET to_status='failed' WHERE seq=?", [event])
        .unwrap();
    store.conn.execute("INSERT INTO deliveries(id,agent_id,terminal_event_seq,state) VALUES('ntf_existing_error',?,?,'waiting_binding')",params![f.agent.as_str(),event]).unwrap();
    store
        .reconcile_pool_enrollments(&serde_json::from_value(pool["pool_id"].clone()).unwrap())
        .unwrap();
    assert_eq!(
        store
            .conn
            .query_row(
                "SELECT state FROM pool_enrollments WHERE agent_id=?",
                [f.agent.as_str()],
                |r| r.get::<_, String>(0)
            )
            .unwrap(),
        "needs_action"
    );
    assert_eq!(
        store
            .conn
            .query_row(
                "SELECT COUNT(*) FROM events WHERE kind='pool_join_needs_action'",
                [],
                |r| r.get::<_, i64>(0)
            )
            .unwrap(),
        0
    );
}

/// Delivery GC clears the transient link without resetting the permanent
/// issued marker or creating another attention for the same pinned enrollment.
#[test]
fn attention_delivery_gc_does_not_reissue_join_notice() {
    let f = Fixture::new(None, 8);
    let pool = f
        .service
        .admit_pool(f.request("attention-gc", 1, None))
        .unwrap();
    let id = serde_json::from_value(pool["pool_id"].clone()).unwrap();
    let mut store = Store::open(&f.root).unwrap();
    store.conn.execute("UPDATE commands SET state='completed',result_json='{\"accepted\":false}' WHERE agent_id=? AND kind='steer'",[f.agent.as_str()]).unwrap();
    store.reconcile_pool_enrollments(&id).unwrap();
    let delivery: String = store
        .conn
        .query_row(
            "SELECT attention_delivery_id FROM pool_enrollments WHERE agent_id=?",
            [f.agent.as_str()],
            |r| r.get(0),
        )
        .unwrap();
    store
        .conn
        .execute("DELETE FROM deliveries WHERE id=?", [delivery])
        .unwrap();
    assert!(
        store
            .conn
            .query_row(
                "SELECT attention_delivery_id FROM pool_enrollments WHERE agent_id=?",
                [f.agent.as_str()],
                |r| r.get::<_, Option<String>>(0)
            )
            .unwrap()
            .is_none()
    );
    store.reconcile_pool_enrollments(&id).unwrap();
    assert_eq!(
        store
            .conn
            .query_row(
                "SELECT COUNT(*) FROM events WHERE kind='pool_join_needs_action'",
                [],
                |r| r.get::<_, i64>(0)
            )
            .unwrap(),
        1
    );
    assert_eq!(
        store
            .conn
            .query_row("SELECT COUNT(*) FROM deliveries", [], |r| r
                .get::<_, i64>(0))
            .unwrap(),
        0
    );
    assert!(
        store
            .conn
            .execute(
                "UPDATE pool_enrollments SET attention_issued=0 WHERE agent_id=?",
                [f.agent.as_str()]
            )
            .is_err()
    );
}

/// Holds another WAL writer and uses a zero busy budget as a strict proof that
/// an unchanged settlement path never asks SQLite for a writer reservation.
fn assert_settlement_uses_only_reads(f: &Fixture, pool: &Value) {
    let mut store = Store::open(&f.root).unwrap();
    let mut writer = Store::open(&f.root).unwrap();
    assert_eq!(
        store
            .conn
            .query_row("PRAGMA journal_mode", [], |r| r.get::<_, String>(0))
            .unwrap(),
        "wal"
    );
    store.conn.busy_timeout(std::time::Duration::ZERO).unwrap();
    let held = writer
        .conn
        .transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)
        .unwrap();
    assert!(
        store
            .settle_pool(&serde_json::from_value(pool["pool_id"].clone()).unwrap())
            .unwrap()
            .is_none()
    );
    drop(held);
}

/// New-only pools and healthy pending enrollment do not acquire a per-tick
/// writer lock; a previously recorded identical join failure is read-only too.
#[test]
fn unchanged_pool_settlement_does_not_reserve_wal_writer() {
    let f = Fixture::new(None, 8);
    let mut request = f.request("pure-new-wal", 2, None);
    request.members.remove(0);
    let pool = f.service.admit_pool(request).unwrap();
    assert_settlement_uses_only_reads(&f, &pool);

    let f = Fixture::new(None, 8);
    let pool = f
        .service
        .admit_pool(f.request("pending-wal", 1, None))
        .unwrap();
    assert_settlement_uses_only_reads(&f, &pool);
    let mut store = Store::open(&f.root).unwrap();
    store.conn.execute("UPDATE commands SET state='completed',result_json='{\"accepted\":false}' WHERE agent_id=? AND kind='steer'",[f.agent.as_str()]).unwrap();
    store
        .reconcile_pool_enrollments(&serde_json::from_value(pool["pool_id"].clone()).unwrap())
        .unwrap();
    assert_settlement_uses_only_reads(&f, &pool);
}
