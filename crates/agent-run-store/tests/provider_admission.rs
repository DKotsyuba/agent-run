//! Trusted candidate admission, replay, scope, reservation, and contention.

use agent_run_domain::{
    Error, PositiveFinite, ProviderStartRequest,
    catalog::{
        AccountId, AccountRecord, AccountStatus, AuthFamily, PhysicalQuotaKey, ProviderCatalog,
        QuotaAdmissionError, QuotaCandidate, QuotaCandidateSet, ResolvedLaunchAuthority,
        SelectionIntent,
    },
    domain::Outcome,
};
use agent_run_store::Store;
use serde_json::{Value, json};
use std::{
    path::Path,
    sync::{Arc, Barrier},
};

#[path = "provider_admission/quota_decision.rs"]
mod quota_decision_fence;

/// Opens one initialized disposable store with four fake account references.
fn home() -> tempfile::TempDir {
    let home = tempfile::tempdir().unwrap();
    agent_run_store::fixture::initialize(home.path()).unwrap();
    let mut store = agent_run_store::fixture::open(home.path()).unwrap();
    for (id, name) in [
        ("acct-a", "FAKE_A"),
        ("acct-b", "FAKE_B"),
        ("acct-disabled", "FAKE_C"),
        ("acct-ineligible", "FAKE_D"),
    ] {
        store
            .register_account(&AccountRecord {
                account_id: id.parse().unwrap(),
                auth_family: "anthropic".parse::<AuthFamily>().unwrap(),
                secret_ref: format!("env:{name}").parse().unwrap(),
                status: AccountStatus::Enabled,
            })
            .unwrap();
    }
    store
        .disable_account(&"acct-disabled".parse().unwrap())
        .unwrap();
    home
}

/// Resolves provider-local bindings without deriving an account from a name.
fn catalog(store: &Store) -> ProviderCatalog {
    let providers = serde_json::from_value(json!([
        {
            "id":"glm-user","harness":"claude-code",
            "connection":{"kind":"custom","endpoint":"https://gateway.example/api","protocol":"messages"},
            "auth_family":"anthropic","limits_source":"none",
            "models":[{"id":"fixture"},{"id":"other"}],
            "bindings":[
                {"label":"alpha","account":"acct-a"},
                {"label":"beta","account":"acct-b"},
                {"label":"disabled","account":"acct-disabled"},
                {"label":"other-only","account":"acct-ineligible","models":["other"]}
            ]
        },
        {
            "id":"glm-alias","harness":"claude-code",
            "connection":{"kind":"custom","endpoint":"https://gateway.example/api","protocol":"messages"},
            "auth_family":"anthropic","limits_source":"none",
            "models":[{"id":"fixture"}],
            "bindings":[{"label":"shared","account":"acct-a"}]
        }
    ])).unwrap();
    ProviderCatalog::new(store.list_accounts().unwrap(), providers).unwrap()
}

/// Builds one strict request for the configured provider and request-id.
fn provider_request(home: &Path, id: &str) -> ProviderStartRequest {
    let mut request: ProviderStartRequest = serde_json::from_value(json!({
        "provider":"glm-user","model":"fixture","profile":"review",
        "task":"fixture","workdir":home,"request_id":id
    }))
    .unwrap();
    request.validate().unwrap();
    request
}

/// Batch rollback, replay and retention share the ordinary admission transaction.
#[test]
fn generic_batch_is_atomic_and_receipt_survives_history_retention() {
    use agent_run_domain::{batch::StartBatchRequest, domain::AgentId};
    use agent_run_store::{
        batch_admission::{BatchAdmission, BatchMember},
        provider_admission::AdmissionInputs,
    };

    let home = home();
    let mut store = agent_run_store::fixture::open(home.path()).unwrap();
    let store_id = agent_run_store::store_identity::read(&store.conn).unwrap();
    let catalog = catalog(&store);
    let authority = authority(&catalog, home.path());
    let mut first = provider_request(home.path(), "temporary-a");
    first.request_id = None;
    first.task = "first".into();
    let mut second = first.clone();
    second.task = "second".into();
    let first_effective = first.storage_projection();
    let second_effective = second.storage_projection();
    let first_identity = identity(&first, &authority);
    let second_identity = identity(&second, &authority);
    let candidates = candidates(store.quota_capacity_revision().unwrap(), &[("acct-a", 0)]);
    let mut request = StartBatchRequest {
        request_id: "batch-key".into(),
        jobs: vec![first.clone(), second.clone()],
        retain_history: true,
        expected_store_id: Some(store_id.clone()),
        preconditions: None,
    };
    request.validate_intent().unwrap();
    let fingerprint = request.fingerprint().unwrap();
    let ids = [AgentId::new(), AgentId::new()];
    let jobs = |cap| {
        vec![
            BatchMember {
                id: ids[0].clone(),
                inputs: AdmissionInputs {
                    request: &first,
                    effective: &first_effective,
                    authority: &authority,
                    candidates: &candidates,
                    identity: &first_identity,
                    global_cap: cap,
                    harness_cap: None,
                    pinned: None,
                },
            },
            BatchMember {
                id: ids[1].clone(),
                inputs: AdmissionInputs {
                    request: &second,
                    effective: &second_effective,
                    authority: &authority,
                    candidates: &candidates,
                    identity: &second_identity,
                    global_cap: cap,
                    harness_cap: None,
                    pinned: None,
                },
            },
        ]
    };
    let revision = store.quota_capacity_revision().unwrap();
    // Fail the second owner observation after the first job was written in the
    // same transaction. Neither the partial job nor the receipt may survive.
    agent_run_store::fixture::fail_owner_inspection_after(1);
    let owner_failure = store.admit_batch(BatchAdmission {
        request_id: &request.request_id,
        fingerprint: &fingerprint,
        retain_history: true,
        expected_store_id: Some(&store_id),
        preconditions: None,
        catalog: &catalog,
        jobs: jobs(4),
    });
    assert!(matches!(owner_failure, Err(Error::Io(_))));
    for table in ["agents", "attempts", "start_batch_receipts", "history_pins"] {
        let count: i64 = store
            .conn
            .query_row(&format!("SELECT COUNT(*) FROM {table}"), [], |r| r.get(0))
            .unwrap();
        assert_eq!(count, 0, "{table} must roll back after owner failure");
    }
    assert_eq!(store.quota_capacity_revision().unwrap(), revision);
    let result = store.admit_batch(BatchAdmission {
        request_id: &request.request_id,
        fingerprint: &fingerprint,
        retain_history: true,
        expected_store_id: Some(&store_id),
        preconditions: None,
        catalog: &catalog,
        jobs: jobs(1),
    });
    assert!(matches!(result, Err(Error::Capacity)));
    for table in ["agents", "attempts", "start_batch_receipts", "history_pins"] {
        let count: i64 = store
            .conn
            .query_row(&format!("SELECT COUNT(*) FROM {table}"), [], |r| r.get(0))
            .unwrap();
        assert_eq!(count, 0, "{table} must roll back");
    }
    assert_eq!(store.quota_capacity_revision().unwrap(), revision);

    // The writer rechecks the restart fence even if preflight ran earlier.
    let restart = store.begin_service_restart("batch-restart", 1.0).unwrap();
    let blocked = store.admit_batch(BatchAdmission {
        request_id: &request.request_id,
        fingerprint: &fingerprint,
        retain_history: true,
        expected_store_id: Some(&store_id),
        preconditions: None,
        catalog: &catalog,
        jobs: jobs(4),
    });
    assert!(matches!(blocked, Err(Error::Transition(_))));
    let count: i64 = store
        .conn
        .query_row("SELECT COUNT(*) FROM start_batch_receipts", [], |r| {
            r.get(0)
        })
        .unwrap();
    assert_eq!(count, 0);
    store
        .conn
        .execute(
            "UPDATE service_restarts SET phase='complete',finished_at=2 WHERE id=?",
            [restart],
        )
        .unwrap();

    let accepted = store
        .admit_batch(BatchAdmission {
            request_id: &request.request_id,
            fingerprint: &fingerprint,
            retain_history: true,
            expected_store_id: Some(&store_id),
            preconditions: None,
            catalog: &catalog,
            jobs: jobs(4),
        })
        .unwrap();
    assert!(!accepted.replayed);
    assert_eq!(accepted.receipt.store_id, store_id);
    let pins: Vec<_> = accepted
        .receipt
        .jobs
        .iter()
        .map(|job| job.pin_id.clone().unwrap())
        .collect();
    assert_ne!(pins[0], pins[1]);
    let other_home = self::home();
    let other_store = agent_run_store::fixture::open(other_home.path()).unwrap();
    let other_id = agent_run_store::store_identity::read(&other_store.conn).unwrap();
    assert_ne!(store_id, other_id);
    let mut foreign_receipt = accepted.receipt.clone();
    foreign_receipt.store_id = other_id.clone();
    other_store.conn.execute(
        "INSERT INTO start_batch_receipts(request_id,fingerprint_version,fingerprint,receipt_json) VALUES(?,1,?,?)",
        rusqlite::params![request.request_id, "a".repeat(64), serde_json::to_string(&foreign_receipt).unwrap()],
    ).unwrap();
    assert!(matches!(
        other_store.replay_batch_scoped(&request.request_id, &fingerprint, Some(&store_id)),
        Err(Error::StoreIdentityMismatch)
    ));
    let wrong_writer = other_store.admit_batch(BatchAdmission {
        request_id: &request.request_id,
        fingerprint: &fingerprint,
        retain_history: true,
        expected_store_id: Some(&store_id),
        preconditions: None,
        catalog: &catalog,
        jobs: jobs(4),
    });
    assert!(matches!(wrong_writer, Err(Error::StoreIdentityMismatch)));
    for table in ["agents", "attempts", "history_pins"] {
        let count: i64 = other_store
            .conn
            .query_row(&format!("SELECT COUNT(*) FROM {table}"), [], |row| {
                row.get(0)
            })
            .unwrap();
        assert_eq!(count, 0, "mismatch cannot admit {table}");
    }
    assert!(matches!(
        store.release_history_pin_scoped(&pins[0], Some(&other_id)),
        Err(Error::StoreIdentityMismatch)
    ));
    assert!(matches!(
        store.history_pins_scoped(
            agent_run_domain::batch::HistoryPinFilter::All,
            10,
            None,
            Some(&other_id)
        ),
        Err(Error::StoreIdentityMismatch)
    ));
    let active: i64 = store
        .conn
        .query_row(
            "SELECT released FROM history_pins WHERE pin_id=?",
            [pins[0].as_str()],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(active, 0);
    let mut unpinned = request.clone();
    unpinned.retain_history = false;
    assert!(matches!(
        store.replay_batch(&request.request_id, &unpinned.fingerprint().unwrap()),
        Err(Error::Conflict)
    ));
    let mut unscoped = request.clone();
    unscoped.expected_store_id = None;
    assert!(matches!(
        store.replay_batch_scoped(
            &request.request_id,
            &unscoped.fingerprint().unwrap(),
            Some(&store_id)
        ),
        Err(Error::Conflict)
    ));
    assert_eq!(
        accepted
            .receipt
            .jobs
            .iter()
            .map(|job| &job.agent_id)
            .collect::<Vec<_>>(),
        vec![&ids[0], &ids[1]]
    );
    for job in &accepted.receipt.jobs {
        let first_attempt: String = store
            .conn
            .query_row(
                "SELECT id FROM attempts WHERE agent_id=? AND number=1",
                [job.agent_id.as_str()],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(
            job.execution_ref,
            agent_run_domain::batch::ExecutionRef::from_first_provider_attempt(
                &job.agent_id,
                &first_attempt
            )
            .unwrap()
        );
    }
    assert_ne!(
        accepted.receipt.jobs[0].execution_ref,
        accepted.receipt.jobs[1].execution_ref
    );
    // A later provider attempt is not a new execution and cannot replace the
    // number-one attempt used to derive the retained identity.
    let later = format!("att_{}", uuid::Uuid::new_v4().simple());
    store.conn.execute(
        "INSERT INTO attempts(id,agent_id,number,state,adapter_state_json,created_at,phase,ownership_active) VALUES(?,?,2,'prepared','{}',1,'prepared',0)",
        rusqlite::params![later, ids[0].as_str()],
    ).unwrap();
    let replay = store
        .admit_batch(BatchAdmission {
            request_id: &request.request_id,
            fingerprint: &fingerprint,
            retain_history: true,
            expected_store_id: Some(&store_id),
            preconditions: None,
            catalog: &catalog,
            jobs: jobs(1),
        })
        .unwrap();
    assert!(replay.replayed);
    assert_eq!(replay.receipt, accepted.receipt);
    // Exact replay bypasses the owner probe, while a new scalar admission fails closed.
    agent_run_store::fixture::fail_owner_inspection_after(0);
    assert!(
        store
            .admit_batch(BatchAdmission {
                request_id: &request.request_id,
                fingerprint: &fingerprint,
                retain_history: true,
                expected_store_id: Some(&store_id),
                preconditions: None,
                catalog: &catalog,
                jobs: jobs(1),
            })
            .unwrap()
            .replayed
    );
    let fresh_candidates =
        self::candidates(store.quota_capacity_revision().unwrap(), &[("acct-a", 0)]);
    let scalar = store.admit_provider(
        &first,
        &first_effective,
        &catalog,
        &authority,
        &fresh_candidates,
        &first_identity,
        4,
        None,
        None,
    );
    assert!(matches!(scalar, Err(Error::Io(_))));
    for pin in &pins {
        let first_release = store.release_history_pin(pin).unwrap().unwrap();
        assert!(!first_release.already_released);
        assert!(
            store
                .release_history_pin(pin)
                .unwrap()
                .unwrap()
                .already_released
        );
    }
    assert_eq!(
        store
            .replay_batch(&request.request_id, &fingerprint)
            .unwrap()
            .unwrap()
            .receipt,
        accepted.receipt
    );
    assert!(matches!(
        store.replay_batch(&request.request_id, &"a".repeat(64)),
        Err(Error::Conflict)
    ));
    store.conn.execute("INSERT INTO start_batch_receipts(request_id,fingerprint_version,fingerprint,receipt_json) VALUES('unknown-codec',99,?,'{}')", [&fingerprint]).unwrap();
    assert!(matches!(
        store.replay_batch("unknown-codec", &fingerprint),
        Err(Error::Unsupported(_))
    ));
    let mut malformed = serde_json::to_value(&accepted.receipt).unwrap();
    malformed["request_id"] = json!("missing-incarnation");
    malformed["jobs"][0]
        .as_object_mut()
        .unwrap()
        .remove("execution_ref");
    store.conn.execute("INSERT INTO start_batch_receipts(request_id,fingerprint_version,fingerprint,receipt_json) VALUES('missing-incarnation',1,?,?)",
        rusqlite::params![fingerprint, malformed.to_string()]).unwrap();
    assert!(matches!(
        store.replay_batch("missing-incarnation", &fingerprint),
        Err(Error::Integrity(_))
    ));
    assert!(
        store
            .conn
            .execute(
                "DELETE FROM start_batch_receipts WHERE request_id='batch-key'",
                []
            )
            .is_err()
    );

    store.conn.execute("UPDATE attempts SET ownership_active=0,phase='cleanup_complete',cleanup_proof_json='{\"never_spawned\":true}',finished_at=1", []).unwrap();
    store
        .conn
        .execute("UPDATE agents SET status='failed',finished_at=1", [])
        .unwrap();
    for _ in 0..20 {
        if store
            .prune_history(agent_run_domain::domain::now() + 15.0 * 86400.0)
            .unwrap()
            == 0
        {
            break;
        }
    }
    let remaining: i64 = store
        .conn
        .query_row("SELECT COUNT(*) FROM agents", [], |r| r.get(0))
        .unwrap();
    assert_eq!(remaining, 0);
    drop(store);
    let reopened = agent_run_store::fixture::open(home.path()).unwrap();
    assert_eq!(
        agent_run_store::store_identity::read(&reopened.conn).unwrap(),
        store_id
    );
    assert_eq!(
        reopened
            .replay_batch_scoped(&request.request_id, &fingerprint, Some(&store_id))
            .unwrap()
            .unwrap()
            .receipt,
        accepted.receipt
    );
}

/// Freezes the provider/model/account scope with a valid role revision.
fn authority(catalog: &ProviderCatalog, workdir: &Path) -> ResolvedLaunchAuthority {
    let provider = catalog.provider(&"glm-user".parse().unwrap()).unwrap();
    let mut role = json!({"role_name":"review"});
    role["config_revision"] = json!(agent_run_domain::canonical::sha256_hex(&role, true));
    ResolvedLaunchAuthority {
        provider: provider.id.clone(),
        harness: provider.harness,
        connection: provider.connection.clone(),
        model: "fixture".into(),
        effort: None,
        profile: "review".into(),
        workdir: workdir.canonicalize().unwrap(),
        role_payload: role,
        assets_sha256: "0".repeat(64).parse().unwrap(),
        eligible_accounts: vec![
            "acct-a".parse().unwrap(),
            "acct-b".parse().unwrap(),
            "acct-disabled".parse().unwrap(),
            "acct-ineligible".parse().unwrap(),
        ],
    }
}

/// Supplies one producer-ranked account with its exact physical lane.
fn candidate(id: &str, rank: u32) -> QuotaCandidate {
    let account: AccountId = id.parse().unwrap();
    QuotaCandidate {
        account: account.clone(),
        rank,
        physical_keys: vec![PhysicalQuotaKey::new(&account, "tokens").unwrap()],
        multiplier: PositiveFinite::try_from(1.0).unwrap(),
        quota_known: true,
    }
}

/// Returns one trusted set; rank ties are resolved only by active count/id.
fn candidates(revision: i64, items: &[(&str, u32)]) -> QuotaCandidateSet {
    QuotaCandidateSet {
        provider: "glm-user".parse().unwrap(),
        model: "fixture".into(),
        intent: SelectionIntent::Auto,
        candidates: items
            .iter()
            .map(|(id, rank)| candidate(id, *rank))
            .collect(),
        capacity_revision: revision,
        quota_decision: None,
    }
}

/// Supplies the explicit identity hash checked by replay before mutable state.
fn identity(request: &ProviderStartRequest, authority: &ResolvedLaunchAuthority) -> Value {
    json!({
        "provider_identity_version":2,
        "provider_request":request,
        "replay_request_sha256":agent_run_domain::canonical::sha256_hex(
            &serde_json::to_value(request).unwrap(), true),
        "authority":authority,
    })
}

mod p3 {
    use super::*;
    use agent_run_domain::{
        attempt_binding::Route,
        domain::AgentId,
        enrollment::{Key, Start},
    };
    use agent_run_store::{
        batch_admission::{BatchAdmission, BatchMember},
        managed_enrollment::Grant,
        provider_admission::AdmissionInputs,
    };

    fn settings(home: &Path, grant: &Grant, enabled: bool) -> String {
        format!(
            "schema_version=2\n[managed_attempt_binding]\nenabled={enabled}\nrole_name='review'\npool_server='pool'\npool_executable_sha256='{}'\nbridge_hook_sha256='{}'\nhost_id='host'\npool_resident_executable='/usr/bin/false'\npool_resident_sha256='{}'\n[managed_attempt_binding.transport]\nrun_route={{component_store_id='{}',route_id='run',worker_uid={}}}\npool_route={{component_store_id='{}',route_id='pool',worker_uid={}}}\nbridge_route={{component_store_id='{}',route_id='bridge',worker_uid={}}}\nproducer='pool'\npool_socket='{}/pool.sock'\nbridge_socket='{}/bridge.sock'\nbridge_executable='/usr/bin/true'\nbridge_executable_sha256='{}'\nbridge_reader_credential_file='{}/reader'\nbridge_reader_credential_sha256='{}'\n",
            "a".repeat(64),
            "b".repeat(64),
            "c".repeat(64),
            grant.run_route.component_store_id,
            grant.run_route.worker_uid,
            grant.pool_route.component_store_id,
            grant.pool_route.worker_uid,
            grant.bridge_route.component_store_id,
            grant.bridge_route.worker_uid,
            home.display(),
            home.display(),
            "d".repeat(64),
            home.display(),
            "e".repeat(64)
        )
    }

    fn fixture() -> (tempfile::TempDir, Store<'static>, Grant, String) {
        let home = super::home();
        let store = agent_run_store::fixture::open(home.path()).unwrap();
        let sid = agent_run_store::store_identity::read(&store.conn).unwrap();
        // SAFETY: geteuid is read-only.
        let uid = unsafe { libc::geteuid() };
        let grant = Grant {
            pool_route: Route {
                component_store_id: "00000000-0000-4000-8000-000000000001".into(),
                route_id: "pool".into(),
                worker_uid: uid,
            },
            run_route: Route {
                component_store_id: sid.to_string(),
                route_id: "run".into(),
                worker_uid: uid,
            },
            bridge_route: Route {
                component_store_id: "00000000-0000-4000-8000-000000000002".into(),
                route_id: "bridge".into(),
                worker_uid: uid,
            },
            producer: "pool".into(),
            host_id: "host".into(),
            transport_pin_sha256: "0".repeat(64),
        };
        std::fs::write(
            home.path().join("config.toml"),
            settings(home.path(), &grant, true),
        )
        .unwrap();
        let (config, revision) =
            agent_run_config::provider_config::ProviderConfig::load(home.path()).unwrap();
        let grant = Grant::from_config(&config).unwrap();
        (home, store, grant, revision)
    }

    fn intent(home: &Path, store: &Store, grant: &Grant, id: &str, count: usize) -> (Value, Key) {
        let mut jobs = Vec::new();
        for index in 0..count {
            let mut spec =
                serde_json::to_value(super::provider_request(home, "discarded")).unwrap();
            spec.as_object_mut().unwrap().remove("request_id");
            spec["task"] = json!(format!("member {index}"));
            jobs.push(json!({"member_id":format!("member-{index}"),"selector":{"version":1,"role_name":"review"},"spec":spec}));
        }
        let expected_store_id = agent_run_store::store_identity::read(&store.conn).unwrap();
        let value = json!({"version":1,"expected_store_id":expected_store_id,"pool_store_id":grant.pool_route.component_store_id,"pool_id":"pool-1","request_id":id,"mode":"start","jobs":jobs,"preconditions":[]});
        let (_, fingerprint) = Start::parse(&value).unwrap();
        let key = Key {
            version: 1,
            expected_store_id,
            pool_store_id: grant.pool_route.component_store_id.clone(),
            request_id: id.into(),
            fingerprint,
        };
        (value, key)
    }

    #[test]
    fn p3_absent_abort_late_submit_and_disabled_recovery() {
        let (home, store, grant, _) = fixture();
        let (request, key) = intent(home.path(), &store, &grant, "before-stage", 1);
        assert!(
            store
                .managed_enrollment_lookup(&key, &grant.pool_route)
                .unwrap()
                .is_none()
        );
        assert_eq!(
            store
                .managed_enrollment_abort(&key, &grant.pool_route)
                .unwrap()
                .state,
            "aborted"
        );
        assert_eq!(
            store
                .managed_enrollment_prepare(&key, &grant, &request)
                .unwrap()
                .state,
            "aborted"
        );
        let (request, key) = intent(home.path(), &store, &grant, "during-stage", 1);
        assert_eq!(
            store
                .managed_enrollment_prepare(&key, &grant, &request)
                .unwrap()
                .state,
            "prepared"
        );
        assert_eq!(
            store
                .managed_enrollment_abort(&key, &grant.pool_route)
                .unwrap()
                .state,
            "aborted"
        );
        assert_eq!(
            store
                .managed_enrollment_abort(&key, &grant.pool_route)
                .unwrap()
                .state,
            "aborted"
        );
        let mut changed = key.clone();
        changed.fingerprint = "f".repeat(64);
        assert!(matches!(
            store.managed_enrollment_lookup(&changed, &grant.pool_route),
            Err(Error::Conflict)
        ));
        let (pending, pending_key) = intent(home.path(), &store, &grant, "pending-disabled", 1);
        assert_eq!(
            store
                .managed_enrollment_prepare(&pending_key, &grant, &pending)
                .unwrap()
                .state,
            "prepared"
        );
        std::fs::write(
            home.path().join("config.toml"),
            settings(home.path(), &grant, false),
        )
        .unwrap();
        assert!(
            store
                .managed_enrollment_prepare(&pending_key, &grant, &pending)
                .is_err()
        );
        assert_eq!(
            store
                .managed_enrollment_lookup(&pending_key, &grant.pool_route)
                .unwrap()
                .unwrap()
                .state,
            "prepared"
        );
        assert_eq!(
            store
                .managed_enrollment_abort(&pending_key, &grant.pool_route)
                .unwrap()
                .state,
            "aborted"
        );
        assert_eq!(
            store
                .managed_enrollment_lookup(&key, &grant.pool_route)
                .unwrap()
                .unwrap()
                .state,
            "aborted"
        );
        let (fresh, fresh_key) = intent(home.path(), &store, &grant, "disabled", 1);
        assert!(
            store
                .managed_enrollment_prepare(&fresh_key, &grant, &fresh)
                .is_err()
        );
        assert!(
            store
                .managed_enrollment_lookup(&fresh_key, &grant.pool_route)
                .unwrap()
                .is_none()
        );
    }

    #[test]
    fn p3_batch_enrollment_replay_crash_and_membership_ever() {
        let (home, mut store, grant, revision) = fixture();
        let (raw, key) = intent(home.path(), &store, &grant, "batch", 2);
        let (start, _) = Start::parse(&raw).unwrap();
        store
            .managed_enrollment_prepare(&key, &grant, &raw)
            .unwrap();
        let catalog = super::catalog(&store);
        let authority = super::authority(&catalog, home.path());
        let role_digest = authority.role_payload["config_revision"]
            .as_str()
            .unwrap()
            .to_owned();
        let mut requests = Vec::new();
        for member in &start.jobs {
            let mut request: ProviderStartRequest =
                serde_json::from_value(member.spec.clone()).unwrap();
            request.validate().unwrap();
            requests.push(request);
        }
        let effective: Vec<_> = requests
            .iter()
            .map(ProviderStartRequest::storage_projection)
            .collect();
        let identities: Vec<_> = requests
            .iter()
            .map(|request| {
                let mut identity = super::identity(request, &authority);
                identity["provider_config_sha256"] = json!(revision);
                identity
            })
            .collect();
        let candidates =
            super::candidates(store.quota_capacity_revision().unwrap(), &[("acct-a", 0)]);
        let ids = [AgentId::new(), AgentId::new()];
        let jobs = || {
            (0..2)
                .map(|i| BatchMember {
                    id: ids[i].clone(),
                    inputs: AdmissionInputs {
                        request: &requests[i],
                        effective: &effective[i],
                        authority: &authority,
                        candidates: &candidates,
                        identity: &identities[i],
                        global_cap: 4,
                        harness_cap: None,
                        pinned: None,
                    },
                })
                .collect()
        };
        let submit = || BatchAdmission {
            request_id: "p3-internal",
            fingerprint: &key.fingerprint,
            retain_history: true,
            expected_store_id: Some(&key.expected_store_id),
            preconditions: None,
            catalog: &catalog,
            jobs: jobs(),
        };
        let digests = vec![role_digest.clone(), role_digest];
        agent_run_store::fixture::fail_owner_inspection_after(1);
        assert!(
            store
                .managed_enrollment_admit(&key, &grant, &revision, &digests, submit())
                .is_err()
        );
        for table in [
            "agents",
            "attempts",
            "history_pins",
            "managed_enrollment_members",
            "start_batch_receipts",
        ] {
            let count: i64 = store
                .conn
                .query_row(&format!("SELECT count(*) FROM {table}"), [], |r| r.get(0))
                .unwrap();
            assert_eq!(count, 0, "partial {table}");
        }
        assert_eq!(
            store
                .managed_enrollment_lookup(&key, &grant.pool_route)
                .unwrap()
                .unwrap()
                .state,
            "prepared"
        );
        let accepted = store
            .managed_enrollment_admit(&key, &grant, &revision, &digests, submit())
            .unwrap();
        assert!(!accepted.replayed);
        assert_eq!(accepted.receipt.jobs.len(), 2);
        for (index, job) in accepted.receipt.jobs.iter().enumerate() {
            assert_eq!(job.index, index);
            assert_eq!(job.lineage_root_id, ids[index]);
            assert_eq!(
                store
                    .conn
                    .query_row(
                        "SELECT pool_membership_ever FROM agents WHERE id=?",
                        [job.lineage_root_id.as_str()],
                        |r| r.get::<_, i64>(0)
                    )
                    .unwrap(),
                1
            );
            assert!(store.existing_pool_member(&job.lineage_root_id).is_err());
        }
        assert!(
            store
                .conn
                .execute("DELETE FROM managed_enrollment_members", [])
                .is_err()
        );
        let replay = store
            .managed_enrollment_admit(&key, &grant, &revision, &digests, submit())
            .unwrap();
        assert!(replay.replayed);
        assert_eq!(
            serde_json::to_value(replay.receipt).unwrap(),
            serde_json::to_value(&accepted.receipt).unwrap()
        );
        let (late, late_key) = intent(home.path(), &store, &grant, "late-aborted", 2);
        store
            .managed_enrollment_prepare(&late_key, &grant, &late)
            .unwrap();
        store
            .managed_enrollment_abort(&late_key, &grant.pool_route)
            .unwrap();
        assert!(
            matches!(store.managed_enrollment_admit(&late_key,&grant,&revision,&digests,submit()),Err(Error::Validation(ref text)) if text == "enrollment aborted")
        );
        assert!(
            store
                .release_history_pin(&accepted.receipt.jobs[0].pin_id)
                .unwrap()
                .is_some()
        );
        assert!(
            store
                .existing_pool_member(&accepted.receipt.jobs[0].lineage_root_id)
                .is_err()
        );
        assert_eq!(
            store
                .conn
                .query_row("SELECT count(*) FROM managed_enrollment_members", [], |r| r
                    .get::<_, i64>(0))
                .unwrap(),
            2
        );
        std::fs::write(
            home.path().join("config.toml"),
            settings(home.path(), &grant, false),
        )
        .unwrap();
        let recovered = store
            .managed_enrollment_abort(&key, &grant.pool_route)
            .unwrap();
        assert_eq!(recovered.state, "admitted");
        assert_eq!(
            serde_json::to_value(recovered.receipt.unwrap()).unwrap(),
            serde_json::to_value(&accepted.receipt).unwrap()
        );
        let mut foreign = grant.pool_route.clone();
        foreign.route_id = "other".into();
        assert!(matches!(
            store.managed_enrollment_lookup(&key, &foreign),
            Err(Error::Conflict)
        ));
    }

    #[test]
    fn p3_replace_precondition_and_resume_race() {
        use agent_run_domain::batch::BatchPrecondition;
        let (home, store, grant, revision) = fixture();
        let catalog = super::catalog(&store);
        let authority = super::authority(&catalog, home.path());
        let mut request = super::provider_request(home.path(), "old");
        request.request_id = None;
        let effective = request.storage_projection();
        let mut identity = super::identity(&request, &authority);
        identity["provider_config_sha256"] = json!(revision);
        let initial_candidates =
            super::candidates(store.quota_capacity_revision().unwrap(), &[("acct-a", 0)]);
        let old_id = AgentId::new();
        let old = store
            .admit_batch(BatchAdmission {
                request_id: "old-internal",
                fingerprint: &"d".repeat(64),
                retain_history: true,
                expected_store_id: Some(
                    &agent_run_store::store_identity::read(&store.conn).unwrap(),
                ),
                preconditions: None,
                catalog: &catalog,
                jobs: vec![BatchMember {
                    id: old_id.clone(),
                    inputs: AdmissionInputs {
                        request: &request,
                        effective: &effective,
                        authority: &authority,
                        candidates: &initial_candidates,
                        identity: &identity,
                        global_cap: 4,
                        harness_cap: None,
                        pinned: None,
                    },
                }],
            })
            .unwrap()
            .receipt
            .jobs
            .remove(0);
        let precondition = BatchPrecondition {
            agent_id: old.agent_id.clone(),
            sequence: old.sequence,
            execution_ref: old.execution_ref.clone(),
            pin_id: old.pin_id.unwrap(),
        };
        let (mut raw, _) = intent(home.path(), &store, &grant, "replace", 1);
        raw["mode"] = json!("replace");
        raw["jobs"][0]["member_id"] = json!("replacement");
        raw["preconditions"] = json!([precondition]);
        let (start, fingerprint) = Start::parse(&raw).unwrap();
        let key = Key {
            version: 1,
            expected_store_id: start.expected_store_id.clone(),
            pool_store_id: start.pool_store_id.clone(),
            request_id: start.request_id.clone(),
            fingerprint,
        };
        store
            .managed_enrollment_prepare(&key, &grant, &raw)
            .unwrap();
        let mut next: ProviderStartRequest =
            serde_json::from_value(start.jobs[0].spec.clone()).unwrap();
        next.validate().unwrap();
        let next_effective = next.storage_projection();
        let mut next_identity = super::identity(&next, &authority);
        next_identity["provider_config_sha256"] = json!(revision);
        let candidates =
            super::candidates(store.quota_capacity_revision().unwrap(), &[("acct-a", 0)]);
        let candidate_id = AgentId::new();
        let submit = || BatchAdmission {
            request_id: "replacement-internal",
            fingerprint: &key.fingerprint,
            retain_history: true,
            expected_store_id: Some(&key.expected_store_id),
            preconditions: Some(&start.preconditions),
            catalog: &catalog,
            jobs: vec![BatchMember {
                id: candidate_id.clone(),
                inputs: AdmissionInputs {
                    request: &next,
                    effective: &next_effective,
                    authority: &authority,
                    candidates: &candidates,
                    identity: &next_identity,
                    global_cap: 4,
                    harness_cap: None,
                    pinned: None,
                },
            }],
        };
        let role_digest = authority.role_payload["config_revision"]
            .as_str()
            .unwrap()
            .to_owned();
        assert!(matches!(
            store.managed_enrollment_admit(
                &key,
                &grant,
                &revision,
                std::slice::from_ref(&role_digest),
                submit()
            ),
            Err(Error::PreconditionFailed { .. })
        ));
        assert_eq!(
            store
                .conn
                .query_row("SELECT count(*) FROM agents", [], |r| r.get::<_, i64>(0))
                .unwrap(),
            1
        );
        store
            .conn
            .execute(
                "UPDATE agents SET status='failed',finished_at=10 WHERE id=?",
                [old_id.as_str()],
            )
            .unwrap();
        store.conn.execute("UPDATE attempts SET state='failed',finished_at=10,phase='cleanup_complete',ownership_active=0,cleanup_proof_json=? WHERE agent_id=?",rusqlite::params![json!({"confirmed":true,"group_gone":true,"descendants_gone":true}).to_string(),old_id.as_str()]).unwrap();
        let admitted = store
            .managed_enrollment_admit(
                &key,
                &grant,
                &revision,
                std::slice::from_ref(&role_digest),
                submit(),
            )
            .unwrap();
        assert_ne!(admitted.receipt.jobs[0].lineage_root_id, old_id);
        assert_eq!(admitted.receipt.jobs[0].lineage_root_id, candidate_id);
        // The Run admission-time snapshot does not hold or cancel the old lineage.
        store
            .conn
            .execute(
                "UPDATE agents SET status='running',finished_at=NULL WHERE id=?",
                [old_id.as_str()],
            )
            .unwrap();
        let replay = store
            .managed_enrollment_admit(
                &key,
                &grant,
                &revision,
                std::slice::from_ref(&role_digest),
                submit(),
            )
            .unwrap();
        assert!(replay.replayed);
        assert_eq!(
            serde_json::to_value(replay.receipt).unwrap(),
            serde_json::to_value(admitted.receipt).unwrap()
        );
    }

    #[test]
    fn p3_batch_twenty_jobs_are_atomic() {
        let (home, store, grant, revision) = fixture();
        let (raw, key) = intent(home.path(), &store, &grant, "twenty", 20);
        let (start, _) = Start::parse(&raw).unwrap();
        store
            .managed_enrollment_prepare(&key, &grant, &raw)
            .unwrap();
        let catalog = super::catalog(&store);
        let authority = super::authority(&catalog, home.path());
        let role_digest = authority.role_payload["config_revision"]
            .as_str()
            .unwrap()
            .to_owned();
        let requests: Vec<ProviderStartRequest> = start
            .jobs
            .iter()
            .map(|job| {
                let mut request: ProviderStartRequest =
                    serde_json::from_value(job.spec.clone()).unwrap();
                request.validate().unwrap();
                request
            })
            .collect();
        let effective: Vec<_> = requests
            .iter()
            .map(ProviderStartRequest::storage_projection)
            .collect();
        let identities: Vec<_> = requests
            .iter()
            .map(|request| {
                let mut value = super::identity(request, &authority);
                value["provider_config_sha256"] = json!(revision);
                value
            })
            .collect();
        let candidates =
            super::candidates(store.quota_capacity_revision().unwrap(), &[("acct-a", 0)]);
        let ids: Vec<_> = (0..20).map(|_| AgentId::new()).collect();
        let submit = || BatchAdmission {
            request_id: "twenty-internal",
            fingerprint: &key.fingerprint,
            retain_history: true,
            expected_store_id: Some(&key.expected_store_id),
            preconditions: None,
            catalog: &catalog,
            jobs: (0..20)
                .map(|i| BatchMember {
                    id: ids[i].clone(),
                    inputs: AdmissionInputs {
                        request: &requests[i],
                        effective: &effective[i],
                        authority: &authority,
                        candidates: &candidates,
                        identity: &identities[i],
                        global_cap: 25,
                        harness_cap: None,
                        pinned: None,
                    },
                })
                .collect(),
        };
        let digests = vec![role_digest; 20];
        agent_run_store::fixture::fail_owner_inspection_after(19);
        assert!(
            store
                .managed_enrollment_admit(&key, &grant, &revision, &digests, submit())
                .is_err()
        );
        for table in [
            "agents",
            "attempts",
            "history_pins",
            "managed_enrollment_members",
        ] {
            assert_eq!(
                store
                    .conn
                    .query_row(&format!("SELECT count(*) FROM {table}"), [], |row| row
                        .get::<_, i64>(0))
                    .unwrap(),
                0,
                "{table}"
            );
        }
        let admitted = store
            .managed_enrollment_admit(&key, &grant, &revision, &digests, submit())
            .unwrap();
        assert_eq!(admitted.receipt.jobs.len(), 20);
        for (index, job) in admitted.receipt.jobs.iter().enumerate() {
            assert_eq!(job.index, index);
            assert_eq!(job.lineage_root_id, ids[index]);
        }
        assert_eq!(
            store
                .conn
                .query_row(
                    "SELECT count(*) FROM history_pins WHERE released=0",
                    [],
                    |r| r.get::<_, i64>(0)
                )
                .unwrap(),
            20
        );
        assert_eq!(
            store
                .conn
                .query_row("SELECT count(*) FROM managed_enrollment_members", [], |r| r
                    .get::<_, i64>(0))
                .unwrap(),
            20
        );
    }

    #[test]
    fn p3_transport_repin_fences_old_key_but_not_new_enrollment() {
        let (home, store, old_grant, old_revision) = fixture();
        let catalog = super::catalog(&store);
        let authority = super::authority(&catalog, home.path());
        let role_digest = authority.role_payload["config_revision"]
            .as_str()
            .unwrap()
            .to_owned();
        let (old_raw, old_key) = intent(home.path(), &store, &old_grant, "before-upgrade", 1);
        store
            .managed_enrollment_prepare(&old_key, &old_grant, &old_raw)
            .unwrap();
        let old_start = Start::parse(&old_raw).unwrap().0;
        let mut old_request: ProviderStartRequest =
            serde_json::from_value(old_start.jobs[0].spec.clone()).unwrap();
        old_request.validate().unwrap();
        let old_effective = old_request.storage_projection();
        let mut old_identity = super::identity(&old_request, &authority);
        old_identity["provider_config_sha256"] = json!(old_revision);
        let old_candidates =
            super::candidates(store.quota_capacity_revision().unwrap(), &[("acct-a", 0)]);
        let old_id = AgentId::new();
        let old_batch = || BatchAdmission {
            request_id: "upgrade-old-internal",
            fingerprint: &old_key.fingerprint,
            retain_history: true,
            expected_store_id: Some(&old_key.expected_store_id),
            preconditions: None,
            catalog: &catalog,
            jobs: vec![BatchMember {
                id: old_id.clone(),
                inputs: AdmissionInputs {
                    request: &old_request,
                    effective: &old_effective,
                    authority: &authority,
                    candidates: &old_candidates,
                    identity: &old_identity,
                    global_cap: 5,
                    harness_cap: None,
                    pinned: None,
                },
            }],
        };
        let old_receipt = store
            .managed_enrollment_admit(
                &old_key,
                &old_grant,
                &old_revision,
                std::slice::from_ref(&role_digest),
                old_batch(),
            )
            .unwrap()
            .receipt;
        let (pending_raw, pending_key) =
            intent(home.path(), &store, &old_grant, "pending-before-upgrade", 1);
        store
            .managed_enrollment_prepare(&pending_key, &old_grant, &pending_raw)
            .unwrap();
        let updated = settings(home.path(), &old_grant, true)
            .replace("producer='pool'", "producer='pool2'")
            .replace("bridge.sock", "bridge-new.sock");
        std::fs::write(home.path().join("config.toml"), updated).unwrap();
        let (new_config, new_revision) =
            agent_run_config::provider_config::ProviderConfig::load(home.path()).unwrap();
        let new_grant = Grant::from_config(&new_config).unwrap();
        assert_ne!(
            new_grant.transport_pin_sha256,
            old_grant.transport_pin_sha256
        );
        assert!(
            matches!(store.managed_enrollment_prepare(&old_key,&new_grant,&old_raw),Err(Error::Validation(ref message)) if message=="enrollment transport changed")
        );
        assert!(
            matches!(store.managed_enrollment_prepare(&pending_key,&new_grant,&pending_raw),Err(Error::Validation(ref message)) if message=="enrollment transport changed")
        );
        assert_eq!(
            store
                .managed_enrollment_lookup(&old_key, &new_grant.pool_route)
                .unwrap()
                .unwrap()
                .state,
            "admitted"
        );
        assert_eq!(
            store
                .managed_enrollment_abort(&old_key, &new_grant.pool_route)
                .unwrap()
                .receipt
                .unwrap()
                .receipt_id,
            old_receipt.receipt_id
        );
        assert_eq!(
            store
                .managed_enrollment_abort(&pending_key, &new_grant.pool_route)
                .unwrap()
                .state,
            "aborted"
        );
        let (mut new_raw, _) = intent(home.path(), &store, &new_grant, "after-upgrade", 1);
        new_raw["jobs"][0]["member_id"] = json!("new-member");
        let (new_start, fingerprint) = Start::parse(&new_raw).unwrap();
        let new_key = Key {
            version: 1,
            expected_store_id: new_start.expected_store_id.clone(),
            pool_store_id: new_start.pool_store_id.clone(),
            request_id: new_start.request_id.clone(),
            fingerprint,
        };
        store
            .managed_enrollment_prepare(&new_key, &new_grant, &new_raw)
            .unwrap();
        let mut new_request: ProviderStartRequest =
            serde_json::from_value(new_start.jobs[0].spec.clone()).unwrap();
        new_request.validate().unwrap();
        let new_effective = new_request.storage_projection();
        let mut new_identity = super::identity(&new_request, &authority);
        new_identity["provider_config_sha256"] = json!(new_revision);
        let candidates =
            super::candidates(store.quota_capacity_revision().unwrap(), &[("acct-a", 0)]);
        let new_id = AgentId::new();
        let batch = BatchAdmission {
            request_id: "upgrade-new-internal",
            fingerprint: &new_key.fingerprint,
            retain_history: true,
            expected_store_id: Some(&new_key.expected_store_id),
            preconditions: None,
            catalog: &catalog,
            jobs: vec![BatchMember {
                id: new_id.clone(),
                inputs: AdmissionInputs {
                    request: &new_request,
                    effective: &new_effective,
                    authority: &authority,
                    candidates: &candidates,
                    identity: &new_identity,
                    global_cap: 5,
                    harness_cap: None,
                    pinned: None,
                },
            }],
        };
        let result = store
            .managed_enrollment_admit(
                &new_key,
                &new_grant,
                &new_revision,
                std::slice::from_ref(&role_digest),
                batch,
            )
            .unwrap();
        assert_eq!(result.receipt.jobs[0].lineage_root_id, new_id);
        assert_ne!(result.receipt.jobs[0].lineage_root_id, old_id);
    }
}

/// Adds a normalized frozen provider document and its digest to a fixture identity.
fn ordered_identity(
    request: &ProviderStartRequest,
    authority: &ResolvedLaunchAuthority,
    catalog: &ProviderCatalog,
) -> Value {
    let mut provider = serde_json::to_value(catalog.provider(&request.provider).unwrap()).unwrap();
    provider.as_object_mut().unwrap().remove("id");
    let mut identity = identity(request, authority);
    identity["provider_config"] = json!({
        "schema_version":2, "harnesses":{}, "providers":{"glm-user":provider}
    });
    identity["provider_config_snapshot"] =
        agent_run_config::provider_config::ProviderConfig::snapshot_document(
            &identity["provider_config"],
        )
        .unwrap();
    identity
}

/// The writer enforces frozen order even when the trusted producer's ranks
/// would select another account, including after a cleaned-up attempt.
#[test]
fn ordered_selection_is_frozen_and_rechecked_by_the_writer() {
    let home = home();
    let mut store = agent_run_store::fixture::open(home.path()).unwrap();
    store
        .register_account(&AccountRecord {
            account_id: "acct-c".parse().unwrap(),
            auth_family: "anthropic".parse().unwrap(),
            secret_ref: "named:claude-code:fixture".parse().unwrap(),
            status: AccountStatus::Enabled,
        })
        .unwrap();
    let mut provider = catalog(&store)
        .provider(&"glm-user".parse().unwrap())
        .unwrap()
        .clone();
    provider.ordered_accounts = true;
    provider.bindings = serde_json::from_value(json!([
        {"label":"api", "account":"acct-a", "priority_multiplier":10.0},
        {"label":"next", "account":"acct-b", "priority_multiplier":5.0},
        {"label":"native", "account":"acct-c", "priority_multiplier":1.0, "connection":{"kind":"native"}}
    ])).unwrap();
    let catalog =
        ProviderCatalog::new(store.list_accounts().unwrap(), vec![provider.clone()]).unwrap();
    let request = provider_request(home.path(), "ordered-start");
    let mut authority = authority(&catalog, home.path());
    authority.eligible_accounts.push("acct-c".parse().unwrap());
    let frozen = ordered_identity(&request, &authority, &catalog);
    let revision = store.quota_capacity_revision().unwrap();
    let offered = candidates(revision, &[("acct-c", 0), ("acct-b", 1), ("acct-a", 2)]);
    let admitted = store
        .admit_provider(
            &request,
            &request.storage_projection(),
            &catalog,
            &authority,
            &offered,
            &frozen,
            8,
            None,
            None,
        )
        .unwrap();
    assert_eq!(admitted.account_id.as_str(), "acct-a");
    store.conn.execute(
        "UPDATE attempts SET phase='cleanup_complete',cleanup_proof_json='{\"confirmed\":true}',adapter_state_json=? WHERE id=?",
        rusqlite::params![json!({"native_history":{"seal":{"session":"s"},"task_proven":true},"native_failure":{"class":"quota_exhausted"}}).to_string(), admitted.attempt_id],
    ).unwrap();
    store
        .conn
        .execute(
            "UPDATE agents SET status='running' WHERE id=?",
            [admitted.agent_id.as_str()],
        )
        .unwrap();
    let revision = store.quota_capacity_revision().unwrap();
    let offered_next = candidates(revision, &[("acct-c", 0), ("acct-b", 1)]);

    // Supplying a different catalog cannot disable or reorder the frozen queue.
    let mut spoofed = provider.clone();
    spoofed.ordered_accounts = false;
    spoofed.bindings[2].connection = None;
    let spoofed = ProviderCatalog::new(store.list_accounts().unwrap(), vec![spoofed]).unwrap();
    assert!(
        store
            .allocate_next_attempt(&admitted.agent_id, &spoofed, &offered_next)
            .is_err()
    );
    assert_eq!(ownership(&store, &admitted.agent_id), (1, 1, 1));
    let next = store
        .allocate_next_attempt(&admitted.agent_id, &catalog, &offered_next)
        .unwrap();
    assert_eq!(next.account_id.as_str(), "acct-b");
    assert_eq!(next.number, 2);
    assert_eq!(ownership(&store, &admitted.agent_id), (1, 0, 2));

    // Exact replay returns the original admission, not the current attempt.
    let replay = store
        .admit_provider(
            &request,
            &request.storage_projection(),
            &catalog,
            &authority,
            &offered,
            &frozen,
            1,
            None,
            None,
        )
        .unwrap();
    assert!(!replay.created);
    assert_eq!(replay.attempt_id, admitted.attempt_id);

    // A pinned request remains pinned even if other ordered candidates exist.
    let mut pinned_request = provider_request(home.path(), "ordered-pinned");
    pinned_request.account = Some("native".parse().unwrap());
    let pinned_id: AccountId = "acct-c".parse().unwrap();
    let mut pinned_candidates = candidates(
        store.quota_capacity_revision().unwrap(),
        &[("acct-a", 0), ("acct-c", 1)],
    );
    pinned_candidates.intent = SelectionIntent::Pinned(pinned_id.clone());
    let pinned = store
        .admit_provider(
            &pinned_request,
            &pinned_request.storage_projection(),
            &catalog,
            &authority,
            &pinned_candidates,
            &ordered_identity(&pinned_request, &authority, &catalog),
            8,
            None,
            Some(&pinned_id),
        )
        .unwrap();
    assert_eq!(pinned.account_id, pinned_id);
    store.conn.execute(
        "UPDATE attempts SET phase='cleanup_complete',cleanup_proof_json='{\"confirmed\":true}',adapter_state_json=? WHERE id=?",
        rusqlite::params![json!({"native_history":{"seal":{"session":"s"},"task_proven":true},"native_failure":{"class":"quota_exhausted"}}).to_string(), pinned.attempt_id],
    ).unwrap();
    let offered = candidates(
        store.quota_capacity_revision().unwrap(),
        &[("acct-a", 0), ("acct-b", 1)],
    );
    assert!(matches!(
        store.allocate_next_attempt(&pinned.agent_id, &catalog, &offered),
        Err(Error::QuotaAdmission(
            QuotaAdmissionError::NoEligibleAccount { .. }
        ))
    ));
}

/// Billing fallback is limited to the exact cleaned custom Claude attempt;
/// it preserves quota facts and never mistakes billing trouble for exhaustion.
#[test]
fn ordered_billing_fallback_requires_matching_custom_attempt() {
    let home = home();
    let mut store = agent_run_store::fixture::open(home.path()).unwrap();
    store
        .register_account(&AccountRecord {
            account_id: "acct-c".parse().unwrap(),
            auth_family: "anthropic".parse().unwrap(),
            secret_ref: "named:claude-code:fixture".parse().unwrap(),
            status: AccountStatus::Enabled,
        })
        .unwrap();
    let mut provider = catalog(&store)
        .provider(&"glm-user".parse().unwrap())
        .unwrap()
        .clone();
    provider.ordered_accounts = true;
    provider.bindings = serde_json::from_value(json!([
        {"label":"api", "account":"acct-a", "priority_multiplier":10.0},
        {"label":"native", "account":"acct-c", "priority_multiplier":5.0, "connection":{"kind":"native"}},
        {"label":"later", "account":"acct-b", "priority_multiplier":1.0}
    ]))
    .unwrap();
    let catalog = ProviderCatalog::new(store.list_accounts().unwrap(), vec![provider]).unwrap();
    let request = provider_request(home.path(), "ordered-billing");
    let mut authority = authority(&catalog, home.path());
    authority.eligible_accounts.push("acct-c".parse().unwrap());
    let frozen = ordered_identity(&request, &authority, &catalog);
    let offered = candidates(
        store.quota_capacity_revision().unwrap(),
        &[("acct-a", 0), ("acct-c", 1), ("acct-b", 2)],
    );
    let admitted = store
        .admit_provider(
            &request,
            &request.storage_projection(),
            &catalog,
            &authority,
            &offered,
            &frozen,
            8,
            None,
            None,
        )
        .unwrap();
    assert_eq!(admitted.account_id.as_str(), "acct-a");
    // A second request can already be in flight when the first failure lands.
    let concurrent_request = provider_request(home.path(), "ordered-billing-concurrent");
    let concurrent = store
        .admit_provider(
            &concurrent_request,
            &concurrent_request.storage_projection(),
            &catalog,
            &authority,
            &candidates(
                store.quota_capacity_revision().unwrap(),
                &[("acct-a", 0), ("acct-c", 1)],
            ),
            &ordered_identity(&concurrent_request, &authority, &catalog),
            8,
            None,
            None,
        )
        .unwrap();
    assert_eq!(concurrent.account_id.as_str(), "acct-a");
    store.conn.execute(
        "UPDATE attempts SET phase='cleanup_complete',cleanup_proof_json='{\"confirmed\":true}' WHERE id=?",
        [&concurrent.attempt_id],
    ).unwrap();
    store.conn.execute(
        "UPDATE attempts SET phase='cleanup_complete',cleanup_proof_json='{\"confirmed\":true}' WHERE id=?",
        [admitted.attempt_id.as_str()],
    ).unwrap();
    store
        .conn
        .execute(
            "UPDATE agents SET status='running' WHERE id=?",
            [admitted.agent_id.as_str()],
        )
        .unwrap();
    let evidence = json!({
        "native_history":{"seal":{"session":"s"},"task_proven":true},
        "native_failure":{"class":"billing_unavailable","attempt":admitted.attempt_id,
            "account":"acct-a","provider":"glm-user","model":"fixture"}
    });
    for (field, value) in [
        ("attempt", json!("foreign-attempt")),
        ("account", json!("acct-c")),
        ("provider", json!("foreign-provider")),
        ("model", json!("other")),
        ("class", json!("throttled")),
    ] {
        let mut invalid = evidence.clone();
        invalid["native_failure"][field] = value;
        store
            .conn
            .execute(
                "UPDATE attempts SET adapter_state_json=? WHERE id=?",
                rusqlite::params![invalid.to_string(), admitted.attempt_id],
            )
            .unwrap();
        let offered = candidates(
            store.quota_capacity_revision().unwrap(),
            &[("acct-c", 0), ("acct-b", 1)],
        );
        assert!(
            store
                .allocate_next_attempt(&admitted.agent_id, &catalog, &offered)
                .is_err(),
            "{field}"
        );
        assert_eq!(ownership(&store, &admitted.agent_id), (1, 2, 1));
    }
    let mut unproven = evidence.clone();
    unproven["native_history"]["task_proven"] = json!(false);
    store
        .conn
        .execute(
            "UPDATE attempts SET adapter_state_json=? WHERE id=?",
            rusqlite::params![unproven.to_string(), admitted.attempt_id],
        )
        .unwrap();
    let offered = candidates(store.quota_capacity_revision().unwrap(), &[("acct-c", 0)]);
    assert!(
        store
            .allocate_next_attempt(&admitted.agent_id, &catalog, &offered)
            .is_err()
    );
    assert_eq!(ownership(&store, &admitted.agent_id), (1, 2, 1));
    store
        .conn
        .execute(
            "UPDATE attempts SET adapter_state_json=? WHERE id=?",
            rusqlite::params![evidence.to_string(), admitted.attempt_id],
        )
        .unwrap();
    let at = agent_run_domain::domain::now();
    let before_revision = store.quota_capacity_revision().unwrap();
    for class in ["throttled", "auth", "network", "context_window", "other"] {
        let mut invalid = evidence.clone();
        invalid["native_failure"]["class"] = json!(class);
        invalid["binding_cooldown_recorded"] = json!("billing_unavailable");
        assert!(
            store
                .provider_history_with_cooldown(
                    &admitted.agent_id,
                    &admitted.attempt_id,
                    &invalid,
                    Some(agent_run_domain::catalog::BindingCooldownReason::BillingUnavailable),
                    at,
                )
                .is_err(),
            "{class} is not a billing refusal"
        );
    }
    assert_eq!(
        store
            .conn
            .query_row("SELECT COUNT(*) FROM binding_cooldowns", [], |row| row
                .get::<_, i64>(0))
            .unwrap(),
        0
    );
    assert_eq!(store.quota_capacity_revision().unwrap(), before_revision);
    let mut cooled = evidence.clone();
    cooled["binding_cooldown_recorded"] = json!("billing_unavailable");
    store
        .provider_history_with_cooldown(
            &admitted.agent_id,
            &admitted.attempt_id,
            &cooled,
            Some(agent_run_domain::catalog::BindingCooldownReason::BillingUnavailable),
            at,
        )
        .unwrap();
    assert_eq!(
        store.quota_capacity_revision().unwrap(),
        before_revision + 1
    );
    let mut older = cooled.clone();
    older["native_failure"]["attempt"] = json!(concurrent.attempt_id);
    store
        .provider_history_with_cooldown(
            &concurrent.agent_id,
            &concurrent.attempt_id,
            &older,
            Some(agent_run_domain::catalog::BindingCooldownReason::BillingUnavailable),
            at - 100.0,
        )
        .unwrap();
    assert_eq!(
        store.quota_capacity_revision().unwrap(),
        before_revision + 1,
        "a late older failure must not shorten the current cooldown"
    );
    // Retrying the same attempt must not extend its original one-hour window.
    store
        .provider_history_with_cooldown(
            &admitted.agent_id,
            &admitted.attempt_id,
            &cooled,
            Some(agent_run_domain::catalog::BindingCooldownReason::BillingUnavailable),
            at + 1200.0,
        )
        .unwrap();
    assert_eq!(
        store.quota_capacity_revision().unwrap(),
        before_revision + 1
    );
    let provider = "glm-user".parse().unwrap();
    let account = "acct-a".parse().unwrap();
    assert_eq!(
        agent_run_store::provider_admission::binding_cooldown(
            &store.conn,
            &provider,
            &account,
            at + 3599.1
        )
        .unwrap(),
        Some((
            agent_run_domain::catalog::BindingCooldownReason::BillingUnavailable,
            1
        ))
    );
    assert_eq!(
        agent_run_store::provider_admission::binding_cooldown(
            &store.conn,
            &provider,
            &account,
            at + 3600.0
        )
        .unwrap(),
        None
    );
    assert_eq!(
        agent_run_store::provider_admission::binding_cooldown(
            &store.conn,
            &"glm-alias".parse().unwrap(),
            &account,
            at
        )
        .unwrap(),
        None
    );
    let stale = candidates(before_revision, &[("acct-c", 0)]);
    assert!(matches!(
        store.allocate_next_attempt(&admitted.agent_id, &catalog, &stale),
        Err(Error::QuotaAdmission(
            QuotaAdmissionError::SelectionStale { .. }
        ))
    ));
    assert_eq!(ownership(&store, &admitted.agent_id), (1, 2, 1));
    let automatic = provider_request(home.path(), "cooldown-auto");
    let forged = candidates(
        store.quota_capacity_revision().unwrap(),
        &[("acct-a", 0), ("acct-c", 1)],
    );
    let selected = store
        .admit_provider(
            &automatic,
            &automatic.storage_projection(),
            &catalog,
            &authority,
            &forged,
            &ordered_identity(&automatic, &authority, &catalog),
            8,
            None,
            None,
        )
        .unwrap();
    assert_eq!(
        selected.account_id.as_str(),
        "acct-c",
        "transaction must recheck cooldown even for a stale ranking"
    );
    let mut pinned = provider_request(home.path(), "cooldown-pinned");
    pinned.account = Some("api".parse().unwrap());
    let mut pinned_set = candidates(store.quota_capacity_revision().unwrap(), &[("acct-a", 0)]);
    pinned_set.intent = SelectionIntent::Pinned(account.clone());
    let refusal = store
        .admit_provider(
            &pinned,
            &pinned.storage_projection(),
            &catalog,
            &authority,
            &pinned_set,
            &ordered_identity(&pinned, &authority, &catalog),
            8,
            None,
            Some(&account),
        )
        .unwrap_err();
    assert!(matches!(
        refusal,
        Error::QuotaAdmission(QuotaAdmissionError::BindingCooldown { .. })
    ));
    let offered = candidates(
        store.quota_capacity_revision().unwrap(),
        &[("acct-b", 0), ("acct-c", 1)],
    );
    let next = store
        .allocate_next_attempt(&admitted.agent_id, &catalog, &offered)
        .unwrap();
    assert_eq!(next.account_id.as_str(), "acct-c");
    let closed: String = store
        .conn
        .query_row(
            "SELECT state FROM attempts WHERE id=?",
            [admitted.attempt_id.as_str()],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(closed, "failed");
    let latches: i64 = store
        .conn
        .query_row("SELECT COUNT(*) FROM quota_exhaustion", [], |row| {
            row.get(0)
        })
        .unwrap();
    assert_eq!(latches, 0);
    let native_failure = json!({
        "native_history":{"seal":{"session":"s"},"task_proven":true},
        "native_failure":{"class":"billing_unavailable","attempt":next.attempt_id,
            "account":"acct-c","provider":"glm-user","model":"fixture"}
    });
    store.conn.execute(
        "UPDATE attempts SET phase='cleanup_complete',cleanup_proof_json='{\"confirmed\":true}',adapter_state_json=? WHERE id=?",
        rusqlite::params![native_failure.to_string(), next.attempt_id],
    ).unwrap();
    let offered = candidates(store.quota_capacity_revision().unwrap(), &[("acct-b", 0)]);
    assert!(
        store
            .allocate_next_attempt(&admitted.agent_id, &catalog, &offered)
            .is_err()
    );
    let count: i64 = store
        .conn
        .query_row(
            "SELECT COUNT(*) FROM attempts WHERE agent_id=?",
            [admitted.agent_id.as_str()],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(
        count, 2,
        "native billing trouble cannot create another attempt"
    );
    let mut wrong = native_failure.clone();
    wrong["binding_cooldown_recorded"] = json!("billing_unavailable");
    assert!(
        store
            .provider_history_with_cooldown(
                &admitted.agent_id,
                &next.attempt_id,
                &wrong,
                Some(agent_run_domain::catalog::BindingCooldownReason::BillingUnavailable),
                at,
            )
            .is_err(),
        "native subscription billing is not an API billing cooldown"
    );
    let subscription = "acct-c".parse().unwrap();
    assert_eq!(
        agent_run_store::provider_admission::binding_cooldown(
            &store.conn,
            &provider,
            &subscription,
            at
        )
        .unwrap(),
        None
    );
    let mut quota = native_failure;
    quota["native_failure"]["class"] = json!("quota_exhausted");
    quota["native_failure"]["signal"] = json!("claude.rate_limit_event.rejected");
    quota["native_failure"]["window"] = json!("five_hour");
    quota["binding_cooldown_recorded"] = json!("quota_exhausted");
    store
        .provider_history_with_cooldown(
            &admitted.agent_id,
            &next.attempt_id,
            &quota,
            Some(agent_run_domain::catalog::BindingCooldownReason::QuotaExhausted),
            at,
        )
        .unwrap();
    assert_eq!(
        agent_run_store::provider_admission::binding_cooldown(
            &store.conn,
            &provider,
            &subscription,
            at
        )
        .unwrap(),
        Some((
            agent_run_domain::catalog::BindingCooldownReason::QuotaExhausted,
            3600
        ))
    );
}

/// Historical attempts without provider identity can still record cleanup history.
#[test]
fn legacy_history_without_provider_identity_remains_writable() {
    let home = home();
    let mut store = agent_run_store::fixture::open(home.path()).unwrap();
    let catalog = catalog(&store);
    let request = provider_request(home.path(), "legacy-history");
    let authority = authority(&catalog, home.path());
    let offered = candidates(store.quota_capacity_revision().unwrap(), &[("acct-a", 0)]);
    let admitted = store
        .admit_provider(
            &request,
            &request.storage_projection(),
            &catalog,
            &authority,
            &offered,
            &identity(&request, &authority),
            8,
            None,
            None,
        )
        .unwrap();
    store
        .conn
        .execute(
            "UPDATE agents SET identity_json=NULL WHERE id=?",
            [admitted.agent_id.as_str()],
        )
        .unwrap();
    store
        .conn
        .execute(
            "UPDATE attempts SET phase='cleanup_complete' WHERE id=?",
            [&admitted.attempt_id],
        )
        .unwrap();
    let state = json!({"native_history_unavailable":"legacy"});
    store
        .provider_history(&admitted.agent_id, &admitted.attempt_id, &state)
        .unwrap();
    assert_eq!(
        store.latest_attempt_state(&admitted.agent_id).unwrap(),
        state.to_string()
    );
}

/// A provider start from OpenCode retains one execution and session across request replay.
#[test]
fn opencode_orchestrator_provider_replay_preserves_one_admission() {
    let home = home();
    let mut store = agent_run_store::fixture::open(home.path()).unwrap();
    let catalog = catalog(&store);
    let mut request = provider_request(home.path(), "opencode-root-start");
    request.orchestrator = Some(agent_run_domain::domain::OrchestratorRef {
        transport: "opencode".into(),
        external_session_id: "root-session".into(),
        external_turn_id: Some("input-1".into()),
    });
    request.validate().unwrap();
    let authority = authority(&catalog, home.path());
    let identity = identity(&request, &authority);
    let candidates = candidates(store.quota_capacity_revision().unwrap(), &[("acct-a", 0)]);
    let mut first = None;
    for created in [true, false] {
        let result = store
            .admit_provider(
                &request,
                &request.storage_projection(),
                &catalog,
                &authority,
                &candidates,
                &identity,
                2,
                None,
                None,
            )
            .unwrap();
        assert_eq!(result.created, created);
        let original = first.get_or_insert(result.agent_id.clone());
        assert_eq!(*original, result.agent_id);
    }
    let (rows, total) = store
        .list(false, 0, 10, request.orchestrator.as_ref())
        .unwrap();
    assert_eq!(total, 1);
    assert_eq!(rows[0].id, first.unwrap());
}

/// Stale input changes no row; eligible rank ties use account id, physical
/// reservations are exact, and replay wins after the revision changes.
#[test]
fn stale_replay_and_ranked_reservation_are_atomic() {
    let home = home();
    let mut store = agent_run_store::fixture::open(home.path()).unwrap();
    let catalog = catalog(&store);
    let request = provider_request(home.path(), "req-one");
    let effective = request.storage_projection();
    let authority = authority(&catalog, home.path());
    let frozen_identity = identity(&request, &authority);
    let ordered = [
        ("acct-b", 0),
        ("acct-disabled", 0),
        ("acct-ineligible", 0),
        ("acct-foreign", 0),
        ("acct-a", 0),
    ];
    let stale = store
        .admit_provider(
            &request,
            &effective,
            &catalog,
            &authority,
            &candidates(3, &ordered),
            &frozen_identity,
            2,
            None,
            None,
        )
        .unwrap_err();
    assert!(
        matches!(
            stale,
            Error::QuotaAdmission(QuotaAdmissionError::SelectionStale { .. })
        ),
        "{stale:?}"
    );
    let count: i64 = store
        .conn
        .query_row("SELECT COUNT(*) FROM agents", [], |r| r.get(0))
        .unwrap();
    assert_eq!(count, 0);
    // Registration advanced the committed revision; candidates carry it.
    let revision = store.quota_capacity_revision().unwrap();
    let admitted = store
        .admit_provider(
            &request,
            &effective,
            &catalog,
            &authority,
            &candidates(revision, &ordered),
            &frozen_identity,
            2,
            None,
            None,
        )
        .unwrap();
    assert!(admitted.created);
    assert_eq!(admitted.account_id.as_str(), "acct-a");
    assert_eq!(store.quota_capacity_revision().unwrap(), revision + 1);
    let facts: (String, String, i64) = store.conn.query_row(
        "SELECT a.selection_intent,k.quota_key,t.ownership_active FROM agents a \
         JOIN attempts t ON t.agent_id=a.id JOIN attempt_quota_keys k ON k.attempt_id=t.id WHERE a.id=?",
        [admitted.agent_id.as_str()], |r| Ok((r.get(0)?,r.get(1)?,r.get(2)?)),
    ).unwrap();
    assert_eq!(facts, ("auto".into(), "acct-a::tokens".into(), 1));
    // Once the spawn has begun, the attempt cannot release without a
    // cleanup proof (only a never-spawned prepared attempt gets one at the
    // terminal commit).
    store
        .conn
        .execute(
            "UPDATE attempts SET phase='spawning' WHERE id=?",
            [&admitted.attempt_id],
        )
        .unwrap();
    assert!(
        store
            .finish(&admitted.agent_id, &Outcome::failure("test"), None, None)
            .is_err()
    );
    assert_eq!(
        store
            .conn
            .query_row(
                "SELECT ownership_active FROM attempts WHERE id=?",
                [&admitted.attempt_id],
                |row| row.get::<_, i64>(0),
            )
            .unwrap(),
        1
    );
    let mut alias_request = provider_request(home.path(), "req-alias");
    alias_request.provider = "glm-alias".parse().unwrap();
    let mut alias_authority = authority.clone();
    alias_authority.provider = alias_request.provider.clone();
    let mut alias_candidates = candidates(revision + 1, &[("acct-a", 0)]);
    alias_candidates.provider = alias_request.provider.clone();
    let alias = store
        .admit_provider(
            &alias_request,
            &alias_request.storage_projection(),
            &catalog,
            &alias_authority,
            &alias_candidates,
            &identity(&alias_request, &alias_authority),
            3,
            Some(2),
            None,
        )
        .unwrap();
    assert_eq!(alias.account_id.as_str(), "acct-a");
    assert_eq!(store.quota_capacity_revision().unwrap(), revision + 2);
    assert_eq!(
        store
            .active_reservation_counts(&[
                PhysicalQuotaKey::new(&alias.account_id, "tokens").unwrap()
            ])
            .unwrap()["acct-a::tokens"],
        2
    );
    store.disable_account(&"acct-a".parse().unwrap()).unwrap();
    let replay = store
        .admit_provider(
            &request,
            &effective,
            &catalog,
            &authority,
            &candidates(revision, &ordered),
            &frozen_identity,
            1,
            None,
            None,
        )
        .unwrap();
    assert!(!replay.created);
    assert_eq!(replay.attempt_id, admitted.attempt_id);
    let next = provider_request(home.path(), "req-two");
    let current = store.quota_capacity_revision().unwrap();
    let cap = store
        .admit_provider(
            &next,
            &next.storage_projection(),
            &catalog,
            &authority,
            &candidates(current, &[("acct-b", 0)]),
            &identity(&next, &authority),
            1,
            None,
            None,
        )
        .unwrap_err();
    assert!(matches!(cap, Error::Capacity));
}

/// Disabled, foreign and model-ineligible higher-ranked candidates cannot
/// override a lower-ranked account still in the frozen eligible scope.
#[test]
fn filters_invalid_candidates_before_rank_tie_break() {
    let home = home();
    let mut store = agent_run_store::fixture::open(home.path()).unwrap();
    let catalog = catalog(&store);
    let request = provider_request(home.path(), "filtered");
    let authority = authority(&catalog, home.path());
    let revision = store.quota_capacity_revision().unwrap();
    let admitted = store
        .admit_provider(
            &request,
            &request.storage_projection(),
            &catalog,
            &authority,
            &candidates(
                revision,
                &[
                    ("acct-disabled", 0),
                    ("acct-ineligible", 0),
                    ("acct-foreign", 0),
                    ("acct-b", 1),
                ],
            ),
            &identity(&request, &authority),
            2,
            None,
            None,
        )
        .unwrap();
    assert_eq!(admitted.account_id.as_str(), "acct-b");
}

/// A second provider alias on the same harness cannot bypass its process
/// ceiling even when the global ceiling and an account lane still have room.
#[test]
fn harness_cap_covers_provider_aliases() {
    let home = home();
    let mut store = agent_run_store::fixture::open(home.path()).unwrap();
    let catalog = catalog(&store);
    let first = provider_request(home.path(), "first");
    let first_authority = authority(&catalog, home.path());
    let revision = store.quota_capacity_revision().unwrap();
    store
        .admit_provider(
            &first,
            &first.storage_projection(),
            &catalog,
            &first_authority,
            &candidates(revision, &[("acct-a", 0)]),
            &identity(&first, &first_authority),
            2,
            Some(1),
            None,
        )
        .unwrap();
    let mut alias = provider_request(home.path(), "second");
    alias.provider = "glm-alias".parse().unwrap();
    let mut alias_authority = first_authority;
    alias_authority.provider = alias.provider.clone();
    let mut alias_candidates = candidates(revision + 1, &[("acct-a", 0)]);
    alias_candidates.provider = alias.provider.clone();
    let refusal = store
        .admit_provider(
            &alias,
            &alias.storage_projection(),
            &catalog,
            &alias_authority,
            &alias_candidates,
            &identity(&alias, &alias_authority),
            2,
            Some(1),
            None,
        )
        .unwrap_err();
    assert!(matches!(refusal, Error::Capacity));
    assert_eq!(store.quota_capacity_revision().unwrap(), revision + 1);
}

/// Two independent store connections cannot both commit against one global
/// revision; one wins and the other observes stale capacity.
#[test]
fn concurrent_provider_admissions_share_one_committed_revision() {
    let home = home();
    let catalog = catalog(&agent_run_store::fixture::open(home.path()).unwrap());
    let authority = authority(&catalog, home.path());
    let barrier = Arc::new(Barrier::new(2));
    let mut workers = Vec::new();
    for suffix in ["first", "second"] {
        let path = home.path().to_path_buf();
        let catalog = catalog.clone();
        let authority = authority.clone();
        let barrier = Arc::clone(&barrier);
        workers.push(std::thread::spawn(move || {
            let mut store = agent_run_store::fixture::open(&path).unwrap();
            let request = provider_request(&path, suffix);
            let revision = store.quota_capacity_revision().unwrap();
            barrier.wait();
            store.admit_provider(
                &request,
                &request.storage_projection(),
                &catalog,
                &authority,
                &candidates(revision, &[("acct-a", 0)]),
                &identity(&request, &authority),
                2,
                None,
                None,
            )
        }));
    }
    let results: Vec<_> = workers
        .into_iter()
        .map(|worker| worker.join().unwrap())
        .collect();
    assert_eq!(
        results.iter().filter(|result| result.is_ok()).count(),
        1,
        "{results:?}"
    );
    assert_eq!(
        results
            .iter()
            .filter(|result| matches!(
                result,
                Err(Error::QuotaAdmission(
                    QuotaAdmissionError::SelectionStale { .. }
                ))
            ))
            .count(),
        1
    );
    let store = agent_run_store::fixture::open(home.path()).unwrap();
    assert_eq!(
        store
            .conn
            .query_row("SELECT COUNT(*) FROM agents", [], |r| r.get::<_, i64>(0))
            .unwrap(),
        1
    );
}

/// Admits one auto run (`request_id`) that selects `acct-a`, then marks its
/// attempt cleaned up with a recorded history seal (as the supervisor does
/// at its cleanup boundary) while the logical agent stays running.
fn cleaned_run(
    home: &Path,
    request_id: &str,
    seal: bool,
) -> (Store<'static>, agent_run_domain::domain::AgentId) {
    let mut store = agent_run_store::fixture::open(home).unwrap();
    let catalog = catalog(&store);
    let request = provider_request(home, request_id);
    let authority = authority(&catalog, home);
    let revision = store.quota_capacity_revision().unwrap();
    let admission = store
        .admit_provider(
            &request,
            &request.storage_projection(),
            &catalog,
            &authority,
            &candidates(revision, &[("acct-a", 0), ("acct-b", 1)]),
            &identity(&request, &authority),
            8,
            None,
            None,
        )
        .unwrap();
    assert_eq!(admission.account_id.as_str(), "acct-a");
    let state = if seal {
        json!({"native_history":{"seal":{"session":"s"},"task_proven":true},"native_failure":{"class":"quota_exhausted"}}).to_string()
    } else {
        json!({"native_failure":{"class":"quota_exhausted"}}).to_string()
    };
    store
        .conn
        .execute(
            "UPDATE attempts SET phase='cleanup_complete',process_identity='p',cleanup_proof_json='{\"confirmed\":true}',adapter_state_json=? WHERE id=?",
            rusqlite::params![state, admission.attempt_id],
        )
        .unwrap();
    store
        .conn
        .execute(
            "UPDATE agents SET status='running' WHERE id=?",
            [admission.agent_id.as_str()],
        )
        .unwrap();
    (store, admission.agent_id)
}

/// Owned attempts and reservations of one agent: (owned attempt count,
/// open physical-key reservations for `acct-a`, total attempts).
fn ownership(store: &Store, id: &agent_run_domain::domain::AgentId) -> (i64, u64, i64) {
    let count = |sql: &str| -> i64 {
        store
            .conn
            .query_row(sql, [id.as_str()], |row| row.get(0))
            .unwrap()
    };
    let reserved = Store::active_reservation_counts_in(
        &store.conn,
        &[PhysicalQuotaKey::new(&"acct-a".parse().unwrap(), "tokens").unwrap()],
    )
    .unwrap()
    .into_values()
    .sum();
    (
        count("SELECT COUNT(*) FROM attempts WHERE agent_id=? AND ownership_active=1"),
        reserved,
        count("SELECT COUNT(*) FROM attempts WHERE agent_id=?"),
    )
}

/// A sealed history that does not prove the admitted task (meta-only or an
/// early rejection) cannot allocate another attempt: the typed
/// continuation refusal writes nothing.
#[test]
fn next_attempt_requires_the_admitted_task_proof() {
    let home = home();
    let (mut store, id) = cleaned_run(home.path(), "unproven-1", true);
    let catalog = catalog(&store);
    for proof in [json!(false), serde_json::Value::Null] {
        let mut state = json!({"native_history":{"seal":{"session":"s"}},"native_failure":{"class":"quota_exhausted"}});
        if !proof.is_null() {
            state["native_history"]["task_proven"] = proof;
        }
        store
            .conn
            .execute(
                "UPDATE attempts SET adapter_state_json=? WHERE agent_id=?",
                rusqlite::params![state.to_string(), id.as_str()],
            )
            .unwrap();
        let before = ownership(&store, &id);
        let revision = store.quota_capacity_revision().unwrap();
        let refused = store
            .allocate_next_attempt(&id, &catalog, &candidates(revision, &[("acct-b", 0)]))
            .unwrap_err();
        assert!(
            refused
                .to_string()
                .contains("admitted task never reached native history"),
            "{refused}"
        );
        assert_eq!(ownership(&store, &id), before);
    }
}

/// After verified cleanup the next attempt goes to an untried account on the
/// same logical agent, releasing A and reserving B exactly once; a stale
/// revision, missing cleanup or seal, the already-tried account (also via an
/// alias), a pending cancel and a second allocator are each refused with
/// nothing written.
#[test]
fn next_attempt_allocation_is_atomic_and_evidence_bound() {
    let home = home();
    let path = home.path();
    let (mut store, id) = cleaned_run(path, "next-1", true);
    let catalog = catalog(&store);
    let revision = store.quota_capacity_revision().unwrap();
    // Stale candidates are refused.
    let stale = store
        .allocate_next_attempt(&id, &catalog, &candidates(revision - 1, &[("acct-b", 0)]))
        .unwrap_err();
    assert!(matches!(
        stale,
        Error::QuotaAdmission(QuotaAdmissionError::SelectionStale { .. })
    ));
    // Only the already-tried account (or nothing new) is offered: refused.
    let tried = store
        .allocate_next_attempt(&id, &catalog, &candidates(revision, &[("acct-a", 0)]))
        .unwrap_err();
    assert!(matches!(
        tried,
        Error::QuotaAdmission(QuotaAdmissionError::NoEligibleAccount { .. })
    ));
    assert_eq!(ownership(&store, &id), (1, 1, 1));
    // A better-ranked tried account never wins; B is allocated once.
    let next = store
        .allocate_next_attempt(
            &id,
            &catalog,
            &candidates(revision, &[("acct-a", 0), ("acct-b", 1)]),
        )
        .unwrap();
    assert_eq!(next.account_id.as_str(), "acct-b");
    assert_eq!(next.number, 2);
    assert_eq!(ownership(&store, &id), (1, 0, 2), "A released, B owned");
    let event_attempt: Option<String> = store
        .conn
        .query_row(
            "SELECT attempt_id FROM events WHERE agent_id=? AND kind='attempt_allocated'",
            [id.as_str()],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(event_attempt.as_deref(), Some(next.attempt_id.as_str()));
    // B is not cleaned up yet: a second allocation is refused.
    let revision = store.quota_capacity_revision().unwrap();
    assert!(
        store
            .allocate_next_attempt(&id, &catalog, &candidates(revision, &[("acct-b", 0)]))
            .is_err()
    );
    assert_eq!(ownership(&store, &id), (1, 0, 2));

    // Missing continuation evidence and pending cancel refuse.
    let (mut bare, bare_id) = cleaned_run(path, "next-2", false);
    let revision = bare.quota_capacity_revision().unwrap();
    let error = bare
        .allocate_next_attempt(&bare_id, &catalog, &candidates(revision, &[("acct-b", 0)]))
        .unwrap_err();
    assert!(
        error.to_string().contains("continuation_unavailable"),
        "{error}"
    );
    let (mut cancelled, cancel_id) = cleaned_run(path, "next-3", true);
    cancelled.enqueue(&cancel_id, "cancel", &json!({})).unwrap();
    let revision = cancelled.quota_capacity_revision().unwrap();
    assert!(
        cancelled
            .allocate_next_attempt(
                &cancel_id,
                &catalog,
                &candidates(revision, &[("acct-b", 0)])
            )
            .is_err()
    );
    assert_eq!(ownership(&cancelled, &cancel_id).2, 1);
}

/// A pinned run never takes an attempt on another account.
#[test]
fn pinned_runs_never_allocate_another_account() {
    let home = home();
    let (mut store, id) = cleaned_run(home.path(), "pinned-1", true);
    store
        .conn
        .execute(
            "UPDATE agents SET selection_intent='pinned',requested_account_id='acct-a' WHERE id=?",
            [id.as_str()],
        )
        .unwrap();
    let catalog = catalog(&store);
    let revision = store.quota_capacity_revision().unwrap();
    assert!(
        store
            .allocate_next_attempt(&id, &catalog, &candidates(revision, &[("acct-b", 0)]))
            .is_err()
    );
    assert_eq!(ownership(&store, &id), (1, 1, 1));
}

/// Two concurrent allocators for one agent: exactly one owns the next
/// attempt; there are never two owned attempts.
#[test]
fn concurrent_allocators_admit_one_next_attempt() {
    let home = home();
    let path = home.path().to_path_buf();
    let (store, id) = cleaned_run(&path, "race-1", true);
    let revision = store.quota_capacity_revision().unwrap();
    drop(store);
    let barrier = Arc::new(Barrier::new(2));
    let wins: usize = (0..2)
        .map(|_| {
            let (path, id, barrier) = (path.clone(), id.clone(), barrier.clone());
            std::thread::spawn(move || {
                let mut store = agent_run_store::fixture::open(&path).unwrap();
                let catalog = catalog(&store);
                barrier.wait();
                store
                    .allocate_next_attempt(&id, &catalog, &candidates(revision, &[("acct-b", 0)]))
                    .is_ok() as usize
            })
        })
        .collect::<Vec<_>>()
        .into_iter()
        .map(|handle| handle.join().unwrap())
        .sum();
    assert_eq!(wins, 1);
    let store = agent_run_store::fixture::open(&path).unwrap();
    assert_eq!(ownership(&store, &id), (1, 0, 2));
}

/// Late records keep their originating attempt: after the next attempt is
/// allocated, a handle bound to the released attempt still writes that
/// attempt's id (never the new owner's, never NULL); an unbound handle
/// keeps the historical rule (current owner); a binding naming another
/// agent's attempt records NULL rather than a guess.
#[test]
fn late_records_keep_their_originating_attempt() {
    let home = home();
    let (mut store, id) = cleaned_run(home.path(), "late-1", true);
    let first: String = store
        .conn
        .query_row(
            "SELECT id FROM attempts WHERE agent_id=?",
            [id.as_str()],
            |row| row.get(0),
        )
        .unwrap();
    let catalog = catalog(&store);
    let revision = store.quota_capacity_revision().unwrap();
    let next = store
        .allocate_next_attempt(&id, &catalog, &candidates(revision, &[("acct-b", 0)]))
        .unwrap();
    let mut late = agent_run_store::fixture::open(home.path()).unwrap();
    late.bind_attempt(&first);
    late.event(&id, "process_cleanup", &json!({"late":true}))
        .unwrap();
    late.message(&id, "assistant", "final words", None, None)
        .unwrap();
    store.event(&id, "unbound", &json!({})).unwrap();
    let (_, other) = cleaned_run(home.path(), "late-2", true);
    let mut foreign = agent_run_store::fixture::open(home.path()).unwrap();
    foreign.bind_attempt(&first);
    foreign.event(&other, "foreign", &json!({})).unwrap();
    let attempt_of = |sql: &str, agent: &str| -> Option<String> {
        store
            .conn
            .query_row(sql, [agent], |row| row.get(0))
            .unwrap()
    };
    assert_eq!(
        attempt_of(
            "SELECT attempt_id FROM events WHERE agent_id=? AND kind='process_cleanup'",
            id.as_str()
        )
        .as_deref(),
        Some(first.as_str())
    );
    assert_eq!(
        attempt_of(
            "SELECT attempt_id FROM messages WHERE agent_id=? AND content='final words'",
            id.as_str()
        )
        .as_deref(),
        Some(first.as_str())
    );
    assert_eq!(
        attempt_of(
            "SELECT attempt_id FROM events WHERE agent_id=? AND kind='unbound'",
            id.as_str()
        )
        .as_deref(),
        Some(next.attempt_id.as_str())
    );
    assert_eq!(
        attempt_of(
            "SELECT attempt_id FROM events WHERE agent_id=? AND kind='foreign'",
            other.as_str()
        ),
        None
    );
}

/// A runtime home with a still-prepared storage layout admits no provider
/// continuation, an already-admitted request id still replays, and the same
/// resume succeeds once the layout is committed.
#[test]
fn prepared_storage_layout_gates_provider_resume_only() {
    use agent_run_store::provider_admission::ProviderResume;
    let home = home();
    let mut store = agent_run_store::fixture::open(home.path()).unwrap();
    let catalog = catalog(&store);
    let request = provider_request(home.path(), "layout-parent");
    let authority = authority(&catalog, home.path());
    let revision = store.quota_capacity_revision().unwrap();
    let admitted = store
        .admit_provider(
            &request,
            &request.storage_projection(),
            &catalog,
            &authority,
            &candidates(revision, &[("acct-a", 0)]),
            &identity(&request, &authority),
            8,
            None,
            None,
        )
        .unwrap();
    // Seal the parent as the supervisor would: verified cleanup, a terminal
    // status with no live process, a native session and its frozen home.
    let runtime_home = home.path().join("runtime").to_string_lossy().to_string();
    store
        .conn
        .execute(
            "UPDATE attempts SET phase='cleanup_complete',process_identity='p',\
             cleanup_proof_json='{\"confirmed\":true}' WHERE id=?",
            [&admitted.attempt_id],
        )
        .unwrap();
    store
        .conn
        .execute(
            "UPDATE agents SET status='failed',finished_at=2.0,runtime_session_id='sess-a',\
             identity_json=json_set(identity_json,'$.runtime_home',?1) WHERE id=?",
            rusqlite::params![runtime_home, admitted.agent_id.as_str()],
        )
        .unwrap();
    let layout = serde_json::to_string(&serde_json::json!({
        "version": 1,
        "runtime_home": runtime_home,
        "index_sha256": "1".repeat(64),
        "roots": {"assets": {"scope": "2".repeat(64), "manifest_sha256": "3".repeat(64)}},
    }))
    .unwrap();
    let prepared = store.prepare_runtime_storage_layout(&layout, None).unwrap();
    let resume_request = provider_request(home.path(), "layout-child");
    let mut resume_identity = identity(&resume_request, &authority);
    resume_identity["runtime_home"] = serde_json::json!(runtime_home);
    let error = store
        .admit_provider_resume(
            &resume_request,
            &resume_request.storage_projection(),
            &catalog,
            &authority,
            &candidates(store.quota_capacity_revision().unwrap(), &[("acct-a", 0)]),
            &resume_identity,
            8,
            None,
            None,
            ProviderResume {
                parent: &admitted.agent_id,
                prefer: &"acct-a".parse().unwrap(),
                preflight_digest: None,
                restart_id: None,
            },
        )
        .unwrap_err();
    assert!(
        matches!(&error, Error::Unsupported(message) if message.contains("prepared storage layout")),
        "{error:?}"
    );
    let children: i64 = store
        .conn
        .query_row(
            "SELECT COUNT(*) FROM agents WHERE parent_agent_id IS NOT NULL",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(children, 0);
    // The parent's own request id is an already-created execution, so its
    // replay is unaffected by the pending layout.
    let replay = store
        .admit_provider(
            &request,
            &request.storage_projection(),
            &catalog,
            &authority,
            &candidates(revision, &[("acct-a", 0)]),
            &identity(&request, &authority),
            8,
            None,
            None,
        )
        .unwrap();
    assert!(!replay.created);
    store
        .commit_runtime_storage_layout(
            &runtime_home,
            &prepared.operation_token,
            &prepared.layout_sha256,
        )
        .unwrap();
    let stale = agent_run_store::provider_admission::resume_preflight_digest(
        &store.conn,
        &admitted.agent_id,
    )
    .unwrap();
    store
        .conn
        .execute(
            "UPDATE attempts SET adapter_state_json='{\"changed\":true}' WHERE id=?",
            [&admitted.attempt_id],
        )
        .unwrap();
    let refused = store.admit_provider_resume(
        &resume_request,
        &resume_request.storage_projection(),
        &catalog,
        &authority,
        &candidates(store.quota_capacity_revision().unwrap(), &[("acct-a", 0)]),
        &resume_identity,
        8,
        None,
        None,
        ProviderResume {
            parent: &admitted.agent_id,
            prefer: &"acct-a".parse().unwrap(),
            preflight_digest: Some(&stale),
            restart_id: None,
        },
    );
    assert!(matches!(refused, Err(Error::ExecutionChanged)));
    let children: i64 = store
        .conn
        .query_row(
            "SELECT COUNT(*) FROM agents WHERE parent_agent_id=?",
            [admitted.agent_id.as_str()],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(children, 0);
    let current = agent_run_store::provider_admission::resume_preflight_digest(
        &store.conn,
        &admitted.agent_id,
    )
    .unwrap();
    let resumed = store
        .admit_provider_resume(
            &resume_request,
            &resume_request.storage_projection(),
            &catalog,
            &authority,
            &candidates(store.quota_capacity_revision().unwrap(), &[("acct-a", 0)]),
            &resume_identity,
            8,
            None,
            None,
            ProviderResume {
                parent: &admitted.agent_id,
                prefer: &"acct-a".parse().unwrap(),
                preflight_digest: Some(&current),
                restart_id: None,
            },
        )
        .unwrap();
    assert!(resumed.created);
    let replay = store
        .admit_provider_resume(
            &resume_request,
            &resume_request.storage_projection(),
            &catalog,
            &authority,
            &candidates(store.quota_capacity_revision().unwrap(), &[("acct-a", 0)]),
            &resume_identity,
            8,
            None,
            None,
            ProviderResume {
                parent: &admitted.agent_id,
                prefer: &"acct-a".parse().unwrap(),
                preflight_digest: Some(&stale),
                restart_id: None,
            },
        )
        .unwrap();
    assert!(!replay.created);
    assert_eq!(replay.agent_id, resumed.agent_id);
}

/// The writer evaluates exact lineage proofs atomically and never rechecks a replay.
#[test]
fn batch_preconditions_fence_resume_and_preserve_replay() {
    use agent_run_domain::{
        PreconditionFailureReason,
        batch::{BatchPrecondition, ExecutionRef, PinId, StartBatchRequest},
        domain::AgentId,
        status::{StatusRequest, StatusSelector, SuccessCommit},
    };
    use agent_run_store::{
        batch_admission::{BatchAdmission, BatchMember},
        provider_admission::{AdmissionInputs, ProviderResume},
    };
    let home = home();
    let mut store = agent_run_store::fixture::open(home.path()).unwrap();
    let mut other = agent_run_store::fixture::open(home.path()).unwrap();
    let store_id = agent_run_store::store_identity::read(&store.conn).unwrap();
    let catalog = catalog(&store);
    let authority = authority(&catalog, home.path());
    let mut job = provider_request(home.path(), "temporary-job");
    job.request_id = None;
    let submit = |store: &mut Store<'_>, request: &StartBatchRequest| {
        let fingerprint = request.fingerprint().unwrap();
        let candidate = candidates(store.quota_capacity_revision().unwrap(), &[("acct-a", 0)]);
        let effective = request.jobs[0].storage_projection();
        let frozen = identity(&request.jobs[0], &authority);
        store.admit_batch(BatchAdmission {
            request_id: &request.request_id,
            fingerprint: &fingerprint,
            retain_history: request.retain_history,
            expected_store_id: request.expected_store_id.as_ref(),
            preconditions: request.preconditions.as_deref(),
            catalog: &catalog,
            jobs: vec![BatchMember {
                id: AgentId::new(),
                inputs: AdmissionInputs {
                    request: &request.jobs[0],
                    effective: &effective,
                    authority: &authority,
                    candidates: &candidate,
                    identity: &frozen,
                    global_cap: 8,
                    harness_cap: None,
                    pinned: None,
                },
            }],
        })
    };
    let request = |key: &str| StartBatchRequest {
        request_id: key.into(),
        jobs: vec![job.clone()],
        retain_history: true,
        expected_store_id: Some(store_id.clone()),
        preconditions: None,
    };
    let seed_a = submit(&mut store, &request("prior-a"))
        .unwrap()
        .receipt
        .jobs
        .remove(0);
    let seed_b = submit(&mut store, &request("prior-b"))
        .unwrap()
        .receipt
        .jobs
        .remove(0);
    for seed in [&seed_a, &seed_b] {
        store.conn.execute(
            "UPDATE agents SET status='failed',finished_at=10,runtime_session_id='fixture-session' WHERE id=?",
            [seed.agent_id.as_str()],
        ).unwrap();
        store.conn.execute(
            "UPDATE attempts SET state='failed',finished_at=10,phase='cleanup_complete',ownership_active=0,cleanup_proof_json=? WHERE agent_id=?",
            rusqlite::params![json!({"confirmed":true,"group_gone":true,"descendants_gone":true}).to_string(), seed.agent_id.as_str()],
        ).unwrap();
    }
    let condition = |job: &agent_run_domain::batch::BatchJobReceipt| BatchPrecondition {
        agent_id: job.agent_id.clone(),
        sequence: job.sequence,
        execution_ref: job.execution_ref.clone(),
        pin_id: job.pin_id.clone().unwrap(),
    };
    let a = condition(&seed_a);
    let b = condition(&seed_b);
    let status = |store: &Store<'_>, c: &BatchPrecondition| {
        store
            .status_snapshot(&StatusRequest {
                version: 1,
                expected_store_id: Some(store_id.clone()),
                include_view: false,
                targets: vec![StatusSelector {
                    agent_id: c.agent_id.clone(),
                    sequence: c.sequence,
                    execution_ref: Some(c.execution_ref.clone()),
                    pin_id: Some(c.pin_id.clone()),
                }],
            })
            .unwrap()
    };
    let snapshot = status(&store, &a);
    assert_eq!(snapshot.items[0].lineage_terminal, Some(true));
    assert_eq!(snapshot.items[0].success_commit, SuccessCommit::Unknown);
    let counts = |store: &Store<'_>| -> Vec<i64> {
        ["agents", "attempts", "start_batch_receipts", "history_pins"]
            .iter()
            .map(|table| {
                store
                    .conn
                    .query_row(&format!("SELECT COUNT(*) FROM {table}"), [], |row| {
                        row.get(0)
                    })
                    .unwrap()
            })
            .collect()
    };
    let before = counts(&store);
    let mut guarded = request("guarded");
    let mut missing = a.clone();
    missing.agent_id = AgentId::new();
    missing.pin_id = PinId::new();
    guarded.preconditions = Some(vec![a.clone(), missing]);
    assert!(matches!(
        submit(&mut store, &guarded),
        Err(Error::PreconditionFailed {
            reason: PreconditionFailureReason::Unknown
        })
    ));
    assert_eq!(counts(&store), before);
    let mut wrong_ref = a.clone();
    wrong_ref.execution_ref = format!("ex1_{}", "f".repeat(64))
        .parse::<ExecutionRef>()
        .unwrap();
    guarded.preconditions = Some(vec![wrong_ref]);
    assert!(matches!(
        submit(&mut store, &guarded),
        Err(Error::PreconditionFailed {
            reason: PreconditionFailureReason::NotMet
        })
    ));
    assert_eq!(counts(&store), before);
    store
        .conn
        .execute(
            "UPDATE attempts SET cleanup_proof_json='invalid' WHERE agent_id=?",
            [a.agent_id.as_str()],
        )
        .unwrap();
    guarded.preconditions = Some(vec![a.clone()]);
    assert!(matches!(
        submit(&mut store, &guarded),
        Err(Error::PreconditionFailed {
            reason: PreconditionFailureReason::Unknown
        })
    ));
    assert_eq!(counts(&store), before);
    store
        .conn
        .execute(
            "UPDATE attempts SET cleanup_proof_json=? WHERE agent_id=?",
            rusqlite::params![
                json!({"confirmed":true,"group_gone":true,"descendants_gone":true}).to_string(),
                a.agent_id.as_str()
            ],
        )
        .unwrap();

    // This independent connection resumes A after the earlier successful read,
    // before admission takes its own writer snapshot.
    let resume = |store: &mut Store<'_>, parent: &AgentId, key: &str| {
        let request = provider_request(home.path(), key);
        let effective = request.storage_projection();
        let frozen = identity(&request, &authority);
        let candidate = candidates(store.quota_capacity_revision().unwrap(), &[("acct-a", 0)]);
        let account = "acct-a".parse().unwrap();
        store
            .admit_provider_resume(
                &request,
                &effective,
                &catalog,
                &authority,
                &candidate,
                &frozen,
                8,
                None,
                None,
                ProviderResume {
                    parent,
                    prefer: &account,
                    preflight_digest: None,
                    restart_id: None,
                },
            )
            .unwrap()
    };
    let child_a = resume(&mut other, &a.agent_id, "resume-prior-a");
    let after_resume = counts(&store);
    assert!(matches!(
        submit(&mut store, &guarded),
        Err(Error::PreconditionFailed {
            reason: PreconditionFailureReason::NotMet
        })
    ));
    assert_eq!(counts(&store), after_resume);
    let alias = BatchPrecondition {
        agent_id: child_a.agent_id.clone(),
        sequence: 2,
        execution_ref: ExecutionRef::from_first_provider_attempt(
            &child_a.agent_id,
            &child_a.attempt_id,
        )
        .unwrap(),
        pin_id: a.pin_id.clone(),
    };
    guarded.preconditions = Some(vec![a.clone(), alias]);
    assert!(matches!(
        submit(&mut store, &guarded),
        Err(Error::Validation(_))
    ));
    assert_eq!(counts(&store), after_resume);

    // B is still complete and clean. Admission succeeds even without success
    // answer evidence, then a resume remains legal after this transaction.
    guarded.preconditions = Some(vec![b.clone()]);
    let accepted = submit(&mut store, &guarded).unwrap();
    assert!(!accepted.replayed);
    assert_eq!(accepted.receipt.preconditions, guarded.preconditions);
    let child_b = resume(&mut other, &b.agent_id, "resume-prior-b");
    assert!(child_b.created);
    let replay = submit(&mut store, &guarded).unwrap();
    assert!(replay.replayed);
    assert_eq!(replay.receipt, accepted.receipt);
    assert_eq!(
        store
            .replay_batch_scoped(
                &guarded.request_id,
                &guarded.fingerprint().unwrap(),
                Some(&store_id)
            )
            .unwrap()
            .unwrap()
            .receipt,
        accepted.receipt
    );
    store.release_history_pin(&b.pin_id).unwrap().unwrap();
    let mut released = guarded.clone();
    released.request_id = "released".into();
    assert!(matches!(
        submit(&mut store, &released),
        Err(Error::PreconditionFailed {
            reason: PreconditionFailureReason::NotMet
        })
    ));
}

/// A pending technical restart refuses atomic admission until it is resolved.
#[test]
fn batch_precondition_restart_pending_blocks_until_resolved() {
    use agent_run_domain::{
        PreconditionFailureReason,
        batch::{BatchPrecondition, StartBatchRequest},
        domain::AgentId,
        status::{RestartResume, StatusRequest, StatusSelector},
    };
    use agent_run_store::{
        batch_admission::{BatchAdmission, BatchMember},
        provider_admission::AdmissionInputs,
    };
    let home = home();
    let mut store = agent_run_store::fixture::open(home.path()).unwrap();
    let store_id = agent_run_store::store_identity::read(&store.conn).unwrap();
    let catalog = catalog(&store);
    let authority = authority(&catalog, home.path());
    let mut job = provider_request(home.path(), "temporary-job");
    job.request_id = None;
    let submit = |store: &mut Store<'_>, request: &StartBatchRequest| {
        let fingerprint = request.fingerprint().unwrap();
        let candidate = candidates(store.quota_capacity_revision().unwrap(), &[("acct-a", 0)]);
        let effective = request.jobs[0].storage_projection();
        let frozen = identity(&request.jobs[0], &authority);
        store.admit_batch(BatchAdmission {
            request_id: &request.request_id,
            fingerprint: &fingerprint,
            retain_history: request.retain_history,
            expected_store_id: request.expected_store_id.as_ref(),
            preconditions: request.preconditions.as_deref(),
            catalog: &catalog,
            jobs: vec![BatchMember {
                id: AgentId::new(),
                inputs: AdmissionInputs {
                    request: &request.jobs[0],
                    effective: &effective,
                    authority: &authority,
                    candidates: &candidate,
                    identity: &frozen,
                    global_cap: 8,
                    harness_cap: None,
                    pinned: None,
                },
            }],
        })
    };
    let request = |key: &str| StartBatchRequest {
        request_id: key.into(),
        jobs: vec![job.clone()],
        retain_history: true,
        expected_store_id: Some(store_id.clone()),
        preconditions: None,
    };
    let seed = submit(&mut store, &request("restart-source"))
        .unwrap()
        .receipt
        .jobs
        .remove(0);
    store
        .conn
        .execute(
            "UPDATE agents SET status='failed',finished_at=10 WHERE id=?",
            [seed.agent_id.as_str()],
        )
        .unwrap();
    store.conn.execute("UPDATE attempts SET state='failed',finished_at=10,phase='cleanup_complete',ownership_active=0,cleanup_proof_json=? WHERE agent_id=?",
        rusqlite::params![json!({"confirmed":true,"group_gone":true,"descendants_gone":true}).to_string(),seed.agent_id.as_str()]).unwrap();
    let condition = BatchPrecondition {
        agent_id: seed.agent_id.clone(),
        sequence: seed.sequence,
        execution_ref: seed.execution_ref.clone(),
        pin_id: seed.pin_id.unwrap(),
    };
    let restart = store
        .begin_service_restart("pending-precondition", 1.0)
        .unwrap();
    store.conn.execute("INSERT INTO service_restart_agents(restart_id,agent_id,cancelled_before_restart,resume_status) VALUES(?,?,0,'pending')",
        rusqlite::params![restart,condition.agent_id.as_str()]).unwrap();
    let status = |store: &Store<'_>| {
        store
            .status_snapshot(&StatusRequest {
                version: 1,
                expected_store_id: Some(store_id.clone()),
                include_view: false,
                targets: vec![StatusSelector {
                    agent_id: condition.agent_id.clone(),
                    sequence: condition.sequence,
                    execution_ref: Some(condition.execution_ref.clone()),
                    pin_id: Some(condition.pin_id.clone()),
                }],
            })
            .unwrap()
    };
    assert_eq!(
        status(&store).items[0].restart_resume,
        RestartResume::Pending
    );
    let counts = |store: &Store<'_>| -> Vec<i64> {
        ["agents", "attempts", "history_pins", "start_batch_receipts"]
            .iter()
            .map(|table| {
                store
                    .conn
                    .query_row(&format!("SELECT COUNT(*) FROM {table}"), [], |row| {
                        row.get(0)
                    })
                    .unwrap()
            })
            .collect()
    };
    let before = counts(&store);
    let mut guarded = request("after-restart");
    guarded.preconditions = Some(vec![condition.clone()]);
    assert!(matches!(
        submit(&mut store, &guarded),
        Err(Error::PreconditionFailed {
            reason: PreconditionFailureReason::NotMet
        })
    ));
    assert_eq!(counts(&store), before);
    store
        .conn
        .execute(
            "UPDATE service_restart_agents SET resume_status='finished' WHERE restart_id=?",
            [restart],
        )
        .unwrap();
    store
        .conn
        .execute(
            "UPDATE service_restarts SET phase='complete',finished_at=2 WHERE id=?",
            [restart],
        )
        .unwrap();
    assert_eq!(status(&store).items[0].restart_resume, RestartResume::None);
    let admitted = submit(&mut store, &guarded).unwrap();
    assert!(!admitted.replayed);
    assert_eq!(admitted.receipt.preconditions, guarded.preconditions);
    assert_eq!(
        counts(&store),
        before
            .into_iter()
            .map(|count| count + 1)
            .collect::<Vec<_>>()
    );
}

/// Only a captured restart member may use the ordinary resume admission while
/// new external starts remain blocked; replay keeps the same child.
#[test]
fn restart_resume_uses_the_existing_provider_admission() {
    use agent_run_store::provider_admission::ProviderResume;
    let home = home();
    let mut store = agent_run_store::fixture::open(home.path()).unwrap();
    let catalog = catalog(&store);
    let request = provider_request(home.path(), "restart-parent");
    let authority = authority(&catalog, home.path());
    let admitted = store
        .admit_provider(
            &request,
            &request.storage_projection(),
            &catalog,
            &authority,
            &candidates(store.quota_capacity_revision().unwrap(), &[("acct-a", 0)]),
            &identity(&request, &authority),
            8,
            None,
            None,
        )
        .unwrap();
    let restart = store
        .begin_service_restart("restart-admission", 1.0)
        .unwrap();
    store.conn.execute(
        "UPDATE attempts SET phase='cleanup_complete',cleanup_proof_json='{\"confirmed\":true}',ownership_active=0,finished_at=2 WHERE id=?",
        [&admitted.attempt_id],
    ).unwrap();
    store.conn.execute(
        "UPDATE agents SET status='cancelled',finished_at=2,runtime_session_id='sess-a' WHERE id=?",
        [admitted.agent_id.as_str()],
    ).unwrap();
    store
        .conn
        .execute(
            "UPDATE service_restarts SET phase='recovering' WHERE id=?",
            [restart],
        )
        .unwrap();
    let resume_request = provider_request(home.path(), "restart-child");
    let candidate = candidates(store.quota_capacity_revision().unwrap(), &[("acct-a", 0)]);
    let resume_identity = identity(&resume_request, &authority);
    let admission = |store: &mut agent_run_store::Store<'_>, restart_id| {
        store.admit_provider_resume(
            &resume_request,
            &resume_request.storage_projection(),
            &catalog,
            &authority,
            &candidate,
            &resume_identity,
            8,
            None,
            None,
            ProviderResume {
                parent: &admitted.agent_id,
                prefer: &"acct-a".parse().unwrap(),
                preflight_digest: None,
                restart_id,
            },
        )
    };
    assert!(matches!(
        admission(&mut store, None),
        Err(Error::Transition(_))
    ));
    assert!(admission(&mut store, Some(restart + 1)).is_err());
    store
        .conn
        .execute(
            "UPDATE service_restart_agents SET cancelled_before_restart=1 WHERE restart_id=?",
            [restart],
        )
        .unwrap();
    assert!(matches!(
        admission(&mut store, Some(restart)),
        Err(Error::Conflict)
    ));
    store
        .conn
        .execute(
            "UPDATE service_restart_agents SET cancelled_before_restart=0 WHERE restart_id=?",
            [restart],
        )
        .unwrap();
    let child = admission(&mut store, Some(restart)).unwrap();
    assert!(child.created);
    assert_eq!(store.conn.query_row(
        "SELECT resumed_execution_id FROM service_restart_agents WHERE restart_id=? AND agent_id=?",
        rusqlite::params![restart,admitted.agent_id.as_str()], |row|row.get::<_,String>(0),
    ).unwrap(), child.agent_id.as_str());
    assert!(!admission(&mut store, Some(restart)).unwrap().created);
}
