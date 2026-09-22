//! Trusted candidate admission, replay, scope, reservation, and contention.

use agent_run_domain::{
    catalog::{
        AccountId, AccountRecord, AccountStatus, AuthFamily, PhysicalQuotaKey, ProviderCatalog,
        QuotaAdmissionError, QuotaCandidate, QuotaCandidateSet, ResolvedLaunchAuthority,
        SelectionIntent,
    },
    domain::Outcome,
    Error, PositiveFinite, ProviderStartRequest,
};
use agent_run_store::Store;
use serde_json::{json, Value};
use std::{
    path::Path,
    sync::{Arc, Barrier},
};

/// Opens one initialized disposable store with four fake account references.
fn home() -> tempfile::TempDir {
    let home = tempfile::tempdir().unwrap();
    Store::initialize(home.path()).unwrap();
    let mut store = Store::open(home.path()).unwrap();
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

/// Stale input changes no row; eligible rank ties use account id, physical
/// reservations are exact, and replay wins after the revision changes.
#[test]
fn stale_replay_and_ranked_reservation_are_atomic() {
    let home = home();
    let mut store = Store::open(home.path()).unwrap();
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
    let admitted = store
        .admit_provider(
            &request,
            &effective,
            &catalog,
            &authority,
            &candidates(0, &ordered),
            &frozen_identity,
            2,
            None,
            None,
        )
        .unwrap();
    assert!(admitted.created);
    assert_eq!(admitted.account_id.as_str(), "acct-a");
    assert_eq!(store.quota_capacity_revision().unwrap(), 1);
    let facts: (String, String, i64) = store.conn.query_row(
        "SELECT a.selection_intent,k.quota_key,t.ownership_active FROM agents a \
         JOIN attempts t ON t.agent_id=a.id JOIN attempt_quota_keys k ON k.attempt_id=t.id WHERE a.id=?",
        [admitted.agent_id.as_str()], |r| Ok((r.get(0)?,r.get(1)?,r.get(2)?)),
    ).unwrap();
    assert_eq!(facts, ("auto".into(), "acct-a::tokens".into(), 1));
    assert!(store
        .finish(&admitted.agent_id, &Outcome::failure("test"), None, None)
        .is_err());
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
    let mut alias_candidates = candidates(1, &[("acct-a", 0)]);
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
    assert_eq!(store.quota_capacity_revision().unwrap(), 2);
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
            &candidates(0, &ordered),
            &frozen_identity,
            1,
            None,
            None,
        )
        .unwrap();
    assert!(!replay.created);
    assert_eq!(replay.attempt_id, admitted.attempt_id);
    let next = provider_request(home.path(), "req-two");
    let cap = store
        .admit_provider(
            &next,
            &next.storage_projection(),
            &catalog,
            &authority,
            &candidates(2, &[("acct-b", 0)]),
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
    let mut store = Store::open(home.path()).unwrap();
    let catalog = catalog(&store);
    let request = provider_request(home.path(), "filtered");
    let authority = authority(&catalog, home.path());
    let admitted = store
        .admit_provider(
            &request,
            &request.storage_projection(),
            &catalog,
            &authority,
            &candidates(
                0,
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
    let mut store = Store::open(home.path()).unwrap();
    let catalog = catalog(&store);
    let first = provider_request(home.path(), "first");
    let first_authority = authority(&catalog, home.path());
    store
        .admit_provider(
            &first,
            &first.storage_projection(),
            &catalog,
            &first_authority,
            &candidates(0, &[("acct-a", 0)]),
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
    let mut alias_candidates = candidates(1, &[("acct-a", 0)]);
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
    assert_eq!(store.quota_capacity_revision().unwrap(), 1);
}

/// Two independent store connections cannot both commit against one global
/// revision; one wins and the other observes stale capacity.
#[test]
fn concurrent_provider_admissions_share_one_committed_revision() {
    let home = home();
    let catalog = catalog(&Store::open(home.path()).unwrap());
    let authority = authority(&catalog, home.path());
    let barrier = Arc::new(Barrier::new(2));
    let mut workers = Vec::new();
    for suffix in ["first", "second"] {
        let path = home.path().to_path_buf();
        let catalog = catalog.clone();
        let authority = authority.clone();
        let barrier = Arc::clone(&barrier);
        workers.push(std::thread::spawn(move || {
            let mut store = Store::open(&path).unwrap();
            let request = provider_request(&path, suffix);
            barrier.wait();
            store.admit_provider(
                &request,
                &request.storage_projection(),
                &catalog,
                &authority,
                &candidates(0, &[("acct-a", 0)]),
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
    let store = Store::open(home.path()).unwrap();
    assert_eq!(
        store
            .conn
            .query_row("SELECT COUNT(*) FROM agents", [], |r| r.get::<_, i64>(0))
            .unwrap(),
        1
    );
}
