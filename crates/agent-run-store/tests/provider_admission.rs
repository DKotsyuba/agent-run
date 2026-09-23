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

/// Admits one auto run (`request_id`) that selects `acct-a`, then marks its
/// attempt cleaned up with a recorded history seal (as the supervisor does
/// at its cleanup boundary) while the logical agent stays running.
fn cleaned_run(
    home: &Path,
    request_id: &str,
    seal: bool,
) -> (Store, agent_run_domain::domain::AgentId) {
    let mut store = Store::open(home).unwrap();
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
        json!({"native_history":{"seal":{"session":"s"}},"native_failure":{"class":"quota_exhausted"}}).to_string()
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
    assert!(store
        .allocate_next_attempt(&id, &catalog, &candidates(revision, &[("acct-b", 0)]))
        .is_err());
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
    assert!(cancelled
        .allocate_next_attempt(
            &cancel_id,
            &catalog,
            &candidates(revision, &[("acct-b", 0)])
        )
        .is_err());
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
    assert!(store
        .allocate_next_attempt(&id, &catalog, &candidates(revision, &[("acct-b", 0)]))
        .is_err());
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
                let mut store = Store::open(&path).unwrap();
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
    let store = Store::open(&path).unwrap();
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
    let mut late = Store::open(home.path()).unwrap();
    late.bind_attempt(&first);
    late.event(&id, "process_cleanup", &json!({"late":true}))
        .unwrap();
    late.message(&id, "assistant", "final words", None, None)
        .unwrap();
    store.event(&id, "unbound", &json!({})).unwrap();
    let (_, other) = cleaned_run(home.path(), "late-2", true);
    let mut foreign = Store::open(home.path()).unwrap();
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
