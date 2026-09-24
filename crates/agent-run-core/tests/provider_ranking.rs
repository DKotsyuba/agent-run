//! Read-only provider ranking: real candidate sets, per-model provider
//! order, exhaustion semantics, pinning, and snapshot consistency.

use agent_run_core::{
    capacity::provider_ranking::{
        provider_candidates_at, provider_order_at, ProviderCapacityOrder,
    },
    state::Store,
};
use agent_run_domain::{
    catalog::{
        AccountId, AccountRecord, AccountStatus, AuthFamily, PhysicalQuotaKey, ProviderCatalog,
        QuotaAdmissionError, SelectionIntent,
    },
    Error, ProviderStartRequest,
};
use serde_json::{json, Value};
use std::collections::BTreeSet;

/// Fixed ranking clock every fixture sample is fresh at.
const AT: f64 = 1000.0;

/// Opens one initialized disposable store with fake account references.
fn home(accounts: &[(&str, &str)]) -> tempfile::TempDir {
    let home = tempfile::tempdir().unwrap();
    Store::initialize(home.path()).unwrap();
    let mut store = Store::open(home.path()).unwrap();
    for (id, name) in accounts {
        store
            .register_account(&AccountRecord {
                account_id: id.parse().unwrap(),
                auth_family: "anthropic".parse::<AuthFamily>().unwrap(),
                secret_ref: format!("env:{name}").parse().unwrap(),
                status: AccountStatus::Enabled,
            })
            .unwrap();
    }
    home
}

/// One Claude Messages provider catalog over the registered accounts.
fn catalog(store: &Store) -> ProviderCatalog {
    let providers = serde_json::from_value(json!([
        {
            "id":"glm-user","harness":"claude-code",
            "connection":{"kind":"custom","endpoint":"https://gateway.example/api","protocol":"messages"},
            "auth_family":"anthropic","limits_source":"none","priority_multiplier":1.5,
            "models":[{"id":"fixture"},{"id":"other","native_model":"other-native"}],
            "bindings":[
                {"label":"alpha","account":"acct-a","priority_multiplier":2.0},
                {"label":"alpha-two","account":"acct-a","priority_multiplier":3.0},
                {"label":"beta","account":"acct-b"},
                {"label":"heavy","account":"acct-c","priority_multiplier":100.0},
                {"label":"other-only","account":"acct-b","models":["other"]}
            ]
        },
        {
            "id":"glm-alias","harness":"claude-code",
            "connection":{"kind":"custom","endpoint":"https://gateway.example/api","protocol":"messages"},
            "auth_family":"anthropic","limits_source":"none",
            "models":[{"id":"fixture"}],
            "bindings":[{"label":"shared","account":"acct-a"}]
        },
        {
            "id":"glm-empty","harness":"claude-code",
            "connection":{"kind":"custom","endpoint":"https://gateway.example/api","protocol":"messages"},
            "auth_family":"anthropic","limits_source":"none",
            "models":[{"id":"fixture"}],
            "bindings":[]
        }
    ]))
    .unwrap();
    ProviderCatalog::new(store.list_accounts().unwrap(), providers).unwrap()
}

/// Inserts one account-bound quota sample row with an explicit identity.
fn sample(store: &Store, account: &str, lane: &str, remaining: f64) {
    sample_full(
        store,
        account,
        lane,
        remaining,
        AT - 100.0,
        AT + 100.0,
        AT + 200.0,
    );
}

/// Inserts one account-bound sample with explicit freshness bounds.
fn sample_full(
    store: &Store,
    account: &str,
    lane: &str,
    remaining: f64,
    observed: f64,
    valid: f64,
    reset: f64,
) {
    store
        .conn
        .execute(
            "INSERT INTO capacity_samples(runtime,lane,window,target,source,remaining_percent,reset_at,observed_at,valid_until,payload_json,account_id,quota_key) \
             VALUES('claude-code',?1,'5h',NULL,'collector',?2,?3,?4,?5,'null',?6,?7)",
            rusqlite::params![
                lane,
                remaining,
                reset,
                observed,
                valid,
                account,
                format!("{account}::{lane}")
            ],
        )
        .unwrap();
}

/// Inserts one legacy NULL-identity sample that must never rank.
fn legacy_sample(store: &Store, remaining: f64) {
    store
        .conn
        .execute(
            "INSERT INTO capacity_samples(runtime,lane,window,target,source,remaining_percent,reset_at,observed_at,valid_until,payload_json) \
             VALUES('claude-code','fixture','5h',NULL,'collector',?1,?2,?3,?4,'null')",
            rusqlite::params![remaining, AT + 200.0, AT - 100.0, AT + 100.0],
        )
        .unwrap();
}

/// Inserts one durable exhaustion latch on an account's lane.
fn latch(store: &Store, account: &str, lane: &str, reset: Option<f64>) {
    store
        .conn
        .execute(
            "INSERT INTO quota_exhaustion(account_id,quota_key,source,window_id,observed_at,reset_at) \
             VALUES(?1,?2,'collector','5h',?3,?4)",
            rusqlite::params![account, format!("{account}::{lane}"), AT - 50.0, reset],
        )
        .unwrap();
}

/// The empty hard-ineligible set used unless a test types its own evidence.
fn none_ineligible() -> BTreeSet<AccountId> {
    BTreeSet::new()
}

/// Produces candidates for the main provider at the fixed clock.
fn candidates(
    store: &Store,
    catalog: &ProviderCatalog,
    model: &str,
) -> agent_run_domain::catalog::QuotaCandidateSet {
    provider_candidates_at(
        store,
        catalog,
        &"glm-user".parse().unwrap(),
        model,
        None,
        &none_ineligible(),
        AT,
    )
    .unwrap()
}

/// Produces candidates for the main provider and expects the typed error.
fn candidates_err(
    store: &Store,
    catalog: &ProviderCatalog,
    model: &str,
) -> agent_run_domain::Error {
    provider_candidates_at(
        store,
        catalog,
        &"glm-user".parse().unwrap(),
        model,
        None,
        &none_ineligible(),
        AT,
    )
    .unwrap_err()
}

/// Summarizes a candidate set as `(account, rank, multiplier, known)` rows.
fn summary(set: &agent_run_domain::catalog::QuotaCandidateSet) -> Vec<(String, u32, f64, bool)> {
    set.candidates
        .iter()
        .map(|candidate| {
            (
                candidate.account.as_str().to_owned(),
                candidate.rank,
                candidate.multiplier.get(),
                candidate.quota_known,
            )
        })
        .collect()
}

/// Governing-window scores multiply by binding weights, the provider
/// multiplier scales the order view, and unknown capacity never outranks
/// known capacity regardless of weight.
#[test]
fn known_scores_weight_and_order_before_unknown() {
    let home = home(&[
        ("acct-a", "FAKE_A"),
        ("acct-b", "FAKE_B"),
        ("acct-c", "FAKE_C"),
    ]);
    let store = Store::open(home.path()).unwrap();
    let catalog = catalog(&store);
    sample(&store, "acct-a", "fixture", 20.0);
    sample(&store, "acct-b", "fixture", 30.0);
    let set = candidates(&store, &catalog, "fixture");
    set.validate().unwrap();
    // score .4 * max label weight 3 (never 2+3+1) beats .6 * 1; acct-c is
    // unknown with weight 100 and still ranks last.
    assert_eq!(
        summary(&set),
        vec![
            ("acct-a".into(), 0, 3.0, true),
            ("acct-b".into(), 1, 1.0, true),
            ("acct-c".into(), 2, 100.0, false),
        ]
    );
    assert_eq!(
        set.capacity_revision,
        store.quota_capacity_revision().unwrap()
    );
    assert_eq!(set.intent, SelectionIntent::Auto);
    let key = PhysicalQuotaKey::new(&"acct-a".parse().unwrap(), "fixture").unwrap();
    assert_eq!(set.candidates[0].physical_keys, vec![key]);
    assert!(set.candidates[2].physical_keys.is_empty());

    let order = provider_order_at(&store, &catalog, &none_ineligible(), AT).unwrap();
    let entry = order
        .providers
        .iter()
        .find(|entry| entry.provider.as_str() == "glm-user")
        .unwrap();
    // Provider score = best account priority (.4*3) * provider weight 1.5.
    assert!((entry.score.unwrap() - 1.8).abs() < 1e-9);
    assert_eq!(entry.models[0].status, "available");
    assert!((entry.models[0].best_priority.unwrap() - 1.2).abs() < 1e-9);
    // The other model has no observations of its own native lane.
    assert_eq!(entry.models[1].status, "unknown");
    assert_eq!(entry.models[1].best_priority, None);
}

/// One global account under several labels and providers stays one physical
/// candidate with one reservation identity, without summing factors.
#[test]
fn labels_and_aliases_deduplicate_to_one_physical_candidate() {
    let home = home(&[
        ("acct-a", "FAKE_A"),
        ("acct-b", "FAKE_B"),
        ("acct-c", "FAKE_C"),
    ]);
    let store = Store::open(home.path()).unwrap();
    let catalog = catalog(&store);
    sample(&store, "acct-a", "fixture", 20.0);
    let set = candidates(&store, &catalog, "fixture");
    // One physical candidate for the doubly-bound account, maximum factor
    // 3 (never 2+3), ahead of the unknown fallbacks.
    assert_eq!(
        summary(&set),
        vec![
            ("acct-a".into(), 0, 3.0, true),
            ("acct-b".into(), 1, 1.0, false),
            ("acct-c".into(), 1, 100.0, false),
        ]
    );
    // The alias provider resolves the same physical key, not a second pool.
    let alias = provider_candidates_at(
        &store,
        &catalog,
        &"glm-alias".parse().unwrap(),
        "fixture",
        None,
        &none_ineligible(),
        AT,
    )
    .unwrap();
    assert_eq!(
        alias.candidates[0].physical_keys,
        set.candidates[0].physical_keys
    );
    // A pin resolves to its account only; the ineligible label's subset
    // never falls over to the known account under another label.
    let pinned = provider_candidates_at(
        &store,
        &catalog,
        &"glm-user".parse().unwrap(),
        "fixture",
        Some("other-only"),
        &none_ineligible(),
        AT,
    )
    .unwrap();
    assert_eq!(
        pinned.intent,
        SelectionIntent::Pinned("acct-b".parse().unwrap())
    );
    assert_eq!(summary(&pinned), vec![("acct-b".into(), 0, 1.0, false)]);
}

/// A renamed collector alias must keep one physical window history, so an
/// expired old row cannot poison the newer fresh observation. The same-alias
/// control verifies that only the stored runtime label changed.
#[test]
fn renamed_alias_keeps_fresh_physical_window_known() {
    for renamed in [false, true] {
        let home = home(&[
            ("acct-a", "FAKE_A"),
            ("acct-b", "FAKE_B"),
            ("acct-c", "FAKE_C"),
        ]);
        let store = Store::open(home.path()).unwrap();
        let catalog = catalog(&store);
        for (runtime, observed, valid, remaining) in [
            (if renamed { "a-old" } else { "b-new" }, 800.0, 900.0, 70.0),
            ("b-new", 950.0, 1100.0, 80.0),
        ] {
            store.conn.execute(
                "INSERT INTO capacity_samples(runtime,lane,window,target,source,remaining_percent,reset_at,observed_at,valid_until,payload_json,account_id,quota_key) \
                 VALUES(?1,'fixture','5h',NULL,'collector',?2,1200,?3,?4,'{\"models\":[\"fixture\"]}','acct-a','acct-a::fixture')",
                rusqlite::params![runtime, remaining, observed, valid],
            ).unwrap();
        }
        let set = provider_candidates_at(
            &store,
            &catalog,
            &"glm-user".parse().unwrap(),
            "fixture",
            Some("alpha"),
            &none_ineligible(),
            AT,
        )
        .unwrap();
        assert_eq!(
            summary(&set),
            vec![("acct-a".into(), 0, 3.0, true)],
            "renamed={renamed}"
        );
        if renamed {
            store.conn.execute(
                "INSERT INTO capacity_samples(runtime,lane,window,target,source,remaining_percent,reset_at,observed_at,valid_until,payload_json,account_id,quota_key) \
                 VALUES('b-new','secondary','5h',NULL,'collector',70,1200,800,900,'{\"models\":[\"fixture\"]}','acct-a','acct-a::secondary')",
                [],
            ).unwrap();
            let distinct = provider_candidates_at(
                &store,
                &catalog,
                &"glm-user".parse().unwrap(),
                "fixture",
                Some("alpha"),
                &none_ineligible(),
                AT,
            )
            .unwrap();
            assert_eq!(summary(&distinct), vec![("acct-a".into(), 0, 3.0, false)]);
        }
    }
}

/// A model-specific exhausted lane excludes only that model; unrelated
/// unknown windows never mask exhaustion, and durable latches survive stale
/// samples and failed sources.
#[test]
fn exhaustion_is_model_specific_and_latches_survive_stale_evidence() {
    let home = home(&[
        ("acct-a", "FAKE_A"),
        ("acct-b", "FAKE_B"),
        ("acct-c", "FAKE_C"),
    ]);
    let store = Store::open(home.path()).unwrap();
    let catalog = catalog(&store);
    // acct-a: fresh authoritative zero on the fixture lane only.
    sample(&store, "acct-a", "fixture", 0.0);
    sample(&store, "acct-a", "other-native", 40.0);
    // acct-b: only a stale sample plus an active durable latch on fixture.
    sample_full(
        &store,
        "acct-b",
        "fixture",
        80.0,
        AT - 300.0,
        AT - 100.0,
        AT + 200.0,
    );
    latch(&store, "acct-b", "fixture", Some(AT + 500.0));

    let fixture = candidates(&store, &catalog, "fixture");
    assert_eq!(summary(&fixture), vec![("acct-c".into(), 0, 100.0, false)]);
    // The other model keeps acct-a: its own lane is fresh and positive, and
    // the exhausted fixture lane plus the latched account do not leak in.
    let other = candidates(&store, &catalog, "other");
    assert_eq!(
        summary(&other),
        vec![
            ("acct-a".into(), 0, 3.0, true),
            ("acct-b".into(), 1, 1.0, false),
            ("acct-c".into(), 1, 100.0, false),
        ]
    );

    // Removing every candidate by exhaustion reports the typed verdict;
    // the earliest finite latch reset is the account the fact attaches to.
    store
        .conn
        .execute("DELETE FROM capacity_samples", [])
        .unwrap();
    latch(&store, "acct-a", "fixture", Some(AT + 400.0));
    latch(&store, "acct-c", "fixture", None);
    let exhausted = candidates_err(&store, &catalog, "fixture");
    assert!(
        matches!(
            &exhausted,
            Error::QuotaAdmission(QuotaAdmissionError::QuotaExhausted { account, .. })
                if account.as_str() == "acct-a"
        ),
        "{exhausted:?}"
    );

    // A passed reset releases the latch without inventing fresh capacity.
    store
        .conn
        .execute(
            "UPDATE quota_exhaustion SET reset_at=? WHERE account_id='acct-b'",
            [AT - 10.0],
        )
        .unwrap();
    store
        .conn
        .execute("DELETE FROM quota_exhaustion WHERE account_id='acct-c'", [])
        .unwrap();
    let released = candidates(&store, &catalog, "fixture");
    assert_eq!(
        summary(&released),
        vec![
            ("acct-b".into(), 0, 1.0, false),
            ("acct-c".into(), 0, 100.0, false),
        ]
    );
}

/// Exactly equal priorities share one rank group; output is independent of
/// row order, and account activity never reorders different quota scores.
#[test]
fn equal_priorities_share_rank_and_output_is_row_order_independent() {
    let first_home = home(&[("acct-a", "FAKE_A"), ("acct-b", "FAKE_B")]);
    let store = Store::open(first_home.path()).unwrap();
    let providers = serde_json::from_value(json!([
        {
            "id":"glm-user","harness":"claude-code",
            "connection":{"kind":"custom","endpoint":"https://gateway.example/api","protocol":"messages"},
            "auth_family":"anthropic","limits_source":"none",
            "models":[{"id":"fixture"}],
            "bindings":[
                {"label":"alpha","account":"acct-a"},
                {"label":"beta","account":"acct-b"}
            ]
        }
    ]))
    .unwrap();
    let catalog = ProviderCatalog::new(store.list_accounts().unwrap(), providers).unwrap();
    sample(&store, "acct-a", "fixture", 20.0);
    sample(&store, "acct-b", "fixture", 20.0);
    let set = candidates(&store, &catalog, "fixture");
    assert_eq!(
        summary(&set),
        vec![
            ("acct-a".into(), 0, 1.0, true),
            ("acct-b".into(), 0, 1.0, true),
        ]
    );

    // Reversed physical row order produces the identical set.
    let other_home = home(&[("acct-a", "FAKE_A"), ("acct-b", "FAKE_B")]);
    let second = Store::open(other_home.path()).unwrap();
    let second_catalog = ProviderCatalog::new(
        second.list_accounts().unwrap(),
        serde_json::from_value(json!([
            {
                "id":"glm-user","harness":"claude-code",
                "connection":{"kind":"custom","endpoint":"https://gateway.example/api","protocol":"messages"},
                "auth_family":"anthropic","limits_source":"none",
                "models":[{"id":"fixture"}],
                "bindings":[
                    {"label":"alpha","account":"acct-a"},
                    {"label":"beta","account":"acct-b"}
                ]
            }
        ]))
        .unwrap(),
    )
    .unwrap();
    sample(&second, "acct-b", "fixture", 20.0);
    sample(&second, "acct-a", "fixture", 20.0);
    let reversed = candidates(&second, &second_catalog, "fixture");
    assert_eq!(set, reversed);

    // Activity on the better-scoring account never reorders producer output:
    // different quota scores keep their order across reproductions.
    let tilted_home = home(&[("acct-a", "FAKE_A"), ("acct-b", "FAKE_B")]);
    let mut busy = Store::open(tilted_home.path()).unwrap();
    let busy_catalog = ProviderCatalog::new(busy.list_accounts().unwrap(), serde_json::from_value(json!([
        {
            "id":"glm-user","harness":"claude-code",
            "connection":{"kind":"custom","endpoint":"https://gateway.example/api","protocol":"messages"},
            "auth_family":"anthropic","limits_source":"none",
            "models":[{"id":"fixture"}],
            "bindings":[
                {"label":"alpha","account":"acct-a"},
                {"label":"beta","account":"acct-b"}
            ]
        }
    ]))
    .unwrap())
    .unwrap();
    sample(&busy, "acct-a", "fixture", 20.0);
    sample(&busy, "acct-b", "fixture", 30.0);
    let before = candidates(&busy, &busy_catalog, "fixture");
    assert_eq!(
        summary(&before),
        vec![
            ("acct-b".into(), 0, 1.0, true),
            ("acct-a".into(), 1, 1.0, true),
        ]
    );
    let request = provider_request(tilted_home.path(), "activity");
    let authority = authority(&busy_catalog, tilted_home.path());
    busy.admit_provider(
        &request,
        &request.storage_projection(),
        &busy_catalog,
        &authority,
        &before,
        &identity(&request, &authority),
        4,
        None,
        None,
    )
    .unwrap();
    let after = candidates(&busy, &busy_catalog, "fixture");
    assert_eq!(summary(&after), summary(&before));
    assert_eq!(after.capacity_revision, before.capacity_revision + 1);
}

/// Pinning resolves one provider-local label to one global account without
/// failover; disabled or model-ineligible pins refuse typed; a never
/// collected account is admitted on account-level ownership alone.
#[test]
fn pinning_resolves_one_account_and_unknown_accounts_admit() {
    let home = home(&[
        ("acct-a", "FAKE_A"),
        ("acct-b", "FAKE_B"),
        ("acct-fresh", "FAKE_F"),
    ]);
    let mut store = Store::open(home.path()).unwrap();
    sample(&store, "acct-a", "fixture", 20.0);
    let catalog = {
        let providers = serde_json::from_value(json!([
        {
            "id":"glm-user","harness":"claude-code",
            "connection":{"kind":"custom","endpoint":"https://gateway.example/api","protocol":"messages"},
            "auth_family":"anthropic","limits_source":"none",
            "models":[{"id":"fixture"},{"id":"other"}],
            "bindings":[
                {"label":"alpha","account":"acct-a"},
                {"label":"fresh","account":"acct-fresh"},
                {"label":"other-only","account":"acct-b","models":["other"]}
            ]
        }]))
        .unwrap();
        ProviderCatalog::new(store.list_accounts().unwrap(), providers).unwrap()
    };
    let pin = |store: &Store, label: &str| {
        provider_candidates_at(
            store,
            &catalog,
            &"glm-user".parse().unwrap(),
            "fixture",
            Some(label),
            &none_ineligible(),
            AT,
        )
    };
    let pinned = pin(&store, "alpha").unwrap();
    assert_eq!(
        pinned.intent,
        SelectionIntent::Pinned("acct-a".parse().unwrap())
    );
    assert_eq!(summary(&pinned), vec![("acct-a".into(), 0, 1.0, true)]);

    let ineligible = pin(&store, "other-only").unwrap_err();
    assert!(
        matches!(
            ineligible,
            Error::QuotaAdmission(QuotaAdmissionError::NoEligibleAccount { .. })
        ),
        "{ineligible:?}"
    );
    let unbound = pin(&store, "missing").unwrap_err();
    assert!(unbound.to_string().contains("label"), "{unbound:?}");

    store.disable_account(&"acct-a".parse().unwrap()).unwrap();
    let disabled = pin(&store, "alpha").unwrap_err();
    assert!(
        matches!(
            disabled,
            Error::QuotaAdmission(QuotaAdmissionError::NoEligibleAccount { .. })
        ),
        "{disabled:?}"
    );
    store
        .conn
        .execute(
            "UPDATE provider_accounts SET status='enabled' WHERE account_id='acct-a'",
            [],
        )
        .unwrap();

    // A pinned unknown account stays itself: the real store admits it on
    // account-level ownership alone, with no fabricated pool keys.
    let fresh = pin(&store, "fresh").unwrap();
    assert_eq!(summary(&fresh), vec![("acct-fresh".into(), 0, 1.0, false)]);
    assert!(fresh.candidates[0].physical_keys.is_empty());
    let request = provider_request(home.path(), "fresh-admit");
    let mut fresh_authority = authority(&catalog, home.path());
    fresh_authority.eligible_accounts = vec!["acct-fresh".parse().unwrap()];
    let admitted = store
        .admit_provider(
            &request,
            &request.storage_projection(),
            &catalog,
            &fresh_authority,
            &fresh,
            &identity(&request, &fresh_authority),
            4,
            None,
            Some(&"acct-fresh".parse().unwrap()),
        )
        .unwrap();
    assert_eq!(admitted.account_id.as_str(), "acct-fresh");
    let reserved: i64 = store
        .conn
        .query_row(
            "SELECT COUNT(*) FROM attempt_quota_keys k JOIN attempts a ON a.id=k.attempt_id \
             WHERE a.agent_id=?",
            [admitted.agent_id.as_str()],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(reserved, 0);

    // The same pinned account refuses typed once its lane is latched.
    latch(&store, "acct-fresh", "fixture", Some(AT + 10.0));
    let refused = pin(&store, "fresh").unwrap_err();
    assert!(
        matches!(
            &refused,
            Error::QuotaAdmission(QuotaAdmissionError::QuotaExhausted { account, .. })
                if account.as_str() == "acct-fresh"
        ),
        "{refused:?}"
    );
}

/// Producing candidates and orders changes no row and opens no transport;
/// the returned revision matches the snapshot and stays admission-fresh.
#[test]
fn producing_candidates_is_read_only_and_revision_consistent() {
    let home = home(&[
        ("acct-a", "FAKE_A"),
        ("acct-b", "FAKE_B"),
        ("acct-c", "FAKE_C"),
    ]);
    let mut store = Store::open(home.path()).unwrap();
    let catalog = catalog(&store);
    sample(&store, "acct-a", "fixture", 20.0);
    sample(&store, "acct-b", "fixture", 30.0);
    legacy_sample(&store, 99.0);
    let before: (i64, i64, i64, i64) = store
        .conn
        .query_row(
            "SELECT (SELECT COUNT(*) FROM capacity_samples),\
             (SELECT COUNT(*) FROM quota_exhaustion),\
             (SELECT COUNT(*) FROM attempts),\
             (SELECT revision FROM quota_capacity_revision WHERE id=1)",
            [],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
        )
        .unwrap();
    let set = candidates(&store, &catalog, "fixture");
    let order: ProviderCapacityOrder =
        provider_order_at(&store, &catalog, &none_ineligible(), AT).unwrap();
    let after: (i64, i64, i64, i64) = store
        .conn
        .query_row(
            "SELECT (SELECT COUNT(*) FROM capacity_samples),\
             (SELECT COUNT(*) FROM quota_exhaustion),\
             (SELECT COUNT(*) FROM attempts),\
             (SELECT revision FROM quota_capacity_revision WHERE id=1)",
            [],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
        )
        .unwrap();
    assert_eq!(before, after);
    assert_eq!(set.capacity_revision, after.3);
    assert_eq!(order.capacity_revision, after.3);
    // The legacy NULL-identity row never became account quota: the known
    // accounts keep the exact scores it would have inflated.
    assert_eq!(
        summary(&set),
        vec![
            ("acct-a".into(), 0, 3.0, true),
            ("acct-b".into(), 1, 1.0, true),
            ("acct-c".into(), 2, 100.0, false),
        ]
    );

    // Admission against the produced revision commits and moves it.
    let request = provider_request(home.path(), "ranked");
    let authority = authority(&catalog, home.path());
    let admitted = store
        .admit_provider(
            &request,
            &request.storage_projection(),
            &catalog,
            &authority,
            &set,
            &identity(&request, &authority),
            4,
            None,
            None,
        )
        .unwrap();
    assert_eq!(admitted.account_id.as_str(), "acct-a");
    assert_eq!(
        store.quota_capacity_revision().unwrap(),
        set.capacity_revision + 1
    );
    let second = provider_request(home.path(), "ranked-two");
    let stale = store
        .admit_provider(
            &second,
            &second.storage_projection(),
            &catalog,
            &authority,
            &set,
            &identity(&second, &authority),
            4,
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
    let refreshed = candidates(&store, &catalog, "fixture");
    assert_eq!(refreshed.capacity_revision, set.capacity_revision + 1);
}

/// Invalid input, overflow, malformed, future, and conflicting alias
/// mappings are all accounted for explicitly.
#[test]
fn invalid_input_overflow_and_alias_conflicts_are_explicit() {
    let home = home(&[("acct-a", "FAKE_A")]);
    let store = Store::open(home.path()).unwrap();
    let providers = serde_json::from_value(json!([
        {
            "id":"plus","harness":"claude-code",
            "connection":{"kind":"custom","endpoint":"https://gateway.example/api","protocol":"messages"},
            "auth_family":"anthropic","limits_source":"none",
            "models":[{"id":"fast","native_model":"lane-a"}],
            "bindings":[{"label":"main","account":"acct-a","priority_multiplier":1e308}]
        },
        {
            "id":"pro","harness":"claude-code",
            "connection":{"kind":"custom","endpoint":"https://gateway.example/api","protocol":"messages"},
            "auth_family":"anthropic","limits_source":"none",
            "models":[{"id":"fast","native_model":"lane-b"}],
            "bindings":[{"label":"main","account":"acct-a"}]
        }
    ]))
    .unwrap();
    let catalog = ProviderCatalog::new(store.list_accounts().unwrap(), providers).unwrap();
    sample(&store, "acct-a", "lane-a", 100.0);

    let blank = provider_candidates_at(
        &store,
        &catalog,
        &"plus".parse().unwrap(),
        " ",
        None,
        &none_ineligible(),
        AT,
    )
    .unwrap_err();
    assert!(blank.to_string().contains("nonblank"), "{blank:?}");
    let unknown_provider = provider_candidates_at(
        &store,
        &catalog,
        &"missing".parse().unwrap(),
        "fast",
        None,
        &none_ineligible(),
        AT,
    )
    .unwrap_err();
    assert!(unknown_provider.to_string().contains("not configured"));
    let unoffered = provider_candidates_at(
        &store,
        &catalog,
        &"plus".parse().unwrap(),
        "slow",
        None,
        &none_ineligible(),
        AT,
    )
    .unwrap_err();
    assert!(
        unoffered.to_string().contains("not offered"),
        "{unoffered:?}"
    );

    // The huge weight overflows the priority; the account is excluded and
    // the producer reports overflow instead of sorting an infinity.
    let overflow = provider_candidates_at(
        &store,
        &catalog,
        &"plus".parse().unwrap(),
        "fast",
        None,
        &none_ineligible(),
        AT,
    )
    .unwrap_err();
    assert!(overflow.to_string().contains("overflow"), "{overflow:?}");
    let order = provider_order_at(&store, &catalog, &none_ineligible(), AT).unwrap();
    let plus = order
        .providers
        .iter()
        .find(|p| p.provider.as_str() == "plus")
        .unwrap();
    assert_eq!(plus.models[0].status, "priority_overflow");
    assert_eq!(plus.score, None);

    // The same model id under the other provider maps to its own native
    // lane; membership is not merged across the aliases.
    let pro = provider_candidates_at(
        &store,
        &catalog,
        &"pro".parse().unwrap(),
        "fast",
        None,
        &none_ineligible(),
        AT,
    )
    .unwrap();
    assert_eq!(summary(&pro), vec![("acct-a".into(), 0, 1.0, false)]);

    // Malformed and future observations are unknown, never known zero.
    store
        .conn
        .execute("DELETE FROM capacity_samples", [])
        .unwrap();
    sample_full(
        &store,
        "acct-a",
        "lane-a",
        150.0,
        AT - 10.0,
        AT + 10.0,
        AT + 20.0,
    );
    sample_full(
        &store,
        "acct-a",
        "lane-b",
        50.0,
        AT + 10.0,
        AT + 20.0,
        AT + 30.0,
    );
    let malformed = provider_candidates_at(
        &store,
        &catalog,
        &"pro".parse().unwrap(),
        "fast",
        None,
        &none_ineligible(),
        AT,
    )
    .unwrap();
    assert_eq!(summary(&malformed), vec![("acct-a".into(), 0, 1.0, false)]);
}

/// A provider with no bindings accounts for its models explicitly.
#[test]
fn empty_providers_are_accounted_explicitly() {
    let home = home(&[
        ("acct-a", "FAKE_A"),
        ("acct-b", "FAKE_B"),
        ("acct-c", "FAKE_C"),
    ]);
    let store = Store::open(home.path()).unwrap();
    let catalog = catalog(&store);
    let order = provider_order_at(&store, &catalog, &none_ineligible(), AT).unwrap();
    let bare = order
        .providers
        .iter()
        .find(|p| p.provider.as_str() == "glm-empty")
        .unwrap();
    assert_eq!(bare.models[0].status, "no_eligible_account");
    assert_eq!(bare.score, None);
}

/// Strict provider request fixture reused from the admission contract.
fn provider_request(home: &std::path::Path, id: &str) -> ProviderStartRequest {
    let mut request: ProviderStartRequest = serde_json::from_value(json!({
        "provider":"glm-user","model":"fixture","profile":"review",
        "task":"fixture","workdir":home,"request_id":id
    }))
    .unwrap();
    request.validate().unwrap();
    request
}

/// Frozen launch authority fixture reused from the admission contract.
fn authority(
    catalog: &ProviderCatalog,
    workdir: &std::path::Path,
) -> agent_run_domain::catalog::ResolvedLaunchAuthority {
    let provider = catalog.provider(&"glm-user".parse().unwrap()).unwrap();
    let mut role = json!({"role_name":"review"});
    role["config_revision"] = json!(agent_run_domain::canonical::sha256_hex(&role, true));
    agent_run_domain::catalog::ResolvedLaunchAuthority {
        provider: provider.id.clone(),
        harness: provider.harness,
        connection: provider.connection.clone(),
        model: "fixture".into(),
        effort: None,
        profile: "review".into(),
        workdir: workdir.canonicalize().unwrap(),
        role_payload: role,
        assets_sha256: "0".repeat(64).parse().unwrap(),
        eligible_accounts: vec!["acct-a".parse().unwrap(), "acct-b".parse().unwrap()],
    }
}

/// Replay identity fixture reused from the admission contract.
fn identity(
    request: &ProviderStartRequest,
    authority: &agent_run_domain::catalog::ResolvedLaunchAuthority,
) -> Value {
    json!({
        "provider_identity_version":2,
        "provider_request":request,
        "replay_request_sha256":agent_run_domain::canonical::sha256_hex(
            &serde_json::to_value(request).unwrap(), true),
        "authority":authority,
    })
}
