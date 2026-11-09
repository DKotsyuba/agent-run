//! Local selection rules driven only by synthetic Quota v2 standings.
use agent_run_core::{capacity::quota_adapter, quota_client::Snapshot, state::Store};
use agent_run_domain::{
    Error,
    catalog::{
        AccountRecord, AccountStatus, BindingCooldownReason, ProviderCatalog, ProviderDefinition,
        QuotaAdmissionError, QuotaCandidateSet, SelectionIntent,
    },
};
use serde_json::{Value, json};
use std::collections::BTreeSet;

const AT: f64 = 10001.0;
const KNOWN: &str = include_str!("fixtures/quota-client-v1/snapshot-v2.json");
const UNKNOWN: &str = include_str!("fixtures/quota-client-v1/unknown-snapshot-v2.json");
const EXHAUSTED: &str = include_str!("fixtures/quota-client-v1/exhausted-snapshot-v2.json");

/// Isolated local registry; no credential contents are read.
fn home() -> tempfile::TempDir {
    let home = tempfile::tempdir().unwrap();
    agent_run_store::fixture::initialize(home.path()).unwrap();
    let mut store = agent_run_store::fixture::open(home.path()).unwrap();
    for (id, name) in [("acct-a", "A"), ("acct-b", "B"), ("acct-c", "C")] {
        store
            .register_account(&AccountRecord {
                account_id: id.parse().unwrap(),
                auth_family: "anthropic".parse().unwrap(),
                secret_ref: format!("env:SYNTHETIC_{name}").parse().unwrap(),
                status: AccountStatus::Enabled,
            })
            .unwrap();
    }
    home
}

/// Two provider aliases share account A; duplicate labels never add weight.
fn catalog(store: &Store) -> ProviderCatalog {
    let definitions = serde_json::from_value(json!([
        {"id":"glm-user","harness":"claude-code","connection":{"kind":"custom","endpoint":"https://example.invalid/api","protocol":"messages"},"auth_family":"anthropic","limits_source":"none",
         "models":[{"id":"fixture"},{"id":"other","native_model":"other-native"}],
         "bindings":[{"label":"alpha","account":"acct-a","priority_multiplier":2.0},{"label":"alpha-two","account":"acct-a","priority_multiplier":3.0},{"label":"beta","account":"acct-b"},{"label":"heavy","account":"acct-c","priority_multiplier":100.0},{"label":"other-only","account":"acct-b","models":["other"]}]},
        {"id":"glm-alias","harness":"claude-code","connection":{"kind":"custom","endpoint":"https://example.invalid/api","protocol":"messages"},"auth_family":"anthropic","limits_source":"none",
         "models":[{"id":"fixture"}],"bindings":[{"label":"shared","account":"acct-a"}]}
    ])).unwrap();
    ProviderCatalog::new(store.list_accounts().unwrap(), definitions).unwrap()
}

/// Supplies explicit source classes and scores, with no local forecast math.
fn account(id: &str, fixture: &str, score: Option<f64>) -> Value {
    let raw: Value = serde_json::from_str(&fixture.replace("acct-test", id)).unwrap();
    let mut account = raw["result"]["accounts"][0].clone();
    account["models"][0]["provider"] = json!("glm-user");
    account["models"][0]["model"] = json!("fixture");
    account["models"][0]["native_model"] = json!("fixture");
    if let Some(score) = score {
        account["models"][0]["quota_score"] = json!(score);
    }
    for window in account["windows"].as_array_mut().unwrap() {
        window["models"] = json!(["fixture"]);
    }
    for latch in account["latches"].as_array_mut().unwrap() {
        latch["models"] = json!(["fixture"]);
    }
    account
}
fn snapshot() -> Snapshot {
    let mut raw: Value = serde_json::from_str(UNKNOWN).unwrap();
    let mut a = account("acct-a", KNOWN, Some(0.4));
    let mut alias = a["models"][0].clone();
    alias["provider"] = json!("glm-alias");
    a["models"].as_array_mut().unwrap().push(alias);
    raw["result"]["evaluated_at"] = json!(AT);
    raw["result"]["accounts"] = json!([
        a,
        account("acct-b", KNOWN, Some(0.6)),
        account("acct-c", UNKNOWN, None)
    ]);
    serde_json::from_value(raw["result"].clone()).unwrap()
}
fn select(
    store: &Store,
    catalog: &ProviderCatalog,
    provider: &str,
    model: &str,
    pin: Option<&str>,
    source: Option<&Snapshot>,
    at: f64,
) -> agent_run_domain::Result<QuotaCandidateSet> {
    Ok(quota_adapter::candidates_at(
        store,
        catalog,
        &provider.parse().unwrap(),
        model,
        pin,
        &BTreeSet::new(),
        source,
        at,
    )?
    .candidates)
}
fn summary(set: &QuotaCandidateSet) -> Vec<(String, u32, f64, bool)> {
    set.candidates
        .iter()
        .map(|c| {
            (
                c.account.as_str().into(),
                c.rank,
                c.multiplier.get(),
                c.quota_known,
            )
        })
        .collect()
}

/// Known source scores use maximum alias weight; unknown stays last and ties
/// remain stable across input order while physical pools stay account-bound.
#[test]
fn known_scores_alias_weights_unknown_and_ties_are_stable() {
    let home = home();
    let store = agent_run_store::fixture::open(home.path()).unwrap();
    let catalog = catalog(&store);
    let mut source = snapshot();
    let expected = vec![
        ("acct-a".into(), 0, 3.0, true),
        ("acct-b".into(), 1, 1.0, true),
        ("acct-c".into(), 2, 100.0, false),
    ];
    let set = select(
        &store,
        &catalog,
        "glm-user",
        "fixture",
        None,
        Some(&source),
        AT,
    )
    .unwrap();
    assert_eq!(summary(&set), expected);
    assert_eq!(
        set.capacity_revision,
        store.quota_capacity_revision().unwrap()
    );
    assert_eq!(
        set.candidates[0].physical_keys[0].as_str(),
        "acct-a::primary"
    );
    source.accounts.reverse();
    assert_eq!(
        summary(
            &select(
                &store,
                &catalog,
                "glm-user",
                "fixture",
                None,
                Some(&source),
                AT
            )
            .unwrap()
        ),
        expected
    );
    let mut tied = snapshot();
    tied.accounts
        .iter_mut()
        .find(|a| a.account == "acct-a")
        .unwrap()
        .models[0]
        .quota_score = Some(0.5);
    tied.accounts
        .iter_mut()
        .find(|a| a.account == "acct-b")
        .unwrap()
        .models[0]
        .quota_score = Some(1.5);
    let tie = select(
        &store,
        &catalog,
        "glm-user",
        "fixture",
        None,
        Some(&tied),
        AT,
    )
    .unwrap();
    assert_eq!(
        summary(&tie)[..2],
        [
            ("acct-a".into(), 0, 3.0, true),
            ("acct-b".into(), 0, 1.0, true)
        ]
    );
    tied.accounts.reverse();
    let reversed = select(
        &store,
        &catalog,
        "glm-user",
        "fixture",
        None,
        Some(&tied),
        AT,
    )
    .unwrap();
    assert_eq!(summary(&reversed), summary(&tie));
    let alias = select(
        &store,
        &catalog,
        "glm-alias",
        "fixture",
        None,
        Some(&source),
        AT,
    )
    .unwrap();
    assert_eq!(
        alias.candidates[0].physical_keys[0].as_str(),
        "acct-a::primary"
    );
    let pinned = select(
        &store,
        &catalog,
        "glm-user",
        "fixture",
        Some("alpha-two"),
        Some(&source),
        AT,
    )
    .unwrap();
    assert_eq!(
        pinned.intent,
        SelectionIntent::Pinned("acct-a".parse().unwrap())
    );
    assert_eq!(pinned.candidates.len(), 1);
    let pinned_unknown = select(
        &store,
        &catalog,
        "glm-user",
        "fixture",
        Some("heavy"),
        Some(&source),
        AT,
    )
    .unwrap();
    assert_eq!(
        pinned_unknown.intent,
        SelectionIntent::Pinned("acct-c".parse().unwrap())
    );
    assert_eq!(pinned_unknown.candidates.len(), 1);
    assert!(!pinned_unknown.candidates[0].quota_known);
    assert!(pinned_unknown.candidates[0].physical_keys.is_empty());
    let other = select(
        &store,
        &catalog,
        "glm-user",
        "other",
        None,
        Some(&source),
        AT,
    )
    .unwrap();
    assert!(other.candidates.iter().all(|c| !c.quota_known));
}

/// Cooldowns are local to provider/account, exact at expiry and retain their
/// original billing/quota reason even when source standings are available.
#[test]
fn cooldown_scope_nearest_expiry_and_exhausted_precedence() {
    let home = home();
    let store = agent_run_store::fixture::open(home.path()).unwrap();
    let catalog = catalog(&store);
    for (id, reason, until) in [
        ("acct-a", "billing_unavailable", AT + 3599.25),
        ("acct-b", "quota_exhausted", AT + 20.1),
        ("acct-c", "billing_unavailable", AT + 3000.0),
    ] {
        store.conn.execute("INSERT INTO binding_cooldowns(provider_id,account_id,reason,observed_at,blocked_until,attempt_id) VALUES('glm-user',?1,?2,?3,?4,'synthetic')",rusqlite::params![id,reason,AT,until]).unwrap();
    }
    let mut source = snapshot();
    let err = select(
        &store,
        &catalog,
        "glm-user",
        "fixture",
        None,
        Some(&source),
        AT,
    )
    .unwrap_err();
    assert!(
        matches!(err,Error::QuotaAdmission(QuotaAdmissionError::BindingCooldown{account,reason:BindingCooldownReason::QuotaExhausted,retry_after_seconds:21,..}) if account.as_str()=="acct-b")
    );
    let err = select(
        &store,
        &catalog,
        "glm-user",
        "fixture",
        Some("alpha"),
        Some(&source),
        AT,
    )
    .unwrap_err();
    assert!(
        matches!(err,Error::QuotaAdmission(QuotaAdmissionError::BindingCooldown{account,reason:BindingCooldownReason::BillingUnavailable,retry_after_seconds:3600,..}) if account.as_str()=="acct-a")
    );
    assert_eq!(
        select(
            &store,
            &catalog,
            "glm-alias",
            "fixture",
            None,
            Some(&source),
            AT
        )
        .unwrap()
        .candidates[0]
            .account
            .as_str(),
        "acct-a"
    );
    store
        .conn
        .execute(
            "DELETE FROM binding_cooldowns WHERE account_id='acct-b'",
            [],
        )
        .unwrap();
    let mut exhausted: Value = account("acct-b", EXHAUSTED, None);
    exhausted["models"][0]["state_validity"] = json!({"kind":"until","at":AT+5.0});
    exhausted["models"][0]["exhausted_until"] = json!(AT + 5.0);
    source.accounts.retain(|view| view.account != "acct-b");
    source
        .accounts
        .push(serde_json::from_value(exhausted).unwrap());
    let err = select(
        &store,
        &catalog,
        "glm-user",
        "fixture",
        None,
        Some(&source),
        AT,
    )
    .unwrap_err();
    assert!(
        matches!(err,Error::QuotaAdmission(QuotaAdmissionError::QuotaExhausted{account,..}) if account.as_str()=="acct-b")
    );
    store.conn.execute("INSERT INTO binding_cooldowns(provider_id,account_id,reason,observed_at,blocked_until,attempt_id) VALUES('glm-user','acct-b','quota_exhausted',?1,?2,'synthetic')",rusqlite::params![AT,AT+20.1]).unwrap();
    assert_eq!(
        select(
            &store,
            &catalog,
            "glm-user",
            "fixture",
            None,
            None,
            AT + 20.1
        )
        .unwrap()
        .candidates[0]
            .account
            .as_str(),
        "acct-b"
    );
}

/// Ordered provider weights choose fallback order, without scaling the source
/// score; disabled and exhausted accounts are still excluded.
#[test]
fn ordered_bindings_keep_fallback_order_and_exclude_blocked_accounts() {
    let home = home();
    let mut store = agent_run_store::fixture::open(home.path()).unwrap();
    let mut provider: ProviderDefinition = catalog(&store)
        .provider(&"glm-user".parse().unwrap())
        .unwrap()
        .clone();
    provider.ordered_accounts = true;
    provider.bindings = serde_json::from_value(json!([
        {"label":"beta","account":"acct-b","priority_multiplier":1.0},
        {"label":"gamma","account":"acct-c","priority_multiplier":1.0},
        {"label":"alpha","account":"acct-a","priority_multiplier":1.7976931348623157e308}
    ]))
    .unwrap();
    let catalog = ProviderCatalog::new(store.list_accounts().unwrap(), vec![provider]).unwrap();
    let mut source = snapshot();
    assert_eq!(
        select(
            &store,
            &catalog,
            "glm-user",
            "fixture",
            None,
            Some(&source),
            AT
        )
        .unwrap()
        .candidates
        .iter()
        .map(|c| c.account.as_str())
        .collect::<Vec<_>>(),
        ["acct-a", "acct-b", "acct-c"]
    );
    source.accounts.retain(|view| view.account != "acct-a");
    source
        .accounts
        .push(serde_json::from_value(account("acct-a", EXHAUSTED, None)).unwrap());
    store.disable_account(&"acct-b".parse().unwrap()).unwrap();
    let set = select(
        &store,
        &catalog,
        "glm-user",
        "fixture",
        None,
        Some(&source),
        AT,
    )
    .unwrap();
    assert_eq!(set.candidates.len(), 1);
    assert_eq!(set.candidates[0].account.as_str(), "acct-c");
    assert!(!set.candidates[0].quota_known);
    let err = select(
        &store,
        &catalog,
        "glm-user",
        "fixture",
        Some("beta"),
        Some(&source),
        AT,
    )
    .unwrap_err();
    assert!(matches!(
        err,
        Error::QuotaAdmission(QuotaAdmissionError::NoEligibleAccount { .. })
    ));
}

/// Bad identities and overflowing local weights refuse explicitly rather than
/// manufacturing unknown or a finite priority.
#[test]
fn invalid_selection_and_priority_overflow_are_explicit() {
    let home = home();
    let store = agent_run_store::fixture::open(home.path()).unwrap();
    let mut provider = catalog(&store)
        .provider(&"glm-user".parse().unwrap())
        .unwrap()
        .clone();
    provider.bindings = serde_json::from_value(
        json!([{"label":"alpha","account":"acct-a","priority_multiplier":1.7976931348623157e308}]),
    )
    .unwrap();
    let catalog = ProviderCatalog::new(store.list_accounts().unwrap(), vec![provider]).unwrap();
    let mut source = snapshot();
    source
        .accounts
        .iter_mut()
        .find(|a| a.account == "acct-a")
        .unwrap()
        .models[0]
        .quota_score = Some(2.0);
    for (provider, model) in [
        ("glm-user", " "),
        ("missing", "fixture"),
        ("glm-user", "missing"),
    ] {
        assert!(select(&store, &catalog, provider, model, None, Some(&source), AT).is_err());
    }
    let error = select(
        &store,
        &catalog,
        "glm-user",
        "fixture",
        None,
        Some(&source),
        AT,
    )
    .unwrap_err();
    assert!(error.to_string().contains("overflow"));
}
