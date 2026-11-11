//! Synthetic Quota standings with the real local registry and admission fence.
use agent_run_core::{capacity::quota_adapter, quota_client::Snapshot, state::Store};
use agent_run_domain::{
    Error, ProviderStartRequest,
    catalog::{
        AccountId, AccountRecord, AccountStatus, ProviderCatalog, ProviderDefinition,
        QuotaAdmissionError, ResolvedLaunchAuthority,
    },
};
use serde_json::{Value, json};
use std::{collections::BTreeSet, path::Path};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::UnixListener,
};

const AT: f64 = 10001.0;
const KNOWN: &str = include_str!("fixtures/quota-client-v1/snapshot-v2.json");
const UNKNOWN: &str = include_str!("fixtures/quota-client-v1/unknown-snapshot-v2.json");
const EXHAUSTED: &str = include_str!("fixtures/quota-client-v1/exhausted-snapshot-v2.json");

/// Initializes a private synthetic registry without reading credentials or starting models.
fn home() -> tempfile::TempDir {
    let base = std::env::var_os("AGENT_RUN_TEST_TMP")
        .map(std::path::PathBuf::from)
        .unwrap_or_else(std::env::temp_dir);
    let home = tempfile::Builder::new()
        .prefix("quota-adapter-v1-")
        .tempdir_in(base)
        .unwrap();
    agent_run_core::state::fixture::initialize(home.path()).unwrap();
    let mut store = agent_run_core::state::fixture::open(home.path()).unwrap();
    for (account, secret) in [
        ("acct-a", "A"),
        ("acct-b", "B"),
        ("acct-c", "C"),
        ("acct-d", "D"),
        ("acct-e", "E"),
    ] {
        store
            .register_account(&AccountRecord {
                account_id: account.parse().unwrap(),
                auth_family: "anthropic".parse().unwrap(),
                secret_ref: format!("env:QUOTA_FAKE_{secret}").parse().unwrap(),
                status: AccountStatus::Enabled,
            })
            .unwrap();
    }
    drop(store);
    home
}

/// Builds one provider with explicit aliases; ordered providers use distinct accounts.
fn catalog(store: &Store, ordered: bool) -> ProviderCatalog {
    let mut bindings = vec![
        json!({"label":"alpha","account":"acct-a","priority_multiplier":2.0}),
        json!({"label":"beta","account":"acct-b","priority_multiplier":3.0}),
        json!({"label":"gamma","account":"acct-c","priority_multiplier":1.0}),
        json!({"label":"delta","account":"acct-d","priority_multiplier":4.0}),
        json!({"label":"epsilon","account":"acct-e","priority_multiplier":1.0}),
    ];
    if !ordered {
        bindings.push(json!({"label":"alpha-alias","account":"acct-a","priority_multiplier":3.0}));
    }
    let definition: ProviderDefinition = serde_json::from_value(json!({
        "id":"Claude", "harness":"claude-code",
        "connection":{"kind":"custom","endpoint":"https://example.invalid/api","protocol":"messages"},
        "auth_family":"anthropic", "limits_source":"none", "ordered_accounts":ordered,
        "models":[{"id":"model-a","native_model":"native-a"}], "bindings":bindings
    })).unwrap();
    ProviderCatalog::new(store.list_accounts().unwrap(), vec![definition]).unwrap()
}

/// Constructs one service-format account with an explicit provider and native model.
fn account(id: &str, input: &str, provider: &str) -> Value {
    let fixture = input.replace("acct-test", id);
    let source: Value = serde_json::from_str(&fixture).unwrap();
    let mut account = source["result"]["accounts"][0].clone();
    account["models"][0]["provider"] = json!(provider);
    account["models"][0]["native_model"] = json!("native-a");
    for window in account["windows"].as_array_mut().unwrap() {
        window["runtime"] = json!(provider);
        window["models"] = json!(["native-a"]);
    }
    for latch in account["latches"].as_array_mut().unwrap() {
        latch["models"] = json!(["native-a"]);
    }
    account
}

/// A coherent bounded source snapshot; model states come from frozen service fixtures.
fn snapshot() -> Snapshot {
    let mut source: Value = serde_json::from_str(UNKNOWN).unwrap();
    source["result"]["evaluated_at"] = json!(AT);
    source["result"]["accounts"] = json!([
        account("acct-a", KNOWN, "Claude"),
        account("acct-b", KNOWN, "Claude"),
        account("acct-c", UNKNOWN, "Claude"),
        account("acct-d", KNOWN, "Claude"),
        account("acct-e", EXHAUSTED, "Claude")
    ]);
    serde_json::from_value(source["result"].clone()).unwrap()
}

/// A source evaluated at the live clock so the real writer can validate it.
fn current_snapshot() -> (Snapshot, f64) {
    let mut source = snapshot();
    let at = agent_run_core::domain::now();
    source.evaluated_at = at;
    for account in &mut source.accounts {
        if matches!(
            account.models[0].state_validity,
            agent_run_core::quota_client::StateValidity::Until { .. }
        ) {
            account.models[0].state_validity =
                agent_run_core::quota_client::StateValidity::Until { at: at + 3600.0 };
            account.models[0].reset_at = Some(at + 7200.0);
            account.models[0].newest_observed_at = Some(at - 1.0);
            for window in &mut account.windows {
                window.observed_at = Some(at - 1.0);
                window.valid_until = Some(at + 3600.0);
                window.last_reset_at = Some(at + 7200.0);
                window.forecast.observed_at = Some(at - 1.0);
                window.forecast.reset_at = Some(at + 7200.0);
                if let Some(evaluation) = &mut window.evaluation {
                    evaluation.reset_at = Some(at + 7200.0);
                }
            }
            for latch in &mut account.latches {
                latch.observed_at = at - 1.0;
                latch.reset_at = Some(at + 3600.0);
                account.models[0].reset_at = latch.reset_at;
                account.models[0].exhausted_until = latch.reset_at;
            }
        }
    }
    (source, at)
}

/// Runs a local decision at the fixed clock against one already obtained snapshot.
fn candidates(
    store: &Store,
    catalog: &ProviderCatalog,
    snapshot: Option<&Snapshot>,
    pin: Option<&str>,
    hard: &BTreeSet<AccountId>,
) -> agent_run_domain::Result<quota_adapter::CandidateOutput> {
    candidates_at(store, catalog, snapshot, pin, hard, AT)
}

/// Uses one explicitly supplied decision clock without touching the Quota service.
fn candidates_at(
    store: &Store,
    catalog: &ProviderCatalog,
    snapshot: Option<&Snapshot>,
    pin: Option<&str>,
    hard: &BTreeSet<AccountId>,
    now: f64,
) -> agent_run_domain::Result<quota_adapter::CandidateOutput> {
    quota_adapter::candidates_at(
        store,
        catalog,
        &"Claude".parse().unwrap(),
        "model-a",
        pin,
        hard,
        snapshot,
        now,
    )
}

#[test]
fn quota_adapter_v1_validity_is_exclusive_and_keeps_reported_pools_on_unknown() {
    let home = home();
    let store = agent_run_core::state::fixture::open(home.path()).unwrap();
    let catalog = catalog(&store, false);
    let source = snapshot();
    for (now, known) in [
        (AT - 1.0, false),
        (AT, true),
        (10899.0, true),
        (10900.0, false),
        (10901.0, false),
    ] {
        let result = candidates_at(
            &store,
            &catalog,
            Some(&source),
            Some("alpha"),
            &BTreeSet::new(),
            now,
        )
        .unwrap();
        let candidate = &result.candidates.candidates[0];
        assert_eq!(candidate.quota_known, known, "clock {now}");
        assert_eq!(candidate.physical_keys[0].as_str(), "acct-a::primary");
    }
    let unavailable =
        candidates_at(&store, &catalog, None, Some("alpha"), &BTreeSet::new(), AT).unwrap();
    assert!(!unavailable.candidates.candidates[0].quota_known);
    assert!(
        unavailable.candidates.candidates[0]
            .physical_keys
            .is_empty()
    );
}

#[test]
fn quota_adapter_v1_keeps_live_exhaustion_despite_stale_window() {
    let home = home();
    let store = agent_run_core::state::fixture::open(home.path()).unwrap();
    let catalog = catalog(&store, false);
    let source = snapshot();
    let clock_backwards = candidates_at(
        &store,
        &catalog,
        Some(&source),
        Some("epsilon"),
        &BTreeSet::new(),
        AT - 1.0,
    )
    .unwrap();
    assert!(!clock_backwards.candidates.candidates[0].quota_known);
    assert_eq!(
        clock_backwards.candidates.candidates[0].physical_keys[0].as_str(),
        "acct-e::primary"
    );
    let live = candidates_at(
        &store,
        &catalog,
        Some(&source),
        Some("epsilon"),
        &BTreeSet::new(),
        11000.0,
    );
    assert!(matches!(
        live,
        Err(Error::QuotaAdmission(
            QuotaAdmissionError::QuotaExhausted { .. }
        ))
    ));
    let expired = candidates_at(
        &store,
        &catalog,
        Some(&source),
        Some("epsilon"),
        &BTreeSet::new(),
        20000.0,
    )
    .unwrap();
    assert!(!expired.candidates.candidates[0].quota_known);
    assert_eq!(
        expired.candidates.candidates[0].physical_keys[0].as_str(),
        "acct-e::primary"
    );

    for now in [AT, 1_000_000.0] {
        let mut fresh = snapshot();
        fresh.evaluated_at = now;
        let account = fresh
            .accounts
            .iter_mut()
            .find(|account| account.account == "acct-e")
            .unwrap();
        account.models[0].state_validity =
            agent_run_core::quota_client::StateValidity::LatchedNoReset;
        account.models[0].reset_at = None;
        account.models[0].exhausted_until = None;
        account.latches[0].reset_at = None;
        if now > 10901.0 {
            account.models[0].remaining_percent = None;
            account.models[0].burn_percent_per_hour = None;
            account.models[0].risk = agent_run_core::quota_client::Risk::Unknown;
            let window = &mut account.windows[0];
            window.forecast.known = false;
            window.forecast.remaining_percent = None;
            window.forecast.reset_at = None;
            window.forecast.observed_at = None;
            window.forecast.warmup = true;
            window.forecast.burn_percent_per_hour = None;
            window.forecast.sustainable_percent_per_hour = None;
            window.forecast.risk = agent_run_core::quota_client::Risk::Unknown;
            window.forecast.burn_span_seconds = None;
            window.evaluation = None;
        }
        let no_reset = candidates_at(
            &store,
            &catalog,
            Some(&fresh),
            Some("epsilon"),
            &BTreeSet::new(),
            now,
        );
        assert!(matches!(
            no_reset,
            Err(Error::QuotaAdmission(
                QuotaAdmissionError::QuotaExhausted { .. }
            ))
        ));
    }
}

#[tokio::test]
async fn quota_adapter_v1_socket_decodes_nonlimiting_deadline_before_decision() {
    let home = home();
    let store = agent_run_core::state::fixture::open(home.path()).unwrap();
    let catalog = catalog(&store, false);
    let mut response: Value = serde_json::from_str(KNOWN).unwrap();
    let mut view = account("acct-a", KNOWN, "Claude");
    view["models"][0]["state_validity"] = json!({"kind":"until","at":10500.0});
    let mut other = view["windows"][0].clone();
    other["window"] = json!("non-limiting");
    other["valid_until"] = json!(10500.0);
    other["last_remaining_percent"] = json!(90.0);
    other["forecast"]["key"]["window"] = json!("non-limiting");
    other["forecast"]["remaining_percent"] = json!(90.0);
    other["evaluation"]["key"]["window"] = json!("non-limiting");
    other["evaluation"]["remaining_percent"] = json!(90.0);
    other["evaluation"]["slack"] = json!(0.8);
    view["windows"].as_array_mut().unwrap().push(other);
    let mut advice = view["advice"][0].clone();
    advice["key"]["window"] = json!("non-limiting");
    advice["remaining_percent"] = json!(90.0);
    view["advice"].as_array_mut().unwrap().push(advice);
    response["result"]["accounts"] = json!([view]);
    response["result"]["coverage"]["last_collection"]["sources"][0]["account"] = json!("acct-a");
    response["result"]["coverage"]["last_collection"]["sources"][0]["provider"] = json!("Claude");
    let mut bytes = serde_json::to_vec(&response).unwrap();
    bytes.push(b'\n');
    let base = std::env::var_os("AGENT_RUN_TEST_SOCKET_TMP")
        .map(std::path::PathBuf::from)
        .unwrap_or_else(std::env::temp_dir);
    let dir = tempfile::Builder::new()
        .prefix("qa-v2-")
        .tempdir_in(base)
        .unwrap();
    let path = dir.path().join("quota.sock");
    let listener = UnixListener::bind(&path).unwrap();
    let server = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        let mut request = Vec::new();
        stream.read_to_end(&mut request).await.unwrap();
        stream.write_all(&bytes).await.unwrap();
    });
    let decoded = tokio::time::timeout(
        std::time::Duration::from_secs(3),
        agent_run_core::quota_client::snapshot(&path, None, &["acct-a".into()], Some("model-a")),
    )
    .await
    .unwrap()
    .unwrap();
    server.await.unwrap();
    let before = candidates_at(
        &store,
        &catalog,
        Some(&decoded),
        Some("alpha"),
        &BTreeSet::new(),
        10499.0,
    )
    .unwrap();
    assert!(before.candidates.candidates[0].quota_known);
    let at_boundary = candidates_at(
        &store,
        &catalog,
        Some(&decoded),
        Some("alpha"),
        &BTreeSet::new(),
        10500.0,
    )
    .unwrap();
    assert!(!at_boundary.candidates.candidates[0].quota_known);
    assert_eq!(
        at_boundary.candidates.candidates[0].physical_keys[0].as_str(),
        "acct-a::primary"
    );
    assert_eq!(
        at_boundary.quota_reference.unwrap().revision,
        decoded.revision
    );
}

/// Summarizes only stable candidate fields for ordering assertions.
fn summary(set: &agent_run_domain::catalog::QuotaCandidateSet) -> Vec<(String, u32, f64, bool)> {
    set.candidates
        .iter()
        .map(|entry| {
            (
                entry.account.to_string(),
                entry.rank,
                entry.multiplier.get(),
                entry.quota_known,
            )
        })
        .collect()
}

#[test]
fn quota_adapter_v1_keeps_states_weights_aliases_and_local_revision() {
    let home = home();
    let store = agent_run_core::state::fixture::open(home.path()).unwrap();
    let catalog = catalog(&store, false);
    // Local legacy latches are not Quota evidence and must not affect this result.
    store.conn.execute("INSERT INTO quota_exhaustion(account_id,quota_key,source,window_id,observed_at,reset_at) VALUES('acct-a','acct-a::primary','old-source','old-window',?1,?2)", rusqlite::params![AT, AT + 10000.0]).unwrap();
    let source = snapshot();
    let output = candidates(&store, &catalog, Some(&source), None, &BTreeSet::new()).unwrap();
    assert_eq!(
        summary(&output.candidates),
        vec![
            ("acct-d".into(), 0, 4.0, true),
            ("acct-a".into(), 1, 3.0, true),
            ("acct-b".into(), 1, 3.0, true),
            ("acct-c".into(), 2, 1.0, false),
        ]
    );
    assert_eq!(
        output.candidates.capacity_revision,
        store.quota_capacity_revision().unwrap()
    );
    let reference = output.quota_reference.unwrap();
    assert_eq!(reference.revision, source.revision);
    assert_eq!(reference.source_epoch, source.source_epoch);
    assert_eq!(reference.config_revision, source.config_revision);
    assert_eq!(reference.evaluated_at, source.evaluated_at);
    assert_ne!(
        Some(output.candidates.capacity_revision),
        reference.revision
    );
    assert_eq!(
        output.candidates.candidates[1].physical_keys[0].as_str(),
        "acct-a::primary"
    );
}

#[test]
fn quota_adapter_v1_fence_includes_exhausted_accounts_and_no_reset_latches() {
    let home = home();
    let store = agent_run_core::state::fixture::open(home.path()).unwrap();
    let catalog = catalog(&store, false);
    let mut source = snapshot();
    let exhausted = source
        .accounts
        .iter_mut()
        .find(|account| account.account == "acct-e")
        .unwrap();
    exhausted.models[0].state_validity =
        agent_run_core::quota_client::StateValidity::Until { at: 10500.0 };
    let result = candidates(&store, &catalog, Some(&source), None, &BTreeSet::new()).unwrap();
    assert!(
        result
            .candidates
            .candidates
            .iter()
            .all(|candidate| candidate.account.as_str() != "acct-e")
    );
    let decision = result.candidates.quota_decision.as_ref().unwrap();
    assert!(decision.requires_clock_fence);
    assert_eq!(decision.valid_until, Some(10500.0));
    assert!(decision.check_admission(10499.0).is_ok());
    assert!(decision.check_admission(10500.0).is_err());

    let exhausted = source
        .accounts
        .iter_mut()
        .find(|account| account.account == "acct-e")
        .unwrap();
    exhausted.models[0].state_validity =
        agent_run_core::quota_client::StateValidity::LatchedNoReset;
    exhausted.models[0].exhausted_until = None;
    exhausted.latches[0].reset_at = None;
    let excluded: BTreeSet<AccountId> = ["acct-a", "acct-b", "acct-d"]
        .into_iter()
        .map(|value| value.parse().unwrap())
        .collect();
    let result = candidates(&store, &catalog, Some(&source), None, &excluded).unwrap();
    let decision = result.candidates.quota_decision.as_ref().unwrap();
    assert!(decision.requires_clock_fence);
    assert_eq!(decision.valid_until, None);
    assert!(decision.check_admission(AT - 1.0).is_err());
    assert!(decision.check_admission(AT).is_ok());
}

#[test]
fn quota_adapter_v1_matches_provider_and_native_model_exactly() {
    let home = home();
    let store = agent_run_core::state::fixture::open(home.path()).unwrap();
    let catalog = catalog(&store, false);
    let mut raw: Value = serde_json::from_str(UNKNOWN).unwrap();
    let mut wrong_native = account("acct-b", KNOWN, "Claude");
    wrong_native["models"][0]["native_model"] = json!("native-other");
    raw["result"]["accounts"] = json!([
        account("acct-a", KNOWN, "claude"),
        wrong_native,
        account("acct-e", EXHAUSTED, "1gateway")
    ]);
    let source: Snapshot = serde_json::from_value(raw["result"].clone()).unwrap();
    let output = candidates(&store, &catalog, Some(&source), None, &BTreeSet::new()).unwrap();
    assert_eq!(output.candidates.candidates.len(), 5);
    assert!(
        output
            .candidates
            .candidates
            .iter()
            .all(|candidate| !candidate.quota_known && candidate.physical_keys.is_empty())
    );
}

#[test]
fn quota_adapter_v1_preserves_ordered_bindings_and_local_cooldown() {
    let home = home();
    let store = agent_run_core::state::fixture::open(home.path()).unwrap();
    let catalog = catalog(&store, true);
    let source = snapshot();
    let result = candidates(&store, &catalog, Some(&source), None, &BTreeSet::new()).unwrap();
    assert_eq!(
        summary(&result.candidates),
        vec![
            ("acct-d".into(), 0, 4.0, true),
            ("acct-b".into(), 1, 3.0, true),
            ("acct-a".into(), 2, 2.0, true),
            ("acct-c".into(), 3, 1.0, false),
        ]
    );
    store.conn.execute("INSERT INTO binding_cooldowns(provider_id,account_id,reason,observed_at,blocked_until,attempt_id) VALUES('Claude','acct-a','billing_unavailable',?1,?2,'synthetic')", rusqlite::params![AT, AT+50.0]).unwrap();
    let blocked = candidates(
        &store,
        &catalog,
        Some(&source),
        Some("alpha"),
        &BTreeSet::new(),
    );
    assert!(matches!(
        blocked,
        Err(Error::QuotaAdmission(
            QuotaAdmissionError::BindingCooldown { .. }
        ))
    ));
    let unpinned = candidates(&store, &catalog, Some(&source), None, &BTreeSet::new()).unwrap();
    assert!(
        unpinned
            .candidates
            .candidates
            .iter()
            .all(|candidate| candidate.account.as_str() != "acct-a")
    );
}

#[test]
fn quota_adapter_v1_unavailable_and_pool_limit_are_explicit() {
    let home = home();
    let store = agent_run_core::state::fixture::open(home.path()).unwrap();
    let catalog = catalog(&store, false);
    let unavailable = candidates(&store, &catalog, None, None, &BTreeSet::new()).unwrap();
    assert!(unavailable.quota_reference.is_none());
    assert!(
        unavailable
            .candidates
            .candidates
            .iter()
            .all(|candidate| !candidate.quota_known && candidate.physical_keys.is_empty())
    );
    let mut raw: Value = serde_json::from_str(KNOWN).unwrap();
    let mut view = account("acct-a", KNOWN, "Claude");
    let pools: Vec<String> = std::iter::once("acct-a::primary".to_owned())
        .chain((1..32).map(|number| format!("acct-a::pool-{number}")))
        .collect();
    view["models"][0]["pools"] = json!(pools);
    raw["result"]["accounts"] = json!([view]);
    let source: Snapshot = serde_json::from_value(raw["result"].clone()).unwrap();
    let accepted = candidates(
        &store,
        &catalog,
        Some(&source),
        Some("alpha"),
        &BTreeSet::new(),
    )
    .unwrap();
    assert_eq!(accepted.candidates.candidates[0].physical_keys.len(), 32);
    raw["result"]["accounts"][0]["models"][0]["pools"]
        .as_array_mut()
        .unwrap()
        .push(json!("acct-a::pool-32"));
    let too_many: Snapshot = serde_json::from_value(raw["result"].clone()).unwrap();
    assert!(matches!(
        candidates(
            &store,
            &catalog,
            Some(&too_many),
            Some("alpha"),
            &BTreeSet::new()
        ),
        Err(Error::Validation(_))
    ));
}

/// Produces a strict admission request and matching frozen authority.
fn request(home: &Path, id: &str) -> ProviderStartRequest {
    let mut request: ProviderStartRequest = serde_json::from_value(json!({
        "provider":"Claude", "model":"model-a", "profile":"review", "task":"synthetic", "workdir":home, "request_id":id
    })).unwrap();
    request.validate().unwrap();
    request
}

/// Creates the authority used by the existing store admission boundary.
fn authority(catalog: &ProviderCatalog, home: &Path) -> ResolvedLaunchAuthority {
    let definition = catalog.provider(&"Claude".parse().unwrap()).unwrap();
    let mut role = json!({"role_name":"review"});
    role["config_revision"] = json!(agent_run_domain::canonical::sha256_hex(&role, true));
    ResolvedLaunchAuthority {
        provider: definition.id.clone(),
        harness: definition.harness,
        connection: definition.connection.clone(),
        model: "model-a".into(),
        effort: None,
        profile: "review".into(),
        workdir: home.canonicalize().unwrap(),
        role_payload: role,
        assets_sha256: "0".repeat(64).parse().unwrap(),
        eligible_accounts: vec!["acct-a".parse().unwrap(), "acct-b".parse().unwrap()],
    }
}

/// Supplies the replay and authority evidence required by the store.
fn identity(request: &ProviderStartRequest, authority: &ResolvedLaunchAuthority) -> Value {
    json!({"provider_identity_version":2, "provider_request":request,
        "replay_request_sha256":agent_run_domain::canonical::sha256_hex(&serde_json::to_value(request).unwrap(), true),
        "authority":authority})
}

#[test]
fn quota_adapter_v1_writer_rechecks_local_revision_and_active_ties() {
    let home = home();
    let mut store = agent_run_core::state::fixture::open(home.path()).unwrap();
    let catalog = catalog(&store, false);
    let excluded: BTreeSet<AccountId> = ["acct-c", "acct-d", "acct-e"]
        .into_iter()
        .map(|value| value.parse().unwrap())
        .collect();
    let (source, source_at) = current_snapshot();
    let first = candidates_at(&store, &catalog, Some(&source), None, &excluded, source_at)
        .unwrap()
        .candidates;
    let authority = authority(&catalog, home.path());
    let first_request = request(home.path(), "adapter-first");
    let admitted = store
        .admit_provider(
            &first_request,
            &first_request.storage_projection(),
            &catalog,
            &authority,
            &first,
            &identity(&first_request, &authority),
            4,
            None,
            None,
        )
        .unwrap();
    assert_eq!(admitted.account_id.as_str(), "acct-a");
    let second_request = request(home.path(), "adapter-second");
    let stale = store.admit_provider(
        &second_request,
        &second_request.storage_projection(),
        &catalog,
        &authority,
        &first,
        &identity(&second_request, &authority),
        4,
        None,
        None,
    );
    assert!(matches!(
        stale,
        Err(Error::QuotaAdmission(
            QuotaAdmissionError::SelectionStale { .. }
        ))
    ));
    let (refreshed_source, refreshed_at) = current_snapshot();
    let refreshed = candidates_at(
        &store,
        &catalog,
        Some(&refreshed_source),
        None,
        &excluded,
        refreshed_at,
    )
    .unwrap();
    assert_eq!(
        refreshed.quota_reference.as_ref().unwrap().revision,
        source.revision
    );
    assert_ne!(
        refreshed.candidates.capacity_revision,
        first.capacity_revision
    );
    let second = store
        .admit_provider(
            &second_request,
            &second_request.storage_projection(),
            &catalog,
            &authority,
            &refreshed.candidates,
            &identity(&second_request, &authority),
            4,
            None,
            None,
        )
        .unwrap();
    assert_eq!(second.account_id.as_str(), "acct-b");
}

#[test]
fn quota_adapter_v1_batch_calls_share_the_callers_local_read_snapshot() {
    let home = home();
    let store = agent_run_core::state::fixture::open(home.path()).unwrap();
    let catalog = catalog(&store, false);
    let source = snapshot();
    let read = store.conn.unchecked_transaction().unwrap();
    let first = candidates(&store, &catalog, Some(&source), None, &BTreeSet::new())
        .unwrap()
        .candidates;

    let mut concurrent = agent_run_core::state::fixture::open(home.path()).unwrap();
    concurrent
        .disable_account(&"acct-b".parse().unwrap())
        .unwrap();
    let second = candidates(&store, &catalog, Some(&source), None, &BTreeSet::new())
        .unwrap()
        .candidates;
    assert_eq!(first.capacity_revision, second.capacity_revision);
    assert_eq!(summary(&first), summary(&second));
    drop(read);

    let after = candidates(&store, &catalog, Some(&source), None, &BTreeSet::new())
        .unwrap()
        .candidates;
    assert_eq!(after.capacity_revision, first.capacity_revision + 1);
    assert!(
        after
            .candidates
            .iter()
            .all(|candidate| candidate.account.as_str() != "acct-b")
    );
}
