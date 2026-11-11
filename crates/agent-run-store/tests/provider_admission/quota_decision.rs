//! External quota clock fences and audit references in disposable transactions.

use super::*;
use agent_run_domain::catalog::QuotaDecision;

/// Builds valid source identity with timestamps chosen by each test.
fn reference(evaluated_at: f64, valid_until: Option<f64>) -> QuotaDecision {
    QuotaDecision {
        source_epoch: "a".repeat(32),
        revision: Some(900_000),
        config_revision: "b".repeat(64),
        evaluated_at,
        valid_until,
        requires_clock_fence: true,
    }
}

/// Reads one event's JSON audit payload without creating any quota storage.
fn event(store: &Store, id: &agent_run_domain::domain::AgentId, kind: &str) -> Value {
    let text: String = store
        .conn
        .query_row(
            "SELECT data_json FROM events WHERE agent_id=? AND kind=?",
            rusqlite::params![id.as_str(), kind],
            |row| row.get(0),
        )
        .unwrap();
    serde_json::from_str(&text).unwrap()
}

/// The event contains precisely four bounded reference fields, not validity,
/// provider quota state, or additional history/adapter records.
fn assert_audit(metadata: &Value, decision: &QuotaDecision) {
    assert_eq!(
        metadata["quota_decision"],
        json!({
            "source_epoch": decision.source_epoch,
            "revision": decision.revision,
            "config_revision": decision.config_revision,
            "evaluated_at": decision.evaluated_at,
        })
    );
}

/// Malformed, expired and future-clock decisions reject without rows; a valid
/// source revision is independent of local revision, and replay bypasses both.
#[test]
fn quota_decision_admission_guard_audit_and_replay() {
    let home = home();
    let mut store = agent_run_store::fixture::open(home.path()).unwrap();
    let catalog = catalog(&store);
    let authority = authority(&catalog, home.path());
    let request = provider_request(home.path(), "quota-decision-start");
    let effective = request.storage_projection();
    let identity = identity(&request, &authority);
    let revision = store.quota_capacity_revision().unwrap();
    let mut offered = candidates(revision, &[("acct-a", 0)]);
    let admit = |store: &mut Store, offered: &QuotaCandidateSet| {
        store.admit_provider(
            &request, &effective, &catalog, &authority, offered, &identity, 8, None, None,
        )
    };
    let mut malformed = reference(0.0, None);
    malformed.source_epoch = "private-value".into();
    let mut unfenced = reference(0.0, None);
    unfenced.requires_clock_fence = false;
    // Fixed extreme timestamps exercise both clock branches without sleeps or
    // changing the host clock; the writer must preserve their distinct causes.
    for (decision, expected) in [
        (
            reference(0.0, Some(0.0)),
            QuotaAdmissionError::QuotaDecisionStale,
        ),
        (
            reference(f64::MAX, None),
            QuotaAdmissionError::QuotaClockRollback,
        ),
        (malformed, QuotaAdmissionError::QuotaDecisionInvalid),
        (unfenced, QuotaAdmissionError::QuotaDecisionInvalid),
    ] {
        offered.quota_decision = Some(decision);
        let error = admit(&mut store, &offered).unwrap_err();
        assert!(matches!(
            &error,
            Error::QuotaAdmission(actual) if actual == &expected
        ));
        assert_eq!(error.public().message, expected.kind());
        let rejection = agent_run_domain::storage::StorageRejection::from_error(
            &error,
            agent_run_domain::storage::RejectStage::Statement,
        );
        let round: agent_run_domain::storage::StorageRejection =
            serde_json::from_value(serde_json::to_value(rejection).unwrap()).unwrap();
        assert!(matches!(
            round.into_error(),
            Error::QuotaAdmission(actual) if actual == expected
        ));
        assert_eq!(store.quota_capacity_revision().unwrap(), revision);
        for table in ["agents", "attempts", "events", "attempt_quota_keys"] {
            let count: i64 = store
                .conn
                .query_row(&format!("SELECT COUNT(*) FROM {table}"), [], |row| {
                    row.get(0)
                })
                .unwrap();
            assert_eq!(count, 0, "{table}");
        }
    }
    offered.capacity_revision = revision - 1;
    assert!(matches!(
        admit(&mut store, &offered),
        Err(Error::QuotaAdmission(
            QuotaAdmissionError::SelectionStale { .. }
        ))
    ));
    offered.capacity_revision = revision;
    let valid = reference(0.0, Some(f64::MAX));
    offered.quota_decision = Some(valid.clone());
    let accepted = admit(&mut store, &offered).unwrap();
    assert!(accepted.created);
    assert_audit(&event(&store, &accepted.agent_id, "start_accepted"), &valid);
    let adapter: String = store
        .conn
        .query_row("SELECT adapter_state_json FROM attempts", [], |row| {
            row.get(0)
        })
        .unwrap();
    assert_eq!(adapter, "{}");
    offered.quota_decision = Some(reference(0.0, Some(0.0)));
    let replay = admit(&mut store, &offered).unwrap();
    assert!(!replay.created);
    assert_eq!(replay.agent_id, accepted.agent_id);
    assert_eq!(store.quota_capacity_revision().unwrap(), revision + 1);
    assert_audit(&event(&store, &accepted.agent_id, "start_accepted"), &valid);
}

/// Failover rejects stale evidence without releasing the previous ownership;
/// successful allocation records the reference in its existing event only.
#[test]
fn quota_decision_failover_preserves_ownership_and_audit() {
    let home = home();
    let (mut store, id) = cleaned_run(home.path(), "quota-decision-next", true);
    let catalog = catalog(&store);
    let revision = store.quota_capacity_revision().unwrap();
    let before = ownership(&store, &id);
    let mut offered = candidates(revision, &[("acct-b", 0)]);
    for (decision, expected) in [
        (
            reference(0.0, Some(0.0)),
            QuotaAdmissionError::QuotaDecisionStale,
        ),
        (
            reference(f64::MAX, None),
            QuotaAdmissionError::QuotaClockRollback,
        ),
    ] {
        offered.quota_decision = Some(decision);
        assert!(matches!(
            store.allocate_next_attempt(&id, &catalog, &offered),
            Err(Error::QuotaAdmission(actual)) if actual == expected
        ));
        assert_eq!(ownership(&store, &id), before);
        assert_eq!(store.quota_capacity_revision().unwrap(), revision);
    }
    let valid = reference(0.0, None);
    offered.quota_decision = Some(valid.clone());
    let next = store
        .allocate_next_attempt(&id, &catalog, &offered)
        .unwrap();
    assert_eq!(next.account_id.as_str(), "acct-b");
    assert_eq!(ownership(&store, &id), (1, 0, 2));
    assert_audit(&event(&store, &id, "attempt_allocated"), &valid);
}

/// A stale second member rolls back the first member and batch receipt; after
/// acceptance the same receipt replays before either member's expired decision.
#[test]
fn quota_decision_batch_rolls_back_and_replays_before_guard() {
    use agent_run_domain::{batch::StartBatchRequest, domain::AgentId};
    use agent_run_store::{
        batch_admission::{BatchAdmission, BatchMember},
        provider_admission::AdmissionInputs,
    };
    let home = home();
    let mut store = agent_run_store::fixture::open(home.path()).unwrap();
    let catalog = catalog(&store);
    let authority = authority(&catalog, home.path());
    let mut first = provider_request(home.path(), "unused");
    first.request_id = None;
    let mut second = first.clone();
    second.task = "second".into();
    let requests = [first, second];
    let effective = requests
        .each_ref()
        .map(ProviderStartRequest::storage_projection);
    let identities = requests
        .each_ref()
        .map(|request| identity(request, &authority));
    let batch = StartBatchRequest {
        request_id: "quota-decision-batch".into(),
        jobs: requests.to_vec(),
        retain_history: true,
        expected_store_id: None,
        preconditions: None,
    };
    let fingerprint = batch.fingerprint().unwrap();
    let ids = [AgentId::new(), AgentId::new()];
    let revision = store.quota_capacity_revision().unwrap();
    let mut offered = [
        candidates(revision, &[("acct-a", 0)]),
        candidates(revision, &[("acct-b", 0)]),
    ];
    offered[0].quota_decision = Some(reference(0.0, Some(f64::MAX)));
    offered[1].quota_decision = Some(reference(0.0, Some(0.0)));
    let admit = |store: &mut Store, offered: &[QuotaCandidateSet; 2]| {
        store.admit_batch(BatchAdmission {
            request_id: &batch.request_id,
            fingerprint: &fingerprint,
            retain_history: true,
            expected_store_id: None,
            preconditions: None,
            catalog: &catalog,
            jobs: (0..2)
                .map(|index| BatchMember {
                    id: ids[index].clone(),
                    inputs: AdmissionInputs {
                        request: &requests[index],
                        effective: &effective[index],
                        authority: &authority,
                        candidates: &offered[index],
                        identity: &identities[index],
                        global_cap: 8,
                        harness_cap: None,
                        pinned: None,
                    },
                })
                .collect(),
        })
    };
    assert!(matches!(
        admit(&mut store, &offered),
        Err(Error::QuotaAdmission(
            QuotaAdmissionError::QuotaDecisionStale
        ))
    ));
    for table in [
        "agents",
        "attempts",
        "events",
        "start_batch_receipts",
        "history_pins",
    ] {
        let count: i64 = store
            .conn
            .query_row(&format!("SELECT COUNT(*) FROM {table}"), [], |row| {
                row.get(0)
            })
            .unwrap();
        assert_eq!(count, 0, "{table}");
    }
    assert_eq!(store.quota_capacity_revision().unwrap(), revision);
    offered[1].quota_decision = offered[0].quota_decision.clone();
    admit(&mut store, &offered).unwrap();
    for set in &mut offered {
        set.quota_decision = Some(reference(0.0, Some(0.0)));
    }
    admit(&mut store, &offered).unwrap();
    assert_eq!(store.quota_capacity_revision().unwrap(), revision + 1);
    let events: i64 = store
        .conn
        .query_row(
            "SELECT COUNT(*) FROM events WHERE kind='start_accepted'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(events, 2);
}
