//! External quota freshness stays separate from Run's local revision fence.

use agent_run_domain::{
    Error,
    catalog::{QuotaAdmissionError, QuotaCandidateSet, QuotaDecision},
    error::MachineCode,
    storage::{RejectStage, StorageRejection},
};
use serde_json::json;

/// Supplies a bounded reference evaluated at a deterministic Unix timestamp.
fn decision() -> QuotaDecision {
    QuotaDecision {
        source_epoch: "a".repeat(32),
        revision: Some(700),
        config_revision: "b".repeat(64),
        evaluated_at: 10.0,
        valid_until: Some(20.0),
        requires_clock_fence: true,
    }
}

/// Evaluation time is inclusive and validity expiry is exclusive, without sleeps.
#[test]
fn quota_decision_clock_boundaries_and_no_reset_latch() {
    let mut value = decision();
    for at in [10.0, 15.0, 19.999] {
        value.check_admission(at).unwrap();
    }
    for at in [20.0, 21.0] {
        assert!(matches!(
            value.check_admission(at),
            Err(Error::QuotaAdmission(
                QuotaAdmissionError::QuotaDecisionStale
            ))
        ));
    }
    for at in [9.999, f64::NAN, f64::INFINITY] {
        assert!(matches!(
            value.check_admission(at),
            Err(Error::QuotaAdmission(
                QuotaAdmissionError::QuotaClockRollback
            ))
        ));
    }
    value.valid_until = None;
    value.check_admission(1000.0).unwrap();
    assert!(matches!(
        value.check_admission(9.0),
        Err(Error::QuotaAdmission(
            QuotaAdmissionError::QuotaClockRollback
        ))
    ));
    value.requires_clock_fence = false;
    // Unknown-only references carry audit identity, not authoritative time state.
    value.check_admission(9.0).unwrap();
    value.valid_until = Some(20.0);
    assert!(matches!(
        value.check_admission(15.0),
        Err(Error::QuotaAdmission(
            QuotaAdmissionError::QuotaDecisionInvalid
        ))
    ));
}

/// Bounded identities, revisions and timestamps are checked at admission even
/// when a trusted caller built the DTO directly instead of deserializing it.
#[test]
fn quota_decision_rejects_malformed_reference_fields() {
    let mut variants = vec![];
    let mut value = decision();
    value.source_epoch = "A".repeat(32);
    variants.push(value);
    let mut value = decision();
    value.source_epoch.push('a');
    variants.push(value);
    let mut value = decision();
    value.config_revision = "z".repeat(64);
    variants.push(value);
    let mut value = decision();
    value.config_revision.pop();
    variants.push(value);
    let mut value = decision();
    value.revision = Some(-1);
    variants.push(value);
    for time in [-1.0, f64::NAN, f64::INFINITY] {
        let mut value = decision();
        value.evaluated_at = time;
        variants.push(value);
        let mut value = decision();
        value.valid_until = Some(time);
        variants.push(value);
    }
    for value in variants {
        for at in [9.0, 15.0, 25.0] {
            assert!(matches!(
                value.check_admission(at),
                Err(Error::QuotaAdmission(
                    QuotaAdmissionError::QuotaDecisionInvalid
                ))
            ));
        }
    }
    let mut value = decision();
    value.revision = None;
    value.check_admission(15.0).unwrap();
}

/// Legacy candidate wire payloads omit the reference; new ones round-trip it
/// without confusing the independent local and source revision namespaces.
#[test]
fn quota_decision_candidate_wire_compatibility() {
    let legacy = json!({
        "provider":"fixture", "model":"fixture", "intent":"auto",
        "candidates":[], "capacity_revision":3,
    });
    let mut candidates: QuotaCandidateSet = serde_json::from_value(legacy.clone()).unwrap();
    assert!(candidates.quota_decision.is_none());
    assert_eq!(serde_json::to_value(&candidates).unwrap(), legacy);
    candidates.quota_decision = Some(decision());
    candidates.validate().unwrap();
    let round: QuotaCandidateSet =
        serde_json::from_value(serde_json::to_value(&candidates).unwrap()).unwrap();
    assert_eq!(round, candidates);
    assert_eq!(round.capacity_revision, 3);
    assert_eq!(round.quota_decision.unwrap().revision, Some(700));
}

/// Known quota cannot disable its clock fence through a contradictory source
/// reference; legacy and unknown-only sets keep their distinct contracts.
#[test]
fn quota_decision_known_candidates_require_clock_fence() {
    let mut candidates: QuotaCandidateSet = serde_json::from_value(json!({
        "provider":"fixture", "model":"fixture", "intent":"auto",
        "candidates":[{
            "account":"acct-a", "rank":0, "physical_keys":["acct-a::tokens"],
            "multiplier":1.0, "quota_known":true,
        }],
        "capacity_revision":3,
    }))
    .unwrap();
    candidates.validate().unwrap();
    candidates.check_quota_decision(9.0).unwrap();
    let mut reference = decision();
    reference.requires_clock_fence = false;
    reference.valid_until = None;
    candidates.quota_decision = Some(reference);
    assert!(matches!(
        candidates.check_quota_decision(15.0),
        Err(Error::QuotaAdmission(
            QuotaAdmissionError::QuotaDecisionInvalid
        ))
    ));
    candidates.candidates[0].quota_known = false;
    candidates.check_quota_decision(9.0).unwrap();
    candidates.candidates[0].quota_known = true;
    candidates.quota_decision = Some(decision());
    candidates.check_quota_decision(15.0).unwrap();
    assert!(candidates.check_quota_decision(9.0).is_err());
}

/// Writer rejection serialization preserves whether a snapshot must be discarded
/// or merely re-evaluated, without changing the public machine code.
#[test]
fn quota_decision_causes_cross_writer_boundary() {
    for cause in [
        QuotaAdmissionError::QuotaDecisionStale,
        QuotaAdmissionError::QuotaClockRollback,
        QuotaAdmissionError::QuotaDecisionInvalid,
    ] {
        let error = Error::QuotaAdmission(cause.clone());
        let refusal = StorageRejection::from_error(&error, RejectStage::Statement);
        let wire = serde_json::to_value(&refusal).unwrap();
        let round: StorageRejection = serde_json::from_value(wire).unwrap();
        let restored = round.into_error();
        assert!(matches!(
            &restored,
            Error::QuotaAdmission(actual) if actual == &cause
        ));
        assert_eq!(restored.machine_code(), MachineCode::SelectionStale);
        assert_eq!(restored.public().message, cause.kind());
    }
}

/// A rollback remains distinguishable from expiry even when both predicates
/// hold; its serialized cause survives subsequent recovery of the test clock.
#[test]
fn quota_decision_rollback_cause_survives_clock_recovery() {
    let mut value = decision();
    let rollback = value.check_admission(9.0).unwrap_err();
    let wire = serde_json::to_value(StorageRejection::from_error(
        &rollback,
        RejectStage::Statement,
    ))
    .unwrap();
    value.check_admission(15.0).unwrap();
    let recovered: StorageRejection = serde_json::from_value(wire).unwrap();
    assert!(matches!(
        recovered.into_error(),
        Error::QuotaAdmission(QuotaAdmissionError::QuotaClockRollback)
    ));
    value.valid_until = Some(5.0);
    assert!(matches!(
        value.check_admission(9.0),
        Err(Error::QuotaAdmission(
            QuotaAdmissionError::QuotaClockRollback
        ))
    ));
    assert!(matches!(
        value.check_admission(15.0),
        Err(Error::QuotaAdmission(
            QuotaAdmissionError::QuotaDecisionStale
        ))
    ));
}
