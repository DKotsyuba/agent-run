//! Adapts validated Quota snapshots to Run's existing local account selection.
//!
//! This module does not collect, forecast, reserve, or admit. Each decision
//! starts with a fresh snapshot and refetches after local selection conflicts.

use super::provider_ranking::{self, LaneStanding, LaneVerdict};
use crate::{Result, error::invalid, quota_client, state::Store};
use agent_run_domain::{
    ProviderId,
    catalog::{
        AccountId, PhysicalQuotaKey, ProviderCatalog, ProviderModel, QuotaCandidateSet,
        QuotaDecision,
    },
};
use serde::Serialize;
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
};

/// Bounded, nonsecret identity of the Quota response used for one decision.
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct QuotaReference {
    pub source_epoch: String,
    pub revision: Option<i64>,
    pub config_revision: String,
    /// The service's evaluation clock in Unix seconds, not an admission fence.
    pub evaluated_at: f64,
}

/// Local candidate set and the independent Quota response identity, if available.
pub struct CandidateOutput {
    /// Its capacity revision is the Run store's local revision, not Quota's revision.
    pub candidates: QuotaCandidateSet,
    /// No reference means quota was unavailable and its standings are unknown.
    pub quota_reference: Option<QuotaReference>,
}

/// Earliest source-class boundary affecting one local decision.
#[derive(Default)]
pub(super) struct StateFence {
    requires_clock_fence: bool,
    valid_until: Option<f64>,
}

/// Fetches one bounded read-only snapshot; any transport or protocol failure means unknown.
pub async fn fetch(
    endpoint: Option<&crate::config::QuotaEndpoint>,
) -> Option<quota_client::Snapshot> {
    let endpoint = endpoint?;
    quota_client::snapshot(&endpoint.socket, endpoint.expected_service_uid, &[], None)
        .await
        .ok()
}

/// Owns snapshots for one decision and bounds local revalidation after quota expiry.
/// It must not be retained across independent admissions or failovers.
#[derive(Default)]
pub(crate) struct DecisionSnapshots {
    last: Option<Arc<quota_client::Snapshot>>,
    revalidate: bool,
    rechecks: u32,
    rollback_fallback: bool,
}

impl DecisionSnapshots {
    /// Fetches for a new attempt, or reuses only this decision's last snapshot
    /// after the external retry budget has been exhausted.
    pub(crate) async fn read(
        &mut self,
        endpoint: Option<&crate::config::QuotaEndpoint>,
    ) -> Option<Arc<quota_client::Snapshot>> {
        if !self.revalidate {
            self.last = fetch(endpoint).await.map(Arc::new);
        }
        self.last.clone()
    }

    /// Requests another bounded attempt after an expired class decision.
    /// Initial retries refetch; later retries revalidate the last snapshot.
    pub(crate) fn retry_expired(&mut self, submission: &mut u32, retries: u32) -> bool {
        if !self.revalidate && *submission < retries {
            *submission += 1;
            return true;
        }
        if !self.revalidate {
            self.revalidate = true;
            return true;
        }
        if self.rechecks < retries {
            self.rechecks += 1;
            return true;
        }
        false
    }

    /// Discards a snapshot after a clock rollback or invalid internal reference.
    /// A fresh fetch is retried while budget remains, then unknown is used once.
    pub(crate) fn retry_invalidated(&mut self, submission: &mut u32, retries: u32) -> bool {
        self.last = None;
        self.revalidate = false;
        self.rechecks = 0;
        if *submission < retries {
            *submission += 1;
            return true;
        }
        if !self.rollback_fallback {
            self.revalidate = true;
            self.rollback_fallback = true;
            return true;
        }
        false
    }

    /// A new local selection or roster retry must fetch a new source snapshot.
    pub(crate) fn require_fresh(&mut self) {
        self.revalidate = false;
        self.rechecks = 0;
        self.rollback_fallback = false;
    }
}

/// Builds one candidate set from a validated snapshot and current local eligibility.
///
/// `None` means Quota was unavailable; it never means zero or unlimited quota.
/// An expired class or a clock before `evaluated_at` is unknown; exact reported
/// pool identities remain available for Run's local reservation tie-break.
/// For a batch, obtain a single snapshot first, then call with that same value
/// inside one short caller-owned local read transaction. On a local stale retry,
/// obtain a new snapshot: its source revision does not freeze time or admission.
/// After bounded expiry retries, revalidation may reuse only the last snapshot
/// within this same decision, downgrading precisely the classes that have expired.
/// A no-reset latch applies only to this newly obtained snapshot; this API does
/// not establish a maximum age or enforce the caller's no-reuse obligation.
/// Run's writer remains responsible for rechecking local state and reservations.
#[allow(clippy::too_many_arguments)] // Mirrors the existing producer plus its external snapshot.
pub fn candidates_at(
    store: &Store,
    catalog: &ProviderCatalog,
    provider: &ProviderId,
    model: &str,
    pin: Option<&str>,
    hard_ineligible: &BTreeSet<AccountId>,
    snapshot: Option<&quota_client::Snapshot>,
    at: f64,
) -> Result<CandidateOutput> {
    let mut fence = StateFence::default();
    let mut candidates = provider_ranking::provider_candidates_prepared_at(
        store,
        catalog,
        provider,
        model,
        pin,
        hard_ineligible,
        at,
        |accounts, offering, clock| {
            score(
                accounts,
                provider,
                offering,
                snapshot,
                clock,
                &mut fence,
                Some(32),
            )
        },
    )?;
    let quota_reference = snapshot.map(|snapshot| QuotaReference {
        source_epoch: snapshot.source_epoch.clone(),
        revision: snapshot.revision,
        config_revision: snapshot.config_revision.clone(),
        evaluated_at: snapshot.evaluated_at,
    });
    candidates.quota_decision = quota_reference.as_ref().map(|reference| QuotaDecision {
        source_epoch: reference.source_epoch.clone(),
        revision: reference.revision,
        config_revision: reference.config_revision.clone(),
        evaluated_at: reference.evaluated_at,
        valid_until: fence.valid_until,
        requires_clock_fence: fence.requires_clock_fence,
    });
    Ok(CandidateOutput {
        candidates,
        quota_reference,
    })
}

/// Maps exact account/provider/model identities to Quota's reported state and score.
pub(super) fn score(
    accounts: &BTreeMap<AccountId, f64>,
    provider: &ProviderId,
    offering: &ProviderModel,
    snapshot: Option<&quota_client::Snapshot>,
    now: f64,
    fence: &mut StateFence,
    max_pools: Option<usize>,
) -> Result<Vec<LaneStanding>> {
    let clock_valid = snapshot.is_some_and(|s| {
        s.evaluated_at.is_finite() && s.evaluated_at >= 0.0 && now >= s.evaluated_at
    });
    let mut known = Vec::new();
    let mut other = Vec::new();
    for (account, factor) in accounts {
        let model = matching_model(snapshot, account, provider, offering);
        let Some(model) = model else {
            other.push((account.clone(), LaneVerdict::Unknown, BTreeSet::new()));
            continue;
        };
        let state_valid = clock_valid
            && match (model.state, model.state_validity) {
                (
                    quota_client::ModelState::Known | quota_client::ModelState::Exhausted,
                    quota_client::StateValidity::Until { at },
                ) => at.is_finite() && at >= 0.0 && now < at,
                (
                    quota_client::ModelState::Exhausted,
                    quota_client::StateValidity::LatchedNoReset,
                ) => true,
                _ => false,
            };
        if state_valid {
            fence.requires_clock_fence = true;
            if let quota_client::StateValidity::Until { at } = model.state_validity {
                fence.valid_until = Some(fence.valid_until.map_or(at, |previous| previous.min(at)));
            }
        }
        if model.state == quota_client::ModelState::Exhausted && state_valid {
            other.push((
                account.clone(),
                LaneVerdict::Exhausted {
                    reset_at: model.exhausted_until,
                },
                BTreeSet::new(),
            ));
            continue;
        }
        let keys = physical_keys(account, &model.pools, max_pools)?;
        if !state_valid {
            other.push((account.clone(), LaneVerdict::Unknown, keys));
            continue;
        }
        match model.state {
            quota_client::ModelState::Known => {
                let raw = model
                    .quota_score
                    .filter(|n| n.is_finite() && *n >= 0.0)
                    .ok_or_else(|| invalid("quota candidate score is invalid"))?;
                if keys.is_empty() {
                    return Err(invalid("known quota candidate has no physical pools"));
                }
                let priority = raw * factor;
                if priority.is_finite() {
                    known.push((account.clone(), LaneVerdict::Known { priority }, keys));
                } else {
                    other.push((account.clone(), LaneVerdict::Overflow, keys));
                }
            }
            quota_client::ModelState::Unknown => {
                other.push((account.clone(), LaneVerdict::Unknown, keys))
            }
            quota_client::ModelState::Exhausted => unreachable!("exhausted state is handled above"),
        }
    }
    known.sort_by(|a, b| {
        let LaneVerdict::Known { priority: left } = &a.1 else {
            unreachable!()
        };
        let LaneVerdict::Known { priority: right } = &b.1 else {
            unreachable!()
        };
        right.total_cmp(left).then_with(|| a.0.cmp(&b.0))
    });
    known.extend(other);
    Ok(known)
}

/// Finds one exact account/provider/model/native binding without alias inference.
pub(super) fn matching_model<'a>(
    snapshot: Option<&'a quota_client::Snapshot>,
    account: &AccountId,
    provider: &ProviderId,
    offering: &ProviderModel,
) -> Option<&'a quota_client::Model> {
    let native = offering.native_model.as_deref().unwrap_or(&offering.id);
    snapshot?
        .accounts
        .iter()
        .find(|view| view.account == account.as_str())?
        .models
        .iter()
        .find(|model| {
            model.provider == provider.as_str()
                && model.model == offering.id
                && model.native_model == native
        })
}

/// Converts every reported pool without truncation or inference of missing pools.
fn physical_keys(
    account: &AccountId,
    pools: &[String],
    max: Option<usize>,
) -> Result<BTreeSet<PhysicalQuotaKey>> {
    if max.is_some_and(|limit| pools.len() > limit) {
        return Err(invalid("quota candidate exceeds the physical pool limit"));
    }
    let mut keys = BTreeSet::new();
    for pool in pools {
        let lane = pool
            .strip_prefix(account.as_str())
            .and_then(|rest| rest.strip_prefix("::"))
            .ok_or_else(|| invalid("quota pool identity is invalid"))?;
        let key = PhysicalQuotaKey::new(account, lane)?;
        if !keys.insert(key) {
            return Err(invalid("quota pool identity is repeated"));
        }
    }
    Ok(keys)
}

#[cfg(test)]
mod tests {
    use super::DecisionSnapshots;

    /// Repeated expiry permits only bounded source and local retries.
    #[test]
    fn quota_decision_expiry_retries_are_bounded() {
        let mut decisions = DecisionSnapshots::default();
        let mut submission = 0;
        for expected in 1..=3 {
            assert!(decisions.retry_expired(&mut submission, 3));
            assert_eq!(submission, expected);
        }
        assert!(decisions.retry_expired(&mut submission, 3));
        for _ in 0..3 {
            assert!(decisions.retry_expired(&mut submission, 3));
        }
        assert!(!decisions.retry_expired(&mut submission, 3));
        decisions.require_fresh();
        assert!(decisions.retry_expired(&mut submission, 3));
    }

    /// Restoring the wall clock cannot restore a snapshot invalidated by rollback.
    #[tokio::test]
    async fn quota_decision_clock_rollback_discard_is_sticky() {
        let raw: serde_json::Value = serde_json::from_str(include_str!(
            "../../tests/fixtures/quota-client-v1/exhausted-snapshot-v2.json"
        ))
        .unwrap();
        let mut snapshot: crate::quota_client::Snapshot =
            serde_json::from_value(raw["result"].clone()).unwrap();
        snapshot.accounts[0].models[0].state_validity =
            crate::quota_client::StateValidity::LatchedNoReset;
        let reference = agent_run_domain::catalog::QuotaDecision {
            source_epoch: snapshot.source_epoch.clone(),
            revision: snapshot.revision,
            config_revision: snapshot.config_revision.clone(),
            evaluated_at: snapshot.evaluated_at,
            valid_until: None,
            requires_clock_fence: true,
        };
        assert!(matches!(
            reference.check_admission(10000.0),
            Err(agent_run_domain::Error::QuotaAdmission(
                agent_run_domain::catalog::QuotaAdmissionError::QuotaClockRollback
            ))
        ));
        let mut decisions = DecisionSnapshots {
            last: Some(std::sync::Arc::new(snapshot)),
            ..DecisionSnapshots::default()
        };
        let mut submission = 3;
        assert!(decisions.retry_invalidated(&mut submission, 3));
        assert!(reference.check_admission(10002.0).is_ok());
        assert!(decisions.read(None).await.is_none());
        assert!(!decisions.retry_invalidated(&mut submission, 3));
    }
}
