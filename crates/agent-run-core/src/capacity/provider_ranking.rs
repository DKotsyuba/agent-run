//! Applies Run-local eligibility, weights, cooldowns and ordering to Quota standings.
//!
//! Quota owns source facts and numerical scores. These helpers consume supplied
//! standings and one local registry snapshot; they never read local quota history.

use crate::{Result, error::invalid, state::Store};
use agent_run_domain::{
    ProviderId,
    catalog::{
        AccountId, AccountStatus, BindingCooldownReason, PhysicalQuotaKey, ProviderCatalog,
        ProviderDefinition, ProviderModel, QuotaAdmissionError, QuotaCandidate, QuotaCandidateSet,
        SelectionIntent,
    },
};
use rusqlite::{Connection, OptionalExtension};
use std::collections::{BTreeMap, BTreeSet};

/// Locally committed eligibility facts and the admission revision that fences them.
struct LocalSnapshot {
    /// The committed local capacity revision.
    revision: i64,
    /// Current enabled status straight from the registry rows.
    enabled: BTreeMap<AccountId, bool>,
    /// Separate provider/account cooldowns, never quota observations.
    cooldowns: BTreeMap<(String, AccountId), (BindingCooldownReason, f64, f64)>,
}

impl LocalSnapshot {
    /// A binding is excluded strictly before its recorded expiry.
    fn cooldown(
        &self,
        provider: &ProviderId,
        account: &AccountId,
        at: f64,
    ) -> Option<(BindingCooldownReason, f64, f64)> {
        self.cooldowns
            .get(&(provider.as_str().to_owned(), account.clone()))
            .filter(|(_, until, _)| *until > at)
            .copied()
    }
}

/// Reads only local registry, cooldowns and revision from one committed snapshot.
fn read_local_snapshot(conn: &Connection, scope: &BTreeSet<AccountId>) -> Result<LocalSnapshot> {
    let _own = if conn.is_autocommit() {
        Some(conn.unchecked_transaction()?)
    } else {
        None
    };
    let mut enabled = BTreeMap::new();
    let mut cooldowns = BTreeMap::new();
    for account in scope {
        let status: Option<String> = conn
            .query_row(
                "SELECT status FROM provider_accounts WHERE account_id=?",
                [account.as_str()],
                |row| row.get(0),
            )
            .optional()?;
        enabled.insert(account.clone(), status.as_deref() == Some("enabled"));
        let mut statement = conn.prepare(
            "SELECT provider_id,reason,blocked_until,observed_at FROM binding_cooldowns WHERE account_id=?",
        )?;
        for row in statement.query_map([account.as_str()], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, f64>(2)?,
                row.get::<_, f64>(3)?,
            ))
        })? {
            let (provider, reason, until, observed) = row?;
            if !observed.is_finite() || observed < 0.0 || !until.is_finite() || until <= observed {
                return Err(invalid("binding cooldown is malformed"));
            }
            let reason = serde_json::from_value(serde_json::json!(reason))
                .map_err(|_| invalid("binding cooldown is malformed"))?;
            cooldowns.insert((provider, account.clone()), (reason, until, observed));
        }
    }
    Ok(LocalSnapshot {
        revision: Store::quota_capacity_revision_in(conn)?,
        enabled,
        cooldowns,
    })
}

/// One account's ranked standing on one requested model lane.
#[derive(Debug, Clone)]
pub(super) enum LaneVerdict {
    /// Known usable capacity carrying the shared ranker's ordering priority
    /// (`governing-window score * effective account multiplier`).
    Known {
        /// The account's ordering priority.
        priority: f64,
        /// Fresh observation of usable plan capacity for this lane.
        recovery_observed_at: Option<f64>,
    },
    /// No fresh known evidence on this lane; a valid fallback, never a zero.
    Unknown,
    /// The plan window is exhausted, and Codex reports paid credits available.
    CreditBacked,
    /// An authoritative fresh zero or an active durable latch blocks the
    /// lane for this model; no multiplier can revive it. Carries when the
    /// limiting restriction lifts, when known.
    Exhausted { reset_at: Option<f64> },
    /// `score * multiplier` overflowed a finite number; excluded from every
    /// order rather than sorted as an infinity.
    Overflow,
}

/// One account's quota standing and its exact physical pools.
pub(super) type LaneStanding = (AccountId, LaneVerdict, BTreeSet<PhysicalQuotaKey>);

/// Returns the provider's model-eligible accounts with each account's
/// effective multiplier: the maximum over its eligible labels, never a sum.
///
/// Labels whose explicit model subset excludes `model` contribute nothing,
/// so contradictory memberships under several labels of one account never
/// merge.
fn eligible_accounts(definition: &ProviderDefinition, model: &str) -> BTreeMap<AccountId, f64> {
    let mut eligible: BTreeMap<AccountId, f64> = BTreeMap::new();
    for binding in &definition.bindings {
        if binding
            .models
            .as_ref()
            .is_none_or(|models| models.iter().any(|id| id == model))
        {
            eligible
                .entry(binding.account.clone())
                .and_modify(|value| *value = (*value).max(binding.multiplier.get()))
                .or_insert(binding.multiplier.get());
        }
    }
    eligible
}

/// Narrows label-eligible accounts to those currently registered, enabled in
/// the snapshot's registry rows, provider-family compatible, and absent from
/// the caller's hard-ineligible set.
fn current_eligible(
    snapshot: &LocalSnapshot,
    catalog: &ProviderCatalog,
    definition: &ProviderDefinition,
    eligible: &BTreeMap<AccountId, f64>,
    hard_ineligible: &BTreeSet<AccountId>,
) -> BTreeMap<AccountId, f64> {
    eligible
        .iter()
        .filter(|(account, _)| {
            !hard_ineligible.contains(account)
                && snapshot.enabled.get(account).copied().unwrap_or(false)
                && catalog.account(account).is_some_and(|record| {
                    record.status == AccountStatus::Enabled
                        && record.auth_family == definition.auth_family
                })
        })
        .map(|(account, factor)| (account.clone(), *factor))
        .collect()
}

/// Builds candidates and identifies credit-backed accounts using supplied standings and local state.
/// The caller may hold one read transaction across multiple calls to share a local revision.
#[allow(clippy::too_many_arguments)] // Matches the existing scoped producer plus its score source.
pub(super) fn provider_candidates_prepared_at(
    store: &Store,
    catalog: &ProviderCatalog,
    provider: &ProviderId,
    model: &str,
    pin: Option<&str>,
    hard_ineligible: &BTreeSet<AccountId>,
    at: f64,
    score: impl FnOnce(&BTreeMap<AccountId, f64>, &ProviderModel, f64) -> Result<Vec<LaneStanding>>,
) -> Result<(QuotaCandidateSet, BTreeSet<AccountId>)> {
    provider_candidates_inner(
        store,
        catalog,
        provider,
        model,
        pin,
        hard_ineligible,
        at,
        |conn, scope| Ok((read_local_snapshot(conn, scope)?, ())),
        |_, scoring, offering, clock| score(scoring, offering, clock),
    )
}

/// Applies eligibility, cooldown and ranking rules and returns the credit-backed accounts.
#[allow(clippy::too_many_arguments)] // Carries the existing selection contract and two read seams.
fn provider_candidates_inner<T>(
    store: &Store,
    catalog: &ProviderCatalog,
    provider: &ProviderId,
    model: &str,
    pin: Option<&str>,
    hard_ineligible: &BTreeSet<AccountId>,
    at: f64,
    load: impl FnOnce(&Connection, &BTreeSet<AccountId>) -> Result<(LocalSnapshot, T)>,
    score: impl FnOnce(&T, &BTreeMap<AccountId, f64>, &ProviderModel, f64) -> Result<Vec<LaneStanding>>,
) -> Result<(QuotaCandidateSet, BTreeSet<AccountId>)> {
    if model.trim().is_empty() {
        return Err(invalid("model must be nonblank"));
    }
    if !at.is_finite() || at < 0.0 {
        return Err(invalid("ranking clock must be finite and nonnegative"));
    }
    let definition = catalog
        .provider(provider)
        .ok_or_else(|| invalid("provider is not configured"))?;
    let offering = definition
        .models
        .iter()
        .find(|offering| offering.id == model)
        .ok_or_else(|| invalid("model is not offered by provider"))?;
    let pinned = match pin {
        Some(label) => {
            let binding = definition
                .binding(label)
                .ok_or_else(|| invalid("account label is not bound to provider"))?;
            Some(binding.account.clone())
        }
        None => None,
    };
    let mut scope: BTreeSet<AccountId> = definition
        .bindings
        .iter()
        .map(|binding| binding.account.clone())
        .collect();
    if let Some(account) = &pinned {
        scope.insert(account.clone());
    }
    let (local, facts) = load(&store.conn, &scope)?;
    let mut usable = current_eligible(
        &local,
        catalog,
        definition,
        &eligible_accounts(definition, model),
        hard_ineligible,
    );
    let mut blocked: BTreeMap<_, _> = usable
        .keys()
        .filter_map(|account| {
            local
                .cooldown(provider, account, at)
                .map(|decision| (account.clone(), decision))
        })
        .collect();
    usable.retain(|account, _| {
        blocked
            .get(account)
            .is_none_or(|(reason, _, _)| *reason == BindingCooldownReason::QuotaExhausted)
    });
    if let Some(account) = &pinned
        && let Some((reason, until, _)) = blocked.get(account)
        && !usable.contains_key(account)
    {
        return Err(QuotaAdmissionError::BindingCooldown {
            provider: provider.clone(),
            model: model.to_owned(),
            account: account.clone(),
            reason: *reason,
            retry_after_seconds: ((*until - at).ceil() as u64).max(1),
        }
        .into());
    }
    let (intent, accounts) = match &pinned {
        Some(account) => {
            let Some(factor) = usable.get(account) else {
                return Err(QuotaAdmissionError::NoEligibleAccount {
                    provider: provider.clone(),
                    model: model.to_owned(),
                }
                .into());
            };
            (
                SelectionIntent::Pinned(account.clone()),
                BTreeMap::from([(account.clone(), *factor)]),
            )
        }
        None => (SelectionIntent::Auto, usable),
    };
    // Ordered weights express sequence, not quota-score multipliers. Use
    // neutral weights to classify capacity without overflowing a score and
    // exclude only the same exhausted/unavailable evidence as usual.
    let neutral: BTreeMap<_, _> = if definition.ordered_accounts {
        accounts
            .keys()
            .cloned()
            .map(|account| (account, 1.0))
            .collect()
    } else {
        BTreeMap::new()
    };
    let scoring = if definition.ordered_accounts {
        &neutral
    } else {
        &accounts
    };
    let ordering = score(&facts, scoring, offering, at)?;
    let mut candidates: Vec<QuotaCandidate> = Vec::new();
    let mut rank: u32 = 0;
    let mut previous_priority: Option<f64> = None;
    let mut unknown: Vec<(AccountId, f64, BTreeSet<PhysicalQuotaKey>)> = Vec::new();
    let mut credits = Vec::new();
    let mut exhausted: Vec<(AccountId, Option<f64>)> = Vec::new();
    let mut overflow = false;
    for (account, verdict, keys) in &ordering {
        let factor = accounts[account];
        if let Some((_, _, observed)) = blocked.get(account) {
            if !matches!(verdict, LaneVerdict::Known { recovery_observed_at: Some(at), .. } if at > observed)
            {
                continue;
            }
            blocked.remove(account);
        }
        match verdict {
            LaneVerdict::Known {
                priority,
                recovery_observed_at,
            } => {
                if previous_priority.is_some_and(|previous| *priority != previous) {
                    rank += 1;
                }
                previous_priority = Some(*priority);
                candidates.push(QuotaCandidate {
                    account: account.clone(),
                    rank,
                    physical_keys: keys.iter().cloned().collect(),
                    multiplier: factor.try_into()?,
                    quota_known: true,
                    recovery_observed_at: *recovery_observed_at,
                });
            }
            LaneVerdict::Unknown => unknown.push((account.clone(), factor, keys.clone())),
            LaneVerdict::CreditBacked => credits.push((account.clone(), factor, keys.clone())),
            LaneVerdict::Exhausted { reset_at, .. } => exhausted.push((account.clone(), *reset_at)),
            LaneVerdict::Overflow => overflow = true,
        }
    }
    let credit_backed: BTreeSet<_> = credits
        .iter()
        .map(|(account, _, _)| account.clone())
        .collect();
    for (group, quota_known) in [(unknown, false), (credits, true)] {
        if group.is_empty() {
            continue;
        }
        if !candidates.is_empty() {
            rank += 1;
        }
        for (account, factor, keys) in group {
            candidates.push(QuotaCandidate {
                account,
                rank,
                physical_keys: keys.into_iter().collect(),
                multiplier: factor.try_into()?,
                quota_known,
                recovery_observed_at: None,
            });
        }
    }
    if candidates.is_empty() {
        if let Some(account) = &pinned
            && let Some((reason, until, _)) = blocked.get(account)
        {
            return Err(QuotaAdmissionError::BindingCooldown {
                provider: provider.clone(),
                model: model.to_owned(),
                account: account.clone(),
                reason: *reason,
                retry_after_seconds: ((*until - at).ceil() as u64).max(1),
            }
            .into());
        }
        if overflow {
            return Err(invalid(
                "provider quota priority overflowed a finite number",
            ));
        }
        let soonest_block = blocked.into_iter().min_by(|a, b| a.1.1.total_cmp(&b.1.1));
        let soonest_quota = exhausted.iter().min_by(|a, b| {
            (a.1.is_none(), a.1.unwrap_or(f64::INFINITY), &a.0)
                .partial_cmp(&(b.1.is_none(), b.1.unwrap_or(f64::INFINITY), &b.0))
                .expect("exhaustion resets compare finite against infinity")
        });
        if let (Some((account, Some(reset))), Some((_, (_, until, _)))) =
            (soonest_quota, &soonest_block)
            && *reset <= *until
        {
            return Err(QuotaAdmissionError::QuotaExhausted {
                provider: provider.clone(),
                model: model.to_owned(),
                account: account.clone(),
            }
            .into());
        }
        if let Some((account, (reason, until, _))) = soonest_block {
            return Err(QuotaAdmissionError::BindingCooldown {
                provider: provider.clone(),
                model: model.to_owned(),
                account,
                reason,
                retry_after_seconds: ((until - at).ceil() as u64).max(1),
            }
            .into());
        }
        if let Some((account, _)) = soonest_quota {
            return Err(QuotaAdmissionError::QuotaExhausted {
                provider: provider.clone(),
                model: model.to_owned(),
                account: account.clone(),
            }
            .into());
        }
        return Err(QuotaAdmissionError::NoEligibleAccount {
            provider: provider.clone(),
            model: model.to_owned(),
        }
        .into());
    }
    if definition.ordered_accounts {
        let order: BTreeMap<_, _> = definition
            .ordered_bindings(model)
            .iter()
            .enumerate()
            .map(|(index, binding)| (binding.account.clone(), index))
            .collect();
        if candidates
            .iter()
            .any(|candidate| !order.contains_key(&candidate.account))
        {
            return Err(invalid(
                "ordered account is outside the provider model scope",
            ));
        }
        candidates.sort_by_key(|candidate| order[&candidate.account]);
        for (index, candidate) in candidates.iter_mut().enumerate() {
            candidate.rank =
                u32::try_from(index).map_err(|_| invalid("too many ordered provider accounts"))?;
        }
    }
    let set = QuotaCandidateSet {
        provider: provider.clone(),
        model: model.to_owned(),
        intent,
        candidates,
        capacity_revision: local.revision,
        quota_decision: None,
    };
    set.validate()?;
    Ok((set, credit_backed))
}
