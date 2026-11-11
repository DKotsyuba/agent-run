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
use serde::Serialize;
use std::collections::{BTreeMap, BTreeSet};

/// Locally committed eligibility facts and the admission revision that fences them.
struct LocalSnapshot {
    /// The committed local capacity revision.
    revision: i64,
    /// Current enabled status straight from the registry rows.
    enabled: BTreeMap<AccountId, bool>,
    /// Separate provider/account cooldowns, never quota observations.
    cooldowns: BTreeMap<(String, AccountId), (BindingCooldownReason, f64)>,
}

impl LocalSnapshot {
    /// A binding is excluded strictly before its recorded expiry.
    fn cooldown(
        &self,
        provider: &ProviderId,
        account: &AccountId,
        at: f64,
    ) -> Option<(BindingCooldownReason, f64)> {
        self.cooldowns
            .get(&(provider.as_str().to_owned(), account.clone()))
            .filter(|(_, until)| *until > at)
            .map(|(reason, until)| (*reason, *until))
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
            "SELECT provider_id,reason,blocked_until FROM binding_cooldowns WHERE account_id=?",
        )?;
        for row in statement.query_map([account.as_str()], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, f64>(2)?,
            ))
        })? {
            let (provider, reason, until) = row?;
            if !until.is_finite() || until < 0.0 {
                return Err(invalid("binding cooldown is malformed"));
            }
            let reason = serde_json::from_value(serde_json::json!(reason))
                .map_err(|_| invalid("binding cooldown is malformed"))?;
            cooldowns.insert((provider, account.clone()), (reason, until));
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
    },
    /// No fresh known evidence on this lane; a valid fallback, never a zero.
    Unknown,
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

/// Builds candidates from already obtained quota standings and local state only.
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
) -> Result<QuotaCandidateSet> {
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

/// Applies local eligibility, cooldown and ranking rules to supplied standings.
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
) -> Result<QuotaCandidateSet> {
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
    let blocked: BTreeMap<_, _> = usable
        .keys()
        .filter_map(|account| {
            local
                .cooldown(provider, account, at)
                .map(|decision| (account.clone(), decision))
        })
        .collect();
    usable.retain(|account, _| !blocked.contains_key(account));
    if let Some(account) = &pinned
        && let Some((reason, until)) = blocked.get(account)
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
    let mut exhausted: Vec<(AccountId, Option<f64>)> = Vec::new();
    let mut overflow = false;
    for (account, verdict, keys) in &ordering {
        let factor = accounts[account];
        match verdict {
            LaneVerdict::Known { priority, .. } => {
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
                });
            }
            LaneVerdict::Unknown => unknown.push((account.clone(), factor, keys.clone())),
            LaneVerdict::Exhausted { reset_at, .. } => exhausted.push((account.clone(), *reset_at)),
            LaneVerdict::Overflow => overflow = true,
        }
    }
    if !unknown.is_empty() {
        if previous_priority.is_some() {
            rank += 1;
        }
        for (account, factor, keys) in unknown {
            candidates.push(QuotaCandidate {
                account,
                rank,
                physical_keys: keys.into_iter().collect(),
                multiplier: factor.try_into()?,
                quota_known: false,
            });
        }
    }
    if candidates.is_empty() {
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
        if let (Some((account, Some(reset))), Some((_, (_, until)))) =
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
        if let Some((account, (reason, until))) = soonest_block {
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
    Ok(set)
}

/// Reads local eligibility once and projects already prepared quota standings.
#[allow(clippy::too_many_arguments)] // Retains the existing filter and both source projections.
pub(super) fn provider_order_prepared_at(
    store: &Store,
    catalog: &ProviderCatalog,
    hard_ineligible: &BTreeSet<AccountId>,
    keep: &dyn Fn(&ProviderDefinition, &ProviderModel) -> bool,
    at: f64,
    score: impl FnMut(
        &BTreeMap<AccountId, f64>,
        &ProviderDefinition,
        &ProviderModel,
        f64,
    ) -> Result<Vec<LaneStanding>>,
    newest: impl FnMut(
        &AccountId,
        &BTreeSet<PhysicalQuotaKey>,
        &ProviderDefinition,
        &ProviderModel,
    ) -> Option<f64>,
) -> Result<(ProviderCapacityOrder, bool)> {
    if !at.is_finite() || at < 0.0 {
        return Err(invalid("ranking clock must be finite and nonnegative"));
    }
    let local = read_local_snapshot(&store.conn, &account_scope(catalog))?;
    provider_order_inner(&local, catalog, hard_ineligible, keep, at, score, newest)
}

/// Every bound account belongs to the single local projection scope.
fn account_scope(catalog: &ProviderCatalog) -> BTreeSet<AccountId> {
    catalog
        .providers()
        .iter()
        .flat_map(|provider| {
            provider
                .bindings
                .iter()
                .map(|binding| binding.account.clone())
        })
        .collect()
}

/// Applies existing local provider ordering to one already read eligibility snapshot.
#[allow(clippy::too_many_arguments)] // Applies one local snapshot and the supplied standings.
fn provider_order_inner(
    local: &LocalSnapshot,
    catalog: &ProviderCatalog,
    hard_ineligible: &BTreeSet<AccountId>,
    keep: &dyn Fn(&ProviderDefinition, &ProviderModel) -> bool,
    at: f64,
    mut score: impl FnMut(
        &BTreeMap<AccountId, f64>,
        &ProviderDefinition,
        &ProviderModel,
        f64,
    ) -> Result<Vec<LaneStanding>>,
    mut newest: impl FnMut(
        &AccountId,
        &BTreeSet<PhysicalQuotaKey>,
        &ProviderDefinition,
        &ProviderModel,
    ) -> Option<f64>,
) -> Result<(ProviderCapacityOrder, bool)> {
    let mut providers: Vec<ProviderOrderEntry> = Vec::new();
    let mut incomplete = false;
    for definition in catalog.providers() {
        let mut models: Vec<ProviderModelOrder> = Vec::new();
        let mut available: Vec<f64> = Vec::new();
        for offering in definition
            .models
            .iter()
            .filter(|offering| keep(definition, offering))
        {
            let mut usable = current_eligible(
                local,
                catalog,
                definition,
                &eligible_accounts(definition, &offering.id),
                hard_ineligible,
            );
            usable.retain(|account, _| local.cooldown(&definition.id, account, at).is_none());
            // Ordered binding multipliers choose fallback order, not quota weight.
            if definition.ordered_accounts {
                for multiplier in usable.values_mut() {
                    *multiplier = 1.0;
                }
            }
            let ordering = score(&usable, definition, offering, at)?;
            let mut best_priority: Option<f64> = None;
            let mut status_rank: u8 = 4;
            for (_, verdict, _) in &ordering {
                match verdict {
                    LaneVerdict::Known { priority, .. } => {
                        if best_priority.is_none_or(|best| *priority > best) {
                            best_priority = Some(*priority);
                        }
                        status_rank = 0;
                    }
                    LaneVerdict::Unknown => {
                        incomplete = true;
                        status_rank = status_rank.min(1);
                    }
                    LaneVerdict::Overflow => {
                        incomplete = true;
                        status_rank = status_rank.min(2);
                    }
                    LaneVerdict::Exhausted { .. } => status_rank = status_rank.min(3),
                }
            }
            let status = match status_rank {
                0 => "available",
                1 => "unknown",
                2 => "priority_overflow",
                3 => "exhausted",
                _ => "no_eligible_account",
            };
            if let Some(priority) = best_priority {
                available.push(priority);
            }
            // Sample age and exhaustion horizon from the same snapshot,
            // aggregated over accounts so no identity is exposed.
            let newest_observed_at = ordering
                .iter()
                .filter_map(|(account, _, keys)| newest(account, keys, definition, offering))
                .fold(None, |newest: Option<f64>, at| {
                    Some(newest.map_or(at, |n| n.max(at)))
                });
            let evidence = if best_priority.is_some() || status_rank == 3 {
                "fresh"
            } else if newest_observed_at.is_some() {
                "stale"
            } else {
                "missing"
            };
            let exhausted_until = (status_rank == 3)
                .then(|| {
                    ordering
                        .iter()
                        .filter_map(|(_, verdict, _)| match verdict {
                            LaneVerdict::Exhausted { reset_at } => *reset_at,
                            _ => None,
                        })
                        .fold(None, |min: Option<f64>, at| {
                            Some(min.map_or(at, |m| m.min(at)))
                        })
                })
                .flatten();
            models.push(ProviderModelOrder {
                model: offering.id.clone(),
                native_model: offering.native_model.clone(),
                status: status.to_owned(),
                best_priority,
                evidence: evidence.to_owned(),
                newest_observed_at,
                exhausted_until,
            });
        }
        if models.is_empty() {
            continue;
        }
        let best = available.into_iter().fold(f64::NEG_INFINITY, f64::max);
        // The multiplier is any positive finite value, so the product itself
        // can overflow; it saturates at the largest finite score so the
        // order and its JSON stay representable.
        let score = best
            .is_finite()
            .then(|| (best * definition.priority_multiplier.get()).min(f64::MAX));
        providers.push(ProviderOrderEntry {
            provider: definition.id.clone(),
            priority_multiplier: definition.priority_multiplier.get(),
            score,
            models,
        });
    }
    let status_rank = |entry: &ProviderOrderEntry| {
        entry
            .models
            .iter()
            .map(|model| match model.status.as_str() {
                "available" => 0,
                "unknown" => 1,
                "priority_overflow" => 2,
                "exhausted" => 3,
                _ => 4,
            })
            .min()
            .unwrap_or(4)
    };
    providers.sort_by(|a, b| {
        let a_score = a.score.unwrap_or(f64::NEG_INFINITY);
        let b_score = b.score.unwrap_or(f64::NEG_INFINITY);
        b_score
            .partial_cmp(&a_score)
            .expect("provider scores are finite or the negative infinity sentinel")
            .then_with(|| status_rank(a).cmp(&status_rank(b)))
            .then_with(|| a.provider.as_str().cmp(b.provider.as_str()))
    });
    Ok((
        ProviderCapacityOrder {
            observed_at: at,
            capacity_revision: local.revision,
            providers,
        },
        incomplete,
    ))
}

/// The provider-only capacity order over the whole catalog.
#[derive(Debug, Clone, Serialize)]
pub struct ProviderCapacityOrder {
    /// The ranking timestamp every projection used.
    pub observed_at: f64,
    /// The committed capacity revision the whole order was read against.
    pub capacity_revision: i64,
    /// Providers in descending score order; the stable provider name breaks
    /// ties.
    pub providers: Vec<ProviderOrderEntry>,
}

/// One provider's public capacity standing; never a provider/account pair.
#[derive(Debug, Clone, Serialize)]
pub struct ProviderOrderEntry {
    /// The configured provider.
    pub provider: ProviderId,
    /// The provider's positive finite priority multiplier.
    pub priority_multiplier: f64,
    /// The best available model priority times the provider multiplier;
    /// `None` when no model has known usable capacity.
    pub score: Option<f64>,
    /// Each offered model's own availability and score.
    pub models: Vec<ProviderModelOrder>,
}

/// One model's own availability inside the provider order.
#[derive(Debug, Clone, Serialize)]
pub struct ProviderModelOrder {
    /// The provider-visible model id.
    pub model: String,
    /// The explicit native model alias, when configured.
    pub native_model: Option<String>,
    /// One of `available`, `unknown`, `priority_overflow`, `exhausted`, or
    /// `no_eligible_account`.
    pub status: String,
    /// The best known account priority (`score * multiplier`) for this
    /// model; `None` when no account has known usable capacity.
    pub best_priority: Option<f64>,
    /// Evidence behind `status`: `fresh` (a current sample or an active
    /// exhaustion fact decided it), `stale` (samples exist but none is
    /// current), or `missing` (never observed for any eligible account).
    pub evidence: String,
    /// Newest sample observation time (Unix seconds) on this model's lane
    /// across eligible accounts, fresh or not; `None` when never observed.
    pub newest_observed_at: Option<f64>,
    /// When `status` is `exhausted`: the earliest known reset among the
    /// exhausted accounts; `None` when unknown or not exhausted.
    pub exhausted_until: Option<f64>,
}
