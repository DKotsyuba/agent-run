//! Read-only account/provider ranking over committed quota facts.
//!
//! This producer turns one consistent store snapshot plus a frozen provider
//! catalog into the real [`QuotaCandidateSet`] transactional admission
//! consumes, and into a provider-only order view with per-model
//! availability. It performs no provider calls, reservations, admissions,
//! credential reads, model choices, or writes; its only side effect is a
//! deferred read transaction on the caller's store connection. All scoring
//! math is delegated to [`ranking::rank_capacity_routes`] through
//! [`super::forecast`]; nothing here reimplements burn, slack, exhaustion,
//! or multiplier semantics. Account-bound reset credits have no committed
//! fact source yet, so the ranker's credit bonus stays exactly one until
//! one exists.
//!
//! Quota facts belong to the global [`AccountId`] plus the physical quota
//! key (`account::lane`), never a provider alias: several labels or
//! providers bound to one account deduplicate to one physical candidate per
//! request using the applicable maximum factor, never a sum. Only rows with
//! an explicit `account_id`/`quota_key` identity are read; legacy
//! runtime/target rows with NULL identities never become current account
//! quota. Missing, stale, failed, or malformed evidence is unknown capacity
//! (a valid fallback that never fabricates a pool or an amount), never a
//! known zero or fresh full capacity; an authoritative fresh zero or an
//! active durable latch is exhaustion no multiplier can revive.
//!
//! The session owns replay-first admission and its bounded (<=3) stale-retry
//! loop. Outside any transaction it refreshes candidates by calling
//! [`provider_candidates`] `(&Store, &ProviderCatalog, &ProviderId, &str,
//! Option<&str> pin label, &BTreeSet<AccountId> hard-ineligible)` again and
//! resubmitting; this helper never admits, reserves, or replays.

use super::{forecast, ranking, Key, Pool, Route, Sample};
use crate::{domain::now, error::invalid, state::Store, Result};
use agent_run_domain::{
    catalog::{
        AccountId, AccountStatus, PhysicalQuotaKey, ProviderCatalog, ProviderDefinition,
        ProviderModel, QuotaAdmissionError, QuotaCandidate, QuotaCandidateSet, SelectionIntent,
    },
    ProviderId,
};
use rusqlite::{Connection, OptionalExtension};
use serde::Serialize;
use std::collections::{BTreeMap, BTreeSet};

/// One account's committed quota facts inside one consistent snapshot.
#[derive(Debug, Clone, Default)]
struct AccountFacts {
    /// Newest-first sample history per physical key, source, and window.
    /// Collector runtime aliases do not split one physical history.
    history: BTreeMap<PhysicalQuotaKey, BTreeMap<Key, Vec<Sample>>>,
    /// Durable exhaustion latches per physical key: each entry is one latched
    /// window's source, name and reset instant, with `None` meaning "until a
    /// later observation supersedes it". Latches survive sample retention.
    latches: BTreeMap<PhysicalQuotaKey, Vec<(String, String, Option<f64>)>>,
    /// The model lanes each physical window `(key, source, window)` governs,
    /// from that window's newest sample (`payload_json.models`, written by
    /// `record_quota_snapshot` from the normalized snapshot) paired with the
    /// sample's `(observed_at, id)` order. The source is part of the identity:
    /// the same window name from another source (a carried latch after a
    /// source switch) keeps its own members. A window whose newest sample
    /// records no membership has `None`.
    membership: BTreeMap<(PhysicalQuotaKey, String, String), WindowMembership>,
}

/// One physical window's newest recorded membership: the `(observed_at, id)`
/// order of the sample it came from and its model lanes (`None` when that
/// sample recorded none).
type WindowMembership = ((f64, i64), Option<BTreeSet<String>>);

impl AccountFacts {
    /// Whether window `window` observed by `source` on pool `key` governs the
    /// model `lane` (its native alias).
    ///
    /// The window's newest recorded membership must name the lane exactly: a
    /// pool id is never read as a model name, so `primary`, `secondary` or
    /// `model:<x>` pools count only through membership, and an older sample's
    /// membership never authorizes a model the newest one dropped. Only a
    /// window with no recorded membership at all (rows written before
    /// membership was recorded, or test rows without it) falls back to the
    /// pool id equalling the lane exactly.
    fn governs(
        &self,
        account: &AccountId,
        key: &PhysicalQuotaKey,
        source: &str,
        window: &str,
        lane: &str,
    ) -> bool {
        match self
            .membership
            .get(&(key.clone(), source.to_owned(), window.to_owned()))
        {
            Some((_, Some(models))) => models.contains(lane),
            _ => {
                key.as_str()
                    .strip_prefix(&format!("{}::", account.as_str()))
                    == Some(lane)
            }
        }
    }

    /// Returns every physical pool of this account with at least one window
    /// (sampled or latched) that governs the model `lane`; nothing is
    /// fabricated for unobserved lanes.
    fn lane_keys(&self, account: &AccountId, lane: &str) -> BTreeSet<PhysicalQuotaKey> {
        let sampled = self.history.iter().filter(|(key, windows)| {
            windows.keys().any(|identity| {
                self.governs(account, key, &identity.source, &identity.window, lane)
            })
        });
        let latched = self.latches.iter().filter(|(key, latches)| {
            latches
                .iter()
                .any(|(source, window, _)| self.governs(account, key, source, window, lane))
        });
        sampled
            .map(|(key, _)| key.clone())
            .chain(latched.map(|(key, _)| key.clone()))
            .collect()
    }

    /// Whether an active durable latch on a window of `key` that governs
    /// `lane` blocks it at epoch `at`: a `None` or future reset keeps the
    /// restriction, while a passed reset releases it without inventing fresh
    /// positive capacity.
    fn latched(&self, account: &AccountId, key: &PhysicalQuotaKey, lane: &str, at: f64) -> bool {
        self.latch_reset(account, key, lane, at).is_some()
    }

    /// Returns the earliest still-active reset among `key`'s latched windows
    /// that govern `lane`, preferring finite resets over open-ended ones;
    /// `None` when nothing applicable is latched.
    fn latch_reset(
        &self,
        account: &AccountId,
        key: &PhysicalQuotaKey,
        lane: &str,
        at: f64,
    ) -> Option<Option<f64>> {
        self.latches
            .get(key)?
            .iter()
            .filter(|(source, window, reset)| {
                self.governs(account, key, source, window, lane)
                    && reset.is_none_or(|reset| reset > at)
            })
            .map(|(_, _, reset)| *reset)
            .min_by(|a, b| {
                (a.is_none(), a.unwrap_or(f64::INFINITY))
                    .partial_cmp(&(b.is_none(), b.unwrap_or(f64::INFINITY)))
                    .expect("latch resets compare finite against infinity")
            })
    }
}

/// One consistent read snapshot of every quota fact ranking consumes.
struct QuotaSnapshot {
    /// The committed capacity revision every score is computed against.
    revision: i64,
    /// Current enabled status straight from the registry rows.
    enabled: BTreeMap<AccountId, bool>,
    /// Per-account samples and durable latches.
    facts: BTreeMap<AccountId, AccountFacts>,
}

/// Reads registry status, account-bound sample history, durable latches, and
/// the committed capacity revision from ONE deferred read transaction.
///
/// Legacy rows whose `account_id`/`quota_key` identity is NULL are never
/// selected, so historical runtime/target evidence cannot become current
/// account quota. The transaction never writes; dropping it is a no-op.
/// `scope` bounds the read, and ids absent from the registry simply carry no
/// facts and a disabled status. History is grouped by physical key, source,
/// and window, with merged alias rows ordered newest first by observation
/// time and row id.
fn read_snapshot(conn: &Connection, scope: &BTreeSet<AccountId>) -> Result<QuotaSnapshot> {
    // Join a caller's open read transaction (one snapshot for registry and
    // quota reads together); otherwise open our own deferred one.
    let _own = if conn.is_autocommit() {
        Some(conn.unchecked_transaction()?)
    } else {
        None
    };
    let tx = conn;
    let mut enabled = BTreeMap::new();
    let mut facts: BTreeMap<AccountId, AccountFacts> = BTreeMap::new();
    for account in scope {
        let status: Option<String> = tx
            .query_row(
                "SELECT status FROM provider_accounts WHERE account_id=?",
                [account.as_str()],
                |row| row.get(0),
            )
            .optional()?;
        enabled.insert(account.clone(), status.as_deref() == Some("enabled"));
        facts.insert(account.clone(), AccountFacts::default());
    }
    for account in scope {
        let prefix = format!("{}::", account.as_str());
        let mut statement = tx.prepare(
            "SELECT quota_key,runtime,lane,window,target,source,remaining_percent,reset_at,observed_at,valid_until,payload_json,id \
             FROM capacity_samples WHERE account_id=? \
             ORDER BY quota_key,source,window,observed_at DESC,id DESC",
        )?;
        let rows = statement.query_map([account.as_str()], |row| {
            Ok((
                row.get::<_, String>(0)?,
                (row.get::<_, Option<String>>(10)?, row.get::<_, i64>(11)?),
                Sample {
                    key: Key {
                        runtime: row.get(1)?,
                        lane: row.get(2)?,
                        window: row.get(3)?,
                        target: row.get(4)?,
                        source: row.get(5)?,
                    },
                    remaining_percent: row.get(6)?,
                    reset_at: row.get(7)?,
                    observed_at: row.get(8)?,
                    valid_until: row.get(9)?,
                },
            ))
        })?;
        for row in rows {
            let (text, (payload, id), mut sample) = row?;
            let lane = text.strip_prefix(&prefix).ok_or_else(|| {
                invalid("capacity sample quota key does not belong to its account")
            })?;
            let key = PhysicalQuotaKey::new(account, lane)?;
            // `runtime`, `lane`, and `target` are legacy collector labels;
            // physical identity is the validated quota key plus source/window.
            sample.key.runtime = account.as_str().to_owned();
            sample.key.lane = lane.to_owned();
            sample.key.target = None;
            let account_facts = facts
                .get_mut(account)
                .expect("scope accounts are pre-seeded");
            // The newest sample of a pool decides its current membership.
            let order = (sample.observed_at.unwrap_or(f64::NEG_INFINITY), id);
            let models = payload
                .as_deref()
                .and_then(|text| serde_json::from_str::<serde_json::Value>(text).ok())
                .and_then(|value| {
                    value["models"].as_array().map(|models| {
                        models
                            .iter()
                            .filter_map(|model| model.as_str().map(str::to_owned))
                            .collect::<BTreeSet<_>>()
                    })
                });
            let window = (
                key.clone(),
                sample.key.source.clone(),
                sample.key.window.clone(),
            );
            let newest = account_facts
                .membership
                .get(&window)
                .is_none_or(|(seen, _)| order > *seen);
            if newest {
                account_facts.membership.insert(window, (order, models));
            }
            account_facts
                .history
                .entry(key)
                .or_default()
                .entry(sample.key.clone())
                .or_default()
                .push(sample);
        }
        let mut statement = tx.prepare(
            "SELECT quota_key,source,window_id,reset_at FROM quota_exhaustion WHERE account_id=?",
        )?;
        let rows = statement.query_map([account.as_str()], |row| {
            Ok((
                row.get::<_, String>(0)?,
                (
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                    row.get::<_, Option<f64>>(3)?,
                ),
            ))
        })?;
        for row in rows {
            let (text, latch) = row?;
            let lane = text
                .strip_prefix(&prefix)
                .ok_or_else(|| invalid("quota exhaustion key does not belong to its account"))?;
            let key = PhysicalQuotaKey::new(account, lane)?;
            facts
                .get_mut(account)
                .expect("scope accounts are pre-seeded")
                .latches
                .entry(key)
                .or_default()
                .push(latch);
        }
    }
    let revision = Store::quota_capacity_revision_in(tx)?;
    Ok(QuotaSnapshot {
        revision,
        enabled,
        facts,
    })
}

/// One account's ranked standing on one requested model lane.
#[derive(Debug, Clone)]
enum LaneVerdict {
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

/// Scores one model lane for every account in `accounts` (global id to its
/// effective multiplier) using the shared ranker, at epoch `at`.
///
/// Accounts latched on the lane are excluded before ranking so the durable
/// latch stays authoritative over any sample state. The rest flow through
/// [`ranking::rank_capacity_routes`] as one synthetic route per account
/// (runtime = the global account id) whose forecasts come from the same
/// [`forecast`] history math the capacity order uses; the ranker's own
/// `omitted` output is the fresh authoritative zero and its `deferred`
/// reasons split unknown evidence from priority overflow. The returned list
/// keeps known accounts first in the ranker's deterministic order and all
/// remaining verdicts in stable account-id order, independent of row order.
fn score_lane(
    facts: &BTreeMap<AccountId, AccountFacts>,
    accounts: &BTreeMap<AccountId, f64>,
    lane: &str,
    at: f64,
) -> Result<Vec<(AccountId, LaneVerdict, BTreeSet<PhysicalQuotaKey>)>> {
    let mut verdicts: BTreeMap<AccountId, LaneVerdict> = BTreeMap::new();
    let mut keys_by_account: BTreeMap<AccountId, BTreeSet<PhysicalQuotaKey>> = BTreeMap::new();
    let mut inputs: Vec<ranking::RouteInput> = Vec::new();
    let mut multipliers: BTreeMap<String, f64> = BTreeMap::new();
    for (account, factor) in accounts {
        let empty = AccountFacts::default();
        let account_facts = facts.get(account).unwrap_or(&empty);
        let keys = account_facts.lane_keys(account, lane);
        if let Some(key) = keys
            .iter()
            .find(|key| account_facts.latched(account, key, lane, at))
        {
            verdicts.insert(
                account.clone(),
                LaneVerdict::Exhausted {
                    reset_at: account_facts.latch_reset(account, key, lane, at).flatten(),
                },
            );
            keys_by_account.insert(account.clone(), keys);
            continue;
        }
        multipliers.insert(account.as_str().to_owned(), *factor);
        let mut forecasts = Vec::new();
        let mut pools = Vec::new();
        for key in &keys {
            let mut pool_keys = BTreeSet::new();
            if let Some(windows) = account_facts.history.get(key) {
                // Only windows that govern this model lane enter its score.
                for (identity, history) in windows.iter().filter(|(identity, _)| {
                    account_facts.governs(account, key, &identity.source, &identity.window, lane)
                }) {
                    pool_keys.insert(identity.clone());
                    forecasts.push(forecast(identity, history, at));
                }
            }
            pools.push(Pool {
                pool_id: key.as_str().to_owned(),
                keys: pool_keys,
            });
        }
        inputs.push(ranking::RouteInput {
            descriptor: Route {
                route_id: account.as_str().to_owned(),
                runtime: account.as_str().to_owned(),
                account: Some(account.as_str().to_owned()),
                quota_lane: lane.to_owned(),
                pool_ids: keys.iter().map(|key| key.as_str().to_owned()).collect(),
                reset_credits: None,
            },
            pools,
            forecasts,
        });
        keys_by_account.insert(account.clone(), keys);
    }
    let order =
        ranking::rank_capacity_routes(inputs, Vec::new(), &multipliers, &BTreeMap::new(), at)?;
    let mut known: Vec<(AccountId, LaneVerdict, BTreeSet<PhysicalQuotaKey>)> = Vec::new();
    for route in order.routes {
        let account: AccountId = route.runtime.parse()?;
        let keys = keys_by_account.get(&account).cloned().unwrap_or_default();
        known.push((
            account,
            LaneVerdict::Known {
                priority: route.priority,
            },
            keys,
        ));
    }
    for omitted in order.omitted {
        let account: AccountId = omitted.runtime.parse()?;
        verdicts.insert(
            account,
            LaneVerdict::Exhausted {
                reset_at: omitted.limiting_reset_at,
            },
        );
    }
    for deferred in order.deferred {
        let text = deferred
            .route_id
            .as_deref()
            .unwrap_or(deferred.runtime.as_str());
        let account: AccountId = text
            .parse()
            .map_err(|_| invalid("ranker deferred evidence lacks a valid account"))?;
        let verdict = if deferred.reason == "priority_overflow" {
            LaneVerdict::Overflow
        } else {
            LaneVerdict::Unknown
        };
        verdicts.insert(account, verdict);
    }
    let mut ordered: Vec<(AccountId, LaneVerdict, BTreeSet<PhysicalQuotaKey>)> = known;
    for (account, verdict) in verdicts {
        let keys = keys_by_account.get(&account).cloned().unwrap_or_default();
        ordered.push((account, verdict, keys));
    }
    Ok(ordered)
}

/// The physical lane a provider model maps to: its explicit native model
/// alias when present, otherwise its own id.
///
/// This mirrors the adapter launch convention exactly. Lane membership is
/// per `(provider, model)` and never merged across aliases: two providers
/// may map the same model id to different native lanes without silently
/// sharing either.
fn model_lane(offering: &ProviderModel) -> &str {
    offering.native_model.as_deref().unwrap_or(&offering.id)
}

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
    snapshot: &QuotaSnapshot,
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

/// Builds the real candidate set for one configured provider and explicit
/// model, at the current ranking clock.
///
/// `pin` is the provider-local account label of a pinned request; it
/// resolves to exactly one global account and never falls over to another.
/// `hard_ineligible` is the narrow typed input for known auth or model
/// ineligibility evidence; capability is never inferred from prose or model
/// names. See [`provider_candidates_at`] for the full contract.
pub fn provider_candidates(
    store: &Store,
    catalog: &ProviderCatalog,
    provider: &ProviderId,
    model: &str,
    pin: Option<&str>,
    hard_ineligible: &BTreeSet<AccountId>,
) -> Result<QuotaCandidateSet> {
    provider_candidates_at(store, catalog, provider, model, pin, hard_ineligible, now())
}

/// Builds the real candidate set for one configured provider and explicit
/// model against one consistent read snapshot taken at epoch `at`.
///
/// Reads only the store (registry status, account-bound sample history,
/// durable latches, capacity revision) inside one deferred transaction and
/// writes nothing. Known usable candidates precede unknown fallbacks
/// regardless of weights; exhausted, disabled, and hard-ineligible accounts
/// are never revived by any multiplier. Rank groups follow the shared
/// ranker's priority order with only exactly equal priorities sharing a
/// rank; ties inside a group are left to admission's active-count/id
/// tie-break. The returned set carries the same committed
/// `capacity_revision` the scores were computed against.
///
/// Errors distinguish invalid input (unknown provider, unoffered model,
/// unbound pin label, or priority overflow),
/// [`QuotaAdmissionError::NoEligibleAccount`] when nothing is currently
/// eligible, and [`QuotaAdmissionError::QuotaExhausted`] when eligible
/// accounts exist but exhaustion removed them all; generic collection
/// failure never appears here because no collector runs.
pub fn provider_candidates_at(
    store: &Store,
    catalog: &ProviderCatalog,
    provider: &ProviderId,
    model: &str,
    pin: Option<&str>,
    hard_ineligible: &BTreeSet<AccountId>,
    at: f64,
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
    let lane = model_lane(offering).to_owned();
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
    let snapshot = read_snapshot(&store.conn, &scope)?;
    let usable = current_eligible(
        &snapshot,
        catalog,
        definition,
        &eligible_accounts(definition, model),
        hard_ineligible,
    );
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
    let ordering = score_lane(&snapshot.facts, &accounts, &lane, at)?;
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
        if let Some((account, _)) = exhausted.iter().min_by(|a, b| {
            (a.1.is_none(), a.1.unwrap_or(f64::INFINITY), &a.0)
                .partial_cmp(&(b.1.is_none(), b.1.unwrap_or(f64::INFINITY), &b.0))
                .expect("exhaustion resets compare finite against infinity")
        }) {
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
    let set = QuotaCandidateSet {
        provider: provider.clone(),
        model: model.to_owned(),
        intent,
        candidates,
        capacity_revision: snapshot.revision,
    };
    set.validate()?;
    Ok(set)
}

/// Provider-only capacity order with per-model availability, at the current
/// ranking clock. See [`provider_order_at`] for the full contract.
pub fn provider_order(
    store: &Store,
    catalog: &ProviderCatalog,
    hard_ineligible: &BTreeSet<AccountId>,
) -> Result<ProviderCapacityOrder> {
    provider_order_at(store, catalog, hard_ineligible, now())
}

/// Builds the provider-only order view against one consistent read snapshot
/// taken at epoch `at`.
///
/// The public order contains providers, never provider/account pairs. Each
/// offered model exposes its own availability and best account priority
/// computed from its own lane, so a best model never implies another
/// exhausted model works. The provider score is the maximum available model
/// priority times the provider's positive finite priority multiplier;
/// providers sort by descending score with the stable provider name breaking
/// ties, and providers with only unknown capacity outrank exhausted ones.
pub fn provider_order_at(
    store: &Store,
    catalog: &ProviderCatalog,
    hard_ineligible: &BTreeSet<AccountId>,
    at: f64,
) -> Result<ProviderCapacityOrder> {
    provider_order_filtered_at(store, catalog, hard_ineligible, &|_, _| true, at)
}

/// [`provider_order_at`] over only the offerings `keep` retains.
///
/// `keep(provider, offering)` decides which offerings exist for this view
/// (an exact model filter, a role's admissible offerings, ...). Scores,
/// statuses and the provider order are computed from the retained
/// offerings only, under the same formula and ordering, so a removed
/// offering never lends its standing to its provider; providers with no
/// retained offering are omitted. When the caller holds an open read
/// transaction on `store`, the snapshot is read inside it.
pub fn provider_order_filtered_at(
    store: &Store,
    catalog: &ProviderCatalog,
    hard_ineligible: &BTreeSet<AccountId>,
    keep: &dyn Fn(&ProviderDefinition, &ProviderModel) -> bool,
    at: f64,
) -> Result<ProviderCapacityOrder> {
    if !at.is_finite() || at < 0.0 {
        return Err(invalid("ranking clock must be finite and nonnegative"));
    }
    let mut scope: BTreeSet<AccountId> = BTreeSet::new();
    for provider in catalog.providers() {
        for binding in &provider.bindings {
            scope.insert(binding.account.clone());
        }
    }
    let snapshot = read_snapshot(&store.conn, &scope)?;
    let mut providers: Vec<ProviderOrderEntry> = Vec::new();
    for definition in catalog.providers() {
        let mut models: Vec<ProviderModelOrder> = Vec::new();
        let mut available: Vec<f64> = Vec::new();
        for offering in definition
            .models
            .iter()
            .filter(|offering| keep(definition, offering))
        {
            let usable = current_eligible(
                &snapshot,
                catalog,
                definition,
                &eligible_accounts(definition, &offering.id),
                hard_ineligible,
            );
            let ordering = score_lane(&snapshot.facts, &usable, model_lane(offering), at)?;
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
                    LaneVerdict::Unknown => status_rank = status_rank.min(1),
                    LaneVerdict::Overflow => status_rank = status_rank.min(2),
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
                .flat_map(|(account, _, keys)| {
                    let history = snapshot.facts.get(account).map(|facts| &facts.history);
                    keys.iter()
                        .filter_map(move |key| history.and_then(|history| history.get(key)))
                        .flat_map(|windows| windows.values().flatten())
                        .filter_map(|sample| sample.observed_at)
                })
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
    Ok(ProviderCapacityOrder {
        observed_at: at,
        capacity_revision: snapshot.revision,
        providers,
    })
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
