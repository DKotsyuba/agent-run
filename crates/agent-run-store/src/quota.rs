//! Account-bound quota observation persistence and the durable exhaustion
//! latch.
//!
//! The store owns raw persistence only: every write lands in one immediate
//! transaction that advances the global `quota_capacity_revision` singleton
//! exactly once when it mutates any row, and never scores or ranks anything.
//! Physical identity is explicit (`capacity_samples.account_id` +
//! `quota_key`), and the exhaustion latch survives sample retention in
//! `quota_exhaustion`.

use crate::Store;
use agent_run_domain::{
    catalog::{AccountId, NormalizedQuotaSnapshot, PhysicalQuotaKey, QuotaWindow},
    error::invalid,
    Result,
};
use rusqlite::{params, OptionalExtension, TransactionBehavior};
use std::collections::{BTreeMap, BTreeSet};

/// Shelf life granted to a latched exhausted fact whose provider reset time is
/// unknown; positive fresh evidence is then the only release.
pub const UNRESET_EXHAUSTION_TTL_SECONDS: f64 = 900.0;

/// Returns the provider-independent lane of `key`, which must belong to
/// `account`; the account prefix is never persisted as a lane.
fn lane_of<'a>(key: &'a PhysicalQuotaKey, account: &AccountId) -> Result<&'a str> {
    key.as_str()
        .strip_prefix(account.as_str())
        .and_then(|rest| rest.strip_prefix("::"))
        .filter(|lane| !lane.is_empty())
        .ok_or_else(|| invalid("quota key does not belong to snapshot account"))
}

/// Reads the durable exhaustion latch for one global account.
///
/// Latch rows live in `quota_exhaustion`, keyed by account, physical key,
/// stable collector source, and provider window, so they are independent of
/// rolling sample retention and of script revisions. Matching in
/// [`retain_exhausted`] is by lane and window across sources.
fn latched_windows(
    tx: &rusqlite::Transaction<'_>,
    account: &AccountId,
) -> Result<BTreeMap<(String, String), QuotaWindow>> {
    let mut stmt = tx.prepare(
        "SELECT quota_key,source,window_id,observed_at,reset_at FROM quota_exhaustion WHERE account_id=?1",
    )?;
    let rows = stmt.query_map(params![account.as_str()], |row| {
        Ok((
            row.get::<_, String>(0)?,
            row.get::<_, String>(1)?,
            row.get::<_, String>(2)?,
            row.get::<_, f64>(3)?,
            row.get::<_, Option<f64>>(4)?,
        ))
    })?;
    let mut latched = BTreeMap::new();
    for row in rows {
        let (quota_key, source, window_id, observed_at, reset_at) = row?;
        let lane = quota_key
            .strip_prefix(account.as_str())
            .and_then(|rest| rest.strip_prefix("::"))
            .unwrap_or_default()
            .to_owned();
        if lane.is_empty() {
            continue;
        }
        latched.insert(
            (lane, window_id.clone()),
            QuotaWindow {
                source,
                name: window_id,
                remaining_percent: Some(0.0),
                reset_at,
                observed_at,
                valid_until: reset_at.unwrap_or(observed_at),
            },
        );
    }
    Ok(latched)
}

/// Recorded model membership per latched `(lane, window)`; `None` for a
/// legacy row without a valid `models` array.
type WindowMembership = BTreeMap<(String, String), Option<BTreeSet<String>>>;

/// The model lanes each latched window of `account` governs, keyed by
/// `(lane, window)`: the membership of that window's newest sample in the
/// same `(observed_at, id)` order the ranker reads. `None` when the newest
/// row records no valid `models` array (rows written before membership was
/// recorded); such a window governs only the lane equal to its pool id, the
/// same legacy rule the ranker applies — malformed metadata is never read as
/// a model mapping.
fn latched_membership(
    tx: &rusqlite::Transaction<'_>,
    account: &AccountId,
    latched: &BTreeMap<(String, String), QuotaWindow>,
) -> Result<WindowMembership> {
    let mut membership = BTreeMap::new();
    for (lane, window) in latched.keys() {
        let payload: Option<String> = tx
            .query_row(
                "SELECT payload_json FROM capacity_samples WHERE quota_key=?1 AND window=?2 \
                 ORDER BY observed_at DESC,id DESC LIMIT 1",
                params![format!("{}::{}", account.as_str(), lane), window],
                |row| row.get(0),
            )
            .optional()?
            .flatten();
        let models = payload
            .and_then(|text| serde_json::from_str::<serde_json::Value>(&text).ok())
            .and_then(|value| {
                value["models"].as_array().map(|models| {
                    models
                        .iter()
                        .filter_map(|model| model.as_str().map(str::to_owned))
                        .collect::<BTreeSet<_>>()
                })
            });
        membership.insert((lane.clone(), window.clone()), models);
    }
    Ok(membership)
}

/// Whether latched window `(lane, window)` governs model lane `model`:
/// through its recorded membership, or for a legacy row without membership,
/// only when the pool id equals the model lane.
fn window_governs(membership: &WindowMembership, lane: &str, window: &str, model: &str) -> bool {
    match membership.get(&(lane.to_owned(), window.to_owned())) {
        Some(Some(models)) => models.contains(model),
        _ => lane == model,
    }
}

/// Extends one latched exhausted window's survival horizon without inventing a
/// new observation time: validity reaches the known reset, or one TTL past
/// `at` when the provider reported none.
fn extend_survival(window: &QuotaWindow, at: f64) -> QuotaWindow {
    let mut carried = window.clone();
    carried.valid_until = carried
        .reset_at
        .unwrap_or(at + UNRESET_EXHAUSTION_TTL_SECONDS)
        .max(carried.valid_until);
    carried
}

/// Whether a positive `window` can release `fact` at host time `at`.
fn authoritative_positive(window: &QuotaWindow, fact: &QuotaWindow, at: f64) -> bool {
    window
        .remaining_percent
        .is_some_and(|remaining| remaining > 0.0)
        && window.observed_at >= fact.observed_at
        && window.observed_at <= at
        && window.valid_until >= at
        && window.reset_at.is_none_or(|reset| reset > at)
}

/// Applies the exhaustion latch to `snapshot` in place.
///
/// A latched zero-remaining window survives a round that only produced
/// unknown data for its pool (empty or `None` remaining) until its known
/// reset time passes, or — when reset is absent — until actually fresh
/// positive evidence arrives (`remaining > 0`, observed at least as late as
/// the exhaustion and not later than `at`, validity unexpired, reset not
/// passed). Fresh exhaustion, fresh
/// percentages, and expired resets are never overridden; carried facts keep
/// their original observation times and collector source.
///
/// A latched window is carried only into models it governs according to
/// `membership` (its newest recorded membership, see [`window_governs`]);
/// it is never copied into another model that merely shares the pool, even
/// when the window is omitted or unknown this round. Fresh positive or
/// fresher zero evidence for the physical window seen under any model
/// settles it for every model.
pub fn retain_exhausted(
    exhausted: &BTreeMap<(String, String), QuotaWindow>,
    membership: &WindowMembership,
    snapshot: &mut NormalizedQuotaSnapshot,
    account: &AccountId,
    at: f64,
) -> Result<()> {
    let mut settled = BTreeSet::new();
    for ((l, name), fact) in exhausted {
        let observed = snapshot
            .models
            .iter()
            .flat_map(|model| &model.pools)
            .any(|pool| {
                lane_of(&pool.key, account).ok() == Some(l.as_str())
                    && pool.windows.iter().any(|w| {
                        w.name == *name
                            && (authoritative_positive(w, fact, at)
                                || (w.remaining_percent == Some(0.0)
                                    && w.observed_at > fact.observed_at
                                    && w.observed_at <= at))
                    })
            });
        if observed {
            settled.insert((l.clone(), name.clone()));
        }
    }
    for model in &mut snapshot.models {
        let model_lane = model.model.clone();
        for pool in &mut model.pools {
            let lane = lane_of(&pool.key, account)?.to_owned();
            // Only actually fresh positive evidence releases the latch: a
            // stale or expired positive observation never revives capacity,
            // and unknown or missing data displaces nothing. Matching is by
            // physical window name across collector sources, so a source
            // change neither forks the pool nor strands the fact.
            let mut carried: Vec<QuotaWindow> = Vec::new();
            for ((l, name), fact) in exhausted {
                if l != &lane
                    || fact.reset_at.is_some_and(|reset| reset <= at)
                    || settled.contains(&(l.clone(), name.clone()))
                    || !window_governs(membership, l, name, &model_lane)
                {
                    continue;
                }
                pool.windows.retain(|w| w.name != *name);
                carried.push(extend_survival(fact, at));
            }
            pool.windows.extend(carried);
        }
    }
    Ok(())
}

/// Persists one validated account-bound quota snapshot atomically.
///
/// `home` is the agent-run home whose store receives the write; `runtime`
/// names the engine scope that observed the facts; the global account must
/// be registered and is persisted explicitly in `account_id`/`quota_key`
/// (with `target` kept as a legacy view mirror). One row is written per
/// physical window with its full model membership in `payload_json`,
/// however many configured models share the pool. The durable exhaustion
/// latch is upserted for retained exhausted windows and cleared for released
/// ones. Every mutating round — samples written, latch upserted, or latch
/// cleared — advances `quota_capacity_revision` exactly once inside the same
/// immediate transaction; a round that changes nothing leaves the revision
/// and history untouched. `at` is the host clock the latch evaluates resets
/// against. Returns the committed revision.
pub fn record_quota_snapshot(
    home: &std::path::Path,
    runtime: &str,
    snapshot: &NormalizedQuotaSnapshot,
    retention: usize,
    at: f64,
) -> Result<i64> {
    if runtime.is_empty() || runtime.len() > 512 || retention == 0 {
        return Err(invalid("invalid quota observation persistence"));
    }
    snapshot.validate()?;
    let account = snapshot.account.clone();
    let mut store = Store::open(home)?;
    let tx = store
        .conn
        .transaction_with_behavior(TransactionBehavior::Immediate)?;
    let registered: i64 = tx.query_row(
        "SELECT COUNT(*) FROM provider_accounts WHERE account_id=?1",
        params![account.as_str()],
        |row| row.get(0),
    )?;
    if registered != 1 {
        return Err(invalid("quota account is not registered"));
    }
    let exhausted = latched_windows(&tx, &account)?;
    let mut released = BTreeSet::new();
    for ((lane, name), fact) in &exhausted {
        if fact.reset_at.is_some_and(|reset| reset <= at)
            || snapshot
                .models
                .iter()
                .flat_map(|model| &model.pools)
                .any(|pool| {
                    lane_of(&pool.key, &account).ok() == Some(lane.as_str())
                        && pool.windows.iter().any(|window| {
                            window.name == *name && authoritative_positive(window, fact, at)
                        })
                })
        {
            released.insert((lane.as_str(), name.as_str()));
        }
    }
    let membership = latched_membership(&tx, &account, &exhausted)?;
    let mut snapshot = snapshot.clone();
    retain_exhausted(&exhausted, &membership, &mut snapshot, &account, at)?;

    // One physical window is persisted exactly once with its full model
    // membership, however many configured models share the pool.
    let mut physical: BTreeMap<(&str, &str, &str), &QuotaWindow> = BTreeMap::new();
    for model in &snapshot.models {
        for pool in &model.pools {
            let lane = lane_of(&pool.key, &account)?;
            for window in &pool.windows {
                // Shared pools repeat identical facts across models (the
                // validator proved it); only a latch carried into the models
                // it governs can differ, and that exhausted fact wins.
                let slot = physical
                    .entry((lane, window.source.as_str(), window.name.as_str()))
                    .or_insert(window);
                if window.remaining_percent == Some(0.0) && slot.remaining_percent != Some(0.0) {
                    *slot = window;
                }
            }
        }
    }
    // Membership is recorded per physical window, and only for the models
    // whose own view of that window is exactly the persisted fact: a narrower
    // window never inherits every model of its pool, and a carried exhaustion
    // never extends to a model that reported the window as unknown.
    let mut pool_models: BTreeMap<(&str, &str), BTreeSet<&str>> = BTreeMap::new();
    for model in &snapshot.models {
        for pool in &model.pools {
            let lane = lane_of(&pool.key, &account)?;
            for window in &pool.windows {
                let key = (lane, window.source.as_str(), window.name.as_str());
                if physical.get(&key).is_some_and(|chosen| *chosen == window) {
                    pool_models
                        .entry((lane, window.name.as_str()))
                        .or_default()
                        .insert(model.model.as_str());
                }
            }
        }
    }
    let mut mutations = 0usize;
    for ((lane, _source, _name), window) in &physical {
        window.validate()?;
        let quota_key = format!("{}::{}", account.as_str(), lane);
        let models = pool_models
            .get(&(*lane, window.name.as_str()))
            .map(|set| serde_json::json!({ "models": set.iter().collect::<Vec<_>>() }));
        tx.execute(
            "INSERT INTO capacity_samples(runtime,lane,window,target,source,remaining_percent,reset_at,observed_at,valid_until,payload_json,account_id,quota_key) VALUES(?,?,?,?,?,?,?,?,?,?,?,?)",
            params![
                runtime,
                lane,
                window.name,
                account.as_str(),
                window.source,
                window.remaining_percent,
                window.reset_at,
                window.observed_at,
                window.valid_until,
                models
                    .unwrap_or(serde_json::json!({ "models": [] }))
                    .to_string(),
                account.as_str(),
                quota_key,
            ],
        )?;
        mutations += 1;
    }

    // Release old latches before inserting this round's exhausted facts: a
    // new zero observed after a reset must remain latched.
    for (lane, name) in &released {
        let quota_key = format!("{}::{}", account.as_str(), lane);
        mutations += tx.execute(
            "DELETE FROM quota_exhaustion WHERE account_id=?1 AND quota_key=?2 AND window_id=?3",
            params![account.as_str(), quota_key, name],
        )?;
    }

    // Durable latch maintenance: every currently exhausted physical window
    // upserts its fact, and sample retention never erases the latch.
    for ((lane, source, name), window) in &physical {
        if window.remaining_percent != Some(0.0)
            || window.reset_at.is_some_and(|reset| reset <= at)
            || exhausted
                .get(&(lane.to_string(), name.to_string()))
                .is_some_and(|fact| window.observed_at < fact.observed_at)
        {
            continue;
        }
        let quota_key = format!("{}::{}", account.as_str(), lane);
        tx.execute(
            "INSERT INTO quota_exhaustion(account_id,quota_key,source,window_id,observed_at,reset_at,collector_revision) \
             VALUES(?,?,?,?,?,?,NULL) \
             ON CONFLICT(account_id,quota_key,source,window_id) DO UPDATE SET \
             observed_at=excluded.observed_at,reset_at=excluded.reset_at",
            params![
                account.as_str(),
                quota_key,
                *source,
                window.name,
                window.observed_at,
                window.reset_at,
            ],
        )?;
        mutations += 1;
        mutations += tx.execute(
            "DELETE FROM quota_exhaustion WHERE account_id=?1 AND quota_key=?2 AND window_id=?3 AND source<>?4",
            params![account.as_str(), quota_key, name, source],
        )?;
    }
    let revision = if mutations > 0 {
        // Retention keeps, for every latched physical window, that window's
        // newest row in the ranker's (observed_at, id) order: it carries the
        // membership the latch needs to keep restricting exactly the models
        // it governs, independently of other windows and of insertion order.
        tx.execute(
            "DELETE FROM capacity_samples WHERE id NOT IN (SELECT id FROM capacity_samples ORDER BY observed_at DESC,id DESC LIMIT ?) \
             AND id NOT IN (SELECT id FROM (SELECT (SELECT s.id FROM capacity_samples s \
               WHERE s.quota_key=q.quota_key AND s.window=q.window_id ORDER BY s.observed_at DESC,s.id DESC LIMIT 1) AS id \
               FROM quota_exhaustion q) WHERE id IS NOT NULL)",
            [retention as i64],
        )?;
        Store::advance_quota_capacity_revision(&tx)?
    } else {
        Store::quota_capacity_revision_in(&tx)?
    };
    tx.commit()?;
    Ok(revision)
}

impl Store {
    /// Latches one authoritative native exhaustion of an exactly mapped
    /// physical window (`account::lane`, `window`) from `source`, in one
    /// immediate transaction that advances `quota_capacity_revision` once.
    ///
    /// The same transaction records the observation itself as a zero-remaining
    /// sample of that pool whose membership is `models` (the model lanes the
    /// pool governs, as the collector mapping defines them), so the ranker
    /// applies the latch to exactly those models; `runtime` is the provider.
    ///
    /// The fact is released only by the existing rules: its reset passing, or
    /// a newer authoritative positive observation of the same lane and
    /// window; an older positive sample never clears it. Callers must pass
    /// only a window whose physical pool the provider's own collector mapping
    /// establishes; unknown windows are never latched.
    // The physical identity (account, runtime, lane, window, source), the
    // membership and the timing are each independent facts of one observation.
    #[allow(clippy::too_many_arguments)]
    pub fn latch_native_exhaustion(
        &mut self,
        account: &AccountId,
        runtime: &str,
        lane: &str,
        window: &str,
        source: &str,
        models: &BTreeSet<String>,
        observed_at: f64,
        reset_at: Option<f64>,
    ) -> Result<i64> {
        if lane.is_empty() || window.is_empty() || source.trim().is_empty() || models.is_empty() {
            return Err(invalid("invalid native exhaustion latch"));
        }
        let quota_key = PhysicalQuotaKey::new(account, lane)?;
        let tx = self
            .conn
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        tx.execute(
            "INSERT INTO quota_exhaustion(account_id,quota_key,source,window_id,observed_at,reset_at,collector_revision) \
             VALUES(?,?,?,?,?,?,NULL) \
             ON CONFLICT(account_id,quota_key,source,window_id) DO UPDATE SET \
             observed_at=MAX(observed_at,excluded.observed_at),reset_at=excluded.reset_at",
            params![account.as_str(), quota_key.as_str(), source, window, observed_at, reset_at],
        )?;
        tx.execute(
            "INSERT INTO capacity_samples(runtime,lane,window,target,source,remaining_percent,reset_at,observed_at,valid_until,payload_json,account_id,quota_key) \
             VALUES(?,?,?,?,?,0.0,?,?,?,?,?,?)",
            params![
                runtime,
                lane,
                window,
                account.as_str(),
                source,
                reset_at,
                observed_at,
                reset_at.unwrap_or(observed_at + UNRESET_EXHAUSTION_TTL_SECONDS),
                serde_json::json!({"models": models}).to_string(),
                account.as_str(),
                quota_key.as_str(),
            ],
        )?;
        let revision = Store::advance_quota_capacity_revision(&tx)?;
        tx.commit()?;
        Ok(revision)
    }
}
