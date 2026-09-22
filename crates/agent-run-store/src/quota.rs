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
use rusqlite::{params, TransactionBehavior};
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

/// Applies the exhaustion latch to `snapshot` in place.
///
/// A latched zero-remaining window survives a round that only produced
/// unknown data for its pool (empty or `None` remaining) until its known
/// reset time passes, or — when reset is absent — until actually fresh
/// positive evidence arrives (`remaining > 0`, observed not later than `at`,
/// validity unexpired, reset not passed). Fresh exhaustion, fresh
/// percentages, and expired resets are never overridden; carried facts keep
/// their original observation times and collector source.
pub fn retain_exhausted(
    exhausted: &BTreeMap<(String, String), QuotaWindow>,
    snapshot: &mut NormalizedQuotaSnapshot,
    account: &AccountId,
    at: f64,
) -> Result<()> {
    for model in &mut snapshot.models {
        for pool in &mut model.pools {
            let lane = lane_of(&pool.key, account)?.to_owned();
            // Only actually fresh positive evidence releases the latch: a
            // stale or expired positive observation never revives capacity,
            // and unknown or missing data displaces nothing. Matching is by
            // physical window name across collector sources, so a source
            // change neither forks the pool nor strands the fact.
            let fresh_positive = |windows: &[QuotaWindow], name: &str| {
                windows.iter().any(|w| {
                    w.name == name
                        && w.remaining_percent.is_some_and(|remaining| remaining > 0.0)
                        && w.observed_at <= at
                        && w.valid_until >= at
                        && w.reset_at.is_none_or(|reset| reset > at)
                })
            };
            let mut carried: Vec<QuotaWindow> = Vec::new();
            for ((l, name), fact) in exhausted {
                if l != &lane
                    || fact.reset_at.is_some_and(|reset| reset <= at)
                    || fresh_positive(&pool.windows, name)
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
    let mut snapshot = snapshot.clone();
    retain_exhausted(&exhausted, &mut snapshot, &account, at)?;

    // One physical window is persisted exactly once with its full model
    // membership, however many configured models share the pool.
    let mut physical: BTreeMap<(&str, &str, &str), &QuotaWindow> = BTreeMap::new();
    let mut pool_models: BTreeMap<&str, BTreeSet<&str>> = BTreeMap::new();
    for model in &snapshot.models {
        for pool in &model.pools {
            let lane = lane_of(&pool.key, &account)?;
            pool_models
                .entry(lane)
                .or_default()
                .insert(model.model.as_str());
            for window in &pool.windows {
                // Shared pools repeat identical facts across models; the
                // snapshot validator already proved them equal.
                physical
                    .entry((lane, window.source.as_str(), window.name.as_str()))
                    .or_insert(window);
            }
        }
    }
    let mut mutations = 0usize;
    for ((lane, _source, _name), window) in &physical {
        window.validate()?;
        let quota_key = format!("{}::{}", account.as_str(), lane);
        let models = pool_models
            .get(lane)
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

    // Durable latch maintenance: every currently exhausted physical window
    // upserts its fact (so a fresh exhaustion latches even without history),
    // and a released prior latch (fresh positive evidence, or an expired
    // reset) is cleared. Both count as relevant mutations for the same
    // revision; sample retention never erases the latch.
    let mut latched_now: BTreeSet<(&str, &str)> = BTreeSet::new();
    for ((lane, source, name), window) in &physical {
        if window.remaining_percent != Some(0.0) {
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
        latched_now.insert((*lane, *name));
        mutations += 1;
    }
    for (lane, name) in exhausted.keys() {
        if !latched_now.contains(&(lane.as_str(), name.as_str())) {
            let quota_key = format!("{}::{}", account.as_str(), lane);
            mutations += tx.execute(
                "DELETE FROM quota_exhaustion WHERE account_id=?1 AND quota_key=?2 AND window_id=?3",
                params![account.as_str(), quota_key, name],
            )?;
        }
    }

    let revision = if mutations > 0 {
        tx.execute(
            "DELETE FROM capacity_samples WHERE id NOT IN (SELECT id FROM capacity_samples ORDER BY observed_at DESC,id DESC LIMIT ?)",
            [retention as i64],
        )?;
        Store::advance_quota_capacity_revision(&tx)?
    } else {
        Store::quota_capacity_revision_in(&tx)?
    };
    tx.commit()?;
    Ok(revision)
}
