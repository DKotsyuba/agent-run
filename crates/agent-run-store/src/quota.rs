//! Account-bound quota observation persistence and the exhaustion latch.
//!
//! The store owns raw persistence only: every write lands in one immediate
//! transaction that advances the global `quota_capacity_revision` singleton
//! exactly once when it mutates any row, and never scores or ranks anything.

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

/// Reads the latest persisted window per `(lane, window)` for one runtime
/// and global account, across every collector source identity, so a source
/// change never strands or forks a latched physical fact.
///
/// Returns only the windows relevant to the exhaustion latch: those whose
/// latest fact reports zero remaining. Unknown history stays absent.
fn latest_exhausted_windows(
    tx: &rusqlite::Transaction<'_>,
    runtime: &str,
    account: &AccountId,
) -> Result<BTreeMap<(String, String), QuotaWindow>> {
    let mut stmt = tx.prepare(
        "SELECT lane,window,source,remaining_percent,reset_at,observed_at,valid_until FROM \
         (SELECT *,ROW_NUMBER() OVER(PARTITION BY lane,window ORDER BY observed_at DESC,id DESC) \
         AS position FROM capacity_samples WHERE runtime=?1 AND target=?2) \
         WHERE position=1 AND remaining_percent=0.0",
    )?;
    let rows = stmt.query_map(params![runtime, account.as_str()], |row| {
        Ok((
            row.get::<_, String>(0)?,
            row.get::<_, String>(1)?,
            row.get::<_, String>(2)?,
            row.get::<_, Option<f64>>(3)?,
            row.get::<_, Option<f64>>(4)?,
            row.get::<_, f64>(5)?,
            row.get::<_, f64>(6)?,
        ))
    })?;
    let mut exhausted = BTreeMap::new();
    for row in rows {
        let (lane, window, source, remaining, reset_at, observed_at, valid_until) = row?;
        if remaining != Some(0.0) {
            continue;
        }
        exhausted.insert(
            (lane.clone(), window.clone()),
            QuotaWindow {
                source,
                name: window,
                remaining_percent: remaining,
                reset_at,
                observed_at,
                valid_until,
            },
        );
    }
    Ok(exhausted)
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
/// A previously persisted zero-remaining window survives a round that only
/// produced unknown data for its pool (empty or `None` remaining) until its
/// known reset time passes, or — when reset is absent — until positive fresh
/// evidence arrives. Fresh exhaustion, fresh percentages, and expired resets
/// are never overridden; carried facts keep their original observation times.
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
/// names the engine scope that observed the facts; the global account id
/// occupies the sample `target` and the physical lane the `lane` column, so
/// alias labels never fork one physical pool. Model membership is recorded
/// per pool row in `payload_json`. The exhaustion latch keeps an unexpired
/// zero-remaining fact authoritative over unknown fresh data. Every mutating
/// round advances `quota_capacity_revision` exactly once inside the same
/// immediate transaction; a round that inserts no row leaves the revision
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
    let exhausted = latest_exhausted_windows(&tx, runtime, &account)?;
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
    let mut inserted = 0usize;
    for ((lane, _source, _name), window) in &physical {
        window.validate()?;
        let models = pool_models
            .get(lane)
            .map(|set| serde_json::json!({ "models": set.iter().collect::<Vec<_>>() }));
        tx.execute(
            "INSERT INTO capacity_samples(runtime,lane,window,target,source,remaining_percent,reset_at,observed_at,valid_until,payload_json) VALUES(?,?,?,?,?,?,?,?,?,?)",
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
                models.unwrap_or(serde_json::json!({ "models": [] })).to_string()
            ],
        )?;
        inserted += 1;
    }
    let revision = if inserted > 0 {
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
