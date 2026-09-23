//! Quota snapshots, exact physical-pool identities and pure read-only ranking.
//! A failed collector never replaces good evidence with an invented zero.
pub mod advice;
pub mod codex_quota;
pub mod collectors;
pub mod lua;
pub mod omniroute;
pub mod provider_catalog;
pub mod provider_ranking;
pub mod quota;
pub mod quota_auth;
pub mod ranking;
pub mod sources;
use crate::{config::Config, domain::now, error::invalid, state::Store, Result};
use rusqlite::{params, TransactionBehavior};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::{
    collections::{BTreeMap, BTreeSet},
    path::Path,
};

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Key {
    pub runtime: String,
    pub lane: String,
    pub window: String,
    pub target: Option<String>,
    pub source: String,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Sample {
    pub key: Key,
    pub remaining_percent: Option<f64>,
    pub reset_at: Option<f64>,
    pub observed_at: Option<f64>,
    pub valid_until: Option<f64>,
}
impl Sample {
    pub fn validate(&self) -> Result<()> {
        for s in [
            &self.key.runtime,
            &self.key.lane,
            &self.key.window,
            &self.key.source,
        ] {
            if s.is_empty() || s.len() > 512 {
                return Err(invalid("invalid quota identity"));
            }
        }
        if self
            .key
            .target
            .as_ref()
            .is_some_and(|s| s.is_empty() || s.len() > 512)
        {
            return Err(invalid("invalid quota target"));
        }
        if self
            .remaining_percent
            .is_some_and(|n| !n.is_finite() || !(0.0..=100.0).contains(&n))
        {
            return Err(invalid("invalid quota percentage"));
        }
        if [self.reset_at, self.observed_at, self.valid_until]
            .into_iter()
            .flatten()
            .any(|n| !n.is_finite() || n < 0.0)
        {
            return Err(invalid("invalid quota timestamp"));
        }
        if let (Some(o), Some(v)) = (self.observed_at, self.valid_until) {
            if v < o {
                return Err(invalid("quota expiry precedes observation"));
            }
        }
        Ok(())
    }
    pub fn fresh(&self, at: f64) -> bool {
        self.validate().is_ok()
            && self.remaining_percent.is_some()
            && self.observed_at.is_some_and(|v| v <= at)
            && self.valid_until.is_some_and(|v| v >= at)
            && self.reset_at.is_none_or(|v| v > at)
    }
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Pool {
    pub pool_id: String,
    pub keys: BTreeSet<Key>,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Route {
    pub route_id: String,
    pub runtime: String,
    pub account: Option<String>,
    pub quota_lane: String,
    pub pool_ids: Vec<String>,
    #[serde(default)]
    pub reset_credits: Option<u64>,
}
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Topology {
    pub pools: Vec<Pool>,
    pub routes: Vec<Route>,
}
impl Topology {
    pub fn validate(&self, runtime: &str) -> Result<()> {
        let mut pools = BTreeSet::new();
        for pool in &self.pools {
            if pool.pool_id.is_empty()
                || pool.pool_id.len() > 2048
                || pool.keys.is_empty()
                || !pools.insert(&pool.pool_id)
            {
                return Err(invalid("invalid or duplicate physical pool"));
            }
            for key in &pool.keys {
                if key.runtime != runtime {
                    return Err(invalid("cross-runtime pool identity"));
                }
                Sample {
                    key: key.clone(),
                    remaining_percent: None,
                    reset_at: None,
                    observed_at: None,
                    valid_until: None,
                }
                .validate()?;
            }
        }
        let mut routes = BTreeSet::new();
        for route in &self.routes {
            let ids: BTreeSet<_> = route.pool_ids.iter().collect();
            if route.runtime != runtime
                || route.route_id.is_empty()
                || route.quota_lane.is_empty()
                || !routes.insert(&route.route_id)
                || ids.is_empty()
                || ids.len() != route.pool_ids.len()
                || ids.iter().any(|id| !pools.contains(*id))
            {
                return Err(invalid("invalid route topology"));
            }
        }
        Ok(())
    }
}
#[derive(Debug, Clone)]
pub struct Slice {
    pub runtime: String,
    pub scope_id: String,
    pub samples: Vec<Sample>,
    pub topology: Topology,
    pub observed_at: f64,
    pub valid_until: f64,
}
/// None is a different identity from every possible account label.
pub fn account_token(account: Option<&str>) -> String {
    account_token_with(account, "base")
}
/// Encodes a nullable account label so it can never collide with a literal one.
///
/// Mirrors Python's `capacity.topology.account_token`. `absent` is the
/// source-defined token standing for "no account"; it must not itself start
/// with `@`, because every present label is rendered as `@` followed by the
/// label with every character outside the unreserved set `A-Za-z0-9-._~`
/// percent-encoded. The mapping is injective: `None` and the literal label
/// equal to `absent` produce different tokens, and the result never contains
/// a `:` separator, so it is safe to embed in a colon-joined identifier.
pub fn account_token_with(account: Option<&str>, absent: &str) -> String {
    match account {
        None => absent.into(),
        Some(a) => {
            let mut encoded = String::from("@");
            for b in a.bytes() {
                if b.is_ascii_alphanumeric() || b"-._~".contains(&b) {
                    encoded.push(b as char);
                } else {
                    encoded.push_str(&format!("%{b:02X}"));
                }
            }
            encoded
        }
    }
}
/// Validates an entire collection slice and commits it atomically.
///
/// `home` is the agent-run home whose store receives the write; `retention` is
/// the positive bound on retained sample rows. The whole slice is checked
/// before anything is written -- non-empty runtime and scope identities, a
/// structurally valid topology owned by that runtime, finite epoch bounds with
/// `valid_until >= observed_at`, per-sample validity, no duplicate or
/// cross-runtime sample identity, and a measurement behind every pool key --
/// so a caller either commits a wholly valid round or nothing. Samples and the
/// route snapshot land in one immediate transaction; the snapshot upserts on
/// `(runtime, scope_id)`, leaving other scopes of the same runtime untouched.
/// Returns the number of samples committed. Errors are `Validation` for a
/// malformed slice or an oversized (>64 KiB) topology payload, or the store's
/// own error when the transaction fails.
pub fn persist(home: &Path, slice: &Slice, retention: usize) -> Result<usize> {
    // A blank runtime is rejected before the topology check: an empty topology
    // would otherwise validate against it and persist an unowned snapshot row.
    if slice.runtime.is_empty() {
        return Err(invalid("invalid quota slice"));
    }
    slice.topology.validate(&slice.runtime)?;
    if slice.scope_id.is_empty()
        || retention == 0
        || !slice.observed_at.is_finite()
        || slice.observed_at < 0.0
        || !slice.valid_until.is_finite()
        || slice.valid_until < slice.observed_at
    {
        return Err(invalid("invalid quota slice"));
    }
    let mut keys = BTreeSet::new();
    for sample in &slice.samples {
        sample.validate()?;
        if sample.key.runtime != slice.runtime || !keys.insert(sample.key.clone()) {
            return Err(invalid("duplicate/cross-runtime quota sample"));
        }
    }
    if slice
        .topology
        .pools
        .iter()
        .flat_map(|p| &p.keys)
        .any(|key| !keys.contains(key))
    {
        return Err(invalid("pool has no measurement in atomic slice"));
    }
    let payload = serde_json::to_string(&slice.topology)?;
    if payload.len() > 65536 {
        return Err(invalid("quota topology exceeds 64 KiB"));
    }
    let mut store = Store::open(home)?;
    let tx = store
        .conn
        .transaction_with_behavior(TransactionBehavior::Immediate)?;
    for s in &slice.samples {
        tx.execute("INSERT INTO capacity_samples(runtime,lane,window,target,source,remaining_percent,reset_at,observed_at,valid_until,payload_json) VALUES(?,?,?,?,?,?,?,?,?,'null')",params![s.key.runtime,s.key.lane,s.key.window,s.key.target,s.key.source,s.remaining_percent,s.reset_at,s.observed_at,s.valid_until])?;
    }
    tx.execute("INSERT INTO capacity_route_snapshots(runtime,scope_id,observed_at,valid_until,payload_json) VALUES(?,?,?,?,?) ON CONFLICT(runtime,scope_id) DO UPDATE SET observed_at=excluded.observed_at,valid_until=excluded.valid_until,payload_json=excluded.payload_json",params![slice.runtime,slice.scope_id,slice.observed_at,slice.valid_until,payload])?;
    tx.execute("DELETE FROM capacity_samples WHERE id NOT IN (SELECT id FROM capacity_samples ORDER BY observed_at DESC,id DESC LIMIT ?)",[retention as i64])?;
    tx.commit()?;
    Ok(slice.samples.len())
}
/// Trims the durable sample history to the newest `retention` rows.
///
/// `home` is the agent-run home whose store is trimmed and `retention` is the
/// positive bound on retained rows; ordering is newest `observed_at` first,
/// breaking ties on insertion id. Mirrors Python's
/// `StateStore.prune_capacity_samples`, which `collect_once` calls once per
/// round **regardless of whether any runtime collected**, so a round in which
/// every runtime failed still enforces the global bound. A zero `retention` is
/// a `Validation` error rather than a silent history wipe.
pub fn prune(home: &Path, retention: usize) -> Result<()> {
    if retention == 0 {
        return Err(invalid("invalid quota retention"));
    }
    let store = Store::open(home)?;
    store.conn.execute(
        "DELETE FROM capacity_samples WHERE id NOT IN (SELECT id FROM capacity_samples ORDER BY observed_at DESC,id DESC LIMIT ?)",
        [retention as i64],
    )?;
    Ok(())
}
fn row_sample(row: &rusqlite::Row<'_>) -> rusqlite::Result<Sample> {
    Ok(Sample {
        key: Key {
            runtime: row.get("runtime")?,
            lane: row.get("lane")?,
            window: row.get("window")?,
            target: row.get("target")?,
            source: row.get("source")?,
        },
        remaining_percent: row.get("remaining_percent")?,
        reset_at: row.get("reset_at")?,
        observed_at: row.get("observed_at")?,
        valid_until: row.get("valid_until")?,
    })
}
fn history(store: &Store, retention: usize) -> Result<BTreeMap<Key, Vec<Sample>>> {
    let mut stmt = store
        .conn
        .prepare("SELECT * FROM capacity_samples ORDER BY observed_at DESC,id DESC LIMIT ?")?;
    let rows = stmt
        .query_map([retention as i64], row_sample)?
        .collect::<std::result::Result<Vec<_>, _>>()?;
    let mut map: BTreeMap<Key, Vec<Sample>> = BTreeMap::new();
    for s in rows {
        map.entry(s.key.clone()).or_default().push(s);
    }
    Ok(map)
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Forecast {
    pub key: Key,
    pub known: bool,
    pub remaining_percent: Option<f64>,
    pub reset_at: Option<f64>,
    pub observed_at: Option<f64>,
    pub warmup: bool,
    pub burn_percent_per_hour: Option<f64>,
    pub sustainable_percent_per_hour: Option<f64>,
    pub risk: String,
    pub burn_span_seconds: Option<f64>,
}
pub fn same_cycle(sample: &Sample, latest: &Sample) -> bool {
    match (sample.reset_at, latest.reset_at) {
        (None, None) => true,
        (Some(a), Some(b)) => {
            a == b || ((a - b).abs() <= 1.0 && latest.observed_at.is_some_and(|t| a.min(b) > t))
        }
        _ => false,
    }
}
/// Mirrors Python's `forecast._is_fresh`: unlike [`Sample::fresh`] (used by
/// the persisted `limits()` view), a missing `valid_until` or `observed_at`
/// is an absent bound, not a failed one -- history rows built straight from
/// already-validated storage may omit them without becoming unknown evidence.
fn forecast_fresh(s: &Sample, at: f64) -> bool {
    s.valid_until.is_none_or(|v| v >= at)
        && s.observed_at.is_none_or(|v| v <= at)
        && s.reset_at.is_none_or(|v| v > at)
}
/// Builds a forecast from a newest-first history for one exact capacity key.
///
/// The newest sample is the sole freshness gate; older samples from its reset
/// cycle remain usable as burn evidence even when their own validity has
/// expired. Missing observation and validity bounds are treated as absent
/// bounds, matching the Python forecast contract.
pub fn forecast(key: &Key, samples: &[Sample], at: f64) -> Forecast {
    let unknown = Forecast {
        key: key.clone(),
        known: false,
        remaining_percent: None,
        reset_at: None,
        observed_at: None,
        warmup: true,
        burn_percent_per_hour: None,
        sustainable_percent_per_hour: None,
        risk: "unknown".into(),
        burn_span_seconds: None,
    };
    let Some(latest) = samples
        .first()
        .filter(|s| forecast_fresh(s, at) && s.remaining_percent.is_some())
    else {
        return unknown;
    };
    let remaining = latest.remaining_percent.unwrap_or(0.0);
    let mut span = None;
    let mut burn = None;
    let matching: Vec<_> = samples.iter().filter(|s| same_cycle(s, latest)).collect();
    if matching.len() >= 2 {
        if let Some(oldest) = matching.last() {
            if let (Some(a), Some(b), Some(old)) = (
                latest.observed_at,
                oldest.observed_at,
                oldest.remaining_percent,
            ) {
                span = Some(a - b);
                if a > b {
                    burn = Some((old - remaining).max(0.0) / ((a - b) / 3600.0));
                }
            }
        }
    }
    let sustainable = latest
        .reset_at
        .filter(|r| *r > at)
        .map(|r| remaining / ((r - at) / 3600.0));
    let risk = if remaining <= 10.0 {
        "high"
    } else if remaining <= 30.0 {
        "medium"
    } else if span.is_some_and(|s| s >= 3600.0) {
        match (burn, sustainable) {
            (Some(b), Some(s)) if b > s * 1.5 => "high",
            (Some(b), Some(s)) if b > s => "medium",
            _ => "low",
        }
    } else {
        "low"
    };
    Forecast {
        key: key.clone(),
        known: true,
        remaining_percent: Some(remaining),
        reset_at: latest.reset_at,
        observed_at: latest.observed_at,
        warmup: burn.is_none(),
        burn_percent_per_hour: burn,
        sustainable_percent_per_hour: sustainable,
        risk: risk.into(),
        burn_span_seconds: span,
    }
}
#[derive(Debug, Clone, Serialize)]
pub struct Window {
    pub key: Key,
    pub remaining_percent: f64,
    pub burn_percent_per_hour: Option<f64>,
    pub burn_span_seconds: Option<f64>,
    pub reset_at: Option<f64>,
    pub projected_percent: Option<f64>,
    pub slack: f64,
    pub marker: String,
    pub risk: String,
}
pub fn window(f: &Forecast, at: f64) -> Option<Window> {
    let remaining = f.remaining_percent?;
    if !f.known
        || !remaining.is_finite()
        || !(0.0..=100.0).contains(&remaining)
        || !f.observed_at.is_some_and(|o| o <= at)
        || f.reset_at.is_some_and(|r| r <= at)
    {
        return None;
    }
    let (projected, slack, marker) =
        match (f.burn_percent_per_hour, f.burn_span_seconds, f.reset_at) {
            (Some(b), Some(s), Some(r)) if s >= 3600.0 => {
                let p = remaining - b * ((r - at) / 3600.0);
                (Some(p), (p / 100.0).clamp(-1.0, 1.0), "projected")
            }
            _ => (
                None,
                (2.0 * remaining / 100.0 - 1.0).clamp(-1.0, 1.0),
                if f.reset_at.is_none() {
                    "no_reset"
                } else if f.warmup {
                    "warmup"
                } else {
                    "thin_evidence"
                },
            ),
        };
    Some(Window {
        key: f.key.clone(),
        remaining_percent: remaining,
        burn_percent_per_hour: f.burn_percent_per_hour,
        burn_span_seconds: f.burn_span_seconds,
        reset_at: f.reset_at,
        projected_percent: projected,
        slack,
        marker: marker.into(),
        risk: f.risk.clone(),
    })
}
#[derive(Debug, Clone, Serialize)]
pub struct Ranked {
    pub runtime: String,
    pub aliases: Vec<Route>,
    pub pool_ids: Vec<String>,
    pub score: f64,
    pub multiplier: f64,
    pub priority: f64,
    pub minimum_remaining_percent: f64,
    pub limiting_key: Key,
    pub limiting_reset_at: Option<f64>,
    pub windows: Vec<Window>,
    pub reset_credits: Option<u64>,
    pub reset_credit_multiplier: f64,
}
pub fn rank(
    runtime: &crate::config::Runtime,
    mut aliases: Vec<Route>,
    mut windows: Vec<Window>,
) -> Option<Ranked> {
    if aliases.is_empty()
        || windows.is_empty()
        || windows.iter().any(|w| w.remaining_percent <= 0.0)
    {
        return None;
    }
    aliases.sort_by(|a, b| {
        runtime
            .weight(b.account.as_deref(), &b.quota_lane)
            .total_cmp(&runtime.weight(a.account.as_deref(), &a.quota_lane))
            .then(a.route_id.cmp(&b.route_id))
    });
    windows.sort_by(|a, b| a.slack.total_cmp(&b.slack).then(a.key.cmp(&b.key)));
    let first = &aliases[0];
    let limiting = &windows[0];
    let score = 1.0 + limiting.slack;
    let multiplier = runtime.weight(first.account.as_deref(), &first.quota_lane);
    let credits = aliases.iter().filter_map(|a| a.reset_credits).max();
    let credit_weight = credits
        .map(|n| 1.0 + (n as f64) / ((n as f64) + 1.0))
        .unwrap_or(1.0);
    if !multiplier.is_finite() || multiplier <= 0.0 {
        return None;
    }
    let minimum = windows
        .iter()
        .map(|w| w.remaining_percent)
        .fold(100.0, f64::min);
    Some(Ranked {
        runtime: first.runtime.clone(),
        pool_ids: first.pool_ids.clone(),
        score,
        multiplier,
        priority: score * multiplier * credit_weight,
        minimum_remaining_percent: minimum,
        limiting_key: limiting.key.clone(),
        limiting_reset_at: limiting.reset_at,
        aliases,
        windows,
        reset_credits: credits,
        reset_credit_multiplier: credit_weight,
    })
}
pub fn limits(home: &Path) -> Result<Value> {
    let store = Store::open(home)?;
    let at = now();
    // Select the latest per identity BEFORE applying a diagnostic result bound.
    let mut q=store.conn.prepare("SELECT * FROM (SELECT *,ROW_NUMBER() OVER(PARTITION BY runtime,lane,window,target,source,account_id,quota_key ORDER BY observed_at DESC,id DESC) AS position FROM capacity_samples) WHERE position=1 ORDER BY runtime,lane,window,target,source,account_id,quota_key LIMIT 1000")?;
    // Account-bound (schema-2) rows keep their account and physical pool so
    // two accounts sharing one provider lane never collapse into one item;
    // legacy rows (no account identity) render exactly as before.
    let rows = q
        .query_map([], |row| {
            Ok((
                row_sample(row)?,
                row.get::<_, Option<String>>("account_id")?,
                row.get::<_, Option<String>>("quota_key")?,
            ))
        })?
        .collect::<std::result::Result<Vec<_>, _>>()?;
    let items:Vec<_>=rows.iter().map(|(s,account,pool)|{let mut item=json!({"key":s.key,"known":s.fresh(at),"remaining_percent":if s.fresh(at){s.remaining_percent}else{None},"reset_at":s.reset_at,"observed_at":s.observed_at,"valid_until":s.valid_until});if let (Some(account),Some(pool))=(account,pool){item["account"]=json!(account);item["pool"]=json!(pool);}item}).collect();
    Ok(json!({"observed_at":at,"items":items}))
}
/// Build the enabled-runtime capacity order from one persisted snapshot.
///
/// Mirrors Python's `build_capacity_order` + `rank_capacity_routes`: reads
/// the persisted topology/sample history, filters to enabled runtimes,
/// resolves each route's account/lane/runtime priority factor once
/// (`Runtime::weight` already implements that exact precedence), and
/// delegates ranking to [`ranking::rank_capacity_routes`] so the ordering
/// math lives in one provider-neutral place.
pub fn order(home: &Path) -> Result<Value> {
    order_with_clock(home, now)
}

/// Builds capacity order after reading the ranking clock exactly once.
pub fn order_with_clock<F>(home: &Path, clock: F) -> Result<Value>
where
    F: FnOnce() -> f64,
{
    order_at(home, clock())
}

/// Builds capacity order from one already-read ranking timestamp.
fn order_at(home: &Path, at: f64) -> Result<Value> {
    let config = Config::load(home)?;
    let store = Store::open(home)?;
    let series = history(&store, config.capacity.sample_retention)?;
    let forecasts: BTreeMap<Key, Forecast> = series
        .iter()
        .map(|(k, s)| (k.clone(), forecast(k, s, at)))
        .collect();
    let mut stmt=store.conn.prepare("SELECT runtime,scope_id,observed_at,valid_until,payload_json FROM capacity_route_snapshots ORDER BY runtime,scope_id")?;
    let rows = stmt
        .query_map([], |r| {
            Ok((
                r.get::<_, String>(0)?,
                r.get::<_, String>(1)?,
                r.get::<_, f64>(2)?,
                r.get::<_, f64>(3)?,
                r.get::<_, String>(4)?,
            ))
        })?
        .collect::<std::result::Result<Vec<_>, _>>()?;

    // Only enabled runtimes ever reach ranking; Python filters the whole
    // snapshot (routes *and* evidence) to `enabled` before ranking, so a
    // disabled runtime's rows must never surface as routes or deferred/
    // omitted evidence either.
    let mut snapshot_evidence: Vec<ranking::OrderEvidence> = Vec::new();
    let mut fresh: Vec<(String, String, Topology)> = Vec::new();
    for (runtime, scope, observed, valid, payload) in rows {
        if !config.runtime(&runtime).is_ok_and(|r| r.enabled) {
            continue;
        }
        let evidence = |reason: &str, detail: &str| ranking::OrderEvidence {
            runtime: runtime.clone(),
            scope_id: Some(scope.clone()),
            route_id: None,
            reason: reason.into(),
            detail: detail.into(),
        };
        if !(observed.is_finite() && valid.is_finite() && observed >= 0.0 && valid >= observed) {
            snapshot_evidence.push(evidence(
                "malformed",
                "quota snapshot timestamps are malformed",
            ));
            continue;
        }
        if observed > at || valid < at {
            snapshot_evidence.push(evidence("expired", "topology snapshot is not fresh"));
            continue;
        }
        let topology = match serde_json::from_str::<Topology>(&payload) {
            Ok(t) if t.validate(&runtime).is_ok() => t,
            _ => {
                snapshot_evidence.push(evidence("malformed", "malformed persisted route snapshot"));
                continue;
            }
        };
        fresh.push((runtime, scope, topology));
    }

    // A pool/route definition is only usable when every fresh scope that
    // declares it agrees on its exact contents; any disagreement defers
    // every contributing scope (never just one), matching snapshot.py.
    let mut pool_defs: BTreeMap<(String, String), Vec<(String, Pool)>> = BTreeMap::new();
    let mut route_defs: BTreeMap<(String, String), Vec<(String, Route)>> = BTreeMap::new();
    for (runtime, scope, topology) in &fresh {
        for pool in &topology.pools {
            pool_defs
                .entry((runtime.clone(), pool.pool_id.clone()))
                .or_default()
                .push((scope.clone(), pool.clone()));
        }
        for route in &topology.routes {
            route_defs
                .entry((runtime.clone(), route.route_id.clone()))
                .or_default()
                .push((scope.clone(), route.clone()));
        }
    }
    let conflicted_pools: BTreeSet<(String, String)> = pool_defs
        .iter()
        .filter(|(_, v)| v.iter().any(|(_, p)| p != &v[0].1))
        .map(|(k, _)| k.clone())
        .collect();
    let conflicted_routes: BTreeSet<(String, String)> = route_defs
        .iter()
        .filter(|(_, v)| v.iter().any(|(_, r)| r != &v[0].1))
        .map(|(k, _)| k.clone())
        .collect();
    let pools: BTreeMap<(String, String), Pool> = pool_defs
        .iter()
        .filter(|(k, _)| !conflicted_pools.contains(*k))
        .map(|(k, v)| (k.clone(), v[0].1.clone()))
        .collect();

    let mut route_inputs: Vec<ranking::RouteInput> = Vec::new();
    for (runtime, scope, topology) in fresh {
        for route in topology.routes {
            let evidence = |reason: &str, detail: String| ranking::OrderEvidence {
                runtime: runtime.clone(),
                scope_id: Some(scope.clone()),
                route_id: None,
                reason: reason.into(),
                detail,
            };
            if conflicted_routes.contains(&(runtime.clone(), route.route_id.clone())) {
                snapshot_evidence.push(evidence("conflict", "conflicting route definition".into()));
                continue;
            }
            if route.pool_ids.iter().any(|id| {
                conflicted_pools.contains(&(runtime.clone(), id.clone()))
                    || !pools.contains_key(&(runtime.clone(), id.clone()))
            }) {
                snapshot_evidence.push(evidence(
                    "conflict",
                    "route references conflicted pool".into(),
                ));
                continue;
            }
            let route_pools: Vec<Pool> = route
                .pool_ids
                .iter()
                .map(|id| pools[&(runtime.clone(), id.clone())].clone())
                .collect();
            let mut keys: Vec<Key> = route_pools
                .iter()
                .flat_map(|p| p.keys.iter().cloned())
                .collect();
            keys.sort();
            if keys.iter().any(|k| !forecasts.contains_key(k)) {
                snapshot_evidence.push(evidence(
                    "missing_forecast",
                    format!("route {} has no exact forecast", route.route_id),
                ));
                continue;
            }
            let matching: Vec<Forecast> = keys.iter().map(|k| forecasts[k].clone()).collect();
            if matching.iter().any(|f| !f.known) {
                snapshot_evidence.push(evidence(
                    "unknown_forecast",
                    format!("route {} has an unknown forecast", route.route_id),
                ));
                continue;
            }
            route_inputs.push(ranking::RouteInput {
                descriptor: route,
                pools: route_pools,
                forecasts: matching,
            });
        }
    }

    let multipliers: BTreeMap<String, f64> = config
        .runtimes
        .iter()
        .filter(|(_, r)| r.enabled)
        .map(|(n, r)| (n.clone(), r.priority_multiplier))
        .collect();
    let mut route_multipliers: BTreeMap<(String, String), f64> = BTreeMap::new();
    for input in &route_inputs {
        let d = &input.descriptor;
        let weight = config
            .runtime(&d.runtime)?
            .weight(d.account.as_deref(), &d.quota_lane);
        route_multipliers.insert((d.runtime.clone(), d.route_id.clone()), weight);
    }
    let evidenced: BTreeSet<String> = route_inputs
        .iter()
        .map(|r| r.descriptor.runtime.clone())
        .chain(snapshot_evidence.iter().map(|e| e.runtime.clone()))
        .collect();

    let mut result = ranking::rank_capacity_routes(
        route_inputs,
        snapshot_evidence,
        &multipliers,
        &route_multipliers,
        at,
    )?;
    let mut unavailable: BTreeSet<String> = result.unavailable_runtimes.into_iter().collect();
    for name in multipliers.keys() {
        if !evidenced.contains(name) {
            unavailable.insert(name.clone());
        }
    }
    result.unavailable_runtimes = unavailable.into_iter().collect();
    Ok(serde_json::to_value(result)?)
}
pub async fn models(home: &Path) -> Result<Value> {
    sources::models(home).await
}
pub async fn collect(home: &Path) -> Result<Value> {
    sources::collect(home).await
}
