//! Capacity sample persistence units of the sole writer (rows 76 and 77).
//!
//! These units validate a capacity slice exactly like the collector
//! (identities, percentages, times, unique same-runtime samples, topology
//! structure and a measurement for every pool key) before any SQL, then
//! write the samples, the route snapshot and the retention trim atomically
//! inside the writer's transaction.

use crate::Store;
use agent_run_domain::{Result, error::invalid};
use rusqlite::params;
use std::collections::BTreeSet;

/// Maximum samples in one persisted slice.
const MAX_SAMPLES: usize = 256;
/// Maximum topology JSON bytes.
const MAX_TOPOLOGY: usize = 65_536;

/// Physical quota identity of one sample (the collector's key shape).
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct Key {
    /// Runtime.
    runtime: String,
    /// Lane.
    lane: String,
    /// Window.
    window: String,
    /// Target.
    target: Option<String>,
    /// Source.
    source: String,
}

/// One physical pool and the keys measuring it.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct Pool {
    /// Pool identity.
    pool_id: String,
    /// Measuring keys.
    keys: BTreeSet<Key>,
}

/// One route over pools.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct Route {
    /// Route identity.
    route_id: String,
    /// Runtime.
    runtime: String,
    /// Account.
    account: Option<String>,
    /// Quota lane.
    quota_lane: String,
    /// Pools of the route.
    pool_ids: Vec<String>,
    /// Reset credits.
    #[serde(default)]
    reset_credits: Option<u64>,
}

/// Validated topology of one capacity slice.
#[derive(Debug, Clone, Default, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct Topology {
    /// Physical pools.
    pools: Vec<Pool>,
    /// Routes.
    routes: Vec<Route>,
}

/// Validates a key's identity fields.
fn valid_key(key: &Key) -> Result<()> {
    for field in [&key.runtime, &key.lane, &key.window, &key.source] {
        if field.is_empty() || field.len() > 512 {
            return Err(invalid("invalid quota identity"));
        }
    }
    if key
        .target
        .as_ref()
        .is_some_and(|t| t.is_empty() || t.len() > 512)
    {
        return Err(invalid("invalid quota target"));
    }
    Ok(())
}

/// Validates one sample: identity, a finite 0..=100 percentage, finite
/// nonnegative times and an expiry not before its observation.
fn valid_sample(sample: &crate::ops::CapacitySampleRow) -> Result<Key> {
    let key = Key {
        runtime: sample.runtime.clone(),
        lane: sample.lane.clone(),
        window: sample.window.clone(),
        target: sample.target.clone(),
        source: sample.source.clone(),
    };
    valid_key(&key)?;
    if sample
        .remaining_percent
        .is_some_and(|n| !n.is_finite() || !(0.0..=100.0).contains(&n))
    {
        return Err(invalid("invalid quota percentage"));
    }
    if [
        sample.reset_at,
        Some(sample.observed_at),
        Some(sample.valid_until),
    ]
    .into_iter()
    .flatten()
    .any(|n| !n.is_finite() || n < 0.0)
    {
        return Err(invalid("invalid quota timestamp"));
    }
    if sample.valid_until < sample.observed_at {
        return Err(invalid("quota expiry precedes observation"));
    }
    Ok(key)
}

/// Validates the topology against `runtime`: unique nonempty pools of
/// same-runtime keys, and unique routes over existing, distinct pools.
fn valid_topology(topology: &Topology, runtime: &str) -> Result<()> {
    let mut pools = BTreeSet::new();
    for pool in &topology.pools {
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
            valid_key(key)?;
        }
    }
    let mut routes = BTreeSet::new();
    for route in &topology.routes {
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

/// Validates a whole slice exactly like the collector before any SQL and
/// returns the canonical topology text.
fn valid_slice(slice: &crate::ops::PersistCapacitySamples) -> Result<String> {
    if slice.runtime.is_empty() || slice.topology_json.len() > MAX_TOPOLOGY {
        return Err(invalid("invalid quota slice"));
    }
    let topology: Topology = serde_json::from_str(&slice.topology_json)
        .map_err(|_| invalid("invalid quota topology"))?;
    valid_topology(&topology, &slice.runtime)?;
    if slice.scope_id.is_empty()
        || slice.retention == 0
        || slice.samples.len() > MAX_SAMPLES
        || !slice.observed_at.is_finite()
        || slice.observed_at < 0.0
        || !slice.valid_until.is_finite()
        || slice.valid_until < slice.observed_at
    {
        return Err(invalid("invalid quota slice"));
    }
    let mut keys = BTreeSet::new();
    for sample in &slice.samples {
        let key = valid_sample(sample)?;
        if key.runtime != slice.runtime || !keys.insert(key) {
            return Err(invalid("duplicate/cross-runtime quota sample"));
        }
    }
    if topology
        .pools
        .iter()
        .flat_map(|pool| &pool.keys)
        .any(|key| !keys.contains(key))
    {
        return Err(invalid("pool has no measurement in atomic slice"));
    }
    let payload = serde_json::to_string(&topology)?;
    if payload.len() > MAX_TOPOLOGY {
        return Err(invalid("quota topology exceeds 64 KiB"));
    }
    Ok(payload)
}

impl Store<'_> {
    /// Persists one validated capacity slice (samples, route snapshot upsert
    /// and retention trim) in one write scope; returns the sample count.
    pub fn persist_capacity_samples(
        &self,
        slice: &crate::ops::PersistCapacitySamples,
    ) -> Result<usize> {
        let topology = valid_slice(slice)?;
        let tx = crate::writer::WriteTx::begin(&self.conn)?;
        for sample in &slice.samples {
            tx.execute(
                "INSERT INTO capacity_samples(runtime,lane,window,target,source,remaining_percent,reset_at,observed_at,valid_until,payload_json) \
                 VALUES(?,?,?,?,?,?,?,?,?,'null')",
                params![
                    sample.runtime,
                    sample.lane,
                    sample.window,
                    sample.target,
                    sample.source,
                    sample.remaining_percent,
                    sample.reset_at,
                    sample.observed_at,
                    sample.valid_until
                ],
            )?;
        }
        tx.execute(
            "INSERT INTO capacity_route_snapshots(runtime,scope_id,observed_at,valid_until,payload_json) VALUES(?,?,?,?,?) \
             ON CONFLICT(runtime,scope_id) DO UPDATE SET observed_at=excluded.observed_at,valid_until=excluded.valid_until,payload_json=excluded.payload_json",
            params![
                slice.runtime,
                slice.scope_id,
                slice.observed_at,
                slice.valid_until,
                topology
            ],
        )?;
        trim(&tx, slice.retention)?;
        tx.commit()?;
        Ok(slice.samples.len())
    }

    /// Trims capacity history to the newest `retention` rows; returns the
    /// number of removed rows. A zero retention is refused, never a wipe.
    pub fn prune_capacity_samples(&self, retention: usize) -> Result<usize> {
        if retention == 0 {
            return Err(invalid("invalid quota retention"));
        }
        trim(&self.conn, retention)
    }
}

/// Deletes all but the newest `retention` capacity samples.
fn trim(conn: &rusqlite::Connection, retention: usize) -> Result<usize> {
    Ok(conn.execute(
        "DELETE FROM capacity_samples WHERE id NOT IN (SELECT id FROM capacity_samples ORDER BY observed_at DESC,id DESC LIMIT ?)",
        [retention as i64],
    )?)
}
