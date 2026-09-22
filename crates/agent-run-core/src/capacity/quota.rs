//! Version-1 collector output normalization into host-bound quota snapshots.
//!
//! The collector engine (see [`super::lua`]) produces raw JSON facts; this
//! module binds them to one global account and validates them through
//! [`NormalizedQuotaSnapshot::validate`] before any persistence or scoring.
//! Freshness, unknown data, and collection failure stay distinct: a malformed
//! round returns an error and never becomes an account fact.

use crate::{error::invalid, Result};
use agent_run_domain::catalog::{
    AccountId, NormalizedQuotaSnapshot, PhysicalQuotaKey, QuotaModelObservation,
    QuotaPoolObservation, QuotaWindow,
};
use serde_json::Value;
use std::collections::{BTreeMap, BTreeSet};

/// Maximum windows accepted in one version-1 collector output.
pub const MAX_OUTPUT_WINDOWS: usize = 256;
/// Maximum model names accepted in one version-1 collector output.
pub const MAX_OUTPUT_MODELS: usize = 256;
/// Shelf-life seconds assumed when a window declares no `valid_until`.
///
/// Mirrors the capacity collector TTL: it bounds staleness only and never
/// fabricates a reset or remaining value.
pub const DEFAULT_WINDOW_TTL_SECONDS: f64 = 900.0;

/// The explicit Rust-side scope one collector invocation runs in.
///
/// `runtime` names the observing engine scope used for persistence, `source`
/// labels windows with the collector identity (never chosen by a script), and
/// `models` is the exhaustive set of configured models a script may report.
#[derive(Debug, Clone)]
pub struct CollectorScope {
    /// Engine scope that owns the persisted rows for this round.
    pub runtime: String,
    /// Stable collector source identity stamped on every emitted window.
    pub source: String,
    /// Configured model ids; output naming anything else is rejected.
    pub models: BTreeSet<String>,
}

/// Reads a finite JSON number, rejecting present-but-nonnumeric facts.
fn finite(value: Option<&Value>, code: &'static str) -> Result<Option<f64>> {
    match value {
        None | Some(Value::Null) => Ok(None),
        Some(v) => v
            .as_f64()
            .filter(|n| n.is_finite())
            .map(Some)
            .ok_or_else(|| invalid(code)),
    }
}

/// Normalizes one version-1 collector output into a validated snapshot.
///
/// `raw` must be an object `{ "windows": [ ... ] }` whose entries carry
/// `pool` (physical lane), `window`, a nonempty explicit `models` list, an
/// optional `remaining_percent` (absent or null means unknown), optional
/// `reset_at`, required `observed_at`, and optional `valid_until` defaulting
/// to `observed_at + DEFAULT_WINDOW_TTL_SECONDS`. Unknown entry keys, foreign
/// or unconfigured models, duplicate or conflicting pool windows, nonfinite or
/// out-of-range numbers, inverted times, and outputs above
/// [`MAX_OUTPUT_WINDOWS`]/[`MAX_OUTPUT_MODELS`] are rejected before any fact
/// is bound to `account`; the account identity itself comes only from Rust.
/// Returns a snapshot that already passed [`NormalizedQuotaSnapshot::validate`].
pub fn normalize_collector_output(
    account: &AccountId,
    scope: &CollectorScope,
    raw: &Value,
    max_windows: usize,
    max_models: usize,
) -> Result<NormalizedQuotaSnapshot> {
    if max_windows == 0 || max_models == 0 || max_windows > MAX_OUTPUT_WINDOWS {
        return Err(invalid("invalid collector output bound"));
    }
    let entries = raw
        .get("windows")
        .and_then(Value::as_array)
        .ok_or_else(|| invalid("quota_output_malformed"))?;
    if entries.len() > max_windows {
        return Err(invalid("quota_output_overflow"));
    }
    // One fact per (lane, source, window); repeats are rejected as duplicates
    // whether or not their facts agree, so one physical window stays singular.
    let mut pools: BTreeMap<String, BTreeMap<(String, String), QuotaWindow>> = BTreeMap::new();
    let mut membership: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();
    let mut models_seen = BTreeSet::new();
    for entry in entries {
        let object = entry
            .as_object()
            .ok_or_else(|| invalid("quota_output_malformed"))?;
        for key in object.keys() {
            if !matches!(
                key.as_str(),
                "pool"
                    | "window"
                    | "models"
                    | "remaining_percent"
                    | "reset_at"
                    | "observed_at"
                    | "valid_until"
            ) {
                return Err(invalid("quota_output_malformed"));
            }
        }
        let pool = object
            .get("pool")
            .and_then(Value::as_str)
            .filter(|s| !s.is_empty() && s.len() <= 128)
            .ok_or_else(|| invalid("quota_output_malformed"))?;
        let window = object
            .get("window")
            .and_then(Value::as_str)
            .filter(|s| !s.is_empty() && s.len() <= 128)
            .ok_or_else(|| invalid("quota_output_malformed"))?;
        let listed = object
            .get("models")
            .and_then(Value::as_array)
            .ok_or_else(|| invalid("quota_output_malformed"))?;
        if listed.is_empty() {
            return Err(invalid("quota_output_malformed"));
        }
        // Unknown remaining is a valid fact; only present nonfinite or
        // out-of-range values fail.
        let remaining = match object.get("remaining_percent") {
            None | Some(Value::Null) => None,
            Some(v) => {
                let n = v
                    .as_f64()
                    .filter(|n| n.is_finite())
                    .ok_or_else(|| invalid("quota_output_invalid_number"))?;
                if !(0.0..=100.0).contains(&n) {
                    return Err(invalid("quota_output_invalid_number"));
                }
                Some(n)
            }
        };
        let reset_at = finite(object.get("reset_at"), "quota_output_invalid_number")?;
        let observed_at = object
            .get("observed_at")
            .and_then(Value::as_f64)
            .filter(|n| n.is_finite() && *n >= 0.0)
            .ok_or_else(|| invalid("quota_output_invalid_time"))?;
        let valid_until = finite(object.get("valid_until"), "quota_output_invalid_time")?
            .unwrap_or(observed_at + DEFAULT_WINDOW_TTL_SECONDS);
        if valid_until < observed_at {
            return Err(invalid("quota_output_invalid_time"));
        }
        let mut window_models = BTreeSet::new();
        for model in listed {
            let model = model
                .as_str()
                .filter(|s| !s.is_empty() && s.len() <= 256)
                .ok_or_else(|| invalid("quota_output_malformed"))?;
            if !scope.models.contains(model) {
                return Err(invalid("quota_output_foreign_model"));
            }
            window_models.insert(model.to_owned());
        }
        models_seen.extend(window_models.iter().cloned());
        if models_seen.len() > max_models {
            return Err(invalid("quota_output_overflow"));
        }
        let fact = QuotaWindow {
            source: scope.source.clone(),
            name: window.to_owned(),
            remaining_percent: remaining,
            reset_at,
            observed_at,
            valid_until,
        };
        let windows = pools.entry(pool.to_owned()).or_default();
        if windows
            .insert((scope.source.clone(), window.to_owned()), fact)
            .is_some()
        {
            return Err(invalid("quota_output_duplicate_window"));
        }
        for model in &window_models {
            membership
                .entry(model.clone())
                .or_default()
                .insert(pool.to_owned());
        }
    }
    let mut models = Vec::new();
    for (model, lanes) in &membership {
        let mut pools_observed = Vec::new();
        for lane in lanes {
            let key = PhysicalQuotaKey::new(account, lane)?;
            let mut windows: Vec<QuotaWindow> = pools[lane].values().cloned().collect();
            windows.sort_by(|a, b| (&a.source, &a.name).cmp(&(&b.source, &b.name)));
            pools_observed.push(QuotaPoolObservation { key, windows });
        }
        pools_observed.sort_by(|a, b| a.key.as_str().cmp(b.key.as_str()));
        models.push(QuotaModelObservation {
            model: model.clone(),
            pools: pools_observed,
        });
    }
    models.sort_by(|a, b| a.model.cmp(&b.model));
    let snapshot = NormalizedQuotaSnapshot {
        account: account.clone(),
        models,
    };
    snapshot.validate()?;
    Ok(snapshot)
}
