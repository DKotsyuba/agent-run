//! Pure, provider-neutral ordering of validated capacity routes.
//!
//! This is a direct port of `agent_run.capacity.ranking` (`rank_capacity_routes`).
//! It performs no provider calls, role selection, or writes: it only turns
//! already-collected forecasts into a deterministic route order. Callers (the
//! `order()` projection in `capacity::mod`) own config lookups and topology
//! parsing; this module never inspects `Key::target` or config.
use super::{Forecast, Key, Pool, Route};
use crate::{error::invalid, Result};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet, HashMap};

/// Scoring evidence for one exact governing quota window.
///
/// Mirrors Python's `CapacityWindowExplanation`. `marker` is `"projected"` for
/// reliable burn evidence or one of `"warmup"`, `"thin_evidence"`, and
/// `"no_reset"` for the centered remaining-percent fallback.
#[derive(Debug, Clone, Serialize)]
pub struct WindowExplanation {
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

/// Explains why one scope or route was deferred from the working order.
///
/// `scope_id` is present for collection/snapshot evidence. `route_id` is
/// present for a defensive ranker rejection; at most one of the two is set.
#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct OrderEvidence {
    pub runtime: String,
    pub scope_id: Option<String>,
    pub route_id: Option<String>,
    pub reason: String,
    pub detail: String,
}
impl OrderEvidence {
    fn order_key(&self) -> (&str, &str, &str, &str, &str) {
        (
            &self.runtime,
            self.scope_id.as_deref().unwrap_or(""),
            self.route_id.as_deref().unwrap_or(""),
            &self.reason,
            &self.detail,
        )
    }
}
impl PartialOrd for OrderEvidence {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}
impl Ord for OrderEvidence {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.order_key().cmp(&other.order_key())
    }
}

/// One physical capacity choice in descending usage priority.
///
/// `aliases` retains every concrete launch descriptor sharing the same
/// runtime and physical pool set, ordered by descending effective factor then
/// `route_id`. `score` is unmultiplied in `[0, 2]`; `priority` equals
/// `score * multiplier * reset_credit_multiplier`. `limiting_key` and
/// `limiting_reset_at` belong to the window with the minimum slack, not
/// merely the earliest reset in the route.
#[derive(Debug, Clone, Serialize)]
pub struct RankedRoute {
    pub runtime: String,
    pub aliases: Vec<Route>,
    pub pool_ids: Vec<String>,
    pub score: f64,
    pub multiplier: f64,
    pub priority: f64,
    pub minimum_remaining_percent: f64,
    pub limiting_key: Key,
    pub limiting_reset_at: Option<f64>,
    pub windows: Vec<WindowExplanation>,
    pub reset_credits: Option<u64>,
    pub reset_credit_multiplier: f64,
}

/// One exhausted physical capacity choice excluded before scoring.
#[derive(Debug, Clone, Serialize)]
pub struct OmittedRoute {
    pub runtime: String,
    pub aliases: Vec<Route>,
    pub pool_ids: Vec<String>,
    pub limiting_key: Key,
    pub limiting_reset_at: Option<f64>,
    pub reason: String,
}

/// Role-independent ordered routes plus complete exclusion evidence.
#[derive(Debug, Clone, Serialize, Default)]
pub struct CapacityOrder {
    pub observed_at: f64,
    pub routes: Vec<RankedRoute>,
    pub deferred: Vec<OrderEvidence>,
    pub omitted: Vec<OmittedRoute>,
    pub unavailable_runtimes: Vec<String>,
    pub insufficient_diversity: bool,
}

/// One routable descriptor with its exact pools and known (unwindowed) forecasts.
///
/// Mirrors Python's `CapacityRoute`. Only pool ids (not the pool key sets) are
/// needed for grouping, so `pools` carries just enough to reproduce the
/// Python grouping key `tuple(sorted(pool.pool_id for pool in route.pools))`.
#[derive(Debug, Clone, Deserialize)]
pub struct RouteInput {
    pub descriptor: Route,
    #[serde(default)]
    pub pools: Vec<Pool>,
    pub forecasts: Vec<Forecast>,
}

fn key_order(key: &Key) -> (&str, &str, &str, &str, &str) {
    (
        &key.runtime,
        &key.lane,
        &key.window,
        key.target.as_deref().unwrap_or(""),
        &key.source,
    )
}

/// Convert one forecast to conservative scoring evidence.
///
/// Unknown, malformed, future-observed, or already-reset forecasts return
/// `None` so the caller defers their route. Reliable projection requires a
/// nonnegative burn rate, at least one hour of evidence, and a future reset;
/// all other known evidence uses the centered remaining-percent fallback.
/// Mirrors Python's `_window`.
fn window_explanation(forecast: &Forecast, now: f64) -> Option<WindowExplanation> {
    if !forecast.known {
        return None;
    }
    let remaining = forecast.remaining_percent?;
    if !remaining.is_finite() || !(0.0..=100.0).contains(&remaining) {
        return None;
    }
    let observed = forecast.observed_at?;
    if !observed.is_finite() || observed > now {
        return None;
    }
    let nonneg = |value: Option<f64>| -> Result<Option<f64>> {
        match value {
            None => Ok(None),
            Some(v) if v.is_finite() && v >= 0.0 => Ok(Some(v)),
            Some(_) => Err(invalid("negative or non-finite forecast field")),
        }
    };
    let burn = nonneg(forecast.burn_percent_per_hour).ok()?;
    let span = nonneg(forecast.burn_span_seconds).ok()?;
    let reset = nonneg(forecast.reset_at).ok()?;
    if reset.is_some_and(|r| r <= now) {
        return None;
    }
    let (projected, slack, marker) = match (burn, span, reset) {
        (Some(b), Some(s), Some(r)) if s >= 3600.0 => {
            let projected = remaining - b * ((r - now) / 3600.0);
            (
                Some(projected),
                (projected / 100.0).clamp(-1.0, 1.0),
                "projected",
            )
        }
        _ => {
            let slack = (2.0 * remaining / 100.0 - 1.0).clamp(-1.0, 1.0);
            let marker = if reset.is_none() {
                "no_reset"
            } else if burn.is_none() || forecast.warmup {
                "warmup"
            } else {
                "thin_evidence"
            };
            (None, slack, marker)
        }
    };
    Some(WindowExplanation {
        key: forecast.key.clone(),
        remaining_percent: remaining,
        burn_percent_per_hour: burn,
        burn_span_seconds: span,
        reset_at: reset,
        projected_percent: projected,
        slack,
        marker: marker.into(),
        risk: forecast.risk.clone(),
    })
}

fn nonblank(name: &str, value: &str) -> Result<()> {
    if value.trim().is_empty() {
        return Err(invalid(format!("{name} must be nonblank")));
    }
    Ok(())
}

fn positive_finite(name: &str, value: f64) -> Result<f64> {
    if !value.is_finite() || value <= 0.0 {
        return Err(invalid(format!("{name} must be a positive finite number")));
    }
    Ok(value)
}

/// Rank capacity routes without provider calls, role selection, or writes.
///
/// `routes` must contain validated fresh routes; `snapshot_evidence` carries
/// pre-ranking deferred/unavailable evidence from the topology join (both
/// collapse into the returned `deferred` list, matching Python's
/// `_snapshot_evidence`). `multipliers` maps opaque runtime names to positive
/// finite factors and defaults missing names to `1.0`. `now` is one finite
/// nonnegative epoch used for every projection. Unknown or malformed evidence
/// is deferred; any zero governing window is omitted before scoring. The
/// result is a total deterministic order independent of input iteration
/// order.
///
/// `route_multipliers` maps `(runtime, route_id)` pairs to absolute effective
/// factors, preventing same-named route ids from crossing runtime
/// boundaries. Aliases sharing runtime and pool ids collapse using their
/// maximum factor; the canonical alias is ordered by factor descending then
/// route id. Exhausted choices are never revived by either multiplier. A
/// known reset credit count adds only `1 + n/(n+1)` after eligibility;
/// aliases use the maximum count once.
///
/// A non-finite priority (large-weight overflow) defers that
/// route with reason `priority_overflow` rather than letting `Infinity`
/// enter the sort or the JSON projection.
pub fn rank_capacity_routes(
    routes: Vec<RouteInput>,
    snapshot_evidence: Vec<OrderEvidence>,
    multipliers: &BTreeMap<String, f64>,
    route_multipliers: &BTreeMap<(String, String), f64>,
    now: f64,
) -> Result<CapacityOrder> {
    if !now.is_finite() || now < 0.0 {
        return Err(invalid("now must be finite and nonnegative"));
    }
    for (runtime, value) in multipliers {
        nonblank("multiplier runtime", runtime)?;
        positive_finite("multiplier", *value)?;
    }
    for ((runtime, route_id), value) in route_multipliers {
        nonblank("route multiplier runtime", runtime)?;
        nonblank("route multiplier route_id", route_id)?;
        positive_finite("route multiplier", *value)?;
    }

    let mut deferred: Vec<OrderEvidence> = snapshot_evidence;
    let mut seen_runtimes: BTreeSet<String> = deferred.iter().map(|e| e.runtime.clone()).collect();
    let mut omitted: Vec<OmittedRoute> = Vec::new();

    let mut grouped: BTreeMap<(String, Vec<String>), Vec<RouteInput>> = BTreeMap::new();
    for route in routes {
        let runtime = route.descriptor.runtime.clone();
        let mut pool_ids: Vec<String> = route.pools.iter().map(|p| p.pool_id.clone()).collect();
        pool_ids.sort();
        seen_runtimes.insert(runtime.clone());
        grouped.entry((runtime, pool_ids)).or_default().push(route);
    }

    let effective_factor = |runtime: &str, route_id: &str| -> f64 {
        route_multipliers
            .get(&(runtime.to_string(), route_id.to_string()))
            .copied()
            .unwrap_or_else(|| multipliers.get(runtime).copied().unwrap_or(1.0))
    };

    let mut ranked: Vec<RankedRoute> = Vec::new();
    let mut available_runtimes: BTreeSet<String> = BTreeSet::new();
    for ((runtime, pool_ids), values) in grouped {
        let mut by_id: HashMap<String, Route> = HashMap::new();
        for value in &values {
            by_id.insert(value.descriptor.route_id.clone(), value.descriptor.clone());
        }
        let mut aliases: Vec<Route> = by_id.into_values().collect();
        aliases.sort_by(|a, b| {
            effective_factor(&runtime, &b.route_id)
                .total_cmp(&effective_factor(&runtime, &a.route_id))
                .then_with(|| a.route_id.cmp(&b.route_id))
        });
        let canonical = values
            .iter()
            .min_by(|a, b| a.descriptor.route_id.cmp(&b.descriptor.route_id))
            .expect("group is nonempty");

        let mut windows: Vec<WindowExplanation> = Vec::new();
        let mut malformed = false;
        for forecast in &canonical.forecasts {
            match window_explanation(forecast, now) {
                Some(explanation) => windows.push(explanation),
                None => {
                    malformed = true;
                    break;
                }
            }
        }
        windows.sort_by(|a, b| key_order(&a.key).cmp(&key_order(&b.key)));

        if malformed || windows.is_empty() {
            deferred.push(OrderEvidence {
                runtime: runtime.clone(),
                scope_id: None,
                route_id: Some(aliases[0].route_id.clone()),
                reason: "ranker_unknown_forecast".into(),
                detail: "forecast is unknown, stale, or malformed".into(),
            });
            continue;
        }

        let exhausted: Vec<&WindowExplanation> = windows
            .iter()
            .filter(|w| w.remaining_percent == 0.0)
            .collect();
        if !exhausted.is_empty() {
            let limiting = exhausted
                .into_iter()
                .min_by(|a, b| {
                    (
                        a.reset_at.is_none(),
                        a.reset_at.unwrap_or(f64::INFINITY),
                        key_order(&a.key),
                    )
                        .partial_cmp(&(
                            b.reset_at.is_none(),
                            b.reset_at.unwrap_or(f64::INFINITY),
                            key_order(&b.key),
                        ))
                        .expect("reset_at compared against infinity is always ordered")
                })
                .expect("nonempty exhausted list");
            omitted.push(OmittedRoute {
                runtime: runtime.clone(),
                aliases,
                pool_ids,
                limiting_key: limiting.key.clone(),
                limiting_reset_at: limiting.reset_at,
                reason: "exhausted".into(),
            });
            available_runtimes.insert(runtime);
            continue;
        }

        let limiting = windows
            .iter()
            .min_by(|a, b| {
                (
                    a.slack,
                    a.reset_at.is_none(),
                    a.reset_at.unwrap_or(f64::INFINITY),
                    key_order(&a.key),
                )
                    .partial_cmp(&(
                        b.slack,
                        b.reset_at.is_none(),
                        b.reset_at.unwrap_or(f64::INFINITY),
                        key_order(&b.key),
                    ))
                    .expect("slack and reset_at are always finite-comparable here")
            })
            .expect("nonempty windows list");
        let score = 1.0 + limiting.slack;
        let multiplier = aliases
            .iter()
            .map(|a| effective_factor(&runtime, &a.route_id))
            .fold(f64::NEG_INFINITY, f64::max);
        let known_credits: Vec<u64> = aliases.iter().filter_map(|a| a.reset_credits).collect();
        let max_credits = known_credits.iter().copied().max().unwrap_or(0);
        let reset_credit_multiplier = 1.0 + (max_credits as f64) / ((max_credits as f64) + 1.0);
        let priority = score * multiplier * reset_credit_multiplier;
        let minimum_remaining_percent = windows
            .iter()
            .map(|w| w.remaining_percent)
            .fold(f64::INFINITY, f64::min);

        if !priority.is_finite() {
            deferred.push(OrderEvidence {
                runtime: runtime.clone(),
                scope_id: None,
                route_id: Some(aliases[0].route_id.clone()),
                reason: "priority_overflow".into(),
                detail: "capacity priority overflowed a finite number".into(),
            });
            continue;
        }

        ranked.push(RankedRoute {
            runtime: runtime.clone(),
            limiting_key: limiting.key.clone(),
            limiting_reset_at: limiting.reset_at,
            aliases,
            pool_ids,
            score,
            multiplier,
            priority,
            minimum_remaining_percent,
            windows,
            reset_credits: if known_credits.is_empty() {
                None
            } else {
                Some(max_credits)
            },
            reset_credit_multiplier,
        });
        available_runtimes.insert(runtime);
    }

    ranked.sort_by(|a, b| {
        (
            -a.priority,
            -a.score,
            -a.minimum_remaining_percent,
            a.limiting_reset_at.is_none(),
            a.limiting_reset_at.unwrap_or(f64::INFINITY),
            &a.aliases[0].route_id,
        )
            .partial_cmp(&(
                -b.priority,
                -b.score,
                -b.minimum_remaining_percent,
                b.limiting_reset_at.is_none(),
                b.limiting_reset_at.unwrap_or(f64::INFINITY),
                &b.aliases[0].route_id,
            ))
            .expect("priority/score/remaining are finite and reset_at compares against infinity")
    });
    omitted.sort_by(|a, b| {
        (
            &a.runtime,
            &a.aliases[0].route_id,
            key_order(&a.limiting_key),
        )
            .cmp(&(
                &b.runtime,
                &b.aliases[0].route_id,
                key_order(&b.limiting_key),
            ))
    });
    deferred.sort();
    deferred.dedup();
    let unavailable_runtimes: Vec<String> = seen_runtimes
        .difference(&available_runtimes)
        .cloned()
        .collect();

    Ok(CapacityOrder {
        observed_at: now,
        insufficient_diversity: ranked.len() < 2,
        routes: ranked,
        deferred,
        omitted,
        unavailable_runtimes,
    })
}
