//! Advisory recommendations and a noise-tolerant semantic capacity key.
//!
//! Port of `agent_run.capacity.advice`. Advice is always advisory: an
//! explicit owner choice wins regardless of risk. `advice_key` intentionally
//! drops high-resolution timestamps so it changes only on material capacity
//! state, not on every collection tick.
use super::{Forecast, Key};
use sha2::{Digest, Sha256};

const REMAINING_BUCKET_PERCENT: f64 = 5.0;
const RESET_BUCKET_SECONDS: i64 = 300;

/// One forecast turned into owner-facing advice.
#[derive(Debug, Clone, PartialEq)]
pub struct CapacityAdvice {
    pub key: Key,
    pub known: bool,
    pub remaining_percent: Option<f64>,
    pub reset_at: Option<f64>,
    pub warmup: bool,
    pub risk: String,
    pub recommendations: Vec<String>,
}

pub fn build_advice(forecasts: &[Forecast]) -> Vec<CapacityAdvice> {
    forecasts
        .iter()
        .map(|f| CapacityAdvice {
            key: f.key.clone(),
            known: f.known,
            remaining_percent: f.remaining_percent,
            reset_at: f.reset_at,
            warmup: f.warmup,
            risk: f.risk.clone(),
            recommendations: recommendations(f),
        })
        .collect()
}

pub fn capacity_label(key: &Key) -> String {
    let mut parts = vec![format!("{}/{}", key.runtime, key.lane), key.window.clone()];
    if let Some(target) = &key.target {
        if !target.is_empty() {
            parts.push(format!("target={target}"));
        }
    }
    if !key.source.is_empty() {
        parts.push(format!("source={}", key.source));
    }
    parts.join(" ")
}

fn recommendations(forecast: &Forecast) -> Vec<String> {
    let label = capacity_label(&forecast.key);
    match forecast.risk.as_str() {
        "unknown" => vec![format!(
            "{label} capacity is unknown; treat limits as unverified."
        )],
        "high" => vec![format!(
            "{label} is near exhaustion; avoid starting new {} work before reset.",
            forecast.key.lane
        )],
        "medium" => vec![format!(
            "{label} is trending toward exhaustion; pace new requests."
        )],
        _ => Vec::new(),
    }
}

fn bucketed_remaining(remaining: Option<f64>) -> Option<i64> {
    remaining.map(|r| (r / REMAINING_BUCKET_PERCENT).round() as i64)
}

fn bucketed_reset(reset_at: Option<f64>) -> Option<i64> {
    reset_at.map(|r| (r / RESET_BUCKET_SECONDS as f64).floor() as i64)
}

fn sort_key(advice: &CapacityAdvice) -> (&str, &str, &str, &str, &str) {
    (
        &advice.key.runtime,
        &advice.key.lane,
        &advice.key.window,
        advice.key.target.as_deref().unwrap_or(""),
        &advice.key.source,
    )
}

/// A stable, bounded hash of every advisory item's material state.
///
/// Mirrors Python's `advice_key`: sorts by identity, buckets remaining
/// percent and reset time to absorb collection-tick jitter, and truncates
/// the digest to 32 hex characters.
pub fn advice_key(items: &[CapacityAdvice]) -> String {
    let mut ordered: Vec<&CapacityAdvice> = items.iter().collect();
    ordered.sort_by(|a, b| sort_key(a).cmp(&sort_key(b)));
    let parts: Vec<String> = ordered
        .iter()
        .map(|item| {
            let key = &item.key;
            let remaining = bucketed_remaining(item.remaining_percent)
                .map(|v| v.to_string())
                .unwrap_or_else(|| "None".into());
            let reset = bucketed_reset(item.reset_at)
                .map(|v| v.to_string())
                .unwrap_or_else(|| "None".into());
            [
                key.runtime.as_str(),
                key.lane.as_str(),
                key.window.as_str(),
                key.target.as_deref().unwrap_or(""),
                key.source.as_str(),
                item.risk.as_str(),
                if item.warmup { "warmup" } else { "steady" },
                remaining.as_str(),
                reset.as_str(),
            ]
            .join("|")
        })
        .collect();
    let digest = Sha256::digest(parts.join("::").as_bytes());
    format!("{digest:x}")[..32].to_string()
}
