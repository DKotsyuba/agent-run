//! Read OmniRoute's current quota-cache projection without contacting a provider.
//!
//! The live reader executes a fixed read-only query inside the local OmniRoute
//! container.  Parsing is intentionally separate so tests and callers can use
//! recorded, sanitized rows without Docker or credentials.

use super::{Key, Sample};
use crate::{domain::now, error::invalid, Error, Result};
use serde_json::Value;
use std::path::Path;

/// The maximum age of a current-cache observation before it becomes unknown.
pub const STALE_SECONDS: f64 = 5_400.0;
const MAX_ROWS: usize = 64;

/// One validated member reading: remaining percent, optional reset, observation.
type Member = (f64, Option<f64>, f64);

/// Converts one bounded RFC-3339 timestamp to epoch seconds.
fn timestamp(value: Option<&Value>) -> Option<f64> {
    let text = value.and_then(Value::as_str)?;
    if text.len() > 64 {
        return None;
    }
    chrono::DateTime::parse_from_rfc3339(text)
        .ok()
        .map(|value| value.timestamp_millis() as f64 / 1000.0)
        .filter(|value| *value >= 0.0)
}

/// Maps OmniRoute's stable cache key to the capacity vocabulary.
fn window(value: Option<&Value>) -> Option<&'static str> {
    match value.and_then(Value::as_str) {
        Some("session") => Some("session_5h"),
        Some("weekly") => Some("weekly"),
        Some("mcp_monthly") => Some("mcp_monthly"),
        _ => None,
    }
}

/// Builds current OmniRoute pool samples from sanitized cache rows.
///
/// Rows must have `window_key`, finite `remaining_percentage`, `fetched_at`,
/// and an optional valid `next_reset_at`. Unknown window keys are ignored;
/// malformed known rows and an overflow are explicit failures. Stale, future,
/// or expired rows become an unknown sample instead of a fabricated zero.
pub fn samples(rows: &Value, at: f64) -> Result<Vec<Sample>> {
    if !at.is_finite() || at < 0.0 {
        return Err(invalid("omniroute_invalid_now"));
    }
    let rows = rows
        .as_array()
        .ok_or_else(|| Error::Runtime("omniroute_unavailable".into()))?;
    if rows.len() > MAX_ROWS {
        return Err(Error::Runtime("omniroute_result_overflow".into()));
    }
    let mut pools: std::collections::BTreeMap<&str, Vec<Member>> =
        std::collections::BTreeMap::new();
    for row in rows {
        let Some(row) = row.as_object() else {
            return Err(Error::Runtime("omniroute_malformed_data".into()));
        };
        let Some(name) = window(row.get("window_key")) else {
            continue;
        };
        let remaining = row
            .get("remaining_percentage")
            .and_then(Value::as_f64)
            .filter(|value| value.is_finite());
        let observed = timestamp(row.get("fetched_at"));
        let reset_value = row.get("next_reset_at");
        let reset = timestamp(reset_value);
        if remaining.is_none()
            || observed.is_none()
            || reset_value.is_some_and(|value| !value.is_null()) && reset.is_none()
        {
            return Err(Error::Runtime("omniroute_malformed_data".into()));
        }
        pools.entry(name).or_default().push((
            remaining.unwrap_or_default(),
            reset,
            observed.unwrap_or_default(),
        ));
    }
    let mut result = Vec::new();
    for (window, members) in pools {
        let observed = members
            .iter()
            .map(|(_, _, observed)| *observed)
            .fold(f64::INFINITY, f64::min);
        let reset = members
            .iter()
            .filter_map(|(_, reset, _)| *reset)
            .min_by(f64::total_cmp);
        let mean = members
            .iter()
            .map(|(remaining, _, _)| remaining)
            .sum::<f64>()
            / members.len() as f64;
        let unknown = observed > at
            || at - observed > STALE_SECONDS
            || reset.is_some_and(|value| value <= at);
        result.push(Sample {
            key: Key {
                runtime: String::new(),
                lane: "pool".into(),
                window: window.into(),
                target: Some("opencode-go:pool".into()),
                source: if unknown {
                    "unknown".into()
                } else {
                    "omniroute_quota_pool".into()
                },
            },
            remaining_percent: (!unknown).then_some(mean.clamp(0.0, 100.0)),
            reset_at: reset,
            observed_at: Some(observed),
            valid_until: Some(observed + STALE_SECONDS),
        });
    }
    Ok(result)
}

/// Reads the OmniRoute current-cache from its local container and parses it.
///
/// The command is a local, read-only Docker invocation with a ten second
/// bound. Failure, non-JSON output, and malformed rows return a fixed reason;
/// no command output or connection identity is exposed.
pub async fn read(
    capture: impl std::future::Future<Output = Result<Vec<u8>>>,
) -> Result<Vec<Sample>> {
    let body = capture
        .await
        .map_err(|_| Error::Runtime("omniroute_unavailable".into()))?;
    let rows: Value = serde_json::from_slice(&body)
        .map_err(|_| Error::Runtime("omniroute_unavailable".into()))?;
    samples(&rows, now())
}

/// Returns the fixed JavaScript reader used inside the OmniRoute container.
///
/// It emits only sanitized current-cache fields and deliberately never emits
/// connection identifiers, raw cache documents, or database diagnostics.
pub fn script() -> String {
    concat!(
        "const D=require('/app/node_modules/better-sqlite3');",
        "const d=new D('/app/data/storage.sqlite',{readonly:true});",
        "const q=\"SELECT kv.value v FROM provider_connections c LEFT JOIN key_value kv ON kv.namespace='providerLimitsCache' AND kv.key=c.id WHERE c.provider='opencode-go' AND c.is_active=1 AND c.quota_visible=1 LIMIT 65\";",
        "let o=[];for(const r of d.prepare(q).all()){let x={};try{x=JSON.parse(r.v)}catch(_){ }",
        "for(const k of ['session','weekly','mcp_monthly']){let z=x&&x.quotas&&x.quotas[k];o.push({window_key:k,remaining_percentage:z&&z.remainingPercentage,next_reset_at:z&&z.resetAt,fetched_at:x&&x.fetchedAt});if(o.length>64)break}if(o.length>64)break}",
        "console.log(JSON.stringify(o));"
    ).into()
}

/// Returns the fixed local Docker executable path used by OmniRoute.
pub fn docker() -> &'static Path {
    Path::new("/Users/pluto/.orbstack/bin/docker")
}
