//! Bounded, isolated Codex rollout rate-limit evidence.

use chrono::{DateTime, Utc};
use serde_json::Value;
use std::{
    cmp::Reverse,
    fs,
    path::{Path, PathBuf},
    time::SystemTime,
};

/// Maximum age at which a rollout observation remains usable.
pub const STALE_SECONDS: f64 = 900.0;
/// Maximum number of newest rollout files inspected per query.
pub const MAX_ROLLOUT_FILES: usize = 24;
/// Maximum byte tail read from one rollout file.
pub const ROLLOUT_TAIL_BYTES: u64 = 262_144;
/// Maximum complete lines retained from one rollout tail.
pub const ROLLOUT_TAIL_LINES: usize = 2_048;

/// One normalized rate-limit observation suitable for capacity persistence.
#[derive(Debug, Clone, PartialEq)]
pub struct LimitSample {
    /// Provider-specific quota lane, such as `primary` or `secondary`.
    pub lane: String,
    /// Stable normalized window name.
    pub window: String,
    /// Remaining percentage, or `None` when the observation is unknown.
    pub remaining_percent: Option<f64>,
    /// Provider reset time, when it is finite and representable.
    pub reset_at: Option<DateTime<Utc>>,
    /// Time at which the provider emitted the observation.
    pub observed_at: Option<DateTime<Utc>>,
    /// Evidence source, either `rollout_evidence` or an unknown marker.
    pub source: String,
    /// Configured model target, when the rollout names one.
    pub target: Option<String>,
    /// Evidence shelf life in seconds.
    pub valid_for_seconds: Option<u64>,
}

/// Returns an epoch timestamp only for finite JSON numbers in a safe range.
fn epoch(value: Option<&Value>) -> Option<f64> {
    let value = value?.as_f64()?;
    value.is_finite().then_some(value)
}

/// Converts a JSON epoch or RFC3339 timestamp to UTC without trusting text.
fn timestamp(value: Option<&Value>) -> Option<DateTime<Utc>> {
    if let Some(epoch) = epoch(value) {
        return DateTime::from_timestamp(epoch as i64, (epoch.fract() * 1e9) as u32);
    }
    value?
        .as_str()
        .filter(|text| text.len() <= 64)
        .and_then(|text| DateTime::parse_from_rfc3339(text).ok())
        .map(|value| value.with_timezone(&Utc))
}

/// Returns the Python-compatible name for one provider window duration.
fn window_name(lane: &str, minutes: Option<f64>) -> String {
    if lane == "individual_limit" {
        "model_weekly".into()
    } else if minutes == Some(300.0) {
        "session_5h".into()
    } else if minutes == Some(10080.0) {
        "weekly".into()
    } else {
        "unknown".into()
    }
}

/// Reads only the bounded UTF-8 tail and returns its final complete lines.
fn tail_lines(path: &Path) -> Option<Vec<String>> {
    let mut file = fs::File::open(path).ok()?;
    let end = file.metadata().ok()?.len();
    let start = end.saturating_sub(ROLLOUT_TAIL_BYTES);
    use std::io::{Read, Seek, SeekFrom};
    file.seek(SeekFrom::Start(start)).ok()?;
    let mut bytes = Vec::new();
    file.take(ROLLOUT_TAIL_BYTES).read_to_end(&mut bytes).ok()?;
    if start > 0 {
        if let Some(newline) = bytes.iter().position(|byte| *byte == b'\n') {
            bytes.drain(..=newline);
        }
    }
    let text = String::from_utf8(bytes).ok()?;
    Some(
        text.lines()
            .map(str::to_owned)
            .rev()
            .take(ROLLOUT_TAIL_LINES)
            .collect(),
    )
}

/// Enumerates only regular rollout files beneath the queried home.
fn rollout_paths(home: &Path) -> Vec<(SystemTime, PathBuf)> {
    let sessions = home.join("sessions");
    if fs::symlink_metadata(&sessions)
        .map(|meta| meta.file_type().is_symlink())
        .unwrap_or(true)
    {
        return Vec::new();
    }
    let mut paths = Vec::new();
    let Ok(years) = fs::read_dir(sessions) else {
        return paths;
    };
    for year in years.flatten() {
        let Ok(months) = fs::read_dir(year.path()) else {
            continue;
        };
        for month in months.flatten() {
            let Ok(days) = fs::read_dir(month.path()) else {
                continue;
            };
            for day in days.flatten() {
                let Ok(files) = fs::read_dir(day.path()) else {
                    continue;
                };
                for file in files.flatten() {
                    let path = file.path();
                    if !path
                        .file_name()
                        .and_then(|name| name.to_str())
                        .is_some_and(|name| {
                            name.starts_with("rollout-") && name.ends_with(".jsonl")
                        })
                    {
                        continue;
                    }
                    let Ok(meta) = fs::symlink_metadata(&path) else {
                        continue;
                    };
                    if meta.file_type().is_symlink() || !meta.file_type().is_file() {
                        continue;
                    }
                    if let Ok(modified) = meta.modified() {
                        paths.push((modified, path));
                    }
                }
            }
        }
    }
    paths.sort_by_key(|(modified, path)| (Reverse(*modified), Reverse(path.clone())));
    paths.truncate(MAX_ROLLOUT_FILES);
    paths
}

/// Converts one token-count event into normalized rate-limit samples.
fn event_samples(event: &Value, models: &[String], now: f64) -> Option<Vec<LimitSample>> {
    let observed = timestamp(event.get("timestamp"))?;
    let payload = event.get("payload")?.as_object()?;
    if payload.get("type")?.as_str()? != "token_count" {
        return None;
    }
    let limits = payload.get("rate_limits")?.as_object()?;
    let stale = now - observed.timestamp() as f64 > STALE_SECONDS;
    let mut samples = Vec::new();
    for lane in ["primary", "secondary", "individual_limit"] {
        let Some(window) = limits.get(lane).and_then(Value::as_object) else {
            continue;
        };
        let used = epoch(window.get("used_percent"));
        let remaining = if stale {
            None
        } else {
            used.map(|value| (100.0 - value).clamp(0.0, 100.0))
        };
        let target = ["target", "model", "limit_name"].iter().find_map(|key| {
            window
                .get(*key)
                .and_then(Value::as_str)
                .filter(|value| models.iter().any(|model| model == value))
                .map(str::to_owned)
        });
        samples.push(LimitSample {
            lane: lane.into(),
            window: window_name(lane, epoch(window.get("window_minutes"))),
            remaining_percent: remaining,
            reset_at: timestamp(window.get("resets_at")),
            observed_at: Some(observed),
            source: if stale {
                "unknown"
            } else {
                "isolated_rollout_evidence"
            }
            .into(),
            target,
            valid_for_seconds: Some(STALE_SECONDS as u64),
        });
    }
    (!samples.is_empty()).then_some(samples)
}

/// Normalizes non-standard JSON numeric spellings accepted by Python's parser.
fn accept_python_nonfinite(text: &str) -> String {
    text.replace("NaN", "null")
        .replace("Infinity", "null")
        .replace("-null", "null")
}

/// Reads one newest valid rollout event from the isolated Codex home.
pub fn rollout_limits(home: &Path, models: &[String], now: f64) -> Vec<LimitSample> {
    for (_, path) in rollout_paths(home) {
        let Some(lines) = tail_lines(&path) else {
            continue;
        };
        for line in lines {
            if !line.contains("\"rate_limits\"") || !line.contains("\"token_count\"") {
                continue;
            }
            let Ok(event) = serde_json::from_str::<Value>(&accept_python_nonfinite(&line)) else {
                continue;
            };
            if let Some(samples) = event_samples(&event, models, now) {
                return samples;
            }
        }
    }
    Vec::new()
}

/// Reads precomputed evidence, falling back to bounded rollout evidence when invalid.
pub fn limits(home: &Path, models: &[String], now: f64) -> Vec<LimitSample> {
    let path = home.join("cache/rollout_evidence.json");
    if let Ok(text) = fs::read_to_string(path) {
        if let Ok(payload) = serde_json::from_str::<Value>(&accept_python_nonfinite(&text)) {
            if let Some(items) = payload.get("samples").and_then(Value::as_array) {
                let mut samples = Vec::new();
                for item in items.iter().filter_map(Value::as_object) {
                    let (Some(lane), Some(window)) = (
                        item.get("lane").and_then(Value::as_str),
                        item.get("window").and_then(Value::as_str),
                    ) else {
                        continue;
                    };
                    let observed_at = timestamp(item.get("observed_at"));
                    let stale = observed_at
                        .is_none_or(|value| now - value.timestamp() as f64 > STALE_SECONDS);
                    let remaining = if stale {
                        None
                    } else {
                        epoch(item.get("remaining_percent"))
                            .filter(|value| (0.0..=100.0).contains(value))
                    };
                    samples.push(LimitSample {
                        lane: lane.into(),
                        window: window.into(),
                        remaining_percent: remaining,
                        reset_at: timestamp(item.get("reset_at")),
                        observed_at,
                        source: if stale { "unknown" } else { "rollout_evidence" }.into(),
                        target: item
                            .get("target")
                            .and_then(Value::as_str)
                            .map(str::to_owned),
                        valid_for_seconds: item.get("valid_for_seconds").and_then(Value::as_u64),
                    });
                }
                if !samples.is_empty() {
                    return samples;
                }
            }
        }
    }
    rollout_limits(home, models, now)
}
