//! Reset-cycle recognition: samples of one shared reset instant must supply
//! burn evidence together despite provider-reported sub-second jitter.
use agent_run_core::capacity::{forecast, Key, Sample};

fn key() -> Key {
    Key {
        runtime: "codex".into(),
        lane: "requests".into(),
        window: "5h".into(),
        target: Some("gpt-5.6-sol".into()),
        source: "app_server".into(),
    }
}

/// Builds a normalized sample with an unbounded (`None`) validity window, as
/// the Python reset-identity fixtures do throughout.
fn sample(remaining: f64, reset_at: Option<f64>, observed_at: Option<f64>) -> Sample {
    Sample {
        key: key(),
        remaining_percent: Some(remaining),
        reset_at,
        observed_at,
        valid_until: None,
    }
}

/// Four live Claude weekly-cycle captures of the same shared reset: every
/// capture carried a slightly different float epoch second around
/// 1788469200 (spread 0.97052s), straddling the integer-second boundary in
/// both directions.
const JITTERED_RESETS: [f64; 4] = [
    1788469200.198643,
    1788469200.164472,
    1788469200.50504,
    1788469199.53452,
];

/// Mirrors `tests/test_capacity_reset_identity.py::test_jittered_resets_across_an_hour_supply_burn_evidence`.
#[test]
fn jittered_resets_across_an_hour_supply_burn_evidence() {
    let now = JITTERED_RESETS[0] - 7200.0;
    let observations = [now, now - 1200.0, now - 2400.0, now - 3600.0];
    let remainings = [40.0, 55.0, 57.5, 60.0];
    let samples: Vec<Sample> = (0..4)
        .map(|i| {
            sample(
                remainings[i],
                Some(JITTERED_RESETS[i]),
                Some(observations[i]),
            )
        })
        .collect();
    let f = forecast(&key(), &samples, now);
    assert!(f.known);
    assert!(!f.warmup);
    assert_eq!(f.reset_at, Some(JITTERED_RESETS[0]));
    assert_eq!(f.burn_percent_per_hour, Some(20.0));
    assert_eq!(f.burn_span_seconds, Some(3600.0));
    assert_eq!(
        f.sustainable_percent_per_hour,
        Some(40.0 / ((JITTERED_RESETS[0] - now) / 3600.0))
    );
    assert_eq!(f.risk, "low");
}

/// Mirrors `tests/test_capacity_reset_identity.py::test_jitter_under_tolerance_escalates_once_the_span_gate_opens`.
#[test]
fn jitter_under_tolerance_escalates_once_the_span_gate_opens() {
    let now = JITTERED_RESETS[0] - 48.0 * 3600.0;
    let samples = vec![
        sample(88.0, Some(JITTERED_RESETS[0]), Some(now)),
        sample(100.0, Some(JITTERED_RESETS[2]), Some(now - 2.0 * 3600.0)),
    ];
    let f = forecast(&key(), &samples, now);
    assert!(!f.warmup);
    assert_eq!(f.burn_percent_per_hour, Some(6.0));
    assert_eq!(f.burn_span_seconds, Some(7200.0));
    assert_eq!(f.risk, "high");
}

/// Mirrors `tests/test_capacity_reset_identity.py::test_young_jittered_history_stays_below_the_span_gate`.
#[test]
fn young_jittered_history_stays_below_the_span_gate() {
    let now = JITTERED_RESETS[0] - 7200.0;
    let samples = vec![
        sample(99.0, Some(JITTERED_RESETS[0]), Some(now)),
        sample(100.0, Some(JITTERED_RESETS[1]), Some(now - 600.0)),
    ];
    let f = forecast(&key(), &samples, now);
    assert!(!f.warmup);
    assert_eq!(f.burn_percent_per_hour, Some(6.0));
    assert_eq!(f.burn_span_seconds, Some(600.0));
    assert_eq!(f.risk, "low");
}

/// Mirrors `tests/test_capacity_reset_identity.py::test_reset_already_passed_at_latest_observation_stays_separate`.
#[test]
fn reset_already_passed_at_latest_observation_stays_separate() {
    let now = 1788469200.2;
    let samples = vec![
        sample(90.0, Some(JITTERED_RESETS[2]), Some(now)),
        sample(5.0, Some(JITTERED_RESETS[3]), Some(now - 3600.0)),
    ];
    let f = forecast(&key(), &samples, now);
    assert!(f.known);
    assert!(f.warmup);
    assert_eq!(f.burn_percent_per_hour, None);
    assert_eq!(f.burn_span_seconds, None);
    assert_eq!(f.reset_at, Some(JITTERED_RESETS[2]));
    assert_eq!(f.remaining_percent, Some(90.0));
}

/// Mirrors `tests/test_capacity_reset_identity.py::test_resets_over_tolerance_stay_separate`.
#[test]
fn resets_over_tolerance_stay_separate() {
    let now = JITTERED_RESETS[0];
    let samples = vec![
        sample(90.0, Some(JITTERED_RESETS[0]), Some(now)),
        sample(5.0, Some(1788469198.5), Some(now - 3600.0)),
    ];
    let f = forecast(&key(), &samples, now);
    assert!(f.warmup);
    assert_eq!(f.burn_percent_per_hour, None);
    assert_eq!(f.burn_span_seconds, None);
}

/// Mirrors `tests/test_capacity_reset_identity.py::test_exact_equal_resets_still_group`.
#[test]
fn exact_equal_resets_still_group() {
    let now = JITTERED_RESETS[0] - 7200.0;
    let reset_at = JITTERED_RESETS[0];
    let samples = vec![
        sample(40.0, Some(reset_at), Some(now)),
        sample(60.0, Some(reset_at), Some(now - 3600.0)),
    ];
    let f = forecast(&key(), &samples, now);
    assert!(!f.warmup);
    assert_eq!(f.burn_percent_per_hour, Some(20.0));
    assert_eq!(f.burn_span_seconds, Some(3600.0));
    assert_eq!(f.reset_at, Some(reset_at));
}

/// Mirrors `tests/test_capacity_reset_identity.py::test_none_reset_matches_only_none_reset`.
#[test]
fn none_reset_matches_only_none_reset() {
    let now = 1_000_000.0;
    let unnamed = vec![
        sample(40.0, None, Some(now)),
        sample(60.0, None, Some(now - 3600.0)),
    ];
    let unnamed_forecast = forecast(&key(), &unnamed, now);
    assert!(!unnamed_forecast.warmup);
    assert_eq!(unnamed_forecast.burn_percent_per_hour, Some(20.0));

    let mixed = vec![
        sample(40.0, None, Some(now)),
        sample(60.0, Some(JITTERED_RESETS[0]), Some(now - 3600.0)),
    ];
    let mixed_forecast = forecast(&key(), &mixed, now);
    assert!(mixed_forecast.warmup);
    assert_eq!(mixed_forecast.burn_percent_per_hour, None);
    assert_eq!(mixed_forecast.reset_at, None);
}

/// Mirrors `tests/test_capacity_reset_identity.py::test_missing_latest_observed_at_keeps_exact_only_grouping`.
#[test]
fn missing_latest_observed_at_keeps_exact_only_grouping() {
    let now = JITTERED_RESETS[0] - 7200.0;
    let samples = vec![
        sample(40.0, Some(JITTERED_RESETS[0]), None),
        sample(60.0, Some(JITTERED_RESETS[1]), Some(now - 3600.0)),
    ];
    let f = forecast(&key(), &samples, now);
    assert!(f.known);
    assert!(f.warmup);
    assert_eq!(f.burn_percent_per_hour, None);
}

/// Mirrors `tests/test_capacity_reset_identity.py::test_stale_and_future_latest_stay_unknown_despite_jitter`.
#[test]
fn stale_and_future_latest_stay_unknown_despite_jitter() {
    let now = JITTERED_RESETS[0] - 7200.0;
    let stale = vec![
        Sample {
            key: key(),
            remaining_percent: Some(40.0),
            reset_at: Some(JITTERED_RESETS[0]),
            observed_at: Some(now),
            valid_until: Some(now - 1.0),
        },
        sample(60.0, Some(JITTERED_RESETS[1]), Some(now - 3600.0)),
    ];
    let stale_forecast = forecast(&key(), &stale, now);
    assert!(!stale_forecast.known);
    assert_eq!(stale_forecast.risk, "unknown");

    let future = vec![
        sample(40.0, Some(JITTERED_RESETS[0]), Some(now + 60.0)),
        sample(60.0, Some(JITTERED_RESETS[1]), Some(now - 3600.0)),
    ];
    let future_forecast = forecast(&key(), &future, now);
    assert!(!future_forecast.known);
}

/// Mirrors `tests/test_capacity_reset_identity.py::test_past_reset_at_latest_stays_unknown`.
#[test]
fn past_reset_at_latest_stays_unknown() {
    let now = JITTERED_RESETS[0];
    let samples = vec![
        sample(40.0, Some(now - 0.5), Some(now)),
        sample(60.0, Some(now - 0.9), Some(now - 3600.0)),
    ];
    let f = forecast(&key(), &samples, now);
    assert!(!f.known);
}
