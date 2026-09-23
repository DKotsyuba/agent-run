mod common;
use agent_run_core::capacity::{
    self, persist,
    ranking::{self, RouteInput},
    Forecast, Key, Pool, Route, Sample, Slice, Topology,
};
use agent_run_domain::domain;
use serde_json::json;
use std::collections::{BTreeMap, BTreeSet};
fn key() -> Key {
    Key {
        runtime: "mock".into(),
        lane: "standard".into(),
        window: "five_hour".into(),
        target: None,
        source: "test".into(),
    }
}
fn sample(remaining: f64, observed: f64, reset: f64) -> Sample {
    Sample {
        key: key(),
        remaining_percent: Some(remaining),
        reset_at: Some(reset),
        observed_at: Some(observed),
        valid_until: Some(observed + 10000.0),
    }
}

/// Builds a forecast fixture with independently optional evidence bounds.
fn forecast_sample(
    remaining: Option<f64>,
    reset_at: Option<f64>,
    observed_at: Option<f64>,
    valid_until: Option<f64>,
) -> Sample {
    Sample {
        key: key(),
        remaining_percent: remaining,
        reset_at,
        observed_at,
        valid_until,
    }
}
fn route(id: &str, account: Option<&str>) -> Route {
    Route {
        route_id: id.into(),
        runtime: "mock".into(),
        account: account.map(str::to_owned),
        quota_lane: "standard".into(),
        pool_ids: vec!["pool".into()],
        reset_credits: None,
    }
}
fn response() -> serde_json::Value {
    json!({"accountId":"ephemeral-do-not-persist","rateLimitResetCredits":{"availableCount":3},"rateLimitsByLimitId":{"codex":{"primary":{"usedPercent":25,"windowDurationMins":300,"resetsAt":20000},"secondary":{"usedPercent":10,"windowDurationMins":10080,"resetsAt":100000}}}})
}

/// Mirrors `tests/test_state_store.py::StateStoreTests::test_capacity_samples_and_route_snapshots_are_atomic_and_isolated`.
#[test]
fn capacity_samples_and_route_snapshots_commit_as_one_batch() {
    let home = tempfile::tempdir().unwrap();
    agent_run_store::Store::initialize(home.path()).unwrap();
    let key = key();
    let topology = Topology {
        pools: vec![Pool {
            pool_id: "pool".into(),
            keys: [key.clone()].into_iter().collect(),
        }],
        routes: vec![route("route", None)],
    };
    let slice = |runtime: &str, observed_at: f64| Slice {
        runtime: runtime.into(),
        scope_id: "main".into(),
        samples: vec![Sample {
            key: Key {
                runtime: runtime.into(),
                ..key.clone()
            },
            remaining_percent: Some(50.0),
            reset_at: None,
            observed_at: Some(observed_at),
            valid_until: Some(observed_at + 10.0),
        }],
        topology: topology.clone(),
        observed_at,
        valid_until: observed_at + 10.0,
    };
    persist(home.path(), &slice("mock", 1.0), 10).unwrap();
    let mut broken = slice("mock", 2.0);
    broken.samples.push(broken.samples[0].clone());
    assert!(persist(home.path(), &broken, 10).is_err());
    let mut invalid_sample = slice("mock", 3.0);
    invalid_sample.samples[0].remaining_percent = Some(101.0);
    assert!(persist(home.path(), &invalid_sample, 10).is_err());
    let mut expired = slice("mock", 4.0);
    expired.valid_until = 3.0;
    assert!(persist(home.path(), &expired, 10).is_err());
    let store = agent_run_store::Store::open(home.path()).unwrap();
    assert_eq!(
        store
            .conn
            .query_row("SELECT COUNT(*) FROM capacity_samples", [], |row| row
                .get::<_, i64>(0))
            .unwrap(),
        1
    );
    assert_eq!(store.conn.query_row("SELECT payload_json FROM capacity_route_snapshots WHERE runtime='mock' AND scope_id='main'", [], |row| row.get::<_, String>(0)).unwrap(), serde_json::to_string(&slice("mock", 1.0).topology).unwrap());
}

/// Mirrors `tests/test_state_store.py::StateStoreTests::test_capacity_sample_runtime_and_payload_constraints`.
#[test]
fn capacity_persistence_rejects_cross_runtime_and_oversized_payloads() {
    let home = tempfile::tempdir().unwrap();
    agent_run_store::Store::initialize(home.path()).unwrap();
    let mut cross_runtime = sample(50.0, 1.0, 100.0);
    cross_runtime.key.runtime = "other".into();
    let topology = Topology::default();
    let slice = Slice {
        runtime: "mock".into(),
        scope_id: "main".into(),
        samples: vec![cross_runtime],
        topology,
        observed_at: 1.0,
        valid_until: 2.0,
    };
    assert!(persist(home.path(), &slice, 10).is_err());
    let key = key();
    let oversized = Topology {
        pools: vec![Pool {
            pool_id: "pool".into(),
            keys: [key.clone()].into_iter().collect(),
        }],
        routes: (0..3_000)
            .map(|index| Route {
                route_id: format!("route-{index}"),
                ..route("route", None)
            })
            .collect(),
    };
    let oversized = Slice {
        runtime: "mock".into(),
        scope_id: "main".into(),
        samples: vec![Sample {
            key,
            remaining_percent: Some(50.0),
            reset_at: None,
            observed_at: Some(1.0),
            valid_until: Some(2.0),
        }],
        topology: oversized,
        observed_at: 1.0,
        valid_until: 2.0,
    };
    assert!(persist(home.path(), &oversized, 10).is_err());
}

/// Mirrors `tests/test_state_store.py::StateStoreTests::test_capacity_history_survives_successful_appends`.
#[test]
fn capacity_history_survives_snapshot_upserts() {
    let home = tempfile::tempdir().unwrap();
    agent_run_store::Store::initialize(home.path()).unwrap();
    let key = key();
    let topology = Topology {
        pools: vec![Pool {
            pool_id: "pool".into(),
            keys: [key.clone()].into_iter().collect(),
        }],
        routes: vec![route("route", None)],
    };
    for (remaining, observed_at) in [(50.0, 1.0), (25.0, 3.0)] {
        persist(
            home.path(),
            &Slice {
                runtime: "mock".into(),
                scope_id: "main".into(),
                samples: vec![Sample {
                    key: key.clone(),
                    remaining_percent: Some(remaining),
                    reset_at: None,
                    observed_at: Some(observed_at),
                    valid_until: Some(observed_at + 10.0),
                }],
                topology: topology.clone(),
                observed_at,
                valid_until: observed_at + 10.0,
            },
            10,
        )
        .unwrap();
    }
    let store = agent_run_store::Store::open(home.path()).unwrap();
    let rows: Vec<(f64, f64)> = store
        .conn
        .prepare(
            "SELECT remaining_percent,observed_at FROM capacity_samples ORDER BY observed_at DESC",
        )
        .unwrap()
        .query_map([], |row| Ok((row.get(0)?, row.get(1)?)))
        .unwrap()
        .collect::<rusqlite::Result<_>>()
        .unwrap();
    assert_eq!(rows, [(25.0, 3.0), (50.0, 1.0)]);
}
#[test]
fn nullable_account_identity_never_collides_with_a_label() {
    for label in ["base", "default", "shared", "a:b", "a/b", "@base"] {
        assert_ne!(
            capacity::account_token(None),
            capacity::account_token(Some(label))
        );
    }
    assert_eq!(capacity::account_token(Some("a:b")), "@a%3Ab");
}
#[test]
fn codex_normalizer_keeps_windows_and_credit_metadata() {
    let (slice, account) =
        capacity::sources::normalize_codex("mock", Some("base"), &response(), 1000.0).unwrap();
    assert_eq!(slice.samples.len(), 2);
    assert_eq!(slice.topology.routes.len(), 1);
    assert_eq!(slice.topology.routes[0].reset_credits, Some(3));
    assert_eq!(account.as_deref(), Some("ephemeral-do-not-persist"));
    assert!(!serde_json::to_string(&slice.topology)
        .unwrap()
        .contains("ephemeral-do-not-persist"));
}
#[test]
fn malformed_present_window_disables_the_whole_route() {
    let mut raw = response();
    raw["rateLimitsByLimitId"]["codex"]["secondary"]["usedPercent"] = json!(true);
    let (slice, _) = capacity::sources::normalize_codex("mock", None, &raw, 1000.0).unwrap();
    assert_eq!(slice.samples.len(), 1);
    assert!(slice.topology.routes.is_empty());
}
#[test]
fn codex_model_specific_bucket_does_not_inherit_standard_reset_credits() {
    let mut raw = response();
    raw["rateLimitsByLimitId"]["spark"] = json!({"limitName":"Spark","primary":{"usedPercent":0,"windowDurationMins":300,"resetsAt":20000}});
    let (slice, _) = capacity::sources::normalize_codex("mock", None, &raw, 1000.0).unwrap();
    assert_eq!(slice.topology.routes.len(), 2);
    let spark = slice
        .topology
        .routes
        .iter()
        .find(|r| r.quota_lane == "Spark")
        .unwrap();
    assert_eq!(spark.reset_credits, None);
}
#[test]
fn freshness_rejects_future_expired_reset_and_unknown_evidence() {
    let mut s = sample(80.0, 1000.0, 10000.0);
    assert!(s.fresh(1001.0));
    assert!(!s.fresh(999.0));
    assert!(!s.fresh(10000.0));
    s.valid_until = Some(1050.0);
    assert!(!s.fresh(1051.0));
    s.remaining_percent = None;
    assert!(!s.fresh(1001.0));
}
#[test]
fn reset_jitter_only_groups_still_open_windows() {
    let latest = sample(80.0, 1000.0, 2000.0);
    let near = sample(90.0, 900.0, 1999.5);
    assert!(capacity::same_cycle(&near, &latest));
    let rolled = sample(80.0, 2000.0, 2000.5);
    assert!(!capacity::same_cycle(&near, &rolled));
}
#[test]
fn burn_and_sustainable_rate_use_the_current_cycle() {
    let series = vec![
        sample(70.0, 4600.0, 11800.0),
        sample(90.0, 1000.0, 11800.0),
        sample(100.0, 900.0, 1000.0),
    ];
    let forecast = capacity::forecast(&key(), &series, 4600.0);
    assert_eq!(forecast.burn_percent_per_hour, Some(20.0));
    assert_eq!(forecast.burn_span_seconds, Some(3600.0));
    assert_eq!(forecast.sustainable_percent_per_hour, Some(35.0));
}

/// Mirrors `tests/test_capacity_forecast.py::CapacityForecastTests::test_future_observation_and_reset_boundary_are_unknown`.
#[test]
fn forecast_future_observation_and_reset_boundary_are_unknown() {
    let now = 1_000.0;
    for sample in [
        forecast_sample(
            Some(80.0),
            Some(now + 900.0),
            Some(now + 60.0),
            Some(now + 3_600.0),
        ),
        forecast_sample(Some(80.0), Some(now), Some(now - 60.0), Some(now + 900.0)),
    ] {
        assert!(!capacity::forecast(&key(), &[sample], now).known);
    }
}

/// Mirrors `tests/test_capacity_forecast.py::CapacityForecastTests::test_no_samples_is_unknown_and_never_blocks`.
#[test]
fn forecast_no_samples_is_unknown_and_never_blocks() {
    let forecast = capacity::forecast(&key(), &[], 1_000_000.0);
    assert_eq!(forecast.key, key());
    assert!(!forecast.known);
    assert_eq!(forecast.remaining_percent, None);
    assert!(forecast.warmup);
    assert_eq!(forecast.risk, "unknown");
}

/// Mirrors `tests/test_capacity_forecast.py::CapacityForecastTests::test_stale_only_sample_is_unknown`.
#[test]
fn forecast_stale_only_sample_is_unknown() {
    let now = 1_000_000.0;
    let sample = forecast_sample(Some(50.0), None, Some(now - 3_600.0), Some(now - 10.0));
    let forecast = capacity::forecast(&key(), &[sample], now);
    assert!(!forecast.known);
    assert_eq!(forecast.risk, "unknown");
}

/// Mirrors `tests/test_capacity_forecast.py::CapacityForecastTests::test_stale_latest_is_unknown_even_when_older_sample_is_fresh`.
#[test]
fn forecast_stale_latest_is_unknown_even_when_older_sample_is_fresh() {
    let now = 1_000_000.0;
    let samples = [
        forecast_sample(Some(50.0), None, Some(now), Some(now - 1.0)),
        forecast_sample(Some(60.0), None, Some(now - 3_600.0), None),
    ];
    let forecast = capacity::forecast(&key(), &samples, now);
    assert!(!forecast.known);
    assert_eq!(forecast.risk, "unknown");
}

/// Mirrors `tests/test_capacity_forecast.py::CapacityForecastTests::test_single_fresh_sample_is_warmup_and_uses_remaining_thresholds`.
#[test]
fn forecast_single_fresh_sample_is_warmup_and_uses_remaining_thresholds() {
    let now = 1_000_000.0;
    let sample = forecast_sample(Some(5.0), None, Some(now), None);
    let forecast = capacity::forecast(&key(), &[sample], now);
    assert!(forecast.known);
    assert!(forecast.warmup);
    assert_eq!(forecast.burn_percent_per_hour, None);
    assert_eq!(forecast.risk, "high");
}

/// Mirrors `tests/test_capacity_forecast.py::CapacityForecastTests::test_burn_below_sustainable_pace_is_low_risk`.
#[test]
fn forecast_burn_below_sustainable_pace_is_low_risk() {
    let now = 1_000_000.0;
    let reset_at = now + 2.0 * 3_600.0;
    let samples = [
        forecast_sample(Some(60.0), Some(reset_at), Some(now), None),
        forecast_sample(Some(80.0), Some(reset_at), Some(now - 3_600.0), None),
    ];
    let forecast = capacity::forecast(&key(), &samples, now);
    assert!(!forecast.warmup);
    assert_eq!(forecast.burn_percent_per_hour, Some(20.0));
    assert_eq!(forecast.sustainable_percent_per_hour, Some(30.0));
    assert_eq!(forecast.risk, "low");
}

/// Mirrors `tests/test_capacity_forecast.py::CapacityForecastTests::test_burn_above_sustainable_pace_is_medium_or_high_risk`.
#[test]
fn forecast_burn_above_sustainable_pace_is_medium_or_high_risk() {
    let now = 1_000_000.0;
    let reset_at = now + 2.0 * 3_600.0;
    let medium = [
        forecast_sample(Some(60.0), Some(reset_at), Some(now), None),
        forecast_sample(Some(95.0), Some(reset_at), Some(now - 3_600.0), None),
    ];
    assert_eq!(capacity::forecast(&key(), &medium, now).risk, "medium");
    let high = [
        forecast_sample(Some(60.0), Some(reset_at), Some(now), None),
        forecast_sample(Some(160.0), Some(reset_at), Some(now - 3_600.0), None),
    ];
    assert_eq!(capacity::forecast(&key(), &high, now).risk, "high");
}

/// Mirrors `tests/test_capacity_forecast.py::CapacityForecastTests::test_young_lane_with_a_scary_short_span_burn_stays_low`.
#[test]
fn forecast_young_lane_with_a_scary_short_span_burn_stays_low() {
    let now = 1_000_000.0;
    let reset_at = now + 48.0 * 3_600.0;
    let samples = [
        forecast_sample(Some(99.0), Some(reset_at), Some(now), None),
        forecast_sample(Some(100.0), Some(reset_at), Some(now - 600.0), None),
    ];
    let forecast = capacity::forecast(&key(), &samples, now);
    assert!(!forecast.warmup);
    assert_eq!(forecast.burn_percent_per_hour, Some(6.0));
    assert_eq!(forecast.sustainable_percent_per_hour, Some(99.0 / 48.0));
    assert_eq!(forecast.risk, "low");
}

/// Mirrors `tests/test_capacity_forecast.py::CapacityForecastTests::test_mature_lane_with_the_same_burn_still_escalates`.
#[test]
fn forecast_mature_lane_with_the_same_burn_still_escalates() {
    let now = 1_000_000.0;
    let reset_at = now + 48.0 * 3_600.0;
    let samples = [
        forecast_sample(Some(88.0), Some(reset_at), Some(now), None),
        forecast_sample(Some(100.0), Some(reset_at), Some(now - 2.0 * 3_600.0), None),
    ];
    let forecast = capacity::forecast(&key(), &samples, now);
    assert_eq!(forecast.burn_percent_per_hour, Some(6.0));
    assert_eq!(forecast.sustainable_percent_per_hour, Some(88.0 / 48.0));
    assert_eq!(forecast.risk, "high");
}

/// Mirrors `tests/test_capacity_forecast.py::CapacityForecastTests::test_window_reset_starts_a_new_warmup`.
#[test]
fn forecast_window_reset_starts_a_new_warmup() {
    let now = 1_000_000.0;
    let samples = [
        forecast_sample(Some(90.0), Some(now + 3_600.0), Some(now), None),
        forecast_sample(Some(5.0), Some(now - 10.0), Some(now - 3_600.0), None),
    ];
    let forecast = capacity::forecast(&key(), &samples, now);
    assert!(forecast.warmup);
    assert_eq!(forecast.burn_percent_per_hour, None);
    assert_eq!(forecast.remaining_percent, Some(90.0));
}

/// Mirrors `tests/test_capacity_forecast.py::CapacityForecastTests::test_latest_sample_with_past_reset_at_is_unknown`.
#[test]
fn forecast_latest_sample_with_past_reset_at_is_unknown() {
    let now = 1_000_000.0;
    let sample = forecast_sample(Some(50.0), Some(now - 1.0), Some(now), None);
    let forecast = capacity::forecast(&key(), &[sample], now);
    assert!(!forecast.known);
    assert_eq!(forecast.remaining_percent, None);
    assert_eq!(forecast.reset_at, None);
    assert_eq!(forecast.risk, "unknown");
}

/// Mirrors `tests/test_capacity_forecast.py::CapacityForecastTests::test_latest_sample_with_future_reset_at_is_known`.
#[test]
fn forecast_latest_sample_with_future_reset_at_is_known() {
    let now = 1_000_000.0;
    let sample = forecast_sample(Some(50.0), Some(now + 1.0), Some(now), None);
    let forecast = capacity::forecast(&key(), &[sample], now);
    assert!(forecast.known);
    assert_eq!(forecast.remaining_percent, Some(50.0));
    assert_eq!(forecast.reset_at, Some(now + 1.0));
}

/// Mirrors `tests/test_capacity_forecast.py::CapacityForecastTests::test_latest_sample_without_reset_at_is_unaffected`.
#[test]
fn forecast_latest_sample_without_reset_at_is_unaffected() {
    let now = 1_000_000.0;
    let sample = forecast_sample(Some(50.0), None, Some(now), None);
    let forecast = capacity::forecast(&key(), &[sample], now);
    assert!(forecast.known);
    assert_eq!(forecast.reset_at, None);
}

/// Mirrors `tests/test_capacity_forecast.py::CapacityForecastTests::test_exhausted_zero_is_high_risk_even_with_zero_burn_and_pace`.
#[test]
fn forecast_exhausted_zero_is_high_risk_even_with_zero_burn_and_pace() {
    let now = 1_000_000.0;
    let reset_at = now + 3_600.0;
    let samples = [
        forecast_sample(Some(0.0), Some(reset_at), Some(now), None),
        forecast_sample(Some(0.0), Some(reset_at), Some(now - 3_600.0), None),
    ];
    let forecast = capacity::forecast(&key(), &samples, now);
    assert_eq!(forecast.burn_percent_per_hour, Some(0.0));
    assert_eq!(forecast.sustainable_percent_per_hour, Some(0.0));
    assert_eq!(forecast.risk, "high");
}

/// Mirrors `tests/test_capacity_forecast.py::CapacityForecastTests::test_expired_same_reset_history_still_drives_burn`.
#[test]
fn forecast_expired_same_reset_history_still_drives_burn() {
    let now = 1_000_000.0;
    let reset_at = now + 3_600.0;
    let samples = [
        forecast_sample(Some(60.0), Some(reset_at), Some(now), Some(now + 60.0)),
        forecast_sample(
            Some(80.0),
            Some(reset_at),
            Some(now - 3_600.0),
            Some(now - 1.0),
        ),
    ];
    let forecast = capacity::forecast(&key(), &samples, now);
    assert!(!forecast.warmup);
    assert_eq!(forecast.burn_percent_per_hour, Some(20.0));
}
#[test]
fn thin_history_does_not_escalate_burn_risk() {
    let series = vec![
        sample(95.0, 1010.0, 20000.0),
        sample(100.0, 1000.0, 20000.0),
    ];
    let f = capacity::forecast(&key(), &series, 1010.0);
    assert_eq!(f.risk, "low");
    assert_eq!(
        capacity::window(&f, 1010.0).unwrap().marker,
        "thin_evidence"
    );
}
#[test]
fn exhausted_window_cannot_be_revived_by_weight() {
    let h = common::Home::new();
    let mut runtime = h.config.runtime("mock").unwrap().clone();
    runtime.priority_multiplier = 1e100;
    let f = capacity::forecast(&key(), &[sample(0.0, 1000.0, 20000.0)], 1000.0);
    let w = capacity::window(&f, 1000.0).unwrap();
    assert!(capacity::rank(&runtime, vec![route("a", None)], vec![w]).is_none());
}
#[test]
fn aliases_use_highest_absolute_weight_not_sum() {
    let h = common::Home::new();
    let mut runtime = h.config.runtime("mock").unwrap().clone();
    runtime
        .priority_account_multipliers
        .insert("premium".into(), 3.0);
    let f = capacity::forecast(&key(), &[sample(50.0, 1000.0, 20000.0)], 1000.0);
    let w = capacity::window(&f, 1000.0).unwrap();
    let r = capacity::rank(
        &runtime,
        vec![route("base", None), route("premium", Some("premium"))],
        vec![w],
    )
    .unwrap();
    assert_eq!(r.multiplier, 3.0);
    assert_eq!(r.priority, 3.0);
    assert_eq!(r.aliases[0].account.as_deref(), Some("premium"));
}
/// Mirrors `tests/test_capacity_topology.py::test_invalid_pool_reference_rejects_the_whole_topology`.
#[test]
fn every_route_pool_reference_is_validated() {
    let mut topology = Topology {
        pools: vec![Pool {
            pool_id: "pool".into(),
            keys: BTreeSet::from([key()]),
        }],
        routes: vec![route("a", None)],
    };
    assert!(topology.validate("mock").is_ok());
    topology.routes[0].pool_ids.push("missing".into());
    assert!(topology.validate("mock").is_err());
}
/// Mirrors `tests/test_capacity_topology.py::test_persist_slice_consumes_the_state_api_atomically`.
#[test]
fn invalid_atomic_slice_does_not_erase_a_committed_snapshot() {
    let h = common::Home::new();
    let (mut slice, _) =
        capacity::sources::normalize_codex("mock", None, &response(), domain::now()).unwrap();
    capacity::persist(&h.path, &slice, 100).unwrap();
    let before = h.store().health().unwrap();
    slice.samples[0].remaining_percent = Some(f64::NAN);
    assert!(capacity::persist(&h.path, &slice, 100).is_err());
    assert_eq!(h.store().health().unwrap(), before);
    let db = rusqlite::Connection::open(h.path.join("state.db")).unwrap();
    let count: i64 = db
        .query_row("SELECT COUNT(*) FROM capacity_samples", [], |r| r.get(0))
        .unwrap();
    assert_eq!(count, 2);
}
/// Mirrors `tests/test_capacity_sources.py::test_malformed_present_entries_and_responses_are_source_failures`.
#[test]
fn native_claude_requires_valid_percent_in_every_entry() {
    let good =
        json!({"limits":[{"kind":"session","percent":20,"resets_at":"2026-09-16T00:00:00Z"}]});
    assert_eq!(
        capacity::sources::normalize_claude("mock", &good, 1000.0)
            .unwrap()
            .samples
            .len(),
        1
    );
    let bad = json!({"limits":[{"kind":"session","percent":"20"}]});
    assert!(capacity::sources::normalize_claude("mock", &bad, 1000.0).is_err());
}
/// Mirrors `tests/test_capacity_order.py::test_account_precedes_lane_and_runtime_for_shared_route_ids`.
///
/// `Runtime::weight` is the exact function `order()` calls once per route to
/// build its `route_multipliers` map; the account multiplier must win over
/// the lane multiplier, which must win over the flat runtime default.
/// Mirrors `tests/test_capacity_order.py::CapacityOrderTests::test_new_accounts_fallbacks_and_runtime_filtering`.
#[test]
fn route_weight_precedence_is_account_then_lane_then_runtime_default() {
    let h = common::Home::new();
    let mut runtime = h.config.runtime("mock").unwrap().clone();
    runtime.priority_multiplier = 1.0;
    runtime
        .priority_lane_multipliers
        .insert("new-lane".into(), 2.0);
    runtime
        .priority_account_multipliers
        .insert("new-account".into(), 3.0);
    assert_eq!(runtime.weight(Some("new-account"), "new-lane"), 3.0);
    assert_eq!(runtime.weight(None, "new-lane"), 2.0);
    assert_eq!(runtime.weight(None, "other-lane"), 1.0);
    assert_eq!(runtime.weight(Some("unknown-account"), "new-lane"), 2.0);
}

// --- Pure ranking (`agent_run.capacity.ranking.rank_capacity_routes`) ---
// The shared injected epoch used by every deterministic ranking test below,
// matching `tests/test_capacity_ranking.py::_NOW`.
const RK_NOW: f64 = 1000.0;

fn rk_key(runtime: &str, lane: &str) -> Key {
    Key {
        runtime: runtime.into(),
        lane: lane.into(),
        window: "window".into(),
        target: None,
        source: "source".into(),
    }
}
fn rk_forecast(runtime: &str, lane: &str, remaining: f64) -> Forecast {
    Forecast {
        key: rk_key(runtime, lane),
        known: true,
        remaining_percent: Some(remaining),
        reset_at: Some(4600.0),
        observed_at: Some(RK_NOW),
        warmup: false,
        burn_percent_per_hour: None,
        sustainable_percent_per_hour: None,
        risk: "low".into(),
        burn_span_seconds: None,
    }
}
fn rk_forecast_unknown(runtime: &str, lane: &str) -> Forecast {
    Forecast {
        key: rk_key(runtime, lane),
        known: false,
        remaining_percent: None,
        reset_at: None,
        observed_at: None,
        warmup: true,
        burn_percent_per_hour: None,
        sustainable_percent_per_hour: None,
        risk: "unknown".into(),
        burn_span_seconds: None,
    }
}
fn rk_route_full(
    runtime: &str,
    route_id: &str,
    pool_id: &str,
    forecasts: Vec<Forecast>,
    account: Option<&str>,
    quota_lane: &str,
    reset_credits: Option<u64>,
) -> RouteInput {
    let keys: BTreeSet<Key> = forecasts.iter().map(|f| f.key.clone()).collect();
    RouteInput {
        descriptor: Route {
            route_id: route_id.into(),
            runtime: runtime.into(),
            account: account.map(str::to_owned),
            quota_lane: quota_lane.into(),
            pool_ids: vec![pool_id.into()],
            reset_credits,
        },
        pools: vec![Pool {
            pool_id: pool_id.into(),
            keys,
        }],
        forecasts,
    }
}
fn rk_route(runtime: &str, route_id: &str, pool_id: &str, forecasts: Vec<Forecast>) -> RouteInput {
    rk_route_full(runtime, route_id, pool_id, forecasts, None, "lane", None)
}

/// Mirrors `tests/test_capacity_ranking.py::test_deferred_evidence_and_unavailable_runtime_are_complete`.
#[test]
fn deferred_evidence_and_unavailable_runtime_are_complete() {
    let good = rk_route(
        "runtime-u",
        "good",
        "good-pool",
        vec![rk_forecast("runtime-u", "good", 50.0)],
    );
    let unknown = rk_route(
        "runtime-x",
        "unknown",
        "unknown-pool",
        vec![rk_forecast_unknown("runtime-x", "unknown")],
    );
    let snapshot_evidence = vec![
        ranking::OrderEvidence {
            runtime: "runtime-d".into(),
            scope_id: Some("scope-d".into()),
            route_id: None,
            reason: "malformed".into(),
            detail: "bad".into(),
        },
        ranking::OrderEvidence {
            runtime: "runtime-u".into(),
            scope_id: Some("scope-old".into()),
            route_id: None,
            reason: "expired".into(),
            detail: "old".into(),
        },
    ];
    let order = ranking::rank_capacity_routes(
        vec![unknown, good],
        snapshot_evidence,
        &BTreeMap::new(),
        &BTreeMap::new(),
        RK_NOW,
    )
    .unwrap();
    let reasons: BTreeSet<_> = order.deferred.iter().map(|e| e.reason.clone()).collect();
    assert_eq!(
        reasons,
        BTreeSet::from([
            "malformed".to_string(),
            "expired".to_string(),
            "ranker_unknown_forecast".to_string()
        ])
    );
    assert_eq!(
        order.unavailable_runtimes,
        vec!["runtime-d".to_string(), "runtime-x".to_string()]
    );
    assert_eq!(order.routes[0].runtime, "runtime-u");
}

/// Mirrors `tests/test_capacity_ranking.py::CapacityRankingTests::test_new_forecast_snapshot_can_change_order`.
#[test]
fn new_forecast_snapshot_can_change_order() {
    let first = ranking::rank_capacity_routes(
        vec![
            rk_route(
                "runtime",
                "a",
                "pool-a",
                vec![rk_forecast("runtime", "a", 20.0)],
            ),
            rk_route(
                "runtime",
                "b",
                "pool-b",
                vec![rk_forecast("runtime", "b", 80.0)],
            ),
        ],
        vec![],
        &BTreeMap::new(),
        &BTreeMap::new(),
        RK_NOW,
    )
    .unwrap();
    let second = ranking::rank_capacity_routes(
        vec![
            rk_route(
                "runtime",
                "a",
                "pool-a",
                vec![rk_forecast("runtime", "a", 90.0)],
            ),
            rk_route(
                "runtime",
                "b",
                "pool-b",
                vec![rk_forecast("runtime", "b", 10.0)],
            ),
        ],
        vec![],
        &BTreeMap::new(),
        &BTreeMap::new(),
        RK_NOW,
    )
    .unwrap();
    assert_eq!(first.routes[0].aliases[0].route_id, "b");
    assert_eq!(second.routes[0].aliases[0].route_id, "a");
}

/// Mirrors `tests/test_capacity_ranking.py::test_invalid_arguments_raise_validation_error`.
#[test]
fn invalid_multipliers_and_now_are_rejected() {
    assert!(ranking::rank_capacity_routes(
        vec![],
        vec![],
        &BTreeMap::new(),
        &BTreeMap::new(),
        -1.0
    )
    .is_err());
    assert!(ranking::rank_capacity_routes(
        vec![],
        vec![],
        &BTreeMap::new(),
        &BTreeMap::new(),
        f64::NAN
    )
    .is_err());
    let blank = BTreeMap::from([(String::new(), 1.0)]);
    assert!(
        ranking::rank_capacity_routes(vec![], vec![], &blank, &BTreeMap::new(), RK_NOW).is_err()
    );
    let zero = BTreeMap::from([("runtime".to_string(), 0.0)]);
    assert!(
        ranking::rank_capacity_routes(vec![], vec![], &zero, &BTreeMap::new(), RK_NOW).is_err()
    );
    let nan = BTreeMap::from([("runtime".to_string(), f64::NAN)]);
    assert!(ranking::rank_capacity_routes(vec![], vec![], &nan, &BTreeMap::new(), RK_NOW).is_err());
}

/// Mirrors `tests/test_capacity_ranking.py::test_route_multiplier_keys_and_values_are_strictly_validated`.
#[test]
fn route_multiplier_values_are_strictly_validated() {
    for bad in [0.0, -1.0, f64::NAN, f64::INFINITY] {
        let route_multipliers =
            BTreeMap::from([(("runtime".to_string(), "route".to_string()), bad)]);
        assert!(ranking::rank_capacity_routes(
            vec![],
            vec![],
            &BTreeMap::new(),
            &route_multipliers,
            RK_NOW
        )
        .is_err());
    }
    let blank_route = BTreeMap::from([(("runtime".to_string(), String::new()), 1.0)]);
    assert!(
        ranking::rank_capacity_routes(vec![], vec![], &BTreeMap::new(), &blank_route, RK_NOW)
            .is_err()
    );
}

/// Mirrors `tests/test_capacity_ranking.py::test_manual_reset_credits_are_bounded_and_never_revive_exhaustion`.
#[test]
fn manual_reset_credits_are_bounded_and_never_revive_exhaustion() {
    let one = rk_route_full(
        "codex",
        "one",
        "one",
        vec![rk_forecast("codex", "one", 80.0)],
        None,
        "lane",
        Some(1),
    );
    let two = rk_route_full(
        "codex",
        "two",
        "two",
        vec![rk_forecast("codex", "two", 80.0)],
        None,
        "lane",
        Some(2),
    );
    let exhausted = rk_route_full(
        "codex",
        "empty",
        "empty",
        vec![rk_forecast("codex", "empty", 0.0)],
        None,
        "lane",
        Some(2),
    );
    let order = ranking::rank_capacity_routes(
        vec![one, two, exhausted],
        vec![],
        &BTreeMap::new(),
        &BTreeMap::new(),
        RK_NOW,
    )
    .unwrap();
    let ids: Vec<_> = order
        .routes
        .iter()
        .map(|r| r.aliases[0].route_id.clone())
        .collect();
    assert_eq!(ids, vec!["two".to_string(), "one".to_string()]);
    assert!((order.routes[0].reset_credit_multiplier - (1.0 + 2.0 / 3.0)).abs() < 1e-9);
    assert!((order.routes[1].reset_credit_multiplier - 1.5).abs() < 1e-9);
    assert_eq!(order.omitted[0].reason, "exhausted");
}

#[test]
fn overflowing_priority_defers_rather_than_producing_infinity() {
    // Large weights/overflow must be deferred with a typed reason before
    // ranking; `Infinity` must never enter the sort or the JSON projection.
    // No Python baseline test covers this.
    let route = rk_route(
        "codex",
        "huge",
        "pool",
        vec![rk_forecast("codex", "huge", 80.0)],
    );
    let multipliers = BTreeMap::from([("codex".to_string(), f64::MAX)]);
    let order =
        ranking::rank_capacity_routes(vec![route], vec![], &multipliers, &BTreeMap::new(), RK_NOW)
            .unwrap();
    assert!(order.routes.is_empty());
    assert_eq!(order.deferred[0].reason, "priority_overflow");
    assert!(serde_json::to_value(&order).unwrap().is_object());
}

/// One golden case: Python's exact `rank_capacity_routes` input and output.
#[derive(serde::Deserialize)]
struct GoldenCase {
    id: String,
    inputs: GoldenInputs,
    output: serde_json::Value,
}
#[derive(serde::Deserialize)]
struct GoldenInputs {
    multipliers: BTreeMap<String, f64>,
    now: f64,
    route_multipliers: BTreeMap<String, f64>,
    routes: Vec<RouteInput>,
}

/// Parse Python's `str((runtime, route_id))` dict-key rendering, e.g.
/// `"('provider-a', 'shared')"`, back into a `(runtime, route_id)` pair.
fn parse_route_multiplier_key(raw: &str) -> (String, String) {
    let inner = raw.trim_start_matches('(').trim_end_matches(')');
    let mut parts = inner.splitn(2, ", ");
    let a = parts.next().unwrap().trim_matches('\'').to_string();
    let b = parts.next().unwrap().trim_matches('\'').to_string();
    (a, b)
}

/// Structural JSON equality that tolerates the last-ULP float noise
/// `serde_json`'s fast text parser introduces when reading the fixture file
/// back in (verified directly: `serde_json::from_str("0.020000000000000018")`
/// yields a different f64 than the literal, absent the `float_roundtrip`
/// cargo feature). The ranking arithmetic itself never round-trips through
/// JSON text, so this only absorbs the harness's own parse, not a ranking
/// bug.
fn values_close(actual: &serde_json::Value, expected: &serde_json::Value) -> bool {
    use serde_json::Value;
    match (actual, expected) {
        (Value::Number(a), Value::Number(b)) => {
            let (a, b) = (a.as_f64().unwrap(), b.as_f64().unwrap());
            (a - b).abs() <= 1e-9 * a.abs().max(b.abs()).max(1.0)
        }
        (Value::Array(a), Value::Array(b)) => {
            a.len() == b.len() && a.iter().zip(b).all(|(x, y)| values_close(x, y))
        }
        (Value::Object(a), Value::Object(b)) => {
            a.len() == b.len()
                && a.iter()
                    .all(|(k, v)| b.get(k).is_some_and(|w| values_close(v, w)))
        }
        _ => actual == expected,
    }
}

/// Mirrors `tests/test_capacity_ranking.py::test_reliable_projection_and_multiplier_determine_priority`.
/// Mirrors `tests/test_capacity_ranking.py::test_equal_current_capacity_is_ordered_by_forecast`.
/// Mirrors `tests/test_capacity_ranking.py::test_fallback_markers_use_centered_remaining_percent`.
/// Mirrors `tests/test_capacity_ranking.py::test_exhaustion_is_omitted_and_multiplier_cannot_revive_zero_score`.
/// Mirrors `tests/test_capacity_ranking.py::test_alias_collapse_preserves_concrete_launch_descriptors`.
/// Mirrors `tests/test_capacity_ranking.py::test_tie_break_is_total_and_input_order_independent`.
/// Mirrors `tests/test_capacity_ranking.py::test_route_multiplier_is_scoped_by_runtime_and_alias_weight_is_maximum`.
#[test]
fn golden_capacity_ranking_cases_match_python() {
    // Data-driven oracle: tests/fixtures/baseline/capacity/cases.json holds
    // exact inputs and outputs captured from the real Python
    // `rank_capacity_routes` (a frozen capture). Every
    // case name mirrors a test in tests/test_capacity_ranking.py (the
    // `reset-credit-bonus` case duplicates
    // `manual_reset_credits_are_bounded_and_never_revive_exhaustion` above and
    // is intentionally left off this list to avoid a coverage-tool conflict).
    let path = concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../tests/fixtures/baseline/capacity/cases.json"
    );
    let text = std::fs::read_to_string(path).expect("golden capacity fixture must exist");
    let cases: Vec<GoldenCase> =
        serde_json::from_str(&text).expect("golden capacity fixture must parse");
    assert!(!cases.is_empty());
    for case in cases {
        let route_multipliers: BTreeMap<(String, String), f64> = case
            .inputs
            .route_multipliers
            .into_iter()
            .map(|(k, v)| (parse_route_multiplier_key(&k), v))
            .collect();
        let order = ranking::rank_capacity_routes(
            case.inputs.routes,
            vec![],
            &case.inputs.multipliers,
            &route_multipliers,
            case.inputs.now,
        )
        .unwrap_or_else(|e| panic!("case {} failed to rank: {e}", case.id));
        let actual = serde_json::to_value(&order).unwrap();
        assert!(
            values_close(&actual, &case.output),
            "case {} produced a different projection\n  actual:   {actual}\n  expected: {}",
            case.id,
            case.output
        );
    }
}
