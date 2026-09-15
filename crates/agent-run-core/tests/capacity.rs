mod common;
use agent_run_core::capacity::{
    self,
    ranking::{self, RouteInput},
    Forecast, Key, Pool, Route, Sample, Topology,
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
#[test]
fn codexbar_observation_time_must_have_a_timezone() {
    let good = json!({"usage":{"updatedAt":"2026-09-15T12:00:00Z","primary":{"usedPercent":25,"windowMinutes":300,"resetsAt":"2026-09-15T16:00:00Z"}}});
    assert!(capacity::sources::normalize_codexbar("mock", &good).is_ok());
    let mut bad = good;
    bad["usage"]["updatedAt"] = json!("2026-09-15T12:00:00");
    assert!(capacity::sources::normalize_codexbar("mock", &bad).is_err());
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

#[test]
fn deferred_evidence_and_unavailable_runtime_are_complete() {
    // Mirrors tests/test_capacity_ranking.py::
    // test_deferred_evidence_and_unavailable_runtime_are_complete
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

#[test]
fn invalid_multipliers_and_now_are_rejected() {
    // Mirrors tests/test_capacity_ranking.py::test_invalid_arguments_raise_validation_error
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

#[test]
fn route_multiplier_values_are_strictly_validated() {
    // Mirrors tests/test_capacity_ranking.py::
    // test_route_multiplier_keys_and_values_are_strictly_validated
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

#[test]
fn manual_reset_credits_are_bounded_and_never_revive_exhaustion() {
    // Mirrors tests/test_capacity_ranking.py::
    // test_manual_reset_credits_are_bounded_and_never_revive_exhaustion
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
    // ADR A16: large weights/overflow must be rejected/deferred with a typed
    // reason before ranking; `Infinity` must never enter the sort or the JSON
    // projection. No direct Python test covers this -- the Python baseline
    // leaves it as an open decision (rust-migration-plan.md 23.2, A16).
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

#[test]
fn golden_capacity_ranking_cases_match_python() {
    // Data-driven oracle: tests/fixtures/baseline/capacity/cases.json holds
    // exact inputs and outputs captured from the real Python
    // `rank_capacity_routes` (migration/tools/capture_baseline.py). Every
    // case name mirrors a test in tests/test_capacity_ranking.py.
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
