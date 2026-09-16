//! Ported tail of the Python capacity suite.
//!
//! Covers `tests/test_capacity_advice.py`, the validation and source-topology
//! halves of `tests/test_capacity_topology.py`, the exact-key route snapshot
//! join of `tests/test_capacity_snapshot.py`, and the bounded collection
//! outcomes of `tests/test_capacity_outcomes.py`. Neighbour of
//! `capacity.rs`/`capacity_collectors.rs`, which hold the already-ported
//! capacity behaviors.

use agent_run_core::capacity::{
    self,
    advice::{advice_key, build_advice, capacity_label},
    persist, sources, Forecast, Key, Pool, Route, Sample, Slice, Topology,
};

// --- Advice (`agent_run.capacity.advice`) -----------------------------------

/// The single advisory identity every ported advice behavior shares.
///
/// Mirrors `KEY` in `tests/test_capacity_advice.py`: a Codex request lane with
/// both an explicit target and an explicit source, so `capacity_label` has to
/// render every optional segment.
fn ad_key() -> Key {
    Key {
        runtime: "codex".into(),
        lane: "requests".into(),
        window: "5h".into(),
        target: Some("gpt-5.6-sol".into()),
        source: "app_server".into(),
    }
}

/// Builds one known forecast fixture for the advice behaviors.
///
/// Mirrors `_forecast` in `tests/test_capacity_advice.py`. `known` is always
/// true there, so it is not a parameter here. `remaining` is a percentage in
/// `[0, 100]` or `None` for absent evidence; `reset_at` is an epoch second or
/// `None`; `warmup` marks a forecast with no burn evidence yet; `burn` and
/// `sustainable` are percent-per-hour rates or `None`; `risk` is one of
/// `unknown`/`low`/`medium`/`high`. `observed_at` is pinned to the Python
/// fixture's epoch because `advice_key` must ignore it. `burn_span_seconds`
/// has no Python counterpart on this fixture and stays `None`.
#[allow(clippy::too_many_arguments)]
fn ad_forecast(
    key: Key,
    remaining: Option<f64>,
    reset_at: Option<f64>,
    warmup: bool,
    burn: Option<f64>,
    sustainable: Option<f64>,
    risk: &str,
) -> Forecast {
    Forecast {
        key,
        known: true,
        remaining_percent: remaining,
        reset_at,
        observed_at: Some(1_000_000.0),
        warmup,
        burn_percent_per_hour: burn,
        sustainable_percent_per_hour: sustainable,
        risk: risk.into(),
        burn_span_seconds: None,
    }
}

/// Builds the default low-risk forecast for `ad_key`.
fn ad_default() -> Forecast {
    ad_forecast(
        ad_key(),
        Some(40.0),
        Some(1_010_000.0),
        false,
        Some(5.0),
        Some(20.0),
        "low",
    )
}

/// Mirrors `tests/test_capacity_advice.py::CapacityAdviceTests::test_identity_is_preserved_from_forecast_to_advice`.
#[test]
fn advice_preserves_the_forecast_identity_and_stays_silent_at_low_risk() {
    let advice = build_advice(&[ad_default()]);
    assert_eq!(advice.len(), 1);
    assert_eq!(advice[0].key, ad_key());
    assert_eq!(advice[0].risk, "low");
    assert!(advice[0].recommendations.is_empty());
}

/// Mirrors `tests/test_capacity_advice.py::CapacityAdviceTests::test_recommendations_scale_with_risk`.
#[test]
fn advice_recommendations_scale_with_risk() {
    let advice = build_advice(&[
        ad_forecast(
            ad_key(),
            None,
            Some(1_010_000.0),
            true,
            None,
            None,
            "unknown",
        ),
        ad_forecast(
            ad_key(),
            Some(40.0),
            Some(1_010_000.0),
            false,
            Some(5.0),
            Some(20.0),
            "medium",
        ),
        ad_forecast(
            ad_key(),
            Some(40.0),
            Some(1_010_000.0),
            false,
            Some(5.0),
            Some(20.0),
            "high",
        ),
    ]);
    let [unknown, medium, high] = &advice[..] else {
        panic!("expected exactly three advisories");
    };
    assert!(unknown.recommendations[0].contains("unknown"));
    assert!(medium.recommendations[0].contains("pace"));
    assert!(high.recommendations[0].contains("exhaustion"));
    assert!(high.recommendations[0].contains("target=gpt-5.6-sol"));
    assert!(high.recommendations[0].contains("source=app_server"));
    assert_eq!(
        capacity_label(&ad_key()),
        "codex/requests 5h target=gpt-5.6-sol source=app_server"
    );
}

/// Mirrors `tests/test_capacity_advice.py::CapacityAdviceTests::test_advice_key_ignores_observed_at_noise`.
#[test]
fn advice_key_ignores_observed_at_noise() {
    let stable_a = ad_forecast(
        ad_key(),
        Some(41.0),
        Some(1_010_000.0),
        false,
        Some(5.0),
        Some(20.0),
        "low",
    );
    // Same 5%-wide remaining bucket, same 300-second reset bucket, pure
    // timestamp noise, and unbucketed rate fields that are not part of the key.
    let mut stable_b = ad_forecast(
        ad_key(),
        Some(42.0),
        Some(1_010_010.0),
        false,
        Some(5.4),
        Some(20.1),
        "low",
    );
    stable_b.observed_at = Some(9_999_999.0);
    assert_eq!(
        advice_key(&build_advice(&[stable_a])),
        advice_key(&build_advice(&[stable_b]))
    );
}

/// Mirrors `tests/test_capacity_advice.py::CapacityAdviceTests::test_advice_key_changes_on_material_state`.
#[test]
fn advice_key_changes_on_material_state() {
    let low = advice_key(&build_advice(&[ad_default()]));
    let high = advice_key(&build_advice(&[ad_forecast(
        ad_key(),
        Some(5.0),
        Some(1_010_000.0),
        false,
        Some(5.0),
        Some(20.0),
        "high",
    )]));
    assert_ne!(low, high);
}

/// Builds the second, distinct advisory identity used by the key behaviors.
///
/// Mirrors `other_key` in `tests/test_capacity_advice.py`.
fn ad_other_key() -> Key {
    Key {
        runtime: "claude".into(),
        lane: "requests".into(),
        window: "5h".into(),
        target: Some("sonnet".into()),
        source: "cli".into(),
    }
}

/// Mirrors `tests/test_capacity_advice.py::CapacityAdviceTests::test_advice_key_changes_when_identity_differs`.
#[test]
fn advice_key_changes_when_identity_differs() {
    let first = advice_key(&build_advice(&[ad_default()]));
    let mut other = ad_default();
    other.key = ad_other_key();
    let second = advice_key(&build_advice(&[other]));
    assert_ne!(first, second);
}

/// Mirrors `tests/test_capacity_advice.py::CapacityAdviceTests::test_advice_key_is_order_independent`.
#[test]
fn advice_key_is_order_independent() {
    let mut other = ad_default();
    other.key = ad_other_key();
    let forward = build_advice(&[ad_default(), other.clone()]);
    let backward = build_advice(&[other, ad_default()]);
    assert_eq!(advice_key(&forward), advice_key(&backward));
}

// --- Topology validation (`agent_run.capacity.topology.validate_topology`) ---

/// The fictitious runtime every topology behavior below is scoped to.
///
/// Mirrors `_RUNTIME` in `tests/test_capacity_topology.py`: no real provider
/// account appears in these fixtures.
const TP_RUNTIME: &str = "fictitious";

/// Builds one pool identity for the topology behaviors.
///
/// `lane`/`window` name the quota bucket and `target` is the optional account
/// or model scope; `source` is fixed to `native` as in the Python fixtures.
fn tp_key(lane: &str, window: &str, target: Option<&str>) -> Key {
    Key {
        runtime: TP_RUNTIME.into(),
        lane: lane.into(),
        window: window.into(),
        target: target.map(str::to_owned),
        source: "native".into(),
    }
}

/// Builds one single-key physical pool descriptor.
fn tp_pool(pool_id: &str, key: Key) -> Pool {
    Pool {
        pool_id: pool_id.into(),
        keys: [key].into_iter().collect(),
    }
}

/// Builds one route descriptor over the named pools.
///
/// `account` is `None` for the default account; `pool_ids` is passed through
/// verbatim so duplicate-reference rejection stays observable.
fn tp_route(route_id: &str, account: Option<&str>, lane: &str, pool_ids: &[&str]) -> Route {
    Route {
        route_id: route_id.into(),
        runtime: TP_RUNTIME.into(),
        account: account.map(str::to_owned),
        quota_lane: lane.into(),
        pool_ids: pool_ids.iter().map(|id| (*id).to_string()).collect(),
        reset_credits: None,
    }
}

/// Mirrors `tests/test_capacity_topology.py::TopologyValidationTests::test_empty_key_set_duplicate_ids_and_duplicate_references_reject`.
#[test]
fn empty_key_set_duplicate_ids_and_duplicate_references_reject() {
    let good = tp_pool("pool-1", tp_key("primary", "five_hour", None));

    // An exact pool is never an empty key set.
    let empty = Topology {
        pools: vec![Pool {
            pool_id: "pool-2".into(),
            keys: Default::default(),
        }],
        routes: vec![],
    };
    assert!(empty.validate(TP_RUNTIME).is_err());

    // Pool ids are unique within one topology.
    let duplicate = Topology {
        pools: vec![good.clone(), good.clone()],
        routes: vec![],
    };
    assert!(duplicate.validate(TP_RUNTIME).is_err());

    // A route never names the same reservoir twice: capacity would double.
    let repeated = Topology {
        pools: vec![good],
        routes: vec![tp_route("route-1", None, "primary", &["pool-1", "pool-1"])],
    };
    assert!(repeated.validate(TP_RUNTIME).is_err());
}

/// Mirrors `tests/test_capacity_topology.py::TopologyValidationTests::test_shared_pool_is_referenced_without_multiplying_and_distinct_stay_distinct`.
#[test]
fn shared_pool_is_referenced_without_multiplying_and_distinct_stay_distinct() {
    let topology = Topology {
        pools: vec![
            tp_pool("pool-shared", tp_key("primary", "five_hour", None)),
            tp_pool("pool-first", tp_key("primary", "five_hour", Some("team1"))),
            tp_pool("pool-second", tp_key("primary", "five_hour", Some("team2"))),
        ],
        routes: vec![
            tp_route("r-default", None, "primary", &["pool-shared"]),
            tp_route(
                "r-team1",
                Some("team1"),
                "primary",
                &["pool-shared", "pool-first"],
            ),
            tp_route(
                "r-team2",
                Some("team2"),
                "primary",
                &["pool-shared", "pool-second"],
            ),
        ],
    };
    topology.validate(TP_RUNTIME).unwrap();

    let references: Vec<&String> = topology
        .routes
        .iter()
        .flat_map(|route| &route.pool_ids)
        .collect();
    // One shared reservoir referenced by three routes, two distinct pools kept
    // apart: capacity is stated once per physical pool, never per route.
    assert_eq!(
        references.iter().filter(|id| **id == "pool-shared").count(),
        3
    );
    assert_eq!(topology.pools.len(), 3);
    assert_ne!(topology.routes[1].pool_ids[0], "pool-first");
}

// --- Slice validation (`agent_run.capacity.topology.validate_slice`) --------

/// Builds one measured sample for the named pool identity.
///
/// `remaining` is a percentage in `[0, 100]`; the sample observes at
/// `observed` and stays valid for 100 seconds so it can back a pool key.
fn tp_sample(key: Key, remaining: f64, observed: f64) -> Sample {
    Sample {
        key,
        remaining_percent: Some(remaining),
        reset_at: None,
        observed_at: Some(observed),
        valid_until: Some(observed + 100.0),
    }
}

/// Assembles one collection slice for the persistence gate behaviors.
fn tp_slice(
    runtime: &str,
    scope_id: &str,
    samples: Vec<Sample>,
    topology: Topology,
    observed_at: f64,
    valid_until: f64,
) -> Slice {
    Slice {
        runtime: runtime.into(),
        scope_id: scope_id.into(),
        samples,
        topology,
        observed_at,
        valid_until,
    }
}

/// Mirrors `tests/test_capacity_topology.py::TopologyValidationTests::test_slice_validates_everything_before_persistence`.
///
/// Rust validates the whole slice inside `persist`, which is the only path to
/// storage, so "validated before persistence" is observable as a rejected
/// write. The two Python sub-cases that pass a non-`LimitSample` sample and a
/// non-`CapacityTopology` topology are unrepresentable in the Rust types and
/// have no runtime counterpart.
#[test]
fn slice_validates_everything_before_persistence() {
    let home = tempfile::tempdir().unwrap();
    agent_run_store::Store::initialize(home.path()).unwrap();
    let shared = tp_key("primary", "five_hour", None);
    let topology = Topology {
        pools: vec![tp_pool("pool-shared", shared.clone())],
        routes: vec![],
    };

    // The whole slice is consistent: every pool key carries a measurement.
    let ok = tp_slice(
        TP_RUNTIME,
        "scope:default",
        vec![tp_sample(shared.clone(), 50.0, 100.0)],
        topology.clone(),
        100.0,
        200.0,
    );
    assert_eq!(persist(home.path(), &ok, 10).unwrap(), 1);

    let empty_topology = Topology::default();
    for (label, bad) in [
        (
            "blank runtime",
            tp_slice(
                "",
                "scope:default",
                vec![],
                empty_topology.clone(),
                100.0,
                200.0,
            ),
        ),
        (
            "blank scope",
            tp_slice(TP_RUNTIME, "", vec![], empty_topology.clone(), 100.0, 200.0),
        ),
        (
            "expiry precedes observation",
            tp_slice(
                TP_RUNTIME,
                "scope:default",
                vec![],
                empty_topology.clone(),
                200.0,
                100.0,
            ),
        ),
        (
            "pool key without a measurement in this slice",
            tp_slice(
                TP_RUNTIME,
                "scope:default",
                vec![],
                topology.clone(),
                100.0,
                200.0,
            ),
        ),
        (
            "non-finite observation",
            tp_slice(
                TP_RUNTIME,
                "scope:default",
                vec![],
                empty_topology,
                f64::NAN,
                200.0,
            ),
        ),
    ] {
        assert!(
            persist(home.path(), &bad, 10).is_err(),
            "{label} must not persist"
        );
    }
}

/// Mirrors `tests/test_capacity_topology.py::CollectSliceTests::test_same_runtime_keeps_distinct_scopes_and_key_runtimes`.
#[test]
fn same_runtime_keeps_distinct_scopes_and_key_runtimes() {
    let home = tempfile::tempdir().unwrap();
    agent_run_store::Store::initialize(home.path()).unwrap();
    let key = tp_key("primary", "five_hour", Some("team1"));
    let topology = Topology {
        pools: vec![tp_pool("pool-team1", key.clone())],
        routes: vec![],
    };
    assert!(topology
        .pools
        .iter()
        .flat_map(|pool| &pool.keys)
        .all(|key| key.runtime == TP_RUNTIME));

    for (scope, observed) in [("account:personal2", 1.0), ("account:work", 3.0)] {
        persist(
            home.path(),
            &tp_slice(
                TP_RUNTIME,
                scope,
                vec![tp_sample(key.clone(), 50.0, observed)],
                topology.clone(),
                observed,
                observed + 1.0,
            ),
            10,
        )
        .unwrap();
    }

    let store = agent_run_store::Store::open(home.path()).unwrap();
    let scopes: Vec<String> = store
        .conn
        .prepare("SELECT scope_id FROM capacity_route_snapshots WHERE runtime=? ORDER BY scope_id")
        .unwrap()
        .query_map([TP_RUNTIME], |row| row.get(0))
        .unwrap()
        .collect::<rusqlite::Result<_>>()
        .unwrap();
    assert_eq!(scopes, ["account:personal2", "account:work"]);

    let payloads: Vec<String> = store
        .conn
        .prepare("SELECT payload_json FROM capacity_route_snapshots WHERE runtime=?")
        .unwrap()
        .query_map([TP_RUNTIME], |row| row.get(0))
        .unwrap()
        .collect::<rusqlite::Result<_>>()
        .unwrap();
    for payload in payloads {
        let topology: Topology = serde_json::from_str(&payload).unwrap();
        assert_eq!(topology.pools[0].keys.first().unwrap().runtime, TP_RUNTIME);
    }
}

// --- Source topology (`agent_run.capacity.sources.sample_topology`) ---------

/// Mirrors `tests/test_capacity_topology.py::SourceTopologyTests::test_native_topology_ignores_sample_input_order`.
///
/// `conservative_topology` is reached through the public `normalize_claude`
/// normalizer, which is the native source's only caller.
#[test]
fn native_topology_ignores_sample_input_order() {
    let shared = serde_json::json!({"kind": "session", "percent": 50});
    let scoped = serde_json::json!({
        "kind": "weekly_scoped",
        "percent": 50,
        "scope": {"model": {"display_name": "Scoped-Alpha"}}
    });
    let forward = sources::normalize_claude(
        "clara-runtime",
        &serde_json::json!({"limits": [shared.clone(), scoped.clone()]}),
        1000.0,
    )
    .unwrap();
    let backward = sources::normalize_claude(
        "clara-runtime",
        &serde_json::json!({"limits": [scoped, shared]}),
        1000.0,
    )
    .unwrap();
    assert_eq!(forward.topology.pools, backward.topology.pools);
    assert_eq!(forward.topology.routes, backward.topology.routes);
    // One pool per sample identity, exactly as Python's `pools_from_samples`
    // groups them: the shared session window and the scoped weekly window.
    assert_eq!(forward.topology.pools.len(), 2);
}

/// Mirrors `tests/test_capacity_topology.py::SourceTopologyTests::test_legacy_samples_and_collect_samples_are_unchanged`.
///
/// The Rust normalizer borrows its samples, so the "grouping never rewrites
/// the samples it is given" half is checked by comparing the slice's samples
/// against the keys the source declared.
#[test]
fn legacy_samples_are_unchanged_by_topology_grouping() {
    let raw = serde_json::json!({"limits": [
        {"kind": "session", "percent": 50},
        {"kind": "weekly_scoped", "percent": 50, "scope": {"model": {"display_name": "Scoped-Alpha"}}}
    ]});
    let slice = sources::normalize_claude("clara-runtime", &raw, 1000.0).unwrap();
    assert_eq!(slice.samples.len(), 2);
    assert_eq!(slice.samples[0].key.lane, "primary");
    assert_eq!(slice.samples[0].key.target, None);
    assert_eq!(slice.samples[1].key.lane, "secondary");
    assert_eq!(slice.samples[1].key.target.as_deref(), Some("scoped-alpha"));
    // The declared topology names exactly the collected sample identities.
    let pooled: Vec<&Key> = slice
        .topology
        .pools
        .iter()
        .flat_map(|pool| &pool.keys)
        .collect();
    for sample in &slice.samples {
        assert!(pooled.contains(&&sample.key));
    }
    assert_eq!(pooled.len(), slice.samples.len());
}

// --- Route snapshot join (`agent_run.capacity.snapshot.build_capacity_routes`) ---
//
// Rust folds the exact-key snapshot join into `capacity::order`, which also
// ranks. The Python snapshot's `deferred`/`unavailable` split collapses into
// one `deferred` list there, so these behaviors assert on the evidence
// `reason`, which carries the same discrimination Python's two lists do.

/// Maps one literal epoch from `tests/test_capacity_snapshot.py` onto the live clock.
///
/// The Python suite injects `now=1_000.0`; `capacity::order` reads the real
/// clock instead, so every fixture timestamp is shifted by the same offset and
/// keeps its Python value visible at the call site. `at` is the live epoch.
fn sn_time(python_epoch: f64, at: f64) -> f64 {
    at + (python_epoch - 1000.0)
}

/// One configurable persisted sample/topology scope.
///
/// Mirrors `_append_scope` in `tests/test_capacity_snapshot.py`. `key_runtime`
/// and `route_runtime` override the payload's declared owner to build
/// deliberately malformed cross-runtime rows; `None` means the row's own
/// `runtime`. `samples` false leaves the exact topology key without history so
/// a missing forecast can be observed. `account` overrides the per-scope
/// launch account, which otherwise derives from the scope name.
struct Scope<'a> {
    scope: &'a str,
    runtime: &'a str,
    key_runtime: Option<&'a str>,
    route_runtime: Option<&'a str>,
    pool_id: &'a str,
    route_id: &'a str,
    lane: &'a str,
    account: Option<&'a str>,
    samples: bool,
    sample_valid_until: f64,
    snapshot_valid_until: f64,
}

impl Default for Scope<'_> {
    /// Returns the Python helper's defaults: a fresh, well-formed, sampled scope.
    fn default() -> Self {
        Self {
            scope: "scope",
            runtime: "opaque-runtime",
            key_runtime: None,
            route_runtime: None,
            pool_id: "pool",
            route_id: "route",
            lane: "lane",
            account: None,
            samples: true,
            sample_valid_until: 2_000.0,
            snapshot_valid_until: 1_100.0,
        }
    }
}

/// Persists one configurable scope's samples and route snapshot.
///
/// Writes straight through SQL rather than `persist` so the malformed and
/// sample-free scopes the Python fixtures need stay expressible. `at` is the
/// live epoch the fixture timestamps are anchored to.
fn append_scope(home: &std::path::Path, at: f64, spec: &Scope) {
    let store = agent_run_store::Store::open(home).unwrap();
    let account = spec
        .account
        .map(str::to_owned)
        .unwrap_or_else(|| format!("account-{}", spec.scope));
    let key_owner = spec.key_runtime.unwrap_or(spec.runtime);
    let route_owner = spec.route_runtime.unwrap_or(spec.runtime);
    if spec.samples {
        store.conn.execute(
            "INSERT INTO capacity_samples(runtime,lane,window,target,source,remaining_percent,reset_at,observed_at,valid_until,payload_json) VALUES(?,?,?,?,?,?,?,?,?,'null')",
            rusqlite::params![
                spec.runtime,
                spec.lane,
                "window",
                account,
                "source",
                50.0,
                sn_time(2_000.0, at),
                sn_time(900.0, at),
                sn_time(spec.sample_valid_until, at)
            ],
        ).unwrap();
    }
    let payload = serde_json::json!({
        "pools": [{
            "pool_id": spec.pool_id,
            "keys": [{
                "runtime": key_owner,
                "lane": spec.lane,
                "window": "window",
                "target": account,
                "source": "source"
            }]
        }],
        "routes": [{
            "route_id": spec.route_id,
            "runtime": route_owner,
            "account": account,
            "quota_lane": spec.lane,
            "pool_ids": [spec.pool_id]
        }]
    });
    store.conn.execute(
        "INSERT INTO capacity_route_snapshots(runtime,scope_id,observed_at,valid_until,payload_json) VALUES(?,?,?,?,?)",
        rusqlite::params![
            spec.runtime,
            spec.scope,
            sn_time(900.0, at),
            sn_time(spec.snapshot_valid_until, at),
            payload.to_string()
        ],
    ).unwrap();
}

/// Builds an agent-run home whose config enables exactly `runtimes`.
///
/// `capacity::order` filters the whole snapshot to enabled runtimes, so every
/// fixture runtime must be configured for its rows to be read at all.
fn order_home(runtimes: &[&str]) -> tempfile::TempDir {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path();
    let binary = if std::path::Path::new("/usr/bin/true").is_file() {
        "/usr/bin/true"
    } else {
        "/bin/true"
    };
    let mut text = String::from("schema_version=1\n");
    for name in runtimes {
        text.push_str(&format!(
            "[runtimes.{name}]\nenabled=true\nadapter=\"claude\"\nbinary={}\nhome={}\nmodels=[\"fixture\"]\nlimits_source=\"none\"\n",
            toml::Value::String(binary.into()),
            toml::Value::String(path.join("runtimes").join(name).to_string_lossy().into_owned()),
        ));
    }
    std::fs::write(path.join("config.toml"), text).unwrap();
    agent_run_store::Store::initialize(path).unwrap();
    temp
}

/// Returns every `(runtime, reason)` pair of deferral evidence in one order.
fn deferred_reasons(order: &serde_json::Value) -> std::collections::BTreeSet<(String, String)> {
    order["deferred"]
        .as_array()
        .unwrap()
        .iter()
        .map(|item| {
            (
                item["runtime"].as_str().unwrap().to_owned(),
                item["reason"].as_str().unwrap().to_owned(),
            )
        })
        .collect()
}

/// Mirrors `tests/test_capacity_snapshot.py::CapacitySnapshotTests::test_fresh_snapshot_joins_exact_key_forecast`.
#[test]
fn fresh_snapshot_joins_exact_key_forecast() {
    let at = agent_run_core::domain::now();
    let home = order_home(&["opaque-runtime"]);
    append_scope(
        home.path(),
        at,
        &Scope {
            scope: "fresh",
            ..Default::default()
        },
    );
    let order = capacity::order(home.path()).unwrap();
    assert_eq!(order["routes"].as_array().unwrap().len(), 1);
    assert_eq!(
        order["routes"][0]["limiting_key"]["target"],
        "account-fresh"
    );
}

/// Mirrors `tests/test_capacity_snapshot.py::CapacitySnapshotTests::test_identical_cross_scope_definitions_collapse`.
#[test]
fn identical_cross_scope_definitions_collapse() {
    let at = agent_run_core::domain::now();
    let home = order_home(&["opaque-runtime"]);
    for scope in ["one", "two"] {
        append_scope(
            home.path(),
            at,
            &Scope {
                scope,
                account: Some("shared-account"),
                ..Default::default()
            },
        );
    }
    let order = capacity::order(home.path()).unwrap();
    assert_eq!(order["routes"].as_array().unwrap().len(), 1);
    assert_eq!(order["routes"][0]["aliases"][0]["route_id"], "route");
    assert!(order["deferred"].as_array().unwrap().is_empty());
    // One physical route definition agreed on by two scopes stays one launch
    // descriptor; it must not be listed once per contributing scope.
    assert_eq!(order["routes"][0]["aliases"].as_array().unwrap().len(), 1);
}

/// Mirrors `tests/test_capacity_snapshot.py::CapacitySnapshotTests::test_conflicting_pool_and_route_definitions_remove_every_route`.
#[test]
fn conflicting_pool_and_route_definitions_remove_every_route() {
    let at = agent_run_core::domain::now();
    let home = order_home(&["opaque-runtime", "route-runtime"]);
    for (scope, pool_id, route_id, lane) in [
        ("pool-a", "shared", "route-a", "a"),
        ("pool-b", "shared", "route-b", "b"),
    ] {
        append_scope(
            home.path(),
            at,
            &Scope {
                scope,
                pool_id,
                route_id,
                lane,
                ..Default::default()
            },
        );
    }
    for (scope, pool_id, lane) in [("route-a", "pool-a", "a"), ("route-b", "pool-b", "b")] {
        append_scope(
            home.path(),
            at,
            &Scope {
                scope,
                runtime: "route-runtime",
                pool_id,
                route_id: "shared-route",
                lane,
                ..Default::default()
            },
        );
    }
    let order = capacity::order(home.path()).unwrap();
    assert!(order["routes"].as_array().unwrap().is_empty());
    let reasons: std::collections::BTreeSet<&str> = order["deferred"]
        .as_array()
        .unwrap()
        .iter()
        .map(|item| item["reason"].as_str().unwrap())
        .collect();
    assert_eq!(reasons, ["conflict"].into_iter().collect());
    let scopes: std::collections::BTreeSet<&str> = order["deferred"]
        .as_array()
        .unwrap()
        .iter()
        .map(|item| item["scope_id"].as_str().unwrap())
        .collect();
    assert_eq!(
        scopes,
        ["pool-a", "pool-b", "route-a", "route-b"]
            .into_iter()
            .collect()
    );
}

/// Mirrors `tests/test_capacity_snapshot.py::CapacitySnapshotTests::test_bad_scopes_do_not_hide_an_independent_fresh_scope`.
#[test]
fn bad_scopes_do_not_hide_an_independent_fresh_scope() {
    let at = agent_run_core::domain::now();
    let home = order_home(&[
        "runtime-good",
        "runtime-old",
        "runtime-cross",
        "runtime-bad",
    ]);
    append_scope(
        home.path(),
        at,
        &Scope {
            scope: "good",
            runtime: "runtime-good",
            ..Default::default()
        },
    );
    append_scope(
        home.path(),
        at,
        &Scope {
            scope: "expired",
            runtime: "runtime-old",
            snapshot_valid_until: 950.0,
            ..Default::default()
        },
    );
    append_scope(
        home.path(),
        at,
        &Scope {
            scope: "cross",
            runtime: "runtime-cross",
            key_runtime: Some("other-runtime"),
            route_runtime: Some("other-runtime"),
            samples: false,
            ..Default::default()
        },
    );
    append_scope(
        home.path(),
        at,
        &Scope {
            scope: "broken",
            runtime: "runtime-bad",
            samples: false,
            ..Default::default()
        },
    );
    agent_run_store::Store::open(home.path())
        .unwrap()
        .conn
        .execute(
            "UPDATE capacity_route_snapshots SET payload_json=? WHERE runtime=? AND scope_id=?",
            rusqlite::params!["{", "runtime-bad", "broken"],
        )
        .unwrap();

    let order = capacity::order(home.path()).unwrap();
    let runtimes: Vec<&str> = order["routes"]
        .as_array()
        .unwrap()
        .iter()
        .map(|route| route["runtime"].as_str().unwrap())
        .collect();
    assert_eq!(runtimes, ["runtime-good"]);
    let reasons = deferred_reasons(&order);
    assert!(reasons.contains(&("runtime-old".into(), "expired".into())));
    assert!(reasons.contains(&("runtime-bad".into(), "malformed".into())));
    assert!(reasons.contains(&("runtime-cross".into(), "malformed".into())));
    assert!(!reasons.iter().any(|(runtime, _)| runtime == "runtime-good"));
}

/// Mirrors `tests/test_capacity_snapshot.py::CapacitySnapshotTests::test_missing_unknown_and_legacy_evidence_never_become_routes`.
#[test]
fn missing_unknown_and_legacy_evidence_never_become_routes() {
    let at = agent_run_core::domain::now();
    let home = order_home(&["runtime-missing", "runtime-unknown", "legacy-runtime"]);
    append_scope(
        home.path(),
        at,
        &Scope {
            scope: "missing",
            runtime: "runtime-missing",
            samples: false,
            ..Default::default()
        },
    );
    append_scope(
        home.path(),
        at,
        &Scope {
            scope: "unknown",
            runtime: "runtime-unknown",
            sample_valid_until: 950.0,
            ..Default::default()
        },
    );
    // A legacy sample with no persisted topology infers no route at all.
    agent_run_store::Store::open(home.path())
        .unwrap()
        .conn
        .execute(
            "INSERT INTO capacity_samples(runtime,lane,window,target,source,remaining_percent,reset_at,observed_at,valid_until,payload_json) VALUES(?,?,?,?,?,?,?,?,?,'null')",
            rusqlite::params![
                "legacy-runtime",
                "legacy",
                "window",
                "legacy-account",
                "legacy-source",
                99.0,
                sn_time(2_000.0, at),
                sn_time(900.0, at),
                sn_time(2_000.0, at)
            ],
        )
        .unwrap();

    let order = capacity::order(home.path()).unwrap();
    assert!(order["routes"].as_array().unwrap().is_empty());
    let reasons = deferred_reasons(&order);
    assert!(reasons.contains(&("runtime-missing".into(), "missing_forecast".into())));
    assert!(reasons.contains(&("runtime-unknown".into(), "unknown_forecast".into())));
    assert!(!reasons
        .iter()
        .any(|(runtime, _)| runtime == "legacy-runtime"));
}

/// Mirrors `tests/test_capacity_snapshot.py::CapacitySnapshotTests::test_invalid_inputs_raise_validation_error`.
///
/// Rust does not take `retention` and `now` as snapshot arguments: the
/// retention bound is a validated config field and the observation time comes
/// from the clock. Both guards are still reachable -- config load rejects a
/// zero retention, and the ranking entry point rejects a non-finite `now`.
#[test]
fn invalid_snapshot_inputs_are_rejected() {
    let home = order_home(&["opaque-runtime"]);
    let mut text = std::fs::read_to_string(home.path().join("config.toml")).unwrap();
    text.push_str("[capacity]\nsample_retention=0\n");
    std::fs::write(home.path().join("config.toml"), text).unwrap();
    assert!(agent_run_core::config::Config::load(home.path()).is_err());

    assert!(capacity::ranking::rank_capacity_routes(
        vec![],
        vec![],
        &std::collections::BTreeMap::new(),
        &std::collections::BTreeMap::new(),
        f64::NAN,
    )
    .is_err());
}

/// Mirrors `tests/test_capacity_topology.py::TopologyValidationTests::test_validation_is_independent_of_input_order`.
///
/// Python canonicalizes pool/route order inside `validate_topology` so equal
/// parts compare equal. Rust keeps a topology in the order it was given and
/// canonicalizes where it matters instead: `order()` keys definitions by id,
/// so two scopes that declare the same parts in opposite order agree rather
/// than conflict.
#[test]
fn topology_validation_is_independent_of_input_order() {
    let at = agent_run_core::domain::now();
    let home = order_home(&[TP_RUNTIME]);
    let key_a = tp_key("primary", "five_hour", None);
    let key_b = tp_key("secondary", "seven_day", None);
    let pool_a = tp_pool("pool-a", key_a.clone());
    let pool_b = tp_pool("pool-b", key_b.clone());
    let route_a = tp_route("route-a", None, "primary", &["pool-a"]);
    let route_b = tp_route("route-b", Some("team1"), "secondary", &["pool-b"]);

    for (scope, pools, routes) in [
        (
            "forward",
            vec![pool_a.clone(), pool_b.clone()],
            vec![route_a.clone(), route_b.clone()],
        ),
        ("backward", vec![pool_b, pool_a], vec![route_b, route_a]),
    ] {
        let topology = Topology { pools, routes };
        topology.validate(TP_RUNTIME).unwrap();
        persist(
            home.path(),
            &tp_slice(
                TP_RUNTIME,
                scope,
                vec![
                    tp_fresh_sample(key_a.clone(), at),
                    tp_fresh_sample(key_b.clone(), at),
                ],
                topology,
                at - 100.0,
                at + 100.0,
            ),
            10,
        )
        .unwrap();
    }

    let order = capacity::order(home.path()).unwrap();
    assert!(
        order["deferred"]
            .as_array()
            .unwrap()
            .iter()
            .all(|item| item["reason"] != "conflict"),
        "reversed input order must not read as a conflicting definition"
    );
    let mut ids: Vec<&str> = order["routes"]
        .as_array()
        .unwrap()
        .iter()
        .flat_map(|route| route["aliases"].as_array().unwrap())
        .map(|alias| alias["route_id"].as_str().unwrap())
        .collect();
    ids.sort_unstable();
    assert_eq!(ids, ["route-a", "route-b"]);
}

/// Builds a sample that stays fresh for the whole of an `order()` call.
///
/// `capacity::order` reads the live clock, which advances past the `at` the
/// fixture captured, so the shelf life has to outlast the test rather than end
/// exactly at `at`. `remaining` is fixed at 50% so the route ranks instead of
/// being omitted as exhausted.
fn tp_fresh_sample(key: Key, at: f64) -> Sample {
    Sample {
        key,
        remaining_percent: Some(50.0),
        reset_at: None,
        observed_at: Some(at - 100.0),
        valid_until: Some(at + 1_000.0),
    }
}

// --- Bounded collection outcomes (`agent_run.capacity.collect`) -------------

/// Builds a home whose config declares one runtime per `(name, adapter, source)`.
///
/// Mirrors the per-runtime fixtures of `tests/test_capacity_outcomes.py`:
/// every runtime is enabled, so one round of `sources::collect` reports one
/// result per entry and each runtime's outcome is isolated from its siblings.
fn collect_home(runtimes: &[(&str, &str, &str)]) -> tempfile::TempDir {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path();
    let mut text = String::from("schema_version=1\n");
    for (name, adapter, source) in runtimes {
        text.push_str(&format!(
            "[runtimes.{name}]\nenabled=true\nadapter=\"{adapter}\"\nbinary={}\nhome={}\nmodels=[\"fixture\"]\nlimits_source=\"{source}\"\n",
            toml::Value::String("/bin/true".into()),
            toml::Value::String(path.join("runtimes").join(name).to_string_lossy().into_owned()),
        ));
    }
    std::fs::write(path.join("config.toml"), text).unwrap();
    agent_run_store::Store::initialize(path).unwrap();
    temp
}

/// Returns one runtime's result object from a collection round.
fn result_for<'a>(report: &'a serde_json::Value, runtime: &str) -> &'a serde_json::Value {
    report["results"]
        .as_array()
        .unwrap()
        .iter()
        .find(|item| item["runtime"] == runtime)
        .expect("every enabled runtime reports one result")
}

/// Mirrors `tests/test_capacity_outcomes.py::CapacityOutcomeRegressionTests::test_failed_empty_and_unsupported_outcomes_are_reported`.
#[tokio::test]
async fn failed_empty_and_unsupported_outcomes_are_reported() {
    let home = collect_home(&[
        // A provider the port does not implement fails with a fixed reason.
        ("failed", "qwen", "codexbar"),
        // The local Claude stream fallback finds no evidence at all.
        ("empty", "claude", "native"),
        ("unsupported", "claude", "none"),
    ]);
    let report = sources::collect(home.path()).await.unwrap();
    for (runtime, status) in [
        ("failed", "failed"),
        ("empty", "no_data"),
        ("unsupported", "unsupported"),
    ] {
        let result = result_for(&report, runtime);
        assert_eq!(result["status"], status, "{runtime}");
        assert_eq!(result["sample_count"], 0, "{runtime}");
    }
    // A failed or empty runtime never hides behind a successful round.
    assert_eq!(report["ok"], false);
}

/// Mirrors `tests/test_capacity_outcomes.py::CapacityOutcomeRegressionTests::test_capacity_collect_cli_preserves_status_counts_and_degraded_exit`.
///
/// The CLI maps this report to its exit code directly (`ok` -> 0, otherwise
/// 2), so the degraded/clean split is asserted on `ok`. An unsupported source
/// is a clean round in Python and must not degrade the exit code.
#[tokio::test]
async fn collect_report_marks_only_degraded_rounds() {
    let clean = collect_home(&[("unsupported", "claude", "none")]);
    let report = sources::collect(clean.path()).await.unwrap();
    assert_eq!(result_for(&report, "unsupported")["status"], "unsupported");
    assert_eq!(report["ok"], true);

    let degraded = collect_home(&[("failed", "qwen", "codexbar")]);
    let report = sources::collect(degraded.path()).await.unwrap();
    assert_eq!(result_for(&report, "failed")["status"], "failed");
    assert_eq!(report["ok"], false);
    // Fixed reason codes only: no provider output ever reaches the report.
    assert_eq!(
        result_for(&report, "failed")["issues"][0],
        "source_not_ported"
    );
}

/// Mirrors `tests/test_capacity_topology.py::CollectSliceTests::test_collect_slice_passes_none_through_for_unsupported_sources`.
///
/// Python returns `None` from `collect_slice` when the source concept does not
/// apply; Rust reports that same "not applicable" outcome as an `unsupported`
/// result carrying no samples and no issues, and never persists a slice.
#[tokio::test]
async fn unsupported_source_yields_no_slice() {
    let home = collect_home(&[("cortex-runtime", "claude", "none")]);
    let report = sources::collect(home.path()).await.unwrap();
    let result = result_for(&report, "cortex-runtime");
    assert_eq!(result["status"], "unsupported");
    assert_eq!(result["sample_count"], 0);
    assert!(result["issues"].as_array().unwrap().is_empty());
    assert_eq!(result["error"], serde_json::Value::Null);
    let store = agent_run_store::Store::open(home.path()).unwrap();
    let snapshots: i64 = store
        .conn
        .query_row("SELECT COUNT(*) FROM capacity_route_snapshots", [], |row| {
            row.get(0)
        })
        .unwrap();
    assert_eq!(snapshots, 0);
}

/// Mirrors `tests/test_capacity_topology.py::SourceTopologyTests::test_codexbar_routes_only_configured_accounts`.
///
/// Rust groups codexbar pools per account rather than per sample identity, so
/// one account's several windows are one reservoir with several keys instead
/// of several pools. The routing contract this behavior is about is identical:
/// only configured accounts become routes, and a discovered stranger keeps its
/// pool and samples as evidence without ever becoming launchable.
#[test]
fn codexbar_routes_only_configured_accounts() {
    let observed = "2026-09-15T12:00:00Z";
    let entry = |email: &str, secondary: bool| {
        let mut usage = serde_json::json!({
            "updatedAt": observed,
            "accountEmail": email,
            "primary": {"usedPercent": 50, "windowMinutes": 300}
        });
        if secondary {
            usage["secondary"] = serde_json::json!({"usedPercent": 50, "windowMinutes": 10080});
        }
        serde_json::json!({"usage": usage})
    };
    let raw = serde_json::json!([
        entry("default@example.test", false),
        entry("team1@example.test", true),
        entry("team2@example.test", false),
        entry("stranger@example.test", false),
    ]);
    let accounts: std::collections::BTreeMap<String, String> = [
        ("team1".to_owned(), "team1@example.test".to_owned()),
        ("team2".to_owned(), "team2@example.test".to_owned()),
    ]
    .into_iter()
    .collect();
    let slice = sources::normalize_codexbar_accounts(
        "cortex-runtime",
        &raw,
        &accounts,
        Some("default@example.test"),
    )
    .unwrap();

    let routed: std::collections::BTreeSet<Option<&str>> = slice
        .topology
        .routes
        .iter()
        .map(|route| route.account.as_deref())
        .collect();
    assert_eq!(
        routed,
        [None, Some("team1"), Some("team2")].into_iter().collect()
    );
    // Primary and secondary windows never split one account into two routes.
    let lanes: std::collections::BTreeSet<&str> = slice
        .topology
        .routes
        .iter()
        .map(|route| route.quota_lane.as_str())
        .collect();
    assert_eq!(lanes, ["default"].into_iter().collect());
    assert_eq!(slice.topology.routes.len(), 3);
    let team1 = slice
        .topology
        .routes
        .iter()
        .find(|route| route.account.as_deref() == Some("team1"))
        .unwrap();
    assert_eq!(team1.pool_ids.len(), 1);
    assert_eq!(
        slice
            .topology
            .pools
            .iter()
            .find(|pool| pool.pool_id == team1.pool_ids[0])
            .unwrap()
            .keys
            .len(),
        2
    );
    // The unknown email keeps its pool and samples but gets no route.
    assert_eq!(slice.topology.pools.len(), 4);
    assert!(!slice
        .topology
        .routes
        .iter()
        .any(|route| route.route_id.contains("stranger")));
    assert!(slice
        .samples
        .iter()
        .any(|sample| sample.key.target.as_deref() == Some("stranger@example.test")));
}
