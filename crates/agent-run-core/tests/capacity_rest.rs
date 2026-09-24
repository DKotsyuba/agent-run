//! Remaining ported capacity behaviors.
//!
//! Covers the per-sample shelf life of `tests/test_capacity_topology.py`, the
//! source route shapes of its `SourceTopologyTests`, the nullable account
//! identities of `tests/test_capacity_identity.py`, and the native Claude
//! OAuth isolation of `tests/test_capacity_oauth_refresh.py`. Neighbour of
//! `capacity.rs`, `capacity_tail.rs`, and `capacity_collectors.rs`.

use agent_run_core::capacity::{
    self, account_token_with, persist,
    sources::{self, claude_native, claude_oauth_token, normalize_codex},
    Key, Route, Sample, Topology,
};
use serde_json::json;
use std::collections::{BTreeMap, BTreeSet};

/// Returns the live epoch, which `capacity::order` reads from the real clock.
fn at_now() -> f64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("epoch is in the past")
        .as_secs_f64()
}

/// Builds one already-normalized sample carrying its own declared shelf life.
///
/// `life` is the declared shelf life in seconds, materialized the way
/// `persist_slice` materializes Python's `valid_for_seconds`: as
/// `observed + life`. `None` leaves the sample without any declared shelf
/// life, exactly like a legacy sample whose `valid_for_seconds` is absent.
fn shelf_sample(
    runtime: &str,
    lane: &str,
    window: &str,
    target: Option<&str>,
    source: &str,
    observed: f64,
    life: Option<f64>,
) -> Sample {
    Sample {
        key: Key {
            runtime: runtime.into(),
            lane: lane.into(),
            window: window.into(),
            target: target.map(str::to_owned),
            source: source.into(),
        },
        remaining_percent: Some(60.0),
        reset_at: None,
        observed_at: Some(observed),
        valid_until: life.map(|life| observed + life),
    }
}

/// The epoch of the Python fixture's `2026-09-02T12:00:00Z` observation.
const OBSERVED: f64 = 1_788_091_200.0;

/// Mirrors `tests/test_capacity_topology.py::CollectSliceTests::test_collect_slice_validates_whole_slice_and_reports_shelf_life`.
#[test]
fn collect_slice_validates_whole_slice_and_reports_shelf_life() {
    let sample = shelf_sample(
        "cortex-runtime",
        "primary",
        "five_hour",
        Some("team1"),
        "native",
        OBSERVED,
        Some(600.0),
    );
    let shorter = shelf_sample(
        "cortex-runtime",
        "secondary",
        "weekly",
        Some("team1"),
        "native",
        OBSERVED,
        Some(300.0),
    );

    let slice = sources::slice_from_samples(
        "cortex-runtime",
        "cortex-runtime",
        vec![sample.clone(), shorter.clone()],
        Topology::default(),
        1_000.0,
    );

    assert_eq!(slice.scope_id, "cortex-runtime");
    assert_eq!(
        slice
            .samples
            .iter()
            .map(|sample| sample.key.clone())
            .collect::<Vec<_>>(),
        vec![sample.key, shorter.key]
    );
    assert_eq!(slice.observed_at, OBSERVED);
    // The slice expires at the SHORTEST positive per-sample shelf life: never
    // the longest one, and never the 900-second default while any sample
    // declares its own. A pool must not outlive its least-fresh constituent.
    assert_eq!(slice.valid_until, OBSERVED + 300.0);
}

/// Records what "shortest positive" means for every non-positive declaration.
///
/// Python filters with `sample.valid_for_seconds and sample.valid_for_seconds
/// > 0`, so an absent, zero, or negative value is ignored rather than
/// shortening (or expiring) the slice; only when no sample declares a positive
/// shelf life does the bounded 900-second default apply. A non-numeric
/// declaration cannot be expressed in Rust's typed `Sample`, where Python
/// raises `ValueError` before persistence begins.
#[test]
fn slice_shelf_life_ignores_absent_zero_and_negative_declarations() {
    let bounded = |life: Option<f64>| {
        sources::slice_from_samples(
            "fictitious",
            "fictitious",
            vec![shelf_sample(
                "fictitious",
                "primary",
                "five_hour",
                None,
                "native",
                OBSERVED,
                life,
            )],
            Topology::default(),
            1.0,
        )
        .valid_until
    };
    for ignored in [None, Some(0.0), Some(-60.0)] {
        assert_eq!(bounded(ignored), OBSERVED + 900.0, "{ignored:?}");
    }
    assert_eq!(bounded(Some(450.0)), OBSERVED + 450.0);

    // One positive declaration among ignored ones still governs the slice.
    let mixed: Vec<Sample> = [None, Some(0.0), Some(-60.0), Some(450.0)]
        .into_iter()
        .enumerate()
        .map(|(index, life)| {
            shelf_sample(
                "fictitious",
                "primary",
                &format!("window{index}"),
                None,
                "native",
                OBSERVED,
                life,
            )
        })
        .collect();
    let slice =
        sources::slice_from_samples("fictitious", "fictitious", mixed, Topology::default(), 1.0);
    assert_eq!(slice.valid_until, OBSERVED + 450.0);

    // With no sample at all the round's own epoch bounds the slice.
    let empty = sources::slice_from_samples(
        "fictitious",
        "fictitious",
        Vec::new(),
        Topology::default(),
        1_234.0,
    );
    assert_eq!(empty.observed_at, 1_234.0);
    assert_eq!(empty.valid_until, 1_234.0 + 900.0);
}

/// Mirrors `tests/test_capacity_topology.py::SourceTopologyTests::test_native_composes_shared_and_scoped_lanes`.
#[test]
fn native_composes_shared_and_scoped_lanes() {
    let slice = sources::normalize_claude(
        "clara-runtime",
        &json!({"limits":[
            {"kind":"session","percent":50},
            {"kind":"weekly_scoped","percent":50,"scope":{"model":{"display_name":"Scoped-Alpha"}}},
            {"kind":"weekly_scoped","percent":50,"scope":{"model":{"display_name":"Scoped-Beta"}}}
        ]}),
        OBSERVED,
    )
    .expect("native limits are valid evidence");

    let routes: BTreeMap<&str, &Route> = slice
        .topology
        .routes
        .iter()
        .map(|route| (route.quota_lane.as_str(), route))
        .collect();
    assert_eq!(
        routes.keys().copied().collect::<BTreeSet<_>>(),
        ["default", "scoped-alpha", "scoped-beta"]
            .into_iter()
            .collect::<BTreeSet<_>>()
    );
    assert!(slice.topology.routes.iter().all(|r| r.account.is_none()));
    assert_eq!(slice.topology.pools.len(), 3);

    let shared = slice
        .topology
        .pools
        .iter()
        .find(|pool| pool.keys.iter().all(|key| key.target.is_none()))
        .expect("one target-less shared pool")
        .pool_id
        .clone();
    // The shared default route spends only shared capacity, while every model
    // scope draws on the shared pool plus its own.
    assert_eq!(routes["default"].pool_ids, vec![shared.clone()]);
    for scope in ["scoped-alpha", "scoped-beta"] {
        assert!(routes[scope].pool_ids.contains(&shared));
        assert_eq!(routes[scope].pool_ids.len(), 2);
    }
}

/// Mirrors `tests/test_capacity_topology.py::SourceTopologyTests::test_omniroute_builds_one_aggregate_route`.
#[test]
fn omniroute_builds_one_aggregate_route() {
    let samples: Vec<Sample> = ["session_5h", "weekly"]
        .into_iter()
        .map(|window| {
            shelf_sample(
                "orion-runtime",
                "pool",
                window,
                None,
                "omniroute_quota_pool",
                OBSERVED,
                Some(900.0),
            )
        })
        .collect();
    let topology = sources::sample_topology("orion-runtime", "omniroute", &samples);

    let [route] = topology.routes.as_slice() else {
        panic!("the pooled source declares exactly one aggregate route");
    };
    assert_eq!(route.route_id, "orion-runtime:aggregate");
    assert_eq!(route.quota_lane, "aggregate");
    assert_eq!(route.account, None);
    // The aggregate route names every pool of the shared reservoir, in order.
    assert_eq!(
        route.pool_ids,
        topology
            .pools
            .iter()
            .map(|pool| pool.pool_id.clone())
            .collect::<Vec<_>>()
    );
    assert_eq!(topology.pools.len(), 2);
}

/// Returns one fresh Codex quota bucket with a fictitious backend identity.
///
/// Mirrors `_response` in `tests/test_capacity_identity.py`; `observed` and
/// `reset` are supplied so the fixture can be anchored to the live clock that
/// `capacity::order` reads.
fn identity_response(account: &str, reset: f64) -> serde_json::Value {
    json!({"accountId": account, "rateLimitsByLimitId": {
        "bucket": {"primary": {"usedPercent": 10, "windowDurationMins": 300, "resetsAt": reset}}
    }})
}

/// Builds an agent-run home whose config enables exactly `runtime`.
fn identity_home(runtime: &str) -> tempfile::TempDir {
    let temp = tempfile::tempdir().expect("temporary home");
    let path = temp.path();
    let binary = if std::path::Path::new("/usr/bin/true").is_file() {
        "/usr/bin/true"
    } else {
        "/bin/true"
    };
    std::fs::write(
        path.join("config.toml"),
        format!(
            "schema_version=1\n[runtimes.{runtime}]\nenabled=true\nadapter=\"codex\"\nbinary={}\nhome={}\nmodels=[\"fixture\"]\nlimits_source=\"none\"\n",
            toml::Value::String(binary.into()),
            toml::Value::String(path.join("runtimes").join(runtime).to_string_lossy().into_owned()),
        ),
    )
    .expect("config writes");
    agent_run_store::Store::initialize(path).expect("store initializes");
    temp
}

/// Mirrors `tests/test_capacity_identity.py::CapacityIdentityTests::test_base_label_persists_beside_the_absent_account`.
#[test]
fn base_label_persists_beside_the_absent_account() {
    let home = identity_home("runtime");
    let at = at_now();
    let observed = at - 1.0;
    let (absent, _) = normalize_codex(
        "runtime",
        None,
        &identity_response("a", at + 3_600.0),
        observed,
    )
    .expect("base scope is valid evidence");
    // A literal account label that looks exactly like the absent sentinel must
    // still be a separate scope and a separate physical pool.
    let (labelled, _) = normalize_codex(
        "runtime",
        Some("base"),
        &identity_response("b", at + 3_600.0),
        observed,
    )
    .expect("labelled scope is valid evidence");

    assert_eq!(
        [absent.scope_id.as_str(), labelled.scope_id.as_str()]
            .into_iter()
            .collect::<BTreeSet<_>>()
            .len(),
        2
    );
    assert_eq!(
        [&absent, &labelled]
            .into_iter()
            .flat_map(|slice| slice.topology.pools.iter().map(|pool| pool.pool_id.clone()))
            .collect::<BTreeSet<_>>()
            .len(),
        2
    );

    for slice in [&absent, &labelled] {
        persist(home.path(), slice, 10).expect("each scope persists on its own");
    }
    let store = agent_run_store::Store::open(home.path()).expect("store opens");
    let snapshots: i64 = store
        .conn
        .query_row("SELECT COUNT(*) FROM capacity_route_snapshots", [], |row| {
            row.get(0)
        })
        .expect("snapshot count");
    assert_eq!(snapshots, 2);

    let order = capacity::order(home.path()).expect("order reads both scopes");
    assert_eq!(order["routes"].as_array().expect("routes").len(), 2);
    assert!(order["deferred"].as_array().expect("deferred").is_empty());
}

/// Mirrors `tests/test_capacity_identity.py::CapacityIdentityTests::test_account_tokens_are_injective_for_separator_like_labels`.
#[test]
fn account_tokens_are_injective_for_separator_like_labels() {
    let labels = [
        None,
        Some("base"),
        Some("default"),
        Some("shared"),
        Some("@base"),
        Some("a:b"),
        Some("a%3Ab"),
        Some("α"),
    ];
    let tokens: Vec<String> = labels
        .iter()
        .map(|label| account_token_with(*label, "base"))
        .collect();
    assert_eq!(tokens.iter().collect::<BTreeSet<_>>().len(), labels.len());
    assert_eq!(tokens[0], "base");
    // No token may contain the separator that joins it into an identifier.
    assert!(tokens.iter().all(|token| !token.contains(':')));
}

/// Mirrors `tests/test_capacity_identity.py::CapacityIdentityTests::test_malformed_present_window_never_becomes_absent_capacity`.
#[test]
fn malformed_present_window_never_becomes_absent_capacity() {
    for malformed in [
        json!("bad"),
        json!({}),
        json!({"usedPercent": "oops", "windowDurationMins": 10080}),
    ] {
        let mut payload = identity_response("fictitious", 2_000.0);
        payload["rateLimitsByLimitId"]["bucket"]["secondary"] = malformed.clone();
        let (slice, _) = normalize_codex("runtime", None, &payload, 1_000.0)
            .expect("a valid primary is still evidence");
        assert_eq!(slice.samples.len(), 1, "{malformed}");
        // A present but unusable window disables the whole bucket's route: a
        // valid primary must never make an unknown secondary look unconstrained.
        assert!(slice.topology.routes.is_empty(), "{malformed}");
    }

    // An explicitly absent window is no quota at all, not a malformed one.
    let mut payload = identity_response("fictitious", 2_000.0);
    payload["rateLimitsByLimitId"]["bucket"]["secondary"] = serde_json::Value::Null;
    let (slice, _) =
        normalize_codex("runtime", None, &payload, 1_000.0).expect("absent windows are valid");
    assert_eq!(slice.topology.routes.len(), 1);
}

/// Mirrors `tests/test_capacity_identity.py::CapacityIdentityTests::test_valid_bucket_survives_a_malformed_sibling`.
#[test]
fn valid_bucket_survives_a_malformed_sibling() {
    let mut payload = identity_response("fictitious", 2_000.0);
    payload["rateLimitsByLimitId"]["bad"] = json!({
        "primary": {"usedPercent": 20, "windowDurationMins": 300},
        "secondary": {"usedPercent": "oops", "windowDurationMins": 10080}
    });
    let (slice, _) = normalize_codex("runtime", None, &payload, 1_000.0)
        .expect("an independent healthy bucket is still evidence");
    assert_eq!(slice.samples.len(), 2);
    assert_eq!(
        slice
            .topology
            .routes
            .iter()
            .map(|route| route.quota_lane.as_str())
            .collect::<Vec<_>>(),
        vec!["bucket"]
    );
}

/// Builds the minimal Claude native-capacity runtime these OAuth tests use.
///
/// `declared` adds the `environment` auth bridge naming
/// `CLAUDE_CODE_OAUTH_TOKEN`. The binary and home are placeholders because
/// native capacity must never spawn Claude or read its scoped credential store.
fn claude_runtime(temp: &std::path::Path, declared: bool) -> agent_run_config::config::Runtime {
    let auth = if declared {
        "\n[runtimes.claude.auth]\nkind=\"environment\"\nnames=[\"CLAUDE_CODE_OAUTH_TOKEN\"]\n"
    } else {
        ""
    };
    std::fs::write(
        temp.join("config.toml"),
        format!(
            "schema_version=1\n[runtimes.claude]\nenabled=true\nadapter=\"claude\"\nbinary={}\nhome={}\nmodels=[\"model-a\"]\nlimits_source=\"native\"\n{auth}",
            toml::Value::String("/usr/bin/fake-claude".into()),
            toml::Value::String(temp.join("fake-claude-home").to_string_lossy().into_owned()),
        ),
    )
    .expect("config writes");
    agent_run_config::config::Config::load(temp)
        .expect("config loads")
        .runtime("claude")
        .expect("claude runtime is configured")
        .clone()
}

/// Mirrors `tests/test_capacity_oauth_refresh.py::ClaudeCapacityOAuthRefreshTests::test_absent_explicit_oauth_is_unknown_without_cli_or_keychain_access`.
/// Mirrors `tests/test_capacity_oauth_refresh.py::ClaudeCapacityOAuthRefreshTests::test_declared_oauth_environment_remains_authoritative`.
/// Mirrors `tests/test_capacity_oauth_refresh.py::ClaudeCapacityOAuthRefreshTests::test_native_capacity_reports_a_safe_fixed_failure_without_explicit_oauth`.
///
/// The three behaviors share one test because they all manipulate the
/// process-wide `CLAUDE_CODE_OAUTH_TOKEN`, which cannot be done safely from
/// tests running concurrently in one binary.
#[tokio::test]
async fn native_claude_capacity_never_reaches_outside_declared_oauth() {
    let temp = tempfile::tempdir().expect("temporary home");
    let undeclared = claude_runtime(temp.path(), false);
    let declared = claude_runtime(temp.path(), true);

    // Scoped CLI state stays opaque rather than becoming a global-token
    // fallback: without a declared variable there is no token at all.
    std::env::remove_var("CLAUDE_CODE_OAUTH_TOKEN");
    assert_eq!(claude_oauth_token(&undeclared), None);
    assert_eq!(claude_oauth_token(&declared), None);

    // An export the runtime never declared must not widen the auth bridge.
    std::env::set_var("CLAUDE_CODE_OAUTH_TOKEN", "env-token");
    assert_eq!(claude_oauth_token(&undeclared), None);
    assert_eq!(
        claude_oauth_token(&declared).as_deref(),
        Some("env-token"),
        "a declared nonempty OAuth environment value powers native usage"
    );
    // An empty export is an absent token, never a blank bearer credential.
    std::env::set_var("CLAUDE_CODE_OAUTH_TOKEN", "");
    assert_eq!(claude_oauth_token(&declared), None);
    std::env::remove_var("CLAUDE_CODE_OAUTH_TOKEN");

    // No scoped credential read and no provider request is attempted when
    // native OAuth is unavailable; the failure is one fixed reason code.
    let error = claude_native("claude", &undeclared)
        .await
        .expect_err("native capacity without OAuth is a source failure");
    assert!(error.to_string().contains("retired"));
}
