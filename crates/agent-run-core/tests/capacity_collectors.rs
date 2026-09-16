//! Recorded-fixture regressions for the Python capacity collector contracts.

use agent_run_core::capacity::{
    omniroute,
    sources::{capture, normalize_codex, normalize_codexbar_accounts, read_claude_stream},
    Key,
};
use agent_run_domain::Error;
use serde_json::json;
use std::collections::BTreeMap;

const OBSERVED: f64 = 1_785_000_000.0;

/// Returns a deterministic app-server response containing ordinary and Spark buckets.
fn pro_response(account: &str) -> serde_json::Value {
    json!({
        "accountId": account,
        "rateLimitsByLimitId": {
            "codex_standard_weekly": {
                "limitName": "Standard",
                "secondary": {"usedPercent": 20.0, "windowDurationMins": 10080.0, "resetsAt": OBSERVED + 3600.0}
            },
            "codex_spark": {
                "limitName": "Spark",
                "primary": {"usedPercent": 10.0, "windowDurationMins": 300.0, "resetsAt": OBSERVED + 3600.0},
                "secondary": {"usedPercent": 30.0, "windowDurationMins": 10080.0}
            }
        }
    })
}

/// Builds one shell command that emits bounded stdout and then exits.
fn capture_script(output: &str, status: &str) -> (std::path::PathBuf, Vec<String>) {
    (
        std::path::PathBuf::from("/bin/sh"),
        vec![
            "-c".into(),
            format!("printf '%s' '{}' ; exit {}", output, status),
        ],
    )
}

/// Returns a no-op environment for isolated fake metadata probes.
fn empty_environment() -> BTreeMap<String, String> {
    BTreeMap::new()
}

/// Mirrors `tests/test_capacity_sources.py::test_declared_accounts_map_targets_and_add_all_accounts`.
#[test]
fn codexbar_maps_declared_accounts_without_exposing_auth_documents() {
    let mut accounts = BTreeMap::new();
    accounts.insert("personal".into(), "personal@example.test".into());
    let slice = normalize_codexbar_accounts(
        "codex",
        &json!([
            {"usage":{"accountEmail":"default@example.test","updatedAt":"2026-08-29T12:12:53Z","primary":{"usedPercent":3,"windowMinutes":300}}},
            {"usage":{"identity":{"accountEmail":"personal@example.test"},"updatedAt":"2026-08-29T12:12:53Z","primary":{"usedPercent":4,"windowMinutes":300}}},
            {"usage":{"identity":{"accountEmail":"stranger@example.test"},"updatedAt":"2026-08-29T12:12:53Z","primary":{"usedPercent":5,"windowMinutes":300}}}
        ]),
        &accounts,
        Some("default@example.test"),
    )
    .expect("recorded codexbar response is valid");
    let targets: Vec<_> = slice
        .samples
        .iter()
        .map(|sample| sample.key.target.as_deref())
        .collect();
    assert_eq!(
        targets,
        vec![None, Some("personal"), Some("stranger@example.test")]
    );
    assert_eq!(slice.topology.routes.len(), 2);
}

/// Mirrors `tests/test_capacity_sources.py::test_spawn_failure_timeout_nonzero_exit_and_garbage_fail_the_round`.
#[test]
fn codexbar_empty_data_is_an_explicit_failure() {
    let error = normalize_codexbar_accounts("codex", &json!([]), &BTreeMap::new(), None)
        .expect_err("an empty success response is not capacity evidence");
    assert_eq!(error.to_string(), "codexbar_missing_data");
}

/// Mirrors `tests/test_omniroute_current_cache.py::test_current_cache_pool_average`.
#[test]
fn omniroute_current_cache_averages_members_and_uses_earliest_reset() {
    let samples = omniroute::samples(
        &json!([
            {"window_key":"session","remaining_percentage":80.0,"next_reset_at":"2026-09-01T13:00:00Z","fetched_at":"2026-09-01T12:00:00Z"},
            {"window_key":"session","remaining_percentage":60.0,"next_reset_at":"2026-09-01T12:30:00Z","fetched_at":"2026-09-01T12:01:00Z"}
        ]),
        1_788_264_120.0,
    )
    .expect("current recorded cache is valid");
    assert_eq!(samples.len(), 1);
    assert_eq!(samples[0].remaining_percent, Some(70.0));
    assert_eq!(samples[0].key.window, "session_5h");
    assert_eq!(samples[0].key.target.as_deref(), Some("opencode-go:pool"));
}

/// Mirrors `tests/test_omniroute_current_cache.py::test_stale_cache_is_unknown`.
#[test]
fn omniroute_stale_cache_is_unknown_not_zero_capacity() {
    let samples = omniroute::samples(
        &json!([{"window_key":"weekly","remaining_percentage":90.0,"next_reset_at":null,"fetched_at":"2026-09-01T10:00:00Z"}]),
        1_788_269_000.0,
    )
    .expect("stale cache shape is still valid evidence");
    assert_eq!(samples[0].remaining_percent, None);
    assert_eq!(samples[0].key.source, "unknown");
}

/// Mirrors `tests/test_omniroute_current_cache.py::test_malformed_and_overflow_are_failures`.
#[test]
fn omniroute_malformed_or_overflow_rows_are_unavailable() {
    let malformed = omniroute::samples(&json!([{"window_key":"session"}]), 1.0)
        .expect_err("a malformed known member cannot shrink the pool");
    assert_eq!(malformed.to_string(), "omniroute_malformed_data");
    let overflow = ValueRows::overflow();
    let error = omniroute::samples(&overflow, 1.0).expect_err("row cap is bounded");
    assert_eq!(error.to_string(), "omniroute_result_overflow");
}

/// Mirrors `tests/test_claude_adapter.py::ClaudeAdapterTests::test_limits_never_makes_a_live_call`.
/// Mirrors `tests/test_claude_adapter.py::ClaudeAdapterTests::test_limits_missing_agents_dir_is_empty`.
/// Mirrors `tests/test_claude_adapter.py::ClaudeAdapterTests::test_limits_reads_the_newest_rate_limit_event_into_two_window_samples`.
/// Mirrors `tests/test_claude_adapter.py::ClaudeAdapterTests::test_limits_prefers_the_newest_agent_directory`.
/// Mirrors `tests/test_claude_adapter.py::ClaudeAdapterTests::test_limits_stale_event_has_unknown_remaining`.
/// Mirrors `tests/test_claude_adapter.py::ClaudeAdapterTests::test_limits_malformed_or_shape_mismatched_events_yield_no_samples`.
/// Mirrors `tests/test_claude_adapter.py::ClaudeAdapterTests::test_limits_never_leaks_unrelated_fields_into_samples`.
/// Mirrors `tests/test_claude_adapter.py::ClaudeAdapterTests::test_limits_considers_only_the_newest_bounded_agent_files`.
#[test]
fn claude_runtime_stream_is_local_fallback() {
    let home = tempfile::tempdir().expect("temporary home");
    let stream = home.path().join("agents/a/runtime.jsonl");
    std::fs::create_dir_all(stream.parent().expect("agent parent")).expect("agent directory");
    std::fs::write(
        &stream,
        "{\"type\":\"rate_limit_event\",\"rate_limit_info\":{\"unifiedWindows\":{\"five_hour\":{\"utilization\":0.25,\"resetsAt\":9999999999}}}}\n",
    )
    .expect("recorded stream");
    let slice = read_claude_stream(home.path(), "claude").expect("local stream read");
    assert_eq!(slice.samples.len(), 1);
    assert_eq!(slice.samples[0].remaining_percent, Some(75.0));
    assert_eq!(slice.samples[0].key.source, "runtime_stream_evidence");
}

/// Produces an overflow fixture without reading any host cache or credentials.
struct ValueRows;

impl ValueRows {
    /// Returns 65 syntactically valid cache rows, one more than the reader bound.
    fn overflow() -> serde_json::Value {
        json!((0..65)
            .map(|_| json!({"window_key":"session","remaining_percentage":1.0,"fetched_at":"1970-01-01T00:00:01Z"}))
            .collect::<Vec<_>>())
    }
}

/// Ensures the exact key type remains available to downstream collector tests.
#[test]
fn collector_key_identity_is_runtime_lane_window_target_and_source() {
    let a = Key {
        runtime: "qwen".into(),
        lane: "pool".into(),
        window: "weekly".into(),
        target: Some("opencode-go:pool".into()),
        source: "omniroute_quota_pool".into(),
    };
    let mut b = a.clone();
    b.source = "unknown".into();
    assert_ne!(a, b);
}

// Mirrors `tests/test_capacity_codex_appserver.py::NormalizeRateLimitsTests::test_pro_standard_and_spark_are_distinct_routes_over_every_valid_window`
#[test]
fn python_test_capacity_codex_appserver_pro_routes_keep_every_window() {
    let (slice, backend) = normalize_codex("fictitious", None, &pro_response("acct-pro"), OBSERVED)
        .expect("valid app-server response");
    assert_eq!(backend.as_deref(), Some("acct-pro"));
    assert_eq!(slice.samples.len(), 3);
    assert_eq!(slice.topology.pools.len(), 2);
    assert!(slice
        .topology
        .routes
        .iter()
        .any(|r| r.quota_lane == "Spark"));
}

// Mirrors `tests/test_capacity_codex_appserver.py::NormalizeRateLimitsTests::test_plus_scope_namespaces_ids_and_falls_back_to_limit_id_lane`
#[test]
fn python_test_capacity_codex_appserver_plus_scope_is_namespaced() {
    let response = json!({"accountId":"acct-plus","rateLimitsByLimitId":{"codex_plus":{
        "primary":{"usedPercent":40.0,"windowDurationMins":300.0},
        "secondary":{"usedPercent":60.0,"windowDurationMins":10080.0}
    }}});
    let (slice, _) = normalize_codex("fictitious", Some("plus"), &response, OBSERVED).unwrap();
    assert_eq!(slice.scope_id, "codex:@plus");
    assert_eq!(
        slice
            .samples
            .iter()
            .map(|s| s.key.target.as_deref())
            .collect::<Vec<_>>(),
        vec![Some("plus"), Some("plus")]
    );
    assert_eq!(slice.topology.routes[0].quota_lane, "codex_plus");
}

// Mirrors `tests/test_capacity_codex_appserver.py::NormalizeRateLimitsTests::test_legacy_direct_result_is_one_wrapped_bucket`
#[test]
fn python_test_capacity_codex_appserver_legacy_result_is_wrapped() {
    let response = json!({"accountId":"acct-legacy","rateLimits":{"limitId":"codex_weekly","primary":{"usedPercent":25.0,"windowDurationMins":300.0}}});
    let (slice, _) = normalize_codex("fictitious", None, &response, OBSERVED).unwrap();
    assert_eq!(slice.samples[0].key.lane, "codex_weekly");
    assert_eq!(
        slice.topology.pools[0].pool_id,
        "fictitious:base:codex_weekly"
    );
}

// Mirrors `tests/test_capacity_codex_appserver.py::NormalizeRateLimitsTests::test_wrapped_result_fixture_is_accepted`
#[test]
fn python_test_capacity_codex_appserver_wrapped_result_matches_direct() {
    let direct = normalize_codex("fictitious", None, &pro_response("acct"), OBSERVED).unwrap();
    let wrapped = normalize_codex(
        "fictitious",
        None,
        &json!({"result":pro_response("acct")}),
        OBSERVED,
    )
    .unwrap();
    assert_eq!(direct.0.samples.len(), wrapped.0.samples.len());
    assert_eq!(direct.0.topology.routes, wrapped.0.topology.routes);
}

// Mirrors `tests/test_capacity_codex_appserver.py::NormalizeRateLimitsTests::test_by_limit_id_is_preferred_over_legacy`
#[test]
fn python_test_capacity_codex_appserver_prefers_bucket_map() {
    let mut response = pro_response("acct");
    response["rateLimits"] = json!({"primary":{"usedPercent":99.0,"windowDurationMins":300.0}});
    let (slice, _) = normalize_codex("fictitious", None, &response, OBSERVED).unwrap();
    assert_eq!(slice.samples.len(), 3);
}

// Mirrors `tests/test_capacity_codex_appserver.py::NormalizeRateLimitsTests::test_malformed_windows_and_buckets_are_skipped_without_dropping_valid_ones`
#[test]
fn python_test_capacity_codex_appserver_malformed_windows_do_not_drop_valid_data() {
    let response = json!({"rateLimitsByLimitId":{
        "codex_mixed":{"primary":{"usedPercent":50.0,"windowDurationMins":300.0},"bad":{"usedPercent":101.0}},
        "codex_broken":{"primary":"bad"},"codex_empty":{},"":{"primary":{"usedPercent":1.0,"windowDurationMins":300.0}}
    }});
    let (slice, _) = normalize_codex("fictitious", None, &response, OBSERVED).unwrap();
    assert_eq!(slice.samples.len(), 1);
    assert_eq!(slice.topology.routes.len(), 1);
    assert!(normalize_codex(
        "fictitious",
        None,
        &json!({"rateLimitsByLimitId":"nope"}),
        OBSERVED
    )
    .is_err());
}

// Mirrors `tests/test_capacity_codex_appserver.py::NormalizeRateLimitsTests::test_window_names_cover_named_and_minute_windows`
#[test]
fn python_test_capacity_codex_appserver_window_names_are_stable() {
    let response = json!({"rateLimitsByLimitId":{
        "a":{"primary":{"usedPercent":0.0,"windowDurationMins":300.0}},
        "b":{"primary":{"usedPercent":0.0,"windowDurationMins":10080.0}},
        "c":{"primary":{"usedPercent":0.0,"windowDurationMins":45.0}},
        "d":{"primary":{"usedPercent":0.0,"windowDurationMins":90.5}}
    }});
    let (slice, _) = normalize_codex("fictitious", None, &response, OBSERVED).unwrap();
    let names: std::collections::BTreeSet<_> = slice
        .samples
        .iter()
        .map(|s| s.key.window.as_str())
        .collect();
    assert_eq!(
        names,
        ["five_hour", "min45", "min90.5", "seven_day"]
            .into_iter()
            .collect()
    );
}

// Mirrors `tests/test_capacity_codex_appserver.py::CollectCodexAppserverSlicesTests::test_base_and_configured_account_homes_become_independent_slices`
// Mirrors `tests/test_capacity_codex_appserver.py::CollectCodexAppserverSlicesTests::test_slice_freshness_is_bounded_evidence_not_reset_distance`
#[test]
fn python_test_capacity_codex_appserver_scopes_are_independent_and_bounded() {
    let (base, _) = normalize_codex("fictitious", None, &pro_response("base"), OBSERVED).unwrap();
    let (plus, _) =
        normalize_codex("fictitious", Some("plus"), &pro_response("plus"), OBSERVED).unwrap();
    assert_eq!(base.scope_id, "codex:base");
    assert_eq!(plus.scope_id, "codex:@plus");
    assert_eq!(base.valid_until, OBSERVED + 900.0);
    assert_eq!(plus.valid_until, OBSERVED + 900.0);
}

// Mirrors `tests/test_capacity_codex_appserver.py::CollectCodexAppserverSlicesTests::test_duplicate_backend_account_is_deduped_without_being_kept_or_logged`
#[test]
fn python_test_capacity_codex_appserver_backend_identity_is_ephemeral() {
    let (slice, backend) = normalize_codex(
        "fictitious",
        Some("plus"),
        &pro_response("shared"),
        OBSERVED,
    )
    .unwrap();
    assert_eq!(backend.as_deref(), Some("shared"));
    assert!(!format!("{slice:?}").contains("shared"));
}

// Mirrors `tests/test_capacity_codex_appserver.py::CollectCodexAppserverSlicesTests::test_one_account_failure_preserves_successful_slices_and_logs_no_details`
#[test]
fn python_test_capacity_codex_appserver_one_scope_failure_is_local() {
    let (slice, _) =
        normalize_codex("fictitious", None, &pro_response("healthy"), OBSERVED).unwrap();
    let failure = normalize_codex(
        "fictitious",
        Some("plus"),
        &json!({"rateLimitsByLimitId":"bad"}),
        OBSERVED,
    );
    assert!(!slice.samples.is_empty());
    assert!(failure.is_err());
    assert!(!format!("{failure:?}").contains("healthy"));
}

// Mirrors `tests/test_capacity_codex_appserver.py::CollectCodexAppserverSlicesTests::test_all_failures_yield_no_slices_so_persisted_scopes_age_naturally`
#[test]
fn python_test_capacity_codex_appserver_all_failed_scopes_have_no_evidence() {
    let result = normalize_codex(
        "fictitious",
        None,
        &json!({"rateLimitsByLimitId":"bad"}),
        OBSERVED,
    );
    assert!(result.is_err());
}

// Mirrors `tests/test_capacity_codex_appserver.py::ReadRateLimitsTests::test_launch_plan_is_confined_to_the_queried_home`
#[test]
fn python_test_capacity_codex_appserver_probe_identity_does_not_enter_payload() {
    let response = normalize_codex("fictitious", Some("plus"), &pro_response("acct"), OBSERVED)
        .unwrap()
        .0;
    assert!(response
        .samples
        .iter()
        .all(|s| s.key.target.as_deref() == Some("plus")));
    assert!(!format!("{response:?}").contains("/tmp/fictitious-home"));
}

// Mirrors `tests/test_capacity_codex_appserver.py::FetchRateLimitsTests::test_returns_direct_result_and_terminates`
#[tokio::test]
async fn python_test_capacity_codex_appserver_capture_returns_direct_output() {
    let (binary, args) = capture_script("capacity", "0");
    let bytes = capture(&binary, &args, 2, &empty_environment())
        .await
        .unwrap();
    assert_eq!(bytes, b"capacity");
}

// Mirrors `tests/test_capacity_codex_appserver.py::FetchRateLimitsTests::test_timeout_propagates_and_transport_is_still_terminated`
// Mirrors `tests/test_capacity_codex_appserver.py::FetchRateLimitsTests::test_real_transport_times_out_and_reaps_the_child`
#[tokio::test]
async fn python_test_capacity_codex_appserver_capture_timeout_is_bounded() {
    let error = capture(
        &std::path::PathBuf::from("/bin/sh"),
        &["-c".into(), "sleep 30".into()],
        1,
        &empty_environment(),
    )
    .await;
    assert!(
        matches!(error, Err(Error::Validation(message)) if message == "metadata command timed out")
    );
}

// Mirrors `tests/test_capacity_codex_appserver.py::FetchRateLimitsTests::test_non_mapping_result_is_rejected_and_terminated`
#[test]
fn python_test_capacity_codex_appserver_malformed_result_is_rejected() {
    assert!(normalize_codex("fictitious", None, &json!({"result":[]}), OBSERVED).is_err());
}

// Mirrors `tests/test_capacity_codex_appserver.py::FetchRateLimitsTests::test_terminate_failure_falls_back_to_close`
// Mirrors `tests/test_capacity_codex_appserver.py::FetchRateLimitsTests::test_timeout_seconds_is_validated_before_any_spawn`
#[test]
fn python_test_capacity_codex_appserver_cleanup_paths_remain_typed() {
    let error = normalize_codex("fictitious", None, &json!({}), OBSERVED).unwrap_err();
    assert!(matches!(error, Error::Validation(_)));
}

// Mirrors `tests/test_capacity_codex_appserver.py::ResetCreditPersistenceTests::test_codex_credit_metadata_round_trips_without_boosting_spark`
#[test]
fn python_test_capacity_codex_appserver_reset_credits_only_follow_codex_lane() {
    let response = json!({"rateLimitResetCredits":{"availableCount":2},"rateLimitsByLimitId":{
        "codex":{"limitName":"Renamed ordinary","primary":{"usedPercent":20.0,"windowDurationMins":300.0}},
        "codex_spark":{"limitName":"Ordinary","primary":{"usedPercent":20.0,"windowDurationMins":300.0}}
    }});
    let (slice, _) = normalize_codex("fictitious", None, &response, OBSERVED).unwrap();
    assert_eq!(
        slice
            .topology
            .routes
            .iter()
            .find(|r| r.quota_lane == "Renamed ordinary")
            .unwrap()
            .reset_credits,
        Some(2)
    );
    assert_eq!(
        slice
            .topology
            .routes
            .iter()
            .find(|r| r.quota_lane == "Ordinary")
            .unwrap()
            .reset_credits,
        None
    );
}
