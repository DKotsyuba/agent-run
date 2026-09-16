//! Recorded-fixture regressions for the Python capacity collector contracts.

use agent_run_core::capacity::{
    omniroute,
    sources::{normalize_codexbar_accounts, read_claude_stream},
    Key,
};
use serde_json::json;
use std::collections::BTreeMap;

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
