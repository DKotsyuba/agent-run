//! Mirrors Python `tests/test_run_stats.py` against the Rust durable store.

mod common;

use agent_run_domain::domain::{AgentId, Outcome};
use agent_run_platform::{fs, verify};
use agent_run_store::{run_stats, Store};
use rusqlite::params;
use serde_json::{json, Value};
use std::path::Path;

/// The Python Claude-family terminal payload with every supported measurement.
fn runtime_result_payload() -> Value {
    json!({
        "duration_ms": 251533,
        "duration_api_ms": 240001.5,
        "num_turns": 29,
        "ttft_ms": 1234.5,
        "total_cost_usd": 0.818,
        "usage": {
            "cache_read_input_tokens": 676672,
            "cache_creation_input_tokens": 1234,
            "input_tokens": 39475,
            "output_tokens": 10812,
            "output_tokens_details": {"thinking_tokens": 4242}
        }
    })
}

/// The Python Codex cumulative token-usage payload with every supported counter.
fn token_usage_payload() -> Value {
    json!({"tokenUsage":{"total":{
        "inputTokens":5001,"outputTokens":902,"cachedInputTokens":3000,
        "cacheWriteInputTokens":700,"reasoningOutputTokens":211,"totalTokens":5903
    }}})
}

/// Admits one test agent and returns its durable id.
fn agent(store: &mut Store, home: &common::Home) -> AgentId {
    store
        .admit(&home.request(), &home.config, &json!({}), None)
        .unwrap()
        .0
}

/// Writes Python-equivalent lifecycle events at deterministic times.
fn terminal(store: &mut Store, id: &AgentId, status: &str) {
    store
        .conn
        .execute(
            "UPDATE agents SET status=?,started_at=100.0,finished_at=110.0 WHERE id=?",
            params![status, id.as_str()],
        )
        .unwrap();
    store
        .conn
        .execute(
            "INSERT INTO events(agent_id,at,kind,to_status,data_json) VALUES(?,100.0,'running','running','{}'),(?,110.0,'terminal',?,'{}')",
            params![id.as_str(), id.as_str(), status],
        )
        .unwrap();
}

/// Reads the persisted statistics projection for one agent after normalization.
fn statistics(store: &Store, id: &AgentId) -> Value {
    store.run_statistics(id).unwrap().unwrap()
}

/// Mirrors Python `test_runtime_result_payload_normalizes_the_claude_family_shape`.
#[test]
fn python_test_run_stats_runtime_result_normalizes_every_claude_measurement() {
    let home = common::Home::new();
    let mut store = home.store();
    let id = agent(&mut store, &home);
    terminal(&mut store, &id, "succeeded");
    store
        .event(&id, "runtime_result", &runtime_result_payload())
        .unwrap();
    run_stats::record(&mut store, &id).unwrap();

    let row = statistics(&store, &id);
    assert_eq!(row["usage_source"], "runtime_result");
    assert_eq!(row["input_tokens"], 39475);
    assert_eq!(row["output_tokens"], 10812);
    assert_eq!(row["cache_read_tokens"], 676672);
    assert_eq!(row["cache_write_tokens"], 1234);
    assert_eq!(row["reasoning_tokens"], 4242);
    assert_eq!(row["num_turns"], 29);
    assert_eq!(row["ttft_ms"], 1234.5);
    assert_eq!(row["api_duration_ms"], 240001.5);
    assert_eq!(row["cost_usd"], 0.818);
    assert!(row["total_tokens"].is_null());
    assert_eq!(row["started_at"], 100.0);
    assert_eq!(row["finished_at"], 110.0);
    assert_eq!(row["duration_seconds"], 10.0);
}

/// Mirrors Python `test_last_token_usage_update_normalizes_the_codex_shape`.
#[test]
fn python_test_run_stats_last_codex_token_update_wins() {
    let home = common::Home::new();
    let mut store = home.store();
    let id = agent(&mut store, &home);
    terminal(&mut store, &id, "succeeded");
    store
        .event(
            &id,
            "thread/tokenUsage/updated",
            &json!({"tokenUsage":{"total":{"inputTokens":1}}}),
        )
        .unwrap();
    store
        .event(&id, "thread/tokenUsage/updated", &token_usage_payload())
        .unwrap();
    run_stats::record(&mut store, &id).unwrap();

    let row = statistics(&store, &id);
    assert_eq!(row["usage_source"], "token_usage_updated");
    assert_eq!(row["input_tokens"], 5001);
    assert_eq!(row["total_tokens"], 5903);
    assert!(row["num_turns"].is_null());
    assert!(row["ttft_ms"].is_null());
    assert!(row["api_duration_ms"].is_null());
    assert!(row["cost_usd"].is_null());
}

/// Mirrors Python `test_resumed_codex_usage_requires_a_baseline_and_counts_only_the_delta`.
#[test]
fn python_test_run_stats_resumed_codex_requires_and_subtracts_baseline() {
    let home = common::Home::new();
    let mut store = home.store();
    let id = agent(&mut store, &home);
    terminal(&mut store, &id, "succeeded");
    store
        .conn
        .execute(
            "UPDATE agents SET resume_of_runtime_session_id='thread-old' WHERE id=?",
            [id.as_str()],
        )
        .unwrap();
    store
        .event(&id, "thread/tokenUsage/updated", &token_usage_payload())
        .unwrap();
    run_stats::record(&mut store, &id).unwrap();
    assert_eq!(statistics(&store, &id)["usage_source"], "none");

    store.event(&id, "resume_usage_baseline", &json!({"tokenUsage":{"total":{"inputTokens":4000,"outputTokens":800,"cachedInputTokens":2000,"cacheWriteInputTokens":600,"reasoningOutputTokens":200,"totalTokens":4800}}})).unwrap();
    run_stats::record(&mut store, &id).unwrap();
    let row = statistics(&store, &id);
    assert_eq!(row["usage_source"], "token_usage_updated");
    assert_eq!(row["input_tokens"], 1001);
    assert_eq!(row["total_tokens"], 1103);
}

/// Mirrors Python `test_an_agent_without_usage_events_records_all_nulls`.
#[test]
fn python_test_run_stats_absent_usage_stays_null() {
    let home = common::Home::new();
    let mut store = home.store();
    let id = agent(&mut store, &home);
    terminal(&mut store, &id, "succeeded");
    run_stats::record(&mut store, &id).unwrap();
    let row = statistics(&store, &id);
    assert_eq!(row["usage_source"], "none");
    for key in [
        "input_tokens",
        "output_tokens",
        "cache_read_tokens",
        "cache_write_tokens",
        "reasoning_tokens",
        "total_tokens",
        "num_turns",
        "ttft_ms",
        "api_duration_ms",
        "cost_usd",
    ] {
        assert!(row[key].is_null(), "{key}");
    }
}

/// Mirrors Python `test_backfill_fills_missing_rows_and_is_idempotent`.
#[test]
fn python_test_run_stats_backfill_is_idempotent() {
    let home = common::Home::new();
    let mut store = home.store();
    let claude = agent(&mut store, &home);
    let codex = agent(&mut store, &home);
    terminal(&mut store, &claude, "succeeded");
    terminal(&mut store, &codex, "succeeded");
    store
        .event(&claude, "runtime_result", &runtime_result_payload())
        .unwrap();
    run_stats::record(&mut store, &codex).unwrap();
    assert_eq!(run_stats::backfill(&mut store).unwrap(), (1, 0));
    assert_eq!(statistics(&store, &claude)["input_tokens"], 39475);
    assert_eq!(run_stats::backfill(&mut store).unwrap(), (0, 0));
}

/// Mirrors the terminal statistics assertions in Python `tests/test_service.py`.
#[test]
fn python_test_service_terminal_commit_projects_runtime_usage() {
    let home = common::Home::new();
    let mut store = home.store();
    let id = agent(&mut store, &home);
    store.running(&id, 42).unwrap();
    let root = home.path.join("agents").join(id.as_str());
    fs::private_dir(&root).unwrap();
    let proof = verify::seal(&root, Path::new("answer.md"), "answer").unwrap();
    store
        .finish(
            &id,
            &Outcome::success(None),
            Some(&proof),
            Some(&runtime_result_payload()),
        )
        .unwrap();
    let row = statistics(&store, &id);
    assert_eq!(row["usage_source"], "runtime_result");
    assert_eq!(row["input_tokens"], 39475);
    assert_eq!(row["ttft_ms"], 1234.5);
    assert_eq!(row["api_duration_ms"], 240001.5);
}

/// Copies the immutable Python fixture before any SQLite connection can create sidecars.
fn copy_golden_database(destination: &Path) {
    std::fs::copy(
        concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../tests/fixtures/baseline/db/current-v16.sqlite"
        ),
        destination.join("state.db"),
    )
    .unwrap();
}

/// Proves Python-written `run_stats` rows retain exact field names, nulls, and aggregates.
#[test]
fn python_test_run_stats_golden_v16_rows_are_read_without_shape_drift() {
    let home = tempfile::tempdir().unwrap();
    copy_golden_database(home.path());
    let store = Store::open(home.path()).unwrap();
    let id: AgentId = "ag-20260101-000005-0000000005".parse().unwrap();
    let row = statistics(&store, &id);
    assert_eq!(row["runtime"], "codex");
    assert_eq!(row["status"], "succeeded");
    assert_eq!(row["input_tokens"], 10);
    assert_eq!(row["output_tokens"], 20);
    assert_eq!(row["num_turns"], 1);
    assert_eq!(row["cost_usd"], 0.01);
    assert!(row["ttft_ms"].is_null());
    assert!(row["api_duration_ms"].is_null());
    assert_eq!(row["usage_source"], "runtime_result");
}
