//! Mirrors Python `tests/test_run_stats.py` against the Rust durable store.

mod common;

use agent_run_domain::domain::{AgentId, Outcome};
use agent_run_platform::{fs, verify};
use agent_run_store::{Store, run_stats};
use rusqlite::params;
use serde_json::{Value, json};
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

/// Mirrors `test_run_stats.py::test_a_created_agent_without_transitions_has_null_timestamps`.
///
/// An admitted-but-never-started run has no inferred lifecycle time or usage.
#[test]
fn python_test_run_stats_created_agent_has_null_timestamps() {
    let home = common::Home::new();
    let mut store = home.store();
    let id = agent(&mut store, &home);

    run_stats::record(&mut store, &id).unwrap();

    let row = statistics(&store, &id);
    assert_eq!(row["status"], "starting");
    assert_eq!(row["usage_source"], "none");
    assert!(row["started_at"].is_null());
    assert!(row["finished_at"].is_null());
    assert!(row["duration_seconds"].is_null());
    assert!(row["recorded_at"].is_number());
}

/// Mirrors `test_run_stats.py::test_a_failed_run_keeps_its_failure_kind_and_timestamps`.
///
/// A failure projection retains both the failure classifier and transition-derived duration.
#[test]
fn python_test_run_stats_failed_run_keeps_failure_and_times() {
    let home = common::Home::new();
    let mut store = home.store();
    let id = agent(&mut store, &home);
    terminal(&mut store, &id, "failed");
    store
        .conn
        .execute(
            "UPDATE agents SET failure_kind='stalled' WHERE id=?",
            [id.as_str()],
        )
        .unwrap();

    run_stats::record(&mut store, &id).unwrap();

    let row = statistics(&store, &id);
    assert_eq!(row["status"], "failed");
    assert_eq!(row["failure_kind"], "stalled");
    assert_eq!(row["started_at"], 100.0);
    assert_eq!(row["finished_at"], 110.0);
    assert_eq!(row["duration_seconds"], 10.0);
}

/// Mirrors `test_run_stats.py::test_recording_replaces_the_row_idempotently`.
///
/// Re-recording replaces one projection row after later journal evidence arrives.
#[test]
fn python_test_run_stats_recording_replaces_one_row() {
    let home = common::Home::new();
    let mut store = home.store();
    let id = agent(&mut store, &home);
    terminal(&mut store, &id, "succeeded");

    run_stats::record(&mut store, &id).unwrap();
    assert_eq!(statistics(&store, &id)["usage_source"], "none");
    store
        .event(&id, "runtime_result", &runtime_result_payload())
        .unwrap();
    run_stats::record(&mut store, &id).unwrap();

    assert_eq!(statistics(&store, &id)["usage_source"], "runtime_result");
    assert_eq!(
        store
            .conn
            .query_row(
                "SELECT COUNT(*) FROM run_stats WHERE agent_id=?",
                [id.as_str()],
                |row| row.get::<_, i64>(0),
            )
            .unwrap(),
        1
    );
}

/// Mirrors `test_run_stats.py::test_malformed_event_payloads_are_skipped_not_fatal`.
///
/// Corrupt historical usage JSON is ignored so its agent remains queryable.
#[test]
fn python_test_run_stats_skips_malformed_usage_events() {
    let home = common::Home::new();
    let mut store = home.store();
    let id = agent(&mut store, &home);
    terminal(&mut store, &id, "succeeded");
    store
        .conn
        .execute(
            "INSERT INTO events(agent_id,at,kind,data_json) VALUES(?,1.0,'runtime_result','not json')",
            [id.as_str()],
        )
        .unwrap();

    run_stats::record(&mut store, &id).unwrap();

    assert_eq!(statistics(&store, &id)["usage_source"], "none");
}

/// Mirrors `test_run_stats.py::test_backfill_counts_a_per_agent_failure_as_skipped`.
///
/// A projection write failure skips just that agent instead of aborting the backfill sweep.
#[test]
fn python_test_run_stats_backfill_counts_per_agent_failure_as_skipped() {
    let home = common::Home::new();
    let mut store = home.store();
    let id = agent(&mut store, &home);
    store
        .conn
        .execute_batch(
            "CREATE TRIGGER reject_run_stats BEFORE INSERT ON run_stats BEGIN SELECT RAISE(ABORT, 'fixture failure'); END;",
        )
        .unwrap();

    assert_eq!(run_stats::backfill(&mut store).unwrap(), (0, 1));
    assert_eq!(
        store
            .conn
            .query_row(
                "SELECT COUNT(*) FROM run_stats WHERE agent_id=?",
                [id.as_str()],
                |row| row.get::<_, i64>(0),
            )
            .unwrap(),
        0
    );
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

/// Marks one agent as a terminal Codex parent of the native thread `session`.
fn codex_parent(store: &mut Store, id: &AgentId, session: &str) {
    terminal(store, id, "succeeded");
    store
        .conn
        .execute(
            "UPDATE agents SET runtime_session_id=? WHERE id=?",
            params![session, id.as_str()],
        )
        .unwrap();
}

/// Admits one child row continuing `parent`'s native thread in its lineage.
fn resumed_child(
    store: &mut Store,
    home: &common::Home,
    parent: &AgentId,
    session: &str,
) -> AgentId {
    let child = agent(store, home);
    let root: String = store
        .conn
        .query_row(
            "SELECT root_agent_id FROM agents WHERE id=?",
            [parent.as_str()],
            |row| row.get(0),
        )
        .unwrap();
    store
        .conn
        .execute(
            "UPDATE agents SET parent_agent_id=?,root_agent_id=?,sequence=2,resume_of_runtime_session_id=? WHERE id=?",
            params![parent.as_str(), root, session, child.as_str()],
        )
        .unwrap();
    child
}

/// The baseline writer copies a completed parent's last cumulative Codex
/// usage payload once, and the resumed row's normalized statistics then
/// report exactly this execution's delta, never the thread total twice.
#[test]
fn resume_baseline_writer_copies_parent_usage_and_counts_only_the_delta() {
    let home = common::Home::new();
    let mut store = home.store();
    let parent = agent(&mut store, &home);
    codex_parent(&mut store, &parent, "thread-old");
    store
        .event(&parent, "thread/tokenUsage/updated", &token_usage_payload())
        .unwrap();
    let child = resumed_child(&mut store, &home, &parent, "thread-old");
    terminal(&mut store, &child, "succeeded");

    store.record_resume_usage_baseline(&child).unwrap();
    store
        .event(&child, "thread/tokenUsage/updated", &token_usage_payload())
        .unwrap();
    run_stats::record(&mut store, &child).unwrap();
    assert_eq!(statistics(&store, &child)["input_tokens"], 0);
    assert!(statistics(&store, &child)["num_turns"].is_null());
    // Repeating the launch-time writer must not append a second baseline.
    store.record_resume_usage_baseline(&child).unwrap();
    store
        .event(
            &child,
            "thread/tokenUsage/updated",
            &json!({"tokenUsage":{"total":{
                "inputTokens":6001,"outputTokens":1002,"cachedInputTokens":3500,
                "cacheWriteInputTokens":800,"reasoningOutputTokens":311,"totalTokens":6903
            },"_source":"token_usage_updated"}}),
        )
        .unwrap();
    run_stats::record(&mut store, &child).unwrap();

    let row = statistics(&store, &child);
    assert_eq!(row["usage_source"], "token_usage_updated");
    assert_eq!(row["input_tokens"], 1000);
    assert_eq!(row["output_tokens"], 100);
    assert_eq!(row["cache_read_tokens"], 500);
    assert_eq!(row["cache_write_tokens"], 100);
    assert_eq!(row["reasoning_tokens"], 100);
    assert_eq!(row["total_tokens"], 1000);
    let baselines: i64 = store
        .conn
        .query_row(
            "SELECT COUNT(*) FROM events WHERE agent_id=? AND kind='resume_usage_baseline'",
            [child.as_str()],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(baselines, 1, "the writer must be idempotent per agent");
}

/// A parent whose last usage belongs to another native session, or that
/// never reported usage, produces no baseline: the resumed row keeps every
/// measurement null instead of inventing a zero or a wrong delta.
#[test]
fn resume_baseline_writer_skips_incomparable_parents() {
    let home = common::Home::new();
    let mut store = home.store();

    // Another native session: the thread this child continues is not the one
    // the parent's usage was measured on.
    let parent = agent(&mut store, &home);
    codex_parent(&mut store, &parent, "thread-other");
    store
        .event(&parent, "thread/tokenUsage/updated", &token_usage_payload())
        .unwrap();
    let child = resumed_child(&mut store, &home, &parent, "thread-old");
    store.record_resume_usage_baseline(&child).unwrap();
    let baselines: i64 = store
        .conn
        .query_row(
            "SELECT COUNT(*) FROM events WHERE agent_id=? AND kind='resume_usage_baseline'",
            [child.as_str()],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(baselines, 0, "a foreign session must not seed a baseline");

    // No parent usage at all.
    let quiet = agent(&mut store, &home);
    codex_parent(&mut store, &quiet, "thread-quiet");
    let quiet_child = resumed_child(&mut store, &home, &quiet, "thread-quiet");
    store.record_resume_usage_baseline(&quiet_child).unwrap();
    terminal(&mut store, &quiet_child, "succeeded");
    store
        .event(
            &quiet_child,
            "thread/tokenUsage/updated",
            &token_usage_payload(),
        )
        .unwrap();
    run_stats::record(&mut store, &quiet_child).unwrap();
    let row = statistics(&store, &quiet_child);
    assert_eq!(row["usage_source"], "none");
    assert!(row["input_tokens"].is_null());
    assert!(row["total_tokens"].is_null());
}

/// A parent that ran a different model than the resumed child reports usage
/// that is not comparable; the writer keeps the child's measurements null.
#[test]
fn resume_baseline_writer_skips_foreign_model() {
    let home = common::Home::new();
    let mut store = home.store();
    let parent = agent(&mut store, &home);
    codex_parent(&mut store, &parent, "thread-model");
    store
        .event(&parent, "thread/tokenUsage/updated", &token_usage_payload())
        .unwrap();
    let child = resumed_child(&mut store, &home, &parent, "thread-model");
    // The comparability guard reads the durable request, so the child is
    // admitted with a different requested model rather than a column edit.
    store
        .conn
        .execute(
            "UPDATE agents SET request_json=json_set(request_json,'$.model','other-model') WHERE id=?",
            [child.as_str()],
        )
        .unwrap();
    store.record_resume_usage_baseline(&child).unwrap();
    let baselines: i64 = store
        .conn
        .query_row(
            "SELECT COUNT(*) FROM events WHERE agent_id=? AND kind='resume_usage_baseline'",
            [child.as_str()],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(baselines, 0, "a foreign model must not seed a baseline");
}

/// Native payload thread mismatches, malformed parent JSON, runtime changes
/// and counter resets cannot supply comparable resumed usage.
#[test]
fn resume_usage_rejects_incomparable_native_evidence() {
    for scenario in [
        "parent-thread",
        "child-thread",
        "malformed",
        "runtime",
        "reset",
    ] {
        let home = common::Home::new();
        let mut store = home.store();
        let parent = agent(&mut store, &home);
        codex_parent(&mut store, &parent, "thread-old");
        let mut payload = token_usage_payload();
        payload["threadId"] = json!(if scenario == "parent-thread" {
            "foreign"
        } else {
            "thread-old"
        });
        store
            .event(&parent, "thread/tokenUsage/updated", &payload)
            .unwrap();
        if scenario == "malformed" {
            store.conn.execute("UPDATE events SET data_json='invalid' WHERE agent_id=? AND kind='thread/tokenUsage/updated'", [parent.as_str()]).unwrap();
        }
        let child = resumed_child(&mut store, &home, &parent, "thread-old");
        if scenario == "runtime" {
            store.conn.execute("UPDATE agents SET request_json=json_set(request_json,'$.runtime','other-runtime') WHERE id=?", [child.as_str()]).unwrap();
        }
        store.record_resume_usage_baseline(&child).unwrap();
        let mut current = token_usage_payload();
        current["threadId"] = json!(if scenario == "child-thread" {
            "foreign"
        } else {
            "thread-old"
        });
        if scenario == "reset" {
            current["tokenUsage"]["total"]["inputTokens"] = json!(1);
        }
        store
            .event(&child, "thread/tokenUsage/updated", &current)
            .unwrap();
        run_stats::record(&mut store, &child).unwrap();
        let row = statistics(&store, &child);
        assert_eq!(row["usage_source"], "none", "{scenario}: {row}");
        assert!(row["input_tokens"].is_null(), "{scenario}: {row}");
        assert!(row["output_tokens"].is_null(), "{scenario}: {row}");
        assert!(row["num_turns"].is_null(), "{scenario}: {row}");
    }
}
