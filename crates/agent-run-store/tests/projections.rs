use agent_run_domain::domain::AgentId;
use agent_run_store::Store;
use std::path::Path;

mod common;

/// Admits one fresh test agent through the shared fixture home.
fn agent(store: &mut Store, home: &common::Home) -> AgentId {
    store
        .admit(&home.request(), &home.config, &serde_json::json!({}), None)
        .unwrap()
        .0
}

/// Mirrors Python `test_service.py::test_list_page_and_transcript_cursor` against Python-written data.
#[test]
fn python_test_service_fixture_pages_are_ordered_and_cursor_stable() {
    let fixture = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../tests/fixtures/baseline/db/current-v16.sqlite");
    let temporary = tempfile::tempdir().unwrap();
    std::fs::copy(&fixture, temporary.path().join("state.db")).unwrap();
    let store = Store::open(temporary.path()).unwrap();
    let page = store.agent_page_at(false, 0, 2, 1_759_000_100.0).unwrap();
    assert_eq!(page.total, 11);
    assert_eq!(page.items.len(), 2);
    assert!(page.items[0].created_at >= page.items[1].created_at);
    assert_eq!(page.next_offset, Some(2));
    assert_eq!(page.revision, 40);
}

/// Mirrors Python `test_state_store.py::test_transcript_uses_immutable_sequence_cursors`.
#[test]
fn python_test_state_store_fixture_transcript_rows_keep_sequence_order() {
    let fixture = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../tests/fixtures/baseline/db/current-v16.sqlite");
    let temporary = tempfile::tempdir().unwrap();
    std::fs::copy(&fixture, temporary.path().join("state.db")).unwrap();
    let store = Store::open(temporary.path()).unwrap();
    let id: AgentId = "ag-20260101-000003-0000000003".parse().unwrap();
    let first = store.transcript_page(&id, 0, 1).unwrap();
    assert_eq!(first.messages.len(), 1);
    let second = store
        .transcript_page(&id, first.next_cursor.unwrap(), 1)
        .unwrap();
    assert!(second.complete);
    assert!(second.messages[0].seq > first.messages[0].seq);
}

/// Writes one complete run_stats row with explicit zeros and nulls alike.
fn stats_row(store: &Store, id: &AgentId, input: Option<i64>, output: Option<i64>) {
    store
        .conn
        .execute(
            "INSERT OR REPLACE INTO run_stats(agent_id,runtime,model,profile,status,failure_kind,started_at,finished_at,duration_seconds,input_tokens,output_tokens,cache_read_tokens,cache_write_tokens,reasoning_tokens,total_tokens,num_turns,ttft_ms,api_duration_ms,cost_usd,usage_source,recorded_at) VALUES(?,?,?,?,?,NULL,1.0,2.0,1.0,?,?,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,'runtime_result',3.0)",
            rusqlite::params![id.as_str(), "mock", "fixture", "review", "succeeded", input, output],
        )
        .unwrap();
}

/// The public usage views mirror recorded measurements exactly: an observed
/// zero stays zero, an unmeasured field stays null, and no statistics row
/// leaves the whole latest-usage view absent rather than zero-filled.
#[test]
fn usage_views_preserve_observed_zero_and_unknown_null() {
    let home = common::Home::new();
    let mut store = home.store();
    let id = agent(&mut store, &home);
    // Before any statistics row exists the view is absent, not zeroed.
    assert!(store.usage_view(&id).unwrap().is_none());
    stats_row(&store, &id, Some(0), None);
    let usage = store.usage_view(&id).unwrap().unwrap();
    assert_eq!(usage.input_tokens, Some(0));
    assert_eq!(usage.output_tokens, None);
    assert_eq!(usage.usage_source, "runtime_result");
}

/// The lineage aggregate sums only complete evidence: one execution without
/// a statistics row keeps every total null, and completing it turns the same
/// history into exact sums without double counting.
#[test]
fn usage_cumulative_is_null_until_lineage_evidence_is_complete() {
    let home = common::Home::new();
    let mut store = home.store();
    let root = agent(&mut store, &home);
    store
        .conn
        .execute(
            "UPDATE agents SET root_agent_id=? WHERE id=?",
            rusqlite::params![root.as_str(), root.as_str()],
        )
        .unwrap();
    stats_row(&store, &root, Some(10), Some(4));
    let child = agent(&mut store, &home);
    store
        .conn
        .execute(
            "UPDATE agents SET parent_agent_id=?,root_agent_id=?,sequence=2 WHERE id=?",
            rusqlite::params![root.as_str(), root.as_str(), child.as_str()],
        )
        .unwrap();

    let incomplete = store.usage_cumulative(&root).unwrap();
    assert_eq!(incomplete.executions, 2);
    assert_eq!(incomplete.input_tokens, None, "a missing row is not zero");
    assert_eq!(incomplete.output_tokens, None);

    stats_row(&store, &child, Some(5), None);
    let partial = store.usage_cumulative(&root).unwrap();
    assert_eq!(partial.input_tokens, Some(15));
    assert_eq!(
        partial.output_tokens, None,
        "an unmeasured metric stays unknown rather than partial"
    );

    // The view keeps internal identifiers out of the public DTO.
    let value = serde_json::to_value(&partial).unwrap();
    assert!(value.get("agent_id").is_none());
    assert!(value.get("run_id").is_none());
}

/// Pruned lineage rows leave totals unknown; integer sums retain precision
/// above the floating-point exact-integer limit, and missing lineages stay null.
#[test]
fn usage_cumulative_requires_history_and_preserves_integer_precision() {
    let home = common::Home::new();
    let mut store = home.store();
    let root = agent(&mut store, &home);
    stats_row(&store, &root, Some(9_007_199_254_740_993), Some(0));
    assert_eq!(
        store.usage_cumulative(&root).unwrap().input_tokens,
        Some(9_007_199_254_740_993)
    );
    store
        .conn
        .execute("UPDATE agents SET sequence=2 WHERE id=?", [root.as_str()])
        .unwrap();
    let incomplete = store.usage_cumulative(&root).unwrap();
    assert_eq!(incomplete.executions, 1);
    assert_eq!(incomplete.input_tokens, None);
    assert_eq!(incomplete.output_tokens, None);
    let missing = store.usage_cumulative(&AgentId::new()).unwrap();
    assert_eq!(missing.executions, 0);
    assert_eq!(missing.input_tokens, None);
}

/// The agent view carries the admitted display label and both usage views.
#[test]
fn agent_view_carries_display_name_and_usage() {
    let home = common::Home::new();
    let mut store = home.store();
    let id = agent(&mut store, &home);
    store
        .conn
        .execute(
            "UPDATE agents SET display_name='Fixture Lead' WHERE id=?",
            [id.as_str()],
        )
        .unwrap();
    let view = store.agent_view_at(&id, 10.0).unwrap();
    assert_eq!(view.name.as_deref(), Some("Fixture Lead"));
    assert!(view.usage.is_none());
    assert_eq!(view.usage_cumulative.as_ref().unwrap().executions, 1);
}
