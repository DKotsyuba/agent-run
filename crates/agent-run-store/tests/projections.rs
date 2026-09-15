use agent_run_domain::domain::AgentId;
use agent_run_store::Store;
use std::path::Path;

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
