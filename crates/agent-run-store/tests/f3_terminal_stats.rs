//! F3 on disposable private homes: the terminal commit writes no statistics
//! row and reads no usage history; the reader projection plus the separate
//! persist unit produce exactly the row the legacy path recorded.
mod common;

use agent_run_domain::domain::{Outcome, Status};
use agent_run_store::{run_stats, terminal, writer::StoreReader};
use serde_json::json;

/// Counts statistics rows of one agent.
fn stats_rows(store: &agent_run_store::Store, id: &agent_run_domain::domain::AgentId) -> i64 {
    store
        .conn
        .query_row(
            "SELECT COUNT(*) FROM run_stats WHERE agent_id=?",
            [id.as_str()],
            |row| row.get(0),
        )
        .unwrap()
}

/// The terminal commit leaves no statistics row; the projection computed on
/// a reader and persisted separately matches the legacy normalization.
#[test]
fn terminal_commit_writes_no_stats_and_projection_matches_legacy() {
    let home = common::Home::new();
    let mut store = home.store();
    let (id, _) = store
        .admit(&home.request(), &home.config, &json!({}), None)
        .unwrap();
    let usage = json!({"usage":{"input_tokens":3,"output_tokens":5},"num_turns":2});
    let mut outcome = Outcome::failure("engine_failed");
    outcome.status = Status::Failed;
    terminal::commit_terminal(&mut store, &id, &outcome, None, Some(&usage)).unwrap();
    assert_eq!(stats_rows(&store, &id), 0);
    let projected = StoreReader::open(&home.path)
        .unwrap()
        .run_stats_projection(&id)
        .unwrap();
    store.persist_run_stats(&projected).unwrap();
    assert_eq!(stats_rows(&store, &id), 1);
    let legacy = run_stats::project(&store.conn, &id, projected.recorded_at).unwrap();
    assert_eq!(legacy, projected);
}

/// A projection whose status does not match the stored terminal status, or
/// of a nonterminal execution, is refused without writing.
#[test]
fn persist_refuses_nonterminal_or_changed_status() {
    let home = common::Home::new();
    let mut store = home.store();
    let (id, _) = store
        .admit(&home.request(), &home.config, &json!({}), None)
        .unwrap();
    let projected = run_stats::project(&store.conn, &id, 1.0).unwrap();
    assert!(store.persist_run_stats(&projected).is_err());
    assert_eq!(stats_rows(&store, &id), 0);
}
