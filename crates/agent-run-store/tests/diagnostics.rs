use agent_run_store::diagnostics::diagnostic_snapshot;
use std::path::Path;

/// Mirrors Python `test_snapshots.py::test_diagnostic_snapshot_reads_python_written_database`.
#[test]
fn python_test_snapshots_fixture_is_read_only_and_bounded() {
    let fixture = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../tests/fixtures/baseline/db/current-v16.sqlite");
    let snapshot = diagnostic_snapshot(&fixture, 1_759_000_100.0, 256).unwrap();
    assert!(!snapshot.agents.is_empty());
    assert!(!snapshot.capacity.is_empty());
    assert_eq!(snapshot.agents[0]["status"], "running");
}
