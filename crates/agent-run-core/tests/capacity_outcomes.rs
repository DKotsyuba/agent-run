//! Atomic persistence of historical capacity scopes.
//! External collection failure and account deduplication are exercised by quota_collectors.
use agent_run_core::capacity;
use agent_run_store::Store;

/// Creates one valid quota slice for atomic persistence tests.
fn slice(runtime: &str, scope: &str, remaining: f64) -> capacity::Slice {
    let key = capacity::Key {
        runtime: runtime.into(),
        lane: "requests".into(),
        window: "five_hour".into(),
        target: None,
        source: "codex_appserver".into(),
    };
    let pool = capacity::Pool {
        pool_id: format!("{runtime}-{scope}"),
        keys: [key.clone()].into_iter().collect(),
    };
    capacity::Slice {
        runtime: runtime.into(),
        scope_id: scope.into(),
        samples: vec![capacity::Sample {
            key: key.clone(),
            remaining_percent: Some(remaining),
            reset_at: Some(2_000.0),
            observed_at: Some(1_000.0),
            valid_until: Some(1_100.0),
        }],
        topology: capacity::Topology {
            pools: vec![pool.clone()],
            routes: vec![capacity::Route {
                route_id: format!("{runtime}-{scope}-route"),
                runtime: runtime.into(),
                account: None,
                quota_lane: "requests".into(),
                pool_ids: vec![pool.pool_id],
                reset_credits: None,
            }],
        },
        observed_at: 1_000.0,
        valid_until: 1_100.0,
    }
}

/// Mirrors `tests/test_capacity_outcomes.py::CapacityOutcomeRegressionTests::test_middle_persistence_failure_keeps_neighboring_scopes_durable`
#[test]
fn test_middle_persistence_failure_keeps_neighboring_scopes_durable() {
    let scratch = tempfile::tempdir().expect("scratch root");
    Store::initialize(scratch.path()).expect("capacity store initializes");
    capacity::persist(scratch.path(), &slice("codex", "middle", 20.0), 1000)
        .expect("seed middle scope");
    let store = Store::open(scratch.path()).expect("capacity store opens");
    store
        .conn
        .execute_batch(
            "CREATE TRIGGER fail_middle BEFORE INSERT ON capacity_route_snapshots
             WHEN NEW.scope_id='middle' BEGIN SELECT RAISE(ABORT, 'provider-secret'); END;",
        )
        .expect("persistence trigger creates");
    drop(store);

    assert_eq!(
        capacity::persist(scratch.path(), &slice("codex", "first", 10.0), 1000).unwrap(),
        1
    );
    assert!(capacity::persist(scratch.path(), &slice("codex", "middle", 25.0), 1000).is_err());
    assert_eq!(
        capacity::persist(scratch.path(), &slice("codex", "last", 30.0), 1000).unwrap(),
        1
    );

    let store = Store::open(scratch.path()).expect("capacity store reopens");
    let mut statement = store
        .conn
        .prepare("SELECT remaining_percent FROM capacity_samples ORDER BY remaining_percent")
        .expect("sample query prepares");
    let rows = statement
        .query_map([], |row| row.get::<_, f64>(0))
        .expect("sample query runs")
        .collect::<rusqlite::Result<Vec<_>>>()
        .expect("sample rows decode");
    assert_eq!(rows, vec![10.0, 20.0, 30.0]);
}
