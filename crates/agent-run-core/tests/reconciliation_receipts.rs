//! No-op recovery sweeps must not manufacture durable writer traffic.
mod common;
use agent_run_core::service::Service;
use serde_json::json;

/// A stranded terminal attempt retains its ownership and evidence. Repeated
/// sweeps still inspect it but do not append another cursor/release receipt
/// for the same unresolved reason. No real process is adopted or signalled.
#[tokio::test]
async fn unchanged_orphan_sweeps_do_not_grow_receipts() {
    let home = common::Home::new();
    let (id, _) = home
        .store()
        .admit(&home.request(), &home.config, &json!({}), None)
        .unwrap();
    let store = home.store();
    store
        .conn
        .execute(
            "UPDATE agents SET status='failed',finished_at=100 WHERE id=?",
            [id.as_str()],
        )
        .unwrap();
    store.conn.execute("INSERT INTO attempts(id,agent_id,number,state,phase,adapter_state_json,created_at,finished_at,ownership_active) VALUES('stranded',?,1,'failed','running','{}',1,100,1)",[id.as_str()]).unwrap();
    let harness = common::writer::Harness::start(&home.path);
    let service = Service::new(home.path.clone()).with_writer(harness.broker());
    service.reconcile().await.unwrap();
    let before: i64 = store
        .conn
        .query_row("SELECT COUNT(*) FROM operation_receipts", [], |r| r.get(0))
        .unwrap();
    assert!(before > 0);
    service.reconcile().await.unwrap();
    let after: i64 = store
        .conn
        .query_row("SELECT COUNT(*) FROM operation_receipts", [], |r| r.get(0))
        .unwrap();
    assert_eq!(after, before);
    assert_eq!(
        store
            .conn
            .query_row(
                "SELECT ownership_active FROM attempts WHERE id='stranded'",
                [],
                |r| r.get::<_, i64>(0)
            )
            .unwrap(),
        1
    );
    assert_eq!(store.conn.query_row("SELECT COUNT(*) FROM events WHERE attempt_id='stranded' AND kind='attempt_cleanup_unresolved'",[],|r|r.get::<_,i64>(0)).unwrap(),1);
}
