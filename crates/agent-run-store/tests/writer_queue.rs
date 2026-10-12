//! Application adapter acceptance on isolated fixture databases; no native engine is run.
mod common;

use agent_run_domain::{
    domain::AgentId,
    storage::{
        MonoDeadline, OpStatus, OperationId, ProducerId, ProducerKind, RejectCode, SubmitResult,
        WriteAuthority,
    },
};
use agent_run_store::{
    ops::{AppendJournal, JournalFragment, RemoteOp, WriteEnvelope},
    writer::{StoreWriter, WriterLease, queued::QueuedWriter},
};
use serde_json::json;
use std::{future::Future, pin::Pin, task::Poll, time::Duration};

/// Create an admitted fixture and adopt the sole writer's already-fenced connection.
fn setup() -> (common::Home, QueuedWriter, AgentId) {
    let home = common::Home::new();
    let (id, _) = home
        .store()
        .admit(&home.request(), &home.config, &json!({}), None)
        .unwrap();
    let mut writer = StoreWriter::open(WriterLease::acquire_online(&home.path).unwrap()).unwrap();
    writer.set_busy_wait(Duration::from_millis(30)).unwrap();
    let queue = writer.into_queue(sqlite_access::Limits::default()).unwrap();
    (home, queue, id)
}

/// Build one immutable request whose original identity can resolve a lost caller result.
fn envelope(id: &AgentId) -> WriteEnvelope {
    envelope_for(id, "one")
}

/// Distinguish concurrent producer lanes while keeping each immutable operation ordinal at one.
fn envelope_for(id: &AgentId, instance: &str) -> WriteEnvelope {
    let (boot, now) = agent_run_platform::monotonic::now().unwrap();
    WriteEnvelope::new(
        OperationId::new(&boot, now),
        ProducerId {
            kind: ProducerKind::Broker,
            scope: Some("queue-test".into()),
            instance: instance.into(),
        },
        1,
        MonoDeadline {
            boot_id: boot,
            ns: now + 60_000_000_000,
        },
        RemoteOp::from(AppendJournal {
            execution: id.clone(),
            fragments: vec![JournalFragment::Event {
                kind: "queued_test".into(),
                data: json!({"value": 1}),
                attempt: None,
            }],
        }),
    )
    .unwrap()
}

/// Count one kind of durable event without participating in write scheduling.
fn events(home: &common::Home, id: &AgentId) -> i64 {
    home.store()
        .conn
        .query_row(
            "SELECT count(*) FROM events WHERE agent_id=? AND kind='queued_test'",
            [id.as_str()],
            |r| r.get(0),
        )
        .unwrap()
}

/// First polling must acquire credits and enqueue before the waiter can be abandoned.
async fn admit<F: Future>(future: Pin<&mut F>) {
    let mut future = future;
    std::future::poll_fn(|cx| {
        assert!(future.as_mut().poll(cx).is_pending());
        Poll::Ready(())
    })
    .await;
}

/// Queue adoption preserves epoch/fence and commits exactly one event plus one immutable receipt.
#[tokio::test]
async fn same_fenced_writer_replays_and_resolves_original_identity() {
    let (home, queue, id) = setup();
    let epoch: i64 = home
        .store()
        .conn
        .query_row("SELECT epoch FROM writer_authority", [], |r| r.get(0))
        .unwrap();
    assert_eq!(queue.epoch(), epoch as u64);
    assert!(WriterLease::acquire_online(&home.path).is_err());
    let request = envelope(&id);
    let operation = request.header().operation_id.clone();
    let digest = request.header().digest.clone();
    let SubmitResult::Committed(first) =
        queue.submit(WriteAuthority::Broker, request.clone()).await
    else {
        panic!("first mutation not committed");
    };
    assert!(!first.replayed);
    let SubmitResult::Committed(replayed) = queue.submit(WriteAuthority::Broker, request).await
    else {
        panic!("receipt replay failed");
    };
    assert!(replayed.replayed);
    assert_eq!(replayed.commit_seq, first.commit_seq);
    assert_eq!(replayed.result, first.result);
    let OpStatus::Committed(status) = queue
        .status(WriteAuthority::Broker, operation, digest)
        .await
    else {
        panic!("original identity unresolved");
    };
    assert_eq!(status.commit_seq, first.commit_seq);
    assert_eq!(events(&home, &id), 1);
    assert_eq!(
        home.store()
            .conn
            .query_row("SELECT count(*) FROM operation_receipts", [], |r| r
                .get::<_, i64>(0))
            .unwrap(),
        1
    );
    queue.close().await.unwrap();
    assert!(WriterLease::acquire_online(&home.path).is_ok());
}

/// Exhausted BUSY is non-durable and recoverable after release without a duplicate business effect.
#[tokio::test]
async fn busy_refusal_then_same_identity_recovery() {
    let (home, queue, id) = setup();
    let request = envelope(&id);
    let operation = request.header().operation_id.clone();
    let digest = request.header().digest.clone();
    let blocker = rusqlite::Connection::open(home.path.join("state.db")).unwrap();
    blocker.execute_batch("BEGIN IMMEDIATE").unwrap();
    let SubmitResult::Rejected(refused) =
        queue.submit(WriteAuthority::Broker, request.clone()).await
    else {
        panic!("locked mutation falsely succeeded");
    };
    assert_eq!(refused.code, RejectCode::StorageUnavailable);
    assert!(!refused.durable);
    assert_eq!(
        refused.stage,
        agent_run_domain::storage::RejectStage::Acquire
    );
    let diagnostic = refused.persistence.unwrap();
    assert_eq!(
        diagnostic.phase,
        agent_run_domain::storage::RejectStage::Acquire
    );
    assert_eq!(diagnostic.sqlite_primary, Some(5));
    assert_eq!(diagnostic.attempts, 1);
    assert!(queue.degraded());
    assert_eq!(events(&home, &id), 0);
    assert!(matches!(
        queue
            .status(WriteAuthority::Broker, operation.clone(), digest.clone())
            .await,
        OpStatus::NotFound { .. }
    ));
    let SubmitResult::Rejected(again) = queue.submit(WriteAuthority::Broker, request.clone()).await
    else {
        panic!("continued lock falsely succeeded");
    };
    assert!(!again.durable);
    assert_eq!(
        again.code,
        RejectCode::StorageUnavailable,
        "recovery BEGIN BUSY must stay retryable, never StorageRolledBack"
    );
    assert_eq!(again.stage, agent_run_domain::storage::RejectStage::Acquire);
    let diagnostic = again.persistence.unwrap();
    assert_eq!(
        diagnostic.phase,
        agent_run_domain::storage::RejectStage::Acquire
    );
    assert_eq!(diagnostic.sqlite_primary, Some(5));
    assert_eq!(diagnostic.attempts, 1);
    assert_eq!(events(&home, &id), 0);
    blocker.execute_batch("ROLLBACK").unwrap();
    assert!(matches!(
        queue.submit(WriteAuthority::Broker, request.clone()).await,
        SubmitResult::Committed(_)
    ));
    assert!(!queue.degraded());
    assert_eq!(events(&home, &id), 1);
    assert!(matches!(
        queue
            .status(WriteAuthority::Broker, operation, digest)
            .await,
        OpStatus::Committed(_)
    ));
    assert!(matches!(
        queue.submit(WriteAuthority::Broker, request).await,
        SubmitResult::Committed(_)
    ));
    assert_eq!(events(&home, &id), 1);
    queue.close().await.unwrap();
}

/// Abandoning an accepted caller cannot release application authority or cancel its durable mutation.
#[tokio::test]
async fn cancelled_application_write_retains_fence_until_drain() {
    let (home, queue, id) = setup();
    let (entered_tx, entered_rx) = tokio::sync::oneshot::channel();
    let (release_tx, release_rx) = std::sync::mpsc::channel();
    let blocked = queue.clone();
    let barrier =
        tokio::spawn(async move { blocked.fixture_barrier(entered_tx, release_rx).await });
    entered_rx.await.unwrap();
    let request = envelope(&id);
    let operation = request.header().operation_id.clone();
    let digest = request.header().digest.clone();
    let mut call = Box::pin(queue.submit(WriteAuthority::Broker, request));
    admit(call.as_mut()).await;
    drop(call);
    assert_eq!(queue.snapshot().accepted_jobs, 2);
    assert!(WriterLease::acquire_online(&home.path).is_err());
    release_tx.send(()).unwrap();
    barrier.await.unwrap().unwrap();
    // Status is ordered after the admitted write on the same library connection.
    assert!(matches!(
        queue
            .status(WriteAuthority::Broker, operation, digest)
            .await,
        OpStatus::Committed(_)
    ));
    assert_eq!(events(&home, &id), 1);
    queue.close().await.unwrap();
    assert!(WriterLease::acquire_online(&home.path).is_ok());
}

/// Concurrent application requests share one fenced connection and produce one receipt/event each.
#[tokio::test]
async fn concurrent_application_load_preserves_every_receipt_once() {
    for concurrency in [1usize, 8, 32, 64] {
        let (home, queue, id) = setup();
        let barrier = std::sync::Arc::new(tokio::sync::Barrier::new(concurrency));
        let mut tasks = Vec::new();
        for index in 0..concurrency {
            let request = envelope_for(&id, &format!("producer-{index}"));
            let queue = queue.clone();
            let barrier = barrier.clone();
            tasks.push(tokio::spawn(async move {
                barrier.wait().await;
                let first = queue.submit(WriteAuthority::Broker, request.clone()).await;
                assert!(
                    matches!(first, SubmitResult::Committed(ref receipt) if !receipt.replayed),
                    "{first:?}"
                );
                let replay = queue.submit(WriteAuthority::Broker, request).await;
                assert!(
                    matches!(replay, SubmitResult::Committed(ref receipt) if receipt.replayed),
                    "{replay:?}"
                );
            }));
        }
        for task in tasks {
            task.await.unwrap();
        }
        assert_eq!(events(&home, &id), concurrency as i64);
        assert_eq!(
            home.store()
                .conn
                .query_row("SELECT count(*) FROM operation_receipts", [], |r| r
                    .get::<_, i64>(0))
                .unwrap(),
            concurrency as i64
        );
        assert!(!queue.degraded());
        queue.close().await.unwrap();
    }
}

/// A durable rejection is discoverable only by its exact original identity and payload digest.
#[tokio::test]
async fn rejected_status_requires_exact_digest() {
    let (home, queue, _) = setup();
    let ghost: AgentId = "ag-20261011-000000-00000000ee".parse().unwrap();
    let request = envelope(&ghost);
    let id = request.header().operation_id.clone();
    let digest = request.header().digest.clone();
    let SubmitResult::Rejected(rejection) = queue.submit(WriteAuthority::Broker, request).await
    else {
        panic!("missing target must be rejected");
    };
    assert!(rejection.durable);
    assert_eq!(rejection.code, RejectCode::NotFound);
    assert!(
        matches!(queue.status(WriteAuthority::Broker, id.clone(), digest).await, OpStatus::Rejected(stored) if stored == rejection)
    );
    assert!(matches!(
        queue
            .status(
                WriteAuthority::Broker,
                id,
                agent_run_domain::storage::PayloadDigest::of(&json!({"wrong": true}))
            )
            .await,
        OpStatus::NotFound { .. }
    ));
    assert_eq!(
        home.store()
            .conn
            .query_row("SELECT count(*) FROM operation_receipts", [], |r| r
                .get::<_, i64>(0))
            .unwrap(),
        1
    );
    queue.close().await.unwrap();
}
