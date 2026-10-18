//! Writer service lifecycle on disposable private homes: a second service on
//! the same home is refused by the lease, submissions reach the dedicated
//! thread and come back typed, and a drained service refuses new work as
//! not enqueued.
mod common;
use agent_run_core::writer::{WriterConfig, WriterService};
use agent_run_domain::storage::{
    MonoDeadline, NotEnqueuedReason, OperationId, ProducerId, ProducerKind, RejectCode,
    SubmitResult, WriteAuthority,
};
use agent_run_store::ops::{EnqueueCommand, RemoteOp, WriteEnvelope};

/// A cancel envelope for one execution.
fn envelope() -> WriteEnvelope {
    WriteEnvelope::new(
        OperationId::new("boot-test", 1),
        ProducerId {
            kind: ProducerKind::Cli,
            scope: None,
            instance: "cli-1".into(),
        },
        1,
        MonoDeadline {
            boot_id: "boot-test".into(),
            ns: u64::MAX / 2,
        },
        RemoteOp::from(EnqueueCommand {
            execution: "ag-20261011-000000-0000000001".parse().unwrap(),
            kind: "cancel".into(),
            payload: serde_json::json!({}),
        }),
    )
    .unwrap()
}

/// One service per home; a submission whose identity belongs to another boot
/// comes back from the writer thread as a non-durable `StaleOperation` refusal
/// (never a fabricated commit) and a shut-down service answers
/// `NotEnqueued(Draining)` through surviving handles.
#[tokio::test]
async fn single_service_typed_outcomes_and_drain() {
    let temp = tempfile::tempdir().unwrap();
    let home = temp.path().canonicalize().unwrap();
    agent_run_platform::fs::private_dir(&home).unwrap();
    agent_run_store::fixture::initialize(&home).unwrap();
    let service = WriterService::start(&home, WriterConfig::default()).unwrap();
    assert!(WriterService::start(&home, WriterConfig::default()).is_err());
    let handle = service.handle();
    let cli = WriteAuthority::Cli {
        uid: 1,
        pid: 2,
        birth: 3.0,
        token: "t".into(),
    };
    match handle.submit(cli.clone(), envelope()).await {
        SubmitResult::Rejected(refusal) => {
            assert_eq!(refusal.code, RejectCode::StaleOperation);
            assert!(!refusal.durable);
        }
        other => panic!("unexpected {other:?}"),
    }
    assert_eq!(handle.health().counters.rejected, 1);
    let (boot, now) = agent_run_platform::monotonic::now().unwrap();
    service
        .shutdown(MonoDeadline {
            boot_id: boot,
            ns: now + 1_000_000_000,
        })
        .await
        .unwrap();
    assert_eq!(
        handle.submit(cli, envelope()).await,
        SubmitResult::NotEnqueued(NotEnqueuedReason::Draining)
    );
    assert!(WriterService::start(&home, WriterConfig::default()).is_ok());
}

/// A real BUSY refusal closes new-agent readiness until same-connection recovery is proven.
#[tokio::test]
async fn busy_health_degrades_then_recovers_without_duplicate_event() {
    let home = common::Home::new();
    let (id, _) = home
        .store()
        .admit(&home.request(), &home.config, &serde_json::json!({}), None)
        .unwrap();
    let service = WriterService::start(
        &home.path,
        WriterConfig {
            writer_busy: std::time::Duration::from_millis(30),
            ..WriterConfig::default()
        },
    )
    .unwrap();
    let handle = service.handle();
    assert!(handle.health().admissions_open);
    let (boot, now) = agent_run_platform::monotonic::now().unwrap();
    let deadline = MonoDeadline {
        boot_id: boot.clone(),
        ns: now + 60_000_000_000,
    };
    let request = WriteEnvelope::new(
        OperationId::new(&boot, now),
        ProducerId {
            kind: ProducerKind::Broker,
            scope: Some("health-test".into()),
            instance: "one".into(),
        },
        1,
        deadline.clone(),
        RemoteOp::from(agent_run_store::ops::AppendJournal {
            execution: id.clone(),
            fragments: vec![agent_run_store::ops::JournalFragment::Event {
                kind: "health_test".into(),
                data: serde_json::json!({}),
                attempt: None,
            }],
        }),
    )
    .unwrap();
    let blocker = rusqlite::Connection::open(home.path.join("state.db")).unwrap();
    blocker.execute_batch("BEGIN IMMEDIATE").unwrap();
    let result = handle.submit(WriteAuthority::Broker, request.clone()).await;
    assert!(
        matches!(result, SubmitResult::Rejected(ref rejection) if rejection.code == RejectCode::StorageUnavailable && !rejection.durable)
    );
    assert!(!handle.health().admissions_open);
    assert_eq!(handle.health().counters.committed, 0);
    blocker.execute_batch("ROLLBACK").unwrap();
    assert!(matches!(
        handle.submit(WriteAuthority::Broker, request.clone()).await,
        SubmitResult::Committed(_)
    ));
    assert!(handle.health().admissions_open);
    assert!(matches!(
        handle.submit(WriteAuthority::Broker, request).await,
        SubmitResult::Committed(_)
    ));
    assert_eq!(
        home.store()
            .conn
            .query_row(
                "SELECT count(*) FROM events WHERE agent_id=? AND kind='health_test'",
                [id.as_str()],
                |r| r.get::<_, i64>(0)
            )
            .unwrap(),
        1
    );
    service.shutdown(deadline).await.unwrap();
    assert!(!handle.health().admissions_open);
}

/// One legacy admission envelope, whose operation identity and ordinal are fixed across retry.
fn admission(home: &common::Home, ordinal: u64) -> agent_run_store::ops::LocalEnvelope {
    let (boot, now) = agent_run_platform::monotonic::now().unwrap();
    let mut request = home.request();
    request.request_id = Some(format!("seal-{ordinal}"));
    agent_run_store::ops::LocalEnvelope::new(
        OperationId::new(&boot, now),
        ProducerId {
            kind: ProducerKind::Broker,
            scope: Some("seal".into()),
            instance: "one".into(),
        },
        ordinal,
        MonoDeadline {
            boot_id: boot,
            ns: now + 60_000_000_000,
        },
        agent_run_store::ops::LocalOp::from(agent_run_store::ops::AdmitStart {
            request,
            config: home.config.clone(),
            config_revision: "fixture".into(),
            identity: serde_json::json!({}),
            parent: None,
        }),
    )
    .unwrap()
}

/// An admission seal denies a fresh operation while preserving exact committed request status/replay.
#[tokio::test]
async fn sealed_admissions_resolve_old_receipts_but_refuse_new_starts() {
    let home = common::Home::new();
    let service = WriterService::start(&home.path, WriterConfig::default()).unwrap();
    let handle = service.handle();
    let first = admission(&home, 1);
    let SubmitResult::Committed(receipt) =
        handle.local(WriteAuthority::Broker, first.clone()).await
    else {
        panic!("admission did not commit");
    };
    handle.close_admissions("fixture seal");
    assert!(!handle.health().admissions_open);
    let replay = handle.local(WriteAuthority::Broker, first).await;
    assert!(
        matches!(replay, SubmitResult::Committed(ref stored) if stored.commit_seq == receipt.commit_seq)
    );
    assert_eq!(
        handle
            .local(WriteAuthority::Broker, admission(&home, 2))
            .await,
        SubmitResult::NotEnqueued(NotEnqueuedReason::AdmissionsClosed)
    );
    assert_eq!(
        home.store()
            .conn
            .query_row("SELECT count(*) FROM agents", [], |r| r.get::<_, i64>(0))
            .unwrap(),
        1
    );
    let (boot, now) = agent_run_platform::monotonic::now().unwrap();
    service
        .shutdown(MonoDeadline {
            boot_id: boot,
            ns: now + 1_000_000_000,
        })
        .await
        .unwrap();
}

/// Expiring shutdown reports unconfirmed closure and holds its fence until the blocked accepted job actually drains.
#[cfg(feature = "test-fixtures")]
#[tokio::test]
async fn shutdown_timeout_retains_fence_until_owned_drain_finishes() {
    let home = common::Home::new();
    let service = WriterService::start(&home.path, WriterConfig::default()).unwrap();
    let handle = service.handle();
    let (entered_tx, entered_rx) = tokio::sync::oneshot::channel();
    let (release_tx, release_rx) = std::sync::mpsc::channel();
    let blocked = handle.clone();
    let job = tokio::spawn(async move { blocked.fixture_barrier(entered_tx, release_rx).await });
    entered_rx.await.unwrap();
    let (boot, now) = agent_run_platform::monotonic::now().unwrap();
    assert!(
        service
            .shutdown(MonoDeadline {
                boot_id: boot,
                ns: now
            })
            .await
            .is_err()
    );
    assert!(!handle.health().admissions_open);
    assert!(agent_run_store::writer::WriterLease::acquire_online(&home.path).is_err());
    release_tx.send(()).unwrap();
    job.await.unwrap().unwrap();
    let replacement = tokio::time::timeout(std::time::Duration::from_secs(5), async {
        loop {
            if let Ok(service) = WriterService::start(&home.path, WriterConfig::default()) {
                break service;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("actual drain releases its sole writer lease");
    let (boot, now) = agent_run_platform::monotonic::now().unwrap();
    replacement
        .shutdown(MonoDeadline {
            boot_id: boot,
            ns: now + 1_000_000_000,
        })
        .await
        .unwrap();
}

/// Queue/operation diagnostics remain observable while actual SQL is blocked.
/// Readiness is established by the writer barrier and accepted-job credits,
/// not a guessed sleep. Payload content never appears in the public ping.
#[cfg(feature = "test-fixtures")]
#[tokio::test]
async fn writer_operation_timing_is_safe_and_nonblocking() {
    let home = common::Home::new();
    let (id, _) = home
        .store()
        .admit(&home.request(), &home.config, &serde_json::json!({}), None)
        .unwrap();
    let writer = WriterService::start(&home.path, WriterConfig::default()).unwrap();
    let handle = writer.handle();
    let (entered_tx, entered_rx) = tokio::sync::oneshot::channel();
    let (release_tx, release_rx) = std::sync::mpsc::channel();
    let blocked = handle.clone();
    let barrier =
        tokio::spawn(async move { blocked.fixture_barrier(entered_tx, release_rx).await });
    entered_rx.await.unwrap();
    let (boot, now) = agent_run_platform::monotonic::now().unwrap();
    let identity = OperationId::new(&boot, now);
    let request = WriteEnvelope::new(
        identity.clone(),
        ProducerId {
            kind: ProducerKind::Broker,
            scope: Some("timing".into()),
            instance: "fixture".into(),
        },
        1,
        MonoDeadline {
            boot_id: boot,
            ns: now + 60_000_000_000,
        },
        RemoteOp::from(agent_run_store::ops::AppendJournal {
            execution: id,
            fragments: vec![agent_run_store::ops::JournalFragment::Event {
                kind: "timing_fixture".into(),
                data: serde_json::json!({"private_payload":"DO_NOT_EXPOSE"}),
                attempt: None,
            }],
        }),
    )
    .unwrap();
    let submitted = handle.clone();
    let work = tokio::spawn(async move { submitted.submit(WriteAuthority::Broker, request).await });
    tokio::time::timeout(std::time::Duration::from_secs(3), async {
        while handle.health().accepted_jobs != 2 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    let service = agent_run_core::service::Service::new(home.path.clone()).with_writer(
        agent_run_core::broker_writer::BrokerWriter::new(handle.clone()),
    );
    let ping = tokio::time::timeout(
        std::time::Duration::from_secs(1),
        agent_run_core::dispatch::call(&service, "ping", serde_json::json!({"writer_health":true})),
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(ping["writer"]["accepted_jobs"], 2);
    assert!(!ping.to_string().contains("DO_NOT_EXPOSE"));
    release_tx.send(()).unwrap();
    barrier.await.unwrap().unwrap();
    assert!(matches!(work.await.unwrap(), SubmitResult::Committed(_)));
    let observed = handle.health();
    assert!(observed.current_operation.is_none());
    let last = observed.last_operation.unwrap();
    assert_eq!(last.operation_id, identity);
    assert_eq!(last.op_kind, "append_journal");
    let (boot, now) = agent_run_platform::monotonic::now().unwrap();
    writer
        .shutdown(MonoDeadline {
            boot_id: boot,
            ns: now + 3_000_000_000,
        })
        .await
        .unwrap();
}
