//! Asynchronous ownership persistence: a pending durable write gates native
//! progress but never stops observation of the primary harness and its
//! descendants, and cancellation never restarts or renews the write.

use agent_run_adapters::{
    LaunchPlan,
    io::{Event, OwnershipSink, PersistFuture, Process},
};
use agent_run_domain::{Error, OwnershipStage};
use agent_run_platform::process::{self, ProcessState};
use std::{
    collections::BTreeMap,
    path::PathBuf,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::{Duration, Instant},
};
use tokio::sync::Notify;

/// Cleans every captured member even when an assertion unwinds.
struct Fixture(Process);

impl Drop for Fixture {
    /// Terminates the fixture's verified members; each also has an eight-second TTL.
    fn drop(&mut self) {
        let _ = self.0.owner.cleanup_blocking(Duration::from_millis(100));
    }
}

/// Starts a shell leader with a TERM-ignoring background descendant, reports the
/// descendant PID, then exits after reading one request line.
async fn spawn() -> (Fixture, process::Identity) {
    let script = "trap '' TERM; sleep 8 & printf '{\"pid\":%s}\\n' \"$!\"; read request; exit 0";
    let plan = LaunchPlan {
        binary: PathBuf::from("/bin/sh"),
        args: vec!["-c".into(), script.into()],
        cwd: std::env::current_dir().unwrap(),
        environment: BTreeMap::new(),
        initial_input: None,
    };
    let mut fixture = Fixture(Process::spawn(&plan).unwrap());
    let Event::Json(event) = tokio::time::timeout(Duration::from_secs(2), fixture.0.next())
        .await
        .unwrap()
    else {
        panic!("fixture child identity missing");
    };
    let child = process::inspect(event["pid"].as_i64().unwrap() as i32).unwrap();
    fixture.0.owner.refresh();
    (fixture, child)
}

/// A sink that counts starts and resolves each write only after `release`.
fn gated_sink(starts: Arc<AtomicUsize>, release: Arc<Notify>) -> OwnershipSink {
    Box::new(move |_| -> PersistFuture {
        starts.fetch_add(1, Ordering::SeqCst);
        let release = release.clone();
        Box::pin(async move {
            release.notified().await;
            Ok(())
        })
    })
}

/// While a durable write is stalled, `observing` still escalates a TERM-ignoring
/// descendant after the primary exits, on the unchanged two-second schedule.
#[tokio::test]
async fn observation_and_escalation_continue_while_storage_waits() {
    let (mut fixture, child) = spawn().await;
    fixture
        .0
        .observe_ownership(Box::new(|_| -> PersistFuture {
            Box::pin(async { Ok(()) })
        }))
        .await
        .unwrap();
    fixture.0.text("go\n").await.unwrap();
    let started = Instant::now();
    fixture
        .0
        .observing(tokio::time::sleep(Duration::from_millis(3500)))
        .await;
    assert!(started.elapsed() >= Duration::from_millis(3400));
    assert_eq!(
        process::observe(Some(child.pid), Some(&child.token), Some(child.birth)),
        ProcessState::Dead,
        "the survivor must be killed during the storage wait"
    );
}

/// The initial write gates its caller until durable; a cancelled wait keeps the
/// same started write instead of starting a second one.
#[tokio::test]
async fn pending_write_survives_cancellation_and_is_started_once() {
    let (mut fixture, _child) = spawn().await;
    let starts = Arc::new(AtomicUsize::new(0));
    let release = Arc::new(Notify::new());
    fixture
        .0
        .install_ownership_sink(gated_sink(starts.clone(), release.clone()));
    for _ in 0..3 {
        let waited = tokio::time::timeout(
            Duration::from_millis(120),
            fixture
                .0
                .checkpoint_ownership_ready(OwnershipStage::Initial),
        )
        .await;
        assert!(
            waited.is_err(),
            "progress must stay gated while the write is pending"
        );
    }
    assert_eq!(
        starts.load(Ordering::SeqCst),
        1,
        "cancellation must not restart the write"
    );
    release.notify_one();
    fixture
        .0
        .checkpoint_ownership_ready(OwnershipStage::Initial)
        .await
        .unwrap();
    assert_eq!(starts.load(Ordering::SeqCst), 1);
    // Unchanged captures do not touch storage again.
    fixture
        .0
        .checkpoint_ownership_ready(OwnershipStage::Streaming)
        .await
        .unwrap();
    assert_eq!(starts.load(Ordering::SeqCst), 1);
}

/// A refused write returns its typed first cause and a later checkpoint starts a fresh write.
#[tokio::test]
async fn refused_write_is_typed_and_a_later_checkpoint_retries() {
    let (mut fixture, _child) = spawn().await;
    let starts = Arc::new(AtomicUsize::new(0));
    let counter = starts.clone();
    fixture
        .0
        .install_ownership_sink(Box::new(move |_| -> PersistFuture {
            let attempt = counter.fetch_add(1, Ordering::SeqCst);
            Box::pin(async move {
                if attempt == 0 {
                    Err(Error::Io(std::io::Error::new(
                        std::io::ErrorKind::PermissionDenied,
                        "SOURCE_SECRET",
                    )))
                } else {
                    Ok(())
                }
            })
        }));
    let error = fixture
        .0
        .checkpoint_ownership_ready(OwnershipStage::Final)
        .await
        .unwrap_err();
    assert!(matches!(
        error,
        Error::OwnershipCheckpoint {
            stage: OwnershipStage::Final,
            ..
        }
    ));
    assert!(!error.public().message.contains("SOURCE_SECRET"));
    fixture
        .0
        .checkpoint_ownership_ready(OwnershipStage::Final)
        .await
        .unwrap();
    assert_eq!(starts.load(Ordering::SeqCst), 2);
}

/// A failure stored while streaming (found by `observing`, not yet reported)
/// does not condemn the final checkpoint: the newest capture is persisted again
/// and judged by that attempt alone. A sink that keeps refusing still fails it.
#[tokio::test]
async fn final_checkpoint_persists_again_after_a_stored_streaming_failure() {
    let (mut fixture, _child) = spawn().await;
    let starts = Arc::new(AtomicUsize::new(0));
    let counter = starts.clone();
    fixture
        .0
        .install_ownership_sink(Box::new(move |_| -> PersistFuture {
            let attempt = counter.fetch_add(1, Ordering::SeqCst);
            Box::pin(async move {
                if attempt == 0 {
                    Err(Error::Io(std::io::Error::other("refused while streaming")))
                } else {
                    Ok(())
                }
            })
        }));
    // The refusal is stored while the engine is observed, not reported.
    fixture
        .0
        .observing(tokio::time::sleep(Duration::from_millis(200)))
        .await;
    assert!(starts.load(Ordering::SeqCst) >= 1);
    fixture
        .0
        .checkpoint_ownership_ready(OwnershipStage::Final)
        .await
        .expect("the newest capture is durable, the old failure is moot");

    // A sink that always refuses cannot pass the final checkpoint.
    let (mut refusing, _child) = spawn().await;
    refusing
        .0
        .install_ownership_sink(Box::new(|_| -> PersistFuture {
            Box::pin(async { Err(Error::Io(std::io::Error::other("always refused"))) })
        }));
    refusing
        .0
        .observing(tokio::time::sleep(Duration::from_millis(200)))
        .await;
    assert!(matches!(
        refusing
            .0
            .checkpoint_ownership_ready(OwnershipStage::Final)
            .await
            .unwrap_err(),
        Error::OwnershipCheckpoint {
            stage: OwnershipStage::Final,
            ..
        }
    ));
}

/// A write that was already pending (and later refuses) when the final
/// checkpoint begins is awaited but cannot fail it: the final checkpoint
/// persists the newest capture again and is judged by that write alone.
#[tokio::test]
async fn final_checkpoint_is_not_failed_by_a_write_started_before_it() {
    let (mut fixture, _child) = spawn().await;
    let starts = Arc::new(AtomicUsize::new(0));
    let counter = starts.clone();
    fixture
        .0
        .install_ownership_sink(Box::new(move |_| -> PersistFuture {
            let attempt = counter.fetch_add(1, Ordering::SeqCst);
            Box::pin(async move {
                if attempt == 0 {
                    tokio::time::sleep(Duration::from_millis(300)).await;
                    Err(Error::Io(std::io::Error::other("refused late")))
                } else {
                    Ok(())
                }
            })
        }));
    // Starts the first write (still pending after 100 ms) and returns.
    fixture
        .0
        .observing(tokio::time::sleep(Duration::from_millis(100)))
        .await;
    assert_eq!(starts.load(Ordering::SeqCst), 1);
    fixture
        .0
        .checkpoint_ownership_ready(OwnershipStage::Final)
        .await
        .expect("only a write started by the final checkpoint can fail it");
    assert_eq!(starts.load(Ordering::SeqCst), 2, "persisted again once");
}

/// A failed first batch never advances the cursor; final retry persists a >2-MiB history in deltas.
#[tokio::test]
async fn large_capture_retries_only_unacknowledged_bounded_batches() {
    use std::collections::BTreeSet;
    use std::sync::Mutex;
    let (mut fixture, _) = spawn().await;
    let mut snapshot = fixture.0.owner.snapshot().unwrap();
    snapshot
        .members
        .extend((100_000..120_000).map(|pid| process::Identity {
            pid,
            ppid: 0,
            group: 0,
            birth: 1.0,
            token: format!("inert-{pid}-{}", "x".repeat(80)),
            zombie: false,
            program: Some("fixture".into()),
        }));
    assert!(serde_json::to_vec(&snapshot).unwrap().len() > 2 * 1024 * 1024);
    fixture.0.owner = process::OwnedProcess::restore(snapshot).unwrap();
    let seen = Arc::new(Mutex::new(BTreeSet::new()));
    let calls = Arc::new(AtomicUsize::new(0));
    let (seen_sink, calls_sink) = (seen.clone(), calls.clone());
    fixture
        .0
        .install_ownership_sink(Box::new(move |batch| -> PersistFuture {
            assert!(batch.members.len() <= 513);
            if calls_sink.fetch_add(1, Ordering::SeqCst) == 0 {
                return Box::pin(async { Err(Error::Validation("injected batch refusal".into())) });
            }
            let seen = seen_sink.clone();
            Box::pin(async move {
                for p in batch.members {
                    seen.lock().unwrap().insert((p.pid, p.token));
                }
                Ok(())
            })
        }));
    assert!(
        fixture
            .0
            .checkpoint_ownership_ready(OwnershipStage::Streaming)
            .await
            .is_err()
    );
    fixture
        .0
        .checkpoint_ownership_ready(OwnershipStage::Final)
        .await
        .unwrap();
    let seen = seen.lock().unwrap();
    for pid in 100_000..120_000 {
        assert!(seen.contains(&(pid, format!("inert-{pid}-{}", "x".repeat(80)))));
    }
    assert!(calls.load(Ordering::SeqCst) > 2);
}
