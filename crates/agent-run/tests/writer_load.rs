//! Real OS producers exercise one private writer socket without model calls.
//! Account writes test authenticated transport, durable receipts, retries and
//! neighbour progress; native streaming is covered by provider_initial tests.

#[path = "support/test_broker.rs"]
mod test_broker;

use agent_run_core::{
    persist,
    storage_wire::{self as wire, Message},
};
use agent_run_domain::{
    catalog::{AccountRecord, AccountStatus},
    storage::{MonoDeadline, OperationId, ProducerId, ProducerKind, SubmitResult},
};
use agent_run_store::{ops, writer::StoreReader};
use std::{
    path::{Path, PathBuf},
    time::{Duration, Instant},
};
use tokio::net::UnixStream;

/// Explicit fixture root from WRITER_LOAD_SCRATCH or required TMPDIR. The caller
/// creates this directory before running tests; there is no system-temp fallback.
/// CI supplies its own project scratch root, while the owner runner supplies the
/// protected project-specific temporary subtree. Invalid/missing roots fail.
fn scratch() -> PathBuf {
    let path = std::env::var_os("WRITER_LOAD_SCRATCH")
        .or_else(|| std::env::var_os("TMPDIR"))
        .expect("set WRITER_LOAD_SCRATCH or TMPDIR to an existing project fixture directory");
    PathBuf::from(path)
        .canonicalize()
        .expect("fixture root exists")
}

/// Introduce a real process with CLI authority; kernel credentials are checked
/// by the server. A refused or missing hello causes the test to fail.
async fn connect(home: &Path) -> UnixStream {
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            if let Some(stream) = try_connect(home).await {
                return stream;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("real peer eventually obtains connection capacity")
}

/// One hello attempt returns None on capacity refusal without changing the
/// kernel-attested authority; the caller retries within a finite bound.
async fn try_connect(home: &Path) -> Option<UnixStream> {
    let mut stream = UnixStream::connect(home.join("writer.sock")).await.ok()?;
    wire::write_frame(
        &mut stream,
        Message::Hello,
        0,
        1,
        &serde_json::to_vec(&persist::cli_hello()).unwrap(),
    )
    .await
    .ok()?;
    let head = wire::read_header(&mut stream, |kind, _| {
        (kind == Message::HelloOk).then_some(wire::HELLO_MAX)
    })
    .await
    .ok()??;
    wire::read_body(&mut stream, &head).await.ok()?;
    Some(stream)
}

/// Retry only explicit queue/byte capacity or retention-gate no-enqueue outcomes
/// within the original 20s bound, using the same envelope, deadline and sequence.
/// Rejections, unknown outcomes and terminal admission refusals remain visible;
/// no SQL mutation or model turn is blindly repeated. Production lane clients
/// already recognize RetentionGate; this raw wire fixture must do so as well.
async fn submit(
    stream: &mut UnixStream,
    envelope: &ops::WriteEnvelope,
    sequence: u64,
) -> SubmitResult {
    tokio::time::timeout(Duration::from_secs(20), async {
        loop {
            let result = exchange(stream, envelope, sequence).await;
            if matches!(
                result,
                SubmitResult::NotEnqueued(
                    agent_run_domain::storage::NotEnqueuedReason::QueueFull
                        | agent_run_domain::storage::NotEnqueuedReason::BytesFull
                        | agent_run_domain::storage::NotEnqueuedReason::RetentionGate
                )
            ) {
                tokio::time::sleep(Duration::from_millis(5)).await;
            } else {
                return result;
            }
        }
    })
    .await
    .expect("capacity retry has a finite bound")
}

/// Send the exact envelope and validate response correlation; returns the
/// typed durable outcome. The caller imposes an outer deadline.
async fn exchange(
    stream: &mut UnixStream,
    envelope: &ops::WriteEnvelope,
    sequence: u64,
) -> SubmitResult {
    let (kind, body) = envelope.to_wire();
    wire::write_frame(stream, Message::Request, kind, sequence, body)
        .await
        .unwrap();
    let head = wire::read_header(stream, |kind, _| {
        (kind == Message::Reply).then_some(wire::REPLY_MAX)
    })
    .await
    .unwrap()
    .expect("correlated reply");
    assert_eq!(head.seq, sequence);
    serde_json::from_slice(&wire::read_body(stream, &head).await.unwrap()).unwrap()
}

/// A unique fixture account; secret_ref is a name only and no secret is read.
fn account(name: String) -> ops::RegisterAccount {
    let secret_ref = format!(
        "env:LOAD_FIXTURE_{}",
        name.replace('-', "_").to_ascii_uppercase()
    );
    ops::RegisterAccount {
        record: AccountRecord {
            account_id: name.parse().unwrap(),
            auth_family: "anthropic".parse().unwrap(),
            secret_ref: secret_ref.parse().unwrap(),
            status: AccountStatus::Enabled,
        },
    }
}

/// Build a stable operation identity for a CLI process; retries must retain all
/// envelope fields. Ordinals increase within this producer only.
fn envelope(worker: usize, ordinal: u64, seconds: u64) -> ops::WriteEnvelope {
    let (boot_id, now) = agent_run_platform::monotonic::now().unwrap();
    ops::WriteEnvelope::new(
        OperationId::new(&boot_id, now),
        ProducerId {
            kind: ProducerKind::Cli,
            scope: None,
            instance: format!("load-{}-{worker}", std::process::id()),
        },
        ordinal,
        MonoDeadline {
            boot_id,
            ns: now + (seconds + 30) * 1_000_000_000,
        },
        account(format!("load-{worker}-{ordinal}")).into(),
    )
    .unwrap()
}

/// A real framed peer refuses queue/bytes/retention admission, then returns one
/// terminal outcome. Every retry must preserve all request bytes and correlation;
/// committed/rejected/unknown/closed outcomes must return without another send.
#[tokio::test]
async fn retention_gate_retry_keeps_identical_requests_and_terminal_outcomes() {
    use agent_run_domain::storage::{
        NotEnqueuedReason, RejectCode, RejectStage, StorageRejection, UnknownCause, WriteReceipt,
    };
    let request = envelope(99, 1, 0);
    let header = request.header();
    let outcomes = [
        SubmitResult::Committed(WriteReceipt {
            operation_id: header.operation_id.clone(),
            digest: header.digest.clone(),
            op_kind: request.kind().name().into(),
            producer: header.producer.clone(),
            ordinal: header.ordinal,
            scope: None,
            writer_epoch: 1,
            commit_seq: 1,
            committed_at: 0.0,
            replayed: false,
            result: serde_json::Value::Null,
        }),
        SubmitResult::Rejected(StorageRejection::new(
            RejectCode::StorageRolledBack,
            RejectStage::Statement,
            "fixture refusal",
        )),
        SubmitResult::Unknown(UnknownCause::CommitUncertain),
        SubmitResult::NotEnqueued(NotEnqueuedReason::AdmissionsClosed),
    ];
    for terminal in outcomes {
        let (mut stream, mut peer) = UnixStream::pair().unwrap();
        let (kind, body) = request.to_wire();
        let body = body.to_vec();
        let sequence = 73;
        let replies = [
            SubmitResult::NotEnqueued(NotEnqueuedReason::QueueFull),
            SubmitResult::NotEnqueued(NotEnqueuedReason::BytesFull),
            SubmitResult::NotEnqueued(NotEnqueuedReason::RetentionGate),
            terminal.clone(),
        ];
        let server = tokio::spawn(async move {
            let mut received = 0;
            for reply in replies {
                let Some(head) = wire::read_header(&mut peer, |message, _| {
                    (message == Message::Request).then_some(wire::BODY_HARD_MAX)
                })
                .await
                .unwrap() else {
                    return received;
                };
                assert_eq!(head.message, Message::Request);
                assert_eq!(head.op_kind, kind);
                assert_eq!(head.seq, sequence);
                assert_eq!(
                    wire::read_body(&mut peer, &head).await.unwrap(),
                    body,
                    "retry must preserve identity, ordinal, digest, deadline and payload bytes"
                );
                received += 1;
                wire::write_frame(
                    &mut peer,
                    Message::Reply,
                    0,
                    sequence,
                    &serde_json::to_vec(&reply).unwrap(),
                )
                .await
                .unwrap();
            }
            assert!(
                wire::read_header(&mut peer, |_, _| Some(wire::BODY_HARD_MAX))
                    .await
                    .unwrap()
                    .is_none(),
                "terminal/unknown outcome must not trigger another request"
            );
            received
        });
        let result = submit(&mut stream, &request, sequence).await;
        drop(stream);
        let received = server.await;
        assert_eq!(
            result, terminal,
            "RetentionGate must retry, and the exact terminal result must remain visible"
        );
        assert_eq!(
            received.unwrap(),
            4,
            "only the three explicit no-enqueue causes permit retry"
        );
    }
}

/// Child entry point in this same test executable. With no explicit fixture
/// home it is a no-op; only the parent load tests enable it. Never starts an
/// engine. Each worker commits eight accounts per explicit round (one to eight)
/// and retries every fixed envelope, including one deliberately unread replay
/// reply, then optionally sustains replays. All workers share a ready/go barrier.
#[tokio::test]
async fn load_producer_child() {
    let Some(home) = std::env::var_os("WRITER_LOAD_HOME") else {
        return;
    };
    let home = PathBuf::from(home);
    assert!(home.starts_with(scratch()));
    assert!(!std::env::vars_os().any(|(key, _)| key.to_string_lossy().starts_with("AGENT_RUN_")));
    let worker: usize = std::env::var("WRITER_LOAD_INDEX").unwrap().parse().unwrap();
    let seconds: u64 = std::env::var("WRITER_LOAD_SECONDS")
        .unwrap()
        .parse()
        .unwrap();
    let rounds: u64 = std::env::var("WRITER_LOAD_ROUNDS")
        .unwrap()
        .parse()
        .unwrap();
    assert!((1..=8).contains(&rounds));
    std::fs::write(home.join(format!("ready-{worker}")), b"ready").unwrap();
    tokio::time::timeout(Duration::from_secs(15), async {
        while !home.join("go").exists() {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("parent releases every already-running process together");
    let started = Instant::now();
    let mut latencies = Vec::new();
    let mut stream = connect(&home).await;
    let mut envelopes = Vec::new();
    for ordinal in 1..=8 * rounds {
        let envelope = envelope(worker, ordinal, seconds);
        let at = Instant::now();
        let first = submit(&mut stream, &envelope, ordinal + 2).await;
        let SubmitResult::Committed(first) = first else {
            panic!("worker {worker} first outcome: {first:?}");
        };
        latencies.push(at.elapsed().as_micros() as u64);
        if ordinal == 1 {
            let (kind, body) = envelope.to_wire();
            wire::write_frame(&mut stream, Message::Request, kind, 2, body)
                .await
                .unwrap();
            drop(stream); // Lose a replay ACK for an already proven durable operation.
            stream = connect(&home).await;
        }
        let SubmitResult::Committed(replay) = submit(&mut stream, &envelope, ordinal + 100).await
        else {
            panic!("same operation must replay");
        };
        assert!(replay.replayed);
        assert_eq!(first.commit_seq, replay.commit_seq);
        assert_eq!(first.digest, replay.digest);
        envelopes.push(envelope);
    }
    let mut sequence = 200;
    while started.elapsed() < Duration::from_secs(seconds) {
        for envelope in &envelopes {
            sequence += 1;
            let at = Instant::now();
            let result = submit(&mut stream, envelope, sequence).await;
            assert!(
                matches!(result, SubmitResult::Committed(ref receipt) if receipt.replayed),
                "{result:?}"
            );
            latencies.push(at.elapsed().as_micros() as u64);
        }
    }
    std::fs::write(
        home.join(format!("metrics-{worker}.json")),
        serde_json::to_vec(&latencies).unwrap(),
    )
    .unwrap();
}

/// Spawn one independent OS producer with all live-agent and credential
/// environment removed from the child only. Output remains available on failure.
fn child(home: &Path, worker: usize, seconds: u64, rounds: usize) -> tokio::process::Child {
    let mut command = tokio::process::Command::new(std::env::current_exe().unwrap());
    for (key, _) in std::env::vars_os() {
        let name = key.to_string_lossy();
        if name.starts_with("AGENT_RUN_")
            || ["TOKEN", "SECRET", "PASSWORD", "API_KEY"]
                .iter()
                .any(|suffix| name.ends_with(suffix))
            || ["SSH_AUTH_SOCK", "GIT_SSH_COMMAND"].contains(&name.as_ref())
        {
            command.env_remove(key);
        }
    }
    command
        .args([
            "--exact",
            "load_producer_child",
            "--nocapture",
            "--test-threads=1",
        ])
        .env("WRITER_LOAD_HOME", home)
        .env("WRITER_LOAD_INDEX", worker.to_string())
        .env("WRITER_LOAD_SECONDS", seconds.to_string())
        .env("WRITER_LOAD_ROUNDS", rounds.to_string())
        .env("WRITER_LOAD_SCRATCH", scratch())
        .env("TMPDIR", scratch())
        .env("TMP", scratch())
        .env("TEMP", scratch())
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .kill_on_drop(true)
        .spawn()
        .unwrap()
}

/// Execute one concurrent width over a new schema-30 fixture and assert exact
/// accounts and receipts, no duplicate effects, integrity, every child success,
/// and lease release after broker shutdown. Latency excludes child startup.
async fn wave(width: usize, seconds: u64) {
    wave_rounds(width, seconds, 1).await;
}

/// The existing process wave with one to eight rounds of eight unique writes
/// per producer. Includes exact row/receipt checks and concurrent read-progress
/// observation for the growth variant; ordinary and 30-minute modes stay one
/// round. Caller bounds producer width and sustained seconds.
async fn wave_rounds(width: usize, seconds: u64, rounds: usize) {
    assert!((1..=8).contains(&rounds));
    let temp = tempfile::Builder::new()
        .prefix("wl-")
        .tempdir_in(scratch())
        .unwrap();
    let home = temp.path().canonicalize().unwrap();
    agent_run_platform::fs::private_dir(&home).unwrap();
    drop(agent_run_store::fixture::initialize(&home).unwrap());
    let broker = test_broker::TestBroker::start(&home).await;
    let mut children = Vec::new();
    for worker in 0..width {
        children.push(child(&home, worker, seconds, rounds));
    }
    tokio::time::timeout(Duration::from_secs(15), async {
        while !(0..width).all(|worker| home.join(format!("ready-{worker}")).exists()) {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("all independent producers reached the common barrier");
    let (reader_stop, mut reader_stopped) = tokio::sync::oneshot::channel::<()>();
    let reader_home = home.clone();
    let observer = tokio::spawn(async move {
        let mut reads = 0;
        let mut partial = 0;
        let mut last = 0;
        let reader = StoreReader::open(&reader_home).unwrap();
        loop {
            let count = reader.list_accounts().unwrap().len();
            assert!(
                count >= last,
                "read neighbour sees monotonically committed growth"
            );
            last = count;
            reads += 1;
            if count > 0 && count < width * 8 * rounds {
                partial += 1;
            }
            tokio::select! {
                _ = &mut reader_stopped => return (reads, partial),
                _ = tokio::time::sleep(Duration::from_millis(10)) => {}
            }
        }
    });
    std::fs::write(home.join("go"), b"go").unwrap();
    for child in children {
        let output =
            tokio::time::timeout(Duration::from_secs(seconds + 60), child.wait_with_output())
                .await
                .expect("load child cannot hang")
                .unwrap();
        assert!(
            output.status.success(),
            "child failed {}\n{}\n{}",
            output.status,
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
    }
    let _ = reader_stop.send(());
    let (reader_observations, partial_observations) = observer.await.unwrap();
    if rounds > 1 {
        assert!(reader_observations > 1);
        assert!(
            partial_observations > 0,
            "read neighbour progressed while producers were writing"
        );
    }
    let reader = StoreReader::open(&home).unwrap();
    let accounts = reader.list_accounts().unwrap();
    assert_eq!(
        accounts.len(),
        width * 8 * rounds,
        "every producer committed without duplicate account effects"
    );
    let receipts: usize = reader
        .conn()
        .query_row("SELECT COUNT(*) FROM operation_receipts", [], |row| {
            row.get(0)
        })
        .unwrap();
    assert_eq!(
        receipts,
        width * 8 * rounds,
        "reconnect/replay cannot add receipt rows"
    );
    let integrity: String = reader
        .conn()
        .query_row("PRAGMA quick_check", [], |row| row.get(0))
        .unwrap();
    assert_eq!(integrity, "ok");
    drop(reader);
    let mut samples: Vec<u64> = Vec::new();
    for worker in 0..width {
        samples.extend(
            serde_json::from_slice::<Vec<u64>>(
                &std::fs::read(home.join(format!("metrics-{worker}.json"))).unwrap(),
            )
            .unwrap(),
        );
    }
    samples.sort_unstable();
    let percentile = |p: usize| samples[(samples.len() - 1) * p / 100];
    let metrics = serde_json::json!({"processes": width, "seconds": seconds, "rounds": rounds, "samples": samples.len(),
        "reader_observations":reader_observations,"partial_growth_observations":partial_observations,
        "latency_us": {"p50": percentile(50), "p95": percentile(95), "p99": percentile(99), "max": samples.last()},
        "accounts": accounts.len(), "receipts": receipts, "rss": "unavailable; not measured by this test"});
    println!("LOAD_METRICS={metrics}");
    std::fs::write(
        scratch().join(format!("load-{width}-{seconds}-rounds-{rounds}.json")),
        serde_json::to_vec_pretty(&metrics).unwrap(),
    )
    .unwrap();
    broker.stop().await;
    let service = agent_run_core::writer::WriterService::start(&home, Default::default())
        .expect("no leaked lease");
    let (boot_id, now) = agent_run_platform::monotonic::now().unwrap();
    service
        .shutdown(MonoDeadline {
            boot_id,
            ns: now + 5_000_000_000,
        })
        .await
        .unwrap();
}

/// Independent OS producers increase from one to sixty-four, with exact durable
/// effects and replay evidence per wave; finite regression, no model calls.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn increasing_real_process_load_is_exact_and_replay_safe() {
    for width in [1, 8, 32, 64] {
        wave(width, 0).await;
    }
}

/// Sixty-four simultaneously running OS producers each commit and replay
/// sixty-four unique writes: 4096 new effects and exactly 4096 receipts while
/// a read-only neighbour observes intermediate committed growth. Finite burst
/// qualification only, not a five-minute sustained-write benchmark.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn bounded_real_process_growth_is_exact_and_readers_progress() {
    wave_rounds(64, 0, 8).await;
}

/// Explicit opt-in sustained replay load defaults to thirty minutes. Accounts
/// and receipts remain fixed; this measures transport/receipt-read stability,
/// not sustained growth, transcript throughput or production qualification.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "explicit finite load run; default 1800 seconds"]
async fn sustained_real_process_replay_load() {
    let seconds = std::env::var("WRITER_LOAD_DURATION_SECONDS")
        .map(|value| value.parse().unwrap())
        .unwrap_or(1800);
    assert!((1..=1800).contains(&seconds));
    wave(64, seconds).await;
}

/// Own a foreign fixture lock thread and release it even when a parent assertion unwinds.
struct HeldWriteLock {
    /// Explicit release follows the long-refusal and no-mutation assertions.
    release: Option<std::sync::mpsc::Sender<()>>,
    /// Joining prevents a failed fixture from leaving a thread or database lock behind.
    thread: Option<std::thread::JoinHandle<()>>,
}

impl HeldWriteLock {
    /// Release and join normally, propagating any SQL/thread failure as a failed test.
    fn release_and_join(mut self) {
        if let Some(release) = self.release.take() {
            let _ = release.send(());
        }
        self.thread.take().unwrap().join().unwrap();
    }
}

impl Drop for HeldWriteLock {
    /// Unwind releases the foreign transaction and reaps its thread without a second panic.
    fn drop(&mut self) {
        if let Some(release) = self.release.take() {
            let _ = release.send(());
        }
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
    }
}

/// Parent unwinding before receiving readiness must release and reap the SQL lock thread.
#[test]
fn held_write_lock_unwind_before_readiness_releases_and_joins() {
    let temp = tempfile::Builder::new()
        .prefix("ar-load-unwind-")
        .tempdir_in(scratch())
        .unwrap();
    let path = temp.path().join("fixture.db");
    rusqlite::Connection::open(&path).unwrap();
    let (ready, readiness) = std::sync::mpsc::sync_channel(1);
    let (release, released) = std::sync::mpsc::channel();
    let (done, completed) = std::sync::mpsc::sync_channel(1);
    let child_path = path.clone();
    let thread = std::thread::spawn(move || {
        let connection = rusqlite::Connection::open(child_path).unwrap();
        connection.execute_batch("BEGIN IMMEDIATE").unwrap();
        ready.send(()).unwrap();
        let result = released.recv_timeout(Duration::from_secs(2));
        connection.execute_batch("ROLLBACK").unwrap();
        result.unwrap();
        done.send(()).unwrap();
    });
    // The moved handle is consumed only by the guard's Drop; no shared state is reused after panic.
    let unwind = std::panic::catch_unwind(std::panic::AssertUnwindSafe(move || {
        let _lock = HeldWriteLock {
            release: Some(release),
            thread: Some(thread),
        };
        panic!("synthetic parent assertion before readiness receive");
    }));
    assert!(unwind.is_err());
    readiness.recv_timeout(Duration::from_secs(2)).unwrap();
    completed.recv_timeout(Duration::from_secs(2)).unwrap();
    let connection = rusqlite::Connection::open(path).unwrap();
    connection
        .execute_batch("BEGIN IMMEDIATE; ROLLBACK")
        .unwrap();
}

/// An independent writer lock is acquired before submitting. The short lock
/// commits before the 250ms allowance; a long lock yields no false receipt or
/// mutation and closed diagnostics while an explicit release channel keeps it
/// occupied through the assertions. After release the same identity commits
/// once, its repeat replays, and a fresh neighbour also succeeds. The release
/// wait is bounded to 30 seconds and unwind always releases and joins the thread.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn lock_release_and_long_refusal_preserve_neighbour_progress() {
    let temp = tempfile::Builder::new()
        .prefix("wl-")
        .tempdir_in(scratch())
        .unwrap();
    let home = temp.path().canonicalize().unwrap();
    agent_run_platform::fs::private_dir(&home).unwrap();
    drop(agent_run_store::fixture::initialize(&home).unwrap());
    let broker = test_broker::TestBroker::start(&home).await;
    let mut stream = connect(&home).await;
    for (worker, hold, expect_commit) in [(100, 50, true), (101, 700, false), (102, 0, true)] {
        let path = home.clone();
        let (send, receive) = std::sync::mpsc::sync_channel(1);
        let (release, released) = std::sync::mpsc::channel();
        let thread = std::thread::spawn(move || {
            let connection = rusqlite::Connection::open(path.join("state.db")).unwrap();
            connection.execute_batch("BEGIN IMMEDIATE").unwrap();
            send.send(()).unwrap();
            let release_result = if worker == 101 {
                Some(released.recv_timeout(Duration::from_secs(30)))
            } else {
                std::thread::sleep(Duration::from_millis(hold));
                None
            };
            connection.execute_batch("ROLLBACK").unwrap();
            if let Some(result) = release_result {
                result.expect("long lock must be explicitly released after assertions");
            }
        });
        let lock = HeldWriteLock {
            release: Some(release),
            thread: Some(thread),
        };
        receive.recv_timeout(Duration::from_secs(2)).unwrap();
        let request = envelope(worker, 1, 0);
        let started = Instant::now();
        let result = tokio::time::timeout(
            Duration::from_secs(3),
            exchange(&mut stream, &request, worker as u64),
        )
        .await
        .expect("blocked database must yield a bounded outcome");
        assert_eq!(
            matches!(result, SubmitResult::Committed(_)),
            expect_commit,
            "{result:?}"
        );
        assert!(started.elapsed() < Duration::from_secs(2));
        if !expect_commit {
            let SubmitResult::Rejected(ref rejection) = result else {
                panic!("a busy transaction must report a precise rejection: {result:?}");
            };
            assert!(!rejection.durable);
            assert_eq!(
                rejection.code,
                agent_run_domain::storage::RejectCode::StorageUnavailable
            );
            let diagnostic = rejection
                .persistence
                .as_ref()
                .expect("closed persistence diagnostics");
            assert_eq!(diagnostic.sqlite_primary, Some(5));
            assert_eq!(diagnostic.attempts, 1);
            println!("LOCK_REFUSAL={}", serde_json::to_string(rejection).unwrap());
            let reader = StoreReader::open(&home).unwrap();
            assert!(
                !reader
                    .list_accounts()
                    .unwrap()
                    .iter()
                    .any(|record| record.account_id.as_str() == "load-101-1")
            );
        }
        lock.release_and_join();
        if !expect_commit {
            let SubmitResult::Committed(first) = submit(&mut stream, &request, 201).await else {
                panic!("same identity must succeed after known no-write refusal");
            };
            let SubmitResult::Committed(replay) = submit(&mut stream, &request, 202).await else {
                panic!("retry receipt must remain replayable");
            };
            assert!(replay.replayed);
            assert_eq!(first.commit_seq, replay.commit_seq);
        }
    }
    let reader = StoreReader::open(&home).unwrap();
    let accounts = reader.list_accounts().unwrap();
    assert_eq!(accounts.len(), 3);
    let receipts: i64 = reader
        .conn()
        .query_row("SELECT COUNT(*) FROM operation_receipts", [], |row| {
            row.get(0)
        })
        .unwrap();
    assert_eq!(receipts, 3);
    drop(reader);
    broker.stop().await;
}
