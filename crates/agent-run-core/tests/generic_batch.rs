//! Synthetic batch admission and interrupted-handoff replay, without engines.
mod common;

use agent_run_core::service::Service;
use agent_run_domain::{
    Error,
    batch::StartBatchRequest,
    catalog::{AccountRecord, AccountStatus},
};
use serde_json::json;

/// Shared operator dispatch returns an idempotent release and bounded pin list.
#[tokio::test]
async fn history_pins_dispatch_lists_and_releases_exact_key() {
    use agent_run_domain::{batch::PinId, domain::AgentId};
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path();
    let store = agent_run_store::fixture::initialize(root).unwrap();
    let agent = AgentId::new();
    let pin = PinId::new();
    store
        .conn
        .execute(
            "INSERT INTO history_pins(pin_id,root_agent_id) VALUES(?,?)",
            rusqlite::params![pin.as_str(), agent.as_str()],
        )
        .unwrap();
    let writer = common::writer::Harness::start(root);
    let service = Service::new(root.to_owned()).with_writer(writer.broker());
    let first = agent_run_core::dispatch::call(
        &service,
        "history_pins",
        json!({"action":"list","status":"active"}),
    )
    .await
    .unwrap();
    assert_eq!(first["pins"].as_array().unwrap().len(), 1);
    assert_eq!(first["pins"][0]["pin_id"], pin.as_str());
    for already in [false, true] {
        let response = agent_run_core::dispatch::call(
            &service,
            "history_pins",
            json!({"action":"release","pin_id":pin}),
        )
        .await
        .unwrap();
        assert_eq!(response["pin_id"], pin.as_str());
        assert_eq!(response["status"], "released");
        assert_eq!(response["already_released"], already);
    }
    let list = agent_run_core::dispatch::call(
        &service,
        "history_pins",
        json!({"action":"list","status":"released"}),
    )
    .await
    .unwrap();
    assert_eq!(list["pins"][0]["agent_id"], agent.as_str());
    let missing = agent_run_core::dispatch::call(
        &service,
        "history_pins",
        json!({"action":"release","pin_id":PinId::new()}),
    )
    .await
    .unwrap_err();
    assert!(matches!(missing, Error::PinNotFound(_)));
    assert_eq!(missing.machine_code().as_str(), "PinNotFound");
    assert!(
        agent_run_core::dispatch::call(
            &service,
            "history_pins",
            json!({"action":"list","limit":1001})
        )
        .await
        .is_err()
    );
    assert!(
        !agent_run_domain::tools::worker_registry()
            .iter()
            .any(|tool| tool.name == "history_pins")
    );
}

/// After durable admission, a lost handoff and changed paths/config never relaunch on replay.
#[tokio::test]
async fn batch_replay_after_admission_does_not_launch_or_reload_config() {
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path();
    agent_run_store::fixture::initialize(root).unwrap();
    std::fs::create_dir_all(root.join("profiles")).unwrap();
    std::fs::create_dir(root.join("work")).unwrap();
    std::fs::write(root.join("profiles/review.md"), "+++\nrevision='1'\nwrite=false\nnetwork=false\nallow_external_read_roots=false\nskills=[]\nmcp=[]\nrequired_constraints=[]\n+++\nReview.\n").unwrap();
    let config = |cap| {
        format!(
            "schema_version=2\n[core]\nmax_active_agents={cap}\n[harnesses.codex]\nbinary='/usr/bin/true'\nhome='{}'\n[harnesses.claude-code]\nbinary='/usr/bin/true'\nhome='{}'\n[providers.codex]\nharness='codex'\nconnection={{kind='native'}}\nauth_family='openai'\nlimits_source='none'\n[[providers.codex.models]]\nid='m'\n[[providers.codex.bindings]]\nlabel='test'\naccount='acct'\n",
            root.join("codex").display(),
            root.join("claude").display()
        )
    };
    std::fs::write(root.join("config.toml"), config(1)).unwrap();
    let mut store = agent_run_store::fixture::open(root).unwrap();
    store
        .register_account(&AccountRecord {
            account_id: "acct".parse().unwrap(),
            auth_family: "openai".parse().unwrap(),
            secret_ref: "native:codex".parse().unwrap(),
            status: AccountStatus::Enabled,
        })
        .unwrap();
    let writer = common::writer::Harness::start(root);
    let service = Service::new(root.to_owned()).with_writer(writer.broker());
    let store_id = agent_run_store::store_identity::read(&store.conn).unwrap();
    let request: StartBatchRequest = serde_json::from_value(json!({"request_id":"test-batch", "expected_store_id":store_id, "jobs":[
        {"provider":"codex","model":"m","profile":"review","task":"first","workdir":root.join("work")},
        {"provider":"codex","model":"m","profile":"review","task":"second","workdir":root.join("work")}
    ]})).unwrap();
    let missing = service.batch_receipt(request.clone()).await.unwrap();
    assert!(!missing.found && missing.receipt.is_none());
    assert!(matches!(
        service.admit_batch(request.clone()).await,
        Err(Error::Capacity)
    ));
    let count = || {
        store
            .conn
            .query_row("SELECT COUNT(*) FROM agents", [], |r| r.get::<_, i64>(0))
            .unwrap()
    };
    assert_eq!(count(), 0);
    std::fs::write(root.join("config.toml"), config(5)).unwrap();
    let (first, second) = tokio::join!(
        service.admit_batch(request.clone()),
        service.admit_batch(request.clone())
    );
    let admitted = first.unwrap();
    let concurrent = second.unwrap();
    assert_ne!(admitted.replayed, concurrent.replayed);
    assert_eq!(admitted.receipt, concurrent.receipt);
    assert!(admitted.receipt.jobs.iter().all(|job| job.pin_id.is_none()));
    assert!(
        serde_json::to_value(&admitted.receipt).unwrap()["jobs"]
            .as_array()
            .unwrap()
            .iter()
            .all(|job| job.get("pin_id").is_none())
    );
    let pins: i64 = store
        .conn
        .query_row("SELECT COUNT(*) FROM history_pins", [], |row| row.get(0))
        .unwrap();
    assert_eq!(pins, 0);
    assert_eq!(count(), 2);
    let found = service.batch_receipt(request.clone()).await.unwrap();
    assert!(found.found);
    assert_eq!(found.receipt.as_ref(), Some(&admitted.receipt));

    // Draining refuses a new batch but keeps an already committed replay readable.
    writer.service.handle().close_admissions("fixture drain");
    let replay = service.start_batch(request.clone()).await.unwrap();
    assert!(replay.replayed);
    assert_eq!(replay.receipt, admitted.receipt);
    let during_drain = service.batch_receipt(request.clone()).await.unwrap();
    assert_eq!(during_drain.receipt.as_ref(), Some(&admitted.receipt));
    let mut fresh = request.clone();
    fresh.request_id = "new-during-drain".into();
    let error = service.admit_batch(fresh).await.unwrap_err();
    assert!(matches!(error, Error::Persistence { source, .. }
        if matches!(*source, Error::WriterFailure(agent_run_domain::storage::WriterFailure::NotEnqueued(
            agent_run_domain::storage::NotEnqueuedReason::AdmissionsClosed
        )))
    ));
    assert_eq!(count(), 2);

    // Simulate a caller losing the response after commit and before handoff.
    std::fs::remove_dir(root.join("work")).unwrap();
    std::fs::write(
        root.join("config.toml"),
        format!(
            "{}\n[capacity.quota]\nsocket='{}'\n",
            config(5),
            root.join("quota-down.sock").display()
        ),
    )
    .unwrap();
    let quota_down = service.batch_receipt(request.clone()).await.unwrap();
    assert_eq!(quota_down.receipt.as_ref(), Some(&admitted.receipt));
    std::fs::write(root.join("config.toml"), "invalid config").unwrap();
    let before = store
        .conn
        .query_row("SELECT COUNT(*) FROM operation_receipts", [], |row| {
            row.get::<_, i64>(0)
        })
        .unwrap();
    let found = service.batch_receipt(request.clone()).await.unwrap();
    assert_eq!(found.receipt.as_ref(), Some(&admitted.receipt));
    let after = store
        .conn
        .query_row("SELECT COUNT(*) FROM operation_receipts", [], |row| {
            row.get::<_, i64>(0)
        })
        .unwrap();
    assert_eq!(before, after, "lookup must not enqueue writes");
    let mut conflict = request.clone();
    conflict.jobs[0].task = "changed".into();
    assert!(matches!(
        service.batch_receipt(conflict).await,
        Err(Error::Conflict)
    ));
    let mut wrong_store = request.clone();
    wrong_store.expected_store_id = Some("store_ffffffffffffffffffffffffffffffff".parse().unwrap());
    assert!(matches!(
        service.batch_receipt(wrong_store).await,
        Err(Error::StoreIdentityMismatch)
    ));
    let mut no_scope = request.clone();
    no_scope.expected_store_id = None;
    assert!(matches!(
        service.batch_receipt(no_scope).await,
        Err(Error::Validation(_))
    ));
    let replay = service.start_batch(request.clone()).await.unwrap();
    assert!(replay.replayed);
    assert_eq!(replay.receipt, admitted.receipt);
    assert_eq!(count(), 2);
    let spawned: i64 = store
        .conn
        .query_row(
            "SELECT COUNT(*) FROM agents WHERE supervisor_pid IS NOT NULL",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(spawned, 0);

    // Model a broker that died after commit, before either handoff existed.
    store.conn.execute(
        "UPDATE agents SET startup_owner_pid_identity=?,startup_owner_birth_time=1 WHERE status='starting'",
        [json!({"pid":4242,"token":"linux:fixture:1","birth":1.0}).to_string()],
    ).unwrap();
    let recovered =
        agent_run_core::lifecycle::reconcile::reconcile_with(&mut store, 10, |_, _, _| {
            agent_run_core::process::ProcessState::Dead
        })
        .unwrap();
    assert_eq!(recovered.len(), 2);
    for id in &recovered {
        assert_eq!(
            store.get(id).unwrap().status,
            agent_run_domain::domain::Status::Lost
        );
    }
    let replay = service.start_batch(request).await.unwrap();
    assert!(replay.replayed);
    assert_eq!(replay.receipt, admitted.receipt);
}

/// New scoped admissions leave room for exact lookup while old receipts replay first.
#[tokio::test]
async fn scoped_batch_lookup_frame_boundary_is_checked_only_for_new_admission() {
    use agent_run_domain::observation::{MAX_RPC_ID_BYTES, SOCKET_MAX_FRAME_BYTES};
    use std::sync::{Arc, atomic::Ordering};

    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().canonicalize().unwrap();
    agent_run_store::fixture::initialize(&root).unwrap();
    std::fs::create_dir_all(root.join("profiles")).unwrap();
    std::fs::write(root.join("profiles/review.md"), "+++\nrevision='1'\nwrite=false\nnetwork=false\nallow_external_read_roots=false\nskills=[]\nmcp=[]\nrequired_constraints=[]\n+++\nReview.\n").unwrap();
    std::fs::write(root.join("config.toml"), format!("schema_version=2\n[core]\nmax_active_agents=3\n[harnesses.codex]\nbinary='/usr/bin/true'\nhome='{}'\n[harnesses.claude-code]\nbinary='/usr/bin/true'\nhome='{}'\n[providers.codex]\nharness='codex'\nconnection={{kind='native'}}\nauth_family='openai'\nlimits_source='none'\n[[providers.codex.models]]\nid='m'\n[[providers.codex.bindings]]\nlabel='test'\naccount='acct'\n",root.join("codex").display(),root.join("claude").display())).unwrap();
    let mut store = agent_run_store::fixture::open(&root).unwrap();
    store
        .register_account(&AccountRecord {
            account_id: "acct".parse().unwrap(),
            auth_family: "openai".parse().unwrap(),
            secret_ref: "native:codex".parse().unwrap(),
            status: AccountStatus::Enabled,
        })
        .unwrap();
    let writer = common::writer::Harness::start(&root);
    let launcher = Arc::new(PanickingLauncher {
        route: agent_run_core::system_launch::SystemRoute {
            provider: "codex".parse().unwrap(),
            harness: agent_run_domain::HarnessId::Codex,
            state_home: root.clone(),
            private_home: root.clone(),
            operator_socket: root.join("operator.sock"),
            writer_socket: root.join("writer.sock"),
            worker_socket: root.join("worker.sock"),
            fingerprint: "a".repeat(64).parse().unwrap(),
        },
        calls: std::sync::atomic::AtomicUsize::new(0),
    });
    let service = Service::new(root.clone())
        .with_writer(writer.broker())
        .with_system_launcher(launcher.clone());
    let store_id = agent_run_store::store_identity::read(&store.conn).unwrap();
    let mut request: StartBatchRequest = serde_json::from_value(json!({
        "request_id":"frame-size", "expected_store_id":store_id, "retain_history":true,
        "jobs":[
            {"provider":"codex","model":"m","profile":"review","task":"a".repeat(512 * 1024),"workdir":root},
            {"provider":"codex","model":"m","profile":"review","task":"b","workdir":root}
        ]
    })).unwrap();
    request.validate_intent().unwrap();
    let rpc_id = "x".repeat(MAX_RPC_ID_BYTES - 2);
    assert_eq!(serde_json::to_vec(&rpc_id).unwrap().len(), MAX_RPC_ID_BYTES);
    let frame = |request: &StartBatchRequest| {
        let mut raw = serde_json::to_vec(&json!({"jsonrpc":"2.0","id":rpc_id,"method":"tools/call","params":{"name":"batch_receipt","arguments":request}})).unwrap();
        raw.push(b'\n');
        raw
    };
    let extra = SOCKET_MAX_FRAME_BYTES - frame(&request).len();
    request.jobs[1].task.push_str(&"b".repeat(extra));
    assert!(request.jobs[1].task.len() <= 512 * 1024);
    assert_eq!(frame(&request).len(), SOCKET_MAX_FRAME_BYTES);
    request.validate_new_lookup_frame().unwrap();
    let exact_frame = frame(&request);
    let mut reader = tokio::io::BufReader::new(exact_frame.as_slice());
    assert!(
        agent_run_platform::frame::read(&mut reader, SOCKET_MAX_FRAME_BYTES)
            .await
            .unwrap()
            .is_some()
    );
    let mut oversized = request.clone();
    oversized.jobs[1].task.push('b');
    assert_eq!(frame(&oversized).len(), SOCKET_MAX_FRAME_BYTES + 1);
    assert!(oversized.validate_new_lookup_frame().is_err());
    let overflowing_frame = frame(&oversized);
    let mut reader = tokio::io::BufReader::new(overflowing_frame.as_slice());
    assert!(
        agent_run_platform::frame::read(&mut reader, SOCKET_MAX_FRAME_BYTES)
            .await
            .is_err()
    );
    let mut escaped = request.clone();
    escaped.jobs[1].task.push('"');
    assert_eq!(frame(&escaped).len(), SOCKET_MAX_FRAME_BYTES + 2);
    assert!(escaped.validate_new_lookup_frame().is_err());

    let before: i64 = store
        .conn
        .query_row("PRAGMA data_version", [], |row| row.get(0))
        .unwrap();
    assert!(matches!(
        service.start_batch(oversized.clone()).await,
        Err(Error::Validation(_))
    ));
    let after: i64 = store
        .conn
        .query_row("PRAGMA data_version", [], |row| row.get(0))
        .unwrap();
    assert_eq!(before, after, "refusal must not commit any database write");
    for table in [
        "agents",
        "attempts",
        "start_batch_receipts",
        "history_pins",
        "operation_receipts",
    ] {
        let count: i64 = store
            .conn
            .query_row(&format!("SELECT COUNT(*) FROM {table}"), [], |row| {
                row.get(0)
            })
            .unwrap();
        assert_eq!(count, 0, "{table}");
    }
    assert_eq!(launcher.calls.load(Ordering::SeqCst), 0);

    let admitted = service.admit_batch(request).await.unwrap();
    assert!(!admitted.replayed);
    assert_eq!(admitted.receipt.jobs.len(), 2);
    assert_eq!(launcher.calls.load(Ordering::SeqCst), 0);

    // Simulate an existing receipt from before the new admission guard.
    oversized.request_id = "historical-frame".into();
    let mut historical = admitted.receipt.clone();
    historical.request_id = oversized.request_id.clone();
    store.conn.execute(
        "INSERT INTO start_batch_receipts(request_id,fingerprint_version,fingerprint,receipt_json) VALUES(?,?,?,?)",
        rusqlite::params![oversized.request_id, agent_run_domain::batch::FINGERPRINT_VERSION, oversized.fingerprint().unwrap(), serde_json::to_string(&historical).unwrap()],
    ).unwrap();
    std::fs::write(root.join("config.toml"), "invalid config").unwrap();
    let replay = service.start_batch(oversized.clone()).await.unwrap();
    assert!(replay.replayed);
    assert_eq!(replay.receipt, historical);
    let lookup = service.batch_receipt(oversized.clone()).await.unwrap();
    assert_eq!(lookup.receipt, Some(historical));
    assert_eq!(launcher.calls.load(Ordering::SeqCst), 0);
    oversized.expected_store_id = None;
    assert!(oversized.validate_new_lookup_frame().is_ok());
    assert!(matches!(
        service.batch_receipt(oversized).await,
        Err(Error::Validation(_))
    ));
}

/// Writer refusals retain closed reasons; committed receipts survive changed proofs.
#[tokio::test]
async fn batch_preconditions_cross_writer_and_read_only_lookup() {
    use agent_run_domain::{MachineCode, PreconditionFailureReason, batch::BatchPrecondition};
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().canonicalize().unwrap();
    agent_run_store::fixture::initialize(&root).unwrap();
    std::fs::create_dir_all(root.join("profiles")).unwrap();
    std::fs::write(root.join("profiles/review.md"), "+++\nrevision='1'\nwrite=false\nnetwork=false\nallow_external_read_roots=false\nskills=[]\nmcp=[]\nrequired_constraints=[]\n+++\nReview.\n").unwrap();
    std::fs::write(root.join("config.toml"), format!("schema_version=2\n[core]\nmax_active_agents=4\n[harnesses.codex]\nbinary='/usr/bin/true'\nhome='{}'\n[harnesses.claude-code]\nbinary='/usr/bin/true'\nhome='{}'\n[providers.codex]\nharness='codex'\nconnection={{kind='native'}}\nauth_family='openai'\nlimits_source='none'\n[[providers.codex.models]]\nid='m'\n[[providers.codex.bindings]]\nlabel='test'\naccount='acct'\n",root.join("codex").display(),root.join("claude").display())).unwrap();
    let mut store = agent_run_store::fixture::open(&root).unwrap();
    store
        .register_account(&AccountRecord {
            account_id: "acct".parse().unwrap(),
            auth_family: "openai".parse().unwrap(),
            secret_ref: "native:codex".parse().unwrap(),
            status: AccountStatus::Enabled,
        })
        .unwrap();
    let writer = common::writer::Harness::start(&root);
    let service = Service::new(root.clone()).with_writer(writer.broker());
    let store_id = agent_run_store::store_identity::read(&store.conn).unwrap();
    let request = |key: &str| -> StartBatchRequest {
        serde_json::from_value(json!({
        "request_id":key,"expected_store_id":store_id,"retain_history":true,
        "jobs":[{"provider":"codex","model":"m","profile":"review","task":"fixture","workdir":root}]
    })).unwrap()
    };
    let seed = service
        .admit_batch(request("source"))
        .await
        .unwrap()
        .receipt
        .jobs
        .remove(0);
    store
        .conn
        .execute(
            "UPDATE agents SET status='failed',finished_at=10 WHERE id=?",
            [seed.agent_id.as_str()],
        )
        .unwrap();
    store.conn.execute("UPDATE attempts SET state='failed',finished_at=10,phase='cleanup_complete',ownership_active=0,cleanup_proof_json=? WHERE agent_id=?",
        rusqlite::params![json!({"confirmed":true,"group_gone":true,"descendants_gone":true}).to_string(),seed.agent_id.as_str()]).unwrap();
    let original = BatchPrecondition {
        agent_id: seed.agent_id.clone(),
        sequence: seed.sequence,
        execution_ref: seed.execution_ref.clone(),
        pin_id: seed.pin_id.clone().unwrap(),
    };
    let mut guarded = request("guarded");
    let mut bad = original.clone();
    bad.execution_ref = format!("ex1_{}", "f".repeat(64)).parse().unwrap();
    guarded.preconditions = Some(vec![bad]);
    let error = service.admit_batch(guarded.clone()).await.unwrap_err();
    assert_eq!(error.machine_code(), MachineCode::PreconditionFailed);
    assert_eq!(
        error.precondition_reason(),
        Some(PreconditionFailureReason::NotMet)
    );
    let mut missing = original.clone();
    missing.agent_id = agent_run_domain::domain::AgentId::new();
    guarded.preconditions = Some(vec![missing]);
    let error = service.admit_batch(guarded.clone()).await.unwrap_err();
    assert_eq!(error.machine_code(), MachineCode::PreconditionFailed);
    assert_eq!(
        error.precondition_reason(),
        Some(PreconditionFailureReason::Unknown)
    );
    let before: i64 = store
        .conn
        .query_row("SELECT COUNT(*) FROM start_batch_receipts", [], |r| {
            r.get(0)
        })
        .unwrap();
    assert_eq!(before, 1);
    guarded.preconditions = Some(vec![original.clone()]);
    let accepted = service.admit_batch(guarded.clone()).await.unwrap();
    assert!(!accepted.replayed);
    assert_eq!(accepted.receipt.preconditions, guarded.preconditions);
    assert_eq!(
        service
            .batch_receipt(guarded.clone())
            .await
            .unwrap()
            .receipt,
        Some(accepted.receipt.clone())
    );
    store
        .release_history_pin(&original.pin_id)
        .unwrap()
        .unwrap();
    let replay = service.start_batch(guarded.clone()).await.unwrap();
    assert!(replay.replayed);
    assert_eq!(replay.receipt, accepted.receipt);
    assert_eq!(
        service
            .batch_receipt(guarded.clone())
            .await
            .unwrap()
            .receipt,
        Some(accepted.receipt)
    );
    guarded.preconditions.as_mut().unwrap()[0].sequence = 2;
    assert!(matches!(
        service.batch_receipt(guarded).await,
        Err(Error::Conflict)
    ));
}

/// A lookup while admission is paused can be false even though that admission later commits.
#[cfg(feature = "test-fixtures")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn batch_receipt_missing_snapshot_can_precede_commit() {
    use std::time::Duration;
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path();
    agent_run_store::fixture::initialize(root).unwrap();
    std::fs::create_dir_all(root.join("profiles")).unwrap();
    std::fs::write(root.join("profiles/review.md"), "+++\nrevision='1'\nwrite=false\nnetwork=false\nallow_external_read_roots=false\nskills=[]\nmcp=[]\nrequired_constraints=[]\n+++\nReview.\n").unwrap();
    std::fs::write(root.join("config.toml"), format!("schema_version=2\n[core]\nmax_active_agents=3\n[harnesses.codex]\nbinary='/usr/bin/true'\nhome='{}'\n[harnesses.claude-code]\nbinary='/usr/bin/true'\nhome='{}'\n[providers.codex]\nharness='codex'\nconnection={{kind='native'}}\nauth_family='openai'\nlimits_source='none'\n[[providers.codex.models]]\nid='m'\n[[providers.codex.bindings]]\nlabel='test'\naccount='acct'\n",root.join("codex").display(),root.join("claude").display())).unwrap();
    let mut store = agent_run_store::fixture::open(root).unwrap();
    store
        .register_account(&AccountRecord {
            account_id: "acct".parse().unwrap(),
            auth_family: "openai".parse().unwrap(),
            secret_ref: "native:codex".parse().unwrap(),
            status: AccountStatus::Enabled,
        })
        .unwrap();
    let store_id = agent_run_store::store_identity::read(&store.conn).unwrap();
    let writer = common::writer::Harness::start(root);
    let service = Service::new(root.to_owned()).with_writer(writer.broker());
    let request: StartBatchRequest = serde_json::from_value(json!({"request_id":"in-flight","expected_store_id":store_id,"retain_history":true,
        "jobs":[{"provider":"codex","model":"m","profile":"review","task":"fixture","workdir":root}]})).unwrap();
    let (entered_tx, entered_rx) = tokio::sync::oneshot::channel();
    let (release_tx, release_rx) = std::sync::mpsc::channel();
    let worker = service.clone();
    let worker_request = request.clone();
    let admission = tokio::spawn(async move {
        let mut entered = Some(entered_tx);
        let mut hook = move |_: u32| {
            if let Some(entered) = entered.take() {
                let _ = entered.send(());
                release_rx
                    .recv_timeout(Duration::from_secs(10))
                    .map_err(|_| Error::Runtime("fixture release unavailable".into()))?;
            }
            Ok(())
        };
        worker.admit_batch_observed(worker_request, &mut hook).await
    });
    tokio::time::timeout(Duration::from_secs(5), entered_rx)
        .await
        .unwrap()
        .unwrap();
    let before = service.batch_receipt(request.clone()).await.unwrap();
    assert!(!before.found && before.receipt.is_none());
    release_tx.send(()).unwrap();
    let accepted = tokio::time::timeout(Duration::from_secs(10), admission)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    let after = service.batch_receipt(request).await.unwrap();
    assert!(after.found);
    assert_eq!(after.receipt.as_ref(), Some(&accepted.receipt));
    let spawned: i64 = store
        .conn
        .query_row(
            "SELECT COUNT(*) FROM agents WHERE supervisor_pid IS NOT NULL",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(spawned, 0);
}

/// Fixture launcher with an explicit first-handoff barrier; no process is spawned.
struct BlockingLauncher {
    /// Frozen route presented for every call.
    route: agent_run_core::system_launch::SystemRoute,
    /// Signals that the first handoff was actually entered.
    entered: std::sync::Mutex<Option<tokio::sync::oneshot::Sender<()>>>,
    /// Releases the first handoff after the client wait is dropped.
    release: std::sync::Mutex<Option<tokio::sync::oneshot::Receiver<()>>>,
    /// Number of handoffs, independent of process creation.
    calls: std::sync::atomic::AtomicUsize,
}

/// Panics synchronously on the first handoff and records the next attempt.
struct PanickingLauncher {
    /// Frozen route presented for both jobs.
    route: agent_run_core::system_launch::SystemRoute,
    /// Number of first-time handoff attempts.
    calls: std::sync::atomic::AtomicUsize,
}

impl agent_run_core::system_launch::SystemLauncher for PanickingLauncher {
    fn route(
        &self,
        _: &agent_run_domain::ProviderId,
        _: agent_run_domain::HarnessId,
    ) -> agent_run_domain::Result<agent_run_core::system_launch::SystemRoute> {
        Ok(self.route.clone())
    }

    fn launch(
        &self,
        _: std::path::PathBuf,
        _: agent_run_domain::domain::AgentId,
        _: agent_run_core::system_launch::SystemRoute,
        _: agent_run_core::broker_writer::BrokerLane,
    ) -> std::pin::Pin<
        Box<dyn std::future::Future<Output = agent_run_domain::Result<()>> + Send + 'static>,
    > {
        if self.calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst) == 0 {
            panic!("synthetic handoff panic");
        }
        Box::pin(async { Err(Error::Runtime("synthetic unavailable launcher".into())) })
    }
}

/// A panic leaves its job unknown, does not skip the next job or replay either handoff.
#[tokio::test]
async fn batch_handoff_panic_does_not_skip_later_job() {
    use std::sync::{Arc, atomic::Ordering};
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().canonicalize().unwrap();
    agent_run_store::fixture::initialize(&root).unwrap();
    std::fs::create_dir_all(root.join("profiles")).unwrap();
    std::fs::write(root.join("profiles/review.md"), "+++\nrevision='1'\nwrite=false\nnetwork=false\nallow_external_read_roots=false\nskills=[]\nmcp=[]\nrequired_constraints=[]\n+++\nReview.\n").unwrap();
    std::fs::write(root.join("config.toml"), format!("schema_version=2\n[core]\nmax_active_agents=3\n[harnesses.codex]\nbinary='/usr/bin/true'\nhome='{}'\n[harnesses.claude-code]\nbinary='/usr/bin/true'\nhome='{}'\n[providers.codex]\nharness='codex'\nconnection={{kind='native'}}\nauth_family='openai'\nlimits_source='none'\n[[providers.codex.models]]\nid='m'\n[[providers.codex.bindings]]\nlabel='test'\naccount='acct'\n",root.join("codex").display(),root.join("claude").display())).unwrap();
    let mut store = agent_run_store::fixture::open(&root).unwrap();
    store
        .register_account(&AccountRecord {
            account_id: "acct".parse().unwrap(),
            auth_family: "openai".parse().unwrap(),
            secret_ref: "native:codex".parse().unwrap(),
            status: AccountStatus::Enabled,
        })
        .unwrap();
    let writer = common::writer::Harness::start(&root);
    let launcher = Arc::new(PanickingLauncher {
        route: agent_run_core::system_launch::SystemRoute {
            provider: "codex".parse().unwrap(),
            harness: agent_run_domain::HarnessId::Codex,
            state_home: root.clone(),
            private_home: root.clone(),
            operator_socket: root.join("operator.sock"),
            writer_socket: root.join("writer.sock"),
            worker_socket: root.join("worker.sock"),
            fingerprint: "a".repeat(64).parse().unwrap(),
        },
        calls: std::sync::atomic::AtomicUsize::new(0),
    });
    let service = Service::new(root.clone())
        .with_writer(writer.broker())
        .with_system_launcher(launcher.clone());
    let request: StartBatchRequest =
        serde_json::from_value(json!({"request_id":"panic-handoff", "jobs":[
            {"provider":"codex","model":"m","profile":"review","task":"one","workdir":root},
            {"provider":"codex","model":"m","profile":"review","task":"two","workdir":root}
        ]}))
        .unwrap();
    let admitted = service.start_batch(request.clone()).await.unwrap();
    assert!(!admitted.replayed);
    assert_eq!(launcher.calls.load(Ordering::SeqCst), 2);
    let first = &admitted.receipt.jobs[0].agent_id;
    let row = store.get(first).unwrap();
    assert_eq!(row.status, agent_run_domain::domain::Status::Starting);
    assert!(row.supervisor_pid.is_none());
    let owner: Option<String> = store
        .conn
        .query_row(
            "SELECT startup_owner_pid_identity FROM agents WHERE id=?",
            [first.as_str()],
            |r| r.get(0),
        )
        .unwrap();
    assert!(owner.is_some());
    let active: i64 = store
        .conn
        .query_row(
            "SELECT ownership_active FROM attempts WHERE agent_id=?",
            [first.as_str()],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(active, 1);
    let event: String = store
        .conn
        .query_row(
            "SELECT data_json FROM events WHERE agent_id=? AND kind='supervisor_handoff_error'",
            [first.as_str()],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(&event).unwrap()["reason"],
        "panic"
    );
    let replay = service.start_batch(request).await.unwrap();
    assert!(replay.replayed);
    assert_eq!(replay.receipt, admitted.receipt);
    assert_eq!(launcher.calls.load(Ordering::SeqCst), 2);
}

impl agent_run_core::system_launch::SystemLauncher for BlockingLauncher {
    fn route(
        &self,
        _: &agent_run_domain::ProviderId,
        _: agent_run_domain::HarnessId,
    ) -> agent_run_domain::Result<agent_run_core::system_launch::SystemRoute> {
        Ok(self.route.clone())
    }

    fn launch(
        &self,
        _: std::path::PathBuf,
        _: agent_run_domain::domain::AgentId,
        _: agent_run_core::system_launch::SystemRoute,
        _: agent_run_core::broker_writer::BrokerLane,
    ) -> std::pin::Pin<
        Box<dyn std::future::Future<Output = agent_run_domain::Result<()>> + Send + 'static>,
    > {
        self.calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        let entered = self.entered.lock().unwrap().take();
        let release = self.release.lock().unwrap().take();
        Box::pin(async move {
            if let Some(entered) = entered {
                let _ = entered.send(());
            }
            if let Some(release) = release {
                let _ = release.await;
            }
            Err(Error::Runtime("synthetic handoff unavailable".into()))
        })
    }
}

/// Cancelling a client during handoff leaves the owned operation running; replay never launches twice.
#[tokio::test]
async fn cancelled_batch_wait_keeps_owned_handoffs_and_original_receipt() {
    use std::sync::{Arc, Mutex, atomic::Ordering};
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().canonicalize().unwrap();
    agent_run_store::fixture::initialize(&root).unwrap();
    std::fs::create_dir_all(root.join("profiles")).unwrap();
    std::fs::write(root.join("profiles/review.md"), "+++\nrevision='1'\nwrite=false\nnetwork=false\nallow_external_read_roots=false\nskills=[]\nmcp=[]\nrequired_constraints=[]\n+++\nReview.\n").unwrap();
    std::fs::write(root.join("config.toml"), format!("schema_version=2\n[core]\nmax_active_agents=3\n[harnesses.codex]\nbinary='/usr/bin/true'\nhome='{}'\n[harnesses.claude-code]\nbinary='/usr/bin/true'\nhome='{}'\n[providers.codex]\nharness='codex'\nconnection={{kind='native'}}\nauth_family='openai'\nlimits_source='none'\n[[providers.codex.models]]\nid='m'\n[[providers.codex.bindings]]\nlabel='test'\naccount='acct'\n",root.join("codex").display(),root.join("claude").display())).unwrap();
    let mut store = agent_run_store::fixture::open(&root).unwrap();
    store
        .register_account(&AccountRecord {
            account_id: "acct".parse().unwrap(),
            auth_family: "openai".parse().unwrap(),
            secret_ref: "native:codex".parse().unwrap(),
            status: AccountStatus::Enabled,
        })
        .unwrap();
    let writer = common::writer::Harness::start(&root);
    let (entered_tx, entered_rx) = tokio::sync::oneshot::channel();
    let (release_tx, release_rx) = tokio::sync::oneshot::channel();
    let launcher = Arc::new(BlockingLauncher {
        route: agent_run_core::system_launch::SystemRoute {
            provider: "codex".parse().unwrap(),
            harness: agent_run_domain::HarnessId::Codex,
            state_home: root.clone(),
            private_home: root.clone(),
            operator_socket: root.join("operator.sock"),
            writer_socket: root.join("writer.sock"),
            worker_socket: root.join("worker.sock"),
            fingerprint: "a".repeat(64).parse().unwrap(),
        },
        entered: Mutex::new(Some(entered_tx)),
        release: Mutex::new(Some(release_rx)),
        calls: std::sync::atomic::AtomicUsize::new(0),
    });
    let service = Service::new(root.clone())
        .with_writer(writer.broker())
        .with_system_launcher(launcher.clone());
    let mut request: StartBatchRequest =
        serde_json::from_value(json!({"request_id":"cancel-wait", "jobs":[
            {"provider":"codex","model":"m","profile":"review","task":"one","workdir":root},
            {"provider":"codex","model":"m","profile":"review","task":"two","workdir":root}
        ]}))
        .unwrap();
    request.validate_intent().unwrap();
    let fingerprint = request.fingerprint().unwrap();
    let caller = tokio::spawn({
        let service = service.clone();
        let request = request.clone();
        async move { service.start_batch(request).await }
    });
    tokio::time::timeout(std::time::Duration::from_secs(10), entered_rx)
        .await
        .unwrap()
        .unwrap();
    let before = store
        .replay_batch(&request.request_id, &fingerprint)
        .unwrap()
        .unwrap()
        .receipt;
    // Dropping the transport's wait detaches its owned task rather than aborting it.
    drop(caller);
    release_tx.send(()).unwrap();
    tokio::time::timeout(std::time::Duration::from_secs(10), async {
        loop {
            let n: i64 = store
                .conn
                .query_row(
                    "SELECT COUNT(*) FROM events WHERE kind='supervisor_handoff_error'",
                    [],
                    |r| r.get(0),
                )
                .unwrap();
            if n == 2 {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    let replay = service.start_batch(request).await.unwrap();
    assert!(replay.replayed);
    assert_eq!(replay.receipt, before);
    assert_eq!(launcher.calls.load(Ordering::SeqCst), 2);
    let spawned: i64 = store
        .conn
        .query_row(
            "SELECT COUNT(*) FROM agents WHERE supervisor_pid IS NOT NULL",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(spawned, 0);
}

/// A restart refusal records every distinct explicit caller, including alias duplicates.
#[tokio::test]
async fn blocked_batch_records_each_explicit_restart_caller_once() {
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path();
    let mut store = agent_run_store::fixture::initialize(root).unwrap();
    let restart = store.begin_service_restart("restart-batch", 1.0).unwrap();
    let writer = common::writer::Harness::start(root);
    let service = Service::new(root.to_owned()).with_writer(writer.broker());
    let job = |transport: &str, session: &str| {
        json!({
            "provider":"codex", "model":"m", "profile":"review", "task":"one", "workdir":root,
            "orchestrator":{"transport":transport,"external_session_id":session}
        })
    };
    let request: StartBatchRequest =
        serde_json::from_value(json!({"request_id":"blocked", "jobs":[
            job("codex", "same"), job("codex_queue", "same"), job("claude", "other")
        ]}))
        .unwrap();
    assert!(matches!(
        service.start_batch(request).await,
        Err(Error::Transition(_))
    ));
    let recipients: i64 = store
        .conn
        .query_row(
            "SELECT COUNT(*) FROM service_restart_recipients WHERE restart_id=? AND tried_start=1",
            [restart],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(recipients, 2);
    let agents: i64 = store
        .conn
        .query_row("SELECT COUNT(*) FROM agents", [], |r| r.get(0))
        .unwrap();
    assert_eq!(agents, 0);
}
