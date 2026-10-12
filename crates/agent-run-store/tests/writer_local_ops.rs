//! Broker-local and recovery rows through the sole writer on disposable
//! private homes: admissions commit with their receipt and replay by request
//! identity, delivery and ledger maintenance rows run as bounded batches,
//! stale unscoped receipts and watermarks are pruned, recovery evidence
//! reads (never occupies) the original receipt, and capacity, delivery,
//! reconciliation, service and history rows apply only their guarded
//! transitions.
mod common;

use agent_run_domain::{
    domain::AgentId,
    storage::{MonoDeadline, OperationId, ProducerId, ProducerKind, SubmitResult, WriteAuthority},
};
use agent_run_store::{
    ops::{
        AdmitStart, AdmitStartOutput, AppendJournal, CapacitySampleRow, ClaimDelivery,
        CompleteDelivery, DeliveryCompletion, DeliveryPolicySpec, EnsureServiceGeneration,
        ExpectedOwnership, ExpireUnboundDeliveries, FailDelivery, GateStep, GenerationAction,
        JournalFragment, LocalEnvelope, LocalOp, MarkLost, MarkServiceReady, MarkServiceUnhealthy,
        OrphanOutcome, PersistCapacitySamples, PersistRecoveryObservation, PrepareServiceGate,
        PruneCapacitySamples, PruneHistoryBatch, PruneIncidentLedger, PruneReceipts,
        ReconcileReapedAgent, RecoveryRecorded, ReleaseOrphanedAttempt, RemoteOp,
        StopServiceGeneration, WriteEnvelope,
    },
    writer::{StoreWriter, WriterLease},
};
use rusqlite::params;
use serde_json::json;
use std::cell::Cell;

/// A home with an open writer and a strictly increasing ordinal.
struct Fixture {
    /// Disposable private home.
    home: common::Home,
    /// Sole writer.
    writer: StoreWriter<'static>,
    /// Last used ordinal.
    ordinal: Cell<u64>,
}

impl Fixture {
    /// A home with an open writer.
    fn new() -> Self {
        let home = common::Home::new();
        let writer = StoreWriter::open(WriterLease::acquire_online(&home.path).unwrap()).unwrap();
        Self {
            home,
            writer,
            ordinal: Cell::new(0),
        }
    }

    /// Identity, producer, next ordinal and deadline of one envelope.
    fn header(&self) -> (OperationId, ProducerId, u64, MonoDeadline) {
        let (boot, now) = agent_run_platform::monotonic::now().unwrap();
        self.ordinal.set(self.ordinal.get() + 1);
        (
            OperationId::new(&boot, now),
            ProducerId {
                kind: ProducerKind::Broker,
                scope: Some("local".into()),
                instance: "instance-1".into(),
            },
            self.ordinal.get(),
            MonoDeadline {
                boot_id: boot,
                ns: now + 60_000_000_000,
            },
        )
    }

    /// A local envelope of `op`.
    fn local(&self, op: LocalOp) -> LocalEnvelope {
        let (id, producer, ordinal, deadline) = self.header();
        LocalEnvelope::new(id, producer, ordinal, deadline, op).unwrap()
    }

    /// A remote envelope of `op`.
    fn remote(&self, op: RemoteOp) -> WriteEnvelope {
        let (id, producer, ordinal, deadline) = self.header();
        WriteEnvelope::new(id, producer, ordinal, deadline, op).unwrap()
    }

    /// Applies a local `op` as the broker and returns the committed result.
    fn commit_local(&mut self, op: LocalOp) -> serde_json::Value {
        let envelope = self.local(op);
        match self.writer.apply_local(&WriteAuthority::Broker, &envelope) {
            SubmitResult::Committed(receipt) => receipt.result,
            other => panic!("not committed: {other:?}"),
        }
    }

    /// Applies a remote `op` as the broker and returns the committed result.
    fn commit_remote(&mut self, op: RemoteOp) -> agent_run_domain::storage::WriteReceipt {
        let envelope = self.remote(op);
        match self.writer.apply(&WriteAuthority::Broker, &envelope) {
            SubmitResult::Committed(receipt) => receipt,
            other => panic!("not committed: {other:?}"),
        }
    }
}

/// A legacy admission commits with its receipt; replaying the same envelope
/// answers the stored receipt and a new identity with the same request
/// replays the admission itself.
#[test]
fn admission_commits_and_replays_by_request() {
    let mut f = Fixture::new();
    let mut request = f.home.request();
    request.request_id = Some("replay-key".into());
    let admit = || {
        LocalOp::from(AdmitStart {
            request: request.clone(),
            config: f.home.config.clone(),
            config_revision: "cfg".into(),
            identity: json!({}),
            parent: None,
        })
    };
    let envelope = f.local(admit());
    let SubmitResult::Committed(first) = f.writer.apply_local(&WriteAuthority::Broker, &envelope)
    else {
        panic!("not committed");
    };
    let output: AdmitStartOutput = first.output().unwrap();
    assert!(!output.replayed);
    let SubmitResult::Committed(again) = f.writer.apply_local(&WriteAuthority::Broker, &envelope)
    else {
        panic!("not replayed");
    };
    assert!(again.replayed);
    assert_eq!(again.result, first.result);
    let second: AdmitStartOutput = serde_json::from_value(f.commit_local(admit())).unwrap();
    assert!(second.replayed);
    assert_eq!(second.agent_id, output.agent_id);
}

/// A stale watermark of a producer the scheduler reports live (unfinished
/// work) is never pruned; the report is consumed by that one batch, after
/// which the watermark is pruned like any other.
#[test]
fn live_producer_watermarks_are_never_pruned() {
    let mut f = Fixture::new();
    let (boot, now) = agent_run_platform::monotonic::now().unwrap();
    for key in ["p-live", "p-gone"] {
        f.sql(
            "INSERT INTO writer_producers(producer_key,last_ordinal,last_boot_tag,last_issued_ns,execution_id,updated_at) VALUES(?,1,?,1,NULL,1.0)",
            params![key, "0".repeat(16)],
        );
    }
    let prune = || {
        LocalOp::from(PruneReceipts {
            at: 2.0e9,
            boot_id: boot.clone(),
            now_ns: now,
        })
    };
    f.writer.set_live_producers(vec!["p-live".into()]);
    assert_eq!(f.commit_local(prune()), json!(1));
    let live = "SELECT COUNT(*) FROM writer_producers WHERE producer_key='p-live'";
    assert_eq!(f.count(live), 1);
    assert_eq!(
        f.count("SELECT COUNT(*) FROM writer_producers WHERE producer_key='p-gone'"),
        0
    );
    assert_eq!(f.commit_local(prune()), json!(1));
    assert_eq!(f.count(live), 0);
}

/// Delivery expiry and incident-ledger pruning run as bounded broker batches.
#[test]
fn maintenance_batches_commit() {
    let mut f = Fixture::new();
    let expired = f.commit_local(LocalOp::from(ExpireUnboundDeliveries { at: 1.0e9 }));
    assert_eq!(expired, json!([]));
    let removed = f.commit_local(LocalOp::from(PruneIncidentLedger { at: 1.0e9 }));
    assert_eq!(removed, json!(0));
}

/// Only receipts and watermarks proven stale on the monotonic clock (another
/// boot) AND older than 14 days of wall time are pruned: a forward wall
/// jump on a fresh identity, a recent commit of an old boot and a scoped
/// receipt all stay; a payload from another boot refuses.
#[test]
fn stale_unscoped_receipts_and_watermarks_are_pruned() {
    let mut f = Fixture::new();
    let store = f.home.store();
    let id = f.admitted();
    let (boot, now) = agent_run_platform::monotonic::now().unwrap();
    let at = 2.0e9;
    for (n, operation, agent, committed_at) in [
        (1, OperationId::new("old-boot", 1), None, 1.0),
        (2, OperationId::new(&boot, now), None, 1.0),
        (3, OperationId::new("old-boot", 3), None, at),
        (4, OperationId::new("old-boot", 4), Some(id.as_str()), 1.0),
    ] {
        store
            .conn
            .execute(
                "INSERT INTO operation_receipts(operation_id,digest,op_kind,producer_key,ordinal,authority_kind,authority_fingerprint,agent_id,outcome,result_json,writer_epoch,committed_at) \
                 VALUES(?,?,'test','p-old',?,'broker',?,?,'committed','{}',1,?)",
                params![operation.as_str(), "d".repeat(64), n, "f".repeat(64), agent, committed_at],
            )
            .unwrap();
    }
    for (key, tag, issued) in [
        ("p-gone", "0".repeat(16), 1i64),
        (
            "p-fresh",
            agent_run_domain::storage::boot_tag(&boot),
            now as i64,
        ),
    ] {
        store
            .conn
            .execute(
                "INSERT INTO writer_producers(producer_key,last_ordinal,last_boot_tag,last_issued_ns,execution_id,updated_at) VALUES(?,1,?,?,NULL,1.0)",
                params![key, tag, issued],
            )
            .unwrap();
    }
    let foreign = f.refuse_local(LocalOp::from(PruneReceipts {
        at,
        boot_id: "another-boot".into(),
        now_ns: now,
    }));
    assert!(foreign.durable);
    let removed = f.commit_local(LocalOp::from(PruneReceipts {
        at,
        boot_id: boot,
        now_ns: now,
    }));
    assert_eq!(removed, json!(2));
    assert_eq!(
        f.count("SELECT COUNT(*) FROM operation_receipts WHERE producer_key='p-old'"),
        3
    );
    assert_eq!(
        f.count("SELECT COUNT(*) FROM writer_producers WHERE producer_key='p-gone'"),
        0
    );
    assert_eq!(
        f.count("SELECT COUNT(*) FROM writer_producers WHERE producer_key='p-fresh'"),
        1
    );
}

/// Recovery evidence reads the original receipt (committed, missing) without
/// occupying it; the original id is a global natural key whose later records
/// replay the first stored record (another execution claiming it refuses);
/// a foreign attempt or an original receipt scoped to another execution
/// refuses.
#[test]
fn recovery_observation_reads_the_original_receipt_once() {
    let mut f = Fixture::new();
    let id = f.admitted();
    let other = f.admitted();
    for (execution, number) in [(&id, 1), (&other, 1)] {
        f.sql(
            "INSERT OR IGNORE INTO attempts(id,agent_id,number,state,adapter_state_json,created_at) VALUES(?,?,?,'running','{}',1)",
            params![format!("{execution}:{number}"), execution.as_str(), number],
        );
    }
    let attempt = format!("{id}:1");
    let original = f.commit_remote(RemoteOp::from(AppendJournal {
        execution: id.clone(),
        fragments: vec![JournalFragment::Event {
            kind: "test_event".into(),
            data: json!({}),
            attempt: None,
        }],
    }));
    let observe = |execution: &AgentId, attempt: &str, operation: OperationId, digest| {
        RemoteOp::from(PersistRecoveryObservation {
            execution: execution.clone(),
            attempt: attempt.into(),
            original_operation_id: operation,
            original_digest: digest,
            kind: "stdin_written".into(),
            result: json!({"accepted": true}),
            evidence: json!({"bytes": 3}),
        })
    };
    let first: RecoveryRecorded = f
        .commit_remote(observe(
            &id,
            &attempt,
            original.operation_id.clone(),
            original.digest.clone(),
        ))
        .output()
        .unwrap();
    assert!(first.recorded);
    assert_eq!(first.original_committed, Some(true));
    let other_digest = f.remote(RemoteOp::from(AppendJournal {
        execution: id.clone(),
        fragments: Vec::new(),
    }));
    let replayed: RecoveryRecorded = f
        .commit_remote(observe(
            &id,
            &attempt,
            original.operation_id.clone(),
            other_digest.header().digest.clone(),
        ))
        .output()
        .unwrap();
    assert!(!replayed.recorded);
    assert_eq!(replayed.original_committed, Some(true));
    let claim = f.remote(observe(
        &other,
        &format!("{other}:1"),
        original.operation_id.clone(),
        original.digest.clone(),
    ));
    assert!(matches!(
        f.writer.apply(&WriteAuthority::Broker, &claim),
        SubmitResult::Rejected(refusal) if refusal.durable
    ));
    let (boot, now) = agent_run_platform::monotonic::now().unwrap();
    let foreign_attempt = f.remote(observe(
        &id,
        &format!("{other}:1"),
        OperationId::new(&boot, now),
        original.digest.clone(),
    ));
    assert!(matches!(
        f.writer.apply(&WriteAuthority::Broker, &foreign_attempt),
        SubmitResult::Rejected(refusal) if refusal.durable
    ));
    let scoped = OperationId::new(&boot, now + 1);
    f.sql(
        "INSERT INTO operation_receipts(operation_id,digest,op_kind,producer_key,ordinal,authority_kind,authority_fingerprint,agent_id,outcome,result_json,writer_epoch,committed_at) \
         VALUES(?,?,'test','p-x',1,'broker',?,?,'committed','{}',1,1)",
        params![scoped.as_str(), "d".repeat(64), "f".repeat(64), other.as_str()],
    );
    let wrong_scope = f.remote(observe(&id, &attempt, scoped, original.digest.clone()));
    assert!(matches!(
        f.writer.apply(&WriteAuthority::Broker, &wrong_scope),
        SubmitResult::Rejected(refusal) if refusal.durable
    ));
    let missing: RecoveryRecorded = f
        .commit_remote(observe(
            &id,
            &attempt,
            OperationId::new(&boot, now + 2),
            original.digest.clone(),
        ))
        .output()
        .unwrap();
    assert!(missing.recorded);
    assert_eq!(missing.original_committed, None);
    assert_eq!(
        f.count("SELECT COUNT(*) FROM events WHERE kind='recovery_observation_v1'"),
        2
    );
}

impl Fixture {
    /// Applies a local `op` as the broker and returns the refusal.
    fn refuse_local(&mut self, op: LocalOp) -> agent_run_domain::storage::StorageRejection {
        let envelope = self.local(op);
        match self.writer.apply_local(&WriteAuthority::Broker, &envelope) {
            SubmitResult::Rejected(refusal) => refusal,
            other => panic!("not refused: {other:?}"),
        }
    }

    /// Refuse a wire operation through the same fenced transaction while preserving its durable rejection.
    fn refuse_remote(&mut self, op: RemoteOp) -> agent_run_domain::storage::StorageRejection {
        let envelope = self.remote(op);
        match self.writer.apply(&WriteAuthority::Broker, &envelope) {
            SubmitResult::Rejected(refusal) => refusal,
            other => panic!("not refused: {other:?}"),
        }
    }

    /// Admits one execution and returns its id.
    fn admitted(&self) -> AgentId {
        let mut store = self.home.store();
        let (id, _) = store
            .admit(&self.home.request(), &self.home.config, &json!({}), None)
            .unwrap();
        id
    }

    /// Runs one SQL statement on a raw store connection.
    fn sql(&self, statement: &str, values: impl rusqlite::Params) {
        self.home.store().conn.execute(statement, values).unwrap();
    }

    /// Counts rows of one query.
    fn count(&self, query: &str) -> i64 {
        self.home
            .store()
            .conn
            .query_row(query, [], |row| row.get(0))
            .unwrap()
    }
}

/// A valid slice persists its samples and route snapshot and trims to the
/// retention bound; malformed topology, an unmeasured pool key, duplicate or
/// cross-runtime samples, an out-of-range percentage and a zero retention
/// are durable validation refusals with nothing written.
#[test]
fn capacity_slices_persist_and_trim() {
    let mut f = Fixture::new();
    let sample = |runtime: &str, window: &str, percent: f64| CapacitySampleRow {
        runtime: runtime.into(),
        lane: "default".into(),
        window: window.into(),
        target: None,
        source: "native".into(),
        remaining_percent: Some(percent),
        reset_at: None,
        observed_at: 10.0,
        valid_until: 20.0,
    };
    let key = |window: &str| json!({"runtime":"claude","lane":"default","window":window,"target":null,"source":"native"});
    let topology =
        json!({"pools":[{"pool_id":"p","keys":[key("five_hour"),key("weekly")]}],"routes":[]})
            .to_string();
    let slice = |topology: &str, samples: Vec<CapacitySampleRow>, retention: usize| {
        RemoteOp::from(PersistCapacitySamples {
            runtime: "claude".into(),
            scope_id: "scope".into(),
            observed_at: 10.0,
            valid_until: 20.0,
            topology_json: topology.into(),
            samples,
            retention,
        })
    };
    let good = || {
        vec![
            sample("claude", "five_hour", 50.0),
            sample("claude", "weekly", 50.0),
        ]
    };
    let receipt = f.commit_remote(slice(&topology, good(), 1));
    assert_eq!(receipt.result, json!(2));
    assert_eq!(f.count("SELECT COUNT(*) FROM capacity_samples"), 1);
    assert_eq!(f.count("SELECT COUNT(*) FROM capacity_route_snapshots"), 1);
    let unmeasured =
        json!({"pools":[{"pool_id":"p","keys":[key("five_hour"),key("monthly")]}],"routes":[]})
            .to_string();
    for bad in [
        slice("not json", good(), 1),
        slice(&unmeasured, good(), 1),
        slice(
            &topology,
            vec![
                sample("claude", "five_hour", 50.0),
                sample("claude", "weekly", 50.0),
                sample("claude", "five_hour", 40.0),
            ],
            1,
        ),
        slice(
            &topology,
            vec![
                sample("codex", "five_hour", 50.0),
                sample("claude", "weekly", 50.0),
            ],
            1,
        ),
        slice(
            &topology,
            vec![
                sample("claude", "five_hour", 150.0),
                sample("claude", "weekly", 50.0),
            ],
            1,
        ),
    ] {
        let envelope = f.remote(bad);
        assert!(matches!(
            f.writer.apply(&WriteAuthority::Broker, &envelope),
            SubmitResult::Rejected(refusal) if refusal.durable
        ));
    }
    let zero = f.refuse_remote(RemoteOp::from(PruneCapacitySamples { retention: 0 }));
    assert!(zero.durable);
    assert_eq!(f.count("SELECT COUNT(*) FROM capacity_samples"), 1);
}

/// The delivery policy used by these tests.
fn policy() -> DeliveryPolicySpec {
    DeliveryPolicySpec {
        retry_base_seconds: 1.0,
        retry_cap_seconds: 10.0,
        max_attempts: 3,
        lease_seconds: 30.0,
    }
}

/// A due completion notice is leased with validated payload facts; only the
/// live lease owner may record its outcome.
#[test]
fn delivery_claim_and_completion_follow_the_lease() {
    let mut f = Fixture::new();
    let id = f.admitted();
    f.sql(
        "INSERT INTO orchestrator_sessions(id,transport,external_session_id,created_at,last_seen_at) VALUES('s','codex_queue','thread',1,1)",
        [],
    );
    f.sql(
        "UPDATE agents SET status='succeeded',finished_at=5,orchestrator_session_id='s' WHERE id=?",
        [id.as_str()],
    );
    f.sql(
        "INSERT INTO events(agent_id,at,kind,from_status,to_status,data_json) VALUES(?,5,'status','running','succeeded','{}')",
        [id.as_str()],
    );
    f.sql(
        "INSERT INTO deliveries(id,agent_id,orchestrator_session_id,terminal_event_seq,state,next_attempt_at) \
         VALUES('ntf_test',?,'s',(SELECT MAX(seq) FROM events),'pending',0)",
        [id.as_str()],
    );
    let claim = f
        .commit_remote(RemoteOp::from(ClaimDelivery {
            owner: "owner-1".into(),
            at: 100.0,
            policy: policy(),
            attention_peer: false,
        }))
        .result;
    assert_eq!(claim["delivery_id"], "ntf_test");
    assert_eq!(claim["attempt"], 1);
    assert_eq!(claim["payload"]["kind"], "completion");
    assert_eq!(claim["payload"]["notice"]["status"], "succeeded");
    let complete = |owner: &str| {
        RemoteOp::from(CompleteDelivery {
            completion: DeliveryCompletion {
                delivery_id: "ntf_test".into(),
                owner: owner.into(),
                attempt: 1,
                transport: "codex_queue".into(),
                accepted: true,
                ambiguous: false,
                classifier: "queue_accepted".into(),
                evidence: None,
                policy: policy(),
                at: 101.0,
            },
        })
    };
    assert_eq!(
        f.commit_remote(complete("intruder")).result,
        json!("not_owned")
    );
    assert_eq!(
        f.commit_remote(complete("owner-1")).result,
        json!("delivered")
    );
    assert_eq!(
        f.count("SELECT COUNT(*) FROM deliveries WHERE state='delivered' AND lease_owner IS NULL"),
        1
    );
}

/// Reconciliation applies only proof-gated, ownership-exact transitions: a
/// mismatched expectation changes nothing and the exact one marks lost;
/// orphaned ownership is never released for a running execution, an
/// unconfirmed proof, never-spawned evidence of a spawned attempt or a
/// stored proof without completed cleanup; unresolved cleanup is recorded
/// once with a closed reason and a confirmed proof of a spawned attempt
/// releases ownership.
#[test]
fn reconciliation_is_ownership_exact_and_proof_gated() {
    let mut f = Fixture::new();
    let id = f.admitted();
    f.commit_local(LocalOp::from(ReconcileReapedAgent {
        cursor: "orphaned_attempts".into(),
        created_at: 3.0,
        execution: id.clone(),
        attempt: None,
    }));
    assert_eq!(
        f.count("SELECT COUNT(*) FROM reconciliation_cursors WHERE name='orphaned_attempts'"),
        1
    );
    f.sql(
        "UPDATE agents SET status='running',supervisor_pid=10,supervisor_identity='t',supervisor_birth_time=1.0,process_group_id=10,heartbeat_at=2 WHERE id=?",
        [id.as_str()],
    );
    let attempt = format!("{id}:1");
    f.sql(
        "INSERT OR REPLACE INTO attempts(id,agent_id,number,state,adapter_state_json,created_at,ownership_active,process_identity,process_birth_time,phase) \
         VALUES(?,?,1,'running','{}',1,1,'darwin:1:1',1.0,'running')",
        params![attempt, id.as_str()],
    );
    let orphan = |outcome: OrphanOutcome| {
        LocalOp::from(ReleaseOrphanedAttempt {
            execution: id.clone(),
            attempt: attempt.clone(),
            outcome,
        })
    };
    let confirmed = || OrphanOutcome::Release {
        proof: Some(json!({"confirmed": true, "group_gone": true})),
    };
    assert!(f.refuse_local(orphan(confirmed())).durable);
    let lost = |identity: &str| {
        LocalOp::from(MarkLost {
            expected: ExpectedOwnership {
                execution: id.clone(),
                supervisor_pid: Some(10),
                supervisor_identity: Some(identity.into()),
                supervisor_birth_time: Some(1.0),
                process_group_id: Some(10),
                startup_owner: None,
                startup_birth_time: None,
            },
            failure_kind: "supervisor_dead".into(),
            failure_text: "supervisor process is dead".into(),
            evidence: json!({"verdict": "dead"}),
            checked_at: 5.0,
        })
    };
    assert_eq!(f.commit_local(lost("other")), json!(false));
    assert_eq!(
        f.count("SELECT COUNT(*) FROM agents WHERE status='lost'"),
        0
    );
    assert_eq!(f.commit_local(lost("t")), json!(true));
    assert_eq!(
        f.count("SELECT COUNT(*) FROM agents WHERE status='lost'"),
        1
    );
    for refused in [
        OrphanOutcome::Release {
            proof: Some(json!({"confirmed": false})),
        },
        OrphanOutcome::Release {
            proof: Some(json!({"never_spawned": true, "reconciled": true})),
        },
        OrphanOutcome::Release { proof: None },
    ] {
        assert!(f.refuse_local(orphan(refused)).durable);
    }
    assert_eq!(
        f.count("SELECT COUNT(*) FROM attempts WHERE ownership_active=1"),
        1
    );
    let unresolved = || OrphanOutcome::Unresolved {
        reason: "cleanup_unconfirmed".into(),
    };
    assert_eq!(f.commit_local(orphan(unresolved())), json!(false));
    assert_eq!(f.commit_local(orphan(unresolved())), json!(false));
    assert_eq!(
        f.count("SELECT COUNT(*) FROM events WHERE kind='attempt_cleanup_unresolved'"),
        1
    );
    let unknown = f.refuse_local(orphan(OrphanOutcome::Unresolved {
        reason: "because".into(),
    }));
    assert!(unknown.durable);
    assert_eq!(f.commit_local(orphan(confirmed())), json!(true));
    assert_eq!(
        f.count("SELECT COUNT(*) FROM attempts WHERE ownership_active=1"),
        0
    );
}

/// Service generations: creation (a second live generation of the same
/// service conflicts), a lease and gate readiness once the generation is
/// ready; a held revision cannot be retired; a stop is first marked
/// stopping and retired only with a confirmed proof and no remaining probe;
/// a stopped generation refuses leases, health transitions and readiness,
/// and a missing one is not found, all durably.
#[test]
fn service_generations_follow_guarded_transitions() {
    let mut f = Fixture::new();
    let id = f.admitted();
    let created = f.commit_local(LocalOp::from(EnsureServiceGeneration {
        action: GenerationAction::Create {
            generation: "g1".into(),
            service: "svc".into(),
            revision: "a".repeat(64),
            definition_json: "{}".into(),
            broker_identity_json: "{}".into(),
            ownership: "managed".into(),
            exec_capability_sha256: None,
            at: 1.0,
        },
    }));
    assert_eq!(created, json!("g1"));
    f.sql(
        "INSERT INTO agent_service_gates(agent_id,state) VALUES(?,'pending')",
        [id.as_str()],
    );
    let gate = |step: GateStep| {
        LocalOp::from(PrepareServiceGate {
            execution: id.clone(),
            step,
            at: 2.0,
        })
    };
    assert_eq!(
        f.commit_local(gate(GateStep::Lease {
            generation: "g1".into()
        })),
        json!(false)
    );
    assert_eq!(
        f.commit_local(gate(GateStep::Ready { expected_leases: 1 })),
        json!(false)
    );
    f.commit_local(LocalOp::from(MarkServiceReady {
        generation: "g1".into(),
        at: 3.0,
    }));
    assert_eq!(
        f.commit_local(gate(GateStep::Ready { expected_leases: 1 })),
        json!(true)
    );
    let retire = f.refuse_local(LocalOp::from(EnsureServiceGeneration {
        action: GenerationAction::Retire {
            generation: "g1".into(),
        },
    }));
    assert!(retire.durable);
    f.sql(
        "INSERT INTO managed_service_probes(id,generation_id,created_at) VALUES('p1','g1',1)",
        [],
    );
    let live_twin = f.refuse_local(LocalOp::from(EnsureServiceGeneration {
        action: GenerationAction::Create {
            generation: "g2".into(),
            service: "svc".into(),
            revision: "b".repeat(64),
            definition_json: "{}".into(),
            broker_identity_json: "{}".into(),
            ownership: "managed".into(),
            exec_capability_sha256: None,
            at: 1.0,
        },
    }));
    assert!(live_twin.durable);
    let stop = |finalize: Option<serde_json::Value>| {
        LocalOp::from(StopServiceGeneration {
            generation: "g1".into(),
            finalize,
            at: 4.0,
        })
    };
    let early = f.refuse_local(stop(Some(json!({"confirmed": true}))));
    assert!(early.durable);
    assert_eq!(f.commit_local(stop(None)), json!("stopping"));
    assert_eq!(
        f.commit_local(stop(Some(json!({"confirmed": true})))),
        json!("unknown")
    );
    assert_eq!(
        f.count("SELECT COUNT(*) FROM managed_service_leases WHERE released_at IS NULL"),
        1
    );
    f.sql("DELETE FROM managed_service_probes", []);
    assert_eq!(
        f.commit_local(stop(Some(json!({"confirmed": true})))),
        json!("stopped")
    );
    assert_eq!(
        f.count("SELECT COUNT(*) FROM managed_service_leases WHERE released_at IS NULL"),
        0
    );
    for refused in [
        gate(GateStep::Lease {
            generation: "g1".into(),
        }),
        LocalOp::from(MarkServiceReady {
            generation: "g1".into(),
            at: 5.0,
        }),
        LocalOp::from(MarkServiceUnhealthy {
            generation: "g1".into(),
            reason: "service_readiness_failed".into(),
            at: 5.0,
        }),
        LocalOp::from(EnsureServiceGeneration {
            action: GenerationAction::Retire {
                generation: "missing".into(),
            },
        }),
    ] {
        assert!(f.refuse_local(refused).durable);
    }
    assert_eq!(
        f.count(
            "SELECT COUNT(*) FROM managed_service_generations WHERE id='g1' AND state='stopped'"
        ),
        1
    );
}

/// A delayed lease step after its gate stopped being pending (ready or
/// failed) or after its execution ended is a durable conflict: no lease is
/// written and the generation's idle mark stays.
#[test]
fn stale_gates_and_ended_executions_refuse_leases() {
    let mut f = Fixture::new();
    let id = f.admitted();
    f.commit_local(LocalOp::from(EnsureServiceGeneration {
        action: GenerationAction::Create {
            generation: "g1".into(),
            service: "svc".into(),
            revision: "a".repeat(64),
            definition_json: "{}".into(),
            broker_identity_json: "{}".into(),
            ownership: "managed".into(),
            exec_capability_sha256: None,
            at: 1.0,
        },
    }));
    f.sql(
        "UPDATE managed_service_generations SET idle_since=1 WHERE id='g1'",
        [],
    );
    let lease = || {
        LocalOp::from(PrepareServiceGate {
            execution: id.clone(),
            step: GateStep::Lease {
                generation: "g1".into(),
            },
            at: 2.0,
        })
    };
    // No gate at all, then a gate that is no longer pending.
    assert!(f.refuse_local(lease()).durable);
    for state in ["ready", "failed"] {
        f.sql(
            "INSERT INTO agent_service_gates(agent_id,state) VALUES(?1,?2) \
             ON CONFLICT(agent_id) DO UPDATE SET state=excluded.state",
            params![id.as_str(), state],
        );
        assert!(f.refuse_local(lease()).durable, "{state}");
    }
    // A pending gate of an execution that already ended.
    f.sql(
        "UPDATE agent_service_gates SET state='pending' WHERE agent_id=?",
        [id.as_str()],
    );
    f.sql(
        "UPDATE agents SET status='failed',finished_at=1 WHERE id=?",
        [id.as_str()],
    );
    assert!(f.refuse_local(lease()).durable);
    assert_eq!(f.count("SELECT COUNT(*) FROM managed_service_leases"), 0);
    assert_eq!(
        f.count("SELECT COUNT(*) FROM managed_service_generations WHERE idle_since IS NOT NULL"),
        1
    );
    // The live, pending gate leases.
    f.sql(
        "UPDATE agents SET status='running',finished_at=NULL WHERE id=?",
        [id.as_str()],
    );
    assert_eq!(f.commit_local(lease()), json!(false));
    assert_eq!(f.count("SELECT COUNT(*) FROM managed_service_leases"), 1);
    // A pending gate of a live execution still cannot lease a generation
    // that is no longer starting or ready.
    f.sql(
        "UPDATE managed_service_generations SET state='stopped',idle_since=7 WHERE id='g1'",
        [],
    );
    assert!(f.refuse_local(lease()).durable);
    assert_eq!(
        f.count("SELECT COUNT(*) FROM managed_service_generations WHERE idle_since=7"),
        1
    );
}

/// Expiry runs in bounded batches whose result stays within the receipt
/// bound, and repeated batches make progress over a large backlog.
#[test]
fn delivery_expiry_is_batched_and_progresses() {
    let mut f = Fixture::new();
    let id = f.admitted();
    f.sql(
        "UPDATE agents SET status='succeeded',finished_at=1 WHERE id=?",
        [id.as_str()],
    );
    f.sql(
        "INSERT INTO orchestrator_sessions(id,transport,external_session_id,created_at,last_seen_at) VALUES('s','codex_queue','thread',1,1)",
        [],
    );
    let store = f.home.store();
    for n in 0..1701 {
        store
            .conn
            .execute(
                "INSERT INTO events(agent_id,at,kind,data_json) VALUES(?,1,'status','{}')",
                [id.as_str()],
            )
            .unwrap();
        store
            .conn
            .execute(
                "INSERT INTO deliveries(id,agent_id,orchestrator_session_id,terminal_event_seq,state) VALUES(?,?,NULL,last_insert_rowid(),'waiting_binding')",
                params![format!("ntf_{n:05}"), id.as_str()],
            )
            .unwrap();
    }
    let first = f.commit_local(LocalOp::from(ExpireUnboundDeliveries { at: 1.0e9 }));
    assert_eq!(first.as_array().unwrap().len(), 1000);
    let second = f.commit_local(LocalOp::from(ExpireUnboundDeliveries { at: 1.0e9 }));
    assert_eq!(second.as_array().unwrap().len(), 701);
    assert_eq!(
        f.count("SELECT COUNT(*) FROM deliveries WHERE state='expired'"),
        1701
    );
}

/// A delivery failure recorded through the writer captures no incident
/// inside the writer transaction (the separate incident row does that).
#[test]
fn writer_delivery_failure_captures_no_inline_incident() {
    let mut f = Fixture::new();
    let id = f.admitted();
    f.sql(
        "INSERT INTO orchestrator_sessions(id,transport,external_session_id,created_at,last_seen_at) VALUES('s','codex_queue','thread',1,1)",
        [],
    );
    f.sql(
        "UPDATE agents SET status='failed',finished_at=5,orchestrator_session_id='s' WHERE id=?",
        [id.as_str()],
    );
    f.sql(
        "INSERT INTO events(agent_id,at,kind,data_json) VALUES(?,5,'execution_failure_v1',?)",
        params![
            id.as_str(),
            json!({"class":"runtime_failed","stage":"streaming"}).to_string()
        ],
    );
    f.sql(
        "INSERT INTO deliveries(id,agent_id,orchestrator_session_id,terminal_event_seq,state,attempts,lease_owner,lease_until) \
         VALUES('ntf_fail',?,'s',(SELECT MAX(seq) FROM events),'sending',1,'owner',1.0e12)",
        [id.as_str()],
    );
    f.commit_local(LocalOp::from(FailDelivery {
        delivery_id: "ntf_fail".into(),
        owner: "owner".into(),
        error: "transport_failed".into(),
        at: 10.0,
        ambiguous: false,
        evidence: None,
    }));
    assert_eq!(
        f.count("SELECT COUNT(*) FROM deliveries WHERE state='failed'"),
        1
    );
    assert_eq!(f.count("SELECT COUNT(*) FROM incident_ledger"), 0);
}

/// A prepared history batch never deletes a candidate whose incident
/// projection is not carried; the reader-prepared batch with its proof lets
/// the writer remove the execution.
#[test]
fn history_batches_defer_candidates_without_projections() {
    let mut f = Fixture::new();
    let id = f.admitted();
    f.sql(
        "UPDATE agents SET status='succeeded',finished_at=1,created_at=1 WHERE id=?",
        [id.as_str()],
    );
    let at = 40.0 * 86_400.0;
    let batch = |projections| {
        LocalOp::from(PruneHistoryBatch {
            at,
            candidates: vec![id.clone()],
            projections,
        })
    };
    let deferred = f.commit_local(batch(vec![]));
    assert_eq!(deferred["deferred"], 1);
    assert_eq!(f.count("SELECT COUNT(*) FROM agents"), 1);
    let prepared = agent_run_store::writer::StoreReader::open(&f.home.path)
        .unwrap()
        .history_batch(at, 10)
        .unwrap();
    assert_eq!(prepared.candidates, std::slice::from_ref(&id));
    let removed = f.commit_local(batch(prepared.projections));
    assert_eq!(removed["deferred"], 0);
    assert!(removed["removed"].as_u64().unwrap() > 0, "{removed}");
}
