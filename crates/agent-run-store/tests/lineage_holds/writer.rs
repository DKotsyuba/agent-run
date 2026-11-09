//! Broker-local hold operations retain ordinary writer atomicity and private proofs.

use super::*;
use agent_run_domain::storage::{
    MonoDeadline, OperationId, ProducerId, ProducerKind, RejectCode, SubmitResult, UnknownCause,
    WriteAuthority, WriteReceipt,
};
use agent_run_store::{
    lineage_holds::LineageHoldRelease,
    ops::{AcquireLineageHold, LocalEnvelope, LocalOp, ReleaseLineageHold},
    writer::{StoreWriter, WriterLease},
};
use std::cell::Cell;

/// One proven synthetic success and a resident writer; no native process runs.
struct Fixture {
    home: common::Home,
    writer: StoreWriter<'static>,
    acquire: LineageHoldAcquire,
    ordinal: Cell<u64>,
}

impl Fixture {
    /// Seeds prerequisites before adopting the single online writer fence.
    fn new() -> Self {
        let home = common::Home::new();
        let mut store = home.store();
        let selected = target(&home, &mut store);
        let acquire = acquire(&store, "writer-hold", vec![selected]);
        drop(store);
        let writer = StoreWriter::open(WriterLease::acquire_online(&home.path).unwrap()).unwrap();
        Self {
            home,
            writer,
            acquire,
            ordinal: Cell::new(0),
        }
    }

    /// Allocates one fresh operation; callers clone its envelope only for exact retries.
    fn envelope(&self, op: impl Into<LocalOp>) -> LocalEnvelope {
        let (boot, now) = agent_run_platform::monotonic::now().unwrap();
        self.ordinal.set(self.ordinal.get() + 1);
        LocalEnvelope::new(
            OperationId::new(&boot, now),
            ProducerId {
                kind: ProducerKind::Broker,
                scope: Some("retention".into()),
                instance: "hold-writer-fixture".into(),
            },
            self.ordinal.get(),
            MonoDeadline {
                boot_id: boot,
                ns: now + 60_000_000_000,
            },
            op.into(),
        )
        .unwrap()
    }

    /// Submits through the real writer and requires its durable receipt.
    fn commit(&mut self, envelope: &LocalEnvelope) -> WriteReceipt {
        match self.writer.apply_local(&WriteAuthority::Broker, envelope) {
            SubmitResult::Committed(receipt) => receipt,
            other => panic!("hold operation did not commit: {other:?}"),
        }
    }

    /// Acquires through the writer and decodes its persisted typed result.
    fn acquire(&mut self) -> LineageHoldAcquireResult {
        let envelope = self.envelope(AcquireLineageHold {
            acquire: self.acquire.clone(),
        });
        self.commit(&envelope).output().unwrap()
    }

    /// Reads an exact count or flag from the disposable fixture database.
    fn scalar(&self, sql: &str) -> i64 {
        self.home
            .store()
            .conn
            .query_row(sql, [], |row| row.get(0))
            .unwrap()
    }

    /// Installs or removes a failure injection on this fixture only.
    fn sql(&self, sql: &str) {
        self.home.store().conn.execute_batch(sql).unwrap();
    }
}

/// Exact writer retries replay one receipt; new envelopes exercise permanent domain replay.
#[test]
fn acquire_writer_receipt_and_domain_replay_are_distinct() {
    let mut fixture = Fixture::new();
    let envelope = fixture.envelope(AcquireLineageHold {
        acquire: fixture.acquire.clone(),
    });
    let first = fixture.commit(&envelope);
    let acquired: LineageHoldAcquireResult = first.output().unwrap();
    assert!(!acquired.replayed);
    let replay = fixture.commit(&envelope);
    assert!(replay.replayed);
    assert_eq!(replay.commit_seq, first.commit_seq);
    assert_eq!(replay.result, first.result);
    let domain_replay = fixture.acquire();
    assert!(domain_replay.replayed);
    assert_eq!(domain_replay.receipt, acquired.receipt);
    assert_eq!(fixture.scalar("SELECT count(*) FROM lineage_holds"), 1);
    assert_eq!(fixture.scalar("SELECT count(*) FROM operation_receipts"), 2);
    assert_eq!(
        fixture.scalar("SELECT count(*) FROM operation_receipts WHERE agent_id IS NOT NULL"),
        0
    );
}

/// A signature is part of replay identity, every fresh release verifies it, and no proof is persisted.
#[test]
fn release_writer_replay_binds_signature_without_persisting_it() {
    let mut fixture = Fixture::new();
    let acquired = fixture.acquire();
    let sig = signature(&acquired);
    let bad_sig = [255; 64];
    let release = |proof| {
        ReleaseLineageHold::new(
            acquired.receipt.hold_id.clone(),
            acquired.receipt.store_id.clone(),
            proof,
        )
    };
    let bad = fixture.envelope(release(bad_sig));
    let SubmitResult::Rejected(refused) = fixture.writer.apply_local(&WriteAuthority::Broker, &bad)
    else {
        panic!("invalid proof was accepted");
    };
    assert_eq!(refused.code, RejectCode::Validation);
    assert!(refused.durable);
    let SubmitResult::Rejected(replayed_refusal) =
        fixture.writer.apply_local(&WriteAuthority::Broker, &bad)
    else {
        panic!("invalid proof retry changed its outcome");
    };
    assert_eq!(replayed_refusal.code, refused.code);
    assert!(replayed_refusal.durable);
    assert_eq!(fixture.scalar("SELECT count(*) FROM operation_receipts"), 2);
    assert_eq!(fixture.scalar("SELECT released FROM lineage_holds"), 0);
    let valid = fixture.envelope(release(sig));
    assert_ne!(valid.header().digest, bad.header().digest);
    let committed = fixture.commit(&valid);
    let released: Option<LineageHoldRelease> = committed.output().unwrap();
    assert!(!released.unwrap().already_released);
    let retry = fixture.commit(&valid);
    assert!(retry.replayed);
    assert_eq!(retry.commit_seq, committed.commit_seq);
    assert_eq!(retry.result, committed.result);

    let header = valid.header();
    let changed = LocalEnvelope::new(
        header.operation_id.clone(),
        header.producer.clone(),
        header.ordinal,
        header.deadline.clone(),
        release(bad_sig).into(),
    )
    .unwrap();
    let SubmitResult::Rejected(changed) = fixture
        .writer
        .apply_local(&WriteAuthority::Broker, &changed)
    else {
        panic!("changed signature replayed a successful receipt");
    };
    assert_eq!(changed.code, RejectCode::ReplayConflict);
    let fresh_bad = fixture.envelope(release(bad_sig));
    let SubmitResult::Rejected(fresh_bad) = fixture
        .writer
        .apply_local(&WriteAuthority::Broker, &fresh_bad)
    else {
        panic!("released hold bypassed proof validation");
    };
    assert_eq!(fresh_bad.code, RejectCode::Validation);
    assert!(fresh_bad.durable);
    let fresh_valid = fixture.envelope(release(sig));
    let again: Option<LineageHoldRelease> = fixture.commit(&fresh_valid).output().unwrap();
    assert!(again.unwrap().already_released);

    let db = fixture.home.store();
    for proof in [sig, bad_sig] {
        let raw_json = serde_json::to_string(proof.as_slice()).unwrap();
        let hex = agent_run_domain::canonical::hex_digest(&proof);
        let debug = format!("{:?}", release(proof));
        assert!(debug.contains("<redacted>"));
        assert!(!debug.contains(&raw_json));
        assert!(!debug.contains(&hex));
        assert_eq!(
            serde_json::to_value(release(proof)).unwrap()["signature"],
            json!(proof.as_slice()),
            "proof must remain in the transient digest input"
        );
        for sql in [
            "SELECT result_json FROM operation_receipts",
            "SELECT data_json FROM events",
            "SELECT payload_json FROM commands",
        ] {
            for value in db
                .conn
                .prepare(sql)
                .unwrap()
                .query_map([], |row| row.get::<_, String>(0))
                .unwrap()
            {
                let value = value.unwrap();
                assert!(!value.contains(&raw_json));
                assert!(!value.contains(&hex));
                assert!(!value.contains("\"signature\""));
            }
        }
    }
    assert_eq!(
        fixture.scalar("SELECT count(*) FROM operation_receipts WHERE outcome='rejected'"),
        2
    );
}

/// Receipt insertion failure rolls back the preceding acquisition or release in the same transaction.
#[test]
fn hold_mutation_and_writer_receipt_rollback_together() {
    let mut fixture = Fixture::new();
    fixture.sql(
        "CREATE TRIGGER refuse_hold_receipt BEFORE INSERT ON operation_receipts
        WHEN NEW.op_kind IN ('acquire_lineage_hold','release_lineage_hold')
        BEGIN SELECT RAISE(ABORT,'fixture receipt rejection'); END;",
    );
    let acquire = fixture.envelope(AcquireLineageHold {
        acquire: fixture.acquire.clone(),
    });
    let SubmitResult::Rejected(refused) = fixture
        .writer
        .apply_local(&WriteAuthority::Broker, &acquire)
    else {
        panic!("failed receipt reported a committed hold");
    };
    assert!(!refused.durable);
    assert_eq!(fixture.scalar("SELECT count(*) FROM lineage_holds"), 0);
    assert_eq!(
        fixture.scalar("SELECT count(*) FROM lineage_hold_targets"),
        0
    );
    assert_eq!(fixture.scalar("SELECT count(*) FROM operation_receipts"), 0);
    fixture.sql("DROP TRIGGER refuse_hold_receipt");
    let acquired: LineageHoldAcquireResult = fixture.commit(&acquire).output().unwrap();
    fixture.sql(
        "CREATE TRIGGER refuse_hold_receipt BEFORE INSERT ON operation_receipts
        WHEN NEW.op_kind='release_lineage_hold'
        BEGIN SELECT RAISE(ABORT,'fixture receipt rejection'); END;",
    );
    let release = fixture.envelope(ReleaseLineageHold::new(
        acquired.receipt.hold_id.clone(),
        acquired.receipt.store_id.clone(),
        signature(&acquired),
    ));
    let SubmitResult::Rejected(refused) = fixture
        .writer
        .apply_local(&WriteAuthority::Broker, &release)
    else {
        panic!("failed receipt reported a committed release");
    };
    assert!(!refused.durable);
    assert_eq!(fixture.scalar("SELECT released FROM lineage_holds"), 0);
    assert_eq!(fixture.scalar("SELECT count(*) FROM operation_receipts"), 1);
    fixture.sql("DROP TRIGGER refuse_hold_receipt");
    fixture.commit(&release);
    assert_eq!(fixture.scalar("SELECT released FROM lineage_holds"), 1);
}

/// A deferred constraint failure at COMMIT preserves the writer's conservative unknown outcome.
#[test]
fn hold_commit_failure_stays_unknown_without_a_durable_receipt() {
    let mut fixture = Fixture::new();
    fixture.sql("CREATE TABLE hold_fixture_parent(id INTEGER PRIMARY KEY);
        CREATE TABLE hold_fixture_pending(id INTEGER REFERENCES hold_fixture_parent(id) DEFERRABLE INITIALLY DEFERRED);
        CREATE TRIGGER fail_hold_commit AFTER INSERT ON lineage_holds
        BEGIN INSERT INTO hold_fixture_pending VALUES(1); END;");
    let envelope = fixture.envelope(AcquireLineageHold {
        acquire: fixture.acquire.clone(),
    });
    assert!(matches!(
        fixture
            .writer
            .apply_local(&WriteAuthority::Broker, &envelope),
        SubmitResult::Unknown(UnknownCause::CommitUncertain)
    ));
    assert_eq!(fixture.scalar("SELECT count(*) FROM operation_receipts"), 0);
    assert_eq!(fixture.scalar("SELECT count(*) FROM lineage_holds"), 0);
    assert_eq!(
        fixture.scalar("SELECT count(*) FROM lineage_hold_targets"),
        0
    );
    assert!(
        fixture
            .writer
            .lookup_digest(
                &envelope.header().operation_id,
                &WriteAuthority::Broker,
                &envelope.header().digest,
            )
            .unwrap()
            .is_none()
    );
}

/// Store scope remains a typed domain refusal and never exposes the proof in its durable error.
#[test]
fn release_store_mismatch_is_typed_and_does_not_release() {
    let mut fixture = Fixture::new();
    let acquired = fixture.acquire();
    let envelope = fixture.envelope(ReleaseLineageHold::new(
        acquired.receipt.hold_id.clone(),
        "store_00000000000000000000000000000001".parse().unwrap(),
        signature(&acquired),
    ));
    let SubmitResult::Rejected(refused) = fixture
        .writer
        .apply_local(&WriteAuthority::Broker, &envelope)
    else {
        panic!("foreign store accepted release");
    };
    assert_eq!(refused.code, RejectCode::StoreIdentityMismatch);
    assert!(refused.durable);
    assert_eq!(fixture.scalar("SELECT released FROM lineage_holds"), 0);
}

/// The largest valid hold fits both the local input budget and the unchanged writer receipt bound.
#[test]
fn maximum_hold_fits_local_payload_and_receipt_bounds() {
    let mut fixture = Fixture::new();
    let mut store = fixture.home.store();
    while fixture.acquire.targets.len() < 20 {
        fixture
            .acquire
            .targets
            .push(target(&fixture.home, &mut store));
    }
    fixture.acquire.request_id = "😀".repeat(512);
    drop(store);
    let envelope = fixture.envelope(AcquireLineageHold {
        acquire: fixture.acquire.clone(),
    });
    let receipt = fixture.commit(&envelope);
    let result: LineageHoldAcquireResult = receipt.output().unwrap();
    assert_eq!(result.receipt.targets, fixture.acquire.targets);
    assert!(
        agent_run_domain::canonical::dumps(&receipt.result, true).len()
            <= agent_run_domain::storage::MAX_RESULT_BYTES
    );
    let release = fixture.envelope(ReleaseLineageHold::new(
        result.receipt.hold_id.clone(),
        result.receipt.store_id.clone(),
        signature(&result),
    ));
    let released: Option<LineageHoldRelease> = fixture.commit(&release).output().unwrap();
    assert_eq!(released.unwrap().state, LineageHoldState::Released);
}
