//! Boundary checks of the sole-writer skeleton on disposable private homes:
//! the closed inventory and authority matrix, envelope wire integrity, the
//! writer lease, read-only handles and schema fences. Unimplemented writer
//! paths must answer typed `Unsupported`, never a committed receipt.
mod common;

use agent_run_domain::storage::{
    MonoDeadline, OperationId, ProducerId, ProducerKind, RejectCode, SubmitResult, WriteAuthority,
    WriteClass,
};
use agent_run_store::{
    ops::{ClaimCommand, EnqueueCommand, OpKindRef, RemoteOp, WriteEnvelope},
    writer::{StoreReader, StoreWriter, WriterLease},
};
use serde_json::json;
use std::collections::BTreeSet;

/// A fixed producer used by the envelope tests.
fn producer() -> ProducerId {
    ProducerId {
        kind: ProducerKind::Supervisor,
        scope: Some("ag-20261011-000000-0000000001".into()),
        instance: "instance-1".into(),
    }
}

/// A deadline of a fixed test boot.
fn deadline() -> MonoDeadline {
    MonoDeadline {
        boot_id: "boot-test".into(),
        ns: u64::MAX / 2,
    }
}

/// A cancel command envelope for one execution.
fn cancel_envelope(kind: &str) -> WriteEnvelope {
    WriteEnvelope::new(
        OperationId::new("boot-test", 1),
        producer(),
        1,
        deadline(),
        RemoteOp::from(EnqueueCommand {
            execution: "ag-20261011-000000-0000000001".parse().unwrap(),
            kind: kind.into(),
            payload: json!({"text": "x"}),
        }),
    )
    .unwrap()
}

/// The inventory has exactly rows 1..=92 with stable, unique wire ids and
/// unique snake-case names, and rows 6, 49 and 79 are internal-only.
#[test]
fn inventory_is_closed_with_stable_unique_ids() {
    let all = OpKindRef::all();
    assert_eq!(all.len(), 92);
    for (index, kind) in all.iter().enumerate() {
        assert_eq!(kind.wire_id() as usize, index + 1);
        assert_eq!(OpKindRef::from_wire_id(kind.wire_id()), Some(*kind));
    }
    assert_eq!(OpKindRef::from_wire_id(0), None);
    assert_eq!(OpKindRef::from_wire_id(93), None);
    let names: BTreeSet<_> = all.iter().map(|kind| kind.name()).collect();
    assert_eq!(names.len(), 92);
    assert!(
        names
            .iter()
            .all(|name| name.chars().all(|c| c.is_ascii_lowercase() || c == '_'))
    );
    let internal: Vec<_> = all
        .iter()
        .filter(|kind| kind.internal_only())
        .map(|kind| kind.wire_id())
        .collect();
    assert_eq!(internal, vec![6, 49, 79]);
    assert_eq!(all.iter().filter(|kind| kind.is_remote()).count(), 49);
}

/// The static matrix confines worker rows to Worker, broker rows to Broker
/// and keeps peers away from supervisor-only and broker-only rows.
#[test]
fn authority_matrix_confines_roles() {
    let cli = WriteAuthority::Cli {
        uid: 1,
        pid: 2,
        birth: 3.0,
        token: "t".into(),
    };
    let worker = WriteAuthority::Worker {
        execution: "ag-20261011-000000-0000000001".parse().unwrap(),
        attempt: "a".into(),
    };
    assert!(OpKindRef::AcceptWorkerFinish.permits(&worker));
    assert!(!OpKindRef::AcceptWorkerFinish.permits(&WriteAuthority::Broker));
    assert!(!OpKindRef::FinishTerminal.permits(&cli));
    assert!(!OpKindRef::FinishTerminal.permits(&worker));
    assert!(OpKindRef::FinishTerminal.permits(&WriteAuthority::Broker));
    assert!(OpKindRef::EnqueueCommand.permits(&cli));
    assert!(!OpKindRef::AdmitStart.permits(&cli));
    assert!(OpKindRef::ClaimDelivery.permits(&cli));
    assert!(OpKindRef::CompleteDelivery.permits(&cli));
    assert!(OpKindRef::PrepareBridgeDelivery.permits(&cli));
    assert!(!OpKindRef::PruneCapacitySamples.permits(&cli));
    let collector = WriteAuthority::Collector {
        uid: 1,
        pid: 2,
        birth: 3.0,
        token: "t".into(),
    };
    let supervisor = WriteAuthority::Supervisor {
        logical: "ag-20261011-000000-0000000001".parse().unwrap(),
        execution: "ag-20261011-000000-0000000001".parse().unwrap(),
        attempt: Some("a".into()),
        capability_sha256: "a".repeat(64),
        pid: 2,
        birth: 3.0,
        token: "t".into(),
    };
    for role in [&worker, &collector, &supervisor] {
        assert!(!OpKindRef::ClaimDelivery.permits(role));
        assert!(!OpKindRef::CompleteDelivery.permits(role));
        assert!(!OpKindRef::PrepareBridgeDelivery.permits(role));
    }
    assert!(OpKindRef::PruneCapacitySamples.permits(&collector));
    assert!(!OpKindRef::ClaimCommand.permits(&WriteAuthority::Broker));
}

/// A body round-trips through the wire with the same digest; tampering,
/// header/body kind mismatch and local kinds are refused at the gate; the
/// class of row 33 follows the verified payload.
#[test]
fn envelope_wire_integrity_and_verified_class() {
    let envelope = cancel_envelope("cancel");
    assert_eq!(envelope.class(), WriteClass::Critical);
    assert_eq!(cancel_envelope("steer").class(), WriteClass::Normal);
    let (kind, body) = envelope.to_wire();
    let back = WriteEnvelope::from_wire(kind, body).unwrap();
    assert_eq!(back.header(), envelope.header());
    assert!(matches!(
        back.decode().unwrap(),
        RemoteOp::EnqueueCommand(_)
    ));
    let tampered = String::from_utf8(body.to_vec())
        .unwrap()
        .replace("\"x\"", "\"y\"");
    assert_eq!(
        WriteEnvelope::from_wire(kind, tampered.as_bytes())
            .unwrap_err()
            .code,
        RejectCode::DigestMismatch
    );
    assert_eq!(
        WriteEnvelope::from_wire(OpKindRef::ClaimCommand.wire_id(), body)
            .unwrap_err()
            .code,
        RejectCode::ProtocolUnsupported
    );
    assert_eq!(
        WriteEnvelope::from_wire(OpKindRef::AdmitStart.wire_id(), body)
            .unwrap_err()
            .code,
        RejectCode::ProtocolUnsupported
    );
    let oversized = vec![b' '; OpKindRef::ClaimCommand.wire_max() + 1];
    assert_eq!(
        WriteEnvelope::from_wire(OpKindRef::ClaimCommand.wire_id(), &oversized)
            .unwrap_err()
            .code,
        RejectCode::PayloadTooLarge
    );
    let _ = ClaimCommand {
        execution: "ag-20261011-000000-0000000001".parse().unwrap(),
        allow_native_wake: true,
    };
}

/// Floating statistics retain every IEEE754 bit and their canonical digest across typed wire decoding; tampering still refuses.
#[test]
fn statistics_float_payload_round_trips_exact_bits_and_canonical_digest() {
    use agent_run_store::ops::{PersistRunStats, RunStatsRow};
    let base = RunStatsRow {
        agent_id: "ag-20261011-000000-0000000001".parse().unwrap(),
        runtime: "codex-user".into(),
        model: "fixture".into(),
        profile: "review".into(),
        status: "succeeded".into(),
        failure_kind: None,
        started_at: Some(f64::from_bits(4_745_301_996_419_366_542)),
        finished_at: Some(f64::from_bits(4_745_301_996_421_158_701)),
        duration_seconds: Some(f64::from_bits(4_601_368_884_212_137_984)),
        input_tokens: Some(100.0),
        output_tokens: Some(20.0),
        cache_read_tokens: Some(10.0),
        cache_write_tokens: Some(-0.0),
        reasoning_tokens: Some(5.0),
        total_tokens: Some(120.0),
        num_turns: Some(1.0),
        ttft_ms: Some(0.10000000000000002),
        api_duration_ms: Some(1.0000000000000002),
        cost_usd: Some(1e-12),
        usage_source: "token_usage_updated".into(),
        recorded_at: 1_791_750_000.0,
    };
    let mut rows = vec![base.clone()];
    for value in [
        1.0000000000000002,
        0.10000000000000002,
        -0.0,
        1e-12,
        1e20,
        f64::MIN_POSITIVE,
        f64::from_bits(1),
    ] {
        let mut row = base.clone();
        row.started_at = Some(value);
        row.finished_at = Some(value);
        row.duration_seconds = Some(value);
        row.input_tokens = Some(value);
        row.output_tokens = Some(value);
        row.cache_read_tokens = Some(value);
        row.cache_write_tokens = Some(value);
        row.reasoning_tokens = Some(value);
        row.total_tokens = Some(value);
        row.num_turns = Some(value);
        row.ttft_ms = Some(value);
        row.api_duration_ms = Some(value);
        row.cost_usd = Some(value);
        row.recorded_at = value;
        rows.push(row);
    }
    for (index, row) in rows.into_iter().enumerate() {
        let envelope = WriteEnvelope::new(
            OperationId::new("boot-test", index as u64 + 1),
            producer(),
            index as u64 + 1,
            deadline(),
            RemoteOp::from(PersistRunStats { row: row.clone() }),
        )
        .unwrap();
        let (kind, wire) = envelope.to_wire();
        let decoded = WriteEnvelope::from_wire(kind, wire)
            .expect("unchanged typed floating payload verifies its original digest");
        assert_eq!(decoded.header().digest, envelope.header().digest);
        assert_eq!(
            decoded.to_wire().1,
            wire,
            "canonical wire bytes remain identical"
        );
        let RemoteOp::PersistRunStats(actual) = decoded.decode().unwrap() else {
            panic!("wrong typed statistics operation");
        };
        let expected_values = [
            row.started_at,
            row.finished_at,
            row.duration_seconds,
            row.input_tokens,
            row.output_tokens,
            row.cache_read_tokens,
            row.cache_write_tokens,
            row.reasoning_tokens,
            row.total_tokens,
            row.num_turns,
            row.ttft_ms,
            row.api_duration_ms,
            row.cost_usd,
            Some(row.recorded_at),
        ];
        let actual_values = [
            actual.row.started_at,
            actual.row.finished_at,
            actual.row.duration_seconds,
            actual.row.input_tokens,
            actual.row.output_tokens,
            actual.row.cache_read_tokens,
            actual.row.cache_write_tokens,
            actual.row.reasoning_tokens,
            actual.row.total_tokens,
            actual.row.num_turns,
            actual.row.ttft_ms,
            actual.row.api_duration_ms,
            actual.row.cost_usd,
            Some(actual.row.recorded_at),
        ];
        assert_eq!(
            actual_values.map(|value| value.map(f64::to_bits)),
            expected_values.map(|value| value.map(f64::to_bits))
        );
        let tampered = String::from_utf8(wire.to_vec())
            .unwrap()
            .replace("\"model\":\"fixture\"", "\"model\":\"changed\"");
        assert_ne!(
            tampered.as_bytes(),
            wire,
            "control actually changes the payload"
        );
        assert_eq!(
            WriteEnvelope::from_wire(kind, tampered.as_bytes())
                .unwrap_err()
                .code,
            RejectCode::DigestMismatch
        );
    }
}

/// The online lease is exclusive; the writer opens only an existing
/// schema-30 store and an internal-only row submitted alone is a typed
/// refusal.
#[test]
fn lease_is_exclusive_and_writer_refuses_unimplemented_paths() {
    let home = common::Home::new();
    let fence = WriterLease::acquire_online(&home.path).unwrap();
    assert!(matches!(
        WriterLease::acquire_online(&home.path),
        Err(agent_run_domain::Error::Conflict)
    ));
    let mut writer = StoreWriter::open(fence).unwrap();
    assert_eq!(writer.epoch(), 1);
    let internal = agent_run_store::ops::LocalEnvelope::new(
        OperationId::new("boot-test", 1),
        producer(),
        1,
        deadline(),
        agent_run_store::ops::LocalOp::from(agent_run_store::ops::AcknowledgeEnrollment {
            root: "ag-20261011-000000-0000000001".parse().unwrap(),
            seq: 1,
        }),
    )
    .unwrap();
    match writer.apply_local(&WriteAuthority::Broker, &internal) {
        SubmitResult::Rejected(refusal) => assert_eq!(refusal.code, RejectCode::Unsupported),
        other => panic!("unexpected {other:?}"),
    }
    drop(writer);
    let again = StoreWriter::open(WriterLease::acquire_online(&home.path).unwrap()).unwrap();
    assert_eq!(again.epoch(), 2);
    let empty = tempfile::tempdir().unwrap();
    let fence = WriterLease::acquire_online(empty.path()).unwrap();
    assert!(matches!(
        StoreWriter::open(fence),
        Err(agent_run_domain::Error::Unsupported(_))
    ));
    assert!(!empty.path().join("state.db").exists());
}

/// Readers never create or migrate: a missing store and an older schema are
/// typed refusals that leave the file untouched, and writes through a reader
/// fail.
#[test]
fn reader_never_creates_or_migrates_and_cannot_write() {
    let empty = tempfile::tempdir().unwrap();
    assert!(StoreReader::open(empty.path()).is_err());
    assert!(!empty.path().join("state.db").exists());
    let old = tempfile::tempdir().unwrap();
    let fixture = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../tests/fixtures/baseline/db/current-v29.sqlite");
    std::fs::copy(&fixture, old.path().join("state.db")).unwrap();
    let before = std::fs::read(old.path().join("state.db")).unwrap();
    assert!(matches!(
        StoreReader::open(old.path()),
        Err(agent_run_domain::Error::Unsupported(_))
    ));
    assert_eq!(std::fs::read(old.path().join("state.db")).unwrap(), before);
    let home = common::Home::new();
    let reader = StoreReader::open(&home.path).unwrap();
    assert_eq!(reader.commit_seq().unwrap(), 0);
    assert!(reader.conn().execute("DELETE FROM agents", []).is_err());
}
