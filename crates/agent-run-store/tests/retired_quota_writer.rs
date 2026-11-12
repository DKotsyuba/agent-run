//! Historical quota requests can replay stored outcomes but cannot mutate the store.
use crate::common;

use agent_run_domain::storage::{
    MonoDeadline, OperationId, ProducerId, ProducerKind, RejectCode, RejectStage, StorageRejection,
    SubmitResult, WriteAuthority,
};
use agent_run_store::{
    ops::{OpKindRef, RemoteOp, WriteEnvelope},
    writer::{StoreWriter, WriterLease},
};
use rusqlite::params;
use serde_json::{Value, json};

/// An authority that could submit the historical native exhaustion request.
fn supervisor() -> WriteAuthority {
    let execution: agent_run_domain::domain::AgentId =
        "ag-20261011-000000-0000000001".parse().unwrap();
    WriteAuthority::Supervisor {
        logical: execution.clone(),
        execution,
        attempt: Some("a".into()),
        capability_sha256: "a".repeat(64),
        pid: 2,
        birth: 3.0,
        token: "supervisor".into(),
    }
}

/// The old payloads preserve wire identity, refuse new writes and replay only
/// a receipt whose payload, producer, ordinal and authority all match.
#[test]
fn retired_operations_only_replay_identical_receipts() {
    let home = common::Home::new();
    let mut writer = StoreWriter::open(WriterLease::acquire_online(&home.path).unwrap()).unwrap();
    let db = home.store();
    let (boot, now) = agent_run_platform::monotonic::now().unwrap();
    let operations: [(u16, Value); 4] = [
        (
            31,
            json!({"kind":"latch_native_exhaustion","op":{
                "execution":"ag-20261011-000000-0000000001", "account":"acct-a",
                "runtime":"r", "lane":"primary", "window":"5h", "source":"native",
                "models":["m"], "observed_at":1.0, "reset_at":null
            }}),
        ),
        (
            76,
            json!({"kind":"persist_capacity_samples","op":{
                "runtime":"r", "scope_id":"s", "observed_at":1.0, "valid_until":2.0,
                "topology_json":"{}", "samples":[{"runtime":"r", "lane":"primary",
                    "window":"5h", "target":null, "source":"native", "remaining_percent":50.0,
                    "reset_at":null, "observed_at":1.0, "valid_until":2.0}], "retention":10
            }}),
        ),
        (
            77,
            json!({"kind":"prune_capacity_samples","op":{"retention":10}}),
        ),
        (
            78,
            json!({"kind":"record_quota_snapshot","op":{
                "runtime":"r", "snapshot":{"account":"acct-a", "models":[{"model":"m",
                    "pools":[{"key":"acct-a::primary", "windows":[{"source":"native",
                        "name":"5h", "remaining_percent":50.0, "reset_at":null,
                        "observed_at":1.0, "valid_until":2.0}]}]}]}, "retention":10, "at":1.0
            }}),
        ),
    ];
    for (index, (id, body)) in operations.into_iter().enumerate() {
        let authority = if id == 31 {
            supervisor()
        } else {
            WriteAuthority::Broker
        };
        let producer = ProducerId {
            kind: if id == 31 {
                ProducerKind::Supervisor
            } else {
                ProducerKind::Broker
            },
            scope: Some("test".into()),
            instance: "old-writer".into(),
        };
        let op: RemoteOp = serde_json::from_value(body.clone()).unwrap();
        assert_eq!(serde_json::to_value(&op).unwrap(), body);
        let envelope = WriteEnvelope::new(
            OperationId::new(&boot, now + index as u64),
            producer.clone(),
            1,
            MonoDeadline {
                boot_id: boot.clone(),
                ns: now + 1_000_000_000,
            },
            op.clone(),
        )
        .unwrap();
        let kind = OpKindRef::from_wire_id(id).unwrap();
        assert!(kind.retired());
        assert!(!agent_run_store::ops::implemented(kind));
        let (wire_id, wire) = envelope.to_wire();
        let decoded = WriteEnvelope::from_wire(wire_id, wire).unwrap();
        assert_eq!(decoded.header().digest, envelope.header().digest);
        for result in [
            writer.apply(&authority, &decoded),
            writer.apply_owned(&authority, decoded),
        ] {
            assert!(matches!(result, SubmitResult::Rejected(refusal)
                if refusal.code == RejectCode::Unsupported && !refusal.durable));
        }

        let mut old_refusal = StorageRejection::new(
            RejectCode::Validation,
            RejectStage::Statement,
            "old refusal",
        );
        old_refusal.durable = true;
        let (outcome, result_json) = if id == 77 {
            ("rejected", serde_json::to_string(&old_refusal).unwrap())
        } else {
            ("committed", "3".to_owned())
        };
        db.conn.execute(
            "INSERT INTO operation_receipts(operation_id,digest,op_kind,producer_key,ordinal,authority_kind,authority_fingerprint,agent_id,outcome,result_json,writer_epoch,committed_at) \
             VALUES(?,?,?,?,?,?,?,NULL,?,?,1,1.0)",
            params![envelope.header().operation_id.as_str(), envelope.header().digest.as_str(),
                kind.name(), producer.key(), 1, authority.kind(), authority.fingerprint(), outcome, result_json],
        ).unwrap();
        let replayed = writer.apply(&authority, &envelope);
        match &replayed {
            SubmitResult::Committed(receipt) if id != 77 => {
                assert!(receipt.replayed);
                assert_eq!(receipt.op_kind, kind.name());
                assert_eq!(receipt.result, json!(3));
            }
            SubmitResult::Rejected(refusal) if id == 77 => assert_eq!(*refusal, old_refusal),
            other => panic!("unexpected replay for {id}: {other:?}"),
        }
        assert_eq!(
            writer
                .lookup(&envelope.header().operation_id, &authority)
                .unwrap(),
            Some(replayed)
        );

        let mut different = producer.clone();
        different.instance = "different".into();
        let conflicting = WriteEnvelope::new(
            envelope.header().operation_id.clone(),
            different,
            1,
            envelope.header().deadline.clone(),
            op,
        )
        .unwrap();
        assert!(matches!(writer.apply(&authority, &conflicting),
            SubmitResult::Rejected(refusal) if refusal.code == RejectCode::ReplayConflict));
    }
    for table in [
        "capacity_samples",
        "capacity_route_snapshots",
        "quota_exhaustion",
    ] {
        let rows: i64 = db
            .conn
            .query_row(&format!("SELECT COUNT(*) FROM {table}"), [], |row| {
                row.get(0)
            })
            .unwrap();
        assert_eq!(rows, 0, "{table}");
    }
}
