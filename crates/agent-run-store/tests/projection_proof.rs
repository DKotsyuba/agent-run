//! Retention source proof (R8/R9) on disposable private homes: the reader
//! prepares history batches with a versioned projection proof from one
//! snapshot, the writer rechecks every carried projection against its source
//! (row 85 defers, row 91 is a conflict), refuses malformed proofs before
//! mutation, keeps required evidence from ledger pruning in the same
//! transaction and drains large histories over fresh batches; migration 030
//! rebuilds a populated ledger exactly and only the proof row refreshes.
mod common;

use agent_run_domain::{
    domain::AgentId,
    storage::{
        MonoDeadline, OperationId, ProducerId, ProducerKind, RejectCode, SubmitResult,
        WriteAuthority,
    },
};
use agent_run_store::{
    migrations,
    ops::{CaptureIncident, IncidentProjection, LocalEnvelope, LocalOp, PruneHistoryBatch},
    writer::{StoreReader, StoreWriter, WriterLease},
};
use rusqlite::{Connection, params};
use serde_json::{Value, json};
use std::{cell::Cell, path::Path};

/// Batch time: forty days after the fixture agents finished.
const AT: f64 = 40.0 * 86_400.0;

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

    /// Applies a local `op` as the broker.
    fn apply(&mut self, op: LocalOp) -> SubmitResult {
        let (boot, now) = agent_run_platform::monotonic::now().unwrap();
        self.ordinal.set(self.ordinal.get() + 1);
        let envelope = LocalEnvelope::new(
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
            op,
        )
        .unwrap();
        self.writer.apply_local(&WriteAuthority::Broker, &envelope)
    }

    /// Applies one history batch and returns `(removed, deferred)`.
    fn prune(&mut self, batch: PruneHistoryBatch) -> (u64, u64) {
        match self.apply(LocalOp::from(batch)) {
            SubmitResult::Committed(receipt) => (
                receipt.result["removed"].as_u64().unwrap(),
                receipt.result["deferred"].as_u64().unwrap(),
            ),
            other => panic!("not committed: {other:?}"),
        }
    }

    /// The refusal code of a history batch that must not commit.
    fn refused(&mut self, batch: PruneHistoryBatch) -> RejectCode {
        match self.apply(LocalOp::from(batch)) {
            SubmitResult::Rejected(refusal) => refusal.code,
            other => panic!("not refused: {other:?}"),
        }
    }

    /// A reader-prepared batch at `at`.
    fn prepare(&self, at: f64) -> PruneHistoryBatch {
        StoreReader::open(&self.home.path)
            .unwrap()
            .history_batch(at, 2000)
            .unwrap()
    }

    /// A terminal agent with `status` finished at time 1.
    fn terminal(&self, status: &str) -> AgentId {
        let (id, _) = self
            .home
            .store()
            .admit(&self.home.request(), &self.home.config, &json!({}), None)
            .unwrap();
        self.sql(
            "UPDATE agents SET status=?2,finished_at=1,created_at=1,exit_code=1 WHERE id=?1",
            params![id.as_str(), status],
        );
        id
    }

    /// Appends one event of `kind` with `data` to `id`.
    fn event(&self, id: &AgentId, kind: &str, data: Value) {
        self.sql(
            "INSERT INTO events(agent_id,at,kind,data_json) VALUES(?,2,?,?)",
            params![id.as_str(), kind, data.to_string()],
        );
    }

    /// Runs one SQL statement on a raw store connection.
    fn sql(&self, statement: &str, values: impl rusqlite::Params) {
        self.home.store().conn.execute(statement, values).unwrap();
    }

    /// Counts rows of one query with one text parameter.
    fn count(&self, query: &str, value: &str) -> i64 {
        self.home
            .store()
            .conn
            .query_row(query, [value], |row| row.get(0))
            .unwrap()
    }

    /// Stored ledger phases of `id`.
    fn phases(&self, id: &AgentId) -> Vec<String> {
        let store = self.home.store();
        let mut statement = store
            .conn
            .prepare("SELECT phase FROM incident_ledger WHERE execution_id=? ORDER BY phase")
            .unwrap();
        statement
            .query_map([id.as_str()], |row| row.get(0))
            .unwrap()
            .collect::<rusqlite::Result<_>>()
            .unwrap()
    }

    /// Stored proof details of `id`.
    fn proof(&self, id: &AgentId) -> Value {
        let raw: String = self
            .home
            .store()
            .conn
            .query_row(
                "SELECT details_json FROM incident_ledger WHERE execution_id=? AND phase='projection_proof'",
                [id.as_str()],
                |row| row.get(0),
            )
            .unwrap();
        serde_json::from_str(&raw).unwrap()
    }
}

/// One source change applied after a batch was prepared.
type Drift = fn(&Fixture, &AgentId);

/// One corruption of a prepared batch (with an unrelated execution).
type Corruption = fn(&mut PruneHistoryBatch, &AgentId);

/// The proof phase of a projection.
fn proof_of(projection: &mut IncidentProjection) -> &mut Value {
    &mut projection
        .phases
        .iter_mut()
        .find(|phase| phase.phase == "projection_proof")
        .unwrap()
        .details
}

/// An ordinary success has an explicit empty proof (no failure phase) and
/// the reader-prepared batch removes it, keeping only that proof.
#[test]
fn empty_success_carries_an_empty_proof() {
    let mut f = Fixture::new();
    let id = f.terminal("succeeded");
    let batch = f.prepare(AT);
    assert_eq!(batch.candidates, std::slice::from_ref(&id));
    let projection = &batch.projections[0];
    assert_eq!(projection.phases.len(), 1);
    assert_eq!(projection.phases[0].phase, "projection_proof");
    assert_eq!(projection.phases[0].occurred_at, AT);
    assert_eq!(projection.phases[0].details["version"], 2);
    assert_eq!(projection.phases[0].details["evidence"], json!({}));
    // The observation time is not part of the source fingerprint.
    assert_eq!(
        f.prepare(AT + 5.0).projections[0].phases[0].details["fingerprint"],
        projection.phases[0].details["fingerprint"]
    );
    let (removed, deferred) = f.prune(batch);
    assert!(removed > 0);
    assert_eq!(deferred, 0);
    assert_eq!(f.phases(&id), ["projection_proof"]);
}

/// Executions outside the prepared candidates are untouched even when
/// eligible, and a candidate whose evidence merge would exceed the bound is
/// carried without a projection and deferred, never truncated.
#[test]
fn only_prepared_candidates_with_bounded_proofs_are_removed() {
    let mut f = Fixture::new();
    let kept = f.terminal("succeeded");
    let removed = f.terminal("succeeded");
    let mut batch = f.prepare(AT);
    batch.candidates.retain(|id| *id == removed);
    batch.projections.retain(|p| p.execution == removed);
    // The other eligible execution is not even considered (nor deferred).
    assert_eq!(f.prune(batch).1, 0);
    assert_eq!(
        f.count("SELECT COUNT(*) FROM agents WHERE id=?", kept.as_str()),
        1
    );
    assert_eq!(
        f.count("SELECT COUNT(*) FROM agents WHERE id=?", removed.as_str()),
        0
    );
    let evidence = json!({"version":1,"pad":"p".repeat(3900)});
    let proof = json!({"version":2,"fingerprint":"0".repeat(64),"evidence":{"cleanup":evidence}});
    f.sql(
        "INSERT INTO incident_ledger VALUES(?,'projection_proof',1,?)",
        params![kept.as_str(), proof.to_string()],
    );
    f.sql(
        "UPDATE agents SET status='failed' WHERE id=?",
        [kept.as_str()],
    );
    let overflow = f.prepare(AT);
    assert_eq!(overflow.candidates, std::slice::from_ref(&kept));
    assert!(overflow.projections.is_empty());
    assert_eq!(f.prune(overflow), (0, 1));
    assert_eq!(f.proof(&kept), proof);
}

/// Any source drift after preparation (status, finish, exit, a first
/// failure, a newer cleanup, a delivery change without any new event)
/// defers the stale candidate; a freshly prepared batch removes it.
#[test]
fn source_drift_defers_until_a_fresh_batch() {
    let drifts: [(&str, Drift); 6] = [
        ("status", |f, id| {
            f.sql("UPDATE agents SET status='lost' WHERE id=?", [id.as_str()])
        }),
        ("finish", |f, id| {
            f.sql("UPDATE agents SET finished_at=2 WHERE id=?", [id.as_str()])
        }),
        ("exit", |f, id| {
            f.sql("UPDATE agents SET exit_code=9 WHERE id=?", [id.as_str()])
        }),
        ("failure", |f, id| {
            f.event(id, "execution_failure_v1", json!({"stage":"initial"}))
        }),
        ("cleanup", |f, id| {
            f.event(id, "process_cleanup", json!({"confirmed":false}))
        }),
        ("delivery", |f, id| {
            f.sql(
                "UPDATE deliveries SET attempts=attempts+1 WHERE agent_id=?",
                [id.as_str()],
            )
        }),
    ];
    // A success has no diagnostic phase: only its fingerprint can drift.
    for (status, (name, drift)) in ["failed", "succeeded"]
        .into_iter()
        .flat_map(|status| drifts.map(|drift| (status, drift)))
    {
        let mut f = Fixture::new();
        let id = f.terminal(status);
        f.event(&id, "process_cleanup", json!({"confirmed":true}));
        f.event(&id, "terminal", json!({}));
        f.sql(
            "INSERT INTO deliveries(id,agent_id,terminal_event_seq,state,attempts) \
             SELECT 'd1',agent_id,MAX(seq),'failed',1 FROM events WHERE agent_id=?",
            [id.as_str()],
        );
        let stale = f.prepare(AT);
        drift(&f, &id);
        assert_eq!(f.prune(stale), (0, 1), "{status} {name}");
        assert_eq!(
            f.count("SELECT COUNT(*) FROM agents WHERE id=?", id.as_str()),
            1
        );
        let (removed, deferred) = f.prune(f.prepare(AT));
        assert!(removed > 0, "{name}");
        assert_eq!(deferred, 0, "{name}");
    }
}

/// A projection observed on an older snapshot than its candidate set (a
/// mixed reader view) is deferred by the writer's recheck.
#[test]
fn projections_from_an_older_snapshot_are_deferred() {
    let mut f = Fixture::new();
    let id = f.terminal("failed");
    let older = f.prepare(AT);
    f.event(&id, "process_cleanup", json!({"confirmed":true}));
    let mut mixed = f.prepare(AT);
    mixed.projections = older.projections;
    assert_eq!(f.prune(mixed), (0, 1));
}

/// Legacy, future, malformed, unrelated, duplicate, inconsistent and
/// oversized proofs are durable validation refusals with nothing written.
#[test]
fn malformed_proofs_refuse_before_mutation() {
    let mut f = Fixture::new();
    let id = f.terminal("failed");
    let good = f.prepare(AT);
    let other: AgentId = "ag-20261011-000000-0000000009".parse().unwrap();
    let cases: [(&str, Corruption); 11] = [
        ("legacy without proof", |b, _| {
            b.projections[0]
                .phases
                .retain(|p| p.phase != "projection_proof")
        }),
        ("version 1", |b, _| {
            proof_of(&mut b.projections[0])["version"] = json!(1)
        }),
        ("future version", |b, _| {
            proof_of(&mut b.projections[0])["version"] = json!(3)
        }),
        ("float version", |b, _| {
            proof_of(&mut b.projections[0])["version"] = json!(2.0)
        }),
        ("unknown field", |b, _| {
            proof_of(&mut b.projections[0])["extra"] = json!(1)
        }),
        ("bad fingerprint", |b, _| {
            proof_of(&mut b.projections[0])["fingerprint"] = json!("ABC")
        }),
        ("unknown evidence", |b, _| {
            proof_of(&mut b.projections[0])["evidence"]["other"] = json!({})
        }),
        ("inconsistent evidence", |b, _| {
            proof_of(&mut b.projections[0])["evidence"]["terminal"]["exit_code"] = json!(7)
        }),
        ("duplicate proof phase", |b, _| {
            let proof = b.projections[0].phases.last().unwrap().clone();
            b.projections[0].phases.push(proof)
        }),
        ("oversized", |b, _| {
            proof_of(&mut b.projections[0])["evidence"]["cleanup"] =
                json!({"version":1,"pad":"x".repeat(4096)})
        }),
        ("unrelated execution", |b, other| {
            b.projections[0].execution = other.clone()
        }),
    ];
    for (name, corrupt) in cases {
        let mut batch = good.clone();
        corrupt(&mut batch, &other);
        assert_eq!(f.refused(batch), RejectCode::Validation, "{name}");
    }
    let mut duplicated = good.clone();
    duplicated.projections.push(good.projections[0].clone());
    assert_eq!(f.refused(duplicated), RejectCode::Validation);
    assert!(f.phases(&id).is_empty());
    assert_eq!(
        f.count("SELECT COUNT(*) FROM agents WHERE id=?", id.as_str()),
        1
    );
    let (removed, deferred) = f.prune(good);
    assert!(removed > 0 && deferred == 0);
}

/// Row 91 durably refuses a stale projection as a conflict, stores a current
/// one, and refreshes the proof on its own key after the source changes.
#[test]
fn stale_capture_is_a_conflict_and_the_proof_refreshes() {
    let mut f = Fixture::new();
    let id = f.terminal("failed");
    let reader = StoreReader::open(&f.home.path).unwrap();
    let stale = reader.incident_projection(&id, AT).unwrap();
    f.event(&id, "process_cleanup", json!({"confirmed":true}));
    match f.apply(LocalOp::from(CaptureIncident { projection: stale })) {
        SubmitResult::Rejected(refusal) => {
            assert_eq!(refusal.code, RejectCode::Conflict);
            assert!(refusal.durable);
        }
        other => panic!("not refused: {other:?}"),
    }
    assert!(f.phases(&id).is_empty());
    let current = reader.incident_projection(&id, AT).unwrap();
    assert!(matches!(
        f.apply(LocalOp::from(CaptureIncident {
            projection: current.clone()
        })),
        SubmitResult::Committed(_)
    ));
    let first = f.proof(&id);
    assert_eq!(first, current.phases.last().unwrap().details);
    f.event(&id, "process_cleanup", json!({"confirmed":false}));
    let refreshed = reader.incident_projection(&id, AT + 1.0).unwrap();
    assert!(matches!(
        f.apply(LocalOp::from(CaptureIncident {
            projection: refreshed.clone()
        })),
        SubmitResult::Committed(_)
    ));
    assert_ne!(f.proof(&id)["fingerprint"], first["fingerprint"]);
    assert_eq!(f.proof(&id)["evidence"]["cleanup"]["confirmed"], false);
    // The ordinary cleanup row keeps its first evidence.
    let first_cleanup: String = f
        .home
        .store()
        .conn
        .query_row(
            "SELECT details_json FROM incident_ledger WHERE execution_id=? AND phase='cleanup'",
            [id.as_str()],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(
        serde_json::from_str::<Value>(&first_cleanup).unwrap()["confirmed"],
        true
    );
}

/// More than 2000 journal rows drain over several fresh batches while the
/// original failure event and the highest sequence disappear; an earlier
/// batch's projection is deferred, never blocking progress, and the saved
/// failure diagnostics survive in the ledger and the latest proof.
#[test]
fn large_histories_drain_over_fresh_batches() {
    let mut f = Fixture::new();
    let id = f.terminal("failed");
    f.event(
        &id,
        "execution_failure_v1",
        json!({"stage":"initial","class":"internal_error"}),
    );
    {
        let store = f.home.store();
        let tx = store.conn.unchecked_transaction().unwrap();
        for _ in 0..2500 {
            tx.execute(
                "INSERT INTO events(agent_id,at,kind,data_json) VALUES(?,3,'progress','{}')",
                [id.as_str()],
            )
            .unwrap();
        }
        tx.commit().unwrap();
    }
    let first = f.prepare(AT);
    let mut fingerprints =
        vec![first.projections[0].phases.last().unwrap().details["fingerprint"].clone()];
    let (removed, deferred) = f.prune(first.clone());
    assert!(removed >= 2000 && deferred == 0);
    assert_eq!(
        f.count(
            "SELECT COUNT(*) FROM events WHERE agent_id=? AND kind='execution_failure_v1'",
            id.as_str()
        ),
        0
    );
    assert_eq!(f.prune(first), (0, 1), "the earlier projection is stale");
    let mut batches = 1;
    while f.count("SELECT COUNT(*) FROM agents WHERE id=?", id.as_str()) == 1 {
        let batch = f.prepare(AT);
        let projection = &batch.projections[0];
        assert!(
            projection
                .phases
                .iter()
                .all(|p| p.phase != "execution_failure")
        );
        fingerprints.push(projection.phases.last().unwrap().details["fingerprint"].clone());
        let (_, deferred) = f.prune(batch);
        assert_eq!(deferred, 0);
        batches += 1;
        assert!(batches < 10);
    }
    assert!(batches >= 2);
    assert_ne!(fingerprints[0], fingerprints[1]);
    assert_eq!(
        f.phases(&id),
        ["execution_failure", "projection_proof", "terminal"]
    );
    assert_eq!(
        f.proof(&id)["evidence"]["execution_failure"]["stage"],
        "initial"
    );
}

/// A failure after the evidence write and source deletions rolls the whole
/// batch back: no proof, no diagnostics, every source row kept.
#[test]
fn a_late_failure_rolls_back_evidence_and_deletions() {
    let mut f = Fixture::new();
    let id = f.terminal("failed");
    f.event(&id, "process_cleanup", json!({"confirmed":true}));
    let events = f.count("SELECT COUNT(*) FROM events WHERE agent_id=?", id.as_str());
    f.sql(
        "CREATE TRIGGER injected_failure BEFORE DELETE ON agents BEGIN SELECT RAISE(ABORT,'injected'); END",
        [],
    );
    assert!(matches!(
        f.apply(LocalOp::from(f.prepare(AT))),
        SubmitResult::Rejected(_)
    ));
    assert!(f.phases(&id).is_empty());
    assert_eq!(
        f.count("SELECT COUNT(*) FROM events WHERE agent_id=?", id.as_str()),
        events
    );
    f.sql("DROP TRIGGER injected_failure", []);
    let (removed, deferred) = f.prune(f.prepare(AT));
    assert!(removed > 0 && deferred == 0);
}

/// Ledger age pruning in the same transaction cannot evict a candidate's
/// sole required evidence (its terminal phase is older than the ledger
/// window), and an insertion the ledger silently dropped is no proof: that
/// candidate is deferred.
#[test]
fn required_evidence_is_protected_and_verified_durable() {
    let mut f = Fixture::new();
    let id = f.terminal("failed");
    let late = AT + 31.0 * 86_400.0;
    let (removed, deferred) = f.prune(f.prepare(late));
    assert!(removed > 0 && deferred == 0);
    assert_eq!(f.phases(&id), ["projection_proof", "terminal"]);
    let dropped = f.terminal("failed");
    f.sql(
        "CREATE TRIGGER dropped_insert BEFORE INSERT ON incident_ledger WHEN NEW.phase='terminal' \
         BEGIN SELECT RAISE(IGNORE); END",
        [],
    );
    assert_eq!(f.prune(f.prepare(late)), (0, 1));
    assert_eq!(
        f.count("SELECT COUNT(*) FROM agents WHERE id=?", dropped.as_str()),
        1
    );
}

/// The reader preparer bounds its limit and time and writes nothing.
#[test]
fn history_batch_bounds_its_inputs() {
    let f = Fixture::new();
    f.terminal("succeeded");
    let reader = StoreReader::open(&f.home.path).unwrap();
    assert!(reader.history_batch(AT, 0).is_err());
    assert!(reader.history_batch(AT, 2001).is_err());
    assert!(reader.history_batch(f64::NAN, 1).is_err());
    assert!(reader.history_batch(1.0, 1).is_err());
    assert_eq!(reader.history_batch(AT, 1).unwrap().candidates.len(), 1);
    assert_eq!(
        f.count("SELECT COUNT(*) FROM incident_ledger WHERE phase<>?", ""),
        0
    );
}

/// Rows of the ledger as exact SQL literals, ordered by key.
fn ledger_rows(conn: &Connection) -> Vec<String> {
    let mut statement = conn
        .prepare(
            "SELECT quote(execution_id)||','||quote(phase)||','||quote(occurred_at)||','||quote(CAST(details_json AS BLOB)) \
             FROM incident_ledger ORDER BY execution_id,phase",
        )
        .unwrap();
    statement
        .query_map([], |row| row.get(0))
        .unwrap()
        .collect::<rusqlite::Result<_>>()
        .unwrap()
}

/// A copy of the committed schema-29 fixture with a populated ledger.
fn populated_v29(path: &Path) -> Vec<String> {
    let fixture = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../tests/fixtures/baseline/db/current-v29.sqlite");
    std::fs::copy(fixture, path).unwrap();
    let conn = Connection::open(path).unwrap();
    for (index, phase) in [
        "terminal",
        "execution_failure",
        "cleanup",
        "delivery_expired",
    ]
    .iter()
    .enumerate()
    {
        conn.execute(
            "INSERT INTO incident_ledger VALUES(?,?,?,?)",
            params![
                format!("ag-20261011-00000{index}-0123456789"),
                phase,
                1.5 + index as f64,
                json!({"version":1,"text":"é\u{1F600}","pad":"y".repeat(index * 1000)}).to_string()
            ],
        )
        .unwrap();
    }
    ledger_rows(&conn)
}

/// Migration 030 rebuilds a populated schema-29 ledger with byte-exact rows,
/// its index and the key-guarded trigger, and only the proof row refreshes.
#[test]
fn migration_030_rebuilds_the_ledger_exactly() {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("state.db");
    let before = populated_v29(&path);
    assert_eq!(migrations::migrate(&path).unwrap(), 31);
    let conn = Connection::open(&path).unwrap();
    assert_eq!(ledger_rows(&conn), before);
    let objects: Vec<String> = conn
        .prepare("SELECT name FROM sqlite_master WHERE tbl_name='incident_ledger' ORDER BY name")
        .unwrap()
        .query_map([], |row| row.get(0))
        .unwrap()
        .collect::<rusqlite::Result<_>>()
        .unwrap();
    assert_eq!(
        objects,
        [
            "idx_incident_ledger_time",
            "incident_ledger",
            "incident_ledger_immutable",
            "sqlite_autoindex_incident_ledger_1"
        ]
    );
    let id = "ag-20261011-000000-0123456789";
    let proof = json!({"version":2,"fingerprint":"0".repeat(64),"evidence":{}}).to_string();
    conn.execute(
        "INSERT INTO incident_ledger VALUES(?,'projection_proof',1,?)",
        params![id, proof],
    )
    .unwrap();
    conn.execute(
        "UPDATE incident_ledger SET occurred_at=2,details_json='{\"version\":2}' WHERE execution_id=? AND phase='projection_proof'",
        [id],
    )
    .unwrap();
    for refused in [
        "UPDATE incident_ledger SET details_json='{}' WHERE execution_id=?1 AND phase='terminal'",
        "UPDATE incident_ledger SET phase='projection_proof' WHERE execution_id=?1 AND phase='terminal'",
        "UPDATE incident_ledger SET phase='cleanup' WHERE execution_id=?1 AND phase='projection_proof'",
        "UPDATE incident_ledger SET execution_id='ag-20261011-000009-0123456789' WHERE execution_id=?1 AND phase='projection_proof'",
        "UPDATE incident_ledger SET details_json='[1]' WHERE execution_id=?1 AND phase='projection_proof'",
        "UPDATE incident_ledger SET details_json='not json' WHERE execution_id=?1 AND phase='projection_proof'",
    ] {
        assert!(conn.execute(refused, [id]).is_err(), "{refused}");
    }
    let oversized = json!({"pad":"z".repeat(4096)}).to_string();
    assert!(
        conn.execute(
            "UPDATE incident_ledger SET details_json=?2 WHERE execution_id=?1 AND phase='projection_proof'",
            params![id, oversized],
        )
        .is_err()
    );
}

/// A rebuild that fails part-way rolls back: schema 29, rows and the
/// original trigger stay.
#[test]
fn a_failed_030_rebuild_rolls_back() {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("state.db");
    let before = populated_v29(&path);
    let trigger_before: String = {
        let conn = Connection::open(&path).unwrap();
        conn.execute_batch("CREATE TABLE incident_ledger_v29(x)")
            .unwrap();
        conn.query_row(
            "SELECT sql FROM sqlite_master WHERE name='incident_ledger_immutable'",
            [],
            |row| row.get(0),
        )
        .unwrap()
    };
    assert!(migrations::migrate(&path).is_err());
    let conn = Connection::open(&path).unwrap();
    let version: i64 = conn
        .pragma_query_value(None, "user_version", |row| row.get(0))
        .unwrap();
    assert_eq!(version, 29);
    assert_eq!(ledger_rows(&conn), before);
    let trigger: String = conn
        .query_row(
            "SELECT sql FROM sqlite_master WHERE name='incident_ledger_immutable'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(trigger, trigger_before);
}

/// The committed current-schema fixture and its manifest entry carry the
/// rebuilt ledger with the proof phase and key-guarded trigger.
#[test]
fn current_fixture_carries_the_rebuilt_ledger() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../tests/fixtures/baseline/db");
    let manifest: Value =
        serde_json::from_slice(&std::fs::read(root.join("manifest.json")).unwrap()).unwrap();
    let entry = manifest["files"]
        .as_array()
        .unwrap()
        .iter()
        .find(|file| file["file"] == "current-v31.sqlite")
        .unwrap();
    assert_eq!(entry["user_version"], 31);
    let conn = Connection::open_with_flags(
        root.join("current-v31.sqlite"),
        rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY,
    )
    .unwrap();
    let ledger: Vec<String> = conn
        .prepare("SELECT sql FROM sqlite_master WHERE tbl_name='incident_ledger' AND sql IS NOT NULL ORDER BY name")
        .unwrap()
        .query_map([], |row| row.get(0))
        .unwrap()
        .collect::<rusqlite::Result<_>>()
        .unwrap();
    assert!(ledger[1].contains("'delivery_expired','projection_proof'"));
    assert!(ledger[2].contains("NEW.execution_id IS NOT OLD.execution_id"));
    let rows: i64 = conn
        .query_row("SELECT COUNT(*) FROM incident_ledger", [], |row| row.get(0))
        .unwrap();
    assert_eq!(Some(rows), entry["rows"]["incident_ledger"].as_i64());
}

/// Two selected deliveries in the same state share that state's phase with
/// both deliveries' attempts and ambiguity in closed fields: the projection
/// stays valid and the candidate is removed.
#[test]
fn same_state_deliveries_share_one_closed_phase() {
    let mut f = Fixture::new();
    let id = f.terminal("failed");
    for (event, delivery, attempts, ambiguous) in [(1, "d1", 2, 0), (2, "d2", 5, 1)] {
        f.event(&id, "terminal", json!({ "n": event }));
        f.sql(
            "INSERT INTO deliveries(id,agent_id,terminal_event_seq,state,attempts,ambiguous_result) \
             SELECT ?2,agent_id,MAX(seq),'failed',?3,?4 FROM events WHERE agent_id=?1",
            params![id.as_str(), delivery, attempts, ambiguous],
        );
    }
    let batch = f.prepare(AT);
    let projection = &batch.projections[0];
    let failed: Vec<_> = projection
        .phases
        .iter()
        .filter(|phase| phase.phase == "delivery_failed")
        .collect();
    assert_eq!(failed.len(), 1);
    assert_eq!(
        failed[0].details,
        json!({"version":1,"state":"failed","attempts":2,"ambiguous":false,
               "deliveries":2,"second_attempts":5,"second_ambiguous":true})
    );
    let (removed, deferred) = f.prune(batch);
    assert!(removed > 0 && deferred == 0);
    assert!(f.phases(&id).contains(&"delivery_failed".to_owned()));
}

/// Between partial purge batches, independent ledger maintenance (row 86)
/// keeps every row of an execution whose source still exists, however old;
/// once the source is gone normal age retention applies.
#[test]
fn partial_purges_keep_evidence_until_the_source_is_gone() {
    let mut f = Fixture::new();
    let id = f.terminal("failed");
    f.event(&id, "execution_failure_v1", json!({"stage":"initial"}));
    {
        let store = f.home.store();
        let tx = store.conn.unchecked_transaction().unwrap();
        for _ in 0..2100 {
            tx.execute(
                "INSERT INTO events(agent_id,at,kind,data_json) VALUES(?,3,'progress','{}')",
                [id.as_str()],
            )
            .unwrap();
        }
        tx.commit().unwrap();
    }
    f.prune(f.prepare(AT));
    assert_eq!(
        f.count("SELECT COUNT(*) FROM agents WHERE id=?", id.as_str()),
        1
    );
    let ledger = |f: &mut Fixture, at: f64| {
        assert!(matches!(
            f.apply(LocalOp::from(agent_run_store::ops::PruneIncidentLedger {
                at
            })),
            SubmitResult::Committed(_)
        ));
    };
    let late = AT + 60.0 * 86_400.0;
    ledger(&mut f, late);
    assert_eq!(
        f.phases(&id),
        ["execution_failure", "projection_proof", "terminal"]
    );
    while f.count("SELECT COUNT(*) FROM agents WHERE id=?", id.as_str()) == 1 {
        f.prune(f.prepare(AT));
    }
    ledger(&mut f, late);
    assert!(f.phases(&id).is_empty());
}

/// Fills the ledger to its hard bound with rows of live (running, so never
/// eligible) executions, every row protected by an existing source.
fn fill_to_the_hard_bound(f: &Fixture) {
    let phases = [
        "terminal",
        "execution_failure",
        "cleanup",
        "delivery_waiting_binding",
        "delivery_pending",
        "delivery_sending",
        "delivery_delivered",
        "delivery_retry_wait",
        "delivery_failed",
        "delivery_cancelled",
        "delivery_expired",
        "projection_proof",
    ];
    {
        let store = f.home.store();
        let tx = store.conn.unchecked_transaction().unwrap();
        let mut rows: i64 = tx
            .query_row("SELECT COUNT(*) FROM incident_ledger", [], |row| row.get(0))
            .unwrap();
        let mut owner = 0;
        'fill: loop {
            let execution = format!("ag-20261011-000000-{owner:010}");
            tx.execute(
                "INSERT INTO agents(id,runtime,model,profile,task,task_summary,workdir,request_json,status,created_at,config_revision) \
                 VALUES (?,'mock','fixture','review','t','t','/tmp','{}','running',1,'cfg')",
                [&execution],
            )
            .unwrap();
            for phase in phases {
                if rows == agent_run_store::incidents::HARD_MAX_RECORDS {
                    break 'fill;
                }
                tx.execute(
                    "INSERT INTO incident_ledger VALUES(?,?,1,'{}')",
                    params![execution, phase],
                )
                .unwrap();
                rows += 1;
            }
            owner += 1;
        }
        tx.commit().unwrap();
    }
}

/// At the hard ledger bound, with every row protected by an existing
/// source, a candidate whose evidence is already stored still purges (its
/// proof refresh adds no row) while one needing new rows is deferred and an
/// incident capture is refused.
#[test]
fn the_hard_ledger_bound_defers_instead_of_evicting() {
    let mut f = Fixture::new();
    let stored = f.terminal("failed");
    let reader = StoreReader::open(&f.home.path).unwrap();
    assert!(matches!(
        f.apply(LocalOp::from(CaptureIncident {
            projection: reader.incident_projection(&stored, AT).unwrap()
        })),
        SubmitResult::Committed(_)
    ));
    let fresh = f.terminal("failed");
    fill_to_the_hard_bound(&f);
    let (removed, deferred) = f.prune(f.prepare(AT));
    assert!(removed > 0);
    assert_eq!(deferred, 1);
    assert_eq!(
        f.count("SELECT COUNT(*) FROM agents WHERE id=?", stored.as_str()),
        0
    );
    assert_eq!(
        f.count("SELECT COUNT(*) FROM agents WHERE id=?", fresh.as_str()),
        1
    );
    assert!(matches!(
        f.apply(LocalOp::from(CaptureIncident {
            projection: reader.incident_projection(&fresh, AT).unwrap()
        })),
        SubmitResult::Rejected(_)
    ));
    assert!(
        f.count("SELECT COUNT(*) FROM incident_ledger WHERE phase<>?", "")
            <= agent_run_store::incidents::HARD_MAX_RECORDS
    );
}

/// A prepared batch is packed within row 85's decoded bound: 2000 failed
/// candidates whose projections together exceed 2 MiB yield a smaller batch
/// that the writer accepts.
#[test]
fn history_batches_are_packed_within_the_decoded_bound() {
    let mut f = Fixture::new();
    {
        let store = f.home.store();
        let tx = store.conn.unchecked_transaction().unwrap();
        for index in 0..2000 {
            let execution = format!("ag-20261011-000001-{index:010}");
            tx.execute(
                "INSERT INTO agents(id,runtime,model,profile,task,task_summary,workdir,request_json,status,created_at,finished_at,exit_code,config_revision) \
                 VALUES (?,'mock','fixture','review','t','t','/tmp','{}','failed',1,1,1,'cfg')",
                [&execution],
            )
            .unwrap();
            tx.execute(
                "INSERT INTO events(agent_id,at,kind,data_json) VALUES(?,2,'execution_failure_v1',?)",
                params![
                    execution,
                    json!({"stage":"initial","class":"internal_error","sqlite_extended_code":5,"os_code":2,"wait_ms":1,"attempts":1}).to_string()
                ],
            )
            .unwrap();
            tx.execute(
                "INSERT INTO events(agent_id,at,kind,data_json) VALUES(?,3,'process_cleanup','{\"confirmed\":true}')",
                [&execution],
            )
            .unwrap();
        }
        tx.commit().unwrap();
    }
    let batch = f.prepare(AT);
    assert!(!batch.candidates.is_empty() && batch.candidates.len() < 2000);
    assert!(batch.projections.len() + 1 >= batch.candidates.len());
    let encoded = serde_json::to_vec(&LocalOp::from(batch.clone()))
        .unwrap()
        .len();
    assert!(encoded <= 2 * 1024 * 1024, "{encoded}");
    let (removed, _) = f.prune(batch);
    assert!(removed > 0);
}

/// Candidates and projections come from one read snapshot: a source change
/// committed between the two reads is not mixed into the prepared
/// projection (and the writer then defers the stale candidate).
#[test]
fn history_batches_read_one_snapshot() {
    let mut f = Fixture::new();
    let id = f.terminal("failed");
    let reader = StoreReader::open(&f.home.path).unwrap();
    let home = f.home.path.clone();
    let changed = id.clone();
    reader.pause_history_batch(move || {
        agent_run_store::fixture::open(&home)
            .unwrap()
            .conn
            .execute(
                "INSERT INTO events(agent_id,at,kind,data_json) VALUES(?,4,'process_cleanup','{\"confirmed\":true}')",
                [changed.as_str()],
            )
            .unwrap();
    });
    let batch = reader.history_batch(AT, 10).unwrap();
    assert_eq!(batch.candidates, std::slice::from_ref(&id));
    assert!(
        batch.projections[0]
            .phases
            .iter()
            .all(|phase| phase.phase != "cleanup")
    );
    assert_eq!(f.prune(batch), (0, 1));
}

/// SQLite VM instructions one projection may spend over a 10000-event
/// journal (about 100000 when the lookups walk the journal).
const PROJECTION_STEP_BUDGET: u64 = 5_000;

/// Incident source lookups seek the composite kind index whether the
/// failure and cleanup phases are present or absent: the actual query plans
/// search it, and projecting an execution with 10000 unrelated journal
/// events stays within a deterministic VM-step budget.
#[test]
fn incident_source_lookups_seek_the_kind_index() {
    let f = Fixture::new();
    let absent = f.terminal("succeeded");
    let present = f.terminal("failed");
    f.event(&present, "execution_failure_v1", json!({"stage":"initial"}));
    {
        let store = f.home.store();
        let tx = store.conn.unchecked_transaction().unwrap();
        for id in [&absent, &present] {
            for _ in 0..10_000 {
                tx.execute(
                    "INSERT INTO events(agent_id,at,kind,data_json) VALUES(?,3,'progress','{}')",
                    [id.as_str()],
                )
                .unwrap();
            }
        }
        tx.commit().unwrap();
    }
    f.event(&present, "process_cleanup", json!({"confirmed":true}));
    let store = f.home.store();
    for id in [&absent, &present] {
        let steps = std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0));
        let counter = std::sync::Arc::clone(&steps);
        store
            .conn
            .progress_handler(
                1,
                Some(move || {
                    counter.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    false
                }),
            )
            .unwrap();
        let projection = agent_run_store::incidents::project(&store.conn, id.as_str(), AT)
            .unwrap()
            .unwrap();
        store
            .conn
            .progress_handler(0, None::<fn() -> bool>)
            .unwrap();
        let steps = steps.load(std::sync::atomic::Ordering::Relaxed);
        assert!(steps < PROJECTION_STEP_BUDGET, "{id}: {steps} VM steps");
        assert_eq!(
            projection.phases.iter().any(|p| p.phase == "cleanup"),
            *id == present
        );
    }
    for order in ["ASC", "DESC"] {
        let plan: Vec<String> = store
            .conn
            .prepare(&format!(
                "EXPLAIN QUERY PLAN SELECT seq,at,data_json FROM events WHERE agent_id=? AND kind=? ORDER BY seq {order} LIMIT 1"
            ))
            .unwrap()
            .query_map(params![absent.as_str(), "process_cleanup"], |row| row.get(3))
            .unwrap()
            .collect::<rusqlite::Result<_>>()
            .unwrap();
        assert!(
            plan.iter()
                .any(|step| step.contains("idx_events_agent_kind_seq")),
            "{plan:?}"
        );
    }
}

/// Candidates deferred at the hard bound do not starve the eligible ones
/// behind them: with the 32 oldest candidates each needing new ledger rows
/// and a 33rd already proven, one batch scans past the deferred prefix and
/// removes the 33rd.
#[test]
fn deferred_candidates_never_starve_proven_ones() {
    let mut f = Fixture::new();
    let blocked: Vec<AgentId> = (0..32).map(|_| f.terminal("failed")).collect();
    let proven = f.terminal("failed");
    f.sql(
        "UPDATE agents SET finished_at=2 WHERE id=?",
        [proven.as_str()],
    );
    let reader = StoreReader::open(&f.home.path).unwrap();
    assert!(matches!(
        f.apply(LocalOp::from(CaptureIncident {
            projection: reader.incident_projection(&proven, AT).unwrap()
        })),
        SubmitResult::Committed(_)
    ));
    fill_to_the_hard_bound(&f);
    let (removed, deferred) = f.prune(f.prepare(AT));
    assert!(removed > 0);
    assert_eq!(deferred, 32);
    assert_eq!(
        f.count("SELECT COUNT(*) FROM agents WHERE id=?", proven.as_str()),
        0
    );
    for id in &blocked {
        assert_eq!(
            f.count("SELECT COUNT(*) FROM agents WHERE id=?", id.as_str()),
            1
        );
    }
}

/// Keyset pages progress beyond two thousand deferred older sources without evicting their protected ledger.
#[test]
fn bounded_keyset_cycles_reach_stored_candidate_after_two_thousand_deferred() {
    let mut f = Fixture::new();
    let blocked: Vec<AgentId> = (0..2000).map(|_| f.terminal("failed")).collect();
    let proven = f.terminal("failed");
    f.sql(
        "UPDATE agents SET finished_at=2 WHERE id=?",
        [proven.as_str()],
    );
    let reader = StoreReader::open(&f.home.path).unwrap();
    assert!(matches!(
        f.apply(LocalOp::from(CaptureIncident {
            projection: reader.incident_projection(&proven, AT).unwrap()
        })),
        SubmitResult::Committed(_)
    ));
    fill_to_the_hard_bound(&f);
    let counts = || {
        f.home.store().conn.query_row("SELECT (SELECT count(*) FROM agents WHERE status='running'), (SELECT count(*) FROM incident_ledger l JOIN agents a ON a.id=l.execution_id WHERE a.status='running')", [], |row| Ok((row.get::<_, i64>(0)?, row.get::<_, i64>(1)?))).unwrap()
    };
    let protected_before = counts();
    let mut cursor = None;
    let mut removed_tail = false;
    for _ in 0..64 {
        let (batch, next) = reader.history_page(AT, 32, cursor.as_ref()).unwrap();
        assert!(batch.candidates.len() <= 32);
        assert!(
            !batch.candidates.is_empty(),
            "cursor cannot wrap before reaching candidate2001"
        );
        f.prune(batch);
        cursor = next;
        if f.count("SELECT count(*) FROM agents WHERE id=?", proven.as_str()) == 0 {
            removed_tail = true;
            break;
        }
    }
    assert!(
        removed_tail,
        "bounded keyset cycles must advance past the deferred prefix"
    );
    for id in blocked {
        assert_eq!(
            f.count("SELECT count(*) FROM agents WHERE id=?", id.as_str()),
            1
        );
    }
    let protected_after = f.home.store().conn.query_row("SELECT (SELECT count(*) FROM agents WHERE status='running'), (SELECT count(*) FROM incident_ledger l JOIN agents a ON a.id=l.execution_id WHERE a.status='running')", [], |row| Ok((row.get::<_, i64>(0)?, row.get::<_, i64>(1)?))).unwrap();
    assert_eq!(protected_after, protected_before);
}
