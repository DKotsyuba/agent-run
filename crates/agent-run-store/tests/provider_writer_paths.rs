//! Provider rows through the sole writer on disposable private homes: a
//! provider attempt that provably never spawned receives its never-spawned
//! cleanup proof inside the terminal transaction (row 42) and nothing else
//! does; runtime layouts (rows 88/89) are authorized against their owner
//! execution, read inside the write transaction for a commit.
mod common;

use agent_run_domain::{
    domain::{AgentId, Outcome, Status},
    storage::{
        MonoDeadline, OperationId, ProducerId, ProducerKind, RejectCode, SubmitResult,
        WriteAuthority,
    },
};
use agent_run_store::{
    ops::{CommitRuntimeLayout, FinishTerminal, PrepareRuntimeLayout, RemoteOp, WriteEnvelope},
    writer::{StoreReader, StoreWriter, WriterLease},
};
use rusqlite::params;
use serde_json::{Value, json};
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

    /// A remote envelope of `op` with the next ordinal.
    fn envelope(&self, op: RemoteOp) -> WriteEnvelope {
        let (boot, now) = agent_run_platform::monotonic::now().unwrap();
        self.ordinal.set(self.ordinal.get() + 1);
        WriteEnvelope::new(
            OperationId::new(&boot, now),
            ProducerId {
                kind: ProducerKind::Broker,
                scope: Some("test".into()),
                instance: "instance-1".into(),
            },
            self.ordinal.get(),
            MonoDeadline {
                boot_id: boot,
                ns: now + 60_000_000_000,
            },
            op,
        )
        .unwrap()
    }

    /// Runs one SQL statement on a raw store connection.
    fn sql(&self, statement: &str, values: impl rusqlite::Params) {
        self.home.store().conn.execute(statement, values).unwrap();
    }

    /// One text value of a query with one parameter.
    fn text(&self, query: &str, value: &str) -> Option<String> {
        self.home
            .store()
            .conn
            .query_row(query, [value], |row| row.get(0))
            .unwrap()
    }

    /// Counts rows of a query with one parameter.
    fn count(&self, query: &str, value: &str) -> i64 {
        self.home
            .store()
            .conn
            .query_row(query, [value], |row| row.get(0))
            .unwrap()
    }

    /// A starting provider execution whose only owned attempt `att` is
    /// still prepared.
    fn provider(&self) -> AgentId {
        let (id, _) = self
            .home
            .store()
            .admit(&self.home.request(), &self.home.config, &json!({}), None)
            .unwrap();
        self.sql(
            "UPDATE agents SET status='starting',identity_json=? WHERE id=?",
            params![
                json!({"provider_identity_version":2}).to_string(),
                id.as_str()
            ],
        );
        self.sql("DELETE FROM attempts WHERE agent_id=?", [id.as_str()]);
        self.sql(
            "INSERT INTO attempts(id,agent_id,number,state,adapter_state_json,created_at,phase,ownership_active) \
             VALUES('att',?,1,'starting','{}',1,'prepared',1)",
            [id.as_str()],
        );
        id
    }
}

/// Spawn evidence recorded for an execution's attempt.
type Evidence = fn(&Fixture, &AgentId);

/// A terminal outcome of `status` with a failure kind.
fn outcome(status: Status, kind: &str) -> Outcome {
    Outcome {
        status,
        exit_code: None,
        failure_kind: Some(kind.into()),
        failure_text: None,
        runtime_session_id: None,
    }
}

/// The terminal row of `id` with `outcome`.
fn finish(id: &AgentId, outcome: Outcome) -> RemoteOp {
    RemoteOp::from(FinishTerminal {
        execution: id.clone(),
        outcome,
        proof: None,
        usage: None,
    })
}

/// Cancellation before spawn, a preparation failure and an OS spawn refusal
/// all commit the terminal row with the never-spawned proof, the attempt
/// closed and released, one terminal event and one outbox row; the
/// read-only preflight agrees beforehand and the proof is never synthesized
/// twice.
#[test]
fn never_spawned_attempts_get_their_proof_in_the_terminal_transaction() {
    for (status, kind) in [
        (Status::Cancelled, "cancelled_before_spawn"),
        (Status::Failed, "preparation_failed"),
        (Status::Failed, "provider_spawn_failed"),
    ] {
        let mut f = Fixture::new();
        let id = f.provider();
        let reader = StoreReader::open(&f.home.path).unwrap();
        assert!(reader.provider_never_spawned_pending(&id).unwrap());
        let envelope = f.envelope(finish(&id, outcome(status, kind)));
        assert!(
            matches!(
                f.writer.apply(&WriteAuthority::Broker, &envelope),
                SubmitResult::Committed(_)
            ),
            "{kind}"
        );
        assert_eq!(
            f.text(
                "SELECT cleanup_proof_json FROM attempts WHERE agent_id=?",
                id.as_str()
            ),
            Some(r#"{"never_spawned":true}"#.into())
        );
        assert_eq!(
            f.count(
                "SELECT COUNT(*) FROM attempts WHERE agent_id=? AND phase='cleanup_complete' AND ownership_active=0 AND finished_at IS NOT NULL",
                id.as_str()
            ),
            1
        );
        assert_eq!(
            f.text("SELECT status FROM agents WHERE id=?", id.as_str()),
            Some(status.as_str().into())
        );
        let terminal_rows = |f: &Fixture| {
            (
                f.count(
                    "SELECT COUNT(*) FROM events WHERE agent_id=? AND kind='status' AND to_status IN ('failed','cancelled')",
                    id.as_str(),
                ),
                f.count("SELECT COUNT(*) FROM deliveries WHERE agent_id=?", id.as_str()),
            )
        };
        let before = terminal_rows(&f);
        assert_eq!(before.0, 1);
        // Replaying the same envelope answers the stored receipt and adds
        // nothing.
        match f.writer.apply(&WriteAuthority::Broker, &envelope) {
            SubmitResult::Committed(receipt) => assert!(receipt.replayed),
            other => panic!("not replayed: {other:?}"),
        }
        assert_eq!(terminal_rows(&f), before);
        assert!(!reader.provider_never_spawned_pending(&id).unwrap());
    }
}

/// Spawning or spawned attempts, recorded process evidence, a released
/// (stale) attempt and an existing (unconfirmed) cleanup proof are never certified
/// never-spawned: the proof is not synthesized (and an existing one is not
/// overwritten), and without any proof the terminal row is refused with
/// nothing changed.
#[test]
fn spawn_evidence_is_never_certified_never_spawned() {
    let cases: [(&str, Evidence); 4] = [
        ("spawning", |f, _| {
            f.sql("UPDATE attempts SET phase='spawning' WHERE id='att'", [])
        }),
        ("identity", |f, _| {
            f.sql(
                "UPDATE attempts SET process_identity='t',process_birth_time=1 WHERE id='att'",
                [],
            )
        }),
        ("ownership", |f, _| {
            f.sql(
                "INSERT INTO process_ownership(owner_kind,owner_id,leader_json,descendants_observed,updated_at) VALUES('attempt','att','{}',1,1)",
                [],
            )
        }),
        ("stale ownership", |f, _| {
            f.sql("UPDATE attempts SET ownership_active=0 WHERE id='att'", [])
        }),
    ];
    for (name, prepare) in cases {
        let mut f = Fixture::new();
        let id = f.provider();
        prepare(&f, &id);
        let reader = StoreReader::open(&f.home.path).unwrap();
        assert!(
            !reader.provider_never_spawned_pending(&id).unwrap(),
            "{name}"
        );
        let envelope = f.envelope(finish(&id, outcome(Status::Cancelled, "cancelled")));
        assert!(
            matches!(
                f.writer.apply(&WriteAuthority::Broker, &envelope),
                SubmitResult::Rejected(_)
            ),
            "{name}"
        );
        assert_eq!(
            f.count(
                "SELECT COUNT(*) FROM attempts WHERE agent_id=? AND cleanup_proof_json IS NOT NULL",
                id.as_str()
            ),
            0,
            "{name}"
        );
        assert_eq!(
            f.text("SELECT status FROM agents WHERE id=?", id.as_str()),
            Some("starting".into()),
            "{name}"
        );
    }
    let mut f = Fixture::new();
    let id = f.provider();
    let unconfirmed = json!({"confirmed": false}).to_string();
    f.sql(
        "UPDATE attempts SET cleanup_proof_json=? WHERE id='att'",
        [&unconfirmed],
    );
    let envelope = f.envelope(finish(&id, outcome(Status::Failed, "failed")));
    assert!(matches!(
        f.writer.apply(&WriteAuthority::Broker, &envelope),
        SubmitResult::Committed(_)
    ));
    assert_eq!(
        f.text("SELECT cleanup_proof_json FROM attempts WHERE id=?", "att"),
        Some(unconfirmed)
    );
}

/// A running provider whose only owned attempt never spawned cannot be
/// finished as a success, even with a valid answer proof: no never-spawned
/// proof is synthesized for a success, the terminal row is refused and the
/// execution, attempt and ownership stay unchanged.
#[test]
fn a_never_spawned_attempt_is_never_a_success() {
    let mut f = Fixture::new();
    let id = f.provider();
    f.sql(
        "UPDATE agents SET status='running' WHERE id=?",
        [id.as_str()],
    );
    let proof = agent_run_platform::verify::Proof {
        path: f
            .home
            .path
            .join("agents")
            .join(id.as_str())
            .join("answer.md"),
        bytes: 6,
        sha256: "a".repeat(64),
        proof_version: 1,
    };
    let envelope = f.envelope(RemoteOp::from(FinishTerminal {
        execution: id.clone(),
        outcome: Outcome::success(None),
        proof: Some(proof),
        usage: None,
    }));
    assert!(matches!(
        f.writer.apply(&WriteAuthority::Broker, &envelope),
        SubmitResult::Rejected(_)
    ));
    assert_eq!(
        f.text("SELECT status FROM agents WHERE id=?", id.as_str()),
        Some("running".into())
    );
    assert_eq!(
        f.count(
            "SELECT COUNT(*) FROM attempts WHERE id='att' AND phase='prepared' AND ownership_active=1 \
             AND cleanup_proof_json IS NULL AND finished_at IS NULL AND agent_id=?",
            id.as_str()
        ),
        1
    );
}

/// A failure later in the terminal transaction rolls the synthesized proof
/// back with everything else; a fresh terminal row then commits once.
#[test]
fn a_failed_terminal_transaction_rolls_back_the_proof() {
    let mut f = Fixture::new();
    let id = f.provider();
    f.sql(
        "CREATE TRIGGER injected_failure BEFORE INSERT ON events WHEN NEW.kind='status' \
         BEGIN SELECT RAISE(ABORT,'injected'); END",
        [],
    );
    let failing = f.envelope(finish(&id, outcome(Status::Cancelled, "cancelled")));
    assert!(matches!(
        f.writer.apply(&WriteAuthority::Broker, &failing),
        SubmitResult::Rejected(_)
    ));
    assert_eq!(
        f.text("SELECT phase FROM attempts WHERE id=?", "att"),
        Some("prepared".into())
    );
    assert_eq!(
        f.count(
            "SELECT COUNT(*) FROM attempts WHERE id=? AND cleanup_proof_json IS NULL AND ownership_active=1",
            "att"
        ),
        1
    );
    assert_eq!(
        f.text("SELECT status FROM agents WHERE id=?", id.as_str()),
        Some("starting".into())
    );
    f.sql("DROP TRIGGER injected_failure", []);
    let retried = f.envelope(finish(&id, outcome(Status::Cancelled, "cancelled")));
    assert!(matches!(
        f.writer.apply(&WriteAuthority::Broker, &retried),
        SubmitResult::Committed(_)
    ));
    assert_eq!(
        f.count(
            "SELECT COUNT(*) FROM events WHERE agent_id=? AND kind='status' AND to_status='cancelled'",
            id.as_str()
        ),
        1
    );
}

/// A canonical version-1 layout for `home`.
fn layout(home: &str) -> String {
    serde_json::to_string(&json!({
        "version": 1,
        "runtime_home": home,
        "index_sha256": "1".repeat(64),
        "roots": {"assets": {"scope": "2".repeat(64), "manifest_sha256": "3".repeat(64)}},
    }))
    .unwrap()
}

/// The supervisor of `id` (capability `c`, pid 10, token `t`), recorded on
/// its agents row.
fn supervisor(f: &Fixture, id: &AgentId) -> WriteAuthority {
    f.sql(
        "UPDATE agents SET supervisor_capability_sha256=?,supervisor_pid=10,supervisor_identity='t',supervisor_birth_time=1.0 WHERE id=?",
        params!["c".repeat(64), id.as_str()],
    );
    WriteAuthority::Supervisor {
        logical: id.clone(),
        execution: id.clone(),
        attempt: None,
        capability_sha256: "c".repeat(64),
        pid: 10,
        birth: 1.0,
        token: "t".into(),
    }
}

/// The refusal code and durability of an envelope that must not commit.
fn refused(f: &mut Fixture, authority: &WriteAuthority, op: RemoteOp) -> (RejectCode, bool) {
    match f.writer.apply(authority, &f.envelope(op)) {
        SubmitResult::Rejected(refusal) => (refusal.code, refusal.durable),
        other => panic!("not refused: {other:?}"),
    }
}

/// The layout receipt result of a committed envelope.
fn committed(f: &mut Fixture, authority: &WriteAuthority, envelope: &WriteEnvelope) -> Value {
    match f.writer.apply(authority, envelope) {
        SubmitResult::Committed(receipt) => receipt.result,
        other => panic!("not committed: {other:?}"),
    }
}

/// A supervisor prepares and commits its own layout (replays answer the
/// stored receipt); a foreign or absent owner, another execution's layout,
/// an unowned layout and an unregistered home are refused without change,
/// while the broker may prepare and commit an unowned layout.
#[test]
fn runtime_layouts_are_authorized_against_their_owner() {
    let mut f = Fixture::new();
    let (own, _) = f
        .home
        .store()
        .admit(&f.home.request(), &f.home.config, &json!({}), None)
        .unwrap();
    let (other, _) = f
        .home
        .store()
        .admit(&f.home.request(), &f.home.config, &json!({}), None)
        .unwrap();
    let mine = supervisor(&f, &own);
    let prepare = |home: &str, owner: Option<&AgentId>| {
        RemoteOp::from(PrepareRuntimeLayout {
            layout_json: layout(home),
            owner: owner.cloned(),
        })
    };
    for (owner, name) in [(Some(&other), "foreign owner"), (None, "no owner")] {
        assert_eq!(
            refused(&mut f, &mine, prepare("/runtime/own", owner)),
            (RejectCode::Unauthorized, true),
            "{name}"
        );
    }
    assert_eq!(
        f.count(
            "SELECT COUNT(*) FROM runtime_storage_layouts WHERE runtime_home<>?",
            ""
        ),
        0
    );
    let prepared_envelope = f.envelope(prepare("/runtime/own", Some(&own)));
    let prepared = committed(&mut f, &mine, &prepared_envelope);
    assert_eq!(prepared["state"], "prepared");
    match f.writer.apply(&mine, &prepared_envelope) {
        SubmitResult::Committed(receipt) => {
            assert!(receipt.replayed);
            assert_eq!(receipt.result, prepared);
        }
        other => panic!("not replayed: {other:?}"),
    }
    let token = f
        .text(
            "SELECT operation_token FROM runtime_storage_layouts WHERE runtime_home=?",
            "/runtime/own",
        )
        .unwrap();
    let commit = |home: &str, token: &str| {
        RemoteOp::from(CommitRuntimeLayout {
            runtime_home: home.into(),
            operation_token: token.into(),
            layout_sha256: prepared["layout_sha256"].as_str().unwrap().into(),
        })
    };
    // Another execution's supervisor may not commit this layout.
    let theirs = supervisor(&f, &other);
    assert_eq!(
        refused(&mut f, &theirs, commit("/runtime/own", &token)),
        (RejectCode::Unauthorized, true)
    );
    // An unregistered home is refused before anything runs.
    let (code, durable) = refused(&mut f, &mine, commit("/runtime/missing", &token));
    assert_eq!((code, durable), (RejectCode::UnknownScope, false));
    let commit_envelope = f.envelope(commit("/runtime/own", &token));
    assert_eq!(
        committed(&mut f, &mine, &commit_envelope)["state"],
        "committed"
    );
    match f.writer.apply(&mine, &commit_envelope) {
        SubmitResult::Committed(receipt) => assert!(receipt.replayed),
        other => panic!("not replayed: {other:?}"),
    }
    // An unowned layout: the broker prepares and commits it, a supervisor
    // may not commit it.
    let broker = WriteAuthority::Broker;
    let unowned = f.envelope(prepare("/runtime/shared", None));
    let shared = committed(&mut f, &broker, &unowned);
    let shared_token = f
        .text(
            "SELECT operation_token FROM runtime_storage_layouts WHERE runtime_home=?",
            "/runtime/shared",
        )
        .unwrap();
    let shared_commit = RemoteOp::from(CommitRuntimeLayout {
        runtime_home: "/runtime/shared".into(),
        operation_token: shared_token.clone(),
        layout_sha256: shared["layout_sha256"].as_str().unwrap().into(),
    });
    assert_eq!(
        refused(&mut f, &mine, shared_commit.clone()),
        (RejectCode::Unauthorized, true)
    );
    assert_eq!(
        f.text(
            "SELECT state FROM runtime_storage_layouts WHERE runtime_home=?",
            "/runtime/shared"
        ),
        Some("prepared".into())
    );
    let shared_envelope = f.envelope(shared_commit);
    assert_eq!(
        committed(&mut f, &broker, &shared_envelope)["state"],
        "committed"
    );
    // A stale owner: the recorded owner changed after preparation, so the
    // former owner's supervisor is refused and the layout stays prepared.
    let stale = f.envelope(prepare("/runtime/moved", Some(&own)));
    let moved = committed(&mut f, &mine, &stale);
    f.sql(
        "UPDATE runtime_storage_layouts SET owner_agent_id=? WHERE runtime_home='/runtime/moved'",
        [other.as_str()],
    );
    let moved_token = f
        .text(
            "SELECT operation_token FROM runtime_storage_layouts WHERE runtime_home=?",
            "/runtime/moved",
        )
        .unwrap();
    assert_eq!(
        refused(
            &mut f,
            &mine,
            RemoteOp::from(CommitRuntimeLayout {
                runtime_home: "/runtime/moved".into(),
                operation_token: moved_token,
                layout_sha256: moved["layout_sha256"].as_str().unwrap().into(),
            })
        ),
        (RejectCode::Unauthorized, true)
    );
    assert_eq!(
        f.text(
            "SELECT state FROM runtime_storage_layouts WHERE runtime_home=?",
            "/runtime/moved"
        ),
        Some("prepared".into())
    );
}
