//! In-transaction authority scope on disposable private homes: process
//! owners resolve their execution or generation from durable rows, attempt
//! payloads must name the supervisor's pinned owned attempt, a native latch
//! must name that attempt's selected account, rows acting on the current
//! owned attempt derive it and enforce the pin, process owner kinds are
//! confined per authority, and recorded process identities (bootstrap
//! leaders included) and authority births must equal the authenticated
//! principal.
//! Legitimate pinned, repinned and replayed submissions keep working.
mod common;

use agent_run_domain::{
    domain::AgentId,
    storage::{
        MonoDeadline, OperationId, ProducerId, ProducerKind, RejectCode, SubmitResult,
        WriteAuthority,
    },
};
use agent_run_platform::process::{Identity, OwnershipSnapshot};
use agent_run_store::{
    ops::{
        AttemptTarget, BootstrapServiceProcess, ClaimServiceProcessIdentity, EnqueueNativeWake,
        FinishAttempt, LatchNativeExhaustion, ObserveWorkerFinishReceipt, ProviderSpawning,
        RecordSupervisorOwner, RememberProcesses, RemoteOp, WorkerTurn, WriteEnvelope,
    },
    writer::{StoreWriter, WriterLease},
};
use rusqlite::params;
use serde_json::json;
use std::cell::Cell;

/// One test home with an open writer and a strictly increasing ordinal.
struct Fixture {
    /// Disposable private home.
    home: common::Home,
    /// Sole writer.
    writer: StoreWriter<'static>,
    /// Last used producer ordinal.
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

    /// Admits one starting execution without a startup owner and adds two
    /// attempts: `<id>:1` owned and selecting `acct-1`, `<id>:2` not owned
    /// and selecting `acct-2`.
    fn execution(&self) -> AgentId {
        let mut store = self.home.store();
        let (id, _) = store
            .admit(&self.home.request(), &self.home.config, &json!({}), None)
            .unwrap();
        store
            .conn
            .execute(
                "UPDATE agents SET startup_owner_pid_identity=NULL,startup_owner_birth_time=NULL WHERE id=?",
                [id.as_str()],
            )
            .unwrap();
        for (number, account, owned) in [(1, "acct-1", 1), (2, "acct-2", 0)] {
            store
                .conn
                .execute(
                    "INSERT OR REPLACE INTO attempts(id,agent_id,number,state,adapter_state_json,created_at,selected_account_id,ownership_active) \
                     VALUES(?,?,?,'running','{}',1,?,?)",
                    params![format!("{id}:{number}"), id.as_str(), number, account, owned],
                )
                .unwrap();
        }
        id
    }

    /// Records supervisor pid 10 / token `t` / birth 1.0 and capability `c`
    /// on `id`, and returns that supervisor pinned to `attempt`.
    fn bind(&self, id: &AgentId, attempt: Option<String>) -> WriteAuthority {
        self.home
            .store()
            .conn
            .execute(
                "UPDATE agents SET supervisor_capability_sha256=?,supervisor_pid=10,supervisor_identity='t',supervisor_birth_time=1.0 WHERE id=?",
                params!["c".repeat(64), id.as_str()],
            )
            .unwrap();
        supervisor(id, attempt, 1.0)
    }

    /// Inserts a starting service generation `id` with executor capability
    /// `e`, launching root pid 30 / token `r` / birth 3.0.
    fn generation(&self, id: &str) {
        self.home
            .store()
            .conn
            .execute(
                "INSERT INTO managed_service_generations(id,service_id,revision,definition_json,state,broker_identity_json,created_at,exec_capability_sha256) \
                 VALUES(?1,?1,?2,'{}','starting',?3,1,?4)",
                params![
                    id,
                    "a".repeat(64),
                    json!({"pid": 30, "token": "r", "birth": 3.0}).to_string(),
                    "e".repeat(64)
                ],
            )
            .unwrap();
    }

    /// Applies `op` under `authority` with the next ordinal.
    fn apply(&mut self, authority: &WriteAuthority, op: RemoteOp) -> SubmitResult {
        let envelope = self.envelope(op);
        self.writer.apply(authority, &envelope)
    }

    /// An envelope of `op` with a fresh identity and the next ordinal.
    fn envelope(&self, op: RemoteOp) -> WriteEnvelope {
        let (boot, now) = agent_run_platform::monotonic::now().unwrap();
        self.ordinal.set(self.ordinal.get() + 1);
        WriteEnvelope::new(
            OperationId::new(&boot, now),
            ProducerId {
                kind: ProducerKind::Broker,
                scope: Some("scope".into()),
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

    /// Counts rows of `sql` with one text parameter.
    fn count(&self, sql: &str, value: &str) -> i64 {
        self.home
            .store()
            .conn
            .query_row(sql, [value], |row| row.get(0))
            .unwrap()
    }
}

/// A supervisor of `id` (pid 10, token `t`, capability `c`) with `birth`.
fn supervisor(id: &AgentId, attempt: Option<String>, birth: f64) -> WriteAuthority {
    WriteAuthority::Supervisor {
        logical: id.clone(),
        execution: id.clone(),
        attempt,
        capability_sha256: "c".repeat(64),
        pid: 10,
        birth,
        token: "t".into(),
    }
}

/// The executor of generation `generation` (pid 20, token `s`) with `birth`
/// and launching-root `root_birth`.
fn executor(generation: &str, birth: f64, root_birth: f64) -> WriteAuthority {
    WriteAuthority::ServiceExec {
        generation: generation.into(),
        capability_sha256: "e".repeat(64),
        pid: 20,
        birth,
        token: "s".into(),
        root_pid: 30,
        root_birth,
        root_token: "r".into(),
    }
}

/// This test process's identity.
fn me() -> Identity {
    agent_run_platform::process::inspect(std::process::id() as i32).unwrap()
}

/// A process snapshot for owner `owner` of `kind`; attempts capture this
/// test process, other owners a distinct recorded identity (one process
/// identity never has two owners).
fn remember(kind: &str, owner: &str) -> RemoteOp {
    let me = if kind == "attempt" {
        me()
    } else {
        Identity {
            pid: 99_999,
            ppid: 1,
            group: 99_999,
            birth: 1.0,
            token: "darwin:1:1".into(),
            zombie: false,
        }
    };
    RemoteOp::from(RememberProcesses {
        kind: kind.into(),
        owner: owner.into(),
        snapshot: OwnershipSnapshot {
            leader: me.clone(),
            members: vec![me],
            descendants_observed: true,
        },
    })
}

/// Asserts a durable refusal with `code`.
fn refused(result: SubmitResult, code: RejectCode) {
    match result {
        SubmitResult::Rejected(refusal) => {
            assert_eq!(refusal.code, code, "{refusal:?}");
            assert!(refusal.durable, "{refusal:?}");
        }
        other => panic!("expected {code:?}, got {other:?}"),
    }
}

/// Asserts the result is not an authority-scope refusal.
fn in_scope(result: &SubmitResult) {
    if let SubmitResult::Rejected(refusal) = result {
        assert!(
            !matches!(
                refusal.code,
                RejectCode::Unauthorized | RejectCode::StaleAuthority | RejectCode::UnknownScope
            ),
            "{refusal:?}"
        );
    }
}

/// Counts recorded ownership of one owner.
const OWNED: &str = "SELECT COUNT(*) FROM process_ownership WHERE owner_id=?";

/// Process owners resolve their scope from rows: a supervisor cannot write
/// another execution's attempt or any service, an executor cannot write an
/// attempt or another generation, a missing owner is a non-durable unknown
/// scope; each authority's own owner commits.
#[test]
fn process_owner_scope_is_resolved_from_rows() {
    let mut f = Fixture::new();
    let a = f.execution();
    let b = f.execution();
    let sup = f.bind(&a, Some(format!("{a}:1")));
    f.generation("g1");
    f.generation("g2");
    let exec = executor("g1", 2.0, 3.0);
    let b1 = format!("{b}:1");
    refused(
        f.apply(&sup, remember("attempt", &b1)),
        RejectCode::Unauthorized,
    );
    refused(
        f.apply(&sup, remember("service", "g1")),
        RejectCode::Unauthorized,
    );
    let a1 = format!("{a}:1");
    refused(
        f.apply(&exec, remember("attempt", &a1)),
        RejectCode::Unauthorized,
    );
    refused(
        f.apply(&exec, remember("service", "g2")),
        RejectCode::Unauthorized,
    );
    match f.apply(&sup, remember("attempt", "missing")) {
        SubmitResult::Rejected(refusal) => {
            assert_eq!(refusal.code, RejectCode::UnknownScope);
            assert!(!refusal.durable);
        }
        other => panic!("unexpected {other:?}"),
    }
    assert_eq!(f.count(OWNED, &b1), 0);
    assert_eq!(f.count(OWNED, &a1), 0);
    assert_eq!(f.count(OWNED, "g2"), 0);
    assert!(matches!(
        f.apply(&sup, remember("attempt", &a1)),
        SubmitResult::Committed(_)
    ));
    let service = f.apply(&exec, remember("service", "g1"));
    assert!(matches!(service, SubmitResult::Committed(_)), "{service:?}");
    assert_eq!(f.count(OWNED, &a1), 1);
    assert_eq!(f.count(OWNED, "g1"), 1);
}

/// Attempt payloads must name the pinned owned attempt: another attempt of
/// the same execution or another execution's attempt is refused; without a
/// pin only the legacy first attempt is in scope; a repinned authority
/// reaches its new attempt and replays the original receipt.
#[test]
fn attempt_payloads_must_name_the_pinned_attempt() {
    let mut f = Fixture::new();
    let a = f.execution();
    let b = f.execution();
    let pinned = f.bind(&a, Some(format!("{a}:1")));
    let finish = |attempt: String| {
        RemoteOp::from(FinishAttempt {
            execution: a.clone(),
            attempt,
            state: "failed".into(),
        })
    };
    refused(
        f.apply(&pinned, finish(format!("{a}:2"))),
        RejectCode::Unauthorized,
    );
    let spawning = RemoteOp::from(ProviderSpawning {
        target: AttemptTarget {
            execution: a.clone(),
            attempt: format!("{b}:1"),
        },
    });
    refused(f.apply(&pinned, spawning), RejectCode::Unauthorized);
    let unpinned = supervisor(&a, None, 1.0);
    refused(
        f.apply(&unpinned, finish(format!("{a}:2"))),
        RejectCode::Unauthorized,
    );
    assert_eq!(
        f.count(
            "SELECT COUNT(*) FROM attempts WHERE id=? AND finished_at IS NULL",
            &format!("{a}:2")
        ),
        1
    );
    let a1 = format!("{a}:1");
    let first = f.envelope(remember("attempt", &a1));
    let SubmitResult::Committed(original) = f.writer.apply(&unpinned, &first) else {
        panic!("legacy attempt not in scope");
    };
    let repinned = supervisor(&a, Some(format!("{a}:2")), 1.0);
    let SubmitResult::Committed(replayed) = f.writer.apply(&repinned, &first) else {
        panic!("repinned replay refused");
    };
    assert!(replayed.replayed);
    assert_eq!(replayed.commit_seq, original.commit_seq);
    in_scope(&f.apply(&repinned, finish(format!("{a}:2"))));
}

/// A native latch must name the account selected by the pinned owned
/// attempt; a cross-account latch is refused before any mutation.
#[test]
fn native_latch_must_name_the_pinned_attempt_account() {
    let mut f = Fixture::new();
    let a = f.execution();
    let sup = f.bind(&a, Some(format!("{a}:1")));
    let latch = |account: &str| {
        RemoteOp::from(LatchNativeExhaustion {
            execution: a.clone(),
            account: account.parse().unwrap(),
            runtime: "claude".into(),
            lane: "default".into(),
            window: "five_hour".into(),
            source: "native".into(),
            models: ["model".to_owned()].into(),
            observed_at: 1.0,
            reset_at: None,
        })
    };
    refused(f.apply(&sup, latch("acct-2")), RejectCode::Unauthorized);
    let unpinned_other = supervisor(&a, Some(format!("{a}:2")), 1.0);
    refused(
        f.apply(&unpinned_other, latch("acct-2")),
        RejectCode::Unauthorized,
    );
    assert_eq!(
        f.count(
            "SELECT COUNT(*) FROM quota_exhaustion WHERE account_id=?",
            "acct-2"
        ),
        0
    );
    in_scope(&f.apply(&sup, latch("acct-1")));
}

/// A recorded supervisor identity must equal the authenticated principal's
/// pid, token and birth; a startup owner must match the full identity
/// including birth.
#[test]
fn supervisor_owner_identity_must_be_the_principal() {
    let mut f = Fixture::new();
    let a = f.execution();
    let sup = f.bind(&a, None);
    let owner = |pid: i32, identity: &str, birth: Option<f64>| {
        RemoteOp::from(RecordSupervisorOwner {
            execution: a.clone(),
            pid,
            identity: identity.into(),
            process_group_id: None,
            birth_time: birth,
            at: 2.0,
        })
    };
    for forged in [
        owner(11, "t", Some(1.0)),
        owner(10, "x", Some(1.0)),
        owner(10, "t", Some(2.0)),
        owner(10, "t", Some(1.0005)),
        owner(10, "t", None),
    ] {
        refused(f.apply(&sup, forged), RejectCode::Unauthorized);
    }
    in_scope(&f.apply(&sup, owner(10, "t", Some(1.0))));
    let c = f.execution();
    f.home
        .store()
        .conn
        .execute(
            "UPDATE agents SET supervisor_capability_sha256=?,startup_owner_pid_identity=? WHERE id=?",
            params![
                "c".repeat(64),
                json!({"pid": 10, "token": "t", "birth": 1.0}).to_string(),
                c.as_str()
            ],
        )
        .unwrap();
    let start = |birth: f64| {
        RemoteOp::from(RecordSupervisorOwner {
            execution: c.clone(),
            pid: 10,
            identity: "t".into(),
            process_group_id: None,
            birth_time: Some(birth),
            at: 2.0,
        })
    };
    refused(
        f.apply(&supervisor(&c, None, 5.0), start(5.0)),
        RejectCode::StaleAuthority,
    );
    assert!(matches!(
        f.apply(&supervisor(&c, None, 1.0), start(1.0)),
        SubmitResult::Committed(_)
    ));
}

/// A claimed service process identity must equal the authenticated
/// executor; executor and launching-root births are compared exactly, so
/// even a sub-millisecond difference refuses.
#[test]
fn service_identity_and_births_must_be_the_principal() {
    let mut f = Fixture::new();
    f.generation("g1");
    let claim = |pid: i32, birth: f64| {
        RemoteOp::from(ClaimServiceProcessIdentity {
            generation: "g1".into(),
            identity: Identity {
                pid,
                ppid: 1,
                group: pid,
                birth,
                token: "s".into(),
                zombie: false,
            },
        })
    };
    let exec = executor("g1", 2.0, 3.0);
    refused(f.apply(&exec, claim(21, 2.0)), RejectCode::Unauthorized);
    refused(f.apply(&exec, claim(20, 9.0)), RejectCode::Unauthorized);
    refused(f.apply(&exec, claim(20, 2.0005)), RejectCode::Unauthorized);
    refused(
        f.apply(&executor("g1", 2.0, 4.0), claim(20, 2.0)),
        RejectCode::StaleAuthority,
    );
    assert_eq!(
        f.count(
            "SELECT COUNT(*) FROM managed_service_generations WHERE id=? AND process_identity_json IS NULL",
            "g1"
        ),
        1
    );
    assert!(matches!(
        f.apply(&exec, claim(20, 2.0)),
        SubmitResult::Committed(_)
    ));
    refused(
        f.apply(&executor("g1", 2.5, 3.0), remember("service", "g1")),
        RejectCode::StaleAuthority,
    );
    refused(
        f.apply(&executor("g1", 2.0005, 3.0), remember("service", "g1")),
        RejectCode::StaleAuthority,
    );
}

/// Rows acting on the execution's current owned attempt derive it inside the
/// transaction: a principal pinned to a stale attempt is refused, the
/// principal pinned to the current owned attempt is in scope.
#[test]
fn derived_owned_attempt_must_be_the_pinned_attempt() {
    let mut f = Fixture::new();
    let a = f.execution();
    let stale = f.bind(&a, Some(format!("{a}:1")));
    f.home
        .store()
        .conn
        .execute(
            "UPDATE attempts SET ownership_active=CASE number WHEN 2 THEN 1 ELSE 0 END WHERE agent_id=?",
            [a.as_str()],
        )
        .unwrap();
    let rows = || {
        [
            RemoteOp::from(WorkerTurn {
                execution: a.clone(),
                idle: true,
                at: 2.0,
            }),
            RemoteOp::from(EnqueueNativeWake {
                execution: a.clone(),
                digest: "d".repeat(64),
                text: "wake".into(),
                at: 2.0,
            }),
            RemoteOp::from(ObserveWorkerFinishReceipt {
                execution: a.clone(),
            }),
        ]
    };
    for op in rows() {
        refused(f.apply(&stale, op), RejectCode::Unauthorized);
    }
    let current = supervisor(&a, Some(format!("{a}:2")), 1.0);
    for op in rows() {
        in_scope(&f.apply(&current, op));
    }
}

/// A bootstrap snapshot's leader must be the authenticated executor; a
/// foreign leader is refused before anything is recorded.
#[test]
fn bootstrap_leader_must_be_the_executor() {
    let mut f = Fixture::new();
    f.generation("g1");
    let bootstrap = |pid: i32, token: &str, birth: f64| {
        let leader = Identity {
            pid,
            ppid: 1,
            group: pid,
            birth,
            token: token.into(),
            zombie: false,
        };
        RemoteOp::from(BootstrapServiceProcess {
            generation: "g1".into(),
            snapshot: OwnershipSnapshot {
                leader: leader.clone(),
                members: vec![leader],
                descendants_observed: true,
            },
        })
    };
    let exec = executor("g1", 2.0, 3.0);
    refused(
        f.apply(&exec, bootstrap(99_999, "s", 2.0)),
        RejectCode::Unauthorized,
    );
    refused(
        f.apply(&exec, bootstrap(20, "other", 2.0)),
        RejectCode::Unauthorized,
    );
    refused(
        f.apply(&exec, bootstrap(20, "s", 2.5)),
        RejectCode::Unauthorized,
    );
    assert_eq!(f.count(OWNED, "g1"), 0);
    let recorded = f.apply(&exec, bootstrap(20, "s", 2.0));
    assert!(
        matches!(recorded, SubmitResult::Committed(_)),
        "{recorded:?}"
    );
    assert_eq!(f.count(OWNED, "g1"), 1);
}

/// Process owner kinds are confined per authority: an executor may record
/// only its own service generation, never a probe of it; probes stay
/// broker-only.
#[test]
fn executors_cannot_record_probes() {
    let mut f = Fixture::new();
    f.generation("g1");
    f.home
        .store()
        .conn
        .execute(
            "INSERT INTO managed_service_probes(id,generation_id,created_at) VALUES('p1','g1',1)",
            [],
        )
        .unwrap();
    refused(
        f.apply(&executor("g1", 2.0, 3.0), remember("probe", "p1")),
        RejectCode::Unauthorized,
    );
    assert_eq!(f.count(OWNED, "p1"), 0);
    let broker = f.apply(&WriteAuthority::Broker, remember("probe", "p1"));
    assert!(matches!(broker, SubmitResult::Committed(_)), "{broker:?}");
}
