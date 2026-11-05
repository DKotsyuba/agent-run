//! Synthetic native evidence tests the store boundary, not end-to-end pausing.
mod common;

use agent_run_domain::{
    Error,
    domain::AgentId,
    worker::{
        FinishGuard, FinishRequest, FinishStatus, MaintenancePauseRequest,
        maintenance_pause_catalog_digest,
    },
};
use agent_run_store::{
    Store,
    maintenance_park::{MaintenancePark, NativeSettlement},
    writer::StoreReader,
};
use serde_json::{Value, json};

const ATTEMPT: &str = "park-attempt";
const TOKEN: &str = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
const CHALLENGE: &str = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
const SESSION: &str = "owned-native-session";
const ACCOUNT: &str = "acct-park";

/// A changed logical sequence cannot inherit a previously captured park proof.
#[test]
fn maintenance_park_refuses_changed_captured_sequence() {
    let mut fixture = Fixture::new();
    fixture
        .store
        .conn
        .execute(
            "UPDATE agents SET sequence=4 WHERE id=?",
            [fixture.run.as_str()],
        )
        .unwrap();
    assert!(fixture.park().is_err());
    assert_eq!(
        fixture.store.get(&fixture.run).unwrap().status.as_str(),
        "running"
    );
}

/// Complete synthetic durable evidence, without any actual process or provider.
struct Fixture {
    home: common::Home,
    store: Store<'static>,
    run: AgentId,
    epoch: i64,
    event: i64,
    input: MaintenancePauseRequest,
}

impl Fixture {
    /// Prepare one owned account-bound execution, receipt, cleanup and history.
    fn new() -> Self {
        Self::with_receipt(true)
    }

    /// Optionally withhold the independent native receipt observation.
    fn with_receipt(observed: bool) -> Self {
        Self::with_command(observed, true)
    }

    /// Optionally omit the native control command binding to test refusal.
    fn with_command(observed: bool, command: bool) -> Self {
        Self::with_protocol("codex", observed, command)
    }

    /// Prepare either supported harness without invoking its real native runtime.
    fn with_protocol(protocol: &str, observed: bool, command: bool) -> Self {
        Self::with_options(protocol, observed, command, true)
    }

    /// Omit closing to exercise the boundary independently of native evidence.
    fn without_closing() -> Self {
        Self::with_options("codex", true, true, false)
    }

    /// Construct valid native evidence and optionally commit the closing fence.
    fn with_options(protocol: &str, observed: bool, command: bool, closing: bool) -> Self {
        let (harness, provider, family, turn, observation) = match protocol {
            "codex" => (
                "codex",
                "codex-fixture",
                "openai",
                "pause-turn",
                "native_accepted",
            ),
            "claude" => (
                "claude-code",
                "claude-fixture",
                "anthropic",
                "11111111-1111-4111-8111-111111111111",
                "stdin_written",
            ),
            _ => panic!("unsupported fixture protocol"),
        };
        let home = common::Home::new();
        let mut store = home.store();
        let account=serde_json::from_value(json!({"account_id":ACCOUNT,"auth_family":family,"secret_ref":"env:FIXTURE_PARK","status":"enabled"})).unwrap();
        store.register_account(&account).unwrap();
        let mut role = json!({"role_name":"review","role_revision":"fixture","prompt":"Review.","grants":{"write":false,"network":false,"allow_external_read_roots":false,"read_roots":[]},"skills":[],"mcp":[],"required_constraints":[],"auth":{"mode":"global","reference":null},"worker_mcp":true});
        role["config_revision"] = json!(agent_run_domain::canonical::sha256_hex(&role, true));
        let mut request = home.request();
        request.explicit_finish = true;
        let identity = json!({"provider_identity_version":2,"authority":{"role_payload":role,"provider":provider,"harness":harness,"assets_sha256":"c".repeat(64)}});
        let (run, _) = store
            .admit(&request, &home.config, &identity, None)
            .unwrap();
        store.conn.execute("UPDATE agents SET status='running',runtime_session_id=?,process_group_id=12345,selection_intent='pinned',sequence=3 WHERE id=?",rusqlite::params![SESSION,run.as_str()]).unwrap();
        store.conn.execute("INSERT INTO attempts(id,agent_id,number,state,adapter_state_json,created_at,ownership_active,selected_account_id,process_identity,process_birth_time) VALUES(?,?,1,'running','{}',1,1,?,'fixture-process',1)",rusqlite::params![ATTEMPT,run.as_str(),ACCOUNT]).unwrap();
        store
            .conn
            .execute(
                "INSERT INTO attempt_quota_keys(attempt_id,quota_key) VALUES(?,?)",
                rusqlite::params![ATTEMPT, format!("{ACCOUNT}::tokens")],
            )
            .unwrap();
        store.conn.execute("INSERT INTO pools(id,request_namespace,request_id,request_sha256,goal,acceptance_json,state,roster_revision,created_at) VALUES('pool-maintenance','fixture','pool',?,'Review','[]','open',1,1)",["0".repeat(64)]).unwrap();
        store.conn.execute("INSERT INTO pool_members(agent_id,pool_id,slot,name,role,personal_task,joined_roster_revision) VALUES(?,'pool-maintenance',1,'worker','review','task',1)",[run.as_str()]).unwrap();
        store
            .issue_worker_capability(&run, ATTEMPT, TOKEN, 1.0)
            .unwrap();
        store.begin_worker_lifecycle(&run, ATTEMPT, 1.0).unwrap();
        store.conn.execute("UPDATE worker_capabilities SET pool_catalog_version=5,pool_catalog_digest=?,pool_catalog_observed_at=1 WHERE attempt_id=?",rusqlite::params![maintenance_pause_catalog_digest(false),ATTEMPT]).unwrap();
        let epoch = store
            .begin_maintenance_fence("park-fixture", 2.0)
            .unwrap()
            .epoch;
        store
            .request_maintenance_checkpoint(epoch, &run, ATTEMPT, CHALLENGE, 3.0)
            .unwrap();
        if command {
            let command = store
                .request_maintenance_steer(epoch, &run, ATTEMPT, CHALLENGE, 3.0)
                .unwrap()
                .command_id;
            assert_eq!(
                store
                    .claim_command_for_turn(&run, false)
                    .unwrap()
                    .unwrap()
                    .0,
                command
            );
            store
                .complete_command(
                    &run,
                    command,
                    &json!({"accepted":true,"observation":observation}),
                )
                .unwrap();
        }
        let input = MaintenancePauseRequest {
            challenge_id: CHALLENGE.into(),
            checkpoint: "Verified work and retained session.".into(),
        };
        let receipt = store
            .accept_maintenance_checkpoint(
                epoch,
                &run,
                ATTEMPT,
                TOKEN,
                &input,
                4.0,
                FinishGuard::Clean,
            )
            .unwrap();
        if observed {
            store
                .observe_maintenance_checkpoint_receipt(
                    epoch,
                    &run,
                    ATTEMPT,
                    CHALLENGE,
                    &receipt.sha256,
                )
                .unwrap();
        }
        let evidence = NativeSettlement {
            version: 1,
            protocol: protocol.into(),
            coverage: "owned_foreground_v1".into(),
            maintenance_epoch: epoch,
            agent_id: run.clone(),
            attempt_id: ATTEMPT.into(),
            challenge_id: CHALLENGE.into(),
            receipt_sha256: receipt.sha256,
            thread_id: SESSION.into(),
            turn_id: turn.into(),
            item_id: "pause-item".into(),
        };
        store.conn.execute("INSERT INTO events(agent_id,attempt_id,at,kind,data_json) VALUES(?,?,5,'maintenance_native_settled_v1',?)",rusqlite::params![run.as_str(),ATTEMPT,serde_json::to_string(&evidence).unwrap()]).unwrap();
        let event = store.conn.last_insert_rowid();
        if observed && command && closing {
            assert!(
                store
                    .begin_maintenance_closing(epoch, &run, ATTEMPT, event, 5.5)
                    .unwrap()
            );
        }
        let cleanup = json!({"signals":[],"scope":"verified_descendants","group_gone":true,"descendants_gone":true,"confirmed":true,"process_group_id":12345});
        let history = json!({"native_history":{"attempt":ATTEMPT,"provider":provider,"assets_sha256":"c".repeat(64),"task_proven":true,"task_proof":{"turn":"original-task-turn","input_sha256":"d".repeat(64)},"seal":{"harness":harness,"session":SESSION,"root":home.path,"relative":"sessions/fixture.jsonl","bytes":128,"records":3,"sha256":"e".repeat(64)}}});
        store.conn.execute("UPDATE attempts SET phase='cleanup_complete',cleanup_proof_json=?,adapter_state_json=? WHERE id=?",rusqlite::params![cleanup.to_string(),history.to_string(),ATTEMPT]).unwrap();
        Self {
            home,
            store,
            run,
            epoch,
            event,
            input,
        }
    }

    /// Exercise the store transition only; synthetic evidence makes no native claim.
    fn park(&mut self) -> agent_run_domain::Result<MaintenancePark> {
        self.store
            .park_maintenance_attempt(self.epoch, &self.run, ATTEMPT, self.event, 6.0)
    }

    /// Change one JSON fact inside this disposable fixture's evidence.
    fn evidence(&self, field: &str, value: Value) {
        self.store
            .conn
            .execute(
                "UPDATE events SET data_json=json_set(data_json,?,json(?)) WHERE seq=?",
                rusqlite::params![format!("$.{field}"), value.to_string(), self.event],
            )
            .unwrap();
    }

    /// A refused transition leaves the active attempt and reservation intact.
    fn assert_unparked(&self) {
        assert!(
            self.store
                .maintenance_park(self.epoch, &self.run)
                .unwrap()
                .is_none()
        );
        let state: (String, bool) = self
            .store
            .conn
            .query_row(
                "SELECT state,ownership_active FROM attempts WHERE id=?",
                [ATTEMPT],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .unwrap();
        assert_eq!(state, ("running".into(), true));
    }
}

/// Parking preserves logical identity, pool, account and physical quota keys,
/// and makes no terminal answer/event/delivery. Exact retries retain one receipt.
#[test]
fn maintenance_park_preserves_logical_work_and_reserved_capacity() {
    let mut f = Fixture::new();
    let before = serde_json::to_value(f.store.get(&f.run).unwrap()).unwrap();
    let revision = f.store.quota_capacity_revision().unwrap();
    let events: i64 = f
        .store
        .conn
        .query_row("SELECT COUNT(*) FROM events", [], |row| row.get(0))
        .unwrap();
    let first = f.park().unwrap();
    assert_eq!(
        serde_json::to_value(f.store.get(&f.run).unwrap()).unwrap(),
        before
    );
    assert_eq!(f.store.quota_capacity_revision().unwrap(), revision);
    assert_eq!(
        f.store
            .conn
            .query_row("SELECT COUNT(*) FROM events", [], |row| row
                .get::<_, i64>(0))
            .unwrap(),
        events
    );
    let state:(String,bool,String,Option<f64>)=f.store.conn.query_row("SELECT state,ownership_active,selected_account_id,finished_at FROM attempts WHERE id=?",[ATTEMPT],|row|Ok((row.get(0)?,row.get(1)?,row.get(2)?,row.get(3)?))).unwrap();
    assert_eq!(
        state,
        ("maintenance_parked".into(), true, ACCOUNT.into(), None)
    );
    assert_eq!(
        f.store
            .conn
            .query_row(
                "SELECT quota_key FROM attempt_quota_keys WHERE attempt_id=?",
                [ATTEMPT],
                |row| row.get::<_, String>(0)
            )
            .unwrap(),
        format!("{ACCOUNT}::tokens")
    );
    assert_eq!(
        f.store
            .conn
            .query_row(
                "SELECT state FROM pools WHERE id='pool-maintenance'",
                [],
                |row| row.get::<_, String>(0)
            )
            .unwrap(),
        "open"
    );
    assert_eq!(
        f.store
            .conn
            .query_row(
                "SELECT COUNT(*) FROM pool_members WHERE agent_id=?",
                [f.run.as_str()],
                |row| row.get::<_, i64>(0)
            )
            .unwrap(),
        1
    );
    assert_eq!(
        f.store
            .conn
            .query_row("SELECT COUNT(*) FROM deliveries", [], |row| row
                .get::<_, i64>(0))
            .unwrap(),
        0
    );
    assert!(
        f.store
            .worker_finish_intent(&f.run, false)
            .unwrap()
            .is_none()
    );
    assert_eq!(
        f.store
            .park_maintenance_attempt(f.epoch, &f.run, ATTEMPT, f.event, 20.0)
            .unwrap(),
        first
    );
    let reader = StoreReader::open(&f.home.path).unwrap();
    assert!(
        reader
            .maintenance_checkpoint_intent(f.epoch, &f.run, ATTEMPT, true)
            .unwrap()
            .is_none()
    );
    assert_eq!(
        reader
            .parked_maintenance_checkpoint(f.epoch, &f.run, ATTEMPT)
            .unwrap(),
        Some(f.input.clone())
    );
    assert!(
        f.store
            .conn
            .execute(
                "UPDATE maintenance_parks SET proof_sha256=?",
                ["0".repeat(64)]
            )
            .is_err()
    );
}

/// Complete closed event identity is required; callback prose cannot substitute.
#[test]
fn maintenance_park_refuses_missing_foreign_and_unversioned_native_evidence() {
    for (field, value) in [
        ("version", json!(2)),
        ("protocol", json!("claude")),
        ("coverage", json!("best_effort")),
        ("maintenance_epoch", json!(999)),
        ("attempt_id", json!("other")),
        ("challenge_id", json!("0".repeat(64))),
        ("receipt_sha256", json!("0".repeat(64))),
        ("thread_id", json!("foreign")),
        ("turn_id", json!("")),
        ("item_id", json!("")),
        ("checkpoint", json!("raw worker prose is not evidence")),
    ] {
        let mut f = Fixture::new();
        f.evidence(field, value);
        assert!(f.park().is_err(), "{field}");
        f.assert_unparked();
    }
    let mut f = Fixture::new();
    f.store
        .conn
        .execute("DELETE FROM events WHERE seq=?", [f.event])
        .unwrap();
    assert!(f.park().is_err());
    f.assert_unparked();
    let mut f = Fixture::with_receipt(false);
    assert!(f.park().is_err());
    f.assert_unparked();
}

/// Cleanup must include the same process group and verified descendants, not
/// just a terminal-looking status or an isolated confirmed boolean.
#[test]
fn maintenance_park_requires_complete_cleanup_and_matching_history() {
    for (field, value) in [
        ("confirmed", json!(false)),
        ("group_gone", json!(false)),
        ("descendants_gone", Value::Null),
        ("scope", json!("process_group")),
        ("process_group_id", json!(42)),
    ] {
        let mut f = Fixture::new();
        f.store.conn.execute("UPDATE attempts SET cleanup_proof_json=json_set(cleanup_proof_json,?,json(?)) WHERE id=?",rusqlite::params![format!("$.{field}"),value.to_string(),ATTEMPT]).unwrap();
        assert!(f.park().is_err(), "{field}");
        f.assert_unparked();
    }
    for (field, value) in [
        ("task_proven", json!(false)),
        ("attempt", json!("other")),
        ("provider", json!("other")),
        ("assets_sha256", json!("0".repeat(64))),
        ("task_proof.input_sha256", json!("bad")),
        ("seal.session", json!("other")),
        ("seal.harness", json!("claude-code")),
        ("seal.relative", json!("../outside")),
        ("seal.sha256", json!("bad")),
        ("seal.bytes", json!(0)),
    ] {
        let mut f = Fixture::new();
        f.store.conn.execute("UPDATE attempts SET adapter_state_json=json_set(adapter_state_json,?,json(?)) WHERE id=?",rusqlite::params![format!("$.native_history.{field}"),value.to_string(),ATTEMPT]).unwrap();
        assert!(f.park().is_err(), "{field}");
        f.assert_unparked();
    }
}

/// Cancellation, a normal finish and a closed epoch always defeat new parking.
#[test]
fn maintenance_park_cancellation_and_real_finish_win() {
    for state in ["cancel", "finish", "closed"] {
        let mut f = if state == "finish" {
            Fixture::without_closing()
        } else {
            Fixture::new()
        };
        match state {
            "cancel" => {
                f.store.conn.execute("INSERT INTO commands(agent_id,kind,payload_json,state,created_at) VALUES(?,'cancel','{}','pending',5)",[f.run.as_str()]).unwrap();
            }
            "finish" => {
                f.store
                    .accept_worker_finish(
                        &f.run,
                        ATTEMPT,
                        TOKEN,
                        &FinishRequest {
                            summary: "Verified completion".into(),
                            status: FinishStatus::Done,
                        },
                        5.0,
                        FinishGuard::Clean,
                    )
                    .unwrap();
            }
            _ => {
                assert!(f.store.end_maintenance_fence(f.epoch, 6.0).is_err());
                // Model a stale/legacy closed fence independently of the
                // current release guard in this disposable database.
                f.store
                    .conn
                    .execute(
                        "UPDATE maintenance_fences SET ended_at=6,phase='complete',outcome='deferred' WHERE epoch=?",
                        [f.epoch],
                    )
                    .unwrap();
            }
        }
        assert!(f.park().is_err());
        f.assert_unparked();
    }
    let mut f = Fixture::new();
    f.park().unwrap();
    f.store.conn.execute("INSERT INTO commands(agent_id,kind,payload_json,state,created_at) VALUES(?,'cancel','{}','pending',7)",[f.run.as_str()]).unwrap();
    assert!(f.park().is_err());
    assert!(
        StoreReader::open(&f.home.path)
            .unwrap()
            .parked_maintenance_checkpoint(f.epoch, &f.run, ATTEMPT)
            .unwrap()
            .is_none()
    );
}

/// Replays cannot replace settled evidence or silently reseal changed history.
#[test]
fn maintenance_park_changed_evidence_conflicts_and_read_refuses() {
    let mut f = Fixture::new();
    f.park().unwrap();
    f.evidence("turn_id", json!("changed-turn"));
    assert!(matches!(f.park(), Err(Error::Conflict)));
    assert!(matches!(
        StoreReader::open(&f.home.path)
            .unwrap()
            .parked_maintenance_checkpoint(f.epoch, &f.run, ATTEMPT),
        Err(Error::Integrity(_))
    ));
    let mut f = Fixture::new();
    f.park().unwrap();
    assert!(matches!(
        f.store
            .park_maintenance_attempt(f.epoch, &f.run, ATTEMPT, f.event + 1, 8.0),
        Err(Error::Conflict)
    ));
    let mut f = Fixture::new();
    f.park().unwrap();
    f.store
        .conn
        .execute(
            "UPDATE events SET from_status='running' WHERE seq=?",
            [f.event],
        )
        .unwrap();
    assert!(matches!(
        StoreReader::open(&f.home.path)
            .unwrap()
            .parked_maintenance_checkpoint(f.epoch, &f.run, ATTEMPT),
        Err(Error::Integrity(_))
    ));
}

/// The park transition and later checkpoint read require the exact committed
/// closing marker, including its immutable event digest.
#[test]
fn maintenance_park_requires_unchanged_committed_closing_marker() {
    let mut missing = Fixture::without_closing();
    assert!(missing.park().is_err());
    missing.assert_unparked();

    let mut different = Fixture::new();
    assert!(matches!(
        different.store.park_maintenance_attempt(
            different.epoch,
            &different.run,
            ATTEMPT,
            different.event + 1,
            6.0
        ),
        Err(Error::Conflict)
    ));
    different.assert_unparked();

    let mut changed = Fixture::new();
    changed.park().unwrap();
    let replace =
        "UPDATE maintenance_checkpoints SET closing_event_sha256=? WHERE epoch=? AND agent_id=?";
    assert!(
        changed
            .store
            .conn
            .execute(
                replace,
                rusqlite::params!["0".repeat(64), changed.epoch, changed.run.as_str()]
            )
            .is_err()
    );
    // Simulate corrupted durable state in this isolated fixture after proving
    // the normal schema refuses marker edits.
    changed
        .store
        .conn
        .execute_batch("DROP TRIGGER maintenance_checkpoint_immutable")
        .unwrap();
    changed
        .store
        .conn
        .execute(
            replace,
            rusqlite::params!["0".repeat(64), changed.epoch, changed.run.as_str()],
        )
        .unwrap();
    assert!(changed.park().is_err());
    assert!(matches!(
        StoreReader::open(&changed.home.path)
            .unwrap()
            .parked_maintenance_checkpoint(changed.epoch, &changed.run, ATTEMPT),
        Err(Error::Integrity(_))
    ));
}

/// The outer writer transaction owns both the park row and attempt transition.
#[test]
fn maintenance_park_rolls_back_atomically() {
    let mut f = Fixture::new();
    f.store.conn.execute_batch("BEGIN IMMEDIATE").unwrap();
    f.park().unwrap();
    f.store.conn.execute_batch("ROLLBACK").unwrap();
    f.assert_unparked();
    assert_eq!(
        f.store.get(&f.run).unwrap().status,
        agent_run_domain::domain::Status::Running
    );
}

/// A stale agent-level steer cannot certify another attempt, and a written or
/// unknown control outcome does not substitute for the separately bound proof.
#[test]
fn maintenance_park_requires_exact_accepted_control_binding() {
    let mut missing = Fixture::with_command(true, false);
    assert!(missing.park().is_err());
    missing.assert_unparked();
    for mismatch in ["attempt", "epoch", "challenge", "outcome", "pending"] {
        let mut f = Fixture::new();
        match mismatch {
            "attempt" => {
                f.store.conn.execute("UPDATE commands SET payload_json=json_set(payload_json,'$.maintenance_attempt','foreign')",[]).unwrap();
            }
            "epoch" => {
                f.store.conn.execute("UPDATE commands SET payload_json=json_set(payload_json,'$.maintenance_epoch',999)",[]).unwrap();
            }
            "challenge" => {
                f.store.conn.execute("UPDATE commands SET payload_json=json_set(payload_json,'$.maintenance_challenge','foreign')",[]).unwrap();
            }
            "outcome" => {
                f.store
                    .conn
                    .execute("UPDATE commands SET result_json='{\"accepted\":null}'", [])
                    .unwrap();
            }
            _ => {
                f.store
                    .conn
                    .execute("UPDATE commands SET state='claimed'", [])
                    .unwrap();
            }
        }
        assert!(f.park().is_err(), "{mismatch}");
        f.assert_unparked();
    }
}

/// Claude evidence binds its own input UUID, tool_use id and sealed native
/// session while preserving the same logical account-bound execution.
#[test]
fn maintenance_park_accepts_exact_claude_session_input_and_tool_evidence() {
    let mut f = Fixture::with_protocol("claude", true, true);
    let before = serde_json::to_value(f.store.get(&f.run).unwrap()).unwrap();
    let first = f.park().unwrap();
    assert_eq!(f.park().unwrap(), first);
    assert_eq!(
        serde_json::to_value(f.store.get(&f.run).unwrap()).unwrap(),
        before
    );
    assert_eq!(
        StoreReader::open(&f.home.path)
            .unwrap()
            .parked_maintenance_checkpoint(f.epoch, &f.run, ATTEMPT)
            .unwrap(),
        Some(f.input)
    );
}

/// An unrelated/absent Claude input, missing tool_use or foreign session and
/// another harness's history cannot be interpreted as this pause's evidence.
#[test]
fn maintenance_park_refuses_incomplete_or_mismatched_claude_evidence() {
    for (field, value) in [
        ("turn_id", json!("not-an-input-uuid")),
        ("item_id", json!("")),
        ("thread_id", json!("foreign-session")),
        ("protocol", json!("unknown")),
    ] {
        let mut f = Fixture::with_protocol("claude", true, true);
        f.evidence(field, value);
        assert!(f.park().is_err(), "{field}");
        f.assert_unparked();
    }
    let mut f = Fixture::with_protocol("claude", true, true);
    f.store.conn.execute("UPDATE attempts SET adapter_state_json=json_set(adapter_state_json,'$.native_history.seal.harness','codex') WHERE id=?",[ATTEMPT]).unwrap();
    assert!(f.park().is_err());
    f.assert_unparked();
}
