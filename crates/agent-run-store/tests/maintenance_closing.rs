//! Store-only closing evidence tests; no native process or restart is involved.
mod common;

use agent_run_domain::{
    Error,
    domain::AgentId,
    worker::{
        FinishGuard, FinishRequest, FinishStatus, MaintenancePauseRequest, WorkerCatalogProof,
        maintenance_pause_catalog_digest,
    },
};
use agent_run_store::{Store, maintenance_park::NativeSettlement};
use serde_json::{Value, json};

const ATTEMPT: &str = "closing-attempt";
const TOKEN: &str = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
const CHALLENGE: &str = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
const SESSION: &str = "closing-native-session";
const CHECKPOINT: &str = "Retained context without a final answer.";

/// An isolated checkpoint and simulated native settlement for one live attempt.
struct Fixture {
    _home: common::Home,
    store: Store<'static>,
    run: AgentId,
    epoch: i64,
    command: i64,
    event: i64,
}

impl Fixture {
    /// Record a broker challenge, accepted receipt and the chosen native proof.
    fn new(protocol: &str) -> Self {
        Self::with_receipt(protocol, true)
    }

    /// Omit observation to exercise the independent durable receipt gate.
    fn with_receipt(protocol: &str, observed: bool) -> Self {
        let (harness, provider, turn, observation) = match protocol {
            "codex" => ("codex", "codex-fixture", "turn-id", "native_accepted"),
            "claude" => (
                "claude-code",
                "claude-fixture",
                "11111111-1111-4111-8111-111111111111",
                "stdin_written",
            ),
            _ => panic!("unsupported fixture protocol"),
        };
        let home = common::Home::new();
        let mut store = home.store();
        let mut role = json!({"role_name":"review","role_revision":"fixture","prompt":"Review.","grants":{"write":false,"network":false,"allow_external_read_roots":false,"read_roots":[]},"skills":[],"mcp":[],"required_constraints":[],"auth":{"mode":"global","reference":null},"worker_mcp":true});
        role["config_revision"] = json!(agent_run_domain::canonical::sha256_hex(&role, true));
        let mut request = home.request();
        request.explicit_finish = true;
        request.output_schema = Some(
            json!({"type":"object","required":["answer"]})
                .as_object()
                .unwrap()
                .clone(),
        );
        let identity = json!({"provider_identity_version":2,"authority":{"role_payload":role,"provider":provider,"harness":harness,"assets_sha256":"c".repeat(64)}});
        let (run, _) = store
            .admit(&request, &home.config, &identity, None)
            .unwrap();
        store
            .conn
            .execute(
                "UPDATE agents SET status='running',runtime_session_id=?,sequence=3 WHERE id=?",
                rusqlite::params![SESSION, run.as_str()],
            )
            .unwrap();
        store.conn.execute("INSERT INTO attempts(id,agent_id,number,state,adapter_state_json,created_at,ownership_active) VALUES(?,?,1,'running','{}',1,1)",rusqlite::params![ATTEMPT,run.as_str()]).unwrap();
        store
            .issue_worker_capability(&run, ATTEMPT, TOKEN, 1.0)
            .unwrap();
        store.begin_worker_lifecycle(&run, ATTEMPT, 1.0).unwrap();
        store
            .register_worker_catalog(
                &WorkerCatalogProof {
                    run_id: run.clone(),
                    attempt_id: ATTEMPT.into(),
                    token: TOKEN.into(),
                    version: 5,
                    digest: maintenance_pause_catalog_digest(false),
                },
                1.0,
            )
            .unwrap();
        let epoch = store
            .begin_maintenance_fence("closing-fixture", 2.0)
            .unwrap()
            .epoch;
        let command = store
            .request_maintenance_steer(epoch, &run, ATTEMPT, CHALLENGE, 3.0)
            .unwrap()
            .command_id;
        let claimed = store.claim_command_for_turn(&run, false).unwrap().unwrap();
        assert_eq!(claimed.0, command);
        store
            .complete_command(
                &run,
                command,
                &json!({"accepted":true,"observation":observation}),
            )
            .unwrap();
        let input = MaintenancePauseRequest {
            challenge_id: CHALLENGE.into(),
            checkpoint: CHECKPOINT.into(),
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
            item_id: "item-id".into(),
        };
        store.conn.execute("INSERT INTO events(agent_id,attempt_id,at,kind,data_json) VALUES(?,?,5,'maintenance_native_settled_v1',?)",rusqlite::params![run.as_str(),ATTEMPT,serde_json::to_string(&evidence).unwrap()]).unwrap();
        let event = store.conn.last_insert_rowid();
        Self {
            _home: home,
            store,
            run,
            epoch,
            command,
            event,
        }
    }

    /// Cross the store boundary without stopping, parking or completing work.
    fn close(&mut self) -> agent_run_domain::Result<bool> {
        self.store
            .begin_maintenance_closing(self.epoch, &self.run, ATTEMPT, self.event, 6.0)
    }

    /// Pending cancellation always takes precedence over maintenance.
    fn cancel(&self) {
        self.store.conn.execute("INSERT INTO commands(agent_id,kind,payload_json,state,created_at) VALUES(?,'cancel','{}','pending',5)",[self.run.as_str()]).unwrap();
    }

    /// The closing path must not complete or automatically resume this work.
    fn assert_live_without_finish(&self) {
        let row:(String,String,bool,Option<String>,i64)=self.store.conn.query_row(
            "SELECT a.status,t.state,t.ownership_active,l.finish_json,(SELECT COUNT(*) FROM maintenance_resumes WHERE epoch=? AND agent_id=?) FROM agents a JOIN attempts t ON t.agent_id=a.id JOIN worker_lifecycle l ON l.attempt_id=t.id WHERE a.id=? AND t.id=?",
            rusqlite::params![self.epoch,self.run.as_str(),self.run.as_str(),ATTEMPT],
            |r|Ok((r.get(0)?,r.get(1)?,r.get(2)?,r.get(3)?,r.get(4)?)),
        ).unwrap();
        assert_eq!(row, ("running".into(), "running".into(), true, None, 0));
    }

    /// Return only the durable closing marker, never checkpoint content.
    fn marker(&self) -> (Option<i64>, Option<String>, Option<f64>) {
        self.store.conn.query_row("SELECT closing_event_seq,closing_event_sha256,closing_at FROM maintenance_checkpoints WHERE epoch=? AND agent_id=? AND attempt_id=?",rusqlite::params![self.epoch,self.run.as_str(),ATTEMPT],|r|Ok((r.get(0)?,r.get(1)?,r.get(2)?))).unwrap()
    }

    /// Change a synthetic event fact without placing its contents in errors.
    fn evidence(&self, field: &str, value: Value) {
        self.store
            .conn
            .execute(
                "UPDATE events SET data_json=json_set(data_json,?,json(?)) WHERE seq=?",
                rusqlite::params![format!("$.{field}"), value.to_string(), self.event],
            )
            .unwrap();
    }

    /// Accept a genuine terminal result, independently of the pause checkpoint.
    fn finish(&mut self) -> agent_run_domain::Result<agent_run_domain::worker::FinishReceipt> {
        self.store.accept_worker_finish(
            &self.run,
            ATTEMPT,
            TOKEN,
            &FinishRequest {
                summary: "{\"answer\":\"Task complete\"}".into(),
                status: FinishStatus::Done,
            },
            5.5,
            FinishGuard::Clean,
        )
    }
}

/// Both exact native protocols bind one immutable event without parking or success.
#[test]
fn maintenance_closing_commits_once_for_each_native_protocol() {
    for protocol in ["codex", "claude"] {
        let mut f = Fixture::new(protocol);
        assert!(f.close().unwrap());
        let marker = f.marker();
        assert_eq!(marker.0, Some(f.event));
        assert_eq!(marker.1.as_ref().unwrap().len(), 64);
        assert_eq!(marker.2, Some(6.0));
        assert!(!f.close().unwrap());
        assert_eq!(f.marker(), marker);
        assert!(matches!(
            f.store
                .begin_maintenance_closing(f.epoch, &f.run, ATTEMPT, f.event + 1, 7.0),
            Err(Error::Conflict)
        ));
        f.assert_live_without_finish();
    }
}

/// Closing retains ordinary queued input for the resumed attempt while a
/// cancellation remains immediately claimable.
#[test]
fn maintenance_closing_holds_new_inputs_but_allows_cancel() {
    let mut f = Fixture::new("codex");
    assert!(f.close().unwrap());
    let steer = f
        .store
        .enqueue(&f.run, "steer", &json!({"text":"continue later"}))
        .unwrap();
    let reader = agent_run_store::writer::StoreReader::open(&f._home.path).unwrap();
    assert!(!reader.command_pending(&f.run, true).unwrap());
    assert!(f.store.claim_command(&f.run).unwrap().is_none());
    f.cancel();
    assert!(reader.command_pending(&f.run, true).unwrap());
    let (id, kind, _) = f.store.claim_command(&f.run).unwrap().unwrap();
    assert_eq!(kind, "cancel");
    f.store
        .complete_command(&f.run, id, &json!({"accepted":true}))
        .unwrap();
    assert!(f.store.claim_command(&f.run).unwrap().is_none());
    assert_eq!(
        f.store
            .conn
            .query_row(
                "SELECT state FROM commands WHERE id=?",
                [steer["command_id"].as_i64().unwrap()],
                |r| r.get::<_, String>(0)
            )
            .unwrap(),
        "pending"
    );
}

/// Even a different well-formed event at the same sequence is not an exact replay.
#[test]
fn maintenance_closing_refuses_event_replacement_after_commit() {
    let mut f = Fixture::new("codex");
    assert!(f.close().unwrap());
    let marker = f.marker();
    f.evidence("turn_id", json!("another-valid-turn"));
    assert!(matches!(f.close(), Err(Error::Integrity(_))));
    assert_eq!(f.marker(), marker);
    f.assert_live_without_finish();
}

/// A genuine terminal finish wins when accepted first; closing wins otherwise.
#[test]
fn maintenance_closing_and_real_finish_have_atomic_ordering() {
    let mut finished = Fixture::new("codex");
    finished.finish().unwrap();
    assert!(finished.close().is_err());
    assert_eq!(finished.marker(), (None, None, None));

    let mut closing = Fixture::new("codex");
    assert!(closing.close().unwrap());
    assert!(closing.finish().is_err());
    closing.assert_live_without_finish();
}

/// A cancellation before closing or before replay never becomes success/resume.
#[test]
fn maintenance_closing_cancel_wins_before_and_after_commit() {
    for before in [true, false] {
        let mut f = Fixture::new("codex");
        if before {
            f.cancel();
        } else {
            assert!(f.close().unwrap());
            f.cancel();
        }
        assert!(f.close().is_err());
        assert_eq!(f.marker().0.is_some(), !before);
        f.assert_live_without_finish();
    }
}

/// Missing, changed, or in-flight evidence must hold the pause without leaking it.
#[test]
fn maintenance_closing_refuses_incomplete_or_mismatched_evidence() {
    for defect in [
        "receipt",
        "sequence",
        "catalog",
        "command_text",
        "command_result",
        "claimed",
        "wake",
        "event_session",
        "event_challenge",
        "event_digest",
        "event_shape",
        "event_time",
    ] {
        let mut f = Fixture::with_receipt("codex", defect != "receipt");
        match defect {
            "receipt" => {}
            "sequence" => {
                f.store
                    .conn
                    .execute(
                        "UPDATE agents SET sequence=sequence+1 WHERE id=?",
                        [f.run.as_str()],
                    )
                    .unwrap();
            }
            "catalog" => {
                f.store
                    .conn
                    .execute(
                        "UPDATE worker_capabilities SET pool_catalog_version=4 WHERE attempt_id=?",
                        [ATTEMPT],
                    )
                    .unwrap();
            }
            "command_text" => {
                f.store.conn.execute("UPDATE commands SET payload_json=json_set(payload_json,'$.text','changed') WHERE id=?",[f.command]).unwrap();
            }
            "command_result" => {
                f.store
                    .conn
                    .execute(
                        "UPDATE commands SET result_json='{\"accepted\":true}' WHERE id=?",
                        [f.command],
                    )
                    .unwrap();
            }
            "claimed" => {
                f.store.conn.execute("INSERT INTO commands(agent_id,kind,payload_json,state,created_at,claimed_at) VALUES(?,'steer','{}','claimed',5,5)",[f.run.as_str()]).unwrap();
            }
            "wake" => {
                f.store.conn.execute("INSERT INTO commands(agent_id,kind,payload_json,state,created_at) VALUES(?,'steer','{\"native_completion\":true}','pending',5)",[f.run.as_str()]).unwrap();
            }
            "event_session" => f.evidence("thread_id", json!("different-session")),
            "event_challenge" => f.evidence("challenge_id", json!("different-challenge")),
            "event_digest" => f.evidence("receipt_sha256", json!("0".repeat(64))),
            "event_shape" => f.evidence("unexpected", json!(CHECKPOINT)),
            "event_time" => {
                f.store
                    .conn
                    .execute("UPDATE events SET at=3 WHERE seq=?", [f.event])
                    .unwrap();
            }
            _ => unreachable!(),
        }
        let error = f.close().unwrap_err().to_string();
        assert!(!error.contains(CHECKPOINT));
        assert!(!error.contains(CHALLENGE));
        assert!(!error.contains(TOKEN));
        assert_eq!(f.marker(), (None, None, None), "{defect}");
        f.assert_live_without_finish();
    }
}
