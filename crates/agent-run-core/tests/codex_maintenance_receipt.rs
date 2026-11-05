//! Synthetic native frames with the real pinned supervisor writer protocol.
mod common;
use agent_run_core::{
    codex::{
        maintenance::{Assessment, Gap, Observer},
        maintenance_receipt::{self, ControlBinding},
    },
    domain::AgentId,
};
use agent_run_domain::{
    Error,
    storage::MonoDeadline,
    worker::{FinishGuard, MaintenancePauseRequest, maintenance_pause_catalog_digest},
};
use agent_run_store::Store;
use serde_json::{Value, json};

const ATTEMPT: &str = "receipt-attempt";
const TOKEN: &str = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
const THREAD: &str = "owned-thread";
const TURN: &str = "owned-control-turn";

/// Durable command/callback setup without any external tool, process or model.
struct Fixture {
    home: common::Home,
    store: Store<'static>,
    run: AgentId,
    epoch: i64,
    binding: ControlBinding,
    input: MaintenancePauseRequest,
    digest: String,
    payload: Value,
    command: i64,
}

impl Fixture {
    /// Claim a synthetic control, retain its binding and accept the exact callback.
    fn new() -> Self {
        let home = common::Home::new();
        let mut store = home.store();
        let mut role = common::role_payload();
        role.as_object_mut().unwrap().remove("config_revision");
        role["worker_mcp"] = json!(true);
        role["config_revision"] = json!(agent_run_domain::canonical::sha256_hex(&role, true));
        let mut request = home.request();
        request.explicit_finish = true;
        let (run, _) = store
            .admit(
                &request,
                &home.config,
                &json!({"provider_identity_version":2,"authority":{"role_payload":role,"harness":"codex"}}),
                None,
            )
            .unwrap();
        store
            .conn
            .execute(
                "UPDATE agents SET status='running',runtime_session_id=? WHERE id=?",
                rusqlite::params![THREAD, run.as_str()],
            )
            .unwrap();
        store.conn.execute("INSERT INTO attempts(id,agent_id,number,state,adapter_state_json,created_at,ownership_active) VALUES(?,?,1,'running','{}',1,1)",rusqlite::params![ATTEMPT,run.as_str()]).unwrap();
        store
            .issue_worker_capability(&run, ATTEMPT, TOKEN, 1.0)
            .unwrap();
        store.begin_worker_lifecycle(&run, ATTEMPT, 1.0).unwrap();
        store.conn.execute("UPDATE worker_capabilities SET pool_catalog_version=5,pool_catalog_digest=?,pool_catalog_observed_at=1 WHERE attempt_id=?",rusqlite::params![maintenance_pause_catalog_digest(false),ATTEMPT]).unwrap();
        let epoch = store
            .begin_maintenance_fence("receipt-fixture", 2.0)
            .unwrap()
            .epoch;
        let input = MaintenancePauseRequest {
            challenge_id: "b".repeat(64),
            checkpoint: "Retain verified task context.".into(),
        };
        let steer = store
            .request_maintenance_steer(epoch, &run, ATTEMPT, &input.challenge_id, 3.0)
            .unwrap();
        let (command, _, payload) = store.claim_command(&run).unwrap().unwrap();
        assert_eq!(command, steer.command_id);
        let binding =
            maintenance_receipt::claimed_control(&store, &run, ATTEMPT, command, &payload)
                .unwrap()
                .unwrap();
        store.conn.execute("UPDATE commands SET state='completed',completed_at=3,result_json='{\"accepted\":true,\"observation\":\"native_accepted\"}' WHERE id=?",[command]).unwrap();
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
        Self {
            home,
            store,
            run,
            epoch,
            binding,
            input,
            digest: receipt.sha256,
            payload,
            command,
        }
    }

    /// Observe the actual native start/result shape with an independently stored digest.
    fn observer(&self) -> Observer {
        let mut observer = Observer::new(THREAD, false);
        observer.observe(&json!({"method":"item/started","params":{"threadId":THREAD,"turnId":TURN,"item":{"id":"pause-item","type":"mcpToolCall","server":"agent_run_worker","tool":"maintenance_pause","arguments":self.input}}}),TURN);
        observer.observe(&json!({"method":"item/completed","params":{"threadId":THREAD,"turnId":TURN,"item":{"id":"pause-item","type":"mcpToolCall","server":"agent_run_worker","tool":"maintenance_pause","status":"completed","result":{"content":[{"type":"text","text":json!({"sha256":self.digest,"duplicate":false}).to_string()}]}}}}),TURN);
        observer
    }
}

/// Only a bound native control and independently accepted callback persist one
/// receipt, without finishing the task; the later turn completes its correlation.
#[tokio::test]
async fn codex_maintenance_receipt_writes_once_through_pinned_supervisor() {
    let mut f = Fixture::new();
    let prep = common::writer::prepare_supervisor(&mut f.store, &f.run);
    let harness = common::writer::Harness::start(&f.home.path);
    let mut persist = harness.supervisor(&f.home.path, prep, &f.run).await;
    let binding = f.binding.clone().accepted_turn(TURN);
    let mut observer = f.observer();
    assert!(observer.settled_checkpoint().is_none());
    assert!(
        maintenance_receipt::native_settlement(&persist, &f.run, &binding, &observer)
            .unwrap()
            .is_none()
    );
    assert!(
        maintenance_receipt::observe(&mut persist, &f.run, Some(&binding), &mut observer)
            .await
            .unwrap()
    );
    assert!(
        !maintenance_receipt::observe(&mut persist, &f.run, Some(&binding), &mut observer)
            .await
            .unwrap()
    );
    assert_eq!(observer.assessment(), Assessment::AwaitingTurnCompletion);
    observer.observe(&json!({"method":"turn/completed","params":{"threadId":THREAD,"turn":{"id":TURN,"status":"completed"}}}),TURN);
    let settled = observer.settled_checkpoint().unwrap();
    assert_eq!(
        (settled.thread_id, settled.turn_id, settled.item_id),
        (THREAD, TURN, "pause-item")
    );
    assert_eq!(settled.challenge_id, f.input.challenge_id);
    assert_eq!(settled.receipt_sha256, f.digest);
    let proof = maintenance_receipt::native_settlement(&persist, &f.run, &binding, &observer)
        .unwrap()
        .unwrap();
    assert_eq!(
        (
            proof.maintenance_epoch,
            proof.agent_id.clone(),
            proof.attempt_id.as_str()
        ),
        (f.epoch, f.run.clone(), ATTEMPT)
    );
    assert_eq!(
        (
            proof.thread_id.as_str(),
            proof.turn_id.as_str(),
            proof.item_id.as_str()
        ),
        (THREAD, TURN, "pause-item")
    );
    let debug = format!("{proof:?}");
    assert!(
        !debug.contains(THREAD)
            && !debug.contains(&f.input.checkpoint)
            && !debug.contains(&f.input.challenge_id)
    );
    assert_eq!(f.store.conn.query_row("SELECT COUNT(*) FROM operation_receipts WHERE op_kind='observe_maintenance_checkpoint'",[],|row|row.get::<_,i64>(0)).unwrap(),1);
    assert!(
        f.store
            .maintenance_checkpoint_intent(f.epoch, &f.run, ATTEMPT, true)
            .unwrap()
            .is_some()
    );
    assert_eq!(
        f.store.get(&f.run).unwrap().status,
        agent_run_domain::domain::Status::Running
    );
    assert!(
        f.store
            .worker_finish_intent(&f.run, false)
            .unwrap()
            .is_none()
    );
    harness
        .broker()
        .lane("codex-maintenance-cancel")
        .remote(
            agent_run_store::ops::EnqueueCommand {
                execution: f.run.clone(),
                kind: "cancel".into(),
                payload: json!({}),
            },
            std::time::Duration::from_secs(5),
        )
        .await
        .unwrap();
    assert!(
        maintenance_receipt::native_settlement(&persist, &f.run, &binding, &observer)
            .unwrap()
            .is_none()
    );
}

/// A command which may have been sent, an ordinary steer or a stale attempt
/// cannot arm an acknowledgement merely because a plausible callback arrived.
#[tokio::test]
async fn codex_maintenance_receipt_refuses_unbound_or_unknown_control() {
    let mut f = Fixture::new();
    assert!(!maintenance_receipt::marked(&json!({"text":"ordinary"})));
    assert!(
        maintenance_receipt::claimed_control(
            &f.store,
            &f.run,
            ATTEMPT,
            f.command,
            &json!({"text":"ordinary"})
        )
        .unwrap()
        .is_none()
    );
    assert!(
        maintenance_receipt::claimed_control(
            &f.store,
            &f.run,
            "foreign-attempt",
            f.command,
            &f.payload
        )
        .unwrap()
        .is_none()
    );
    let prep = common::writer::prepare_supervisor(&mut f.store, &f.run);
    let harness = common::writer::Harness::start(&f.home.path);
    let mut persist = harness.supervisor(&f.home.path, prep, &f.run).await;
    let mut observer = f.observer();
    assert!(
        !maintenance_receipt::observe(&mut persist, &f.run, None, &mut observer)
            .await
            .unwrap()
    );
    assert!(
        !maintenance_receipt::observe(&mut persist, &f.run, Some(&f.binding), &mut observer)
            .await
            .unwrap()
    );
    let foreign = f.binding.clone().accepted_turn("foreign-turn");
    assert!(
        !maintenance_receipt::observe(&mut persist, &f.run, Some(&foreign), &mut observer)
            .await
            .unwrap()
    );
    assert!(
        f.store
            .maintenance_checkpoint_intent(f.epoch, &f.run, ATTEMPT, true)
            .unwrap()
            .is_none()
    );
}

/// A real writer shutdown causes an observation error; the runner's exact
/// handler retains unavailable maintenance state instead of returning a failure.
#[tokio::test]
async fn codex_maintenance_receipt_write_failure_holds_only_maintenance() {
    let mut f = Fixture::new();
    let prep = common::writer::prepare_supervisor(&mut f.store, &f.run);
    let harness = common::writer::Harness::start(&f.home.path);
    let mut persist = harness.supervisor(&f.home.path, prep, &f.run).await;
    let (boot, now) = agent_run_platform::monotonic::now().unwrap();
    harness
        .service
        .shutdown(MonoDeadline {
            boot_id: boot,
            ns: now + 5_000_000_000,
        })
        .await
        .unwrap();
    let binding = f.binding.clone().accepted_turn(TURN);
    let mut observer = f.observer();
    let result = tokio::time::timeout(
        std::time::Duration::from_secs(5),
        maintenance_receipt::observe(&mut persist, &f.run, Some(&binding), &mut observer),
    )
    .await
    .expect("bounded writer refusal");
    assert!(result.is_err());
    assert!(!maintenance_receipt::receipt_outcome(&mut observer, result));
    assert_eq!(
        observer.assessment(),
        Assessment::Unavailable(Gap::ReceiptPersistenceUnavailable)
    );
    assert_eq!(
        f.store.get(&f.run).unwrap().status,
        agent_run_domain::domain::Status::Running
    );
    assert!(
        f.store
            .maintenance_checkpoint_intent(f.epoch, &f.run, ATTEMPT, true)
            .unwrap()
            .is_none()
    );
}

/// Every possibly-sent failure remains unknown and cannot be mistaken for an
/// accepted turn or a safe-to-repeat unsent command; errors leak no source text.
#[test]
fn codex_maintenance_unknown_send_never_becomes_acceptance_or_runner_error() {
    use agent_run_adapters::io::{RpcDisposition, RpcUncertain};
    for reason in [
        RpcUncertain::Timeout,
        RpcUncertain::Send,
        RpcUncertain::Closed,
        RpcUncertain::MalformedReply,
    ] {
        let result = maintenance_receipt::send_result(Ok(RpcDisposition::Uncertain(reason)));
        assert!(result["accepted"].is_null());
    }
    let failed =
        maintenance_receipt::send_result(Err(Error::Runtime("private-error-content".into())));
    assert!(failed["accepted"].is_null());
    assert!(!failed.to_string().contains("private-error-content"));
    assert_eq!(
        maintenance_receipt::send_result(Ok(RpcDisposition::UnsentPressure))["accepted"],
        false
    );
    assert_eq!(
        maintenance_receipt::send_result(Ok(RpcDisposition::Replied(json!({}))))["accepted"],
        true
    );
}
