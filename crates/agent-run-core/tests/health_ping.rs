//! Real writer, native stream and process cleanup; no provider or model is invoked.
mod common;
use agent_run_adapters::{LaunchPlan, io::Process};
use agent_run_core::{domain::Status, service::Service, stream};
use agent_run_domain::{health::PingPolicy, storage::WriteAuthority};
use agent_run_store::ops::HealthTick;
use serde_json::{Value, json};
use std::{collections::BTreeMap, time::Duration};

/// Fixture driver uses the same read preparation as the real broker before enqueue.
fn health_op(
    store: &agent_run_store::Store<'_>,
    id: &agent_run_domain::domain::AgentId,
    at: f64,
) -> HealthTick {
    HealthTick {
        execution: id.clone(),
        at,
        facts: store.prepare_health(id).unwrap().unwrap(),
    }
}

/// Fixture-only capability of the exact native attempt.
const TOKEN: &str = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
/// A finite successful inference turn; it never means the logical run is done.
const TURN: &str = r#"{"type":"result","subtype":"success","is_error":false,"session_id":"s","result":"","usage":{}}"#;

/// Observe a durable predicate in the fixture driver, with a separate hang bound.
/// This is not model polling and neither starts nor repeats a worker turn.
async fn observed(mut ready: impl FnMut() -> bool) {
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if ready() {
                return;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("fixture predicate reached");
}

/// Capture fixture-driver assertion panics so the owning test can always run
/// verified child/group cleanup before propagating failure. This is test-only;
/// production errors and task outcomes never use panic recovery.
async fn guarded<F: std::future::Future>(future: F) -> std::thread::Result<F::Output> {
    let mut future = std::pin::pin!(future);
    std::future::poll_fn(|cx| {
        match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| future.as_mut().poll(cx))) {
            Ok(value) => value.map(Ok),
            Err(panic) => std::task::Poll::Ready(Err(panic)),
        }
    })
    .await
}

/// Control pings do not write native input;
/// One opted-in health message reaches
/// the same process/session, a private reply traverses the real worker dispatcher,
/// its pong cannot rearm inference, and ordinary cancellation/cleanup still work.
#[tokio::test]
async fn stream_service_ping_then_one_correlated_model_turn() {
    run_ping_fixture(false).await;
}

/// The original Codex idle case uses a new correlated turn on its existing
/// app-server thread; a service ping never starts that native turn.
#[tokio::test]
async fn codex_service_ping_then_one_correlated_model_turn() {
    run_ping_fixture(true).await;
}

/// Shared native fixture ownership, writer, challenge, reply and cleanup proof
/// for the two supported engine transports. No model or provider is invoked.
async fn run_ping_fixture(codex_mode: bool) {
    let home = common::Home::new();
    let mut req = home.request();
    req.explicit_finish = true;
    req.idle_ping = PingPolicy::Recover;
    let (id, _) = home
        .store()
        .admit(&req, &home.config, &json!({}), None)
        .unwrap();
    let mut store = home.store();
    store.conn.execute("INSERT INTO attempts(id,agent_id,number,state,adapter_state_json,created_at,ownership_active) VALUES('attempt',?,1,'running','{}',100,1)",[id.as_str()]).unwrap();
    store
        .conn
        .execute(
            "UPDATE agents SET status='running' WHERE id=?",
            [id.as_str()],
        )
        .unwrap();
    store
        .issue_worker_capability(&id, "attempt", TOKEN, 100.0)
        .unwrap();
    store
        .register_worker_catalog(
            &agent_run_domain::worker::WorkerCatalogProof {
                run_id: id.clone(),
                attempt_id: "attempt".into(),
                token: TOKEN.into(),
                version: agent_run_domain::worker::FINISH_CATALOG_VERSION,
                digest: agent_run_domain::worker::finish_catalog_digest(false),
            },
            100.0,
        )
        .unwrap();
    store.begin_worker_lifecycle(&id, "attempt", 100.0).unwrap();
    let row = store.get(&id).unwrap();
    let prep = common::writer::prepare_supervisor(&mut store, &id);
    let harness = common::writer::Harness::start(&home.path);
    let mut persist = harness.supervisor(&home.path, prep, &id).await;
    let service = Service::new(home.path.clone()).with_writer(harness.broker());
    let broker = harness.broker();
    let lane = broker.lane("health-fixture");
    let ping_id = agent_run_core::fs::sha256(b"attempt:0");
    let script = format!(
        r#"read initial
printf '%s\n' '{TURN}'
read wake
printf '%s\n' "$wake" > wake.json
printf '%s\n' '{{"type":"assistant","session_id":"s","message":{{"role":"assistant","content":[{{"type":"tool_use","id":"health-call","name":"mcp__agent_run_worker__health_reply","input":{{"ping_id":"{ping_id}","state":"working"}}}}]}}}}'
while test ! -f health-receipt; do sleep 0.01; done
printf '%s\n' '{{"type":"user","session_id":"s","message":{{"role":"user","content":[{{"type":"tool_result","tool_use_id":"health-call","content":"accepted"}}]}}}}'
printf '%s\n' '{TURN}'
read unexpected
printf '%s\n' "$unexpected" > unexpected.json
"#
    );
    let app_home = home.path.join("codex-home");
    agent_run_platform::fs::private_dir(&app_home).unwrap();
    let runtime:agent_run_config::config::Runtime=serde_json::from_value(json!({"enabled":true,"adapter":"codex","binary":"/bin/sh","home":home.path.join("native"),"models":["fixture"]})).unwrap();
    let role = agent_run_config::profiles::Profile {
        name: "review".into(),
        body: "Review fixture".into(),
        write: false,
        network: false,
        revision: "fixture".into(),
        canonical: false,
        allow_external_read_roots: true,
        read_roots: vec![],
        skills: vec![],
        mcp: vec![],
        mcp_tools: Default::default(),
        required_constraints: Default::default(),
    };
    let (script, environment) = if codex_mode {
        let grant =
            agent_run_core::codex::Grant::new(&runtime, &row.request, &role, &app_home).unwrap();
        codex_fixture(&grant)
    } else {
        (script, BTreeMap::new())
    };
    let mut process = Process::spawn(&LaunchPlan {
        binary: "/bin/sh".into(),
        args: vec!["-c".into(), script],
        cwd: home.path.clone(),
        environment,
        initial_input: None,
    })
    .unwrap();
    let passthrough = agent_run_core::health::command(
        &mut process,
        &mut persist,
        &id,
        (-1, "cancel", &json!({"health_ping":"malformed"})),
        true,
    )
    .await;
    let pid = process.owner.pid;
    let driver = async {
        observed(|| {
            store
                .worker_lifecycle_view(&id)
                .unwrap()
                .is_some_and(|v| v["phase"] == "idle")
        })
        .await;
        // Fixture clock only: represent a five-minute-old idle boundary after
        // the real native stream has acknowledged the actual turn completion.
        store
            .conn
            .execute(
                "UPDATE worker_lifecycle SET transition_at=101 WHERE attempt_id='attempt'",
                [],
            )
            .unwrap();
        let at = agent_run_core::domain::now();
        assert!(
            lane.local(health_op(&store, &id, at - 61.0), Duration::from_secs(5))
                .await
                .unwrap()
        );
        observed(||store.conn.query_row("SELECT EXISTS(SELECT 1 FROM commands WHERE agent_id=? AND kind='ping' AND state='completed')",[id.as_str()],|r|r.get::<_,bool>(0)).unwrap()).await;
        let receipt:String=store.conn.query_row("SELECT result_json FROM commands WHERE agent_id=? AND kind='ping' ORDER BY id DESC LIMIT 1",[id.as_str()],|r|r.get(0)).unwrap();
        let receipt: Value = serde_json::from_str(&receipt).unwrap();
        assert_eq!(receipt["native_alive"], true);
        assert_eq!(receipt["phase"], "idle");
        assert!(
            !home.path.join("wake.json").exists(),
            "service ping must not write model stdin"
        );
        assert!(
            lane.local(
                health_op(&store, &id, agent_run_core::domain::now()),
                Duration::from_secs(5)
            )
            .await
            .unwrap()
        );
        observed(|| home.path.join("wake.json").exists()).await;
        let wake = std::fs::read_to_string(home.path.join("wake.json")).unwrap();
        assert!(wake.contains(&ping_id));
        let response=agent_run_core::dispatch::call(&service,agent_run_domain::worker::TOOL_METHOD,json!({"run_id":id,"attempt_id":"attempt","token":TOKEN,"tool":"health_reply","input":{"ping_id":ping_id,"state":"working"}})).await.unwrap();
        assert_eq!(response["accepted"], true);
        std::fs::write(home.path.join("health-receipt"), "accepted").unwrap();
        observed(|| {
            store
                .worker_lifecycle_view(&id)
                .unwrap()
                .is_some_and(|v| v["phase"] == "idle" && v["turn_count"] == 2)
        })
        .await;
        let _ = lane
            .local(
                health_op(&store, &id, agent_run_core::domain::now() + 121.0),
                Duration::from_secs(5),
            )
            .await
            .unwrap();
        assert_eq!(
            store.health_view(&id).unwrap().unwrap()["reason"],
            "idle_without_wait_source_after_ping"
        );
        assert_eq!(store.conn.query_row("SELECT COUNT(*) FROM commands WHERE agent_id=? AND json_extract(payload_json,'$.health_ping') IS NOT NULL",[id.as_str()],|r|r.get::<_,i64>(0)).unwrap(),1);
        service.cancel(&id).await.unwrap();
    };
    let result = tokio::time::timeout(Duration::from_secs(10), async {
        let runner = async {
            if codex_mode {
                agent_run_core::codex::run(
                    &mut process,
                    &mut persist,
                    &row,
                    &row,
                    &runtime,
                    &role,
                    &app_home,
                )
                .await
            } else {
                stream::run(&mut process, &mut persist, &row, &row, Some("initial\n")).await
            }
        };
        let (result, driver_result) = tokio::join!(guarded(runner), guarded(driver));
        (result, driver_result)
    })
    .await;
    let proof = process.owner.cleanup(Duration::from_secs(2)).await.unwrap();
    process.reap().await;
    let (result, driver_result) = result.expect("bounded real stream fixture");
    if let Err(panic) = driver_result {
        std::panic::resume_unwind(panic);
    }
    let result = match result {
        Ok(result) => result.unwrap(),
        Err(panic) => std::panic::resume_unwind(panic),
    };
    assert_eq!(result.outcome.status, Status::Cancelled);
    assert!(
        matches!(passthrough, Ok(false)),
        "health metadata cannot swallow cancellation"
    );
    assert_eq!(process.owner.pid, pid);
    assert!(proof.confirmed && proof.group_gone);
    assert!(!home.path.join("unexpected.json").exists());
    if codex_mode {
        assert_eq!(
            std::fs::read_to_string(home.path.join("turns.txt"))
                .unwrap()
                .trim(),
            "2"
        );
    }
    // The reply callback has worker authority, not a broker/operator capability.
    assert!(!agent_run_store::ops::OpKindRef::AcceptHealthReply.permits(&WriteAuthority::Broker));
}

/// A shell-hosted app-server echoes the real frozen grant and private catalog.
/// Environment data is printed as data, never evaluated as shell source. Two
/// turn/start calls are observable; health replies are driven by the real writer.
fn codex_fixture(grant: &agent_run_core::codex::Grant) -> (String, BTreeMap<String, String>) {
    let legacy = json!({"model":grant.model,"cwd":grant.cwd,"roots":grant.roots,"writableRoots":grant.writable_roots,"sandbox":grant.sandbox,"approvalPolicy":grant.approval_policy,"threadId":"thread"});
    let modern = json!({"model":grant.model,"cwd":grant.cwd,"runtimeWorkspaceRoots":grant.roots,"sandbox":{"type":"readOnly","networkAccess":grant.network_access},"approvalPolicy":grant.approval_policy,"activePermissionProfile":{"id":grant.permission_profile.as_deref().unwrap_or(":read-only")},"threadId":"thread"});
    let tools: serde_json::Map<String, Value> = agent_run_domain::worker::finish_tools_json(false)
        .into_iter()
        .map(|tool| (tool["name"].as_str().unwrap().to_owned(), tool))
        .collect();
    let catalog =
        json!({"data":[{"name":"agent_run_worker","runtimeStatus":"connected","tools":tools}]});
    let environment = BTreeMap::from([
        ("ECHO_LEGACY".into(), legacy.to_string()),
        ("ECHO_MODERN".into(), modern.to_string()),
        ("CATALOG".into(), catalog.to_string()),
    ]);
    let script = r#"turns=0
while IFS= read -r line; do
id=$(printf '%s' "$line" | sed -n 's/.*"id":\([0-9][0-9]*\).*/\1/p')
case "$line" in
*'"method":"initialize"'*) printf '{"id":%s,"result":{}}\n' "$id" ;;
*'"method":"initialized"'*) : ;;
*'"method":"model/list"'*) printf '{"id":%s,"result":{"data":[{"id":"fixture"}]}}\n' "$id" ;;
*'"method":"config/read"'*) printf '{"id":%s,"result":{"config":{"developer_instructions":null}}}\n' "$id" ;;
*'"method":"thread/start"'*) case "$line" in *'"permissions"'*) echo="$ECHO_MODERN" ;; *) echo="$ECHO_LEGACY" ;; esac; printf '{"id":%s,"result":%s}\n' "$id" "$echo" ;;
*'"method":"mcpServerStatus/list"'*) printf '{"id":%s,"result":%s}\n' "$id" "$CATALOG" ;;
*'"method":"turn/start"'*)
turns=$((turns+1))
printf '%s\n' "$turns" > turns.txt
if test "$turns" -gt 1; then printf '%s\n' "$line" > wake.json; fi
printf '{"id":%s,"result":{"turn":{"id":"turn-%s"}}}\n' "$id" "$turns"
if test "$turns" -gt 1; then while test ! -f health-receipt; do sleep 0.01; done; fi
printf '{"method":"turn/completed","params":{"threadId":"thread","turn":{"id":"turn-%s","status":"completed","items":[]}}}\n' "$turns"
;;
*'"method":"turn/interrupt"'*) printf '{"id":%s,"result":{}}\n' "$id" ;;
esac
done"#;
    (script.into(), environment)
}
