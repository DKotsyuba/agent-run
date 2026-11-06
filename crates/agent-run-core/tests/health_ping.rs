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
    run_native_fixture(false, None, false, false).await;
}

/// The original Codex idle case uses a new correlated turn on its existing
/// app-server thread; a service ping never starts that native turn.
#[tokio::test]
async fn codex_service_ping_then_one_correlated_model_turn() {
    run_native_fixture(true, None, false, false).await;
}

/// Shared native fixture ownership, writer, challenge, reply and cleanup proof
/// for the two supported engine transports. `background=None` exercises ping;
/// Some(true) exercises an early asynchronous completion, Some(false) its
/// synchronous negative control. `completion_after_idle` gates completion on
/// the driver's actual idle observation. `fulfilled` wraps a background/foreground
/// pair exactly like Promise.allSettled output. No model or provider is invoked.
async fn run_native_fixture(
    codex_mode: bool,
    background: Option<bool>,
    completion_after_idle: bool,
    fulfilled: bool,
) {
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
printf '%s\n' "$wake" > wake.pending
mv wake.pending wake.json
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
        let sessions = app_home.join("sessions");
        let history = sessions.join("fixture.jsonl");
        codex_fixture(
            &grant,
            background,
            &history,
            completion_after_idle,
            fulfilled,
        )
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
        if let Some(background) = background {
            if completion_after_idle {
                observed(|| {
                    store
                        .worker_lifecycle_view(&id)
                        .unwrap()
                        .is_some_and(|v| v["phase"] == "idle")
                })
                .await;
                std::fs::write(home.path.join("release-completion"), "release").unwrap();
            }
            if background {
                observed(|| home.path.join("wake.json").exists()).await;
                let wake = std::fs::read_to_string(home.path.join("wake.json")).unwrap();
                assert!(
                    wake.contains("agent-run/native-job-completed"),
                    "unexpected synthetic wake: {wake}"
                );
                assert!(wake.contains("BACKGROUND_PASS"));
                std::fs::write(home.path.join("health-receipt"), "release").unwrap();
                observed(|| {
                    store
                        .worker_lifecycle_view(&id)
                        .unwrap()
                        .is_some_and(|v| v["phase"] == "idle" && v["turn_count"] == 2)
                })
                .await;
            } else {
                observed(|| {
                    store
                        .worker_lifecycle_view(&id)
                        .unwrap()
                        .is_some_and(|v| v["phase"] == "idle")
                })
                .await;
                observed(|| store.conn.query_row("SELECT COUNT(*) FROM messages WHERE agent_id=? AND role='tool_result' AND raw_ref='background-job'",[id.as_str()],|r|r.get::<_,i64>(0)).unwrap()==1).await;
                // The free ping is a FIFO supervisor-pass barrier: return
                // observation runs before command handling, with zero inference.
                assert!(
                    lane.local(
                        health_op(&store, &id, agent_run_core::domain::now()),
                        Duration::from_secs(5)
                    )
                    .await
                    .unwrap()
                );
                observed(||store.conn.query_row("SELECT EXISTS(SELECT 1 FROM commands WHERE agent_id=? AND kind='ping' AND state='completed')",[id.as_str()],|r|r.get::<_,bool>(0)).unwrap()).await;
                assert!(!home.path.join("wake.json").exists());
            }
            assert_eq!(
                store
                    .conn
                    .query_row(
                        "SELECT COUNT(*) FROM worker_native_wakes WHERE attempt_id='attempt'",
                        [],
                        |r| r.get::<_, i64>(0)
                    )
                    .unwrap(),
                i64::from(background)
            );
            assert_eq!(store.conn.query_row("SELECT COUNT(*) FROM messages WHERE agent_id=? AND role='tool_result' AND raw_ref='background-job'",[id.as_str()],|r|r.get::<_,i64>(0)).unwrap(),1,"duplicate completion cannot duplicate journal");
            if fulfilled {
                assert_eq!(store.conn.query_row("SELECT COUNT(*) FROM messages WHERE agent_id=? AND role='tool_result' AND raw_ref='foreground-job'",[id.as_str()],|r|r.get::<_,i64>(0)).unwrap(),1,"wrapped foreground completion journals once without a wake");
            }
            service.cancel(&id).await.unwrap();
            return;
        }
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
            if background == Some(false) { "1" } else { "2" }
        );
    }
    // The reply callback has worker authority, not a broker/operator capability.
    assert!(!agent_run_store::ops::OpKindRef::AcceptHealthReply.permits(&WriteAuthority::Broker));
}

/// A shell-hosted app-server echoes the real frozen grant and private catalog.
/// Environment data is printed as data, never evaluated as shell source. Two
/// turn/start calls are observable; health replies are driven by the real writer.
/// `background` optionally emits one started/duplicated terminal command before
/// idle. Both command forms have a PTY id; only the asynchronous result
/// returns a pending session_id in its complete model-facing native history.
/// `history` names a rollout lazily created on the first turn, as in real Codex;
/// `completion_after_idle`
/// waits for the driver's release file after the first real idle boundary.
/// `fulfilled` adds a matched foreground command and groups both returns in
/// standard Promise result wrappers. Returns shell source and environment data;
/// the shell never evaluates env data. Wake receipts are renamed into place only
/// after their complete write, so an existence observer never reads an empty file.
fn codex_fixture(
    grant: &agent_run_core::codex::Grant,
    background: Option<bool>,
    history: &std::path::Path,
    completion_after_idle: bool,
    fulfilled: bool,
) -> (String, BTreeMap<String, String>) {
    let legacy = json!({"model":grant.model,"cwd":grant.cwd,"roots":grant.roots,"writableRoots":grant.writable_roots,"sandbox":grant.sandbox,"approvalPolicy":grant.approval_policy,"threadId":"thread","thread":{"path":history}});
    let modern = json!({"model":grant.model,"cwd":grant.cwd,"runtimeWorkspaceRoots":grant.roots,"sandbox":{"type":"readOnly","networkAccess":grant.network_access},"approvalPolicy":grant.approval_policy,"activePermissionProfile":{"id":grant.permission_profile.as_deref().unwrap_or(":read-only")},"threadId":"thread","thread":{"path":history}});
    let tools: serde_json::Map<String, Value> = agent_run_domain::worker::finish_tools_json(false)
        .into_iter()
        .map(|tool| (tool["name"].as_str().unwrap().to_owned(), tool))
        .collect();
    let catalog =
        json!({"data":[{"name":"agent_run_worker","runtimeStatus":"connected","tools":tools}]});
    let mut environment = BTreeMap::from([
        ("ECHO_LEGACY".into(), legacy.to_string()),
        ("ECHO_MODERN".into(), modern.to_string()),
        ("CATALOG".into(), catalog.to_string()),
        ("HISTORY".into(), history.display().to_string()),
        (
            "COMPLETE_LATE".into(),
            if completion_after_idle { "yes" } else { "no" }.into(),
        ),
    ]);
    if let Some(background) = background {
        let mut item = json!({"type":"commandExecution","id":"background-job","status":"completed","exitCode":0,"aggregatedOutput":"BACKGROUND_PASS"});
        // Native 0.163 assigns a PTY processId to foreground commands too.
        item["processId"] = json!("86309");
        let mut result = json!({"chunk_id":"fixture-chunk","wall_time_seconds":0.1,"original_token_count":0,"output":""});
        if background {
            result["session_id"] = json!(86309);
        } else {
            result["exit_code"] = json!(0);
        }
        let returned = if fulfilled {
            let foreground = json!({"chunk_id":"foreground","wall_time_seconds":0.2,"exit_code":0,"original_token_count":1,"output":"FOREGROUND_INLINE"});
            environment.insert("FG_STARTED".into(), json!({"method":"item/started","params":{"threadId":"thread","turnId":"turn-1","item":{"type":"commandExecution","id":"foreground-job","processId":"86310","status":"inProgress"}}}).to_string());
            environment.insert("FG_COMPLETED".into(), json!({"method":"item/completed","params":{"threadId":"thread","turnId":"turn-1","item":{"type":"commandExecution","id":"foreground-job","processId":"86310","status":"completed","exitCode":0,"aggregatedOutput":"FOREGROUND_INLINE"}}}).to_string());
            json!([
                {"type":"input_text","text":"Script completed\nOutput:\n"},
                {"type":"input_text","text":json!({"status":"fulfilled","value":result}).to_string()},
                {"type":"input_text","text":json!({"status":"fulfilled","value":foreground}).to_string()}
            ])
        } else {
            json!([{"type":"input_text","text":result.to_string()}])
        };
        environment.insert("RETURNED".into(), json!({"type":"response_item","payload":{"type":"custom_tool_call_output","internal_chat_message_metadata_passthrough":{"turn_id":"turn-1"},"output":returned}}).to_string());
        environment.insert("JOB_STARTED".into(),json!({"method":"item/started","params":{"threadId":"thread","turnId":"turn-1","item":{"type":"commandExecution","id":"background-job","status":"inProgress"}}}).to_string());
        environment.insert("JOB_COMPLETED".into(),json!({"method":"item/completed","params":{"threadId":"thread","turnId":"turn-1","item":item}}).to_string());
    }
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
if test "$turns" -eq 1; then
mkdir -p "$(dirname "$HISTORY")"
printf '%s\n' '{"type":"session_meta","payload":{"id":"thread"}}' > "$HISTORY"
fi
printf '%s\n' "$turns" > turns.txt
if test "$turns" -gt 1; then printf '%s\n' "$line" > wake.pending; mv wake.pending wake.json; fi
printf '{"id":%s,"result":{"turn":{"id":"turn-%s"}}}\n' "$id" "$turns"
if test "$turns" -eq 1 && test -n "$JOB_STARTED"; then
printf '%s\n' "$JOB_STARTED"
if test -n "$FG_STARTED"; then printf '%s\n' "$FG_STARTED"; fi
if test "$COMPLETE_LATE" = no; then printf '%s\n' "$JOB_COMPLETED" "$JOB_COMPLETED"; fi
if test -n "$FG_COMPLETED"; then printf '%s\n' "$FG_COMPLETED" "$FG_COMPLETED"; fi
printf '%s\n' "$RETURNED" >> "$HISTORY"
printf '%s\n' '{"type":"event_msg","payload":{"type":"task_complete","turn_id":"turn-1"}}' >> "$HISTORY"
fi
if test "$turns" -gt 1; then while test ! -f health-receipt; do sleep 0.01; done; fi
printf '{"method":"turn/completed","params":{"threadId":"thread","turn":{"id":"turn-%s","status":"completed","items":[]}}}\n' "$turns"
if test "$turns" -eq 1 && test -n "$JOB_STARTED" && test "$COMPLETE_LATE" = yes; then
while test ! -f release-completion; do sleep 0.01; done
printf '%s\n' "$JOB_COMPLETED" "$JOB_COMPLETED"
fi
;;
*'"method":"turn/interrupt"'*) printf '{"id":%s,"result":{}}\n' "$id" ;;
esac
done"#;
    (script.into(), environment)
}

/// An asynchronous command can finish before its originating inference turn
/// yields. Its actual pending return proves the model needs a completion wake;
/// a duplicate item still journals/enqueues once, while synchronous calls do not
/// create a redundant turn. All paths retain exact process cleanup.
#[tokio::test]
async fn codex_background_completion_before_idle_wakes_once() {
    run_native_fixture(true, Some(true), false, false).await;
    run_native_fixture(true, Some(false), false, false).await;
}

/// A terminal command already returned inline must not create another model
/// turn merely because its native completion retains a PTY process identifier.
#[tokio::test]
async fn codex_foreground_with_process_id_does_not_wake() {
    run_native_fixture(true, Some(false), false, false).await;
}

/// A real pending return still wakes exactly once when completion follows the
/// observed idle boundary; replayed native terminal frames do not add turns.
#[tokio::test]
async fn codex_background_completion_after_idle_wakes_once() {
    run_native_fixture(true, Some(true), true, false).await;
}

/// A delayed native terminal frame is not a new result if the command returned
/// its final output inline before the observed native inference turn ended.
#[tokio::test]
async fn codex_foreground_completion_after_idle_does_not_wake() {
    run_native_fixture(true, Some(false), true, false).await;
}

/// The real reported shape has a fulfilled pending background return alongside
/// a fulfilled terminal foreground return. Early and late completion each wake
/// once; wrapped foreground-only cases never add a turn, including duplicates.
#[tokio::test]
async fn codex_fulfilled_background_completions_wake_once() {
    for late in [false, true] {
        run_native_fixture(true, Some(true), late, true).await;
        run_native_fixture(true, Some(false), late, true).await;
    }
}
