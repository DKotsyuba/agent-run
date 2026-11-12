//! Real Codex runner, writer and process cleanup; no provider or model is invoked.
use crate::common;
use agent_run_adapters::{LaunchPlan, io::Process};
use agent_run_core::domain::Status;
use agent_run_domain::worker::{FinishRequest, FinishStatus};
use serde_json::json;
use std::{collections::BTreeMap, time::Duration};

/// Fixture-only capability of the exact native attempt.
const TOKEN: &str = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";

/// A shell-hosted app-server: its only turn completes at once, so the session
/// is idle when the worker's finish result arrives for that completed turn,
/// followed by a repeated completion of the same turn.
fn idle_finish_fixture(
    grant: &agent_run_core::codex::Grant,
    history: &std::path::Path,
) -> (String, BTreeMap<String, String>) {
    let legacy = json!({"model":grant.model,"cwd":grant.cwd,"roots":grant.roots,"writableRoots":grant.writable_roots,"sandbox":grant.sandbox,"approvalPolicy":grant.approval_policy,"threadId":"thread","thread":{"path":history}});
    let modern = json!({"model":grant.model,"cwd":grant.cwd,"runtimeWorkspaceRoots":grant.roots,"sandbox":{"type":"readOnly","networkAccess":grant.network_access},"approvalPolicy":grant.approval_policy,"activePermissionProfile":{"id":grant.permission_profile.as_deref().unwrap_or(":read-only")},"threadId":"thread","thread":{"path":history}});
    let tools: serde_json::Map<String, serde_json::Value> =
        agent_run_domain::worker::finish_tools_json(false)
            .into_iter()
            .map(|tool| (tool["name"].as_str().unwrap().to_owned(), tool))
            .collect();
    let catalog =
        json!({"data":[{"name":"agent_run_worker","runtimeStatus":"connected","tools":tools}]});
    let environment = BTreeMap::from([
        ("ECHO_LEGACY".into(), legacy.to_string()),
        ("ECHO_MODERN".into(), modern.to_string()),
        ("CATALOG".into(), catalog.to_string()),
        ("HISTORY".into(), history.display().to_string()),
    ]);
    let script = r#"while IFS= read -r line; do
id=$(printf '%s' "$line" | sed -n 's/.*"id":\([0-9][0-9]*\).*/\1/p')
case "$line" in
*'"method":"initialize"'*) printf '{"id":%s,"result":{}}\n' "$id" ;;
*'"method":"initialized"'*) : ;;
*'"method":"model/list"'*) printf '{"id":%s,"result":{"data":[{"id":"fixture"}]}}\n' "$id" ;;
*'"method":"config/read"'*) printf '{"id":%s,"result":{"config":{"developer_instructions":null}}}\n' "$id" ;;
*'"method":"thread/start"'*) case "$line" in *'"permissions"'*) echo="$ECHO_MODERN" ;; *) echo="$ECHO_LEGACY" ;; esac; printf '{"id":%s,"result":%s}\n' "$id" "$echo" ;;
*'"method":"mcpServerStatus/list"'*) printf '{"id":%s,"result":%s}\n' "$id" "$CATALOG" ;;
*'"method":"turn/start"'*)
mkdir -p "$(dirname "$HISTORY")"
printf '%s\n' '{"type":"session_meta","payload":{"id":"thread"}}' > "$HISTORY"
printf '{"id":%s,"result":{"turn":{"id":"turn-1"}}}\n' "$id"
printf '%s\n' '{"method":"turn/completed","params":{"threadId":"thread","turn":{"id":"turn-1","status":"completed","items":[]}}}'
while test ! -f receipt; do sleep 0.01; done
read digest < receipt
printf '{"method":"item/completed","params":{"threadId":"thread","turnId":"turn-1","item":{"type":"mcpToolCall","id":"finish-1","server":"agent_run_worker","tool":"finish","status":"completed","arguments":{},"result":{"content":[{"type":"text","text":"%s"}]},"error":null}}}\n' "$digest"
printf '%s\n' '{"method":"turn/completed","params":{"threadId":"thread","turn":{"id":"turn-1","status":"completed","items":[]}}}'
;;
*'"method":"turn/interrupt"'*) printf '{"id":%s,"result":{}}\n' "$id" ;;
esac
done"#;
    (script.into(), environment)
}

/// A worker calls its private finish after its last native turn already went
/// idle (an idle member whose wake was refused, for example after an in-place
/// Bridge upgrade). The finish result arrives for that completed turn; it is
/// still the observed receipt, so the execution closes with the exact summary
/// instead of staying running forever.
#[tokio::test]
async fn codex_finish_observed_after_idle_settles_the_execution() {
    let home = common::Home::new();
    let mut req = home.request();
    req.explicit_finish = true;
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
    let grant =
        agent_run_core::codex::Grant::new(&runtime, &row.request, &role, &app_home).unwrap();
    let (script, environment) =
        idle_finish_fixture(&grant, &app_home.join("sessions").join("fixture.jsonl"));
    let mut process = Process::spawn(&LaunchPlan {
        binary: "/bin/sh".into(),
        args: vec!["-c".into(), script],
        cwd: home.path.clone(),
        environment,
        initial_input: None,
    })
    .unwrap();
    let mut control = home.store();
    let driver = async {
        tokio::time::timeout(Duration::from_secs(5), async {
            while !control
                .worker_lifecycle_view(&id)
                .unwrap()
                .is_some_and(|v| v["phase"] == "idle")
            {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("native turn went idle");
        let receipt = control
            .accept_worker_finish(
                &id,
                "attempt",
                TOKEN,
                &FinishRequest {
                    summary: "FINAL".into(),
                    status: FinishStatus::Done,
                },
                agent_run_domain::domain::now(),
                agent_run_domain::worker::FinishGuard::Clean,
            )
            .unwrap();
        std::fs::write(home.path.join("receipt"), format!("{}\n", receipt.sha256)).unwrap();
    };
    let result = tokio::time::timeout(Duration::from_secs(15), async {
        let (result, ()) = tokio::join!(
            agent_run_core::codex::run(
                &mut process,
                &mut persist,
                &row,
                &row,
                &runtime,
                &role,
                &app_home,
            ),
            driver
        );
        persist.flush().await.unwrap();
        result
    })
    .await;
    let proof = process.owner.cleanup(Duration::from_secs(2)).await.unwrap();
    process.reap().await;
    let result = result
        .expect("an accepted finish observed while idle must settle")
        .unwrap();
    assert_eq!(result.outcome.status, Status::Succeeded);
    assert_eq!(result.answer.as_deref(), Some("FINAL"));
    assert!(proof.confirmed && proof.group_gone);
    let observed: i64 = home
        .store()
        .conn
        .query_row(
            "SELECT receipt_observed FROM worker_lifecycle WHERE attempt_id='attempt'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(observed, 1);
}
