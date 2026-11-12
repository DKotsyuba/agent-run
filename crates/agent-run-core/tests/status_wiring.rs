//! Synthetic status wiring: one database snapshot, no provider process.

use crate::common;

use agent_run_core::{dispatch, service::Service};
use agent_run_domain::{
    ProviderStartRequest,
    batch::{ExecutionRef, PinId},
    catalog::{HarnessId, ProviderConnection, ResolvedLaunchAuthority},
    domain::AgentId,
};
use rusqlite::params;
use serde_json::json;

fn execution(home: &common::Home, status: &str) -> (AgentId, ExecutionRef) {
    let mut store = home.store();
    let request = home.request();
    let (id, _) = store
        .admit(&request, &home.config, &json!({}), None)
        .unwrap();
    let provider: ProviderStartRequest = serde_json::from_value(json!({"provider":"mock","model":"fixture","profile":"review","task":request.task,"workdir":home.path,"explicit_finish":false})).unwrap();
    let authority = ResolvedLaunchAuthority {
        provider: "mock".parse().unwrap(),
        harness: HarnessId::ClaudeCode,
        connection: ProviderConnection::Native,
        model: "fixture".into(),
        effort: None,
        profile: "review".into(),
        workdir: home.path.clone(),
        role_payload: common::role_payload(),
        assets_sha256: "0".repeat(64).parse().unwrap(),
        eligible_accounts: vec!["acct".parse().unwrap()],
    };
    let raw = serde_json::to_value(&provider).unwrap();
    let identity = json!({"provider_identity_version":2,"provider_request":raw,
        "replay_request_sha256":agent_run_domain::canonical::sha256_hex(&serde_json::to_value(provider).unwrap(),true),
        "authority":authority,"effective_policy":{"pid":4242,"token":"private-token"}});
    store.conn.execute("UPDATE agents SET identity_json=?,status=?,finished_at=1,failure_kind='native_failed',failure_text='private-token' WHERE id=?",
        params![identity.to_string(),status,id.as_str()]).unwrap();
    let attempt = store.create_attempt(&id, "failed", &json!({})).unwrap();
    store.conn.execute("UPDATE attempts SET selected_account_id='acct',phase='cleanup_complete',ownership_active=0,cleanup_proof_json=? WHERE id=?",
        params![json!({"confirmed":true,"group_gone":true,"descendants_gone":true}).to_string(),attempt]).unwrap();
    let cleanup = json!({"signals":["SIGTERM"],"scope":"verified_descendants","group_gone":true,"descendants_gone":true,"confirmed":true,"process_group_id":4242,"token":"private-token"});
    store.conn.execute("INSERT INTO events(agent_id,attempt_id,at,kind,data_json) VALUES(?,?,1,'process_cleanup',?)",
        params![id.as_str(),attempt,cleanup.to_string()]).unwrap();
    let reference = ExecutionRef::from_first_provider_attempt(&id, &attempt).unwrap();
    (id, reference)
}

/// The view follows the selected historical row and strips native identifiers.
#[tokio::test]
async fn selected_view_is_not_tail_and_is_redacted() {
    let home = common::Home::new();
    let (root, reference) = execution(&home, "succeeded");
    let store = home.store();
    let event: i64 = store
        .conn
        .query_row(
            "SELECT seq FROM events WHERE agent_id=? AND kind='process_cleanup' LIMIT 1",
            [root.as_str()],
            |row| row.get(0),
        )
        .unwrap();
    store.conn.execute("INSERT INTO deliveries(id,agent_id,terminal_event_seq,state,attempts,ambiguous_result,last_error) VALUES('status-fixture',?,?,'retry_wait',1,1,'private-token')",params![root.as_str(),event]).unwrap();
    store.conn.execute("INSERT INTO delivery_attempt_evidence(delivery_id,attempt,recorded_at,evidence_json) VALUES('status-fixture',1,1,?)",[json!({"classifier":"relay_ambiguous","token":"private-token"}).to_string()]).unwrap();
    drop(store);
    let pin = PinId::new();
    home.store()
        .conn
        .execute(
            "INSERT INTO history_pins(pin_id,root_agent_id) VALUES(?,?)",
            params![pin.as_str(), root.as_str()],
        )
        .unwrap();
    let (child, _) = execution(&home, "running");
    home.store()
        .conn
        .execute(
            "UPDATE agents SET root_agent_id=?,parent_agent_id=?,sequence=2 WHERE id=?",
            params![root.as_str(), root.as_str(), child.as_str()],
        )
        .unwrap();
    let store_id = agent_run_store::store_identity::read(&home.store().conn).unwrap();
    let service = Service::new(home.path.clone());
    let mut request = json!({"version":1,"expected_store_id":store_id,"targets":[{"agent_id":root,"sequence":1,"execution_ref":reference,"pin_id":pin}],"include_view":true});
    let result = dispatch::call(&service, "status", request.clone())
        .await
        .unwrap();
    assert_eq!(result["items"][0]["match"], "verified");
    assert_eq!(result["items"][0]["selected"]["sequence"], 1);
    assert_eq!(result["items"][0]["retained_latest"]["sequence"], 2);
    assert_eq!(result["view"]["selector_index"], 0);
    assert_eq!(result["view"]["match"], "verified");
    assert_eq!(result["view"]["status_scope"], "selected_execution_raw");
    assert_eq!(result["view"]["agent"]["agent_id"], root.as_str());
    assert_eq!(result["view"]["agent"]["sequence"], 1);
    assert_eq!(result["view"]["agent"]["status"], "succeeded");
    assert_eq!(result["view"]["agent"]["failure_kind"], "native_failed");
    assert!(result["view"]["agent"]["cleanup"].is_null());
    assert!(result["view"]["agent"]["policy"].is_null());
    assert!(result["view"]["agent"]["failure_text"].is_null());
    assert!(result["view"]["agent"].get("health").is_none());
    assert_eq!(result["view"]["agent"]["delivery"]["ambiguous"], true);
    assert!(result["view"]["agent"]["delivery"]["last_error"].is_null());
    assert!(result["view"]["agent"]["delivery"]["last_attempt"].is_null());
    assert_eq!(
        result["view"]["sampled_at"],
        result["view"]["agent"]["observed_at"]
    );
    let encoded = result.to_string();
    assert!(!encoded.contains("private-token"));
    assert!(!encoded.contains("process_group_id"));
    assert!(!encoded.contains("\"pid\""));

    request["targets"][0]["execution_ref"] = json!(format!("ex1_{}", "f".repeat(64)));
    let mismatch = dispatch::call(&service, "status", request.clone())
        .await
        .unwrap();
    assert_eq!(mismatch["items"][0]["match"], "mismatch");
    assert!(mismatch.get("view").is_none());
    request["targets"][0]
        .as_object_mut()
        .unwrap()
        .remove("execution_ref");
    let unverified = dispatch::call(&service, "status", request.clone())
        .await
        .unwrap();
    assert_eq!(unverified["view"]["match"], "unverified");
    assert_eq!(unverified["view"]["agent"]["sequence"], 1);
    request["targets"][0]["sequence"] = json!(9);
    let missing = dispatch::call(&service, "status", request).await.unwrap();
    assert_eq!(missing["items"][0]["match"], "missing");
    assert!(missing.get("view").is_none());
}

/// The old scalar request remains invalid and a mismatched store leaks no proof.
#[tokio::test]
async fn status_rejects_legacy_scalar_and_wrong_store() {
    let home = common::Home::new();
    let (id, _) = execution(&home, "failed");
    let service = Service::new(home.path.clone());
    assert!(
        dispatch::call(&service, "status", json!({"agent_id":id}))
            .await
            .is_err()
    );
    let wrong = json!({"version":1,"expected_store_id":"store_ffffffffffffffffffffffffffffffff","targets":[{"agent_id":id,"sequence":1}]});
    assert!(matches!(
        dispatch::call(&service, "status", wrong).await,
        Err(agent_run_domain::Error::StoreIdentityMismatch)
    ));
    let capabilities = dispatch::call(&service, "capabilities", json!({}))
        .await
        .unwrap();
    assert!(
        capabilities["read_methods"]
            .as_array()
            .unwrap()
            .iter()
            .any(|method| method["name"] == "status")
    );
    assert!(
        !agent_run_domain::tools::worker_registry()
            .iter()
            .any(|tool| tool.name == "status")
    );
}

/// This deliberately contradictory fixture guards UI handling; it is not a normal restart record.
#[tokio::test]
async fn selected_view_exposes_raw_status_while_restart_is_pending_or_unknown() {
    let home = common::Home::new();
    let (root, reference) = execution(&home, "succeeded");
    let (child, _) = execution(&home, "running");
    let pin = PinId::new();
    let store = home.store();
    store
        .conn
        .execute(
            "UPDATE agents SET root_agent_id=?,parent_agent_id=?,sequence=2 WHERE id=?",
            params![root.as_str(), root.as_str(), child.as_str()],
        )
        .unwrap();
    store
        .conn
        .execute(
            "UPDATE agents SET answer_path=? WHERE id=?",
            params![
                home.path
                    .join("agents")
                    .join(root.as_str())
                    .join("answer.md")
                    .to_string_lossy(),
                root.as_str()
            ],
        )
        .unwrap();
    store
        .conn
        .execute(
            "INSERT INTO history_pins(pin_id,root_agent_id) VALUES(?,?)",
            params![pin.as_str(), root.as_str()],
        )
        .unwrap();
    store.conn.execute("INSERT INTO service_restarts(request_id,phase,started_at,boot_id) VALUES('fixture-restart','preparing',1,'fixture')",[]).unwrap();
    let restart = store.conn.last_insert_rowid();
    store.conn.execute("INSERT INTO service_restart_agents(restart_id,agent_id,cancelled_before_restart,resume_status) VALUES(?,?,0,'pending')",params![restart,root.as_str()]).unwrap();
    let service = Service::new(home.path.clone());
    let request = json!({"version":1,"targets":[{"agent_id":root,"sequence":1,"execution_ref":reference,"pin_id":pin}],"include_view":true});
    let pending = dispatch::call(&service, "status", request.clone())
        .await
        .unwrap();
    assert_eq!(pending["items"][0]["restart_resume"], "pending");
    assert_eq!(pending["items"][0]["lineage_terminal"], false);
    assert_eq!(pending["items"][0]["selected"]["raw_status"], "succeeded");
    assert_eq!(pending["items"][0]["selected"]["sequence"], 1);
    assert_eq!(pending["items"][0]["retained_latest"]["sequence"], 2);
    assert_eq!(pending["view"]["status_scope"], "selected_execution_raw");
    assert_eq!(pending["view"]["agent"]["status"], "succeeded");
    assert_eq!(pending["view"]["agent"]["phase"], "terminal");
    assert_eq!(pending["view"]["agent"]["runtime_outcome"], "succeeded");
    assert_eq!(pending["view"]["agent"]["answer_available"], true);
    store
        .conn
        .execute(
            "UPDATE service_restart_agents SET resume_status='unconfirmed' WHERE restart_id=?",
            [restart],
        )
        .unwrap();
    store
        .conn
        .execute(
            "UPDATE service_restarts SET phase='complete',finished_at=2 WHERE id=?",
            [restart],
        )
        .unwrap();
    let unknown = dispatch::call(&service, "status", request).await.unwrap();
    assert_eq!(unknown["items"][0]["restart_resume"], "unknown");
    assert!(unknown["items"][0]["lineage_terminal"].is_null());
    assert_eq!(unknown["items"][0]["selected"]["raw_status"], "succeeded");
    assert_eq!(unknown["items"][0]["retained_latest"]["sequence"], 2);
    assert_eq!(unknown["view"]["status_scope"], "selected_execution_raw");
    assert_eq!(unknown["view"]["agent"]["status"], "succeeded");
    assert_eq!(unknown["view"]["agent"]["answer_available"], true);
}

/// Public registry dispatch exposes the exact store/pin-fenced export without a worker.
#[tokio::test]
async fn transcript_exact_dispatch_and_capabilities() {
    let home = common::Home::new();
    let (id, reference) = execution(&home, "succeeded");
    let store = home.store();
    let pin = PinId::new();
    store
        .conn
        .execute(
            "INSERT INTO history_pins(pin_id,root_agent_id) VALUES(?,?)",
            params![pin.as_str(), id.as_str()],
        )
        .unwrap();
    store.conn.execute("INSERT INTO messages(agent_id,root_agent_id,at,role,content,content_complete) VALUES(?1,?1,1,'assistant','complete body',1)",[id.as_str()]).unwrap();
    let store_id = agent_run_store::store_identity::read(&store.conn).unwrap();
    let service = Service::new(home.path.clone());
    let request = json!({"version":1,"expected_store_id":store_id,"selector":{"agent_id":id,"sequence":1,"execution_ref":reference,"pin_id":pin},"cursor":"0","offset":0});
    let output = dispatch::call(&service, "transcript_exact", request)
        .await
        .unwrap();
    assert_eq!(output["snapshot_complete"], true);
    assert!(
        output["message"]["text"]
            .as_str()
            .unwrap()
            .contains("complete body")
    );
    let capabilities = agent_run_core::observation::capabilities();
    assert!(
        capabilities["read_methods"]
            .as_array()
            .unwrap()
            .iter()
            .any(|m| m["name"] == "transcript_exact")
    );
}
