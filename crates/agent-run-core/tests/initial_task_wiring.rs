//! Synthetic initial-task wiring through the shared dispatcher; no provider process.

mod common;

use agent_run_core::{dispatch, service::Service};
use agent_run_domain::{
    Error, ProviderStartRequest,
    batch::{ExecutionRef, PinId},
    catalog::{HarnessId, ProviderConnection, ResolvedLaunchAuthority},
    domain::AgentId,
};
use rusqlite::params;
use serde_json::{Value, json};

/// One fake provider-v2 root whose three task sources agree.
fn root(home: &common::Home, task: &str) -> (AgentId, ExecutionRef, PinId) {
    let mut store = home.store();
    let mut request = home.request();
    request.task = task.into();
    let (id, _) = store
        .admit(&request, &home.config, &json!({}), None)
        .unwrap();
    let provider: ProviderStartRequest = serde_json::from_value(json!({"provider":"mock","model":"fixture","profile":"review","task":task,"workdir":home.path,"explicit_finish":false})).unwrap();
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
        "replay_request_sha256":agent_run_domain::canonical::sha256_hex(&raw,true),
        "authority":authority});
    store
        .conn
        .execute(
            "UPDATE agents SET identity_json=?,status='succeeded',finished_at=1 WHERE id=?",
            params![identity.to_string(), id.as_str()],
        )
        .unwrap();
    let attempt = store.create_attempt(&id, "failed", &json!({})).unwrap();
    store.conn.execute("UPDATE attempts SET selected_account_id='acct',phase='cleanup_complete',ownership_active=0,cleanup_proof_json=? WHERE id=?",
        params![json!({"confirmed":true,"group_gone":true,"descendants_gone":true}).to_string(),attempt]).unwrap();
    let pin = PinId::new();
    store
        .conn
        .execute(
            "INSERT INTO history_pins(pin_id,root_agent_id) VALUES(?,?)",
            params![pin.as_str(), id.as_str()],
        )
        .unwrap();
    let reference = ExecutionRef::from_first_provider_attempt(&id, &attempt).unwrap();
    (id, reference, pin)
}

/// The one dispatcher entry serves both modes, keeps errors typed and is operator-only.
#[tokio::test]
async fn initial_task_dispatch_capability_and_typed_failures() {
    let home = common::Home::new();
    let task = "dispatch task €🦀";
    let (id, reference, pin) = root(&home, task);
    let store_id = agent_run_store::store_identity::read(&home.store().conn).unwrap();
    let service = Service::new(home.path.clone());

    let strict = json!({"version":1,"mode":"strict","expected_store_id":store_id,"agent_id":id,
        "sequence":1,"execution_ref":reference,"pin_id":pin});
    let output = dispatch::call(&service, "initial_task", strict.clone())
        .await
        .unwrap();
    assert_eq!(output["state"], "available");
    assert_eq!(output["mode"], "strict");
    assert_eq!(output["text"], task);
    assert_eq!(output["total_bytes"], task.len());
    assert_eq!(output["pin_id"], json!(pin));
    let current = json!({"version":1,"mode":"current","agent_id":id});
    let output = dispatch::call(&service, "initial_task", current.clone())
        .await
        .unwrap();
    assert_eq!(output["state"], "available");
    assert!(output.get("pin_id").is_none());
    assert_eq!(output["execution_ref"], json!(reference));

    // Typed unavailable, not an error, and never a partial text.
    let mut gone = current.clone();
    gone["agent_id"] = json!(AgentId::new());
    let output = dispatch::call(&service, "initial_task", gone)
        .await
        .unwrap();
    assert_eq!(
        (output["state"].as_str(), output["reason"].as_str()),
        (Some("unavailable"), Some("not_found"))
    );
    assert!(output.get("text").is_none());

    // Validation and identity failures stay typed errors.
    let mut wrong = strict.clone();
    wrong["expected_store_id"] = json!("store_ffffffffffffffffffffffffffffffff");
    assert!(matches!(
        dispatch::call(&service, "initial_task", wrong).await,
        Err(Error::StoreIdentityMismatch)
    ));
    for (key, value) in [
        ("surprise", json!(true)),
        ("max_bytes", json!(3)),
        ("offset", json!(1)),
        ("pin_id", json!(pin)),
        ("sequence", Value::Null),
    ] {
        let mut bad = current.clone();
        bad[key] = value;
        assert!(
            matches!(
                dispatch::call(&service, "initial_task", bad).await,
                Err(Error::Validation(_))
            ),
            "{key}"
        );
    }
    let mut no_pin = strict.clone();
    no_pin.as_object_mut().unwrap().remove("pin_id");
    assert!(matches!(
        dispatch::call(&service, "initial_task", no_pin).await,
        Err(Error::Validation(_))
    ));

    // Support is discoverable only on request, and no worker capability gains the read.
    let default = dispatch::call(&service, "capabilities", json!({}))
        .await
        .unwrap();
    assert!(default.get("initial_task").is_none());
    let flagged = dispatch::call(
        &service,
        "capabilities",
        json!({"include_initial_task":true}),
    )
    .await
    .unwrap();
    let typed: agent_run_domain::observation::CapabilitiesView =
        serde_json::from_value(flagged).unwrap();
    assert_eq!(
        typed.initial_task,
        Some(agent_run_domain::initial_task::InitialTaskCapability::current())
    );
    assert!(
        typed
            .read_methods
            .iter()
            .any(|method| method.name == "initial_task")
    );
    assert!(
        !agent_run_domain::tools::worker_registry()
            .iter()
            .any(|tool| tool.name == "initial_task")
    );
}
