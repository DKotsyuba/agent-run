//! Dispatch parity tests for the transport-neutral public tool table.
mod common;

use agent_run_core::{dispatch, policy, profiles, service::LaunchIdentity, service::Service};
use agent_run_domain::domain::Status;
use serde_json::{json, Value};
use std::collections::BTreeSet;

/// Creates the launch identity required by the steer capability gate.
fn identity(home: &common::Home) -> Value {
    let request = home.request();
    let runtime = home.config.runtime(&request.runtime).unwrap();
    let profile = profiles::Profile {
        name: request.profile.clone(),
        body: "fixture".into(),
        write: request.write,
        network: false,
        revision: "fixture".into(),
        canonical: false,
        allow_external_read_roots: true,
        read_roots: request.read_roots.clone(),
        skills: vec![],
        mcp: vec![],
        required_constraints: BTreeSet::new(),
    };
    serde_json::to_value(LaunchIdentity {
        rust_identity_version: 1,
        replay_request_sha256: None,
        config: home.config.clone(),
        effective_policy: policy::evaluate(&request.runtime, runtime, &profile),
        profile,
        runtime_home: None,
        snapshot_sha256: None,
    })
    .unwrap()
}

/// Mirrors `tests/test_dispatch.py::test_retained_agent_tools_dispatch`.
#[tokio::test]
async fn retained_agent_tools_dispatch() {
    let home = common::Home::new();
    let request = home.request();
    let (agent_id, _) = home
        .store()
        .admit(&request, &home.config, &identity(&home), None)
        .unwrap();
    let service = Service::new(home.path.clone());
    let id = agent_id.to_string();
    dispatch::call(&service, "cancel", json!({"agent_id": id}))
        .await
        .unwrap();
    dispatch::call(
        &service,
        "steer",
        json!({"agent_id": agent_id, "text": "continue"}),
    )
    .await
    .unwrap();
    dispatch::call(&service, "answer", json!({"agent_id": agent_id}))
        .await
        .unwrap();
    dispatch::call(
        &service,
        "transcript",
        json!({"agent_id": agent_id, "cursor": 2, "limit": 3}),
    )
    .await
    .unwrap();
    assert_eq!(
        home.store().get(&agent_id).unwrap().status,
        Status::Starting
    );
}

/// Mirrors `tests/test_dispatch.py::test_restored_operator_tools_dispatch`.
#[tokio::test]
async fn restored_operator_tools_dispatch() {
    let home = common::Home::new();
    let service = Service::new(home.path);
    dispatch::call(&service, "models", json!({})).await.unwrap();
    dispatch::call(&service, "limits", json!({})).await.unwrap();
}
