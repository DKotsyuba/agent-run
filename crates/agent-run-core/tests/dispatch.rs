//! Dispatch parity tests for the transport-neutral public tool table.
mod common;

use agent_run_core::{dispatch, policy, profiles, service::LaunchIdentity, service::Service};
use agent_run_domain::domain::Status;
use serde_json::{Value, json};
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
        mcp_tools: Default::default(),
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

/// Mirrors `tests/test_dispatch.py::test_retained_agent_tools_dispatch`
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

/// Service and shared dispatch expose the same exact, filtered discovery page;
/// bad pagination, lifecycle values and unknown wait arguments stay typed.
#[tokio::test]
async fn list_pools_service_and_dispatch_share_strict_read_projection() {
    use agent_run_domain::pool::{ListPoolsQuery, PoolState};
    let home = common::Home::new();
    let store = home.store();
    for (id, state, at) in [
        ("pool-20260101-000000-012345678a", "open", 1.0),
        ("pool-20260102-000000-012345678b", "completed", 2.0),
    ] {
        store.conn.execute(
            "INSERT INTO pools(id,request_namespace,request_id,request_sha256,goal,acceptance_json,state,roster_revision,created_at,completed_at) \
             VALUES(?,'ns',?,?, 'goal','[]',?,1,?,?)",
            rusqlite::params![id, id, "0".repeat(64), state, at,
                (state == "completed").then_some(3.0)],
        ).unwrap();
    }
    let service = Service::new(home.path.clone());
    let revision = store.revision().unwrap();
    let first = dispatch::call(&service, "list_pools", json!({"limit": 1}))
        .await
        .unwrap();
    assert_eq!(first["total"], 2);
    assert_eq!(
        first["items"][0]["pool_id"],
        "pool-20260102-000000-012345678b"
    );
    assert_eq!(first["next_offset"], 1);
    let filtered = service
        .list_pools(ListPoolsQuery {
            state: Some(PoolState::Open),
            ..Default::default()
        })
        .unwrap();
    assert_eq!(filtered["total"], 1);
    assert_eq!(
        filtered["items"][0]["pool_id"],
        "pool-20260101-000000-012345678a"
    );
    let page = dispatch::call(&service, "list_pools", json!({"offset":1,"limit":1}))
        .await
        .unwrap();
    assert_eq!(page["items"], filtered["items"]);
    assert_eq!(page["complete"], true);
    assert_eq!(store.revision().unwrap(), revision);
    for bad in [
        json!({"limit":0}),
        json!({"limit":201}),
        json!({"offset":-1}),
        json!({"state":"closed"}),
        json!({"after_revision":0}),
        json!({"wait_seconds":1}),
    ] {
        assert!(matches!(
            dispatch::call(&service, "list_pools", bad).await,
            Err(agent_run_domain::Error::Validation(_))
        ));
    }
}

/// Mirrors `tests/test_dispatch.py::test_restored_operator_tools_dispatch`
#[tokio::test]
async fn restored_operator_tools_dispatch() {
    let home = common::Home::new();
    let service = Service::new(home.path);
    dispatch::call(&service, "models", json!({})).await.unwrap();
    dispatch::call(&service, "limits", json!({})).await.unwrap();
}

/// Mirrors `tests/test_dispatch.py::test_cancel_returns_the_current_agent_view`.
///
/// The public cancel response is the agent view with its top-level `status`,
/// not the internal command acknowledgement, while the queued command keeps
/// its durable pending semantics.
#[tokio::test]
async fn cancel_returns_the_current_agent_view() {
    let home = common::Home::new();
    let request = home.request();
    let (agent_id, _) = home
        .store()
        .admit(&request, &home.config, &identity(&home), None)
        .unwrap();
    let service = Service::new(home.path.clone());
    let view = dispatch::call(&service, "cancel", json!({"agent_id": agent_id}))
        .await
        .unwrap();
    assert_eq!(view["agent_id"], agent_id.to_string());
    assert_eq!(view["status"], "starting");
    assert_eq!(view["runtime"], request.runtime);
    assert!(
        view.get("command_id").is_none(),
        "cancel must not return the command acknowledgement"
    );
    let pending = home
        .store()
        .conn
        .query_row(
            "SELECT COUNT(*) FROM commands WHERE agent_id=? AND kind='cancel' AND state='pending'",
            [agent_id.as_str()],
            |r| r.get::<_, i64>(0),
        )
        .unwrap();
    assert_eq!(pending, 1, "cancellation stays durably queued");
}
