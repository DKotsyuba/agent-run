//! Private checkpoint callback with real writer guards and no provider or socket.
mod common;

use agent_run_core::{dispatch, service::Service};
use agent_run_domain::{
    Error,
    storage::WriteAuthority,
    worker::{FinishGuard, MaintenancePauseRequest, WorkerCatalogProof},
};
use agent_run_store::ops::AcceptMaintenanceCheckpoint;
use serde_json::json;
use std::{collections::BTreeMap, time::Duration};

const TOKEN: &str = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
const ATTEMPT: &str = "maintenance-callback-attempt";
const CHALLENGE: &str = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";

/// A forged clean guard cannot bypass the writer; an installed supervisor
/// context rejects secrets, and an exact accepted retry survives guard loss.
#[tokio::test]
async fn maintenance_callback_uses_writer_guard_and_attempt_capability() {
    let home = common::Home::new();
    let mut store = home.store();
    let mut role = json!({
        "role_name":"review", "role_revision":"fixture", "prompt":"Review the task.",
        "grants":{"write":false,"network":false,"allow_external_read_roots":false,"read_roots":[]},
        "skills":[],"mcp":[],"required_constraints":[],"auth":{"mode":"global","reference":null},"worker_mcp":true,
    });
    role["config_revision"] = json!(agent_run_domain::canonical::sha256_hex(&role, true));
    let mut request = home.request();
    request.explicit_finish = true;
    request.output_schema = Some(
        json!({"type":"object","required":["answer"]})
            .as_object()
            .unwrap()
            .clone(),
    );
    let (id, _) = store
        .admit(
            &request,
            &home.config,
            &json!({"authority":{"role_payload":role}}),
            None,
        )
        .unwrap();
    store
        .conn
        .execute(
            "UPDATE agents SET status='running' WHERE id=?",
            [id.as_str()],
        )
        .unwrap();
    store.conn.execute("INSERT INTO attempts(id,agent_id,number,state,adapter_state_json,created_at,ownership_active) VALUES(?,?,1,'running','{}',1,1)",rusqlite::params![ATTEMPT,id.as_str()]).unwrap();
    store
        .issue_worker_capability(&id, ATTEMPT, TOKEN, 1.0)
        .unwrap();
    store.begin_worker_lifecycle(&id, ATTEMPT, 1.0).unwrap();
    store
        .register_worker_catalog(
            &WorkerCatalogProof {
                run_id: id.clone(),
                attempt_id: ATTEMPT.into(),
                token: TOKEN.into(),
                version: 5,
                digest: agent_run_domain::worker::maintenance_pause_catalog_digest(false),
            },
            1.0,
        )
        .unwrap();
    let epoch = store
        .begin_maintenance_fence("callback-fixture", 2.0)
        .unwrap()
        .epoch;
    store
        .request_maintenance_checkpoint(epoch, &id, ATTEMPT, CHALLENGE, 3.0)
        .unwrap();
    let prep = common::writer::prepare_supervisor(&mut store, &id);
    let writer = common::writer::Harness::start(&home.path);
    let mut supervisor = writer.supervisor(&home.path, prep, &id).await;
    let service = Service::new(home.path.clone()).with_writer(writer.broker());
    let input = MaintenancePauseRequest {
        challenge_id: CHALLENGE.into(),
        checkpoint: "Non-JSON checkpoint".into(),
    };
    let forged = AcceptMaintenanceCheckpoint {
        execution: id.clone(),
        attempt: ATTEMPT.into(),
        token: TOKEN.into(),
        input: input.clone(),
        at: agent_run_domain::domain::now(),
        guard: FinishGuard::Clean,
    };
    let lane = writer.broker().lane("maintenance-fixture");
    let authority = WriteAuthority::Worker {
        execution: id.clone(),
        attempt: ATTEMPT.into(),
    };
    let refused = lane
        .local_as(authority, forged, Duration::from_secs(5))
        .await
        .unwrap_err();
    assert!(matches!(refused, Error::Unsupported(_)));
    assert!(
        store
            .maintenance_checkpoint_intent(epoch, &id, ATTEMPT, false)
            .unwrap()
            .is_none()
    );
    let environment =
        BTreeMap::from([("MODEL_ACCESS_TOKEN".into(), "fixture-launch-secret".into())]);
    supervisor.install_finish_guard(&environment).await.unwrap();
    let call = |checkpoint: &str, token: &str| {
        json!({"run_id":id.as_str(),"attempt_id":ATTEMPT,"token":token,
        "tool":"maintenance_pause","input":{"challenge_id":CHALLENGE,"checkpoint":checkpoint}})
    };
    let secret = call("fixture-launch-secret", TOKEN);
    let error = dispatch::call(&service, agent_run_domain::worker::TOOL_METHOD, secret)
        .await
        .unwrap_err();
    let diagnostic = format!("{error:?}");
    assert!(
        !diagnostic.contains("fixture-launch-secret")
            && !diagnostic.contains(TOKEN)
            && !diagnostic.contains(CHALLENGE)
    );
    assert!(
        store
            .maintenance_checkpoint_intent(epoch, &id, ATTEMPT, false)
            .unwrap()
            .is_none()
    );
    assert!(
        dispatch::call(
            &service,
            agent_run_domain::worker::TOOL_METHOD,
            call(&input.checkpoint, &"c".repeat(64))
        )
        .await
        .is_err()
    );
    let receipt = dispatch::call(
        &service,
        agent_run_domain::worker::TOOL_METHOD,
        call(&input.checkpoint, TOKEN),
    )
    .await
    .unwrap();
    assert_eq!(receipt["duplicate"], false);
    assert!(!receipt.to_string().contains(&input.checkpoint));
    supervisor.producer().set_finish_guard(None).await.unwrap();
    let replay = dispatch::call(
        &service,
        agent_run_domain::worker::TOOL_METHOD,
        call(&input.checkpoint, TOKEN),
    )
    .await
    .unwrap();
    assert_eq!(replay["duplicate"], true);
    assert_eq!(replay["sha256"], receipt["sha256"]);
    let changed = dispatch::call(
        &service,
        agent_run_domain::worker::TOOL_METHOD,
        call("different checkpoint", TOKEN),
    )
    .await;
    assert!(matches!(changed, Err(Error::Conflict)));
    let stored: Option<MaintenancePauseRequest> = store
        .maintenance_checkpoint_intent(epoch, &id, ATTEMPT, false)
        .unwrap();
    assert_eq!(stored, Some(input));
    assert_eq!(
        store.get(&id).unwrap().status,
        agent_run_domain::domain::Status::Running
    );
    assert!(store.worker_finish_intent(&id, false).unwrap().is_none());
}
