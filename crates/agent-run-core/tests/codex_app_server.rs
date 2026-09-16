//! Runner-level Codex app-server coverage using a shell-hosted fake server.

mod common;

use agent_run_adapters::{io::Process, LaunchPlan};
use agent_run_config::{config::Runtime, profiles::Profile};
use agent_run_core::codex;
use agent_run_domain::domain::Status;
use agent_run_platform::fs;
use serde_json::json;
use std::{collections::BTreeMap, path::PathBuf};

/// Builds the read-only profile used by the fake app-server runner tests.
fn profile() -> Profile {
    Profile {
        name: "review".into(),
        body: "Review the fixture.".into(),
        write: false,
        network: false,
        revision: "fixture".into(),
        canonical: false,
        allow_external_read_roots: true,
        read_roots: vec![],
        skills: vec![],
        mcp: vec![],
        required_constraints: Default::default(),
    }
}

/// Creates the minimal Codex runtime accepted by the runner under test.
fn runtime(home: PathBuf) -> Runtime {
    serde_json::from_value(json!({
        "enabled": true,
        "adapter": "codex",
        "binary": "/bin/sh",
        "home": home,
        "models": ["fixture"],
    }))
    .expect("fixture runtime")
}

/// Responds to the runner's fixed handshake, roster, thread, and turn sequence.
fn fake_plan() -> LaunchPlan {
    LaunchPlan {
        binary: PathBuf::from("/bin/sh"),
        args: vec!["-c".into(), r#"n=0; while IFS= read -r line; do n=$((n+1)); case "$n" in 1) printf '%s\n' '{"id":1,"result":{}}' ;; 3) printf '%s\n' '{"id":2,"result":{"data":[{"id":"fixture"}]}}' ;; 4) case "$line" in *'"permissions"'*) printf '%s\n' '{"id":3,"result":{"model":"fixture","cwd":"/private/tmp","runtimeWorkspaceRoots":["/private/tmp"],"sandbox":{"type":"readOnly","networkAccess":false},"approvalPolicy":"never","activePermissionProfile":{"id":":read-only"},"threadId":"thread"}}' ;; *) printf '%s\n' '{"id":3,"result":{"model":"fixture","cwd":"/private/tmp","roots":["/private/tmp"],"writableRoots":[],"sandbox":"read-only","approvalPolicy":"never","threadId":"thread"}}' ;; esac ;; 5) printf '%s\n' '{"id":4,"result":{"turn":{"id":"turn"}}}'; printf '%s\n' '{"method":"item/completed","params":{"threadId":"thread","turnId":"turn","item":{"type":"commandExecution"}}}'; printf '%s\n' '{"method":"turn/completed","params":{"threadId":"thread","turn":{"id":"turn","status":"completed","items":[]}}}' ;; esac; done"#.into()],
        cwd: PathBuf::from("/private/tmp"),
        environment: BTreeMap::new(),
        initial_input: None,
    }
}

/// Mirrors `test_codex_adapter.py::test_models_missing_cache_refreshes_live_roster_and_writes_cache`.
/// Mirrors `test_codex_app_server.py::test_unknown_method_forwards_its_params`.
#[tokio::test]
async fn python_test_codex_app_server_unknown_item_completion_is_durable() {
    let fixture = common::Home::new();
    let mut request = fixture.request();
    request.workdir = PathBuf::from("/private/tmp");
    request.validate().expect("fixture request");
    let (id, _) = fixture
        .store()
        .admit(&request, &fixture.config, &json!({}), None)
        .expect("admit fixture");
    let mut store = fixture.store();
    let record = store.get(&id).expect("admitted row");
    let app_home = fixture.path.join("codex-home");
    fs::private_dir(&app_home).expect("owned Codex home");
    let mut process = Process::spawn(&fake_plan()).expect("fake app-server starts");

    let result = codex::run(
        &mut process,
        &mut store,
        &record,
        &runtime(fixture.path.join("runtime")),
        &profile(),
        &app_home,
    )
    .await
    .expect("fake turn succeeds");

    assert_eq!(result.outcome.status, Status::Succeeded);
    assert!(app_home.join("cache/models.json").is_file());
    assert_eq!(
        store
            .last_event(&id, "item/completed")
            .expect("read forwarded event"),
        Some(json!({"threadId":"thread","turnId":"turn","item":{"type":"commandExecution"}}))
    );
    drop(process.input.take());
    process.reap().await;
}
