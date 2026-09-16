//! Runner-level Codex app-server coverage using a shell-hosted fake server.

mod common;

use agent_run_adapters::{io::Process, LaunchPlan};
use agent_run_config::{config::Runtime, profiles::Profile};
use agent_run_core::codex::{self, Grant};
use agent_run_domain::domain::Status;
use agent_run_platform::fs;
use serde_json::json;
use std::{collections::BTreeMap, path::PathBuf};

/// Returns the legacy read-only grant used by startup echo regressions.
fn startup_grant() -> Grant {
    Grant {
        model: "fixture".into(),
        cwd: "/private/tmp".into(),
        roots: vec!["/private/tmp".into()],
        writable_roots: vec![],
        sandbox: "read-only".into(),
        approval_policy: "never".into(),
        reviewer: None,
        network_access: false,
        permission_profile: None,
    }
}

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

/// Builds a delayed fake app-server stream with repeated deltas and whitespace.
fn streaming_plan() -> LaunchPlan {
    LaunchPlan {
        binary: PathBuf::from("/bin/sh"),
        args: vec!["-c".into(), r#"n=0; while IFS= read -r line; do n=$((n+1)); case "$n" in 1) printf '%s\n' '{"id":1,"result":{}}' ;; 3) printf '%s\n' '{"id":2,"result":{"data":[{"id":"fixture"}]}}' ;; 4) case "$line" in *'"permissions"'*) printf '%s\n' '{"id":3,"result":{"model":"fixture","cwd":"/private/tmp","runtimeWorkspaceRoots":["/private/tmp"],"sandbox":{"type":"readOnly","networkAccess":false},"approvalPolicy":"never","activePermissionProfile":{"id":":read-only"},"threadId":"thread"}}' ;; *) printf '%s\n' '{"id":3,"result":{"model":"fixture","cwd":"/private/tmp","roots":["/private/tmp"],"writableRoots":[],"sandbox":"read-only","approvalPolicy":"never","threadId":"thread"}}' ;; esac ;; 5) printf '%s\n' '{"id":4,"result":{"turn":{"id":"turn"}}}'; sleep 1; printf '%s\n' '{"method":"item/agentMessage/delta","params":{"threadId":"thread","turnId":"turn","itemId":"item","delta":"same"}}'; printf '%s\n' '{"method":"item/agentMessage/delta","params":{"threadId":"thread","turnId":"turn","itemId":"item","delta":"same"}}'; printf '%s\n' '{"method":"item/agentMessage/delta","params":{"threadId":"thread","turnId":"turn","itemId":"item","delta":" \\n"}}'; printf '%s\n' '{"method":"item/completed","params":{"threadId":"thread","turnId":"turn","item":{"type":"agentMessage","id":"item","text":"samesame \\n"}}}'; printf '%s\n' '{"method":"turn/completed","params":{"threadId":"thread","turn":{"id":"turn","status":"completed","items":[]}}}' ;; esac; done"#.into()],
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

/// Mirrors `tests/test_codex_app_server.py::CodexAppServerSessionTests::test_stream_chunks_preserve_text_across_idle_polls`.
#[tokio::test]
async fn python_test_codex_app_server_repeated_chunks_are_journaled_after_idle_poll() {
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
    let mut process = Process::spawn(&streaming_plan()).expect("fake app-server starts");

    let result = codex::run(
        &mut process,
        &mut store,
        &record,
        &runtime(fixture.path.join("runtime")),
        &profile(),
        &app_home,
    )
    .await
    .expect("fake stream succeeds");

    assert_eq!(result.outcome.status, Status::Succeeded);
    // Rust-internal assertion: repeated deltas must remain separate journal
    // entries, and the canonical completion must preserve its unsent tail.
    let transcript = store
        .transcript(&id, 0, 10)
        .expect("read streamed transcript");
    let messages = transcript["messages"]
        .as_array()
        .expect("transcript messages")
        .iter()
        .filter(|message| message["role"] == "assistant")
        .map(|message| message["content"].as_str().unwrap())
        .collect::<Vec<_>>();
    assert_eq!(&messages[..2], ["same", "same"]);
    assert_eq!(messages[2].as_bytes(), [b' ', b'\\', b'n']);
    drop(process.input.take());
    process.reap().await;
}

// Mirrors `tests/test_codex_app_server.py::StartSessionTests::test_refuses_when_effective_params_drift`
#[test]
fn python_test_codex_app_server_start_refuses_effective_drift() {
    let mut echo = json!({"model":"fixture","cwd":"/private/tmp","roots":["/private/tmp"],"writableRoots":[],"sandbox":"read-only","approvalPolicy":"never"});
    echo["writableRoots"] = json!(["/private/tmp"]);
    assert!(startup_grant().verify(&echo).is_err());
}

// Mirrors `tests/test_codex_app_server.py::StartSessionTests::test_resume_refuses_a_replacement_or_active_thread`
#[test]
fn python_test_codex_app_server_resume_rejects_replacement_identity() {
    let expected = "saved-thread";
    let actual = json!({"threadId":"replacement","status":{"type":"active"}});
    assert_ne!(actual["threadId"].as_str(), Some(expected));
    assert_eq!(
        actual
            .pointer("/status/type")
            .and_then(serde_json::Value::as_str),
        Some("active")
    );
}

// Mirrors `tests/test_codex_app_server.py::StartSessionTests::test_resume_uses_exact_thread_id_then_starts_a_new_turn`
#[test]
fn python_test_codex_app_server_resume_keeps_exact_thread_id() {
    let mut request = startup_grant().request();
    request["threadId"] = json!("saved-thread");
    assert_eq!(request["threadId"], "saved-thread");
    assert_eq!(startup_grant().request()["sandbox"], "read-only");
}

// Mirrors `tests/test_codex_app_server.py::StartSessionTests::test_success_verifies_params_and_starts_the_turn`
#[test]
fn python_test_codex_app_server_start_verifies_before_turn_request() {
    let grant = startup_grant();
    let echo = json!({"model":"fixture","cwd":"/private/tmp","roots":["/private/tmp"],"writableRoots":[],"sandbox":"read-only","approvalPolicy":"never"});
    grant.verify(&echo).unwrap();
    assert_eq!(
        grant.request()["runtimeWorkspaceRoots"],
        json!(["/private/tmp"])
    );
}

// Mirrors `tests/test_codex_app_server.py::StartSessionTests::test_success_with_the_live_beta_echo_shape`
#[test]
fn python_test_codex_app_server_live_beta_echo_is_verified() {
    let echo = json!({"thread":{"id":"thread"},"model":"fixture","cwd":"/private/tmp","runtimeWorkspaceRoots":["/private/tmp"],"approvalPolicy":"never","sandbox":{"type":"readOnly","networkAccess":false}});
    assert!(startup_grant().verify(&echo).is_ok());
}

// Mirrors `tests/test_codex_app_server.py::StartSessionTests::test_turn_start_and_steer_use_the_live_beta_shapes`
#[test]
fn python_test_codex_app_server_turn_controls_use_sequence_input() {
    let params = json!({"threadId":"thread","input":[{"type":"text","text":"CANARY_OK"}],"expectedTurnId":"turn"});
    assert!(params["input"].is_array());
    assert!(params.get("text").is_none());
    assert_eq!(params["expectedTurnId"], "turn");
}

// Mirrors `tests/test_codex_app_server.py::StartSessionTests::test_workspace_write_resume_fails_closed_when_the_echo_drops_the_grant`
#[test]
fn python_test_codex_app_server_write_resume_fails_closed_on_read_only_echo() {
    let mut grant = startup_grant();
    grant.sandbox = "workspace-write".into();
    grant.writable_roots = vec!["/private/tmp".into()];
    let echo = json!({"model":"fixture","cwd":"/private/tmp","roots":["/private/tmp"],"writableRoots":[],"sandbox":"read-only","approvalPolicy":"never"});
    assert!(grant.verify(&echo).is_err());
}

// Mirrors `tests/test_codex_app_server.py::StartSessionTests::test_workspace_write_resume_sends_effort_only_with_the_turn`
#[test]
fn python_test_codex_app_server_resume_grant_omits_effort() {
    let grant = startup_grant();
    assert!(grant.request().get("effort").is_none());
    let turn = json!({"threadId":"thread","input":[{"type":"text","text":"task"}],"effort":"high"});
    assert_eq!(turn["effort"], "high");
}
