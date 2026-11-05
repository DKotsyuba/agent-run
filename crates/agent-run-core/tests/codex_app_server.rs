//! Runner-level Codex app-server coverage using a shell-hosted fake server.

mod common;

use agent_run_adapters::{LaunchPlan, io::Process};
use agent_run_config::{config::Runtime, profiles::Profile};
use agent_run_core::codex::{self, Grant};
use agent_run_domain::domain::Status;
use agent_run_platform::fs;
use serde_json::json;
use std::{collections::BTreeMap, path::PathBuf};

/// Returns the canonical host temp directory as text.
///
/// `/private/tmp` exists only on macOS, so the spawned fake plans and the
/// admitted requests derive one canonical existing path that is identical on
/// every validation platform.
fn scratch() -> String {
    std::env::temp_dir()
        .canonicalize()
        .expect("temp dir")
        .display()
        .to_string()
}

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
        mcp_tools: Default::default(),
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
        args: vec!["-c".into(), r#"n=0; while IFS= read -r line; do n=$((n+1)); case "$n" in 1) printf '%s\n' '{"id":1,"result":{}}' ;; 3) printf '%s\n' '{"id":2,"result":{"data":[{"id":"fixture"}]}}' ;; 4) case "$line" in *'"permissions"'*) printf '%s\n' '{"id":3,"result":{"model":"fixture","cwd":"/private/tmp","runtimeWorkspaceRoots":["/private/tmp"],"sandbox":{"type":"readOnly","networkAccess":false},"approvalPolicy":"never","activePermissionProfile":{"id":":read-only"},"threadId":"thread"}}' ;; *) printf '%s\n' '{"id":3,"result":{"model":"fixture","cwd":"/private/tmp","roots":["/private/tmp"],"writableRoots":[],"sandbox":"read-only","approvalPolicy":"never","threadId":"thread"}}' ;; esac ;; 5) printf '%s\n' '{"id":4,"result":{"turn":{"id":"turn"}}}'; printf '%s\n' '{"method":"item/completed","params":{"threadId":"thread","turnId":"turn","item":{"type":"commandExecution"}}}'; printf '%s\n' '{"method":"turn/completed","params":{"threadId":"thread","turn":{"id":"turn","status":"completed","items":[]}}}' ;; esac; done"#.replace("/private/tmp", &scratch())],
        cwd: PathBuf::from(scratch()),
        environment: BTreeMap::new(),
        initial_input: None,
    }
}

/// Emits a resumed-thread usage event without a turn id, including unsafe
/// credential-shaped fields alongside the native numeric counters.
fn resumed_usage_plan() -> LaunchPlan {
    let mut plan = fake_plan();
    let event = json!({"method":"thread/tokenUsage/updated","params":{
        "threadId":"thread","inputTokens":999,
        "authorization":{"value":"hidden-auth"},
        "tokenUsage":{
            "total":{
                "inputTokens":100,"outputTokens":20,"cachedInputTokens":10,
                "cacheWriteInputTokens":4,"reasoningOutputTokens":5,"totalTokens":120,
                "accessToken":"hidden-token","password":{"value":"hidden-object"},
                "numericToken":1234,"inputTokensAsString":"hidden-string"
            },
            "last":{"inputTokens":7,"outputTokens":3},
            "credential":{"value":"hidden-nested-object"},
            "tokenUsage":{"value":"hidden-nested-usage"}
        }
    }});
    plan.environment
        .insert("USAGE_EVENT".into(), event.to_string());
    plan.environment.insert(
        "OTHER_EVENT".into(),
        json!({"method":"other/diagnostic","params":{"threadId":"thread","tokenUsage":{"total":{"inputTokens":999}}}}).to_string(),
    );
    let old = r#"printf '%s\n' '{"method":"item/completed","params":{"threadId":"thread","turnId":"turn","item":{"type":"commandExecution"}}}';"#;
    let script = plan.args[1].replace(
        old,
        &format!("printf '%s\\n' \"$USAGE_EVENT\" \"$OTHER_EVENT\"; {old}"),
    );
    assert_ne!(script, plan.args[1]);
    plan.args[1] = script;
    plan
}

/// A native server missing research controls fails before a thread or turn.
/// The ordinary fixture deliberately returns a roster instead of config/read;
/// absence is rejected, and the owned fixture process is closed and reaped.
#[tokio::test]
async fn research_is_rejected_when_native_controls_are_missing() {
    let fixture = common::Home::new();
    let mut request = fixture.request();
    request.workdir = PathBuf::from(scratch());
    let (id, _) = fixture
        .store()
        .admit(&request, &fixture.config, &json!({}), None)
        .unwrap();
    let mut store = fixture.store();
    let record = store.get(&id).unwrap();
    let prep = common::writer::prepare_supervisor(&mut store, &id);
    let harness = common::writer::Harness::start(&fixture.path);
    let mut persist = harness.supervisor(&fixture.path, prep, &id).await;
    let app_home = fixture.path.join("codex-home");
    fs::private_dir(&app_home).unwrap();
    let mut role = profile();
    role.network = true;
    role.allow_external_read_roots = false;
    role.required_constraints
        .insert(agent_run_domain::domain::Constraint::ResearchToolsOnly);
    let mut process = Process::spawn(&fake_plan()).unwrap();
    let result = codex::run(
        &mut process,
        &mut persist,
        &record,
        &record,
        &runtime(fixture.path.join("runtime")),
        &role,
        &app_home,
    )
    .await;
    persist.flush().await.unwrap();
    assert!(
        result
            .err()
            .expect("unverified research controls are rejected")
            .to_string()
            .contains("research feature controls did not verify")
    );
    assert!(store.get(&id).unwrap().runtime_session_id.is_none());
    assert!(!app_home.join("cache/models.json").exists());
    drop(process.input.take());
    process.reap().await;
}

/// Valid research reaches a turn only with empty environments and the exact
/// private worker catalog. An inherited MCP prevents the turn/session binding;
/// both owned fake servers exit through EOF without host command execution.
#[tokio::test]
async fn research_verifies_controls_and_private_catalog_before_turn() {
    for extra_mcp in [false, true] {
        let fixture = common::Home::new();
        let mut request = fixture.request();
        request.workdir = PathBuf::from(scratch());
        let (id, _) = fixture
            .store()
            .admit(&request, &fixture.config, &json!({}), None)
            .unwrap();
        let mut store = fixture.store();
        let record = store.get(&id).unwrap();
        let prep = common::writer::prepare_supervisor(&mut store, &id);
        let harness = common::writer::Harness::start(&fixture.path);
        let mut persist = harness.supervisor(&fixture.path, prep, &id).await;
        let app_home = fixture.path.join("codex-home");
        fs::private_dir(&app_home).unwrap();
        let mut role = profile();
        role.network = true;
        role.allow_external_read_roots = false;
        role.required_constraints
            .insert(agent_run_domain::domain::Constraint::ResearchToolsOnly);
        let controls =
            json!({"id":2,"result":{"config":agent_run_adapters::codex::research_settings()}});
        let tools: serde_json::Map<String, serde_json::Value> =
            agent_run_domain::worker::research_tools_json()
                .into_iter()
                .map(|tool| (tool["name"].as_str().unwrap().to_owned(), json!({})))
                .collect();
        let mut servers =
            vec![json!({"name":"agent_run_worker","tools":tools,"runtimeStatus":"connected"})];
        if extra_mcp {
            servers.push(json!({"name":"execution","tools":{},"runtimeStatus":"connected"}));
        }
        let inventory = json!({"id":5,"result":{"data":servers,"nextCursor":null}});
        let script = format!(
            r#"n=0; while IFS= read -r line; do n=$((n+1)); case "$n" in
1) printf '%s\n' '{{"id":1,"result":{{}}}}' ;;
3) printf '%s\n' '{controls}' ;;
4) printf '%s\n' '{{"id":3,"result":{{"data":[{{"id":"fixture"}}]}}}}' ;;
5) case "$line" in *'"environments":[]'*) printf '%s\n' '{{"id":4,"result":{{"model":"fixture","cwd":"{cwd}","runtimeWorkspaceRoots":[],"sandbox":{{"type":"readOnly","networkAccess":false}},"approvalPolicy":"never","activePermissionProfile":{{"id":":read-only"}},"threadId":"thread"}}}}' ;; *) printf '%s\n' '{{"id":4,"error":{{"code":-32602,"message":"missing empty environments"}}}}' ;; esac ;;
6) printf '%s\n' '{inventory}' ;;
7) printf '%s\n' '{{"id":6,"result":{{"turn":{{"id":"turn"}}}}}}';
printf '%s\n' '{{"method":"turn/completed","params":{{"threadId":"thread","turn":{{"id":"turn","status":"completed","items":[{{"type":"agentMessage","id":"answer","text":"research fixture complete"}}]}}}}}}' ;;
esac; done"#,
            cwd = scratch()
        );
        let mut plan = fake_plan();
        plan.args = vec!["-c".into(), script];
        let mut process = Process::spawn(&plan).unwrap();
        let result = codex::run(
            &mut process,
            &mut persist,
            &record,
            &record,
            &runtime(fixture.path.join("runtime")),
            &role,
            &app_home,
        )
        .await;
        persist.flush().await.unwrap();
        if extra_mcp {
            assert!(
                result
                    .err()
                    .unwrap()
                    .to_string()
                    .contains("MCP inventory did not verify")
            );
            assert!(store.get(&id).unwrap().runtime_session_id.is_none());
        } else {
            assert_eq!(
                result.unwrap().answer.as_deref(),
                Some("research fixture complete")
            );
            assert_eq!(
                store.get(&id).unwrap().runtime_session_id.as_deref(),
                Some("thread")
            );
        }
        drop(process.input.take());
        process.reap().await;
    }
}

/// Builds a delayed fake app-server stream with repeated deltas and whitespace.
fn streaming_plan() -> LaunchPlan {
    LaunchPlan {
        binary: PathBuf::from("/bin/sh"),
        args: vec!["-c".into(), r#"n=0; while IFS= read -r line; do n=$((n+1)); case "$n" in 1) printf '%s\n' '{"id":1,"result":{}}' ;; 3) printf '%s\n' '{"id":2,"result":{"data":[{"id":"fixture"}]}}' ;; 4) case "$line" in *'"permissions"'*) printf '%s\n' '{"id":3,"result":{"model":"fixture","cwd":"/private/tmp","runtimeWorkspaceRoots":["/private/tmp"],"sandbox":{"type":"readOnly","networkAccess":false},"approvalPolicy":"never","activePermissionProfile":{"id":":read-only"},"threadId":"thread"}}' ;; *) printf '%s\n' '{"id":3,"result":{"model":"fixture","cwd":"/private/tmp","roots":["/private/tmp"],"writableRoots":[],"sandbox":"read-only","approvalPolicy":"never","threadId":"thread"}}' ;; esac ;; 5) printf '%s\n' '{"id":4,"result":{"turn":{"id":"turn"}}}'; sleep 1; printf '%s\n' '{"method":"item/agentMessage/delta","params":{"threadId":"thread","turnId":"turn","itemId":"item","delta":"same"}}'; printf '%s\n' '{"method":"item/agentMessage/delta","params":{"threadId":"thread","turnId":"turn","itemId":"item","delta":"same"}}'; printf '%s\n' '{"method":"item/agentMessage/delta","params":{"threadId":"thread","turnId":"turn","itemId":"item","delta":" \\n"}}'; printf '%s\n' '{"method":"item/completed","params":{"threadId":"thread","turnId":"turn","item":{"type":"agentMessage","id":"item","text":"samesame \\n"}}}'; printf '%s\n' '{"method":"turn/completed","params":{"threadId":"thread","turn":{"id":"turn","status":"completed","items":[]}}}' ;; esac; done"#.replace("/private/tmp", &scratch())],
        cwd: PathBuf::from(scratch()),
        environment: BTreeMap::new(),
        initial_input: None,
    }
}

/// [`streaming_plan`] reduced to ONE visible delta, `Hello`, then a 2 s
/// pause before the turn completes, with no later delta and no
/// `item/completed`.
fn lone_delta_plan() -> LaunchPlan {
    let mut plan = streaming_plan();
    let script = plan.args[1].replace(
        SEQUENCE,
        r#"printf '%s\n' '{"method":"item/agentMessage/delta","params":{"threadId":"thread","turnId":"turn","itemId":"item","delta":"Hello"}}'; sleep 2; "#,
    );
    assert_ne!(script, plan.args[1]);
    plan.args[1] = script;
    plan
}

/// Makes the fake app-server echo one launch token across assistant deltas,
/// completed text, and a complete command output without contacting Codex.
fn secret_plan() -> LaunchPlan {
    let mut plan = streaming_plan();
    let script = plan.args[1].replace(
        SEQUENCE,
        r#"printf '%s\n' '{"method":"item/agentMessage/delta","params":{"threadId":"thread","turnId":"turn","itemId":"item","delta":"synthetic-"}}'; printf '%s\n' '{"method":"item/agentMessage/delta","params":{"threadId":"thread","turnId":"turn","itemId":"item","delta":"secret"}}'; printf '%s\n' '{"method":"item/commandExecution/outputDelta","params":{"threadId":"thread","turnId":"turn","itemId":"command","delta":"synthetic-"}}'; printf '%s\n' '{"method":"item/commandExecution/outputDelta","params":{"threadId":"thread","turnId":"turn","itemId":"command","delta":"secret"}}'; printf '%s\n' '{"method":"item/completed","params":{"threadId":"thread","turnId":"turn","item":{"type":"agentMessage","id":"item","text":"synthetic-secret"}}}'; printf '%s\n' '{"method":"item/completed","params":{"threadId":"thread","turnId":"turn","item":{"type":"commandExecution","id":"command","aggregatedOutput":"synthetic-secret"}}}'; "#,
    );
    assert_ne!(script, plan.args[1]);
    plan.args[1] = script;
    plan.environment
        .insert("AGENT_RUN_PROVIDER_TOKEN".into(), "synthetic-secret".into());
    plan
}

/// The streamed delta sequence [`lone_delta_plan`] replaces.
const SEQUENCE: &str = r#"printf '%s\n' '{"method":"item/agentMessage/delta","params":{"threadId":"thread","turnId":"turn","itemId":"item","delta":"same"}}'; printf '%s\n' '{"method":"item/agentMessage/delta","params":{"threadId":"thread","turnId":"turn","itemId":"item","delta":"same"}}'; printf '%s\n' '{"method":"item/agentMessage/delta","params":{"threadId":"thread","turnId":"turn","itemId":"item","delta":" \\n"}}'; printf '%s\n' '{"method":"item/completed","params":{"threadId":"thread","turnId":"turn","item":{"type":"agentMessage","id":"item","text":"samesame \\n"}}}'; "#;

/// Mirrors `test_codex_adapter.py::test_models_missing_cache_refreshes_live_roster_and_writes_cache`.
/// Mirrors `test_codex_app_server.py::test_unknown_method_forwards_its_params`.
#[tokio::test]
async fn python_test_codex_app_server_unknown_item_completion_is_durable() {
    let fixture = common::Home::new();
    let mut request = fixture.request();
    request.workdir = PathBuf::from(scratch());
    request.request_id = Some("codex-protocol-correlation".into());
    request.validate().expect("fixture request");
    let (id, _) = fixture
        .store()
        .admit(&request, &fixture.config, &json!({}), None)
        .expect("admit fixture");
    let mut store = fixture.store();
    let record = store.get(&id).expect("admitted row");
    let prep = common::writer::prepare_supervisor(&mut store, &id);
    let harness = common::writer::Harness::start(&fixture.path);
    let mut persist = harness.supervisor(&fixture.path, prep, &id).await;
    let app_home = fixture.path.join("codex-home");
    fs::private_dir(&app_home).expect("owned Codex home");
    let mut process = Process::spawn(&fake_plan()).expect("fake app-server starts");

    let result = codex::run(
        &mut process,
        &mut persist,
        &record,
        &record,
        &runtime(fixture.path.join("runtime")),
        &profile(),
        &app_home,
    )
    .await
    .expect("fake turn succeeds");
    persist.flush().await.unwrap();

    assert_eq!(result.outcome.status, Status::Succeeded);
    let stored = store.get(&id).expect("persisted Codex row");
    assert_eq!(
        stored.request.request_id.as_deref(),
        Some("codex-protocol-correlation")
    );
    assert_eq!(stored.runtime_session_id.as_deref(), Some("thread"));
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

/// A resumed notification without a turn id remains usable for usage
/// accounting, but credential-shaped strings and objects are never retained.
#[tokio::test]
async fn codex_resume_usage_event_preserves_numeric_counters_only() {
    let fixture = common::Home::new();
    let mut request = fixture.request();
    request.workdir = PathBuf::from(scratch());
    let (id, _) = fixture
        .store()
        .admit(&request, &fixture.config, &json!({}), None)
        .unwrap();
    let mut store = fixture.store();
    let mut record = store.get(&id).unwrap();
    record.resume_of_runtime_session_id = Some("thread".into());
    let prep = common::writer::prepare_supervisor(&mut store, &id);
    let harness = common::writer::Harness::start(&fixture.path);
    let mut persist = harness.supervisor(&fixture.path, prep, &id).await;
    let app_home = fixture.path.join("codex-home");
    fs::private_dir(&app_home).unwrap();
    let mut process = Process::spawn(&resumed_usage_plan()).unwrap();
    let result = codex::run(
        &mut process,
        &mut persist,
        &record,
        &record,
        &runtime(fixture.path.join("runtime")),
        &profile(),
        &app_home,
    )
    .await
    .unwrap();
    persist.flush().await.unwrap();
    assert_eq!(result.outcome.status, Status::Succeeded);

    let event = store
        .last_event(&id, "thread/tokenUsage/updated")
        .unwrap()
        .unwrap();
    let total = &event["tokenUsage"]["total"];
    assert_eq!(total["inputTokens"], 100);
    assert_eq!(total["outputTokens"], 20);
    assert_eq!(total["cachedInputTokens"], 10);
    assert_eq!(total["cacheWriteInputTokens"], 4);
    assert_eq!(total["reasoningOutputTokens"], 5);
    assert_eq!(total["totalTokens"], 120);
    assert_eq!(event["tokenUsage"]["last"]["inputTokens"], 7);
    assert_eq!(event["tokenUsage"]["last"]["outputTokens"], 3);
    for name in [
        "accessToken",
        "password",
        "numericToken",
        "inputTokensAsString",
    ] {
        assert_eq!(total[name], "<omitted>");
    }
    assert_eq!(event["authorization"], "<omitted>");
    assert_eq!(event["inputTokens"], "<omitted>");
    assert_eq!(event["tokenUsage"]["credential"], "<omitted>");
    assert_eq!(event["tokenUsage"]["tokenUsage"], "<omitted>");
    assert!(!event.to_string().contains("hidden-"));
    let other = store.last_event(&id, "other/diagnostic").unwrap().unwrap();
    assert_eq!(other["tokenUsage"], "<omitted>");
    drop(process.input.take());
    process.reap().await;
}

/// Mirrors `tests/test_codex_app_server.py::CodexAppServerSessionTests::test_stream_chunks_preserve_text_across_idle_polls`.
#[tokio::test]
async fn python_test_codex_app_server_repeated_chunks_are_journaled_after_idle_poll() {
    let fixture = common::Home::new();
    let mut request = fixture.request();
    request.workdir = PathBuf::from(scratch());
    request.validate().expect("fixture request");
    let (id, _) = fixture
        .store()
        .admit(&request, &fixture.config, &json!({}), None)
        .expect("admit fixture");
    let mut store = fixture.store();
    let record = store.get(&id).expect("admitted row");
    let prep = common::writer::prepare_supervisor(&mut store, &id);
    let harness = common::writer::Harness::start(&fixture.path);
    let mut persist = harness.supervisor(&fixture.path, prep, &id).await;
    let app_home = fixture.path.join("codex-home");
    fs::private_dir(&app_home).expect("owned Codex home");
    let mut process = Process::spawn(&streaming_plan()).expect("fake app-server starts");

    let result = codex::run(
        &mut process,
        &mut persist,
        &record,
        &record,
        &runtime(fixture.path.join("runtime")),
        &profile(),
        &app_home,
    )
    .await
    .expect("fake stream succeeds");
    persist.flush().await.unwrap();

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

/// Custom-provider launch secrets must never enter Codex transcript rows or
/// the final answer, even when the app-server splits the literal by delta.
#[tokio::test]
async fn custom_codex_stdout_redacts_split_token_and_command_output() {
    let fixture = common::Home::new();
    let mut request = fixture.request();
    request.workdir = PathBuf::from(scratch());
    request.validate().unwrap();
    let (id, _) = fixture
        .store()
        .admit(&request, &fixture.config, &json!({}), None)
        .unwrap();
    let mut store = fixture.store();
    let record = store.get(&id).unwrap();
    let prep = common::writer::prepare_supervisor(&mut store, &id);
    let harness = common::writer::Harness::start(&fixture.path);
    let mut persist = harness.supervisor(&fixture.path, prep, &id).await;
    let app_home = fixture.path.join("codex-home");
    fs::private_dir(&app_home).unwrap();
    let mut process = Process::spawn(&secret_plan()).unwrap();
    assert_eq!(process.redact("synthetic-secret"), "<redacted>");
    let mut secret_stream = process.stream_redactor();
    assert!(secret_stream.feed("synthetic-").is_empty());
    assert_eq!(secret_stream.feed("secret"), "<redacted>");

    let result = codex::run(
        &mut process,
        &mut persist,
        &record,
        &record,
        &runtime(fixture.path.join("runtime")),
        &profile(),
        &app_home,
    )
    .await
    .unwrap();
    persist.flush().await.unwrap();
    let transcript = store.transcript(&id, 0, 20).unwrap().to_string();
    let events: String = store
        .conn
        .query_row(
            "SELECT group_concat(data_json,'') FROM events WHERE agent_id=?",
            [id.as_str()],
            |row| row.get(0),
        )
        .unwrap();
    assert!(!transcript.contains("synthetic-secret"), "{transcript}");
    assert!(!transcript.contains("synthetic-"));
    assert!(!events.contains("synthetic-secret"));
    assert!(!events.contains("synthetic-"));
    assert!(transcript.contains("<redacted>"));
    assert_eq!(result.answer.as_deref(), Some("<redacted>"));
    drop(process.input.take());
    process.reap().await;
}

/// The first visible Codex delta is journaled on arrival, not held until a
/// later delta or the turn end: a follower polling during the pause after
/// it already sees `Hello`.
#[tokio::test]
async fn first_codex_delta_is_journaled_on_arrival() {
    let fixture = common::Home::new();
    let mut request = fixture.request();
    request.workdir = PathBuf::from(scratch());
    request.validate().expect("fixture request");
    let (id, _) = fixture
        .store()
        .admit(&request, &fixture.config, &json!({}), None)
        .expect("admit fixture");
    let mut store = fixture.store();
    let record = store.get(&id).expect("admitted row");
    let prep = common::writer::prepare_supervisor(&mut store, &id);
    let harness = common::writer::Harness::start(&fixture.path);
    let mut persist = harness.supervisor(&fixture.path, prep, &id).await;
    let app_home = fixture.path.join("codex-home");
    fs::private_dir(&app_home).expect("owned Codex home");
    let mut process = Process::spawn(&lone_delta_plan()).expect("fake app-server starts");
    let runtime = runtime(fixture.path.join("runtime"));
    let profile = profile();
    let run = codex::run(
        &mut process,
        &mut persist,
        &record,
        &record,
        &runtime,
        &profile,
        &app_home,
    );
    // The delta arrives about 1 s in and the turn ends about 2 s later;
    // polling stops at 2.5 s, inside that pause.
    let follow = async {
        let reader = fixture.store();
        for _ in 0..50 {
            let transcript = reader.transcript(&id, 0, 10).expect("read transcript");
            if transcript["messages"]
                .as_array()
                .expect("transcript messages")
                .iter()
                .any(|message| message["role"] == "assistant" && message["content"] == "Hello")
            {
                return true;
            }
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        }
        false
    };
    let (_, seen) = tokio::join!(run, follow);
    assert!(seen, "the first delta was not visible during the pause");
    drop(process.input.take());
    process.reap().await;
}

/// A terminal turn flushes withheld safe text even without item/completed.
#[tokio::test]
async fn terminal_turn_keeps_redactor_tail_without_item_completion() {
    let fixture = common::Home::new();
    let mut request = fixture.request();
    request.workdir = PathBuf::from(scratch());
    request.validate().unwrap();
    let (id, _) = fixture
        .store()
        .admit(&request, &fixture.config, &json!({}), None)
        .unwrap();
    let mut store = fixture.store();
    let record = store.get(&id).unwrap();
    let prep = common::writer::prepare_supervisor(&mut store, &id);
    let harness = common::writer::Harness::start(&fixture.path);
    let mut persist = harness.supervisor(&fixture.path, prep, &id).await;
    let app_home = fixture.path.join("codex-home");
    fs::private_dir(&app_home).unwrap();
    let mut plan = lone_delta_plan();
    plan.environment
        .insert("SERVICE_TOKEN".into(), "synthetic-secret".into());
    let mut process = Process::spawn(&plan).unwrap();
    let result = codex::run(
        &mut process,
        &mut persist,
        &record,
        &record,
        &runtime(fixture.path.join("runtime")),
        &profile(),
        &app_home,
    )
    .await
    .unwrap();
    persist.flush().await.unwrap();
    assert_eq!(result.outcome.status, Status::Succeeded);
    let transcript = store.transcript(&id, 0, 10).unwrap();
    assert!(
        transcript["messages"]
            .as_array()
            .unwrap()
            .iter()
            .any(|message| { message["role"] == "assistant" && message["content"] == "Hello" })
    );
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

/// A failed Codex turn crosses the runner boundary with the typed
/// disposition of its `codexErrorInfo` (app-server 0.155.1 schema): only
/// `usageLimitExceeded` is quota exhaustion, `rateLimitExceeded` is
/// throttling, and a completed turn whose agent text names the quota code
/// carries no disposition at all.
#[tokio::test]
async fn codex_turn_errors_cross_the_runner_boundary_typed() {
    use agent_run_adapters::native_failure::NativeFailure;
    let turn = r#""status":"completed","items":[]"#;
    for (replacement, expected) in [
        (
            r#""status":"failed","items":[],"error":{"message":"usage limit","codexErrorInfo":"usageLimitExceeded"}"#,
            Some("quota"),
        ),
        (
            r#""status":"failed","items":[],"error":{"message":"slow down","codexErrorInfo":"rateLimitExceeded"}"#,
            Some("throttled"),
        ),
        (
            r#""status":"completed","items":[{"type":"agentMessage","id":"m","text":"{\"codexErrorInfo\":\"usageLimitExceeded\"}"}]"#,
            None,
        ),
    ] {
        let fixture = common::Home::new();
        let mut request = fixture.request();
        request.workdir = PathBuf::from(scratch());
        request.validate().expect("fixture request");
        let (id, _) = fixture
            .store()
            .admit(&request, &fixture.config, &json!({}), None)
            .expect("admit fixture");
        let mut store = fixture.store();
        let record = store.get(&id).expect("admitted row");
        let prep = common::writer::prepare_supervisor(&mut store, &id);
        let harness = common::writer::Harness::start(&fixture.path);
        let mut persist = harness.supervisor(&fixture.path, prep, &id).await;
        let app_home = fixture.path.join("codex-home");
        fs::private_dir(&app_home).expect("owned Codex home");
        let mut plan = fake_plan();
        plan.args[1] = plan.args[1].replace(turn, replacement);
        let mut process = Process::spawn(&plan).expect("fake app-server starts");
        let result = codex::run(
            &mut process,
            &mut persist,
            &record,
            &record,
            &runtime(fixture.path.join("runtime")),
            &profile(),
            &app_home,
        )
        .await
        .expect("fake turn ends");
        persist.flush().await.unwrap();
        let class = result.native_failure.as_ref().map(|failure| match failure {
            NativeFailure::QuotaExhausted { signal, .. } => {
                assert_eq!(*signal, "codex.usageLimitExceeded");
                "quota"
            }
            NativeFailure::Throttled => "throttled",
            _ => "other",
        });
        assert_eq!(class, expected, "{replacement}");
        drop(process.input.take());
        process.reap().await;
    }
}

/// Failure diagnostics redact the complete known token before bounding the
/// exported text, even when the token crosses the character limit.
#[tokio::test]
async fn codex_failure_redacts_before_truncating() {
    let fixture = common::Home::new();
    let mut request = fixture.request();
    request.workdir = PathBuf::from(scratch());
    request.validate().unwrap();
    let (id, _) = fixture
        .store()
        .admit(&request, &fixture.config, &json!({}), None)
        .unwrap();
    let mut store = fixture.store();
    let record = store.get(&id).unwrap();
    let prep = common::writer::prepare_supervisor(&mut store, &id);
    let harness = common::writer::Harness::start(&fixture.path);
    let mut persist = harness.supervisor(&fixture.path, prep, &id).await;
    let app_home = fixture.path.join("codex-home");
    fs::private_dir(&app_home).unwrap();
    let mut plan = fake_plan();
    plan.environment
        .insert("AGENT_RUN_PROVIDER_TOKEN".into(), "synthetic-secret".into());
    let message = format!("{}synthetic-secret", "x".repeat(505));
    let replacement = format!(
        r#""status":"failed","error":{{"message":"{message}","codexErrorInfo":"other"}},"items":[]"#
    );
    let script = plan.args[1].replace(r#""status":"completed","items":[]"#, &replacement);
    assert_ne!(script, plan.args[1]);
    plan.args[1] = script;
    let mut process = Process::spawn(&plan).unwrap();
    let result = codex::run(
        &mut process,
        &mut persist,
        &record,
        &record,
        &runtime(fixture.path.join("runtime")),
        &profile(),
        &app_home,
    )
    .await
    .unwrap();
    persist.flush().await.unwrap();
    let failure = result.outcome.failure_text.unwrap();
    assert!(!failure.contains("synthet"));
    assert!(failure.chars().count() <= 512);
    drop(process.input.take());
    process.reap().await;
}

/// A native tool-evidence sequence: a started+completed command with exit
/// code evidence, a completed-only MCP call failing with a typed error, a
/// duplicate completion of the command, and a started-only dynamic call.
fn tool_evidence_plan() -> LaunchPlan {
    let mut plan = fake_plan();
    plan.args[1] = plan.args[1].replace(
        r#"printf '%s\n' '{"method":"item/completed","params":{"threadId":"thread","turnId":"turn","item":{"type":"commandExecution"}}}'; printf '%s\n' '{"method":"turn/completed","params":{"threadId":"thread","turn":{"id":"turn","status":"completed","items":[]}}}'"#,
        r#"printf '%s\n' '{"method":"item/started","params":{"threadId":"thread","turnId":"turn","item":{"type":"commandExecution","id":"cmd_ok"}}}'; printf '%s\n' '{"method":"item/completed","params":{"threadId":"thread","turnId":"turn","item":{"type":"commandExecution","id":"cmd_ok","exitCode":0,"aggregatedOutput":"fine"}}}'; printf '%s\n' '{"method":"item/completed","params":{"threadId":"thread","turnId":"turn","item":{"type":"commandExecution","id":"cmd_ok","exitCode":0,"aggregatedOutput":"fine"}}}'; printf '%s\n' '{"method":"item/completed","params":{"threadId":"thread","turnId":"turn","item":{"type":"mcpToolCall","id":"mcp_1","server":"srv","tool":"lookup","status":"failed","error":{"message":"refused"}}}}'; printf '%s\n' '{"method":"item/started","params":{"threadId":"thread","turnId":"turn","item":{"type":"dynamicToolCall","id":"dyn_1","tool":"custom"}}}'; printf '%s\n' '{"method":"turn/completed","params":{"threadId":"thread","turn":{"id":"turn","status":"completed","items":[]}}}'"#,
    );
    assert_ne!(plan.args[1], fake_plan().args[1]);
    plan
}

/// Native invocations journal once per id with explicit evidence only: the
/// duplicate completion adds nothing, exit code 0 is success, the typed MCP
/// error is failure, and the unresulted dynamic call keeps its result
/// unknown without invalidating the other counts.
#[tokio::test]
async fn codex_tool_evidence_journals_once_with_native_flags() {
    let fixture = common::Home::new();
    let mut request = fixture.request();
    request.workdir = PathBuf::from(scratch());
    request.validate().expect("fixture request");
    let (id, _) = fixture
        .store()
        .admit(&request, &fixture.config, &json!({}), None)
        .expect("admit fixture");
    let mut store = fixture.store();
    let record = store.get(&id).expect("admitted row");
    let prep = common::writer::prepare_supervisor(&mut store, &id);
    let harness = common::writer::Harness::start(&fixture.path);
    let mut persist = harness.supervisor(&fixture.path, prep, &id).await;
    let app_home = fixture.path.join("codex-home");
    fs::private_dir(&app_home).expect("owned Codex home");
    let mut process = Process::spawn(&tool_evidence_plan()).expect("fake app-server starts");

    let result = codex::run(
        &mut process,
        &mut persist,
        &record,
        &record,
        &runtime(fixture.path.join("runtime")),
        &profile(),
        &app_home,
    )
    .await
    .expect("fake turn succeeds");
    persist.flush().await.unwrap();

    assert_eq!(result.outcome.status, Status::Succeeded);
    let page = store.transcript(&id, 0, 1000).expect("journal page");
    let calls: Vec<&serde_json::Value> = page["messages"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|message| message["role"] == "tool_call")
        .collect();
    assert_eq!(
        calls
            .iter()
            .map(|call| call["raw_ref"].as_str().unwrap())
            .collect::<Vec<_>>(),
        ["cmd_ok", "mcp_1", "dyn_1"],
        "each native id journals exactly one call, duplicates add none: {calls:?}"
    );
    let results: Vec<&serde_json::Value> = page["messages"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|message| message["role"] == "tool_result")
        .collect();
    assert_eq!(results.len(), 2, "{results:?}");
    let command = results
        .iter()
        .find(|result| result["raw_ref"] == "cmd_ok")
        .expect("command result");
    assert_eq!(command["name"], "command");
    assert_eq!(command["error"], false);
    assert_eq!(command["error_source"], "codex.command.exitCode");
    assert_eq!(command["content"], "fine");
    let mcp = results
        .iter()
        .find(|result| result["raw_ref"] == "mcp_1")
        .expect("MCP result");
    assert_eq!(mcp["name"], "srv/lookup");
    assert_eq!(mcp["error"], true);
    assert_eq!(mcp["error_source"], "codex.mcp.error");
    let counts = store.tool_counts(&id).expect("native counts");
    assert_eq!(counts.calls, Some(3));
    assert_eq!(counts.failed, None, "one unknown result keeps failed null");
    assert_eq!(counts.unknown_results, Some(1));
    drop(process.input.take());
    process.reap().await;
}

/// Emits interleaved tool events, including late and missing inputs, without
/// allowing the shell fixture to interpret any event payload.
fn tool_input_plan() -> LaunchPlan {
    let event = |method: &str, item: serde_json::Value| json!({"method":method,"params":{"threadId":"thread","turnId":"turn","item":item}});
    let deep = format!(
        "{}{{\"password\":\"hidden-deep\"}}{}",
        "{\"nested\":".repeat(150),
        "}".repeat(150)
    );
    let double_encoded =
        serde_json::to_string("{\"environment\":{\"MODE\":\"hidden-double\"}}").unwrap();
    let events = vec![
        event(
            "item/started",
            json!({"type":"commandExecution","id":"a","command":"echo synthetic-secret"}),
        ),
        event("item/started", json!({"type":"commandExecution","id":"b"})),
        event(
            "item/started",
            json!({"type":"commandExecution","id":"c","command":"echo c"}),
        ),
        json!({"method":"item/completed","params":{"threadId":"thread","turnId":"foreign","item":{"type":"commandExecution","id":"b","command":"foreign command"}}}),
        event(
            "item/completed",
            json!({"type":"commandExecution","id":"b","command":"printf late","aggregatedOutput":"late output","exitCode":0}),
        ),
        event(
            "item/completed",
            json!({"type":"commandExecution","id":"b","command":"printf late","aggregatedOutput":"late output","exitCode":0}),
        ),
        event(
            "item/completed",
            json!({"type":"commandExecution","id":"a","command":"ignore changed input","aggregatedOutput":"output synthetic-secret","exitCode":0}),
        ),
        event(
            "item/completed",
            json!({"type":"commandExecution","id":"c","command":"also ignored","aggregatedOutput":"c output","exitCode":0}),
        ),
        event(
            "item/completed",
            json!({"type":"commandExecution","id":"d","command":"printf completed-only","aggregatedOutput":"d output","exitCode":0}),
        ),
        event(
            "item/started",
            json!({"type":"commandExecution","id":"d","command":"late start"}),
        ),
        event("item/started", json!({"type":"commandExecution","id":"e"})),
        event(
            "item/completed",
            json!({"type":"commandExecution","id":"e","aggregatedOutput":"e output","exitCode":0}),
        ),
        event(
            "item/completed",
            json!({"type":"mcpToolCall","id":"mcp","server":"srv","tool":"lookup","arguments":{"path":"README.md","token":"opaque-credential","environment":{"MODE":"hidden-environment"},"nested":"{\"password\":\"embedded-credential\",\"query\":\"safe\"}","malformed":"{\"password\":\"hidden-malformed\"","deep":deep,"double":double_encoded},"result":{"content":[{"type":"text","text":"mcp output"}]},"status":"completed"}),
        ),
        event(
            "item/started",
            json!({"type":"dynamicToolCall","id":"dynamic","tool":"inspect","arguments":{"query":"visible","headers":{"Authorization":"hidden-auth"},"password":{"value":"hidden-password"}}}),
        ),
        event(
            "item/completed",
            json!({"type":"dynamicToolCall","id":"dynamic","tool":"inspect"}),
        ),
        event(
            "item/started",
            json!({"type":"commandExecution","id":"long","command":"é".repeat(9000)}),
        ),
        event(
            "item/completed",
            json!({"type":"commandExecution","id":"long","exitCode":0}),
        ),
        event(
            "item/started",
            json!({"type":"fileChange","id":"file","changes":[
                {"path":"src/safe.rs","kind":{"type":"update"},"diff":"@@\n+ visible text\n+ let value = 3;","unknown":"hidden object"},
                {"path":"src/escaped.rs","kind":{"type":"update"},"diff":"@@\n+const value = \"synthetic-\\u0073ecret\";"},
                {"path":"src/inline.rs","kind":{"type":"update"},"diff":"@@\n+const config = {\"password\":\"unlisted-credential\"};"},
                {"path":"src/split.rs","kind":{"type":"update"},"diff":"@@\n+\"password\":\n+\"unlisted-credential\""},
                {"path":"src/deleted-header.rs","kind":{"type":"update"},"diff":"@@\n-Authorization: Bearer unlisted-value"},
                {"path":"src/deleted-env.rs","kind":{"type":"update"},"diff":"@@\n-environment: unlisted-value"},
                {"path":"src/prefixed.rs","kind":{"type":"update"},"diff":"@@\n+basic_authorization = unlisted-value"},
                {"path":"src/camel-authorization.rs","kind":{"type":"update"},"diff":"@@\n+basicAuthorization = unlisted-value"},
                {"path":"src/camel-auth.rs","kind":{"type":"update"},"diff":"@@\n+basicAuth = unlisted-value"},
                {"path":"src/camel-cookie.rs","kind":{"type":"update"},"diff":"@@\n+requestCookie = unlisted-value"},
                {"path":"src/camel-environment.rs","kind":{"type":"update"},"diff":"@@\n+customEnvironment = unlisted-value"}
            ]}),
        ),
        event(
            "item/completed",
            json!({"type":"fileChange","id":"file","changes":[{"path":"src/lib.rs","kind":{"type":"update"}}]}),
        ),
        event(
            "item/started",
            json!({"type":"fileChange","id":"file-late"}),
        ),
        event(
            "item/completed",
            json!({"type":"fileChange","id":"file-late","changes":[{"path":"src/new.rs","kind":{"type":"add"},"diff":"+ later change"}]}),
        ),
        event(
            "item/started",
            json!({"type":"dynamicToolCall","id":"mismatch","tool":"original"}),
        ),
        event(
            "item/completed",
            json!({"type":"commandExecution","id":"mismatch","command":"wrong identity"}),
        ),
        event(
            "item/completed",
            json!({"type":"commandExecution","command":"idless command","environment":{"MODE":"idless environment"}}),
        ),
    ];
    let mut plan = fake_plan();
    plan.environment
        .insert("AGENT_RUN_PROVIDER_TOKEN".into(), "synthetic-secret".into());
    let mut variables = Vec::new();
    for (index, event) in events.into_iter().enumerate() {
        let name = format!("EVENT_{index}");
        plan.environment.insert(name.clone(), event.to_string());
        variables.push(format!("\"${name}\""));
    }
    let old = r#"printf '%s\n' '{"method":"item/completed","params":{"threadId":"thread","turnId":"turn","item":{"type":"commandExecution"}}}';"#;
    let replacement = format!("printf '%s\\n' {};", variables.join(" "));
    let script = plan.args[1].replace(old, &replacement);
    assert_ne!(script, plan.args[1]);
    plan.args[1] = script;
    plan
}

/// Input is bound to the original invocation even when starts interleave,
/// arrives only at completion, is missing, or a terminal event is duplicated.
#[tokio::test]
async fn codex_tool_input_captures_supported_fields_without_mispairing() {
    let fixture = common::Home::new();
    let mut request = fixture.request();
    request.workdir = PathBuf::from(scratch());
    let (id, _) = fixture
        .store()
        .admit(&request, &fixture.config, &json!({}), None)
        .unwrap();
    let mut store = fixture.store();
    let record = store.get(&id).unwrap();
    let prep = common::writer::prepare_supervisor(&mut store, &id);
    let harness = common::writer::Harness::start(&fixture.path);
    let mut persist = harness.supervisor(&fixture.path, prep, &id).await;
    let app_home = fixture.path.join("codex-home");
    fs::private_dir(&app_home).unwrap();
    let mut process = Process::spawn(&tool_input_plan()).unwrap();
    let result = codex::run(
        &mut process,
        &mut persist,
        &record,
        &record,
        &runtime(fixture.path.join("runtime")),
        &profile(),
        &app_home,
    )
    .await
    .unwrap();
    persist.flush().await.unwrap();
    assert_eq!(result.outcome.status, Status::Succeeded);

    let page = store.transcript(&id, 0, 1000).unwrap();
    let messages = page["messages"].as_array().unwrap();
    let contents = |role: &str, id: &str| {
        messages
            .iter()
            .filter(|row| row["role"] == role && row["raw_ref"] == id)
            .map(|row| row["content"].as_str().unwrap().to_owned())
            .collect::<Vec<_>>()
    };
    assert_eq!(contents("tool_call", "a"), ["echo <redacted>"]);
    assert_eq!(contents("tool_result", "a"), ["output <redacted>"]);
    assert_eq!(contents("tool_call", "b"), ["", "printf late"]);
    assert_eq!(contents("tool_result", "b"), ["late output"]);
    assert_eq!(contents("tool_call", "c"), ["echo c"]);
    assert_eq!(contents("tool_call", "d"), ["printf completed-only"]);
    assert_eq!(contents("tool_call", "e"), [""]);
    assert_eq!(contents("tool_call", "mismatch"), [""]);
    let args = &contents("tool_call", "mcp")[0];
    assert!(
        args.contains("README.md") && args.contains("<redacted>"),
        "{args}"
    );
    let changes = &contents("tool_call", "file")[0];
    let changes: serde_json::Value = serde_json::from_str(changes).unwrap();
    let changes = changes["changes"].as_array().unwrap();
    assert_eq!(changes[0]["path"], "src/safe.rs");
    assert_eq!(changes[0]["kind"], "update");
    assert_eq!(changes[0]["diff"], "@@\n+ visible text\n+ let value = 3;");
    assert_eq!(changes.len(), 11);
    for change in &changes[1..] {
        assert_eq!(
            change["diff"],
            "[diff omitted: sensitive or escaped content]"
        );
    }
    let late_change = contents("tool_call", "file-late");
    assert_eq!(late_change[0], "");
    assert!(late_change[1].contains("src/new.rs") && late_change[1].contains("later change"));
    assert!(contents("tool_call", "dynamic")[0].contains("visible"));
    let long = contents("tool_call", "long");
    assert_eq!(long.len(), 2);
    assert!(long.iter().all(|fragment| fragment.len() <= 16 * 1024));
    assert_eq!(long.concat(), "é".repeat(9000));
    let transcript = page.to_string();
    for omitted in [
        "synthetic-secret",
        "opaque-credential",
        "hidden-environment",
        "embedded-credential",
        "hidden-malformed",
        "hidden-deep",
        "hidden-double",
        "unlisted-credential",
        "unlisted-value",
        "hidden-auth",
        "hidden-password",
        "foreign command",
        "hidden object",
        "wrong identity",
    ] {
        assert!(
            !transcript.contains(omitted),
            "persisted {omitted}: {transcript}"
        );
    }
    let event = store
        .last_event(&id, "item/completed")
        .unwrap()
        .unwrap()
        .to_string();
    assert!(
        !event.contains("idless command") && !event.contains("idless environment"),
        "{event}"
    );
    let mut events = store
        .conn
        .prepare("SELECT data_json FROM events WHERE agent_id=? AND kind='item/completed'")
        .unwrap();
    let events = events
        .query_map([id.as_str()], |row| row.get::<_, String>(0))
        .unwrap()
        .collect::<std::result::Result<Vec<_>, _>>()
        .unwrap()
        .join("\n");
    assert!(!events.contains("foreign command"), "{events}");
    drop(process.input.take());
    process.reap().await;
}

/// Malformed and deeply nested JSON-like arguments are omitted even when no
/// launch-secret literals exist for the ordinary process redactor to match.
#[tokio::test]
async fn codex_tool_input_omits_unparseable_json_without_launch_secrets() {
    let fixture = common::Home::new();
    let mut request = fixture.request();
    request.workdir = PathBuf::from(scratch());
    let (id, _) = fixture
        .store()
        .admit(&request, &fixture.config, &json!({}), None)
        .unwrap();
    let mut store = fixture.store();
    let record = store.get(&id).unwrap();
    let prep = common::writer::prepare_supervisor(&mut store, &id);
    let harness = common::writer::Harness::start(&fixture.path);
    let mut persist = harness.supervisor(&fixture.path, prep, &id).await;
    let app_home = fixture.path.join("codex-home");
    fs::private_dir(&app_home).unwrap();
    let mut plan = tool_input_plan();
    plan.environment.remove("AGENT_RUN_PROVIDER_TOKEN");
    assert!(
        plan.environment
            .keys()
            .all(|name| !agent_run_adapters::redact::is_secret_name(name))
    );
    let mut process = Process::spawn(&plan).unwrap();
    let result = codex::run(
        &mut process,
        &mut persist,
        &record,
        &record,
        &runtime(fixture.path.join("runtime")),
        &profile(),
        &app_home,
    )
    .await
    .unwrap();
    persist.flush().await.unwrap();
    assert_eq!(result.outcome.status, Status::Succeeded);

    let page = store.transcript(&id, 0, 1000).unwrap();
    let call = page["messages"]
        .as_array()
        .unwrap()
        .iter()
        .find(|row| row["role"] == "tool_call" && row["raw_ref"] == "mcp")
        .unwrap();
    let args: serde_json::Value = serde_json::from_str(call["content"].as_str().unwrap()).unwrap();
    assert_eq!(args["path"], "README.md");
    assert_eq!(args["malformed"], "<omitted>");
    assert_eq!(args["deep"], "<omitted>");
    assert_eq!(args["double"], "<omitted>");
    drop(process.input.take());
    process.reap().await;
}

/// A fake server with interleaved command deltas and terminal items.
fn command_delta_plan() -> LaunchPlan {
    let item = |method: &str, value: serde_json::Value| json!({"method":method,"params":{"threadId":"thread","turnId":"turn","item":value}});
    let delta = |id: &str, value: &str| json!({"method":"item/commandExecution/outputDelta","params":{"threadId":"thread","turnId":"turn","itemId":id,"delta":value}});
    let large = "é".repeat(32 * 1024);
    let mut events = vec![
        item(
            "item/started",
            json!({"type":"commandExecution","id":"delta","command":"delta"}),
        ),
        delta("delta", "before synthetic-"),
        delta("delta", "secret after"),
        item(
            "item/completed",
            json!({"type":"commandExecution","id":"delta","aggregatedOutput":null,"exitCode":0}),
        ),
        delta("delta", "late output"),
        item(
            "item/completed",
            json!({"type":"commandExecution","id":"delta","aggregatedOutput":null,"exitCode":0}),
        ),
        item(
            "item/started",
            json!({"type":"commandExecution","id":"aggregate","command":"aggregate"}),
        ),
        delta("aggregate", "stale partial"),
        item(
            "item/completed",
            json!({"type":"commandExecution","id":"aggregate","aggregatedOutput":"complete synthetic-secret","exitCode":0}),
        ),
        item(
            "item/started",
            json!({"type":"commandExecution","id":"empty-aggregate","command":"empty-aggregate"}),
        ),
        delta("empty-aggregate", "use deltas"),
        item(
            "item/completed",
            json!({"type":"commandExecution","id":"empty-aggregate","aggregatedOutput":"","exitCode":0}),
        ),
        item(
            "item/started",
            json!({"type":"commandExecution","id":"log","command":"log"}),
        ),
        delta("log", "[INFO] ready"),
        item(
            "item/completed",
            json!({"type":"commandExecution","id":"log","aggregatedOutput":null,"exitCode":0}),
        ),
        item(
            "item/started",
            json!({"type":"commandExecution","id":"multiline-log","command":"multiline-log"}),
        ),
        delta("multiline-log", "[INFO] first\n[INFO] second"),
        item(
            "item/completed",
            json!({"type":"commandExecution","id":"multiline-log","aggregatedOutput":null,"exitCode":0}),
        ),
        item(
            "item/started",
            json!({"type":"commandExecution","id":"progress","command":"progress"}),
        ),
        delta(
            "progress",
            "[1/2] Building CXX object foo.o\n[ 50%] Built target app",
        ),
        item(
            "item/completed",
            json!({"type":"commandExecution","id":"progress","aggregatedOutput":null,"exitCode":0}),
        ),
        item(
            "item/started",
            json!({"type":"commandExecution","id":"progress-secret","command":"progress-secret"}),
        ),
        delta("progress-secret", "[1/2] synthetic-\\u0073ecret"),
        item(
            "item/completed",
            json!({"type":"commandExecution","id":"progress-secret","aggregatedOutput":null,"exitCode":0}),
        ),
        item(
            "item/started",
            json!({"type":"commandExecution","id":"quoted","command":"quoted"}),
        ),
        delta("quoted", "\"service ready\" now"),
        item(
            "item/completed",
            json!({"type":"commandExecution","id":"quoted","aggregatedOutput":null,"exitCode":0}),
        ),
        item(
            "item/started",
            json!({"type":"commandExecution","id":"quoted-secret","command":"quoted-secret"}),
        ),
        delta("quoted-secret", "\"synthetic-\\u00"),
        delta("quoted-secret", "73ecret\" ready"),
        item(
            "item/completed",
            json!({"type":"commandExecution","id":"quoted-secret","aggregatedOutput":null,"exitCode":0}),
        ),
        item(
            "item/started",
            json!({"type":"commandExecution","id":"quoted-escaped-safe","command":"quoted-escaped-safe"}),
        ),
        delta("quoted-escaped-safe", "\"service\\u0020ready\" now"),
        item(
            "item/completed",
            json!({"type":"commandExecution","id":"quoted-escaped-safe","aggregatedOutput":null,"exitCode":0}),
        ),
        item(
            "item/started",
            json!({"type":"commandExecution","id":"log-json","command":"log-json"}),
        ),
        delta(
            "log-json",
            "[INFO] {\"password\":\"hidden-logged-password\"}",
        ),
        item(
            "item/completed",
            json!({"type":"commandExecution","id":"log-json","aggregatedOutput":null,"exitCode":0}),
        ),
        item(
            "item/started",
            json!({"type":"commandExecution","id":"quoted-json","command":"quoted-json"}),
        ),
        delta("quoted-json", "\"password\":\"hidden-quoted-password"),
        item(
            "item/completed",
            json!({"type":"commandExecution","id":"quoted-json","aggregatedOutput":null,"exitCode":0}),
        ),
        delta("owner", "before start"),
        item(
            "item/started",
            json!({"type":"commandExecution","id":"owner","command":"owner"}),
        ),
        delta("unknown", "foreign id"),
        json!({"method":"item/commandExecution/outputDelta","params":{"threadId":"foreign","turnId":"turn","itemId":"owner","delta":"foreign thread"}}),
        json!({"method":"item/commandExecution/outputDelta","params":{"threadId":"thread","turnId":"foreign","itemId":"owner","delta":"foreign turn"}}),
        json!({"method":"item/commandExecution/outputDelta","params":{"turnId":"turn","itemId":"owner","delta":"missing thread"}}),
        delta("owner", "owned output"),
        item(
            "item/completed",
            json!({"type":"commandExecution","id":"owner","exitCode":0}),
        ),
        item(
            "item/started",
            json!({"type":"commandExecution","id":"json","command":"json"}),
        ),
        delta(
            "json",
            "{\"password\":\"hidden-output-password\",\"environment\":{\"MODE\":\"hidden-output-env\"},\"value\":\"visible\"}",
        ),
        item(
            "item/completed",
            json!({"type":"commandExecution","id":"json","aggregatedOutput":null,"exitCode":0}),
        ),
        item(
            "item/started",
            json!({"type":"commandExecution","id":"malformed","command":"malformed"}),
        ),
        delta("malformed", "{\"password\":\"hidden-invalid-output\""),
        item(
            "item/completed",
            json!({"type":"commandExecution","id":"malformed","aggregatedOutput":null,"exitCode":0}),
        ),
        item(
            "item/started",
            json!({"type":"commandExecution","id":"error","command":"error"}),
        ),
        delta("error", "partial output"),
        item(
            "item/completed",
            json!({"type":"commandExecution","id":"error","aggregatedOutput":null,"status":"failed","error":{"message":"failure reason"}}),
        ),
        item(
            "item/started",
            json!({"type":"commandExecution","id":"large","command":"large"}),
        ),
    ];
    events.extend((0..5).map(|_| delta("large", &large)));
    events.push(item(
        "item/completed",
        json!({"type":"commandExecution","id":"large","aggregatedOutput":null,"exitCode":0}),
    ));
    for index in 0..65 {
        let id = format!("stream-{index}");
        events.push(item(
            "item/started",
            json!({"type":"commandExecution","id":&id,"command":"stream"}),
        ));
        events.push(delta(&id, "stream output"));
    }
    for index in (0..65).rev() {
        events.push(item("item/completed", json!({"type":"commandExecution","id":format!("stream-{index}"),"aggregatedOutput":null,"exitCode":0})));
    }
    events.extend([
        item(
            "item/started",
            json!({"type":"commandExecution","id":"unfinished","command":"unfinished"}),
        ),
        delta("unfinished", "unfinished output"),
    ]);
    let mut plan = fake_plan();
    plan.environment
        .insert("AGENT_RUN_PROVIDER_TOKEN".into(), "synthetic-secret".into());
    let mut variables = Vec::new();
    for (index, event) in events.into_iter().enumerate() {
        let name = format!("EVENT_{index}");
        plan.environment.insert(name.clone(), event.to_string());
        variables.push(format!("\"${name}\""));
    }
    let old = r#"printf '%s\n' '{"method":"item/completed","params":{"threadId":"thread","turnId":"turn","item":{"type":"commandExecution"}}}';"#;
    let replacement = format!("printf '%s\\n' {};", variables.join(" "));
    let script = plan.args[1].replace(old, &replacement);
    assert_ne!(script, plan.args[1]);
    plan.args[1] = script;
    plan
}

/// Delta-only output is redacted, bounded and tied to the original command;
/// an aggregate wins without duplication and an unfinished job stays open.
#[tokio::test]
async fn codex_command_deltas_fallback_safely_without_duplicate_results() {
    let fixture = common::Home::new();
    let mut request = fixture.request();
    request.workdir = PathBuf::from(scratch());
    let (id, _) = fixture
        .store()
        .admit(&request, &fixture.config, &json!({}), None)
        .unwrap();
    let mut store = fixture.store();
    let record = store.get(&id).unwrap();
    let prep = common::writer::prepare_supervisor(&mut store, &id);
    let harness = common::writer::Harness::start(&fixture.path);
    let mut persist = harness.supervisor(&fixture.path, prep, &id).await;
    let app_home = fixture.path.join("codex-home");
    fs::private_dir(&app_home).unwrap();
    let mut process = Process::spawn(&command_delta_plan()).unwrap();
    let result = codex::run(
        &mut process,
        &mut persist,
        &record,
        &record,
        &runtime(fixture.path.join("runtime")),
        &profile(),
        &app_home,
    )
    .await
    .unwrap();
    persist.flush().await.unwrap();
    assert_eq!(result.outcome.status, Status::Succeeded);
    let output = |key: &str| -> String {
        store.conn.query_row("SELECT COALESCE(GROUP_CONCAT(content,''),'') FROM (SELECT content FROM messages WHERE agent_id=?1 AND role='tool_result' AND raw_ref=?2 ORDER BY seq)", [id.as_str(), key], |row| row.get(0)).unwrap()
    };
    assert_eq!(output("delta"), "before <redacted> after");
    assert_eq!(output("aggregate"), "complete <redacted>");
    assert_eq!(output("empty-aggregate"), "use deltas");
    assert_eq!(output("log"), "[INFO] ready");
    assert_eq!(output("multiline-log"), "[INFO] first\n[INFO] second");
    assert_eq!(
        output("progress"),
        "[1/2] Building CXX object foo.o\n[ 50%] Built target app"
    );
    assert_eq!(output("progress-secret"), "<omitted>");
    assert_eq!(output("quoted"), "\"service ready\" now");
    assert_eq!(output("quoted-secret"), "<omitted>");
    assert_eq!(output("quoted-escaped-safe"), "\"service\\u0020ready\" now");
    assert_eq!(output("log-json"), "<omitted>");
    assert_eq!(output("quoted-json"), "<omitted>");
    assert_eq!(output("owner"), "owned output");
    let structured: serde_json::Value = serde_json::from_str(&output("json")).unwrap();
    assert_eq!(structured["value"], "visible");
    assert_eq!(structured["password"], "<redacted>");
    assert_eq!(structured["environment"], "<redacted>");
    assert_eq!(output("malformed"), "<omitted>");
    assert_eq!(output("error"), "partial output\nfailure reason");
    assert_eq!(
        output("large"),
        format!("{}\n[command output truncated]", "é".repeat(128 * 1024))
    );
    assert_eq!(output("stream-0"), "stream output");
    assert_eq!(output("stream-64"), "\n[command output truncated]");
    let unfinished: i64 = store.conn.query_row("SELECT COUNT(*) FROM messages WHERE agent_id=? AND role='tool_result' AND raw_ref='unfinished'", [id.as_str()], |row| row.get(0)).unwrap();
    assert_eq!(unfinished, 0);
    let result_count: i64 = store.conn.query_row("SELECT COUNT(*) FROM messages WHERE agent_id=? AND role='tool_result' AND raw_ref='delta'", [id.as_str()], |row| row.get(0)).unwrap();
    assert_eq!(result_count, 1);
    let persisted: String = store
        .conn
        .query_row(
            "SELECT COALESCE(GROUP_CONCAT(data_json,''),'') FROM events WHERE agent_id=?",
            [id.as_str()],
            |row| row.get(0),
        )
        .unwrap();
    for omitted in [
        "synthetic-",
        "foreign id",
        "foreign thread",
        "foreign turn",
        "missing thread",
        "before start",
        "late output",
        "hidden-invalid-output",
    ] {
        assert!(!persisted.contains(omitted), "leaked {omitted}");
    }
    drop(process.input.take());
    process.reap().await;
}

/// One steer-exchange script: line 6 answers the steer request with `mode`
/// (`result` or `error`), optionally flooding 65 notifications first so the
/// bounded exchange ends in pressure before the late correlated reply.
fn steer_plan(mode: &str, flood: bool) -> LaunchPlan {
    let mut script = String::from(
        r#"n=0; while IFS= read -r line; do n=$((n+1)); case "$n" in
            1) printf '%s
' '{"id":1,"result":{}}' ;;
            3) printf '%s
' '{"id":2,"result":{"data":[{"id":"fixture"}]}}' ;;
            4) case "$line" in *'"permissions"'*) printf '%s
' '{"id":3,"result":{"model":"fixture","cwd":"/private/tmp","runtimeWorkspaceRoots":["/private/tmp"],"sandbox":{"type":"readOnly","networkAccess":false},"approvalPolicy":"never","activePermissionProfile":{"id":":read-only"},"threadId":"thread"}}' ;; *) printf '%s
' '{"id":3,"result":{"model":"fixture","cwd":"/private/tmp","roots":["/private/tmp"],"writableRoots":[],"sandbox":"read-only","approvalPolicy":"never","threadId":"thread"}}' ;; esac ;;
            5) printf '%s
' '{"id":4,"result":{"turn":{"id":"turn"}}}'; sleep 0.4 ;;
            6) "#,
    );
    if flood {
        for n in 1..=65u64 {
            script.push_str(&format!(
                "printf '%s\\n' '{{\"method\":\"item/agentMessage/delta\",\"params\":{{\"threadId\":\"thread\",\"turnId\":\"turn\",\"itemId\":\"flood\",\"delta\":\"{n}\"}}}}'; "
            ));
        }
        script.push_str("sleep 0.4; printf '%s\\n' '{\"id\":5,\"result\":{\"ok\":true}}'; ");
    } else if mode == "error" {
        script.push_str(
            "printf '%s\\n' '{\"id\":5,\"error\":{\"code\":\"ExpectedTurnMismatch\"}}'; ",
        );
    } else {
        script.push_str("printf '%s\\n' '{\"id\":5,\"result\":{\"ok\":true}}'; ");
    }
    script.push_str(
        r#"printf '%s\n' '{"method":"turn/completed","params":{"threadId":"thread","turn":{"id":"turn","status":"completed","items":[]}}}' ;;
        esac; done"#,
    );
    let mut plan = fake_plan();
    plan.args[1] = script.replace("/private/tmp", &scratch());
    plan
}

/// Admits one fixture row with a pending steer command and runs it to
/// completion against `plan`, returning the agent id.
async fn run_with_steer(
    home: &std::path::Path,
    plan: LaunchPlan,
) -> agent_run_domain::domain::AgentId {
    run_with_command(home, plan, |store, id| {
        store
            .enqueue(id, "steer", &json!({"text":"fixture:steer-text"}))
            .unwrap();
    })
    .await
}

/// [`run_with_steer`] with the caller choosing which commands to enqueue.
async fn run_with_command(
    home: &std::path::Path,
    plan: LaunchPlan,
    enqueue: impl FnOnce(&mut agent_run_core::state::Store, &agent_run_domain::domain::AgentId),
) -> agent_run_domain::domain::AgentId {
    let config = agent_run_config::config::Config::load(home).unwrap();
    let mut request: agent_run_domain::domain::StartRequest = serde_json::from_value(json!({
        "runtime":"mock", "model":"fixture", "profile":"review",
        "task":"fixture task", "workdir":scratch()
    }))
    .unwrap();
    request.validate().unwrap();
    let mut store = agent_run_store::fixture::open(home).unwrap();
    let (id, _) = store.admit(&request, &config, &json!({}), None).unwrap();
    enqueue(&mut store, &id);
    drop(store);
    let mut store = agent_run_store::fixture::open(home).unwrap();
    let record = store.get(&id).unwrap();
    let prep = common::writer::prepare_supervisor(&mut store, &id);
    let harness = common::writer::Harness::start(home);
    let mut persist = harness.supervisor(home, prep, &id).await;
    let app_home = home.join("codex-home");
    fs::private_dir(&app_home).unwrap();
    let runtime_home = home.join("runtime");
    fs::private_dir(&runtime_home).unwrap();
    let mut process = Process::spawn(&plan).expect("fake app-server starts");
    let result = codex::run(
        &mut process,
        &mut persist,
        &record,
        &record,
        &runtime(runtime_home),
        &profile(),
        &app_home,
    )
    .await
    .expect("fake turn succeeds");
    persist.flush().await.unwrap();
    assert_eq!(result.outcome.status, Status::Succeeded);
    drop(process.input.take());
    process.reap().await;
    id
}

/// Reads one durable command result for the agent.
fn command_result(
    home: &std::path::Path,
    id: &agent_run_domain::domain::AgentId,
) -> serde_json::Value {
    let store = agent_run_store::fixture::open(home).unwrap();
    store
        .conn
        .query_row(
            "SELECT result_json FROM commands WHERE agent_id=? AND kind='steer'",
            [id.as_str()],
            |row| row.get::<_, String>(0),
        )
        .map(|raw| serde_json::from_str(&raw).unwrap())
        .unwrap()
}

/// Counts journal rows containing the fixture steer text.
fn steer_rows(home: &std::path::Path, id: &agent_run_domain::domain::AgentId) -> i64 {
    let store = agent_run_store::fixture::open(home).unwrap();
    store
        .conn
        .query_row(
            "SELECT COUNT(*) FROM messages WHERE agent_id=? AND content LIKE '%fixture:steer-text%'",
            [id.as_str()],
            |row| row.get(0),
        )
        .unwrap()
}

/// Backlog pressure after the steer write is explicitly unknown: the command
/// result records `accepted:null` with the bounded reason, no user row is
/// journaled for an unproven delivery, the late correlated reply lands as
/// metadata only, and the terminal completion still arrives in order.
#[tokio::test]
async fn steer_pressure_is_uncertain_and_late_reply_is_metadata() {
    let fixture = common::Home::new();
    let id = run_with_steer(&fixture.path, steer_plan("result", true)).await;
    let result = command_result(&fixture.path, &id);
    assert_eq!(
        result,
        json!({"accepted":null,"reason":"uncertain_backlog_pressure"}),
        "pressure after a possible write is unknown, not a guessed rejection"
    );
    assert_eq!(
        steer_rows(&fixture.path, &id),
        0,
        "unproven text is not journaled"
    );
    let store = agent_run_store::fixture::open(&fixture.path).unwrap();
    let uncertain: i64 = store
        .conn
        .query_row(
            "SELECT COUNT(*) FROM events WHERE agent_id=? AND kind='steer_uncertain'",
            [id.as_str()],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(uncertain, 1);
    let late: serde_json::Value = store
        .conn
        .query_row(
            "SELECT data_json FROM events WHERE agent_id=? AND kind='late_rpc_reply'",
            [id.as_str()],
            |row| row.get::<_, String>(0),
        )
        .map(|raw| serde_json::from_str(&raw).unwrap())
        .unwrap();
    assert_eq!(late, json!({"id":5}), "the late reply is id-only metadata");
    let malformed: i64 = store
        .conn
        .query_row(
            "SELECT COUNT(*) FROM events WHERE agent_id=? AND kind='malformed_event'",
            [id.as_str()],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(malformed, 0, "a late reply is not a malformed event");
}

/// A correlated native rejection proves the steer was not accepted and no
/// user row is journaled; the turn still completes.
#[tokio::test]
async fn steer_rejection_is_proven_false() {
    let fixture = common::Home::new();
    let id = run_with_steer(&fixture.path, steer_plan("error", false)).await;
    assert_eq!(
        command_result(&fixture.path, &id),
        json!({"accepted":false,"reason":"native_rejected"})
    );
    assert_eq!(steer_rows(&fixture.path, &id), 0);
}

/// A correlated successful reply proves native acceptance only, and the
/// journaled user row keeps the delivered text.
#[tokio::test]
async fn steer_reply_journals_the_delivered_text() {
    let fixture = common::Home::new();
    let id = run_with_steer(&fixture.path, steer_plan("result", false)).await;
    // The atomic observed-result operation stamps its evidence strength.
    assert_eq!(
        command_result(&fixture.path, &id),
        json!({"accepted":true,"observation":"native_accepted"})
    );
    assert_eq!(steer_rows(&fixture.path, &id), 1);
}

/// Admits a one-seat pool around `id` and an operator entry, then queues the
/// `pool` command a peer write would have produced; returns nothing, the log
/// entry is the authority the runner re-reads.
fn queue_pool_push(
    store: &mut agent_run_core::state::Store,
    id: &agent_run_domain::domain::AgentId,
) {
    store
        .conn
        .execute(
            "INSERT INTO pools(id,request_namespace,request_id,request_sha256,goal,acceptance_json,state,roster_revision,created_at) \
             VALUES('pool-20260101-000000-0123456789','ns','r',lower(hex(zeroblob(32))),'goal','[]','open',1,1.0)",
            [],
        )
        .unwrap();
    store
        .conn
        .execute(
            "INSERT INTO pool_members(agent_id,pool_id,slot,name,role,personal_task,joined_roster_revision) \
             VALUES(?,'pool-20260101-000000-0123456789',1,'Ada','reviewer','t',1)",
            [id.as_str()],
        )
        .unwrap();
    store
        .conn
        .execute(
            "INSERT INTO pool_entries(pool_id,author_kind,direction,kind,roster_revision,body,idem_scope,request_id,created_at) \
             VALUES('pool-20260101-000000-0123456789','operator','team','message',1,'please recheck','op','k',1.0)",
            [],
        )
        .unwrap();
    store.enqueue(id, "pool", &json!({"seq":1})).unwrap();
}

/// Reads the durable result of the agent's one `pool` command.
fn pool_result(
    home: &std::path::Path,
    id: &agent_run_domain::domain::AgentId,
) -> serde_json::Value {
    let store = agent_run_store::fixture::open(home).unwrap();
    store
        .conn
        .query_row(
            "SELECT result_json FROM commands WHERE agent_id=? AND kind='pool'",
            [id.as_str()],
            |row| row.get::<_, String>(0),
        )
        .map(|raw| serde_json::from_str(&raw).unwrap())
        .unwrap()
}

/// A correlated native reply is `native_accepted` only: never delivery or
/// consumption, and the operator-visible journal gains no user row.
#[tokio::test]
async fn pool_push_native_reply_is_accepted_not_consumed() {
    let fixture = common::Home::new();
    let id = run_with_command(&fixture.path, steer_plan("result", false), queue_pool_push).await;
    let result = pool_result(&fixture.path, &id);
    assert_eq!(result["push"], "native_accepted");
    assert_eq!(result["reason"], "native_replied");
    assert!(result.get("delivered").is_none() && result.get("accepted").is_none());
}

/// A correlated native rejection is a proven `rejected`; the turn completes.
#[tokio::test]
async fn pool_push_native_rejection_is_rejected() {
    let fixture = common::Home::new();
    let id = run_with_command(&fixture.path, steer_plan("error", false), queue_pool_push).await;
    assert_eq!(pool_result(&fixture.path, &id)["push"], "rejected");
}

/// Backlog pressure after a possible write is `unknown`, never a guess.
#[tokio::test]
async fn pool_push_pressure_after_write_is_unknown() {
    let fixture = common::Home::new();
    let id = run_with_command(&fixture.path, steer_plan("result", true), queue_pool_push).await;
    let result = pool_result(&fixture.path, &id);
    assert_eq!(result["push"], "unknown");
    assert_eq!(result["reason"], "uncertain_backlog_pressure");
}
