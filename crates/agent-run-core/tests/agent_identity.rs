//! Stable public references preserve exact run history and delayed hook binding.

mod common;

use agent_run_core::{agent_identity, dispatch, hooks::bind, journal, service::Service};
use agent_run_domain::{domain::AgentId, domain::Outcome, Error};
use agent_run_platform::{fs, verify};
use serde_json::json;
use std::path::Path;

/// Create one terminal, unspawned run with native lineage and a verified answer.
/// No operating-system process or provider is involved in this fixture.
fn terminal_run(home: &common::Home, parent: Option<&AgentId>, text: &str) -> AgentId {
    let mut store = home.store();
    let parent = parent.map(|id| store.get(id).unwrap());
    let mut request = home.request();
    request.task = text.into();
    let (id, created) = store
        .admit(&request, &home.config, &json!({}), parent.as_ref())
        .unwrap();
    assert!(created);
    store.runtime_session(&id, "native-fixture").unwrap();
    journal(&store, &id, "assistant", text, None, None).unwrap();
    let directory = home.path.join("agents").join(id.as_str());
    fs::private_dir(&directory).unwrap();
    let proof = verify::seal(&directory, Path::new("answer.md"), text).unwrap();
    store
        .finish(&id, &Outcome::failure("fixture"), Some(&proof), None)
        .unwrap();
    id
}

/// Stable ids follow the tip while explicit run ids retain historical content.
#[tokio::test]
async fn public_reads_preserve_history_and_do_not_rewrite_user_content() {
    let home = common::Home::new();
    let root = terminal_run(&home, None, "first answer");
    let user_content = r#"{"agent_id":"user data, not a routing field"}"#;
    let child = terminal_run(&home, Some(&root), user_content);
    let service = Service::new(home.path.clone());

    let latest = dispatch::call(&service, "answer", json!({"agent_id":root}))
        .await
        .unwrap();
    assert_eq!(latest["agent_id"], root.as_str());
    assert!(latest.get("run_id").is_none());
    assert_eq!(latest["content"], user_content);
    let historical = dispatch::call(&service, "answer", json!({"agent_id":child,"run_id":root}))
        .await
        .unwrap();
    assert_eq!(historical["agent_id"], root.as_str());
    assert!(historical.get("run_id").is_none());
    assert_eq!(historical["content"], "first answer");
    let transcript = dispatch::call(
        &service,
        "transcript",
        json!({"agent_id":root,"run_id":root}),
    )
    .await
    .unwrap();
    assert!(transcript.get("run_id").is_none());
    assert_eq!(transcript["messages"][0]["content"], "first answer");
    let listed = dispatch::call(&service, "list_agents", json!({}))
        .await
        .unwrap();
    assert_eq!(listed["total"], 1);
    let items = listed["items"].as_array().unwrap();
    assert!(items.iter().all(|item| item["agent_id"] == root.as_str()));
    assert!(items.iter().all(|item| item.get("run_id").is_none()));
    assert_eq!(items[0]["sequence"], 2);

    let other = terminal_run(&home, None, "another agent");
    assert!(matches!(
        dispatch::call(&service, "answer", json!({"agent_id":root,"run_id":other})).await,
        Err(Error::Validation(_))
    ));
}

/// A cancellation sent to the original identity applies only to its current run.
#[tokio::test]
async fn stable_control_targets_the_active_execution() {
    let home = common::Home::new();
    let root = terminal_run(&home, None, "finished parent");
    let mut store = home.store();
    let parent = store.get(&root).unwrap();
    let (child, _) = store
        .admit(&home.request(), &home.config, &json!({}), Some(&parent))
        .unwrap();
    let service = Service::new(home.path.clone());
    let result = dispatch::call(&service, "cancel", json!({"agent_id":root}))
        .await
        .unwrap();
    assert_eq!(result["agent_id"], root.as_str());
    assert!(result.get("run_id").is_none());
    let command_run: String = store
        .conn
        .query_row(
            "SELECT agent_id FROM commands WHERE kind='cancel'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(command_run, child.as_str());
    assert!(store.get(&root).unwrap().status.terminal());
    let reference = agent_run_domain::domain::OrchestratorRef {
        transport: "codex_queue".into(),
        external_session_id: "active-identity".into(),
        external_turn_id: None,
    };
    bind::bind(
        &mut store,
        child.clone(),
        reference.clone(),
        agent_run_domain::domain::now(),
    )
    .unwrap();
    let context = agent_run_core::hooks::context::build(&home.path, &reference, None).unwrap();
    assert!(context.text.contains(root.as_str()), "{}", context.text);
    assert!(!context.text.contains(child.as_str()), "{}", context.text);
}

/// Discovery advertises only the stable agent selector on every public tool.
#[test]
fn tool_inventory_has_no_execution_selector() {
    for tool in dispatch::tools() {
        assert!(
            tool["inputSchema"]["properties"].get("run_id").is_none(),
            "{tool}"
        );
        assert!(
            !tool["description"].as_str().unwrap().contains("run_id"),
            "{tool}"
        );
    }
}

/// A delayed post-tool hook binds the reported run even after another resume.
#[test]
fn delayed_hook_binds_exact_run_and_rejects_malformed_run_identity() {
    let home = common::Home::new();
    let root = terminal_run(&home, None, "root");
    let child = terminal_run(&home, Some(&root), "child");
    let latest = terminal_run(&home, Some(&child), "latest");
    let mut store = home.store();
    assert_eq!(
        agent_identity::resolve(&store, &child, None).unwrap().id,
        latest
    );
    let payload = json!({
        "hook_event_name":"PostToolUse", "session_id":"caller-session",
        "tool_response":{"structuredContent":{"agent_id":root,"run_id":child}}
    });
    let bound = bind::run_hook(&mut store, &payload, "codex_queue", None).unwrap();
    assert_eq!(bound.agent_id, root);
    assert_eq!(bound.run_id, child);
    assert_eq!(store.delivery_status(&root).unwrap()["bound"], false);
    assert_eq!(store.delivery_status(&child).unwrap()["bound"], true);
    assert_eq!(store.delivery_status(&latest).unwrap()["bound"], false);
    let malformed = json!({
        "session_id":"caller-session",
        "tool_response":{"agent_id":root,"run_id":17}
    });
    assert!(bind::normalize(&malformed, true, "codex_queue").is_err());
}

/// User JSON mentioning another run cannot override or poison hook metadata.
#[test]
fn hook_execution_identity_excludes_arbitrary_task_content() {
    let mut response = json!({
        "agent_id":"ag-root", "run_id":"ag-current", "created":true,
        "agent":{
            "agent_id":"ag-root", "run_id":"ag-current",
            "task_summary":json!({"run_id":"ag-other"}).to_string(),
        }
    });
    let normalize = |response: &serde_json::Value| {
        bind::normalize(
            &json!({"session_id":"caller","tool_response":response}),
            true,
            "codex_queue",
        )
        .unwrap()
        .agent_id
    };
    assert_eq!(normalize(&response).as_deref(), Some("ag-current"));
    response.as_object_mut().unwrap().remove("run_id");
    response["agent"].as_object_mut().unwrap().remove("run_id");
    assert_eq!(normalize(&response).as_deref(), Some("ag-root"));
}

/// Admission counters bind an old execution even after later resumes exist.
#[tokio::test]
async fn receipt_pins_delayed_binding_and_wait_without_exposing_run_ids() {
    let home = common::Home::new();
    let root = terminal_run(&home, None, "root");
    let child = terminal_run(&home, Some(&root), "child");
    let latest = terminal_run(&home, Some(&child), "latest");
    let mut store = home.store();
    let payload = json!({"session_id":"receipt-session", "tool_response": {
        "structuredContent":{"agent_id":root,"sequence":2}
    }});
    let bound = bind::run_hook(&mut store, &payload, "codex_queue", None).unwrap();
    assert_eq!(bound.run_id, child);
    assert!(serde_json::to_value(&bound)
        .unwrap()
        .get("run_id")
        .is_none());
    assert!(!bound.message().contains(child.as_str()));
    assert_eq!(store.delivery_status(&latest).unwrap()["bound"], false);
    let service = Service::new(home.path.clone());
    let answer = dispatch::call(&service, "wait", json!({"agent_id":root,"sequence":2}))
        .await
        .unwrap();
    assert_eq!(answer["agent_id"], root.as_str());
    assert!(answer.get("run_id").is_none());
    assert!(dispatch::call(
        &service,
        "wait",
        json!({"agent_id":root,"sequence":2,"run_id":latest})
    )
    .await
    .is_err());
    for sequence in [
        json!(0),
        json!(-1),
        json!(1.5),
        json!("2"),
        json!(null),
        json!(4294967296_u64),
    ] {
        let invalid =
            json!({"session_id":"caller", "tool_response":{"agent_id":root,"sequence":sequence}});
        assert!(bind::normalize(&invalid, true, "codex_queue").is_err());
    }
    let conflict = json!({"session_id":"caller", "tool_response":[
        {"agent_id":root,"sequence":1},{"agent_id":root,"sequence":2}
    ]});
    assert!(bind::normalize(&conflict, true, "codex_queue").is_err());
    assert!(agent_identity::resolve_sequence(&store, &root, 99).is_err());
}

/// Public pagination counts agents, and transcript cursors survive a resume.
#[tokio::test]
async fn logical_pages_and_transcript_cursor_span_resumes() {
    let home = common::Home::new();
    let root = terminal_run(&home, None, "first");
    let service = Service::new(home.path.clone());
    let first = dispatch::call(&service, "transcript", json!({"agent_id":root,"limit":1}))
        .await
        .unwrap();
    let cursor = first["messages"][0]["seq"].as_i64().unwrap();
    terminal_run(&home, Some(&root), "second");
    let second = dispatch::call(
        &service,
        "transcript",
        json!({"agent_id":root,"cursor":cursor,"limit":1}),
    )
    .await
    .unwrap();
    assert_eq!(second["messages"][0]["content"], "second");
    assert_eq!(second["agent_id"], root.as_str());
    let other = terminal_run(&home, None, "other");
    let first = dispatch::call(&service, "list_agents", json!({"limit":1}))
        .await
        .unwrap();
    let second = dispatch::call(&service, "list_agents", json!({"limit":1,"offset":1}))
        .await
        .unwrap();
    assert_eq!(first["total"], 2);
    assert_eq!(first["next_offset"], 1);
    assert_eq!(second["complete"], true);
    let ids = [
        first["items"][0]["agent_id"].as_str().unwrap(),
        second["items"][0]["agent_id"].as_str().unwrap(),
    ];
    assert!(ids.contains(&root.as_str()) && ids.contains(&other.as_str()));
    let transcript = dispatch::call(&service, "transcript", json!({"agent_id":root,"limit":1}))
        .await
        .unwrap();
    assert_eq!(transcript["next_cursor"], cursor);
}
