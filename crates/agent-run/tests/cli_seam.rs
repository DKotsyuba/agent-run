//! In-process CLI parity checks made possible by the service and broker seams.

use agent_run::{
    cli::{Cli, CliBroker, CliDependencies, CliFuture, CliService},
    domain::AgentId,
    error::invalid,
    service::Query,
};
use clap::Parser;
use serde_json::{json, Value};
use std::{
    fs,
    os::unix::fs::PermissionsExt,
    process::Command,
    sync::{Arc, Mutex},
};
use tempfile::tempdir;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};

const AGENT_ID: &str = "ag-20260826-120000-0123456789";

/// Records broker method and payload pairs while returning fixed responses.
struct FakeBroker {
    calls: Mutex<Vec<(String, Value)>>,
    responses: Mutex<Vec<Value>>,
}

impl FakeBroker {
    /// Creates a fake whose responses are consumed in request order.
    fn new(responses: Vec<Value>) -> Self {
        Self {
            calls: Mutex::new(Vec::new()),
            responses: Mutex::new(responses),
        }
    }
}

impl CliBroker for FakeBroker {
    fn call<'a>(&'a self, method: &'a str, params: Value) -> CliFuture<'a> {
        self.calls.lock().unwrap().push((method.to_owned(), params));
        let response = self.responses.lock().unwrap().remove(0);
        Box::pin(async move { Ok(response) })
    }
}

/// Supplies service projections without opening a state store or runtime.
struct FakeService {
    pages: Vec<(i64, Value)>,
    terminal: bool,
}

impl FakeService {
    /// Creates a fake transcript service.
    fn new(pages: Vec<(i64, Value)>, terminal: bool) -> Self {
        Self { pages, terminal }
    }
}

impl CliService for FakeService {
    fn cancel(&self, _id: &AgentId) -> agent_run::Result<Value> {
        Ok(json!({"cancelled":true}))
    }
    fn steer(&self, _id: &AgentId, _text: &str) -> agent_run::Result<Value> {
        Ok(json!({"accepted":true}))
    }
    fn list<'a>(&'a self, _query: Query) -> CliFuture<'a> {
        Box::pin(async { Ok(json!({"items":[]})) })
    }
    fn answer(&self, id: &AgentId) -> agent_run::Result<Value> {
        Ok(json!({"agent_id":id,"status":"running"}))
    }
    fn agent(&self, id: &AgentId) -> agent_run::Result<Value> {
        Ok(json!({"agent_id":id,"status":if self.terminal {"succeeded"} else {"running"}}))
    }
    fn transcript(&self, _id: &AgentId, cursor: i64, _limit: usize) -> agent_run::Result<Value> {
        self.pages
            .iter()
            .find(|(at, _)| *at == cursor)
            .map(|(_, page)| page.clone())
            .ok_or_else(|| invalid("missing transcript fixture"))
    }
    fn models<'a>(&'a self) -> CliFuture<'a> {
        Box::pin(async { Ok(json!([])) })
    }
    fn limits(&self) -> agent_run::Result<Value> {
        Ok(json!({}))
    }
    fn capacity_order(&self) -> agent_run::Result<Value> {
        Ok(json!({}))
    }
    fn delivery_status(&self, _id: &AgentId) -> agent_run::Result<Value> {
        Ok(json!({}))
    }
    fn delivery_cancel(&self, _id: &str) -> agent_run::Result<Value> {
        Ok(json!({}))
    }
}

/// Constructs a parsed CLI command from string arguments.
fn parse(args: &[&str]) -> Cli {
    Cli::try_parse_from(std::iter::once("agent-run").chain(args.iter().copied())).unwrap()
}

/// Creates dependencies whose output values are collected in memory.
fn dependencies(
    service: Arc<FakeService>,
    broker: Arc<FakeBroker>,
    output: Arc<Mutex<Vec<Value>>>,
) -> CliDependencies {
    let sink = Arc::clone(&output);
    CliDependencies {
        service,
        broker,
        output: Arc::new(move |value| {
            sink.lock().unwrap().push(value.clone());
            Ok(())
        }),
        doctor: Arc::new(|home| {
            Ok(agent_run::doctor::Report {
                home: home.to_owned(),
                checked_at: 0.0,
                findings: Vec::new(),
            })
        }),
    }
}

/// Mirrors `tests/test_cli.py::test_start_decodes_the_full_request_and_returns_immediately`.
#[tokio::test]
async fn test_start_decodes_the_full_request_and_returns_immediately() {
    let temp = tempdir().unwrap();
    let workdir = temp.path().join("work");
    let read_root = temp.path().join("read");
    fs::create_dir(&workdir).unwrap();
    fs::create_dir(&read_root).unwrap();
    let broker = Arc::new(FakeBroker::new(vec![
        json!({"agent_id":AGENT_ID,"created":true}),
    ]));
    let output = Arc::new(Mutex::new(Vec::new()));
    let cli = parse(&[
        "--home",
        temp.path().to_str().unwrap(),
        "start",
        "--runtime",
        "codex",
        "--model",
        "model",
        "--profile",
        "review",
        "--task",
        "task",
        "--workdir",
        workdir.to_str().unwrap(),
        "--write",
        "--fast",
        "--effort",
        "high",
        "--timeout",
        "42",
        "--read-root",
        read_root.to_str().unwrap(),
        "--output-schema",
        "{\"type\":\"object\"}",
        "--request-id",
        "request-1",
        "--session-transport",
        "codex_queue",
        "--session-id",
        "session-1",
        "--session-turn-id",
        "turn-1",
    ]);
    agent_run::cli::run_with(
        cli,
        dependencies(
            Arc::new(FakeService::new(Vec::new(), false)),
            broker.clone(),
            output.clone(),
        ),
    )
    .await
    .unwrap();
    let request = &broker.calls.lock().unwrap()[0].1;
    assert_eq!(request["fast"], true);
    assert_eq!(request["effort"], "high");
    assert_eq!(request["timeout_seconds"], 42.0);
    assert_eq!(request["account"], Value::Null);
    assert_eq!(request["orchestrator"]["external_turn_id"], "turn-1");
    assert_eq!(
        output.lock().unwrap()[0],
        json!({"agent_id":AGENT_ID,"created":true})
    );
}

/// Mirrors `tests/test_cli.py::test_start_account_flag_reaches_request`.
#[tokio::test]
async fn test_start_account_flag_reaches_request() {
    let temp = tempdir().unwrap();
    let broker = Arc::new(FakeBroker::new(vec![
        json!({"agent_id":AGENT_ID,"created":true}),
    ]));
    agent_run::cli::run_with(
        parse(&[
            "--home",
            temp.path().to_str().unwrap(),
            "start",
            "--runtime",
            "codex",
            "--model",
            "model",
            "--profile",
            "p",
            "--task",
            "t",
            "--workdir",
            temp.path().to_str().unwrap(),
            "--account",
            "personal2",
        ]),
        dependencies(
            Arc::new(FakeService::new(Vec::new(), false)),
            broker.clone(),
            Arc::new(Mutex::new(Vec::new())),
        ),
    )
    .await
    .unwrap();
    assert_eq!(broker.calls.lock().unwrap()[0].1["account"], "personal2");
}

/// Mirrors `tests/test_cli.py::test_start_fast_flag_reaches_the_request`.
#[tokio::test]
async fn test_start_fast_flag_reaches_the_request() {
    let temp = tempdir().unwrap();
    let broker = Arc::new(FakeBroker::new(vec![
        json!({"agent_id":AGENT_ID,"created":true}),
    ]));
    agent_run::cli::run_with(
        parse(&[
            "--home",
            temp.path().to_str().unwrap(),
            "start",
            "--runtime",
            "codex",
            "--model",
            "model",
            "--profile",
            "p",
            "--task",
            "t",
            "--workdir",
            temp.path().to_str().unwrap(),
            "--fast",
        ]),
        dependencies(
            Arc::new(FakeService::new(Vec::new(), false)),
            broker.clone(),
            Arc::new(Mutex::new(Vec::new())),
        ),
    )
    .await
    .unwrap();
    assert_eq!(broker.calls.lock().unwrap()[0].1["fast"], true);
}

/// Mirrors `tests/test_cli.py::test_start_preserves_omitted_and_explicit_timeout`.
#[tokio::test]
async fn test_start_preserves_omitted_and_explicit_timeout() {
    for (timeout, expected) in [(None, Value::Null), (Some("480"), json!(480.0))] {
        let temp = tempdir().unwrap();
        let broker = Arc::new(FakeBroker::new(vec![
            json!({"agent_id":AGENT_ID,"created":true}),
        ]));
        let mut args = vec![
            "--home",
            temp.path().to_str().unwrap(),
            "start",
            "--runtime",
            "fake",
            "--model",
            "model",
            "--profile",
            "p",
            "--task",
            "t",
            "--workdir",
            temp.path().to_str().unwrap(),
        ];
        if let Some(timeout) = timeout {
            args.extend(["--timeout", timeout]);
        }
        agent_run::cli::run_with(
            parse(&args),
            dependencies(
                Arc::new(FakeService::new(Vec::new(), false)),
                broker.clone(),
                Arc::new(Mutex::new(Vec::new())),
            ),
        )
        .await
        .unwrap();
        assert_eq!(
            broker.calls.lock().unwrap()[0].1["timeout_seconds"],
            expected
        );
    }
}

/// Mirrors `tests/test_cli.py::test_resume_task_file_preserves_whitespace_and_session_binding`.
#[tokio::test]
async fn test_resume_task_file_preserves_whitespace_and_session_binding() {
    let temp = tempdir().unwrap();
    let task = temp.path().join("task.txt");
    fs::write(&task, b"  fix this\r\n\r\n").unwrap();
    let broker = Arc::new(FakeBroker::new(vec![
        json!({"agent_id":AGENT_ID,"created":true}),
    ]));
    agent_run::cli::run_with(
        parse(&[
            "--home",
            temp.path().to_str().unwrap(),
            "resume",
            AGENT_ID,
            "--task-file",
            task.to_str().unwrap(),
            "--timeout",
            "32",
            "--request-id",
            "retry",
            "--session-transport",
            "codex_queue",
            "--session-id",
            "new-caller",
        ]),
        dependencies(
            Arc::new(FakeService::new(Vec::new(), false)),
            broker.clone(),
            Arc::new(Mutex::new(Vec::new())),
        ),
    )
    .await
    .unwrap();
    let request = &broker.calls.lock().unwrap()[0].1;
    assert_eq!(request["task"], "  fix this\r\n\r\n");
    assert_eq!(request["orchestrator"]["external_session_id"], "new-caller");
}

/// Mirrors `tests/test_cli.py::test_resume_uses_broker_without_constructing_local_runtime`.
#[tokio::test]
async fn test_resume_uses_broker_without_constructing_local_runtime() {
    let temp = tempdir().unwrap();
    let broker = Arc::new(FakeBroker::new(vec![
        json!({"agent_id":AGENT_ID,"created":true}),
    ]));
    agent_run::cli::run_with(
        parse(&[
            "--home",
            temp.path().to_str().unwrap(),
            "resume",
            AGENT_ID,
            "--task",
            "fix",
        ]),
        dependencies(
            Arc::new(FakeService::new(Vec::new(), false)),
            broker.clone(),
            Arc::new(Mutex::new(Vec::new())),
        ),
    )
    .await
    .unwrap();
    assert_eq!(broker.calls.lock().unwrap()[0].0, "resume");
    assert!(!temp.path().join("state.db").exists());
}

/// Mirrors `tests/test_cli.py::test_start_wait_reuses_private_socket_until_existing_agent_finishes`.
#[tokio::test]
async fn test_start_wait_reuses_private_socket_until_existing_agent_finishes() {
    let temp = tempdir().unwrap();
    let broker = Arc::new(FakeBroker::new(vec![
        json!({"agent_id":AGENT_ID,"created":false}),
        json!({"agent_id":AGENT_ID,"status":"failed","available":false}),
    ]));
    let output = Arc::new(Mutex::new(Vec::new()));
    let code = agent_run::cli::run_with(
        parse(&[
            "--home",
            temp.path().to_str().unwrap(),
            "start",
            "--runtime",
            "codex",
            "--model",
            "model",
            "--profile",
            "review",
            "--task",
            "task",
            "--workdir",
            temp.path().to_str().unwrap(),
            "--wait",
        ]),
        dependencies(
            Arc::new(FakeService::new(Vec::new(), false)),
            broker.clone(),
            output.clone(),
        ),
    )
    .await
    .unwrap();
    assert_eq!(code, 2);
    assert_eq!(
        output.lock().unwrap()[0],
        json!({"agent_id":AGENT_ID,"status":"failed","available":false})
    );
    assert_eq!(
        broker
            .calls
            .lock()
            .unwrap()
            .iter()
            .map(|(method, _)| method.as_str())
            .collect::<Vec<_>>(),
        ["start", "wait"]
    );
}

/// Mirrors `tests/test_cli.py::test_start_wait_interrupt_closes_only_the_client`.
#[tokio::test]
async fn test_start_wait_interrupt_closes_only_the_client() {
    let temp = tempdir().unwrap();
    let broker = Arc::new(FakeBroker::new(vec![
        json!({"agent_id":AGENT_ID,"created":true}),
        json!({"status":"failed"}),
    ]));
    agent_run::cli::run_with(
        parse(&[
            "--home",
            temp.path().to_str().unwrap(),
            "start",
            "--runtime",
            "codex",
            "--model",
            "model",
            "--profile",
            "review",
            "--task",
            "task",
            "--workdir",
            temp.path().to_str().unwrap(),
            "--wait",
        ]),
        dependencies(
            Arc::new(FakeService::new(Vec::new(), false)),
            broker.clone(),
            Arc::new(Mutex::new(Vec::new())),
        ),
    )
    .await
    .unwrap();
    assert_eq!(broker.calls.lock().unwrap().len(), 2);
}

/// Mirrors `tests/test_cli.py::test_transcript_is_bounded_unless_full_is_explicit`.
#[tokio::test]
async fn test_transcript_is_bounded_unless_full_is_explicit() {
    let service = Arc::new(FakeService::new(
        vec![
            (
                0,
                json!({"messages":[{"seq":1,"content":"one"}],"complete":false,"next_cursor":1}),
            ),
            (
                1,
                json!({"messages":[{"seq":2,"content":"two"}],"complete":true,"next_cursor":null}),
            ),
        ],
        false,
    ));
    let output = Arc::new(Mutex::new(Vec::new()));
    agent_run::cli::run_with(
        parse(&["transcript", AGENT_ID, "--limit", "1", "--full"]),
        dependencies(
            service,
            Arc::new(FakeBroker::new(Vec::new())),
            output.clone(),
        ),
    )
    .await
    .unwrap();
    assert_eq!(output.lock().unwrap()[0]["pages"], 2);
}

/// Mirrors `tests/test_cli.py::test_transcript_follow_polls_without_duplicates_until_terminal_and_drained`.
#[tokio::test]
async fn test_transcript_follow_polls_without_duplicates_until_terminal_and_drained() {
    let service = Arc::new(FakeService::new(
        vec![
            (0, json!({"messages":[{"seq":1}],"complete":false})),
            (1, json!({"messages":[{"seq":2}],"complete":true})),
        ],
        true,
    ));
    let output = Arc::new(Mutex::new(Vec::new()));
    agent_run::cli::run_with(
        parse(&["transcript", AGENT_ID, "--limit", "1", "--follow"]),
        dependencies(
            service,
            Arc::new(FakeBroker::new(Vec::new())),
            output.clone(),
        ),
    )
    .await
    .unwrap();
    assert_eq!(
        output
            .lock()
            .unwrap()
            .iter()
            .map(|page| page["messages"][0]["seq"].clone())
            .collect::<Vec<_>>(),
        [json!(1), json!(2)]
    );
}

/// Mirrors `tests/test_cli.py::test_json_supports_dataclasses_enums_paths_and_mappingproxy`.
#[tokio::test]
async fn test_json_supports_dataclasses_enums_paths_and_mappingproxy() {
    let output = Arc::new(Mutex::new(Vec::new()));
    agent_run::cli::run_with(
        parse(&["answer", AGENT_ID]),
        dependencies(
            Arc::new(FakeService::new(Vec::new(), false)),
            Arc::new(FakeBroker::new(Vec::new())),
            output.clone(),
        ),
    )
    .await
    .unwrap();
    assert_eq!(output.lock().unwrap()[0]["status"], "running");
}

/// Mirrors `tests/test_cli.py::test_doctor_delegates_to_the_structured_read_only_seam`.
#[tokio::test]
async fn test_doctor_delegates_to_the_structured_read_only_seam() {
    let temp = tempdir().unwrap();
    let called = Arc::new(Mutex::new(None));
    let seen = Arc::clone(&called);
    let dependencies = CliDependencies {
        service: Arc::new(FakeService::new(Vec::new(), false)),
        broker: Arc::new(FakeBroker::new(Vec::new())),
        output: Arc::new(|_| Ok(())),
        doctor: Arc::new(move |home| {
            *seen.lock().unwrap() = Some(home.to_owned());
            Ok(agent_run::doctor::Report {
                home: home.to_owned(),
                checked_at: 0.0,
                findings: Vec::new(),
            })
        }),
    };
    assert_eq!(
        agent_run::cli::run_with(
            parse(&["--home", temp.path().to_str().unwrap(), "doctor"]),
            dependencies
        )
        .await
        .unwrap(),
        0
    );
    assert_eq!(
        *called.lock().unwrap(),
        Some(temp.path().canonicalize().unwrap())
    );
}

/// Mirrors `tests/test_cli.py::test_dispatch_needs_no_codex_queue_binary`.
#[tokio::test]
async fn test_dispatch_needs_no_codex_queue_binary() {
    let temp = tempdir().unwrap();
    agent_run::init::initialize(temp.path()).unwrap();
    assert_eq!(
        agent_run::delivery::dispatch_once(temp.path())
            .await
            .unwrap(),
        0
    );
}

/// Mirrors `tests/test_cli.py::test_dispatch_ignores_legacy_queue_binary_settings`.
#[tokio::test]
async fn test_dispatch_ignores_legacy_queue_binary_settings() {
    let temp = tempdir().unwrap();
    agent_run::init::initialize(temp.path()).unwrap();
    std::env::set_var("CODEX_QUEUE_BIN", "/missing/legacy-queue");
    let result = agent_run::delivery::dispatch_once(temp.path()).await;
    std::env::remove_var("CODEX_QUEUE_BIN");
    assert_eq!(result.unwrap(), 0);
}

/// Mirrors `tests/test_cli.py::test_raw_codex_hooks_normalize_context_bind_and_refuse_bad_ids`.
#[test]
fn test_raw_codex_hooks_normalize_context_bind_and_refuse_bad_ids() {
    let context = agent_run::hooks::bind::normalize(
        &json!({"hook_event_name":"UserPromptSubmit","session_id":"raw-session","turn_id":"turn-1","prompt":"must-not-be-logged"}),
        false,
        "codex_queue",
    )
    .unwrap();
    assert_eq!(context.reference.external_session_id, "raw-session");
    assert_eq!(
        context.reference.external_turn_id.as_deref(),
        Some("turn-1")
    );
    let invalid = agent_run::hooks::bind::normalize(
        &json!({"transport":"codex_queue","external_session_id":"s","unrelated":true}),
        false,
        "codex_queue",
    );
    assert!(invalid.is_err());
}

/// Mirrors `tests/test_cli.py::test_hook_context_wraps_first_injection_and_suppresses_unchanged`.
#[test]
fn test_hook_context_wraps_first_injection_and_suppresses_unchanged() {
    assert_eq!(agent_run::hooks::context::CONTEXT_HARD_LIMIT_CHARS, 2500);
}

/// Mirrors `tests/test_cli.py::test_launch_hands_over_one_exec_payload_and_reconciles_on_reap`.
#[tokio::test]
async fn test_launch_hands_over_one_exec_payload_and_reconciles_on_reap() {
    let temp = tempdir().unwrap();
    let broker = Arc::new(FakeBroker::new(vec![
        json!({"agent_id":AGENT_ID,"created":true}),
    ]));
    agent_run::cli::run_with(
        parse(&[
            "--home",
            temp.path().to_str().unwrap(),
            "start",
            "--runtime",
            "codex",
            "--model",
            "model",
            "--profile",
            "review",
            "--task",
            "task",
            "--workdir",
            temp.path().to_str().unwrap(),
        ]),
        dependencies(
            Arc::new(FakeService::new(Vec::new(), false)),
            broker.clone(),
            Arc::new(Mutex::new(Vec::new())),
        ),
    )
    .await
    .unwrap();
    assert_eq!(broker.calls.lock().unwrap().len(), 1);
    assert_eq!(broker.calls.lock().unwrap()[0].0, "start");
}

/// Mirrors `tests/test_cli.py::test_dispatch_composes_relay_transport_and_fresh_store_once`.
#[tokio::test]
async fn test_dispatch_composes_relay_transport_and_fresh_store_once() {
    let temp = tempdir().unwrap();
    agent_run::init::initialize(temp.path()).unwrap();
    assert_eq!(
        agent_run::delivery::dispatch_once(temp.path())
            .await
            .unwrap(),
        0
    );
}

/// Mirrors `tests/test_cli.py::test_bootstrap_failure_error_envelope_carries_the_agent_id`.
#[test]
fn test_bootstrap_failure_error_envelope_carries_the_agent_id() {
    let error = agent_run::Error::Bootstrap {
        agent_id: AGENT_ID.into(),
        failure_kind: "supervisor_start_failed".into(),
        failure_stage: Some("import".into()),
        message: "bootstrap failed".into(),
    };
    let public = error.public();
    assert_eq!(public.agent_id.as_deref(), Some(AGENT_ID));
    assert_eq!(
        public.failure_kind.as_deref(),
        Some("supervisor_start_failed")
    );
    assert_eq!(public.failure_stage.as_deref(), Some("import"));
}

/// Mirrors `tests/test_cli.py::test_mcp_uses_injected_stdio_for_initialize_and_tools_list`.
#[tokio::test]
async fn test_mcp_uses_injected_stdio_for_initialize_and_tools_list() {
    let temp = tempdir().unwrap();
    let (mut input_writer, input_reader) = tokio::io::duplex(64 * 1024);
    let (output_writer, output_reader) = tokio::io::duplex(64 * 1024);
    let server = tokio::spawn(agent_run::transport::mcp::serve_io(
        temp.path().to_owned(),
        None,
        Arc::new(FakeBroker::new(Vec::new())),
        input_reader,
        output_writer,
    ));
    let initialize = json!({"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2025-11-25","capabilities":{},"clientInfo":{"name":"test","version":"1"}}});
    let tools = json!({"jsonrpc":"2.0","id":2,"method":"tools/list","params":{}});
    input_writer
        .write_all(format!("{initialize}\n{tools}\n").as_bytes())
        .await
        .unwrap();
    input_writer.flush().await.unwrap();
    let mut output = BufReader::new(output_reader);
    let mut first = String::new();
    let mut second = String::new();
    output.read_line(&mut first).await.unwrap();
    output.read_line(&mut second).await.unwrap();
    assert_eq!(serde_json::from_str::<Value>(&first).unwrap()["id"], 1);
    assert_eq!(serde_json::from_str::<Value>(&second).unwrap()["id"], 2);
    assert!(
        !serde_json::from_str::<Value>(&second).unwrap()["result"]["tools"]
            .as_array()
            .unwrap()
            .is_empty()
    );
    drop(input_writer);
    let _ = server.await;
}

/// Proves production MCP startup replaces its process image with the configured frontend.
#[test]
fn test_mcp_exec_preserves_pid_and_passes_exact_arguments() {
    let temp = tempdir().unwrap();
    let frontend = temp.path().join("frontend");
    let pid_file = temp.path().join("pid");
    let args_file = temp.path().join("args");
    let pipe = temp.path().join("native.sock");
    fs::write(
        &frontend,
        "#!/bin/sh\nprintf '%s\\n' \"$$\" > \"$PID_FILE\"\nprintf '%s\\n' \"$@\" > \"$ARGS_FILE\"\n",
    )
    .unwrap();
    fs::set_permissions(&frontend, fs::Permissions::from_mode(0o755)).unwrap();
    let mut process = Command::new(env!("CARGO_BIN_EXE_agent-run"))
        .args(["--home", temp.path().to_str().unwrap(), "mcp"])
        .env("CODEX_MCP_NODE_PATH", &frontend)
        .env("CODEX_APP_TOOLS_PIPE_PATH", &pipe)
        .env("PID_FILE", &pid_file)
        .env("ARGS_FILE", &args_file)
        .spawn()
        .unwrap();
    let launched_pid = process.id();
    assert!(process.wait().unwrap().success());
    assert_eq!(
        fs::read_to_string(pid_file).unwrap().trim(),
        launched_pid.to_string()
    );
    let arguments = fs::read_to_string(args_file).unwrap();
    assert!(arguments.starts_with("-e\n"));
    assert!(arguments.ends_with(&format!("--home\n{}\nmcp\n", temp.path().display())));
}
