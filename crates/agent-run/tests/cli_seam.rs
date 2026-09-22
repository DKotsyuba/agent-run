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
    process::{Command, Stdio},
    sync::{Arc, Mutex},
};
use tempfile::tempdir;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};

const AGENT_ID: &str = "ag-20260826-120000-0123456789";

/// Serializes follow-mode tests: the SIGINT test signals the whole process, so
/// no other test may hold a Ctrl-C handler while it runs.
static FOLLOW_TESTS: Mutex<()> = Mutex::new(());

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
        text_output: Arc::new(|_| Ok(())),
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
    let _guard = FOLLOW_TESTS.lock().unwrap();
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
        text_output: Arc::new(|_| Ok(())),
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

/// Runs production MCP startup with an unusable optional frontend and verifies raw MCP protocol.
fn assert_direct_mcp_fallback(frontend: &std::path::Path, home: &std::path::Path) {
    use std::io::Write as _;

    let pipe = home.join("unreachable-native.sock");
    let mut process = Command::new(env!("CARGO_BIN_EXE_agent-run"))
        .args(["--home", home.to_str().unwrap(), "mcp"])
        .env("CODEX_MCP_NODE_PATH", frontend)
        .env("CODEX_APP_TOOLS_PIPE_PATH", &pipe)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    let initialize = json!({"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2025-11-25","capabilities":{},"clientInfo":{"name":"test","version":"1"}}});
    let tools = json!({"jsonrpc":"2.0","id":2,"method":"tools/list","params":{}});
    let mut input = process.stdin.take().unwrap();
    writeln!(input, "{initialize}").unwrap();
    writeln!(input, "{tools}").unwrap();
    drop(input);
    let output = process.wait_with_output().unwrap();
    assert!(
        output.status.success(),
        "fallback MCP failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let responses = String::from_utf8(output.stdout).unwrap();
    let responses = responses
        .lines()
        .map(|line| serde_json::from_str::<Value>(line).unwrap())
        .collect::<Vec<_>>();
    assert_eq!(responses.len(), 2);
    assert_eq!(responses[0]["id"], 1);
    assert_eq!(responses[1]["id"], 2);
    assert!(!responses[1]["result"]["tools"]
        .as_array()
        .unwrap()
        .is_empty());
    assert_eq!(
        String::from_utf8(output.stderr).unwrap(),
        "agent-run: Desktop MCP frontend unavailable; continuing without relay delivery\n"
    );
}

/// Proves malformed, missing, non-executable, and failed frontends preserve direct MCP service.
#[test]
fn test_mcp_unusable_frontends_fall_back_to_direct_protocol() {
    let temp = tempdir().unwrap();
    assert_direct_mcp_fallback(std::path::Path::new("relative-node"), temp.path());

    let missing = temp.path().join("missing-node");
    assert_direct_mcp_fallback(&missing, temp.path());

    let non_executable = temp.path().join("non-executable-node");
    fs::write(&non_executable, "#!/bin/sh\nexit 0\n").unwrap();
    fs::set_permissions(&non_executable, fs::Permissions::from_mode(0o644)).unwrap();
    assert_direct_mcp_fallback(&non_executable, temp.path());

    let invalid_executable = temp.path().join("invalid-executable-node");
    fs::write(
        &invalid_executable,
        "#!/definitely/missing/agent-run-interpreter\n",
    )
    .unwrap();
    fs::set_permissions(&invalid_executable, fs::Permissions::from_mode(0o755)).unwrap();
    assert_direct_mcp_fallback(&invalid_executable, temp.path());
}

/// Creates dependencies whose human-readable transcript chunks are
/// concatenated in memory exactly as the raw sink would write them.
fn text_dependencies(service: Arc<FakeService>, text: Arc<Mutex<String>>) -> CliDependencies {
    let sink = Arc::clone(&text);
    CliDependencies {
        service,
        broker: Arc::new(FakeBroker::new(Vec::new())),
        output: Arc::new(|_| Ok(())),
        text_output: Arc::new(move |chunk| {
            sink.lock().unwrap().push_str(chunk);
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

/// One transcript page fixture mixing model text, tool activity, and controls.
fn activity_page(complete: bool) -> Value {
    json!({"messages":[
        {"seq":1,"role":"user","content":"review \x1b[1mthis\x1b[0m"},
        {"seq":2,"role":"assistant","content":"looking now"},
        {"seq":3,"role":"tool_call","name":"shell","content":"{\"cmd\":\"ls\"}"},
        {"seq":4,"role":"tool_result","content":"file.txt\x1b]0;pwned\x07"},
        {"seq":5,"role":"runtime_session","content":"{\"id\":\"s1\"}"}
    ],"complete":complete})
}

/// Proves `--format text` renders model text, tool activity, and sanitized results.
#[tokio::test]
async fn test_transcript_explicit_text_format_renders_sanitized_activity() {
    let text = Arc::new(Mutex::new(String::new()));
    agent_run::cli::run_with(
        parse(&["transcript", AGENT_ID, "--format", "text"]),
        text_dependencies(
            Arc::new(FakeService::new(vec![(0, activity_page(true))], true)),
            text.clone(),
        ),
    )
    .await
    .unwrap();
    assert_eq!(
        *text.lock().unwrap(),
        "review this\nlooking now\n-> shell {\"cmd\":\"ls\"}\n<- file.txt\nruntime_session: {\"id\":\"s1\"}\n"
    );
}

/// Proves explicit JSON and the piped default keep the historical page shape.
#[tokio::test]
async fn test_transcript_json_format_stays_the_default_when_not_a_tty() {
    for args in [
        vec!["transcript", AGENT_ID],
        vec!["transcript", AGENT_ID, "--format", "json"],
    ] {
        let output = Arc::new(Mutex::new(Vec::new()));
        let text = Arc::new(Mutex::new(String::new()));
        let json_sink = Arc::clone(&output);
        let text_sink = Arc::clone(&text);
        let mut argv = vec!["agent-run"];
        argv.extend(args.iter().copied());
        agent_run::cli::run_with(
            Cli::try_parse_from(argv).unwrap(),
            CliDependencies {
                service: Arc::new(FakeService::new(vec![(0, activity_page(true))], true)),
                broker: Arc::new(FakeBroker::new(Vec::new())),
                output: Arc::new(move |value| {
                    json_sink.lock().unwrap().push(value.clone());
                    Ok(())
                }),
                text_output: Arc::new(move |chunk| {
                    text_sink.lock().unwrap().push_str(chunk);
                    Ok(())
                }),
                doctor: Arc::new(|home| {
                    Ok(agent_run::doctor::Report {
                        home: home.to_owned(),
                        checked_at: 0.0,
                        findings: Vec::new(),
                    })
                }),
            },
        )
        .await
        .unwrap();
        let pages = output.lock().unwrap();
        assert_eq!(pages.len(), 1, "one JSON page for {args:?}");
        assert_eq!(pages[0]["messages"][2]["role"], "tool_call", "for {args:?}");
        assert!(
            text.lock().unwrap().is_empty(),
            "no text output for {args:?}"
        );
    }
}

/// Proves text follow drains terminal journals in durable order without repeats.
#[tokio::test]
async fn test_transcript_text_follow_drains_without_duplicates() {
    let _guard = FOLLOW_TESTS.lock().unwrap();
    let text = Arc::new(Mutex::new(String::new()));
    agent_run::cli::run_with(
        parse(&[
            "transcript",
            AGENT_ID,
            "--limit",
            "1",
            "--follow",
            "--format",
            "text",
        ]),
        text_dependencies(
            Arc::new(FakeService::new(
                vec![
                    (
                        0,
                        json!({"messages":[{"seq":1,"role":"assistant","content":"one","raw_ref":"item-1"}],"complete":false}),
                    ),
                    (
                        1,
                        json!({"messages":[{"seq":2,"role":"assistant","content":"two","raw_ref":"item-2"}],"complete":true}),
                    ),
                ],
                true,
            )),
            text.clone(),
        ),
    )
    .await
    .unwrap();
    assert_eq!(*text.lock().unwrap(), "one\ntwo\n");
}

/// Proves interrupting the viewer exits cleanly while the agent keeps running.
#[tokio::test]
async fn test_transcript_viewer_interrupt_leaves_the_agent_running() {
    let _guard = FOLLOW_TESTS.lock().unwrap();
    let text = Arc::new(Mutex::new(String::new()));
    let service = Arc::new(FakeService::new(
        vec![(
            0,
            json!({"messages":[{"seq":1,"role":"assistant","content":"hello"}],"complete":true}),
        )],
        false,
    ));
    let runner = agent_run::cli::run_with(
        parse(&["transcript", AGENT_ID, "--follow", "--format", "text"]),
        text_dependencies(service.clone(), text.clone()),
    );
    let task = tokio::spawn(runner);
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    // SAFETY: delivering SIGINT to this test process; tokio's installed
    // handler turns it into the viewer's Ctrl-C branch instead of an abort.
    unsafe {
        libc::kill(std::process::id() as libc::pid_t, libc::SIGINT);
    }
    tokio::time::timeout(std::time::Duration::from_secs(10), task)
        .await
        .expect("viewer exits on Ctrl-C")
        .unwrap()
        .unwrap();
    assert_eq!(*text.lock().unwrap(), "hello\n");
    // The viewer has no cancellation path: the agent status stays untouched.
    assert_eq!(
        service
            .agent(&AGENT_ID.parse().expect("fixture agent id"))
            .expect("agent view")["status"],
        "running"
    );
}

/// Proves same-identity journal fragments stream continuously across polls.
///
/// Mirrors the real Codex producer: `item/agentMessage/delta` journals
/// word-sized fragments of one agentMessage under one `itemId`, so the
/// viewer must render `Hello`, ` `, `world` from three pages as one line.
#[tokio::test]
async fn test_transcript_text_streams_fragments_across_pages() {
    let _guard = FOLLOW_TESTS.lock().unwrap();
    let text = Arc::new(Mutex::new(String::new()));
    let rows = |seq: i64, content: &str| json!({"seq":seq,"role":"assistant","content":content,"raw_ref":"same-message"});
    agent_run::cli::run_with(
        parse(&[
            "transcript",
            AGENT_ID,
            "--limit",
            "1",
            "--follow",
            "--format",
            "text",
        ]),
        text_dependencies(
            Arc::new(FakeService::new(
                vec![
                    (0, json!({"messages":[rows(1,"Hello")],"complete":false})),
                    (1, json!({"messages":[rows(2," ")],"complete":false})),
                    (2, json!({"messages":[rows(3,"world")],"complete":true})),
                ],
                true,
            )),
            text.clone(),
        ),
    )
    .await
    .unwrap();
    assert_eq!(*text.lock().unwrap(), "Hello world\n");
}

/// Proves escape sequences split across polling pages never leak payload.
#[tokio::test]
async fn test_transcript_text_consumes_escape_splits_across_pages() {
    let _guard = FOLLOW_TESTS.lock().unwrap();
    let text = Arc::new(Mutex::new(String::new()));
    let row = |seq: i64, content: &str| json!({"seq":seq,"role":"assistant","content":content,"raw_ref":"m"});
    agent_run::cli::run_with(
        parse(&[
            "transcript",
            AGENT_ID,
            "--limit",
            "1",
            "--follow",
            "--format",
            "text",
        ]),
        text_dependencies(
            Arc::new(FakeService::new(
                vec![
                    (
                        0,
                        json!({"messages":[row(1,"bad \u{1b}[3")],"complete":false}),
                    ),
                    (
                        1,
                        json!({"messages":[row(2,"1mred\u{1b}[0m ok")],"complete":true}),
                    ),
                ],
                true,
            )),
            text.clone(),
        ),
    )
    .await
    .unwrap();
    assert_eq!(*text.lock().unwrap(), "bad red ok\n");
}

/// Supplies pages while holding every poll after the first behind a barrier.
///
/// The viewer's second `transcript` call parks until the test releases it,
/// modeling a producer that has journaled one fragment and is still running.
struct PausingService {
    /// Barrier the second and later polls wait on.
    release: Mutex<std::sync::mpsc::Receiver<()>>,
}

impl CliService for PausingService {
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
        Ok(json!({"agent_id":id,"status":"succeeded"}))
    }
    fn transcript(&self, _id: &AgentId, cursor: i64, _limit: usize) -> agent_run::Result<Value> {
        if cursor >= 1 {
            self.release
                .lock()
                .unwrap()
                .recv_timeout(std::time::Duration::from_secs(30))
                .expect("second poll barrier released");
            Ok(
                json!({"messages":[{"seq":2,"role":"assistant","content":" world","raw_ref":"m"}],"complete":true}),
            )
        } else {
            Ok(
                json!({"messages":[{"seq":1,"role":"assistant","content":"Hello","raw_ref":"m"}],"complete":false}),
            )
        }
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

/// Proves rendered bytes are sink-visible while the producer is still open:
/// after page 1 and before page 2 exists, the raw output is exactly `Hello`.
#[tokio::test(flavor = "multi_thread")]
async fn test_transcript_text_bytes_are_visible_before_the_stream_completes() {
    let _guard = FOLLOW_TESTS.lock().unwrap();
    let text = Arc::new(Mutex::new(String::new()));
    let (release, released) = std::sync::mpsc::channel::<()>();
    let (chunk_tx, chunk_rx) = std::sync::mpsc::channel::<()>();
    let sink = Arc::clone(&text);
    let notify = chunk_tx.clone();
    let dependencies = CliDependencies {
        service: Arc::new(PausingService {
            release: Mutex::new(released),
        }),
        broker: Arc::new(FakeBroker::new(Vec::new())),
        output: Arc::new(|_| Ok(())),
        text_output: Arc::new(move |chunk| {
            sink.lock().unwrap().push_str(chunk);
            let _ = notify.send(());
            Ok(())
        }),
        doctor: Arc::new(|home| {
            Ok(agent_run::doctor::Report {
                home: home.to_owned(),
                checked_at: 0.0,
                findings: Vec::new(),
            })
        }),
    };
    let runner = tokio::spawn(agent_run::cli::run_with(
        parse(&["transcript", AGENT_ID, "--follow", "--format", "text"]),
        dependencies,
    ));
    chunk_rx
        .recv_timeout(std::time::Duration::from_secs(10))
        .expect("first fragment reaches the sink");
    // The producer's second poll is still parked behind the barrier: the
    // viewer must already hold exactly the first fragment's bytes.
    assert!(!runner.is_finished(), "viewer is still following");
    assert_eq!(*text.lock().unwrap(), "Hello");
    release.send(()).expect("release second poll");
    tokio::time::timeout(std::time::Duration::from_secs(10), runner)
        .await
        .expect("viewer drains and exits")
        .unwrap()
        .unwrap();
    assert_eq!(*text.lock().unwrap(), "Hello world\n");
}

/// Drives the real `agent-run` binary against a live durable home.
///
/// Admits a row directly (the preparation `Service::start` performs, minus
/// any launch), journals fragments from the test process while the viewer
/// follows, and reads the child's pipe byte-exactly before it exits.
#[tokio::test(flavor = "multi_thread")]
async fn test_transcript_text_pipe_receives_fragments_before_the_agent_finishes() {
    let _guard = FOLLOW_TESTS.lock().unwrap();
    let temp = tempdir().unwrap();
    let home = temp.path().canonicalize().unwrap();
    let initialized = Command::new(env!("CARGO_BIN_EXE_agent-run"))
        .args(["--home", home.to_str().unwrap(), "init"])
        .output()
        .unwrap();
    assert!(initialized.status.success(), "{initialized:?}");
    // A configured-but-never-launched runtime satisfies request validation;
    // the viewer only reads the journal, so the binary is never executed.
    std::fs::write(
        home.join("config.toml"),
        format!(
            "schema_version=1\n[runtimes.mock]\nenabled=true\nadapter='claude'\nbinary={}\nhome={}\nmodels=['fixture']\nlimits_source='none'\n",
            serde_json::json!("/bin/true"),
            serde_json::json!(home.join("runtime").to_string_lossy().into_owned()),
        ),
    )
    .unwrap();
    std::fs::create_dir_all(home.join("profiles")).unwrap();
    std::fs::write(home.join("profiles/review.md"), "Review.\n").unwrap();
    let mut request = agent_run::domain::StartRequest {
        runtime: "mock".into(),
        model: "fixture".into(),
        profile: "review".into(),
        task: "live pipe fixture".into(),
        workdir: home.clone(),
        write: false,
        fast: false,
        effort: None,
        timeout_seconds: None,
        read_roots: vec![],
        output_schema: None,
        orchestrator: None,
        request_id: None,
        account: None,
        required_constraints: Default::default(),
    };
    request.validate().unwrap();
    let config = agent_run::config::Config::load(&home).unwrap();
    let mut store = agent_run::state::Store::open(&home).unwrap();
    let (id, created) = store
        .admit(&request, &config, &serde_json::json!({}), None)
        .unwrap();
    assert!(created, "expected a freshly admitted row");

    let mut child = Command::new(env!("CARGO_BIN_EXE_agent-run"))
        .args([
            "--home",
            home.to_str().unwrap(),
            "transcript",
            &id.to_string(),
            "--follow",
            "--format",
            "text",
        ])
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    let stdout = child.stdout.take().unwrap();
    let stderr_handle = child.stderr.take().unwrap();
    let pipe = Arc::new(Mutex::new(Vec::<u8>::new()));
    let writer = Arc::clone(&pipe);
    let reader = std::thread::spawn(move || {
        use std::io::Read;
        let mut stdout = stdout;
        let mut buf = [0u8; 4096];
        while let Ok(n) = stdout.read(&mut buf) {
            if n == 0 {
                break;
            }
            writer.lock().unwrap().extend_from_slice(&buf[..n]);
        }
    });
    let piperr = std::thread::spawn(move || {
        use std::io::Read;
        let mut stderr_handle = stderr_handle;
        let mut err = Vec::new();
        let _ = stderr_handle.read_to_end(&mut err);
        err
    });
    // Producer side: journal the first fragment while the agent is running.
    agent_run::state::Store::open(&home)
        .unwrap()
        .message(&id, "assistant", "Hello", None, Some("m1"))
        .unwrap();
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
    while *pipe.lock().unwrap() != b"Hello".to_vec() {
        assert!(
            std::time::Instant::now() < deadline,
            "pipe must hold exactly `Hello`, held {:?}",
            pipe.lock().unwrap()
        );
        assert!(
            child.try_wait().unwrap().is_none(),
            "viewer must still be running"
        );
        std::thread::sleep(std::time::Duration::from_millis(50));
    }
    // The agent is still nonterminal: no terminal row exists, and the pipe
    // holds the fragment alone — no newline, no reflow, no truncation.
    std::thread::sleep(std::time::Duration::from_millis(300));
    assert_eq!(*pipe.lock().unwrap(), b"Hello".to_vec());
    agent_run::state::Store::open(&home)
        .unwrap()
        .message(&id, "assistant", " world", None, Some("m1"))
        .unwrap();
    agent_run::state::Store::open(&home)
        .unwrap()
        .finish(
            &id,
            &agent_run::domain::Outcome::failure("fixture_complete"),
            None,
            None,
        )
        .unwrap();
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
    loop {
        if child.try_wait().unwrap().is_some() {
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "viewer drains and exits"
        );
        std::thread::sleep(std::time::Duration::from_millis(50));
    }
    // Exit is observed before the pipe reaches EOF; join the reader so the
    // exact final bytes are captured, not just what was drained so far.
    reader.join().expect("pipe reader finishes");
    let status = child.wait().unwrap();
    let stderr =
        String::from_utf8_lossy(&piperr.join().expect("stderr reader finishes")).into_owned();
    assert_eq!(
        *pipe.lock().unwrap(),
        b"Hello world\n".to_vec(),
        "viewer exit {status:?}, stderr {stderr:?}"
    );
}
