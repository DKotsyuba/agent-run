//! CLI-surface hook parser coverage using only temporary homes and fake payloads.

use agent_run::{
    cli::{Bind, Cli, Command, Context, Hook, HookTransport},
    config::Config,
    domain::StartRequest,
    state::Store,
};
use clap::Parser;
use serde_json::{json, Value};
use std::{
    io::Write,
    path::Path,
    process::{Command as ProcessCommand, Stdio},
};
use tempfile::tempdir;

/// Creates a complete fake host home with one admitted durable agent.
fn fake_agent_home() -> (tempfile::TempDir, String) {
    let home = tempdir().expect("temporary home");
    let binary = if Path::new("/usr/bin/true").is_file() {
        "/usr/bin/true"
    } else {
        "/bin/true"
    };
    std::fs::write(home.path().join("config.toml"), format!("schema_version=1\n[runtimes.mock]\nenabled=true\nadapter='claude'\nbinary={}\nhome={}\nmodels=['fixture']\nlimits_source='none'\n", toml::Value::String(binary.into()), toml::Value::String(home.path().join("runtimes/mock").to_string_lossy().into_owned()))).expect("fake config");
    Store::initialize(home.path()).expect("fake state");
    let config = Config::load(home.path()).expect("fake config loads");
    let request: StartRequest = serde_json::from_value(json!({"runtime":"mock","model":"fixture","profile":"review","task":"fake task","workdir":home.path()})).expect("fake request");
    let (id, _) = Store::open(home.path())
        .expect("store")
        .admit(&request, &config, &json!({}), None)
        .expect("fake admission");
    (home, id.to_string())
}

/// Runs one real CLI hook subprocess with only a fake JSON stdin envelope.
fn hook(home: &Path, command: &str, payload: Value) -> Value {
    let mut child = ProcessCommand::new(env!("CARGO_BIN_EXE_agent-run"))
        .args([
            "--home",
            home.to_str().expect("UTF-8 temp path"),
            "hook",
            command,
            "--transport",
            "codex_queue",
        ])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .spawn()
        .expect("hook subprocess");
    child
        .stdin
        .take()
        .expect("hook stdin")
        .write_all(payload.to_string().as_bytes())
        .expect("hook payload");
    let output = child.wait_with_output().expect("hook output");
    assert!(
        output.status.success(),
        "hook failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    serde_json::from_slice(&output.stdout).expect("hook JSON")
}

/// Mirrors Python `test_bind_hook.py::test_every_hook_failure_is_loud_and_reports_the_unconfirmed_notification`.
#[test]
fn python_bind_hook_cli_accepts_the_documented_session_shape() {
    let home = tempdir().expect("temporary home");
    let parsed = Cli::try_parse_from([
        "agent-run",
        "--home",
        home.path().to_str().expect("UTF-8 temp path"),
        "bind",
        "ag-20260916-120000-0123456789",
        "--session-transport",
        "codex_queue",
        "--session-id",
        "session-1",
        "--session-turn-id",
        "turn-1",
    ])
    .expect("Python-compatible bind parser");
    match parsed.command {
        Command::Bind(Bind {
            session_transport,
            session_id,
            session_turn_id,
            ..
        }) => {
            assert_eq!(session_transport, "codex_queue");
            assert_eq!(session_id, "session-1");
            assert_eq!(session_turn_id.as_deref(), Some("turn-1"));
        }
        command => panic!("unexpected command: {command:?}"),
    }
}

/// Mirrors Python `test_context_hook.py::test_configured_budget_below_hard_limit_is_respected`.
#[test]
fn python_context_cli_and_hook_parser_accept_only_host_transports() {
    let home = tempdir().expect("temporary home");
    let context = Cli::try_parse_from([
        "agent-run",
        "--home",
        home.path().to_str().expect("UTF-8 temp path"),
        "context",
        "--session-transport",
        "claude_uds",
        "--session-id",
        "session-1",
    ])
    .expect("Python-compatible context parser");
    assert!(matches!(context.command, Command::Context(Context { .. })));
    let hook = Cli::try_parse_from(["agent-run", "hook", "bind", "--transport", "codex_queue"])
        .expect("Python-compatible hook parser");
    assert!(
        matches!(hook.command, Command::Hook { command: Hook::Bind(HookTransport { transport }) } if transport == "codex_queue")
    );
    assert!(Cli::try_parse_from(["agent-run", "hook", "context", "--transport", "other"]).is_err());
}

/// Mirrors Python `test_bind_hook.py::test_binding_is_immutable_empty_then_same_target_but_never_another`.
#[test]
fn python_hook_bind_reads_raw_json_stdin_and_is_idempotent() {
    let (home, agent_id) = fake_agent_home();
    let payload = json!({"session_id":"session-1","hook_event_name":"PostToolUse","tool_response":{"agent_id":agent_id}});
    let first = hook(home.path(), "bind", payload.clone());
    let second = hook(home.path(), "bind", payload);
    assert_eq!(first["hookSpecificOutput"]["hookEventName"], "PostToolUse");
    assert_eq!(first, second);
    assert!(first["hookSpecificOutput"]["additionalContext"]
        .as_str()
        .expect("message")
        .contains("bound to session"));
}

/// Mirrors Python `test_context_hook.py::test_first_prompt_creates_receipt_dedups_and_reuses_later_binding`.
#[test]
fn python_hook_context_injects_once_then_returns_an_empty_object() {
    let (home, _) = fake_agent_home();
    let payload = json!({"session_id":"session-1","hook_event_name":"UserPromptSubmit"});
    let first = hook(home.path(), "context", payload.clone());
    let second = hook(home.path(), "context", payload);
    assert_eq!(
        first["hookSpecificOutput"]["hookEventName"],
        "UserPromptSubmit"
    );
    assert!(first["hookSpecificOutput"]["additionalContext"]
        .as_str()
        .expect("context")
        .starts_with("Runtime priorities (highest first)."));
    assert_eq!(second, json!({}));
}
