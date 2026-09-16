//! Frozen parser and process-boundary checks mirroring Python CLI tests.

use agent_run::{
    cli::{run, Cli},
    Error,
};
use clap::{Command, CommandFactory, Parser};
use serde_json::Value;
use std::{
    collections::BTreeSet,
    io::Write,
    process::{Command as ProcessCommand, Stdio},
};
use tempfile::tempdir;

/// Returns the parser command at one space-separated baseline command path.
fn command_at(root: &Command, path: &str) -> Command {
    path.split_whitespace()
        .filter(|part| *part != "agent-run")
        .fold(root.clone(), |command, part| {
            command
                .get_subcommands()
                .find(|child| child.get_name() == part)
                .unwrap_or_else(|| panic!("missing command path {path}"))
                .clone()
        })
}

/// Loads the captured Python parser metadata as JSON without scraping help text.
fn baseline() -> Vec<Value> {
    serde_json::from_str(include_str!(
        "../../../tests/fixtures/baseline/cli-spec.json"
    ))
    .expect("checked-in Python CLI capture is valid JSON")
}

/// Locates a baseline action's clap argument, excluding subcommand selectors.
fn argument_for<'a>(command: &'a Command, action: &Value) -> Option<&'a clap::Arg> {
    let destination = action["dest"].as_str()?;
    if destination == "command" || destination.ends_with("_command") {
        return None;
    }
    if action["option_strings"]
        .as_array()
        .is_some_and(|options| options.is_empty())
    {
        return command
            .get_positionals()
            .find(|argument| argument.get_id().as_str() == destination);
    }
    command
        .get_arguments()
        .find(|argument| argument.get_id().as_str() == destination)
}

/// Mirrors Python parser coverage by requiring every captured option and positional.
#[test]
fn python_cli_spec_command_surface_is_present() {
    let root = Cli::command();
    let rows = baseline();
    assert_eq!(rows.len(), 33, "the Python capture has 33 command records");
    for row in rows {
        let path = row["command"].as_str().expect("command name");
        let mut command = command_at(&root, path);
        command.build();
        let expected_options = row["actions"]
            .as_array()
            .expect("actions")
            .iter()
            .filter_map(|action| action["option_strings"].as_array())
            .flat_map(|options| options.iter().filter_map(Value::as_str).map(str::to_owned))
            .collect::<BTreeSet<_>>();
        let actual_options = command
            .get_arguments()
            .flat_map(|argument| {
                [
                    argument.get_short().map(|value| format!("-{value}")),
                    argument.get_long().map(|value| format!("--{value}")),
                ]
                .into_iter()
                .flatten()
            })
            .collect::<BTreeSet<_>>();
        assert_eq!(actual_options, expected_options, "options for {path}");
        for action in row["actions"].as_array().expect("actions") {
            if action["option_strings"]
                .as_array()
                .is_some_and(|options| options.is_empty())
            {
                let destination = action["dest"].as_str().expect("destination");
                if destination == "command" || destination.ends_with("_command") {
                    continue;
                }
                assert!(
                    command
                        .get_positionals()
                        .any(|argument| argument.get_id().as_str() == destination),
                    "missing positional {destination} for {path}"
                );
            }
            let Some(argument) = argument_for(&command, action) else {
                continue;
            };
            assert_eq!(
                argument.is_required_set(),
                action["required"].as_bool().expect("required marker"),
                "required marker for {} in {path}",
                action["dest"].as_str().expect("destination")
            );
            let expected_default = &action["default"];
            if expected_default != "==SUPPRESS==" && expected_default != "${WORKTREE}" {
                let actual_defaults = argument
                    .get_default_values()
                    .iter()
                    .map(|value| value.to_string_lossy().into_owned())
                    .collect::<Vec<_>>();
                let expected_defaults = match expected_default {
                    Value::Null => Vec::new(),
                    Value::Array(values) if values.is_empty() => Vec::new(),
                    Value::String(value) => vec![value.clone()],
                    Value::Bool(value) => vec![value.to_string()],
                    Value::Number(value) => vec![value.to_string()],
                    other => panic!("unexpected Python default {other} for {path}"),
                };
                assert_eq!(
                    actual_defaults,
                    expected_defaults,
                    "default for {} in {path}",
                    action["dest"].as_str().expect("destination")
                );
            }
            if let Some(expected_choices) = action["choices"].as_array() {
                let actual_choices = argument
                    .get_possible_values()
                    .iter()
                    .map(|value| value.get_name().to_owned())
                    .collect::<Vec<_>>();
                let expected_choices = expected_choices
                    .iter()
                    .filter_map(Value::as_str)
                    .map(str::to_owned)
                    .collect::<Vec<_>>();
                assert_eq!(
                    actual_choices,
                    expected_choices,
                    "choices for {} in {path}",
                    action["dest"].as_str().expect("destination")
                );
            }
        }
    }
}

/// Mirrors `test_one_shot_start_uses_resident_broker_without_ephemeral_runtime`.
#[tokio::test]
async fn python_start_refuses_a_missing_broker() {
    let home = tempdir().expect("temporary home");
    let cli = Cli::try_parse_from([
        "agent-run",
        "--home",
        home.path().to_str().expect("UTF-8 temporary path"),
        "start",
        "--runtime",
        "codex",
        "--model",
        "model",
        "--profile",
        "review",
        "--task",
        "do work",
    ])
    .expect("valid Python-compatible start arguments");
    assert!(matches!(run(cli).await, Err(Error::BrokerUnavailable)));
}

/// Mirrors Python CLI validation tests: failures are JSON with the `error.type` field.
#[test]
fn python_cli_validation_error_uses_the_expected_json_field_names() {
    let output = ProcessCommand::new(env!("CARGO_BIN_EXE_agent-run"))
        .arg("start")
        .output()
        .expect("agent-run binary executes");
    assert_eq!(output.status.code(), Some(2));
    let value: Value = serde_json::from_slice(&output.stderr).expect("JSON error on stderr");
    assert_eq!(value["error"]["type"], "ValidationError");
    assert!(value["error"]["message"].is_string());
}

/// Mirrors the Python `init` CLI shape with paths rendered as JSON strings.
#[test]
fn python_init_emits_home_config_and_state_fields() {
    let home = tempdir().expect("temporary home");
    let output = ProcessCommand::new(env!("CARGO_BIN_EXE_agent-run"))
        .args([
            "--home",
            home.path().to_str().expect("UTF-8 temporary path"),
            "init",
        ])
        .output()
        .expect("agent-run binary executes");
    assert!(output.status.success(), "{output:?}");
    let value: Value = serde_json::from_slice(&output.stdout).expect("JSON result");
    for field in ["home", "config", "state"] {
        assert!(value[field].is_string(), "missing JSON string {field}");
    }
}

/// Mirrors `test_cli.py::test_producer_shims_and_all_top_level_commands_parse`.
#[test]
fn python_delivery_cancel_returns_a_durable_acknowledgement() {
    let home = tempdir().expect("temporary home");
    let initialized = ProcessCommand::new(env!("CARGO_BIN_EXE_agent-run"))
        .args([
            "--home",
            home.path().to_str().expect("UTF-8 temporary path"),
            "init",
        ])
        .output()
        .expect("agent-run binary executes");
    assert!(initialized.status.success(), "{initialized:?}");
    let output = ProcessCommand::new(env!("CARGO_BIN_EXE_agent-run"))
        .args([
            "--home",
            home.path().to_str().expect("UTF-8 temporary path"),
            "delivery",
            "cancel",
            "missing-delivery",
        ])
        .output()
        .expect("agent-run binary executes");
    assert!(output.status.success(), "{output:?}");
    assert_eq!(
        serde_json::from_slice::<Value>(&output.stdout).expect("JSON acknowledgement"),
        serde_json::json!({"delivery_id":"missing-delivery","cancelled":false})
    );
}

/// Mirrors `test_codex_permission_request.py::test_main_emits_allow_only_for_a_trusted_mcp`.
#[test]
fn python_permission_request_allows_only_proven_trusted_mcp_calls() {
    let mut allowed = ProcessCommand::new(env!("CARGO_BIN_EXE_agent-run"))
        .args(["_permission-request", "--allow-mcp", "agent-run"])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .spawn()
        .expect("agent-run permission helper executes");
    allowed
        .stdin
        .take()
        .expect("permission helper stdin")
        .write_all(
            br#"{"hook_event_name":"PermissionRequest","tool_name":"mcp__agent_run__answer"}"#,
        )
        .expect("permission request input writes");
    let allowed = allowed
        .wait_with_output()
        .expect("permission helper completes");
    assert_eq!(allowed.status.code(), Some(0));
    assert_eq!(
        serde_json::from_slice::<Value>(&allowed.stdout).expect("allow JSON"),
        serde_json::json!({"hookSpecificOutput":{"hookEventName":"PermissionRequest","decision":{"behavior":"allow"}}})
    );

    let mut refused = ProcessCommand::new(env!("CARGO_BIN_EXE_agent-run"))
        .args(["_permission-request", "--allow-mcp", "agent-run"])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .spawn()
        .expect("agent-run permission helper executes");
    refused
        .stdin
        .take()
        .expect("permission helper stdin")
        .write_all(br#"{"hook_event_name":"PermissionRequest","tool_name":"Bash"}"#)
        .expect("permission request input writes");
    let refused = refused
        .wait_with_output()
        .expect("permission helper completes");
    assert_eq!(refused.status.code(), Some(0));
    assert!(refused.stdout.is_empty(), "unproven requests stay silent");
}
