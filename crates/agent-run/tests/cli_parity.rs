//! Frozen parser and process-boundary checks mirroring Python CLI tests.

use agent_run::{
    cli::{run, Cli},
    Error,
};
use clap::{Command, CommandFactory, Parser};
use serde_json::Value;
use std::{
    collections::BTreeSet,
    fs,
    io::Write,
    os::unix::fs::PermissionsExt,
    path::Path,
    process::{Command as ProcessCommand, Stdio},
};
use tempfile::tempdir;

/// A durable agent identity shaped like the Python tests' fixture id.
const AGENT_ID: &str = "ag-20260826-120000-0123456789";

/// Runs the built binary against one home and returns its completed output.
fn agent_run(home: &Path, args: &[&str]) -> std::process::Output {
    ProcessCommand::new(env!("CARGO_BIN_EXE_agent-run"))
        .args(["--home", home.to_str().expect("UTF-8 home")])
        .args(args)
        .output()
        .expect("agent-run binary executes")
}

/// Returns a resolved private home so rendered paths match asserted text.
fn resolved_home(temp: &tempfile::TempDir) -> std::path::PathBuf {
    temp.path().canonicalize().expect("resolved temporary home")
}

/// Asserts a launchd descriptor was rendered without touching durable state.
fn rendered_descriptor(home: &Path, output: &std::process::Output) -> Value {
    assert!(output.status.success(), "{output:?}");
    assert!(output.stderr.is_empty(), "{output:?}");
    assert!(
        !home.join("state.db").exists(),
        "rendering a launchd job must not create durable state"
    );
    serde_json::from_slice(&output.stdout).expect("JSON launchd descriptor")
}

/// Writes a fake provider CLI recording argv and credential-bearing variables.
///
/// The capture path is baked into the script because agent-run runs native
/// login with a cleared environment, so no variable can carry it in. Every
/// invocation after the first exits with `later`, which lets one script model
/// Python's "login succeeds, status fails" sequence.
fn fake_provider_cli(path: &Path, capture: &Path, later: i32) {
    let script = format!(
        "#!/bin/sh\nif [ -s '{capture}' ]; then first=no; else first=yes; fi\n\
         {{ printf 'ARGV'; for a in \"$@\"; do printf ' %s' \"$a\"; done; printf '\\n'; \
         printf 'CODEX_HOME=%s\\n' \"${{CODEX_HOME-unset}}\"; \
         printf 'CLAUDE_CONFIG_DIR=%s\\n' \"${{CLAUDE_CONFIG_DIR-unset}}\"; \
         printf 'CLAUDE_CODE_OAUTH_TOKEN=%s\\n' \"${{CLAUDE_CODE_OAUTH_TOKEN-unset}}\"; \
         }} >> '{capture}'\nif [ \"$first\" = yes ]; then exit 0; fi\nexit {later}\n",
        capture = capture.display(),
    );
    fs::write(path, script).expect("fake provider CLI writes");
    fs::set_permissions(path, fs::Permissions::from_mode(0o755)).expect("provider CLI is runnable");
}

/// Returns the recorded argv lines of a fake provider CLI capture.
fn captured_argv(capture: &str) -> Vec<&str> {
    capture
        .lines()
        .filter(|line| line.starts_with("ARGV"))
        .collect()
}

/// Returns the recorded values of one captured environment variable.
fn captured_env<'a>(capture: &'a str, name: &str) -> Vec<&'a str> {
    let prefix = format!("{name}=");
    capture
        .lines()
        .filter_map(|line| line.strip_prefix(prefix.as_str()))
        .collect()
}

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

/// Mirrors `test_cli.py::test_producer_shims_and_all_top_level_commands_parse`.
#[test]
fn python_cli_spec_command_surface_is_present() {
    let root = Cli::command();
    let rows = baseline();
    assert_eq!(rows.len(), 33, "the Python capture has 33 command records");
    for row in rows {
        let path = row["command"].as_str().expect("command name");
        let mut command = command_at(&root, path);
        command.build();
        let mut expected_options = row["actions"]
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
            // The Rust parser additionally carries the package version, which
            // the archived Python capture predates; the dedicated --version
            // test below pins that surface instead.
            .filter(|option| option != "--version" && option != "-V")
            .collect::<BTreeSet<_>>();
        // Deliberate schema-2 divergences from the Python capture: start names a
        // provider instead of a runtime, catalog reads take exact filters, and
        // stable agent operations can pin an exact historical execution.
        let (removed, added): (&[&str], &[&str]) = match path {
            "start" => (&["--runtime"], &["--provider"]),
            "models" => (&[], &["--provider", "--profile", "--model"]),
            "capacity order" => (&[], &["--model"]),
            "resume" | "cancel" | "steer" | "answer" | "transcript" | "bind"
            | "delivery status" => (&[], &["--run-id"]),
            _ => (&[], &[]),
        };
        for option in removed {
            assert!(expected_options.remove(*option), "{path} {option}");
        }
        expected_options.extend(added.iter().map(|option| (*option).to_owned()));
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

/// Mirrors `test_cli.py::test_one_shot_start_uses_resident_broker_without_ephemeral_runtime`.
#[tokio::test]
async fn python_start_refuses_a_missing_broker() {
    let home = tempdir().expect("temporary home");
    let cli = Cli::try_parse_from([
        "agent-run",
        "--home",
        home.path().to_str().expect("UTF-8 temporary path"),
        "start",
        "--provider",
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

/// Mirrors `test_cli.py::test_expected_errors_are_stable_json_but_unexpected_faults_propagate`.
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

/// Mirrors `test_cli.py::test_version_reports_the_workspace_package_version`.
///
/// The parser carries the package version metadata, so clap's DisplayVersion
/// handling in `main` prints it and exits successfully.
#[test]
fn python_cli_version_reports_the_package_version() {
    let error =
        Cli::try_parse_from(["agent-run", "--version"]).expect_err("--version is not a subcommand");
    assert_eq!(error.kind(), clap::error::ErrorKind::DisplayVersion);
    assert!(error.to_string().contains(env!("CARGO_PKG_VERSION")));
    let output = ProcessCommand::new(env!("CARGO_BIN_EXE_agent-run"))
        .arg("--version")
        .output()
        .expect("agent-run binary executes");
    assert_eq!(output.status.code(), Some(0));
    assert!(
        String::from_utf8_lossy(&output.stdout).contains(env!("CARGO_PKG_VERSION")),
        "printed version must include the package version"
    );
}

/// Mirrors `test_cli.py::test_init_bootstraps_private_minimal_home_without_credentials`.
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

/// Mirrors `test_cli.py::test_login_claude_defaults_global_and_rejects_unsupported_runtime`.
#[test]
fn python_login_rejects_codex_but_auth_executes_its_declared_account() {
    let home = tempdir().expect("temporary home");
    let home_text = home.path().to_str().expect("UTF-8 temporary path");
    let initialized = ProcessCommand::new(env!("CARGO_BIN_EXE_agent-run"))
        .args(["--home", home_text, "init"])
        .output()
        .expect("agent-run binary executes");
    assert!(initialized.status.success(), "{initialized:?}");
    std::fs::write(
        home.path().join("config.toml"),
        format!(
            "schema_version=1\n[runtimes.codex]\nenabled=true\nadapter='codex'\nbinary='/usr/bin/true'\nhome={}\nmodels=['fixture']\naccounts=['personal']\nlimits_source='none'\n",
            toml::Value::String(home.path().join("runtime").display().to_string())
        ),
    )
    .expect("fixture config writes");
    let login = ProcessCommand::new(env!("CARGO_BIN_EXE_agent-run"))
        .args(["--home", home_text, "login", "codex"])
        .output()
        .expect("agent-run binary executes");
    assert_eq!(login.status.code(), Some(2));
    assert!(
        String::from_utf8_lossy(&login.stderr).contains("agent-run auth <label> codex"),
        "{login:?}"
    );
    let auth = ProcessCommand::new(env!("CARGO_BIN_EXE_agent-run"))
        .args(["--home", home_text, "auth", "personal", "codex"])
        .output()
        .expect("agent-run binary executes");
    assert!(auth.status.success(), "{auth:?}");
    assert_eq!(
        serde_json::from_slice::<Value>(&auth.stdout).expect("JSON acknowledgement"),
        serde_json::json!({"account":"personal","runtime":"codex","status":"ok"})
    );
}

/// Mirrors `test_cli.py::test_removed_workflow_commands_are_not_parsed`.
#[test]
fn python_removed_workflow_commands_are_not_parsed() {
    for command in [
        vec!["workflow", "status", "wf_old"],
        vec!["batch", "--file", "-"],
        vec!["chain", AGENT_ID],
        vec!["status", AGENT_ID],
        vec!["summary"],
        vec!["wait", AGENT_ID],
        vec!["stats", "backfill"],
    ] {
        let argv = std::iter::once("agent-run").chain(command.iter().copied());
        assert!(
            Cli::try_parse_from(argv).is_err(),
            "the removed command {command:?} must not parse"
        );
    }
}

/// Mirrors `test_cli.py::test_mcp_command_is_reserved_without_importing_parallel_module`.
///
/// Python additionally proves that parsing does not import `agent_run.mcp`;
/// the Rust binary links its stdio transport statically, so the reserved
/// command name is the only live behavior left to protect here.
#[test]
fn python_mcp_command_is_reserved() {
    let parsed = Cli::try_parse_from(["agent-run", "mcp"]).expect("mcp stays a reserved command");
    assert!(matches!(parsed.command, agent_run::cli::Command::Mcp));
}

/// Returns the transport a parsed hook command carries.
fn hook_transport(cli: &Cli) -> &str {
    match &cli.command {
        agent_run::cli::Command::Hook { command } => match command {
            agent_run::cli::Hook::Context(hook) | agent_run::cli::Hook::Bind(hook) => {
                &hook.transport
            }
        },
        other => panic!("expected a hook command, got {other:?}"),
    }
}

/// Mirrors `test_cli.py::test_hook_transport_is_per_runtime_and_dispatch_routes_by_the_recorded_name`.
///
/// This ports the per-command transport contract: each hook defaults to the
/// Codex queue, accepts the Claude UDS transport, and refuses any other name.
/// The test's second half -- a drained delivery reaching the transport named
/// on the recorded session -- needs a populated durable store and is not
/// covered here.
#[test]
fn python_hook_transport_is_per_runtime() {
    for command in ["context", "bind"] {
        let parsed =
            Cli::try_parse_from(["agent-run", "hook", command]).expect("default transport parses");
        assert_eq!(hook_transport(&parsed), "codex_queue");
        let parsed =
            Cli::try_parse_from(["agent-run", "hook", command, "--transport", "claude_uds"])
                .expect("claude_uds parses");
        assert_eq!(hook_transport(&parsed), "claude_uds");
        assert!(
            Cli::try_parse_from(["agent-run", "hook", command, "--transport", "slack"]).is_err(),
            "an unknown transport must be refused for hook {command}"
        );
    }
}

/// Mirrors `test_cli.py::test_capacity_launchd_renders_config_without_state_or_collection`.
#[test]
fn python_capacity_launchd_renders_config_without_state_or_collection() {
    if !cfg!(target_os = "macos") {
        return;
    }
    let temp = tempdir().expect("temporary home");
    let home = resolved_home(&temp);
    let config = home.join("config.toml");
    fs::write(
        &config,
        "schema_version = 1\n[capacity]\ncollect_interval_seconds = 17\n",
    )
    .expect("capacity config writes");
    let binary = home.join("agent&<run>");
    let binary_text = binary.to_str().expect("UTF-8 binary path");
    let args = ["capacity", "launchd", "--binary", binary_text];

    let output = agent_run(&home, &args);
    let rendered = rendered_descriptor(&home, &output);
    assert_eq!(
        rendered
            .as_object()
            .expect("descriptor object")
            .keys()
            .map(String::as_str)
            .collect::<BTreeSet<_>>(),
        BTreeSet::from(["argv", "interval_seconds", "label", "plist"])
    );
    assert_eq!(rendered["label"], "com.pluto.agent-run.capacity");
    assert_eq!(rendered["interval_seconds"], 17);
    assert_eq!(
        rendered["argv"],
        serde_json::json!([binary_text, "capacity", "collect", "--once"])
    );
    let plist = rendered["plist"].as_str().expect("plist text");
    assert!(
        plist.contains(&format!(
            "<string>{}/agent&amp;&lt;run&gt;</string>",
            home.display()
        )),
        "{plist}"
    );
    assert!(
        plist.contains("<key>StartInterval</key><integer>17</integer>"),
        "{plist}"
    );
    assert!(plist.contains("<key>RunAtLoad</key><false/>"), "{plist}");
    assert!(!plist.contains("<key>KeepAlive</key>"), "{plist}");
    assert!(
        plist.contains("<key>StandardOutPath</key><string>/dev/null</string>"),
        "{plist}"
    );
    assert!(
        plist.contains(&format!(
            "<key>StandardErrorPath</key><string>{}</string>",
            home.join("capacity-worker.err.log").display()
        )),
        "{plist}"
    );

    fs::write(
        &config,
        "schema_version = 1\n[capacity]\ncollect_interval_seconds = 19\n",
    )
    .expect("capacity config rewrites");
    let stdout_log = home.join("capacity<&out.log");
    let stderr_log = home.join("capacity&err.log");
    let output = agent_run(
        &home,
        &[
            "capacity",
            "launchd",
            "--binary",
            binary_text,
            "--label",
            "com.example.<capacity&>",
            "--stdout-log",
            stdout_log.to_str().expect("UTF-8 stdout log"),
            "--stderr-log",
            stderr_log.to_str().expect("UTF-8 stderr log"),
        ],
    );
    let rendered = rendered_descriptor(&home, &output);
    assert_eq!(rendered["interval_seconds"], 19);
    let plist = rendered["plist"].as_str().expect("plist text");
    assert!(
        plist.contains("<key>Label</key><string>com.example.&lt;capacity&amp;&gt;</string>"),
        "{plist}"
    );
    assert!(
        plist.contains(&format!(
            "<key>StandardOutPath</key><string>{}/capacity&lt;&amp;out.log</string>",
            home.display()
        )),
        "{plist}"
    );
    assert!(
        plist.contains(&format!(
            "<key>StandardErrorPath</key><string>{}/capacity&amp;err.log</string>",
            home.display()
        )),
        "{plist}"
    );
    assert!(!plist.contains("<key>KeepAlive</key>"), "{plist}");

    let relative = agent_run(&home, &["capacity", "launchd", "--binary", "agent-run"]);
    assert_eq!(relative.status.code(), Some(2));
    assert!(relative.stdout.is_empty(), "{relative:?}");
    assert_eq!(
        serde_json::from_slice::<Value>(&relative.stderr).expect("JSON error envelope")["error"]
            ["type"],
        "ValidationError"
    );
}

/// Mirrors `test_cli.py::test_delivery_launchd_renders_a_durable_bounded_sweeper`.
#[test]
fn python_delivery_launchd_renders_a_durable_bounded_sweeper() {
    if !cfg!(target_os = "macos") {
        return;
    }
    let temp = tempdir().expect("temporary home");
    let home = resolved_home(&temp);
    fs::write(
        home.join("config.toml"),
        format!(
            "schema_version = 1\n[delivery]\nretry_base_seconds = 2.1\ncodex_queue_bin = {}\n",
            toml::Value::String(home.join("queue").display().to_string())
        ),
    )
    .expect("delivery config writes");
    let binary = home.join("agent-run");
    let binary_text = binary.to_str().expect("UTF-8 binary path");

    let output = agent_run(&home, &["delivery", "launchd", "--binary", binary_text]);
    let rendered = rendered_descriptor(&home, &output);
    assert_eq!(rendered["interval_seconds"], 3);
    assert_eq!(
        rendered["argv"],
        serde_json::json!([
            binary_text,
            "--home",
            home.to_str().expect("UTF-8 home"),
            "delivery",
            "dispatch"
        ])
    );
    let plist = rendered["plist"].as_str().expect("plist text");
    assert!(
        plist.contains("<key>StartInterval</key><integer>3</integer>"),
        "{plist}"
    );
    assert!(plist.contains("<key>RunAtLoad</key><false/>"), "{plist}");
    assert!(!plist.contains("<key>KeepAlive</key>"), "{plist}");
}

/// Mirrors `test_cli.py::test_api_launchd_renders_a_keep_alive_resident_daemon`.
#[test]
fn python_api_launchd_renders_a_keep_alive_resident_daemon() {
    if !cfg!(target_os = "macos") {
        return;
    }
    let temp = tempdir().expect("temporary home");
    let home = resolved_home(&temp);
    let binary = home.join("agent-run");
    let binary_text = binary.to_str().expect("UTF-8 binary path");

    let output = agent_run(&home, &["api", "launchd", "--binary", binary_text]);
    let rendered = rendered_descriptor(&home, &output);
    assert_eq!(rendered["label"], "com.agent-run.api");
    assert_eq!(
        rendered["argv"],
        serde_json::json!([
            binary_text,
            "--home",
            home.to_str().expect("UTF-8 home"),
            "api",
            "serve"
        ])
    );
    let plist = rendered["plist"].as_str().expect("plist text");
    assert!(plist.contains("<key>RunAtLoad</key><true/>"), "{plist}");
    assert!(plist.contains("<key>KeepAlive</key><true/>"), "{plist}");
    assert!(
        plist.contains(
            "<key>SoftResourceLimits</key><dict><key>NumberOfFiles</key><integer>65536</integer></dict>"
        ),
        "{plist}"
    );
    assert!(
        plist.contains(&format!(
            "<key>StandardOutPath</key><string>{}</string>",
            home.join("logs/api.log").display()
        )),
        "{plist}"
    );
    assert!(
        plist.contains(&format!(
            "<key>StandardErrorPath</key><string>{}</string>",
            home.join("logs/api.err.log").display()
        )),
        "{plist}"
    );
}

/// Mirrors `test_cli.py::test_auth_runs_codex_login_in_account_home`.
#[test]
fn python_auth_runs_codex_login_in_account_home() {
    let temp = tempdir().expect("temporary home");
    let home = resolved_home(&temp);
    let capture = home.join("capture.log");
    let binary = home.join("fake-codex");
    fake_provider_cli(&binary, &capture, 0);
    fs::write(
        home.join("config.toml"),
        format!(
            "schema_version = 1\n[runtimes.codex]\nenabled = true\nadapter = 'codex'\nbinary = {binary}\nhome = {runtime_home}\nmodels = ['model']\naccounts = ['personal2']\nlimits_source = 'none'\n",
            binary = toml::Value::String(binary.display().to_string()),
            runtime_home = toml::Value::String(home.join("runtime-codex").display().to_string()),
        ),
    )
    .expect("codex config writes");

    let output = agent_run(&home, &["auth", "personal2", "codex"]);
    assert!(output.status.success(), "{output:?}");
    assert_eq!(
        serde_json::from_slice::<Value>(&output.stdout).expect("JSON acknowledgement"),
        serde_json::json!({"account":"personal2","runtime":"codex","status":"ok"})
    );
    let captured = fs::read_to_string(&capture).expect("provider capture");
    assert_eq!(
        captured_argv(&captured),
        ["ARGV login", "ARGV login status"]
    );
    let expected = home.join("accounts/codex/personal2").display().to_string();
    assert_eq!(
        captured_env(&captured, "CODEX_HOME"),
        [expected.as_str(), expected.as_str()]
    );
}

/// Mirrors `test_cli.py::test_login_claude_uses_the_native_global_config_directory`.
#[test]
fn python_login_claude_uses_the_native_global_config_directory() {
    let temp = tempdir().expect("temporary home");
    let home = resolved_home(&temp);
    let capture = home.join("capture.log");
    let binary = home.join("fake-claude");
    fake_provider_cli(&binary, &capture, 0);
    fs::write(
        home.join("config.toml"),
        format!(
            "schema_version = 1\n[runtimes.claude]\nenabled = true\nadapter = 'claude'\nbinary = {binary}\nhome = {runtime_home}\nmodels = ['sonnet']\n[runtimes.claude.auth]\nkind = 'environment'\nnames = ['CLAUDE_CODE_OAUTH_TOKEN']\n",
            binary = toml::Value::String(binary.display().to_string()),
            runtime_home = toml::Value::String(home.join("runtime-claude").display().to_string()),
        ),
    )
    .expect("claude config writes");

    let output = ProcessCommand::new(env!("CARGO_BIN_EXE_agent-run"))
        .args([
            "--home",
            home.to_str().expect("UTF-8 home"),
            "login",
            "claude",
        ])
        .env("CLAUDE_CONFIG_DIR", "/ambient")
        .env("CLAUDE_CODE_OAUTH_TOKEN", "secret")
        .output()
        .expect("agent-run binary executes");
    assert!(output.status.success(), "{output:?}");
    assert_eq!(
        serde_json::from_slice::<Value>(&output.stdout).expect("JSON acknowledgement"),
        serde_json::json!({"account":null,"runtime":"claude","status":"ok"})
    );
    let captured = fs::read_to_string(&capture).expect("provider capture");
    assert_eq!(
        captured_argv(&captured),
        ["ARGV auth login", "ARGV auth status --json"]
    );
    assert_eq!(
        captured_env(&captured, "CLAUDE_CONFIG_DIR"),
        ["/ambient", "/ambient"]
    );
    assert_eq!(
        captured_env(&captured, "CLAUDE_CODE_OAUTH_TOKEN"),
        ["unset", "unset"],
        "declared credential variables must not reach the login child"
    );
}

/// Mirrors `test_cli.py::test_login_claude_account_and_status_failure_are_scoped_and_safe`.
#[test]
fn python_login_claude_account_and_status_failure_are_scoped_and_safe() {
    let temp = tempdir().expect("temporary home");
    let home = resolved_home(&temp);
    let capture = home.join("capture.log");
    let binary = home.join("fake-claude");
    fake_provider_cli(&binary, &capture, 17);
    let runtime_home = home.join("runtime-claude");
    fs::write(
        home.join("config.toml"),
        format!(
            "schema_version = 1\n[runtimes.claude]\nenabled = true\nadapter = 'claude'\nbinary = {binary}\nhome = {runtime_home}\nmodels = ['sonnet']\naccounts = ['personal']\n[runtimes.claude.auth]\nkind = 'environment'\nnames = ['CLAUDE_CODE_OAUTH_TOKEN']\n",
            binary = toml::Value::String(binary.display().to_string()),
            runtime_home = toml::Value::String(runtime_home.display().to_string()),
        ),
    )
    .expect("claude config writes");

    let output = agent_run(&home, &["login", "claude", "--account", "personal"]);
    assert_eq!(output.status.code(), Some(17), "{output:?}");
    assert!(output.stdout.is_empty(), "{output:?}");
    assert_eq!(
        String::from_utf8_lossy(&output.stderr),
        "auth login status failed for personal claude (exit 17)\n"
    );
    let captured = fs::read_to_string(&capture).expect("provider capture");
    // Deliberate divergence from Python: a fresh labelled login lands in the
    // canonical account home that runs and quota collection also read.
    let expected = home
        .join("accounts/claude/personal/claude-config")
        .display()
        .to_string();
    assert_eq!(
        captured_env(&captured, "CLAUDE_CONFIG_DIR"),
        [expected.as_str(), expected.as_str()],
        "a selected account gets its own scoped Claude config directory"
    );
}
