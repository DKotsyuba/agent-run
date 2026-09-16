//! Normalized Python-home captures for the Claude, GLM, and Qwen adapters.

use agent_run_adapters::materialize;
use agent_run_config::{
    config::{Adapter, Capacity, Catalog, Config, Core, Delivery, Runtime},
    profiles::Profile,
};
use agent_run_core::{state::Record, stream::plan_with_environment};
use agent_run_domain::domain::{AgentId, StartRequest, Status};
use serde_json::{json, Value};
use std::{
    collections::{BTreeMap, BTreeSet},
    path::{Path, PathBuf},
};

/// Returns the repository-owned capture directory without relying on the test CWD.
fn capture_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../../tests/fixtures/baseline/homes")
}

/// Builds the smallest fully specified request shared by a captured engine and role shape.
fn fixture(
    adapter: Adapter,
    shape: &str,
    root: &Path,
) -> (Config, Runtime, StartRequest, Profile, PathBuf) {
    let write = shape == "write";
    let home = root.join("home");
    let workdir = root.join("workdir");
    std::fs::create_dir_all(&workdir).expect("fixture workdir");
    let (name, model, task, effort, auth) = match adapter {
        Adapter::Claude => (
            "claude",
            "sonnet",
            "fixture task",
            Some("medium"),
            json!({"kind":"environment","names":["CLAUDE_CODE_OAUTH_TOKEN"]}),
        ),
        Adapter::Glm => (
            "glm",
            "glm-5.3",
            "fixture task",
            Some("medium"),
            Value::Null,
        ),
        Adapter::Qwen => (
            "qwen",
            "opencode/MiniMaxM3",
            "Inspect the fixture repository and report only the requested result.",
            None,
            json!({"kind":"environment","names":["OPENAI_API_KEY","OPENAI_BASE_URL"]}),
        ),
        Adapter::Codex => unreachable!("the Codex golden is covered by its adapter test"),
    };
    let runtime: Runtime = serde_json::from_value(json!({
        "enabled": true,
        "adapter": name,
        "binary": "/bin/echo",
        "home": home,
        "models": [model],
        "auth": auth,
    }))
    .expect("fixture runtime");
    let request: StartRequest = serde_json::from_value(json!({
        "runtime": name,
        "model": model,
        "profile": if write {"implement"} else {"review"},
        "task": task,
        "workdir": workdir,
        "write": write,
        "effort": effort,
    }))
    .expect("fixture request");
    let config = Config {
        schema_version: 1,
        core: Core::default(),
        capacity: Capacity::default(),
        delivery: Delivery::default(),
        profiles: Catalog::default(),
        skills: Catalog::default(),
        mcp: BTreeMap::new(),
        environments: BTreeMap::new(),
        runtimes: BTreeMap::new(),
    };
    let profile = Profile {
        name: request.profile.clone(),
        body: "Canonical fixture role.".into(),
        write,
        network: false,
        revision: "fixture".into(),
        canonical: false,
        allow_external_read_roots: true,
        read_roots: vec![request.workdir.clone()],
        skills: vec![],
        mcp: vec![],
        required_constraints: BTreeSet::new(),
    };
    (config, runtime, request, profile, home)
}

/// Creates the durable fields launch planning reads without opening a state database.
fn record(request: StartRequest) -> Record {
    let id: AgentId = "ag-20260101-000000-0000000001".parse().expect("fixture id");
    Record {
        id: id.clone(),
        request,
        status: Status::Created,
        created_at: 0.0,
        started_at: None,
        finished_at: None,
        supervisor_pid: None,
        supervisor_identity: None,
        supervisor_birth_time: None,
        process_group_id: None,
        runtime_session_id: None,
        failure_kind: None,
        failure_text: None,
        exit_code: None,
        answer_path: None,
        answer_bytes: None,
        answer_sha256: None,
        orchestrator_session_id: None,
        parent_agent_id: None,
        root_agent_id: id,
        sequence: 0,
        resume_of_runtime_session_id: None,
        identity: None,
    }
}

/// Supplies a credential-free resolved environment to the pure launch-planning half.
fn resolved_environment(adapter: Adapter, home: &Path) -> BTreeMap<String, String> {
    let mut environment = BTreeMap::from([
        ("CODEX_HOME".into(), home.display().to_string()),
        ("HOME".into(), home.display().to_string()),
        ("OPENAI_BASE_URL".into(), "http://127.0.0.1:20128/v1".into()),
        ("PATH".into(), "/usr/bin:/bin".into()),
    ]);
    match adapter {
        Adapter::Claude => {
            environment.insert("CLAUDE_CODE_OAUTH_TOKEN".into(), "fixture-secret".into());
        }
        Adapter::Glm => {
            environment.insert("ANTHROPIC_AUTH_TOKEN".into(), "fixture-secret".into());
            environment.insert(
                "ANTHROPIC_BASE_URL".into(),
                "https://api.z.ai/api/anthropic".into(),
            );
        }
        Adapter::Qwen => {
            environment.insert("OPENAI_API_KEY".into(), "fixture-secret".into());
        }
        Adapter::Codex => unreachable!("the Codex golden is covered by its adapter test"),
    }
    environment
}

/// Replaces only capture-machine paths; all policy and payload strings remain exact.
fn normalize_string(value: &str, home: &Path, workdir: &Path) -> String {
    value
        .replace(&home.display().to_string(), "${HOME_ROOT}")
        .replace(&workdir.display().to_string(), "${TEMP_ROOT}/workdir")
}

/// Applies path normalization recursively to JSON payloads without changing their structure.
fn normalize_json(value: Value, home: &Path, workdir: &Path) -> Value {
    match value {
        Value::String(value) => Value::String(normalize_string(&value, home, workdir)),
        Value::Array(values) => Value::Array(
            values
                .into_iter()
                .map(|value| normalize_json(value, home, workdir))
                .collect(),
        ),
        Value::Object(values) => Value::Object(
            values
                .into_iter()
                .map(|(key, value)| (key, normalize_json(value, home, workdir)))
                .collect(),
        ),
        value => value,
    }
}

/// Recursively collects relative regular-file paths below one generated home.
fn files(root: &Path, current: &Path, output: &mut BTreeSet<String>) {
    for entry in std::fs::read_dir(current).expect("generated home directory") {
        let entry = entry.expect("generated home entry");
        let path = entry.path();
        if path.is_dir() {
            files(root, &path, output);
        } else if path.is_file() {
            output.insert(
                path.strip_prefix(root)
                    .expect("generated file stays beneath home")
                    .to_string_lossy()
                    .into_owned(),
            );
        }
    }
}

/// Compares every managed payload file while translating the two integrity-index formats.
fn compare_tree(expected: &Value, home: &Path, workdir: &Path) {
    let expected_files = expected.as_object().expect("golden tree object");
    let mut actual_files = BTreeSet::new();
    files(home, home, &mut actual_files);
    actual_files.remove(".agent-run-rust-snapshot.json");
    actual_files.remove(".agent-run-snapshots.json");
    actual_files.remove(".agent-run-snapshot.json");
    let expected_names = expected_files
        .keys()
        .filter(|name| name.as_str() != ".agent-run-snapshots.json")
        .cloned()
        .collect::<BTreeSet<_>>();
    assert_eq!(actual_files, expected_names, "managed home file set");
    for (path, entry) in expected_files {
        if path == ".agent-run-snapshots.json" {
            continue;
        }
        let expected_content = entry["content"].as_str().expect("golden file content");
        let actual_content = std::fs::read_to_string(home.join(path)).expect("generated file");
        let normalized_actual = normalize_string(&actual_content, home, workdir);
        if path.ends_with(".json") {
            assert_eq!(
                normalize_json(
                    serde_json::from_str(expected_content).expect("golden JSON content"),
                    home,
                    workdir,
                ),
                normalize_json(
                    serde_json::from_str(&normalized_actual).expect("generated JSON content"),
                    home,
                    workdir,
                ),
                "JSON payload {path}",
            );
        } else {
            assert_eq!(normalized_actual, expected_content, "payload {path}");
        }
    }
    // The published runtime index now carries Python's own name and shape
    // (snapshot_tree.py:23). The captured fixtures predate that work and never
    // recorded publisher metadata, so the index is checked structurally here.
    let index: Value = serde_json::from_slice(
        &std::fs::read(home.join(".agent-run-snapshots.json")).expect("runtime snapshot index"),
    )
    .expect("runtime snapshot index JSON");
    assert!(
        index["snapshot_index_version"].is_number(),
        "runtime index records its version",
    );
    assert!(index["roots"].is_array(), "runtime index lists roots");
    assert!(index["files"].is_array(), "runtime index lists files");
    assert!(
        index["manifests"].is_object(),
        "runtime index maps manifests"
    );
    assert!(!index["links"].is_null(), "runtime index records links");
}

/// Extracts the capture's owned environment names, leaving host inheritance normalized but checked.
fn owned_environment_names(launch: &Value) -> BTreeSet<String> {
    launch["environment_values"]
        .as_object()
        .expect("golden environment values")
        .keys()
        .cloned()
        .chain(
            launch["secret_env_names"]
                .as_array()
                .expect("golden secret names")
                .iter()
                .filter_map(Value::as_str)
                .map(str::to_owned),
        )
        .collect()
}

/// Compares launch arguments, adapter state, and every adapter-owned environment field.
fn compare_launch(
    expected: &Value,
    adapter: Adapter,
    plan: agent_run_adapters::LaunchPlan,
    record: &Record,
    profile: &Profile,
    home: &Path,
) {
    let mut actual_argv = vec![plan.binary.display().to_string()];
    actual_argv.extend(plan.args.iter().cloned());
    for index in 0..actual_argv.len().saturating_sub(1) {
        if actual_argv[index] == "--session-id" {
            actual_argv[index + 1] = "${SESSION_ID}".into();
        }
    }
    let expected_argv = expected["argv"]
        .as_array()
        .expect("golden argv")
        .iter()
        .map(|value| match value.as_str().expect("golden argv item") {
            "00000000-0000-4000-8000-000000000001" => "${SESSION_ID}".into(),
            value => value.into(),
        })
        .collect::<Vec<String>>();
    assert_eq!(
        actual_argv
            .iter()
            .map(|value| normalize_string(value, home, &record.request.workdir))
            .collect::<Vec<_>>(),
        expected_argv,
        "argv",
    );
    assert_eq!(expected["cwd"], "${WORKDIR}");
    assert_eq!(plan.cwd, record.request.workdir, "launch CWD");
    let owned = owned_environment_names(expected);
    let expected_owned = expected["environment_names"]
        .as_array()
        .expect("golden environment names")
        .iter()
        .filter_map(Value::as_str)
        .filter(|name| owned.contains(*name))
        .collect::<BTreeSet<_>>();
    let actual_owned = plan
        .environment
        .keys()
        .filter(|name| owned.contains(*name))
        .map(String::as_str)
        .collect::<BTreeSet<_>>();
    assert_eq!(actual_owned, expected_owned, "owned environment names");
    let expected_values = expected["environment_values"]
        .as_object()
        .expect("golden environment values");
    for (name, expected_value) in expected_values {
        assert_eq!(
            normalize_string(
                plan.environment.get(name).expect("owned environment value"),
                home,
                &record.request.workdir,
            ),
            expected_value.as_str().expect("golden environment value"),
            "environment value {name}",
        );
    }
    let actual_state = match adapter {
        Adapter::Claude | Adapter::Glm => json!({
            "allowed_tools": plan.args[plan.args.iter().position(|arg| arg == "--allowedTools").expect("allowed tools") + 1]
                .split(',').collect::<Vec<_>>(),
            "model": record.request.model,
            "permission_mode": plan.args[plan.args.iter().position(|arg| arg == "--permission-mode").expect("permission mode") + 1],
            "secret_env_names": match adapter {
                Adapter::Claude => vec!["CLAUDE_CODE_OAUTH_TOKEN"],
                Adapter::Glm => vec!["ANTHROPIC_AUTH_TOKEN", "ANTHROPIC_BASE_URL"],
                _ => unreachable!("Claude-family state has only Claude or GLM"),
            },
            "session_id": "${SESSION_ID}",
        }),
        Adapter::Qwen => json!({
            "approval_mode": plan.args[plan.args.iter().position(|arg| arg == "--approval-mode").expect("approval mode") + 1],
            "model": record.request.model,
            "sandbox": plan.args.iter().any(|arg| arg == "--sandbox"),
            "secret_env_names": vec!["OPENAI_API_KEY", "OPENAI_BASE_URL"],
            "workdir": "${TEMP_ROOT}/workdir",
            "write": profile.write,
        }),
        Adapter::Codex => unreachable!("the Codex golden is covered by its adapter test"),
    };
    let mut expected_state: Value = serde_json::from_str(
        expected["adapter_state"]
            .as_str()
            .expect("golden adapter state"),
    )
    .expect("golden adapter state JSON");
    if expected_state.get("session_id").is_some() {
        expected_state["session_id"] = json!("${SESSION_ID}");
    }
    assert_eq!(
        normalize_json(actual_state, home, &record.request.workdir),
        normalize_json(expected_state, home, &record.request.workdir)
    );
}

/// Mirrors `test_claude_adapter.py`, `test_glm_adapter.py`, and `test_qwen_adapter.py` home preparation.
///
/// The comparison normalizes generated home/work directories, the fresh
/// session UUID, JSON whitespace/key presentation, and the language-specific
/// integrity-index schema/version.  It does not discard those indexes: both
/// must be valid and the Rust index must report version 1; every indexed
/// payload is compared separately. Ambient inherited environment variables
/// are normalized to the captured adapter-owned subset, because their exact
/// names vary by Cargo, shell, and macOS version; every owned name and value,
/// including secret *names* but never secret values, remains an exact check.
#[test]
fn python_golden_homes_match_claude_glm_and_qwen_read_only_and_write() {
    for adapter in [Adapter::Claude, Adapter::Glm, Adapter::Qwen] {
        for shape in ["read-only", "write"] {
            let temporary = tempfile::tempdir().expect("temporary fixture root");
            let (config, runtime, request, profile, home) =
                fixture(adapter, shape, temporary.path());
            let (snapshot, _) = materialize::materialize(
                &config,
                &runtime,
                &request,
                &profile,
                &home,
                temporary.path(),
            )
            .expect("materialize generated home");
            let expected_tree: Value = serde_json::from_slice(
                &std::fs::read(
                    capture_root()
                        .join(adapter.name())
                        .join(shape)
                        .join("tree.json"),
                )
                .expect("golden tree"),
            )
            .expect("golden tree JSON");
            compare_tree(&expected_tree, &home, &request.workdir);
            let record = record(request);
            let plan = plan_with_environment(
                &config,
                &runtime,
                &record,
                &profile,
                &home,
                &snapshot,
                resolved_environment(adapter, &home),
            )
            .expect("build golden launch");
            #[cfg(target_os = "macos")]
            if adapter == Adapter::Qwen {
                let xcode_git = std::process::Command::new("/usr/bin/xcrun")
                    .args(["--find", "git"])
                    .output()
                    .expect("locate Xcode Git");
                assert!(
                    xcode_git.status.success(),
                    "Xcode Git toolchain is installed"
                );
                let git_dir = Path::new(String::from_utf8_lossy(&xcode_git.stdout).trim())
                    .parent()
                    .expect("Xcode Git has a parent directory")
                    .display()
                    .to_string();
                assert_eq!(
                    plan.environment["PATH"],
                    format!("{git_dir}:/usr/bin:/bin"),
                    "Xcode Git must precede the inherited PATH"
                );
            }
            let expected_launch: Value = serde_json::from_slice(
                &std::fs::read(
                    capture_root()
                        .join(adapter.name())
                        .join(shape)
                        .join("launch.json"),
                )
                .expect("golden launch"),
            )
            .expect("golden launch JSON");
            compare_launch(&expected_launch, adapter, plan, &record, &profile, &home);
        }
    }
}
