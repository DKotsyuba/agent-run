#![cfg(all(feature = "test-fixtures", target_os = "linux"))]
//! Optional real-UID service probe. It touches only freshly created fixtures.

use agent_run::{domain::AgentId, transport::socket};
use agent_run_domain::catalog::{AccountRecord, AccountStatus};
use std::{
    fs,
    io::Write,
    os::unix::{
        fs::{MetadataExt, PermissionsExt, chown},
        process::CommandExt,
    },
    path::{Path, PathBuf},
    process::Stdio,
    time::Duration,
};
use tokio::process::{Child, Command};

const GROUP: u32 = 65000;

/// Accepts only absolute, normalized paths safe to embed in fixture configuration.
fn safe_fixture_path(value: &str) -> bool {
    value.starts_with('/')
        && value.len() > 1
        && value[1..].split('/').all(|part| {
            !part.is_empty()
                && part != "."
                && part != ".."
                && part
                    .bytes()
                    .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-' | b'.'))
        })
}

/// Requires an explicit safe path without displaying its value on failure.
fn fixture_path_env(name: &str) -> String {
    let value = std::env::var(name).unwrap_or_else(|_| panic!("{name} is required"));
    assert!(
        safe_fixture_path(&value),
        "{name} must be a safe absolute path"
    );
    value
}

/// Rejects missing, relative, non-normalized, and configuration-unsafe paths.
#[test]
fn fixture_paths_reject_unsafe_inputs() {
    assert!(safe_fixture_path("/opt/fixture/bin/engine"));
    for path in [
        "", "/", "relative", "/a/../b", "/a/./b", "/a//b", "/a/'b", "/a/\nb",
    ] {
        assert!(!safe_fixture_path(path));
    }
}

/// An exact test-owned child, killed on unwind.
struct Process(Child);
impl Drop for Process {
    fn drop(&mut self) {
        let _ = self.0.start_kill();
    }
}

/// Runs a fixed fixture command under one actual host account and temporary group.
fn user_command(uid: u32, group: bool, tmp: &Path, program: &Path) -> Command {
    let mut command = Command::new("/usr/bin/setpriv");
    command.args(["--reuid", &uid.to_string(), "--regid", &uid.to_string()]);
    if group {
        command.args(["--groups", &GROUP.to_string()]);
    } else {
        command.arg("--clear-groups");
    }
    if std::env::var_os("AR_UID_CODEX").is_some() {
        command.args(["--inh-caps=-all", "--ambient-caps=-all"]);
    } else {
        command.args([
            "--no-new-privs",
            "--inh-caps=-all",
            "--ambient-caps=-all",
            "--bounding-set=-all",
        ]);
    }
    command
        .arg(program)
        .env_clear()
        .env("PATH", "/usr/bin:/bin")
        .env("TMPDIR", tmp)
        .env("TMP", tmp)
        .env("TEMP", tmp)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::inherit())
        .kill_on_drop(true);
    // SAFETY: umask is async-signal-safe and this hook runs only in the child.
    unsafe {
        command.as_std_mut().pre_exec(|| {
            libc::umask(0o007);
            Ok(())
        });
    }
    command
}

/// Creates the disposable broker database and selects either a fake account or
/// an explicitly requested live Claude file reference; it never reads the key.
#[test]
#[ignore = "subprocess of the root-only UID fixture"]
fn prepare_state() {
    let state = PathBuf::from(std::env::var_os("AR_UID_STATE").unwrap());
    let private = PathBuf::from(std::env::var_os("AR_UID_PRIVATE").unwrap());
    let engine = std::env::var("AR_UID_ENGINE").unwrap();
    let codex = std::env::var_os("AR_UID_CODEX").is_some();
    let live = std::env::var_os("AR_UID_REAL").is_some();
    let (provider, harness, model, connection, family, credential) = if codex {
        (
            "codex",
            "codex",
            "gpt-6-luna",
            "{kind='native'}",
            "openai",
            "native:codex".to_owned(),
        )
    } else if live {
        (
            "claude",
            "claude-code",
            "claude-haiku-4-5-20251001",
            "{kind='custom',endpoint='https://api.anthropic.com',protocol='messages',auth_header='x_api_key'}",
            "anthropic",
            format!("file:{}", fixture_path_env("AR_UID_LIVE_CREDENTIAL_FILE")),
        )
    } else {
        (
            "claude",
            "claude-code",
            "fixture",
            "{kind='custom',endpoint='https://fixture.invalid',protocol='messages',auth_header='x_api_key'}",
            "anthropic",
            "env:FAKE_TOKEN".to_owned(),
        )
    };
    let model_params = if codex {
        "allowed_params={effort=['low']}\n"
    } else {
        ""
    };
    agent_run_store::fixture::initialize(&state).unwrap();
    fs::create_dir(state.join("agents")).unwrap();
    fs::create_dir(state.join("profiles")).unwrap();
    fs::write(state.join("profiles/review.md"), "+++\nrevision='1'\nwrite=false\nnetwork=false\nallow_external_read_roots=false\nskills=[]\nmcp=[]\nrequired_constraints=[]\n+++\nReview safely.\n").unwrap();
    fs::write(state.join("config.toml"), format!(
        "schema_version=2\n[harnesses.codex]\nbinary='{engine}'\nhome='{0}/codex'\n[harnesses.claude-code]\nbinary='{engine}'\nhome='{0}/claude'\n[providers.{provider}]\nharness='{harness}'\nconnection={connection}\nauth_family='{family}'\nlimits_source='none'\n[[providers.{provider}.models]]\nid='{model}'\n{model_params}[[providers.{provider}.bindings]]\nlabel='fixture'\naccount='fixture-account'\n", private.display())).unwrap();
    agent_run_store::fixture::open(&state)
        .unwrap()
        .register_account(&AccountRecord {
            account_id: "fixture-account".parse().unwrap(),
            auth_family: family.parse().unwrap(),
            secret_ref: credential.parse().unwrap(),
            status: AccountStatus::Enabled,
        })
        .unwrap();
}

/// Starts and observes one run solely through the group-authorized API.
#[test]
#[ignore = "subprocess of the root-only UID fixture"]
fn group_client() {
    let api = PathBuf::from(std::env::var_os("AR_UID_API").unwrap());
    let work = PathBuf::from(std::env::var_os("AR_UID_WORK").unwrap());
    let id_file = PathBuf::from(std::env::var_os("AR_UID_ID").unwrap());
    let codex = std::env::var_os("AR_UID_CODEX").is_some();
    let live = std::env::var_os("AR_UID_REAL").is_some();
    let finish = std::env::var_os("AR_UID_FINISH").is_some();
    let (provider, model, task) = if codex {
        (
            "codex",
            "gpt-6-luna",
            "Reply with exactly OK. Do not use tools.",
        )
    } else if live {
        (
            "claude",
            "claude-haiku-4-5-20251001",
            "Reply with exactly OK. Do not use tools.",
        )
    } else if finish {
        ("claude", "fixture", "fixture:worker-finish")
    } else {
        ("claude", "fixture", "fixture:answer")
    };
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    runtime.block_on(async {
        let mut request = serde_json::json!({"provider":provider,"model":model,"profile":"review",
            "explicit_finish":finish,"task":task,"workdir":work,
            "account":"fixture","request_id":"real-uid-fixture"});
        if codex {
            request["effort"] = serde_json::json!("low");
        }
        let started = socket::client_at(&api, 1000, "start", request)
            .await
            .unwrap();
        let id: AgentId = started["agent_id"].as_str().unwrap().parse().unwrap();
        fs::write(&id_file, id.as_str()).unwrap();
        let done = socket::client_at(&api, 1000, "wait", serde_json::json!({"agent_id":id}))
            .await
            .unwrap();
        assert_eq!(done["status"], "succeeded", "native run did not succeed");
        let answer = socket::client_at(&api, 1000, "answer", serde_json::json!({"agent_id":id}))
            .await
            .unwrap();
        if live || codex {
            assert!(
                answer["content"]
                    .as_str()
                    .is_some_and(|text| !text.trim().is_empty())
            );
        } else if finish {
            assert_eq!(answer["content"], "fixture callback answer");
        } else {
            assert_eq!(answer["content"], "fixture final answer\n");
        }
    });
}

/// Polls only a fixture's freshly created local socket.
async fn ready(path: &Path, process: &mut Process) {
    for _ in 0..300 {
        if path.exists() {
            return;
        }
        assert!(
            process.0.try_wait().unwrap().is_none(),
            "fixture service exited early"
        );
        tokio::time::sleep(Duration::from_millis(30)).await;
    }
    panic!("fixture socket did not appear");
}

/// Sends SIGTERM only to this fixture process and waits for its drain.
async fn stop(mut process: Process) {
    if let Some(pid) = process.0.id() {
        // SAFETY: this exact child is owned by the fixture.
        assert_eq!(unsafe { libc::kill(pid as i32, libc::SIGTERM) }, 0);
    }
    assert!(
        tokio::time::timeout(Duration::from_secs(15), process.0.wait())
            .await
            .unwrap()
            .unwrap()
            .success()
    );
}

/// Runs broker UID 1000 and worker/client UIDs 1001/1002 without changing account groups.
/// Live runs require AR_UID_LIVE_ENGINE and, for Claude, AR_UID_LIVE_CREDENTIAL_FILE;
/// Codex runs additionally require an isolated AR_UID_CODEX_HOME because the
/// native CLI may refresh its login. Paths must be absolute and safe.
#[tokio::test]
#[ignore = "requires root and configured test accounts; live modes require explicit opt-in"]
async fn real_uid_shared_worker() {
    // SAFETY: geteuid only reads the current identity.
    assert_eq!(unsafe { libc::geteuid() }, 0);
    let live = std::env::var_os("AR_UID_REAL").is_some();
    let codex = std::env::var_os("AR_UID_CODEX").is_some();
    let finish = std::env::var_os("AR_UID_FINISH").is_some();
    let live_engine = (live || codex).then(|| fixture_path_env("AR_UID_LIVE_ENGINE"));
    let live_credential = (live && !codex).then(|| fixture_path_env("AR_UID_LIVE_CREDENTIAL_FILE"));
    let codex_home = codex.then(|| fixture_path_env("AR_UID_CODEX_HOME"));
    let target_uid = if codex { 1001 } else { 1002 };
    let client_uid = if codex { 1002 } else { 1001 };
    let executable_dir = PathBuf::from(std::env::var_os("AGENT_RUN_UID_TEST_EXEC").unwrap());
    assert_eq!(
        executable_dir.parent(),
        Some(Path::new("/usr/local/libexec"))
    );
    assert!(
        executable_dir
            .file_name()
            .unwrap()
            .to_string_lossy()
            .starts_with("agent-run-shared-test-")
    );
    assert!(!executable_dir.exists());
    fs::create_dir(&executable_dir).unwrap();
    fs::set_permissions(&executable_dir, fs::Permissions::from_mode(0o755)).unwrap();
    let binary = executable_dir.join("agent-run");
    let engine = executable_dir.join("fake-engine");
    let harness = executable_dir.join("uid-test");
    fs::copy(env!("CARGO_BIN_EXE_agent-run"), &binary).unwrap();
    fs::copy(env!("CARGO_BIN_EXE_agent-run-fixture"), &engine).unwrap();
    fs::copy(std::env::current_exe().unwrap(), &harness).unwrap();
    for path in [&binary, &engine, &harness] {
        fs::set_permissions(path, fs::Permissions::from_mode(0o755)).unwrap();
    }
    let temp = tempfile::Builder::new()
        .prefix("agent-run-shared-")
        .tempdir_in("/run")
        .unwrap();
    let root = temp.path();
    fs::set_permissions(root, fs::Permissions::from_mode(0o755)).unwrap();
    let state = root.join("state");
    let user = root.join("user");
    let private = user.join(".agent-run");
    let work = root.join("work");
    let broker_tmp = root.join("bt");
    let target_tmp = root.join("tt");
    let client_tmp = root.join("ct");
    let broker_home = root.join("bh");
    for path in [
        &state,
        &user,
        &private,
        &work,
        &broker_tmp,
        &target_tmp,
        &client_tmp,
        &broker_home,
    ] {
        fs::create_dir(path).unwrap();
    }
    for path in [&state, &broker_tmp, &broker_home] {
        chown(path, Some(1000), Some(GROUP)).unwrap();
    }
    for path in [&user, &private, &target_tmp] {
        chown(path, Some(target_uid), Some(target_uid)).unwrap();
    }
    chown(&client_tmp, Some(client_uid), Some(client_uid)).unwrap();
    for path in [
        &user,
        &private,
        &broker_tmp,
        &target_tmp,
        &client_tmp,
        &broker_home,
    ] {
        fs::set_permissions(path, fs::Permissions::from_mode(0o700)).unwrap();
    }
    fs::set_permissions(&state, fs::Permissions::from_mode(0o700)).unwrap();
    fs::set_permissions(&work, fs::Permissions::from_mode(0o755)).unwrap();
    let mut setup = user_command(1000, true, &broker_tmp, &harness);
    setup
        .args(["--ignored", "--exact", "prepare_state", "--nocapture"])
        .env("AR_UID_STATE", &state)
        .env("AR_UID_PRIVATE", &private)
        .env(
            "AR_UID_ENGINE",
            live_engine.as_deref().map(Path::new).unwrap_or(&engine),
        );
    if live {
        setup.env("AR_UID_REAL", "1");
    }
    if let Some(credential) = &live_credential {
        setup.env("AR_UID_LIVE_CREDENTIAL_FILE", credential);
    }
    if codex {
        setup.env("AR_UID_CODEX", "1");
    }
    assert!(setup.status().await.unwrap().success());
    let runtime = private.join(if codex { "codex" } else { "claude" });
    let runs = runtime.join("runs");
    let old_runtime = runs.join("ag-20200101-000000-0000000002");
    let configured_runtime = runs.join("ag-20200101-000000-0000000003");
    for path in [&runtime, &runs, &old_runtime, &configured_runtime] {
        fs::create_dir(path).unwrap();
        chown(path, Some(target_uid), Some(target_uid)).unwrap();
        fs::set_permissions(path, fs::Permissions::from_mode(0o700)).unwrap();
    }
    for path in [&old_runtime, &configured_runtime] {
        let file = path.join("fixture");
        fs::write(&file, "fixture").unwrap();
        chown(file, Some(target_uid), Some(target_uid)).unwrap();
    }
    writeln!(
        fs::OpenOptions::new()
            .append(true)
            .open(state.join("config.toml"))
            .unwrap(),
        "\n[environments.retention_fixture.variables]\nRETAIN='{}'",
        configured_runtime.join("fixture").display()
    )
    .unwrap();
    for path in [state.clone(), state.join("agents")] {
        chown(&path, Some(1000), Some(GROUP)).unwrap();
        fs::set_permissions(path, fs::Permissions::from_mode(0o2750)).unwrap();
    }
    let expired = state.join("agents/ag-20200101-000000-0000000001");
    fs::create_dir(&expired).unwrap();
    chown(&expired, Some(1000), Some(GROUP)).unwrap();
    fs::set_permissions(&expired, fs::Permissions::from_mode(0o2770)).unwrap();
    let old_artifact = expired.join("old-answer.md");
    fs::write(&old_artifact, b"synthetic expired artifact").unwrap();
    chown(&old_artifact, Some(target_uid), Some(GROUP)).unwrap();
    fs::set_permissions(&old_artifact, fs::Permissions::from_mode(0o660)).unwrap();
    let database = state.join("state.db");
    chown(&database, Some(1000), Some(GROUP)).unwrap();
    fs::set_permissions(database, fs::Permissions::from_mode(0o640)).unwrap();
    let broker_dir = root.join("broker");
    let worker_dir = root.join("worker");
    fs::create_dir(&broker_dir).unwrap();
    fs::create_dir(&worker_dir).unwrap();
    chown(&broker_dir, Some(1000), Some(GROUP)).unwrap();
    chown(&worker_dir, Some(target_uid), Some(GROUP)).unwrap();
    for path in [&broker_dir, &worker_dir] {
        fs::set_permissions(path, fs::Permissions::from_mode(0o2750)).unwrap();
    }
    let api = broker_dir.join("api.sock");
    let writer = broker_dir.join("writer.sock");
    let worker = worker_dir.join("worker.sock");
    let config = root.join("routes.toml");
    let harness_kind = if codex { "codex" } else { "claude-code" };
    fs::write(&config, format!("operator_socket='{}'\nwriter_socket='{}'\n[workers.{harness_kind}]\nsocket='{}'\nprivate_home='{}'\n", api.display(), writer.display(), worker.display(), private.display())).unwrap();
    fs::set_permissions(&config, fs::Permissions::from_mode(0o644)).unwrap();
    let mut target = Process(
        user_command(target_uid, true, &target_tmp, &binary)
            .env(
                "HOME",
                codex_home.as_deref().map(Path::new).unwrap_or(&user),
            )
            .env("FAKE_TOKEN", "synthetic-token")
            .env("AGENT_RUN_SYSTEM_WRITER_SOCKET", &writer)
            .env("AGENT_RUN_SYSTEM_OPERATOR_SOCKET", &api)
            .args([
                "--home",
                state.to_str().unwrap(),
                "_launch-worker",
                "--socket",
                worker.to_str().unwrap(),
            ])
            .spawn()
            .unwrap(),
    );
    ready(&worker, &mut target).await;
    let mut broker = Process(
        user_command(1000, true, &broker_tmp, &binary)
            .env("HOME", &broker_home)
            .args([
                "--home",
                state.to_str().unwrap(),
                "api",
                "serve",
                "--shared-config",
                config.to_str().unwrap(),
            ])
            .spawn()
            .unwrap(),
    );
    ready(&api, &mut broker).await;
    for _ in 0..200 {
        if !expired.exists() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    assert!(
        !expired.exists(),
        "shared retention did not remove the expired foreign-owned file"
    );
    tokio::time::timeout(Duration::from_secs(15), async {
        while old_runtime.exists() {
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("the account worker did not reclaim its private orphan");
    assert!(configured_runtime.exists(), "configured path was removed");
    let deny = "import errno,socket,sys; s=socket.socket(socket.AF_UNIX); assert s.connect_ex(sys.argv[1]) == errno.EACCES";
    assert!(
        user_command(
            client_uid,
            false,
            &client_tmp,
            Path::new("/usr/bin/python3")
        )
        .args(["-c", deny, api.to_str().unwrap()])
        .status()
        .await
        .unwrap()
        .success()
    );
    let id_file = client_tmp.join("id");
    let mut client_command = user_command(client_uid, true, &client_tmp, &harness);
    client_command
        .args(["--ignored", "--exact", "group_client", "--nocapture"])
        .env("AR_UID_API", &api)
        .env("AR_UID_WORK", &work)
        .env("AR_UID_ID", &id_file);
    if live {
        client_command.env("AR_UID_REAL", "1");
    }
    if codex {
        client_command.env("AR_UID_CODEX", "1");
    }
    if finish {
        client_command.env("AR_UID_FINISH", "1");
    }
    let client = client_command.spawn().unwrap();
    let status = tokio::time::timeout(Duration::from_secs(90), client.wait_with_output())
        .await
        .unwrap()
        .unwrap()
        .status;
    if !status.success() {
        let retained = temp.keep();
        panic!(
            "group client failed; isolated fixture retained at {}",
            retained.display()
        );
    }
    let id = fs::read_to_string(id_file).unwrap();
    let answer = fs::metadata(state.join("agents").join(id).join("answer.md")).unwrap();
    assert_eq!((answer.uid(), answer.gid()), (target_uid, GROUP));
    assert_ne!(
        answer.mode() & 0o040,
        0,
        "broker group must be able to read the result"
    );
    stop(broker).await;
    stop(target).await;
    for path in [&binary, &engine, &harness] {
        fs::remove_file(path).unwrap();
    }
    fs::remove_dir(executable_dir).unwrap();
}
