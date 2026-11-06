#![cfg(feature = "test-fixtures")]
//! Real broker, account-owned worker and supervisor with an offline fake engine.

use agent_run::{
    domain::{AgentId, Status},
    transport::socket,
};
use agent_run_domain::catalog::{AccountRecord, AccountStatus};
use std::{
    fs,
    os::unix::fs::{MetadataExt, PermissionsExt},
    path::{Path, PathBuf},
    process::Stdio,
    time::{Duration, Instant},
};
use tokio::io::AsyncWriteExt;
use tokio::process::{Child, Command};

/// Kills only the test-owned process if the fixture unwinds unexpectedly.
struct Process(Child);
impl Drop for Process {
    fn drop(&mut self) {
        let _ = self.0.start_kill();
    }
}

/// Starts a disposable process with no inherited Agent Run bindings.
fn command(home: &Path) -> Command {
    let mut command = Command::new(env!("CARGO_BIN_EXE_agent-run"));
    command.env_remove("AGENT_RUN_HOME");
    for (name, _) in std::env::vars_os() {
        if name.to_string_lossy().starts_with("AGENT_RUN_") {
            command.env_remove(name);
        }
    }
    command
        .args(["--home", home.to_str().unwrap()])
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::inherit())
        .kill_on_drop(true);
    command
}

/// Waits for an actual group socket to accept a connection.
async fn wait_socket(path: &Path, process: &mut Process) {
    let deadline = Instant::now() + Duration::from_secs(15);
    loop {
        if tokio::net::UnixStream::connect(path).await.is_ok() {
            return;
        }
        assert!(
            process.0.try_wait().unwrap().is_none(),
            "fixture process stopped before socket readiness"
        );
        assert!(
            Instant::now() < deadline,
            "fixture socket did not become ready"
        );
        tokio::time::sleep(Duration::from_millis(30)).await;
    }
}

/// Stops a service process normally, then waits for its owned tasks to drain.
async fn stop(mut process: Process) {
    if let Some(pid) = process.0.id() {
        // SAFETY: pid belongs to the child owned by this fixture.
        assert_eq!(unsafe { libc::kill(pid as i32, libc::SIGTERM) }, 0);
    }
    let status = tokio::time::timeout(Duration::from_secs(10), process.0.wait())
        .await
        .unwrap()
        .unwrap();
    assert!(
        status.success(),
        "fixture process did not shut down cleanly"
    );
}

/// Runs one host hook through the explicit shared broker endpoint.
async fn hook(api: &Path, uid: u32, kind: &str, payload: serde_json::Value) -> serde_json::Value {
    let mut child = Command::new(env!("CARGO_BIN_EXE_agent-run"))
        .args([
            "--broker-socket",
            api.to_str().unwrap(),
            "--broker-uid",
            &uid.to_string(),
            "hook",
            kind,
        ])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::inherit())
        .kill_on_drop(true)
        .spawn()
        .unwrap();
    let mut input = child.stdin.take().unwrap();
    input
        .write_all(payload.to_string().as_bytes())
        .await
        .unwrap();
    drop(input);
    let result = tokio::time::timeout(Duration::from_secs(10), child.wait_with_output())
        .await
        .unwrap()
        .unwrap();
    assert!(result.status.success(), "shared hook failed");
    serde_json::from_slice(&result.stdout).unwrap()
}

/// A start crosses the real shared socket, worker, supervisor and writer; no
/// network, real provider credential, privileged process or model is used.
#[tokio::test]
async fn shared_worker_runs_a_real_supervisor_with_a_fake_engine() {
    let base = std::env::var_os("AGENT_RUN_TEST_SOCKET_TMP")
        .map(PathBuf::from)
        .unwrap_or_else(std::env::temp_dir);
    let temp = tempfile::Builder::new()
        .prefix("sw-")
        .tempdir_in(base)
        .unwrap();
    let root = temp.path().canonicalize().unwrap();
    let state = root.join("state");
    let user_home = root.join("user");
    let private = user_home.join(".agent-run");
    for path in [&state, &user_home, &private] {
        fs::create_dir(path).unwrap();
    }
    fs::set_permissions(&private, fs::Permissions::from_mode(0o700)).unwrap();
    agent_run_store::fixture::initialize(&state).unwrap();
    fs::create_dir(state.join("agents")).unwrap();
    fs::create_dir(state.join("profiles")).unwrap();
    fs::write(state.join("profiles/review.md"), "+++\nrevision='1'\nwrite=false\nnetwork=false\nallow_external_read_roots=false\nskills=[]\nmcp=[]\nrequired_constraints=[]\n+++\nReview safely.\n").unwrap();
    let fixture = env!("CARGO_BIN_EXE_agent-run-fixture");
    fs::write(state.join("config.toml"), format!(
        "schema_version=2\n[harnesses.codex]\nbinary='{fixture}'\nhome='{0}/codex'\n[harnesses.claude-code]\nbinary='{fixture}'\nhome='{0}/claude'\n[providers.claude]\nharness='claude-code'\nconnection={{kind='custom',endpoint='https://fixture.invalid',protocol='messages',auth_header='x_api_key'}}\nauth_family='anthropic'\nlimits_source='none'\nordered_accounts=true\n[[providers.claude.models]]\nid='fixture'\n[[providers.claude.bindings]]\nlabel='fixture'\naccount='fixture-account'\n[[providers.claude.bindings]]\nlabel='reserve'\naccount='fixture-sub'\npriority_multiplier=0.5\nconnection={{kind='native'}}\n[environments.retention_fixture.variables]\nRETAIN='{0}/claude/runs/ag-20200101-000000-0000000002/keep'\n", private.display())).unwrap();
    let orphan = private.join("claude/runs/ag-20200101-000000-0000000001");
    let retained = private.join("claude/runs/ag-20200101-000000-0000000002");
    for dir in [&orphan, &retained] {
        fs::create_dir_all(dir).unwrap();
        fs::write(dir.join("keep"), "fixture").unwrap();
    }
    agent_run_store::fixture::open(&state)
        .unwrap()
        .register_account(&AccountRecord {
            account_id: "fixture-account".parse().unwrap(),
            auth_family: "anthropic".parse().unwrap(),
            secret_ref: "env:FAKE_TOKEN".parse().unwrap(),
            status: AccountStatus::Enabled,
        })
        .unwrap();
    agent_run_store::fixture::open(&state)
        .unwrap()
        .register_account(&AccountRecord {
            account_id: "fixture-sub".parse().unwrap(),
            auth_family: "anthropic".parse().unwrap(),
            secret_ref: "named:claude-code:reserve".parse().unwrap(),
            status: AccountStatus::Enabled,
        })
        .unwrap();
    agent_run::fs::private_dir(&private.join("accounts/claude/reserve/claude-config")).unwrap();
    let api = root.join("api.sock");
    let writer = root.join("writer.sock");
    let worker = root.join("worker.sock");
    let client_a = root.join("client-a.sock");
    let client_b = root.join("client-b.sock");
    let routes = root.join("routes.toml");
    fs::write(&routes, format!(
        "operator_socket='{}'\nwriter_socket='{}'\noperator_sockets=['{}','{}']\n[workers.claude-code]\nsocket='{}'\nprivate_home='{}'\n",
        api.display(), writer.display(), client_a.display(), client_b.display(), worker.display(), private.display())).unwrap();
    let mut worker_process = Process(
        command(&state)
            .env("HOME", &user_home)
            .env("FAKE_TOKEN", "synthetic-token")
            .env("AGENT_RUN_SYSTEM_WRITER_SOCKET", &writer)
            .env("AGENT_RUN_SYSTEM_OPERATOR_SOCKET", &api)
            .args(["_launch-worker", "--socket", worker.to_str().unwrap()])
            .spawn()
            .unwrap(),
    );
    wait_socket(&worker, &mut worker_process).await;
    let mut broker = Process(
        command(&state)
            .args(["api", "serve", "--shared-config", routes.to_str().unwrap()])
            .spawn()
            .unwrap(),
    );
    wait_socket(&api, &mut broker).await;
    let broker_uid = fs::metadata(&state).unwrap().uid();
    for endpoint in [&client_a, &client_b] {
        assert_eq!(
            socket::client_at(endpoint, broker_uid, "ping", serde_json::json!({}))
                .await
                .unwrap()["ok"],
            true
        );
    }
    let config_proof = socket::client_at(
        &api,
        broker_uid,
        "_runtime_retention",
        serde_json::json!({}),
    )
    .await
    .unwrap();
    assert!(
        config_proof["protected_paths"]
            .as_array()
            .unwrap()
            .contains(&serde_json::json!(retained.join("keep")))
    );
    assert!(
        socket::client_at(
            &api,
            broker_uid,
            "_runtime_retention",
            serde_json::json!({"unexpected":true}),
        )
        .await
        .is_err()
    );
    tokio::time::timeout(Duration::from_secs(15), async {
        while orphan.exists() {
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("the owning worker should reclaim the orphan");
    assert!(retained.exists(), "configured paths must remain");

    // A refused private-history preflight must not consume the parent's one
    // continuation. Restoring the fixture then permits the identical request.
    let parent = socket::client_at(
        &api,
        broker_uid,
        "start",
        serde_json::json!({
            "provider":"claude", "model":"fixture", "profile":"review", "explicit_finish":false,
            "task":"fixture:answer", "workdir":root, "account":"fixture", "request_id":"system-resume-parent"
        }),
    )
    .await
    .unwrap();
    let parent: AgentId = parent["agent_id"].as_str().unwrap().parse().unwrap();
    let finished = socket::client_at(
        &api,
        broker_uid,
        "wait",
        serde_json::json!({"agent_id":parent}),
    )
    .await
    .unwrap();
    assert_eq!(finished["status"], "succeeded");
    let policy = socket::client_at(
        &api,
        broker_uid,
        "_system_policy",
        serde_json::json!({"execution":parent}),
    )
    .await
    .unwrap();
    assert_eq!(policy["status"], "allowed");
    assert_eq!(policy.as_object().unwrap().len(), 4);
    let serialized = policy.to_string();
    assert!(!serialized.contains("synthetic-token"));
    assert!(!serialized.contains(private.to_str().unwrap()));
    assert!(
        socket::client_at(
            &api,
            broker_uid,
            "_system_policy",
            serde_json::json!({"execution":parent,"path":private}),
        )
        .await
        .is_err()
    );
    let fixture_store = agent_run_store::fixture::open(&state).unwrap();
    let original_identity: String = fixture_store
        .conn
        .query_row(
            "SELECT identity_json FROM agents WHERE id=?",
            [parent.as_str()],
            |row| row.get(0),
        )
        .unwrap();
    let mut missing: serde_json::Value = serde_json::from_str(&original_identity).unwrap();
    missing["runtime_home"] = serde_json::json!(private.join("missing-fixture-home"));
    fixture_store
        .conn
        .execute(
            "UPDATE agents SET identity_json=? WHERE id=?",
            rusqlite::params![missing.to_string(), parent.as_str()],
        )
        .unwrap();
    let resume =
        serde_json::json!({"agent_id":parent,"task":"fixture:answer","request_id":"system-resume"});
    let assets_error = socket::client_at(&api, broker_uid, "resume", resume.clone())
        .await
        .unwrap_err()
        .to_string();
    assert!(
        assets_error.contains("assets_unverifiable"),
        "{assets_error}"
    );
    assert!(!assets_error.contains(private.to_str().unwrap()));
    fixture_store
        .conn
        .execute(
            "UPDATE agents SET identity_json=? WHERE id=?",
            rusqlite::params![original_identity, parent.as_str()],
        )
        .unwrap();
    drop(fixture_store);
    let reader = agent_run_store::writer::StoreReader::open(&state).unwrap();
    let adapter: String = reader
        .conn()
        .query_row(
            "SELECT adapter_state_json FROM attempts WHERE agent_id=? ORDER BY number DESC LIMIT 1",
            [parent.as_str()],
            |row| row.get(0),
        )
        .unwrap();
    let adapter: serde_json::Value = serde_json::from_str(&adapter).unwrap();
    let seal = &adapter["native_history"]["seal"];
    let history =
        PathBuf::from(seal["root"].as_str().unwrap()).join(seal["relative"].as_str().unwrap());
    let original = fs::read(&history).unwrap();
    fs::write(&history, b"tampered fixture history\n").unwrap();
    let history_error = socket::client_at(&api, broker_uid, "resume", resume.clone())
        .await
        .unwrap_err()
        .to_string();
    assert!(
        history_error.contains("history_unverifiable"),
        "{history_error}"
    );
    assert!(!history_error.contains(private.to_str().unwrap()));
    let children: i64 = reader
        .conn()
        .query_row(
            "SELECT COUNT(*) FROM agents WHERE parent_agent_id=?",
            [parent.as_str()],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(children, 0);
    drop(reader);
    fs::write(&history, original).unwrap();
    let alternate = serde_json::json!({"agent_id":parent,"task":"fixture:answer","request_id":"system-resume-other"});
    let (first, second) = tokio::join!(
        socket::client_at(&api, broker_uid, "resume", resume.clone()),
        socket::client_at(&api, broker_uid, "resume", alternate.clone()),
    );
    let (child, replay_request) = match (first, second) {
        (Ok(child), Err(_)) => (child, resume),
        (Err(_), Ok(child)) => (child, alternate),
        _ => panic!("exactly one concurrent continuation must be admitted"),
    };
    assert_eq!(child["created"], true);
    let child_id: AgentId = child["agent_id"].as_str().unwrap().parse().unwrap();
    let done = socket::client_at(
        &api,
        broker_uid,
        "wait",
        serde_json::json!({"agent_id":child_id}),
    )
    .await
    .unwrap();
    assert_eq!(done["status"], "succeeded", "{done}");
    let replay = socket::client_at(&api, broker_uid, "resume", replay_request)
        .await
        .unwrap();
    assert_eq!(replay["created"], false);
    assert_eq!(replay["agent_id"], child_id.as_str());

    // Revoking the selected ordered binding after allocation must still stop
    // the worker before it starts the replacement native process.
    fs::write(state.join("fixture-pause-spawn-2"), "").unwrap();
    let switched = socket::client_at(
        &api,
        broker_uid,
        "start",
        serde_json::json!({
            "provider":"claude", "model":"fixture", "profile":"review", "explicit_finish":false,
            "task":"fixture:billing", "workdir":root, "request_id":"system-policy-revocation"
        }),
    )
    .await
    .unwrap();
    let switched: AgentId = switched["agent_id"].as_str().unwrap().parse().unwrap();
    tokio::time::timeout(Duration::from_secs(20), async {
        while !state.join("fixture-paused-spawn-2").exists() {
            tokio::time::sleep(Duration::from_millis(30)).await;
        }
    })
    .await
    .expect("fallback did not reach the handoff barrier");
    let config_path = state.join("config.toml");
    let original_config = fs::read_to_string(&config_path).unwrap();
    let changed = original_config.replace("connection={kind='native'}", "connection={kind='custom',endpoint='https://changed.invalid',protocol='messages',auth_header='x_api_key'}");
    assert_ne!(changed, original_config);
    fs::write(&config_path, changed).unwrap();
    fs::write(state.join("fixture-resume-spawn-2"), "").unwrap();
    let refused = socket::client_at(
        &api,
        broker_uid,
        "wait",
        serde_json::json!({"agent_id":switched}),
    )
    .await
    .unwrap();
    assert_eq!(refused["status"], "failed");
    let reader = agent_run_store::writer::StoreReader::open(&state).unwrap();
    let (process, proof): (Option<String>, String) = reader.conn().query_row(
        "SELECT process_identity,cleanup_proof_json FROM attempts WHERE agent_id=? AND number=2",
        [switched.as_str()], |row| Ok((row.get(0)?, row.get(1)?)),
    ).unwrap();
    assert!(process.is_none());
    assert!(proof.contains("never_spawned"));
    drop(reader);
    fs::write(&config_path, original_config).unwrap();

    // The previous billing failure still cools the API binding; later
    // independent fixture scenarios must use the unaffected native account.
    let blocked_api = socket::client_at(
        &api,
        broker_uid,
        "start",
        serde_json::json!({
            "provider":"claude", "model":"fixture", "profile":"review", "explicit_finish":false,
            "task":"fixture:answer", "workdir":root, "account":"fixture", "request_id":"cooled-api-refusal"
        }),
    )
    .await
    .unwrap_err();
    assert!(blocked_api.to_string().contains("binding_cooldown"));

    let callback_request = serde_json::json!({
        "provider":"claude", "model":"fixture", "profile":"review", "explicit_finish":true,
        "task":"fixture:worker-finish", "workdir":root, "account":"reserve", "request_id":"worker-finish-fixture"
    });
    let started = socket::client_at(&api, broker_uid, "start", callback_request)
        .await
        .unwrap();
    let callback_id: AgentId = started["agent_id"].as_str().unwrap().parse().unwrap();
    let callback = tokio::time::timeout(
        Duration::from_secs(30),
        socket::client_at(
            &api,
            broker_uid,
            "wait",
            serde_json::json!({"agent_id":callback_id}),
        ),
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(
        callback["status"], "succeeded",
        "callback failed: {callback}"
    );
    assert_eq!(callback["content"], "fixture callback answer");
    let binding = hook(
        &api,
        broker_uid,
        "bind",
        serde_json::json!({
            "agent_id":callback_id,"transport":"codex_queue","external_session_id":"fixture-session"
        }),
    )
    .await;
    assert_eq!(
        binding["hookSpecificOutput"]["hookEventName"],
        "PostToolUse"
    );
    assert!(
        binding["hookSpecificOutput"]["additionalContext"]
            .as_str()
            .unwrap()
            .contains("notifications are not configured")
    );
    let context = hook(
        &api,
        broker_uid,
        "context",
        serde_json::json!({
            "transport":"codex_queue","external_session_id":"fixture-session"
        }),
    )
    .await;
    assert!(context.is_object());
    let request = serde_json::json!({
        "provider":"claude", "model":"fixture", "profile":"review", "explicit_finish":false,
        "task":"fixture:load-barrier:answer", "workdir":root, "account":"reserve", "request_id":"shared-worker-fixture"
    });
    let result = socket::client_at(&api, broker_uid, "start", request)
        .await
        .unwrap();
    let id: AgentId = result["agent_id"].as_str().unwrap().parse().unwrap();
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        let reader = agent_run_store::writer::StoreReader::open(&state).unwrap();
        let spawned: bool = reader.conn().query_row(
            "SELECT EXISTS(SELECT 1 FROM attempts WHERE agent_id=? AND process_identity IS NOT NULL)",
            [id.as_str()], |row| row.get(0),
        ).unwrap();
        if spawned {
            break;
        }
        assert!(Instant::now() < deadline, "fake engine was not spawned");
        tokio::time::sleep(Duration::from_millis(30)).await;
    }
    // SAFETY: this is the broker child owned by the fixture.
    let stopped = unsafe { libc::kill(broker.0.id().unwrap() as i32, libc::SIGTERM) };
    assert_eq!(stopped, 0);
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert!(
        broker.0.try_wait().unwrap().is_none(),
        "broker stopped while a run was active"
    );
    let blocked = serde_json::json!({
        "provider":"claude", "model":"fixture", "profile":"review", "explicit_finish":false,
        "task":"fixture:answer", "workdir":root, "account":"reserve", "request_id":"drain-refusal"
    });
    assert!(
        socket::client_at(&api, broker_uid, "start", blocked)
            .await
            .is_err()
    );
    fs::write(root.join("fixture-load-release"), b"release").unwrap();
    loop {
        let row = agent_run_store::writer::StoreReader::open(&state)
            .unwrap()
            .get(&id)
            .unwrap();
        if row.status.terminal() {
            assert_eq!(
                row.status,
                Status::Succeeded,
                "failure={:?}",
                row.failure_kind
            );
            break;
        }
        assert!(
            Instant::now() < deadline,
            "supervisor did not finish: {:?}",
            row.status
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    let answer = agent_run::service::Service::new(state.clone())
        .answer(&id)
        .unwrap();
    assert_eq!(answer["content"], "fixture final answer\n");
    let broker_status = tokio::time::timeout(Duration::from_secs(10), broker.0.wait())
        .await
        .unwrap()
        .unwrap();
    assert!(broker_status.success());
    stop(worker_process).await;
}
