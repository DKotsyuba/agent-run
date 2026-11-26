#![cfg(feature = "test-fixtures")]
//! Live CLI and MCP batch admission through an isolated broker and fake engine.

use agent_run::transport::socket::BrokerClient;
use agent_run_domain::domain::{AgentId, Status};
use serde_json::{Value, json};
use std::{
    io::{BufRead, BufReader, Write},
    os::unix::net::UnixStream,
    path::{Path, PathBuf},
    process::{Child, Command, ExitStatus, Stdio},
    time::{Duration, Instant},
};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt};

/// A child command whose home, temporary paths and credential are synthetic.
fn command(home: &Path) -> Command {
    let mut command = Command::new(env!("CARGO_BIN_EXE_agent-run"));
    command
        .env_clear()
        .env("HOME", home)
        .env("PATH", "/usr/bin:/bin")
        .env("TMPDIR", home.join("tmp"))
        .env("TMP", home.join("tmp"))
        .env("TEMP", home.join("tmp"))
        .arg("--home")
        .arg(home);
    command
}

/// Runs one bounded CLI command and reaps the exact child on timeout.
async fn cli(home: &Path, args: &[&str]) -> (ExitStatus, String, String) {
    let stdout = home.join("cli.stdout");
    let stderr = home.join("cli.stderr");
    let mut process = tokio::process::Command::from(command(home))
        .args(args)
        .stdin(Stdio::null())
        .stdout(std::fs::File::create(&stdout).unwrap())
        .stderr(std::fs::File::create(&stderr).unwrap())
        .kill_on_drop(true)
        .spawn()
        .unwrap();
    let status = match tokio::time::timeout(Duration::from_secs(15), process.wait()).await {
        Ok(status) => status.unwrap(),
        Err(_) => {
            process.kill().await.unwrap();
            process.wait().await.unwrap();
            panic!("fixture CLI timed out: {args:?}");
        }
    };
    (
        status,
        std::fs::read_to_string(stdout).unwrap(),
        std::fs::read_to_string(stderr).unwrap(),
    )
}

/// A disposable home and its real broker, including panic-path cleanup.
struct Broker {
    home: PathBuf,
    temp: Option<tempfile::TempDir>,
    child: Child,
}

impl Broker {
    /// Initializes one synthetic account and waits until the owned socket answers.
    async fn start() -> Self {
        let base = std::env::var_os("AGENT_RUN_TEST_SOCKET_TMP")
            .unwrap_or_else(|| std::env::temp_dir().into_os_string());
        let temp = tempfile::Builder::new()
            .prefix("ar-batch-")
            .tempdir_in(base)
            .unwrap();
        let home = temp.path().canonicalize().unwrap();
        std::fs::create_dir(home.join("tmp")).unwrap();
        let (status, _, error) = cli(&home, &["init"]).await;
        assert!(status.success(), "init: {error}");
        std::fs::write(home.join("profiles/review.md"), "+++\nrevision = \"1\"\nwrite = false\nnetwork = false\nallow_external_read_roots = false\nskills = []\nmcp = []\nrequired_constraints = []\n+++\nReview safely.\n").unwrap();
        configure(&home, 1);
        let (status, _, error) = cli(
            &home,
            &[
                "accounts",
                "register",
                "--id",
                "acct-work",
                "--auth-family",
                "anthropic",
                "--reference",
                "env:FAKE_TOKEN",
            ],
        )
        .await;
        assert!(status.success(), "register: {error}");
        let child = command(&home)
            .args(["api", "serve"])
            .env("FAKE_TOKEN", "synthetic-token")
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(std::fs::File::create(home.join("broker.stderr")).unwrap())
            .spawn()
            .unwrap();
        let mut broker = Self {
            home,
            temp: Some(temp),
            child,
        };
        let deadline = Instant::now() + Duration::from_secs(10);
        loop {
            if BrokerClient::new(broker.home.join("api.sock"))
                .call("ping", None)
                .await
                .is_ok()
            {
                return broker;
            }
            assert!(
                broker.child.try_wait().unwrap().is_none() && Instant::now() < deadline,
                "broker failed to start: {}",
                std::fs::read_to_string(broker.home.join("broker.stderr")).unwrap_or_default()
            );
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }
}

/// Changes only the disposable broker's fixture configuration.
fn configure(home: &Path, limit: usize) {
    let value = |s: &str| toml::Value::String(s.into());
    let binary = value(env!("CARGO_BIN_EXE_agent-run-fixture"));
    let codex = value(home.join("codex").to_str().unwrap());
    let claude = value(home.join("claude").to_str().unwrap());
    std::fs::write(
        home.join("config.toml"),
        format!(
            "schema_version = 2\n[core]\nmax_active_agents = {limit}\n[harnesses.codex]\nbinary = {binary}\nhome = {codex}\n[harnesses.claude-code]\nbinary = {binary}\nhome = {claude}\n[providers.glm-user]\nharness = 'claude-code'\nconnection = {{ kind = 'custom', endpoint = 'https://gateway.example/api', protocol = 'messages' }}\nauth_family = 'anthropic'\nlimits_source = 'none'\n[[providers.glm-user.models]]\nid = 'fixture'\nnative_model = 'fixture'\n[[providers.glm-user.bindings]]\nlabel = 'work'\naccount = 'acct-work'\n"
        ),
    )
    .unwrap();
}

/// Reads the fixture state without opening a writer or migrating it.
fn read_store(home: &Path) -> rusqlite::Connection {
    rusqlite::Connection::open_with_flags(
        home.join("state.db"),
        rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY,
    )
    .unwrap()
}

/// True only after every admitted supervisor and owned attempt has stopped.
fn quiescent(home: &Path) -> bool {
    let Ok(store) = agent_run_store::fixture::open(home) else {
        return false;
    };
    let Ok(active) = store.conn.query_row(
        "SELECT COUNT(*) FROM attempts WHERE ownership_active=1",
        [],
        |row| row.get::<_, i64>(0),
    ) else {
        return false;
    };
    let Ok(mut statement) = store.conn.prepare("SELECT id FROM agents") else {
        return false;
    };
    let Ok(mut ids) = statement.query_map([], |row| row.get::<_, String>(0)) else {
        return false;
    };
    active == 0
        && ids.all(|id| {
            let Ok(id) = id else { return false };
            let Ok(id) = id.parse() else { return false };
            let Ok(row) = store.get(&id) else {
                return false;
            };
            row.status.terminal()
                && matches!(
                    agent_run_platform::process::observe(
                        row.supervisor_pid,
                        row.supervisor_identity.as_deref(),
                        row.supervisor_birth_time,
                    ),
                    agent_run_platform::process::ProcessState::Dead
                        | agent_run_platform::process::ProcessState::Reused
                        | agent_run_platform::process::ProcessState::NotStarted
                )
        })
}

impl Drop for Broker {
    /// Cancels only this fixture's active jobs, reaps its broker and preserves uncertain state.
    fn drop(&mut self) {
        if !quiescent(&self.home)
            && let Ok(store) = rusqlite::Connection::open_with_flags(
                self.home.join("state.db"),
                rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY,
            )
            && let Ok(mut query) = store.prepare(
                "SELECT id FROM agents WHERE status IN ('created','starting','running','cancelling')",
            )
            && let Ok(ids) = query.query_map([], |row| row.get::<_, String>(0))
        {
            for id in ids.flatten() {
                if let Ok(mut socket) = UnixStream::connect(self.home.join("api.sock")) {
                    let _ = socket.set_write_timeout(Some(Duration::from_secs(2)));
                    let _ = socket.set_read_timeout(Some(Duration::from_secs(2)));
                    let _ = writeln!(socket, "{}", json!({"jsonrpc":"2.0","id":1,"method":"cancel","params":{"agent_id":id}}));
                    let _ = BufReader::new(socket).read_line(&mut String::new());
                }
            }
        }
        let deadline = Instant::now() + Duration::from_secs(30);
        while !quiescent(&self.home) && Instant::now() < deadline {
            std::thread::sleep(Duration::from_millis(20));
        }
        if !quiescent(&self.home)
            && let Some(temp) = self.temp.take()
        {
            eprintln!(
                "fixture cleanup uncertain; retained {}",
                temp.keep().display()
            );
        }
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

/// Calls one live MCP tool, then waits for or kills and reaps its exact child.
async fn mcp_call(home: &Path, tool: &str, request: &Value) -> Value {
    let mut child = tokio::process::Command::from(command(home))
        .arg("mcp")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .kill_on_drop(true)
        .spawn()
        .unwrap();
    let result = tokio::time::timeout(Duration::from_secs(15), async {
        let mut input = child.stdin.take().unwrap();
        let mut output = tokio::io::BufReader::new(child.stdout.take().unwrap());
        for frame in [
            json!({"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2025-11-25","capabilities":{},"clientInfo":{"name":"batch-smoke","version":"1"}}}),
            json!({"jsonrpc":"2.0","method":"notifications/initialized"}),
            json!({"jsonrpc":"2.0","id":2,"method":"tools/call","params":{"name":tool,"arguments":request}}),
        ] {
            input.write_all(format!("{frame}\n").as_bytes()).await?;
        }
        input.flush().await?;
        loop {
            let mut line = String::new();
            if output.read_line(&mut line).await? == 0 {
                return Err(std::io::Error::other("MCP exited without a reply"));
            }
            let value: Value = serde_json::from_str(&line)?;
            if value["id"] == 2 {
                return Ok(value);
            }
        }
    })
    .await;
    let status = match tokio::time::timeout(Duration::from_secs(3), child.wait()).await {
        Ok(status) => status.unwrap(),
        Err(_) => {
            child.kill().await.unwrap();
            child.wait().await.unwrap()
        }
    };
    assert!(status.success(), "MCP exited: {status}");
    result.expect("MCP call timed out").expect("MCP reply")
}

/// Asserts orchestrator MCP refuses a socket-only service method as unknown.
async fn assert_mcp_refuses(home: &Path, tool: &str, request: &Value) {
    let reply = mcp_call(home, tool, request).await;
    assert!(reply["result"].is_null(), "{tool} reached MCP: {reply}");
    assert!(
        reply["error"]["message"]
            .as_str()
            .is_some_and(|message| message.contains("unknown tool")),
        "{tool}: {reply}"
    );
}

/// A failed second admission rolls back; CLI admission and MCP replay share one receipt.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn batch_is_atomic_and_replayed_across_live_cli_and_mcp() {
    let broker = Broker::start().await;
    let socket = BrokerClient::new(broker.home.join("api.sock"));
    let default = socket.call("capabilities", Some(json!({}))).await.unwrap();
    assert!(default.get("store_id").is_none());
    let (status, output, error) = cli(&broker.home, &["capabilities"]).await;
    assert!(status.success(), "default capabilities CLI: {error}");
    assert!(
        serde_json::from_str::<Value>(&output)
            .unwrap()
            .get("store_id")
            .is_none()
    );
    // Service integration reads are operator-socket only, never orchestrator MCP.
    assert_mcp_refuses(&broker.home, "capabilities", &json!({})).await;
    let capabilities = socket
        .call("capabilities", Some(json!({"include_store_id":true})))
        .await
        .unwrap();
    let store_id: agent_run_domain::StoreId =
        serde_json::from_value(capabilities["store_id"].clone()).unwrap();
    let (status, output, error) = cli(&broker.home, &["capabilities", "--include-store-id"]).await;
    assert!(status.success(), "capabilities CLI: {error}");
    assert_eq!(
        serde_json::from_str::<Value>(&output).unwrap()["store_id"],
        store_id.as_str()
    );
    let request = json!({"request_id":"socket-batch", "retain_history":true, "expected_store_id":store_id, "jobs":[
        {"provider":"glm-user","model":"fixture","profile":"review","explicit_finish":false,"task":"fixture:answer","workdir":broker.home},
        {"provider":"glm-user","model":"fixture","profile":"review","explicit_finish":false,"task":"fixture:answer","workdir":broker.home}
    ]});
    let spec = broker.home.join("batch.json");
    std::fs::write(&spec, request.to_string()).unwrap();
    let args = ["start-batch", "--spec", spec.to_str().unwrap()];
    assert_eq!(
        socket
            .call("batch_receipt", Some(request.clone()))
            .await
            .unwrap(),
        json!({"found":false})
    );
    assert_mcp_refuses(&broker.home, "batch_receipt", &request).await;

    // A retained notification cannot admit a batch that has no reply channel
    // for the release credential. The following request observes the same socket.
    let mut notification = request.clone();
    notification["request_id"] = json!("retained-notification");
    notification["jobs"].as_array_mut().unwrap().truncate(1);
    let mut raw = tokio::net::UnixStream::connect(broker.home.join("api.sock"))
        .await
        .unwrap();
    raw.write_all(
        format!(
            "{}\n{}\n",
            json!({"jsonrpc":"2.0","method":"start_batch","params":notification}),
            json!({"jsonrpc":"2.0","id":1,"method":"batch_receipt","params":notification}),
        )
        .as_bytes(),
    )
    .await
    .unwrap();
    let mut line = String::new();
    tokio::time::timeout(
        Duration::from_secs(10),
        tokio::io::BufReader::new(raw).read_line(&mut line),
    )
    .await
    .unwrap()
    .unwrap();
    let lookup: Value = serde_json::from_str(&line).unwrap();
    assert_eq!(lookup["id"], 1);
    assert_eq!(lookup["result"], json!({"found":false}));

    // The first job fits; the second reaches the writer's one-slot cap.
    let (status, _, error) = cli(&broker.home, &args).await;
    assert_eq!(status.code(), Some(2), "{error}");
    let error: Value = serde_json::from_str(&error).unwrap();
    assert_eq!(error["error"]["type"], "CapacityExhausted", "{error}");
    let db = read_store(&broker.home);
    for table in ["agents", "attempts", "start_batch_receipts", "history_pins"] {
        let count: i64 = db
            .query_row(&format!("SELECT COUNT(*) FROM {table}"), [], |row| {
                row.get(0)
            })
            .unwrap();
        assert_eq!(count, 0, "{table} must roll back");
    }
    drop(db);

    configure(&broker.home, 3);
    let token_file = broker.home.join("new-release-token");
    let private_args = [
        "start-batch",
        "--spec",
        spec.to_str().unwrap(),
        "--release-token-file",
        token_file.to_str().unwrap(),
    ];
    use std::os::unix::fs::{PermissionsExt, symlink};
    let occupied = broker.home.join("occupied-token");
    std::fs::write(&occupied, "already present").unwrap();
    for use_symlink in [false, true] {
        if use_symlink {
            symlink(&occupied, &token_file).unwrap();
        } else {
            std::fs::write(&token_file, "already present").unwrap();
        }
        let (status, _, _) = cli(&broker.home, &private_args).await;
        assert!(!status.success());
        assert_eq!(
            std::fs::read_to_string(&token_file).unwrap(),
            "already present"
        );
        std::fs::remove_file(&token_file).unwrap();
    }
    assert_eq!(
        socket
            .call("batch_receipt", Some(request.clone()))
            .await
            .unwrap(),
        json!({"found":false})
    );
    let (status, output, error) = cli(&broker.home, &private_args).await;
    assert!(status.success(), "batch admission: {error}");
    let first: Value = serde_json::from_str(&output).unwrap();
    assert_eq!(first["replayed"], false, "{first}");
    assert!(first.get("release_token").is_none());
    assert_eq!(
        std::fs::metadata(&token_file).unwrap().permissions().mode() & 0o777,
        0o600
    );
    assert_eq!(first["receipt"]["request_id"], "socket-batch");
    assert_eq!(first["receipt"]["store_id"], store_id.as_str());
    let jobs = first["receipt"]["jobs"].as_array().unwrap();
    assert_eq!(jobs.len(), 2);
    let ids: Vec<AgentId> = jobs
        .iter()
        .enumerate()
        .map(|(index, job)| {
            assert_eq!(job["index"], index);
            assert_eq!(job["sequence"], 1);
            serde_json::from_value(job["agent_id"].clone()).unwrap()
        })
        .collect();
    assert_ne!(ids[0], ids[1]);
    let execution_refs: Vec<agent_run_domain::batch::ExecutionRef> = jobs
        .iter()
        .map(|job| serde_json::from_value(job["execution_ref"].clone()).unwrap())
        .collect();
    assert_ne!(execution_refs[0], execution_refs[1]);
    let pins: Vec<agent_run_domain::batch::PinId> = jobs
        .iter()
        .map(|job| serde_json::from_value(job["pin_id"].clone()).unwrap())
        .collect();
    assert_ne!(pins[0], pins[1]);
    let lookup = socket
        .call("batch_receipt", Some(request.clone()))
        .await
        .unwrap();
    assert_eq!(lookup["found"], true);
    assert_eq!(lookup["receipt"], first["receipt"]);
    let (lookup_status, lookup_output, lookup_error) = cli(
        &broker.home,
        &["batch-receipt", "--spec", spec.to_str().unwrap()],
    )
    .await;
    assert!(lookup_status.success(), "batch receipt CLI: {lookup_error}");
    assert_eq!(
        serde_json::from_str::<Value>(&lookup_output).unwrap(),
        lookup
    );
    let deadline = Instant::now() + Duration::from_secs(30);
    while !quiescent(&broker.home) {
        assert!(Instant::now() < deadline, "fixture agents did not stop");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    let store = agent_run_store::fixture::open(&broker.home).unwrap();
    for id in &ids {
        let row = store.get(id).unwrap();
        assert_eq!(row.status, Status::Succeeded, "{:?}", row.failure_text);
        assert!(
            row.supervisor_pid.is_some(),
            "fixture supervisor must have started"
        );
    }
    drop(store);

    let status_request = json!({"version":1,"expected_store_id":store_id,"include_view":true,"targets":[{
        "agent_id":ids[0],"sequence":1,"execution_ref":execution_refs[0],"pin_id":pins[0]
    }]});
    let status = socket
        .call("status", Some(status_request.clone()))
        .await
        .unwrap();
    assert_eq!(status["items"][0]["match"], "verified");
    assert_eq!(status["view"]["agent"]["agent_id"], ids[0].as_str());
    assert_eq!(status["view"]["agent"]["sequence"], 1);
    assert_eq!(
        status["view"]["agent"]["name"],
        agent_run_domain::agent_name(ids[0].as_str())
    );
    assert_eq!(status["view"]["match"], "verified");
    assert_eq!(status["view"]["status_scope"], "selected_execution_raw");
    assert!(!status.to_string().contains("process_group_id"));
    assert_mcp_refuses(&broker.home, "status", &status_request).await;
    let status_spec = broker.home.join("status.json");
    std::fs::write(&status_spec, status_request.to_string()).unwrap();
    let (code, output, error) = cli(
        &broker.home,
        &["status", "--spec", status_spec.to_str().unwrap()],
    )
    .await;
    assert!(code.success(), "status CLI: {error}");
    assert_eq!(
        serde_json::from_str::<Value>(&output).unwrap()["view"]["agent"]["sequence"],
        1
    );
    let legacy = socket
        .call("status", Some(json!({"agent_id":ids[0]})))
        .await;
    assert!(
        legacy.is_err(),
        "legacy scalar status must never decode as a view"
    );

    // The fixture seam records a handoff error if replay tries to start a supervisor.
    std::fs::write(broker.home.join("fixture-supervisor-spawn-error"), "refuse").unwrap();
    let replay = mcp_call(&broker.home, "start_batch", &request).await;
    assert_ne!(replay["result"]["isError"], true, "{replay}");
    let replay = &replay["result"]["structuredContent"];
    assert_eq!(replay["replayed"], true, "{replay}");
    assert_eq!(replay["receipt"], first["receipt"]);
    let replay_refs: Vec<agent_run_domain::batch::ExecutionRef> = replay["receipt"]["jobs"]
        .as_array()
        .unwrap()
        .iter()
        .map(|job| serde_json::from_value(job["execution_ref"].clone()).unwrap())
        .collect();
    assert_eq!(replay_refs, execution_refs);
    let mut changed = request.clone();
    changed["retain_history"] = json!(false);
    std::fs::write(&spec, changed.to_string()).unwrap();
    let (status, _, error) = cli(&broker.home, &args).await;
    assert_eq!(status.code(), Some(2), "{error}");
    assert_eq!(
        serde_json::from_str::<Value>(&error).unwrap()["error"]["type"],
        "RequestConflict"
    );
    assert_eq!(
        socket.call("batch_receipt", Some(changed)).await.unwrap(),
        json!({"found":false})
    );
    let (status, output, error) = cli(
        &broker.home,
        &["batch-receipt", "--spec", spec.to_str().unwrap()],
    )
    .await;
    assert!(status.success(), "{error}");
    assert_eq!(
        serde_json::from_str::<Value>(&output).unwrap(),
        json!({"found":false})
    );
    assert!(lookup.get("release_token").is_none());
    assert!(replay.get("release_token").is_none());

    // The known public batch idempotency key does not authorize release.
    let old_request = json!({"pin_id":pins[0],"request_id":"socket-batch"});
    let error = socket
        .call("history_pins_release", Some(old_request))
        .await
        .unwrap_err();
    assert_eq!(error.machine_code().as_str(), "ValidationError");
    let wrong = json!({"pin_id":pins[0],"release_token":"a".repeat(64)});
    let error = socket
        .call("history_pins_release", Some(wrong))
        .await
        .unwrap_err();
    assert_eq!(error.machine_code().as_str(), "RequestConflict");
    let credential = std::fs::read_to_string(&token_file).unwrap();
    let credential = credential.trim();
    assert_eq!(credential.len(), 64);
    assert_mcp_refuses(
        &broker.home,
        "history_pins_release",
        &json!({"pin_id":pins[0],"release_token":credential}),
    )
    .await;
    for already in [false, true] {
        let (status, output, error) = cli(
            &broker.home,
            &[
                "history-pins",
                "release",
                "--pin-id",
                pins[0].as_str(),
                "--release-token-file",
                token_file.to_str().unwrap(),
                "--expected-store-id",
                store_id.as_str(),
            ],
        )
        .await;
        assert!(status.success(), "pin release: {error}");
        let release: Value = serde_json::from_str(&output).unwrap();
        assert_eq!(release["pin_id"], pins[0].as_str());
        assert_eq!(release["status"], "released");
        assert_eq!(release["already_released"], already);
    }
    for path in ["state.db", "state.db-wal", "broker.stderr"] {
        if let Ok(contents) = std::fs::read(broker.home.join(path)) {
            assert!(
                !contents
                    .windows(credential.len())
                    .any(|window| window == credential.as_bytes()),
                "{path} contains a release credential"
            );
        }
    }
    let (status, output, error) = cli(
        &broker.home,
        &["history-pins", "list", "--status", "active"],
    )
    .await;
    assert!(status.success(), "pin list: {error}");
    let active: Value = serde_json::from_str(&output).unwrap();
    assert_eq!(active["pins"].as_array().unwrap().len(), 1);
    assert_eq!(active["pins"][0]["pin_id"], pins[1].as_str());
    let missing = agent_run_domain::batch::PinId::new();
    let (status, _, error) = cli(
        &broker.home,
        &[
            "history-pins",
            "release",
            "--pin-id",
            missing.as_str(),
            "--release-token-file",
            token_file.to_str().unwrap(),
        ],
    )
    .await;
    assert_eq!(status.code(), Some(2), "{error}");
    assert_eq!(
        serde_json::from_str::<Value>(&error).unwrap()["error"]["type"],
        "RequestConflict"
    );
    let released_page = socket
        .call("history_pins_list", Some(json!({"status":"released"})))
        .await
        .unwrap();
    assert_eq!(released_page["pins"][0]["pin_id"], pins[0].as_str());
    let removed = socket
        .call("history_pins", Some(json!({"action":"list"})))
        .await
        .unwrap_err();
    assert!(
        removed.to_string().contains("method not found"),
        "{removed}"
    );
    let after_release = mcp_call(&broker.home, "start_batch", &request).await;
    assert_eq!(
        after_release["result"]["structuredContent"]["receipt"],
        first["receipt"]
    );
    assert_eq!(
        after_release["result"]["structuredContent"]["replayed"],
        true
    );
    let lookup_after_release = socket
        .call("batch_receipt", Some(request.clone()))
        .await
        .unwrap();
    assert_eq!(lookup_after_release["receipt"], first["receipt"]);
    let db = read_store(&broker.home);
    for (table, expected) in [
        ("agents", 2),
        ("attempts", 2),
        ("start_batch_receipts", 1),
        ("history_pins", 2),
    ] {
        let count: i64 = db
            .query_row(&format!("SELECT COUNT(*) FROM {table}"), [], |row| {
                row.get(0)
            })
            .unwrap();
        assert_eq!(count, expected, "{table}");
    }
    let released: i64 = db
        .query_row(
            "SELECT COUNT(*) FROM history_pins WHERE released=1",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(released, 1, "batch replay must not revive the released pin");
    let handoff_errors: i64 = db
        .query_row(
            "SELECT COUNT(*) FROM events WHERE kind='supervisor_handoff_error'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(handoff_errors, 0, "replay must not try to launch");
    drop(db);
    let home = broker.home.clone();
    drop(broker);
    assert!(!home.exists(), "owned fixture home was not removed");
}

/// A supervised Claude fixture records the stdout and stderr of its nested
/// CLI command. Only the inherited private descriptor receives the bearer.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn supervised_cli_batch_bearer_stays_out_of_transcript_and_mcp() {
    let broker = Broker::start().await;
    configure(&broker.home, 3);
    let client = BrokerClient::new(broker.home.join("api.sock"));
    let store_id = client
        .call("capabilities", Some(json!({"include_store_id":true})))
        .await
        .unwrap()["store_id"]
        .clone();
    let job = json!({"provider":"glm-user","model":"fixture","profile":"review","explicit_finish":false,
        "task":"fixture:answer","workdir":broker.home});
    let request = json!({"request_id":"nested-retained-canary","expected_store_id":store_id,
        "retain_history":true,"jobs":[job.clone()]});
    let spec = broker.home.join("batch-canary-spec.json");
    std::fs::write(&spec, request.to_string()).unwrap();
    let alias = command(&broker.home);
    let refused = tokio::process::Command::new("/bin/sh")
        .args([
            "-c",
            r#"exec "$1" --home "$2" start-batch --spec "$3" --release-token-fd 3 3>&1"#,
            "alias-canary",
            env!("CARGO_BIN_EXE_agent-run"),
        ])
        .arg(&broker.home)
        .arg(&spec)
        .env_clear()
        .envs(
            alias
                .get_envs()
                .filter_map(|(key, value)| value.map(|value| (key, value))),
        )
        .output()
        .await
        .unwrap();
    assert_eq!(refused.status.code(), Some(2));
    assert!(String::from_utf8_lossy(&refused.stderr).contains("separate from standard streams"));
    assert_eq!(
        client
            .call("batch_receipt", Some(request.clone()))
            .await
            .unwrap(),
        json!({"found":false})
    );
    let exposed = tokio::process::Command::new("/bin/sh")
        .args([
            "-c",
            r#"umask 022; exec "$1" --home "$2" start-batch --spec "$3" --release-token-fd 3 3>"$4""#,
            "fd-permissions-canary",
            env!("CARGO_BIN_EXE_agent-run"),
        ])
        .arg(&broker.home)
        .arg(&spec)
        .arg(broker.home.join("public-token-fd"))
        .env_clear()
        .envs(alias.get_envs().filter_map(|(key, value)| value.map(|value| (key, value))))
        .output().await.unwrap();
    use std::os::unix::fs::PermissionsExt;
    let exposed_file = std::fs::metadata(broker.home.join("public-token-fd")).unwrap();
    assert_eq!(exposed_file.permissions().mode() & 0o777, 0o644);
    assert_eq!(exposed_file.len(), 0);
    assert_eq!(exposed.status.code(), Some(2));
    assert!(String::from_utf8_lossy(&exposed.stderr).contains("Use --release-token-file"));
    assert_eq!(
        client
            .call("batch_receipt", Some(request.clone()))
            .await
            .unwrap(),
        json!({"found":false})
    );
    std::fs::write(
        broker.home.join("batch-canary-cli"),
        env!("CARGO_BIN_EXE_agent-run"),
    )
    .unwrap();

    let mut outer = job.clone();
    outer["task"] = json!("fixture:batch-token-canary");
    let launched = client.call("start", Some(outer)).await.unwrap();
    let outer_id = launched["agent_id"].as_str().unwrap();
    let deadline = Instant::now() + Duration::from_secs(30);
    while !quiescent(&broker.home) {
        assert!(
            Instant::now() < deadline,
            "nested batch fixture did not stop"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert_eq!(
        std::fs::metadata(broker.home.join("batch-canary-token"))
            .unwrap()
            .permissions()
            .mode()
            & 0o777,
        0o600
    );
    let token = std::fs::read_to_string(broker.home.join("batch-canary-token")).unwrap();
    let token = token.trim();
    assert_eq!(token.len(), 64);
    let journal: String = read_store(&broker.home).query_row(
        "SELECT group_concat(content, char(10)) FROM messages WHERE agent_id=? AND role='tool_result'",
        [outer_id], |row| row.get(0),
    ).unwrap();
    assert!(journal.contains("nested-retained-canary"));
    assert!(!journal.contains(token) && !journal.contains("release_token"));
    let transcript = client
        .call("transcript", Some(json!({"agent_id":outer_id})))
        .await
        .unwrap()
        .to_string();
    assert!(transcript.contains("nested-retained-canary"));
    assert!(!transcript.contains(token) && !transcript.contains("release_token"));

    let lookup = client
        .call("batch_receipt", Some(request.clone()))
        .await
        .unwrap();
    let pin = &lookup["receipt"]["jobs"][0]["pin_id"];
    let released = client
        .call(
            "history_pins_release",
            Some(json!({
                "pin_id":pin,"release_token":token,"expected_store_id":store_id
            })),
        )
        .await
        .unwrap();
    assert_eq!(released["status"], "released");

    let mut discard = request.clone();
    discard["request_id"] = json!("cli-discarded-canary");
    std::fs::write(&spec, discard.to_string()).unwrap();
    let (status, stdout, stderr) = cli(
        &broker.home,
        &["start-batch", "--spec", spec.to_str().unwrap()],
    )
    .await;
    assert!(status.success());
    assert!(stderr.is_empty());
    let output: Value = serde_json::from_str(&stdout).unwrap();
    assert_eq!(output["receipt"]["request_id"], "cli-discarded-canary");
    assert_eq!(output["replayed"], false);
    assert!(output.get("release_token").is_none());

    let mut mcp_request = request;
    mcp_request["request_id"] = json!("mcp-retained-canary");
    mcp_request["jobs"][0]["orchestrator"] = json!({
        "transport": "codex_queue",
        "external_session_id": "mcp-owner-session"
    });
    let mcp = mcp_call(&broker.home, "start_batch", &mcp_request).await;
    assert_eq!(mcp["result"]["isError"], false);
    assert_eq!(
        mcp["result"]["structuredContent"]["receipt"]["request_id"],
        "mcp-retained-canary"
    );
    assert_eq!(mcp["result"]["structuredContent"]["replayed"], false);
    assert!(!mcp.to_string().contains("release_token"));
    let store = agent_run_store::Store::open(&broker.home).unwrap();
    let owner: Option<u32> = store.conn.query_row(
        "SELECT owner_uid FROM orchestrator_sessions WHERE transport='codex_queue' AND external_session_id='mcp-owner-session'",
        [],
        |row| row.get(0),
    ).unwrap();
    use std::os::unix::fs::MetadataExt;
    assert_eq!(owner, Some(std::fs::metadata(&broker.home).unwrap().uid()));
}

/// The local owner can recover a public legacy pin offline, with an exact
/// store and receipt match. Durable managed enrollments remain Pool-owned.
#[tokio::test]
async fn legacy_pin_recovery_requires_offline_owner_and_exact_receipt() {
    use agent_run_domain::batch::PinId;
    let broker = Broker::start().await;
    let store_id = BrokerClient::new(broker.home.join("api.sock"))
        .call("capabilities", Some(json!({"include_store_id":true})))
        .await
        .unwrap()["store_id"]
        .as_str()
        .unwrap()
        .to_owned();
    let public_pin = PinId::new();
    let managed_pin = PinId::new();
    let args = |pin: &PinId, request_id: &str| {
        [
            "history-pins",
            "release-offline",
            "--pin-id",
            pin.as_str(),
            "--request-id",
            request_id,
            "--expected-store-id",
            &store_id,
        ]
        .map(str::to_owned)
    };
    let live_args = args(&public_pin, "old-public");
    let (status, _, error) = cli(
        &broker.home,
        &live_args.iter().map(String::as_str).collect::<Vec<_>>(),
    )
    .await;
    assert!(!status.success());
    assert!(error.contains("stop the resident broker"), "{error}");
    drop(broker);

    let temp = tempfile::tempdir().unwrap();
    let home = temp.path();
    std::fs::create_dir(home.join("tmp")).unwrap();
    let (status, _, error) = cli(home, &["init"]).await;
    assert!(status.success(), "{error}");
    let db = agent_run_store::fixture::open(home).unwrap();
    let store_id = agent_run_store::store_identity::read(&db.conn)
        .unwrap()
        .to_string();
    let public_root = AgentId::new();
    let managed_root = AgentId::new();
    for (pin, key, root) in [
        (&public_pin, "old-public", &public_root),
        (&managed_pin, "old-managed", &managed_root),
    ] {
        db.conn
            .execute(
                "INSERT INTO history_pins(pin_id,root_agent_id) VALUES(?,?)",
                (pin.as_str(), root.as_str()),
            )
            .unwrap();
        db.conn.execute(
            "INSERT INTO start_batch_receipts(request_id,fingerprint_version,fingerprint,receipt_json) VALUES(?,1,?,?)",
            (key, "a".repeat(64), json!({"jobs":[{"pin_id":pin}]}).to_string()),
        ).unwrap();
    }
    db.conn.execute(
        "INSERT INTO managed_enrollments(pool_route_json,pool_store_id,request_id,fingerprint_version,fingerprint,state,grant_json,receipt_id,receipt_json)
         VALUES('{}','pool','owned',1,?,'admitted','{}','receipt',?)",
        ("a".repeat(64), json!({"jobs":[{"pin_id":managed_pin}]}).to_string()),
    ).unwrap();
    db.conn.execute(
        "INSERT INTO managed_enrollment_members(lineage_root_id,pool_route_json,pool_store_id,pool_id,request_id,member_id,member_index)
         VALUES(?,'{}','pool','fixture','owned','member',0)",
        [managed_root.as_str()],
    ).unwrap();
    drop(db);
    let release = |pin, key| {
        let store_id = &store_id;
        async move {
            cli(
                home,
                &[
                    "history-pins",
                    "release-offline",
                    "--pin-id",
                    pin,
                    "--request-id",
                    key,
                    "--expected-store-id",
                    store_id,
                ],
            )
            .await
        }
    };
    for (pin, key) in [
        (&public_pin, "foreign"),
        (&managed_pin, "old-public"),
        (&managed_pin, "old-managed"),
        (&managed_pin, "p3_managed"),
    ] {
        let (status, _, _) = release(pin.as_str(), key).await;
        assert!(!status.success());
    }
    for already in [false, true] {
        let (status, output, error) = release(public_pin.as_str(), "old-public").await;
        assert!(status.success(), "{error}");
        assert_eq!(
            serde_json::from_str::<Value>(&output).unwrap()["already_released"],
            already
        );
    }
}

/// A key or pin from one database cannot replay, admit, list or release in another.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn store_identity_fences_batch_and_pin_across_distinct_homes() {
    let a = Broker::start().await;
    let b = Broker::start().await;
    configure(&a.home, 3);
    configure(&b.home, 3);
    let client_a = BrokerClient::new(a.home.join("api.sock"));
    let client_b = BrokerClient::new(b.home.join("api.sock"));
    let id = |value: Value| value["store_id"].as_str().unwrap().to_owned();
    let store_a = id(client_a
        .call("capabilities", Some(json!({"include_store_id":true})))
        .await
        .unwrap());
    let store_b = id(client_b
        .call("capabilities", Some(json!({"include_store_id":true})))
        .await
        .unwrap());
    assert_ne!(store_a, store_b);
    let job = |home: &Path| json!({"provider":"glm-user","model":"fixture","profile":"review","explicit_finish":false,"task":"fixture:answer","workdir":home});
    let request_a = json!({"request_id":"shared-key","retain_history":true,"expected_store_id":store_a,"jobs":[job(&a.home)]});
    let request_b = json!({"request_id":"shared-key","retain_history":true,"jobs":[job(&b.home)]});
    let receipt_a = client_a
        .call("start_batch", Some(request_a.clone()))
        .await
        .unwrap();
    let receipt_b = client_b
        .call("start_batch", Some(request_b.clone()))
        .await
        .unwrap();
    assert_eq!(receipt_a["receipt"]["store_id"], store_a);
    assert_eq!(receipt_b["receipt"]["store_id"], store_b);
    let pin_a = receipt_a["receipt"]["jobs"][0]["pin_id"].as_str().unwrap();
    let pin_b = receipt_b["receipt"]["jobs"][0]["pin_id"].as_str().unwrap();
    let error = client_b
        .call("batch_receipt", Some(request_a.clone()))
        .await
        .unwrap_err();
    assert_eq!(error.machine_code().as_str(), "StoreIdentityMismatch");
    let spec = b.home.join("foreign-batch.json");
    std::fs::write(&spec, request_a.to_string()).unwrap();
    let (status, _, error) = cli(
        &b.home,
        &["batch-receipt", "--spec", spec.to_str().unwrap()],
    )
    .await;
    assert_eq!(status.code(), Some(2), "{error}");
    assert_eq!(
        serde_json::from_str::<Value>(&error).unwrap()["error"]["type"],
        "StoreIdentityMismatch"
    );
    let error = client_b
        .call("start_batch", Some(request_a.clone()))
        .await
        .unwrap_err();
    assert_eq!(error.machine_code().as_str(), "StoreIdentityMismatch");
    let mut new_key = request_a.clone();
    new_key["request_id"] = json!("new-key");
    let error = client_b
        .call("start_batch", Some(new_key))
        .await
        .unwrap_err();
    assert_eq!(error.machine_code().as_str(), "StoreIdentityMismatch");
    let error = client_b
        .call(
            "history_pins_release",
            Some(json!({"pin_id":pin_a,"release_token":receipt_a["release_token"],"expected_store_id":store_a})),
        )
        .await
        .unwrap_err();
    assert_eq!(error.machine_code().as_str(), "StoreIdentityMismatch");
    let error = client_b
        .call(
            "history_pins_list",
            Some(json!({"expected_store_id":store_a})),
        )
        .await
        .unwrap_err();
    assert_eq!(error.machine_code().as_str(), "StoreIdentityMismatch");
    let page = client_b
        .call(
            "history_pins_list",
            Some(json!({"status":"active","expected_store_id":store_b})),
        )
        .await
        .unwrap();
    assert_eq!(page["pins"].as_array().unwrap().len(), 1);
    assert_eq!(page["pins"][0]["pin_id"], pin_b);
    let replay = client_a.call("start_batch", Some(request_a)).await.unwrap();
    assert_eq!(replay["replayed"], true);
    assert_eq!(replay["receipt"], receipt_a["receipt"]);
    let db = read_store(&b.home);
    for table in ["agents", "attempts", "start_batch_receipts", "history_pins"] {
        let count: i64 = db
            .query_row(&format!("SELECT COUNT(*) FROM {table}"), [], |row| {
                row.get(0)
            })
            .unwrap();
        assert_eq!(count, 1, "{table} must have no foreign admission");
    }
    drop(db);
    let deadline = Instant::now() + Duration::from_secs(30);
    while !(quiescent(&a.home) && quiescent(&b.home)) {
        assert!(Instant::now() < deadline, "synthetic jobs did not stop");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

/// Real socket, CLI and MCP retain the exact prerequisite receipt and closed
/// refusal reason. Replaying a committed batch never rechecks a released pin.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn batch_preconditions_cross_all_public_transports() {
    let broker = Broker::start().await;
    let socket = BrokerClient::new(broker.home.join("api.sock"));
    let store_id = socket
        .call("capabilities", Some(json!({"include_store_id":true})))
        .await
        .unwrap()["store_id"]
        .clone();
    let job = json!({"provider":"glm-user","model":"fixture","profile":"review","explicit_finish":false,"task":"fixture:answer","workdir":broker.home.clone()});
    let seed = json!({"request_id":"precondition-seed","expected_store_id":store_id.clone(),"retain_history":true,"jobs":[job.clone()]});
    let seed_result = socket.call("start_batch", Some(seed)).await.unwrap();
    let previous = &seed_result["receipt"]["jobs"][0];
    let condition = json!({"agent_id":previous["agent_id"],"sequence":previous["sequence"],
        "execution_ref":previous["execution_ref"],"pin_id":previous["pin_id"]});
    let deadline = Instant::now() + Duration::from_secs(30);
    while !quiescent(&broker.home) {
        assert!(Instant::now() < deadline, "seed fixture did not stop");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    let dependent = json!({"request_id":"precondition-child","expected_store_id":store_id,
        "preconditions":[condition.clone()],"jobs":[job]});
    let spec = broker.home.join("precondition.json");
    std::fs::write(&spec, dependent.to_string()).unwrap();
    let (status, stdout, stderr) = cli(
        &broker.home,
        &["start-batch", "--spec", spec.to_str().unwrap()],
    )
    .await;
    assert!(status.success(), "{stderr}");
    let admitted: Value = serde_json::from_str(&stdout).unwrap();
    assert_eq!(
        admitted["receipt"]["preconditions"],
        dependent["preconditions"]
    );
    let lookup = socket
        .call("batch_receipt", Some(dependent.clone()))
        .await
        .unwrap();
    assert_eq!(lookup["receipt"], admitted["receipt"]);
    let deadline = Instant::now() + Duration::from_secs(30);
    while !quiescent(&broker.home) {
        assert!(Instant::now() < deadline, "dependent fixture did not stop");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    socket.call("history_pins_release", Some(json!({"pin_id":condition["pin_id"],"release_token":seed_result["release_token"],"expected_store_id":store_id}))).await.unwrap();
    let replay = socket
        .call("start_batch", Some(dependent.clone()))
        .await
        .unwrap();
    assert_eq!(replay["replayed"], true);
    assert_eq!(replay["receipt"], admitted["receipt"]);

    for (label, reason, prereq) in [
        ("released", "not_met", condition.clone()),
        (
            "missing",
            "unknown",
            json!({"agent_id":"ag-20260826-120000-fffffffffe","sequence":1,
            "execution_ref":format!("ex1_{}","a".repeat(64)),"pin_id":"pin_550e8400e29b41d4a716446655440099"}),
        ),
    ] {
        let mut request = dependent.clone();
        request["request_id"] = json!(format!("precondition-{label}"));
        request["preconditions"] = json!([prereq]);
        std::fs::write(&spec, request.to_string()).unwrap();
        let (status, stdout, stderr) = cli(
            &broker.home,
            &["start-batch", "--spec", spec.to_str().unwrap()],
        )
        .await;
        assert_eq!(status.code(), Some(2), "{label}: {stderr}");
        assert!(stdout.is_empty());
        let error: Value = serde_json::from_str(&stderr).unwrap();
        assert_eq!(error["error"]["type"], "PreconditionFailed");
        assert_eq!(error["error"]["reason"], reason);
        let error = socket
            .call("start_batch", Some(request.clone()))
            .await
            .unwrap_err();
        assert_eq!(error.machine_code().as_str(), "PreconditionFailed");
        assert_eq!(serde_json::to_value(error.public().reason).unwrap(), reason);
        let mcp = mcp_call(&broker.home, "start_batch", &request).await;
        assert_eq!(mcp["result"]["isError"], true);
        assert_eq!(
            mcp["result"]["structuredContent"],
            json!({"code":"PreconditionFailed","reason":reason})
        );
        let mut stream = tokio::net::UnixStream::connect(broker.home.join("api.sock"))
            .await
            .unwrap();
        stream
            .write_all(
                format!(
                    "{}\n",
                    json!({"jsonrpc":"2.0","id":19,"method":"start_batch","params":request})
                )
                .as_bytes(),
            )
            .await
            .unwrap();
        let mut input = tokio::io::BufReader::new(stream);
        let mut line = String::new();
        tokio::time::timeout(Duration::from_secs(5), input.read_line(&mut line))
            .await
            .unwrap()
            .unwrap();
        let response: Value = serde_json::from_str(&line).unwrap();
        assert_eq!(response["error"]["data"]["code"], "PreconditionFailed");
        assert_eq!(response["error"]["data"]["reason"], reason);
    }
    let store = read_store(&broker.home);
    let count: i64 = store
        .query_row("SELECT COUNT(*) FROM start_batch_receipts", [], |row| {
            row.get(0)
        })
        .unwrap();
    assert_eq!(count, 2, "refused requests created a receipt");
}

/// Real socket export (refused by orchestrator MCP) preserves a pinned fake-engine transcript, reconstructs
/// escaped UTF-8 chunks, and refuses wrong stores and released pins without data.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn transcript_exact_crosses_live_socket_and_mcp_with_pin_fences() {
    use agent_run_domain::transcript_exact::{TranscriptExactRequest, TranscriptExactResponse};

    let broker = Broker::start().await;
    let socket = BrokerClient::new(broker.home.join("api.sock"));
    let store_id = socket
        .call("capabilities", Some(json!({"include_store_id":true})))
        .await
        .unwrap()["store_id"]
        .clone();
    let admitted = socket.call("start_batch", Some(json!({
        "request_id":"exact-transcript-seed","expected_store_id":store_id,
        "retain_history":true,"jobs":[{"provider":"glm-user","model":"fixture",
        "profile":"review","explicit_finish":false,"task":"fixture:answer","workdir":broker.home}]
    }))).await.unwrap();
    let receipt = &admitted["receipt"]["jobs"][0];
    let selector = json!({"agent_id":receipt["agent_id"],"sequence":receipt["sequence"],
        "execution_ref":receipt["execution_ref"],"pin_id":receipt["pin_id"]});
    let deadline = Instant::now() + Duration::from_secs(30);
    while !quiescent(&broker.home) {
        assert!(
            Instant::now() < deadline,
            "exact-export fake engine did not stop"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    let id: AgentId = serde_json::from_value(receipt["agent_id"].clone()).unwrap();
    let expected = {
        let store = agent_run_store::fixture::open(&broker.home).unwrap();
        assert_eq!(store.get(&id).unwrap().status, Status::Succeeded);
        // This fixture row fits the real 32 KiB inline limit, but escaping makes
        // its serialized message span chunks. Native fake-engine rows remain intact.
        let body = "€\n\"\\".repeat(4096);
        store.message(&id, "assistant", &body, None, None).unwrap();
        let mut query = store
            .conn
            .prepare("SELECT CAST(seq AS TEXT),content FROM messages WHERE agent_id=? ORDER BY seq")
            .unwrap();
        query
            .query_map([id.as_str()], |row| {
                Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
            })
            .unwrap()
            .collect::<rusqlite::Result<Vec<_>>>()
            .unwrap()
    };
    assert!(
        expected.len() >= 2,
        "must include real fake-engine history and the chunk fixture"
    );
    let initial = json!({"version":1,"expected_store_id":store_id,"selector":selector,"cursor":"0","offset":0});
    let mut request: TranscriptExactRequest = serde_json::from_value(initial.clone()).unwrap();
    let mut assembled = String::new();
    let mut recovered = Vec::new();
    let mut chunks = 0;
    let mut current_hash = None;
    loop {
        assert!(chunks < 100, "export must make bounded progress");
        let params = serde_json::to_value(&request).unwrap();
        let value = socket
            .call("transcript_exact", Some(params.clone()))
            .await
            .unwrap();
        if chunks == 0 {
            assert_mcp_refuses(&broker.home, "transcript_exact", &params).await;
        }
        assert_eq!(value["store_id"], store_id);
        assert_eq!(value["selector"], selector);
        let page: TranscriptExactResponse = serde_json::from_value(value).unwrap();
        page.validate_for(&request).unwrap();
        let fragment = page
            .message
            .as_ref()
            .expect("fixture transcript is nonempty");
        if request.offset == 0 {
            assert!(assembled.is_empty());
            current_hash = Some(fragment.sha256.clone());
        }
        assert_eq!(current_hash.as_ref(), Some(&fragment.sha256));
        assert_eq!(fragment.start, assembled.len());
        assembled.push_str(&fragment.text);
        chunks += 1;
        if fragment.complete {
            assert_eq!(assembled.len(), fragment.total_bytes);
            assert_eq!(
                agent_run_platform::fs::sha256(assembled.as_bytes()),
                fragment.sha256.as_str()
            );
            let message: Value = serde_json::from_str(&assembled).unwrap();
            assert_eq!(message["seq"], fragment.seq);
            assert_eq!(message["content_complete"], true);
            recovered.push((
                fragment.seq.clone(),
                message["content"].as_str().unwrap().to_owned(),
            ));
            assembled.clear();
        }
        request.upper_seq = Some(page.upper_seq);
        request.cursor = page.next_cursor;
        request.offset = page.next_offset;
        request.expected_sha256 = (page.next_offset != 0).then(|| fragment.sha256.clone());
        if page.snapshot_complete {
            break;
        }
    }
    assert_eq!(recovered, expected);
    assert!(
        chunks > recovered.len(),
        "at least one message must cross a chunk boundary"
    );
    let spec = broker.home.join("transcript-exact.json");
    std::fs::write(&spec, initial.to_string()).unwrap();
    let (status, output, error) = cli(
        &broker.home,
        &["transcript-exact", "--spec", spec.to_str().unwrap()],
    )
    .await;
    assert!(status.success(), "exact transcript CLI: {error}");
    let cli_page: TranscriptExactResponse = serde_json::from_str(&output).unwrap();
    cli_page
        .validate_for(&serde_json::from_value(initial.clone()).unwrap())
        .unwrap();

    let mut wrong_store = initial.clone();
    wrong_store["expected_store_id"] = json!("store_00000000000000000000000000000000");
    assert_ne!(wrong_store["expected_store_id"], store_id);
    let wrong = socket
        .call("transcript_exact", Some(wrong_store.clone()))
        .await
        .unwrap_err();
    assert_eq!(wrong.machine_code().as_str(), "StoreIdentityMismatch");

    socket.call("history_pins_release", Some(json!({"expected_store_id":store_id,"release_token":admitted["release_token"],"pin_id":selector["pin_id"]}))).await.unwrap();
    let continuation = serde_json::to_value(request).unwrap();
    let released = socket
        .call("transcript_exact", Some(continuation.clone()))
        .await
        .unwrap_err();
    assert_eq!(released.machine_code().as_str(), "TranscriptExportAborted");
}

/// A real broker admits, recovers and releases a hold without persisting its proof.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn lineage_hold_socket_keeps_proof_private_and_replays_safely() {
    use agent_run_domain::{
        canonical::hex_digest,
        lineage_hold::{HoldId, LineageHoldAcquire, release_signed_bytes},
        lineage_hold_wire::LineageHoldAcquireRequest,
    };
    use ed25519_dalek::{Signer, SigningKey};

    let broker = Broker::start().await;
    let socket = BrokerClient::new(broker.home.join("api.sock"));
    let capabilities = socket
        .call(
            "capabilities",
            Some(json!({"include_store_id":true,"include_lineage_holds":true})),
        )
        .await
        .unwrap();
    assert_eq!(capabilities["lineage_holds"]["version"], 1);
    assert_eq!(
        capabilities["lineage_holds"]["transport"],
        "operator_socket"
    );
    let store_id = capabilities["store_id"].clone();
    let start = json!({"request_id":"hold-seed","expected_store_id":store_id,
        "retain_history":true,"jobs":[{"provider":"glm-user","model":"fixture",
        "profile":"review","explicit_finish":false,"task":"fixture:answer","workdir":broker.home}]});
    let admitted = socket.call("start_batch", Some(start)).await.unwrap();
    let job = &admitted["receipt"]["jobs"][0];
    let target = json!({"agent_id":job["agent_id"],"sequence":job["sequence"],
        "execution_ref":job["execution_ref"],"pin_id":job["pin_id"]});
    let deadline = Instant::now() + Duration::from_secs(30);
    while !quiescent(&broker.home) {
        assert!(Instant::now() < deadline, "fixture agent did not stop");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    let status = socket
        .call(
            "status",
            Some(json!({"version":1,"expected_store_id":store_id,
        "targets":[target.clone()]})),
        )
        .await
        .unwrap();
    assert_eq!(status["items"][0]["success_commit"], "recorded");
    assert_eq!(status["items"][0]["cleanup"]["state"], "confirmed");

    let key = SigningKey::from_bytes(&[7; 32]);
    let acquire = json!({"version":1,"expected_store_id":store_id,
        "request_id":"hold-socket","targets":[target],
        "verifying_key":hex_digest(&key.verifying_key().to_bytes())});
    let parsed: LineageHoldAcquireRequest = serde_json::from_value(acquire.clone()).unwrap();
    let intent: LineageHoldAcquire = parsed.try_into().unwrap();
    let fingerprint = intent.fingerprint().unwrap();
    let lookup = json!({"version":1,"expected_store_id":store_id,
        "request_id":"hold-socket","fingerprint":hex_digest(&fingerprint)});
    assert_eq!(
        socket
            .call("_lineage_hold_lookup", Some(lookup.clone()))
            .await
            .unwrap()["found"],
        false
    );

    // Notifications cannot create a hold even when their contents are valid.
    let mut notification = acquire.clone();
    notification["request_id"] = json!("hold-notification");
    let parsed: LineageHoldAcquireRequest = serde_json::from_value(notification.clone()).unwrap();
    let intent: LineageHoldAcquire = parsed.try_into().unwrap();
    let notification_lookup = json!({"version":1,"expected_store_id":store_id,
        "request_id":"hold-notification","fingerprint":hex_digest(&intent.fingerprint().unwrap())});
    let mut raw = tokio::net::UnixStream::connect(broker.home.join("api.sock"))
        .await
        .unwrap();
    raw.write_all(format!("{}\n{}\n",
        json!({"jsonrpc":"2.0","method":"_lineage_hold_acquire","params":notification}),
        json!({"jsonrpc":"2.0","id":1,"method":"_lineage_hold_lookup","params":notification_lookup}),
    ).as_bytes()).await.unwrap();
    let mut raw_reader = tokio::io::BufReader::new(raw);
    let mut line = String::new();
    tokio::time::timeout(Duration::from_secs(10), raw_reader.read_line(&mut line))
        .await
        .unwrap()
        .unwrap();
    let notification_result: Value = serde_json::from_str(&line).unwrap();
    assert_eq!(notification_result["id"], 1);
    assert_eq!(notification_result["result"]["found"], false);

    let acquired = socket
        .call("_lineage_hold_acquire", Some(acquire.clone()))
        .await
        .unwrap();
    assert_eq!(acquired["state"], "active");
    assert_eq!(acquired["replayed"], false);
    assert_eq!(acquired["receipt"]["fingerprint"], hex_digest(&fingerprint));
    let hold_id: HoldId = serde_json::from_value(acquired["receipt"]["hold_id"].clone()).unwrap();
    assert_eq!(
        socket
            .call("_lineage_hold_lookup", Some(lookup.clone()))
            .await
            .unwrap()["receipt"],
        acquired["receipt"]
    );
    let page = socket
        .call(
            "_lineage_hold_list",
            Some(json!({"version":1,
        "expected_store_id":store_id,"state":"active","limit":100,"after_hold_id":null})),
        )
        .await
        .unwrap();
    assert_eq!(page["holds"].as_array().unwrap().len(), 1);
    assert!(
        socket
            .call(
                "history_pins_release",
                Some(json!({"release_token":admitted["release_token"],
        "expected_store_id":store_id,"pin_id":job["pin_id"]}))
            )
            .await
            .is_err()
    );

    let message = release_signed_bytes(
        &serde_json::from_value(store_id.clone()).unwrap(),
        &hold_id,
        &fingerprint,
    )
    .unwrap();
    let signature = key.sign(&message).to_bytes();
    let signature_hex = hex_digest(&signature);
    let bad_release = json!({"version":1,"expected_store_id":store_id,
        "hold_id":hold_id,"signature":"00".repeat(64)});
    let refusal = socket
        .call("_lineage_hold_release", Some(bad_release.clone()))
        .await
        .unwrap_err();
    assert!(!refusal.to_string().contains(&signature_hex));
    assert!(
        mcp_call(&broker.home, "_lineage_hold_release", &bad_release).await["error"].is_object()
    );
    let mut good_release = bad_release.clone();
    good_release["signature"] = json!(signature_hex);
    let released = socket
        .call("_lineage_hold_release", Some(good_release.clone()))
        .await
        .unwrap();
    assert_eq!(released["found"], true);
    assert_eq!(released["state"], "released");
    assert_eq!(released["already_released"], false);
    let repeat = socket
        .call("_lineage_hold_release", Some(good_release))
        .await
        .unwrap();
    assert_eq!(repeat["already_released"], true);
    assert!(
        socket
            .call("_lineage_hold_release", Some(bad_release))
            .await
            .is_err()
    );
    let replay = socket
        .call("_lineage_hold_acquire", Some(acquire))
        .await
        .unwrap();
    assert_eq!(replay["replayed"], true);
    assert_eq!(replay["state"], "released");
    assert_eq!(replay["receipt"], acquired["receipt"]);
    assert_eq!(
        socket
            .call("_lineage_hold_lookup", Some(lookup))
            .await
            .unwrap()["state"],
        "released"
    );
    socket
        .call(
            "history_pins_release",
            Some(json!({"release_token":admitted["release_token"],
        "expected_store_id":store_id,"pin_id":job["pin_id"]})),
        )
        .await
        .unwrap();

    let decimal_signature = serde_json::to_vec(&signature.to_vec()).unwrap();
    for path in ["state.db", "state.db-wal", "broker.stderr"] {
        if let Ok(contents) = std::fs::read(broker.home.join(path)) {
            assert!(
                !contents
                    .windows(signature.len())
                    .any(|window| window == signature)
            );
            assert!(
                !contents
                    .windows(signature_hex.len())
                    .any(|window| window == signature_hex.as_bytes())
            );
            assert!(
                !contents
                    .windows(decimal_signature.len())
                    .any(|window| window == decimal_signature)
            );
        }
    }
}

/// Exact initial task pages agree across live socket and CLI, fail closed, and
/// are refused by orchestrator MCP.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn initial_task_crosses_live_socket_cli_and_mcp_in_both_modes() {
    use agent_run_domain::initial_task::{
        InitialTaskPage, InitialTaskRequest, InitialTaskResponse,
    };

    let broker = Broker::start().await;
    let socket = BrokerClient::new(broker.home.join("api.sock"));
    let capabilities = socket
        .call(
            "capabilities",
            Some(json!({"include_store_id":true,"include_initial_task":true})),
        )
        .await
        .unwrap();
    let store_id = capabilities["store_id"].clone();
    assert_eq!(capabilities["initial_task"]["method"], "initial_task");
    assert_eq!(
        capabilities["initial_task"]["modes"],
        json!(["strict", "current"])
    );
    assert!(
        capabilities["read_methods"]
            .as_array()
            .unwrap()
            .iter()
            .any(|method| method["name"] == "initial_task")
    );
    let default = socket.call("capabilities", Some(json!({}))).await.unwrap();
    assert!(
        default.get("initial_task").is_none(),
        "support flag is opt-in"
    );
    assert_mcp_refuses(
        &broker.home,
        "capabilities",
        &json!({"include_initial_task":true}),
    )
    .await;
    let (status, output, error) =
        cli(&broker.home, &["capabilities", "--include-initial-task"]).await;
    assert!(status.success(), "capabilities CLI: {error}");
    assert_eq!(
        serde_json::from_str::<Value>(&output).unwrap()["initial_task"],
        capabilities["initial_task"]
    );

    // Multi-byte text over two default pages; the filler keeps the fake-engine prefix.
    let task = format!("fixture:answer\n{}", "Задача €🦀\n\"\\".repeat(6000));
    assert!(task.len() > 65_536);
    let admitted = socket
        .call(
            "start_batch",
            Some(json!({
                "request_id":"initial-task-seed","expected_store_id":store_id,
                "retain_history":true,"jobs":[{"provider":"glm-user","model":"fixture",
                "profile":"review","explicit_finish":false,"task":task,"workdir":broker.home}]
            })),
        )
        .await
        .unwrap();
    let receipt = &admitted["receipt"]["jobs"][0];
    let deadline = Instant::now() + Duration::from_secs(30);
    while !quiescent(&broker.home) {
        assert!(
            Instant::now() < deadline,
            "initial-task fake engine did not stop"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    let strict = json!({"version":1,"mode":"strict","expected_store_id":store_id,
        "agent_id":receipt["agent_id"],"sequence":1,"execution_ref":receipt["execution_ref"],
        "pin_id":receipt["pin_id"]});
    let current = json!({"version":1,"mode":"current","agent_id":receipt["agent_id"]});

    for base in [&strict, &current] {
        let mut request: InitialTaskRequest = serde_json::from_value(base.clone()).unwrap();
        let mut assembled = String::new();
        let mut pages = 0;
        loop {
            assert!(pages < 20, "paging must make bounded progress");
            let params = serde_json::to_value(&request).unwrap();
            let value = socket
                .call("initial_task", Some(params.clone()))
                .await
                .unwrap();
            if pages == 0 {
                assert_mcp_refuses(&broker.home, "initial_task", &params).await;
            }
            let response: InitialTaskResponse = serde_json::from_value(value.clone()).unwrap();
            response.validate_for(&request).unwrap();
            let InitialTaskResponse::Available(page): InitialTaskResponse = response else {
                panic!("initial task unavailable: {value}");
            };
            let InitialTaskPage {
                text,
                sha256,
                total_bytes,
                next_offset,
                ..
            } = page;
            assert_eq!(page.offset, assembled.len());
            assembled.push_str(&text);
            pages += 1;
            request.expected_sha256 = Some(sha256.clone());
            request.expected_total_bytes = Some(total_bytes);
            match next_offset {
                Some(next) => request.offset = next,
                None => {
                    assert_eq!(assembled, task);
                    assert_eq!(
                        agent_run_platform::fs::sha256(assembled.as_bytes()),
                        sha256.as_str()
                    );
                    break;
                }
            }
        }
        assert!(pages >= 2, "the task must cross a page boundary");
    }
    let spec = broker.home.join("initial-task.json");
    std::fs::write(&spec, strict.to_string()).unwrap();
    let (status, output, error) = cli(
        &broker.home,
        &["initial-task", "--spec", spec.to_str().unwrap()],
    )
    .await;
    assert!(status.success(), "initial task CLI: {error}");
    assert_eq!(
        serde_json::from_str::<Value>(&output).unwrap(),
        socket
            .call("initial_task", Some(strict.clone()))
            .await
            .unwrap()
    );

    // A wrong store is a typed error on every transport; a bad fence is typed unavailable.
    let mut wrong = strict.clone();
    wrong["expected_store_id"] = json!("store_00000000000000000000000000000000");
    let error = socket
        .call("initial_task", Some(wrong.clone()))
        .await
        .unwrap_err();
    assert_eq!(error.machine_code().as_str(), "StoreIdentityMismatch");
    let mut bad_ref = current.clone();
    bad_ref["execution_ref"] = json!(format!("ex1_{}", "0".repeat(64)));
    let refused = socket
        .call("initial_task", Some(bad_ref.clone()))
        .await
        .unwrap();
    assert_eq!(refused["state"], "unavailable");
    assert_eq!(refused["reason"], "execution_ref_mismatch");
    assert!(refused.get("text").is_none());
    let mut bad_sha = current.clone();
    bad_sha["offset"] = json!(4);
    bad_sha["expected_sha256"] = json!("0".repeat(64));
    bad_sha["expected_total_bytes"] = json!(task.len());
    let changed = socket.call("initial_task", Some(bad_sha)).await.unwrap();
    assert_eq!(changed["reason"], "content_changed");

    // Releasing the pin withholds the strict text; current mode makes no pin claim.
    socket
        .call(
            "history_pins_release",
            Some(
                json!({"release_token":admitted["release_token"],"expected_store_id":store_id,"pin_id":receipt["pin_id"]}),
            ),
        )
        .await
        .unwrap();
    let released = socket
        .call("initial_task", Some(strict.clone()))
        .await
        .unwrap();
    assert_eq!(
        (released["state"].as_str(), released["reason"].as_str()),
        (Some("unavailable"), Some("pin_released"))
    );
    let still = socket.call("initial_task", Some(current)).await.unwrap();
    assert_eq!(still["state"], "available");
    assert!(still.get("pin_id").is_none());
}
