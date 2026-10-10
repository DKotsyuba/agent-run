#![cfg(feature = "test-fixtures")]
//! Real public-boundary checks for provider admission: a disposable schema-2
//! home, the real `api serve` broker, the real CLI and MCP child processes,
//! and the exported socket client, all driving the fake engine only.
//! The fixture engine and stale-revision admission seam require `test-fixtures`.

use agent_run::transport::socket::BrokerClient;
use agent_run_domain::{Error, ProviderStartRequest, views::StartResult};
use serde_json::{Value, json};
use std::{
    io::{BufRead, BufReader, Write},
    os::unix::fs::FileTypeExt,
    path::{Path, PathBuf},
    process::{Child, Command, Output, Stdio},
    time::{Duration, Instant},
};

/// One disposable schema-2 home with one Claude Messages provider bound to
/// one fake environment reference, and its real resident broker.
struct Broker {
    /// Owns the short socket directory unless retained as fixture evidence.
    _temp: Option<tempfile::TempDir>,
    /// Home passed to every child process.
    home: PathBuf,
    /// The broker child, terminated on drop.
    child: Child,
}

impl Broker {
    /// Initializes the home through the CLI, writes the provider config,
    /// registers the account and starts `api serve`, waiting for its socket.
    fn start() -> Self {
        Self::start_with(&[])
    }

    /// [`Self::start`] with extra environment for the broker process only
    /// (test-fixtures seams such as `AGENT_RUN_FIXTURE_ALWAYS_STALE`).
    fn start_with(environment: &[(&str, &str)]) -> Self {
        Self::start_with_limit(
            environment,
            agent_run_config::config::Core::default().max_active_agents,
        )
    }

    /// Starts an isolated synthetic broker with a fixture-only active limit;
    /// production configuration, credentials and running jobs are untouched.
    fn start_with_limit(environment: &[(&str, &str)], active_limit: usize) -> Self {
        let base = std::env::var_os("AGENT_RUN_TEST_TMP").unwrap_or_else(|| "/tmp".into());
        let temp = tempfile::Builder::new()
            .prefix("ar-pb-")
            .tempdir_in(base)
            .unwrap();
        let home = temp.path().canonicalize().unwrap();
        assert!(cli(&home, &["init"]).status.success());
        std::fs::write(
            home.join("profiles/review.md"),
            "+++\nrevision = \"1\"\nwrite = false\nnetwork = false\nallow_external_read_roots = false\nskills = []\nmcp = []\nrequired_constraints = []\n+++\nReview safely.\n",
        )
        .unwrap();
        let fixture = env!("CARGO_BIN_EXE_agent-run-fixture");
        std::fs::write(
            home.join("config.toml"),
            format!(
                "schema_version = 2\n[core]\nmax_active_agents = {active_limit}\n[harnesses.codex]\nbinary = \"{fixture}\"\nhome = \"{h}/codex\"\n[harnesses.claude-code]\nbinary = \"{fixture}\"\nhome = \"{h}/claude\"\n[providers.glm-user]\nharness = \"claude-code\"\nconnection = {{ kind = \"custom\", endpoint = \"https://gateway.example/api\", protocol = \"messages\" }}\nauth_family = \"anthropic\"\nlimits_source = \"none\"\n[[providers.glm-user.models]]\nid = \"fixture\"\nnative_model = \"fixture\"\n[[providers.glm-user.bindings]]\nlabel = \"work\"\naccount = \"acct-work\"\n",
                h = home.display()
            ),
        )
        .unwrap();
        let registered = cli(
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
        );
        assert!(registered.status.success(), "{registered:?}");
        let child = Command::new(qualification_binary())
            .arg("--home")
            .arg(&home)
            .args(["api", "serve"])
            .env("FAKE_TOKEN", "synthetic-token")
            .envs(environment.iter().copied())
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .unwrap();
        let deadline = Instant::now() + Duration::from_secs(10);
        while !std::fs::symlink_metadata(home.join("api.sock"))
            .is_ok_and(|meta| meta.file_type().is_socket())
        {
            assert!(Instant::now() < deadline, "broker did not publish api.sock");
            std::thread::sleep(Duration::from_millis(20));
        }
        Self {
            _temp: Some(temp),
            home,
            child,
        }
    }

    /// Retains this synthetic home as qualification evidence even after panic;
    /// only its broker/processes are stopped, never a production home or job.
    fn retain_evidence(&mut self) {
        if let Some(temp) = self._temp.take() {
            let _retained_path = temp.keep();
        }
    }

    /// Restarts only this fixture's unreaped broker child while native fixture
    /// supervisors retain their immutable identities and work. The replacement
    /// must answer a real socket ping within ten seconds; no agent is replayed.
    async fn restart(&mut self) {
        self.child.kill().unwrap();
        self.child.wait().unwrap();
        self.child = Command::new(qualification_binary())
            .arg("--home")
            .arg(&self.home)
            .args(["api", "serve"])
            .env("FAKE_TOKEN", "synthetic-token")
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .unwrap();
        let until = Instant::now() + Duration::from_secs(10);
        loop {
            let client = BrokerClient::new(self.home.join("api.sock"));
            if client.call("ping", None).await.is_ok() {
                break;
            }
            assert!(
                Instant::now() < until,
                "owned replacement broker did not become ready"
            );
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }

    /// Legacy fake-engine start for `task`/`request_id`; explicit false keeps the
    /// pre-callback native-result fixtures meaningful under the new public default.
    fn request(&self, task: &str, request_id: &str) -> ProviderStartRequest {
        serde_json::from_value(json!({
            "provider":"glm-user","model":"fixture","profile":"review","explicit_finish":false,
            "task":task,"workdir":self.home,"request_id":request_id,
        }))
        .unwrap()
    }

    /// Waits (bounded) until the agent reaches a terminal status.
    fn wait_terminal(&self, agent: &str) {
        let deadline = Instant::now() + Duration::from_secs(30);
        loop {
            let store = agent_run::state::Store::open(&self.home).unwrap();
            if store
                .get(&agent.parse().unwrap())
                .unwrap()
                .status
                .terminal()
            {
                return;
            }
            assert!(Instant::now() < deadline, "agent did not finish");
            std::thread::sleep(Duration::from_millis(50));
        }
    }

    /// Asserts the exact public class of a refused start on all three
    /// transports: CLI `error.type`, MCP tool `error.code`, and the socket
    /// client's rendered `PublicError.kind`; ordinary validation decodes to
    /// `Error::Validation`, while domain refusals retain their broker code.
    async fn assert_refused(&self, code: &str) {
        let output = cli(
            &self.home,
            &[
                "start",
                "--provider",
                "glm-user",
                "--model",
                "fixture",
                "--profile",
                "review",
                "--task",
                "fixture:answer",
                "--workdir",
                self.home.to_str().unwrap(),
            ],
        );
        assert_eq!(output.status.code(), Some(2), "{output:?}");
        let error: Value = serde_json::from_slice(&output.stderr).unwrap();
        assert_eq!(error["error"]["type"], code, "CLI: {error}");

        let reply = mcp_tool(
            &self.home,
            "start",
            json!({
                "provider":"glm-user", "model":"fixture", "profile":"review","explicit_finish":false,
                "task":"fixture:answer", "workdir":self.home,
            }),
        )
        .await;
        assert_eq!(reply["id"], 2);
        assert_eq!(reply["result"]["isError"], true, "MCP: {reply}");
        assert!(
            reply["result"]["content"][0]["text"]
                .as_str()
                .is_some_and(|text| text.contains(code)),
            "MCP: {reply}"
        );

        let client = BrokerClient::new(self.home.join("api.sock"));
        let error = client
            .start(&self.request("fixture:answer", &format!("refused-{code}")))
            .await
            .unwrap_err();
        if code == "ValidationError" {
            assert!(matches!(&error, Error::Validation(_)), "socket: {error:?}");
        } else {
            assert!(
                matches!(&error, Error::Broker { broker_error_code: Some(found), .. } if found == code),
                "socket: {error:?}"
            );
        }
        assert_eq!(error.public().kind, code, "socket render");
    }
}

impl Drop for Broker {
    /// Releases an owned load barrier even on panic and gives only this home's
    /// supervisors a finite cleanup interval before reaping its broker child.
    fn drop(&mut self) {
        let _ = std::fs::write(self.home.join("fixture-load-release"), "fixture cleanup");
        let deadline = Instant::now() + Duration::from_secs(10);
        while Instant::now() < deadline {
            let remaining = rusqlite::Connection::open_with_flags(self.home.join("state.db"), rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY).ok().and_then(|s| s.query_row("SELECT (SELECT COUNT(*) FROM agents WHERE status IN ('created','starting','running','cancelling'))+(SELECT COUNT(*) FROM attempts WHERE ownership_active=1)", [], |r|r.get::<_,i64>(0)).ok());
            if remaining == Some(0) {
                break;
            }
            std::thread::sleep(Duration::from_millis(20));
        }
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

/// Selects the compiled candidate by default, or an explicitly supplied absolute
/// existing binary for a comparative qualification. The override affects only
/// this isolated fixture; every invocation still names its synthetic home.
fn qualification_binary() -> std::ffi::OsString {
    match std::env::var_os("AGENT_RUN_QUALIFICATION_BINARY") {
        Some(path) => {
            assert!(Path::new(&path).is_absolute() && Path::new(&path).is_file());
            path
        }
        None => env!("CARGO_BIN_EXE_agent-run").into(),
    }
}

/// Runs the selected real CLI against the explicitly supplied synthetic `home`.
fn cli(home: &Path, args: &[&str]) -> Output {
    Command::new(qualification_binary())
        .arg("--home")
        .arg(home)
        .args(args)
        .output()
        .unwrap()
}

/// Executes one real MCP tool call using the historical 2025-06-18 protocol,
/// bounded I/O and exact child cleanup; modern tests opt in explicitly.
async fn mcp_tool(home: &Path, name: &str, arguments: Value) -> Value {
    mcp_request(
        home,
        "2025-06-18",
        "tools/call",
        json!({"name":name,"arguments":arguments}),
    )
    .await
}

/// Executes one raw SDK request against the fixture broker after initialization.
/// Input/output and exit waits are finite; the exact child is killed/reaped even
/// on an observation timeout. Host Desktop routing is disabled for this fixture.
async fn mcp_request(home: &Path, protocol: &str, method: &str, params: Value) -> Value {
    use tokio::io::{AsyncBufReadExt, AsyncWriteExt};
    let mut child = tokio::process::Command::new(qualification_binary())
        .arg("--home")
        .arg(home)
        .arg("mcp")
        .env_remove("CODEX_MCP_NODE_PATH")
        .env_remove("CODEX_APP_TOOLS_PIPE_PATH")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .kill_on_drop(true)
        .spawn()
        .unwrap();
    let reply = tokio::time::timeout(Duration::from_secs(15), async {
        let mut input = child.stdin.take().unwrap();
        let mut output = tokio::io::BufReader::new(child.stdout.take().unwrap());
        for message in [
            json!({"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":protocol,"capabilities":{},"clientInfo":{"name":"stable-id-test","version":"1"}}}),
            json!({"jsonrpc":"2.0","method":"notifications/initialized"}),
            json!({"jsonrpc":"2.0","id":2,"method":method,"params":params}),
        ] {
            input.write_all(format!("{message}\n").as_bytes()).await.unwrap();
        }
        input.flush().await.unwrap();
        loop {
            let mut line = String::new();
            assert!(output.read_line(&mut line).await.unwrap() > 0);
            let reply: Value = serde_json::from_str(&line).unwrap();
            if reply["id"] == 2 {
                break reply;
            }
        }
    }).await;
    if tokio::time::timeout(Duration::from_secs(3), child.wait())
        .await
        .is_err()
    {
        child.kill().await.unwrap();
        child.wait().await.unwrap();
    }
    reply.expect("MCP call deadline")
}

/// A real broker, public raw MCP admission and an already initialized private
/// worker MCP join one pool without restarting or reserving the existing run.
/// Fixture TTLs bound every child; no provider endpoint or production home runs.
#[tokio::test]
async fn mixed_pool_attaches_active_worker_over_live_mcp_without_restart() {
    let broker = Broker::start();
    let client = BrokerClient::new(broker.home.join("api.sock"));
    let request = broker.request("fixture:pool-enroll", "independent-join");
    let started = client.start(&request).await.unwrap();
    let agent: agent_run_domain::domain::AgentId = started.agent_id.parse().unwrap();
    let until = Instant::now() + Duration::from_secs(10);
    loop {
        let store = agent_run::state::Store::open(&broker.home).unwrap();
        let ready:bool=store.conn.query_row("SELECT EXISTS(SELECT 1 FROM worker_capabilities c JOIN attempts t ON t.id=c.attempt_id WHERE t.agent_id=? AND c.pool_catalog_version=1 AND t.state='running' AND t.ownership_active=1)",[agent.as_str()],|r|r.get(0)).unwrap();
        if ready && broker.home.join("enrollment-worker-ready").exists() {
            break;
        }
        assert!(
            Instant::now() < until,
            "actual worker catalog proof must arrive"
        );
        tokio::time::sleep(Duration::from_millis(30)).await;
    }
    let before = agent_run::state::Store::open(&broker.home)
        .unwrap()
        .get(&agent)
        .unwrap();
    let response=mcp_tool(&broker.home,"start_pool",json!({"request_id":"live-mixed","goal":"Verify mixed pool enrollment","acceptance":[{"id":"goal","text":"Existing work remains intact"}],"members":[{"existing_agent_id":agent,"role":"review"},{"start":{"provider":"glm-user","model":"fixture","profile":"review","explicit_finish":false,"task":"fixture:pool-observe","workdir":broker.home,"display_name":"new helper"},"role":"helper"}]})).await;
    assert_ne!(response["result"]["isError"], true, "{response}");
    assert!(response.get("error").is_none(), "{response}");
    let until = Instant::now() + Duration::from_secs(10);
    loop {
        if broker.home.join("enrollment-acked").exists() {
            break;
        }
        assert!(
            Instant::now() < until,
            "current worker must ACK over private MCP"
        );
        tokio::time::sleep(Duration::from_millis(30)).await;
    }
    let store = agent_run::state::Store::open(&broker.home).unwrap();
    let after = store.get(&agent).unwrap();
    assert_eq!(after.id, before.id);
    assert_eq!(after.request, before.request);
    assert_eq!(after.identity, before.identity);
    assert_eq!(after.runtime_session_id, before.runtime_session_id);
    assert_eq!(after.created_at, before.created_at);
    assert_eq!(
        store
            .conn
            .query_row(
                "SELECT COUNT(*) FROM attempts WHERE agent_id=?",
                [agent.as_str()],
                |r| r.get::<_, i64>(0)
            )
            .unwrap(),
        1
    );
    assert_eq!(
        store
            .conn
            .query_row(
                "SELECT state FROM pool_enrollments WHERE agent_id=?",
                [agent.as_str()],
                |r| r.get::<_, String>(0)
            )
            .unwrap(),
        "joined"
    );
    let until = Instant::now() + Duration::from_secs(10);
    while !broker.home.join("pool-observed.txt").exists() {
        assert!(
            Instant::now() < until,
            "only the new member must launch with the committed roster"
        );
        tokio::time::sleep(Duration::from_millis(30)).await;
    }
    assert!(
        std::fs::read_to_string(broker.home.join("pool-observed.txt"))
            .unwrap()
            .contains(agent.as_str())
    );
    std::fs::write(
        broker.home.join("enrollment-release"),
        "release owned fixture",
    )
    .unwrap();
    broker.wait_terminal(agent.as_str());
}

/// The exported socket client sends the strict provider request the real
/// schema-2 dispatcher accepts without exposing an attempt id, and the
/// real start response decodes as the exported `StartResult`.
#[tokio::test]
async fn broker_client_starts_through_the_real_schema2_dispatcher() {
    let broker = Broker::start();
    let client = BrokerClient::new(broker.home.join("api.sock"));
    let started = client
        .start(&broker.request("fixture:answer", "client-1"))
        .await
        .unwrap();
    assert!(started.created);
    assert!(started.attempt_id.is_none());
    let replay = client
        .start(&broker.request("fixture:answer", "client-1"))
        .await
        .unwrap();
    assert!(!replay.created);
    assert_eq!(replay.agent_id, started.agent_id);
    assert_eq!(replay.attempt_id, started.attempt_id);
    broker.wait_terminal(&started.agent_id);

    // The complete broker start response (the CLI prints only a summary).
    let params = serde_json::to_value(broker.request("fixture:answer", "client-2")).unwrap();
    let response = client.call("start", Some(params)).await.unwrap();
    let typed: StartResult = serde_json::from_value(response.clone()).unwrap();
    assert!(typed.created, "{response}");
    assert_eq!(typed.attempt_id.as_deref(), response["attempt_id"].as_str());
    assert!(typed.attempt_id.is_none());
    broker.wait_terminal(typed.agent_id.as_str());
}

/// Finite fifteen-process cohorts cover dense native tool streams, empty and
/// malformed answers, quota faults, nonzero exits, owned cancellation/restart,
/// idempotent admission and measured quiet/draining reads. Every terminal tip
/// requires actual cleanup and released ownership. Homes/receipts are retained;
/// no provider request, credential or production job is involved.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fifteen_agent_cohorts_keep_controls_idempotent_and_cleanup_verified() {
    let scenarios = [
        ("fixture:load-barrier", "succeeded"),
        ("fixture:load-barrier", "succeeded"),
        ("fixture:load-barrier", "succeeded"),
        ("fixture:load-barrier:dense-tools", "succeeded"),
        ("fixture:load-barrier:empty-result", "failed"),
        ("fixture:load-barrier:truncated", "failed"),
        ("fixture:load-barrier:quota", "failed"),
        ("fixture:load-barrier:nonzero-after-result", "failed"),
        ("fixture:load-barrier", "cancelled"),
        ("fixture:load-barrier", "succeeded"), // held WAL writer after native roots are durable
    ];
    for (round, (task, outcome)) in scenarios.into_iter().enumerate() {
        let mut broker = Broker::start_with_limit(&[], 20);
        broker.retain_evidence();
        if round == 0 {
            let version = cli(&broker.home, &["--version"]);
            assert!(version.status.success());
            eprintln!(
                "LOAD_BINARY path={} version={}",
                Path::new(&qualification_binary()).display(),
                String::from_utf8_lossy(&version.stdout).trim()
            );
        }
        let socket = broker.home.join("api.sock");
        let mut launches = Vec::new();
        for n in 0..15 {
            let request = broker.request(task, &format!("load-{round}-{n}"));
            let client = BrokerClient::new(socket.clone());
            launches.push(tokio::spawn(async move {
                let deadline = Instant::now() + Duration::from_secs(15);
                let mut busy = 0_u32;
                loop {
                    match client.start(&request).await {
                        Ok(started) => return Ok((started, busy)),
                        Err(Error::Broker {
                            broker_error_code: Some(ref code),
                            ..
                        }) if code == "selection_busy" && Instant::now() < deadline => {
                            busy += 1;
                            tokio::time::sleep(Duration::from_millis(10)).await;
                        }
                        Err(error) => return Err(error),
                    }
                }
            }));
        }
        let mut ids = Vec::new();
        let mut busy_refusals = 0_u32;
        for job in launches {
            let (started, busy) = tokio::time::timeout(Duration::from_secs(20), job)
                .await
                .unwrap()
                .unwrap()
                .unwrap();
            busy_refusals += busy;
            assert!(started.created);
            ids.push(started.agent_id);
        }
        let client = BrokerClient::new(socket.clone());
        let ready_until = Instant::now() + Duration::from_secs(20);
        let leaders = loop {
            let page = client
                .call(
                    "list_agents",
                    Some(json!({"active":true,"limit":50,"offset":0})),
                )
                .await
                .unwrap();
            assert_eq!(page["total"], 15);
            if page["items"]
                .as_array()
                .unwrap()
                .iter()
                .all(|a| a["status"] == "running")
            {
                let read = rusqlite::Connection::open_with_flags(
                    broker.home.join("state.db"),
                    rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY,
                )
                .unwrap();
                let mut stmt = read.prepare("SELECT leader_json FROM process_ownership WHERE owner_kind='attempt' ORDER BY owner_id").unwrap();
                let roots: Vec<agent_run_platform::process::Identity> = stmt
                    .query_map([], |r| r.get::<_, String>(0))
                    .unwrap()
                    .map(|raw| serde_json::from_str(&raw.unwrap()).unwrap())
                    .collect();
                if roots.len() == 15
                    && roots.iter().all(|p| {
                        agent_run_platform::process::observe(
                            Some(p.pid),
                            Some(&p.token),
                            Some(p.birth),
                        ) == agent_run_platform::process::ProcessState::Alive
                    })
                {
                    break roots;
                }
            }
            assert!(Instant::now() < ready_until, "cohort never reached running");
            tokio::time::sleep(Duration::from_millis(20)).await;
        };
        eprintln!(
            "LOAD_OWNERS round={round} verified_live_native_leaders={}",
            leaders.len()
        );
        let replay = client
            .start(&broker.request(task, &format!("load-{round}-0")))
            .await
            .unwrap();
        assert!(!replay.created);
        assert_eq!(replay.agent_id, ids[0]);
        let mut samples = Vec::new();
        for n in 0..200 {
            let before = Instant::now();
            let reply = if n % 10 == 0 {
                client
                    .start(&broker.request(task, &format!("load-{round}-0")))
                    .await
                    .map(|replayed| {
                        assert!(!replayed.created);
                        assert_eq!(replayed.agent_id, ids[0]);
                    })
            } else if n % 2 == 0 {
                client.call("ping", None).await.map(drop)
            } else {
                client
                    .call(
                        "list_agents",
                        Some(json!({"active":true,"limit":50,"offset":0})),
                    )
                    .await
                    .map(drop)
            };
            reply.unwrap();
            samples.push(before.elapsed().as_micros() as u64);
        }
        samples.sort_unstable();
        eprintln!("LOAD_ADMISSION round={round} busy_refusals={busy_refusals}");
        eprintln!(
            "LOAD_COHORT round={round} active=15 samples=200 p50_us={} p95_us={} p99_us={}",
            samples[99], samples[189], samples[197]
        );
        assert!(
            samples[189] <= 250_000,
            "warm p95 read/control exceeded 250 ms"
        );
        assert!(
            samples[197] <= 1_000_000,
            "warm p99 read/control exceeded one second"
        );
        if round == 2 {
            broker.restart().await;
            let recovered = client
                .call(
                    "list_agents",
                    Some(json!({"active":true,"limit":50,"offset":0})),
                )
                .await
                .unwrap();
            assert_eq!(
                recovered["total"], 15,
                "broker restart must retain all original work"
            );
            assert_eq!(
                client
                    .start(&broker.request(task, &format!("load-{round}-0")))
                    .await
                    .unwrap()
                    .agent_id,
                ids[0]
            );
        }
        if round == 1 || outcome == "cancelled" {
            for id in ids.iter().take(if round == 1 { 3 } else { 15 }) {
                let before = Instant::now();
                client
                    .call("cancel", Some(json!({"agent_id":id})))
                    .await
                    .unwrap();
                let elapsed = before.elapsed();
                assert!(
                    elapsed <= Duration::from_millis(250),
                    "cancel acknowledgement exceeded250ms"
                );
                eprintln!("LOAD_CANCEL_ACK round={round} us={}", elapsed.as_micros());
            }
        }
        let held_writer = if round == 9 {
            let (ready, waiting) = std::sync::mpsc::sync_channel(0);
            let path = broker.home.join("state.db");
            let job = std::thread::spawn(move || {
                let conn = rusqlite::Connection::open(path).unwrap();
                conn.execute_batch("BEGIN IMMEDIATE").unwrap();
                ready.send(()).unwrap();
                std::thread::sleep(Duration::from_millis(350));
                conn.execute_batch("ROLLBACK").unwrap();
            });
            waiting.recv_timeout(Duration::from_secs(2)).unwrap();
            Some(job)
        } else {
            None
        };
        std::fs::write(broker.home.join("fixture-load-release"), "release").unwrap();
        let drain_client = BrokerClient::new(socket.clone());
        let drain_reads = tokio::spawn(async move {
            let mut samples = Vec::new();
            for _ in 0..200 {
                let before = Instant::now();
                drain_client.call("ping", None).await.unwrap();
                samples.push(before.elapsed().as_micros() as u64);
            }
            samples.sort_unstable();
            samples
        });
        for (n, id) in ids.iter().enumerate() {
            let answer = tokio::time::timeout(
                Duration::from_secs(30),
                client.call("wait", Some(json!({"agent_id":id,"timeout_seconds":25}))),
            )
            .await
            .unwrap()
            .unwrap();
            let expected = if round == 1 && n < 3 {
                "cancelled"
            } else {
                outcome
            };
            assert_eq!(answer["status"], expected, "{answer}");
            if expected == "succeeded" {
                assert_eq!(answer["available"], true);
            }
        }
        if let Some(writer) = held_writer {
            writer.join().unwrap();
        }
        let draining = drain_reads.await.unwrap();
        eprintln!(
            "LOAD_DRAIN round={round} task={task} samples=200 p50_us={} p95_us={} p99_us={}",
            draining[99], draining[189], draining[197]
        );
        assert!(draining[189] <= 250_000, "draining p95 exceeded 250 ms");
        assert!(
            draining[197] <= 1_000_000,
            "draining p99 exceeded one second"
        );
        // Read the chosen version's state without migrating a live baseline.
        let store = rusqlite::Connection::open_with_flags(
            broker.home.join("state.db"),
            rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY,
        )
        .unwrap();
        if round == 3 {
            let results: i64 = store.query_row("SELECT COUNT(*) FROM messages WHERE role='tool_result' AND name='fixture_tool'", [], |r| r.get(0)).unwrap();
            let session_writes: i64 = store
                .query_row(
                    "SELECT COUNT(*) FROM events WHERE kind='runtime_session'",
                    [],
                    |r| r.get(0),
                )
                .unwrap();
            assert_eq!(
                results, 3000,
                "all dense native tool results were journaled"
            );
            assert_eq!(
                session_writes, 15,
                "unchanged frame identities cannot flood the SQLite writer"
            );
            eprintln!("LOAD_DENSE tool_results={results} session_writes={session_writes}");
        }
        let terminal_delay: f64 = store.query_row("SELECT MAX(a.finished_at-(SELECT MAX(e.at) FROM events e WHERE e.agent_id=a.id AND e.kind='process_cleanup')) FROM agents a", [], |r| r.get(0)).unwrap();
        assert!(
            terminal_delay.is_finite() && terminal_delay <= 1.0,
            "cleanup to terminal exceeded one second: {terminal_delay}"
        );
        eprintln!(
            "LOAD_TERMINAL round={round} cleanup_to_terminal_max_ms={} home={}",
            terminal_delay * 1000.0,
            broker.home.display()
        );
        assert_eq!(
            store
                .query_row(
                    "SELECT COUNT(*) FROM attempts WHERE ownership_active=1",
                    [],
                    |r| r.get::<_, i64>(0)
                )
                .unwrap(),
            0
        );
        let bad: i64=store.query_row("SELECT COUNT(*) FROM attempts WHERE cleanup_proof_json IS NULL OR json_extract(cleanup_proof_json,'$.confirmed') IS NOT 1 OR json_extract(cleanup_proof_json,'$.group_gone') IS NOT 1 OR json_extract(cleanup_proof_json,'$.descendants_gone') IS NOT 1",[],|r|r.get(0)).unwrap();
        assert_eq!(
            bad, 0,
            "every owned process/descendant requires real cleanup proof"
        );
        for p in &leaders {
            assert!(
                matches!(
                    agent_run_platform::process::observe(
                        Some(p.pid),
                        Some(&p.token),
                        Some(p.birth)
                    ),
                    agent_run_platform::process::ProcessState::Dead
                        | agent_run_platform::process::ProcessState::Reused
                ),
                "an owned native identity survived or became unobservable"
            );
        }
        assert_eq!(
            client
                .call(
                    "list_agents",
                    Some(json!({"active":true,"limit":50,"offset":0}))
                )
                .await
                .unwrap()["total"],
            0
        );
    }
}

/// Fifteen overlapping owned harnesses lose their initial cleanup observation.
/// The real broker must retain that diagnostic and recover only through fresh
/// process proof; a valid native result never becomes success or a new attempt.
/// Fixture files/processes remain isolated and the synthetic home is retained.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fifteen_agents_with_unavailable_cleanup_never_publish_success() {
    let mut broker = Broker::start_with_limit(&[], 20);
    broker.retain_evidence();
    std::fs::write(
        broker.home.join("fixture-cleanup-error-1"),
        "fixture denial",
    )
    .unwrap();
    let client = BrokerClient::new(broker.home.join("api.sock"));
    let mut ids = Vec::new();
    for n in 0..15 {
        let started = client
            .start(&broker.request("fixture:load-barrier", &format!("cleanup-error-{n}")))
            .await
            .unwrap();
        assert!(started.created);
        ids.push(started.agent_id);
    }
    let until = Instant::now() + Duration::from_secs(20);
    loop {
        let read = rusqlite::Connection::open_with_flags(
            broker.home.join("state.db"),
            rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY,
        )
        .unwrap();
        let mut stmt = read
            .prepare("SELECT leader_json FROM process_ownership WHERE owner_kind='attempt'")
            .unwrap();
        let roots: Vec<agent_run_platform::process::Identity> = stmt
            .query_map([], |r| r.get::<_, String>(0))
            .unwrap()
            .map(|raw| serde_json::from_str(&raw.unwrap()).unwrap())
            .collect();
        if roots.len() == 15
            && roots.iter().all(|p| {
                agent_run_platform::process::observe(Some(p.pid), Some(&p.token), Some(p.birth))
                    == agent_run_platform::process::ProcessState::Alive
            })
        {
            break;
        }
        assert!(
            Instant::now() < until,
            "all fifteen actual native identities must be live before release"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    std::fs::write(broker.home.join("fixture-load-release"), "release").unwrap();
    for id in &ids {
        let result = tokio::time::timeout(
            Duration::from_secs(30),
            client.call("wait", Some(json!({"agent_id":id,"timeout_seconds":25}))),
        )
        .await
        .unwrap()
        .unwrap();
        assert_eq!(result["status"], "lost", "{result}");
        assert_eq!(
            result["available"], false,
            "unverified initial cleanup cannot seal a successful answer"
        );
    }
    let read = rusqlite::Connection::open_with_flags(
        broker.home.join("state.db"),
        rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY,
    )
    .unwrap();
    let unavailable: i64 = read
        .query_row(
            "SELECT COUNT(*) FROM events WHERE kind='process_cleanup_unavailable'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    let attempts: i64 = read
        .query_row("SELECT COUNT(*) FROM attempts", [], |r| r.get(0))
        .unwrap();
    let successes: i64 = read
        .query_row(
            "SELECT COUNT(*) FROM agents WHERE status='succeeded'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!((unavailable, attempts, successes), (15, 15, 0));
    eprintln!(
        "LOAD_UNAVAILABLE_CLEANUP actual_native_leaders=15 original_denials={unavailable} successes={successes} home={}",
        broker.home.display()
    );
}

/// Stable references survive repeated native resumes and old-request replay;
/// exact historical reads remain available through both socket and CLI. Names
/// normalize on start, inherit on resume, can be replaced through MCP and
/// appear in all public list transports; changed-name replays return Conflict.
#[tokio::test]
async fn stable_agent_identity_survives_resume_history_and_concurrent_controls() {
    let broker = Broker::start();
    let client = BrokerClient::new(broker.home.join("api.sock"));
    let mut request = broker.request("fixture:answer", "stable-start");
    request.display_name = Some("  Мария / review  ".into());
    let started = client
        .call("start", Some(serde_json::to_value(request).unwrap()))
        .await
        .unwrap();
    let agent = started["agent_id"].as_str().unwrap().to_owned();
    let mut runs = vec![agent.clone()];
    assert!(started.get("run_id").is_none());
    assert_eq!(started["agent"]["name"], "Мария / review");
    broker.wait_terminal(&runs[0]);
    let parent = agent_run::state::Store::open(&broker.home)
        .unwrap()
        .get(&agent.parse().unwrap())
        .unwrap();
    assert_eq!(
        parent.status,
        agent_run::domain::Status::Succeeded,
        "{:?}",
        parent.failure_text
    );
    assert!(
        parent.runtime_session_id.is_some(),
        "{:?}",
        parent.failure_text
    );
    for number in 1..=2 {
        let mut arguments = json!({
            "agent_id":agent, "task":"fixture:answer",
            "request_id":format!("stable-resume-{number}")
        });
        if number == 2 {
            arguments["display_name"] = json!("工程師: next");
        }
        let continued = if number == 2 {
            let reply = mcp_tool(&broker.home, "resume", arguments).await;
            assert_ne!(reply["result"]["isError"], true, "{reply}");
            let identity = reply["result"]["structuredContent"].clone();
            assert_eq!(identity.as_object().unwrap().len(), 2);
            let text = reply["result"]["content"][0]["text"].as_str().unwrap();
            assert!(text.contains(&format!("- Agent: {agent}")), "{text}");
            assert!(!text.contains("- Run:"), "{text}");
            assert!(text.contains("Name: 工程師: next"), "{text}");
            identity
        } else {
            let result = client.call("resume", Some(arguments)).await.unwrap();
            assert_eq!(result["agent"]["agent_id"], agent);
            assert_eq!(result["agent"]["name"], "Мария / review");
            result
        };
        assert_eq!(continued["agent_id"], agent);
        assert!(continued.get("run_id").is_none());
        let store = agent_run::state::Store::open(&broker.home).unwrap();
        let run = agent_run_core::agent_identity::resolve_sequence(
            &store,
            &agent.parse().unwrap(),
            continued["sequence"].as_u64().unwrap() as u32,
        )
        .unwrap()
        .id
        .to_string();
        assert!(!runs.contains(&run));
        broker.wait_terminal(&run);
        runs.push(run);
    }
    let list = client.call("list_agents", Some(json!({}))).await.unwrap();
    assert_eq!(list["items"][0]["name"], "工程師: next");
    assert_eq!(list["items"][0]["usage"]["input_tokens"], 2);
    for field in ["usage", "usage_cumulative"] {
        let object = list["items"][0][field].as_object().unwrap();
        for private in ["agent_id", "run_id", "attempt_id", "root_agent_id"] {
            assert!(!object.contains_key(private), "{object:?}");
        }
    }
    let cli_list = cli(&broker.home, &["agents"]);
    assert!(cli_list.status.success(), "{cli_list:?}");
    let cli_page: Value = serde_json::from_slice(&cli_list.stdout).unwrap();
    assert_eq!(cli_page["items"][0]["name"], "工程師: next");
    let mcp_list = mcp_tool(&broker.home, "list_agents", json!({})).await;
    let text = mcp_list["result"]["content"][0]["text"].as_str().unwrap();
    assert!(text.contains("name: 工程師: next"), "{text}");
    assert!(text.contains("usage: in 2 out 3"), "{text}");
    let store = agent_run::state::Store::open(&broker.home).unwrap();
    let native = store
        .get(&runs[0].parse().unwrap())
        .unwrap()
        .runtime_session_id;
    for (index, run) in runs.iter().enumerate() {
        let row = store.get(&run.parse().unwrap()).unwrap();
        assert_eq!(row.root_agent_id.as_str(), agent);
        assert_eq!(row.sequence as usize, index + 1);
        assert_eq!(row.runtime_session_id, native);
        let answer = client
            .call("answer", Some(json!({"agent_id":agent,"run_id":run})))
            .await
            .unwrap();
        assert_eq!(answer["agent_id"], agent);
        assert!(answer.get("run_id").is_none());
        assert_eq!(answer["available"], true);
        let output = cli(&broker.home, &["answer", &agent, "--run-id", run]);
        assert!(output.status.success(), "{output:?}");
        let from_cli: Value = serde_json::from_slice(&output.stdout).unwrap();
        assert_eq!(from_cli["agent_id"], agent);
        assert!(from_cli.get("run_id").is_none());
    }
    // A delayed retry must return the original child, not append a fourth turn.
    let replay = client
        .call(
            "resume",
            Some(json!({
                "agent_id":agent,"task":"fixture:answer",
                "request_id":"stable-resume-1"
            })),
        )
        .await
        .unwrap();
    assert_eq!(replay["created"], false);
    assert_eq!(replay["agent_id"], agent);
    assert_eq!(replay["sequence"], 2);
    assert!(replay.get("run_id").is_none());
    let conflicting = client
        .call(
            "resume",
            Some(json!({
                "agent_id":agent,"task":"different intent",
                "request_id":"stable-resume-1"
            })),
        )
        .await;
    assert!(conflicting.is_err());
    let changed_name = client
        .call(
            "resume",
            Some(json!({
                "agent_id":agent, "task":"fixture:answer",
                "request_id":"stable-resume-1", "display_name":"changed"
            })),
        )
        .await
        .unwrap_err();
    assert!(
        matches!(&changed_name, Error::Broker { broker_error_code: Some(code), .. } if code == "RequestConflict"),
        "{changed_name:?}"
    );
    assert_eq!(changed_name.public().kind, "RequestConflict");
    let count: i64 = store
        .conn
        .query_row("SELECT COUNT(*) FROM agents", [], |row| row.get(0))
        .unwrap();
    assert_eq!(count, 3);
    let latest = client
        .call("answer", Some(json!({"agent_id":runs[1]})))
        .await
        .unwrap();
    assert_eq!(latest["agent_id"], agent);
    assert!(latest.get("run_id").is_none());

    // A bounded hanging fixture keeps the winner active until explicitly cancelled.
    let other = BrokerClient::new(broker.home.join("api.sock"));
    let first = json!({"agent_id":agent,"task":"fixture:hang","request_id":"stable-race-a"});
    let second = json!({"agent_id":agent,"task":"fixture:hang","request_id":"stable-race-b"});
    let (left, right) = tokio::join!(
        client.call("resume", Some(first)),
        other.call("resume", Some(second))
    );
    assert_eq!(usize::from(left.is_ok()) + usize::from(right.is_ok()), 1);
    let winner = left.or(right).unwrap();
    assert_eq!(winner["agent_id"], agent);
    let active_run = agent_run_core::agent_identity::resolve_sequence(
        &store,
        &agent.parse().unwrap(),
        winner["sequence"].as_u64().unwrap() as u32,
    )
    .unwrap()
    .id
    .to_string();
    let cancelled = client
        .call("cancel", Some(json!({"agent_id":agent})))
        .await
        .unwrap();
    assert!(cancelled.get("run_id").is_none());
    assert_eq!(cancelled["agent_id"], agent);
    broker.wait_terminal(&active_run);
    let active: i64 = store.conn.query_row(
        "SELECT COUNT(*) FROM agents WHERE status IN ('created','starting','running','cancelling')", [], |row| row.get(0)
    ).unwrap();
    assert_eq!(active, 0);
}

/// Authoritative quota exhaustion (a durable native latch on the model's
/// lane, written through the same store API the supervisor uses) and a
/// disabled sole account are refused with their exact allowlisted public
/// codes through the real broker's ranking on every transport.
#[tokio::test]
async fn admission_codes_survive_every_public_transport() {
    let broker = Broker::start();
    let now = agent_run_domain::domain::now();
    agent_run::state::Store::open(&broker.home)
        .unwrap()
        .latch_native_exhaustion(
            &"acct-work".parse().unwrap(),
            "glm-user",
            "fixture",
            "5h",
            "native-signal",
            &std::collections::BTreeSet::from(["fixture".to_owned()]),
            now,
            Some(now + 3600.0),
        )
        .unwrap();
    broker.assert_refused("quota_exhausted").await;

    assert!(
        cli(&broker.home, &["accounts", "disable", "acct-work"])
            .status
            .success()
    );
    broker.assert_refused("no_eligible_account").await;
}

/// A static credential/connection mismatch returns a typed refusal through
/// real CLI, MCP and socket starts before any agent or attempt row exists.
#[tokio::test]
async fn static_launch_preflight_refuses_every_public_transport_without_admission() {
    let broker = Broker::start();
    let path = broker.home.join("config.toml");
    let mut config: toml::Value = toml::from_str(&std::fs::read_to_string(&path).unwrap()).unwrap();
    config["providers"]["glm-user"]["connection"] = toml::Value::Table(
        [("kind".into(), toml::Value::String("native".into()))]
            .into_iter()
            .collect(),
    );
    std::fs::write(path, toml::to_string(&config).unwrap()).unwrap();
    broker.assert_refused("ValidationError").await;
    let store = agent_run::state::Store::open(&broker.home).unwrap();
    for table in ["agents", "attempts"] {
        let count: i64 = store
            .conn
            .query_row(&format!("SELECT COUNT(*) FROM {table}"), [], |row| {
                row.get(0)
            })
            .unwrap();
        assert_eq!(count, 0, "{table}");
    }
}

/// A real broker whose committed capacity revision moves between every
/// ranking and submission (test-fixtures seam) spends its stale-retry
/// budget; the contention verdict `selection_busy` keeps its exact class on
/// the CLI, MCP and socket clients, and nothing is admitted.
#[tokio::test]
async fn selection_busy_survives_every_public_transport() {
    let broker = Broker::start_with(&[("AGENT_RUN_FIXTURE_ALWAYS_STALE", "1")]);
    broker.assert_refused("selection_busy").await;
    let admitted: i64 = agent_run::state::Store::open(&broker.home)
        .unwrap()
        .conn
        .query_row("SELECT COUNT(*) FROM agents", [], |row| row.get(0))
        .unwrap();
    assert_eq!(admitted, 0);
}

/// A role profile that is a symlink escaping the configured profile root is
/// refused with the typed `PathEscapeError` class on the CLI, MCP and socket
/// clients alike (never collapsed into `ValidationError`), without echoing
/// the escaped target; an ordinary malformed argument stays
/// `ValidationError`.
#[tokio::test]
async fn profile_symlink_escape_keeps_path_escape_error_on_every_transport() {
    let broker = Broker::start();
    let outside = tempfile::tempdir().unwrap();
    let target = outside.path().join("escaped-profile.md");
    std::fs::copy(broker.home.join("profiles/review.md"), &target).unwrap();
    std::fs::remove_file(broker.home.join("profiles/review.md")).unwrap();
    std::os::unix::fs::symlink(&target, broker.home.join("profiles/review.md")).unwrap();
    broker.assert_refused("PathEscapeError").await;
    let output = cli(
        &broker.home,
        &[
            "start",
            "--provider",
            "glm-user",
            "--model",
            "fixture",
            "--profile",
            "review",
            "--task",
            "fixture:answer",
            "--workdir",
            broker.home.to_str().unwrap(),
        ],
    );
    let shown = String::from_utf8_lossy(&output.stderr);
    assert!(!shown.contains(outside.path().to_str().unwrap()), "{shown}");
    let client = BrokerClient::new(broker.home.join("api.sock"));
    let malformed = client
        .call("start", Some(json!({"provider":"glm-user","bogus":true})))
        .await
        .unwrap_err();
    assert!(matches!(malformed, Error::Validation(_)), "{malformed:?}");
    assert_eq!(malformed.public().kind, "ValidationError");
}

/// One isolated broker and real raw MCP exchanges prove fifteen advertised
/// tools, filtered guidance, typed errors, diagnostic privacy/numbers, and
/// unchanged structured CLI/socket compatibility aliases without provider turns.
#[tokio::test]
async fn consolidated_routing_discovery_and_legacy_aliases_work_live() {
    let broker = Broker::start();
    let client = BrokerClient::new(broker.home.join("api.sock"));
    let store = agent_run_store::Store::open(&broker.home).unwrap();
    let at = agent_run_core::domain::now();
    store.conn.execute(
        "INSERT INTO capacity_samples(runtime,lane,window,target,source,remaining_percent,reset_at,observed_at,valid_until,payload_json,account_id,quota_key)
         VALUES('glm-user','fixture','5h',NULL,'fixture',60,?1,?2,?3,'null','acct-work','acct-work::fixture')",
        rusqlite::params![at + 3600.0, at, at + 600.0],
    ).unwrap();
    let discovery = mcp_request(&broker.home, "2026-07-28", "tools/list", json!({})).await;
    let legacy_discovery = mcp_request(&broker.home, "2025-06-18", "tools/list", json!({})).await;
    assert_eq!(
        legacy_discovery["result"]["tools"],
        discovery["result"]["tools"]
    );
    let tools = discovery["result"]["tools"].as_array().unwrap();
    assert_eq!(tools.len(), 15, "{discovery}");
    for name in ["models", "capacity_order"] {
        assert!(!tools.iter().any(|tool| tool["name"] == name));
    }
    let socket_tools = client.call("tools", Some(json!({}))).await.unwrap();
    assert_eq!(socket_tools.as_array().unwrap().len(), 15);
    let filtered_cli = cli(
        &broker.home,
        &[
            "delegation-guide",
            "--provider",
            "glm-user",
            "--model",
            "fixture",
            "--profile",
            "review",
        ],
    );
    assert!(filtered_cli.status.success(), "{filtered_cli:?}");
    let guide = String::from_utf8(filtered_cli.stdout).unwrap();
    assert!(guide.contains("fixture") && guide.contains("review"));
    let unknown_cli = cli(&broker.home, &["delegation-guide", "--model", "missing"]);
    assert!(!unknown_cli.status.success());
    let error: Value = serde_json::from_slice(&unknown_cli.stderr).unwrap();
    assert_eq!(error["error"]["type"], "ValidationError", "{error}");
    for protocol in ["2025-06-18", "2026-07-28"] {
        let call = |name: &str, arguments: Value| {
            mcp_request(
                &broker.home,
                protocol,
                "tools/call",
                json!({"name":name,"arguments":arguments}),
            )
        };
        let guide = call(
            "delegation_guide",
            json!({"provider":"glm-user","model":"fixture","profile":"review"}),
        )
        .await;
        assert_eq!(guide["result"]["isError"], false, "{protocol}: {guide}");
        let text = guide["result"]["content"][0]["text"].as_str().unwrap();
        assert!(text.contains("fixture") && text.contains("review"));
        for key in ["provider", "model", "profile"] {
            let bad = call("delegation_guide", json!({key:"missing"})).await;
            assert_eq!(bad["result"]["isError"], true, "{protocol}: {bad}");
            assert!(
                bad["result"]["content"][0]["text"]
                    .as_str()
                    .unwrap()
                    .contains("ValidationError")
            );
        }
        let limits = call("limits", json!({})).await;
        assert_eq!(limits["result"]["isError"], false, "{protocol}: {limits}");
        let text = limits["result"]["content"][0]["text"].as_str().unwrap();
        assert!(text.contains("60.0% remaining") && text.contains("resets in"));
        assert!(text.contains("score") && text.contains("multiplier") && text.contains("priority"));
        for private in [
            "acct-work",
            "synthetic-token",
            "gateway.example",
            "FAKE_TOKEN",
        ] {
            assert!(!text.contains(private), "{private}: {text}");
        }
        for method in ["models", "capacity_order"] {
            let alias = call(method, json!({})).await;
            assert_eq!(alias["result"]["isError"], false, "{protocol}: {alias}");
        }
    }
    for (method, command) in [
        ("models", &["models"][..]),
        ("capacity_order", &["capacity", "order"][..]),
    ] {
        let legacy = client.call(method, Some(json!({}))).await.unwrap();
        assert_eq!(legacy["schema_version"], 2);
        assert!(legacy.get("ranking").is_none());
        let output = cli(&broker.home, command);
        assert!(output.status.success(), "{output:?}");
        let value: Value = serde_json::from_slice(&output.stdout).unwrap();
        assert_eq!(
            value["providers"][0]["provider"],
            legacy["providers"][0]["provider"]
        );
        assert_eq!(
            value.as_object().unwrap().keys().collect::<Vec<_>>(),
            legacy.as_object().unwrap().keys().collect::<Vec<_>>()
        );
    }
    let agents: i64 = store
        .conn
        .query_row("SELECT COUNT(*) FROM agents", [], |row| row.get(0))
        .unwrap();
    assert_eq!(agents, 0, "read-only smoke must not launch provider turns");
}

/// The delegation guide is one plain-text result on every public transport:
/// the private socket envelope carries the dispatcher's JSON string, MCP
/// exposes that string as real text content with no structured placeholder
/// and no JSON quoting, and the CLI prints the text itself with a normal
/// newline. Strict argument validation holds on the wire, and no account,
/// credential, or endpoint identity appears in the text.
#[tokio::test]
async fn delegation_guide_is_plain_text_on_every_public_transport() {
    let broker = Broker::start();
    let client = BrokerClient::new(broker.home.join("api.sock"));
    let guide = client
        .call("delegation_guide", Some(json!({})))
        .await
        .unwrap();
    let text = guide.as_str().expect("guide result is a string").to_owned();
    assert!(
        text.contains("provider glm-user (harness claude-code)"),
        "{text}"
    );
    assert!(text.contains("- fixture:"), "{text}");
    assert!(
        text.contains("provider glm-user (harness claude-code) — all models admit: review"),
        "{text}"
    );
    for private in [
        "acct-work",
        "synthetic-token",
        "gateway.example",
        "FAKE_TOKEN",
    ] {
        assert!(!text.contains(private), "{private} leaked: {text}");
    }
    let strict = client
        .call("delegation_guide", Some(json!({"unexpected": true})))
        .await
        .unwrap_err();
    assert_eq!(strict.public().kind, "ValidationError");

    let output = cli(&broker.home, &["delegation-guide"]);
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert_eq!(
        String::from_utf8_lossy(&output.stdout),
        format!("{text}\n"),
        "CLI prints the text itself, not JSON"
    );

    let mut mcp = Command::new(env!("CARGO_BIN_EXE_agent-run"))
        .arg("--home")
        .arg(&broker.home)
        .arg("mcp")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()
        .unwrap();
    let mut input = mcp.stdin.take().unwrap();
    for message in [
        json!({"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2025-06-18","capabilities":{},"clientInfo":{"name":"test","version":"0"}}}),
        json!({"jsonrpc":"2.0","method":"notifications/initialized"}),
        json!({"jsonrpc":"2.0","id":2,"method":"tools/call","params":{"name":"delegation_guide","arguments":{}}}),
        json!({"jsonrpc":"2.0","id":3,"method":"tools/call","params":{"name":"delegation_guide","arguments":{"unexpected":true}}}),
    ] {
        writeln!(input, "{message}").unwrap();
    }
    input.flush().unwrap();
    let mut replies = BufReader::new(mcp.stdout.take().unwrap());
    // rmcp serves requests concurrently, so the locally-rejected strict call
    // can reply before the broker-backed guide call: match by id, not order.
    let mut call = Value::Null;
    let mut strict_reply = Value::Null;
    for _ in 0..3 {
        let mut line = String::new();
        replies.read_line(&mut line).unwrap();
        let reply: Value = serde_json::from_str(&line).unwrap();
        match reply["id"].as_i64() {
            Some(2) => call = reply,
            Some(3) => strict_reply = reply,
            _ => {}
        }
        if !call.is_null() && !strict_reply.is_null() {
            break;
        }
    }
    drop(input);
    let _ = mcp.wait();
    assert_eq!(call["id"], 2);
    assert_eq!(call["result"]["isError"], false, "{call}");
    assert_eq!(call["result"]["content"][0]["type"], "text", "{call}");
    assert_eq!(call["result"]["content"][0]["text"], text, "{call}");
    assert!(
        call["result"].get("structuredContent").is_none(),
        "no structured mirror: {call}"
    );
    assert_eq!(strict_reply["id"], 3);
    assert_eq!(strict_reply["result"]["isError"], true, "{strict_reply}");
    assert!(
        strict_reply["result"]["content"][0]["text"]
            .as_str()
            .is_some_and(|line| line.contains("ValidationError")),
        "{strict_reply}"
    );
    assert!(
        strict_reply["result"].get("structuredContent").is_none(),
        "{strict_reply}"
    );
}
