//! Process-boundary checks for the explicit operator client and listener flags.

use clap::Parser;
use serde_json::{Value, json};
use std::{
    fs,
    io::{BufRead, BufReader, Read, Write},
    os::unix::net::UnixListener,
    path::{Path, PathBuf},
    process::{Child, ChildStdin, Command, Output, Stdio},
    sync::mpsc,
    thread,
    time::{Duration, Instant},
};

const AGENT_ID: &str = "ag-20260826-120000-0123456789";

/// Keeps the mock socket and the deliberately absent client homes isolated.
struct Fixture {
    dir: tempfile::TempDir,
    socket: PathBuf,
    local_home: PathBuf,
    env_home: PathBuf,
    caller_home: PathBuf,
}

impl Fixture {
    /// Allocate a short Unix-socket path beneath the configured test temp root.
    fn new() -> Self {
        let dir = tempfile::Builder::new().prefix("oc-").tempdir().unwrap();
        let socket = dir.path().join("broker.sock");
        let local_home = dir.path().join("local-home");
        let env_home = dir.path().join("env-home");
        let caller_home = dir.path().join("caller-home");
        Self {
            dir,
            socket,
            local_home,
            env_home,
            caller_home,
        }
    }

    /// Build an isolated CLI process with all home locations pointed at the fixture.
    fn command(&self) -> Command {
        let mut command = Command::new(env!("CARGO_BIN_EXE_agent-run"));
        command
            .arg("--home")
            .arg(&self.local_home)
            .env("AGENT_RUN_HOME", &self.env_home)
            .env("HOME", &self.caller_home)
            .env_remove("AGENT_RUN_MCP_PARENT_PID")
            .current_dir(self.dir.path());
        command
    }

    /// Add the explicit endpoint and optionally its trusted kernel UID.
    fn operator(&self, uid: Option<u32>) -> Command {
        let mut command = self.command();
        command.arg("--broker-socket").arg(&self.socket);
        if let Some(uid) = uid {
            command.args(["--broker-uid", &uid.to_string()]);
        }
        command
    }

    /// Verify that neither configured home nor the caller's home was created.
    fn assert_homes_absent(&self) {
        assert!(!self.local_home.exists());
        assert!(!self.env_home.exists());
        assert!(!self.caller_home.exists());
    }
}

/// Kernel UID used by both this fixture server and its child processes.
fn uid() -> u32 {
    // SAFETY: geteuid reads process identity without changing it.
    unsafe { libc::geteuid() }
}

/// Accept a connection with a bounded deadline so failed tests never hang.
fn accept(listener: &UnixListener) -> std::os::unix::net::UnixStream {
    listener.set_nonblocking(true).unwrap();
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        match listener.accept() {
            Ok((stream, _)) => {
                stream.set_nonblocking(false).unwrap();
                return stream;
            }
            Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                assert!(Instant::now() < deadline, "mock broker accept deadline");
                thread::sleep(Duration::from_millis(5));
            }
            Err(error) => panic!("mock broker accept failed: {error}"),
        }
    }
}

/// Serve one connection per response, recording complete JSON-RPC requests.
fn serve(socket: &Path, responses: Vec<Value>) -> thread::JoinHandle<Vec<Value>> {
    let listener = UnixListener::bind(socket).unwrap();
    thread::spawn(move || {
        let mut requests = Vec::new();
        for result in responses {
            let mut stream = accept(&listener);
            stream
                .set_read_timeout(Some(Duration::from_secs(5)))
                .unwrap();
            let mut input = BufReader::new(stream.try_clone().unwrap());
            let mut line = String::new();
            input.read_line(&mut line).unwrap();
            let request: Value = serde_json::from_str(&line).unwrap();
            writeln!(
                stream,
                "{}",
                json!({"jsonrpc":"2.0", "id":request["id"], "result":result})
            )
            .unwrap();
            requests.push(request);
        }
        requests
    })
}

/// Run a CLI command and require a successful JSON response.
fn output_json(mut command: Command, args: &[&str]) -> Value {
    let output = command.args(args).stdin(Stdio::null()).output().unwrap();
    assert!(
        output.status.success(),
        "stdout: {}, stderr: {}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    serde_json::from_slice(&output.stdout).unwrap()
}

/// The supported CLI commands share the real client path, payloads and wait fencing.
#[test]
fn operator_cli_routes_broker_commands_without_local_state() {
    let fixture = Fixture::new();
    let spec = fixture.dir.path().join("spec.json");
    let request_spec = json!({"request_id":"p-1", "goal":"fixture", "members":[]});
    fs::write(&spec, serde_json::to_vec(&request_spec).unwrap()).unwrap();
    let ok = json!({"fixture":true});
    let server = serve(
        &fixture.socket,
        vec![
            ok.clone(),
            json!({"agent_id":AGENT_ID,"sequence":7,"created":true}),
            json!({"status":"succeeded","answer":"done"}),
            json!({"agent_id":AGENT_ID,"sequence":8,"created":false}),
            ok.clone(),
            ok.clone(),
            ok.clone(),
            ok.clone(),
            ok.clone(),
            ok.clone(),
        ],
    );
    assert_eq!(output_json(fixture.operator(None), &["capabilities"]), ok);
    assert_eq!(
        output_json(
            fixture.operator(Some(uid())),
            &[
                "start",
                "--provider",
                "codex",
                "--model",
                "fixture",
                "--profile",
                "review",
                "--task",
                "inspect",
                "--workdir",
                fixture.dir.path().to_str().unwrap(),
                "--request-id",
                "start-1",
                "--wait"
            ]
        ),
        json!({"status":"succeeded","answer":"done"})
    );
    assert_eq!(
        output_json(
            fixture.operator(Some(uid())),
            &[
                "resume",
                AGENT_ID,
                "--task",
                "continue",
                "--request-id",
                "resume-1"
            ]
        ),
        json!({"agent_id":AGENT_ID,"created":false})
    );
    assert_eq!(
        output_json(
            fixture.operator(None),
            &["start-pool", "--spec", spec.to_str().unwrap()]
        ),
        ok
    );
    assert_eq!(
        output_json(
            fixture.operator(None),
            &[
                "pool-post",
                "--pool-id",
                "pool-1",
                "--request-id",
                "post-1",
                "--text",
                "hello"
            ]
        ),
        ok
    );
    assert_eq!(
        output_json(
            fixture.operator(None),
            &[
                "pool-replace",
                "--pool-id",
                "pool-1",
                "--agent-id",
                AGENT_ID,
                "--request-id",
                "replace-1",
                "--spec",
                spec.to_str().unwrap()
            ]
        ),
        ok
    );
    assert_eq!(
        output_json(
            fixture.operator(None),
            &[
                "pool",
                "--pool-id",
                "pool-1",
                "--after-seq",
                "2",
                "--limit",
                "5"
            ]
        ),
        ok
    );
    assert_eq!(
        output_json(
            fixture.operator(None),
            &[
                "list-pools",
                "--state",
                "open",
                "--offset",
                "2",
                "--limit",
                "3"
            ]
        ),
        ok
    );
    let hash = "a".repeat(64);
    assert_eq!(
        output_json(
            fixture.operator(None),
            &[
                "answer-chunk",
                AGENT_ID,
                "--sequence",
                "7",
                "--expected-sha256",
                &hash,
                "--offset-bytes",
                "12",
                "--max-bytes",
                "20"
            ]
        ),
        ok
    );
    let calls = server.join().unwrap();
    assert_eq!(
        calls
            .iter()
            .map(|call| call["method"].as_str().unwrap())
            .collect::<Vec<_>>(),
        [
            "capabilities",
            "start",
            "wait",
            "resume",
            "start_pool",
            "pool_post",
            "pool_replace",
            "pool",
            "list_pools",
            "answer_chunk"
        ]
    );
    assert_eq!(calls[0]["params"], json!({}));
    assert_eq!(calls[1]["params"]["provider"], "codex");
    assert_eq!(calls[1]["params"]["task"], "inspect");
    assert_eq!(calls[1]["params"]["request_id"], "start-1");
    assert_eq!(
        calls[2]["params"],
        json!({"agent_id":AGENT_ID,"sequence":7,"timeout_seconds":60.0})
    );
    assert_eq!(
        calls[3]["params"],
        json!({"agent_id":AGENT_ID,"task":"continue","request_id":"resume-1","orchestrator":null})
    );
    assert_eq!(calls[4]["params"], request_spec);
    assert_eq!(
        calls[5]["params"],
        json!({"pool_id":"pool-1","request_id":"post-1","message":"hello"})
    );
    assert_eq!(
        calls[6]["params"],
        json!({"pool_id":"pool-1","agent_id":AGENT_ID,"request_id":"replace-1","start":request_spec})
    );
    assert_eq!(
        calls[7]["params"],
        json!({"pool_id":"pool-1","after_seq":2,"before_seq":null,"limit":5})
    );
    assert_eq!(
        calls[8]["params"],
        json!({"state":"open","offset":2,"limit":3})
    );
    assert_eq!(
        calls[9]["params"],
        json!({"agent_id":AGENT_ID,"sequence":7,"expected_sha256":hash,"offset_bytes":12,"max_bytes":20})
    );
    fixture.assert_homes_absent();
}

/// Operator controls preserve optional exact-run selectors and broker-projected output.
#[test]
fn operator_controls_send_exact_selectors_and_preserve_public_results() {
    const RUN_ID: &str = "ag-20260826-120001-fedcba9876";
    let fixture = Fixture::new();
    let cancelled = json!({"agent_id":AGENT_ID,"status":"running","sequence":4});
    let steered = json!({"agent_id":AGENT_ID,"accepted":true,"sequence":4,"command_id":12});
    let server = serve(
        &fixture.socket,
        vec![
            cancelled.clone(),
            cancelled.clone(),
            steered.clone(),
            steered.clone(),
        ],
    );
    assert_eq!(
        output_json(fixture.operator(None), &["cancel", AGENT_ID]),
        cancelled
    );
    assert_eq!(
        output_json(
            fixture.operator(None),
            &["cancel", AGENT_ID, "--run-id", RUN_ID]
        ),
        cancelled
    );
    assert_eq!(
        output_json(
            fixture.operator(None),
            &["steer", AGENT_ID, "--text", "continue"]
        ),
        steered
    );
    assert_eq!(
        output_json(
            fixture.operator(None),
            &["steer", AGENT_ID, "--run-id", RUN_ID, "--text", "exact run"]
        ),
        steered
    );
    let calls = server.join().unwrap();
    assert_eq!(calls[0]["method"], "cancel");
    assert_eq!(
        calls[0]["params"],
        json!({"agent_id":AGENT_ID,"run_id":null})
    );
    assert_eq!(calls[1]["method"], "cancel");
    assert_eq!(
        calls[1]["params"],
        json!({"agent_id":AGENT_ID,"run_id":RUN_ID})
    );
    assert_eq!(calls[2]["method"], "steer");
    assert_eq!(
        calls[2]["params"],
        json!({"agent_id":AGENT_ID,"run_id":null,"text":"continue"})
    );
    assert_eq!(calls[3]["method"], "steer");
    assert_eq!(
        calls[3]["params"],
        json!({"agent_id":AGENT_ID,"run_id":RUN_ID,"text":"exact run"})
    );
    fixture.assert_homes_absent();
}

/// Catalog reads keep filter/null semantics, JSON results and exact guide text output.
#[test]
fn operator_catalog_reads_keep_filters_and_output_formats() {
    let fixture = Fixture::new();
    let models =
        json!({"schema_version":2,"providers":[{"provider":"p","models":[{"model":"m"}]}]});
    let limits = json!({"schema_version":2,"revision":4,"limits":[]});
    let order = json!({"schema_version":2,"providers":[{"provider":"p"}]});
    let server = serve(
        &fixture.socket,
        vec![
            models.clone(),
            models.clone(),
            limits.clone(),
            json!("filtered guide\n"),
            json!("default guide"),
            order.clone(),
            order.clone(),
        ],
    );
    assert_eq!(output_json(fixture.operator(None), &["models"]), models);
    assert_eq!(
        output_json(
            fixture.operator(None),
            &[
                "models",
                "--provider",
                "p",
                "--profile",
                "review",
                "--model",
                "m"
            ]
        ),
        models
    );
    assert_eq!(output_json(fixture.operator(None), &["limits"]), limits);
    for (args, expected) in [
        (
            vec![
                "delegation-guide",
                "--provider",
                "p",
                "--profile",
                "review",
                "--model",
                "m",
            ],
            "filtered guide\n\n",
        ),
        (vec!["delegation-guide"], "default guide\n"),
    ] {
        let output = fixture
            .operator(None)
            .args(args)
            .stdin(Stdio::null())
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        assert_eq!(output.stdout, expected.as_bytes());
    }
    assert_eq!(
        output_json(fixture.operator(None), &["capacity", "order"]),
        order
    );
    assert_eq!(
        output_json(
            fixture.operator(None),
            &["capacity", "order", "--model", "m"]
        ),
        order
    );
    let calls = server.join().unwrap();
    assert_eq!(calls[0]["method"], "models");
    assert_eq!(
        calls[0]["params"],
        json!({"provider":null,"profile":null,"model":null})
    );
    assert_eq!(calls[1]["method"], "models");
    assert_eq!(
        calls[1]["params"],
        json!({"provider":"p","profile":"review","model":"m"})
    );
    assert_eq!(calls[2]["method"], "limits");
    assert_eq!(calls[2]["params"], json!({}));
    assert_eq!(calls[3]["method"], "delegation_guide");
    assert_eq!(
        calls[3]["params"],
        json!({"provider":"p","profile":"review","model":"m"})
    );
    assert_eq!(calls[4]["method"], "delegation_guide");
    assert_eq!(
        calls[4]["params"],
        json!({"provider":null,"profile":null,"model":null})
    );
    assert_eq!(calls[5]["method"], "capacity_order");
    assert_eq!(calls[5]["params"], json!({"model":null}));
    assert_eq!(calls[6]["method"], "capacity_order");
    assert_eq!(calls[6]["params"], json!({"model":"m"}));
    fixture.assert_homes_absent();
}

/// Broker-side errors retain their public CLI class, message and failure status.
#[test]
fn operator_controls_and_catalog_preserve_broker_error_classes() {
    let fixture = Fixture::new();
    let listener = UnixListener::bind(&fixture.socket).unwrap();
    let errors = [
        (-32000, Some("AgentNotFound"), "unknown agent"),
        (
            -32000,
            Some("StateTransitionError"),
            "invalid agent transition",
        ),
        (-32602, None, "unknown provider"),
        (-32000, Some("Unsupported"), "catalog unavailable"),
    ];
    let server = thread::spawn(move || {
        for (code, kind, message) in errors {
            let mut stream = accept(&listener);
            stream
                .set_read_timeout(Some(Duration::from_secs(5)))
                .unwrap();
            let mut input = BufReader::new(stream.try_clone().unwrap());
            let mut line = String::new();
            input.read_line(&mut line).unwrap();
            let request: Value = serde_json::from_str(&line).unwrap();
            let mut error = json!({"code":code,"message":message});
            if let Some(kind) = kind {
                error["data"] = json!({"code":kind,"message":message});
            }
            writeln!(
                stream,
                "{}",
                json!({"jsonrpc":"2.0","id":request["id"],"error":error})
            )
            .unwrap();
        }
    });
    for (args, kind, message) in [
        (vec!["cancel", AGENT_ID], "AgentNotFound", "unknown agent"),
        (
            vec!["steer", AGENT_ID, "--text", "continue"],
            "StateTransitionError",
            "invalid agent transition",
        ),
        (
            vec!["models", "--provider", "unknown"],
            "ValidationError",
            "unknown provider",
        ),
        (
            vec!["capacity", "order", "--model", "m"],
            "Unsupported",
            "catalog unavailable",
        ),
    ] {
        let output = fixture
            .operator(None)
            .args(args)
            .stdin(Stdio::null())
            .output()
            .unwrap();
        assert_eq!(output.status.code(), Some(2));
        assert!(output.stdout.is_empty());
        assert_eq!(
            serde_json::from_slice::<Value>(&output.stderr).unwrap(),
            json!({"error":{"type":kind,"message":message}})
        );
    }
    server.join().unwrap();
    fixture.assert_homes_absent();
}

/// A one-shot page preserves session, activity and pagination filters exactly.
#[test]
fn operator_agents_forward_query_and_page() {
    let fixture = Fixture::new();
    let page = json!({"items":[],"total":2,"offset":2,"limit":3,"next_offset":null,
        "complete":true,"revision":5,"message_revision":3,"observed_at":10.0});
    let server = serve(&fixture.socket, vec![page.clone()]);
    let output = output_json(
        fixture.operator(Some(uid())),
        &[
            "agents",
            "--active",
            "--offset",
            "2",
            "--limit",
            "3",
            "--session-transport",
            "codex_queue",
            "--session-id",
            "session-1",
            "--session-turn-id",
            "turn-1",
        ],
    );
    assert_eq!(output, page);
    let calls = server.join().unwrap();
    assert_eq!(calls[0]["method"], "list_agents");
    assert_eq!(
        calls[0]["params"],
        json!({
            "active":true,"offset":2,"limit":3,"after_revision":null,
            "after_message_revision":null,"wait_seconds":0.0,
            "orchestrator":{"transport":"codex_queue","external_session_id":"session-1",
                            "external_turn_id":"turn-1"}
        })
    );
    fixture.assert_homes_absent();
}

/// Follow suppresses unchanged observations, advances both cursors and closes its socket on SIGINT.
#[test]
fn operator_agents_follow_preserves_snapshots_and_ctrl_c() {
    let fixture = Fixture::new();
    let page = |phase, revision, message_revision, observed_at| {
        json!({
            "items":[{"agent_id":AGENT_ID,"status":"running","phase":phase,"observed_at":observed_at}],
            "total":2,"offset":1,"limit":2,"next_offset":null,"complete":true,
            "revision":revision,"message_revision":message_revision,"observed_at":observed_at
        })
    };
    let pages = vec![
        page("running", 5, 3, 10),
        page("running", 6, 3, 11),
        page("answering", 7, 9, 12),
    ];
    let expected = vec![pages[0].clone(), pages[2].clone()];
    let listener = UnixListener::bind(&fixture.socket).unwrap();
    let (ready, received) = mpsc::channel();
    let server = thread::spawn(move || {
        let mut calls = Vec::new();
        for response in pages {
            let mut stream = accept(&listener);
            stream
                .set_read_timeout(Some(Duration::from_secs(5)))
                .unwrap();
            let mut line = String::new();
            BufReader::new(stream.try_clone().unwrap())
                .read_line(&mut line)
                .unwrap();
            let request: Value = serde_json::from_str(&line).unwrap();
            writeln!(
                stream,
                "{}",
                json!({"jsonrpc":"2.0","id":request["id"],"result":response})
            )
            .unwrap();
            calls.push(request);
        }
        let mut pending = accept(&listener);
        pending
            .set_read_timeout(Some(Duration::from_secs(10)))
            .unwrap();
        let mut line = String::new();
        BufReader::new(pending.try_clone().unwrap())
            .read_line(&mut line)
            .unwrap();
        calls.push(serde_json::from_str::<Value>(&line).unwrap());
        ready.send(()).unwrap();
        let mut byte = [0];
        assert_eq!(
            pending.read(&mut byte).unwrap(),
            0,
            "SIGINT closes the pending client socket"
        );
        calls
    });
    let child = fixture
        .operator(Some(uid()))
        .args([
            "agents",
            "--follow",
            "--active",
            "--offset",
            "1",
            "--limit",
            "2",
            "--session-transport",
            "codex_queue",
            "--session-id",
            "session-1",
        ])
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    let mut viewer = Mcp::capture(child);
    received
        .recv_timeout(Duration::from_secs(10))
        .expect("pending long-poll deadline");
    let actual: Vec<Value> = (0..2)
        .map(|_| {
            serde_json::from_str(
                &viewer
                    .receive
                    .recv_timeout(Duration::from_secs(10))
                    .expect("snapshot deadline"),
            )
            .unwrap()
        })
        .collect();
    assert_eq!(actual, expected);
    assert!(matches!(
        viewer.receive.try_recv(),
        Err(mpsc::TryRecvError::Empty)
    ));
    assert_eq!(
        // SAFETY: this live, owned child is waiting at the established socket checkpoint.
        unsafe { libc::kill(i32::try_from(viewer.child.id()).unwrap(), libc::SIGINT) },
        0
    );
    let deadline = Instant::now() + Duration::from_secs(10);
    let status = loop {
        if let Some(status) = viewer.child.try_wait().unwrap() {
            break status;
        }
        assert!(Instant::now() < deadline, "viewer did not exit on SIGINT");
        thread::sleep(Duration::from_millis(5));
    };
    assert!(status.success(), "viewer exit: {status}");
    let calls = server.join().unwrap();
    for (request, (revision, message_revision, wait)) in calls.iter().zip([
        (None, None, 0.0),
        (Some(5), Some(3), 25.0),
        (Some(6), Some(3), 25.0),
        (Some(7), Some(9), 25.0),
    ]) {
        assert_eq!(request["method"], "list_agents");
        assert_eq!(
            request["params"],
            json!({"active":true,"offset":1,"limit":2,
            "after_revision":revision,"after_message_revision":message_revision,"wait_seconds":wait,
            "orchestrator":{"transport":"codex_queue","external_session_id":"session-1","external_turn_id":null}})
        );
    }
    drop(viewer);
    fixture.assert_homes_absent();
}

/// Corrupt page cursors or bytes cannot produce even a partial CLI answer.
#[test]
fn operator_answer_rejects_corrupt_pages_before_output() {
    for corrupted_offset in [false, true] {
        let fixture = Fixture::new();
        let digest = agent_run_platform::fs::sha256(b"abc");
        let metadata = json!({"answer":{"agent_id":AGENT_ID,"status":"succeeded",
            "available":true,"path":"/private/answer.md","size_bytes":3,"sha256":digest,
            "content":null,"inline_complete":false,"relative_path":"answer.md",
            "kind":"agent_answer","media_type":agent_run_platform::verify::MEDIA_TYPE,"proof_version":2},
            "sequence":1,"inline":true,"proof_token":"f".repeat(64)});
        let chunk = json!({"agent_id":AGENT_ID,"sequence":1,"sha256":digest,"total_bytes":3,
            "offset_bytes":if corrupted_offset {1} else {0},"next_offset":3,
            "content":if corrupted_offset {"abc"} else {"abd"},"complete":true});
        let server = serve(&fixture.socket, vec![metadata, chunk]);
        let output = fixture
            .operator(None)
            .args(["answer", AGENT_ID])
            .stdin(Stdio::null())
            .output()
            .unwrap();
        assert!(!output.status.success());
        assert!(output.stdout.is_empty());
        let error: Value = serde_json::from_slice(&output.stderr).unwrap();
        assert_eq!(
            error["error"]["type"],
            if corrupted_offset {
                "AnswerIntegrityError"
            } else {
                "AnswerHashMismatch"
            }
        );
        let calls = server.join().unwrap();
        assert_eq!(calls[0]["method"], "_cli_answer");
        assert_eq!(calls[1]["method"], "_cli_answer_chunk");
        fixture.assert_homes_absent();
    }
}

/// UID mismatch is checked before request bytes leave the process.
#[test]
fn operator_cli_rejects_untrusted_uid_before_writing() {
    let fixture = Fixture::new();
    let listener = UnixListener::bind(&fixture.socket).unwrap();
    let server = thread::spawn(move || {
        let mut stream = accept(&listener);
        stream.set_nonblocking(true).unwrap();
        let mut byte = [0];
        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            match stream.read(&mut byte) {
                Ok(read) => {
                    assert_eq!(read, 0);
                    break;
                }
                Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                    assert!(Instant::now() < deadline, "client did not close the socket");
                    thread::sleep(Duration::from_millis(10));
                }
                Err(error) => panic!("mock broker read failed: {error}"),
            }
        }
    });
    let output = fixture
        .operator(Some(uid().wrapping_add(1)))
        .arg("capabilities")
        .stdin(Stdio::null())
        .output()
        .unwrap();
    assert!(!output.status.success());
    assert!(String::from_utf8_lossy(&output.stderr).contains("broker belongs to another user"));
    server.join().unwrap();
    fixture.assert_homes_absent();
}

/// Missing endpoints cannot fall back to the private home socket or configure logging.
#[test]
fn operator_failure_never_falls_back_to_local_home() {
    let fixture = Fixture::new();
    fs::create_dir(&fixture.local_home).unwrap();
    let local_socket = fixture.local_home.join("api.sock");
    let local = UnixListener::bind(&local_socket).unwrap();
    local.set_nonblocking(true).unwrap();
    for args in [
        vec!["capabilities"],
        vec!["agents"],
        vec!["agents", "--follow"],
    ] {
        let output = fixture
            .operator(Some(uid()))
            .args(args)
            .stdin(Stdio::null())
            .output()
            .unwrap();
        assert!(!output.status.success());
        assert!(output.stdout.is_empty());
        assert!(String::from_utf8_lossy(&output.stderr).contains("BrokerUnavailable"));
    }
    assert_eq!(
        local.accept().unwrap_err().kind(),
        std::io::ErrorKind::WouldBlock
    );
    assert_eq!(fs::read_dir(&fixture.local_home).unwrap().count(), 1);
    assert!(!fixture.env_home.exists());
    assert!(!fixture.caller_home.exists());
}

/// Unsupported and partially configured invocations fail before private state or stdin.
#[test]
fn operator_refusals_are_strict_for_unsupported_commands() {
    let fixture = Fixture::new();
    for args in [
        vec!["init"],
        vec!["doctor"],
        vec![
            "bind",
            AGENT_ID,
            "--session-transport",
            "codex_queue",
            "--session-id",
            "session",
        ],
        vec![
            "context",
            "--session-transport",
            "codex_queue",
            "--session-id",
            "session",
        ],
        vec!["api", "serve"],
        vec!["capacity", "collect", "--once"],
        vec!["capacity", "launchd", "--binary", "/bin/true"],
        vec!["delivery", "status", AGENT_ID],
        vec!["login", "codex"],
        vec!["accounts", "list"],
        vec!["storage", "status"],
        vec!["_worker-mcp"],
    ] {
        let output = fixture
            .operator(Some(uid()))
            .args(&args)
            .stdin(Stdio::null())
            .output()
            .unwrap();
        assert!(!output.status.success(), "unexpected success for {args:?}");
        assert!(
            output.stdout.is_empty(),
            "stdout for {args:?}: {:?}",
            output.stdout
        );
        let expected = if args[0] == "capacity" {
            "unrecognized subcommand"
        } else {
            "unavailable in operator mode"
        };
        assert!(
            String::from_utf8_lossy(&output.stderr).contains(expected),
            "stderr for {args:?}: {}",
            String::from_utf8_lossy(&output.stderr)
        );
    }
    for args in [
        vec!["--broker-uid", "1", "capabilities"],
        vec!["hook", "context", "--broker-uid", "1"],
        vec!["hook", "context", "--broker-socket", ""],
    ] {
        let output = fixture
            .command()
            .args(&args)
            .stdin(Stdio::null())
            .output()
            .unwrap();
        assert!(!output.status.success(), "unexpected success for {args:?}");
        assert!(
            output.stdout.is_empty(),
            "context hook error was masked for {args:?}"
        );
    }
    fixture.assert_homes_absent();
}

/// Ordinary unconfigured CLI still calls the same-UID private home socket.
#[test]
fn ordinary_cli_keeps_private_same_uid_path() {
    let fixture = Fixture::new();
    fs::create_dir(&fixture.local_home).unwrap();
    let server = serve(
        &fixture.local_home.join("api.sock"),
        vec![json!({"local":true})],
    );
    assert_eq!(
        output_json(fixture.command(), &["capabilities"]),
        json!({"local":true})
    );
    assert_eq!(server.join().unwrap()[0]["method"], "capabilities");
    assert!(!fixture.env_home.exists());
    assert!(!fixture.caller_home.exists());
}

/// Own one child and stdout reader so assertion failures cannot leak either.
struct Mcp {
    child: Child,
    input: Option<ChildStdin>,
    receive: mpsc::Receiver<String>,
    reader: Option<thread::JoinHandle<()>>,
}

impl Mcp {
    /// Start the operator MCP transport with a bounded asynchronous output reader.
    fn start(fixture: &Fixture) -> Self {
        let child = fixture
            .operator(Some(uid()))
            .arg("mcp")
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap();
        Self::capture(child)
    }

    /// Capture one child with piped stdout, preserving any stdin for MCP requests.
    fn capture(mut child: Child) -> Self {
        let input = child.stdin.take();
        let stdout = child.stdout.take().unwrap();
        let (send, receive) = mpsc::channel();
        let reader = thread::spawn(move || {
            for line in BufReader::new(stdout).lines() {
                if send.send(line.unwrap()).is_err() {
                    break;
                }
            }
        });
        Self {
            child,
            input,
            receive,
            reader: Some(reader),
        }
    }

    /// Send one protocol message and wait only for replies with request IDs.
    fn send(&mut self, message: Value) -> Option<Value> {
        let input = self.input.as_mut().unwrap();
        writeln!(input, "{message}").unwrap();
        input.flush().unwrap();
        message.get("id")?;
        let line = self
            .receive
            .recv_timeout(Duration::from_secs(10))
            .expect("MCP reply deadline");
        Some(serde_json::from_str(&line).unwrap())
    }
}

impl Drop for Mcp {
    /// Close and reap the fixture-owned MCP child, including on assertion failure.
    fn drop(&mut self) {
        drop(self.input.take());
        let _ = self.child.kill();
        let _ = self.child.wait();
        if let Some(reader) = self.reader.take() {
            let _ = reader.join();
        }
    }
}

/// MCP initializes normally and forwards an advertised tool through the selected socket.
#[test]
fn operator_mcp_initializes_and_forwards_to_broker() {
    let fixture = Fixture::new();
    let server = serve(
        &fixture.socket,
        vec![json!({"topic":"index","text":"operator fixture"})],
    );
    let mut mcp = Mcp::start(&fixture);
    let initialization = mcp.send(json!({"jsonrpc":"2.0","id":1,"method":"initialize","params":{
        "protocolVersion":"2025-11-25","capabilities":{},"clientInfo":{"name":"fixture","version":"1"}}})).unwrap();
    assert_eq!(initialization["result"]["serverInfo"]["name"], "agent-run");
    mcp.send(json!({"jsonrpc":"2.0","method":"notifications/initialized"}));
    let reply = mcp.send(json!({"jsonrpc":"2.0","id":2,"method":"tools/call", "params":{"name":"doc","arguments":{"topic":"index"}}})).unwrap();
    assert_ne!(reply["result"]["isError"], true, "{reply}");
    assert!(
        reply["result"]["content"][0]["text"]
            .as_str()
            .unwrap()
            .contains("operator fixture")
    );
    drop(mcp);
    let calls = server.join().unwrap();
    assert_eq!(calls[0]["method"], "doc");
    assert_eq!(calls[0]["params"], json!({"topic":"index"}));
    fixture.assert_homes_absent();
}

/// The listener's group and path must be paired and cannot replace the private socket.
#[test]
fn api_operator_listener_flags_require_a_pair_and_disallow_custom_private_socket() {
    use agent_run::cli::{Api, Cli, Command as CliCommand};
    let parsed = Cli::try_parse_from([
        "agent-run",
        "api",
        "serve",
        "--operator-socket",
        "/run/agent-run/operator.sock",
        "--operator-gid",
        "42",
    ])
    .unwrap();
    assert!(matches!(
        parsed.command,
        CliCommand::Api {
            command: Api::Serve {
                socket: None,
                operator_socket: Some(_),
                operator_gid: Some(42),
                shared_config: None,
            }
        }
    ));
    for args in [
        vec![
            "agent-run",
            "api",
            "serve",
            "--operator-socket",
            "/run/agent-run/operator.sock",
        ],
        vec!["agent-run", "api", "serve", "--operator-gid", "42"],
        vec![
            "agent-run",
            "api",
            "serve",
            "--socket",
            "/run/private.sock",
            "--operator-socket",
            "/run/operator.sock",
            "--operator-gid",
            "42",
        ],
        vec![
            "agent-run",
            "api",
            "serve",
            "--operator-socket",
            "/run/operator.sock",
            "--operator-gid",
            "nope",
        ],
    ] {
        assert!(Cli::try_parse_from(&args).is_err(), "accepted {args:?}");
    }
    let fixture = Fixture::new();
    let output: Output = fixture
        .command()
        .args(["api", "serve", "--operator-gid", "42"])
        .stdin(Stdio::null())
        .output()
        .unwrap();
    assert!(!output.status.success());
    fixture.assert_homes_absent();
}
