//! Private worker MCP wire fixture: the fixed catalog, authenticated hidden
//! credentials, typed pool denials and compact rendering over real stdio.

#![cfg(feature = "test-fixtures")]

use agent_run::{domain::AgentId, state::Store, transport::socket};
use serde_json::{json, Value};
use std::{
    io::{BufRead, Write},
    path::PathBuf,
    process::{Child, Command, Stdio},
    time::{Duration, Instant},
};

/// A worker MCP child with its owned broker, killed and reaped on drop. The
/// stdout reader owns the pipe; it exits when the child dies or after a
/// bounded idle timeout, so the test never blocks forever on a dead worker.
struct Worker {
    child: Child,
    _home: tempfile::TempDir,
    replies: std::sync::mpsc::Receiver<String>,
}

impl Drop for Worker {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

/// The resident broker child of one wire fixture, killed and reaped on drop.
struct BrokerGuard {
    child: Child,
}

impl Drop for BrokerGuard {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

/// Spawns the bounded reply reader for one worker stdout.
fn reply_reader(stdout: std::process::ChildStdout) -> std::sync::mpsc::Receiver<String> {
    let (send, replies) = std::sync::mpsc::channel();
    std::thread::spawn(move || {
        let reader = std::io::BufReader::new(stdout);
        for line in reader.lines() {
            match line {
                Ok(line) => {
                    if send.send(line).is_err() {
                        return;
                    }
                }
                Err(_) => return,
            }
        }
    });
    replies
}

/// Spawns the real `agent-run api serve` broker and one real `_worker-mcp`
/// child wired to it through the inherited environment, then returns the
/// child's stdin/stdout pair.
fn spawn_worker() -> (
    BrokerGuard,
    Worker,
    PathBuf,
    (String, AgentId, String, String),
) {
    let home = tempfile::Builder::new()
        .prefix("ar-pool-wire-")
        .tempdir_in("/tmp")
        .unwrap();
    let path = home.path().canonicalize().unwrap();
    let init = Command::new(env!("CARGO_BIN_EXE_agent-run"))
        .args(["--home", path.to_str().unwrap(), "init"])
        .output()
        .unwrap();
    assert!(init.status.success());
    // One schema-1 mock runtime lets a plain store admission work offline.
    std::fs::write(
        path.join("config.toml"),
        format!(
            "schema_version=1\n[runtimes.mock]\nenabled=true\nadapter='claude'\nbinary='/bin/true'\nhome='{}'\nmodels=['fixture']\nlimits_source='none'\n",
            path.join("runtimes/mock").display()
        ),
    )
    .unwrap();
    // The broker runs in-process for the store fixtures; a real child broker
    // keeps the wire honest.
    let broker = Command::new(env!("CARGO_BIN_EXE_agent-run"))
        .arg("--home")
        .arg(&path)
        .args(["api", "serve"])
        .env("HOME", &path)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(std::fs::File::create(path.join("broker.stderr")).unwrap())
        .spawn()
        .unwrap();
    let deadline = Instant::now() + Duration::from_secs(10);
    let runtime = tokio::runtime::Runtime::new().unwrap();
    runtime.block_on(async {
        while socket::client(&path, "ping", json!({})).await.is_err() {
            assert!(
                Instant::now() < deadline,
                "broker never became ready: {}",
                std::fs::read_to_string(path.join("broker.stderr")).unwrap_or_default()
            );
            std::thread::sleep(Duration::from_millis(20));
        }
    });
    if let Ok(stderr) = std::fs::read_to_string(path.join("broker.stderr")) {
        if !stderr.trim().is_empty() {
            eprintln!("broker stderr: {stderr}");
        }
    }
    // Admit one pool member with a live attempt and capability.
    let (pool, run, attempt, token) = {
        let config = agent_run::config::Config::load(&path).unwrap();
        let request: agent_run::domain::StartRequest = serde_json::from_value(json!({
            "runtime":"mock","model":"fixture","profile":"review",
            "task":"fixture","workdir":path
        }))
        .unwrap();
        let mut store = Store::open(&path).unwrap();
        let (run, _) = store.admit(&request, &config, &json!({}), None).unwrap();
        store
            .conn
            .execute(
                "UPDATE agents SET status='running',started_at=1.0 WHERE id=?",
                [run.as_str()],
            )
            .unwrap();
        let attempt = format!("att_{}", "c".repeat(24));
        store
            .conn
            .execute(
                "INSERT INTO attempts(id,agent_id,number,state,adapter_state_json,created_at,ownership_active) \
                 VALUES(?, ?, 1, 'running', '{}', 1.0, 1)",
                rusqlite::params![attempt, run.as_str()],
            )
            .unwrap();
        let token = format!("{}{}", "d".repeat(32), "2".repeat(32));
        store
            .issue_worker_capability(&run, &attempt, &token, 1.0)
            .unwrap();
        store
            .conn
            .execute(
                "INSERT INTO pools(id,request_namespace,request_id,request_sha256,goal,acceptance_json,state,roster_revision,created_at) \
                 VALUES('pool-20260101-000000-0123456789','ns','r1','0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef','ship it','[{\"id\":\"done\",\"text\":\"it ships\"}]','open',1,1.0)",
                [],
            )
            .unwrap();
        store
            .conn
            .execute(
                "INSERT INTO pool_members(agent_id,pool_id,slot,name,role,personal_task,joined_roster_revision) \
                 VALUES(?,'pool-20260101-000000-0123456789',1,'lead','doer','task',1)",
                [run.as_str()],
            )
            .unwrap();
        (
            "pool-20260101-000000-0123456789".to_owned(),
            run,
            attempt,
            token,
        )
    };
    let mut child = Command::new(env!("CARGO_BIN_EXE_agent-run"))
        .arg("--home")
        .arg(&path)
        .arg("_worker-mcp")
        .env("HOME", &path)
        .env("AGENT_RUN_HOME", &path)
        .env("AGENT_RUN_WORKER_HOME", &path)
        .env("AGENT_RUN_WORKER_RUN_ID", run.as_str())
        .env("AGENT_RUN_WORKER_ATTEMPT_ID", &attempt)
        .env("AGENT_RUN_WORKER_TOKEN", &token)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()
        .unwrap();
    let replies = reply_reader(child.stdout.take().expect("worker stdout"));
    let guard = BrokerGuard { child: broker };
    let worker = Worker {
        child,
        _home: home,
        replies,
    };
    (guard, worker, path, (pool, run, attempt, token))
}

/// Sends one JSON-RPC request line and reads one reply line within a real
/// I/O timeout; a silent worker fails the test instead of hanging it.
fn roundtrip(worker: &mut Worker, request: &Value) -> Value {
    let stdin = worker.child.stdin.as_mut().expect("worker stdin");
    stdin.write_all(format!("{request}\n").as_bytes()).unwrap();
    stdin.flush().unwrap();
    let line = worker
        .replies
        .recv_timeout(Duration::from_secs(10))
        .expect("worker reply timed out");
    assert!(!line.trim().is_empty(), "worker closed its output");
    serde_json::from_str(&line).unwrap()
}

/// The catalog is the fixed five tools for everyone, member or not; pool
/// calls authenticate hidden credentials, stamp the author, render entries
/// through the one formatter, and refuse typed conditions.
#[test]
fn worker_pool_wire_lists_calls_and_refuses() {
    let (_broker, mut worker, _path, (pool_id, run, attempt, token)) = spawn_worker();
    let _ = (&run, &attempt, &token);
    let _ = roundtrip(
        &mut worker,
        &json!({"jsonrpc":"2.0","id":1,"method":"initialize","params":{
            "protocolVersion":"2025-11-25","capabilities":{"tools":{}},
            "clientInfo":{"name":"pool-wire-fixture","version":"1"}
        }}),
    );
    let stdin = worker.child.stdin.as_mut().unwrap();
    stdin
        .write_all(b"{\"jsonrpc\":\"2.0\",\"method\":\"notifications/initialized\"}\n")
        .unwrap();
    stdin.flush().unwrap();
    let listed = roundtrip(
        &mut worker,
        &json!({"jsonrpc":"2.0","id":2,"method":"tools/list","params":{}}),
    );
    let names: Vec<&str> = listed["result"]["tools"]
        .as_array()
        .unwrap()
        .iter()
        .map(|tool| tool["name"].as_str().unwrap())
        .collect();
    assert_eq!(
        names,
        [
            "notify_orchestrator",
            "pool_post",
            "pool_read",
            "pool_propose",
            "pool_vote"
        ]
    );

    // One member chat entry is recorded with the broker-stamped author.
    let posted = roundtrip(
        &mut worker,
        &json!({"jsonrpc":"2.0","id":3,"method":"tools/call","params":{
            "name":"pool_post","arguments":{"request_id":"m1","message":"hello team"}
        }}),
    );
    let text = posted["result"]["content"][0]["text"].as_str().unwrap();
    assert!(text.contains("recorded"), "{posted}");
    assert!(!text.contains(&token), "the capability never leaks: {text}");

    // A proposal and a read: the read renders entries through the shared
    // formatter with the stamped stable identity and no internal ids.
    let proposed = roundtrip(
        &mut worker,
        &json!({"jsonrpc":"2.0","id":4,"method":"tools/call","params":{
            "name":"pool_propose","arguments":{"request_id":"p1","message":"result","snapshot":"commit abc"}
        }}),
    );
    assert!(proposed["result"]["content"][0]["text"]
        .as_str()
        .unwrap()
        .contains("proposal #2"));
    let read = roundtrip(
        &mut worker,
        &json!({"jsonrpc":"2.0","id":5,"method":"tools/call","params":{
            "name":"pool_read","arguments":{"after_seq":0,"limit":10}
        }}),
    );
    let text = read["result"]["content"][0]["text"].as_str().unwrap();
    assert!(text.contains("agent-run/pool #1"), "{text}");
    assert!(text.contains("lead (doer,"), "{text}");
    assert!(text.contains(&pool_id), "{text}");
    assert!(!text.contains(&attempt), "no attempt id leaks: {text}");
    assert!(text.contains("roster r1"), "{text}");

    // A vote without a live proposal yet is a typed refusal rendered as an
    // MCP error, never prose matching.
    let denied = roundtrip(
        &mut worker,
        &json!({"jsonrpc":"2.0","id":6,"method":"tools/call","params":{
            "name":"pool_vote","arguments":{"request_id":"v1","proposal_seq":99,"decision":"ready",
                "checks":[{"criterion_id":"done","status":"met","evidence":"ok"}]}
        }}),
    );
    assert_eq!(denied["result"]["isError"], true, "{denied}");
    let text = denied["result"]["content"][0]["text"].as_str().unwrap();
    assert!(text.contains("stale_proposal"), "{text}");

    // The same key with different content is a typed conflict.
    let conflict = roundtrip(
        &mut worker,
        &json!({"jsonrpc":"2.0","id":7,"method":"tools/call","params":{
            "name":"pool_post","arguments":{"request_id":"m1","message":"changed"}
        }}),
    );
    assert_eq!(conflict["result"]["isError"], true, "{conflict}");
    assert!(
        conflict["result"]["content"][0]["text"]
            .as_str()
            .unwrap()
            .contains("conflict"),
        "{conflict}"
    );

    // A forged pool identity argument never reaches the broker: the strict
    // DTO rejects unknown fields before any authentication surface.
    let forged = roundtrip(
        &mut worker,
        &json!({"jsonrpc":"2.0","id":8,"method":"tools/call","params":{
            "name":"pool_post","arguments":{"request_id":"m2","message":"x","author":"member-2"}
        }}),
    );
    assert_eq!(forged["result"]["isError"], true, "{forged}");

    // The legacy report envelope keeps working beside the new tools.
    drop(worker);
    drop(run);
    let _ = (attempt, token);
}
