//! Real public-boundary checks for provider admission: a disposable schema-2
//! home, the real `api serve` broker, the real CLI and MCP child processes,
//! and the exported socket client, all driving the fake engine only.

use agent_run::transport::socket::BrokerClient;
use agent_run_domain::{views::StartResult, Error, ProviderStartRequest};
use serde_json::{json, Value};
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
    /// Keeps the short Unix-socket base directory alive.
    _temp: tempfile::TempDir,
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
                "schema_version = 2\n[harnesses.codex]\nbinary = \"{fixture}\"\nhome = \"{h}/codex\"\n[harnesses.claude-code]\nbinary = \"{fixture}\"\nhome = \"{h}/claude\"\n[providers.glm-user]\nharness = \"claude-code\"\nconnection = {{ kind = \"custom\", endpoint = \"https://gateway.example/api\", protocol = \"messages\" }}\nauth_family = \"anthropic\"\nlimits_source = \"none\"\n[[providers.glm-user.models]]\nid = \"fixture\"\nnative_model = \"fixture\"\n[[providers.glm-user.bindings]]\nlabel = \"work\"\naccount = \"acct-work\"\n",
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
        let child = Command::new(env!("CARGO_BIN_EXE_agent-run"))
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
            _temp: temp,
            home,
            child,
        }
    }

    /// The strict provider start request for `task` with `request_id`.
    fn request(&self, task: &str, request_id: &str) -> ProviderStartRequest {
        serde_json::from_value(json!({
            "provider":"glm-user","model":"fixture","profile":"review",
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
    /// client's rendered `PublicError.kind` plus retained broker code.
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

        let mut mcp = Command::new(env!("CARGO_BIN_EXE_agent-run"))
            .arg("--home")
            .arg(&self.home)
            .arg("mcp")
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .spawn()
            .unwrap();
        let mut input = mcp.stdin.take().unwrap();
        let mut replies = BufReader::new(mcp.stdout.take().unwrap());
        for message in [
            json!({"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2025-06-18","capabilities":{},"clientInfo":{"name":"test","version":"0"}}}),
            json!({"jsonrpc":"2.0","method":"notifications/initialized"}),
            json!({"jsonrpc":"2.0","id":2,"method":"tools/call","params":{"name":"start","arguments":{"provider":"glm-user","model":"fixture","profile":"review","task":"fixture:answer","workdir":self.home}}}),
        ] {
            writeln!(input, "{message}").unwrap();
        }
        input.flush().unwrap();
        let mut reply = Value::Null;
        for _ in 0..2 {
            let mut line = String::new();
            replies.read_line(&mut line).unwrap();
            reply = serde_json::from_str(&line).unwrap();
        }
        drop(input);
        let _ = mcp.wait();
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
        assert!(
            matches!(&error, Error::Broker { broker_error_code: Some(found), .. } if found == code),
            "socket: {error:?}"
        );
        assert_eq!(error.public().kind, code, "socket render");
    }
}

impl Drop for Broker {
    /// Terminates only this test's broker.
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

/// Runs the real CLI against `home`.
fn cli(home: &Path, args: &[&str]) -> Output {
    Command::new(env!("CARGO_BIN_EXE_agent-run"))
        .arg("--home")
        .arg(home)
        .args(args)
        .output()
        .unwrap()
}

/// The exported socket client sends the strict provider request the real
/// schema-2 dispatcher accepts and returns the admitted attempt id, and the
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
    assert!(started
        .attempt_id
        .as_deref()
        .is_some_and(|id| id.starts_with("att_")));
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
    assert!(typed.attempt_id.is_some());
    broker.wait_terminal(typed.agent_id.as_str());
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

    assert!(cli(&broker.home, &["accounts", "disable", "acct-work"])
        .status
        .success());
    broker.assert_refused("no_eligible_account").await;
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
