//! External collector contract, account isolation, history retention and process lifetime.
use agent_run_config::provider_config::ProviderConfig;
use agent_run_core::capacity::{collectors::collect_providers, executable};
use agent_run_domain::catalog::{AccountRecord, CollectorBinding};
use agent_run_platform::process;
use serde_json::json;
use std::{fs, os::unix::fs::PermissionsExt, path::Path, time::Duration};

/// Builds a direct executable binding with a deliberately non-shell argument.
fn binding(script: &Path) -> CollectorBinding {
    serde_json::from_value(
        json!({"command":"/bin/sh","args":[script, "literal $(touch INJECTED)"],
        "source":"fixture", "timeout_seconds":3}),
    )
    .unwrap()
}

/// Creates a private script fixture and returns its configured executable binding.
fn script(root: &Path, body: &str) -> CollectorBinding {
    let path = root.join("collector.sh");
    fs::write(&path, body).unwrap();
    binding(&path)
}

/// Creates an isolated account store and a public schema-2 config with two aliases.
fn configured(root: &Path, command: &CollectorBinding) -> ProviderConfig {
    let secret = root.join("credential");
    fs::write(&secret, "private-fixture-token").unwrap();
    fs::set_permissions(&secret, fs::Permissions::from_mode(0o600)).unwrap();
    let mut store = agent_run_store::Store::initialize(root).unwrap();
    store
        .register_account(&AccountRecord {
            account_id: "acct-test".parse().unwrap(),
            auth_family: "anthropic".parse().unwrap(),
            secret_ref: format!("file:{}", secret.display()).parse().unwrap(),
            status: agent_run_domain::catalog::AccountStatus::Enabled,
        })
        .unwrap();
    let provider = json!({"harness":"claude-code","connection":{"kind":"native"},"auth_family":"anthropic",
        "limits_source":"exec","collector":command,"models":[{"id":"sonnet"}],
        "bindings":[{"label":"main","account":"acct-test"}]});
    let mut config: ProviderConfig = serde_json::from_value(json!({"schema_version":2,
        "harnesses":{"codex":{"binary":"/bin/true","home":root.join("codex")},
                     "claude-code":{"binary":"/bin/true","home":root.join("claude")}},
        "providers":{"first":provider,"alias":provider}}))
    .unwrap();
    config.validate(root).unwrap();
    fs::write(root.join("config.toml"), toml::to_string(&config).unwrap()).unwrap();
    config
}

/// A real script proves credentials enter stdin, argv stays literal and model scope is bound.
#[tokio::test]
async fn executable_accounts_aliases_and_literal_arguments() {
    let root = tempfile::tempdir().unwrap();
    let command = script(
        root.path(),
        r#"set -eu
printf x >> calls
[ "$1" = 'literal $(touch INJECTED)' ]
jq -e 'if .version != 1 or .account.id != "acct-test" or .auth.token != "private-fixture-token" then error("context") else
{version:1,windows:[{pool:"shared",window:"five_hour",models:(.models|keys),remaining_percent:73,observed_at:.now}]} end'
"#,
    );
    let config = configured(root.path(), &command);
    let result = agent_run_core::capacity::sources::collect(root.path())
        .await
        .unwrap();
    assert_eq!(result["ok"], true, "{result}");
    assert_eq!(result["results"].as_array().unwrap().len(), 1);
    assert_eq!(fs::read_to_string(root.path().join("calls")).unwrap(), "x");
    assert!(!root.path().join("INJECTED").exists());
    assert!(!result.to_string().contains("private-fixture-token"));
    let store = agent_run_store::Store::open(root.path()).unwrap();
    let remaining: f64 = store
        .conn
        .query_row("SELECT remaining_percent FROM capacity_samples", [], |r| {
            r.get(0)
        })
        .unwrap();
    assert_eq!(remaining, 73.0);
    // A conflicting alias must be refused before another executable runs.
    let mut conflicting = config;
    conflicting
        .providers
        .get_mut(&"alias".parse().unwrap())
        .unwrap()
        .collector
        .as_mut()
        .unwrap()
        .args
        .push("conflict".into());
    assert!(collect_providers(root.path(), &conflicting).await.is_err());
    assert_eq!(fs::read_to_string(root.path().join("calls")).unwrap(), "x");
}

/// Script replacement is effective on the next round; failure neither erases facts nor retries immediately.
#[tokio::test]
async fn changed_script_failure_preserves_facts_and_backs_off() {
    let root = tempfile::tempdir().unwrap();
    let command = script(
        root.path(),
        r#"jq '{version:1,windows:[{pool:"shared",window:"five_hour",models:(.models|keys),remaining_percent:50,observed_at:.now}]}'"#,
    );
    let config = configured(root.path(), &command);
    assert_eq!(
        collect_providers(root.path(), &config).await.unwrap()["ok"],
        true
    );
    fs::write(
        root.path().join("collector.sh"),
        "cat >/dev/null; echo private-fixture-token >&2; exit 7",
    )
    .unwrap();
    let failed = collect_providers(root.path(), &config).await.unwrap();
    assert_eq!(failed["ok"], false, "{failed}");
    assert!(!failed.to_string().contains("private-fixture-token"));
    let later = collect_providers(root.path(), &config).await.unwrap();
    assert_eq!(later["results"][0]["issues"], json!(["backoff"]));
    let store = agent_run_store::Store::open(root.path()).unwrap();
    let count: i64 = store
        .conn
        .query_row("SELECT count(*) FROM capacity_samples", [], |r| r.get(0))
        .unwrap();
    assert_eq!(count, 1);
}

/// An emitted credential, a foreign model and malformed output cannot become durable quota facts.
#[tokio::test]
async fn invalid_or_secret_output_never_persists() {
    for expression in [
        r#"{version:1,windows:[{pool:.auth.token,window:"five_hour",models:(.models|keys),remaining_percent:50,observed_at:.now}]}"#,
        r#"{version:1,windows:[{pool:"shared",window:"five_hour",models:["unconfigured"],remaining_percent:50,observed_at:.now}]}"#,
        r#"{version:1,windows:[],extra:"unknown"}"#,
    ] {
        let root = tempfile::tempdir().unwrap();
        let command = script(root.path(), &format!("jq '{expression}'"));
        let config = configured(root.path(), &command);
        let result = collect_providers(root.path(), &config).await.unwrap();
        assert_eq!(result["ok"], false, "{result}");
        assert!(!result.to_string().contains("private-fixture-token"));
        let store = agent_run_store::Store::open(root.path()).unwrap();
        let count: i64 = store
            .conn
            .query_row("SELECT count(*) FROM capacity_samples", [], |r| r.get(0))
            .unwrap();
        assert_eq!(count, 0);
    }
}

/// A failed command has no built-in fallback, even when its source is a former provider name.
#[tokio::test]
async fn missing_command_and_empty_catalog_are_explicit() {
    let root = tempfile::tempdir().unwrap();
    let mut command = binding(&root.path().join("missing.sh"));
    command.command = root.path().join("missing-executable");
    command.source = "glm-quota".into();
    let mut config = configured(root.path(), &command);
    let failed = collect_providers(root.path(), &config).await.unwrap();
    assert_eq!(
        failed["results"][0]["issues"],
        json!(["collector_spawn_failed"])
    );
    config.providers.clear();
    config.harnesses.clear();
    assert_eq!(
        collect_providers(root.path(), &config).await.unwrap(),
        json!({"ok":true,"results":[]})
    );
}

/// Returns identities captured from a fixture's PID file; keeps PID reuse distinguishable.
async fn identities(root: &Path) -> Vec<process::Identity> {
    for _ in 0..100 {
        if let Ok(text) = fs::read_to_string(root.join("pids")) {
            let observed: Vec<_> = text
                .split_whitespace()
                .filter_map(|s| s.parse().ok())
                .filter_map(|pid| process::inspect(pid).ok())
                .collect();
            if observed.len() == 2 {
                return observed;
            }
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    panic!("fixture processes did not start");
}

/// Checks exact captured processes, never signals a bare test PID after a failure.
fn assert_gone(identities: &[process::Identity]) {
    for identity in identities {
        assert!(matches!(
            process::observe(
                Some(identity.pid),
                Some(&identity.token),
                Some(identity.birth)
            ),
            process::ProcessState::Dead | process::ProcessState::Reused
        ));
    }
}

/// Both deadline expiry and caller cancellation clean a TERM-resistant script and its child.
#[tokio::test]
async fn timeout_and_cancel_clean_children() {
    for cancel in [false, true] {
        let root = tempfile::tempdir().unwrap();
        let mut command=script(root.path(),"cat >/dev/null; trap '' TERM; sleep 30 & child=$!; printf '%s %s' $$ $child >pids; wait");
        command.timeout_seconds = 1;
        let cwd = root.path().to_owned();
        let task =
            tokio::spawn(
                async move { executable::run(&command, &json!({"version":1}), &cwd).await },
            );
        let owned = identities(root.path()).await;
        tokio::time::sleep(Duration::from_millis(100)).await;
        if cancel {
            task.abort();
            assert!(task.await.unwrap_err().is_cancelled());
        } else {
            assert_eq!(task.await.unwrap(), Err("collector_timeout"));
        }
        assert_gone(&owned);
    }
}

/// A child holding stdout open does not delay observation of the script's own exit.
#[tokio::test]
async fn exited_script_cleans_inherited_pipes_without_waiting_for_deadline() {
    let root = tempfile::tempdir().unwrap();
    let mut command=script(root.path(),"cat >/dev/null; sleep 30 & child=$!; printf '%s %s' $$ $child >pids; echo '{\"version\":1,\"windows\":[]}'; sleep 0.3");
    command.timeout_seconds = 20;
    let cwd = root.path().to_owned();
    let started = std::time::Instant::now();
    let task =
        tokio::spawn(async move { executable::run(&command, &json!({"version":1}), &cwd).await });
    let owned = identities(root.path()).await;
    assert_eq!(
        task.await.unwrap().unwrap(),
        json!({"version":1,"windows":[]})
    );
    assert!(started.elapsed() < Duration::from_secs(5));
    assert_gone(&owned);
}

/// Streams exceeding the fixed output bound fail without reading unlimited data.
#[tokio::test]
async fn oversized_stdout_is_bounded() {
    let root = tempfile::tempdir().unwrap();
    let command = script(
        root.path(),
        "cat >/dev/null; dd if=/dev/zero bs=1048576 count=3 2>/dev/null; sleep 30",
    );
    assert_eq!(
        executable::run(&command, &json!({}), root.path()).await,
        Err("collector_output_too_large")
    );
}

/// Serves one bounded local HTTP fixture and returns the captured request for auth assertions.
fn endpoint(body: &'static str) -> (String, std::thread::JoinHandle<String>) {
    use std::io::{Read, Write};
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    listener.set_nonblocking(true).unwrap();
    let url = format!("http://{}/quota", listener.local_addr().unwrap());
    let handle = std::thread::spawn(move || {
        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        let mut stream = loop {
            if let Ok((stream, _)) = listener.accept() {
                break stream;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "script did not request fixture endpoint"
            );
            std::thread::sleep(Duration::from_millis(10));
        };
        stream.set_nonblocking(false).unwrap();
        stream
            .set_read_timeout(Some(Duration::from_secs(2)))
            .unwrap();
        let mut bytes = Vec::new();
        let mut chunk = [0; 1024];
        while !bytes.windows(4).any(|v| v == b"\r\n\r\n") {
            let n = stream.read(&mut chunk).unwrap();
            assert!(n > 0 && bytes.len() < 65536);
            bytes.extend_from_slice(&chunk[..n]);
        }
        write!(stream,"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",body.len(),body).unwrap();
        String::from_utf8(bytes).unwrap()
    });
    (url, handle)
}

/// Real supplied Bash/curl/jq collectors retain their distinct HTTP auth and quota semantics.
#[tokio::test]
async fn supplied_http_collectors_emit_the_shared_contract() {
    for (script_name, body, authorization, windows) in [
        (
            "glm",
            r#"{"data":{"limits":[{"type":"CREDIT_LIMIT","unit":3,"number":5,"usage":100,"currentValue":25,"percentage":25},{"type":"CREDIT_LIMIT","unit":6,"number":1,"percentage":12},{"type":"TIME_LIMIT","percentage":3}]}}"#,
            "authorization: private-fixture-token",
            2,
        ),
        (
            "claude",
            r#"{"limits":[{"kind":"session","percent":25,"is_active":false},{"kind":"weekly_all","percent":12},{"kind":"weekly_scoped","percent":75,"scope":{"model":{"display_name":"Sonnet","id":null}},"is_active":true}]}"#,
            "authorization: bearer private-fixture-token",
            3,
        ),
    ] {
        let root = tempfile::tempdir().unwrap();
        let (url, server) = endpoint(body);
        let external = Path::new(env!("CARGO_MANIFEST_DIR"))
            .join(format!("../../scripts/collectors/{script_name}.sh"));
        let command: CollectorBinding = serde_json::from_value(
            json!({"command":"/bin/bash","args":[external,url],"source":"fixture"}),
        )
        .unwrap();
        let config = configured(root.path(), &command);
        let result = collect_providers(root.path(), &config).await.unwrap();
        assert_eq!(result["ok"], true, "{script_name}: {result}");
        assert_eq!(result["results"][0]["windows"], windows);
        assert!(server
            .join()
            .unwrap()
            .to_ascii_lowercase()
            .contains(authorization));
    }
}

/// Invalid first-party provider data fails closed in the external parser before storage.
#[test]
fn supplied_parsers_reject_unknown_and_contradictory_windows() {
    use std::io::Write;
    for (script_name, body) in [
        (
            "glm",
            r#"{"data":{"limits":[{"type":"CREDIT_LIMIT","unit":3,"number":5,"usage":100,"currentValue":80,"percentage":25}]}}"#,
        ),
        (
            "glm",
            r#"{"data":{"limits":[{"type":"CREDIT_LIMIT","unit":99,"number":5,"percentage":25}]}}"#,
        ),
        ("claude", r#"{"limits":[{"kind":"session","percent":101}]}"#),
        (
            "claude",
            r#"{"limits":[{"kind":"weekly_scoped","percent":25,"scope":{"surface":"foreign","model":{"id":"sonnet"}}}]}"#,
        ),
        (
            "claude",
            r#"{"limits":[{"kind":"weekly_all","percent":25,"resets_at":"invalid"}]}"#,
        ),
    ] {
        let directory = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../scripts/collectors");
        let mut child = std::process::Command::new("jq")
            .args(["-e", "-L"])
            .arg(&directory)
            .args([
                "--argjson",
                "ctx",
                r#"{"models":{"sonnet":{}},"now":1000}"#,
                "-f",
            ])
            .arg(directory.join(format!("{script_name}.jq")))
            .stdin(std::process::Stdio::piped())
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .spawn()
            .unwrap();
        child
            .stdin
            .take()
            .unwrap()
            .write_all(body.as_bytes())
            .unwrap();
        assert!(
            !child.wait().unwrap().success(),
            "{script_name}: invalid payload was accepted"
        );
    }
}

/// One bad account does not prevent successful facts from another account in the same round.
#[tokio::test]
async fn failing_account_does_not_block_healthy_account() {
    let root = tempfile::tempdir().unwrap();
    let command = script(
        root.path(),
        r#"jq -e 'if .account.id == "acct-bad" then error("bad account") else
{version:1,windows:[{pool:"shared",window:"five_hour",models:(.models|keys),remaining_percent:50,observed_at:.now}]} end'"#,
    );
    let mut config = configured(root.path(), &command);
    let credential = root.path().join("second-credential");
    fs::write(&credential, "second-fixture-token").unwrap();
    fs::set_permissions(&credential, fs::Permissions::from_mode(0o600)).unwrap();
    let mut store = agent_run_store::Store::open(root.path()).unwrap();
    store
        .register_account(&AccountRecord {
            account_id: "acct-bad".parse().unwrap(),
            auth_family: "anthropic".parse().unwrap(),
            secret_ref: format!("file:{}", credential.display()).parse().unwrap(),
            status: agent_run_domain::catalog::AccountStatus::Enabled,
        })
        .unwrap();
    let mut provider = config.providers[&"first".parse().unwrap()].clone();
    provider.bindings[0].account = "acct-bad".parse().unwrap();
    config.providers.insert("broken".parse().unwrap(), provider);
    let report = collect_providers(root.path(), &config).await.unwrap();
    assert_eq!(report["ok"], false, "{report}");
    let rows = report["results"].as_array().unwrap();
    assert!(rows
        .iter()
        .any(|r| r["account"] == "acct-test" && r["status"] == "collected"));
    assert!(rows
        .iter()
        .any(|r| r["account"] == "acct-bad" && r["status"] == "failed"));
    let count: i64 = store
        .conn
        .query_row(
            "SELECT count(*) FROM capacity_samples WHERE account_id='acct-test'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(count, 1);
}

/// Reject credentials before Bash command substitution can strip a NUL or send a changed header.
#[test]
fn supplied_http_scripts_reject_control_bytes_before_network() {
    use std::io::Write;
    for name in ["glm", "claude"] {
        for (version, token) in [(1, "bad\0token"), (1, "bad\ntoken"), (2, "valid-token")] {
            let root = tempfile::tempdir().unwrap();
            let fake = root.path().join("curl");
            fs::write(&fake, "#!/bin/sh\n: > called\nexit 99\n").unwrap();
            fs::set_permissions(&fake, fs::Permissions::from_mode(0o755)).unwrap();
            let path = Path::new(env!("CARGO_MANIFEST_DIR"))
                .join(format!("../../scripts/collectors/{name}.sh"));
            let mut child = std::process::Command::new("/bin/bash")
                .arg(path)
                .env_clear()
                .env("PATH", format!("{}:/usr/bin:/bin", root.path().display()))
                .current_dir(root.path())
                .stdin(std::process::Stdio::piped())
                .stdout(std::process::Stdio::null())
                .stderr(std::process::Stdio::null())
                .spawn()
                .unwrap();
            child.stdin.take().unwrap().write_all(json!({"version":version,"auth":{"token":token},"models":{"sonnet":{}},"now":1000}).to_string().as_bytes()).unwrap();
            assert!(!child.wait().unwrap().success());
            assert!(
                !root.path().join("called").exists(),
                "invalid input reached HTTP transport"
            );
        }
    }
}
