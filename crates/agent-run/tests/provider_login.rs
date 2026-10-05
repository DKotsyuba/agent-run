//! Schema-2 bootstrap and native login: a fresh `init` writes an explicitly
//! empty v2 catalog (never a schema-1 config beside a current database), and
//! `login`/`auth` resolve a configured provider, its harness and the bound
//! account's protected storage. Fake harness executables only record what
//! they were asked; no live login or credential store is touched.

use serde_json::Value;
use std::{
    fs,
    io::Read,
    os::unix::fs::PermissionsExt,
    path::Path,
    process::{Child, Command, Stdio},
    sync::mpsc,
    time::{Duration, Instant},
};

/// Owns one real CLI fixture child, killed and reaped on panic or deadline.
struct CliChild(Child);

impl Drop for CliChild {
    /// Reaps this exact unreaped child; never signals an unrelated process group.
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

/// Drains an owned output pipe concurrently, retaining at most one MiB plus
/// an overflow sentinel. The caller bounds EOF retrieval after child termination.
fn capture(reader: impl Read + Send + 'static) -> mpsc::Receiver<Vec<u8>> {
    let (sender, receiver) = mpsc::sync_channel(1);
    std::thread::spawn(move || {
        let mut bytes = Vec::new();
        reader
            .take(1024 * 1024 + 1)
            .read_to_end(&mut bytes)
            .unwrap();
        let _ = sender.send(bytes);
    });
    receiver
}

/// Runs the real CLI only against a temporary home, returning (success, JSON
/// or text). A 20-second deadline kills/reaps hangs; each output pipe is bounded.
/// Optional Desktop capabilities are removed from this child only.
fn run(home: &Path, args: &[&str]) -> (bool, Value) {
    let mut child = CliChild(
        Command::new(env!("CARGO_BIN_EXE_agent-run"))
            .arg("--home")
            .arg(home)
            .args(args)
            .env_remove("CODEX_MCP_NODE_PATH")
            .env_remove("CODEX_APP_TOOLS_PIPE_PATH")
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap(),
    );
    let stdout = capture(child.0.stdout.take().unwrap());
    let stderr = capture(child.0.stderr.take().unwrap());
    let deadline = Instant::now() + Duration::from_secs(20);
    let status = loop {
        if let Some(status) = child.0.try_wait().unwrap() {
            break status;
        }
        assert!(
            Instant::now() < deadline,
            "provider-login CLI fixture exceeded deadline"
        );
        std::thread::sleep(Duration::from_millis(10));
    };
    let stdout = stdout.recv_timeout(Duration::from_secs(1)).unwrap();
    let stderr = stderr.recv_timeout(Duration::from_secs(1)).unwrap();
    assert!(stdout.len() <= 1024 * 1024 && stderr.len() <= 1024 * 1024);
    let text = if status.success() {
        stdout
    } else {
        [stdout, stderr].concat()
    };
    (
        status.success(),
        serde_json::from_slice(&text)
            .unwrap_or(Value::String(String::from_utf8_lossy(&text).into())),
    )
}

/// A fake harness that appends its arguments and credential directories.
fn fake(path: &Path, log: &Path) {
    fs::write(
        path,
        format!(
            "#!/bin/sh\necho \"$* codex_home=${{CODEX_HOME:-}} claude_dir=${{CLAUDE_CONFIG_DIR:-}}\" >> '{}'\nexit 0\n",
            log.display()
        ),
    )
    .unwrap();
    fs::set_permissions(path, fs::Permissions::from_mode(0o755)).unwrap();
}

/// Requires one named canonical local check to have the exact evidence status.
fn assert_check(report: &Value, name: &str, status: &str) {
    let check = report["checks"]
        .as_array()
        .unwrap()
        .iter()
        .find(|check| check["name"] == name)
        .unwrap_or_else(|| panic!("missing {name}: {report}"));
    assert_eq!(check["status"], status, "{name}: {report}");
}

/// A fresh empty v2 catalog passes its real local canary and read-only state
/// checks, lists no accounts and refuses agent work or schema-1 bootstrapping.
#[test]
fn fresh_init_is_an_empty_v2_catalog() {
    let temp = tempfile::tempdir_in("/tmp").unwrap();
    let home = temp.path().canonicalize().unwrap().join("home");
    let (ok, init) = run(&home, &["init"]);
    assert!(ok, "{init}");
    assert_eq!(
        fs::read_to_string(home.join("config.toml")).unwrap(),
        "schema_version = 2\n"
    );
    let (ok, models) = run(&home, &["models"]);
    assert!(ok, "{models}");
    // Doctor sees a coherent home: an empty catalog is information, not an
    // invalid config.
    // The detached canary belongs to the real CLI entrypoint. The integration
    // test executable does not implement its hidden descriptor protocol.
    let (ok, doctor) = run(&home, &["doctor", "--json"]);
    assert!(ok, "{doctor}");
    let codes: Vec<&str> = doctor["findings"]
        .as_array()
        .unwrap()
        .iter()
        .filter_map(|finding| finding["code"].as_str())
        .collect();
    assert!(codes.contains(&"provider_catalog_empty"), "{doctor}");
    assert!(!codes.contains(&"config_invalid"), "{doctor}");
    // Native process inventory may add observations; required checks retain
    // exact named outcomes, and no error-severity finding is accepted.
    for name in [
        "state",
        "provider_catalog_empty",
        "supervisor_canary_ok",
        "mcp_inventory_self",
    ] {
        assert_check(&doctor, name, "ok");
    }
    assert!(
        doctor["findings"]
            .as_array()
            .unwrap()
            .iter()
            .all(|finding| finding["severity"] != "error"),
        "{doctor}"
    );
    let (ok, accounts) = run(&home, &["accounts", "list"]);
    assert!(
        ok && accounts["accounts"] == serde_json::json!([]),
        "{accounts}"
    );
    let (ok, start) = run(
        &home,
        &[
            "start",
            "--provider",
            "codex",
            "--model",
            "gpt-6-luna",
            "--profile",
            "review",
            "--task",
            "x",
            "--workdir",
            "/tmp",
        ],
    );
    assert!(!ok, "an empty catalog starts nothing: {start}");

    // A schema-1 config never seeds a new current-schema database.
    let legacy = temp.path().join("legacy");
    fs::create_dir(&legacy).unwrap();
    fs::write(legacy.join("config.toml"), "schema_version = 1\n").unwrap();
    let (ok, refused) = run(&legacy, &["init"]);
    assert!(
        !ok && refused.to_string().contains("schema_version 2"),
        "{refused}"
    );
    assert!(!legacy.join("state.db").exists());
}

/// Native/named logins resolve the configured harness and account storage;
/// doctor proves its own canary while preserving the missing-role failure.
#[test]
fn login_resolves_provider_harness_and_bound_account() {
    let temp = tempfile::tempdir_in("/tmp").unwrap();
    let home = temp.path().canonicalize().unwrap();
    let log = home.join("calls.log");
    fake(&home.join("fake-codex"), &log);
    fake(&home.join("fake-claude"), &log);
    let (ok, init) = run(&home, &["init"]);
    assert!(ok, "{init}");
    for (id, family, reference) in [
        ("acct-codex-p2", "openai", "named:codex:personal2"),
        ("acct-codex-native", "openai", "native:codex"),
        ("acct-claude-work", "anthropic", "named:claude-code:work"),
    ] {
        let (ok, registered) = run(
            &home,
            &[
                "accounts",
                "register",
                "--id",
                id,
                "--auth-family",
                family,
                "--reference",
                reference,
            ],
        );
        assert!(ok, "{registered}");
    }
    let quoted = |name: &str| toml::Value::String(home.join(name).display().to_string());
    fs::write(
        home.join("config.toml"),
        format!(
            "schema_version=2\n[harnesses.codex]\nbinary={}\nhome={}\n[harnesses.claude-code]\nbinary={}\nhome={}\n\
             [providers.codex]\nharness='codex'\nconnection={{kind='native'}}\nauth_family='openai'\nlimits_source='none'\n\
             [[providers.codex.models]]\nid='gpt-6-luna'\n\
             [[providers.codex.bindings]]\nlabel='personal2'\naccount='acct-codex-p2'\n\
             [[providers.codex.bindings]]\nlabel='main'\naccount='acct-codex-native'\n\
             [providers.claude]\nharness='claude-code'\nconnection={{kind='native'}}\nauth_family='anthropic'\nlimits_source='none'\n\
             [[providers.claude.models]]\nid='sonnet'\n\
             [[providers.claude.bindings]]\nlabel='work'\naccount='acct-claude-work'\n",
            quoted("fake-codex"),
            quoted("codex"),
            quoted("fake-claude"),
            quoted("claude"),
        ),
    )
    .unwrap();

    let (ok, named) = run(&home, &["auth", "personal2", "codex"]);
    assert!(ok, "{named}");
    assert_eq!(named["account"], "acct-codex-p2");
    assert_eq!(named["provider"], "codex");
    let (ok, native) = run(&home, &["auth", "acct-codex-native", "codex"]);
    assert!(ok, "{native}");
    assert_eq!(native["account"], "acct-codex-native");
    assert_eq!(native["provider"], "codex");
    let (ok, claude) = run(&home, &["login", "claude"]);
    assert!(ok && claude["account"] == "acct-claude-work", "{claude}");
    let (ok, codex_login) = run(&home, &["login", "codex"]);
    assert!(
        !ok && codex_login.to_string().contains("Claude only"),
        "{codex_login}"
    );
    let (ok, unbound) = run(&home, &["auth", "someone", "codex"]);
    assert!(
        !ok && unbound.to_string().contains("not bound"),
        "{unbound}"
    );

    // This login fixture deliberately has no canonical role assets. Doctor
    // must retain that real readiness failure and still prove its own canary.
    let (ready, readiness) = run(&home, &["doctor", "--json"]);
    assert!(
        !ready,
        "missing canonical roles must remain a failure: {readiness}"
    );
    assert_check(&readiness, "state", "ok");
    assert_check(&readiness, "supervisor_canary_ok", "ok");
    assert_check(&readiness, "canonical_role_required", "failed");
    let errors: Vec<&str> = readiness["findings"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|finding| finding["severity"] == "error")
        .map(|finding| finding["code"].as_str().unwrap())
        .collect();
    assert_eq!(errors, ["canonical_role_required"], "{readiness}");
    for (name, file) in [
        ("harness:codex", "fake-codex"),
        ("harness:claude-code", "fake-claude"),
    ] {
        let tool = readiness["tools"]
            .as_array()
            .unwrap()
            .iter()
            .find(|tool| tool["name"] == name)
            .unwrap();
        assert_eq!(
            tool["executable"].as_str().unwrap(),
            home.join(file).to_str().unwrap()
        );
        assert_eq!(
            tool["status"], "not_checked",
            "fake tools have no version interface"
        );
    }
    // Bindings are owned by the typed configuration, not diagnostic check rows.
    let (configured, _) = agent_run_config::provider_config::ProviderConfig::load(&home).unwrap();
    let codex = configured
        .providers
        .iter()
        .find(|(id, _)| id.as_str() == "codex")
        .unwrap()
        .1;
    assert_eq!(codex.harness.as_str(), "codex");
    let accounts: Vec<&str> = codex
        .bindings
        .iter()
        .map(|binding| binding.account.as_str())
        .collect();
    assert_eq!(accounts, ["acct-codex-p2", "acct-codex-native"]);
    let claude_provider = configured
        .providers
        .iter()
        .find(|(id, _)| id.as_str() == "claude")
        .unwrap()
        .1;
    assert_eq!(claude_provider.harness.as_str(), "claude-code");
    assert_eq!(
        claude_provider.bindings[0].account.as_str(),
        "acct-claude-work"
    );
    let (ok, plist) = run(&home, &["capacity", "launchd", "--binary", "/bin/true"]);
    assert!(ok, "{plist}");

    let calls = fs::read_to_string(&log).unwrap();
    let lines: Vec<&str> = calls.lines().collect();
    let p2 = home.join("accounts/codex/personal2");
    let work = home.join("accounts/claude/work/claude-config");
    assert_eq!(
        lines,
        [
            format!("login codex_home={} claude_dir=", p2.display()),
            format!("login status codex_home={} claude_dir=", p2.display()),
            "login codex_home= claude_dir=".to_owned(),
            "login status codex_home= claude_dir=".to_owned(),
            format!("auth login codex_home= claude_dir={}", work.display()),
            format!(
                "auth status --json codex_home= claude_dir={}",
                work.display()
            ),
        ],
        "{calls}"
    );
}
