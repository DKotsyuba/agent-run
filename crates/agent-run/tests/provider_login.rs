//! Schema-2 bootstrap and native login: a fresh `init` writes an explicitly
//! empty v2 catalog (never a schema-1 config beside a current database), and
//! `login`/`auth` resolve a configured provider, its harness and the bound
//! account's protected storage. Fake harness executables only record what
//! they were asked; no live login or credential store is touched.

use serde_json::Value;
use std::{fs, os::unix::fs::PermissionsExt, path::Path, process::Command};

/// Runs the real CLI against `home`, returning (success, JSON or text).
fn run(home: &Path, args: &[&str]) -> (bool, Value) {
    let output = Command::new(env!("CARGO_BIN_EXE_agent-run"))
        .arg("--home")
        .arg(home)
        .args(args)
        .output()
        .unwrap();
    let text = if output.status.success() {
        output.stdout
    } else {
        [output.stdout, output.stderr].concat()
    };
    (
        output.status.success(),
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

/// The library readiness report (`cli::doctor`) for `home`; no broker runs.
fn readiness(home: &Path) -> Value {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(agent_run::cli::doctor(home))
        .unwrap()
}

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
    let (ok, doctor) = run(&home, &["doctor"]);
    assert!(ok, "{doctor}");
    let codes: Vec<&str> = doctor["findings"]
        .as_array()
        .unwrap()
        .iter()
        .filter_map(|finding| finding["code"].as_str())
        .collect();
    assert!(codes.contains(&"provider_catalog_empty"), "{doctor}");
    assert!(!codes.contains(&"config_invalid"), "{doctor}");
    let empty = readiness(&home);
    assert_eq!(empty["checks"].as_array().unwrap().len(), 1, "{empty}");
    assert_eq!(empty["checks"][0]["name"], "state");
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

    // Readiness and launchd rendering read the same schema-2 view.
    let readiness = readiness(&home);
    let names: Vec<&str> = readiness["checks"]
        .as_array()
        .unwrap()
        .iter()
        .filter_map(|check| check["name"].as_str())
        .collect();
    assert_eq!(
        names,
        [
            "state",
            "harness:codex",
            "harness:claude-code",
            "claude",
            "codex"
        ],
        "{readiness}"
    );
    assert_eq!(
        readiness["checks"][4]["accounts"],
        serde_json::json!(["acct-codex-p2", "acct-codex-native"])
    );
    assert_eq!(readiness["checks"][1]["executable"], true);
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
