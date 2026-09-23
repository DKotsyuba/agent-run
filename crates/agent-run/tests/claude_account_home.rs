//! One labelled Claude login directory across `auth login`, run
//! materialization, and quota credential resolution.
//!
//! A fake Claude CLI "logs in" by writing a private fake credential into the
//! `CLAUDE_CONFIG_DIR` that `agent-run login` hands it; the same label must
//! then resolve to that exact directory for a run environment and for the
//! quota reader. No live login or protected store is touched.

use agent_run_adapters::authorized_request::CredentialReader;
use agent_run_config::{config::Config, profiles::Profile};
use agent_run_core::capacity::quota_auth::QuotaCredentialReader;
use agent_run_domain::CredentialRef;
use std::{
    collections::{BTreeMap, BTreeSet},
    fs,
    os::unix::fs::PermissionsExt,
    path::{Path, PathBuf},
    process::Command,
    str::FromStr,
};
use tempfile::tempdir;

/// Writes a fake Claude CLI whose `auth login` stores `token` privately in
/// `$CLAUDE_CONFIG_DIR/.credentials.json` and whose `auth status` succeeds.
fn fake_claude(path: &Path, token: &str) {
    let script = format!(
        "#!/bin/sh\nif [ \"$2\" = login ]; then\n  mkdir -p \"$CLAUDE_CONFIG_DIR\"\n  umask 077\n  printf '%s' '{{\"claudeAiOauth\":{{\"accessToken\":\"{token}\"}}}}' > \"$CLAUDE_CONFIG_DIR/.credentials.json\"\nfi\nexit 0\n"
    );
    fs::write(path, script).unwrap();
    fs::set_permissions(path, fs::Permissions::from_mode(0o755)).unwrap();
}

/// Writes a schema-1 home with one Claude runtime declaring `personal`.
fn claude_home(root: &Path, token: &str) -> (PathBuf, PathBuf) {
    let home = root.canonicalize().unwrap();
    let binary = home.join("fake-claude");
    fake_claude(&binary, token);
    let runtime_home = home.join("runtime-claude");
    fs::write(
        home.join("config.toml"),
        format!(
            "schema_version = 1\n[runtimes.claude]\nenabled = true\nadapter = 'claude'\nbinary = {}\nhome = {}\nmodels = ['sonnet']\naccounts = ['personal']\n",
            toml::Value::String(binary.display().to_string()),
            toml::Value::String(runtime_home.display().to_string()),
        ),
    )
    .unwrap();
    (home, runtime_home)
}

/// Runs `agent-run login claude --account personal` against `home`.
fn login(home: &Path) -> std::process::Output {
    Command::new(env!("CARGO_BIN_EXE_agent-run"))
        .args(["--home", home.to_str().unwrap()])
        .args(["login", "claude", "--account", "personal"])
        .output()
        .unwrap()
}

/// Returns the run environment's `CLAUDE_CONFIG_DIR` for label `personal`.
fn run_config_dir(home: &Path) -> agent_run_domain::Result<String> {
    let config = Config::load(home)?;
    let runtime = config.runtime("claude")?;
    let profile = Profile {
        name: "probe".into(),
        body: "Read only.".into(),
        write: false,
        network: false,
        revision: "probe-v1".into(),
        canonical: true,
        allow_external_read_roots: false,
        read_roots: vec![],
        skills: vec![],
        mcp: vec![],
        required_constraints: BTreeSet::new(),
    };
    let host: BTreeMap<String, String> = [("HOME", home), ("PATH", Path::new("/usr/bin"))]
        .into_iter()
        .map(|(key, value)| (key.to_owned(), value.display().to_string()))
        .collect();
    let env = agent_run_adapters::materialize::environment_with_host(
        &config,
        runtime,
        &profile,
        &home.join("run"),
        Some("personal"),
        home,
        &host,
    )?;
    Ok(env["CLAUDE_CONFIG_DIR"].clone())
}

/// Reads the `named:claude-code:personal` token through the quota reader.
fn quota_token(home: &Path, runtime_home: &Path) -> agent_run_domain::Result<String> {
    QuotaCredentialReader::new(
        home.to_path_buf(),
        runtime_home.to_path_buf(),
        home.join("no-native"),
        true,
    )
    .read(&CredentialRef::from_str("named:claude-code:personal").unwrap())
}

/// A fresh login, a run, and quota collection all use the canonical home.
#[test]
fn fresh_login_is_the_directory_runs_and_quota_read() {
    let temp = tempdir().unwrap();
    let (home, runtime_home) = claude_home(temp.path(), "fresh-token");
    let output = login(&home);
    assert!(output.status.success(), "{output:?}");
    let canonical = home.join("accounts/claude/personal/claude-config");
    assert!(canonical.join(".credentials.json").is_file());
    assert_eq!(
        run_config_dir(&home).unwrap(),
        canonical.display().to_string()
    );
    assert_eq!(quota_token(&home, &runtime_home).unwrap(), "fresh-token");
}

/// An existing legacy login stays usable in place, and re-login refreshes it
/// there; a second directory for the same label is ambiguous everywhere.
#[test]
fn legacy_login_is_reused_and_ambiguity_fails_everywhere() {
    let temp = tempdir().unwrap();
    let (home, runtime_home) = claude_home(temp.path(), "relogin-token");
    let legacy = home.join("runtime-claude@personal/claude-config");
    fs::create_dir_all(&legacy).unwrap();
    let file = legacy.join(".credentials.json");
    fs::write(&file, r#"{"claudeAiOauth":{"accessToken":"legacy-token"}}"#).unwrap();
    fs::set_permissions(&file, fs::Permissions::from_mode(0o600)).unwrap();
    assert_eq!(run_config_dir(&home).unwrap(), legacy.display().to_string());
    assert_eq!(quota_token(&home, &runtime_home).unwrap(), "legacy-token");
    let output = login(&home);
    assert!(output.status.success(), "{output:?}");
    assert!(!home.join("accounts/claude/personal").exists());
    assert_eq!(quota_token(&home, &runtime_home).unwrap(), "relogin-token");

    fs::create_dir_all(home.join("accounts/claude/personal/claude-config")).unwrap();
    assert!(run_config_dir(&home).is_err());
    let quota = quota_token(&home, &runtime_home).unwrap_err().to_string();
    assert!(!quota.contains("relogin-token"));
    let output = login(&home);
    assert!(!output.status.success(), "{output:?}");
}
