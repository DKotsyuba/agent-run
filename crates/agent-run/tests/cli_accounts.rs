//! Account administration against disposable homes and fake references only.

use agent_run_store::Store;
use serde_json::Value;
use std::{path::Path, process::Command};

/// Runs one account command without invoking a provider or credential store.
fn invoke(home: &Path, args: &[&str]) -> std::process::Output {
    Command::new(env!("CARGO_BIN_EXE_agent-run"))
        .arg("--home")
        .arg(home)
        .arg("accounts")
        .args(args)
        .output()
        .unwrap()
}

/// Register/list/disable publishes only account metadata, retains disabled
/// rows, and refuses a raw-token shaped command-line value.
#[test]
fn scoped_account_cli_never_emits_credential_references() {
    let home = tempfile::tempdir().unwrap();
    Store::initialize(home.path()).unwrap();
    let registered = invoke(
        home.path(),
        &[
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
    let registered: Value = serde_json::from_slice(&registered.stdout).unwrap();
    assert_eq!(registered["source"], "environment");
    assert!(registered.get("secret_ref").is_none());

    let listed = invoke(home.path(), &["list"]);
    assert!(listed.status.success());
    let listed: Value = serde_json::from_slice(&listed.stdout).unwrap();
    assert_eq!(listed["accounts"][0]["account_id"], "acct-work");
    assert!(!listed.to_string().contains("FAKE_TOKEN"));

    let disabled = invoke(home.path(), &["disable", "acct-work"]);
    assert!(disabled.status.success());
    let listed: Value = serde_json::from_slice(&invoke(home.path(), &["list"]).stdout).unwrap();
    assert_eq!(listed["accounts"][0]["status"], "disabled");

    let invalid = invoke(
        home.path(),
        &[
            "register",
            "--id",
            "acct-bad",
            "--auth-family",
            "openai",
            "--reference",
            "fake-raw-token",
        ],
    );
    assert!(!invalid.status.success());
    assert!(!String::from_utf8_lossy(&invalid.stderr).contains("fake-raw-token"));
}
