//! Native-login account isolation remains independent of the collector implementation.
use agent_run_adapters::authorized_request::CredentialReader;
use agent_run_domain::CredentialRef;
use std::{path::Path, str::FromStr};
use tempfile::tempdir;
fn claude_store(dir: &Path, token: &str) {
    use std::os::unix::fs::PermissionsExt;
    std::fs::create_dir_all(dir).unwrap();
    let file = dir.join(".credentials.json");
    std::fs::write(
        &file,
        format!(r#"{{"claudeAiOauth":{{"accessToken":"{token}"}}}}"#),
    )
    .unwrap();
    std::fs::set_permissions(&file, std::fs::Permissions::from_mode(0o600)).unwrap();
}

/// Native and named Claude logins resolve their own distinct stores, and a
/// missing named store never falls back to the default login.
#[test]
fn claude_native_and_named_logins_resolve_distinct_stores() {
    use agent_run_core::capacity::collectors::QuotaCredentialReader;
    let root = tempdir().unwrap();
    let app_home = root.path().join("agent-run");
    let native = root.path().join("native-claude");
    claude_store(&native, "native-token");
    claude_store(
        &app_home.join("accounts/claude/alpha/claude-config"),
        "alpha-token",
    );
    let reader =
        QuotaCredentialReader::new(app_home, root.path().join("runtime-claude"), native, true);
    let read = |reference: &str| reader.read(&CredentialRef::from_str(reference).unwrap());
    assert_eq!(read("native:claude-code").unwrap(), "native-token");
    assert_eq!(read("named:claude-code:alpha").unwrap(), "alpha-token");
    let missing = read("named:claude-code:beta").unwrap_err().to_string();
    assert!(!missing.contains("native-token") && !missing.contains("alpha-token"));
    assert!(read("native:codex").is_err());
    assert!(read("named:codex:alpha").is_err());
}

/// Keychain service names follow Claude Code's own per-directory rule.
#[test]
fn claude_keychain_service_is_directory_scoped() {
    use agent_run_core::capacity::quota_auth::keychain_service;
    assert_eq!(
        keychain_service(Path::new("/x/.claude"), false),
        "Claude Code-credentials"
    );
    let a = keychain_service(Path::new("/a/claude-config"), true);
    let b = keychain_service(Path::new("/b/claude-config"), true);
    assert!(a.starts_with("Claude Code-credentials-") && a.len() == 32);
    assert_ne!(a, b);
}
