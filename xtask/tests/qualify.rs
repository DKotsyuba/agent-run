//! Fail-closed qualification checks for the recorded migration evidence.

use std::fs;
use tempfile::tempdir;
use xtask::qualify;

/// Creates the smallest complete scope/evidence fixture used by both paths.
fn fixture() -> tempfile::TempDir {
    let temporary = tempdir().expect("temporary root");
    let adr = temporary.path().join("migration/adr");
    let evidence = temporary.path().join("migration/evidence");
    fs::create_dir_all(&adr).expect("ADR directory");
    fs::create_dir_all(&evidence).expect("evidence directory");
    let mut scope = String::from(
        "| id | scenario | layer | status | evidence | live |\n|---|---|---|---|---|---|\n",
    );
    for number in 1..=84 {
        let live = matches!(number, 58 | 59 | 71 | 82);
        let status = if number == 57 { "divergent" } else { "covered" };
        scope.push_str(&format!(
            "| T{number:02} | scenario | pure/unit | {status} | fixture | {} |\n",
            if live { "yes" } else { "no" }
        ));
    }
    fs::write(evidence.join("qualification-scope.md"), scope).expect("scope fixture");
    fs::write(
        adr.join("A15-platforms.md"),
        "Every suite run was executed on macOS (Darwin, arm64).",
    )
    .expect("ADR fixture");
    fs::write(
        evidence.join("verification.md"),
        "Full suite: 84 passed, 0 failed.\nRelease smoke: cargo build --release — exit 0.",
    )
    .expect("run fixture");
    temporary
}

/// Protects the refusal contract: an unrecorded platform cannot pass as a warning.
#[test]
fn unevidenced_platform_is_a_hard_failure() {
    let temporary = fixture();
    let error = qualify::qualify(temporary.path(), "linux/x86_64", true)
        .expect_err("unevidenced platform must refuse qualification");
    assert_eq!(
        error,
        "refusing qualification for linux/x86_64: no recorded full-suite evidence proves this host platform"
    );
}

/// Protects the success contract and ensures pending live work is reported rather than fabricated.
#[test]
fn evidenced_release_platform_reports_pending_live_work() {
    let temporary = fixture();
    let report = qualify::qualify(temporary.path(), "macos/arm64", true).expect("qualification");
    assert!(report.contains("scenarios: 84/84 covered"));
    assert!(report.contains("live portions retained: 3"));
    assert!(report.contains("pending live portions are not claimed as run"));
}
