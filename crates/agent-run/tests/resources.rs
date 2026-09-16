//! Python `tests/test_doc.py` parity for embedded operator-guide pages.

use std::{env, process::Command};

/// Mirrors `DocTopicsTests.test_every_topic_loads_and_is_bounded`.
#[test]
fn python_doc_topics_are_embedded_and_bounded() {
    for topic in [
        "index",
        "completion",
        "config",
        "skills",
        "mcp-servers",
        "plugins",
        "models",
        "releases",
        "migrations",
        "troubleshoot",
    ] {
        let text = agent_run::dispatch::doc(topic).expect("declared topic loads");
        assert!(!text.trim().is_empty(), "{topic}");
        assert!(text.len() <= 8192, "{topic}");
    }
}

/// Mirrors `DocTopicsTests.test_topic_text_rejects_unknown_topic`.
#[test]
fn python_doc_rejects_unknown_topic() {
    assert!(agent_run::dispatch::doc("not-a-real-topic").is_err());
}

/// Mirrors the requested installed-binary CWD independence check.
#[test]
fn doc_works_outside_the_checkout() {
    let directory = tempfile::tempdir().expect("unrelated current directory");
    let output = Command::new(env!("CARGO_BIN_EXE_agent-run"))
        .current_dir(directory.path())
        .args(["doc", "models"])
        .output()
        .expect("agent-run runs");
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let value: serde_json::Value = serde_json::from_slice(&output.stdout).expect("JSON response");
    assert_eq!(value["topic"], "models");
    assert!(value["text"]
        .as_str()
        .is_some_and(|text| text.contains("opencode/")));
}
