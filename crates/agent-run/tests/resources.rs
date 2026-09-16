//! Python `tests/test_doc.py` parity for embedded operator-guide pages.

use std::{env, process::Command};

/// Mirrors Python `test_doc.py::test_every_topic_loads_and_is_bounded`.
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

/// Mirrors Python `test_doc.py::test_topic_text_rejects_unknown_topic`.
#[test]
fn python_doc_rejects_unknown_topic() {
    assert!(agent_run::dispatch::doc("not-a-real-topic").is_err());
}

/// Mirrors Python `test_doc.py::test_doc_with_topic_returns_that_topic`.
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

/// Mirrors Python `test_doc.py::test_topic_text_completion_is_contract_template`.
#[test]
fn completion_topic_contains_the_shared_notice_template() {
    let text = agent_run::dispatch::doc("completion").expect("completion topic");
    assert!(text.contains("agent-run/completion"));
    assert!(text.contains("- Notice:"));
}

/// Mirrors Python `test_doc.py::test_doc_with_no_topic_returns_index`.
#[test]
fn doc_cli_without_topic_returns_the_index() {
    let output = Command::new(env!("CARGO_BIN_EXE_agent-run"))
        .arg("doc")
        .output()
        .expect("agent-run runs");
    assert!(output.status.success(), "{output:?}");
    let value: serde_json::Value = serde_json::from_slice(&output.stdout).expect("JSON output");
    assert_eq!(value["topic"], "index");
    assert!(value["text"]
        .as_str()
        .is_some_and(|text| text.contains("agent-run")));
}

/// Mirrors Python `test_doc.py::test_doc_with_completion_topic_returns_contract_text`.
#[test]
fn doc_cli_returns_the_completion_contract() {
    let output = Command::new(env!("CARGO_BIN_EXE_agent-run"))
        .args(["doc", "completion"])
        .output()
        .expect("agent-run runs");
    assert!(output.status.success(), "{output:?}");
    let value: serde_json::Value = serde_json::from_slice(&output.stdout).expect("JSON output");
    assert_eq!(value["topic"], "completion");
    assert!(value["text"]
        .as_str()
        .is_some_and(|text| text.contains("agent-run/completion")));
}

/// Mirrors Python `test_doc.py::test_doc_with_unknown_topic_is_refused`.
#[test]
fn doc_cli_rejects_an_unknown_topic() {
    let output = Command::new(env!("CARGO_BIN_EXE_agent-run"))
        .args(["doc", "not-a-real-topic"])
        .output()
        .expect("agent-run runs");
    assert_eq!(output.status.code(), Some(2));
    let value: serde_json::Value = serde_json::from_slice(&output.stderr).expect("JSON error");
    assert_eq!(value["error"]["type"], "ValidationError");
}
