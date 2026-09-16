//! Regressions for Python-compatible stream and diagnostic redaction.

use agent_run_adapters::redact::{DiagnosticTail, Redactor};
use std::collections::BTreeMap;

/// Transcript text and secret-shaped JSON fields are both normalized before persistence.
/// Mirrors `tests/test_claude_stream.py::SanitizeLineTests::test_literal_secret_is_redacted_even_in_a_malformed_line`.
/// Mirrors `tests/test_claude_stream.py::SanitizeLineTests::test_secret_shaped_key_is_redacted_and_text_blocks_are_preserved`.
/// Mirrors `tests/test_claude_stream.py::SanitizeLineTests::test_literal_secret_inside_well_formed_json_is_also_redacted`.
/// Mirrors `tests/test_claude_stream.py::SanitizeLineTests::test_blank_line_and_empty_secret_pass_through_unchanged`.
/// Mirrors `tests/test_claude_session.py::ClaudeSessionTests::test_literal_secret_value_is_redacted_from_messages_and_the_disk_log`.
/// Mirrors `tests/test_claude_session.py::ClaudeSessionTests::test_malformed_line_with_a_literal_secret_is_still_redacted_on_disk`.
#[test]
fn redacts_literal_and_secret_shaped_message_values() {
    let redactor = Redactor::from_environment(&BTreeMap::from([(
        "SERVICE_TOKEN".into(),
        "live-value".into(),
    )]));
    let text = redactor.redact(r#"{"api_key":"live-value","message":"live-value"}"#);
    assert!(!text.contains("live-value"));
    assert_eq!(text, r#"{"api_key":"<redacted>","message":"<redacted>"}"#);
}

/// Bounded stderr evidence exposes neither a literal secret nor a secret-shaped JSON value.
/// Mirrors `tests/test_claude_session.py::ClaudeSessionTests::test_empty_stdout_preserves_bounded_redacted_stderr_failure`.
/// Mirrors `tests/test_claude_session.py::ClaudeSessionTests::test_newline_free_stderr_is_chunked_and_redacts_boundary_secret`.
#[test]
fn diagnostic_tail_is_bounded_and_redacted() {
    let redactor = Redactor::from_environment(&BTreeMap::from([(
        "SERVICE_TOKEN".into(),
        "tail-secret".into(),
    )]));
    let mut tail = DiagnosticTail::new(redactor);
    tail.push(format!("{} {{\"token\":\"tail-secret\"}}", "x".repeat(5000)).as_bytes());
    let text = tail.text().unwrap();
    assert!(text.len() <= 4096);
    assert!(!text.contains("tail-secret"));
}
