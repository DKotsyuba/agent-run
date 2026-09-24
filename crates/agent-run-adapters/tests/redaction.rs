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

/// A launch secret split at any stream boundary never reaches durable output.
#[test]
fn streaming_redaction_holds_partial_secrets_until_they_are_resolved() {
    let redactor = Redactor::from_environment(&BTreeMap::from([(
        "SERVICE_TOKEN".into(),
        "synthetic-secret".into(),
    )]));
    let secret = "synthetic-secret";
    for split in 1..secret.len() {
        let mut stream = redactor.stream();
        let first = stream.feed(&format!("before {}", &secret[..split]));
        let second = stream.feed(&format!("{} after", &secret[split..]));
        let visible = format!("{first}{second}{}", stream.finish());
        assert_eq!(visible, "before <redacted> after", "split={split}");
        assert!(!first.contains(&secret[..split]), "split={split}");
    }
}

/// Parsed native events hide secrets even when JSON escaping changed their bytes.
#[test]
fn parsed_event_redaction_covers_escaped_values_and_keys() {
    let redactor =
        Redactor::from_environment(&BTreeMap::from([("SERVICE_TOKEN".into(), "a\"b".into())]));
    let event = serde_json::json!({"a\"b":"a\"b","message":"a\"b"});
    let safe = redactor.redact_value(&event).to_string();
    assert!(!safe.contains("a\\\"b"));
    assert!(safe.contains("<redacted>"));
}

/// Buffer boundaries cannot parse and reserialize ordinary message fragments.
#[test]
fn streaming_redaction_preserves_nonsecret_whitespace() {
    let redactor = Redactor::from_environment(&BTreeMap::from([(
        "API_TOKEN".into(),
        "abcdefghijklmnop".into(),
    )]));
    let input = " 1 xxxxxxxxxxxxxxx";
    let mut stream = redactor.stream();
    assert_eq!(format!("{}{}", stream.feed(input), stream.finish()), input);
}

/// Native logins without environment secrets still keep text bytes intact.
#[test]
fn streaming_redaction_without_literals_preserves_text() {
    let mut stream = Redactor::default().stream();
    assert_eq!(stream.feed(" 1 "), " 1 ");
    assert_eq!(stream.finish(), "");
}
