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

/// Secret detection for exact worker summaries ignores harmless JSON
/// formatting, key order and escaping choices, yet still finds literals,
/// literals hidden by JSON escaping and credential-shaped string fields.
#[test]
fn contains_secret_separates_credentials_from_json_normalization() {
    let redactor = Redactor::from_environment(&BTreeMap::from([(
        "SERVICE_TOKEN".into(),
        "synthetic-secret".into(),
    )]));
    for clean in [
        "{ \"ok\" : true }",
        "{\n  \"b\": 1,\n  \"a\": \"caf\\u00e9\"\n}",
        "[1, 2.50, \"x\\/y\"]",
        "plain text summary",
        "{\"tokens_used\": 12}",
    ] {
        assert!(!redactor.contains_secret(clean), "{clean}");
    }
    // Transcript redaction still compacts JSON, which is why byte comparison
    // with its output cannot detect secrets in an exact summary.
    assert_eq!(redactor.redact("{ \"ok\" : true }"), "{\"ok\":true}");
    for leaked in [
        "leaked synthetic-secret",
        "{ \"note\" : \"synthetic-secret\" }",
        "{\"note\":\"synthetic-secre\\u0074\"}",
        "{\"synthetic-secret\":1}",
        "{\"api_key\":\"other\"}",
    ] {
        assert!(redactor.contains_secret(leaked), "{leaked}");
    }
}

/// A later duplicate key cannot hide an earlier decoded credential: every
/// decoded string and key of a JSON summary is checked, including values a
/// parsed map would overwrite, while duplicates holding only safe values stay
/// accepted and are never rewritten.
#[test]
fn contains_secret_sees_values_overwritten_by_duplicate_keys() {
    let redactor = Redactor::from_environment(&BTreeMap::from([(
        "SERVICE_TOKEN".into(),
        "synthetic-secret".into(),
    )]));
    for leaked in [
        r#"{"note":"synthetic-secre\u0074","note":"safe"}"#,
        r#"{"outer":{"note":"synthetic-secre\u0074","note":1},"outer":2}"#,
        r#"[{"k":["synthetic-secre\u0074"],"k":[]}]"#,
        r#"{"synthetic-secre\u0074":1,"synthetic-secre\u0074":2}"#,
        r#"{"api_key":"other","api_key":5}"#,
    ] {
        assert!(redactor.contains_secret(leaked), "{leaked}");
    }
    for clean in [
        r#"{"note":"safe","note":"also safe"}"#,
        r#"{"api_key":"<redacted>","api_key":7}"#,
        r#"{ "tokens_used" : 3, "tokens_used" : 4 }"#,
    ] {
        assert!(!redactor.contains_secret(clean), "{clean}");
    }
    // JSON deeper than the parser limit cannot be decoded, so it fails closed
    // (a deliberate bound) instead of hiding an escaped secret.
    let deep = |inner: &str| format!("{}{inner}{}", "[".repeat(200), "]".repeat(200));
    assert!(redactor.contains_secret(&deep(r#""synthetic-secre\u0074""#)));
    assert!(redactor.contains_secret(&deep("1")));
    // Text that is not JSON keeps only the raw literal check.
    assert!(!redactor.contains_secret(r#"{"note":"synthetic-secre\u0074""#));
}

/// A journaled copy of private finish arguments cannot keep a launch secret
/// that only its nested JSON summary reveals (the exact OpenCode tool_call
/// row: an escaped final character behind a later duplicate key). The copy's
/// summary becomes the redaction marker, other fields and safe nested JSON
/// keep their bytes, and existing literal and field policies are unchanged.
#[test]
fn journal_copy_hides_secret_in_nested_json_summary() {
    use serde_json::{Value, json};
    let redactor = Redactor::from_environment(&BTreeMap::from([(
        "CANARY_SIGNING_KEY".into(),
        "synthetic-canary-k3y-77d1".into(),
    )]));
    let journaled = r##"{"status":"done","summary":"{\"ok\":true,\"note\":\"synthetic-canary-k3y-77d\\u0031\",\"note\":\"safe\"}"}"##;
    assert!(!journaled.contains("synthetic-canary-k3y-77d1"));
    let safe: Value = serde_json::from_str(&redactor.redact(journaled)).unwrap();
    assert_eq!(safe, json!({"status": "done", "summary": "<redacted>"}));
    let input: Value = serde_json::from_str(journaled).unwrap();
    let event = redactor.redact_value(&json!({"type": "tool_use", "input": input}));
    assert_eq!(event["input"]["summary"], "<redacted>");
    assert_eq!(event["type"], "tool_use");
    for summary in [
        r#"{ "ok" : true, "note":"safe","note":"also safe" }"#,
        r#"{"token":"not a launch value"}"#,
        "plain words",
    ] {
        let arguments = json!({"status": "done", "summary": summary}).to_string();
        let safe: Value = serde_json::from_str(&redactor.redact(&arguments)).unwrap();
        assert_eq!(safe["summary"], summary);
    }
    assert_eq!(
        redactor.redact(r#"{"note":"x synthetic-canary-k3y-77d1 y"}"#),
        r#"{"note":"x <redacted> y"}"#
    );
}
