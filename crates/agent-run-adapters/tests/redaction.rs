//! Regressions for Python-compatible stream and diagnostic redaction.

use agent_run_adapters::redact::{DiagnosticTail, Redactor};
use std::collections::BTreeMap;

/// Transcript text and secret-shaped JSON fields are both normalized before persistence.
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
