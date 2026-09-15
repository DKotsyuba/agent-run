//! Durable transcript coverage for adapter-provided redaction.

mod common;

use agent_run_adapters::redact::Redactor;
use std::collections::BTreeMap;

/// The stream-journal call persists only the adapter-redacted message text.
#[test]
fn journal_stores_redacted_message_text() {
    let h = common::Home::new();
    let (id, _) = h
        .store()
        .admit(&h.request(), &h.config, &serde_json::json!({}), None)
        .unwrap();
    let secret = "stored-secret";
    let redactor =
        Redactor::from_environment(&BTreeMap::from([("SERVICE_TOKEN".into(), secret.into())]));
    agent_run_core::journal(
        &h.store(),
        &id,
        "assistant",
        &redactor.redact(&format!("provider echoed {secret}")),
        None,
        None,
    )
    .unwrap();
    let transcript = h.store().transcript(&id, 0, 10).unwrap().to_string();
    assert!(!transcript.contains(secret));
    assert!(transcript.contains("<redacted>"));
}
