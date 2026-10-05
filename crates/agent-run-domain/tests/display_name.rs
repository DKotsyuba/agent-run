//! Human-label validation and normalization at both request boundaries.

use agent_run_domain::{domain::display_name, provider_start::ProviderStartRequest};
use serde_json::json;

/// Labels accept Unicode and punctuation, count scalars, trim whitespace and
/// reject blank, oversized or terminal-unsafe input without slug restrictions.
#[test]
fn human_labels_are_normalized_and_bounded() {
    for label in ["Lead 1 / review", "Мария: обзор", "工程師 🦀", "👩‍💻"] {
        assert_eq!(display_name(&format!("  {label}  ")).unwrap(), label);
    }
    assert_eq!(display_name(&"界".repeat(64)).unwrap(), "界".repeat(64));
    for invalid in ["".to_owned(), "   ".to_owned(), "界".repeat(65)] {
        assert!(display_name(&invalid).is_err(), "{invalid:?}");
    }
    for c in [
        '\0', '\n', '\r', '\u{1b}', '\u{85}', '\u{2028}', '\u{2029}', '\u{202e}',
    ] {
        assert!(display_name(&format!("a{c}b")).is_err(), "{c:?}");
    }
}

/// Provider requests retain the normalized label in their public request and
/// storage projection, so replay identity uses the same value as durable storage.
#[test]
fn provider_validation_preserves_normalized_label() {
    let workdir = std::env::current_dir().unwrap();
    let mut request: ProviderStartRequest = serde_json::from_value(json!({
        "provider":"codex", "model":"fixture", "profile":"review",
        "task":"review", "workdir":workdir, "display_name":"  工程師 🦀  "
    }))
    .unwrap();
    request.validate().unwrap();
    assert_eq!(request.display_name.as_deref(), Some("工程師 🦀"));
    assert_eq!(
        request.storage_projection().display_name,
        request.display_name
    );
}

/// Unnamed requests preserve pre-label serialized replay fingerprints by
/// omitting the new field, while deserialization still accepts explicit null.
#[test]
fn unnamed_requests_preserve_historical_replay_shape() {
    let request: ProviderStartRequest = serde_json::from_value(json!({
        "provider":"codex", "model":"fixture", "profile":"review",
        "task":"review", "workdir":std::env::current_dir().unwrap(), "display_name":null
    }))
    .unwrap();
    assert!(!serde_json::to_value(&request)
        .unwrap()
        .as_object()
        .unwrap()
        .contains_key("display_name"));
    assert!(!serde_json::to_value(request.storage_projection())
        .unwrap()
        .as_object()
        .unwrap()
        .contains_key("display_name"));
}
