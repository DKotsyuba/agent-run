//! Secret-safe output normalization for adapter stream and diagnostic paths.

use regex::Regex;
use serde_json::Value;
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::LazyLock,
};

static SECRET_NAME: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new("(?i)(key|token|secret|password|credential)").expect("valid regex")
});

/// Returns whether a variable or JSON field name has Python's credential shape.
pub fn is_secret_name(name: &str) -> bool {
    SECRET_NAME.is_match(name)
}

/// Replaces known literal secrets and string JSON values under secret-shaped keys.
///
/// The redactor intentionally leaves numeric token-count and cost fields
/// intact, matching Python.  It is constructed from a launch environment and
/// must be used before stream text is persisted or retained as diagnostics.
#[derive(Clone, Default)]
pub struct Redactor {
    literals: Vec<String>,
}

impl Redactor {
    /// Builds a redactor from nonblank values whose environment names are secret-shaped.
    pub fn from_environment(environment: &BTreeMap<String, String>) -> Self {
        Self::from_environment_with_secret_names(environment, &BTreeSet::new())
    }

    /// Builds a redactor while extending the secret-name allow-list for declared auth values.
    pub fn from_environment_with_secret_names(
        environment: &BTreeMap<String, String>,
        secret_names: &BTreeSet<String>,
    ) -> Self {
        let mut literals: Vec<_> = environment
            .iter()
            .filter(|(name, value)| {
                (is_secret_name(name) || secret_names.contains(*name)) && !value.is_empty()
            })
            .map(|(_, value)| value.clone())
            .collect();
        literals.sort_by_key(|value| std::cmp::Reverse(value.len()));
        literals.dedup();
        Self { literals }
    }

    /// Sanitizes text before it enters a transcript or bounded diagnostic tail.
    pub fn redact(&self, text: &str) -> String {
        let mut safe = text.to_owned();
        for literal in &self.literals {
            safe = safe.replace(literal, "<redacted>");
        }
        let Ok(mut value) = serde_json::from_str::<Value>(safe.trim()) else {
            return safe;
        };
        redact_value(&mut value);
        serde_json::to_string(&value).unwrap_or(safe)
    }

    /// Returns the longest literal-secret byte length used to preserve tail overlap.
    fn overlap_bytes(&self) -> usize {
        self.literals.iter().map(String::len).max().unwrap_or(0)
    }
}

/// Retains a bounded diagnostic suffix and redacts it only when read.
///
/// Raw bytes stay in process memory long enough to preserve a literal secret
/// spanning the retained-tail boundary. `text` is the only export and always
/// applies literal and structural redaction before truncating to `limit`.
pub struct DiagnosticTail {
    limit: usize,
    raw: Vec<u8>,
    redactor: Redactor,
}

impl DiagnosticTail {
    /// Creates a 4KiB diagnostic tail for a launch-specific redactor.
    pub fn new(redactor: Redactor) -> Self {
        Self {
            limit: 4096,
            raw: Vec::new(),
            redactor,
        }
    }

    /// Appends stderr bytes while retaining enough overlap for literal redaction.
    pub fn push(&mut self, bytes: &[u8]) {
        self.raw.extend_from_slice(bytes);
        let keep = self.limit + self.redactor.overlap_bytes();
        if self.raw.len() > keep {
            self.raw.drain(..self.raw.len() - keep);
        }
    }

    /// Returns the final bounded, UTF-8-safe redacted diagnostic suffix.
    pub fn text(&self) -> Option<String> {
        let safe = self.redactor.redact(&String::from_utf8_lossy(&self.raw));
        let mut bytes = safe.into_bytes();
        if bytes.len() > self.limit {
            bytes.drain(..bytes.len() - self.limit);
        }
        let text = String::from_utf8_lossy(&bytes).trim().to_owned();
        (!text.is_empty()).then_some(text)
    }
}

/// Recursively removes string values whose object keys look credential-shaped.
fn redact_value(value: &mut Value) {
    match value {
        Value::Object(object) => {
            for (name, value) in object {
                if value.is_string() && is_secret_name(name) {
                    *value = Value::String("<redacted>".into());
                } else {
                    redact_value(value);
                }
            }
        }
        Value::Array(values) => values.iter_mut().for_each(redact_value),
        _ => {}
    }
}
