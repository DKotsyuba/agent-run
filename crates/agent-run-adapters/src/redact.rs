//! Secret-safe output normalization for adapter stream and diagnostic paths.

use regex::Regex;
use serde::de::{DeserializeSeed, Deserializer, MapAccess, SeqAccess, Visitor};
use serde_json::Value;
use std::{
    collections::{BTreeMap, BTreeSet},
    fmt,
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
        let Ok(value) = serde_json::from_str::<Value>(text.trim()) else {
            return redact_literals(text, &self.literals);
        };
        serde_json::to_string(&self.redact_value(&value))
            .unwrap_or_else(|_| redact_literals(text, &self.literals))
    }

    /// Returns whether `text` carries material this redactor would remove: a
    /// launch-secret literal in the raw bytes, or, when the trimmed text is a
    /// JSON document, a literal in any decoded key or string, or a string
    /// value under a credential-shaped key. The JSON check streams every map
    /// entry, so a value that a later duplicate key would overwrite in a
    /// parsed [`Value`] is still seen. JSON formatting, key order and escaping
    /// choices alone never count, unlike comparing [`Self::redact`] output,
    /// which compacts JSON, and safe duplicate keys are not refused. Used to
    /// refuse, never to rewrite, exact worker summaries.
    ///
    /// Bounds: text that is not JSON gets only the raw literal check. A
    /// decoded hit in a JSON value counts even if trailing text makes the
    /// whole document invalid. JSON nested deeper than the parser's recursion
    /// limit cannot be decoded and is reported as secret-bearing (fail
    /// closed), never as clean.
    pub fn contains_secret(&self, text: &str) -> bool {
        if redact_literals(text, &self.literals) != text {
            return true;
        }
        let mut document = serde_json::Deserializer::from_str(text.trim());
        let scan = SecretScan {
            literals: &self.literals,
            credential_field: false,
        };
        match scan.deserialize(&mut document) {
            Ok(found) => found,
            // ponytail: serde_json exposes no typed recursion-limit category, so
            // its fixed message selects this case; other syntax errors mean the
            // text is not JSON and only the raw check above applies.
            Err(error) => error.to_string().starts_with("recursion limit exceeded"),
        }
    }

    /// Sanitizes a parsed native event without losing literal matching to
    /// JSON string escaping or leaking a secret used as an object key.
    pub fn redact_value(&self, value: &Value) -> Value {
        let mut safe = value.clone();
        redact_value(&mut safe, &self.literals);
        safe
    }

    /// Starts one message-local redactor that withholds possible secret prefixes
    /// until a later fragment proves whether they complete a launch secret.
    pub fn stream(&self) -> StreamingRedactor {
        StreamingRedactor {
            redactor: self.clone(),
            pending: String::new(),
        }
    }

    /// Returns the longest literal-secret byte length used to preserve tail overlap.
    fn overlap_bytes(&self) -> usize {
        self.literals.iter().map(String::len).max().unwrap_or(0)
    }
}

/// Redacts launch secrets across adjacent text fragments before persistence.
///
/// The caller owns one instance per native message and must call [`Self::finish`]
/// at its terminal boundary. At most one maximum-secret-length suffix is held
/// in memory; earlier text is returned promptly and can be journaled.
pub struct StreamingRedactor {
    /// Launch-specific literal and JSON-field redaction policy.
    redactor: Redactor,
    /// Raw suffix that might still become a complete secret.
    pending: String,
}

impl StreamingRedactor {
    /// Adds the next UTF-8 fragment and returns only text safe to persist now.
    /// A possible secret prefix remains buffered until more input or `finish`;
    /// fragment text is never parsed as a complete JSON document.
    pub fn feed(&mut self, fragment: &str) -> String {
        if self.redactor.literals.is_empty() {
            return fragment.to_owned();
        }
        let mut raw = std::mem::take(&mut self.pending);
        raw.push_str(fragment);
        let keep = self.redactor.overlap_bytes().saturating_sub(1);
        let mut ready = raw.len().saturating_sub(keep);
        while !raw.is_char_boundary(ready) {
            ready -= 1;
        }
        let mut cursor = 0;
        let mut safe = String::new();
        while cursor < ready {
            let next = self
                .redactor
                .literals
                .iter()
                .filter_map(|literal| {
                    raw[cursor..]
                        .find(literal)
                        .map(|offset| (cursor + offset, literal))
                })
                .filter(|(start, _)| *start < ready)
                .min_by(|(left, a), (right, b)| {
                    left.cmp(right).then_with(|| b.len().cmp(&a.len()))
                });
            if let Some((start, literal)) = next {
                safe.push_str(&raw[cursor..start]);
                safe.push_str("<redacted>");
                cursor = start + literal.len();
            } else {
                safe.push_str(&raw[cursor..ready]);
                cursor = ready;
            }
        }
        self.pending = raw[cursor..].to_owned();
        redact_literals(&safe, &self.redactor.literals)
    }

    /// Resolves the last buffered suffix without normalizing unrelated text.
    pub fn finish(&mut self) -> String {
        redact_literals(&std::mem::take(&mut self.pending), &self.redactor.literals)
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

/// Replaces complete literal secrets in an ordinary UTF-8 text fragment.
fn redact_literals(text: &str, literals: &[String]) -> String {
    literals.iter().fold(text.to_owned(), |safe, literal| {
        safe.replace(literal, "<redacted>")
    })
}

/// Recursively removes launch secrets and string values under credential-shaped keys.
fn redact_value(value: &mut Value, literals: &[String]) {
    match value {
        Value::Object(object) => {
            let original = std::mem::take(object);
            for (name, mut nested) in original {
                if nested.is_string() && is_secret_name(&name) {
                    nested = Value::String("<redacted>".into());
                } else {
                    redact_value(&mut nested, literals);
                }
                object.insert(redact_literals(&name, literals), nested);
            }
        }
        Value::Array(values) => values
            .iter_mut()
            .for_each(|value| redact_value(value, literals)),
        Value::String(text) => *text = redact_literals(text, literals),
        _ => {}
    }
}

/// Streams one JSON value and reports whether it holds material
/// [`redact_value`] would remove: a launch-secret literal in any decoded key
/// or string, or a string other than the `<redacted>` marker directly under a
/// credential-shaped key. Unlike a parsed [`Value`], it visits every map
/// entry in order, including values a later duplicate key overwrites, and it
/// keeps nothing but the verdict. Numbers, booleans and null never count.
#[derive(Clone, Copy)]
struct SecretScan<'a> {
    /// Launch-secret literals to look for; nonempty values only.
    literals: &'a [String],
    /// Whether the value being visited sits directly under a credential-shaped key.
    credential_field: bool,
}

impl SecretScan<'_> {
    /// The scan of a child value, directly under a credential-shaped key or not.
    fn child(self, credential_field: bool) -> Self {
        Self {
            credential_field,
            ..self
        }
    }

    /// Whether decoded `text` contains any launch-secret literal.
    fn holds_literal(self, text: &str) -> bool {
        self.literals
            .iter()
            .any(|literal| text.contains(literal.as_str()))
    }
}

impl<'de> DeserializeSeed<'de> for SecretScan<'_> {
    type Value = bool;

    /// Scans one complete JSON value of any type.
    fn deserialize<D: Deserializer<'de>>(self, deserializer: D) -> Result<bool, D::Error> {
        deserializer.deserialize_any(self)
    }
}

impl<'de> Visitor<'de> for SecretScan<'_> {
    type Value = bool;

    /// Names the accepted input for parser error messages.
    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("a JSON value")
    }

    /// A boolean holds no secret.
    fn visit_bool<E>(self, _: bool) -> Result<bool, E> {
        Ok(false)
    }

    /// An integer holds no secret, even under a credential-shaped key.
    fn visit_i64<E>(self, _: i64) -> Result<bool, E> {
        Ok(false)
    }

    /// An unsigned integer holds no secret, even under a credential-shaped key.
    fn visit_u64<E>(self, _: u64) -> Result<bool, E> {
        Ok(false)
    }

    /// A float holds no secret, even under a credential-shaped key.
    fn visit_f64<E>(self, _: f64) -> Result<bool, E> {
        Ok(false)
    }

    /// `null` holds no secret.
    fn visit_unit<E>(self) -> Result<bool, E> {
        Ok(false)
    }

    /// A decoded string counts when it contains a literal, or when it sits
    /// under a credential-shaped key and is not already the redaction marker.
    fn visit_str<E>(self, text: &str) -> Result<bool, E> {
        Ok((self.credential_field && text != "<redacted>") || self.holds_literal(text))
    }

    /// Scans every element; credential-field status does not pass into arrays.
    fn visit_seq<A: SeqAccess<'de>>(self, mut items: A) -> Result<bool, A::Error> {
        let mut found = false;
        while let Some(item) = items.next_element_seed(self.child(false))? {
            found |= item;
        }
        Ok(found)
    }

    /// Scans every entry, duplicates included: each decoded key for literals
    /// and its value as a credential field when the key is credential-shaped.
    fn visit_map<A: MapAccess<'de>>(self, mut entries: A) -> Result<bool, A::Error> {
        let mut found = false;
        while let Some(key) = entries.next_key::<String>()? {
            found |= self.holds_literal(&key);
            found |= entries.next_value_seed(self.child(is_secret_name(&key)))?;
        }
        Ok(found)
    }
}
