//! Shared bounded transcript options used by CLI and every wire transport.

use crate::{Result, error::invalid};
use serde::{Deserialize, Serialize};

/// Transcript representation; raw immutable journal rows remain the default.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TranscriptView {
    /// Historical raw journal rows, including opaque spool references.
    #[default]
    Raw,
    /// Consecutive known-identity fragments grouped into bounded blocks.
    Blocks,
}

impl std::str::FromStr for TranscriptView {
    type Err = crate::Error;

    /// Decodes only the enum's serialized names, sharing the wire validator
    /// with CLI input; other spellings return a typed ValidationError.
    fn from_str(value: &str) -> Result<Self> {
        serde_json::from_value(serde_json::Value::String(value.to_owned()))
            .map_err(|_| invalid("transcript view must be raw or blocks"))
    }
}

/// One read-only transcript request, independent of its stable agent selector.
/// Cursors are exclusive global journal sequences; zero starts forward history.
/// Reverse pages return chronological content and an exclusive previous cursor.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TranscriptQuery {
    /// Exclusive forward cursor; defaults to zero and must be nonnegative.
    #[serde(default)]
    pub cursor: i64,
    /// Maximum rows or blocks; defaults to 200, raw cap 1000, block cap 200.
    #[serde(default = "default_limit")]
    pub limit: usize,
    /// Representation; omission preserves raw history.
    #[serde(default)]
    pub view: TranscriptView,
    /// Last 1..=200 blocks; requires blocks and a zero forward cursor.
    #[serde(default)]
    pub tail_blocks: Option<usize>,
    /// Exclusive upper sequence for an older block page; requires blocks.
    #[serde(default)]
    pub before_cursor: Option<i64>,
}

/// Returns the shared historical transcript page-size default.
pub const fn default_limit() -> usize {
    200
}

impl Default for TranscriptQuery {
    /// Builds the compatible first raw page without selecting an agent.
    fn default() -> Self {
        Self {
            cursor: 0,
            limit: default_limit(),
            view: TranscriptView::Raw,
            tail_blocks: None,
            before_cursor: None,
        }
    }
}

impl TranscriptQuery {
    /// Rejects invalid bounds and conflicting forward/reverse selections before
    /// I/O. Blocks allow at most 200; raw permits its historical cap of 1000.
    pub fn validate(&self) -> Result<()> {
        let cap = if self.view == TranscriptView::Raw {
            1000
        } else {
            200
        };
        if self.cursor < 0 || self.limit == 0 || self.limit > cap {
            return Err(invalid("invalid transcript cursor or limit"));
        }
        if self.tail_blocks.is_some_and(|n| n == 0 || n > 200)
            || self.before_cursor.is_some_and(|seq| seq <= 0)
            || ((self.tail_blocks.is_some() || self.before_cursor.is_some())
                && (self.view != TranscriptView::Blocks || self.cursor != 0))
        {
            return Err(invalid(
                "tail_blocks/before_cursor require blocks, valid bounds and cursor zero",
            ));
        }
        Ok(())
    }
}

/// Decode an intact MCP text-block array for display only. Non-text blocks,
/// malformed JSON, empty arrays and oversized inputs remain verbatim so a view
/// cannot silently discard image/resource/error evidence. No status is inferred
/// from text. The caller still applies its terminal sanitizer and redaction.
pub fn display_tool_text(content: &str) -> std::borrow::Cow<'_, str> {
    use std::borrow::Cow;
    if content.len() > 65536 || !content.trim_start().starts_with('[') {
        return Cow::Borrowed(content);
    }
    let Ok(serde_json::Value::Array(blocks)) = serde_json::from_str(content) else {
        return Cow::Borrowed(content);
    };
    if blocks.is_empty()
        || !blocks
            .iter()
            .all(|b| b["type"] == "text" && b["text"].is_string())
    {
        return Cow::Borrowed(content);
    }
    Cow::Owned(
        blocks
            .iter()
            .map(|b| b["text"].as_str().expect("checked text block"))
            .collect::<Vec<_>>()
            .join("\n"),
    )
}

#[cfg(test)]
mod tool_display_tests {
    /// Only complete text-only MCP payloads decode; missing/error/image evidence
    /// is never turned into a success or discarded by presentation.
    #[test]
    fn mcp_text_blocks_decode_without_status_inference() {
        use super::display_tool_text;
        assert_eq!(
            display_tool_text(
                r#"[{"type":"text","text":"one"},{"type":"text","text":"FAILED two"}]"#
            ),
            "one\nFAILED two"
        );
        for raw in [
            "plain",
            "[]",
            r#"[{"type":"image","data":"x"}]"#,
            r#"[{"type":"text"}]"#,
            "[broken",
        ] {
            assert_eq!(display_tool_text(raw), raw);
        }
    }
}
