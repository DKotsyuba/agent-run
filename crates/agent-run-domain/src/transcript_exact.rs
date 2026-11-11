//! Pinned exact-execution export; content is historical data, never authority.

use crate::{
    Result, Sha256Digest, StoreId,
    batch::{ExecutionRef, PinId},
    domain::AgentId,
    error::invalid,
};
use serde::{Deserialize, Serialize};

/// Maximum UTF-8 bytes in a single serialized-message fragment.
pub const MAX_CHUNK_BYTES: usize = 32 * 1024;
/// Maximum source text and maximum serialized message, independently enforced.
pub const MAX_MESSAGE_BYTES: usize = 16 * 1024 * 1024;
/// Maximum bytes per metadata string before loading it from SQLite.
pub const MAX_METADATA_BYTES: usize = 4096;

/// Complete caller-supplied execution and retention fence.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ExactSelector {
    /// Stable lineage ID or retained execution alias; never normalized.
    pub agent_id: AgentId,
    /// Exact positive execution sequence within the lineage.
    pub sequence: u32,
    /// Opaque incarnation receipt derived from the first provider attempt.
    pub execution_ref: ExecutionRef,
    /// Permanent retention handle that must still be active for this lineage.
    pub pin_id: PinId,
}

/// One export fragment request. Start with cursor "0", offset 0, no watermark/hash.
/// Continue with the returned watermark, cursor and offset; nonzero offsets require hash.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TranscriptExactRequest {
    /// Wire codec version, currently exactly one.
    pub version: u32,
    /// Mandatory store identity fence; checked before reading content.
    pub expected_store_id: StoreId,
    /// Full exact execution and active-retention identity.
    pub selector: ExactSelector,
    /// Last fully consumed global message sequence as canonical decimal text.
    pub cursor: String,
    #[serde(
        default,
        deserialize_with = "present_optional",
        skip_serializing_if = "Option::is_none"
    )]
    /// Fixed inclusive message watermark; capture on omission, echo thereafter.
    pub upper_seq: Option<String>,
    /// UTF-8 byte offset within the next serialized message; zero starts a row.
    pub offset: usize,
    #[serde(
        default,
        deserialize_with = "present_optional",
        skip_serializing_if = "Option::is_none"
    )]
    /// Required complete-message digest for nonzero offsets; absent at offset zero.
    pub expected_sha256: Option<Sha256Digest>,
}

/// Optional fields may be omitted, but explicit null is not a continuation token.
fn present_optional<'de, D, T>(deserializer: D) -> std::result::Result<Option<T>, D::Error>
where
    D: serde::Deserializer<'de>,
    T: Deserialize<'de>,
{
    T::deserialize(deserializer).map(Some)
}

/// Requires the field to be present while retaining explicit JSON null.
fn required_nullable<'de, D, T>(deserializer: D) -> std::result::Result<Option<T>, D::Error>
where
    D: serde::Deserializer<'de>,
    T: Deserialize<'de>,
{
    Option::<T>::deserialize(deserializer)
}

/// Parses a canonical nonnegative decimal SQLite sequence without floating point.
pub fn sequence(value: &str) -> Result<i64> {
    if value.is_empty()
        || value.len() > 19
        || (value.len() > 1 && value.starts_with('0'))
        || !value.bytes().all(|byte| byte.is_ascii_digit())
    {
        return Err(invalid("sequence must be a canonical decimal string"));
    }
    value
        .parse::<i64>()
        .map_err(|_| invalid("sequence exceeds i64 maximum"))
}

impl TranscriptExactRequest {
    /// Rejects ambiguous starts/continuations before reading any store content.
    pub fn validate(&self) -> Result<()> {
        let cursor = sequence(&self.cursor)?;
        let upper = self.upper_seq.as_deref().map(sequence).transpose()?;
        if self.version != 1
            || self.selector.sequence == 0
            || self.offset >= MAX_MESSAGE_BYTES
            || (self.offset != 0) != self.expected_sha256.is_some()
            || (upper.is_none() && (cursor != 0 || self.offset != 0))
            || upper.is_some_and(|upper| cursor > upper || (cursor == upper && self.offset != 0))
        {
            return Err(invalid("invalid exact transcript continuation"));
        }
        Ok(())
    }
}

/// Fragment of the immutable JSON message; offsets count unescaped UTF-8 bytes.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ExactMessageChunk {
    /// Global immutable message sequence encoded as decimal text.
    pub seq: String,
    /// Digest of all bytes of the complete serialized message, before transport escaping.
    pub sha256: Sha256Digest,
    /// Complete serialized message length in UTF-8 bytes, at most 16 MiB.
    pub total_bytes: usize,
    /// Inclusive UTF-8 byte offset of this fragment.
    pub start: usize,
    /// Exclusive UTF-8 byte offset of this fragment.
    pub end: usize,
    /// At most 32 KiB of serialized message JSON; may require assembly before parsing.
    pub text: String,
    /// True only when this fragment ends the selected message.
    pub complete: bool,
}

/// A bounded historical export page. Completion covers only rows up to upper_seq;
/// it makes no statement about execution completion, success or authorization.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TranscriptExactResponse {
    /// Wire codec version, currently exactly one.
    pub version: u32,
    /// Actual store identity observed in the content read snapshot.
    pub store_id: StoreId,
    /// Full exact execution and active-retention identity.
    pub selector: ExactSelector,
    /// Last fully consumed global message sequence as canonical decimal text.
    pub cursor: String,
    /// Fixed inclusive message watermark; capture on omission, echo thereafter.
    pub upper_seq: String,
    /// UTF-8 byte offset within the next serialized message; zero starts a row.
    pub offset: usize,
    /// One message fragment, or null at an explicitly empty/end boundary.
    #[serde(deserialize_with = "required_nullable")]
    pub message: Option<ExactMessageChunk>,
    /// Cursor for the next call; advances only when the current message ends.
    pub next_cursor: String,
    /// Byte offset for the next call; zero when the current message ends.
    pub next_offset: usize,
    /// No later row remains through the watermark; clients must follow every cursor from zero.
    pub snapshot_complete: bool,
}

impl TranscriptExactResponse {
    /// Checks correlation and continuation arithmetic before a transport publishes data.
    /// Clients must additionally verify SHA-256 after assembling the complete message.
    pub fn validate_for(&self, request: &TranscriptExactRequest) -> Result<()> {
        request.validate()?;
        let bad = || invalid("invalid exact transcript response");
        let upper = sequence(&self.upper_seq)?;
        let cursor = sequence(&self.cursor)?;
        let next = sequence(&self.next_cursor)?;
        if self.version != 1
            || self.store_id != request.expected_store_id
            || self.selector != request.selector
            || self.cursor != request.cursor
            || self.offset != request.offset
            || cursor > upper
            || request
                .upper_seq
                .as_ref()
                .is_some_and(|v| v != &self.upper_seq)
        {
            return Err(bad());
        }
        match &self.message {
            None if cursor == upper
                && self.offset == 0
                && self.next_offset == 0
                && next == cursor
                && self.snapshot_complete => {}
            Some(chunk) => {
                let seq = sequence(&chunk.seq)?;
                if seq <= cursor
                    || seq > upper
                    || chunk.start != self.offset
                    || chunk.total_bytes == 0
                    || chunk.total_bytes > MAX_MESSAGE_BYTES
                    || chunk.start >= chunk.end
                    || chunk.end > chunk.total_bytes
                    || chunk.end - chunk.start != chunk.text.len()
                    || chunk.text.len() > MAX_CHUNK_BYTES
                    || chunk.complete != (chunk.end == chunk.total_bytes)
                    || request
                        .expected_sha256
                        .as_ref()
                        .is_some_and(|h| h != &chunk.sha256)
                    || next != if chunk.complete { seq } else { cursor }
                    || self.next_offset != if chunk.complete { 0 } else { chunk.end }
                    || self.snapshot_complete != (chunk.complete && seq == upper)
                {
                    return Err(bad());
                }
            }
            _ => return Err(bad()),
        }
        Ok(())
    }
}
