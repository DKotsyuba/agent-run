//! Read-only exact transcript export fenced inside one SQLite read snapshot.

use agent_run_domain::{
    Error, Result,
    status::{Coverage, PinState, StatusMatch, StatusRequest, StatusSelector},
    transcript_exact::{
        ExactMessageChunk, MAX_CHUNK_BYTES, MAX_MESSAGE_BYTES, MAX_METADATA_BYTES,
        TranscriptExactRequest, TranscriptExactResponse, sequence,
    },
};
use rusqlite::{Connection, OptionalExtension, params};
use serde::Serialize;
use std::io::{self, Write};

/// Bounded serializer; no partial value escapes on source or encoding failure.
struct BoundedJson {
    bytes: Vec<u8>,
    limit: usize,
}

impl Write for BoundedJson {
    /// Refuses allocation past the declared encoded-message bound.
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        if bytes.len() > self.limit.saturating_sub(self.bytes.len()) {
            return Err(io::Error::other("exact transcript message exceeds budget"));
        }
        self.bytes.extend_from_slice(bytes);
        Ok(bytes.len())
    }
    /// No buffered external resource needs flushing.
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

/// Stable v1 JSON field order; sequence stays decimal text even above 2^53.
#[derive(Serialize)]
struct Message {
    seq: String,
    at: f64,
    role: String,
    name: Option<String>,
    content: String,
    raw_ref: Option<String>,
    error: Option<bool>,
    error_source: Option<String>,
    content_complete: bool,
}

/// Fixed payload-free abort reason. Prior pages remain historical, never a complete export.
fn aborted(reason: &'static str) -> Error {
    Error::TranscriptExportAborted(reason)
}

/// Reads one fragment with store, incarnation, active pin and lineage coverage
/// checked in the same snapshot as row bounds and bytes. No spool files are read.
pub fn read(
    conn: &Connection,
    request: &TranscriptExactRequest,
) -> Result<TranscriptExactResponse> {
    request.validate()?;
    let tx = conn.unchecked_transaction()?;
    let result = read_in_snapshot(&tx, request)?;
    tx.commit()?;
    Ok(result)
}

/// Caller-owned snapshot seam; used to verify concurrent append/release isolation.
pub fn read_in_snapshot(
    conn: &Connection,
    request: &TranscriptExactRequest,
) -> Result<TranscriptExactResponse> {
    request.validate()?;
    let status = crate::status::read_in_snapshot(
        conn,
        &StatusRequest {
            version: 1,
            expected_store_id: Some(request.expected_store_id.clone()),
            targets: vec![StatusSelector {
                agent_id: request.selector.agent_id.clone(),
                sequence: request.selector.sequence,
                execution_ref: Some(request.selector.execution_ref.clone()),
                pin_id: Some(request.selector.pin_id.clone()),
            }],
            include_view: false,
        },
    )?;
    let item = &status.response.items[0];
    if item.r#match != StatusMatch::Verified
        || item.pin_state != PinState::Active
        || item.coverage != Coverage::Complete
    {
        return Err(aborted(
            "exact transcript identity, active pin or coverage unavailable",
        ));
    }
    let id = status
        .selected_execution(0)
        .ok_or_else(|| aborted("exact transcript execution missing"))?;
    let cursor = sequence(&request.cursor)?;
    let upper = match request.upper_seq.as_deref() {
        Some(value) => sequence(value)?,
        None => conn.query_row(
            "SELECT COALESCE(MAX(seq),0) FROM messages WHERE agent_id=?",
            [id.as_str()],
            |row| row.get::<_, i64>(0),
        )?,
    };
    // A caller cannot substitute a global sequence owned by a different execution.
    for boundary in [cursor, upper] {
        if boundary > 0
            && !conn.query_row(
                "SELECT EXISTS(SELECT 1 FROM messages WHERE agent_id=?1 AND seq=?2)",
                params![id.as_str(), boundary],
                |row| row.get::<_, bool>(0),
            )?
        {
            return Err(aborted("exact transcript continuation row missing"));
        }
    }
    let mut output = TranscriptExactResponse {
        version: 1,
        store_id: status.response.store_id.clone(),
        selector: request.selector.clone(),
        cursor: request.cursor.clone(),
        upper_seq: upper.to_string(),
        offset: request.offset,
        message: None,
        next_cursor: request.cursor.clone(),
        next_offset: 0,
        snapshot_complete: cursor == upper,
    };
    // First inspect only scalar size/evidence columns; do not allocate oversized text.
    let metadata: Option<(i64, Option<i64>, bool)> = conn.query_row(
        "SELECT seq,content_complete,typeof(content)='text' AND length(CAST(content AS BLOB))<=?4 AND \
        typeof(role)='text' AND length(CAST(role AS BLOB))<=?5 AND \
        (name IS NULL OR (typeof(name)='text' AND length(CAST(name AS BLOB))<=?5)) AND \
        (raw_ref IS NULL OR (typeof(raw_ref)='text' AND length(CAST(raw_ref AS BLOB))<=?5)) AND \
        (error_source IS NULL OR (typeof(error_source)='text' AND length(CAST(error_source AS BLOB))<=?5)) \
        FROM messages WHERE agent_id=?1 AND seq>?2 AND seq<=?3 ORDER BY seq LIMIT 1",
        params![id.as_str(),cursor,upper,MAX_MESSAGE_BYTES as i64,MAX_METADATA_BYTES as i64],
        |row| Ok((row.get(0)?,row.get(1)?,row.get(2)?))).optional()?;
    if let Some((seq, complete, bounded)) = metadata {
        if complete != Some(1) {
            return Err(aborted("exact transcript content is incomplete or unknown"));
        }
        if !bounded {
            return Err(aborted(
                "exact transcript source exceeds budget or has invalid types",
            ));
        }
        let message = conn.query_row("SELECT at,role,name,content,raw_ref,error,error_source FROM messages WHERE agent_id=?1 AND seq=?2",
            params![id.as_str(),seq], |row| Ok(Message { seq: seq.to_string(), at: row.get(0)?, role: row.get(1)?, name: row.get(2)?, content: row.get(3)?,
                raw_ref: row.get(4)?, error: row.get(5)?, error_source: row.get(6)?, content_complete: true }))?;
        if !message.at.is_finite() || message.at < 0.0 {
            return Err(aborted("exact transcript timestamp is invalid"));
        }
        let mut encoded = BoundedJson {
            bytes: Vec::new(),
            limit: MAX_MESSAGE_BYTES,
        };
        serde_json::to_writer(&mut encoded, &message)
            .map_err(|_| aborted("exact transcript serialized message exceeds budget"))?;
        let sha256 = agent_run_platform::fs::sha256(&encoded.bytes)
            .parse::<agent_run_domain::Sha256Digest>()?;
        if request
            .expected_sha256
            .as_ref()
            .is_some_and(|expected| expected != &sha256)
        {
            return Err(aborted("exact transcript message digest changed"));
        }
        let text = String::from_utf8(encoded.bytes)
            .map_err(|_| aborted("exact transcript encoding is invalid"))?;
        let start = request.offset;
        if start >= text.len() || !text.is_char_boundary(start) {
            return Err(agent_run_domain::error::invalid(
                "exact transcript offset must be a UTF-8 boundary within the message",
            ));
        }
        let mut end = (start + MAX_CHUNK_BYTES).min(text.len());
        while !text.is_char_boundary(end) {
            end -= 1;
        }
        let complete = end == text.len();
        output.message = Some(ExactMessageChunk {
            seq: seq.to_string(),
            sha256,
            total_bytes: text.len(),
            start,
            end,
            text: text[start..end].to_owned(),
            complete,
        });
        output.next_cursor = if complete {
            seq.to_string()
        } else {
            request.cursor.clone()
        };
        output.next_offset = if complete { 0 } else { end };
        output.snapshot_complete = complete && seq == upper;
    } else if cursor != upper || request.offset != 0 {
        return Err(aborted("exact transcript continuation is missing"));
    }
    output.validate_for(request)?;
    // Leave 8 KiB for the bounded JSON-RPC ID and MCP structured-result envelope.
    let mut wire = BoundedJson {
        bytes: Vec::new(),
        limit: agent_run_domain::observation::SOCKET_MAX_FRAME_BYTES - 8192,
    };
    serde_json::to_writer(&mut wire, &output)
        .map_err(|_| aborted("exact transcript response exceeds frame budget"))?;
    Ok(output)
}
