//! Bounded indexed block views over immutable transcript rows.

use crate::Store;
use agent_run_domain::{
    domain::AgentId,
    transcript::{TranscriptQuery, TranscriptView},
    views::MessageView,
    Result,
};
use rusqlite::{params, Connection, OptionalExtension};
use serde_json::{json, Value};

/// Maximum fragments inspected per page, plus one boundary lookahead.
pub const MAX_BLOCK_SCAN_ROWS: usize = 4096;
/// Maximum included UTF-8 content per page; oversized rows report omission.
pub const MAX_BLOCK_PAGE_BYTES: usize = 256 * 1024;

/// Internal row identity; execution and attempt fields never leave this module.
struct Fragment {
    /// Exact execution owning the immutable row.
    execution: String,
    /// Native attempt scope; identical refs across attempts never merge.
    attempt: Option<String>,
    /// Public content and native evidence.
    message: MessageView,
    /// Original stored inline byte length, before bounded SQLite substring.
    stored_bytes: usize,
}

impl Fragment {
    /// Decodes the fixed allowlisted select; malformed SQLite values propagate.
    fn read(row: &rusqlite::Row<'_>) -> rusqlite::Result<Self> {
        Ok(Self {
            execution: row.get(0)?,
            attempt: row.get(1)?,
            message: MessageView {
                seq: row.get(2)?,
                at: row.get(3)?,
                role: row.get(4)?,
                name: row.get(5)?,
                content: row.get(6)?,
                raw_ref: row.get(7)?,
                error: row.get(8)?,
                error_source: row.get(9)?,
                content_complete: row.get(10)?,
                ..MessageView::default()
            },
            stored_bytes: row.get(11)?,
        })
    }

    /// Only adjacent fragments with a known shared identity and matching
    /// role/name/evidence/execution/attempt can belong to the same block.
    fn joins(&self, other: &Self) -> bool {
        self.message.raw_ref.is_some()
            && self.execution == other.execution
            && self.attempt == other.attempt
            && self.message.raw_ref == other.message.raw_ref
            && self.message.role == other.message.role
            && self.message.name == other.message.name
            && self.message.error == other.message.error
            && self.message.error_source == other.message.error_source
    }

    /// Caps one oversized legacy row on UTF-8 boundaries, explicitly counting
    /// omitted bytes. Reverse requests retain its suffix; no spool is expanded.
    fn bound(&mut self, reverse: bool) {
        let content = &mut self.message.content;
        if content.len() > MAX_BLOCK_PAGE_BYTES {
            if reverse {
                let mut start = content.len() - MAX_BLOCK_PAGE_BYTES;
                while !content.is_char_boundary(start) {
                    start += 1;
                }
                content.drain(..start);
            } else {
                let mut end = MAX_BLOCK_PAGE_BYTES;
                while !content.is_char_boundary(end) {
                    end -= 1;
                }
                content.truncate(end);
            }
        }
        if self.stored_bytes > content.len() {
            self.message.omitted_bytes = Some(self.stored_bytes - content.len());
            self.message.content_complete = Some(false);
        }
    }
}

/// Selects bounded row content; native fields and internal ownership stay separate.
const FIELDS: &str = "agent_id,attempt_id,seq,at,role,name,substr(content,1,65536),raw_ref,error,error_source,content_complete,length(CAST(content AS BLOB))";

/// Reads one adjacent row on the excluded side without loading its content.
/// It supplies only identity/evidence for partial-block boundary decisions.
fn neighbor(
    conn: &Connection,
    selection: &str,
    id: &AgentId,
    cursor: i64,
    reverse: bool,
) -> Result<Option<Fragment>> {
    let (comparison, order) = if reverse {
        (">=", "ASC")
    } else {
        ("<=", "DESC")
    };
    let fields = FIELDS.replace("substr(content,1,65536)", "''");
    Ok(conn.query_row(
        &format!("SELECT {fields} FROM messages WHERE {selection} AND seq{comparison}?2 ORDER BY seq {order} LIMIT 1"),
        params![id.as_str(), cursor], Fragment::read,
    ).optional()?)
}

impl Store {
    /// Counts unique observed native tool IDs of this exact execution. Only
    /// versioned native observers establish coverage; old/unobserved attempts
    /// and missing call IDs remain unknown. Duplicate fragments/completions do
    /// not add calls. Missing or conflicting result flags make failed null.
    /// Empty measured history is a known zero. This reads no tool arguments.
    pub fn tool_counts(&self, id: &AgentId) -> Result<agent_run_domain::views::ToolCountsView> {
        use agent_run_domain::views::ToolCountsView;
        self.get(id)?;
        let covered: bool = self.conn.query_row(
            "SELECT EXISTS(SELECT 1 FROM events WHERE agent_id=?1 AND kind='native_tool_observer_v1') AND NOT EXISTS(SELECT 1 FROM events WHERE agent_id=?1 AND kind='native_tool_coverage_gap_v1')              AND NOT EXISTS(SELECT 1 FROM attempts a WHERE a.agent_id=?1 AND a.process_identity IS NOT NULL              AND NOT EXISTS(SELECT 1 FROM events e WHERE e.agent_id=?1 AND e.attempt_id=a.id AND e.kind='native_tool_observer_v1'))",
            [id.as_str()], |row| row.get(0),
        )?;
        if !covered {
            return Ok(ToolCountsView::default());
        }
        // ponytail: per-execution scan; persist summaries only if measured list latency requires it.
        let mut statement = self.conn.prepare(
            "SELECT raw_ref,MIN(CASE WHEN role='tool_result' THEN error END),             MAX(CASE WHEN role='tool_result' THEN error END),             SUM(CASE WHEN role='tool_result' THEN 1 ELSE 0 END),COUNT(error)              FROM messages WHERE agent_id=?1 AND role IN ('tool_call','tool_result')              GROUP BY attempt_id,raw_ref"
        )?;
        let mut rows = statement.query([id.as_str()])?;
        let mut calls = 0;
        let mut failed = 0;
        let mut unknown = 0;
        while let Some(row) = rows.next()? {
            if row.get::<_, Option<String>>(0)?.is_none() {
                return Ok(ToolCountsView::default());
            }
            calls += 1;
            let min: Option<bool> = row.get(1)?;
            let max: Option<bool> = row.get(2)?;
            let results: u64 = row.get(3)?;
            let reported: u64 = row.get(4)?;
            if min.is_none() || min != max || results != reported {
                unknown += 1;
            } else if min == Some(true) {
                failed += 1;
            }
        }
        Ok(ToolCountsView {
            calls: Some(calls),
            failed: (unknown == 0).then_some(failed),
            unknown_results: Some(unknown),
        })
    }

    /// Reads raw compatible history or bounded blocks according to shared
    /// validated options. lineage selects the stable root; false pins execution.
    /// Invalid inputs reject before reads. No journal or spool file is modified.
    pub fn transcript_query(
        &self,
        id: &AgentId,
        query: &TranscriptQuery,
        lineage: bool,
    ) -> Result<Value> {
        query.validate()?;
        if query.view == TranscriptView::Raw {
            return if lineage {
                self.transcript_lineage(id, query.cursor, query.limit)
            } else {
                self.transcript(id, query.cursor, query.limit)
            };
        }
        let record = self.get(id)?;
        let selected = if lineage { &record.root_agent_id } else { id };
        let selection = if lineage {
            "root_agent_id=?1"
        } else {
            "agent_id=?1"
        };
        let reverse = query.tail_blocks.is_some() || query.before_cursor.is_some();
        let cursor = if reverse {
            query.before_cursor.unwrap_or(i64::MAX)
        } else {
            query.cursor
        };
        let limit = query.tail_blocks.unwrap_or(query.limit);
        let (comparison, order) = if reverse { ("<", "DESC") } else { (">", "ASC") };
        let fields = if reverse {
            FIELDS.replace("substr(content,1,65536)", "substr(content,-65536)")
        } else {
            FIELDS.to_owned()
        };
        let tx = self.conn.unchecked_transaction()?;
        let excluded = neighbor(&tx, selection, selected, cursor, reverse)?;
        let mut statement = tx.prepare(&format!(
            "SELECT {fields} FROM messages WHERE {selection} AND seq{comparison}?2 ORDER BY seq {order} LIMIT ?3"
        ))?;
        let mut rows = statement.query(params![
            selected.as_str(),
            cursor,
            MAX_BLOCK_SCAN_ROWS as i64 + 1
        ])?;
        let mut fragments: Vec<Fragment> = Vec::new();
        let mut bytes = 0;
        let mut groups = 0;
        let mut lookahead = None;
        while let Some(row) = rows.next()? {
            let mut fragment = Fragment::read(row)?;
            fragment.bound(reverse);
            let new_group = fragments
                .last()
                .is_none_or(|previous| !previous.joins(&fragment));
            if (new_group && groups == limit)
                || fragments.len() == MAX_BLOCK_SCAN_ROWS
                || (!fragments.is_empty()
                    && bytes + fragment.message.content.len() > MAX_BLOCK_PAGE_BYTES)
            {
                lookahead = Some(fragment);
                break;
            }
            groups += usize::from(new_group);
            bytes += fragment.message.content.len();
            fragments.push(fragment);
        }
        drop(rows);
        drop(statement);
        let mut partial_before = false;
        let mut partial_after = false;
        if let (Some(first), Some(last)) = (fragments.first(), fragments.last()) {
            if reverse {
                partial_after = excluded.as_ref().is_some_and(|other| first.joins(other));
                partial_before = lookahead.as_ref().is_some_and(|other| last.joins(other));
            } else {
                partial_before = excluded.as_ref().is_some_and(|other| first.joins(other));
                partial_after = lookahead.as_ref().is_some_and(|other| last.joins(other));
            }
        }
        let complete = lookahead.is_none();
        if reverse {
            fragments.reverse();
        }
        let mut messages: Vec<MessageView> = Vec::new();
        let mut previous: Option<Fragment> = None;
        for fragment in fragments {
            let joins = previous
                .as_ref()
                .is_some_and(|other| other.joins(&fragment));
            if joins {
                let block = messages.last_mut().expect("prior fragment has a block");
                block.content.push_str(&fragment.message.content);
                block.seq = fragment.message.seq;
                block.last_seq = Some(fragment.message.seq);
                block.content_complete =
                    match (block.content_complete, fragment.message.content_complete) {
                        (Some(false), _) | (_, Some(false)) => Some(false),
                        (Some(true), Some(true)) => Some(true),
                        _ => None,
                    };
                if let Some(bytes) = fragment.message.omitted_bytes {
                    block.omitted_bytes = Some(block.omitted_bytes.unwrap_or(0) + bytes);
                }
            } else {
                let mut message = fragment.message.clone();
                message.first_seq = Some(message.seq);
                message.last_seq = Some(message.seq);
                message.partial_before = Some(false);
                message.partial_after = Some(false);
                message.starts_block = Some(true);
                messages.push(message);
            }
            previous = Some(fragment);
        }
        if let Some(first) = messages.first_mut() {
            first.partial_before = Some(partial_before);
            first.starts_block = Some(!partial_before);
        }
        if let Some(last) = messages.last_mut() {
            last.partial_after = Some(partial_after);
        }
        let first = messages.first().and_then(|m| m.first_seq);
        let last = messages.last().and_then(|m| m.last_seq);
        let result = json!({
            "agent_id":id,"messages":messages,"cursor":query.cursor,"limit":limit,
            "view":"blocks","direction":if reverse {"backward"} else {"forward"},
            "before_cursor":query.before_cursor,
            "next_cursor":if !reverse && !complete {last} else {None},
            "previous_cursor":if reverse && !complete {first} else {None},
            "resume_cursor":last,"complete":complete,
        });
        tx.commit()?;
        Ok(result)
    }
}
