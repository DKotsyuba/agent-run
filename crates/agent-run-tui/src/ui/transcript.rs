//! The transcript pane: a four-row session header plus the conversation.
//!
//! Engines stream assistant text in small deltas, and the supervisor journals
//! every delta as its own message (`agent-run-core/src/stream.rs`), so the
//! viewer coalesces consecutive text with equal known native references,
//! roles and names into one flowing block; server boundary flags and tool activity
//! breaks the stream. A tool call and its following results render as one
//! compact row; the operator expands them (Enter or click) into the full
//! argument and payload listing on a code background.
//!
//! Lines are pre-wrapped by [`super::text::wrap`] and cut to the exact pane
//! width, so layout math (scroll offsets, click mapping) and the drawn frame
//! can never drift apart.

use super::{text, theme};
use crate::app::{self, App, SILENCE_WARN_SECONDS, TranscriptBuffer, human_duration};
use agent_run_domain::views::MessageView;
use ratatui::{
    Frame,
    layout::Rect,
    style::{Modifier, Style},
    text::{Line, Span},
    widgets::{Paragraph, Scrollbar, ScrollbarOrientation, ScrollbarState},
};

/// Header rows above the transcript body (title, meta, workdir, native usage).
pub const HEADER_ROWS: u16 = 4;
/// Columns the body reserves on top of its content width: a two-column
/// selection gutter plus the scrollbar track.
const FRAME_COLS: u16 = 3;
/// Wrapped lines a user prompt shows before collapsing.
const USER_COLLAPSE_LINES: usize = 4;

/// A coalesced run of same-identity streaming text, one tool call with its
/// paired results, or an orphan tool result.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StreamBlock {
    /// First message index of the block: the call, the orphan result, or
    /// the opening fragment of a text run.
    pub start: usize,
    /// Contiguous message count of the block's own span: text fragments,
    /// partial call arguments, or chunks of an unpaired tool result.
    pub len: usize,
    /// Whether the block renders as flowing text (as opposed to one tool row).
    pub text: bool,
    /// Message indices of the results paired to this tool call, in journal
    /// order; empty for text blocks and result-less calls.
    pub results: Vec<usize>,
}

/// Whether one role renders as coalesced flowing text.
fn is_text_role(role: &str) -> bool {
    matches!(role, "assistant" | "user" | "system")
}

/// Width-independent block grouping of one buffer, extended incrementally.
///
/// The transcript store is append-only, so grouping resumes at the first
/// message it has not consumed yet: a sync after an appended page costs the
/// new messages only, never a rescan of the history. An in-place change
/// (the buffer's content epoch moved) or a shrunken buffer resets it.
#[derive(Default)]
struct Grouping {
    /// Content epoch the grouping was built for.
    epoch: u64,
    /// Blocks in render order.
    blocks: Vec<StreamBlock>,
    /// Block index of every consumed message, in message order; a paired
    /// result maps to its call's block. Its length is the number of
    /// consumed messages.
    block_of: Vec<usize>,
    /// Block index of the latest call per known `raw_ref` in the current
    /// boundary scope; result names must also match before pairing.
    calls_by_ref: std::collections::HashMap<String, usize>,
    /// Blocks whose grouping changed after they were opened (an extended
    /// text run, a newly paired result), in change order with adjacent
    /// repeats collapsed; drained by [`cache`], which ignores the entries
    /// past its memoized rows (those build fresh anyway).
    touched: Vec<usize>,
}

impl Grouping {
    /// Brings the grouping up to date with the buffer and returns whether it
    /// was reset, in which case every memoized row is stale.
    fn sync(&mut self, buffer: &TranscriptBuffer) -> bool {
        let reset = self.epoch != buffer.epoch() || self.block_of.len() > buffer.messages.len();
        if reset {
            *self = Grouping {
                epoch: buffer.epoch(),
                ..Grouping::default()
            };
        }
        for index in self.block_of.len()..buffer.messages.len() {
            self.push(&buffer.messages, index);
        }
        reset
    }

    /// Consumes the next message at `index`, extending grouping and invalidating
    /// only affected cached blocks. Text joins only consecutive messages with
    /// an equal known native reference, role and name. Results pair to the
    /// latest call with an equal known reference and name, even out of order.
    /// Raw-row `starts_block` clears pending identities at execution/attempt
    /// boundaries; projected blocks mark ordinary starts too, so their tool
    /// results continue to pair by the scoped native reference and tool name.
    fn push(&mut self, messages: &[MessageView], index: usize) {
        let message = &messages[index];
        let boundary = message.starts_block == Some(true);
        // Projected blocks mark every logical block's start, including
        // results. Raw starts_block still marks a scope boundary.
        if boundary && message.first_seq.is_none() {
            self.calls_by_ref.clear();
        }
        let block = if is_text_role(&message.role) {
            let extends = !boundary
                && self.blocks.last().is_some_and(|last| {
                    let first = &messages[last.start];
                    last.text
                        && last.start + last.len == index
                        && first.role == message.role
                        && first.name == message.name
                        && first.error == message.error
                        && first.error_source == message.error_source
                        && first.raw_ref.is_some()
                        && first.raw_ref == message.raw_ref
                });
            if extends {
                let last = self.blocks.len() - 1;
                self.blocks[last].len += 1;
                self.touch(last);
                last
            } else {
                self.open(index, true)
            }
        } else {
            match message.role.as_str() {
                "tool_call" => {
                    let existing = message
                        .raw_ref
                        .as_ref()
                        .and_then(|raw_ref| self.calls_by_ref.get(raw_ref).copied())
                        .filter(|block| {
                            let prior = &self.blocks[*block];
                            message.partial_before == Some(true)
                                && prior.start + prior.len == index
                                && messages[prior.start].name == message.name
                        });
                    let block = if let Some(block) = existing {
                        self.blocks[block].len += 1;
                        self.touch(block);
                        block
                    } else {
                        self.open(index, false)
                    };
                    if let Some(raw_ref) = &message.raw_ref {
                        self.calls_by_ref.insert(raw_ref.clone(), block);
                    }
                    block
                }
                "tool_result" => {
                    let paired = message
                        .raw_ref
                        .as_ref()
                        .and_then(|raw_ref| self.calls_by_ref.get(raw_ref).copied())
                        .filter(|block| messages[self.blocks[*block].start].name == message.name);
                    match paired {
                        Some(block) => {
                            self.blocks[block].results.push(index);
                            self.touch(block);
                            block
                        }
                        None => {
                            let previous = self
                                .blocks
                                .last()
                                .filter(|block| {
                                    let prior = &messages[block.start];
                                    message.partial_before == Some(true)
                                        && block.start + block.len == index
                                        && prior.role == "tool_result"
                                        && prior.name == message.name
                                        && prior.raw_ref.is_some()
                                        && prior.raw_ref == message.raw_ref
                                })
                                .map(|_| self.blocks.len() - 1);
                            if let Some(block) = previous {
                                self.blocks[block].len += 1;
                                self.touch(block);
                                block
                            } else {
                                self.open(index, false)
                            }
                        }
                    }
                }
                _ => self.open(index, false),
            }
        };
        self.block_of.push(block);
    }

    /// Opens a one-message block at message `start` and returns its index.
    fn open(&mut self, start: usize, text: bool) -> usize {
        self.blocks.push(StreamBlock {
            start,
            len: 1,
            text,
            results: Vec::new(),
        });
        self.blocks.len() - 1
    }

    /// Records that an existing block's grouping changed.
    fn touch(&mut self, block: usize) {
        if self.touched.last() != Some(&block) {
            self.touched.push(block);
        }
    }
}

/// Groups every message of the buffer into render blocks from scratch (see
/// [`Grouping::push`] for the rules); the render cache keeps the same
/// grouping incrementally instead.
#[cfg_attr(not(test), allow(dead_code))] // exercised by the rendering tests
pub fn blocks(buffer: &TranscriptBuffer) -> Vec<StreamBlock> {
    let mut grouping = Grouping::default();
    grouping.sync(buffer);
    grouping.blocks
}

/// Rendered extent of one block: where its lines start and how many follow.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MessageLayout {
    /// First message index of the block.
    pub message: usize,
    /// First rendered body line of the block.
    pub line_start: usize,
    /// Rendered body lines of the block.
    pub line_count: usize,
}

/// Render context shared by every block of one frame.
pub(crate) struct Ctx {
    /// Animated spinner frame for running activity (always a static frame).
    pub(crate) spinner: &'static str,
    /// Whether the session is still live (running tools animate).
    pub(crate) live: bool,
    /// The caller's clock, in Unix epoch seconds: the wall clock in
    /// production ([`live_ctx`]), a fixed value in tests. Derived durations
    /// (a pending tool call's elapsed time) read it.
    pub(crate) now: f64,
}

/// Wall-clock seconds since the Unix epoch.
fn wall_clock_seconds() -> f64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|duration| duration.as_secs_f64())
        .unwrap_or(0.0)
}

/// The render context of one app frame: the current spinner tick and the
/// wall clock.
///
/// Running-tool elapsed time derives from the wall clock, not the newest
/// journal timestamp: a pending call's own message is the newest one, so
/// journal time would freeze its elapsed at zero.
pub(crate) fn live_ctx(app: &App, buffer: &TranscriptBuffer) -> Ctx {
    Ctx {
        spinner: app.spinner(),
        live: !buffer.agent.status.terminal(),
        now: wall_clock_seconds(),
    }
}

/// One rendered body row before pane assembly: left spans, optional
/// right-aligned spans, and a background that survives selection.
///
/// Rows are memoized per block, so nothing frame-animated is baked in: a
/// pending live call stores only its start time ([`Row::running_at`]) and
/// system rows mark their spinner slot ([`Row::spins`]); the spinner frame,
/// the running elapsed, and the session-liveness wording are substituted at
/// assembly time.
#[derive(Clone)]
struct Row {
    /// Block index the row belongs to, if any.
    block: Option<usize>,
    /// Left-aligned content (the pane prepends the selection gutter).
    left: Vec<Span<'static>>,
    /// Right-aligned content pinned to the pane's right edge.
    right: Vec<Span<'static>>,
    /// Row background kept even on selection (expanded payload rows).
    keep: Option<ratatui::style::Color>,
    /// Start time of a pending tool call: the row's right side renders as
    /// `running` with the live elapsed, or `no result` once the session
    /// ended, decided at assembly time.
    running_at: Option<f64>,
    /// Whether the first left span is the animated spinner slot.
    spins: bool,
}

/// One memoized block: its built rows, the expansion flag they were built
/// for, and whether a blank separator line precedes it.
#[derive(Default)]
struct CachedBlock {
    /// Whether any member was expanded when the rows were built.
    expanded: bool,
    /// Incremental text state; completed rows stay in this block.
    stream: Option<TextMemo>,
    /// Incremental result summary; collapsed tools never join payloads.
    results: ResultMemo,
    /// Built body rows of the block.
    rows: Vec<Row>,
    /// Whether a blank separator line precedes the block.
    gap: bool,
    /// Whether a temporary partial/truncated marker ends the cached rows.
    text_marker: bool,
    /// Accumulated text omission evidence, updated only from new fragments.
    text_truncated: bool,
    /// Argument fragments already used to build a call's cached description.
    call_parts: usize,
}

/// Unfinished text suffix and sanitizer state of one growing block.
struct TextMemo {
    /// Number of consumed deltas of the block.
    consumed: usize,
    /// Streaming terminal-escape gate.
    sanitizer: text::Sanitizer,
    /// Streaming wrapper with only the unfinished word/row retained.
    wrapper: text::Wrapper,
    /// Preview rows to replace on the next delta.
    preview: usize,
    /// Full user rows parked while only the collapsed prefix is displayed.
    parked: Vec<Row>,
    /// Whether the preceding user display used the parked rows.
    collapsed: bool,
    /// Bytes consumed, for the linear-work regression check.
    processed: usize,
}

/// Incremental bounded summary of a tool's paired result chunks.
#[derive(Default)]
struct ResultMemo {
    /// Number of paired chunks consumed.
    consumed: usize,
    /// First meaningful line, capped to 512 characters for cached spans.
    first: String,
    /// Characters retained in the bounded first line.
    first_chars: usize,
    /// Cached call description, parsed once even as results arrive.
    brief: Option<String>,
    /// Result bytes processed, for linear-work checks.
    processed: usize,
    /// Whether that first meaningful line has ended.
    first_done: bool,
    /// Newlines across all chunks, used without joining the payload.
    newlines: usize,
    /// Whether any sanitized content has arrived.
    nonempty: bool,
    /// Whether the last character was a newline.
    trailing_newline: bool,
    /// Cross-delta escape scanner.
    sanitizer: text::Sanitizer,
    /// Native failure evidence across the paired chunks: `Some(true)` when
    /// any chunk explicitly failed, `Some(false)` when all reported success,
    /// `None` when any chunk is unreported and none explicitly failed —
    /// never shown as success.
    error: Option<bool>,
    /// Whether any chunk reported its stored content as truncated.
    truncated: bool,
    /// Time of the last result chunk.
    last_at: f64,
}

impl ResultMemo {
    /// Adds exactly one result chunk, retaining bounded summary state.
    ///
    /// Failure evidence comes from the journaled native flag only
    /// (`error`), never from the content text.
    fn push(&mut self, message: &MessageView) {
        self.processed += message.content.len();
        let clean = self.sanitizer.push(&message.content);
        if !clean.is_empty() {
            self.nonempty = true;
            self.trailing_newline = clean.ends_with('\n');
            self.newlines += clean.bytes().filter(|c| *c == b'\n').count();
        }
        for segment in clean.split_inclusive('\n') {
            if self.first_done {
                break;
            }
            let part = if self.first.is_empty() {
                segment.trim_start()
            } else {
                segment
            };
            if part.is_empty() {
                continue;
            }
            self.first.extend(
                part.trim_end_matches('\n')
                    .chars()
                    .take(512 - self.first_chars),
            );
            self.first_chars = self.first.chars().count();
            if segment.ends_with('\n') {
                self.first_done = !self.first.is_empty();
            }
        }
        self.error = if self.consumed == 0 {
            message.error
        } else {
            match (self.error, message.error) {
                (Some(true), _) | (_, Some(true)) => Some(true),
                (Some(false), Some(false)) => Some(false),
                _ => None,
            }
        };
        self.truncated |= message.content_complete == Some(false);
        self.consumed += 1;
        self.last_at = message.at;
    }

    /// First meaningful line plus the exact running line count.
    fn summary(&self) -> String {
        let count = self.newlines + usize::from(self.nonempty && !self.trailing_newline);
        if count > 1 {
            format!("{} ({} lines)", truncate_one(self.first.trim(), 56), count)
        } else {
            truncate_one(self.first.trim(), 64)
        }
    }
}

/// Whether any member of one block is expanded.
fn block_is_expanded(buffer: &TranscriptBuffer, block: &StreamBlock) -> bool {
    buffer.is_expanded(buffer.messages[block.start].seq)
        || buffer.expanded.iter().any(|seq| {
            buffer
                .messages
                .binary_search_by_key(seq, |message| message.seq)
                .ok()
                .is_some_and(|index| block.results.binary_search(&index).is_ok())
        })
}

/// Per-block memoized render products for one body width.
///
/// The block grouping is width-independent and extends incrementally
/// ([`Grouping`]); each block's rows — the expensive sanitize/wrap/
/// pretty-print pipeline — are built once and reused until the block's
/// grouping changes (reported by the grouping as touched), its expansion
/// flag flips, the width changes, or the content epoch moves. A sync
/// therefore costs work proportional to what changed, not to the history:
/// an appended delta rebuilds the tail text block, a newly paired result its
/// call block, a toggle one block, and an unchanged state nothing. Line
/// starts are prefix sums recomputed from the first changed block only, and
/// the trailing blank plus the terminal outcome line render outside the
/// cache at assembly time.
pub struct RenderCache {
    /// Width-independent block grouping, kept across width changes.
    grouping: Grouping,
    /// Body width the memoized rows were built for.
    width: u16,
    /// Expansion revision the memoized expansion flags were checked against.
    expansion: u64,
    /// Memoized blocks in render order; after a sync one per grouped block.
    blocks: Vec<CachedBlock>,
    /// First body line of each memoized block, separators included;
    /// always as long as `blocks`.
    line_starts: Vec<usize>,
    /// Body height excluding the trailing blank and the outcome line.
    body: usize,
    /// Telemetry: block row builds since the buffer opened (tests).
    builds: u32,
}

impl RenderCache {
    /// An empty cache; every block builds on first use.
    pub(crate) fn new() -> Self {
        Self {
            grouping: Grouping::default(),
            width: 0,
            expansion: 0,
            blocks: Vec::new(),
            line_starts: Vec::new(),
            body: 0,
            builds: 0,
        }
    }

    /// Brings the grouping up to date; a grouping reset (an in-place content
    /// change) also drops every memoized row.
    fn sync_grouping(&mut self, buffer: &TranscriptBuffer) {
        if self.grouping.sync(buffer) {
            self.clear_rows();
        }
    }

    /// Rebases row memos after inserting `count` chronological messages at
    /// the front. Regrouping handles partial text/tool edges, but unchanged
    /// blocks move their rows and streaming state rather than rebuilding.
    /// Prefix sums are refreshed on the next sync; tail appends remain incremental.
    pub(crate) fn prepend(&mut self, messages: &[MessageView], count: usize) {
        if self.blocks.is_empty() {
            self.grouping = Grouping::default();
            return;
        }
        let mut old = std::collections::HashMap::new();
        for (mut block, cached) in std::mem::take(&mut self.grouping.blocks)
            .into_iter()
            .zip(std::mem::take(&mut self.blocks))
        {
            block.start += count;
            for result in &mut block.results {
                *result += count;
            }
            old.insert(block.start, (block, cached));
        }
        let epoch = self.grouping.epoch;
        self.grouping = Grouping {
            epoch,
            ..Grouping::default()
        };
        for index in 0..messages.len() {
            self.grouping.push(messages, index);
        }
        self.grouping.touched.clear();
        for (index, block) in self.grouping.blocks.iter().enumerate() {
            let cached = old.remove(&block.start).filter(|(prior, _)| prior == block);
            let mut cached = match cached {
                Some((_, cached)) => cached,
                None => {
                    self.grouping.touched.push(index);
                    CachedBlock::default()
                }
            };
            for row in &mut cached.rows {
                row.block = Some(index);
            }
            if let Some(stream) = &mut cached.stream {
                for row in &mut stream.parked {
                    row.block = Some(index);
                }
            }
            self.blocks.push(cached);
        }
        // Reuse the prefix pass even if only separators changed.
        self.line_starts.resize(self.blocks.len(), 0);
        if !self.blocks.is_empty() {
            self.grouping.touch(0);
        }
    }

    /// Drops every memoized row and line start.
    fn clear_rows(&mut self) {
        self.blocks.clear();
        self.line_starts.clear();
        self.body = 0;
    }
}

/// Borrow of a synced render cache; the slot stays borrowed while it lives.
pub(crate) struct CacheGuard<'a> {
    slot: std::cell::Ref<'a, RenderCache>,
}

impl std::ops::Deref for CacheGuard<'_> {
    type Target = RenderCache;

    fn deref(&self) -> &RenderCache {
        &self.slot
    }
}

/// Builds the memo of one block: its rows tagged with the block index and
/// its expansion flag. The separator is filled in by the prefix pass.
fn build_block(
    buffer: &TranscriptBuffer,
    grouped: &[StreamBlock],
    index: usize,
    width: u16,
    previous: Option<CachedBlock>,
) -> CachedBlock {
    let block = &grouped[index];
    let head = &buffer.messages[block.start];
    let mut cached = previous.unwrap_or_default();
    if cached.text_marker {
        cached.rows.pop();
        cached.text_marker = false;
    }
    let mut first_tag = 0;
    cached.expanded = block_is_expanded(buffer, block);
    if block.text {
        let body_width = usize::from(width).saturating_sub(2);
        let first_width = if head.role == "assistant" {
            body_width.saturating_sub(chrono_stamp(head.at).len() + 1)
        } else {
            body_width
        };
        let memo = cached.stream.get_or_insert_with(|| TextMemo {
            consumed: 0,
            sanitizer: text::Sanitizer::default(),
            wrapper: text::Wrapper::new(first_width, body_width),
            preview: 0,
            parked: Vec::new(),
            collapsed: false,
            processed: 0,
        });
        let mut rows = if memo.collapsed {
            std::mem::take(&mut memo.parked)
        } else {
            std::mem::take(&mut cached.rows)
        };
        first_tag = rows.len().saturating_sub(memo.preview);
        rows.truncate(first_tag);
        for message in &buffer.messages[block.start + memo.consumed..block.start + block.len] {
            cached.text_truncated |= message.content_complete == Some(false);
            let clean = memo.sanitizer.push(&message.content);
            memo.processed += message.content.len() + clean.len();
            memo.wrapper.push(&clean);
        }
        memo.consumed = block.len;
        let completed = std::mem::take(&mut memo.wrapper.rows);
        let offset = rows.len();
        rows.extend(
            text_rows_wrapped(buffer, block, &completed, offset, false)
                .into_iter()
                .map(|mut row| {
                    row.block = Some(index);
                    row
                }),
        );
        let preview = memo.wrapper.preview();
        memo.processed += preview.iter().map(String::len).sum::<usize>();
        memo.preview = preview.len();
        let offset = rows.len();
        rows.extend(
            text_rows_wrapped(buffer, block, &preview, offset, false)
                .into_iter()
                .map(|mut row| {
                    row.block = Some(index);
                    row
                }),
        );
        memo.collapsed =
            head.role == "user" && !cached.expanded && rows.len() > USER_COLLAPSE_LINES + 2;
        if memo.collapsed {
            cached.rows = rows.iter().take(USER_COLLAPSE_LINES + 1).cloned().collect();
            cached
                .rows
                .push(user_more_row(rows.len() - 1 - USER_COLLAPSE_LINES));
            memo.parked = rows;
            first_tag = 0;
        } else {
            cached.rows = rows;
        }
    } else if head.role == "tool_call" || head.role == "tool_result" {
        if head.role == "tool_call" {
            if cached.call_parts != block.len {
                cached.results.truncated |= buffer.messages
                    [block.start + cached.call_parts..block.start + block.len]
                    .iter()
                    .any(|message| message.content_complete == Some(false));
                let content: String = buffer.messages[block.start..block.start + block.len]
                    .iter()
                    .map(|message| message.content.as_str())
                    .collect();
                cached.results.brief = Some(call_summary(&content));
                cached.call_parts = block.len;
            }
            for message in &block.results[cached.results.consumed..] {
                cached.results.push(&buffer.messages[*message]);
            }
        } else {
            for message in
                &buffer.messages[block.start + cached.results.consumed..block.start + block.len]
            {
                cached.results.push(message);
            }
        }
        cached.rows = tool_rows(buffer, block, usize::from(width), &cached.results);
    } else {
        cached.rows = block_rows(buffer, grouped, index, usize::from(width));
    }
    let span = &buffer.messages[block.start..block.start + block.len];
    let partial = span
        .first()
        .is_some_and(|message| message.partial_before == Some(true))
        || span
            .last()
            .is_some_and(|message| message.partial_after == Some(true))
        || block
            .results
            .first()
            .is_some_and(|index| buffer.messages[*index].partial_before == Some(true))
        || block
            .results
            .last()
            .is_some_and(|index| buffer.messages[*index].partial_after == Some(true));
    let truncated = block.text && cached.text_truncated;
    if partial || truncated {
        cached.rows.push(Row {
            block: Some(index),
            left: vec![Span::styled(
                if truncated {
                    "  … content truncated"
                } else {
                    "  … partial block"
                },
                theme::dim(),
            )],
            right: Vec::new(),
            keep: None,
            running_at: None,
            spins: false,
        });
        cached.text_marker = true;
    }
    for row in cached.rows.iter_mut().skip(first_tag) {
        if row.block.is_none() {
            row.block = Some(index);
        }
    }
    cached
}

/// Syncs the render cache for one width and returns a borrow of it.
///
/// Only the blocks the grouping reports as touched, the blocks whose
/// expansion flipped (checked only when the buffer's expansion revision
/// moved), and new tail blocks build; separators and line starts are
/// recomputed from the first changed block onward. With nothing changed the
/// sync is a few integer comparisons.
fn cache(buffer: &TranscriptBuffer, width: u16) -> CacheGuard<'_> {
    {
        let mut guard = buffer.render_cache.borrow_mut();
        let slot = &mut *guard;
        slot.sync_grouping(buffer);
        if slot.width != width {
            // A resize: every row rewraps; the grouping survives.
            slot.clear_rows();
            slot.width = width;
        }
        let memo = slot.blocks.len();
        let mut stale: Vec<usize> = std::mem::take(&mut slot.grouping.touched)
            .into_iter()
            .filter(|index| *index < memo)
            .collect();
        let grouped = &slot.grouping.blocks;
        if slot.expansion != buffer.expansion() {
            slot.expansion = buffer.expansion();
            stale.extend((0..memo).filter(|index| {
                slot.blocks[*index].expanded != block_is_expanded(buffer, &grouped[*index])
            }));
        }
        // A block can be both regrouped and flipped: build it once.
        stale.sort_unstable();
        stale.dedup();
        // First block whose separator or line start may have moved.
        let mut first_dirty = memo;
        for index in stale {
            let previous = std::mem::take(&mut slot.blocks[index]);
            slot.blocks[index] = build_block(buffer, grouped, index, width, Some(previous));
            slot.builds += 1;
            first_dirty = first_dirty.min(index);
        }
        for index in memo..grouped.len() {
            slot.blocks
                .push(build_block(buffer, grouped, index, width, None));
            slot.builds += 1;
        }
        // Prefix sums from the first changed block: O(1) for tail appends.
        let mut line = match first_dirty.checked_sub(1) {
            Some(prev) => slot.line_starts[prev] + slot.blocks[prev].rows.len(),
            None => 0,
        };
        slot.line_starts.truncate(first_dirty);
        for index in first_dirty..grouped.len() {
            let gap = has_separator(buffer, grouped, index);
            let cached = &mut slot.blocks[index];
            cached.gap = gap;
            line += usize::from(gap);
            slot.line_starts.push(line);
            line += cached.rows.len();
        }
        slot.body = line;
    }
    CacheGuard {
        slot: buffer.render_cache.borrow(),
    }
}

/// The row a global body line belongs to, and the block it came from.
enum BodyLine<'a> {
    /// A blank separator row before a block.
    Gap(usize),
    /// A memoized row of one block.
    Row(&'a Row),
}

/// Resolves one global body line against the synced cache.
///
/// A binary search over the line starts: O(log blocks) per visible row.
fn body_line(cache: &RenderCache, line: usize) -> Option<BodyLine<'_>> {
    // Blocks starting at or before the line; the last of them owns it.
    let after = cache.line_starts.partition_point(|start| *start <= line);
    if after < cache.blocks.len() && cache.blocks[after].gap && line + 1 == cache.line_starts[after]
    {
        return Some(BodyLine::Gap(after));
    }
    let index = after.checked_sub(1)?;
    let offset = line - cache.line_starts[index];
    cache.blocks[index].rows.get(offset).map(BodyLine::Row)
}

/// Whether a blank separator row sits before block `index`.
///
/// Collapsed tool rows stack without a gap; every other block pair is
/// separated by exactly one blank row.
fn has_separator(buffer: &TranscriptBuffer, blocks: &[StreamBlock], index: usize) -> bool {
    if index == 0 {
        return false;
    }
    let prev = &blocks[index - 1];
    let current = &blocks[index];
    let both_tools = !prev.text && !current.text;
    let prev_expanded = buffer.is_expanded(buffer.messages[prev.start].seq);
    !(both_tools && !prev_expanded)
}

/// Computes the body layout for one width from the memoized blocks.
#[cfg_attr(not(test), allow(dead_code))] // exercised by the layout tests
pub fn layout(buffer: &TranscriptBuffer, width: u16) -> Vec<MessageLayout> {
    let guard = cache(buffer, width);
    guard
        .blocks
        .iter()
        .enumerate()
        .map(|(index, cached)| MessageLayout {
            message: guard.grouping.blocks[index].start,
            line_start: guard.line_starts[index] + usize::from(buffer.blocks_view),
            line_count: cached.rows.len(),
        })
        .collect()
}

/// Rendered history height excluding the outcome tail, for prepend anchoring.
pub(crate) fn history_height(buffer: &TranscriptBuffer, width: u16) -> usize {
    cache(buffer, width).body + usize::from(buffer.blocks_view)
}

/// Total rendered body height for one width, the trailing blank and the
/// terminal outcome line included (both render outside the cache).
pub(crate) fn total_height(buffer: &TranscriptBuffer, width: u16) -> usize {
    let guard = cache(buffer, width);
    tail_height(buffer) + guard.body + usize::from(buffer.blocks_view)
}

/// Height of the trailing blank and the terminal outcome. An outcome needs
/// the beginning and current forward tail to have arrived; reverse complete
/// alone cannot establish that the forward catch-up finished.
fn tail_height(buffer: &TranscriptBuffer) -> usize {
    1 + usize::from(
        buffer.agent.status.terminal() && buffer.history_complete && buffer.tail_complete,
    )
}

/// The body scroll offset: pinned to the tail in follow mode, `from_bottom`
/// rows above it otherwise.
pub fn scroll_offset(buffer: &TranscriptBuffer, total: usize, viewport: usize) -> usize {
    let max = total.saturating_sub(viewport);
    if buffer.follow {
        max
    } else {
        max.saturating_sub(buffer.from_bottom)
    }
}

/// The body line index of the message cursor, if any message is loaded.
pub(crate) fn cursor_line(buffer: &TranscriptBuffer, width: u16) -> Option<usize> {
    let guard = cache(buffer, width);
    let current = *guard.grouping.block_of.get(buffer.cursor)?;
    guard
        .line_starts
        .get(current)
        .map(|line| line + usize::from(buffer.blocks_view))
}

/// Moves the cursor one block up or down (positive toward the tail).
///
/// Only the width-independent grouping is synced (incrementally) and the
/// cursor's block is an index lookup, so a move costs no row work and never
/// invalidates the memoized rows.
pub fn move_cursor_block(buffer: &mut TranscriptBuffer, delta: i64) {
    let target = {
        let mut slot = buffer.render_cache.borrow_mut();
        slot.sync_grouping(buffer);
        let grouping = &slot.grouping;
        grouping.blocks.len().checked_sub(1).map(|last| {
            let current = grouping
                .block_of
                .get(buffer.cursor)
                .copied()
                .unwrap_or(last);
            let target = (current as i64 + delta).clamp(0, last as i64) as usize;
            grouping.blocks[target].start
        })
    };
    buffer.cursor = target.unwrap_or(0);
}

/// The first message of the block under one body line, if any.
///
/// Separator lines belong to the block that follows them.
pub(crate) fn message_at(buffer: &TranscriptBuffer, width: u16, line: usize) -> Option<usize> {
    let guard = cache(buffer, width);
    let line = line.checked_sub(usize::from(buffer.blocks_view))?;
    let index = match body_line(&guard, line)? {
        BodyLine::Gap(index) => index,
        BodyLine::Row(row) => row.block?,
    };
    Some(guard.grouping.blocks[index].start)
}

/// How many block row builds the buffer's cache has performed; test telemetry.
#[cfg(test)]
pub(crate) fn cache_block_builds(buffer: &TranscriptBuffer) -> u32 {
    buffer.render_cache.borrow().builds
}

/// Content width of the body for one pane: pane columns minus gutter and
/// scrollbar track.
pub fn body_width(pane: Rect) -> u16 {
    pane.width.saturating_sub(FRAME_COLS)
}

/// Builds the rows of one block according to its role and expansion.
fn block_rows(
    buffer: &TranscriptBuffer,
    blocks: &[StreamBlock],
    index: usize,
    width: usize,
) -> Vec<Row> {
    let block = &blocks[index];
    let message = &buffer.messages[block.start];
    match message.role.as_str() {
        "tool_call" | "tool_result" => unreachable!("tool blocks use their incremental memo"),
        _ => text_rows(buffer, block, width),
    }
}

/// Builds standalone text rows and the whole-block test oracle. Coalesced
/// roles instead retain their scanner, wrapper and completed rows in TextMemo.
fn text_rows(buffer: &TranscriptBuffer, block: &StreamBlock, width: usize) -> Vec<Row> {
    let first = &buffer.messages[block.start];
    let stamp = chrono_stamp(first.at);
    let mut raw = String::new();
    for message in &buffer.messages[block.start..block.start + block.len] {
        raw.push_str(&message.content);
    }
    let content = agent_run::transcript::sanitize(&raw);
    // Assistant text shares its first row with the right-aligned timestamp,
    // so that first wrapped line gets a narrower budget.
    let body_width = width.saturating_sub(2);
    let wrapped = if first.role == "assistant" {
        let first_budget = body_width.saturating_sub(stamp.chars().count() + 1);
        text::wrap_first(&content, first_budget, body_width)
    } else {
        text::wrap(&content, body_width)
    };
    text_rows_wrapped(buffer, block, &wrapped, 0, true)
}

/// Builds only the newly completed or unfinished rows of a text block.
/// Offset counts its already cached prefix, keeping the marker/stamp on row zero.
/// Standalone user rows may collapse when allowed; incremental callers retain
/// full rows themselves and construct only the bounded visible prefix.
fn text_rows_wrapped(
    buffer: &TranscriptBuffer,
    block: &StreamBlock,
    wrapped: &[String],
    offset: usize,
    collapse_allowed: bool,
) -> Vec<Row> {
    let p = theme::palette();
    let first = &buffer.messages[block.start];
    let expanded = buffer.is_expanded(first.seq);
    let stamp = chrono_stamp(first.at);
    let mut rows = Vec::new();
    match first.role.as_str() {
        "user" => {
            if offset == 0 {
                rows.push(Row {
                    block: None,
                    left: vec![Span::styled("› prompt", theme::accent())],
                    right: vec![Span::styled(stamp, theme::dim())],
                    keep: None,
                    running_at: None,
                    spins: false,
                });
            }
            let collapsed =
                collapse_allowed && !expanded && wrapped.len() > USER_COLLAPSE_LINES + 1;
            let shown = if collapsed {
                &wrapped[..USER_COLLAPSE_LINES]
            } else {
                wrapped
            };
            for line in shown {
                rows.push(Row {
                    block: None,
                    left: vec![Span::styled(format!("  {line}"), Style::new().fg(p.white))],
                    right: Vec::new(),
                    keep: None,
                    running_at: None,
                    spins: false,
                });
            }
            if collapsed {
                rows.push(user_more_row(wrapped.len() - USER_COLLAPSE_LINES));
            }
        }

        "assistant" => {
            for (index, line) in wrapped
                .iter()
                .enumerate()
                .map(|(index, line)| (index + offset, line))
            {
                let mut left = Vec::new();
                if index == 0 {
                    left.push(Span::styled("◆ ", Style::new().fg(p.accent)));
                    left.push(Span::styled(line.clone(), Style::new().fg(p.fg)));
                } else {
                    left.push(Span::styled(format!("  {line}"), Style::new().fg(p.fg)));
                }
                rows.push(Row {
                    block: None,
                    left,
                    right: if index == 0 {
                        vec![Span::styled(stamp.clone(), theme::dim())]
                    } else {
                        Vec::new()
                    },
                    keep: None,
                    running_at: None,
                    spins: false,
                });
            }
        }
        _ => {
            // System and other notes render dimmed and italic.
            let italic = Style::new().fg(p.gray).add_modifier(Modifier::ITALIC);
            for (index, line) in wrapped
                .iter()
                .enumerate()
                .map(|(index, line)| (index + offset, line))
            {
                let mut left = Vec::new();
                let spins = index == 0;
                if spins {
                    // The spinner frame substitutes at assembly time.
                    left.push(Span::styled("  ", theme::dim()));
                } else {
                    left.push(Span::raw("  "));
                }
                left.push(Span::styled(line.clone(), italic));
                rows.push(Row {
                    block: None,
                    left,
                    right: Vec::new(),
                    keep: None,
                    running_at: None,
                    spins,
                });
            }
        }
    }
    rows
}

/// Bounded expansion hint for hidden user rows; shared by static and incremental views.
fn user_more_row(hidden: usize) -> Row {
    Row {
        block: None,
        left: vec![
            Span::styled(format!("  … {hidden} more lines  "), theme::dim()),
            Span::styled("⏎", theme::accent()),
            Span::styled(" expand", theme::dim()),
        ],
        right: Vec::new(),
        keep: None,
        running_at: None,
        spins: false,
    }
}

/// Builds the rows of one tool block: a single collapsed summary row per
/// call (or orphan result), or the expanded argument and payload listing on
/// a code background.
///
/// All result chunks paired to the call by known `raw_ref` and tool name
/// update the bounded summary in journal order. Evidence and truncation
/// annotations take precedence over long summary text within the row width.
/// Expansion joins the full payload once; duration comes from the last chunk.
fn tool_rows(
    buffer: &TranscriptBuffer,
    block: &StreamBlock,
    width: usize,
    memo: &ResultMemo,
) -> Vec<Row> {
    let p = theme::palette();
    let head = &buffer.messages[block.start];
    let is_call = head.role == "tool_call";
    let expanded = buffer.is_expanded(head.seq);
    let name = truncate_one(&agent_run::transcript::sanitize(&tool_name(head)), 512);
    let brief = if is_call {
        memo.brief.clone().unwrap_or_default()
    } else {
        memo.summary()
    };
    let error = memo.error == Some(true);
    let name_color = if error { p.red } else { p.accent };
    let marker = if expanded { "▾ " } else { "▸ " };
    let (right, running_at) = tool_summary_right(
        head,
        is_call,
        memo,
        width.saturating_sub(Span::raw(&name).width() + 4),
    );

    let mut rows = vec![Row {
        block: None,
        left: vec![
            Span::styled(marker, theme::dim()),
            Span::styled(
                name,
                Style::new().fg(name_color).add_modifier(Modifier::BOLD),
            ),
            Span::styled(
                format!("  {brief}"),
                Style::new().fg(if !is_call && error { p.red } else { p.white }),
            ),
        ],
        right,
        running_at,
        keep: if expanded { Some(p.code) } else { None },
        spins: false,
    }];
    if !expanded {
        return rows;
    }

    // Full arguments are wrapped once; every cached span stays pane-bounded.
    let joined_head;
    let args_head = if block.len > 1 && is_call {
        joined_head = MessageView {
            content: buffer.messages[block.start..block.start + block.len]
                .iter()
                .map(|message| message.content.as_str())
                .collect(),
            ..head.clone()
        };
        &joined_head
    } else {
        head
    };
    let args = tool_args(args_head);
    for (key, value) in &args {
        let color = if key == "command" || key == "cmd" {
            p.orange
        } else {
            p.white
        };
        let value = agent_run::transcript::sanitize(value);
        let label = truncate_one(
            &agent_run::transcript::sanitize(key),
            width.saturating_sub(5).min(512),
        );
        let label_width = Span::raw(&label).width().max(10);
        for (index, line) in text::wrap(&value, width.saturating_sub(4 + label_width))
            .into_iter()
            .enumerate()
        {
            rows.push(Row {
                block: None,
                left: vec![
                    Span::raw("    "),
                    Span::styled(
                        if index == 0 {
                            format!("{label:<label_width$}")
                        } else {
                            " ".repeat(label_width)
                        },
                        theme::dim(),
                    ),
                    Span::styled(line, Style::new().fg(color)),
                ],
                right: Vec::new(),
                keep: Some(p.code),
                running_at: None,
                spins: false,
            });
        }
    }
    let results: Vec<&MessageView> = if is_call {
        block.results.iter().map(|i| &buffer.messages[*i]).collect()
    } else {
        buffer.messages[block.start..block.start + block.len]
            .iter()
            .collect()
    };
    let joined = concat_results(&results);
    let payloads = payload_lines(&joined);
    if !payloads.is_empty() && !args.is_empty() {
        let dots = "┄".repeat(width.saturating_sub(8).min(60));
        rows.push(Row {
            block: None,
            left: vec![
                Span::raw("    "),
                Span::styled(dots, Style::new().fg(p.black)),
            ],
            right: Vec::new(),
            keep: Some(p.code),
            running_at: None,
            spins: false,
        });
    }
    for payload in payloads {
        for line in text::wrap(&payload, width.saturating_sub(4)) {
            rows.push(Row {
                block: None,
                left: vec![Span::raw("    "), Span::styled(line, Style::new().fg(p.fg))],
                right: Vec::new(),
                keep: Some(p.code),
                running_at: None,
                spins: false,
            });
        }
    }
    // Spooled-out content stays explicit instead of reading as the whole
    // payload.
    if memo.truncated {
        rows.push(Row {
            block: None,
            left: vec![
                Span::raw("    "),
                Span::styled("… content truncated", theme::dim()),
            ],
            right: Vec::new(),
            keep: Some(p.code),
            running_at: None,
            spins: false,
        });
    }
    rows.push(Row {
        block: None,
        left: Vec::new(),
        right: Vec::new(),
        keep: Some(p.code),
        running_at: None,
        spins: false,
    });
    rows
}

/// Concatenates the result chunks of one call in journal order.
fn concat_results(results: &[&MessageView]) -> String {
    let mut joined = String::new();
    for result in results {
        joined.push_str(&result.content);
    }
    joined
}

/// Builds a tool row's right side within `width` terminal cells. Native
/// evidence and truncation/duration suffixes survive long payload summaries.
/// Pending calls retain a truncation annotation and defer clocks to assembly.
fn tool_summary_right(
    head: &MessageView,
    is_call: bool,
    memo: &ResultMemo,
    width: usize,
) -> (Vec<Span<'static>>, Option<f64>) {
    let p = theme::palette();
    if memo.consumed > 0 {
        let style = if memo.error == Some(true) {
            Style::new().fg(p.red)
        } else {
            theme::dim()
        };
        let evidence = match memo.error {
            Some(true) => "[error] ",
            Some(false) => "[ok] ",
            None => "[unknown] ",
        };
        let summary = vec![
            Span::styled(evidence, style),
            Span::styled(memo.summary(), style),
        ];
        let mut suffix = Vec::new();
        if memo.truncated {
            suffix.push(Span::styled(" … content truncated", theme::dim()));
        }
        if is_call {
            let duration = (memo.last_at - head.at).max(0.0);
            suffix.push(Span::styled(
                format!("  {:>5}", human_duration(duration)),
                theme::dim(),
            ));
        }
        let cells: usize = summary.iter().chain(&suffix).map(Span::width).sum();
        let right = if cells > width {
            text::lr(summary, suffix, width, Style::new())
        } else {
            summary.into_iter().chain(suffix).collect()
        };
        return (right, None);
    }
    let right = if memo.truncated {
        vec![Span::styled("… content truncated  ", theme::dim())]
    } else {
        Vec::new()
    };
    (right, Some(head.at))
}

/// The outcome row that closes a finished session's transcript.
fn end_row(agent: &agent_run_domain::AgentView) -> Row {
    use agent_run_domain::domain::Status;
    let p = theme::palette();
    let (glyph, text, color) = match agent.status {
        Status::Succeeded => (
            "✓",
            format!("finished in {}", human_duration(agent.elapsed_seconds)),
            p.green,
        ),
        Status::Failed => (
            "✗",
            match &agent.failure_text {
                Some(failure) => format!("failed · {}", truncate_one(failure, 80)),
                None => "failed".to_string(),
            },
            p.red,
        ),
        Status::TimedOut => (
            "◷",
            format!("timed out after {}", human_duration(agent.elapsed_seconds)),
            p.red,
        ),
        Status::Cancelled => ("⊘", "cancelled".to_string(), p.gray),
        Status::Lost => ("◌", "lost".to_string(), p.magenta),
        _ => {
            return Row {
                block: None,
                left: Vec::new(),
                right: Vec::new(),
                keep: None,
                running_at: None,
                spins: false,
            };
        }
    };
    let style = Style::new().fg(color).add_modifier(Modifier::BOLD);
    Row {
        block: None,
        left: vec![
            Span::styled(glyph, style),
            Span::styled(format!(" {text}"), style),
        ],
        right: Vec::new(),
        keep: None,
        running_at: None,
        spins: false,
    }
}

/// The tool display name (role-specific `name`, falling back to the role).
fn tool_name(message: &MessageView) -> String {
    message.name.clone().unwrap_or_else(|| message.role.clone())
}

/// Brief description of a tool call: the command-like argument, then the
/// human `description`.
fn call_summary(content: &str) -> String {
    let parsed: Option<serde_json::Value> = serde_json::from_str(content).ok();
    let candidate = parsed
        .as_ref()
        .and_then(|v| v.get("command"))
        .or_else(|| parsed.as_ref().and_then(|v| v.get("cmd")))
        .or_else(|| parsed.as_ref().and_then(|v| v.get("file_path")))
        .or_else(|| parsed.as_ref().and_then(|v| v.get("pattern")))
        .or_else(|| parsed.as_ref().and_then(|v| v.get("path")))
        .or_else(|| parsed.as_ref().and_then(|v| v.get("skill")))
        .or_else(|| parsed.as_ref().and_then(|v| v.get("prompt")))
        .or_else(|| parsed.as_ref().and_then(|v| v.get("description")));
    match candidate.and_then(|v| v.as_str()) {
        Some(text) => truncate_one(&agent_run::transcript::sanitize(text), 64),
        None => truncate_one(&agent_run::transcript::sanitize(content), 64),
    }
}

/// The arguments of one tool call as display strings, key-sorted.
fn tool_args(message: &MessageView) -> Vec<(String, String)> {
    let Ok(serde_json::Value::Object(map)) = serde_json::from_str(&message.content) else {
        return Vec::new();
    };
    map.into_iter()
        .map(|(key, value)| {
            let text = match value {
                serde_json::Value::String(text) => text,
                other => other.to_string(),
            };
            (key, text)
        })
        .collect()
}

/// The full payload lines of one result body, pretty-printed when JSON.
fn payload_lines(content: &str) -> Vec<String> {
    let content = agent_run::transcript::sanitize(content);
    match serde_json::from_str::<serde_json::Value>(&content) {
        Ok(value) => serde_json::to_string_pretty(&value)
            .unwrap_or(content)
            .lines()
            .map(str::to_string)
            .collect(),
        Err(_) => content.lines().map(str::to_string).collect(),
    }
}

/// Renders the transcript pane into the given area.
///
/// The body reads the per-block memo cache, assembles only the visible
/// rows, and substitutes the frame-animated parts (spinner frames, running
/// elapsed, the outcome line) at assembly time.
pub fn render(f: &mut Frame, app: &App, area: Rect, split: bool) {
    let p = theme::palette();
    let focused = app.screen == crate::app::Screen::Transcript;
    let Some(buffer) = app.transcript.as_ref() else {
        f.render_widget(
            Paragraph::new(Line::from(Span::styled(
                "  no session selected",
                theme::dim(),
            ))),
            area,
        );
        return;
    };
    if area.width < FRAME_COLS + 4 || area.height <= HEADER_ROWS {
        return;
    }
    render_header(
        f,
        app,
        buffer,
        split,
        Rect {
            height: HEADER_ROWS,
            ..area
        },
    );

    let body = Rect {
        x: area.x,
        y: area.y + HEADER_ROWS,
        width: area.width,
        height: area.height - HEADER_ROWS,
    };
    let content_width = body_width(area);
    let ctx = live_ctx(app, buffer);
    let viewport = body.height as usize;

    if buffer.messages.is_empty() {
        let placeholder = Line::from(Span::styled(
            if buffer.history_complete {
                "No transcript messages yet.".to_string()
            } else {
                format!("{} Loading transcript…", app.spinner())
            },
            theme::dim(),
        ));
        f.render_widget(
            Paragraph::new(placeholder),
            Rect {
                width: area.width - 1,
                ..body
            },
        );
        return;
    }

    let guard = cache(buffer, content_width);
    let top = usize::from(buffer.blocks_view);
    let total = tail_height(buffer) + guard.body + top;
    let offset = scroll_offset(buffer, total, viewport);

    let mut lines = Vec::with_capacity(viewport);
    for line in offset..(offset + viewport).min(total) {
        if top > 0 && line == 0 {
            lines.push(Line::from(Span::styled(
                if buffer.loading_older {
                    format!("  {} loading older history…", app.spinner())
                } else if buffer.previous_cursor.is_none() {
                    "  beginning of transcript".into()
                } else {
                    "  scroll up for older history".into()
                },
                theme::dim(),
            )));
            continue;
        }
        let line = line - top;
        match body_line(&guard, line) {
            Some(BodyLine::Row(row)) => lines.push(assemble_row(
                buffer,
                &guard,
                row,
                content_width as usize,
                focused,
                &ctx,
            )),
            Some(BodyLine::Gap(_)) => lines.push(Line::from("")),
            None => {
                // The tail below the last block: trailing blank, then the
                // outcome line of finished sessions.
                if line == guard.body {
                    lines.push(Line::from(""));
                } else {
                    lines.push(assemble_row(
                        buffer,
                        &guard,
                        &end_row(&buffer.agent),
                        content_width as usize,
                        focused,
                        &ctx,
                    ));
                }
            }
        }
    }
    drop(guard);
    f.render_widget(
        Paragraph::new(lines),
        Rect {
            width: area.width - 1,
            ..body
        },
    );

    if total > viewport {
        // Ratatui maps `position` over `content_length - 1`, so the scrollable
        // range — not the whole content — is the length: at the tail
        // (offset == total - viewport) the thumb reaches the last track row.
        let mut scrollbar = ScrollbarState::new(total - viewport + 1)
            .position(offset)
            .viewport_content_length(viewport);
        f.render_stateful_widget(
            Scrollbar::new(ScrollbarOrientation::VerticalRight)
                .begin_symbol(None)
                .end_symbol(None)
                .thumb_symbol("┃")
                .track_symbol(Some("│"))
                .thumb_style(Style::new().fg(if focused { p.accent } else { p.gray }))
                .track_style(Style::new().fg(p.black)),
            body,
            &mut scrollbar,
        );
    }
}

/// Fills one body row: selection gutter, fitted content, row background, and
/// the frame-animated parts (spinner slot and pending-call right side).
///
/// Expanded payload rows keep their code background even under the cursor;
/// every other block row carries the selection or hover background.
fn assemble_row(
    buffer: &TranscriptBuffer,
    cache: &RenderCache,
    row: &Row,
    width: usize,
    focused: bool,
    ctx: &Ctx,
) -> Line<'static> {
    let p = theme::palette();
    let base = match row.block {
        Some(index) => {
            let style = message_base(buffer, cache, index);
            match row.keep {
                Some(color) => Style::new().bg(color),
                None => style,
            }
        }
        None => match row.keep {
            Some(color) => Style::new().bg(color),
            None => Style::new(),
        },
    };
    let bar_color = if focused { p.accent } else { p.gray };
    let selected = row
        .block
        .is_some_and(|index| block_holds(cache, index, buffer.cursor));
    let gutter = Span::styled(
        if selected { "▌ " } else { "  " },
        match base.bg {
            Some(color) => Style::new().fg(bar_color).bg(color),
            None => Style::new().fg(bar_color),
        },
    );
    let mut left = row.left.clone();
    if row.spins {
        // The system-row spinner slot animates without a rebuild.
        if let Some(first) = left.first_mut() {
            *first = Span::styled(format!("{} ", ctx.spinner), theme::dim());
        }
    }
    left.insert(0, gutter);
    let mut right = row.right.clone();
    match row.running_at {
        // A pending call: `running` with the live elapsed, or a dim
        // `no result` once the session ended.
        Some(started_at) if ctx.live => right.extend([
            Span::styled(
                format!("{} running  ", ctx.spinner),
                Style::new().fg(p.yellow),
            ),
            Span::styled(
                human_duration((ctx.now - started_at).max(0.0)),
                theme::dim(),
            ),
        ]),
        Some(_) => right.push(Span::styled("no result", theme::dim())),
        None => (),
    };
    Line::from(text::lr(left, right, width + 2, base))
}

/// Whether the message at `message` belongs to block `index` (its own span
/// or one of its paired results); an index lookup.
fn block_holds(cache: &RenderCache, index: usize, message: usize) -> bool {
    cache.grouping.block_of.get(message) == Some(&index)
}

/// The base style of one block: cursor beats hover beats plain.
fn message_base(buffer: &TranscriptBuffer, cache: &RenderCache, index: usize) -> Style {
    if block_holds(cache, index, buffer.cursor) {
        theme::selection()
    } else if buffer
        .hover
        .is_some_and(|hover| block_holds(cache, index, hover))
    {
        theme::hover()
    } else {
        Style::new()
    }
}

/// Formats nullable native measurements without inventing zero counts or
/// success. Complete lineage metrics print only when different from the latest
/// execution; the header's display-width fitter bounds the resulting dim row.
fn usage_line(agent: &agent_run_domain::AgentView) -> String {
    let mut parts = Vec::new();
    if let Some(usage) = &agent.usage {
        let number =
            |value: Option<i64>| value.map_or_else(|| "—".to_owned(), |value| value.to_string());
        parts.push(format!(
            "in {} · out {} · cache {}",
            number(usage.input_tokens),
            number(usage.output_tokens),
            number(usage.cache_read_tokens)
        ));
        parts.push(
            usage
                .cost_usd
                .map_or_else(|| "$—".to_owned(), |cost| format!("${cost:.2}")),
        );
        parts.push(format!("turns {}", number(usage.num_turns)));
    }
    if let Some(counts) = &agent.tool_counts {
        let number =
            |value: Option<u64>| value.map_or_else(|| "—".to_owned(), |value| value.to_string());
        parts.push(format!(
            "tools {} · {} failed · {} unknown",
            number(counts.calls),
            number(counts.failed),
            number(counts.unknown_results)
        ));
    }
    if let Some(total) = &agent.usage_cumulative {
        let differs = agent.usage.as_ref().is_none_or(|latest| {
            total
                .cost_usd
                .is_some_and(|value| Some(value) != latest.cost_usd)
                || total
                    .input_tokens
                    .is_some_and(|value| Some(value) != latest.input_tokens)
                || total
                    .output_tokens
                    .is_some_and(|value| Some(value) != latest.output_tokens)
                || total
                    .cache_read_tokens
                    .is_some_and(|value| Some(value) != latest.cache_read_tokens)
                || total
                    .num_turns
                    .is_some_and(|value| Some(value) != latest.num_turns)
                || total
                    .total_tokens
                    .is_some_and(|value| Some(value) != latest.total_tokens)
        });
        if differs {
            if let Some(cost) = total.cost_usd {
                parts.push(format!("Σ {} runs ${cost:.2}", total.executions));
            } else if let Some(tokens) = total.total_tokens {
                parts.push(format!("Σ {} runs {tokens} tokens", total.executions));
            } else if let Some(tokens) = total.input_tokens {
                parts.push(format!("Σ {} runs {tokens} in", total.executions));
            }
        }
    }
    parts.join(" · ")
}

/// Renders title, execution metadata, workdir/follow, and nullable native usage.
fn render_header(f: &mut Frame, app: &App, buffer: &TranscriptBuffer, split: bool, area: Rect) {
    let p = theme::palette();
    let agent = &buffer.agent;
    let width = area.width as usize;
    let plain = Style::new();
    let white = Style::new().fg(p.white);

    // Row 1: status glyph and task title.
    let title = text::lr(
        vec![
            Span::styled(
                crate::app::status_glyph(agent.status, app.spinner()).to_string(),
                Style::new().fg(theme::status_color(agent.status)),
            ),
            Span::raw(" "),
            Span::styled(
                crate::app::display_title(agent),
                Style::new().fg(p.bwhite).add_modifier(Modifier::BOLD),
            ),
        ],
        Vec::new(),
        width,
        plain,
    );
    // Row 2: badge, runtime/model, uptime, silence, failure — and the id.
    let (badge, badge_fg, badge_bg) = theme::status_badge(agent.status, app.spinner());
    let mut meta = vec![
        Span::styled(
            badge,
            Style::new()
                .fg(badge_fg)
                .bg(badge_bg)
                .add_modifier(Modifier::BOLD),
        ),
        Span::styled(
            format!("  {}/{}", agent.runtime.to_uppercase(), agent.model),
            white,
        ),
        Span::styled(
            format!("  up {}", human_duration(agent.elapsed_seconds)),
            theme::dim(),
        ),
    ];
    if let Some(silence) = agent.silence_seconds
        && !agent.status.terminal()
    {
        let warn = silence >= SILENCE_WARN_SECONDS;
        meta.push(Span::raw(" "));
        meta.push(Span::styled(
            format!("silence {}", human_duration(silence)),
            if warn { theme::warning() } else { theme::dim() },
        ));
    }
    if let Some(failure) = &agent.failure_text {
        meta.push(Span::raw(" "));
        meta.push(Span::styled(truncate_one(failure, 80), theme::failure()));
    }
    // Incompleteness is loud: a truncated history must never read as the
    // whole story, least of all under a `finished` badge.
    if let Some(error) = &buffer.last_page_error {
        meta.push(Span::raw(" "));
        meta.push(Span::styled(
            format!("broker error, retrying · {}", truncate_one(error, 64)),
            theme::failure(),
        ));
    } else if !buffer.blocks_view && !buffer.history_complete {
        meta.push(Span::raw(" "));
        meta.push(Span::styled(
            format!("loading {} messages…", buffer.messages.len()),
            theme::dim(),
        ));
    }
    let meta = text::lr(
        meta,
        vec![Span::styled(
            if split {
                agent.agent_id.as_str().to_string()
            } else {
                app::id_hash(agent.agent_id.as_str(), 10)
            },
            theme::dim(),
        )],
        width,
        plain,
    );
    // Row 3: workdir and the follow state.
    let workdir = crate::app::agent_workdir(agent)
        .map(|path| {
            Span::styled(
                shorten_home(&path, app.home_prefix.as_deref()),
                Style::new().fg(p.blue),
            )
        })
        .unwrap_or_else(|| Span::raw(""));
    let follow = if buffer.follow {
        vec![
            Span::styled("●", Style::new().fg(p.green)),
            Span::styled(" follow", white),
        ]
    } else {
        vec![Span::styled("○ scrolled", theme::dim())]
    };
    let follow_row = text::lr(vec![workdir], follow, width, plain);
    // Row 4: nullable native usage; absent statistics leave the row blank.
    let usage = text::fit(
        vec![Span::styled(usage_line(agent), theme::dim())],
        width,
        plain,
    );

    f.render_widget(
        Paragraph::new(vec![
            Line::from(title),
            Line::from(meta),
            Line::from(follow_row),
            Line::from(usage),
        ]),
        area,
    );
}

/// Shortens one path against the home prefix, `~` style.
fn shorten_home(path: &str, home: Option<&str>) -> String {
    match home {
        Some(home) if path.starts_with(home) => format!("~{}", &path[home.len()..]),
        _ => path.to_string(),
    }
}

/// Renders `at` (epoch seconds) as a stable UTC HH:MM:SS stamp.
fn chrono_stamp(at: f64) -> String {
    let secs = at.max(0.0) as u64;
    format!(
        "{:02}:{:02}:{:02}",
        (secs / 3600) % 24,
        (secs / 60) % 60,
        secs % 60
    )
}

/// Truncates to the first line and `max` characters with an ellipsis.
fn truncate_one(text: &str, max: usize) -> String {
    let one_line = text.lines().next().unwrap_or("");
    if one_line.chars().take(max.saturating_add(1)).count() <= max {
        return one_line.to_string();
    }
    let cut: String = one_line.chars().take(max.saturating_sub(1)).collect();
    format!("{cut}…")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tests_support::agent_view;
    use agent_run_domain::views::TranscriptPage;

    const STABLE: &str = "ag-20260928-101500-aaaaaaaaaa";
    const RUN: &str = "ag-20260928-101500-bbbbbbbbbb";

    /// One transcript page built from JSON message fixtures.
    fn page(messages: serde_json::Value) -> TranscriptPage {
        serde_json::from_value(serde_json::json!({
            "agent_id": STABLE, "run_id": RUN,
            "messages": messages,
            "cursor": 0, "limit": 500, "next_cursor": null, "complete": true,
        }))
        .unwrap()
    }

    /// A buffer holding one tool call with its result.
    fn buffer() -> TranscriptBuffer {
        let mut buffer = TranscriptBuffer::open(agent_view(STABLE, RUN, "running"));
        buffer.merge(page(serde_json::json!([
            {"seq": 1, "at": 100.0, "role": "tool_call", "name": "Bash",
             "content": "{\"command\":\"ls\"}", "raw_ref": "fixture-tool-1"},
            {"seq": 2, "at": 101.0, "role": "tool_result", "name": "Bash",
             "content": "ok", "raw_ref": "fixture-tool-1"},
        ])));
        buffer
    }

    /// One message fixture deserialized from JSON, so optional 0.20
    /// evidence fields can be exercised exactly.
    fn view(fields: serde_json::Value) -> MessageView {
        let mut base = serde_json::json!({
            "seq": 1, "at": 100.0, "role": "tool_result", "name": null,
            "content": "", "raw_ref": null,
        });
        if let (Some(target), Some(extra)) = (base.as_object_mut(), fields.as_object()) {
            for (key, value) in extra {
                target.insert(key.clone(), value.clone());
            }
        }
        serde_json::from_value(base).unwrap()
    }

    /// Failure dominates mixed chunks; success requires every chunk to report
    /// success, and failure-looking text never supplies native evidence.
    #[test]
    fn result_evidence_comes_only_from_native_flags() {
        let mut memo = ResultMemo::default();
        memo.push(&view(
            serde_json::json!({"content": "exit 101", "error": false}),
        ));
        assert_eq!(memo.error, Some(false));
        memo.push(&view(serde_json::json!({"seq": 2, "content": "fine"})));
        assert_eq!(memo.error, None, "unreported chunks cannot prove success");
        memo.push(&view(serde_json::json!({
            "seq": 3,
            "content": "fine",
            "error": true,
        })));
        assert_eq!(memo.error, Some(true), "any failure wins");

        // Failure-looking text alone never becomes error evidence.
        let mut heuristic = ResultMemo::default();
        heuristic.push(&view(serde_json::json!({
            "content": "error: exploded, FAILED, exit 101",
        })));
        assert_eq!(heuristic.error, None);
        assert!(!heuristic.truncated);

        let mut truncated = ResultMemo::default();
        truncated.push(&view(
            serde_json::json!({"content": "part", "content_complete": false}),
        ));
        assert!(truncated.truncated);
        assert_eq!(truncated.error, None);
    }

    #[test]
    fn tool_rows_color_and_mark_native_evidence() {
        let p = theme::palette();
        let mut buffer = TranscriptBuffer::open(agent_view(STABLE, RUN, "running"));
        buffer.merge(page(serde_json::json!([
            {"seq": 1, "at": 100.0, "role": "tool_call", "name": "Bash",
             "content": "{\"command\":\"ls\"}", "raw_ref": "call-1"},
            {"seq": 2, "at": 101.0, "role": "tool_result", "name": "Bash",
             "content": "error: boom", "raw_ref": "call-1", "error": true},
            {"seq": 3, "at": 102.0, "role": "tool_call", "name": "Read",
             "content": "{\"file_path\":\"a.rs\"}", "raw_ref": "call-2"},
            {"seq": 4, "at": 103.0, "role": "tool_result", "name": "Read",
             "content": "error: looking text", "raw_ref": "call-2"},
        ])));
        let guard = cache(&buffer, 80);
        // The flagged failure is red and carries `[error]`.
        let failed = &guard.blocks[0].rows[0];
        assert!(
            failed
                .left
                .iter()
                .any(|span| span.content.contains("Bash") && span.style.fg == Some(p.red)),
            "failed name is red"
        );
        assert!(
            failed
                .right
                .iter()
                .any(|span| span.content.starts_with("[error]") && span.style.fg == Some(p.red)),
            "failed summary is red with its evidence"
        );
        // The unreported failure-looking text stays neutral `[unknown]` and
        // is never shown as success.
        let unknown = &guard.blocks[1].rows[0];
        assert!(
            unknown
                .left
                .iter()
                .any(|span| span.content.contains("Read") && span.style.fg == Some(p.accent)),
            "unreported name stays neutral"
        );
        assert!(
            unknown
                .right
                .iter()
                .any(|span| span.content == "[unknown] "),
            "unreported evidence marker"
        );
    }

    #[test]
    fn truncated_results_show_explicit_markers() {
        let mut buffer = TranscriptBuffer::open(agent_view(STABLE, RUN, "running"));
        buffer.merge(page(serde_json::json!([
            {"seq": 1, "at": 100.0, "role": "tool_call", "name": "Bash",
             "content": "{\"command\":\"ls\"}", "raw_ref": "call-1"},
            {"seq": 2, "at": 101.0, "role": "tool_result", "name": "Bash",
             "content": "visible part", "raw_ref": "call-1",
             "content_complete": false},
        ])));
        let guard = cache(&buffer, 80);
        assert!(
            guard.blocks[0].rows[0]
                .right
                .iter()
                .any(|span| span.content.contains("… content truncated")),
            "collapsed summary names the truncation"
        );
        // Expanded: the payload listing ends with the marker row.
        drop(guard);
        buffer.toggle_expanded(1);
        let guard = cache(&buffer, 80);
        assert!(
            guard.blocks[0].rows.iter().any(|row| row
                .left
                .iter()
                .any(|span| span.content.contains("… content truncated"))),
            "expanded payload names the truncation"
        );
    }

    /// Long payload summaries leave native evidence and truncation visible
    /// within the assembled pane instead of dropping the entire right side.
    #[test]
    fn long_summaries_preserve_native_evidence_and_truncation() {
        let mut buffer = TranscriptBuffer::open(agent_view(STABLE, RUN, "running"));
        buffer.merge(page(serde_json::json!([
            {"seq":1,"at":100.0,"role":"tool_call","name":"Bash","content":"{}","raw_ref":"call"},
            {"seq":2,"at":101.0,"role":"tool_result","name":"Bash","content":"界".repeat(500),
             "raw_ref":"call","error":true,"content_complete":false}
        ])));
        let guard = cache(&buffer, 70);
        let ctx = Ctx {
            spinner: "⠋",
            live: true,
            now: 101.0,
        };
        let line = assemble_row(&buffer, &guard, &guard.blocks[0].rows[0], 70, true, &ctx);
        let rendered: String = line
            .spans
            .iter()
            .map(|span| span.content.as_ref())
            .collect();
        assert!(rendered.contains("Bash"), "{rendered}");
        assert!(rendered.contains("[error]"), "{rendered}");
        assert!(rendered.contains("… content truncated"), "{rendered}");
        assert_eq!(line.width(), 72);
    }

    /// Orphan results render all three native outcomes; failure colors both
    /// the tool name and payload summary, regardless of failure-looking text.
    #[test]
    fn orphan_results_show_native_outcomes() {
        let p = theme::palette();
        for (error, marker, color) in [
            (Some(true), "[error] ", p.red),
            (Some(false), "[ok] ", p.accent),
            (None, "[unknown] ", p.accent),
        ] {
            let mut buffer = TranscriptBuffer::open(agent_view(STABLE, RUN, "running"));
            buffer.merge(page(serde_json::json!([{
                "seq":1,"at":1.0,"role":"tool_result","name":"Bash",
                "content":"error: exit 101","raw_ref":"orphan","error":error
            }])));
            let guard = cache(&buffer, 80);
            let row = &guard.blocks[0].rows[0];
            assert!(
                row.left
                    .iter()
                    .any(|span| span.content == "Bash" && span.style.fg == Some(color))
            );
            assert!(row.right.iter().any(|span| span.content == marker));
            if error == Some(true) {
                assert!(
                    row.left
                        .iter()
                        .any(|span| span.content.contains("error: exit 101")
                            && span.style.fg == Some(p.red))
                );
                assert!(
                    row.right
                        .iter()
                        .filter(|span| !span.content.is_empty())
                        .all(|span| span.style.fg == Some(p.red))
                );
            }
        }
    }

    /// Truncated tool arguments remain explicit before a result arrives, in
    /// both the assembled summary and expanded code payload.
    #[test]
    fn truncated_calls_show_markers_while_pending() {
        let mut buffer = TranscriptBuffer::open(agent_view(STABLE, RUN, "running"));
        buffer.merge(page(serde_json::json!([{
            "seq":1,"at":100.0,"role":"tool_call","name":"Bash",
            "content":"partial args","raw_ref":"call","content_complete":false
        }])));
        let guard = cache(&buffer, 100);
        let ctx = Ctx {
            spinner: "⠋",
            live: true,
            now: 101.0,
        };
        let line = assemble_row(&buffer, &guard, &guard.blocks[0].rows[0], 100, true, &ctx);
        assert!(
            line.spans
                .iter()
                .any(|span| span.content.contains("… content truncated")
                    && span.style.fg == theme::dim().fg)
        );
        drop(guard);
        buffer.toggle_expanded(1);
        assert!(cache(&buffer, 100).blocks[0].rows.iter().any(|row| {
            row.left
                .iter()
                .any(|span| span.content == "… content truncated")
        }));
    }

    #[test]
    fn starts_block_opens_a_hard_boundary() {
        let mut buffer = TranscriptBuffer::open(agent_view(STABLE, RUN, "running"));
        buffer.merge(page(serde_json::json!([
            {"seq": 1, "at": 100.0, "role": "assistant", "name": null,
             "content": "one ", "raw_ref": "m-1"},
            {"seq": 2, "at": 100.5, "role": "assistant", "name": null,
             "content": "two", "raw_ref": "m-1", "starts_block": true},
            {"seq": 3, "at": 101.0, "role": "tool_call", "name": "Bash",
             "content": "{\"command\":\"ls\"}", "raw_ref": "call-1"},
            {"seq": 4, "at": 102.0, "role": "tool_result", "name": null,
             "content": "orphan", "raw_ref": null, "starts_block": true},
        ])));
        let guard = cache(&buffer, 80);
        let grouped = &guard.grouping.blocks;
        // The flagged assistant message does not extend the first run…
        assert_eq!(grouped.len(), 4, "four blocks: {grouped:?}");
        assert_eq!((grouped[0].start, grouped[0].len), (0, 1));
        assert_eq!((grouped[1].start, grouped[1].len), (1, 1));
        // …and the flagged ref-less result pairs to nothing, not by adjacency.
        assert!(grouped[2].results.is_empty(), "call stays pending");
        assert_eq!(grouped[3].start, 3, "the result opens its own block");
    }

    #[test]
    fn known_refs_must_match_to_join_text() {
        let mut buffer = TranscriptBuffer::open(agent_view(STABLE, RUN, "running"));
        buffer.merge(page(serde_json::json!([
            {"seq": 1, "at": 100.0, "role": "assistant", "name": null,
             "content": "first message", "raw_ref": "m-1"},
            {"seq": 2, "at": 101.0, "role": "assistant", "name": null,
             "content": "second message", "raw_ref": "m-2"},
            {"seq": 3, "at": 102.0, "role": "assistant", "name": null,
             "content": " tail", "raw_ref": null},
        ])));
        let guard = cache(&buffer, 80);
        let grouped = &guard.grouping.blocks;
        // Distinct native ids and unknown identities remain separate.
        assert_eq!(grouped.len(), 3, "three blocks: {grouped:?}");
        assert_eq!((grouped[0].start, grouped[0].len), (0, 1));
        assert_eq!((grouped[1].start, grouped[1].len), (1, 1));
    }

    /// Unknown, differently named and differently roled text never coalesces.
    #[test]
    fn text_grouping_requires_a_known_complete_identity() {
        let mut buffer = TranscriptBuffer::open(agent_view(STABLE, RUN, "running"));
        buffer.merge(page(serde_json::json!([
            {"seq":1,"at":1.0,"role":"assistant","name":null,"content":"a","raw_ref":null},
            {"seq":2,"at":2.0,"role":"assistant","name":null,"content":"b","raw_ref":null},
            {"seq":3,"at":3.0,"role":"assistant","name":null,"content":"c","raw_ref":"same"},
            {"seq":4,"at":4.0,"role":"assistant","name":"other","content":"d","raw_ref":"same"},
            {"seq":5,"at":5.0,"role":"user","name":"other","content":"e","raw_ref":"same"}
        ])));
        assert_eq!(blocks(&buffer).len(), 5);
    }

    /// Pairing requires a known native reference and equal tool name.
    #[test]
    fn uncorrelated_results_remain_orphans() {
        let mut buffer = TranscriptBuffer::open(agent_view(STABLE, RUN, "running"));
        buffer.merge(page(serde_json::json!([
            {"seq":1,"at":1.0,"role":"tool_call","name":"Read","content":"{}","raw_ref":"call"},
            {"seq":2,"at":2.0,"role":"tool_result","name":"Bash","content":"wrong name","raw_ref":"call"},
            {"seq":3,"at":3.0,"role":"tool_call","name":"Read","content":"{}","raw_ref":null},
            {"seq":4,"at":4.0,"role":"tool_result","name":"Read","content":"unknown","raw_ref":null}
        ])));
        let grouped = blocks(&buffer);
        assert_eq!(grouped.len(), 4);
        assert!(grouped.iter().all(|block| block.results.is_empty()));
    }

    /// Boundaries clear identities across pages; a reused call id belongs to
    /// its new call, including later chunks after a boundary on a result.
    #[test]
    fn tool_boundaries_clear_pending_calls_across_pages() {
        let fixtures = serde_json::json!([
            {"seq":1,"at":1.0,"role":"tool_call","name":"Read","content":"{}","raw_ref":"reused"},
            {"seq":2,"at":2.0,"role":"assistant","name":null,"content":"new execution","raw_ref":"text","starts_block":true},
            {"seq":3,"at":3.0,"role":"tool_result","name":"Read","content":"orphan","raw_ref":"reused"},
            {"seq":4,"at":4.0,"role":"tool_call","name":"Read","content":"{}","raw_ref":"reused"},
            {"seq":5,"at":5.0,"role":"tool_result","name":"Read","content":"paired","raw_ref":"reused"},
            {"seq":6,"at":6.0,"role":"tool_result","name":"Read","content":"boundary","raw_ref":"reused","starts_block":true},
            {"seq":7,"at":7.0,"role":"tool_result","name":"Read","content":"orphan chunk","raw_ref":"reused"}
        ]);
        let mut buffer = TranscriptBuffer::open(agent_view(STABLE, RUN, "running"));
        for fixture in fixtures.as_array().unwrap() {
            buffer.merge(page(serde_json::json!([fixture])));
            cache(&buffer, 80);
        }
        let grouped = blocks(&buffer);
        assert_eq!(grouped.len(), 6);
        assert!(grouped[0].results.is_empty());
        assert_eq!(grouped[3].results, vec![4]);
        assert_eq!(buffer.render_cache.borrow().grouping.blocks, grouped);
    }

    /// A late result breaks text adjacency even when it renders beside an
    /// earlier call; the following text cannot accidentally absorb its payload.
    #[test]
    fn late_tool_results_interrupt_text_coalescing() {
        let mut buffer = TranscriptBuffer::open(agent_view(STABLE, RUN, "running"));
        buffer.merge(page(serde_json::json!([
            {"seq":1,"at":1.0,"role":"tool_call","name":"Read","content":"{}","raw_ref":"call"},
            {"seq":2,"at":2.0,"role":"assistant","name":null,"content":"before","raw_ref":"text"},
            {"seq":3,"at":3.0,"role":"tool_result","name":"Read","content":"payload","raw_ref":"call"},
            {"seq":4,"at":4.0,"role":"assistant","name":null,"content":"after","raw_ref":"text"}
        ])));
        let grouped = blocks(&buffer);
        assert_eq!(grouped.len(), 3);
        assert_eq!(grouped[0].results, vec![2]);
        assert_eq!((grouped[1].start, grouped[1].len), (1, 1));
        assert_eq!((grouped[2].start, grouped[2].len), (3, 1));
    }

    /// Evidence-only replacements refresh memoized outcome and truncation rows.
    #[test]
    fn changed_evidence_invalidates_cached_rows() {
        let mut buffer = self::buffer();
        cache(&buffer, 80);
        let mut replacement = buffer.messages[1].clone();
        replacement.error = Some(true);
        replacement.error_source = Some("claude.is_error".into());
        replacement.content_complete = Some(false);
        assert!(buffer.merge(page(serde_json::json!([replacement]))));
        let guard = cache(&buffer, 80);
        assert_eq!(guard.blocks[0].results.error, Some(true));
        assert!(guard.blocks[0].results.truncated);
        drop(guard);
        let mut replacement = buffer.messages[1].clone();
        replacement.starts_block = Some(true);
        assert!(buffer.merge(page(serde_json::json!([replacement]))));
        assert_eq!(cache(&buffer, 80).grouping.blocks.len(), 2);
    }

    #[test]
    fn unchanged_state_reuses_every_block() {
        let mut buffer = buffer(); // one tool call with its result
        buffer.merge(page(serde_json::json!([
            {"seq": 3, "at": 102.0, "role": "assistant", "name": null,
             "content": "done", "raw_ref": null},
        ])));
        cache(&buffer, 80);
        let built = cache_block_builds(&buffer);
        assert_eq!(built, 2, "two blocks build once");

        // Reads at the same width reuse every memo; cursor, hover, follow,
        // and scroll are pure view state.
        total_height(&buffer, 80);
        cursor_line(&buffer, 80);
        assert!(message_at(&buffer, 80, 0).is_some());
        move_cursor_block(&mut buffer, 1);
        buffer.follow = false;
        buffer.from_bottom = 3;
        buffer.hover = Some(1);
        cache(&buffer, 80);
        assert_eq!(
            cache_block_builds(&buffer),
            built,
            "view state and repeated reads never rebuild"
        );

        // A different width rebuilds every block.
        cache(&buffer, 60);
        assert_eq!(cache_block_builds(&buffer), built + 2);
    }

    #[test]
    fn appended_delta_rebuilds_only_the_tail_block() {
        let mut buffer = buffer(); // [call, result]
        buffer.merge(page(serde_json::json!([
            {"seq": 3, "at": 102.0, "role": "assistant", "name": null,
             "content": "hello ", "raw_ref": "stream"},
        ])));
        cache(&buffer, 80);
        let built = cache_block_builds(&buffer);
        for step in 0..5 {
            buffer.merge(page(serde_json::json!([
                {"seq": 4 + step, "at": 103.0 + step as f64, "role": "assistant",
                 "name": null, "content": "more ", "raw_ref": "stream"},
            ])));
            cache(&buffer, 80);
        }
        assert_eq!(
            cache_block_builds(&buffer),
            built + 5,
            "each appended delta rebuilds exactly the tail text block"
        );
    }

    #[test]
    fn arriving_result_rebuilds_only_its_call_block() {
        // Two pending calls; the results arrive in reverse order.
        let mut buffer = TranscriptBuffer::open(agent_view(STABLE, RUN, "running"));
        buffer.merge(page(serde_json::json!([
            {"seq": 1, "at": 100.0, "role": "tool_call", "name": "Read",
             "content": "{\"file_path\":\"a.rs\"}", "raw_ref": "call-a"},
            {"seq": 2, "at": 101.0, "role": "tool_call", "name": "Read",
             "content": "{\"file_path\":\"b.rs\"}", "raw_ref": "call-b"},
        ])));
        cache(&buffer, 80);
        let built = cache_block_builds(&buffer);
        buffer.merge(page(serde_json::json!([
            {"seq": 3, "at": 102.0, "role": "tool_result", "name": "Read",
             "content": "b content", "raw_ref": "call-b"},
        ])));
        cache(&buffer, 80);
        assert_eq!(
            cache_block_builds(&buffer),
            built + 1,
            "only call-b's block rebuilds"
        );
        buffer.merge(page(serde_json::json!([
            {"seq": 4, "at": 103.0, "role": "tool_result", "name": "Read",
             "content": "a content", "raw_ref": "call-a"},
        ])));
        cache(&buffer, 80);
        assert_eq!(cache_block_builds(&buffer), built + 2);
    }

    #[test]
    fn toggle_rebuilds_one_block_and_view_refresh_none() {
        let mut buffer = buffer(); // [call+result, then text]
        buffer.merge(page(serde_json::json!([
            {"seq": 3, "at": 102.0, "role": "assistant", "name": null,
             "content": "done", "raw_ref": null},
        ])));
        cache(&buffer, 80);
        let built = cache_block_builds(&buffer);

        buffer.toggle_expanded_at_cursor(); // expand the tool block
        cache(&buffer, 80);
        assert_eq!(cache_block_builds(&buffer), built + 1, "one block rebuilds");

        // A refreshed session view is header-only data: no block rebuilds.
        let mut fresh = agent_view(STABLE, RUN, "running");
        fresh.silence_seconds = Some(300.0);
        fresh.elapsed_seconds = 400.0;
        buffer.agent = fresh;
        cache(&buffer, 80);
        assert_eq!(
            cache_block_builds(&buffer),
            built + 1,
            "AgentView refreshes never rebuild body rows"
        );

        buffer.toggle_expanded_at_cursor(); // collapse again
        cache(&buffer, 80);
        assert_eq!(cache_block_builds(&buffer), built + 2);
    }

    #[test]
    fn spinner_frames_and_clocks_substitute_without_rebuilding() {
        // A pending call row and a system row both animate at assembly time.
        let mut buffer = TranscriptBuffer::open(agent_view(STABLE, RUN, "running"));
        buffer.merge(page(serde_json::json!([
            {"seq": 1, "at": 100.0, "role": "tool_call", "name": "Bash",
             "content": "{\"command\":\"sleep 300\"}", "raw_ref": null},
            {"seq": 2, "at": 101.0, "role": "system", "name": null,
             "content": "thinking", "raw_ref": null},
        ])));
        cache(&buffer, 80);
        let built = cache_block_builds(&buffer);

        let row_text = |spinner: &'static str, now: f64| -> String {
            let guard = cache(&buffer, 80);
            let ctx = Ctx {
                spinner,
                live: true,
                now,
            };
            guard
                .blocks
                .iter()
                .flat_map(|cached| cached.rows.iter())
                .map(|row| assemble_row(&buffer, &guard, row, 80, true, &ctx))
                .flat_map(|line| line.spans.into_iter().map(|s| s.content))
                .collect()
        };
        let first = row_text("\u{280b}", 160.0);
        assert!(first.contains("running"), "{first}");
        assert!(first.contains("1m00s"), "60s of elapsed: {first}");
        let next = row_text("\u{2819}", 161.0);
        assert!(next.contains("1m01s"), "{next}");
        assert_eq!(
            cache_block_builds(&buffer),
            built,
            "frames and clocks animate without rebuilding any block"
        );
    }

    #[test]
    fn incremental_grouping_matches_a_full_regroup() {
        // One message per append: text runs across appends, a result pairing
        // into an old call by raw_ref, a ref-less adjacent result, an orphan
        // result, and a role switch.
        let fixtures = serde_json::json!([
            {"seq": 1, "at": 1.0, "role": "user", "name": null, "content": "go", "raw_ref": null},
            {"seq": 2, "at": 2.0, "role": "assistant", "name": null, "content": "a ", "raw_ref": "stream"},
            {"seq": 3, "at": 3.0, "role": "assistant", "name": null, "content": "b", "raw_ref": "stream"},
            {"seq": 4, "at": 4.0, "role": "tool_call", "name": "Read", "content": "{}", "raw_ref": "r1"},
            {"seq": 5, "at": 5.0, "role": "tool_call", "name": "Bash", "content": "{}", "raw_ref": "fixture-tool-2"},
            {"seq": 6, "at": 6.0, "role": "tool_result", "name": "Bash", "content": "ok", "raw_ref": "fixture-tool-2"},
            {"seq": 7, "at": 7.0, "role": "assistant", "name": null, "content": "c", "raw_ref": null},
            {"seq": 8, "at": 8.0, "role": "tool_result", "name": "Read", "content": "late", "raw_ref": "r1"},
            {"seq": 9, "at": 9.0, "role": "tool_result", "name": null, "content": "?", "raw_ref": "nope"},
            {"seq": 10, "at": 10.0, "role": "system", "name": null, "content": "note", "raw_ref": null},
        ]);
        let mut buffer = TranscriptBuffer::open(agent_view(STABLE, RUN, "running"));
        for message in fixtures.as_array().unwrap() {
            buffer.merge(page(serde_json::json!([message])));
            cache(&buffer, 80);
            let slot = buffer.render_cache.borrow();
            assert_eq!(slot.grouping.blocks, blocks(&buffer));
            assert_eq!(slot.grouping.block_of.len(), buffer.messages.len());
        }
        // The late r1 result joined the first call block; the unknown ref
        // opened its own orphan row.
        let grouped = blocks(&buffer);
        assert_eq!(grouped[2].results, vec![7], "late result pairs by raw_ref");
        assert_eq!(buffer.render_cache.borrow().grouping.block_of[7], 2);
        assert_eq!(grouped.iter().filter(|b| b.start == 8).count(), 1);

        // An in-place replacement moves the epoch: the grouping resets and
        // still matches a full regroup.
        buffer.merge(page(serde_json::json!([
            {"seq": 3, "at": 3.0, "role": "assistant", "name": null, "content": "B", "raw_ref": "stream"},
        ])));
        cache(&buffer, 80);
        assert_eq!(
            buffer.render_cache.borrow().grouping.blocks,
            blocks(&buffer)
        );
    }

    #[test]
    fn pending_tool_elapsed_reads_the_injected_clock() {
        // A live call without a result is its own newest message, so the
        // elapsed must come from the caller's clock, not the journal.
        let mut buffer = TranscriptBuffer::open(agent_view(STABLE, RUN, "running"));
        buffer.merge(page(serde_json::json!([
            {"seq": 1, "at": 100.0, "role": "tool_call", "name": "Bash",
             "content": "{\"command\":\"/bin/sleep 300\"}", "raw_ref": null},
        ])));
        let guard = cache(&buffer, 80);
        let ctx = Ctx {
            spinner: "\u{280b}",
            live: true,
            now: 160.0,
        };
        let line = assemble_row(&buffer, &guard, &guard.blocks[0].rows[0], 80, true, &ctx);
        let row: String = line.spans.into_iter().map(|s| s.content).collect();
        assert!(row.contains("running"), "{row}");
        assert!(row.contains("1m00s"), "60 seconds of elapsed: {row}");
    }
    /// 2000 updates consume only deltas and a bounded unfinished suffix.
    #[test]
    fn growing_text_and_collapsed_results_process_linear_bytes() {
        let mut buffer = TranscriptBuffer::open(agent_view(STABLE, RUN, "running"));
        let delta = "streamed words stay on the same block and wrap across deltas. ";
        let start = std::time::Instant::now();
        for seq in 1..=2000 {
            buffer.merge(page(serde_json::json!([{"seq":seq, "at":100.0, "role":"assistant", "name":null, "content":delta, "raw_ref":"stream"}])));
            cache(&buffer, 80);
        }
        let total = 2000 * delta.len();
        let guard = cache(&buffer, 80);
        let processed = guard.blocks[0].stream.as_ref().unwrap().processed;
        assert!(
            processed <= total * 5,
            "{processed} processed for {total} appended bytes"
        );
        eprintln!(
            "growing block: 2000 deltas, {total} bytes, {processed} processed, {:?}",
            start.elapsed()
        );
        drop(guard);
        let expected = text_rows(&buffer, &blocks(&buffer)[0], 80);
        let guard = cache(&buffer, 80);
        assert_eq!(guard.blocks[0].rows.len(), expected.len());
        for (actual, expected) in guard.blocks[0].rows.iter().zip(&expected) {
            assert_eq!(actual.left, expected.left);
        }
        drop(guard);
        let mut tool = self::buffer();
        cache(&tool, 80);
        for seq in 3..=2002 {
            tool.merge(page(serde_json::json!([{"seq":seq,"at":101.0,"role":"tool_result","name":"Bash","content":"more\n","raw_ref":"fixture-tool-1"}])));
            cache(&tool, 80);
        }
        let guard = cache(&tool, 80);
        assert_eq!(guard.blocks[0].rows.len(), 1);
        assert_eq!(guard.blocks[0].results.processed, 2 + 2000 * 5);
        assert_eq!(guard.blocks[0].results.consumed, 2001);
        drop(guard);
        let mut prompt = TranscriptBuffer::open(agent_view(STABLE, RUN, "running"));
        for seq in 1..=2000 {
            prompt.merge(page(serde_json::json!([{"seq":seq,"at":100.0,"role":"user","name":null,"content":delta,"raw_ref":"prompt"}])));
            cache(&prompt, 80);
        }
        let guard = cache(&prompt, 80);
        assert_eq!(guard.blocks[0].rows.len(), USER_COLLAPSE_LINES + 2);
        assert!(guard.blocks[0].stream.as_ref().unwrap().processed <= total * 5);
        drop(guard);
        prompt.toggle_expanded(1);
        let expected = text_rows(&prompt, &blocks(&prompt)[0], 80);
        let guard = cache(&prompt, 80);
        assert_eq!(guard.blocks[0].rows.len(), expected.len());
        for (actual, expected) in guard.blocks[0].rows.iter().zip(&expected) {
            assert_eq!(actual.left, expected.left);
        }
    }

    /// Every possible split of CSI/OSC text produces the CLI's sanitized rows.
    #[test]
    fn streamed_escapes_and_partial_words_match_a_whole_block() {
        let raw = "hello \x1b[31mworld\x1b[0m \x1b]title hidden\x1b\\ words\nnext paragraph";
        for split in 0..=raw.len() {
            let mut buffer = TranscriptBuffer::open(agent_view(STABLE, RUN, "running"));
            for (seq, delta) in [(1, &raw[..split]), (2, &raw[split..])] {
                buffer.merge(page(serde_json::json!([{"seq":seq,"at":100.0,"role":"assistant","name":null,"content":delta,"raw_ref":"stream"}])));
                cache(&buffer, 25);
            }
            let expected = text_rows(&buffer, &blocks(&buffer)[0], 25);
            let guard = cache(&buffer, 25);
            assert_eq!(guard.blocks[0].rows.len(), expected.len(), "split {split}");
            for (actual, expected) in guard.blocks[0].rows.iter().zip(&expected) {
                assert_eq!(actual.left, expected.left, "split {split}");
            }
        }
    }

    /// Expanded huge arguments stay fully accessible as bounded cached rows.
    #[test]
    fn huge_arguments_are_wrapped_once_into_bounded_spans() {
        let mut buffer = TranscriptBuffer::open(agent_view(STABLE, RUN, "running"));
        let argument = "x".repeat(1024 * 1024);
        buffer.merge(page(serde_json::json!([{"seq":1,"at":100.0,"role":"tool_call","name":"Bash","content":serde_json::json!({"command":argument}).to_string(),"raw_ref":null}])));
        cache(&buffer, 80);
        buffer.toggle_expanded(1);
        let guard = cache(&buffer, 80);
        assert!(
            guard.blocks[0]
                .rows
                .iter()
                .flat_map(|row| &row.left)
                .all(|span| span.content.len() <= 512)
        );
        let full: usize = guard.blocks[0]
            .rows
            .iter()
            .skip(1)
            .flat_map(|row| &row.left)
            .map(|span| span.content.chars().filter(|c| *c == 'x').count())
            .sum();
        assert_eq!(full, argument.len());
    }

    /// Result summaries count split lines exactly; evidence never comes
    /// from the content text, so failure-looking fixtures stay unknown.
    #[test]
    fn result_summary_chunks_keep_lines_and_evidence_native() {
        for content in [
            "",
            "\n",
            "one\ntwo\n",
            "\n first line \nsecond",
            "exit 0",
            "exit \n",
            "exit 101",
            "tool FAILED",
            "  error: broken",
        ] {
            for split in 0..=content.len() {
                let mut memo = ResultMemo::default();
                for part in [&content[..split], &content[split..]] {
                    memo.push(&crate::tests_support::message(1, "tool_result", part));
                }
                let first = content
                    .lines()
                    .map(str::trim)
                    .find(|line| !line.is_empty())
                    .unwrap_or("");
                let count = content.lines().count();
                let expected = if count > 1 {
                    format!("{} ({} lines)", truncate_one(first, 56), count)
                } else {
                    truncate_one(first, 64)
                };
                assert_eq!(memo.summary(), expected);
                assert_eq!(
                    memo.error, None,
                    "no native flag means unknown: {content:?}, split {split}"
                );
            }
        }
    }

    /// Edge fragments extend one text block and consume only new bytes;
    /// replaying the forward page never duplicates visible content.
    #[test]
    fn partial_tail_extends_without_duplicate_text() {
        use crate::tests_support::{block_message, block_page};
        let mut buffer = TranscriptBuffer::open(agent_view(STABLE, RUN, "running"));
        let mut tail = block_message(10, 20, "assistant", "middle ", "stream");
        tail.partial_before = Some(true);
        tail.starts_block = Some(false);
        buffer.merge(block_page(vec![tail], None, Some(10), 20, true));
        cache(&buffer, 80);
        let first_bytes = buffer.render_cache.borrow().blocks[0]
            .stream
            .as_ref()
            .unwrap()
            .processed;
        let mut follow = block_message(21, 30, "assistant", "newest", "stream");
        follow.partial_before = Some(true);
        follow.starts_block = Some(false);
        let follow = block_page(vec![follow], None, None, 30, false);
        buffer.merge(follow.clone());
        cache(&buffer, 80);
        let built = cache_block_builds(&buffer);
        assert!(!buffer.merge(follow));
        let guard = cache(&buffer, 80);
        assert_eq!(guard.grouping.blocks.len(), 1);
        assert_eq!(guard.blocks[0].stream.as_ref().unwrap().consumed, 2);
        assert!(guard.blocks[0].stream.as_ref().unwrap().processed - first_bytes < 100);
        let visible: String = guard.blocks[0]
            .rows
            .iter()
            .flat_map(|row| &row.left)
            .map(|span| span.content.as_ref())
            .collect();
        assert_eq!(visible.matches("middle newest").count(), 1, "{visible}");
        assert!(visible.contains("partial block"));
        assert_eq!(guard.builds, built);
        drop(guard);
        let older = block_message(1, 9, "assistant", "beginning ", "stream");
        buffer.merge(block_page(vec![older], Some(10), None, 9, true));
        let guard = cache(&buffer, 80);
        let visible: String = guard.blocks[0]
            .rows
            .iter()
            .flat_map(|row| &row.left)
            .map(|span| span.content.as_ref())
            .collect();
        assert!(visible.contains("beginning middle newest"), "{visible}");
        assert!(!visible.contains("partial block"), "{visible}");
        assert_eq!(guard.grouping.blocks.len(), 1);
        assert!(buffer.history_complete);
        assert_eq!(buffer.resume_cursor, 30);
    }

    /// Server block boundaries still pair call/result identities, including
    /// split arguments and results that initially arrived without their call.
    #[test]
    fn projected_tool_edges_pair_and_extend_without_duplicate_rows() {
        use crate::tests_support::{block_message, block_page};
        let mut buffer = TranscriptBuffer::open(agent_view(STABLE, RUN, "running"));
        let mut result = block_message(20, 30, "tool_result", "middle ", "tool");
        result.name = Some("Bash".to_owned());
        result.error = Some(false);
        result.partial_before = Some(true);
        result.starts_block = Some(false);
        buffer.merge(block_page(vec![result], None, Some(20), 30, true));
        cache(&buffer, 80);
        let mut next = block_message(31, 40, "tool_result", "tail", "tool");
        next.name = Some("Bash".to_owned());
        next.error = Some(false);
        next.partial_before = Some(true);
        next.starts_block = Some(false);
        buffer.merge(block_page(vec![next], None, None, 40, false));
        assert_eq!(blocks(&buffer).len(), 1);
        let mut call = block_message(1, 10, "tool_call", "{\"command\":\"echo ", "tool");
        call.name = Some("Bash".to_owned());
        call.partial_after = Some(true);
        let mut suffix = block_message(11, 18, "tool_call", "hi\"}", "tool");
        suffix.name = Some("Bash".to_owned());
        suffix.partial_before = Some(true);
        suffix.starts_block = Some(false);
        let mut result_prefix = block_message(19, 19, "tool_result", "head ", "tool");
        result_prefix.name = Some("Bash".to_owned());
        result_prefix.error = Some(false);
        result_prefix.partial_after = Some(true);
        buffer.merge(block_page(
            vec![call, suffix, result_prefix],
            Some(20),
            None,
            19,
            true,
        ));
        buffer.toggle_expanded(10);
        let guard = cache(&buffer, 80);
        assert_eq!(
            guard.grouping.blocks.len(),
            1,
            "orphan results now pair to the prepended call"
        );
        let visible: String = guard.blocks[0]
            .rows
            .iter()
            .flat_map(|row| row.left.iter().chain(&row.right))
            .map(|span| span.content.as_ref())
            .collect();
        assert!(visible.contains("echo hi"), "{visible}");
        assert!(visible.contains("middle tail"), "{visible}");
        assert!(visible.contains("[ok]"), "{visible}");
        assert!(!visible.contains("partial block"), "{visible}");
    }

    /// Overlapping replay ranges replace smaller fragments, while omitted
    /// content stays explicitly truncated instead of triggering a body fetch.
    #[test]
    fn replayed_range_replaces_partial_and_keeps_truncation() {
        use crate::tests_support::{block_message, block_page};
        let mut buffer = TranscriptBuffer::open(agent_view(STABLE, RUN, "running"));
        buffer.merge(block_page(
            vec![block_message(10, 20, "assistant", "suffix", "stream")],
            None,
            Some(10),
            20,
            true,
        ));
        cache(&buffer, 80);
        let mut full = block_message(1, 30, "assistant", "full body", "stream");
        full.content_complete = Some(false);
        full.omitted_bytes = Some(400);
        buffer.merge(block_page(vec![full], None, None, 30, false));
        assert_eq!(buffer.messages.len(), 1);
        let guard = cache(&buffer, 80);
        let visible: String = guard.blocks[0]
            .rows
            .iter()
            .flat_map(|row| &row.left)
            .map(|span| span.content.as_ref())
            .collect();
        assert!(visible.contains("full body"), "{visible}");
        assert!(!visible.contains("suffix"), "{visible}");
        assert!(visible.contains("content truncated"), "{visible}");
    }

    /// Native usage is compact, width-fitted and dim; absent fields stay
    /// unknown, and equal/absent lineage measurements never invent totals.
    #[test]
    fn usage_and_tools_preserve_nullable_native_measurements() {
        let mut agent = agent_view(STABLE, RUN, "running");
        assert_eq!(usage_line(&agent), "");
        agent.usage = Some(
            serde_json::from_value(serde_json::json!({
                "input_tokens":1234,"output_tokens":56,"cache_read_tokens":1200,
                "cache_write_tokens":null,"reasoning_tokens":null,"total_tokens":1290,
                "num_turns":4,"ttft_ms":null,"api_duration_ms":null,"cost_usd":0.25,
                "usage_source":"runtime_result","recorded_at":100.0
            }))
            .unwrap(),
        );
        agent.tool_counts = Some(agent_run_domain::views::ToolCountsView {
            calls: Some(41),
            failed: Some(2),
            unknown_results: Some(3),
        });
        agent.usage_cumulative = Some(
            serde_json::from_value(serde_json::json!({
                "input_tokens":4000,"output_tokens":120,"cache_read_tokens":3000,
                "cache_write_tokens":null,"reasoning_tokens":null,"total_tokens":4120,
                "num_turns":10,"cost_usd":1.24,"executions":3
            }))
            .unwrap(),
        );
        let line = usage_line(&agent);
        assert_eq!(
            line,
            "in 1234 · out 56 · cache 1200 · $0.25 · turns 4 · tools 41 · 2 failed · 3 unknown · Σ 3 runs $1.24"
        );
        let mut app = App::new();
        app.screen = crate::app::Screen::Transcript;
        app.transcript = Some(TranscriptBuffer::open(agent.clone()));
        let mut terminal =
            ratatui::Terminal::new(ratatui::backend::TestBackend::new(120, 16)).unwrap();
        terminal
            .draw(|frame| render(frame, &app, frame.area(), false))
            .unwrap();
        let row: String = (0..120)
            .map(|x| terminal.backend().buffer()[(x, 3)].symbol())
            .collect();
        assert!(row.contains("tools 41 · 2 failed · 3 unknown"), "{row}");
        assert_eq!(
            terminal.backend().buffer()[(0, 3)].fg,
            theme::palette().gray
        );
        agent.usage.as_mut().unwrap().input_tokens = None;
        agent.usage.as_mut().unwrap().output_tokens = None;
        agent.usage.as_mut().unwrap().cache_read_tokens = None;
        agent.usage.as_mut().unwrap().num_turns = None;
        agent.usage.as_mut().unwrap().cost_usd = None;
        agent.tool_counts = Some(agent_run_domain::views::ToolCountsView::default());
        agent.usage_cumulative = None;
        assert_eq!(
            usage_line(&agent),
            "in — · out — · cache — · $— · turns — · tools — · — failed · — unknown"
        );
        agent.usage_cumulative = Some(
            serde_json::from_value(serde_json::json!({
                "input_tokens":null,"output_tokens":null,"cache_read_tokens":null,
                "cache_write_tokens":null,"reasoning_tokens":null,"total_tokens":null,
                "num_turns":null,"cost_usd":null,"executions":3
            }))
            .unwrap(),
        );
        assert!(!usage_line(&agent).contains("Σ"));
    }

    /// Reverse complete cannot close a finished transcript before its forward
    /// catch-up; top history markers remain distinct from terminal evidence.
    #[test]
    fn reverse_complete_waits_for_forward_tail_before_outcome() {
        use crate::tests_support::{block_message, block_page};
        let mut buffer = TranscriptBuffer::open(agent_view(STABLE, RUN, "succeeded"));
        buffer.merge(block_page(
            vec![block_message(1, 10, "assistant", "answer", "stream")],
            None,
            None,
            10,
            true,
        ));
        assert!(buffer.history_complete);
        assert_eq!(tail_height(&buffer), 1);
        buffer.merge(block_page(vec![], None, None, 10, false));
        assert_eq!(tail_height(&buffer), 2);
        let mut app = App::new();
        app.transcript = Some(buffer);
        let mut terminal =
            ratatui::Terminal::new(ratatui::backend::TestBackend::new(100, 18)).unwrap();
        terminal
            .draw(|frame| render(frame, &app, frame.area(), false))
            .unwrap();
        let screen: String = terminal
            .backend()
            .buffer()
            .content()
            .iter()
            .map(|cell| cell.symbol())
            .collect();
        assert!(screen.contains("beginning of transcript"), "{screen}");
        assert!(screen.contains("finished in"), "{screen}");
    }
}
