//! The transcript pane: header summary plus collapsible tool activity.
//!
//! Engines stream assistant text in small deltas, and the supervisor journals
//! every delta as its own message (`agent-run-core/src/stream.rs`), so the
//! viewer coalesces runs of same-identity text messages into one flowing
//! block — the same rule the CLI transcript viewer applies; tool activity
//! breaks the stream. Tool calls and results render as one collapsed summary
//! line each; the operator expands them (Enter or click) into the full
//! pretty-printed payload.
//!
//! Line wrapping is delegated to the same ratatui `WordWrapper` engine that
//! renders the pane: layout math asks `Paragraph::line_count` per block, so
//! scroll offsets and click mapping can never drift from what is on screen.

use super::theme;
use crate::app::{status_label, status_pictogram, App, TranscriptBuffer};
use agent_run_domain::views::MessageView;
use ratatui::{
    layout::Rect,
    style::Style,
    text::{Line, Span},
    widgets::{Block, Borders, Paragraph, Scrollbar, ScrollbarOrientation, ScrollbarState, Wrap},
    Frame,
};

/// A coalesced run of same-identity streaming text, one tool call with its
/// results, or an orphan tool result.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StreamBlock {
    /// First message index of the block.
    pub start: usize,
    /// Message count in the block.
    pub len: usize,
    /// Whether the block renders as flowing text (as opposed to one tool call).
    pub text: bool,
}

impl StreamBlock {
    /// Whether one message index falls inside this block.
    fn contains(&self, index: usize) -> bool {
        index >= self.start && index < self.start + self.len
    }
}

/// Whether one role renders as coalesced flowing text.
fn is_text_role(role: &str) -> bool {
    matches!(role, "assistant" | "user" | "system")
}

/// Groups messages into render blocks.
///
/// Only consecutive known raw refs with matching role/name/evidence join;
/// safe server boundary flags separate executions and attempts without IDs.
/// Unknown identities and unrelated calls/results remain separate. Rendering
/// separators are never inserted into the retained content itself.
pub fn blocks(buffer: &TranscriptBuffer) -> Vec<StreamBlock> {
    let mut blocks: Vec<StreamBlock> = Vec::new();
    for (index, message) in buffer.messages.iter().enumerate() {
        if is_text_role(&message.role) {
            if let Some(last) = blocks.last_mut() {
                let same_identity = last.text && {
                    let first = &buffer.messages[last.start];
                    first.role == message.role
                        && first.name == message.name
                        && first.raw_ref.is_some()
                        && first.raw_ref == message.raw_ref
                        && message.starts_block != Some(true)
                };
                if same_identity {
                    last.len += 1;
                    continue;
                }
            }
            blocks.push(StreamBlock {
                start: index,
                len: 1,
                text: true,
            });
        } else {
            // tool_result joins the tool group opened by the last tool_call
            // when both carry the same native call id and tool name; a
            // tool_call (or an orphan or uncorrelated result) always opens a
            // fresh group. Roles differ by design inside one invocation.
            let joins = match blocks.last_mut() {
                Some(last) => {
                    let first = &buffer.messages[last.start];
                    !last.text
                        && first.role == "tool_call"
                        && message.role == "tool_result"
                        && first.name == message.name
                        && first.raw_ref.is_some()
                        && first.raw_ref == message.raw_ref
                        && message.starts_block != Some(true)
                }
                None => false,
            };
            if joins {
                blocks.last_mut().expect("checked above").len += 1;
            } else {
                blocks.push(StreamBlock {
                    start: index,
                    len: 1,
                    text: false,
                });
            }
        }
    }
    blocks
}

/// Rendered extent of one block: where its lines start and how many follow.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MessageLayout {
    /// First message index of the block.
    pub message: usize,
    /// First rendered body line of the block.
    pub line_start: usize,
    /// Rendered body lines including the trailing separator.
    pub line_count: usize,
}

/// The wrapped line count of one block at one body width.
fn block_height(
    buffer: &TranscriptBuffer,
    blocks: &[StreamBlock],
    index: usize,
    width: u16,
) -> usize {
    Paragraph::new(block_lines(buffer, blocks, index))
        .wrap(Wrap { trim: false })
        .line_count(width)
}

/// Computes the body layout for one width; shared by rendering and click mapping.
///
/// One blank separator row sits between neighboring blocks, so block `i`
/// starts at the sum of the previous heights plus `i` separator rows.
pub fn layout(buffer: &TranscriptBuffer, width: u16) -> Vec<MessageLayout> {
    let blocks = blocks(buffer);
    let mut layouts = Vec::with_capacity(blocks.len());
    let mut line_start = 0usize;
    for (index, _) in blocks.iter().enumerate() {
        if index > 0 {
            line_start += 1; // separator row between blocks
        }
        let count = block_height(buffer, &blocks, index, width);
        layouts.push(MessageLayout {
            message: blocks[index].start,
            line_start,
            line_count: count,
        });
        line_start += count;
    }
    layouts
}

/// Total rendered body height for one width, separators included.
pub fn total_height(buffer: &TranscriptBuffer, width: u16) -> usize {
    if buffer.messages.is_empty() {
        return 1;
    }
    let layouts = layout(buffer, width);
    match layouts.last() {
        Some(last) => last.line_start + last.line_count,
        None => 0,
    }
}

/// The block containing one message index.
fn block_of(blocks: &[StreamBlock], index: usize) -> usize {
    blocks
        .iter()
        .position(|b| b.contains(index))
        .unwrap_or_else(|| blocks.len().saturating_sub(1))
}

/// The body line index of the message cursor, if any message is loaded.
pub fn cursor_line(buffer: &TranscriptBuffer, width: u16) -> Option<usize> {
    let blocks = blocks(buffer);
    let current = block_of(&blocks, buffer.cursor);
    layout(buffer, width).get(current).map(|l| l.line_start)
}

/// Moves the cursor one block up or down (positive toward the tail).
pub fn move_cursor_block(buffer: &mut TranscriptBuffer, delta: i64) {
    let blocks = blocks(buffer);
    if blocks.is_empty() {
        buffer.cursor = 0;
        return;
    }
    let current = block_of(&blocks, buffer.cursor);
    let target = (current as i64 + delta).clamp(0, blocks.len() as i64 - 1) as usize;
    buffer.cursor = blocks[target].start;
}

/// The first message of the block under one body line, if any.
pub fn message_at(buffer: &TranscriptBuffer, width: u16, line: usize) -> Option<usize> {
    layout(buffer, width)
        .iter()
        .find(|l| line >= l.line_start && line < l.line_start + l.line_count)
        .map(|l| l.message)
}

/// Builds the logical lines of one block according to its role and expansion.
fn block_lines(
    buffer: &TranscriptBuffer,
    blocks: &[StreamBlock],
    index: usize,
) -> Vec<Line<'static>> {
    let block = &blocks[index];
    let message = &buffer.messages[block.start];
    match message.role.as_str() {
        "tool_call" | "tool_result" => tool_lines(buffer, blocks, index),
        _ => text_lines(buffer, blocks, index, block),
    }
}

/// The base style of one block: cursor beats hover beats plain.
fn message_base(buffer: &TranscriptBuffer, blocks: &[StreamBlock], index: usize) -> Style {
    if blocks[index].contains(buffer.cursor) {
        theme::selection()
    } else if buffer
        .hover
        .is_some_and(|hover| blocks[index].contains(hover))
    {
        theme::hover()
    } else {
        Style::new()
    }
}

/// Renders one tool group block: the call plus each of its results, one
/// collapsed summary line per message, each independently expandable.
///
/// Blocks carry no trailing blank row; the renderer puts one separator row
/// between neighboring blocks, so a call and its results stay compact.
fn tool_lines(
    buffer: &TranscriptBuffer,
    blocks: &[StreamBlock],
    index: usize,
) -> Vec<Line<'static>> {
    let base = message_base(buffer, blocks, index);
    let block = &blocks[index];
    let mut lines = Vec::new();
    for message in &buffer.messages[block.start..block.start + block.len] {
        let is_call = message.role == "tool_call";
        if !buffer.is_expanded(message.seq) {
            let (marker, name, summary) = tool_summary(message, is_call);
            lines.push(
                Line::from(vec![
                    Span::styled(marker, Style::new().fg(theme::role_color(&message.role))),
                    Span::styled(format!(" {name}"), Style::new().bold()),
                    Span::styled(format!(" — {summary}"), theme::dim()),
                ])
                .style(base),
            );
        } else {
            lines.push(
                Line::from(vec![
                    Span::styled("▾", Style::new().fg(theme::role_color(&message.role))),
                    Span::styled(
                        format!(" {} expanded (Enter collapses)", tool_name(message)),
                        Style::new().bold(),
                    ),
                ])
                .style(base),
            );
            for raw in payload_lines(message) {
                lines.push(Line::from(Span::styled(raw, theme::dim())).style(base));
            }
        }
    }
    lines
}

/// Renders one coalesced text block (user, assistant, system) with a role tag.
///
/// The fragments of a streamed message concatenate exactly as they arrived —
/// the CLI viewer streams them the same way — and the whole block is
/// sanitized once, so control sequences split across delta boundaries are
/// still stripped.
fn text_lines(
    buffer: &TranscriptBuffer,
    blocks: &[StreamBlock],
    index: usize,
    block: &StreamBlock,
) -> Vec<Line<'static>> {
    let base = message_base(buffer, blocks, index);
    let first = &buffer.messages[block.start];
    let stamp = chrono_stamp(first.at);
    let role_tag = match &first.name {
        Some(name) => format!("{}:{name}", first.role),
        None => first.role.clone(),
    };
    let mut raw = String::new();
    for message in &buffer.messages[block.start..block.start + block.len] {
        raw.push_str(&message.content);
    }
    let content = agent_run::transcript::sanitize(&raw);
    let mut lines = Vec::new();
    for (line_index, raw_line) in content.lines().enumerate() {
        if line_index == 0 {
            lines.push(
                Line::from(vec![
                    Span::styled(stamp.clone(), theme::dim()),
                    Span::raw(" "),
                    Span::styled(
                        role_tag.clone(),
                        Style::new().fg(theme::role_color(&first.role)),
                    ),
                    Span::raw(" "),
                    Span::raw(raw_line.to_string()),
                ])
                .style(base),
            );
        } else {
            // Continuation lines keep the tag column visually open.
            lines.push(
                Line::from(vec![
                    Span::styled(" ".repeat(stamp.chars().count() + 1), theme::dim()),
                    Span::raw(raw_line.to_string()),
                ])
                .style(base),
            );
        }
    }
    lines
}

/// One-line summary of a tool message: marker, tool name, and brief action.
fn tool_summary(message: &MessageView, is_call: bool) -> (String, String, String) {
    let name = tool_name(message);
    let marker = if is_call { "▸ ⚙" } else { "▾ ↳" };
    let summary = if is_call {
        call_summary(&message.content)
    } else {
        let evidence = match message.error {
            Some(true) => "error",
            Some(false) => "ok",
            None => "unknown",
        };
        format!("[{evidence}] {}", result_summary(&message.content))
    };
    (marker.to_string(), name, summary)
}

/// The tool display name (role-specific `name`, falling back to the role).
fn tool_name(message: &MessageView) -> String {
    message.name.clone().unwrap_or_else(|| message.role.clone())
}

/// Brief description of a tool call: `description`, then known argument keys.
fn call_summary(content: &str) -> String {
    let parsed: Option<serde_json::Value> = serde_json::from_str(content).ok();
    let candidate = parsed
        .as_ref()
        .and_then(|v| v.get("description"))
        .or_else(|| parsed.as_ref().and_then(|v| v.get("command")))
        .or_else(|| parsed.as_ref().and_then(|v| v.get("cmd")))
        .or_else(|| parsed.as_ref().and_then(|v| v.get("file_path")))
        .or_else(|| parsed.as_ref().and_then(|v| v.get("prompt")));
    match candidate.and_then(|v| v.as_str()) {
        Some(text) => truncate_one(text, 64),
        None => truncate_one(content, 64),
    }
}

/// Brief description of a tool result: its first meaningful line and size.
fn result_summary(content: &str) -> String {
    let first = content
        .lines()
        .map(str::trim)
        .find(|line| !line.is_empty())
        .unwrap_or("");
    let total = content.lines().count();
    if total > 1 {
        format!("{} ({} lines)", truncate_one(first, 56), total)
    } else {
        truncate_one(first, 64)
    }
}

/// The full payload lines of a tool message, pretty-printed when JSON.
fn payload_lines(message: &MessageView) -> Vec<String> {
    let content = agent_run::transcript::sanitize(&message.content);
    match serde_json::from_str::<serde_json::Value>(&content) {
        Ok(value) => serde_json::to_string_pretty(&value)
            .unwrap_or(content)
            .lines()
            .map(str::to_string)
            .collect(),
        Err(_) => content.lines().map(str::to_string).collect(),
    }
}

/// Renders the transcript screen into the given area.
pub fn render(f: &mut Frame, app: &App, area: Rect) {
    let Some(buffer) = app.transcript.as_ref() else {
        return;
    };
    let agent = &buffer.agent;

    let title = format!(
        " transcript {} {}{} ",
        agent.agent_id.as_str(),
        if buffer.history_complete {
            ""
        } else {
            "⏳ backfilling "
        },
        if buffer.follow {
            "following"
        } else {
            "scrolled"
        },
    );
    let block = Block::new()
        .title(Line::from(title).style(theme::accent()))
        .borders(Borders::ALL);
    let inner = block.inner(area);
    f.render_widget(block, area);

    if inner.height == 0 || inner.width == 0 {
        return;
    }
    let header_height = 1u16.min(inner.height);
    let (header_area, body_area) = (
        Rect {
            height: header_height,
            ..inner
        },
        Rect {
            y: inner.y + header_height,
            height: inner.height.saturating_sub(header_height),
            ..inner
        },
    );

    let mut header_line = vec![
        Span::styled(
            format!(
                "{} {}",
                status_pictogram(agent.status),
                status_label(agent.status)
            ),
            theme::status_style(agent.status),
        ),
        Span::raw(" "),
        Span::styled(
            format!("{} · {}", agent.runtime, agent.model),
            Style::new().bold(),
        ),
        Span::raw(" "),
        Span::styled(
            format!("up {}", crate::app::human_duration(agent.elapsed_seconds)),
            theme::dim(),
        ),
    ];
    if let Some(silence) = agent.silence_seconds {
        if !agent.status.terminal() {
            let warn = silence >= crate::app::SILENCE_WARN_SECONDS;
            header_line.push(Span::raw(" "));
            header_line.push(Span::styled(
                format!("silent {}", crate::app::human_duration(silence)),
                if warn { theme::warning() } else { theme::dim() },
            ));
        }
    }
    if let Some(failure) = &agent.failure_text {
        header_line.push(Span::raw(" "));
        header_line.push(Span::styled(truncate_one(failure, 80), theme::failure()));
    }
    f.render_widget(Paragraph::new(Line::from(header_line)), header_area);

    let width = body_area.width;
    let mut lines: Vec<Line> = Vec::new();
    if buffer.messages.is_empty() {
        lines.push(Line::from(Span::styled(
            if buffer.history_complete {
                "No transcript messages yet."
            } else {
                "Loading transcript…"
            },
            theme::dim(),
        )));
    } else {
        let blocks = blocks(buffer);
        for (index, _) in blocks.iter().enumerate() {
            if index > 0 {
                lines.push(Line::from(""));
            }
            lines.extend(block_lines(buffer, &blocks, index));
        }
    }

    let total = total_height(buffer, width) as u16;
    let offset = if buffer.follow {
        total.saturating_sub(body_area.height)
    } else {
        total
            .saturating_sub(body_area.height)
            .saturating_sub(buffer.from_bottom)
    };
    let paragraph = Paragraph::new(lines)
        .wrap(Wrap { trim: false })
        .scroll((offset, 0));
    f.render_widget(paragraph, body_area);

    if total > body_area.height {
        let mut scrollbar_state = ScrollbarState::new(total as usize)
            .position(offset as usize)
            .content_length(total as usize)
            .viewport_content_length(body_area.height as usize);
        f.render_stateful_widget(
            Scrollbar::new(ScrollbarOrientation::VerticalRight)
                .begin_symbol(None)
                .end_symbol(None),
            body_area,
            &mut scrollbar_state,
        );
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
    if one_line.chars().count() <= max {
        return one_line.to_string();
    }
    let cut: String = one_line.chars().take(max.saturating_sub(1)).collect();
    format!("{cut}…")
}
