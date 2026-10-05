//! Pools shelf, roster, untrusted chat blocks and full-criteria overlay.
//! Entry rows are memoized; spinner and roster facts use the broker status.

use super::{overlay, text, theme};
use crate::{
    app::{self, App},
    pools::{Buffer, PoolStatus, Target},
};
use agent_run_domain::pool::{
    AuthorKind, Direction, EntryKind, PoolEntryView, PoolState, VoteDecision,
};
use ratatui::{
    Frame,
    layout::Rect,
    style::{Color, Modifier, Style},
    text::{Line, Span},
    widgets::Paragraph,
};
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
};

/// Rendered chat row with its immutable entry identity for hit-testing.
#[derive(Clone)]
pub struct Row {
    /// Owning entry sequence.
    pub seq: u64,
    /// Sanitized styled display row.
    pub line: Line<'static>,
}
/// Memoized one-entry expansion at a single terminal width.
struct BlockMemo {
    /// Last wrapping width.
    width: usize,
    /// Whether the entry body was expanded.
    expanded: bool,
    /// Snapshot at this proposal, absent when unavailable.
    snapshot: Option<String>,
    /// Sanitized wrapped rows.
    rows: Vec<Row>,
}
/// Per-pool block cache and shared flattened index, reused on unchanged draws.
#[derive(Default)]
pub struct Memo {
    /// Rows per immutable entry.
    blocks: BTreeMap<u64, BlockMemo>,
    /// Last flattened index.
    flat: Arc<Vec<Row>>,
    /// Whether eviction invalidated the index.
    dirty: bool,
}
impl Memo {
    /// Removes memoized entries evicted by the pool's memory bound.
    pub fn retain(&mut self, retained: &BTreeSet<u64>) {
        let old = self.blocks.len();
        self.blocks.retain(|seq, _| retained.contains(seq));
        self.dirty |= old != self.blocks.len();
    }
}
/// Removes terminal escapes and controls through the existing transcript scanner.
fn sanitize(value: &str) -> String {
    text::Sanitizer::default().push(value)
}

/// Creates sanitized wrapped body rows with an explicit untrusted marker.
/// Bodies preview four rows; expansion restores all rows with no hidden truncation.
fn body(value: &str, width: usize, expanded: bool) -> Vec<String> {
    let mut rows = text::wrap(&format!("  untrusted: {}", sanitize(value)), width.max(1));
    if !expanded && rows.len() > 5 {
        let omitted = rows.len() - 4;
        rows.truncate(4);
        rows.push(format!("  … {omitted} more lines  ⏎ expand"));
    }
    rows
}
/// Palette style of report severity and raw vote outcome.
fn entry_color(entry: &PoolEntryView) -> Color {
    let p = theme::palette();
    if let Some(severity) = entry.severity {
        return match severity.as_str() {
            "risk" => p.yellow,
            "question" => p.magenta,
            "blocker" => p.red,
            _ => p.white,
        };
    }
    if let Some(decision) = entry.decision {
        return if decision == VoteDecision::Block {
            p.red
        } else {
            p.white
        };
    }
    match entry.author_kind {
        AuthorKind::Operator => p.accent,
        AuthorKind::Broker => p.gray,
        _ => p.white,
    }
}
/// Builds one immutable chat block, using historical stamped names and exact snapshots.
fn block(entry: &PoolEntryView, width: usize, expanded: bool, snapshot: Option<&str>) -> Vec<Row> {
    let p = theme::palette();
    let author = match entry.author_kind {
        AuthorKind::Member => format!(
            "{} ({})",
            sanitize(entry.author_name.as_deref().unwrap_or("member")),
            sanitize(entry.author_role.as_deref().unwrap_or("role unavailable"))
        ),
        AuthorKind::Operator => "OPERATOR".into(),
        AuthorKind::Broker => "BROKER".into(),
    };
    let to = if entry.direction == Direction::Team {
        "team"
    } else {
        "orchestrator (team copy)"
    };
    let mut kind = entry.kind.as_str().to_string();
    if let Some(severity) = entry.severity {
        kind.push_str(&format!(": {}", severity.as_str()));
    }
    if let Some(decision) = entry.decision {
        kind = decision.as_str().into();
    }
    if let Some(seq) = entry.proposal_seq {
        kind.push_str(&format!(" #{seq}"));
    }
    let stamp = format!(
        "#{} r{} · {author} → {to} · {kind}",
        entry.seq, entry.roster_revision
    );
    let mut lines = Vec::new();
    if entry.kind == EntryKind::Proposal {
        let frame_width = width.clamp(4, 80);
        let inner = frame_width.saturating_sub(4).max(1);
        let heading = format!(
            "╭─ #{} r{} · {} → {to} · PROPOSAL",
            entry.seq,
            entry.roster_revision,
            sanitize(entry.author_name.as_deref().unwrap_or("member"))
        );
        let mut spans = text::fit(
            vec![Span::styled(heading, theme::accent())],
            frame_width.saturating_sub(1),
            Style::new().bg(p.code),
        );
        if let Some(padding) = spans
            .last_mut()
            .filter(|s| s.content.chars().all(|c| c == ' '))
        {
            padding.content = "─".repeat(padding.width()).into();
            padding.style = theme::accent().bg(p.code);
        }
        spans.push(Span::styled("╮", theme::accent()));
        lines.push(Line::from(spans).style(Style::new().bg(p.code)));
        let mut contents = body(&entry.body, inner, expanded);
        match snapshot {
            Some(snapshot) => {
                contents.push(format!("snapshot (#{}; untrusted):", entry.seq));
                contents.extend(body(snapshot, inner, expanded));
            }
            None => contents.push("snapshot unavailable through public history".into()),
        }
        for content in contents {
            let mut spans = vec![Span::styled("│ ", theme::accent())];
            spans.extend(text::fit(
                vec![Span::styled(content, Style::new().fg(p.white))],
                inner,
                Style::new().bg(p.code),
            ));
            spans.push(Span::styled(" │", theme::accent()));
            lines.push(Line::from(spans).style(Style::new().bg(p.code)));
        }
        lines.push(Line::styled(
            format!("╰{}╯", "─".repeat(frame_width.saturating_sub(2))),
            Style::new().fg(p.accent).bg(p.code),
        ));
    } else {
        let mut header = vec![
            Span::styled(
                format!("#{} r{} · ", entry.seq, entry.roster_revision),
                theme::dim(),
            ),
            Span::styled(
                stamp
                    .split_once(" · ")
                    .map_or(stamp.clone(), |(_, rest)| rest.to_string()),
                Style::new().fg(entry_color(entry)),
            ),
        ];
        if entry.author_kind == AuthorKind::Operator {
            for span in &mut header {
                span.style = span.style.bg(p.accent_bg).add_modifier(Modifier::BOLD);
            }
        }
        lines.push(Line::from(text::fit(header, width, Style::new())));
        if expanded || !matches!(entry.kind, EntryKind::Vote | EntryKind::Revoke) {
            lines.extend(
                body(&entry.body, width, expanded)
                    .into_iter()
                    .map(|v| Line::styled(v, Style::new().fg(p.fg))),
            );
        }
    }
    lines.push(Line::from(""));
    lines
        .into_iter()
        .map(|line| Row {
            seq: entry.seq,
            line,
        })
        .collect()
}
/// Returns cached wrapped chat rows; only new/expanded/width-changed blocks rebuild.
pub fn rows(buffer: &Buffer, width: usize) -> Arc<Vec<Row>> {
    let mut memo = buffer.memo.borrow_mut();
    let mut changed = memo.dirty;
    for entry in &buffer.entries {
        let expanded = buffer.expanded.contains(&entry.seq);
        let snapshot = buffer.snapshots.get(&entry.seq);
        let valid = memo.blocks.get(&entry.seq).is_some_and(|m| {
            m.width == width && m.expanded == expanded && m.snapshot.as_ref() == snapshot
        });
        if !valid {
            memo.blocks.insert(
                entry.seq,
                BlockMemo {
                    width,
                    expanded,
                    snapshot: snapshot.cloned(),
                    rows: block(entry, width, expanded, snapshot.map(String::as_str)),
                },
            );
            changed = true;
        }
    }
    if changed {
        memo.flat = Arc::new(
            buffer
                .entries
                .iter()
                .flat_map(|entry| memo.blocks[&entry.seq].rows.iter().cloned())
                .collect(),
        );
        memo.dirty = false;
    }
    memo.flat.clone()
}
/// Number of fixed detail rows including a bounded replacement-history window.
fn header_height(buffer: &Buffer) -> usize {
    let members = buffer.status.as_ref().map_or(0, |s| s.members.len());
    let retired = if buffer.history {
        buffer
            .status
            .as_ref()
            .map_or(0, |s| s.replaced_members.len().min(5))
    } else {
        0
    };
    10 + members * 2 + retired
}
/// Available scrolling chat rows in the current terminal.
pub fn viewport(app: &App) -> usize {
    app.pools.buffer().map_or(1, |b| {
        usize::from(app.last_height.saturating_sub(3))
            .saturating_sub(header_height(b) + 3)
            .max(1)
    })
}
/// Writes one fitted row and clips it safely to the pane.
fn put(f: &mut Frame, area: Rect, row: usize, line: Line<'static>, style: Style) {
    if row >= usize::from(area.height) {
        return;
    }
    let style = style.patch(line.style);
    f.render_widget(
        Paragraph::new(Line::from(text::fit(
            line.spans,
            usize::from(area.width),
            style,
        )))
        .style(style),
        Rect {
            y: area.y + row as u16,
            height: 1,
            ..area
        },
    );
}
/// Sanitized one-line display text, with all control sequences removed.
fn plain(value: impl AsRef<str>) -> Line<'static> {
    Line::from(sanitize(value.as_ref()).replace('\n', " "))
}
/// Builds stable list row identities and rendered headings, scrolling to the selected pool.
fn list_rows(app: &App) -> Vec<(Line<'static>, Option<agent_run_domain::pool::PoolId>)> {
    let mut rows = vec![];
    for (state, title, total) in [
        (PoolState::Open, "OPEN", app.pools.open_total),
        (PoolState::Completed, "COMPLETED", app.pools.completed_total),
    ] {
        rows.push((
            Line::styled(format!(" {title}  {total}"), theme::dim()),
            None,
        ));
        rows.push((Line::from(""), None));
        for pool in app.pools.items.iter().filter(|pool| pool.state == state) {
            let glyph = if state == PoolState::Open {
                "●"
            } else {
                "✓"
            };
            rows.push((
                Line::from(vec![
                    Span::styled(
                        format!("  {glyph} "),
                        Style::new().fg(if state == PoolState::Open {
                            theme::palette().accent
                        } else {
                            theme::palette().green
                        }),
                    ),
                    Span::raw(format!(
                        "{}{}",
                        sanitize(&pool.goal).replace('\n', " "),
                        if pool.goal_truncated { "…" } else { "" }
                    )),
                ]),
                Some(pool.pool_id.clone()),
            ));
            rows.push((
                plain(format!(
                    "    {}/{} ready · {} · {}",
                    pool.ready,
                    pool.members_count,
                    pool.current_proposal_seq
                        .map_or("no proposal".into(), |seq| format!("#{seq}")),
                    app::id_hash(pool.pool_id.as_str(), 10)
                )),
                Some(pool.pool_id.clone()),
            ));
            rows.push((Line::from(""), None));
        }
        if !app.pools.items.iter().any(|pool| pool.state == state) {
            rows.push((Line::styled("  no pools on this page", theme::dim()), None));
            rows.push((Line::from(""), None));
        }
    }
    rows.push((
        Line::styled(
            format!(" [ ] pages · offset {} · 50/state", app.pools.offset),
            theme::dim(),
        ),
        None,
    ));
    if app.pools.fallback {
        rows.push((
            Line::styled(
                "  discovered from sessions (broker has no list_pools)",
                theme::dim(),
            ),
            None,
        ));
        rows.push((
            Line::styled("  older pools need broker list_pools", theme::dim()),
            None,
        ));
    } else if let Some(error) = &app.pools.error {
        rows.push((plain(format!(" broker error, retrying · {error}")), None));
    }
    rows
}
/// Renders OPEN then COMPLETED shelf and captures stable pool IDs under pointer rows.
pub fn render_list(f: &mut Frame, app: &App, area: Rect) {
    let p = theme::palette();
    let rows = list_rows(app);
    let at = rows
        .iter()
        .position(|(_, id)| id.as_ref() == app.pools.selected.as_ref() && id.is_some())
        .unwrap_or(0);
    let offset = at.saturating_sub(usize::from(area.height).saturating_sub(4));
    for (row, (line, id)) in rows
        .into_iter()
        .skip(offset)
        .take(usize::from(area.height))
        .enumerate()
    {
        let selected = id.is_some() && id.as_ref() == app.pools.selected.as_ref();
        let style = if selected {
            Style::new().fg(p.bwhite).bg(p.surf)
        } else {
            Style::new().fg(p.white).bg(p.panel)
        };
        let mut line = line;
        if selected {
            line.spans.insert(
                0,
                Span::styled(
                    "▌",
                    Style::new().fg(if app.pools.focused { p.gray } else { p.accent }),
                ),
            );
        }
        put(f, area, row, line, style);
        if let Some(id) = id {
            app.pools.hits.borrow_mut().push((
                Rect {
                    y: area.y + row as u16,
                    height: 1,
                    ..area
                },
                Target::Pool(id),
            ));
        }
    }
}
/// Formats vote validity with the broker reason; raw ready decisions never appear valid.
fn vote(member: &crate::pools::Member) -> String {
    if member.counts {
        "ready".into()
    } else {
        match &member.vote {
            Some(vote) => format!("{} · {}", vote.decision.as_str(), sanitize(&member.why)),
            None => sanitize(&member.why),
        }
    }
}
/// One broker-only state summary; unanimity is distinct from completion.
fn summary(status: &PoolStatus) -> Vec<String> {
    let n = status.members.len();
    let ready = status.ready();
    if status.state == PoolState::Completed {
        return vec![
            "✓ Pool completed · frozen broker status".into(),
            format!(
                "Accepted {} · {ready} ready · {n} succeeded · {n} cleaned",
                status
                    .current_proposal
                    .as_ref()
                    .map_or("—".into(), |p| format!("#{}", p.seq))
            ),
            "Formal checks only; not a judgment of the result.".into(),
        ];
    }
    if ready == n && n > 0 {
        return vec!["Agreement; waiting for success/cleanup.".into()];
    }
    let missing = status
        .members
        .iter()
        .filter(|m| !m.counts)
        .map(|m| sanitize(&m.name))
        .collect::<Vec<_>>()
        .join(", ");
    vec![
        format!("{ready}/{n} valid-ready · pool remains open."),
        format!("Waiting for {missing}; success + cleanup still required."),
    ]
}
/// Renders header, frozen/current roster and scrolling chat on the existing detail pane.
pub fn render_detail(f: &mut Frame, app: &App, pane: Rect) {
    let p = theme::palette();
    let area = Rect {
        width: pane.width.saturating_sub(1),
        ..pane
    };
    let Some(buffer) = app.pools.buffer() else {
        put(
            f,
            area,
            0,
            plain("Select a pool to observe its conversation."),
            theme::dim(),
        );
        return;
    };
    let Some(status) = &buffer.status else {
        put(
            f,
            area,
            0,
            plain(format!(
                "Opening pool {}…",
                app::id_hash(buffer.id.as_str(), 10)
            )),
            theme::dim(),
        );
        if let Some(error) = &buffer.error {
            put(f, area, 2, plain(error), theme::failure());
        }
        return;
    };
    put(
        f,
        area,
        0,
        plain(&status.goal),
        Style::new().fg(p.bwhite).add_modifier(Modifier::BOLD),
    );
    let completed = status.state == PoolState::Completed;
    let badge = if completed {
        " ✓ COMPLETED "
    } else {
        " ● OPEN "
    };
    put(
        f,
        area,
        1,
        Line::from(vec![
            Span::styled(
                badge,
                Style::new()
                    .fg(if completed { p.green } else { p.accent })
                    .bg(if completed { p.gbg } else { p.accent_bg })
                    .add_modifier(Modifier::BOLD),
            ),
            Span::styled(
                format!(
                    " {}/{} ready · proposal {}",
                    status.ready(),
                    status.members.len(),
                    status
                        .current_proposal
                        .as_ref()
                        .map_or("—".into(), |p| format!("#{}", p.seq))
                ),
                Style::new().fg(p.white),
            ),
        ]),
        Style::new(),
    );
    let follow = if buffer.follow {
        "● follow"
    } else {
        "○ scrolled"
    };
    put(
        f,
        area,
        2,
        Line::from(text::lr(
            vec![Span::styled(
                format!(
                    "pool {} · roster r{}",
                    app::id_hash(buffer.id.as_str(), 10),
                    status.roster_revision
                ),
                theme::dim(),
            )],
            vec![Span::styled(follow, theme::dim())],
            usize::from(area.width),
            Style::new(),
        )),
        Style::new(),
    );
    let criteria = status
        .criteria
        .iter()
        .map(|c| format!("{}: {}", c.id, c.text))
        .collect::<Vec<_>>()
        .join(" · ");
    put(
        f,
        area,
        3,
        plain(format!("Criteria  {criteria}")),
        Style::new().fg(p.white),
    );
    put(
        f,
        area,
        4,
        plain("c full goal + criteria · m roster · h replaced history"),
        theme::dim(),
    );
    if let Some(error) = &buffer.error {
        put(
            f,
            area,
            5,
            plain(format!("broker error, retrying · {error}")),
            theme::failure(),
        );
    }
    put(
        f,
        area,
        6,
        plain(format!(
            " MEMBERS  {} current · t transcript{}",
            status.members.len(),
            if completed { " · frozen status" } else { "" }
        )),
        theme::dim(),
    );
    let mut row = 7;
    for (at, member) in status.members.iter().enumerate() {
        let selected = buffer.roster && buffer.member == at && app.pools.focused;
        let style = if selected {
            theme::selection()
        } else {
            Style::new().bg(p.panel)
        };
        let native = app
            .sessions
            .iter()
            .find(|a| a.agent_id == member.agent_id)
            .map_or("runtime/model unavailable".into(), |a| {
                format!(
                    "{}/{}{}",
                    a.runtime.to_uppercase(),
                    a.model,
                    if completed { " (latest)" } else { "" }
                )
            });
        let color = if member.counts {
            p.green
        } else if member.why == "blocked" || member.why == "checks_unmet" {
            p.red
        } else {
            p.gray
        };
        put(
            f,
            area,
            row,
            Line::from(vec![
                Span::styled(
                    format!(
                        "{} {} ",
                        app::status_glyph(member.tip_status, app.spinner()),
                        member.slot
                    ),
                    Style::new().fg(theme::status_color(member.tip_status)),
                ),
                Span::styled(
                    sanitize(&format!("{} ({}) · {native} · ", member.name, member.role)),
                    Style::new().fg(p.white),
                ),
                Span::styled(vote(member), Style::new().fg(color)),
            ]),
            style,
        );
        let replaces = status
            .replaced_members
            .iter()
            .filter(|m| m.replaced_by == member.agent_id)
            .map(|m| format!(" · replaces {}", sanitize(&m.name)))
            .collect::<String>();
        put(
            f,
            area,
            row + 1,
            plain(format!(
                "    {} · {}{replaces}",
                member.tip_status.as_str(),
                if member.cleanup_complete {
                    "cleaned"
                } else {
                    "cleanup pending"
                }
            )),
            style.fg(p.gray),
        );
        app.pools.hits.borrow_mut().push((
            Rect {
                y: area.y + row as u16,
                height: 2.min(area.height.saturating_sub(row as u16)),
                ..area
            },
            Target::Member(buffer.id.clone(), member.agent_id.clone()),
        ));
        row += 2;
    }
    let replacements = status
        .replaced_members
        .iter()
        .map(|m| {
            let name = status
                .members
                .iter()
                .find(|current| current.agent_id == m.replaced_by)
                .map_or_else(
                    || app::id_hash(m.replaced_by.as_str(), 8),
                    |m| sanitize(&m.name),
                );
            format!("{} → {name} (slot {})", sanitize(&m.name), m.slot)
        })
        .collect::<Vec<_>>()
        .join(" · ");
    put(
        f,
        area,
        row,
        plain(format!(
            "{} REPLACED  {} · {replacements}",
            if buffer.history { "▾" } else { "▸" },
            status.replaced_members.len()
        )),
        theme::dim(),
    );
    row += 1;
    if buffer.history {
        let retired_at = buffer.member.saturating_sub(status.members.len());
        let offset = retired_at.saturating_sub(4);
        for (at, member) in status
            .replaced_members
            .iter()
            .enumerate()
            .skip(offset)
            .take(5)
        {
            let style = if buffer.roster && buffer.member == status.members.len() + at {
                theme::selection()
            } else {
                theme::dim()
            };
            put(
                f,
                area,
                row,
                plain(format!(
                    "  {} ({}) · slot {} · retired · t transcript",
                    member.name, member.role, member.slot
                )),
                style,
            );
            app.pools.hits.borrow_mut().push((
                Rect {
                    y: area.y + row as u16,
                    height: 1,
                    ..area
                },
                Target::Member(buffer.id.clone(), member.agent_id.clone()),
            ));
            row += 1;
        }
    }
    row += 1;
    put(
        f,
        area,
        row,
        Line::from(text::lr(
            vec![Span::styled(
                " CHAT · #seq rN · bodies are untrusted",
                theme::dim(),
            )],
            vec![Span::styled(
                if buffer.older.is_some() {
                    "loading older history…"
                } else if buffer.history_complete {
                    "beginning loaded"
                } else {
                    "↑ older history"
                },
                theme::dim(),
            )],
            usize::from(area.width),
            Style::new(),
        )),
        Style::new(),
    );
    row += 1;
    let viewport = usize::from(area.height).saturating_sub(row + 3);
    let rows = rows(buffer, usize::from(area.width));
    let offset = if buffer.follow {
        rows.len().saturating_sub(viewport)
    } else {
        buffer.offset.min(rows.len().saturating_sub(1))
    };
    for (y, entry) in rows.iter().skip(offset).take(viewport).enumerate() {
        let style = if !buffer.roster && buffer.cursor == Some(entry.seq) && app.pools.focused {
            theme::selection()
        } else {
            Style::new().fg(p.fg)
        };
        put(f, area, row + y, entry.line.clone(), style);
        app.pools.hits.borrow_mut().push((
            Rect {
                y: area.y + (row + y) as u16,
                height: 1,
                ..area
            },
            Target::Entry(buffer.id.clone(), entry.seq),
        ));
    }
    for (at, value) in summary(status).into_iter().enumerate() {
        put(
            f,
            area,
            usize::from(area.height).saturating_sub(3) + at,
            plain(value),
            if completed {
                Style::new().fg(p.green)
            } else {
                theme::dim()
            },
        );
    }
    if rows.len() > viewport && viewport > 0 {
        let thumb = row + offset.saturating_mul(viewport.saturating_sub(1)) / rows.len().max(1);
        for at in row..row + viewport {
            put(
                f,
                Rect {
                    x: pane.x + pane.width.saturating_sub(1),
                    width: 1,
                    ..pane
                },
                at,
                Line::from(if at == thumb { "┃" } else { "│" }),
                Style::new().fg(if at == thumb { p.accent } else { p.black }),
            );
        }
    }
}

/// Wraps sanitized full goal and criteria at the overlay's content width in columns.
fn criteria_rows(status: &PoolStatus, width: usize) -> Vec<String> {
    let mut content = vec![
        "Goal (untrusted):".into(),
        status.goal.clone(),
        String::new(),
        "Criteria (untrusted):".into(),
    ];
    content.extend(
        status
            .criteria
            .iter()
            .map(|c| format!("{}: {}", c.id, c.text)),
    );
    content
        .into_iter()
        .flat_map(|v| text::wrap(&sanitize(&v), width))
        .collect()
}

/// Last-page row offset for the full criteria at the app's current terminal size.
/// Returns zero before status arrives or when all wrapped rows fit the viewport.
pub fn criteria_max_offset(app: &App) -> usize {
    let Some(status) = app.pools.buffer().and_then(|b| b.status.as_ref()) else {
        return 0;
    };
    let area = Rect::new(0, 0, app.last_width, app.last_height);
    let popup = overlay::rect(area, 90, area.height.saturating_sub(4));
    criteria_rows(status, usize::from(popup.width.saturating_sub(2)))
        .len()
        .saturating_sub(usize::from(popup.height.saturating_sub(2)))
}

/// Renders a scrollable, sanitized full goal/criteria overlay using shared chrome.
pub fn render_criteria(f: &mut Frame, app: &App, area: Rect) {
    if !app.pools.criteria {
        return;
    }
    let Some(status) = app.pools.buffer().and_then(|b| b.status.as_ref()) else {
        return;
    };
    let inner = overlay::begin(
        f,
        area,
        "goal + criteria · untrusted · ↑↓ scroll",
        90,
        area.height.saturating_sub(4),
    );
    let rows = criteria_rows(status, usize::from(inner.width));
    let offset = app
        .pools
        .criteria_offset
        .min(rows.len().saturating_sub(usize::from(inner.height)));
    f.render_widget(
        Paragraph::new(
            rows.into_iter()
                .skip(offset)
                .take(usize::from(inner.height))
                .map(Line::from)
                .collect::<Vec<_>>(),
        )
        .style(Style::new().fg(theme::palette().fg)),
        inner,
    );
}
