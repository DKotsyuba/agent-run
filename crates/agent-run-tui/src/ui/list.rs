//! The session list pane: two-row session entries under a `LIVE` header,
//! with finished sessions behind an expandable `FINISHED` section.
//!
//! Rows live in a flat logical line space shared by rendering and
//! hit-testing: line 0 is the `LIVE` header, line 1 is blank, every session
//! occupies three lines (two content rows plus one gap), and the finished
//! section adds its header plus one blank line before the finished rows.

use super::{text, theme};
use crate::app::{self, human_duration, App};
use ratatui::{
    layout::Rect,
    style::{Modifier, Style},
    text::{Line, Span},
    widgets::Paragraph,
    Frame,
};

/// Terminal rows one session entry occupies: two content rows plus one gap.
pub const ROW_PITCH: usize = 3;
/// Logical lines above the first session row (the `LIVE` header and a gap).
const HEADER_ROWS: usize = 2;
/// Logical lines the finished-section header adds before its first row.
const FINISHED_HEADER_ROWS: usize = 2;

/// The logical line of the finished-section header.
fn finished_header_line(live_len: usize) -> usize {
    HEADER_ROWS + live_len * ROW_PITCH
}

/// The first logical line of the session at card position `pos` (live
/// sessions first, finished ones behind the section header).
fn card_line(live_len: usize, pos: usize) -> usize {
    if pos < live_len {
        HEADER_ROWS + pos * ROW_PITCH
    } else {
        finished_header_line(live_len) + FINISHED_HEADER_ROWS + (pos - live_len) * ROW_PITCH
    }
}

/// The card position under one logical line, if the line belongs to a card.
fn card_of_line(live_len: usize, total: usize, line: usize) -> Option<usize> {
    if line < HEADER_ROWS {
        return None;
    }
    if line < finished_header_line(live_len) {
        let pos = (line - HEADER_ROWS) / ROW_PITCH;
        return if pos < live_len { Some(pos) } else { None };
    }
    if total > live_len {
        let fin_start = finished_header_line(live_len) + FINISHED_HEADER_ROWS;
        if line >= fin_start {
            let pos = live_len + (line - fin_start) / ROW_PITCH;
            return if pos < total { Some(pos) } else { None };
        }
    }
    None
}

/// Live-card count of one card list.
fn live_len(app: &App, cards: &[usize]) -> usize {
    cards
        .iter()
        .filter(|index| !app.sessions[**index].status.terminal())
        .count()
}

/// The first visible logical line for the current scroll offset.
///
/// Scrolling moves in whole session entries (three lines each); offset zero
/// keeps the `LIVE` header and its blank row pinned at the top.
fn top_line(app: &App, cards: &[usize]) -> usize {
    let scroll = app.list_scroll.min(cards.len().saturating_sub(1));
    scroll * ROW_PITCH
}

/// The card position under one terminal position, if any.
pub fn card_at(app: &App, area: Rect, x: u16, y: u16) -> Option<usize> {
    if x < area.x || x >= area.x + area.width || y < area.y || y >= area.y + area.height {
        return None;
    }
    let cards = app.card_list();
    let line = top_line(app, &cards) + (y - area.y) as usize;
    card_of_line(live_len(app, &cards), cards.len(), line)
}

/// Whether one click lands on the finished-section header row.
///
/// The header always renders — even when the active-only scope has loaded no
/// finished sessions yet — because it is the operator's way to widen the
/// scope.
pub fn finished_header_clicked(app: &App, area: Rect, y: u16) -> bool {
    if y < area.y || y >= area.y + area.height {
        return false;
    }
    let cards = app.card_list();
    let live = live_len(app, &cards);
    let line = top_line(app, &cards) + (y - area.y) as usize;
    line == finished_header_line(live)
}

/// Card indices and finished-toggle flags for each pane row of this frame.
/// Listing projection and live count are evaluated once for the whole hit map.
pub(crate) fn hit_rows(app: &App, area: Rect) -> Vec<(Option<usize>, bool)> {
    let cards = app.card_list();
    let live = live_len(app, &cards);
    let top = top_line(app, &cards);
    (0..usize::from(area.height))
        .map(|row| {
            let line = top + row;
            (
                card_of_line(live, cards.len(), line),
                line == finished_header_line(live),
            )
        })
        .collect()
}

/// Renders the session list pane.
pub fn render(f: &mut Frame, app: &App, area: Rect) {
    let p = theme::palette();
    let focused = app.screen == crate::app::Screen::Sessions;
    let cards = app.card_list();
    let live = live_len(app, &cards);
    let width = area.width as usize;
    let top = top_line(app, &cards);

    let mut lines = Vec::with_capacity(area.height as usize);
    let finished = app.finished_in_scope();
    for row in 0..area.height as usize {
        let line = top + row;
        lines.push(Line::from(match line {
            0 => header_line(live, focused, width),
            1 => Vec::new(),
            _ if line == finished_header_line(live) => {
                finished_header_line_content(app, finished, focused, width)
            }
            _ => match card_of_line(live, cards.len(), line) {
                Some(pos) if pos < cards.len() => {
                    let sub = (line - card_line(live, pos)) % ROW_PITCH;
                    card_row(app, &cards, pos, sub, focused, width)
                }
                _ => Vec::new(),
            },
        }));
    }
    f.render_widget(Paragraph::new(lines).style(Style::new().bg(p.panel)), area);
    if cards.is_empty() {
        render_empty_hint(f, app, area);
    }
}

/// The `LIVE` header row with the live count.
fn header_line(live: usize, focused: bool, width: usize) -> Vec<Span<'static>> {
    let p = theme::palette();
    let heading = if focused {
        theme::accent()
    } else {
        Style::new().fg(p.gray).add_modifier(Modifier::BOLD)
    };
    text::fit(
        vec![
            Span::raw("  "),
            Span::styled("LIVE", heading),
            Span::styled(format!("  {live}"), theme::dim()),
        ],
        width,
        Style::new(),
    )
}

/// The `▾/▸ FINISHED` section header with its tab hint.
///
/// The count appears once it is known — the broker's unfiltered total
/// arrives with the listings, or finished rows are already in the list.
fn finished_header_line_content(
    app: &App,
    finished: usize,
    focused: bool,
    width: usize,
) -> Vec<Span<'static>> {
    let p = theme::palette();
    let heading = if focused {
        theme::accent()
    } else {
        Style::new().fg(p.gray).add_modifier(Modifier::BOLD)
    };
    let marker = if app.completed_open { "▾ " } else { "▸ " };
    let mut left = vec![
        Span::styled(marker, Style::new().fg(p.accent)),
        Span::styled("FINISHED", heading),
    ];
    if app.completed_open || finished > 0 {
        left.push(Span::styled(format!("  {finished}"), theme::dim()));
    }
    text::lr(
        left,
        vec![
            Span::styled("tab", theme::accent()),
            Span::styled(
                if app.completed_open {
                    " collapse "
                } else {
                    " expand "
                },
                theme::dim(),
            ),
            Span::raw(" "),
        ],
        width,
        Style::new(),
    )
}

/// One rendered row of a session entry (`sub` 0..3).
fn card_row(
    app: &App,
    cards: &[usize],
    pos: usize,
    sub: usize,
    focused: bool,
    width: usize,
) -> Vec<Span<'static>> {
    let p = theme::palette();
    let agent = &app.sessions[cards[pos]];
    let selected = pos == app.selected;
    let hovered = !selected && app.hover_card == Some(pos);
    let base = if selected {
        theme::selection()
    } else if hovered {
        theme::hover()
    } else {
        Style::new()
    };
    let bg = base.bg;
    // Every span of a highlighted row carries the row background so the
    // highlight fills the full row width.
    let cell = |content: String, style: Style| {
        Span::styled(
            content,
            match bg {
                Some(color) => style.bg(color),
                None => style,
            },
        )
    };
    let bar = if focused { p.accent } else { p.gray };
    let bar_span = cell(
        if selected { "▌".into() } else { " ".into() },
        Style::new().fg(bar),
    );

    match sub {
        0 => {
            let title_style = if selected || hovered {
                Style::new().fg(p.bwhite).add_modifier(Modifier::BOLD)
            } else {
                Style::new().fg(p.white)
            };
            text::lr(
                vec![
                    bar_span,
                    cell(" ".into(), Style::new()),
                    cell(
                        app::status_glyph(agent.status, app.spinner()).to_string(),
                        Style::new().fg(theme::status_color(agent.status)),
                    ),
                    cell(" ".into(), Style::new()),
                    cell(crate::app::display_title(agent), title_style),
                ],
                vec![
                    cell(" ".into(), Style::new()),
                    cell(
                        human_duration(agent.elapsed_seconds),
                        Style::new().fg(theme::status_time_color(agent.status)),
                    ),
                    cell(" ".into(), Style::new()),
                ],
                width,
                base,
            )
        }
        1 => {
            let detail = match crate::app::agent_workdir(agent) {
                Some(workdir) => workdir_basename(&workdir),
                None => String::new(),
            };
            let mut left = vec![
                bar_span,
                cell("   ".into(), Style::new()),
                cell(agent.model.clone(), theme::dim()),
            ];
            if !detail.is_empty() {
                let detail_style = if hovered {
                    Style::new().fg(p.white)
                } else {
                    theme::dim()
                };
                left.push(cell(" · ".into(), theme::dim()));
                left.push(cell(detail, detail_style));
            }
            let right = if agent.status.terminal() {
                cell(
                    format!("{} ", app::id_hash(agent.agent_id.as_str(), 8)),
                    theme::dim(),
                )
            } else {
                cell(
                    format!(
                        "idle {} ",
                        human_duration(agent.silence_seconds.unwrap_or(0.0))
                    ),
                    theme::dim(),
                )
            };
            text::lr(left, vec![right], width, base)
        }
        _ => text::fit(Vec::new(), width, base),
    }
}

/// Renders an empty-state hint when no sessions are loaded or in scope.
///
/// The `FINISHED` header sits directly under the pinned header rows even for
/// an empty list — it is the operator's way to widen the scope — so the hint
/// renders below it and stays bare. With finished sessions known to exist,
/// the empty state belongs to the live section alone.
fn render_empty_hint(f: &mut Frame, app: &App, area: Rect) {
    let text = if !app.loaded {
        "Waiting for the resident broker…"
    } else if app.finished_count() > 0 {
        "no live sessions"
    } else {
        "No sessions in this scope."
    };
    let hint_row = (HEADER_ROWS + FINISHED_HEADER_ROWS) as u16;
    f.render_widget(
        Paragraph::new(Line::from(vec![
            Span::styled("  ", Style::new()),
            Span::styled(app.spinner(), theme::accent()),
            Span::raw(" "),
            Span::styled(text, theme::dim()),
        ])),
        Rect {
            y: area.y + hint_row,
            height: area.height.saturating_sub(hint_row),
            ..area
        },
    );
}

/// The final path component of one working directory.
fn workdir_basename(workdir: &str) -> String {
    std::path::Path::new(workdir)
        .file_name()
        .map(|name| name.to_string_lossy().into_owned())
        .unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tests_support::agent_view;

    const STABLE: &str = "ag-20260928-101500-aaaaaaaaaa";
    const RUN: &str = "ag-20260928-101500-bbbbbbbbbb";

    /// Three sessions: two live, one finished.
    fn app() -> App {
        let mut app = App::new();
        app.sessions = vec![
            agent_view(STABLE, RUN, "running"),
            agent_view(
                "ag-20260928-101501-cccccccccc",
                "ag-20260928-101501-dddddddddd",
                "starting",
            ),
            agent_view(
                "ag-20260928-101502-eeeeeeeeee",
                "ag-20260928-101502-ffffffffff",
                "succeeded",
            ),
        ];
        app
    }

    #[test]
    fn card_lines_place_the_finished_header_between_sections() {
        assert_eq!(card_line(2, 0), 2);
        assert_eq!(card_line(2, 1), 5);
        assert_eq!(finished_header_line(2), 8);
        assert_eq!(card_line(2, 2), 10, "finished card sits below the header");
    }

    #[test]
    fn card_at_maps_rows_to_cards_in_all_sections() {
        let app = app();
        let area = Rect {
            x: 1,
            y: 2,
            width: 43,
            height: 30,
        };
        // Collapsed: only live cards exist.
        assert_eq!(card_at(&app, area, 5, 4), Some(0));
        assert_eq!(card_at(&app, area, 5, 7), Some(1));
        assert_eq!(card_at(&app, area, 5, 10), None, "finished hidden");
        // Expanded: the finished header sits at row 10, its card at 12.
        let mut open = app;
        open.completed_open = true;
        assert_eq!(card_at(&open, area, 5, 10), None, "header row");
        assert!(finished_header_clicked(&open, area, 10));
        assert_eq!(card_at(&open, area, 5, 12), Some(2));
    }

    #[test]
    fn card_at_rejects_positions_outside_the_pane() {
        let app = app();
        let area = Rect {
            x: 1,
            y: 2,
            width: 43,
            height: 30,
        };
        assert_eq!(card_at(&app, area, 0, 4), None, "padding column");
        assert_eq!(card_at(&app, area, 5, 2), None, "header row");
        assert_eq!(card_at(&app, area, 5, 40), None, "below the pane");
    }
}
