//! The sessions list: compact session cards stacked in one column, with the
//! finished sessions hidden behind a dropdown.

use super::theme;
use crate::app::{App, human_duration, status_pictogram};
use ratatui::{
    Frame,
    layout::Rect,
    style::Style,
    text::{Line, Span},
    widgets::Paragraph,
};

/// Card height in rows: one padding row on each side plus five content rows
/// (task, id, runtime/model, workdir, elapsed), and one blank separator row
/// below the card.
pub const CARD_PITCH_Y: u16 = 8;
/// Content rows of one card, padding included.
pub const CARD_HEIGHT: u16 = CARD_PITCH_Y - 1;
/// Horizontal padding columns on each side of the card content.
const CARD_PAD_X: u16 = 2;

/// The card under one terminal position, if any.
///
/// Cards stack in one column; the first visible card row is the scroll
/// offset, clamped by [`App::sync_list_scroll`] and the render pass.
pub fn card_at(app: &App, area: Rect, x: u16, y: u16) -> Option<usize> {
    if x < area.x + 1 || x >= area.x + area.width.saturating_sub(1) {
        return None;
    }
    let inner_y = y.checked_sub(area.y + 1)?;
    let rows = visible_rows(area);
    let row = (inner_y / CARD_PITCH_Y) as usize;
    if row >= rows {
        return None;
    }
    let card = app.list_scroll + row;
    if card < app.card_list().len() {
        Some(card)
    } else {
        None
    }
}

/// Fully visible card rows inside the bordered area.
pub fn visible_rows(area: Rect) -> usize {
    (area.height.saturating_sub(2) / CARD_PITCH_Y) as usize
}

/// Whether one click lands on the finished-sessions dropdown line.
pub fn dropdown_clicked(app: &App, area: Rect, y: u16) -> bool {
    if app.finished_count() == 0 {
        return false;
    }
    y == area.y + area.height.saturating_sub(2)
}

/// Renders the sessions screen into the given area.
pub fn render(f: &mut Frame, app: &App, area: Rect) {
    let cards = app.card_list();
    let live = cards
        .iter()
        .filter(|index| !app.sessions[**index].status.terminal())
        .count();
    let title = format!(
        " agent-run — {} {} ",
        if app.completed_open { "all" } else { "active" },
        if app.loaded {
            format!(
                "({} live, {} finished, rev {})",
                live,
                app.finished_count(),
                app.revision.unwrap_or(0)
            )
        } else {
            format!("{} loading", app.spinner())
        },
    );
    let block = ratatui::widgets::Block::bordered().title(Line::from(title).style(theme::accent()));
    let inner = block.inner(area);
    f.render_widget(block, area);

    if cards.is_empty() {
        render_empty_hint(f, app, inner);
        return;
    }

    let rows = visible_rows(area);
    let first_row = app.list_scroll.min(cards.len().saturating_sub(1));
    for row in 0..rows {
        let card = first_row + row;
        if card >= cards.len() {
            break;
        }
        let agent = &app.sessions[cards[card]];
        let rect = Rect {
            x: inner.x,
            y: inner.y + row as u16 * CARD_PITCH_Y,
            width: inner.width,
            height: CARD_HEIGHT.min(inner.height.saturating_sub(row as u16 * CARD_PITCH_Y)),
        };
        if rect.height == 0 {
            break;
        }
        render_card(f, app, agent, card == app.selected, rect);
    }
}

/// Renders one session card: `<pictogram> <task>` / id / RUNTIME/model / time.
fn render_card(
    f: &mut Frame,
    app: &App,
    agent: &agent_run_domain::AgentView,
    selected: bool,
    rect: Rect,
) {
    let background = if selected {
        Color24::selected()
    } else {
        Color24::card()
    };
    let bg = Style::new().bg(background.rat());
    for y in rect.y..rect.y.saturating_add(rect.height) {
        for x in rect.x..rect.x.saturating_add(rect.width) {
            if let Some(cell) = f.buffer_mut().cell_mut((x, y)) {
                cell.set_style(bg);
            }
        }
    }
    let width = rect.width as usize;
    let pad = " ".repeat(CARD_PAD_X as usize);
    let text_width = width.saturating_sub(CARD_PAD_X as usize * 2);
    // Every content row carries the side padding; the first and last rows
    // stay empty so the card reads as a padded tile.
    let lines = vec![
        Line::from(Span::styled(" ".repeat(width), bg)),
        Line::from(vec![
            Span::styled(pad.clone(), bg),
            Span::styled(
                status_pictogram(agent.status).to_string(),
                Style::new()
                    .fg(theme::status_color(agent.status))
                    .bg(background.rat()),
            ),
            Span::styled(" ", bg),
            Span::styled(
                truncate(&agent.task_summary, text_width.saturating_sub(3)),
                Style::new().bold().bg(background.rat()),
            ),
        ]),
        Line::from(Span::styled(
            format!("{pad}{}", truncate(agent.agent_id.as_str(), text_width)),
            Style::new()
                .fg(ratatui::style::Color::DarkGray)
                .bg(background.rat()),
        )),
        Line::from(Span::styled(
            format!(
                "{pad}{}",
                truncate(
                    &format!("{}/{}", agent.runtime.to_uppercase(), agent.model),
                    text_width,
                )
            ),
            Style::new()
                .fg(ratatui::style::Color::Gray)
                .bg(background.rat()),
        )),
        Line::from(Span::styled(
            format!("{pad}{}", truncate(&workdir_line(app, agent), text_width)),
            Style::new()
                .fg(ratatui::style::Color::DarkGray)
                .bg(background.rat()),
        )),
        Line::from(Span::styled(
            format!(
                "{pad}{}",
                truncate(&human_duration(agent.elapsed_seconds), text_width)
            ),
            Style::new()
                .fg(theme::status_color(agent.status))
                .bg(background.rat()),
        )),
        Line::from(Span::styled(" ".repeat(width), bg)),
    ];
    f.render_widget(Paragraph::new(lines), rect);
}

/// Renders an empty-state hint when no cards are loaded or available.
fn render_empty_hint(f: &mut Frame, app: &App, area: Rect) {
    let text = if app.loaded {
        "No sessions in this scope. Tab expands finished sessions."
    } else {
        "Waiting for the resident broker…"
    };
    f.render_widget(
        Paragraph::new(Line::from(vec![
            Span::styled(app.spinner(), theme::accent()),
            Span::raw(" "),
            Span::styled(text, theme::dim()),
        ])),
        area,
    );
}

/// Renders the finished-sessions dropdown line along the bottom border.
pub fn render_dropdown(f: &mut Frame, app: &App, area: Rect) {
    let finished = app.finished_count();
    if finished == 0 {
        return;
    }
    let line_y = area.y + area.height.saturating_sub(2);
    let rect = Rect {
        y: line_y,
        height: 1,
        x: area.x + 1,
        width: area.width.saturating_sub(2),
    };
    let marker = if app.completed_open { "▾" } else { "▸" };
    let hint = if app.completed_open {
        "Tab/click to collapse"
    } else {
        "Tab/click to expand"
    };
    let line = Line::from(vec![
        Span::styled(format!("{marker} finished ({finished})"), theme::accent()),
        Span::styled(format!(" — {hint}"), theme::dim()),
    ]);
    f.render_widget(Paragraph::new(line), rect);
}

/// Builds the workdir line: `workdir ~/path`, home shortened to `~`.
///
/// Older resident brokers predate the `workdir` projection and omit the
/// field; the task text is the fallback — orchestrator tasks conventionally
/// carry a `Workdir: <path>` sentence.
fn workdir_line(app: &App, agent: &agent_run_domain::AgentView) -> String {
    let path = agent
        .workdir
        .as_deref()
        .map(str::to_string)
        .or_else(|| task_workdir(&agent.task_summary));
    match path {
        Some(path) => {
            let shortened = match &app.home_prefix {
                Some(home) if path.starts_with(home.as_str()) => {
                    format!("~{}", &path[home.len()..])
                }
                _ => path,
            };
            format!("workdir {shortened}")
        }
        None => "workdir —".to_string(),
    }
}

/// Extracts the first absolute path after a `Workdir:` sentence fragment.
fn task_workdir(task: &str) -> Option<String> {
    let lowered = task.to_lowercase();
    let start = lowered.find("workdir:")? + "workdir:".len();
    let rest = &task[start..];
    // Skip filler words such as `worktree` before the path itself.
    rest.split_whitespace()
        .find(|token| token.starts_with('/') || token.starts_with('~'))
        .map(|token| {
            token
                .trim_matches(|c: char| ".,;:)\u{2019}\"'`".contains(c))
                .to_string()
        })
}

/// Truncates one line of text to `max` characters with an ellipsis.
fn truncate(text: &str, max: usize) -> String {
    if max == 0 {
        return String::new();
    }
    if text.chars().count() <= max {
        return text.to_string();
    }
    let cut: String = text.chars().take(max.saturating_sub(1)).collect();
    format!("{cut}…")
}

/// 24-bit card palette kept in one place for selection and body fills.
#[derive(Clone, Copy)]
struct Color24(u8, u8, u8);

impl Color24 {
    fn card() -> Self {
        Self(28, 32, 42)
    }
    fn selected() -> Self {
        Self(44, 54, 74)
    }
    fn rat(self) -> ratatui::style::Color {
        ratatui::style::Color::Rgb(self.0, self.1, self.2)
    }
}
