//! Shared overlay chrome: a rounded accent border on a panel background,
//! with the title on the top border and an `esc close` hint on the bottom
//! border. The help, project picker, and sealed-answer popups all wear it.

use super::theme;
use crate::app::App;
use ratatui::{
    layout::Rect,
    style::{Modifier, Style},
    text::{Line, Span},
    widgets::{Block, BorderType, Clear, Paragraph},
    Frame,
};

/// The centered overlay rectangle for one content size, clipped to the area.
pub fn rect(area: Rect, width: u16, height: u16) -> Rect {
    let width = width.clamp(3, area.width.saturating_sub(2).max(3));
    let height = height.clamp(2, area.height.saturating_sub(2).max(2));
    Rect {
        x: area.x + (area.width.saturating_sub(width)) / 2,
        y: area.y + (area.height.saturating_sub(height)) / 2,
        width,
        height,
    }
}

/// Clears the area behind the overlay and draws the shared frame, returning
/// the inner content rectangle.
pub fn begin(f: &mut Frame, area: Rect, title: &str, width: u16, height: u16) -> Rect {
    let p = theme::palette();
    let popup = rect(area, width, height);
    f.render_widget(Clear, popup);
    let block = Block::bordered()
        .border_type(BorderType::Rounded)
        .border_style(Style::new().fg(p.accent))
        .title(Line::from(format!(" {title} ")).style(theme::accent()))
        .title_bottom(Line::from(" esc close ").style(theme::dim()))
        .style(Style::new().bg(p.panel));
    let inner = block.inner(popup);
    f.render_widget(block, popup);
    inner
}

/// Renders the key-help overlay, when open.
pub fn render_help(f: &mut Frame, app: &App, area: Rect) {
    if !app.help {
        return;
    }
    let p = theme::palette();
    let inner = begin(f, area, "keys", 52, 16);
    let key = |k: &str, d: &str| {
        Line::from(vec![
            Span::styled(format!("{k:<10}"), theme::accent()),
            Span::styled(d.to_string(), Style::new().fg(p.white)),
        ])
    };
    let heading = |name: &str| {
        Line::from(Span::styled(
            name.to_string(),
            Style::new().fg(p.gray).add_modifier(Modifier::BOLD),
        ))
    };
    let lines = vec![
        Line::from(""),
        heading("agents"),
        key("↑↓ j k", "select agent · move between blocks"),
        key("⏎", "open transcript · expand tool"),
        key("esc", "back to agent list"),
        key("tab", "show / hide finished"),
        key("g G", "top · bottom"),
        Line::from(""),
        heading("session"),
        key("f", "follow live output"),
        key("a", "sealed answer"),
        key("p", "project picker"),
        key("r", "refresh from broker"),
        key("? q", "help · quit"),
    ];
    f.render_widget(Paragraph::new(lines).style(Style::new().bg(p.panel)), inner);
}
