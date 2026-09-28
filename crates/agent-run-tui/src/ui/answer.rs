//! The answer popup: the sealed, verified answer of one session.

use super::theme;
use crate::app::App;
use ratatui::{
    layout::Rect,
    style::Style,
    text::{Line, Span},
    widgets::{Block, Borders, Clear, Paragraph, Wrap},
    Frame,
};

/// Renders the answer popup centered over the current screen, when open.
pub fn render(f: &mut Frame, app: &App, area: Rect) {
    let Some(answer) = app.answer.as_ref() else {
        return;
    };
    let popup = centered(area, 70, 70);
    let block = Block::new()
        .title(Line::from(" sealed answer ").style(theme::accent()))
        .borders(Borders::ALL);
    let inner = block.inner(popup);
    f.render_widget(Clear, popup);
    f.render_widget(block, popup);

    let mut lines: Vec<Line> = Vec::new();
    if !answer.available {
        lines.push(Line::from(Span::styled(
            "No sealed answer yet.",
            theme::dim(),
        )));
    } else {
        lines.push(Line::from(vec![
            Span::styled("status ", theme::dim()),
            Span::styled(answer.status.as_str().to_string(), Style::new().bold()),
        ]));
        if let Some(content) = &answer.content {
            let text = agent_run::transcript::sanitize(content);
            lines.push(Line::from(""));
            for raw_line in text.lines() {
                lines.push(Line::from(raw_line.to_string()));
            }
            if !answer.inline_complete {
                lines.push(Line::from(""));
                lines.push(Line::from(Span::styled(
                    "…truncated inline preview; the full answer is on disk:",
                    theme::warning(),
                )));
            }
        }
        if let Some(path) = &answer.path {
            lines.push(Line::from(""));
            lines.push(Line::from(vec![
                Span::styled("path ", theme::dim()),
                Span::raw(path.display().to_string()),
            ]));
        }
        if let Some(sha256) = &answer.sha256 {
            lines.push(Line::from(vec![
                Span::styled("sha256 ", theme::dim()),
                Span::raw(sha256.clone()),
            ]));
        }
        if let Some(bytes) = answer.size_bytes {
            lines.push(Line::from(vec![
                Span::styled("size ", theme::dim()),
                Span::raw(format!("{bytes} bytes")),
            ]));
        }
    }
    lines.push(Line::from(""));
    lines.push(Line::from(Span::styled("Esc closes", theme::dim())));
    f.render_widget(Paragraph::new(lines).wrap(Wrap { trim: false }), inner);
}

/// Computes a centered rectangle covering `percent` of the area in both axes.
fn centered(area: Rect, percent_x: u16, percent_y: u16) -> Rect {
    let x = area.width.saturating_mul(100 - percent_x) / 200 + area.x;
    let y = area.height.saturating_mul(100 - percent_y) / 200 + area.y;
    let width = area.width.saturating_mul(percent_x) / 100;
    let height = area.height.saturating_mul(percent_y) / 100;
    Rect {
        x,
        y,
        width,
        height,
    }
}
