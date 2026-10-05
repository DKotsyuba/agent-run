//! The project picker: filter the session list to one project (checkout plus
//! its worktrees) or clear the filter.

use super::{overlay, theme};
use crate::app::{App, project_name};
use ratatui::{
    Frame,
    layout::Rect,
    style::Style,
    text::{Line, Span},
    widgets::Paragraph,
};

/// Popup width in columns.
const WIDTH: u16 = 66;

/// Rows the picker offers (projects plus the reset entry), clamped.
fn rows(app: &App) -> u16 {
    (app.projects().len() as u16 + 1).clamp(1, 14)
}

/// Renders the project picker popup centered over the current screen, when open.
pub fn render(f: &mut Frame, app: &App, area: Rect) {
    if !app.project_picker {
        return;
    }
    let projects = app.projects();
    let inner = overlay::begin(f, area, "projects", WIDTH, rows(app) + 4);

    let mut lines = Vec::new();
    let reset_selected = app.picker_cursor >= projects.len();
    let marker = |selected: bool| {
        if selected {
            Span::styled("› ", theme::accent())
        } else {
            Span::raw("  ")
        }
    };
    lines.push(
        Line::from(vec![
            marker(reset_selected),
            Span::styled(
                "all projects",
                if reset_selected {
                    Style::new().bold()
                } else {
                    Style::new()
                },
            ),
        ])
        .style(if reset_selected {
            theme::selection()
        } else {
            Style::new()
        }),
    );
    for (index, (root, live, finished)) in projects.iter().enumerate() {
        let selected = index == app.picker_cursor;
        let base = if selected {
            theme::selection()
        } else {
            Style::new()
        };
        let shortened = match &app.home_prefix {
            Some(home) if root.starts_with(home.as_str()) => {
                format!("~{}", &root[home.len()..])
            }
            _ => root.clone(),
        };
        lines.push(
            Line::from(vec![
                marker(selected),
                Span::styled(format!("{}/", project_name(root)), Style::new().bold()),
                Span::styled(
                    format!(" {live} live, {finished} finished — {shortened}"),
                    theme::dim(),
                ),
            ])
            .style(base),
        );
    }
    if projects.is_empty() {
        lines.push(Line::from(Span::styled(
            "No attributable workdirs in the loaded sessions.",
            theme::dim(),
        )));
    }
    lines.push(Line::from(""));
    lines.push(Line::from(Span::styled("↑↓ choose ⏎ apply", theme::dim())));
    f.render_widget(Paragraph::new(lines), inner);
}

/// The picker row under one terminal position, if any.
pub fn row_at(app: &App, area: Rect, x: u16, y: u16) -> Option<usize> {
    if !app.project_picker {
        return None;
    }
    let popup = overlay::rect(area, WIDTH, rows(app) + 4);
    let inner_y = y.checked_sub(popup.y + 1)?;
    if inner_y >= rows(app) {
        return None;
    }
    let _ = x;
    Some(inner_y as usize)
}
