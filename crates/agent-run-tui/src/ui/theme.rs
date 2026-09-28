//! Color and symbol conventions shared by every pane.

use agent_run_domain::domain::Status;
use ratatui::style::{Color, Modifier, Style};

/// Badge label color per lifecycle status.
pub fn status_color(status: Status) -> Color {
    match status {
        Status::Created => Color::Blue,
        Status::Starting => Color::LightBlue,
        Status::Running => Color::Cyan,
        Status::Cancelling => Color::Yellow,
        Status::Succeeded => Color::Green,
        Status::Failed => Color::Red,
        Status::TimedOut => Color::LightRed,
        Status::Cancelled => Color::DarkGray,
        Status::Lost => Color::Magenta,
    }
}

/// Style of one status badge.
pub fn status_style(status: Status) -> Style {
    Style::new().fg(Color::Black).bg(status_color(status))
}

/// Role color of a transcript message.
pub fn role_color(role: &str) -> Color {
    match role {
        "user" => Color::Blue,
        "assistant" => Color::Green,
        "tool" | "tool_use" | "tool_result" => Color::Yellow,
        "system" => Color::DarkGray,
        _ => Color::Magenta,
    }
}

/// Dim annotation style (timestamps, meta, hints).
pub fn dim() -> Style {
    Style::new().fg(Color::DarkGray)
}

/// Emphasized key-label style used by hints and titles.
pub fn accent() -> Style {
    Style::new().fg(Color::Cyan).add_modifier(Modifier::BOLD)
}

/// Style of the selected message row.
pub fn selection() -> Style {
    Style::new().bg(Color::Rgb(40, 48, 64))
}

/// Subtle background highlighting the message under the mouse pointer.
pub fn hover() -> Style {
    Style::new().bg(Color::Rgb(33, 39, 51))
}

/// Style of a warning (stall silence, delivery trouble).
pub fn warning() -> Style {
    Style::new().fg(Color::Yellow).add_modifier(Modifier::BOLD)
}

/// Style of failure text.
pub fn failure() -> Style {
    Style::new().fg(Color::Red)
}
