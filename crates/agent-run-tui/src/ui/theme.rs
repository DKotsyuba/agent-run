//! Palette and shared styles: a 24-bit design with a 16-color fallback.
//!
//! The palette mirrors the approved terminal design exactly; when the
//! terminal does not advertise `COLORTERM=truecolor` (or `24bit`) a nearest
//! named-color mapping is used instead, with selection kept on a dark named
//! background so highlights stay visible. The choice is made once per
//! process from the environment (see [`palette_for`], [`palette`]).

use agent_run_domain::domain::Status;
use ratatui::style::{Color, Modifier, Style};
use std::sync::OnceLock;

/// Every color token of the design, already resolved to terminal colors.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Palette {
    /// Base background of the whole frame.
    pub bg: Color,
    /// Background of the app bar, key bar, list pane, and code rows.
    pub panel: Color,
    /// Selected-row background.
    pub surf: Color,
    /// Hovered-row background.
    pub hov: Color,
    /// Deepest structural tone (scrollbar track, separators).
    pub black: Color,
    /// Background of expanded tool payloads.
    pub code: Color,
    /// Body text.
    pub fg: Color,
    /// Secondary text.
    pub white: Color,
    /// Emphasized text (titles, selected rows).
    pub bwhite: Color,
    /// Dim annotations (timestamps, meta, hints).
    pub gray: Color,
    /// Failure text.
    pub red: Color,
    /// Success text.
    pub green: Color,
    /// Running activity and warnings.
    pub yellow: Color,
    /// Paths.
    pub blue: Color,
    /// Ask/attention text.
    pub magenta: Color,
    /// Command arguments.
    pub orange: Color,
    /// Green-tinted badge background.
    pub gbg: Color,
    /// Red-tinted badge background.
    pub rbg: Color,
    /// Magenta-tinted badge background.
    pub mbg: Color,
    /// Accent (keys, markers, focus).
    pub accent: Color,
    /// Accent-tinted badge background.
    pub accent_bg: Color,
    /// Whether the palette carries real 24-bit values.
    pub truecolor: bool,
}

impl Palette {
    /// The approved truecolor design.
    pub fn truecolor() -> Self {
        Self {
            bg: Color::Rgb(15, 17, 21),
            panel: Color::Rgb(19, 22, 27),
            surf: Color::Rgb(29, 34, 43),
            hov: Color::Rgb(23, 27, 34),
            black: Color::Rgb(38, 43, 52),
            code: Color::Rgb(19, 22, 27),
            fg: Color::Rgb(205, 210, 218),
            white: Color::Rgb(170, 177, 188),
            bwhite: Color::Rgb(241, 243, 246),
            gray: Color::Rgb(107, 114, 128),
            red: Color::Rgb(240, 113, 120),
            green: Color::Rgb(156, 204, 122),
            yellow: Color::Rgb(230, 192, 123),
            blue: Color::Rgb(122, 167, 240),
            magenta: Color::Rgb(201, 155, 240),
            orange: Color::Rgb(240, 164, 108),
            gbg: Color::Rgb(23, 40, 29),
            rbg: Color::Rgb(46, 25, 28),
            mbg: Color::Rgb(34, 27, 45),
            accent: Color::Rgb(92, 207, 230),
            accent_bg: Color::Rgb(19, 42, 48),
            truecolor: true,
        }
    }

    /// The 16-color fallback: foregrounds map to nearest named colors and
    /// backgrounds fall back to the terminal default, except selection,
    /// which keeps a dark named background so the highlight stays visible.
    pub fn ansi() -> Self {
        Self {
            bg: Color::Reset,
            panel: Color::Reset,
            surf: Color::Black,
            hov: Color::Reset,
            black: Color::DarkGray,
            code: Color::Reset,
            fg: Color::Gray,
            white: Color::Gray,
            bwhite: Color::White,
            gray: Color::DarkGray,
            red: Color::LightRed,
            green: Color::LightGreen,
            yellow: Color::Yellow,
            blue: Color::LightBlue,
            magenta: Color::LightMagenta,
            orange: Color::Yellow,
            gbg: Color::Reset,
            rbg: Color::Reset,
            mbg: Color::Reset,
            accent: Color::Cyan,
            accent_bg: Color::Reset,
            truecolor: false,
        }
    }
}

/// Chooses the palette for one `COLORTERM` value.
///
/// `truecolor` and `24bit` select the 24-bit design; anything else (including
/// `None`) selects the named-color fallback.
pub fn palette_for(colorterm: Option<&str>) -> Palette {
    match colorterm {
        Some("truecolor") | Some("24bit") => Palette::truecolor(),
        _ => Palette::ansi(),
    }
}

/// The one process-wide palette instance.
static PALETTE: OnceLock<Palette> = OnceLock::new();

/// The process-wide palette, fixed on first use from `COLORTERM`.
///
/// Unit tests always draw with the truecolor design so golden frames stay
/// deterministic regardless of the environment they run in; the fallback is
/// covered by the [`palette_for`] tests.
pub fn palette() -> &'static Palette {
    PALETTE.get_or_init(|| {
        #[cfg(test)]
        {
            palette_for(Some("truecolor"))
        }
        #[cfg(not(test))]
        {
            palette_for(std::env::var("COLORTERM").ok().as_deref())
        }
    })
}

/// Pins the process palette; the first call wins and later calls are no-ops.
///
/// Startup and the golden tests use this to make rendering deterministic.
pub fn set_palette(palette: Palette) {
    let _ = PALETTE.set(palette);
}

/// Glyph color of one lifecycle status (session rows and pane titles).
pub fn status_color(status: Status) -> Color {
    let p = palette();
    match status {
        Status::Created | Status::Cancelled => p.gray,
        Status::Starting | Status::Running | Status::Cancelling => p.yellow,
        Status::Succeeded => p.green,
        Status::Failed | Status::TimedOut => p.red,
        Status::Lost => p.magenta,
    }
}

/// Color of the elapsed readout on one session row.
///
/// Live runs read as neutral; only outcomes carry color.
pub fn status_time_color(status: Status) -> Color {
    let p = palette();
    match status {
        Status::Succeeded => p.green,
        Status::Failed | Status::TimedOut => p.red,
        Status::Lost => p.magenta,
        _ => p.gray,
    }
}

/// Badge label, foreground, and tinted background for one status.
///
/// `spinner` supplies the animated glyph of a running session.
pub fn status_badge(status: Status, spinner: &str) -> (String, Color, Color) {
    let p = palette();
    match status {
        Status::Running => (format!(" {spinner} live "), p.accent, p.accent_bg),
        Status::Starting => (" ◐ starting ".into(), p.accent, p.accent_bg),
        Status::Created => (" ○ created ".into(), p.accent, p.accent_bg),
        Status::Cancelling => (" ◑ cancelling ".into(), p.accent, p.accent_bg),
        Status::Succeeded => (" ✓ done ".into(), p.green, p.gbg),
        Status::Failed => (" ✗ failed ".into(), p.red, p.rbg),
        Status::TimedOut => (" ◷ timed out ".into(), p.red, p.rbg),
        Status::Cancelled => (" ⊘ cancelled ".into(), p.gray, p.surf),
        Status::Lost => (" ◌ lost ".into(), p.magenta, p.mbg),
    }
}

/// Dim annotation style (timestamps, meta, hints).
pub fn dim() -> Style {
    Style::new().fg(palette().gray)
}

/// Emphasized key-label style used by hints, markers, and titles.
pub fn accent() -> Style {
    Style::new()
        .fg(palette().accent)
        .add_modifier(Modifier::BOLD)
}

/// Style of the selected message row.
pub fn selection() -> Style {
    Style::new().bg(palette().surf)
}

/// Subtle background highlighting the message under the mouse pointer.
pub fn hover() -> Style {
    Style::new().bg(palette().hov)
}

/// Style of a warning (stall silence, delivery trouble).
pub fn warning() -> Style {
    Style::new()
        .fg(palette().yellow)
        .add_modifier(Modifier::BOLD)
}

/// Style of failure text.
pub fn failure() -> Style {
    Style::new().fg(palette().red)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn palette_for_selects_truecolor_only_when_advertised() {
        assert!(palette_for(Some("truecolor")).truecolor);
        assert!(palette_for(Some("24bit")).truecolor);
        assert!(!palette_for(None).truecolor);
        assert!(!palette_for(Some("256color")).truecolor);
    }

    #[test]
    fn truecolor_palette_carries_the_design_values() {
        let p = palette_for(Some("truecolor"));
        assert_eq!(p.bg, Color::Rgb(15, 17, 21));
        assert_eq!(p.surf, Color::Rgb(29, 34, 43));
        assert_eq!(p.accent, Color::Rgb(92, 207, 230));
        assert_eq!(p.accent_bg, Color::Rgb(19, 42, 48));
        assert_eq!(p.gray, Color::Rgb(107, 114, 128));
    }

    #[test]
    fn fallback_keeps_selection_visible() {
        let p = palette_for(None);
        assert_ne!(p.surf, Color::Reset, "selection must stay visible");
        assert_eq!(p.fg, Color::Gray);
        assert_eq!(p.accent, Color::Cyan);
        assert_eq!(p.gray, Color::DarkGray);
    }
}
