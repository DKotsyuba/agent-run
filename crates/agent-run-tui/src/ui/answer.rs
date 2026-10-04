//! The answer popup: the sealed, verified answer of one session.

use super::{overlay, theme};
use crate::app::App;
use ratatui::{
    layout::Rect,
    style::Style,
    text::{Line, Span},
    widgets::Paragraph,
    Frame,
};

/// Renders the answer popup centered over the current screen, when open.
pub fn render(f: &mut Frame, app: &App, area: Rect) {
    if app.answer.is_none() {
        return;
    }
    let width = area.width.saturating_mul(70) / 100;
    let height = area.height.saturating_mul(70) / 100;
    let inner = overlay::begin(f, area, "sealed answer", width, height);

    let mut cache = app.answer_lines.borrow_mut();
    cache.sync(inner.width);
    f.render_widget(&cache.paragraph, inner);
}

/// Builds owned, sanitized answer lines once on receipt; the frame borrows them.
fn lines(answer: &agent_run_domain::views::AnswerView) -> Vec<Line<'static>> {
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
    lines
}

/// Sanitized answer source and width-specific wrapped rows, shared by redraws.
#[derive(Default)]
pub(crate) struct Cache {
    /// Source lines, sanitized only on envelope receipt.
    source: Vec<Line<'static>>,
    /// Width whose wrapping the paragraph owns; zero means not built yet.
    width: u16,
    /// Already wrapped lines; rendering borrows without cloning answer bytes.
    paragraph: Paragraph<'static>,
    /// Wrapping builds, checked by the redraw regression.
    #[cfg(test)]
    pub(crate) builds: usize,
}

impl Cache {
    /// Sanitizes one received answer; wrapping waits for the actual popup width.
    pub(crate) fn new(answer: &agent_run_domain::views::AnswerView) -> Self {
        Self {
            source: lines(answer),
            ..Self::default()
        }
    }

    /// Wraps once per positive popup width. Repeated redraws touch no source bytes.
    fn sync(&mut self, width: u16) {
        let width = width.max(1);
        if self.width == width {
            return;
        }
        self.width = width;
        let mut rows = Vec::new();
        for line in &self.source {
            if line.width() <= usize::from(width) {
                rows.push(line.clone());
            } else {
                let style = line
                    .spans
                    .first()
                    .map(|span| span.style)
                    .unwrap_or_default();
                rows.extend(
                    super::text::wrap(&line.to_string(), usize::from(width))
                        .into_iter()
                        .map(|text| Line::styled(text, style)),
                );
            }
        }
        self.paragraph = Paragraph::new(rows);
        #[cfg(test)]
        {
            self.builds += 1;
        }
    }
}
