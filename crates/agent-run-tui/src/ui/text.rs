//! Character-grid line assembly: exact-width fitting, left/right rows, and
//! word wrapping.
//!
//! The panes build every row from styled spans and then cut or pad it to the
//! exact cell width of its pane, mirroring how a terminal clips a row. All
//! width math uses terminal display cells and never splits a grapheme.

use ratatui::style::Style;
use ratatui::text::Span;

/// The terminal display width of one span.
fn span_width(span: &Span<'_>) -> usize {
    span.width()
}

/// Truncates spans to exactly `width` cells, padding the remainder.
///
/// A span that crosses the boundary is cut and terminated with `…`; the
/// padding span carries `pad` so row highlights fill their whole row.
pub fn fit(spans: Vec<Span<'static>>, width: usize, pad: Style) -> Vec<Span<'static>> {
    let mut out = Vec::with_capacity(spans.len() + 1);
    let mut used = 0usize;
    for span in spans {
        if used >= width {
            break;
        }
        let room = width - used;
        let span_w = span
            .styled_graphemes(Style::new())
            .map(|g| Span::raw(g.symbol).width())
            .scan(0usize, |used, cells| {
                *used += cells;
                Some(*used)
            })
            .find(|used| *used > room)
            .unwrap_or_else(|| span.width());
        if span_w <= room {
            out.push(span);
            used += span_w;
        } else {
            let mut cells = 0;
            let cut: String = span
                .styled_graphemes(Style::new())
                .take_while(|g| {
                    cells += Span::raw(g.symbol).width();
                    cells <= room.saturating_sub(1)
                })
                .map(|g| g.symbol)
                .collect();
            used += Span::raw(&cut).width() + 1;
            out.push(Span::styled(format!("{cut}…"), span.style));
        }
    }
    if used < width {
        out.push(Span::styled(" ".repeat(width - used), pad));
    }
    out
}

/// Left-aligned spans plus right-aligned spans on one row.
///
/// The right side wins the trailing cells; the left side is fitted into the
/// remaining width. Empty or oversized right sides drop out entirely.
pub fn lr(
    left: Vec<Span<'static>>,
    right: Vec<Span<'static>>,
    width: usize,
    pad: Style,
) -> Vec<Span<'static>> {
    let right_width: usize = right.iter().map(span_width).sum();
    if right_width == 0 || right_width >= width {
        return fit(left, width, pad);
    }
    let mut out = fit(left, width - right_width, pad);
    out.extend(right);
    out
}

/// Word-wraps one paragraph to `width`, preserving leading indentation.
///
/// Empty lines stay empty and overlong words are hard-cut at the width, with
/// continuation lines re-indented.
pub fn wrap(text: &str, width: usize) -> Vec<String> {
    wrap_first(text, width, width)
}

/// Word-wraps one paragraph with a narrower budget for its first wrapped
/// line, so a right-aligned annotation (a timestamp) fits beside it.
pub fn wrap_first(text: &str, first_width: usize, width: usize) -> Vec<String> {
    let mut state = Wrapper::new(first_width, width);
    state.push(text);
    state.finish();
    state.rows
}

/// Incremental word wrapper. Only an unfinished row and word are retained;
/// callers drain completed rows before feeding another delta. Indentation is
/// capped when it would consume the row; oversized graphemes become ellipses.
#[derive(Clone)]
pub(crate) struct Wrapper {
    /// Completed rows, drainable by the incremental renderer.
    pub(crate) rows: Vec<String>,
    /// Current row of completed words.
    line: String,
    /// Display cells in the current row.
    used: usize,
    /// Unfinished word, bounded by a row plus one grapheme.
    word: String,
    /// Display cells in the unfinished word.
    word_width: usize,
    /// Leading ASCII spaces for this paragraph.
    indent: usize,
    /// Whether the paragraph is still in its leading spaces.
    leading: bool,
    /// Whether the current row contains a word.
    has_word: bool,
    /// Whether any row has already been emitted.
    emitted: bool,
    /// First row's display budget.
    first_width: usize,
    /// Later rows' display budget.
    width: usize,
}

impl Wrapper {
    /// Creates an empty stream with positive cell budgets (zero means one).
    pub(crate) fn new(first_width: usize, width: usize) -> Self {
        Self {
            rows: Vec::new(),
            line: String::new(),
            used: 0,
            word: String::new(),
            word_width: 0,
            indent: 0,
            leading: true,
            has_word: false,
            emitted: false,
            first_width: first_width.max(1),
            width: width.max(1),
        }
    }

    /// Current row's cell budget, including indentation.
    fn budget(&self) -> usize {
        if self.emitted {
            self.width
        } else {
            self.first_width
        }
    }

    /// Initializes a continuation row with safe indentation.
    fn start(&mut self) {
        let indent = if self.indent >= self.budget().min(self.width) {
            self.indent.min(self.budget().min(self.width) / 2)
        } else {
            self.indent
        };
        self.line = " ".repeat(indent);
        self.used = indent;
        self.has_word = false;
    }

    /// Emits a completed row without copying any unconsumed text.
    fn emit(&mut self) {
        self.rows.push(std::mem::take(&mut self.line));
        self.emitted = true;
        self.start();
    }

    /// Commits the unfinished word, which always fits the current row.
    fn end_word(&mut self) {
        if self.word.is_empty() {
            return;
        }
        if self.has_word {
            self.line.push(' ');
            self.used += 1;
        }
        self.line.push_str(&self.word);
        self.used += self.word_width;
        self.word.clear();
        self.word_width = 0;
        self.has_word = true;
    }

    /// Consumes a delta once. Whitespace is normalized within paragraphs;
    /// newlines preserve empty paragraphs. Work is linear in input bytes.
    pub(crate) fn push(&mut self, text: &str) {
        let span = Span::raw(text);
        for grapheme in span.styled_graphemes(Style::new()) {
            let g = grapheme.symbol;
            if g.chars().all(char::is_whitespace) {
                self.end_word();
                if g.contains('\n') {
                    if self.has_word {
                        self.rows.push(std::mem::take(&mut self.line));
                    } else {
                        self.rows.push(String::new());
                    }
                    self.emitted = true;
                    self.indent = 0;
                    self.leading = true;
                    self.line.clear();
                    self.used = 0;
                    self.has_word = false;
                } else if self.leading && g == " " {
                    self.indent += 1;
                } else {
                    self.leading = false;
                }
                continue;
            }
            if self.leading || (self.line.is_empty() && !self.has_word) {
                self.start();
            }
            self.leading = false;
            let cells = Span::raw(g).width();
            let available = self
                .budget()
                .saturating_sub(self.used + usize::from(self.has_word));
            if self.word_width + cells > available && self.has_word {
                self.emit();
            }
            let available = self.budget().saturating_sub(self.used);
            if self.word_width + cells > available && !self.word.is_empty() {
                self.line.push_str(&self.word);
                self.word.clear();
                self.word_width = 0;
                self.emit();
            }
            let capacity = self.budget().saturating_sub(self.used);
            if cells > capacity {
                self.word.push('…');
                self.word_width += 1;
            } else {
                self.word.push_str(g);
                self.word_width += cells;
            }
        }
    }

    /// Finishes the stream, emitting its last row, including an empty one.
    pub(crate) fn finish(&mut self) {
        self.end_word();
        self.rows.push(if self.has_word {
            std::mem::take(&mut self.line)
        } else {
            String::new()
        });
    }

    /// Returns the bounded unfinished suffix without changing stream state.
    pub(crate) fn preview(&self) -> Vec<String> {
        let mut tail = self.clone();
        tail.rows.clear();
        tail.finish();
        tail.rows
    }
}

/// Escape scanner matching the CLI sanitization gate, with constant-size state
/// across deltas (including arbitrarily long CSI and OSC sequences).
#[derive(Default)]
pub(crate) struct Sanitizer {
    /// 0 text, 1 ESC, 2 CSI, 3 OSC, 4 OSC ESC, 5 intermediate ESC bytes.
    state: u8,
}

impl Sanitizer {
    /// Consumes raw untrusted text and returns only visible characters.
    pub(crate) fn push(&mut self, text: &str) -> String {
        let mut out = String::with_capacity(text.len());
        for c in text.chars() {
            self.state = match self.state {
                0 => match c {
                    '\n' | '\t' => {
                        out.push(c);
                        0
                    }
                    '\x1b' => 1,
                    c if c.is_control() => 0,
                    c => {
                        out.push(c);
                        0
                    }
                },
                1 => match c {
                    '[' => 2,
                    ']' => 3,
                    '\u{20}'..='\u{2f}' => 5,
                    _ => 0,
                },
                2 => {
                    if ('@'..='~').contains(&c) {
                        0
                    } else {
                        2
                    }
                }
                3 => match c {
                    '\x07' => 0,
                    '\x1b' => 4,
                    _ => 3,
                },
                4 => {
                    if c == '\\' {
                        0
                    } else {
                        3
                    }
                }
                _ => {
                    if ('\u{20}'..='\u{2f}').contains(&c) {
                        5
                    } else {
                        0
                    }
                }
            };
        }
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Asserts on the concatenated text and total width of fitted spans.
    fn text(spans: &[Span<'static>]) -> String {
        spans.iter().map(|s| s.content.clone()).collect()
    }

    #[test]
    fn fit_pads_short_rows_and_truncates_long_ones() {
        let plain = Style::new();
        let short = fit(vec![Span::raw("ab")], 5, plain);
        assert_eq!(text(&short), "ab   ");
        let long = fit(vec![Span::raw("abcdef")], 5, plain);
        assert_eq!(text(&long), "abcd…");
        // Truncation lands inside the span that crosses the boundary.
        let mixed = fit(vec![Span::raw("ab"), Span::raw("cdef")], 4, plain);
        assert_eq!(text(&mixed), "abc…");
    }

    #[test]
    fn lr_places_the_right_side_in_the_trailing_cells() {
        let plain = Style::new();
        let row = lr(vec![Span::raw("left")], vec![Span::raw("R")], 8, plain);
        assert_eq!(text(&row), "left   R");
        // An oversized right side falls back to the fitted left side.
        let dropped = lr(
            vec![Span::raw("left")],
            vec![Span::raw("012345678")],
            8,
            plain,
        );
        assert_eq!(text(&dropped), "left    ");
    }

    /// Deep indentation terminates, Unicode fits cells, and huge tokens stay linear.
    #[test]
    fn wrapping_is_bounded_and_cell_safe() {
        assert_eq!(wrap("界界", 2), vec!["界", "界"]);
        assert!(
            wrap("     x", 5)
                .iter()
                .all(|line| Span::raw(line).width() <= 5)
        );
        assert!(
            wrap("👩‍💻👩‍💻", 2)
                .iter()
                .all(|line| Span::raw(line).width() <= 2)
        );
        assert_eq!(text(&fit(vec![Span::raw("界界")], 3, Style::new())), "界…");
        let token = "x".repeat(1024 * 1024);
        let start = std::time::Instant::now();
        let rows = wrap(&token, 80);
        assert_eq!(rows.iter().map(String::len).sum::<usize>(), token.len());
        assert!(start.elapsed() < std::time::Duration::from_secs(5));
    }

    #[test]
    fn wrap_breaks_on_words_and_hard_cutting_overlong_runs() {
        let lines = wrap("alpha beta", 5);
        assert_eq!(lines, vec!["alpha", "beta"]);
        let hard = wrap("alphabet", 5);
        assert_eq!(hard, vec!["alpha", "bet"]);
        let indented = wrap("  alpha beta", 9);
        assert_eq!(indented, vec!["  alpha", "  beta"]);
        assert_eq!(wrap("", 5), vec![String::new()]);
    }
}
