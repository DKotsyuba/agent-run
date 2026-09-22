//! Human-readable streaming rendering for the `transcript` viewer.
//!
//! This is a pure view over the existing transcript page JSON: it never reads
//! durable state and never defines new storage or event contracts. Journal
//! rows for one logical model message arrive as many small fragments that
//! share the journal's stream identity (`raw_ref`), so the renderer keeps
//! bounded cross-page state and appends fragments of the same item
//! continuously instead of printing one row per line. Every untrusted field
//! (role, name, content) is sanitized with terminal-escape state carried
//! across fragments and pages, so a split CSI/OSC sequence can never leak its
//! payload as controls or garbage.

use serde_json::Value;

use crate::Result;

/// Maximum characters held in the streaming buffer before it is flushed as a
/// rendered line.
///
/// The journal already bounds each row to 16 KiB; flushing at this watermark
/// keeps viewer memory bounded without truncating a long streamed model
/// response — content simply continues on the next rendered line.
const FLUSH_CHARS: usize = 8 * 1024;

/// Maximum rendered characters for one standalone (non-streamed) row.
const MAX_ROW_CHARS: usize = 8 * 1024;

/// Terminal-escape scanner state carried across fragments and pages.
#[derive(Default)]
struct Escape {
    /// Where the scanner currently sits inside a possibly split sequence.
    state: State,
}

/// States of the incremental ANSI escape scanner.
#[derive(Default, PartialEq)]
enum State {
    /// Ordinary text.
    #[default]
    Text,
    /// One ESC seen; the next byte decides the sequence kind.
    Esc,
    /// ESC [ ... : consume until a final byte in `@`..`~`.
    Csi,
    /// ESC ] ... : consume until BEL or ST (ESC \).
    Osc,
    /// Inside an OSC string, one ESC seen; only `\` may close ST.
    OscEsc,
    /// ESC followed by intermediate bytes `20`..`2F`; next byte ends it.
    EscIntermediate,
}

impl Escape {
    /// Feeds one character, appending whatever belongs in visible text.
    fn push(&mut self, c: char, out: &mut String) {
        self.state = match self.state {
            State::Text => match c {
                '\n' | '\t' => {
                    out.push(c);
                    State::Text
                }
                '\x1b' => State::Esc,
                c if c.is_control() => State::Text,
                c => {
                    out.push(c);
                    State::Text
                }
            },
            State::Esc => match c {
                '[' => State::Csi,
                ']' => State::Osc,
                c if ('\u{20}'..='\u{2f}').contains(&c) => State::EscIntermediate,
                // Two-character escape: consumed, nothing becomes visible.
                _ => State::Text,
            },
            State::Csi => {
                if ('@'..='~').contains(&c) {
                    State::Text
                } else {
                    State::Csi
                }
            }
            State::Osc => match c {
                '\x07' => State::Text,
                '\x1b' => State::OscEsc,
                _ => State::Osc,
            },
            // An ESC inside an OSC that is not ST aborts the string; both the
            // ESC and this character stay consumed rather than leaking.
            State::OscEsc => {
                if c == '\\' {
                    State::Text
                } else {
                    State::Osc
                }
            }
            State::EscIntermediate => {
                if ('\u{20}'..='\u{2f}').contains(&c) {
                    State::EscIntermediate
                } else {
                    State::Text
                }
            }
        };
    }
}

/// Removes terminal control sequences from one standalone string.
///
/// Equivalent to streaming the text through [`Renderer`] state; used where no
/// cross-fragment context exists.
pub fn sanitize(text: &str) -> String {
    let mut escape = Escape::default();
    let mut out = String::with_capacity(text.len());
    for c in text.chars() {
        escape.push(c, &mut out);
    }
    out
}

/// Bounded cross-page streaming state of the transcript viewer.
///
/// One renderer lives for the whole viewer invocation (including follow
/// polls): consecutive `assistant` rows that share the journal's stream
/// identity (`raw_ref`, the Codex `itemId`) are appended continuously with
/// whitespace preserved, and any role or identity change closes the current
/// line so distinct messages and tool activity stay readable. Only the
/// current item's identity, escape state, and one bounded text buffer are
/// retained — never an unbounded message accumulator.
#[derive(Default)]
pub struct Renderer {
    /// Role of the assistant item currently streamed, if any.
    streamed_role: bool,
    /// `raw_ref` identity of the item currently streamed.
    identity: Option<String>,
    /// Incremental terminal-escape scanner state.
    escape: Escape,
    /// Buffered visible text of the current streamed item.
    buffer: String,
    /// Cached `buffer.chars().count()` so streaming stays linear.
    buffer_chars: usize,
}

/// Streams whole pages through one renderer, flushing the tail at the end.
///
/// Convenience for invocations that already hold every page in memory.
pub fn render_pages(pages: &[Vec<Value>], emit: &mut impl FnMut(&str) -> Result<()>) -> Result<()> {
    let mut renderer = Renderer::default();
    for page in pages {
        renderer.page(page, emit)?;
    }
    renderer.finish(emit)
}

impl Renderer {
    /// Renders one page's messages, emitting finished lines to `emit`.
    ///
    /// `messages` are the JSON objects of a `TranscriptPage::messages`
    /// serialization (`seq`, `role`, `name`, `content`, `raw_ref`). Lines are
    /// emitted as soon as item boundaries or the flush watermark are reached
    /// so follow output stays live; call [`Renderer::finish`] after the last
    /// page to flush the tail of the current item. Errors propagate from
    /// `emit` unchanged.
    pub fn page(
        &mut self,
        messages: &[Value],
        emit: &mut impl FnMut(&str) -> Result<()>,
    ) -> Result<()> {
        for message in messages {
            let role = message["role"].as_str().unwrap_or("message");
            let identity = message["raw_ref"].as_str().map(str::to_owned);
            // A journal stream identity joins consecutive assistant rows; the
            // Codex producer journals delta fragments and completion tails of
            // one agentMessage under the same itemId. Any other role, or a
            // changed/absent identity, starts a new rendered item.
            let continues = role == "assistant" && self.streamed_role && identity == self.identity;
            if continues {
                self.stream(message["content"].as_str().unwrap_or(""), emit)?;
            } else {
                self.flush(emit)?;
                match role {
                    "assistant" => {
                        self.streamed_role = true;
                        self.identity = identity;
                        self.stream(message["content"].as_str().unwrap_or(""), emit)?;
                    }
                    role => self.standalone(
                        role,
                        message["name"].as_str(),
                        message["content"].as_str().unwrap_or(""),
                        emit,
                    )?,
                }
            }
        }
        Ok(())
    }

    /// Flushes the tail of a streamed item after the last page.
    pub fn finish(&mut self, emit: &mut impl FnMut(&str) -> Result<()>) -> Result<()> {
        self.flush(emit)?;
        self.streamed_role = false;
        self.identity = None;
        Ok(())
    }

    /// Appends sanitized content of the current streamed item to the buffer,
    /// emitting complete lines at the flush watermark without truncation.
    fn stream(&mut self, content: &str, emit: &mut impl FnMut(&str) -> Result<()>) -> Result<()> {
        for c in content.chars() {
            let mut next = std::mem::take(&mut self.buffer);
            self.escape.push(c, &mut next);
            self.buffer = next;
            self.buffer_chars += 1;
            if self.buffer_chars >= FLUSH_CHARS {
                let line = std::mem::take(&mut self.buffer);
                self.buffer_chars = 0;
                emit(&line)?;
            }
        }
        Ok(())
    }

    /// Emits buffered streamed text, preserving meaningful whitespace.
    fn flush(&mut self, emit: &mut impl FnMut(&str) -> Result<()>) -> Result<()> {
        if !self.buffer.is_empty() {
            let line = std::mem::take(&mut self.buffer);
            self.buffer_chars = 0;
            emit(&line)?;
        }
        Ok(())
    }

    /// Renders one non-streamed row (tool activity, results, user, unknown
    /// roles) as a standalone line, truncating oversized content.
    fn standalone(
        &mut self,
        role: &str,
        name: Option<&str>,
        content: &str,
        emit: &mut impl FnMut(&str) -> Result<()>,
    ) -> Result<()> {
        let mut raw = String::new();
        match role {
            "user" => {}
            "tool_call" => {
                // Names are untrusted engine data too; sanitize before use.
                let name = sanitize(name.unwrap_or("tool"));
                raw.push_str("-> ");
                raw.push_str(&name);
                raw.push(' ');
            }
            "tool_result" => raw.push_str("<- "),
            other => {
                raw.push_str(other);
                raw.push_str(": ");
            }
        }
        let mut count = raw.chars().count();
        for c in content.chars() {
            self.escape.push(c, &mut raw);
            count += 1;
            if count >= MAX_ROW_CHARS {
                break;
            }
        }
        let line = raw.trim_end();
        if !line.is_empty() {
            if count >= MAX_ROW_CHARS {
                emit(&format!("{line}\n…[truncated]"))?;
            } else {
                emit(line)?;
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::{render_pages, sanitize, Renderer};
    use serde_json::{json, Value};

    /// Renders the given pages in order and collects the emitted lines.
    fn render(pages: &[&[Value]]) -> Vec<String> {
        let mut lines = Vec::new();
        let mut renderer = Renderer::default();
        for page in pages {
            renderer
                .page(page, &mut |line: &str| {
                    lines.push(line.to_owned());
                    Ok(())
                })
                .unwrap();
        }
        renderer
            .finish(&mut |line: &str| {
                lines.push(line.to_owned());
                Ok(())
            })
            .unwrap();
        lines
    }

    /// Control sequences are removed without leaking escape payloads.
    #[test]
    fn sanitize_strips_escape_and_control_sequences() {
        assert_eq!(sanitize("\x1b[31mred\x1b[0m"), "red");
        assert_eq!(sanitize("\x1b]0;title\x07after"), "after");
        assert_eq!(sanitize("\x1b]0;t\x1b\\end"), "end");
        assert_eq!(sanitize("\x1b(Bx"), "x");
        assert_eq!(sanitize("a\x07\x08b\x7f"), "ab");
        assert_eq!(sanitize("keep\nline\ttabs"), "keep\nline\ttabs");
        assert_eq!(sanitize("\x1b"), "");
        assert_eq!(sanitize("crlf\r\n"), "crlf\n");
    }

    /// Same-identity fragments stream continuously, whitespace included.
    #[test]
    fn fragments_of_one_message_stream_continuously() {
        let rows = |content: &str| json!({"seq":0,"role":"assistant","content":content,"raw_ref":"same-message"});
        let hello = [rows("Hello")];
        let space = [rows(" ")];
        let world = [rows("world")];
        assert_eq!(
            render(&[hello.as_slice(), space.as_slice(), world.as_slice()]),
            vec!["Hello world"]
        );
    }

    /// Role, tool, and identity changes keep readable item boundaries.
    #[test]
    fn boundaries_separate_messages_and_tool_activity() {
        let pages = [&[
            json!({"seq":1,"role":"user","content":"review this"}),
            json!({"seq":2,"role":"assistant","content":"one","raw_ref":"item-a"}),
            json!({"seq":3,"role":"assistant","content":"two","raw_ref":"item-b"}),
            json!({"seq":4,"role":"tool_call","name":"shell","content":"{\"cmd\":\"ls\"}"}),
            json!({"seq":5,"role":"tool_result","content":"file.txt"}),
        ][..]];
        assert_eq!(
            render(&pages),
            vec![
                "review this",
                "one",
                "two",
                "-> shell {\"cmd\":\"ls\"}",
                "<- file.txt",
            ]
        );
    }

    /// Escape sequences split across pages or fragments never leak payload.
    #[test]
    fn split_escape_sequences_across_pages_are_consumed() {
        let first = [json!({"seq":1,"role":"assistant","content":"bad \x1b[3"})];
        let second = [json!({"seq":2,"role":"assistant","content":"1mred\x1b[0m ok"})];
        assert_eq!(
            render(&[first.as_slice(), second.as_slice()]),
            vec!["bad red ok"]
        );
        let osc_a = [json!({"seq":1,"role":"assistant","content":"\x1b]0;tit"})];
        let osc_b = [json!({"seq":2,"role":"assistant","content":"le\x1b\\after"})];
        assert_eq!(render(&[osc_a.as_slice(), osc_b.as_slice()]), vec!["after"]);
    }

    /// Tool names and unknown roles are sanitized and still rendered.
    #[test]
    fn untrusted_name_and_unknown_role_are_sanitized() {
        let pages = [&[
            json!({"seq":1,"role":"tool_call","name":"\x1b]0;injected\x07shell","content":"ok"}),
            json!({"seq":2,"role":"runtime_session","content":"{\"id\":\"s1\"}\x1b[31m"}),
        ][..]];
        assert_eq!(
            render(&pages),
            vec!["-> shell ok", "runtime_session: {\"id\":\"s1\"}",]
        );
    }

    /// Whitespace-only fragments survive and empty content renders nothing.
    #[test]
    fn whitespace_fragments_are_meaningful() {
        let rows =
            |content: &str| json!({"seq":0,"role":"assistant","content":content,"raw_ref":"m"});
        let a = [rows("Hello")];
        let b = [rows(" ")];
        let c = [rows("\n")];
        let d = [rows("world")];
        assert_eq!(
            render(&[a.as_slice(), b.as_slice(), c.as_slice(), d.as_slice()]),
            vec!["Hello \nworld"]
        );
        assert!(render(&[
            &[json!({"seq":1,"role":"assistant","content":""})][..],
            &[json!({"seq":2,"role":"assistant","content":""})][..]
        ])
        .is_empty());
    }

    /// The rendered page helper keeps the module surface honest.
    #[test]
    fn render_pages_helper_matches_manual_streaming() {
        let pages = [
            vec![json!({"seq":1,"role":"assistant","content":"Hel","raw_ref":"k"})],
            vec![json!({"seq":2,"role":"assistant","content":"lo","raw_ref":"k"})],
        ];
        let mut lines = Vec::new();
        render_pages(&pages, &mut |line: &str| {
            lines.push(line.to_owned());
            Ok(())
        })
        .unwrap();
        assert_eq!(lines, vec!["Hello"]);
    }
}
