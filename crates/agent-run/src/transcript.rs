//! Human-readable streaming rendering for the `transcript` viewer.
//!
//! This is a pure view over the existing transcript page JSON: it never reads
//! durable state and never defines new storage or event contracts. Journal
//! rows for one logical model message arrive as many small fragments that
//! share the journal's stream identity (`raw_ref`), so the renderer keeps
//! bounded cross-page state and emits sanitized fragments of the same item
//! immediately as raw chunks — the sink writes exactly the bytes it is given
//! and flushes, and the renderer itself owns every intentional newline, so a
//! fragment is visible on the viewer's pipe before the next page exists.
//! Every untrusted field (role, name, content) is sanitized, with
//! terminal-escape scanner state carried only across fragments of the same
//! journal item, so a split CSI/OSC sequence can never leak its payload as
//! controls or garbage.
//!
//! Upstream producer limitation, documented rather than patched here: the
//! Codex core producer journals `item/agentMessage/delta` fragments and
//! completion tails under one `itemId` `raw_ref`, so same-identity assistant
//! rows are fragments of one message. The Claude core producer instead
//! journals assistant deltas and completion tails with `raw_ref` set to `None`
//! and emits no message-boundary row, so consecutive assistant rows join as
//! one rendered item and no text-matching heuristic is invented to split
//! them.

use serde_json::Value;

use crate::Result;

/// Terminal-escape scanner state carried across fragments of one item.
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
/// identity (`raw_ref`, the Codex `itemId`) are emitted as sanitized raw
/// chunks with whitespace preserved, and any role or identity change closes
/// the current line so distinct messages and tool activity stay readable.
/// Only the current item's identity, its escape-scanner state, and whether
/// the sink sits on an open line are retained — never a text accumulator, so
/// a long streamed response is emitted fragment by fragment without
/// truncation or artificial line breaks.
#[derive(Default)]
pub struct Renderer {
    /// Whether an assistant item is currently being streamed.
    streamed_role: bool,
    /// `raw_ref` identity of the item currently streamed.
    identity: Option<String>,
    /// Incremental terminal-escape scanner state of the current item.
    escape: Escape,
    /// Whether emitted output currently ends without a newline.
    line_open: bool,
}

/// Streams whole pages through one renderer, closing the tail at the end.
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
    /// Renders one page's messages, emitting raw chunks to `emit`.
    ///
    /// `messages` are the JSON objects of a `TranscriptPage::messages`
    /// serialization (`seq`, `role`, `name`, `content`, `raw_ref`). Each
    /// fragment's visible text is sanitized and emitted before returning, so
    /// follow output is visible immediately; the renderer appends the one
    /// intentional newline at each role/item boundary itself, and
    /// [`Renderer::finish`] closes the tail of the last open line. Errors
    /// propagate from `emit` unchanged.
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
                // A real item boundary: close the prior row, discard any
                // escape sequence pending in it, and start fresh item state.
                self.close_line(emit)?;
                self.escape = Escape::default();
                match role {
                    "assistant" => {
                        self.streamed_role = true;
                        self.identity = identity;
                        self.stream(message["content"].as_str().unwrap_or(""), emit)?;
                    }
                    role => {
                        // Standalone rows never continue a streamed item and
                        // never leave one open behind them.
                        self.streamed_role = false;
                        self.identity = None;
                        self.standalone(
                            role,
                            message["name"].as_str(),
                            message["content"].as_str().unwrap_or(""),
                            emit,
                        )?;
                    }
                }
            }
        }
        Ok(())
    }

    /// Closes the tail line of a streamed item after the last page.
    ///
    /// Adds at most the one final newline when the last emitted chunk does
    /// not already end in one, then clears all streaming state.
    pub fn finish(&mut self, emit: &mut impl FnMut(&str) -> Result<()>) -> Result<()> {
        self.close_line(emit)?;
        self.streamed_role = false;
        self.identity = None;
        self.escape = Escape::default();
        Ok(())
    }

    /// Emits one sanitized fragment of the current streamed item.
    ///
    /// The fragment's visible characters are appended to one chunk and
    /// written before returning; journal rows are already bounded, so no
    /// buffering, watermark, or truncation applies. Whitespace inside the
    /// logical item is preserved exactly.
    fn stream(&mut self, content: &str, emit: &mut impl FnMut(&str) -> Result<()>) -> Result<()> {
        let mut chunk = String::with_capacity(content.len());
        for c in content.chars() {
            self.escape.push(c, &mut chunk);
        }
        if !chunk.is_empty() {
            self.line_open = !chunk.ends_with('\n');
            emit(&chunk)?;
        }
        Ok(())
    }

    /// Emits the renderer-owned newline when a row is still open.
    fn close_line(&mut self, emit: &mut impl FnMut(&str) -> Result<()>) -> Result<()> {
        if self.line_open {
            self.line_open = false;
            emit("\n")?;
        }
        Ok(())
    }

    /// Renders one non-streamed row (tool activity, results, user, unknown
    /// roles) as a single sanitized chunk ended by one newline.
    ///
    /// The role label, tool name, and content are all untrusted engine data
    /// and are each sanitized with fresh scanner state, so no prefix can
    /// bypass sanitization. The row is one already-bounded journal message;
    /// oversized content is emitted as-is rather than truncated.
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
            // Unknown role labels are untrusted data as well; the prefix is
            // sanitized so an embedded OSC cannot inject terminal controls.
            other => {
                raw.push_str(&sanitize(other));
                raw.push_str(": ");
            }
        }
        for c in content.chars() {
            self.escape.push(c, &mut raw);
        }
        let line = raw.trim_end();
        if !line.is_empty() {
            emit(&format!("{line}\n"))?;
        }
        self.line_open = false;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::{render_pages, sanitize, Renderer};
    use serde_json::{json, Value};

    /// Renders the given pages in order and returns the concatenated chunks.
    fn render(pages: &[&[Value]]) -> String {
        let mut raw = String::new();
        let mut renderer = Renderer::default();
        for page in pages {
            renderer
                .page(page, &mut |chunk: &str| {
                    raw.push_str(chunk);
                    Ok(())
                })
                .unwrap();
        }
        renderer
            .finish(&mut |chunk: &str| {
                raw.push_str(chunk);
                Ok(())
            })
            .unwrap();
        raw
    }

    /// Feeds pages one at a time, capturing the concatenated output observed
    /// after each page and after the final `finish`.
    ///
    /// This is the timing probe: the entry after page *n* is exactly what a
    /// live sink must have already written before page *n+1* exists.
    fn render_steps(pages: &[&[Value]]) -> Vec<String> {
        let mut seen = Vec::new();
        let mut raw = String::new();
        let mut renderer = Renderer::default();
        for page in pages {
            renderer
                .page(page, &mut |chunk: &str| {
                    raw.push_str(chunk);
                    Ok(())
                })
                .unwrap();
            seen.push(raw.clone());
        }
        renderer
            .finish(&mut |chunk: &str| {
                raw.push_str(chunk);
                Ok(())
            })
            .unwrap();
        seen.push(raw);
        seen
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

    /// Same-identity fragments are visible immediately, chunk by chunk.
    #[test]
    fn fragments_are_visible_before_the_next_page_arrives() {
        let rows = |content: &str| json!({"seq":0,"role":"assistant","content":content,"raw_ref":"same-message"});
        let steps = render_steps(&[&[rows("Hello")][..], &[rows(" ")][..], &[rows("world")][..]]);
        assert_eq!(steps[0], "Hello", "page 1 must emit before page 2 exists");
        assert_eq!(steps[1], "Hello ");
        assert_eq!(steps[2], "Hello world");
    }

    /// `finish` adds at most the one intended final newline.
    #[test]
    fn finish_adds_only_the_final_newline() {
        let rows =
            |content: &str| json!({"seq":0,"role":"assistant","content":content,"raw_ref":"m"});
        let steps = render_steps(&[&[rows("Hello")][..], &[rows(" world")][..]]);
        assert_eq!(steps[0], "Hello");
        assert_eq!(steps[1], "Hello world");
        assert_eq!(steps[2], "Hello world\n");
        // A stream that already ended in a newline gains nothing.
        let rows_nl =
            |content: &str| json!({"seq":0,"role":"assistant","content":content,"raw_ref":"m"});
        assert_eq!(
            render(&[&[rows_nl("done\n")][..], &[rows_nl("")][..]]),
            "done\n"
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
            "review this\none\ntwo\n-> shell {\"cmd\":\"ls\"}\n<- file.txt\n"
        );
    }

    /// A tool row between two assistant fragments of the same identity still
    /// starts a fresh rendered item on both sides of the boundary.
    #[test]
    fn tool_activity_between_same_identity_fragments_breaks_the_stream() {
        let pages = [&[
            json!({"seq":1,"role":"assistant","content":"part ","raw_ref":"m1"}),
            json!({"seq":2,"role":"tool_call","name":"shell","content":"ls"}),
            json!({"seq":3,"role":"assistant","content":"more","raw_ref":"m1"}),
        ][..]];
        assert_eq!(render(&pages), "part \n-> shell ls\nmore\n");
    }

    /// Escape sequences split across pages or fragments never leak payload.
    #[test]
    fn split_escape_sequences_across_pages_are_consumed() {
        let first = [json!({"seq":1,"role":"assistant","content":"bad \x1b[3","raw_ref":"m"})];
        let second =
            [json!({"seq":2,"role":"assistant","content":"1mred\x1b[0m ok","raw_ref":"m"})];
        assert_eq!(
            render(&[first.as_slice(), second.as_slice()]),
            "bad red ok\n"
        );
        let osc_a = [json!({"seq":1,"role":"assistant","content":"\x1b]0;tit","raw_ref":"m"})];
        let osc_b = [json!({"seq":2,"role":"assistant","content":"le\x1b\\after","raw_ref":"m"})];
        assert_eq!(render(&[osc_a.as_slice(), osc_b.as_slice()]), "after\n");
        // A CSI split inside its parameter bytes joins across fragments too.
        let csi_a = [json!({"seq":1,"role":"assistant","content":"x\x1b[3","raw_ref":"m"})];
        let csi_b = [json!({"seq":2,"role":"assistant","content":"8;5;196mred","raw_ref":"m"})];
        assert_eq!(render(&[csi_a.as_slice(), csi_b.as_slice()]), "xred\n");
    }

    /// A pending escape at a real item boundary is discarded, not carried.
    #[test]
    fn pending_escape_at_item_boundary_is_discarded() {
        let pages = [&[
            json!({"seq":1,"role":"assistant","content":"bad \x1b]0;ti","raw_ref":"m1"}),
            json!({"seq":2,"role":"tool_call","name":"shell","content":"ok"}),
            json!({"seq":3,"role":"assistant","content":"x","raw_ref":"m1"}),
        ][..]];
        assert_eq!(render(&pages), "bad \n-> shell ok\nx\n");
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
            "-> shell ok\nruntime_session: {\"id\":\"s1\"}\n"
        );
        // A hostile unknown-role label must not inject terminal controls.
        let hostile = [&[json!({
            "seq":1,
            "role":"\x1b]0;injected\x07evil",
            "content":"payload"
        })][..]];
        assert_eq!(render(&hostile), "evil: payload\n");
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
            "Hello \nworld\n"
        );
        assert!(render(&[
            &[json!({"seq":1,"role":"assistant","content":""})][..],
            &[json!({"seq":2,"role":"assistant","content":""})][..]
        ])
        .is_empty());
    }

    /// A response larger than the old flush watermark is neither truncated
    /// nor reflowed: every character is emitted, with no artificial newline.
    #[test]
    fn long_responses_stream_without_truncation_or_reflow() {
        let long = "a".repeat(20 * 1024);
        let rows =
            |content: &str| json!({"seq":0,"role":"assistant","content":content,"raw_ref":"m"});
        let first = [rows(&long)];
        let tail = [rows("!")];
        let steps = render_steps(&[first.as_slice(), tail.as_slice()]);
        assert_eq!(steps[0].chars().count(), 20 * 1024);
        assert_eq!(steps[0].matches('a').count(), 20 * 1024);
        assert!(!steps[0].contains('\n'), "no artificial line breaks");
        assert_eq!(steps[2], format!("{}!\n", "a".repeat(20 * 1024)));
    }

    /// The rendered page helper keeps the module surface honest.
    #[test]
    fn render_pages_helper_matches_manual_streaming() {
        let pages = [
            vec![json!({"seq":1,"role":"assistant","content":"Hel","raw_ref":"k"})],
            vec![json!({"seq":2,"role":"assistant","content":"lo","raw_ref":"k"})],
        ];
        let mut raw = String::new();
        render_pages(&pages, &mut |chunk: &str| {
            raw.push_str(chunk);
            Ok(())
        })
        .unwrap();
        assert_eq!(raw, "Hello\n");
    }
}
