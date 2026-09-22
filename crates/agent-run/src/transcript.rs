//! Human-readable transcript rendering for the `transcript` viewer.
//!
//! This is a pure view over the existing transcript page JSON: it never reads
//! durable state and never defines new storage or event contracts. Model text,
//! tool activity, and tool results are rendered from the roles the committed
//! journal already stores (`user`, `assistant`, `tool_call` with a tool name,
//! `tool_result`, plus opaque future roles). Every rendered line is sanitized
//! so hostile or accidental terminal control sequences in journaled text can
//! never reprogram the operator's terminal.

use serde_json::Value;

/// Maximum rendered characters taken from one message's content.
///
/// The journal already stores text in 16 KiB chunks, but a long tool result can
/// still span many chunks; the viewer stays bounded by truncating per message
/// instead of buffering unbounded output in a terminal.
const MAX_CONTENT_CHARS: usize = 8 * 1024;

/// Removes terminal control sequences from journaled text.
///
/// Strips C0 controls except newline and tab, DEL, all C1 controls, and ANSI
/// escape sequences (CSI through its final byte, OSC through BEL or ST, and
/// two-character escapes) so the payload of an escape can never leak through
/// as visible garbage or an active sequence.
pub fn sanitize(text: &str) -> String {
    let mut out = String::with_capacity(text.len());
    let mut chars = text.chars().peekable();
    while let Some(c) = chars.next() {
        match c {
            '\n' | '\t' => out.push(c),
            '\x1b' => match chars.peek() {
                Some('[') => {
                    chars.next();
                    // CSI: consume through the first final byte in @..~.
                    while let Some(&n) = chars.peek() {
                        chars.next();
                        if ('@'..='~').contains(&n) {
                            break;
                        }
                    }
                }
                Some(']') => {
                    chars.next();
                    // OSC: consume through BEL or ESC \ (ST).
                    while let Some(n) = chars.next() {
                        if n == '\x07' {
                            break;
                        }
                        if n == '\x1b' && chars.peek() == Some(&'\\') {
                            chars.next();
                            break;
                        }
                    }
                }
                Some(_) => {
                    // Escape with intermediate bytes such as ESC ( B: consume
                    // intermediates through the final byte.
                    while let Some(&n) = chars.peek() {
                        chars.next();
                        if !('\x20'..='\x2f').contains(&n) {
                            break;
                        }
                    }
                }
                None => {}
            },
            c if c.is_control() => {}
            c => out.push(c),
        }
    }
    out
}

/// Renders one page's transcript messages as human-readable text lines.
///
/// `messages` are the JSON objects of a [`agent_run_domain`]
/// `TranscriptPage::messages` serialization (`seq`, `role`, `name`,
/// `content`). Assistant and user text is printed as-is; `tool_call` messages
/// render as `-> name(args)` using the journaled tool name; `tool_result`
/// messages render as `<- result`. Unknown roles fall back to `role: content`
/// so committed journals remain readable without inventing new contracts.
/// Every line is control-sanitized and content is truncated to
/// [`MAX_CONTENT_CHARS`].
pub fn render(messages: &[Value]) -> Vec<String> {
    messages
        .iter()
        .filter_map(|message| {
            let content = message["content"].as_str().unwrap_or("");
            let line = match message["role"].as_str() {
                Some("assistant") | Some("user") => sanitize(content),
                Some("tool_call") => {
                    let name = message["name"].as_str().unwrap_or("tool");
                    format!("-> {} {}", name, sanitize(content))
                }
                Some("tool_result") => format!("<- {}", sanitize(content)),
                other => format!("{}: {}", other.unwrap_or("message"), sanitize(content)),
            };
            let line = line.trim_end();
            if line.is_empty() {
                None
            } else {
                Some(truncate(line, message["seq"].as_i64().unwrap_or(0)))
            }
        })
        .collect()
}

/// Truncates one rendered line at [`MAX_CONTENT_CHARS`] characters.
fn truncate(line: &str, seq: i64) -> String {
    if line.chars().count() <= MAX_CONTENT_CHARS {
        return line.to_owned();
    }
    let cut = line
        .char_indices()
        .nth(MAX_CONTENT_CHARS)
        .map(|(i, _)| i)
        .unwrap_or(line.len());
    format!("{}\n…[truncated at seq {}]", &line[..cut], seq)
}

#[cfg(test)]
mod tests {
    use super::{render, sanitize, MAX_CONTENT_CHARS};
    use serde_json::json;

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

    /// Roles render distinctly and unknown roles fall back without loss.
    #[test]
    fn render_formats_roles_and_sanitizes() {
        let lines = render(&[
            json!({"seq":1,"role":"user","content":"review \x1b[1mthis\x1b[0m"}),
            json!({"seq":2,"role":"assistant","content":"looking"}),
            json!({"seq":3,"role":"tool_call","name":"shell","content":"{\"cmd\":\"ls\"}"}),
            json!({"seq":4,"role":"tool_result","content":"file.txt\x1b[0m"}),
            json!({"seq":5,"role":"runtime_session","content":"{\"id\":\"s1\"}"}),
        ]);
        assert_eq!(
            lines,
            vec![
                "review this",
                "looking",
                "-> shell {\"cmd\":\"ls\"}",
                "<- file.txt",
                "runtime_session: {\"id\":\"s1\"}",
            ]
        );
    }

    /// Empty content and whitespace-only messages render nothing.
    #[test]
    fn render_skips_empty_messages() {
        assert!(render(&[
            json!({"seq":1,"role":"assistant","content":""}),
            json!({"seq":2,"role":"assistant","content":"  \n"}),
        ])
        .is_empty());
    }

    /// Oversized content is truncated to a bounded number of characters.
    #[test]
    fn render_truncates_oversized_content() {
        let lines = render(&[json!({
            "seq": 9,
            "role": "tool_result",
            "content": "x".repeat(MAX_CONTENT_CHARS * 2),
        })]);
        assert_eq!(lines.len(), 1);
        assert!(lines[0].contains("…[truncated at seq 9]"));
        assert!(lines[0].chars().count() < MAX_CONTENT_CHARS * 2);
    }
}
