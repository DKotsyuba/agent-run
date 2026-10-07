//! Shared test fixtures for reducer and rendering tests.

use agent_run_domain::AgentView;
use serde_json::json;

/// Pins the truecolor palette so golden rendering tests are independent of
/// the running environment's `COLORTERM`.
pub fn force_truecolor() {
    crate::ui::theme::set_palette(crate::ui::theme::palette_for(Some("truecolor")));
}

/// Builds one session view from a sparse fixture with valid identities.
///
/// Ids must already be valid stable ids (`ag-YYYYMMDD-HHMMSS-<10 lowercase hex>`).
pub fn agent_view(stable_id: &str, run_id: &str, status: &str) -> AgentView {
    serde_json::from_value(json!({
        "agent_id": stable_id,
        "run_id": run_id,
        "runtime": "codex",
        "model": "gpt-5",
        "profile": "default",
        "task_summary": "ship the thing",
        "status": status,
        "created_at": 1.0,
        "started_at": 1.0,
        "finished_at": None::<f64>,
        "elapsed_seconds": 42.0,
        "last_progress_at": None::<f64>,
        "silence_seconds": Some(3.0),
        "warned": false,
        "failure_kind": None::<String>,
        "failure_text": None::<String>,
        "answer_available": false,
        "answer_bytes": None::<u64>,
        "answer_sha256": None::<String>,
        "effort": None::<String>,
        "delivery": {
            "agent_id": stable_id,
            "bound": false,
            "orchestrator_session_id": None::<String>,
            "notification_id": None::<String>,
            "state": "idle",
            "attempts": 0,
            "ambiguous": false,
            "last_error": None::<String>,
            "last_attempt": None::<serde_json::Value>,
        },
        "parent_agent_id": None::<String>,
        "root_agent_id": stable_id,
        "sequence": 1,
        "cleanup": None::<serde_json::Value>,
        "policy": None::<serde_json::Value>,
        "phase": "running",
        "phase_started_at": 1.0,
        "process_state": "alive",
        "observed_at": 1.0,
        "runtime_outcome": None::<String>,
        "acceptance": "pending",
    }))
    .expect("fixture parses")
}

/// Builds one transcript message fixture.
pub fn message(seq: i64, role: &str, content: &str) -> agent_run_domain::MessageView {
    serde_json::from_value(json!({
        "seq": seq,
        "at": 100.0 + seq as f64,
        "role": role,
        "name": None::<String>,
        "content": content,
        "raw_ref": None::<String>,
    }))
    .expect("message fixture parses")
}

/// Message fixtures of the realistic live transcript the timing probes use:
/// 500 rounds of one `Read` call, its 16 KiB result paired by `raw_ref`, and
/// six streamed assistant deltas — 4000 messages, about 8 MB of content.
///
/// Returns the JSON messages in sequence order (sequences start at 1) and
/// the last sequence, so probes can append past it.
pub fn probe_messages() -> (Vec<serde_json::Value>, i64) {
    const KIB16: usize = 16 * 1024;
    let mut messages = Vec::new();
    let mut seq = 0i64;
    for round in 0..500 {
        seq += 1;
        messages.push(json!({
            "seq": seq, "at": 100.0 + seq as f64, "role": "tool_call", "name": "Read",
            "content": "{\"file_path\":\"src/big.rs\"}", "raw_ref": format!("call-{round}"),
        }));
        seq += 1;
        messages.push(json!({
            "seq": seq, "at": 100.0 + seq as f64, "role": "tool_result", "name": null,
            "content": "x".repeat(KIB16), "raw_ref": format!("call-{round}"),
        }));
        for _ in 0..6 {
            seq += 1;
            messages.push(json!({
                "seq": seq, "at": 100.0 + seq as f64, "role": "assistant", "name": null,
                "content": format!("delta {round} "), "raw_ref": null,
            }));
        }
    }
    (messages, seq)
}

/// Terminal writer that discards output and counts its bytes into a shared
/// counter, so a `CrosstermBackend` over it measures exactly what a frame
/// would write to the tty (cell diffs, style changes, cursor moves).
#[derive(Debug)]
pub struct CountingWriter {
    /// Bytes written; shared with the caller, who may read and reset it.
    bytes: std::rc::Rc<std::cell::Cell<usize>>,
}

impl std::io::Write for CountingWriter {
    /// Counts and accepts the whole buffer; never fails.
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.bytes.set(self.bytes.get() + buf.len());
        Ok(buf.len())
    }

    /// Nothing is buffered; always succeeds.
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

/// A fixed-size terminal of `width` × `height` cells over a byte-counting
/// crossterm backend, plus the shared byte counter; the fixed viewport never
/// queries a real tty.
pub fn counting_terminal(
    width: u16,
    height: u16,
) -> (
    ratatui::Terminal<ratatui::backend::CrosstermBackend<CountingWriter>>,
    std::rc::Rc<std::cell::Cell<usize>>,
) {
    let bytes = std::rc::Rc::new(std::cell::Cell::new(0));
    let terminal = ratatui::Terminal::with_options(
        ratatui::backend::CrosstermBackend::new(CountingWriter {
            bytes: std::rc::Rc::clone(&bytes),
        }),
        ratatui::TerminalOptions {
            viewport: ratatui::Viewport::Fixed(ratatui::layout::Rect::new(0, 0, width, height)),
        },
    )
    .expect("fixed terminal builds");
    (terminal, bytes)
}
