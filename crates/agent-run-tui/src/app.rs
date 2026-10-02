//! Application state and pure reducers for the terminal observer.
//!
//! Everything the network workers deliver lands here first; rendering reads
//! the state without mutating it, and every reducer is synchronous and
//! unit-tested. The observer never mutates broker state on its own.

use agent_run_domain::domain::{AgentId, Status};
use agent_run_domain::{AgentView, AnswerView, MessageView, TranscriptPage};
use std::collections::BTreeMap;

/// Which full-screen view is on display.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Screen {
    /// The sortable session table.
    Sessions,
    /// One selected session's transcript.
    Transcript,
}

/// Connection health of the resident broker link, shown in the status bar.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Link {
    /// At least one successful broker round trip so far.
    Up,
    /// The latest broker round trip failed; watchers are retrying.
    Down,
}

/// Seconds of transcript silence before the silence column turns into a warning.
pub const SILENCE_WARN_SECONDS: f64 = 120.0;

/// Buffered transcript of one selected session.
pub struct TranscriptBuffer {
    /// Session snapshot as selected; refreshed whenever the sessions page updates.
    pub agent: AgentView,
    /// Messages ordered by their immutable sequence cursor.
    pub messages: Vec<MessageView>,
    /// Deduplicated merge buffer keyed by `seq`.
    seen: BTreeMap<i64, MessageView>,
    /// Cursor for the next transcript fetch.
    pub next_cursor: i64,
    /// Whether the full known history is buffered (`complete` page observed).
    pub history_complete: bool,
    /// Whether the view sticks to the tail on new content.
    pub follow: bool,
    /// Rendered lines below the viewport while the user scrolled away from the tail.
    pub from_bottom: u16,
    /// Message index under the transcript cursor (moved by Up/Down, click).
    pub cursor: usize,
    /// Message index under the mouse pointer, for hover styling.
    pub hover: Option<usize>,
    /// Sequences of collapsed messages the operator expanded (tool calls/results).
    pub expanded: std::collections::BTreeSet<i64>,
}

impl TranscriptBuffer {
    /// Opens a fresh buffer for one session; history is refetched from the start.
    pub fn open(agent: AgentView) -> Self {
        Self {
            agent,
            messages: Vec::new(),
            seen: BTreeMap::new(),
            next_cursor: 0,
            history_complete: false,
            follow: true,
            from_bottom: 0,
            cursor: 0,
            hover: None,
            expanded: std::collections::BTreeSet::new(),
        }
    }

    /// Merges one page into the buffer, dropping duplicates by sequence cursor.
    pub fn merge(&mut self, page: &TranscriptPage) {
        for message in &page.messages {
            self.seen.insert(message.seq, message.clone());
        }
        self.messages = self.seen.values().cloned().collect();
        if let Some(next_cursor) = page.next_cursor {
            self.next_cursor = next_cursor;
        }
        if page.complete {
            self.history_complete = true;
        }
        self.cursor = self.cursor.min(self.messages.len().saturating_sub(1));
    }

    /// Whether one message is currently expanded.
    pub fn is_expanded(&self, seq: i64) -> bool {
        self.expanded.contains(&seq)
    }

    /// Flips the expansion of one message sequence.
    pub fn toggle_expanded(&mut self, seq: i64) {
        if !self.expanded.remove(&seq) {
            self.expanded.insert(seq);
        }
    }

    /// Expands or collapses whatever message the cursor points at.
    pub fn toggle_expanded_at_cursor(&mut self) -> Option<i64> {
        let seq = self.messages.get(self.cursor)?.seq;
        self.toggle_expanded(seq);
        Some(seq)
    }

    /// Scrolls one step away from the tail; leaves follow mode.
    pub fn scroll_up(&mut self, lines: u16) {
        self.follow = false;
        self.from_bottom = self.from_bottom.saturating_add(lines);
    }

    /// Scrolls one step toward the tail; re-enters follow mode at the tail.
    pub fn scroll_down(&mut self, lines: u16) {
        if self.from_bottom > lines {
            self.from_bottom -= lines;
        } else {
            self.follow = true;
            self.from_bottom = 0;
        }
    }

    /// Jumps to the top, leaving follow mode.
    pub fn scroll_top(&mut self) {
        self.follow = false;
        self.from_bottom = u16::MAX;
    }

    /// Jumps to the tail and re-enters follow mode.
    pub fn scroll_bottom(&mut self) {
        self.follow = true;
        self.from_bottom = 0;
    }

    /// Toggles follow mode; enabling it returns to the tail.
    pub fn toggle_follow(&mut self) {
        if self.follow {
            self.follow = false;
        } else {
            self.scroll_bottom();
        }
    }
}

/// Full terminal state; rendering is a pure function of this value.
pub struct App {
    /// Which screen is on display.
    pub screen: Screen,
    /// Latest session snapshots, sorted by [`sort_sessions`].
    pub sessions: Vec<AgentView>,
    /// Last committed store revision observed from the broker.
    pub revision: Option<i64>,
    /// Card index selected on the sessions grid (see [`App::card_list`]).
    pub selected: usize,
    /// First visible card row of the grid window.
    pub list_scroll: usize,
    /// Whether finished sessions are expanded below the live cards.
    pub completed_open: bool,
    /// Buffered transcript of the selected session, when one is open.
    pub transcript: Option<TranscriptBuffer>,
    /// Latest fetched answer envelope, when the popup is open.
    pub answer: Option<AnswerView>,
    /// Broker link health.
    pub link: Link,
    /// Bounded human-readable reason of the latest broker failure.
    pub last_error: Option<String>,
    /// Whether at least one sessions page has arrived.
    pub loaded: bool,
    /// Last rendered terminal width; used to remap clicks between frames.
    pub last_width: u16,
    /// Last rendered terminal height; used to remap clicks between frames.
    pub last_height: u16,
    /// Resolved agent-run home path, used to shorten workdir display.
    pub home_prefix: Option<String>,
    /// Monotonic tick counter driving the spinner and local clocks.
    pub ticks: u64,
    /// Whether the operator asked to leave the application.
    pub quit: bool,
}

impl App {
    /// Creates the initial state: sessions screen, collapsed finished, nothing loaded.
    pub fn new() -> Self {
        Self {
            screen: Screen::Sessions,
            sessions: Vec::new(),
            revision: None,
            selected: 0,
            list_scroll: 0,
            completed_open: false,
            transcript: None,
            answer: None,
            link: Link::Down,
            last_error: None,
            loaded: false,
            last_width: 100,
            last_height: 30,
            home_prefix: None,
            ticks: 0,
            quit: false,
        }
    }

    /// Card order of the grid: live sessions first, then finished ones only
    /// while the finished section is expanded. Yields session indices.
    pub fn card_list(&self) -> Vec<usize> {
        let live = self
            .sessions
            .iter()
            .enumerate()
            .filter(|(_, agent)| !agent.status.terminal())
            .map(|(index, _)| index);
        if !self.completed_open {
            return live.collect();
        }
        let finished = self
            .sessions
            .iter()
            .enumerate()
            .filter(|(_, agent)| agent.status.terminal())
            .map(|(index, _)| index);
        live.chain(finished).collect()
    }

    /// Number of finished sessions hidden behind the dropdown when collapsed.
    pub fn finished_count(&self) -> usize {
        self.sessions.iter().filter(|a| a.status.terminal()).count()
    }

    /// Applies one sessions page: sorts rows, tracks the revision, and keeps
    /// the selection anchored on the same agent across refreshes.
    pub fn apply_sessions(&mut self, page: &agent_run_domain::AgentPage) {
        let anchored = self.selected_agent_id();
        self.sessions = sort_sessions(page.items.clone());
        self.revision = Some(page.revision);
        self.loaded = true;
        self.link = Link::Up;
        self.last_error = None;
        let cards = self.card_list();
        self.selected = anchored
            .and_then(|id| {
                cards
                    .iter()
                    .position(|index| self.sessions[*index].agent_id == id)
            })
            .unwrap_or(0);
        self.selected = self.selected.min(cards.len().saturating_sub(1));
        if let Some(buffer) = &mut self.transcript
            && let Some(fresh) = self
                .sessions
                .iter()
                .find(|agent| agent.agent_id == buffer.agent.agent_id)
        {
            buffer.agent = fresh.clone();
        }
    }

    /// Records one failed broker round trip without dropping the last state.
    pub fn apply_broker_error(&mut self, message: String) {
        self.link = Link::Down;
        self.last_error = Some(message);
    }

    /// Merges one transcript page into the open buffer when it still belongs
    /// to the selected session.
    pub fn apply_transcript(&mut self, agent_id: &AgentId, page: &TranscriptPage) {
        if let Some(buffer) = &mut self.transcript
            && buffer.agent.agent_id == *agent_id
        {
            buffer.merge(page);
        }
    }

    /// Stores one fetched answer envelope for the popup.
    pub fn apply_answer(&mut self, view: AnswerView) {
        self.answer = Some(view);
    }

    /// The session of the selected card, when the grid holds any card.
    fn selected_session(&self) -> Option<&AgentView> {
        let cards = self.card_list();
        cards.get(self.selected).map(|index| &self.sessions[*index])
    }

    /// The agent id of the selected card, when the grid holds any card.
    pub fn selected_agent_id(&self) -> Option<AgentId> {
        self.selected_session().map(|a| a.agent_id.clone())
    }

    /// The run id of the selected card, when the exact execution is known.
    pub fn selected_run_id(&self) -> Option<AgentId> {
        self.selected_session().and_then(|a| a.run_id.clone())
    }

    /// Moves the card selection, clamping to the visible cards.
    pub fn move_selection(&mut self, delta: i64) {
        let count = self.card_list().len();
        if count == 0 {
            self.selected = 0;
            return;
        }
        let next = self.selected as i64 + delta;
        self.selected = next.clamp(0, count as i64 - 1) as usize;
    }

    /// Keeps the grid window anchored on the selection between redraws.
    ///
    /// Redraws clamp with the real rendered height; this keeps the stored
    /// window offset close enough for click mapping between frames.
    pub fn sync_list_scroll(&mut self, visible: usize) {
        let visible = visible.max(1);
        if self.selected < self.list_scroll {
            self.list_scroll = self.selected;
        } else if self.selected >= self.list_scroll + visible {
            self.list_scroll = self.selected + 1 - visible;
        }
    }

    /// Selects the card rendered at one flat grid offset, clamped to the grid.
    pub fn select_card(&mut self, card: usize) {
        let count = self.card_list().len();
        if count > 0 {
            self.selected = card.min(count - 1);
        }
    }

    /// Flips the finished-sessions dropdown; the caller reissues the listing
    /// request because the fetch scope follows this state.
    pub fn toggle_completed(&mut self) {
        self.completed_open = !self.completed_open;
        self.loaded = false;
        self.selected = 0;
        self.list_scroll = 0;
    }

    /// Opens the transcript of the selected card and returns its identity.
    ///
    /// The exact run id travels with the request so the broker pins the page
    /// to the execution the operator actually selected.
    pub fn open_selected_transcript(&mut self) -> Option<(AgentId, Option<AgentId>)> {
        let agent = self.selected_session()?.clone();
        let identity = (agent.agent_id.clone(), agent.run_id.clone());
        self.transcript = Some(TranscriptBuffer::open(agent));
        self.screen = Screen::Transcript;
        Some(identity)
    }

    /// Closes the transcript view and returns to the sessions grid.
    pub fn close_transcript(&mut self) {
        self.transcript = None;
        self.screen = Screen::Sessions;
    }

    /// Closes the answer popup.
    pub fn close_answer(&mut self) {
        self.answer = None;
    }

    /// Advances the local clock by one tick.
    pub fn tick(&mut self) {
        self.ticks = self.ticks.wrapping_add(1);
    }

    /// Braille spinner glyph for the current tick, shown while loading.
    pub fn spinner(&self) -> &'static str {
        const SPINNER: [&str; 10] = ["⠋", "⠙", "⠹", "⠸", "⠼", "⠴", "⠦", "⠧", "⠇", "⠏"];
        SPINNER[(self.ticks % SPINNER.len() as u64) as usize]
    }
}

/// Narrow-safe status pictogram shown on session cards and transcript headers.
pub fn status_pictogram(status: Status) -> &'static str {
    match status {
        Status::Created => "○",
        Status::Starting => "◐",
        Status::Running => "●",
        Status::Cancelling => "◑",
        Status::Succeeded => "✓",
        Status::Failed => "✗",
        Status::TimedOut => "◷",
        Status::Cancelled => "⊘",
        Status::Lost => "◌",
    }
}

/// Sorts sessions for the table: live runs first, newest admission on top.
pub fn sort_sessions(mut items: Vec<AgentView>) -> Vec<AgentView> {
    items.sort_by(|a, b| {
        a.status
            .terminal()
            .cmp(&b.status.terminal())
            .then(b.created_at.total_cmp(&a.created_at))
    });
    items
}

/// Formats seconds as a compact human duration (`42s`, `5m09s`, `3h04m`, `2d03h`).
pub fn human_duration(seconds: f64) -> String {
    let seconds = seconds.max(0.0) as u64;
    if seconds < 60 {
        return format!("{seconds}s");
    }
    let minutes = seconds / 60;
    if minutes < 60 {
        return format!("{}m{:02}s", minutes, seconds % 60);
    }
    let hours = minutes / 60;
    if hours < 24 {
        return format!("{}h{:02}m", hours, minutes % 60);
    }
    format!("{}d{:02}h", hours / 24, hours % 24)
}

/// Status display label used by the table badge and the transcript header.
pub fn status_label(status: Status) -> &'static str {
    match status {
        Status::Created => "CREATED",
        Status::Starting => "STARTING",
        Status::Running => "RUNNING",
        Status::Cancelling => "CANCELLING",
        Status::Succeeded => "SUCCEEDED",
        Status::Failed => "FAILED",
        Status::TimedOut => "TIMED OUT",
        Status::Cancelled => "CANCELLED",
        Status::Lost => "LOST",
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use std::str::FromStr;

    /// Builds one session view from a sparse JSON fixture with valid identities.
    fn agent(id: &str, status: &str, created_at: f64) -> AgentView {
        let run = run_id(id);
        serde_json::from_value(json!({
            "agent_id": id,
            "run_id": run,
            "runtime": "codex",
            "model": "gpt-5",
            "profile": "default",
            "task_summary": "ship the thing",
            "status": status,
            "created_at": created_at,
            "started_at": created_at,
            "finished_at": None::<f64>,
            "elapsed_seconds": 1.0,
            "last_progress_at": None::<f64>,
            "silence_seconds": None::<f64>,
            "warned": false,
            "failure_kind": None::<String>,
            "failure_text": None::<String>,
            "answer_available": false,
            "answer_bytes": None::<u64>,
            "answer_sha256": None::<String>,
            "effort": None::<String>,
            "delivery": {
                "agent_id": id,
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
            "root_agent_id": id,
            "sequence": 1,
            "cleanup": None::<serde_json::Value>,
            "policy": None::<serde_json::Value>,
            "phase": "running",
            "phase_started_at": created_at,
            "process_state": "alive",
            "observed_at": created_at,
            "runtime_outcome": None::<String>,
            "acceptance": "pending",
        }))
        .expect("fixture parses")
    }

    /// A valid stable agent id (`ag-YYYYMMDD-HHMMSS-<10 lowercase hex>`).
    fn id(n: u8) -> String {
        format!("ag-20260928-1015{n:02}-aaaaaaaa{n:02}")
    }

    /// The exact run id paired with a stable id: same shape, last char rotated.
    fn run_id(id: &str) -> String {
        let mut run = id.to_string();
        run.replace_range(28..29, "b");
        run
    }

    #[test]
    fn sessions_sort_puts_live_rows_first_then_newest() {
        let sorted = sort_sessions(vec![
            agent(&id(1), "succeeded", 300.0),
            agent(&id(2), "running", 100.0),
            agent(&id(3), "running", 200.0),
            agent(&id(4), "failed", 400.0),
        ]);
        let ids: Vec<String> = sorted
            .iter()
            .map(|a| a.agent_id.as_str().to_string())
            .collect();
        assert_eq!(ids, vec![id(3), id(2), id(4), id(1)]);
    }

    #[test]
    fn apply_sessions_keeps_selection_anchor() {
        let mut app = App::new();
        let first = agent(&id(1), "running", 100.0);
        let second = agent(&id(2), "running", 200.0);
        app.apply_sessions(
            &serde_json::from_value(json!({
                "items": [first.clone(), second.clone()],
                "total": 2, "offset": 0, "limit": 200,
                "next_offset": None::<usize>, "complete": true,
                "revision": 1, "observed_at": 1.0,
            }))
            .unwrap(),
        );
        // Sorted view: the newer admission leads.
        assert_eq!(
            app.sessions[0].agent_id, second.agent_id,
            "newest live run sorts first"
        );
        app.selected = 0;
        assert_eq!(app.selected_agent_id().as_ref(), Some(&second.agent_id));

        // The anchored agent disappears; the selection falls back to row zero.
        app.apply_sessions(
            &serde_json::from_value(json!({
                "items": [first],
                "total": 1, "offset": 0, "limit": 200,
                "next_offset": None::<usize>, "complete": true,
                "revision": 2, "observed_at": 2.0,
            }))
            .unwrap(),
        );
        assert_eq!(app.selected, 0);
        assert_eq!(app.revision, Some(2));
        assert_eq!(app.link, Link::Up);
    }

    #[test]
    fn transcript_merge_deduplicates_and_tracks_cursor() {
        let agent = agent(&id(1), "running", 1.0);
        let mut app = App::new();
        app.sessions = vec![agent.clone()];
        app.open_selected_transcript();

        let message = |seq: i64| {
            json!({
                "seq": seq, "at": 1.0, "role": "assistant", "name": None::<String>,
                "content": format!("m{seq}"), "raw_ref": None::<String>,
            })
        };
        let run = agent.run_id.clone();
        let page =
            |messages: serde_json::Value, next: Option<i64>, complete: bool| -> TranscriptPage {
                serde_json::from_value(json!({
                    "agent_id": id(1),
                    "run_id": run,
                    "messages": messages,
                    "cursor": 0, "limit": 500,
                    "next_cursor": next, "complete": complete,
                }))
                .unwrap()
            };
        app.apply_transcript(
            &agent.agent_id,
            &page(json!([message(1), message(2)]), Some(2), false),
        );
        // Overlapping page: seq 2 repeats and must not duplicate.
        app.apply_transcript(
            &agent.agent_id,
            &page(json!([message(2), message(3)]), None, true),
        );
        let buffer = app.transcript.as_ref().unwrap();
        let seqs: Vec<i64> = buffer.messages.iter().map(|m| m.seq).collect();
        assert_eq!(seqs, vec![1, 2, 3]);
        assert!(buffer.history_complete);
        assert_eq!(buffer.next_cursor, 2);
        assert!(buffer.follow);
    }

    #[test]
    fn transcript_ignores_pages_of_other_sessions() {
        let agent = agent(&id(1), "running", 1.0);
        let mut app = App::new();
        app.sessions = vec![agent.clone()];
        app.open_selected_transcript();
        let page: TranscriptPage = serde_json::from_value(json!({
            "agent_id": id(2), "run_id": None::<String>,
            "messages": [], "cursor": 0, "limit": 500,
            "next_cursor": None::<i64>, "complete": true,
        }))
        .unwrap();
        app.apply_transcript(&AgentId::from_str(&id(2)).unwrap(), &page);
        let buffer = app.transcript.as_ref().unwrap();
        assert!(buffer.messages.is_empty());
        assert!(!buffer.history_complete);
    }

    #[test]
    fn scroll_leaves_and_returns_to_follow() {
        let mut buffer = TranscriptBuffer::open(agent(&id(1), "running", 1.0));
        buffer.scroll_up(3);
        assert!(!buffer.follow);
        assert_eq!(buffer.from_bottom, 3);
        buffer.scroll_up(2);
        assert_eq!(buffer.from_bottom, 5);
        buffer.scroll_down(2);
        assert_eq!(buffer.from_bottom, 3);
        buffer.scroll_down(10);
        assert!(buffer.follow);
        assert_eq!(buffer.from_bottom, 0);
        buffer.scroll_up(1);
        buffer.scroll_top();
        assert_eq!(buffer.from_bottom, u16::MAX);
        buffer.scroll_bottom();
        assert!(buffer.follow);
        buffer.scroll_up(4);
        buffer.toggle_follow();
        assert!(buffer.follow);
    }

    #[test]
    fn card_list_partitions_live_and_finished() {
        let mut app = App::new();
        app.sessions = sort_sessions(vec![
            agent(&id(1), "running", 100.0),
            agent(&id(2), "succeeded", 50.0),
            agent(&id(3), "running", 300.0),
        ]);
        // Collapsed: only live cards, newest first.
        let live: Vec<String> = app
            .card_list()
            .into_iter()
            .map(|i| app.sessions[i].agent_id.as_str().to_string())
            .collect();
        assert_eq!(live, vec![id(3), id(1)]);
        assert_eq!(app.finished_count(), 1);

        // Expanded: finished cards follow the live ones.
        app.toggle_completed();
        let all: Vec<String> = app
            .card_list()
            .into_iter()
            .map(|i| app.sessions[i].agent_id.as_str().to_string())
            .collect();
        assert_eq!(all, vec![id(3), id(1), id(2)]);
        assert_eq!(app.selected, 0);
    }

    #[test]
    fn transcript_cursor_and_expansion_track_messages() {
        let agent = agent(&id(1), "running", 1.0);
        let mut app = App::new();
        app.sessions = vec![agent.clone()];
        app.open_selected_transcript();
        let message = |seq: i64| {
            json!({
                "seq": seq, "at": 1.0, "role": "tool_call", "name": "Bash",
                "content": "{\"command\":\"ls\"}", "raw_ref": null,
            })
        };
        let page: TranscriptPage = serde_json::from_value(json!({
            "agent_id": id(1), "run_id": agent.run_id,
            "messages": [message(1), message(2), message(3)],
            "cursor": 0, "limit": 500, "next_cursor": None::<i64>, "complete": true,
        }))
        .unwrap();
        app.apply_transcript(&agent.agent_id, &page);

        let buffer = app.transcript.as_mut().unwrap();
        assert_eq!(buffer.cursor, 0);
        // Three tool messages form three blocks; navigation moves by block.
        crate::ui::transcript::move_cursor_block(buffer, 2);
        assert_eq!(buffer.cursor, 2);
        crate::ui::transcript::move_cursor_block(buffer, 10);
        assert_eq!(buffer.cursor, 2, "cursor clamps to the last block");
        assert!(!buffer.is_expanded(3));
        assert_eq!(buffer.toggle_expanded_at_cursor(), Some(3));
        assert!(buffer.is_expanded(3));
        buffer.toggle_expanded_at_cursor();
        assert!(!buffer.is_expanded(3));
    }

    #[test]
    fn open_and_close_transcript_switch_screens() {
        let mut app = App::new();
        app.sessions = vec![agent(&id(1), "running", 1.0)];
        let identity = app.open_selected_transcript();
        assert_eq!(app.screen, Screen::Transcript);
        let (agent_id, run) = identity.unwrap();
        assert_eq!(agent_id.as_str(), id(1));
        assert_eq!(run.unwrap().as_str(), run_id(&id(1)));
        app.close_transcript();
        assert_eq!(app.screen, Screen::Sessions);
        assert!(app.transcript.is_none());
    }

    #[test]
    fn human_duration_formats_compactly() {
        assert_eq!(human_duration(42.0), "42s");
        assert_eq!(human_duration(61.0), "1m01s");
        assert_eq!(human_duration(3600.0 * 3.0 + 240.0), "3h04m");
        assert_eq!(human_duration(3600.0 * 27.0), "1d03h");
        assert_eq!(human_duration(-5.0), "0s");
    }
}
