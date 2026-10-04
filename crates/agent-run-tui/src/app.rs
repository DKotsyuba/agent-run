//! Application state and pure reducers for the terminal observer.
//!
//! Everything the network workers deliver lands here first; rendering reads
//! the state without mutating it, and every reducer is synchronous and
//! unit-tested. The observer never mutates broker state on its own.

use agent_run_domain::domain::{AgentId, Status};
use agent_run_domain::{AgentView, AnswerView, MessageView, TranscriptPage};

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

/// Terminal width at and above which the observer renders the split view
/// (session list on the left, transcript on the right).
pub const SPLIT_WIDTH: u16 = 110;

/// Buffered transcript of one selected session.
pub struct TranscriptBuffer {
    /// Session snapshot as selected; refreshed whenever the sessions page updates.
    /// Header-only data: body rows never depend on it (running states and
    /// the outcome line render at assembly time).
    pub agent: AgentView,
    /// Messages ordered by their immutable sequence cursor; append-only in
    /// the common case (tail pages), with rare in-place inserts for
    /// out-of-order or replaced messages.
    pub messages: Vec<MessageView>,
    /// Cursor for the next transcript fetch.
    pub next_cursor: i64,
    /// Whether the full known history is buffered (`complete` page observed).
    pub history_complete: bool,
    /// Whether the view sticks to the tail on new content.
    pub follow: bool,
    /// Rendered lines below the viewport while the user scrolled away from the tail.
    pub from_bottom: usize,
    /// Message index under the transcript cursor (moved by Up/Down, click).
    pub cursor: usize,
    /// Message index under the mouse pointer, for hover styling.
    pub hover: Option<usize>,
    /// Sequences of collapsed messages the operator expanded (tool calls/results).
    pub expanded: std::collections::BTreeSet<i64>,
    /// Expansion revision: bumped by every [`TranscriptBuffer::toggle_expanded`],
    /// so the render cache rechecks per-block expansion flags only on the
    /// frames where one actually moved.
    pub(crate) expansion: u64,
    /// Content epoch: bumped whenever an existing message is replaced or
    /// inserted in place (the rare path); the render cache drops every
    /// memoized block when it moves. Appends never bump it.
    pub(crate) epoch: u64,
    /// Render cache owned by [`crate::ui::transcript`]: per-block memoized
    /// rows and layout sums, keyed by block identity, expansion, width,
    /// and the epoch above.
    pub(crate) render_cache: std::cell::RefCell<crate::ui::transcript::RenderCache>,
    /// The bounded reason of the last failed transcript page, shown in the
    /// pane header while the watcher retries; cleared by any success.
    pub last_page_error: Option<String>,
}

impl TranscriptBuffer {
    /// Opens a fresh buffer for one session; history is refetched from the start.
    pub fn open(agent: AgentView) -> Self {
        Self {
            agent,
            messages: Vec::new(),
            next_cursor: 0,
            history_complete: false,
            follow: true,
            from_bottom: 0,
            cursor: 0,
            hover: None,
            expanded: std::collections::BTreeSet::new(),
            expansion: 0,
            epoch: 0,
            render_cache: std::cell::RefCell::new(crate::ui::transcript::RenderCache::new()),
            last_page_error: None,
        }
    }

    /// Consumes one owned page into the buffer, moving payloads and dropping duplicates by sequence
    /// cursor, and returns whether anything changed.
    ///
    /// The store is append-only: a page whose sequences all sit past the
    /// tail extends the vector without touching existing data, so backfill
    /// stays linear and an unchanged tail poll costs nothing beyond the
    /// sequence comparison. Only out-of-order or replaced messages take the
    /// rare in-place path, which bumps the content epoch for any changed
    /// content, identity or native evidence so cached rows cannot stay stale.
    pub fn merge(&mut self, page: TranscriptPage) -> bool {
        let resume = page
            .next_cursor
            .or_else(|| page.messages.last().map(|m| m.seq));
        let mut changed = false;
        let tail = self.messages.last().map(|m| m.seq);
        let appendable = page
            .messages
            .iter()
            .all(|message| tail.is_some_and(|tail| message.seq > tail));
        if appendable {
            if page.messages.is_empty() {
                // Unchanged tail poll: nothing to do.
            } else {
                let jumbled = page
                    .messages
                    .windows(2)
                    .any(|pair| pair[0].seq > pair[1].seq);
                self.messages.extend(page.messages);
                if jumbled {
                    // A jumbled page: restore the order once (rare).
                    self.messages.sort_by_key(|m| m.seq);
                    self.epoch += 1;
                }
                changed = true;
            }
        } else {
            for message in page.messages {
                match self.messages.binary_search_by_key(&message.seq, |m| m.seq) {
                    Ok(pos) => {
                        if self.messages[pos] != message {
                            self.messages[pos] = message;
                            self.epoch += 1;
                            changed = true;
                        }
                    }
                    Err(pos) => {
                        self.messages.insert(pos, message);
                        self.epoch += 1;
                        changed = true;
                    }
                }
            }
        }
        // The resume cursor mirrors the watcher's: the page's `next_cursor`,
        // else the last sequence it carried (a stashed buffer continues from
        // here instead of refetching from zero).
        if let Some(resume) = resume {
            self.next_cursor = resume;
        }
        if page.complete && !self.history_complete {
            self.history_complete = true;
            changed = true;
        }
        // A successful page clears the last failure, visibly.
        changed |= self.last_page_error.take().is_some();
        self.cursor = self.cursor.min(self.messages.len().saturating_sub(1));
        changed
    }

    /// Whether one message is currently expanded.
    pub fn is_expanded(&self, seq: i64) -> bool {
        self.expanded.contains(&seq)
    }

    /// Flips the expansion of one message sequence.
    ///
    /// Expansion is part of the render cache's per-block key, so only the
    /// affected block rebuilds; the bumped expansion revision tells the
    /// cache that a recheck is due.
    pub fn toggle_expanded(&mut self, seq: i64) {
        if !self.expanded.remove(&seq) {
            self.expanded.insert(seq);
        }
        self.expansion = self.expansion.wrapping_add(1);
    }

    /// The expansion revision (see [`TranscriptBuffer::toggle_expanded`]).
    pub fn expansion(&self) -> u64 {
        self.expansion
    }

    /// The content epoch (see [`TranscriptBuffer::merge`]).
    pub fn epoch(&self) -> u64 {
        self.epoch
    }

    /// Expands or collapses whatever message the cursor points at.
    pub fn toggle_expanded_at_cursor(&mut self) -> Option<i64> {
        let seq = self.messages.get(self.cursor)?.seq;
        self.toggle_expanded(seq);
        Some(seq)
    }

    /// Scrolls one step away from the tail; leaves follow mode.
    pub fn scroll_up(&mut self, lines: usize) {
        self.follow = false;
        self.from_bottom = self.from_bottom.saturating_add(lines);
    }

    /// Scrolls one step toward the tail; re-enters follow mode at the tail.
    pub fn scroll_down(&mut self, lines: usize) {
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
        self.from_bottom = usize::MAX;
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

/// Most buffered transcripts kept for instant re-selection.
pub const TRANSCRIPT_CACHE_CAPACITY: usize = 8;
/// Total content bytes the stashed transcripts may hold.
pub const TRANSCRIPT_CACHE_BYTES: usize = 64 * 1024 * 1024;

/// One stashed transcript with its resume key and content size.
struct StashedTranscript {
    /// The session and execution the buffer belongs to.
    key: (AgentId, Option<AgentId>),
    /// The buffered transcript, ready to resume from its cursor.
    buffer: TranscriptBuffer,
    /// Total content bytes, for the memory bound.
    bytes: usize,
}

/// Recently viewed transcript buffers, most recently used first.
///
/// Re-selecting a session (constant while moving through the split view)
/// restores its buffer instantly and resumes from its fetch cursor instead
/// of refetching history from zero. The stash is bounded by entry count and
/// total content bytes, evicting least recently used first.
#[derive(Default)]
pub struct TranscriptCache {
    /// Stashed buffers, front = most recently used.
    entries: Vec<StashedTranscript>,
    /// Total stashed content bytes.
    bytes: usize,
    /// The byte bound in force ([`TRANSCRIPT_CACHE_BYTES`] in production).
    byte_cap: usize,
}

impl TranscriptCache {
    /// A stash bounded by an explicit byte cap (tests).
    #[cfg(test)]
    fn with_byte_cap(byte_cap: usize) -> Self {
        Self {
            byte_cap,
            ..Self::default()
        }
    }

    /// Removes and returns the stashed buffer of one session, if present.
    fn take(&mut self, key: &(AgentId, Option<AgentId>)) -> Option<TranscriptBuffer> {
        let position = self.entries.iter().position(|entry| &entry.key == key)?;
        let entry = self.entries.remove(position);
        self.bytes = self.bytes.saturating_sub(entry.bytes);
        Some(entry.buffer)
    }

    /// Stashes one buffer under its session key, evicting from the back
    /// until both bounds hold (the freshly stashed entry always survives).
    fn stash(&mut self, key: (AgentId, Option<AgentId>), buffer: TranscriptBuffer) {
        let bytes = transcript_bytes(&buffer);
        self.bytes += bytes;
        self.entries
            .insert(0, StashedTranscript { key, buffer, bytes });
        let byte_cap = if self.byte_cap == 0 {
            TRANSCRIPT_CACHE_BYTES
        } else {
            self.byte_cap
        };
        while self.entries.len() > TRANSCRIPT_CACHE_CAPACITY
            || (self.bytes > byte_cap && self.entries.len() > 1)
        {
            let Some(evicted) = self.entries.pop() else {
                break;
            };
            self.bytes = self.bytes.saturating_sub(evicted.bytes);
        }
    }
}

/// The content bytes one buffer holds (its memory-dominating share).
fn transcript_bytes(buffer: &TranscriptBuffer) -> usize {
    buffer
        .messages
        .iter()
        .map(|message| message.content.len())
        .sum()
}

/// Listing-derived products shared across redraws and pointer hit-testing.
struct CardCache {
    /// Number of source sessions; direct initial setup is detected lazily.
    len: usize,
    /// Normalized project root per session, calculated once per listing.
    roots: Vec<Option<String>>,
    /// Project counts ordered by activity.
    projects: std::sync::Arc<Vec<(String, usize, usize)>>,
    /// Visible session indices in card order.
    cards: std::sync::Arc<Vec<usize>>,
    /// Filter used for the card projection.
    filter: Option<String>,
    /// Finished-section scope used for the card projection.
    completed: bool,
    /// Finished count within the current project scope.
    finished: usize,
}

/// Full terminal state; rendering is a pure function of this value.
pub struct App {
    /// Which screen is on display.
    pub screen: Screen,
    /// Latest session snapshots, sorted by [`sort_sessions`].
    pub sessions: Vec<AgentView>,
    /// Derived listing products, invalidated by listing/scope/filter changes.
    cards: std::cell::RefCell<Option<CardCache>>,
    /// Last committed store revision observed from the broker.
    pub revision: Option<i64>,
    /// Card index selected on the sessions grid (see [`App::card_list`]).
    pub selected: usize,
    /// First visible card row of the grid window.
    pub list_scroll: usize,
    /// Whether finished sessions are expanded below the live cards.
    pub completed_open: bool,
    /// Project root the card list is filtered by, when one is selected.
    pub project_filter: Option<String>,
    /// Whether the project picker popup is open.
    pub project_picker: bool,
    /// Cursor inside the project picker.
    pub picker_cursor: usize,
    /// Whether the key-help overlay is open.
    pub help: bool,
    /// Card position under the mouse pointer, for hover styling.
    pub hover_card: Option<usize>,
    /// Buffered transcript of the selected session, when one is open.
    pub transcript: Option<TranscriptBuffer>,
    /// Recently viewed transcripts, kept for instant re-selection.
    pub(crate) transcripts: TranscriptCache,
    /// Latest fetched answer envelope, when the popup is open.
    pub answer: Option<AnswerView>,
    /// Rendered answer lines, sanitized once when the envelope arrives.
    pub answer_lines: std::cell::RefCell<crate::ui::answer::Cache>,
    /// Broker link health.
    pub link: Link,
    /// Bounded human-readable reason of the latest broker failure.
    pub last_error: Option<String>,
    /// Whether at least one sessions page has arrived.
    pub loaded: bool,
    /// Exact finished-session total the broker counted over every session;
    /// `None` while unknown (the active start scope loads no finished rows
    /// to count). Failures keep the last known value.
    pub finished_total: Option<usize>,
    /// Last rendered terminal width; used to remap clicks between frames.
    pub last_width: u16,
    /// Last rendered terminal height; used to remap clicks between frames.
    pub last_height: u16,
    /// Stable pointer targets captured from the last completed frame.
    pub(crate) hits: std::cell::RefCell<crate::events::HitMap>,
    /// Resolved agent-run home path, used to shorten workdir display.
    pub home_prefix: Option<String>,
    /// Monotonic tick counter driving the spinner and local clocks.
    pub ticks: u64,
    /// Whether anything visible changed since the last draw; the event loop
    /// draws only while this is set and clears it after.
    pub dirty: bool,
    /// Whether the operator asked to leave the application.
    pub quit: bool,
}

impl App {
    /// Creates the initial state: sessions screen, collapsed finished, nothing loaded.
    pub fn new() -> Self {
        Self {
            screen: Screen::Sessions,
            sessions: Vec::new(),
            cards: std::cell::RefCell::new(None),
            revision: None,
            selected: 0,
            list_scroll: 0,
            completed_open: false,
            project_filter: None,
            project_picker: false,
            picker_cursor: 0,
            help: false,
            hover_card: None,
            transcript: None,
            transcripts: TranscriptCache::default(),
            answer: None,
            answer_lines: std::cell::RefCell::default(),
            link: Link::Down,
            last_error: None,
            loaded: false,
            finished_total: None,
            last_width: 100,
            last_height: 30,
            hits: std::cell::RefCell::default(),
            home_prefix: None,
            ticks: 0,
            dirty: true,
            quit: false,
        }
    }

    /// Card order of the grid: live sessions first, then finished ones only
    /// while the finished section is expanded. When a project filter is set,
    /// only sessions of that project (its checkout and any of its worktrees)
    /// remain. Yields session indices.
    pub fn card_list(&self) -> std::sync::Arc<Vec<usize>> {
        self.sync_cards();
        self.cards
            .borrow()
            .as_ref()
            .expect("synced cards")
            .cards
            .clone()
    }

    /// Returns cached project roots/counts, sorted by activity then name.
    pub fn projects(&self) -> std::sync::Arc<Vec<(String, usize, usize)>> {
        self.sync_cards();
        self.cards
            .borrow()
            .as_ref()
            .expect("synced cards")
            .projects
            .clone()
    }

    /// Lazily rebuilds projections only after a listing, scope or filter change.
    /// Root normalization and project aggregation run once per listing.
    fn sync_cards(&self) {
        let mut slot = self.cards.borrow_mut();
        if slot
            .as_ref()
            .is_none_or(|cache| cache.len != self.sessions.len())
        {
            let roots: Vec<_> = self
                .sessions
                .iter()
                .map(|a| agent_workdir(a).map(|w| project_root(&w)))
                .collect();
            let mut projects = std::collections::BTreeMap::<String, (usize, usize)>::new();
            for (agent, root) in self.sessions.iter().zip(&roots) {
                if let Some(root) = root {
                    let counts = projects.entry(root.clone()).or_default();
                    if agent.status.terminal() {
                        counts.1 += 1;
                    } else {
                        counts.0 += 1;
                    }
                }
            }
            let mut projects: Vec<_> = projects
                .into_iter()
                .map(|(root, (live, finished))| (root, live, finished))
                .collect();
            projects.sort_by(|a, b| (b.1 + b.2).cmp(&(a.1 + a.2)).then_with(|| a.0.cmp(&b.0)));
            *slot = Some(CardCache {
                len: self.sessions.len(),
                roots,
                projects: std::sync::Arc::new(projects),
                cards: std::sync::Arc::default(),
                filter: None,
                completed: !self.completed_open,
                finished: 0,
            });
        }
        let cache = slot.as_mut().expect("initialized cards");
        if cache.filter != self.project_filter || cache.completed != self.completed_open {
            cache.filter.clone_from(&self.project_filter);
            cache.completed = self.completed_open;
            let mut live = Vec::new();
            let mut finished = Vec::new();
            for (index, agent) in self.sessions.iter().enumerate() {
                if self
                    .project_filter
                    .as_ref()
                    .is_some_and(|root| cache.roots[index].as_ref() != Some(root))
                {
                    continue;
                }
                if agent.status.terminal() {
                    finished.push(index);
                } else {
                    live.push(index);
                }
            }
            cache.finished = finished.len();
            if self.completed_open {
                live.extend(finished);
            }
            cache.cards = std::sync::Arc::new(live);
        }
    }

    /// Opens the project picker and resets its cursor.
    pub fn open_project_picker(&mut self) {
        self.project_picker = true;
        self.picker_cursor = 0;
    }

    /// Closes the project picker without changing the filter.
    pub fn close_project_picker(&mut self) {
        self.project_picker = false;
    }

    /// Moves the picker cursor, clamping to the discovered projects.
    pub fn move_picker_cursor(&mut self, delta: i64) {
        let count = self.projects().len();
        if count == 0 {
            self.picker_cursor = 0;
            return;
        }
        let next = self.picker_cursor as i64 + delta;
        self.picker_cursor = next.clamp(0, count as i64 - 1) as usize;
    }

    /// Applies the picker cursor as the project filter and closes the popup.
    pub fn apply_project_picker(&mut self) {
        self.project_filter = self
            .projects()
            .get(self.picker_cursor)
            .map(|(root, _, _)| root.clone());
        self.project_picker = false;
        self.selected = 0;
        self.list_scroll = 0;
    }

    /// Clears the project filter, showing every loaded session again.
    pub fn clear_project_filter(&mut self) {
        self.project_filter = None;
        self.picker_cursor = 0;
        self.selected = 0;
        self.list_scroll = 0;
    }

    /// Number of finished sessions hidden behind the dropdown when collapsed.
    ///
    /// Prefers the broker-counted total: the active-only start scope loads
    /// no finished rows, so loaded rows undercount until a wider listing
    /// arrives.
    pub fn finished_count(&self) -> usize {
        self.finished_total
            .unwrap_or_else(|| self.sessions.iter().filter(|a| a.status.terminal()).count())
    }

    /// Finished-session count of the current scope, shown on the `FINISHED`
    /// section header.
    ///
    /// Without a project filter this is the broker-counted total (the active
    /// start scope loads no finished rows to count); with one, only the
    /// loaded rows of that project can be counted — exact once expanded.
    pub fn finished_in_scope(&self) -> usize {
        if self.project_filter.is_none() {
            return self.finished_count();
        }
        self.sync_cards();
        self.cards.borrow().as_ref().expect("synced cards").finished
    }

    /// Records the broker-counted finished-session total.
    pub fn apply_finished_total(&mut self, finished: usize) {
        if self.finished_total != Some(finished) {
            self.finished_total = Some(finished);
            self.dirty = true;
        }
    }

    /// Applies one sessions page: sorts rows, tracks the revision, and keeps
    /// the selection anchored on the same agent across refreshes.
    ///
    /// A refreshed [`AgentView`] updates the pane header only — body rows
    /// never depend on it — so the transcript cache is untouched here.
    pub fn apply_sessions(&mut self, page: &agent_run_domain::AgentPage) {
        if self.loaded && self.revision == Some(page.revision) {
            // An unchanged page (same committed revision) changes nothing.
            return;
        }
        let anchored = self.selected_agent_id();
        self.sessions = sort_sessions(page.items.clone());
        self.cards.get_mut().take();
        self.revision = Some(page.revision);
        self.loaded = true;
        self.link = Link::Up;
        self.last_error = None;
        self.dirty = true;
        let cards = self.card_list();
        self.selected = anchored
            .and_then(|id| {
                cards
                    .iter()
                    .position(|index| self.sessions[*index].agent_id == id)
            })
            .unwrap_or(0);
        self.selected = self.selected.min(cards.len().saturating_sub(1));
        if let Some(buffer) = &mut self.transcript {
            if let Some(fresh) = self
                .sessions
                .iter()
                .find(|agent| agent.agent_id == buffer.agent.agent_id)
            {
                if buffer.agent != *fresh {
                    buffer.agent = fresh.clone();
                }
            }
        }
    }

    /// Records one failed broker round trip without dropping the last state.
    pub fn apply_broker_error(&mut self, message: String) {
        self.link = Link::Down;
        self.last_error = Some(message);
        self.dirty = true;
    }

    /// Consumes one owned transcript page into the open buffer when it still belongs
    /// to the selected session; returns whether anything changed.
    ///
    /// An unchanged tail poll changes nothing and marks nothing dirty.
    pub fn apply_transcript(&mut self, agent_id: &AgentId, page: TranscriptPage) -> bool {
        let Some(buffer) = &mut self.transcript else {
            return false;
        };
        if buffer.agent.agent_id != *agent_id {
            return false;
        }
        let changed = buffer.merge(page);
        self.dirty |= changed;
        changed
    }

    /// Records one failed transcript page against the open buffer.
    ///
    /// The partial history stays on screen with its retry state visible;
    /// the next successful page clears it.
    pub fn apply_transcript_error(&mut self, agent_id: &AgentId, message: String) {
        self.link = Link::Down;
        self.last_error = Some(message.clone());
        self.dirty = true;
        if let Some(buffer) = &mut self.transcript {
            if buffer.agent.agent_id == *agent_id {
                buffer.last_page_error = Some(message);
            }
        }
    }

    /// Stores one fetched answer envelope for the popup.
    pub fn apply_answer(&mut self, view: AnswerView) {
        *self.answer_lines.get_mut() = crate::ui::answer::Cache::new(&view);
        self.answer = Some(view);
        self.dirty = true;
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

    /// Opens the transcript of the selected card and returns its identity
    /// with the cursor the watcher should resume from.
    ///
    /// The exact run id travels with the request so the broker pins the page
    /// to the execution the operator actually selected. When the pane already
    /// buffers the selected session the buffer is reused and no identity is
    /// returned — the watcher is still attached to it.
    pub fn open_selected_transcript(&mut self) -> Option<(AgentId, Option<AgentId>, i64)> {
        let identity = self.watch_selected();
        if self.transcript.is_some() {
            self.screen = Screen::Transcript;
        }
        identity
    }

    /// Points the transcript pane at the selected card without leaving the
    /// sessions screen (split view) and returns the identity to watch plus
    /// the cursor to resume from.
    ///
    /// The outgoing buffer is stashed in the LRU and a previously viewed one
    /// is restored instantly, resuming from its fetch cursor instead of
    /// refetching history from zero.
    pub fn watch_selected(&mut self) -> Option<(AgentId, Option<AgentId>, i64)> {
        let agent = self.selected_session()?.clone();
        if self
            .transcript
            .as_ref()
            .is_some_and(|buffer| buffer.agent.agent_id == agent.agent_id)
        {
            return None;
        }
        let key = (agent.agent_id.clone(), agent.run_id.clone());
        if let Some(outgoing) = self.transcript.take() {
            let out_key = (
                outgoing.agent.agent_id.clone(),
                outgoing.agent.run_id.clone(),
            );
            self.transcripts.stash(out_key, outgoing);
        }
        match self.transcripts.take(&key) {
            Some(mut buffer) => {
                // Header data went stale while stashed; refresh the view.
                if let Some(fresh) = self
                    .sessions
                    .iter()
                    .find(|view| view.agent_id == buffer.agent.agent_id)
                {
                    buffer.agent = fresh.clone();
                }
                let resume = buffer.next_cursor;
                self.transcript = Some(buffer);
                Some((key.0, key.1, resume))
            }
            None => {
                self.transcript = Some(TranscriptBuffer::open(agent));
                Some((key.0, key.1, 0))
            }
        }
    }

    /// Whether the current width renders the split view (two panes).
    pub fn split_view(&self) -> bool {
        self.last_width >= SPLIT_WIDTH
    }

    /// Whether the transcript pane no longer shows the selected session.
    ///
    /// The split view keeps the pane glued to the selection; the event loop
    /// re-attaches the watcher whenever this flips to true.
    pub fn transcript_stale(&self) -> bool {
        match (&self.transcript, self.selected_agent_id()) {
            (Some(buffer), Some(id)) => buffer.agent.agent_id != id,
            (None, None) => false,
            _ => true,
        }
    }

    /// Closes the transcript view and returns to the sessions grid.
    ///
    /// The buffer is stashed, so reopening the session restores it.
    pub fn close_transcript(&mut self) {
        if let Some(buffer) = self.transcript.take() {
            let key = (buffer.agent.agent_id.clone(), buffer.agent.run_id.clone());
            self.transcripts.stash(key, buffer);
        }
        self.screen = Screen::Sessions;
    }

    /// Closes the answer popup.
    pub fn close_answer(&mut self) {
        self.answer = None;
        self.answer_lines = std::cell::RefCell::default();
    }

    /// Advances the local clock by one tick and marks the frame dirty when
    /// the tick changes anything visible ([`App::tick_animates`]); a tick
    /// over a quiet screen draws nothing.
    pub fn tick(&mut self) {
        self.ticks = self.ticks.wrapping_add(1);
        if self.tick_animates() {
            self.dirty = true;
        }
    }

    /// Whether a tick changes anything visible: any live session animates
    /// the spinner, elapsed counters, and idle clocks; an unloaded app
    /// animates the waiting spinner.
    pub fn tick_animates(&self) -> bool {
        !self.loaded || self.sessions.iter().any(|agent| !agent.status.terminal())
    }

    /// Braille spinner glyph for the current tick, shown while loading.
    pub fn spinner(&self) -> &'static str {
        const SPINNER: [&str; 10] = ["⠋", "⠙", "⠹", "⠸", "⠼", "⠴", "⠦", "⠧", "⠇", "⠏"];
        SPINNER[(self.ticks % SPINNER.len() as u64) as usize]
    }
}

/// Resolves the working directory of one session.
///
/// The broker `workdir` projection wins; older resident brokers omit it, so
/// the operator-visible task text is the fallback — orchestrator tasks
/// conventionally carry a `Workdir: <path>` sentence.
pub fn agent_workdir(agent: &AgentView) -> Option<String> {
    agent
        .workdir
        .clone()
        .or_else(|| task_workdir(&agent.task_summary))
}

/// The operator-facing title of one session: the optional human display
/// label leads the first task-summary line (`<name> — <task>`); unnamed
/// agents keep the bare task text.
pub fn display_title(agent: &AgentView) -> String {
    let task = agent.task_summary.lines().next().unwrap_or_default();
    match &agent.name {
        Some(name) => format!("{name} — {task}"),
        None => task.to_string(),
    }
}

/// Extracts the first absolute path after a `Workdir:` sentence fragment.
fn task_workdir(task: &str) -> Option<String> {
    let lowered = task.to_lowercase();
    let start = lowered.find("workdir:")? + "workdir:".len();
    let rest = &task[start..];
    // Skip filler words such as `worktree` before the path itself.
    rest.split_whitespace()
        .find(|token| token.starts_with('/') || token.starts_with('~'))
        .map(|token| {
            token
                .trim_matches(|c: char| ".,;:)\u{2019}\"'`".contains(c))
                .to_string()
        })
}

/// Reduces a working directory to its project root: the checkout its
/// worktree belongs to. `<root>/.claude/worktrees/x`, `<root>/.worktrees/x`,
/// and `<root>/worktrees/x` all reduce to `<root>`; other paths stay whole.
pub fn project_root(workdir: &str) -> String {
    let path = std::path::Path::new(workdir);
    let components: Vec<_> = path.components().collect();
    for (index, component) in components.iter().enumerate() {
        let name = component.as_os_str().to_string_lossy();
        if name == "worktrees" || name == ".worktrees" {
            // A dot-directory between the checkout and `worktrees` (such as
            // `.claude`) belongs to the harness, not the project.
            let keep = if index > 0
                && components[index - 1]
                    .as_os_str()
                    .to_string_lossy()
                    .starts_with('.')
            {
                index - 1
            } else {
                index
            };
            let mut root = std::path::PathBuf::new();
            for component in &components[..keep] {
                root.push(component);
            }
            return root.display().to_string();
        }
    }
    workdir.to_string()
}

/// The short display name of one project root: its last path component.
pub fn project_name(root: &str) -> String {
    std::path::Path::new(root)
        .file_name()
        .map(|name| name.to_string_lossy().into_owned())
        .unwrap_or_else(|| root.to_string())
}

/// Narrow-safe status glyph shown on session rows and pane titles.
///
/// A running session renders the current spinner frame instead of a static
/// glyph, so the row animates while the broker link is alive.
pub fn status_glyph(status: Status, spinner: &str) -> &str {
    match status {
        Status::Created => "○",
        Status::Starting => "◐",
        Status::Running => spinner,
        Status::Cancelling => "◑",
        Status::Succeeded => "✓",
        Status::Failed => "✗",
        Status::TimedOut => "◷",
        Status::Cancelled => "⊘",
        Status::Lost => "◌",
    }
}

/// The trailing hash fragment of an agent id, `n` characters long.
///
/// Agent ids end in a 10-character hex tail (`ag-YYYYMMDD-HHMMSS-<hash>`);
/// list rows and the narrow app bar abbreviate identities to it.
pub fn id_hash(id: &str, n: usize) -> String {
    let tail = id.rsplit('-').next().unwrap_or(id);
    tail.chars().take(n).collect()
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
            page(json!([message(1), message(2)]), Some(2), false),
        );
        // Overlapping page: seq 2 repeats and must not duplicate.
        app.apply_transcript(
            &agent.agent_id,
            page(json!([message(2), message(3)]), None, true),
        );
        let buffer = app.transcript.as_ref().unwrap();
        let seqs: Vec<i64> = buffer.messages.iter().map(|m| m.seq).collect();
        assert_eq!(seqs, vec![1, 2, 3]);
        assert!(buffer.history_complete);
        // The resume cursor mirrors the watcher's: the last sequence carried,
        // so a stashed buffer continues from the tail, not the page boundary.
        assert_eq!(buffer.next_cursor, 3);
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
        app.apply_transcript(&AgentId::from_str(&id(2)).unwrap(), page.clone());
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
        assert_eq!(buffer.from_bottom, usize::MAX);
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
            .iter()
            .copied()
            .map(|i| app.sessions[i].agent_id.as_str().to_string())
            .collect();
        assert_eq!(live, vec![id(3), id(1)]);
        assert_eq!(app.finished_count(), 1);

        // Expanded: finished cards follow the live ones.
        app.toggle_completed();
        let all: Vec<String> = app
            .card_list()
            .iter()
            .copied()
            .map(|i| app.sessions[i].agent_id.as_str().to_string())
            .collect();
        assert_eq!(all, vec![id(3), id(1), id(2)]);
        assert_eq!(app.selected, 0);
    }

    #[test]
    fn project_root_normalizes_every_worktree_layout() {
        assert_eq!(
            project_root("/Users/pluto/projects/agent-ide/.claude/worktrees/m003"),
            "/Users/pluto/projects/agent-ide"
        );
        assert_eq!(
            project_root("/Users/pluto/projects/agent-worktree/.worktrees/notify"),
            "/Users/pluto/projects/agent-worktree"
        );
        assert_eq!(
            project_root("/Users/pluto/projects/agent-run/worktrees/w1"),
            "/Users/pluto/projects/agent-run"
        );
        assert_eq!(
            project_root("/Users/pluto/projects/agent-run"),
            "/Users/pluto/projects/agent-run",
            "plain checkouts stay whole"
        );
    }

    #[test]
    fn project_filter_groups_worktrees_under_one_root() {
        let mut app = App::new();
        let workdir = |path: &str| Some(path.to_string());
        let mut mainline = agent(&id(1), "running", 100.0);
        mainline.workdir = workdir("/Users/pluto/projects/agent-ide");
        let mut tree = agent(&id(2), "running", 90.0);
        tree.workdir = workdir("/Users/pluto/projects/agent-ide/.claude/worktrees/m001");
        let mut other = agent(&id(3), "running", 80.0);
        other.workdir = workdir("/Users/pluto/projects/other");
        app.sessions = sort_sessions(vec![mainline, tree, other]);

        // Unattributed task text also resolves: same project root.
        let mut task_based = agent(&id(4), "running", 70.0);
        task_based.workdir = None;
        task_based.task_summary =
            "ROLE: implement. Workdir: worktree /Users/pluto/projects/agent-ide/.claude/worktrees/m002".into();
        app.sessions.push(task_based);
        app.sessions = sort_sessions(app.sessions.clone());

        app.project_filter = Some("/Users/pluto/projects/agent-ide".to_string());
        let cards: Vec<String> = app
            .card_list()
            .iter()
            .copied()
            .map(|index| app.sessions[index].agent_id.as_str().to_string())
            .collect();
        assert_eq!(cards.len(), 3, "checkout + two worktrees, one project");
        assert!(
            !cards.contains(&id(3)),
            "other project filtered out: {cards:?}"
        );

        // Clearing the filter restores everything.
        app.clear_project_filter();
        assert_eq!(app.card_list().len(), 4);
    }

    #[test]
    fn picker_applies_and_clears_the_filter() {
        let mut app = App::new();
        let mut one = agent(&id(1), "running", 100.0);
        one.workdir = Some("/Users/pluto/projects/one".into());
        let mut two = agent(&id(2), "running", 90.0);
        two.workdir = Some("/Users/pluto/projects/two".into());
        app.sessions = vec![one, two];

        app.open_project_picker();
        assert!(app.project_picker);
        app.move_picker_cursor(10);
        let projects = app.projects();
        assert_eq!(projects.len(), 2);
        // Busiest first; both have one live session, so alphabetical: one, two.
        app.picker_cursor = 0;
        app.apply_project_picker();
        assert_eq!(
            app.project_filter.as_deref(),
            Some("/Users/pluto/projects/one")
        );
        assert!(!app.project_picker);
        assert_eq!(app.card_list().len(), 1);

        // The reset entry ("all projects", cursor == projects.len()) clears it.
        app.open_project_picker();
        app.picker_cursor = app.projects().len();
        app.apply_project_picker();
        assert!(app.project_filter.is_none());
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
        app.apply_transcript(&agent.agent_id, page.clone());

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
        let (agent_id, run, _) = identity.unwrap();
        assert_eq!(agent_id.as_str(), id(1));
        assert_eq!(run.unwrap().as_str(), run_id(&id(1)));
        app.close_transcript();
        assert_eq!(app.screen, Screen::Sessions);
        assert!(app.transcript.is_none());
    }

    #[test]
    fn transcript_lru_restores_and_resumes() {
        let mut app = App::new();
        app.sessions = sort_sessions(vec![
            agent(&id(1), "running", 100.0),
            agent(&id(2), "running", 90.0),
        ]);
        // selected = 0 -> the newest admission, id(1).
        let (_, _, cursor) = app.watch_selected().expect("fresh watch");
        assert_eq!(cursor, 0, "a fresh buffer restarts history");
        let page: TranscriptPage = serde_json::from_value(json!({
            "agent_id": id(1),
            "run_id": run_id(&id(1)),
            "messages": [
                {"seq": 1, "at": 1.0, "role": "assistant", "name": null,
                 "content": "one", "raw_ref": null},
                {"seq": 2, "at": 2.0, "role": "assistant", "name": null,
                 "content": "two", "raw_ref": null},
            ],
            "cursor": 0, "limit": 1000, "next_cursor": 2, "complete": false,
        }))
        .unwrap();
        let first = app.sessions[0].agent_id.clone();
        assert!(app.apply_transcript(&first, page.clone()));

        // Moving the selection away stashes the buffer.
        app.move_selection(1);
        let (_, _, cursor) = app.watch_selected().expect("switch to the other session");
        assert_eq!(cursor, 0);
        assert_eq!(
            app.transcript.as_ref().unwrap().messages.len(),
            0,
            "the second session starts empty"
        );

        // Coming back restores instantly: same messages, resume cursor, and
        // a header view refreshed from the latest sessions page.
        app.sessions[0].silence_seconds = Some(77.0);
        app.move_selection(-1);
        let (agent, _, cursor) = app.watch_selected().expect("restore");
        assert_eq!(agent, app.sessions[0].agent_id);
        assert_eq!(cursor, 2, "the watcher resumes from the stash cursor");
        let buffer = app.transcript.as_ref().unwrap();
        assert_eq!(buffer.messages.len(), 2, "history survived the detour");
        assert_eq!(buffer.next_cursor, 2);
        assert_eq!(buffer.agent.silence_seconds, Some(77.0));
    }

    #[test]
    fn transcript_lru_evicts_least_recently_used() {
        let mut cache = TranscriptCache::default();
        let view = |n: u8| agent(&id(n), "running", 100.0 + n as f64);
        let stash_with = |cache: &mut TranscriptCache, n: u8| {
            let mut buffer = TranscriptBuffer::open(view(n));
            buffer.messages = vec![message_like(n)];
            cache.stash((view(n).agent_id, None), buffer);
        };
        for n in 1..=10u8 {
            stash_with(&mut cache, n);
        }
        assert_eq!(cache.entries.len(), TRANSCRIPT_CACHE_CAPACITY);
        // The oldest entries evicted; the recent half survives in order.
        for n in 1..=(10 - TRANSCRIPT_CACHE_CAPACITY as u8) {
            assert!(
                cache.take(&(view(n).agent_id, None)).is_none(),
                "entry {n} evicted"
            );
        }
        for n in (10 - TRANSCRIPT_CACHE_CAPACITY as u8 + 1)..=10 {
            assert!(
                cache.take(&(view(n).agent_id, None)).is_some(),
                "entry {n} kept"
            );
        }
    }

    /// One minimal message fixture carrying `n` as its sequence.
    fn message_like(n: u8) -> MessageView {
        MessageView {
            seq: n as i64,
            at: 1.0,
            role: "assistant".into(),
            name: None,
            content: format!("message {n}"),
            raw_ref: None,
            ..MessageView::default()
        }
    }

    #[test]
    fn transcript_lru_evicts_by_total_bytes() {
        let mut cache = TranscriptCache::with_byte_cap(100);
        let view = |n: u8| agent(&id(n), "running", 100.0 + n as f64);
        let stash_with = |cache: &mut TranscriptCache, n: u8, size: usize| {
            let mut buffer = TranscriptBuffer::open(view(n));
            buffer.messages = vec![MessageView {
                seq: n as i64,
                at: 1.0,
                role: "assistant".into(),
                name: None,
                content: "x".repeat(size),
                raw_ref: None,
                ..MessageView::default()
            }];
            cache.stash((view(n).agent_id, None), buffer);
        };
        stash_with(&mut cache, 1, 60);
        stash_with(&mut cache, 2, 60);
        // 120 bytes > 100 cap: the older entry evicts.
        assert_eq!(cache.entries.len(), 1);
        assert!(cache.take(&(view(1).agent_id, None)).is_none());
        assert!(cache.take(&(view(2).agent_id, None)).is_some());
        // One oversized entry always survives on its own.
        stash_with(&mut cache, 3, 500);
        assert_eq!(cache.entries.len(), 1);
        assert!(cache.take(&(view(3).agent_id, None)).is_some());
    }

    #[test]
    fn human_duration_formats_compactly() {
        assert_eq!(human_duration(42.0), "42s");
        assert_eq!(human_duration(61.0), "1m01s");
        assert_eq!(human_duration(3600.0 * 3.0 + 240.0), "3h04m");
        assert_eq!(human_duration(3600.0 * 27.0), "1d03h");
        assert_eq!(human_duration(-5.0), "0s");
    }

    #[test]
    fn watch_selected_tracks_the_pane_to_the_selection() {
        let mut app = App::new();
        app.sessions = sort_sessions(vec![
            agent(&id(1), "running", 100.0),
            agent(&id(2), "running", 200.0),
        ]);
        assert!(
            app.transcript_stale(),
            "empty pane is stale once a session exists"
        );

        // Watching the selection opens a buffer without switching screens.
        assert!(app.watch_selected().is_some());
        assert_eq!(app.screen, Screen::Sessions);
        assert!(!app.transcript_stale());
        assert_eq!(
            app.transcript.as_ref().unwrap().agent.agent_id.as_str(),
            id(2),
            "sorted view puts the newest run under the selection"
        );
        // Re-watching the same session reuses the buffer.
        assert!(app.watch_selected().is_none());

        // Opening the transcript of an already-buffered session keeps it.
        app.open_selected_transcript();
        assert_eq!(app.screen, Screen::Transcript);
        assert_eq!(app.transcript.as_ref().unwrap().messages.len(), 0);

        // Moving the selection detaches the pane again.
        app.screen = Screen::Sessions;
        app.move_selection(1);
        assert!(app.transcript_stale());
        assert!(app.watch_selected().is_some());
        assert_eq!(
            app.transcript.as_ref().unwrap().agent.agent_id.as_str(),
            id(1)
        );

        // With no session to select nothing is stale.
        app.sessions.clear();
        app.transcript = None;
        assert!(!app.transcript_stale());
    }

    #[test]
    fn split_view_follows_the_rendered_width() {
        let mut app = App::new();
        app.last_width = 109;
        assert!(!app.split_view());
        app.last_width = 110;
        assert!(app.split_view());
    }

    #[test]
    fn id_hash_takes_the_tail_fragment() {
        assert_eq!(id_hash("ag-20260930-181418-1e1d9d2211", 8), "1e1d9d22");
        assert_eq!(id_hash("short", 8), "short");
    }
    /// Owned page reduction moves the incoming allocation without cloning bytes.
    #[test]
    fn transcript_merge_moves_content_and_saturates_document_scroll() {
        let mut buffer = TranscriptBuffer::open(agent(&id(1), "running", 1.0));
        let incoming: TranscriptPage = serde_json::from_value(json!({"agent_id":id(1),"run_id":id(2),"messages":[crate::tests_support::message(1,"assistant","owned payload")],"cursor":0,"limit":1000,"next_cursor":null,"complete":true})).unwrap();
        let allocation = incoming.messages[0].content.as_ptr();
        buffer.merge(incoming);
        assert_eq!(buffer.messages[0].content.as_ptr(), allocation);
        buffer.scroll_top();
        buffer.scroll_up(1);
        assert_eq!(buffer.from_bottom, usize::MAX);
        buffer.scroll_down(usize::MAX);
        assert!(buffer.follow);
    }

    /// Projections reuse their allocations and invalidate on every relevant change.
    #[test]
    fn card_and_project_caches_invalidate_with_listing_filter_and_scope() {
        let mut app = App::new();
        let mut first = agent(&id(1), "running", 1.0);
        first.task_summary = "Workdir: /tmp/one".into();
        app.sessions = vec![first.clone()];
        let cards = app.card_list();
        let projects = app.projects();
        assert!(std::sync::Arc::ptr_eq(&cards, &app.card_list()));
        assert!(std::sync::Arc::ptr_eq(&projects, &app.projects()));
        app.project_filter = Some("/tmp/missing".into());
        assert!(app.card_list().is_empty());
        app.project_filter = None;
        app.completed_open = true;
        assert_eq!(app.card_list().len(), 1);
        first.status = Status::Succeeded;
        app.apply_sessions(&serde_json::from_value(json!({"items":[first],"total":1,"offset":0,"limit":200,"next_offset":null,"complete":true,"revision":2,"observed_at":1.0})).unwrap());
        assert!(!std::sync::Arc::ptr_eq(&projects, &app.projects()));
        assert_eq!(app.finished_in_scope(), 1);
        app.completed_open = false;
        assert!(app.card_list().is_empty());
    }

    /// The broker-counted finished total feeds the app bar and the section
    /// header even when the active scope loads no finished rows; a project
    /// filter keeps the header scoped to loaded rows.
    #[test]
    fn finished_total_serves_counts_without_loaded_finished_rows() {
        let mut app = App::new();
        let mut live = agent(&id(1), "running", 1.0);
        live.task_summary = "Workdir: /tmp/one".into();
        app.sessions = vec![live];
        assert_eq!(app.finished_count(), 0);
        assert_eq!(app.finished_in_scope(), 0);

        // The broker counted 103 finished sessions over every project.
        app.apply_finished_total(103);
        assert_eq!(app.finished_count(), 103);
        assert_eq!(app.finished_in_scope(), 103);
        assert!(app.dirty, "a new total redraws");

        // An unchanged total costs no redraw.
        app.dirty = false;
        app.apply_finished_total(103);
        assert!(!app.dirty);

        // A project filter keeps the header scoped to loaded rows while the
        // app bar stays global.
        app.project_filter = Some("/tmp/one".into());
        assert_eq!(app.finished_in_scope(), 0, "no finished rows loaded");
        assert_eq!(app.finished_count(), 103);
        let mut done = agent(&id(2), "succeeded", 0.5);
        done.task_summary = "Workdir: /tmp/one".into();
        app.sessions = sort_sessions(vec![app.sessions.pop().unwrap(), done]);
        assert_eq!(app.finished_in_scope(), 1, "loaded rows of the project");
    }

    /// Answer redraws share cached spans; raw escape payloads never render.
    #[test]
    fn answer_lines_are_cached_and_sanitized_on_receipt() {
        let mut app = App::new();
        let view = serde_json::from_value(json!({"agent_id":id(1),"run_id":id(2),"status":"succeeded","available":true,"path":null,"size_bytes":null,"sha256":null,"content":"safe\x1b]hidden\x07 answer","inline_complete":true})).unwrap();
        app.apply_answer(view);
        let mut terminal =
            ratatui::Terminal::new(ratatui::backend::TestBackend::new(100, 30)).unwrap();
        for _ in 0..2 {
            terminal
                .draw(|f| crate::ui::answer::render(f, &app, f.area()))
                .unwrap();
        }
        let screen: String = terminal
            .backend()
            .buffer()
            .content
            .iter()
            .map(|cell| cell.symbol())
            .collect();
        assert!(screen.contains("safe answer"));
        assert_eq!(app.answer_lines.borrow().builds, 1);
        assert!(!screen.contains("hidden"));
        app.close_answer();
        assert!(app.answer.is_none());
    }
}
