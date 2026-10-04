//! Input, ticking, and broker watchers driving the application state.
//!
//! Three async workers feed one event loop: the terminal input thread, the
//! sessions watcher (long-polling the committed store revision), and the
//! transcript watcher (cursor paging one selected session). The event loop
//! owns the [`App`] state, applies every delivered event through cheap
//! reducers as it arrives, dispatches worker commands, and draws at most once
//! per [`FRAME_INTERVAL`] ([`Pipeline`]): any number of events between two
//! frames costs one draw of the state as it stands at the frame.

use crate::app::App;
use crate::net::{self, Broker, SharedBroker};
use agent_run_domain::domain::AgentId;
use agent_run_domain::views::{AgentPage, AnswerView, TranscriptPage};
use ratatui::backend::Backend;
use ratatui::backend::CrosstermBackend;
use ratatui::crossterm::event::{
    DisableMouseCapture, EnableMouseCapture, Event as TerminalEvent, KeyCode, KeyEvent,
    KeyEventKind, KeyModifiers, MouseButton, MouseEvent, MouseEventKind,
};
use ratatui::Terminal;
use std::io::Stdout;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::{mpsc, watch};

/// Delay before a broker watcher retries after a failed round trip.
const RETRY_DELAY: Duration = Duration::from_secs(2);
/// Idle poll interval of the transcript tail while the session is open.
///
/// Engines stream in bursts; a fast cadence keeps follow mode visually at
/// the tail instead of chasing it a second behind.
const TRANSCRIPT_TAIL_POLL: Duration = Duration::from_millis(300);
/// Terminal redraw cadence for the spinner and locally derived clocks.
const TICK: Duration = Duration::from_secs(1);
/// Session rows the selection logic assumes between redraws; rendering
/// clamps with the real height, this only keeps `list_scroll` current.
const ASSUMED_CARD_ROWS: usize = 12;
/// Minimum spacing between sessions listings once data is flowing; bursts
/// of revision commits coalesce into one fetch per interval.
const LISTING_INTERVAL: Duration = Duration::from_millis(500);
/// Minimum spacing between two draws: the frame-rate cap (33 ms, ~30 fps).
///
/// Events apply to the state as they arrive; draws are paced by
/// [`FramePacer`], so a firehose of broker commits or input costs at most
/// one draw per interval.
pub const FRAME_INTERVAL: Duration = Duration::from_millis(33);

/// Events produced by the terminal (keys, mouse, resizes).
#[derive(Debug)]
pub enum UiEvent {
    /// A terminal input event.
    Terminal(TerminalEvent),
    /// The input thread died; the loop should exit.
    InputClosed,
}

/// Events produced by broker calls.
///
/// Failures travel as bounded display text: the TUI never interprets typed
/// broker errors, it only shows them in the status bar.
#[derive(Debug)]
pub enum BrokerEvent {
    /// One sessions page for the active scope.
    Sessions(Result<AgentPage, String>),
    /// One transcript page of the selected session.
    Transcript {
        /// Stable agent id the page was requested for.
        agent: AgentId,
        /// The page, or the bounded failure reason.
        page: Result<TranscriptPage, String>,
    },
    /// One answer envelope of the selected session.
    Answer(Result<AnswerView, String>),
    /// Exact finished-session total the broker counted over every session;
    /// delivered alongside the listings that observed a new revision.
    FinishedTotal(usize),
    /// Whole discovery round; stale offsets are ignored.
    Pools {
        /// Discovery offset used for both state-filtered pages.
        offset: usize,
        /// Open and completed pages, or a bounded read error.
        page: Result<
            (
                agent_run_domain::views::ListPoolsView,
                agent_run_domain::views::ListPoolsView,
            ),
            String,
        >,
    },
    /// Public pool read and requested reverse cursor.
    Pool {
        /// Stable identity of the requested buffer.
        id: agent_run_domain::pool::PoolId,
        /// Explicit reverse cursor, absent for initial tail/forward polling.
        before: Option<u64>,
        /// Status and entries, or a bounded read error.
        page: Result<Box<crate::pools::Page>, String>,
    },
    /// Public status read for a selected roster member.
    PoolMember(Result<Box<agent_run_domain::AgentView>, String>),
}

/// Commands addressed to the transcript watcher.
#[derive(Debug, Clone)]
pub enum WatchCommand {
    /// Follow one session after its forward cursor. Zero opens at the tail;
    /// restored buffers resume where they left off. Raw fallback starts at zero.
    Watch {
        /// Stable agent id to follow.
        agent: AgentId,
        /// Exact execution to pin, when known.
        run: Option<AgentId>,
        /// Exclusive forward sequence; zero requests the initial tail page.
        cursor: i64,
    },
    /// Open at the tail with a viewport-sized block count (1..=200).
    Tail {
        /// Stable session selector.
        agent: AgentId,
        /// Exact execution when known.
        run: Option<AgentId>,
        /// Approximately two screens of blocks, bounded by the wire cap.
        blocks: usize,
    },
    /// Fetch one older page, then resume forward follow without rewinding.
    Older {
        /// Stable session selector.
        agent: AgentId,
        /// Exact execution when known.
        run: Option<AgentId>,
        /// Current forward resume cursor.
        cursor: i64,
        /// Exclusive upper sequence of the older page.
        before: i64,
    },
    /// Stop following; the watcher idles until the next command.
    Clear,
}

/// The sessions scope currently requested by the operator.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Scope {
    /// Whether terminal (finished) sessions are included.
    pub show_all: bool,
    /// Monotonic request token: every dispatch bumps it so a forced refresh
    /// (`r`) wakes the watcher even when the scope itself is unchanged.
    pub token: u64,
}

/// Runs one broker listing round for the scope and revision.
async fn fetch_sessions(
    broker: &dyn Broker,
    scope: Scope,
    revision: Option<i64>,
) -> agent_run::Result<AgentPage> {
    match revision {
        None => net::list_agents(broker, !scope.show_all).await,
        Some(after_revision) => {
            net::list_after_revision(broker, after_revision, net::REVISION_WAIT_SECONDS).await
        }
    }
}

/// Long-polls the sessions list and reports every page.
///
/// The watcher reissues a full listing after every failure and whenever the
/// operator flips the scope or forces a refresh; a successful round carries
/// the fresh revision for the next long-poll watch. Once data is flowing,
/// fetches throttle to one per [`LISTING_INTERVAL`] so a busy broker (many
/// revision commits per second) cannot flood the UI thread with listings;
/// the first load and every scope change stay immediate.
pub async fn sessions_worker(
    broker: SharedBroker,
    mut scope: watch::Receiver<Scope>,
    tx: mpsc::Sender<BrokerEvent>,
) {
    let mut current = *scope.borrow();
    let mut revision: Option<i64> = None;
    let mut last_fetch = tokio::time::Instant::now() - LISTING_INTERVAL;
    // Live-session total of the latest page and the store revision the
    // finished count was taken at, so the finished total refreshes once per
    // changed revision under the same listing throttle.
    let mut live: usize = 0;
    let mut counted: Option<i64> = None;
    loop {
        tokio::select! {
            changed = scope.changed() => {
                if changed.is_err() {
                    return;
                }
                current = *scope.borrow();
                revision = None;
                last_fetch = tokio::time::Instant::now() - LISTING_INTERVAL;
            }
            result = async {
                if revision.is_some() {
                    let elapsed = last_fetch.elapsed();
                    if elapsed < LISTING_INTERVAL {
                        tokio::time::sleep(LISTING_INTERVAL - elapsed).await;
                    }
                }
                last_fetch = tokio::time::Instant::now();
                fetch_sessions(&*broker, current, revision).await
            } => {
                match result {
                    Ok(page) => {
                        // A live-filtered page carries its exact total; a
                        // complete wide page counts its own rows; an
                        // incomplete wide page keeps the previous total
                        // (rows were truncated at the page limit).
                        if !current.show_all && revision.is_none() {
                            live = page.total;
                        } else if page.complete {
                            live = page
                                .items
                                .iter()
                                .filter(|a| !a.status.terminal())
                                .count();
                        }
                        revision = Some(page.revision);
                        if tx.send(BrokerEvent::Sessions(Ok(page))).await.is_err() {
                            return;
                        }
                        // Refresh the broker-counted finished total only
                        // when the store moved past the counted revision: a
                        // failure keeps the last known count and retries
                        // with the next listing round.
                        if counted != revision {
                            if let Ok(total_page) = net::total_agents(&*broker).await {
                                counted = Some(total_page.revision);
                                let finished = total_page.total.saturating_sub(live);
                                if tx
                                    .send(BrokerEvent::FinishedTotal(finished))
                                    .await
                                    .is_err()
                                {
                                    return;
                                    }
                            }
                        }
                    }
                    Err(error) => {
                        revision = None;
                        if tx.send(BrokerEvent::Sessions(Err(error.to_string()))).await.is_err() {
                            return;
                        }
                        tokio::time::sleep(RETRY_DELAY).await;
                    }                }
            }
        }
    }
}

/// First retry delay after a failed transcript page.
const TRANSCRIPT_RETRY_BASE: Duration = Duration::from_millis(150);
/// Upper bound of the transcript retry backoff.
const TRANSCRIPT_RETRY_CAP: Duration = Duration::from_secs(2);

/// The next retry delay after `previous`: 150 ms doubling to the 2 s cap.
fn next_retry_delay(previous: Duration) -> Duration {
    if previous < TRANSCRIPT_RETRY_BASE {
        TRANSCRIPT_RETRY_BASE
    } else {
        previous.saturating_mul(2).min(TRANSCRIPT_RETRY_CAP)
    }
}

/// Opens at the newest blocks, fetches older pages on demand, and follows
/// forward from the reverse page's resume cursor even when complete is true.
/// Sends through the bounded UI queue before fetching another page. Commands
/// cancel both I/O and delivery; unsupported blocks fall back once per worker
/// to legacy raw backfill. Other failures retain the retry/backoff policy.
pub async fn transcript_worker(
    broker: SharedBroker,
    mut commands: watch::Receiver<WatchCommand>,
    tx: mpsc::Sender<BrokerEvent>,
) {
    use agent_run_domain::transcript::{TranscriptQuery, TranscriptView};
    let mut raw = false;
    'target: loop {
        let command = commands.borrow_and_update().clone();
        let (agent, run, mut cursor, mut tail, mut before) = match command {
            WatchCommand::Watch { agent, run, cursor } => {
                (agent, run, cursor, (cursor == 0).then_some(48), None)
            }
            WatchCommand::Tail { agent, run, blocks } => {
                (agent, run, 0, Some(blocks.clamp(1, 200)), None)
            }
            WatchCommand::Older {
                agent,
                run,
                cursor,
                before,
            } => (agent, run, cursor, None, Some(before)),
            WatchCommand::Clear => {
                if commands.changed().await.is_err() {
                    return;
                }
                continue;
            }
        };
        let mut pause = Duration::ZERO;
        let mut retry_delay = Duration::ZERO;
        loop {
            tokio::select! {
                biased;
                changed = commands.changed() => {
                    if changed.is_err() { return; }
                    continue 'target;
                }
                _ = tokio::time::sleep(pause) => {}
            }
            let reverse = !raw && (tail.is_some() || before.is_some());
            let query = TranscriptQuery {
                cursor: if reverse { 0 } else { cursor },
                limit: 200,
                view: TranscriptView::Blocks,
                tail_blocks: tail,
                before_cursor: before,
            };
            let result = tokio::select! {
                biased;
                changed = commands.changed() => {
                    if changed.is_err() { return; }
                    continue 'target;
                }
                result = async {
                    if raw {
                        net::transcript_page(&*broker, &agent, run.as_ref(), cursor, net::TRANSCRIPT_PAGE_LIMIT).await
                    } else {
                        net::transcript_blocks(&*broker, &agent, run.as_ref(), query).await
                    }
                } => result,
            };
            let page = match result {
                Ok(page) => {
                    // Some old brokers ignore extra arguments and return raw.
                    raw |= page.view != Some(TranscriptView::Blocks);
                    retry_delay = Duration::ZERO;
                    pause = if reverse || page.next_cursor.is_some() {
                        Duration::ZERO
                    } else {
                        TRANSCRIPT_TAIL_POLL
                    };
                    let resume = page
                        .resume_cursor
                        .or(page.next_cursor)
                        .or_else(|| page.messages.last().map(|m| m.seq));
                    if let Some(resume) = resume {
                        cursor = cursor.max(resume);
                    }
                    tail = None;
                    before = None;
                    Ok(page)
                }
                Err(error) if !raw && net::blocks_unsupported(&error) => {
                    raw = true;
                    tail = None;
                    before = None;
                    cursor = 0;
                    pause = Duration::ZERO;
                    continue;
                }
                Err(error) => {
                    retry_delay = next_retry_delay(retry_delay);
                    pause = retry_delay;
                    Err(error.to_string())
                }
            };
            tokio::select! {
                biased;
                changed = commands.changed() => {
                    if changed.is_err() { return; }
                    continue 'target;
                }
                sent = tx.send(BrokerEvent::Transcript { agent: agent.clone(), page }) => {
                    if sent.is_err() { return; }
                }
            }
        }
    }
}

/// Actions the event loop derives from one key press.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Action {
    /// No-op.
    None,
    /// Leave the application.
    Quit,
    /// Move the sessions selection.
    Move(i64),
    /// Open the selected session's transcript.
    Open,
    /// Close the transcript or popup view.
    Back,
    /// Flip the finished-sessions dropdown.
    ToggleCompleted,
    /// Toggle the project picker popup.
    Projects,
    /// Move the project picker cursor; negative moves up.
    PickerMove(i64),
    /// Apply the project under the picker cursor.
    PickerApply,
    /// Expand or collapse the tool payload under the transcript cursor.
    ToggleExpand,
    /// Move the transcript message cursor; negative moves up.
    CursorMove(i64),
    /// Force an immediate sessions refetch.
    Refresh,
    /// Toggle transcript follow mode.
    ToggleFollow,
    /// Scroll the transcript; negative moves toward the top.
    ScrollTranscript(i64),
    /// Request the selected session's answer envelope.
    Answer,
    /// Jump to the transcript top.
    Top,
    /// Jump to the transcript tail.
    Bottom,
    /// Toggle the key-help overlay.
    Help,
    /// Switch Sessions (false) / Pools (true), preserving cached buffers.
    Tab(bool),
    /// Pure read-only pool navigation.
    Pool(crate::pools::Action),
}

/// Maps one key press to an action given the current screen and popup state.
pub fn key_action(app: &App, key: KeyEvent) -> Action {
    if key.kind != KeyEventKind::Press {
        return Action::None;
    }
    if key.modifiers.contains(KeyModifiers::CONTROL) && key.code == KeyCode::Char('c') {
        return Action::Quit;
    }
    if app.answer.is_some() {
        return match key.code {
            KeyCode::Esc | KeyCode::Char('a') | KeyCode::Char('q') => Action::Back,
            _ => Action::None,
        };
    }
    if app.help {
        return match key.code {
            KeyCode::Esc | KeyCode::Char('?') | KeyCode::Char('q') => Action::Help,
            _ => Action::None,
        };
    }
    if app.project_picker {
        return match key.code {
            KeyCode::Esc | KeyCode::Char('p') => Action::Projects,
            KeyCode::Up | KeyCode::Char('k') => Action::PickerMove(-1),
            KeyCode::Down | KeyCode::Char('j') => Action::PickerMove(1),
            KeyCode::Enter => Action::PickerApply,
            _ => Action::None,
        };
    }
    match key.code {
        KeyCode::Char('1') => return Action::Tab(false),
        KeyCode::Char('2') => return Action::Tab(true),
        KeyCode::BackTab => return Action::Tab(!app.pools.visible),
        _ => {}
    }
    if app.pools.visible && !app.pools.member_transcript {
        return crate::pools::key(app, key.code);
    }
    match app.screen {
        crate::app::Screen::Sessions => match key.code {
            KeyCode::Char('q') => Action::Quit,
            KeyCode::Up | KeyCode::Char('k') => Action::Move(-1),
            KeyCode::Down | KeyCode::Char('j') => Action::Move(1),
            KeyCode::PageUp => Action::Move(-10),
            KeyCode::PageDown => Action::Move(10),
            KeyCode::Enter | KeyCode::Right => Action::Open,
            KeyCode::Tab | KeyCode::Char('o') => Action::ToggleCompleted,
            KeyCode::Char('p') => Action::Projects,
            KeyCode::Char('r') => Action::Refresh,
            KeyCode::Char('a') => Action::Answer,
            KeyCode::Char('?') => Action::Help,
            _ => Action::None,
        },
        crate::app::Screen::Transcript => match key.code {
            KeyCode::Char('q') => Action::Quit,
            KeyCode::Esc | KeyCode::Left | KeyCode::Backspace => Action::Back,
            KeyCode::Up | KeyCode::Char('k') => Action::CursorMove(-1),
            KeyCode::Down | KeyCode::Char('j') => Action::CursorMove(1),
            KeyCode::PageUp => Action::ScrollTranscript(-10),
            KeyCode::PageDown => Action::ScrollTranscript(10),
            KeyCode::Home | KeyCode::Char('g') => Action::Top,
            KeyCode::End | KeyCode::Char('G') => Action::Bottom,
            KeyCode::Enter | KeyCode::Char(' ') => Action::ToggleExpand,
            KeyCode::Char('f') => Action::ToggleFollow,
            KeyCode::Char('a') => Action::Answer,
            KeyCode::Char('?') => Action::Help,
            _ => Action::None,
        },
    }
}

/// Side effects the event loop must dispatch after applying an action.
#[derive(Debug)]
pub enum Dispatched {
    /// Nothing to dispatch.
    None,
    /// Follow the opened session, resuming from the cursor.
    Watch(AgentId, Option<AgentId>, i64),
    /// Stop transcript watching.
    Clear,
    /// Reissue the sessions listing for the current scope.
    Scope,
    /// Fetch one older history page; forward follow resumes afterward.
    Older(AgentId, Option<AgentId>, i64, i64),
    /// Fetch the answer of one session.
    Answer(AgentId, Option<AgentId>),
    /// Fetch a pool member status before watching the transcript.
    PoolMember(AgentId),
}

/// Applies one action to the state and returns its side effects.
pub fn apply_action(app: &mut App, action: Action) -> Dispatched {
    match action {
        Action::Tab(pools) => {
            if app.pools.visible != pools {
                app.close_transcript();
                app.pools.visible = pools;
                app.pools.member_transcript = false;
                app.pools.member_watch = false;
                app.pools.pending_member = None;
                app.dirty = true;
                return Dispatched::Clear;
            }
            Dispatched::None
        }
        Action::Pool(action) => crate::pools::apply(app, action),
        Action::None => Dispatched::None,
        Action::Quit => {
            app.quit = true;
            Dispatched::None
        }
        Action::Move(delta) => {
            if app.pools.visible && !app.pools.member_transcript {
                app.pools.focused = false;
                return crate::pools::apply(app, crate::pools::Action::Move(delta));
            }
            app.move_selection(delta);
            app.sync_list_scroll(ASSUMED_CARD_ROWS);
            Dispatched::None
        }
        Action::Open => match app.open_selected_transcript() {
            Some((agent, run, cursor)) => Dispatched::Watch(agent, run, cursor),
            None => Dispatched::None,
        },
        Action::Back => {
            if app.answer.is_some() {
                app.close_answer();
                Dispatched::None
            } else if app.pools.member_transcript {
                app.close_transcript();
                app.pools.member_transcript = false;
                app.pools.focused = true;
                Dispatched::Clear
            } else if app.screen == crate::app::Screen::Transcript {
                app.screen = crate::app::Screen::Sessions;
                if app.split_view() {
                    // The split view keeps the pane glued to the selection,
                    // so the watcher stays attached to it.
                    Dispatched::None
                } else {
                    app.close_transcript();
                    Dispatched::Clear
                }
            } else {
                Dispatched::None
            }
        }
        Action::ToggleCompleted => {
            app.toggle_completed();
            Dispatched::Scope
        }
        Action::Projects => {
            if app.project_picker {
                app.close_project_picker();
            } else {
                app.open_project_picker();
            }
            Dispatched::None
        }
        Action::PickerMove(delta) => {
            app.move_picker_cursor(delta);
            Dispatched::None
        }
        Action::PickerApply => {
            app.apply_project_picker();
            Dispatched::None
        }
        Action::CursorMove(delta) => {
            if let Some(buffer) = app.transcript.as_mut() {
                crate::ui::transcript::move_cursor_block(buffer, delta);
                scroll_cursor_into_view(app);
            }
            Dispatched::None
        }
        Action::ToggleExpand => {
            if let Some(buffer) = app.transcript.as_mut() {
                buffer.toggle_expanded_at_cursor();
            }
            Dispatched::None
        }
        Action::Refresh => {
            app.loaded = false;
            Dispatched::Scope
        }
        Action::ToggleFollow => {
            if let Some(buffer) = app.transcript.as_mut() {
                buffer.toggle_follow();
            }
            Dispatched::None
        }
        Action::ScrollTranscript(delta) => {
            if app.pools.visible && !app.pools.member_transcript {
                app.pools.focused = true;
                return crate::pools::apply(app, crate::pools::Action::Scroll(delta));
            }
            scroll_transcript(app, delta);
            Dispatched::None
        }
        Action::Top => {
            if let Some(buffer) = app.transcript.as_mut() {
                buffer.scroll_top();
            }
            scroll_transcript(app, 0);
            Dispatched::None
        }
        Action::Bottom => {
            if let Some(buffer) = app.transcript.as_mut() {
                buffer.scroll_bottom();
            }
            Dispatched::None
        }
        Action::Answer => match if app.pools.member_transcript {
            app.transcript.as_ref().map_or((None, None), |b| {
                (Some(b.agent.agent_id.clone()), b.agent.run_id.clone())
            })
        } else {
            (app.selected_agent_id(), app.selected_run_id())
        } {
            (Some(agent), run) => Dispatched::Answer(agent, run),
            (None, _) => Dispatched::None,
        },
        Action::Help => {
            app.help = !app.help;
            Dispatched::None
        }
    }
}

/// Scrolls the transcript so the message cursor stays on screen.
///
/// The layout is recomputed with the last rendered width, matching the
/// renderer; the scroll lands with the cursor line at the viewport top when
/// moving down and just above it when moving up.
fn scroll_cursor_into_view(app: &mut App) {
    let Some(pane) = crate::ui::panes(app, app.last_width, app.last_height).transcript else {
        return;
    };
    let width = crate::ui::transcript::body_width(pane);
    let Some(buffer) = app.transcript.as_ref() else {
        return;
    };
    let Some(cursor_line) = crate::ui::transcript::cursor_line(buffer, width) else {
        return;
    };
    let total = crate::ui::transcript::total_height(buffer, width);
    let viewport = pane
        .height
        .saturating_sub(crate::ui::transcript::HEADER_ROWS) as usize;
    if viewport == 0 {
        return;
    }
    let buffer = app.transcript.as_mut().expect("checked above");
    if cursor_line < viewport {
        // Cursor is near the top: jump there and leave follow mode.
        buffer.follow = false;
        buffer.from_bottom = total.saturating_sub(viewport);
    } else {
        buffer.follow = false;
        let from_top = cursor_line.saturating_sub(viewport.saturating_sub(1));
        buffer.from_bottom = total.saturating_sub(from_top.saturating_add(viewport));
    }
}

/// Applies one signed transcript scroll; negative moves toward the top.
pub fn scroll_transcript(app: &mut App, delta: i64) {
    let max = crate::ui::panes(app, app.last_width, app.last_height)
        .transcript
        .and_then(|pane| {
            let buffer = app.transcript.as_ref()?;
            let viewport = usize::from(
                pane.height
                    .saturating_sub(crate::ui::transcript::HEADER_ROWS),
            );
            Some(
                crate::ui::transcript::total_height(
                    buffer,
                    crate::ui::transcript::body_width(pane),
                )
                .saturating_sub(viewport),
            )
        });
    let Some(buffer) = app.transcript.as_mut() else {
        return;
    };
    if buffer.from_bottom == usize::MAX {
        if let Some(max) = max {
            buffer.from_bottom = max;
        }
    }
    // Coalesced wheel bursts can exceed the row range; clamp, never wrap.
    let lines = usize::try_from(delta.unsigned_abs()).unwrap_or(usize::MAX);
    if delta < 0 {
        buffer.scroll_up(lines);
    } else if delta > 0 {
        buffer.scroll_down(lines);
    }
}

/// Requests older blocks once the scrolled viewport is within half a screen
/// of the loaded beginning. Marks the in-flight state before dispatch so
/// input bursts and broker refreshes cannot enqueue duplicate reverse reads.
fn older_if_needed(app: &mut App) -> Dispatched {
    let Some(pane) = crate::ui::panes(app, app.last_width, app.last_height).transcript else {
        return Dispatched::None;
    };
    let Some(buffer) = app.transcript.as_mut() else {
        return Dispatched::None;
    };
    let viewport = usize::from(
        pane.height
            .saturating_sub(crate::ui::transcript::HEADER_ROWS),
    );
    let total =
        crate::ui::transcript::total_height(buffer, crate::ui::transcript::body_width(pane));
    let offset = crate::ui::transcript::scroll_offset(buffer, total, viewport);
    if !buffer.follow && !buffer.loading_older && offset <= (viewport / 2).max(3) {
        if let Some(before) = buffer.previous_cursor {
            buffer.loading_older = true;
            app.dirty = true;
            return Dispatched::Older(
                buffer.agent.agent_id.clone(),
                buffer.agent.run_id.clone(),
                buffer.resume_cursor,
                before,
            );
        }
    }
    Dispatched::None
}

/// Terminal handle owning raw mode, the alternate screen, and mouse capture.
pub struct TerminalGuard {
    terminal: Terminal<CrosstermBackend<Stdout>>,
}

impl TerminalGuard {
    /// Enters raw mode, the alternate screen, and mouse capture.
    pub fn enter() -> std::io::Result<Self> {
        let mut stdout = std::io::stdout();
        ratatui::crossterm::terminal::enable_raw_mode()?;
        ratatui::crossterm::execute!(
            stdout,
            ratatui::crossterm::terminal::EnterAlternateScreen,
            EnableMouseCapture
        )?;
        Ok(Self {
            terminal: Terminal::new(CrosstermBackend::new(stdout))?,
        })
    }

    /// Borrows the terminal for one run invocation.
    pub fn terminal(&mut self) -> &mut Terminal<CrosstermBackend<Stdout>> {
        &mut self.terminal
    }
}

impl Drop for TerminalGuard {
    fn drop(&mut self) {
        let _ = ratatui::crossterm::execute!(
            std::io::stdout(),
            ratatui::crossterm::terminal::LeaveAlternateScreen,
            DisableMouseCapture
        );
        let _ = ratatui::crossterm::terminal::disable_raw_mode();
    }
}

/// Spawns the blocking thread that reads terminal input into [`UiEvent`]s.
pub fn spawn_input(tx: mpsc::UnboundedSender<UiEvent>) {
    std::thread::spawn(move || loop {
        match ratatui::crossterm::event::read() {
            Ok(event) => {
                if tx.send(UiEvent::Terminal(event)).is_err() {
                    return;
                }
            }
            Err(_) => {
                let _ = tx.send(UiEvent::InputClosed);
                return;
            }
        }
    });
}

/// Events drained from each queue between two frame-due checks; a firehose
/// therefore never delays a due frame by more than one batch of reducers.
const EVENT_BATCH: usize = 128;

/// Frame-rate cap with leading- and trailing-edge scheduling.
///
/// The pacer only decides *when* a frame may draw; whether anything needs
/// drawing is the caller's dirty state. When nothing was drawn for at least
/// the interval, the next dirty state draws immediately (leading edge, no
/// added latency after idle); later changes within the interval wait for
/// its end and draw together (trailing edge).
#[derive(Debug, Clone, Copy)]
pub struct FramePacer {
    /// Minimum spacing between two draws; [`FRAME_INTERVAL`] in production,
    /// zero disables the cap (probe comparison only).
    interval: Duration,
    /// Start instant of the latest draw; `None` before the first frame.
    last_draw: Option<Instant>,
}

impl FramePacer {
    /// A pacer spacing draws at least `interval` apart; nothing drawn yet.
    pub fn new(interval: Duration) -> Self {
        Self {
            interval,
            last_draw: None,
        }
    }

    /// The earliest instant the next draw may start, given the current
    /// instant: `now` itself on the leading edge (first frame, or the last
    /// draw is at least one interval old), otherwise the end of the running
    /// interval.
    pub fn due(&self, now: Instant) -> Instant {
        match self.last_draw {
            Some(last) => (last + self.interval).max(now),
            None => now,
        }
    }

    /// Records a draw that started at `at`.
    pub fn record(&mut self, at: Instant) {
        self.last_draw = Some(at);
    }
}

/// Kind of a coalescable navigation step.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Nav {
    /// Session-list selection (↑/↓, PgUp/PgDn, wheel over the list).
    Selection,
    /// Transcript block cursor (↑/↓ on the transcript).
    Cursor,
    /// Transcript scroll (PgUp/PgDn, wheel over the transcript).
    Scroll,
    /// Project picker cursor.
    Picker,
}

impl Nav {
    /// The step of one action, when the action is a coalescable navigation
    /// move; `None` for every other action.
    fn of(action: Action) -> Option<(Nav, i64)> {
        match action {
            Action::Move(delta) => Some((Nav::Selection, delta)),
            Action::CursorMove(delta) => Some((Nav::Cursor, delta)),
            Action::ScrollTranscript(delta) => Some((Nav::Scroll, delta)),
            Action::PickerMove(delta) => Some((Nav::Picker, delta)),
            _ => None,
        }
    }

    /// The action applying `delta` steps of this kind.
    fn action(self, delta: i64) -> Action {
        match self {
            Nav::Selection => Action::Move(delta),
            Nav::Cursor => Action::CursorMove(delta),
            Nav::Scroll => Action::ScrollTranscript(delta),
            Nav::Picker => Action::PickerMove(delta),
        }
    }
}

/// Navigation input accumulated between two frames.
///
/// Wheel, arrow, and page keys arrive in bursts far faster than frames; each
/// step would otherwise run a reducer (and, for the transcript cursor, a
/// layout pass). Instead consecutive steps of one kind and one direction
/// merge into one net delta applied once — at the next frame, or earlier
/// when an input that depends on it arrives. Only same-direction steps
/// merge because the clamping reducers (list bounds, the follow-mode tail)
/// compose exactly only then; a direction change applies the pending run
/// first. Pointer moves keep just the latest position: hover is resolved
/// once per frame against the final layout.
#[derive(Debug, Default)]
pub struct InputBatch {
    /// Pending net navigation step: its kind and signed delta.
    nav: Option<(Nav, i64)>,
    /// Latest pointer position (column, row) of pending mouse moves.
    hover: Option<(u16, u16)>,
}

impl InputBatch {
    /// Adds one navigation step, applying the pending run to `app` first
    /// when the kind or the direction differs.
    pub fn push(&mut self, app: &mut App, kind: Nav, delta: i64) {
        match &mut self.nav {
            Some((pending, total)) if *pending == kind && total.signum() == delta.signum() => {
                *total = total.saturating_add(delta);
            }
            _ => {
                self.flush(app);
                self.nav = Some((kind, delta));
            }
        }
    }

    /// Applies the pending navigation run, if any, in one reducer pass and
    /// marks the frame dirty.
    pub fn flush(&mut self, app: &mut App) {
        if let Some((kind, delta)) = self.nav.take() {
            // Navigation actions never dispatch worker commands.
            let _ = apply_action(app, kind.action(delta));
            app.dirty = true;
        }
    }

    /// Whether any input awaits the next frame.
    pub fn pending(&self) -> bool {
        self.nav.is_some() || self.hover.is_some()
    }
}

/// Event intake and frame scheduling of the event loop, free of channels so
/// tests and probes drive exactly the production logic.
///
/// Every event applies to the state immediately through cheap reducers
/// (navigation bursts coalesce in the [`InputBatch`]); frames are paced by
/// the [`FramePacer`] and render the state as it stands at the frame.
#[derive(Debug)]
pub struct Pipeline {
    /// Navigation and pointer input waiting for the next frame.
    input: InputBatch,
    /// Frame-rate cap.
    pacer: FramePacer,
    /// Last pointer position, retained through scrolling/content changes.
    pointer: Option<(u16, u16)>,
}

impl Pipeline {
    /// A pipeline drawing at most once per `interval`.
    pub fn new(interval: Duration) -> Self {
        Self {
            input: InputBatch::default(),
            pacer: FramePacer::new(interval),
            pointer: None,
        }
    }

    /// Applies one terminal event and returns its worker side effect.
    ///
    /// Navigation keys and wheel steps coalesce, pointer moves only record
    /// their position, and every other input first applies the pending
    /// navigation (it may depend on the moved selection) and then its own
    /// reducer. A closed input stream quits.
    pub fn ui_event(&mut self, app: &mut App, event: UiEvent) -> Dispatched {
        let UiEvent::Terminal(event) = event else {
            app.quit = true;
            return Dispatched::None;
        };
        match event {
            TerminalEvent::Key(key) => {
                let action = key_action(app, key);
                if let Some((kind, delta)) = Nav::of(action) {
                    self.input.push(app, kind, delta);
                    return Dispatched::None;
                }
                if action == Action::None {
                    return Dispatched::None;
                }
                self.input.flush(app);
                app.dirty = true;
                apply_action(app, action)
            }
            TerminalEvent::Mouse(mouse) => match mouse.kind {
                MouseEventKind::Moved => {
                    self.input.hover = Some((mouse.column, mouse.row));
                    Dispatched::None
                }
                MouseEventKind::ScrollUp | MouseEventKind::ScrollDown => {
                    if let Some((kind, delta)) = wheel_step(app, &mouse) {
                        self.input.push(app, kind, delta);
                    }
                    Dispatched::None
                }
                MouseEventKind::Down(MouseButton::Left) => {
                    self.pointer = Some((mouse.column, mouse.row));
                    self.input.flush(app);
                    app.dirty = true;
                    apply_mouse(app, mouse)
                }
                _ => Dispatched::None,
            },
            TerminalEvent::Resize(_, _) => {
                self.input.flush(app);
                app.dirty = true;
                Dispatched::None
            }
            _ => Dispatched::None,
        }
    }

    /// Applies one broker event to the state.
    ///
    /// A sessions page re-anchors the selection, so pending navigation
    /// applies first; transcript pages (the firehose) never wait on input.
    /// Unchanged pages mark nothing dirty.
    pub fn broker_event(&mut self, app: &mut App, event: BrokerEvent) {
        match event {
            BrokerEvent::Sessions(Ok(page)) => {
                self.input.flush(app);
                app.apply_sessions(&page);
            }
            BrokerEvent::Sessions(Err(message)) => app.apply_broker_error(message),
            BrokerEvent::Transcript {
                agent,
                page: Ok(page),
            } => {
                let _ = app.apply_transcript(&agent, page);
            }
            BrokerEvent::Transcript {
                agent,
                page: Err(message),
            } => app.apply_transcript_error(&agent, message),
            BrokerEvent::Answer(Ok(view)) => app.apply_answer(view),
            BrokerEvent::Answer(Err(message)) => app.apply_broker_error(message),
            BrokerEvent::FinishedTotal(finished) => app.apply_finished_total(finished),
            BrokerEvent::Pools {
                offset,
                page: Ok((open, completed)),
            } => {
                app.dirty |= app.pools.listing(offset, open, completed);
            }
            BrokerEvent::Pools {
                offset,
                page: Err(error),
            } => {
                if offset == app.pools.offset && app.pools.error.as_ref() != Some(&error) {
                    app.pools.error = Some(error);
                    app.dirty = true;
                }
            }
            BrokerEvent::Pool { id, before, page } => {
                let width = crate::pools::width(app);
                if let Some(buffer) = app.pools.buffers.iter_mut().find(|b| b.id == id) {
                    match page {
                        Ok(page) => app.dirty |= buffer.merge(*page, width),
                        Err(error) => {
                            if before == buffer.older {
                                buffer.older = None;
                            }
                            if buffer.error.as_ref() != Some(&error) {
                                buffer.error = Some(error);
                                app.dirty = true;
                            }
                        }
                    }
                }
            }
            BrokerEvent::PoolMember(Ok(agent)) => {
                let _ = crate::pools::open_member(app, *agent);
            }
            BrokerEvent::PoolMember(Err(error)) => {
                app.pools.error = Some(error);
                app.dirty = true;
            }
        }
    }

    /// When the next frame should run: `None` while nothing is dirty and no
    /// input is pending (no draws at all when idle), otherwise the pacer's
    /// due instant for `now`.
    pub fn next_frame(&self, app: &App, now: Instant) -> Option<Instant> {
        (app.dirty || self.input.pending()).then(|| self.pacer.due(now))
    }

    /// Readies the state for a frame: applies pending navigation, resolves
    /// the latest pointer position into hover targets (dirty only when one
    /// changed), and keeps the split view's transcript pane glued to the
    /// selection. Returns a re-attachment command or one older-page request
    /// when scrolling approaches the loaded beginning.
    pub fn prepare_frame(&mut self, app: &mut App) -> Dispatched {
        self.input.flush(app);
        if app.pools.visible {
            if app.pools.member_transcript && app.pools.member_watch {
                app.pools.member_watch = false;
                if let Some(b) = &app.transcript {
                    return Dispatched::Watch(
                        b.agent.agent_id.clone(),
                        b.agent.run_id.clone(),
                        b.next_cursor,
                    );
                }
            }
            if !app.pools.member_transcript {
                if let Some(position) = self.input.hover.take() {
                    self.pointer = Some(position);
                }
                crate::pools::older(app);
                return Dispatched::None;
            }
        }
        if let Some((column, row)) = self.input.hover.take() {
            self.pointer = Some((column, row));
            if apply_hover(app, column, row) {
                app.dirty = true;
            }
        }
        if app.screen == crate::app::Screen::Sessions && app.split_view() && app.transcript_stale()
        {
            app.dirty = true;
            if let Some((agent, run, cursor)) = app.watch_selected() {
                return Dispatched::Watch(agent, run, cursor);
            }
            // Nothing is visible to select (e.g. the finished section
            // collapsed away): the pane must not keep showing a session the
            // list no longer holds — clear it and stop the watcher.
            app.close_transcript();
            return Dispatched::Clear;
        }
        older_if_needed(app)
    }

    /// Draws one frame when the state is dirty and returns whether it drew.
    ///
    /// Records the draw with the pacer at `now` (the frame start) and the
    /// rendered size for hit-testing. A size change that flips the layout
    /// leaves the state dirty so the next frame re-runs [`Pipeline::prepare_frame`]
    /// against the new panes.
    ///
    /// # Errors
    ///
    /// Returns [`agent_run::Error::Runtime`] when the terminal write fails.
    pub fn draw<B: Backend>(
        &mut self,
        app: &mut App,
        terminal: &mut Terminal<B>,
        now: Instant,
    ) -> agent_run::Result<bool> {
        if !app.dirty {
            return Ok(false);
        }
        app.dirty = false;
        let completed = terminal
            .draw(|frame| {
                capture_hits(app, frame.area());
                if let Some((column, row)) = self.pointer {
                    apply_hover(app, column, row);
                }
                crate::ui::render(frame, app);
            })
            .map_err(|error| agent_run::Error::Runtime(format!("terminal draw failed: {error}")))?;
        let (width, height) = (completed.area.width.max(1), completed.area.height.max(1));
        let split_flipped = app.split_view() != (width >= crate::app::SPLIT_WIDTH);
        app.last_width = width;
        app.last_height = height;
        if split_flipped {
            app.dirty = true;
        }
        self.pacer.record(now);
        Ok(true)
    }
}

/// The worker plumbing the event loop owns: watchers and dispatch targets.
struct Loop {
    /// Transcript watcher commands.
    watch_tx: watch::Sender<WatchCommand>,
    /// Sessions-scope updates.
    scope_tx: watch::Sender<Scope>,
    /// Monotonic scope token; every dispatch bumps it.
    scope_token: std::cell::Cell<u64>,
    /// Broker handle for one-off fetches.
    broker: SharedBroker,
    /// Broker event sink.
    broker_tx: mpsc::Sender<BrokerEvent>,
}

impl Loop {
    /// Dispatches one action's side effects to the workers.
    fn dispatch(&self, app: &App, dispatched: Dispatched) {
        match dispatched {
            Dispatched::None => {}
            Dispatched::Watch(agent, run, cursor) => {
                let command = if cursor == 0
                    && app
                        .transcript
                        .as_ref()
                        .is_some_and(|buffer| buffer.messages.is_empty())
                {
                    WatchCommand::Tail {
                        agent,
                        run,
                        blocks: usize::from(
                            app.last_height
                                .saturating_sub(crate::ui::transcript::HEADER_ROWS + 3),
                        )
                        .saturating_mul(2)
                        .clamp(1, 200),
                    }
                } else {
                    WatchCommand::Watch { agent, run, cursor }
                };
                let _ = self.watch_tx.send(command);
            }
            Dispatched::Older(agent, run, cursor, before) => {
                let _ = self.watch_tx.send(WatchCommand::Older {
                    agent,
                    run,
                    cursor,
                    before,
                });
            }
            Dispatched::Clear => {
                let _ = self.watch_tx.send(WatchCommand::Clear);
            }
            Dispatched::Scope => {
                let token = self.scope_token.get() + 1;
                self.scope_token.set(token);
                let _ = self.scope_tx.send(Scope {
                    show_all: app.completed_open,
                    token,
                });
            }
            Dispatched::PoolMember(agent) => {
                let broker = self.broker.clone();
                let tx = self.broker_tx.clone();
                tokio::spawn(async move {
                    let result = async {
                        Ok::<_, agent_run::Error>(serde_json::from_value(
                            broker
                                .call("status", serde_json::json!({"agent_id": agent}))
                                .await?,
                        )?)
                    }
                    .await
                    .map_err(|e| e.to_string());
                    let _ = tx.send(BrokerEvent::PoolMember(result.map(Box::new))).await;
                });
            }
            Dispatched::Answer(agent, run) => {
                let broker = Arc::clone(&self.broker);
                let tx = self.broker_tx.clone();
                tokio::spawn(async move {
                    let result = net::answer(&*broker, &agent, run.as_ref())
                        .await
                        .map_err(|error| error.to_string());
                    let _ = tx.send(BrokerEvent::Answer(result)).await;
                });
            }
        }
    }
}

/// Runs the interactive loop until the operator quits.
///
/// Event intake is decoupled from drawing: every event applies to the state
/// as it arrives (queued bursts drain in batches), and frames are paced by
/// the [`Pipeline`] — at most one draw per [`FRAME_INTERVAL`], immediately
/// after idle, batched to the end of the interval otherwise, and none at all
/// while nothing is dirty. A tick marks the state dirty only while something
/// animated is visible (spinners, elapsed and idle clocks).
pub async fn run(
    mut app: App,
    broker: SharedBroker,
    terminal: &mut Terminal<CrosstermBackend<Stdout>>,
) -> agent_run::Result<()> {
    let (broker_tx, mut broker_rx) = mpsc::channel::<BrokerEvent>(2);
    let (ui_tx, mut ui_rx) = mpsc::unbounded_channel::<UiEvent>();
    let (watch_tx, watch_rx) = watch::channel(WatchCommand::Clear);
    let (scope_tx, scope_rx) = watch::channel(Scope {
        show_all: app.completed_open,
        token: 0,
    });

    let (pool_tx, pool_rx) = watch::channel(app.pools.request());
    tokio::spawn(crate::pools::worker(
        broker.clone(),
        pool_rx,
        broker_tx.clone(),
    ));
    spawn_input(ui_tx);
    tokio::spawn(sessions_worker(broker.clone(), scope_rx, broker_tx.clone()));
    tokio::spawn(transcript_worker(
        broker.clone(),
        watch_rx,
        broker_tx.clone(),
    ));

    let loop_ctx = Loop {
        watch_tx,
        scope_tx,
        scope_token: std::cell::Cell::new(0),
        broker: broker.clone(),
        broker_tx,
    };
    let mut pipeline = Pipeline::new(FRAME_INTERVAL);

    let mut ticker = tokio::time::interval(TICK);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

    loop {
        pool_tx.send_if_modified(|current| {
            let next = app.pools.request();
            if *current == next {
                false
            } else {
                *current = next;
                true
            }
        });
        let deadline = pipeline.next_frame(&app, Instant::now());
        tokio::select! {
            _ = tokio::time::sleep_until(
                tokio::time::Instant::from_std(deadline.unwrap_or_else(Instant::now)),
            ), if deadline.is_some() => {}
            _ = ticker.tick() => app.tick(),
            event = ui_rx.recv() => {
                let event = event.unwrap_or(UiEvent::InputClosed);
                let dispatched = pipeline.ui_event(&mut app, event);
                loop_ctx.dispatch(&app, dispatched);
            }
            event = broker_rx.recv() => match event {
                Some(event) => pipeline.broker_event(&mut app, event),
                None => app.quit = true,
            },
        }
        // Apply everything already queued before looking at the clock.
        for _ in 0..EVENT_BATCH {
            let Ok(event) = ui_rx.try_recv() else { break };
            let dispatched = pipeline.ui_event(&mut app, event);
            loop_ctx.dispatch(&app, dispatched);
        }
        for _ in 0..EVENT_BATCH {
            let now = Instant::now();
            if pipeline.next_frame(&app, now).is_some_and(|due| due <= now) {
                break;
            }
            let Ok(event) = broker_rx.try_recv() else {
                break;
            };
            pipeline.broker_event(&mut app, event);
            let now = Instant::now();
            if pipeline.next_frame(&app, now).is_some_and(|due| due <= now) {
                break;
            }
        }
        if app.quit {
            return Ok(());
        }
        let now = Instant::now();
        if pipeline.next_frame(&app, now).is_some_and(|due| due <= now) {
            let dispatched = pipeline.prepare_frame(&mut app);
            loop_ctx.dispatch(&app, dispatched);
            pipeline.draw(&mut app, terminal, now)?;
        }
    }
}

/// Stable identities for one terminal row in the last completed draw.
#[derive(Default, Clone)]
struct RowHit {
    /// Session rendered on the list row.
    card: Option<AgentId>,
    /// Agent and message sequence rendered on the transcript row.
    message: Option<(AgentId, i64)>,
    /// Whether this row rendered the finished-section toggle.
    finished: bool,
}

/// Last frame's panes and stable row identities; independent of pending state.
#[derive(Default)]
pub(crate) struct HitMap {
    /// Pane rectangles that were actually drawn.
    panes: crate::ui::Panes,
    /// One stable target record per terminal row.
    rows: Vec<RowHit>,
}

/// Captures pointer targets from exactly the state and dimensions being drawn.
pub(crate) fn capture_hits(app: &App, area: ratatui::layout::Rect) {
    let panes = crate::ui::panes(app, area.width, area.height);
    let cards = app.card_list();
    let list_rows = panes
        .list
        .map(|rect| (rect, crate::ui::list::hit_rows(app, rect)));
    let mut rows = vec![RowHit::default(); usize::from(area.height)];
    for (y, hit) in rows.iter_mut().enumerate() {
        let row = y as u16;
        if let Some((rect, targets)) = &list_rows {
            if let Some((card, finished)) = row
                .checked_sub(rect.y)
                .and_then(|row| targets.get(usize::from(row)))
            {
                hit.card = card
                    .and_then(|card| cards.get(card))
                    .map(|index| app.sessions[*index].agent_id.clone());
                hit.finished = *finished;
            }
        }
        if let Some(rect) = panes.transcript {
            hit.message = current_transcript_message_at(app, rect, row).and_then(|index| {
                app.transcript
                    .as_ref()
                    .map(|buffer| (buffer.agent.agent_id.clone(), buffer.messages[index].seq))
            });
        }
    }
    *app.hits.borrow_mut() = HitMap { panes, rows };
}

/// Panes from the visible frame; before first draw, use the initial geometry.
fn drawn_panes(app: &App) -> crate::ui::Panes {
    let hits = app.hits.borrow();
    if hits.rows.is_empty() {
        crate::ui::panes(app, app.last_width, app.last_height)
    } else {
        hits.panes
    }
}

/// Resolves a visible session identity into its current card position.
fn drawn_card_at(app: &App, rect: ratatui::layout::Rect, column: u16, row: u16) -> Option<usize> {
    let hits = app.hits.borrow();
    if hits.rows.is_empty() {
        return crate::ui::list::card_at(app, rect, column, row);
    }
    if !inside(rect, column, row) {
        return None;
    }
    let agent = hits.rows.get(usize::from(row))?.card.as_ref()?;
    app.card_list()
        .iter()
        .position(|index| &app.sessions[*index].agent_id == agent)
}

/// Whether one terminal position lies inside a pane rectangle.
fn inside(rect: ratatui::layout::Rect, x: u16, y: u16) -> bool {
    x >= rect.x && x < rect.x + rect.width && y >= rect.y && y < rect.y + rect.height
}

/// The navigation step of one wheel event: a transcript scroll over the
/// transcript pane, otherwise a selection move while the list is visible.
fn wheel_step(app: &App, mouse: &MouseEvent) -> Option<(Nav, i64)> {
    let delta = if matches!(mouse.kind, MouseEventKind::ScrollDown) {
        1
    } else {
        -1
    };
    let panes = drawn_panes(app);
    if app.pools.visible && !app.pools.member_transcript {
        let over_detail = panes
            .transcript
            .is_some_and(|rect| inside(rect, mouse.column, mouse.row));
        return Some((
            if over_detail {
                Nav::Scroll
            } else {
                Nav::Selection
            },
            delta,
        ));
    }
    if panes
        .transcript
        .is_some_and(|rect| inside(rect, mouse.column, mouse.row))
    {
        Some((Nav::Scroll, delta))
    } else if panes.list.is_some() {
        Some((Nav::Selection, delta))
    } else {
        None
    }
}

/// Resolves one pointer position into the hover targets (transcript block
/// and session row) and returns whether either changed.
///
/// Hit-testing follows the pane layout of the last rendered frame; a
/// position over neither pane clears both targets. An unchanged target
/// changes nothing, so the frame stays clean.
fn apply_hover(app: &mut App, column: u16, row: u16) -> bool {
    if app.pools.visible && !app.pools.member_transcript {
        return false;
    }
    let panes = drawn_panes(app);
    let hover = panes
        .transcript
        .filter(|rect| inside(*rect, column, row))
        .and_then(|rect| transcript_message_at(app, rect, row));
    let card = panes
        .list
        .filter(|rect| inside(*rect, column, row))
        .and_then(|rect| drawn_card_at(app, rect, column, row));
    let current = app.transcript.as_ref().and_then(|buffer| buffer.hover);
    if current == hover && app.hover_card == card {
        return false;
    }
    if let Some(buffer) = app.transcript.as_mut() {
        buffer.hover = hover;
    }
    app.hover_card = card;
    true
}

/// Applies one mouse event immediately (no coalescing) and returns its side
/// effects.
///
/// Hit-testing follows the pane layout of the last rendered frame, so hover,
/// wheel, and clicks land on the row the operator sees under the pointer.
/// The event loop routes only clicks here; wheel steps and moves go through
/// the [`InputBatch`], which applies the same reducers once per burst.
fn apply_mouse(app: &mut App, mouse: MouseEvent) -> Dispatched {
    if mouse.row == 1
        && mouse.kind == MouseEventKind::Down(MouseButton::Left)
        && !app.help
        && !app.project_picker
        && app.answer.is_none()
        && !app.pools.criteria
    {
        if (1..11).contains(&mouse.column) {
            return apply_action(app, Action::Tab(false));
        }
        if (12..32).contains(&mouse.column) {
            return apply_action(app, Action::Tab(true));
        }
    }
    if app.pools.visible && !app.pools.member_transcript && !app.help {
        return crate::pools::mouse(app, mouse);
    }
    let panes = drawn_panes(app);
    let over = |pane: Option<ratatui::layout::Rect>| {
        pane.is_some_and(|rect| inside(rect, mouse.column, mouse.row))
    };
    match mouse.kind {
        MouseEventKind::ScrollUp | MouseEventKind::ScrollDown => {
            if let Some((kind, delta)) = wheel_step(app, &mouse) {
                let _ = apply_action(app, kind.action(delta));
            }
            Dispatched::None
        }
        MouseEventKind::Moved => {
            apply_hover(app, mouse.column, mouse.row);
            Dispatched::None
        }
        MouseEventKind::Down(MouseButton::Left) => {
            if app.help {
                app.help = false;
                return Dispatched::None;
            }
            if app.project_picker {
                if let Some(row) =
                    crate::ui::projects::row_at(app, main_area(app), mouse.column, mouse.row)
                {
                    if row == 0 {
                        app.clear_project_filter();
                        app.close_project_picker();
                    } else {
                        app.picker_cursor = row - 1;
                        app.apply_project_picker();
                    }
                }
                return Dispatched::None;
            }
            if over(panes.transcript) && app.transcript.is_some() {
                let pane = panes.transcript.expect("checked above");
                mouse_click_transcript(app, pane, &mouse);
                return Dispatched::None;
            }
            match panes.list {
                Some(rect) if over(Some(rect)) => mouse_click_sessions(app, mouse, rect),
                _ => Dispatched::None,
            }
        }
        _ => Dispatched::None,
    }
}

/// Handles one list-pane click: section toggle, row select, row open.
fn mouse_click_sessions(
    app: &mut App,
    mouse: MouseEvent,
    area: ratatui::layout::Rect,
) -> Dispatched {
    let finished = {
        let hits = app.hits.borrow();
        if hits.rows.is_empty() {
            crate::ui::list::finished_header_clicked(app, area, mouse.row)
        } else {
            hits.rows
                .get(usize::from(mouse.row))
                .is_some_and(|hit| hit.finished)
        }
    };
    if finished {
        app.toggle_completed();
        return Dispatched::Scope;
    }
    let Some(card) = drawn_card_at(app, area, mouse.column, mouse.row) else {
        return Dispatched::None;
    };
    let already_selected = app.selected == card;
    app.select_card(card);
    app.sync_list_scroll(ASSUMED_CARD_ROWS);
    if already_selected {
        match app.open_selected_transcript() {
            Some((agent, run, cursor)) => Dispatched::Watch(agent, run, cursor),
            None => Dispatched::None, // pane already buffers the selection
        }
    } else {
        Dispatched::None
    }
}

/// Handles one transcript click: selects and toggles the message under it.
fn mouse_click_transcript(app: &mut App, area: ratatui::layout::Rect, mouse: &MouseEvent) {
    let Some(message) = transcript_message_at(app, area, mouse.row) else {
        return;
    };
    let buffer = app
        .transcript
        .as_mut()
        .expect("hover target implies open transcript");
    buffer.cursor = message;
    buffer.toggle_expanded_at_cursor();
}

/// The transcript message under one terminal row of the pane, if any.
///
/// Recomputes the body offset exactly like the renderer: the layout and the
/// scroll position both derive from the same state, so hover and click land
/// on the message the operator sees under the pointer.
fn current_transcript_message_at(
    app: &App,
    area: ratatui::layout::Rect,
    row: u16,
) -> Option<usize> {
    let width = crate::ui::transcript::body_width(area);
    let header = crate::ui::transcript::HEADER_ROWS;
    let body_row = row.checked_sub(area.y + header)?;
    if body_row >= area.height.saturating_sub(header) {
        return None; // below the body
    }
    let buffer = app.transcript.as_ref()?;
    let viewport = (area.height - header) as usize;
    let total = crate::ui::transcript::total_height(buffer, width);
    let offset = crate::ui::transcript::scroll_offset(buffer, total, viewport);
    let clicked_line = offset + body_row as usize;
    crate::ui::transcript::message_at(buffer, width, clicked_line)
}

/// Resolves a last-drawn message sequence in the current buffer; stale agents
/// and removed/replaced identities yield no target instead of a different row.
fn transcript_message_at(app: &App, area: ratatui::layout::Rect, row: u16) -> Option<usize> {
    let hits = app.hits.borrow();
    if hits.rows.is_empty() {
        return current_transcript_message_at(app, area, row);
    }
    let (agent, seq) = hits.rows.get(usize::from(row))?.message.as_ref()?;
    let buffer = app.transcript.as_ref()?;
    if &buffer.agent.agent_id != agent {
        return None;
    }
    buffer
        .messages
        .binary_search_by_key(seq, |message| message.seq)
        .ok()
}

/// The full frame area of the last rendered draw, for popup hit-testing.
fn main_area(app: &App) -> ratatui::layout::Rect {
    ratatui::layout::Rect {
        x: 0,
        y: 0,
        width: app.last_width,
        height: app.last_height,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::str::FromStr;

    const STABLE: &str = "ag-20260928-101500-aaaaaaaaaa";
    const RUN: &str = "ag-20260928-101500-bbbbbbbbbb";

    fn key(code: KeyCode) -> KeyEvent {
        KeyEvent::new(code, KeyModifiers::NONE)
    }

    fn answer_view(available: bool) -> AnswerView {
        serde_json::from_value(serde_json::json!({
            "agent_id": STABLE,
            "run_id": RUN,
            "status": "succeeded",
            "available": available,
            "path": None::<String>,
            "size_bytes": None::<u64>,
            "sha256": None::<String>,
            "content": None::<String>,
            "inline_complete": false,
            "relative_path": None::<String>,
            "kind": None::<String>,
            "media_type": None::<String>,
            "proof_version": None::<u32>,
        }))
        .unwrap()
    }

    /// `&Arc<T>` derefs to the inner broker, so shared fakes plug in too.
    impl net::Broker for std::sync::Arc<ScriptedTranscripts> {
        fn call<'a>(&'a self, method: &'a str, params: serde_json::Value) -> net::BrokerFuture<'a> {
            (**self).call(method, params)
        }
    }

    /// The transcript retry ladder: 150 ms doubling to the 2 s cap.
    #[test]
    fn transcript_retry_delay_doubles_to_the_cap() {
        assert_eq!(next_retry_delay(Duration::ZERO), Duration::from_millis(150));
        assert_eq!(
            next_retry_delay(Duration::from_millis(150)),
            Duration::from_millis(300)
        );
        assert_eq!(
            next_retry_delay(Duration::from_millis(300)),
            Duration::from_millis(600)
        );
        assert_eq!(
            next_retry_delay(Duration::from_millis(1200)),
            Duration::from_millis(2000)
        );
        assert_eq!(
            next_retry_delay(Duration::from_millis(2000)),
            Duration::from_millis(2000),
            "the cap holds"
        );
    }

    /// A broker serving a scripted sequence of transcript responses and
    /// recording every request's cursor, limit, and arrival time.
    struct ScriptedTranscripts {
        script: std::sync::Mutex<std::vec::IntoIter<Result<serde_json::Value, String>>>,
        requests: std::sync::Mutex<Vec<(i64, usize, std::time::Instant)>>,
    }

    impl ScriptedTranscripts {
        /// Serves the given responses in order; anything beyond serves an
        /// empty complete page (the idle tail).
        fn new(script: Vec<Result<serde_json::Value, String>>) -> std::sync::Arc<Self> {
            std::sync::Arc::new(Self {
                script: std::sync::Mutex::new(script.into_iter()),
                requests: std::sync::Mutex::new(Vec::new()),
            })
        }

        /// The (cursor, limit) pairs requested so far, in order.
        fn requests(&self) -> Vec<(i64, usize)> {
            self.requests
                .lock()
                .expect("requests")
                .iter()
                .map(|(cursor, limit, _)| (*cursor, *limit))
                .collect()
        }
    }

    impl net::Broker for ScriptedTranscripts {
        fn call<'a>(
            &'a self,
            _method: &'a str,
            params: serde_json::Value,
        ) -> net::BrokerFuture<'a> {
            Box::pin(async move {
                self.requests.lock().expect("requests").push((
                    params["cursor"].as_i64().unwrap_or(-1),
                    params["limit"].as_u64().unwrap_or(0) as usize,
                    std::time::Instant::now(),
                ));
                match self.script.lock().expect("script").next() {
                    Some(Ok(value)) => Ok(value),
                    Some(Err(message)) => Err(agent_run::Error::Runtime(message)),
                    None => Ok(serde_json::json!({
                        "agent_id": params["agent_id"],
                        "run_id": null,
                        "messages": [],
                        "cursor": params["cursor"],
                        "limit": params["limit"],
                        "next_cursor": null,
                        "complete": true,
                    })),
                }
            })
        }
    }

    /// One transcript page serving `seqs` under one agent id.
    fn script_page(agent: &AgentId, seqs: &[i64], complete: bool) -> serde_json::Value {
        serde_json::json!({
            "agent_id": agent.as_str(),
            "run_id": null,
            "messages": seqs.iter().map(|seq| serde_json::json!({
                "seq": seq, "at": 100.0 + *seq as f64, "role": "assistant",
                "name": null, "content": format!("m{seq}"), "raw_ref": null,
            })).collect::<Vec<_>>(),
            "cursor": 0,
            "limit": net::TRANSCRIPT_PAGE_LIMIT,
            "next_cursor": if complete {
                serde_json::Value::Null
            } else {
                serde_json::json!(seqs.last())
            },
            "complete": complete,
        })
    }

    #[tokio::test]
    async fn transcript_worker_backfills_back_to_back_and_retries_with_backoff() {
        let agent: AgentId = "ag-20260928-101500-aaaaaaaaaa".parse().unwrap();
        let broker = ScriptedTranscripts::new(vec![
            Err("state database operation failed".into()),
            Err("state database operation failed".into()),
            Ok(script_page(&agent, &[1, 2], false)),
            Ok(script_page(&agent, &[3], true)),
        ]);
        let (tx, mut rx) = tokio::sync::mpsc::channel::<BrokerEvent>(2);
        let (cmd_tx, cmd_rx) = tokio::sync::watch::channel(WatchCommand::Clear);
        tokio::spawn(transcript_worker(
            std::sync::Arc::new(broker.clone()) as SharedBroker,
            cmd_rx,
            tx,
        ));
        let start = std::time::Instant::now();
        cmd_tx
            .send(WatchCommand::Watch {
                agent: agent.clone(),
                run: None,
                cursor: 0,
            })
            .unwrap();

        // Two failures surface, then the backfill completes.
        let mut errors = 0;
        let mut pages = 0;
        let mut complete = false;
        while errors < 2 || pages < 2 || !complete {
            let event = tokio::time::timeout(Duration::from_secs(5), rx.recv())
                .await
                .expect("worker alive")
                .expect("channel open");
            match event {
                BrokerEvent::Transcript { page: Err(_), .. } => errors += 1,
                BrokerEvent::Transcript { page: Ok(page), .. } => {
                    pages += 1;
                    complete = page.complete;
                }
                _ => unreachable!("the scripted worker only emits transcript events"),
            }
        }
        let elapsed = start.elapsed();

        // Pages request the broker maximum and advance the cursor.
        assert_eq!(
            broker.requests(),
            vec![(0, 200), (0, 200), (0, 200), (2, 1000)],
            "failures retry the same cursor; success advances it"
        );
        // The two retries cost the 150 ms + 300 ms backoff, and the second
        // backfill page follows the first immediately (no tail-poll pause).
        let requests = broker.requests.lock().unwrap().clone();
        let backfill_gap = requests[3]
            .2
            .saturating_duration_since(requests[2].2)
            .as_millis();
        assert!(
            backfill_gap < 200,
            "backfill pages fetch back to back ({backfill_gap} ms)"
        );
        assert!(
            elapsed >= Duration::from_millis(400),
            "the backoff delays the retries ({elapsed:?})"
        );
        assert!(
            elapsed < Duration::from_secs(4),
            "retries stay far below the old flat 2 s per failure ({elapsed:?})"
        );
    }

    #[tokio::test]
    async fn transcript_worker_resumes_from_the_requested_cursor() {
        let agent: AgentId = "ag-20260928-101501-cccccccccc".parse().unwrap();
        let broker = ScriptedTranscripts::new(vec![Ok(script_page(&agent, &[7, 8], true))]);
        let (tx, mut rx) = tokio::sync::mpsc::channel::<BrokerEvent>(2);
        let (cmd_tx, cmd_rx) = tokio::sync::watch::channel(WatchCommand::Clear);
        tokio::spawn(transcript_worker(
            std::sync::Arc::new(broker.clone()) as SharedBroker,
            cmd_rx,
            tx,
        ));
        cmd_tx
            .send(WatchCommand::Watch {
                agent: agent.clone(),
                run: None,
                cursor: 6,
            })
            .unwrap();
        let event = tokio::time::timeout(Duration::from_secs(5), rx.recv())
            .await
            .expect("worker alive")
            .expect("channel open");
        match event {
            BrokerEvent::Transcript { page: Ok(page), .. } => {
                assert!(page.complete);
                assert_eq!(page.messages.len(), 2);
            }
            other => panic!("expected a page, got {other:?}"),
        }
        assert_eq!(broker.requests(), vec![(6, 200)]);
    }

    #[test]
    fn quit_and_global_keys_map() {
        let app = App::new();
        assert_eq!(key_action(&app, key(KeyCode::Char('q'))), Action::Quit);
        assert_eq!(
            key_action(
                &app,
                KeyEvent::new(KeyCode::Char('c'), KeyModifiers::CONTROL)
            ),
            Action::Quit
        );
        assert_eq!(key_action(&app, key(KeyCode::Char('x'))), Action::None);
        // Release events never produce actions.
        let release = KeyEvent {
            code: KeyCode::Char('q'),
            modifiers: KeyModifiers::NONE,
            kind: KeyEventKind::Release,
            state: ratatui::crossterm::event::KeyEventState::NONE,
        };
        assert_eq!(key_action(&app, release), Action::None);
    }

    #[test]
    fn sessions_keys_map_to_actions() {
        let app = App::new();
        assert_eq!(key_action(&app, key(KeyCode::Char('j'))), Action::Move(1));
        assert_eq!(key_action(&app, key(KeyCode::Up)), Action::Move(-1));
        assert_eq!(key_action(&app, key(KeyCode::Enter)), Action::Open);
        assert_eq!(key_action(&app, key(KeyCode::Tab)), Action::ToggleCompleted);
        assert_eq!(key_action(&app, key(KeyCode::Char('r'))), Action::Refresh);
        assert_eq!(key_action(&app, key(KeyCode::Char('a'))), Action::Answer);
        // Transcript-only keys are inert on the sessions screen.
        assert_eq!(key_action(&app, key(KeyCode::Char('f'))), Action::None);
    }

    #[test]
    fn transcript_keys_map_to_actions() {
        let mut app = App::new();
        app.screen = crate::app::Screen::Transcript;
        assert_eq!(key_action(&app, key(KeyCode::Esc)), Action::Back);
        assert_eq!(key_action(&app, key(KeyCode::Down)), Action::CursorMove(1));
        assert_eq!(key_action(&app, key(KeyCode::Up)), Action::CursorMove(-1));
        assert_eq!(key_action(&app, key(KeyCode::Enter)), Action::ToggleExpand);
        assert_eq!(
            key_action(&app, key(KeyCode::PageUp)),
            Action::ScrollTranscript(-10)
        );
        assert_eq!(
            key_action(&app, key(KeyCode::Char('f'))),
            Action::ToggleFollow
        );
        assert_eq!(key_action(&app, key(KeyCode::Home)), Action::Top);
        assert_eq!(key_action(&app, key(KeyCode::Char('G'))), Action::Bottom);
        // Enter is the tool-payload toggle on the transcript screen.
        assert_eq!(key_action(&app, key(KeyCode::Enter)), Action::ToggleExpand);
    }

    #[test]
    fn answer_popup_captures_keys() {
        let mut app = App::new();
        app.answer = Some(answer_view(false));
        assert_eq!(key_action(&app, key(KeyCode::Esc)), Action::Back);
        assert_eq!(key_action(&app, key(KeyCode::Char('j'))), Action::None);
        app.close_answer();
        assert_eq!(key_action(&app, key(KeyCode::Char('j'))), Action::Move(1));
    }

    #[test]
    fn apply_action_quit_sets_flag() {
        let mut app = App::new();
        assert!(matches!(
            apply_action(&mut app, Action::Quit),
            Dispatched::None
        ));
        assert!(app.quit);
    }

    #[test]
    fn apply_action_open_watches_selected() {
        let mut app = App::new();
        app.sessions = vec![crate::tests_support::agent_view(STABLE, RUN, "running")];
        match apply_action(&mut app, Action::Open) {
            Dispatched::Watch(agent, run, _) => {
                assert_eq!(agent.as_str(), STABLE);
                assert_eq!(run.map(|r| r.as_str().to_string()), Some(RUN.to_string()));
            }
            other => panic!("expected watch, got {other:?}"),
        }
        assert_eq!(app.screen, crate::app::Screen::Transcript);
        app.close_transcript();
        // An empty table cannot open anything.
        app.sessions.clear();
        assert!(matches!(
            apply_action(&mut app, Action::Open),
            Dispatched::None
        ));
        let _ = AgentId::from_str(STABLE).unwrap();
    }

    #[test]
    fn back_from_transcript_clears_watch() {
        let mut app = App::new();
        app.sessions = vec![crate::tests_support::agent_view(STABLE, RUN, "running")];
        assert!(matches!(
            apply_action(&mut app, Action::Open),
            Dispatched::Watch(..)
        ));
        assert!(matches!(
            apply_action(&mut app, Action::Back),
            Dispatched::Clear
        ));
        assert_eq!(app.screen, crate::app::Screen::Sessions);
        // Back on the sessions screen without a popup does nothing.
        assert!(matches!(
            apply_action(&mut app, Action::Back),
            Dispatched::None
        ));
    }

    #[test]
    fn answer_request_dispatches_fetch() {
        let mut app = App::new();
        app.sessions = vec![crate::tests_support::agent_view(STABLE, RUN, "running")];
        match apply_action(&mut app, Action::Answer) {
            Dispatched::Answer(agent, run) => {
                assert_eq!(agent.as_str(), STABLE);
                assert_eq!(run.map(|r| r.as_str().to_string()), Some(RUN.to_string()));
            }
            other => panic!("expected answer fetch, got {other:?}"),
        }
    }

    #[test]
    fn mouse_click_selects_then_reclick_opens() {
        let mut app = App::new();
        app.last_width = 100;
        app.last_height = 30;
        app.sessions = vec![
            crate::tests_support::agent_view(STABLE, RUN, "running"),
            crate::tests_support::agent_view(
                "ag-20260928-101501-cccccccccc",
                "ag-20260928-101501-dddddddddd",
                "running",
            ),
        ];
        // The list pane starts at row 2 (app bar + blank); every card spans
        // three rows (two content rows plus one gap), so the second card's
        // first row is row 7 and the finished header sits below two live
        // cards at row 10.
        let click_card = |column, row| MouseEvent {
            kind: MouseEventKind::Down(MouseButton::Left),
            column,
            row,
            modifiers: KeyModifiers::NONE,
        };
        assert!(matches!(
            apply_mouse(&mut app, click_card(5, 7)),
            Dispatched::None
        ));
        assert_eq!(app.selected, 1);
        // A second click on the same card opens the transcript.
        match apply_mouse(&mut app, click_card(5, 7)) {
            Dispatched::Watch(agent, ..) => {
                assert_eq!(agent.as_str(), "ag-20260928-101501-cccccccccc")
            }
            other => panic!("expected watch, got {other:?}"),
        }
        // Clicking the finished header toggles the finished section.
        app.close_transcript();
        let finished = crate::tests_support::agent_view(
            "ag-20260928-101502-eeeeeeeeee",
            "ag-20260928-101502-ffffffffff",
            "succeeded",
        );
        app.sessions.push(finished);
        let header_row = 10; // two live cards below the two pinned rows
        assert!(matches!(
            apply_mouse(&mut app, click_card(5, header_row)),
            Dispatched::Scope
        ));
        assert!(app.completed_open);
    }

    #[test]
    fn mouse_wheel_scrolls_transcript() {
        let mut app = App::new();
        app.sessions = vec![crate::tests_support::agent_view(STABLE, RUN, "running")];
        app.open_selected_transcript();
        let wheel = |kind| MouseEvent {
            kind,
            column: 5,
            row: 5,
            modifiers: KeyModifiers::NONE,
        };
        apply_mouse(&mut app, wheel(MouseEventKind::ScrollUp));
        assert!(!app.transcript.as_ref().unwrap().follow);
        apply_mouse(&mut app, wheel(MouseEventKind::ScrollDown));
        assert!(app.transcript.as_ref().unwrap().follow);
    }

    #[test]
    fn mouse_move_sets_and_clears_hover() {
        let mut app = App::new();
        app.last_width = 100;
        app.last_height = 30;
        app.sessions = vec![crate::tests_support::agent_view(STABLE, RUN, "running")];
        app.open_selected_transcript();
        let page: agent_run_domain::views::TranscriptPage =
            serde_json::from_value(serde_json::json!({
                "agent_id": STABLE, "run_id": RUN,
                "messages": [
                    {"seq": 1, "at": 1.0, "role": "assistant", "name": null,
                     "content": "first", "raw_ref": null},
                    {"seq": 2, "at": 2.0, "role": "tool_call", "name": "Bash",
                     "content": "{\"command\":\"ls\"}", "raw_ref": null},
                ],
                "cursor": 0, "limit": 500, "next_cursor": null, "complete": true,
            }))
            .unwrap();
        app.apply_transcript(&AgentId::from_str(STABLE).unwrap(), page.clone());

        let move_to = |column, row| MouseEvent {
            kind: MouseEventKind::Moved,
            column,
            row,
            modifiers: KeyModifiers::NONE,
        };
        // The body starts four header rows below the pane top (row 2): the
        // tool block sits two rows below the first message (one separator).
        apply_mouse(&mut app, move_to(5, 8));
        assert_eq!(app.transcript.as_ref().unwrap().hover, Some(1));
        // Leaving the body area clears the highlight.
        apply_mouse(&mut app, move_to(5, 0));
        assert_eq!(app.transcript.as_ref().unwrap().hover, None);
    }

    #[test]
    fn scroll_preserves_direction() {
        let mut app = App::new();
        app.sessions = vec![crate::tests_support::agent_view(STABLE, RUN, "running")];
        app.open_selected_transcript();
        scroll_transcript(&mut app, -3);
        let buffer = app.transcript.as_ref().unwrap();
        assert!(!buffer.follow);
        assert_eq!(buffer.from_bottom, 3);
        scroll_transcript(&mut app, 10);
        let buffer = app.transcript.as_ref().unwrap();
        assert!(buffer.follow);
        assert_eq!(buffer.from_bottom, 0);
    }

    #[test]
    fn mouse_moves_without_a_hover_change_skip_the_redraw() {
        let mut app = App::new();
        app.last_width = 100;
        app.last_height = 30;
        app.sessions = vec![crate::tests_support::agent_view(STABLE, RUN, "running")];
        app.open_selected_transcript();
        let page: agent_run_domain::views::TranscriptPage =
            serde_json::from_value(serde_json::json!({
                "agent_id": STABLE, "run_id": RUN,
                "messages": [
                    {"seq": 1, "at": 1.0, "role": "assistant", "name": null,
                     "content": "first", "raw_ref": null},
                    {"seq": 2, "at": 2.0, "role": "tool_call", "name": "Bash",
                     "content": "{\"command\":\"ls\"}", "raw_ref": null},
                ],
                "cursor": 0, "limit": 500, "next_cursor": null, "complete": true,
            }))
            .unwrap();
        app.apply_transcript(&AgentId::from_str(STABLE).unwrap(), page.clone());

        // The first move onto the tool block changes the hover target.
        assert!(apply_hover(&mut app, 5, 8));
        assert_eq!(app.transcript.as_ref().unwrap().hover, Some(1));
        // Repeating the same position (or moving within the same row) is a
        // no-op: neither hover target changed, so the frame is identical.
        assert!(!apply_hover(&mut app, 5, 8));
        assert!(!apply_hover(&mut app, 9, 8));
        // Moving away clears hover again — a redraw.
        assert!(apply_hover(&mut app, 5, 0));
        assert_eq!(app.transcript.as_ref().unwrap().hover, None);
    }

    /// A running session list with one live transcript open in the split
    /// view (160×48), `messages` loaded.
    fn split_app(messages: serde_json::Value) -> App {
        let mut app = App::new();
        app.last_width = 160;
        app.last_height = 48;
        app.apply_sessions(&sessions_page(
            vec![crate::tests_support::agent_view(STABLE, RUN, "running")],
            1,
        ));
        assert!(app.watch_selected().is_some());
        let page: TranscriptPage = serde_json::from_value(serde_json::json!({
            "agent_id": STABLE, "run_id": RUN, "messages": messages,
            "cursor": 0, "limit": 500, "next_cursor": null, "complete": true,
        }))
        .unwrap();
        app.apply_transcript(&AgentId::from_str(STABLE).unwrap(), page.clone());
        app
    }

    /// One sessions page holding `items` at `revision`.
    fn sessions_page(items: Vec<agent_run_domain::AgentView>, revision: i64) -> AgentPage {
        AgentPage {
            total: items.len(),
            items,
            offset: 0,
            limit: 200,
            next_offset: None,
            complete: true,
            revision,
            message_revision: 0,
            observed_at: 1.0,
        }
    }

    /// One single-message transcript page appending `seq` to the live session.
    fn delta(seq: i64) -> BrokerEvent {
        BrokerEvent::Transcript {
            agent: AgentId::from_str(STABLE).unwrap(),
            page: Ok(TranscriptPage {
                agent_id: AgentId::from_str(STABLE).unwrap(),
                run_id: Some(AgentId::from_str(RUN).unwrap()),
                messages: vec![crate::tests_support::message(
                    seq,
                    "assistant",
                    &format!("delta-{seq} "),
                )],
                cursor: seq - 1,
                limit: 500,
                next_cursor: Some(seq),
                complete: true,
                view: None,
                direction: None,
                before_cursor: None,
                previous_cursor: None,
                resume_cursor: None,
            }),
        }
    }

    /// One mouse event of `kind` at a terminal position.
    fn mouse(kind: MouseEventKind, column: u16, row: u16) -> UiEvent {
        UiEvent::Terminal(TerminalEvent::Mouse(MouseEvent {
            kind,
            column,
            row,
            modifiers: KeyModifiers::NONE,
        }))
    }

    /// One key press event.
    fn press(code: KeyCode) -> UiEvent {
        UiEvent::Terminal(TerminalEvent::Key(key(code)))
    }

    /// Runs the event loop's frame step at virtual instant `now`: when a
    /// frame is due, prepares and draws it at its due instant (where the
    /// loop's timer would have fired). Returns whether it drew.
    fn frame_step<B: Backend>(
        pipeline: &mut Pipeline,
        app: &mut App,
        terminal: &mut Terminal<B>,
        now: Instant,
    ) -> bool {
        match pipeline.next_frame(app, now) {
            Some(due) if due <= now => {
                let _ = pipeline.prepare_frame(app);
                pipeline.draw(app, terminal, due).unwrap()
            }
            _ => false,
        }
    }

    #[test]
    fn frame_pacer_leads_after_idle_and_trails_within_the_interval() {
        let t0 = Instant::now();
        let mut pacer = FramePacer::new(FRAME_INTERVAL);
        assert_eq!(pacer.due(t0), t0, "the first frame draws at once");
        pacer.record(t0);
        let inside = t0 + Duration::from_millis(10);
        assert_eq!(pacer.due(inside), t0 + FRAME_INTERVAL, "trailing edge");
        let idle = t0 + Duration::from_millis(500);
        assert_eq!(pacer.due(idle), idle, "leading edge after idle");
    }

    #[test]
    fn events_inside_one_interval_cost_one_draw() {
        crate::tests_support::force_truecolor();
        let mut app = split_app(serde_json::json!([]));
        let mut terminal = Terminal::new(ratatui::backend::TestBackend::new(160, 48)).unwrap();
        let mut pipeline = Pipeline::new(FRAME_INTERVAL);
        let t0 = Instant::now();
        assert!(frame_step(&mut pipeline, &mut app, &mut terminal, t0));

        // 100 appended deltas and pointer moves within one interval draw
        // nothing until the interval ends.
        let mut draws = 0;
        for step in 0..100u32 {
            let now = t0 + Duration::from_micros(300 * u64::from(step + 1));
            pipeline.broker_event(&mut app, delta(i64::from(step) + 1));
            let _ = pipeline.ui_event(&mut app, mouse(MouseEventKind::Moved, 80, 6));
            draws += u32::from(frame_step(&mut pipeline, &mut app, &mut terminal, now));
        }
        assert_eq!(draws, 0, "no draw inside the interval");
        // The trailing edge draws once, showing the state at the frame.
        assert!(frame_step(
            &mut pipeline,
            &mut app,
            &mut terminal,
            t0 + FRAME_INTERVAL
        ));
        assert!(!frame_step(
            &mut pipeline,
            &mut app,
            &mut terminal,
            t0 + FRAME_INTERVAL * 2
        ));
        let screen: String = terminal
            .backend()
            .buffer()
            .content()
            .iter()
            .map(|cell| cell.symbol())
            .collect();
        assert!(screen.contains("delta-100"), "the last delta is on screen");
    }

    #[test]
    fn a_change_after_idle_draws_on_the_leading_edge() {
        let mut app = split_app(serde_json::json!([]));
        let mut terminal = Terminal::new(ratatui::backend::TestBackend::new(160, 48)).unwrap();
        let mut pipeline = Pipeline::new(FRAME_INTERVAL);
        let t0 = Instant::now();
        assert!(frame_step(&mut pipeline, &mut app, &mut terminal, t0));
        let later = t0 + Duration::from_millis(400);
        pipeline.broker_event(&mut app, delta(1));
        assert_eq!(pipeline.next_frame(&app, later), Some(later));
        assert!(frame_step(&mut pipeline, &mut app, &mut terminal, later));
    }

    #[test]
    fn a_clean_quiet_screen_never_draws() {
        let mut app = App::new();
        app.last_width = 160;
        app.last_height = 48;
        app.apply_sessions(&sessions_page(
            vec![crate::tests_support::agent_view(STABLE, RUN, "succeeded")],
            1,
        ));
        let mut terminal = Terminal::new(ratatui::backend::TestBackend::new(160, 48)).unwrap();
        let mut pipeline = Pipeline::new(FRAME_INTERVAL);
        let t0 = Instant::now();
        // The first frames settle the split pane onto the selection.
        let mut now = t0;
        while frame_step(&mut pipeline, &mut app, &mut terminal, now) {
            now += FRAME_INTERVAL;
        }
        assert_eq!(pipeline.next_frame(&app, now), None, "nothing pending");
        // Ticks over finished sessions animate nothing.
        app.tick();
        assert_eq!(
            pipeline.next_frame(&app, now),
            None,
            "a quiet tick is clean"
        );
        // An unchanged sessions revision and an empty tail poll change nothing.
        let same = sessions_page(app.sessions.clone(), 1);
        pipeline.broker_event(&mut app, BrokerEvent::Sessions(Ok(same)));
        assert_eq!(pipeline.next_frame(&app, now), None);
        // A pointer move that changes no hover target resolves clean.
        let _ = pipeline.ui_event(&mut app, mouse(MouseEventKind::Moved, 150, 0));
        now += Duration::from_secs(1);
        assert!(!frame_step(&mut pipeline, &mut app, &mut terminal, now));
        assert_eq!(pipeline.next_frame(&app, now), None);
    }

    #[test]
    fn an_unchanged_redraw_writes_almost_nothing() {
        crate::tests_support::force_truecolor();
        let mut app = split_app(serde_json::json!([
            {"seq": 1, "at": 1.0, "role": "assistant", "name": null,
             "content": "hello", "raw_ref": null},
        ]));
        let (mut terminal, bytes) = crate::tests_support::counting_terminal(160, 48);
        let mut pipeline = Pipeline::new(FRAME_INTERVAL);
        let t0 = Instant::now();
        assert!(frame_step(&mut pipeline, &mut app, &mut terminal, t0));
        let first = bytes.replace(0);
        assert!(first > 1000, "the first frame paints the screen: {first}");
        // Force a redraw of the identical state: ratatui diffs cells, so no
        // cell goes out — only the crossterm backend's fixed epilogue (four
        // style resets) and the cursor-hide sequence, 25 bytes in all.
        app.dirty = true;
        assert!(frame_step(
            &mut pipeline,
            &mut app,
            &mut terminal,
            t0 + FRAME_INTERVAL
        ));
        let idle = bytes.get();
        assert!(idle <= 32, "an unchanged frame writes {idle} bytes");
    }

    #[test]
    fn wheel_bursts_apply_once_as_one_net_scroll() {
        let mut app = split_app(serde_json::json!([]));
        let mut pipeline = Pipeline::new(FRAME_INTERVAL);
        // Twenty wheel-ups over the transcript pane accumulate unapplied.
        for _ in 0..20 {
            let _ = pipeline.ui_event(&mut app, mouse(MouseEventKind::ScrollUp, 80, 20));
        }
        assert_eq!(pipeline.input.nav, Some((Nav::Scroll, -20)));
        let buffer = app.transcript.as_ref().unwrap();
        assert!(buffer.follow && buffer.from_bottom == 0, "not applied yet");
        let _ = pipeline.prepare_frame(&mut app);
        let buffer = app.transcript.as_ref().unwrap();
        assert!(!buffer.follow);
        assert_eq!(buffer.from_bottom, 20, "one net scroll of twenty rows");

        // A direction change applies the pending run first: 3 up, 5 down.
        for _ in 0..3 {
            let _ = pipeline.ui_event(&mut app, mouse(MouseEventKind::ScrollUp, 80, 20));
        }
        for _ in 0..5 {
            let _ = pipeline.ui_event(&mut app, mouse(MouseEventKind::ScrollDown, 80, 20));
        }
        assert_eq!(app.transcript.as_ref().unwrap().from_bottom, 23);
        assert_eq!(pipeline.input.nav, Some((Nav::Scroll, 5)));
        let _ = pipeline.prepare_frame(&mut app);
        assert_eq!(app.transcript.as_ref().unwrap().from_bottom, 18);
        assert!(pipeline.input.nav.is_none());
    }

    #[test]
    fn selection_bursts_coalesce_and_flush_before_dependent_keys() {
        let mut app = App::new();
        app.last_width = 100;
        app.last_height = 30;
        let ids = [
            (
                "ag-20260928-101500-aaaaaaaaaa",
                "ag-20260928-101500-bbbbbbbbbb",
            ),
            (
                "ag-20260928-101501-cccccccccc",
                "ag-20260928-101501-dddddddddd",
            ),
            (
                "ag-20260928-101502-eeeeeeeeee",
                "ag-20260928-101502-ffffffffff",
            ),
        ];
        app.sessions = ids
            .iter()
            .map(|(stable, run)| crate::tests_support::agent_view(stable, run, "running"))
            .collect();
        let mut pipeline = Pipeline::new(FRAME_INTERVAL);
        // Thirty Down presses: one pending run, clamped once when applied.
        for _ in 0..30 {
            let _ = pipeline.ui_event(&mut app, press(KeyCode::Down));
        }
        assert_eq!(pipeline.input.nav, Some((Nav::Selection, 30)));
        assert_eq!(app.selected, 0, "nothing applied before the frame");
        let _ = pipeline.prepare_frame(&mut app);
        assert_eq!(app.selected, 2, "clamped to the last card");

        // Up once, then Enter: the pending move applies before the open.
        let _ = pipeline.ui_event(&mut app, press(KeyCode::Up));
        match pipeline.ui_event(&mut app, press(KeyCode::Enter)) {
            Dispatched::Watch(agent, ..) => assert_eq!(agent.as_str(), ids[1].0),
            other => panic!("expected watch, got {other:?}"),
        }
    }

    /// Totals of one event-storm run of the end-to-end probe.
    struct StormReport {
        /// Events applied (broker pages, input, ticks).
        events: usize,
        /// Frames drawn.
        draws: u32,
        /// Wall time inside the event reducers (single-threaded, ≈ CPU).
        reducers: Duration,
        /// Longest single reducer call.
        max_reducer: Duration,
        /// Wall time inside frame preparation plus drawing.
        frames: Duration,
        /// Longest single frame (preparation plus draw).
        max_frame: Duration,
        /// Bytes the frames wrote to the (counting) terminal.
        bytes: usize,
        /// Largest single-frame write.
        max_bytes: usize,
    }

    /// Feeds 5 s of a synthetic 2000-events-per-second storm through the
    /// production [`Pipeline`] over a live 4000-message (~8 MB) transcript
    /// in the 160×48 split view, on a virtual clock (0.5 ms per event), and
    /// measures the real cost of every reducer call and frame.
    ///
    /// The mix per 20 events: 1 AgentView refresh (new sessions revision),
    /// 5 appended deltas, 4 unchanged tail polls, 6 pointer moves, 4 wheel
    /// steps (direction flips every 200 ms); plus one tick per second.
    fn event_storm(interval: Duration) -> StormReport {
        crate::tests_support::force_truecolor();
        let (messages, last_seq) = crate::tests_support::probe_messages();
        let agent_id = AgentId::from_str(STABLE).unwrap();
        let mut items = vec![crate::tests_support::agent_view(STABLE, RUN, "running")];
        items[0].created_at = 100.0;
        for index in 1..20u64 {
            let stable = format!("ag-20260928-1016{:02}-{:010x}", index, index);
            let run = format!("ag-20260928-1017{:02}-{:010x}", index, index);
            let status = if index % 3 == 0 {
                "running"
            } else {
                "succeeded"
            };
            items.push(crate::tests_support::agent_view(&stable, &run, status));
        }
        let mut app = App::new();
        app.last_width = 160;
        app.last_height = 48;
        app.apply_sessions(&sessions_page(items.clone(), 1));
        let (mut terminal, bytes) = crate::tests_support::counting_terminal(160, 48);
        let mut pipeline = Pipeline::new(interval);
        let _ = pipeline.prepare_frame(&mut app); // attaches the split pane
        for chunk in messages.chunks(500) {
            let page: TranscriptPage = serde_json::from_value(serde_json::json!({
                "agent_id": STABLE, "run_id": RUN, "messages": chunk,
                "cursor": 0, "limit": 500, "next_cursor": 500, "complete": false,
            }))
            .unwrap();
            app.apply_transcript(&agent_id, page.clone());
        }
        let t0 = Instant::now();
        assert!(frame_step(&mut pipeline, &mut app, &mut terminal, t0));
        bytes.set(0);

        let mut report = StormReport {
            events: 0,
            draws: 0,
            reducers: Duration::ZERO,
            max_reducer: Duration::ZERO,
            frames: Duration::ZERO,
            max_frame: Duration::ZERO,
            bytes: 0,
            max_bytes: 0,
        };
        let frame = |pipeline: &mut Pipeline,
                     app: &mut App,
                     terminal: &mut Terminal<_>,
                     now: Instant,
                     report: &mut StormReport| {
            let start = Instant::now();
            let drew = frame_step(pipeline, app, terminal, now);
            let spent = start.elapsed();
            report.frames += spent;
            if drew {
                let written = bytes.replace(0);
                report.draws += 1;
                report.max_frame = report.max_frame.max(spent);
                report.bytes += written;
                report.max_bytes = report.max_bytes.max(written);
            }
        };
        let mut seq = last_seq;
        let mut revision = 1;
        const EVENTS: u64 = 10_000;
        for step in 0..EVENTS {
            let now = t0 + Duration::from_micros(500 * (step + 1));
            // A frame whose timer fired before this event draws first.
            frame(&mut pipeline, &mut app, &mut terminal, now, &mut report);
            // Build the event outside the timed region: in production the
            // workers parse pages off the UI thread.
            enum Next {
                Broker(BrokerEvent),
                Ui(UiEvent),
                Tick,
            }
            let next = if step % 2000 == 1999 {
                Next::Tick
            } else {
                match step % 20 {
                    0 => {
                        revision += 1;
                        items[0].silence_seconds = Some(step as f64 / 2000.0);
                        items[0].elapsed_seconds = 42.0 + step as f64 / 2000.0;
                        Next::Broker(BrokerEvent::Sessions(Ok(sessions_page(
                            items.clone(),
                            revision,
                        ))))
                    }
                    1..=5 => {
                        seq += 1;
                        Next::Broker(delta(seq))
                    }
                    6..=9 => Next::Broker(BrokerEvent::Transcript {
                        agent: agent_id.clone(),
                        page: Ok(TranscriptPage {
                            agent_id: agent_id.clone(),
                            run_id: Some(AgentId::from_str(RUN).unwrap()),
                            messages: Vec::new(),
                            cursor: seq,
                            limit: 500,
                            next_cursor: None,
                            complete: true,
                            view: None,
                            direction: None,
                            before_cursor: None,
                            previous_cursor: None,
                            resume_cursor: None,
                        }),
                    }),
                    10..=15 => Next::Ui(mouse(
                        MouseEventKind::Moved,
                        80,
                        6 + ((step / 20) % 40) as u16,
                    )),
                    _ => Next::Ui(mouse(
                        if (step / 400) % 2 == 0 {
                            MouseEventKind::ScrollUp
                        } else {
                            MouseEventKind::ScrollDown
                        },
                        80,
                        20,
                    )),
                }
            };
            let start = Instant::now();
            match next {
                Next::Broker(event) => pipeline.broker_event(&mut app, event),
                Next::Ui(event) => {
                    let _ = pipeline.ui_event(&mut app, event);
                }
                Next::Tick => app.tick(),
            }
            let spent = start.elapsed();
            report.events += 1;
            report.reducers += spent;
            report.max_reducer = report.max_reducer.max(spent);
        }
        let end = t0 + Duration::from_micros(500 * EVENTS) + interval;
        frame(&mut pipeline, &mut app, &mut terminal, end, &mut report);
        report
    }

    /// End-to-end probe of the frame-capped pipeline under an event storm;
    /// run with `cargo test --release --locked -p agent-run-tui -- --ignored
    /// --nocapture`. The same storm with the cap disabled (draw whenever
    /// dirty) is printed for comparison.
    #[test]
    #[ignore = "timing probe"]
    fn end_to_end_event_storm_probe() {
        for (label, interval) in [
            ("capped at FRAME_INTERVAL", FRAME_INTERVAL),
            ("cap disabled", Duration::ZERO),
        ] {
            let report = event_storm(interval);
            println!(
                "event storm ({label}): 5 s x 2000 ev/s over 4000 msgs (~8 MB), 160x48\n  events {}  draws {}\n  reducers {:?} total, max {:?}\n  frames {:?} total, max {:?}\n  bytes written {} total, {} per draw avg, {} max",
                report.events,
                report.draws,
                report.reducers,
                report.max_reducer,
                report.frames,
                report.max_frame,
                report.bytes,
                report.bytes / report.draws.max(1) as usize,
                report.max_bytes,
            );
            if interval == FRAME_INTERVAL {
                let cap = (5000 / FRAME_INTERVAL.as_millis()) as u32 + 2;
                assert!(
                    report.draws <= cap,
                    "at most one draw per interval: {} > {cap}",
                    report.draws
                );
            }
        }
    }
    /// A deterministic broker whose listing and one transcript never finish.
    struct GatedBroker {
        /// Reports entered requests so tests wait for actual in-flight work.
        entered: mpsc::Sender<String>,
    }

    impl Broker for GatedBroker {
        /// Blocks listings/the first agent; other transcripts complete immediately.
        fn call<'a>(&'a self, method: &'a str, params: serde_json::Value) -> net::BrokerFuture<'a> {
            Box::pin(async move {
                self.entered.send(method.to_string()).await.unwrap();
                if method == "list_agents" || params["agent_id"] == STABLE {
                    std::future::pending::<()>().await;
                }
                let agent: AgentId = params["agent_id"].as_str().unwrap().parse().unwrap();
                Ok(script_page(&agent, &[1], true))
            })
        }
    }

    /// Listing long polls run concurrently, and Watch/Clear abandons stale fetches.
    #[tokio::test]
    async fn transcript_switch_cancels_fetch_while_listing_is_blocked() {
        let (entered, mut requests) = mpsc::channel(8);
        let broker: SharedBroker = Arc::new(GatedBroker { entered });
        let (tx, mut rx) = mpsc::channel(2);
        let (_scope_tx, scope_rx) = watch::channel(Scope {
            show_all: false,
            token: 0,
        });
        let listing = tokio::spawn(sessions_worker(broker.clone(), scope_rx, tx.clone()));
        assert_eq!(requests.recv().await.unwrap(), "list_agents");
        let (cmd_tx, cmd_rx) = watch::channel(WatchCommand::Watch {
            agent: STABLE.parse().unwrap(),
            run: None,
            cursor: 0,
        });
        let transcript = tokio::spawn(transcript_worker(broker, cmd_rx, tx));
        assert_eq!(requests.recv().await.unwrap(), "transcript");
        // Only the last of this burst is relevant; Clear must not stop the worker.
        cmd_tx.send(WatchCommand::Clear).unwrap();
        let latest: AgentId = RUN.parse().unwrap();
        cmd_tx
            .send(WatchCommand::Watch {
                agent: latest.clone(),
                run: None,
                cursor: 0,
            })
            .unwrap();
        let event = tokio::time::timeout(Duration::from_millis(500), rx.recv())
            .await
            .unwrap()
            .unwrap();
        assert!(matches!(event, BrokerEvent::Transcript { agent, page: Ok(_) } if agent == latest));
        cmd_tx.send(WatchCommand::Clear).unwrap();
        assert!(!transcript.is_finished());
        transcript.abort();
        listing.abort();
        let _ = transcript.await;
        let _ = listing.await;
    }

    /// Two queued pages apply backpressure, and a new target interrupts delivery.
    #[tokio::test]
    async fn backfill_waits_for_the_ui_with_two_queued_pages() {
        let agent: AgentId = STABLE.parse().unwrap();
        let broker = ScriptedTranscripts::new(
            (0..100)
                .map(|_| Ok(script_page(&agent, &[1], false)))
                .collect(),
        );
        let (tx, mut rx) = mpsc::channel(2);
        let (cmd_tx, cmd_rx) = watch::channel(WatchCommand::Watch {
            agent,
            run: None,
            cursor: 0,
        });
        let worker = tokio::spawn(transcript_worker(Arc::new(broker.clone()), cmd_rx, tx));
        tokio::time::timeout(Duration::from_secs(1), async {
            while broker.requests().len() < 3 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        tokio::time::sleep(Duration::from_millis(20)).await;
        assert_eq!(
            broker.requests().len(),
            3,
            "two queued plus one fetch awaiting delivery"
        );
        cmd_tx.send(WatchCommand::Clear).unwrap();
        tokio::time::sleep(Duration::from_millis(20)).await;
        assert!(rx.recv().await.is_some());
        assert!(rx.recv().await.is_some());
        assert!(rx.try_recv().is_err(), "abandoned third page never queues");
        assert!(
            !worker.is_finished(),
            "Clear idles, keeping the watcher reusable"
        );
        worker.abort();
        let _ = worker.await;
    }

    /// Broker serving the active listing (two live rows) and, for the cheap
    /// unfiltered count call, a total of 105 sessions; every `list_agents`
    /// request is recorded.
    struct CountedListings {
        /// Recorded request params, in order.
        requests: std::sync::Mutex<Vec<serde_json::Value>>,
        /// When set, the unfiltered count call fails.
        fail_counts: std::sync::atomic::AtomicBool,
    }

    impl CountedListings {
        fn new() -> std::sync::Arc<Self> {
            std::sync::Arc::new(Self {
                requests: std::sync::Mutex::new(Vec::new()),
                fail_counts: std::sync::atomic::AtomicBool::new(false),
            })
        }

        fn requests(&self) -> Vec<serde_json::Value> {
            self.requests.lock().expect("requests").clone()
        }
    }

    impl net::Broker for CountedListings {
        fn call<'a>(&'a self, method: &'a str, params: serde_json::Value) -> net::BrokerFuture<'a> {
            self.requests.lock().expect("requests").push(params.clone());
            Box::pin(async move {
                if method != "list_agents" {
                    return Err(agent_run::Error::Runtime("unexpected method".into()));
                }
                if params.get("active").is_none() {
                    // The cheap unfiltered count page.
                    if self.fail_counts.load(std::sync::atomic::Ordering::Relaxed) {
                        return Err(agent_run::Error::Runtime("count failed".into()));
                    }
                    return Ok(serde_json::json!({
                        "items": [], "total": 105, "offset": 0, "limit": 1,
                        "next_offset": Some(1), "complete": false,
                        "revision": 7, "observed_at": 1.0,
                    }));
                }
                // The active listing: two live rows at the same revision.
                let page = AgentPage {
                    items: vec![
                        crate::tests_support::agent_view(STABLE, RUN, "running"),
                        crate::tests_support::agent_view(
                            "ag-20260928-101501-cccccccccc",
                            "ag-20260928-101501-dddddddddd",
                            "running",
                        ),
                    ],
                    total: 2,
                    offset: 0,
                    limit: 200,
                    next_offset: None,
                    complete: true,
                    revision: 7,
                    message_revision: 0,
                    observed_at: 1.0,
                };
                Ok(serde_json::to_value(page).expect("page serializes"))
            })
        }
    }

    /// The finished total arrives from one cheap unfiltered call per revision,
    /// without loading finished rows into the listing.
    #[tokio::test]
    async fn sessions_worker_counts_finished_without_loading_their_rows() {
        let broker = CountedListings::new();
        let (tx, mut rx) = mpsc::channel(4);
        let (_scope_tx, scope_rx) = watch::channel(Scope {
            show_all: false,
            token: 0,
        });
        tokio::spawn(sessions_worker(
            broker.clone() as SharedBroker,
            scope_rx,
            tx,
        ));

        match tokio::time::timeout(Duration::from_secs(5), rx.recv()).await {
            Ok(Some(BrokerEvent::Sessions(Ok(page)))) => {
                assert_eq!(
                    page.items.len(),
                    2,
                    "the active listing loads live rows only"
                );
            }
            other => panic!("expected a sessions page, got {other:?}"),
        }
        match tokio::time::timeout(Duration::from_secs(5), rx.recv()).await {
            Ok(Some(BrokerEvent::FinishedTotal(finished))) => {
                assert_eq!(finished, 103, "105 total sessions minus 2 live");
            }
            other => panic!("expected the finished total, got {other:?}"),
        }

        // The count call is the cheap unfiltered one, once per revision.
        let requests = broker.requests();
        let counts: Vec<&serde_json::Value> = requests
            .iter()
            .filter(|params| params.get("active").is_none())
            .collect();
        assert_eq!(counts.len(), 1, "one count per revision: {counts:?}");
        assert_eq!(counts[0]["limit"], 1);

        // The same revision never recounts: the next listing round delivers
        // only the page.
        let event = tokio::time::timeout(Duration::from_millis(700), rx.recv())
            .await
            .expect("worker alive")
            .expect("channel open");
        assert!(
            matches!(event, BrokerEvent::Sessions(Ok(_))),
            "no recount on an unchanged revision: {event:?}"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert!(rx.try_recv().is_err(), "nothing else followed");
    }

    /// A failed count keeps quiet — the last known total stands — and the
    /// next listing round retries it.
    #[tokio::test]
    async fn a_failed_count_keeps_the_last_known_finished_total() {
        let broker = CountedListings::new();
        broker
            .fail_counts
            .store(true, std::sync::atomic::Ordering::Relaxed);
        let (tx, mut rx) = mpsc::channel(4);
        let (_scope_tx, scope_rx) = watch::channel(Scope {
            show_all: false,
            token: 0,
        });
        tokio::spawn(sessions_worker(
            broker.clone() as SharedBroker,
            scope_rx,
            tx,
        ));

        match tokio::time::timeout(Duration::from_secs(5), rx.recv()).await {
            Ok(Some(BrokerEvent::Sessions(Ok(_)))) => {}
            other => panic!("expected a sessions page, got {other:?}"),
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert!(rx.try_recv().is_err(), "a failed count emits nothing");

        // Once the count succeeds again the next listing round reports it.
        broker
            .fail_counts
            .store(false, std::sync::atomic::Ordering::Relaxed);
        let mut total = None;
        for _ in 0..4 {
            let event = tokio::time::timeout(Duration::from_secs(5), rx.recv())
                .await
                .expect("worker alive")
                .expect("channel open");
            match event {
                BrokerEvent::FinishedTotal(finished) => {
                    total = Some(finished);
                    break;
                }
                BrokerEvent::Sessions(Ok(_)) => continue,
                other => panic!("unexpected event {other:?}"),
            }
        }
        assert_eq!(total, Some(103), "105 total sessions minus 2 live");
    }

    /// Collapsing the finished section when nothing visible remains must not
    /// keep the split pane showing the hidden session.
    #[test]
    fn collapsing_without_visible_sessions_closes_the_stale_transcript() {
        let mut app = App::new();
        app.last_width = 160;
        app.last_height = 48;
        app.completed_open = true;
        let finished = crate::tests_support::agent_view(
            "ag-20260928-101502-eeeeeeeeee",
            "ag-20260928-101502-ffffffffff",
            "succeeded",
        );
        app.apply_sessions(&sessions_page(vec![finished], 1));
        assert!(app.watch_selected().is_some(), "pane on the finished card");

        app.toggle_completed();
        let mut pipeline = Pipeline::new(FRAME_INTERVAL);
        match pipeline.prepare_frame(&mut app) {
            Dispatched::Clear => {}
            other => panic!("expected clear, got {other:?}"),
        }
        assert!(app.transcript.is_none(), "the hidden session is gone");
    }

    /// Collapsing with live cards left moves the split pane to the first
    /// visible session instead of the hidden finished one.
    #[test]
    fn collapsing_moves_the_split_pane_to_the_first_visible_session() {
        let mut app = App::new();
        app.last_width = 160;
        app.last_height = 48;
        app.completed_open = true;
        let live = crate::tests_support::agent_view(STABLE, RUN, "running");
        let finished = crate::tests_support::agent_view(
            "ag-20260928-101502-eeeeeeeeee",
            "ag-20260928-101502-ffffffffff",
            "succeeded",
        );
        app.apply_sessions(&sessions_page(vec![live, finished], 1));
        app.selected = 1; // the finished card
        assert!(app.watch_selected().is_some());

        app.toggle_completed();
        let mut pipeline = Pipeline::new(FRAME_INTERVAL);
        match pipeline.prepare_frame(&mut app) {
            Dispatched::Watch(agent, ..) => assert_eq!(agent.as_str(), STABLE),
            other => panic!("expected watch, got {other:?}"),
        }
        let buffer = app.transcript.as_ref().unwrap();
        assert_eq!(
            buffer.agent.agent_id.as_str(),
            STABLE,
            "the pane follows the first visible card"
        );
    }

    /// Home and cursor visibility use full document offsets beyond u16::MAX.
    #[test]
    fn seventy_thousand_lines_scroll_to_the_top_and_cursor() {
        let mut app = split_app(
            serde_json::json!([{"seq":1,"at":100.0,"role":"assistant","name":null,"content":"x\n".repeat(70_000),"raw_ref":null}, {"seq":2,"at":101.0,"role":"user","name":null,"content":"next","raw_ref":null}]),
        );
        let pane = crate::ui::panes(&app, app.last_width, app.last_height)
            .transcript
            .unwrap();
        let width = crate::ui::transcript::body_width(pane);
        let viewport = usize::from(
            pane.height
                .saturating_sub(crate::ui::transcript::HEADER_ROWS),
        );
        let total = crate::ui::transcript::total_height(app.transcript.as_ref().unwrap(), width);
        apply_action(&mut app, Action::Top);
        assert_eq!(
            crate::ui::transcript::scroll_offset(app.transcript.as_ref().unwrap(), total, viewport),
            0
        );
        scroll_transcript(&mut app, 1);
        assert_eq!(
            crate::ui::transcript::scroll_offset(app.transcript.as_ref().unwrap(), total, viewport),
            1
        );
        apply_action(&mut app, Action::Top);
        scroll_cursor_into_view(&mut app);
        assert_eq!(
            crate::ui::transcript::scroll_offset(app.transcript.as_ref().unwrap(), total, viewport),
            0
        );
        app.transcript.as_mut().unwrap().cursor = 1;
        scroll_cursor_into_view(&mut app);
        let buffer = app.transcript.as_ref().unwrap();
        let cursor = crate::ui::transcript::cursor_line(buffer, width).unwrap();
        let offset = crate::ui::transcript::scroll_offset(buffer, total, viewport);
        assert!(cursor >= offset && cursor < offset + viewport);
    }

    /// A pending listing reorder cannot redirect a click to a different agent.
    #[test]
    fn clicks_resolve_the_drawn_session_identity_after_a_reorder() {
        let first = crate::tests_support::agent_view(STABLE, RUN, "running");
        let mut second = first.clone();
        second.agent_id = RUN.parse().unwrap();
        let mut app = App::new();
        app.last_width = 100;
        app.last_height = 30;
        app.apply_sessions(&sessions_page(vec![first.clone()], 1));
        let mut terminal = Terminal::new(ratatui::backend::TestBackend::new(100, 30)).unwrap();
        let mut pipeline = Pipeline::new(FRAME_INTERVAL);
        pipeline
            .draw(&mut app, &mut terminal, Instant::now())
            .unwrap();
        second.created_at = first.created_at + 1.0;
        app.apply_sessions(&sessions_page(vec![first, second], 2));
        app.selected = 0;
        pipeline.ui_event(
            &mut app,
            mouse(MouseEventKind::Down(MouseButton::Left), 4, 4),
        );
        assert_eq!(app.selected_agent_id().unwrap().as_str(), STABLE);
    }

    /// Content expansion between frames cannot redirect a transcript click;
    /// a stationary pointer follows the newly drawn rows after scrolling.
    #[test]
    fn transcript_click_uses_drawn_sequence_and_hover_tracks_a_stationary_pointer() {
        let mut app = split_app(
            serde_json::json!([{"seq":1,"at":100.0,"role":"user","name":null,"content":"line\n".repeat(30),"raw_ref":null}, {"seq":2,"at":101.0,"role":"assistant","name":null,"content":"second block","raw_ref":null}]),
        );
        let mut terminal = Terminal::new(ratatui::backend::TestBackend::new(160, 20)).unwrap();
        let mut pipeline = Pipeline::new(FRAME_INTERVAL);
        pipeline
            .draw(&mut app, &mut terminal, Instant::now())
            .unwrap();
        let row = app
            .hits
            .borrow()
            .rows
            .iter()
            .position(|hit| hit.message.as_ref().is_some_and(|(_, seq)| *seq == 2))
            .unwrap() as u16;
        app.transcript.as_mut().unwrap().toggle_expanded(1);
        pipeline.ui_event(
            &mut app,
            mouse(MouseEventKind::Down(MouseButton::Left), 60, row),
        );
        assert_eq!(
            app.transcript.as_ref().unwrap().messages[app.transcript.as_ref().unwrap().cursor].seq,
            2
        );
        pipeline.ui_event(&mut app, mouse(MouseEventKind::Moved, 60, 6));
        pipeline.prepare_frame(&mut app);
        app.dirty = true;
        pipeline
            .draw(&mut app, &mut terminal, Instant::now())
            .unwrap();
        assert_eq!(app.transcript.as_ref().unwrap().hover, Some(0));
        apply_action(&mut app, Action::Bottom);
        app.dirty = true;
        pipeline
            .draw(&mut app, &mut terminal, Instant::now())
            .unwrap();
        assert_eq!(
            app.transcript.as_ref().unwrap().hover,
            transcript_message_at(&app, crate::ui::panes(&app, 160, 20).transcript.unwrap(), 6)
        );
    }
    /// Hover follows a stationary pointer when Home changes the visible blocks.
    #[test]
    fn stationary_pointer_hover_changes_with_document_scroll() {
        let messages: Vec<_> = (1..=40).map(|seq| serde_json::json!({"seq":seq,"at":100.0,"role":"assistant","name":format!("block-{seq}"),"content":"block","raw_ref":format!("item-{seq}")})).collect();
        let mut app = split_app(serde_json::json!(messages));
        let mut terminal = Terminal::new(ratatui::backend::TestBackend::new(160, 20)).unwrap();
        let mut pipeline = Pipeline::new(FRAME_INTERVAL);
        pipeline
            .draw(&mut app, &mut terminal, Instant::now())
            .unwrap();
        pipeline.ui_event(&mut app, mouse(MouseEventKind::Moved, 60, 6));
        pipeline.prepare_frame(&mut app);
        app.dirty = true;
        pipeline
            .draw(&mut app, &mut terminal, Instant::now())
            .unwrap();
        let old = app.transcript.as_ref().unwrap().hover.unwrap();
        apply_action(&mut app, Action::Top);
        app.dirty = true;
        pipeline
            .draw(&mut app, &mut terminal, Instant::now())
            .unwrap();
        assert_eq!(app.transcript.as_ref().unwrap().hover, Some(0));
        assert_ne!(old, 0);
    }

    /// Tail opens render newest content immediately and follow the reverse
    /// resume cursor even when reverse complete means beginning reached.
    #[tokio::test]
    async fn tail_first_open_follows_resume_even_when_reverse_complete() {
        use crate::tests_support::{block_message, block_page, FakeBroker};
        let newest = block_page(
            vec![block_message(
                901,
                950,
                "assistant",
                "newest content",
                "tail",
            )],
            None,
            None,
            950,
            true,
        );
        let (broker, mut requests) = FakeBroker::scripted(vec![
            Ok(serde_json::to_value(newest).unwrap()),
            Ok(serde_json::to_value(block_page(vec![], None, None, 950, false)).unwrap()),
        ]);
        let (tx, mut rx) = mpsc::channel(2);
        let (_cmd_tx, cmd_rx) = watch::channel(WatchCommand::Tail {
            agent: STABLE.parse().unwrap(),
            run: Some(RUN.parse().unwrap()),
            blocks: 34,
        });
        let task = tokio::spawn(transcript_worker(broker, cmd_rx, tx));
        let request = requests.recv().await.unwrap();
        assert_eq!(request["view"], "blocks");
        assert_eq!(request["tail_blocks"], 34);
        assert_eq!(request["cursor"], 0);
        assert_eq!(request["run_id"], RUN);
        let mut app = App::new();
        app.sessions = vec![crate::tests_support::agent_view(STABLE, RUN, "running")];
        app.open_selected_transcript();
        let event = tokio::time::timeout(Duration::from_secs(2), rx.recv())
            .await
            .unwrap()
            .unwrap();
        Pipeline::new(FRAME_INTERVAL).broker_event(&mut app, event);
        let buffer = app.transcript.as_ref().unwrap();
        assert_eq!(buffer.messages[0].content, "newest content");
        assert!(buffer.follow);
        assert_eq!(buffer.resume_cursor, 950);
        assert!(buffer.history_complete);
        let request = tokio::time::timeout(Duration::from_secs(2), requests.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(request["cursor"], 950);
        assert!(request["tail_blocks"].is_null());
        assert!(request["before_cursor"].is_null());
        task.abort();
    }

    /// Home requests exclusive older history once; prepend keeps the reading
    /// anchor and cursor while unchanged block row memos survive.
    #[test]
    fn older_prepend_preserves_anchor_and_memos() {
        use crate::tests_support::{block_message, block_page};
        let mut app = App::new();
        app.last_width = 100;
        app.last_height = 20;
        app.sessions = vec![crate::tests_support::agent_view(STABLE, RUN, "running")];
        app.open_selected_transcript();
        let agent = STABLE.parse().unwrap();
        let messages = (100..120)
            .map(|seq| {
                block_message(
                    seq,
                    seq,
                    "assistant",
                    &format!("tail {seq}"),
                    &format!("ref{seq}"),
                )
            })
            .collect();
        app.apply_transcript(&agent, block_page(messages, None, Some(100), 119, true));
        apply_action(&mut app, Action::Top);
        let pane = crate::ui::panes(&app, 100, 20).transcript.unwrap();
        let width = crate::ui::transcript::body_width(pane);
        let viewport = usize::from(pane.height - crate::ui::transcript::HEADER_ROWS);
        let buffer = app.transcript.as_mut().unwrap();
        buffer.cursor = 3;
        buffer.hover = Some(4);
        let offset = crate::ui::transcript::scroll_offset(
            buffer,
            crate::ui::transcript::total_height(buffer, width),
            viewport,
        );
        let built = crate::ui::transcript::cache_block_builds(buffer);
        let mut pipeline = Pipeline::new(FRAME_INTERVAL);
        assert!(matches!(
            pipeline.prepare_frame(&mut app),
            Dispatched::Older(_, _, 119, 100)
        ));
        assert!(
            matches!(pipeline.prepare_frame(&mut app), Dispatched::None),
            "one reverse request per page"
        );
        let older = (80..100)
            .map(|seq| {
                block_message(
                    seq,
                    seq,
                    "assistant",
                    &format!("old {seq}"),
                    &format!("ref{seq}"),
                )
            })
            .collect();
        app.apply_transcript(&agent, block_page(older, Some(100), Some(80), 99, true));
        let buffer = app.transcript.as_ref().unwrap();
        let total = crate::ui::transcript::total_height(buffer, width);
        let next_offset = crate::ui::transcript::scroll_offset(buffer, total, viewport);
        assert_eq!(next_offset - offset, 40, "20 new blocks plus separators");
        assert_eq!(buffer.messages[buffer.cursor].seq, 103);
        assert_eq!(buffer.messages[buffer.hover.unwrap()].seq, 104);
        assert_eq!(buffer.resume_cursor, 119, "older reads never rewind follow");
        assert_eq!(buffer.previous_cursor, Some(80));
        assert_eq!(
            crate::ui::transcript::cache_block_builds(buffer) - built,
            20,
            "existing blocks are moved, not rebuilt"
        );
    }

    /// Older wire requests use cursor zero and the exclusive previous cursor;
    /// the next forward call retains the newest resume cursor.
    #[tokio::test]
    async fn older_worker_keeps_forward_resume_cursor() {
        use crate::tests_support::{block_message, block_page, FakeBroker};
        let older = block_page(
            vec![block_message(1, 10, "assistant", "old", "old")],
            Some(100),
            None,
            10,
            true,
        );
        let (broker, mut requests) =
            FakeBroker::scripted(vec![Ok(serde_json::to_value(older).unwrap())]);
        let (tx, mut rx) = mpsc::channel(2);
        let (_cmd_tx, cmd_rx) = watch::channel(WatchCommand::Older {
            agent: STABLE.parse().unwrap(),
            run: None,
            cursor: 150,
            before: 100,
        });
        let task = tokio::spawn(transcript_worker(broker, cmd_rx, tx));
        let request = requests.recv().await.unwrap();
        assert_eq!(request["view"], "blocks");
        assert_eq!(request["cursor"], 0);
        assert_eq!(request["before_cursor"], 100);
        assert!(matches!(
            rx.recv().await,
            Some(BrokerEvent::Transcript { page: Ok(_), .. })
        ));
        let request = tokio::time::timeout(Duration::from_secs(2), requests.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(request["cursor"], 150);
        assert!(request["before_cursor"].is_null());
        task.abort();
    }

    /// Unsupported parameters downgrade once; subsequent targets keep raw
    /// requests. Other failures still use the ordinary retry policy.
    #[tokio::test]
    async fn unsupported_blocks_fall_back_once_to_raw() {
        use crate::tests_support::FakeBroker;
        let agent = STABLE.parse().unwrap();
        let (broker, mut requests) = FakeBroker::scripted(vec![
            Err("unknown field 'view'".to_owned()),
            Ok(script_page(&agent, &[1], true)),
        ]);
        let (tx, mut rx) = mpsc::channel(2);
        let (cmd_tx, cmd_rx) = watch::channel(WatchCommand::Tail {
            agent: agent.clone(),
            run: None,
            blocks: 40,
        });
        let task = tokio::spawn(transcript_worker(broker, cmd_rx, tx));
        assert_eq!(requests.recv().await.unwrap()["view"], "blocks");
        let raw = requests.recv().await.unwrap();
        assert!(raw.get("view").is_none());
        assert_eq!(raw["limit"], 1000);
        assert_eq!(raw["cursor"], 0);
        assert!(matches!(
            rx.recv().await,
            Some(BrokerEvent::Transcript { page: Ok(_), .. })
        ));
        cmd_tx
            .send(WatchCommand::Watch {
                agent: RUN.parse().unwrap(),
                run: None,
                cursor: 123,
            })
            .unwrap();
        let next = tokio::time::timeout(Duration::from_secs(2), requests.recv())
            .await
            .unwrap()
            .unwrap();
        assert!(next.get("view").is_none());
        assert_eq!(next["cursor"], 123);
        assert!(!net::blocks_unsupported(&agent_run::Error::Runtime(
            "state database operation failed".to_owned()
        )));
        task.abort();
    }
}
