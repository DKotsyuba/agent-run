//! Input, ticking, and broker watchers driving the application state.
//!
//! Three async workers feed one event loop: the terminal input thread, the
//! sessions watcher (long-polling the committed store revision), and the
//! transcript watcher (cursor paging one selected session). The event loop
//! owns the [`App`] state, applies pure reducers, dispatches worker commands,
//! and redraws after every delivered event.

use crate::app::App;
use crate::net::{self, Broker, SharedBroker};
use agent_run_domain::domain::AgentId;
use agent_run_domain::views::{AgentPage, AnswerView, TranscriptPage};
use ratatui::Terminal;
use ratatui::backend::CrosstermBackend;
use ratatui::crossterm::event::{
    DisableMouseCapture, EnableMouseCapture, Event as TerminalEvent, KeyCode, KeyEvent,
    KeyEventKind, KeyModifiers, MouseButton, MouseEvent, MouseEventKind,
};
use std::io::Stdout;
use std::sync::Arc;
use std::time::Duration;
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
/// Table rows the selection logic assumes between redraws; rendering clamps
/// with the real height, this only keeps `list_scroll` roughly current.
const ASSUMED_CARD_ROWS: usize = 20;

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
}

/// Commands addressed to the transcript watcher.
#[derive(Debug)]
pub enum WatchCommand {
    /// Follow one session; history is refetched from the start.
    Watch {
        /// Stable agent id to follow.
        agent: AgentId,
        /// Exact execution to pin, when known.
        run: Option<AgentId>,
    },
    /// Stop following; the watcher idles until the next command.
    Clear,
}

/// The sessions scope currently requested by the operator.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Scope {
    /// Whether terminal (finished) sessions are included.
    pub show_all: bool,
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
/// operator flips the scope; a successful round carries the fresh revision
/// for the next long-poll watch.
pub async fn sessions_worker(
    broker: SharedBroker,
    mut scope: watch::Receiver<Scope>,
    tx: mpsc::UnboundedSender<BrokerEvent>,
) {
    let mut current = *scope.borrow();
    let mut revision: Option<i64> = None;
    loop {
        tokio::select! {
            changed = scope.changed() => {
                if changed.is_err() {
                    return;
                }
                current = *scope.borrow();
                revision = None;
            }
            result = fetch_sessions(&*broker, current, revision) => {
                match result {
                    Ok(page) => {
                        revision = Some(page.revision);
                        if tx.send(BrokerEvent::Sessions(Ok(page))).is_err() {
                            return;
                        }
                    }
                    Err(error) => {
                        revision = None;
                        if tx.send(BrokerEvent::Sessions(Err(error.to_string()))).is_err() {
                            return;
                        }
                        tokio::time::sleep(RETRY_DELAY).await;
                    }                }
            }
        }
    }
}

/// Cursor-pages one selected transcript and keeps following its tail.
///
/// Backfill rounds run without pauses until the known history is complete;
/// afterwards the watcher polls the tail once per second so new messages and
/// steering output appear live.
pub async fn transcript_worker(
    broker: SharedBroker,
    mut commands: mpsc::UnboundedReceiver<WatchCommand>,
    tx: mpsc::UnboundedSender<BrokerEvent>,
) {
    let mut target: Option<(AgentId, Option<AgentId>)> = None;
    let mut cursor: i64 = 0;
    let mut fetch_now = false;
    loop {
        if target.is_none() {
            match commands.recv().await {
                Some(WatchCommand::Watch { agent, run }) => {
                    target = Some((agent, run));
                    cursor = 0;
                    fetch_now = true;
                }
                Some(WatchCommand::Clear) | None => return,
            }
            continue;
        }
        let pause = if fetch_now {
            Duration::ZERO
        } else {
            TRANSCRIPT_TAIL_POLL
        };
        fetch_now = false;
        tokio::select! {
            command = commands.recv() => match command {
                Some(WatchCommand::Watch { agent, run }) => {
                    target = Some((agent, run));
                    cursor = 0;
                    fetch_now = true;
                }
                Some(WatchCommand::Clear) => target = None,
                None => return,
            },
            _ = tokio::time::sleep(pause) => {
                let Some((agent, run)) = target.clone() else { continue };
                match net::transcript_page(&*broker, &agent, run.as_ref(), cursor, net::TRANSCRIPT_PAGE_LIMIT).await {
                    Ok(page) => {
                        if let Some(next) = page.next_cursor {
                            cursor = next;
                        } else if let Some(last) = page.messages.last() {
                            cursor = last.seq;
                        }
                        if tx.send(BrokerEvent::Transcript { agent, page: Ok(page) }).is_err() {
                            return;
                        }
                    }
                    Err(error) => {
                        let failed = tx.send(BrokerEvent::Transcript {
                            agent,
                            page: Err(error.to_string()),
                        });
                        if failed.is_err() {
                            return;
                        }
                        tokio::time::sleep(RETRY_DELAY).await;
                    }
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
    match app.screen {
        crate::app::Screen::Sessions => match key.code {
            KeyCode::Char('q') => Action::Quit,
            KeyCode::Up | KeyCode::Char('k') => Action::Move(-1),
            KeyCode::Down | KeyCode::Char('j') => Action::Move(1),
            KeyCode::PageUp => Action::Move(-10),
            KeyCode::PageDown => Action::Move(10),
            KeyCode::Enter | KeyCode::Right => Action::Open,
            KeyCode::Tab | KeyCode::Char('o') => Action::ToggleCompleted,
            KeyCode::Char('r') => Action::Refresh,
            KeyCode::Char('a') => Action::Answer,
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
            _ => Action::None,
        },
    }
}

/// Side effects the event loop must dispatch after applying an action.
#[derive(Debug)]
pub enum Dispatched {
    /// Nothing to dispatch.
    None,
    /// Follow the opened session.
    Watch(AgentId, Option<AgentId>),
    /// Stop transcript watching.
    Clear,
    /// Reissue the sessions listing for the current scope.
    Scope,
    /// Fetch the answer of one session.
    Answer(AgentId, Option<AgentId>),
}

/// Applies one action to the state and returns its side effects.
pub fn apply_action(app: &mut App, action: Action) -> Dispatched {
    match action {
        Action::None => Dispatched::None,
        Action::Quit => {
            app.quit = true;
            Dispatched::None
        }
        Action::Move(delta) => {
            app.move_selection(delta);
            app.sync_list_scroll(ASSUMED_CARD_ROWS);
            Dispatched::None
        }
        Action::Open => match app.open_selected_transcript() {
            Some((agent, run)) => Dispatched::Watch(agent, run),
            None => Dispatched::None,
        },
        Action::Back => {
            if app.answer.is_some() {
                app.close_answer();
                Dispatched::None
            } else if app.screen == crate::app::Screen::Transcript {
                app.close_transcript();
                Dispatched::Clear
            } else {
                Dispatched::None
            }
        }
        Action::ToggleCompleted => {
            app.toggle_completed();
            Dispatched::Scope
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
            scroll_transcript(app, delta);
            Dispatched::None
        }
        Action::Top => {
            if let Some(buffer) = app.transcript.as_mut() {
                buffer.scroll_top();
            }
            Dispatched::None
        }
        Action::Bottom => {
            if let Some(buffer) = app.transcript.as_mut() {
                buffer.scroll_bottom();
            }
            Dispatched::None
        }
        Action::Answer => match (app.selected_agent_id(), app.selected_run_id()) {
            (Some(agent), run) => Dispatched::Answer(agent, run),
            (None, _) => Dispatched::None,
        },
    }
}

/// Scrolls the transcript so the message cursor stays on screen.
///
/// The layout is recomputed with the last rendered width, matching the
/// renderer; the scroll lands with the cursor line at the viewport top when
/// moving down and just above it when moving up.
fn scroll_cursor_into_view(app: &mut App) {
    let width = app.last_width.saturating_sub(2);
    let Some(buffer) = app.transcript.as_ref() else {
        return;
    };
    let Some(cursor_line) = crate::ui::transcript::cursor_line(buffer, width) else {
        return;
    };
    let total = crate::ui::transcript::total_height(buffer, width);
    let viewport = app.last_height.saturating_sub(4) as usize; // status bar + border + header
    if viewport == 0 {
        return;
    }
    let buffer = app.transcript.as_mut().expect("checked above");
    if cursor_line < viewport {
        // Cursor is near the top: jump there and leave follow mode.
        buffer.follow = false;
        buffer.from_bottom = (total.saturating_sub(cursor_line + 1)) as u16;
    } else {
        buffer.follow = false;
        let from_top = cursor_line.saturating_sub(viewport.saturating_sub(1));
        buffer.from_bottom = (total.saturating_sub(from_top + viewport)) as u16;
    }
}

/// Applies one signed transcript scroll; negative moves toward the top.
pub fn scroll_transcript(app: &mut App, delta: i64) {
    let Some(buffer) = app.transcript.as_mut() else {
        return;
    };
    if delta < 0 {
        buffer.scroll_up(delta.unsigned_abs() as u16);
    } else if delta > 0 {
        buffer.scroll_down(delta as u16);
    }
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
    std::thread::spawn(move || {
        loop {
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
        }
    });
}

/// Runs the interactive loop until the operator quits.
pub async fn run(
    mut app: App,
    broker: SharedBroker,
    terminal: &mut Terminal<CrosstermBackend<Stdout>>,
) -> agent_run::Result<()> {
    let (broker_tx, mut broker_rx) = mpsc::unbounded_channel::<BrokerEvent>();
    let (ui_tx, mut ui_rx) = mpsc::unbounded_channel::<UiEvent>();
    let (watch_tx, watch_rx) = mpsc::unbounded_channel::<WatchCommand>();
    let (scope_tx, scope_rx) = watch::channel(Scope {
        show_all: app.completed_open,
    });

    spawn_input(ui_tx);
    tokio::spawn(sessions_worker(broker.clone(), scope_rx, broker_tx.clone()));
    tokio::spawn(transcript_worker(
        broker.clone(),
        watch_rx,
        broker_tx.clone(),
    ));

    let mut ticker = tokio::time::interval(TICK);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

    loop {
        tokio::select! {
            _ = ticker.tick() => app.tick(),
            event = ui_rx.recv() => match event {
                Some(UiEvent::Terminal(TerminalEvent::Key(key))) => {
                    let action = key_action(&app, key);
                    let dispatched = apply_action(&mut app, action);
                    dispatch(&app, dispatched, &watch_tx, &scope_tx, &broker, &broker_tx);
                }
                Some(UiEvent::Terminal(TerminalEvent::Mouse(mouse))) => {
                    let dispatched = apply_mouse(&mut app, mouse);
                    dispatch(&app, dispatched, &watch_tx, &scope_tx, &broker, &broker_tx);
                }
                Some(UiEvent::Terminal(TerminalEvent::Resize(_, _))) => {}
                Some(UiEvent::Terminal(_)) => {}
                Some(UiEvent::InputClosed) | None => app.quit = true,
            },
            event = broker_rx.recv() => match event {
                Some(BrokerEvent::Sessions(Ok(page))) => app.apply_sessions(&page),
                Some(BrokerEvent::Sessions(Err(message))) => app.apply_broker_error(message),
                Some(BrokerEvent::Transcript { agent, page: Ok(page) }) => {
                    app.apply_transcript(&agent, &page)
                }
                Some(BrokerEvent::Transcript { page: Err(message), .. }) => {
                    app.apply_broker_error(message)
                }
                Some(BrokerEvent::Answer(Ok(view))) => app.apply_answer(view),
                Some(BrokerEvent::Answer(Err(message))) => app.apply_broker_error(message),
                None => app.quit = true,
            },
        }
        let completed = terminal
            .draw(|frame| crate::ui::render(frame, &app))
            .map_err(|error| agent_run::Error::Runtime(format!("terminal draw failed: {error}")))?;
        app.last_width = completed.area.width.max(1);
        app.last_height = completed.area.height.max(1);
        if app.quit {
            return Ok(());
        }
    }
}

/// Applies one mouse event and returns its side effects.
fn apply_mouse(app: &mut App, mouse: MouseEvent) -> Dispatched {
    match mouse.kind {
        MouseEventKind::ScrollUp => match app.screen {
            crate::app::Screen::Sessions => {
                app.move_selection(-1);
                app.sync_list_scroll(ASSUMED_CARD_ROWS);
                Dispatched::None
            }
            crate::app::Screen::Transcript => {
                scroll_transcript(app, -1);
                Dispatched::None
            }
        },
        MouseEventKind::ScrollDown => match app.screen {
            crate::app::Screen::Sessions => {
                app.move_selection(1);
                app.sync_list_scroll(ASSUMED_CARD_ROWS);
                Dispatched::None
            }
            crate::app::Screen::Transcript => {
                scroll_transcript(app, 1);
                Dispatched::None
            }
        },
        MouseEventKind::Moved => {
            if app.screen == crate::app::Screen::Transcript {
                let hover = transcript_message_at(app, &mouse);
                if let Some(buffer) = app.transcript.as_mut() {
                    buffer.hover = hover;
                }
            } else if let Some(buffer) = app.transcript.as_mut() {
                buffer.hover = None;
            }
            Dispatched::None
        }
        MouseEventKind::Down(MouseButton::Left) => match app.screen {
            crate::app::Screen::Sessions => mouse_click_sessions(app, mouse),
            crate::app::Screen::Transcript => {
                mouse_click_transcript(app, mouse);
                Dispatched::None
            }
        },
        _ => Dispatched::None,
    }
}

/// Handles one sessions-screen click: dropdown toggle, card select, card open.
fn mouse_click_sessions(app: &mut App, mouse: MouseEvent) -> Dispatched {
    let Some(area) = main_area(app) else {
        return Dispatched::None;
    };
    if crate::ui::list::dropdown_clicked(app, area, mouse.row) {
        app.toggle_completed();
        return Dispatched::Scope;
    }
    let Some(card) = crate::ui::list::card_at(app, area, mouse.column, mouse.row) else {
        return Dispatched::None;
    };
    let already_selected = app.selected == card;
    app.select_card(card);
    app.sync_list_scroll(ASSUMED_CARD_ROWS);
    if already_selected {
        match app.open_selected_transcript() {
            Some((agent, run)) => Dispatched::Watch(agent, run),
            None => Dispatched::None,
        }
    } else {
        Dispatched::None
    }
}

/// Handles one transcript click: selects and toggles the message under it.
fn mouse_click_transcript(app: &mut App, mouse: MouseEvent) {
    let Some(message) = transcript_message_at(app, &mouse) else {
        return;
    };
    let buffer = app
        .transcript
        .as_mut()
        .expect("hover target implies open transcript");
    buffer.cursor = message;
    buffer.toggle_expanded_at_cursor();
}

/// The transcript message under one mouse position, if any.
///
/// Recomputes the body offset exactly like the renderer: the layout and the
/// scroll position both derive from the same state, so hover and click land
/// on the message the operator sees under the pointer.
fn transcript_message_at(app: &App, mouse: &MouseEvent) -> Option<usize> {
    let area = main_area(app)?;
    // Body rows start below the block border and the summary header line.
    let body_row = mouse.row.checked_sub(area.y + 2)?;
    if body_row >= area.height.saturating_sub(3) {
        return None; // bottom border
    }
    let width = area.width.saturating_sub(2);
    let buffer = app.transcript.as_ref()?;
    let viewport = area.height.saturating_sub(3);
    let total = crate::ui::transcript::total_height(buffer, width) as u16;
    let offset = if buffer.follow {
        total.saturating_sub(viewport)
    } else {
        total
            .saturating_sub(viewport)
            .saturating_sub(buffer.from_bottom)
    };
    let clicked_line = offset as usize + body_row as usize;
    crate::ui::transcript::message_at(buffer, width, clicked_line)
}

/// The main content area above the status bar, from the last rendered frame.
fn main_area(app: &App) -> Option<ratatui::layout::Rect> {
    if app.last_height < 2 {
        return None;
    }
    Some(ratatui::layout::Rect {
        x: 0,
        y: 0,
        width: app.last_width,
        height: app.last_height - 1,
    })
}

/// Dispatches one action's side effects to the workers.
fn dispatch(
    app: &App,
    dispatched: Dispatched,
    watch_tx: &mpsc::UnboundedSender<WatchCommand>,
    scope_tx: &watch::Sender<Scope>,
    broker: &SharedBroker,
    broker_tx: &mpsc::UnboundedSender<BrokerEvent>,
) {
    match dispatched {
        Dispatched::None => {}
        Dispatched::Watch(agent, run) => {
            let _ = watch_tx.send(WatchCommand::Watch { agent, run });
        }
        Dispatched::Clear => {
            let _ = watch_tx.send(WatchCommand::Clear);
        }
        Dispatched::Scope => {
            let _ = scope_tx.send(Scope {
                show_all: app.completed_open,
            });
        }
        Dispatched::Answer(agent, run) => {
            let broker = Arc::clone(broker);
            let tx = broker_tx.clone();
            tokio::spawn(async move {
                let result = net::answer(&*broker, &agent, run.as_ref())
                    .await
                    .map_err(|error| error.to_string());
                let _ = tx.send(BrokerEvent::Answer(result));
            });
        }
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
            Dispatched::Watch(agent, run) => {
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
        // Card list: the border consumes row 0; each card spans eight
        // terminal rows (padding + content + gap), so the second card starts
        // at row 3 + 8.
        let click_card = |column, row| MouseEvent {
            kind: MouseEventKind::Down(MouseButton::Left),
            column,
            row,
            modifiers: KeyModifiers::NONE,
        };
        assert!(matches!(
            apply_mouse(&mut app, click_card(5, 11)),
            Dispatched::None
        ));
        assert_eq!(app.selected, 1);
        // A second click on the same card opens the transcript.
        match apply_mouse(&mut app, click_card(5, 11)) {
            Dispatched::Watch(agent, _) => {
                assert_eq!(agent.as_str(), "ag-20260928-101501-cccccccccc")
            }
            other => panic!("expected watch, got {other:?}"),
        }
        // Clicking the dropdown line toggles the finished section.
        app.close_transcript();
        let finished = crate::tests_support::agent_view(
            "ag-20260928-101502-eeeeeeeeee",
            "ag-20260928-101502-ffffffffff",
            "succeeded",
        );
        app.sessions.push(finished);
        let dropdown_row = 30 - 1 - 2; // status bar row and block border excluded
        assert!(matches!(
            apply_mouse(&mut app, click_card(5, dropdown_row)),
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
        app.apply_transcript(&AgentId::from_str(STABLE).unwrap(), &page);

        let move_to = |column, row| MouseEvent {
            kind: MouseEventKind::Moved,
            column,
            row,
            modifiers: KeyModifiers::NONE,
        };
        // Hover over the second message (body row 2, below the first pair).
        apply_mouse(&mut app, move_to(5, 4));
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
}
