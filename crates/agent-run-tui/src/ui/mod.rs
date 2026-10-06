//! Root rendering: frame chrome (app bar, key bar), pane layout, and the
//! shared overlay popup family.

pub mod answer;
pub mod list;
pub mod overlay;
pub mod pools;
pub mod projects;
pub mod text;
pub mod theme;
pub mod transcript;

use crate::app::{self, App, Link, Screen};
use ratatui::{
    Frame,
    layout::Rect,
    style::{Modifier, Style},
    text::{Line, Span},
    widgets::{Block, Paragraph},
};

/// Content columns of the session list pane in the split view.
pub const LIST_COLS: u16 = 43;

/// The content panes of one frame.
#[derive(Debug, Clone, Copy, Default)]
pub struct Panes {
    /// The session list pane, when visible.
    pub list: Option<Rect>,
    /// The transcript pane, when visible.
    pub transcript: Option<Rect>,
}

/// Lays out one frame: app bar (row 0), one blank row, the panes, and the
/// key bar on the last row.
///
/// Wide terminals (at least [`app::SPLIT_WIDTH`] columns) always show both
/// panes side by side — the session list plus the transcript of the
/// selection; narrow terminals switch between the two full-screen panes.
pub fn panes(app: &App, width: u16, height: u16) -> Panes {
    let main = Rect {
        x: 0,
        y: 2,
        width,
        height: height.saturating_sub(3),
    };
    let mut panes = Panes::default();
    if main.height == 0 || width < 4 {
        return panes;
    }
    if app.pools.visible && app.pools.member_transcript {
        panes.transcript = Some(Rect {
            x: 1,
            width: width - 2,
            ..main
        });
    } else if width >= app::SPLIT_WIDTH {
        panes.list = Some(Rect {
            x: 1,
            width: LIST_COLS.min(width.saturating_sub(2)),
            ..main
        });
        let transcript_x = LIST_COLS + 3; // list pad column plus one gap column
        panes.transcript = Some(Rect {
            x: transcript_x,
            width: width - transcript_x,
            ..main
        });
    } else {
        let pane = Rect {
            x: 1,
            width: width - 2,
            ..main
        };
        match app.screen {
            Screen::Sessions if app.pools.visible && app.pools.focused => {
                panes.transcript = Some(pane)
            }
            Screen::Sessions => panes.list = Some(pane),
            Screen::Transcript => panes.transcript = Some(pane),
        }
    }
    panes
}

/// Renders one full frame from the current application state.
pub fn render(f: &mut Frame, app: &App) {
    let area = f.area();
    let p = theme::palette();
    // Base coat: the whole frame carries the design background.
    f.render_widget(Block::new().style(Style::new().bg(p.bg)), area);
    let panes = panes(app, area.width, area.height);
    app.pools.hits.borrow_mut().clear();
    if let Some(list) = panes.list {
        // The list pane sits on a panel shelf one padding column wide on
        // each side of the content.
        let shelf = Rect {
            width: (list.width + 2).min(area.width),
            y: 2,
            height: area.height.saturating_sub(3),
            x: 0,
        };
        f.render_widget(Paragraph::new("").style(Style::new().bg(p.panel)), shelf);
        if app.pools.visible {
            pools::render_list(f, app, list);
        } else {
            list::render(f, app, list);
        }
    }
    if let Some(transcript) = panes.transcript {
        if app.pools.visible && !app.pools.member_transcript {
            pools::render_detail(f, app, transcript);
        } else {
            transcript::render(f, app, transcript, area.width >= app::SPLIT_WIDTH);
        }
    }
    if area.height > 0 {
        render_app_bar(f, app, Rect { height: 1, ..area });
    }
    if area.height > 1 {
        render_tabs(
            f,
            app,
            Rect {
                y: 1,
                height: 1,
                ..area
            },
        );
        render_key_bar(
            f,
            app,
            Rect {
                y: area.height - 1,
                height: 1,
                ..area
            },
            panes,
        );
    }
    overlay::render_help(f, app, area);
    projects::render(f, app, area);
    answer::render(f, app, area);
    pools::render_criteria(f, app, area);
}

/// Renders clickable tabs in row one; fixed cells are shared with pointer routing.
fn render_tabs(f: &mut Frame, app: &App, area: Rect) {
    let p = theme::palette();
    let session = if app.pools.visible {
        theme::dim()
    } else {
        theme::accent().bg(p.accent_bg)
    };
    let pool = if app.pools.visible {
        theme::accent().bg(p.accent_bg)
    } else {
        theme::dim()
    };
    f.render_widget(
        Paragraph::new(Line::from(vec![
            Span::raw(" "),
            Span::styled(" Sessions ", session),
            Span::styled("│", theme::dim()),
            Span::styled(format!(" Pools {} open ", app.pools.open_total), pool),
        ])),
        area,
    );
}

/// Renders the app bar: brand, live/finished counts (or the open agent's
/// hash in the narrow transcript view), and the broker link state.
fn render_app_bar(f: &mut Frame, app: &App, area: Rect) {
    let p = theme::palette();
    let live = app
        .sessions
        .iter()
        .filter(|agent| !agent.status.terminal())
        .count();
    let white = Style::new().fg(p.white);
    let mut left = vec![
        Span::styled(
            " agent-run ",
            Style::new()
                .fg(p.bg)
                .bg(p.accent)
                .add_modifier(Modifier::BOLD),
        ),
        Span::raw("  "),
    ];
    if area.width < app::SPLIT_WIDTH && app.screen == Screen::Transcript {
        if let Some(buffer) = &app.transcript {
            left.push(Span::styled("‹ ", theme::dim()));
            left.push(Span::styled(
                app::id_hash(buffer.agent.agent_id.as_str(), 8),
                white,
            ));
        }
    } else {
        left.push(Span::styled("●", Style::new().fg(p.yellow)));
        left.push(Span::styled(format!(" {live} live   "), white));
        left.push(Span::styled("✓", Style::new().fg(p.green)));
        left.push(Span::styled(
            format!(" {} finished", app.finished_count()),
            white,
        ));
    }
    if let Some(error) = &app.last_error {
        left.push(Span::styled(format!("  {error}"), theme::failure()));
    }
    let mut right = vec![match app.link {
        Link::Up => Span::styled("●", Style::new().fg(p.green)),
        Link::Down => Span::styled("○", Style::new().fg(p.red)),
    }];
    if area.width >= app::SPLIT_WIDTH {
        right.push(Span::styled(
            match app.link {
                Link::Up => " broker",
                Link::Down => " retrying",
            },
            white,
        ));
        right.push(Span::styled(
            format!(
                " rev {}   sessions {} ",
                app.revision.unwrap_or(0),
                app.sessions.len()
            ),
            theme::dim(),
        ));
    } else {
        right.push(Span::raw(" "));
        right.push(Span::styled(
            format!("rev {} ", app.revision.unwrap_or(0)),
            theme::dim(),
        ));
    }
    let spans = text::lr(left, right, area.width as usize, Style::new());
    f.render_widget(
        Paragraph::new(Line::from(spans)).style(Style::new().bg(p.panel)),
        area,
    );
}

/// Renders the key bar: accent keys with dim descriptions, and the visible
/// line counter when a transcript is on screen.
fn render_key_bar(f: &mut Frame, app: &App, area: Rect, panes: Panes) {
    let p = theme::palette();
    let mut left = vec![Span::raw(" ")];
    let mut hints = key_hints(app);
    if app.copy_request.is_some() {
        hints = vec![("esc", "cancel copy"), ("q", "quit")];
    } else if app.answer.is_none() && !app.help && !app.project_picker && !app.pools.criteria {
        hints.insert(0, ("y", "copy"));
    }
    for (key, description) in hints {
        left.push(Span::styled(key, theme::accent()));
        left.push(Span::styled(format!(" {description}  "), theme::dim()));
    }
    let right = key_bar_right(app, panes);
    let spans = text::lr(left, right, area.width as usize, Style::new());
    f.render_widget(
        Paragraph::new(Line::from(spans)).style(Style::new().bg(p.panel)),
        area,
    );
}

/// Key hints of the key bar for the current screen and popup state.
fn key_hints(app: &App) -> Vec<(&'static str, &'static str)> {
    if app.answer.is_some() || app.help || app.project_picker || app.pools.criteria {
        return vec![("esc", "close")];
    }
    if app.pools.visible && !app.pools.member_transcript {
        return if app.pools.focused {
            let roster = app.pools.buffer().is_some_and(|b| b.roster);
            if !app.split_view() {
                return if roster {
                    vec![
                        ("↑↓", "member"),
                        ("t", "transcript"),
                        ("m", "chat"),
                        ("c", "details"),
                        ("h", "history"),
                        ("?", "help"),
                        ("esc", "back"),
                    ]
                } else {
                    vec![
                        ("↑↓", "block"),
                        ("⏎", "expand"),
                        ("m", "roster"),
                        ("c", "details"),
                        ("h", "history"),
                        ("?", "help"),
                        ("esc", "back"),
                    ]
                };
            }
            vec![
                ("↑↓", if roster { "member" } else { "block" }),
                ("⏎", "expand/open"),
                ("m", "roster"),
                ("c", "criteria"),
                ("h", "history"),
                ("f", "follow"),
                ("t", "transcript"),
                ("esc", "list"),
                ("?", "help"),
            ]
        } else {
            vec![
                ("↑↓", "pool"),
                ("⏎", "open"),
                ("[ ]", "pages"),
                ("1 2", "tabs"),
                ("?", "help"),
                ("q", "quit"),
            ]
        };
    }
    match (app.screen, app.split_view()) {
        (Screen::Sessions, true) => vec![
            ("↑↓", "select"),
            ("⏎", "open"),
            ("→", "transcript"),
            ("tab", "finished"),
            ("p", "projects"),
            ("r", "refresh"),
            ("a", "answer"),
            ("?", "help"),
            ("q", "quit"),
        ],
        (Screen::Sessions, false) => vec![
            ("↑↓", "select"),
            ("⏎", "open"),
            ("tab", "finished"),
            ("a", "answer"),
            ("?", "help"),
            ("q", "quit"),
        ],
        (Screen::Transcript, true) => vec![
            ("↑↓", "block"),
            ("⏎", "expand"),
            ("esc", "list"),
            ("f", "follow"),
            ("a", "answer"),
            ("?", "help"),
            ("q", "quit"),
        ],
        (Screen::Transcript, false) => vec![
            ("↑↓", "block"),
            ("⏎", "expand"),
            ("g G", "top · bottom"),
            ("f", "follow"),
            ("a", "answer"),
            ("esc", "back"),
            ("q", "quit"),
        ],
    }
}

/// The right-aligned readout of the key bar: `<last visible>/<total> lines`.
fn key_bar_right(app: &App, panes: Panes) -> Vec<Span<'static>> {
    if let Some(request) = &app.copy_request {
        return vec![Span::styled(
            format!("{} Copying {} ", app.spinner(), request.target.label()),
            theme::accent(),
        )];
    }
    if let Some(feedback) = &app.copy_feedback {
        return vec![Span::styled(
            text::Sanitizer::default().push(feedback),
            theme::dim(),
        )];
    }
    if app.answer.is_some() || app.help || app.project_picker {
        return Vec::new();
    }
    if app.pools.visible && !app.pools.member_transcript {
        return app.pools.buffer().map_or_else(Vec::new, |b| {
            vec![Span::styled(
                b.last_seq
                    .map_or("seq — ".into(), |_| format!("seq #{} ", b.after)),
                theme::dim(),
            )]
        });
    }
    let (Some(pane), Some(buffer)) = (panes.transcript, app.transcript.as_ref()) else {
        return Vec::new();
    };
    let width = transcript::body_width(pane);
    let total = transcript::total_height(buffer, width);
    let viewport = pane.height.saturating_sub(transcript::HEADER_ROWS) as usize;
    let offset = transcript::scroll_offset(buffer, total, viewport);
    vec![Span::styled(
        format!("{}/{} ", offset.saturating_add(viewport).min(total), total),
        theme::dim(),
    )]
}

#[cfg(test)]
mod tests {
    use crate::app::App;
    use crate::tests_support::{agent_view, force_truecolor, message};
    use agent_run_domain::domain::AgentId;
    use agent_run_domain::views::TranscriptPage;
    use ratatui::{Terminal, backend::TestBackend};
    use std::str::FromStr;

    const STABLE: &str = "ag-20260928-101500-aaaaaaaaaa";
    const RUN: &str = "ag-20260928-101500-bbbbbbbbbb";

    /// Renders one frame on a fixed-size canvas and returns its text grid.
    fn render_to_string(app: &App, width: u16, height: u16) -> String {
        force_truecolor();
        let mut terminal = Terminal::new(TestBackend::new(width, height)).unwrap();
        terminal
            .draw(|frame| crate::ui::render(frame, app))
            .unwrap();
        let buffer = terminal.backend().buffer();
        (0..buffer.area.height)
            .map(|y| {
                (0..buffer.area.width)
                    .map(|x| buffer[(x, y)].symbol())
                    .collect::<String>()
            })
            .collect::<Vec<_>>()
            .join("\n")
    }

    /// Builds an app with two sessions (one live, one finished) loaded.
    fn loaded_app() -> App {
        let mut app = App::new();
        app.apply_sessions(&serde_json::from_value(serde_json::json!({
            "items": [
                agent_view(STABLE, RUN, "running"),
                agent_view("ag-20260928-101501-cccccccccc", "ag-20260928-101501-dddddddddd", "failed"),
            ],
            "total": 2, "offset": 0, "limit": 200,
            "next_offset": None::<usize>, "complete": true,
            "revision": 5, "observed_at": 1.0,
        }))
        .unwrap());
        app
    }

    #[test]
    fn sessions_screen_renders_rows_hints_and_status() {
        let mut app = loaded_app();
        let screen = render_to_string(&app, 120, 20);
        assert!(screen.contains("agent-run"), "brand: {screen}");
        assert!(screen.contains("LIVE"), "section header: {screen}");
        assert!(screen.contains("● 1 live"), "stable live count: {screen}");
        assert!(screen.contains("✓ 1 finished"), "finished count: {screen}");
        // Finished sessions stay behind the collapsed header.
        assert!(screen.contains("▸ FINISHED"), "collapsed section: {screen}");
        assert!(
            !screen.contains("✗"),
            "finished row hidden while collapsed: {screen}"
        );
        // The live row shows glyph, task, model, elapsed, and idle silence.
        assert!(screen.contains("ship the thing"), "task: {screen}");
        assert!(screen.contains("gpt-5"), "model: {screen}");
        assert!(screen.contains("42s"), "elapsed: {screen}");
        assert!(screen.contains("idle 3s"), "silence: {screen}");
        assert!(screen.contains("rev 5"), "revision: {screen}");
        assert!(screen.contains("broker"), "link state: {screen}");

        // Expanding the section reveals finished rows.
        app.toggle_completed();
        let expanded = render_to_string(&app, 120, 24);
        assert!(expanded.contains("▾ FINISHED"), "open section: {expanded}");
        assert!(expanded.contains("✗"), "finished row visible: {expanded}");
    }

    #[test]
    fn split_view_shows_list_and_transcript_side_by_side() {
        let mut app = loaded_app();
        // The event loop keeps the split-view pane glued to the selection.
        app.watch_selected();
        let screen = render_to_string(&app, 160, 48);
        // Left: the session list with its header and finished section.
        let list_half: Vec<String> = screen
            .lines()
            .map(|l| l.chars().take(48).collect::<String>())
            .collect();
        assert!(
            list_half.iter().any(|l| l.contains("LIVE")),
            "list header in the left half: {screen}"
        );
        assert!(
            list_half.iter().any(|l| l.contains("ship the thing")),
            "session row in the left half: {screen}"
        );
        // Right: the transcript pane of the selected session.
        assert!(
            screen.contains("CODEX/gpt-5"),
            "transcript runtime/model header: {screen}"
        );
        assert!(screen.contains("● follow"), "follow state: {screen}");
        assert!(
            screen.contains(STABLE),
            "full agent id in split view: {screen}"
        );
        // The transcript pane renders to the right of the list pane.
        let meta = screen.lines().find(|l| l.contains("CODEX/gpt-5")).unwrap();
        assert!(
            meta.find("CODEX/gpt-5").unwrap() > 46,
            "transcript header sits in the right pane: {meta:?}"
        );
    }

    /// Named sessions lead their card and transcript title with the human
    /// display label; unnamed ones keep the bare task text.
    #[test]
    fn cards_show_human_display_names() {
        let mut app = loaded_app();
        app.sessions[0].name = Some("Мария / review".into());
        app.watch_selected();
        let screen = render_to_string(&app, 120, 24);
        assert_eq!(
            screen.matches("Мария / review — ship the thing").count(),
            2,
            "the label leads both the list card and the transcript title: {screen}"
        );
        // The unnamed finished row keeps its bare task summary.
        app.toggle_completed();
        let expanded = render_to_string(&app, 120, 24);
        assert!(
            expanded.matches("Мария / review — ship the thing").count() >= 2,
            "labeled rows survive expanding finished sessions: {expanded}"
        );
        assert!(
            expanded.contains("ship the thing"),
            "task text stays visible beside the label: {expanded}"
        );
    }

    /// Wide display labels fit both panes without overwriting elapsed time
    /// or spilling a split CJK glyph into neighboring cells.
    #[test]
    fn display_names_truncate_by_terminal_width() {
        force_truecolor();
        let mut app = loaded_app();
        app.sessions[0].name = Some("工程師🙂".repeat(30));
        app.watch_selected();
        let mut terminal = Terminal::new(TestBackend::new(120, 24)).unwrap();
        terminal
            .draw(|frame| crate::ui::render(frame, &app))
            .unwrap();
        let buffer = terminal.backend().buffer();
        let list = super::panes(&app, 120, 24).list.unwrap();
        let title_y = list.y + 2;
        let last_title_x = (list.x..list.x + list.width)
            .find(|x| buffer[(*x, title_y)].symbol() == "…")
            .expect("the wide list label is truncated");
        assert!(
            last_title_x < list.x + list.width - 5,
            "elapsed time retains its cells"
        );
        let transcript = super::panes(&app, 120, 24).transcript.unwrap();
        assert_eq!(buffer[(transcript.x, transcript.y)].symbol(), "⠋");
        // A two-cell glyph may leave one padding cell after the ellipsis.
        let end = transcript.x + transcript.width;
        assert!((end - 2..end).any(|x| buffer[(x, transcript.y)].symbol() == "…"));
    }

    #[test]
    fn narrow_view_switches_between_list_and_transcript() {
        let mut app = loaded_app();
        let list_only = render_to_string(&app, 66, 52);
        assert!(list_only.contains("LIVE"), "list visible: {list_only}");
        assert!(
            !list_only.contains("● follow"),
            "transcript hidden before opening: {list_only}"
        );
        assert!(list_only.contains("rev 5"), "compact app bar: {list_only}");

        app.open_selected_transcript();
        let transcript_only = render_to_string(&app, 66, 52);
        assert!(
            transcript_only.contains("● follow"),
            "transcript visible after opening: {transcript_only}"
        );
        assert!(
            transcript_only.contains("‹ aaaaaaaa"),
            "narrow app bar shows the open agent hash: {transcript_only}"
        );
        assert!(
            !transcript_only.contains("LIVE  1"),
            "list hidden in the transcript view: {transcript_only}"
        );
    }

    #[test]
    fn list_rows_show_workdir_basename_and_hash_tail() {
        let mut app = App::new();
        app.home_prefix = Some("/Users/pluto".to_string());
        let mut agent = agent_view(STABLE, RUN, "running");
        agent.workdir = Some("/Users/pluto/projects/agent-run".to_string());
        app.apply_sessions(
            &serde_json::from_value(serde_json::json!({
                "items": [agent],
                "total": 1, "offset": 0, "limit": 200,
                "next_offset": None::<usize>, "complete": true,
                "revision": 1, "observed_at": 1.0,
            }))
            .unwrap(),
        );
        app.watch_selected();
        let screen = render_to_string(&app, 120, 24);
        // Row two carries the model and the workdir basename.
        assert!(
            screen.contains("gpt-5 · agent-run"),
            "model and workdir basename: {screen}"
        );
        // The transcript header shows the full workdir, home shortened.
        assert!(
            screen.contains("~/projects/agent-run"),
            "shortened workdir in the transcript pane: {screen}"
        );

        // Finished rows carry the agent hash tail instead of idle silence.
        app.sessions[0].status = agent_run_domain::domain::Status::Succeeded;
        app.completed_open = true;
        app.watch_selected();
        let finished = render_to_string(&app, 120, 24);
        assert!(
            finished.contains("aaaaaaaa"),
            "hash tail on finished rows: {finished}"
        );
    }

    #[test]
    fn workdir_falls_back_to_task_text_for_old_brokers() {
        let mut app = App::new();
        app.home_prefix = Some("/Users/pluto".to_string());
        let mut agent = agent_view(STABLE, RUN, "running");
        agent.task_summary =
            "Role: implementer on Agent IDE. Workdir: worktree /Users/pluto/projects/agent-ide/.claude/worktrees/m001".to_string();
        app.apply_sessions(
            &serde_json::from_value(serde_json::json!({
                "items": [agent],
                "total": 1, "offset": 0, "limit": 200,
                "next_offset": None::<usize>, "complete": true,
                "revision": 1, "observed_at": 1.0,
            }))
            .unwrap(),
        );
        app.watch_selected();
        let screen = render_to_string(&app, 140, 24);
        assert!(
            screen.contains("~/projects/agent-ide/.claude/worktrees/m001"),
            "task-text fallback: {screen}"
        );
    }

    #[test]
    fn project_picker_lists_projects_and_marks_cursor() {
        let mut app = App::new();
        app.home_prefix = Some("/Users/pluto".into());
        let mut one = agent_view(STABLE, RUN, "running");
        one.workdir = Some("/Users/pluto/projects/agent-ide/.claude/worktrees/m001".into());
        let mut two = agent_view(
            "ag-20260928-101501-cccccccccc",
            "ag-20260928-101501-dddddddddd",
            "succeeded",
        );
        two.workdir = Some("/Users/pluto/projects/agent-ide".into());
        app.sessions = vec![one, two];

        app.open_project_picker();
        app.move_picker_cursor(1);
        let screen = render_to_string(&app, 120, 24);
        assert!(screen.contains("projects"), "title: {screen}");
        assert!(screen.contains("agent-ide/"), "project name: {screen}");
        assert!(screen.contains("1 live, 1 finished"), "counts: {screen}");
        assert!(
            screen.contains("~/projects/agent-ide"),
            "shortened root: {screen}"
        );
        assert!(screen.contains("all projects"), "reset entry: {screen}");
        assert!(screen.contains("› "), "cursor marker: {screen}");
        assert!(screen.contains("esc close"), "overlay hint: {screen}");
        assert!(screen.contains("╭"), "rounded border: {screen}");
    }

    #[test]
    fn help_overlay_lists_real_keys_and_closes_on_esc() {
        let mut app = loaded_app();
        app.help = true;
        let screen = render_to_string(&app, 120, 24);
        assert!(screen.contains("keys"), "overlay title: {screen}");
        assert!(screen.contains("agents"), "section: {screen}");
        assert!(screen.contains("session"), "section: {screen}");
        assert!(screen.contains("follow live output"), "hint: {screen}");
        assert!(screen.contains("esc close"), "hint: {screen}");
    }

    #[test]
    fn sessions_empty_state_waits_for_broker() {
        let app = App::new();
        let screen = render_to_string(&app, 80, 12);
        assert!(
            screen.contains("Waiting for the resident broker"),
            "{screen}"
        );
        // The finished-section header renders even before anything loaded:
        // it is how the operator widens the scope.
        assert!(screen.contains("▸ FINISHED"), "header: {screen}");
        assert!(
            !screen.contains("FINISHED  0"),
            "count hidden while unknown: {screen}"
        );
    }

    #[test]
    fn finished_header_shows_without_a_known_count_and_stays_clickable() {
        let mut app = App::new();
        // Active-only scope: the broker delivered a page with zero sessions,
        // so no finished rows were ever fetched and the count is unknown.
        app.apply_sessions(
            &serde_json::from_value(serde_json::json!({
                "items": [],
                "total": 0, "offset": 0, "limit": 200,
                "next_offset": None::<usize>, "complete": true,
                "revision": 3, "observed_at": 1.0,
            }))
            .unwrap(),
        );
        let screen = render_to_string(&app, 100, 20);
        assert!(
            screen.contains("▸ FINISHED"),
            "header always visible: {screen}"
        );
        assert!(
            !screen.contains("FINISHED  0"),
            "no count while the wide scope is unloaded: {screen}"
        );
        assert!(screen.contains("No sessions in this scope."), "{screen}");
        assert!(
            !screen.contains("Tab expands"),
            "the header replaces the tab hint: {screen}"
        );

        // The header row sits below the two pinned rows (no live cards) and
        // stays hit-testable, toggling the scope.
        let list = ratatui::layout::Rect {
            x: 1,
            y: 2,
            width: 98,
            height: 17,
        };
        assert!(crate::ui::list::finished_header_clicked(&app, list, 2 + 2));
        app.toggle_completed();
        assert!(app.completed_open);
        // Once the wide scope is loaded the count appears.
        app.sessions = vec![agent_view(
            "ag-20260928-101501-cccccccccc",
            "ag-20260928-101501-dddddddddd",
            "failed",
        )];
        let wide = render_to_string(&app, 100, 24);
        assert!(wide.contains("▾ FINISHED  1"), "count once known: {wide}");
    }

    /// The active-only startup scope with finished sessions counted by the
    /// broker: the collapsed header shows the total, the empty state belongs
    /// to the live section alone, and the app bar knows the count.
    #[test]
    fn finished_total_renders_without_loading_finished_rows() {
        let mut app = App::new();
        app.apply_sessions(
            &serde_json::from_value(serde_json::json!({
                "items": [],
                "total": 0, "offset": 0, "limit": 200,
                "next_offset": None::<usize>, "complete": true,
                "revision": 3, "observed_at": 1.0,
            }))
            .unwrap(),
        );
        app.apply_finished_total(103);
        let screen = render_to_string(&app, 100, 20);
        assert!(
            screen.contains("▸ FINISHED  103"),
            "collapsed header carries the broker total: {screen}"
        );
        assert!(screen.contains("✓ 103 finished"), "app bar: {screen}");
        assert!(screen.contains("no live sessions"), "{screen}");
        assert!(!screen.contains("No sessions in this scope."), "{screen}");
    }

    #[test]
    fn transcript_screen_renders_messages_and_follow_state() {
        let mut app = App::new();
        app.sessions = vec![agent_view(STABLE, RUN, "running")];
        let page: TranscriptPage = serde_json::from_value(serde_json::json!({
            "agent_id": STABLE, "run_id": RUN,
            "messages": [
                {"seq": 1, "at": 3600.0, "role": "user", "name": null, "content": "hello agent", "raw_ref": null},
                {"seq": 2, "at": 3660.0, "role": "assistant", "name": null, "content": "hi operator", "raw_ref": null},
            ],
            "cursor": 0, "limit": 500, "next_cursor": null, "complete": true,
        }))
        .unwrap();
        app.open_selected_transcript();
        app.apply_transcript(&AgentId::from_str(STABLE).unwrap(), page.clone());
        let screen = render_to_string(&app, 100, 24);
        assert!(screen.contains("› prompt"), "user marker: {screen}");
        assert!(screen.contains("hello agent"), "user message: {screen}");
        assert!(
            screen.contains("hi operator"),
            "assistant message: {screen}"
        );
        assert!(screen.contains("◆"), "assistant marker: {screen}");
        assert!(screen.contains("● follow"), "follow state: {screen}");
        assert!(screen.contains("silence 3s"), "silence readout: {screen}");
    }

    #[test]
    fn answer_popup_renders_sealed_metadata() {
        let mut app = App::new();
        app.apply_answer(
            serde_json::from_value(serde_json::json!({
                "agent_id": STABLE, "run_id": RUN,
                "status": "succeeded",
                "available": true,
                "path": "/tmp/answer.md",
                "size_bytes": 12,
                "sha256": "abc123",
                "content": "the answer",
                "inline_complete": true,
                "relative_path": null,
                "kind": null,
                "media_type": null,
                "proof_version": null,
            }))
            .unwrap(),
        );
        let screen = render_to_string(&app, 90, 20);
        assert!(screen.contains("sealed answer"), "popup title: {screen}");
        assert!(screen.contains("the answer"), "inline content: {screen}");
        assert!(screen.contains("abc123"), "digest: {screen}");
        assert!(screen.contains("/tmp/answer.md"), "path: {screen}");
    }

    #[test]
    fn tool_call_and_result_render_as_one_row_with_summary() {
        let mut app = App::new();
        app.sessions = vec![agent_view(STABLE, RUN, "running")];
        let page: TranscriptPage = serde_json::from_value(serde_json::json!({
            "agent_id": STABLE, "run_id": RUN,
            "messages": [
                {"seq": 1, "at": 100.0, "role": "tool_call", "name": "Bash",
                 "content": "{\"command\":\"cargo test\",\"description\":\"Run the suite\",\"timeout\":300000}",
                 "raw_ref": "fixture-tool-1"},
                {"seq": 2, "at": 130.0, "role": "tool_result", "name": "Bash",
                 "content": "first line\nsecond line\nthird line", "raw_ref": "fixture-tool-1"},
            ],
            "cursor": 0, "limit": 500, "next_cursor": null, "complete": true,
        }))
        .unwrap();
        app.open_selected_transcript();
        app.apply_transcript(&AgentId::from_str(STABLE).unwrap(), page.clone());

        let collapsed = render_to_string(&app, 100, 24);
        // One row carries the tool name, its argument, the result summary,
        // and the duration computed from the message timestamps.
        let row = collapsed
            .lines()
            .find(|l| l.contains("Bash"))
            .expect("tool row");
        assert!(row.contains("cargo test"), "argument: {row:?}");
        assert!(row.contains("first line"), "result summary: {row:?}");
        assert!(row.contains("30s"), "duration: {row:?}");
        assert!(
            !collapsed.contains("timeout"),
            "json hidden while collapsed: {collapsed}"
        );
        assert!(
            !collapsed.contains("second line"),
            "payload hidden while collapsed: {collapsed}"
        );

        // Enter on the cursor expands the arguments and payload below.
        app.transcript.as_mut().unwrap().toggle_expanded_at_cursor();
        let expanded = render_to_string(&app, 100, 30);
        assert!(
            expanded.contains("command   cargo test"),
            "argument row: {expanded}"
        );
        assert!(expanded.contains("┄"), "separator: {expanded}");
        assert!(expanded.contains("second line"), "payload: {expanded}");
        assert!(expanded.contains("300000"), "argument value: {expanded}");
    }

    #[test]
    fn live_tool_without_result_shows_running_state() {
        let mut app = App::new();
        app.sessions = vec![agent_view(STABLE, RUN, "running")];
        let page: TranscriptPage = serde_json::from_value(serde_json::json!({
            "agent_id": STABLE, "run_id": RUN,
            "messages": [
                {"seq": 1, "at": 100.0, "role": "tool_call", "name": "Bash",
                 "content": "{\"command\":\"/bin/sleep 300\"}", "raw_ref": null},
            ],
            "cursor": 0, "limit": 500, "next_cursor": null, "complete": true,
        }))
        .unwrap();
        app.open_selected_transcript();
        app.apply_transcript(&AgentId::from_str(STABLE).unwrap(), page.clone());
        let screen = render_to_string(&app, 100, 24);
        let row = screen.lines().find(|l| l.contains("Bash")).unwrap();
        assert!(row.contains("running"), "running note: {row:?}");
    }

    #[test]
    fn finished_sessions_end_with_an_outcome_line() {
        let mut app = App::new();
        let mut agent = agent_view(STABLE, RUN, "succeeded");
        agent.failure_text = None;
        app.sessions = vec![agent];
        app.completed_open = true; // finished sessions must be in scope
        let page: TranscriptPage = serde_json::from_value(serde_json::json!({
            "agent_id": STABLE, "run_id": RUN,
            "messages": [
                {"seq": 1, "at": 100.0, "role": "assistant", "name": null,
                 "content": "DONE", "raw_ref": null},
            ],
            "cursor": 0, "limit": 500, "next_cursor": null, "complete": true,
        }))
        .unwrap();
        app.open_selected_transcript();
        app.apply_transcript(&AgentId::from_str(STABLE).unwrap(), page.clone());
        let screen = render_to_string(&app, 100, 24);
        assert!(
            screen.contains("✓ finished in 42s"),
            "outcome line: {screen}"
        );

        app.sessions[0].status = agent_run_domain::domain::Status::Failed;
        app.sessions[0].failure_text = Some("Bash exit 101".into());
        // The refreshed session view reaches the pane through the reducer
        // path (a fresh sessions page), which also invalidates the cache.
        app.apply_sessions(
            &serde_json::from_value(serde_json::json!({
                "items": [app.sessions[0].clone()],
                "total": 1, "offset": 0, "limit": 200,
                "next_offset": None::<usize>, "complete": true,
                "revision": 2, "observed_at": 2.0,
            }))
            .unwrap(),
        );
        let failed = render_to_string(&app, 100, 24);
        assert!(
            failed.contains("✗ failed · Bash exit 101"),
            "failure line: {failed}"
        );
    }

    #[test]
    fn user_messages_collapse_beyond_five_lines() {
        let mut app = App::new();
        app.sessions = vec![agent_view(STABLE, RUN, "running")];
        let long = (0..60)
            .map(|n| format!("line {n} of the prompt"))
            .collect::<Vec<_>>()
            .join(" ");
        let page: TranscriptPage = serde_json::from_value(serde_json::json!({
            "agent_id": STABLE, "run_id": RUN,
            "messages": [
                {"seq": 1, "at": 100.0, "role": "user", "name": null,
                 "content": long, "raw_ref": null},
            ],
            "cursor": 0, "limit": 500, "next_cursor": null, "complete": true,
        }))
        .unwrap();
        app.open_selected_transcript();
        app.apply_transcript(&AgentId::from_str(STABLE).unwrap(), page.clone());
        let screen = render_to_string(&app, 100, 24);
        assert!(screen.contains("more lines"), "collapse hint: {screen}");
        assert!(
            !screen.contains("line 30 of the prompt"),
            "tail hidden while collapsed: {screen}"
        );
        app.transcript.as_mut().unwrap().toggle_expanded_at_cursor();
        let expanded = render_to_string(&app, 100, 60);
        assert!(
            expanded.contains("line 30 of the prompt"),
            "full text after expansion: {expanded}"
        );
    }

    #[test]
    fn streamed_fragments_coalesce_into_one_flowing_block() {
        let mut app = App::new();
        app.sessions = vec![agent_view(STABLE, RUN, "running")];
        // Engines stream text in small deltas; the supervisor journals each
        // delta as its own assistant message (agent-run-core stream.rs).
        let page: TranscriptPage = serde_json::from_value(serde_json::json!({
            "agent_id": STABLE, "run_id": RUN,
            "messages": [
                {"seq": 1, "at": 100.0, "role": "assistant", "name": null, "content": "The me", "raw_ref": "stream"},
                {"seq": 2, "at": 100.0, "role": "assistant", "name": null, "content": "asurem", "raw_ref": "stream"},
                {"seq": 3, "at": 100.0, "role": "assistant", "name": null, "content": "ent is decisive", "raw_ref": "stream"},
            ],
            "cursor": 0, "limit": 500, "next_cursor": null, "complete": true,
        }))
        .unwrap();
        app.open_selected_transcript();
        app.apply_transcript(&AgentId::from_str(STABLE).unwrap(), page.clone());
        let buffer = app.transcript.as_ref().unwrap();

        // One text block spanning all three fragments.
        let blocks = crate::ui::transcript::blocks(buffer);
        assert_eq!(blocks.len(), 1);
        assert_eq!(blocks[0].start, 0);
        assert_eq!(blocks[0].len, 3);

        let screen = render_to_string(&app, 100, 24);
        // The fragments flow together behind one marker.
        assert!(screen.contains("The measurement is decisive"), "{screen}");
        assert_eq!(
            screen.matches("◆").count(),
            1,
            "one marker, not per fragment: {screen}"
        );
    }

    #[test]
    fn tool_activity_breaks_the_stream() {
        let mut app = App::new();
        app.sessions = vec![agent_view(STABLE, RUN, "running")];
        let page: TranscriptPage = serde_json::from_value(serde_json::json!({
            "agent_id": STABLE, "run_id": RUN,
            "messages": [
                {"seq": 1, "at": 100.0, "role": "assistant", "name": null, "content": "before", "raw_ref": null},
                {"seq": 2, "at": 101.0, "role": "tool_call", "name": "Bash", "content": "{\"command\":\"ls\"}", "raw_ref": null},
                {"seq": 3, "at": 102.0, "role": "assistant", "name": null, "content": "after", "raw_ref": null},
            ],
            "cursor": 0, "limit": 500, "next_cursor": null, "complete": true,
        }))
        .unwrap();
        app.open_selected_transcript();
        app.apply_transcript(&AgentId::from_str(STABLE).unwrap(), page.clone());
        let blocks = crate::ui::transcript::blocks(app.transcript.as_ref().unwrap());
        assert_eq!(blocks.len(), 3, "tool call breaks the text stream");
        assert!(blocks.iter().map(|b| b.text).eq([true, false, true]));
    }

    #[test]
    fn separators_sit_between_blocks_not_inside_tool_groups() {
        let mut app = App::new();
        app.sessions = vec![agent_view(STABLE, RUN, "running")];
        let page: TranscriptPage = serde_json::from_value(serde_json::json!({
            "agent_id": STABLE, "run_id": RUN,
            "messages": [
                {"seq": 1, "at": 100.0, "role": "tool_call", "name": "Bash",
                 "content": "{\"command\":\"ls\"}", "raw_ref": "fixture-tool-2"},
                {"seq": 2, "at": 101.0, "role": "tool_result", "name": "Bash",
                 "content": "ok", "raw_ref": "fixture-tool-2"},
                message(3, "assistant", "done"),
            ],
            "cursor": 0, "limit": 500, "next_cursor": null, "complete": true,
        }))
        .unwrap();
        app.open_selected_transcript();
        app.apply_transcript(&AgentId::from_str(STABLE).unwrap(), page.clone());
        let buffer = app.transcript.as_ref().unwrap();
        // The call and its result form one compact row; the text block
        // follows after exactly one separator row.
        let layouts = crate::ui::transcript::layout(buffer, 80);
        assert_eq!(layouts.len(), 2);
        assert_eq!(layouts[0].line_count, 1, "call and result share one row");
        assert_eq!(layouts[0].line_start, 0);
        assert_eq!(layouts[1].line_start, 2, "one separator row between blocks");
        assert_eq!(layouts[1].line_count, 1);
        // One trailing blank row below the last block.
        assert_eq!(
            crate::ui::transcript::total_height(buffer, 80),
            4,
            "row + separator + row + trailing blank"
        );
    }

    #[test]
    fn consecutive_tool_rows_stay_compact() {
        let mut app = App::new();
        app.sessions = vec![agent_view(STABLE, RUN, "running")];
        let page: TranscriptPage = serde_json::from_value(serde_json::json!({
            "agent_id": STABLE, "run_id": RUN,
            "messages": [
                {"seq": 1, "at": 100.0, "role": "tool_call", "name": "Glob",
                 "content": "{\"pattern\":\"src/*\"}", "raw_ref": "fixture-tool-3"},
                {"seq": 2, "at": 101.0, "role": "tool_result", "name": "Glob",
                 "content": "20 files", "raw_ref": "fixture-tool-3"},
                {"seq": 3, "at": 102.0, "role": "tool_call", "name": "Read",
                 "content": "{\"file_path\":\"src/main.rs\"}", "raw_ref": "fixture-tool-4"},
                {"seq": 4, "at": 103.0, "role": "tool_result", "name": "Read",
                 "content": "80 lines", "raw_ref": "fixture-tool-4"},
            ],
            "cursor": 0, "limit": 500, "next_cursor": null, "complete": true,
        }))
        .unwrap();
        app.open_selected_transcript();
        app.apply_transcript(&AgentId::from_str(STABLE).unwrap(), page.clone());
        let buffer = app.transcript.as_ref().unwrap();
        let layouts = crate::ui::transcript::layout(buffer, 80);
        assert_eq!(layouts.len(), 2);
        assert_eq!(
            layouts[1].line_start, 1,
            "no blank row between collapsed tool rows"
        );
        let screen = render_to_string(&app, 100, 24);
        let glob_row = screen.lines().find(|l| l.contains("Glob")).unwrap();
        let read_row = screen.lines().find(|l| l.contains("Read")).unwrap();
        let glob_y = screen.lines().position(|l| l == glob_row).unwrap();
        let read_y = screen.lines().position(|l| l == read_row).unwrap();
        assert_eq!(read_y, glob_y + 1, "tool rows stack tightly: {screen}");
    }

    #[test]
    fn hover_highlights_the_message_under_the_pointer() {
        use ratatui::style::Color;

        let mut app = App::new();
        app.sessions = vec![agent_view(STABLE, RUN, "running")];
        let page: TranscriptPage = serde_json::from_value(serde_json::json!({
            "agent_id": STABLE, "run_id": RUN,
            "messages": [
                message(1, "assistant", "hover me not"),
                {"seq": 2, "at": 101.0, "role": "tool_call", "name": "Bash",
                 "content": "{\"command\":\"ls\"}", "raw_ref": null},
            ],
            "cursor": 0, "limit": 500, "next_cursor": null, "complete": true,
        }))
        .unwrap();
        app.open_selected_transcript();
        app.apply_transcript(&AgentId::from_str(STABLE).unwrap(), page.clone());
        // The cursor owns its own highlight; hover targets another message.
        app.transcript.as_mut().unwrap().hover = Some(1);
        force_truecolor();

        let mut terminal = Terminal::new(TestBackend::new(100, 20)).unwrap();
        terminal
            .draw(|frame| crate::ui::render(frame, &app))
            .unwrap();
        let buffer = terminal.backend().buffer();
        // The body starts four header rows below the pane top (row 2); the
        // first block carries the cursor highlight, the tool block two rows
        // later (one separator between them) carries the hover highlight.
        assert_eq!(buffer[(1, 6)].bg, Color::Rgb(29, 34, 43), "selection bg");
        assert_eq!(buffer[(1, 8)].bg, Color::Rgb(23, 27, 34), "hover bg");

        // Without hover the same cell renders without the highlight.
        app.transcript.as_mut().unwrap().hover = None;
        let mut terminal = Terminal::new(TestBackend::new(100, 20)).unwrap();
        terminal
            .draw(|frame| crate::ui::render(frame, &app))
            .unwrap();
        let buffer = terminal.backend().buffer();
        assert_eq!(buffer[(1, 8)].bg, crate::ui::theme::palette().bg);
    }

    #[test]
    fn reasoning_renders_as_plain_text() {
        let mut app = App::new();
        app.sessions = vec![agent_view(STABLE, RUN, "running")];
        let page: TranscriptPage = serde_json::from_value(serde_json::json!({
            "agent_id": STABLE, "run_id": RUN,
            "messages": [
                message(1, "assistant", "The measurement is decisive here: 40 warning lines dropped oversize events and the full gate must run clean before any commit lands."),
            ],
            "cursor": 0, "limit": 500, "next_cursor": null, "complete": true,
        }))
        .unwrap();
        app.open_selected_transcript();
        app.apply_transcript(&AgentId::from_str(STABLE).unwrap(), page.clone());
        let screen = render_to_string(&app, 100, 24);
        assert!(
            screen.contains("The measurement is decisive here: 40 warning"),
            "wrapped first line: {screen}"
        );
        assert!(
            screen.contains("full gate must run clean before any commit lands."),
            "wrapped second line: {screen}"
        );
    }

    #[test]
    fn message_fixture_roles_render_distinctly() {
        let mut app = App::new();
        app.sessions = vec![agent_view(STABLE, RUN, "running")];
        let page: TranscriptPage = serde_json::from_value(serde_json::json!({
            "agent_id": STABLE, "run_id": RUN,
            "messages": [
                message(1, "system", "reading files"),
                message(2, "assistant", "done"),
            ],
            "cursor": 0, "limit": 500, "next_cursor": null, "complete": true,
        }))
        .unwrap();
        app.open_selected_transcript();
        app.apply_transcript(&AgentId::from_str(STABLE).unwrap(), page.clone());
        let screen = render_to_string(&app, 100, 24);
        assert!(screen.contains("reading files"), "system content: {screen}");
        assert!(screen.contains("done"), "assistant content: {screen}");
    }

    /// An app whose open transcript overflows the viewport at 100x24.
    fn scrolling_app(complete: bool) -> App {
        let mut app = App::new();
        app.sessions = vec![agent_view(STABLE, RUN, "running")];
        let messages: Vec<_> = (0..40)
            .map(|n| serde_json::json!({
                "seq": n + 1, "at": 100.0 + n as f64, "role": "assistant", "name": null,
                "content": format!(
                    "message {n} of the transcript, padded long enough that                      the wrapped body overflows the viewport"
                ),
                "raw_ref": null,
            }))
            .collect();
        let page: TranscriptPage = serde_json::from_value(serde_json::json!({
            "agent_id": STABLE, "run_id": RUN,
            "messages": messages,
            "cursor": 0, "limit": 500,
            "next_cursor": if complete { None::<i64> } else { Some(40) },
            "complete": complete,
        }))
        .unwrap();
        app.open_selected_transcript();
        app.apply_transcript(&AgentId::from_str(STABLE).unwrap(), page.clone());
        app
    }

    /// Renders one frame and reports where the scrollbar thumb sits within
    /// the body track (row offsets from the track top).
    fn thumb_rows(app: &App, width: u16, height: u16) -> Vec<u16> {
        force_truecolor();
        let mut terminal = Terminal::new(TestBackend::new(width, height)).unwrap();
        terminal
            .draw(|frame| crate::ui::render(frame, app))
            .unwrap();
        let buffer = terminal.backend().buffer();
        // The track is the last column of the body (the pane keeps one
        // trailing column of frame background): rows 6..height-1 at 100x24,
        // column width-2.
        (6..height - 1)
            .filter(|&y| buffer[(width - 2, y)].symbol() == "┃")
            .collect()
    }

    #[test]
    fn scrollbar_thumb_reaches_both_ends_of_the_track() {
        // At the tail (follow mode) the thumb sits on the last track row.
        let app = scrolling_app(true);
        let thumbs = thumb_rows(&app, 100, 24);
        assert!(!thumbs.is_empty(), "a thumb renders");
        assert_eq!(*thumbs.last().unwrap(), 22, "thumb at the track bottom");

        // Scrolled to the top, the thumb sits on the first track row.
        let mut top = scrolling_app(true);
        top.transcript.as_mut().unwrap().scroll_top();
        let thumbs = thumb_rows(&top, 100, 24);
        assert_eq!(*thumbs.first().unwrap(), 6, "thumb at the track top");
    }

    #[test]
    fn transcript_header_shows_incompleteness_and_errors() {
        // Incomplete history: the loading count shows and a terminal session
        // withholds its outcome line.
        let mut app = scrolling_app(false);
        app.sessions[0].status = agent_run_domain::domain::Status::Succeeded;
        app.transcript.as_mut().unwrap().agent = app.sessions[0].clone();
        let screen = render_to_string(&app, 100, 24);
        assert!(
            screen.contains("loading 40 messages…"),
            "loading count: {screen}"
        );
        assert!(
            !screen.contains("finished in"),
            "no outcome line while incomplete: {screen}"
        );

        // A failing page shows the retry state instead.
        app.transcript.as_mut().unwrap().last_page_error =
            Some("state database operation failed".into());
        let errored = render_to_string(&app, 100, 24);
        assert!(
            errored.contains("broker error, retrying · state database operation failed"),
            "error state: {errored}"
        );
        assert!(!errored.contains("loading 40 messages"), "{errored}");

        // Once complete the outcome line returns and the states clear.
        let mut done = scrolling_app(true);
        done.sessions[0].status = agent_run_domain::domain::Status::Succeeded;
        done.sessions[0].elapsed_seconds = 42.0;
        done.transcript.as_mut().unwrap().agent = done.sessions[0].clone();
        let finished = render_to_string(&done, 100, 24);
        assert!(
            finished.contains("finished in 42s"),
            "outcome line once complete: {finished}"
        );
        assert!(!finished.contains("loading"), "{finished}");
    }

    /// Builds an app holding a 5000-message transcript (streamed assistant
    /// deltas broken by tool calls every hundred messages).
    fn long_transcript_app() -> App {
        let mut app = loaded_app();
        let mut messages = Vec::new();
        let mut seq = 0i64;
        for round in 0..50 {
            seq += 1;
            messages.push(serde_json::json!({
                "seq": seq, "at": 100.0 + seq as f64, "role": "tool_call", "name": "Bash",
                "content": "{\"command\":\"cargo test\"}", "raw_ref": format!("round-{round}"),
            }));
            seq += 1;
            messages.push(serde_json::json!({
                "seq": seq, "at": 100.0 + seq as f64, "role": "tool_result", "name": "Bash",
                "content": "test result: ok. 12 passed", "raw_ref": format!("round-{round}"),
            }));
            for _ in 0..98 {
                seq += 1;
                messages.push(serde_json::json!({
                    "seq": seq, "at": 100.0 + seq as f64, "role": "assistant", "name": null,
                    "content": format!("delta {round} "), "raw_ref": format!("round-{round}"),
                }));
            }
        }
        let page: TranscriptPage = serde_json::from_value(serde_json::json!({
            "agent_id": STABLE, "run_id": RUN,
            "messages": messages,
            "cursor": 0, "limit": 6000, "next_cursor": null, "complete": true,
        }))
        .unwrap();
        app.transcript = Some(crate::app::TranscriptBuffer::open(app.sessions[0].clone()));
        app.apply_transcript(&AgentId::from_str(STABLE).unwrap(), page.clone());
        app
    }

    /// Rough render-cost probe for long transcripts; run with
    /// `cargo test --release --locked -p agent-run-tui -- --ignored` and
    /// read the printed elapsed time.
    #[test]
    #[ignore = "timing probe"]
    fn render_timing_5000_messages() {
        let app = long_transcript_app();
        render_to_string(&app, 160, 48); // warm caches once
        let start = std::time::Instant::now();
        for _ in 0..100 {
            render_to_string(&app, 160, 48);
        }
        println!(
            "100 frames of a 5000-message transcript at 160x48: {:?}",
            start.elapsed()
        );
    }

    /// Realistic update-cost probe: a live 4000-message transcript (~8 MB:
    /// streamed deltas, tool calls, and 500 results of 16 KiB) measured
    /// across the five scenarios the owner reported as laggy, plus a real
    /// payload expand of one call head (5b). Run with
    /// `cargo test --release --locked -p agent-run-tui -- --ignored
    /// --nocapture`.
    #[test]
    #[ignore = "timing probe"]
    fn update_cost_probe_4000_messages() {
        let (messages, seq) = crate::tests_support::probe_messages();
        let page = |msgs: serde_json::Value, complete: bool| -> TranscriptPage {
            serde_json::from_value(serde_json::json!({
                "agent_id": STABLE, "run_id": RUN,
                "messages": msgs,
                "cursor": 0, "limit": 500,
                "next_cursor": if complete { None::<i64> } else { Some(500) },
                "complete": complete,
            }))
            .unwrap()
        };

        let mut app = loaded_app();
        app.transcript = Some(crate::app::TranscriptBuffer::open(app.sessions[0].clone()));
        let agent_id = AgentId::from_str(STABLE).unwrap();

        // Block-row builds; the probe asserts the incremental invariants.
        let builds_before_backfill =
            crate::ui::transcript::cache_block_builds(app.transcript.as_ref().unwrap());

        // (1) Full backfill: 8 pages of 500, one frame per page.
        let start = std::time::Instant::now();
        for chunk in messages.chunks(500) {
            let value = serde_json::Value::Array(chunk.to_vec());
            app.apply_transcript(&agent_id, page(value, false));
            render_to_string(&app, 160, 48);
        }
        let backfill = start.elapsed();
        let builds_after_backfill =
            crate::ui::transcript::cache_block_builds(app.transcript.as_ref().unwrap());
        let backfill_builds = builds_after_backfill - builds_before_backfill;
        assert!(
            (1000..=1008).contains(&backfill_builds),
            "backfill is linear: 1000 blocks, each built once, plus at most one              rebuild of the block a page boundary cuts ({backfill_builds})"
        );

        // (2) 200 AgentView refreshes (silence/elapsed ticking), one frame each.
        let start = std::time::Instant::now();
        for step in 0..200 {
            let mut fresh = app.sessions[0].clone();
            fresh.silence_seconds = Some(0.5 + step as f64);
            fresh.elapsed_seconds = 42.0 + step as f64;
            app.sessions[0] = fresh;
            let buffer = app.transcript.as_mut().unwrap();
            buffer.agent = app.sessions[0].clone();
            render_to_string(&app, 160, 48);
        }
        let refreshes = start.elapsed();
        assert_eq!(
            crate::ui::transcript::cache_block_builds(app.transcript.as_ref().unwrap()),
            builds_after_backfill,
            "view refreshes rebuild no blocks"
        );

        // (3) 200 appended single deltas, one frame each.
        let start = std::time::Instant::now();
        let mut tail = seq;
        for step in 0..200 {
            tail += 1;
            let delta = serde_json::json!([{
                "seq": tail, "at": 5000.0 + step as f64, "role": "assistant", "name": null,
                "content": "tail delta ", "raw_ref": null,
            }]);
            app.apply_transcript(&agent_id, page(delta, false));
            render_to_string(&app, 160, 48);
        }
        let appends = start.elapsed();
        assert_eq!(
            crate::ui::transcript::cache_block_builds(app.transcript.as_ref().unwrap()),
            builds_after_backfill + 200,
            "each appended delta rebuilds exactly one block"
        );

        // (4) 200 frames with no change (ticks).
        let start = std::time::Instant::now();
        for _ in 0..200 {
            render_to_string(&app, 160, 48);
        }
        let idle = start.elapsed();
        let builds_after_idle =
            crate::ui::transcript::cache_block_builds(app.transcript.as_ref().unwrap());
        assert_eq!(
            builds_after_idle,
            builds_after_backfill + 200,
            "idle frames rebuild no blocks"
        );

        // (5) 100 expand/collapse toggles of one mid-transcript block.
        let start = std::time::Instant::now();
        for _ in 0..100 {
            app.transcript.as_mut().unwrap().toggle_expanded(2);
            render_to_string(&app, 160, 48);
        }
        let toggles = start.elapsed();
        assert_eq!(
            crate::ui::transcript::cache_block_builds(app.transcript.as_ref().unwrap()),
            builds_after_idle + 100,
            "each toggle rebuilds one block"
        );

        // (5b) 100 toggles of the call head: a real expand/collapse of the
        // 16 KiB payload (50 expanded builds, 50 collapsed).
        let start = std::time::Instant::now();
        for _ in 0..100 {
            app.transcript.as_mut().unwrap().toggle_expanded(1);
            render_to_string(&app, 160, 48);
        }
        let head_toggles = start.elapsed();

        println!(
            "update-cost probe (4000 msgs, ~8 MB, 160x48):\n  (1) backfill 8x500 + 8 frames: {backfill:?}\n  (2) 200 view refreshes + frames: {refreshes:?}\n  (3) 200 appended deltas + frames: {appends:?}\n  (4) 200 idle frames: {idle:?}\n  (5) 100 expand toggles + frames: {toggles:?}\n  (5b) 100 head expand/collapse + frames: {head_toggles:?}"
        );
    }

    /// Prints rendered frames for the documentation when `TUI_DUMP` is set;
    /// always asserts the frames carry both pane families.
    #[test]
    fn frame_dumps_for_documentation() {
        let mut app = loaded_app();
        app.sessions[0].task_summary = "Finish the audit tail before edits".to_string();
        app.sessions[0].model = "glm-5.3".to_string();
        app.home_prefix = Some("/Users/pluto".to_string());
        app.sessions[0].workdir = Some("/Users/pluto/projects/agent-run".to_string());
        let page: TranscriptPage = serde_json::from_value(serde_json::json!({
            "agent_id": STABLE, "run_id": RUN,
            "messages": [
                {"seq": 1, "at": 3600.0, "role": "user", "name": null,
                 "content": "Finish the audit tail before edits: confirm where retry_limit is defined and align the scheduler with the documented default.", "raw_ref": null},
                {"seq": 2, "at": 3660.0, "role": "tool_call", "name": "Grep",
                 "content": "{\"pattern\":\"retry_limit\"}", "raw_ref": "fixture-tool-5"},
                {"seq": 3, "at": 3661.0, "role": "tool_result", "name": "Grep",
                 "content": "3 matches", "raw_ref": "fixture-tool-5"},
                {"seq": 4, "at": 3700.0, "role": "assistant", "name": null,
                 "content": "docs/scheduler.md:31 documents a default of 3, while defaults.rs:22 uses 5.", "raw_ref": null},
            ],
            "cursor": 0, "limit": 500, "next_cursor": null, "complete": true,
        }))
        .unwrap();
        app.transcript = Some(crate::app::TranscriptBuffer::open(app.sessions[0].clone()));
        app.apply_transcript(&AgentId::from_str(STABLE).unwrap(), page.clone());

        let wide = render_to_string(&app, 160, 48);
        let narrow = render_to_string(&app, 66, 52);
        assert!(
            wide.contains("LIVE") && wide.contains("◆"),
            "wide frame: {wide}"
        );
        assert!(narrow.contains("LIVE"), "narrow frame: {narrow}");
        if std::env::var_os("TUI_DUMP").is_some() {
            println!("--- 160x48 ---\n{wide}\n--- 66x52 ---\n{narrow}");
        }
    }
}
