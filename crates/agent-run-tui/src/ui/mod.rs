//! Root rendering: screen routing, the status bar, and popup overlays.

pub mod answer;
pub mod list;
pub mod theme;
pub mod transcript;

use crate::app::{App, Link};
use ratatui::{
    layout::{Constraint, Layout},
    text::{Line, Span},
    widgets::Paragraph,
    Frame,
};

/// Renders one full frame from the current application state.
pub fn render(f: &mut Frame, app: &App) {
    let area = f.area();
    let [main, status] = Layout::vertical([Constraint::Min(1), Constraint::Length(1)]).areas(area);

    match app.screen {
        crate::app::Screen::Sessions => {
            list::render(f, app, main);
            list::render_dropdown(f, app, main);
        }
        crate::app::Screen::Transcript => transcript::render(f, app, main),
    }
    answer::render(f, app, area);
    render_status_bar(f, app, status);
}

/// Renders the one-line broker status and key hints.
fn render_status_bar(f: &mut Frame, app: &App, area: ratatui::layout::Rect) {
    let (link_glyph, link_note) = match app.link {
        Link::Up => (
            Span::styled("●", ratatui::style::Style::new().green()),
            "broker",
        ),
        Link::Down => (
            Span::styled("○", ratatui::style::Style::new().red()),
            "retrying",
        ),
    };
    let status_text = format!(
        "{link_note} rev {} sessions {} ",
        app.revision.unwrap_or(0),
        app.sessions.len()
    );
    let [hints_area, status_area] = Layout::horizontal([
        Constraint::Min(10),
        Constraint::Length(status_text.chars().count() as u16 + 2),
    ])
    .areas(area);

    let hints = match app.screen {
        crate::app::Screen::Sessions => {
            "↑↓ select ⏎ open tab finished o toggle a answer r refresh q quit"
        }
        crate::app::Screen::Transcript => {
            "↑↓ message ⏎ expand tool f follow g/G a answer esc back q quit"
        }
    };
    f.render_widget(
        Paragraph::new(Line::from(vec![
            Span::styled(" ", theme::dim()),
            Span::styled(hints, theme::dim()),
        ])),
        hints_area,
    );

    let mut status_line = vec![
        Span::raw(" "),
        link_glyph,
        Span::raw(format!(" {status_text}")),
    ];
    if let Some(error) = &app.last_error {
        status_line.push(Span::styled(
            error.clone(),
            ratatui::style::Style::new().red(),
        ));
        status_line.push(Span::raw(" "));
    }
    f.render_widget(
        Paragraph::new(Line::from(status_line)).right_aligned(),
        status_area,
    );
}

#[cfg(test)]
mod tests {
    use crate::app::App;
    use crate::tests_support::{agent_view, message};
    use agent_run_domain::domain::AgentId;
    use agent_run_domain::views::TranscriptPage;
    use ratatui::{backend::TestBackend, Terminal};
    use std::str::FromStr;

    const STABLE: &str = "ag-20260928-101500-aaaaaaaaaa";
    const RUN: &str = "ag-20260928-101500-bbbbbbbbbb";

    /// Renders one frame on a fixed-size canvas and returns its text grid.
    fn render_to_string(app: &App, width: u16, height: u16) -> String {
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

    #[test]
    fn sessions_screen_renders_cards_hints_and_status() {
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
        let screen = render_to_string(&app, 120, 20);
        assert!(screen.contains("agent-run"), "title: {screen}");
        // Finished sessions stay behind the collapsed dropdown.
        assert!(screen.contains("▸ finished (1)"), "dropdown: {screen}");
        assert!(
            !screen.contains("✗"),
            "finished card hidden while collapsed: {screen}"
        );
        // The live card shows pictogram, task, id, runtime/model, and time.
        assert!(screen.contains("●"), "running pictogram: {screen}");
        assert!(screen.contains("ship the thing"), "task: {screen}");
        assert!(screen.contains(&format!("{:.23}", STABLE)), "id: {screen}");
        assert!(screen.contains("CODEX/gpt-5"), "runtime/model: {screen}");
        assert!(screen.contains("42s"), "elapsed: {screen}");
        assert!(screen.contains("rev 5"), "revision: {screen}");

        // Expanding the dropdown reveals finished cards.
        app.toggle_completed();
        let expanded = render_to_string(&app, 120, 24);
        assert!(
            expanded.contains("▾ finished (1)"),
            "open dropdown: {expanded}"
        );
        assert!(expanded.contains("✗"), "finished card visible: {expanded}");
    }

    #[test]
    fn cards_show_workdir_shortened_and_padded() {
        let mut app = App::new();
        app.home_prefix = Some("/home/developer".to_string());
        let mut agent = agent_view(STABLE, RUN, "running");
        agent.workdir = Some("/home/developer/projects/agent-run".to_string());
        app.apply_sessions(
            &serde_json::from_value(serde_json::json!({
                "items": [agent],
                "total": 1, "offset": 0, "limit": 200,
                "next_offset": None::<usize>, "complete": true,
                "revision": 1, "observed_at": 1.0,
            }))
            .unwrap(),
        );
        let screen = render_to_string(&app, 120, 24);
        assert!(
            screen.contains("workdir ~/projects/agent-run"),
            "workdir line: {screen}"
        );
        // Padding: content rows start two columns inside the card, and the
        // rows above and below the content stay blank.
        let card_line = screen
            .lines()
            .find(|l| l.contains("ship the thing"))
            .unwrap();
        assert!(card_line.starts_with("│  "), "side padding: {card_line:?}");
        let card_lines: Vec<&str> = screen.lines().collect();
        let content_row = screen
            .lines()
            .position(|l| l.contains("ship the thing"))
            .unwrap();
        let empty_row = |row: &str| row.trim_matches(|c: char| c == '│' || c == ' ').is_empty();
        assert!(
            empty_row(card_lines[content_row - 1]) && empty_row(card_lines[content_row + 5]),
            "top and bottom padding rows: {screen}"
        );
    }

    #[test]
    fn workdir_falls_back_to_task_text_for_old_brokers() {
        let mut app = App::new();
        let mut agent = agent_view(STABLE, RUN, "running");
        agent.task_summary = "Role: implementer. Workdir: worktree /workspaces/example".to_string();
        app.apply_sessions(
            &serde_json::from_value(serde_json::json!({
                "items": [agent],
                "total": 1, "offset": 0, "limit": 200,
                "next_offset": None::<usize>, "complete": true,
                "revision": 1, "observed_at": 1.0,
            }))
            .unwrap(),
        );
        let screen = render_to_string(&app, 140, 24);
        assert!(
            screen.contains("workdir /workspaces/example"),
            "task-text fallback: {screen}"
        );
    }

    #[test]
    fn sessions_empty_state_waits_for_broker() {
        let app = App::new();
        let screen = render_to_string(&app, 80, 12);
        assert!(
            screen.contains("Waiting for the resident broker"),
            "{screen}"
        );
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
        app.apply_transcript(&AgentId::from_str(STABLE).unwrap(), &page);
        let screen = render_to_string(&app, 100, 24);
        assert!(screen.contains("transcript"), "title: {screen}");
        assert!(screen.contains(STABLE), "stable identity: {screen}");
        assert!(
            !screen.contains(&RUN[RUN.len() - 12..]),
            "internal run leaked: {screen}"
        );
        assert!(screen.contains("following"), "follow state: {screen}");
        assert!(screen.contains("hello agent"), "user message: {screen}");
        assert!(
            screen.contains("hi operator"),
            "assistant message: {screen}"
        );
        assert!(screen.contains("silent 3s"), "silence readout: {screen}");
    }

    #[test]
    fn answer_popup_renders_sealed_metadata() {
        let mut app = App::new();
        app.answer = Some(
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
    fn tool_calls_collapse_then_expand_to_pretty_json() {
        let mut app = App::new();
        app.sessions = vec![agent_view(STABLE, RUN, "running")];
        let page: TranscriptPage = serde_json::from_value(serde_json::json!({
            "agent_id": STABLE, "run_id": RUN,
            "messages": [
                {"seq": 1, "at": 100.0, "role": "tool_call", "name": "Bash",
                 "content": "{\"command\":\"cargo test\",\"description\":\"Run the suite\",\"timeout\":300000}",
                 "raw_ref": null},
                {"seq": 2, "at": 130.0, "role": "tool_result", "name": null,
                 "content": "first line\nsecond line\nthird line", "raw_ref": null},
            ],
            "cursor": 0, "limit": 500, "next_cursor": null, "complete": true,
        }))
        .unwrap();
        app.open_selected_transcript();
        app.apply_transcript(&AgentId::from_str(STABLE).unwrap(), &page);

        let collapsed = render_to_string(&app, 100, 24);
        assert!(
            collapsed.contains("▸ ⚙ Bash"),
            "collapsed call: {collapsed}"
        );
        assert!(
            collapsed.contains("Run the suite"),
            "call summary: {collapsed}"
        );
        assert!(collapsed.contains("▾ ↳"), "collapsed result: {collapsed}");
        assert!(collapsed.contains("(3 lines)"), "result size: {collapsed}");
        assert!(
            !collapsed.contains("timeout"),
            "json hidden while collapsed: {collapsed}"
        );

        // Enter on the cursor expands the payload under it.
        app.transcript.as_mut().unwrap().toggle_expanded_at_cursor();
        let expanded = render_to_string(&app, 100, 30);
        assert!(
            expanded.contains("expanded (Enter collapses)"),
            "header: {expanded}"
        );
        assert!(expanded.contains("\"command\""), "pretty key: {expanded}");
        assert!(expanded.contains("300000"), "pretty value: {expanded}");
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
                {"seq": 1, "at": 100.0, "role": "assistant", "name": null, "content": "The me", "raw_ref": null},
                {"seq": 2, "at": 100.0, "role": "assistant", "name": null, "content": "asurem", "raw_ref": null},
                {"seq": 3, "at": 100.0, "role": "assistant", "name": null, "content": "ent is decisive", "raw_ref": null},
            ],
            "cursor": 0, "limit": 500, "next_cursor": null, "complete": true,
        }))
        .unwrap();
        app.open_selected_transcript();
        app.apply_transcript(&AgentId::from_str(STABLE).unwrap(), &page);
        let buffer = app.transcript.as_ref().unwrap();

        // One text block spanning all three fragments.
        let blocks = crate::ui::transcript::blocks(buffer);
        assert_eq!(blocks.len(), 1);
        assert_eq!(blocks[0].start, 0);
        assert_eq!(blocks[0].len, 3);

        let screen = render_to_string(&app, 100, 24);
        // The fragments flow together on one line with a single role tag.
        assert!(
            screen.contains("assistant The measurement is decisive"),
            "{screen}"
        );
        assert_eq!(
            screen.matches("assistant").count(),
            1,
            "one tag, not per fragment: {screen}"
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
        app.apply_transcript(&AgentId::from_str(STABLE).unwrap(), &page);
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
                 "content": "{\"command\":\"ls\"}", "raw_ref": null},
                {"seq": 2, "at": 101.0, "role": "tool_result", "name": null,
                 "content": "ok", "raw_ref": null},
                message(3, "assistant", "done"),
            ],
            "cursor": 0, "limit": 500, "next_cursor": null, "complete": true,
        }))
        .unwrap();
        app.open_selected_transcript();
        app.apply_transcript(&AgentId::from_str(STABLE).unwrap(), &page);
        let buffer = app.transcript.as_ref().unwrap();
        // The call and its result form one compact block; the text block
        // follows after exactly one separator row.
        let layouts = crate::ui::transcript::layout(buffer, 80);
        assert_eq!(layouts.len(), 2);
        assert_eq!(layouts[0].line_count, 2, "call + result, no inner gap");
        assert_eq!(layouts[0].line_start, 0);
        assert_eq!(layouts[1].line_start, 3, "one separator row between blocks");
        assert_eq!(layouts[1].line_count, 1);
        assert_eq!(
            crate::ui::transcript::total_height(buffer, 80),
            4,
            "two blocks + one separator"
        );
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
        app.apply_transcript(&AgentId::from_str(STABLE).unwrap(), &page);
        // The cursor owns its own highlight; hover targets another message.
        app.transcript.as_mut().unwrap().hover = Some(1);

        let mut terminal = Terminal::new(TestBackend::new(100, 20)).unwrap();
        terminal
            .draw(|frame| crate::ui::render(frame, &app))
            .unwrap();
        let buffer = terminal.backend().buffer();
        // Message one starts two rows into the frame (border + header) and
        // carries the cursor highlight; message two starts two rows lower
        // and carries the hover highlight.
        assert_eq!(buffer[(1, 2)].bg, Color::Rgb(40, 48, 64));
        assert_eq!(buffer[(1, 4)].bg, Color::Rgb(33, 39, 51));

        // Without hover the same cell renders without the highlight.
        app.transcript.as_mut().unwrap().hover = None;
        let mut terminal = Terminal::new(TestBackend::new(100, 20)).unwrap();
        terminal
            .draw(|frame| crate::ui::render(frame, &app))
            .unwrap();
        let buffer = terminal.backend().buffer();
        assert_eq!(buffer[(1, 4)].bg, Color::Reset);
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
        app.apply_transcript(&AgentId::from_str(STABLE).unwrap(), &page);
        let screen = render_to_string(&app, 100, 24);
        assert!(
            screen.contains("The measurement is decisive here: 40 warning"),
            "wrapped first line: {screen}"
        );
        assert!(
            screen.contains("the full gate must run clean before any commit lands."),
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
                message(1, "tool", "reading files"),
                message(2, "assistant", "done"),
            ],
            "cursor": 0, "limit": 500, "next_cursor": null, "complete": true,
        }))
        .unwrap();
        app.open_selected_transcript();
        app.apply_transcript(&AgentId::from_str(STABLE).unwrap(), &page);
        let screen = render_to_string(&app, 100, 24);
        assert!(screen.contains("tool"), "tool role tag: {screen}");
        assert!(screen.contains("reading files"), "tool content: {screen}");
        assert!(screen.contains("done"), "assistant content: {screen}");
    }
}
