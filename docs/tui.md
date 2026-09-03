# Terminal dashboard (`agent-run-tui`)

`agent_run_tui` is a separate, standard-library-only package that renders a
full-screen curses dashboard over the agents supervised by agent-run. It never
imports the core package and never opens the state database: every value it
shows comes from the JSON-RPC Unix socket described in [api.md](api.md). The
boundary is enforced by `tests/test_tui_boundary.py`.

## Running

```bash
agent-run api serve            # in another terminal, or via launchd
agent-run-tui                  # or: python -m agent_run_tui
```

Options:

| Flag | Meaning |
|---|---|
| `--socket PATH` | API socket to read; default `$AGENT_RUN_HOME/api.sock` (`~/.agent-run/api.sock`) |
| `--refresh SECONDS` | data refresh interval, default 2 |
| `--demo` | render built-in sample data without a server |

If the socket is missing, the status bar shows the error and the last
successful snapshot stays on screen; the dashboard keeps retrying.

## Screens

The layout adapts to the terminal width, caps the content width at 72 columns,
and stays usable in a narrow side pane (about 36 columns).

**Sessions.** One card per orchestrator session from `list_orchestrators`:
the session title, then `active N · total M · <transport> · <cwd basename>`.
Agents launched without a binding appear under a synthetic `unbound` card.

**Agents of a session.** Active agents first, newest first: task summary;
`runtime · model · effort`; a spinner with the elapsed time and, when the
runtime streams a transcript, a one-line hint of the newest message. Finished
agents sit below a divider in a collapsed section (`Tab` expands it) and show
their terminal status, elapsed time, and failure kind.

Keys: `j`/`k` or arrows move, `Enter` opens a session, `Esc`/`Backspace`/`h`
goes back, `Tab`/`Space` toggles the finished section, `r` reloads, `q` quits.
A left mouse click selects a card and opens it on the sessions screen.

## Session titles

The API exposes only the host transport and external session id. Titles and
working directories are resolved from the host runtime's own files, read-only
and incrementally (files are append-only, so only bytes added since the last
refresh are parsed):

| Transport | Title | Working directory |
|---|---|---|
| `claude_uds` | last `custom-title` record in `~/.claude/projects/*/<session>.jsonl`; fallback: first prompt for that session in `~/.claude/history.jsonl` | `cwd` of the last transcript record that carries one |
| `codex_queue` | `thread_name` for the thread id in `~/.codex/session_index.jsonl` | `payload.cwd` of the `session_meta` line in the matching `~/.codex/sessions/**/rollout-*-<id>.jsonl` |
| other / not found | first eight characters of the external id | — |

External session ids are validated as a single safe path component before any
file is opened; anything else falls back to the short id.

## Data path per refresh

1. `list_orchestrators` — sessions and counters.
2. One paginated `list_agents` sweep (200 per page, newest first, capped at
   1000 agents), grouped client-side by `delivery.orchestrator_session_id`.
3. `transcript` from the stored cursor for each active agent only, so a
   refresh reads new messages, never the whole history.

The dashboard issues read-only methods only; it cannot start, steer, or cancel
agents.
