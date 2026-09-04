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

Data is loaded on a background thread: the key loop only picks up the newest
finished snapshot, so keys, mouse and the spinner never wait for socket or
file I/O (a `⟳` in the header hint marks a load in progress). `--refresh` is
the pause between the end of one load and the start of the next; `r` starts
the next load immediately. The cursor follows the selected session and agent
by id, not by row: when a session becomes active and jumps to the top, or an
agent moves from running to finished, the selection stays on it (a finished
agent stays selected only while the finished section is expanded; otherwise
the cursor falls back to the nearest active card).

## Screens

The layout adapts to the terminal width, caps the content width at 72 columns,
and stays usable in a narrow side pane (about 36 columns).

Every card is a light box (`┌─┐ │ └─┘`) separated from the next by a blank
row. Nothing is drawn with a background fill: the selected card has a bold
border and a `▶` before its title; colour is used only as an accent (green
for active/succeeded, red for failed/timed out, dim for secondary rows).

**Sessions.** Header `agent-run · sessions` with the key hint on the right.
One card per orchestrator session from `list_orchestrators`:

```
┌────────────────────────────────┐
│ ▶ Implement dashboard          │   title
│ claude · agent-run             │   host runtime · cwd basename
│ ● 2 active   ○ 1 done          │   child counts
└────────────────────────────────┘
```

The host runtime is derived from the transport (`claude_uds` → `claude`,
`codex_queue` → `codex`, `unbound` → `—`). Agents launched without a binding
appear under a synthetic `unbound` card.

**Agents of a session.** Header `◀ <session title> · <host runtime>`. A
`RUNNING (n)` section lists active agents, newest first: `<spinner> summary`;
`runtime · model · effort` with `⏱ elapsed` right-aligned; `↳ last event`
when the runtime streams a transcript. A `FINISHED (k)` section is collapsed
by default (`Tab` toggles it) and shows `✔`/`✘`/`⏰`/`⊘`/`⚠` for succeeded,
failed, timed out, cancelled and lost agents, plus `✘ <failure kind>` when
one is recorded. Below about 40 columns the right-aligned parts collapse and
summaries are cut with `…`.

Keys work on the Russian layout as well as the Latin one: `j`/`k` (`о`/`л`)
or arrows move, `Enter`/`l`/`→` (`д`) opens a session, `h`/`←`/`Esc`/
`Backspace` (`р`) goes back, `Tab`/`Space` toggles the finished section,
`r` (`к`) reloads, `q` (`й`) quits. A left mouse click selects a card and
opens it on the sessions screen.

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
