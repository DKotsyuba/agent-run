# Terminal UI (`agent-run-tui`)

`agent-run-tui` is an interactive terminal observer for the resident broker.
It renders live sessions as compact cards and the transcript of any selected
session. The observer is a **pure broker client**: it starts nothing, owns no
store, and never mutates broker state — every fact on screen comes from the
same public JSON-RPC surface the CLI and MCP transports use.

## Session cards

Live sessions stack as a single-column list of padded cards:

```
(blank padding row)
  <status pictogram> <task summary>
  <agent id>
  <RUNTIME>/<model>
  workdir <~/path>
  <elapsed>
(blank padding row)
```

The working directory comes from the run's admission request; the agent-run
home prefix is shortened to `~`.

Pictograms: `●` running, `◐` starting, `○` created, `◑` cancelling,
`✓` succeeded, `✗` failed, `◷` timed out, `⊘` cancelled, `◌` lost.
Finished sessions stay hidden behind a dropdown line at the bottom of the
list (`▸ finished (N)`); Tab or a click on it expands them into cards below
the live ones and refetches the listing with the wider scope.

## Running

The shared release installer installs `agent-run-tui` as a separate binary beside
`agent-run`; both always use the same workspace release version. Run
`agent-run-tui` after installation. For source builds, the binary lives in
`crates/agent-run-tui` and connects to the resident
daemon's socket (`<home>/api.sock`, same default home resolution as the CLI,
including `AGENT_RUN_HOME`):

```bash
cargo run --locked --release --package agent-run-tui --bin agent-run-tui
# overrides
agent-run-tui --home ~/.agent-run --socket ~/.agent-run/api.sock
```

Start the resident broker first (`agent-run` daemon per `docs/architecture.md`);
without it the status bar shows `○ retrying` until the socket appears.

## Keys and mouse

Global:

| Input | Action |
|---|---|
| `q`, Ctrl-C | quit |
| `a` | fetch and show the selected session's sealed answer |
| mouse wheel | scroll / move selection |
| click | select a row; clicking the selected row opens its transcript |

Sessions grid:

| Input | Action |
|---|---|
| ↑/↓, `k`/`j`, PgUp/PgDn, wheel | move selection |
| Enter, → | open the transcript of the selected card |
| Tab, `o` | expand/collapse the finished-sessions dropdown |
| click | select a card; clicking the selected card opens its transcript |
| click on `▸ finished` | toggle the dropdown |
| `r` | force a refetch |

Transcript view:

| Input | Action |
|---|---|
| ↑/↓, `k`/`j` | move the message cursor |
| Enter, Space | expand or collapse the selected tool call/result |
| click on a tool line | expand or collapse that payload |
| hover | highlight the message under the pointer |
| PgUp/PgDn, wheel | scroll; scrolling away from the tail leaves follow mode |
| `f` | toggle tail-follow |
| `g`/Home, `G`/End | jump to top / tail |
| Esc, ←, Backspace | back to the sessions grid |

## How it updates

- The sessions table long-polls `list_agents` with `after_revision` and a
  25 s `wait_seconds` window, so the list refreshes as soon as the store
  revision commits anything. Failures fall back to a full listing after a
  2 s delay.
- The transcript watcher cursor-pages the `transcript` method: backfill runs
  without pauses until the stored history is complete, then the tail is
  polled every 300 ms while the view is open.
- Each concurrent broker request owns its socket connection, so the list
  long-poll cannot delay transcript or answer reads.
- Messages are deduplicated by their immutable `seq` cursor, and all rendered
  transcript content passes the same `sanitize` gate as the CLI transcript
  viewer (untrusted engine output is stripped of escape/control sequences).

## Transcript rendering

- `user`, `assistant`, and `system` messages render as plain text with a
  timestamp and role tag; agent reasoning is never collapsed. Engines stream
  text in small deltas and the supervisor journals every delta as its own
  message, so the viewer coalesces runs of same-identity text messages into
  one flowing block — the same rule the CLI transcript viewer applies; any
  tool message breaks the stream.
- A `tool_call` together with every following `tool_result` forms one
  compact group: each message renders as one collapsed line — the tool name
  plus a brief description (`description`, falling back to
  `command`/`cmd`/`file_path`/`prompt`, then a raw preview; results show the
  first meaningful line and the total line count). Blank separator rows sit
  only between groups, never inside one.
- Enter or a click expands the payload in place: JSON content is
  pretty-printed, non-JSON content is shown verbatim. Expansion state is
  per message and survives transcript refreshes.

The observer performs no writes: `start`, `cancel`, and `steer` remain CLI/MCP
operations. Mutation keys may be added later, but they would go through the
same resident broker and keep the "no asynchronous work under a one-shot
process" invariant.

## Layout

```
crates/agent-run-tui/
  src/main.rs     CLI (--home/--socket), terminal setup and teardown
  src/net.rs      typed broker seam (`Broker` trait, JSON-RPC helpers)
  src/app.rs      state + pure reducers (sorting, transcript merge, follow)
  src/events.rs   input thread, sessions/transcript watchers, event loop
  src/ui/         list, transcript, answer popup, status bar, theme
```

## Tests

```bash
cargo test --locked --package agent-run-tui
```

Reducer behavior is unit-tested; rendering is checked with golden
`TestBackend` assertions at fixed terminal sizes (no real tty needed).
