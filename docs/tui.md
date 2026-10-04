# Terminal UI (`agent-run-tui`)

`agent-run-tui` is an interactive terminal observer for the resident broker.
It renders live sessions as a two-row list and the transcript of the selected
session. The observer is a **pure broker client**: it starts nothing, owns no
store, and never mutates broker state — every fact on screen comes from the
same public JSON-RPC surface the CLI and MCP transports use.

## Layout

The frame is chrome-free (no borders): row 0 is the app bar, row 1 is blank,
the last row is the key bar, and everything between belongs to the panes.

- **Wide terminals (≥ 110 columns) render a split view**: the session list
  pane on the left (one padding column, 43 content columns, one padding
  column, one gap column) and the transcript pane of the selected session on
  the right. Enter/→ moves focus to the transcript, Esc/←/Backspace returns
  it to the list; the transcript pane always shows the selected session, and
  the watcher re-attaches whenever the selection moves.
- **Narrow terminals (< 110 columns)** switch full-screen between the list
  and the transcript of the opened session (Enter opens, Esc returns).

The app bar carries ` agent-run ` on the accent color, the spinner with the
live count, `✓` with the finished count, and (in the narrow transcript view)
a `‹ <agent hash>` crumb instead of the counts. The right side shows the
broker link (`● broker` / `○ retrying`), the store revision, the session
count, and the last broker failure in red. The key bar lists the keys of the
focused pane (accent key, dim description); with a transcript on screen it
also shows a dim `<last visible line>/<total lines>` counter.

## Session list

```
  LIVE  <count>
(blank row)
▌ <glyph> <task summary>                     <elapsed>
▌   <model> · <workdir basename>    idle <silence> / <hash tail>
(blank row)
▾/▸ FINISHED  <count>                tab collapse/expand
```

Selected rows carry the selection background and the `▌` bar in the accent
color when the pane is focused (gray when it is not); hovered rows carry a
subtler hover background. Elapsed time is colored by status. The `FINISHED`
header always renders and is clickable (Tab toggles it too); expanding
refetches the listing with the wider scope. Its count comes from the
broker's unfiltered session total, so it is exact in both the collapsed and
expanded states even before any finished row loads; under a project filter
it stays scoped to loaded rows and fills in once the section is expanded.
With no live sessions in scope but finished ones known to exist, the empty
state shows a dim `no live sessions` instead of `No sessions in this
scope.`

Pictograms: `⠋` (animated spinner) running, `◐` starting, `○` created,
`◑` cancelling, `✓` succeeded, `✗` failed, `◷` timed out, `⊘` cancelled,
`◌` lost — running/starting/cancelling in yellow, created/cancelled gray,
succeeded green, failed/timed out red, lost magenta.

Display labels: when the session carries a human display name (the optional
`name` of `agent-run start --name`), the card title and the transcript
header title lead with it — `<name> — <task summary>`, truncated by display
width. Unnamed agents keep the bare task summary.

## Transcript pane

Four header rows — glyph + task title; status badge (` ● live `, ` ✓ done `,
` ✗ failed `, ` ◷ timed out `, ` ⊘ cancelled `, ` ◌ lost `, …) with
`RUNTIME/model`, `up <elapsed>`, `silence <x>` (a warning past 120 s), and
the failure text; the workdir (home shortened to `~`) with `● follow` or
`○ scrolled` on the right; then a blank row. The body shows a two-column
selection gutter, the conversation, and a scrollbar (`┃` thumb, `│` track)
once the content overflows the viewport.

- `user` messages render as `› prompt` with a dim right-aligned timestamp
  and the body indented two columns; more than five wrapped lines collapse
  to four plus `… N more lines  ⏎ expand` (Enter or click toggles).
- `assistant` text starts with the `◆ ` marker; streamed deltas coalesce
  into one flowing block. Fragments join only inside one native message —
  the same role and name with equal, known `raw_ref`s. Unknown references
  never merge. A `starts_block` boundary always breaks the stream, so
  executions and attempts never run together.
- `system` and other notes render dimmed and italic.
- A `tool_call` and its following `tool_result`(s) render as **one row**:
  `▸ <tool>  <brief argument>` with a dim result summary and duration on the
  right. Results pair to their call by equal, known `raw_ref` and matching
  tool name, including out-of-order results. Uncorrelated results get their
  own row. A `starts_block` flag clears pending call identities before
  grouping, including across pages. The summary
  carries the journaled native evidence — `[error]` when the result's
  `error` flag is true, `[ok]` when it is false, `[unknown]` when unreported
  — and the tool name and summary turn red only on `[error]`; `[unknown]`
  stays neutral and is never shown as success. Across result chunks, any
  explicit failure wins; success requires every chunk to report success.
  Neither payload keywords nor `error_source` invent an outcome. A
  live call without a result yet shows a yellow spinner with `running` and
  the elapsed time. Consecutive collapsed tool rows stack without a gap;
  every other block pair is separated by one blank row.
- Enter or click expands a tool row onto a code background: the arguments
  as `    <key padded to 10> <value>` (command-like values in orange), a dim
  `┄` separator, the result payload (JSON pretty-printed, otherwise
  verbatim), a dim `… content truncated` marker when the journal reports
  `content_complete: false` for arguments or results (also appended to the
  collapsed summary, including while a call is pending), and
  one blank code row.
- Finished sessions end with an outcome line: `✓ finished in X`,
  `✗ failed · <failure>`, or `◷ timed out after X`.

All rendered transcript content passes the same `sanitize` gate as the CLI
transcript viewer (untrusted engine output is stripped of escape/control
sequences), and expansion state is per message and survives refreshes.

## Overlays

The help (`?`), project picker (`p`), and sealed answer (`a`) popups share
one style: a panel background, a rounded accent border (`╭╮╰╯`), the title
on the top border, and a dim `esc close` on the bottom border. Esc, `?`, or
`q` closes the help; a click anywhere closes it too.

## Running

The binary lives in `crates/agent-run-tui` and connects to the resident
daemon's socket (`<home>/api.sock`, same default home resolution as the CLI,
including `AGENT_RUN_HOME`):

```bash
cargo run --locked --release --package agent-run-tui --bin agent-run-tui
# observe one project only: its checkout and every worktree
agent-run-tui ~/projects/agent-ide
# overrides
agent-run-tui --home ~/.agent-run --socket ~/.agent-run/api.sock
```

A project argument filters the session list to sessions whose working directory
belongs to that project: the checkout itself and its worktrees
(`<dir>/.claude/worktrees/*`, `<dir>/.worktrees/*`, `<dir>/worktrees/*`)
collapse to one project root. `p` opens an interactive project picker built
from the loaded sessions; the first entry (`all projects`) clears the filter,
and `Esc` closes the popup without changing it.

Start the resident broker first (`agent-run` daemon per `docs/architecture.md`);
without it the app bar shows `○ retrying` until the socket appears.

## Keys and mouse

Global:

| Input | Action |
|---|---|
| `q`, Ctrl-C | quit |
| `?` | key-help overlay (Esc/`?`/`q` closes) |
| `a` | fetch and show the selected session's sealed answer |
| mouse wheel | scroll the pane under the pointer |
| click | select a row; clicking the selected row opens its transcript |

Sessions list:

| Input | Action |
|---|---|
| ↑/↓, `k`/`j`, PgUp/PgDn, wheel | move selection (the split pane follows it) |
| Enter, → | open / focus the transcript of the selected row |
| Tab, `o` | expand/collapse the `FINISHED` section |
| `p` | project picker (filter by project; worktrees group together) |
| `r` | force a refetch |
| click on `▸ FINISHED` | toggle the finished section |

Transcript pane:

| Input | Action |
|---|---|
| ↑/↓, `k`/`j` | move the block cursor |
| Enter, Space | expand or collapse the selected tool row / prompt |
| click on a row | expand or collapse that block |
| hover | highlight the block under the pointer |
| PgUp/PgDn, wheel | scroll; scrolling away from the tail leaves follow mode |
| `f` | toggle tail-follow |
| `g`/Home, `G`/End | jump to top / tail |
| Esc, ←, Backspace | back to the list (narrow) / return focus to the list (split) |

## Colors

The design is a fixed 24-bit palette: backgrounds `#0f1115` (base),
`#13161b` (panel/code), `#1d222b` (selection), `#171b22` (hover),
`#262b34` (scrollbar track); text `#cdd2da` (body), `#aab1bc` (secondary),
`#f1f3f6` (emphasis), `#6b7280` (dim); status colors `#f07178` red,
`#9ccc7a` green, `#e6c07b` yellow, `#7aa7f0` blue, `#c99bf0` magenta,
`#f0a46c` orange (commands); accent `#5ccfe6` on `#132a30`, plus the tinted
badge backgrounds `#17281d` (done), `#2e191c` (failed), `#221b2d` (lost).

When the terminal does not advertise truecolor (`COLORTERM` is neither
`truecolor` nor `24bit`), the observer picks a 16-color fallback once at
startup: foregrounds map to the nearest named colors and backgrounds fall
back to the terminal default — except selection, which keeps a dark named
background so highlights stay visible.

## How it updates

- Sessions, transcript pages, and one-shot answer requests use three separate
  persistent broker connections. The sessions long-poll cannot hold up opening
  a transcript or fetching an answer. Switching selection cancels the previous
  transcript fetch and retires its socket; watcher commands keep only the latest
  target, including a clear while idle or during delivery.
- The sessions table long-polls `list_agents` with `after_revision` and a
  25 s `wait_seconds` window. Once data is flowing, listing fetches throttle
  to one per 500 ms so a busy broker (many revision commits per second)
  cannot flood the UI; the first load, every scope change, and a forced
  refresh (`r`) stay immediate. Failures fall back to a full listing after a
  2 s delay. Each listing round that observes a new store revision also
  refreshes the finished-session total with one cheap unfiltered call
  (`list_agents` without `active` and `limit: 1`), reading the exact total
  instead of loading finished rows; the finished count is that total minus
  the live count, so the app bar and the `FINISHED` header know the number
  while the scope still lists active sessions only. A failed call keeps the
  last known count. The count call rides the sessions lane, so the
  transcript and answer sockets never wait on it.
- The transcript watcher cursor-pages the `transcript` method at the
  broker's maximum page size (1000 messages): backfill pages fetch back to
  back while a two-event queue has room, then the tail is polled while the view
  is open. The worker waits for the UI when that queue is full, bounding queued
  page memory. Reducers move message payloads into the buffer without cloning.
  A failed page
  retries with a short doubling backoff (150 ms to 2 s, reset by any
  success), so an intermittently failing broker costs milliseconds per
  retry. In the split view the watcher follows the selected session even
  while the list owns the focus.
- Recently viewed transcripts (up to 8, and 64 MB of content in total) stay
  buffered: re-selecting a session — constant while moving through the
  split view — restores its transcript instantly and resumes fetching from
  where it left off instead of reloading from the start.
- The transcript store is append-only and `seq`-deduplicated: tail pages
  whose sequences all sit past the buffered tail extend the vector without
  touching existing data, and an unchanged tail poll costs one sequence
  comparison. Rare out-of-order or replaced messages insert in place.
- Incomplete history is visible: the transcript header shows a dim
  `loading <n> messages…` count while the backfill runs and a red
  `broker error, retrying · <reason>` while the last page failed; a
  finished session only shows its `✓ finished in X` outcome line once the
  history fully arrived.
- Rendering is incremental. The body is grouped into blocks (streamed text
  runs, one row per tool call with its `raw_ref`-paired results); the
  grouping is width-independent and extends from the first unconsumed
  message, so an append never rescans the history, and a message-to-block
  index answers cursor, hover, and selection lookups directly. Each block's
  rows are memoized by block identity, expansion, and width: an appended
  delta extends the tail text block by consuming only the new bytes and
  replacing its unfinished wrapped row; completed sanitized rows remain cached.
  A constant-size escape scanner preserves CSI/OSC handling across delta
  boundaries. Collapsed tools keep a bounded first meaningful result line and
  a running line count; only expansion concatenates their payloads. Full
  arguments wrap once into pane-sized cached rows. An arriving result updates
  only its call block, and an expand/collapse only that block. Line starts are
  prefix sums
  recomputed from the first changed block, and a visible row is found by
  binary search, so an unchanged state syncs in a few integer comparisons.
  Session-view refreshes (uptime, silence) update the header only and
  rebuild nothing. Spinner frames, running-tool elapsed clocks, and the
  terminal outcome line are substituted at assembly time, so animation never
  rebuilds rows, and only the visible viewport rows are assembled each frame.
- Event intake is decoupled from drawing. Every broker page and input event
  applies to the state as it arrives through cheap reducers (JSON parsing
  stays in the worker tasks); drawing is capped at one frame per 33 ms
  (`FRAME_INTERVAL`, ~30 fps). When nothing was drawn for longer than the
  interval, the next change draws immediately (leading edge); further
  changes within the interval wait for its end and draw together (trailing
  edge), so any number of events between two frames costs one draw of the
  state as it stands at the frame. The intake loop checks the frame deadline
  after each page rather than draining a large page batch before drawing.
  Nothing draws while the state is clean,
  and a tick marks it dirty only while something animated is visible
  (spinners, elapsed and idle clocks).
- Input bursts coalesce. Consecutive wheel, arrow, and page steps of one
  kind and direction merge into one net selection, cursor, or scroll delta
  applied once per frame (a direction change, or any other key or click,
  applies the pending run first so it acts on the moved state). Pointer
  moves keep only the latest position and resolve the hover target once per
  frame; a move that changes no hover target draws nothing. Clicks resolve the
  stable agent id or message sequence from the last drawn frame, even when a
  listing or content update is waiting to draw. Hover is recomputed from the
  remembered pointer position whenever scrolling or new content changes the
  visible rows.
- Text fitting uses graphemes and terminal display width, including wide CJK
  and emoji. Continuation indentation leaves room for content, and document
  offsets use full-size integers so Home reaches the start of large histories.
  Session card/project projections are cached until a listing, scope, or filter
  changes; answer text is sanitized once and wrapped once per popup width.
- Terminal output is diffed cell by cell (ratatui); the observer never
  clears or resets the screen between frames, so an unchanged redraw writes
  only the backend's fixed style-reset and cursor-hide sequences (25 bytes).
- `cargo test --release --locked -p agent-run-tui -- --ignored --nocapture`
  runs the timing probes, including an end-to-end storm of 2000 events per
  second for 5 s over a live 4000-message (~8 MB) transcript that reports
  draws, reducer and frame time, the longest frame, and bytes written.

The observer performs no writes: `start`, `cancel`, and `steer` remain CLI/MCP
operations. Mutation keys may be added later, but they would go through the
same resident broker and keep the "no asynchronous work under a one-shot
process" invariant.

## Layout

```
crates/agent-run-tui/
  src/main.rs     CLI (--home/--socket), palette choice, terminal setup
  src/net.rs      typed broker seam (`Broker` trait, JSON-RPC helpers)
  src/app.rs      state + pure reducers (sorting, transcript merge, follow)
  src/events.rs   input thread, sessions/transcript watchers, event loop
  src/ui/         frame chrome, list, transcript, overlays, text, theme
```

## Tests

```bash
cargo test --locked --package agent-run-tui
```

Reducer behavior is unit-tested; rendering is checked with golden
`TestBackend` assertions at fixed terminal sizes (no real tty needed),
including the split view at 160×48 and the narrow view at 66×52.
`TUI_DUMP=1 cargo test … frame_dumps -- --nocapture` prints rendered
reference frames.
