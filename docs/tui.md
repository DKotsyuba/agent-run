# Terminal UI (`agent-run-tui`)

`agent-run-tui` is an interactive terminal observer for the resident broker.
It has separate Sessions and Pools tabs for session transcripts and cooperative
pool conversations. The observer is a **pure broker client**: it starts nothing, owns no
store, and never mutates broker state — every fact on screen comes from the
same public JSON-RPC surface the CLI and MCP transports use.

## Layout

The frame is chrome-free: row 0 is the app bar, row 1 is the tab bar,
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
`○ scrolled` on the right; then a compact dim native-usage row: input/output
and cache-read tokens, USD cost, turns, and `tools 41 · 2 failed · 3 unknown`.
Unreported measurements remain `—`; absent statistics leave the row blank.
Complete lineage totals appear only when different from the latest execution
(for example `Σ 3 runs $1.24`). The row fits the available display width.
The body shows a two-column
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
  own row. Raw-row `starts_block` flags clear pending call identities at scope
  boundaries, including across pages; projected block starts mark ordinary
  logical blocks too, so their results still pair by native reference. The summary
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

## Pools tab

`2` selects **Pools**, `1` selects **Sessions**, and Shift-Tab switches tabs.
The tabs are clickable; ordinary Tab keeps its Sessions finished toggle.
`Pools <n> open` uses the exact broker-wide open total. Project filtering
applies to sessions; a pool always shows the entire team.

At ≥110 columns, the left shelf lists **OPEN** then **COMPLETED** pools,
showing the state glyph, goal excerpt, valid-ready/member count, proposal
sequence and short pool ID. The selected pool is previewed on the right;
Enter focuses it. Narrow terminals show the list first and open the selected
pool full-screen with Enter; Esc returns to the list. `[ ]` changes discovery
pages (50 rows per state), while section counts remain exact across pages.
`agent-run-tui --pool <id>` opens a pool directly, including a pool outside
the current discovery page. If the connected broker predates `list_pools`,
the tab confirms strict pool ids found in loaded session summaries through
the public `pool` read and labels this discovery as session-derived; older
pools whose members are all finished may be unavailable until `list_pools`
is supported.

The detail header shows at most two goal rows, state badge, valid readiness,
proposal and short identity. An empty log preserves the absent sequence
watermark and displays `seq —`. `c` opens the full sanitized goal and criteria
in a scrollable overlay (↑/↓, PgUp/PgDn, Home/End; Esc closes). The MEMBERS
shelf shows broker execution status, verified cleanup, raw vote and its
validity reason. Readiness counts only `counts: true`; a raw ready vote with
a stale roster/tip, revoked vote or unmet checks does not count. Unanimity
while open means **agreement; waiting for success/cleanup**. Only the broker's
completed state receives the completed badge; its proof stays frozen even
if a member resumes. Joined runtime/model labels are marked `latest` on a
completed pool and never replace frozen execution or cleanup facts.

The CHAT pane shows member messages, operator posts, broker roster events,
reports with severity and `orchestrator (team copy)` addressing, framed
proposals, votes and revokes. Headers use historical stamped author names
and `#seq rN`, with no invented timestamps. Every body and snapshot is
sanitized; the CHAT section labels all its content **untrusted**. Long bodies
preview four wrapped lines with an omitted-line count; Enter/click expands them. Votes
and revokes use compact headers and expand to their bodies. Proposal
snapshots are attached only to their exact sequence; historical snapshots
not observed in a status read say `snapshot unavailable`.

`m` switches chat/roster focus. In the roster, ↑/↓ selects a member and
`t`/Enter opens that stable member's existing transcript view; Esc returns
to the pool with chat scroll, follow, expansion and roster state preserved.
`h` expands replacement history; retired members also open transcripts.
Chat ↑/↓ moves the entry cursor; PgUp/PgDn and wheel scroll, leaving follow
mode. `f` toggles follow, `g`/Home requests older history, and `G`/End follows
the tail. Incoming entries preserve the scrolled entry/row anchor.

The tab is read-only: it never posts, replaces members or binds a pool.

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
# open a cooperative pool directly
agent-run-tui --pool pool-20261004-100000-7ac03b9e12
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
| `1`, `2`, Shift-Tab | Sessions / Pools / switch tabs |
| click tab | switch tabs |
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
| `a` | fetch and show the selected session's sealed answer |
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
| `a` | fetch and show this session's sealed answer (including pool member transcripts) |
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

- Sessions, transcript pages, one-shot answer/member-status requests and pools use four separate
  persistent broker connections. The sessions long-poll cannot hold up opening
  a transcript or fetching an answer. Switching selection cancels the previous
  transcript fetch and retires its socket; watcher commands keep only the latest
  target, including a clear while idle or during delivery.
- Pools use their own persistent socket. Visible discovery polls `list_pools`
  with separate open/completed filters, limit and offset, at most once per
  second. Selected pools open with a reverse tail page and poll forward with
  `after_seq` approximately every second; public `pool` has no wait parameter.
  A broker without `list_pools` falls back to confirming candidate ids from
  loaded session summaries through `pool`; retiring the pool socket resets
  discovery capability so the list method is retried on its replacement.
  Hidden tabs cancel pool reads. Completed pools stop forward polling once
  the known tail is loaded; older history remains available on demand.
  Empty unchanged status/log pages draw nothing. Reverse requests use
  `before_seq` equal to the **minimum returned sequence**, independently of
  the broker's overlapping reverse `next_cursor`. Overlapping pages merge
  once by immutable sequence. A four-pool MRU retains at most 500 entries
  per pool; wrapped entry rows are memoized by sequence, width, expansion
  and exact snapshot. At the memory ceiling, a scrolled history window keeps
  its older edge and defers incoming bodies until follow resumes; status
  still refreshes, and the forward cursor never advances past unseen bodies.
- The sessions table long-polls `list_agents` with `after_revision` and a
  25 s `wait_seconds` window. Once data is flowing, listing fetches throttle
  to one per 500 ms so a busy broker (many revision commits per second)
  cannot flood the UI; the first load, every scope change, and a forced
  refresh (`r`) stay immediate. Failures fall back to a full listing after a
  2 s delay. Every successful page restores link health and refreshes session
  observations, including elapsed and silence clocks at an unchanged revision;
  card and project projections remain cached while their keys stay unchanged.
  Each listing round that observes a new store revision also
  refreshes the finished-session total with one cheap unfiltered call
  (`list_agents` without `active` and `limit: 1`), reading the exact total
  instead of loading finished rows; the finished count is that total minus
  the live count, so the app bar and the `FINISHED` header know the number
  while the scope still lists active sessions only. A failed call keeps the
  last known count. The count call rides the sessions lane, so the
  transcript and answer sockets never wait on it.
- Opening a transcript requests `view: "blocks"` with approximately two screens
  of `tail_blocks` (at most 200), renders the newest content at the bottom,
  and follows forward from `resume_cursor`, even when the reverse page is
  marked complete. Scrolling near the loaded top, or Home/`g`, requests one
  older page with the exclusive `before_cursor`. Prepending preserves the line
  being read and existing block row memos; prefix sums and cursor indices move
  with the added history. A top row shows `loading older history…` during the
  request and `beginning of transcript` once no previous cursor remains.
  Partial edge fragments join by sequence range and known native reference;
  unresolved edges show `… partial block`. Omitted/spooled bodies keep their
  truncation marker; the observer cannot fetch an omitted body.
  Block pages contain at most 200 blocks and 256 KiB of content. The worker
  waits for the UI when its two-event queue is full, bounding queued page memory.
  Reducers move message payloads without cloning. Brokers rejecting the blocks
  options fall back once to the legacy raw, 1000-row forward backfill.
  A failed page
  retries with a short doubling backoff (150 ms to 2 s, reset by any
  success), so an intermittently failing broker costs milliseconds per
  retry. In the split view the watcher follows the selected session even
  while the list owns the focus.
- Recently viewed transcripts (up to 8, and 64 MB of content in total) stay
  buffered: re-selecting a session — constant while moving through the
  split view — restores its transcript instantly and resumes fetching from
  where it left off instead of reloading from the start. Both the older-page
  cursor and the independent forward resume cursor survive the restore.
- The transcript store is append-only and `seq`-deduplicated: tail pages
  whose sequences all sit past the buffered tail extend the vector without
  touching existing data, and an unchanged tail poll costs one sequence
  comparison. Rare out-of-order or replaced messages insert in place.
- Incomplete history is visible in the top history row; legacy raw backfill
  retains the header's dim `loading <n> messages…` count. The header shows a red
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
  stays in the worker tasks); drawing is capped at one frame per 33,333,334 ns
  (`FRAME_INTERVAL`, at most 30 fps). Resize dimensions update the state
  before frame preparation, so the first draw uses the current layout.
  When nothing was drawn for longer than the
  interval, the next change draws immediately (leading edge); further
  changes within the interval wait for its end and draw together (trailing
  edge), so any number of events between two frames costs one draw of the
  state as it stands at the frame. The intake loop checks the frame deadline
  after each page rather than draining a large page batch before drawing.
  Nothing draws while the state is clean,
  and a tick marks it dirty only while something animated is visible
  (spinners, elapsed and idle clocks). Needed first-load spinners advance
  every 100 ms without keyboard input, including loads for finished sessions.
  Cached content remains visible during refresh; aggregate live counts and
  loaded empty lists use static indicators. Hidden live sessions do not
  animate a quiet pool view.
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
- `cargo test --release --locked -p agent-run-tui end_to_end_event_storm_probe -- --ignored --nocapture`
  runs an end-to-end timing storm of 2000 events per
  second for 5 s over a live 4000-message (~8 MB) transcript that reports
  draws, reducer and frame time, the longest frame, and bytes written.

The observer performs no writes: `start`, `cancel`, and `steer` remain CLI/MCP
operations. Mutation keys may be added later, but they would go through the
same resident broker and keep the "no asynchronous work under a one-shot
process" invariant.

## Layout

```
crates/agent-run-tui/
  src/main.rs     CLI (--home/--socket/--pool), palette choice, terminal setup
  src/net.rs      typed broker seam (`Broker` trait, JSON-RPC helpers)
  src/app.rs      state + pure reducers (sorting, transcript merge, follow)
  src/events.rs   input thread, sessions/transcript watchers, event loop
  src/pools.rs    discovery, pool watcher, bounded log cache, pool navigation
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
