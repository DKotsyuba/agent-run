# agent-run JSON-RPC API (Unix socket)

Programmatic access to agent-run for external processes on the same machine.
This is the third transport next to the CLI and the stdio MCP server; all
three expose the same operator tool surface through one shared dispatcher
(`crates/agent-run-core/src/dispatch.rs`), so a tool that exists in MCP exists here
under the same name with the same parameters.

Workers use a [separate fixed five-tool MCP surface](worker-mcp.md):
`notify_orchestrator`, `pool_post`, `pool_read`, `pool_propose` and `pool_vote`.
Pool actions require live authenticated membership. Private worker broker routes are not in operator
discovery and requires the current run/attempt capability on every call.

Audience: an integrating agent or developer who has never seen this repo.
Everything needed to connect is on this page.

## Starting the server

Foreground:

```bash
agent-run --home ~/.agent-run api serve
```

For the recommended long-lived macOS setup, generate a keep-alive launchd
plist, then bootstrap it for the current user:

```bash
plist="$HOME/Library/LaunchAgents/com.agent-run.api.plist"
agent-run --home "$HOME/.agent-run" api launchd --binary "$(command -v agent-run)" \
  | plutil -extract plist raw -o "$plist" -
launchctl bootstrap "gui/$(id -u)" "$plist"
```

The launchd command emits a JSON result. Extract its `plist` field as shown;
redirecting the full JSON object does not create a valid launchd plist.

The generated service runs `BINARY --home HOME api serve` with `RunAtLoad` and
`KeepAlive` enabled. It sets the job's soft open-file limit to 65,536; the API
broker, supervisors, and runtime children inherit that limit instead of
launchd's default 256. A foreground process can still be run under another
supervisor when launchd is unavailable.

On Linux, the foreground command can run under an external service manager.
For a shared service with separate worker accounts, see the bundled
[systemd deployment guide](linux-access-control.md). A per-user unit can use:

```ini
[Unit]
Description=agent-run broker

[Service]
ExecStart=/home/you/.local/bin/agent-run --home /home/you/.agent-run api serve
Restart=on-failure
LimitNOFILE=65536

[Install]
WantedBy=default.target
```

Install it as `~/.config/systemd/user/agent-run.service`, adjust both absolute
paths, then run `systemctl --user enable --now agent-run.service`.

- Socket path defaults to `<home>/api.sock` (with `--home ~/.agent-run`
  that is `~/.agent-run/api.sock`). Override with `--socket PATH`.
  macOS caps `AF_UNIX` paths at ~104 bytes — keep the path short.
- The home must be an owned private directory (mode `0700`) and the socket is
  `chmod 0600`. The broker checks each peer's UID, and the built-in client checks
  the broker's UID against its effective UID. There is no network listener or
  general operator token. The internal worker notification route additionally
  verifies an attempt-specific capability; it cannot choose its recipient.
- If a live server already owns the socket, a second `api serve` refuses
  to start (native ownership lock plus a connect probe). A stale socket file
  left by a crash is replaced automatically.
- `SIGTERM`/`SIGINT` shut the server down and remove the socket file.
- Startup takes a couple of seconds (service construction); wait for the
  socket file to appear before connecting.

## Planned service restart

Use `agent-run --home ~/.agent-run restart` for the standard systemd unit on
Linux or launchd job on macOS. For a Linux user unit, add `--systemd-user`;
`--target` selects a different systemd unit or launchd service target. A shared
broker can instead be reached using the existing `--broker-socket` and
`--broker-uid` options. The caller must already have the OS permission to
restart the selected service.

The command blocks new starts, queues a warning to affected chats, asks workers
to call `finish` with the supplied restart marker, and sends a stricter warning after 30 seconds. Workers that
have not stopped after another 30 seconds are interrupted. After restart it
resumes the same sessions and reports each outcome. An unconfirmed continuation
is reported as unknown after 60 seconds. Cancelled or previously finished
agents are not resumed.

The existing Bridge delivery policy handles restart notices. Before stopping
workers, the broker allows up to 10 seconds for queued warnings to be attempted;
an unavailable dispatcher cannot block restart indefinitely. Bridge custody
does not confirm that the native client accepted or the model read the notice.
The existing Bridge producer grant must also allow the `agent-run.maintenance`
kind with `message` mode and `keep_all` policy for these notices.

The schema-35 migration refuses to remove records from the earlier experimental
schema-34 maintenance implementation. It retains the original database and its
pre-migration backup when such records exist.

The current chat binding stores a transport and session ID, while Bridge has one
configured store per transport. It cannot safely distinguish multiple stores
for the same transport. A first-time caller without an explicit binding also
depends on its post-tool hook reaching the broker; a disconnect in that gap can
leave that caller unrecorded. These cases need per-call routing before restart
notifications can be relied on for all chats in a shared deployment. An agent
bound only after a recipient's result has been attempted cannot be added to
that already frozen result.

If the manager definitively fails, the original broker resumes the workers and
the initiating command reports the error. The first JSON output includes the
restart ID and request ID. If the manager outcome is unknown, do not invoke it
again blindly; after confirming it is no longer running and did not restart the
service, `agent-run restart --recover ID` resumes the prepared work on the old
broker. A repeated `--request-id` selects the same recorded operation and never
invokes an already-claimed manager request a second time.

## Wire protocol

JSON-RPC 2.0, one JSON object per newline-terminated line, UTF-8, both
directions, over a `SOCK_STREAM` Unix socket. Maximum line size 1 MiB.
Notifications (requests without `id`) get no reply. Batch arrays are not
supported (`-32600`).

`method` is the tool name; `params` is a single object whose fields are the
tool's arguments.

```json
{"jsonrpc": "2.0", "id": 1, "method": "list_agents", "params": {"limit": 1}}
{"jsonrpc": "2.0", "id": 1, "result": {"items": [...], "revision": 42, ...}}
```

Terminal agent responses include `delivery.last_attempt` when Codex queue made
at least one delivery attempt. The additive nullable object records a safe
classifier, executable provenance, argv shape without values, duration, exact
return code or spawn errno, error class, original output byte counts,
truncation flags, bounded redacted stdout/stderr tails, and whether a remote
message id was observed. Each tail is at most 4096 UTF-8 bytes. Messages,
session ids, argv/environment values, and credentials are never persisted.

One connection may send many requests; on a single connection they are
answered in order; no cross-connection ordering is promised. Separate
connections execute concurrently within independently bounded control and read
groups. Durable start/resume/cancel/steer admission uses the control group;
ordinary reads use the read group, so slow reads cannot consume control permits.
Long polls hold neither group's permit. The server caps
connections and queued calls, reserves one connection slot for parsed control
methods, rejects ordinary overload with JSON-RPC code `-32001`, and
returns `-32002` when its request deadline expires. Input frames remain limited
to 1 MiB; idle reads and response writes also have finite deadlines. A client
holding the reserved slot without completing its first frame is disconnected
within 0.5 seconds.

The socket path is fenced by a lifetime native file lock. A pre-existing socket
is reclaimed only after a refused or missing-endpoint connect result and an
unchanged-inode check. A slow or malformed ping is never evidence that an owner
is dead. Shutdown stops accepting connections and gives admitted handlers a
bounded grace period before closing remaining streams.

## Method surface

Discover the authoritative surface at runtime:

- `tools` (no params) — returns the full tool table **with JSON schemas
  for every tool's parameters**. This is the contract; prefer it over any
  hardcoded list.
- `ping` (no params) — `{"ok": true}`; liveness probe.
- `capabilities` — versioned compiled read schemas and transport bounds.
  With `include_store_id:true`, it also reads this database's `store_id`;
  without that opt-in it needs no database and returns the existing shape.
  New observers should retain `ping`/`tools` fallback for older brokers.

`start_batch` takes one `request_id` and an ordered `jobs` array of 1–20
ordinary provider start requests. A job must omit its own `request_id` or set
it to null. Optional `retain_history` defaults to false and is part of the
batch identity. When true, each job receives a distinct `pin_id` in its
receipt and the pins commit with all jobs and the receipt. All jobs and one
permanent batch receipt commit together, or no
job is admitted. The shared key is scoped to this Run home, not to a caller
identity. Exact replay requires the same durable database; replacing it under
the same socket or restoring an older copy cannot preserve absent receipts or
pins. A matching retry returns the original ordered `agent_id`, `sequence`,
and opaque `execution_ref` receipt; changed intent conflicts. The reference
identifies the admitted execution across provider attempts and is not a secret
or an authorization capability. The receipt outlives ordinary agent history
retention, so an old ID may no longer have a readable status or answer. Status
and batch preconditions accept the reference together with its exact sequence.

The receipt includes the actual `store_id`. Clients that need a database fence
can record the opt-in capabilities identity with their durable intent and supply
it as `expected_store_id` on `start_batch`. The expected ID is part of the
fingerprint and is checked with the receipt or admission in one database
snapshot. A different ID yields `StoreIdentityMismatch`; if a prior outcome
was unknown, this mismatch does not prove whether that earlier request committed.
Do not add an expected ID retrospectively to an unresolved older intent.
The ID is generated on database creation or migration and survives ordinary
restart. A clone or restored backup keeps the same ID, so it cannot detect
rollback to an earlier copy; it is public identity, never a credential.

An optional `preconditions` array requires a non-null `expected_store_id` and
contains 1–20 distinct lineages in the caller's chosen order. Every element
requires `agent_id`, positive `sequence`, `execution_ref`, and `pin_id`; null,
an empty array, duplicate typed fields or duplicate canonical lineages are invalid.
Inside the same local admission transaction, every selector must identify the
latest retained execution of its lineage with verified identity, an active
pin, complete history, no pending restart, terminal lineage and confirmed
lineage cleanup. Any terminal outcome may qualify; this does not prove success
or that an answer file is currently readable. The ordered conditions are part
of the immutable intent and are echoed in the receipt. Omission preserves the
older wire shape, and exact receipt replay precedes any new condition check.

`PreconditionFailed` describes only the current physical admission attempt.
Its `reason` is `not_met` when a required fact is known false or `unknown` when
the evidence is insufficient. The socket exposes `error.data.code` and
`error.data.reason`, CLI exposes `error.type` and `error.reason`, and MCP uses
`structuredContent.code` and `structuredContent.reason`. A prior lost or
uncertain submission remains unresolved even if a later submission fails a
condition; retain its original intent and reconcile its permanent receipt.
Separate CLI/MCP processes do not remember earlier outcomes for the caller.

An active history pin retains the database execution and attempt rows, including
cleanup proofs, for its entire lineage and future resumes, against both age and
count retention. It does not prevent resume or promise to retain process files.
The operator group calls `history_pins {"action":"release","pin_id":"..."}`
to release it; both `already_released:false` and `true` confirm release.
The permanent released tombstone prevents an old batch replay from reviving it.
`PinNotFound`, timeout or an absent reply does not confirm release and does not
restore a missing tombstone. Retry the same pin ID to reconcile an uncertain
release. A pin ID itself grants no authority.

`history_pins {"action":"list"}` lists active and released pins, with optional
`status` (`active`, `released` or `all`), `limit` (default 100, maximum 1000),
and exclusive `after_pin_id`. Continue with `next_after_pin_id`; null marks the
last page. The ordering is by pin ID; concurrent new pins can require a fresh
scan. CLI equivalents are `agent-run history-pins list` and
`agent-run history-pins release --pin-id PIN_ID`.
Both list and release also accept optional `expected_store_id`; their database
scope check shares the list snapshot or release transaction respectively.
CLI supplies it with `--expected-store-id` and obtains an ID with
`agent-run capabilities --include-store-id`.

The receipt proves admission, not process launch or task completion. Supervisor
handoffs happen separately, and a lost reply, timeout, or uncertain commit must
be reconciled using the same key and exact intent. `BatchOutcomeUnknown` is the
stable error code when admission may have committed; a missing or malformed
response remains unknown even if no code arrives. Batch transport loss is not
retried implicitly. The whole encoded request
must fit the existing 1 MiB socket frame and the writer's limits, even though
each job separately permits a task up to 512 KiB. Automatic multi-agent hook
binding is not provided. Supply an explicit `orchestrator` on each job when a
completion notice is required, and inspect each agent's status independently.
CLI callers can use `agent-run start-batch --spec FILE`, or `--spec -` for
standard input. Restoring a backup taken before admission or resetting the
state database cannot preserve a receipt absent from that database.

`batch_receipt` performs a read-only lookup using the original `start_batch`
intent, including `request_id`, ordered `jobs`, `retain_history`, optional
ordered `preconditions`, and a required non-null `expected_store_id`. It uses the same strict decoder and fingerprint,
without loading workdirs, configuration or quota. A committed exact match
returns `{"found":true,"receipt":{...}}`; an absent receipt returns only
`{"found":false}`. Absence is a snapshot fact: an earlier in-flight request
may still commit. Wrong database or conflicting intent returns the existing
typed error and does not establish the outcome of an earlier unknown request.
CLI uses `agent-run batch-receipt --spec FILE` (or `--spec -`).
For a new scoped batch, admission checks that its normalized lookup fits a
compact MCP JSON-RPC frame with an encoded ID of up to 1024 bytes and a trailing
LF; the same bound covers the shorter socket envelope. Extra metadata, whitespace
or noncompact escaping still consume frame space. Existing exact replays are
checked first. Unscoped batches cannot use the scoped receipt lookup.

`status` reads one to twenty exact selectors in one SQLite snapshot:
`{"version":1,"expected_store_id":"store_…","targets":[{"agent_id":"ag-…","sequence":1,"execution_ref":"ex1_…","pin_id":"pin_…"}]}`.
`expected_store_id`, `execution_ref`, and `pin_id` are optional, but omitted
identity or retention evidence cannot establish the corresponding guarantee.
Items follow request order; each reports `match`, pin state, selected and
retained-latest facts, coverage, restart state, lineage terminality, historical
`success_commit`, and lineage cleanup. `retained_latest` proves latest-ever
only when coverage is complete. `terminal_outcome` and `success_commit` refer
to that retained latest execution in the same snapshot. `success_commit` is a
historical database fact: it neither opens the answer file nor proves its
current existence or bytes. A reference or pin ID is not authorization.

`include_view:true` is accepted only for one selector. A verified or
unverified selected row receives a redacted `view`; missing, mismatch, and
unknown matches do not. The view is of the selected row, never a substituted
tail. `view.sampled_at` and its process state are separately sampled from the
database proof's `observed_at` and are not authority. Process IDs, process
tokens, cleanup details, unverified policy/health payloads, raw failure text,
and delivery attempt evidence are not returned. CLI uses
`agent-run status --spec FILE` (or `--spec -`). Old scalar
`status {agent_id}` requests are invalid; existing list methods are unchanged.
The complete response must fit the transport frame; no proof is truncated.
`view.status_scope` is always `selected_execution_raw`: its agent status,
phase, runtime outcome and answer-available flag describe the selected row,
even during a pending restart. Use the item's restart and lineage fields for
the logical outcome.
The store's row/byte/time checks are cooperative and do not impose a hard
deadline on an individual SQLite query.

Cooperative pools use five strict methods from the same table: `start_pool`
(two to five members sharing one goal and acceptance criteria, admitted
atomically; each member supplies exactly one `start` or `existing_agent_id` plus
`role`; returns the stable `pool_id` and each member's `agent_id`,
name, role and status), `pool_post` (an operator message stamped as the
orchestrator), `pool_replace` (replace a terminal, fully cleaned member; same
`request_id` returns the same new member) and `pool` (status plus a cursor-paged
log), and `list_pools` (read-only discovery). The first three ride the control
lane; `pool` and `list_pools` use the read lane. Refusals use the shared error codes
with the pool code leading the message (`member_busy`, `pool_completed`,
`pool_not_found`, ...). A pool completes only by formal verification (every
member voted ready on one proposal, ended successfully, cleanup verified) and
delivers exactly one common notice; that is not proof the result is correct.

An attached existing member keeps its current owned RUNNING execution, task,
account, grants, native session and reservation. Its admission
member row adds `existing: true` and an `enrollment` view (`state`, optional failure reason); ordinary new-only member shapes stay unchanged.
The worker-only `pool_read` adds its own attempt-pinned ACK challenge. Only an
exact `pool_post` summary using that challenge changes `pending` to `joined`;
unjoined members cannot vote or settle. `needs_action` reports failed awareness
without restarting or cancelling old work. See [worker enrollment](worker-mcp.md).

Over MCP, existing tool results render as one compact plain-text page (see
`assets/mcp/*.txt.j2`) instead of the structured JSON below; `start`/`resume`
additionally keep `structuredContent` `{"agent_id": ..., "sequence": ...}` so
automatic PostToolUse binding can pin its exact admission. The numeric counter
is transport metadata, not another agent identifier; orchestrators use only
`agent_id`. This socket API and the CLI
keep the structured contracts documented here.
The additive `capabilities`, `answer_chunk`, and `start_batch` methods return
their exact structured result in MCP `structuredContent`, with a fixed text summary.
When an answer exceeds the MCP text budget, `answer` returns a bounded summary
with the verified artifact size, SHA-256 and execution sequence, plus a link to
the existing local answer file where its managed path permits one. Access to
that file follows operating-system permissions; `answer_chunk` provides the
content over MCP without direct filesystem access. The artifact remains subject
to the existing history retention policy.

The current MCP contract, byte/row budgets, protocol-versus-business error
channels, and status-preserving presentation fallback are documented in
[MCP presentation](mcp-presentation.md). Current discovery is generated with
`cargo xtask contract export` and checked against both live registries;
`schemas/mcp-registration.json` describes commands and environment names
without registering anything with a host.

Shared MCP/socket discovery advertises 20 tools: `start`, `start_batch`, `resume`, `cancel`,
`steer`, `list_agents`, `answer`, `transcript`, `doc`, `delegation_guide`,
`limits`, `start_pool`, `pool_post`, `pool_replace`, `pool`, `list_pools`, `history_pins`, and
`agent_integration`, `capabilities`, and `answer_chunk`.
`models` and `capacity_order` remain call-only compatibility methods in the
same registry/dispatcher; their original structured CLI/socket responses and
direct MCP calls by known name remain available. Schema-1 `capacity_order`
and `limits` return `Unsupported` with guidance to migrate to schema 2.

### Operator lineage holds (v1)

The operator socket provides an opt-in compare-and-hold over 1–20 ordered,
distinct lineages. Query `capabilities` with `{"include_lineage_holds":true}`
to discover the v1 method names. These methods are not MCP tools; in particular,
do not put release signatures in model calls, command-line arguments, logs or
transcripts. Run does not control whether an external client records its input.

Every request below requires `version:1` and `expected_store_id`. IDs and
hexadecimal values use their canonical lowercase forms. The operator must
durably retain its exact request, private signing key and optional intent
digest before acquire; Run receives only the Ed25519 public key.

| Method | Additional request fields |
| --- | --- |
| `_lineage_hold_acquire` | `request_id`, ordered `targets` of `{agent_id,sequence,execution_ref,pin_id}`, `verifying_key` (64 hex), optional `intent_digest` (64 hex or null) |
| `_lineage_hold_lookup` | Original `request_id` and `fingerprint` (64 hex) |
| `_lineage_hold_list` | `state` (`all`, `active`, or `released`), `limit` (1–100), and `after_hold_id` (exclusive cursor or null) |
| `_lineage_hold_release` | `hold_id` and `signature` (128 hex) |

Acquire verifies, in the same writer transaction, the exact store and latest
execution, an active original pin with complete coverage, no pending or unknown
restart, recorded successful terminal commit, and confirmed cleanup of the
whole lineage. No hold is created if any target is unverified. An active hold
prevents resuming or replacing its evidence and releasing its pin; overlapping
holds protect independently. It does not authenticate a worker or a ballot.

The immutable receipt contains `version`, `store_id`, `hold_id`,
`fingerprint`, the exact ordered `targets` and corresponding
`canonical_roots`. Acquire also returns `state` and `replayed`. An exact
replay returns the original receipt before rechecking mutable history and never
reactivates a released hold; a changed intent conflicts. Lookup returns
`{store_id,found:false}` or `{store_id,found:true,receipt,state}`. Release
returns `{store_id,found:false}` when absent or
`{store_id,found:true,hold_id,state:"released",already_released}` after
signature verification, including on every new repeated release. Neither
response includes the signing key, signature or intent digest.

For the v1 fingerprint, SHA-256 covers the following unambiguous bytes in
order: ASCII `agent-run:lineage-hold:acquire:v1` and NUL; 16 raw store-ID
bytes; the UTF-8 request-ID byte length as u16 big-endian and its exact bytes;
the target count as u8; each target's exact 29 ASCII agent-ID bytes, sequence
as u32 big-endian, 32 raw execution-ref bytes and 16 raw pin-ID bytes; 32
public-key bytes; and either byte 0 for no intent digest or byte 1 followed by
its 32 bytes. The signed release message is ASCII
`agent-run:lineage-hold:release:v1` and NUL, followed by the 16 store-ID
bytes, 16 hold-ID bytes and 32 fingerprint bytes. Keys must be canonical,
nonidentity prime-order Ed25519 public keys; release uses strict verification.

An error, lost acknowledgement or timeout after sending a mutation leaves its
outcome unknown. Recover using the same store, request ID and fingerprint;
`found:false` only describes that snapshot and does not exclude an in-flight
commit. Do not generate another request ID as a retry. Each list page is its
own snapshot: concurrent new holds can sort before a saved cursor. A null
cursor marks the end of that page's snapshot; oversized responses fail rather
than truncate. Mutations sent as JSON-RPC notifications do nothing.

Holds never expire automatically, and released tombstones remain durable.
Each store permits at most 1024 active holds; exact receipt replay and signed
release remain available at that limit. Released tombstones continue to consume
storage, so operators must monitor growth and abandoned holds using list and
preserve the signing key;
loss of the key needs separate trusted administrative recovery. There is no
automatic protection against restoring or cloning an old database backup.
Direct write access to the database and the broker process remains a trusted
administrative boundary.

### Read-only observer discovery and complete answers

`capabilities` accepts `{}` or `{"include_store_id":true}`. The default response
omits the store ID and has this shape:

```json
{"schema_version":1,"product":"agent-run","version":"0.30.0","source_revision":null,"transports":["cli","mcp","unix_socket"],"limits":{"socket_max_frame_bytes":1048576,"answer_max_bytes":16777216,"answer_chunk_default_bytes":65536,"answer_chunk_max_bytes":131072},"read_methods":[{"name":"answer_chunk","input_schema":{"type":"object"},"result_shape":{"type":"object"}}]}
```

The example abbreviates schemas and the method list. The actual list derives
from every read-only method in the authoritative registry, including call-only
compatibility reads. Each entry contains its exact `name`, full `input_schema`
and registered `result_shape` (null for historical undeclared result shapes).
`version` is the compiled package version. `source_revision` is null because
this build has no verified source identity input; it never guesses a deployed
commit from a checkout or asserts release qualification. The default request
does not read the database; the opt-in reads the current store ID. Neither form
collects provider data, runs inference or looks up Bridge. The CLI equivalents
are `agent-run capabilities` and `agent-run capabilities --include-store-id`,
querying the resident broker.

`answer_chunk` accepts the strict object
`{"agent_id":"...","sequence":1,"expected_sha256":"64 lowercase hex characters","offset_bytes":0,"max_bytes":65536}`.
The positive u32 `sequence` and exact lowercase SHA-256 are required fences,
obtained from the current agent view and verified `answer` metadata. This read
selects the current execution, rejecting a retained older sequence after resume.
`offset_bytes` is required, nonnegative and must identify a UTF-8 boundary at or
before EOF. `max_bytes` may be omitted (65536), accepts 1..131072, and cannot
be null. Missing fences, nulls, extra fields and caller paths are rejected.

```json
{"agent_id":"ag-20261020-120000-0123456789","sequence":1,"sha256":"...","total_bytes":9,"offset_bytes":0,"next_offset":5,"content":"hello","complete":false}
```

Continue with the returned `next_offset`, measured in bytes, preserving both
fences. The end rounds down to a UTF-8 boundary; a budget too small for the next
character rejects. At EOF the content is empty, `next_offset` remains EOF and
`complete` is true; beyond EOF rejects. Each read verifies the entire artifact
and seal using broker-derived paths, owned no-follow descriptors, size, UTF-8
and SHA-256, then revalidates the current execution and recorded proof before
returning. Reads change no lifecycle or durable state. Reconstructed exact
artifact bytes hash to `sha256`; legacy artifacts include their exact terminal
frame in chunks, while historical full `answer` display still strips it once.
Consumers aggregate only on demand, checking sequence, cursors and final hash.

Stable failures are `ExecutionChanged` (sequence changed, including after resume),
`AnswerHashMismatch` (required digest differs), `AnswerUnavailable` (no recorded
answer), `AnswerIntegrityError` (corrupt/missing artifact, invalid UTF-8 or seal),
and `PathEscapeError` (unsafe/no-follow path). These are JSON-RPC `-32000`, except
path rejection is `-32602`. Invalid ranges/boundaries/budgets/arguments use
`ValidationError` / `-32602`. Unknown agents retain `AgentNotFound`. CLI and MCP
preserve the same machine categories. Filesystem/store errors remain typed.

The CLI equivalent is `agent-run answer-chunk AGENT_ID --sequence N
--expected-sha256 HASH --offset-bytes N [--max-bytes N]`.
The existing full answer verification limit remains 16 MiB and the CLI inline
content cutoff stays 1 MiB. Socket serialization counts the entire escaped JSON
envelope and terminating newline. Only a successful `answer` response exceeding
that frame cap falls back to metadata with `content:null` and
`inline_complete:false`, retaining proof and availability. The metadata form is
checked again; other oversized replies keep the existing error. No text is
silently truncated and no wire limit is increased.

`list_pools` accepts a strict object with optional `state` (`"open"` or
`"completed"`; omitted/null means all), `limit` (integer 1..200, default 50)
and `offset` (nonnegative integer, default 0). It returns one consistent read
snapshot, an exact filtered `total`, and `items` ordered by `created_at`
descending with `pool_id` descending as the tie-breaker. `next_offset` is null
at the end; `complete` also holds for an offset beyond the total. Offset pages
may shift if pools are admitted or purged between calls.

Each item has `pool_id`, `state`, a `goal` excerpt capped at 512 UTF-8 bytes
on a character boundary, `goal_truncated`, `created_at` (UTC epoch seconds),
`last_seq` (greatest retained log sequence, zero for an empty log), nullable
`completed_at`, `roster_revision`, `members_count`, `ready`, nullable
`current_proposal_seq`, and `members` in ascending slot order. Member rows
contain only `slot`, `name`, `role`, stable `agent_id` and `tip_status`.
`ready` counts only members whose derived `pool` status has `counts: true`;
raw ready decisions invalidated by a changed roster, tip or failed execution
do not count. Completed summaries reuse the frozen completion evidence even
if a member resumes later. Full criteria, proposal snapshots and log bodies
are available through `pool`, and are absent from discovery.

```json
{"jsonrpc":"2.0","id":2,"method":"list_pools","params":{"state":"open","limit":1,"offset":0}}
{"jsonrpc":"2.0","id":2,"result":{"items":[{"pool_id":"pool-20261004-120000-0123456789","state":"open","goal":"Ship the observer","goal_truncated":false,"created_at":1791115200.0,"last_seq":7,"completed_at":null,"roster_revision":1,"members_count":2,"ready":1,"current_proposal_seq":4,"members":[{"slot":1,"name":"Reviewer","role":"review","agent_id":"ag-20261004-120000-0123456789","tip_status":"running"},{"slot":2,"name":"Builder","role":"implement","agent_id":"ag-20261004-120000-abcdef0123","tip_status":"running"}]}],"total":2,"offset":0,"limit":1,"next_offset":1,"complete":false}}
```

The CLI equivalent is `agent-run pools [--state open|completed] [--offset N]
[--limit N]`, alias `list-pools`; default output is JSON and `--text` prints
the compact MCP page. No worker capability is accepted. Discovery has no
long-poll: operator pool posts do not advance the existing event revision,
so it would miss changes to `last_seq`. Members provide the join with
`list_agents`; `AgentView` remains unchanged.

See [continuations](continuations.md) for native-context `resume`, inherited
authority, idempotency and history availability.

`start.required_constraints` is an optional array of unique policy constraint
names from tool discovery. Omission means no additional requirement. A named
constraint must have enforcement strong enough for that boundary before any
agent row is admitted; advisory evidence and unrelated tool filtering do not
satisfy isolation requirements. Unknown or duplicate names are invalid. The
effective requirement is the union of the role's `required_constraints` and the
request's, for every profile kind; a caller can add a boundary but no role or
request can drop one the other declared.

`start.required_mcp` optionally asserts the already resolved MCP selection:
`{"agent-pool":["pool_join","pool_read"]}`. It accepts at most eight configured
server names and 32 exact unprefixed tool names per server. Names use the existing
1–128-byte ASCII config/tool grammar; duplicate keys/tools, wildcards and null
are invalid. An empty tool list requires that server to be selected. Each named
tool must fit the frozen role/global cap; an unrestricted cap permits any exact
tool assertion, while an empty cap permits none. This check runs before durable
admission or reservation and never selects/injects a server or widens authority.
It does not prove tool liveness. Map and tool order are canonical; omitted and
empty maps have the historical request fingerprint. Exact retained start replay
can succeed after the canonical workdir disappears, without consulting new
configuration or launching another worker. New starts still require live paths.

`agent_integration` accepts only `{"agent_id":"...","sequence":1}`. Sequence
is an optional positive u32 execution receipt; omission pins latest once and an
explicit sequence selects exactly that retained execution. Null, zero and unknown
fields are invalid. Missing executions return `AgentNotFound`. The response is
`{"schema_version":1,"agent":AgentView,"communication":...}` with one of:

- `{"state":"observed","address":"canonical Bridge address","sampled_at_ms":1}`;
- `{"state":"unknown","reason":"not_started|unavailable|identity_changed|terminal|closing|cancelled"}`;
- `{"state":"unsupported","reason":"unsupported_harness|legacy_completion|restricted_role|observer_unconfigured|observer_forbidden"}`.

Each reason is one literal code, not the pipe-separated list. The stable agent
view retains lifecycle, completion, cleanup and frozen MCP selection; its cleanup
process-group ID is null on this privacy-preserving surface. No native session,
store, PID, process token, full birth time or credentials are added. Storage and
integrity errors remain typed errors rather than fabricated observations.

Observation requires a current owned explicit-finish attempt and unchanged
supervisor/native process tokens and full-precision births before and after the
Bridge SDK lookup. The supervisor records actual launch storage; the adapter
records the session on its exact attempt. Parent notification bindings never
identify workers. Restricted research/IPC roles, closing, cancelled and terminal
executions cannot yield an address. A busy observed worker is not necessarily
idle, addressable later, or enrolled in any service.

Set `delivery.bridge.observation_token_file` to a separate absolute protected
credential file explicitly granted Bridge `host_observations`, recipients and
native scopes. Its file must differ from `delivery.bridge.token_file`; omission
does not borrow delivery authority. The Bridge-configured harness namespace owns
the returned address, independently of the parent delivery namespace. This read
is operator/service-only and absent from worker capabilities.

`request_id` replay is scoped to the caller namespace in the original request.
A later PostToolUse notification binding does not change that identity. Clients
that omit `orchestrator` share the unbound namespace across fresh connections.

`list_agents` accepts optional `after_revision` and `wait_seconds`. When the
current event revision is not newer, the call waits up to 60 seconds and wakes
as soon as a committed event advances it. The returned `revision` becomes the
next cursor, so terminal completion is observable without a notification
worker or polling at a fixed interval. Pages also return a
`message_revision` transcript watermark: journal rows (transcript text and
native tool counts) never advance the event revision, so an observer that
needs to wake on progress passes it back as optional `after_message_revision`
together with `after_revision`. Journal-only wakes are paced to at most one
per second per waiting follower; event wakes stay immediate.

The CLI equivalent is `agent-run agents [--active] [--offset N] [--limit N]`,
and `agent-run agents --follow` runs one persistent process instead of
respawning polls: it prints the first page immediately and then one NDJSON
snapshot per meaningful change (status, phase, name, usage, tool counts,
failure, delivery and answer facts), never reprinting a page whose only
movement is observation time. Ctrl-C ends the viewer only; supervised agents
keep running, and a closed output pipe terminates the process.

Durable steering outcomes are truthful about evidence. `steer` still queues
one command and returns the same acknowledgement; the command's recorded
outcome distinguishes a correlated native reply (`accepted:true` — the
engine accepted the input, which is not a claim the model consumed it), a
correlated native rejection (`accepted:false`, `native_rejected`), a refusal
to send under bounded backlog pressure (`accepted:false`,
`backlog_pressure_unsent` — provably nothing was written), and every bounded
end after a possible write (`accepted:null` with the finite reason, e.g.
`uncertain_timeout` or `uncertain_backlog_pressure` — the input may have been
taken). Interleaved engine notifications are never dropped to make room for
a control exchange: retention is fixed and bounded, a full backlog stops the
exchange before reading, and a late correlated reply is recorded as
metadata only.

`start` and `resume` accept optional `display_name` (CLI `--name`, alias
`--display-name`): a trimmed, nonblank UTF-8 human label of 1–64 Unicode
scalar values, with no control or unsafe directional formatting. Punctuation
and non-ASCII text are accepted. Omitted or null start labels mean unnamed;
omitted or null resume labels inherit the parent, and an explicit label replaces
it. The normalized label participates in request-id replay: changing it with the
same key returns `Conflict`. Public agent views expose it as nullable `name`;
labels confer no authority and are never inferred from task text.

Views expose nullable `usage` from the latest execution's existing `run_stats`
row and `usage_cumulative` across lineage executions. Missing or pruned history, missing rows or
unreported metrics remain null; an explicitly observed zero stays zero. Each
cumulative metric is available only when every lineage execution reported it.
Usage objects contain measurements, source and recording time, never internal
execution IDs. Codex resumes record a comparable parent-thread baseline before
launch and subtract it from native cumulative counters; absent, foreign-session,
foreign-model or decreasing counters remain unknown. Codex turn counts are null
unless the native protocol reports a counter, never inferred from messages,
turn IDs, tools or execution count. Compact MCP output uses `?` for unknown
measurements and names incomplete lineage evidence.

`transcript` accepts optional representation options while keeping raw rows
and forward `cursor`/`limit` semantics unchanged. `view` is `raw` (default) or
`blocks`: blocks group only consecutive journal rows that share one known
native reference (`raw_ref`), role, name and one execution/attempt scope, so
streamed fragments of one native message read as one entry with exact
whitespace and content, `first_seq`/`last_seq`, and a `fragment`-safe
`next_cursor`/`resume_cursor`. Rows without a native reference never merge,
and equal references of different executions stay separate; a `starts_block`
flag marks safe boundaries without exposing run or attempt IDs.
`tail_blocks: 1..=200` returns the last blocks chronologically with an
exclusive `previous_cursor` (pass it as `before_cursor` for older pages);
blocks are the paging unit, so a limit bounds blocks, never fragments inside
one. Reads are bounded index scans: no whole-journal load, and no raw history
or spool file is modified. Oversized or spooled content is never silently
dropped — `content_complete: false` and `omitted_bytes` say what is missing,
and `raw_ref` stays opaque.

Transcript rows and agent views also carry native tool evidence. A
`tool_result` row may carry `error` (boolean) with `error_source` naming the
allowlisted native field that supplied it: Claude `is_error`, Codex command
`exitCode`/`status`, or Codex MCP `status`/`error`. Only explicit native
markers set the flag; error-shaped words, agent exit codes and unrelated
statuses never do, and unreported results stay `null` (unknown), distinct from
an observed `false`. Agent views include nullable `tool_counts` for the latest
execution — `calls` counts unique native invocation IDs (started, completed
and fragment rows of one invocation count once), `failed` counts explicitly
failed invocations and stays `null` while any result is unknown, and
`unknown_results` counts results without consistent evidence. Executions
recorded before the versioned native observers, or with observed coverage
gaps, report all counts as `null` rather than a fake zero.

Agent views returned by `list_agents` include
`effort` — the reasoning effort requested at launch, or `null` when the
request did not set one.

Those views also include nullable `cleanup` evidence from the latest owned
process cleanup observation: attempted `signals`, `scope`, `group_gone`,
nullable `descendants_gone`, `confirmed`, and nullable `process_group_id`.
Confirmation requires the original group and the readable pre-signal owned set
to be gone. Page projections resolve progress, warnings, delivery evidence and
cleanup in one batched state query.

New rows also expose immutable `policy` evidence with the runtime/platform and
one entry for every known constraint: actual enforcement, support, whether the
caller required it, exact scope, and reason. Historical rows return `null`.

With schema 2, `capacity_order` accepts an optional exact `model` filter and
returns provider-only advice: `schema_version`, `config_revision`,
`capacity_revision`, `ranked_at`, account-free `quota_coverage`, and ordered `providers`. Each provider has its
id, `priority_multiplier`, nullable `score`, and models with `native_model`
and Quota-provided `quota` standing. It does not expose accounts or legacy physical-route
aliases. An unknown model is a `ValidationError`. The view uses one newly fetched
Quota response and one separate local read without collecting quota, reserving
capacity or starting work. `quota_coverage.evaluated_at` is the Quota score's
source clock, while `ranked_at` is the local projection time; local revision
equality does not establish score freshness or cross-service atomicity.

The CLI equivalent is `agent-run capacity order [--model MODEL]`. Select a
compatible model and canonical role using `delegation_guide`; `models` is a
structured compatibility read for existing clients.
Schema 1 returns `Unsupported` and suggests migration to schema 2.

Three methods exist only on this socket transport: `tools` and `ping` above,
plus:

- `wait` — params `{"agent_id": "...", "timeout_seconds": 240}` (timeout
  optional, positive number; omitted = wait forever). Blocks until the
  agent is terminal, then returns the answer envelope (same shape as the
  `answer` tool). If the watcher timeout expires first, the result is a
  normal reply carrying `"timed_out": true` and the current status — not
  a JSON-RPC error.
A pending `wait` does not block other requests: run it on its own
connection and keep issuing calls on another.

## Typical integration loop

Open a Unix stream socket in the client language, write one compact JSON-RPC
object plus `\n`, and read one reply line. A shell smoke can use BSD netcat:

```bash
printf '%s\n' '{"jsonrpc":"2.0","id":1,"method":"ping"}' \
  | nc -U ~/.agent-run/api.sock
```

For asynchronous work, call `start`, retain the returned `agent_id`, then call
`wait` on a separate connection. If `wait` returns `"timed_out": true`, the
agent is still running and the same id can be waited on again.

Notes for the loop:

- `start` returns immediately with a durable `agent_id`; the agent runs
  detached and survives your process.
- The returned agent view is a snapshot, not a promise of `starting`: a fast
  bootstrap failure may already be terminal. Match concurrent results by agent
  or request ID rather than submission order.
- Bound Codex/Claude chats receive completion notices automatically when
  delivery is configured. The MCP `start` description includes the shared
  notice format and handling contract; `agent-run doc completion` (or MCP
  `doc` with `{"topic": "completion"}`) serves the same contract. A notification ID is any nonblank
  string of at most 512 UTF-8 bytes, and delivery attempt evidence accepts only
  its declared fields. The `wait`
  example above is for an unbound API caller, not a bound-chat polling loop.
  Successful members of an open pool are represented by its single common pool
  completion notice; failed or otherwise non-successful members still receive
  individual completion notices.
- The one-shot CLI `agent-run start` submits through this resident socket too;
  it never owns an in-process start worker that would die with the CLI. A down
  daemon is reported as `BrokerUnavailable` instead of falling back locally.
- CLI `start --wait` repeatedly uses the private socket `wait` method and emits
  its terminal answer; interrupting that client leaves the durable run active.
- Use `limits` for available Quota windows, percentages, reset times and
  freshness. Schema 2 adds `ranking` and `coverage` (`state`, `incomplete`,
  nullable `evaluated_at`); unreported quota is unknown when coverage is
  incomplete. Quota unavailability produces unknown coverage, not a confirmed
  empty history. `observed_at` equals the local `ranking.ranked_at`; score
  evaluation uses Quota's separate clock. At most 1000 rows are returned and
  truncation marks coverage incomplete. MCP omits account/pool identities and
  refuses a page over its whole-response budget. Schema 1 returns `Unsupported`
  with migration guidance. Historical storage and low-level latch code do not
  supply these schema-2 reads; the
  supervisor no longer writes native failures to that latch.
- `models` and `capacity_order` retain their structured compatibility responses
  for existing CLI/socket clients, without joining the advertised MCP catalog.
- `delegation_guide` (optional exact `provider`, `model`, `profile` filters;
  schema 2 only) returns one compact plain-text
  routing guide — providers in capacity order with each exact model id, cached
  quota standing, admissible profiles, params, restrictions, and configured
  guidance prose — instead of reading the full `models` JSON just to pick a
  route. Its result is a JSON string here and real MCP text content on the MCP
  transport; the CLI equivalent is `agent-run delegation-guide` with optional
  `--provider`, `--model`, and `--profile` filters.
  Unknown filters are `ValidationError`; omitted filters keep default guidance.
  Before delegating a task, the orchestrator must call it and read the result
  before choosing provider, model, effort or profile. Routing advice belongs in
  provider/model `recommendations`, not a separately maintained delegation skill.
  The tool does not select a model, authorize new access, or replace the
  start/resume completion and permission contracts.
- `answer` re-fetches a finished agent's result any time later by id —
  results are durable, a dropped connection loses nothing.
- `start.write` is a compatibility intent flag; the selected canonical role
  owns actual write permission. Choose a role with `write = false` for read-only
  work: request `"write": false` is not an independent read-only guard, and
  `"write": true` cannot grant writes beyond the role.

## Errors

| Code | Meaning |
|---|---|
| -32700 | unparseable line, or line over 1 MiB |
| -32600 | not a JSON-RPC 2.0 request; batch array; bad `id` |
| -32601 | unknown method |
| -32602 | invalid params (message carries the validation detail); a more specific class such as `PathEscapeError` is kept in `error.data.code` |
| -32000 | domain error; `error.data.code` holds the agent-run error class (e.g. `AgentNotFound`, `selection_busy`, `no_eligible_account`, `quota_exhausted`), plus context fields |
| -32603 | internal error (bounded message, details in server log) |

Treat `-32602`/`-32000` as actionable (fix the request / the referenced
id); `-32603` as a bug to report. The CLI (`error.type`) and MCP tool
errors report the same class the broker returned; only an unknown class is
rendered as `RuntimeError`.

## Versioning and compatibility

- The tool surface is pinned to the MCP surface by a parity test; new
  tools appear in both transports simultaneously. Re-read `tools` after
  an agent-run upgrade instead of caching schemas across versions.
- Restart `api serve` after switching the verified sealed release at
  `~/.agent-run/standalone/current`.
- The current database schema is version 40, reached through the paired
  `agent-run config migrate`. Older resident processes refuse a newer database
  and must be restarted after an upgrade migrates it.

Linux provider launches for Codex and Claude record an attempt launch journal
and kernel incarnation. Launch correlation is not a credential or managed
Pool authorization: managed enrollment remains unqualified until the separate
role, admission and real-provider checks are complete. A spawning record left
without a bound leader is unresolved; recovery must not infer that no child ran.

### Exact pinned transcript export (v1)

`transcript_exact` is a read-only public tool on the same CLI/socket/MCP registry.
`capabilities.read_methods` publishes its input and output schemas. The CLI reads
one page with `agent-run transcript-exact --spec request.json` (`--spec -` uses stdin).
MCP returns the same complete JSON result in `structuredContent`, with a fixed text
label; transcript text is not interpreted or rendered as instructions.

```json
{
  "version": 1,
  "expected_store_id": "store_0123456789abcdef0123456789abcdef",
  "selector": {
    "agent_id": "ag-20260826-120000-0123456789",
    "sequence": 1,
    "execution_ref": "ex1_0000000000000000000000000000000000000000000000000000000000000000",
    "pin_id": "pin_00000000000040008000000000000000"
  },
  "cursor": "0",
  "offset": 0
}
```

The identities above illustrate syntax; use the real receipt and active pin.
Each call verifies store identity, exact execution incarnation, active pin and
complete lineage/attempt coverage through the existing status proof, in the same
SQLite read snapshot used for message bounds and bytes. It reads only that selected
execution. Wrong or unknown evidence returns no content. Pins prevent ordinary
retention, not arbitrary database tampering. No filesystem spool is expanded.

The first response captures `upper_seq`, the selected execution's maximum message
sequence in that snapshot (or `"0"` for an empty transcript). All global message
sequences/cursors are canonical decimal **strings**, bounded by `9223372036854775807`.
They must never pass through JavaScript `Number`. Later calls must echo `upper_seq`;
new messages and later resume executions remain outside this export.

Every response repeats `store_id`, `selector`, `cursor`, `upper_seq`, and `offset`,
with `message`, `next_cursor`, `next_offset`, and `snapshot_complete`. `message` is
null only at the empty/end boundary. Otherwise it contains:

| Field | Meaning |
|---|---|
| `seq` | Decimal string identifying this selected execution's immutable message |
| `sha256` | SHA-256 of the exact complete serialized UTF-8 JSON message |
| `total_bytes` | Complete serialized byte length |
| `start`, `end` | Inclusive start and exclusive end byte offsets |
| `text` | At most 32 KiB of the serialized JSON, split only at UTF-8 boundaries |
| `complete` | This fragment reaches the end of that one message |

Assemble `text` bytes in order; check offsets, identity, hash and length on every
page, then verify the full SHA-256 before parsing the JSON. Its v1 ordered fields
are `seq` (string), `at`, `role`, `name`, `content`, `raw_ref`, `error`,
`error_source`, and `content_complete` (always true). Optional stored fields remain
null. `raw_ref` is opaque historical data, never a path to open automatically.

Continue with `cursor = next_cursor`, `offset = next_offset` and the unchanged
`upper_seq`. For a nonzero offset, also send `expected_sha256 = message.sha256`;
otherwise omit that field. The cursor advances only when that whole message ends.
An omitted watermark requires cursor `"0"` and offset 0. Null options, ambiguous
numeric spellings, non-UTF-8 offsets, missing cursor/watermark rows and digest
changes are refused. One message may require many calls. Source content and
serialized JSON are independently limited to 16 MiB; each metadata string to
4096 bytes. Limits reject rather than truncate. Responses reserve envelope space
within the shared 1 MiB socket/MCP frame budget.

`TranscriptExportAborted` means stop: no fragment from that call is usable. In
particular, `content_complete=false` or null aborts at the first such row; old
spools have no stored content digest and cannot establish full content. A released
pin aborts the next read snapshot. Already-returned pages remain historical partial
data. `StoreIdentityMismatch` separately reports the wrong store; malformed
continuations report `ValidationError`.

Only after collecting every message from cursor `"0"` through a response with
`snapshot_complete=true` may a client call the captured export complete. That flag
alone is not proof the client collected preceding pages. It never proves execution
completion, success, present filesystem contents or authority. The empty captured
snapshot is explicit (`message=null`, watermark/cursors `"0"`, completion true),
and stays empty when its watermark is echoed after later appends.

### Exact initial task (v1)

`initial_task` is a read-only public tool on the same CLI/socket/MCP registry
(one dispatcher entry). It returns the exact initial task text of one retained
**root** execution (sequence 1), in verified pages, or a typed `unavailable`
state. It never reconstructs a system prompt, a Pool binding or any text from
another string, never truncates, and does not change retention. The CLI reads one
page with `agent-run initial-task --spec request.json` (`--spec -` uses stdin).
MCP returns the same complete JSON in `structuredContent` with a fixed text
label; the task is data, not instructions or authority.

**Capability.** `capabilities` with `{"include_initial_task":true}` (CLI:
`agent-run capabilities --include-initial-task`) adds
`"initial_task":{"version":1,"method":"initial_task","modes":["strict","current"],"max_task_bytes":524288,"max_page_bytes":65536}`.
Without the flag the key is absent, like `lineage_holds`; `initial_task` also
appears in `read_methods` with its schemas. A build that predates the method
rejects the unknown flag and lacks the read method.

**Request** (strict object; omit optional fields, explicit `null` is refused):

| Field | Meaning |
|---|---|
| `version` | exactly `1` |
| `mode` | `strict` or `current` |
| `expected_store_id` | database identity fence; required for `strict`, optional for `current` |
| `agent_id` | canonical root agent ID; a resume alias is never normalized |
| `sequence` | `strict`: required; `current`: optional. Only `1` can be available |
| `execution_ref` | `strict`: required `ex1_…` receipt of the first provider attempt; `current`: optional per-page identity fence |
| `pin_id` | `strict`: required active history pin; `current`: rejected |
| `offset` | UTF-8 byte offset of the page, default 0, must be a character boundary |
| `max_bytes` | page budget 4..=65536, default 65536; the page ends at a character boundary |
| `expected_sha256`, `expected_total_bytes` | whole-text fences from an earlier page; both or neither; required for a nonzero `offset` |

**Strict mode** proves, in the same SQLite read snapshot as the bytes: store
identity, canonical root/self (`id` = root, sequence 1, no parent), the first
attempt's computed `execution_ref`, an **active** pin of this root, and
*complete* lineage/attempt coverage (the `status` proof: every retained
execution and attempt is continuous with verified provider-v2 identity). **Current mode**
reads the currently retained root without a pin: it makes no pin, coverage or
latest-ever claim, and an unpinned result proves nothing about an older Pool
binding or system prompt. It still checks store (if given), canonical root, first
provider-v2 attempt and ref (and the client's `execution_ref` fence, if any).

Both modes require `agents.task`, `request_json.task` and the frozen provider-v2
task (the replay-proven `provider_request.task`) to be byte-identical text of at
most 512 KiB (524288 bytes), NUL-free and not blank.

**Result.** `state` is `available` or `unavailable`:

```json
{"state":"available","version":1,"mode":"strict","store_id":"store_0123456789abcdef0123456789abcdef",
 "agent_id":"ag-20261109-120000-0123456789","sequence":1,
 "execution_ref":"ex1_0000000000000000000000000000000000000000000000000000000000000000",
 "pin_id":"pin_00000000000040008000000000000000","total_bytes":70000,
 "sha256":"<64 lowercase hex of the whole text>","offset":0,"end":65536,"next_offset":65536,
 "text":"…first 65536 bytes…","complete":false}
```

`pin_id` appears only in `strict`. `total_bytes` and `sha256` describe the whole
text and are identical on every page; `next_offset` is `null` on the last page
(`complete: true`, `end == total_bytes`). Pages are split only at UTF-8
boundaries, so a page may be shorter than `max_bytes`.

```json
{"state":"unavailable","version":1,"mode":"strict","store_id":"store_0123456789abcdef0123456789abcdef",
 "agent_id":"ag-20261109-120000-0123456789","reason":"pin_released"}
```

`unavailable` is a successful result, never accompanied by text, and `reason`
is one of:

| `reason` | Meaning |
|---|---|
| `not_found` | no retained execution has this ID (absent, or removed with no pin tombstone) |
| `retention_deleted` | `strict` only: the pin tombstone exists but retention removed the lineage |
| `not_root_execution` | not a canonical root, or `sequence` is not 1 (resume executions are never exported) |
| `execution_ref_unavailable` | no usable first provider attempt to derive a ref |
| `execution_ref_mismatch` | the supplied ref differs from the computed ref |
| `pin_missing`, `pin_released`, `pin_mismatch` | `strict` only: pin absent, released, or owned by another root |
| `coverage_incomplete` | `strict` only: lineage/attempt continuity is not proven complete |
| `task_missing` | a task source is absent, not text, blank, unparsable or beyond the read budget |
| `task_mismatch` | the three task sources disagree |
| `task_too_large` | a task source exceeds 512 KiB |
| `provider_identity_invalid` | the frozen provider-v2 identity does not verify |
| `content_changed` | the supplied `expected_sha256` or `expected_total_bytes` differs from the stored text |

**Errors** stay typed on every transport: `ValidationError` (malformed request,
`null`/unknown fields, a missing strict fence, a bad `offset`/`max_bytes`, an
offset inside a character or past the text), `StoreIdentityMismatch` (a wrong
`expected_store_id`; no content, and not an `unavailable` result), and the usual
`StorageError`/`IOError`/`RuntimeError` classes.

**Client protocol.** Start at `offset` 0 without fences. Buffer each page's
`text`; continue with `offset = next_offset` and the first page's `sha256` as
`expected_sha256` and `total_bytes` as `expected_total_bytes`, every time. Check
that each page's `offset` equals the bytes buffered so far and that `store_id`,
`agent_id`, `execution_ref` (and, in `strict`, `pin_id`) never change; cancel on
any change or on a session change. Only after `complete`, verify the buffered
byte length and the SHA-256 of the whole text; use nothing before that. Any
`unavailable` page (including `content_changed`) discards the buffer: earlier
pages are partial historical data. Each page is its own snapshot; the fences,
not a retained snapshot, bind the pages together.

Example `strict` request:

```json
{"version":1,"mode":"strict","expected_store_id":"store_0123456789abcdef0123456789abcdef",
 "agent_id":"ag-20261109-120000-0123456789","sequence":1,
 "execution_ref":"ex1_0000000000000000000000000000000000000000000000000000000000000000",
 "pin_id":"pin_00000000000040008000000000000000"}
```

Example `current` continuation (second page):

```json
{"version":1,"mode":"current","agent_id":"ag-20261109-120000-0123456789",
 "execution_ref":"ex1_0000000000000000000000000000000000000000000000000000000000000000",
 "offset":65536,"expected_sha256":"<sha256 from page one>","expected_total_bytes":70000}
```

The identities above illustrate syntax. A page is bounded to fit the shared 1 MiB
socket/MCP frame even with worst-case JSON escaping. No SQLite schema change.
