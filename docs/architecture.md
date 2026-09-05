# agent-run architecture

How the pieces fit, as shipped today. For the operator's how-to see
`agent-run doc`; for API integration see [api.md](api.md); for workflow
scripts see [workflows.md](workflows.md).

## The shape

```
transports          CLI (cli.py)   MCP stdio (mcp.py)   JSON-RPC socket (api_socket.py)
                          \               |                see docs/api.md
one tool surface           `──────  dispatch.py  ──────'
                                        │  TOOLS table + call_tool
domain facade                     service.py (AgentService)
                                        │
durable state              state/  (versioned SQLite schema, migrations)
                                        │
engine drivers          adapters/  ──  supervisor  ──  detached children
                     codex · claude · glm · qwen · opencode
```

MCP is a thin stdio proxy forwarding `tools/call` to the resident Unix-socket
daemon. The daemon is the single launch host, closing the nested-sandbox
failure mode; a proxy without store access survives release switches.

Three transports expose one dispatcher. A tool added to
`dispatch.TOOLS` appears in all of them; parity tests fail otherwise.
`AgentService` is the only door to state and adapters — transports never
touch the store or an engine directly.

## Durable agents

`start` validates and durably admits the request as `starting`, then returns its
agent id before authentication, materialization, adapter preparation, spawn, or
READY. A bounded worker with its own thread-affine store performs those slow
steps and launches a **detached supervisor process** that owns the child engine.
The accepted preparation lease is finite. Immediately before spawning, the
coordinator atomically transfers its live claim into a bounded handoff window
covering READY and failed-launch cleanup. Once the supervisor records its
ownership proof, the preparation deadline no longer invalidates that proof.
An abandoned handoff still expires as `lost` and cannot revive the agent later.
Payload writes share the READY deadline and honor cancellation even when the
child stops reading the pipe.
On supported POSIX systems the launcher uses `posix_spawn(..., setsid=True)`;
the legacy fork path is only a compatibility fallback when session-creating
spawn is explicitly unavailable.
From that point the orchestrating process is optional: state transitions
(`starting → running → succeeded/failed/timed_out/cancelled/lost`) are
recorded as events; transcripts are journaled as messages with large
payloads kept as file references.

The supervisor enforces:

- **timeouts** — with a configurable warning to the child at 90% asking it
  to summarize what is done and what remains;
- **stall detection** — a child silent on its output stream past
  `core.stalled_after_seconds` (default 900) is killed and classified
  `stalled`, distinct from a timeout;
- **cancellation** — kills the process tree, not just the first child;
- **outcome classification** — the terminal status is derived from
  recorded evidence (result payloads, completion sentinels, error-only
  answer detection), never from exit code alone.

Answers are stored with size and sha256; `answer <id>` re-serves the
verified envelope indefinitely.

Codex assistant deltas are journaled as transcript chunks as they arrive.
Completed items contribute only text not already streamed. These partial
messages do not create an answer proof or imply success; an interrupted or
timed-out run can therefore have a useful transcript without a complete answer.
Unexpected app-server EOF is a transport failure, not an indefinitely silent
session. Initialization and native interruption follow the
[Codex app-server contract](https://learn.chatgpt.com/docs/app-server).

An owned engine may exit before process-group discovery. This is not an
automatic failure or success: completion still requires the real engine outcome,
answer proof, and absence of both the leader PID and live members of its expected
group. Without verified ownership, neither native cancellation nor group signals
are used; a surviving or reused process identity fails closed.

## Adapters

One package per engine under `src/agent_run/adapters/`. An adapter renders
a **fully generated child home** — settings, auth wiring, declared skills,
declared MCP servers, hooks, plugins — so nothing ambient leaks into the
child. What the adapters drive:

| Runtime | Engine process | Notes |
|---|---|---|
| `codex` | `codex app-server` (stdio JSON-RPC, one-shot) | sandboxed; external read roots supported on read-only runs |
| `claude` | `claude` CLI headless | `--setting-sources ""`, per-run plugin dirs |
| `glm` | `claude` CLI pointed at Z.ai's Anthropic-compatible endpoint | subclass of the claude adapter; auth via env/keychain, base URL pinned in the adapter |
| `qwen` | `qwen -p … --output-format stream-json --sandbox` | headless one-shot; approval mode maps to write/read-only; macOS uses Xcode's real Git binary instead of the sandbox-hostile `/usr/bin` shim |
| `opencode` | managed long-lived `opencode serve` HTTP service | the only runtime with a managed service (`agent-run service start`) |

Auth is declared per runtime as env-var **names** or file links — secret
values never appear in config. On macOS, adapters fall back to Keychain
lookups where configured.

## State

Single SQLite database at `<home>/state.db`, `PRAGMA user_version = 10`.
Main tables: `agents`, `attempts`, `events`, `messages` (transcripts),
`commands` (steer/cancel outbox to supervisors), `orchestrator_sessions`,
`deliveries`, immutable `delivery_attempt_evidence`, `capacity_samples`,
`capacity_route_snapshots`,
`workflow_runs` / `workflow_steps` / `workflow_deliveries`, `run_stats`,
`context_receipts`.

Schema changes ship as numbered migrations (`state/migrations/`) with a
pre-migration backup; components version-check and refuse to run against a
newer schema rather than corrupt it.

SQLite connections are **thread-affine** and the code treats that as law:
a store is used only on the thread that created it (the socket API runs a
dedicated dispatch thread for exactly this reason).

## Orchestrator binding and delivery

An agent started by an MCP session (or with explicit `--session-*` flags)
is **bound** to that orchestrator session. On terminal state, a delivery
row is created and a dispatcher pushes the completion notice back to the
orchestrator's chat (the relay-backed `codex_queue` compatibility identifier
and Claude UDS transports exist).
Unbound runs create no delivery row — `wait` on them is the delivery.
Deliveries retry with backoff and expire instead of retrying forever.
Each Codex delivery attempt records an immutable bounded evidence row in the same
transaction that completes, retries, or fails its owned delivery claim. The
record distinguishes exit status (including 127), spawn errno, timeout, session
loss, and success while storing no message, session id, argv/environment value,
or credential. Status exposes only the latest validated safe summary.

Codex Desktop delivery uses a volatile local relay. With both
`CODEX_APP_TOOLS_PIPE_PATH` and `CODEX_MCP_NODE_PATH` supplied by the host, the
MCP CLI replaces itself with the host's signed Node executable. That wrapper
owns a private Unix socket and a thin Python MCP child; the child receives
neither host capability, preventing recursive wrappers. The wrapper calls only
`send_message_to_thread` and renders the same structured completion notice from
validated lifecycle fields, immutable runtime/model/effort selectors, and an
optional bounded failure category. Task, answer, and runtime error prose never
enter the notice; metadata is escaped for safe single-line display. Failure
reasons and recovery advice come from a package-owned allowlist keyed by that
category. Effort is the explicit value persisted in the launch request, not an
inferred runtime default; missing effort is `unspecified`.
Host tool inventories have an 8 MiB frame limit;
local delivery requests remain bounded to 8 KiB. No socket path, host response,
or message text enters delivery evidence.

Agent completion notices use the `agent-run/completion` header and a short list:
ID, terminal status, optional Failure/Advice lines, `runtime/model:effort`, and
notification identity. Failure/Advice appear only for failed, lost, and timed-out
agents. Python and the Node relay render one packaged template. Handling instructions live in
the same contract, exposed by the MCP `start` description and `agent-run doc
completion`; they are not repeated in each notice. The contract explains
asynchronous launch, bound delivery, result retrieval, and why a completion
notice is neither a new task nor user approval. Host-added trust warnings remain
under the host's control. Workflow notices keep their separate format.
The local relay protocol accepts strict legacy v1 requests, selector-bearing v2,
and failure-aware v3. A current host advertises `ar-cdx-v3-*.sock`; clients
prefer v3, then v2, and use legacy requests for older hosts. Old clients send
their v1 shape to a v3 host and remain compatible. Existing
outbox rows require no migration. Already-running old MCP hosts keep delivering
their old format until the `agent-run` MCP connection is restarted.

Codex completion delivery uses only the signed Desktop relay. The persisted
`codex_queue` name is a compatibility identifier; it never invokes the Codex
UI queue or requires a queue executable. Missing or rejected relays are
retryable; unknown acceptance after transmission remains ambiguous and
retryable. Relay discovery has a ten-second total budget and the host call
has an eight-second budget, within the existing thirty-second lease.
The Node wrapper preserves MCP stdio and removes its socket on child exit.

## Workflows

A restricted Python script (AST-guarded: five names, no imports/IO) runs
in a detached runner; every `agent()` step goes through the same
`AgentService.start`, so limits, profiles, and permissions apply
unchanged. Steps are journaled with their spec hash; `workflow resume`
replays a failed/lost run under the same id, serving completed steps from
the journal cache and re-running only the broken tail. `batch` generates
the one-phase parallel script for you.

## Capacity and limits

A collector (`agent-run capacity collect`, launchd-schedulable) samples
remaining quota per runtime through a pluggable per-runtime source:
`native` engine data, a short-lived Codex app-server, the `codexbar` CLI,
a local OmniRoute router, or `none`. A collection stores samples and its
explicit physical-pool/route topology atomically. Per-account scopes refresh
independently, so a failed account keeps its previous topology only until that
snapshot expires instead of deleting healthy sibling scopes. Samples carry
validity windows; `limits` serves projections with burn-rate–based exhaustion
risk per lane, worst first, hiding nothing.

Collection outcomes distinguish `collected`, `partial`, `failed`, `no_data`,
and `unsupported`. A source failure is not a successful collection of zero
samples. Successful account slices remain independently committed when a
sibling probe or write fails; reported sample counts reflect those commits.
`capacity collect --once` still prints its JSON report, but exits with status
2 for partial, failed, or empty supported-source collection. Logs carry safe
reason codes and counts, never provider response bodies or raw child stderr.
Sources retain their bounded call deadlines; a round exceeding the configured
collection interval is explicitly warned about.

Freshness uses source observations, not the time an old payload was fetched
again. Future observations and windows whose reset has arrived are unknown.
Diagnostic snapshots select the newest row per quota identity before applying
their result cap, so a busy account cannot hide a stale sibling through repeated
samples. Stored sample history retains its separate, global retention bound.
Reset-cycle grouping tolerates up to one second of reporting jitter only when
both reported resets were still in the future at the latest observation.
Stored timestamps and the reported latest reset remain unchanged; a reset
that already passed is not merged into the next cycle.

OmniRoute quotas come from its current `key_value` cache under the
`providerLimitsCache` namespace, using `fetchedAt` as the observation clock.
Its `quota_snapshots` table records changes, not every successful poll, and
therefore cannot establish freshness for unchanged quota values. Active,
quota-visible members with missing or malformed current evidence fail the
collection rather than silently disappearing from the pool average. Only
quota percentages and timestamps cross the Docker reader boundary; cache
messages, plans, credentials and connection identifiers are not emitted.
OmniRoute pool freshness is limited by its oldest included member; future
observations and expired resets invalidate the whole window, never just remove
the inconvenient member from the mean. Its quota query detects overflow of the
64-row bound rather than silently returning a truncated, apparently healthy
pool. Upstream snapshot timestamps and the source's shelf life remain intact.

For Claude's native usage source, a usable Keychain token is read without a
refresh. Missing or expired credentials trigger the adapter's existing Claude
CLI renewal once, bounded to 60 seconds, followed by a validated Keychain
reread. Failure stays an explicit collection failure. A declared, explicitly
exported OAuth token remains authoritative and is never replaced by this
renewal path; API keys are not treated as OAuth credentials.

Account labels are opaque: a labelled account cannot collide with the absent
account even when its label is `base`, `default`, or `shared`. Provider-scoped
identifiers encode that distinction without changing the original sample keys.
For Codex, a malformed present quota window disables that bucket's route;
valid sibling samples remain advisory evidence, never proof that the unknown
governing limit does not exist.

Capacity route ranking is a pure read of those snapshots. Every governing
window must be fresh and known; a zero window is omitted before scoring.
Evidence spanning at least one hour projects remaining capacity to reset,
while warmup/thin/no-reset evidence uses a remaining-percent fallback centered
at 50%. The worst window defines a nonnegative route score, then a positive
runtime multiplier scales its priority. Optional account and quota-lane weight
maps override that default, with account taking precedence over lane. Weights
are absolute replacements, not products. Concrete account/model aliases sharing
the same runtime and physical pool set remain one capacity choice, with the
highest applicable alias weight and preferred selector first.
`agent-run capacity order` exposes that same read-only, role-independent order:
its first route is highest priority, while the orchestrator still chooses a
compatible role/model alias and decides whether to launch. The output retains
deferred evidence, exhausted omissions, unavailable runtimes, and the
`insufficient_diversity` signal alongside the working routes.

The context hook uses the same capacity-order builder and injects a compact
English Runtime priorities summary, not raw quota windows. It instructs the
orchestrator to choose the first compatible route while retaining role/model
selection. A model-specific quota lane cannot lend its priority to a different
model on the same runtime/account. Per-session component receipts suppress unchanged visible summaries,
including when only active-agent context changes; a later return to a previous
summary is delivered again. Visible rounded priorities and route identities
determine changes, not observation timestamps or insignificant float tails.
The diagnostic `limits` view is not a required routing step.
Run-level usage (tokens, ttft, cost estimate) lands in `run_stats` at
terminal, with an idempotent `stats backfill`.

## Releases

The runtime deploys as a **sealed release**: a venv built from a git SHA
under `~/.agent-run/standalone/releases/<sha>` with a `COMPLETE` marker,
selected by the `standalone/current` symlink. Rollback is repointing the
symlink; retention keeps releases that live sessions still execute from.
Details: `agent-run doc releases`.

## Design invariants

1. Fail-closed configuration: unknown keys are rejected; a typo cannot
   silently widen permissions.
2. Isolation by construction: children see only what config declares.
3. Evidence over optimism: terminal states are derived from recorded
   facts; fabricated success must be structurally impossible.
4. One dispatcher, many transports; parity is tested, not promised.
5. Standard library only; the supervisor of other people's agents should
   not have a supply chain of its own.
