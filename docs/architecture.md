# agent-run v1 architecture

Status: approved for implementation after the three runtime-isolation canaries in
the acceptance section. This document is the implementation contract for the new
standalone project in `/Users/pluto/projects/agent-run`; it does not modify the
legacy implementation in `/Users/pluto/projects/crew`.

## 1. Outcome

`agent-run` is a local asynchronous supervisor for external coding-agent
runtimes. It owns configuration, runtime isolation, durable state, process
lifecycle, capacity intelligence, orchestration-session binding, and completion
delivery. A caller starts an agent, receives a durable `agent_id`, and may exit;
the agent continues under its own supervisor.

The smallest v1 uses:

- Python 3.11+ and the standard library;
- one SQLite database as the authoritative metadata, transcript, command, and
  delivery store;
- private per-agent directories for large immutable runtime streams and
  artifacts;
- one detached supervisor process per active agent;
- one short periodic capacity collector launched by the OS scheduler;
- one on-demand completion-delivery dispatcher while notifications are pending;
- thin CLI and MCP adapters over one service API.

There is no resident central daemon. SQLite is the cross-process bus. This keeps
the proven property that active agents survive CLI/MCP restarts without adding a
new socket protocol or a single in-memory owner.

## 2. Non-goals

The first release does not include:

- a monitor UI or MCP App resource;
- blocking `run` or `batch` launch paths;
- a workflow/DAG language or automatic agent fan-out;
- raw unvalidated runtime flags;
- a remote HTTP API, multi-host execution, or remote-user authentication;
- an ORM, migration framework, dependency-injection container, or plugin
  marketplace;
- automatic replay of an ambiguous attempt that may have performed writes;
- copying model output into a trusted orchestration notification;
- inheriting undeclared global runtime configuration.

Grouped workflows may be added later as a relation between existing agents;
they do not belong in the v1 execution core.

## 3. Invariants

1. Every accepted start creates durable state before the runtime is launched.
2. Every start is asynchronous and returns a durable `agent_id` after the
   supervisor has installed its signal handlers, not after the model answers.
3. Every engine process belongs to a verified process group owned by exactly one
   supervisor.
4. The same validation and service path is used by CLI and MCP.
5. Unknown configuration fields, flags, runtime capabilities, models, and
   permission combinations fail before launch.
6. A permission switch may narrow authority but never widen the selected agent
   profile.
7. Prompt text never grants filesystem access. Read roots must be explicit,
   absolute, existing, resolved directories and form a minimal antichain.
8. Runtime configuration, skills, MCP servers, and hooks are materialized only
   from `~/.agent-run/config.toml` and `~/.agent-run/*/<runtime>` directories.
9. Credentials cross the isolation boundary only through an explicit auth
   bridge. Secret values are never written to generated configuration, SQLite,
   events, or logs.
10. Runtime adapters own runtime dialects; the shared core contains no
    `if runtime == ...` execution logic.
11. A terminal state means the engine is no longer running and the final
    artifact has been sealed.
12. Terminal transition and publication of the completion-notification outbox
    item are one SQLite transaction.
13. Completion notifications contain only trusted lifecycle fields: notification
    id, agent id, and terminal status. The orchestrator retrieves summary or
    transcript separately.
14. Delivery is at-least-once. Duplicate wakeups may occur; duplicate agents may
    not.
15. Missing or stale capacity is `unknown` and advisory. It never overrides an
    explicit owner choice.
16. Hook context is bounded to 2,500 characters and deduplicated per root
    orchestration session by a stable semantic key.
17. State and artifacts are private (`0700` directories, `0600` files).
18. Bookkeeping failure cannot destroy an answer that already exists.

## 4. Filesystem layout

### Repository

```text
bin/agent-run
src/agent_run/
  __init__.py
  cli.py
  config.py
  domain.py
  errors.py
  paths.py
  profiles.py
  service.py
  store.py
  supervisor.py
  lifecycle.py
  verify.py
  mcp/server.py
  adapters/
    base.py
    registry.py
    home.py
    codex/
      adapter.py
      app_server.py
    claude/
      adapter.py
      stream.py
    opencode/
      adapter.py
      http.py
      service.py
  capacity/
    collect.py
    history.py
    forecast.py
    advice.py
  delivery/
    base.py
    dispatch.py
    codex_queue.py
  hooks/
    context.py
    bind.py
tests/
docs/architecture.md
```

Production Python files receive a review warning at 400 lines and fail the
repository size check above 700 lines. Generated files and test fixtures are
excluded. The intent is one owner and one contract per module, not mechanical
line splitting.

### System home

```text
~/.agent-run/
  config.toml
  state.db
  state.db-wal
  state.db-shm
  profiles/
    review.md
    explore.md
    implement.md
    ...
  skills/
    codex/<skill>/SKILL.md
    claude/<skill>/SKILL.md
    opencode/<skill>/SKILL.md
  hooks/
    codex/
    claude/
    opencode/
  runtimes/
    codex/home/
    claude/home/
    opencode/home/
  agents/<agent_id>/
    supervisor.log
    runtime.jsonl
    answer.md
    metadata.json
  locks/
    delivery-dispatcher.lock
    capacity-collector.lock
  logs/
    capacity.log
    delivery.log
```

`config.toml` is owner-authored. Files below `runtimes/*/home` are generated
engine-native state. Files below `skills`, `hooks`, and `profiles` are owner
inputs and are never rewritten by runtime adapters.

## 5. Configuration contract

The file uses TOML because Python 3.11 includes `tomllib`. Schema versioning is
strict: unknown fields and unsupported versions fail with a path to the exact
field.

```toml
schema_version = 1

[core]
default_timeout_seconds = 480
max_active_agents = 6
warning_fraction = 0.90

[capacity]
collect_interval_seconds = 300
sample_retention = 1000
context_max_chars = 2500

[delivery]
retry_base_seconds = 2
retry_cap_seconds = 60
max_attempts = 0 # durable retries; zero means unlimited
codex_queue_bin = "/absolute/path/to/codex"

[profiles]
directory = "~/.agent-run/profiles"

[mcp.agent_lsp]
transport = "stdio"
command = "/absolute/path/to/agent-lsp"
args = []
env_from = []

[runtimes.codex]
enabled = true
adapter = "agent_run.adapters.codex.adapter:ADAPTER"
binary = "/absolute/path/to/codex"
home = "~/.agent-run/runtimes/codex/home"
models = ["gpt-5.6-sol", "gpt-5.6-terra"]
skills = ["delegate", "code-reading"]
mcp = ["agent_lsp"]
max_active_agents = 4

[runtimes.codex.auth]
kind = "file_link"
source = "~/.codex/auth.json"
target = "auth.json"

[[runtimes.codex.hooks]]
event = "UserPromptSubmit"
command = ["agent-run", "hook", "context"]

[[runtimes.codex.hooks]]
event = "PostToolUse"
matcher = "^mcp__agent_run__start$"
command = ["agent-run", "hook", "bind"]

[runtimes.claude]
enabled = true
adapter = "agent_run.adapters.claude.adapter:ADAPTER"
binary = "/absolute/path/to/claude"
home = "~/.agent-run/runtimes/claude/home"
models = ["fable", "opus", "sonnet"]
skills = ["delegate", "code-reading"]
mcp = ["agent_lsp"]

[runtimes.claude.auth]
kind = "environment"
names = ["CLAUDE_CODE_OAUTH_TOKEN", "ANTHROPIC_API_KEY", "ANTHROPIC_AUTH_TOKEN"]

[runtimes.opencode]
enabled = true
adapter = "agent_run.adapters.opencode.adapter:ADAPTER"
binary = "/absolute/path/to/opencode2"
home = "~/.agent-run/runtimes/opencode/home"
models = ["MiniMaxM3", "deepseek-v4-pro"]
skills = ["delegate", "code-reading"]
mcp = ["agent_lsp"]
service_mode = "managed"
```

Rules:

- runtime model discovery is intersected with the configured allowlist;
- skill names resolve only below `~/.agent-run/skills/<runtime>`;
- MCP names resolve only from `[mcp.<name>]` and are rendered by the selected
  runtime adapter;
- `env_from` and auth bridges name environment variables or source files but do
  not contain secret values;
- `core.default_timeout_seconds` applies only when a start request omits its
  timeout; an explicit positive timeout is preserved;
- `capacity.sample_retention` bounds collector-managed history to the global
  newest rows ordered by `observed_at DESC, id DESC`, including expired rows
  used for trends;
- `delivery.codex_queue_bin` is an absolute owner-authored executable; an
  explicit absolute `CODEX_QUEUE_BIN` environment value overrides it for
  compatibility;
- a runtime disabled in config cannot be launched or queried for live capacity;
- generated runtime configuration is content-addressed by a configuration hash;
- runtime-owned session/cache/trust state is not erased during regeneration;
- `agent-run doctor` reports missing skills, hooks, MCP executables, auth
  bridges, unsupported features, stale homes, plaintext-looking secrets, and
  untrusted hooks.

## 6. Domain model and state machine

One public `agent_id` represents one logical agent requested by the
orchestrator. Internal execution retries receive attempt rows; callers do not
need a second public job identifier.

```python
class AgentStatus(str, Enum):
    CREATED = "created"
    STARTING = "starting"
    RUNNING = "running"
    CANCELLING = "cancelling"
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    TIMED_OUT = "timed_out"
    CANCELLED = "cancelled"
    LOST = "lost"

ACTIVE = {CREATED, STARTING, RUNNING, CANCELLING}
TERMINAL = {SUCCEEDED, FAILED, TIMED_OUT, CANCELLED, LOST}

@dataclass(frozen=True)
class OrchestratorRef:
    transport: str
    external_session_id: str
    external_turn_id: str | None = None

@dataclass(frozen=True)
class StartRequest:
    runtime: str
    model: str
    profile: str
    task: str
    workdir: Path
    write: bool = False
    effort: str | None = None
    timeout_seconds: float | None = None
    read_roots: tuple[Path, ...] = ()
    output_schema: dict | None = None
    orchestrator: OrchestratorRef | None = None
    request_id: str | None = None

class MessageRole(str, Enum):
    USER = "user"
    ASSISTANT = "assistant"
    TOOL_CALL = "tool_call"
    TOOL_RESULT = "tool_result"
    SYSTEM = "system"

@dataclass(frozen=True, slots=True)
class Message:
    at: float
    role: MessageRole
    content: str
    name: str | None = None
    raw_ref: str | None = None

@dataclass(frozen=True, slots=True)
class Outcome:
    status: AgentStatus
    exit_code: int | None = None
    failure_kind: str | None = None
    failure_text: str | None = None
    runtime_session_id: str | None = None
    answer_path: Path | None = None
    answer_bytes: int | None = None
    answer_sha256: str | None = None
```

Public agent ids use `ag-YYYYMMDD-HHMMSS-<10 lowercase hex>`. Domain constructors
require actual enum instances rather than accepting equal raw strings. Message
timestamps are finite and nonnegative, message content is nonblank, outcomes are
terminal, and answer byte counts are nonnegative. `timeout_seconds=None` means
the caller omitted the timeout; any supplied timeout remains positive and finite.

Transitions:

```text
created -> starting -> running -> succeeded | failed | timed_out
    |          |          `----> cancelling -> cancelled
    |          `---------------> cancelled | failed
    `--------------------------> cancelled | failed

any active state -> lost only after durable reconciliation
```

Terminal states never reopen. `warned`, `silent_seconds`, and delivery status
are fields, not agent states.

## 7. Runtime adapter contract

The adapter API is deliberately small. Adapter-specific polling, protocol
events, model quirks, home rendering, and auth checks stay behind it.

```python
ADAPTER_API_VERSION = 1

class Capability(str, Enum):
    STEER = "steer"
    EFFORT = "effort"
    OUTPUT_SCHEMA = "output_schema"
    READ_ROOTS = "read_roots"
    WRITE = "write"
    TRANSCRIPT = "transcript"
    MODEL_ROSTER = "model_roster"
    LIVE_LIMITS = "live_limits"
    MCP = "mcp"
    SKILLS = "skills"
    HOOKS = "hooks"

@dataclass(frozen=True)
class RuntimeInfo:
    name: str
    adapter_api_version: int
    capabilities: frozenset[Capability]

@dataclass(frozen=True)
class RuntimeHealth:
    available: bool
    version: str | None
    authenticated: bool | None
    reason: str | None

@dataclass(frozen=True)
class ModelInfo:
    id: str
    description: str
    efforts: tuple[str, ...] = ()

@dataclass(frozen=True)
class LimitSample:
    lane: str
    window: str
    remaining_percent: float | None
    reset_at: datetime | None
    observed_at: datetime | None
    source: str
    target: str | None = None
    valid_for_seconds: int | None = None

@dataclass(frozen=True)
class LaunchPlan:
    argv: tuple[str, ...]
    cwd: Path
    environment: Mapping[str, str]
    initial_input: str | None
    runtime_stream_path: Path
    adapter_state: Mapping[str, object]

class EventSink(Protocol):
    def message(self, message: Message) -> None: ...
    def session(self, runtime_session_id: str) -> None: ...
    def event(self, kind: str, data: Mapping[str, object]) -> None: ...

class RuntimeSession(Protocol):
    @property
    def pid(self) -> int | None: ...
    def wait(self, timeout_seconds: float | None) -> Outcome | None: ...
    def steer(self, text: str) -> None: ...
    def cancel(self, grace_seconds: float) -> None: ...

class RuntimeAdapter(Protocol):
    def describe(self) -> RuntimeInfo: ...
    def validate(self, config: RuntimeConfig) -> None: ...
    def materialize(
        self,
        config: RuntimeConfig,
        home: Path,
        *,
        mcp_servers: Mapping[str, McpConfig],
    ) -> str: ...
    def probe(self, config: RuntimeConfig, home: Path) -> RuntimeHealth: ...
    def models(self, config: RuntimeConfig, home: Path) -> tuple[ModelInfo, ...]: ...
    def limits(self, config: RuntimeConfig, home: Path) -> tuple[LimitSample, ...]: ...
    def prepare(
        self,
        request: StartRequest,
        profile: AgentProfile,
        config: RuntimeConfig,
        home: Path,
        agent_dir: Path,
        *,
        mcp_servers: Mapping[str, McpConfig],
    ) -> LaunchPlan: ...
    def launch(self, plan: LaunchPlan, sink: EventSink) -> RuntimeSession: ...
```

Contract rules:

- `validate`, `materialize`, `probe`, and `prepare` finish before an engine model
  call;
- unsupported requested capabilities refuse before the public agent row is
  accepted;
- `materialize` may write only inside the adapter's generated home;
- `materialize` and `prepare` receive the resolved MCP servers as a required
  keyword-only `mcp_servers` mapping; adapters never read ambient config to
  resolve the names listed in `RuntimeConfig.mcp`;
- `prepare` receives only a request whose timeout is resolved to a positive,
  finite value; adapters never consume the omission marker;
- `prepare` starts nothing and returns no live handles;
- `launch` runs only inside the detached supervisor;
- `RuntimeSession.cancel` performs engine-native interruption before the shared
  process-group termination fallback;
- adapters may retry only failures they can prove are transient;
- the core never retries an ambiguous model run;
- a module loaded from trusted local config must expose an exact API version;
  mismatches fail. No compatibility shim is added until a second API version
  exists.

Adding a runtime requires a physical adapter module and one config section. It
does not require edits to CLI, MCP, service, store, supervisor, or capacity
forecast code.

## 8. Runtime isolation

### Codex

- Set `CODEX_HOME=~/.agent-run/runtimes/codex/home`; create it before launch.
- Generate `config.toml`, MCP definitions, `skills.config`, and hooks only from
  agent-run config and `~/.agent-run/skills/codex`.
- Use an explicit auth-file bridge, normally a symlink to the selected
  `auth.json`; verify it resolves to the configured file.
- Launch `codex app-server` and verify the effective model, cwd, sandbox,
  approval policy, workspace roots, and writable roots returned by
  `thread/start`.
- Read roots may be visible but never become writable roots.
- Preserve runtime-owned hook trust state during managed-config regeneration.
  A canary must prove this before the Codex adapter is accepted.

Official Codex documentation confirms that `CODEX_HOME` roots configuration,
auth, logs, sessions, skills, and package metadata, while `config.toml` supports
MCP, skill paths, and lifecycle hooks:

- https://learn.chatgpt.com/docs/config-file/config-reference
- https://learn.chatgpt.com/docs/hooks
- https://developers.openai.com/plugins/concepts/skills

### Claude

- Keep subscription/keychain authentication through an explicit auth bridge.
- Do not inherit user setting sources.
- Launch with an empty setting-source set, strict generated MCP config,
  generated plugin directories for selected skills/hooks, no session
  persistence, explicit tools, explicit allowed tools, and an explicit
  permission mode.
- Pass only named Claude auth environment variables into the Claude child; strip
  them from every other runtime.
- Treat `~/.agent-run/runtimes/claude/home` as generated runtime assets even if
  `CLAUDE_CONFIG_DIR` cannot be changed without losing subscription auth.
- Canary `CLAUDE_CONFIG_DIR` isolation. If it breaks keychain auth, retain the
  proven empty-setting-sources isolation boundary; never fall back to inheriting
  global settings.

### OpenCode

- Use only the v2 HTTP service; no per-run CLI fallback.
- Start an agent-run-owned service with generated config and isolated
  `XDG_CONFIG_HOME`/`XDG_DATA_HOME` under the runtime home.
- Set `OPENCODE_DISABLE_CLAUDE_CODE=1`.
- Generate roles, skills, MCP, and provider routes only from agent-run config.
- Auto-reject every interactive permission except a one-time
  `external_directory` grant fully contained by normalized read roots.
- Poll message state; do not use the long `wait` endpoint and do not pipe large
  API responses through a transport known to truncate them.
- The isolation canary must prove the service uses the generated home. If it
  cannot, the adapter remains unavailable; it must not attach to the user's
  global service as a silent fallback.

## 9. SQLite state

SQLite is authoritative for state, messages, commands, capacity samples, and
delivery. Raw high-volume streams and immutable artifacts remain files.

Required pragmas:

```sql
PRAGMA journal_mode=WAL;
PRAGMA synchronous=FULL;
PRAGMA foreign_keys=ON;
PRAGMA busy_timeout=5000;
```

Core tables:

```sql
CREATE TABLE orchestrator_sessions (
  id TEXT PRIMARY KEY,
  transport TEXT NOT NULL,
  external_session_id TEXT NOT NULL,
  external_turn_id TEXT,
  created_at REAL NOT NULL,
  last_seen_at REAL NOT NULL,
  UNIQUE (transport, external_session_id)
);

CREATE TABLE agents (
  id TEXT PRIMARY KEY,
  request_id TEXT,
  orchestrator_session_id TEXT REFERENCES orchestrator_sessions(id),
  runtime TEXT NOT NULL,
  model TEXT NOT NULL,
  profile TEXT NOT NULL,
  task TEXT NOT NULL,
  task_summary TEXT NOT NULL,
  workdir TEXT NOT NULL,
  request_json TEXT NOT NULL,
  status TEXT NOT NULL,
  created_at REAL NOT NULL,
  started_at REAL,
  finished_at REAL,
  timeout_seconds REAL NOT NULL,
  supervisor_pid INTEGER,
  supervisor_identity TEXT,
  process_group_id INTEGER,
  heartbeat_at REAL,
  runtime_session_id TEXT,
  config_revision TEXT NOT NULL,
  exit_code INTEGER,
  failure_kind TEXT,
  failure_text TEXT,
  warned INTEGER NOT NULL DEFAULT 0,
  silent_seconds REAL,
  answer_path TEXT,
  answer_bytes INTEGER,
  answer_sha256 TEXT,
  UNIQUE (orchestrator_session_id, request_id)
);

CREATE TABLE attempts (
  id TEXT PRIMARY KEY,
  agent_id TEXT NOT NULL REFERENCES agents(id),
  number INTEGER NOT NULL,
  state TEXT NOT NULL,
  adapter_state_json TEXT NOT NULL,
  created_at REAL NOT NULL,
  finished_at REAL,
  UNIQUE (agent_id, number)
);

CREATE TABLE events (
  seq INTEGER PRIMARY KEY AUTOINCREMENT,
  agent_id TEXT NOT NULL REFERENCES agents(id),
  attempt_id TEXT REFERENCES attempts(id),
  at REAL NOT NULL,
  kind TEXT NOT NULL,
  from_status TEXT,
  to_status TEXT,
  data_json TEXT NOT NULL DEFAULT '{}'
);

CREATE TABLE messages (
  seq INTEGER PRIMARY KEY AUTOINCREMENT,
  agent_id TEXT NOT NULL REFERENCES agents(id),
  attempt_id TEXT REFERENCES attempts(id),
  at REAL NOT NULL,
  role TEXT NOT NULL,
  name TEXT,
  content TEXT NOT NULL,
  raw_ref TEXT
);

CREATE TABLE commands (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  agent_id TEXT NOT NULL REFERENCES agents(id),
  kind TEXT NOT NULL,
  payload_json TEXT NOT NULL,
  state TEXT NOT NULL,
  created_at REAL NOT NULL,
  claimed_at REAL,
  completed_at REAL,
  result_json TEXT
);

CREATE TABLE deliveries (
  id TEXT PRIMARY KEY,
  agent_id TEXT NOT NULL REFERENCES agents(id),
  orchestrator_session_id TEXT REFERENCES orchestrator_sessions(id),
  terminal_event_seq INTEGER REFERENCES events(seq),
  state TEXT NOT NULL,
  attempts INTEGER NOT NULL DEFAULT 0,
  lease_owner TEXT,
  lease_until REAL,
  next_attempt_at REAL,
  remote_message_id TEXT,
  last_error TEXT,
  ambiguous_result INTEGER NOT NULL DEFAULT 0,
  UNIQUE (agent_id, terminal_event_seq)
);

CREATE TABLE capacity_samples (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  runtime TEXT NOT NULL,
  lane TEXT NOT NULL,
  window TEXT NOT NULL,
  target TEXT,
  source TEXT NOT NULL,
  remaining_percent REAL,
  reset_at REAL,
  observed_at REAL,
  valid_until REAL,
  payload_json TEXT NOT NULL
);

CREATE TABLE context_receipts (
  orchestrator_session_id TEXT PRIMARY KEY REFERENCES orchestrator_sessions(id),
  context_key TEXT NOT NULL,
  injected_at REAL NOT NULL
);
```

Indexes cover active status, messages by agent/sequence, due commands, due
deliveries, and capacity samples by lane/window/source/reset.

Transaction rules:

```python
@dataclass(frozen=True, slots=True)
class AgentCreation:
    agent_id: AgentId
    created: bool

class StateStore:
    def create_agent(
        self,
        request: StartRequest,
        *,
        task_summary: str,
        config_revision: str,
        agent_id: str | AgentId | None = None,
        at: float | None = None,
    ) -> AgentCreation: ...

    def prune_capacity_samples(self, retention: int) -> int: ...

    def capacity_sample_history(
        self,
        *,
        retention: int,
        runtime: str | None = None,
    ) -> list[dict[str, object]]: ...
```

- `BEGIN IMMEDIATE` for state transitions and outbox claims;
- agent creation refuses a request whose timeout omission has not been resolved;
- start inserts the agent and initial event before supervisor spawn;
- a caller-supplied `request_id` is globally idempotent before binding; under
  `BEGIN IMMEDIATE`, retries must match the serialized request, task summary,
  and configuration revision exactly;
- configured global and per-runtime active-agent caps are checked in that same
  `BEGIN IMMEDIATE` after the idempotent duplicate lookup and before session,
  agent, or initial-event insertion, so concurrent starts cannot exceed either
  cap and a retry still returns its existing agent when capacity is full;
- worker claims use conditional status/attempt updates;
- the final artifact is flushed and hashed before terminal commit;
- terminal commit updates the agent, appends the terminal event, and activates
  the unique delivery row atomically;
- if terminal completion happens before orchestration binding, delivery remains
  `waiting_binding`; immutable binding later activates it in the same transaction;
- expired delivery leases are reclaimable;
- capacity pruning runs under `BEGIN IMMEDIATE` and atomically keeps the global
  newest `retention` rows by `observed_at DESC, id DESC`; history uses the same
  global bound and ordering without filtering expired rows;
- terminal states never transition back;
- state migrations are numbered SQL files, backed up before application, and
  applied in one transaction. No migration framework is needed.

Large raw runtime streams are not SQLite blobs. Normalized chat messages are
stored in SQLite; raw or oversized tool content is referenced through `raw_ref`.
`transcript --full` follows all references without silent truncation.

## 10. Process topology

```text
CLI / MCP / runtime hook
        |
        v
service.start()
  validate -> create SQLite row -> spawn detached supervisor -> ready ack -> agent_id
                                      |
                                      v
                       supervisor <agent_id>
                     adapter.launch() -> engine process group
                       heartbeat / events / messages / commands
                                      |
                                      v
                     terminal SQLite transaction
                                      |
                   +------------------+------------------+
                   |                                     |
                   v                                     v
             sealed artifacts                    delivery outbox
                                                         |
                                                         v
                                            on-demand dispatcher
                                                         |
                                                         v
                                           orchestrator chat message
```

The capacity path is independent:

```text
launchd every 300 seconds
  -> agent-run capacity collect --once
  -> RuntimeAdapter.limits() for enabled runtimes
  -> normalized SQLite samples

UserPromptSubmit hook
  -> agent-run context <orchestrator-session>
  -> read-only SQLite query
  -> bounded capacity + active-agent summary
```

Supervisor rules:

- install signal handlers before reporting ready;
- start the engine in its own process group;
- heartbeat every five seconds;
- process durable cancel/steer commands;
- at 90% of deadline, issue one model-visible completion steer when supported;
- at 100%, perform native cancel then TERM/KILL the verified process group;
- distinguish no answer from an answer cut off during write;
- semantic completion checks remain profile/runtime-specific;
- record terminal state only after the engine group is gone.

Reconciliation checks PID, full command identity, heartbeat, and process group.
A stale supervisor with a verified surviving engine group is terminated before
`lost` is committed. An ambiguous execution is never automatically replayed.

## 11. Orchestrator binding and completion messages

Chat delivery is a separate adapter boundary, not part of `RuntimeAdapter`:

```python
class ChatTransport(Protocol):
    name: str
    api_version: int
    def validate(self, config: ChatTransportConfig) -> None: ...
    def send(self, target: OrchestratorRef, notice: CompletionNotice) -> DeliveryReceipt: ...
```

The first transport is Codex queue delivery. More transports may be added
without changing runtime adapters or state.

Binding rules:

- the start response contains the durable `agent_id`;
- a runtime-specific synchronous `PostToolUse` hook binds that id to the current
  orchestration session;
- an unbound agent may continue running, but the hook must explicitly report that
  chat notification is not confirmed and keep the root turn alive for recovery;
- session identity is `(transport, external_session_id)`; `external_turn_id` is
  latest non-null bookkeeping metadata, and an omitted turn does not clear it;
- binding may fill an empty target or repeat the same session identity; binding
  to a different identity is refused;
- if binding happens after terminal completion, the waiting delivery is activated
  transactionally;
- terminal completion must always create or activate exactly one local outbox
  item for chat delivery;
- raw third-party hook envelopes may contain unrelated fields, but the normalized
  binding payload remains strict and missing or conflicting agent ids refuse.

Trusted payload:

```json
{
  "version": 1,
  "notification_id": "ntf_...",
  "agent_id": "ag-...",
  "status": "succeeded"
}
```

The rendered chat message contains the same lifecycle facts and instructs the
orchestrator to call `summary(agent_id)` or `transcript(agent_id)`. It never
contains task text, external answer text, runtime error prose, or tool output.

Delivery states:

```text
waiting_binding -> pending -> sending -> delivered
                         \-> retry_wait -> sending
                         \-> failed | cancelled
```

Claims use leases and capped exponential backoff. Retries are unlimited by
default. Where a transport offers an idempotency key, use `notification_id`.
Codex queue currently does not, so ambiguous acceptance remains at-least-once.
Duplicate messages reference the same `agent_id` and must never launch a
replacement. A one-shot periodic launchd sweeper reclaims due `retry_wait` rows
after the terminal child exits. Each trigger drains a bounded multi-row batch;
the next trigger recovers overflow without a resident daemon or busy loop.

## 12. Capacity and active-agent context

Every enabled runtime adapter returns `LimitSample` records. Shared capacity code
owns history, reset alignment, burn, sustainable pace, risk, recommendations,
and the stable semantic key.

Preserved behavior:

- samples are normalized and private;
- stale or incomplete sources become `unknown`;
- limits are segmented by lane, window, target, reset, and source;
- collection prunes once after all enabled runtimes produce success, failure, or
  unsupported results, so a partial source failure cannot bypass retention;
- service and context history readers use the same configured
  `capacity.sample_retention`; expired rows remain available for reset-aligned
  trends within that global bound;
- advice is advisory and explicit owner choice wins;
- partial source failure does not suppress healthy sources;
- credential values and raw provider responses are never stored.

The hook projection adds a compact session-scoped activity block:

```text
Active agents (3): ag-... codex/sol review 4m; ag-... claude/fable architect 12m; +1 more.
Use agent-run status/transcript; do not start replacements for existing ids.
```

Rules:

- include only agents bound to the current orchestrator session by default;
- show exact total, runtime/model/profile, safe task summary, state, elapsed time,
  warning state, and material silence;
- cap listed agents and report `+N more`;
- active block receives at most 600 of the 2,500-character context budget;
- `context_key` changes on capacity semantics or agent set/state/warning change,
  not on every elapsed minute;
- capacity freshness comes from the collector's `valid_until`;
- hook execution is read-only and does not perform network/provider calls;
- per-session receipt prevents repeated injection of unchanged context;
  reference-based receipt bookkeeping creates or reuses the session and updates
  its conditional receipt atomically without advancing `injected_at` for an
  unchanged key.

## 13. Unified service, CLI, and MCP

CLI and MCP decode inputs and render outputs. They do not validate runtimes,
spawn engines, calculate limits, or query SQLite directly outside the service.

```python
class AgentService(Protocol):
    def start(self, request: StartRequest) -> StartResult: ...
    def bind(self, agent_id: str, orchestrator: OrchestratorRef) -> DeliveryView: ...
    def cancel(self, agent_id: str) -> AgentView: ...
    def steer(self, agent_id: str, text: str) -> CommandView: ...
    def get(self, agent_id: str) -> AgentView: ...
    def list(self, query: AgentQuery) -> AgentPage: ...
    def transcript(self, agent_id: str, cursor: int = 0, limit: int = 200) -> TranscriptPage: ...
    def answer(self, agent_id: str) -> AnswerView: ...
    def summary(
        self,
        agent_id: str | None = None,
        orchestrator: OrchestratorRef | None = None,
    ) -> WorkSummary: ...
    def models(self) -> Mapping[str, tuple[ModelInfo, ...]]: ...
    def limits(self) -> CapacityReport: ...
```

At the start boundary, `timeout_seconds=None` is replaced exactly once with
`core.default_timeout_seconds` before adapter preparation, durable persistence,
or detached launch. An explicit positive timeout is passed through unchanged.

`summary` requires exactly one of `agent_id` or `orchestrator`. Trusted
completion notices use `summary(agent_id)`; session-scoped views use the
orchestrator reference.

Minimum CLI:

```text
agent-run start
agent-run bind
agent-run cancel
agent-run steer
agent-run status
agent-run agents [--active] [--session]
agent-run summary [--session]
agent-run transcript [--follow|--full]
agent-run answer
agent-run models
agent-run limits
agent-run context
agent-run capacity collect --once
agent-run delivery status|cancel|dispatch|launchd
agent-run doctor
agent-run init
```

`transcript --follow` advances its cursor without duplicates, polls at a bounded
interval while the agent is active, and exits after terminal state plus a drained
transcript. `--full` keeps its finite current-pagination semantics. On a fresh
home, `init` creates a private minimal config and state database without inventing
runtime, delivery, or credential values; an existing config is never replaced.

Minimum MCP tools:

```text
start
cancel
steer
status
list_agents
summary
transcript
answer
models
limits
```

`start` always returns immediately. `list_agents` returns an exact SQL count in
addition to a bounded page. `status` and `summary` show who is doing what, how
long it has run, last progress, silence, capacity warning, and notification
state. `transcript` is cursor-paged; MCP never silently truncates and claim the
chat is complete.

No UI resources are registered.

## 14. Migration map

Reuse behavior, tests, and failure evidence; do not copy the monolithic file.

| Legacy source | New owner |
|---|---|
| `load_role`, `guard_write`, `normalize_read_roots`, strict parsing | config/profile/domain validation |
| output-path freshness and private permissions | paths/artifact store |
| process groups, warning, deadline, signal reaping | lifecycle/supervisor |
| `build_codex`, `codex-app-run` | Codex adapter |
| `build_claude`, stream result decoding | Claude adapter |
| `build_opencode`, `oc2-run`, service startup | OpenCode adapter |
| registry and job events | SQLite store |
| capacity collectors and forecasting | adapter limits + shared capacity package |
| PostToolUse attachment and queue dispatcher | binding hook + delivery package |
| `t_answer`, status projections | service read models |

Do not migrate:

- public `raw` runtime flags;
- OpenCode CLI fallback;
- blocking MCP launch tools;
- monitor HTML/resources;
- path-containment joins between job directories and run registry;
- capacity math duplicated inside MCP;
- literal machine paths or hard-coded model rosters;
- stale prose claiming Codex still uses `codex exec`;
- UI-only metadata not consumed by CLI/MCP.

## 15. Implementation modules and parallel waves

Interfaces in this document are frozen before implementation. If a module finds
one insufficient, work returns to design and every consumer is checked; an
implementer does not silently widen it.

| Module | Outcome | Depends on | Estimate |
|---|---|---|---:|
| M001 architecture contract | This document, invariants, interfaces, acceptance | — | 575k |
| M002 config/domain/runtime homes | typed config, profiles, adapter base/loader, materialization | M001 | 520k |
| M003 SQLite state | schema, store, transcripts, commands, reconciliation | M001 | 520k |
| M004 async supervision | detached supervisor, lifecycle, cancellation, timeout | M002, M003 | 560k |
| M005 Codex adapter | isolated home, app-server, models/limits/events | M002 | 360k |
| M006 Claude adapter | strict settings/plugins/MCP/tools, stream decode | M002 | 360k |
| M007 OpenCode adapter | isolated managed v2 service, permissions, polling | M002 | 420k |
| M008 service + CLI + MCP | one service path and thin transports | M002, M003, M004 | 520k |
| M009 capacity + context hook | collectors, forecast, launchd job, bounded context | M002, M003 | 460k |
| M010 chat delivery | immutable binding, outbox, dispatcher, Codex queue hook | M003 | 440k |
| M011 integration and migration | live canaries, end-to-end acceptance, legacy cutover | M004-M010 | 500k |

Parallel waves:

1. M001 freezes the contract.
2. M002 and M003 run independently.
3. M004, M005, M006, M007, M009, and M010 run as soon as their listed
   dependencies close. Adapter modules are independent of each other.
4. M008 uses fake adapters first and integrates after M004.
5. M011 performs live canaries and cutover; it does not redesign modules.

Each module is sliced into tasks only when it starts. Worker briefs own disjoint
packages and must preserve concurrent edits.

## 16. Required evidence and acceptance

Runtime switch-point canaries before adapters are accepted:

1. Codex: regenerate the isolated home three times and prove configured MCP,
   skills, and hooks remain isolated and trusted; undeclared global canaries do
   not appear.
2. Claude: prove empty setting sources, strict generated MCP, selected generated
   skill/plugin directories, and explicit tools exclude global customizations
   while the explicit auth bridge still authenticates.
3. OpenCode: prove a managed service honors isolated XDG config/data homes and a
   distinct service endpoint. If not, keep the adapter unavailable.

Core acceptance tests:

1. A fake runtime sleeping ten seconds still yields a durable id promptly.
2. A timed-out duplicate start with the same session/request id returns the same
   agent, not a second launch.
3. Immediate cancel cannot race signal-handler installation into an orphan.
4. Cancel removes an engine grandchild as well as its wrapper.
5. Hard timeout steers once at 90%, kills at 100%, and records no-answer versus
   cut-off-answer evidence.
6. A dead supervisor is reconciled durably; a verified surviving process group
   is stopped before `lost`.
7. Unsupported adapter capabilities and unknown config keys fail before launch.
8. Read roots cannot become write roots or escape through symlinks.
9. Global skill/MCP/hook canaries are absent from every isolated runtime; selected
   agent-run assets are present.
10. Terminal state, terminal event, and delivery activation are atomic.
11. Completion before binding still sends after immutable binding succeeds.
12. Stale delivery leases recover; cancellation stops retries but preserves the
    result; ambiguous queue timeout may duplicate a wake but never an agent.
13. Full transcript pagination is ordered and exposes explicit raw references for
    oversized content.
14. Active count is exact above list page limits.
15. Capacity output validates with stale/unknown inputs and partial source
    failures; explicit model choice remains allowed.
16. Context injection stays within 2,500 characters, changes on meaningful agent
    state/capacity changes, and does not repeat unchanged context per session.
17. Completion chat messages contain only trusted id/status fields.
18. `doctor` flags plaintext-looking secrets, missing homes, untrusted hooks,
    stale capacity, dead supervisors, and suspected orphans without mutating
    state.
19. CLI, MCP, and direct service starts apply the configured timeout when it is
    omitted, preserve an explicit timeout, and refuse unresolved direct
    persistence.
20. Capacity collection prunes after all runtime results, including partial
    failures, and keeps exactly the configured global newest rows with `id` as
    the stable tie-breaker.
21. Service and context capacity history use the configured retention, include
    expired trend rows within that bound, and do not fall back to a separate
    hardcoded limit.
22. Due delivery retries wake after terminal child exit, respect
    `next_attempt_at`, drain a bounded backlog larger than one, and leave overflow
    recoverable by the next one-shot sweep.
23. Delivery uses the absolute owner-configured Codex queue executable unless an
    explicit absolute compatibility environment override is present.
24. Fresh `init` creates private minimal config/state artifacts idempotently and
    writes no credentials or inferred runtime configuration.
25. Transcript follow is duplicate-free and terminal-bounded; raw hook envelopes
    ignore unrelated fields while normalized payloads remain strict.

## 17. Decisions

- Use SQLite metadata plus immutable artifact files, not SQLite-only blobs and
  not JSONL-only state.
- Use one detached supervisor per agent, not a resident central daemon.
- Keep capacity collection periodic and hook injection read-only.
- Keep completion delivery independent from runtime adapters.
- Support one public agent id; retries are internal attempts.
- Implement Codex queue delivery first and keep the transport interface open.
- Preserve current security and failure semantics before adding workflows.
- Prefer verified runtime-native isolation over pretending a separate home is
  effective when the engine still reads global state.
