# Changelog

All notable changes are documented here. Versions follow Semantic Versioning.

## [Unreleased]

- feat(transcript): add the optional `blocks` view and bounded `tail_blocks`/
  `before_cursor` paging over the existing transcript API; raw rows, cursors and
  ordering stay unchanged, unknown identities never merge, and omitted or
  spooled content is reported explicitly instead of dropped
- feat(evidence): persist native tool-result failure flags with allowlisted
  provenance (Claude `is_error`, Codex command/MCP markers), journal Codex tool
  calls once per native id, and expose nullable latest-execution `tool_counts`
  with unknown kept distinct from observed success
- feat(pools): add the cooperative-pool foundation: validated provider-neutral
  domain types, one compact English entry renderer that stamps the author and
  marks the body untrusted, and the schema below
- feat(pools): admit a pool's members, reservations and roster atomically in one
  store transaction and compose each member's task with the shared goal and every
  peer's stable identity before launch
- feat(pools): give members a fixed private tool catalog (notify, chat, read,
  propose, vote) over one authenticated broker route: authors are stamped from
  durable membership, refusals are typed codes, reads page the append-only log
  by cursor with an optional bounded wait and render plain-text entries, and
  derived vote status is reported without ever completing the pool
- feat(pools): enqueue every new pool entry, and a member's notify team copy,
  for each peer's tip in the same transaction; delivery records only a finite
  disposition (native accepted, written, rejected, unsent, refused, unknown),
  never consumption, and the orchestrator's notice for a pool member's report
  carries the stamped sender
- feat(pools): add core operator operations: post a stamped operator message,
  read a pool's status and log, and replace a terminal, fully cleaned member
  atomically with a roster change, a broker roster entry and a replayable result;
  not yet exposed through any tool, CLI or MCP method
- feat(pools): complete a pool only when every current member voted ready on the
  current proposal, ended successfully and has verified cleanup, in one
  transaction that freezes one completion record and queues exactly one common
  notice (Desktop relay `pool_completion`, Claude inbox text) naming no run,
  attempt or session identity; unbound pools keep the notice waiting until a pool
  binding exists (store `bind_pool`, not yet exposed); completed pools stay frozen
  if a member is resumed later, and retention of completed pools is not
  implemented yet

Database schema 25 adds the `pools`, `pool_members` and `pool_entries` tables
with historical membership, one current member per slot and immutable
author-at-send log entries; existing rows are untouched.

Database schema 24 adds nullable `messages.error`/`error_source`,
`content_complete` and the lineage index on `messages.root_agent_id`; historical
rows keep unknown evidence and no backfill invents it. Upgrade existing homes
through the paired `agent-run config migrate` procedure before restarting the
broker. Configuration schema remains 2.

## [0.20.0] - 2026-10-03

- feat(agents): accept optional human display labels on start and resume, inherit
  omitted labels, and show them in agent lists, the TUI and compact MCP output
- feat(usage): expose nullable native usage for the latest execution and complete
  lineage totals; record comparable Codex resume baselines before launch so
  resumed counters report deltas without inventing missing measurements or turns

Database schema 23 adds nullable `agents.display_name`; historical rows remain
unnamed. Upgrade existing homes through the paired `agent-run config migrate`
procedure before restarting the broker. Configuration schema remains 2.

## [0.19.5] - 2026-10-02

- fix(mcp): answer `tools/list` on both the operator and the worker MCP surface
  with `ttlMs` (60 s) and `cacheScope: private` when the request negotiated MCP
  2026-07-28 or later, so such clients no longer reject the tool list (Claude
  child agents lost `notify_orchestrator`); 2025-11-25 and earlier sessions
  keep their byte-identical replies

No schema change. The client-side `mcp_catalog` probe remains legacy-only.

## [0.19.4] - 2026-10-01

- feat(retention): keep the newest 100 logical agent sessions alongside
  fourteen-day expiry; count resumes once, protect active references and
  reclaim count-pruned run directories through bounded admission-safe checks

The database schema remains 22. Active and otherwise protected sessions may
temporarily exceed the count limit. No migration from 0.19.1–0.19.3 is needed.

## [0.19.3] - 2026-10-01

- fix(delivery): require a correlated native Claude inbox receipt before
  marking completion and worker notices delivered; verify receiver process
  credentials, bound receipt reads, and remove only owned reply sockets
- fix(delivery): persist safe Claude attempt diagnostics and stop uncertain
  retries after three attempts instead of reporting success from a socket write

The database schema remains 22. Receipt confirmation proves inbox acceptance,
not that the recipient model has read the notice.

## [0.19.2] - 2026-10-01

- fix(maintenance): schedule database expiry independently from filesystem
  cleanup, avoiding repeated idle writer transactions during file backlogs;
  defer contention and record safe SQLite primary and extended result codes
- feat(config): add `core.timeout_multiplier`, defaulting to 1.2 (+20%), for
  explicit and default agent timeouts; preserve inherited resume allowances
  without compounding and retain historical configuration snapshot digests

The database schema remains 22. Set `core.timeout_multiplier = 1.0` to disable
the timeout margin. Existing admitted runs keep their original deadlines.

## [0.19.1] - 2026-09-30

- perf(storage): share immutable runtime assets and compatible native Codex
  caches across independent starts while keeping history, databases,
  credentials, generated configuration and worker capabilities private
- feat(storage): add offline `storage status`, `storage compact` and
  `storage recover` commands for retained homes; preserve their original
  snapshot digests and resume history, with schema 22 guarding the new layout
- fix(storage): retain shared data while a home or recovery operation still
  references it, and qualify native launches before publishing shared assets;
  the shared-store launch protection is currently qualified on macOS only

Upgrade: use the new binary's `config migrate` procedure before installing
against an existing schema-21 home (`agent-run doc migrations`). Installation
does not silently migrate the database; compaction is a separate offline step.

## [0.19.0] - 2026-09-28

- feat(tui): ship `agent-run-tui` as a separate read-only terminal observer
  with live agent cards, transcripts, answer viewing and mouse navigation
- feat(install): install both native binaries from the same versioned release
- fix(tui): keep transcript reads independent of pending list watches and show
  only the stable public agent identity

- fix(identity): expose only the stable agent ID in public responses, completion
  and worker notices, CLI output, and hook context; keep execution records private
- fix(binding): pin delayed notification bindings and CLI waits to the admitted
  execution using the existing lineage counter, preserving legacy receipt support
- fix(history): list each logical agent once and keep transcript pagination
  continuous across resumes without rewriting prior messages or answer content

Public contract change: run and attempt identifiers are no longer returned;
legacy execution selectors remain accepted but are hidden from discovery.
Reconnect Agent Run MCP clients together with the broker update so their
binding receipts use the matching format. No database migration is required.

## [0.18.2] - 2026-09-28

- fix(runtime): forward resolved Rust roots to Codex MCP tools, avoid repeated
  remote catalog downloads, and reuse identical same-account native plugin
  cache blocks after verified cleanup while keeping each run's files private

- perf(runtime): clone managed plugin and skill snapshot files on supported
  macOS filesystems, preserving per-run isolation and historical verification;
  retain the byte writer on unsupported volumes or incompatible source metadata

## [0.18.1] - 2026-09-28

- fix(retention): expire unreferenced run directories, completed deployment and
  migration backups, and obsolete configuration/profile backups after 14 days
- fix(retention): preserve retained session and credential references, pending
  recovery, and unknown filesystem entries during bounded background cleanup
- fix(logging): write UTC-daily component logs and expire old daily files
  after 30 days; preserve undated legacy and launchd logs with unknown writers
- fix(maintenance): keep database expiry and vacuum running independently of
  filesystem cleanup failures, and reclaim verified stale Desktop relay sockets

No configuration or database migration is required. Restart the broker after
installation to enable filesystem cleanup; long-lived clients adopt daily
logging when restarted. Shared runtime-home optimization is not included.

## [0.18.0] - 2026-09-28

- feat(workers): add a separate one-tool MCP for durable worker-to-orchestrator
  reports, bound to an active run and attempt with no recipient selector
- feat(delivery): deliver worker reports through the existing outbox and Desktop
  transports without ending the run or changing completion delivery status
- feat(roles): attach the worker channel to newly frozen schema-2 roles while
  preserving historical snapshots on resume

Requires the paired database upgrade to schema 21 and refreshed Desktop Agent Run
MCP frontends for worker-message delivery. The operator tool table is unchanged.

## [0.17.2] - 2026-09-28

- fix(delivery): send the callerSource field required by the current Codex Desktop
  native tool protocol, restoring completion notifications
- fix(delivery): classify correlated invalid-parameter replies as rejected while
  retaining ambiguity for interrupted calls and unknown execution errors
- test(delivery): cover the required caller identity and distinguish known rejection
  from uncertain delivery

Reconnect Agent Run MCP clients after installing so their long-lived Desktop
frontends load this fix. Restarting only the broker does not refresh those clients.
No configuration or database migration is required.

## [0.17.1] - 2026-09-27

- fix(roles): prepend shared worker instructions to new canonical roles so
  delegated agents focus on execution and concise final reports instead of
  conversational progress narration; preserve verification and approval obligations
- fix(roles): freeze the complete role prompt for continuation without duplicating
  the preamble or changing historical sessions
- docs: describe the shared prompt asset and its native Claude/GLM and Codex delivery

No configuration or database migration is required. The new instructions apply
to newly created sessions; resumed sessions retain their admitted prompt.

## [0.17.0] - 2026-09-27

- feat(mcp): declare global servers once and narrow their exact tool sets in
  catalog configuration and individual profiles; preserve existing string-only profiles
- feat(mcp): use Codex native enabled_tools and Claude/GLM native disallowedTools
  computed from bounded, authenticated tools/list discovery before each attempt;
  MCP connections remain direct, with no filtering proxy
- fix(lifecycle): journal temporary discovery process identities before execution,
  clean them on errors and cancellation, and recover them after supervisor crashes
- fix(resume): preserve frozen MCP selections and refuse newly revoked tool grants
- feat(diagnostics): show selected MCP sources and tool counts in compact start/resume responses
- docs: describe configuration examples and Claude's launch-time catalog limitation;
  tools added after discovery are not covered by the generated deny list

Existing schema-2 configuration and schema-20 databases need no migration.

## [0.16.1] - 2026-09-25

- build(deps): update MCP, SQLite, hashing and TOML dependencies and GitHub
  artifact actions
- feat(routing): require orchestrators to read the dynamic delegation guide
  before choosing a provider, model, effort and profile
- fix(services): optionally reuse an already-ready external backend before
  spawning, recover a lost startup race only after owned cleanup, and never
  claim or stop the external process; preserve ownership across broker restarts
- build: schema 20 records managed/external service ownership and keeps every
  historical generation managed; paired configuration/database migration is required
- fix(services): support external CodeGraph readiness in the shipped probe
  while retaining PID verification for broker-owned instances

- fix(continuations): use the existing idempotent broker client for CLI and MCP
  resumes, preserving one request key across a bounded transport reconnect
- docs: align operator guidance with current provider, permission, migration,
  quota, continuation and transport contracts
- docs(models): document verified GLM effort passthrough and explicit defaults;
  cover low, high and max in the custom-provider adapter regression test

## [0.16.0] - 2026-09-25

### Changed

- Agent identities now remain stable across explicit resumes. Public responses
  include an immutable `run_id` for each execution; historical reads and controls
  accept that selector. Existing run ids remain aliases for their logical agent.
- Completion notices and binding hooks distinguish the stable agent from the
  exact completed run. Resume transport retries reuse an idempotency key.

### Compatibility

- **Breaking:** `agent_id` selects the latest execution of a lineage. Supply
  `run_id` to retrieve or control a specific historical execution. Stored rows,
  native history and the SQLite schema are unchanged.

## [0.15.0] - 2026-09-25

- feat(mcp)!: render all public tool results with compact, embedded MiniJinja
  templates instead of duplicating the full structured JSON into model context;
  preserve answer text, pagination, typed errors and actionable lifecycle diagnostics
- feat(mcp): retain only the durable `agent_id` as structured metadata for
  `start` and `resume`, preserving automatic chat binding; the broker socket
  and existing CLI commands keep their structured JSON contracts
- feat(delegation): add the read-only `delegation_guide` tool and
  `agent-run delegation-guide` command with current provider order, quota
  evidence, admissible profiles, configured effort choices and recommendation prose
- feat(templates): keep provider/model guidance and shared profile lists compact,
  with one template per tool and a shared error template; templates are sealed
  with the binary and require no runtime filesystem loader

## [0.14.0] - 2026-09-24

- fix(store): expire completed database history after fourteen days while
  retaining active work and recovery dependencies; reclaim free pages in
  bounded incremental-vacuum batches, prepared by the schema-19 migration
- feat(migration): upgrade existing schema-2 homes with an explicit target
  configuration while preserving account references and history; reuse paired
  rollback and stream database verification without whole-database allocations
- feat(services): warm configured foreground backends before harness launch,
  share durable generations across agents, monitor readiness, and stop after
  thirty minutes without active agents; recover owned services and interrupted
  readiness probes after broker restart
- fix(supervisor): checkpoint captured descendants and use fresh identity
  checks to recover their cleanup even after the original harness leader exits
- build: schema 19 stores broker-owned service generations, agent leases,
  startup gates and captured process identities, with indexed history cleanup
- feat(install): install or update a sealed release with curl/wget, preserving
  configuration and data and refusing active or incompatible installations
- feat(capacity)!: run externally configured quota executables with private JSON
  input and normalized JSON output; remove the embedded Lua runtime
- fix(supervisor): observe the primary harness PID independently of inherited
  pipes and terminate captured descendants on exit
- fix(mcp): bound test fixture lifetimes and stop the Rust frontend when its
  desktop transport parent disappears

## [0.13.3] - 2026-09-24

- fix(reconcile): restrict orphaned-attempt diagnostic lookup to its indexed agent history so a large transcript cannot hold the SQLite writer and block broker commands
- fix(reconcile): mark a verified-dead supervisor lost even if it exited before recording an engine group; release only a prepared attempt with never-spawned proof and retain uncertain spawning ownership
- 0.13.3 publishes a native artifact only for macOS Apple silicon; Linux x86-64 remains a non-blocking validation lane with no published artifact

## [0.13.2] - 2026-09-24

- fix(supervisor): settle verified descendant cleanup after the process group exits instead of losing runs when short-lived helpers finish moments later
- fix(supervisor): record bounded cleanup failures and route supervisor errors to their component log without persisting task text or credentials
- fix(delivery): use schema-2 retry policy after migration so a claimed notice reaches a durable result instead of repeating a validation failure
- fix(transcript): redact launch credentials across Claude and Codex text fragments, tool output, native event diagnostics, and sealed answers before persistence
- fix(transcript): preserve nonsecret streamed text, flush pending Codex fragments on terminal turns, and redact failures before truncation
- fix(config): apply offered model effort defaults to the frozen launch while preserving caller replay intent; reject unsupported model parameter keys
- fix(auth): refuse provider login selectors that ambiguously name one account label and another account id
- fix(doctor): detect schema-2 provider bindings that cannot resolve against the registered accounts without mutating state
- fix(capacity): treat a valid empty schema-2 provider catalog as a successful empty collection round
- docs: align provider cutover and continuation guidance with the implemented Codex A→B proof and Claude-family refusal
- 0.13.2 publishes a native artifact only for macOS Apple silicon; Linux x86-64 remains a non-blocking validation lane with no published artifact

## [0.13.1] - 2026-09-23

- fix(claude): schema-2 Claude Code and custom gateway launches use the
  admitted canonical role's skill list to authorize whole-plugin exports;
  undeclared plugin skills still fail before the engine starts
- fix(doctor): flag a schema-2 home whose profile catalog has no canonical
  roles, before an operator resumes agent launches
- 0.13.1 publishes a native artifact only for macOS Apple silicon; Linux
  x86-64 remains a non-blocking validation lane with no published artifact

## [0.13.0] - 2026-09-23

- feat!: schema-2 provider configuration; public `start` takes an explicit
  provider and model, resolves accounts from the registry, and ranks them from
  account-scoped quota observations. A schema-1 home must run the one-time
  paired `config migrate` (state schema 16 to 17) before any other command
- feat: provider catalog exposes configured models and recommendations without
  account choices; Codex app-server and bounded Claude/GLM collectors replace
  CodexBar for quota readings
- feat: `transcript --follow --format text` streams model output and tool
  activity as the run proceeds
- feat: explicit provider resume continues the recorded native session as a
  new logical child; an authoritative quota exhaustion switches a Codex run to
  another account within the same logical run. Claude-family routes refuse
  cross-account continuation; a custom Codex gateway follows the Codex
  harness contract when its frozen connection and native history verify
- feat: one enforced overall run deadline, handoff checks serialized with
  cancellation, and proof-gated recovery of orphaned provider attempts
- fix: quota pools are matched to models through recorded per-window
  membership rather than pool names
- 0.13.0 publishes a native artifact only for macOS Apple silicon; Linux
  x86-64 remains a non-blocking validation lane with no published artifact

## [0.12.6] - 2026-09-23

- fix(claude): resumed Claude/GLM agents keep the parent's `--plugin-dir`
  list; resume previously dropped every plugin, so agent-ide hooks and skill
  plugins were missing (`unavailable: host_binding`, `Unknown skill`). Snapshots
  from 0.12.5 and earlier rebuild the list from the recorded runtime and profile
  or fail with an integrity error
- 0.12.6 publishes a native artifact only for macOS Apple silicon; Linux
  x86-64 remains a non-blocking validation lane with no published artifact

## [0.12.5] - 2026-09-22

- fix(mcp): replace Desktop-launched MCP with the supplied signed Node frontend,
  which owns the native-tools pipe and long-lived typed completion relay while
  its Rust MCP child runs without host capabilities
- fix(mcp): preserve direct MCP `initialize` and `tools/list` service with a
  fixed bounded warning when the optional frontend path is malformed, missing,
  non-executable, or fails during `exec`; fallback never opens a native-host
  client
- 0.12.5 publishes a native artifact only for macOS Apple silicon; Linux
  x86-64 remains a non-blocking validation lane with no published artifact

## [0.12.4] - 2026-09-22

- fix(core): continue Codex relay discovery past stale endpoints so a live
  listener beyond the first sixteen candidates can receive completion notices

## [0.12.3] - 2026-09-21

- fix(core): compare Codex readable and writable root echoes as exact multisets
  so app-server ordering differences no longer reject an otherwise identical
  managed Projects grant
- test(core): keep extra, missing, duplicate, malformed, and reordered readable
  and writable root cases fail-closed under focused regression coverage
- 0.12.3 publishes a native artifact only for macOS Apple silicon; Linux
  x86-64 remains a non-blocking validation lane with no published artifact

## [0.12.2] - 2026-09-21

- feat(codex): accept multiple operator-authorized write roots through
  `workspace_roots = ["...", "..."]`, while preserving the legacy singular
  `workspace_root` declaration and rejecting configurations that declare both
- fix(codex): admit write workdirs below any configured root, require every root
  in a managed Projects policy, and generate the complete root set for unmanaged
  Projects without widening access to the rest of the home directory
- docs(config): expose the plural snapshot and operator contract, including that
  a write role admitted under one root receives write access to every configured
  Projects root
- 0.12.2 publishes a native artifact only for macOS Apple silicon; Linux
  x86-64 remains a non-blocking validation lane with no published artifact

## [0.12.1] - 2026-09-21

- fix(cli,mcp): `cancel` returns the current agent view with top-level status
  after enqueuing the durable cancellation, restoring the archived response
  shape shared by CLI, socket, and MCP transports
- fix(core): lost-process reconciliation drains late pending steer/cancel
  commands through the shared completion path, and finalizes pending commands
  inside the transaction that commits the lost status so a crash cannot strand
  them behind a terminal row
- fix(cli): the CLI parser carries the package version so `--version` prints it
- fix(config): reload adoption and rejection are recorded in bounded process
  logs naming the active SHA-256 revision; adoption is logged only after the
  snapshot is installed, and no config contents, paths, environment values, or
  credentials are persisted
- fix(mcp): the `start` tool description names both direct host-visible engine
  aliases and is pinned to the archived Python baseline plus exactly that
  guidance
- fix(platform): parse `/proc/<pid>/stat` as bytes so a non-UTF-8 command name
  can no longer discard leader identity and fail Linux cleanup observation
- fix(core): classify a non-socket Claude UDS endpoint as unavailable instead
  of session-gone; Linux reports `ECONNREFUSED` for both
- test(core,store): derive Codex app-server fixture paths from the host temp
  directory and run the `chflags uchg` reopen fixture only on macOS so Linux
  validation passes
- chore: remove the completed Rust migration archive and its migration-only
  `xtask evidence verify` and `xtask qualify` commands; release safety stays
  with the workspace checks, source archive verification, the sealed native
  release, and the Desktop transport fixture
- 0.12.1 publishes a native artifact only for macOS Apple silicon; Linux
  x86-64 remains a non-blocking validation lane with no published artifact

## [0.12.0] - 2026-09-20

- feat!: make the self-contained Rust broker the sole primary implementation
- build!: replace interpreter packages with a sealed macOS Apple-silicon
  binary; Linux release and qualification are deferred
- ci!: gate releases with Rust-only macOS checks and sealed verification while
  retaining Linux as visible non-blocking validation
- fix(codex): honor safe plugin-declared relative hook manifest paths
- feat!: remove the deprecated Qwen runtime; legacy Qwen configuration now
  fails with explicit removal guidance

BREAKING CHANGE: Python packages, interpreter-based installation, and the
Python implementation are no longer shipped from the primary branch. Install
the native release artifact and use the built-in `codex`, `claude`, or `glm`
runtime aliases.

## [0.11.15] - 2026-09-15

- docs(codex): clarify snapshot helper contracts
- refactor(codex): isolate runtime snapshot helpers
- fix(codex): prepare project trust before sealing runtime

## [0.11.14] - 2026-09-15

- test: isolate native settings runtime homes
- fix(config): reserve native capability and auth controls
- docs(config): clarify native preference validation
- test(config): materialize snapshot-restored settings
- fix(config): tighten native_settings review findings
- feat(config): declarative native runtime settings
- fix(codex): reuse managed permission profiles
- fix(codex): keep runtime test caches sandboxed

## [0.11.13] - 2026-09-15

- fix(codex): preserve reviewed network rules on prepare

## [0.11.12] - 2026-09-15

- feat(codex): opt into reviewed workspace network

## [0.11.11] - 2026-09-15

- fix(codex): verify named-profile runtime roots

## [0.11.10] - 2026-09-15

- fix(codex): verify Projects profile on launch

## [0.11.9] - 2026-09-14

- feat(codex): add native permission guards

## [0.11.8] - 2026-09-14

- fix(launchd): raise API broker file limit (#46)

## [0.11.7] - 2026-09-10

- docs(claude): clarify unlabelled home and config env contract

## [0.11.6] - 2026-09-10

- docs(claude): document native credential home
- fix(claude): preserve host HOME for native credential state
- fix(claude): resolve native config home from login account

## [0.11.5] - 2026-09-09

- fix(capacity): preserve host path in launchd

## [0.11.4] - 2026-09-09

- refactor(codex): split error classification
- fix(codex): classify provider overloads

## [0.11.3] - 2026-09-09

- fix(adapters): inherit installed Rust homes

## [0.11.2] - 2026-09-09

- fix(codex): admit public GPT-6 profiles

## [0.11.1] - 2026-09-09

- fix(capacity): restore forecast-based routing

## [0.11.0] - 2026-09-09

- test(roles): canonicalize fixture command path
- fix(api): isolate list long polls
- test(supervisor): prepare detached launch fixtures
- test(capacity): remove obsolete history coverage
- test(service): follow supervisor preparation boundary
- test(service): remove obsolete bootstrap wait
- fix(claude): reap leader after group cancellation
- test(runtime): execute preparation at supervisor boundary
- test(capacity): read current identity snapshots
- feat(cli)!: fold waiting into start
- feat: restore push delivery and operator tools
- refactor!: shrink public agent tool surface
- refactor!: remove completion delivery pipeline
- refactor(capacity)!: keep only current snapshots
- test(runtime): align startup fixtures with supervisor preparation
- fix(runtime): require process proof for startup loss
- test(runtime): prepare captured supervisor launches
- refactor(tui)!: remove standalone terminal dashboard
- feat(supervisor): remove automatic runtime timers
- test(supervisor): prove explicit cancel cleanup
- feat(status): expose factual agent revisions
- refactor(supervisor): own preparation before ready
- fix(runtime): tighten resolved role decoding
- fix(runtime): preserve resolved role compatibility
- fix(runtime): compile isolated resolved role contracts
- test(claude): cover inherited host environment
- refactor(config): move runtime readiness out of launch
- refactor(adapters): compile resolved roles through one boundary
- feat(auth): default to native runtime accounts
- feat(roles): resolve one canonical runtime contract
- refactor(adapters): inherit host development environment

## [0.10.2] - 2026-09-08

- fix(snapshots): preserve legacy resume compatibility
- fix(claude): protect scoped credential config
- fix(claude): preserve scoped credential state
- feat(claude): add scoped CLI login
- fix(claude): scope CLI credential refresh state

## [0.10.1] - 2026-09-08

- Fix bounded MCP release smoke (#27)

## [0.10.0] - 2026-09-08

- Stabilize core execution and recovery (#25)

## [0.9.0] - 2026-09-07

- feat(codex): review developer write escalations
- fix(codex): disable login shells for developer environments
- docs(runtime): link supported developer environment adapters
- refactor(codex): move launch environment preparation
- refactor(claude): keep adapter focused
- fix(claude): render effective MCP environment
- fix(qwen): preserve declared developer toolchains
- feat(claude): integrate developer-environment presets and command policy
- feat(codex): apply developer environments to launches
- feat(qwen): connect declared developer environment providers
- fix(codex): align native thread grants with the app server schema
- test(runtime): preserve lexical command path evidence
- feat(runtime): add configurable command denial policies
- feat(config): add declared developer environment presets
- fix(codex): provision declared Rust for shell and MCP

## [0.8.0] - 2026-09-06

- fix(codex): negotiate experimental workspace roots capability
- docs: declare owner-authorized native and agent-run parity
- refactor(adapters): separate static policy and TOML helpers
- fix(rust): provision coding shells and MCP environments explicitly
- docs: verify worktree source selection before tests
- fix(start): distinguish handoff and supervisor diagnostics
- test(start): verify prepare stage before release
- fix(start): log preparation stage entry
- fix(start): log preparation stage on failure
- test(codex): cover multiple runtime workspace roots
- fix(codex): send runtime workspace roots
- docs: clarify scoped fixture checks and Codex root grants
- fix(resume): exclude unsupported opencode continuation
- feat(resume): expose native continuation across runtime adapters
- fix(resume): preserve profile grants and accepted request replay
- chore: checkpoint native session continuation
- feat(adapters): carry native session identity in launch plans
- fix(codex): retain extended context settings on generation

## [0.7.3] - 2026-09-05

- fix(codex): preserve streamed whitespace and early-exit evidence

## [0.7.2] - 2026-09-05

- fix(runtime): harden supervised execution and workflow recovery

## [0.7.1] - 2026-09-05

- fix: keep Python hooks working in isolated runtime homes (#15)

## [0.7.0] - 2026-09-05

- build!: require Python 3.14

## [0.6.4] - 2026-09-05

- feat(delivery): add failure guidance to completion notices

## [0.6.3] - 2026-09-04

- fix(glm): preserve bounded stderr diagnostics
- docs(release): add one-command operational runbook
- feat(release): automate verified publication and local deployment

## [0.6.2] - 2026-09-04

- Prevented accepted starts from being marked lost while a live coordinator is
  preparing them or waiting to hand ownership to the supervisor.
- Bounded preparation ownership to 120 seconds, preserving orphan cleanup and
  preventing expired or cancelled workers from launching a runtime later.
- Kept supervisor process-group updates valid after a successful handoff and
  moved process identity probes outside SQLite write transactions.
- Added schema v11 startup ownership fields. Restart long-lived agent-run
  processes and reconnect MCP clients after upgrading.

## [0.6.1] - 2026-09-04

- Fixed the dashboard reporting the API as unavailable on stores with many
  agents: a refresh now lists orchestrators and active agents only, fetches
  finished agents just for the opened session at most every 15 seconds, skips
  transcript polling for runtimes that never stream one, and waits up to 20
  seconds for a busy server instead of 5.

## [0.6.0] - 2026-09-04

- Added `agent-run-tui`, a standard-library curses dashboard shipped as the
  separate `agent_run_tui` package: orchestrator sessions with host runtime,
  working directory, and child counters, then each session's running and
  finished agents as bordered cards with status glyphs and elapsed time. Data
  is read only through the JSON-RPC socket in a background thread; keys work on
  Latin and Russian layouts; selection follows session and agent ids across
  refreshes.
- Resolved session titles from the host runtime's own files (Claude Code
  custom titles and history, Codex thread names and rollout metadata) with
  validated ids and incremental, cached reads.
- Added the read-only `list_orchestrators` tool to the shared CLI/MCP/socket
  surface with active and total child counters per orchestrator session, and
  exposed the launch `effort` on agent views.

## [0.5.0] - 2026-09-04

- Added a bounded priority bonus for available manual Codex reset credits, read
  from the existing account-scoped app-server response and preserved in capacity
  snapshots. One reset gives a 1.5x factor; further resets approach a 2x ceiling.
- Kept exhausted and stale quota routes excluded regardless of reset credits;
  accounts return only when fresh quota evidence confirms availability. Credits
  are never redeemed automatically, and Spark receives no reset-credit bonus.
- Exposed the reset-credit count and bonus separately from configured priority
  multipliers. Older snapshots without the optional metadata remain compatible.

## [0.4.1] - 2026-09-03

- Fixed successful engines being misclassified when they exit before process-group
  discovery; completion still requires the actual outcome, a verified answer,
  and proof that no owned processes remain.
- Prevented native cancellation of unverified owned processes, including PIDs
  observed in the wrong group, and tightened leaderless-group cleanup checks.
- Standardized compact Claude and Codex completion notices with one packaged
  template. The MCP `start` description and `doc completion` now share the
  handling instructions and format instead of repeating them in every notice.
- Stabilized asynchronous-start tests without imposing execution order or
  weakening durable-failure and idempotency checks.
- Kept schema v10 and relay wire v1/v2 compatibility. Reconnect existing Codex
  agent-run MCP hosts after upgrading to load the compact notice template.

## [0.4.0] - 2026-09-03

- Added quota-aware runtime ordering through the CLI, MCP, and socket API,
  using burn rate, reset time, and shared physical quota pools.
- Added configurable runtime, account, and quota-lane priority multipliers;
  exhausted or unknown routes remain excluded from the usable order.
- Replaced automatic raw-quota context with a changed-only priority summary,
  while leaving role and model suitability decisions to the orchestrator.
- Improved scheduled quota collection with account-scoped failure isolation,
  honest degraded outcomes, current OmniRoute cache observations, and reset
  timestamp jitter handling.
- Routed Codex completion delivery exclusively through the signed Desktop
  relay, with no direct queue fallback.
- Structured agent completion notices as a concise list with ID, status,
  runtime/model/effort, and result lookup guidance; retained compatibility
  with older relay hosts.
- Added the schema v10 quota-topology migration. Restart long-lived agent-run
  processes after upgrading, and reconnect the agent-run MCP connection in
  Codex to activate the richer notification format.

## [0.3.1] - 2026-09-02

- Accepted the LSP plugin's `PostToolUseFailure` hook in Codex plugin trust
  materialization, restoring Codex launches after the plugin event expansion.

## [0.3.0] - 2026-09-02

- Made `start` durably asynchronous: runtime authentication, materialization,
  preparation, spawn, and READY no longer block neighboring requests, while
  cancellation and unowned-start reconciliation remain exact.
- Routed one-shot CLI starts through the resident broker so accepted workers
  outlive the CLI process and unavailable brokers fail explicitly.
- Added schema v9 immutable, bounded, redacted Codex queue attempt evidence and
  exposed the latest safe summary through `status.delivery.last_attempt`.
- Resolved the public Claude `fable` alias to Claude Fable 5.1 while preserving
  the stable configured and persisted model id.
- Stopped read-only state inspection from attempting to change database modes.

## [0.2.1] - 2026-09-01

- Prevented an exiting resident API daemon from unlinking the replacement
  daemon's Unix socket during launchd restarts.

## [0.2.0] - 2026-09-01

- Added isolated multi-account Codex authentication, account-aware workflows,
  and per-account capacity reporting.
- Made MCP a uniform thin proxy over the launchd-manageable resident Unix-socket
  daemon while preserving the shared 17-tool surface.
- Hardened detached supervision with targeted child reaping,
  `posix_spawn(..., setsid=True)`, bounded startup headroom, and exact cleanup
  evidence under launchd load.
- Restored live Codex model metadata automatically and allowed slow app-server
  initialization without truncating the agent execution budget.
- Added native Claude/Fable capacity sampling with verified TLS handling.
- Fixed Qwen macOS sandbox startup by bypassing the Xcode Git shim; Qwen can run
  without unsupported Agent-LSP/codegraph grants.

## [0.1.0] - 2026-08-31

First public release.

- Durable asynchronous runs for Codex, Claude Code, GLM, Qwen Code, and OpenCode.
- Shared CLI, MCP, and Unix-socket JSON-RPC tool surface.
- SQLite-backed state, verified outcomes, delivery, capacity tracking, and run statistics.
- Resumable multi-step workflows with parallel and pipeline execution.
- Isolated runtime homes, explicit read/write permissions, diagnostics, and operator guide.

[Unreleased]: https://github.com/DKotsyuba/agent-run/compare/v0.12.4...HEAD
[0.12.4]: https://github.com/DKotsyuba/agent-run/compare/v0.12.3...v0.12.4
[0.12.3]: https://github.com/DKotsyuba/agent-run/compare/v0.12.2...v0.12.3
[0.12.2]: https://github.com/DKotsyuba/agent-run/compare/v0.12.1...v0.12.2
[0.12.1]: https://github.com/DKotsyuba/agent-run/compare/v0.12.0...v0.12.1
[0.12.0]: https://github.com/DKotsyuba/agent-run/releases/tag/v0.12.0
[0.11.15]: https://github.com/DKotsyuba/agent-run/releases/tag/v0.11.15
[0.11.14]: https://github.com/DKotsyuba/agent-run/releases/tag/v0.11.14
[0.11.13]: https://github.com/DKotsyuba/agent-run/releases/tag/v0.11.13
[0.11.12]: https://github.com/DKotsyuba/agent-run/releases/tag/v0.11.12
[0.11.11]: https://github.com/DKotsyuba/agent-run/releases/tag/v0.11.11
[0.11.10]: https://github.com/DKotsyuba/agent-run/releases/tag/v0.11.10
[0.11.9]: https://github.com/DKotsyuba/agent-run/releases/tag/v0.11.9
[0.11.8]: https://github.com/DKotsyuba/agent-run/releases/tag/v0.11.8
[0.11.7]: https://github.com/DKotsyuba/agent-run/releases/tag/v0.11.7
[0.11.6]: https://github.com/DKotsyuba/agent-run/releases/tag/v0.11.6
[0.11.5]: https://github.com/DKotsyuba/agent-run/releases/tag/v0.11.5
[0.11.4]: https://github.com/DKotsyuba/agent-run/releases/tag/v0.11.4
[0.11.3]: https://github.com/DKotsyuba/agent-run/releases/tag/v0.11.3
[0.11.2]: https://github.com/DKotsyuba/agent-run/releases/tag/v0.11.2
[0.11.1]: https://github.com/DKotsyuba/agent-run/releases/tag/v0.11.1
[0.11.0]: https://github.com/DKotsyuba/agent-run/releases/tag/v0.11.0
[0.10.2]: https://github.com/DKotsyuba/agent-run/releases/tag/v0.10.2
[0.10.1]: https://github.com/DKotsyuba/agent-run/releases/tag/v0.10.1
[0.10.0]: https://github.com/DKotsyuba/agent-run/releases/tag/v0.10.0
[0.9.0]: https://github.com/DKotsyuba/agent-run/releases/tag/v0.9.0
[0.8.0]: https://github.com/DKotsyuba/agent-run/releases/tag/v0.8.0
[0.7.3]: https://github.com/DKotsyuba/agent-run/releases/tag/v0.7.3
[0.7.2]: https://github.com/DKotsyuba/agent-run/releases/tag/v0.7.2
[0.7.1]: https://github.com/DKotsyuba/agent-run/releases/tag/v0.7.1
[0.7.0]: https://github.com/DKotsyuba/agent-run/releases/tag/v0.7.0
[0.6.4]: https://github.com/DKotsyuba/agent-run/releases/tag/v0.6.4
[0.6.3]: https://github.com/DKotsyuba/agent-run/releases/tag/v0.6.3
[0.6.2]: https://github.com/DKotsyuba/agent-run/releases/tag/v0.6.2
[0.6.1]: https://github.com/DKotsyuba/agent-run/releases/tag/v0.6.1
[0.6.0]: https://github.com/DKotsyuba/agent-run/releases/tag/v0.6.0
[0.5.0]: https://github.com/DKotsyuba/agent-run/releases/tag/v0.5.0
[0.4.1]: https://github.com/DKotsyuba/agent-run/releases/tag/v0.4.1
[0.4.0]: https://github.com/DKotsyuba/agent-run/releases/tag/v0.4.0
[0.3.1]: https://github.com/DKotsyuba/agent-run/releases/tag/v0.3.1
[0.3.0]: https://github.com/DKotsyuba/agent-run/releases/tag/v0.3.0
[0.2.1]: https://github.com/DKotsyuba/agent-run/releases/tag/v0.2.1
[0.2.0]: https://github.com/DKotsyuba/agent-run/releases/tag/v0.2.0
[0.1.0]: https://github.com/DKotsyuba/agent-run/releases/tag/v0.1.0
