# Provider configuration v2

Schema v2 permits an empty catalog; configured providers require Codex and
Claude Code declarations and may also use OpenCode. Agent Quota owns quota
collection and calculations. The retained provider `limits_source` and
collector settings are legacy migration inputs, not the source of Run's new
quota decisions.
Provider ids are operator-chosen; names such as `codex-plus` and `glm` carry
no built-in subscription or model-intelligence meaning. The orchestrator
selects a configured model id. Profiles, skills, and MCP definitions remain
in their existing canonical role catalogs. Harness settings own native
binary/home, hooks, plugin assets, workspace policy, and unowned native
settings. Account weights and prose recommendations live only in TOML.

`[harnesses.codex].fast_mode = true` forces Fast for every provider/model/account
using Codex, including pool members. Omission or `false` preserves explicit
per-request `fast` behavior. Starts capture the effective value once; retries
keep it. Each resume captures the current flag while preserving the parent
role, native session and sealed assets. Turning off the flag stops forcing
Fast for later resumes; an original explicit `fast = true` still applies.
Running harness processes are not reconfigured. Claude Code rejects an enabled
flag. Effective launch flags never alter raw request/replay hashes.

Codex receives `-c service_tier=fast -c features.fast_mode=true`. Fast has higher
subscription usage or credit rates; see [OpenAI Fast mode documentation](https://learn.chatgpt.com/docs/agent-configuration/speed).

```toml
schema_version = 2

[capacity.quota]
socket = "/absolute/path/to/agent-quota.sock"
# expected_service_uid = 1000 # optional; defaults to Run's effective UID

[harnesses.codex]
fast_mode = false # set true to force Fast across this harness
binary = "/usr/local/bin/codex"
home = "/var/lib/agent-run/codex"

[harnesses.claude-code]
binary = "/usr/local/bin/claude"
home = "/var/lib/agent-run/claude"

[providers.codex]
harness = "codex"
connection = { kind = "native" }
auth_family = "openai"
limits_source = "none"
recommendations = ["Use for coding work."]

[[providers.codex.models]]
id = "gpt"
native_model = "gpt"
allowed_params = { effort = ["medium", "high"] }
params = { effort = "medium" } # applied when start omits effort
restrictions = ["web_tools_disabled"]

[[providers.codex.bindings]]
label = "personal"
account = "acct-personal"

[providers.codex-plus]
harness = "codex"
connection = { kind = "native" }
auth_family = "openai"
limits_source = "none"
priority_multiplier = 5.0

[[providers.codex-plus.models]]
id = "gpt"

[[providers.codex-plus.bindings]]
label = "plus"
account = "acct-personal"
priority_multiplier = 2.0
models = ["gpt"]

[providers.glm]
harness = "claude-code"
connection = { kind = "custom", endpoint = "https://gateway.example/api/anthropic", protocol = "messages" }
auth_family = "anthropic"
limits_source = "none"

[[providers.glm.models]]
id = "glm-5.3"
native_model = "glm-5.3[1m]"

[[providers.glm.bindings]]
label = "work"
account = "acct-glm"
```

`native` preserves the harness's existing login and endpoint. It has no
custom URL or implicit API-key conversion. `custom` requires an explicit
HTTPS endpoint and protocol: Codex accepts `responses`, Claude Code accepts
`messages`. Custom connections use bearer authorization by default and may
set `auth_header = "x_api_key"` for compatible gateways. Loopback HTTP
requires `allow_loopback_http = true` inside the
custom connection. Credentials are selected from registered account
references, never embedded in this file; see the [account registry](account-registry.md).
Codex's Responses provider settings
and Claude's Messages gateway environment are materialized by their later
adapter consumer; this config parser does not launch either CLI. See the
[Codex custom-provider configuration](https://learn.chatgpt.com/docs/config-file/config-advanced)
and [Claude gateway configuration](https://code.claude.com/docs/en/llm-gateway-connect)
for the native wire settings.

### Fixed gateway headers

A custom Claude Code connection may send fixed, non-secret HTTP headers with
every request through an optional `headers` table inside `connection`. This
supports gateways that require a request-correlation header, such as OpenCode
Go:

```toml
[providers.opencode-go]
harness = "claude-code"
connection = { kind = "custom", endpoint = "https://opencode.ai/inference/go/anthropic", protocol = "messages", headers = { "x-opencode-session" = "{launch}" } }
auth_family = "anthropic"
limits_source = "none"

[[providers.opencode-go.models]]
id = "example-model"

[[providers.opencode-go.bindings]]
label = "work"
account = "acct-opencode-go"
```

The launch sets `ANTHROPIC_CUSTOM_HEADERS` from the sorted `Name: Value`
lines; when no headers are configured the variable is removed, so a host value
is never inherited. The only substitution is the exact token `{launch}`, replaced
at each launch by a fresh non-secret identifier; any other `{` or `}` is
rejected. Configuration loading fails closed unless:

- the connection is `custom` and the harness is `claude-code` (Codex custom
  headers are unsupported, and `native` connections accept no headers);
- there are at most 8 headers, names are RFC 7230 tokens of at most 64
  characters and unique ignoring case;
- no name is, case-insensitively, `authorization`, `proxy-authorization`,
  `x-api-key`, `cookie`, `host`, `content-length`, `content-type`,
  `transfer-encoding` or `connection`, starts with `anthropic-` or
  `x-stainless-`, or contains `token`, `secret`, `password`, `key`,
  `credential` or `auth`; credentials belong in the account registry;
- each value is non-empty printable ASCII of at most 256 characters, with no
  CR, LF or NUL and no leading or trailing space.

Headers are part of the sealed provider configuration, so changing them
changes its digest; configurations without headers keep their existing digest.

Historical model aliases such as `fable` → `claude-fable-5-1` and
`glm-5.3` → `glm-5.3[1m]` become explicit `native_model` entries when
migrated. Any model-specific hard policy belongs in typed `restrictions`;
the loader does not infer it from a model name. Existing role and request
constraints remain authoritative.

The quota/session handoff is `ProviderConfig::load_if_changed(home, revision)`
for exact-byte SHA reload, `ProviderConfig::resolve_catalog(account_records)`
for registry-checked provider bindings, and `ProviderConfig::snapshot()`
for a secret-free digest and stable ids. A failed reload returns an error;
the caller retains its last valid cached value. Public starts use schema 2;
the v1 `Config` remains only for historical rows, migration inputs, and shared
operator controls. It is not a public runtime launch alias.
The [provider materialization contract](provider-materialization.md) names
the sealed adapter APIs that consume this configuration.
The [one-time migration plan](provider-migration.md) requires explicit
runtime, model, account, and harness mappings before any later apply step.

## Ordered Claude API and subscription accounts

An explicitly ordered Claude provider can use an API credential first and a
separate native Claude Code login next. Register the accounts before adding the
bindings; the configuration contains references, never credential values.

```toml
[providers.claude]
harness = "claude-code"
connection = { kind = "custom", endpoint = "https://api.anthropic.com", protocol = "messages", auth_header = "x_api_key" }
auth_family = "anthropic"
limits_source = "none"
ordered_accounts = true

[[providers.claude.models]]
id = "example-model"

[[providers.claude.bindings]]
label = "api"
account = "acct-api"
priority_multiplier = 2.0

[[providers.claude.bindings]]
label = "subscription"
account = "acct-subscription"
priority_multiplier = 1.0
connection = { kind = "native" }
```

Register the API account with an explicit file/environment/Keychain reference
and the subscription with `named:claude-code:<label>`. The named login is owned
by the Claude worker's OS account. In a shared system service, authenticate in
that worker's private home; the shared broker's home is not the login store.

Ordered bindings use descending `priority_multiplier`, with declaration order
breaking ties. Known exhausted and disabled accounts are still excluded; an
unknown balance does not move a lower-priority account ahead. This initial
mode is limited to Claude custom Messages providers with
`limits_source = "none"`; native is the only binding connection override. An explicitly pinned
account never switches. Existing executions retain their frozen configuration.

A structured Claude billing error on a custom binding, or an authoritative
quota rejection on an ordered binding, temporarily excludes that provider/account
binding for one hour. New automatic executions select the next eligible binding
immediately. The cooldown survives a broker restart and is separate from quota
measurements: a billing error does not establish a zero balance. After one hour,
the normal priority order applies again, subject to any independently known quota.
An explicitly pinned account is never redirected; while it is cooling down the
request is refused with `binding_cooldown` and the remaining wait in seconds.
Executions already admitted before the failure are not cancelled or retroactively
moved to another account.

The current execution can advance after verified cleanup and history/input proof.
Generic throttling, authentication and network errors do not authorize a switch
or a cooldown. An unresolved tool call or uncertain history stops continuation;
the original task is not resent. See
[session continuity](session-continuity.md) for evidence and limits.

## MCP selection and tool caps

Declare a stdio server once in the shared catalog. `global` is optional and
defaults to false; when true it joins every new run's profile-selected MCPs.
Only selected servers are materialized. Existing running/resumed sessions keep
their frozen selection rather than importing newly configured global servers.

```toml
[mcp.tracker]
transport = "stdio"
command = "/opt/tracker/connect"
env_from = ["TRACKER_TOKEN"]
global = true
allowed_tools = ["get_context", "update_task"]
```

The profile's Markdown TOML front matter supports both forms:

```toml
mcp = ["codegraph", { name = "tracker", allowed_tools = ["get_context"] }]
```

Omitting `allowed_tools` means all tools; an empty list means none. Exact,
case-sensitive names use 1–128 ASCII letters, digits, underscores, hyphens or
dots, with no wildcard patterns. The profile cap intersects the catalog cap,
so it cannot add permissions. Duplicate server selections, duplicate tool names
and unknown fields are rejected. Selected servers retain profile order, followed
by remaining global servers in catalog order. Existing string-only profiles and
historical snapshot digests remain compatible. These controls require schema 2.

| Harness | Enforcement |
|---|---|
| Codex | Generated `mcp_servers.<name>.enabled_tools` is the native allowlist. |
| Claude Code, including GLM | Before each attempt/resume, Agent Run initializes the selected stdio MCP with the launch environment, reads every `tools/list` page, and passes the complement to native `--disallowedTools`. The harness connects directly to the original server. |

Claude discovery has a 15-second per-server and 30-second total budget, plus
bounded process teardown. A missing allowed name, incomplete/malformed catalog,
catalog change during discovery, authentication failure or timeout refuses the
restricted launch. An empty allowlist uses `mcp__server__*` without discovery.
No filter means no discovery. Temporary discovery processes are cleaned on
success, failure and cancellation; captured identities allow crash recovery.

Claude's inversion is a **launch-time catalog snapshot**, not a permanent
positive allowlist: a tool added after discovery or exposed differently to
another client/connection can escape the generated deny list. Use a stable
catalog for the duration of a run. Restricted discovery currently rejects
`${...}` interpolation in command/arguments and native names containing dots
or other unsupported characters instead of guessing Claude's normalization.
Server names containing `__` or ending in `_` are also rejected for ambiguous
native namespace boundaries. Declared `env_from` values remain supported.

The frozen role records each selection's source (`global`, `profile`, `both`)
and effective cap. Start/resume diagnostics summarize these **selected** servers;
they do not assert live availability. Resume refuses newly revoked server/tool
rights and never expands a previous session's frozen cap. Tool filtering is a
harness feature, not operating-system isolation or server-side authorization
against separate connections made with other granted tools.

## Quota service

Configure collection in Agent Quota and point Run to its local socket as shown
above. The required provider `limits_source` field may be `"none"`; that value
does not disable Agent Quota. Retained `exec` collector settings are compatibility and migration
inputs; Run no longer executes them. Previously installed collection jobs
must be migrated separately; old Run history does not feed new selection.
The former `lua` and `codex_appserver` choices are retired.

## OpenCode v2

OpenCode CLI runs as an owned `serve --stdio --port 0` child. Stdin is
its lifetime lease; commands use authenticated loopback HTTP. Each lineage
retains private config/data/cache/state roots and its own native database.
The selected `native_model` must be an explicit `provider/model` reference.

Qualified versions are exactly **2.3.0** and **2.0.28**. `/api/info` must report
one of them and the owned child's PID, otherwise the launch refuses before any
session or input. 2.3.0 is the official release to install (the release asset
`opencode-linux-x64.tar.gz` for v2.3.0). 2.0.28 stays qualified because its
protocol and schema were qualified from a source build and existing seals name
it, but it has no official release binary. A session sealed under one version
resumes only under that same version; see
[session continuity](session-continuity.md#opencode-v2-continuation).

The worker capability never enters the `serve` environment: OpenCode would pass
it to every local MCP server and to its shell tool. It reaches only the worker
MCP, through a private per-attempt context file; see
[private launch variables](provider-materialization.md#private-launch-variables-and-tool-processes).

```toml
[harnesses.opencode]
binary = "/opt/bin/opencode"
home = "/var/lib/agent-run/opencode"

[providers.opencode-zen]
harness = "opencode"
connection = { kind = "native" }
auth_family = "opencode"
limits_source = "none"
[[providers.opencode-zen.models]]
id = "muse-free"
native_model = "opencode/muse-spark-1.3-contributor-free"
[[providers.opencode-zen.bindings]]
label = "public"
account = "acct-opencode-public"
```

Register `acct-opencode-public` with family `opencode` and reference
`native:opencode` for the isolated native public connection. It imports no
host login/database. Zen API keys may instead use an existing environment,
file or Keychain reference; only the selected value reaches child
`OPENCODE_API_KEY`. Named native accounts and custom gateways are unsupported.
If Agent Quota has no usable observation for this model, capacity is unknown,
not an unlimited quota or a measured balance.

The managed role supplies exact MCP caps and skill permissions. Native editor
access is scoped to the workdir; read-only shell asks are rejected. These are
tool filters, not an OS filesystem/network sandbox. Explicit finish is required.
Start, cancel, idle steering and same-account explicit resume use the shared
supervisor. Native shell background completion resumes the same parent through
OpenCode's own notification mechanism; native inputs and synthetic notifications
are journaled by stable native IDs. No second background scheduler is added.

Busy detection uses `session.execution.started` plus fresh native idle proof
(awaitIdle, then an empty inbox); 2.3.0 emits no `session.status` events and
answers awaitIdle with 204 and no body. Message types and part types Run does not
journal, such as 2.3.0 `idle` messages and `reasoning` parts, are skipped. After
a recorded finish whose native pass is still running, and on cancel, Run
interrupts the pass and awaits fresh idle proof for at most 5 seconds before
stopping `serve`, so the session stays settled and can be sealed for resume.

A configured launch-owned Bridge channel may admit a queued input only after
fresh native idle and empty-inbox proof. The original absolute deadline, logical
admission identity and exact input acknowledgement remain binding; admission is
not a claim that the model consumed the input. Busy/gapped evidence cannot certify
idle. One-shot, research and IPC-isolated launches do not receive this channel.
Arbitrary hooks/plugins, native tuning, Codex workspace roots, restricted research,
effort and output schemas are still refused. Bridge lifecycle plugin projection
and standalone OpenCode root delivery require their separate trusted integration;
other native background/subagent variants are not advertised as qualified. Larger native responses/snapshots retain the
existing bounded transport and transcript limits.

## Managed attempt selector (experimental)

The `managed_attempt_binding` table is absent by default. An explicit table
selects only the named dedicated role when `enabled = true`; ordinary roles
remain ordinary. It requires `role_name`, `pool_server`, `host_id`, and the
lowercase SHA-256 values `pool_executable_sha256` and `bridge_hook_sha256`;
`codex_code_mode_host_sha256` is optional (see Codex Code Mode below).
The selector refuses plugins, skills, non-stdio or unrestricted MCP servers,
unapproved component binaries, non-explicit completion, research and broader
roles. The selected Pool command must be the pinned binary with exactly
`mcp --config <absolute-path>` and an explicit safe tool subset; the private
worker surface is limited to `finish` and `health_reply`: the supervisor sets
`AGENT_RUN_WORKER_MANAGED=1` and the worker MCP then serves only those two
tools and registers a completion-only catalog, which is health-capable but
never satisfies Pool enrollment. A managed Codex thread runs with
`environments: []` and therefore requests and verifies no runtime workspace
roots (codex-cli 0.167.0 echoes `runtimeWorkspaceRoots: []`); cwd, the
read-only profile, approval and network stay as admitted. Its MCP status is
re-read until every server is connected or one failed, within 30 seconds,
then verified exactly once. Codex live configuration
and native inventory are checked before input;
their digests and exact bound launch/kernel identity are recorded by the owning
supervisor. This switch does not itself enable a Pool-managed command or claim
native-provider qualification. Do not point it at unreviewed executables.
Worker capability variables remain in the harness environment in this revision;
the managed native role must therefore exclude shell and unsafe subprocess tools.
Scoping these variables solely to the worker MCP channel remains backlog work.

A selected managed launch also requires the configured Bridge transport plus
`BRIDGE_NATIVE_OWNER_CHANNEL` and `BRIDGE_NATIVE_OWNER_DIRECTORY` in its selected
environment. Run creates a separate per-launch `BRIDGE_NATIVE_OWNER_SOCKET_V2`
address before exec and suppresses the v1 owner socket. The address does not grant
authority: before exact native qualification is durable it returns the definitive
`qualification_pending` refusal without writing model input. Egress currentness
uses the pinned Bridge `run_actor_lookup_v1` and a fresh exact leader check; a
missing or historical actor cannot be substituted by an ingress observation.
Pool can discover `attempt_egress_actor_recovery_v1` and opt into a correlated
negative hint for that exact launch: retry the same key for `awaiting_root_hook`,
request operator/relaunch action for `relaunch_required`, and fail closed for
`unavailable`. The hint never supplies actor authority or a current receipt.

The Pool resident that connects to `<run-home>/attempt.sock` must run under a
service supervisor such as `systemd --user` (a `[Service]` unit of the same
user), not directly from an interactive login. Run verifies the resident's
complete same-UID kernel ancestry up to PID 1, including each ancestor's
environment for a Run launch marker. A non-dumpable ancestor, such as the
per-session `sshd` child of an SSH login or a process that changed
credentials, denies that read; the connection then fails closed as
`peer_not_authorized` before any request is read. Restart the resident through
its unit after upgrading either component. Exact-key enrollment lookup and abort
do not read the Bridge reader credential; a missing or mismatched credential
refuses only Bridge-backed operations with `recovery_required`/`operator`.

`[[managed_attempt_binding.bridge_custody_readers]]` grants effect recovery to
separately configured same-UID Bridge builds. Each entry pins `reader_id`,
`worker_uid`, `executable`, `executable_sha256`, `run_route`, `bridge_route`,
and one or both of `producer_scopes` and `producer_scope_prefixes`. Prefixes
end in `/` and authorize only nonempty descendants with no empty path segments;
all prefixes are non-overlapping and cannot overlap another reader's literal.
Recovery retains each effect's original run and Bridge routes;
repointing a grant cannot retarget an existing effect or tombstone. Disabling the
switch refuses new delivery while configured readers retain exact-key recovery.
Operator-reviewed builds must be actual native components: interpreter names,
scripts and renamed copies of common installed interpreters are refused.
Doctor reports active-currentness, admission, retirement, qualification,
owner-effect and prepared-enrollment headroom, plus the count of permanent
admitted/aborted enrollment keys, without
exposing identities. A prepared enrollment key retains its complete start
request (including task text) only until it is admitted or aborted; that same
transition discards the body while the fingerprint, transport pin and receipt
or tombstone remain permanent and replayable. Only prepared keys count against
the enrollment bound. Pending/resolved currentness entries count against active
capacity only while their exact launch remains owned and active; historical
entries remain immutable and cannot grant authority. Permanent-key compaction
is not yet implemented: a future numbered migration must first prove Pool and
Run recovery settled, commit Bridge namespace retirement, and retain a permanent
rejecting namespace/key fence before removing any detailed records. Unknown
issuer associations require operator reconciliation; neither age nor TTL is a
proof and existing deletion triggers must not be bypassed. Accepted and unknown
owner effects remain recoverable after retirement and cannot be discarded merely
because their namespace was retired. Doctor warns when the permanent owner-effect
ledger reaches 90% of its capacity; it does not silently evict those effects.
Claude Code 2.1.311 emits `system/init` only after its first user input. Before
that input, Run sends bare `initialize` and `mcp_status` control requests and
requires no slash commands and exactly the configured connected MCP servers.
2.1.311 connects MCP servers asynchronously, so `mcp_status` is repeated
within the same 30-second deadline while any server is `pending`.
The declarative agents list is digested; `--tools ""` disables native tools.
MCP status may omit per-server tool inventories. When present, a server's list
is its raw `tools/list` result before deny rules apply: 2.1.311 reports the
whole Pool catalog, including the tools Run denies. A Pool server's list is
accepted when it equals the allowlist or exactly the catalog Run discovered for
this launch, whose digest is recorded in the config digest, which contains the
allowlist, and whose complement is exactly the `--disallowedTools` argument.
Unknown servers, tools outside that catalog, missing allowed tools and a catalog
digest mismatch are refused; the worker server stays exact. Run then checks the
subsequent `system/init` tool list, which 2.1.311 reports after deny rules, as a
mandatory exact tripwire before durable qualification or v2 owner handoff.
Only the `command_lifecycle` acknowledgements of exactly that input and
session, `queued` then `started`, may precede `system/init`.
Mismatch closes stdin and fails the run. The accepted shape per server
(`allowlist`, `catalog` or `omitted`) is bound into the inventory digest and
journaled as `managed_native_drift_v1`.

Managed Codex launches (only) also write `api_key_cyber_access_programs`,
`api_key_model_discovery`, `auth_elicitation`, `mentions_v2` and `view_image`
as disabled features: codex-cli 0.167.0 reports the first four enabled by
default in `config/read` and ignores the historical `tools.view_image`.
`config/read` must report every pinned feature exactly; any other reported
feature is accepted only while false, null or `{enabled = false}`, and
`view_image` must be reported false through `tools.view_image` or
`features.view_image` (at least one, neither true). 0.167.0 lists every hook
event: events other than `PreToolUse`, `PermissionRequest` and `state` are
accepted only with no handler groups. The two pinned hook groups must match
every configured field; the echoed handler fields `commandWindows`, `async`
and `statusMessage` are accepted only at `null`, `false` and `null`. The
accepted feature values and empty hook events are bound into the config digest
and journaled as `managed_native_drift_v1`. Ordinary Codex launches are
unchanged. Actual provider and hook qualification still requires the approved
dobi run.

Managed Codex launches also write `goals` and `sleep_tool` disabled (they grant
the goal tools and `clock.sleep`) and `tools.experimental_request_user_input`
and `tools.update_plan` as `{enabled = false}`. 0.167.0 reports no value for
these two tool switches, so `config/read` must report each one's origin as the
same `user` layer as `features.hooks`, the Run-generated file.

#### Codex tool names

codex-cli 0.167.0 names an MCP server's tools `mcp__<segment>__<tool>`, where
`<segment>` is the server id with every character outside `[A-Za-z0-9_]`
replaced by `_` (`agent-pool` becomes `agent_pool`). This name is what model
code calls, what `PreToolUse` hooks receive as `tool_name`, and what their
matcher is tested against. For a managed Codex launch Run writes the declared
Bridge matcher `mcp__<pool_server>__.*` as `mcp__<segment>__.*` and verifies the
pinned group carries exactly that matcher; the declaration in this file keeps
the raw id. The selector refuses an id Codex would rewrite further or that is
not uniquely parseable (anything outside ASCII `[A-Za-z0-9_.-]`, longer than
64 bytes, or whose segment is empty, starts or ends with `_` or contains `__`),
a Pool segment equal to the worker's (`agent_run_worker`), and allowed tool
names that are not already `[A-Za-z0-9_]`, start with `_`, or exceed Codex's
128-byte name bound (beyond which it substitutes a hashed name). The
normalized prefix is journaled in `managed_native_drift_v1` as
`pool_tool_prefix` and bound into the config digest. A Bridge that maps the
raw id must accept the normalized name for Codex calls.

#### Codex Code Mode

Every codex-cli 0.167.0 model reports `tool_mode = "code_mode_only"`: MCP tools
reach the model only through Code Mode's `exec`, which runs model-written
JavaScript in a V8 isolate inside the separate `codex-code-mode-host` process
and dispatches its nested tool calls back through Codex's normal tool path,
including `PreToolUse` hooks and approvals. The isolate has no filesystem,
network, process, `console`, WebAssembly or module import; nested calls are
its only effect. Without a host a `code_mode_only` model has no callable tool.

`codex_code_mode_host_sha256` (optional, lowercase SHA-256) enables Code Mode
for managed Codex launches. The host is the one Codex itself spawns: for the
standalone package layout `<package>/bin/codex` beside
`<package>/codex-package.json`, `<package>/codex-resources/codex-code-mode-host`
if it exists, otherwise `<package>/bin/codex-code-mode-host`. The configured
Codex binary may be a link to that `bin/codex`. Selection refuses any other
layout and a host that is a link, not a regular file owned by Run's user or
root, group/world-writable, larger than 256 MiB or not the pinned build; it is
re-verified at every launch and the verified path is journaled as
`code_mode_host`. The digest is a component build pin like the Bridge hook:
each new launch takes the current value. A Codex upgrade replaces the host, so
update the pin with it; until then new managed Codex launches are refused. `agent-run doctor` reports the host
(`managed_codex_code_mode_host`, or `_invalid`) and warns
(`managed_codex_code_mode_unpinned`) when no host is pinned.

With the pin, the generated configuration and the verified `config/read`
report exactly:

```toml
[features.code_mode]
enabled = false
excluded_tool_namespaces = ["clock", "functions"]

[features.code_mode_host]
enabled = true
disable_in_process_fallback = true
```

Every built-in 0.167.0 tool lives in the `functions` namespace (patch, MCP
resources, goals, plan, user input) or `clock`; excluded tools are neither
nested nor directly visible to a `code_mode_only` model. Model code can
therefore call exactly the enabled Pool tools and `finish`/`health_reply`, and
the model sees only `exec` and `wait`. `code_mode.enabled` stays false, so only
a model whose catalog requires Code Mode uses it. Without the pin Code Mode
stays disabled (`code_mode_host = false`, `code_mode = {enabled = false}`).

After `model/list`, Run reads the selected model's `tool_mode` from Codex's
`models_cache.json` in the runtime home. Without an enabled host, a
`code_mode_only` model or an unreadable mode is refused before any thread
starts (`managed Codex model requires Code Mode; pin the Codex code-mode
host`). The mode is journaled as `managed_native_tool_mode_v1` and bound into
the qualification digest. An unrecognized mode is refused.

### Upgrading Bridge

Stop Pool delivery and the Bridge daemon before atomically replacing Bridge's
binary at its pinned path. Start the new Bridge daemon, update Run's transport
executable SHA-256 and any Bridge reader/custody pins, then restart Run. Update
Pool's Bridge pin and run `agent-pool repin` before restarting Pool delivery.
Verify the daemon actually holds the configured socket and runs the new inode;
replacing a file does not upgrade an already running process.

Resume running managed members as new launches under the new configuration.
Bridge transport (socket, executable, build SHA-256 and routes) is delivery
configuration, not execution identity: every new launch, whether a fresh
start, an explicit resume or an account switch, pins the current validated
`managed_attempt_binding.transport` at spawn, while model, provider, account
and role keep their frozen resume snapshot. A system route keeps its
root-approved admission transport. Each launch freezes that pin for its life;
re-pinning Pool or reloading Run never changes an old launch's authority, and
its destination fence refuses once the current transport differs. The same
split applies to managed role selection: a real Bridge or Pool upgrade also
replaces the Bridge `PreToolUse` hook or the Pool frontend, so update
`bridge_hook_sha256`, `pool_executable_sha256` and the resident pins with the
transport. Each new launch verifies the hook and frontend against the current
pins, both when the launch plan is built and when the role is selected, while
the execution identity keeps its admission snapshot. Role semantics stay frozen: the role payload (and so the role
digest Run attests to Pool), harness kind, explicit finish, the selector's
`role_name`, `pool_server` and `host_id`, the `[mcp.<pool_server>]`
declaration including `allowed_tools`, and the harness hook declarations. A
resume whose current configuration changes any of these is refused as
`managed role changed since admission` rather than adopting it; start a new
member instead. System routes keep their admission snapshot. Preserve old
delivery keys and resolve their outcomes before retiring the original
namespace. A new launch must not replay an uncertain native effect under a new
key.

Peer executable proof uses the immutable loaded build, without a wall-clock
freshness comparison or installation delay. Run opens `/proc/PID/exe`, requires
its device/inode to equal the configured path, hashes that open file against
the pin, and checks current ownership and non-writable group/other mode. The
open file's `mnt_id` from `/proc/self/fdinfo` is resolved in
`/proc/self/mountinfo`: only ext2/ext3/ext4 (0xEF53), xfs (0x58465342), btrfs
(0x9123683E), and tmpfs (0x01021994) qualify for kernel executable write
exclusion. Missing mount evidence, unknown filesystems, and overlayfs fail
closed; overlay's visible type does not prove its upper filesystem's exclusion.
PID/start-tick incarnation checks remain unchanged. Metadata timestamps and
clock steps confer no authority; current unsafe permissions still refuse.
A stale frozen launch build pin remains `attempt_mismatch/none`, requiring a
correctly pinned relaunch: on the per-launch owner-v2 endpoint, a live
configured Bridge that passes every other proof but runs another build than
the launch pin receives `known_unsent` with reason `attempt_mismatch`, and no
native input is written. Once the launch is qualified, an effect addressed to
that exact attempt is also recorded durably in `owner_effects` as
`known_unsent`/`attempt_mismatch`, and an identical retry replays it. Such a
launch can no longer be woken, but its explicit finish still settles it: a
Codex finish result that arrives after its turn went idle is the observed
receipt. Other peer authentication failures (including an unsupported
filesystem or replaced inode) are `peer_not_authorized/none`.
