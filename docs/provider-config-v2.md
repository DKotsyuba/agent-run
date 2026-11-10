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

OpenCode CLI 2.0.28 runs as an owned `serve --stdio --port 0` child. Stdin is
its lifetime lease; commands use authenticated loopback HTTP. Each lineage
retains private config/data/cache/state roots and its own native database.
The selected `native_model` must be an explicit `provider/model` reference.

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
lowercase SHA-256 values `pool_executable_sha256` and `bridge_hook_sha256`.
The selector refuses plugins, skills, non-stdio or unrestricted MCP servers,
unapproved component binaries, non-explicit completion, research and broader
roles. The selected Pool command must be the pinned binary with exactly
`mcp --config <absolute-path>` and an explicit safe tool subset; the private
worker surface is limited to `finish` and `health_reply`. Codex live configuration
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
The declarative agents list is digested; `--tools ""` disables native tools.
MCP status may omit per-server tool inventories; Run therefore also checks the
subsequent `system/init` tool list as a mandatory tripwire before
durable qualification or v2 owner handoff. Mismatch closes stdin and fails the
run. Actual provider and hook qualification still requires the approved dobi run.

### Upgrading Bridge

Stop Pool delivery and the Bridge daemon before atomically replacing Bridge's
binary at its pinned path. Start the new Bridge daemon, update Run's transport
executable SHA-256 and any Bridge reader/custody pins, then restart Run. Update
Pool's Bridge pin and run `agent-pool repin` before restarting Pool delivery.
Verify the daemon actually holds the configured socket and runs the new inode;
replacing a file does not upgrade an already running process.

Resume running managed members as new launches under the new configuration.
Each launch freezes its owner-v2 Bridge build pin; re-pinning Pool or reloading
Run never changes an old launch's authority. Preserve old delivery keys and
resolve their outcomes before retiring the original namespace. A new launch
must not replay an uncertain native effect under a new key.

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
correctly pinned relaunch. Other peer authentication failures (including an
unsupported filesystem or replaced inode) are `peer_not_authorized/none`.
