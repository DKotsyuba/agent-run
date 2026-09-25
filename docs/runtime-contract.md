# Runtime contract

Agent-run gives every child the host PATH, language toolchains, SDK variables,
locale, and ordinary build environment. It changes only the runtime HOME/config
locations and credentials explicitly selected for that runtime or one of its MCP
servers. Claude accounts with a native credential reference preserve host
`HOME` and intentionally do not set `CLAUDE_CONFIG_DIR`; named account references
use an isolated `HOME` and an explicit account-private `CLAUDE_CONFIG_DIR`.
Credential-shaped ambient variables not selected by that contract
are omitted. Exported ``RUSTUP_HOME`` and ``CARGO_HOME`` remain authoritative;
when unset, existing `.rustup` and `.cargo` directories beneath the original
host ``HOME`` are forwarded before the child ``HOME`` is isolated. Agent-run
does not create these directories or provision or probe Python, Node, or Rust
during start.

Each runtime gets generated lightweight configuration so subagents see only the
skills and MCP servers selected by their role. The generated directory is
configuration separation, not an OS security boundary.

Current schema-2 configuration separates `[harnesses.<id>]` launch settings
from `[providers.<id>]` models, connections and account bindings. Historical
schema-1 `runtimes.*`, singular `workspace_root` and `default_account` fields
are not accepted in fresh schema-2 configuration.

Codex write roles normally receive only their assigned workdir. An explicit
`harnesses.codex.workspace_roots` array replaces that scope with operator-authorized
project trees after agent-run proves the workdir is contained by at least one of
them; read-only roles ignore the setting. A write role admitted under one
configured root receives the full configured Projects root set, not only the
tree containing its workdir. MCP declarations preserve native approval modes, and
only servers explicitly set to `approve` receive the generated narrow
PermissionRequest allow hook. Unknown tools and every shell call retain normal
Codex review. The generated profile grants its isolated uv, Cargo, npm, pip, and
Go cache directories write access so normal tests stay sandboxed instead of
requesting a boundary escalation, while the generated auth bridge is denied to
shell tools. A declared DCG `PreToolUse` hook is an additional deny-only layer.

When the host's `/etc/codex/requirements.toml` defines `Projects`, write roles
with configured workspace roots select it, including network roles when
`workspace_network` explicitly permits network. Managed read-only roles select
`:read-only`; managed write roles without configured roots select `:workspace`
and cannot request network. Agent-run sends the native `permissions` selector
and verifies the active profile, roots and network echo before the first turn.
Every configured `workspace_roots` entry must be explicitly granted by that
managed profile and `workspace_network` must agree with it. Agent-run verifies its effective
write roots and uses its granted uv, npm, pip, and Go cache locations; existing
host `CARGO_HOME` forwarding remains unchanged. A mismatched policy fails the
launch rather than silently changing its scope. Without a managed definition,
write roles with configured roots and no network role grant use the generated
`Projects` profile; remaining grants use the explicit sandbox request. Desktop
and phone Remote on the same host can use that same definition. This behavior does not
select Full Access or relax a read-only role.

## Canonical roles

`[profiles].directory` contains one Markdown file per role. A revisioned role is
complete and must declare every field:

```markdown
+++
revision = "1"
write = false
network = false
allow_external_read_roots = true
skills = ["code-reading", "document-code"]
mcp = ["codegraph"]
required_constraints = []
+++
Review the assigned revision and report actionable defects.
```

`[skills].directory` names the single shared skill catalog. MCP definitions
remain in `[mcp.<name>]`. Missing role assets fail before admission. A canonical
role selects its skills and MCP servers; legacy per-runtime lists belong only
to historical schema-1 compatibility profiles.

## Accounts

Omitting `account` requests automatic allocation among the provider's eligible
registered accounts. An explicit provider-local label pins that account and
disables failover. The selected account's credential reference determines
whether native global or named credential state is used. Codex can replace an
unavailable automatic account during resume and can switch after verified
in-flight quota exhaustion; Claude Code continuation retains its original
account. Credential bytes remain in process memory or their native credential
store; frozen authority and attempt records retain nonsecret identities and
references. See [provider-contract.md](provider-contract.md).

## Native settings

`harnesses.<id>.native_settings` declares tuning values merged into the
generated native preference file (Codex `config.toml` or Claude Code
`settings.json`). Values are strict
scalar/array/table types; TOML has no null and dates, non-finite floats, and
non-string map keys are rejected. Codex's packaged extended-context defaults
(1,000,000-token window, 780,000-token total compaction) remain the baseline;
declared keys override them, omitted keys keep them.

Ownership is explicit: agent-run controls task model/effort, auth/provider
routing, home/cwd, MCP/tools/skills/plugins, hook trust, sandbox/permissions/
reviewer, and protocol/output mode. `native_settings` cannot override or
disable any of these. Known control surfaces — including Codex
`model_providers`/`openai_base_url`/`approvals_reviewer`/`notify`/`features`,
and Claude `apiKeyHelper`/`statusLine`/credential helpers/hook disablement —
fail closed with the generic validation error
`native setting is reserved or has an invalid key`; it does not echo the key.
Accepting an unreserved unknown key is convenience tuning, not
a guarantee that every upstream key is safe to relax.

Changes flow through scoped runtime copies and config snapshots: the snapshot
records the declared settings verbatim — auth and credential sources are never
resolved, but the tree is persisted, so secret values must not be placed in it —
and changed options change snapshot identity for the next fresh start. A
continuation verifies and reuses its frozen files instead of importing edited
settings. Running sessions are unaffected by configuration edits.

## Readiness

`agent-run doctor` checks runtime executables, role syntax, the shared skill
catalog, and selected MCP definitions outside the start path. It does not gate a
launch on repeated language-toolchain subprocesses or require a pre-generated
runtime home.
