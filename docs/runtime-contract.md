# Runtime contract

Agent-run gives every child the host PATH, language toolchains, SDK variables,
locale, and ordinary build environment. It changes only the runtime HOME/config
locations and credentials explicitly selected for that runtime or one of its MCP
servers. Unlabelled/native Claude starts preserve host `HOME` and intentionally do
not set `CLAUDE_CONFIG_DIR`; labelled account-scoped Claude starts use an
isolated `HOME` and an explicit account-private `CLAUDE_CONFIG_DIR`.
Credential-shaped ambient variables not selected by that contract
are omitted. Exported ``RUSTUP_HOME`` and ``CARGO_HOME`` remain authoritative;
when unset, existing `.rustup` and `.cargo` directories beneath the original
host ``HOME`` are forwarded before the child ``HOME`` is isolated. Agent-run
does not create these directories or provision or probe Python, Node, or Rust
during start.

Each runtime gets generated lightweight configuration so subagents see only the
skills and MCP servers selected by their role. The generated directory is
configuration separation, not an OS security boundary.

Codex write roles normally receive only their assigned workdir. An explicit
`runtimes.codex.workspace_root` replaces that root with one operator-authorized
project tree after agent-run proves the workdir is contained by it; read-only
roles ignore the setting. Ordinary write roles use the generated native
`Projects` profile and must echo that exact active profile before their first
turn; read-only and network roles retain the stricter legacy sandbox path. MCP
declarations preserve native approval modes, and
only servers explicitly set to `approve` receive the generated narrow
PermissionRequest allow hook. Unknown tools and every shell call retain normal
Codex review. The generated profile grants its isolated uv, Cargo, npm, pip, and
Go cache directories write access so normal tests stay sandboxed instead of
requesting a boundary escalation, while the generated auth bridge is denied to
shell tools. A declared DCG `PreToolUse` hook is an additional deny-only layer.

When the host's `/etc/codex/requirements.toml` defines `Projects`, ordinary write
roles select it instead of generating a conflicting duplicate. The existing
`workspace_root` must be explicitly granted by that managed profile and
`workspace_network` must agree with it. Agent-run verifies its effective
write roots and uses its granted uv, npm, pip, and Go cache locations; existing
host `CARGO_HOME` forwarding remains unchanged. A mismatched policy fails the
launch rather than silently changing its scope; without a managed definition,
the standalone generated profile is retained. Desktop and phone
Remote on the same host can use that same definition. This behavior does not
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
role cannot be combined with legacy per-runtime `skills` or `mcp` lists.

## Accounts

Omitting `account` always uses the runtime CLI's native global account. An
explicit configured label selects separate credential state for multi-account
use. `default_account` is accepted only as legacy configuration and is ignored.
Credential bytes remain in process memory or their native credential store;
snapshots contain only the `global` choice or account label.

## Native settings

`runtimes.<name>.native_settings` declares tuning values merged into the
generated native preference file (Codex `config.toml`, Claude/GLM
`settings.json`, Qwen `.qwen/settings.json`). Values are strict
scalar/array/table types; TOML has no null and dates, non-finite floats, and
non-string map keys are rejected. Codex's packaged extended-context defaults
(1,000,000-token window, 780,000-token total compaction) remain the baseline;
declared keys override them, omitted keys keep them.

Ownership is explicit: agent-run controls task model/effort, auth/provider
routing, home/cwd, MCP/tools/skills/plugins, hook trust, sandbox/permissions/
reviewer, and protocol/output mode. `native_settings` cannot override or
disable any of these. Known control surfaces — including Codex
`model_providers`/`openai_base_url`/`approvals_reviewer`/`notify`/`features`,
Claude `apiKeyHelper`/`statusLine`/credential helpers/hook disablement, and
Qwen `tools.sandbox`/`mcp`/`security` — fail closed with a validation error
naming the key. Accepting an unreserved unknown key is convenience tuning, not
a guarantee that every upstream key is safe to relax.

Changes flow through scoped runtime copies and config snapshots: the snapshot
records the declared settings verbatim — auth and credential sources are never
resolved, but the tree is persisted, so secret values must not be placed in it —
and changed options change snapshot identity and the next launch regenerates
the native file. Settings apply at launch preparation; running sessions are
unaffected until relaunch.

## Readiness

`agent-run doctor` checks runtime executables, role syntax, the shared skill
catalog, and selected MCP definitions outside the start path. It does not gate a
launch on repeated language-toolchain subprocesses or require a pre-generated
runtime home.
