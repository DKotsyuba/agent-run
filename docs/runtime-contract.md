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

## Readiness

`agent-run doctor` checks runtime executables, role syntax, the shared skill
catalog, and selected MCP definitions outside the start path. It does not gate a
launch on repeated language-toolchain subprocesses or require a pre-generated
runtime home.
