# Codex developer environments

When a Codex runtime selects an `environment`, its generated home forwards only
the preset's `PATH`, declared variables, and effective Rust variables to its
configured MCP servers. The app-server child receives the same isolated preset
environment, with `{workdir}` and `{home}` expanded for that launch.

Selected developer environments disable login shells so shell startup files
cannot reorder the preset's command lookup path. Legacy Codex runtimes leave
that native setting unset.

Write-capable developer-environment threads use `on-request` approval with
Codex's `auto_review` reviewer. The native thread echo must confirm both
settings before agent-run starts the turn; read-only and legacy threads keep
their existing approval behavior.

`required_commands` are resolved through that child `PATH` before launch. A
missing command rejects the launch.

For `denied_commands`, agent-run places private refusal shims before the child
`PATH` and writes `rules/agent-run-command-policy.rules` in the managed Codex
home. The native rules deny bare commands plus resolved executable and symlink
paths, including direct-path aliases. They are Codex command policy, not OS
confinement: a user process may still have other ways to execute machine code.

The materialized Codex fingerprint includes the selected environment
declaration, so changing paths, variables, command requirements or denials
regenerates the runtime home.
