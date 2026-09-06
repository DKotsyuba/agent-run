# Claude developer environments

The `claude` runtime (and `glm`, which inherits its adapter unchanged) selects
a named preset from `[environments.<name>]` the same way every runtime does;
see [developer-environments.md](developer-environments.md) for the shared
declaration and provider contract. This document covers only what is specific
to how `ClaudeAdapter` applies a selection.

## Child environment

`ClaudeAdapter.prepare` builds its isolated baseline exactly as before --
`HOME` pinned to the generated managed home, the ambient uv-managed Python
root, and the ambient `PATH` -- then passes that baseline through
`developer_environment(baseline, config, request.workdir)` instead of calling
the Rust provider directly. A selected preset's declared paths are prepended
to `PATH`, its non-secret variables are expanded and set, and effective Rust
(runtime-local overriding preset) is applied exactly once inside that call.
`HOME` and shell-startup variables remain protected and the required-auth
contract is unchanged.

## Command denial

When a selected preset declares `denied_commands`, `prepare` materializes a
private PATH refusal-shim directory under the runtime's managed `home`
(`home/command-policy`) via the shared `command_policy` provider, resolving
each denied name's target executable through the preset's own final `PATH`.
That directory is prepended ahead of the rest of `PATH`, so a shimmed lookup
of a denied bare name fails with exit 126 before reaching its real
executable. Because a shim only intercepts ordinary PATH lookup, the same
denied names (plus their resolved absolute paths and symlink targets) are
also rendered as native `Bash(...)`/`Bash(... *)` entries and appended to the
existing `--disallowedTools` list, so Claude's own tool-use layer refuses an
absolute-path invocation too. Commands not on the denied list, such as Git,
are unaffected by either mechanism. Neither mechanism is OS-level
confinement: both are ordinary-use refusals, not a sandbox guarantee.

## MCP subprocess environment

Claude spawns each configured stdio MCP server inheriting its own launched
process environment. The `env_from` validation loop that populates that
environment now sources a requested name from the *effective* child
environment (the preset's `PATH`/variables and any effective Rust roots, as
reported by `configured_environment_keys(config)`) instead of unconditionally
reading the ambient `os.environ`, for every name that selection provides.
Names outside that contract still read the ambient environment exactly as
before, so MCP subprocesses see the same PATH and non-secret preset variables
that Claude itself was launched with, never a copy of the whole parent
environment.

## Materialization revision

`ClaudeAdapter.materialize`'s returned digest now also folds in
`environment_digest(config)`, so a settings/plugin-identical materialization
still produces a new revision when the selected preset's paths, variables,
command policy, or effective Rust declaration change.
