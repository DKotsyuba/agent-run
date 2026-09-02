# config

`~/.agent-run/config.toml` is the single source of truth for every runtime,
service, and integration. There is no other config file and no environment
variable that overrides it.

## Fail-closed

Anything not declared in config.toml is absent, not defaulted. An undeclared
skill, MCP server, plugin, or model does not exist for that runtime, even if
matching files sit on disk. This extends to plugins that ship skills: a
plugin skill not also listed in a runtime's `skills = [...]` is refused, not
silently loaded — see `skills` and `plugins`.

## Editing discipline (every time, no exceptions)

1. **Backup.** Copy the current file into `backups/config.toml.<UTC
   timestamp>` under the agent-run home before touching anything.
2. **Edit** config.toml.
3. **Validate** with `tomllib` before stopping any running service:
   `python3 -c "import tomllib; tomllib.load(open('config.toml', 'rb'))"`.
   A config that fails to parse must never reach a service restart — a
   broken file plus a stopped service is an outage with no rollback path
   already loaded.
4. **Restart** only after validation passes. See `service` for the exact
   stop/start order; restarting before validating can leave a runtime with
   no service at all.

## Per-runtime keys

Each `[runtimes.<name>]` table may declare:

- `enabled` — bool; a disabled runtime is invisible everywhere (models,
  doc, service).
- `adapter` — which adapter drives this runtime (claude, codex, glm, qwen,
  opencode).
- `binary` — absolute path to the runtime's executable; the configured launcher
  path is preserved rather than replaced with its symlink target. This keeps
  interpreter lookup relative to the launcher directory reliable for packaged
  executables. Runtime homes and credential paths still resolve normally.
- `home` — the runtime's private home directory; materialized config,
  skills, and plugins live under here.
- `models` — the declared model roster for this runtime.
- `skills` — skill names this runtime may use.
- `mcp` — MCP server names attached to this runtime.
- `plugins` — absolute plugin directory paths.
- `max_active_agents` — concurrency ceiling for this runtime.
- `hooks` — codex only; hook trust digests and commands.
- `limits_source` — `native`, `codex_appserver`, `codexbar`, `omniroute`,
  or `none`; controls how quota evidence is collected.
- `accounts` / `default_account` — path-safe labels for configured account
  homes when the runtime supports multi-account launches.
- `priority_multiplier` — positive finite number, default `1.0`; multiplies
  this runtime's nonnegative capacity-order score. It cannot make an exhausted
  route usable and does not affect role/model suitability.

Declaring a key with an unsupported value fails closed at load time
(`ValidationError`), not silently at first use.

Use `agent-run capacity order` to inspect the resulting role-independent
priority order. The first route is highest priority, but this is not an
automatic launch: the orchestrator must still choose a compatible role/model
alias. The output includes working routes, deferred evidence, exhausted
omissions, unavailable runtimes, and `insufficient_diversity`.
