# plugins

Declare plugins per runtime with absolute paths:

```toml
[runtimes.claude]
plugins = ["/abs/path/to/tokenpipe-compressor", "/abs/path/to/agent-lsp-plugin"]
```

Live examples in this deployment: the tokenpipe compressor and
agent-lsp-plugin.

## Per-runtime load mechanics

- **claude** loads plugins via `--plugin-dir`. A plugin's hooks auto-load
  with the plugin. A plugin skill that is not also listed in the runtime's
  `skills = [...]` is REFUSED — fail-closed, not silently dropped. See
  `skills` for the ownership rule this implies.
- **codex** copies the plugin into the generated home, and hook trust
  digests are auto-seeded for it (config-level `hooks` entries are seeded
  the same way).
- **opencode** only picks up plugin-shipped skills; it has no plugin
  hook/tool mechanism of its own today.

## Operator checklist for a new plugin

1. Add the absolute plugin path to the target runtime(s)' `plugins =
   [...]` in config.toml.
2. If the plugin ships skills, add each skill name to that runtime's
   `skills = [...]` too — omitting this step is the most common cause of
   "the plugin is configured but its skill is refused."
3. Rematerialize and restart per `service` / `skills` as appropriate for
   the runtime.
