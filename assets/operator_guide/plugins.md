# plugins

Declare plugins per runtime with absolute paths:

```toml
[runtimes.claude]
plugins = ["/abs/path/to/tokenpipe-compressor", "/abs/path/to/agent-lsp-plugin"]
```

## Per-runtime load mechanics

- **claude** and **glm** load declared plugins via `--plugin-dir`. A plugin's
  native hooks and tools load with the plugin. Every plugin-shipped skill must
  be selected by the effective profile so a whole-plugin load cannot widen the
  role silently.
- **codex** copies the plugin into the generated home, and hook trust
  digests are auto-seeded for it (config-level `hooks` entries are seeded
  the same way).

Canonical revisioned profiles own `skills = [...]`. Runtime skill lists remain
only for compatibility profiles and cannot be combined with canonical assets.

## Operator checklist for a new plugin

1. Add the absolute plugin path to the target runtime(s)' `plugins =
   [...]` in config.toml.
2. If the plugin ships skills, add every skill name to each applicable
   revisioned profile's `skills = [...]`. For a compatibility profile, use the
   runtime skill list instead.
3. Start a new run. Existing sessions retain their immutable generated home;
   resume only when its stored snapshot still verifies.
