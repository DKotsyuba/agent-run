# plugins

Declare plugins per runtime with absolute paths:

```toml
[runtimes.claude]
plugins = ["/abs/path/to/tokenpipe-compressor", "/abs/path/to/agent-lsp-plugin"]
```

## Per-runtime load mechanics

- **claude** and **glm** load declared plugins via `--plugin-dir`. A plugin's
  native hooks, tools, and skills load with the plugin.
- **codex** copies the plugin into the generated home, and hook trust
  digests are auto-seeded for it (config-level `hooks` entries are seeded
  the same way).

Plugin paths are always runtime-declared. With a compatibility profile, every
skill shipped by a Claude/GLM plugin must also appear in the runtime's
`skills = [...]` list. Canonical revisioned profiles cannot currently use a
skill-bearing Claude/GLM plugin: adapter validation requires that runtime list,
while canonical asset rules reject mixing runtime skills with profile-owned
skills. The configuration fails closed instead of exposing an undeclared
prompt. Skill-free plugins remain usable with canonical profiles.

## Operator checklist for a new plugin

1. Add the absolute plugin path to the target runtime(s)' `plugins =
   [...]` in config.toml.
2. For a compatibility profile, list every plugin skill in the runtime's
   `skills = [...]`. For a canonical profile, use only a skill-free plugin until
   canonical plugin-skill validation is supported.
3. Start a new run. Existing sessions retain their immutable generated home;
   resume only when its stored snapshot still verifies.
