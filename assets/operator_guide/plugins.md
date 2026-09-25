# plugins

Schema 2 declares plugins on the native harness with absolute paths:

```toml
[harnesses.claude-code]
plugins = ["/abs/path/to/tokenpipe-compressor", "/abs/path/to/agent-lsp-plugin"]
```

## Harness load mechanics

- **claude-code** loads declared plugins via `--plugin-dir`, including their
  native hooks, tools and skills. This applies to native Claude and custom
  providers such as GLM using the same harness.
- **codex** copies declared plugins into the generated home and seeds their
  hook trust digests, as it does for configured hooks.

Claude Code loads a plugin as a whole. Every exported skill must be present
in the canonical skill catalog and declared by the selected role's
`skills = [...]`; an undeclared export is refused before launch. Canonical
roles therefore support skill-bearing plugins without a runtime-level skill
list. In historical schema-1 compatibility profiles, plugin paths and the
exported-skill allowlist remain runtime declarations.

## Operator checklist for a new plugin

1. Add its absolute path to `harnesses.<id>.plugins` in `config.toml`.
2. For Claude Code, make every exported skill available in the canonical
   catalog and list it in each role that will use that harness's plugins.
3. Start a new run. Existing sessions retain their immutable generated home;
   resume only when its stored snapshot still verifies.
