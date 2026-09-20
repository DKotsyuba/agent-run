# skills

Configure one shared catalog:

```toml
[skills]
directory = "/absolute/path/to/agent-run-skills"
```

Each revisioned profile selects its complete `skills = [...]` list. Adapters
translate that same list into Codex or Claude/GLM native configuration.
Missing skill directories or `SKILL.md` files fail before admission; content
revisions are stored in the resolved role snapshot.

Compatibility per-runtime skill lists remain readable for existing
configurations. New configurations should use revisioned roles; the two forms
cannot be combined.
