# skills

Configure one shared catalog:

```toml
[skills]
directory = "/absolute/path/to/agent-run-skills"
```

Each revisioned profile selects its complete `skills = [...]` list. Adapters
translate that same list into Codex, Claude/GLM, or Qwen native configuration.
Missing skill directories or `SKILL.md` files fail before admission; content
revisions are stored in the resolved role snapshot.

Legacy per-runtime skill lists remain readable during migration. Do not combine
them with revisioned roles.
