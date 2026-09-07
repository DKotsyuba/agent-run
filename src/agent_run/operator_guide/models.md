# models

## claude, codex, glm, and qwen

Static rosters: whatever model ids are listed in each runtime's `models =
[...]` in config.toml. There is nothing to sync — editing the list and
rematerializing is the whole operation.

## Qwen OmniRoute aliases

Qwen may use OmniRoute model ids with an `opencode/<alias>` prefix. The prefix
is an OmniRoute route name, not a supported agent-run runtime; keep valid
aliases in Qwen's `models = [...]` configuration.

### Verifying

Roster warmup validates every declared model against the live service at
service start; a model that OmniRoute doesn't actually serve fails warmup
loudly rather than appearing broken later. After a successful start,
`agent-run models` must show every declared model — if one is missing,
the sync in step 1–3 used a stale or wrong OmniRoute combo name.
