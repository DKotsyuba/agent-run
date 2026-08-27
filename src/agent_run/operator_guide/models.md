# models

## claude and codex

Static rosters: whatever model ids are listed in each runtime's `models =
[...]` in config.toml. There is nothing to sync — editing the list and
rematerializing is the whole operation.

## opencode

opencode model ids in config are `omniroute/<alias>`, mapping to OmniRoute
combos named `opencode/<alias>`. These aliases are not invented locally —
they must match what the live OmniRoute service actually serves.

### Syncing the opencode roster

1. `GET http://127.0.0.1:20128/v1/models` — use a long timeout, the
   response is a large JSON document.
2. Filter returned ids by the `opencode/` prefix.
3. Write the matching `omniroute/<alias>` names into the opencode
   runtime's `models = [...]` in config.toml (backup + validate first,
   per `config`).
4. Rematerialize and restart the opencode service (see `service`) so the
   generated config reflects the new roster.

### Verifying

Roster warmup validates every declared model against the live service at
service start; a model that OmniRoute doesn't actually serve fails warmup
loudly rather than appearing broken later. After a successful start,
`agent-run models` must show every declared model — if one is missing,
the sync in step 1–3 used a stale or wrong OmniRoute combo name.
