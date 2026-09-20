# models

## claude, codex, and glm

Static rosters: whatever model ids are listed in each runtime's `models =
[...]` in config.toml. There is nothing to sync — editing the list and
rematerializing is the whole operation.

### Verifying

`agent-run models` reports each configured runtime's declared models together
with the availability evidence its adapter can obtain. A missing model is a
configuration or provider-roster error; fix the runtime declaration before
admitting work with that model.
