# skills

Each `[runtimes.<name>]` table declares `skills = [names]`: the skill names
that runtime may use. Physical copies live under
`<home>/skills/<runtime>/<name>/SKILL.md`.

## Plugin ownership

A declared plugin that ships `skills/<name>/SKILL.md` OWNS that skill name
for every runtime it applies to (enforced by `plugin_skills.py`). Once a
plugin owns a name, a stale local copy under `<home>/skills/<runtime>/<name>`
stops being read — the plugin's copy wins. Two declared plugins shipping the
same skill name is a conflict and fails closed rather than picking one
silently.

Symlinked skill directories are allowed and expected: the `role-*` skills
are symlinks into the agent-workflows checkout, not physical copies, so
editing the checkout updates every runtime that uses them without
rematerializing skill content itself.

## After changing skills

1. Edit config.toml (see `config` for the safe-edit discipline).
2. Rematerialize the affected runtime homes.
3. Restart the opencode service if opencode's skill list changed — its
   generated config embeds skills paths directly, so a running service
   keeps serving the old list until restarted.
4. claude and codex pick up the new skill list at their next agent start;
   no running claude/codex process needs to be restarted.

## Common failure

A skill referenced in an agent's task but missing from that runtime's
`skills = [...]` is invisible to the runtime, not an error at config load
time — check `agent-run doc troubleshoot` and the runtime's materialized
home if a skill "isn't showing up."
