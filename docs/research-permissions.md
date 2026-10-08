# Restricted research

Use the [canonical profile example](../examples/profiles/research.md), requiring
`research_tools_only`, `write=false`, `network=true`, no external read roots,
no configured work MCPs, and only the optional `role-research` skill.
The same adapter check governs catalog compatibility, admission and launch.
Other roles retain their existing grants and tools. The protected report channel
requires a schema-2 provider launch; legacy starts reject this constraint.

## Supported harness and native enforcement

Claude Code uses `--restricted`, an explicit `WebSearch,WebFetch` native tool
list, strict first-party MCP settings, no plugin directories, disabled slash
commands and Chrome, and explicit refusal of Bash, Agent, Task, Skill, native
file tools and NotebookEdit. GLM uses the same adapter; live qualification
has not been completed. Research retains only native web
and the first-party completion/pool-message tools plus the report writer.
Global MCP servers and plugin execution paths are excluded.

Codex research uses native hosted web and the native isolated composition host.
Generated controls disable shell tools, local REPLs, apps, plugins, hooks,
permission expansion and skill installation. `agents.enabled=false` disables
both direct and composed delegation; feature flags alone are insufficient when
model metadata selects the newer agent tool implementation. Empty sticky
`environments=[]` on thread start/resume removes environment tools, including
patching and local filesystem access. The built-in read-only permission grant
has raw command networking disabled; hosted web needs no command-network grant.

Before a model turn, the runner verifies the native effective configuration and
requires exactly the connected first-party worker server with its six reviewed
tools. Missing controls, inherited servers, a changed catalog or incomplete
inventory fail closed. A composition tool called `exec` is not a shell: its
native isolate exposes the allowed tools and has no Node process, filesystem,
module loading or general network globals. An ordinary unmarked Codex
read-only/network profile remains unsupported. No MCP proxy, global policy
change or model-capability override is used.

## Confined reports

The assigned report directory is the canonical request `workdir`; broader
project write roots are not inherited. Create it before launch. The private
`save_report` accepts a flat `filename` ending in `.md`, `.txt` or `.json`
(maximum 128 UTF-8 bytes), and nonblank `content` (maximum 64 KiB). Dot-prefixed
names, absolute paths, traversal, subdirectories, symlinks, special files and
overwrites are refused. The broker authenticates a running, unexpired attempt
and its frozen role, opens directory components without following links, and
publishes an owner-only file atomically without replacement. An identical
filename/content replay returns the same hash; different content conflicts.
Receipts contain filename, byte count and SHA-256, never report text.

## Canonical sources and activation

Profiles are operator-owned under the configured profile directory. The example
is not an automatic mutation of an installed profile. The configured research
skill permits report-directory access only through `save_report`. Maintain
operator-owned catalog sources, never immutable run snapshots.
An older broker cannot parse the new constraint; activate this profile only
after separate approval to install a compatible broker and frontend. Old
snapshots retain their grants and dependencies on resume. Never restore
`omniroute-web`.

Qualification requires actual primary-page retrieval, a sealed nonblank answer,
a cited report, outside-directory refusal, verified native controls and confirmed
process cleanup. Admission, a catalog entry or a zero exit alone is insufficient.
