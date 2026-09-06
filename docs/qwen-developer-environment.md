# Qwen developer environments

Qwen uses the selected `environment` preset to build a child environment. The
preset paths lead the inherited launcher baseline; declared variables expand
only `{workdir}` and `{home}`. The child keeps its generated `HOME`, Qwen model
and configured authentication, not the parent's full environment.

Configured `required_commands` are checked against that final PATH before
Qwen settings are written. Each configured MCP server receives `${PATH}` and
the declared variable names in its Qwen settings, so it sees the same declared
toolchain without copying credentials or unrelated parent variables.

`denied_commands` create private PATH refusal shims and Qwen
`permissions.deny` entries for the bare name, its executable path, and a
symlink target when present. These rules are command policy, not operating
system confinement.

Qwen 0.22.2's current adapter rejects `profile.network = true`. Qwen's macOS
Seatbelt documentation distinguishes an open profile, which permits outbound
network access, from a closed profile with no network; it documents no
loopback or Unix-socket exception. A network-profile mapping therefore needs
an explicit policy decision rather than silently widening a role.
