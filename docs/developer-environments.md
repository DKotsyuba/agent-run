# Developer environments

Owner configuration may define named developer environments under
`[environments.<name>]`. Each declaration may contain absolute `path` entries,
non-secret string `variables`, bare `required_commands` and `denied_commands`,
and an optional `rust` table with `rustup_home` and `cargo_bin`.

A runtime selects one with `environment = "<name>"`. Config resolves that name
to an `EnvironmentConfig`; an unknown name and unknown fields are rejected.
Runtime-local `rust` overrides preset Rust.

`agent_run.adapters.developer_environment` is the shared pure provider. It
copies an already isolated baseline, adds declared paths and variables, allows
only `{workdir}` and `{home}` templates, protects identity/startup variables,
validates required commands through PATH, and delegates effective Rust handling
once to the existing Rust provider. It records denied commands for consumers;
it does not enforce shell policies, install tools, create directories, or pass
the complete parent environment to MCP subprocesses.
