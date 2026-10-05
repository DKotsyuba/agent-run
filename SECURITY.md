# Security policy

## Trust boundary

agent-run trusts one subject: the same-UID operator reaching the owned,
mode-0700 product home through the CLI, the stdio MCP server, or the
peer-identity-checked Unix-socket API. Everything else — task text, engine
output, provider responses, repository and transcript content, and every
`actor_role`/orchestrator field — is untrusted data, never an authority
grant.

Effects the product may produce: spawning explicitly configured engine CLIs
(never auto-downloaded toolchains), writing inside the declared per-agent
roots with path-escape checks, recording durable local state in SQLite, and
delivering bound completion notices to sessions that were bound at admission.
Credentials live in nonsecret references (environment names, Keychain
entries, mode-0600 files) and never enter tool arguments, logs, diagnostics,
or rendered results.

Known limits, stated rather than denied: filesystem permissions do not
protect against a hostile process with the same UID; a fixed local socket
fences only a second listener on the same machine; attested release
checksums prove payload integrity within GitHub's trust boundary, not
independence from it. The family profile declared in `family.toml` and
[docs/family-standard.md](docs/family-standard.md) records what is verified
and what is not.

## Reporting

Security fixes are made on the latest released `0.x` line and `main`. Pre-1.0
releases may change interfaces, but credential isolation, filesystem
permissions, and outcome verification remain security boundaries.

Do not report credentials, tokens, private paths, or an exploitable workflow in
a public issue. Use GitHub's private vulnerability-reporting form for this
repository. If that form is unavailable, contact the maintainer through the
[GitHub profile](https://github.com/DKotsyuba) before sharing sensitive details.

Include the affected version, operating system, runtime adapter, minimal
reproduction, and impact. You should receive an acknowledgement within seven
days. Public disclosure is coordinated after a fix or mitigation is available.
