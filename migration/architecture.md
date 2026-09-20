# Migration architecture record

> Historical pointer. The implemented Rust architecture is documented in
> [docs/architecture.md](../docs/architecture.md); crate ownership is summarized
> in the repository `AGENTS.md`.

The migration preserved three transports over one domain dispatcher: CLI,
MCP stdio, and Unix-socket JSON-RPC. The resident broker owns asynchronous
launches, SQLite state, supervision, configuration reload, delivery, and
evidence. Adapters support Codex, Claude, and GLM. Qwen is intentionally absent
under [ADR A22](adr/A22-qwen-removal.md).

Historical design alternatives remain in [rust-migration-plan.md](rust-migration-plan.md)
and the [ADR directory](adr/); they are not a second source of current runtime
truth.
