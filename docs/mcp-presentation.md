# MCP contract and presentation

The operator `agent-run` and private `agent-run-worker` servers advertise the
Cargo product version. It is independent of rmcp 3.4.0, the negotiated MCP
protocol, state schema 22, and family response profile `rust-minijinja-v1/0.1.0`.
Tool names, resident execution, stable agent IDs and worker capabilities keep
their existing contracts. The worker registration namespace remains
`agent_run_worker`; only the supervisor supplies its attempt context.

## Discovery and registration

`assets/tools.json` and `assets/worker_tools.json` are the schema-first
authorities, parsed by the domain registries. The same definitions drive live
MCP and `cargo xtask contract export|check`; current snapshots are
`schemas/tools.json` and `schemas/worker-tools.json`. Check compares complete
bytes, including descriptions and annotations. Historical Python fixtures stay
unchanged and tests account explicitly for the reviewed compatibility deltas.

Read tools declare read-only hints. Start/resume do not claim unconditional
idempotency: use the same nonempty request_id and unchanged arguments for an
identical retry. Steer is additive but repeated calls can enqueue additional
commands. Cancel is destructive and is not declared unconditionally idempotent.
Worker reports require a request key scoped to their immutable run context.
Hints are descriptive, never authorization.

`schemas/mcp-registration.json` is host-neutral metadata: actual commands,
server/registration names, protocol/version data and environment names only.
Null startup/tool timeout fields mean no universal server deadline is promised;
hosts choose observation deadlines and the broker has route-specific deadlines.
Cancellation abandons the MCP wait while admitted work continues. No Crew API,
host registration or host qualification is implied by this descriptor.

Unknown tool names return MCP invalid-params (-32602). Malformed supported
requests erased into the SDK's custom-method route also return invalid-params;
unknown protocol methods return method-not-found (-32601). Expected input and
business failures remain tool results with isError=true. SDK framing and
negotiation remain authoritative.

## Presentation policy

MiniJinja 2.24.0 uses only serde and fuel features. A closed embedded environment
is parsed before forwarding work, with strict undefined values, plain text,
recursion limit 16, 50,000 VM instructions and one explicitly registered pure
optional-field predicate. It has no loader, I/O, secrets, implicit globals or
runtime template selection. Registration failures prevent startup; branch,
fuel and output failures use safe receipt-based fallback.

Dynamic broker JSON is the adapter boundary. Critical shapes/statuses are
validated and explicit typed acknowledgement, agent, page, transcript, answer,
catalog, diagnostics, excerpt and error views whitelist the fields rendered.
Private account/pool/provider envelopes, credentials and capabilities are
excluded. Display labels are quoted, controls/ANSI/bidi made visible, and long
friendly labels marked shortened. Actionable values use exact references or
reversible JSON quoting; excerpts retain their bytes, including literal Jinja.

| Tools | Hard UTF-8 text cap | Whole-row cap | Exact content cap |
|---|---:|---:|---:|
| start, resume | 4096 | 8 displayed MCP selections, plus explicit omitted count | none |
| cancel, steer, notify_orchestrator, routine errors | 2048 | 8 | none |
| list_agents, limits | 8192 | 20 | none |
| transcript | 16384 | 100 | 14336 |
| answer, doc, delegation_guide | 16384 | none | 14336 |
| models, capacity_order | 16384 | 100 providers and 100 models in total | none |

Admission replies reserve the original request key, whose existing contract
allows 512 characters; reversible quoting can expand it beyond 2 KiB. These
are byte limits, not token or MCP envelope limits. Stdio independently enforces
one MiB per LF-delimited frame. No tokenizer savings are claimed.

Pages are budgeted before projection, then rendered into a bounded private
writer. Every page row is displayed in source order or the entire page is
refused. No partial buffer, shortened critical value or upstream continuation
for undisplayed rows is published. Narrow models with the existing filters;
request smaller list/transcript limits; use CLI/socket for full documents or
answer artifacts. No new detail-reference service is invented.

Semantic execution receipts are captured before presentation. Degradation after
accepted admission, steering or enqueue keeps the confirmed identity, original
request key and no-replay advice with isError=false. A valid admission counter
keeps the tiny agent_id/sequence structured mirror for PostToolUse binding.
Invalid counters never bind, even when acceptance is known. Unknown writes stay
unknown with isError=true and reconciliation advice; expected business failures
stay errors. Fallback never emits raw JSON, Debug values, context or error chains.

The binding hook reads the tiny structured object directly; legacy protocol and
hook-normalizer tests exercise compact text plus this mirror. This product
compatibility profile does not duplicate the full upstream payload in text and
does not imply formal host-matrix qualification.

Resume currently uses a client error that merges a missing broker before connect
with exhausted reconnects after a possibly lost acknowledgement. MCP deliberately
classifies both as outcome_unknown, retaining the requested stable agent ID and
supplied request_id. Reconcile that same target/request before any retry; a new
replacement or replay without idempotency proof is unsafe. The socket/CLI client
and its existing bounded same-key reconnect behavior are unchanged.

## Compatibility and verification

The next release needs the pre-1.0 minor 0.20: discovery metadata, error channels,
quoted labels, private metadata removal, page refusal and degradation semantics
are intentional MCP changes. CLI/socket business results are retained. The
workspace remains at its current version until separate release preparation.

Rust tests cover registry drift, typed projection, secret canaries, exact
content/critical values, byte/row boundaries, strict variables, fuel and receipt
fallback. Exact compiled binaries cover operator and worker discovery, calls,
invalid/unknown requests, cancellation notifications and EOF. Cancellation
after durable admission is separately tested with an injected broker. These
checks do not establish real-engine/host qualification or release acceptance;
the coherent native gate and later doctor/release stages remain required.
