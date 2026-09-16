# A12: Python-compatible canonical JSON for the Rust port

- Status: accepted (spike — backlog M08)
- Related: migration plan backlog M08/M16, requirement T46, risk K05

## Decision

Add a byte-exact reimplementation of CPython's
`json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=...)`
over `serde_json::Value`, parameterized only on `ensure_ascii` (the one
axis that actually varies across the Python call sites that hash or persist
JSON — see the inventory below). `sort_keys=True` and
`separators=(",", ":")` are constant across every such call site, so they
are not parameters.

Two entry points:

- `canonical::dumps(&Value, ensure_ascii: bool) -> Vec<u8>` — the exact bytes.
- `canonical::sha256_hex(&Value, ensure_ascii: bool) -> String` — `dumps` +
  SHA-256, lowercase hex, matching
  `hashlib.sha256(json.dumps(...).encode("utf-8")).hexdigest()`.
- `canonical::python_float_repr(f64) -> String` is exposed separately
  because it is independently interesting/testable: it is CPython's
  `repr(float)` (and therefore `json.dumps`'s float encoder), not just an
  internal helper.

No existing Rust call site was changed. See "Existing Rust canonicalizations"
below for why.

## Why byte-exactness matters here

Several Python-persisted values are re-derived and compared, not just
stored: `inspect_config_snapshot`/`inspect_runtime_snapshots` in
`src/agent_run/adapters/snapshot_config.py:244-248` and
`src/agent_run/adapters/snapshot_runtime.py:202,275-289` re-serialize the
parsed document and assert `raw == canonical` before trusting it. A Rust
reader or writer that lands on different bytes for equivalent data (wrong
key order, wrong float formatting, wrong escaping) fails that check outright
— an existing run becomes unresumable, or a legitimate replay is rejected
as a conflict. This is exactly risk K05 in the migration plan.

## Inventory: Python canonical JSON call sites

All `json.dumps(..., sort_keys=True, separators=(",", ":"))` call sites in
`src/agent_run` that produce something hashed and/or persisted as the
durable bytes (as opposed to a one-off log line or wire message reformatted
freely on both ends). `ensure_ascii` is `True` unless shown otherwise — that
is `json.dumps`'s default when the argument is omitted, and most of these
call sites omit it.

| # | Site | Hashes/produces | `ensure_ascii` | Persisted as |
|---|---|---|---|---|
| 1 | `role_plan.py:371-374` (`resolve_role_plan`), `role_plan.py:236-239` (`ResolvedRolePlan.from_payload`) | `content_hash(json.dumps(_canonical_payload(plan), ...))` → `config_revision` | True (default) | `agents.config_revision` DB column (`state/store.py:172-197`, `state/db.py:538,568,584`) |
| 2 | `adapters/snapshot_config.py:176-197` (`build_config_snapshot`) | nested `runtime_config_sha256` + outer config-snapshot document, both `content_hash`-ed | True (default) | Config-snapshot file bytes; re-derived and byte-compared on read (`snapshot_config.py:244-248,252-256`) |
| 3 | `adapters/snapshot_runtime.py:~101-115` (index builder, `+ b"\n"`) | `.agent-run-snapshots.json` runtime-snapshot-index document | True (default) | File bytes; `snapshot_index_sha256` embedded in #2; re-derived and byte-compared on read (`snapshot_runtime.py:264-275`) |
| 4 | `verify.py:376-391` (`answer_proof_document`, `+ b"\n"`) | Answer proof sidecar document | True (default) | Sidecar file bytes. Read back via field-by-field comparison (`_proof_mismatch`, `verify.py:354-373`), not a whole-file hash — byte-exactness matters only if something else (or a future Rust writer) re-hashes the whole file |
| 5 | `adapters/snapshot_tree.py:57-72` (`tree_revision`) | `content_hash(json.dumps(content_entries, separators=(",", ":")))` (list, not object — `sort_keys` is a no-op here) | True (default) | Feeds into `ResolvedSkill.revision`, itself part of #1's payload |
| 6 | `state/db.py:138-146` (`request_json`) via `json_text` (`state/db.py:129-133`) | Canonical `StartRequest` document | **False** (explicit) | `agents.request_json` DB column (`state/start.py:96-181`). Replay compares via `request_json_matches` (`state/db.py:176-194`, `json.loads` + dict equality), not raw bytes, but the column's own stored bytes are this canonical form |
| 7 | `resume.py:32-65` (`record_profile_grants`) | Rewrites `identity_json` after adding `profile_grants` | **True (default — no `ensure_ascii` argument)** | `agents.identity_json` DB column |
| 8 | `resume.py:~120-155` (`identity_snapshot`), `service.py:587-593` (initial write) | Initial effective-identity snapshot | **False (explicit)** | `agents.identity_json` DB column |
| 9 | `state/db.py:411-431` (`encode_context_components`) | `{"v": 2, "components": {...}}` | **False (explicit)** | `context_receipts.context_key` DB column — used as a dedup key, so its exact bytes are load-bearing even though there is no `sha256` involved |
| 10 | `state/capacity.py:21-36` (`_route_payload`) | Capacity topology payload | **False (explicit)** | Capacity-sample DB row (`insert_capacity_row`), not itself hashed downstream (`capacity/advice.py`'s `advice_key` hashes a `"::"`-joined string, not JSON) |

Finding: #7 and #8 both write `agents.identity_json`, but #7 omits
`ensure_ascii` (→ `True`) while #8 passes `ensure_ascii=False`. This is a
real inconsistency in the Python release, not a canonicalization bug in
this spike — noted here as evidence in case it ever produces a
byte-mismatched `identity_json` for a non-ASCII grant path (unlikely today:
every field involved is an identifier or filesystem path, not
free-form text).

Not included: `delivery/claude_uds.py:207` (`ensure_ascii=True, sort_keys=True`,
default separators `(', ', ': ')` — a wire frame between two Python
processes, not a hashed/persisted document, and not compact JSON in the
first place); `launch.py:228` (default `json.dumps`, no `sort_keys`, sent
over a pipe to an exec'd child, not persisted); `dispatch.py:334-336` and
`mcp.py:301` (MCP/JSON-RPC wire responses); `capacity/advice.py:103`
(`advice_key` hashes a delimited string, not JSON at all).

## Inventory: existing Rust canonicalizations

`crates/agent-run-platform/src/fs.rs` already has a `canonical_json` helper (recursive
key-sort over `serde_json::Value`, `serde_json::to_vec` for the rest —
i.e. `ensure_ascii=False`-shaped output, no Python float-repr handling).
Its only call site is `crates/agent-run-core/src/service.rs`, which fingerprints an
in-flight `StartRequest` into `replay_request_sha256` inside Rust's own
`LaunchIdentity` (`crates/agent-run-core/src/service.rs:25-51`) — written and read back only
by Rust (`LaunchIdentity::read`, same file — the record has no separate store-side
reader), with a semantic-equality
fallback (`found.request == *request`, `state/mod.rs:277`) when the
fingerprint is absent, e.g. for a historical Python-created row. It is
never compared against a Python-computed hash.

The other Rust SHA-256 producers are the same shape — self-consistent,
not cross-language:

- `adapters/plugins.rs:36-79` (`digest`): Codex hook-trust digest, Rust
  writes and Rust re-derives it (`adapters/codex.rs` trust checks).
- `adapters/materialize.rs:145-171` (`Publisher::finish`/`verify`): Rust's
  own `.agent-run-rust-snapshot.json`, explicitly a *different* format from
  Python's snapshot manifests (`adapters/materialize.rs:16` doc comment;
  confirmed incompatible in the migration status document, now `migration/status.md`).
- `supervisor.rs:118-142` (`snapshot_sha256`): produced by
  `materialize::materialize` and verified by `materialize::verify` — same
  Rust-only round trip.

Conclusion: **no existing Rust call site was replaced.** None of them
currently produce or consume a hash that must match Python bytes —
`LaunchIdentity::read` (`crates/agent-run-core/src/service.rs:35`) explicitly refuses to
resume a Python-created run today ("resuming Python-created runs is not yet
supported"). `canonical.rs` is landed ahead of the features that will need
it: once Rust resumes Python-created runs, or verifies a Python-computed
`config_revision`/config-snapshot/runtime-snapshot-index hash (M16 and
later backlog items), those call sites should use
`canonical::sha256_hex(value, ensure_ascii)` with the `ensure_ascii` value
from the matching row in the table above, instead of `fs::canonical_json`
or ad hoc `serde_json::to_vec`.

## Implementation notes

- **Key order.** Python's `sort_keys=True` sorts by Unicode code point
  (`str.__lt__`). Rust's `String`/`str` `Ord` compares UTF-8 bytes
  lexicographically, which is the same order for any valid UTF-8 string, so
  a plain `Vec<&String>::sort()` matches without decoding to `char` (used
  in both `fs::canonical_json` and `canonical.rs`).
- **Escaping.** Two Python regexes govern this: `ESCAPE` (control chars
  0x00-0x1f, `"`, `\`) for `ensure_ascii=False`, and `ESCAPE_ASCII`
  (anything outside printable ASCII 0x20-0x7e, i.e. including DEL 0x7f) for
  `ensure_ascii=True`. Astral code points (> 0xFFFF) under `ensure_ascii=True`
  are written as a UTF-16 surrogate pair (`\uD800`-`\uDBFF` + `\uDC00`-`\uDFFF`),
  matching CPython's `py_encode_basestring_ascii`.
- **Floats.** Python's JSON float encoder is `float.__repr__` — the
  shortest decimal string that round-trips, formatted with CPython's
  fixed/scientific threshold (scientific when the decimal point's position
  is `<= -4` or `> 16` digits from the first significant digit). Rust's
  `{:e}` formatting of an `f64` (no explicit precision) already produces
  the shortest round-tripping decimal digit string, just always in
  normalized scientific form; `python_float_repr` re-derives Python's
  layout from those digits rather than reimplementing shortest-digit
  generation. `NaN`/`Infinity`/`-Infinity` are emitted as the bare
  (non-standard-JSON) literals CPython's default `allow_nan=True` writes;
  no inventoried call site sets `allow_nan=False` or hashes a non-finite
  float today.
- **Integers.** ponytail: limited to what `serde_json::Number` holds
  without the `arbitrary_precision` feature — `i64`/`u64`. That feature is
  a Cargo feature on an already-used crate, but it is unified project-wide
  and changes how *every* `serde_json::Value` in this crate parses and
  compares numbers (arbitrary-precision numbers are stored as strings
  internally), which is a much bigger blast radius than this spike should
  take while another agent is restructuring `rust/` into a workspace. No
  inventoried document carries a Python int outside i64/u64 range today.
  Upgrade path if one ever does: a small typed integer variant in
  `canonical.rs`, not the crate-wide feature flag.

## Evidence

- Test vectors: `crates/agent-run-domain/tests/fixtures/canonical/vectors.json` — 64 primitive
  cases (both `ensure_ascii` modes: null/bool, empty/ASCII/quote-and-backslash/
  all-control-char/DEL/BMP-unicode/astral strings, i64/u64 boundary ints,
  15 float edge cases including the ±10^15/10^16 fixed/scientific boundary
  and the smallest subnormal, and a nested-ordering case with a Cyrillic
  key) plus 6 documents produced by the real Python functions from the
  inventory table (#1, #4, #5, #6, #9, #10 above), generated by
  `migration/tools/gen_canonical_vectors.py` against
  `/Users/pluto/projects/agent-run/.venv-py314/bin/python`.
- `crates/agent-run-domain/tests/canonical.rs`: `primitives_match_python_byte_for_byte` and
  `real_documents_match_python_byte_for_byte` assert byte equality and
  SHA-256 equality for every vector. Both pass against the generated
  fixture (`cargo test --offline --all-features --test canonical`).

## Note on the addresses in this record

The single-crate `rust/` tree this decision was written against no longer
exists; the port now lives in the `crates/` workspace. The addresses above were
repointed at the code as it stands. One item is not a moved address but a
decision that did not happen as proposed: no separate canonical-JSON module was
added, and the helper still lives in `crates/agent-run-platform/src/fs.rs`.
