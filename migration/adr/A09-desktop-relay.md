# ADR A09: Codex Desktop completion relay boundary

## Status

Proposed.

This is the read-only half of M09. It records source and static application-bundle
evidence only. No Codex Desktop process was launched, no live socket was opened,
and no chat, user-content log, or credential was read. The installed bundle
inspected was `/Applications/ChatGPT.app`, identifier `com.openai.codex`, version
`26.908.70816` (build `9275`). A later authorized live test is still required by
T71.

## Context

Completion delivery is durable and at-least-once. A terminal agent creates a
bounded notice which is sent only to its existing orchestrator binding
([`src/agent_run/delivery/dispatch.py:257-296`](../../src/agent_run/delivery/dispatch.py),
[`src/agent_run/delivery/base.py:239-321`](../../src/agent_run/delivery/base.py)).
The persisted transport name `codex_queue` now denotes the Desktop relay, not a
CLI queue fallback ([`docs/architecture.md:178-191`](../../docs/architecture.md),
[`src/agent_run/delivery/codex_queue.py:258-317`](../../src/agent_run/delivery/codex_queue.py)).

The migration gate is narrower: can the ordinary agent-run Rust executable be
the process which connects to the Desktop host pipe? Correct Rust framing on a
fake socket does not prove admission at that pipe
([`migration/rust-migration-plan.md:620-630`](../rust-migration-plan.md),
[`migration/rust-migration-plan.md:1037-1042`](../rust-migration-plan.md)).

## Findings

### 1. Process and capability chain

| Boundary | How it starts / communicates | Authority evidence |
|---|---|---|
| Desktop -> agent-run MCP | Desktop starts `agent-run ... mcp` and supplies `CODEX_MCP_NODE_PATH` and `CODEX_APP_TOOLS_PIPE_PATH`. Only a real, uninjected `mcp` CLI attempts the wrapper ([`src/agent_run/cli.py:946-950`](../../src/agent_run/cli.py)). | Possession of the environment values is necessary but not sufficient for host admission. |
| Python CLI -> signed Node wrapper | `_exec_desktop_relay` requires both values, rejects NUL/overlong/non-absolute values, and requires Node to be an executable file. It then calls `execv(node, [node, wrapper.cjs, python, home, -m, agent_run.cli, --home, home, mcp])`; there is no shell or PATH lookup ([`src/agent_run/cli.py:888-904`](../../src/agent_run/cli.py), [`tests/test_codex_desktop_relay.py:402-418`](../../tests/test_codex_desktop_relay.py)). | The executable is selected by Desktop through an absolute environment path. In the inspected bundle, `cua_node/bin/node` is Developer-ID signed by OpenAI team `2DC432GLL2`, signing identifier `node` (`codesign -dvvv`). |
| Signed Node wrapper -> Python MCP child | The wrapper creates its relay first, then spawns the exact Python/arguments passed above with inherited stdio. It removes both Desktop capability variables from the child environment, preventing recursive wrapping ([`src/agent_run/delivery/codex_desktop_host.cjs:198-225`](../../src/agent_run/delivery/codex_desktop_host.cjs)). | The child has neither the pipe path nor signed-Node path. Node retains the Desktop capability. |
| Delivery dispatcher -> wrapper | The wrapper listens at `<agent-run-home>/ar-cdx-v3-<pid>.sock`. It creates the home with mode `0700`, uses umask `077`, and chmods the Unix stream socket to `0600` ([`src/agent_run/delivery/codex_desktop_host.cjs:9-13`](../../src/agent_run/delivery/codex_desktop_host.cjs), [`src/agent_run/delivery/codex_desktop_host.cjs:217-225`](../../src/agent_run/delivery/codex_desktop_host.cjs)). Python discovers at most 16 `ar-cdx-*.sock` paths in that home ([`src/agent_run/delivery/codex_desktop_relay.py:176-196`](../../src/agent_run/delivery/codex_desktop_relay.py)). | This local hop has filesystem/same-user access control only: no token, inherited descriptor, code-signature check, or server-side peer-credential check is present. Python also does not stat the discovered endpoint. Rust currently strengthens its client and listener with socket-owner/peer-UID checks ([`crates/agent-run-core/src/delivery/relay.rs:40-71`](../../crates/agent-run-core/src/delivery/relay.rs), [`crates/agent-run-core/src/delivery/relay.rs:218-235`](../../crates/agent-run-core/src/delivery/relay.rs)). |
| Signed Node -> Desktop tools pipe | Node reads the opaque absolute pipe path from `CODEX_APP_TOOLS_PIPE_PATH` and opens a new `net.Socket`; the descriptor is not inherited and no protocol token is sent. It first calls `tools/list`, then only `send_message_to_thread` through `tools/call` ([`src/agent_run/delivery/codex_desktop_host.cjs:168-187`](../../src/agent_run/delivery/codex_desktop_host.cjs)). The bundle's own app-tools MCP does the same ([`server.mjs:24771-24817`](</Applications/ChatGPT.app/Contents/Resources/plugins/openai-bundled/plugins/codex-app-tools/server.mjs>), [`server.mjs:24960-25023`](</Applications/ChatGPT.app/Contents/Resources/plugins/openai-bundled/plugins/codex-app-tools/server.mjs>)). | Desktop applies both socket permissions and macOS code-signature authorization, detailed below. |

The bundled launcher searches, in order, the injected MCP Node, browser-use
Node, bundled `cua_node/bin/node`, a CLI-adjacent runtime, a cached runtime, and
finally an absolute `node` on PATH
([`launch_codex_app_tools_mcp:17-35`](</Applications/ChatGPT.app/Contents/Resources/plugins/openai-bundled/plugins/codex-app-tools/scripts/launch_codex_app_tools_mcp>)).
Python itself performs no such search: it uses only `CODEX_MCP_NODE_PATH`.

### 2. Relay and host protocols

The relay version is selected solely by the discovered socket filename. The
notice format version inside rendered text remains independently fixed at v1
([`src/agent_run/delivery/codex_desktop_relay.py:23-60`](../../src/agent_run/delivery/codex_desktop_relay.py),
[`src/agent_run/delivery/codex_desktop_host.cjs:76-87`](../../src/agent_run/delivery/codex_desktop_host.cjs)).

| Relay wire | Advertised socket | Exact request keys |
|---|---|---|
| v1 legacy | `ar-cdx-*.sock` other than the richer prefixes | `version`, `op="completion"`, `thread_id`, `notification_id`, `agent_id`, `status` |
| v2 selector-rich | `ar-cdx-v2-*.sock` | v1 plus `runtime`, `model`, `effort` |
| v3 failure-aware | `ar-cdx-v3-*.sock` | v2 plus `failure_kind` |

The host requires the exact key set: unknown, omitted, or cross-version fields
are rejected. IDs must be nonblank, NUL-free, and at most 512 characters;
`agent_id`, `notification_id`, and terminal status have stricter allowlists.
Optional metadata is null or a nonblank maximum-128-code-point string
([`src/agent_run/delivery/codex_desktop_host.cjs:125-165`](../../src/agent_run/delivery/codex_desktop_host.cjs),
[`tests/test_codex_desktop_relay.py:312-380`](../../tests/test_codex_desktop_relay.py)).

| Property | Local relay | Desktop host pipe |
|---|---|---|
| Framing | 4-byte unsigned little-endian length followed by UTF-8 JSON object | Same framing around JSON-RPC 2.0 objects |
| Bound | 1..8192 payload bytes | 1..8 MiB payload bytes |
| Deadline | One 10-second discovery/send budget across at most 16 endpoints | One 8-second connect/list/call budget |
| Calls | One completion request per connection | `tools/list({threadStartKind:"all"})`, then `tools/call` for discovered `send_message_to_thread` only |
| Response | Exactly `{ "outcome": "accepted" }` or `{ "outcome": "rejected" }`; the current host may also produce `ambiguous` | Correlated JSON-RPC response; `result.success` is the delivery acknowledgement |

Framing and deadlines are implemented at
[`src/agent_run/delivery/codex_desktop_relay.py:12-15`](../../src/agent_run/delivery/codex_desktop_relay.py),
[`src/agent_run/delivery/codex_desktop_relay.py:63-95`](../../src/agent_run/delivery/codex_desktop_relay.py),
and [`src/agent_run/delivery/codex_desktop_host.cjs:7-73`](../../src/agent_run/delivery/codex_desktop_host.cjs).
The fixed host calls are at
[`src/agent_run/delivery/codex_desktop_host.cjs:168-186`](../../src/agent_run/delivery/codex_desktop_host.cjs).

Acknowledgement is deliberately conservative:

| Event | Classification / next action |
|---|---|
| Connect fails before entering the write phase | Try the next relay; refused/missing stale paths are unlinked. |
| Relay explicitly returns `rejected` | Try the next relay; if none accepts, `relay_rejected`. |
| Relay explicitly returns `accepted` | `relay_accepted`; delivery completes. |
| EOF, malformed/unknown response, timeout, or I/O error after the write phase begins | `relay_ambiguous`; stop discovery because the notice may have arrived. |
| No usable relay and no explicit rejection | `relay_unavailable`. |
| Host validation fails, tool is absent, pipe/connect/list fails, or `success:false` is returned before/for the call | `rejected`. |
| Host call was attempted and acknowledgement is missing, malformed, mismatched, errors, or disconnects | `ambiguous`. |

The client transition is at
[`src/agent_run/delivery/codex_desktop_relay.py:114-174`](../../src/agent_run/delivery/codex_desktop_relay.py);
the host transition is at
[`src/agent_run/delivery/codex_desktop_host.cjs:168-196`](../../src/agent_run/delivery/codex_desktop_host.cjs).
Durable dispatch retries both rejected/unavailable and ambiguous attempts, but
records ambiguity so consumers can deduplicate by notification identity
([`src/agent_run/delivery/dispatch.py:270-331`](../../src/agent_run/delivery/dispatch.py)).
It never tries a second queue path after relay rejection or ambiguity
([`tests/test_codex_queue.py:142-177`](../../tests/test_codex_queue.py)).

### 3. Desktop peer checks (static evidence)

The installed production bundle enables a native peer authorizer for the same
dynamic app-tools pipe. Static `app.asar` line 438 shows that the server passes
each accepted socket FD to `authorizeSocketPeer`; it chmods the socket to
`0600` and rejects an unauthorized socket before parsing a request:

- `/Applications/ChatGPT.app/Contents/Resources/app.asar:438` (minified source,
  `dynamic-app-tools-native-pipe`, `authorizeSocketPeer`);
- `/Applications/ChatGPT.app/Contents/Resources/native/browser-use-peer-authorization.node`
  Mach-O offsets `0x220c-0x2284` obtain `LOCAL_PEERTOKEN` and resolve the code
  identity of the peer, its parent, and its grandparent;
- offsets `0x25b8-0x288c` require every one of those three identities to match
  team `2DC432GLL2` and an allowed signing identifier before returning
  `authorized`; otherwise the reason is `missing-code-signing-identity` or
  `untrusted-code-signing-identity`;
- `strings -a -t x` shows team ID at `0x4c78` and candidate identifiers at
  `0x4fdc-0x508d`: Codex release variants, `com.openai.codex.runtime`,
  `com.openai.codex.agent`, `com.openai.codex.dev`, `codex`, `node`, and
  `node_repl`.

The binary imports Security.framework, `libbsm`, `getsockopt`, and
`proc_pidinfo`; its exported identity helpers use `SecCodeCopyGuestWithAttributes`
and `SecCodeCopySigningInformation` (`nm -gU`, `otool -L`, and `otool -tvV`).
`codesign -dvvv` confirms the bundled `codex` and `cua_node/bin/node` binaries
carry the required OpenAI team and the allowlisted identifiers `codex` and
`node`. There is no evidence of a bearer token, shared secret, or inherited
socket descriptor in this path.

**Finding:** an ordinary agent-run Rust executable cannot directly obtain the
current macOS host capability. Its peer signing identity is neither OpenAI team
`2DC432GLL2` nor in the embedded identifier allowlist. Protocol compatibility
does not change that result. Confidence is high for the inspected build, but a
live negative/positive control remains required and a Desktop update can change
the policy.

## Options

| Option | Feasibility and evidence | Security / parity |
|---|---|---|
| **A. Native Rust connects directly** | Wire implementation is straightforward: the bundled app-tools client documents the u32LE JSON-RPC protocol and 8 MiB bound ([`server.mjs:24974-25160`](</Applications/ChatGPT.app/Contents/Resources/plugins/openai-bundled/plugins/codex-app-tools/server.mjs>)). Admission is not feasible for a normally signed or unsigned agent-run binary in the inspected build. It needs an OpenAI signing/allowlist change or a documented host API with different authorization. | Best language boundary if officially admitted. Until then it fails before v1/v2/v3 semantics matter. A native relay could preserve all three local wires, but T71 cannot pass on fake-host evidence alone. |
| **B. Rust owns notice/relay; minimal host-executed shim crosses the pipe** | Feasible and already prototyped. Rust validates v1/v2/v3, renders the notice, owns the local listener and spawns the exact Desktop-supplied Node with embedded JS via `node -e`; the child gets a cleared environment containing only the pipe path ([`crates/agent-run-core/src/delivery/relay.rs:120-192`](../../crates/agent-run-core/src/delivery/relay.rs), [`crates/agent-run-core/src/delivery/relay.rs:204-292`](../../crates/agent-run-core/src/delivery/relay.rs)). The shim accepts only `{notificationId,prompt,threadId}` and exposes only list plus `send_message_to_thread` ([`assets/desktop-transport.cjs:39-73`](../../assets/desktop-transport.cjs)). | Preserves v1/v2/v3 at the Rust relay boundary and accepted/rejected/ambiguous host semantics. The signed executable, not script bytes, receives Desktop authority, so the embedded script must remain fixed, bounded, non-configurable, and covered by contract tests. This is a hybrid external-host dependency, not a pure/full-Rust relay. |
| **C1. Bundled `codex-app-tools` MCP** | Not an independent escape hatch. Its launcher selects a Node runtime and its JS connects to the same code-signature-authorized pipe. Its plugin definition marks `send_message_to_thread` approval as `prompt` and declares the same host environment ([`.mcp.json:1-44`](</Applications/ChatGPT.app/Contents/Resources/plugins/openai-bundled/plugins/codex-app-tools/.mcp.json>)). | Same trust boundary as B, broader general MCP surface, and no v1/v2/v3 relay contract. Unsuitable as an implicit completion transport. |
| **C2. Legacy `codex queue` CLI** | Python retains a bounded sender which executes absolute `codex queue --thread ID --message TEXT` ([`src/agent_run/delivery/codex_queue.py:42-137`](../../src/agent_run/delivery/codex_queue.py)), but the production transport intentionally never calls it ([`src/agent_run/delivery/codex_queue.py:258-317`](../../src/agent_run/delivery/codex_queue.py), [`docs/architecture.md:185-190`](../../docs/architecture.md)). Current CLI support/stability is therefore unproved. | Separate protocol: cannot claim v1/v2/v3 parity. It places notice/session values in argv, has timeout ambiguity, and would reintroduce a second-send risk if used after an ambiguous relay attempt. Consider only as a separately authorized, capability-detected transport with its own persisted name and tests. |
| **C3. URL scheme, Apple events, or OS notification** | The bundle declares a `codex:` URL scheme and Apple-events entitlement, but static inspection found no documented route which inserts a bounded message into an existing thread. An OS notification is not a chat message. | No acknowledgement, session binding, deduplication, or v1/v2/v3 parity evidence. Not feasible on current evidence. |

## Recommendation

Do not claim that native Rust can acquire the Desktop host capability. Under A09,
the **pure/full-Rust Desktop cutover is blocked** until OpenAI exposes a native
admission path or a live experiment disproves the installed policy analysis.

For a release allowed to retain an explicit external host dependency, choose
Option B: Rust owns the durable outbox, notice construction, v1/v2/v3 relay,
limits, deadlines, and acknowledgement classification; a minimal embedded shim
runs only under Desktop's signed Node to make the two fixed host calls. Describe
that release as hybrid at this boundary. If the release definition forbids any
installed JavaScript, keep the shim only as a development oracle and do not ship
Desktop completion delivery or declare full parity.

## Live experiment plan

This plan requires separate authorization because it sends two synthetic notices
to a disposable Desktop chat. M48 should first add a non-release
`a09_desktop_probe` example which speaks minimal MCP over stdio, inherits the
Desktop-provided capability environment, obtains the dedicated test thread ID
from MCP metadata, and supports `--mode direct` and `--mode signed-node-shim`.
It must emit only a result enum and timing—never a pipe path, thread ID, tool
inventory, response body, or environment.

1. Prepare and verify the probe offline:

   ```sh
   A09_REPO="$(pwd -P)"
   cargo test -p agent-run-core --test delivery_hosts --locked
   cargo build -p agent-run-core --example a09_desktop_probe --locked
   A09_HOME="$(mktemp -d /tmp/agent-run-a09.XXXXXX)"
   chmod 700 "$A09_HOME"
   printf '%s\n' "$A09_REPO/target/debug/examples/a09_desktop_probe --mode direct --home $A09_HOME"
   printf '%s\n' "$A09_REPO/target/debug/examples/a09_desktop_probe --mode signed-node-shim --home $A09_HOME"
   ```

2. In a dedicated Codex Desktop test profile, create one disposable chat named
   `A09 relay smoke`. Register the first exact command printed during preparation,
   then the second, restarting only that temporary MCP entry between runs; do not
   replace either resolved path with a symlink. Invoke the probe tool once
   per mode with distinct fixed IDs `ntf_a09_direct_1` and `ntf_a09_shim_1` and
   the exact notice `agent-run/completion\n\n- ID: ag-20000101-000000-0000000000\n- Status: succeeded\n- Runtime/model: test/test:none\n- Notice: [notification <id> v1]`.

3. Record these bounded signals:

   | Mode | Success | Failure |
   |---|---|---|
   | direct | `tools/list` and fixed `tools/call` receive correlated success, and exactly one matching notice appears in the disposable chat | authorization/EOF before list, explicit host error, missing tool, non-success result, or no/excess chat notice |
   | signed-node-shim positive control | Same success signal and exactly one notice | Any authorization, protocol, or acknowledgement failure |

   The static prediction is **direct rejected, shim accepted**. A direct success
   must trigger a fresh inspection of the exact executing binary's team/signing
   identity and process ancestry before changing this ADR.

4. Roll back by removing the two temporary MCP entries, closing/deleting only
   the disposable chat/profile, terminating only probe PIDs recorded by the
   harness, and deleting the exact `A09_HOME` directory after verifying its
   `/tmp/agent-run-a09.*` prefix.

Never touch the production Codex profile, `~/.codex`, the production
`~/.agent-run`, existing relay sockets, other chats, conversation/log databases,
credentials, Keychain, launchd jobs, or installed application files. Do not read
Desktop logs as a substitute for the bounded probe result.

## Consequences for Rust tasks M46-M48

- **M46 — dispatch:** keep one owned attempt, durable retry/backoff/expiry, and
  ambiguity atomic with evidence. Acceptance: relay/host rejection is retryable;
  post-call uncertainty is ambiguous; neither path invokes a second transport;
  duplicate retries keep the same notification ID.
- **M47 — notice:** port the package-owned template/guidance and exact control /
  Unicode escaping into Rust. Acceptance: Python/Rust byte-for-byte vectors for
  every terminal status, known/unknown failure kind, absent selectors, C0/C1,
  DEL, U+2028/U+2029, braces, and replacement-like text; no task, answer, error
  prose, session, environment, or credential enters notice/evidence.
- **M48 — hosts:** retain strict v1/v2/v3 and same-user local-socket checks, port
  Python discovery/error semantics, and differential-test fake relays plus the
  signed shim. Acceptance: u32LE bounds (8 KiB local, 8 MiB host), 10/8-second
  budgets, exact key sets, accepted/rejected/ambiguous matrices, stale endpoint
  behavior, same-UID enforcement, and the ignored authorized live experiment
  above. M48 may ship B only under an explicit hybrid-release decision; it may
  not mark native Desktop admission complete from shim or fake-host success.

## Open questions

- Will OpenAI document and support a native third-party signing identifier or a
  different scoped capability for completion delivery?
- Does the peer/parent/grandparent policy or allowlist differ across Desktop
  release channels, Windows, or a later production build?
- Is `codex queue` still a supported external API with stable acknowledgement
  semantics, and if so should it become a separately named transport rather than
  a fallback?
- Is a hybrid release acceptable, or does “full Rust” require Desktop completion
  delivery to remain blocked/omitted?
- Who authorizes the disposable-profile live test and supplies a test-only
  Desktop session without exposing production homes or conversations?
