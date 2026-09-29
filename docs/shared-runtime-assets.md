# Shared asset launch guard prototype

`agent-run-platform::shared_asset_guard::SharedAssetGuard` prepares a macOS
`sandbox-exec` argv for one canonical shared asset root. No launcher uses this
primitive yet. `new` requires an absolute directory path with no symlinked
components. `wrap` returns an argv for the requested child; it does not spawn
the child.

The generated profile contains `file-write*` rules for the root subtree and
exact ancestor paths. Filesystem paths travel as `-D` arguments, not profile
source text. A no-follow scan before argv creation counts regular-file paths
by device and inode. Internal hardlinks pass; a link count exceeding the paths
found inside the root refuses the launch. The scan stops after 100,000 paths.

The scan is a point-in-time check. It cannot prevent an unrelated same-UID
process from creating a new external alias later, and a wrapped child does not
constrain unrelated processes that were already running. The ignored
`live_native_guard` test checks native deny/allow behavior, including child and
grandchild processes, on a macOS host that permits `sandbox-exec` to apply a
profile. A nested test executor may prevent profile application. Wrapping an
entire Codex app-server may also prevent its own nested sandbox application;
that integration remains unqualified. This primitive does not choose an adapter
launch boundary or change Codex permissions.

## Codex integration gate

Keep Codex's own shell sandbox. If the selected managed `Projects` profile
already grants the canonical shared store read access and the store is disjoint
from every effective write grant and denied subtree, reuse that profile. If
either condition cannot be proved, refuse the launch until a supported policy
is available. Do not place the store under `/tmp`, `$TMPDIR`, a workspace root,
or a writable cache. In Codex CLI 0.156.1 on macOS, a nominal `read` override
under `/private/tmp` did not stop writes because of the temporary-path grant.

Before admitting a Codex launch, compare the canonical shared root against
every canonical writable root in `Grant::writable_roots`, plus the built-in
`:tmpdir` and `:slash_tmp` roots. Reject if either path is an ancestor of the
other, or if a root cannot be resolved. `Grant::new` already freezes the chosen
profile and writable roots; `Grant::verify` checks the app-server's echo. The
shared-root overlap check belongs beside that existing admission path, before
`Grant::request`, and must fail closed. A disposable native `codex sandbox
--include-managed-config -P <profile>` fixture should then prove shared reads,
ordinary sibling writes, and protected write denials under the actual selected
profile. The ignored test in this module uses a temporary profile and owns all
of its fixture files. It does not install a profile or alter managed policy.

Codex permission profiles constrain its sandboxed commands, not the app-server
plugin loader or an already-running external MCP server. Keep the app-server
outside the outer Seatbelt wrapper so its own sandbox can start. A harness-owned
stdio MCP child can use `SharedAssetGuard::wrap` at its launch boundary; no
proxy is needed. Any future shared-layout bridge must also preserve the
independent `materialize::verify` check in `provider::sealed` before credentials
bind. The ignored fixture also checks metadata-only `claude --version` startup
under the outer guard. See the [Codex permissions reference](https://developers.openai.com/codex/permissions)
for profile scope and path precedence.
