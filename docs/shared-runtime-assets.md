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
