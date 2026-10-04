# Shared managed runtime assets

New provider homes can place their indexed, immutable managed trees in the
caller-owned store at `<app home>/shared-assets/v1`. The home then contains one
exact whole-tree symlink per indexed root. Its original runtime index bytes,
index SHA-256, flat config, credential binding and native history remain in
the private home. The committed registry layout binds that original index to
the shared trees. Launch and resume verify that binding and the unchanged
tree content; the old strict verifier still rejects arbitrary shared links.

Managed Codex plugin version roots are the one mounted geometry: native
plugin discovery ignores a version directory that is itself a symlink, so a
root shaped `plugins/cache/personal/<plugin>/<version>` (both components from
the safe identity charset) is switched one level higher. The relocation
coordinator proves the plugin parent holds exactly its single indexed
version — a second version, remote metadata marker, or any sibling refuses
the layout during planning and preserves the parent untouched — then moves
the whole parent into the token-bound backup and replaces it with one exact
symlink onto a readonly container at
`<store>/plugin-views/<scope>/<view-id>/<version>/…`. `<view-id>` is the
SHA-256 of the exact preimage
`plugin-view-v1\n<scope>\n<manifest-sha256>\n<version>`, binding the original
`SharedTreeRef` plus the safe version name and no mutable state. Inside, the
named version directory is real, every payload and the unchanged
`.agent-run-snapshot.json` bytes are internal hardlinks of the imported
`trees/<scope>/<manifest-sha256>` entries, directories are owner-only `0o700`
(previously published `0o500` trees and views remain verifiable and
collectible), and no
payload byte is copied. Ordinary skill and other roots keep the plain
whole-root link; the original index bytes and index SHA-256 never change.
Owner-only writable directory modes let the broker publish and collect on
macOS versions that refuse renaming write-disabled directories. Payload files
remain readonly; the qualified native shared-root guard prevents agent writes,
entry replacement and permission changes throughout the store.
Collection must drain obsolete `plugin-views` containers **before** the
trees and blobs beneath them, and treat every view still referenced by a
registered layout row as live (`agent-run-platform::plugin_views::{plugin_mount,
view_root, verify_view}` derive and check the exact names).

The guard is `/usr/bin/sandbox-exec` with a deny-write Seatbelt profile on
macOS and `/usr/bin/bwrap` on Linux. The Linux wrapper starts the child in a
fresh user and mount namespace that keeps the whole host view, binds the
store read-only (write, create, truncate, unlink, rename, link, chmod, chown
and timestamp changes fail) and binds every ancestor directory onto itself so
renaming the store or any ancestor fails with `EBUSY`. Nested namespaces
cannot unmount or remount those binds, and `/proc/<pid>/root` links of
unconfined same-UID processes are not reachable from the child. PIDs, the
process group, signals, environment, network, stdio and exit status are
unchanged; setuid programs cannot gain privilege in the child, and a rename
between the workdir and a directory on another bound ancestor reports
`EXDEV` (tools fall back to copying). Before launch the Linux wrapper also
refuses a store that another mount of the same filesystem exposes at a second
path. A host without the helper, or whose kernel or container denies
unprivileged user and mount namespaces (Docker's default seccomp profile
does), refuses shared conversion; nothing falls back to private copies.

The supervisor validates the real native guard before moving a fresh home.
It creates a unique owned sentinel in the shared root, then runs finite
guarded children that must read it and fail to write or chmod it; a child that
cannot start or read is reported as an unusable guard. For Codex it also
compares the root with every effective writable grant and native temporary
root, then runs the selected Codex binary's `sandbox` command with the same
private config and permission profile. That command must read the sentinel,
fail to alter its bytes or mode, and write an owned workspace sentinel when the admitted
role allows writes. Claude and GLM instead run guarded `--version` metadata
startup. Each probe has a five-second process bound and removes only its own
sentinels. A failed or unsupported native check refuses conversion and launch.
The preflight holds the store's publish/GC lock while its sentinel files exist
and while guard scans run; install releases and reacquires that lock at its own
prepare and import steps. Launch wrapper scans use the same lock.

On macOS Codex keeps its original app-server and nested sandbox. The launch
adds native `-c` overrides that wrap each harness-owned stdio MCP child with
`sandbox-exec`, leaving the sealed config bytes, account environment, tool
filters and approvals unchanged. Server names that the native override key
cannot address unambiguously are refused. On Linux the whole Codex
app-server launches under the guard, because Codex's own Linux sandbox
cannot deny chmod; its executor, sandboxed commands and MCP children inherit
the read-only store, and the Codex qualification probe runs `codex sandbox`
under the same guard. Claude and GLM launch their whole child process under
the guard on both platforms. An already-running external MCP server or
daemon is outside the child guard; it is not treated as a protected child.

The guard's path and hardlink scan runs at launch time. It cannot control an
unrelated same-UID process that later creates a new alias, or retroactively
constrain another process. Shared conversion currently covers the roots in
the frozen runtime index. Native unindexed caches remain separate work.

Operator migration of retained homes and shared-store collection are wired:
`agent-run storage status | compact | recover` surveys, relocates and
recovers, and `agent-run-core::storage_gc` collects unreferenced objects
inside the existing housekeeping cycle — see
`docs/runtime-storage-layouts.md` for both contracts.
