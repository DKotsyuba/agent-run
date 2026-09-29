# Shared managed runtime assets

New provider homes can place their indexed, immutable managed trees in the
caller-owned store at `<app home>/shared-assets/v1`. The home then contains one
exact whole-tree symlink per indexed root. Its original runtime index bytes,
index SHA-256, flat config, credential binding and native history remain in
the private home. The committed registry layout binds that original index to
the shared trees. Launch and resume verify that binding and the unchanged
tree content; the old strict verifier still rejects arbitrary shared links.

The supervisor validates the real native guard before moving a fresh home.
It creates a unique owned sentinel in the shared root, then runs a finite
`sandbox-exec` child that must read it and fail to write it. For Codex it also
compares the root with every effective writable grant and native temporary
root, then runs the selected Codex binary's `sandbox` command with the same
private config and permission profile. That command must read the sentinel,
fail to alter it, and write an owned workspace sentinel when the admitted
role allows writes. Claude and GLM instead run guarded `--version` metadata
startup. Each probe has a five-second process bound and removes only its own
sentinels. A failed or unsupported native check refuses conversion and launch.
The preflight holds the store's publish/GC lock while its sentinel files exist
and while guard scans run; install releases and reacquires that lock at its own
prepare and import steps. Launch wrapper scans use the same lock.

Codex keeps its original app-server and nested sandbox. The launch adds
native `-c` overrides that wrap each harness-owned stdio MCP child with
`sandbox-exec`, leaving the sealed config bytes, account environment, tool
filters and approvals unchanged. Server names that the native override key
cannot address unambiguously are refused. Claude and GLM launch their whole
child process under the guard. An already-running external MCP server or
daemon is outside the child guard; it is not treated as a protected child.

The guard's path and hardlink scan runs at launch time. It cannot control an
unrelated same-UID process that later creates a new alias, or retroactively
constrain another process. Shared conversion currently covers the roots in
the frozen runtime index. Native unindexed caches, operator migration of old
homes and shared-store collection remain separate work.
