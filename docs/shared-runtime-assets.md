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
remain readonly. Agent Run adds no OS-level write denial for the store (see
"Enforcement" below): payload immutability is a mode and digest-verification
property, not a sandbox guarantee.
Collection must drain obsolete `plugin-views` containers **before** the
trees and blobs beneath them, and treat every view still referenced by a
registered layout row as live (`agent-run-platform::plugin_views::{plugin_mount,
view_root, verify_view}` derive and check the exact names).

## Enforcement

Agent Run does not wrap any harness or MCP child in a launch sandbox, and it
runs no write probes against the store. Before moving a fresh home the
supervisor only validates the store root: it is created owner-only when
absent and must be an existing absolute real directory whose canonical form
equals its path, so no symlinked component aliases it. For Codex the
effective grant must also keep the store outside every writable root and
native temporary root; otherwise the conversion is refused and the home stays
private. Claude and GLM launch their sealed native executable directly with
their own tool and permission modes, and Codex keeps its original app-server,
native sandbox, approvals and network policy unchanged. Harness-owned stdio
MCP servers start exactly as the sealed config declares them.

What this does and does not guarantee:

- Hashes, manifests, no-follow descriptor opens, atomic import/switch,
  recovery journals, the store publish/GC lock and reference-aware collection
  are unchanged. Launch and resume still verify the original index and the
  shared tree content, so a modified payload, a replaced link or a foreign
  target is refused at the next verification.
- Nothing prevents a process running as the same user — including a harness
  child that its own native permissions allow to write there, an MCP server,
  or an unrelated process — from modifying, replacing or chmod-ing the store
  between verifications. Detection happens at the next launch or verification;
  prevention is not claimed, and `0o500`/`0o700` modes are not a security
  boundary against the same UID. Codex's own native sandbox still applies
  whatever Seatbelt or equivalent restrictions the selected Codex profile
  enforces; Agent Run neither adds to nor removes them.
- The pre-launch external-hardlink alias scan is gone with the path-deny
  rules it protected. An alias that survives outside the store is therefore
  not refused up front; the content digest check remains the detector.
- An already-running external MCP server or daemon is outside any Agent Run
  control, as before.

Shared conversion currently covers the roots in the frozen runtime index.
Native unindexed caches remain separate work.

Operator migration of retained homes and shared-store collection are wired:
`agent-run storage status | compact | recover` surveys, relocates and
recovers, and `agent-run-core::storage_gc` collects unreferenced objects
inside the existing housekeeping cycle — see
`docs/runtime-storage-layouts.md` for both contracts.
