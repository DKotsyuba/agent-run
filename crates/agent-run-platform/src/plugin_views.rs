//! Readonly plugin-parent view containers for the shared store.
//!
//! Native Codex plugin discovery (rust-v0.156.1 `core-plugins/src/store.rs`)
//! classifies a managed plugin version with `entry.file_type().is_dir()` and
//! ignores a version directory that is a symbolic link; the same walker hides
//! plugin skills whose immediate children or leaf files are links. A home
//! converted by [`crate::shared_assets`] therefore becomes invisible to the
//! native plugin store exactly when the indexed version root itself is the
//! shared-tree symlink. The proven-compatible geometry keeps the link one
//! level higher: the plugin parent directory in the runtime home is one
//! symlink onto a store-owned container that holds a **real** directory named
//! after the version, with every payload an internal hardlink of the already
//! published shared tree. This module derives, materializes, and verifies
//! those containers; the relocation coordinator in `agent-run-core` performs
//! the home-side parent switch.
//!
//! Layout below the shared-store root (`shared-assets/v1`):
//!
//! - `plugin-views/<scope>/<view-id>/<version>/…`: one readonly container per
//!   (scope, original tree, version) triple. `<view-id>` is the SHA-256 of the
//!   exact preimage `plugin-view-v1\n<scope>\n<manifest-sha256>\n<version>`,
//!   so the identity binds the original [`SharedTreeRef`] plus the safe
//!   version name and never any mutable current state. Directories are
//!   owner-only `0o700` (legacy `0o500` containers stay verifiable), files
//!   keep the shared tree's physical `0o400`/`0o500` modes,
//!   and every file — including the unchanged
//!   `.agent-run-snapshot.json` bytes — is one internal hardlink of the
//!   corresponding `trees/<scope>/<manifest-sha256>` entry, so no payload
//!   byte is copied and every file stays the single protected-store inode.
//!
//! Publication stages under a `.agent-run-staging-*.tmp` name and renames with
//! the kernel's no-replace rename, exactly like shared trees; an interrupted
//! publisher leaves at most that recoverable staging orphan for the store's
//! collector. Garbage collection must collect obsolete views **before** the
//! trees and blobs they hardlink.

use crate::{
    fs::{self, Dir, EntryType},
    shared_assets::{self, SharedStoreLock, SharedTreeRef, TEMP_PREFIX, TEMP_SUFFIX},
    snapshot_tree::{entry_map, load_manifest, MAX_METADATA, SNAPSHOT_MANIFEST},
};
use agent_run_domain::{error::invalid, Error, Result};
use std::{
    collections::{BTreeMap, BTreeSet},
    os::unix::fs::MetadataExt,
    path::{Path, PathBuf},
};

/// Store namespace holding derived plugin-parent view containers.
pub const VIEW_NAMESPACE: &str = "plugin-views";
/// Home-relative prefix of managed Codex plugin version roots; only roots
/// directly below it, shaped `<prefix>/<plugin>/<version>`, use parent views.
const PLUGIN_PREFIX: &str = "plugins/cache/personal";
/// Domain separator opening the view-identity preimage.
const VIEW_ID_DOMAIN: &str = "plugin-view-v1";
/// Bound on entries one verification walk may visit, mirroring the shared
/// tree bound plus the container's own version and manifest entries.
const MAX_VIEW_ENTRIES: usize = 4098;
/// Upper bound for one safe version component.
const MAX_COMPONENT: usize = 128;

/// Returns whether `value` is safe path material for a view directory name:
/// nonempty, bounded, and restricted to the plugin identity charset.
fn safe_component(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= MAX_COMPONENT
        && value != "."
        && value != ".."
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || b"_.+-".contains(&byte))
}

/// Classifies one indexed managed-root path as a managed Codex plugin version
/// root and returns its mount geometry.
///
/// `root_key` qualifies exactly when it reads
/// `plugins/cache/personal/<plugin>/<version>` with both trailing components
/// made of safe identity material (`[A-Za-z0-9_.+-]`, bounded, neither `.` nor
/// `..`). The result is `(parent, version)` where `parent` is the plugin
/// parent path relative to the runtime home — the entry the coordinator
/// replaces with one exact symlink — and `version` names the real version
/// directory the store-side container must hold. Any other shape returns
/// `None` and keeps the unchanged whole-root link geometry.
pub fn plugin_mount(root_key: &str) -> Option<(String, String)> {
    let rest = root_key.strip_prefix(PLUGIN_PREFIX)?.strip_prefix('/')?;
    if rest.matches('/').count() != 1 {
        return None;
    }
    let mut parts = rest.split('/');
    let plugin = parts.next().expect("one separator splits two parts");
    let version = parts.next().expect("one separator splits two parts");
    if !safe_component(plugin) || !safe_component(version) {
        return None;
    }
    Some((format!("{PLUGIN_PREFIX}/{plugin}"), version.to_owned()))
}

/// Returns the deterministic view identity digest for one mount triple.
///
/// The preimage is exactly `plugin-view-v1\n<scope>\n<manifest-sha256>\n<version>`
/// encoded UTF-8; the result is its SHA-256 in lowercase hex. Collectors and
/// verifiers derive the same directory name from a registered
/// [`SharedTreeRef`] plus the indexed version without any mutable state.
pub fn view_identity(reference: &SharedTreeRef, version: &str) -> Result<String> {
    if !shared_assets::is_scope(&reference.scope) || !is_manifest(&reference.manifest_sha256) {
        return Err(invalid("shared tree reference is invalid"));
    }
    if !safe_component(version) {
        return Err(invalid("plugin version is not safe view path material"));
    }
    Ok(fs::sha256(
        format!(
            "{VIEW_ID_DOMAIN}\n{}\n{}\n{version}",
            reference.scope, reference.manifest_sha256
        )
        .as_bytes(),
    ))
}

/// Returns whether `value` is a strict 64-lowercase-hex manifest digest.
fn is_manifest(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| matches!(byte, b'0'..=b'9' | b'a'..=b'f'))
}

/// Returns the container path `<store>/plugin-views/<scope>/<view-id>` after
/// validating the reference digests and the version name, without touching
/// the filesystem.
pub fn view_root(store_root: &Path, reference: &SharedTreeRef, version: &str) -> Result<PathBuf> {
    let identity = view_identity(reference, version)?;
    Ok(store_root
        .join(VIEW_NAMESPACE)
        .join(&reference.scope)
        .join(identity))
}

/// Materializes one readonly plugin-parent view container idempotently and
/// returns its absolute path.
///
/// `store_root` must be the canonical shared-store root, `reference` the
/// already imported shared tree (its `trees/<scope>/<manifest-sha256>`
/// directory must exist and be readable), and `version` the safe indexed
/// version name. The container is rebuilt as `<view>/<version>/…`: one real
/// directory per manifest directory entry, one internal hardlink of the
/// corresponding shared-tree file per file entry, plus one hardlink of the
/// tree's unchanged `.agent-run-snapshot.json` bytes. Nothing is copied, no
/// link leaves the store, and every directory keeps its final owner-only
/// `0o700` mode — the mode [`Dir::make_directory`] created it with — so the
/// publish rename is never denied as a write-disabled-directory rename.
///
/// An existing container is verified through [`verify_view`] and reused,
/// never rebuilt. A missing container is staged under a fresh
/// `.agent-run-staging-*.tmp` name while holding [`SharedStoreLock`] and
/// renamed into place with the kernel's no-replace rename; a
/// concurrent publisher winning that name loses nothing and is verified
/// instead. On error the staging directory is removed and no partial
/// container exists under its final name.
pub fn materialize(store_root: &Path, reference: &SharedTreeRef, version: &str) -> Result<PathBuf> {
    let root = shared_assets::validated_root(store_root)?;
    let identity = view_identity(reference, version)?;
    let tree = shared_assets::shared_tree_root(&root, reference)?;
    let relative = Path::new(VIEW_NAMESPACE)
        .join(&reference.scope)
        .join(&identity);
    let store = Dir::open(&root)?;
    match store.entry_type(&relative) {
        Ok(EntryType::Directory) => {
            verify_view(&root, reference, version)?;
            return Ok(root.join(&relative));
        }
        Err(Error::Io(error)) if error.kind() == std::io::ErrorKind::NotFound => {}
        Ok(_) => {
            return Err(invalid(
                "plugin view destination exists and is not a real directory",
            ));
        }
        Err(error) => return Err(error),
    }
    let tree_dir = Dir::open(&tree)?;
    let manifest_bytes = tree_dir.read(Path::new(SNAPSHOT_MANIFEST), MAX_METADATA)?;
    if fs::sha256(&manifest_bytes) != reference.manifest_sha256 {
        return Err(invalid(
            "shared tree manifest hash does not match its reference",
        ));
    }
    let entries = entry_map(
        &load_manifest(&tree_dir)?.ok_or_else(|| invalid("shared tree manifest is missing"))?,
    )?;
    let staging = Path::new(VIEW_NAMESPACE)
        .join(&reference.scope)
        .join(format!(
            "{TEMP_PREFIX}{}{TEMP_SUFFIX}",
            uuid::Uuid::new_v4().simple()
        ));
    let published = {
        let _guard = SharedStoreLock::acquire(&root)?;
        store.directory(staging.parent().expect("scoped staging parent"))?;
        store.directory(&staging)?;
        let inside = staging.join(version);
        stage_view(&store, &inside, &entries, &tree_dir, &manifest_bytes)
            .and_then(|()| store.rename_entry_no_replace(&staging, &relative))
    };
    match published {
        Ok(true) => Ok(root.join(&relative)),
        Ok(false) => {
            discard_staging(&store, &staging);
            verify_view(&root, reference, version)?;
            Ok(root.join(&relative))
        }
        Err(error) => {
            discard_staging(&store, &staging);
            Err(error)
        }
    }
}

/// Stages one complete `<version>` subtree beneath `inside` from the shared
/// tree, manifest hardlink last.
///
/// Every directory comes from [`Dir::directory`]; every file, including the
/// manifest, is one internal hardlink of the already published shared-tree
/// entry, so the staged view shares the tree's canonical payload inodes and
/// no payload byte is duplicated.
fn stage_view(
    store: &Dir,
    inside: &Path,
    entries: &BTreeMap<String, serde_json::Value>,
    tree_dir: &Dir,
    manifest_bytes: &[u8],
) -> Result<()> {
    for (path, entry) in entries {
        let relative = Path::new(path.as_str());
        if entry["type"] == "directory" {
            store.directory(&inside.join(relative))?;
            continue;
        }
        if !store.hardlink(&inside.join(relative), tree_dir, relative)? {
            return Err(invalid("plugin view staging name already exists"));
        }
    }
    if tree_dir.read(Path::new(SNAPSHOT_MANIFEST), MAX_METADATA)? != manifest_bytes {
        return Err(invalid(
            "shared tree manifest changed while the view was staged",
        ));
    }
    if !store.hardlink(
        &inside.join(SNAPSHOT_MANIFEST),
        tree_dir,
        Path::new(SNAPSHOT_MANIFEST),
    )? {
        return Err(invalid("plugin view staging manifest name already exists"));
    }
    Ok(())
}

/// Removes one publisher-owned staging subtree through no-follow descriptors,
/// tolerating the readonly modes restriction already applied; a removal that
/// fails leaves the documented recoverable orphan in place.
fn discard_staging(store: &Dir, staging: &Path) {
    let Ok(directory) = store.subdir(staging) else {
        return;
    };
    if remove_view_tree(&directory).is_ok() {
        let _ = store.remove_directory(staging);
    }
}

/// Empties one owned readonly directory through live descriptors, leaving the
/// directory itself for the caller to remove; unexpected entry kinds abort.
fn remove_view_tree(directory: &Dir) -> Result<()> {
    directory.permit_owner_write()?;
    for name in directory.list(None)? {
        let relative = PathBuf::from(&name);
        match directory.entry_type(&relative)? {
            EntryType::Directory => {
                remove_view_tree(&directory.subdir(&relative)?)?;
                directory.remove_directory(&relative)?;
            }
            EntryType::File => directory.remove(&relative)?,
            kind => {
                return Err(invalid(format!(
                    "plugin view holds an unexpected entry: {kind:?}"
                )));
            }
        }
    }
    Ok(())
}

/// Verifies one plugin-parent view container against its reference, failing
/// closed on any drift.
///
/// Proves, through no-follow descriptors only: the container is a real
/// owner-held directory at a valid owner-only directory mode (`0o700`, or
/// legacy `0o500`) holding exactly one entry — the named
/// real version directory, also owner-held at a valid owner-only mode; the
/// hardlinked manifest bytes still hash to `reference.manifest_sha256` and
/// parse through the shared manifest validators; the version subtree's
/// topology is exactly the manifest's (no orphans, nothing missing, bounded
/// by [`MAX_VIEW_ENTRIES`]); every directory carries a valid owner-only
/// mode; and every file is one internal hardlink of the corresponding
/// `trees/<scope>/<manifest-sha256>` entry — same device and inode — at the
/// physical mode that entry's logical mode maps to. The referenced shared
/// tree itself is proven separately by
/// [`shared_assets::verify_shared_tree`], which this check deliberately does
/// not repeat.
pub fn verify_view(store_root: &Path, reference: &SharedTreeRef, version: &str) -> Result<()> {
    let root = shared_assets::validated_root(store_root)?;
    let identity = view_identity(reference, version)?;
    let tree = shared_assets::shared_tree_root(&root, reference)?;
    let store = Dir::open(&root)?;
    let relative = Path::new(VIEW_NAMESPACE)
        .join(&reference.scope)
        .join(&identity);
    match store.entry_type(&relative) {
        Ok(EntryType::Directory) => {}
        Err(Error::Io(error)) if error.kind() == std::io::ErrorKind::NotFound => {
            return Err(invalid("plugin view is missing from the store"));
        }
        Ok(_) => return Err(invalid("plugin view root is not a real directory")),
        Err(error) => return Err(error),
    }
    let container = store.entry(Some(&relative))?;
    require_owner(&container, "plugin view")?;
    check_directory_mode(&container, "plugin view")?;
    let view = store.subdir(&relative)?;
    let names = view.list(None)?;
    if names.len() != 1 || names[0].to_str() != Some(version) {
        return Err(invalid(
            "plugin view must hold exactly its named real version directory",
        ));
    }
    let version_entry = view.entry(Some(Path::new(version)))?;
    require_owner(&version_entry, "plugin view version")?;
    if version_entry.kind != EntryType::Directory {
        return Err(invalid(
            "plugin view version entry must be a real directory",
        ));
    }
    check_directory_mode(&version_entry, "plugin view version")?;
    let version_dir = view.subdir(Path::new(version))?;
    let manifest_bytes = version_dir.read(Path::new(SNAPSHOT_MANIFEST), MAX_METADATA)?;
    if fs::sha256(&manifest_bytes) != reference.manifest_sha256 {
        return Err(invalid("plugin view manifest does not match its reference"));
    }
    let entries = entry_map(
        &load_manifest(&version_dir)?.ok_or_else(|| invalid("plugin view manifest is missing"))?,
    )?;
    let tree_dir = Dir::open(&tree)?;
    let mut seen = BTreeSet::new();
    let mut visited = 0_usize;
    walk_view(
        &version_dir,
        "",
        &entries,
        &tree_dir,
        &mut seen,
        &mut visited,
    )?;
    if seen.len() != entries.len() {
        return Err(invalid(
            "plugin view is missing entries described by its manifest",
        ));
    }
    Ok(())
}

/// One recursion level of [`verify_view`]'s bounded walk over the version
/// subtree; `prefix` is the visited subtree's relative path text.
fn walk_view(
    directory: &Dir,
    prefix: &str,
    entries: &BTreeMap<String, serde_json::Value>,
    tree_dir: &Dir,
    seen: &mut BTreeSet<String>,
    visited: &mut usize,
) -> Result<()> {
    for name in directory.list(None)? {
        *visited += 1;
        if *visited > MAX_VIEW_ENTRIES {
            return Err(invalid("plugin view exceeds the topology entry bound"));
        }
        let Some(text) = name.to_str() else {
            return Err(invalid("plugin view holds a non-UTF-8 entry name"));
        };
        let path = if prefix.is_empty() {
            text.to_owned()
        } else {
            format!("{prefix}/{text}")
        };
        let identity = directory.entry(Some(Path::new(text)))?;
        require_owner(&identity, "plugin view entry")?;
        if path == SNAPSHOT_MANIFEST {
            if identity.kind != EntryType::File {
                return Err(invalid("plugin view manifest must be a regular file"));
            }
            continue;
        }
        let entry = entries.get(&path).ok_or_else(|| {
            invalid(format!(
                "plugin view holds an entry its manifest does not describe: {path}"
            ))
        })?;
        seen.insert(path.clone());
        match entry["type"].as_str() {
            Some("directory") => {
                if identity.kind != EntryType::Directory {
                    return Err(invalid(format!(
                        "plugin view entry is not a real directory: {path}"
                    )));
                }
                check_directory_mode(&identity, "plugin view directory")?;
                walk_view(
                    &directory.subdir(Path::new(text))?,
                    &path,
                    entries,
                    tree_dir,
                    seen,
                    visited,
                )?;
            }
            Some("file") => {
                if identity.kind != EntryType::File {
                    return Err(invalid(format!(
                        "plugin view entry is not a regular file: {path}"
                    )));
                }
                let mode = entry
                    .get("mode")
                    .and_then(serde_json::Value::as_u64)
                    .unwrap_or(0);
                if !matches!(mode, 0o600 | 0o700) {
                    return Err(invalid("plugin view manifest mode is not normalized"));
                }
                check_mode(&identity, physical_mode(mode as u32), "plugin view file")?;
                let tree_file = tree_dir.open_file(Path::new(&path))?;
                let tree_metadata = tree_file.metadata()?;
                if tree_metadata.dev() != identity.device || tree_metadata.ino() != identity.inode {
                    return Err(invalid(format!(
                        "plugin view file is not the shared tree payload hardlink: {path}"
                    )));
                }
            }
            _ => return Err(invalid("plugin view manifest entry type is invalid")),
        }
    }
    Ok(())
}

/// Returns the readonly physical store mode for one normalized logical mode.
fn physical_mode(logical: u32) -> u32 {
    if logical & 0o111 != 0 {
        0o500
    } else {
        0o400
    }
}

/// Fails unless one owner-held identity carries exactly `mode`'s permission
/// bits.
fn check_mode(identity: &fs::Entry, mode: u32, label: &str) -> Result<()> {
    if identity.mode != mode {
        return Err(invalid(format!("shared store {label} mode drifted")));
    }
    Ok(())
}

/// Fails unless one owner-held directory identity carries a valid shared
/// directory mode: the portable publication mode `0o700` or the legacy
/// readonly `0o500` containers earlier releases published, exactly as
/// [`shared_assets::is_shared_directory_mode`] defines them.
fn check_directory_mode(identity: &fs::Entry, label: &str) -> Result<()> {
    if !shared_assets::is_shared_directory_mode(identity.mode) {
        return Err(invalid(format!("shared store {label} mode drifted")));
    }
    Ok(())
}

/// Fails when one store object is not owned by the effective user.
fn require_owner(identity: &fs::Entry, label: &str) -> Result<()> {
    // SAFETY: geteuid only reads kernel credential state and retains nothing.
    if identity.uid != unsafe { libc::geteuid() } {
        return Err(invalid(format!("shared store {label} has a foreign owner")));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::shared_assets::import_shared_tree;
    use crate::snapshot_tree::snapshot_managed_tree;
    use std::fs;
    use std::os::unix::fs::MetadataExt;

    /// Seals `files` into a fresh managed tree and imports it, returning the
    /// store root and the resulting shared-tree reference.
    fn imported(
        files: &[(&'static str, &'static [u8], bool)],
    ) -> (tempfile::TempDir, PathBuf, SharedTreeRef) {
        let store = tempfile::tempdir().unwrap();
        let root = store.path().canonicalize().unwrap();
        let source = tempfile::tempdir().unwrap();
        for (path, bytes, executable) in files {
            let target = source.path().join(path);
            fs::create_dir_all(target.parent().unwrap()).unwrap();
            fs::write(&target, bytes).unwrap();
            use std::os::unix::fs::PermissionsExt;
            fs::set_permissions(
                &target,
                fs::Permissions::from_mode(if *executable { 0o700 } else { 0o600 }),
            )
            .unwrap();
        }
        let home = tempfile::tempdir().unwrap();
        snapshot_managed_tree(
            home.path(),
            Path::new("plugins/cache/personal/probe/1.0.0"),
            source.path(),
            None,
        )
        .unwrap();
        let reference = import_shared_tree(
            &root,
            &crate::fs::sha256(b"view-scope"),
            home.path(),
            Path::new("plugins/cache/personal/probe/1.0.0"),
        )
        .unwrap();
        (store, root, reference)
    }

    /// Only the exact managed plugin root shape classifies as a parent mount.
    #[test]
    fn classifies_managed_codex_plugin_roots_only() {
        assert_eq!(
            plugin_mount("plugins/cache/personal/ponytail/4.10.0"),
            Some(("plugins/cache/personal/ponytail".into(), "4.10.0".into()))
        );
        assert_eq!(plugin_mount("skills/demo"), None);
        assert_eq!(plugin_mount("plugins/cache/remote/pkg/1"), None);
        assert_eq!(plugin_mount("plugins/cache/personal/ponytail"), None);
        assert_eq!(
            plugin_mount("plugins/cache/personal/ponytail/4.10.0/hooks"),
            None
        );
        assert_eq!(plugin_mount("plugins/cache/personal/pon y/4.10.0"), None);
        assert_eq!(plugin_mount("plugins/cache/personal/ponytail/.."), None);
    }

    /// The view identity binds scope, manifest digest, and version only.
    #[test]
    fn identity_binds_reference_and_version() {
        let reference = SharedTreeRef {
            scope: crate::fs::sha256(b"scope-a"),
            manifest_sha256: crate::fs::sha256(b"manifest-a"),
        };
        let base = view_identity(&reference, "1.0.0").unwrap();
        assert_eq!(base, view_identity(&reference, "1.0.0").unwrap());
        assert_ne!(base, view_identity(&reference, "2.0.0").unwrap());
        assert_ne!(
            base,
            view_identity(
                &SharedTreeRef {
                    scope: crate::fs::sha256(b"scope-b"),
                    manifest_sha256: reference.manifest_sha256.clone(),
                },
                "1.0.0"
            )
            .unwrap()
        );
        // The exact preimage is part of the collector-facing contract.
        assert_eq!(
            base,
            crate::fs::sha256(
                format!(
                    "plugin-view-v1\n{}\n{}\n1.0.0",
                    reference.scope, reference.manifest_sha256
                )
                .as_bytes()
            )
        );
        assert!(view_identity(&reference, "not safe").is_err());
        assert!(view_identity(&reference, "..").is_err());
    }

    /// Materialization publishes one real readonly version subtree whose
    /// files are the shared tree's own inodes, and is idempotent.
    #[test]
    fn materialize_publishes_real_hardlinked_version_dir() {
        let (_store, root, reference) = imported(&[
            ("SKILL.md", b"# probe skill\n", false),
            ("hooks/run.sh", b"#!/bin/sh\n", true),
        ]);
        let scope = reference.scope.clone();
        let view = materialize(&root, &reference, "1.0.0").unwrap();
        assert_eq!(
            view,
            root.join(VIEW_NAMESPACE)
                .join(&scope)
                .join(view_identity(&reference, "1.0.0").unwrap())
        );
        let version = view.join("1.0.0");
        assert!(fs::symlink_metadata(&version).unwrap().is_dir());
        let tree = shared_assets::shared_tree_root(&root, &reference).unwrap();
        assert_eq!(
            fs::metadata(version.join("SKILL.md")).unwrap().ino(),
            fs::metadata(tree.join("SKILL.md")).unwrap().ino()
        );
        assert_eq!(
            fs::metadata(version.join("hooks/run.sh")).unwrap().ino(),
            fs::metadata(tree.join("hooks/run.sh")).unwrap().ino()
        );
        verify_view(&root, &reference, "1.0.0").unwrap();
        assert_eq!(materialize(&root, &reference, "1.0.0").unwrap(), view);
        verify_view(&root, &reference, "1.0.0").unwrap();
    }

    /// Verification refuses a missing view, a foreign sibling, an orphan, a
    /// mode drift, and — through the shared tree it hardlinks — a rewritten
    /// payload, without repairing anything.
    #[test]
    fn verification_rejects_drift() {
        use std::os::unix::fs::PermissionsExt;
        let (_store, root, reference) = imported(&[("SKILL.md", b"# probe skill\n", false)]);
        assert!(verify_view(&root, &reference, "1.0.0").is_err());
        let view = materialize(&root, &reference, "1.0.0").unwrap();
        let permit = |path: &Path, mode: u32| {
            fs::set_permissions(path, fs::Permissions::from_mode(mode)).unwrap();
        };
        permit(&view.join("1.0.0"), 0o700);
        fs::write(view.join("1.0.0/orphan.txt"), b"orphan\n").unwrap();
        assert!(verify_view(&root, &reference, "1.0.0").is_err());
        fs::remove_file(view.join("1.0.0/orphan.txt")).unwrap();
        permit(&view, 0o700);
        fs::create_dir(view.join("extra")).unwrap();
        assert!(verify_view(&root, &reference, "1.0.0").is_err());
        fs::remove_dir(view.join("extra")).unwrap();
        permit(&view, 0o500);
        permit(&view.join("1.0.0/SKILL.md"), 0o600);
        assert!(verify_view(&root, &reference, "1.0.0").is_err());
        fs::write(view.join("1.0.0/SKILL.md"), b"rewritten\n").unwrap();
        permit(&view.join("1.0.0/SKILL.md"), 0o400);
        // One inode with the tree means the shared payload itself drifted:
        // the physical tree verifier must refuse it even though every view
        // link still points at the same object.
        assert!(shared_assets::verify_shared_tree(&root, &reference).is_err());
        permit(&view.join("1.0.0/SKILL.md"), 0o600);
        fs::write(view.join("1.0.0/SKILL.md"), b"# probe skill\n").unwrap();
        permit(&view.join("1.0.0/SKILL.md"), 0o400);
        permit(&view.join("1.0.0"), 0o500);
        verify_view(&root, &reference, "1.0.0").unwrap();
        shared_assets::verify_shared_tree(&root, &reference).unwrap();
    }

    /// A Node fixture resolves relative module imports from the real version
    /// subtree a mounted parent exposes, when a local `node` exists.
    #[test]
    fn node_resolves_relative_modules_from_view_version_dir() {
        if std::process::Command::new("node")
            .arg("--version")
            .output()
            .is_err()
        {
            eprintln!("skipping: node is unavailable");
            return;
        }
        let (_store, root, reference) = imported(&[
            (
                "index.js",
                b"const util = require('./lib/util.js'); process.stdout.write(util.marker);\n",
                true,
            ),
            ("lib/util.js", b"exports.marker = 'view-ok';\n", false),
        ]);
        let view = materialize(&root, &reference, "1.0.0").unwrap();
        let output = std::process::Command::new("node")
            .arg("index.js")
            .current_dir(view.join("1.0.0"))
            .output()
            .unwrap();
        assert!(output.status.success(), "{output:?}");
        assert_eq!(String::from_utf8_lossy(&output.stdout), "view-ok");
    }
}
