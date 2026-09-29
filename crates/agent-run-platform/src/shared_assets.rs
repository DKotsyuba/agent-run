//! Immutable content-addressed storage for sealed managed snapshot trees.
//!
//! This module imports one already sealed managed
//! [`crate::snapshot_tree`] tree into a caller-owned shared store and returns
//! a small [`SharedTreeRef`] naming it. The store root is a canonical,
//! owner-controlled, real directory; namespace v1 is:
//!
//! - `blobs/<scope>/<content-sha256>-<normalized-mode>`: payload bytes stored
//!   once per scope, content digest, and normalized logical mode (`0o600` or
//!   `0o700`), at physical mode `0o400`/`0o500`.
//! - `trees/<scope>/<original-manifest-sha256>/`: the original tree layout,
//!   including the unchanged `.agent-run-snapshot.json` bytes, with every
//!   file an internal hardlink to its canonical blob, every directory at
//!   mode `0o500`, and the manifest at mode `0o400`.
//!
//! `<scope>` is a caller-provided 64-character lowercase hexadecimal digest
//! separating account or compatibility domains; payloads never alias across
//! scopes or execution modes. Source trees are verified with a bounded
//! streaming walker (never buffering a whole tree), publication is atomic
//! and no-replace at every layer, and an existing content-addressed object
//! is verified and reused, never overwritten. Staging directories
//! (`.agent-run-staging-*.tmp`) are the only recoverable orphans an
//! interrupted publisher can leave behind; garbage collection and reference
//! tracking are later units.

use crate::{
    fs::{sha256, Dir, EntryType},
    snapshot_tree::{entry_map, load_manifest, MAX_METADATA, SNAPSHOT_MANIFEST},
};
use agent_run_domain::{canonical::hex_digest, error::invalid, Error, Result};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::{
    collections::{BTreeMap, BTreeSet},
    io::{Read, Write},
    os::unix::fs::{MetadataExt, PermissionsExt},
    path::{Path, PathBuf},
};

/// Per-file payload bound, matching `snapshot_tree`'s capture limit.
const MAX_FILE_BYTES: usize = 16 * 1024 * 1024;
/// Maximum manifest entries one shared tree may describe, also the walker's
/// visited-entry bound so unexpected orphan growth cannot make a scan run
/// away (plus one for the manifest itself).
const MAX_TREE_ENTRIES: usize = 4096;
/// Maximum aggregate payload bytes one shared tree may describe.
const MAX_TREE_BYTES: u64 = 256 * 1024 * 1024;
/// Streaming chunk size for hashing and copying payloads.
const STREAM_CHUNK: usize = 64 * 1024;
/// Prefix naming publisher-owned staging directories beneath `trees/<scope>`
/// and temporary blobs beneath `blobs/<scope>`.
const TEMP_PREFIX: &str = ".agent-run-staging-";
/// Suffix matching the publisher-owned temporary convention of `snapshot_tree`.
const TEMP_SUFFIX: &str = ".tmp";
/// Name of the store-wide publish/GC lock file at the store root.
const LOCK_NAME: &str = ".publish.lock";

/// Strict shared-store digest shape: exactly 64 lowercase hexadecimal digits.
/// Scope and manifest identities both use it, so a reference can never carry
/// an uppercase or malformed digest into a derived store path.
fn is_digest(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| matches!(byte, b'0'..=b'9' | b'a'..=b'f'))
}

/// Returns whether `value` is a valid shared-store scope: a strict 64
/// lowercase hexadecimal digit digest, the shape callers derive their
/// account or compatibility domain identities from.
pub fn is_scope(value: &str) -> bool {
    is_digest(value)
}

/// Serializable identity of one shared managed snapshot tree.
///
/// `scope` names the account/compatibility domain the tree was imported
/// under; `manifest_sha256` is the SHA-256 of the tree's exact original
/// canonical manifest bytes. Both fields are validated as strict digests by
/// [`verify_shared_tree`] before any target is derived, and the store path is
/// always derived from a trusted store root plus this validated pair, never
/// from caller-supplied paths. Later consumers persist this value in their
/// versioned physical-layout receipts; unknown fields are rejected on
/// deserialization so a receipt cannot smuggle extra state.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SharedTreeRef {
    /// 64-lowercase-hex scope the tree was imported under.
    pub scope: String,
    /// SHA-256 of the tree's original canonical manifest bytes.
    pub manifest_sha256: String,
}

/// Derives the absolute directory of one shared tree from `store_root` and a
/// validated `reference`, without touching the filesystem.
///
/// The caller must still pass the result through [`verify_shared_tree`] (or
/// hold the publishing lock) before linking session homes against it. No
/// component of the result comes from unvalidated reference text: both fields
/// are re-validated here so a malformed reference cannot steer traversal.
pub fn shared_tree_root(store_root: &Path, reference: &SharedTreeRef) -> Result<PathBuf> {
    if !is_digest(&reference.scope) || !is_digest(&reference.manifest_sha256) {
        return Err(invalid("shared tree reference is invalid"));
    }
    Ok(store_root
        .join(trees_scope(&reference.scope))
        .join(&reference.manifest_sha256))
}

/// Store-wide publish/GC lock serializing shared-store mutations.
///
/// The guard holds one exclusive advisory lock on
/// `<store_root>/.publish.lock`, created owner-only with `O_NOFOLLOW`, for
/// the lifetime of the value. Publication holds it only for the short window
/// that stages and renames objects; a later garbage collector acquires the
/// same lock through [`SharedStoreLock::acquire`] so it never unlinks an
/// object a concurrent publisher is still installing. The lock is advisory:
/// it orders cooperating agent-run processes, it cannot defend the store
/// against an unrelated same-UID writer.
pub struct SharedStoreLock {
    /// Open descriptor owning the advisory lock; closed on drop, releasing it.
    file: std::fs::File,
    /// Absolute path of the lock file, retained for diagnostics.
    lock_path: PathBuf,
}

impl SharedStoreLock {
    /// Blocks until this process owns the shared store's publish lock.
    ///
    /// `store_root` must already exist as the canonical owner-controlled real
    /// directory this module validates. Ordinary contention waits; the call
    /// fails only when the lock file cannot be opened (including when it is a
    /// symbolic link) or locked. The guard releases the lock on drop.
    pub fn acquire(store_root: &Path) -> Result<Self> {
        use std::os::unix::fs::OpenOptionsExt;
        let lock_path = store_root.join(LOCK_NAME);
        let file = std::fs::OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(false)
            .mode(0o600)
            .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC)
            .open(&lock_path)?;
        file.lock().map_err(Error::from)?;
        Ok(Self { file, lock_path })
    }

    /// Absolute path of the lock file this guard holds.
    pub fn path(&self) -> &Path {
        &self.lock_path
    }
}

impl Drop for SharedStoreLock {
    /// Releases the advisory lock; the descriptor closes right after.
    fn drop(&mut self) {
        let _ = self.file.unlock();
    }
}

/// Validates the store root: an existing absolute real directory whose
/// canonical form equals the given path, so no symlinked component can alias
/// the trusted root. Returns the validated path for subsequent descriptor
/// opens.
fn validated_root(store_root: &Path) -> Result<PathBuf> {
    if !store_root.is_absolute() {
        return Err(invalid("shared store root must be absolute"));
    }
    let canonical = store_root
        .canonicalize()
        .map_err(|_| invalid("shared store root must be an existing real directory"))?;
    if canonical != store_root {
        return Err(invalid(
            "shared store root must be canonical without symlinked components",
        ));
    }
    Ok(canonical)
}

/// Returns the `blobs/<scope>` container path relative to the store root.
fn blobs_scope(scope: &str) -> PathBuf {
    PathBuf::from("blobs").join(scope)
}

/// Returns the `trees/<scope>` container path relative to the store root.
fn trees_scope(scope: &str) -> PathBuf {
    PathBuf::from("trees").join(scope)
}

/// Returns the content-addressed tree path `trees/<scope>/<manifest sha>`.
fn tree_rel(reference: &SharedTreeRef) -> PathBuf {
    trees_scope(&reference.scope).join(&reference.manifest_sha256)
}

/// Returns the blob path for one payload digest and normalized logical mode.
fn blob_rel(scope: &str, sha256: &str, logical: u32) -> PathBuf {
    blobs_scope(scope).join(format!("{sha256}-{logical:o}"))
}

/// Returns the physical store mode for one normalized logical mode: readonly
/// `0o400` for data, readonly-executable `0o500` for executables. The
/// manifest's logical modes stay `0o600`/`0o700`; only the stored files are
/// restricted, and restoring owner write later requires the verified native
/// launch guard.
fn physical_mode(logical: u32) -> u32 {
    if logical & 0o111 != 0 {
        0o500
    } else {
        0o400
    }
}

/// Validates one manifest file entry and returns its `(sha256, logical mode,
/// size)` triple. Rejects malformed or non-lowercase digests, modes outside
/// the normalized `0o600`/`0o700` pair, and sizes beyond the payload bound,
/// so a malformed manifest can never steer a store path.
fn file_identity(entry: &Value) -> Result<(String, u32, u64)> {
    let sha256 = entry.get("sha256").and_then(Value::as_str).unwrap_or("");
    if !is_digest(sha256) {
        return Err(invalid("shared tree manifest digest is invalid"));
    }
    let mode = entry.get("mode").and_then(Value::as_u64).unwrap_or(0);
    if !matches!(mode, 0o600 | 0o700) {
        return Err(invalid("shared tree manifest mode is not normalized"));
    }
    let bytes = entry.get("bytes").and_then(Value::as_u64).unwrap_or(u64::MAX);
    if bytes > MAX_FILE_BYTES as u64 {
        return Err(invalid("shared tree file exceeds the payload bound"));
    }
    Ok((sha256.to_owned(), mode as u32, bytes))
}

/// Validates the parsed manifest shape and returns the aggregate payload
/// bytes, refusing trees above the entry or total-size bounds before any
/// filesystem work. The manifest's own 64 KiB parse bound keeps this check
/// cheap.
fn validate_entries(entries: &BTreeMap<String, Value>) -> Result<u64> {
    if entries.len() > MAX_TREE_ENTRIES {
        return Err(invalid("shared tree exceeds the manifest entry bound"));
    }
    let mut total = 0_u64;
    for entry in entries.values() {
        if entry["type"] == "file" {
            total += file_identity(entry)?.2;
        }
    }
    if total > MAX_TREE_BYTES {
        return Err(invalid("shared tree exceeds the aggregate payload bound"));
    }
    Ok(total)
}

/// Fails when one store or source object is not owned by the effective user.
fn require_owner(uid: u32, label: &str) -> Result<()> {
    // SAFETY: geteuid only reads kernel credential state and retains nothing.
    if uid != unsafe { libc::geteuid() } {
        return Err(invalid(format!("shared store {label} has a foreign owner")));
    }
    Ok(())
}

/// Which contract a bounded topology walk checks file modes against.
#[derive(Clone, Copy, PartialEq, Eq)]
enum TreeShape {
    /// A source managed snapshot: files at `0o600`/`0o700`, no mode assertion
    /// on directories (source homes own their own modes).
    Source,
    /// A published shared tree: files at physical `0o400`/`0o500` and every
    /// directory at `0o500`.
    Shared,
}

/// Verifies one tree's bounded topology against validated manifest entries
/// without buffering any payload.
///
/// Walks only through no-follow descriptors. Every visited entry must be
/// described by the manifest (rejecting orphans, foreign additions, and
/// symlinks by kind), owned by the effective user, and carry the mode the
/// `shape` contract promises; the walk refuses after [`MAX_TREE_ENTRIES`]
/// visited entries so unexpected orphan growth cannot make it run away. The
/// manifest file itself is accepted as the one extra regular file. File
/// content digests are verified separately by streaming (import stages from
/// one captured descriptor; [`verify_shared_tree`] checks blobs and proves
/// tree files are hardlinks to them), so nothing here buffers a payload.
fn verify_topology(
    root: &Dir,
    entries: &BTreeMap<String, Value>,
    shape: TreeShape,
    label: &str,
) -> Result<()> {
    require_owner(root.entry(None)?.uid, label)?;
    if shape == TreeShape::Shared && root.entry(None)?.mode != 0o500 {
        return Err(invalid(format!("shared tree {label} directory mode drifted")));
    }
    let mut visited = 0_usize;
    let mut seen = BTreeSet::new();
    walk_topology(root, "", entries, shape, label, &mut visited, &mut seen)?;
    if seen.len() != entries.len() {
        return Err(invalid(format!(
            "{label} is missing entries described by its manifest"
        )));
    }
    Ok(())
}

/// One recursion level of [`verify_topology`]; `prefix` is the visited
/// subtree's relative path text and `visited`/`seen` accumulate the bounded
/// walk state.
fn walk_topology(
    directory: &Dir,
    prefix: &str,
    entries: &BTreeMap<String, Value>,
    shape: TreeShape,
    label: &str,
    visited: &mut usize,
    seen: &mut BTreeSet<String>,
) -> Result<()> {
    for name in directory.list(None)? {
        *visited += 1;
        if *visited > MAX_TREE_ENTRIES + 1 {
            return Err(invalid("tree exceeds the topology entry bound"));
        }
        let Some(text) = name.to_str() else {
            return Err(invalid("tree holds a non-UTF-8 entry name"));
        };
        let path = if prefix.is_empty() {
            text.to_owned()
        } else {
            format!("{prefix}/{text}")
        };
        let relative = PathBuf::from(text);
        let identity = directory.entry(Some(&relative))?;
        require_owner(identity.uid, &format!("{label} entry"))?;
        if path == SNAPSHOT_MANIFEST {
            if identity.kind != EntryType::File {
                return Err(invalid("tree manifest must be a regular file"));
            }
            continue;
        }
        let entry = entries.get(&path).ok_or_else(|| {
            invalid(format!("{label} holds an entry its manifest does not describe: {path}"))
        })?;
        seen.insert(path.clone());
        match entry["type"].as_str() {
            Some("directory") => {
                if identity.kind != EntryType::Directory {
                    return Err(invalid(format!("{label} entry is not a real directory: {path}")));
                }
                if shape == TreeShape::Shared && identity.mode != 0o500 {
                    return Err(invalid(format!(
                        "shared tree directory mode drifted: {path}"
                    )));
                }
                walk_topology(
                    &directory.subdir(&relative)?,
                    &path,
                    entries,
                    shape,
                    label,
                    visited,
                    seen,
                )?;
            }
            Some("file") => {
                if identity.kind != EntryType::File {
                    return Err(invalid(format!(
                        "{label} entry is not a regular file: {path}"
                    )));
                }
                let (_, logical, _) = file_identity(entry)?;
                let expected = match shape {
                    TreeShape::Source => logical,
                    TreeShape::Shared => physical_mode(logical),
                };
                if identity.mode != expected {
                    return Err(invalid(format!("{label} file mode drifted: {path}")));
                }
            }
            _ => return Err(invalid("shared tree manifest entry type is invalid")),
        }
    }
    Ok(())
}

/// Streams one open regular file and returns its lowercase hex SHA-256 and
/// exact length, reading at most [`MAX_FILE_BYTES`].
fn hash_open_file(file: &mut std::fs::File) -> Result<(String, u64)> {
    let mut hasher = Sha256::new();
    let mut buffer = vec![0_u8; STREAM_CHUNK];
    let mut length = 0_u64;
    loop {
        let read = file.read(&mut buffer)?;
        if read == 0 {
            return Ok((hex_digest(&hasher.finalize()), length));
        }
        hasher.update(&buffer[..read]);
        length += read as u64;
        if length > MAX_FILE_BYTES as u64 {
            return Err(invalid("shared tree file exceeds the payload bound"));
        }
    }
}

/// Checks one freshly opened source file's owner, size, and execution bit
/// against the manifest expectations before any byte is copied. This is the
/// captured-descriptor comparison that closes the race between the topology
/// walk and payload staging: the metadata read belongs to the exact inode
/// whose bytes are then streamed.
fn check_source_file(
    file: &std::fs::File,
    expected_len: u64,
    logical: u32,
) -> Result<()> {
    let metadata = file.metadata()?;
    require_owner(metadata.uid(), "source file")?;
    if metadata.len() != expected_len {
        return Err(invalid("managed snapshot changed while it was shared"));
    }
    let executable = metadata.permissions().mode() & 0o111 != 0;
    if logical != if executable { 0o700 } else { 0o600 } {
        return Err(invalid("managed snapshot file mode drifted"));
    }
    Ok(())
}

/// Imports one sealed managed snapshot tree into the shared store.
///
/// `source_home` plus `relative_root` must name an existing managed snapshot
/// produced by [`crate::snapshot_tree::snapshot_managed_tree`]. The source is
/// verified first from its exact manifest bytes: entries are parsed with the
/// same validator [`crate::snapshot_tree::inspect_managed_snapshot`] uses,
/// bounded by the entry and aggregate-size limits, then a bounded streaming
/// walker proves the topology, owners, and modes match with no symlinked or
/// orphan entries — without buffering the tree. The source keeps its own
/// independent inodes and is never linked into the store.
///
/// `scope` must be a 64-lowercase-hex digest ([`is_scope`]). The tree's
/// original manifest bytes, hash, and path topology are preserved exactly;
/// each payload is copied once per scope/content/mode into a fresh temporary
/// blob, streamed through a running hash compared against the manifest
/// digest, fsynced, and atomically renamed into its content-addressed name
/// with the kernel's no-replace rename — so an existing blob is verified and
/// reused, never overwritten, and the tree's files are internal hardlinks to
/// that canonical blob. Publication holds [`SharedStoreLock`] only for the
/// staging window and renames the finished staging directory into place with
/// the same no-replace guarantee.
///
/// An interrupted publisher leaves at most one recoverable
/// `.agent-run-staging-*.tmp` orphan directory beneath `trees/<scope>`; this
/// unit performs no garbage collection. Returns the [`SharedTreeRef`] naming
/// the tree; on error no partial tree exists under its final name (blobs are
/// content-addressed and safe to leave for reuse or a later collector).
pub fn import_shared_tree(
    store_root: &Path,
    scope: &str,
    source_home: &Path,
    relative_root: &Path,
) -> Result<SharedTreeRef> {
    if !is_scope(scope) {
        return Err(invalid(
            "shared store scope must be 64 lowercase hexadecimal digits",
        ));
    }
    if !source_home.is_absolute() {
        return Err(invalid("managed snapshot home must be an absolute path"));
    }
    let source_dir = Dir::open(&source_home.join(relative_root))?;
    let manifest_bytes = source_dir.read(Path::new(SNAPSHOT_MANIFEST), MAX_METADATA)?;
    let reference = SharedTreeRef {
        scope: scope.to_owned(),
        manifest_sha256: sha256(&manifest_bytes),
    };
    let entries = entry_map(
        &load_manifest(&source_dir)?.ok_or_else(|| invalid("managed snapshot manifest is missing"))?,
    )?;
    validate_entries(&entries)?;
    verify_topology(&source_dir, &entries, TreeShape::Source, "managed snapshot")?;
    let root = validated_root(store_root)?;
    let _guard = SharedStoreLock::acquire(&root)?;
    let store = Dir::open(&root)?;
    require_owner(store.entry(None)?.uid, "root")?;
    store.directory(&blobs_scope(scope))?;
    store.directory(&trees_scope(scope))?;
    let destination = tree_rel(&reference);
    match store.entry_type(&destination) {
        Ok(EntryType::Directory) => {
            verify_shared_tree(&root, &reference)?;
            return Ok(reference);
        }
        Ok(_) => {
            return Err(invalid(
                "shared tree destination exists and is not a real directory",
            ))
        }
        Err(Error::Io(error)) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => return Err(error),
    }
    let staging = trees_scope(scope).join(format!(
        "{TEMP_PREFIX}{}{TEMP_SUFFIX}",
        uuid::Uuid::new_v4().simple()
    ));
    store.directory(&staging)?;
    let staged = stage_shared_tree(&store, &staging, scope, &entries, &source_dir, &manifest_bytes);
    let published = staged.and_then(|()| restrict_staging(&store, &staging, &entries))
        .and_then(|()| store.rename_entry_no_replace(&staging, &destination));
    match published {
        Ok(true) => Ok(reference),
        Ok(false) => {
            // An object already occupies the content-addressed destination —
            // most often a concurrent publisher of the identical tree.
            discard_staging(&store, &staging);
            verify_shared_tree(&root, &reference)?;
            Ok(reference)
        }
        Err(error) => {
            discard_staging(&store, &staging);
            Err(error)
        }
    }
}

/// Stages one complete shared tree beneath `staging`, manifest bytes last.
///
/// Each file is opened once from `source_dir` through a no-follow
/// descriptor, checked against the manifest, streamed into a fresh temporary
/// blob under a running hash, and published with a no-replace rename; the
/// staged tree file is then an internal hardlink to that blob. A source that
/// changed since its capture is refused by the hash comparison. Nothing
/// outside `blobs/<scope>`, `trees/<scope>`, and `staging` is touched.
fn stage_shared_tree(
    store: &Dir,
    staging: &Path,
    scope: &str,
    entries: &BTreeMap<String, Value>,
    source_dir: &Dir,
    manifest_bytes: &[u8],
) -> Result<()> {
    for (path, entry) in entries {
        let relative = Path::new(path.as_str());
        if entry["type"] == "directory" {
            store.directory(&staging.join(relative))?;
            continue;
        }
        let (sha256, logical, expected_len) = file_identity(entry)?;
        let mut source_file = source_dir.open_file(relative)?;
        check_source_file(&source_file, expected_len, logical)?;
        let blob = blob_rel(scope, &sha256, logical);
        ensure_blob(store, &blob, &mut source_file, &sha256, expected_len, logical)?;
        if !store.hardlink(&staging.join(relative), store, &blob)? {
            return Err(invalid("shared tree staging name already exists"));
        }
    }
    store.write(&staging.join(SNAPSHOT_MANIFEST), manifest_bytes, 0o400)
}

/// Publishes the canonical payload for one file unless a verified blob
/// already exists, and never overwrites an existing object.
///
/// When the blob already exists it is re-verified by streaming against the
/// manifest digest, size, owner, and physical mode and then reused — the
/// source descriptor's metadata was already checked, and its bytes are not
/// copied on this path. Otherwise [`publish_blob`] streams the captured
/// source descriptor into a fresh temporary blob and renames it into place
/// with the kernel's no-replace rename.
fn ensure_blob(
    store: &Dir,
    blob: &Path,
    source: &mut std::fs::File,
    sha256: &str,
    expected_len: u64,
    logical: u32,
) -> Result<()> {
    match store.entry_type(blob) {
        Err(Error::Io(error)) if error.kind() == std::io::ErrorKind::NotFound => {
            publish_blob(store, blob, source, sha256, expected_len, logical)
        }
        Ok(EntryType::File) => verify_blob(store, blob, sha256, expected_len, logical),
        Ok(_) => Err(invalid("shared store blob exists with an unexpected type")),
        Err(error) => Err(error),
    }
}

/// Streams one captured source descriptor into a fresh temporary blob and
/// publishes it with a no-replace rename.
///
/// The bytes are hashed while they are copied and compared against the
/// manifest digest and size, so a source mutated mid-copy is refused before
/// publication. The temporary file is created exclusively at its readonly
/// physical mode, fsynced, then renamed into the content-addressed name
/// without replacing anything; if a concurrent publisher won that name, this
/// temporary is removed and the existing blob is verified and reused.
fn publish_blob(
    store: &Dir,
    blob: &Path,
    source: &mut std::fs::File,
    sha256: &str,
    expected_len: u64,
    logical: u32,
) -> Result<()> {
    let temporary = blob
        .parent()
        .expect("a blob always has a scope parent")
        .join(format!("{TEMP_PREFIX}{}{TEMP_SUFFIX}", uuid::Uuid::new_v4().simple()));
    let staged = stream_into_blob(store, &temporary, source, sha256, expected_len, logical)
        .and_then(|()| store.rename_entry_no_replace(&temporary, blob));
    // Removing the exclusive temporary this call created (or nothing when
    // creation itself failed) never touches another object.
    let discard = |temporary: &Path| {
        if store.entry_type(temporary).is_ok() {
            let _ = store.remove(temporary);
        }
    };
    match staged {
        Ok(true) => Ok(()),
        Ok(false) => {
            discard(&temporary);
            verify_blob(store, blob, sha256, expected_len, logical)
        }
        Err(error) => {
            discard(&temporary);
            Err(error)
        }
    }
}

/// Copies `source` into the exclusive `temporary` name, hashing as it goes.
///
/// Fails (leaving the temporary in place for [`publish_blob`]'s cleanup)
/// when the streamed bytes exceed the payload bound or their digest and
/// length do not match the manifest expectations.
fn stream_into_blob(
    store: &Dir,
    temporary: &Path,
    source: &mut std::fs::File,
    sha256: &str,
    expected_len: u64,
    logical: u32,
) -> Result<()> {
    let mut file = store.create_exclusive(temporary, physical_mode(logical))?;
    let written = (|| -> Result<(String, u64)> {
        let mut hasher = Sha256::new();
        let mut buffer = vec![0_u8; STREAM_CHUNK];
        let mut length = 0_u64;
        loop {
            let read = source.read(&mut buffer)?;
            if read == 0 {
                break;
            }
            file.write_all(&buffer[..read])?;
            hasher.update(&buffer[..read]);
            length += read as u64;
            if length > MAX_FILE_BYTES as u64 {
                return Err(invalid("shared tree file exceeds the payload bound"));
            }
        }
        Ok((hex_digest(&hasher.finalize()), length))
    })();
    let (digest, length) = written?;
    file.sync_all()?;
    if digest != sha256 || length != expected_len {
        return Err(invalid("managed snapshot changed while it was shared"));
    }
    Ok(())
}

/// Re-verifies one published blob against the manifest digest, size, owner,
/// and physical mode by streaming, failing on any drift.
fn verify_blob(
    store: &Dir,
    blob: &Path,
    sha256: &str,
    expected_len: u64,
    logical: u32,
) -> Result<()> {
    let mut file = store.open_file(blob)?;
    let metadata = file.metadata()?;
    require_owner(metadata.uid(), "blob")?;
    if metadata.permissions().mode() & 0o7777 != physical_mode(logical) {
        return Err(invalid("shared store blob mode drifted"));
    }
    let (digest, length) = hash_open_file(&mut file)?;
    if digest != sha256 || length != expected_len {
        return Err(invalid("shared store blob content drifted"));
    }
    Ok(())
}

/// Drops owner write permission on every staged directory, including the
/// staging root, leaving the tree readonly at `0o500` before its rename.
fn restrict_staging(
    store: &Dir,
    staging: &Path,
    entries: &BTreeMap<String, Value>,
) -> Result<()> {
    for (path, entry) in entries {
        if entry["type"] == "directory" {
            store
                .subdir(&staging.join(path.as_str()))?
                .restrict_owner_read()?;
        }
    }
    store.subdir(staging)?.restrict_owner_read()
}

/// Removes every entry beneath one owned directory through no-follow
/// descriptors only, restoring owner write on each visited directory through
/// its live descriptor. The directory itself is left in place for the caller
/// to remove; unexpected entry kinds abort removal rather than touching them.
fn remove_owned_tree(directory: &Dir) -> Result<()> {
    directory.permit_owner_write()?;
    for name in directory.list(None)? {
        let relative = PathBuf::from(&name);
        match directory.entry_type(&relative)? {
            EntryType::Directory => {
                remove_owned_tree(&directory.subdir(&relative)?)?;
                directory.remove_directory(&relative)?;
            }
            EntryType::File => directory.remove(&relative)?,
            _ => return Err(invalid("staging holds an unexpected entry")),
        }
    }
    Ok(())
}

/// Best-effort removal of one publisher staging directory using only
/// descriptor-anchored operations, tolerating the readonly modes a
/// nearly-finished publish already applied. A staging directory that cannot
/// be removed is left as the documented recoverable orphan; the staged blobs
/// are canonical content-addressed objects and stay for reuse or a later
/// garbage collector.
fn discard_staging(store: &Dir, staging_rel: &Path) {
    let Ok(directory) = store.subdir(staging_rel) else {
        return;
    };
    if remove_owned_tree(&directory).is_ok() {
        let _ = store.remove_directory(staging_rel);
    }
}

/// Verifies one shared tree against its reference and the store's physical
/// contract, failing closed on any drift.
///
/// Both reference fields are re-validated as strict digests, the store root
/// must be canonical and owner-held, and the tree directory must be a real
/// owner-held directory. The original manifest bytes must hash to
/// `reference.manifest_sha256`, parse through the same manifest validation
/// as [`crate::snapshot_tree::inspect_managed_snapshot`], and stay within
/// the entry and aggregate bounds. A bounded streaming walk then proves the
/// exact topology with no orphan, missing, symlinked, or foreign-typed
/// entries, every directory owner-held at `0o500`, every file owner-held at
/// its readonly physical mode, and the manifest owner-held at `0o400`.
/// Finally each file must be an internal hardlink to its canonical blob —
/// same device and inode — whose content is streamed against the manifest
/// digest, so dangling blobs, foreign targets, and drifted or overwritten
/// payloads are all rejected. Returns `Ok(())` only for a fully trusted
/// tree.
pub fn verify_shared_tree(store_root: &Path, reference: &SharedTreeRef) -> Result<()> {
    if !is_digest(&reference.scope) || !is_digest(&reference.manifest_sha256) {
        return Err(invalid("shared tree reference is invalid"));
    }
    let root = validated_root(store_root)?;
    let store = Dir::open(&root)?;
    require_owner(store.entry(None)?.uid, "root")?;
    let destination = tree_rel(reference);
    match store.entry_type(&destination) {
        Ok(EntryType::Directory) => {}
        Err(Error::Io(error)) if error.kind() == std::io::ErrorKind::NotFound => {
            return Err(invalid("shared tree is missing from the store"));
        }
        Ok(_) => return Err(invalid("shared tree root is not a real directory")),
        Err(error) => return Err(error),
    }
    require_owner(store.entry(Some(&destination))?.uid, "tree")?;
    let tree_dir = store.subdir(&destination)?;
    let manifest_bytes = tree_dir.read(Path::new(SNAPSHOT_MANIFEST), MAX_METADATA)?;
    if sha256(&manifest_bytes) != reference.manifest_sha256 {
        return Err(invalid("shared tree manifest hash does not match its reference"));
    }
    let entries = entry_map(
        &load_manifest(&tree_dir)?.ok_or_else(|| invalid("shared tree manifest is missing"))?,
    )?;
    validate_entries(&entries)?;
    verify_topology(&tree_dir, &entries, TreeShape::Shared, "shared tree")?;
    let manifest_metadata = tree_dir.open_file(Path::new(SNAPSHOT_MANIFEST))?.metadata()?;
    require_owner(manifest_metadata.uid(), "manifest")?;
    if manifest_metadata.permissions().mode() & 0o7777 != 0o400 {
        return Err(invalid("shared tree manifest mode drifted"));
    }
    for (path, entry) in &entries {
        if entry["type"] != "file" {
            continue;
        }
        let (sha256, logical, expected_len) = file_identity(entry)?;
        let blob = blob_rel(&reference.scope, &sha256, logical);
        match store.open_file(&blob) {
            Ok(file) => {
                let blob_metadata = file.metadata()?;
                let tree_metadata = store
                    .open_file(&destination.join(path))?
                    .metadata()?;
                require_owner(blob_metadata.uid(), "blob")?;
                require_owner(tree_metadata.uid(), "tree file")?;
                let physical = physical_mode(logical);
                if blob_metadata.permissions().mode() & 0o7777 != physical
                    || tree_metadata.permissions().mode() & 0o7777 != physical
                {
                    return Err(invalid(format!("shared tree file mode drifted: {path}")));
                }
                if blob_metadata.dev() != tree_metadata.dev()
                    || blob_metadata.ino() != tree_metadata.ino()
                {
                    return Err(invalid(format!(
                        "shared tree file is not the canonical payload hardlink: {path}"
                    )));
                }
                drop(file);
                verify_blob(&store, &blob, &sha256, expected_len, logical)?;
            }
            Err(Error::Io(error)) if error.kind() == std::io::ErrorKind::NotFound => {
                return Err(invalid("shared store blob is dangling"));
            }
            Err(error) => return Err(error),
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fs::sha256;
    use crate::snapshot_tree::{inspect_managed_snapshot, snapshot_managed_tree};
    use std::{
        fs,
        os::unix::fs::{MetadataExt, PermissionsExt},
        path::Path,
        thread,
    };

    /// One fixture file: relative path, bytes, and executable flag.
    type Fixture = (&'static str, &'static [u8], bool);

    /// Seals `files` into a fresh verified managed snapshot and returns
    /// `(source, home)`; both temporaries are RAII-cleaned.
    fn sealed(files: &[Fixture]) -> (tempfile::TempDir, tempfile::TempDir) {
        let source = tempfile::tempdir().unwrap();
        for (path, bytes, executable) in files {
            let target = source.path().join(path);
            fs::create_dir_all(target.parent().expect("fixture has a parent")).unwrap();
            fs::write(&target, bytes).unwrap();
            fs::set_permissions(
                &target,
                fs::Permissions::from_mode(if *executable { 0o700 } else { 0o600 }),
            )
            .unwrap();
        }
        let home = tempfile::tempdir().unwrap();
        snapshot_managed_tree(
            home.path(),
            Path::new("assets/runtime"),
            source.path(),
            None,
        )
        .unwrap();
        (source, home)
    }

    /// Imports `home`'s sealed tree under `scope` using one shared store path.
    fn import(store: &Path, scope: &str, home: &Path) -> SharedTreeRef {
        import_shared_tree(store, scope, home, Path::new("assets/runtime")).unwrap()
    }


    /// One fresh shared-store root in its canonical form; the `TempDir` is
    /// RAII-cleaned and `root` is the path handed to the module.
    fn store_root() -> (tempfile::TempDir, PathBuf) {
        let store = tempfile::tempdir().unwrap();
        let root = store.path().canonicalize().unwrap();
        (store, root)
    }

    /// Returns one path's inode number for hardlink identity assertions.
    fn inode_of(path: &Path) -> u64 {
        fs::metadata(path).unwrap().ino()
    }

    /// Returns the staged-`0o600` blob path for `payload` under `scope`.
    fn plain_blob(store: &Path, scope: &str, payload: &[u8]) -> PathBuf {
        store
            .join("blobs")
            .join(scope)
            .join(format!("{}-600", sha256(payload)))
    }

    /// A real digest-derived scope (digits included) and the strict
    /// lowercase validation both reference fields use.
    #[test]
    fn scope_validation_accepts_real_digests_only() {
        let lowercase = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";
        let uppercase = "0123456789ABCDEF0123456789abcdef0123456789abcdef0123456789ABCDEF";
        assert!(is_scope(lowercase));
        assert!(!is_scope(uppercase));
        assert!(crate::snapshot_tree::is_sha256(uppercase));
        assert!(!is_digest(uppercase));
        assert!(serde_json::from_value::<SharedTreeRef>(serde_json::json!({
            "scope": lowercase,
            "manifest_sha256": "ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff",
        }))
        .is_ok());
        assert!(serde_json::from_value::<SharedTreeRef>(serde_json::json!({
            "scope": lowercase,
            "manifest_sha256": "ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff",
            "extra": 1,
        }))
        .is_err());
    }

    /// Two imports of one sealed tree converge on one ref and shared inodes
    /// while the source keeps its independent inode.
    #[test]
    fn identical_imports_share_ref_and_inode() {
        let (source, home) = sealed(&[
            ("index.js", b"console.log('index')\n", true),
            ("lib/util.js", b"module.exports = () => 42\n", false),
        ]);
        let (_store, root) = store_root();
        let scope = sha256(b"account-alpha");
        let first = import(&root, &scope, home.path());
        let util = shared_tree_root(&root, &first)
            .unwrap()
            .join("lib/util.js");
        let shared_inode = inode_of(&util);
        let second = import(&root, &scope, home.path());
        assert_eq!(first, second);
        assert_eq!(inode_of(&util), shared_inode);
        assert_eq!(
            shared_inode,
            inode_of(&plain_blob(&root, &scope, b"module.exports = () => 42\n"))
        );
        assert_ne!(shared_inode, inode_of(&source.path().join("lib/util.js")));
        let sealed_manifest =
            fs::read(home.path().join("assets/runtime").join(SNAPSHOT_MANIFEST)).unwrap();
        assert_eq!(first.manifest_sha256, sha256(&sealed_manifest));
        assert!(
            inspect_managed_snapshot(
                &root.join("trees").join(&scope),
                Path::new(&first.manifest_sha256)
            )
            .unwrap()
            .verified
        );
        verify_shared_tree(&root, &first).unwrap();
        let tree = shared_tree_root(&root, &first).unwrap();
        assert_eq!(fs::metadata(&tree).unwrap().permissions().mode() & 0o777, 0o500);
        assert_eq!(
            fs::metadata(tree.join("index.js")).unwrap().permissions().mode() & 0o777,
            0o500
        );
        assert_eq!(
            fs::metadata(&util).unwrap().permissions().mode() & 0o777,
            0o400
        );
    }

    /// A changed tree version shares only the unchanged file's inode.
    #[test]
    fn changed_version_reuses_only_unchanged_payloads() {
        let (source, home) =
            sealed(&[("keep.txt", b"stable\n", false), ("change.txt", b"v1\n", false)]);
        let (_store, root) = store_root();
        let scope = sha256(b"account-beta");
        let v1 = import(&root, &scope, home.path());
        let v1_tree = shared_tree_root(&root, &v1).unwrap();
        fs::write(source.path().join("change.txt"), b"v2\n").unwrap();
        snapshot_managed_tree(
            home.path(),
            Path::new("assets/runtime"),
            source.path(),
            None,
        )
        .unwrap();
        let v2 = import(&root, &scope, home.path());
        assert_ne!(v1.manifest_sha256, v2.manifest_sha256);
        let v2_tree = shared_tree_root(&root, &v2).unwrap();
        assert_eq!(
            inode_of(&v2_tree.join("keep.txt")),
            inode_of(&v1_tree.join("keep.txt"))
        );
        assert_ne!(
            inode_of(&v2_tree.join("change.txt")),
            inode_of(&v1_tree.join("change.txt"))
        );
        verify_shared_tree(&root, &v2).unwrap();
    }

    /// Identical bytes never alias across scopes or execution modes.
    #[test]
    fn scope_and_exec_mode_do_not_alias() {
        let (_, plain_home) = sealed(&[("asset.txt", b"same bytes\n", false)]);
        let (_, exec_home) = sealed(&[("asset.txt", b"same bytes\n", true)]);
        let (_store, root) = store_root();
        let same_scope = sha256(b"domain-one");
        let other_scope = sha256(b"domain-two");
        let plain = import(&root, &same_scope, plain_home.path());
        let exec = import(&root, &same_scope, exec_home.path());
        let foreign = import(&root, &other_scope, plain_home.path());
        assert_ne!(plain, exec);
        assert_ne!(plain, foreign);
        let exec_blob = root
            .join("blobs")
            .join(&same_scope)
            .join(format!("{}-700", sha256(b"same bytes\n")));
        let inodes: Vec<_> = [
            plain_blob(&root, &same_scope, b"same bytes\n"),
            exec_blob,
            plain_blob(&root, &other_scope, b"same bytes\n"),
        ]
        .iter()
        .map(|path| inode_of(path))
        .collect();
        assert_ne!(inodes[0], inodes[1]);
        assert_ne!(inodes[0], inodes[2]);
        assert_ne!(inodes[1], inodes[2]);
        for reference in [&plain, &exec, &foreign] {
            verify_shared_tree(&root, reference).unwrap();
        }
    }

    /// Verification rejects tampered bytes, leaf symlinks, dangling blobs,
    /// and malformed references, and accepts again after repair.
    #[test]
    fn verification_rejects_drift_and_foreign_shapes() {
        let (_, home) = sealed(&[("plain.txt", b"plain\n", false)]);
        let (_store, root) = store_root();
        let scope = sha256(b"domain-drift");
        let reference = import(&root, &scope, home.path());
        let tree = shared_tree_root(&root, &reference).unwrap();
        let blob = plain_blob(&root, &scope, b"plain\n");
        let target = tree.join("plain.txt");

        fs::set_permissions(&target, fs::Permissions::from_mode(0o600)).unwrap();
        fs::write(&target, b"tampered\n").unwrap();
        fs::set_permissions(&target, fs::Permissions::from_mode(0o400)).unwrap();
        assert!(verify_shared_tree(&root, &reference).is_err());
        fs::set_permissions(&target, fs::Permissions::from_mode(0o600)).unwrap();
        fs::write(&target, b"plain\n").unwrap();
        fs::set_permissions(&target, fs::Permissions::from_mode(0o400)).unwrap();
        verify_shared_tree(&root, &reference).unwrap();

        fs::set_permissions(&tree, fs::Permissions::from_mode(0o700)).unwrap();
        fs::remove_file(&target).unwrap();
        std::os::unix::fs::symlink("/etc/passwd", &target).unwrap();
        fs::set_permissions(&tree, fs::Permissions::from_mode(0o500)).unwrap();
        assert!(verify_shared_tree(&root, &reference).is_err());
        fs::set_permissions(&tree, fs::Permissions::from_mode(0o700)).unwrap();
        fs::remove_file(&target).unwrap();
        fs::hard_link(&blob, &target).unwrap();
        fs::set_permissions(&tree, fs::Permissions::from_mode(0o500)).unwrap();
        verify_shared_tree(&root, &reference).unwrap();

        fs::remove_file(&blob).unwrap();
        assert!(verify_shared_tree(&root, &reference).is_err());
        fs::hard_link(&target, &blob).unwrap();
        verify_shared_tree(&root, &reference).unwrap();

        assert!(
            verify_shared_tree(
                &root,
                &SharedTreeRef {
                    scope: "not-a-scope".into(),
                    manifest_sha256: reference.manifest_sha256.clone(),
                }
            )
            .is_err()
        );
        let manifest = tree.join(SNAPSHOT_MANIFEST);
        fs::set_permissions(&manifest, fs::Permissions::from_mode(0o600)).unwrap();
        let bytes = fs::read(&manifest).unwrap();
        let mut drifted = bytes.clone();
        let seat = drifted.iter().position(|byte| *byte == b'p').expect("digit");
        drifted[seat] = b'q';
        fs::write(&manifest, &drifted).unwrap();
        fs::set_permissions(&manifest, fs::Permissions::from_mode(0o400)).unwrap();
        assert!(verify_shared_tree(&root, &reference).is_err());
        fs::set_permissions(&manifest, fs::Permissions::from_mode(0o600)).unwrap();
        fs::write(&manifest, &bytes).unwrap();
        fs::set_permissions(&manifest, fs::Permissions::from_mode(0o400)).unwrap();
        verify_shared_tree(&root, &reference).unwrap();
    }

    /// An unverified or missing source tree is refused, and a drifted source
    /// leaves no published tree under its final name.
    #[test]
    fn unverified_source_is_refused() {
        let (_, home) = sealed(&[("a.txt", b"one\n", false)]);
        let (_store, root) = store_root();
        let scope = sha256(b"domain-refused");
        let manifest =
            fs::read(home.path().join("assets/runtime").join(SNAPSHOT_MANIFEST)).unwrap();
        let destination = root.join("trees")
            .join(&scope)
            .join(sha256(&manifest));
        assert!(
            import_shared_tree(&root, &scope, home.path(), Path::new("assets/absent"))
                .is_err()
        );
        assert!(!root.join("trees").exists());
        fs::write(home.path().join("assets/runtime/a.txt"), b"drifted\n").unwrap();
        assert!(
            import_shared_tree(&root, &scope, home.path(), Path::new("assets/runtime"))
                .is_err()
        );
        assert!(!destination.exists());
        let names: Vec<_> = match fs::read_dir(root.join("trees").join(&scope)) {
            Ok(iter) => iter
                .flatten()
                .map(|entry| entry.file_name().to_string_lossy().into_owned())
                .collect(),
            Err(_) => Vec::new(),
        };
        assert!(
            names.iter().all(|name| name.starts_with(TEMP_PREFIX)),
            "no published tree may remain: {names:?}"
        );
    }

    /// A pre-existing entry at the content-addressed destination — even an
    /// empty directory a plain rename could replace — is never overwritten.
    #[test]
    fn preexisting_empty_destination_is_never_replaced() {
        let (_, home) = sealed(&[("a.txt", b"one\n", false)]);
        let (_store, root) = store_root();
        let scope = sha256(b"domain-empty");
        let manifest =
            fs::read(home.path().join("assets/runtime").join(SNAPSHOT_MANIFEST)).unwrap();
        let destination = root.join("trees")
            .join(&scope)
            .join(sha256(&manifest));
        fs::create_dir_all(destination.parent().unwrap()).unwrap();
        fs::create_dir(&destination).unwrap();
        assert!(
            import_shared_tree(&root, &scope, home.path(), Path::new("assets/runtime"))
                .is_err()
        );
        assert!(destination.read_dir().unwrap().next().is_none());
        assert!(
            verify_shared_tree(
                &root,
                &SharedTreeRef {
                    scope: scope.clone(),
                    manifest_sha256: sha256(&manifest),
                }
            )
            .is_err()
        );
    }

    /// Concurrent publishers of one tree converge on one ref with no staging
    /// leftovers and one lock file.
    #[test]
    fn concurrent_imports_converge() {
        let (_, home) = sealed(&[("x.txt", b"x\n", false), ("sub/y.txt", b"y\n", false)]);
        let (_store, root) = store_root();
        let scope = sha256(b"domain-race");
        let references = thread::scope(|scope_handle| {
            let mut handles = Vec::new();
            for _ in 0..4 {
                let store_root = root.to_path_buf();
                let home_path = home.path().to_path_buf();
                let scope_value = scope.clone();
                handles.push(scope_handle.spawn(move || {
                    import_shared_tree(
                        &store_root,
                        &scope_value,
                        &home_path,
                        Path::new("assets/runtime"),
                    )
                }));
            }
            handles
                .into_iter()
                .map(|handle| {
                    handle
                        .join()
                        .expect("publisher thread panicked")
                        .expect("concurrent import")
                })
                .collect::<Vec<_>>()
        });
        assert!(references.windows(2).all(|pair| pair[0] == pair[1]));
        let container = root.join("trees").join(&scope);
        let names: Vec<_> = fs::read_dir(&container)
            .unwrap()
            .map(|entry| entry.unwrap().file_name().to_string_lossy().into_owned())
            .collect();
        assert_eq!(names, vec![references[0].manifest_sha256.clone()]);
        assert!(root.join(".publish.lock").exists());
        verify_shared_tree(&root, &references[0]).unwrap();
    }

    /// A drifted blob refuses reuse for a second tree and leaves no staging
    /// orphan; repairing the blob restores imports.
    #[test]
    fn drifted_blob_refuses_reuse_and_cleans_staging() {
        let (_, home) = sealed(&[("shared.txt", b"payload\n", false)]);
        let (_store, root) = store_root();
        let scope = sha256(b"domain-blob");
        let first = import(&root, &scope, home.path());
        let (_, second_home) = sealed(&[
            ("shared.txt", b"payload\n", false),
            ("extra.txt", b"more\n", false),
        ]);
        let blob = plain_blob(&root, &scope, b"payload\n");
        fs::set_permissions(&blob, fs::Permissions::from_mode(0o600)).unwrap();
        fs::write(&blob, b"drifted\n").unwrap();
        fs::set_permissions(&blob, fs::Permissions::from_mode(0o400)).unwrap();
        assert!(
            import_shared_tree(&root, &scope, second_home.path(), Path::new("assets/runtime"))
                .is_err()
        );
        let container = root.join("trees").join(&scope);
        let names: Vec<_> = fs::read_dir(&container)
            .unwrap()
            .map(|entry| entry.unwrap().file_name().to_string_lossy().into_owned())
            .collect();
        assert_eq!(names, vec![first.manifest_sha256.clone()]);
        fs::set_permissions(&blob, fs::Permissions::from_mode(0o600)).unwrap();
        fs::write(&blob, b"payload\n").unwrap();
        fs::set_permissions(&blob, fs::Permissions::from_mode(0o400)).unwrap();
        let second = import(&root, &scope, second_home.path());
        verify_shared_tree(&root, &second).unwrap();
    }

    /// A Node fixture resolves relative module imports from the shared tree
    /// root when a finite local `node` binary is available.
    #[test]
    fn node_resolves_relative_modules_from_tree_root() {
        if std::process::Command::new("node")
            .arg("--version")
            .output()
            .is_err()
        {
            eprintln!("skipping: node is unavailable");
            return;
        }
        let (_, home) = sealed(&[
            (
                "index.js",
                b"const util = require('./lib/util.js'); process.stdout.write(util.marker);\n",
                true,
            ),
            ("lib/util.js", b"exports.marker = 'shared-ok';\n", false),
        ]);
        let (_store, root) = store_root();
        let reference = import(&root, &sha256(b"domain-node"), home.path());
        let tree = shared_tree_root(&root, &reference).unwrap();
        let output = std::process::Command::new("node")
            .arg("index.js")
            .current_dir(&tree)
            .output()
            .unwrap();
        assert!(output.status.success(), "{output:?}");
        assert_eq!(String::from_utf8_lossy(&output.stdout), "shared-ok");
    }
}
