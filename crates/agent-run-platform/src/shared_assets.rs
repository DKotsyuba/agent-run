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
//!   owner-only mode `0o700` (legacy `0o500` trees stay verifiable), and
//!   the manifest at mode `0o400`.
//!
//! `<scope>` is a caller-provided 64-character lowercase hexadecimal digest
//! separating account or compatibility domains; payloads never alias across
//! scopes or execution modes. Source trees are verified with a bounded
//! streaming walker (never buffering a whole tree), publication is atomic
//! and no-replace at every layer, and an existing content-addressed object
//! is verified and reused, never overwritten. Staging directories and
//! temporary blobs (`.agent-run-staging-*.tmp`) are the only recoverable
//! orphans an interrupted publisher can leave behind; garbage collection and
//! reference tracking are later units.

use crate::{
    fs::{Dir, EntryType, Flush, sha256},
    snapshot_tree::{SNAPSHOT_MANIFEST, entry_map, load_manifest_bounded},
};
use agent_run_domain::{Error, Result, canonical::hex_digest, error::invalid};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::{
    collections::{BTreeMap, BTreeSet},
    io::{Read, Write},
    os::unix::fs::{MetadataExt, PermissionsExt},
    path::{Path, PathBuf},
};

/// Per-file payload bound. Managed snapshot capture still stops at 16 MiB;
/// the store admits up to 32 MiB so a native Git pack (measured 24 MB)
/// streams through the same publisher, and anything larger is refused.
const MAX_FILE_BYTES: usize = 32 * 1024 * 1024;
/// Maximum manifest entries one shared tree may describe, also the walker's
/// visited-entry bound so unexpected orphan growth cannot make a scan run
/// away (plus one for the manifest itself). Sized for the native curated
/// plugin mirror (measured 7732 entries).
const MAX_TREE_ENTRIES: usize = 16_384;
/// Read bound for one shared tree's manifest bytes — shared trees only.
///
/// A 16384-entry tree's canonical manifest needs well over the historical
/// 64 KiB metadata bound (the measured curated mirror is ~1.3 MiB), so
/// import, verification and census read shared-tree manifests under this
/// bound instead. Runtime indexes, operation records, markers, plugin views
/// and managed snapshots keep the 64 KiB bound; the manifest format, bytes
/// and digests are unchanged.
pub const MAX_TREE_MANIFEST_BYTES: usize = 4 * 1024 * 1024;
/// Maximum aggregate payload bytes one shared tree may describe.
const MAX_TREE_BYTES: u64 = 256 * 1024 * 1024;
/// Streaming chunk size for hashing and copying payloads.
const STREAM_CHUNK: usize = 64 * 1024;
/// Prefix naming publisher-owned staging directories beneath `trees/<scope>`
/// and temporary blobs beneath `blobs/<scope>`.
pub const TEMP_PREFIX: &str = ".agent-run-staging-";
/// Suffix matching the publisher-owned temporary convention of `snapshot_tree`.
pub const TEMP_SUFFIX: &str = ".tmp";
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
    /// Opens the store-wide lock file without locking it, creating it only
    /// when `create` is set.
    fn open_lock(store_root: &Path, create: bool) -> Result<Option<std::fs::File>> {
        use std::os::unix::fs::OpenOptionsExt;
        let lock_path = store_root.join(LOCK_NAME);
        let mut options = std::fs::OpenOptions::new();
        options
            .write(true)
            .truncate(false)
            .mode(0o600)
            .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC);
        if create {
            options.create(true);
        }
        match options.open(&lock_path) {
            Ok(file) => Ok(Some(file)),
            // Without `create`, a missing lock file is simply nothing to take.
            Err(error) if !create && error.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(error) => Err(error.into()),
        }
    }

    /// Blocks until this process owns the shared store's publish lock.
    ///
    /// `store_root` must already exist as the canonical owner-controlled real
    /// directory this module validates. Ordinary contention waits; the call
    /// fails only when the lock file cannot be opened (including when it is a
    /// symbolic link) or locked. The guard releases the lock on drop.
    pub fn acquire(store_root: &Path) -> Result<Self> {
        let lock_path = store_root.join(LOCK_NAME);
        let file = Self::open_lock(store_root, true)?.expect("created lock file");
        file.lock().map_err(Error::from)?;
        Ok(Self { file, lock_path })
    }

    /// Tries once to own the shared store's publish lock without waiting.
    ///
    /// Returns `Ok(None)` when another cooperating process holds it, so a
    /// maintenance caller can skip this pass instead of stalling the broker
    /// behind an import, and — with `create`
    /// unset — when the lock file does not exist yet, so a read-only caller
    /// never creates store state just to look at it. Any failure to open or
    /// lock the file other than those is an error, never a silent pass.
    pub fn try_acquire(store_root: &Path, create: bool) -> Result<Option<Self>> {
        let lock_path = store_root.join(LOCK_NAME);
        let Some(file) = Self::open_lock(store_root, create)? else {
            return Ok(None);
        };
        match file.try_lock() {
            Ok(()) => Ok(Some(Self { file, lock_path })),
            Err(std::fs::TryLockError::WouldBlock) => Ok(None),
            Err(std::fs::TryLockError::Error(error)) => Err(error.into()),
        }
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
/// the trusted root. Returns the validated canonical path for subsequent
/// descriptor opens and store-target derivation. Shared by this module's
/// import/verify entry points and `snapshot_tree`'s explicit shared-bridge
/// verifier, which derives each expected home symlink target from it.
pub fn validated_root(store_root: &Path) -> Result<PathBuf> {
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
    blobs_scope(scope).join(blob_name(sha256, logical))
}

/// Canonical file name of one payload inside its scope's blob directory.
fn blob_name(sha256: &str, logical: u32) -> PathBuf {
    PathBuf::from(format!("{sha256}-{logical:o}"))
}

/// Returns the physical store mode for one normalized logical mode: readonly
/// `0o400` for data, readonly-executable `0o500` for executables. The
/// manifest's logical modes stay `0o600`/`0o700`; only the stored files are
/// restricted. The readonly bits stop accidental writes only; they are not a
/// boundary against another process of the same user, and integrity rests on
/// digest verification.
fn physical_mode(logical: u32) -> u32 {
    if logical & 0o111 != 0 { 0o500 } else { 0o400 }
}

/// Returns whether `mode` is a valid shared-tree or plugin-view directory
/// mode: owner-only `0o700` (the portable publication mode) or owner-only
/// readonly `0o500` (the legacy publication mode).
///
/// `0o700` is the mode new directories are published at because macOS
/// denies renaming a write-disabled directory even within its parent, which
/// would break the no-replace atomic publication and collection renames.
/// `0o500` directories published before that contract remain first-class
/// verifiable objects, so verification and collection accept exactly these
/// two owner-only modes and no group- or other-accessible mode.
pub fn is_shared_directory_mode(mode: u32) -> bool {
    matches!(mode, 0o500 | 0o700)
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
    let bytes = entry
        .get("bytes")
        .and_then(Value::as_u64)
        .unwrap_or(u64::MAX);
    if bytes > MAX_FILE_BYTES as u64 {
        return Err(invalid("shared tree file exceeds the payload bound"));
    }
    Ok((sha256.to_owned(), mode as u32, bytes))
}

/// Validates the parsed manifest shape and returns the aggregate payload
/// bytes, refusing trees above the entry or total-size bounds before any
/// filesystem work. The manifest's own [`MAX_TREE_MANIFEST_BYTES`] parse
/// bound keeps this check cheap.
///
/// Every entry naming one payload digest must also declare the same size:
/// all of them resolve to one content-addressed blob, which cannot satisfy
/// two sizes, so a conflicting manifest is refused before publication
/// rather than linking an entry to a blob of another length. Same-digest
/// entries with the same size stay valid and share one blob.
fn validate_entries(entries: &BTreeMap<String, Value>) -> Result<u64> {
    if entries.len() > MAX_TREE_ENTRIES {
        return Err(invalid("shared tree exceeds the manifest entry bound"));
    }
    let mut total = 0_u64;
    let mut sizes: BTreeMap<String, u64> = BTreeMap::new();
    for entry in entries.values() {
        if entry["type"] == "file" {
            let (sha256, _, bytes) = file_identity(entry)?;
            if *sizes.entry(sha256).or_insert(bytes) != bytes {
                return Err(invalid(
                    "shared tree manifest gives one payload digest conflicting sizes",
                ));
            }
            total += bytes;
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
    /// directory at a valid [`is_shared_directory_mode`] owner-only mode.
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
    if shape == TreeShape::Shared && !is_shared_directory_mode(root.entry(None)?.mode) {
        return Err(invalid(format!(
            "shared tree {label} directory mode drifted"
        )));
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
            invalid(format!(
                "{label} holds an entry its manifest does not describe: {path}"
            ))
        })?;
        seen.insert(path.clone());
        match entry["type"].as_str() {
            Some("directory") => {
                if identity.kind != EntryType::Directory {
                    return Err(invalid(format!(
                        "{label} entry is not a real directory: {path}"
                    )));
                }
                if shape == TreeShape::Shared && !is_shared_directory_mode(identity.mode) {
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
fn check_source_file(file: &std::fs::File, expected_len: u64, logical: u32) -> Result<()> {
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
/// digest, pushed with `fsync(2)`, persisted by one device barrier, and only
/// then atomically renamed into its content-addressed name with the
/// kernel's no-replace rename — so a canonical blob name never refers to
/// non-durable data, an existing blob is verified and reused, never
/// overwritten, and the tree's files are internal hardlinks to that
/// canonical blob. The staged tree's entries and modes are pushed and a
/// second barrier persists them before the no-replace rename publishes it
/// (see [`crate::fs::Flush`] for the exact flush semantics). Publication
/// holds [`SharedStoreLock`] only for the staging window.
///
/// An interrupted publisher leaves at most one recoverable
/// `.agent-run-staging-*.tmp` orphan directory beneath `trees/<scope>` and
/// `.agent-run-staging-*.tmp` temporary blobs beneath `blobs/<scope>`; this
/// unit performs no garbage collection. Returns the [`SharedTreeRef`] naming
/// the tree; on error no partial tree exists under its final name (blobs are
/// content-addressed and safe to leave for reuse or a later collector).
pub fn import_shared_tree(
    store_root: &Path,
    scope: &str,
    source_home: &Path,
    relative_root: &Path,
) -> Result<SharedTreeRef> {
    import_shared_tree_with_contention(store_root, scope, source_home, relative_root, false)?
        .ok_or_else(|| invalid("shared tree publication lock unavailable"))
}

/// Imports a verified snapshot only when the publish lock is immediately free.
/// Returns `Ok(None)` on contention without publishing or changing the source.
/// All topology, ownership, digest and durability checks are identical to
/// `import_shared_tree`; ordinary validation/I/O failures remain errors.
/// This bounds lock waiting, not filesystem system-call latency.
pub fn try_import_shared_tree(
    store_root: &Path,
    scope: &str,
    source_home: &Path,
    relative_root: &Path,
) -> Result<Option<SharedTreeRef>> {
    import_shared_tree_with_contention(store_root, scope, source_home, relative_root, true)
}

/// Shared publisher for required and optional imports; `nonblocking` selects
/// only lock acquisition policy. Contention may return no reference only for
/// optional imports, before any store mutation; the source is never changed.
fn import_shared_tree_with_contention(
    store_root: &Path,
    scope: &str,
    source_home: &Path,
    relative_root: &Path,
    nonblocking: bool,
) -> Result<Option<SharedTreeRef>> {
    if !is_scope(scope) {
        return Err(invalid(
            "shared store scope must be 64 lowercase hexadecimal digits",
        ));
    }
    if !source_home.is_absolute() {
        return Err(invalid("managed snapshot home must be an absolute path"));
    }
    let source_dir = Dir::open(&source_home.join(relative_root))?;
    let manifest_bytes = source_dir.read(Path::new(SNAPSHOT_MANIFEST), MAX_TREE_MANIFEST_BYTES)?;
    let reference = SharedTreeRef {
        scope: scope.to_owned(),
        manifest_sha256: sha256(&manifest_bytes),
    };
    let entries = entry_map(
        &load_manifest_bounded(&source_dir, MAX_TREE_MANIFEST_BYTES)?
            .ok_or_else(|| invalid("managed snapshot manifest is missing"))?,
    )?;
    validate_entries(&entries)?;
    verify_topology(&source_dir, &entries, TreeShape::Source, "managed snapshot")?;
    let root = validated_root(store_root)?;
    let _guard = if nonblocking {
        let Some(guard) = SharedStoreLock::try_acquire(&root, true)? else {
            return Ok(None);
        };
        guard
    } else {
        SharedStoreLock::acquire(&root)?
    };
    let store = Dir::open(&root)?;
    require_owner(store.entry(None)?.uid, "root")?;
    store.directory(&blobs_scope(scope))?;
    store.directory(&trees_scope(scope))?;
    let destination = tree_rel(&reference);
    match store.entry_type(&destination) {
        Ok(EntryType::Directory) => {
            verify_shared_tree(&root, &reference)?;
            return Ok(Some(reference));
        }
        Ok(_) => {
            return Err(invalid(
                "shared tree destination exists and is not a real directory",
            ));
        }
        Err(Error::Io(error)) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => return Err(error),
    }
    let staging = trees_scope(scope).join(format!(
        "{TEMP_PREFIX}{}{TEMP_SUFFIX}",
        uuid::Uuid::new_v4().simple()
    ));
    store.directory(&staging)?;
    let staged = stage_shared_tree(
        &store,
        &staging,
        scope,
        &entries,
        &source_dir,
        &manifest_bytes,
    );
    // Barrier: the staged tree's data, entries and modes are all persisted
    // before the no-replace rename makes it authoritative.
    let published = staged
        .and_then(|()| seal_staging(&store, &staging, &entries))
        .and_then(|()| store.sync())
        .and_then(|()| store.rename_entry_no_replace(&staging, &destination));
    match published {
        Ok(true) => Ok(Some(reference)),
        Ok(false) => {
            // An object already occupies the content-addressed destination —
            // most often a concurrent publisher of the identical tree.
            discard_staging(&store, &staging);
            verify_shared_tree(&root, &reference)?;
            Ok(Some(reference))
        }
        Err(error) => {
            discard_staging(&store, &staging);
            Err(error)
        }
    }
}

/// Stages one complete shared tree beneath `staging`, manifest bytes last.
///
/// Publication crosses one durability barrier per authority point instead
/// of a full device flush per object:
///
/// 1. every file is opened once from `source_dir` through a no-follow
///    descriptor and checked against the manifest; each payload not yet in
///    the store streams into an exclusive temporary blob under a running
///    hash and is pushed to the device with plain `fsync(2)`
///    ([`crate::fs::push`]); an existing blob is re-verified and reused;
/// 2. one [`Dir::sync`] barrier (`F_FULLFSYNC` on Apple hosts) persists
///    every pushed payload, and only then are the temporaries renamed to
///    their canonical content-addressed names with the no-replace rename —
///    so a canonical blob name can never refer to data that is not durable;
/// 3. the staged directories and internal hardlinks are created unflushed
///    and the manifest is written durably.
///
/// Each directory whose entries changed is pushed once after all of its
/// changes rather than once per entry: every blob directory after the
/// renames here, and every staged directory by the caller's `seal_staging`
/// (entries and final mode), whose barrier then persists
/// the staged namespace before its no-replace rename makes it
/// authoritative. A source that changed
/// since its capture is refused by the hash comparison, and on any error
/// every temporary this call created is removed; canonical blobs already
/// renamed stay content-addressed and verified for reuse or collection.
/// Nothing outside `blobs/<scope>`, `trees/<scope>`, and `staging` is
/// touched.
fn stage_shared_tree(
    store: &Dir,
    staging: &Path,
    scope: &str,
    entries: &BTreeMap<String, Value>,
    source_dir: &Dir,
    manifest_bytes: &[u8],
) -> Result<()> {
    // Every blob of this scope lives in one directory, resolved once so each
    // payload operation below names a single component.
    let blobs = store.subdir(&blobs_scope(scope))?;
    let mut temporaries: Vec<PathBuf> = Vec::new();
    let staged = stage_payloads(&blobs, entries, source_dir, &mut temporaries)
        .and_then(|pending| {
            // Barrier: every pushed payload is persisted before any
            // canonical name can refer to it.
            store.sync()?;
            publish_payloads(&blobs, &pending)
        })
        .and_then(|()| link_staging(store, staging, &blobs, entries))
        .and_then(|()| store.write(&staging.join(SNAPSHOT_MANIFEST), manifest_bytes, 0o400));
    if staged.is_err() {
        // Only the exclusive temporaries this call created are removed; a
        // name already renamed to its canonical blob no longer exists here.
        for temporary in &temporaries {
            if blobs.entry_type(temporary).is_ok() {
                let _ = blobs.discard(temporary);
            }
        }
    }
    staged
}

/// One payload streamed into an exclusive temporary blob and pushed, not yet
/// published: `(temporary, canonical name, sha256, length, logical mode)`,
/// both names relative to the scope's blob directory.
type PendingBlob = (PathBuf, PathBuf, String, u64, u32);

/// Phase 1 of [`stage_shared_tree`]: checks every source file and streams
/// each payload the store lacks into a pushed exclusive temporary inside
/// `blobs`, the scope's blob directory.
///
/// Every temporary created is recorded in `temporaries` before its first
/// byte, so the caller can remove it on any error. A payload shared by
/// several entries is streamed once; an existing canonical blob is
/// re-verified by streaming and reused. Returns the pending publications.
fn stage_payloads(
    blobs: &Dir,
    entries: &BTreeMap<String, Value>,
    source_dir: &Dir,
    temporaries: &mut Vec<PathBuf>,
) -> Result<Vec<PendingBlob>> {
    let mut pending = Vec::new();
    let mut planned = BTreeSet::new();
    for (path, entry) in entries {
        if entry["type"] == "directory" {
            continue;
        }
        let (sha256, logical, expected_len) = file_identity(entry)?;
        let mut source_file = source_dir.open_file(Path::new(path.as_str()))?;
        check_source_file(&source_file, expected_len, logical)?;
        let blob = blob_name(&sha256, logical);
        if !planned.insert(blob.clone()) {
            continue;
        }
        match blobs.entry_type(&blob) {
            Err(Error::Io(error)) if error.kind() == std::io::ErrorKind::NotFound => {
                let temporary = PathBuf::from(format!(
                    "{TEMP_PREFIX}{}{TEMP_SUFFIX}",
                    uuid::Uuid::new_v4().simple()
                ));
                temporaries.push(temporary.clone());
                stream_into_blob(
                    blobs,
                    &temporary,
                    &mut source_file,
                    &sha256,
                    expected_len,
                    logical,
                )?;
                pending.push((temporary, blob, sha256, expected_len, logical));
            }
            Ok(EntryType::File) => verify_blob(blobs, &blob, &sha256, expected_len, logical)?,
            Ok(_) => return Err(invalid("shared store blob exists with an unexpected type")),
            Err(error) => return Err(error),
        }
    }
    Ok(pending)
}

/// Phase 2 of [`stage_shared_tree`], after the durability barrier: renames
/// each already-persisted temporary to its canonical name inside `blobs`
/// with the no-replace rename, then pushes the blob directory once. When a
/// canonical blob appeared meanwhile the temporary is removed and the
/// existing blob is verified and reused — an existing object is never
/// overwritten. The names are persisted by the caller's second barrier.
fn publish_payloads(blobs: &Dir, pending: &[PendingBlob]) -> Result<()> {
    for (temporary, blob, sha256, expected_len, logical) in pending {
        if !blobs.rename_entry_no_replace_flushed(temporary, blob, Flush::Skipped)? {
            blobs.discard(temporary)?;
            verify_blob(blobs, blob, sha256, *expected_len, *logical)?;
        }
    }
    blobs.push()
}

/// Phase 3 of [`stage_shared_tree`]: creates every staged directory and
/// links every staged file to its canonical blob in `blobs`, unflushed.
///
/// Directories are created parent-first in manifest order; files are then
/// linked per parent directory, each parent resolved once, so every link
/// names a single component on both sides. The caller's
/// `seal_staging` pushes each staged directory afterwards and its
/// barrier persists them before publication.
fn link_staging(
    store: &Dir,
    staging: &Path,
    blobs: &Dir,
    entries: &BTreeMap<String, Value>,
) -> Result<()> {
    let mut files: BTreeMap<PathBuf, Vec<(PathBuf, PathBuf)>> = BTreeMap::new();
    for (path, entry) in entries {
        let relative = Path::new(path.as_str());
        if entry["type"] == "directory" {
            store.make_directory(&staging.join(relative), Flush::Skipped)?;
            continue;
        }
        let (sha256, logical, _) = file_identity(entry)?;
        let name = relative
            .file_name()
            .ok_or_else(|| invalid("shared tree manifest path is invalid"))?;
        files
            .entry(staging.join(relative.parent().unwrap_or(Path::new(""))))
            .or_default()
            .push((PathBuf::from(name), blob_name(&sha256, logical)));
    }
    for (parent, names) in files {
        let directory = store.subdir(&parent)?;
        for (name, blob) in names {
            if !directory.hardlink_flushed(&name, blobs, &blob, Flush::Skipped)? {
                return Err(invalid("shared tree staging name already exists"));
            }
        }
    }
    Ok(())
}

/// Copies `source` into the exclusive `temporary` name, hashing as it goes,
/// and pushes it to the device with plain `fsync(2)`.
///
/// The temporary is created at its readonly physical mode. Fails (leaving
/// the temporary for the caller's cleanup) when the streamed bytes exceed
/// the payload bound or their digest and length do not match the manifest
/// expectations. The pushed bytes become durable at the caller's next
/// [`Dir::sync`] barrier, which must precede the canonical rename.
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
    crate::fs::push(&file)?;
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

/// Seals every staged directory, including the staging root, at its final
/// owner-only `0o700` mode and pushes each changed directory to the device;
/// the caller's [`Dir::sync`] barrier then persists the whole staged
/// namespace before the rename makes it authoritative.
///
/// Directories keep the `0o700` mode [`Dir::make_directory`] created them
/// with instead of being restricted to readonly `0o500`: macOS denies
/// renaming a write-disabled directory even within its parent, so a `0o500`
/// staged tree could not be published by the no-replace atomic rename on
/// those hosts. Same-UID agent immutability is enforced by the qualified
/// native shared-root guard, not by directory modes; payload files keep
/// their readonly physical `0o400`/`0o500` modes.
fn seal_staging(store: &Dir, staging: &Path, entries: &BTreeMap<String, Value>) -> Result<()> {
    for (path, entry) in entries {
        if entry["type"] == "directory" {
            store.subdir(&staging.join(path.as_str()))?.push()?;
        }
    }
    store.subdir(staging)?.push()
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

/// Returns the store-relative blob paths one shared tree's manifest describes.
///
/// The tree is read through no-follow descriptors and only its manifest bytes
/// are parsed, through the same validators publication uses; the tree's own
/// files are not opened. A tree whose manifest is missing, unparsable, or
/// violates the manifest contract is an error, so a caller collecting the
/// still-referenced payload set treats it as incomplete evidence rather than
/// proof that its blobs are unreferenced.
pub fn shared_tree_blob_names(
    store_root: &Path,
    reference: &SharedTreeRef,
) -> Result<BTreeSet<PathBuf>> {
    if !is_digest(&reference.scope) || !is_digest(&reference.manifest_sha256) {
        return Err(invalid("shared tree reference is invalid"));
    }
    let root = validated_root(store_root)?;
    let store = Dir::open(&root)?;
    let tree_dir = store.subdir(&tree_rel(reference))?;
    let entries = entry_map(
        &load_manifest_bounded(&tree_dir, MAX_TREE_MANIFEST_BYTES)?
            .ok_or_else(|| invalid("shared tree manifest is missing"))?,
    )?;
    validate_entries(&entries)?;
    let mut blobs = BTreeSet::new();
    for entry in entries.values() {
        if entry["type"] != "file" {
            continue;
        }
        let (sha256, logical, _) = file_identity(entry)?;
        blobs.insert(blob_rel(&reference.scope, &sha256, logical));
    }
    Ok(blobs)
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
/// entries, every directory owner-held at a valid owner-only mode
/// (`0o700`, or legacy `0o500`), every file owner-held at
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
    let manifest_bytes = tree_dir.read(Path::new(SNAPSHOT_MANIFEST), MAX_TREE_MANIFEST_BYTES)?;
    if sha256(&manifest_bytes) != reference.manifest_sha256 {
        return Err(invalid(
            "shared tree manifest hash does not match its reference",
        ));
    }
    let entries = entry_map(
        &load_manifest_bounded(&tree_dir, MAX_TREE_MANIFEST_BYTES)?
            .ok_or_else(|| invalid("shared tree manifest is missing"))?,
    )?;
    validate_entries(&entries)?;
    verify_topology(&tree_dir, &entries, TreeShape::Shared, "shared tree")?;
    let manifest_metadata = tree_dir
        .open_file(Path::new(SNAPSHOT_MANIFEST))?
        .metadata()?;
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
                let tree_metadata = store.open_file(&destination.join(path))?.metadata()?;
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

    /// Optional publication skips a held lock without changing the sealed source
    /// or publishing a tree, then performs the same verified import after release.
    #[test]
    fn optional_import_skips_busy_lock_and_retries_without_source_changes() {
        let (_source, home) = sealed(&[("file.txt", b"fixture", false)]);
        let (_store, root) = store_root();
        let scope = "a".repeat(64);
        let before = inspect_managed_snapshot(home.path(), Path::new("assets/runtime")).unwrap();
        let held = SharedStoreLock::acquire(&root).unwrap();
        assert!(
            try_import_shared_tree(&root, &scope, home.path(), Path::new("assets/runtime"))
                .unwrap()
                .is_none()
        );
        assert!(!root.join("trees").exists());
        assert_eq!(
            inspect_managed_snapshot(home.path(), Path::new("assets/runtime")).unwrap(),
            before
        );
        drop(held);
        let reference =
            try_import_shared_tree(&root, &scope, home.path(), Path::new("assets/runtime"))
                .unwrap()
                .unwrap();
        verify_shared_tree(&root, &reference).unwrap();
        assert_eq!(
            inspect_managed_snapshot(home.path(), Path::new("assets/runtime")).unwrap(),
            before
        );
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

    /// Exercises the readonly publication primitives with stage-specific
    /// failures, so a host permission difference is distinguishable from a
    /// manifest or supervisor error. Published payload FILES stay readonly
    /// while staged DIRECTORY entries keep their portable owner-only `0o700`
    /// mode, so the no-replace publish rename of the tree itself cannot be
    /// denied as a write-disabled-directory rename.
    #[test]
    fn readonly_publication_primitives_keep_their_contract() {
        let (_temporary, root) = store_root();
        let directory = Dir::open(&root).expect("open owned store");
        let mut file = directory
            .create_exclusive(Path::new("pending"), 0o400)
            .expect("create readonly temporary blob");
        file.write_all(b"payload")
            .expect("write through creator descriptor");
        crate::fs::push(&file).expect("push readonly blob contents");
        drop(file);
        directory.sync().expect("persist blob contents");
        assert!(
            directory
                .rename_entry_no_replace_flushed(
                    Path::new("pending"),
                    Path::new("blob"),
                    Flush::Skipped
                )
                .expect("rename readonly blob")
        );
        directory.push().expect("push blob namespace");
        directory
            .directory(Path::new("staged"))
            .expect("create staged tree");
        assert!(
            directory
                .hardlink_flushed(
                    Path::new("staged/leaf"),
                    &directory,
                    Path::new("blob"),
                    Flush::Skipped
                )
                .expect("link readonly blob into staged tree")
        );
        directory
            .write(Path::new("staged/manifest"), b"manifest", 0o400)
            .expect("write readonly manifest");
        let tree = directory
            .subdir(Path::new("staged"))
            .expect("open staged tree");
        assert_eq!(tree.entry(None).unwrap().mode, 0o700);
        tree.push().expect("push sealed directory metadata");
        directory.sync().expect("persist staged namespace");
        assert!(
            directory
                .rename_entry_no_replace(Path::new("staged"), Path::new("published"))
                .expect("publish sealed tree")
        );
        directory
            .directory(Path::new("other"))
            .expect("create sibling tree");
        assert!(
            !directory
                .rename_entry_no_replace(Path::new("other"), Path::new("published"))
                .expect("probe occupied destination"),
            "publication never replaces an existing entry"
        );
        assert!(root.join("other").is_dir() && root.join("published").is_dir());
        assert_eq!(
            inode_of(&root.join("blob")),
            inode_of(&root.join("published/leaf"))
        );
        assert_eq!(
            directory.entry(Some(Path::new("published"))).unwrap().mode,
            0o700
        );
        assert_eq!(
            directory
                .entry(Some(Path::new("published/leaf")))
                .unwrap()
                .mode,
            0o400
        );
        assert_eq!(
            directory
                .entry(Some(Path::new("published/manifest")))
                .unwrap()
                .mode,
            0o400
        );
        tree.permit_owner_write()
            .expect("allow owned fixture cleanup");
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
        let util = shared_tree_root(&root, &first).unwrap().join("lib/util.js");
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
        assert_eq!(
            fs::metadata(&tree).unwrap().permissions().mode() & 0o777,
            0o700
        );
        assert_eq!(
            fs::metadata(tree.join("index.js"))
                .unwrap()
                .permissions()
                .mode()
                & 0o777,
            0o500
        );
        assert_eq!(
            fs::metadata(&util).unwrap().permissions().mode() & 0o777,
            0o400
        );
    }

    /// Trees published at the legacy readonly `0o500` directory mode stay
    /// verifiable objects, while any group- or other-accessible directory
    /// mode is rejected — the contract accepts exactly the two owner-only
    /// modes, so an old store keeps working and a loosened one does not.
    #[test]
    fn legacy_readonly_directories_verify_and_group_modes_do_not() {
        let (_, home) = sealed(&[
            ("index.js", b"console.log('index')\n", true),
            ("lib/util.js", b"module.exports = () => 42\n", false),
        ]);
        let (_store, root) = store_root();
        let scope = sha256(b"domain-legacy");
        let reference = import(&root, &scope, home.path());
        let tree = shared_tree_root(&root, &reference).unwrap();
        let restore = |mode: u32| {
            for directory in [tree.clone(), tree.join("lib")] {
                fs::set_permissions(&directory, fs::Permissions::from_mode(mode)).unwrap();
            }
        };
        restore(0o500);
        verify_shared_tree(&root, &reference).unwrap();
        restore(0o750);
        assert!(
            verify_shared_tree(&root, &reference).is_err(),
            "a group-accessible directory mode is drift"
        );
        restore(0o700);
        verify_shared_tree(&root, &reference).unwrap();
    }

    /// A changed tree version shares only the unchanged file's inode.
    #[test]
    fn changed_version_reuses_only_unchanged_payloads() {
        let (source, home) = sealed(&[
            ("keep.txt", b"stable\n", false),
            ("change.txt", b"v1\n", false),
        ]);
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
        let seat = drifted
            .iter()
            .position(|byte| *byte == b'p')
            .expect("digit");
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
        let destination = root.join("trees").join(&scope).join(sha256(&manifest));
        assert!(
            import_shared_tree(&root, &scope, home.path(), Path::new("assets/absent")).is_err()
        );
        assert!(!root.join("trees").exists());
        fs::write(home.path().join("assets/runtime/a.txt"), b"drifted\n").unwrap();
        assert!(
            import_shared_tree(&root, &scope, home.path(), Path::new("assets/runtime")).is_err()
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
        let destination = root.join("trees").join(&scope).join(sha256(&manifest));
        fs::create_dir_all(destination.parent().unwrap()).unwrap();
        fs::create_dir(&destination).unwrap();
        assert!(
            import_shared_tree(&root, &scope, home.path(), Path::new("assets/runtime")).is_err()
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
            import_shared_tree(
                &root,
                &scope,
                second_home.path(),
                Path::new("assets/runtime")
            )
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

    /// Two manifest entries naming one payload digest with different sizes
    /// are refused before anything is published, even when each source
    /// file matches its own entry's size: one blob identity cannot satisfy
    /// both. Same-digest duplicates with the same size still share a blob.
    #[test]
    fn conflicting_sizes_for_one_digest_are_refused() {
        let (_, home) = sealed(&[
            ("a.txt", b"payload\n", false),
            ("b.txt", b"payload plus more\n", false),
            ("c.txt", b"payload\n", false),
        ]);
        let tree = home.path().join("assets/runtime");
        let manifest_path = tree.join(SNAPSHOT_MANIFEST);
        let mut manifest: Value =
            serde_json::from_slice(&fs::read(&manifest_path).unwrap()).unwrap();
        let entries = manifest["entries"].as_array_mut().unwrap();
        let digest_a = entries
            .iter()
            .find(|entry| entry["path"] == "a.txt")
            .unwrap()["sha256"]
            .clone();
        for entry in entries.iter_mut() {
            if entry["path"] == "b.txt" {
                entry["sha256"] = digest_a.clone();
            }
        }
        fs::set_permissions(&manifest_path, fs::Permissions::from_mode(0o600)).unwrap();
        fs::write(&manifest_path, serde_json::to_vec(&manifest).unwrap()).unwrap();
        let (_store, root) = store_root();
        let scope = sha256(b"domain-conflict");
        let error = import_shared_tree(&root, &scope, home.path(), Path::new("assets/runtime"))
            .expect_err("conflicting sizes for one digest are refused");
        assert!(error.to_string().contains("size"), "{error}");
        assert!(
            !root.join("blobs").join(&scope).exists()
                || fs::read_dir(root.join("blobs").join(&scope))
                    .unwrap()
                    .next()
                    .is_none()
        );
        // Valid same-digest duplicates (a.txt and c.txt) keep sharing a blob.
        let (_, valid) = sealed(&[
            ("a.txt", b"payload\n", false),
            ("c.txt", b"payload\n", false),
        ]);
        let reference = import(&root, &scope, valid.path());
        let shared = shared_tree_root(&root, &reference).unwrap();
        assert_eq!(
            inode_of(&shared.join("a.txt")),
            inode_of(&shared.join("c.txt"))
        );
    }

    /// A source that drifts after its manifest was sealed fails before the
    /// durability barrier, so nothing is published: no canonical blob, no
    /// temporary blob and no tree — not even for the payloads that streamed
    /// correctly before the drifted one.
    #[test]
    fn source_drift_before_the_barrier_publishes_nothing() {
        let (_, home) = sealed(&[
            ("a.txt", b"first payload\n", false),
            ("b.txt", b"second payload\n", false),
        ]);
        let staged = home.path().join("assets/runtime/b.txt");
        fs::write(&staged, b"SECOND PAYLOAD\n").unwrap();
        let (_store, root) = store_root();
        let scope = sha256(b"domain-drift");
        assert!(
            import_shared_tree(&root, &scope, home.path(), Path::new("assets/runtime")).is_err()
        );
        let listing = |namespace: &str| -> Vec<String> {
            fs::read_dir(root.join(namespace).join(&scope))
                .map(|entries| {
                    entries
                        .map(|entry| entry.unwrap().file_name().to_string_lossy().into_owned())
                        .collect()
                })
                .unwrap_or_default()
        };
        assert!(listing("blobs").is_empty(), "{:?}", listing("blobs"));
        assert!(listing("trees").is_empty(), "{:?}", listing("trees"));
    }

    /// Leftovers of a publication interrupted after its first barrier — a
    /// canonical blob already renamed, a temporary blob and a staging tree —
    /// never block or poison a retry: the canonical blob is verified and
    /// reused by inode, the orphans are left for collection, and the retried
    /// tree verifies.
    #[test]
    fn interrupted_publication_leftovers_are_inert() {
        let (_, home) = sealed(&[
            ("a.txt", b"kept payload\n", false),
            ("b.txt", b"new payload\n", false),
        ]);
        let (_store, root) = store_root();
        let scope = sha256(b"domain-crash");
        let canonical = plain_blob(&root, &scope, b"kept payload\n");
        fs::create_dir_all(canonical.parent().unwrap()).unwrap();
        fs::write(&canonical, b"kept payload\n").unwrap();
        fs::set_permissions(&canonical, fs::Permissions::from_mode(0o400)).unwrap();
        let temporary = root
            .join("blobs")
            .join(&scope)
            .join(format!("{TEMP_PREFIX}crashed{TEMP_SUFFIX}"));
        fs::write(&temporary, b"torn").unwrap();
        let staging = root
            .join("trees")
            .join(&scope)
            .join(format!("{TEMP_PREFIX}crashed{TEMP_SUFFIX}"));
        fs::create_dir_all(&staging).unwrap();
        fs::write(staging.join("a.txt"), b"partial").unwrap();
        let reused = inode_of(&canonical);
        let reference = import(&root, &scope, home.path());
        verify_shared_tree(&root, &reference).unwrap();
        let tree = shared_tree_root(&root, &reference).unwrap();
        assert_eq!(
            inode_of(&tree.join("a.txt")),
            reused,
            "the canonical blob is reused"
        );
        assert!(
            temporary.is_file() && staging.is_dir(),
            "orphans are left for collection"
        );
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
