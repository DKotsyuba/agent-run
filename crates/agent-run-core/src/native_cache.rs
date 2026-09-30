//! Bounded packing and reference census for Codex's unindexed native
//! metadata-cache files, stored once inside the shared managed-asset store.
//!
//! Codex keeps two best-effort cold-start caches beneath a home
//! (`codex-rs/connectors/src/connector_runtime/persistence.rs`,
//! rust-v0.156.1): `cache/codex_apps_tools/<identity>.json` (disk schema 4)
//! and `cache/codex_apps_server_info/<identity>.json` (disk schema 1), each
//! bounded at 32 MiB, `<identity>` being the SHA-1 of the account key — a
//! 40-lowercase-hex filename that is independent of `CODEX_HOME` and is
//! never an account label. Native reads go through `File::open` (a file
//! symlink is followed), and native refresh writes a `NamedTempFile` inside
//! the private cache directory and atomically `persist`s it over the cache
//! path, which *replaces* that symlink and leaves any shared target
//! untouched. So one exact file symlink per cache entry preserves every
//! native read and write behavior; the cache directories themselves stay
//! real private directories, because native stages its temporary files
//! inside them.
//!
//! This unit publishes each eligible entry's bytes once, as an immutable
//! content-keyed FILE object inside the caller-owned shared store at
//! `<store>/native-cache/<scope>/<content-sha256>` — one fixed namespace
//! beside `trees/`, `blobs/` and `plugin-views/`, under the same
//! [`SharedStoreLock`] and the same guard that protects those. `<scope>` is
//! the SHA-256 of the exact preimage `native-cache-v1\n<cache dir>\n<schema>
//! \n<identity file>\n<effective uid>\n<compatibility domain>`, so payloads
//! never alias across file kind, native disk schema, native identity,
//! effective user, or the endpoint/harness compatibility domain the trusted
//! caller supplies. No tree payload is copied, and no index, history,
//! config, auth, plugin or data path is ever touched.
//!
//! Freshness is conservative and never fabricated: an object's content and
//! mode are immutable, and its modification time is the *minimum* of every
//! proven source mtime — set to the captured source mtime on first
//! publication, and afterwards only ever *lowered* under the publish lock,
//! never raised and never taken from the import wall clock. A reader
//! resolving its cache link sees data at least as old as its own proven
//! source, so cold-start freshness can only under-estimate, never fabricate.
//! Identical payloads supplied by many homes or runs keep exactly one
//! object; no per-run timestamp ever enters a name.
//!
//! Packing is not admission: the caller proves the home quiescent and the
//! identity binding through the existing store checks before calling. This
//! code refuses non-canonical homes, foreign-owned entries, and any home
//! that aliases the store or vice versa; it refuses a home whose finalized
//! managed index names a root, flat file or link overlapping the cache
//! directory (and one whose index cannot be parsed), it skips entries whose
//! known cache path is a symlink (the shape a converted immutable managed
//! root would leave) and entries that are unknown, oversized, malformed, or
//! foreign links — every skipped entry stays in place with an explicit
//! disposition, and an already-shared link is claimed only after its object
//! is re-verified. The final switch is one complete temporary link
//! atomically renamed over the original name, so an interrupted pack always
//! leaves the original file or the complete shared link. Reference
//! enumeration for the shared-store collector reports exact live physical
//! references with explicit complete/incomplete evidence: a scan that meets
//! I/O trouble, a bound, or a drifted or dangling in-namespace link is
//! `complete == false` — never an empty success — and
//! [`deletion_candidates`] returns nothing from incomplete evidence. This
//! unit never deletes anything; collection is a later integration that
//! consumes these censuses.

use crate::{fs, Result};
use agent_run_domain::{canonical::hex_digest, error::invalid, Error};
use agent_run_platform::shared_assets::{is_scope, SharedStoreLock, TEMP_PREFIX, TEMP_SUFFIX};
use agent_run_platform::snapshot_tree::RUNTIME_SNAPSHOT_INDEX;
use serde::Serialize;
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::{
    collections::{BTreeMap, BTreeSet},
    fs::FileTimes,
    io::{Read, Write},
    os::unix::fs::{MetadataExt, PermissionsExt},
    path::{Path, PathBuf},
    time::SystemTime,
};

/// Fixed namespace this unit owns beneath the validated shared-store root.
pub const NATIVE_CACHE_NAMESPACE: &str = "native-cache";
/// Native tools-cache directory relative to a Codex home.
pub const TOOLS_CACHE_DIR: &str = "cache/codex_apps_tools";
/// Native server-info-cache directory relative to a Codex home.
pub const SERVER_INFO_CACHE_DIR: &str = "cache/codex_apps_server_info";
/// Native tools disk-cache schema this unit accepts.
pub const TOOLS_SCHEMA_VERSION: u8 = 4;
/// Native server-info disk-cache schema this unit accepts.
pub const SERVER_INFO_SCHEMA_VERSION: u8 = 1;
/// Per-file payload bound the native reader itself enforces on both caches.
pub const NATIVE_CACHE_MAX_BYTES: u64 = 32 * 1024 * 1024;
/// Domain separator opening the scope-digest preimage.
const SCOPE_PREFIX: &str = "native-cache-v1";
/// Upper bound for one trusted compatibility-domain label, in UTF-8 bytes.
const MAX_DOMAIN_BYTES: usize = 256;
/// Upper bound on entries one bounded directory scan visits before the pass
/// reports itself incomplete.
const MAX_DIR_ENTRIES: usize = 4096;
/// Upper bound on entries the object census classifies before it reports
/// itself incomplete.
const MAX_NAMESPACE_ENTRIES: usize = 65_536;
/// Upper bound on homes one reference census accepts before it reports
/// itself incomplete.
const MAX_CENSUS_HOMES: usize = 4096;
/// Upper bound on live objects one reference census accumulates before it
/// reports itself incomplete.
const MAX_VERIFIED_OBJECTS: usize = 65_536;
/// Names one bounded directory read fetches at a time.
const SCAN_BATCH: usize = 256;
/// Streaming chunk for bounded reads and payload hashing.
const STREAM_CHUNK: usize = 64 * 1024;
/// Upper bound for one read of the home's managed snapshot index, matching
/// the platform metadata bound.
const MAX_INDEX_BYTES: usize = 64 * 1024;
/// Prefix of pack-owned temporary link names inside one native cache
/// directory; a fired fault or lost race can leave one recoverable orphan.
const BACKUP_PREFIX: &str = ".agent-run-native-";
/// Suffix of pack-owned temporary link names inside one native cache
/// directory.
const BACKUP_SUFFIX: &str = ".tmp";
/// Physical mode every published object carries: owner-read only.
const OBJECT_MODE: u32 = 0o400;

/// Returns the effective user id; every touched entry must be owned by it.
fn euid() -> u32 {
    // SAFETY: geteuid reads kernel credential state and retains nothing.
    unsafe { libc::geteuid() }
}

/// Returns `true` when `name` is exactly 40 lowercase hexadecimal digits,
/// the native identity-filename stem shape.
fn is_identity_stem(name: &str) -> bool {
    name.len() == 40
        && name
            .bytes()
            .all(|byte| matches!(byte, b'0'..=b'9' | b'a'..=b'f'))
}

/// Returns `true` when `name` is `<40 lowercase hex>.json`, the only cache
/// entry shape this unit ever packs or links.
fn is_identity_file(name: &str) -> bool {
    match name.strip_suffix(".json") {
        Some(stem) => is_identity_stem(stem),
        None => false,
    }
}

/// The two native metadata caches this unit knows, each bound to its own
/// fixed directory and native disk schema.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum NativeCacheKind {
    /// `cache/codex_apps_tools`, the connector-tools cache at disk schema 4.
    Tools,
    /// `cache/codex_apps_server_info`, the server-info cache at disk schema 1.
    ServerInfo,
}

impl NativeCacheKind {
    /// Returns the exact native cache directory, relative to a Codex home.
    pub fn cache_dir(self) -> &'static Path {
        match self {
            Self::Tools => Path::new(TOOLS_CACHE_DIR),
            Self::ServerInfo => Path::new(SERVER_INFO_CACHE_DIR),
        }
    }

    /// Returns the native disk schema this unit requires for the kind.
    pub fn schema_version(self) -> u8 {
        match self {
            Self::Tools => TOOLS_SCHEMA_VERSION,
            Self::ServerInfo => SERVER_INFO_SCHEMA_VERSION,
        }
    }
}

/// The trusted compatibility scope one pack runs under: which native cache
/// it covers, the effective user whose homes are packed, and the
/// caller-supplied endpoint/harness compatibility domain label. A bare
/// filename never proves compatibility across endpoints, so the label is
/// explicit input and is never derived from an account label.
///
/// [`Self::scope_digest`] folds these fields with each entry's native
/// identity filename into one 64-hex scope; payloads never alias across any
/// of them.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NativeCacheDomain {
    /// Which of the two native caches this domain covers.
    kind: NativeCacheKind,
    /// Native disk schema; fixed by `kind` at construction so a caller
    /// cannot claim a foreign schema.
    schema_version: u8,
    /// Effective user id captured at construction; every packed home, entry
    /// and store object must be owned by it.
    effective_uid: u32,
    /// Trusted endpoint/harness compatibility label; non-empty, at most
    /// [`MAX_DOMAIN_BYTES`] UTF-8 bytes.
    compatibility_domain: String,
}

impl NativeCacheDomain {
    /// Creates one domain for `kind` under `compatibility_domain`.
    ///
    /// The schema is fixed by `kind` and the effective uid is captured from
    /// the running process, so neither can drift from what the caller
    /// actually runs. Fails when the label is empty or exceeds
    /// [`MAX_DOMAIN_BYTES`] UTF-8 bytes.
    pub fn new(kind: NativeCacheKind, compatibility_domain: impl Into<String>) -> Result<Self> {
        let compatibility_domain = compatibility_domain.into();
        let bytes = compatibility_domain.len();
        if bytes == 0 || bytes > MAX_DOMAIN_BYTES {
            return Err(invalid(
                "native cache compatibility domain must hold 1..=256 UTF-8 bytes",
            ));
        }
        Ok(Self {
            kind,
            schema_version: kind.schema_version(),
            effective_uid: euid(),
            compatibility_domain,
        })
    }

    /// Returns the native cache kind this domain covers.
    pub fn kind(&self) -> NativeCacheKind {
        self.kind
    }

    /// Returns the native disk schema this domain requires.
    pub fn schema_version(&self) -> u8 {
        self.schema_version
    }

    /// Returns the effective user id captured at construction.
    pub fn effective_uid(&self) -> u32 {
        self.effective_uid
    }

    /// Returns the trusted compatibility label this domain was built with.
    pub fn compatibility_domain(&self) -> &str {
        &self.compatibility_domain
    }

    /// Derives the 64-hex scope one cache entry is stored under.
    ///
    /// `identity_file` must be exactly `<40 lowercase hex>.json` — the
    /// native SHA-1 of the account key, never an account label. The
    /// preimage is exactly `native-cache-v1\n<cache dir>\n<schema>\n<identity
    /// file>\n<uid decimal>\n<compatibility domain>`, hashed with SHA-256;
    /// every field is validated first so a malformed name can never steer a
    /// store path.
    pub fn scope_digest(&self, identity_file: &str) -> Result<String> {
        if !is_identity_file(identity_file) {
            return Err(invalid(
                "native cache identity file must be 40 lowercase hex digits plus .json",
            ));
        }
        let preimage = format!(
            "{SCOPE_PREFIX}\n{}\n{}\n{identity_file}\n{}\n{}",
            self.kind
                .cache_dir()
                .to_str()
                .expect("static relative path"),
            self.schema_version,
            self.effective_uid,
            self.compatibility_domain
        );
        Ok(hex_digest(&Sha256::digest(preimage.as_bytes())))
    }
}

/// Returns the validated canonical form of one trusted absolute real
/// directory, refusing any path a symlinked component would alias.
fn validated_dir(path: &Path, label: &str) -> Result<PathBuf> {
    if !path.is_absolute() {
        return Err(invalid(format!("{label} must be an absolute path")));
    }
    let canonical = path
        .canonicalize()
        .map_err(|_| invalid(format!("{label} must be an existing real directory")))?;
    if canonical != path {
        return Err(invalid(format!(
            "{label} must be canonical without symlinked components"
        )));
    }
    Ok(canonical)
}

/// Returns the absolute object path `<store>/native-cache/<scope>/<digest>`
/// after validating both 64-hex digest components, deriving every component
/// from the trusted root rather than from caller paths.
pub fn native_cache_object_path(
    store_root: &Path,
    scope: &str,
    content_sha256: &str,
) -> Result<PathBuf> {
    if !is_scope(scope) || !is_scope(content_sha256) {
        return Err(invalid(
            "native cache object names must be 64 lowercase hexadecimal digits",
        ));
    }
    Ok(store_root
        .join(NATIVE_CACHE_NAMESPACE)
        .join(scope)
        .join(content_sha256))
}

/// What one pack did with one cache entry. Every variant except
/// [`Self::Packed`] and [`Self::AlreadyShared`] leaves the entry exactly as
/// it was found.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(tag = "disposition", rename_all = "snake_case")]
pub enum NativeCacheDisposition {
    /// The entry's captured bytes were published once and the home entry is
    /// now one exact symlink onto the named store object. `lowered_mtime`
    /// is set when an existing object's mtime was lowered to this source's
    /// older proven mtime.
    Packed {
        /// Absolute path of the immutable store object the entry now links to.
        object: PathBuf,
        /// SHA-256 of the captured bytes, also the object's final name.
        content_sha256: String,
        /// Whether an existing object's mtime was lowered to this source's.
        lowered_mtime: bool,
    },
    /// The entry is already one exact in-namespace link for this scope whose
    /// object was re-verified (owner, mode and content digest) before reuse
    /// was claimed.
    AlreadyShared,
    /// The known cache path is a symlink — the shape a converted immutable
    /// managed root would leave — so the whole directory is skipped
    /// untouched.
    ManagedRootOverlap,
    /// The home's finalized managed index names a root, flat managed file or
    /// managed link overlapping the cache directory, so packing — which
    /// would rewrite indexed material — is refused for the whole directory.
    /// `entry` names the overlapping index path.
    ManagedIndexOverlap {
        /// The indexed root, file or link path that overlaps the cache.
        entry: String,
    },
    /// The home holds a managed index this unit cannot read and parse as the
    /// finalized v1 schema; the whole directory is left untouched, because
    /// overlap cannot be proven otherwise.
    ManagedIndexUnreadable,
    /// The entry name is not the native identity shape; retained untouched.
    UnsupportedName,
    /// The entry is not a regular file (directory, socket, …); retained.
    UnsupportedEntry,
    /// The entry is owned by another user; retained untouched.
    ForeignOwner,
    /// The entry exceeds the native 32 MiB cache bound; retained untouched.
    Oversized {
        /// The entry's size in bytes.
        bytes: u64,
    },
    /// The bytes are not the kind's native JSON disk schema; retained.
    Malformed,
    /// The entry is a symlink somewhere other than this unit's exact object
    /// path for this scope; retained untouched and never followed.
    ForeignLink,
    /// The source changed identity while it was captured; the original
    /// stays in place untouched.
    ChangedDuringCapture,
    /// An object already holds this content-addressed name but fails
    /// verification (owner, mode or content drifted); nothing was linked.
    ExistingObjectDrifted,
    /// The final link could not be installed; the original entry stays in
    /// place — an interrupted switch leaves it plus one recoverable
    /// `.agent-run-native-*.tmp` link orphan this unit never deletes — and
    /// `detail` reports what failed.
    LinkFailed {
        /// Bounded description of the failure.
        detail: String,
    },
}

/// One classified cache entry.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct NativeCacheEntry {
    /// Absolute path of the entry inside its home.
    pub path: PathBuf,
    /// What the pack did with it.
    pub disposition: NativeCacheDisposition,
}

/// The outcome of one pack over one native cache directory of one home.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct NativeCacheReport {
    /// The canonical home that was packed.
    pub home: PathBuf,
    /// The native cache directory, relative to the home, this report covers.
    pub cache_dir: PathBuf,
    /// Every entry examined, in directory order.
    pub entries: Vec<NativeCacheEntry>,
    /// `false` when the scan stopped at the entry bound, or lost a stream,
    /// before exhausting the directory; a later pass resumes.
    pub complete: bool,
}

/// Why publication of one entry's bytes could not complete.
#[derive(Debug)]
enum PublishFailure {
    /// The store itself could not be written or read.
    Io(Error),
    /// An existing object failed verification, so it must not be linked.
    Drift,
}

/// The captured no-follow identity of one open source descriptor: the exact
/// inode whose bytes were read, its length, and its modification time.
struct Captured {
    /// Device half of the inode identity.
    device: u64,
    /// Inode half of the inode identity.
    inode: u64,
    /// File length in bytes at capture.
    length: u64,
    /// Modification time at capture, nanosecond precision.
    modified: SystemTime,
}

/// Captures one open descriptor's identity, size and mtime.
fn captured_of(file: &std::fs::File) -> Result<Captured> {
    let metadata = file.metadata()?;
    Ok(Captured {
        device: metadata.dev(),
        inode: metadata.ino(),
        length: metadata.len(),
        modified: metadata.modified()?,
    })
}

/// Returns whether the descriptor still holds the exact captured identity —
/// the same inode, length and mtime — so the bytes read from it are
/// provably the source's current content. A native refresh replaces the
/// inode by rename, which this catches; a hostile same-uid rewrite that
/// forges mtime is outside an owner-controlled home's threat model.
fn still_holds(file: &std::fs::File, captured: &Captured) -> Result<bool> {
    let metadata = file.metadata()?;
    Ok(metadata.dev() == captured.device
        && metadata.ino() == captured.inode
        && metadata.len() == captured.length
        && metadata.modified()? == captured.modified)
}

/// Reads one open descriptor's bytes through bounded chunks, hashing as it
/// goes, and returns them with their SHA-256. Fails when the stream exceeds
/// the native 32 MiB cache bound, so a growing source can never be buffered
/// without a proven ceiling.
fn read_bounded(file: &mut std::fs::File) -> Result<(Vec<u8>, String)> {
    let mut hasher = Sha256::new();
    let mut bytes = Vec::new();
    let mut buffer = vec![0_u8; STREAM_CHUNK];
    loop {
        let read = file.read(&mut buffer)?;
        if read == 0 {
            return Ok((bytes, hex_digest(&hasher.finalize())));
        }
        hasher.update(&buffer[..read]);
        bytes.extend_from_slice(&buffer[..read]);
        if bytes.len() as u64 > NATIVE_CACHE_MAX_BYTES {
            return Err(invalid("native cache file exceeds the payload bound"));
        }
    }
}

/// Returns whether the parsed document is the kind's native disk cache: a
/// JSON object whose `schema_version` equals the native constant. All other
/// payload fields are opaque native data and are never interpreted here.
fn native_schema_holds(document: &Value, schema_version: u8) -> bool {
    document.as_object().is_some_and(|object| {
        object
            .get("schema_version")
            .and_then(Value::as_u64)
            .is_some_and(|version| version == u64::from(schema_version))
    })
}

/// Verifies one existing store object for reuse: an owned regular file at
/// the immutable physical mode whose content still hashes to its own name.
/// Returns its current mtime on success.
fn verify_object(store: &fs::Dir, relative: &Path, content_sha256: &str) -> Result<SystemTime> {
    let mut file = store.open_file(relative)?;
    let metadata = file.metadata()?;
    if metadata.uid() != euid() {
        return Err(invalid("native cache object has a foreign owner"));
    }
    if metadata.permissions().mode() & 0o7777 != OBJECT_MODE {
        return Err(invalid("native cache object mode drifted"));
    }
    let (_, digest) = read_bounded(&mut file)?;
    if digest != content_sha256 {
        return Err(invalid("native cache object content drifted"));
    }
    metadata.modified().map_err(Error::from)
}

/// Publishes the captured bytes as the scope's immutable content-keyed
/// object and returns the object's store-relative path plus whether an
/// existing object's mtime was lowered to the captured source mtime.
///
/// Publication is exclusive and no-replace: a fresh temporary is written,
/// synced, restricted to [`OBJECT_MODE`], stamped with the *source's*
/// captured mtime — never the wall clock — and renamed without replacing
/// anything. A pre-existing object is verified and reused, and the only
/// freshness change ever applied afterwards is *lowering* its mtime to an
/// older proven source mtime, so one payload keeps exactly one object no
/// matter how many homes or runs supply it. Content and mode stay immutable
/// throughout. The caller holds the store publish lock.
fn publish_object(
    store: &fs::Dir,
    scope: &str,
    bytes: &[u8],
    content_sha256: &str,
    source_mtime: SystemTime,
) -> std::result::Result<(PathBuf, bool), PublishFailure> {
    let scope_rel = Path::new(NATIVE_CACHE_NAMESPACE).join(scope);
    store.directory(&scope_rel).map_err(PublishFailure::Io)?;
    let object_rel = scope_rel.join(content_sha256);
    let mut lowered = false;
    match store.entry_type(&object_rel) {
        Err(Error::Io(error)) if error.kind() == std::io::ErrorKind::NotFound => {
            let temporary = scope_rel.join(format!(
                "{TEMP_PREFIX}{}{TEMP_SUFFIX}",
                uuid::Uuid::new_v4().simple()
            ));
            let staged = (|| -> Result<()> {
                let mut file = store.create_exclusive(&temporary, OBJECT_MODE)?;
                file.set_permissions(std::fs::Permissions::from_mode(OBJECT_MODE))?;
                file.write_all(bytes)?;
                // The proven source mtime is stamped and flushed before the
                // object becomes reachable under its final name, so a crash
                // can never leave a freshly-stamped object for stale bytes.
                file.set_times(FileTimes::new().set_modified(source_mtime))?;
                file.sync_all()?;
                Ok(())
            })();
            let moved =
                staged.and_then(|()| store.rename_entry_no_replace(&temporary, &object_rel));
            match moved {
                Ok(true) => {}
                Ok(false) => {
                    let _ = store.remove(&temporary);
                    lowered = lower_object_mtime(store, &object_rel, source_mtime)?;
                }
                Err(error) => {
                    let _ = store.remove(&temporary);
                    return Err(PublishFailure::Io(error));
                }
            }
        }
        Ok(fs::EntryType::File) => {
            lowered = lower_object_mtime(store, &object_rel, source_mtime)?;
        }
        Ok(_) => {
            return Err(PublishFailure::Io(invalid(
                "native cache object exists with an unexpected type",
            )));
        }
        Err(error) => return Err(PublishFailure::Io(error)),
    }
    Ok((object_rel, lowered))
}

/// Lowers one verified existing object's mtime to `source_mtime` when that
/// timestamp is strictly older, returning whether a change was applied.
/// Raising is impossible by construction, so no import can fabricate
/// freshness; content and mode are never touched. The lowered timestamp is
/// flushed before the caller may link any home entry onto the object, so a
/// crash can never publish a freshly-stamped object for stale bytes.
fn lower_object_mtime(
    store: &fs::Dir,
    object_rel: &Path,
    source_mtime: SystemTime,
) -> std::result::Result<bool, PublishFailure> {
    let content_sha256 = object_rel
        .file_name()
        .and_then(|name| name.to_str())
        .ok_or_else(|| PublishFailure::Drift)?
        .to_owned();
    let current =
        verify_object(store, object_rel, &content_sha256).map_err(|error| match error {
            Error::Io(error) => PublishFailure::Io(Error::Io(error)),
            _ => PublishFailure::Drift,
        })?;
    if source_mtime >= current {
        return Ok(false);
    }
    let file = store.open_file(object_rel).map_err(PublishFailure::Io)?;
    file.set_times(FileTimes::new().set_modified(source_mtime))
        .map_err(|error| PublishFailure::Io(Error::Io(error)))?;
    file.sync_all()
        .map_err(|error| PublishFailure::Io(Error::Io(error)))?;
    Ok(true)
}

/// Packs one native cache directory of one caller-proven quiescent owned
/// home into the shared store.
///
/// `store_root` must be the canonical owner-controlled shared-store root
/// (callers derive it with [`crate::runtime_storage::store_root`]) and
/// `home` an existing canonical real directory owned by the effective user
/// that aliases neither the store nor is aliased by it. Admission —
/// quiescence and the account/identity binding — is the caller's; this
/// function owns only the filesystem safety envelope. Before anything is
/// touched, a finalized managed index in the home is parsed read-only and
/// any indexed root, flat managed file or managed link overlapping the
/// cache directory refuses the whole pack, and a symlinked cache path is
/// refused the same way. Every mutation holds the store-wide
/// [`SharedStoreLock`]: bytes are published as immutable objects first,
/// then the home entry is swapped by one complete temporary symlink
/// atomically renamed over the original name, so an interrupted or racing
/// pack always leaves either the original file or the complete shared link
/// under the native name — plus, at worst, one recoverable
/// `.agent-run-native-*.tmp` link orphan this unit never deletes. Unknown,
/// oversized, malformed, foreign-owned or foreign-linked entries are
/// preserved untouched with explicit dispositions, and reuse of an
/// already-shared link is claimed only after its object is re-verified.
/// The report's `complete` is `false` when the directory scan hit its entry
/// bound or lost its stream. Nothing outside the one cache directory, this
/// unit's own temporary link names, and `native-cache/<scope>/` is ever
/// touched.
pub fn pack_native_cache(
    store_root: &Path,
    home: &Path,
    domain: &NativeCacheDomain,
) -> Result<NativeCacheReport> {
    pack_native_cache_with_fault(store_root, home, domain, None)
}

/// One simulated crash point inside [`pack_native_cache_with_fault`]'s link
/// switch; production callers never pass a `fault`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NativeCacheFault {
    /// After the complete temporary link exists and before it is atomically
    /// renamed over the original name — the original entry is still intact.
    BeforeLinkSwap,
    /// After the atomic rename committed the shared link over the original
    /// name.
    AfterLinkSwap,
}

/// Test seam over [`pack_native_cache`] firing `fault` at one
/// [`NativeCacheFault`] crash point; no production caller passes `Some`.
/// A fired fault aborts the pack like a process death at that instruction,
/// leaving exactly the durable state a real crash would.
pub fn pack_native_cache_with_fault(
    store_root: &Path,
    home: &Path,
    domain: &NativeCacheDomain,
    fault: Option<&dyn Fn(NativeCacheFault) -> Result<()>>,
) -> Result<NativeCacheReport> {
    let root = validated_dir(store_root, "shared store root")?;
    let home = validated_dir(home, "native cache home")?;
    if home == root || home.starts_with(&root) || root.starts_with(&home) {
        return Err(invalid(
            "native cache home and shared store root must not alias each other",
        ));
    }
    let store = fs::Dir::open(&root)?;
    if store.entry(None)?.uid != euid() {
        return Err(invalid("shared store root is not owned by this user"));
    }
    let home_dir = fs::Dir::open(&home)?;
    if home_dir.entry(None)?.uid != euid() {
        return Err(invalid("native cache home is not owned by this user"));
    }
    let cache_dir = domain.kind.cache_dir();
    let mut report = NativeCacheReport {
        cache_dir: cache_dir.to_path_buf(),
        home: home.clone(),
        entries: Vec::new(),
        complete: true,
    };
    // An index that cannot be proven free of overlap pins the directory:
    // nothing is mutated, because an indexed file or link beneath the cache
    // must never be rewritten by a pack.
    match managed_index_overlap(&home_dir, cache_dir) {
        Ok(Some(entry)) => {
            report.entries.push(NativeCacheEntry {
                path: home.join(cache_dir),
                disposition: NativeCacheDisposition::ManagedIndexOverlap { entry },
            });
            return Ok(report);
        }
        Err(_) => {
            report.entries.push(NativeCacheEntry {
                path: home.join(cache_dir),
                disposition: NativeCacheDisposition::ManagedIndexUnreadable,
            });
            return Ok(report);
        }
        Ok(None) => {}
    }
    let _guard = SharedStoreLock::acquire(&root)?;
    let Some(cache) = open_native_cache_dir(&home_dir, cache_dir, &mut report) else {
        return Ok(report);
    };
    let mut scan = BoundedNames::open(&cache)?;
    while let Some(name) = scan.next(&mut report.complete)? {
        if report.entries.len() >= MAX_DIR_ENTRIES {
            report.complete = false;
            break;
        }
        let disposition = match name.to_str() {
            Some(name) => pack_entry(&root, &store, &cache, domain, name, fault),
            None => NativeCacheDisposition::UnsupportedName,
        };
        report.entries.push(NativeCacheEntry {
            path: home.join(cache_dir).join(&name),
            disposition,
        });
    }
    Ok(report)
}

/// Parses the home's finalized managed index read-only and returns the one
/// indexed root, flat managed file or managed link path overlapping
/// `cache_dir`, if any. `Ok(None)` means provably no overlap (including a
/// home with no index at all); any read or parse failure is an `Err`, so
/// the caller refuses the pack instead of guessing. The index bytes are
/// never written, moved or re-hashed by this unit.
fn managed_index_overlap(home_dir: &fs::Dir, cache_dir: &Path) -> Result<Option<String>> {
    let Some(raw) = home_dir.optional(Path::new(RUNTIME_SNAPSHOT_INDEX), MAX_INDEX_BYTES)? else {
        return Ok(None);
    };
    let document: Value =
        serde_json::from_slice(&raw).map_err(|_| invalid("runtime snapshot index is malformed"))?;
    if !document.as_object().is_some_and(|object| {
        object
            .get("snapshot_index_version")
            .and_then(Value::as_u64)
            .is_some_and(|version| version == 1)
    }) {
        return Err(invalid("runtime snapshot index is malformed"));
    }
    let mut indexed = Vec::new();
    for key in ["roots", "files", "links"] {
        let entries = document
            .get(key)
            .and_then(Value::as_array)
            .ok_or_else(|| invalid("runtime snapshot index is malformed"))?;
        for entry in entries {
            let path = if key == "roots" {
                entry.as_str()
            } else {
                entry
                    .as_object()
                    .and_then(|entry| entry.get("path"))
                    .and_then(Value::as_str)
            };
            let path = path.ok_or_else(|| invalid("runtime snapshot index is malformed"))?;
            indexed.push(path.to_owned());
        }
    }
    let cache = cache_dir
        .to_str()
        .ok_or_else(|| invalid("native cache directory is not UTF-8"))?;
    Ok(indexed.into_iter().find(|path| paths_overlap(cache, path)))
}

/// Returns whether two `/`-separated relative home paths name the same entry
/// or one contains the other, so an indexed tree, file or link anywhere
/// across the cache boundary counts as an overlap.
fn paths_overlap(cache: &str, indexed: &str) -> bool {
    cache == indexed
        || cache.starts_with(&format!("{indexed}/"))
        || indexed.starts_with(&format!("{cache}/"))
}

/// Opens the home's native cache directory through no-follow descriptors,
/// recording an explicit disposition for a missing, linked or unswitchable
/// path. Returns `None` when this pack must not scan anything; the only
/// silently-empty case is a home without the directory at all.
fn open_native_cache_dir(
    home_dir: &fs::Dir,
    cache_dir_rel: &Path,
    report: &mut NativeCacheReport,
) -> Option<fs::Dir> {
    // Both native caches live beneath one real `cache` parent; a symlink
    // anywhere on the way is the shape of an immutable managed root or a
    // foreign alias, never a writable native cache directory.
    for ancestor in [Path::new("cache"), cache_dir_rel] {
        match home_dir.entry_type(ancestor) {
            Ok(fs::EntryType::Directory) => {}
            Err(Error::Io(error)) if error.kind() == std::io::ErrorKind::NotFound => {
                return None;
            }
            Ok(fs::EntryType::Symlink) => {
                report.entries.push(NativeCacheEntry {
                    path: report.home.join(ancestor),
                    disposition: NativeCacheDisposition::ManagedRootOverlap,
                });
                return None;
            }
            Ok(_) => {
                report.entries.push(NativeCacheEntry {
                    path: report.home.join(ancestor),
                    disposition: NativeCacheDisposition::UnsupportedEntry,
                });
                return None;
            }
            Err(_) => {
                report.complete = false;
                return None;
            }
        }
    }
    match home_dir.subdir(cache_dir_rel) {
        Ok(directory) => Some(directory),
        Err(_) => {
            report.complete = false;
            None
        }
    }
}

/// Packs exactly one entry and returns its disposition, preserving the
/// original on every outcome except a successful link swap. `fault` is the
/// optional crash seam of [`pack_native_cache_with_fault`].
fn pack_entry(
    root: &Path,
    store: &fs::Dir,
    cache: &fs::Dir,
    domain: &NativeCacheDomain,
    name: &str,
    fault: Option<&dyn Fn(NativeCacheFault) -> Result<()>>,
) -> NativeCacheDisposition {
    let entry = match cache.entry(Some(Path::new(name))) {
        Ok(entry) => entry,
        Err(_) => return NativeCacheDisposition::UnsupportedEntry,
    };
    if entry.kind == fs::EntryType::Symlink {
        let target = match cache.read_link(Path::new(name)) {
            Ok(Some(target)) => target,
            _ => return NativeCacheDisposition::ForeignLink,
        };
        // Shape alone never proves reuse: the exact target must still verify
        // as this scope's live immutable object before AlreadyShared.
        return match own_object_target(root, domain, name, &target) {
            Some((scope, content)) => {
                let relative = Path::new(NATIVE_CACHE_NAMESPACE)
                    .join(&scope)
                    .join(&content);
                match verify_object(store, &relative, &content) {
                    Ok(_) => NativeCacheDisposition::AlreadyShared,
                    Err(Error::Io(error)) => NativeCacheDisposition::LinkFailed {
                        detail: error.to_string().chars().take(256).collect(),
                    },
                    Err(_) => NativeCacheDisposition::ExistingObjectDrifted,
                }
            }
            None => NativeCacheDisposition::ForeignLink,
        };
    }
    if entry.kind != fs::EntryType::File {
        return NativeCacheDisposition::UnsupportedEntry;
    }
    if entry.uid != euid() {
        return NativeCacheDisposition::ForeignOwner;
    }
    if !is_identity_file(name) {
        return NativeCacheDisposition::UnsupportedName;
    }
    let mut source = match cache.open_file(Path::new(name)) {
        Ok(file) => file,
        Err(_) => return NativeCacheDisposition::UnsupportedEntry,
    };
    let captured = match captured_of(&source) {
        Ok(captured) => captured,
        Err(_) => return NativeCacheDisposition::UnsupportedEntry,
    };
    if captured.length > NATIVE_CACHE_MAX_BYTES {
        return NativeCacheDisposition::Oversized {
            bytes: captured.length,
        };
    }
    let (bytes, content_sha256) = match read_bounded(&mut source) {
        Ok(read) => read,
        Err(_) => return NativeCacheDisposition::Malformed,
    };
    let document: Value = match serde_json::from_slice(&bytes) {
        Ok(document) => document,
        Err(_) => return NativeCacheDisposition::Malformed,
    };
    if !native_schema_holds(&document, domain.schema_version) {
        return NativeCacheDisposition::Malformed;
    }
    // Prove the captured descriptor still holds the exact identity whose
    // bytes were read, and that the name still resolves to that inode.
    match (
        still_holds(&source, &captured),
        cache.entry(Some(Path::new(name))),
    ) {
        (Ok(true), Ok(live))
            if live.kind == fs::EntryType::File
                && live.device == captured.device
                && live.inode == captured.inode => {}
        (Err(_), _) | (_, Err(_)) => return NativeCacheDisposition::UnsupportedEntry,
        _ => return NativeCacheDisposition::ChangedDuringCapture,
    }
    let scope = match domain.scope_digest(name) {
        Ok(scope) => scope,
        Err(_) => return NativeCacheDisposition::UnsupportedName,
    };
    let (object_rel, lowered_mtime) =
        match publish_object(store, &scope, &bytes, &content_sha256, captured.modified) {
            Ok(published) => published,
            Err(PublishFailure::Drift) => return NativeCacheDisposition::ExistingObjectDrifted,
            Err(PublishFailure::Io(error)) => {
                return NativeCacheDisposition::LinkFailed {
                    detail: error.to_string().chars().take(256).collect(),
                };
            }
        };
    swap_for_link(
        cache,
        name,
        &root.join(&object_rel),
        captured,
        lowered_mtime,
        &content_sha256,
        fault,
    )
}

/// Parses `target` as exactly the in-namespace object path this domain
/// derives for `name`, returning its validated `(scope, content)` digest
/// pair for verification. Any other target, in-namespace or not, is `None`
/// — foreign — and is never followed.
fn own_object_target(
    root: &Path,
    domain: &NativeCacheDomain,
    name: &str,
    target: &Path,
) -> Option<(String, String)> {
    let scope = target
        .parent()
        .and_then(|parent| parent.file_name())
        .and_then(|value| value.to_str())?;
    let content = target.file_name().and_then(|value| value.to_str())?;
    let namespace = root.join(NATIVE_CACHE_NAMESPACE);
    if target.parent().and_then(Path::parent) != Some(namespace.as_path()) {
        return None;
    }
    let own_scope = domain.scope_digest(name).ok()?;
    (scope == own_scope && is_scope(content)).then(|| (scope.to_owned(), content.to_owned()))
}

/// Switches one packed entry onto `object` with one atomic replace.
///
/// The complete symlink is created first under this unit's fresh
/// `.agent-run-native-*.tmp` name and then atomically renamed over the
/// original name, so under the caller-proven quiescence every observer —
/// including a crash at either fault point — sees either the original file
/// or the complete shared link, never a missing name and never a partial
/// link. A native rewrite that lands between the captured-identity checks
/// and the replace wins nothing: the name is re-verified to still hold the
/// exact captured inode immediately before the move, and any mismatch keeps
/// the native file live (`ChangedDuringCapture`) and removes this unit's
/// temporary. A failed move removes the temporary as well; only a fired
/// fault (a simulated death) leaves it behind as the documented recoverable
/// orphan, which later packs classify as a foreign link and retain.
fn swap_for_link(
    cache: &fs::Dir,
    name: &str,
    object: &Path,
    captured: Captured,
    lowered_mtime: bool,
    content_sha256: &str,
    fault: Option<&dyn Fn(NativeCacheFault) -> Result<()>>,
) -> NativeCacheDisposition {
    let temporary = format!(
        "{BACKUP_PREFIX}{}{BACKUP_SUFFIX}",
        uuid::Uuid::new_v4().simple()
    );
    let holds_captured = |live: fs::Entry| {
        live.kind == fs::EntryType::File
            && live.device == captured.device
            && live.inode == captured.inode
    };
    let live = match cache.entry(Some(Path::new(name))) {
        Ok(live) => live,
        Err(_) => return NativeCacheDisposition::ChangedDuringCapture,
    };
    if !holds_captured(live) {
        return NativeCacheDisposition::ChangedDuringCapture;
    }
    if cache.symlink(object, Path::new(&temporary)).is_err() {
        return NativeCacheDisposition::LinkFailed {
            detail: "temporary link could not be created".into(),
        };
    }
    // One last identity check: a native refresh that already replaced the
    // name wins and must stay live.
    match cache.entry(Some(Path::new(name))) {
        Ok(live) if holds_captured(live) => {}
        Ok(_) => {
            let _ = cache.remove(Path::new(&temporary));
            return NativeCacheDisposition::ChangedDuringCapture;
        }
        Err(_) => {
            let _ = cache.remove(Path::new(&temporary));
            return NativeCacheDisposition::ChangedDuringCapture;
        }
    }
    let fire = |point: NativeCacheFault| -> Result<()> {
        match fault {
            Some(fault) => fault(point),
            None => Ok(()),
        }
    };
    let interrupted = |stage: &str, error: Error| NativeCacheDisposition::LinkFailed {
        detail: format!("{stage}: {error}").chars().take(256).collect(),
    };
    if let Err(error) = fire(NativeCacheFault::BeforeLinkSwap) {
        // A simulated death here leaves the original plus the temporary
        // link orphan exactly as a real crash would; nothing is cleaned up.
        return interrupted("interrupted before the link swap", error);
    }
    if let Err(error) = cache.rename_entry_replace(Path::new(&temporary), Path::new(name)) {
        let _ = cache.remove(Path::new(&temporary));
        return interrupted("link swap failed", error);
    }
    if let Err(error) = fire(NativeCacheFault::AfterLinkSwap) {
        return interrupted("interrupted after the link swap committed", error);
    }
    NativeCacheDisposition::Packed {
        object: object.to_path_buf(),
        content_sha256: content_sha256.to_owned(),
        lowered_mtime,
    }
}

/// The exact live-reference evidence of one reference census.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
pub struct NativeCacheRefCensus {
    /// Every store object proven live, mapped to the absolute home cache
    /// paths whose exact symlink currently resolves onto it.
    pub references: BTreeMap<PathBuf, Vec<PathBuf>>,
    /// Exact in-namespace links whose target object is missing from the
    /// store, as `(home path, target)`; not live references.
    pub dangling: Vec<(PathBuf, PathBuf)>,
    /// Exact in-namespace links whose target object exists but fails
    /// verification (owner, mode or content digest), as `(home path, target)`.
    pub drifted: Vec<(PathBuf, PathBuf)>,
    /// Cache symlinks pointing anywhere other than this unit's exact
    /// namespace spelling, as `(home path, target)`; retained, never
    /// followed, and never treated as proof of no reference — a relative or
    /// foreign link may resolve onto a live store object, so every entry
    /// here forces `complete == false`.
    pub foreign: Vec<(PathBuf, PathBuf)>,
    /// Directories or homes the census could not enumerate, with the
    /// failure; every entry here forces `complete == false`.
    pub unscanned: Vec<(PathBuf, String)>,
    /// `true` only when every supplied home was enumerated to the end of
    /// both cache directories inside every bound. A collector must treat
    /// `false` as "no object may be deleted", never as an empty reference
    /// set: I/O failure and parse failure are uncertainty, not proof.
    pub complete: bool,
}

impl NativeCacheRefCensus {
    /// Returns whether `object`, an absolute canonical store path, is proved
    /// live by this census. Only meaningful when `complete` is `true`.
    pub fn references(&self, object: &Path) -> bool {
        self.references.contains_key(object)
    }
}

/// Enumerates and verifies the exact live native-cache references of every
/// supplied home for the shared-store collector.
///
/// Each home's two known cache directories are scanned through no-follow
/// descriptors; only entries that are symlinks whose target is *exactly*
/// `<trusted store root>/native-cache/<64 hex>/<64 hex>` are considered,
/// and such a reference counts as live only when the target object exists,
/// is owned, carries the immutable mode, and its bytes hash to its own
/// name. Nothing outside the trusted root, the two known cache paths, and
/// those verified digests is ever followed or opened. Physical references
/// suffice: a complete census over every retained and extant protected home
/// is exactly the live set. Any I/O failure, unopenable home, unreadable
/// directory, or bound hit records the failure and forces
/// `complete == false` — never an empty success.
pub fn collect_native_cache_references(
    store_root: &Path,
    homes: &[PathBuf],
) -> Result<NativeCacheRefCensus> {
    let root = validated_dir(store_root, "shared store root")?;
    let store = fs::Dir::open(&root)?;
    let namespace = root.join(NATIVE_CACHE_NAMESPACE);
    let mut census = NativeCacheRefCensus {
        complete: true,
        ..Default::default()
    };
    if homes.len() > MAX_CENSUS_HOMES {
        census.complete = false;
        return Ok(census);
    }
    for home in homes {
        let canonical = match validated_dir(home, "native cache home") {
            Ok(canonical) => canonical,
            Err(_) => {
                census.unscanned.push((
                    home.clone(),
                    "home is not a canonical real directory".into(),
                ));
                census.complete = false;
                continue;
            }
        };
        let home_dir = match fs::Dir::open(&canonical) {
            Ok(directory) => directory,
            Err(error) => {
                census.unscanned.push((canonical, error.to_string()));
                census.complete = false;
                continue;
            }
        };
        for kind in [NativeCacheKind::Tools, NativeCacheKind::ServerInfo] {
            collect_cache_references(
                &home_dir,
                &canonical,
                kind.cache_dir(),
                &store,
                &namespace,
                &mut census,
            )?;
        }
    }
    if census.references.len() > MAX_VERIFIED_OBJECTS {
        census.complete = false;
    }
    Ok(census)
}

/// Scans one home's one known cache directory for exact live references,
/// recording every anomaly in `census`.
fn collect_cache_references(
    home_dir: &fs::Dir,
    home: &Path,
    cache_dir: &Path,
    store: &fs::Dir,
    namespace: &Path,
    census: &mut NativeCacheRefCensus,
) -> Result<()> {
    let cache = match home_dir.subdir(cache_dir) {
        Ok(directory) => directory,
        Err(Error::Io(error)) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(error) => {
            census
                .unscanned
                .push((home.join(cache_dir), error.to_string()));
            census.complete = false;
            return Ok(());
        }
    };
    let mut scanned = 0_usize;
    let mut scan = BoundedNames::open(&cache)?;
    while let Some(name) = scan.next(&mut census.complete)? {
        scanned += 1;
        if scanned > MAX_DIR_ENTRIES {
            census.complete = false;
            break;
        }
        let path = home.join(cache_dir).join(&name);
        match cache.entry_type(Path::new(&name)) {
            Ok(fs::EntryType::Symlink) => {}
            Ok(_) => continue,
            Err(error) => {
                census.unscanned.push((path, error.to_string()));
                census.complete = false;
                continue;
            }
        }
        let target = match cache.read_link(Path::new(&name)) {
            Ok(Some(target)) => target,
            Ok(None) => continue,
            Err(error) => {
                census.unscanned.push((path, error.to_string()));
                census.complete = false;
                continue;
            }
        };
        let Some((scope, content)) = parse_object_target(namespace, &target) else {
            // An unrecognized link — relative, foreign, or malformed — is
            // unknown reference evidence, not proof of no reference: it may
            // resolve onto a store object this census would otherwise offer
            // as a deletion candidate. The link is recorded and never
            // followed or rewritten; the census turns incomplete so the
            // collector retains until the anomaly is resolved.
            census.foreign.push((path, target));
            census.complete = false;
            continue;
        };
        let relative = Path::new(NATIVE_CACHE_NAMESPACE)
            .join(&scope)
            .join(&content);
        // An object whose verification itself fails on I/O is uncertainty:
        // the census turns incomplete instead of guessing drift.
        let live = match store.entry_type(&relative) {
            Err(Error::Io(error)) if error.kind() == std::io::ErrorKind::NotFound => None,
            Ok(fs::EntryType::File) => match verify_object(store, &relative, &content) {
                Ok(_) => Some(true),
                Err(Error::Io(error)) => {
                    census
                        .unscanned
                        .push((namespace.join(&scope).join(&content), error.to_string()));
                    census.complete = false;
                    continue;
                }
                Err(_) => Some(false),
            },
            Ok(_) => Some(false),
            Err(error) => {
                census
                    .unscanned
                    .push((namespace.join(&scope).join(&content), error.to_string()));
                census.complete = false;
                continue;
            }
        };
        match live {
            // A dangling or drifted link is corrupt or missing evidence, not
            // proof of no reference: the link still names the object, so it
            // pins every deletion candidate until the anomaly is resolved.
            None => {
                census.dangling.push((path, target));
                census.complete = false;
            }
            Some(false) => {
                census.drifted.push((path, target));
                census.complete = false;
            }
            Some(true) => {
                census
                    .references
                    .entry(namespace.join(&scope).join(&content))
                    .or_default()
                    .push(path);
            }
        }
    }
    Ok(())
}

/// Parses one symlink target as this unit's exact in-namespace object
/// reference: `<namespace>/<64 hex scope>/<64 hex content>` and nothing
/// else. Any other shape — relative, deeper, shallower, foreign root — is
/// rejected without touching the filesystem.
fn parse_object_target(namespace: &Path, target: &Path) -> Option<(String, String)> {
    let rest = target.strip_prefix(namespace).ok()?;
    let mut parts = rest.components();
    let scope = parts.next()?.as_os_str().to_str()?;
    let content = parts.next()?.as_os_str().to_str()?;
    if parts.next().is_some() || !is_scope(scope) || !is_scope(content) {
        return None;
    }
    Some((scope.to_owned(), content.to_owned()))
}

/// The classification of every object in the native-cache namespace.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
pub struct NativeCacheObjectCensus {
    /// Every canonical object: an owned regular file at
    /// `<namespace>/<64 hex>/<64 hex>`. These are a collector's only
    /// deletion candidates.
    pub objects: BTreeSet<PathBuf>,
    /// Everything else found inside the namespace, publisher staging
    /// temporaries included; retained and never touched by this unit.
    pub foreign: Vec<PathBuf>,
    /// `true` only when the whole namespace was enumerated within its bound.
    /// `false` means the object set is not proven exhaustive, so a collector
    /// must retain everything.
    pub complete: bool,
}

/// Enumerates and classifies every object in the store's native-cache
/// namespace for the collector.
///
/// Only the fixed namespace beneath the trusted store root is scanned, entry
/// by entry through bounded reads; a canonical object is an owned regular
/// file named by two strict 64-hex components, and every other entry —
/// including publisher staging temporaries — is reported as foreign and
/// retained. A missing namespace is an empty complete census; any I/O
/// failure or the entry bound makes the census `complete == false`, which a
/// collector must treat as "delete nothing". This function never mutates
/// anything.
pub fn enumerate_native_cache_objects(store_root: &Path) -> Result<NativeCacheObjectCensus> {
    let root = validated_dir(store_root, "shared store root")?;
    let mut census = NativeCacheObjectCensus {
        complete: true,
        ..Default::default()
    };
    let namespace_dir = match fs::Dir::open(&root.join(NATIVE_CACHE_NAMESPACE)) {
        Ok(directory) => directory,
        Err(Error::Io(error)) if error.kind() == std::io::ErrorKind::NotFound => return Ok(census),
        Err(error) => return Err(error),
    };
    let namespace = root.join(NATIVE_CACHE_NAMESPACE);
    let mut visited = 0_usize;
    let mut scopes = BoundedNames::open(&namespace_dir)?;
    while let Some(scope_name) = scopes.next(&mut census.complete)? {
        let Some(scope) = scope_name.to_str().filter(|name| is_scope(name)) else {
            census.foreign.push(namespace.join(&scope_name));
            continue;
        };
        let scope_dir = match namespace_dir.subdir(Path::new(&scope)) {
            Ok(directory) => directory,
            Err(_) => {
                census.complete = false;
                continue;
            }
        };
        let mut entries = BoundedNames::open(&scope_dir)?;
        while let Some(name) = entries.next(&mut census.complete)? {
            visited += 1;
            if visited > MAX_NAMESPACE_ENTRIES {
                census.complete = false;
                return Ok(census);
            }
            let object = namespace.join(scope).join(&name);
            let owned = scope_dir
                .entry(Some(Path::new(&name)))
                .map(|entry| entry.uid == euid())
                .unwrap_or(false);
            let canonical = name.to_str().is_some_and(is_scope)
                && matches!(
                    scope_dir.entry_type(Path::new(&name)),
                    Ok(fs::EntryType::File)
                )
                && owned;
            if canonical {
                census.objects.insert(object);
            } else {
                census.foreign.push(object);
            }
        }
    }
    Ok(census)
}

/// Returns the native-cache objects a collector may delete: the canonical
/// objects of a **complete** object census that no **complete** reference
/// census proves live. Any incompleteness — a bound, an unreadable
/// directory, a drifted or dangling link, an unscanned home — yields the
/// empty set, so corrupt or missing evidence pins everything instead of
/// widening the deletion candidates. This function changes nothing; the
/// collector owns deletion itself.
pub fn deletion_candidates(
    objects: &NativeCacheObjectCensus,
    references: &NativeCacheRefCensus,
) -> BTreeSet<PathBuf> {
    if !objects.complete || !references.complete {
        return BTreeSet::new();
    }
    let live: BTreeSet<&PathBuf> = references.references.keys().collect();
    objects
        .objects
        .iter()
        .filter(|object| !live.contains(*object))
        .cloned()
        .collect()
}

/// One bounded, resumable stream over a directory's entry names.
///
/// Names arrive in batches of at most [`SCAN_BATCH`], so no scan loads an
/// unbounded namespace into memory. A lost stream marks the pass incomplete
/// and ends iteration instead of pretending the directory was exhausted.
struct BoundedNames {
    /// Live directory stream, closed when the reader is dropped.
    scan: fs::DirScan,
    /// Not-yet-yielded names of the last batch read.
    pending: std::vec::IntoIter<std::ffi::OsString>,
    /// Whether the stream already reached end of list.
    finished: bool,
}

impl BoundedNames {
    /// Opens one bounded stream over `dir`.
    fn open(dir: &fs::Dir) -> Result<Self> {
        Ok(Self {
            scan: dir.scan()?,
            pending: Vec::new().into_iter(),
            finished: false,
        })
    }

    /// Returns the next entry name, or `None` at end of list or when the
    /// stream fails, marking `complete` false in the failure case.
    fn next(&mut self, complete: &mut bool) -> Result<Option<std::ffi::OsString>> {
        loop {
            if let Some(name) = self.pending.next() {
                return Ok(Some(name));
            }
            if self.finished {
                return Ok(None);
            }
            match self.scan.next_batch(SCAN_BATCH) {
                Ok((names, done)) => {
                    self.pending = names.into_iter();
                    self.finished = done;
                }
                Err(_) => {
                    *complete = false;
                    return Ok(None);
                }
            }
        }
    }
}

#[cfg(test)]
/// Exercises the pack, refresh, freshness and census contracts against
/// owned temporary fixtures only.
mod tests {
    use super::*;
    use agent_run_platform::snapshot_tree;
    use std::{
        fs,
        os::unix::fs::{symlink, MetadataExt, PermissionsExt},
        thread,
    };

    /// One fresh shared-store root in canonical form, RAII-cleaned.
    fn store_root() -> (tempfile::TempDir, PathBuf) {
        let store = tempfile::tempdir().unwrap();
        let root = store.path().canonicalize().unwrap();
        (store, root)
    }

    /// One fresh canonical owned home, RAII-cleaned.
    fn home() -> (tempfile::TempDir, PathBuf) {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().canonicalize().unwrap();
        (dir, path)
    }

    /// One valid tools disk-cache payload naming one tool.
    fn tools_json(tool: &str) -> Vec<u8> {
        serde_json::to_vec_pretty(&serde_json::json!({
            "schema_version": 4,
            "tools": [{"name": tool}]
        }))
        .unwrap()
    }

    /// One valid server-info disk-cache payload.
    fn server_info_json() -> Vec<u8> {
        serde_json::to_vec_pretty(&serde_json::json!({
            "schema_version": 1,
            "server_info": {"name": "codex"}
        }))
        .unwrap()
    }

    /// Writes one cache entry and returns its absolute path.
    fn entry(home: &Path, dir: &str, identity: &str, bytes: &[u8]) -> PathBuf {
        let path = home.join(dir).join(identity);
        fs::create_dir_all(path.parent().unwrap()).unwrap();
        fs::write(&path, bytes).unwrap();
        path
    }

    /// A well-formed native identity filename.
    const IDENTITY: &str = "0123456789abcdef0123456789abcdef01234567.json";
    /// The one compatibility label the helper packs run under.
    const DOMAIN: &str = "https://chatgpt.com+codex";

    /// Packs `home`'s `kind` cache under the fixed test domain.
    fn pack(store: &Path, home: &Path, kind: NativeCacheKind) -> NativeCacheReport {
        let domain = NativeCacheDomain::new(kind, DOMAIN).unwrap();
        pack_native_cache(store, home, &domain).unwrap()
    }

    /// The one packed object path of a completed single-entry pack.
    fn sole_object(report: &NativeCacheReport) -> PathBuf {
        assert!(report.complete, "pack should complete: {report:?}");
        assert_eq!(report.entries.len(), 1, "one entry: {report:?}");
        match &report.entries[0].disposition {
            NativeCacheDisposition::Packed { object, .. } => object.clone(),
            other => panic!("expected packed, got {other:?}"),
        }
    }

    /// Unique inode bytes across a set of paths: each distinct
    /// `(device, inode)` counted once, with its length.
    fn unique_inode_bytes(paths: &[&Path]) -> u64 {
        let mut seen: BTreeMap<(u64, u64), u64> = BTreeMap::new();
        for path in paths {
            let metadata = fs::metadata(path).unwrap();
            seen.insert((metadata.dev(), metadata.ino()), metadata.len());
        }
        seen.values().sum()
    }

    /// Two homes with identical schema-4 bytes, native identity and domain
    /// share exactly one payload inode through their exact file links, with
    /// a measured unique-inode-byte drop.
    #[test]
    fn identical_payloads_share_one_object() {
        let (store, root) = store_root();
        let (first_home, first) = home();
        let (second_home, second) = home();
        let bytes = tools_json("bash");
        let first_entry = entry(&first, TOOLS_CACHE_DIR, IDENTITY, &bytes);
        let second_entry = entry(&second, TOOLS_CACHE_DIR, IDENTITY, &bytes);
        let before = unique_inode_bytes(&[&first_entry, &second_entry]);

        let first_object = sole_object(&pack(&root, &first, NativeCacheKind::Tools));
        let second_object = sole_object(&pack(&root, &second, NativeCacheKind::Tools));

        assert_eq!(first_object, second_object);
        let shared_inode = fs::metadata(&first_object).unwrap().ino();
        assert_eq!(fs::metadata(&second_object).unwrap().ino(), shared_inode);
        for entry in [&first_entry, &second_entry] {
            assert!(entry.is_symlink(), "entry must be one exact file link");
            assert_eq!(fs::read_link(entry).unwrap(), first_object);
            // Native File::open reads straight through the link.
            assert_eq!(fs::read(entry).unwrap(), bytes);
        }
        assert_eq!(
            fs::metadata(&first_object).unwrap().permissions().mode() & 0o7777,
            0o400
        );
        // Before: two private copies. After: exactly the one shared object.
        assert_eq!(before, 2 * bytes.len() as u64);
        assert_eq!(unique_inode_bytes(&[&first_object]), bytes.len() as u64);
        let _ = (store, first_home, second_home);
    }

    /// Different identity, domain, kind or schema never share an object.
    #[test]
    fn distinct_scopes_do_not_alias() {
        let (store, root) = store_root();
        let (_, alpha) = home();
        let (_, beta) = home();
        let other_identity = "ffffffffffffffffffffffffffffffffffffffff.json";
        let alpha_entry = entry(&alpha, TOOLS_CACHE_DIR, IDENTITY, &tools_json("bash"));
        entry(&beta, TOOLS_CACHE_DIR, other_identity, &tools_json("bash"));
        let identity_object = sole_object(&pack(&root, &alpha, NativeCacheKind::Tools));
        let foreign_identity = sole_object(&pack(&root, &beta, NativeCacheKind::Tools));

        let (_, gamma) = home();
        entry(&gamma, SERVER_INFO_CACHE_DIR, IDENTITY, &server_info_json());
        let server_object = sole_object(&pack(&root, &gamma, NativeCacheKind::ServerInfo));

        let (_, delta) = home();
        entry(&delta, TOOLS_CACHE_DIR, IDENTITY, &tools_json("bash"));
        let other_domain =
            NativeCacheDomain::new(NativeCacheKind::Tools, "other-endpoint").unwrap();
        pack_native_cache(&root, &delta, &other_domain).unwrap();

        let namespace = root.join(NATIVE_CACHE_NAMESPACE);
        let objects: Vec<_> = fs::read_dir(&namespace)
            .unwrap()
            .flat_map(|scope| {
                let scope = scope.unwrap().path();
                fs::read_dir(scope)
                    .unwrap()
                    .map(|object| object.unwrap().path())
                    .collect::<Vec<_>>()
            })
            .collect();
        assert_eq!(objects.len(), 4, "identity, other identity, kind, domain");
        let inodes: Vec<_> = objects
            .iter()
            .map(|path| fs::metadata(path).unwrap().ino())
            .collect();
        for pair in inodes.windows(2) {
            assert_ne!(pair[0], pair[1]);
        }
        assert_eq!(fs::read_link(&alpha_entry).unwrap(), identity_object);
        assert_ne!(identity_object, foreign_identity);
        assert_ne!(identity_object, server_object);
        let _ = store;
    }

    /// A native atomic refresh (private temp file + rename over the cache
    /// path) replaces the link, leaves the shared object and every other
    /// home untouched, and a later repack publishes the new content.
    #[test]
    fn native_atomic_refresh_preserves_shared_bytes() {
        let (store, root) = store_root();
        let (first_home, first) = home();
        let (second_home, second) = home();
        let original = tools_json("bash");
        let first_entry = entry(&first, TOOLS_CACHE_DIR, IDENTITY, &original);
        let second_entry = entry(&second, TOOLS_CACHE_DIR, IDENTITY, &original);
        let object = sole_object(&pack(&root, &first, NativeCacheKind::Tools));
        sole_object(&pack(&root, &second, NativeCacheKind::Tools));

        // The exact native writer pattern: NamedTempFile in the private
        // cache parent, then atomic persist (rename) over the cache path.
        let refreshed = tools_json("edit");
        let temp = first.join(TOOLS_CACHE_DIR).join(".tmp-native-rewrite");
        fs::write(&temp, &refreshed).unwrap();
        fs::rename(&temp, &first_entry).unwrap();

        assert!(!first_entry.is_symlink(), "persist replaced the link");
        assert_eq!(fs::read(&first_entry).unwrap(), refreshed);
        assert_eq!(fs::read(&object).unwrap(), original, "object untouched");
        assert_eq!(
            fs::read(&second_entry).unwrap(),
            original,
            "other home unchanged"
        );
        assert_eq!(
            fs::metadata(&object).unwrap().ino(),
            fs::metadata(fs::read_link(&second_entry).unwrap())
                .unwrap()
                .ino()
        );

        let repacked = sole_object(&pack(&root, &first, NativeCacheKind::Tools));
        assert_ne!(repacked, object);
        assert_eq!(fs::read(&repacked).unwrap(), refreshed);
        assert_eq!(fs::read_link(&first_entry).unwrap(), repacked);
        assert_eq!(fs::read(&object).unwrap(), original, "old object stays");
        let _ = (store, first_home, second_home);
    }

    /// The finalized index, managed flat file, managed link, history and
    /// plugin bytes are conserved exactly across a pack of a non-overlapping
    /// cache directory.
    #[test]
    fn original_home_bytes_are_conserved() {
        let (store, root) = store_root();
        let (home_dir, house) = home();
        fs::write(house.join("config.toml"), "managed flat config\n").unwrap();
        fs::create_dir_all(house.join("credentials")).unwrap();
        fs::write(house.join("credentials/store.json"), b"{}\n").unwrap();
        symlink("credentials/store.json", house.join("auth.json")).unwrap();
        fs::write(house.join("history.jsonl"), "original history\n").unwrap();
        fs::create_dir_all(house.join("plugins/personal/tool")).unwrap();
        fs::write(house.join("plugins/personal/tool/manifest.json"), b"{}\n").unwrap();
        entry(&house, TOOLS_CACHE_DIR, IDENTITY, &tools_json("bash"));
        // A real finalized managed index, produced by the platform's own
        // finalizer over the home's actual managed material.
        snapshot_tree::finalize_runtime_snapshots(
            &house,
            "rev-fixture",
            &["config.toml".to_owned()],
            &[("auth.json".to_owned(), "credentials/store.json".to_owned())],
        )
        .unwrap();
        let index_before = fs::read(house.join(".agent-run-snapshots.json")).unwrap();
        let config_before = fs::read(house.join("config.toml")).unwrap();
        let plugin_before = fs::read(house.join("plugins/personal/tool/manifest.json")).unwrap();
        let history_before = fs::read(house.join("history.jsonl")).unwrap();

        let report = pack(&root, &house, NativeCacheKind::Tools);

        assert!(matches!(
            report.entries[0].disposition,
            NativeCacheDisposition::Packed { .. }
        ));
        assert_eq!(
            fs::read(house.join(".agent-run-snapshots.json")).unwrap(),
            index_before,
            "the finalized index bytes are conserved exactly"
        );
        assert_eq!(fs::read(house.join("config.toml")).unwrap(), config_before);
        assert!(house.join("auth.json").is_symlink());
        assert_eq!(
            fs::read_link(house.join("auth.json")).unwrap(),
            Path::new("credentials/store.json")
        );
        assert_eq!(
            fs::read(house.join("plugins/personal/tool/manifest.json")).unwrap(),
            plugin_before
        );
        assert_eq!(
            fs::read(house.join("history.jsonl")).unwrap(),
            history_before
        );
        let _ = (store, home_dir);
    }

    /// An indexed root, flat managed file or managed link overlapping the
    /// cache directory refuses the whole pack without mutating anything, and
    /// a malformed index refuses the same way.
    #[test]
    fn managed_index_overlap_refuses_packing() {
        let (store, root) = store_root();
        let bytes = tools_json("bash");

        // An indexed managed tree rooted exactly at the cache directory.
        let (tree_home, tree_house) = home();
        let source = tempfile::tempdir().unwrap();
        fs::create_dir_all(source.path().join("inner")).unwrap();
        fs::write(source.path().join("inner/payload.txt"), b"sealed\n").unwrap();
        snapshot_tree::snapshot_managed_tree(
            &tree_house,
            Path::new(TOOLS_CACHE_DIR),
            source.path(),
            None,
        )
        .unwrap();
        snapshot_tree::finalize_runtime_snapshots(&tree_house, "rev-tree", &[], &[]).unwrap();
        let index = tree_house.join(".agent-run-snapshots.json");
        let index_before = fs::read(&index).unwrap();
        let sealed = tree_house.join(TOOLS_CACHE_DIR).join("inner/payload.txt");
        let report = pack(&root, &tree_house, NativeCacheKind::Tools);
        assert_eq!(
            report.entries[0].disposition,
            NativeCacheDisposition::ManagedIndexOverlap {
                entry: TOOLS_CACHE_DIR.to_owned()
            }
        );
        assert_eq!(fs::read(&index).unwrap(), index_before);
        assert!(!sealed.is_symlink());
        assert_eq!(fs::read(&sealed).unwrap(), b"sealed\n");
        assert!(
            !root.join(NATIVE_CACHE_NAMESPACE).exists(),
            "nothing packed"
        );

        // An indexed flat managed file inside the cache directory.
        let (flat_home, flat_house) = home();
        let flat = entry(&flat_house, TOOLS_CACHE_DIR, IDENTITY, &bytes);
        snapshot_tree::finalize_runtime_snapshots(
            &flat_house,
            "rev-flat",
            &[format!("{TOOLS_CACHE_DIR}/{IDENTITY}")],
            &[],
        )
        .unwrap();
        let report = pack(&root, &flat_house, NativeCacheKind::Tools);
        assert_eq!(
            report.entries[0].disposition,
            NativeCacheDisposition::ManagedIndexOverlap {
                entry: format!("{TOOLS_CACHE_DIR}/{IDENTITY}")
            }
        );
        assert!(!flat.is_symlink());
        assert_eq!(fs::read(&flat).unwrap(), bytes);

        // An indexed managed link inside the cache directory.
        let (link_home, link_house) = home();
        fs::create_dir_all(link_house.join("credentials")).unwrap();
        fs::write(link_house.join("credentials/store.json"), b"{}\n").unwrap();
        fs::create_dir_all(link_house.join(TOOLS_CACHE_DIR)).unwrap();
        let link_path = link_house.join(TOOLS_CACHE_DIR).join(IDENTITY);
        symlink("../../../credentials/store.json", &link_path).unwrap();
        snapshot_tree::finalize_runtime_snapshots(
            &link_house,
            "rev-link",
            &[],
            &[(
                format!("{TOOLS_CACHE_DIR}/{IDENTITY}"),
                "../../../credentials/store.json".to_owned(),
            )],
        )
        .unwrap();
        let report = pack(&root, &link_house, NativeCacheKind::Tools);
        assert!(matches!(
            report.entries[0].disposition,
            NativeCacheDisposition::ManagedIndexOverlap { .. }
        ));
        assert_eq!(
            fs::read_link(&link_path).unwrap(),
            Path::new("../../../credentials/store.json")
        );

        // A malformed index refuses the pack and preserves every byte.
        let (broken_home, broken_house) = home();
        let broken_entry = entry(&broken_house, TOOLS_CACHE_DIR, IDENTITY, &bytes);
        fs::write(
            broken_house.join(".agent-run-snapshots.json"),
            b"{ not finalized",
        )
        .unwrap();
        let report = pack(&root, &broken_house, NativeCacheKind::Tools);
        assert_eq!(
            report.entries[0].disposition,
            NativeCacheDisposition::ManagedIndexUnreadable
        );
        assert!(!broken_entry.is_symlink());
        assert_eq!(fs::read(&broken_entry).unwrap(), bytes);
        let _ = (store, tree_home, flat_home, link_home, broken_home);
    }

    /// An object's mtime is the minimum proven source mtime: identical
    /// payloads from later sources never freshen it, older ones lower it,
    /// and one payload always stays exactly one object.
    #[test]
    fn mtime_is_never_freshened() {
        let (store, root) = store_root();
        let bytes = tools_json("bash");
        let base = SystemTime::UNIX_EPOCH;
        let mtimes = [
            base + std::time::Duration::from_secs(4_000),
            base + std::time::Duration::from_secs(9_000),
            base + std::time::Duration::from_secs(1_000),
        ];
        let (old_home, old) = home();
        let (new_home, recent) = home();
        let (older_home, older) = home();
        for (path, mtime) in [(&old, mtimes[0]), (&recent, mtimes[1]), (&older, mtimes[2])] {
            let path = entry(path, TOOLS_CACHE_DIR, IDENTITY, &bytes);
            fs::File::options()
                .write(true)
                .open(&path)
                .unwrap()
                .set_times(FileTimes::new().set_modified(mtime))
                .unwrap();
        }

        let object = sole_object(&pack(&root, &old, NativeCacheKind::Tools));
        assert_eq!(
            fs::metadata(&object).unwrap().modified().unwrap(),
            mtimes[0],
            "first publication stamps the proven source mtime"
        );

        sole_object(&pack(&root, &recent, NativeCacheKind::Tools));
        assert_eq!(
            fs::metadata(&object).unwrap().modified().unwrap(),
            mtimes[0],
            "a later source mtime never freshens the object"
        );
        assert_eq!(
            fs::metadata(recent.join(TOOLS_CACHE_DIR).join(IDENTITY))
                .unwrap()
                .modified()
                .unwrap(),
            mtimes[0],
            "the newer home reads the conservative older mtime"
        );

        let report = pack(&root, &older, NativeCacheKind::Tools);
        assert!(
            matches!(
                report.entries[0].disposition,
                NativeCacheDisposition::Packed {
                    lowered_mtime: true,
                    ..
                }
            ),
            "an older proven mtime lowers the object"
        );
        assert_eq!(
            fs::metadata(&object).unwrap().modified().unwrap(),
            mtimes[2]
        );
        let objects: Vec<_> = fs::read_dir(root.join(NATIVE_CACHE_NAMESPACE))
            .unwrap()
            .flat_map(|scope| fs::read_dir(scope.unwrap().path()).unwrap())
            .map(|object| object.unwrap().path())
            .collect();
        assert_eq!(objects.len(), 1, "one payload stays one object");
        let _ = (store, old_home, new_home, older_home);
    }

    /// Unknown, oversized, malformed and foreign entries are preserved with
    /// explicit dispositions, and a cache-path symlink (a converted managed
    /// root shape) skips the whole directory.
    #[test]
    fn unsupported_entries_are_retained() {
        let (store, root) = store_root();
        let (home_dir, house) = home();
        let cache = house.join(TOOLS_CACHE_DIR);
        fs::create_dir_all(&cache).unwrap();
        fs::write(cache.join("notes.txt"), b"unknown name\n").unwrap();
        fs::write(
            cache.join("1111111111111111111111111111111111111111.json"),
            b"{ not json",
        )
        .unwrap();
        let wrong_schema = cache.join("2222222222222222222222222222222222222222.json");
        fs::write(&wrong_schema, tools_json_v3()).unwrap();
        let oversized = cache.join("3333333333333333333333333333333333333333.json");
        fs::File::create(&oversized)
            .unwrap()
            .set_len(NATIVE_CACHE_MAX_BYTES + 1)
            .unwrap();
        symlink(
            "/etc/passwd",
            cache.join("4444444444444444444444444444444444444444.json"),
        )
        .unwrap();
        fs::create_dir(cache.join("5555555555555555555555555555555555555555.json")).unwrap();

        let report = pack(&root, &house, NativeCacheKind::Tools);

        let disposition = |name: &str| {
            report
                .entries
                .iter()
                .find(|entry| entry.path.file_name().unwrap() == name)
                .map(|entry| entry.disposition.clone())
                .unwrap()
        };
        assert_eq!(
            disposition("notes.txt"),
            NativeCacheDisposition::UnsupportedName
        );
        assert_eq!(
            disposition("1111111111111111111111111111111111111111.json"),
            NativeCacheDisposition::Malformed
        );
        assert_eq!(
            disposition("2222222222222222222222222222222222222222.json"),
            NativeCacheDisposition::Malformed
        );
        assert_eq!(
            disposition("3333333333333333333333333333333333333333.json"),
            NativeCacheDisposition::Oversized {
                bytes: NATIVE_CACHE_MAX_BYTES + 1
            }
        );
        assert_eq!(
            disposition("4444444444444444444444444444444444444444.json"),
            NativeCacheDisposition::ForeignLink
        );
        assert_eq!(
            disposition("5555555555555555555555555555555555555555.json"),
            NativeCacheDisposition::UnsupportedEntry
        );
        for name in [
            "notes.txt",
            "1111111111111111111111111111111111111111.json",
            "2222222222222222222222222222222222222222.json",
            "5555555555555555555555555555555555555555.json",
        ] {
            assert!(!cache.join(name).is_symlink(), "{name} must be retained");
        }
        assert!(
            cache
                .join("4444444444444444444444444444444444444444.json")
                .is_symlink(),
            "the foreign link must be retained as a link"
        );
        assert_eq!(
            fs::metadata(&oversized).unwrap().len(),
            NATIVE_CACHE_MAX_BYTES + 1
        );
        assert!(
            !root.join(NATIVE_CACHE_NAMESPACE).exists(),
            "nothing packed"
        );

        // A cache directory that is itself a link — the shape of a
        // converted immutable managed root — is refused as a whole.
        let (linked_home, linked) = home();
        fs::create_dir_all(linked.join("elsewhere")).unwrap();
        fs::create_dir_all(linked.join("cache")).unwrap();
        symlink(linked.join("elsewhere"), linked.join(TOOLS_CACHE_DIR)).unwrap();
        let report = pack(&root, &linked, NativeCacheKind::Tools);
        assert_eq!(report.entries.len(), 1);
        assert_eq!(
            report.entries[0].disposition,
            NativeCacheDisposition::ManagedRootOverlap
        );
        assert!(linked.join(TOOLS_CACHE_DIR).is_symlink());
        let _ = (store, home_dir, linked_home);
    }

    /// One tools payload carrying the wrong native schema version.
    fn tools_json_v3() -> Vec<u8> {
        serde_json::to_vec_pretty(&serde_json::json!({
            "schema_version": 3,
            "tools": []
        }))
        .unwrap()
    }

    /// Packing an already-shared entry is idempotent, and a link into
    /// another scope stays foreign and untouched.
    #[test]
    fn repack_is_idempotent_and_foreign_scope_links_are_kept() {
        let (store, root) = store_root();
        let (home_dir, subject) = home();
        let entry_path = entry(&subject, TOOLS_CACHE_DIR, IDENTITY, &tools_json("bash"));
        let object = sole_object(&pack(&root, &subject, NativeCacheKind::Tools));
        let inode = fs::metadata(&object).unwrap().ino();

        let again = pack(&root, &subject, NativeCacheKind::Tools);
        assert_eq!(
            again.entries[0].disposition,
            NativeCacheDisposition::AlreadyShared
        );
        assert_eq!(fs::metadata(&object).unwrap().ino(), inode);

        let (other_home, other) = home();
        entry(&other, TOOLS_CACHE_DIR, IDENTITY, &tools_json("bash"));
        let foreign_domain = NativeCacheDomain::new(NativeCacheKind::Tools, "another").unwrap();
        pack_native_cache(&root, &other, &foreign_domain).unwrap();
        let report = pack(&root, &other, NativeCacheKind::Tools);
        assert_eq!(
            report.entries[0].disposition,
            NativeCacheDisposition::ForeignLink
        );
        assert_ne!(
            fs::read_link(other.join(TOOLS_CACHE_DIR).join(IDENTITY)).unwrap(),
            object
        );
        assert_eq!(fs::read_link(&entry_path).unwrap(), object);
        let _ = (store, home_dir, other_home);
    }

    /// Reuse of an already-shared link is claimed only after its object is
    /// re-verified: a tampered or mode-drifted target reports drift instead
    /// of a silent AlreadyShared that would lose the reference.
    #[test]
    fn already_shared_verifies_the_object() {
        let (store, root) = store_root();
        let (home_dir, house) = home();
        let entry_path = entry(&house, TOOLS_CACHE_DIR, IDENTITY, &tools_json("bash"));
        let bytes = fs::read(&entry_path).unwrap();
        let object = sole_object(&pack(&root, &house, NativeCacheKind::Tools));

        // Tampered content under the same name: drift, never reuse.
        fs::set_permissions(&object, fs::Permissions::from_mode(0o600)).unwrap();
        fs::write(&object, b"tampered").unwrap();
        fs::set_permissions(&object, fs::Permissions::from_mode(0o400)).unwrap();
        let report = pack(&root, &house, NativeCacheKind::Tools);
        assert_eq!(
            report.entries[0].disposition,
            NativeCacheDisposition::ExistingObjectDrifted
        );
        assert_eq!(fs::read_link(&entry_path).unwrap(), object);

        // Mode drift alone is drift as well.
        fs::set_permissions(&object, fs::Permissions::from_mode(0o600)).unwrap();
        let report = pack(&root, &house, NativeCacheKind::Tools);
        assert_eq!(
            report.entries[0].disposition,
            NativeCacheDisposition::ExistingObjectDrifted
        );

        // A verified object restores the idempotent reuse claim.
        fs::write(&object, &bytes).unwrap();
        fs::set_permissions(&object, fs::Permissions::from_mode(0o400)).unwrap();
        let report = pack(&root, &house, NativeCacheKind::Tools);
        assert_eq!(
            report.entries[0].disposition,
            NativeCacheDisposition::AlreadyShared
        );
        let _ = (store, home_dir);
    }

    /// An interrupted link switch always leaves the original file or the
    /// complete shared link under the native name, a retry converges, and no
    /// unowned or overwritten entry is ever deleted.
    #[test]
    fn fault_at_link_switch_preserves_old_or_new() {
        let (store, root) = store_root();
        let bytes = tools_json("bash");
        let mtime = SystemTime::UNIX_EPOCH + std::time::Duration::from_secs(12_345);

        // Crash before the atomic replace: the original stays live and one
        // recoverable temporary link orphan is left behind, never deleted.
        let (before_home, before) = home();
        let before_entry = entry(&before, TOOLS_CACHE_DIR, IDENTITY, &bytes);
        fs::File::options()
            .write(true)
            .open(&before_entry)
            .unwrap()
            .set_times(FileTimes::new().set_modified(mtime))
            .unwrap();
        let domain = NativeCacheDomain::new(NativeCacheKind::Tools, DOMAIN).unwrap();
        let fault = |point: NativeCacheFault| -> Result<()> {
            match point {
                NativeCacheFault::BeforeLinkSwap => {
                    Err(invalid("simulated crash before the link swap"))
                }
                NativeCacheFault::AfterLinkSwap => Ok(()),
            }
        };
        let report = pack_native_cache_with_fault(&root, &before, &domain, Some(&fault)).unwrap();
        assert!(matches!(
            report.entries[0].disposition,
            NativeCacheDisposition::LinkFailed { .. }
        ));
        assert!(!before_entry.is_symlink(), "the original file survived");
        assert_eq!(fs::read(&before_entry).unwrap(), bytes);
        let names: Vec<_> = fs::read_dir(before.join(TOOLS_CACHE_DIR))
            .unwrap()
            .map(|name| name.unwrap().file_name().to_string_lossy().into_owned())
            .collect();
        let orphan = names
            .iter()
            .find(|name| name.starts_with(BACKUP_PREFIX))
            .expect("the temporary link orphan survives a crash");
        assert!(
            before.join(TOOLS_CACHE_DIR).join(orphan).is_symlink(),
            "the orphan is this unit's link, and it is never deleted"
        );
        // The retry converges: the original is packed, the orphan is
        // classified as a foreign link and retained untouched.
        let retry = pack(&root, &before, NativeCacheKind::Tools);
        assert!(matches!(
            retry
                .entries
                .iter()
                .find(|entry| entry.path.file_name() == Some(std::ffi::OsStr::new(IDENTITY)))
                .expect("the identity entry is examined")
                .disposition,
            NativeCacheDisposition::Packed { .. }
        ));
        assert!(retry.entries.iter().any(|entry| entry.path.file_name()
            == Some(std::ffi::OsStr::new(orphan))
            && entry.disposition == NativeCacheDisposition::ForeignLink));
        assert!(before.join(TOOLS_CACHE_DIR).join(orphan).is_symlink());

        // Crash after the atomic replace committed: the name holds the
        // complete link, the published bytes and their proven mtime are
        // already durable, and a retry reports the idempotent reuse.
        let (after_home, after) = home();
        let after_entry = entry(&after, TOOLS_CACHE_DIR, IDENTITY, &bytes);
        fs::File::options()
            .write(true)
            .open(&after_entry)
            .unwrap()
            .set_times(FileTimes::new().set_modified(mtime))
            .unwrap();
        let fault = |point: NativeCacheFault| -> Result<()> {
            match point {
                NativeCacheFault::BeforeLinkSwap => Ok(()),
                NativeCacheFault::AfterLinkSwap => {
                    Err(invalid("simulated crash after the link swap"))
                }
            }
        };
        let report = pack_native_cache_with_fault(&root, &after, &domain, Some(&fault)).unwrap();
        assert!(matches!(
            report.entries[0].disposition,
            NativeCacheDisposition::LinkFailed { .. }
        ));
        assert!(after_entry.is_symlink(), "the committed link survived");
        let object = fs::read_link(&after_entry).unwrap();
        assert_eq!(fs::read(&object).unwrap(), bytes);
        assert_eq!(
            fs::metadata(&object).unwrap().modified().unwrap(),
            mtime,
            "the proven source mtime was flushed before the link was published"
        );
        let names: Vec<_> = fs::read_dir(after.join(TOOLS_CACHE_DIR))
            .unwrap()
            .map(|name| name.unwrap().file_name().to_string_lossy().into_owned())
            .collect();
        assert!(
            !names.iter().any(|name| name.starts_with(BACKUP_PREFIX)),
            "the committed switch leaves no orphan"
        );
        let retry = pack(&root, &after, NativeCacheKind::Tools);
        assert_eq!(
            retry.entries[0].disposition,
            NativeCacheDisposition::AlreadyShared
        );
        let _ = (store, before_home, after_home);
    }

    /// Concurrent packs of the same payload converge on one verified object
    /// and one inode.
    #[test]
    fn concurrent_packs_converge() {
        let (store, root) = store_root();
        let bytes = tools_json("bash");
        let homes: Vec<_> = (0..4)
            .map(|_| {
                let (dir, path) = home();
                entry(&path, TOOLS_CACHE_DIR, IDENTITY, &bytes);
                (dir, path)
            })
            .collect();
        let reports = thread::scope(|scope| {
            let mut handles = Vec::new();
            for (_, path) in &homes {
                let root = root.clone();
                let path = path.clone();
                handles.push(scope.spawn(move || {
                    let domain = NativeCacheDomain::new(NativeCacheKind::Tools, DOMAIN).unwrap();
                    pack_native_cache(&root, &path, &domain)
                }));
            }
            handles
                .into_iter()
                .map(|handle| handle.join().unwrap().unwrap())
                .collect::<Vec<_>>()
        });
        let mut objects = BTreeSet::new();
        for report in &reports {
            assert!(report.complete);
            objects.insert(sole_object(report));
        }
        assert_eq!(objects.len(), 1, "every publisher converged on one object");
        let object = objects.pop_first().unwrap();
        for (_, path) in &homes {
            let link = path.join(TOOLS_CACHE_DIR).join(IDENTITY);
            assert_eq!(fs::read_link(&link).unwrap(), object);
            assert_eq!(fs::read(&link).unwrap(), bytes);
        }
        assert_eq!(
            fs::metadata(&object).unwrap().permissions().mode() & 0o7777,
            0o400
        );
        let _ = store;
    }

    /// The reference census counts exact verified links as live, rejects
    /// tampered, missing and foreign targets, and reports scan failures as
    /// incomplete — never as an empty success.
    #[test]
    fn census_verifies_and_rejects_drift() {
        let (store, root) = store_root();
        let (first_home, first) = home();
        let (second_home, second) = home();
        let bytes = tools_json("bash");
        let first_link = entry(&first, TOOLS_CACHE_DIR, IDENTITY, &bytes);
        let second_link = entry(
            &second,
            SERVER_INFO_CACHE_DIR,
            IDENTITY,
            &server_info_json(),
        );
        let tools_object = sole_object(&pack(&root, &first, NativeCacheKind::Tools));
        let info_object = sole_object(&pack(&root, &second, NativeCacheKind::ServerInfo));

        // Tamper with the tools object: same name, different bytes.
        fs::set_permissions(&tools_object, fs::Permissions::from_mode(0o600)).unwrap();
        fs::write(&tools_object, b"tampered").unwrap();
        fs::set_permissions(&tools_object, fs::Permissions::from_mode(0o400)).unwrap();
        // Dangle the server-info reference.
        fs::remove_file(&info_object).unwrap();

        let homes = vec![first.clone(), second.clone()];
        let census = collect_native_cache_references(&root, &homes).unwrap();
        assert!(
            !census.complete,
            "drifted and dangling links are corrupt evidence, not proof of no reference"
        );
        assert!(census.references.is_empty(), "no live reference remains");
        assert_eq!(census.drifted.len(), 1);
        assert_eq!(census.drifted[0].0, first_link);
        assert_eq!(census.drifted[0].1, tools_object);
        assert_eq!(census.dangling.len(), 1);
        assert_eq!(census.dangling[0].0, second_link);
        assert_eq!(census.dangling[0].1, info_object);
        // The still-present tampered object is NOT a deletion candidate:
        // incomplete evidence pins every object, even extant ones.
        let objects = enumerate_native_cache_objects(&root).unwrap();
        assert!(objects.objects.contains(&tools_object));
        assert!(
            deletion_candidates(&objects, &census).is_empty(),
            "corrupt evidence must pin every deletion candidate"
        );

        // A foreign link is unknown reference evidence: it may resolve onto
        // a store object this census would otherwise offer for deletion, so
        // it is recorded, never followed, and turns the census incomplete.
        fs::create_dir_all(second.join(TOOLS_CACHE_DIR)).unwrap();
        symlink("/etc/passwd", second.join(TOOLS_CACHE_DIR).join(IDENTITY)).unwrap();
        let census = collect_native_cache_references(&root, &homes).unwrap();
        assert!(!census.complete, "the drifted and dangling links remain");
        assert_eq!(census.foreign.len(), 1);
        assert!(census.references.is_empty());

        // Restored objects plus a second live link are counted exactly, and
        // with complete evidence nothing is deletable while all is live.
        fs::remove_file(second.join(TOOLS_CACHE_DIR).join(IDENTITY)).unwrap();
        fs::remove_file(&tools_object).unwrap();
        fs::write(&tools_object, &bytes).unwrap();
        fs::set_permissions(&tools_object, fs::Permissions::from_mode(0o400)).unwrap();
        fs::write(&info_object, server_info_json()).unwrap();
        fs::set_permissions(&info_object, fs::Permissions::from_mode(0o400)).unwrap();
        entry(&second, TOOLS_CACHE_DIR, IDENTITY, &bytes);
        pack(&root, &second, NativeCacheKind::Tools);
        let census = collect_native_cache_references(&root, &homes).unwrap();
        assert!(census.complete);
        let second_tools = fs::read_link(second.join(TOOLS_CACHE_DIR).join(IDENTITY)).unwrap();
        assert_eq!(second_tools, tools_object, "same content, one object");
        assert_eq!(
            census.references.get(&tools_object).map(Vec::as_slice),
            Some(
                &[
                    first_link.clone(),
                    second.join(TOOLS_CACHE_DIR).join(IDENTITY)
                ][..]
            ),
            "every live physical reference is enumerated with its home path"
        );
        let objects = enumerate_native_cache_objects(&root).unwrap();
        assert_eq!(
            deletion_candidates(&objects, &census),
            BTreeSet::new(),
            "every object is referenced"
        );

        // An unscannable home is uncertainty: incomplete, never empty
        // success, and the evidence already gathered is kept.
        let mut homes = homes;
        homes.push(root.join("definitely-missing-home"));
        let census = collect_native_cache_references(&root, &homes).unwrap();
        assert!(!census.complete);
        assert_eq!(census.unscanned.len(), 1);
        assert!(!census.references.is_empty());
        assert!(deletion_candidates(&objects, &census).is_empty());
        let _ = (store, first_home, second_home);
    }

    /// Returns a relative path from `from` (a directory) to `to`, for
    /// building links that resolve equivalently to an absolute target.
    fn relative_to(from: &Path, to: &Path) -> PathBuf {
        let from: Vec<_> = from.components().collect();
        let to: Vec<_> = to.components().collect();
        let shared = from.iter().zip(&to).take_while(|(a, b)| a == b).count();
        let mut relative = PathBuf::new();
        for _ in shared..from.len() {
            relative.push("..");
        }
        for component in &to[shared..] {
            relative.push(component.as_os_str());
        }
        relative
    }

    /// An unrecognized link in a cache slot — a relative spelling resolving
    /// onto a live store object, or a foreign absolute target — is unknown
    /// reference evidence: the census turns incomplete, the link stays
    /// untouched, and no object may be deleted from that evidence.
    #[test]
    fn census_treats_relative_and_foreign_links_as_unknown_evidence() {
        let (_store, root) = store_root();
        let (_home_guard, home) = home();
        let bytes = tools_json("bash");
        entry(&home, TOOLS_CACHE_DIR, IDENTITY, &bytes);
        let object = sole_object(&pack(&root, &home, NativeCacheKind::Tools));
        let link = home.join(TOOLS_CACHE_DIR).join(IDENTITY);
        let exact = fs::read_link(&link).unwrap();
        let objects = enumerate_native_cache_objects(&root).unwrap();
        assert_eq!(objects.objects.len(), 1);

        // A relative spelling resolving onto the very same object.
        let relative = relative_to(&home.join(TOOLS_CACHE_DIR), &object);
        assert!(relative.is_relative());
        assert_eq!(
            home.join(TOOLS_CACHE_DIR)
                .join(&relative)
                .canonicalize()
                .unwrap(),
            object.canonicalize().unwrap(),
            "the relative spelling resolves onto the live object"
        );
        fs::remove_file(&link).unwrap();
        symlink(&relative, &link).unwrap();
        let census = collect_native_cache_references(&root, std::slice::from_ref(&home)).unwrap();
        assert!(
            !census.complete,
            "an unrecognized link is uncertainty, never proof of no reference"
        );
        assert_eq!(census.foreign.len(), 1);
        assert_eq!(
            census.foreign[0].1, relative,
            "the link itself is untouched"
        );
        assert!(
            !census.references.contains_key(&object),
            "an unrecognized link proves no live reference either"
        );
        assert!(
            deletion_candidates(&objects, &census).is_empty(),
            "the still-referenced object must not become a deletion candidate"
        );

        // A foreign absolute target is equally conservative.
        fs::remove_file(&link).unwrap();
        symlink("/etc/passwd", &link).unwrap();
        let census = collect_native_cache_references(&root, std::slice::from_ref(&home)).unwrap();
        assert!(!census.complete);
        assert_eq!(census.foreign.len(), 1);
        assert!(deletion_candidates(&objects, &census).is_empty());

        // The exact controlled spelling restores complete live evidence.
        fs::remove_file(&link).unwrap();
        symlink(&exact, &link).unwrap();
        let census = collect_native_cache_references(&root, std::slice::from_ref(&home)).unwrap();
        assert!(census.complete);
        assert!(census.references(&object));
        assert!(deletion_candidates(&objects, &census).is_empty());
    }

    /// The object census classifies canonical objects and foreign entries,
    /// stays complete for a missing namespace, and reports I/O trouble as
    /// incomplete.
    #[test]
    fn object_census_classifies_the_namespace() {
        let (store, root) = store_root();
        let empty = enumerate_native_cache_objects(&root).unwrap();
        assert!(empty.complete && empty.objects.is_empty() && empty.foreign.is_empty());

        let (first_home, first) = home();
        let (second_home, second) = home();
        entry(&first, TOOLS_CACHE_DIR, IDENTITY, &tools_json("bash"));
        entry(&second, TOOLS_CACHE_DIR, IDENTITY, &tools_json("bash"));
        entry(
            &second,
            SERVER_INFO_CACHE_DIR,
            IDENTITY,
            &server_info_json(),
        );
        pack(&root, &first, NativeCacheKind::Tools);
        pack(&root, &second, NativeCacheKind::Tools);
        pack(&root, &second, NativeCacheKind::ServerInfo);
        let namespace = root.join(NATIVE_CACHE_NAMESPACE);
        let first_scope = fs::read_dir(&namespace)
            .unwrap()
            .next()
            .unwrap()
            .unwrap()
            .path();
        fs::write(first_scope.join("foreign-note"), b"stray").unwrap();

        let census = enumerate_native_cache_objects(&root).unwrap();
        assert!(census.complete);
        assert_eq!(
            census.objects.len(),
            2,
            "tools payload deduped across homes"
        );
        assert_eq!(census.foreign.len(), 1);
        for object in &census.objects {
            assert_eq!(
                fs::metadata(object).unwrap().permissions().mode() & 0o7777,
                0o400
            );
        }
        assert!(census
            .objects
            .contains(&fs::read_link(first.join(TOOLS_CACHE_DIR).join(IDENTITY)).unwrap()));

        // An unreadable scope directory is I/O trouble, not emptiness.
        fs::set_permissions(&first_scope, fs::Permissions::from_mode(0o000)).unwrap();
        let census = enumerate_native_cache_objects(&root).unwrap();
        assert!(!census.complete, "unreadable scope must not look exhausted");
        fs::set_permissions(&first_scope, fs::Permissions::from_mode(0o755)).unwrap();
        let _ = (store, first_home, second_home);
    }

    /// A directory beyond the entry bound is reported bounded, not complete.
    #[test]
    fn bounded_pack_scan_reports_incomplete() {
        let (store, root) = store_root();
        let (home_dir, house) = home();
        let cache = house.join(TOOLS_CACHE_DIR);
        fs::create_dir_all(&cache).unwrap();
        for index in 0..=MAX_DIR_ENTRIES {
            fs::write(cache.join(format!("{index:040x}.json")), b"not json").unwrap();
        }
        let report = pack(&root, &house, NativeCacheKind::Tools);
        assert!(!report.complete, "entry bound must surface as incomplete");
        assert_eq!(report.entries.len(), MAX_DIR_ENTRIES);
        let _ = (store, home_dir);
    }

    /// Homes that alias the store, foreign shapes and non-canonical inputs
    /// are refused before anything is touched.
    #[test]
    fn untrusted_roots_and_homes_are_refused() {
        let (store, root) = store_root();
        let domain = NativeCacheDomain::new(NativeCacheKind::Tools, "domain").unwrap();
        assert!(pack_native_cache(&root, Path::new("relative/path"), &domain).is_err());
        assert!(pack_native_cache(Path::new("relative"), &root, &domain).is_err());
        assert!(pack_native_cache(&root, &root, &domain).is_err());
        let inside = root.join("nested/home");
        fs::create_dir_all(&inside).unwrap();
        assert!(pack_native_cache(&root, &inside, &domain).is_err());
        let (home_dir, home) = home();
        let aliased = home.join("alias");
        symlink(&home, &aliased).unwrap();
        assert!(pack_native_cache(&root, &aliased, &domain).is_err());
        assert!(NativeCacheDomain::new(NativeCacheKind::Tools, "").is_err());
        assert!(
            NativeCacheDomain::new(NativeCacheKind::Tools, "x".repeat(MAX_DOMAIN_BYTES + 1))
                .is_err()
        );
        assert!(domain.scope_digest("nope.json").is_err());
        assert!(domain
            .scope_digest("0123456789ABCDEF0123456789abcdef01234567.json")
            .is_err());
        assert!(native_cache_object_path(&root, "short", &"0".repeat(64)).is_err());
        let _ = (store, home_dir);
    }

    /// The scope preimage is the exact documented contract: every field
    /// separated, ordered and versioned, so a bare filename never proves
    /// compatibility across endpoints.
    #[test]
    fn scope_preimage_is_exact() {
        let domain = NativeCacheDomain::new(NativeCacheKind::Tools, "endpoint").unwrap();
        let preimage = format!(
            "{SCOPE_PREFIX}\n{TOOLS_CACHE_DIR}\n4\n{IDENTITY}\n{}\nendpoint",
            domain.effective_uid()
        );
        assert_eq!(
            domain.scope_digest(IDENTITY).unwrap(),
            hex_digest(&Sha256::digest(preimage.as_bytes()))
        );
        let other = NativeCacheDomain::new(NativeCacheKind::Tools, "other").unwrap();
        assert_ne!(
            domain.scope_digest(IDENTITY).unwrap(),
            other.scope_digest(IDENTITY).unwrap()
        );
    }
}
