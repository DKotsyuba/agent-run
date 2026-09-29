//! Reference-aware garbage collection for the shared managed-asset store.
//!
//! Collection runs inside the existing bounded housekeeping and socket
//! maintenance cycles, never as a daemon of its own. One pass takes the same
//! store-wide publish/GC lock publication uses — nonblocking, so a broker is
//! never stalled behind an import or a native guard preflight — and while it
//! holds that lock it re-derives every reference from durable evidence before
//! deleting anything: every registered layout row (a `prepared` row always
//! pins what it names; a `committed` row pins while its physical home
//! exists), the bounded storage-protection snapshot of frozen identities,
//! frozen and current configuration, and not-conclusively-released service
//! definitions — any of which may name a shared tree, a derived view, or a
//! single payload file directly.
//!
//! The safety rule throughout is **a complete proof before any deletion**: a
//! reference census that is partial, unreadable, corrupt or beyond its bound
//! retains every candidate and reports `incomplete` for retry, and unknown is
//! never classified as unreferenced. Blobs are deleted only after one pass
//! enumerated every remaining tree and proved what each still references, so
//! a missing or corrupt manifest stops destructive work instead of widening
//! the candidate set. There are no live reference counts to drift.
//!
//! Committed layout rows are removed only once their physical home is
//! conclusively gone (a `NotFound` resolution, never a permission or I/O
//! error) and no protected path or agent identity still references the home;
//! prepared rows never age out. Collection order is derived plugin views,
//! then trees, then the payload blobs derived from the manifests of the
//! trees that remain: a view file is an internal hardlink, so an obsolete
//! view left behind keeps its payload's inode allocated and a stale view
//! whose tree is gone would corrupt a future import's reuse of the same
//! content-addressed name. Publisher staging orphans are removed only inside
//! the owned namespaces, only for the exact staging name shape and entry
//! type, and only while this process holds the publish lock — never by age.
//! The lock file itself, and everything outside the store's own namespaces,
//! is never touched.

use crate::{fs, state::Store, Result};
use agent_run_domain::{error::invalid, Error};
use agent_run_platform::{
    plugin_views,
    shared_assets::{self, SharedStoreLock, TEMP_PREFIX, TEMP_SUFFIX},
    snapshot_tree::SNAPSHOT_MANIFEST,
};
use std::{
    collections::{BTreeMap, BTreeSet},
    path::{Path, PathBuf},
    sync::{Mutex, OnceLock},
};

/// Upper bound on layout rows one reference census reads.
const ROWS_PER_PASS: usize = 256;
/// Upper bound on layout pages one census accepts before it is incomplete.
const ROW_PAGES_LIMIT: usize = 16;
/// Upper bound on tree or view directories one pass begins draining.
const TREE_ROOTS_PER_PASS: usize = 4;
/// Upper bound on entry unlinks inside draining directories per pass.
const TREE_UNLINKS_PER_PASS: usize = 512;
/// Upper bound on blobs one pass unlinks.
const BLOBS_PER_PASS: usize = 512;
/// Upper bound on staging orphans one pass removes.
const STAGING_PER_PASS: usize = 64;
/// Upper bound on names one bounded directory stream reads per batch.
const SCAN_BATCH: usize = 256;
/// Upper bound on remembered tree references before a process refuses.
const MEMO_LIMIT: usize = 20_000;
/// Upper bound on referenced blob paths held while collecting.
const REFERENCED_BLOBS_LIMIT: usize = 200_000;
/// Cooperative wall-clock budget for one pass's proofs and scans, matching
/// housekeeping's own pass deadline; exceeding it retains and retries.
const PASS_SECONDS: u64 = 2;
/// Upper bound on homes one native reference census page covers.
const NATIVE_HOMES_PER_PAGE: usize = 64;
/// Upper bound on native metadata objects one pass unlinks.
const NATIVE_OBJECTS_PER_PASS: usize = 512;

/// The registry census page bound, overridable only in test builds so a test
/// can prove a partial census never permits deletion.
#[cfg(feature = "test-fixtures")]
fn row_pages_limit() -> usize {
    std::env::var("AGENT_RUN_GC_ROW_PAGES")
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(ROW_PAGES_LIMIT)
}

/// The registry census page bound.
#[cfg(not(feature = "test-fixtures"))]
fn row_pages_limit() -> usize {
    ROW_PAGES_LIMIT
}

/// Whether one pass may unlink, or only report what it would reclaim.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Mode {
    /// Measure candidates and report them; change nothing anywhere.
    Preview,
    /// Delete conclusively unreferenced objects and removable rows.
    Apply,
}

/// One pass's explicit outcome and backlog counts.
#[derive(Debug, Default, Clone)]
pub struct Outcome {
    /// Shared tree directories removed (or, in preview, that would go).
    pub trees_removed: usize,
    /// Payload blobs removed (or, in preview, that would go).
    pub blobs_removed: usize,
    /// Derived views removed (or, in preview, that would go).
    pub views_removed: usize,
    /// Publisher staging orphans removed (or, in preview, that would go).
    pub staging_removed: usize,
    /// Committed layout rows removed for conclusively gone homes.
    pub rows_removed: usize,
    /// Unique inode bytes of the blobs and staging files removed.
    pub bytes_reclaimed: u64,
    /// Tree candidates proved referenced and retained.
    pub trees_retained: usize,
    /// Blob candidates proved referenced and retained.
    pub blobs_retained: usize,
    /// Native metadata-cache objects removed (or, in preview, that would go).
    pub native_removed: usize,
    /// Another process held the publish lock, so nothing was examined.
    pub lock_busy: bool,
    /// Evidence or a scan bound was hit; every candidate was retained.
    pub incomplete: bool,
}

impl Outcome {
    /// Total store entries this pass removed (or would remove in preview).
    pub fn removed(&self) -> usize {
        self.trees_removed
            + self.blobs_removed
            + self.views_removed
            + self.native_removed
            + self.staging_removed
            + self.rows_removed
    }

    /// True when another pass should be scheduled soon.
    pub fn backlog(&self) -> bool {
        self.lock_busy || self.incomplete
    }
}

/// Process-local memo of the payload paths each retained tree references.
///
/// Manifests are immutable and content-addressed, so a read result stays
/// valid for the life of the process; the memo only saves re-reading them on
/// later passes. It is scratch space a restart rebuilds, never a durable
/// reference count, and an entry is dropped the moment its tree is removed so
/// its payloads become collectable.
#[derive(Default)]
struct Memo {
    /// Tree key to the store-relative blob paths its manifest names.
    trees: BTreeMap<String, BTreeSet<PathBuf>>,
}

/// Broker-local memos only, keyed by store root identity.
static MEMOS: OnceLock<Mutex<BTreeMap<String, Memo>>> = OnceLock::new();

/// Returns the shared memo registry without touching the filesystem.
fn memos() -> &'static Mutex<BTreeMap<String, Memo>> {
    MEMOS.get_or_init(|| Mutex::new(BTreeMap::new()))
}

/// Returns the effective user id; collection only ever touches owned entries.
fn euid() -> u32 {
    // SAFETY: geteuid inspects process identity and takes no pointers.
    unsafe { libc::geteuid() }
}

/// One referenced shared tree, keyed by scope and manifest digest.
fn tree_key(scope: &str, manifest: &str) -> String {
    format!("{scope}/{manifest}")
}

/// Returns `true` when `name` is exactly 64 lowercase hexadecimal digits.
fn is_digest_name(name: &str) -> bool {
    name.len() == 64
        && name
            .bytes()
            .all(|byte| matches!(byte, b'0'..=b'9' | b'a'..=b'f'))
}

/// Returns `true` for one canonical payload name `<64 hex>-600` or `-700`,
/// the only shapes a published blob can carry; any other owned file is a
/// foreign object, never a deletion candidate.
fn is_blob_name(name: &str) -> bool {
    match name.split_once('-') {
        Some((digest, mode)) => is_digest_name(digest) && (mode == "600" || mode == "700"),
        None => false,
    }
}

/// Returns `true` for one publisher staging name: `.agent-run-staging-<32
/// lowercase hex>.tmp`, the only provenance this codebase ever creates.
fn is_staging_name(name: &str) -> bool {
    let Some(rest) = name.strip_prefix(TEMP_PREFIX) else {
        return false;
    };
    let Some(hex) = rest.strip_suffix(TEMP_SUFFIX) else {
        return false;
    };
    hex.len() == 32
        && hex
            .bytes()
            .all(|byte| matches!(byte, b'0'..=b'9' | b'a'..=b'f'))
}

/// Returns `true` when a protected path covers `object`: names it, lies
/// inside it, or is an ancestor of it.
///
/// A configuration or service may reference a whole scope container rather
/// than one leaf — `trees/<scope>` or `plugin-views/<scope>` — and every
/// object beneath that ancestor stays referenced with it.
fn covers(path: &Path, object: &Path) -> bool {
    path == object || path.starts_with(object) || object.starts_with(path)
}

/// Returns `true` only when `error` is a plain `NotFound`.
fn not_found(error: &Error) -> bool {
    matches!(error, Error::Io(inner) if inner.kind() == std::io::ErrorKind::NotFound)
}

/// Returns `true` only when a raw metadata error is a plain `NotFound`.
fn not_found_io(error: &std::io::Error) -> bool {
    error.kind() == std::io::ErrorKind::NotFound
}

/// Bounded streaming reader for one directory's entry names.
///
/// Names arrive in batches of at most [`SCAN_BATCH`], so a huge namespace
/// never loads into memory at once. What each name costs is the caller's
/// decision — a proved-referenced name is checked by name alone — so retained
/// objects ahead of garbage never consume a destructive budget and passes
/// converge instead of restarting at the first page forever.
struct Names {
    /// Live directory stream, closed when the reader is dropped.
    scan: fs::DirScan,
    /// Not-yet-yielded names of the last batch read.
    pending: std::vec::IntoIter<std::ffi::OsString>,
    /// The stream already reached end of list.
    finished: bool,
}

impl Names {
    /// Opens one bounded stream over `dir`.
    fn open(dir: &fs::Dir) -> Result<Self> {
        Ok(Self {
            scan: dir.scan()?,
            pending: Vec::new().into_iter(),
            finished: false,
        })
    }

    /// Returns the next entry name, or `None` at end of list.
    fn next(&mut self) -> Result<Option<std::ffi::OsString>> {
        loop {
            if let Some(name) = self.pending.next() {
                return Ok(Some(name));
            }
            if self.finished {
                return Ok(None);
            }
            let (names, done) = self.scan.next_batch(SCAN_BATCH)?;
            self.pending = names.into_iter();
            self.finished = done;
        }
    }
}

/// Runs one bounded, reference-aware collection pass over the shared store.
///
/// `app_home` is the canonical agent-run home; a missing shared namespace is
/// an empty outcome, not an error. Every deletion decision is made while the
/// publish lock is held, after the references below it were re-derived, so a
/// concurrent publisher — which must hold the same lock to install an object
/// or register a layout — can never introduce a reference this pass missed.
/// In [`Mode::Preview`] nothing is written anywhere: rows and objects are
/// only counted, the lock file is never created, and the process memo is not
/// consulted or updated, so a preview cannot report a synchronized plan it
/// did not actually prove — an unprovable preview reports `lock_busy` or
/// `incomplete` instead.
pub fn sweep(store: &mut Store, app_home: &Path, mode: Mode) -> Result<Outcome> {
    let mut outcome = Outcome::default();
    let root = crate::runtime_storage::store_root(app_home)?;
    let store_dir = match fs::Dir::open(&root) {
        Ok(directory) => directory,
        Err(_) => return Ok(outcome),
    };
    let Some(_guard) = SharedStoreLock::try_acquire(&root, mode == Mode::Apply)? else {
        outcome.lock_busy = true;
        return Ok(outcome);
    };
    let identity = store_dir.entry(None)?;
    if identity.uid != euid() {
        return Err(invalid("shared store root is not owned by this user"));
    }
    let memo_key = format!("{}:{}", identity.device, identity.inode);
    // Evidence first, under the lock. Evidence that cannot be collected
    // completely — a corrupt registry row, an oversized snapshot, an
    // unreadable configuration — retains every candidate; the caller retries.
    let mut protected: Vec<PathBuf> = match store.storage_protection_snapshot() {
        Ok(proof) => proof.protected_paths().map(Path::to_path_buf).collect(),
        Err(_) => {
            outcome.incomplete = true;
            return Ok(outcome);
        }
    };
    match crate::housekeeping::config_paths(app_home) {
        // The live configuration may name a shared tree, view or payload file
        // directly (a service command or working directory); retention
        // collects the same evidence for the same reason. A configuration
        // that exists but cannot be read or parsed is uncertainty, not proof
        // of no references.
        Ok(paths) => protected.extend(paths),
        Err(_) => {
            outcome.incomplete = true;
            return Ok(outcome);
        }
    }
    let mut references = match registered_references(store, &protected, mode, &mut outcome) {
        Ok(references) => references,
        Err(_) => {
            outcome.incomplete = true;
            return Ok(outcome);
        }
    };
    // A configuration or service path inside the view namespace pins that
    // view and, through its manifest, the tree and payloads beneath it.
    protect_views(&root, &protected, &mut references, &mut outcome)?;
    // Native cache references — controlled links, operation journals and the
    // metadata objects behind them — join the same census before anything is
    // deleted. A partial or unreadable native census retains every candidate.
    if !native_census(&root, &mut references, &mut outcome)? {
        staging_pass(&root, mode, &mut outcome)?;
        return Ok(outcome);
    }
    let mut memo = match mode {
        Mode::Apply => memos()
            .lock()
            .expect("memo lock is not poisoned")
            .remove(&memo_key)
            .unwrap_or_default(),
        // A preview never mutates remembered scan state; it builds its own.
        Mode::Preview => Memo {
            trees: BTreeMap::new(),
        },
    };
    if memo.trees.len() > MEMO_LIMIT {
        return Err(invalid("shared store collection memo exceeds bound"));
    }
    view_pass(&root, &references, mode, &mut outcome)?;
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(PASS_SECONDS);
    let trees_complete = tree_pass(
        &root,
        &protected,
        &references,
        mode,
        &mut memo,
        deadline,
        &mut outcome,
    )?;
    if !trees_complete {
        // No blob can be proven unreferenced until every remaining tree was
        // enumerated in one pass; retain all blobs and retry.
        outcome.incomplete = true;
    } else {
        blob_pass(
            &root,
            &protected,
            mode,
            &references,
            &memo,
            deadline,
            &mut outcome,
        )?;
    }
    native_cache_pass(&root, &protected, &references, mode, &mut outcome)?;
    staging_pass(&root, mode, &mut outcome)?;
    if mode == Mode::Apply {
        memos()
            .lock()
            .expect("memo lock is not poisoned")
            .insert(memo_key, memo);
    }
    Ok(outcome)
}

/// The complete reference census of the layout registry and native caches.
struct References {
    /// Trees extant registered rows pin, by scope and manifest digest.
    pinned_trees: BTreeSet<String>,
    /// Derived views extant registered rows pin, by scope and view identity.
    expected_views: BTreeSet<String>,
    /// Payload paths native cache roots and journals reference.
    native_blobs: BTreeSet<PathBuf>,
    /// Extant registered homes, the census input for native references.
    extant_homes: Vec<PathBuf>,
}

/// Pages through the whole layout registry and returns the trees and views
/// every extant row pins, removing the committed rows whose home is
/// conclusively gone and unreferenced.
///
/// A `prepared` row always pins what it names, whatever the filesystem says,
/// and never ages out — even while its physical home is absent. A `committed`
/// row pins while its physical home exists; the home counts as gone only on a
/// plain `NotFound` resolution — a permission error, an I/O error, a wrong
/// entry type or an alias keeps the row and marks the census incomplete. A
/// live row whose observed resolution cannot open its home is retained whole:
/// its recorded references stay in the census, its row stays in the registry,
/// and an error is returned so no destructive pass of any kind runs — the
/// native and journal references inside an unreadable home are unprovable, so
/// an unreadable live home is uncertainty, never an empty reference set. The
/// census is all-or-nothing: a page that fails, or a registry beyond the page
/// bound, returns an error so **no** candidate of any kind is deleted from
/// partial evidence. Rows are removed only in [`Mode::Apply`]; the store's own
/// removal additionally refuses while any agent identity still binds the
/// home, which is authoritative.
fn registered_references(
    store: &mut Store,
    protected: &[PathBuf],
    mode: Mode,
    outcome: &mut Outcome,
) -> Result<References> {
    let mut references = References {
        pinned_trees: BTreeSet::new(),
        expected_views: BTreeSet::new(),
        native_blobs: BTreeSet::new(),
        extant_homes: Vec::new(),
    };
    let mut removable = Vec::new();
    // Set when a live home exists but cannot be opened: its native references
    // are unprovable, so the whole census is withheld after every row pinned.
    let mut unreadable_live_home = false;
    let mut after: Option<String> = None;
    let mut pages = 0;
    loop {
        let (page, more) = store.runtime_storage_layouts_page(after.as_deref(), ROWS_PER_PASS)?;
        pages += 1;
        for record in page {
            after = Some(record.runtime_home.clone());
            let layout = record.layout();
            let home = PathBuf::from(&record.runtime_home);
            // Only a plain `NotFound` on **both** resolutions proves the home
            // gone. A resolution that fails any other way — permission, I/O,
            // a wrong entry type — means the home's existence is unknown: the
            // row stays live and the census reports incomplete evidence
            // instead of presenting a silent guess as clean.
            let (opened, metadata) = (fs::Dir::open(&home), std::fs::symlink_metadata(&home));
            let home_gone = match (&opened, &metadata) {
                (Err(a), Err(b)) if not_found(a) && not_found_io(b) => true,
                (Ok(_), _) | (_, Ok(_)) => false,
                // The home's existence is unknown, so the census is not a
                // census: nothing of any kind may be deleted from it.
                (Err(_), Err(_)) => {
                    outcome.incomplete = true;
                    return Err(invalid(
                        "a registered home's existence could not be determined",
                    ));
                }
            };
            // A prepared row is always live. A committed row is live while
            // its physical home exists, and one more time while any protected
            // path still lies inside the home.
            let live = match record.state {
                crate::state::runtime_storage::LayoutState::Prepared => true,
                crate::state::runtime_storage::LayoutState::Committed => {
                    !home_gone || protected.iter().any(|path| path.starts_with(&home))
                }
            };
            if !live {
                removable.push(record.runtime_home.clone());
                continue;
            }
            // A live row pins what it records whatever its home's state; only
            // the native census input depends on the home opening.
            match &opened {
                // Every extant registered home — mapped, cache-only or
                // mid-switch — is the census input for native references.
                Ok(_) => references.extant_homes.push(home.clone()),
                // An absent live home (a prepared row, or a committed one a
                // protected path still names) holds no native references.
                Err(error) if not_found(error) => {}
                Err(_) => unreadable_live_home = true,
            }
            for (root, mapping) in &layout.roots {
                references
                    .pinned_trees
                    .insert(tree_key(&mapping.scope, &mapping.manifest_sha256));
                // A plugin version root keeps one derived view alive: its
                // identity is deterministic from the registered mapping,
                // so the expected set needs no mutable state.
                if let Some((_, version)) = plugin_views::plugin_mount(root) {
                    let identity = plugin_views::view_identity(
                        &shared_assets::SharedTreeRef {
                            scope: mapping.scope.clone(),
                            manifest_sha256: mapping.manifest_sha256.clone(),
                        },
                        &version,
                    )?;
                    references
                        .expected_views
                        .insert(format!("{}/{}", mapping.scope, identity));
                }
            }
        }
        if !more {
            break;
        }
        if pages >= row_pages_limit() {
            // A registry beyond the bound is not a census; deleting anything
            // from it could delete a live reference's object.
            return Err(invalid("layout registry exceeds the census bound"));
        }
    }
    if unreadable_live_home {
        // Rows stay and nothing is deleted: an unreadable live home is
        // uncertainty, never an empty reference set.
        outcome.incomplete = true;
        return Err(invalid("a live registered home could not be opened"));
    }
    if mode == Mode::Apply {
        for home in &removable {
            if store.remove_runtime_storage_layout(home).unwrap_or(false) {
                outcome.rows_removed += 1;
            }
        }
    } else {
        outcome.rows_removed = removable.len();
    }
    Ok(references)
}

/// Pins every derived view a protected path names or contains, with the tree
/// and payloads beneath it.
///
/// A configuration or service path may point straight into a view container,
/// or at a file inside one, without any layout row left to describe it. The
/// view's own manifest is the proof: its bytes hash to the backing tree's
/// manifest digest, which pins that tree so its payloads stay referenced. A
/// protected view whose metadata cannot be read and parsed is uncertainty —
/// every view is retained and the census stops destructive work.
fn protect_views(
    root: &Path,
    protected: &[PathBuf],
    references: &mut References,
    outcome: &mut Outcome,
) -> Result<()> {
    let namespace = root.join(plugin_views::VIEW_NAMESPACE);
    for path in protected {
        // A service rooted above the namespace protects every view beneath it.
        let path = if namespace.starts_with(path) {
            &namespace
        } else {
            path
        };
        let Ok(rest) = path.strip_prefix(&namespace) else {
            continue;
        };
        let mut parts = rest.iter();
        // A reference may name the namespace root, one scope container, one
        // view container, or a file inside one. Every level protects whole
        // views — and, through each view's own manifest, the tree and payload
        // beneath it — using the same proof a leaf reference uses.
        let scope = parts.next();
        let view = parts.next();
        let Some(scope) = scope else {
            // The namespace root itself: every view in every scope.
            let views = match fs::Dir::open(&namespace) {
                Ok(views) => views,
                // An explicitly protected namespace that cannot be inspected
                // is uncertainty, never a silent empty reference set.
                Err(error) if not_found(&error) => continue,
                Err(_) => {
                    outcome.incomplete = true;
                    return Ok(());
                }
            };
            for name in views.list(None)? {
                let Some(scope) = name.to_str().map(str::to_owned) else {
                    continue;
                };
                pin_whole_scope(root, &scope, references, outcome)?;
            }
            continue;
        };
        let scope = scope.to_string_lossy().into_owned();
        if !is_digest_name(&scope) {
            continue;
        }
        match view {
            // A reference to the scope container protects every view in it.
            None => pin_whole_scope(root, &scope, references, outcome)?,
            Some(view) => {
                let view = view.to_string_lossy().into_owned();
                if is_digest_name(&view) {
                    pin_view(root, &scope, &view, references, outcome)?;
                }
            }
        }
    }
    Ok(())
}

/// Pins every view of one scope container, and each one's backing tree.
fn pin_whole_scope(
    root: &Path,
    scope: &str,
    references: &mut References,
    outcome: &mut Outcome,
) -> Result<()> {
    let namespace = root.join(plugin_views::VIEW_NAMESPACE);
    let scope_dir = match fs::Dir::open(&namespace.join(scope)) {
        Ok(directory) => directory,
        // A scope that simply has no views yet protects nothing further.
        Err(error) if not_found(&error) => return Ok(()),
        // A protected scope that cannot be inspected is uncertainty.
        Err(_) => {
            outcome.incomplete = true;
            return Ok(());
        }
    };
    for name in scope_dir.list(None)? {
        let Some(view) = name.to_str().filter(|name| is_digest_name(name)) else {
            continue;
        };
        pin_view(root, scope, view, references, outcome)?;
    }
    Ok(())
}

/// Pins one explicitly protected view and the tree whose payloads it holds.
///
/// The view's own manifest bytes are the proof: they hash to the backing
/// tree's manifest digest. Without that proof no object in the store may be
/// deleted this pass — an unreadable or malformed manifest on a protected
/// view is missing evidence, never an empty reference set.
fn pin_view(
    root: &Path,
    scope: &str,
    view: &str,
    references: &mut References,
    outcome: &mut Outcome,
) -> Result<()> {
    references.expected_views.insert(format!("{scope}/{view}"));
    match read_view_manifest(root, scope, view)? {
        Some(digest) => {
            references.pinned_trees.insert(tree_key(scope, &digest));
        }
        None => outcome.incomplete = true,
    }
    Ok(())
}

/// Returns the backing tree's manifest digest of one view container, read
/// from the container's own unchanged manifest bytes.
///
/// `None` when the container cannot be opened, holds no manifest, or its
/// bytes do not hash to a digest shape — never a guess.
fn read_view_manifest(root: &Path, scope: &str, view: &str) -> Result<Option<String>> {
    let container = match fs::Dir::open(
        &root
            .join(plugin_views::VIEW_NAMESPACE)
            .join(scope)
            .join(view),
    ) {
        Ok(container) => container,
        Err(_) => return Ok(None),
    };
    for name in container.list(None)? {
        let version = PathBuf::from(name);
        if container.entry_type(&version)? != fs::EntryType::Directory {
            continue;
        }
        let inside = container.subdir(&version)?;
        let Some(bytes) = inside.optional(Path::new(SNAPSHOT_MANIFEST), 64 * 1024)? else {
            return Ok(None);
        };
        let digest = fs::sha256(&bytes);
        return Ok(is_digest_name(&digest).then_some(digest));
    }
    Ok(None)
}

/// Merges every extant registered home's native cache references into the
/// census, under the same lock and before any destructive pass.
///
/// Homes are paged so one pass's work stays bounded no matter how many are
/// registered; each page's trees and journals pin through the normal tree
/// pass, which proves each pinned tree under this lock. A page that reports
/// `complete == false` — an unreadable home, link, record or manifest, or a
/// census bound — is partial evidence: `false` is returned, every candidate
/// of every kind is retained, and the caller retries.
fn native_census(root: &Path, references: &mut References, outcome: &mut Outcome) -> Result<bool> {
    if references.extant_homes.is_empty() {
        return Ok(true);
    }
    let mut complete = true;
    let mut merged = crate::native_tree_cache::NativeRefScan::default();
    for page in references.extant_homes.chunks(NATIVE_HOMES_PER_PAGE) {
        let homes: Vec<&Path> = page.iter().map(PathBuf::as_path).collect();
        let scan = crate::native_tree_cache::scan_refs(root, &homes);
        complete &= scan.complete;
        merged.merge(scan);
        if !complete {
            break;
        }
    }
    if !complete {
        outcome.incomplete = true;
        return Ok(false);
    }
    for key in merged.trees.keys() {
        references.pinned_trees.insert(key.clone());
    }
    if references.native_blobs.len() + merged.blobs.len() > REFERENCED_BLOBS_LIMIT {
        outcome.incomplete = true;
        return Ok(false);
    }
    references.native_blobs.extend(merged.blobs);
    Ok(true)
}

/// Collects metadata-cache objects no live reference covers.
///
/// Both censuses — every object in the namespace, and every exact live link
/// from the extant registered homes — must be complete before
/// `deletion_candidates` returns anything at all; otherwise the objects are
/// retained and the pass reports `incomplete`. Protected paths, including
/// ancestors, pin a whole scope's objects the same way they pin trees.
fn native_cache_pass(
    root: &Path,
    protected: &[PathBuf],
    references: &References,
    mode: Mode,
    outcome: &mut Outcome,
) -> Result<()> {
    let objects = crate::native_cache::enumerate_native_cache_objects(root)?;
    let homes: Vec<PathBuf> = references.extant_homes.clone();
    let census = crate::native_cache::collect_native_cache_references(root, &homes)?;
    if !objects.complete || !census.complete {
        outcome.incomplete = true;
        return Ok(());
    }
    let namespace = root.join(crate::native_cache::NATIVE_CACHE_NAMESPACE);
    let mut removed = 0;
    for object in crate::native_cache::deletion_candidates(&objects, &census) {
        if removed >= NATIVE_OBJECTS_PER_PASS {
            outcome.incomplete = true;
            return Ok(());
        }
        // A protected path naming the object, something inside it, or any of
        // its ancestors — up to the namespace itself — keeps the object
        // regardless of the link census.
        if protected
            .iter()
            .any(|path| covers(path, &object) || covers(path, &namespace))
        {
            continue;
        }
        outcome.native_removed += 1;
        if mode == Mode::Apply {
            let relative = object
                .strip_prefix(root)
                .map_err(|_| invalid("native cache object escapes its store root"))?;
            let store = fs::Dir::open(root)?;
            let parent = relative
                .parent()
                .ok_or_else(|| invalid("native cache object has no scope parent"))?;
            let name = relative
                .file_name()
                .ok_or_else(|| invalid("native cache object has no name"))?;
            let scope_dir = store.subdir(parent)?;
            let name = Path::new(name);
            let size = scope_dir
                .open_file(name)?
                .metadata()
                .map_err(Error::from)?
                .len();
            scope_dir.remove(name)?;
            outcome.bytes_reclaimed += size;
            removed += 1;
        }
    }
    Ok(())
}

/// Collects obsolete derived plugin-parent views before their trees.
///
/// A view is live when an extant registered row maps a plugin version root
/// onto its tree, or a protected path names it or something inside it. A
/// deletion candidate must parse as one of this store's view containers —
/// digest-named, owned, a real directory — and draining that cannot finish
/// leaves the directory in place, resumable, with `incomplete` set; the count
/// reports views actually removed, not removals started.
fn view_pass(
    root: &Path,
    references: &References,
    mode: Mode,
    outcome: &mut Outcome,
) -> Result<()> {
    let views = match fs::Dir::open(&root.join(plugin_views::VIEW_NAMESPACE)) {
        Ok(directory) => directory,
        Err(_) => return Ok(()),
    };
    let mut roots = TREE_ROOTS_PER_PASS;
    let mut scopes = Names::open(&views)?;
    while let Some(scope_name) = scopes.next()? {
        let Some(scope) = scope_name
            .to_str()
            .filter(|name| is_digest_name(name))
            .map(str::to_owned)
        else {
            outcome.incomplete = true;
            continue;
        };
        let scope_dir = match views.subdir(Path::new(&scope)) {
            Ok(directory) => directory,
            Err(_) => {
                outcome.incomplete = true;
                continue;
            }
        };
        let mut entries = Names::open(&scope_dir)?;
        while let Some(name) = entries.next()? {
            let Some(identity) = name.to_str().filter(|name| is_digest_name(name)) else {
                // A foreign name in the namespace is an unknown object.
                outcome.incomplete = true;
                continue;
            };
            if references
                .expected_views
                .contains(&format!("{scope}/{identity}"))
            {
                continue;
            }
            if roots == 0 {
                outcome.incomplete = true;
                return Ok(());
            }
            let relative = Path::new(identity);
            let entry = match scope_dir.entry(Some(relative)) {
                Ok(entry) => entry,
                Err(_) => continue,
            };
            if entry.kind != fs::EntryType::Directory || entry.uid != euid() {
                // Not provably one of this store's views: retained.
                outcome.incomplete = true;
                continue;
            }
            if mode == Mode::Apply {
                roots -= 1;
                // Same shape as trees: move into the staging namespace first,
                // so a partially drained view is an ordinary resumable orphan.
                let staging = PathBuf::from(format!(
                    "{TEMP_PREFIX}{}{TEMP_SUFFIX}",
                    uuid::Uuid::new_v4().simple()
                ));
                if !scope_dir.rename_entry_no_replace(relative, &staging)? {
                    return Err(invalid("shared store view removal lost its race"));
                }
            }
            outcome.views_removed += 1;
        }
    }
    Ok(())
}

/// Collects unreferenced trees and returns whether the enumeration of every
/// remaining tree completed inside this pass.
///
/// A tree is a deletion candidate only when no registered row pins it, no
/// protected path lies inside it, and its own manifest still parses through
/// the store's validators — the read both proves provenance and yields the
/// payloads it references. Retained trees are checked by name and memo, so
/// hundreds of retained objects ahead of garbage never consume the budget
/// and passes converge. Every pinned tree the census does not actually find,
/// and every manifest it cannot read, stops destructive blob work for the
/// pass: an absent proof never widens the candidate set.
fn tree_pass(
    root: &Path,
    protected: &[PathBuf],
    references: &References,
    mode: Mode,
    memo: &mut Memo,
    deadline: std::time::Instant,
    outcome: &mut Outcome,
) -> Result<bool> {
    let trees = match fs::Dir::open(&root.join("trees")) {
        Ok(directory) => directory,
        Err(error) => {
            if not_found(&error) {
                return Ok(references.pinned_trees.is_empty());
            }
            outcome.incomplete = true;
            return Ok(false);
        }
    };
    let mut roots = TREE_ROOTS_PER_PASS;
    let mut complete = true;
    let mut seen: BTreeSet<String> = BTreeSet::new();
    let mut scopes = Names::open(&trees)?;
    while let Some(scope_name) = scopes.next()? {
        let Some(scope) = scope_name
            .to_str()
            .filter(|name| is_digest_name(name))
            .map(str::to_owned)
        else {
            outcome.incomplete = true;
            complete = false;
            continue;
        };
        let scope_dir = match trees.subdir(Path::new(&scope)) {
            Ok(directory) => directory,
            Err(_) => {
                outcome.incomplete = true;
                complete = false;
                continue;
            }
        };
        let mut entries = Names::open(&scope_dir)?;
        while let Some(name) = entries.next()? {
            let Some(manifest) = name.to_str().filter(|name| is_digest_name(name)) else {
                // A foreign name in the namespace is an unknown object.
                outcome.incomplete = true;
                complete = false;
                continue;
            };
            let key = tree_key(&scope, manifest);
            seen.insert(key.clone());
            let absolute = root.join("trees").join(&scope).join(manifest);
            let pinned = references.pinned_trees.contains(&key)
                || protected.iter().any(|path| covers(path, &absolute));
            if pinned {
                outcome.trees_retained += 1;
                // The payloads a retained tree references must be proved
                // before anything may be deleted; an unverifiable tree is
                // missing proof, never an empty reference set. Proofs stream
                // payload bytes, so a fresh one respects the pass deadline and
                // the rest retries later — memoized proofs cost nothing.
                if !memo.trees.contains_key(&key) && std::time::Instant::now() >= deadline {
                    outcome.incomplete = true;
                    complete = false;
                    continue;
                }
                if proved_tree(root, memo, &scope, manifest).is_err() {
                    outcome.incomplete = true;
                    complete = false;
                }
                continue;
            }
            if roots == 0 || std::time::Instant::now() >= deadline {
                outcome.incomplete = true;
                return Ok(false);
            }
            // The store's own verifier proves the entry really is one of its
            // published trees before anything is unlinked; anything less
            // would let a foreign directory with a digest name pass.
            if proved_tree(root, memo, &scope, manifest).is_err() {
                // Unreadable, corrupt or foreign evidence retains the object.
                outcome.incomplete = true;
                outcome.trees_retained += 1;
                complete = false;
                continue;
            }
            if mode == Mode::Apply {
                roots -= 1;
                // The proved-unreferenced object is moved, atomically and
                // under the lock, into the recognized publisher-staging
                // namespace of its own scope before any byte is unlinked. A
                // canonical published tree therefore never becomes partial —
                // a partial drain of a staging name resumes without needing a
                // verifier, across passes and across process restarts. The
                // staging pass below drains it within its unlink budget.
                let staging = PathBuf::from(format!(
                    "{TEMP_PREFIX}{}{TEMP_SUFFIX}",
                    uuid::Uuid::new_v4().simple()
                ));
                if !scope_dir.rename_entry_no_replace(Path::new(manifest), &staging)? {
                    return Err(invalid("shared store tree removal lost its race"));
                }
                // Its payloads are no longer referenced by this tree.
                memo.trees.remove(&key);
            }
            outcome.trees_removed += 1;
        }
    }
    // A pinned tree the scan never found means the census cannot account for
    // every reference: stop destructive work rather than trust it.
    if !references.pinned_trees.iter().all(|key| seen.contains(key)) {
        outcome.incomplete = true;
        complete = false;
    }
    Ok(complete && !outcome.incomplete)
}

/// Proves one shared tree and memoizes the payloads it references.
///
/// The proof is the store's own full verifier — topology, hardlink identity
/// and content — never a bare manifest read: a manifest that parses but was
/// replaced lists no payloads and would fabricate an empty reference set,
/// unlinking blobs the tree's files still hold. A verified tree's manifest is
/// immutable and content-addressed, so the result is memoized once per
/// process; a tree that fails the verifier is missing proof, and the caller
/// stops destructive work.
///
/// ponytail: streaming every retained payload once per process is the price
/// of a complete proof; if it measures slow on huge stores, add a cheaper
/// manifest-vs-topology shape check in the platform verifier and reuse it.
fn proved_tree(
    root: &Path,
    memo: &mut Memo,
    scope: &str,
    manifest: &str,
) -> Result<BTreeSet<PathBuf>> {
    let key = tree_key(scope, manifest);
    if let Some(names) = memo.trees.get(&key) {
        return Ok(names.clone());
    }
    let reference = shared_assets::SharedTreeRef {
        scope: scope.to_owned(),
        manifest_sha256: manifest.to_owned(),
    };
    shared_assets::verify_shared_tree(root, &reference)?;
    let names = shared_assets::shared_tree_blob_names(root, &reference)?;
    if memo.trees.len() > MEMO_LIMIT {
        return Err(invalid("shared store collection memo exceeds bound"));
    }
    memo.trees.insert(key, names.clone());
    Ok(names)
}

/// Collects payload blobs no remaining valid tree references.
///
/// Runs only on a pass whose tree enumeration completed, so the referenced
/// set covers every tree still present. A blob named directly by a protected
/// path is retained as well, because configuration or a service may hold it
/// open without any tree. Only canonical payload names are candidates; any
/// other owned file in the namespace is a foreign object and is retained.
fn blob_pass(
    root: &Path,
    protected: &[PathBuf],
    mode: Mode,
    references: &References,
    memo: &Memo,
    deadline: std::time::Instant,
    outcome: &mut Outcome,
) -> Result<()> {
    let mut referenced: BTreeSet<PathBuf> = memo.trees.values().flatten().cloned().collect();
    referenced.extend(references.native_blobs.iter().cloned());
    if referenced.len() > REFERENCED_BLOBS_LIMIT {
        outcome.incomplete = true;
        return Ok(());
    }
    let blobs = match fs::Dir::open(&root.join("blobs")) {
        Ok(directory) => directory,
        Err(_) => return Ok(()),
    };
    let mut removed = 0;
    let mut scopes = Names::open(&blobs)?;
    while let Some(scope_name) = scopes.next()? {
        let Some(scope) = scope_name
            .to_str()
            .filter(|name| is_digest_name(name))
            .map(str::to_owned)
        else {
            outcome.incomplete = true;
            continue;
        };
        let scope_dir = match blobs.subdir(Path::new(&scope)) {
            Ok(directory) => directory,
            Err(_) => {
                outcome.incomplete = true;
                continue;
            }
        };
        let mut entries = Names::open(&scope_dir)?;
        while let Some(name) = entries.next()? {
            let Some(blob) = name.to_str() else {
                outcome.incomplete = true;
                continue;
            };
            if !is_blob_name(blob) {
                // A foreign file in the blob namespace is not a payload.
                outcome.incomplete = true;
                continue;
            }
            let relative = PathBuf::from("blobs").join(&scope).join(blob);
            let absolute = root.join(&relative);
            if referenced.contains(&relative)
                || protected.iter().any(|path| covers(path, &absolute))
            {
                outcome.blobs_retained += 1;
                continue;
            }
            if std::time::Instant::now() >= deadline {
                outcome.incomplete = true;
                return Ok(());
            }
            if removed >= BLOBS_PER_PASS {
                outcome.incomplete = true;
                return Ok(());
            }
            let entry = match scope_dir.entry(Some(Path::new(blob))) {
                Ok(entry) => entry,
                Err(_) => continue,
            };
            if entry.kind != fs::EntryType::File || entry.uid != euid() {
                continue;
            }
            let size = scope_dir
                .open_file(Path::new(blob))?
                .metadata()
                .map_err(Error::from)?
                .len();
            if mode == Mode::Apply {
                scope_dir.remove(Path::new(blob))?;
                removed += 1;
            }
            outcome.blobs_removed += 1;
            outcome.bytes_reclaimed += size;
        }
    }
    Ok(())
}

/// Removes publisher staging orphans inside the owned namespaces.
///
/// A candidate qualifies only under the exact staging name shape, inside a
/// digest-named scope directory of the matching namespace, as the entry type
/// that namespace's publisher creates, and owned by the effective user. This
/// process holds the publish lock, so no live publisher can own the orphan;
/// age alone never qualifies anything. A directory whose drain cannot finish
/// stays in place, resumable on the next pass, and only completed removals
/// are counted.
fn staging_pass(root: &Path, mode: Mode, outcome: &mut Outcome) -> Result<()> {
    let store = fs::Dir::open(root)?;
    for (namespace, kind) in [
        ("trees", fs::EntryType::Directory),
        ("blobs", fs::EntryType::File),
        (plugin_views::VIEW_NAMESPACE, fs::EntryType::Directory),
        (
            crate::native_cache::NATIVE_CACHE_NAMESPACE,
            fs::EntryType::File,
        ),
    ] {
        let Some(container) = store.subdir(Path::new(namespace)).ok() else {
            continue;
        };
        let mut scopes = Names::open(&container)?;
        while let Some(scope_name) = scopes.next()? {
            let Some(scope) = scope_name
                .to_str()
                .filter(|name| is_digest_name(name))
                .map(str::to_owned)
            else {
                continue;
            };
            let Some(scope_dir) = container.subdir(Path::new(&scope)).ok() else {
                continue;
            };
            let mut entries = Names::open(&scope_dir)?;
            while let Some(name) = entries.next()? {
                if outcome.staging_removed >= STAGING_PER_PASS {
                    outcome.incomplete = true;
                    return Ok(());
                }
                let Some(name) = name.to_str() else { continue };
                if !is_staging_name(name) {
                    continue;
                }
                let relative = Path::new(name);
                let entry = match scope_dir.entry(Some(relative)) {
                    Ok(entry) => entry,
                    Err(_) => continue,
                };
                if entry.kind != kind || entry.uid != euid() {
                    continue;
                }
                let size = match kind {
                    fs::EntryType::File => scope_dir
                        .open_file(relative)?
                        .metadata()
                        .map_err(Error::from)?
                        .len(),
                    _ => 0,
                };
                if mode == Mode::Apply {
                    match kind {
                        fs::EntryType::Directory => {
                            let staged = scope_dir.subdir(relative)?;
                            let mut unlinks = TREE_UNLINKS_PER_PASS;
                            if !drain(&staged, &mut unlinks)? {
                                // Partially drained stays resumable.
                                outcome.incomplete = true;
                                continue;
                            }
                            scope_dir.remove_directory(relative)?;
                        }
                        _ => scope_dir.remove(relative)?,
                    }
                }
                outcome.staging_removed += 1;
                outcome.bytes_reclaimed += size;
            }
        }
    }
    Ok(())
}

/// Drains one owned read-only staging directory through its live descriptor
/// under a bounded unlink budget. Returns whether the directory became empty;
/// the caller removes it from its parent once it does, and an interrupted
/// drain stays in place and resumes on a later pass — a staging name never
/// needs a verifier, which is exactly why removals move there first.
fn drain(directory: &fs::Dir, unlinks: &mut usize) -> Result<bool> {
    directory.permit_owner_write()?;
    for name in directory.list(None)? {
        if *unlinks == 0 {
            return Ok(false);
        }
        let relative = PathBuf::from(&name);
        match directory.entry_type(&relative)? {
            fs::EntryType::Directory => {
                let child = directory.subdir(&relative)?;
                if !drain(&child, unlinks)? {
                    return Ok(false);
                }
                directory.remove_directory(&relative)?;
            }
            fs::EntryType::File => directory.remove(&relative)?,
            kind => {
                return Err(invalid(format!(
                    "shared store holds an unexpected entry: {kind:?}"
                )))
            }
        }
        *unlinks -= 1;
    }
    Ok(true)
}
