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
//! definitions — any of which may name a shared tree or a single payload file
//! directly. Anything unreadable, malformed, unparsable or beyond a scan bound
//! is retained and retried; unknown is never classified as unreferenced, and
//! there are no live reference counts to drift.
//!
//! Committed layout rows are removed only once their physical home is
//! conclusively gone and no protected path or agent identity still references
//! the home; prepared rows never age out. Collection order is derived plugin
//! views, then trees, then the payload blobs derived from the manifests of
//! the trees that remain: a view file is an internal hardlink, so an obsolete
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

/// Upper bound on layout rows read per pass.
const ROWS_PER_PASS: usize = 256;
/// Upper bound on tree manifests examined per pass.
const TREES_PER_PASS: usize = 256;
/// Upper bound on tree directories one pass begins draining.
const TREE_ROOTS_PER_PASS: usize = 4;
/// Upper bound on entry unlinks inside draining trees per pass.
const TREE_UNLINKS_PER_PASS: usize = 512;
/// Upper bound on blob candidates examined per pass.
const BLOBS_PER_PASS: usize = 512;
/// Upper bound on staging orphans examined per pass.
const STAGING_PER_PASS: usize = 64;
/// Upper bound on remembered round state before a round is refused.
const ROUND_TREES_LIMIT: usize = 20_000;
/// Upper bound on referenced blob paths held while collecting.
const REFERENCED_BLOBS_LIMIT: usize = 200_000;

/// Whether one pass may unlink, or only report what it would reclaim.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Mode {
    /// Measure candidates and report them; change nothing.
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
    /// Tree candidates examined and retained because a reference pinned them.
    pub trees_retained: usize,
    /// Blob candidates examined and retained because a reference pinned them.
    pub blobs_retained: usize,
    /// Another process held the publish lock, so nothing was examined.
    pub lock_busy: bool,
    /// Evidence or a scan bound was hit; candidates were retained for retry.
    pub incomplete: bool,
}

impl Outcome {
    /// Total store entries this pass removed (or would remove in preview).
    pub fn removed(&self) -> usize {
        self.trees_removed + self.blobs_removed + self.staging_removed + self.rows_removed
    }

    /// True when another pass should be scheduled soon.
    pub fn backlog(&self) -> bool {
        self.lock_busy || self.incomplete
    }
}

/// Broker-local collection-round state for one store root.
///
/// `retained` memoizes the blob paths of every tree the round proved pinned,
/// so later passes rebuild the complete referenced set without re-reading
/// their manifests, and a huge store drains over successive passes instead of
/// re-reading the same first page forever. It is process-local scratch space
/// that a restart simply rebuilds, never a durable reference count.
#[derive(Default)]
struct Round {
    /// Pinned tree key to the store-relative blob paths its manifest names.
    retained: BTreeMap<String, Vec<PathBuf>>,
    /// True once every tree namespace reached end-of-list in this round.
    trees_complete: bool,
    /// True once every blob namespace reached end-of-list in this round.
    blobs_complete: bool,
}

/// Broker-local rounds only; a restart starts a fresh round.
static ROUNDS: OnceLock<Mutex<BTreeMap<String, Round>>> = OnceLock::new();

/// Returns the shared round registry without touching the filesystem.
fn rounds() -> &'static Mutex<BTreeMap<String, Round>> {
    ROUNDS.get_or_init(|| Mutex::new(BTreeMap::new()))
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

/// Returns `true` for one publisher staging name: `.agent-run-staging-<32
/// lowercase hex>.tmp`, the only provenance this codebase ever creates.
fn is_staging_name(name: &str) -> bool {
    let Some(rest) = name.strip_prefix(TEMP_PREFIX) else {
        return false;
    };
    let Some(hex) = rest.strip_suffix(TEMP_SUFFIX) else {
        return false;
    };
    hex.len() == 32 && hex.bytes().all(|byte| matches!(byte, b'0'..=b'9' | b'a'..=b'f'))
}

/// Runs one bounded, reference-aware collection pass over the shared store.
///
/// `app_home` is the canonical agent-run home; a missing shared namespace is
/// an empty outcome, not an error. Every deletion decision is made while the
/// publish lock is held, after the references below it were re-derived, so a
/// concurrent publisher — which must hold the same lock to install an object
/// or register a layout — can never introduce a reference this pass missed.
/// The store connection is only used for bounded reads and the explicit row
/// removal; no write transaction stays open across filesystem work.
pub fn sweep(store: &mut Store, app_home: &Path, mode: Mode) -> Result<Outcome> {
    let mut outcome = Outcome::default();
    let root = crate::runtime_storage::store_root(app_home)?;
    let store_dir = match fs::Dir::open(&root) {
        Ok(directory) => directory,
        Err(_) => return Ok(outcome),
    };
    let Some(_guard) = SharedStoreLock::try_acquire(&root)? else {
        outcome.lock_busy = true;
        return Ok(outcome);
    };
    let identity = store_dir.entry(None)?;
    if identity.uid != euid() {
        return Err(invalid("shared store root is not owned by this user"));
    }
    let round_key = format!("{}:{}", identity.device, identity.inode);
    // Evidence first, under the lock. Evidence that cannot be collected
    // completely retains every candidate; the caller retries.
    // Evidence first, under the lock. Evidence that cannot be collected
    // completely — a corrupt registry row, an oversized snapshot — retains
    // every candidate; the caller retries.
    let protected: Vec<PathBuf> = match store.storage_protection_snapshot() {
        Ok(proof) => {
            let mut paths: Vec<PathBuf> =
                proof.protected_paths().map(Path::to_path_buf).collect();
            // The live configuration may name a shared tree or payload file
            // directly (a service command or working directory); retention
            // collects the same evidence for the same reason.
            paths.extend(crate::housekeeping::config_paths(app_home));
            paths
        }
        Err(_) => {
            outcome.incomplete = true;
            return Ok(outcome);
        }
    };
    let (pinned_trees, expected_views) =
        match registered_references(store, &protected, &mut outcome) {
            Ok(references) => references,
            Err(_) => {
                outcome.incomplete = true;
                return Ok(outcome);
            }
        };
    view_pass(&root, &expected_views, mode, &mut outcome)?;
    let mut round = rounds()
        .lock()
        .expect("round lock is not poisoned")
        .remove(&round_key)
        .unwrap_or_default();
    tree_pass(
        &root,
        &protected,
        &pinned_trees,
        mode,
        &mut round,
        &mut outcome,
    )?;
    if round.trees_complete {
        blob_pass(&root, &protected, mode, &mut round, &mut outcome)?;
    } else {
        // No blob can be proven unreferenced until every remaining tree was
        // enumerated in one pass; retain all blobs and retry.
        outcome.incomplete = true;
    }
    staging_pass(&root, mode, &mut outcome)?;
    if round.trees_complete && round.blobs_complete {
        round = Round::default();
    }
    if round.retained.len() > ROUND_TREES_LIMIT {
        return Err(invalid("shared store collection round exceeds bound"));
    }
    rounds()
        .lock()
        .expect("round lock is not poisoned")
        .insert(round_key, round);
    Ok(outcome)
}

/// Pages through the layout registry, removes the committed rows whose home
/// is conclusively gone and unreferenced, and returns the trees every
/// remaining extant row pins.
///
/// A `prepared` row always pins what it names, whatever the filesystem says.
/// A `committed` row pins while its physical home exists; once the home is
/// gone it stops pinning, and its row is removed only when no protected path
/// lies inside the home either — the store's own removal additionally refuses
/// while any agent identity still binds the home, which is authoritative.
fn registered_references(
    store: &mut Store,
    protected: &[PathBuf],
    outcome: &mut Outcome,
) -> Result<(BTreeSet<String>, BTreeSet<String>)> {
    let mut pinned = BTreeSet::new();
    let mut expected_views = BTreeSet::new();
    let mut removable = Vec::new();
    let mut after: Option<String> = None;
    let mut pages = 0;
    loop {
        let (page, more) = store.runtime_storage_layouts_page(after.as_deref(), ROWS_PER_PASS)?;
        pages += 1;
        for record in page {
            after = Some(record.runtime_home.clone());
            let layout = record.layout();
            let home = PathBuf::from(&record.runtime_home);
            let home_gone = fs::Dir::open(&home).is_err()
                && std::fs::symlink_metadata(&home).is_err();
            // A prepared row is always live. A committed row is live while
            // its physical home exists, and one more time while any protected
            // path still lies inside the home.
            let live = match record.state {
                crate::state::runtime_storage::LayoutState::Prepared => true,
                crate::state::runtime_storage::LayoutState::Committed => {
                    !home_gone || protected.iter().any(|path| path.starts_with(&home))
                }
            };
            if live {
                for (root, mapping) in &layout.roots {
                    pinned.insert(tree_key(&mapping.scope, &mapping.manifest_sha256));
                    // A plugin version root keeps one derived view alive: its
                    // identity is deterministic from the registered mapping,
                    // so the expected set needs no mutable state.
                    if let Some((_, version)) = plugin_views::plugin_mount(root) {
                        if let Ok(identity) = plugin_views::view_identity(
                            &shared_assets::SharedTreeRef {
                                scope: mapping.scope.clone(),
                                manifest_sha256: mapping.manifest_sha256.clone(),
                            },
                            &version,
                        ) {
                            expected_views.insert(format!("{}/{}", mapping.scope, identity));
                        }
                    }
                }
            } else {
                removable.push(record.runtime_home.clone());
            }
        }
        if !more {
            break;
        }
        if pages * ROWS_PER_PASS >= ROUND_TREES_LIMIT {
            outcome.incomplete = true;
            break;
        }
    }
    for home in &removable {
        if store.remove_runtime_storage_layout(home).unwrap_or(false) {
            outcome.rows_removed += 1;
        }
    }
    Ok((pinned, expected_views))
}

/// Collects obsolete derived plugin-parent views before their trees.
///
/// A view is live exactly when some extant registered layout row maps a plugin
/// version root onto the tree the view was derived from; `expected` holds the
/// deterministic view identities of exactly those rows. Everything else under
/// the namespace is collected first — a view file is an internal hardlink, so
/// leaving an obsolete one alive keeps its payload allocated and a stale view
/// whose tree is gone would corrupt a future import's reuse of the same
/// content-addressed name. Unknown names and unreadable entries are retained.
fn view_pass(
    root: &Path,
    expected: &BTreeSet<String>,
    mode: Mode,
    outcome: &mut Outcome,
) -> Result<()> {
    let views = match fs::Dir::open(&root.join(plugin_views::VIEW_NAMESPACE)) {
        Ok(directory) => directory,
        Err(_) => return Ok(()),
    };
    let mut roots = TREE_ROOTS_PER_PASS;
    let mut unlinks = TREE_UNLINKS_PER_PASS;
    for scope_name in views.list(None)? {
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
        for name in scope_dir.list(None)? {
            let Some(identity) = name.to_str().filter(|name| is_digest_name(name)) else {
                continue;
            };
            if expected.contains(&format!("{scope}/{identity}")) {
                continue;
            }
            if outcome.views_removed >= TREE_ROOTS_PER_PASS || roots == 0 || unlinks == 0 {
                outcome.incomplete = true;
                return Ok(());
            }
            let relative = Path::new(identity);
            let entry = match scope_dir.entry(Some(relative)) {
                Ok(entry) => entry,
                Err(_) => continue,
            };
            if entry.kind != fs::EntryType::Directory || entry.uid != euid() {
                continue;
            }
            outcome.views_removed += 1;
            if mode == Mode::Apply {
                roots -= 1;
                let view = scope_dir.subdir(relative)?;
                if drain_tree(&view, Path::new(""), &mut unlinks)? {
                    scope_dir.remove_directory(relative)?;
                } else {
                    outcome.incomplete = true;
                }
            }
        }
    }
    Ok(())
}

/// Collects unreferenced trees and records the payload set retained trees name.
///
/// A tree is removed only when no registered row pins it and no protected
/// path lies inside it. A tree whose manifest cannot be read and validated is
/// retained with `incomplete`, never treated as unreferenced; draining removes
/// the manifest last so an interrupted drain stays classifiable and resumes.
fn tree_pass(
    root: &Path,
    protected: &[PathBuf],
    pinned_trees: &BTreeSet<String>,
    mode: Mode,
    round: &mut Round,
    outcome: &mut Outcome,
) -> Result<()> {
    let trees = match fs::Dir::open(&root.join("trees")) {
        Ok(directory) => directory,
        Err(_) => {
            round.trees_complete = true;
            return Ok(());
        }
    };
    let mut examined = 0;
    let mut roots = TREE_ROOTS_PER_PASS;
    let mut unlinks = TREE_UNLINKS_PER_PASS;
    let mut complete = true;
    'scopes: for scope_name in trees.list(None)? {
        let Some(scope) = scope_name
            .to_str()
            .filter(|name| is_digest_name(name))
            .map(str::to_owned)
        else {
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
        for name in scope_dir.list(None)? {
            let Some(manifest) = name.to_str().filter(|name| is_digest_name(name)) else {
                complete = false;
                continue;
            };
            if examined >= TREES_PER_PASS {
                complete = false;
                break 'scopes;
            }
            examined += 1;
            let key = tree_key(&scope, manifest);
            let absolute = root.join("trees").join(&scope).join(manifest);
            if pinned_trees.contains(&key) {
                outcome.trees_retained += 1;
                memoize(root, round, &scope, manifest)?;
                continue;
            }
            if protected.iter().any(|path| path.starts_with(&absolute)) {
                outcome.trees_retained += 1;
                round.retained.insert(key, Vec::new());
                continue;
            }
            let reference = shared_assets::SharedTreeRef {
                scope: scope.clone(),
                manifest_sha256: manifest.to_owned(),
            };
            // Reading the manifest proves the entry really is one of this
            // store's published trees before anything is unlinked.
            if shared_assets::shared_tree_blob_names(root, &reference).is_err() {
                // Unreadable or corrupt evidence retains the object.
                outcome.incomplete = true;
                outcome.trees_retained += 1;
                complete = false;
                continue;
            }
            outcome.trees_removed += 1;
            if mode == Mode::Apply {
                if roots == 0 || unlinks == 0 {
                    complete = false;
                    outcome.incomplete = true;
                    break 'scopes;
                }
                roots -= 1;
                let tree_dir = scope_dir.subdir(Path::new(manifest))?;
                if drain_tree(&tree_dir, Path::new(SNAPSHOT_MANIFEST), &mut unlinks)? {
                    scope_dir.remove_directory(Path::new(manifest))?;
                } else {
                    outcome.incomplete = true;
                }
            }
        }
    }
    round.trees_complete = complete && !outcome.incomplete;
    Ok(())
}

/// Memoizes one pinned tree's blob paths for the rest of the round.
fn memoize(root: &Path, round: &mut Round, scope: &str, manifest: &str) -> Result<()> {
    let key = tree_key(scope, manifest);
    if round.retained.contains_key(&key) {
        return Ok(());
    }
    let reference = shared_assets::SharedTreeRef {
        scope: scope.to_owned(),
        manifest_sha256: manifest.to_owned(),
    };
    let names = match shared_assets::shared_tree_blob_names(root, &reference) {
        Ok(names) => names.into_iter().collect(),
        Err(_) => {
            // The tree is pinned regardless; unknown payloads simply are not
            // added to the referenced set, which can only retain a blob
            // longer, never delete a referenced one.
            Vec::new()
        }
    };
    round.retained.insert(key, names);
    Ok(())
}

/// Collects payload blobs no remaining valid tree references.
///
/// Runs only on a pass whose tree enumeration was complete, so the referenced
/// set covers every tree still present. A blob named directly by a protected
/// path is retained as well, because configuration or a service may hold it
/// open without any tree.
fn blob_pass(
    root: &Path,
    protected: &[PathBuf],
    mode: Mode,
    round: &mut Round,
    outcome: &mut Outcome,
) -> Result<()> {
    let referenced: BTreeSet<PathBuf> = round.retained.values().flatten().cloned().collect();
    if referenced.len() > REFERENCED_BLOBS_LIMIT {
        outcome.incomplete = true;
        return Ok(());
    }
    let blobs = match fs::Dir::open(&root.join("blobs")) {
        Ok(directory) => directory,
        Err(_) => {
            round.blobs_complete = true;
            return Ok(());
        }
    };
    let mut examined = 0;
    let mut removed = 0;
    let mut complete = true;
    'scopes: for scope_name in blobs.list(None)? {
        let Some(scope) = scope_name
            .to_str()
            .filter(|name| is_digest_name(name))
            .map(str::to_owned)
        else {
            complete = false;
            continue;
        };
        let scope_dir = match blobs.subdir(Path::new(&scope)) {
            Ok(directory) => directory,
            Err(_) => {
                outcome.incomplete = true;
                complete = false;
                continue;
            }
        };
        for name in scope_dir.list(None)? {
            let Some(blob) = name.to_str() else {
                complete = false;
                continue;
            };
            if examined >= BLOBS_PER_PASS || removed >= BLOBS_PER_PASS {
                complete = false;
                break 'scopes;
            }
            examined += 1;
            let relative = PathBuf::from("blobs").join(&scope).join(blob);
            let absolute = root.join(&relative);
            let entry = match scope_dir.entry(Some(Path::new(blob))) {
                Ok(entry) => entry,
                Err(_) => {
                    complete = false;
                    continue;
                }
            };
            if entry.kind != fs::EntryType::File || entry.uid != euid() {
                continue;
            }
            if referenced.contains(&relative)
                || protected
                    .iter()
                    .any(|path| path == &absolute || path.starts_with(&absolute))
            {
                outcome.blobs_retained += 1;
                continue;
            }
            outcome.blobs_removed += 1;
            if mode == Mode::Apply {
                removed += 1;
                let size = scope_dir
                    .open_file(Path::new(blob))?
                    .metadata()
                    .map_err(Error::from)?
                    .len();
                scope_dir.remove(Path::new(blob))?;
                outcome.bytes_reclaimed += size;
            } else {
                outcome.bytes_reclaimed += scope_dir
                    .open_file(Path::new(blob))?
                    .metadata()
                    .map_err(Error::from)?
                    .len();
            }
        }
    }
    if removed >= BLOBS_PER_PASS {
        outcome.incomplete = true;
    }
    round.blobs_complete = complete && !outcome.incomplete;
    Ok(())
}

/// Removes publisher staging orphans inside the two owned namespaces.
///
/// A candidate qualifies only under the exact staging name shape, inside a
/// digest-named scope directory of the matching namespace, as the entry type
/// that namespace's publisher creates, and owned by the effective user. This
/// process holds the publish lock, so no live publisher can own the orphan;
/// age alone never qualifies anything.
fn staging_pass(root: &Path, mode: Mode, outcome: &mut Outcome) -> Result<()> {
    let store = fs::Dir::open(root)?;
    for (namespace, kind) in [
        ("trees", fs::EntryType::Directory),
        ("blobs", fs::EntryType::File),
        (plugin_views::VIEW_NAMESPACE, fs::EntryType::Directory),
    ] {
        let Some(container) = store.subdir(Path::new(namespace)).ok() else {
            continue;
        };
        for scope_name in container.list(None)? {
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
            for name in scope_dir.list(None)? {
                if outcome.staging_removed >= STAGING_PER_PASS {
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
                if kind == fs::EntryType::File {
                    outcome.bytes_reclaimed +=
                        scope_dir.open_file(relative)?.metadata().map_err(Error::from)?.len();
                }
                outcome.staging_removed += 1;
                if mode == Mode::Apply {
                    match kind {
                        fs::EntryType::Directory => {
                            let staged = scope_dir.subdir(relative)?;
                            let mut unlinks = TREE_UNLINKS_PER_PASS;
                            drain_tree(&staged, Path::new(""), &mut unlinks)?;
                            scope_dir.remove_directory(relative)?;
                        }
                        _ => scope_dir.remove(relative)?,
                    }
                }
            }
        }
    }
    Ok(())
}

/// Drains one owned read-only tree directory through its live descriptor,
/// removing its manifest last so an interrupted drain stays classifiable and
/// resumes on a later pass. Returns whether the directory became empty; the
/// caller removes it from its parent once it does.
fn drain_tree(directory: &fs::Dir, manifest: &Path, unlinks: &mut usize) -> Result<bool> {
    directory.permit_owner_write()?;
    for name in directory.list(None)? {
        if *unlinks == 0 {
            return Ok(false);
        }
        let relative = PathBuf::from(&name);
        if relative == manifest {
            continue;
        }
        match directory.entry_type(&relative)? {
            fs::EntryType::Directory => {
                let child = directory.subdir(&relative)?;
                if !drain_tree(&child, Path::new(""), unlinks)? {
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
    // The manifest goes last, so a partially drained tree keeps describing
    // itself and stays resumable.
    if directory.list(None)?.iter().any(|name| name != manifest) {
        return Ok(false);
    }
    if *unlinks == 0 {
        return Ok(false);
    }
    if manifest != Path::new("") {
        directory.remove(manifest)?;
        *unlinks -= 1;
    }
    Ok(true)
}
