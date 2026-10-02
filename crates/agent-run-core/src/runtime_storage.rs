//! Filesystem relocation coordinator moving sealed managed trees into the
//! shared store, with roll-forward crash recovery.
//!
//! This unit owns the physical switch itself. It plans a version-1
//! `RuntimeStorageLayout` from a strictly verified original home, imports
//! every indexed managed tree into the caller's shared store, swaps each
//! private root for one exact whole-tree symlink, verifies the unchanged
//! original index through the shared bridge, and commits the registry row.
//! The original private copies are staged inside the runtime home under an
//! operation-token-bound backup name, so one prepared registry row pins both
//! the home and its recovery material; nothing is ever removed before the
//! owning assets are proven. Frozen index bytes, native-history files,
//! credential links, and authority digests are never rewritten.
//!
//! Managed Codex plugin version roots are the one geometry switched
//! differently: native plugin discovery ignores a version directory that is
//! itself a symlink, so [`agent_run_platform::plugin_views::plugin_mount`]
//! roots are mounted one level higher. The whole plugin parent moves into
//! the token-bound backup and one exact symlink replaces the parent,
//! pointing at a readonly store container holding the correctly named real
//! version subtree hardlinked from the imported tree. A parent holding
//! anything but its single indexed version is refused during planning and
//! preserved untouched.

use crate::{Result, adapters, domain::AgentId, fs, state};
use agent_run_domain::{Error, error::invalid};
use agent_run_platform::{
    plugin_views,
    shared_assets::{self, SharedStoreLock, SharedTreeRef},
    snapshot_tree::{self, RUNTIME_SNAPSHOT_INDEX, SNAPSHOT_MANIFEST},
};
use std::{
    collections::BTreeMap,
    path::{Path, PathBuf},
};

/// Fixed shared-store namespace below the canonical app home.
const STORE_NAMESPACE: [&str; 2] = ["shared-assets", "v1"];
/// Prefix of the operation backup directory inside one runtime home.
const BACKUP_PREFIX: &str = ".agent-run-storage-";
/// Upper bound for one index read, matching the platform metadata bound.
const MAX_INDEX_BYTES: usize = 64 * 1024;

/// One simulated crash point inside [`install_with_fault`]'s switch
/// sequence; production [`install`] never fires any of these.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StorageFault {
    /// Before a private root is moved into the operation backup.
    BeforeRename,
    /// After that move and before the whole-tree link is created — the window
    /// where the home root name does not exist yet.
    AfterRename,
    /// After every root is linked and the original index verified through the
    /// bridge, before the registry row is committed.
    AfterLink,
    /// After the registry commit, before the proven backups are removed.
    AfterCommit,
}

/// Returns the shared-store root below `app_home` without mutating anything.
///
/// `app_home` must be an existing absolute canonical real directory; the
/// result is always `<app_home>/shared-assets/v1`. When any part of the
/// namespace already exists it must be a real directory reached without
/// symlinked components (checked through no-follow descriptors and by
/// canonical form), so an aliased or foreign entry is refused instead of
/// adopted. A missing namespace is derived, not created: the launch path may
/// create the empty trusted root before guard validation, and [`install`]
/// creates it before its first import.
pub fn store_root(app_home: &Path) -> Result<PathBuf> {
    if !app_home.is_absolute() {
        return Err(invalid("app home must be absolute"));
    }
    let canonical = app_home
        .canonicalize()
        .map_err(|_| invalid("app home must be an existing real directory"))?;
    if canonical != app_home {
        return Err(invalid(
            "app home must be canonical without symlinked components",
        ));
    }
    let root = app_home.join(STORE_NAMESPACE[0]).join(STORE_NAMESPACE[1]);
    if std::fs::symlink_metadata(&root).is_ok() {
        let directory = fs::Dir::open(app_home)?;
        match directory.entry_type(&Path::new(STORE_NAMESPACE[0]).join(STORE_NAMESPACE[1])) {
            Ok(fs::EntryType::Directory) => {}
            _ => {
                return Err(invalid(
                    "existing shared store namespace must be a real directory without links",
                ));
            }
        }
        if root.canonicalize().map_err(|_| {
            invalid("existing shared store namespace must be an openable real directory")
        })? != root
        {
            return Err(invalid(
                "existing shared store namespace must be canonical without symlinked components",
            ));
        }
    }
    Ok(root)
}

/// Returns the canonical absolute form of one existing runtime home, the only
/// key form the registry accepts.
fn canonical_home(runtime_home: &Path) -> Result<PathBuf> {
    let canonical = runtime_home
        .canonicalize()
        .map_err(|_| invalid("runtime home must be an existing real directory"))?;
    if !canonical.is_absolute() || canonical == Path::new("/") {
        return Err(invalid("runtime home must be an absolute canonical path"));
    }
    Ok(canonical)
}

/// Reads one home's exact original index bytes and returns them with the
/// materialization revision and parsed document, proving the bytes still hash
/// to `expected` first.
fn index_document(home: &Path, expected: &str) -> Result<(String, serde_json::Value)> {
    let directory = fs::Dir::open(home)?;
    let raw = directory.read(Path::new(RUNTIME_SNAPSHOT_INDEX), MAX_INDEX_BYTES)?;
    if fs::sha256(&raw) != expected {
        return Err(invalid(
            "runtime snapshot index hash does not match the expected original digest",
        ));
    }
    let document: serde_json::Value =
        serde_json::from_slice(&raw).map_err(|_| invalid("runtime snapshot index is malformed"))?;
    let revision = document
        .get("materialize_revision")
        .and_then(serde_json::Value::as_str)
        .ok_or_else(|| invalid("runtime snapshot index is malformed"))?
        .to_owned();
    Ok((revision, document))
}

/// Returns whether `token` is safe path material: the registry's own
/// `rt_`-prefixed lowercase-hex spelling and nothing else.
fn valid_token(token: &str) -> bool {
    token.strip_prefix("rt_").is_some_and(|rest| {
        !rest.is_empty()
            && rest
                .bytes()
                .all(|byte| matches!(byte, b'0'..=b'9' | b'a'..=b'f'))
    })
}

/// The operation backup directory name inside one home for one token.
fn backup_base(token: &str) -> Result<String> {
    if !valid_token(token) {
        return Err(invalid(
            "storage operation token is not valid path material",
        ));
    }
    Ok(format!("{BACKUP_PREFIX}{token}"))
}

/// Converts one layout's root mappings into shared references for the
/// platform import and bridge APIs.
fn references(
    layout: &state::runtime_storage::RuntimeStorageLayout,
) -> BTreeMap<String, SharedTreeRef> {
    layout
        .roots
        .iter()
        .map(|(root, mapping)| {
            (
                root.clone(),
                SharedTreeRef {
                    scope: mapping.scope.clone(),
                    manifest_sha256: mapping.manifest_sha256.clone(),
                },
            )
        })
        .collect()
}

/// Plans the shared layout for one sealed home without mutating anything.
///
/// `expected` is the home's original frozen runtime-index digest and `scope`
/// the validated account or compatibility domain every tree will be imported
/// under. The home (or an existing committed registry binding for it) is
/// strictly verified first: the exact index bytes must hash to `expected`,
/// every indexed root must still verify as a private directory, and a
/// committed row must bind the same index digest. Root references are derived
/// only from the original index's own `manifests` map. Returns `None` when
/// the index describes no managed roots, otherwise a validated version-1
/// layout carrying the home's canonical path. A prepared row is not a
/// binding: planning continues against the original home and fails if the
/// home is mid-switch, directing the caller to `recover` first. This
/// function performs no database or filesystem mutation.
pub fn plan(
    store: &state::Store,
    app_home: &Path,
    runtime_home: &Path,
    expected: &str,
    scope: &str,
) -> Result<Option<state::runtime_storage::RuntimeStorageLayout>> {
    if !shared_assets::is_scope(scope) {
        return Err(invalid(
            "shared store scope must be 64 lowercase hexadecimal digits",
        ));
    }
    store_root(app_home)?;
    let canonical = canonical_home(runtime_home)?;
    let key = canonical.to_string_lossy().into_owned();
    if let Some(record) = store.runtime_storage_layout(&key)?
        && record.state == state::runtime_storage::LayoutState::Committed
    {
        if record.index_sha256 != expected {
            return Err(invalid(
                "registered storage layout binds a different original index digest",
            ));
        }
        return Ok(Some(record.layout().clone()));
    }
    let (revision, document) = index_document(&canonical, expected)?;
    let inspection = snapshot_tree::inspect_runtime_snapshots(&canonical, &revision, expected)?;
    if !inspection.verified {
        return Err(invalid(
            "runtime home must strictly verify before a shared layout is planned",
        ));
    }
    let manifests = document
        .get("manifests")
        .and_then(serde_json::Value::as_object)
        .ok_or_else(|| invalid("runtime snapshot index is malformed"))?;
    let roots = document
        .get("roots")
        .and_then(serde_json::Value::as_array)
        .ok_or_else(|| invalid("runtime snapshot index is malformed"))?;
    let mut mapped = BTreeMap::new();
    for root in roots {
        let root = root
            .as_str()
            .ok_or_else(|| invalid("runtime snapshot index is malformed"))?;
        let manifest = manifests
            .get(root)
            .and_then(serde_json::Value::as_str)
            .ok_or_else(|| invalid("runtime snapshot index is malformed"))?;
        mapped.insert(
            root.to_owned(),
            state::runtime_storage::SharedRootMapping {
                scope: scope.to_owned(),
                manifest_sha256: manifest.to_owned(),
            },
        );
    }
    if mapped.is_empty() {
        return Ok(None);
    }
    validate_plugin_parents(&canonical, &mapped)?;
    let layout = state::runtime_storage::RuntimeStorageLayout {
        version: 1,
        runtime_home: key,
        index_sha256: expected.to_owned(),
        roots: mapped,
    };
    layout.validate()?;
    Ok(Some(layout))
}

/// Validates the parent geometry every managed plugin root needs before any
/// layout is planned or prepared.
///
/// A strictly verified home already guarantees each root's ancestors are real
/// directories; this check additionally proves every plugin-classified root's
/// parent holds exactly its one indexed version entry and nothing else — no
/// second version, remote metadata marker, or any sibling an unmountable
/// parent link would strand. Two indexed versions of one plugin therefore
/// fail here together, before a registry row exists, with the whole original
/// parent preserved for the caller. Any failure is explicit: nothing is
/// mutated, skipped, or dropped.
fn validate_plugin_parents(
    home: &Path,
    mapped: &BTreeMap<String, state::runtime_storage::SharedRootMapping>,
) -> Result<()> {
    let directory = fs::Dir::open(home)?;
    for root_key in mapped.keys() {
        let Some((parent, version)) = plugin_views::plugin_mount(root_key) else {
            continue;
        };
        match directory.entry_type(Path::new(&parent)) {
            Ok(fs::EntryType::Directory) => {}
            _ => {
                return Err(invalid(format!(
                    "managed plugin parent {parent} must be a real directory"
                )));
            }
        }
        let names = directory.list(Some(Path::new(&parent)))?;
        if names.len() != 1 || names[0].to_str() != Some(version.as_str()) {
            return Err(invalid(format!(
                "managed plugin parent {parent} must hold exactly its indexed version \
                 {version}; unexpected sibling entries refuse the shared layout"
            )));
        }
    }
    Ok(())
}

/// Anchors one strictly verified sealed home that maps no managed roots.
///
/// A cache-only home still owns shared content — its native caches — and the
/// registry row is the one durable anchor that keeps the physical home in the
/// collector's census after its agent history expires. The home is verified
/// exactly as [`plan`] verifies it, then an empty version-1 layout is
/// installed through the same coordinator path: registry `prepare` under the
/// store publish lock, bridge verification of the unchanged original index
/// with an empty reference map, compare-and-swap commit. The row survives
/// until the physical home is conclusively gone, exactly like a mapped row.
/// An existing committed row binding the same digest is idempotent; a
/// prepared row is an explicit refusal directing the caller to `recover`.
/// Index bytes, native history and authority digests are never rewritten.
pub fn anchor(
    store: &mut state::Store,
    app_home: &Path,
    runtime_home: &Path,
    expected: &str,
    owner: Option<&AgentId>,
) -> Result<()> {
    store_root(app_home)?;
    let canonical = canonical_home(runtime_home)?;
    let key = canonical.to_string_lossy().into_owned();
    if let Some(record) = store.runtime_storage_layout(&key)? {
        return match record.state {
            state::runtime_storage::LayoutState::Committed if record.index_sha256 == expected => {
                Ok(())
            }
            state::runtime_storage::LayoutState::Committed => Err(invalid(
                "registered storage layout binds a different original index digest",
            )),
            state::runtime_storage::LayoutState::Prepared => Err(invalid(
                "runtime home has a prepared storage layout; recover it before anchoring",
            )),
        };
    }
    let (revision, _) = index_document(&canonical, expected)?;
    let inspection = snapshot_tree::inspect_runtime_snapshots(&canonical, &revision, expected)?;
    if !inspection.verified {
        return Err(invalid(
            "runtime home must strictly verify before it is anchored",
        ));
    }
    let layout = state::runtime_storage::RuntimeStorageLayout {
        version: 1,
        runtime_home: key,
        index_sha256: expected.to_owned(),
        roots: BTreeMap::new(),
    };
    layout.validate()?;
    install(store, app_home, &layout, owner)
}

/// Installs one planned layout: imports every tree, swaps each private root
/// for the exact whole-tree link (or, for a managed plugin root, its whole
/// parent for the exact plugin-view link), verifies the unchanged original
/// index through the shared bridge, commits the registry row, then removes
/// only backups proven to still hold the replaced assets.
///
/// The caller validates the actual harness guard before calling. The layout
/// must validate and be its own canonical encoding. An existing committed row
/// for the same layout makes this idempotent: the installed state is verified
/// through the bridge and any leftover proven backups are cleaned, then the
/// call returns. An existing prepared row for the same layout is finished by
/// roll-forward recovery instead of a second switch; any other existing row
/// is a conflict. A fresh install strictly verifies the original home first,
/// prepares the registry under the store publish lock (pins before import,
/// lock dropped before the import reacquires it), imports every tree, swaps
/// roots under a token-bound in-home backup, bridge-verifies, commits, and
/// only then deletes proven backups. On any failure the row stays `prepared`
/// and `recover` completes the switch without re-downloading anything.
pub fn install(
    store: &mut state::Store,
    app_home: &Path,
    layout: &state::runtime_storage::RuntimeStorageLayout,
    owner: Option<&AgentId>,
) -> Result<()> {
    install_with_fault(store, app_home, layout, owner, None)
}

/// Test seam over [`install`] firing `fault` at one [`StorageFault`] crash
/// point; no production caller passes `Some`.
pub fn install_with_fault(
    store: &mut state::Store,
    app_home: &Path,
    layout: &state::runtime_storage::RuntimeStorageLayout,
    owner: Option<&AgentId>,
    fault: Option<&dyn Fn(StorageFault) -> Result<()>>,
) -> Result<()> {
    layout.validate()?;
    let bytes = layout.canonical_bytes()?;
    let layout_json = String::from_utf8(bytes).map_err(|_| invalid("layout is not UTF-8"))?;
    let digest = fs::sha256(layout_json.as_bytes());
    let home_path = Path::new(&layout.runtime_home).to_path_buf();
    let root = store_root(app_home)?;
    match store.runtime_storage_layout(&layout.runtime_home)? {
        Some(record) => {
            if record.layout_sha256 != digest {
                return Err(Error::Conflict);
            }
            match record.state {
                state::runtime_storage::LayoutState::Committed => {
                    verify(store, app_home, &home_path, &layout.index_sha256)?;
                    let refs = references(layout);
                    discard_backups(&home_path, &refs, &record.operation_token)?;
                    Ok(())
                }
                state::runtime_storage::LayoutState::Prepared => {
                    recover_prepared(store, app_home, &record)
                }
            }
        }
        None => {
            let (revision, _) = index_document(&home_path, &layout.index_sha256)?;
            let inspection = snapshot_tree::inspect_runtime_snapshots(
                &home_path,
                &revision,
                &layout.index_sha256,
            )?;
            if !inspection.verified {
                return Err(invalid(
                    "runtime home must strictly verify before a shared layout is installed",
                ));
            }
            fs::private_dir(&root)?;
            let record = {
                let _guard = SharedStoreLock::acquire(&root)?;
                store.prepare_runtime_storage_layout(&layout_json, owner)?
            };
            switch_home(store, app_home, &record, &revision, fault)
        }
    }
}

/// Completes one prepared row's switch: idempotent imports, per-root roll
/// forward, bridge verification, commit, and proven backup cleanup. The
/// registry row is the only authority — every path and reference is derived
/// from its validated layout and operation token.
fn switch_home(
    store: &mut state::Store,
    app_home: &Path,
    record: &state::runtime_storage::RuntimeStorageLayoutRecord,
    revision: &str,
    fault: Option<&dyn Fn(StorageFault) -> Result<()>>,
) -> Result<()> {
    let fire = |point: StorageFault| -> Result<()> {
        match fault {
            Some(fault) => fault(point),
            None => Ok(()),
        }
    };
    let layout = record.layout();
    let refs = references(layout);
    let root = store_root(app_home)?;
    fs::private_dir(&root)?;
    let home_path = Path::new(&layout.runtime_home).to_path_buf();
    let home = fs::Dir::open(&home_path)?;
    let base = backup_base(&record.operation_token)?;
    home.directory(Path::new(&base))?;
    for (root_key, reference) in &refs {
        let target = shared_assets::shared_tree_root(&root, reference)?;
        if let Some((parent, version)) = plugin_views::plugin_mount(root_key) {
            let view = plugin_views::view_root(&root, reference, &version)?;
            match home.entry_type(Path::new(&parent)) {
                Ok(fs::EntryType::Directory) => {
                    // The parent must still hold exactly the indexed version;
                    // anything else was refused at planning and stays an
                    // explicit failure here rather than a silent drop.
                    let names = home.list(Some(Path::new(&parent)))?;
                    if names.len() != 1 || names[0].to_str() != Some(version.as_str()) {
                        return Err(invalid(format!(
                            "managed plugin parent {parent} must hold exactly its indexed \
                             version {version}"
                        )));
                    }
                    shared_assets::import_shared_tree(
                        &root,
                        &reference.scope,
                        &home_path,
                        Path::new(root_key),
                    )?;
                    plugin_views::materialize(&root, reference, &version)?;
                    fire(StorageFault::BeforeRename)?;
                    home.rename_entry_no_replace(
                        Path::new(&parent),
                        &Path::new(&base).join(&parent),
                    )?;
                    fire(StorageFault::AfterRename)?;
                    home.symlink(&view, Path::new(&parent))?;
                }
                Ok(fs::EntryType::Symlink) => {
                    if home.read_link(Path::new(&parent))? != Some(view) {
                        return Err(invalid(format!(
                            "managed plugin parent {parent} is a foreign link"
                        )));
                    }
                }
                Err(Error::Io(error)) if error.kind() == std::io::ErrorKind::NotFound => {
                    // Crash between this parent's rename and link: the backup
                    // holds the original parent and the import is already
                    // pinned; roll forward.
                    let backup = Path::new(&base).join(&parent);
                    match home.entry_type(&backup) {
                        Ok(fs::EntryType::Directory) => {}
                        _ => {
                            return Err(invalid(format!(
                                "managed plugin parent {parent} is missing and has no staged \
                                 backup"
                            )));
                        }
                    }
                    shared_assets::import_shared_tree(
                        &root,
                        &reference.scope,
                        &home_path,
                        &backup.join(&version),
                    )?;
                    plugin_views::materialize(&root, reference, &version)?;
                    home.symlink(&view, Path::new(&parent))?;
                }
                Err(error) => return Err(error),
                Ok(kind) => {
                    return Err(invalid(format!(
                        "managed plugin parent {parent} has an unexpected shape: {kind:?}"
                    )));
                }
            }
            continue;
        }
        match home.entry_type(Path::new(root_key)) {
            Ok(fs::EntryType::Directory) => {
                shared_assets::import_shared_tree(
                    &root,
                    &reference.scope,
                    &home_path,
                    Path::new(root_key),
                )?;
                fire(StorageFault::BeforeRename)?;
                home.rename_entry_no_replace(
                    Path::new(root_key),
                    &Path::new(&base).join(root_key),
                )?;
                fire(StorageFault::AfterRename)?;
                home.symlink(&target, Path::new(root_key))?;
            }
            Ok(fs::EntryType::Symlink) => {
                if home.read_link(Path::new(root_key))? != Some(target) {
                    return Err(invalid(format!(
                        "managed root {root_key} is a foreign link"
                    )));
                }
            }
            Err(Error::Io(error)) if error.kind() == std::io::ErrorKind::NotFound => {
                // Crash between this root's rename and link: the backup holds
                // the original and the import is already pinned; roll forward.
                let backup = Path::new(&base).join(root_key);
                match home.entry_type(&backup) {
                    Ok(fs::EntryType::Directory) => {}
                    _ => {
                        return Err(invalid(format!(
                            "managed root {root_key} is missing and has no staged backup"
                        )));
                    }
                }
                shared_assets::import_shared_tree(&root, &reference.scope, &home_path, &backup)?;
                home.symlink(&target, Path::new(root_key))?;
            }
            Err(error) => return Err(error),
            Ok(kind) => {
                return Err(invalid(format!(
                    "managed root {root_key} has an unexpected shape: {kind:?}"
                )));
            }
        }
    }
    let inspection = snapshot_tree::inspect_runtime_snapshots_with_shared(
        &home_path,
        revision,
        &layout.index_sha256,
        &root,
        &refs,
    )?;
    if !inspection.verified {
        return Err(invalid("converted runtime home failed shared verification"));
    }
    fire(StorageFault::AfterLink)?;
    store.commit_runtime_storage_layout(
        &layout.runtime_home,
        &record.operation_token,
        &record.layout_sha256,
    )?;
    fire(StorageFault::AfterCommit)?;
    discard_backups(&home_path, &refs, &record.operation_token)
}

/// Verifies one runtime home against its original index digest, choosing the
/// verifier from the registry.
///
/// With no registered row the original strict verifier runs unchanged. A
/// committed row binding the same index digest runs the shared bridge with
/// that row's converted references. A prepared row, a committed row binding
/// a different digest, or a corrupt row is an explicit failure — never a
/// silent fallback that would mask a half-installed or foreign state.
pub fn verify(
    store: &state::Store,
    app_home: &Path,
    runtime_home: &Path,
    expected: &str,
) -> Result<adapters::materialize::Snapshot> {
    let canonical = canonical_home(runtime_home)?;
    let key = canonical.to_string_lossy().into_owned();
    match store.runtime_storage_layout(&key)? {
        None => adapters::materialize::verify(&canonical, expected),
        Some(record) => {
            if record.index_sha256 != expected {
                return Err(invalid(
                    "registered storage layout binds a different original index digest",
                ));
            }
            match record.state {
                state::runtime_storage::LayoutState::Committed => {
                    let root = store_root(app_home)?;
                    adapters::materialize::verify_with_shared(
                        &canonical,
                        expected,
                        &root,
                        &references(record.layout()),
                    )
                }
                state::runtime_storage::LayoutState::Prepared => Err(invalid(
                    "runtime home has a prepared storage layout; recover it before verification",
                )),
            }
        }
    }
}

/// Finishes one provably-owned interrupted install by rolling forward.
///
/// With no registered row there is nothing this process provably owns and the
/// call succeeds without touching anything. A prepared row is completed from
/// its own layout and token: still-private roots are imported and swapped,
/// roots stranded between rename and link are relinked from their staged
/// backup, already-correct links are left alone, a foreign or missing root is
/// an explicit failure, the converted home must verify through the bridge,
/// and the row is committed. A committed row only finishes the leftover
/// backup cleanup. Backups are deleted only while still proven to hold the
/// exact replaced assets by manifest digest and strict inspection; anything
/// unknown or changed stays in place with an explicit failure. Recovery never
/// guesses from age, never rewrites the frozen index, and never re-downloads:
/// imports are idempotent reuses of pinned or existing objects.
pub fn recover(store: &mut state::Store, app_home: &Path, runtime_home: &Path) -> Result<()> {
    let canonical = canonical_home(runtime_home)?;
    let key = canonical.to_string_lossy().into_owned();
    let Some(record) = store.runtime_storage_layout(&key)? else {
        return Ok(());
    };
    match record.state {
        state::runtime_storage::LayoutState::Prepared => recover_prepared(store, app_home, &record),
        state::runtime_storage::LayoutState::Committed => {
            let home_path = Path::new(&record.runtime_home).to_path_buf();
            discard_backups(
                &home_path,
                &references(record.layout()),
                &record.operation_token,
            )
        }
    }
}

/// Completes one prepared row, deriving the revision from the home's own
/// untouched index bytes.
fn recover_prepared(
    store: &mut state::Store,
    app_home: &Path,
    record: &state::runtime_storage::RuntimeStorageLayoutRecord,
) -> Result<()> {
    let layout = record.layout();
    let home_path = Path::new(&layout.runtime_home).to_path_buf();
    let (revision, _) = index_document(&home_path, &layout.index_sha256)?;
    switch_home(store, app_home, record, &revision, None)
}

/// Removes the operation backup for every root of one record, but only after
/// proving each staged tree still holds the exact replaced assets; an unknown
/// or changed backup is left in place with an explicit failure.
///
/// A plugin-parent root stages its whole moved parent — the indexed version
/// tree lives one component below it — so the proof reads the manifest and
/// inspects the managed tree at that inner path while removal deletes the
/// staged parent itself. Every other root stages and proves the same
/// single directory, exactly as before.
fn discard_backups(
    home_path: &Path,
    refs: &BTreeMap<String, SharedTreeRef>,
    token: &str,
) -> Result<()> {
    let base = backup_base(token)?;
    let home = fs::Dir::open(home_path)?;
    match home.entry_type(Path::new(&base)) {
        Err(Error::Io(error)) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        _ => {}
    }
    for (root_key, reference) in refs {
        let (staged, tree) = match plugin_views::plugin_mount(root_key) {
            Some((parent, version)) => {
                let staged = Path::new(&base).join(&parent);
                let tree = staged.join(&version);
                (staged, tree)
            }
            None => {
                let staged = Path::new(&base).join(root_key);
                let tree = staged.clone();
                (staged, tree)
            }
        };
        match home.entry_type(&staged) {
            Err(Error::Io(error)) if error.kind() == std::io::ErrorKind::NotFound => continue,
            Ok(fs::EntryType::Directory) => {}
            entry => {
                return Err(invalid(format!(
                    "backup for {root_key} has an unexpected shape: {entry:?}"
                )));
            }
        }
        let proven = home
            .subdir(&tree)
            .ok()
            .and_then(|dir| dir.read(Path::new(SNAPSHOT_MANIFEST), MAX_INDEX_BYTES).ok())
            .is_some_and(|bytes| fs::sha256(&bytes) == reference.manifest_sha256)
            && snapshot_tree::inspect_managed_snapshot(home_path, &tree)
                .map(|inspection| inspection.verified)
                .unwrap_or(false);
        if !proven {
            return Err(invalid(format!(
                "backup for {root_key} cannot be proven to hold the replaced assets; left in place"
            )));
        }
        remove_owned_tree(&home.subdir(&staged)?)?;
        home.remove_directory(&staged)?;
        prune_empty_backup_parents(&home, &staged, Path::new(&base))?;
    }
    if home.list(Some(Path::new(&base)))?.is_empty() {
        home.remove_directory(Path::new(&base))?;
    }
    Ok(())
}

/// Removes now-empty backup intermediates between one removed root backup and
/// the backup base itself.
fn prune_empty_backup_parents(home: &fs::Dir, removed: &Path, base: &Path) -> Result<()> {
    let mut parent = removed.parent().map(Path::to_path_buf);
    while let Some(current) = parent {
        if current == Path::new("") || current == base {
            break;
        }
        if home.list(Some(&current))?.is_empty() {
            home.remove_directory(&current)?;
        } else {
            break;
        }
        parent = current.parent().map(Path::to_path_buf);
    }
    Ok(())
}

/// Empties one owned directory through no-follow descriptors only, restoring
/// owner write permission on each visited directory through its live
/// descriptor. The directory itself is left in place for the caller.
fn remove_owned_tree(directory: &fs::Dir) -> Result<()> {
    directory.permit_owner_write()?;
    for name in directory.list(None)? {
        let relative = PathBuf::from(&name);
        match directory.entry_type(&relative)? {
            fs::EntryType::Directory => {
                remove_owned_tree(&directory.subdir(&relative)?)?;
                directory.remove_directory(&relative)?;
            }
            fs::EntryType::File => directory.remove(&relative)?,
            kind => {
                return Err(invalid(format!(
                    "backup holds an unexpected entry: {kind:?}"
                )));
            }
        }
    }
    Ok(())
}
